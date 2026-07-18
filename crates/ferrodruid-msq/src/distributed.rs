// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Top-level distributed MSQ orchestrator (CL-5 sub-item 5).
//!
//! Wires the pieces:
//!
//! * SQL is planned via [`crate::executor::plan_msq`].
//! * For `SELECT` plans the engine [`crate::engine::QueryDefinition`]
//!   is built via [`crate::executor::plan_to_query_definition`] and
//!   handed to [`MsqCoordinator::run`].
//! * For `INSERT INTO` / `REPLACE INTO` plans the source datasource
//!   is scanned from [`DeepStorage`] via [`crate::segment_io`], the
//!   transform stage runs distributed, and the result rows are
//!   published as a new segment under the target datasource (CL-5
//!   sub-item 4).
//!
//! ## Idempotent insert
//!
//! The published segment id is deterministic from
//! `(task_id, target_data_source)`.  Re-running the same task
//! overwrites the same segment file — combined with the worker
//! idempotency cache, this lets the coordinator retry without
//! producing duplicate segments.
//!
//! ## Closure bar mapping
//!
//! * Sub-item 1: `MsqCoordinator::run` dispatches one slice per
//!   worker; 3+ workers on loopback in
//!   `tests/msq-compat/distributed_e2e.rs`.
//! * Sub-item 2: every inter-stage hop traverses TCP between the
//!   coordinator and workers (loopback ports in the E2E).
//! * Sub-item 3: `MsqCoordinator` reassigns failed slices to a
//!   surviving worker on RPC error / timeout, with deterministic
//!   idempotency tokens.
//! * Sub-item 4: `scan_data_source` and `publish_segment` walk the
//!   real `DeepStorage` trait; INSERT writes a real segment artifact
//!   readable via `Segment::read_jsonl`.
//! * Sub-item 5: `submit_insert_distributed` drives 1-5 end-to-end.

use std::sync::Arc;

use ferrodruid_common::Result;
use ferrodruid_deep_storage::DeepStorage;

use crate::coordinator::{CoordinatorConfig, DistributedResult, MsqCoordinator, WorkerFleet};
use crate::engine::{Row, RowSignature};
use crate::executor::{ExecutionPlan, InputTable, StageType, plan_msq, plan_to_query_definition};
use crate::segment_io::{publish_segment, scan_data_source};
use crate::{
    MsqColumnSignature, MsqError, MsqManager, MsqResults, MsqStage, MsqTaskId, MsqTaskSpec,
};

/// Outcome of a distributed INSERT execution.
#[derive(Debug, Clone)]
pub struct InsertOutcome {
    /// Task id assigned to the run.
    pub task_id: MsqTaskId,
    /// The data source that was written to.
    pub target_data_source: String,
    /// The segment id that was published.
    pub segment_id: String,
    /// Number of rows published.
    pub rows_published: u64,
    /// Stage-level retry count observed on this run.
    pub retry_count: usize,
}

/// Run an `INSERT INTO <target> SELECT ... FROM <source>` MSQ task
/// across the given fleet, reading the source from `deep_storage` and
/// publishing the output as a new segment.
///
/// `deep_storage` is used for both the leaf scan and the insert sink.
///
/// # Errors
///
/// Returns `(task_id, MsqError)` on planning or execution failure
/// (mirrors [`crate::MsqManager::submit_and_run`]'s shape).
#[allow(clippy::too_many_arguments)]
pub async fn submit_insert_distributed(
    spec: MsqTaskSpec,
    manager: &MsqManager,
    fleet: WorkerFleet,
    cfg: CoordinatorConfig,
    deep_storage: Arc<dyn DeepStorage>,
) -> std::result::Result<InsertOutcome, (MsqTaskId, MsqError)> {
    let plan = match plan_msq(&spec.query) {
        Ok(p) => p,
        Err(e) => {
            return Err((
                String::new(),
                MsqError {
                    error: "SqlPlanningError".to_owned(),
                    error_message: e.to_string(),
                },
            ));
        }
    };

    // Find source / target / aggregate stages.
    let target = plan.stages.iter().find_map(|s| match &s.stage_type {
        StageType::Insert {
            target_data_source, ..
        } => Some(target_data_source.clone()),
        _ => None,
    });
    let source = plan.stages.iter().find_map(|s| match &s.stage_type {
        StageType::Scan { data_source, .. } => Some(data_source.clone()),
        _ => None,
    });

    let target = match target {
        Some(t) => t,
        None => {
            return Err((
                String::new(),
                MsqError {
                    error: "MsqPlanError".to_owned(),
                    error_message: "submit_insert_distributed requires an INSERT/REPLACE INTO plan"
                        .to_owned(),
                },
            ));
        }
    };
    let source = match source {
        Some(s) => s,
        None => {
            return Err((
                String::new(),
                MsqError {
                    error: "MsqPlanError".to_owned(),
                    error_message: "insert plan missing source datasource".to_owned(),
                },
            ));
        }
    };

    // Submit the task to the manager so REST callers can observe state.
    let task_id = match manager.submit(spec.clone()) {
        Ok(id) => id,
        Err(e) => {
            return Err((
                String::new(),
                MsqError {
                    error: "MsqSubmitError".to_owned(),
                    error_message: e.to_string(),
                },
            ));
        }
    };

    // 1. Scan source from deep storage.
    let scanned = match scan_data_source(deep_storage.as_ref(), &source).await {
        Ok(s) => s,
        Err(e) => {
            let err = mk_err("MsqScanError", e.to_string());
            let _ = manager.fail_task(&task_id, err.clone());
            return Err((task_id, err));
        }
    };
    let input_signature = scanned.signature.clone();
    let input_rows = scanned.rows;
    let rows_in: u64 = input_rows.len() as u64;

    // 2. Build engine plan from the SQL plan over the scanned signature.
    let qdef = match plan_to_query_definition(&plan, &input_signature) {
        Ok(q) => q,
        Err(e) => {
            let err = mk_err("MsqPlanError", e.to_string());
            let _ = manager.fail_task(&task_id, err.clone());
            return Err((task_id, err));
        }
    };

    // 3. Run distributed.
    let coordinator = MsqCoordinator::new(fleet, cfg);
    let dist = match coordinator.run(&task_id, &qdef, input_rows).await {
        Ok(d) => d,
        Err(e) => {
            let err = mk_err("MsqExecutionError", e.to_string());
            let _ = manager.fail_task(&task_id, err.clone());
            return Err((task_id, err));
        }
    };

    // 4. Publish result rows as a segment under target.
    let segment_id = format!("msq_{task_id}");
    let signature_for_publish = ensure_time_column(&dist.signature);
    let rows_for_publish = ensure_time_values(&dist.signature, &signature_for_publish, &dist.rows);
    if let Err(e) = publish_segment(
        deep_storage.as_ref(),
        &target,
        &segment_id,
        &signature_for_publish,
        &rows_for_publish,
    )
    .await
    {
        let err = mk_err("MsqPublishError", e.to_string());
        let _ = manager.fail_task(&task_id, err.clone());
        return Err((task_id, err));
    }

    // 5. Build MSQ result envelope + complete the task.
    let signature: Vec<MsqColumnSignature> = signature_for_publish
        .columns
        .iter()
        .zip(signature_for_publish.types.iter())
        .map(|(name, ty)| MsqColumnSignature {
            name: name.clone(),
            sql_type: ty.clone(),
        })
        .collect();
    let envelope = MsqResults {
        signature,
        results: Vec::new(), // INSERT returns no rows to caller.
    };

    // Stage reports.
    let mut msq_stages: Vec<MsqStage> = dist
        .stage_counters
        .iter()
        .map(|c| MsqStage {
            stage_number: c.stage_number,
            phase: "RESULTS_READY".to_owned(),
            worker_count: c.worker_count,
            input_row_count: c.rows_in,
            output_row_count: c.rows_out,
            shuffle_type: c.shuffle_type.clone(),
        })
        .collect();
    // Append a synthetic insert stage so the report records the publish.
    let insert_stage_no = dist.stage_counters.len();
    msq_stages.push(MsqStage {
        stage_number: insert_stage_no,
        phase: "RESULTS_READY".to_owned(),
        worker_count: 1,
        input_row_count: dist.rows.len() as u64,
        output_row_count: dist.rows.len() as u64,
        shuffle_type: Some("INSERT".to_owned()),
    });

    if let Err(e) = manager.complete_task(&task_id, envelope) {
        let err = mk_err("MsqCompleteError", e.to_string());
        let _ = manager.fail_task(&task_id, err.clone());
        return Err((task_id, err));
    }
    let _ = manager.update_stages(&task_id, msq_stages);

    let _ = rows_in; // currently informational only

    Ok(InsertOutcome {
        task_id,
        target_data_source: target,
        segment_id,
        rows_published: dist.rows.len() as u64,
        retry_count: dist.retry_count,
    })
}

/// Run a `SELECT` MSQ task across a worker fleet, returning the
/// collected result rows.  Source rows come from
/// `input` (callers compose this from `scan_data_source` for real
/// deep-storage queries, or build it inline for unit tests).
///
/// # Errors
///
/// Propagates planning, validation, and coordinator errors.
pub async fn run_select_distributed(
    plan: &ExecutionPlan,
    input: InputTable,
    fleet: WorkerFleet,
    cfg: CoordinatorConfig,
    task_id: &str,
) -> Result<DistributedResult> {
    let qdef = plan_to_query_definition(plan, &input.signature)?;
    MsqCoordinator::new(fleet, cfg)
        .run(task_id, &qdef, input.rows)
        .await
}

fn mk_err(code: &str, msg: String) -> MsqError {
    MsqError {
        error: code.to_owned(),
        error_message: msg,
    }
}

/// Guarantee the signature carries a `__time` column at index 0
/// (segments are required to have it per [`ferrodruid_deep_storage::Segment::parse_jsonl`]).
fn ensure_time_column(sig: &RowSignature) -> RowSignature {
    if sig.columns.iter().any(|c| c == "__time") {
        return sig.clone();
    }
    let mut columns = vec!["__time".to_owned()];
    columns.extend(sig.columns.iter().cloned());
    let mut types = vec!["BIGINT".to_owned()];
    types.extend(sig.types.iter().cloned());
    RowSignature { columns, types }
}

/// If the output signature gained a `__time` column relative to the
/// engine signature, splice the publish wall-clock into every row.
fn ensure_time_values(
    engine_sig: &RowSignature,
    publish_sig: &RowSignature,
    rows: &[Row],
) -> Vec<Row> {
    if engine_sig.columns == publish_sig.columns {
        return rows.to_vec();
    }
    let now = crate::segment_io::now_ms();
    rows.iter()
        .map(|row| {
            let mut out = Vec::with_capacity(publish_sig.columns.len());
            for col in &publish_sig.columns {
                if col == "__time" {
                    out.push(crate::engine::Value::Long(now));
                } else if let Some(i) = engine_sig.index_of(col) {
                    out.push(row.get(i).cloned().unwrap_or(crate::engine::Value::Null));
                } else {
                    out.push(crate::engine::Value::Null);
                }
            }
            out
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Value;
    use crate::worker::MsqWorker;
    use ferrodruid_deep_storage::InMemoryDeepStorage;

    async fn spawn_workers(n: usize) -> (WorkerFleet, Vec<crate::worker::WorkerHandle>) {
        let mut addrs = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..n {
            let w = MsqWorker::bind("127.0.0.1:0".parse().unwrap())
                .await
                .expect("bind");
            let h = w.start();
            addrs.push(h.addr);
            handles.push(h);
        }
        (WorkerFleet::new(addrs), handles)
    }

    async fn seed_source(ds: &InMemoryDeepStorage) {
        let sig = RowSignature {
            columns: vec!["__time".into(), "city".into(), "n".into()],
            types: vec!["BIGINT".into(), "VARCHAR".into(), "BIGINT".into()],
        };
        let rows = vec![
            vec![Value::Long(1), Value::Str("a".into()), Value::Long(1)],
            vec![Value::Long(2), Value::Str("b".into()), Value::Long(2)],
            vec![Value::Long(3), Value::Str("a".into()), Value::Long(3)],
            vec![Value::Long(4), Value::Str("b".into()), Value::Long(4)],
            vec![Value::Long(5), Value::Str("c".into()), Value::Long(5)],
            vec![Value::Long(6), Value::Str("a".into()), Value::Long(6)],
        ];
        publish_segment(ds, "src", "src_seg_0", &sig, &rows)
            .await
            .expect("seed");
    }

    #[tokio::test]
    async fn insert_distributed_three_workers_publishes_segment() {
        let raw = InMemoryDeepStorage::new();
        seed_source(&raw).await;
        let ds: Arc<dyn DeepStorage> = Arc::new(raw);

        let (fleet, handles) = spawn_workers(3).await;
        let mgr = MsqManager::new();
        let spec = MsqTaskSpec {
            query: "INSERT INTO tgt SELECT city, COUNT(*), SUM(n) FROM src GROUP BY city".into(),
            context: serde_json::Value::Null,
            parameters: vec![],
        };

        let outcome = submit_insert_distributed(
            spec,
            &mgr,
            fleet,
            CoordinatorConfig::default(),
            Arc::clone(&ds),
        )
        .await
        .expect("insert");

        assert_eq!(outcome.target_data_source, "tgt");
        assert!(outcome.segment_id.starts_with("msq_"));
        assert!(
            outcome.rows_published >= 3,
            "got {}",
            outcome.rows_published
        );

        // Read back via the historical scan path.
        let read_back = scan_data_source(ds.as_ref(), "tgt")
            .await
            .expect("scan back");
        assert!(read_back.rows.len() >= 3, "read_back = {read_back:?}");

        for h in handles {
            h.shutdown().await;
            h.abort();
        }
    }
}
