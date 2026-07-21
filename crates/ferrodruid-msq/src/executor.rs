// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! MSQ query executor — executes SQL-based queries as multi-stage tasks.
//!
//! This module implements a single-node MSQ execution engine that decomposes
//! SQL queries into a DAG of execution stages. Each stage can perform scanning,
//! aggregation, shuffling, sorting, or insertion.

use std::cmp::Ordering;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use ferrodruid_common::{DruidError, Result};
use ferrodruid_sql::parser::{OrderByExpr, Projection, SqlExpr};
use ferrodruid_sql::{DruidSqlStatement, SelectQuery, parse_druid_sql};

use crate::engine::{
    AggFn, EngineConfig, Processor, QueryDefinition, Row, RowSignature, ShuffleSpec,
    StageDefinition, Value,
};
use crate::{MsqColumnSignature, MsqError, MsqManager, MsqResults, MsqStage};

// ---------------------------------------------------------------------------
// Stage Types
// ---------------------------------------------------------------------------

/// The type of work performed by an execution stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StageType {
    /// Read from a datasource (scan).
    Scan {
        /// The datasource to scan.
        data_source: String,
        /// Optional filter applied during scan (native filter JSON).
        #[serde(skip_serializing_if = "Option::is_none")]
        filter: Option<serde_json::Value>,
    },
    /// Aggregate (groupBy, timeseries).
    Aggregate {
        /// Dimensions to group by.
        dimensions: Vec<String>,
        /// Aggregation specs (native aggregator JSON).
        aggregations: Vec<serde_json::Value>,
    },
    /// Shuffle (redistribute by key).
    Shuffle {
        /// Partition key columns.
        partition_key: Vec<String>,
        /// Target partition count.
        num_partitions: usize,
    },
    /// Sort and output.
    Sort {
        /// Columns to sort by.
        order_by: Vec<String>,
        /// Optional row limit.
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    /// Insert results into a datasource.
    Insert {
        /// Target datasource name.
        target_data_source: String,
        /// Whether to replace existing data (REPLACE INTO vs INSERT INTO).
        replace_existing: bool,
    },
}

// ---------------------------------------------------------------------------
// Execution Stage
// ---------------------------------------------------------------------------

/// A stage in the MSQ execution DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionStage {
    /// Zero-based stage number.
    pub stage_number: usize,
    /// The type of work this stage performs.
    pub stage_type: StageType,
    /// Indices of stages that must complete before this stage can start.
    pub input_stages: Vec<usize>,
    /// Number of workers assigned to this stage.
    pub worker_count: usize,
}

// ---------------------------------------------------------------------------
// Execution Plan
// ---------------------------------------------------------------------------

/// An execution plan consisting of a DAG of stages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionPlan {
    /// The ordered list of execution stages.
    pub stages: Vec<ExecutionStage>,
    /// Index of the final output stage.
    pub output_stage: usize,
    /// The original SQL that produced this plan.
    pub sql: String,
}

impl ExecutionPlan {
    /// Validate that the plan is internally consistent before execution.
    ///
    /// Wave 45-B closure of Wave 37B `msq` Medium #2 (Codex
    /// `executor.rs:238-285`): pre-fix `execute_msq` simply iterated
    /// `plan.stages` in vector order without inspecting `input_stages`,
    /// `stage_number`, or `output_stage`.  A malformed plan could
    /// reference a non-existent input or schedule a child before its
    /// parent and still report success.
    ///
    /// The checks are intentionally conservative — single-node execution
    /// expects each `stage_number == idx` and each `input_stages[i] < idx`
    /// (acyclicity is implied because every input must be a strictly
    /// earlier stage in the topological order).
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Query`] when:
    ///
    /// * `stages` is empty
    /// * any `stage_number` does not match its slice index
    /// * any `input_stages` entry is `>= stage_number` (forward reference
    ///   or self-loop, both of which break topological order)
    /// * `output_stage` is out of bounds
    pub fn validate(&self) -> Result<()> {
        if self.stages.is_empty() {
            return Err(DruidError::Query(
                "MSQ execution plan must contain at least one stage".to_owned(),
            ));
        }
        if self.output_stage >= self.stages.len() {
            return Err(DruidError::Query(format!(
                "MSQ outputStage index {} is out of bounds for {} stages",
                self.output_stage,
                self.stages.len()
            )));
        }
        for (idx, stage) in self.stages.iter().enumerate() {
            if stage.stage_number != idx {
                return Err(DruidError::Query(format!(
                    "MSQ stage at slice index {idx} has stageNumber {} (must be {idx} for \
                     single-node executor)",
                    stage.stage_number
                )));
            }
            for input in &stage.input_stages {
                if *input >= idx {
                    return Err(DruidError::Query(format!(
                        "MSQ stage {idx} references input stage {input} (must be strictly less \
                         than {idx} so the DAG is acyclic and topologically ordered)"
                    )));
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Plan generation
// ---------------------------------------------------------------------------

/// Plan an MSQ execution from a SQL statement.
///
/// Analyzes the SQL to determine whether it is an INSERT/REPLACE statement
/// or a SELECT query, then generates appropriate execution stages.
///
/// # Errors
///
/// Returns an error if the SQL cannot be parsed or is unsupported.
pub fn plan_msq(sql: &str) -> Result<ExecutionPlan> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // DD R39: the lightweight string-scan planner only models COUNT(*) and
    // SUM/MIN/MAX(<col>). Other aggregate forms were silently MIS-PLANNED
    // (AVG -> a COUNT fallback, COUNT(col)/COUNT(DISTINCT) -> COUNT(*), a window
    // `SUM(..) OVER (..)` -> a global aggregate), returning wrong results. Reject
    // them explicitly (fail closed) so callers get an honest "unsupported" error
    // rather than silently incorrect data.
    //
    // DD R40: both the reject scan and `extract_agg_specs` matched aggregate
    // calls with the literal `FUNC(` (no space), so `SUM (n)` (whitespace before
    // the paren) bypassed BOTH — the reject missed `AVG (n)`/`COUNT (col)`/
    // `COUNT (DISTINCT x)` and the spec extractor emitted nothing, leaving the
    // COUNT fallback to return counts instead of sums. Scan the
    // whitespace-collapsed form so detection is space-tolerant. The `upper`
    // string used for GROUP BY / column-offset logic is left untouched.
    reject_unsupported_aggregates(&collapse_ws_before_paren(&upper))?;

    if upper.starts_with("INSERT INTO") || upper.starts_with("REPLACE INTO") {
        plan_insert(trimmed, &upper)
    } else if upper.starts_with("SELECT") {
        plan_select(trimmed, &upper)
    } else {
        Err(DruidError::Query(format!(
            "MSQ only supports SELECT and INSERT/REPLACE INTO statements, got: {}",
            &trimmed[..trimmed.len().min(50)]
        )))
    }
}

/// Reject aggregate / window forms the lightweight MSQ string-scan planner does
/// not model, so they fail closed instead of returning silently-wrong results
/// (DD R39). Supported: `COUNT(*)`/`COUNT(1)`, `SUM`/`MIN`/`MAX(<col>)`.
fn reject_unsupported_aggregates(upper: &str) -> Result<()> {
    // Window functions: a `SUM(..) OVER (..)` without GROUP BY would otherwise be
    // mis-planned as a global aggregate that collapses the row set.
    if upper.contains(" OVER (") || upper.contains(" OVER(") {
        return Err(DruidError::Query(
            "MSQ does not support window functions (OVER ...); run it as a broker query".to_owned(),
        ));
    }
    // Aggregates the scanner does not recognize (it would silently drop them and
    // fall back to COUNT, or ignore them).
    for unsupported in [
        "AVG(",
        "STDDEV",
        "VARIANCE",
        "VAR_",
        "VARPOP",
        "VARSAMP",
        "MEDIAN(",
        "APPROX_",
        "EARLIEST(",
        "LATEST(",
    ] {
        if upper.contains(unsupported) {
            let label = unsupported.trim_end_matches('(');
            return Err(DruidError::Query(format!(
                "MSQ does not support the {label} aggregate; supported aggregates are \
                 COUNT(*), SUM(col), MIN(col), MAX(col)"
            )));
        }
    }
    // COUNT only supports COUNT(*) / COUNT(1): COUNT(<col>) (null-sensitive) and
    // COUNT(DISTINCT <col>) have different semantics the scanner cannot honour.
    let mut from = 0;
    while let Some(rel) = upper[from..].find("COUNT(") {
        let inner_start = from + rel + "COUNT(".len();
        let end = upper[inner_start..]
            .find(')')
            .map_or(upper.len(), |e| inner_start + e);
        let arg = upper[inner_start..end].trim();
        from = end;
        if !(arg == "*" || arg == "1" || arg.is_empty()) {
            return Err(DruidError::Query(format!(
                "MSQ COUNT supports only COUNT(*); COUNT({arg}) (including \
                 COUNT(DISTINCT ...)) is not supported"
            )));
        }
    }
    Ok(())
}

/// Plan an INSERT INTO / REPLACE INTO statement.
fn plan_insert(sql: &str, upper: &str) -> Result<ExecutionPlan> {
    let replace_existing = upper.starts_with("REPLACE INTO");

    // Extract target datasource (word after INTO).
    let target = extract_target_datasource(sql, upper)?;
    // Extract source datasource from the SELECT ... FROM portion.
    let source = extract_source_datasource(sql, upper)?;

    // Extract dimensions and aggregations from the SELECT clause (simplified).
    let (dimensions, aggregations) = extract_group_info(upper);

    let mut stages = Vec::new();

    // Stage 0: Scan source.
    stages.push(ExecutionStage {
        stage_number: 0,
        stage_type: StageType::Scan {
            data_source: source.clone(),
            filter: None,
        },
        input_stages: vec![],
        worker_count: 1,
    });

    // Stage 1: Aggregate (if GROUP BY present).
    if !dimensions.is_empty() || !aggregations.is_empty() {
        stages.push(ExecutionStage {
            stage_number: 1,
            stage_type: StageType::Aggregate {
                dimensions: dimensions.clone(),
                aggregations: aggregations.clone(),
            },
            input_stages: vec![0],
            worker_count: 1,
        });
    }

    let prev_stage = if stages.len() > 1 { 1 } else { 0 };

    // Stage N-1: Shuffle by __time for time-partitioned output.
    let shuffle_idx = stages.len();
    stages.push(ExecutionStage {
        stage_number: shuffle_idx,
        stage_type: StageType::Shuffle {
            partition_key: vec!["__time".to_owned()],
            num_partitions: 1,
        },
        input_stages: vec![prev_stage],
        worker_count: 1,
    });

    // Stage N: Insert into target.
    let insert_idx = stages.len();
    stages.push(ExecutionStage {
        stage_number: insert_idx,
        stage_type: StageType::Insert {
            target_data_source: target,
            replace_existing,
        },
        input_stages: vec![shuffle_idx],
        worker_count: 1,
    });

    Ok(ExecutionPlan {
        output_stage: insert_idx,
        stages,
        sql: sql.to_owned(),
    })
}

/// Plan a SELECT query.
fn plan_select(sql: &str, upper: &str) -> Result<ExecutionPlan> {
    // Extract source datasource.
    let source = extract_source_datasource(sql, upper)?;
    let (dimensions, aggregations) = extract_group_info(upper);
    let has_order = upper.contains("ORDER BY");
    let limit = extract_limit(upper);

    // DD R43 (Finding 5): lower the SQL WHERE clause into the scan stage filter
    // so the plan honestly records (and execution can apply) it, instead of
    // hard-coding `filter: None` and silently scanning every row. The filter is
    // built with the broker's own `convert_filter`, so there is no second SQL
    // parser.
    let scan_filter = lower_where_filter(sql, upper)?;

    let mut stages = Vec::new();

    // Stage 0: Scan.
    stages.push(ExecutionStage {
        stage_number: 0,
        stage_type: StageType::Scan {
            data_source: source,
            filter: scan_filter,
        },
        input_stages: vec![],
        worker_count: 1,
    });

    // Stage 1: Aggregate (if GROUP BY present).
    if !dimensions.is_empty() || !aggregations.is_empty() {
        stages.push(ExecutionStage {
            stage_number: 1,
            stage_type: StageType::Aggregate {
                dimensions,
                aggregations,
            },
            input_stages: vec![0],
            worker_count: 1,
        });
    }

    // Final stage: Sort (always present for SELECT output).
    let prev_stage = stages.len() - 1;
    let sort_idx = stages.len();
    let order_by = if has_order {
        extract_order_columns(upper)
    } else {
        vec![]
    };

    stages.push(ExecutionStage {
        stage_number: sort_idx,
        stage_type: StageType::Sort { order_by, limit },
        input_stages: vec![prev_stage],
        worker_count: 1,
    });

    Ok(ExecutionPlan {
        output_stage: sort_idx,
        stages,
        sql: sql.to_owned(),
    })
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Execute an MSQ plan (single-node implementation).
///
/// Executes stages in topological order, updates the task status in the
/// manager, and returns the collected results.
///
/// # Current Limitations
///
/// This is a single-node implementation that executes stages sequentially.
/// Real distributed execution (multi-worker, cross-node shuffle) is planned
/// for a future release.
///
/// # Errors
///
/// Returns an error if execution fails at any stage or if the task cannot
/// be found/updated in the manager.
pub async fn execute_msq(
    plan: &ExecutionPlan,
    manager: &MsqManager,
    task_id: &str,
) -> Result<MsqResults> {
    // Wave 45-B closure of Wave 37B `msq` Medium #2: reject malformed
    // plans before the executor mutates task state.
    plan.validate()?;

    // Execute stages in topological order (they are already ordered by stage_number).
    let mut stage_reports = Vec::new();

    for stage in &plan.stages {
        let report = execute_stage(stage)?;
        stage_reports.push(report);
    }

    // Build result based on the output stage type.
    let results = build_results(plan, &stage_reports);

    // Update manager with stages and mark complete.
    let msq_stages: Vec<MsqStage> = stage_reports
        .iter()
        .map(|sr| MsqStage {
            stage_number: sr.stage_number,
            phase: "RESULTS_READY".to_owned(),
            worker_count: sr.worker_count,
            input_row_count: sr.rows_in,
            output_row_count: sr.rows_out,
            shuffle_type: sr.shuffle_type.clone(),
        })
        .collect();

    // Update task stages via complete_task.
    manager
        .complete_task(task_id, results.clone())
        .map_err(|e| DruidError::Internal(format!("failed to update task {task_id}: {e}")))?;

    // Update stage details (best-effort via internal access).
    let _ = manager.update_stages(task_id, msq_stages);

    Ok(results)
}

/// Execute an MSQ plan, handling errors by marking the task as failed.
///
/// This is the top-level entry point for the REST handler. It calls
/// [`execute_msq`] and on failure updates the task status accordingly.
pub async fn execute_msq_task(
    plan: &ExecutionPlan,
    manager: &MsqManager,
    task_id: &str,
) -> std::result::Result<MsqResults, MsqError> {
    match execute_msq(plan, manager, task_id).await {
        Ok(results) => Ok(results),
        Err(e) => {
            let error = MsqError {
                error: "MsqExecutionError".to_owned(),
                error_message: e.to_string(),
            };
            let _ = manager.fail_task(task_id, error.clone());
            Err(error)
        }
    }
}

// ---------------------------------------------------------------------------
// Engine bridge — run the real multi-stage engine over supplied input rows
// ---------------------------------------------------------------------------

/// A column of the external input fed to the MSQ engine.
///
/// The SQL planner ([`plan_msq`]) only resolves *datasource names*; it has
/// no segment store wired in.  This crate therefore takes the input rows
/// explicitly so the real engine pipeline can be driven end-to-end.  An
/// [`InputTable`] is the caller-supplied data for the leaf scan.
#[derive(Debug, Clone)]
pub struct InputTable {
    /// Column signature of the input rows.
    pub signature: RowSignature,
    /// The input rows, aligned to `signature`.
    pub rows: Vec<Row>,
}

/// Translate a SQL [`ExecutionPlan`] into an engine [`QueryDefinition`]
/// over the given `input` signature.
///
/// Supported shapes:
///
/// * `SELECT … FROM …` (no GROUP BY) → `scan → shuffle(mix)`,
/// * `SELECT g…, agg… FROM … GROUP BY g…` → `scan → shuffle(hash by g) →
///   aggregate(group_by g, aggs)`.
///
/// The aggregations recognised are `COUNT(*)`, `SUM(col)`, `MIN(col)`,
/// `MAX(col)` (see [`AggFn`]); unrecognised aggregations cause a
/// [`DruidError::Query`].
///
/// # Errors
///
/// Returns [`DruidError::Query`] if the plan references columns absent
/// from `input.signature` or contains an unsupported aggregation.
pub fn plan_to_query_definition(
    plan: &ExecutionPlan,
    input: &RowSignature,
) -> Result<QueryDefinition> {
    // Locate the aggregate stage (if any) in the SQL plan.
    let agg_stage = plan.stages.iter().find_map(|s| match &s.stage_type {
        StageType::Aggregate {
            dimensions,
            aggregations,
        } => Some((dimensions.clone(), aggregations.clone())),
        _ => None,
    });

    let mut stages = Vec::new();

    // Stage 0: scan — project the full input signature.
    stages.push(StageDefinition {
        stage_number: 0,
        inputs: vec![],
        processor: Processor::Scan {
            project: input.columns.clone(),
        },
        signature: input.clone(),
        shuffle: ShuffleSpec::None,
    });

    match agg_stage {
        None => {
            // SELECT without GROUP BY: a single mix shuffle stage.
            stages.push(StageDefinition {
                stage_number: 1,
                inputs: vec![0],
                processor: Processor::Shuffle,
                signature: input.clone(),
                shuffle: ShuffleSpec::None,
            });
            Ok(QueryDefinition {
                stages,
                final_stage: 1,
            })
        }
        Some((dimensions, aggregations)) => {
            // Validate group columns exist.
            for d in &dimensions {
                if input.index_of(d).is_none() {
                    return Err(DruidError::Query(format!(
                        "GROUP BY column `{d}` not present in input signature"
                    )));
                }
            }
            let aggs = translate_aggregations(&aggregations, input)?;

            // Stage 1: shuffle (hash by the group key) — the SHUFFLE.
            stages.push(StageDefinition {
                stage_number: 1,
                inputs: vec![0],
                processor: Processor::Shuffle,
                signature: input.clone(),
                shuffle: ShuffleSpec::Hash {
                    key: dimensions.clone(),
                    partitions: 4,
                },
            });

            // Stage 2: aggregate.
            let mut out_pairs: Vec<(String, String)> = dimensions
                .iter()
                .map(|d| (d.clone(), "VARCHAR".to_owned()))
                .collect();
            for a in &aggs {
                out_pairs.push((a.output_name(), "BIGINT".to_owned()));
            }
            let out_sig = RowSignature {
                columns: out_pairs.iter().map(|(n, _)| n.clone()).collect(),
                types: out_pairs.iter().map(|(_, t)| t.clone()).collect(),
            };
            stages.push(StageDefinition {
                stage_number: 2,
                inputs: vec![1],
                processor: Processor::Aggregate {
                    group_by: dimensions,
                    aggs,
                },
                signature: out_sig,
                shuffle: ShuffleSpec::None,
            });
            Ok(QueryDefinition {
                stages,
                final_stage: 2,
            })
        }
    }
}

/// Translate native-aggregation JSON specs (as produced by [`plan_msq`])
/// into engine [`AggFn`]s.
fn translate_aggregations(
    aggregations: &[serde_json::Value],
    input: &RowSignature,
) -> Result<Vec<AggFn>> {
    let mut out = Vec::new();
    for agg in aggregations {
        // Prefer the explicit `func` discriminator emitted by
        // `extract_agg_specs`; fall back to the Druid `type` label so
        // hand-built specs still translate.
        let func = agg
            .get("func")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .or_else(|| {
                agg.get("type").and_then(|v| v.as_str()).map(|t| {
                    match t {
                        "count" => "count",
                        "longSum" | "doubleSum" | "floatSum" => "sum",
                        "longMin" | "doubleMin" | "floatMin" => "min",
                        "longMax" | "doubleMax" | "floatMax" => "max",
                        _ => t,
                    }
                    .to_owned()
                })
            })
            .unwrap_or_default();
        let field = agg.get("fieldName").and_then(|v| v.as_str());

        if func == "count" {
            out.push(AggFn::Count);
            continue;
        }
        // Field-based aggregations require a resolved, present field.
        let field = field.ok_or_else(|| {
            DruidError::Query(format!("aggregation `{func}` is missing its source column"))
        })?;
        if input.index_of(field).is_none() {
            return Err(DruidError::Query(format!(
                "aggregation field `{field}` not present in input signature"
            )));
        }
        match func.as_str() {
            "sum" => out.push(AggFn::LongSum {
                field: field.to_owned(),
            }),
            "min" => out.push(AggFn::LongMin {
                field: field.to_owned(),
            }),
            "max" => out.push(AggFn::LongMax {
                field: field.to_owned(),
            }),
            other => {
                return Err(DruidError::Query(format!(
                    "unsupported MSQ aggregation `{other}`"
                )));
            }
        }
    }
    if out.is_empty() {
        // A GROUP BY with only dimensions still needs a deterministic
        // aggregate; emit COUNT so each group reports a row count.
        out.push(AggFn::Count);
    }
    Ok(out)
}

/// Execute a SQL plan through the **real multi-stage engine** over the
/// supplied input rows, populating the task report.
///
/// Unlike [`execute_msq`] (which produces an empty result set because no
/// segment store is wired in), this drives the full
/// scan → shuffle → aggregate pipeline with real partitioning, spill, and
/// multi-worker execution, then records per-stage counters into the MSQ
/// report.
///
/// # Errors
///
/// Returns an error if planning, validation, or engine execution fails, or
/// if the task cannot be completed in the manager.
pub async fn execute_msq_with_input(
    plan: &ExecutionPlan,
    input: InputTable,
    config: &EngineConfig,
    manager: &MsqManager,
    task_id: &str,
) -> Result<MsqResults> {
    plan.validate()?;

    // DD R43 (Finding 5): `plan_msq` accepts WHERE / ORDER BY / LIMIT and a
    // column projection, but execution previously ignored all of them — the
    // scan filter was hard-coded `None`, the engine scan cloned every input
    // column, and `plan_to_query_definition` dropped the Sort stage. We re-read
    // the original SQL with the real Druid parser (no second parser is
    // invented) and apply each clause around the engine run:
    //   * WHERE  -> filter the raw input rows before aggregation,
    //   * ORDER BY + LIMIT + projection -> applied to the engine output.
    // A statement the engine path cannot lower (e.g. one the plan recorded a
    // filter / order / limit for, but which does not parse as a plain SELECT)
    // fails closed instead of returning unfiltered / unordered rows.
    let select = match parse_druid_sql(&plan.sql) {
        Ok(DruidSqlStatement::Select(select)) => Some(*select),
        _ => None,
    };
    if select.is_none() && plan_has_unapplied_clauses(plan) {
        return Err(DruidError::Query(
            "MSQ cannot execute this query through the engine: its WHERE / ORDER BY / LIMIT \
             could not be lowered (only plain SELECT statements are supported here)"
                .to_owned(),
        ));
    }

    // 1. WHERE — filter the input rows before they reach the engine.
    let input = if let Some(ref select) = select {
        apply_where_filter(input, select)?
    } else {
        input
    };

    let qdef = plan_to_query_definition(plan, &input.signature)?;
    let engine_result = qdef.run(input.rows, config).await?;

    let mut out_signature = engine_result.signature;
    let mut out_rows = engine_result.rows;

    // 2. ORDER BY, 3. LIMIT, 4. projection — applied to the engine output.
    if let Some(ref select) = select {
        // DD R44 (Finding 2): resolve the SELECT projections (which carry the
        // user's `AS <alias>`) against the engine output signature ONCE, so the
        // OUTPUT column names (aliases) drive both ORDER BY resolution and the
        // final projection. Without this, aggregates were named with the
        // synthetic `count` / `sum_<field>` and plain columns with the source
        // name, so `ORDER BY <alias>` failed closed and the result mislabeled
        // the columns with the engine names instead of the requested aliases.
        let projection_plan = resolve_projection_plan(select)?;
        apply_order_by(
            &mut out_rows,
            &out_signature,
            &select.order_by,
            projection_plan.as_deref(),
        )?;
        if let Some(limit) = select.limit {
            out_rows.truncate(limit);
        }
        apply_projection(
            &mut out_signature,
            &mut out_rows,
            projection_plan.as_deref(),
        )?;
    }

    // Build the MSQ result envelope from the (filtered/ordered/projected) output.
    let signature: Vec<MsqColumnSignature> = out_signature
        .columns
        .iter()
        .zip(out_signature.types.iter())
        .map(|(name, ty)| MsqColumnSignature {
            name: name.clone(),
            sql_type: ty.clone(),
        })
        .collect();

    let results: Vec<serde_json::Value> = out_rows
        .iter()
        .map(|row| row_to_json(&out_signature, row))
        .collect();

    let msq_results = MsqResults { signature, results };

    // Per-stage report from engine counters.
    let msq_stages: Vec<MsqStage> = engine_result
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

    manager
        .complete_task(task_id, msq_results.clone())
        .map_err(|e| DruidError::Internal(format!("failed to complete task {task_id}: {e}")))?;
    let _ = manager.update_stages(task_id, msq_stages);

    Ok(msq_results)
}

/// Map an engine row to a JSON object keyed by the signature column names.
fn row_to_json(signature: &RowSignature, row: &Row) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (i, name) in signature.columns.iter().enumerate() {
        let val = row.get(i).map_or(serde_json::Value::Null, Value::to_json);
        obj.insert(name.clone(), val);
    }
    serde_json::Value::Object(obj)
}

// ---------------------------------------------------------------------------
// DD R43 (Finding 5): SQL clause lowering helpers
// ---------------------------------------------------------------------------

/// Lower a SQL `WHERE` clause into a native filter JSON for the scan stage.
///
/// Returns `Ok(None)` when there is no top-level `WHERE`. Returns
/// [`DruidError::Query`] when the SQL contains a `WHERE` keyword that does not
/// resolve to a lowerable filter (a plain SELECT with a convertible predicate),
/// so the query fails closed rather than scanning every row unfiltered.
fn lower_where_filter(sql: &str, upper: &str) -> Result<Option<serde_json::Value>> {
    if !upper.contains("WHERE") {
        return Ok(None);
    }
    let Ok(DruidSqlStatement::Select(select)) = parse_druid_sql(sql) else {
        return Err(DruidError::Query(
            "MSQ could not parse the WHERE clause into a filter; only plain SELECT statements \
             with a supported predicate are accepted"
                .to_owned(),
        ));
    };
    match &select.filter {
        // The `WHERE` keyword was inside a string/identifier — no real filter.
        None => Ok(None),
        Some(expr) => {
            let filter = ferrodruid_sql::planner::convert_filter(expr)?;
            let json = serde_json::to_value(&filter).map_err(|e| {
                DruidError::Internal(format!("failed to serialize MSQ scan filter: {e}"))
            })?;
            Ok(Some(json))
        }
    }
}

/// Whether `plan` recorded a WHERE / ORDER BY / LIMIT that execution would have
/// to apply. Used to fail closed when the SQL cannot be re-parsed as a plain
/// SELECT (so those clauses cannot be lowered) instead of silently ignoring
/// them.
fn plan_has_unapplied_clauses(plan: &ExecutionPlan) -> bool {
    plan.stages.iter().any(|s| match &s.stage_type {
        StageType::Scan { filter, .. } => filter.is_some(),
        StageType::Sort { order_by, limit } => !order_by.is_empty() || limit.is_some(),
        _ => false,
    })
}

/// Apply the SQL `WHERE` filter to the raw input rows (before aggregation).
fn apply_where_filter(input: InputTable, select: &SelectQuery) -> Result<InputTable> {
    let Some(expr) = select.filter.as_ref() else {
        return Ok(input);
    };
    let filter = ferrodruid_sql::planner::convert_filter(expr)?;
    let InputTable { signature, rows } = input;
    let kept: Vec<Row> = rows
        .into_iter()
        .filter(|row| filter.matches(&row_to_map(&signature, row)))
        .collect();
    Ok(InputTable {
        signature,
        rows: kept,
    })
}

/// Build a `column -> JSON value` map for one row so it can be matched against
/// a [`FilterSpec`].
fn row_to_map(signature: &RowSignature, row: &Row) -> HashMap<String, serde_json::Value> {
    signature
        .columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let val = row.get(i).map_or(serde_json::Value::Null, Value::to_json);
            (name.clone(), val)
        })
        .collect()
}

/// Apply `ORDER BY` to the engine output rows. Every order key must reference
/// an output column by name; an order key that does not (e.g. an aggregate
/// expression or unknown column) fails closed rather than being ignored.
///
/// DD R44 (Finding 2): the order key is resolved against the SELECT OUTPUT
/// names (the aliases recorded in `projection_plan`) FIRST, then mapped to the
/// engine column that actually holds the data. This makes `ORDER BY cnt` sort
/// by the aggregate output as `cnt` even though the engine names it `count`. A
/// key that matches neither an alias nor an engine column still fails closed
/// (the R43 behavior).
fn apply_order_by(
    rows: &mut [Row],
    signature: &RowSignature,
    order_by: &[OrderByExpr],
    projection_plan: Option<&[ProjectionItem]>,
) -> Result<()> {
    if order_by.is_empty() {
        return Ok(());
    }
    // Resolve each order key to a (column index, ascending) pair.
    let mut keys: Vec<(usize, bool)> = Vec::with_capacity(order_by.len());
    for ob in order_by {
        let SqlExpr::Column(name) = &ob.expr else {
            return Err(DruidError::Query(
                "MSQ ORDER BY supports only output column references; expression / aggregate \
                 ordering is not supported"
                    .to_owned(),
            ));
        };
        // DD R44: map an output alias to its engine source column; fall back to
        // the name as-is so a direct engine-column reference still resolves.
        let engine_col = projection_plan
            .and_then(|plan| {
                plan.iter()
                    .find(|item| item.output == *name)
                    .map(|item| item.source.as_str())
            })
            .unwrap_or(name.as_str());
        let idx = signature.index_of(engine_col).ok_or_else(|| {
            DruidError::Query(format!(
                "MSQ ORDER BY column `{name}` is not an output column of the query"
            ))
        })?;
        keys.push((idx, ob.asc));
    }
    rows.sort_by(|a, b| {
        for &(idx, asc) in &keys {
            let va = a.get(idx).unwrap_or(&Value::Null);
            let vb = b.get(idx).unwrap_or(&Value::Null);
            let ord = compare_values(va, vb);
            if ord != Ordering::Equal {
                return if asc { ord } else { ord.reverse() };
            }
        }
        Ordering::Equal
    });
    Ok(())
}

/// Total ordering over engine [`Value`]s for `ORDER BY`. Null sorts first;
/// numbers compare numerically; strings lexicographically; mixed types order by
/// a fixed type rank so the comparison is total.
fn compare_values(a: &Value, b: &Value) -> Ordering {
    fn rank(v: &Value) -> u8 {
        match v {
            Value::Null => 0,
            Value::Long(_) | Value::Double(_) => 1,
            Value::Str(_) => 2,
        }
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Long(x), Value::Long(y)) => x.cmp(y),
        (Value::Str(x), Value::Str(y)) => x.cmp(y),
        (Value::Long(_) | Value::Double(_), Value::Long(_) | Value::Double(_)) => {
            a.as_double().total_cmp(&b.as_double())
        }
        _ => rank(a).cmp(&rank(b)),
    }
}

/// DD R44 (Finding 2): one resolved SELECT projection — the engine column that
/// holds the data (`source`) and the OUTPUT name the user should see
/// (`output`, the `AS <alias>` when present, otherwise the engine name).
struct ProjectionItem {
    /// The engine output-signature column the data comes from.
    source: String,
    /// The user-visible output column name (alias if any).
    output: String,
}

/// Resolve the SELECT projection list into a `(source, output)` plan over the
/// engine output `signature`, honoring `AS <alias>` (DD R44, Finding 2).
///
/// Returns `Ok(None)` when the engine output should be left untouched: a
/// `SELECT *` (wildcard) or a projection containing a computed expression this
/// lightweight path does not model (matching the pre-DD-R44 pass-through). When
/// a plan is returned, each item maps an existing engine column to the alias
/// the user requested:
///
/// * a bare column `page` / `page AS p` → source `page`, output `page` / `p`;
/// * an aggregate `COUNT(*) AS cnt` / `SUM(x) AS s` → source is the synthetic
///   engine name (`count` / `sum_x`), output is the alias (or the synthetic
///   name when none is given).
///
/// # Errors
///
/// Fails closed ([`DruidError::Query`]) when an aggregate projection cannot be
/// mapped to a recognized engine column, rather than silently mislabeling or
/// dropping it. (Unsupported aggregate forms are already rejected by
/// [`reject_unsupported_aggregates`] at plan time, so this is a defensive
/// boundary.)
fn resolve_projection_plan(select: &SelectQuery) -> Result<Option<Vec<ProjectionItem>>> {
    let mut plan: Vec<ProjectionItem> = Vec::with_capacity(select.projections.len());
    for p in &select.projections {
        match p {
            // `SELECT *` keeps the full engine output unchanged.
            Projection::Wildcard => return Ok(None),
            Projection::Expr { expr, alias } => {
                let source = match expr {
                    SqlExpr::Column(name) => name.clone(),
                    SqlExpr::Aggregate {
                        func,
                        arg,
                        distinct,
                    } => agg_engine_column(func, arg.as_deref(), *distinct)?,
                    // A computed expression (function / arithmetic / cast / …):
                    // this path does not evaluate it, so leave the engine output
                    // untouched (pre-DD-R44 pass-through behavior).
                    _ => return Ok(None),
                };
                let output = alias.clone().unwrap_or_else(|| source.clone());
                // DD R45: the result is an object keyed by output name, so two
                // projections sharing an output name (e.g. `page AS x, channel AS
                // x`) would silently collapse — the second overwrites the first in
                // `row_to_json`, and ORDER BY on the name binds ambiguously. Reject
                // duplicate output names rather than return mislabeled data.
                if plan.iter().any(|item| item.output == output) {
                    return Err(DruidError::Query(format!(
                        "duplicate output column name `{output}` in the SELECT list"
                    )));
                }
                plan.push(ProjectionItem { source, output });
            }
        }
    }
    if plan.is_empty() {
        return Ok(None);
    }
    Ok(Some(plan))
}

/// Compute the synthetic engine output-column name for an aggregate projection,
/// mirroring [`extract_agg_specs`] / [`AggFn::output_name`] so the alias can be
/// attached to the right engine column (DD R44, Finding 2).
///
/// # Errors
///
/// Fails closed for any aggregate form not modeled by the engine bridge
/// (`COUNT(*)`, `SUM`/`MIN`/`MAX(<col>)`).
fn agg_engine_column(func: &str, arg: Option<&SqlExpr>, distinct: bool) -> Result<String> {
    let lower = func.to_lowercase();
    match lower.as_str() {
        "count" if !distinct && matches!(arg, None | Some(SqlExpr::Star)) => Ok("count".to_owned()),
        "sum" | "min" | "max" if !distinct => match arg {
            // `extract_agg_specs` lower-cases the field, so mirror that here.
            Some(SqlExpr::Column(field)) => Ok(format!("{lower}_{}", field.to_lowercase())),
            _ => Err(DruidError::Query(format!(
                "MSQ cannot alias the {func} aggregate over this argument; only \
                 {func}(<column>) is supported"
            ))),
        },
        _ => Err(DruidError::Query(format!(
            "MSQ cannot alias the unsupported aggregate `{func}`"
        ))),
    }
}

/// Project / rename the engine output to the SELECT output columns.
///
/// `plan` is the `(source, output)` mapping from [`resolve_projection_plan`].
/// `None` leaves the engine output unchanged (a `SELECT *` or an unmodeled
/// expression projection). Each `source` must exist in the output signature;
/// otherwise the query fails closed (DD R44, Finding 2).
fn apply_projection(
    signature: &mut RowSignature,
    rows: &mut [Row],
    plan: Option<&[ProjectionItem]>,
) -> Result<()> {
    let Some(plan) = plan else {
        return Ok(());
    };

    // Resolve each projected source column to an index in the current
    // signature, collecting the (output name, type) of the result column.
    let mut indices = Vec::with_capacity(plan.len());
    let mut columns = Vec::with_capacity(plan.len());
    let mut types = Vec::with_capacity(plan.len());
    for item in plan {
        let idx = signature.index_of(&item.source).ok_or_else(|| {
            DruidError::Query(format!(
                "SELECT column `{}` is not present in the query output",
                item.source
            ))
        })?;
        indices.push(idx);
        types.push(signature.types[idx].clone());
        columns.push(item.output.clone());
    }

    // Identity projection (same order AND same names) — nothing to do.
    if indices.iter().copied().eq(0..signature.columns.len()) && columns == signature.columns {
        return Ok(());
    }

    for row in rows.iter_mut() {
        *row = indices.iter().map(|&i| row[i].clone()).collect();
    }
    *signature = RowSignature { columns, types };
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Per-stage execution result (internal).
struct StageResult {
    stage_number: usize,
    worker_count: usize,
    rows_in: u64,
    rows_out: u64,
    shuffle_type: Option<String>,
}

/// Execute a single stage (single-node, in-process).
fn execute_stage(stage: &ExecutionStage) -> Result<StageResult> {
    let shuffle_type = match &stage.stage_type {
        StageType::Scan { .. } => None,
        StageType::Aggregate { .. } => None,
        StageType::Shuffle { .. } => Some("HASH".to_owned()),
        StageType::Sort { .. } => Some("MIX".to_owned()),
        StageType::Insert { .. } => None,
    };

    // In the single-node implementation, stages are logical markers.
    // Real execution would read segment data, apply filters, aggregate, etc.
    // For now we produce an empty result set that proves the DAG executes.
    Ok(StageResult {
        stage_number: stage.stage_number,
        worker_count: stage.worker_count,
        rows_in: 0,
        rows_out: 0,
        shuffle_type,
    })
}

/// Build final results from stage execution.
///
/// The output signature of the plan is the signature **propagated through
/// the DAG**, not just the local stage type.  In particular, when the
/// output stage is a `Sort` (typical for `SELECT … FROM … GROUP BY …`),
/// the sort merely re-orders rows; the schema is whatever its input
/// stage produces.  Wave 36-G1 (Wave 37B msq High #1): previously this
/// helper always reported `__time:TIMESTAMP` for `Sort | Scan` outputs,
/// which silently lied about the column metadata of every aggregated
/// SELECT result.  This walks the input chain to find the schema-
/// defining ancestor (Aggregate / Insert / Shuffle / raw Scan) before
/// formatting the signature.
fn build_results(plan: &ExecutionPlan, _stage_reports: &[StageResult]) -> MsqResults {
    let signature = signature_for_stage(plan, plan.output_stage);

    // In single-node mode, return empty results (no real data processed).
    MsqResults {
        signature,
        results: vec![],
    }
}

/// Resolve the column signature contributed by the given stage by
/// walking the DAG towards the schema-defining ancestor when the local
/// stage merely permutes / partitions rows.
fn signature_for_stage(plan: &ExecutionPlan, stage_idx: usize) -> Vec<MsqColumnSignature> {
    // Defensive: out-of-range stage_idx should never happen in a
    // well-formed plan, but if it does fall back to the legacy `__time`
    // schema rather than panicking.
    let Some(stage) = plan.stages.get(stage_idx) else {
        return vec![MsqColumnSignature {
            name: "__time".to_owned(),
            sql_type: "TIMESTAMP".to_owned(),
        }];
    };

    match &stage.stage_type {
        StageType::Aggregate {
            dimensions,
            aggregations,
        } => {
            let mut sig: Vec<MsqColumnSignature> = dimensions
                .iter()
                .map(|d| MsqColumnSignature {
                    name: d.clone(),
                    sql_type: "VARCHAR".to_owned(),
                })
                .collect();
            for agg in aggregations {
                if let Some(name) = agg.get("name").and_then(|n| n.as_str()) {
                    sig.push(MsqColumnSignature {
                        name: name.to_owned(),
                        sql_type: "BIGINT".to_owned(),
                    });
                }
            }
            if sig.is_empty() {
                sig.push(MsqColumnSignature {
                    name: "EXPR$0".to_owned(),
                    sql_type: "BIGINT".to_owned(),
                });
            }
            sig
        }
        StageType::Insert { .. } => vec![
            MsqColumnSignature {
                name: "rows_inserted".to_owned(),
                sql_type: "BIGINT".to_owned(),
            },
            MsqColumnSignature {
                name: "target".to_owned(),
                sql_type: "VARCHAR".to_owned(),
            },
        ],
        // Sort and Shuffle are pure permutations / repartitions — the
        // result schema is whatever their (single) upstream stage emits.
        // Walk towards that stage.  If the chain root is a bare Scan
        // (i.e. `SELECT * FROM …` with no aggregation) keep the
        // single-node placeholder schema so existing behaviour is
        // preserved when there really is no GROUP BY.
        StageType::Sort { .. } | StageType::Shuffle { .. } => {
            if let Some(&parent) = stage.input_stages.first() {
                signature_for_stage(plan, parent)
            } else {
                vec![MsqColumnSignature {
                    name: "__time".to_owned(),
                    sql_type: "TIMESTAMP".to_owned(),
                }]
            }
        }
        StageType::Scan { .. } => vec![MsqColumnSignature {
            name: "__time".to_owned(),
            sql_type: "TIMESTAMP".to_owned(),
        }],
    }
}

// ---------------------------------------------------------------------------
// SQL parsing helpers (lightweight, avoids pulling in full parser dep)
// ---------------------------------------------------------------------------

/// Extract the target datasource from INSERT INTO <ds> or REPLACE INTO <ds>.
///
/// Wave 45-B closure of Wave 37B `msq` Medium #5 (Codex
/// `executor.rs:89-98,102-111,416-461`): the previous implementation
/// uppercased the entire SQL then lowercased the extracted token, which
/// silently rewrote mixed-case identifiers like `MyDataSource`.  We
/// keep the byte offset found in `upper` (which is byte-for-byte aligned
/// with `original` because [`str::to_uppercase`] only changes ASCII
/// alphabetics 1:1 in this context) and slice from `original` so the
/// caller-supplied case survives.  Identifiers that contain non-ASCII
/// characters (which can up-case to a different byte length) are
/// rejected because we cannot guarantee offset alignment for those.
fn extract_target_datasource(original: &str, upper: &str) -> Result<String> {
    // Pattern: "INSERT INTO <name>" or "REPLACE INTO <name>"
    let into_pos = upper
        .find("INTO")
        .ok_or_else(|| DruidError::Query("Missing INTO clause".to_owned()))?;

    let slice_start = into_pos + 4;
    if !ascii_aligned(original, upper, slice_start) {
        return Err(DruidError::Query(
            "MSQ INSERT/REPLACE INTO target identifier contains non-ASCII characters; \
             quoted identifiers are not yet supported"
                .to_owned(),
        ));
    }

    let after_in_upper = &upper[slice_start..];
    let lead_ws = after_in_upper.len() - after_in_upper.trim_start().len();
    let after_in_original = &original[slice_start + lead_ws..];

    let end = after_in_original
        .find(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .unwrap_or(after_in_original.len());

    let name = after_in_original[..end].trim().to_owned();
    if name.is_empty() {
        return Err(DruidError::Query(
            "Missing target datasource after INTO".to_owned(),
        ));
    }
    Ok(name)
}

/// Extract the source datasource from a FROM clause.
///
/// See [`extract_target_datasource`] for the case-preservation rationale
/// (Wave 45-B closure of Wave 37B `msq` Medium #5).
fn extract_source_datasource(original: &str, upper: &str) -> Result<String> {
    let from_pos = upper
        .find(" FROM ")
        .ok_or_else(|| DruidError::Query("Missing FROM clause".to_owned()))?;

    let slice_start = from_pos + 6;
    if !ascii_aligned(original, upper, slice_start) {
        return Err(DruidError::Query(
            "MSQ FROM source identifier contains non-ASCII characters; \
             quoted identifiers are not yet supported"
                .to_owned(),
        ));
    }

    let after_in_upper = &upper[slice_start..];
    let lead_ws = after_in_upper.len() - after_in_upper.trim_start().len();
    let after_in_original = &original[slice_start + lead_ws..];

    let end = after_in_original
        .find(|c: char| c.is_whitespace() || c == ';' || c == ')')
        .unwrap_or(after_in_original.len());

    let name = after_in_original[..end].trim().to_owned();
    if name.is_empty() {
        return Err(DruidError::Query(
            "Missing datasource after FROM".to_owned(),
        ));
    }
    Ok(name)
}

/// Returns `true` if `original[..up_to]` and `upper[..up_to]` are
/// guaranteed to occupy identical byte offsets.  This holds when the
/// prefix is pure ASCII (every ASCII byte uppercases to a single ASCII
/// byte; non-ASCII characters can up-case to a different byte length
/// and would shift offsets).
fn ascii_aligned(original: &str, upper: &str, up_to: usize) -> bool {
    let stop = up_to.min(original.len()).min(upper.len());
    original.as_bytes()[..stop].is_ascii() && upper.as_bytes()[..stop].is_ascii()
}

/// Extract GROUP BY dimensions (simplified).
fn extract_group_info(upper: &str) -> (Vec<String>, Vec<serde_json::Value>) {
    let dimensions = if let Some(pos) = upper.find("GROUP BY") {
        let after = &upper[pos + 8..];

        // Find the end of GROUP BY clause (before HAVING, ORDER BY, LIMIT).
        let clause_end = ["HAVING", "ORDER BY", "LIMIT"]
            .iter()
            .filter_map(|kw| after.find(kw))
            .min()
            .unwrap_or(after.len());

        after[..clause_end]
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        vec![]
    };

    // Extract aggregate function native specs from the SELECT clause.
    // DD R40: scan the whitespace-collapsed form so `SUM (n)` (space before the
    // paren) is recognised rather than silently dropped (which let the COUNT
    // fallback in `translate_aggregations` return counts instead of sums). Only
    // the agg scan is collapsed; GROUP BY above still uses `upper`.
    let aggregations = extract_agg_specs(&collapse_ws_before_paren(upper));

    (dimensions, aggregations)
}

/// Collapse any run of ASCII whitespace that appears immediately before a `(`.
///
/// DD R40: the lightweight aggregate scanners ([`reject_unsupported_aggregates`]
/// and [`extract_agg_specs`]) match function calls by the literal `FUNC(`
/// prefix, so `SUM (n)` / `AVG (n)` / `COUNT (DISTINCT x)` (a space before the
/// paren) slipped past both. Running them over the collapsed string makes
/// aggregate detection whitespace-tolerant. `SUM(added) OVER (PARTITION …)`
/// collapses to `… OVER(PARTITION …)`, which the window check still catches via
/// its `" OVER("` variant.
///
/// Only the strings those two functions scan are collapsed; the `upper` string
/// used for GROUP BY parsing and datasource byte-offset alignment is left
/// untouched.
fn collapse_ws_before_paren(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '(' {
            while out.ends_with(|ch: char| ch.is_ascii_whitespace()) {
                out.pop();
            }
        }
        out.push(c);
    }
    out
}

/// Extract aggregate functions from the SELECT list as native aggregation
/// JSON specs.
///
/// Each spec carries:
///
/// * `"func"` — the lower-cased function name (`count` / `sum` / `min` /
///   `max`),
/// * `"name"` — the output column name (`count` for `COUNT(*)`, otherwise
///   `<func>_<field>`),
/// * `"fieldName"` — the *argument column* parsed from inside the
///   parentheses (`null` for `COUNT(*)`),
/// * `"type"` — a Druid-style aggregator type for report compatibility.
///
/// The argument column is captured so the engine bridge can resolve the
/// real source column rather than collapsing `SUM(added)` to a synthetic
/// `sum` field.  Parsing is a lightweight scan (no full SQL parser): it
/// finds EVERY occurrence of each function (DD R38 — a single `find` per kind
/// silently dropped repeated aggregates such as `SUM(added), SUM(deleted)`) and
/// reads the token up to the matching `)`. Specs are de-duplicated by output
/// name so a repeated identical aggregate yields one column.
fn extract_agg_specs(upper: &str) -> Vec<serde_json::Value> {
    let mut aggs = Vec::new();
    let mut seen_names = std::collections::HashSet::new();
    // (uppercase prefix, lower-case func, Druid type label)
    let agg_funcs = [
        ("COUNT(", "count", "count"),
        ("SUM(", "sum", "longSum"),
        ("MIN(", "min", "longMin"),
        ("MAX(", "max", "longMax"),
    ];

    for (prefix, func, dtype) in &agg_funcs {
        // DD R38: scan ALL occurrences of this function, not just the first.
        let mut search_from = 0;
        while let Some(rel) = upper[search_from..].find(prefix) {
            let pos = search_from + rel;
            let inner_start = pos + prefix.len();
            let inner = &upper[inner_start..];
            let end = inner.find(')').unwrap_or(inner.len());
            let arg = inner[..end].trim();
            // Advance past this occurrence so the next iteration finds the next.
            search_from = inner_start + end;

            // `COUNT(*)` / `COUNT(1)` have no real source column.
            let field: Option<String> = if *func == "count" || arg == "*" || arg.is_empty() {
                None
            } else {
                Some(arg.to_lowercase())
            };
            let name = match &field {
                Some(f) => format!("{func}_{f}"),
                None => "count".to_owned(),
            };
            // De-dup by output name so a repeated identical aggregate (or
            // multiple `COUNT(*)`) does not emit colliding columns.
            if !seen_names.insert(name.clone()) {
                continue;
            }
            aggs.push(serde_json::json!({
                "type": dtype,
                "func": func,
                "name": name,
                "fieldName": field,
            }));
        }
    }
    aggs
}

/// Extract LIMIT value.
fn extract_limit(upper: &str) -> Option<usize> {
    if let Some(pos) = upper.rfind("LIMIT") {
        let after = upper[pos + 5..].trim_start();
        let end = after
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after.len());
        after[..end].parse().ok()
    } else {
        None
    }
}

/// Extract ORDER BY column names.
fn extract_order_columns(upper: &str) -> Vec<String> {
    if let Some(pos) = upper.find("ORDER BY") {
        let after = &upper[pos + 8..];
        let end = ["LIMIT", ";"]
            .iter()
            .filter_map(|kw| after.find(kw))
            .min()
            .unwrap_or(after.len());

        after[..end]
            .split(',')
            .map(|s| s.split_whitespace().next().unwrap_or("").to_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        vec![]
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_simple_select() {
        let plan = plan_msq("SELECT __time, page FROM wiki").expect("plan");
        assert_eq!(plan.stages.len(), 2); // Scan + Sort
        assert_eq!(plan.output_stage, 1);
        assert_eq!(plan.sql, "SELECT __time, page FROM wiki");

        // Stage 0 is Scan.
        match &plan.stages[0].stage_type {
            StageType::Scan {
                data_source,
                filter,
            } => {
                assert_eq!(data_source, "wiki");
                assert!(filter.is_none());
            }
            _ => panic!("expected Scan stage"),
        }

        // Stage 1 is Sort.
        match &plan.stages[1].stage_type {
            StageType::Sort { order_by, limit } => {
                assert!(order_by.is_empty());
                assert!(limit.is_none());
            }
            _ => panic!("expected Sort stage"),
        }
    }

    #[test]
    fn plan_select_with_group_by() {
        let plan = plan_msq(
            "SELECT channel, COUNT(*) FROM wiki GROUP BY channel ORDER BY channel LIMIT 10",
        )
        .expect("plan");

        // Scan + Aggregate + Sort.
        assert_eq!(plan.stages.len(), 3);
        assert_eq!(plan.output_stage, 2);

        match &plan.stages[1].stage_type {
            StageType::Aggregate { dimensions, .. } => {
                assert_eq!(dimensions, &["channel"]);
            }
            _ => panic!("expected Aggregate stage"),
        }

        match &plan.stages[2].stage_type {
            StageType::Sort { order_by, limit } => {
                assert_eq!(order_by, &["channel"]);
                assert_eq!(*limit, Some(10));
            }
            _ => panic!("expected Sort stage"),
        }
    }

    #[test]
    fn plan_insert_into() {
        let plan =
            plan_msq("INSERT INTO wiki_agg SELECT channel, COUNT(*) FROM wiki GROUP BY channel")
                .expect("plan");

        // Scan + Aggregate + Shuffle + Insert.
        assert_eq!(plan.stages.len(), 4);
        assert_eq!(plan.output_stage, 3);

        match &plan.stages[3].stage_type {
            StageType::Insert {
                target_data_source,
                replace_existing,
            } => {
                assert_eq!(target_data_source, "wiki_agg");
                assert!(!replace_existing);
            }
            _ => panic!("expected Insert stage"),
        }
    }

    #[test]
    fn plan_replace_into() {
        let plan = plan_msq("REPLACE INTO target SELECT * FROM source").expect("plan");

        match &plan.stages.last().expect("last").stage_type {
            StageType::Insert {
                replace_existing, ..
            } => {
                assert!(*replace_existing);
            }
            _ => panic!("expected Insert stage"),
        }
    }

    #[test]
    fn plan_unsupported_sql() {
        let result = plan_msq("DROP TABLE wiki");
        assert!(result.is_err());
    }

    #[test]
    fn execution_plan_serde_roundtrip() {
        let plan = plan_msq("SELECT __time FROM wiki ORDER BY __time LIMIT 5").expect("plan");
        let json = serde_json::to_string(&plan).expect("serialize");
        let parsed: ExecutionPlan = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.stages.len(), plan.stages.len());
        assert_eq!(parsed.output_stage, plan.output_stage);
        assert_eq!(parsed.sql, plan.sql);
    }

    #[test]
    fn stage_dependency_ordering() {
        let plan = plan_msq("INSERT INTO out SELECT dim, SUM(val) FROM source GROUP BY dim")
            .expect("plan");

        // Verify topological ordering: each stage's input_stages refer to
        // earlier stages only.
        for stage in &plan.stages {
            for &dep in &stage.input_stages {
                assert!(
                    dep < stage.stage_number,
                    "stage {} depends on {} which is not earlier",
                    stage.stage_number,
                    dep
                );
            }
        }
    }

    #[tokio::test]
    async fn execute_simple_select() {
        let manager = MsqManager::new();
        let spec = crate::MsqTaskSpec {
            query: "SELECT __time FROM wiki".to_owned(),
            context: serde_json::Value::Null,
            parameters: vec![],
        };
        let task_id = manager.submit(spec).expect("submit");

        let plan = plan_msq("SELECT __time FROM wiki").expect("plan");
        let results = execute_msq(&plan, &manager, &task_id).await.expect("exec");

        // Results have a signature (columns).
        assert!(!results.signature.is_empty());
        // Task is now SUCCESS.
        let report = manager.get_task(&task_id).expect("task");
        assert_eq!(report.status, crate::MsqTaskStatus::Success);
    }

    #[tokio::test]
    async fn aggregated_select_reports_real_schema_not_synthetic_time() {
        // Wave 36-G1 / Wave 37B msq High: `SELECT channel, COUNT(*)
        // FROM wiki GROUP BY channel ORDER BY channel` plans as
        // Scan(0) -> Aggregate(1) -> Sort(2) with output_stage = 2.
        // The buggy code returned `__time:TIMESTAMP` for the Sort
        // output; the fix walks input_stages to recover the real
        // Aggregate schema (channel:VARCHAR, count:BIGINT).
        let manager = MsqManager::new();
        let spec = crate::MsqTaskSpec {
            query: "SELECT channel, COUNT(*) FROM wiki GROUP BY channel ORDER BY channel".into(),
            context: serde_json::Value::Null,
            parameters: vec![],
        };
        let task_id = manager.submit(spec).expect("submit");
        let plan = plan_msq("SELECT channel, COUNT(*) FROM wiki GROUP BY channel ORDER BY channel")
            .expect("plan");
        let results = execute_msq(&plan, &manager, &task_id).await.expect("exec");

        let sig_names: Vec<&str> = results.signature.iter().map(|c| c.name.as_str()).collect();
        assert!(
            sig_names.contains(&"channel"),
            "expected `channel` column in signature, got {sig_names:?}",
        );
        assert!(
            sig_names.contains(&"count"),
            "expected `count` column in signature, got {sig_names:?}",
        );
        // Specifically, the buggy single-column `__time` schema must NOT
        // be reported here.
        assert!(
            !(results.signature.len() == 1 && results.signature[0].name == "__time"),
            "Sort-over-Aggregate must not collapse to bare __time schema, got {:?}",
            results.signature,
        );
    }

    #[test]
    fn extract_agg_specs_captures_repeated_aggregates() {
        // DD R38: two SUMs of different columns must BOTH be planned — a single
        // `find` per function kind previously dropped all but the first.
        let specs =
            extract_agg_specs("SELECT CITY, SUM(ADDED), SUM(DELETED) FROM WIKI GROUP BY CITY");
        let names: Vec<&str> = specs.iter().filter_map(|s| s["name"].as_str()).collect();
        assert!(names.contains(&"sum_added"), "missing sum_added: {names:?}");
        assert!(
            names.contains(&"sum_deleted"),
            "missing sum_deleted: {names:?}",
        );
        // A repeated identical aggregate de-dups to one column.
        let dup = extract_agg_specs("SELECT SUM(X), SUM(X) FROM T");
        let dup_names: Vec<&str> = dup.iter().filter_map(|s| s["name"].as_str()).collect();
        assert_eq!(dup_names, vec!["sum_x"], "identical aggregate must de-dup");
    }

    #[test]
    fn rejects_unsupported_aggregate_forms() {
        // DD R39: unsupported aggregate/window forms must FAIL CLOSED rather than
        // silently mis-plan into wrong results.
        assert!(
            plan_msq("SELECT channel, AVG(added) FROM wiki GROUP BY channel").is_err(),
            "AVG must be rejected"
        );
        assert!(
            plan_msq("SELECT COUNT(DISTINCT user) FROM wiki").is_err(),
            "COUNT(DISTINCT) must be rejected"
        );
        assert!(
            plan_msq("SELECT channel, COUNT(user) FROM wiki GROUP BY channel").is_err(),
            "COUNT(col) must be rejected"
        );
        assert!(
            plan_msq("SELECT language, SUM(added) OVER (PARTITION BY language) FROM wiki").is_err(),
            "window OVER must be rejected"
        );
        // Supported forms still plan.
        assert!(
            plan_msq("SELECT channel, COUNT(*) FROM wiki GROUP BY channel").is_ok(),
            "COUNT(*) must still plan"
        );
        assert!(
            plan_msq("SELECT city, SUM(added), SUM(deleted) FROM wiki GROUP BY city").is_ok(),
            "SUM(col) must still plan"
        );
    }

    #[test]
    fn aggregate_detection_tolerates_whitespace_before_paren() {
        // DD R40: `FUNC (` (whitespace before the paren) must not bypass
        // aggregate detection. Pre-fix, `SUM (n)` emitted no spec and the COUNT
        // fallback returned counts instead of sums, while `AVG (n)` /
        // `COUNT (col)` / `COUNT (DISTINCT x)` slipped past the R39 reject.

        // `SUM (n)` must plan a REAL sum, not fall back to COUNT.
        let plan = plan_msq("SELECT city, SUM (n) FROM t GROUP BY city").expect("plan");
        let agg_names: Vec<String> = plan
            .stages
            .iter()
            .find_map(|s| match &s.stage_type {
                StageType::Aggregate { aggregations, .. } => Some(
                    aggregations
                        .iter()
                        .filter_map(|a| a["name"].as_str().map(str::to_owned))
                        .collect(),
                ),
                _ => None,
            })
            .expect("aggregate stage present");
        assert!(
            agg_names.iter().any(|n| n == "sum_n"),
            "`SUM (n)` must plan a real sum, got {agg_names:?}"
        );

        // MIN/MAX with assorted spacing also plan real aggregates.
        assert!(
            plan_msq("SELECT city, MIN (n), MAX  (n) FROM t GROUP BY city").is_ok(),
            "spaced MIN/MAX must still plan"
        );

        // Unsupported forms must FAIL CLOSED even with a space before the paren.
        assert!(
            plan_msq("SELECT city, AVG (n) FROM t GROUP BY city").is_err(),
            "`AVG (n)` must be rejected"
        );
        assert!(
            plan_msq("SELECT COUNT (user) FROM t").is_err(),
            "`COUNT (col)` must be rejected"
        );
        assert!(
            plan_msq("SELECT COUNT (DISTINCT user) FROM t").is_err(),
            "`COUNT (DISTINCT x)` must be rejected"
        );
        assert!(
            plan_msq("SELECT lang, SUM(added) OVER (PARTITION BY lang) FROM t").is_err(),
            "spaced window OVER must be rejected"
        );
    }

    #[test]
    fn bare_select_keeps_legacy_time_schema() {
        // Sort-over-Scan with no GROUP BY remains the placeholder
        // single-node `__time` schema, preserving existing behaviour for
        // `SELECT * FROM …` paths until full row-level execution lands.
        let plan = plan_msq("SELECT __time FROM wiki ORDER BY __time").expect("plan");
        let sig = signature_for_stage(&plan, plan.output_stage);
        assert_eq!(sig.len(), 1);
        assert_eq!(sig[0].name, "__time");
        assert_eq!(sig[0].sql_type, "TIMESTAMP");
    }

    #[tokio::test]
    async fn execute_msq_task_error_handling() {
        let manager = MsqManager::new();
        let spec = crate::MsqTaskSpec {
            query: "SELECT __time FROM wiki".to_owned(),
            context: serde_json::Value::Null,
            parameters: vec![],
        };
        let task_id = manager.submit(spec).expect("submit");

        // Complete the task first so a second execution attempt fails.
        let plan = plan_msq("SELECT __time FROM wiki").expect("plan");
        let _ = execute_msq(&plan, &manager, &task_id).await;

        // Attempting to execute again should fail (task already complete).
        let result = execute_msq_task(&plan, &manager, &task_id).await;
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Wave 45-B regression: case preservation in datasource extraction
    // (Wave 37B msq Medium #5)
    // -----------------------------------------------------------------------

    /// Pre-fix `extract_target_datasource` and `extract_source_datasource`
    /// uppercased the whole SQL then lowercased the extracted token,
    /// silently rewriting `MyTable` to `mytable`.  Datasources are
    /// case-sensitive in Druid, so the executor would have ended up
    /// scanning a different (or non-existent) table.
    #[test]
    fn extract_datasource_preserves_mixed_case_for_select() {
        let plan = plan_msq("SELECT * FROM MyDataSource").expect("plan");
        // Stage 0 is the Scan; assert the datasource kept its case.
        match &plan.stages[0].stage_type {
            StageType::Scan { data_source, .. } => {
                assert_eq!(
                    data_source, "MyDataSource",
                    "case must be preserved (Wave 45-B Medium #5 closure)"
                );
            }
            _ => panic!("expected Scan stage"),
        }
    }

    #[test]
    fn extract_datasource_preserves_mixed_case_for_insert() {
        let plan = plan_msq("INSERT INTO MyTarget SELECT * FROM MySource").expect("plan");
        match &plan.stages[0].stage_type {
            StageType::Scan { data_source, .. } => {
                assert_eq!(data_source, "MySource");
            }
            _ => panic!("expected Scan stage"),
        }
        match &plan.stages.last().expect("last").stage_type {
            StageType::Insert {
                target_data_source, ..
            } => {
                assert_eq!(target_data_source, "MyTarget");
            }
            _ => panic!("expected Insert stage"),
        }
    }

    // -----------------------------------------------------------------------
    // Wave 45-B regression: ExecutionPlan::validate (Wave 37B msq Medium #2)
    // -----------------------------------------------------------------------

    /// Build a trivially-valid 2-stage plan (Scan + Sort) for validation tests.
    fn good_plan() -> ExecutionPlan {
        ExecutionPlan {
            stages: vec![
                ExecutionStage {
                    stage_number: 0,
                    stage_type: StageType::Scan {
                        data_source: "ds".to_owned(),
                        filter: None,
                    },
                    input_stages: vec![],
                    worker_count: 1,
                },
                ExecutionStage {
                    stage_number: 1,
                    stage_type: StageType::Sort {
                        order_by: vec![],
                        limit: None,
                    },
                    input_stages: vec![0],
                    worker_count: 1,
                },
            ],
            output_stage: 1,
            sql: "SELECT * FROM ds".to_owned(),
        }
    }

    #[test]
    fn validate_accepts_well_formed_plan() {
        good_plan().validate().expect("good plan");
    }

    #[test]
    fn validate_rejects_empty_plan() {
        let plan = ExecutionPlan {
            stages: vec![],
            output_stage: 0,
            sql: "SELECT 1".to_owned(),
        };
        let err = plan.validate().expect_err("empty plan must fail");
        assert!(format!("{err}").contains("at least one stage"));
    }

    #[test]
    fn validate_rejects_output_stage_out_of_bounds() {
        let mut plan = good_plan();
        plan.output_stage = 99;
        let err = plan.validate().expect_err("OOB output stage must fail");
        assert!(format!("{err}").contains("outputStage"));
    }

    #[test]
    fn validate_rejects_misnumbered_stage() {
        let mut plan = good_plan();
        plan.stages[1].stage_number = 7;
        let err = plan
            .validate()
            .expect_err("mismatched stage_number must fail");
        let msg = format!("{err}");
        assert!(msg.contains("stageNumber"), "msg = {msg}");
    }

    #[test]
    fn validate_rejects_self_referential_input() {
        let mut plan = good_plan();
        // Stage 1 lists itself as an input → not strictly less than its
        // own index, so the topological invariant is broken.
        plan.stages[1].input_stages = vec![1];
        let err = plan
            .validate()
            .expect_err("self-referential input must fail");
        assert!(format!("{err}").contains("input stage"));
    }

    #[test]
    fn validate_rejects_forward_input_reference() {
        let mut plan = good_plan();
        // Stage 0 declares stage 1 (a later stage) as its input, which
        // would have caused pre-fix execution to read from an unfilled
        // downstream stage's results.
        plan.stages[0].input_stages = vec![1];
        let err = plan
            .validate()
            .expect_err("forward input reference must fail");
        assert!(format!("{err}").contains("input stage"));
    }

    #[tokio::test]
    async fn execute_msq_rejects_invalid_plan_before_mutating_state() {
        let manager = MsqManager::new();
        let spec = crate::MsqTaskSpec {
            query: "SELECT 1".to_owned(),
            context: serde_json::Value::Null,
            parameters: vec![],
        };
        let task_id = manager.submit(spec).expect("submit");

        let mut plan = good_plan();
        plan.stages[0].input_stages = vec![1]; // forward ref

        let err = execute_msq(&plan, &manager, &task_id)
            .await
            .expect_err("invalid plan must error");
        assert!(format!("{err}").contains("input stage"));

        // Task must remain Running because validation aborts before
        // `complete_task` runs.
        let report = manager.get_task(&task_id).expect("task exists");
        assert_eq!(report.status, crate::MsqTaskStatus::Running);
    }

    // -----------------------------------------------------------------------
    // Engine bridge: SQL plan -> QueryDefinition -> real pipeline execution
    // -----------------------------------------------------------------------

    use crate::engine::{EngineConfig, RowSignature, Value};

    fn wiki_input() -> InputTable {
        // (channel, added) rows.
        let signature = RowSignature::new(&[("channel", "VARCHAR"), ("added", "BIGINT")]);
        let rows = vec![
            vec![Value::Str("en".into()), Value::Long(10)],
            vec![Value::Str("fr".into()), Value::Long(5)],
            vec![Value::Str("en".into()), Value::Long(20)],
            vec![Value::Str("en".into()), Value::Long(2)],
            vec![Value::Str("fr".into()), Value::Long(7)],
            vec![Value::Str("de".into()), Value::Long(100)],
        ];
        InputTable { signature, rows }
    }

    #[test]
    fn plan_to_query_definition_group_by_shape() {
        let plan = plan_msq("SELECT channel, COUNT(*), SUM(added) FROM wiki GROUP BY channel")
            .expect("plan");
        let input = wiki_input();
        let qdef = plan_to_query_definition(&plan, &input.signature).expect("translate");
        // scan -> shuffle -> aggregate.
        assert_eq!(qdef.stages.len(), 3);
        assert_eq!(qdef.final_stage, 2);
        qdef.validate().expect("valid");
    }

    #[tokio::test]
    async fn execute_with_input_group_by_count_sum() {
        let manager = MsqManager::new();
        let q = "SELECT channel, COUNT(*), SUM(added) FROM wiki GROUP BY channel";
        let spec = crate::MsqTaskSpec {
            query: q.to_owned(),
            context: serde_json::Value::Null,
            parameters: vec![],
        };
        let task_id = manager.submit(spec).expect("submit");
        let plan = plan_msq(q).expect("plan");

        let cfg = EngineConfig {
            workers: 3,
            spill_threshold_bytes: usize::MAX,
        };
        let results = execute_msq_with_input(&plan, wiki_input(), &cfg, &manager, &task_id)
            .await
            .expect("exec");

        // 3 groups: de, en, fr.
        assert_eq!(results.results.len(), 3);

        // Map channel -> (count, sum_added).
        let mut by_chan = std::collections::HashMap::new();
        for r in &results.results {
            let chan = r["channel"].as_str().expect("channel").to_owned();
            let count = r["count"].as_i64().expect("count");
            let sum = r["sum_added"].as_i64().expect("sum");
            by_chan.insert(chan, (count, sum));
        }
        assert_eq!(by_chan["en"], (3, 32));
        assert_eq!(by_chan["fr"], (2, 12));
        assert_eq!(by_chan["de"], (1, 100));

        // Task is SUCCESS with populated stages.
        let report = manager.get_task(&task_id).expect("task");
        assert_eq!(report.status, crate::MsqTaskStatus::Success);
        assert_eq!(report.stages.len(), 3);
        assert_eq!(report.stages[0].input_row_count, 6); // scan reads 6 rows
        assert_eq!(report.stages[2].output_row_count, 3); // aggregate -> 3 groups
    }

    #[tokio::test]
    async fn execute_with_input_spill_matches_no_spill() {
        let manager_a = MsqManager::new();
        let manager_b = MsqManager::new();
        let q = "SELECT channel, COUNT(*), SUM(added) FROM wiki GROUP BY channel";
        let plan = plan_msq(q).expect("plan");

        let id_a = manager_a
            .submit(crate::MsqTaskSpec {
                query: q.to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit a");
        let id_b = manager_b
            .submit(crate::MsqTaskSpec {
                query: q.to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit b");

        let no_spill = EngineConfig {
            workers: 2,
            spill_threshold_bytes: usize::MAX,
        };
        let spill = EngineConfig {
            workers: 2,
            spill_threshold_bytes: 1,
        };

        let r_a = execute_msq_with_input(&plan, wiki_input(), &no_spill, &manager_a, &id_a)
            .await
            .expect("a");
        let r_b = execute_msq_with_input(&plan, wiki_input(), &spill, &manager_b, &id_b)
            .await
            .expect("b");

        // Results identical regardless of spill.
        assert_eq!(r_a.results, r_b.results);
    }

    /// DD R43 (Finding 5): `SELECT page FROM wiki WHERE channel='en' ORDER BY
    /// page LIMIT 1` must actually filter (channel='en'), order (by page), and
    /// limit (1 row), returning only the projected `page` column.
    ///
    /// Fail-before evidence: prior to the fix `plan_msq` produced a scan with
    /// `filter: None`, `plan_to_query_definition` dropped the Sort stage, and
    /// the engine scan cloned every column — so this query returned all four
    /// input rows (both columns), unfiltered and unordered.
    #[tokio::test]
    async fn execute_with_input_applies_where_order_limit_projection() {
        let manager = MsqManager::new();
        let q = "SELECT page FROM wiki WHERE channel = 'en' ORDER BY page LIMIT 1";
        let plan = plan_msq(q).expect("plan");

        // The lowered WHERE must be recorded on the scan stage (no longer None).
        match &plan.stages[0].stage_type {
            StageType::Scan { filter, .. } => {
                assert!(filter.is_some(), "WHERE must lower into the scan filter");
            }
            _ => panic!("expected Scan stage"),
        }

        let signature = RowSignature::new(&[("page", "VARCHAR"), ("channel", "VARCHAR")]);
        let rows = vec![
            vec![Value::Str("Foo".into()), Value::Str("en".into())],
            vec![Value::Str("Bar".into()), Value::Str("en".into())],
            vec![Value::Str("Zed".into()), Value::Str("fr".into())],
            vec![Value::Str("Aaa".into()), Value::Str("en".into())],
        ];
        let input = InputTable { signature, rows };

        let id = manager
            .submit(crate::MsqTaskSpec {
                query: q.to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");
        let cfg = EngineConfig::default();
        let results = execute_msq_with_input(&plan, input, &cfg, &manager, &id)
            .await
            .expect("exec");

        // Filter (channel='en' -> Foo/Bar/Aaa) + order-by page asc + limit 1
        // => the single smallest `en` page, projected to just `page`.
        assert_eq!(results.results.len(), 1, "LIMIT 1 must apply");
        let row = &results.results[0];
        assert_eq!(
            row["page"].as_str(),
            Some("Aaa"),
            "ORDER BY page asc, first row"
        );
        assert!(
            row.get("channel").is_none(),
            "projection must drop the unselected `channel` column, got {row:?}"
        );
    }

    /// DD R43 (Finding 5): a descending order must not be silently treated as
    /// ascending.
    #[tokio::test]
    async fn execute_with_input_order_by_desc() {
        let manager = MsqManager::new();
        let q = "SELECT page, channel FROM wiki WHERE channel = 'en' ORDER BY page DESC";
        let plan = plan_msq(q).expect("plan");

        let signature = RowSignature::new(&[("page", "VARCHAR"), ("channel", "VARCHAR")]);
        let rows = vec![
            vec![Value::Str("Bar".into()), Value::Str("en".into())],
            vec![Value::Str("Zed".into()), Value::Str("en".into())],
            vec![Value::Str("Aaa".into()), Value::Str("fr".into())],
        ];
        let input = InputTable { signature, rows };
        let id = manager
            .submit(crate::MsqTaskSpec {
                query: q.to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");
        let results = execute_msq_with_input(&plan, input, &EngineConfig::default(), &manager, &id)
            .await
            .expect("exec");
        let pages: Vec<&str> = results
            .results
            .iter()
            .map(|r| r["page"].as_str().expect("page"))
            .collect();
        assert_eq!(pages, vec!["Zed", "Bar"], "DESC order over the en rows");
    }

    #[tokio::test]
    async fn execute_with_input_plain_select_passes_rows_through() {
        let manager = MsqManager::new();
        let q = "SELECT channel, added FROM wiki";
        let plan = plan_msq(q).expect("plan");
        let id = manager
            .submit(crate::MsqTaskSpec {
                query: q.to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");
        let cfg = EngineConfig::default();
        let results = execute_msq_with_input(&plan, wiki_input(), &cfg, &manager, &id)
            .await
            .expect("exec");
        // No GROUP BY: all 6 input rows flow through.
        assert_eq!(results.results.len(), 6);
    }

    /// DD R44 (Finding 2): `SELECT channel, COUNT(*) AS cnt FROM wiki GROUP BY
    /// channel ORDER BY cnt DESC LIMIT 1` must honor the `cnt` alias in both the
    /// ORDER BY resolution and the output signature.
    ///
    /// Fail-before evidence: prior to the fix the aggregate was named with the
    /// synthetic `count`, `apply_order_by` resolved `cnt` against the engine
    /// signature (which only had `count`) and FAILED CLOSED — so this query
    /// errored instead of returning the largest group.
    #[tokio::test]
    async fn execute_with_input_aggregate_alias_order_by_and_projection() {
        let manager = MsqManager::new();
        let q =
            "SELECT channel, COUNT(*) AS cnt FROM wiki GROUP BY channel ORDER BY cnt DESC LIMIT 1";
        let plan = plan_msq(q).expect("plan");
        let id = manager
            .submit(crate::MsqTaskSpec {
                query: q.to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");
        let cfg = EngineConfig::default();
        let results = execute_msq_with_input(&plan, wiki_input(), &cfg, &manager, &id)
            .await
            .expect("exec");

        // en has the most rows (3), so ORDER BY cnt DESC LIMIT 1 -> en.
        assert_eq!(results.results.len(), 1, "LIMIT 1 must apply");
        let row = &results.results[0];
        assert_eq!(row["channel"].as_str(), Some("en"), "largest group first");
        assert_eq!(
            row["cnt"].as_i64(),
            Some(3),
            "aggregate output as alias `cnt`"
        );
        assert!(
            row.get("count").is_none(),
            "synthetic `count` name must be replaced by the alias, got {row:?}"
        );

        // The result signature carries the alias, not the synthetic name.
        let sig_names: Vec<&str> = results.signature.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            sig_names,
            vec!["channel", "cnt"],
            "signature carries the alias"
        );
    }

    /// DD R44 (Finding 2): `SELECT page AS p FROM wiki LIMIT 1` must return the
    /// column under its alias `p`, not the source name `page`.
    ///
    /// Fail-before evidence: prior to the fix `apply_projection` pushed the
    /// source column name, so the output column (and signature) was `page`.
    #[tokio::test]
    async fn execute_with_input_rejects_duplicate_output_alias() {
        // DD R45: two projections sharing an output name would collapse in the
        // object-shaped result (second overwrites first) and make ORDER BY
        // ambiguous; it must be rejected.
        let manager = MsqManager::new();
        let q = "SELECT page AS x, channel AS x FROM wiki";
        let plan = plan_msq(q).expect("plan");
        let signature = RowSignature::new(&[("page", "VARCHAR"), ("channel", "VARCHAR")]);
        let rows = vec![vec![Value::Str("Foo".into()), Value::Str("en".into())]];
        let input = InputTable { signature, rows };
        let id = manager
            .submit(crate::MsqTaskSpec {
                query: q.to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");
        let result =
            execute_msq_with_input(&plan, input, &EngineConfig::default(), &manager, &id).await;
        assert!(
            result.is_err(),
            "duplicate output alias must be rejected, got {result:?}"
        );
    }

    #[tokio::test]
    async fn execute_with_input_plain_column_alias() {
        let manager = MsqManager::new();
        let q = "SELECT page AS p FROM wiki LIMIT 1";
        let plan = plan_msq(q).expect("plan");
        let signature = RowSignature::new(&[("page", "VARCHAR"), ("channel", "VARCHAR")]);
        let rows = vec![
            vec![Value::Str("Foo".into()), Value::Str("en".into())],
            vec![Value::Str("Bar".into()), Value::Str("en".into())],
        ];
        let input = InputTable { signature, rows };
        let id = manager
            .submit(crate::MsqTaskSpec {
                query: q.to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");
        let results = execute_msq_with_input(&plan, input, &EngineConfig::default(), &manager, &id)
            .await
            .expect("exec");

        assert_eq!(results.results.len(), 1, "LIMIT 1 must apply");
        let row = &results.results[0];
        assert_eq!(
            row["p"].as_str(),
            Some("Foo"),
            "column returned under alias `p`"
        );
        assert!(
            row.get("page").is_none(),
            "source name `page` must be renamed to the alias, got {row:?}"
        );
        let sig_names: Vec<&str> = results.signature.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(sig_names, vec!["p"], "signature carries the alias");
    }

    #[test]
    fn translate_rejects_unknown_aggregation_field() {
        let input = RowSignature::new(&[("channel", "VARCHAR")]);
        let aggs = vec![
            serde_json::json!({"type":"longSum","func":"sum","name":"sum_nope","fieldName":"nope"}),
        ];
        let err = translate_aggregations(&aggs, &input).expect_err("missing field");
        assert!(format!("{err}").contains("nope"));
    }
}
