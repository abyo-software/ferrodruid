// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! CL-5 closure E2E — 3-worker MSQ distributed execution on loopback.
//!
//! Covers the W1-E closure bar:
//!
//! * Sub-item 1: 3 workers each receive an `ExecuteSlice` per stage.
//! * Sub-item 2: stage I/O moves over real `TcpStream` between the
//!   coordinator and the workers (loopback ports — multi-host EC2 is
//!   the explicit follow-up gated on AWS approval).
//! * Sub-item 3: a worker is killed before dispatch; the coordinator
//!   reassigns its slice to a survivor and the run still completes
//!   with the same golden output.
//! * Sub-item 4: `Scan` reads via `DeepStorage` (`InMemoryDeepStorage`
//!   for this test); `Insert` publishes a real `Segment` artifact via
//!   `publish_segment` → `DeepStorage::upload_segment`.
//! * Sub-item 5: an `INSERT INTO tgt SELECT city, COUNT(*), SUM(n)
//!   FROM src GROUP BY city` task drives 1-4 end-to-end and the
//!   resulting segment is read back by `scan_data_source` (the same
//!   API a historical-tier scan would use).
//!
//! Honest scope: loopback only.  The same code path will run multi-host
//! once AWS approval lands — closure for that wave is tracked
//! separately as CL-5-R1 in the W1-E reconcile commit.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use ferrodruid_deep_storage::{DeepStorage, InMemoryDeepStorage};

use ferrodruid_msq::coordinator::{CoordinatorConfig, MsqCoordinator, WorkerFleet};
use ferrodruid_msq::distributed::submit_insert_distributed;
use ferrodruid_msq::engine::{
    AggFn, Processor, QueryDefinition, ShuffleSpec, StageDefinition, Value,
};
use ferrodruid_msq::engine::{Row, RowSignature};
use ferrodruid_msq::executor::plan_msq;
use ferrodruid_msq::segment_io::{publish_segment, scan_data_source};
use ferrodruid_msq::worker::{MsqWorker, WorkerHandle};
use ferrodruid_msq::{MsqManager, MsqTaskSpec, MsqTaskStatus};

/// Spawn `n` MsqWorkers on loopback `127.0.0.1:0` ports.  Returns the
/// fleet and the handles (drop / abort closes the workers).
async fn spawn_workers(n: usize) -> (WorkerFleet, Vec<WorkerHandle>) {
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(n);
    let mut handles: Vec<WorkerHandle> = Vec::with_capacity(n);
    for _ in 0..n {
        let w = MsqWorker::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("worker bind");
        let h = w.start();
        addrs.push(h.addr);
        handles.push(h);
    }
    (WorkerFleet::new(addrs), handles)
}

async fn shutdown(handles: Vec<WorkerHandle>) {
    for h in handles {
        h.shutdown().await;
        h.abort();
    }
}

fn seed_signature() -> RowSignature {
    RowSignature {
        columns: vec!["__time".into(), "city".into(), "n".into()],
        types: vec!["BIGINT".into(), "VARCHAR".into(), "BIGINT".into()],
    }
}

async fn seed_source(ds: &dyn DeepStorage) -> usize {
    let sig = seed_signature();
    let rows: Vec<Row> = vec![
        vec![Value::Long(1), Value::Str("tokyo".into()), Value::Long(10)],
        vec![Value::Long(2), Value::Str("osaka".into()), Value::Long(20)],
        vec![Value::Long(3), Value::Str("tokyo".into()), Value::Long(30)],
        vec![Value::Long(4), Value::Str("kyoto".into()), Value::Long(40)],
        vec![Value::Long(5), Value::Str("tokyo".into()), Value::Long(50)],
        vec![Value::Long(6), Value::Str("osaka".into()), Value::Long(60)],
        vec![Value::Long(7), Value::Str("nagoya".into()), Value::Long(70)],
        vec![Value::Long(8), Value::Str("kyoto".into()), Value::Long(80)],
        vec![Value::Long(9), Value::Str("tokyo".into()), Value::Long(90)],
    ];
    let n = rows.len();
    publish_segment(ds, "src", "src_seg_0", &sig, &rows)
        .await
        .expect("seed");
    n
}

// -------------------------------------------------------------------------
// Sub-item 1 + 2: 3-worker GROUP BY over TCP loopback, deterministic output.
// -------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_groupby_three_workers_matches_golden() {
    let (fleet, handles) = spawn_workers(3).await;
    let coord = MsqCoordinator::new(fleet, CoordinatorConfig::default());

    // scan -> shuffle (hash by city, 3 partitions) -> aggregate (partial).
    let in_sig = RowSignature::new(&[("city", "VARCHAR"), ("n", "BIGINT")]);
    let agg_sig = RowSignature::new(&[
        ("city", "VARCHAR"),
        ("count", "BIGINT"),
        ("sum_n", "BIGINT"),
    ]);
    let qdef = QueryDefinition {
        stages: vec![
            StageDefinition {
                stage_number: 0,
                inputs: vec![],
                processor: Processor::Scan {
                    project: in_sig.columns.clone(),
                },
                signature: in_sig.clone(),
                shuffle: ShuffleSpec::None,
            },
            StageDefinition {
                stage_number: 1,
                inputs: vec![0],
                processor: Processor::Shuffle,
                signature: in_sig.clone(),
                shuffle: ShuffleSpec::Hash {
                    key: vec!["city".into()],
                    partitions: 3,
                },
            },
            StageDefinition {
                stage_number: 2,
                inputs: vec![1],
                processor: Processor::Aggregate {
                    group_by: vec!["city".into()],
                    aggs: vec![AggFn::Count, AggFn::LongSum { field: "n".into() }],
                },
                signature: agg_sig,
                shuffle: ShuffleSpec::None,
            },
        ],
        final_stage: 2,
    };

    let input = vec![
        vec![Value::Str("tokyo".into()), Value::Long(10)],
        vec![Value::Str("osaka".into()), Value::Long(20)],
        vec![Value::Str("tokyo".into()), Value::Long(30)],
        vec![Value::Str("kyoto".into()), Value::Long(40)],
        vec![Value::Str("tokyo".into()), Value::Long(50)],
        vec![Value::Str("osaka".into()), Value::Long(60)],
        vec![Value::Str("nagoya".into()), Value::Long(70)],
        vec![Value::Str("kyoto".into()), Value::Long(80)],
        vec![Value::Str("tokyo".into()), Value::Long(90)],
    ];

    let res = coord.run("e2e-task-001", &qdef, input).await.expect("run");

    let mut by_city: HashMap<String, (i64, i64)> = HashMap::new();
    for row in &res.rows {
        let city = match &row[0] {
            Value::Str(s) => s.clone(),
            other => panic!("city not Str: {other:?}"),
        };
        let cnt = match &row[1] {
            Value::Long(n) => *n,
            other => panic!("count not Long: {other:?}"),
        };
        let sum = match &row[2] {
            Value::Long(n) => *n,
            other => panic!("sum not Long: {other:?}"),
        };
        by_city.insert(city, (cnt, sum));
    }

    // Golden — these are deterministic for the inputs above.
    assert_eq!(by_city.get("tokyo"), Some(&(4, 180)));
    assert_eq!(by_city.get("osaka"), Some(&(2, 80)));
    assert_eq!(by_city.get("kyoto"), Some(&(2, 120)));
    assert_eq!(by_city.get("nagoya"), Some(&(1, 70)));

    // Sub-item 1 evidence: every stage was dispatched to 3 workers.
    assert_eq!(res.stage_counters.len(), 3);
    for sc in &res.stage_counters {
        assert_eq!(
            sc.worker_count, 3,
            "stage {} ran with {} workers (expected 3)",
            sc.stage_number, sc.worker_count
        );
    }
    // Clean baseline: no retries.
    assert_eq!(res.retry_count, 0, "expected 0 retries on a clean fleet");

    shutdown(handles).await;
}

// -------------------------------------------------------------------------
// Sub-item 3: worker-kill mid-stage triggers retry, golden output stable.
// -------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_worker_kill_triggers_retry_and_matches_baseline() {
    // Baseline run on 3 healthy workers.
    let (fleet, handles) = spawn_workers(3).await;
    let coord = MsqCoordinator::new(fleet, CoordinatorConfig::default());

    let qdef = groupby_three_stage_qdef();
    let input = baseline_input();
    let baseline = coord
        .run("baseline-001", &qdef, input.clone())
        .await
        .expect("baseline run");
    let golden = sorted_rows(&baseline.rows);
    assert_eq!(baseline.retry_count, 0);

    shutdown(handles).await;

    // Killed-worker run: spawn 3, abort #1 before dispatch.
    let (fleet, mut handles) = spawn_workers(3).await;
    let dead = handles.remove(1);
    dead.abort();
    let _ = dead.join().await;

    let coord = MsqCoordinator::new(fleet, CoordinatorConfig::default());
    let res = coord.run("kill-001", &qdef, input).await.expect("kill run");
    let killed_sorted = sorted_rows(&res.rows);

    assert!(
        res.retry_count > 0,
        "expected retry_count > 0 after killing a worker, got {}",
        res.retry_count
    );
    assert_eq!(
        killed_sorted, golden,
        "output differs after worker kill (golden vs killed): \n  golden = {golden:?}\n  killed = {killed_sorted:?}"
    );

    shutdown(handles).await;
}

fn groupby_three_stage_qdef() -> QueryDefinition {
    let in_sig = RowSignature::new(&[("city", "VARCHAR"), ("n", "BIGINT")]);
    let agg_sig = RowSignature::new(&[
        ("city", "VARCHAR"),
        ("count", "BIGINT"),
        ("sum_n", "BIGINT"),
    ]);
    QueryDefinition {
        stages: vec![
            StageDefinition {
                stage_number: 0,
                inputs: vec![],
                processor: Processor::Scan {
                    project: in_sig.columns.clone(),
                },
                signature: in_sig.clone(),
                shuffle: ShuffleSpec::None,
            },
            StageDefinition {
                stage_number: 1,
                inputs: vec![0],
                processor: Processor::Shuffle,
                signature: in_sig.clone(),
                shuffle: ShuffleSpec::Hash {
                    key: vec!["city".into()],
                    partitions: 3,
                },
            },
            StageDefinition {
                stage_number: 2,
                inputs: vec![1],
                processor: Processor::Aggregate {
                    group_by: vec!["city".into()],
                    aggs: vec![AggFn::Count, AggFn::LongSum { field: "n".into() }],
                },
                signature: agg_sig,
                shuffle: ShuffleSpec::None,
            },
        ],
        final_stage: 2,
    }
}

fn baseline_input() -> Vec<Row> {
    vec![
        vec![Value::Str("tokyo".into()), Value::Long(1)],
        vec![Value::Str("osaka".into()), Value::Long(2)],
        vec![Value::Str("tokyo".into()), Value::Long(3)],
        vec![Value::Str("kyoto".into()), Value::Long(4)],
        vec![Value::Str("nagoya".into()), Value::Long(5)],
        vec![Value::Str("tokyo".into()), Value::Long(6)],
    ]
}

fn sorted_rows(rows: &[Row]) -> Vec<Row> {
    let mut v: Vec<Row> = rows.to_vec();
    v.sort_by(|a, b| {
        let key_a = match &a[0] {
            Value::Str(s) => s.clone(),
            _ => String::new(),
        };
        let key_b = match &b[0] {
            Value::Str(s) => s.clone(),
            _ => String::new(),
        };
        key_a.cmp(&key_b)
    });
    v
}

// -------------------------------------------------------------------------
// Sub-item 4 + 5: INSERT INTO drives 3-worker plan, segment readable back.
// -------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_into_three_workers_publishes_readable_segment() {
    let raw = InMemoryDeepStorage::new();
    let seeded = seed_source(&raw).await;
    let ds: Arc<dyn DeepStorage> = Arc::new(raw);

    let (fleet, handles) = spawn_workers(3).await;
    let manager = MsqManager::new();
    let spec = MsqTaskSpec {
        query: "INSERT INTO tgt SELECT city, COUNT(*), SUM(n) FROM src GROUP BY city".into(),
        context: serde_json::Value::Null,
        parameters: vec![],
    };

    let outcome = submit_insert_distributed(
        spec,
        &manager,
        fleet,
        CoordinatorConfig::default(),
        Arc::clone(&ds),
    )
    .await
    .expect("INSERT");

    // The MSQ manager records the task as SUCCESS.
    let report = manager.get_task(&outcome.task_id).expect("task report");
    assert_eq!(report.status, MsqTaskStatus::Success);

    // Verify the segment landed on deep storage.
    assert!(
        ds.segment_exists(&outcome.target_data_source, &outcome.segment_id)
            .await
            .expect("segment_exists"),
        "segment {} not present on tgt",
        outcome.segment_id
    );

    // Re-read via the historical scan path.
    let scanned = scan_data_source(ds.as_ref(), "tgt")
        .await
        .expect("scan_data_source");

    // Should contain 1 row per distinct city.  The seeded source has
    // {tokyo, osaka, kyoto, nagoya} → 4 groups.
    let mut by_city: HashMap<String, (i64, i64)> = HashMap::new();
    let city_idx = scanned.signature.index_of("city").expect("city col");
    let cnt_idx = scanned.signature.index_of("count").expect("count col");
    let sum_idx = scanned.signature.index_of("sum_n").expect("sum_n col");
    for row in &scanned.rows {
        let city = match &row[city_idx] {
            Value::Str(s) => s.clone(),
            other => panic!("city not Str on read-back: {other:?}"),
        };
        let cnt = match &row[cnt_idx] {
            Value::Long(n) => *n,
            other => panic!("count not Long on read-back: {other:?}"),
        };
        let sum = match &row[sum_idx] {
            Value::Long(n) => *n,
            other => panic!("sum not Long on read-back: {other:?}"),
        };
        by_city.insert(city, (cnt, sum));
    }
    // Golden — seed rows summed per city:
    //   tokyo: 4 rows, n=10+30+50+90 = 180
    //   osaka: 2 rows, n=20+60 = 80
    //   kyoto: 2 rows, n=40+80 = 120
    //   nagoya: 1 row, n=70
    assert_eq!(by_city.get("tokyo"), Some(&(4, 180)));
    assert_eq!(by_city.get("osaka"), Some(&(2, 80)));
    assert_eq!(by_city.get("kyoto"), Some(&(2, 120)));
    assert_eq!(by_city.get("nagoya"), Some(&(1, 70)));
    assert_eq!(scanned.rows.len(), 4);

    // Sub-item 4 evidence: source row count traceable through report.
    let stages = &report.stages;
    let scan_stage_in = stages.iter().map(|s| s.input_row_count).max().unwrap_or(0);
    assert!(
        scan_stage_in as usize >= seeded,
        "expected scan input ≥ {seeded}, got {scan_stage_in}"
    );
    // Sub-item 1: at least one stage ran 3 workers.
    let max_workers = stages.iter().map(|s| s.worker_count).max().unwrap_or(0);
    assert!(
        max_workers >= 3,
        "expected at least one stage with ≥3 workers, got {max_workers}"
    );
    // Sub-item 5: outcome reports the published segment.
    assert!(outcome.segment_id.starts_with("msq_"));
    assert_eq!(outcome.rows_published, 4);

    shutdown(handles).await;
}

// -------------------------------------------------------------------------
// Sub-item 5 (retry idempotency): INSERT with a worker kill produces the
// same segment + same row count (deterministic segment id, no duplicates).
// -------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_into_with_worker_kill_remains_idempotent() {
    let raw = InMemoryDeepStorage::new();
    let _ = seed_source(&raw).await;
    let ds: Arc<dyn DeepStorage> = Arc::new(raw);

    // Spawn 3, kill #1 before dispatch — coordinator must retry to a
    // survivor.
    let (fleet, mut handles) = spawn_workers(3).await;
    let dead = handles.remove(1);
    dead.abort();
    let _ = dead.join().await;

    let manager = MsqManager::new();
    let spec = MsqTaskSpec {
        query: "INSERT INTO tgt SELECT city, COUNT(*), SUM(n) FROM src GROUP BY city".into(),
        context: serde_json::Value::Null,
        parameters: vec![],
    };

    let outcome = submit_insert_distributed(
        spec,
        &manager,
        fleet,
        CoordinatorConfig::default(),
        Arc::clone(&ds),
    )
    .await
    .expect("INSERT with kill");

    let scanned = scan_data_source(ds.as_ref(), "tgt").await.expect("scan");
    // Same row count as the no-kill case → no duplicates from retry.
    assert_eq!(scanned.rows.len(), 4);
    // Same segment id shape (deterministic from task id).
    assert!(outcome.segment_id.starts_with("msq_"));

    shutdown(handles).await;
}

// -------------------------------------------------------------------------
// plan_msq + SELECT bridge: a plain SELECT plans cleanly even though
// distributed `submit_select_distributed` is not on the closure bar; this
// pins the planner accepts the same shapes the distributed coordinator
// would consume.
// -------------------------------------------------------------------------

#[test]
fn plan_msq_accepts_insert_into_select() {
    let plan =
        plan_msq("INSERT INTO tgt SELECT city, COUNT(*) FROM src GROUP BY city").expect("plan");
    assert!(plan.stages.iter().any(|s| matches!(
        s.stage_type,
        ferrodruid_msq::executor::StageType::Insert { .. }
    )));
    assert!(plan.stages.iter().any(|s| matches!(
        s.stage_type,
        ferrodruid_msq::executor::StageType::Scan { .. }
    )));
}
