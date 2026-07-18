// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! CL-5 evidence capture — runs the same 3-worker INSERT path as the
//! main E2E and prints per-stage counters / retry counts / segment ids
//! to stdout so `cargo test -- --nocapture` produces a quotable
//! transcript for `tests/msq-compat/RESULTS_distributed_*.md`.

use std::net::SocketAddr;
use std::sync::Arc;

use ferrodruid_deep_storage::{DeepStorage, InMemoryDeepStorage};

use ferrodruid_msq::coordinator::{CoordinatorConfig, WorkerFleet};
use ferrodruid_msq::distributed::submit_insert_distributed;
use ferrodruid_msq::engine::{Row, RowSignature, Value};
use ferrodruid_msq::segment_io::{publish_segment, scan_data_source};
use ferrodruid_msq::worker::{MsqWorker, WorkerHandle};
use ferrodruid_msq::{MsqManager, MsqTaskSpec};

async fn spawn_workers(n: usize) -> (WorkerFleet, Vec<WorkerHandle>) {
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(n);
    let mut handles: Vec<WorkerHandle> = Vec::with_capacity(n);
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

async fn seed(ds: &dyn DeepStorage) -> usize {
    let sig = RowSignature {
        columns: vec!["__time".into(), "city".into(), "n".into()],
        types: vec!["BIGINT".into(), "VARCHAR".into(), "BIGINT".into()],
    };
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evidence_clean_3_worker_insert() {
    let raw = InMemoryDeepStorage::new();
    let seeded = seed(&raw).await;
    let ds: Arc<dyn DeepStorage> = Arc::new(raw);

    let (fleet, handles) = spawn_workers(3).await;
    let addrs: Vec<SocketAddr> = fleet.addrs.clone();
    let manager = MsqManager::new();
    let spec = MsqTaskSpec {
        query: "INSERT INTO tgt SELECT city, COUNT(*), SUM(n) FROM src GROUP BY city".into(),
        context: serde_json::Value::Null,
        parameters: vec![],
    };

    let t0 = std::time::Instant::now();
    let outcome = submit_insert_distributed(
        spec,
        &manager,
        fleet,
        CoordinatorConfig::default(),
        Arc::clone(&ds),
    )
    .await
    .expect("INSERT");
    let elapsed = t0.elapsed();

    eprintln!("==== EVIDENCE: clean 3-worker INSERT ====");
    eprintln!("worker_addrs = {addrs:?}");
    eprintln!("seeded_rows = {seeded}");
    eprintln!("elapsed_ms = {}", elapsed.as_millis());
    eprintln!("task_id = {}", outcome.task_id);
    eprintln!("segment_id = {}", outcome.segment_id);
    eprintln!("target = {}", outcome.target_data_source);
    eprintln!("rows_published = {}", outcome.rows_published);
    eprintln!("retry_count = {}", outcome.retry_count);
    let report = manager.get_task(&outcome.task_id).expect("report");
    for stage in &report.stages {
        eprintln!(
            "stage {}  workers={}  rows_in={}  rows_out={}  shuffle={:?}  phase={}",
            stage.stage_number,
            stage.worker_count,
            stage.input_row_count,
            stage.output_row_count,
            stage.shuffle_type,
            stage.phase,
        );
    }
    let scanned = scan_data_source(ds.as_ref(), "tgt").await.expect("scan");
    eprintln!("read_back_rows = {}", scanned.rows.len());
    eprintln!("read_back_signature = {:?}", scanned.signature);

    for h in handles {
        h.shutdown().await;
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evidence_3_worker_insert_with_kill() {
    let raw = InMemoryDeepStorage::new();
    let seeded = seed(&raw).await;
    let ds: Arc<dyn DeepStorage> = Arc::new(raw);

    let (fleet, mut handles) = spawn_workers(3).await;
    let addrs: Vec<SocketAddr> = fleet.addrs.clone();
    let dead = handles.remove(1);
    dead.abort();
    let _ = dead.join().await;

    let manager = MsqManager::new();
    let spec = MsqTaskSpec {
        query: "INSERT INTO tgt SELECT city, COUNT(*), SUM(n) FROM src GROUP BY city".into(),
        context: serde_json::Value::Null,
        parameters: vec![],
    };

    let t0 = std::time::Instant::now();
    let outcome = submit_insert_distributed(
        spec,
        &manager,
        fleet,
        CoordinatorConfig::default(),
        Arc::clone(&ds),
    )
    .await
    .expect("INSERT with kill");
    let elapsed = t0.elapsed();

    eprintln!("==== EVIDENCE: 3-worker INSERT with worker[1] killed ====");
    eprintln!("worker_addrs (all 3) = {addrs:?}");
    eprintln!("killed_worker_idx = 1");
    eprintln!("seeded_rows = {seeded}");
    eprintln!("elapsed_ms = {}", elapsed.as_millis());
    eprintln!("task_id = {}", outcome.task_id);
    eprintln!("segment_id = {}", outcome.segment_id);
    eprintln!("retry_count = {}", outcome.retry_count);
    let scanned = scan_data_source(ds.as_ref(), "tgt").await.expect("scan");
    eprintln!("read_back_rows = {}", scanned.rows.len());

    for h in handles {
        h.shutdown().await;
        h.abort();
    }
}
