// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Multi-process smoke test for Wave 39.HH cross-role wire.
//!
//! Spawns the actual `ferrodruid-broker` and `ferrodruid-middlemanager`
//! binaries as child processes and exercises the cross-role HTTP
//! contracts. This is the Wave 39.HH equivalent of "did the wire
//! actually go over loopback between two OS processes" — strictly
//! stronger than the in-process axum integration tests.
//!
//! ## How the binaries are located
//!
//! The test resolves the binary path via `CARGO_BIN_EXE_<name>` if
//! present (works when invoked through `cargo test -p
//! ferrodruid-broker-bin`). Otherwise it falls back to
//! `<workspace>/target/<profile>/<name>` so a developer running
//! `cargo test --workspace` after a prior `cargo build --workspace`
//! still gets the test executed.
//!
//! When the binaries are not yet built, the test logs a notice and
//! marks itself as skipped via `eprintln!` + `return`. This keeps the
//! suite green on a fresh checkout where the operator only ran
//! `cargo test -p ferrodruid-rpc` without a prior workspace build.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use ferrodruid_rpc::{
    BrokerClient, HistoricalClient, HttpBrokerClient, HttpHistoricalClient,
    HttpMiddleManagerClient, MiddleManagerClient, SegmentDropCommand, SegmentLoadCommand,
    SegmentLoadState, SegmentQuery, SqlQuery, TaskAssignment, TaskKind, TaskState,
};
use tokio::process::{Child, Command};

/// Best-effort binary lookup. Returns `None` when the binary cannot be
/// found, in which case the test prints a skip message and exits OK.
fn find_binary(name: &str) -> Option<PathBuf> {
    // 1. Honour cargo's per-test env var if the test is being invoked
    //    through the binary's own package. (Cargo only sets this for
    //    the package owning the bin target, which the rpc crate is
    //    NOT — but check anyway in case Cargo's behaviour changes.)
    let env_key = format!("CARGO_BIN_EXE_{name}");
    if let Ok(p) = std::env::var(&env_key) {
        let path = PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }

    // 2. Fall back to <workspace>/target/<profile>/<name>. Try the
    //    common profiles in priority order.
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    // ferrodruid-rpc lives at <workspace>/crates/ferrodruid-rpc, so
    // pop two segments.
    let workspace_root = PathBuf::from(manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)?;
    for profile in ["debug", "release"] {
        let candidate = workspace_root.join("target").join(profile).join(name);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

/// Pick an ephemeral port by binding then immediately releasing.
async fn pick_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    addr.port()
}

/// Wait until `url` responds 2xx to a GET, or the budget runs out.
async fn wait_until_listening(url: &str, max_wait: Duration) -> bool {
    let started = std::time::Instant::now();
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    while started.elapsed() < max_wait {
        if let Ok(resp) = client.get(url).send().await
            && resp.status().is_success()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Convenience: spawn a child with stdout/stderr inherited so a test
/// failure shows the binary's banner/log lines.
fn spawn_inheriting(mut cmd: Command) -> std::io::Result<Child> {
    cmd.stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
}

#[tokio::test]
async fn multi_process_router_to_broker_smoke() {
    let Some(broker_bin) = find_binary("ferrodruid-broker") else {
        eprintln!(
            "skipping multi_process_router_to_broker_smoke: ferrodruid-broker binary \
             not found (run `cargo build --bin ferrodruid-broker` first)"
        );
        return;
    };

    let port = pick_port().await;
    let mut cmd = Command::new(&broker_bin);
    cmd.arg("--cross-role-mtls").arg("disabled");
    cmd.arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--data-dir")
        .arg(std::env::temp_dir().join(format!("ferrodruid-broker-test-{port}")))
        .arg("--broker-id")
        .arg("smoke-broker");

    let mut child = spawn_inheriting(cmd).expect("spawn broker");
    let info_url = format!("http://127.0.0.1:{port}/druid/v2/info");
    let ready = wait_until_listening(&info_url, Duration::from_secs(15)).await;
    if !ready {
        let _ = child.kill().await;
        panic!("broker did not become ready within 15s");
    }

    // Wave 43.TT replaced the W3 echo with a real SQL → native query
    // bridge. With no historicals configured (this smoke test), the
    // broker correctly returns 503 instead of an echo. Round-tripping
    // the cross-process wire is still asserted: the broker accepted
    // the request, parsed it, and returned the documented "no
    // historicals" body. The full SQL → scatter → merge happy-path
    // is exercised in unit tests against a `MockHistoricalClient`.
    let url = format!("http://127.0.0.1:{port}");
    let client = HttpBrokerClient::try_new(&url).expect("client builds");

    // The router-side `info` round-trip still works — the broker is
    // fully alive even when scatter is unavailable.
    let info_result = client.info().await;
    let sql_result = client.query(SqlQuery::new("SELECT 'multi-process'")).await;
    let _ = child.kill().await;

    let info = info_result.expect("info still works");
    assert_eq!(info.role, "broker");
    assert_eq!(info.broker_id, "smoke-broker");

    match sql_result {
        Err(ferrodruid_rpc::RpcError::Http { status, body }) => {
            assert_eq!(status, 503);
            assert!(
                body.contains("no historicals"),
                "expected 'no historicals' body, got: {body}"
            );
        }
        other => panic!("expected 503 from bridgeless broker, got: {other:?}"),
    }
}

#[tokio::test]
async fn multi_process_overlord_to_middlemanager_smoke() {
    let Some(mm_bin) = find_binary("ferrodruid-middlemanager") else {
        eprintln!(
            "skipping multi_process_overlord_to_middlemanager_smoke: \
             ferrodruid-middlemanager binary not found"
        );
        return;
    };

    let port = pick_port().await;
    let mut cmd = Command::new(&mm_bin);
    cmd.arg("--cross-role-mtls").arg("disabled");
    cmd.arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--data-dir")
        .arg(std::env::temp_dir().join(format!("ferrodruid-mm-test-{port}")))
        .arg("--pending-to-running-ms")
        .arg("10")
        .arg("--running-to-success-ms")
        .arg("10");

    let mut child = spawn_inheriting(cmd).expect("spawn middlemanager");

    // The middleManager has no /druid/v1/middlemanager/info endpoint
    // yet; readiness probe = TCP-connect on the port.
    let started = std::time::Instant::now();
    let mut up = false;
    while started.elapsed() < Duration::from_secs(15) {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !up {
        let _ = child.kill().await;
        panic!("middlemanager did not bind within 15s");
    }

    let url = format!("http://127.0.0.1:{port}");
    let client = HttpMiddleManagerClient::try_new(&url).expect("client builds");

    let task = TaskAssignment::new(TaskKind::Index, "smoke-ds");
    let id = task.task_id.clone();
    let initial = client.assign_task(task).await;
    let initial = match initial {
        Ok(s) => s,
        Err(e) => {
            let _ = child.kill().await;
            panic!("dispatch failed: {e}");
        }
    };
    assert_eq!(initial.state, TaskState::Pending);

    let mut last = TaskState::Pending;
    for _ in 0..100 {
        match client.task_status(&id).await {
            Ok(s) => {
                last = s.state;
                if last == TaskState::Success {
                    break;
                }
            }
            Err(e) => {
                let _ = child.kill().await;
                panic!("poll failed: {e}");
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = child.kill().await;
    assert_eq!(last, TaskState::Success, "task should reach Success");
}

// =========================================================================
// Wave 40.LL — multi-process historical smoke tests
// =========================================================================

#[tokio::test]
async fn multi_process_broker_to_historical_scatter_smoke() {
    let Some(hist_bin) = find_binary("ferrodruid-historical") else {
        eprintln!(
            "skipping multi_process_broker_to_historical_scatter_smoke: \
             ferrodruid-historical binary not found"
        );
        return;
    };

    let port = pick_port().await;
    let mut cmd = Command::new(&hist_bin);
    cmd.arg("--cross-role-mtls").arg("disabled");
    cmd.arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--data-dir")
        .arg(std::env::temp_dir().join(format!("ferrodruid-hist-test-{port}")))
        .arg("--historical-id")
        .arg("smoke-historical")
        .arg("--loading-to-loaded-ms")
        .arg("10");

    let mut child = spawn_inheriting(cmd).expect("spawn historical");

    // Readiness probe = TCP-connect on the port (no GET endpoint
    // returns 200 unconditionally; the loadstatus endpoint does, so
    // poll that).
    let status_url = format!("http://127.0.0.1:{port}/druid/v1/historical/loadstatus");
    let ready = wait_until_listening(&status_url, Duration::from_secs(15)).await;
    if !ready {
        let _ = child.kill().await;
        panic!("historical did not become ready within 15s");
    }

    let url = format!("http://127.0.0.1:{port}");
    let client = HttpHistoricalClient::try_new(&url).expect("client builds");

    // Scatter query: cross-process echo.
    let resp = client
        .scatter_query(SegmentQuery::new("SELECT 'multi-process'", "seg-mp"))
        .await;
    let resp = match resp {
        Ok(r) => r,
        Err(e) => {
            let _ = child.kill().await;
            panic!("scatter failed: {e}");
        }
    };
    assert_eq!(resp.segment_id, "seg-mp");
    assert_eq!(
        resp.rows[0][0],
        serde_json::Value::String("SELECT 'multi-process'".into())
    );

    let _ = child.kill().await;
}

#[tokio::test]
async fn multi_process_coordinator_to_historical_load_smoke() {
    let Some(hist_bin) = find_binary("ferrodruid-historical") else {
        eprintln!(
            "skipping multi_process_coordinator_to_historical_load_smoke: \
             ferrodruid-historical binary not found"
        );
        return;
    };

    let port = pick_port().await;
    let mut cmd = Command::new(&hist_bin);
    cmd.arg("--cross-role-mtls").arg("disabled");
    cmd.arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--data-dir")
        .arg(std::env::temp_dir().join(format!("ferrodruid-hist-load-{port}")))
        .arg("--loading-to-loaded-ms")
        .arg("10");

    let mut child = spawn_inheriting(cmd).expect("spawn historical");
    let status_url = format!("http://127.0.0.1:{port}/druid/v1/historical/loadstatus");
    if !wait_until_listening(&status_url, Duration::from_secs(15)).await {
        let _ = child.kill().await;
        panic!("historical did not become ready within 15s");
    }

    let url = format!("http://127.0.0.1:{port}");
    let client = HttpHistoricalClient::try_new(&url).expect("client builds");

    let load = client
        .load_segment(SegmentLoadCommand::new(
            "seg-mp",
            "ds-mp",
            "deepstore://mp/seg-mp",
        ))
        .await;
    let load = match load {
        Ok(r) => r,
        Err(e) => {
            let _ = child.kill().await;
            panic!("load failed: {e}");
        }
    };
    assert_eq!(load.state, SegmentLoadState::Loading);

    // Poll loadstatus until Loaded or budget exhausted.
    let mut last = SegmentLoadState::Loading;
    for _ in 0..100 {
        match client.load_status().await {
            Ok(table) => {
                if let Some(&s) = table.get("seg-mp") {
                    last = s;
                    if last == SegmentLoadState::Loaded {
                        break;
                    }
                }
            }
            Err(e) => {
                let _ = child.kill().await;
                panic!("loadstatus failed: {e}");
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Drop and confirm.
    let drop_resp = client.drop_segment(SegmentDropCommand::new("seg-mp")).await;
    let drop_resp = match drop_resp {
        Ok(r) => r,
        Err(e) => {
            let _ = child.kill().await;
            panic!("drop failed: {e}");
        }
    };

    let _ = child.kill().await;
    assert_eq!(
        last,
        SegmentLoadState::Loaded,
        "segment should reach Loaded"
    );
    assert_eq!(drop_resp.state, SegmentLoadState::Dropped);
}

// =========================================================================
// Wave 41.OO — multi-process real-loader + native query E2E
// =========================================================================

#[tokio::test]
async fn multi_process_real_loader_native_timeseries_e2e() {
    use ferrodruid_rpc::native_query::{Aggregation, NativeQuery, TimeseriesSpec};

    let Some(hist_bin) = find_binary("ferrodruid-historical") else {
        eprintln!(
            "skipping multi_process_real_loader_native_timeseries_e2e: \
             ferrodruid-historical binary not found"
        );
        return;
    };

    // Compose a deep-storage tree under a tempdir.
    let dir = tempfile::tempdir().expect("tempdir");
    let seg_dir = dir.path().join("wikipedia").join("wiki_e2e_v0");
    tokio::fs::create_dir_all(&seg_dir).await.expect("mkdir");
    let artifact = r#"{"segmentId":"wiki_e2e_v0","dataSource":"wikipedia","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"},{"name":"count","type":"long"}]}
{"__time":1714694400000,"page":"home","count":3}
{"__time":1714694460000,"page":"home","count":2}
{"__time":1714694520000,"page":"about","count":1}
"#;
    tokio::fs::write(seg_dir.join("segment.jsonl"), artifact)
        .await
        .expect("write");

    let port = pick_port().await;
    let mut cmd = Command::new(&hist_bin);
    cmd.arg("--cross-role-mtls").arg("disabled");
    cmd.arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--data-dir")
        .arg(std::env::temp_dir().join(format!("ferrodruid-hist-w41-{port}")))
        .arg("--loading-to-loaded-ms")
        .arg("10")
        .arg("--real-loader")
        .arg("--deep-storage-root")
        .arg(dir.path());

    let mut child = spawn_inheriting(cmd).expect("spawn historical");
    let status_url = format!("http://127.0.0.1:{port}/druid/v1/historical/loadstatus");
    if !wait_until_listening(&status_url, Duration::from_secs(15)).await {
        let _ = child.kill().await;
        panic!("historical did not become ready within 15s");
    }

    let url = format!("http://127.0.0.1:{port}");
    let client = HttpHistoricalClient::try_new(&url).expect("client builds");

    // Load.
    let load = client
        .load_segment(SegmentLoadCommand::new(
            "wiki_e2e_v0",
            "wikipedia",
            "deepstore://wikipedia/wiki_e2e_v0",
        ))
        .await;
    if let Err(e) = &load {
        let _ = child.kill().await;
        panic!("load failed: {e}");
    }

    // Wait for Loaded.
    let mut last = SegmentLoadState::Loading;
    for _ in 0..100 {
        match client.load_status().await {
            Ok(table) => {
                if let Some(&s) = table.get("wiki_e2e_v0") {
                    last = s;
                    if last == SegmentLoadState::Loaded {
                        break;
                    }
                }
            }
            Err(_) => break,
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if last != SegmentLoadState::Loaded {
        let _ = child.kill().await;
        panic!("segment did not reach Loaded (last = {last:?})");
    }

    // Run a real timeseries query against the loaded segment.
    let q = NativeQuery::Timeseries(TimeseriesSpec {
        data_source: "wikipedia".into(),
        granularity_ms: 0,
        aggregations: vec![Aggregation::LongSum {
            name: "total".into(),
            field_name: "count".into(),
        }],
        filter: None,
    });
    let resp = client.native_scatter("wiki_e2e_v0", &q).await;
    let _ = child.kill().await;
    let resp = resp.expect("native scatter");
    assert_eq!(resp.segment_id, "wiki_e2e_v0");
    assert_eq!(resp.rows.len(), 1);
    let bucket = resp.rows[0][1].as_object().expect("bucket obj");
    assert_eq!(bucket.get("total"), Some(&serde_json::json!(6)), "3+2+1=6");
}

// =========================================================================
// Wave 42.RR — multi-process groupBy + topN E2E with --real-loader
// =========================================================================

#[tokio::test]
async fn multi_process_real_loader_native_group_by_and_top_n_e2e() {
    use ferrodruid_rpc::native_query::{
        Aggregation, GroupBySpec, NativeQuery, SortDirection, SortSpec, TopNSpec,
    };

    let Some(hist_bin) = find_binary("ferrodruid-historical") else {
        eprintln!(
            "skipping multi_process_real_loader_native_group_by_and_top_n_e2e: \
             ferrodruid-historical binary not found"
        );
        return;
    };

    // Stage a small artifact under a tempdir.
    let dir = tempfile::tempdir().expect("tempdir");
    let seg_dir = dir.path().join("clicks").join("c_v0");
    tokio::fs::create_dir_all(&seg_dir).await.expect("mkdir");
    let artifact = r#"{"segmentId":"c_v0","dataSource":"clicks","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"},{"name":"count","type":"long"}]}
{"__time":1,"page":"home","count":3}
{"__time":2,"page":"home","count":2}
{"__time":3,"page":"about","count":1}
{"__time":4,"page":"kb","count":7}
"#;
    tokio::fs::write(seg_dir.join("segment.jsonl"), artifact)
        .await
        .expect("write");

    let port = pick_port().await;
    let mut cmd = Command::new(&hist_bin);
    cmd.arg("--cross-role-mtls").arg("disabled");
    cmd.arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--data-dir")
        .arg(std::env::temp_dir().join(format!("ferrodruid-hist-w42-{port}")))
        .arg("--loading-to-loaded-ms")
        .arg("10")
        .arg("--real-loader")
        .arg("--deep-storage-root")
        .arg(dir.path());

    let mut child = spawn_inheriting(cmd).expect("spawn historical");
    let status_url = format!("http://127.0.0.1:{port}/druid/v1/historical/loadstatus");
    if !wait_until_listening(&status_url, Duration::from_secs(15)).await {
        let _ = child.kill().await;
        panic!("historical did not become ready within 15s");
    }

    let url = format!("http://127.0.0.1:{port}");
    let client = HttpHistoricalClient::try_new(&url).expect("client builds");

    let load = client
        .load_segment(SegmentLoadCommand::new(
            "c_v0",
            "clicks",
            "deepstore://clicks/c_v0",
        ))
        .await;
    if let Err(e) = &load {
        let _ = child.kill().await;
        panic!("load failed: {e}");
    }

    let mut last = SegmentLoadState::Loading;
    for _ in 0..100 {
        match client.load_status().await {
            Ok(table) => {
                if let Some(&s) = table.get("c_v0") {
                    last = s;
                    if last == SegmentLoadState::Loaded {
                        break;
                    }
                }
            }
            Err(_) => break,
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if last != SegmentLoadState::Loaded {
        let _ = child.kill().await;
        panic!("segment did not reach Loaded (last = {last:?})");
    }

    // groupBy: page → total descending.
    let group_by = NativeQuery::GroupBy(GroupBySpec {
        data_source: "clicks".into(),
        dimensions: vec!["page".into()],
        aggregations: vec![Aggregation::LongSum {
            name: "total".into(),
            field_name: "count".into(),
        }],
        filter: None,
        having: None,
        sort: Some(vec![SortSpec {
            dimension: "total".into(),
            direction: SortDirection::Descending,
        }]),
        limit: None,
    });
    let group_resp = client.native_scatter("c_v0", &group_by).await;
    let top_n = NativeQuery::TopN(TopNSpec {
        data_source: "clicks".into(),
        dimension: "page".into(),
        aggregations: vec![Aggregation::LongSum {
            name: "total".into(),
            field_name: "count".into(),
        }],
        metric: "total".into(),
        threshold: 1,
        filter: None,
    });
    let top_resp = client.native_scatter("c_v0", &top_n).await;
    let _ = child.kill().await;

    let group_resp = group_resp.expect("groupBy scatter");
    assert_eq!(group_resp.rows.len(), 3, "three distinct pages");
    let first = group_resp.rows[0][0]
        .as_object()
        .expect("groupBy row 0 object");
    assert_eq!(first.get("page"), Some(&serde_json::json!("kb")));
    assert_eq!(first.get("total"), Some(&serde_json::json!(7)));

    let top_resp = top_resp.expect("topN scatter");
    assert_eq!(top_resp.rows.len(), 1, "threshold=1");
    let winner = top_resp.rows[0][0].as_object().expect("topN row 0 object");
    assert_eq!(winner.get("page"), Some(&serde_json::json!("kb")));
    assert_eq!(winner.get("total"), Some(&serde_json::json!(7)));
}
