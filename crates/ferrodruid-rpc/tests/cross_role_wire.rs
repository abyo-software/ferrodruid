// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Cross-role wire integration tests for Waves 39.HH + 40.LL.
//!
//! These tests exercise the *full HTTP path* — axum server bound to a
//! real TCP port, reqwest client connecting via loopback — for **all
//! four** v1.0 cross-role flows. They are stronger than the in-crate
//! unit tests because they also catch regressions in axum routing,
//! header negotiation, and reqwest body framing.
//!
//! Wave 40.LL adds the broker→historical scatter and
//! coordinator→historical load/drop/status flows.

use std::sync::Arc;
use std::time::Duration;

use ferrodruid_rpc::broker_server::{self, BrokerServerState};
use ferrodruid_rpc::historical_server::{self, HistoricalServerState};
use ferrodruid_rpc::mm_server::{self, MiddleManagerServerState};
use ferrodruid_rpc::{
    BrokerClient, HistoricalClient, HttpBrokerClient, HttpHistoricalClient,
    HttpMiddleManagerClient, MiddleManagerClient, SegmentDropCommand, SegmentLoadCommand,
    SegmentLoadState, SegmentQuery, SqlQuery, SqlResultFormat, TaskAssignment, TaskKind, TaskState,
};

/// Helper: spawn a broker server on an ephemeral port, return its
/// base URL. The server runs until the test process exits.
async fn spawn_broker(state: BrokerServerState) -> String {
    let app = broker_server::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

/// Helper: spawn a middleManager server on an ephemeral port.
async fn spawn_middlemanager(state: Arc<MiddleManagerServerState>) -> String {
    let app = mm_server::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

/// Helper: spawn a historical server on an ephemeral port (Wave 40.LL).
async fn spawn_historical(state: Arc<HistoricalServerState>) -> String {
    let app = historical_server::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn router_to_broker_roundtrip_echoes_query_via_real_tcp() {
    let url = spawn_broker(BrokerServerState::default()).await;
    let client = HttpBrokerClient::try_new(&url).expect("client builds");

    let mut q = SqlQuery::new("SELECT count(*) FROM wikipedia");
    q.result_format = SqlResultFormat::Array;
    let resp = client.query(q.clone()).await.expect("query roundtrips");
    assert_eq!(resp.columns, vec!["echo".to_string()]);
    assert_eq!(resp.rows[0][0], serde_json::Value::String(q.query.clone()));
    assert!(resp.query_id.starts_with("q-"));
}

#[tokio::test]
async fn router_to_broker_info_endpoint_returns_advertised_tier() {
    let state = BrokerServerState {
        broker_id: "broker-itest".to_string(),
        tier: "hot".to_string(),
        version: "0.0.0-test".to_string(),
    };
    let url = spawn_broker(state).await;
    let client = HttpBrokerClient::try_new(&url).expect("client builds");
    let info = client.info().await.expect("info roundtrips");
    assert_eq!(info.role, "broker");
    assert_eq!(info.tier, "hot");
    assert_eq!(info.version, "0.0.0-test");
    assert_eq!(info.broker_id, "broker-itest");
}

#[tokio::test]
async fn overlord_to_middlemanager_dispatch_and_status_lifecycle() {
    let state = Arc::new(MiddleManagerServerState::with_timings(
        Duration::from_millis(10),
        Duration::from_millis(10),
    ));
    let url = spawn_middlemanager(Arc::clone(&state)).await;
    let client = HttpMiddleManagerClient::try_new(&url).expect("client builds");

    let task = TaskAssignment::new(TaskKind::Index, "integration-ds");
    let id = task.task_id.clone();
    let initial = client.assign_task(task).await.expect("dispatch");
    assert_eq!(initial.state, TaskState::Pending);
    assert_eq!(initial.task_id, id);

    // Poll until Success or budget exhausted.
    let mut last = TaskState::Pending;
    for _ in 0..100 {
        let s = client.task_status(&id).await.expect("poll");
        last = s.state;
        if last == TaskState::Success {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(last, TaskState::Success);

    // Server-side snapshot agrees with the wire-observed state.
    let snap = state.snapshot().await;
    let entry = snap.get(&id).expect("task tracked server-side");
    assert_eq!(entry.state, TaskState::Success);
}

#[tokio::test]
async fn overlord_to_middlemanager_status_for_unknown_task_returns_404() {
    let state = Arc::new(MiddleManagerServerState::default());
    let url = spawn_middlemanager(state).await;
    let client = HttpMiddleManagerClient::try_new(&url).expect("client builds");
    let err = client
        .task_status("does-not-exist")
        .await
        .expect_err("404 expected");
    match err {
        ferrodruid_rpc::RpcError::Http { status, .. } => {
            assert_eq!(status, 404, "wrong status: got {status}");
        }
        other => panic!("expected RpcError::Http(404), got {other:?}"),
    }
}

#[tokio::test]
async fn router_to_broker_handles_concurrent_queries_without_id_collision() {
    // Smoke: 16 concurrent queries each get a distinct query_id and
    // see their own SQL text echoed back. Catches a class of
    // server-side state races that an in-crate unit test does not.
    let url = spawn_broker(BrokerServerState::default()).await;
    let client = HttpBrokerClient::try_new(&url).expect("client builds");

    let handles: Vec<_> = (0..16)
        .map(|i| {
            let c = client.clone();
            tokio::spawn(async move {
                let sql = format!("SELECT {i}");
                let resp = c.query(SqlQuery::new(&sql)).await.expect("query");
                (resp.query_id, resp.rows[0][0].clone())
            })
        })
        .collect();

    let mut ids = Vec::new();
    for h in handles {
        let (id, echoed) = h.await.expect("task joined");
        ids.push(id);
        assert!(matches!(echoed, serde_json::Value::String(_)));
    }
    ids.sort();
    let unique = {
        let mut copy = ids.clone();
        copy.dedup();
        copy.len()
    };
    assert_eq!(unique, 16, "every concurrent query needs its own id");
}

#[tokio::test]
async fn middlemanager_dispatch_records_task_kind_in_server_snapshot() {
    let state = Arc::new(MiddleManagerServerState::with_timings(
        Duration::from_millis(0),
        Duration::from_millis(0),
    ));
    let url = spawn_middlemanager(Arc::clone(&state)).await;
    let client = HttpMiddleManagerClient::try_new(&url).expect("client builds");

    let task = TaskAssignment::new(TaskKind::Kafka, "ds-kafka");
    let id = task.task_id.clone();
    let _ = client.assign_task(task).await.expect("dispatch");

    // Wait briefly so the simulated executor flips Pending → Running
    // → Success. With both delays at zero, this should be near
    // immediate, but yield the runtime a few times to be safe.
    for _ in 0..20 {
        let snap = state.snapshot().await;
        if snap.get(&id).map(|s| s.state) == Some(TaskState::Success) {
            return;
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("task did not reach Success within budget");
}

#[tokio::test]
async fn http_broker_client_surfaces_connection_refused_as_transport_error() {
    // Bind a listener, take its port, drop the listener so the port
    // is now refused. (Mac would re-allocate fast, Linux usually
    // stays refused for the test window — we accept either Transport
    // or Http error.)
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);

    let url = format!("http://{addr}");
    let client = HttpBrokerClient::try_new(&url).expect("client builds");
    let err = client
        .query(SqlQuery::new("SELECT 1"))
        .await
        .expect_err("refused expected");
    matches!(err, ferrodruid_rpc::RpcError::Transport(_));
}

// =========================================================================
// Wave 40.LL — broker→historical and coordinator→historical wire tests
// =========================================================================

#[tokio::test]
async fn broker_to_historical_scatter_query_echoes_via_real_tcp() {
    let state = Arc::new(HistoricalServerState::default());
    let url = spawn_historical(Arc::clone(&state)).await;
    let client = HttpHistoricalClient::try_new(&url).expect("client builds");

    let resp = client
        .scatter_query(SegmentQuery::new(
            "SELECT count(*) FROM wikipedia",
            "wikipedia_2026-05-04_v0_0",
        ))
        .await
        .expect("scatter roundtrips");
    assert_eq!(resp.segment_id, "wikipedia_2026-05-04_v0_0");
    assert_eq!(resp.rows.len(), 1);
    assert_eq!(
        resp.rows[0][0],
        serde_json::Value::String("SELECT count(*) FROM wikipedia".into())
    );
}

#[tokio::test]
async fn coordinator_to_historical_load_then_status_observes_loaded() {
    let state = Arc::new(HistoricalServerState::with_config(
        "hist-itest",
        "hot",
        Duration::from_millis(10),
    ));
    let url = spawn_historical(Arc::clone(&state)).await;
    let client = HttpHistoricalClient::try_new(&url).expect("client builds");

    let cmd = SegmentLoadCommand::new("seg-Z", "ds-Z", "deepstore://Z/seg-Z");
    let initial = client.load_segment(cmd).await.expect("load");
    assert_eq!(initial.state, SegmentLoadState::Loading);
    assert_eq!(initial.segment_id, "seg-Z");

    // Poll until Loaded or budget exhausted.
    let mut last = SegmentLoadState::Loading;
    for _ in 0..100 {
        let table = client.load_status().await.expect("poll");
        if let Some(&s) = table.get("seg-Z") {
            last = s;
            if last == SegmentLoadState::Loaded {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(last, SegmentLoadState::Loaded);

    // Server-side snapshot agrees.
    let snap = state.snapshot().await;
    assert_eq!(snap.get("seg-Z"), Some(&SegmentLoadState::Loaded));
}

#[tokio::test]
async fn coordinator_to_historical_drop_marks_dropped() {
    let state = Arc::new(HistoricalServerState::with_config(
        "hist-itest",
        "default",
        Duration::from_millis(0),
    ));
    let url = spawn_historical(Arc::clone(&state)).await;
    let client = HttpHistoricalClient::try_new(&url).expect("client builds");

    let _ = client
        .load_segment(SegmentLoadCommand::new(
            "seg-D",
            "ds-D",
            "deepstore://D/seg-D",
        ))
        .await
        .expect("load");
    let drop_resp = client
        .drop_segment(SegmentDropCommand::new("seg-D"))
        .await
        .expect("drop");
    assert_eq!(drop_resp.state, SegmentLoadState::Dropped);

    let table = client.load_status().await.expect("status");
    assert_eq!(table.get("seg-D"), Some(&SegmentLoadState::Dropped));
}

#[tokio::test]
async fn broker_to_historical_handles_concurrent_scatter_without_id_collision() {
    let state = Arc::new(HistoricalServerState::default());
    let url = spawn_historical(Arc::clone(&state)).await;
    let client = HttpHistoricalClient::try_new(&url).expect("client builds");

    let handles: Vec<_> = (0..16)
        .map(|i| {
            let c = client.clone();
            tokio::spawn(async move {
                let sql = format!("SELECT {i}");
                let seg = format!("seg-{i}");
                let resp = c
                    .scatter_query(SegmentQuery::new(&sql, &seg))
                    .await
                    .expect("scatter");
                (resp.segment_id, resp.rows[0][0].clone())
            })
        })
        .collect();

    let mut seen_ids = Vec::new();
    for h in handles {
        let (id, echoed) = h.await.expect("task joined");
        seen_ids.push(id);
        assert!(matches!(echoed, serde_json::Value::String(_)));
    }
    seen_ids.sort();
    let unique = {
        let mut copy = seen_ids.clone();
        copy.dedup();
        copy.len()
    };
    assert_eq!(
        unique, 16,
        "every concurrent scatter needs its own segment_id echo"
    );
}

#[tokio::test]
async fn historical_loadstatus_starts_empty() {
    let state = Arc::new(HistoricalServerState::default());
    let url = spawn_historical(state).await;
    let client = HttpHistoricalClient::try_new(&url).expect("client builds");

    let table = client.load_status().await.expect("status");
    assert!(table.is_empty(), "fresh historical has no segments tracked");
}

#[tokio::test]
async fn full_4_of_4_cross_role_loop_load_then_scatter() {
    // Wave 40.LL milestone test: spawn a historical, have a
    // "coordinator" load a segment, then have a "broker" scatter a
    // query against that segment. Exercises both new cross-role
    // flows on a single fresh historical via real loopback HTTP.
    let state = Arc::new(HistoricalServerState::with_config(
        "hist-fullloop",
        "default",
        Duration::from_millis(0),
    ));
    let url = spawn_historical(Arc::clone(&state)).await;
    let coord_client = HttpHistoricalClient::try_new(&url).expect("client");
    let broker_client = HttpHistoricalClient::try_new(&url).expect("client");

    // 1. Coordinator loads a segment.
    let load = coord_client
        .load_segment(SegmentLoadCommand::new(
            "seg-loop",
            "ds-loop",
            "deepstore://loop/seg-loop",
        ))
        .await
        .expect("load");
    assert_eq!(load.segment_id, "seg-loop");

    // Wait briefly for Loading→Loaded with 0 ms delay (still need to
    // yield back to the spawned task that flips state).
    for _ in 0..50 {
        let table = coord_client.load_status().await.expect("status");
        if table.get("seg-loop") == Some(&SegmentLoadState::Loaded) {
            break;
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    // 2. Broker scatters a query against the loaded segment.
    let scatter = broker_client
        .scatter_query(SegmentQuery::new("SELECT count(*)", "seg-loop"))
        .await
        .expect("scatter");
    assert_eq!(scatter.segment_id, "seg-loop");
    assert_eq!(
        scatter.rows[0][0],
        serde_json::Value::String("SELECT count(*)".into())
    );

    // 3. Server-side snapshot confirms the segment is tracked.
    let snap = state.snapshot().await;
    assert!(
        snap.contains_key("seg-loop"),
        "historical knows about loaded segment"
    );
}

// =========================================================================
// Wave 41.OO — real loader + real native query execution
// =========================================================================

#[tokio::test]
async fn wave_41_real_loader_then_timeseries_query_returns_real_aggregate() {
    use ferrodruid_rpc::native_query::{Aggregation, NativeQuery, TimeseriesSpec};

    // Build a deep-storage root with a fixture segment artifact.
    let dir = tempfile::tempdir().expect("tempdir");
    let seg_dir = dir.path().join("wikipedia").join("wiki_v0_0");
    tokio::fs::create_dir_all(&seg_dir).await.expect("mkdir");
    let artifact = r#"{"segmentId":"wiki_v0_0","dataSource":"wikipedia","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"},{"name":"count","type":"long"}]}
{"__time":1714694400000,"page":"home","count":3}
{"__time":1714694460000,"page":"home","count":2}
{"__time":1714694520000,"page":"about","count":1}
{"__time":1714694580000,"page":"home","count":5}
"#;
    tokio::fs::write(seg_dir.join("segment.jsonl"), artifact)
        .await
        .expect("write");

    // Spawn a real-loader historical pointed at our tempdir.
    let state = Arc::new(HistoricalServerState::with_root(
        "hist-w41",
        "default",
        Duration::from_millis(0),
        dir.path().to_path_buf(),
    ));
    let url = spawn_historical(Arc::clone(&state)).await;
    let client = HttpHistoricalClient::try_new(&url).expect("client");

    // Coordinator load.
    client
        .load_segment(SegmentLoadCommand::new(
            "wiki_v0_0",
            "wikipedia",
            "deepstore://wikipedia/wiki_v0_0",
        ))
        .await
        .expect("load");

    // Wait for Loaded.
    for _ in 0..50 {
        let table = client.load_status().await.expect("status");
        if table.get("wiki_v0_0") == Some(&SegmentLoadState::Loaded) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let table = client.load_status().await.expect("status");
    assert_eq!(table.get("wiki_v0_0"), Some(&SegmentLoadState::Loaded));

    // Now run a native timeseries query against the loaded segment.
    let query = NativeQuery::Timeseries(TimeseriesSpec {
        data_source: "wikipedia".into(),
        granularity_ms: 0,
        aggregations: vec![Aggregation::LongSum {
            name: "total".into(),
            field_name: "count".into(),
        }],
        filter: None,
        intervals: Vec::new(),
    });
    let resp = client
        .native_scatter("wiki_v0_0", &query)
        .await
        .expect("native scatter");
    assert_eq!(resp.segment_id, "wiki_v0_0");
    assert_eq!(resp.rows.len(), 1, "single bucket for granularity=0");
    let bucket_obj = resp.rows[0][1].as_object().expect("result obj");
    assert_eq!(
        bucket_obj.get("total"),
        Some(&serde_json::json!(11)),
        "3+2+1+5=11"
    );
}

#[tokio::test]
async fn wave_41_real_loader_failed_state_when_artifact_missing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = Arc::new(HistoricalServerState::with_root(
        "hist-w41-fail",
        "default",
        Duration::from_millis(0),
        dir.path().to_path_buf(),
    ));
    let url = spawn_historical(Arc::clone(&state)).await;
    let client = HttpHistoricalClient::try_new(&url).expect("client");

    client
        .load_segment(SegmentLoadCommand::new("missing", "ds-x", "deepstore://x"))
        .await
        .expect("load");

    // Wait for Failed.
    let mut last = SegmentLoadState::Loading;
    for _ in 0..50 {
        let table = client.load_status().await.expect("status");
        if let Some(&s) = table.get("missing") {
            last = s;
            if last == SegmentLoadState::Failed {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(last, SegmentLoadState::Failed);
}

#[tokio::test]
async fn wave_41_native_scan_query_returns_filtered_rows() {
    use ferrodruid_rpc::native_query::{EqualsFilter, NativeQuery, ScanSpec};

    let dir = tempfile::tempdir().expect("tempdir");
    let seg_dir = dir.path().join("ds-scan").join("seg-scan");
    tokio::fs::create_dir_all(&seg_dir).await.expect("mkdir");
    let artifact = r#"{"segmentId":"seg-scan","dataSource":"ds-scan","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"}]}
{"__time":1,"page":"home"}
{"__time":2,"page":"about"}
{"__time":3,"page":"home"}
{"__time":4,"page":"kb"}
"#;
    tokio::fs::write(seg_dir.join("segment.jsonl"), artifact)
        .await
        .expect("write");

    let state = Arc::new(HistoricalServerState::with_root(
        "hist-scan",
        "default",
        Duration::from_millis(0),
        dir.path().to_path_buf(),
    ));
    let url = spawn_historical(Arc::clone(&state)).await;
    let client = HttpHistoricalClient::try_new(&url).expect("client");

    client
        .load_segment(SegmentLoadCommand::new(
            "seg-scan",
            "ds-scan",
            "deepstore://x",
        ))
        .await
        .expect("load");
    for _ in 0..50 {
        let t = client.load_status().await.expect("st");
        if t.get("seg-scan") == Some(&SegmentLoadState::Loaded) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let query = NativeQuery::Scan(ScanSpec {
        data_source: "ds-scan".into(),
        columns: Some(vec!["page".into()]),
        limit: None,
        filter: Some(EqualsFilter {
            dimension: "page".into(),
            value: "home".into(),
        }),
    });
    let resp = client
        .native_scatter("seg-scan", &query)
        .await
        .expect("scan");
    assert_eq!(resp.rows.len(), 2);
    for row in &resp.rows {
        let obj = row[0].as_object().expect("row obj");
        assert_eq!(obj.get("page"), Some(&serde_json::json!("home")));
        assert!(!obj.contains_key("__time"), "projection drops __time");
    }
}
