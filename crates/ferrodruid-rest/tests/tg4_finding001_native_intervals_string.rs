// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! TG-4-finding-001 — REST-level regression: the native query
//! `intervals` field must accept both the single ISO `"start/end"`
//! string form (as documented by Apache Druid and emitted by pydruid /
//! the `druid` Python client by default) and the array-of-strings form.
//!
//! Pre-fix, FerroDruid rejected the single-string shape with
//! `HTTP 400 "invalid type: string, expected a sequence"` — the W2-D
//! pydruid hello-query suite had to monkey-patch its calls to use the
//! array form. This test posts both shapes to `/druid/v2` over an
//! ingested wikipedia segment and asserts identical row counts.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ferrodruid_auth::AuthStore;
use ferrodruid_authz::Authorizer;
use ferrodruid_broker::Broker;
use ferrodruid_coordinator::Coordinator;
use ferrodruid_historical::Historical;
use ferrodruid_ingest_batch::BatchIngester;
use ferrodruid_metadata::{MetadataStore, SegmentMetadataRow};
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_rest::{AppState, create_router};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn setup_app() -> Router {
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("create metadata store");
    metadata.initialize().await.expect("initialize schema");
    let metadata = Arc::new(metadata);

    let coordinator = Arc::new(Coordinator::new(Arc::clone(&metadata)));
    let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));

    let ingester = BatchIngester::new(
        "wikipedia_compat".to_string(),
        "__time".to_string(),
        vec!["page".to_string()],
        vec![json!({"type": "longSum", "name": "added"})],
    );
    let rows = vec![
        json!({"__time":"2024-01-01T00:00:00Z","page":"Main_Page","added":100}),
        json!({"__time":"2024-01-01T01:00:00Z","page":"Accueil","added":200}),
        json!({"__time":"2024-01-02T00:00:00Z","page":"Hauptseite","added":150}),
    ];
    let ingested = ingester.ingest(rows).expect("ingest");
    assert_eq!(ingested.num_rows, 3);

    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 100_000_000);
    let segment_id = "wikipedia_compat_2024-01-01T00:00:00.000Z_2024-01-04T00:00:00.000Z_v1_0";
    historical
        .load_segment(segment_id, ingested.segment_data)
        .expect("load segment");
    historical
        .set_segment_datasource(segment_id, "wikipedia_compat")
        .expect("set datasource");
    let historical = Arc::new(historical);

    let seg_row = SegmentMetadataRow {
        id: segment_id.to_string(),
        data_source: "wikipedia_compat".to_string(),
        created_date: "2024-01-01T00:00:00.000Z".to_string(),
        start: "2024-01-01T00:00:00.000Z".to_string(),
        end: "2024-01-04T00:00:00.000Z".to_string(),
        version: "v1".to_string(),
        used: true,
        payload: json!({
            "dataSource": "wikipedia_compat",
            "interval": "2024-01-01/2024-01-04"
        }),
    };
    metadata
        .insert_segment(&seg_row)
        .await
        .expect("insert segment metadata");

    let auth_store = Arc::new(parking_lot::RwLock::new(AuthStore::new()));
    let authorizer = Arc::new(Authorizer::new().with_admin_role());
    let broker = Arc::new(Broker::new());

    let state = Arc::new(AppState {
        coordinator,
        overlord,
        metadata,
        auth_store,
        auth_cred_dir: None,
        authorizer,
        auth_enabled: false,
        broker,
        historicals: vec![historical],
        start_time: chrono::Utc::now(),
        lookup_manager: Arc::new(ferrodruid_lookup::LookupManager::new()),
        metrics: Arc::new(ferrodruid_telemetry::Metrics::new()),
        msq_manager: Arc::new(MsqManager::new()),
        rate_limit_max_concurrent: 0,
    });

    std::mem::forget(cache_dir);
    create_router(state)
}

async fn post_native(app: Router, query: Value) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/v2")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&query).expect("serialize query"),
                ))
                .expect("build request"),
        )
        .await
        .expect("send request");
    let status = response.status();
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: Value = serde_json::from_slice(&body_bytes).expect("parse json");
    (status, json)
}

/// pydruid's default `query.timeseries(...)` call emits this shape with
/// `intervals` as a single ISO string. Apache Druid accepts it; pre-fix
/// FerroDruid rejected with HTTP 400 "expected a sequence".
#[tokio::test]
async fn tg4_f001_native_timeseries_intervals_string_accepted() {
    let q = json!({
        "queryType": "timeseries",
        "dataSource": "wikipedia_compat",
        "intervals": "2024-01-01/2024-01-04",
        "granularity": "all",
        "aggregations": [{"type": "longSum", "name": "total", "fieldName": "added"}]
    });
    let (status, body) = post_native(setup_app().await, q).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "single-string intervals must be accepted; body = {body}"
    );
}

#[tokio::test]
async fn tg4_f001_native_timeseries_string_and_array_match() {
    let q_string = json!({
        "queryType": "timeseries",
        "dataSource": "wikipedia_compat",
        "intervals": "2024-01-01/2024-01-04",
        "granularity": "all",
        "aggregations": [{"type": "longSum", "name": "total", "fieldName": "added"}]
    });
    let q_array = json!({
        "queryType": "timeseries",
        "dataSource": "wikipedia_compat",
        "intervals": ["2024-01-01/2024-01-04"],
        "granularity": "all",
        "aggregations": [{"type": "longSum", "name": "total", "fieldName": "added"}]
    });
    let (s1, b1) = post_native(setup_app().await, q_string).await;
    let (s2, b2) = post_native(setup_app().await, q_array).await;
    assert_eq!(s1, StatusCode::OK, "string form body: {b1}");
    assert_eq!(s2, StatusCode::OK, "array form body: {b2}");
    assert_eq!(
        b1, b2,
        "string and array intervals must return identical bodies; string={b1} array={b2}"
    );
}

#[tokio::test]
async fn tg4_f001_native_groupby_intervals_string_accepted() {
    let q = json!({
        "queryType": "groupBy",
        "dataSource": "wikipedia_compat",
        "intervals": "2024-01-01/2024-01-04",
        "granularity": "all",
        "dimensions": ["page"],
        "aggregations": [{"type": "longSum", "name": "total", "fieldName": "added"}]
    });
    let (status, body) = post_native(setup_app().await, q).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "groupBy single-string intervals must be accepted; body = {body}"
    );
}

#[tokio::test]
async fn tg4_f001_native_scan_intervals_string_accepted() {
    let q = json!({
        "queryType": "scan",
        "dataSource": "wikipedia_compat",
        "intervals": "2024-01-01/2024-01-04",
    });
    let (status, body) = post_native(setup_app().await, q).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "scan single-string intervals must be accepted; body = {body}"
    );
}
