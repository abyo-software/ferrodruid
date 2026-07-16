// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! TG-4-finding-003 — REST-level regression: a double-quoted datasource
//! name in the SQL FROM clause must resolve to the same datasource as
//! the unquoted form. Pre-fix, `convert_table_ref` re-emitted the
//! sqlparser quote_style, so `FROM "wikipedia_compat"` propagated the
//! literal string `"wikipedia_compat"` (quotes included) into
//! `extract_datasource_name` / `build_schema_for`, which failed to
//! match any loaded segment and silently returned an empty result —
//! exactly the chart-render failure mode W2-D observed driving
//! Superset against FerroDruid HEAD `48b216d`.

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
        json!({"__time":"2024-01-01T02:00:00Z","page":"Hauptseite","added":150}),
    ];
    let ingested = ingester.ingest(rows).expect("ingest");
    assert_eq!(ingested.num_rows, 3);

    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 100_000_000);
    let segment_id = "wikipedia_compat_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_v1_0";
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
        end: "2024-01-02T00:00:00.000Z".to_string(),
        version: "v1".to_string(),
        used: true,
        payload: json!({
            "dataSource": "wikipedia_compat",
            "interval": "2024-01-01/2024-01-02"
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

async fn post_sql(app: Router, sql: &str) -> (StatusCode, Value) {
    let body = json!({ "query": sql });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/v2/sql")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
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

#[tokio::test]
async fn tg4_f003_quoted_from_resolves_same_as_unquoted() {
    // Unquoted form — Hello-Query baseline from W2-D Superset SQL Lab.
    let (status_unq, body_unq) = post_sql(
        setup_app().await,
        r#"SELECT "page", COUNT(*) AS cnt FROM wikipedia_compat GROUP BY "page""#,
    )
    .await;
    assert_eq!(status_unq, StatusCode::OK, "unquoted body: {body_unq}");
    let rows_unq = body_unq.as_array().expect("array body");
    assert_eq!(
        rows_unq.len(),
        3,
        "unquoted form must return 3 page rows, got {body_unq}"
    );

    // Quoted form — pre-fix returned `[]` silently because the planner
    // propagated `"wikipedia_compat"` (quotes included) into catalog
    // lookup. Post-fix it must match the unquoted form exactly.
    let (status_q, body_q) = post_sql(
        setup_app().await,
        r#"SELECT "page", COUNT(*) AS cnt FROM "wikipedia_compat" GROUP BY "page""#,
    )
    .await;
    assert_eq!(status_q, StatusCode::OK, "quoted body: {body_q}");
    let rows_q = body_q.as_array().expect("array body");
    assert_eq!(
        rows_q.len(),
        rows_unq.len(),
        "quoted FROM \"wikipedia_compat\" must match unquoted row count; \
         quoted={body_q} unquoted={body_unq}"
    );

    // The page set must be identical between the two forms — proves the
    // catalog lookup hit the same segment, not a structurally-similar
    // empty fallback.
    let pages_of = |rows: &[Value]| -> Vec<String> {
        let mut p: Vec<String> = rows
            .iter()
            .filter_map(|r| r.get("page").and_then(Value::as_str).map(String::from))
            .collect();
        p.sort();
        p
    };
    assert_eq!(pages_of(rows_unq), pages_of(rows_q));
}

#[tokio::test]
async fn tg4_f003_quoted_scan_resolves_same_as_unquoted() {
    let (status_unq, body_unq) =
        post_sql(setup_app().await, "SELECT \"page\" FROM wikipedia_compat").await;
    assert_eq!(status_unq, StatusCode::OK);
    let n_unq = body_unq.as_array().expect("array").len();
    assert_eq!(n_unq, 3, "unquoted scan must return 3 rows; got {body_unq}");

    let (status_q, body_q) = post_sql(
        setup_app().await,
        r#"SELECT "page" FROM "wikipedia_compat""#,
    )
    .await;
    assert_eq!(status_q, StatusCode::OK);
    let n_q = body_q.as_array().expect("array").len();
    assert_eq!(
        n_q, n_unq,
        "quoted scan must match unquoted row count; quoted={body_q}"
    );
}
