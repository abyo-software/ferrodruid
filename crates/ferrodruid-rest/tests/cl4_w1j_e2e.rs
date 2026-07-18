// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! CL-4 / W1-J — REST-level end-to-end tests for the findings that
//! W1-G's docker harness surfaced against the W1-D matrix.
//!
//! Bar (W1-J prompt § per-finding):
//!   * Finding A (NTILE / CUME_DIST / PERCENT_RANK): ≥ 1 e2e per
//!     function over the live REST `/druid/v2/sql` happy path,
//!     proving the parser+planner+executor wiring closed by W1-D
//!     actually returns deep-equal rows.
//!   * Finding D (JOIN inline VALUES + CTE single-level):
//!     fail-closed contract — silent-drop is the worst possible
//!     outcome, so the SQL endpoint MUST return a clear error
//!     instead of the un-joined / un-grouped base rows it used to
//!     emit pre-W1-J.  Two tests (one JOIN, one CTE) assert the
//!     HTTP status + Druid error envelope shape.
//!
//! Parse + plan coverage for the same surface lives in
//! `crates/ferrodruid-sql/tests/cl4_calcite.rs`; this file
//! exercises the REST → SQL → planner → executor pipeline
//! end-to-end through the in-process Axum router.

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

// ---------------------------------------------------------------------------
// Dataset shared by every W1-J test.
//
// 10 wikipedia-style rows mirroring the harness `wikipedia_compat`
// ingestion spec (`tests/druid-compat/sample_ingestion_spec.json`):
// `__time` / `page` / `user` / `language` / `city` / `namespace` /
// `channel` / `added` / `deleted` / `delta`.  Keeping the schema
// identical to the docker harness means a tester can rerun the same
// SQL against either backend and read the diff directly.
// ---------------------------------------------------------------------------

fn build_wikipedia_compat_rows() -> Vec<Value> {
    vec![
        json!({"__time":"2024-01-01T00:00:00Z","page":"Main_Page","user":"Alice","language":"en","city":"London","namespace":"Main","channel":"#en.wikipedia","added":100,"deleted":5,"delta":95}),
        json!({"__time":"2024-01-01T01:00:00Z","page":"Talk:Main_Page","user":"Bob","language":"en","city":"London","namespace":"Talk","channel":"#en.wikipedia","added":50,"deleted":2,"delta":48}),
        json!({"__time":"2024-01-01T02:00:00Z","page":"Accueil","user":"Claude","language":"fr","city":"Paris","namespace":"Main","channel":"#fr.wikipedia","added":200,"deleted":10,"delta":190}),
        json!({"__time":"2024-01-01T03:00:00Z","page":"Hauptseite","user":"Diana","language":"de","city":"Berlin","namespace":"Main","channel":"#de.wikipedia","added":150,"deleted":7,"delta":143}),
        json!({"__time":"2024-01-01T12:00:00Z","page":"Main_Page","user":"Edward","language":"en","city":"NYC","namespace":"Main","channel":"#en.wikipedia","added":75,"deleted":3,"delta":72}),
        json!({"__time":"2024-01-02T00:00:00Z","page":"Main_Page","user":"Frank","language":"en","city":"London","namespace":"Main","channel":"#en.wikipedia","added":120,"deleted":6,"delta":114}),
        json!({"__time":"2024-01-02T12:00:00Z","page":"Accueil","user":"Claude","language":"fr","city":"Paris","namespace":"Main","channel":"#fr.wikipedia","added":180,"deleted":9,"delta":171}),
        json!({"__time":"2024-01-03T00:00:00Z","page":"Main_Page","user":"Alice","language":"en","city":"London","namespace":"Main","channel":"#en.wikipedia","added":90,"deleted":4,"delta":86}),
        json!({"__time":"2024-01-03T08:00:00Z","page":"Pagina_principale","user":"Heidi","language":"it","city":"Rome","namespace":"Main","channel":"#it.wikipedia","added":110,"deleted":5,"delta":105}),
        json!({"__time":"2024-01-03T09:00:00Z","page":"Portal:Current_events","user":"Frank","language":"en","city":"NYC","namespace":"Portal","channel":"#en.wikipedia","added":300,"deleted":12,"delta":288}),
    ]
}

// ---------------------------------------------------------------------------
// Test app setup
// ---------------------------------------------------------------------------

async fn setup_w1j_app() -> Router {
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
        vec![
            "page".to_string(),
            "user".to_string(),
            "language".to_string(),
            "city".to_string(),
            "namespace".to_string(),
            "channel".to_string(),
        ],
        vec![
            json!({"type": "longSum", "name": "added"}),
            json!({"type": "longSum", "name": "deleted"}),
            json!({"type": "longSum", "name": "delta"}),
        ],
    );
    let ingested = ingester
        .ingest(build_wikipedia_compat_rows())
        .expect("ingest wikipedia_compat rows");
    assert_eq!(ingested.num_rows, 10);

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

// ===========================================================================
// Finding A — NTILE / CUME_DIST / PERCENT_RANK end-to-end via REST.
//
// The W1-G v35 / v36 harness reported all three as `ferro-fail
// (Unsupported window function: ...)`, contradicting the W1-D matrix
// which claimed 100% support.  The parser, planner, and
// `crates/ferrodruid-query/src/window.rs` executor all carry the
// corresponding `WindowFunctionType::{Ntile, CumeDist, PercentRank}`
// variants; this test proves the wiring is intact end-to-end through
// the live SQL endpoint so the harness re-run actually flips them to
// `deep`.
// ===========================================================================

/// Superset's Druid engine `do_ping()` connection health check issues
/// `SELECT 1`. It must succeed over `/druid/v2/sql` with a single synthetic
/// row so "Test Connection" passes natively (no ORM workaround). This drives
/// the REST → parser → constant-SELECT materialisation path end-to-end.
#[tokio::test]
async fn constant_select_ping_returns_synthetic_row() {
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(app, "SELECT 1").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "SELECT 1 must succeed; body = {body}"
    );
    let rows = body.as_array().expect("array body");
    assert_eq!(
        rows.len(),
        1,
        "SELECT 1 yields exactly one row; body = {body}"
    );
    assert_eq!(
        rows[0].get("EXPR$0").and_then(Value::as_i64),
        Some(1),
        "unaliased literal is named EXPR$0 = 1; body = {body}"
    );

    // Aliased multi-column ping.
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(app, "SELECT 1 AS ping, 'ok' AS status").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "aliased ping must succeed; body = {body}"
    );
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("ping").and_then(Value::as_i64), Some(1));
    assert_eq!(rows[0].get("status").and_then(Value::as_str), Some("ok"));
}

/// INFORMATION_SCHEMA metadata introspection — the pydruid SQLAlchemy dialect
/// queries these to populate Superset's dataset picker (`get_table_names`),
/// column sync (`get_columns`), and schema list (`get_schema_names`). They must
/// return the live datasource + column signature as flat SQL rows.
#[tokio::test]
async fn information_schema_introspection_end_to_end() {
    // get_schema_names
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(app, "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "SCHEMATA must succeed; body = {body}"
    );
    let schemas: Vec<&str> = body
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|r| r.get("SCHEMA_NAME").and_then(Value::as_str))
        .collect();
    assert!(schemas.contains(&"druid"), "schemas = {schemas:?}");

    // get_table_names
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(app, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES").await;
    assert_eq!(status, StatusCode::OK, "TABLES must succeed; body = {body}");
    let tables: Vec<&str> = body
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|r| r.get("TABLE_NAME").and_then(Value::as_str))
        .collect();
    assert!(tables.contains(&"wikipedia_compat"), "tables = {tables:?}");

    // get_columns — the exact pydruid query shape (projection + WHERE filter).
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COLUMN_NAME, JDBC_TYPE, IS_NULLABLE, COLUMN_DEFAULT \
         FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'wikipedia_compat'",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "COLUMNS must succeed; body = {body}"
    );
    let cols: Vec<(&str, i64)> = body
        .as_array()
        .expect("array")
        .iter()
        .map(|r| {
            (
                r.get("COLUMN_NAME").and_then(Value::as_str).unwrap_or(""),
                r.get("JDBC_TYPE").and_then(Value::as_i64).unwrap_or(0),
            )
        })
        .collect();
    let names: Vec<&str> = cols.iter().map(|(n, _)| *n).collect();
    // Dimensions + metrics from the wikipedia_compat ingest + __time.
    assert!(names.contains(&"__time"), "cols = {cols:?}");
    assert!(names.contains(&"language"), "cols = {cols:?}");
    assert!(names.contains(&"added"), "cols = {cols:?}");
    // __time is TIMESTAMP (JDBC 93); a string dim is VARCHAR (12).
    assert_eq!(
        cols.iter().find(|(n, _)| *n == "__time").map(|(_, t)| *t),
        Some(93)
    );
    assert_eq!(
        cols.iter().find(|(n, _)| *n == "language").map(|(_, t)| *t),
        Some(12)
    );

    // Null-semantics T6 — Druid SORTS INFORMATION_SCHEMA results, so
    // `ORDER BY COLUMN_NAME` must return exactly sorted rows (live diff
    // 2026-07-11 showed FerroDruid returning ingestion-order rows).
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'wikipedia_compat' ORDER BY COLUMN_NAME",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "COLUMNS ORDER BY must succeed; body = {body}"
    );
    let ordered: Vec<String> = body
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|r| r.get("COLUMN_NAME").and_then(Value::as_str))
        .map(str::to_string)
        .collect();
    let mut expected = ordered.clone();
    expected.sort();
    assert!(!ordered.is_empty(), "body = {body}");
    assert_eq!(
        ordered, expected,
        "INFORMATION_SCHEMA rows must be exactly sorted by COLUMN_NAME"
    );
    assert_eq!(
        ordered.first().map(String::as_str),
        Some("__time"),
        "`__time` sorts first lexicographically; body = {body}"
    );

    // DESC + LIMIT: the limit applies AFTER the sort (top-of-sorted rows).
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'wikipedia_compat' ORDER BY COLUMN_NAME DESC LIMIT 2",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let desc_rows: Vec<String> = body
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|r| r.get("COLUMN_NAME").and_then(Value::as_str))
        .map(str::to_string)
        .collect();
    let mut all_desc = ordered.clone();
    all_desc.sort_by(|a, b| b.cmp(a));
    assert_eq!(
        desc_rows,
        all_desc.into_iter().take(2).collect::<Vec<_>>(),
        "ORDER BY ... DESC LIMIT must sort first, then limit; body = {body}"
    );
}

/// codex QA r15 (Medium) — INFORMATION_SCHEMA ORDER BY must resolve keys in
/// the OUTPUT (SELECT alias) namespace the projected rows carry. Pre-fix,
/// `extract_post_sort` resolved `ORDER BY c` back to the RAW column
/// (`COLUMN_NAME`), but the rows are keyed by the alias `c`, so the
/// comparator saw only missing keys and the stable sort left ingestion
/// order on the wire.
#[tokio::test]
async fn information_schema_order_by_select_alias_end_to_end() {
    // The exact wikipedia_compat column set, sorted ascending — the ground
    // truth every shape below must reproduce.
    let sorted_cols: Vec<&str> = vec![
        "__time",
        "added",
        "channel",
        "city",
        "deleted",
        "delta",
        "language",
        "namespace",
        "page",
        "user",
    ];
    let col_values = |body: &Value, key: &str| -> Vec<String> {
        body.as_array()
            .expect("array body")
            .iter()
            .filter_map(|r| r.get(key).and_then(Value::as_str))
            .map(str::to_string)
            .collect()
    };

    // 1. ORDER BY the SELECT alias.
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COLUMN_NAME AS c FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'wikipedia_compat' ORDER BY c",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        col_values(&body, "c"),
        sorted_cols,
        "ORDER BY alias must sort the aliased rows; body = {body}"
    );

    // 2. ORDER BY alias DESC honors direction.
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COLUMN_NAME AS c FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'wikipedia_compat' ORDER BY c DESC LIMIT 3",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        col_values(&body, "c"),
        vec!["user", "page", "namespace"],
        "ORDER BY alias DESC must sort descending before LIMIT; body = {body}"
    );

    // 3. Ordinal ORDER BY over an aliased projection.
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COLUMN_NAME AS c FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'wikipedia_compat' ORDER BY 1",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        col_values(&body, "c"),
        sorted_cols,
        "ordinal ORDER BY must resolve to the aliased output column; body = {body}"
    );

    // 4. ORDER BY the RAW column name while the projection is aliased
    //    (Calcite accepts this too; the key must map onto the alias).
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COLUMN_NAME AS c FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'wikipedia_compat' ORDER BY COLUMN_NAME",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        col_values(&body, "c"),
        sorted_cols,
        "ORDER BY the underlying column of an aliased projection; body = {body}"
    );

    // 5. Multi-key: aliased TABLE_NAME asc, aliased COLUMN_NAME desc.
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT TABLE_NAME AS t, COLUMN_NAME AS c FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'wikipedia_compat' ORDER BY t ASC, c DESC",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let mut desc_expected: Vec<String> = sorted_cols.iter().map(|s| (*s).to_string()).collect();
    desc_expected.reverse();
    assert_eq!(
        col_values(&body, "c"),
        desc_expected,
        "multi-key aliased ORDER BY must honor per-key direction; body = {body}"
    );
}

/// S-3 Superset dataset-creation + query surface: (1) `has_table`'s
/// `COUNT(*) > 0` existence probe over INFORMATION_SCHEMA, and (2) a
/// schema-qualified `"druid"."table"` reference (Superset emits both). Both
/// previously failed (the first un-projectable, the second an unknown
/// datasource → empty).
#[tokio::test]
async fn superset_has_table_and_schema_qualified_end_to_end() {
    // has_table: COUNT(*) > 0 over INFORMATION_SCHEMA.TABLES.
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COUNT(*) > 0 AS exists_ FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_NAME = 'wikipedia_compat'",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "has_table must succeed; body = {body}"
    );
    assert_eq!(
        body.as_array()
            .and_then(|r| r.first())
            .and_then(|r| r.get("exists_")),
        Some(&Value::Bool(true)),
        "existing table must report exists_=true; body = {body}"
    );

    // has_table for a missing table -> false.
    let app = setup_w1j_app().await;
    let (_s, body) = post_sql(
        app,
        "SELECT COUNT(*) > 0 AS exists_ FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_NAME = 'does_not_exist'",
    )
    .await;
    assert_eq!(
        body.as_array()
            .and_then(|r| r.first())
            .and_then(|r| r.get("exists_")),
        Some(&Value::Bool(false)),
        "missing table must report exists_=false; body = {body}"
    );

    // Schema-qualified `"druid"."wikipedia_compat"` resolves to the datasource.
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COUNT(*) AS c FROM \"druid\".\"wikipedia_compat\"",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "schema-qualified must succeed; body = {body}"
    );
    assert_eq!(
        body.as_array()
            .and_then(|r| r.first())
            .and_then(|r| r.get("c"))
            .and_then(Value::as_i64),
        Some(10),
        "druid.wikipedia_compat must resolve to 10 rows; body = {body}"
    );
}

/// Superset time-series charts issue `SELECT TIME_FLOOR(__time, 'PT…') AS t,
/// <agg> … GROUP BY 1`. The result must carry the bucket timestamp under the
/// alias (`t`) and, per SQL GROUP BY semantics, contain no empty buckets.
#[tokio::test]
async fn time_floor_time_series_end_to_end() {
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT TIME_FLOOR(__time, 'PT1H') AS t, COUNT(*) AS c \
         FROM wikipedia_compat GROUP BY 1 ORDER BY 1",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "TIME_FLOOR must succeed; body = {body}"
    );
    let rows = body.as_array().expect("array body");
    assert!(!rows.is_empty(), "must have buckets; body = {body}");
    for row in rows {
        // Every row carries the aliased time column (ISO-8601 string, matching
        // Druid's TIMESTAMP wire shape) and the count.
        assert!(
            row.get("t").and_then(Value::as_str).is_some(),
            "row missing time column `t`: {row}"
        );
        let c = row.get("c").and_then(Value::as_i64).expect("count");
        // SQL GROUP BY has no empty groups — skipEmptyBuckets must be honoured.
        assert!(c > 0, "empty bucket leaked (c=0): {row}");
    }
    // Total count across buckets equals the 10 ingested rows.
    let total: i64 = rows
        .iter()
        .filter_map(|r| r.get("c").and_then(Value::as_i64))
        .sum();
    assert_eq!(total, 10, "bucket counts must sum to 10; body = {body}");
}

/// DATE_TRUNC('hour', __time) lowers to the same TIME_FLOOR path end-to-end.
#[tokio::test]
async fn date_trunc_time_series_end_to_end() {
    let app = setup_w1j_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT DATE_TRUNC('hour', __time) AS t, COUNT(*) AS c \
         FROM wikipedia_compat GROUP BY 1 ORDER BY 1",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "DATE_TRUNC must succeed; body = {body}"
    );
    let rows = body.as_array().expect("array body");
    let total: i64 = rows
        .iter()
        .filter_map(|r| r.get("c").and_then(Value::as_i64))
        .sum();
    assert_eq!(
        total, 10,
        "DATE_TRUNC bucket counts must sum to 10; body = {body}"
    );
    assert!(rows.iter().all(|r| r.get("t").is_some()));
}

#[tokio::test]
async fn cl4_w1j_finding_a_ntile_end_to_end() {
    let app = setup_w1j_app().await;
    let sql = "SELECT \"language\", \"page\", \"added\", \
               NTILE(4) OVER (PARTITION BY \"language\" ORDER BY \"added\" ASC, \"page\" ASC) \
               AS quartile FROM wikipedia_compat \
               ORDER BY \"language\", \"added\" ASC, \"page\"";
    let (status, body) = post_sql(app, sql).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "NTILE OVER must execute end-to-end via /druid/v2/sql; body = {body}"
    );
    let rows = body.as_array().expect("array body");
    assert_eq!(
        rows.len(),
        10,
        "10 input rows ⇒ 10 output rows; body = {body}"
    );
    // Every row must carry the `quartile` column populated by the
    // executor — anything else proves the window projection was
    // dropped on the SQL→native dispatch.
    for row in rows {
        let q = row
            .get("quartile")
            .and_then(Value::as_i64)
            .unwrap_or_else(|| panic!("missing/non-int quartile in row: {row}"));
        assert!(
            (1..=4).contains(&q),
            "NTILE(4) must produce a tile in 1..=4, got {q} in row {row}"
        );
    }
    // The English partition has 6 rows ⇒ buckets of size 2/2/1/1.
    let en_quartiles: Vec<i64> = rows
        .iter()
        .filter(|r| r.get("language") == Some(&json!("en")))
        .filter_map(|r| r.get("quartile").and_then(Value::as_i64))
        .collect();
    assert_eq!(en_quartiles, vec![1, 1, 2, 2, 3, 4]);
}

#[tokio::test]
async fn cl4_w1j_finding_a_cume_dist_end_to_end() {
    let app = setup_w1j_app().await;
    let sql = "SELECT \"language\", \"page\", \"added\", \
               CUME_DIST() OVER (PARTITION BY \"language\" ORDER BY \"added\" ASC, \"page\" ASC) \
               AS cd FROM wikipedia_compat \
               ORDER BY \"language\", \"added\" ASC, \"page\"";
    let (status, body) = post_sql(app, sql).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "CUME_DIST OVER must execute end-to-end; body = {body}"
    );
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 10);
    for row in rows {
        let cd = row
            .get("cd")
            .and_then(Value::as_f64)
            .unwrap_or_else(|| panic!("missing/non-float cd in row: {row}"));
        assert!(
            (0.0..=1.0 + f64::EPSILON).contains(&cd),
            "CUME_DIST must be in (0, 1], got {cd} in row {row}"
        );
    }
    // The last (max-ordered) row in each partition must have cd == 1.0.
    let de_cd = rows
        .iter()
        .find(|r| r.get("language") == Some(&json!("de")))
        .and_then(|r| r.get("cd"))
        .and_then(Value::as_f64)
        .expect("de cd");
    assert!(
        (de_cd - 1.0).abs() < 1e-9,
        "single-row partition ⇒ cd=1.0, got {de_cd}"
    );
}

#[tokio::test]
async fn cl4_w1j_finding_a_percent_rank_end_to_end() {
    let app = setup_w1j_app().await;
    let sql = "SELECT \"language\", \"page\", \"added\", \
               PERCENT_RANK() OVER (PARTITION BY \"language\" ORDER BY \"added\" ASC, \"page\" ASC) \
               AS pr FROM wikipedia_compat \
               ORDER BY \"language\", \"added\" ASC, \"page\"";
    let (status, body) = post_sql(app, sql).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "PERCENT_RANK OVER must execute end-to-end; body = {body}"
    );
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 10);
    // PERCENT_RANK of the first row in each partition is 0.0.
    let first_of = |lang: &str| {
        rows.iter()
            .find(|r| r.get("language") == Some(&json!(lang)))
            .and_then(|r| r.get("pr"))
            .and_then(Value::as_f64)
            .unwrap_or_else(|| panic!("missing pr for first {lang} row"))
    };
    assert!(
        first_of("de").abs() < 1e-9,
        "single-row partition pr must be 0.0"
    );
    assert!(first_of("en").abs() < 1e-9, "en first row pr must be 0.0");
    assert!(first_of("fr").abs() < 1e-9, "fr first row pr must be 0.0");
}

// ===========================================================================
// Finding D — fail-closed for JOIN inline VALUES and CTE single-level.
//
// Pre-W1-J the SQL endpoint silently dropped the JOIN / CTE on the
// SQL→native dispatch in `handle_sql_query` and returned the
// un-joined / un-grouped base rows.  That's strictly worse than a
// hard error because clients see plausible-looking results.  W1-J
// adds an explicit fail-closed guard until the executor wires
// `PlannedQuery::joins` and `DataSource::Query` on the REST happy
// path.
//
// Bar: 2 e2e tests — one per surface — asserting HTTP
// `501 NOT_IMPLEMENTED` with the Druid-shaped error envelope.
// ===========================================================================

#[tokio::test]
async fn cl4_w1j_finding_d_join_inline_values_fails_closed() {
    let app = setup_w1j_app().await;
    let sql = "SELECT w.\"language\", c.label AS lang_label, COUNT(*) AS cnt \
               FROM wikipedia_compat w \
               INNER JOIN (VALUES ('en','English'),('fr','French')) AS c(code, label) \
                 ON w.\"language\" = c.code \
               GROUP BY w.\"language\", c.label \
               ORDER BY cnt DESC, w.\"language\"";
    let (status, body) = post_sql(app, sql).await;
    assert_eq!(
        status,
        StatusCode::NOT_IMPLEMENTED,
        "JOIN inline VALUES must fail closed pre-CL-4-R8; body = {body}"
    );
    assert_eq!(body["error"], json!("SQL planning error"));
    let msg = body["errorMessage"].as_str().expect("errorMessage");
    assert!(
        msg.contains("JOIN") && msg.contains("CL-4 / W1-J finding-D"),
        "fail-closed message must name JOIN + W1-J finding-D for grep-ability; got {msg}"
    );
}

#[tokio::test]
async fn cl4_w1j_finding_d_cte_single_level_fails_closed() {
    let app = setup_w1j_app().await;
    let sql = "WITH per_lang AS (\
                 SELECT \"language\", COUNT(*) AS cnt FROM wikipedia_compat GROUP BY \"language\"\
               ) SELECT \"language\", cnt FROM per_lang ORDER BY cnt DESC, \"language\"";
    let (status, body) = post_sql(app, sql).await;
    assert_eq!(
        status,
        StatusCode::NOT_IMPLEMENTED,
        "CTE single-level must fail closed pre-CL-4-R8; body = {body}"
    );
    assert_eq!(body["error"], json!("SQL planning error"));
    let msg = body["errorMessage"].as_str().expect("errorMessage");
    assert!(
        msg.contains("CTE") && msg.contains("CL-4 / W1-J finding-D"),
        "fail-closed message must name CTE + W1-J finding-D for grep-ability; got {msg}"
    );
}
