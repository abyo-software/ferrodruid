// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! SQL wire-row projection tests (`/druid/v2/sql`, resultFormat=object).
//!
//! Since AVG / arithmetic-over-aggregates lower to hidden `$`-prefixed
//! helper aggregators plus post-aggregations, the native result maps
//! carry (a) internal `$avg_sum_N` / `$avg_count_N` fields that Druid
//! never emits on the wire, and (b) post-aggregation values appended
//! LAST, after every plain aggregator — so the JSON document key order
//! diverges from the SELECT list. pydruid / Superset map result columns
//! to the SELECT list POSITIONALLY (document key order, which is why the
//! workspace enables serde_json `preserve_order`), so both defects are
//! wire-visible: hidden columns appear, and metric columns swap.
//!
//! These tests pin the fixed contract: SQL wire rows are projected to
//! the planner's `output_columns` — exactly those columns, in SELECT
//! order, with no `$`-prefixed leakage — across the GroupBy, TopN, and
//! Timeseries (TIME_FLOOR) native paths.

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
// Dataset: 5 telemetry rows across 2 sites / 2 days.
//
//   site_a: values 5, 10, 15  (day 1) → SUM 30, COUNT 3, AVG 10.0
//   site_b: values 20, 60     (day 2) → SUM 80, COUNT 2, AVG 40.0
//
// Distinct timestamps prevent ingest-time rollup from merging rows, so
// COUNT(*) counts the ingested rows.
// ---------------------------------------------------------------------------

fn build_telemetry_rows() -> Vec<Value> {
    vec![
        json!({"__time":"2024-01-01T00:00:00Z","site_id":"site_a","value":5}),
        json!({"__time":"2024-01-01T01:00:00Z","site_id":"site_a","value":10}),
        json!({"__time":"2024-01-01T02:00:00Z","site_id":"site_a","value":15}),
        json!({"__time":"2024-01-02T00:00:00Z","site_id":"site_b","value":20}),
        json!({"__time":"2024-01-02T01:00:00Z","site_id":"site_b","value":60}),
    ]
}

async fn setup_telemetry_app() -> Router {
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("create metadata store");
    metadata.initialize().await.expect("initialize schema");
    let metadata = Arc::new(metadata);

    let coordinator = Arc::new(Coordinator::new(Arc::clone(&metadata)));
    let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));

    let ingester = BatchIngester::new(
        "telemetry".to_string(),
        "__time".to_string(),
        vec!["site_id".to_string()],
        vec![json!({"type": "longSum", "name": "value"})],
    );
    let ingested = ingester
        .ingest(build_telemetry_rows())
        .expect("ingest telemetry rows");
    assert_eq!(ingested.num_rows, 5);

    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 100_000_000);
    let segment_id = "telemetry_2024-01-01T00:00:00.000Z_2024-01-03T00:00:00.000Z_v1_0";
    historical
        .load_segment(segment_id, ingested.segment_data)
        .expect("load segment");
    historical
        .set_segment_datasource(segment_id, "telemetry")
        .expect("set datasource");
    let historical = Arc::new(historical);

    let seg_row = SegmentMetadataRow {
        id: segment_id.to_string(),
        data_source: "telemetry".to_string(),
        created_date: "2024-01-01T00:00:00.000Z".to_string(),
        start: "2024-01-01T00:00:00.000Z".to_string(),
        end: "2024-01-03T00:00:00.000Z".to_string(),
        version: "v1".to_string(),
        used: true,
        payload: json!({
            "dataSource": "telemetry",
            "interval": "2024-01-01/2024-01-03"
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

/// App with a multi-metric datasource whose metricsSpec order is NOT
/// alphabetical (`count, added, deleted, delta` — the docker-harness
/// `wikipedia_compat` shape) and whose dimensionsSpec order is NOT
/// alphabetical either (`site_id, area`). Exercises the `SELECT *`
/// column-order contract.
async fn setup_multimetric_app() -> Router {
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("create metadata store");
    metadata.initialize().await.expect("initialize schema");
    let metadata = Arc::new(metadata);

    let coordinator = Arc::new(Coordinator::new(Arc::clone(&metadata)));
    let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));

    let ingester = BatchIngester::new(
        "factory".to_string(),
        "__time".to_string(),
        // Dimension spec order is NOT alphabetical (alphabetical would put
        // `area` first) — Druid keeps dimensions in spec order.
        vec!["site_id".to_string(), "area".to_string()],
        // Metric spec order is NOT alphabetical (`count` first) — Druid
        // re-orders metrics alphabetically in SELECT *.
        vec![
            json!({"type": "count", "name": "count"}),
            json!({"type": "longSum", "name": "added", "fieldName": "added"}),
            json!({"type": "longSum", "name": "deleted", "fieldName": "deleted"}),
            json!({"type": "longSum", "name": "delta", "fieldName": "delta"}),
        ],
    );
    let ingested = ingester
        .ingest(vec![
            json!({"__time":"2024-01-01T00:00:00Z","site_id":"s1","area":"north","added":100,"deleted":10,"delta":90}),
            json!({"__time":"2024-01-01T01:00:00Z","site_id":"s2","area":"south","added":50,"deleted":5,"delta":45}),
        ])
        .expect("ingest factory rows");
    assert_eq!(ingested.num_rows, 2);

    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 100_000_000);
    let segment_id = "factory_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_v1_0";
    historical
        .load_segment(segment_id, ingested.segment_data)
        .expect("load segment");
    historical
        .set_segment_datasource(segment_id, "factory")
        .expect("set datasource");
    let historical = Arc::new(historical);

    let seg_row = SegmentMetadataRow {
        id: segment_id.to_string(),
        data_source: "factory".to_string(),
        created_date: "2024-01-01T00:00:00.000Z".to_string(),
        start: "2024-01-01T00:00:00.000Z".to_string(),
        end: "2024-01-02T00:00:00.000Z".to_string(),
        version: "v1".to_string(),
        used: true,
        payload: json!({
            "dataSource": "factory",
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

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

/// Recursively assert that no object key anywhere in `v` starts with `$`
/// (internal helper aggregators must never reach the SQL wire).
fn assert_no_hidden_keys(v: &Value) {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                assert!(
                    !k.starts_with('$'),
                    "hidden internal column leaked onto the SQL wire: {k:?} in {v}"
                );
                assert_no_hidden_keys(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                assert_no_hidden_keys(item);
            }
        }
        _ => {}
    }
}

/// Document-order keys of a row object. With workspace-wide serde_json
/// `preserve_order`, `serde_json::Map` iteration order IS the JSON
/// document key order — i.e. exactly what pydruid/Superset see.
fn row_keys(row: &Value) -> Vec<&str> {
    row.as_object()
        .expect("row must be a JSON object")
        .keys()
        .map(String::as_str)
        .collect()
}

fn f64_field(row: &Value, key: &str) -> f64 {
    row.get(key)
        .and_then(Value::as_f64)
        .unwrap_or_else(|| panic!("field {key} must be numeric; row = {row}"))
}

// ---------------------------------------------------------------------------
// GroupBy path
// ---------------------------------------------------------------------------

/// `SELECT site_id, AVG(value) AS avg_v, COUNT(*) AS c … GROUP BY site_id`
/// must emit rows shaped exactly `{"site_id": …, "avg_v": …, "c": …}`:
/// no `$avg_sum_N` / `$avg_count_N` leakage, SELECT-list key order (the
/// positional contract Superset relies on), and AVG ≡ SUM/COUNT.
#[tokio::test]
async fn groupby_wire_rows_project_to_select_list() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id, AVG(value) AS avg_v, COUNT(*) AS c FROM telemetry GROUP BY site_id",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query must succeed; body = {body}");

    // (a) No hidden helper aggregators anywhere in the response.
    assert_no_hidden_keys(&body);

    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "one row per site; body = {body}");

    // (b) Document key order is exactly the SELECT list.
    for row in rows {
        assert_eq!(
            row_keys(row),
            vec!["site_id", "avg_v", "c"],
            "wire key order must match the SELECT list; row = {row}"
        );
    }

    // (c) AVG values numerically equal SUM/COUNT per site.
    let mut by_site = std::collections::HashMap::new();
    for row in rows {
        let site = row
            .get("site_id")
            .and_then(Value::as_str)
            .expect("site_id")
            .to_string();
        let avg = f64_field(row, "avg_v");
        let c = row.get("c").and_then(Value::as_i64).expect("count");
        by_site.insert(site, (avg, c));
    }
    let (avg_a, c_a) = by_site.get("site_a").expect("site_a row");
    let (avg_b, c_b) = by_site.get("site_b").expect("site_b row");
    assert!(
        (avg_a - 10.0).abs() < 1e-9,
        "site_a AVG = 30/3 = 10.0, got {avg_a}"
    );
    assert_eq!(*c_a, 3);
    assert!(
        (avg_b - 40.0).abs() < 1e-9,
        "site_b AVG = 80/2 = 40.0, got {avg_b}"
    );
    assert_eq!(*c_b, 2);

    // Druid emits AVG (a DOUBLE column) with a trailing `.0` — the
    // integer-collapse normalisation must not flatten `40.0` to `40`
    // for post-aggregation output columns.
    for row in rows {
        let v = row.get("avg_v").expect("avg_v present");
        assert!(
            v.as_f64().is_some() && !v.is_i64() && !v.is_u64(),
            "avg_v must stay a DOUBLE (trailing .0) on the wire, got {v:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// TopN path (single dimension + ORDER BY avg alias + LIMIT)
// ---------------------------------------------------------------------------

/// The same projection contract on the TopN-shaped lowering: single
/// dimension, ORDER BY the AVG alias DESC, LIMIT. Rows must be ranked by
/// the post-aggregation and carry only the SELECT-list columns, in order.
#[tokio::test]
async fn topn_wire_rows_project_to_select_list() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id, AVG(value) AS avg_v FROM telemetry \
         GROUP BY site_id ORDER BY avg_v DESC LIMIT 5",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query must succeed; body = {body}");

    assert_no_hidden_keys(&body);

    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "two sites; body = {body}");
    for row in rows {
        assert_eq!(
            row_keys(row),
            vec!["site_id", "avg_v"],
            "wire key order must match the SELECT list; row = {row}"
        );
    }

    // Ranked DESC by the AVG post-aggregation: site_b (40.0) first.
    assert_eq!(
        rows[0].get("site_id").and_then(Value::as_str),
        Some("site_b")
    );
    assert!((f64_field(&rows[0], "avg_v") - 40.0).abs() < 1e-9);
    assert_eq!(
        rows[1].get("site_id").and_then(Value::as_str),
        Some("site_a")
    );
    assert!((f64_field(&rows[1], "avg_v") - 10.0).abs() < 1e-9);
}

// ---------------------------------------------------------------------------
// Timeseries path (TIME_FLOOR bucket)
// ---------------------------------------------------------------------------

/// TIME_FLOOR GROUP BY lowers to a Timeseries; the bucket timestamp must
/// stay an ISO-8601 string surfaced under the SQL alias, positioned by
/// the SELECT list (first here), with the same strict projection applied
/// to the aggregate columns.
#[tokio::test]
async fn timeseries_time_floor_wire_rows_project_to_select_list() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT TIME_FLOOR(__time, 'P1D') AS d, AVG(value) AS avg_v, COUNT(*) AS c \
         FROM telemetry GROUP BY 1 ORDER BY 1",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query must succeed; body = {body}");

    assert_no_hidden_keys(&body);

    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "two day buckets; body = {body}");
    for row in rows {
        assert_eq!(
            row_keys(row),
            vec!["d", "avg_v", "c"],
            "wire key order must match the SELECT list; row = {row}"
        );
        // The time bucket is an ISO-8601 *string* (Druid TIMESTAMP wire
        // shape), and it is the FIRST document key (asserted above).
        let d = row
            .get("d")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("time column `d` must be an ISO string; row = {row}"));
        assert!(
            d.starts_with("2024-01-0") && d.contains('T'),
            "time column must be ISO-8601, got {d:?}"
        );
    }

    // Day 1 = site_a rows (AVG 10.0, COUNT 3); day 2 = site_b rows
    // (AVG 40.0, COUNT 2). ORDER BY 1 puts day 1 first.
    assert!((f64_field(&rows[0], "avg_v") - 10.0).abs() < 1e-9);
    assert_eq!(rows[0].get("c").and_then(Value::as_i64), Some(3));
    assert!((f64_field(&rows[1], "avg_v") - 40.0).abs() < 1e-9);
    assert_eq!(rows[1].get("c").and_then(Value::as_i64), Some(2));
}

// ---------------------------------------------------------------------------
// codex QA r5 — SELECT-list order + dimension aliases.
//
// The planner previously assembled `output_columns` as TIME_FLOOR alias,
// then ALL grouping dimensions under their RAW names, then all aggregates —
// so `SELECT COUNT(*) AS c, site_id AS s … GROUP BY site_id` wire rows were
// `{site_id, c}`: the alias `s` vanished entirely and positional clients
// (pydruid / Superset bind columns by document position) saw the columns
// swapped. Druid's SQL layer emits exactly the SELECT list: aliased names,
// SELECT order. These tests pin that contract end-to-end.
// ---------------------------------------------------------------------------

/// Per-site count keyed by the aliased dimension column `s`.
fn counts_by_alias(
    rows: &[Value],
    dim_key: &str,
    agg_key: &str,
) -> std::collections::HashMap<String, i64> {
    rows.iter()
        .map(|row| {
            let site = row
                .get(dim_key)
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("row must carry `{dim_key}`; row = {row}"))
                .to_string();
            let c = row
                .get(agg_key)
                .and_then(Value::as_i64)
                .unwrap_or_else(|| panic!("row must carry `{agg_key}`; row = {row}"));
            (site, c)
        })
        .collect()
}

/// The r5 trigger query: `SELECT COUNT(*) AS c, site_id AS s … GROUP BY
/// site_id` must emit rows shaped exactly `{"c": …, "s": …}`.
#[tokio::test]
async fn groupby_agg_before_aliased_dim_projects_alias_in_select_order() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COUNT(*) AS c, site_id AS s FROM telemetry GROUP BY site_id",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query must succeed; body = {body}");
    assert_no_hidden_keys(&body);

    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "one row per site; body = {body}");
    for row in rows {
        assert_eq!(
            row_keys(row),
            vec!["c", "s"],
            "wire keys must be the SELECT list (alias included, agg first); row = {row}"
        );
    }
    let by_site = counts_by_alias(rows, "s", "c");
    assert_eq!(by_site.get("site_a"), Some(&3));
    assert_eq!(by_site.get("site_b"), Some(&2));
}

/// (c) A dimension alias BEFORE the aggregate keeps working.
#[tokio::test]
async fn groupby_aliased_dim_before_agg_still_projects() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id AS s, COUNT(*) AS c FROM telemetry GROUP BY site_id",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query must succeed; body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "one row per site; body = {body}");
    for row in rows {
        assert_eq!(row_keys(row), vec!["s", "c"], "row = {row}");
    }
    let by_site = counts_by_alias(rows, "s", "c");
    assert_eq!(by_site.get("site_a"), Some(&3));
    assert_eq!(by_site.get("site_b"), Some(&2));
}

/// `GROUP BY <alias>` / `GROUP BY <ordinal>` group by the RAW column and
/// still produce one row per site (not one all-null group).
#[tokio::test]
async fn group_by_alias_and_ordinal_group_raw_column() {
    for sql in [
        "SELECT COUNT(*) AS c, site_id AS s FROM telemetry GROUP BY s",
        "SELECT COUNT(*) AS c, site_id AS s FROM telemetry GROUP BY 2",
    ] {
        let app = setup_telemetry_app().await;
        let (status, body) = post_sql(app, sql).await;
        assert_eq!(status, StatusCode::OK, "sql = {sql}; body = {body}");
        let rows = body.as_array().expect("array body");
        assert_eq!(rows.len(), 2, "sql = {sql}; body = {body}");
        let by_site = counts_by_alias(rows, "s", "c");
        assert_eq!(by_site.get("site_a"), Some(&3), "sql = {sql}");
        assert_eq!(by_site.get("site_b"), Some(&2), "sql = {sql}");
    }
}

/// (d) Timeseries path: the TIME_FLOOR bucket NOT first in the SELECT list
/// keeps its SELECT position (previously hardcoded first).
#[tokio::test]
async fn timeseries_time_floor_not_first_projects_select_order() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COUNT(*) AS c, TIME_FLOOR(__time, 'P1D') AS d FROM telemetry GROUP BY 2",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query must succeed; body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "two day buckets; body = {body}");
    for row in rows {
        assert_eq!(row_keys(row), vec!["c", "d"], "row = {row}");
        let d = row
            .get("d")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("time column `d` must be an ISO string; row = {row}"));
        assert!(d.starts_with("2024-01-0") && d.contains('T'), "d = {d:?}");
    }
    // Buckets ascend: day 1 (site_a, 3 rows) then day 2 (site_b, 2 rows).
    assert_eq!(rows[0].get("c").and_then(Value::as_i64), Some(3));
    assert_eq!(rows[1].get("c").and_then(Value::as_i64), Some(2));
}

/// TopN path: aggregate-first SELECT list with an aliased dimension.
#[tokio::test]
async fn topn_agg_before_aliased_dim_projects_select_order() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT AVG(value) AS avg_v, site_id AS s FROM telemetry \
         GROUP BY site_id ORDER BY avg_v DESC LIMIT 5",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query must succeed; body = {body}");
    assert_no_hidden_keys(&body);
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "two sites; body = {body}");
    for row in rows {
        assert_eq!(row_keys(row), vec!["avg_v", "s"], "row = {row}");
    }
    assert_eq!(rows[0].get("s").and_then(Value::as_str), Some("site_b"));
    assert!((f64_field(&rows[0], "avg_v") - 40.0).abs() < 1e-9);
    assert_eq!(rows[1].get("s").and_then(Value::as_str), Some("site_a"));
    assert!((f64_field(&rows[1], "avg_v") - 10.0).abs() < 1e-9);
}

/// (e) ORDER BY the dimension alias sorts the wire rows — the native sort
/// spec must reference the executor's emitted key (the `outputName`).
#[tokio::test]
async fn order_by_dimension_alias_sorts_groupby_rows() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id AS s, COUNT(*) AS c FROM telemetry GROUP BY site_id ORDER BY s DESC",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query must succeed; body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "body = {body}");
    assert_eq!(rows[0].get("s").and_then(Value::as_str), Some("site_b"));
    assert_eq!(rows[1].get("s").and_then(Value::as_str), Some("site_a"));
}

/// A TIME_FLOOR grouped WITH another dimension lowers to a granular GroupBy
/// whose bucket lives in the native result's `timestamp` (not the event
/// map); it must still surface under the SQL alias at its SELECT position.
#[tokio::test]
async fn groupby_time_floor_with_dim_surfaces_bucket_alias() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id AS s, TIME_FLOOR(__time, 'P1D') AS d, COUNT(*) AS c \
         FROM telemetry GROUP BY 1, 2",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query must succeed; body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "site_a day1 + site_b day2; body = {body}");
    for row in rows {
        assert_eq!(row_keys(row), vec!["s", "d", "c"], "row = {row}");
        let d = row
            .get("d")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("bucket `d` must be an ISO string; row = {row}"));
        assert!(d.starts_with("2024-01-0") && d.contains('T'), "d = {d:?}");
    }
    let by_site = counts_by_alias(rows, "s", "c");
    assert_eq!(by_site.get("site_a"), Some(&3));
    assert_eq!(by_site.get("site_b"), Some(&2));
}

/// codex QA r10: duplicate projections of the same dimension
/// (`SELECT site_id AS a, site_id AS b …`) must BOTH carry the value —
/// previously only the first alias bound and the second null-filled.
/// Covers the GroupBy path and the TopN-shaped query (which must fall
/// back to GroupBy rather than emit a duplicate-named column).
#[tokio::test]
async fn duplicate_dim_projections_both_carry_value() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id AS a, site_id AS b, COUNT(*) AS c FROM telemetry GROUP BY site_id",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(row_keys(row), vec!["a", "b", "c"], "row = {row}");
        let a = row.get("a").and_then(Value::as_str).expect("a value");
        let b = row.get("b").and_then(Value::as_str);
        assert_eq!(b, Some(a), "both aliases must carry the dimension value");
    }

    // TopN-shaped (single dim + ORDER BY + LIMIT): must still return both.
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id AS a, site_id AS b, COUNT(*) AS c FROM telemetry \
         GROUP BY site_id ORDER BY c DESC LIMIT 5",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    for row in rows {
        let a = row.get("a").and_then(Value::as_str).expect("a value");
        assert_eq!(
            row.get("b").and_then(Value::as_str),
            Some(a),
            "TopN-shaped duplicate-dim query must carry both aliases; row = {row}"
        );
    }
}

/// codex QA r12: bare-scan SELECT aliases must surface on the wire —
/// `SELECT site_id AS s …` previously emitted the RAW column key
/// (`site_id`), and duplicate scan aliases could not emit both names.
#[tokio::test]
async fn scan_aliases_project_on_the_wire() {
    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(app, "SELECT site_id AS s FROM telemetry LIMIT 2").await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(row_keys(row), vec!["s"], "row = {row}");
        assert!(row.get("s").and_then(Value::as_str).is_some());
    }

    let app = setup_telemetry_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id AS a, site_id AS b, value FROM telemetry LIMIT 1",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    let row = &rows[0];
    assert_eq!(row_keys(row), vec!["a", "b", "value"], "row = {row}");
    let a = row.get("a").and_then(Value::as_str).expect("a");
    assert_eq!(row.get("b").and_then(Value::as_str), Some(a));
}

// ---------------------------------------------------------------------------
// SELECT * column order (Druid parity)
// ---------------------------------------------------------------------------

/// Druid emits `SELECT *` columns as `__time`, then dimensions in SPEC
/// order, then metrics ALPHABETICAL — measured against Druid 35
/// (tests/druid-compat/RESULTS_wave47b_v35_run.md `superset_preview_limit`:
/// metricsSpec order `count,added,deleted,delta` came back
/// `added,count,deleted,delta` while dimensions `page,user,language,city,
/// namespace,channel` kept spec order). FerroDruid used to emit metrics in
/// ingest order, a wire-visible shape diff for positional consumers.
#[tokio::test]
async fn wildcard_scan_orders_metrics_alphabetically() {
    let app = setup_multimetric_app().await;
    let (status, body) = post_sql(app, "SELECT * FROM factory ORDER BY __time LIMIT 10").await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "body = {body}");
    for row in rows {
        assert_eq!(
            row_keys(row),
            vec![
                "__time", "site_id", "area", "added", "count", "deleted", "delta"
            ],
            "SELECT * must emit __time, dims (spec order), metrics (sorted); row = {row}"
        );
    }
}

/// An EXPLICIT projection list is untouched by the wildcard re-order — the
/// SELECT list order stays authoritative (metrics deliberately reversed).
#[tokio::test]
async fn explicit_scan_projection_order_is_preserved() {
    let app = setup_multimetric_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT delta, deleted, count, added, site_id FROM factory LIMIT 1",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(
        row_keys(&rows[0]),
        vec!["delta", "deleted", "count", "added", "site_id"],
        "explicit SELECT list order must be preserved; body = {body}"
    );
}

/// End-to-end: a real `UNION ALL` of DIFFERENTLY-NAMED scan branches, driven
/// through `/druid/v2/sql`. Druid names the output from the FIRST branch and
/// maps later branches into it by POSITION, so branch 2's `added` values must
/// appear under `delta` (the first branch's column), both branches must
/// contribute, and no `added` key may leak onto the wire.
#[tokio::test]
async fn sql_union_all_maps_differently_named_branches_positionally() {
    let app = setup_multimetric_app().await;
    let (single_status, single) = post_sql(app.clone(), "SELECT delta FROM factory").await;
    assert_eq!(
        single_status,
        StatusCode::OK,
        "single-branch body = {single}"
    );
    let single_n = single.as_array().expect("single array body").len();
    assert!(single_n > 0, "factory scan must return rows");

    let (status, body) = post_sql(
        app,
        "SELECT delta FROM factory UNION ALL SELECT added FROM factory",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "union body = {body}");
    let rows = body.as_array().expect("union array body");
    assert_eq!(
        rows.len(),
        single_n * 2,
        "both UNION ALL branches must contribute all rows; body = {body}"
    );
    assert!(
        rows.iter().all(|r| r.get("added").is_none()),
        "branch 2's native key `added` must be remapped to `delta`; body = {body}"
    );
    assert!(
        rows.iter().all(|r| r.get("delta").is_some()),
        "every row must be projected under the first branch's column `delta`; body = {body}"
    );
}
