// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! REST-level (`/druid/v2/sql`) null-semantics + wire-shape e2e.
//!
//! Drives the SQL null-semantics program end-to-end through the Axum
//! router against the 7-row `nulltest` segment (loaded directly into the
//! Historical — live ingestion of nulls lands in the parallel ingestion
//! task; string nulls are out-of-dictionary ordinals, numeric nulls are
//! NaN-encoded doubles, both of which the executor reads as JSON `null`):
//!
//! * T1/T4 — AVG / ROUND(AVG) per group: 15.0 / 30.0 / null on the wire
//!   (DOUBLE columns keep the trailing `.0`; the all-null group is null).
//! * T2 — COUNT(col) counts non-null rows (BIGINT).
//! * T3 — COUNT(DISTINCT col) / APPROX_COUNT_DISTINCT(col) return the
//!   INTEGER `3` on the wire (BIGINT — not `3.0`).
//! * T5 — scan ORDER BY a non-time column fails closed (HTTP 400 with
//!   Druid's error text); ORDER BY __time keeps working.
//! * T7 — scan `__time` renders as an ISO-8601 millis string on the SQL
//!   wire (`2024-01-01T00:00:00.000Z`), matching Druid SQL.
//! * T9 — `WHERE __time >= TIME_PARSE('...')` actually filters rows, and
//!   the Superset grain shape `TIME_FLOOR(CAST(__time AS TIMESTAMP), p)`
//!   executes.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ferrodruid_auth::AuthStore;
use ferrodruid_authz::Authorizer;
use ferrodruid_bitmap::DruidBitmap;
use ferrodruid_broker::Broker;
use ferrodruid_coordinator::Coordinator;
use ferrodruid_dict::FrontCodedDictionary;
use ferrodruid_historical::Historical;
use ferrodruid_metadata::{MetadataStore, SegmentMetadataRow};
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_rest::{AppState, create_router};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::{ColumnData, StringColumnData};
use serde_json::{Value, json};
use tower::ServiceExt;

const NULL_ORD: u32 = 3;

/// 7-row nulltest mirror (same dataset as the diff-harness Section 7):
/// site_a values 10,20,NULL; site_b 30,NULL,NULL; site_c NULL;
/// device_id d1,d2,d2,d1,NULL,d3,d3.
fn build_nulltest_segment() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("base ts")
        .timestamp_millis();
    let num_rows = 7usize;
    let timestamps: Vec<i64> = (0..num_rows as i64).map(|i| base + i * 3_600_000).collect();

    let site_ords: Vec<u32> = vec![0, 0, 0, 1, 1, 1, 2];
    let site_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec![
            "site_a".to_string(),
            "site_b".to_string(),
            "site_c".to_string(),
        ]),
        encoded_values: site_ords.clone(),
        bitmap_indexes: build_bitmaps(3, &site_ords),
    });

    let device_ords: Vec<u32> = vec![0, 1, 1, 0, NULL_ORD, 2, 2];
    let device_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec![
            "d1".to_string(),
            "d2".to_string(),
            "d3".to_string(),
        ]),
        encoded_values: device_ords.clone(),
        bitmap_indexes: build_bitmaps(3, &device_ords),
    });

    let value_col = ColumnData::Double(vec![
        10.0,
        20.0,
        f64::NAN,
        30.0,
        f64::NAN,
        f64::NAN,
        f64::NAN,
    ]);

    let mut columns = HashMap::new();
    columns.insert("__time".to_string(), ColumnData::Long(timestamps));
    columns.insert("site_id".to_string(), site_col);
    columns.insert("device_id".to_string(), device_col);
    columns.insert("value".to_string(), value_col);

    SegmentData {
        version: 9,
        num_rows,
        interval: ferrodruid_segment::Interval {
            start_millis: base,
            end_millis: base + 86_400_000,
        },
        dimensions: vec!["site_id".to_string(), "device_id".to_string()],
        metrics: vec!["value".to_string()],
        columns,
        time_sorted: true,
    }
}

fn build_bitmaps(cardinality: usize, ordinals: &[u32]) -> Vec<DruidBitmap> {
    let mut bitmaps: Vec<DruidBitmap> = (0..cardinality).map(|_| DruidBitmap::new()).collect();
    for (row_idx, &ord) in ordinals.iter().enumerate() {
        if (ord as usize) < cardinality {
            bitmaps[ord as usize].insert(row_idx as u32);
        }
    }
    bitmaps
}

async fn setup_nulltest_app() -> Router {
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("create metadata store");
    metadata.initialize().await.expect("initialize schema");
    let metadata = Arc::new(metadata);

    let coordinator = Arc::new(Coordinator::new(Arc::clone(&metadata)));
    let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));

    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 100_000_000);
    let segment_id = "nulltest_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_v1_0";
    historical
        .load_segment(segment_id, build_nulltest_segment())
        .expect("load segment");
    historical
        .set_segment_datasource(segment_id, "nulltest")
        .expect("set datasource");
    let historical = Arc::new(historical);

    let seg_row = SegmentMetadataRow {
        id: segment_id.to_string(),
        data_source: "nulltest".to_string(),
        created_date: "2024-01-01T00:00:00.000Z".to_string(),
        start: "2024-01-01T00:00:00.000Z".to_string(),
        end: "2024-01-02T00:00:00.000Z".to_string(),
        version: "v1".to_string(),
        used: true,
        payload: json!({
            "dataSource": "nulltest",
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
    post_sql_body(app, json!({ "query": sql })).await
}

/// POST /druid/v2/sql with an explicit query `context` object (E16: the
/// `useApproximateCountDistinct` flag).
async fn post_sql_with_context(app: Router, sql: &str, context: Value) -> (StatusCode, Value) {
    post_sql_body(app, json!({ "query": sql, "context": context })).await
}

async fn post_sql_body(app: Router, body: Value) -> (StatusCode, Value) {
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

/// Extract `(site_id, field)` pairs sorted by site from an SQL row array.
fn rows_by_site(body: &Value, field: &str) -> Vec<(String, Value)> {
    let mut out: Vec<(String, Value)> = body
        .as_array()
        .expect("array body")
        .iter()
        .map(|r| {
            (
                r.get("site_id")
                    .and_then(Value::as_str)
                    .expect("site_id")
                    .to_string(),
                r.get(field).cloned().unwrap_or(Value::Null),
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

// ===========================================================================
// T1 + T4 — AVG / ROUND(AVG) on the wire: DOUBLE with nulls preserved
// ===========================================================================

#[tokio::test]
async fn wire_avg_by_site_null_faithful() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id, AVG(\"value\") AS avg_v FROM nulltest GROUP BY site_id",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        rows_by_site(&body, "avg_v"),
        vec![
            ("site_a".to_string(), json!(15.0)),
            ("site_b".to_string(), json!(30.0)),
            ("site_c".to_string(), Value::Null),
        ],
        "body = {body}"
    );
    // DOUBLE columns keep the trailing .0 on the wire (no integer collapse).
    for row in body.as_array().expect("array") {
        let v = row.get("avg_v").expect("avg_v");
        if !v.is_null() {
            assert!(
                v.is_f64(),
                "avg_v must stay a DOUBLE on the wire, got {v:?}"
            );
        }
    }
}

#[tokio::test]
async fn wire_round_avg_by_site_null_faithful() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id, ROUND(AVG(\"value\"), 1) AS r FROM nulltest GROUP BY site_id",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        rows_by_site(&body, "r"),
        vec![
            ("site_a".to_string(), json!(15.0)),
            ("site_b".to_string(), json!(30.0)),
            ("site_c".to_string(), Value::Null),
        ],
        "body = {body}"
    );
}

/// MIN/MAX over an all-null group return SQL null on the wire (Druid 35
/// default null handling) — NOT the aggregator init sentinels
/// (`f64::MAX`/`f64::MIN` here). site_c has only a NULL `value`.
#[tokio::test]
async fn wire_min_max_by_site_null_faithful() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id, MIN(\"value\") AS mn, MAX(\"value\") AS mx \
         FROM nulltest GROUP BY site_id",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    // NOTE: integral doubles collapse to wire integers for plain (non
    // post-agg) aggregators — pre-existing REST normalization, asserted
    // as-is here; this test pins the NULL contract, not the float typing.
    assert_eq!(
        rows_by_site(&body, "mn"),
        vec![
            ("site_a".to_string(), json!(10)),
            ("site_b".to_string(), json!(30)),
            ("site_c".to_string(), Value::Null),
        ],
        "MIN over an all-null group must be SQL null; body = {body}"
    );
    assert_eq!(
        rows_by_site(&body, "mx"),
        vec![
            ("site_a".to_string(), json!(20)),
            ("site_b".to_string(), json!(30)),
            ("site_c".to_string(), Value::Null),
        ],
        "MAX over an all-null group must be SQL null; body = {body}"
    );
}

// ===========================================================================
// T2 — COUNT(col) non-null count, BIGINT on the wire
// ===========================================================================

#[tokio::test]
async fn wire_count_column_counts_non_null() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id, COUNT(\"value\") AS c FROM nulltest GROUP BY site_id",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        rows_by_site(&body, "c"),
        vec![
            ("site_a".to_string(), json!(2)),
            ("site_b".to_string(), json!(1)),
            ("site_c".to_string(), json!(0)),
        ],
        "body = {body}"
    );
}

// ===========================================================================
// T3 — COUNT(DISTINCT) / APPROX_COUNT_DISTINCT: integer 3 on the wire
// ===========================================================================

#[tokio::test]
async fn wire_count_distinct_is_integer_three() {
    for sql in [
        "SELECT COUNT(DISTINCT device_id) AS dc FROM nulltest",
        "SELECT APPROX_COUNT_DISTINCT(device_id) AS dc FROM nulltest",
    ] {
        let app = setup_nulltest_app().await;
        let (status, body) = post_sql(app, sql).await;
        assert_eq!(status, StatusCode::OK, "sql = {sql}; body = {body}");
        let rows = body.as_array().expect("array body");
        assert_eq!(rows.len(), 1, "sql = {sql}; body = {body}");
        let dc = rows[0].get("dc").expect("dc column");
        // Ground truth: BIGINT on the wire — `3`, not `3.0`.
        assert_eq!(dc, &json!(3), "sql = {sql}; body = {body}");
        assert!(
            dc.is_i64() || dc.is_u64(),
            "COUNT(DISTINCT) must be an integer on the wire (BIGINT), got {dc:?}"
        );
    }
}

// ===========================================================================
// E16 — exact COUNT(DISTINCT) via context useApproximateCountDistinct=false
// ===========================================================================

/// The E16 exact-mode SQL context.
fn exact_ctx() -> Value {
    json!({"useApproximateCountDistinct": false})
}

/// Exact mode returns the true distinct count as a wire integer. At this
/// cardinality (3) the approximate HLL estimate is also exactly 3, so the
/// value additionally EQUALS the default-mode result — the plan-level
/// discrimination lives in `wire_exact_count_distinct_explain_shows_plan`.
#[tokio::test]
async fn wire_exact_count_distinct_returns_exact_integer() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql_with_context(
        app,
        "SELECT COUNT(DISTINCT device_id) AS dc FROM nulltest",
        exact_ctx(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 1, "body = {body}");
    let dc = rows[0].get("dc").expect("dc column");
    // Ground truth: d1,d2,d3 → 3 (the NULL device_id row is NOT counted;
    // an unfiltered cardinality set would report 4).
    assert_eq!(dc, &json!(3), "body = {body}");
    assert!(
        dc.is_i64() || dc.is_u64(),
        "exact COUNT(DISTINCT) must be a wire integer (BIGINT), got {dc:?}"
    );

    // Same query in default (approximate) mode returns the same value at
    // this small cardinality — exact must equal approx here.
    let app = setup_nulltest_app().await;
    let (status, approx_body) =
        post_sql(app, "SELECT COUNT(DISTINCT device_id) AS dc FROM nulltest").await;
    assert_eq!(status, StatusCode::OK, "body = {approx_body}");
    assert_eq!(
        approx_body.as_array().and_then(|r| r[0].get("dc")),
        Some(&json!(3)),
        "approx and exact must agree at n=3; approx body = {approx_body}"
    );
}

/// Grouped exact distinct counts skip NULLs per group: site_b has devices
/// d1, NULL, d3 → 2 (an unfiltered cardinality would report 3 there).
#[tokio::test]
async fn wire_exact_count_distinct_grouped_skips_nulls() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql_with_context(
        app,
        "SELECT site_id, COUNT(DISTINCT device_id) AS dc FROM nulltest GROUP BY site_id",
        exact_ctx(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        rows_by_site(&body, "dc"),
        vec![
            ("site_a".to_string(), json!(2)), // d1, d2
            ("site_b".to_string(), json!(2)), // d1, d3 — NULL skipped
            ("site_c".to_string(), json!(1)), // d3
        ],
        "body = {body}"
    );
}

/// Plan-level discrimination: EXPLAIN with the exact context must plan a
/// `cardinality` aggregation (no HLL sketch); EXPLAIN without a context
/// must keep the HLL sketch (no cardinality). This is the assertion that
/// actually proves the context flag reaches the planner — the small-n
/// VALUE of COUNT(DISTINCT) is identical in both modes.
#[tokio::test]
async fn wire_exact_count_distinct_explain_shows_plan() {
    let sql = "EXPLAIN PLAN FOR SELECT COUNT(DISTINCT device_id) AS dc FROM nulltest";

    let app = setup_nulltest_app().await;
    let (status, body) = post_sql_with_context(app, sql, exact_ctx()).await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let plan = body[0]["PLAN"].as_str().expect("PLAN string");
    assert!(
        plan.contains("cardinality"),
        "exact-mode plan must use the cardinality aggregation; plan = {plan}"
    );
    assert!(
        !plan.contains("HLLSketchBuild"),
        "exact-mode plan must not build an HLL sketch; plan = {plan}"
    );

    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(app, sql).await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let plan = body[0]["PLAN"].as_str().expect("PLAN string");
    assert!(
        plan.contains("HLLSketchBuild"),
        "default-mode plan must keep the HLL sketch; plan = {plan}"
    );
    assert!(
        !plan.contains("cardinality"),
        "default-mode plan must not use the cardinality aggregation; plan = {plan}"
    );
}

/// APPROX_COUNT_DISTINCT is an explicit approximate request — it stays on
/// the HLL path (and keeps working) even in exact mode, matching Druid.
#[tokio::test]
async fn wire_approx_count_distinct_stays_approx_in_exact_mode() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql_with_context(
        app,
        "SELECT APPROX_COUNT_DISTINCT(device_id) AS adc FROM nulltest",
        exact_ctx(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array().and_then(|r| r[0].get("adc")),
        Some(&json!(3)),
        "body = {body}"
    );

    let app = setup_nulltest_app().await;
    let (status, body) = post_sql_with_context(
        app,
        "EXPLAIN PLAN FOR SELECT APPROX_COUNT_DISTINCT(device_id) AS adc FROM nulltest",
        exact_ctx(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let plan = body[0]["PLAN"].as_str().expect("PLAN string");
    assert!(
        plan.contains("HLLSketchBuild") && !plan.contains("cardinality"),
        "APPROX_COUNT_DISTINCT must stay on the HLL path in exact mode; plan = {plan}"
    );
}

/// Druid's query context accepts stringified booleans — `"false"` (any
/// case) selects exact mode too.
#[tokio::test]
async fn wire_exact_count_distinct_accepts_string_false() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql_with_context(
        app,
        "EXPLAIN PLAN FOR SELECT COUNT(DISTINCT device_id) AS dc FROM nulltest",
        json!({"useApproximateCountDistinct": "false"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let plan = body[0]["PLAN"].as_str().expect("PLAN string");
    assert!(
        plan.contains("cardinality") && !plan.contains("HLLSketchBuild"),
        "string \"false\" must select exact mode; plan = {plan}"
    );
}

/// A present-but-unparseable `useApproximateCountDistinct` fails closed
/// (HTTP 400) — never silently runs in the wrong distinct mode.
#[tokio::test]
async fn wire_malformed_use_approx_context_fails_closed() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql_with_context(
        app,
        "SELECT COUNT(DISTINCT device_id) AS dc FROM nulltest",
        json!({"useApproximateCountDistinct": 42}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "malformed context value must fail closed; body = {body}"
    );
    let msg = body["errorMessage"].as_str().expect("errorMessage");
    assert!(
        msg.contains("useApproximateCountDistinct"),
        "error must name the offending context key; got {msg}"
    );
}

// ===========================================================================
// T5 — scan ORDER BY non-time column fails closed; __time keeps working
// ===========================================================================

#[tokio::test]
async fn wire_scan_order_by_non_time_fails_closed() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id, device_id FROM nulltest ORDER BY site_id",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "scan ORDER BY non-time must fail closed (Druid does too); body = {body}"
    );
    let msg = body["errorMessage"].as_str().expect("errorMessage");
    assert!(
        msg.contains("non-time column [[site_id]]") && msg.contains("not supported"),
        "must match Druid's error shape; got {msg}"
    );
}

#[tokio::test]
async fn wire_scan_order_by_time_still_works() {
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT __time, site_id FROM nulltest ORDER BY __time DESC LIMIT 2",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "body = {body}");
    // Descending: the last two rows (site_c hour 6, site_b hour 5).
    assert_eq!(
        rows[0].get("site_id").and_then(Value::as_str),
        Some("site_c"),
        "body = {body}"
    );
}

// ===========================================================================
// T7 — scan __time is an ISO-8601 string on the SQL wire
// ===========================================================================

#[tokio::test]
async fn wire_scan_time_column_renders_iso() {
    // Explicit projection.
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(app, "SELECT __time, site_id FROM nulltest LIMIT 1").await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(
        rows[0].get("__time"),
        Some(&json!("2024-01-01T00:00:00.000Z")),
        "scan __time must be Druid's ISO-8601 millis string; body = {body}"
    );

    // Wildcard scan.
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(app, "SELECT * FROM nulltest LIMIT 1").await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(
        rows[0].get("__time"),
        Some(&json!("2024-01-01T00:00:00.000Z")),
        "wildcard scan __time must be ISO too; body = {body}"
    );
}

// ===========================================================================
// T9 — TIME_PARSE WHERE filter + Superset grain shape
// ===========================================================================

#[tokio::test]
async fn wire_time_parse_where_filters_rows() {
    // Rows are hourly from 00:00; cutoff at 03:00 keeps 4 of 7 rows.
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COUNT(*) AS c FROM nulltest \
         WHERE __time >= TIME_PARSE('2024-01-01T03:00:00')",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array()
            .and_then(|r| r.first())
            .and_then(|r| r.get("c"))
            .and_then(Value::as_i64),
        Some(4),
        "TIME_PARSE cutoff must actually filter; body = {body}"
    );

    // CAST(TIME_PARSE(...) AS DATE) — day floor; 2024-01-01 keeps all 7.
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT COUNT(*) AS c FROM nulltest \
         WHERE __time >= CAST(TIME_PARSE('2024-01-01') AS DATE)",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array()
            .and_then(|r| r.first())
            .and_then(|r| r.get("c"))
            .and_then(Value::as_i64),
        Some(7),
        "body = {body}"
    );
}

#[tokio::test]
async fn wire_superset_grain_shape_executes() {
    // The exact chart shape Superset emits for the PT1H grain.
    let app = setup_nulltest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT TIME_FLOOR(CAST(__time AS TIMESTAMP), 'PT1H') AS __timestamp, \
         COUNT(*) AS \"count\" FROM nulltest \
         WHERE __time >= TIME_PARSE('2024-01-01T00:00:00') \
         GROUP BY 1 ORDER BY 1",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 7, "7 hourly buckets; body = {body}");
    assert_eq!(
        rows[0].get("__timestamp"),
        Some(&json!("2024-01-01T00:00:00.000Z")),
        "body = {body}"
    );
    let total: i64 = rows
        .iter()
        .filter_map(|r| r.get("count").and_then(Value::as_i64))
        .sum();
    assert_eq!(total, 7, "body = {body}");
}
