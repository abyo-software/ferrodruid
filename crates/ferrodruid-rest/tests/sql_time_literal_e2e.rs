// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! REST-level (`/druid/v2/sql`) e2e for SQL `TIMESTAMP '...'` time-filter
//! literals and `MIN(__time)` / `MAX(__time)` wire shape (P1-#2).
//!
//! Apache Superset's time-range filter emits exactly
//! `WHERE __time >= TIMESTAMP 'YYYY-MM-DD HH:MM:SS[.ffffff]' AND __time <
//! TIMESTAMP '...'` — these must filter rows like the equivalent
//! `TIME_PARSE('...')` bound (they previously matched ZERO rows: the typed
//! literal fell through as a plain string into a numeric bound, whose
//! `f64` parse of `"2024-01-01 00:00:00"` fails-closed for every row).
//!
//! `MIN(__time)` / `MAX(__time)` must emit Druid SQL's ISO-8601 millis
//! string (`2024-01-01T00:00:00.000Z`) on the SQL wire, not raw epoch
//! millis (they previously lowered to `doubleMin`/BIGINT). The native
//! `/druid/v2` endpoint keeps epoch millis exactly like Druid.

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

/// 7-row `timetest` segment: hourly rows starting 2024-01-01T00:00:00Z
/// (…T00 through …T06), one string dim, one double metric.
fn build_timetest_segment() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("base ts")
        .timestamp_millis();
    let num_rows = 7usize;
    let timestamps: Vec<i64> = (0..num_rows as i64).map(|i| base + i * 3_600_000).collect();

    let site_ords: Vec<u32> = vec![0, 0, 0, 1, 1, 1, 1];
    let site_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec![
            "site_a".to_string(),
            "site_b".to_string(),
        ]),
        encoded_values: site_ords.clone(),
        bitmap_indexes: build_bitmaps(2, &site_ords),
    });

    let value_col = ColumnData::Double(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0]);

    let mut columns = HashMap::new();
    columns.insert("__time".to_string(), ColumnData::Long(timestamps));
    columns.insert("site_id".to_string(), site_col);
    columns.insert("value".to_string(), value_col);

    SegmentData {
        version: 9,
        num_rows,
        interval: ferrodruid_segment::Interval {
            start_millis: base,
            end_millis: base + 86_400_000,
        },
        dimensions: vec!["site_id".to_string()],
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

async fn setup_timetest_app() -> Router {
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("create metadata store");
    metadata.initialize().await.expect("initialize schema");
    let metadata = Arc::new(metadata);

    let coordinator = Arc::new(Coordinator::new(Arc::clone(&metadata)));
    let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));

    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 100_000_000);
    let segment_id = "timetest_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_v1_0";
    historical
        .load_segment(segment_id, build_timetest_segment())
        .expect("load segment");
    historical
        .set_segment_datasource(segment_id, "timetest")
        .expect("set datasource");
    let historical = Arc::new(historical);

    let seg_row = SegmentMetadataRow {
        id: segment_id.to_string(),
        data_source: "timetest".to_string(),
        created_date: "2024-01-01T00:00:00.000Z".to_string(),
        start: "2024-01-01T00:00:00.000Z".to_string(),
        end: "2024-01-02T00:00:00.000Z".to_string(),
        version: "v1".to_string(),
        used: true,
        payload: json!({
            "dataSource": "timetest",
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

/// Like [`post_sql`] but with an explicit query `context` object (used by
/// the `sqlTimeZone` fail-closed tests).
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

/// Run `SELECT COUNT(*)` with the given WHERE clause and return the count.
async fn count_where(where_clause: &str) -> i64 {
    let app = setup_timetest_app().await;
    let sql = format!("SELECT COUNT(*) AS c FROM timetest WHERE {where_clause}");
    let (status, body) = post_sql(app, &sql).await;
    assert_eq!(status, StatusCode::OK, "sql = {sql}, body = {body}");
    body.as_array()
        .and_then(|rows| rows.first())
        .and_then(|r| r.get("c"))
        .and_then(Value::as_i64)
        .unwrap_or_else(|| panic!("no count in body {body} for {sql}"))
}

// ===========================================================================
// TIMESTAMP '...' literal bounds actually filter (Superset time-range shape)
// ===========================================================================

/// `__time >= TIMESTAMP '...'` (space-separated Calcite form) matches the
/// rows at/after the bound — previously ZERO rows.
#[tokio::test]
async fn where_timestamp_literal_ge_filters_rows() {
    // Rows are hourly from 00:00 to 06:00; >= 03:00 keeps 4 rows.
    let c = count_where("__time >= TIMESTAMP '2024-01-01 03:00:00'").await;
    assert_eq!(c, 4);
}

/// Superset emits fractional seconds (microsecond precision).
#[tokio::test]
async fn where_timestamp_literal_fractional_filters_rows() {
    let c = count_where("__time >= TIMESTAMP '2024-01-01 03:00:00.000000'").await;
    assert_eq!(c, 4);
}

/// The full Superset time-range shape: half-open interval of two TIMESTAMP
/// literals.
#[tokio::test]
async fn where_timestamp_literal_range_filters_rows() {
    let c = count_where(
        "__time >= TIMESTAMP '2024-01-01 01:00:00' AND __time < TIMESTAMP '2024-01-01 05:00:00'",
    )
    .await;
    assert_eq!(c, 4); // rows at 01,02,03,04
}

/// The TIMESTAMP-literal bound must agree with the (already working)
/// TIME_PARSE bound.
#[tokio::test]
async fn timestamp_literal_matches_time_parse() {
    let via_literal = count_where("__time >= TIMESTAMP '2024-01-01 02:00:00'").await;
    let via_time_parse = count_where("__time >= TIME_PARSE('2024-01-01T02:00:00')").await;
    assert_eq!(via_literal, 5);
    assert_eq!(via_literal, via_time_parse);
}

/// `DATE '...'` literal folds to midnight UTC. (Both assertions are
/// discriminating: pre-fix the unparseable string bound matched ZERO rows,
/// so each count came back 0/empty instead of 7. A zero-match COUNT
/// grand-total intentionally isn't asserted here — FerroDruid currently
/// returns `[]` where Druid returns a `0` row, a pre-existing divergence
/// shared by the untouched `TIME_PARSE` path.)
#[tokio::test]
async fn where_date_literal_filters_rows() {
    let c = count_where("__time >= DATE '2024-01-01'").await;
    assert_eq!(c, 7);
    let c2 = count_where("__time < DATE '2024-01-02'").await;
    assert_eq!(c2, 7);
}

/// Codex-review HIGH finding A: a `DATE` literal must be date-only
/// (`YYYY-MM-DD`); a time component is invalid SQL (Calcite rejects it)
/// and previously slipped through the timestamp parser silently. It now
/// FAILS CLOSED with a planning error.
#[tokio::test]
async fn date_literal_with_time_component_fails_closed() {
    for bad in [
        "DATE '2024-01-01 12:00:00'",
        "DATE '2024-01-01T12:00:00'",
        "DATE '2024-01-01 00:00:00.000000'",
        "DATE '2024-01-01T00:00:00Z'",
    ] {
        let app = setup_timetest_app().await;
        let sql = format!("SELECT COUNT(*) AS c FROM timetest WHERE __time >= {bad}");
        let (status, body) = post_sql(app, &sql).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{bad} must fail closed, body = {body}"
        );
        let msg = body
            .get("errorMessage")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            msg.contains("DATE"),
            "error must name the DATE literal, body = {body}"
        );
    }
}

// ===========================================================================
// sqlTimeZone — non-UTC fails closed (R-6 hardening, codex HIGH finding B)
// ===========================================================================

/// A non-UTC `sqlTimeZone` in the SQL query context FAILS CLOSED: FerroDruid
/// evaluates all timestamps in UTC (documented residual R-6), and silently
/// returning UTC-shifted-wrong results to a client that asked for another
/// zone would be strictly worse than an explicit error. (Stock
/// Superset + pydruid sends NO `sqlTimeZone` — pydruid defaults
/// `context or {}` — so this cannot break default BI dashboards.)
#[tokio::test]
async fn non_utc_sql_time_zone_fails_closed() {
    for tz in ["America/Los_Angeles", "Asia/Tokyo", "+09:00", "PST"] {
        let app = setup_timetest_app().await;
        let (status, body) = post_sql_with_context(
            app,
            "SELECT COUNT(*) AS c FROM timetest",
            json!({ "sqlTimeZone": tz }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "sqlTimeZone {tz} must fail closed, body = {body}"
        );
        assert_eq!(
            body.get("errorClass").and_then(Value::as_str),
            Some("io.druid.sql.SqlPlanningException"),
            "body = {body}"
        );
        let msg = body
            .get("errorMessage")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            msg.contains("sqlTimeZone") && msg.contains("UTC"),
            "error must name sqlTimeZone and UTC, body = {body}"
        );
    }
}

/// Codex round-2: a CONSTANT `SELECT` (no FROM) returns before the main plan
/// path, so the non-UTC `sqlTimeZone` fail-closed gate must run BEFORE that
/// early return — otherwise `SELECT TIMESTAMP '...'` with a non-UTC zone would
/// slip through UTC-folded at HTTP 200.
#[tokio::test]
async fn constant_select_non_utc_sql_time_zone_fails_closed() {
    let app = setup_timetest_app().await;
    let (status, body) = post_sql_with_context(
        app,
        "SELECT TIMESTAMP '2024-01-01 00:00:00' AS t",
        json!({ "sqlTimeZone": "America/Los_Angeles" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "constant SELECT must not bypass the sqlTimeZone gate, body = {body}"
    );
    assert_eq!(
        body.get("errorClass").and_then(Value::as_str),
        Some("io.druid.sql.SqlPlanningException"),
        "body = {body}"
    );
}

/// A constant SELECT with a UTC (or absent) zone still works.
#[tokio::test]
async fn constant_select_utc_sql_time_zone_ok() {
    let app = setup_timetest_app().await;
    let (status, body) = post_sql_with_context(
        app,
        "SELECT TIMESTAMP '2024-01-01 00:00:00' AS t",
        json!({ "sqlTimeZone": "UTC" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body[0]["t"].as_str(),
        Some("2024-01-01T00:00:00.000Z"),
        "body = {body}"
    );
}

/// UTC spellings (and an explicit null) keep working — they are exactly the
/// semantics FerroDruid implements.
#[tokio::test]
async fn utc_sql_time_zone_forms_accepted() {
    for tz in [json!("UTC"), json!("Etc/UTC"), json!("+00:00"), Value::Null] {
        let app = setup_timetest_app().await;
        let (status, body) = post_sql_with_context(
            app,
            "SELECT COUNT(*) AS c FROM timetest WHERE __time >= TIMESTAMP '2024-01-01 03:00:00'",
            json!({ "sqlTimeZone": tz }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "tz = {tz}, body = {body}");
        let c = body
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|r| r.get("c"))
            .and_then(Value::as_i64);
        assert_eq!(c, Some(4), "tz = {tz}, body = {body}");
    }
}

/// A non-string `sqlTimeZone` is malformed — fail closed, never guess.
#[tokio::test]
async fn non_string_sql_time_zone_fails_closed() {
    let app = setup_timetest_app().await;
    let (status, body) = post_sql_with_context(
        app,
        "SELECT COUNT(*) AS c FROM timetest",
        json!({ "sqlTimeZone": 9 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
}

/// `BETWEEN TIMESTAMP '...' AND TIMESTAMP '...'` (inclusive) also folds.
#[tokio::test]
async fn where_timestamp_literal_between_filters_rows() {
    let c = count_where(
        "__time BETWEEN TIMESTAMP '2024-01-01 01:00:00' AND TIMESTAMP '2024-01-01 05:00:00'",
    )
    .await;
    assert_eq!(c, 5); // rows at 01,02,03,04,05 (inclusive)
}

// ===========================================================================
// MIN(__time) / MAX(__time) — ISO-8601 string on the SQL wire
// ===========================================================================

/// `MIN(__time)`/`MAX(__time)` emit Druid's ISO-8601 millis string on the
/// SQL wire — previously raw epoch millis.
#[tokio::test]
async fn min_max_time_emit_iso_strings() {
    let app = setup_timetest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT MIN(__time) AS mn, MAX(__time) AS mx FROM timetest",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let row = &body.as_array().expect("array body")[0];
    assert_eq!(
        row.get("mn"),
        Some(&json!("2024-01-01T00:00:00.000Z")),
        "body = {body}"
    );
    assert_eq!(
        row.get("mx"),
        Some(&json!("2024-01-01T06:00:00.000Z")),
        "body = {body}"
    );
}

/// Grouped MIN(__time) keeps the ISO shape per group (GroupBy wire arm).
#[tokio::test]
async fn grouped_min_time_emits_iso_strings() {
    let app = setup_timetest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id, MIN(__time) AS mn FROM timetest GROUP BY site_id",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let mut rows: Vec<(String, Value)> = body
        .as_array()
        .expect("array body")
        .iter()
        .map(|r| {
            (
                r.get("site_id")
                    .and_then(Value::as_str)
                    .expect("site_id")
                    .to_string(),
                r.get("mn").cloned().unwrap_or(Value::Null),
            )
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        rows,
        vec![
            ("site_a".to_string(), json!("2024-01-01T00:00:00.000Z")),
            ("site_b".to_string(), json!("2024-01-01T03:00:00.000Z")),
        ],
        "body = {body}"
    );
}

/// Guard: MIN/MAX over a non-time metric keeps its numeric wire shape —
/// the ISO formatting is scoped to TIMESTAMP-typed output columns only.
#[tokio::test]
async fn min_max_non_time_stays_numeric() {
    let app = setup_timetest_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT MIN(\"value\") AS mn, MAX(\"value\") AS mx FROM timetest",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let row = &body.as_array().expect("array body")[0];
    assert_eq!(row.get("mn"), Some(&json!(10)), "body = {body}");
    assert_eq!(row.get("mx"), Some(&json!(70)), "body = {body}");
}

/// Guard: the native `/druid/v2` endpoint keeps epoch millis for a
/// timeseries longMin over `__time` (ISO formatting is SQL-wire only).
#[tokio::test]
async fn native_min_time_keeps_epoch_millis() {
    let app = setup_timetest_app().await;
    let body = json!({
        "queryType": "timeseries",
        "dataSource": "timetest",
        "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"],
        "granularity": "all",
        "aggregations": [
            { "type": "longMin", "name": "mn", "fieldName": "__time" }
        ]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/v2")
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
    assert_eq!(status, StatusCode::OK, "body = {json}");
    let mn = json
        .as_array()
        .and_then(|a| a.first())
        .and_then(|e| e.get("result"))
        .and_then(|r| r.get("mn"));
    assert_eq!(
        mn,
        Some(&json!(1_704_067_200_000_i64)),
        "native wire must keep epoch millis, body = {json}"
    );
}
