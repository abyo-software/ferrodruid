// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! REST-level fail-closed exact-cardinality saturation e2e (2026-07-11).
//!
//! Exact `COUNT(DISTINCT col)` (E16, `useApproximateCountDistinct: false`)
//! lowers to the exact `cardinality` aggregator, whose distinct set is
//! capped at `MAX_CARDINALITY_SET_SIZE` (Wave 36-G2 DoS bound). Before this
//! program a saturated set silently finalized to a capped (under-counted)
//! scalar. Druid never silently returns a wrong exact distinct count, so a
//! saturated result must FAIL the query with a Druid-shaped error envelope:
//! HTTP 400, `errorClass = io.druid.query.ResourceLimitExceededException`,
//! and a message naming the limit and the `APPROX_COUNT_DISTINCT` remedy.
//!
//! Every test lowers the cap to the same small value ([`TEST_CAP`]) via
//! the aggregator crate's test-only override (lower-only, process-wide) so
//! saturation can be driven without 1,000,000 distinct keys. This file is
//! its own integration-test binary, so the override cannot leak into any
//! other test process.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ferrodruid_aggregator::set_exact_cardinality_cap_for_tests;
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

/// Exact-set cap shared by every test in this binary (see module docs).
const TEST_CAP: usize = 3;

/// 6-row segment: `device_id` has 6 distinct values (saturates a cap of
/// 3); `site_id` has 2 distinct values (stays exact under the cap).
fn build_cardtest_segment() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("base ts")
        .timestamp_millis();
    let num_rows = 6usize;
    let timestamps: Vec<i64> = (0..num_rows as i64).map(|i| base + i * 3_600_000).collect();

    let device_ords: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    let device_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(
            (0..6).map(|i| format!("d{i}")).collect::<Vec<_>>(),
        ),
        encoded_values: device_ords.clone(),
        bitmap_indexes: build_bitmaps(6, &device_ords),
    });

    let site_ords: Vec<u32> = vec![0, 0, 0, 1, 1, 1];
    let site_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec![
            "site_a".to_string(),
            "site_b".to_string(),
        ]),
        encoded_values: site_ords.clone(),
        bitmap_indexes: build_bitmaps(2, &site_ords),
    });

    let mut columns = HashMap::new();
    columns.insert("__time".to_string(), ColumnData::Long(timestamps));
    columns.insert("device_id".to_string(), device_col);
    columns.insert("site_id".to_string(), site_col);

    SegmentData {
        version: 9,
        num_rows,
        interval: ferrodruid_segment::Interval {
            start_millis: base,
            end_millis: base + 86_400_000,
        },
        dimensions: vec!["site_id".to_string(), "device_id".to_string()],
        metrics: Vec::new(),
        columns,
        time_sorted: true,
    }
}

/// Second segment for the SAME datasource + interval (multi-segment
/// union-over-cap test): `site_id` has 3 distinct values {site_b, site_c,
/// site_d} — each within the cap of 3 per segment, but the cross-segment
/// union with the first segment's {site_a, site_b} is 4 > 3.
fn build_cardtest_segment_two() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z")
        .expect("base ts")
        .timestamp_millis();
    let num_rows = 3usize;
    let timestamps: Vec<i64> = (0..num_rows as i64).map(|i| base + i * 3_600_000).collect();

    let device_ords: Vec<u32> = vec![0, 0, 0];
    let device_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec!["d0".to_string()]),
        encoded_values: device_ords.clone(),
        bitmap_indexes: build_bitmaps(1, &device_ords),
    });

    let site_ords: Vec<u32> = vec![0, 1, 2];
    let site_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec![
            "site_b".to_string(),
            "site_c".to_string(),
            "site_d".to_string(),
        ]),
        encoded_values: site_ords.clone(),
        bitmap_indexes: build_bitmaps(3, &site_ords),
    });

    let mut columns = HashMap::new();
    columns.insert("__time".to_string(), ColumnData::Long(timestamps));
    columns.insert("device_id".to_string(), device_col);
    columns.insert("site_id".to_string(), site_col);

    let day_start = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("day start")
        .timestamp_millis();
    SegmentData {
        version: 9,
        num_rows,
        interval: ferrodruid_segment::Interval {
            start_millis: day_start,
            end_millis: day_start + 86_400_000,
        },
        dimensions: vec!["site_id".to_string(), "device_id".to_string()],
        metrics: Vec::new(),
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

async fn setup_app() -> Router {
    setup_app_inner(false).await
}

/// Same app with a SECOND segment loaded for the same datasource +
/// interval (multi-segment union-over-cap fail-closed test).
async fn setup_app_two_segments() -> Router {
    setup_app_inner(true).await
}

async fn setup_app_inner(two_segments: bool) -> Router {
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("create metadata store");
    metadata.initialize().await.expect("initialize schema");
    let metadata = Arc::new(metadata);

    let coordinator = Arc::new(Coordinator::new(Arc::clone(&metadata)));
    let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));

    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 100_000_000);
    let segment_id = "cardtest_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_v1_0";
    historical
        .load_segment(segment_id, build_cardtest_segment())
        .expect("load segment");
    historical
        .set_segment_datasource(segment_id, "cardtest")
        .expect("set datasource");
    if two_segments {
        let segment_id_two = "cardtest_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_v1_1";
        historical
            .load_segment(segment_id_two, build_cardtest_segment_two())
            .expect("load segment two");
        historical
            .set_segment_datasource(segment_id_two, "cardtest")
            .expect("set datasource two");
    }
    let historical = Arc::new(historical);

    let seg_row = SegmentMetadataRow {
        id: segment_id.to_string(),
        data_source: "cardtest".to_string(),
        created_date: "2024-01-01T00:00:00.000Z".to_string(),
        start: "2024-01-01T00:00:00.000Z".to_string(),
        end: "2024-01-02T00:00:00.000Z".to_string(),
        version: "v1".to_string(),
        used: true,
        payload: json!({
            "dataSource": "cardtest",
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

async fn post_json(app: Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
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

fn assert_druid_resource_limit_envelope(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errorClass").and_then(Value::as_str),
        Some("io.druid.query.ResourceLimitExceededException"),
        "body = {body}"
    );
    let msg = body
        .get("errorMessage")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        msg.contains("cardinality.maxExactSetSize"),
        "errorMessage must name the exact-set limit, got: {msg}"
    );
    assert!(
        msg.contains(&format!("limit={TEST_CAP}")),
        "errorMessage must carry the effective limit value, got: {msg}"
    );
    assert!(
        msg.contains("APPROX_COUNT_DISTINCT"),
        "errorMessage must point at the approximate alternative, got: {msg}"
    );
    assert!(body.get("error").is_some(), "body = {body}");
}

/// E16 exact COUNT(DISTINCT) over a column whose distinct count exceeds
/// the (test-lowered) exact-set cap must fail closed with the Druid-shaped
/// resource-limit envelope — never a silently capped number.
#[tokio::test]
async fn sql_exact_count_distinct_saturation_fails_closed() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let app = setup_app().await;
    let (status, body) = post_json(
        app,
        "/druid/v2/sql",
        json!({
            "query": "SELECT COUNT(DISTINCT device_id) AS uniq FROM cardtest",
            "context": {"useApproximateCountDistinct": false}
        }),
    )
    .await;
    assert_druid_resource_limit_envelope(status, &body);
}

/// Regression guard: a non-saturated exact COUNT(DISTINCT) keeps returning
/// its exact count (2 distinct sites, cap 3).
#[tokio::test]
async fn sql_exact_count_distinct_below_cap_stays_exact() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let app = setup_app().await;
    let (status, body) = post_json(
        app,
        "/druid/v2/sql",
        json!({
            "query": "SELECT COUNT(DISTINCT site_id) AS uniq FROM cardtest",
            "context": {"useApproximateCountDistinct": false}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("uniq")),
        Some(&json!(2)),
        "body = {body}"
    );
}

/// The approximate HLL path (`useApproximateCountDistinct` default true)
/// has no exact-set cap and must be unaffected by the fail-closed change —
/// it is the documented remedy for unbounded-cardinality columns.
#[tokio::test]
async fn sql_approx_count_distinct_unaffected_by_cap() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let app = setup_app().await;
    let (status, body) = post_json(
        app,
        "/druid/v2/sql",
        json!({
            "query": "SELECT COUNT(DISTINCT device_id) AS uniq FROM cardtest"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("uniq")),
        Some(&json!(6)),
        "HLL estimate is exact at 6 distinct values; body = {body}"
    );
}

/// Multi-segment exact union (2026-07-12): a cross-segment UNION that
/// exceeds the exact-set cap must still FAIL CLOSED.  Each segment's
/// per-aggregator set stays within the cap of 3 ({site_a, site_b} and
/// {site_b, site_c, site_d}), so the executors emit exact envelopes — but
/// the broker union (4 distinct) exceeds the cap and must degrade to the
/// fail-closed cross-shard merge error, never to the over-counting
/// saturating-add (5) and never to a silently capped number.
#[tokio::test]
async fn sql_exact_count_distinct_multi_segment_union_over_cap_fails_closed() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let app = setup_app_two_segments().await;
    let (status, body) = post_json(
        app,
        "/druid/v2/sql",
        json!({
            "query": "SELECT COUNT(DISTINCT site_id) AS uniq FROM cardtest",
            "context": {"useApproximateCountDistinct": false}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errorClass").and_then(Value::as_str),
        Some("io.druid.query.ResourceLimitExceededException"),
        "body = {body}"
    );
    let msg = body
        .get("errorMessage")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        msg.contains("cardinality.crossShardExactMerge"),
        "errorMessage must name the cross-shard merge limit, got: {msg}"
    );
    assert!(
        msg.contains(&format!("limit={TEST_CAP}")),
        "errorMessage must carry the effective cap, got: {msg}"
    );
    assert!(
        msg.contains("APPROX_COUNT_DISTINCT"),
        "errorMessage must point at the approximate alternative, got: {msg}"
    );
}

/// Counterpart guard: with the SAME two segments, a cross-segment union
/// that stays within the cap returns the exact union — the fail-closed
/// above only fires beyond the cap.  device_id unions to {d0..d5} ∪ {d0}
/// = 6 > 3 (fails closed), while restricting to one segment's sites via
/// WHERE keeps the union at 2 ≤ 3 and must stay exact.
#[tokio::test]
async fn sql_exact_count_distinct_multi_segment_below_cap_stays_exact() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let app = setup_app_two_segments().await;
    // site_b appears in BOTH segments; the per-bucket union of
    // {site_b} ∪ {site_b} = 1 ≤ 3 must return the exact 1.
    let (status, body) = post_json(
        app,
        "/druid/v2/sql",
        json!({
            "query": "SELECT COUNT(DISTINCT site_id) AS uniq FROM cardtest \
                      WHERE site_id = 'site_b'",
            "context": {"useApproximateCountDistinct": false}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("uniq")),
        Some(&json!(1)),
        "within-cap multi-segment union must stay exact (site_b in both \
         segments must not double-count); body = {body}"
    );
}

/// The native `/druid/v2` cardinality query hits the same aggregator and
/// must fail closed with the same envelope.
#[tokio::test]
async fn native_cardinality_saturation_fails_closed() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let app = setup_app().await;
    let (status, body) = post_json(
        app,
        "/druid/v2",
        json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "cardtest"},
            "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
            "granularity": "all",
            "aggregations": [
                {"type": "cardinality", "name": "uniq",
                 "fields": ["device_id"], "byRow": false}
            ]
        }),
    )
    .await;
    assert_druid_resource_limit_envelope(status, &body);
}
