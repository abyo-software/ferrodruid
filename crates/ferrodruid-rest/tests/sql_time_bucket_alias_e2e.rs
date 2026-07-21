// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! REST-level (`/druid/v2/sql`) e2e for the TIME_FLOOR bucket-column wire
//! surfacing when a SELECT alias collides with a HIDDEN `$`-prefixed helper
//! aggregation name (codex-review HIGH finding C on P1-#2).
//!
//! `AVG(x)` lowers to hidden helper aggregations named `$avg_sum_N` /
//! `$avg_count_N`. The REST layer used to infer the time-bucket column by
//! excluding every output column whose NAME matched any native aggregation
//! name — including those hidden helpers. A bucket column legitimately
//! aliased `"$avg_sum_0"` therefore lost its bucket role, and the wire
//! formatter emitted the hidden SUM value as an ISO-8601 timestamp
//! (e.g. `1970-01-01T00:00:00.010Z` for a sum of 10).
//!
//! The bucket column is now marked by the PLANNER by role
//! (`PlannedQuery::time_bucket_column`) — name collisions against hidden
//! helpers can no longer drop it, and hidden `$`-helpers never participate.

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

/// 7-row `buckettest` segment: hourly rows starting 2024-01-01T00:00:00Z
/// (…T00 through …T06), one string dim, one double metric (10.0 … 70.0).
fn build_buckettest_segment() -> SegmentData {
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

async fn setup_app() -> Router {
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("create metadata store");
    metadata.initialize().await.expect("initialize schema");
    let metadata = Arc::new(metadata);

    let coordinator = Arc::new(Coordinator::new(Arc::clone(&metadata)));
    let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));

    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 100_000_000);
    let segment_id = "buckettest_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_v1_0";
    historical
        .load_segment(segment_id, build_buckettest_segment())
        .expect("load segment");
    historical
        .set_segment_datasource(segment_id, "buckettest")
        .expect("set datasource");
    let historical = Arc::new(historical);

    let seg_row = SegmentMetadataRow {
        id: segment_id.to_string(),
        data_source: "buckettest".to_string(),
        created_date: "2024-01-01T00:00:00.000Z".to_string(),
        start: "2024-01-01T00:00:00.000Z".to_string(),
        end: "2024-01-02T00:00:00.000Z".to_string(),
        version: "v1".to_string(),
        used: true,
        payload: json!({
            "dataSource": "buckettest",
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

/// The REPRO (failing on base): the bucket column is aliased to the exact
/// name of AVG's hidden sum helper (`$avg_sum_0`). The inference used to
/// exclude it as "an aggregation output", dropping the bucket role — and
/// the wire then rendered the hidden SUM (10.0 for the T00 bucket) as
/// `1970-01-01T00:00:00.010Z`. The bucket timestamps must surface instead.
#[tokio::test]
async fn bucket_aliased_to_hidden_helper_name_keeps_bucket_role() {
    let app = setup_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT TIME_FLOOR(__time, 'PT1H') AS \"$avg_sum_0\", AVG(\"value\") AS avg_v \
         FROM buckettest GROUP BY 1",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 7, "body = {body}");
    for (i, row) in rows.iter().enumerate() {
        let expected_ts = format!("2024-01-01T{i:02}:00:00.000Z");
        assert_eq!(
            row.get("$avg_sum_0"),
            Some(&json!(expected_ts)),
            "row {i} bucket column must carry the bucket timestamp, body = {body}"
        );
        // 1 row per hourly bucket: AVG == the row's value (10.0 … 70.0).
        #[allow(clippy::cast_precision_loss)]
        let expected_avg = (i as f64 + 1.0) * 10.0;
        assert_eq!(
            row.get("avg_v").and_then(Value::as_f64),
            Some(expected_avg),
            "row {i} avg, body = {body}"
        );
    }
}

/// Same collision through the granular-GroupBy arm (bucket + another
/// grouping dimension + AVG) — also mislabeled on base.
#[tokio::test]
async fn groupby_bucket_aliased_to_hidden_helper_name_keeps_bucket_role() {
    let app = setup_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT TIME_FLOOR(__time, 'PT1H') AS \"$avg_sum_0\", site_id, \
         AVG(\"value\") AS avg_v FROM buckettest GROUP BY 1, site_id",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 7, "body = {body}");
    for row in rows {
        let ts = row
            .get("$avg_sum_0")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            ts.starts_with("2024-01-01T"),
            "bucket column must be a 2024 bucket timestamp, got {row}"
        );
    }
}

/// Guard (green on base AND after): a normally-aliased bucket keeps
/// working — the planner-role marking must not disturb the plain path.
#[tokio::test]
async fn normally_aliased_bucket_still_surfaces() {
    let app = setup_app().await;
    let (status, body) = post_sql(
        app,
        "SELECT TIME_FLOOR(__time, 'PT1H') AS bucket, AVG(\"value\") AS avg_v \
         FROM buckettest GROUP BY 1",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 7, "body = {body}");
    assert_eq!(
        rows[0].get("bucket"),
        Some(&json!("2024-01-01T00:00:00.000Z")),
        "body = {body}"
    );
    assert_eq!(
        rows[0].get("avg_v").and_then(Value::as_f64),
        Some(10.0),
        "body = {body}"
    );
}

/// Guard (green on base AND after): `MAX(__time)` stays an aggregate VALUE
/// (P1-#2) — the planner never marks it as the bucket column, so it must
/// not be clobbered by the bucket envelope (epoch 0 under granularity=all).
#[tokio::test]
async fn max_time_aggregate_is_not_mistaken_for_bucket() {
    let app = setup_app().await;
    let (status, body) = post_sql(app, "SELECT MAX(__time) AS mx FROM buckettest").await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array().expect("array body")[0].get("mx"),
        Some(&json!("2024-01-01T06:00:00.000Z")),
        "body = {body}"
    );
}
