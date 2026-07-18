// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! REST-level native-wire sketch finalization e2e (P1-#3, 2026-07-12).
//!
//! A native `/druid/v2` query whose `aggregations` include a raw sketch
//! aggregator (`HLLSketchBuild`, `thetaSketch`, `quantilesDoublesSketch`)
//! must return the FINALIZED scalar by default, matching Apache Druid's
//! native `finalize` semantics (measured against real Druid 36.0.0 on
//! 2026-07-12, local Docker, `wikipedia_compat` fixture):
//!
//! * `HLLSketchBuild` / `HLLSketchMerge` — unrounded double estimate
//!   (e.g. `8.000000139077509`);
//! * `thetaSketch` — double estimate (e.g. `8.0`);
//! * `quantilesDoublesSketch` — the sketch's value COUNT `n` as an
//!   integer (e.g. `10`, NOT a quantile);
//! * empty timeseries buckets finalize to the empty-sketch scalar
//!   (`0.0` / `0`), while `longSum` stays `null` and `count` stays `0`;
//! * `"context":{"finalize":false}` keeps the intermediate (FerroDruid's
//!   `@sketch` partial-state envelope; Druid ships DataSketches base64 —
//!   intermediate representations are engine-internal by design);
//! * `bloomFilter` is NOT finalized to a scalar — Druid's finalized bloom
//!   aggregation IS the filter itself.
//!
//! Failing-first: on base `854411c` the assertions on the default
//! (finalize) shape fail because the raw `@sketch` envelope leaked onto
//! the native wire for timeseries / topN / groupBy.

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

/// 6-row segment: `user` has 4 distinct values (u0 u0 u1 u2 u3 u3),
/// `language` has 2 distinct values, `added` is a long metric.
/// Rows are one hour apart starting 2024-01-01T00:00Z, with a gap at
/// T02 (rows at hours 0,1,3,4,5,6) so hour-grain timeseries has an
/// empty bucket inside the fill range.
fn build_sketch_segment() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("base ts")
        .timestamp_millis();
    let num_rows = 6usize;
    let hours: [i64; 6] = [0, 1, 3, 4, 5, 6];
    let timestamps: Vec<i64> = hours.iter().map(|h| base + h * 3_600_000).collect();

    let user_ords: Vec<u32> = vec![0, 0, 1, 2, 3, 3];
    let user_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(
            (0..4).map(|i| format!("u{i}")).collect::<Vec<_>>(),
        ),
        encoded_values: user_ords.clone(),
        bitmap_indexes: build_bitmaps(4, &user_ords),
    });

    let lang_ords: Vec<u32> = vec![0, 0, 0, 0, 1, 1];
    let lang_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec!["en".to_string(), "fr".to_string()]),
        encoded_values: lang_ords.clone(),
        bitmap_indexes: build_bitmaps(2, &lang_ords),
    });

    let added: Vec<i64> = vec![100, 50, 200, 150, 75, 300];

    let mut columns = HashMap::new();
    columns.insert("__time".to_string(), ColumnData::Long(timestamps));
    columns.insert("user".to_string(), user_col);
    columns.insert("language".to_string(), lang_col);
    columns.insert("added".to_string(), ColumnData::Long(added));

    SegmentData {
        version: 9,
        num_rows,
        interval: ferrodruid_segment::Interval {
            start_millis: base,
            end_millis: base + 86_400_000,
        },
        dimensions: vec!["language".to_string(), "user".to_string()],
        metrics: vec!["added".to_string()],
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

/// Second segment for the SAME datasource + interval: 3 rows with users
/// u10..u12 — DISJOINT from the first segment's u0..u3, so the merged
/// distinct-user union is 7.  Used to prove the broker merges sketch
/// INTERMEDIATES across segments first and the wire finalizes the merged
/// sketch (never per-segment scalars).
fn build_sketch_segment_two() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T07:00:00Z")
        .expect("base ts")
        .timestamp_millis();
    let num_rows = 3usize;
    let timestamps: Vec<i64> = (0..num_rows as i64).map(|i| base + i * 3_600_000).collect();

    let user_ords: Vec<u32> = vec![0, 1, 2];
    let user_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(
            (10..13).map(|i| format!("u{i}")).collect::<Vec<_>>(),
        ),
        encoded_values: user_ords.clone(),
        bitmap_indexes: build_bitmaps(3, &user_ords),
    });

    let lang_ords: Vec<u32> = vec![0, 0, 0];
    let lang_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec!["en".to_string()]),
        encoded_values: lang_ords.clone(),
        bitmap_indexes: build_bitmaps(1, &lang_ords),
    });

    let added: Vec<i64> = vec![10, 20, 30];

    let mut columns = HashMap::new();
    columns.insert("__time".to_string(), ColumnData::Long(timestamps));
    columns.insert("user".to_string(), user_col);
    columns.insert("language".to_string(), lang_col);
    columns.insert("added".to_string(), ColumnData::Long(added));

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
        dimensions: vec!["language".to_string(), "user".to_string()],
        metrics: vec!["added".to_string()],
        columns,
        time_sorted: true,
    }
}

async fn setup_app() -> Router {
    setup_app_inner(false).await
}

/// Same app with a SECOND segment loaded for the same datasource +
/// interval (broker merge-then-finalize ordering test).
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
    let segment_id = "sketchtest_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_v1_0";
    historical
        .load_segment(segment_id, build_sketch_segment())
        .expect("load segment");
    historical
        .set_segment_datasource(segment_id, "sketchtest")
        .expect("set datasource");
    if two_segments {
        let segment_id_two = "sketchtest_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_v1_1";
        historical
            .load_segment(segment_id_two, build_sketch_segment_two())
            .expect("load segment two");
        historical
            .set_segment_datasource(segment_id_two, "sketchtest")
            .expect("set datasource two");
    }
    let historical = Arc::new(historical);

    let seg_row = SegmentMetadataRow {
        id: segment_id.to_string(),
        data_source: "sketchtest".to_string(),
        created_date: "2024-01-01T00:00:00.000Z".to_string(),
        start: "2024-01-01T00:00:00.000Z".to_string(),
        end: "2024-01-02T00:00:00.000Z".to_string(),
        version: "v1".to_string(),
        used: true,
        payload: json!({
            "dataSource": "sketchtest",
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

async fn post_native(app: Router, body: Value) -> (StatusCode, Value) {
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
    (status, json)
}

/// Assert `v` is a finite JSON number whose value rounds to `expected`.
fn assert_estimate(v: &Value, expected: f64, label: &str) {
    let f = v
        .as_f64()
        .unwrap_or_else(|| panic!("{label}: expected a JSON number, got {v}"));
    assert!(
        (f - expected).abs() < 0.5,
        "{label}: estimate {f} does not round to {expected}"
    );
}

fn assert_envelope(v: &Value, tag: &str, label: &str) {
    assert_eq!(
        v.get("@sketch").and_then(Value::as_str),
        Some(tag),
        "{label}: expected a '{tag}' @sketch envelope, got {v}"
    );
}

// ---------------------------------------------------------------------------
// Timeseries
// ---------------------------------------------------------------------------

/// Default (no context): raw HLL / theta / quantiles aggregations finalize
/// to scalars on the native wire, matching measured Druid 36.
#[tokio::test]
async fn timeseries_raw_sketches_finalize_to_scalars_by_default() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"},
                {"type": "thetaSketch", "name": "tt", "fieldName": "user"},
                {"type": "quantilesDoublesSketch", "name": "qq", "fieldName": "added"}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let result = &body[0]["result"];
    assert_estimate(&result["uu"], 4.0, "timeseries uu (HLL)");
    assert_estimate(&result["tt"], 4.0, "timeseries tt (theta)");
    // Druid finalizes quantilesDoublesSketch to its value count `n` as an
    // integer (measured: `10` over the 10-row fixture).
    assert_eq!(
        result["qq"],
        json!(6),
        "timeseries qq (quantiles n), body = {body}"
    );
}

/// `"finalize": true` explicitly — same scalars as the default.
#[tokio::test]
async fn timeseries_finalize_true_explicit_matches_default() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"}
            ],
            "context": {"finalize": true}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_estimate(&body[0]["result"]["uu"], 4.0, "timeseries uu finalize=true");
}

/// `"finalize": false` keeps the intermediate `@sketch` envelope so a
/// merging consumer can union exact sketches.
#[tokio::test]
async fn timeseries_finalize_false_keeps_intermediate_envelope() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"},
                {"type": "thetaSketch", "name": "tt", "fieldName": "user"},
                {"type": "quantilesDoublesSketch", "name": "qq", "fieldName": "added"}
            ],
            "context": {"finalize": false}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let result = &body[0]["result"];
    assert_envelope(&result["uu"], "hll", "timeseries uu finalize=false");
    assert_envelope(&result["tt"], "theta", "timeseries tt finalize=false");
    assert_envelope(&result["qq"], "quantiles", "timeseries qq finalize=false");
}

/// Empty hour-grain buckets finalize sketches to the empty-sketch scalar
/// (`0.0` HLL/theta, `0` quantiles) while `longSum` stays `null` and
/// `count` stays `0` — measured Druid 36 shape.
#[tokio::test]
async fn timeseries_empty_bucket_finalizes_to_zero_scalars() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "hour",
            "intervals": ["2024-01-01T00:00:00Z/2024-01-01T07:00:00Z"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"},
                {"type": "quantilesDoublesSketch", "name": "qq", "fieldName": "added"},
                {"type": "longSum", "name": "a", "fieldName": "added"},
                {"type": "count", "name": "c"}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    // The T02 bucket is empty but inside the fill range (rows at T00..T06
    // with a gap at T02).
    let empty = rows
        .iter()
        .find(|r| {
            r["timestamp"]
                .as_str()
                .is_some_and(|t| t.starts_with("2024-01-01T02"))
        })
        .unwrap_or_else(|| panic!("no T02 bucket in {body}"));
    assert_eq!(empty["result"]["uu"], json!(0.0), "empty-bucket HLL");
    assert_eq!(empty["result"]["qq"], json!(0), "empty-bucket quantiles n");
    assert_eq!(empty["result"]["a"], Value::Null, "empty-bucket longSum");
    assert_eq!(empty["result"]["c"], json!(0), "empty-bucket count");
}

// ---------------------------------------------------------------------------
// GroupBy / TopN
// ---------------------------------------------------------------------------

/// GroupBy events finalize raw sketch outputs per group.
#[tokio::test]
async fn groupby_raw_sketches_finalize_per_group() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "groupBy",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "dimensions": ["language"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"},
                {"type": "quantilesDoublesSketch", "name": "qq", "fieldName": "added"}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("array body");
    assert_eq!(rows.len(), 2, "body = {body}");
    for row in rows {
        let event = &row["event"];
        let lang = event["language"].as_str().expect("language");
        // en rows: users u0,u0,u1,u2 (3 distinct, 4 values);
        // fr rows: users u3,u3 (1 distinct, 2 values).
        let (exp_uu, exp_qq) = if lang == "en" { (3.0, 4) } else { (1.0, 2) };
        assert_estimate(&event["uu"], exp_uu, &format!("groupBy uu[{lang}]"));
        assert_eq!(
            event["qq"],
            json!(exp_qq),
            "groupBy qq[{lang}], body = {body}"
        );
    }
}

/// GroupBy with `finalize:false` keeps envelopes.
#[tokio::test]
async fn groupby_finalize_false_keeps_envelopes() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "groupBy",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "dimensions": ["language"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"}
            ],
            "context": {"finalize": false}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    for row in body.as_array().expect("array body") {
        assert_envelope(&row["event"]["uu"], "hll", "groupBy finalize=false");
    }
}

/// TopN result rows finalize raw sketch outputs.
#[tokio::test]
async fn topn_raw_sketches_finalize_in_rows() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "topN",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "dimension": "language",
            "metric": "cnt",
            "threshold": 3,
            "aggregations": [
                {"type": "count", "name": "cnt"},
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body[0]["result"].as_array().expect("topN rows");
    assert_eq!(rows.len(), 2, "body = {body}");
    for row in rows {
        let lang = row["language"].as_str().expect("language");
        let exp = if lang == "en" { 3.0 } else { 1.0 };
        assert_estimate(&row["uu"], exp, &format!("topN uu[{lang}]"));
    }
}

/// A `filtered`-wrapped sketch aggregation finalizes through the wrapper.
#[tokio::test]
async fn filtered_wrapped_sketch_finalizes() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {
                    "type": "filtered",
                    "filter": {"type": "selector", "dimension": "language", "value": "en"},
                    "aggregator": {"type": "HLLSketchBuild", "name": "uu_en", "fieldName": "user"}
                }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_estimate(&body[0]["result"]["uu_en"], 3.0, "filtered HLL");
}

/// TWO segments, disjoint user sets (4 + 3 distinct): the broker must
/// merge the sketch INTERMEDIATES across segments first, and only the
/// wire output finalizes — so the finalized estimate reflects the union
/// (7), never a single segment's scalar.  Guards the "do not finalize
/// before/during merge" ordering.
#[tokio::test]
async fn two_segment_merge_happens_on_intermediates_then_finalizes() {
    let (status, body) = post_native(
        setup_app_two_segments().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"},
                {"type": "quantilesDoublesSketch", "name": "qq", "fieldName": "added"}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let result = &body[0]["result"];
    assert_estimate(&result["uu"], 7.0, "two-segment merged HLL union");
    // 6 + 3 values fed across the two segments.
    assert_eq!(result["qq"], json!(9), "two-segment merged quantiles n");
}

/// Same two-segment app with `finalize:false`: the MERGED intermediate
/// envelope ships (cross-cluster consumers can keep unioning it).
#[tokio::test]
async fn two_segment_finalize_false_ships_merged_envelope() {
    let (status, body) = post_native(
        setup_app_two_segments().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"}
            ],
            "context": {"finalize": false}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let envelope = &body[0]["result"]["uu"];
    assert_envelope(envelope, "hll", "two-segment finalize=false");
    // The envelope's convenience estimate reflects the merged union.
    let est = envelope["estimate"].as_f64().expect("envelope estimate");
    assert!(
        (est - 7.0).abs() < 0.5,
        "merged envelope estimate {est} does not round to 7"
    );
}

// ---------------------------------------------------------------------------
// Per-aggregator `shouldFinalize` (codex-review HIGH finding D)
// ---------------------------------------------------------------------------

/// An aggregator with `"shouldFinalize": false` keeps its INTERMEDIATE
/// sketch even under the default (context finalize = true) semantics —
/// matching Druid, where the per-aggregator flag overrides the context
/// default for that aggregator. Previously the flag was silently ignored
/// and the sketch was finalized anyway.
#[tokio::test]
async fn should_finalize_false_keeps_intermediate_by_default() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user",
                 "shouldFinalize": false},
                {"type": "thetaSketch", "name": "tt", "fieldName": "user",
                 "shouldFinalize": false},
                {"type": "quantilesDoublesSketch", "name": "qq", "fieldName": "added",
                 "shouldFinalize": false}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let result = &body[0]["result"];
    assert_envelope(&result["uu"], "hll", "shouldFinalize=false HLL");
    assert_envelope(&result["tt"], "theta", "shouldFinalize=false theta");
    assert_envelope(&result["qq"], "quantiles", "shouldFinalize=false quantiles");
}

/// `shouldFinalize: false` also wins over an EXPLICIT context
/// `finalize: true` (per-aggregator override, Druid semantics).
#[tokio::test]
async fn should_finalize_false_overrides_explicit_context_finalize_true() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user",
                 "shouldFinalize": false}
            ],
            "context": {"finalize": true}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_envelope(
        &body[0]["result"]["uu"],
        "hll",
        "shouldFinalize=false vs finalize=true",
    );
}

/// The override is PER aggregator: a sibling without the flag still
/// finalizes in the same query.
#[tokio::test]
async fn should_finalize_false_is_per_aggregator() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "raw", "fieldName": "user",
                 "shouldFinalize": false},
                {"type": "HLLSketchBuild", "name": "fin", "fieldName": "user"}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let result = &body[0]["result"];
    assert_envelope(&result["raw"], "hll", "shouldFinalize=false sibling");
    assert_estimate(&result["fin"], 4.0, "default sibling still finalizes");
}

/// An explicit `shouldFinalize: true` matches the default (finalizes).
#[tokio::test]
async fn should_finalize_true_explicit_matches_default() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user",
                 "shouldFinalize": true}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_estimate(&body[0]["result"]["uu"], 4.0, "shouldFinalize=true");
}

/// The flag reaches through a `filtered` wrapper (the wrapper delegates
/// finalization to its inner aggregator, which carries the flag).
#[tokio::test]
async fn filtered_wrapped_should_finalize_false_keeps_intermediate() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {
                    "type": "filtered",
                    "filter": {"type": "selector", "dimension": "language", "value": "en"},
                    "aggregator": {"type": "HLLSketchBuild", "name": "uu_en",
                                   "fieldName": "user", "shouldFinalize": false}
                }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_envelope(
        &body[0]["result"]["uu_en"],
        "hll",
        "filtered shouldFinalize=false",
    );
}

/// GroupBy rows honor the per-aggregator opt-out too.
#[tokio::test]
async fn groupby_should_finalize_false_keeps_envelopes() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "groupBy",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "dimensions": ["language"],
            "aggregations": [
                {"type": "thetaSketch", "name": "tt", "fieldName": "user",
                 "shouldFinalize": false}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    for row in body.as_array().expect("array body") {
        assert_envelope(&row["event"]["tt"], "theta", "groupBy shouldFinalize=false");
    }
}

/// `bloomFilter` is NOT finalized to a scalar: Druid's finalized bloom
/// aggregation is the filter itself, so the (FerroDruid-format) filter
/// envelope stays on the wire under default finalize.
#[tokio::test]
async fn bloom_filter_is_not_over_finalized() {
    let (status, body) = post_native(
        setup_app().await,
        json!({
            "queryType": "timeseries",
            "dataSource": "sketchtest",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [
                {"type": "bloomFilter", "name": "bf", "fieldName": "user", "numEntries": 64}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_envelope(
        &body[0]["result"]["bf"],
        "ferrodruid-bloom-v1",
        "bloom default finalize",
    );
}
