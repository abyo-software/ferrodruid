// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! REST-level multi-segment exact `COUNT(DISTINCT)` union e2e (2026-07-12).
//!
//! Exact `COUNT(DISTINCT col)` (E16, `useApproximateCountDistinct: false`)
//! lowers to the exact `cardinality` aggregator.  Pre-fix, the query
//! executors emitted the bare per-segment count as the partial, so the
//! broker could not set-union across segments and the fail-closed program
//! (2026-07-11) rejected ANY exact-distinct query whose same time bucket /
//! group was produced by two or more segments (CL-C2).  The executors now
//! emit the full-set `CardinalityState` envelope as the partial and the
//! broker unions the exact sets, so multi-segment exact distinct counts
//! return the EXACT UNION (not the over-counting per-segment sum, and not
//! fail-closed) up to the 1,000,000-key exact-set cap.
//!
//! Ingestion fidelity note (honest limitation): the REST
//! `POST /druid/indexer/v1/task` endpoint only records the spec on the
//! overlord — it does not run a batch task — so "two `index_parallel`
//! appends to the same datasource + interval" cannot be driven through
//! HTTP in this harness.  Following the established
//! `end_to_end_pipeline.rs` pattern, each segment is produced by the
//! production [`ferrodruid_ingest_batch::BatchIngester`] (the exact call
//! site a real batch task uses) and loaded into one Historical under the
//! same datasource, which yields byte-identical segments to a production
//! two-append ingest.  Queries then run through the REAL wire path:
//! REST router → `Broker::execute_local` → Historical (one partial per
//! segment) → `Broker::merge_results` → cardinality finalization.
//!
//! No cardinality-cap override is used — these tests exercise the
//! PRODUCTION caps (exact up to 1,000,000 keys).  The over-cap fail-closed
//! counterpart lives in `cardinality_failclosed_e2e.rs` (test-lowered cap).

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
use ferrodruid_metadata::MetadataStore;
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_rest::{AppState, create_router};
use serde_json::{Value, json};
use tower::ServiceExt;

const INTERVAL_START: &str = "2024-01-01T00:00:00.000Z";
const INTERVAL_END: &str = "2024-01-02T00:00:00.000Z";

/// Boot an in-process app with one Historical holding ONE SEGMENT PER
/// entry of `row_batches`, all registered to the same `datasource` and
/// covering the same interval (the "two appends to one datasource +
/// interval" shape).  Every segment is produced by the production
/// `BatchIngester`.
async fn boot_app(datasource: &str, row_batches: Vec<Vec<Value>>) -> Router {
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("create metadata store");
    metadata.initialize().await.expect("initialize schema");
    let metadata = Arc::new(metadata);

    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 1_000_000_000);

    for (i, rows) in row_batches.into_iter().enumerate() {
        let ingester = BatchIngester::new(
            datasource.to_string(),
            "__time".to_string(),
            vec!["site_id".to_string(), "device_id".to_string()],
            Vec::new(),
        );
        let row_count = rows.len();
        let ingested = ingester.ingest(rows).expect("BatchIngester::ingest");
        assert_eq!(ingested.num_rows, row_count, "ingester must keep all rows");
        let segment_id = format!("{datasource}_{INTERVAL_START}_{INTERVAL_END}_v1_{i}");
        historical
            .load_segment(&segment_id, ingested.segment_data)
            .expect("load segment");
        historical
            .set_segment_datasource(&segment_id, datasource)
            .expect("set datasource");
    }
    let historical = Arc::new(historical);

    let state = Arc::new(AppState {
        coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
        overlord: Arc::new(Overlord::new(Arc::clone(&metadata))),
        metadata,
        auth_store: Arc::new(parking_lot::RwLock::new(AuthStore::new())),
        auth_cred_dir: None,
        authorizer: Arc::new(Authorizer::new().with_admin_role()),
        auth_enabled: false,
        broker: Arc::new(Broker::new()),
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

fn row(ts_hour: u32, site: &str, device: &str) -> Value {
    json!({
        "__time": format!("2024-01-01T{ts_hour:02}:00:00Z"),
        "site_id": site,
        "device_id": device,
    })
}

/// The canonical overlapping two-segment shape: seg1 devices {d1,d2,d3},
/// seg2 devices {d2,d3,d4} — true exact distinct = 4 (over-count = 6).
fn overlapping_batches() -> Vec<Vec<Value>> {
    vec![
        vec![
            row(0, "site_a", "d1"),
            row(1, "site_a", "d2"),
            row(2, "site_b", "d3"),
        ],
        vec![
            row(3, "site_a", "d2"),
            row(4, "site_b", "d3"),
            row(5, "site_b", "d4"),
        ],
    ]
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

async fn post_sql(app: Router, sql: &str, exact: bool) -> (StatusCode, Value) {
    let body = if exact {
        json!({"query": sql, "context": {"useApproximateCountDistinct": false}})
    } else {
        json!({"query": sql})
    };
    post_json(app, "/druid/v2/sql", body).await
}

/// THE CRUX (CL-C2 closure): exact COUNT(DISTINCT) over a datasource whose
/// single time bucket is produced by TWO segments with partially
/// overlapping device sets must return the exact union (4) — NOT the
/// per-segment over-count (6) and NOT the fail-closed 400 the pre-fix
/// bare-count partials forced.
#[tokio::test]
async fn sql_exact_count_distinct_multi_segment_returns_exact_union() {
    let app = boot_app("cardu_sql", overlapping_batches()).await;
    let (status, body) = post_sql(
        app,
        "SELECT COUNT(DISTINCT device_id) AS uniq FROM cardu_sql",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("uniq")),
        Some(&json!(4)),
        "must be the exact union 4 (not the over-count 6, not fail-closed); body = {body}"
    );
}

/// Same crux through the native `/druid/v2` wire: a timeseries
/// `cardinality` aggregation across two overlapping segments returns the
/// exact union count.
#[tokio::test]
async fn native_timeseries_multi_segment_returns_exact_union() {
    let app = boot_app("cardu_native", overlapping_batches()).await;
    let (status, body) = post_json(
        app,
        "/druid/v2",
        json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "cardu_native"},
            "intervals": [format!("{INTERVAL_START}/{INTERVAL_END}")],
            "granularity": "all",
            "aggregations": [
                {"type": "cardinality", "name": "uniq",
                 "fields": ["device_id"], "byRow": false}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let uniq = body
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|r| r.get("result"))
        .and_then(|r| r.get("uniq"));
    assert_eq!(
        uniq,
        Some(&json!(4)),
        "native cardinality across 2 segments must union to 4; body = {body}"
    );
    // The internal CardinalityState envelope must never leak to clients.
    assert!(
        uniq.is_some_and(Value::is_u64),
        "wire value must be a bare integer, not an envelope; body = {body}"
    );
}

/// GroupBy: each site group spans both segments; per-group exact unions
/// must not over-count the devices seen by both segments.
/// site_a = {d1,d2} ∪ {d2} = 2; site_b = {d3} ∪ {d3,d4} = 2.
#[tokio::test]
async fn sql_groupby_multi_segment_exact_union_per_group() {
    let app = boot_app("cardu_grp", overlapping_batches()).await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id, COUNT(DISTINCT device_id) AS uniq FROM cardu_grp \
         GROUP BY site_id",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("rows");
    assert_eq!(rows.len(), 2, "two site groups; body = {body}");
    for (site, expected) in [("site_a", 2u64), ("site_b", 2u64)] {
        let row = rows
            .iter()
            .find(|r| r.get("site_id").and_then(Value::as_str) == Some(site))
            .unwrap_or_else(|| panic!("row for {site} missing; body = {body}"));
        assert_eq!(
            row.get("uniq"),
            Some(&json!(expected)),
            "per-group union for {site}; body = {body}"
        );
    }
}

/// Native topN ranked BY the exact-cardinality metric across two segments:
/// the ranking must use the exact union counts (per-segment partial counts
/// are substituted after per-segment ranking; the broker re-ranks on the
/// union).  site_a and site_b both union to 2 devices.
#[tokio::test]
async fn native_topn_multi_segment_ranks_on_exact_union() {
    let app = boot_app("cardu_topn", overlapping_batches()).await;
    let (status, body) = post_json(
        app,
        "/druid/v2",
        json!({
            "queryType": "topN",
            "dataSource": {"type": "table", "name": "cardu_topn"},
            "intervals": [format!("{INTERVAL_START}/{INTERVAL_END}")],
            "granularity": "all",
            "dimension": "site_id",
            "metric": "uniq",
            "threshold": 10,
            "aggregations": [
                {"type": "cardinality", "name": "uniq",
                 "fields": ["device_id"], "byRow": false}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let entries = body
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|r| r.get("result"))
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("topN result rows; body = {body}"));
    assert_eq!(entries.len(), 2, "two sites; body = {body}");
    for entry in entries {
        assert_eq!(
            entry.get("uniq"),
            Some(&json!(2)),
            "both sites union to 2 distinct devices; body = {body}"
        );
    }
}

/// Single-segment NO-REGRESSION above the former 1,000-key wire cap: one
/// segment with 5,000 distinct devices must return the exact 5,000 — the
/// live-verified single-segment E16 path must not start failing closed
/// because partials now travel as set envelopes.
#[tokio::test]
async fn sql_single_segment_5000_distinct_stays_exact() {
    let rows: Vec<Value> = (0..5_000)
        .map(|i| row((i % 24) as u32, "site_a", &format!("dev{i:05}")))
        .collect();
    let app = boot_app("cardu_5k", vec![rows]).await;
    let (status, body) = post_sql(
        app,
        "SELECT COUNT(DISTINCT device_id) AS uniq FROM cardu_5k",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("uniq")),
        Some(&json!(5_000)),
        "single segment with 5,000 distinct must stay exact; body = {body}"
    );
}

/// Multi-segment exact union ABOVE the former 1,000-key wire cap: two
/// overlapping 1,500-device segments (750 shared) union to the exact
/// 2,250 — the pre-fix wire cap would have saturated both envelopes.
#[tokio::test]
async fn sql_multi_segment_union_above_former_wire_cap_stays_exact() {
    let seg1: Vec<Value> = (0..1_500)
        .map(|i| row((i % 24) as u32, "site_a", &format!("dev{i:05}")))
        .collect();
    let seg2: Vec<Value> = (750..2_250)
        .map(|i| row((i % 24) as u32, "site_a", &format!("dev{i:05}")))
        .collect();
    let app = boot_app("cardu_wide", vec![seg1, seg2]).await;
    let (status, body) = post_sql(
        app,
        "SELECT COUNT(DISTINCT device_id) AS uniq FROM cardu_wide",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("uniq")),
        Some(&json!(2_250)),
        "1,500 ∪ 1,500 with 750 overlap must union to the exact 2,250; body = {body}"
    );
}

/// GroupBy ordered BY the exact-cardinality output (single segment): the
/// executor's limitSpec ordering runs while the cardinality outputs are
/// envelope-shaped partials, so it must order on the envelope's exact
/// count (`CardinalityState::peek_json`), not on the envelope object
/// (which would sort as 0 and leave the rows in dimension order).
/// ASC ordering makes a broken sort deterministically detectable:
/// site_c(1) must come first, not site_a (the dimension-order head).
#[tokio::test]
async fn sql_groupby_order_by_exact_cardinality_sorts_on_count() {
    let rows = vec![
        row(0, "site_a", "d1"),
        row(1, "site_a", "d2"),
        row(2, "site_a", "d3"),
        row(3, "site_b", "d1"),
        row(4, "site_b", "d2"),
        row(5, "site_c", "d1"),
    ];
    let app = boot_app("cardu_order", vec![rows]).await;
    let (status, body) = post_sql(
        app,
        "SELECT site_id, COUNT(DISTINCT device_id) AS uniq FROM cardu_order \
         GROUP BY site_id ORDER BY uniq ASC LIMIT 2",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    let rows = body.as_array().expect("rows");
    assert_eq!(rows.len(), 2, "LIMIT 2; body = {body}");
    assert_eq!(
        (
            rows[0].get("site_id"),
            rows[0].get("uniq"),
            rows[1].get("site_id"),
            rows[1].get("uniq"),
        ),
        (
            Some(&json!("site_c")),
            Some(&json!(1)),
            Some(&json!("site_b")),
            Some(&json!(2)),
        ),
        "ORDER BY uniq ASC must sort on the envelope's exact count; body = {body}"
    );
}

/// Regression guard for the approximate path (`useApproximateCountDistinct`
/// default true → HLL sketch): the HLL aggregator itself is untouched by
/// the envelope wiring.  Writing this guard EXPOSED a pre-existing latent
/// bug (verified failing identically on the pre-change base): the hidden
/// `HLLSketchBuild` sketch merged correctly across segments, but the SQL
/// output comes from the `HLLSketchEstimate` POST-AGGREGATION, which
/// `merge_agg_maps` kept dst-wins — so multi-segment approx COUNT(DISTINCT)
/// silently returned the FIRST segment's estimate (3, not 4).  Fixed by
/// the broker's post-merge post-aggregation recompute (2026-07-12).
#[tokio::test]
async fn sql_approx_count_distinct_multi_segment_unaffected() {
    let app = boot_app("cardu_hll", overlapping_batches()).await;
    let (status, body) = post_sql(
        app,
        "SELECT COUNT(DISTINCT device_id) AS uniq FROM cardu_hll",
        false,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("uniq")),
        Some(&json!(4)),
        "HLL estimate is exact at 4 distinct values; body = {body}"
    );
}
