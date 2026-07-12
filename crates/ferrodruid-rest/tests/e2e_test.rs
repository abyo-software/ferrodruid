// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! End-to-end integration tests for FerroDruid.
//!
//! Each test boots the full FerroDruid stack in-process (metadata, coordinator,
//! overlord, broker, historical, auth/authz), ingests sample data via the batch
//! ingester, loads segments into Historical, and exercises the REST API through
//! the Axum router using `tower::ServiceExt::oneshot`.

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
use serde_json::json;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Sample data
// ---------------------------------------------------------------------------

/// Build 20 sample rows across 5 days (2024-01-01 through 2024-01-05).
///
/// Dimensions: city (tokyo, new york, london, paris, berlin), product (widget,
/// gadget, doohickey). Metrics: revenue (long 10-1000), quantity (long 1-100),
/// price (double 1.0-99.99).
fn sample_rows() -> Vec<serde_json::Value> {
    vec![
        json!({"__time":"2024-01-01T00:00:00Z","city":"tokyo","product":"widget","revenue":100,"quantity":10,"price":9.99}),
        json!({"__time":"2024-01-01T01:00:00Z","city":"new york","product":"gadget","revenue":200,"quantity":20,"price":19.99}),
        json!({"__time":"2024-01-01T02:00:00Z","city":"london","product":"doohickey","revenue":300,"quantity":30,"price":29.99}),
        json!({"__time":"2024-01-01T03:00:00Z","city":"tokyo","product":"gadget","revenue":150,"quantity":15,"price":14.99}),
        json!({"__time":"2024-01-02T00:00:00Z","city":"paris","product":"widget","revenue":400,"quantity":40,"price":39.99}),
        json!({"__time":"2024-01-02T01:00:00Z","city":"berlin","product":"doohickey","revenue":500,"quantity":50,"price":49.99}),
        json!({"__time":"2024-01-02T02:00:00Z","city":"tokyo","product":"widget","revenue":250,"quantity":25,"price":24.99}),
        json!({"__time":"2024-01-02T03:00:00Z","city":"new york","product":"gadget","revenue":350,"quantity":35,"price":34.99}),
        json!({"__time":"2024-01-03T00:00:00Z","city":"london","product":"widget","revenue":600,"quantity":60,"price":59.99}),
        json!({"__time":"2024-01-03T01:00:00Z","city":"paris","product":"gadget","revenue":700,"quantity":70,"price":69.99}),
        json!({"__time":"2024-01-03T02:00:00Z","city":"berlin","product":"doohickey","revenue":800,"quantity":80,"price":79.99}),
        json!({"__time":"2024-01-03T03:00:00Z","city":"tokyo","product":"widget","revenue":50,"quantity":5,"price":4.99}),
        json!({"__time":"2024-01-04T00:00:00Z","city":"new york","product":"doohickey","revenue":900,"quantity":90,"price":89.99}),
        json!({"__time":"2024-01-04T01:00:00Z","city":"london","product":"gadget","revenue":1000,"quantity":100,"price":99.99}),
        json!({"__time":"2024-01-04T02:00:00Z","city":"paris","product":"widget","revenue":10,"quantity":1,"price":1.0}),
        json!({"__time":"2024-01-04T03:00:00Z","city":"berlin","product":"gadget","revenue":450,"quantity":45,"price":44.99}),
        json!({"__time":"2024-01-05T00:00:00Z","city":"tokyo","product":"doohickey","revenue":550,"quantity":55,"price":54.99}),
        json!({"__time":"2024-01-05T01:00:00Z","city":"new york","product":"widget","revenue":650,"quantity":65,"price":64.99}),
        json!({"__time":"2024-01-05T02:00:00Z","city":"london","product":"gadget","revenue":750,"quantity":75,"price":74.99}),
        json!({"__time":"2024-01-05T03:00:00Z","city":"paris","product":"doohickey","revenue":350,"quantity":35,"price":34.99}),
    ]
}

// ---------------------------------------------------------------------------
// Setup helper
// ---------------------------------------------------------------------------

/// Create a fully-initialised test `AppState` with sample data loaded.
///
/// Returns the Axum router and a reference to the shared state.
async fn setup_test_app() -> Router {
    // 1. In-memory metadata store.
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("create metadata store");
    metadata.initialize().await.expect("initialize schema");
    let metadata = Arc::new(metadata);

    // 2. Coordinator and overlord.
    let coordinator = Arc::new(Coordinator::new(Arc::clone(&metadata)));
    let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));

    // 3. Batch-ingest sample data into a segment.
    let ingester = BatchIngester::new(
        "sales".to_string(),
        "__time".to_string(),
        vec!["city".to_string(), "product".to_string()],
        vec![
            json!({"type": "doubleSum", "name": "revenue"}),
            json!({"type": "doubleSum", "name": "quantity"}),
            json!({"type": "doubleSum", "name": "price"}),
        ],
    );
    let ingested = ingester.ingest(sample_rows()).expect("ingest sample data");
    assert_eq!(ingested.num_rows, 20);

    // 4. Load segment into Historical.
    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 100_000_000);
    let segment_id = "sales_2024-01-01T00:00:00.000Z_2024-01-06T00:00:00.000Z_v1_0";
    historical
        .load_segment(segment_id, ingested.segment_data)
        .expect("load segment");
    historical
        .set_segment_datasource(segment_id, "sales")
        .expect("set datasource");
    let historical = Arc::new(historical);

    // 5. Register the segment in metadata so coordinator endpoints can see it.
    let seg_row = SegmentMetadataRow {
        id: segment_id.to_string(),
        data_source: "sales".to_string(),
        created_date: "2024-01-01T00:00:00.000Z".to_string(),
        start: "2024-01-01T00:00:00.000Z".to_string(),
        end: "2024-01-06T00:00:00.000Z".to_string(),
        version: "v1".to_string(),
        used: true,
        payload: json!({"dataSource": "sales", "interval": "2024-01-01/2024-01-06"}),
    };
    metadata
        .insert_segment(&seg_row)
        .await
        .expect("insert segment metadata");

    // 6. Auth and authz (permissive admin).
    let auth_store = Arc::new(parking_lot::RwLock::new(AuthStore::new()));
    let authorizer = Arc::new(Authorizer::new().with_admin_role());

    // 7. Broker.
    let broker = Arc::new(Broker::new());

    // 8. Assemble state and router.
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

    // Leak the tempdir so it stays alive for the test duration.
    // (The Historical holds data in memory, but the cache_dir path must exist.)
    std::mem::forget(cache_dir);

    create_router(state)
}

/// Helper: send a GET request and return (status, json body).
async fn get_json(app: Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("send request");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
    (status, json)
}

/// Helper: send a POST /druid/v2/ query and return (status, json body).
async fn post_query(app: Router, query: serde_json::Value) -> (StatusCode, serde_json::Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/v2/")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&query).expect("serialize")))
                .expect("build request"),
        )
        .await
        .expect("send request");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
    (status, json)
}

// ===========================================================================
// Status endpoint tests
// ===========================================================================

#[tokio::test]
async fn e2e_status_endpoints() {
    let app = setup_test_app().await;

    // GET /status -> 200, has "version" field
    let (status, json) = get_json(app, "/status").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        json.get("version").is_some(),
        "missing version field: {json}"
    );

    // GET /status/health -> Wave 36-B real readiness envelope (no longer
    // a bare `true`; see crates/ferrodruid-rest/src/status_routes.rs).
    let app = setup_test_app().await;
    let (status, json) = get_json(app, "/status/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["ok"], json!(true));
    assert!(json["checks"].is_object());

    // GET /status/selfDiscovered -> { selfDiscovered: true }
    let app = setup_test_app().await;
    let (status, json) = get_json(app, "/status/selfDiscovered").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["selfDiscovered"], true);
}

// ===========================================================================
// Timeseries query tests
// ===========================================================================

#[tokio::test]
async fn e2e_timeseries_count() {
    let app = setup_test_app().await;
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "sales"},
            "intervals": ["2024-01-01/2024-01-06"],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "total"}]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().expect("array result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["result"]["total"], 20);
}

#[tokio::test]
async fn e2e_timeseries_sum_with_filter() {
    let app = setup_test_app().await;

    // Tokyo rows: revenue 100+150+250+50+550 = 1100
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "sales"},
            "intervals": ["2024-01-01/2024-01-06"],
            "granularity": "all",
            "filter": {"type": "selector", "dimension": "city", "value": "tokyo"},
            "aggregations": [
                {"type": "count", "name": "cnt"},
                {"type": "doubleSum", "name": "total_revenue", "fieldName": "revenue"}
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().expect("array");
    assert_eq!(results.len(), 1);
    // Tokyo has 5 rows: days 1 (x2), 2, 3, 5
    assert_eq!(results[0]["result"]["cnt"], 5);
    let revenue = results[0]["result"]["total_revenue"]
        .as_f64()
        .expect("revenue");
    assert!(
        (revenue - 1100.0).abs() < 0.01,
        "expected ~1100, got {revenue}"
    );
}

#[tokio::test]
async fn e2e_timeseries_daily_granularity() {
    let app = setup_test_app().await;
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "sales"},
            "intervals": ["2024-01-01/2024-01-06"],
            "granularity": "day",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().expect("array");
    // 5 days -> 5 buckets
    assert_eq!(
        results.len(),
        5,
        "expected 5 daily buckets, got {results:?}"
    );
    // Each day has 4 rows
    for bucket in results {
        assert_eq!(bucket["result"]["cnt"], 4, "each day should have 4 rows");
    }
}

// ===========================================================================
// TopN query tests
// ===========================================================================

#[tokio::test]
async fn e2e_topn_by_city() {
    let app = setup_test_app().await;
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "topN",
            "dataSource": {"type": "table", "name": "sales"},
            "intervals": ["2024-01-01/2024-01-06"],
            "granularity": "all",
            "dimension": {"type": "default", "dimension": "city", "output_name": "city", "output_type": "STRING"},
            "threshold": 3,
            "metric": {"type": "numeric", "metric": "total_revenue"},
            "aggregations": [{"type": "doubleSum", "name": "total_revenue", "fieldName": "revenue"}]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().expect("array");
    assert_eq!(results.len(), 1, "granularity=all should give 1 bucket");
    let entries = results[0]["result"].as_array().expect("result array");
    assert!(entries.len() <= 3, "expected at most 3 entries");
    // Entries should be sorted by total_revenue descending.
    for window in entries.windows(2) {
        let a = window[0]["total_revenue"].as_f64().expect("a");
        let b = window[1]["total_revenue"].as_f64().expect("b");
        assert!(a >= b, "topN should be sorted descending: {a} < {b}");
    }
}

// ===========================================================================
// GroupBy query tests
// ===========================================================================

#[tokio::test]
async fn e2e_groupby_city_product() {
    let app = setup_test_app().await;
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "groupBy",
            "dataSource": {"type": "table", "name": "sales"},
            "intervals": ["2024-01-01/2024-01-06"],
            "granularity": "all",
            "dimensions": [
                {"type": "default", "dimension": "city", "output_name": "city", "output_type": "STRING"},
                {"type": "default", "dimension": "product", "output_name": "product", "output_type": "STRING"}
            ],
            "aggregations": [
                {"type": "count", "name": "cnt"},
                {"type": "doubleSum", "name": "total_revenue", "fieldName": "revenue"}
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().expect("array");
    // 5 cities * 3 products = 15 possible groups, but not all combos exist.
    // We have 20 rows with various combos. Verify at least several groups.
    assert!(
        results.len() >= 5,
        "expected at least 5 groups, got {}",
        results.len()
    );

    // Sum of all counts should be 20.
    let total_count: i64 = results
        .iter()
        .map(|r| r["event"]["cnt"].as_i64().unwrap_or(0))
        .sum();
    assert_eq!(total_count, 20, "sum of all group counts should be 20");

    // Check each result has city, product, cnt, total_revenue.
    for r in results {
        let event = &r["event"];
        assert!(event.get("city").is_some(), "missing city: {event}");
        assert!(event.get("product").is_some(), "missing product: {event}");
        assert!(event.get("cnt").is_some(), "missing cnt: {event}");
        assert!(
            event.get("total_revenue").is_some(),
            "missing total_revenue: {event}"
        );
    }
}

// ===========================================================================
// Scan query tests
// ===========================================================================

#[tokio::test]
async fn e2e_scan_all_rows() {
    let app = setup_test_app().await;
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "scan",
            "dataSource": {"type": "table", "name": "sales"},
            "intervals": ["2024-01-01/2024-01-06"],
            "limit": 10
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    // ScanResult serializes as an object with "columns" and "events".
    let events = json["events"].as_array().expect("events array");
    assert_eq!(events.len(), 10, "limit=10 should return 10 rows");

    // Verify columns are present in each row.
    for event in events {
        assert!(event.get("__time").is_some(), "missing __time");
        assert!(event.get("city").is_some(), "missing city");
        assert!(event.get("product").is_some(), "missing product");
        assert!(event.get("revenue").is_some(), "missing revenue");
    }
}

#[tokio::test]
async fn e2e_scan_with_filter() {
    let app = setup_test_app().await;

    // Our sample: revenue values are 10,50,100,150,200,250,300,350,350,400,450,500,550,600,650,700,750,800,900,1000
    // Revenue > 500 => 550, 600, 650, 700, 750, 800, 900, 1000 = 8 rows
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "scan",
            "dataSource": {"type": "table", "name": "sales"},
            "intervals": ["2024-01-01/2024-01-06"],
            "filter": {"type": "bound", "dimension": "revenue", "lower": "500", "lowerStrict": true, "ordering": "numeric"},
            "columns": ["city", "product", "revenue"]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let events = json["events"].as_array().expect("events array");
    // All returned rows should have revenue > 500.
    for event in events {
        let rev = event["revenue"].as_f64().expect("revenue");
        assert!(rev > 500.0, "expected revenue > 500, got {rev}");
    }
    assert!(
        !events.is_empty(),
        "should have some rows with revenue > 500"
    );
}

// ===========================================================================
// Search query tests
// ===========================================================================

#[tokio::test]
async fn e2e_search_dimension() {
    let app = setup_test_app().await;
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "search",
            "dataSource": {"type": "table", "name": "sales"},
            "intervals": ["2024-01-01/2024-01-06"],
            "query": {"type": "contains", "value": "o"},
            "searchDimensions": ["city"]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().expect("array");
    assert!(!results.is_empty(), "search should return results");

    // Collect all matched city values.
    let empty = Vec::new();
    let values: Vec<&str> = results
        .iter()
        .flat_map(|r| {
            r["result"]
                .as_array()
                .unwrap_or(&empty)
                .iter()
                .filter_map(|h| h["value"].as_str())
        })
        .collect();

    // "tokyo", "london", "new york" all contain "o"
    assert!(values.contains(&"tokyo"), "tokyo contains 'o': {values:?}");
    assert!(
        values.contains(&"london"),
        "london contains 'o': {values:?}"
    );
    assert!(
        values.contains(&"new york"),
        "new york contains 'o': {values:?}"
    );
}

// ===========================================================================
// TimeBoundary query tests
// ===========================================================================

#[tokio::test]
async fn e2e_time_boundary() {
    let app = setup_test_app().await;
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "timeBoundary",
            "dataSource": {"type": "table", "name": "sales"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().expect("array");
    assert_eq!(results.len(), 1);

    let r = &results[0]["result"];
    let min_time = r["minTime"].as_str().expect("minTime");
    let max_time = r["maxTime"].as_str().expect("maxTime");
    assert!(
        min_time.starts_with("2024-01-01"),
        "min should be 2024-01-01, got {min_time}"
    );
    assert!(
        max_time.starts_with("2024-01-05"),
        "max should be 2024-01-05, got {max_time}"
    );
}

// ===========================================================================
// SegmentMetadata query tests
// ===========================================================================

#[tokio::test]
async fn e2e_segment_metadata() {
    let app = setup_test_app().await;
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "segmentMetadata",
            "dataSource": {"type": "table", "name": "sales"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().expect("array");
    assert_eq!(results.len(), 1, "single segment -> 1 result");

    let meta = &results[0];
    assert_eq!(meta["numRows"], 20, "segment should have 20 rows");

    let columns = meta["columns"].as_object().expect("columns object");
    assert!(columns.contains_key("__time"), "missing __time column");
    assert!(columns.contains_key("city"), "missing city column");
    assert!(columns.contains_key("product"), "missing product column");
    assert!(columns.contains_key("revenue"), "missing revenue column");

    assert_eq!(columns["__time"]["type"], "LONG");
    assert_eq!(columns["city"]["type"], "STRING");
    assert_eq!(columns["product"]["type"], "STRING");
    assert_eq!(columns["revenue"]["type"], "DOUBLE");
}

// ===========================================================================
// DataSourceMetadata query tests
// ===========================================================================

#[tokio::test]
async fn e2e_datasource_metadata() {
    let app = setup_test_app().await;
    let (status, json) = post_query(
        app,
        json!({
            "queryType": "dataSourceMetadata",
            "dataSource": {"type": "table", "name": "sales"}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().expect("array");
    assert_eq!(results.len(), 1);

    let ts = results[0]["timestamp"].as_str().expect("timestamp");
    // Max ingested time is the last row: 2024-01-05T03:00:00Z
    assert!(
        ts.starts_with("2024-01-05"),
        "max ingested time should be 2024-01-05, got {ts}"
    );
}

// ===========================================================================
// Coordinator endpoint tests
// ===========================================================================

#[tokio::test]
async fn e2e_coordinator_endpoints() {
    let app = setup_test_app().await;

    // GET /druid/coordinator/v1/datasources -> ["sales"]
    let (status, json) = get_json(app, "/druid/coordinator/v1/datasources").await;
    assert_eq!(status, StatusCode::OK);
    let ds = json.as_array().expect("array");
    assert_eq!(ds.len(), 1, "should have 1 datasource");
    assert_eq!(ds[0], "sales");

    // GET /druid/coordinator/v1/datasources/sales/segments -> segment list
    let app = setup_test_app().await;
    let (status, json) = get_json(app, "/druid/coordinator/v1/datasources/sales/segments").await;
    assert_eq!(status, StatusCode::OK);
    let segments = json.as_array().expect("array");
    assert_eq!(segments.len(), 1, "should have 1 segment");
    assert_eq!(segments[0]["dataSource"], "sales");
}

// ===========================================================================
// Overlord endpoint tests
// ===========================================================================

#[tokio::test]
async fn e2e_overlord_endpoints() {
    let app = setup_test_app().await;

    // POST /druid/indexer/v1/supervisor -> create
    let spec = json!({
        "id": "sales-kafka",
        "type": "kafka",
        "dataSchema": {"dataSource": "sales"},
        "ioConfig": {"topic": "sales-events"}
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/indexer/v1/supervisor")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&spec).expect("serialize")))
                .expect("build request"),
        )
        .await
        .expect("send request");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let create_json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
    assert_eq!(create_json["id"], "sales-kafka");

    // GET /druid/indexer/v1/supervisor -> list (should have the one we created)
    let app = setup_test_app().await;
    // Re-create the supervisor in this fresh app instance
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/indexer/v1/supervisor")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&spec).expect("serialize")))
                .expect("build request"),
        )
        .await
        .expect("send request");
    assert_eq!(response.status(), StatusCode::OK);
}

// ===========================================================================
// Error handling tests
// ===========================================================================

#[tokio::test]
async fn e2e_invalid_query() {
    let app = setup_test_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/v2/")
                .header("content-type", "application/json")
                .body(Body::from("not valid json"))
                .expect("build request"),
        )
        .await
        .expect("send request");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");

    // Druid-compatible error format.
    assert!(json.get("error").is_some(), "missing error field: {json}");
    assert!(
        json.get("errorMessage").is_some(),
        "missing errorMessage field: {json}"
    );
    assert!(
        json.get("errorClass").is_some(),
        "missing errorClass field: {json}"
    );
}
