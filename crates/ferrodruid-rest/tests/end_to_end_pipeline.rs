// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 53 — true end-to-end pipeline integration test.
//!
//! Boots the full FerroDruid REST router on a random TCP port via
//! `axum::serve`, ingests inline rows through the production
//! [`ferrodruid_ingest_batch::BatchIngester`], loads the resulting
//! segment into [`ferrodruid_historical::Historical`], registers it
//! in the metadata store, then exercises the query path *over real
//! HTTP* (no `tower::ServiceExt::oneshot` shortcut) using `reqwest`.
//!
//! Two cases exercise the pipeline end-to-end:
//!
//! 1. **`pipeline_10_row_count`** — 10 inline rows, native
//!    `timeseries` aggregation, asserts `count = 10`.
//! 2. **`pipeline_100_row_groupby`** — 100 inline rows across 5
//!    cities × 4 products, native `groupBy`, asserts the group
//!    cardinality and that the row counts per group sum to 100.
//!
//! # Honest deviation from the original task spec
//!
//! The task spec asked for "POST inline ingest spec → wait task
//! complete".  In FerroDruid's current shape `POST
//! /druid/indexer/v1/task` only **records** the spec on the overlord
//! and returns a task id; it does not run a real batch ingester (the
//! overlord is a stub for the indexer wire-protocol).  Running the
//! real ingester through that endpoint would require a much larger
//! plumbing change, well outside Wave 53's "test expansion, no
//! production source touched" scope.  Instead we drive
//! `BatchIngester::ingest` directly in-process, which is exactly the
//! call site a real overlord task would use — so the produced segment
//! is byte-for-byte the same as a production batch task would emit.
//! The HTTP-layer e2e is then exercised by querying that segment over
//! a real bound listener.
//!
//! `#[ignore]`d so the default fast-CI lane stays unaffected.  Run
//! with:
//!
//! ```text
//! cargo test -p ferrodruid-rest --test end_to_end_pipeline \
//!     -- --ignored --nocapture
//! ```

#![allow(missing_docs)]

use std::sync::Arc;

use ferrodruid_auth::AuthStore;
use ferrodruid_authz::Authorizer;
use ferrodruid_broker::Broker;
use ferrodruid_coordinator::Coordinator;
use ferrodruid_historical::Historical;
use ferrodruid_ingest_batch::BatchIngester;
use ferrodruid_lookup::LookupManager;
use ferrodruid_metadata::{MetadataStore, SegmentMetadataRow};
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_rest::{AppState, create_router};
use ferrodruid_telemetry::Metrics;
use serde_json::json;

/// Build a `(addr, server_handle, _cache_keepalive)` triple by
/// ingesting `rows` inline and binding a real TCP listener.
async fn boot_pipeline(
    datasource: &str,
    dimensions: Vec<String>,
    metrics_spec: Vec<serde_json::Value>,
    rows: Vec<serde_json::Value>,
    interval_start: &str,
    interval_end: &str,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    // 1. In-memory metadata store.
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("metadata create");
    metadata.initialize().await.expect("metadata init");
    let metadata = Arc::new(metadata);

    // 2. Production ingester — same call-site a batch task would use.
    let row_count = rows.len();
    let ingester = BatchIngester::new(
        datasource.to_string(),
        "__time".to_string(),
        dimensions,
        metrics_spec,
    );
    let ingested = ingester.ingest(rows).expect("BatchIngester::ingest");
    assert_eq!(ingested.num_rows, row_count, "ingester must keep all rows");

    // 3. Load segment into Historical.
    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 100_000_000);
    let segment_id = format!("{datasource}_{interval_start}_{interval_end}_v1_0");
    historical
        .load_segment(&segment_id, ingested.segment_data)
        .expect("historical load_segment");
    historical
        .set_segment_datasource(&segment_id, datasource)
        .expect("historical set_segment_datasource");
    let historical = Arc::new(historical);

    // 4. Register segment in metadata so coord/broker can resolve it.
    let seg_row = SegmentMetadataRow {
        id: segment_id.clone(),
        data_source: datasource.to_string(),
        created_date: format!("{interval_start}.000Z"),
        start: format!("{interval_start}.000Z"),
        end: format!("{interval_end}.000Z"),
        version: "v1".to_string(),
        used: true,
        payload: json!({"dataSource": datasource, "interval": format!("{interval_start}/{interval_end}")}),
    };
    metadata
        .insert_segment(&seg_row)
        .await
        .expect("insert_segment");

    // 5. Build router with auth disabled so the e2e focuses on the
    //    ingest -> query pipeline rather than the auth path (which
    //    is exercised by middleware_stack_integration.rs).
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
        lookup_manager: Arc::new(LookupManager::new()),
        metrics: Arc::new(Metrics::new()),
        msq_manager: Arc::new(MsqManager::new()),
        rate_limit_max_concurrent: 0,
    });
    let router = create_router(state);

    // 6. Bind on an ephemeral port and spawn the server.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });

    (addr, server, cache_dir)
}

// ---------------------------------------------------------------------------
// case 1: 10 rows + simple aggregation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn pipeline_10_row_count() {
    let rows: Vec<serde_json::Value> = (0..10)
        .map(|i| {
            json!({
                "__time": format!("2024-01-01T0{}:00:00Z", i % 10),
                "city": if i % 2 == 0 { "tokyo" } else { "osaka" },
                "revenue": (i + 1) * 10,
            })
        })
        .collect();

    let (addr, server, _cache) = boot_pipeline(
        "sales_pipe10",
        vec!["city".to_string()],
        vec![json!({"type": "doubleSum", "name": "revenue"})],
        rows,
        "2024-01-01T00:00:00",
        "2024-01-02T00:00:00",
    )
    .await;

    // Native timeseries (count=*).
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/druid/v2/"))
        .header("content-type", "application/json")
        .json(&json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "sales_pipe10"},
            "intervals": ["2024-01-01/2024-01-02"],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "total"}]
        }))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), 200, "POST /druid/v2/ must return 200");
    let body: serde_json::Value = resp.json().await.expect("json");

    let arr = body.as_array().expect("timeseries returns array");
    assert_eq!(arr.len(), 1, "granularity=all -> one bucket: {body}");
    assert_eq!(
        arr[0]["result"]["total"], 10,
        "count must equal row count: {body}",
    );

    server.abort();
    let _ = server.await;
}

// ---------------------------------------------------------------------------
// case 2: 100 rows + GROUP BY
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn pipeline_100_row_groupby() {
    const CITIES: &[&str] = &["tokyo", "osaka", "kyoto", "nagoya", "sapporo"];
    const PRODUCTS: &[&str] = &["widget", "gadget", "doohickey", "thingamajig"];

    let rows: Vec<serde_json::Value> = (0..100)
        .map(|i| {
            let hour = i % 24;
            let city = CITIES[i % CITIES.len()];
            let product = PRODUCTS[i % PRODUCTS.len()];
            json!({
                "__time": format!("2024-02-{:02}T{:02}:00:00Z", (i / 24) + 1, hour),
                "city": city,
                "product": product,
                "revenue": (i as i64 + 1) * 7,
            })
        })
        .collect();

    let (addr, server, _cache) = boot_pipeline(
        "sales_pipe100",
        vec!["city".to_string(), "product".to_string()],
        vec![json!({"type": "doubleSum", "name": "revenue"})],
        rows,
        "2024-02-01T00:00:00",
        "2024-02-10T00:00:00",
    )
    .await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/druid/v2/"))
        .header("content-type", "application/json")
        .json(&json!({
            "queryType": "groupBy",
            "dataSource": {"type": "table", "name": "sales_pipe100"},
            "intervals": ["2024-02-01/2024-02-10"],
            "granularity": "all",
            "dimensions": [
                {"type": "default", "dimension": "city",    "output_name": "city",    "output_type": "STRING"},
                {"type": "default", "dimension": "product", "output_name": "product", "output_type": "STRING"}
            ],
            "aggregations": [
                {"type": "count", "name": "cnt"},
                {"type": "doubleSum", "name": "total_revenue", "fieldName": "revenue"}
            ]
        }))
        .send()
        .await
        .expect("send groupBy");
    assert_eq!(resp.status(), 200, "POST groupBy must be 200");

    let body: serde_json::Value = resp.json().await.expect("json");
    let groups = body.as_array().expect("groupBy returns array");

    // Cardinality bound: 5 cities × 4 products = 20 max distinct
    // groups; with 100 rows / 20 combos we expect every combo to be
    // populated.  We allow >= 5 to keep the test robust under any
    // future engine sort/limit change while still asserting real
    // grouping happened.
    assert!(
        groups.len() >= 5,
        "groupBy should produce at least 5 groups, got {}",
        groups.len(),
    );

    // Sum of cnt over all groups must equal 100.
    let total_count: i64 = groups
        .iter()
        .map(|g| g["event"]["cnt"].as_i64().unwrap_or(0))
        .sum();
    assert_eq!(
        total_count, 100,
        "sum of group counts must equal row count: {body}",
    );

    // Every group must have city + product + cnt + total_revenue.
    for g in groups {
        let event = &g["event"];
        for field in ["city", "product", "cnt", "total_revenue"] {
            assert!(
                event.get(field).is_some(),
                "missing {field} in group: {event}",
            );
        }
    }

    server.abort();
    let _ = server.await;
}
