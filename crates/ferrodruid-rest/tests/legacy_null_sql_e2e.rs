// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-B legacy null mode (v1.5.0) — REST-level (`/druid/v2/sql` +
//! `/druid/v2`) oracle diff-battery.
//!
//! **This test binary latches the process-global legacy-null mode ON**
//! (`useDefaultValueForNull=true`) in every test — the latch is set-once
//! per process, which is why these tests live in their own integration
//! binary and no ANSI-mode test may ever be added here.
//!
//! Every expected value below is a MEASURED Apache Druid legacy answer —
//! Druid 27.0.0 (legacy default) captured 2026-07-20/21 into
//! `tests/segment-compat/fixtures/legacy_null_druid27/oracle/legacy27/`
//! and `oracle/legacy27_ext/` (the same fixture's ANSI answers from Druid
//! 31.0.2 live next to them and diverge on every cell marked `ANSI:`).
//! Nothing here is reasoned from memory.
//!
//! The 6-row `legacy_null_compat` fixture (dims `tag`,`strcol` string,
//! `x`,`y` long):
//!
//! | row | tag | strcol | x      | y      |
//! |-----|-----|--------|--------|--------|
//! | 0   | r0  | "a"    | absent | 10     |
//! | 1   | r1  | "b"    | absent | absent |
//! | 2   | r2  | ""     | absent | 20     |
//! | 3   | r3  | absent | absent | absent |
//! | 4   | r4  | ""     | absent | absent |
//! | 5   | r5  | "c"    | absent | 30     |
//!
//! The battery runs against BOTH storage generations:
//!
//! * `legacy_null_compat` — ingested through the REAL `BatchIngester`
//!   under legacy mode, so nulls are COERCED at ingest (`""`/`0`, no null
//!   markers) exactly like a legacy Druid writes; and
//! * `legacy_null_ansistore` — the same rows loaded as an ANSI-STORED
//!   segment (explicit null markers: string null-row bitmap, nullable-long
//!   bitmaps), the shape a segment migrated from an ANSI cluster has.
//!
//! Druid answers queries over both generations identically under
//! `useDefaultValueForNull=true` (nulls read as defaults), so every SQL
//! cell asserts the SAME legacy answer for both datasources.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ferrodruid_auth::AuthStore;
use ferrodruid_authz::Authorizer;
use ferrodruid_broker::Broker;
use ferrodruid_coordinator::Coordinator;
use ferrodruid_historical::Historical;
use ferrodruid_ingest_batch::{BatchIngester, parse_dimension_entries};
use ferrodruid_metadata::MetadataStore;
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_rest::{AppState, create_router};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::SegmentDataBuilder;
use serde_json::{Value, json};
use tower::ServiceExt;

/// Latch legacy mode ON for this whole test process (set-once; first call
/// wins, and every test in this binary wants `true`).
fn latch_legacy() {
    assert!(
        ferrodruid_common::null_mode::init_legacy_null_mode(true),
        "this test binary requires the legacy-null latch; something latched ANSI first"
    );
}

const INTERVAL_START: &str = "2024-01-01T00:00:00.000Z";
const INTERVAL_END: &str = "2024-01-02T00:00:00.000Z";

fn base_millis() -> i64 {
    chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("base ts")
        .timestamp_millis()
}

/// The 6 fixture rows exactly as `ingest_spec.json` inlines them (absent
/// fields stay absent).
fn fixture_rows() -> Vec<Value> {
    let base = base_millis();
    let t = |i: i64| base + i * 3_600_000;
    vec![
        json!({"__time": t(0), "tag": "r0", "strcol": "a", "y": 10}),
        json!({"__time": t(1), "tag": "r1", "strcol": "b"}),
        json!({"__time": t(2), "tag": "r2", "strcol": "", "y": 20}),
        json!({"__time": t(3), "tag": "r3"}),
        json!({"__time": t(4), "tag": "r4", "strcol": ""}),
        json!({"__time": t(5), "tag": "r5", "strcol": "c", "y": 30}),
    ]
}

/// The 4-row `legacy_null_ext` fixture rows (double `d`, float `f`).
fn ext_rows() -> Vec<Value> {
    let base = base_millis();
    let t = |i: i64| base + i * 3_600_000;
    vec![
        json!({"__time": t(0), "tag": "e0", "d": 1.5, "f": 2.5}),
        json!({"__time": t(1), "tag": "e1"}),
        json!({"__time": t(2), "tag": "e2", "d": 3.5}),
        json!({"__time": t(3), "tag": "e3", "f": 4.5}),
    ]
}

/// Ingest `rows` through the REAL batch ingester with the fixture's typed
/// dimensions (mirrors `ingest_spec.json` / `ingest_spec_ext.json`).
fn ingest(datasource: &str, dims: &Value, rows: Vec<Value>) -> SegmentData {
    let dim_schemas =
        parse_dimension_entries(dims.as_array().expect("dims array")).expect("dim schemas");
    let ingester = BatchIngester::with_schemas(
        datasource.to_string(),
        "__time".to_string(),
        dim_schemas,
        Vec::new(),
    );
    ingester.ingest(rows).expect("ingest fixture").segment_data
}

/// The same 6 rows as an ANSI-STORED segment: explicit null markers
/// (string null-row bitmap; nullable-long null bitmaps) — the storage a
/// segment migrated from an ANSI-mode cluster carries.
fn build_ansi_stored_segment() -> SegmentData {
    let base = base_millis();
    let s = |v: &str| Some(v.to_string());
    SegmentDataBuilder::new()
        .add_timestamp_column((0..6).map(|i| base + i * 3_600_000).collect())
        .add_string_column(
            "tag",
            ["r0", "r1", "r2", "r3", "r4", "r5"]
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
        .add_string_column_nullable("strcol", vec![s("a"), s("b"), s(""), None, s(""), s("c")])
        .add_long_column_nullable("x", false, vec![None, None, None, None, None, None])
        .add_long_column_nullable(
            "y",
            false,
            vec![Some(10), None, Some(20), None, None, Some(30)],
        )
        .build()
        .expect("ansi-stored segment builds")
}

/// Boot an in-process app carrying all three fixture datasources.
async fn boot_app() -> Router {
    latch_legacy();
    let metadata = MetadataStore::new_in_memory()
        .await
        .expect("create metadata store");
    metadata.initialize().await.expect("initialize schema");
    let metadata = Arc::new(metadata);

    let cache_dir = tempfile::tempdir().expect("tempdir");
    let historical = Historical::new(cache_dir.path().to_path_buf(), 1_000_000_000);

    let compat_dims = json!([
        {"type": "string", "name": "tag"},
        {"type": "string", "name": "strcol"},
        {"type": "long", "name": "x"},
        {"type": "long", "name": "y"}
    ]);
    let ext_dims = json!([
        {"type": "string", "name": "tag"},
        {"type": "double", "name": "d"},
        {"type": "float", "name": "f"}
    ]);
    let loads: Vec<(&str, SegmentData)> = vec![
        (
            "legacy_null_compat",
            ingest("legacy_null_compat", &compat_dims, fixture_rows()),
        ),
        ("legacy_null_ansistore", build_ansi_stored_segment()),
        (
            "legacy_null_ext",
            ingest("legacy_null_ext", &ext_dims, ext_rows()),
        ),
    ];
    for (ds, segment) in loads {
        let segment_id = format!("{ds}_{INTERVAL_START}_{INTERVAL_END}_v1_0");
        historical
            .load_segment(&segment_id, segment)
            .expect("load segment");
        historical
            .set_segment_datasource(&segment_id, ds)
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
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("non-JSON body ({e}): {:?}", String::from_utf8_lossy(&bytes)));
    (status, json)
}

async fn sql(app: Router, query: &str) -> Value {
    let (status, body) = post_json(app, "/druid/v2/sql", json!({"query": query})).await;
    assert_eq!(status, StatusCode::OK, "SQL failed for [{query}]: {body}");
    body
}

async fn sql_ctx(app: Router, query: &str, context: Value) -> Value {
    let (status, body) = post_json(
        app,
        "/druid/v2/sql",
        json!({"query": query, "context": context}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "SQL failed for [{query}]: {body}");
    body
}

/// Compare an SQL result against the oracle answer with NUMERIC tolerance
/// on number cells (Druid 27 renders `10` where 30/31 render `10.0`; the
/// VALUE is the oracle, the integer-vs-float rendering is engine-version
/// noise) and EXACT match on strings / nulls (the `""`-vs-null rendering
/// IS the semantics under test).
fn assert_rows_match(actual: &Value, expected: &Value, label: &str) {
    let (a, e) = (
        actual
            .as_array()
            .unwrap_or_else(|| panic!("{label}: non-array actual: {actual}")),
        expected.as_array().expect("expected is array"),
    );
    assert_eq!(
        a.len(),
        e.len(),
        "{label}: row count\nactual: {actual}\nexpected: {expected}"
    );
    for (i, (ar, er)) in a.iter().zip(e.iter()).enumerate() {
        let (ao, eo) = (
            ar.as_object()
                .unwrap_or_else(|| panic!("{label}: row {i} not object")),
            er.as_object().expect("expected row object"),
        );
        assert_eq!(
            ao.len(),
            eo.len(),
            "{label}: row {i} field count\nactual: {ar}\nexpected: {er}"
        );
        for (k, ev) in eo {
            let av = ao
                .get(k)
                .unwrap_or_else(|| panic!("{label}: row {i} missing field {k}; actual: {ar}"));
            match (av.as_f64(), ev.as_f64()) {
                (Some(af), Some(ef)) => {
                    assert!(
                        (af - ef).abs() < 1e-9,
                        "{label}: row {i} field {k}: actual {av} != expected {ev}"
                    );
                }
                _ => assert_eq!(
                    av, ev,
                    "{label}: row {i} field {k}\nactual: {ar}\nexpected: {er}"
                ),
            }
        }
    }
}

/// Run one SQL cell against BOTH storage generations of the 6-row fixture
/// (`{DS}` substitutes the datasource name) and assert the same oracle
/// answer.
async fn assert_sql_both_stores(app: &Router, query_tpl: &str, expected: Value, label: &str) {
    for ds in ["legacy_null_compat", "legacy_null_ansistore"] {
        let q = query_tpl.replace("{DS}", ds);
        let body = sql(app.clone(), &q).await;
        assert_rows_match(&body, &expected, &format!("{label} [{ds}]"));
    }
}

// ===========================================================================
// SQL battery — base fixture (oracle/legacy27 + oracle/legacy27_ext)
// ===========================================================================

#[tokio::test]
async fn sql_aggregates_over_null_bearing_longs() {
    let app = boot_app().await;
    // oracle legacy27/sum_x.json — ANSI: null
    assert_sql_both_stores(
        &app,
        "SELECT SUM(x) AS s FROM \"{DS}\"",
        json!([{"s": 0}]),
        "SUM(x) all-null col = 0",
    )
    .await;
    // oracle legacy27/sum_y.json — control (no divergence)
    assert_sql_both_stores(
        &app,
        "SELECT SUM(y) AS s FROM \"{DS}\"",
        json!([{"s": 60}]),
        "SUM(y) = 60",
    )
    .await;
    // oracle legacy27/count_y.json — ANSI: 3
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(y) AS c FROM \"{DS}\"",
        json!([{"c": 6}]),
        "COUNT(y) counts coerced 0s = 6",
    )
    .await;
    // oracle legacy27/avg_y.json — 60/6 (ANSI: 20.0 = 60/3)
    assert_sql_both_stores(
        &app,
        "SELECT AVG(y) AS a FROM \"{DS}\"",
        json!([{"a": 10}]),
        "AVG(y) = 10",
    )
    .await;
    // oracle legacy27_ext/avg_x.json — 0/6 (ANSI: null)
    assert_sql_both_stores(
        &app,
        "SELECT AVG(x) AS a FROM \"{DS}\"",
        json!([{"a": 0}]),
        "AVG(x) all-null col = 0",
    )
    .await;
    // oracle legacy27_ext/{min,max}_{x,y}.json — ANSI: null/null/10/30
    assert_sql_both_stores(
        &app,
        "SELECT MIN(x) AS m FROM \"{DS}\"",
        json!([{"m": 0}]),
        "MIN(x) = 0",
    )
    .await;
    assert_sql_both_stores(
        &app,
        "SELECT MAX(x) AS m FROM \"{DS}\"",
        json!([{"m": 0}]),
        "MAX(x) = 0",
    )
    .await;
    assert_sql_both_stores(
        &app,
        "SELECT MIN(y) AS m FROM \"{DS}\"",
        json!([{"m": 0}]),
        "MIN(y) sees coerced 0s = 0",
    )
    .await;
    assert_sql_both_stores(
        &app,
        "SELECT MAX(y) AS m FROM \"{DS}\"",
        json!([{"m": 30}]),
        "MAX(y) = 30",
    )
    .await;
    // oracle legacy27/count_star.json
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\"",
        json!([{"c": 6}]),
        "COUNT(*) = 6",
    )
    .await;
}

#[tokio::test]
async fn sql_string_null_empty_equivalence() {
    let app = boot_app().await;
    // oracle legacy27/count_strcol.json — '' ≡ null is not counted (ANSI: 5)
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(strcol) AS c FROM \"{DS}\"",
        json!([{"c": 3}]),
        "COUNT(strcol) = 3",
    )
    .await;
    // oracle legacy27/count_strcol_is_null.json — ANSI: 1
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\" WHERE strcol IS NULL",
        json!([{"c": 3}]),
        "strcol IS NULL matches ''/null = 3",
    )
    .await;
    // oracle legacy27_ext/count_strcol_not_null.json — ANSI: 5
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\" WHERE strcol IS NOT NULL",
        json!([{"c": 3}]),
        "strcol IS NOT NULL = 3",
    )
    .await;
    // oracle legacy27_ext/count_strcol_eq_empty.json — ANSI: 2
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\" WHERE strcol = ''",
        json!([{"c": 3}]),
        "strcol = '' matches ''/null = 3",
    )
    .await;
    // oracle legacy27_ext/count_strcol_in_a_empty.json — ANSI: 3
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\" WHERE strcol IN ('a','')",
        json!([{"c": 4}]),
        "strcol IN ('a','') = 4",
    )
    .await;
}

#[tokio::test]
async fn sql_numeric_filters_see_coerced_zeros() {
    let app = boot_app().await;
    // oracle legacy27/count_x_is_null.json — ANSI: 6
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\" WHERE x IS NULL",
        json!([{"c": 0}]),
        "x IS NULL never matches = 0",
    )
    .await;
    // oracle legacy27_ext/count_x_not_null.json — ANSI: 0
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\" WHERE x IS NOT NULL",
        json!([{"c": 6}]),
        "x IS NOT NULL = 6",
    )
    .await;
    // oracle legacy27_ext/count_x_eq_0.json — ANSI: 0
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\" WHERE x = 0",
        json!([{"c": 6}]),
        "x = 0 matches all coerced rows = 6",
    )
    .await;
    // oracle legacy27_ext/count_y_eq_0.json — ANSI: 0
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\" WHERE y = 0",
        json!([{"c": 3}]),
        "y = 0 matches the 3 coerced rows",
    )
    .await;
    // oracle legacy27_ext/count_y_lt_15.json — 10 + three 0s (ANSI: 1)
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\" WHERE y < 15",
        json!([{"c": 4}]),
        "y < 15 = 4",
    )
    .await;
    // oracle legacy27_ext/count_not_y_eq_10.json — 3VL delta (ANSI: 2)
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(*) AS c FROM \"{DS}\" WHERE NOT (y = 10)",
        json!([{"c": 5}]),
        "NOT (y = 10) = 5",
    )
    .await;
}

#[tokio::test]
async fn sql_empty_set_aggregate_defaults() {
    let app = boot_app().await;
    // oracle legacy27_ext/empty_{sum,count,avg}_y.json — ANSI: null/0/null
    assert_sql_both_stores(
        &app,
        "SELECT SUM(y) AS s FROM \"{DS}\" WHERE tag = 'nope'",
        json!([{"s": 0}]),
        "empty-set SUM = 0",
    )
    .await;
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(y) AS c FROM \"{DS}\" WHERE tag = 'nope'",
        json!([{"c": 0}]),
        "empty-set COUNT = 0",
    )
    .await;
    assert_sql_both_stores(
        &app,
        "SELECT AVG(y) AS a FROM \"{DS}\" WHERE tag = 'nope'",
        json!([{"a": 0}]),
        "empty-set AVG = 0",
    )
    .await;
    // oracle legacy27_ext/empty_{min,max}_y.json — the long init sentinels
    // leak through in legacy Druid (ANSI: null/null).
    assert_sql_both_stores(
        &app,
        "SELECT MIN(y) AS m FROM \"{DS}\" WHERE tag = 'nope'",
        json!([{"m": 9_223_372_036_854_775_807_i64}]),
        "empty-set MIN = i64::MAX sentinel",
    )
    .await;
    assert_sql_both_stores(
        &app,
        "SELECT MAX(y) AS m FROM \"{DS}\" WHERE tag = 'nope'",
        json!([{"m": -9_223_372_036_854_775_808_i64}]),
        "empty-set MAX = i64::MIN sentinel",
    )
    .await;
}

#[tokio::test]
async fn sql_count_distinct() {
    let app = boot_app().await;
    // oracle legacy27_ext/count_distinct_strcol.json — the merged ''/null
    // value COUNTS as a distinct value on the default (approximate) path
    // (ANSI: 3 — Druid's sketch drops '').
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(DISTINCT strcol) AS c FROM \"{DS}\"",
        json!([{"c": 4}]),
        "approx COUNT(DISTINCT strcol) = 4",
    )
    .await;
    // oracle legacy27_ext/count_distinct_y.json — {0,10,20,30} (ANSI: 3)
    assert_sql_both_stores(
        &app,
        "SELECT COUNT(DISTINCT y) AS c FROM \"{DS}\"",
        json!([{"c": 4}]),
        "approx COUNT(DISTINCT y) = 4",
    )
    .await;
    // oracle legacy27_ext/count_distinct_strcol_exact.json — the EXACT
    // (useApproximateCountDistinct=false) path EXCLUDES the merged
    // ''/null value: 3 (ANSI exact: 4 — the exact paths are the MIRROR
    // IMAGE of the approximate ones, both measured).
    for ds in ["legacy_null_compat", "legacy_null_ansistore"] {
        let q = format!("SELECT COUNT(DISTINCT strcol) AS c FROM \"{ds}\"");
        let body = sql_ctx(
            app.clone(),
            &q,
            json!({"useApproximateCountDistinct": false}),
        )
        .await;
        assert_rows_match(
            &body,
            &json!([{"c": 3}]),
            &format!("exact COUNT(DISTINCT strcol) = 3 [{ds}]"),
        );
    }
}

#[tokio::test]
async fn sql_group_by_merges_null_and_empty() {
    let app = boot_app().await;
    // oracle legacy27/group_strcol.json — ONE merged group RENDERED "" on
    // the SQL wire, n=3, first in ASC order (ANSI: separate null:1 + "":2).
    assert_sql_both_stores(
        &app,
        "SELECT strcol, COUNT(*) AS n FROM \"{DS}\" GROUP BY strcol ORDER BY strcol",
        json!([
            {"strcol": "", "n": 3},
            {"strcol": "a", "n": 1},
            {"strcol": "b", "n": 1},
            {"strcol": "c", "n": 1}
        ]),
        "GROUP BY strcol ASC",
    )
    .await;
    // oracle legacy27_ext/group_strcol_desc.json — merged "" group LAST in
    // DESC (ANSI: "" then null last).
    assert_sql_both_stores(
        &app,
        "SELECT strcol, COUNT(*) AS n FROM \"{DS}\" GROUP BY strcol ORDER BY strcol DESC",
        json!([
            {"strcol": "c", "n": 1},
            {"strcol": "b", "n": 1},
            {"strcol": "a", "n": 1},
            {"strcol": "", "n": 3}
        ]),
        "GROUP BY strcol DESC",
    )
    .await;
    // oracle legacy27_ext/group_strcol_sum_y.json — the b group's SUM over
    // its single coerced-0 row is 0, not null (ANSI: null).
    assert_sql_both_stores(
        &app,
        "SELECT strcol, SUM(y) AS s FROM \"{DS}\" GROUP BY strcol ORDER BY strcol",
        json!([
            {"strcol": "", "s": 20},
            {"strcol": "a", "s": 10},
            {"strcol": "b", "s": 0},
            {"strcol": "c", "s": 30}
        ]),
        "GROUP BY strcol SUM(y)",
    )
    .await;
    // oracle legacy27_ext/group_y.json — numeric group key is the literal
    // coerced 0 (ANSI: a null-keyed group).
    assert_sql_both_stores(
        &app,
        "SELECT y, COUNT(*) AS n FROM \"{DS}\" GROUP BY y ORDER BY y",
        json!([
            {"y": 0, "n": 3},
            {"y": 10, "n": 1},
            {"y": 20, "n": 1},
            {"y": 30, "n": 1}
        ]),
        "GROUP BY y",
    )
    .await;
}

/// Expression-inside-aggregate cells: the oracle pins legacy
/// `SUM(y + 1)` = 66 (`sum_y_plus_1.json`; ANSI: 63) and
/// `SUM(COALESCE(y, -1))` = 60 (`sum_coalesce_y.json`; ANSI: 57) — but
/// FerroDruid's SQL planner does not lower an EXPRESSION inside an
/// aggregate argument in ANY mode (aggregates take plain field names;
/// a PRE-EXISTING SQL-surface limitation, not a null-semantics
/// divergence).  This test pins that the legacy latch does not change
/// that failure mode (fails loud at planning, never a silent wrong
/// number); wiring aggregate-argument expressions is a follow-on, at
/// which point the two oracle answers above become the assertions.
#[tokio::test]
async fn sql_expressions_in_aggregates_fail_loud_not_wrong() {
    let app = boot_app().await;
    for q in [
        "SELECT SUM(y + 1) AS s FROM \"legacy_null_compat\"",
        "SELECT SUM(COALESCE(y, -1)) AS s FROM \"legacy_null_compat\"",
    ] {
        let (status, body) = post_json(app.clone(), "/druid/v2/sql", json!({"query": q})).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "[{q}] must fail loud at planning (pre-existing expression-in-aggregate \
             limitation), got: {body}"
        );
    }
}

#[tokio::test]
async fn sql_projection_renders_empty_string_and_zeros() {
    let app = boot_app().await;
    // oracle legacy27_ext/select_all_rows.json — on the SQL wire the
    // merged ''/null strcol rows render as "" (the NATIVE scan renders the
    // same rows as null — both pinned), and x/y render coerced 0s.
    assert_sql_both_stores(
        &app,
        "SELECT tag, strcol, x, y FROM \"{DS}\"",
        json!([
            {"tag": "r0", "strcol": "a", "x": 0, "y": 10},
            {"tag": "r1", "strcol": "b", "x": 0, "y": 0},
            {"tag": "r2", "strcol": "", "x": 0, "y": 20},
            {"tag": "r3", "strcol": "", "x": 0, "y": 0},
            {"tag": "r4", "strcol": "", "x": 0, "y": 0},
            {"tag": "r5", "strcol": "c", "x": 0, "y": 30}
        ]),
        "SELECT * projection",
    )
    .await;
}

// ===========================================================================
// SQL battery — DOUBLE / FLOAT coercion (oracle/legacy27_ext/ext_*)
// ===========================================================================

#[tokio::test]
async fn sql_double_float_coercion() {
    let app = boot_app().await;
    let cells: Vec<(&str, Value, &str)> = vec![
        // ANSI answers in comments.
        (
            "SELECT SUM(d) AS s FROM \"legacy_null_ext\"",
            json!([{"s": 5.0}]),
            "SUM(d)",
        ), // ANSI 5.0
        (
            "SELECT COUNT(d) AS c FROM \"legacy_null_ext\"",
            json!([{"c": 4}]),
            "COUNT(d)",
        ), // ANSI 2
        (
            "SELECT AVG(d) AS a FROM \"legacy_null_ext\"",
            json!([{"a": 1.25}]),
            "AVG(d)",
        ), // ANSI 2.5
        (
            "SELECT MIN(d) AS m FROM \"legacy_null_ext\"",
            json!([{"m": 0.0}]),
            "MIN(d)",
        ), // ANSI 1.5
        (
            "SELECT MAX(d) AS m FROM \"legacy_null_ext\"",
            json!([{"m": 3.5}]),
            "MAX(d)",
        ), // ANSI 3.5
        (
            "SELECT COUNT(*) AS c FROM \"legacy_null_ext\" WHERE d IS NULL",
            json!([{"c": 0}]),
            "d IS NULL", // ANSI 2
        ),
        (
            "SELECT COUNT(*) AS c FROM \"legacy_null_ext\" WHERE d = 0",
            json!([{"c": 2}]),
            "d = 0", // ANSI 0
        ),
        (
            "SELECT SUM(f) AS s FROM \"legacy_null_ext\"",
            json!([{"s": 7.0}]),
            "SUM(f)",
        ), // ANSI 7.0
        (
            "SELECT COUNT(f) AS c FROM \"legacy_null_ext\"",
            json!([{"c": 4}]),
            "COUNT(f)",
        ), // ANSI 2
        (
            "SELECT COUNT(*) AS c FROM \"legacy_null_ext\" WHERE f IS NULL",
            json!([{"c": 0}]),
            "f IS NULL", // ANSI 2
        ),
        // oracle legacy27_ext/ext_group_d.json — d groups: 0.0 (merged
        // nulls), 1.5, 3.5 (ANSI: null group first).
        (
            "SELECT d, COUNT(*) AS n FROM \"legacy_null_ext\" GROUP BY d ORDER BY d",
            json!([{"d": 0.0, "n": 2}, {"d": 1.5, "n": 1}, {"d": 3.5, "n": 1}]),
            "GROUP BY d",
        ),
        // oracle legacy27_ext/ext_empty_sum_d.json (ANSI: null)
        (
            "SELECT SUM(d) AS s FROM \"legacy_null_ext\" WHERE tag = 'nope'",
            json!([{"s": 0.0}]),
            "empty-set SUM(d)",
        ),
    ];
    for (q, expected, label) in cells {
        let body = sql(app.clone(), q).await;
        assert_rows_match(&body, &expected, label);
    }
    // oracle legacy27_ext/ext_empty_{min,max}_d.json — legacy Druid leaks
    // the ±Infinity init sentinels, rendered as JSON STRINGS on the SQL
    // wire (ANSI: null).
    let body = sql(
        app.clone(),
        "SELECT MIN(d) AS m FROM \"legacy_null_ext\" WHERE tag = 'nope'",
    )
    .await;
    assert_eq!(
        body,
        json!([{"m": "Infinity"}]),
        "empty-set MIN(d) = \"Infinity\" sentinel string"
    );
    let body = sql(
        app.clone(),
        "SELECT MAX(d) AS m FROM \"legacy_null_ext\" WHERE tag = 'nope'",
    )
    .await;
    assert_eq!(
        body,
        json!([{"m": "-Infinity"}]),
        "empty-set MAX(d) = \"-Infinity\" sentinel string"
    );
}

// ===========================================================================
// Native battery — /druid/v2 (oracle/legacy27_ext/native_*)
// ===========================================================================

async fn native(app: Router, body: Value) -> Value {
    let (status, json) = post_json(app, "/druid/v2", body).await;
    assert_eq!(status, StatusCode::OK, "native query failed: {json}");
    json
}

/// oracle legacy27/native_scan.json — the NATIVE scan renders the merged
/// ''/null strcol rows as JSON null (the SQL wire renders "" — both
/// pinned), and numerics as coerced 0s.
///
/// NOTE on the envelope: FerroDruid's `/druid/v2` scan responds with ONE
/// object carrying map-shaped `events` (Druid wraps per-segment objects
/// in an array and honors `compactedList` with column-ordered value
/// arrays).  That envelope shape is a PRE-EXISTING FerroDruid native-wire
/// divergence present in ANSI mode too — out of W-B scope; this test
/// pins the legacy CELL VALUES inside FerroDruid's envelope.
#[tokio::test]
async fn native_scan_renders_merged_string_as_null() {
    let app = boot_app().await;
    for ds in ["legacy_null_compat", "legacy_null_ansistore"] {
        let body = native(
            app.clone(),
            json!({
                "queryType": "scan", "dataSource": ds,
                "intervals": ["2024-01-01/2024-01-02"],
                "columns": ["__time", "tag", "strcol", "x", "y"],
                "resultFormat": "list", "limit": 10
            }),
        )
        .await;
        let events = body["events"]
            .as_array()
            .unwrap_or_else(|| panic!("events missing from scan response: {body}"));
        assert_eq!(events.len(), 6, "scan row count [{ds}]");
        let base = base_millis();
        // (tag, strcol, x, y) per row — strcol JSON null on the merged
        // ''/null rows r2/r3/r4 (oracle native_scan.json events).
        let expected: Vec<(&str, Value, i64, i64)> = vec![
            ("r0", json!("a"), 0, 10),
            ("r1", json!("b"), 0, 0),
            ("r2", json!(null), 0, 20),
            ("r3", json!(null), 0, 0),
            ("r4", json!(null), 0, 0),
            ("r5", json!("c"), 0, 30),
        ];
        for (i, (tag, strcol, x, y)) in expected.iter().enumerate() {
            let row = &events[i];
            assert_eq!(
                row["__time"],
                json!(base + i as i64 * 3_600_000),
                "row {i} time [{ds}]"
            );
            assert_eq!(row["tag"], json!(tag), "row {i} tag [{ds}]");
            assert_eq!(&row["strcol"], strcol, "row {i} strcol [{ds}]");
            assert_eq!(row["x"], json!(x), "row {i} x [{ds}]");
            assert_eq!(row["y"], json!(y), "row {i} y [{ds}]");
        }
    }
}

/// oracle legacy27_ext/native_ts_aggs.json — sums/mins/maxes over the
/// coerced values (ANSI: sx/minx/maxx null, miny 10).
#[tokio::test]
async fn native_timeseries_aggs() {
    let app = boot_app().await;
    for ds in ["legacy_null_compat", "legacy_null_ansistore"] {
        let body = native(
            app.clone(),
            json!({
                "queryType": "timeseries", "dataSource": ds, "granularity": "all",
                "intervals": ["2024-01-01/2024-01-02"],
                "aggregations": [
                    {"type": "longSum", "name": "sx", "fieldName": "x"},
                    {"type": "longSum", "name": "sy", "fieldName": "y"},
                    {"type": "count", "name": "cnt"},
                    {"type": "longMin", "name": "miny", "fieldName": "y"},
                    {"type": "longMax", "name": "maxy", "fieldName": "y"},
                    {"type": "longMin", "name": "minx", "fieldName": "x"},
                    {"type": "longMax", "name": "maxx", "fieldName": "x"}
                ]
            }),
        )
        .await;
        let result = &body[0]["result"];
        assert_eq!(result["sx"], json!(0), "sx [{ds}]");
        assert_eq!(result["sy"], json!(60), "sy [{ds}]");
        assert_eq!(result["cnt"], json!(6), "cnt [{ds}]");
        assert_eq!(result["miny"], json!(0), "miny [{ds}]");
        assert_eq!(result["maxy"], json!(30), "maxy [{ds}]");
        assert_eq!(result["minx"], json!(0), "minx [{ds}]");
        assert_eq!(result["maxx"], json!(0), "maxx [{ds}]");
    }
}

/// oracle legacy27_ext/native_ts_empty.json — an empty bucket still
/// emits a row carrying the legacy init sentinels (ANSI: nulls).
#[tokio::test]
async fn native_timeseries_empty_bucket_sentinels() {
    let app = boot_app().await;
    let body = native(
        app.clone(),
        json!({
            "queryType": "timeseries", "dataSource": "legacy_null_compat",
            "granularity": "all", "intervals": ["2024-01-01/2024-01-02"],
            "filter": {"type": "selector", "dimension": "tag", "value": "nope"},
            "aggregations": [
                {"type": "longSum", "name": "sy", "fieldName": "y"},
                {"type": "count", "name": "cnt"},
                {"type": "longMin", "name": "miny", "fieldName": "y"},
                {"type": "longMax", "name": "maxy", "fieldName": "y"}
            ]
        }),
    )
    .await;
    let result = &body[0]["result"];
    assert_eq!(result["cnt"], json!(0));
    assert_eq!(result["sy"], json!(0), "empty-set longSum = 0");
    assert_eq!(
        result["miny"],
        json!(9_223_372_036_854_775_807_i64),
        "empty-set longMin = i64::MAX sentinel"
    );
    assert_eq!(
        result["maxy"],
        json!(-9_223_372_036_854_775_808_i64),
        "empty-set longMax = i64::MIN sentinel"
    );
}

/// oracle legacy27_ext/native_groupby_strcol.json — the merged group's
/// NATIVE dimension value is JSON null (n=3, sy=20); the b group's sy is
/// the coerced 0.
#[tokio::test]
async fn native_groupby_merged_null_group() {
    let app = boot_app().await;
    for ds in ["legacy_null_compat", "legacy_null_ansistore"] {
        let body = native(
            app.clone(),
            json!({
                "queryType": "groupBy", "dataSource": ds, "granularity": "all",
                "intervals": ["2024-01-01/2024-01-02"],
                "dimensions": ["strcol"],
                "aggregations": [
                    {"type": "count", "name": "n"},
                    {"type": "longSum", "name": "sy", "fieldName": "y"}
                ]
            }),
        )
        .await;
        let mut groups: Vec<(Value, Value, Value)> = body
            .as_array()
            .expect("groupBy rows")
            .iter()
            .map(|r| {
                (
                    r["event"]["strcol"].clone(),
                    r["event"]["n"].clone(),
                    r["event"]["sy"].clone(),
                )
            })
            .collect();
        groups.sort_by_key(|(k, _, _)| k.to_string());
        assert_eq!(
            groups,
            vec![
                (json!("a"), json!(1), json!(10)),
                (json!("b"), json!(1), json!(0)),
                (json!("c"), json!(1), json!(30)),
                (json!(null), json!(3), json!(20)),
            ],
            "native groupBy strcol [{ds}]"
        );
    }
}

/// oracle legacy27_ext/native_topn_strcol.json — rank by sy: c(30),
/// merged-null(20), a(10), b(0).
#[tokio::test]
async fn native_topn_merged_null_group() {
    let app = boot_app().await;
    for ds in ["legacy_null_compat", "legacy_null_ansistore"] {
        let body = native(
            app.clone(),
            json!({
                "queryType": "topN", "dataSource": ds, "granularity": "all",
                "intervals": ["2024-01-01/2024-01-02"],
                "dimension": "strcol", "metric": "sy", "threshold": 10,
                "aggregations": [{"type": "longSum", "name": "sy", "fieldName": "y"}]
            }),
        )
        .await;
        let rows: Vec<(Value, Value)> = body[0]["result"]
            .as_array()
            .expect("topN rows")
            .iter()
            .map(|r| (r["strcol"].clone(), r["sy"].clone()))
            .collect();
        assert_eq!(
            rows,
            vec![
                (json!("c"), json!(30)),
                (json!(null), json!(20)),
                (json!("a"), json!(10)),
                (json!("b"), json!(0)),
            ],
            "native topN strcol [{ds}]"
        );
    }
}

/// oracle legacy27_ext/native_filter_*.json — filtered counts over the
/// native filter grammar (ANSI counts in comments).
#[tokio::test]
async fn native_filtered_counts() {
    let app = boot_app().await;
    let cells: Vec<(Value, i64, &str)> = vec![
        (
            json!({"type": "selector", "dimension": "strcol", "value": null}),
            3, // ANSI: 1
            "selector strcol null",
        ),
        (
            json!({"type": "selector", "dimension": "strcol", "value": ""}),
            3, // ANSI: 2
            "selector strcol ''",
        ),
        (
            json!({"type": "selector", "dimension": "x", "value": "0"}),
            6, // ANSI: 0
            "selector x 0",
        ),
        (
            json!({"type": "selector", "dimension": "y", "value": "0"}),
            3, // ANSI: 0
            "selector y 0",
        ),
        (
            json!({"type": "bound", "dimension": "y", "upper": "15",
                   "upperStrict": true, "ordering": "numeric"}),
            4, // ANSI: 1
            "bound y < 15",
        ),
        (
            json!({"type": "in", "dimension": "strcol", "values": ["a", null]}),
            4, // ANSI: 2
            "in strcol [a, null]",
        ),
        (
            json!({"type": "in", "dimension": "strcol", "values": ["a", ""]}),
            4, // ANSI: 3
            "in strcol [a, '']",
        ),
    ];
    for ds in ["legacy_null_compat", "legacy_null_ansistore"] {
        for (filter, expected, label) in &cells {
            let body = native(
                app.clone(),
                json!({
                    "queryType": "timeseries", "dataSource": ds, "granularity": "all",
                    "intervals": ["2024-01-01/2024-01-02"], "filter": filter,
                    "aggregations": [{"type": "count", "name": "cnt"}]
                }),
            )
            .await;
            assert_eq!(body[0]["result"]["cnt"], json!(expected), "{label} [{ds}]");
        }
    }
}
