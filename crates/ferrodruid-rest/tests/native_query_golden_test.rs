// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)
//
// Native-query golden-fixture round-trip tests.
//
// Loads each `tests/native-query-compat/fixtures/<type>/query.json`,
// parses it into [`ferrodruid_query::DruidQuery`], asserts the variant
// matches the directory name, and round-trips through serde to verify
// the structure is stable.  Also loads each `expected_response.json`
// to confirm the canonical Druid response shape parses as JSON.
//
// **No live Druid is required** — these are offline contracts that
// describe the wire shapes FerroDruid's native-query parser must
// accept (queries) and the response shapes it must emit (responses).

use std::path::PathBuf;

use ferrodruid_query::DruidQuery;

/// Locate the workspace root by walking up from `CARGO_MANIFEST_DIR`
/// (the `ferrodruid-rest` crate dir) until we find a `Cargo.toml`
/// whose contents include `[workspace]`.
fn workspace_root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        let cargo_toml = dir.join("Cargo.toml");
        if cargo_toml.exists()
            && std::fs::read_to_string(&cargo_toml)
                .map(|s| s.contains("[workspace]"))
                .unwrap_or(false)
        {
            return dir;
        }
        if !dir.pop() {
            panic!("could not locate workspace root from CARGO_MANIFEST_DIR");
        }
    }
}

fn fixtures_dir() -> PathBuf {
    workspace_root().join("tests/native-query-compat/fixtures")
}

fn read_fixture(query_type: &str, name: &str) -> String {
    let path = fixtures_dir().join(query_type).join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read fixture {}: {e}", path.display()))
}

/// Round-trip: parse query JSON → re-serialize → parse again.
/// Returns the parsed `DruidQuery` for the caller to inspect.
fn parse_and_round_trip(query_type: &str) -> DruidQuery {
    let raw = read_fixture(query_type, "query.json");
    let q: DruidQuery =
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {query_type}/query.json: {e}"));
    let re_json = serde_json::to_string(&q).expect("re-serialize");
    let _: DruidQuery =
        serde_json::from_str(&re_json).unwrap_or_else(|e| panic!("re-parse {query_type}: {e}"));
    q
}

/// Verify `expected_response.json` is well-formed JSON of the documented
/// outer shape (top-level array for time-bucketed query types, top-level
/// object for `scan`).
fn check_response_shape(query_type: &str, expect_array: bool) {
    let raw = read_fixture(query_type, "expected_response.json");
    let v: serde_json::Value = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("parse {query_type}/expected_response.json: {e}"));
    if expect_array {
        assert!(
            v.is_array(),
            "{query_type} expected_response.json must be a top-level array (Druid native \
             {query_type} returns `[...]`)"
        );
        assert!(
            !v.as_array().unwrap().is_empty(),
            "{query_type} expected_response.json must be non-empty"
        );
    } else {
        assert!(
            v.is_object(),
            "{query_type} expected_response.json must be a top-level object"
        );
    }
}

// ---------------------------------------------------------------------------
// Per-query-type golden tests
// ---------------------------------------------------------------------------

#[test]
fn timeseries_golden_round_trips() {
    let q = parse_and_round_trip("timeseries");
    assert!(
        matches!(q, DruidQuery::Timeseries(_)),
        "expected DruidQuery::Timeseries, got {q:?}"
    );
    check_response_shape("timeseries", true);

    // First entry must have the documented `timestamp` + `result` keys.
    let raw = read_fixture("timeseries", "expected_response.json");
    let arr: Vec<serde_json::Value> = serde_json::from_str(&raw).unwrap();
    let first = arr.first().expect("non-empty");
    assert!(
        first.get("timestamp").is_some(),
        "timeseries entry missing `timestamp`"
    );
    assert!(
        first.get("result").is_some(),
        "timeseries entry missing `result`"
    );
}

#[test]
fn topn_golden_round_trips() {
    let q = parse_and_round_trip("topn");
    assert!(
        matches!(q, DruidQuery::TopN(_)),
        "expected DruidQuery::TopN, got {q:?}"
    );
    check_response_shape("topn", true);

    // `result` for topN is an array of dimension/metric maps.
    let raw = read_fixture("topn", "expected_response.json");
    let arr: Vec<serde_json::Value> = serde_json::from_str(&raw).unwrap();
    let first = arr.first().expect("non-empty");
    assert!(
        first.get("result").and_then(|r| r.as_array()).is_some(),
        "topN entry `result` must be an array of {{dimension, metric}} maps"
    );
}

#[test]
fn groupby_golden_round_trips() {
    let q = parse_and_round_trip("groupby");
    assert!(
        matches!(q, DruidQuery::GroupBy(_)),
        "expected DruidQuery::GroupBy, got {q:?}"
    );
    check_response_shape("groupby", true);

    // Druid groupBy response entries always carry `version: "v1"` and an
    // `event` map.
    let raw = read_fixture("groupby", "expected_response.json");
    let arr: Vec<serde_json::Value> = serde_json::from_str(&raw).unwrap();
    for entry in &arr {
        assert_eq!(
            entry.get("version").and_then(|v| v.as_str()),
            Some("v1"),
            "groupBy entry must have version=\"v1\""
        );
        assert!(
            entry.get("event").is_some_and(|e| e.is_object()),
            "groupBy entry must have an object `event`"
        );
        assert!(
            entry.get("timestamp").is_some(),
            "groupBy entry missing `timestamp`"
        );
    }
}

#[test]
fn scan_golden_round_trips() {
    let q = parse_and_round_trip("scan");
    assert!(
        matches!(q, DruidQuery::Scan(_)),
        "expected DruidQuery::Scan, got {q:?}"
    );
    // Scan returns a single object with `columns` + `events`.
    check_response_shape("scan", false);

    let raw = read_fixture("scan", "expected_response.json");
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(
        v.get("columns").and_then(|c| c.as_array()).is_some(),
        "scan response must have `columns: [...]`"
    );
    assert!(
        v.get("events").and_then(|e| e.as_array()).is_some(),
        "scan response must have `events: [...]`"
    );
}

#[test]
fn timeboundary_golden_round_trips() {
    let q = parse_and_round_trip("timeboundary");
    assert!(
        matches!(q, DruidQuery::TimeBoundary(_)),
        "expected DruidQuery::TimeBoundary, got {q:?}"
    );
    check_response_shape("timeboundary", true);

    // The query.json sets `bound: "maxTime"` so the response must carry
    // exactly `maxTime` (not `minTime`).
    let raw = read_fixture("timeboundary", "expected_response.json");
    let arr: Vec<serde_json::Value> = serde_json::from_str(&raw).unwrap();
    let first = arr.first().expect("non-empty");
    let result = first
        .get("result")
        .and_then(|r| r.as_object())
        .expect("timeBoundary result must be an object");
    assert!(
        result.contains_key("maxTime"),
        "with `bound: maxTime`, expected_response must include `maxTime`"
    );
}

// ---------------------------------------------------------------------------
// Fixture-discovery sanity check
// ---------------------------------------------------------------------------

/// Walk the fixtures dir and ensure every subdirectory has the
/// expected pair of files. Catches future fixture additions that
/// forget one half of the pair.
#[test]
fn every_fixture_has_query_and_response() {
    let dir = fixtures_dir();
    assert!(dir.is_dir(), "fixtures dir missing: {}", dir.display());

    let mut count = 0_usize;
    for entry in std::fs::read_dir(&dir).expect("read fixtures dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let q = path.join("query.json");
        let r = path.join("expected_response.json");
        assert!(q.is_file(), "{name}: query.json missing");
        assert!(r.is_file(), "{name}: expected_response.json missing");
        count += 1;
    }
    assert!(
        count >= 5,
        "expected at least 5 fixture directories, found {count}"
    );
}

// ---------------------------------------------------------------------------
// Notes on the API gap
// ---------------------------------------------------------------------------
//
// The strongly-typed response enum `ferrodruid_query::QueryResult` is
// `#[serde(untagged)]` and contains internal representations such as
// `GroupByResult.event: serde_json::Map<String, serde_json::Value>` and
// `TopNResult.result: Vec<serde_json::Map<...>>`.  Those types DO
// deserialise the canonical Druid response shapes — but pinning each
// `expected_response.json` to a specific enum variant via untagged
// deserialisation would mostly verify "this happens to match the first
// untagged variant", not "this matches the documented Druid shape".
//
// Therefore the runner stops at:
//   1. Query JSON → strongly-typed `DruidQuery` round-trip (real type
//      check), and
//   2. Response JSON → `serde_json::Value` + structural shape assertions
//      based on Druid's documented contracts.
//
// Adding executor-level golden assertions (parse + execute against a
// synthetic in-memory segment + diff resulting JSON) is left as a
// follow-up; today the executor unit tests in `ferrodruid-query/src/lib.rs`
// already cover that path against a hard-coded segment, and the
// `tests/druid-compat/` harness covers the live-Druid diff.
