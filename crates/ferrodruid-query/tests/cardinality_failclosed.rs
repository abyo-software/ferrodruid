// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Fail-closed exact-cardinality saturation tests (2026-07-11).
//!
//! The exact-distinct `cardinality` aggregator saturates at
//! `MAX_CARDINALITY_SET_SIZE` (a deliberate Wave 36-G2 DoS bound). Before
//! this program, a saturated aggregator finalized to a silently
//! under-counted scalar; Druid never silently returns a wrong exact
//! distinct count, so FerroDruid must FAIL the query at finalization
//! instead ([`DruidError::ResourceLimit`]).
//!
//! Every test lowers the cap to the same small value via
//! `set_exact_cardinality_cap_for_tests` (a process-wide, lower-only
//! override) so saturation can be driven without inserting 1,000,000 keys.
//! All tests in this binary use the SAME cap value, so parallel test
//! threads cannot race each other into different caps.

use std::collections::HashMap;

use ferrodruid_aggregator::set_exact_cardinality_cap_for_tests;
use ferrodruid_common::error::DruidError;
use ferrodruid_query::{DruidQuery, QueryResult, execute_query};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::ColumnData;
use serde_json::json;

/// Cap shared by every test in this binary (see module docs).
const TEST_CAP: usize = 4;

/// Build a 6-row segment where `user_id` has 6 distinct values (over the
/// test cap of 4) and `site_id` has 3 distinct values (under the cap).
fn build_segment() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("parse base ts")
        .timestamp_millis();
    let num_rows = 6usize;
    let timestamps: Vec<i64> = (0..num_rows as i64).map(|i| base + i * 60_000).collect();

    let mut columns = HashMap::new();
    columns.insert("__time".to_string(), ColumnData::Long(timestamps));
    // 6 distinct values -> saturates a cap of 4.
    columns.insert(
        "user_id".to_string(),
        ColumnData::Long(vec![101, 102, 103, 104, 105, 106]),
    );
    // 3 distinct values -> stays exact under a cap of 4.
    columns.insert(
        "site_id".to_string(),
        ColumnData::Long(vec![1, 2, 3, 1, 2, 3]),
    );
    // Constant column: groups all 6 rows into ONE topN cell so that cell's
    // per-cell cardinality aggregator sees all 6 distinct user_ids.
    columns.insert("k".to_string(), ColumnData::Long(vec![7, 7, 7, 7, 7, 7]));

    SegmentData {
        version: 9,
        num_rows,
        interval: ferrodruid_segment::Interval {
            start_millis: base,
            end_millis: base + 86_400_000,
        },
        dimensions: vec!["user_id".to_string(), "site_id".to_string()],
        metrics: Vec::new(),
        columns,
        time_sorted: true,
    }
}

fn parse_query(v: serde_json::Value) -> DruidQuery {
    serde_json::from_value(v).expect("query JSON must parse")
}

fn assert_fails_closed(result: Result<QueryResult, DruidError>, ctx: &str) {
    match result {
        Err(DruidError::ResourceLimit { kind, limit, .. }) => {
            assert!(
                kind.contains("cardinality.maxExactSetSize"),
                "{ctx}: kind must name the cardinality exact-set limit, got: {kind}"
            );
            assert_eq!(limit, TEST_CAP, "{ctx}: limit must be the effective cap");
        }
        Err(other) => panic!("{ctx}: expected DruidError::ResourceLimit, got {other:?}"),
        Ok(r) => panic!(
            "{ctx}: saturated exact cardinality must FAIL CLOSED, but query \
             succeeded with {r:?} (silent under-count)"
        ),
    }
}

#[test]
fn timeseries_exact_cardinality_saturation_fails_closed() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let segment = build_segment();
    let query = parse_query(json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        "granularity": "all",
        "aggregations": [
            {"type": "cardinality", "name": "uniq", "fields": ["user_id"], "byRow": false}
        ]
    }));
    assert_fails_closed(execute_query(&query, &segment), "timeseries");
}

#[test]
fn timeseries_exact_cardinality_below_cap_stays_exact() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let segment = build_segment();
    let query = parse_query(json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        "granularity": "all",
        "aggregations": [
            {"type": "cardinality", "name": "uniq", "fields": ["site_id"], "byRow": false}
        ]
    }));
    let result = execute_query(&query, &segment).expect("below-cap query must succeed");
    match result {
        QueryResult::Timeseries(rows) => {
            assert_eq!(rows.len(), 1);
            // Multi-shard exact union (2026-07-12): the raw per-segment
            // partial now carries the full-set `CardinalityState` envelope
            // (so the broker can union across segments); its exact count
            // must be 3 and non-saturated.  The broker's finalization pass
            // collapses this to the bare `3` before any client sees it
            // (covered by the REST e2e binaries).
            let value = rows[0].result.get("uniq").expect("uniq present");
            assert_eq!(
                ferrodruid_aggregator::CardinalityState::peek_json(value),
                Ok(Some((false, 3))),
                "non-saturated exact cardinality partial must carry the \
                 exact count 3, got {value}"
            );
        }
        other => panic!("expected timeseries result, got {other:?}"),
    }
}

#[test]
fn groupby_exact_cardinality_saturation_fails_closed() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let segment = build_segment();
    let query = parse_query(json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        "granularity": "all",
        "dimensions": [],
        "aggregations": [
            {"type": "cardinality", "name": "uniq", "fields": ["user_id"], "byRow": false}
        ]
    }));
    assert_fails_closed(execute_query(&query, &segment), "groupBy");
}

#[test]
fn topn_exact_cardinality_saturation_fails_closed() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let segment = build_segment();
    // Group on the constant column `k` so the single TopN cell sees all
    // 6 distinct user_ids and saturates the test cap of 4.
    let query = parse_query(json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        "granularity": "all",
        "dimension": "k",
        "metric": "uniq",
        "threshold": 10,
        "aggregations": [
            {"type": "cardinality", "name": "uniq", "fields": ["user_id"], "byRow": false}
        ]
    }));
    assert_fails_closed(execute_query(&query, &segment), "topN");
}

/// E16 SQL lowering shape: exact COUNT(DISTINCT col) is a `cardinality`
/// aggregator wrapped in a not-null `filtered`. The wrapper must not
/// swallow the saturation report.
#[test]
fn filtered_wrapped_cardinality_saturation_fails_closed() {
    set_exact_cardinality_cap_for_tests(TEST_CAP);
    let segment = build_segment();
    let query = parse_query(json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        "granularity": "all",
        "aggregations": [
            {"type": "filtered",
             "filter": {"type": "not",
                        "field": {"type": "selector", "dimension": "user_id", "value": null}},
             "aggregator": {"type": "cardinality", "name": "uniq",
                            "fields": ["user_id"], "byRow": false}}
        ]
    }));
    assert_fails_closed(execute_query(&query, &segment), "filtered(timeseries)");
}
