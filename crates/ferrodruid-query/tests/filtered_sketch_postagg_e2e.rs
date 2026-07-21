// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Executor-level tests for the aggregator/post-aggregator capabilities the
//! SQL planner lowers `ROUND(AVG(x), n)` / `COUNT(DISTINCT col)` /
//! `COUNT(col)` to:
//!
//! (a) `filtered` aggregator with a not-null selector filter counting exactly
//!     the non-null rows per group — through GroupBy, Timeseries, and TopN
//!     (all of which must fall back from their vectorized fast paths when a
//!     `filtered` aggregator is present, and must agree with the fast path on
//!     the plain count/sum aggregators);
//! (b) `HLLSketchBuild` aggregator + `HLLSketchEstimate` post-aggregator with
//!     `"round": true` returning the exact distinct count for a
//!     small-cardinality column;
//! (c) `expression` post-aggregator `round("s" / "c", 1)` over sum/count
//!     aggregators in Timeseries, GroupBy, and TopN — including the decimal
//!     (BigDecimal-style) rounding semantics where `61 / 20` rounds to `3.1`
//!     even though the IEEE quotient is slightly below 3.05.
//!
//! Null representation note: the segment format has no null bitmaps.  A
//! string-column row materialises as JSON `null` when its ordinal has no
//! dictionary entry (`FrontCodedDictionary::get -> None`); the batch-ingest
//! layer currently encodes ingested nulls as `""` instead (see the honest
//! limitations in the task report).  These tests build the null-bearing
//! column directly with out-of-dictionary ordinals so the executor-visible
//! value is a true JSON `null`.

use std::collections::HashMap;

use ferrodruid_bitmap::DruidBitmap;
use ferrodruid_dict::FrontCodedDictionary;
use ferrodruid_query::{DruidQuery, QueryResult, execute_query};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::{ColumnData, StringColumnData};
use serde_json::json;

/// Ordinal used to represent a null row in the `user` column (one past the
/// last dictionary entry, so `dictionary.get(ord)` is `None`).
const NULL_ORD: u32 = 3;

/// Build a 40-row segment with two `country` groups and a nullable `user`
/// column.
///
/// - `country`: rows 0..20 = `"jp"`, rows 20..40 = `"us"`.
/// - `user`: dictionary `["alice","bob","carol"]`;
///   jp rows: 15 non-null (`alice`/`bob` only), 5 null;
///   us rows: 12 non-null (`alice`/`bob`/`carol`), 8 null.
/// - `v` (long): jp rows sum to 61 (19 threes + one 4), us rows sum to 50
///   (alternating 2/3) — so `sum/count` is 3.05 for jp (a decimal-rounding
///   edge case: the IEEE quotient is the double just below 3.05) and 2.5
///   for us; the overall ratio is 111/40 = 2.775.
fn build_null_user_segment() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2013-01-01T00:00:00Z")
        .expect("base ts")
        .timestamp_millis();
    let num_rows = 40usize;
    let timestamps: Vec<i64> = (0..num_rows as i64).map(|i| base + i * 1000).collect();

    // country: 20x jp (ord 0), 20x us (ord 1)
    let country_ords: Vec<u32> = (0..num_rows).map(|i| u32::from(i >= 20)).collect();
    let country_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec!["jp".to_string(), "us".to_string()]),
        encoded_values: country_ords.clone(),
        bitmap_indexes: build_bitmaps(2, &country_ords),
    });

    // user: dictionary [alice=0, bob=1, carol=2]; NULL_ORD = null row.
    // jp rows (0..20): 15 non-null alternating alice/bob, then 5 nulls.
    // us rows (20..40): 12 non-null cycling alice/bob/carol, then 8 nulls.
    let mut user_ords: Vec<u32> = Vec::with_capacity(num_rows);
    for i in 0..20u32 {
        user_ords.push(if i < 15 { i % 2 } else { NULL_ORD });
    }
    for i in 0..20u32 {
        user_ords.push(if i < 12 { i % 3 } else { NULL_ORD });
    }
    // Bitmaps only cover real dictionary entries; null rows are unindexed.
    let user_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec![
            "alice".to_string(),
            "bob".to_string(),
            "carol".to_string(),
        ]),
        encoded_values: user_ords.clone(),
        bitmap_indexes: build_bitmaps(3, &user_ords),
    });

    // v: jp = nineteen 3s + one 4 (sum 61), us = alternating 2/3 (sum 50).
    let mut v: Vec<i64> = Vec::with_capacity(num_rows);
    for i in 0..20 {
        v.push(if i == 19 { 4 } else { 3 });
    }
    for i in 0..20 {
        v.push(if i % 2 == 0 { 2 } else { 3 });
    }
    debug_assert_eq!(v.iter().take(20).sum::<i64>(), 61);
    debug_assert_eq!(v.iter().skip(20).sum::<i64>(), 50);

    let mut columns = HashMap::new();
    columns.insert("__time".to_string(), ColumnData::Long(timestamps));
    columns.insert("country".to_string(), country_col);
    columns.insert("user".to_string(), user_col);
    columns.insert("v".to_string(), ColumnData::Long(v));

    let end = chrono::DateTime::parse_from_rfc3339("2013-01-02T00:00:00Z")
        .expect("end ts")
        .timestamp_millis();
    SegmentData {
        version: 9,
        num_rows,
        interval: ferrodruid_segment::Interval {
            start_millis: base,
            end_millis: end,
        },
        dimensions: vec!["country".to_string(), "user".to_string()],
        metrics: vec!["v".to_string()],
        columns,
        time_sorted: true,
    }
}

/// One bitmap per real dictionary entry; out-of-dictionary (null) ordinals
/// are simply not indexed.
fn build_bitmaps(cardinality: usize, ordinals: &[u32]) -> Vec<DruidBitmap> {
    let mut bitmaps: Vec<DruidBitmap> = (0..cardinality).map(|_| DruidBitmap::new()).collect();
    for (row_idx, &ord) in ordinals.iter().enumerate() {
        if (ord as usize) < cardinality {
            bitmaps[ord as usize].insert(row_idx as u32);
        }
    }
    bitmaps
}

const INTERVAL: &str = "2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z";

/// The `filtered` aggregator JSON the SQL planner emits for `COUNT(col)`.
fn filtered_not_null_count_json() -> serde_json::Value {
    json!({
        "type": "filtered",
        "filter": {
            "type": "not",
            "field": {"type": "selector", "dimension": "user", "value": null}
        },
        "aggregator": {"type": "count", "name": "user_cnt"}
    })
}

fn run(query_json: &serde_json::Value, segment: &SegmentData) -> QueryResult {
    let query: DruidQuery =
        serde_json::from_value(query_json.clone()).expect("query JSON must parse");
    execute_query(&query, segment).expect("query must execute")
}

/// Extract `(event[field])` per country from a GroupBy result, sorted by
/// country value.
fn groupby_field_by_country(result: &QueryResult, field: &str) -> Vec<(String, serde_json::Value)> {
    let QueryResult::GroupBy(rows) = result else {
        panic!("expected GroupBy result");
    };
    let mut out: Vec<(String, serde_json::Value)> = rows
        .iter()
        .map(|r| {
            let country = r
                .event
                .get("country")
                .and_then(|v| v.as_str())
                .expect("country value")
                .to_string();
            let val = r
                .event
                .get(field)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            (country, val)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

// ===========================================================================
// (a) filtered not-null count — GroupBy / Timeseries / TopN
// ===========================================================================

#[test]
fn groupby_filtered_not_null_count_per_group() {
    let segment = build_null_user_segment();
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["country"],
        "aggregations": [
            {"type": "count", "name": "c"},
            {"type": "longSum", "name": "s", "fieldName": "v"},
            filtered_not_null_count_json()
        ]
    });
    let result = run(&query, &segment);
    assert_eq!(
        groupby_field_by_country(&result, "user_cnt"),
        vec![("jp".to_string(), json!(15)), ("us".to_string(), json!(12)),],
        "filtered not-null count must count exactly the non-null rows per group"
    );
    // Plain aggregators in the same (general-path) query.
    assert_eq!(
        groupby_field_by_country(&result, "c"),
        vec![("jp".to_string(), json!(20)), ("us".to_string(), json!(20))],
    );
    assert_eq!(
        groupby_field_by_country(&result, "s"),
        vec![("jp".to_string(), json!(61)), ("us".to_string(), json!(50))],
    );
}

/// Fast-path vs general-path consistency: the same count/sum aggregators
/// must produce identical values whether the query qualifies for the
/// vectorized fast path (plain count/longSum) or is forced onto the general
/// path by the presence of a `filtered` aggregator.
#[test]
fn groupby_fast_path_and_filtered_fallback_agree_on_shared_aggregators() {
    let segment = build_null_user_segment();
    let fast = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["country"],
        "aggregations": [
            {"type": "count", "name": "c"},
            {"type": "longSum", "name": "s", "fieldName": "v"}
        ]
    });
    let mut general = fast.clone();
    general["aggregations"]
        .as_array_mut()
        .expect("aggregations array")
        .push(filtered_not_null_count_json());

    let fast_result = run(&fast, &segment);
    let general_result = run(&general, &segment);
    for field in ["c", "s"] {
        assert_eq!(
            groupby_field_by_country(&fast_result, field),
            groupby_field_by_country(&general_result, field),
            "fast path and filtered-fallback path disagree on aggregator '{field}'"
        );
    }
}

#[test]
fn timeseries_filtered_not_null_count() {
    let segment = build_null_user_segment();
    let query = json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [
            {"type": "count", "name": "c"},
            filtered_not_null_count_json()
        ]
    });
    let QueryResult::Timeseries(rows) = run(&query, &segment) else {
        panic!("expected Timeseries result");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].result.get("c"), Some(&json!(40)));
    assert_eq!(
        rows[0].result.get("user_cnt"),
        Some(&json!(27)),
        "timeseries filtered not-null count must be 15 + 12 = 27"
    );
}

#[test]
fn topn_filtered_not_null_count() {
    let segment = build_null_user_segment();
    let query = json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimension": "country",
        "metric": "user_cnt",
        "threshold": 2,
        "aggregations": [
            {"type": "count", "name": "c"},
            filtered_not_null_count_json()
        ]
    });
    let QueryResult::TopN(buckets) = run(&query, &segment) else {
        panic!("expected TopN result");
    };
    assert_eq!(buckets.len(), 1);
    let entries = &buckets[0].result;
    assert_eq!(entries.len(), 2);
    // Ranked by user_cnt descending: jp (15) then us (12).
    assert_eq!(entries[0].get("country"), Some(&json!("jp")));
    assert_eq!(entries[0].get("user_cnt"), Some(&json!(15)));
    assert_eq!(entries[1].get("country"), Some(&json!("us")));
    assert_eq!(entries[1].get("user_cnt"), Some(&json!(12)));
}

// ===========================================================================
// (b) HLLSketchBuild + HLLSketchEstimate(round=true) — exact for small n
// ===========================================================================

#[test]
fn timeseries_hll_build_estimate_round_exact_distinct_count() {
    let segment = build_null_user_segment();
    let query = json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [
            {"type": "count", "name": "c"},
            {"type": "HLLSketchBuild", "name": "hll_user", "fieldName": "user"}
        ],
        "postAggregations": [
            {
                "type": "HLLSketchEstimate",
                "name": "distinct_users",
                "field": {"type": "fieldAccess", "name": "fa", "fieldName": "hll_user"},
                "round": true
            }
        ]
    });
    let QueryResult::Timeseries(rows) = run(&query, &segment) else {
        panic!("expected Timeseries result");
    };
    assert_eq!(rows.len(), 1);
    // 3 distinct non-null users (alice, bob, carol); nulls must not count.
    assert_eq!(
        rows[0].result.get("distinct_users"),
        Some(&json!(3.0)),
        "rounded HLL estimate must be the exact distinct count for small n"
    );
}

#[test]
fn groupby_hll_build_estimate_round_exact_per_group() {
    let segment = build_null_user_segment();
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["country"],
        "aggregations": [
            {"type": "HLLSketchBuild", "name": "hll_user", "fieldName": "user"}
        ],
        "postAggregations": [
            {
                "type": "HLLSketchEstimate",
                "name": "distinct_users",
                "field": {"type": "fieldAccess", "name": "fa", "fieldName": "hll_user"},
                "round": true
            }
        ]
    });
    let result = run(&query, &segment);
    // jp users: alice/bob only -> 2; us users: alice/bob/carol -> 3.
    assert_eq!(
        groupby_field_by_country(&result, "distinct_users"),
        vec![
            ("jp".to_string(), json!(2.0)),
            ("us".to_string(), json!(3.0)),
        ],
    );
}

// ===========================================================================
// (c) expression post-agg round("s" / "c", 1) — Timeseries / GroupBy / TopN
// ===========================================================================

fn avg_expression_post_agg() -> serde_json::Value {
    json!({
        "type": "expression",
        "name": "avg_v",
        "expression": "round(\"s\" / \"c\", 1)"
    })
}

#[test]
fn timeseries_expression_round_avg() {
    let segment = build_null_user_segment();
    let query = json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [
            {"type": "count", "name": "c"},
            {"type": "longSum", "name": "s", "fieldName": "v"}
        ],
        "postAggregations": [avg_expression_post_agg()]
    });
    let QueryResult::Timeseries(rows) = run(&query, &segment) else {
        panic!("expected Timeseries result");
    };
    assert_eq!(rows.len(), 1);
    // 111 / 40 = 2.775 -> round half-up at 1dp -> 2.8
    assert_eq!(rows[0].result.get("avg_v"), Some(&json!(2.8)));
}

#[test]
fn groupby_expression_round_avg_decimal_semantics() {
    let segment = build_null_user_segment();
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["country"],
        "aggregations": [
            {"type": "count", "name": "c"},
            {"type": "longSum", "name": "s", "fieldName": "v"}
        ],
        "postAggregations": [avg_expression_post_agg()]
    });
    let result = run(&query, &segment);
    // jp: 61/20 = 3.05 (IEEE quotient just below 3.05) -> decimal half-up
    // on the shortest repr gives 3.1, not the naive 3.0.
    // us: 50/20 = 2.5 -> 2.5.
    assert_eq!(
        groupby_field_by_country(&result, "avg_v"),
        vec![
            ("jp".to_string(), json!(3.1)),
            ("us".to_string(), json!(2.5))
        ],
        "expression round must use decimal (BigDecimal-style) half-up semantics"
    );
}

#[test]
fn topn_expression_round_avg() {
    let segment = build_null_user_segment();
    let query = json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimension": "country",
        "metric": "s",
        "threshold": 2,
        "aggregations": [
            {"type": "count", "name": "c"},
            {"type": "longSum", "name": "s", "fieldName": "v"}
        ],
        "postAggregations": [avg_expression_post_agg()]
    });
    let QueryResult::TopN(buckets) = run(&query, &segment) else {
        panic!("expected TopN result");
    };
    assert_eq!(buckets.len(), 1);
    let entries = &buckets[0].result;
    assert_eq!(entries.len(), 2);
    // Ranked by s descending: jp (61) then us (50).
    assert_eq!(entries[0].get("country"), Some(&json!("jp")));
    assert_eq!(entries[0].get("avg_v"), Some(&json!(3.1)));
    assert_eq!(entries[1].get("country"), Some(&json!("us")));
    assert_eq!(entries[1].get("avg_v"), Some(&json!(2.5)));
}

/// codex-review r3 (2026-07-11): a `filtered`-wrapped multi-field
/// `cardinality` aggregator with `byRow: true` must keep TUPLE semantics —
/// the FilteredAggregator wrapper previously fell back to the trait's
/// per-field `aggregate_multi` default, counting field values instead of
/// row tuples.
#[test]
fn filtered_cardinality_by_row_keeps_tuple_semantics() {
    let segment = build_null_user_segment();
    // Restrict to country=jp AND non-null user: rows are (jp,alice)/(jp,bob)
    // repeated -> 2 distinct TUPLES, but 3 distinct field VALUES
    // ({jp} + {alice,bob}) — so tuple loss is unambiguously detectable.
    let query = json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [{
            "type": "filtered",
            "filter": {
                "type": "and",
                "fields": [
                    {"type": "not",
                     "field": {"type": "selector", "dimension": "user", "value": null}},
                    {"type": "selector", "dimension": "country", "value": "jp"}
                ]
            },
            "aggregator": {
                "type": "cardinality",
                "name": "tuple_card",
                "fields": ["country", "user"],
                "byRow": true
            }
        }]
    });
    let QueryResult::Timeseries(rows) = run(&query, &segment) else {
        panic!("expected Timeseries result");
    };
    // Multi-shard exact union (2026-07-12): the raw per-segment partial
    // carries the full-set `CardinalityState` envelope; read its exact
    // count (the broker collapses it to a bare number before clients).
    let value = rows[0]
        .result
        .get("tuple_card")
        .expect("tuple_card present");
    let (saturated, card) = ferrodruid_aggregator::CardinalityState::peek_json(value)
        .expect("tuple_card envelope must be well-formed")
        .expect("tuple_card must be a cardinality state envelope");
    assert!(!saturated);
    assert_eq!(
        card, 2,
        "byRow tuple cardinality must be 2 distinct (country,user) tuples \
         (per-field value counting would give 3), got {card}"
    );
}

/// codex-review r3 (2026-07-11): a post-aggregation that evaluates to SQL
/// null (e.g. expression x/0 -> IEEE inf -> None) must appear in native
/// result rows as an explicit JSON null under its output name — previously
/// the key was silently omitted.
#[test]
fn null_post_agg_emits_explicit_json_null_in_native_rows() {
    let segment = build_null_user_segment();
    let query = json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [{"type": "count", "name": "c"}],
        "postAggregations": [
            {"type": "expression", "name": "e", "expression": "1 / 0"}
        ]
    });
    let QueryResult::Timeseries(rows) = run(&query, &segment) else {
        panic!("expected Timeseries result");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].result.get("e"),
        Some(&serde_json::Value::Null),
        "null post-agg must be an explicit null key, not absent: {:?}",
        rows[0].result
    );
    assert_eq!(rows[0].result.get("c"), Some(&json!(40)));
}
