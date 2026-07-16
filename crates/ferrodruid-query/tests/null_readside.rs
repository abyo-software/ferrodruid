// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! READ-side null-faithfulness tests (E2 query-layer task, 2026-07-11).
//!
//! Ground truth measured live against apache/druid 35.0.1 / 36.0.0 with
//! default (SQL-compatible) null handling:
//!
//! * Scan renders a null string row as JSON `null` — NOT `""`.
//! * `COUNT(DISTINCT dim)` (HLLSketchBuild) does not count null rows.
//! * `{"type":"selector","dimension":d,"value":null}` matches exactly the
//!   null rows; `not(selector null)` counts exactly the non-null rows.
//! * GroupBy on a nullable string dimension emits a group keyed by JSON
//!   `null`, distinct from any `""` group.
//! * `SUM(x)` skips nulls: a mixed group of `10, 20, null` sums to `30`.
//! * `SUM(x)` over an ALL-null group is SQL `null` (Druid 35: 30 / 30 /
//!   null per site on the 7-row nulltest dataset), and arithmetic over a
//!   null aggregate (`SUM(x)/COUNT(*)`) is null too. The fast paths and
//!   the general path must agree on `null`.
//!
//! Segments are built with the nullable `SegmentDataBuilder` methods so the
//! null representation is exactly what batch ingestion produces: DOUBLE
//! nulls = in-band NaN, STRING nulls = trailing null-row bitmap whose rows
//! point at a `""` placeholder ordinal.

use ferrodruid_query::{DruidQuery, QueryResult, execute_query};
use ferrodruid_segment::{SegmentData, SegmentDataBuilder};
use serde_json::json;

const INTERVAL: &str = "2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z";

fn base_millis() -> i64 {
    chrono::DateTime::parse_from_rfc3339("2013-01-01T00:00:00Z")
        .expect("base ts")
        .timestamp_millis()
}

/// 6-row segment covering every null shape the read path must handle.
///
/// | row | device_id | device2 | site | value (double) | v (long) |
/// |-----|-----------|---------|------|----------------|----------|
/// | 0   | "d1"      | "d1"    | "a"  | 10.0           | 1        |
/// | 1   | "d2"      | "d2"    | "a"  | 20.0           | 2        |
/// | 2   | null      | "d3"    | "b"  | null           | 3        |
/// | 3   | ""        | null    | "b"  | 5.0            | 4        |
/// | 4   | "d3"      | null    | "a"  | 2.5            | 5        |
/// | 5   | "d1"      | "d1"    | "b"  | 2.5            | 6        |
///
/// `device_id` carries a null AND a real `""` row (null ≠ "" distinctness),
/// `device2` carries nulls but NO real `""` (HLL over-count detection),
/// `site` is null-free (fast-path eligibility must be preserved).
fn build_null_segment() -> SegmentData {
    let base = base_millis();
    let s = |v: &str| Some(v.to_string());
    SegmentDataBuilder::new()
        .add_timestamp_column((0..6).map(|i| base + i * 1000).collect())
        .add_string_column_nullable(
            "device_id",
            vec![s("d1"), s("d2"), None, s(""), s("d3"), s("d1")],
        )
        .add_string_column_nullable(
            "device2",
            vec![s("d1"), s("d2"), s("d3"), None, None, s("d1")],
        )
        .add_string_column(
            "site",
            ["a", "a", "b", "b", "a", "b"]
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
        .add_double_column_nullable(
            "value",
            true,
            vec![
                Some(10.0),
                Some(20.0),
                None,
                Some(5.0),
                Some(2.5),
                Some(2.5),
            ],
        )
        .add_long_column("v", true, vec![1, 2, 3, 4, 5, 6])
        .build()
        .expect("segment builds")
}

fn run(query_json: &serde_json::Value, segment: &SegmentData) -> QueryResult {
    let query: DruidQuery =
        serde_json::from_value(query_json.clone()).expect("query JSON must parse");
    execute_query(&query, segment).expect("query must execute")
}

/// Single-bucket timeseries: return `result[field]`.
fn ts_field(result: &QueryResult, field: &str) -> serde_json::Value {
    let QueryResult::Timeseries(rows) = result else {
        panic!("expected Timeseries result, got {result:?}");
    };
    assert_eq!(rows.len(), 1, "expected a single all-granularity bucket");
    rows[0]
        .result
        .get(field)
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

/// GroupBy: collect `(dim value, event[field])` sorted by the dim's JSON text.
fn groupby_pairs(
    result: &QueryResult,
    dim: &str,
    field: &str,
) -> Vec<(serde_json::Value, serde_json::Value)> {
    let QueryResult::GroupBy(rows) = result else {
        panic!("expected GroupBy result, got {result:?}");
    };
    let mut out: Vec<(serde_json::Value, serde_json::Value)> = rows
        .iter()
        .map(|r| {
            (
                r.event.get(dim).cloned().unwrap_or(serde_json::Value::Null),
                r.event
                    .get(field)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            )
        })
        .collect();
    out.sort_by_key(|(k, _)| k.to_string());
    out
}

// ===========================================================================
// D1 — string-null rows must render as JSON null (not "")
// ===========================================================================

#[test]
fn scan_renders_string_null_as_json_null_distinct_from_empty() {
    let segment = build_null_segment();
    let query = json!({
        "queryType": "scan",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "columns": ["device_id"],
        "resultFormat": "list"
    });
    let QueryResult::Scan(scan) = run(&query, &segment) else {
        panic!("expected Scan result");
    };
    assert_eq!(scan.events.len(), 6);
    assert_eq!(
        scan.events[2].get("device_id"),
        Some(&json!(null)),
        "null string row must scan as JSON null (Druid 35 ground truth), not \"\""
    );
    assert_eq!(
        scan.events[3].get("device_id"),
        Some(&json!("")),
        "a real \"\" row must stay \"\" — null and empty string are distinct"
    );
    assert_eq!(scan.events[0].get("device_id"), Some(&json!("d1")));
}

#[test]
fn hll_count_distinct_skips_null_rows() {
    // device2 = d1, d2, d3, null, null, d1 → 3 distinct non-null values.
    // Pre-fix the two null rows materialised as "" (the dictionary
    // placeholder), so HLL counted a phantom 4th value.
    let segment = build_null_segment();
    let query = json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [
            {"type": "HLLSketchBuild", "name": "hll_dev", "fieldName": "device2"}
        ],
        "postAggregations": [
            {
                "type": "HLLSketchEstimate",
                "name": "distinct_devices",
                "field": {"type": "fieldAccess", "name": "fa", "fieldName": "hll_dev"},
                "round": true
            }
        ]
    });
    let result = run(&query, &segment);
    assert_eq!(
        ts_field(&result, "distinct_devices"),
        json!(3.0),
        "COUNT(DISTINCT device2) must not count null rows (Druid 35: 3)"
    );
}

#[test]
fn selector_null_filter_matches_exactly_the_null_rows() {
    let segment = build_null_segment();
    let count_with_filter = |filter: serde_json::Value| -> serde_json::Value {
        let query = json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "t"},
            "intervals": [INTERVAL],
            "granularity": "all",
            "filter": filter,
            "aggregations": [{"type": "count", "name": "c"}]
        });
        ts_field(&run(&query, &segment), "c")
    };
    assert_eq!(
        count_with_filter(json!({
            "type": "selector", "dimension": "device2", "value": null
        })),
        json!(2),
        "selector-null must match exactly the 2 null rows of device2"
    );
    assert_eq!(
        count_with_filter(json!({
            "type": "not",
            "field": {"type": "selector", "dimension": "device2", "value": null}
        })),
        json!(4),
        "not(selector null) must count exactly the 4 non-null rows"
    );
    // null and "" stay distinct on the filter path too: selector "" over
    // device_id matches only the real "" row (row 3), not the null row.
    assert_eq!(
        count_with_filter(json!({
            "type": "selector", "dimension": "device_id", "value": ""
        })),
        json!(1),
        "selector \"\" must match only the real empty-string row, not the null row"
    );
}

// ===========================================================================
// D2 — vectorized fast paths must not NaN-poison sums over nullable DOUBLEs
// ===========================================================================

#[test]
fn timeseries_vectorized_doublesum_skips_nan_nulls() {
    // No filter + granularity "all" + doubleSum over a Double column →
    // qualifies for try_vectorized_all. value = 10+20+null+5+2.5+2.5.
    let segment = build_null_segment();
    let query = json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [
            {"type": "doubleSum", "name": "s", "fieldName": "value"},
            {"type": "count", "name": "c"}
        ]
    });
    let result = run(&query, &segment);
    assert_eq!(
        ts_field(&result, "s"),
        json!(40.0),
        "SUM over a null-bearing double column must skip nulls (Druid: 40), not NaN-poison"
    );
    assert_eq!(ts_field(&result, "c"), json!(6));
}

#[test]
fn groupby_vectorized_doublesum_skips_nan_nulls() {
    // Null-free string dim (`site`) keeps the query on the vectorized dense
    // path; the nullable double agg column is where the NaN guard matters.
    // site=a rows {0,1,4}: 10+20+2.5 = 32.5; site=b rows {2,3,5}: null+5+2.5 = 7.5.
    let segment = build_null_segment();
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["site"],
        "aggregations": [{"type": "doubleSum", "name": "s", "fieldName": "value"}]
    });
    let result = run(&query, &segment);
    assert_eq!(
        groupby_pairs(&result, "site", "s"),
        vec![(json!("a"), json!(32.5)), (json!("b"), json!(7.5))],
        "mixed null groups must sum their non-null values (Druid: 7.5 for b)"
    );
}

#[test]
fn topn_vectorized_doublesum_skips_nan_nulls() {
    let segment = build_null_segment();
    let query = json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimension": "site",
        "metric": "s",
        "threshold": 10,
        "aggregations": [{"type": "doubleSum", "name": "s", "fieldName": "value"}]
    });
    let QueryResult::TopN(results) = run(&query, &segment) else {
        panic!("expected TopN result");
    };
    assert_eq!(results.len(), 1);
    let rows = &results[0].result;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("site"), Some(&json!("a")));
    assert_eq!(rows[0].get("s"), Some(&json!(32.5)));
    assert_eq!(rows[1].get("site"), Some(&json!("b")));
    assert_eq!(
        rows[1].get("s"),
        Some(&json!(7.5)),
        "topN vectorized doubleSum must skip NaN nulls"
    );
}

/// Parallel + L1-tiled dense groupBy path (>= 250K rows, sorted single
/// interval → interval pruning elides the per-row timestamp check).
#[test]
fn groupby_parallel_tiled_doublesum_skips_nan_nulls() {
    let (segment, expect_a, expect_b) = build_parallel_segment(true);
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["site"],
        "aggregations": [{"type": "doubleSum", "name": "s", "fieldName": "value"}]
    });
    let result = run(&query, &segment);
    assert_eq!(
        groupby_pairs(&result, "site", "s"),
        vec![(json!("a"), json!(expect_a)), (json!("b"), json!(expect_b))],
        "parallel tiled dense path must skip NaN nulls"
    );
}

/// Parallel dense groupBy path, per-row interval-check branch (two intervals
/// defeat `pruned_row_range`, forcing the `check_interval` loop).
#[test]
fn groupby_parallel_interval_check_doublesum_skips_nan_nulls() {
    let (segment, expect_a, expect_b) = build_parallel_segment(true);
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [
            "2013-01-01T00:00:00.000Z/2013-01-01T12:00:00.000Z",
            "2013-01-01T12:00:00.000Z/2013-01-02T00:00:00.000Z"
        ],
        "granularity": "all",
        "dimensions": ["site"],
        "aggregations": [{"type": "doubleSum", "name": "s", "fieldName": "value"}]
    });
    let result = run(&query, &segment);
    assert_eq!(
        groupby_pairs(&result, "site", "s"),
        vec![(json!("a"), json!(expect_a)), (json!("b"), json!(expect_b))],
        "parallel interval-check dense path must skip NaN nulls"
    );
}

/// 260K-row segment: alternating null-free `site` dim (a/b), nullable
/// `value` double (every 10th row null, else 1.5). All timestamps stay
/// inside [`INTERVAL`] (spread over ~1000ms steps would overflow the day —
/// use 200ms). Returns (segment, expected sum a, expected sum b); the sums
/// are multiples of 1.5 so f64 accumulation is exact in any order.
fn build_parallel_segment(sorted: bool) -> (SegmentData, f64, f64) {
    const N: usize = 260_000;
    let base = base_millis();
    let times: Vec<i64> = (0..N as i64).map(|i| base + i * 200).collect();
    assert!(sorted, "unsorted variant unused");
    let site: Vec<String> = (0..N)
        .map(|i| if i % 2 == 0 { "a" } else { "b" }.to_string())
        .collect();
    let value: Vec<Option<f64>> = (0..N)
        .map(|i| if i % 10 == 0 { None } else { Some(1.5) })
        .collect();
    let (mut expect_a, mut expect_b) = (0.0f64, 0.0f64);
    for (i, v) in value.iter().enumerate() {
        if let Some(x) = v {
            if i % 2 == 0 {
                expect_a += x;
            } else {
                expect_b += x;
            }
        }
    }
    let segment = SegmentDataBuilder::new()
        .add_timestamp_column(times)
        .add_string_column("site", site)
        .add_double_column_nullable("value", true, value)
        .build()
        .expect("segment builds");
    (segment, expect_a, expect_b)
}

// ===========================================================================
// D3 — null group keys: JSON null group, distinct from ""
// ===========================================================================

#[test]
fn groupby_null_string_dim_forms_distinct_null_group() {
    // device_id = d1, d2, null, "", d3, d1 → 5 groups. Pre-fix the dense
    // fast path keyed on raw ordinals, silently merging null into "".
    let segment = build_null_segment();
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["device_id"],
        "aggregations": [{"type": "count", "name": "c"}]
    });
    let result = run(&query, &segment);
    let pairs = groupby_pairs(&result, "device_id", "c");
    assert_eq!(
        pairs,
        vec![
            (json!(""), json!(1)),
            (json!("d1"), json!(2)),
            (json!("d2"), json!(1)),
            (json!("d3"), json!(1)),
            (json!(null), json!(1)),
        ],
        "null rows must form their own JSON-null group, distinct from the \"\" group"
    );
}

#[test]
fn topn_null_string_dim_forms_distinct_null_group() {
    let segment = build_null_segment();
    let query = json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimension": "device_id",
        "metric": "c",
        "threshold": 10,
        "aggregations": [{"type": "count", "name": "c"}]
    });
    let QueryResult::TopN(results) = run(&query, &segment) else {
        panic!("expected TopN result");
    };
    assert_eq!(results.len(), 1);
    let rows = &results[0].result;
    assert_eq!(
        rows.len(),
        5,
        "d1, d2, d3, \"\" and null are 5 distinct groups"
    );
    let count_of = |key: &serde_json::Value| -> Option<serde_json::Value> {
        rows.iter()
            .find(|r| r.get("device_id") == Some(key))
            .and_then(|r| r.get("c").cloned())
    };
    assert_eq!(count_of(&json!(null)), Some(json!(1)), "null group present");
    assert_eq!(count_of(&json!("")), Some(json!(1)), "\"\" group distinct");
    assert_eq!(count_of(&json!("d1")), Some(json!(2)));
}

// ===========================================================================
// Fast-path/general-path agreement + all-null-group SUM = null pinning
// ===========================================================================

/// A numeric bound filter must never match a null (NaN) double row — on the
/// row-map path NaN renders as JSON null and is rejected; the typed/compiled
/// filter paths must agree.
#[test]
fn numeric_bound_filter_excludes_null_double_rows() {
    let segment = build_null_segment();
    let query = json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "filter": {
            "type": "bound",
            "dimension": "value",
            "lower": "0",
            "ordering": "numeric"
        },
        "aggregations": [{"type": "count", "name": "c"}]
    });
    let result = run(&query, &segment);
    assert_eq!(
        ts_field(&result, "c"),
        json!(5),
        "bound(value >= 0) must exclude the null row: NaN never satisfies a bound"
    );
}

/// 3-row segment where `site` = a, a, b and `value` = 10, 20, null — the
/// b group has NO non-null input, so `SUM(value)` must be SQL null there.
fn build_all_null_group_segment() -> SegmentData {
    let base = base_millis();
    let s = |v: &str| Some(v.to_string());
    SegmentDataBuilder::new()
        .add_timestamp_column((0..3).map(|i| base + i * 1000).collect())
        .add_string_column_nullable("site", vec![s("a"), s("a"), s("b")])
        .add_double_column_nullable("value", true, vec![Some(10.0), Some(20.0), None])
        .build()
        .expect("segment builds")
}

/// Druid 35 ground truth (default null handling): `SUM(x)` over a group
/// whose inputs are ALL null is SQL `null`, not `0.0`. Fast path and
/// general path must both say null.
#[test]
fn all_null_group_doublesum_is_null_on_both_paths() {
    let segment = build_all_null_group_segment();
    // `site` is null-FREE in content but built via the nullable builder; a
    // null bitmap only exists when a None was ingested, so this still takes
    // the fast path. Force the general path with a filtered aggregator.
    let fast = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["site"],
        "aggregations": [{"type": "doubleSum", "name": "s", "fieldName": "value"}]
    });
    let mut general = fast.clone();
    general["aggregations"]
        .as_array_mut()
        .expect("aggregations array")
        .push(json!({
            "type": "filtered",
            "filter": {"type": "true"},
            "aggregator": {"type": "count", "name": "c"}
        }));
    let expected = vec![(json!("a"), json!(30.0)), (json!("b"), json!(null))];
    assert_eq!(
        groupby_pairs(&run(&fast, &segment), "site", "s"),
        expected,
        "fast path: all-null group SUM must be null (Druid 35 ground truth)"
    );
    assert_eq!(
        groupby_pairs(&run(&general, &segment), "site", "s"),
        expected,
        "general path: all-null group SUM must be null (Druid 35 ground truth)"
    );
}

/// Timeseries `try_vectorized_all` + the row-oriented path: a single-bucket
/// doubleSum whose matched rows are ALL null must emit null (count still
/// counts the rows — a bucket with rows exists, its sum has no input).
#[test]
fn timeseries_all_null_doublesum_is_null_on_both_paths() {
    let base = base_millis();
    let segment = SegmentDataBuilder::new()
        .add_timestamp_column((0..4).map(|i| base + i * 1000).collect())
        .add_double_column_nullable("value", true, vec![None, None, None, None])
        .build()
        .expect("segment builds");
    let fast = json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [
            {"type": "doubleSum", "name": "s", "fieldName": "value"},
            {"type": "count", "name": "c"}
        ]
    });
    // A filtered aggregator is not vectorizable → forces the row path.
    let mut general = fast.clone();
    general["aggregations"]
        .as_array_mut()
        .expect("aggregations array")
        .push(json!({
            "type": "filtered",
            "filter": {"type": "true"},
            "aggregator": {"type": "count", "name": "c2"}
        }));
    for (label, q) in [("fast", &fast), ("general", &general)] {
        let result = run(q, &segment);
        assert_eq!(
            ts_field(&result, "s"),
            json!(null),
            "{label} path: all-null timeseries doubleSum must be null"
        );
        assert_eq!(ts_field(&result, "c"), json!(4), "{label} path count");
    }
}

/// TopN vectorized + general path: the all-null group's SUM emits null.
/// Ranking treats the null metric as 0.0 on both paths (general path sorts
/// via `as_f64().unwrap_or(0.0)`), so `b` sorts after `a`.
#[test]
fn topn_all_null_group_doublesum_is_null() {
    let segment = build_all_null_group_segment();
    let query = json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimension": "site",
        "metric": "s",
        "threshold": 10,
        "aggregations": [{"type": "doubleSum", "name": "s", "fieldName": "value"}]
    });
    let QueryResult::TopN(results) = run(&query, &segment) else {
        panic!("expected TopN result");
    };
    assert_eq!(results.len(), 1);
    let rows = &results[0].result;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("site"), Some(&json!("a")));
    assert_eq!(rows[0].get("s"), Some(&json!(30.0)));
    assert_eq!(rows[1].get("site"), Some(&json!("b")));
    assert_eq!(
        rows[1].get("s"),
        Some(&json!(null)),
        "topN: all-null group SUM must be null (Druid 35 ground truth)"
    );
}

/// Parallel + L1-tiled dense groupBy path: an all-null group at parallel
/// scale (site `c` never has a non-null `value`) must emit null while the
/// mixed groups keep their sums.
#[test]
fn groupby_parallel_tiled_all_null_group_doublesum_is_null() {
    let (segment, expect_a, expect_b) = build_parallel_all_null_c_segment();
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["site"],
        "aggregations": [{"type": "doubleSum", "name": "s", "fieldName": "value"}]
    });
    let result = run(&query, &segment);
    assert_eq!(
        groupby_pairs(&result, "site", "s"),
        vec![
            (json!("a"), json!(expect_a)),
            (json!("b"), json!(expect_b)),
            (json!("c"), json!(null)),
        ],
        "parallel tiled dense path: all-null group SUM must be null"
    );
}

/// Parallel dense groupBy path, per-row interval-check branch (two intervals
/// defeat `pruned_row_range`): all-null group must emit null there too.
#[test]
fn groupby_parallel_interval_check_all_null_group_doublesum_is_null() {
    let (segment, expect_a, expect_b) = build_parallel_all_null_c_segment();
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [
            "2013-01-01T00:00:00.000Z/2013-01-01T12:00:00.000Z",
            "2013-01-01T12:00:00.000Z/2013-01-02T00:00:00.000Z"
        ],
        "granularity": "all",
        "dimensions": ["site"],
        "aggregations": [{"type": "doubleSum", "name": "s", "fieldName": "value"}]
    });
    let result = run(&query, &segment);
    assert_eq!(
        groupby_pairs(&result, "site", "s"),
        vec![
            (json!("a"), json!(expect_a)),
            (json!("b"), json!(expect_b)),
            (json!("c"), json!(null)),
        ],
        "parallel interval-check dense path: all-null group SUM must be null"
    );
}

/// longSum over a null-bearing numeric column: a null-bearing "long" input
/// is stored as DOUBLE with NaN nulls (no in-band i64 null marker), so the
/// query falls back to the general row path — the aggregator itself must
/// yield null for the all-null group.
#[test]
fn all_null_group_longsum_is_null_general_path() {
    let base = base_millis();
    let s = |v: &str| Some(v.to_string());
    let segment = SegmentDataBuilder::new()
        .add_timestamp_column((0..3).map(|i| base + i * 1000).collect())
        .add_string_column_nullable("site", vec![s("a"), s("a"), s("b")])
        .add_long_column_nullable("v", true, vec![Some(10), Some(20), None])
        .build()
        .expect("segment builds");
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["site"],
        "aggregations": [{"type": "longSum", "name": "s", "fieldName": "v"}]
    });
    let result = run(&query, &segment);
    assert_eq!(
        groupby_pairs(&result, "site", "s"),
        vec![(json!("a"), json!(30)), (json!("b"), json!(null))],
        "all-null group longSum must be null (Druid 35 ground truth)"
    );
}

/// Executor-level mirror of the SQL-planned `SELECT site, SUM(x)/COUNT(*)`
/// shape (the exact native query `ferrodruid-sql` emits: groupBy + an
/// arithmetic post-agg over fieldAccess(sum) / fieldAccess(count)). Druid:
/// arithmetic over a null aggregate is null for the all-null group.
#[test]
fn sql_planned_sum_div_count_star_is_null_for_all_null_group() {
    let segment = build_all_null_group_segment();
    let query = json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["site"],
        "aggregations": [
            {"type": "doubleSum", "name": "s", "fieldName": "value"},
            {"type": "count", "name": "cnt"}
        ],
        "postAggregations": [
            {"type": "arithmetic", "name": "ratio", "fn": "/", "fields": [
                {"type": "fieldAccess", "name": "fs", "fieldName": "s"},
                {"type": "fieldAccess", "name": "fc", "fieldName": "cnt"}
            ]}
        ]
    });
    let result = run(&query, &segment);
    assert_eq!(
        groupby_pairs(&result, "site", "ratio"),
        vec![(json!("a"), json!(15.0)), (json!("b"), json!(null))],
        "SUM(x)/COUNT(*) over an all-null group must be null (Druid arithmetic over null)"
    );
}

/// 260K-row segment for the parallel all-null-group tests: `site` cycles
/// a/b/c; `value` is 1.5 on a/b rows (every 10th a/b row null) and ALWAYS
/// null on c rows. Returns (segment, expected sum a, expected sum b).
fn build_parallel_all_null_c_segment() -> (SegmentData, f64, f64) {
    const N: usize = 260_000;
    let base = base_millis();
    let times: Vec<i64> = (0..N as i64).map(|i| base + i * 200).collect();
    let site: Vec<String> = (0..N)
        .map(|i| {
            match i % 3 {
                0 => "a",
                1 => "b",
                _ => "c",
            }
            .to_string()
        })
        .collect();
    let value: Vec<Option<f64>> = (0..N)
        .map(|i| {
            if i % 3 == 2 || i % 10 == 0 {
                None
            } else {
                Some(1.5)
            }
        })
        .collect();
    let (mut expect_a, mut expect_b) = (0.0f64, 0.0f64);
    for (i, v) in value.iter().enumerate() {
        if let Some(x) = v {
            match i % 3 {
                0 => expect_a += x,
                1 => expect_b += x,
                _ => unreachable!("c rows are always null"),
            }
        }
    }
    let segment = SegmentDataBuilder::new()
        .add_timestamp_column(times)
        .add_string_column("site", site)
        .add_double_column_nullable("value", true, value)
        .build()
        .expect("segment builds");
    (segment, expect_a, expect_b)
}
