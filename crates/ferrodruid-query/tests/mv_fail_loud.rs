// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! compat-11 multi-value (MV) fail-loud tests for the not-yet-supported
//! MV contexts.
//!
//! A genuine `StringMulti` column is element-wise data.  The contexts
//! below (aggregating over an MV field, an expression / virtual column
//! referencing an MV column) have no element-wise implementation yet —
//! pre-fix they silently STRINGIFIED the row's array into JSON text
//! (`["a","b"]` → the scalar `"[\"a\",\"b\"]"`) and computed corrupt
//! results.  Until element-wise MV support lands they must FAIL LOUD
//! with a clear [`DruidError::Query`] naming the column, never corrupt.
//!
//! The WORKING MV paths (groupBy/topN explosion, selector/IN filters,
//! scan rendering, metadata) are covered by `mv_druid_oracle.rs` and are
//! intentionally NOT touched by these guards.

use ferrodruid_ingest_batch::BatchIngester;
use ferrodruid_ingest_batch::{DimensionSchema, DimensionType};
use ferrodruid_query::{
    GroupByQuery, ScanQuery, SearchQuery, SortDirection, TimeseriesQuery, TopNQuery,
    WindowFunctionKind, WindowOrderBy, WindowQuery, WindowSpec,
};
use ferrodruid_segment::SegmentData;

/// The shared MV fixture (same rows as `mv_druid_oracle.rs`).
fn ingest_fixture() -> SegmentData {
    let ingester = BatchIngester::with_schemas(
        "mv_compat".into(),
        "__time".into(),
        vec![
            DimensionSchema::string("tags"),
            DimensionSchema::new("m", DimensionType::Long),
        ],
        vec![],
    );
    let rows = vec![
        serde_json::json!({"__time": "2024-01-01T00:00:00Z", "tags": ["a", "b"], "m": 10}),
        serde_json::json!({"__time": "2024-01-01T01:00:00Z", "tags": "a", "m": 20}),
        serde_json::json!({"__time": "2024-01-01T02:00:00Z", "tags": [], "m": 30}),
        serde_json::json!({"__time": "2024-01-01T03:00:00Z", "tags": ["c", "a"], "m": 40}),
    ];
    ingester.ingest(rows).expect("ingest fixture").segment_data
}

const INTERVAL: &str = "2024-01-01T00:00:00Z/2024-01-02T00:00:00Z";

/// Assert the error is loud AND names both the MV column and the context.
fn assert_mv_error(err: &ferrodruid_common::error::DruidError, context_word: &str) {
    let msg = err.to_string();
    assert!(
        msg.contains("multi-value"),
        "error must mention multi-value: {msg}"
    );
    assert!(msg.contains("tags"), "error must name the column: {msg}");
    assert!(
        msg.contains(context_word),
        "error must name the context ({context_word}): {msg}"
    );
}

/// A `cardinality` aggregator whose field is the MV column must ERROR —
/// pre-fix it hashed each row's whole array as ONE value ("[\"a\",\"b\"]"),
/// yielding a corrupt distinct count instead of element-wise semantics.
#[test]
fn mv_cardinality_aggregation_fails_loud() {
    let segment = ingest_fixture();
    let ts: TimeseriesQuery = serde_json::from_value(serde_json::json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [
            {"type": "cardinality", "name": "distinct_tags", "fields": ["tags"]}
        ]
    }))
    .expect("parse timeseries");
    let err = ts
        .execute(&segment)
        .expect_err("cardinality over an MV column must fail loud");
    assert_mv_error(&err, "aggregation");
}

/// A numeric aggregator (longSum) over the MV column must ERROR — pre-fix
/// the array was coerced as one scalar and silently summed as garbage.
#[test]
fn mv_numeric_aggregation_fails_loud() {
    let segment = ingest_fixture();
    let q: GroupByQuery = serde_json::from_value(serde_json::json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{
            "type": "default",
            "dimension": "m",
            "outputName": "m",
            "outputType": "STRING"
        }],
        "aggregations": [
            {"type": "longSum", "name": "sum_tags", "fieldName": "tags"}
        ]
    }))
    .expect("parse groupBy");
    let err = q
        .execute(&segment)
        .expect_err("longSum over an MV column must fail loud");
    assert_mv_error(&err, "aggregation");
}

/// A `filtered`-wrapped aggregator over the MV column must ERROR too —
/// the guard has to see through the wrapper to the inner field.
#[test]
fn mv_filtered_wrapped_aggregation_fails_loud() {
    let segment = ingest_fixture();
    let ts: TimeseriesQuery = serde_json::from_value(serde_json::json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [{
            "type": "filtered",
            "filter": {"type": "selector", "dimension": "m", "value": "10"},
            "aggregator": {"type": "stringFirst", "name": "first_tag", "fieldName": "tags"}
        }]
    }))
    .expect("parse timeseries");
    let err = ts
        .execute(&segment)
        .expect_err("filtered aggregator over an MV column must fail loud");
    assert_mv_error(&err, "aggregation");
}

/// A first/last aggregator whose `timeColumn` is the MV column must ERROR
/// — pre-fix the array (or scalar-string) "timestamp" read as `None` via
/// `as_i64` and was silently substituted with `0`, giving
/// insertion-order-dependent first/last results.  Covers the two-argument
/// forms across the first/last family, including a `filtered` wrapper.
#[test]
fn mv_first_last_time_column_fails_loud() {
    let segment = ingest_fixture();
    let aggs = [
        serde_json::json!(
            {"type": "longFirst", "name": "f", "fieldName": "m", "timeColumn": "tags"}
        ),
        serde_json::json!(
            {"type": "stringFirst", "name": "f", "fieldName": "m", "timeColumn": "tags"}
        ),
        serde_json::json!(
            {"type": "doubleLast", "name": "f", "fieldName": "m", "timeColumn": "tags"}
        ),
        serde_json::json!({
            "type": "filtered",
            "filter": {"type": "selector", "dimension": "m", "value": "10"},
            "aggregator":
                {"type": "longLast", "name": "f", "fieldName": "m", "timeColumn": "tags"}
        }),
    ];
    for agg in aggs {
        let ts: TimeseriesQuery = serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "mv_compat"},
            "intervals": [INTERVAL],
            "granularity": "all",
            "aggregations": [agg.clone()]
        }))
        .expect("parse timeseries");
        let err = ts
            .execute(&segment)
            .expect_err("first/last timeColumn over MV must fail loud");
        assert_mv_error(&err, "aggregation");
    }
}

/// Guard must NOT over-fire: a first/last with the default `__time`
/// ordering, or with an explicit SCALAR `timeColumn`, still executes.
#[test]
fn mv_first_last_scalar_time_column_still_works() {
    let segment = ingest_fixture();
    let run = |agg: serde_json::Value| {
        let ts: TimeseriesQuery = serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "mv_compat"},
            "intervals": [INTERVAL],
            "granularity": "all",
            "aggregations": [agg]
        }))
        .expect("parse timeseries");
        ts.execute(&segment)
            .expect("scalar/default timeColumn must not be rejected")
    };
    // Default __time ordering (no timeColumn) — executes without error
    // (the insertion-order fallback semantics of the one-arg path are a
    // pre-existing behaviour, out of scope here).
    let result = run(serde_json::json!(
        {"type": "longFirst", "name": "f", "fieldName": "m"}
    ));
    assert!(
        result[0].result.get("f").is_some_and(|v| v.is_i64()),
        "default __time longFirst must produce a value: {result:?}"
    );
    // Explicit SCALAR (Long) timeColumn — the two-argument path orders by
    // the real column values: min(m) = 10 sits on the m=10 row, so
    // longFirst(m, timeColumn=m) = 10.
    let result = run(serde_json::json!(
        {"type": "longFirst", "name": "f", "fieldName": "m", "timeColumn": "m"}
    ));
    assert_eq!(result[0].result.get("f"), Some(&serde_json::json!(10)));
}

/// A virtual-column expression referencing the MV column must ERROR —
/// pre-fix `concat(tags, '_x')` stringified row 0 into `["a","b"]_x`.
#[test]
fn mv_virtual_column_expression_fails_loud() {
    let segment = ingest_fixture();
    let q: ScanQuery = serde_json::from_value(serde_json::json!({
        "queryType": "scan",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "virtualColumns": [
            {"type": "expression", "name": "t2", "expression": "concat(tags, '_x')"}
        ],
        "columns": ["__time", "t2", "m"],
        "resultFormat": "list"
    }))
    .expect("parse scan");
    let err = q
        .execute(&segment)
        .expect_err("virtual column over an MV column must fail loud");
    assert_mv_error(&err, "expression");
}

/// Same guard on the aggregating executors: a topN whose virtual column
/// references the MV column must ERROR (not feed stringified rows).
#[test]
fn mv_virtual_column_in_topn_fails_loud() {
    let segment = ingest_fixture();
    let q: TopNQuery = serde_json::from_value(serde_json::json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "virtualColumns": [
            {"type": "expression", "name": "t2", "expression": "concat(tags, '_x')"}
        ],
        "dimension": {
            "type": "default",
            "dimension": "t2",
            "outputName": "t2",
            "outputType": "STRING"
        },
        "threshold": 10,
        "metric": "cnt",
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse topN");
    let err = q
        .execute(&segment)
        .expect_err("topN virtual column over an MV column must fail loud");
    assert_mv_error(&err, "expression");
}

// ---------------------------------------------------------------------------
// compat-11 R2: the comprehensive PLAN-TIME filter guard.
//
// Element-aware filters (selector, in, bound, range, like, regex, search,
// bloomFilter, mvFilterOnly/mvFilterNone, null) stay fully working on MV
// columns.  Every OTHER filter that reads an MV column (columnComparison,
// expression, interval, any future variant) used to silently compare the
// row's stringified JSON-array text — the guard now rejects them loudly
// once per query, in EVERY filter-applying executor (timeseries, topN,
// groupBy, scan, search) and inside `filtered`-aggregator wrappers.
// ---------------------------------------------------------------------------

/// A `columnComparison` filter listing the MV column must ERROR — pre-fix
/// it compared the whole JSON-array text against the other column instead
/// of Druid's any-shared-element overlap semantics.
#[test]
fn mv_column_comparison_filter_fails_loud() {
    let segment = ingest_fixture();
    let q: ScanQuery = serde_json::from_value(serde_json::json!({
        "queryType": "scan",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "filter": {"type": "columnComparison", "dimensions": ["tags", "m"]},
        "columns": ["__time", "tags", "m"],
        "resultFormat": "list"
    }))
    .expect("parse scan");
    let err = q
        .execute(&segment)
        .expect_err("columnComparison over an MV column must fail loud");
    assert_mv_error(&err, "columnComparison");
}

/// An `expression` filter referencing the MV column must ERROR — pre-fix
/// `tags == 'a'` compared the JSON text `["a","b"]` to `"a"` instead of
/// Druid's element-wise scalar-expression application.
#[test]
fn mv_expression_filter_fails_loud() {
    let segment = ingest_fixture();
    let ts: TimeseriesQuery = serde_json::from_value(serde_json::json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "filter": {"type": "expression", "expression": "tags == 'a'"},
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse timeseries");
    let err = ts
        .execute(&segment)
        .expect_err("expression filter over an MV column must fail loud");
    assert_mv_error(&err, "expression");
}

/// The guard is wired into groupBy too (not just timeseries/scan).
#[test]
fn mv_expression_filter_in_groupby_fails_loud() {
    let segment = ingest_fixture();
    let q: GroupByQuery = serde_json::from_value(serde_json::json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{
            "type": "default",
            "dimension": "m",
            "outputName": "m",
            "outputType": "STRING"
        }],
        "filter": {"type": "expression", "expression": "tags == 'a'"},
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse groupBy");
    let err = q
        .execute(&segment)
        .expect_err("expression filter over an MV column must fail loud in groupBy");
    assert_mv_error(&err, "expression");
}

/// The guard is wired into topN too — including filters nested under
/// boolean combinators (`and`/`or`/`not` recursion).
#[test]
fn mv_column_comparison_filter_in_topn_fails_loud() {
    let segment = ingest_fixture();
    let q: TopNQuery = serde_json::from_value(serde_json::json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimension": {
            "type": "default",
            "dimension": "m",
            "outputName": "m",
            "outputType": "STRING"
        },
        "threshold": 10,
        "metric": "cnt",
        "filter": {"type": "and", "fields": [
            {"type": "selector", "dimension": "m", "value": "10"},
            {"type": "columnComparison", "dimensions": ["tags", "m"]}
        ]},
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse topN");
    let err = q
        .execute(&segment)
        .expect_err("nested columnComparison over an MV column must fail loud in topN");
    assert_mv_error(&err, "columnComparison");
}

/// The guard is wired into the search executor too.
#[test]
fn mv_expression_filter_in_search_fails_loud() {
    let segment = ingest_fixture();
    let q: SearchQuery = serde_json::from_value(serde_json::json!({
        "queryType": "search",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "filter": {"type": "expression", "expression": "tags == 'a'"},
        "query": {"type": "contains", "value": "a"}
    }))
    .expect("parse search");
    let err = q
        .execute(&segment)
        .expect_err("expression filter over an MV column must fail loud in search");
    assert_mv_error(&err, "expression");
}

/// A `filtered` aggregator whose WRAPPER FILTER (not its field) targets
/// the MV column with a non-element-aware filter must ERROR too — that
/// filter runs through the same row-map evaluation.
#[test]
fn mv_filtered_aggregator_wrapper_filter_fails_loud() {
    let segment = ingest_fixture();
    let ts: TimeseriesQuery = serde_json::from_value(serde_json::json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [{
            "type": "filtered",
            "filter": {"type": "columnComparison", "dimensions": ["tags", "m"]},
            "aggregator": {"type": "longSum", "name": "sum_m", "fieldName": "m"}
        }]
    }))
    .expect("parse timeseries");
    let err = ts
        .execute(&segment)
        .expect_err("filtered-aggregator wrapper filter over an MV column must fail loud");
    assert_mv_error(&err, "columnComparison");
}

/// No-over-fire: element-aware filters on the MV column must NOT be
/// rejected by the plan-time guard — a lexicographic bound (any element
/// >= "b") keeps working end-to-end (rows 0 `["a","b"]` and 3 `["c","a"]`).
#[test]
fn mv_supported_filters_not_rejected_by_guard() {
    let segment = ingest_fixture();
    let q: ScanQuery = serde_json::from_value(serde_json::json!({
        "queryType": "scan",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "filter": {"type": "bound", "dimension": "tags", "lower": "b"},
        "columns": ["__time", "tags", "m"],
        "resultFormat": "list"
    }))
    .expect("parse scan");
    let res = q
        .execute(&segment)
        .expect("bound filter on an MV column is element-aware, guard must not fire");
    let ms: Vec<i64> = res
        .events
        .iter()
        .map(|e| e.get("m").and_then(|v| v.as_i64()).expect("m"))
        .collect();
    assert_eq!(ms, vec![10, 40], "rows 0 and 3 have an element >= \"b\"");
}

// ---------------------------------------------------------------------------
// compat-11 R3: window PARTITION BY / ORDER BY keys over an MV column.
//
// The window executor builds partition and rank-tie keys by STRINGIFYING
// each row's value — pre-fix an MV row's `["a","b"]` array became the
// JSON-text key `"[\"a\",\"b\"]"` while a 1-element row keyed as its
// scalar, silently corrupting partitions and ranks (no explosion, no
// guard).  Until element-wise MV windowing lands, any window key (or the
// outer post-window ORDER BY, or a window aggregate input) naming an MV
// column must FAIL LOUD at plan time.
// ---------------------------------------------------------------------------

/// Build the inner scan the window tests wrap (all rows, no filter).
fn window_inner_scan() -> ScanQuery {
    ScanQuery {
        data_source: ferrodruid_common::types::DataSource::Table {
            name: "mv_compat".into(),
        },
        intervals: vec![INTERVAL.into()],
        filter: None,
        virtual_columns: None,
        columns: Some(vec!["__time".into(), "tags".into(), "m".into()]),
        limit: None,
        offset: None,
        order: Some("none".into()),
        result_format: None,
        context: None,
    }
}

/// `ROW_NUMBER() OVER (PARTITION BY tags)` must ERROR — pre-fix the
/// partition key was the row's stringified JSON array.
#[test]
fn mv_window_partition_by_fails_loud() {
    let segment = ingest_fixture();
    let q = WindowQuery {
        inner: window_inner_scan(),
        windows: vec![WindowSpec {
            output_name: "rn".into(),
            function: WindowFunctionKind::RowNumber,
            partition_by: vec!["tags".into()],
            order_by: vec![WindowOrderBy {
                column: "m".into(),
                direction: SortDirection::Ascending,
            }],
            frame: None,
        }],
        post_order_by: vec![],
        post_limit: None,
        context: None,
    };
    let err = q
        .execute(&segment)
        .expect_err("window PARTITION BY over an MV column must fail loud");
    assert_mv_error(&err, "PARTITION BY");
}

/// `RANK() OVER (ORDER BY tags)` must ERROR — pre-fix the rank-tie key
/// was the stringified array and the sort compared JSON-array text.
#[test]
fn mv_window_order_by_fails_loud() {
    let segment = ingest_fixture();
    let q = WindowQuery {
        inner: window_inner_scan(),
        windows: vec![WindowSpec {
            output_name: "rk".into(),
            function: WindowFunctionKind::Rank,
            partition_by: vec![],
            order_by: vec![WindowOrderBy {
                column: "tags".into(),
                direction: SortDirection::Ascending,
            }],
            frame: None,
        }],
        post_order_by: vec![],
        post_limit: None,
        context: None,
    };
    let err = q
        .execute(&segment)
        .expect_err("window ORDER BY over an MV column must fail loud");
    assert_mv_error(&err, "ORDER BY");
}

/// The outer (post-window) SQL ORDER BY is the same scalar-key sort and
/// must be guarded too.
#[test]
fn mv_window_outer_order_by_fails_loud() {
    let segment = ingest_fixture();
    let q = WindowQuery {
        inner: window_inner_scan(),
        windows: vec![WindowSpec {
            output_name: "rn".into(),
            function: WindowFunctionKind::RowNumber,
            partition_by: vec![],
            order_by: vec![WindowOrderBy {
                column: "m".into(),
                direction: SortDirection::Ascending,
            }],
            frame: None,
        }],
        post_order_by: vec![WindowOrderBy {
            column: "tags".into(),
            direction: SortDirection::Ascending,
        }],
        post_limit: None,
        context: None,
    };
    let err = q
        .execute(&segment)
        .expect_err("outer ORDER BY over an MV column must fail loud");
    assert_mv_error(&err, "ORDER BY");
}

/// A window AGGREGATE whose input column is the MV column must ERROR —
/// pre-fix `SUM(tags)` silently skipped every array row (`as_f64` =
/// None) and emitted NULL/garbage instead of element-wise semantics
/// (same species as the R1 aggregation guard).
#[test]
fn mv_window_aggregate_input_fails_loud() {
    let segment = ingest_fixture();
    let q = WindowQuery {
        inner: window_inner_scan(),
        windows: vec![WindowSpec {
            output_name: "s".into(),
            function: WindowFunctionKind::Sum {
                column: "tags".into(),
            },
            partition_by: vec![],
            order_by: vec![],
            frame: None,
        }],
        post_order_by: vec![],
        post_limit: None,
        context: None,
    };
    let err = q
        .execute(&segment)
        .expect_err("window aggregate over an MV column must fail loud");
    assert_mv_error(&err, "window function");
}

/// No-over-fire: a window over SCALAR keys keeps working on a segment
/// that merely CONTAINS an MV column (rendered arrays pass through the
/// scan untouched).
#[test]
fn mv_window_over_scalar_keys_still_works() {
    let segment = ingest_fixture();
    let q = WindowQuery {
        inner: window_inner_scan(),
        windows: vec![WindowSpec {
            output_name: "rn".into(),
            function: WindowFunctionKind::RowNumber,
            partition_by: vec![],
            order_by: vec![WindowOrderBy {
                column: "m".into(),
                direction: SortDirection::Ascending,
            }],
            frame: None,
        }],
        post_order_by: vec![],
        post_limit: None,
        context: None,
    };
    let res = q
        .execute(&segment)
        .expect("scalar-key window on an MV segment must not over-fire");
    assert_eq!(res.events.len(), 4);
    // m ascending: 10, 20, 30, 40 → rn 1..4.
    let rn_by_m: Vec<(i64, i64)> = res
        .events
        .iter()
        .map(|e| {
            (
                e.get("m").and_then(|v| v.as_i64()).expect("m"),
                e.get("rn").and_then(|v| v.as_i64()).expect("rn"),
            )
        })
        .collect();
    for (m, rn) in rn_by_m {
        assert_eq!(rn, m / 10, "ROW_NUMBER over m must be m/10");
    }
}

// ---------------------------------------------------------------------------
// compat-11 R3: DimensionSpec outputType / extractionFn coercion over an
// MV column.
//
// The explosion path groups an MV row per ELEMENT — but it ignores a
// non-STRING `outputType` (Druid coerces each element, so `["01"]` and
// `["1"]` both become the numeric group `1`; pre-fix they stayed two
// distinct string groups) and the per-element extractionFn behaviour is
// unverified against the Druid oracle.  Both coercions now FAIL LOUD at
// plan time; the plain-STRING explosion stays untouched.
// ---------------------------------------------------------------------------

/// groupBy with `outputType: "LONG"` over the MV column must ERROR.
#[test]
fn mv_groupby_output_type_long_fails_loud() {
    let segment = ingest_fixture();
    let q: GroupByQuery = serde_json::from_value(serde_json::json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{
            "type": "default",
            "dimension": "tags",
            "outputName": "tags",
            "outputType": "LONG"
        }],
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse groupBy");
    let err = q
        .execute(&segment)
        .expect_err("groupBy outputType LONG over an MV column must fail loud");
    assert_mv_error(&err, "outputType");
}

/// topN with `outputType: "LONG"` over the MV column must ERROR.
#[test]
fn mv_topn_output_type_long_fails_loud() {
    let segment = ingest_fixture();
    let q: TopNQuery = serde_json::from_value(serde_json::json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimension": {
            "type": "default",
            "dimension": "tags",
            "outputName": "tags",
            "outputType": "LONG"
        },
        "threshold": 10,
        "metric": "cnt",
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse topN");
    let err = q
        .execute(&segment)
        .expect_err("topN outputType LONG over an MV column must fail loud");
    assert_mv_error(&err, "outputType");
}

/// groupBy with an extractionFn over the MV column must ERROR (element-
/// wise extraction is applied but unverified — a follow-on, not a silent
/// maybe).
#[test]
fn mv_groupby_extraction_fn_fails_loud() {
    let segment = ingest_fixture();
    let q: GroupByQuery = serde_json::from_value(serde_json::json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{
            "type": "extraction",
            "dimension": "tags",
            "outputName": "tags_len",
            "extractionFn": {"type": "strlen"}
        }],
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse groupBy");
    let err = q
        .execute(&segment)
        .expect_err("extractionFn over an MV column must fail loud");
    assert_mv_error(&err, "extractionFn");
}

/// No-over-fire: the plain-STRING MV grouping (explicit outputType
/// STRING, no extractionFn) keeps its oracle-verified explosion.
#[test]
fn mv_groupby_plain_string_still_explodes() {
    let segment = ingest_fixture();
    let q: GroupByQuery = serde_json::from_value(serde_json::json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{
            "type": "default",
            "dimension": "tags",
            "outputName": "tags",
            "outputType": "STRING"
        }],
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse groupBy");
    let res = q.execute(&segment).expect("plain-STRING MV grouping works");
    // Oracle explosion expectation: a→3 rows, b→1, c→1, null (empty)→1.
    let mut groups: Vec<(String, i64)> = res
        .iter()
        .map(|r| {
            let tag = match r.event.get("tags") {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(serde_json::Value::Null) | None => "<null>".to_string(),
                other => panic!("exploded group key must be scalar, got {other:?}"),
            };
            let cnt = r.event.get("cnt").and_then(|v| v.as_i64()).expect("cnt");
            (tag, cnt)
        })
        .collect();
    groups.sort();
    assert_eq!(
        groups,
        vec![
            ("<null>".to_string(), 1),
            ("a".to_string(), 3),
            ("b".to_string(), 1),
            ("c".to_string(), 1),
        ],
        "plain-STRING explosion must stay byte-identical"
    );
}

/// No-over-fire (sweep): a `listFiltered` wrapper whose delegate is a
/// plain-STRING Default spec keeps Druid's element-filtering on MV — and
/// never emits a JSON-array-text key.
#[test]
fn mv_groupby_list_filtered_wrapper_still_element_filters() {
    let segment = ingest_fixture();
    let q: GroupByQuery = serde_json::from_value(serde_json::json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{
            "type": "listFiltered",
            "delegate": {
                "type": "default",
                "dimension": "tags",
                "outputName": "tags",
                "outputType": "STRING"
            },
            "values": ["a"],
            "isWhitelist": true
        }],
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse groupBy");
    let res = q
        .execute(&segment)
        .expect("listFiltered wrapper over MV is element filtering, guard must not fire");
    // Element filtering: rows 0 (["a","b"]), 1 ("a"), 3 (["c","a"]) each
    // contribute exactly their "a" element.
    let a_count: i64 = res
        .iter()
        .filter(|r| r.event.get("tags") == Some(&serde_json::json!("a")))
        .map(|r| r.event.get("cnt").and_then(|v| v.as_i64()).expect("cnt"))
        .sum();
    assert_eq!(a_count, 3, "whitelisted element `a` groups 3 rows");
    for r in res {
        if let Some(serde_json::Value::String(s)) = r.event.get("tags") {
            assert!(
                !s.starts_with('['),
                "no JSON-array-text key may leak: {s:?}"
            );
        }
    }
}

/// Sweep pin: `subtotalsSpec` groupings over the MV dim EXPLODE per
/// subset (and the grand-total subset aggregates whole rows) — no
/// scalarized keys.
#[test]
fn mv_groupby_subtotals_explode_per_grouping() {
    let segment = ingest_fixture();
    let q: GroupByQuery = serde_json::from_value(serde_json::json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{
            "type": "default",
            "dimension": "tags",
            "outputName": "tags",
            "outputType": "STRING"
        }],
        "subtotalsSpec": [["tags"], []],
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse groupBy");
    let res = q.execute(&segment).expect("subtotals over MV works");
    // The ["tags"] grouping explodes into 4 groups; [] adds the grand
    // total (4 rows, dimension nulled).
    assert_eq!(res.len(), 5, "4 exploded groups + 1 grand total");
    let grand_total: Vec<i64> = res
        .iter()
        .filter(|r| matches!(r.event.get("tags"), Some(serde_json::Value::Null)))
        .filter_map(|r| r.event.get("cnt").and_then(|v| v.as_i64()))
        .collect();
    assert!(
        grand_total.contains(&4),
        "grand-total subset counts whole rows (4), got {grand_total:?}"
    );
    for r in &res {
        if let Some(serde_json::Value::String(s)) = r.event.get("tags") {
            assert!(!s.starts_with('['), "no JSON-array-text key: {s:?}");
        }
    }
}

/// Sweep pin: `limitSpec` ordering on the MV grouping dim orders the
/// EXPLODED scalar keys (never JSON-array text).
#[test]
fn mv_groupby_limit_spec_orders_exploded_keys() {
    let segment = ingest_fixture();
    let q: GroupByQuery = serde_json::from_value(serde_json::json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{
            "type": "default",
            "dimension": "tags",
            "outputName": "tags",
            "outputType": "STRING"
        }],
        "limitSpec": {
            "type": "default",
            "limit": 2,
            "columns": [{"dimension": "tags", "direction": "descending"}]
        },
        "aggregations": [{"type": "count", "name": "cnt"}]
    }))
    .expect("parse groupBy");
    let res = q.execute(&segment).expect("limitSpec over MV grouping");
    let keys: Vec<Option<String>> = res
        .iter()
        .map(|r| match r.event.get("tags") {
            Some(serde_json::Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        keys,
        vec![Some("c".to_string()), Some("b".to_string())],
        "descending over exploded scalar keys: c, b"
    );
}

/// A virtual column NOT referencing the MV column stays fully working on
/// an MV segment (the guard must not over-fire on mere MV presence).
#[test]
fn mv_unrelated_virtual_column_still_works() {
    let segment = ingest_fixture();
    let q: ScanQuery = serde_json::from_value(serde_json::json!({
        "queryType": "scan",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "virtualColumns": [
            {"type": "expression", "name": "m2", "expression": "m * 2"}
        ],
        "columns": ["__time", "tags", "m2"],
        "resultFormat": "list"
    }))
    .expect("parse scan");
    let res = q.execute(&segment).expect("unrelated virtual column works");
    assert_eq!(res.events.len(), 4);
    assert_eq!(
        res.events[0].get("m2"),
        Some(&serde_json::json!(20)),
        "m2 computed"
    );
}
