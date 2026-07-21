// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid Native Query JSON spec compatibility tests.
//!
//! These tests verify that FerroDruid correctly parses, re-serializes, and
//! executes all Druid Native Query JSON formats documented at druid.apache.org.

use std::collections::HashMap;

use ferrodruid_bitmap::DruidBitmap;
use ferrodruid_dict::FrontCodedDictionary;
use ferrodruid_query::{DruidQuery, QueryResult, execute_query};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::{ColumnData, StringColumnData};
use serde_json::json;

// ===========================================================================
// Test segment builder
// ===========================================================================

/// Build a "wikipedia"-style test segment with 12 rows.
///
/// Dimensions: page (string), user (string), channel (string), language (string)
/// Metrics: added (double), deleted (double), delta (double)
/// Timestamps: 2013-01-01 hourly (4 hours x 3 rows per hour)
fn build_wikipedia_segment() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2013-01-01T00:00:00Z")
        .unwrap()
        .timestamp_millis();
    let hour = 3_600_000i64;

    // 12 rows: 3 rows at hour 0, 3 at hour 1, 3 at hour 2, 3 at hour 3
    let timestamps: Vec<i64> = (0..12).map(|i| base + (i / 3) * hour).collect();

    // page dimension: 4 unique pages
    let page_values = vec![
        "Main_Page",
        "Talk:Main_Page",
        "Wikipedia:Help",
        "Main_Page",
        "Talk:Main_Page",
        "Category:Foo",
        "Main_Page",
        "Wikipedia:Help",
        "Talk:Main_Page",
        "Category:Foo",
        "Main_Page",
        "Wikipedia:Help",
    ];
    let page_dict = FrontCodedDictionary::from_sorted(vec![
        "Category:Foo".to_string(),
        "Main_Page".to_string(),
        "Talk:Main_Page".to_string(),
        "Wikipedia:Help".to_string(),
    ]);
    let page_ordinals: Vec<u32> = page_values
        .iter()
        .map(|v| match *v {
            "Category:Foo" => 0,
            "Main_Page" => 1,
            "Talk:Main_Page" => 2,
            "Wikipedia:Help" => 3,
            _ => 0,
        })
        .collect();
    let page_bitmaps = build_bitmaps(4, &page_ordinals, 12);
    let page_col = ColumnData::String(StringColumnData {
        dictionary: page_dict,
        encoded_values: page_ordinals,
        bitmap_indexes: page_bitmaps,
    });

    // user dimension: 3 unique users
    let user_values = vec![
        "Alice", "Bob", "Charlie", "Alice", "Charlie", "Bob", "Bob", "Alice", "Charlie", "Alice",
        "Bob", "Charlie",
    ];
    let user_dict = FrontCodedDictionary::from_sorted(vec![
        "Alice".to_string(),
        "Bob".to_string(),
        "Charlie".to_string(),
    ]);
    let user_ordinals: Vec<u32> = user_values
        .iter()
        .map(|v| match *v {
            "Alice" => 0,
            "Bob" => 1,
            "Charlie" => 2,
            _ => 0,
        })
        .collect();
    let user_bitmaps = build_bitmaps(3, &user_ordinals, 12);
    let user_col = ColumnData::String(StringColumnData {
        dictionary: user_dict,
        encoded_values: user_ordinals,
        bitmap_indexes: user_bitmaps,
    });

    // channel dimension: 2 unique channels
    let channel_values = vec![
        "#en", "#en", "#de", "#en", "#de", "#en", "#de", "#en", "#en", "#de", "#en", "#de",
    ];
    let channel_dict =
        FrontCodedDictionary::from_sorted(vec!["#de".to_string(), "#en".to_string()]);
    let channel_ordinals: Vec<u32> = channel_values
        .iter()
        .map(|v| if *v == "#de" { 0 } else { 1 })
        .collect();
    let channel_bitmaps = build_bitmaps(2, &channel_ordinals, 12);
    let channel_col = ColumnData::String(StringColumnData {
        dictionary: channel_dict,
        encoded_values: channel_ordinals,
        bitmap_indexes: channel_bitmaps,
    });

    // language dimension
    let lang_values = vec![
        "en", "en", "de", "en", "de", "en", "de", "en", "en", "de", "en", "de",
    ];
    let lang_dict = FrontCodedDictionary::from_sorted(vec!["de".to_string(), "en".to_string()]);
    let lang_ordinals: Vec<u32> = lang_values
        .iter()
        .map(|v| if *v == "de" { 0 } else { 1 })
        .collect();
    let lang_bitmaps = build_bitmaps(2, &lang_ordinals, 12);
    let lang_col = ColumnData::String(StringColumnData {
        dictionary: lang_dict,
        encoded_values: lang_ordinals,
        bitmap_indexes: lang_bitmaps,
    });

    // Metrics: added, deleted, delta
    let added: Vec<f64> = vec![
        100.0, 50.0, 200.0, 150.0, 30.0, 80.0, 120.0, 60.0, 90.0, 40.0, 110.0, 70.0,
    ];
    let deleted: Vec<f64> = vec![
        10.0, 5.0, 20.0, 15.0, 3.0, 8.0, 12.0, 6.0, 9.0, 4.0, 11.0, 7.0,
    ];
    let delta: Vec<f64> = added
        .iter()
        .zip(deleted.iter())
        .map(|(a, d)| a - d)
        .collect();

    let mut columns = HashMap::new();
    columns.insert("__time".to_string(), ColumnData::Long(timestamps));
    columns.insert("page".to_string(), page_col);
    columns.insert("user".to_string(), user_col);
    columns.insert("channel".to_string(), channel_col);
    columns.insert("language".to_string(), lang_col);
    columns.insert("added".to_string(), ColumnData::Double(added));
    columns.insert("deleted".to_string(), ColumnData::Double(deleted));
    columns.insert("delta".to_string(), ColumnData::Double(delta));

    let start = chrono::DateTime::parse_from_rfc3339("2013-01-01T00:00:00Z")
        .unwrap()
        .timestamp_millis();
    let end = chrono::DateTime::parse_from_rfc3339("2013-01-02T00:00:00Z")
        .unwrap()
        .timestamp_millis();

    SegmentData {
        version: 9,
        num_rows: 12,
        interval: ferrodruid_segment::Interval {
            start_millis: start,
            end_millis: end,
        },
        dimensions: vec![
            "page".to_string(),
            "user".to_string(),
            "channel".to_string(),
            "language".to_string(),
        ],
        metrics: vec![
            "added".to_string(),
            "deleted".to_string(),
            "delta".to_string(),
        ],
        columns,
        time_sorted: false,
    }
}

/// Build bitmap indexes for a string column from ordinals.
fn build_bitmaps(cardinality: usize, ordinals: &[u32], _num_rows: usize) -> Vec<DruidBitmap> {
    let mut bitmaps: Vec<DruidBitmap> = (0..cardinality).map(|_| DruidBitmap::new()).collect();
    for (row_idx, &ord) in ordinals.iter().enumerate() {
        bitmaps[ord as usize].insert(row_idx as u32);
    }
    bitmaps
}

/// Helper: parse a DruidQuery from JSON, re-serialize, re-parse, and return it.
fn parse_round_trip(json: &str) -> DruidQuery {
    let query: DruidQuery = serde_json::from_str(json).expect("parse query");
    let reserialized = serde_json::to_string(&query).expect("serialize");
    let _reparsed: DruidQuery = serde_json::from_str(&reserialized).expect("re-parse");
    query
}

// ===========================================================================
// Part 1: JSON Parsing Compatibility (all 8 query types)
// ===========================================================================

#[test]
fn compat_parse_timeseries_full() {
    let json = r#"{
        "queryType": "timeseries",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "hour",
        "filter": {
            "type": "selector",
            "dimension": "page",
            "value": "Main_Page"
        },
        "aggregations": [
            { "type": "count", "name": "edits" },
            { "type": "longSum", "name": "added", "fieldName": "added" },
            { "type": "longSum", "name": "deleted", "fieldName": "deleted" }
        ],
        "postAggregations": [
            {
                "type": "arithmetic",
                "name": "net_change",
                "fn": "+",
                "fields": [
                    { "type": "fieldAccess", "name": "added_val", "fieldName": "added" },
                    { "type": "arithmetic", "name": "neg_deleted", "fn": "*",
                      "fields": [
                          { "type": "fieldAccess", "name": "del", "fieldName": "deleted" },
                          { "type": "constant", "name": "neg", "value": -1 }
                      ]
                    }
                ]
            }
        ],
        "context": { "skipEmptyBuckets": true }
    }"#;
    let query = parse_round_trip(json);
    match &query {
        DruidQuery::Timeseries(ts) => {
            assert_eq!(ts.aggregations.len(), 3);
            assert!(ts.filter.is_some());
            assert!(ts.post_aggregations.is_some());
            assert!(ts.context.is_some());
        }
        _ => panic!("wrong query type"),
    }
}

#[test]
fn compat_parse_timeseries_granularity_all() {
    let json = r#"{
        "queryType": "timeseries",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "aggregations": [{"type":"count","name":"cnt"}]
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::Timeseries(_)));
}

#[test]
fn compat_parse_timeseries_granularity_day() {
    let json = r#"{
        "queryType": "timeseries",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-08T00:00:00.000Z"],
        "granularity": "day",
        "aggregations": [{"type":"count","name":"cnt"}],
        "descending": true
    }"#;
    let query = parse_round_trip(json);
    match &query {
        DruidQuery::Timeseries(ts) => {
            assert_eq!(ts.descending, Some(true));
        }
        _ => panic!("wrong query type"),
    }
}

#[test]
fn compat_parse_topn_numeric_metric() {
    let json = r#"{
        "queryType": "topN",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimension": {"type":"default","dimension":"page","output_name":"page","output_type":"STRING"},
        "threshold": 10,
        "metric": {"type":"numeric","metric":"edits"},
        "aggregations": [
            {"type":"count","name":"edits"},
            {"type":"doubleSum","name":"total_added","fieldName":"added"}
        ]
    }"#;
    let query = parse_round_trip(json);
    match &query {
        DruidQuery::TopN(q) => {
            assert_eq!(q.threshold, 10);
            assert_eq!(q.aggregations.len(), 2);
        }
        _ => panic!("wrong query type"),
    }
}

#[test]
fn compat_parse_topn_dimension_metric() {
    let json = r#"{
        "queryType": "topN",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimension": {"type":"default","dimension":"page","output_name":"page","output_type":"STRING"},
        "threshold": 5,
        "metric": {"type":"dimension","ordering":"lexicographic"},
        "aggregations": [{"type":"count","name":"cnt"}]
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::TopN(_)));
}

#[test]
fn compat_parse_topn_inverted_metric() {
    let json = r#"{
        "queryType": "topN",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimension": {"type":"default","dimension":"page","output_name":"page","output_type":"STRING"},
        "threshold": 3,
        "metric": {"type":"inverted","metric":{"type":"numeric","metric":"cnt"}},
        "aggregations": [{"type":"count","name":"cnt"}]
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::TopN(_)));
}

#[test]
fn compat_parse_groupby_full() {
    let json = r#"{
        "queryType": "groupBy",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimensions": [
            {"type":"default","dimension":"page","output_name":"page","output_type":"STRING"},
            {"type":"default","dimension":"user","output_name":"user","output_type":"STRING"}
        ],
        "aggregations": [
            {"type":"count","name":"cnt"},
            {"type":"doubleSum","name":"total_added","fieldName":"added"}
        ],
        "having": {"type":"greaterThan","aggregation":"cnt","value":1.0},
        "limitSpec": {
            "type": "default",
            "limit": 5,
            "columns": [
                {"dimension":"cnt","direction":"descending","dimensionOrder":"numeric"}
            ]
        }
    }"#;
    let query = parse_round_trip(json);
    match &query {
        DruidQuery::GroupBy(q) => {
            assert_eq!(q.dimensions.len(), 2);
            assert!(q.having.is_some());
            assert!(q.limit_spec.is_some());
        }
        _ => panic!("wrong query type"),
    }
}

#[test]
fn compat_parse_groupby_with_post_agg() {
    let json = r#"{
        "queryType": "groupBy",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "day",
        "dimensions": [
            {"type":"default","dimension":"channel","output_name":"channel","output_type":"STRING"}
        ],
        "aggregations": [
            {"type":"count","name":"cnt"},
            {"type":"doubleSum","name":"total_added","fieldName":"added"}
        ],
        "postAggregations": [
            {
                "type": "arithmetic",
                "name": "avg_added",
                "fn": "/",
                "fields": [
                    {"type":"fieldAccess","name":"ta","fieldName":"total_added"},
                    {"type":"fieldAccess","name":"c","fieldName":"cnt"}
                ]
            }
        ]
    }"#;
    let query = parse_round_trip(json);
    match &query {
        DruidQuery::GroupBy(q) => {
            assert!(q.post_aggregations.is_some());
        }
        _ => panic!("wrong query type"),
    }
}

#[test]
fn compat_parse_scan_full() {
    let json = r##"{
        "queryType": "scan",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "columns": ["__time", "page", "user", "added"],
        "limit": 100,
        "offset": 10,
        "order": "descending",
        "resultFormat": "list",
        "filter": {
            "type": "selector",
            "dimension": "channel",
            "value": "#en"
        }
    }"##;
    let query = parse_round_trip(json);
    match &query {
        DruidQuery::Scan(q) => {
            assert_eq!(q.columns.as_ref().unwrap().len(), 4);
            assert_eq!(q.limit, Some(100));
            assert_eq!(q.offset, Some(10));
            assert_eq!(q.order.as_deref(), Some("descending"));
            assert!(q.filter.is_some());
        }
        _ => panic!("wrong query type"),
    }
}

#[test]
fn compat_parse_scan_ascending() {
    let json = r#"{
        "queryType": "scan",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "order": "ascending",
        "limit": 50
    }"#;
    let query = parse_round_trip(json);
    match &query {
        DruidQuery::Scan(q) => {
            assert_eq!(q.order.as_deref(), Some("ascending"));
        }
        _ => panic!("wrong query type"),
    }
}

#[test]
fn compat_parse_scan_none_order() {
    let json = r#"{
        "queryType": "scan",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "order": "none"
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::Scan(_)));
}

#[test]
fn compat_parse_search_contains() {
    let json = r#"{
        "queryType": "search",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "query": {"type":"contains","value":"Main"},
        "searchDimensions": ["page"]
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::Search(_)));
}

#[test]
fn compat_parse_search_insensitive_contains() {
    let json = r#"{
        "queryType": "search",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "query": {"type":"insensitive_contains","value":"main"},
        "searchDimensions": ["page"]
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::Search(_)));
}

#[test]
fn compat_parse_search_fragment() {
    let json = r#"{
        "queryType": "search",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "query": {"type":"fragment","values":["Main","Page"],"case_sensitive":true},
        "searchDimensions": ["page"],
        "sort": {"type":"lexicographic"},
        "limit": 25
    }"#;
    let query = parse_round_trip(json);
    match &query {
        DruidQuery::Search(q) => {
            assert_eq!(q.limit, Some(25));
            assert!(q.sort.is_some());
        }
        _ => panic!("wrong query type"),
    }
}

#[test]
fn compat_parse_search_regex() {
    let json = r#"{
        "queryType": "search",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "query": {"type":"regex","pattern":"^Main.*"},
        "searchDimensions": ["page"]
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::Search(_)));
}

#[test]
fn compat_parse_segment_metadata_full() {
    let json = r#"{
        "queryType": "segmentMetadata",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"]
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::SegmentMetadata(_)));
}

#[test]
fn compat_parse_segment_metadata_minimal() {
    let json = r#"{
        "queryType": "segmentMetadata",
        "dataSource": {"type":"table","name":"wikipedia"}
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::SegmentMetadata(_)));
}

#[test]
fn compat_parse_datasource_metadata() {
    let json = r#"{
        "queryType": "dataSourceMetadata",
        "dataSource": {"type":"table","name":"wikipedia"}
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::DataSourceMetadata(_)));
}

#[test]
fn compat_parse_time_boundary_both() {
    let json = r#"{
        "queryType": "timeBoundary",
        "dataSource": {"type":"table","name":"wikipedia"}
    }"#;
    let query = parse_round_trip(json);
    assert!(matches!(query, DruidQuery::TimeBoundary(_)));
}

#[test]
fn compat_parse_time_boundary_max() {
    let json = r#"{
        "queryType": "timeBoundary",
        "dataSource": {"type":"table","name":"wikipedia"},
        "bound": "maxTime"
    }"#;
    let query = parse_round_trip(json);
    match &query {
        DruidQuery::TimeBoundary(q) => {
            assert_eq!(q.bound.as_deref(), Some("maxTime"));
        }
        _ => panic!("wrong query type"),
    }
}

#[test]
fn compat_parse_time_boundary_min() {
    let json = r#"{
        "queryType": "timeBoundary",
        "dataSource": {"type":"table","name":"wikipedia"},
        "bound": "minTime"
    }"#;
    let query = parse_round_trip(json);
    match &query {
        DruidQuery::TimeBoundary(q) => {
            assert_eq!(q.bound.as_deref(), Some("minTime"));
        }
        _ => panic!("wrong query type"),
    }
}

// ===========================================================================
// Part 2: Filter Compatibility
// ===========================================================================

use ferrodruid_query::FilterSpec;

/// Helper: parse a filter from JSON and verify round-trip.
fn parse_filter(json: &str) -> FilterSpec {
    let f: FilterSpec = serde_json::from_str(json).expect("parse filter");
    let re = serde_json::to_string(&f).expect("serialize filter");
    let _: FilterSpec = serde_json::from_str(&re).expect("re-parse filter");
    f
}

#[test]
fn compat_filter_selector() {
    let f = parse_filter(r#"{"type":"selector","dimension":"page","value":"Main_Page"}"#);
    assert!(matches!(f, FilterSpec::Selector { .. }));
}

#[test]
fn compat_filter_selector_null() {
    let f = parse_filter(r#"{"type":"selector","dimension":"page","value":null}"#);
    assert!(matches!(f, FilterSpec::Selector { .. }));
}

#[test]
fn compat_filter_in() {
    let f = parse_filter(
        r#"{"type":"in","dimension":"page","values":["Main_Page","Talk:Main_Page","Other"]}"#,
    );
    assert!(matches!(f, FilterSpec::In { .. }));
}

#[test]
fn compat_filter_bound_numeric() {
    let f = parse_filter(
        r#"{
            "type": "bound",
            "dimension": "added",
            "lower": "100",
            "upper": "500",
            "lowerStrict": false,
            "upperStrict": true,
            "ordering": "numeric"
        }"#,
    );
    assert!(matches!(f, FilterSpec::Bound { .. }));
}

#[test]
fn compat_filter_bound_lexicographic() {
    let f = parse_filter(
        r#"{
            "type": "bound",
            "dimension": "page",
            "lower": "A",
            "upper": "N"
        }"#,
    );
    assert!(matches!(f, FilterSpec::Bound { .. }));
}

#[test]
fn compat_filter_range() {
    let f = parse_filter(
        r#"{
            "type": "range",
            "column": "added",
            "matchValueType": "DOUBLE",
            "lower": 50.0,
            "upper": 200.0,
            "lowerOpen": false,
            "upperOpen": true
        }"#,
    );
    assert!(matches!(f, FilterSpec::Range { .. }));
}

#[test]
fn compat_filter_range_string() {
    let f = parse_filter(
        r#"{
            "type": "range",
            "column": "page",
            "matchValueType": "STRING",
            "lower": "A",
            "upper": "Z"
        }"#,
    );
    assert!(matches!(f, FilterSpec::Range { .. }));
}

#[test]
fn compat_filter_like() {
    let f = parse_filter(
        r#"{
            "type": "like",
            "dimension": "page",
            "pattern": "Main%",
            "escape": "\\"
        }"#,
    );
    assert!(matches!(f, FilterSpec::Like { .. }));
}

#[test]
fn compat_filter_regex() {
    let f = parse_filter(
        r#"{
            "type": "regex",
            "dimension": "page",
            "pattern": "^Main.*Page$"
        }"#,
    );
    assert!(matches!(f, FilterSpec::Regex { .. }));
}

#[test]
fn compat_filter_search_contains() {
    let f = parse_filter(
        r#"{
            "type": "search",
            "dimension": "page",
            "query": {"type":"contains","value":"Main"}
        }"#,
    );
    assert!(matches!(f, FilterSpec::Search { .. }));
}

#[test]
fn compat_filter_search_fragment() {
    let f = parse_filter(
        r#"{
            "type": "search",
            "dimension": "page",
            "query": {"type":"fragment","values":["Main","Page"],"case_sensitive":false}
        }"#,
    );
    assert!(matches!(f, FilterSpec::Search { .. }));
}

#[test]
fn compat_filter_and_or_not() {
    let f = parse_filter(
        r##"{
            "type": "and",
            "fields": [
                {"type":"selector","dimension":"channel","value":"#en"},
                {"type": "or", "fields": [
                    {"type":"selector","dimension":"page","value":"Main_Page"},
                    {"type":"not","field":{"type":"selector","dimension":"user","value":"Bot"}}
                ]}
            ]
        }"##,
    );
    assert!(matches!(f, FilterSpec::And { .. }));
}

#[test]
fn compat_filter_interval() {
    let f = parse_filter(
        r#"{
            "type": "interval",
            "dimension": "__time",
            "intervals": [
                "2013-01-01T00:00:00.000Z/2013-01-01T12:00:00.000Z",
                "2013-01-02T00:00:00.000Z/2013-01-02T12:00:00.000Z"
            ]
        }"#,
    );
    assert!(matches!(f, FilterSpec::Interval { .. }));
}

#[test]
fn compat_filter_expression() {
    let f = parse_filter(r#"{"type":"expression","expression":"\"added\" > 100"}"#);
    assert!(matches!(f, FilterSpec::Expression { .. }));
}

#[test]
fn compat_filter_true_false() {
    let t = parse_filter(r#"{"type":"true"}"#);
    let f = parse_filter(r#"{"type":"false"}"#);
    assert!(matches!(t, FilterSpec::True));
    assert!(matches!(f, FilterSpec::False));
}

#[test]
fn compat_filter_null() {
    let f = parse_filter(r#"{"type":"null","column":"user"}"#);
    assert!(matches!(f, FilterSpec::Null { .. }));
}

// ===========================================================================
// Part 3: Aggregation Compatibility
// ===========================================================================

use ferrodruid_aggregator::AggregatorSpec;

fn parse_agg(json: &str) -> AggregatorSpec {
    let spec: AggregatorSpec = serde_json::from_str(json).expect("parse agg");
    let re = serde_json::to_string(&spec).expect("serialize agg");
    let _: AggregatorSpec = serde_json::from_str(&re).expect("re-parse agg");
    spec
}

#[test]
fn compat_agg_count() {
    let spec = parse_agg(r#"{"type":"count","name":"cnt"}"#);
    assert_eq!(spec.name(), "cnt");
    assert!(spec.field_name().is_none());
}

#[test]
fn compat_agg_long_sum() {
    let spec = parse_agg(r#"{"type":"longSum","name":"total","fieldName":"added"}"#);
    assert_eq!(spec.name(), "total");
    assert_eq!(spec.field_name(), Some("added"));
}

#[test]
fn compat_agg_double_sum() {
    let spec = parse_agg(r#"{"type":"doubleSum","name":"total","fieldName":"added"}"#);
    assert_eq!(spec.name(), "total");
}

#[test]
fn compat_agg_float_sum() {
    let spec = parse_agg(r#"{"type":"floatSum","name":"total","fieldName":"added"}"#);
    assert_eq!(spec.name(), "total");
}

#[test]
fn compat_agg_long_min() {
    let spec = parse_agg(r#"{"type":"longMin","name":"min_val","fieldName":"added"}"#);
    assert_eq!(spec.name(), "min_val");
}

#[test]
fn compat_agg_long_max() {
    let spec = parse_agg(r#"{"type":"longMax","name":"max_val","fieldName":"added"}"#);
    assert_eq!(spec.name(), "max_val");
}

#[test]
fn compat_agg_double_min() {
    let spec = parse_agg(r#"{"type":"doubleMin","name":"min_val","fieldName":"price"}"#);
    assert_eq!(spec.name(), "min_val");
}

#[test]
fn compat_agg_double_max() {
    let spec = parse_agg(r#"{"type":"doubleMax","name":"max_val","fieldName":"price"}"#);
    assert_eq!(spec.name(), "max_val");
}

#[test]
fn compat_agg_float_min() {
    let spec = parse_agg(r#"{"type":"floatMin","name":"min_val","fieldName":"price"}"#);
    assert_eq!(spec.name(), "min_val");
}

#[test]
fn compat_agg_float_max() {
    let spec = parse_agg(r#"{"type":"floatMax","name":"max_val","fieldName":"price"}"#);
    assert_eq!(spec.name(), "max_val");
}

#[test]
fn compat_agg_long_first() {
    let spec = parse_agg(r#"{"type":"longFirst","name":"first_val","fieldName":"added"}"#);
    assert_eq!(spec.name(), "first_val");
}

#[test]
fn compat_agg_long_last() {
    let spec = parse_agg(r#"{"type":"longLast","name":"last_val","fieldName":"added"}"#);
    assert_eq!(spec.name(), "last_val");
}

#[test]
fn compat_agg_double_first() {
    let spec = parse_agg(r#"{"type":"doubleFirst","name":"first_val","fieldName":"price"}"#);
    assert_eq!(spec.name(), "first_val");
}

#[test]
fn compat_agg_double_last() {
    let spec = parse_agg(r#"{"type":"doubleLast","name":"last_val","fieldName":"price"}"#);
    assert_eq!(spec.name(), "last_val");
}

#[test]
fn compat_agg_float_first() {
    let spec = parse_agg(r#"{"type":"floatFirst","name":"first_val","fieldName":"temp"}"#);
    assert_eq!(spec.name(), "first_val");
}

#[test]
fn compat_agg_float_last() {
    let spec = parse_agg(r#"{"type":"floatLast","name":"last_val","fieldName":"temp"}"#);
    assert_eq!(spec.name(), "last_val");
}

#[test]
fn compat_agg_string_first() {
    let spec = parse_agg(
        r#"{"type":"stringFirst","name":"first_page","fieldName":"page","maxStringBytes":1024}"#,
    );
    assert_eq!(spec.name(), "first_page");
}

#[test]
fn compat_agg_string_last() {
    let spec = parse_agg(
        r#"{"type":"stringLast","name":"last_page","fieldName":"page","maxStringBytes":2048}"#,
    );
    assert_eq!(spec.name(), "last_page");
}

#[test]
fn compat_agg_filtered() {
    let spec = parse_agg(
        r##"{
            "type": "filtered",
            "filter": {"type":"selector","dimension":"channel","value":"#en"},
            "aggregator": {"type":"count","name":"en_edits"}
        }"##,
    );
    assert_eq!(spec.name(), "en_edits");
}

// ===========================================================================
// Part 3b: Post-Aggregation Compatibility
// ===========================================================================

use ferrodruid_aggregator::PostAggregatorSpec;

fn parse_post_agg(json: &str) -> PostAggregatorSpec {
    let spec: PostAggregatorSpec = serde_json::from_str(json).expect("parse post-agg");
    let re = serde_json::to_string(&spec).expect("serialize post-agg");
    let _: PostAggregatorSpec = serde_json::from_str(&re).expect("re-parse post-agg");
    spec
}

#[test]
fn compat_post_agg_arithmetic() {
    let spec = parse_post_agg(
        r#"{
            "type": "arithmetic",
            "name": "avg",
            "fn": "/",
            "fields": [
                {"type":"fieldAccess","name":"t","fieldName":"total"},
                {"type":"fieldAccess","name":"c","fieldName":"cnt"}
            ]
        }"#,
    );
    assert_eq!(spec.name(), "avg");
}

#[test]
fn compat_post_agg_field_access() {
    let spec =
        parse_post_agg(r#"{"type":"fieldAccess","name":"access_total","fieldName":"total_added"}"#);
    assert_eq!(spec.name(), "access_total");
}

#[test]
fn compat_post_agg_constant() {
    let spec = parse_post_agg(r#"{"type":"constant","name":"const_pi","value":3.14159}"#);
    assert_eq!(spec.name(), "const_pi");
}

#[test]
fn compat_post_agg_expression() {
    let spec = parse_post_agg(r#"{"type":"expression","name":"expr1","expression":"x + y * 2"}"#);
    assert_eq!(spec.name(), "expr1");
}

#[test]
fn compat_post_agg_hyper_unique_cardinality() {
    let spec = parse_post_agg(
        r#"{"type":"hyperUniqueCardinality","name":"unique_users","fieldName":"user_hll"}"#,
    );
    assert_eq!(spec.name(), "unique_users");
}

// ===========================================================================
// Part 4: Execution Compatibility
// ===========================================================================

#[test]
fn compat_exec_timeseries_count_all() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "timeseries",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "aggregations": [
            {"type":"count","name":"count"},
            {"type":"doubleSum","name":"total_added","fieldName":"added"}
        ]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Timeseries(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].result.get("count"), Some(&json!(12)));
            // total_added = sum of all added values = 1100.0
            let total = results[0]
                .result
                .get("total_added")
                .and_then(|v| v.as_f64())
                .expect("total_added");
            assert!((total - 1100.0).abs() < 0.01);
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_timeseries_hourly() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "timeseries",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "hour",
        "aggregations": [{"type":"count","name":"count"}]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Timeseries(results) => {
            // 4 hourly buckets, each with 3 rows
            assert_eq!(results.len(), 4);
            for r in &results {
                assert_eq!(r.result.get("count"), Some(&json!(3)));
            }
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_timeseries_with_filter() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "timeseries",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "filter": {"type":"selector","dimension":"page","value":"Main_Page"},
        "aggregations": [
            {"type":"count","name":"edits"},
            {"type":"doubleSum","name":"total_added","fieldName":"added"}
        ]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Timeseries(results) => {
            assert_eq!(results.len(), 1);
            // Main_Page rows: indices 0, 3, 6, 10 -> 4 rows
            assert_eq!(results[0].result.get("edits"), Some(&json!(4)));
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_timeseries_with_post_agg() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "timeseries",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "aggregations": [
            {"type":"count","name":"cnt"},
            {"type":"doubleSum","name":"total_added","fieldName":"added"}
        ],
        "postAggregations": [
            {
                "type": "arithmetic",
                "name": "avg_added",
                "fn": "/",
                "fields": [
                    {"type":"fieldAccess","name":"t","fieldName":"total_added"},
                    {"type":"fieldAccess","name":"c","fieldName":"cnt"}
                ]
            }
        ]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Timeseries(results) => {
            assert_eq!(results.len(), 1);
            let avg = results[0]
                .result
                .get("avg_added")
                .and_then(|v| v.as_f64())
                .expect("avg_added");
            // 1100.0 / 12 = 91.666...
            assert!((avg - (1100.0 / 12.0)).abs() < 0.01);
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_timeseries_descending() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "timeseries",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "hour",
        "descending": true,
        "aggregations": [{"type":"count","name":"cnt"}]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Timeseries(results) => {
            assert_eq!(results.len(), 4);
            // Descending: last hour first
            assert!(results[0].timestamp > results[3].timestamp);
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_topn_by_count() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "topN",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimension": {"type":"default","dimension":"page","output_name":"page","output_type":"STRING"},
        "threshold": 2,
        "metric": {"type":"numeric","metric":"edits"},
        "aggregations": [{"type":"count","name":"edits"}]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::TopN(results) => {
            assert_eq!(results.len(), 1);
            assert!(results[0].result.len() <= 2);
            // Main_Page has 4 edits (most), should be first
            assert_eq!(results[0].result[0].get("page"), Some(&json!("Main_Page")));
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_topn_lexicographic() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "topN",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimension": {"type":"default","dimension":"page","output_name":"page","output_type":"STRING"},
        "threshold": 10,
        "metric": {"type":"dimension","ordering":"lexicographic"},
        "aggregations": [{"type":"count","name":"cnt"}]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::TopN(results) => {
            assert_eq!(results.len(), 1);
            let pages: Vec<&str> = results[0]
                .result
                .iter()
                .filter_map(|m| m.get("page").and_then(|v| v.as_str()))
                .collect();
            // Lexicographic: Category:Foo < Main_Page < Talk:Main_Page < Wikipedia:Help
            assert_eq!(pages[0], "Category:Foo");
            assert_eq!(pages[1], "Main_Page");
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_topn_inverted() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "topN",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimension": {"type":"default","dimension":"user","output_name":"user","output_type":"STRING"},
        "threshold": 3,
        "metric": {"type":"inverted","metric":{"type":"numeric","metric":"cnt"}},
        "aggregations": [{"type":"count","name":"cnt"}]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::TopN(results) => {
            assert_eq!(results.len(), 1);
            // Inverted: least frequent first
            let counts: Vec<i64> = results[0]
                .result
                .iter()
                .filter_map(|m| m.get("cnt").and_then(|v| v.as_i64()))
                .collect();
            // Sorted ascending (inverted from descending)
            for w in counts.windows(2) {
                assert!(w[0] <= w[1]);
            }
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_groupby_single_dim() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "groupBy",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimensions": [
            {"type":"default","dimension":"channel","output_name":"channel","output_type":"STRING"}
        ],
        "aggregations": [
            {"type":"count","name":"cnt"},
            {"type":"doubleSum","name":"total_added","fieldName":"added"}
        ]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::GroupBy(results) => {
            assert_eq!(results.len(), 2); // #de and #en
            for r in &results {
                assert_eq!(r.version, "v1");
                assert!(r.event.contains_key("channel"));
                assert!(r.event.contains_key("cnt"));
                assert!(r.event.contains_key("total_added"));
            }
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_groupby_multi_dim() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "groupBy",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimensions": [
            {"type":"default","dimension":"channel","output_name":"channel","output_type":"STRING"},
            {"type":"default","dimension":"user","output_name":"user","output_type":"STRING"}
        ],
        "aggregations": [{"type":"count","name":"cnt"}]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::GroupBy(results) => {
            // channel (2) x user (3) = up to 6 groups
            assert!(results.len() >= 2 && results.len() <= 6);
            for r in &results {
                assert!(r.event.contains_key("channel"));
                assert!(r.event.contains_key("user"));
            }
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_groupby_having() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "groupBy",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimensions": [
            {"type":"default","dimension":"page","output_name":"page","output_type":"STRING"}
        ],
        "aggregations": [{"type":"count","name":"cnt"}],
        "having": {"type":"greaterThan","aggregation":"cnt","value":2.0}
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::GroupBy(results) => {
            for r in &results {
                let cnt = r.event.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0);
                assert!(cnt > 2, "having filter should exclude cnt <= 2");
            }
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_groupby_limit_spec() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "groupBy",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimensions": [
            {"type":"default","dimension":"page","output_name":"page","output_type":"STRING"}
        ],
        "aggregations": [{"type":"count","name":"cnt"}],
        "limitSpec": {
            "type": "default",
            "limit": 2,
            "columns": [{"dimension":"cnt","direction":"descending","dimensionOrder":"numeric"}]
        }
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::GroupBy(results) => {
            assert_eq!(results.len(), 2);
            let c0 = results[0]
                .event
                .get("cnt")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let c1 = results[1]
                .event
                .get("cnt")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            assert!(c0 >= c1, "should be sorted descending by cnt");
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_scan_all_columns() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "scan",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "limit": 5
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Scan(scan) => {
            assert_eq!(scan.events.len(), 5);
            // Should include all columns
            assert!(scan.columns.contains(&"__time".to_string()));
            assert!(scan.columns.contains(&"page".to_string()));
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_scan_selected_columns() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "scan",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "columns": ["page", "added"],
        "limit": 3
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Scan(scan) => {
            assert_eq!(scan.events.len(), 3);
            assert_eq!(scan.columns, vec!["page", "added"]);
            for ev in &scan.events {
                assert!(ev.contains_key("page"));
                assert!(ev.contains_key("added"));
                // Should not contain other columns
                assert!(!ev.contains_key("user"));
            }
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_scan_with_filter() {
    let segment = build_wikipedia_segment();
    let json = r##"{
        "queryType": "scan",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "filter": {"type":"selector","dimension":"channel","value":"#de"},
        "columns": ["page", "channel", "added"]
    }"##;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Scan(scan) => {
            for ev in &scan.events {
                assert_eq!(ev.get("channel"), Some(&json!("#de")));
            }
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_scan_descending() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "scan",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "order": "descending",
        "columns": ["__time", "page"]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Scan(scan) => {
            assert_eq!(scan.events.len(), 12);
            let first_ts = scan.events[0]
                .get("__time")
                .and_then(|v| v.as_i64())
                .expect("ts");
            let last_ts = scan.events[11]
                .get("__time")
                .and_then(|v| v.as_i64())
                .expect("ts");
            assert!(first_ts >= last_ts);
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_scan_offset() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "scan",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "limit": 3,
        "offset": 5
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Scan(scan) => {
            assert_eq!(scan.events.len(), 3);
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_search_contains() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "search",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "query": {"type":"contains","value":"Main"},
        "searchDimensions": ["page"]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Search(results) => {
            assert!(!results.is_empty());
            let all_values: Vec<&str> = results
                .iter()
                .flat_map(|r| r.result.iter().map(|h| h.value.as_str()))
                .collect();
            assert!(all_values.contains(&"Main_Page"));
            assert!(all_values.contains(&"Talk:Main_Page"));
            // Should NOT contain Wikipedia:Help or Category:Foo
            assert!(!all_values.contains(&"Category:Foo"));
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_search_insensitive_contains() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "search",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "query": {"type":"insensitive_contains","value":"main"},
        "searchDimensions": ["page"]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Search(results) => {
            let all_values: Vec<&str> = results
                .iter()
                .flat_map(|r| r.result.iter().map(|h| h.value.as_str()))
                .collect();
            // Case-insensitive: "main" should match "Main_Page" and "Talk:Main_Page"
            assert!(all_values.contains(&"Main_Page"));
            assert!(all_values.contains(&"Talk:Main_Page"));
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_search_regex() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "search",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "query": {"type":"regex","pattern":"^Wiki.*"},
        "searchDimensions": ["page"]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::Search(results) => {
            let all_values: Vec<&str> = results
                .iter()
                .flat_map(|r| r.result.iter().map(|h| h.value.as_str()))
                .collect();
            assert!(all_values.contains(&"Wikipedia:Help"));
            assert!(!all_values.contains(&"Main_Page"));
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_segment_metadata() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "segmentMetadata",
        "dataSource": {"type":"table","name":"wikipedia"}
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::SegmentMetadata(results) => {
            assert_eq!(results.len(), 1);
            let meta = &results[0];
            assert_eq!(meta.num_rows, 12);
            assert!(meta.columns.contains_key("__time"));
            assert!(meta.columns.contains_key("page"));
            assert!(meta.columns.contains_key("added"));
            assert_eq!(
                meta.columns.get("__time").map(|c| c.typ.as_str()),
                Some("LONG")
            );
            assert_eq!(
                meta.columns.get("page").map(|c| c.typ.as_str()),
                Some("STRING")
            );
            assert_eq!(
                meta.columns.get("added").map(|c| c.typ.as_str()),
                Some("DOUBLE")
            );
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_datasource_metadata() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "dataSourceMetadata",
        "dataSource": {"type":"table","name":"wikipedia"}
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::DataSourceMetadata(results) => {
            assert_eq!(results.len(), 1);
            // Timestamp should be the max timestamp (hour 3)
            assert!(results[0].timestamp.starts_with("2013-01-01T03"));
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_time_boundary_both() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "timeBoundary",
        "dataSource": {"type":"table","name":"wikipedia"}
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::TimeBoundary(results) => {
            assert_eq!(results.len(), 1);
            let r = &results[0].result;
            assert!(r.contains_key("minTime"));
            assert!(r.contains_key("maxTime"));
            let min = r.get("minTime").and_then(|v| v.as_str()).expect("minTime");
            let max = r.get("maxTime").and_then(|v| v.as_str()).expect("maxTime");
            assert!(min.starts_with("2013-01-01T00"));
            assert!(max.starts_with("2013-01-01T03"));
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_time_boundary_min_only() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "timeBoundary",
        "dataSource": {"type":"table","name":"wikipedia"},
        "bound": "minTime"
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::TimeBoundary(results) => {
            assert_eq!(results.len(), 1);
            assert!(results[0].result.contains_key("minTime"));
            assert!(!results[0].result.contains_key("maxTime"));
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_exec_time_boundary_max_only() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "timeBoundary",
        "dataSource": {"type":"table","name":"wikipedia"},
        "bound": "maxTime"
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match result {
        QueryResult::TimeBoundary(results) => {
            assert_eq!(results.len(), 1);
            assert!(!results[0].result.contains_key("minTime"));
            assert!(results[0].result.contains_key("maxTime"));
        }
        _ => panic!("wrong result type"),
    }
}

// ===========================================================================
// Part 5: Response Format Compatibility
// ===========================================================================

#[test]
fn compat_response_format_timeseries() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "timeseries",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "aggregations": [{"type":"count","name":"count"}]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match &result {
        QueryResult::Timeseries(results) => {
            // Druid timeseries format: [{"timestamp": "...", "result": {...}}]
            let serialized = serde_json::to_value(results).expect("serialize");
            let arr = serialized.as_array().expect("should be array");
            assert!(!arr.is_empty());
            let first = &arr[0];
            assert!(first.get("timestamp").is_some());
            assert!(first.get("result").is_some());
            assert!(first["result"].get("count").is_some());
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_response_format_topn() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "topN",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimension": {"type":"default","dimension":"page","output_name":"page","output_type":"STRING"},
        "threshold": 3,
        "metric": {"type":"numeric","metric":"cnt"},
        "aggregations": [{"type":"count","name":"cnt"}]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match &result {
        QueryResult::TopN(results) => {
            // Druid topN format: [{"timestamp": "...", "result": [{...}, ...]}]
            let serialized = serde_json::to_value(results).expect("serialize");
            let arr = serialized.as_array().expect("should be array");
            assert!(!arr.is_empty());
            let first = &arr[0];
            assert!(first.get("timestamp").is_some());
            let inner = first["result"].as_array().expect("result should be array");
            assert!(!inner.is_empty());
            // Each entry has the dimension value and metric
            assert!(inner[0].get("page").is_some());
            assert!(inner[0].get("cnt").is_some());
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_response_format_groupby() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "groupBy",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "granularity": "all",
        "dimensions": [
            {"type":"default","dimension":"channel","output_name":"channel","output_type":"STRING"}
        ],
        "aggregations": [{"type":"count","name":"cnt"}]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match &result {
        QueryResult::GroupBy(results) => {
            // Druid groupBy format: [{"version":"v1","timestamp":"...","event":{...}}]
            let serialized = serde_json::to_value(results).expect("serialize");
            let arr = serialized.as_array().expect("should be array");
            for entry in arr {
                assert_eq!(entry.get("version"), Some(&json!("v1")));
                assert!(entry.get("timestamp").is_some());
                assert!(entry.get("event").is_some());
                let event = entry.get("event").expect("event");
                assert!(event.get("channel").is_some());
                assert!(event.get("cnt").is_some());
            }
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_response_format_scan() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "scan",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "columns": ["page", "added"],
        "limit": 2
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match &result {
        QueryResult::Scan(scan) => {
            // Druid scan format: {"segmentId":..., "columns":[...], "events":[{...},...]}
            let serialized = serde_json::to_value(scan).expect("serialize");
            assert!(serialized.get("columns").is_some());
            assert!(serialized.get("events").is_some());
            let events = serialized["events"].as_array().expect("events array");
            assert_eq!(events.len(), 2);
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_response_format_search() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "search",
        "dataSource": {"type":"table","name":"wikipedia"},
        "intervals": ["2013-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"],
        "query": {"type":"contains","value":"Help"},
        "searchDimensions": ["page"]
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match &result {
        QueryResult::Search(results) => {
            // Druid search format: [{"timestamp":"...", "result":[{"dimension":"...","value":"...","count":N}]}]
            let serialized = serde_json::to_value(results).expect("serialize");
            let arr = serialized.as_array().expect("should be array");
            assert!(!arr.is_empty());
            let first = &arr[0];
            assert!(first.get("timestamp").is_some());
            let hits = first["result"].as_array().expect("result array");
            for hit in hits {
                assert!(hit.get("dimension").is_some());
                assert!(hit.get("value").is_some());
                assert!(hit.get("count").is_some());
            }
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_response_format_segment_metadata() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "segmentMetadata",
        "dataSource": {"type":"table","name":"wikipedia"}
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match &result {
        QueryResult::SegmentMetadata(results) => {
            // Druid format: [{"id":"...","intervals":[...],"columns":{...},"numRows":N}]
            let serialized = serde_json::to_value(results).expect("serialize");
            let arr = serialized.as_array().expect("should be array");
            let first = &arr[0];
            assert!(first.get("id").is_some());
            assert!(first.get("intervals").is_some());
            assert!(first.get("columns").is_some());
            assert!(first.get("numRows").is_some());
            // Column metadata should have type, hasMultipleValues, size
            let cols = first["columns"].as_object().expect("columns object");
            for (_name, col_meta) in cols {
                assert!(col_meta.get("type").is_some());
                assert!(col_meta.get("hasMultipleValues").is_some());
                assert!(col_meta.get("size").is_some());
            }
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_response_format_datasource_metadata() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "dataSourceMetadata",
        "dataSource": {"type":"table","name":"wikipedia"}
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match &result {
        QueryResult::DataSourceMetadata(results) => {
            // Druid format: [{"timestamp":"...","result":{"maxIngestedEventTime":"..."}}]
            let serialized = serde_json::to_value(results).expect("serialize");
            let arr = serialized.as_array().expect("should be array");
            let first = &arr[0];
            assert!(first.get("timestamp").is_some());
            assert!(first.get("result").is_some());
            assert!(first["result"].get("maxIngestedEventTime").is_some());
        }
        _ => panic!("wrong result type"),
    }
}

#[test]
fn compat_response_format_time_boundary() {
    let segment = build_wikipedia_segment();
    let json = r#"{
        "queryType": "timeBoundary",
        "dataSource": {"type":"table","name":"wikipedia"}
    }"#;
    let query: DruidQuery = serde_json::from_str(json).expect("parse");
    let result = execute_query(&query, &segment).expect("execute");
    match &result {
        QueryResult::TimeBoundary(results) => {
            // Druid format: [{"timestamp":"...","result":{"minTime":"...","maxTime":"..."}}]
            let serialized = serde_json::to_value(results).expect("serialize");
            let arr = serialized.as_array().expect("should be array");
            let first = &arr[0];
            assert!(first.get("timestamp").is_some());
            assert!(first.get("result").is_some());
            assert!(first["result"].get("minTime").is_some());
            assert!(first["result"].get("maxTime").is_some());
        }
        _ => panic!("wrong result type"),
    }
}
