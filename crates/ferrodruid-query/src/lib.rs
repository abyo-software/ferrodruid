// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid Native Query JSON parser and executor (timeseries, topN, groupBy, scan, search,
//! segmentMetadata, dataSourceMetadata, timeBoundary).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod context;
mod dim_spec;
pub mod executor;
pub mod expr;
pub mod filter;
pub mod groupby;
mod helpers;
pub mod join;
pub mod metadata;
pub mod scan;
pub mod search;
pub mod timeseries;
pub mod topn;
pub mod virtual_columns;
pub mod window;

use serde::{Deserialize, Serialize};

pub use context::QueryContext;
pub use executor::{execute_query, execute_query_with_timeout};
pub use filter::FilterSpec;
pub use groupby::{GroupByQuery, GroupByResult, HavingSpec, LimitSpec, OrderByColumnSpec};
pub use helpers::{GranularitySpec, parse_iso_millis};
pub use join::{
    JoinCondition, JoinDataSource, JoinRight, JoinType, Row as JoinRow, execute_join,
    join_output_columns,
};
pub use metadata::{
    DataSourceMetadataQuery, DataSourceMetadataResult, SegmentMetadataQuery, SegmentMetadataResult,
    TimeBoundaryQuery, TimeBoundaryResult,
};
pub use scan::{ScanQuery, ScanResult, align_union_branch};
pub use search::{SearchHit, SearchQuery, SearchResult};
pub use timeseries::{TimeseriesQuery, TimeseriesResult};
pub use topn::{TopNMetricSpec, TopNQuery, TopNResult};
pub use virtual_columns::{VirtualColumnSpec, VirtualColumns};
pub use window::{SortDirection, WindowFunctionKind, WindowOrderBy, WindowQuery, WindowSpec};

// ---------------------------------------------------------------------------
// DruidQuery — top-level query enum
// ---------------------------------------------------------------------------

/// Top-level enum representing all Druid native query types.
///
/// The query is tagged by the `queryType` JSON field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "queryType", rename_all = "camelCase")]
pub enum DruidQuery {
    /// Timeseries query — aggregate metrics over time buckets.
    Timeseries(TimeseriesQuery),
    /// TopN query — find top N dimension values by metric.
    #[serde(rename = "topN")]
    TopN(TopNQuery),
    /// GroupBy query — group by dimensions and aggregate.
    GroupBy(GroupByQuery),
    /// Scan query — raw row scan.
    Scan(ScanQuery),
    /// Search query — find matching dimension values.
    Search(SearchQuery),
    /// Segment metadata query.
    SegmentMetadata(SegmentMetadataQuery),
    /// Data source metadata query.
    DataSourceMetadata(DataSourceMetadataQuery),
    /// Time boundary query.
    TimeBoundary(TimeBoundaryQuery),
    /// Union of multiple queries — execute independently and concatenate results.
    #[serde(rename = "unionAll")]
    UnionAll(Vec<DruidQuery>),
    /// Window query — apply SQL window functions on top of an inner scan.
    Window(WindowQuery),
}

impl DruidQuery {
    /// Returns a reference to the query context, if any.
    pub fn context(&self) -> Option<&QueryContext> {
        match self {
            DruidQuery::Timeseries(q) => q.context.as_ref(),
            DruidQuery::TopN(q) => q.context.as_ref(),
            DruidQuery::GroupBy(q) => q.context.as_ref(),
            DruidQuery::Scan(q) => q.context.as_ref(),
            DruidQuery::Search(q) => q.context.as_ref(),
            DruidQuery::SegmentMetadata(q) => q.context.as_ref(),
            DruidQuery::DataSourceMetadata(q) => q.context.as_ref(),
            DruidQuery::TimeBoundary(q) => q.context.as_ref(),
            DruidQuery::UnionAll(queries) => queries.first().and_then(|q| q.context()),
            DruidQuery::Window(q) => q.context.as_ref(),
        }
    }
}

// ---------------------------------------------------------------------------
// Native-wire finalization (P1-#3)
// ---------------------------------------------------------------------------

/// Finalize raw sketch aggregator outputs for the native `/druid/v2` wire.
///
/// Apache Druid native queries FINALIZE aggregator outputs by default (the
/// `finalize` query-context flag defaults to `true`): a raw
/// `HLLSketchBuild` / `thetaSketch` / `quantilesDoublesSketch` aggregation
/// returns its finalized scalar, not the intermediate sketch (measured
/// against Druid 36.0.0 on 2026-07-12 — see
/// [`ferrodruid_aggregator::finalize_sketch_json`] for the exact shapes).
/// With `"context":{"finalize":false}` the intermediate is kept so a
/// merging consumer can union exact sketches.
///
/// Per-aggregator override (codex-review HIGH finding D): an aggregator
/// carrying `"shouldFinalize": false` keeps its intermediate even when the
/// context finalizes — [`ferrodruid_aggregator::finalize_sketch_json`]
/// returns `None` for it (the flag reaches through `filtered` wrappers).
/// Absent / `true` follows the context default, matching Druid.
///
/// Call this at the native WIRE OUTPUT stage only, AFTER the broker has
/// merged partials — merging requires the sketch intermediates, so this
/// must never run before or during the broker merge.  The SQL path is
/// deliberately not routed through here: it already finalizes via explicit
/// estimate post-aggregations (`APPROX_COUNT_DISTINCT` →
/// `HLLSketchEstimate`) and projects its own output columns.
///
/// Only Timeseries / TopN / GroupBy results carry aggregator maps; every
/// other result shape passes through untouched.  Non-sketch aggregators
/// (including `bloomFilter`, whose Druid-finalized form IS the filter) are
/// left untouched by [`ferrodruid_aggregator::finalize_sketch_json`]
/// returning `None`.
pub fn finalize_native_wire_outputs(query: &DruidQuery, result: &mut QueryResult) {
    // Druid's native default is finalize = true.
    let finalize = query.context().and_then(|c| c.finalize).unwrap_or(true);
    if !finalize {
        return;
    }
    let aggregations: &[ferrodruid_aggregator::AggregatorSpec] = match query {
        DruidQuery::Timeseries(q) => &q.aggregations,
        DruidQuery::TopN(q) => &q.aggregations,
        DruidQuery::GroupBy(q) => &q.aggregations,
        // Scan / Search / metadata / boundary results carry no aggregator
        // maps; UnionAll and Window merge to Scan-shaped results.
        _ => return,
    };
    match result {
        QueryResult::Timeseries(entries) => {
            for entry in entries {
                finalize_sketches_in_map(&mut entry.result, aggregations);
            }
        }
        QueryResult::TopN(entries) => {
            for entry in entries {
                for row in &mut entry.result {
                    finalize_sketches_in_map(row, aggregations);
                }
            }
        }
        QueryResult::GroupBy(entries) => {
            for entry in entries {
                finalize_sketches_in_map(&mut entry.event, aggregations);
            }
        }
        _ => {}
    }
}

/// Apply [`ferrodruid_aggregator::finalize_sketch_json`] to every declared
/// aggregation output present in one result map (JSON `null` included —
/// empty timeseries buckets finalize to the empty-sketch scalar).
fn finalize_sketches_in_map(
    map: &mut serde_json::Map<String, serde_json::Value>,
    aggregations: &[ferrodruid_aggregator::AggregatorSpec],
) {
    for spec in aggregations {
        let name = spec.name();
        let Some(value) = map.get(name) else {
            continue;
        };
        if let Some(finalized) = ferrodruid_aggregator::finalize_sketch_json(spec, value) {
            map.insert(name.to_string(), finalized);
        }
    }
}

// ---------------------------------------------------------------------------
// QueryResult — unified result enum
// ---------------------------------------------------------------------------

/// Unified query result enum.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryResult {
    /// Result from a timeseries query.
    Timeseries(Vec<TimeseriesResult>),
    /// Result from a TopN query.
    TopN(Vec<TopNResult>),
    /// Result from a GroupBy query.
    GroupBy(Vec<GroupByResult>),
    /// Result from a Scan query.
    Scan(ScanResult),
    /// Result from a Search query.
    Search(Vec<SearchResult>),
    /// Result from a segmentMetadata query.
    SegmentMetadata(Vec<SegmentMetadataResult>),
    /// Result from a dataSourceMetadata query.
    DataSourceMetadata(Vec<DataSourceMetadataResult>),
    /// Result from a timeBoundary query.
    TimeBoundary(Vec<TimeBoundaryResult>),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use ferrodruid_bitmap::DruidBitmap;
    use ferrodruid_dict::FrontCodedDictionary;
    use ferrodruid_segment::SegmentData;
    use ferrodruid_segment::column::{ColumnData, StringColumnData};
    use serde_json::json;

    /// Build a synthetic segment for testing.
    ///
    /// 6 rows:
    ///   __time:  day1, day1, day1, day2, day2, day2
    ///   region:  us,   us,   eu,   eu,   jp,   us
    ///   value:   10.0, 20.0, 30.0, 40.0, 50.0, 60.0
    ///   count_col: 1, 1, 1, 1, 1, 1
    fn build_test_segment() -> SegmentData {
        // Timestamps: 2024-01-01 and 2024-01-02
        let day1 = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("parse")
            .timestamp_millis();
        let day2 = chrono::DateTime::parse_from_rfc3339("2024-01-02T00:00:00Z")
            .expect("parse")
            .timestamp_millis();

        let timestamps = vec![day1, day1, day1, day2, day2, day2];

        // region: string column
        let dict = FrontCodedDictionary::from_sorted(vec![
            "eu".to_string(),
            "jp".to_string(),
            "us".to_string(),
        ]);
        // ordinals: us=2, us=2, eu=0, eu=0, jp=1, us=2
        let encoded_values = vec![2, 2, 0, 0, 1, 2];
        let mut bm_eu = DruidBitmap::new();
        bm_eu.insert(2);
        bm_eu.insert(3);
        let mut bm_jp = DruidBitmap::new();
        bm_jp.insert(4);
        let mut bm_us = DruidBitmap::new();
        bm_us.insert(0);
        bm_us.insert(1);
        bm_us.insert(5);
        let region_col = ColumnData::String(StringColumnData {
            dictionary: dict,
            encoded_values,
            bitmap_indexes: vec![bm_eu, bm_jp, bm_us],
        });

        let value_col = ColumnData::Double(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);

        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(timestamps));
        columns.insert("region".to_string(), region_col);
        columns.insert("value".to_string(), value_col);

        let start = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("parse")
            .timestamp_millis();
        let end = chrono::DateTime::parse_from_rfc3339("2024-01-03T00:00:00Z")
            .expect("parse")
            .timestamp_millis();

        SegmentData {
            version: 9,
            num_rows: 6,
            interval: ferrodruid_segment::Interval {
                start_millis: start,
                end_millis: end,
            },
            dimensions: vec!["region".to_string()],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: false,
        }
    }

    // -----------------------------------------------------------------------
    // JSON parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_timeseries_query_json() {
        let json_str = r#"{
            "queryType": "timeseries",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "day",
            "aggregations": [
                {"type":"count","name":"cnt"},
                {"type":"doubleSum","name":"total","fieldName":"value"}
            ]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse timeseries");
        assert!(matches!(query, DruidQuery::Timeseries(_)));

        // Re-serialize and parse again.
        let re_json = serde_json::to_string(&query).expect("serialize");
        let _: DruidQuery = serde_json::from_str(&re_json).expect("re-parse");
    }

    #[test]
    fn parse_topn_query_json() {
        let json_str = r#"{
            "queryType": "topN",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "all",
            "dimension": {"type":"default","dimension":"region","output_name":"region","output_type":"STRING"},
            "threshold": 3,
            "metric": {"type":"numeric","metric":"cnt"},
            "aggregations": [{"type":"count","name":"cnt"}]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse topN");
        assert!(matches!(query, DruidQuery::TopN(_)));
    }

    #[test]
    fn parse_groupby_query_json() {
        let json_str = r#"{
            "queryType": "groupBy",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "all",
            "dimensions": [{"type":"default","dimension":"region","output_name":"region","output_type":"STRING"}],
            "aggregations": [
                {"type":"count","name":"cnt"},
                {"type":"doubleSum","name":"total","fieldName":"value"}
            ]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse groupBy");
        assert!(matches!(query, DruidQuery::GroupBy(_)));
    }

    #[test]
    fn parse_scan_query_json() {
        let json_str = r#"{
            "queryType": "scan",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "columns": ["__time","region","value"],
            "limit": 10
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse scan");
        assert!(matches!(query, DruidQuery::Scan(_)));
    }

    #[test]
    fn parse_search_query_json() {
        let json_str = r#"{
            "queryType": "search",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "query": {"type":"contains","value":"us"},
            "searchDimensions": ["region"]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse search");
        assert!(matches!(query, DruidQuery::Search(_)));
    }

    #[test]
    fn parse_segment_metadata_query_json() {
        let json_str = r#"{
            "queryType": "segmentMetadata",
            "dataSource": {"type":"table","name":"wiki"}
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse segmentMetadata");
        assert!(matches!(query, DruidQuery::SegmentMetadata(_)));
    }

    #[test]
    fn parse_datasource_metadata_query_json() {
        let json_str = r#"{
            "queryType": "dataSourceMetadata",
            "dataSource": {"type":"table","name":"wiki"}
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse dataSourceMetadata");
        assert!(matches!(query, DruidQuery::DataSourceMetadata(_)));
    }

    #[test]
    fn parse_time_boundary_query_json() {
        let json_str = r#"{
            "queryType": "timeBoundary",
            "dataSource": {"type":"table","name":"wiki"}
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse timeBoundary");
        assert!(matches!(query, DruidQuery::TimeBoundary(_)));
    }

    // -----------------------------------------------------------------------
    // Execution tests
    // -----------------------------------------------------------------------

    #[test]
    fn exec_timeseries_count_and_sum() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "timeseries",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "day",
            "aggregations": [
                {"type":"count","name":"cnt"},
                {"type":"doubleSum","name":"total","fieldName":"value"}
            ]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::Timeseries(results) => {
                assert_eq!(results.len(), 2);
                // Day 1: 3 rows, sum=60
                assert_eq!(results[0].result.get("cnt"), Some(&json!(3)));
                assert_eq!(results[0].result.get("total"), Some(&json!(60.0)));
                // Day 2: 3 rows, sum=150
                assert_eq!(results[1].result.get("cnt"), Some(&json!(3)));
                assert_eq!(results[1].result.get("total"), Some(&json!(150.0)));
            }
            _ => panic!("expected timeseries result"),
        }
    }

    #[test]
    fn exec_timeseries_with_filter() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "timeseries",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "all",
            "filter": {"type":"selector","dimension":"region","value":"us"},
            "aggregations": [
                {"type":"count","name":"cnt"},
                {"type":"doubleSum","name":"total","fieldName":"value"}
            ]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::Timeseries(results) => {
                assert_eq!(results.len(), 1);
                // us rows: 10, 20, 60 = 3 rows, sum=90
                assert_eq!(results[0].result.get("cnt"), Some(&json!(3)));
                assert_eq!(results[0].result.get("total"), Some(&json!(90.0)));
            }
            _ => panic!("expected timeseries result"),
        }
    }

    #[test]
    fn exec_timeseries_descending() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "timeseries",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "day",
            "descending": true,
            "aggregations": [{"type":"count","name":"cnt"}]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::Timeseries(results) => {
                assert_eq!(results.len(), 2);
                // Descending: day2 first
                assert!(results[0].timestamp > results[1].timestamp);
            }
            _ => panic!("expected timeseries result"),
        }
    }

    #[test]
    fn exec_topn_top3_by_count() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "topN",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "all",
            "dimension": {"type":"default","dimension":"region","output_name":"region","output_type":"STRING"},
            "threshold": 3,
            "metric": {"type":"numeric","metric":"cnt"},
            "aggregations": [{"type":"count","name":"cnt"}]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::TopN(results) => {
                assert_eq!(results.len(), 1);
                let entries = &results[0].result;
                assert!(entries.len() <= 3);
                // us has 3 rows, eu has 2, jp has 1 → us first
                assert_eq!(entries[0].get("region"), Some(&json!("us")));
                assert_eq!(entries[0].get("cnt"), Some(&json!(3)));
            }
            _ => panic!("expected topN result"),
        }
    }

    #[test]
    fn exec_groupby_region() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "groupBy",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "all",
            "dimensions": [{"type":"default","dimension":"region","output_name":"region","output_type":"STRING"}],
            "aggregations": [
                {"type":"count","name":"cnt"},
                {"type":"doubleSum","name":"total","fieldName":"value"}
            ]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::GroupBy(results) => {
                // 3 groups: eu, jp, us
                assert_eq!(results.len(), 3);
                // Find "us" group.
                let us = results
                    .iter()
                    .find(|r| r.event.get("region") == Some(&json!("us")))
                    .expect("us group");
                assert_eq!(us.event.get("cnt"), Some(&json!(3)));
                assert_eq!(us.event.get("total"), Some(&json!(90.0)));
            }
            _ => panic!("expected groupBy result"),
        }
    }

    #[test]
    fn exec_groupby_with_having() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "groupBy",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "all",
            "dimensions": [{"type":"default","dimension":"region","output_name":"region","output_type":"STRING"}],
            "aggregations": [{"type":"count","name":"cnt"}],
            "having": {"type":"greaterThan","aggregation":"cnt","value":1.0}
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::GroupBy(results) => {
                // Only us (3) and eu (2) pass having > 1
                assert_eq!(results.len(), 2);
                assert!(
                    results.iter().all(|r| r
                        .event
                        .get("cnt")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0)
                        > 1)
                );
            }
            _ => panic!("expected groupBy result"),
        }
    }

    #[test]
    fn exec_scan_with_filter() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "scan",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "filter": {"type":"selector","dimension":"region","value":"eu"},
            "columns": ["region","value"],
            "limit": 10
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::Scan(scan) => {
                assert_eq!(scan.events.len(), 2);
                for event in &scan.events {
                    assert_eq!(event.get("region"), Some(&json!("eu")));
                }
            }
            _ => panic!("expected scan result"),
        }
    }

    #[test]
    fn exec_scan_with_limit_and_offset() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "scan",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "limit": 2,
            "offset": 1
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::Scan(scan) => {
                assert_eq!(scan.events.len(), 2);
            }
            _ => panic!("expected scan result"),
        }
    }

    #[test]
    fn exec_time_boundary() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "timeBoundary",
            "dataSource": {"type":"table","name":"wiki"}
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::TimeBoundary(results) => {
                assert_eq!(results.len(), 1);
                let r = &results[0].result;
                assert!(r.contains_key("minTime"));
                assert!(r.contains_key("maxTime"));
                let min = r.get("minTime").and_then(|v| v.as_str()).expect("minTime");
                let max = r.get("maxTime").and_then(|v| v.as_str()).expect("maxTime");
                assert!(min.starts_with("2024-01-01"));
                assert!(max.starts_with("2024-01-02"));
            }
            _ => panic!("expected timeBoundary result"),
        }
    }

    #[test]
    fn exec_time_boundary_max_only() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "timeBoundary",
            "dataSource": {"type":"table","name":"wiki"},
            "bound": "maxTime"
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::TimeBoundary(results) => {
                assert_eq!(results.len(), 1);
                assert!(results[0].result.contains_key("maxTime"));
                assert!(!results[0].result.contains_key("minTime"));
            }
            _ => panic!("expected timeBoundary result"),
        }
    }

    #[test]
    fn exec_segment_metadata() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "segmentMetadata",
            "dataSource": {"type":"table","name":"wiki"}
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::SegmentMetadata(results) => {
                assert_eq!(results.len(), 1);
                let meta = &results[0];
                assert_eq!(meta.num_rows, 6);
                assert!(meta.columns.contains_key("__time"));
                assert!(meta.columns.contains_key("region"));
                assert!(meta.columns.contains_key("value"));
                assert_eq!(
                    meta.columns.get("__time").map(|c| c.typ.as_str()),
                    Some("LONG")
                );
                assert_eq!(
                    meta.columns.get("region").map(|c| c.typ.as_str()),
                    Some("STRING")
                );
                assert_eq!(
                    meta.columns.get("value").map(|c| c.typ.as_str()),
                    Some("DOUBLE")
                );
            }
            _ => panic!("expected segmentMetadata result"),
        }
    }

    #[test]
    fn exec_datasource_metadata() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "dataSourceMetadata",
            "dataSource": {"type":"table","name":"wiki"}
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::DataSourceMetadata(results) => {
                assert_eq!(results.len(), 1);
                assert!(results[0].timestamp.starts_with("2024-01-02"));
            }
            _ => panic!("expected dataSourceMetadata result"),
        }
    }

    #[test]
    fn exec_search() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "search",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "query": {"type":"contains","value":"u"},
            "searchDimensions": ["region"]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::Search(results) => {
                assert!(!results.is_empty());
                // "us" and "eu" both contain "u"
                let all_values: Vec<&str> = results
                    .iter()
                    .flat_map(|r| r.result.iter().map(|h| h.value.as_str()))
                    .collect();
                assert!(all_values.contains(&"us"));
                assert!(all_values.contains(&"eu"));
            }
            _ => panic!("expected search result"),
        }
    }

    #[test]
    fn exec_timeseries_post_agg() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "timeseries",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "all",
            "aggregations": [
                {"type":"count","name":"cnt"},
                {"type":"doubleSum","name":"total","fieldName":"value"}
            ],
            "postAggregations": [
                {
                    "type": "arithmetic",
                    "name": "avg",
                    "fn": "/",
                    "fields": [
                        {"type":"fieldAccess","name":"t","fieldName":"total"},
                        {"type":"fieldAccess","name":"c","fieldName":"cnt"}
                    ]
                }
            ]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::Timeseries(results) => {
                assert_eq!(results.len(), 1);
                // total=210, cnt=6, avg=35.0
                let avg = results[0]
                    .result
                    .get("avg")
                    .and_then(|v| v.as_f64())
                    .expect("avg");
                assert!((avg - 35.0).abs() < 0.001);
            }
            _ => panic!("expected timeseries result"),
        }
    }

    #[test]
    fn exec_groupby_with_limit_spec() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "groupBy",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "all",
            "dimensions": [{"type":"default","dimension":"region","output_name":"region","output_type":"STRING"}],
            "aggregations": [{"type":"count","name":"cnt"}],
            "limitSpec": {
                "type": "default",
                "limit": 2,
                "columns": [{"dimension":"cnt","direction":"descending","dimensionOrder":"numeric"}]
            }
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::GroupBy(results) => {
                assert_eq!(results.len(), 2);
                // Sorted descending by cnt: us(3), eu(2)
                let first_cnt = results[0].event.get("cnt").and_then(|v| v.as_i64());
                let second_cnt = results[1].event.get("cnt").and_then(|v| v.as_i64());
                assert!(first_cnt >= second_cnt);
            }
            _ => panic!("expected groupBy result"),
        }
    }

    #[test]
    fn exec_scan_descending() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "scan",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "order": "descending",
            "columns": ["__time","region"]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::Scan(scan) => {
                assert_eq!(scan.events.len(), 6);
                // First row should be from day2
                let first_ts = scan.events[0]
                    .get("__time")
                    .and_then(|v| v.as_i64())
                    .expect("ts");
                let last_ts = scan.events[5]
                    .get("__time")
                    .and_then(|v| v.as_i64())
                    .expect("ts");
                assert!(first_ts >= last_ts);
            }
            _ => panic!("expected scan result"),
        }
    }

    #[test]
    fn exec_topn_dimension_ordering() {
        let segment = build_test_segment();
        let json_str = r#"{
            "queryType": "topN",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
            "granularity": "all",
            "dimension": {"type":"default","dimension":"region","output_name":"region","output_type":"STRING"},
            "threshold": 10,
            "metric": {"type":"dimension","ordering":"lexicographic"},
            "aggregations": [{"type":"count","name":"cnt"}]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");
        let result = execute_query(&query, &segment).expect("execute");
        match result {
            QueryResult::TopN(results) => {
                assert_eq!(results.len(), 1);
                let entries = &results[0].result;
                assert_eq!(entries.len(), 3);
                // Lexicographic: eu, jp, us
                assert_eq!(entries[0].get("region"), Some(&json!("eu")));
                assert_eq!(entries[1].get("region"), Some(&json!("jp")));
                assert_eq!(entries[2].get("region"), Some(&json!("us")));
            }
            _ => panic!("expected topN result"),
        }
    }

    // --- Native-wire sketch finalization (P1-#3) ---

    /// Build a raw-HLL timeseries query, optionally with a `finalize`
    /// context flag, plus a one-bucket result carrying the envelope.
    fn hll_query_and_result(finalize: Option<bool>) -> (DruidQuery, QueryResult) {
        let mut query_json = json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": ["2024-01-01/2024-01-02"],
            "granularity": "all",
            "aggregations": [{"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"}]
        });
        if let Some(f) = finalize {
            query_json["context"] = json!({ "finalize": f });
        }
        let query: DruidQuery = serde_json::from_value(query_json).expect("parse query");

        use ferrodruid_aggregator::Aggregator as _;
        let mut agg = ferrodruid_aggregator::HllSketchAggregator::build(14);
        for i in 0_u32..50 {
            agg.aggregate(Some(&json!(i)));
        }
        let mut map = serde_json::Map::new();
        map.insert("uu".to_string(), agg.get());
        let result = QueryResult::Timeseries(vec![TimeseriesResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: map,
        }]);
        (query, result)
    }

    fn ts_value(result: &QueryResult, name: &str) -> serde_json::Value {
        match result {
            QueryResult::Timeseries(entries) => {
                entries[0].result.get(name).cloned().expect("value")
            }
            _ => panic!("expected timeseries result"),
        }
    }

    #[test]
    fn wire_finalize_default_collapses_envelope_to_number() {
        let (query, mut result) = hll_query_and_result(None);
        finalize_native_wire_outputs(&query, &mut result);
        let v = ts_value(&result, "uu");
        let est = v
            .as_f64()
            .unwrap_or_else(|| panic!("expected number, got {v}"));
        assert!(
            (est - 50.0).abs() / 50.0 < 0.05,
            "estimate {est} not within 5% of 50"
        );
    }

    #[test]
    fn wire_finalize_true_explicit_collapses_envelope() {
        let (query, mut result) = hll_query_and_result(Some(true));
        finalize_native_wire_outputs(&query, &mut result);
        assert!(ts_value(&result, "uu").is_number());
    }

    #[test]
    fn wire_finalize_false_keeps_envelope() {
        let (query, mut result) = hll_query_and_result(Some(false));
        finalize_native_wire_outputs(&query, &mut result);
        let v = ts_value(&result, "uu");
        assert_eq!(
            v.get("@sketch").and_then(serde_json::Value::as_str),
            Some("hll"),
            "finalize=false must keep the intermediate envelope, got {v}"
        );
    }

    #[test]
    fn wire_finalize_ignores_non_agg_and_scan_results() {
        // A Scan result passes through untouched even under a groupBy-less
        // query with finalize defaulting to true.
        let query: DruidQuery = serde_json::from_value(json!({
            "queryType": "scan",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": ["2024-01-01/2024-01-02"]
        }))
        .expect("parse scan");
        let mut result = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["c".to_string()],
            events: Vec::new(),
        });
        finalize_native_wire_outputs(&query, &mut result);
        match result {
            QueryResult::Scan(s) => assert_eq!(s.columns, vec!["c".to_string()]),
            _ => panic!("scan result must remain scan"),
        }
    }
}
