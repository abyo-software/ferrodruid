// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Query executor — dispatches a parsed [`DruidQuery`] against [`SegmentData`].

use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_segment::SegmentData;

use crate::{DruidQuery, QueryResult};

/// Execute a query with an optional timeout validation.
///
/// The actual wall-clock timeout is enforced at the HTTP layer via
/// `tokio::time::timeout`. This function validates that the requested timeout
/// value is reasonable and then delegates to [`execute_query`].
pub fn execute_query_with_timeout(
    query: &DruidQuery,
    segment: &SegmentData,
    timeout_ms: Option<u64>,
) -> Result<QueryResult> {
    // Validate the timeout is reasonable.
    if let Some(0) = timeout_ms {
        return Err(DruidError::Query("timeout must be > 0".into()));
    }
    execute_query(query, segment)
}

/// Execute a parsed Druid native query against a single segment.
///
/// This is the main entry point for query execution. It dispatches to the
/// appropriate query-type-specific executor and returns a unified [`QueryResult`].
pub fn execute_query(query: &DruidQuery, segment: &SegmentData) -> Result<QueryResult> {
    match query {
        DruidQuery::Timeseries(q) => {
            let results = q.execute(segment)?;
            Ok(QueryResult::Timeseries(results))
        }
        DruidQuery::TopN(q) => {
            let results = q.execute(segment)?;
            Ok(QueryResult::TopN(results))
        }
        DruidQuery::GroupBy(q) => {
            let results = q.execute(segment)?;
            Ok(QueryResult::GroupBy(results))
        }
        DruidQuery::Scan(q) => {
            let result = q.execute(segment)?;
            Ok(QueryResult::Scan(result))
        }
        DruidQuery::Search(q) => {
            let results = q.execute(segment)?;
            Ok(QueryResult::Search(results))
        }
        DruidQuery::SegmentMetadata(q) => {
            let results = q.execute(segment)?;
            Ok(QueryResult::SegmentMetadata(results))
        }
        DruidQuery::DataSourceMetadata(q) => {
            let results = q.execute(segment)?;
            Ok(QueryResult::DataSourceMetadata(results))
        }
        DruidQuery::TimeBoundary(q) => {
            let results = q.execute(segment)?;
            Ok(QueryResult::TimeBoundary(results))
        }
        DruidQuery::Window(q) => {
            let result = q.execute(segment)?;
            Ok(QueryResult::Scan(result))
        }
        DruidQuery::UnionAll(queries) => {
            // DD R49: concatenate ALL sub-query Scan results. The previous code
            // ran every branch but returned only the first, silently dropping all
            // later branches (`A UNION ALL B` returned only A's rows). Events are
            // name-keyed row maps, so concatenation is order-independent; the
            // column list is the first-seen union across branches.
            let mut merged: Option<crate::scan::ScanResult> = None;
            for sub_query in queries {
                // DD R50: this executor can only concatenate Scan-shaped branches.
                // A branch that plans to a non-Scan result (e.g. an aggregate
                // SELECT COUNT(*) -> Timeseries) was previously SILENTLY DROPPED,
                // so `agg UNION ALL agg` returned empty. Fail closed instead.
                match execute_query(sub_query, segment)? {
                    // A scan carries its DECLARED columns even when it matches
                    // no rows, so the first branch (empty or not) establishes
                    // the target column names; later branches are aligned to
                    // it. Druid names the union output from the first branch
                    // and maps later branches POSITIONALLY into it.
                    QueryResult::Scan(mut result) => match merged.as_mut() {
                        None => merged = Some(result),
                        Some(acc) => {
                            crate::scan::align_union_branch(&acc.columns, &mut result)?;
                            acc.events.extend(result.events);
                        }
                    },
                    _ => {
                        return Err(DruidError::Query(
                            "UNION ALL is only supported over scan-shaped branches; \
                             aggregate/groupBy/topN branches are not supported"
                                .to_owned(),
                        ));
                    }
                }
            }
            Ok(QueryResult::Scan(merged.unwrap_or_else(|| {
                crate::scan::ScanResult {
                    segment_id: None,
                    columns: Vec::new(),
                    events: Vec::new(),
                }
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_all_names_output_from_first_branch_even_when_it_is_empty() {
        // A scan carries its declared columns even with no matching rows, so an
        // empty FIRST branch must still name the union output. Branch 1 (empty,
        // declares `code`) UNION branch 2 (rows, declares `__time`): the output
        // must be one column `code` holding branch 2's `__time` values.
        let n = 3usize;
        let mut columns = std::collections::HashMap::new();
        columns.insert(
            "__time".to_string(),
            ferrodruid_segment::column::ColumnData::Long(
                (0..n as i64).map(|i| (i + 1) * 100).collect(),
            ),
        );
        columns.insert(
            "code".to_string(),
            ferrodruid_segment::column::ColumnData::Long((0..n as i64).collect()),
        );
        let seg = ferrodruid_segment::SegmentData {
            version: 9,
            num_rows: n,
            interval: ferrodruid_segment::Interval {
                start_millis: 0,
                end_millis: (n as i64 + 1) * 100,
            },
            dimensions: vec!["code".to_string()],
            metrics: vec![],
            columns,
            time_sorted: false,
        };
        // Branch 1 declares `code` but its interval matches no rows.
        let empty_first: DruidQuery = serde_json::from_str(
            r#"{"queryType":"scan","dataSource":{"type":"table","name":"wiki"},
                "intervals":["2100-01-01/2200-01-01"],"columns":["code"]}"#,
        )
        .expect("parse empty branch");
        // Branch 2 declares `__time` and matches the data.
        let populated: DruidQuery = serde_json::from_str(
            r#"{"queryType":"scan","dataSource":{"type":"table","name":"wiki"},
                "intervals":["1970-01-01/2100-01-01"],"columns":["__time"]}"#,
        )
        .expect("parse populated branch");
        let union = DruidQuery::UnionAll(vec![empty_first, populated]);
        let QueryResult::Scan(res) = execute_query(&union, &seg).expect("exec") else {
            panic!("expected a Scan result");
        };
        assert_eq!(
            res.columns,
            vec!["code".to_string()],
            "output must be named from the first branch even when it is empty"
        );
        assert_eq!(
            res.events.len(),
            n,
            "the populated branch's rows must survive"
        );
        assert!(
            res.events
                .iter()
                .all(|e| e.contains_key("code") && !e.contains_key("__time")),
            "branch 2's `__time` must be remapped to the first branch's `code`"
        );
    }

    #[test]
    fn union_all_concatenates_all_branches() {
        // DD R49: `A UNION ALL B` must return the rows of EVERY branch, not just
        // the first (the executor previously returned only scan_results.next()).
        let n = 3usize;
        let mut columns = std::collections::HashMap::new();
        columns.insert(
            "__time".to_string(),
            ferrodruid_segment::column::ColumnData::Long(
                (0..n as i64).map(|i| (i + 1) * 100).collect(),
            ),
        );
        columns.insert(
            "code".to_string(),
            ferrodruid_segment::column::ColumnData::Long((0..n as i64).collect()),
        );
        let seg = ferrodruid_segment::SegmentData {
            version: 9,
            num_rows: n,
            interval: ferrodruid_segment::Interval {
                start_millis: 0,
                end_millis: (n as i64 + 1) * 100,
            },
            dimensions: vec!["code".to_string()],
            metrics: vec![],
            columns,
            time_sorted: false,
        };
        let scan_json = r#"{
            "queryType":"scan",
            "dataSource":{"type":"table","name":"wiki"},
            "intervals":["1970-01-01/2100-01-01"],
            "columns":["code"]
        }"#;
        let sub: DruidQuery = serde_json::from_str(scan_json).expect("parse");
        let union = DruidQuery::UnionAll(vec![sub.clone(), sub]);
        let QueryResult::Scan(res) = execute_query(&union, &seg).expect("exec") else {
            panic!("expected a Scan result");
        };
        assert_eq!(
            res.events.len(),
            2 * n,
            "UNION ALL of two branches must concatenate all rows, got {}",
            res.events.len()
        );
    }

    #[test]
    fn union_all_non_scan_branch_fails_closed() {
        // DD R50: an aggregate branch (plans to Timeseries, not Scan) in a
        // UNION ALL must fail closed, not be silently dropped (returning empty).
        let seg = ferrodruid_segment::SegmentData {
            version: 9,
            num_rows: 0,
            interval: ferrodruid_segment::Interval {
                start_millis: 0,
                end_millis: 1,
            },
            dimensions: Vec::new(),
            metrics: Vec::new(),
            columns: std::collections::HashMap::new(),
            time_sorted: false,
        };
        let ts_json = r#"{
            "queryType":"timeseries",
            "dataSource":{"type":"table","name":"wiki"},
            "intervals":["1970-01-01/2100-01-01"],
            "granularity":"all",
            "aggregations":[{"type":"count","name":"cnt"}]
        }"#;
        let ts: DruidQuery = serde_json::from_str(ts_json).expect("parse");
        let union = DruidQuery::UnionAll(vec![ts.clone(), ts]);
        assert!(
            execute_query(&union, &seg).is_err(),
            "a non-scan UNION ALL branch must be rejected, not silently dropped"
        );
    }

    #[test]
    fn timeout_zero_is_error() {
        // Build a minimal scan query.
        let json_str = r#"{
            "queryType": "scan",
            "dataSource": {"type":"table","name":"wiki"},
            "intervals": ["2024-01-01/2024-01-02"],
            "columns": ["__time"]
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");

        // Need a segment — use a minimal one.
        let seg = ferrodruid_segment::SegmentData {
            version: 9,
            num_rows: 0,
            interval: ferrodruid_segment::Interval {
                start_millis: 0,
                end_millis: 1,
            },
            dimensions: Vec::new(),
            metrics: Vec::new(),
            columns: std::collections::HashMap::new(),
            time_sorted: false,
        };

        let err = execute_query_with_timeout(&query, &seg, Some(0)).unwrap_err();
        assert!(err.to_string().contains("timeout must be > 0"));
    }

    #[test]
    fn timeout_none_delegates_normally() {
        let json_str = r#"{
            "queryType": "segmentMetadata",
            "dataSource": {"type":"table","name":"wiki"}
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");

        let seg = ferrodruid_segment::SegmentData {
            version: 9,
            num_rows: 0,
            interval: ferrodruid_segment::Interval {
                start_millis: 0,
                end_millis: 1,
            },
            dimensions: Vec::new(),
            metrics: Vec::new(),
            columns: std::collections::HashMap::new(),
            time_sorted: false,
        };

        let result = execute_query_with_timeout(&query, &seg, None);
        assert!(result.is_ok());
    }

    #[test]
    fn timeout_positive_delegates_normally() {
        let json_str = r#"{
            "queryType": "segmentMetadata",
            "dataSource": {"type":"table","name":"wiki"}
        }"#;
        let query: DruidQuery = serde_json::from_str(json_str).expect("parse");

        let seg = ferrodruid_segment::SegmentData {
            version: 9,
            num_rows: 0,
            interval: ferrodruid_segment::Interval {
                start_millis: 0,
                end_millis: 1,
            },
            dimensions: Vec::new(),
            metrics: Vec::new(),
            columns: std::collections::HashMap::new(),
            time_sorted: false,
        };

        let result = execute_query_with_timeout(&query, &seg, Some(5000));
        assert!(result.is_ok());
    }
}
