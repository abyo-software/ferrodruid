// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W2-C — `AVG(x)` / arithmetic-over-aggregates post-aggregation e2e tests.
//!
//! Each test parses a SQL query, plans it against a synthetic schema, and
//! executes the resulting native query against an in-memory `SegmentData`,
//! asserting the AVG / arithmetic output equals SUM/COUNT numerically in all
//! three native paths (Timeseries, GroupBy, TopN) plus the Druid-faithful
//! divide-by-zero → 0 semantics of the `/` arithmetic post-aggregator.

use ferrodruid_common::types::ColumnType;
use ferrodruid_query::DruidQuery;
use ferrodruid_query::topn::TopNMetricSpec;
use ferrodruid_segment::{SegmentData, SegmentDataBuilder};
use ferrodruid_sql::parser::parse_druid_sql;
use ferrodruid_sql::planner::{ColumnSchema, DataSourceSchema, plan_sql};

// ---------------------------------------------------------------------------
// Synthetic schema + segment
// ---------------------------------------------------------------------------

fn avg_schema() -> DataSourceSchema {
    DataSourceSchema {
        name: "t".to_string(),
        dimensions: vec![ColumnSchema {
            name: "site_id".to_string(),
            column_type: ColumnType::String,
        }],
        metrics: vec![
            ColumnSchema {
                name: "value".to_string(),
                column_type: ColumnType::Double,
            },
            ColumnSchema {
                name: "zero".to_string(),
                column_type: ColumnType::Double,
            },
        ],
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

/// 5 rows: site `a` has values 10, 20, 40 (avg 70/3); site `b` has 30, 50
/// (avg 40). `zero` is 0.0 everywhere so `SUM(zero)` drives a divide-by-zero.
fn build_segment() -> SegmentData {
    SegmentDataBuilder::new()
        .add_timestamp_column(vec![100, 200, 300, 400, 500])
        .add_string_column(
            "site_id",
            vec!["a".into(), "a".into(), "b".into(), "a".into(), "b".into()],
        )
        .add_double_column("value", true, vec![10.0, 20.0, 30.0, 40.0, 50.0])
        .add_double_column("zero", true, vec![0.0, 0.0, 0.0, 0.0, 0.0])
        .build()
        .expect("segment")
}

fn plan(sql: &str) -> DruidQuery {
    let stmt = parse_druid_sql(sql).expect("parse");
    let planned = plan_sql(&stmt, &avg_schema()).expect("plan");
    planned.native_query
}

fn as_f64(v: &serde_json::Value) -> f64 {
    v.as_f64()
        .unwrap_or_else(|| panic!("expected number, got {v:?}"))
}

// ---------------------------------------------------------------------------
// Timeseries path (no GROUP BY)
// ---------------------------------------------------------------------------

#[test]
fn avg_executes_in_timeseries_path() {
    let q = plan("SELECT AVG(value) AS avg_v FROM t");
    let DruidQuery::Timeseries(ts) = q else {
        panic!("expected Timeseries");
    };
    let rows = ts.execute(&build_segment()).expect("execute");
    assert_eq!(rows.len(), 1);
    // AVG(value) = (10+20+30+40+50) / 5 = 30
    let avg = as_f64(rows[0].result.get("avg_v").expect("avg_v present"));
    assert!((avg - 30.0).abs() < 1e-9, "expected 30.0, got {avg}");
}

// ---------------------------------------------------------------------------
// GroupBy path
// ---------------------------------------------------------------------------

#[test]
fn avg_executes_in_groupby_path() {
    let q = plan("SELECT site_id, AVG(value) AS avg_v FROM t GROUP BY site_id");
    let DruidQuery::GroupBy(gb) = q else {
        panic!("expected GroupBy");
    };
    let rows = gb.execute(&build_segment()).expect("execute");
    assert_eq!(rows.len(), 2);
    let mut by_site = std::collections::HashMap::new();
    for r in &rows {
        let site = r
            .event
            .get("site_id")
            .and_then(serde_json::Value::as_str)
            .expect("site_id")
            .to_string();
        let avg = as_f64(r.event.get("avg_v").expect("avg_v present"));
        by_site.insert(site, avg);
    }
    // a: (10+20+40)/3 = 23.333..., b: (30+50)/2 = 40
    let a = by_site.get("a").expect("site a");
    let b = by_site.get("b").expect("site b");
    assert!(
        (a - 70.0 / 3.0).abs() < 1e-9,
        "site a: expected 23.33.., got {a}"
    );
    assert!((b - 40.0).abs() < 1e-9, "site b: expected 40.0, got {b}");
}

// ---------------------------------------------------------------------------
// TopN path (single dim + ORDER BY avg alias + LIMIT)
// ---------------------------------------------------------------------------

#[test]
fn avg_executes_and_sorts_in_topn_path() {
    let q = plan(
        "SELECT site_id, AVG(value) AS avg_v FROM t GROUP BY site_id ORDER BY avg_v DESC LIMIT 2",
    );
    let DruidQuery::TopN(tq) = q else {
        panic!("expected TopN");
    };
    assert!(
        matches!(&tq.metric, TopNMetricSpec::Numeric { metric } if metric == "avg_v"),
        "TopN must rank by the AVG post-aggregation output, got {:?}",
        tq.metric
    );
    let rows = tq.execute(&build_segment()).expect("execute");
    assert_eq!(rows.len(), 1);
    let entries = &rows[0].result;
    assert_eq!(entries.len(), 2);
    // DESC by avg_v: b (40.0) first, a (23.33..) second.
    let first_site = entries[0]
        .get("site_id")
        .and_then(serde_json::Value::as_str);
    let second_site = entries[1]
        .get("site_id")
        .and_then(serde_json::Value::as_str);
    assert_eq!(first_site, Some("b"));
    assert_eq!(second_site, Some("a"));
    let first = as_f64(entries[0].get("avg_v").expect("avg_v"));
    let second = as_f64(entries[1].get("avg_v").expect("avg_v"));
    assert!((first - 40.0).abs() < 1e-9);
    assert!((second - 70.0 / 3.0).abs() < 1e-9);
}

// ---------------------------------------------------------------------------
// Explicit SUM/COUNT arithmetic (the silent-drop repro)
// ---------------------------------------------------------------------------

#[test]
fn sum_div_count_arithmetic_executes() {
    let q = plan("SELECT site_id, SUM(value) / COUNT(*) AS ratio FROM t GROUP BY site_id");
    let DruidQuery::GroupBy(gb) = q else {
        panic!("expected GroupBy");
    };
    let rows = gb.execute(&build_segment()).expect("execute");
    assert_eq!(rows.len(), 2);
    for r in &rows {
        let site = r
            .event
            .get("site_id")
            .and_then(serde_json::Value::as_str)
            .expect("site_id");
        let ratio = as_f64(
            r.event
                .get("ratio")
                .expect("ratio present — was silently dropped"),
        );
        let expected = if site == "a" { 70.0 / 3.0 } else { 40.0 };
        assert!(
            (ratio - expected).abs() < 1e-9,
            "site {site}: expected {expected}, got {ratio}"
        );
    }
}

// ---------------------------------------------------------------------------
// Divide-by-zero (Druid-faithful `/` semantics)
// ---------------------------------------------------------------------------

/// Druid's arithmetic post-aggregator documents that `fn: "/"` "always
/// returns 0 if dividing by 0, regardless of the numerator" — the executor's
/// `PostAggregatorSpec::evaluate` implements exactly that, short-circuiting
/// before a non-finite value (which would finalize to null) can arise.
#[test]
fn arithmetic_divide_by_zero_is_zero_druid_semantics() {
    let q = plan("SELECT site_id, SUM(value) / SUM(zero) AS dz FROM t GROUP BY site_id");
    let DruidQuery::GroupBy(gb) = q else {
        panic!("expected GroupBy");
    };
    let rows = gb.execute(&build_segment()).expect("execute");
    assert_eq!(rows.len(), 2);
    for r in &rows {
        let dz = r
            .event
            .get("dz")
            .expect("dz present — was silently dropped");
        assert_eq!(
            dz.as_f64(),
            Some(0.0),
            "Druid `/` post-agg returns 0 on division by zero, got {dz:?}"
        );
    }
}
