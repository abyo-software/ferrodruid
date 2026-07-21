// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-B legacy null mode — AVG lowering under the latch (H2 scoping fix).
//!
//! **This test binary latches the process-global legacy-null mode ON** in
//! every test; no ANSI-mode test may be added here (own-process latch).
//! The ANSI lowering (`expression` post-agg, 0/0 → None → SQL null) is
//! pinned by the in-crate planner unit test
//! `avg_lowers_to_expression_post_agg_in_timeseries`.
//!
//! Under the latch AVG must lower to an **arithmetic `/` post-agg**
//! (divide-by-zero → 0, Druid arithmetic post-agg semantics) so the
//! oracle empty-set answer (`empty_avg_y.json`: 0) comes from AVG's OWN
//! lowering — never from a global 0/0 → 0 exception inside the generic
//! expression evaluator, which would corrupt every legacy expression
//! containing 0/0 (e.g. `ROUND(SUM(x)/COUNT(*), 2)` over an empty match
//! must stay null).

use ferrodruid_aggregator::PostAggregatorSpec;
use ferrodruid_query::DruidQuery;
use ferrodruid_sql::planner::{ColumnSchema, DataSourceSchema};
use ferrodruid_sql::{parse_druid_sql, plan_sql};

fn latch_legacy() {
    assert!(
        ferrodruid_common::null_mode::init_legacy_null_mode(true),
        "this test binary requires the legacy-null latch"
    );
}

fn schema() -> DataSourceSchema {
    DataSourceSchema {
        name: "sales".to_string(),
        dimensions: vec![ColumnSchema {
            name: "city".to_string(),
            column_type: ferrodruid_common::types::ColumnType::String,
        }],
        metrics: vec![ColumnSchema {
            name: "revenue".to_string(),
            column_type: ferrodruid_common::types::ColumnType::Double,
        }],
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

/// Under the legacy latch `AVG(x)` finalises via
/// `arithmetic { fn: "/", fields: [fieldAccess(sum), fieldAccess(count)] }`
/// over the SAME hidden helpers as the ANSI lowering — the
/// divide-by-zero → 0 rule then lives in AVG's own post-agg, scoped away
/// from every other expression.
#[test]
fn legacy_avg_lowers_to_arithmetic_divide_post_agg() {
    latch_legacy();
    let stmt = parse_druid_sql("SELECT AVG(revenue) AS avg_r FROM sales").expect("parse");
    let planned = plan_sql(&stmt, &schema()).expect("AVG must plan");
    let DruidQuery::Timeseries(ref ts) = planned.native_query else {
        panic!("expected Timeseries, got {:?}", planned.native_query);
    };
    let post_aggs = ts.post_aggregations.as_ref().expect("post aggregations");
    assert_eq!(post_aggs.len(), 1);
    let PostAggregatorSpec::Arithmetic {
        name,
        fn_name,
        fields,
    } = &post_aggs[0]
    else {
        panic!(
            "legacy AVG must lower to the arithmetic `/` post-agg \
             (divide-by-zero → 0 scoped to AVG), got {:?}",
            post_aggs[0]
        );
    };
    assert_eq!(name, "avg_r");
    assert_eq!(fn_name, "/");
    let field_names: Vec<&str> = fields
        .iter()
        .map(|f| match f {
            PostAggregatorSpec::FieldAccess { field_name, .. } => field_name.as_str(),
            other => panic!("expected fieldAccess operands, got {other:?}"),
        })
        .collect();
    assert_eq!(field_names.len(), 2, "sum / count operands");
    assert!(
        field_names[0].starts_with("$avg_sum_"),
        "numerator must be the hidden AVG sum helper: {field_names:?}"
    );
    assert!(
        field_names[1].starts_with("$avg_count_"),
        "denominator must be the hidden not-null-filtered AVG count helper: {field_names:?}"
    );
}
