// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! SQL null-semantics e2e — plan through `plan_sql`, execute through
//! `ferrodruid_query::execute_query`, and assert the Druid-measured ground
//! truth (apache/druid 35.0.1 + 36.0.0, live 2026-07-11) for the 7-row
//! `nulltest` dataset:
//!
//! | site   | values          | non-null count | AVG   |
//! |--------|-----------------|----------------|-------|
//! | site_a | 10, 20, NULL    | 2              | 15.0  |
//! | site_b | 30, NULL, NULL  | 1              | 30.0  |
//! | site_c | NULL            | 0              | null  |
//!
//! `device_id` = d1,d2,d2,d1,NULL,d3,d3 → COUNT(DISTINCT device_id) = 3.
//!
//! Null representation note (same caveat as
//! `crates/ferrodruid-query/tests/filtered_sketch_postagg_e2e.rs`): the
//! segment format has no null bitmaps. String nulls are out-of-dictionary
//! ordinals; numeric nulls are NaN-encoded doubles — `column_value_at`
//! renders both as JSON `null`, which is exactly what the executor-level
//! aggregators see. Live ingestion of nulls lands in the parallel ingestion
//! task; the diff harness Section 7 re-verifies against real Druid after
//! both merge.

use std::collections::HashMap;

use ferrodruid_bitmap::DruidBitmap;
use ferrodruid_common::types::ColumnType;
use ferrodruid_dict::FrontCodedDictionary;
use ferrodruid_query::{QueryResult, execute_query};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::{ColumnData, StringColumnData};
use ferrodruid_sql::parser::parse_druid_sql;
use ferrodruid_sql::planner::{ColumnSchema, DataSourceSchema, plan_sql};
use serde_json::{Value, json};

/// Out-of-dictionary ordinal representing a null `device_id` row.
const NULL_ORD: u32 = 3;

fn nulltest_schema() -> DataSourceSchema {
    DataSourceSchema {
        name: "nulltest".to_string(),
        dimensions: vec![
            ColumnSchema {
                name: "site_id".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "device_id".to_string(),
                column_type: ColumnType::String,
            },
        ],
        metrics: vec![ColumnSchema {
            name: "value".to_string(),
            column_type: ColumnType::Double,
        }],
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

/// Build the 7-row nulltest segment mirror.
fn build_nulltest_segment() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("base ts")
        .timestamp_millis();
    let num_rows = 7usize;
    let timestamps: Vec<i64> = (0..num_rows as i64).map(|i| base + i * 3_600_000).collect();

    // site_id: a,a,a,b,b,b,c (all non-null).
    let site_ords: Vec<u32> = vec![0, 0, 0, 1, 1, 1, 2];
    let site_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec![
            "site_a".to_string(),
            "site_b".to_string(),
            "site_c".to_string(),
        ]),
        encoded_values: site_ords.clone(),
        bitmap_indexes: build_bitmaps(3, &site_ords),
    });

    // device_id: d1,d2,d2,d1,NULL,d3,d3.
    let device_ords: Vec<u32> = vec![0, 1, 1, 0, NULL_ORD, 2, 2];
    let device_col = ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(vec![
            "d1".to_string(),
            "d2".to_string(),
            "d3".to_string(),
        ]),
        encoded_values: device_ords.clone(),
        bitmap_indexes: build_bitmaps(3, &device_ords),
    });

    // value: 10, 20, NULL, 30, NULL, NULL, NULL — NaN encodes null
    // (`column_value_at` maps a NaN double to JSON `null`).
    let value_col = ColumnData::Double(vec![
        10.0,
        20.0,
        f64::NAN,
        30.0,
        f64::NAN,
        f64::NAN,
        f64::NAN,
    ]);

    let mut columns = HashMap::new();
    columns.insert("__time".to_string(), ColumnData::Long(timestamps));
    columns.insert("site_id".to_string(), site_col);
    columns.insert("device_id".to_string(), device_col);
    columns.insert("value".to_string(), value_col);

    SegmentData {
        version: 9,
        num_rows,
        interval: ferrodruid_segment::Interval {
            start_millis: base,
            end_millis: base + 86_400_000,
        },
        dimensions: vec!["site_id".to_string(), "device_id".to_string()],
        metrics: vec!["value".to_string()],
        columns,
        time_sorted: true,
    }
}

fn build_bitmaps(cardinality: usize, ordinals: &[u32]) -> Vec<DruidBitmap> {
    let mut bitmaps: Vec<DruidBitmap> = (0..cardinality).map(|_| DruidBitmap::new()).collect();
    for (row_idx, &ord) in ordinals.iter().enumerate() {
        if (ord as usize) < cardinality {
            bitmaps[ord as usize].insert(row_idx as u32);
        }
    }
    bitmaps
}

/// Plan + execute a SQL query against the nulltest segment.
fn run_sql(sql: &str) -> QueryResult {
    let stmt = parse_druid_sql(sql).unwrap_or_else(|e| panic!("parse failed for [{sql}]: {e}"));
    let planned = plan_sql(&stmt, &nulltest_schema())
        .unwrap_or_else(|e| panic!("plan failed for [{sql}]: {e}"));
    execute_query(&planned.native_query, &build_nulltest_segment())
        .unwrap_or_else(|e| panic!("execute failed for [{sql}]: {e}"))
}

/// Extract `(site_id, field)` pairs from a GroupBy result, sorted by site.
fn groupby_field_by_site(result: &QueryResult, field: &str) -> Vec<(String, Value)> {
    let QueryResult::GroupBy(rows) = result else {
        panic!("expected GroupBy result");
    };
    let mut out: Vec<(String, Value)> = rows
        .iter()
        .map(|r| {
            let site = r
                .event
                .get("site_id")
                .and_then(|v| v.as_str())
                .expect("site_id value")
                .to_string();
            let val = r.event.get(field).cloned().unwrap_or(Value::Null);
            (site, val)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

// ---------------------------------------------------------------------------
// Ground truth #1 — AVG skips nulls; all-null group → null
// ---------------------------------------------------------------------------

#[test]
fn avg_group_by_site_null_faithful() {
    let result = run_sql("SELECT site_id, AVG(\"value\") AS avg_v FROM nulltest GROUP BY site_id");
    assert_eq!(
        groupby_field_by_site(&result, "avg_v"),
        vec![
            ("site_a".to_string(), json!(15.0)),
            ("site_b".to_string(), json!(30.0)),
            ("site_c".to_string(), Value::Null),
        ],
        "AVG must divide by the non-null count and return null for an all-null group"
    );
}

// ---------------------------------------------------------------------------
// Ground truth #2 — COUNT(col) counts non-null rows only
// ---------------------------------------------------------------------------

#[test]
fn count_column_group_by_site_counts_non_null() {
    let result = run_sql("SELECT site_id, COUNT(\"value\") AS c FROM nulltest GROUP BY site_id");
    assert_eq!(
        groupby_field_by_site(&result, "c"),
        vec![
            ("site_a".to_string(), json!(2)),
            ("site_b".to_string(), json!(1)),
            ("site_c".to_string(), json!(0)),
        ],
        "COUNT(col) must count exactly the non-null rows per group"
    );
}

// ---------------------------------------------------------------------------
// Ground truth #3 — COUNT(DISTINCT) / APPROX_COUNT_DISTINCT skip nulls
// ---------------------------------------------------------------------------

#[test]
fn count_distinct_device_id_is_three() {
    for sql in [
        "SELECT COUNT(DISTINCT device_id) AS dc FROM nulltest",
        "SELECT APPROX_COUNT_DISTINCT(device_id) AS dc FROM nulltest",
    ] {
        let result = run_sql(sql);
        let QueryResult::Timeseries(rows) = result else {
            panic!("expected Timeseries result for {sql}");
        };
        assert_eq!(rows.len(), 1, "sql = {sql}");
        let dc = rows[0]
            .result
            .get("dc")
            .and_then(Value::as_f64)
            .unwrap_or_else(|| panic!("dc missing for {sql}: {:?}", rows[0].result));
        assert!(
            (dc - 3.0).abs() < 1e-9,
            "3 distinct non-null devices (nulls not counted), got {dc} for {sql}"
        );
        // Note: the NATIVE result map legitimately carries the hidden
        // `$hll_N` sketch aggregator — hidden helpers are stripped by the
        // REST layer's strict SQL projection (asserted on the wire in
        // `crates/ferrodruid-rest/tests/sql_null_semantics_e2e.rs`), not here.
    }
}

// ---------------------------------------------------------------------------
// Ground truth #4 — ROUND(AVG(x), 1) preserves null
// ---------------------------------------------------------------------------

#[test]
fn round_avg_group_by_site_preserves_null() {
    let result =
        run_sql("SELECT site_id, ROUND(AVG(\"value\"), 1) AS r FROM nulltest GROUP BY site_id");
    assert_eq!(
        groupby_field_by_site(&result, "r"),
        vec![
            ("site_a".to_string(), json!(15.0)),
            ("site_b".to_string(), json!(30.0)),
            ("site_c".to_string(), Value::Null),
        ],
        "ROUND over AVG must keep values and propagate the all-null-group null"
    );
}

// ---------------------------------------------------------------------------
// Plain arithmetic `/` keeps its Druid-documented divide-by-zero → 0 for
// NON-null operands, but a null aggregate operand short-circuits the whole
// arithmetic to null (Druid: arithmetic over a null aggregate is null).
// Since SUM over an all-null group is now null, site_c's numerator is null
// and the row is null BEFORE the divide-by-zero rule can apply.
// ---------------------------------------------------------------------------

#[test]
fn plain_arithmetic_divide_null_numerator_is_null_for_all_null_group() {
    // SUM("value")/COUNT("value"): site_c's SUM is null (all-null group), so
    // the arithmetic result is null — matching Druid 35 ground truth. The
    // divide-by-zero → 0 rule is untouched for non-null operands (pinned by
    // `avg_postagg_e2e.rs::arithmetic_divide_by_zero_is_zero_druid_semantics`).
    let result = run_sql(
        "SELECT site_id, SUM(\"value\") / COUNT(\"value\") AS r FROM nulltest GROUP BY site_id",
    );
    let rows = groupby_field_by_site(&result, "r");
    assert_eq!(rows[0], ("site_a".to_string(), json!(15.0)));
    assert_eq!(rows[1], ("site_b".to_string(), json!(30.0)));
    assert_eq!(
        rows[2],
        ("site_c".to_string(), Value::Null),
        "arithmetic over a null SUM must be null (Druid 35: null / 0 → null)"
    );
}
