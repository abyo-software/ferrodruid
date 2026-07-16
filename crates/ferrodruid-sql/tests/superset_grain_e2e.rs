// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Superset time-grain e2e — plan the EXACT SQL Superset 4.1.4's
//! `DruidEngineSpec` emits for its previously fail-closed grains through
//! `plan_sql`, execute through `ferrodruid_query::execute_query`, and assert
//! the bucketing:
//!
//! - `PT5S` / `PT30S` (and any single-field `PTnS`/`PTnM`/`PTnH` multiple)
//!   lower to an epoch-anchored fixed-period `duration` granularity.
//! - `week_starting_sunday` (nested `TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(...`)
//!   lowers to a 7-day `duration` granularity anchored on the Sunday before
//!   the epoch.
//!
//! `week_ending_saturday` stays fail-closed (its label is 6 days AFTER the
//! bucket start — not expressible as a floor granularity) and is pinned as
//! an error here.

use ferrodruid_query::{QueryResult, execute_query};
use ferrodruid_segment::{SegmentData, SegmentDataBuilder};
use ferrodruid_sql::parser::parse_druid_sql;
use ferrodruid_sql::planner::{ColumnSchema, DataSourceSchema, plan_sql};

use ferrodruid_common::types::ColumnType;

fn grains_schema() -> DataSourceSchema {
    DataSourceSchema {
        name: "grains".to_string(),
        dimensions: vec![ColumnSchema {
            name: "channel".to_string(),
            column_type: ColumnType::String,
        }],
        metrics: vec![ColumnSchema {
            name: "added".to_string(),
            column_type: ColumnType::Double,
        }],
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

fn iso_millis(s: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(s)
        .expect("parse ts")
        .timestamp_millis()
}

fn build_segment(timestamps: Vec<i64>) -> SegmentData {
    let n = timestamps.len();
    SegmentDataBuilder::new()
        .add_timestamp_column(timestamps)
        .add_double_column("added", true, vec![1.0; n])
        .add_string_column("channel", vec!["en".to_string(); n])
        .build()
        .expect("build segment")
}

fn run_sql(sql: &str, segment: &SegmentData) -> QueryResult {
    let stmt = parse_druid_sql(sql).unwrap_or_else(|e| panic!("parse failed for [{sql}]: {e}"));
    let planned = plan_sql(&stmt, &grains_schema())
        .unwrap_or_else(|e| panic!("plan failed for [{sql}]: {e}"));
    execute_query(&planned.native_query, segment)
        .unwrap_or_else(|e| panic!("execute failed for [{sql}]: {e}"))
}

fn timeseries_buckets(result: &QueryResult) -> Vec<(String, i64)> {
    let QueryResult::Timeseries(rows) = result else {
        panic!("expected Timeseries result, got {result:?}");
    };
    rows.iter()
        .map(|r| {
            let count = r
                .result
                .get("count")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_else(|| panic!("count missing in {r:?}"));
            (r.timestamp.clone(), count)
        })
        .collect()
}

/// Superset's `PT5S` grain, exact SQL shape (CAST wrapper, `__timestamp`
/// alias, positional GROUP BY): epoch-anchored 5-second buckets.
#[test]
fn superset_pt5s_grain_buckets_on_five_seconds() {
    let segment = build_segment(vec![
        iso_millis("2024-01-01T00:00:00Z"),
        iso_millis("2024-01-01T00:00:02Z"),
        iso_millis("2024-01-01T00:00:04Z"),
        iso_millis("2024-01-01T00:00:07Z"),
        iso_millis("2024-01-01T00:00:11Z"),
    ]);
    let result = run_sql(
        "SELECT TIME_FLOOR(CAST(__time AS TIMESTAMP), 'PT5S') AS __timestamp, \
         COUNT(*) AS \"count\" FROM grains GROUP BY 1 ORDER BY 1",
        &segment,
    );
    assert_eq!(
        timeseries_buckets(&result),
        vec![
            ("2024-01-01T00:00:00.000Z".to_string(), 3),
            ("2024-01-01T00:00:05.000Z".to_string(), 1),
            ("2024-01-01T00:00:10.000Z".to_string(), 1),
        ]
    );
}

/// Superset's `PT30S` grain: epoch-anchored 30-second buckets.
#[test]
fn superset_pt30s_grain_buckets_on_thirty_seconds() {
    let segment = build_segment(vec![
        iso_millis("2024-01-01T00:00:00Z"),
        iso_millis("2024-01-01T00:00:29Z"),
        iso_millis("2024-01-01T00:00:30Z"),
        iso_millis("2024-01-01T00:01:05Z"),
    ]);
    let result = run_sql(
        "SELECT TIME_FLOOR(CAST(__time AS TIMESTAMP), 'PT30S') AS __timestamp, \
         COUNT(*) AS \"count\" FROM grains GROUP BY 1 ORDER BY 1",
        &segment,
    );
    assert_eq!(
        timeseries_buckets(&result),
        vec![
            ("2024-01-01T00:00:00.000Z".to_string(), 2),
            ("2024-01-01T00:00:30.000Z".to_string(), 1),
            ("2024-01-01T00:01:00.000Z".to_string(), 1),
        ]
    );
}

/// Superset's `week_starting_sunday` grain, exact SQL shape: rows bucket
/// into `[Sunday, Sunday+7d)` labeled by the Sunday start. 2024-01-01 is a
/// Monday, so it belongs to the week starting Sunday 2023-12-31; the first
/// row of Sunday 2024-01-07 starts the next bucket.
#[test]
fn superset_week_starting_sunday_buckets_on_sundays() {
    let segment = build_segment(vec![
        iso_millis("2024-01-01T12:00:00Z"), // Monday
        iso_millis("2024-01-03T00:00:00Z"), // Wednesday
        iso_millis("2024-01-06T23:59:59Z"), // Saturday — still the first bucket
        iso_millis("2024-01-07T00:00:00Z"), // Sunday — next bucket
        iso_millis("2024-01-10T08:00:00Z"), // Wednesday
    ]);
    let result = run_sql(
        "SELECT TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(CAST(__time AS TIMESTAMP), 'P1D', 1), \
         'P1W'), 'P1D', -1) AS __timestamp, COUNT(*) AS \"count\" FROM grains \
         GROUP BY 1 ORDER BY 1",
        &segment,
    );
    assert_eq!(
        timeseries_buckets(&result),
        vec![
            ("2023-12-31T00:00:00.000Z".to_string(), 3),
            ("2024-01-07T00:00:00.000Z".to_string(), 2),
        ]
    );
}

/// Superset's `week_ending_saturday` grain has NO floor-based lowering (its
/// bucket label is the Saturday ENDING the bucket, 6 days after the bucket
/// start) — it must keep failing closed rather than silently mis-label.
#[test]
fn superset_week_ending_saturday_fails_closed() {
    let sql = "SELECT TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(CAST(__time AS TIMESTAMP), 'P1D', 1), \
               'P1W'), 'P1D', 5) AS __timestamp, COUNT(*) AS \"count\" FROM grains \
               GROUP BY 1 ORDER BY 1";
    let stmt = parse_druid_sql(sql).expect("parse");
    plan_sql(&stmt, &grains_schema()).expect_err("week_ending_saturday must fail closed");
}
