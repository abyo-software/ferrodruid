// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Query-bridge tests for a migrated Druid `thetaSketch` complex column
//! (compat-8 sketch #2).
//!
//! A rollup datasource migrated from Apache Druid stores one DataSketches
//! compact Theta image per row.  The segment reader decodes those into a
//! [`ColumnData::ComplexTheta`] column of union-only Druid-origin sketches;
//! this file proves the QUERY side: `column_value_at` renders each row as
//! the theta partial-state envelope, the `thetaSketch` aggregator unions
//! the rows, and the resulting cardinalities are exact for small sets —
//! per-country US=2 / JP=2, total=4, deliberately the same constants the
//! live `sketch_rollup_day` Druid oracle produces (per-country uu US=2,
//! JP=2; total=4), so this test and the real-Druid A→B harness assert the
//! same numbers.
//!
//! The fixture also pins that unioning is a genuine set-union: the two US
//! rows share a user hash, so a sum-not-union bug would report US=3 and
//! total=5.

use std::collections::HashMap;

use ferrodruid_query::{DruidQuery, QueryResult, execute_query};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::{ColumnData, StringColumnData, ThetaSketch};
use serde_json::json;

/// Build a DataSketches compact Theta image (little-endian, preLongs 2,
/// exact mode): the on-disk per-row form of a Druid `thetaSketch` metric.
fn compact_theta(hashes: &[u64]) -> Vec<u8> {
    let mut buf = vec![2u8, 3, 3, 12, 13, 0, 0x1E, 0x93];
    buf.extend_from_slice(&(hashes.len() as i32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    for &h in hashes {
        buf.extend_from_slice(&h.to_le_bytes());
    }
    buf
}

/// A 3-row "migrated rollup segment": two US rows whose user sets overlap
/// (h2 appears in both) and one JP row.  Distinct users: US {h1,h2} = 2,
/// JP {h3,h4} = 2, total = 4.
fn build_theta_segment() -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("parse base ts")
        .timestamp_millis();
    let (h1, h2, h3, h4) = (1_000u64, 2_000, 3_000, 4_000);
    let rows = vec![
        ThetaSketch::from_druid_compact(&compact_theta(&[h1, h2])).expect("decode US row 1"),
        ThetaSketch::from_druid_compact(&compact_theta(&[h2])).expect("decode US row 2"),
        ThetaSketch::from_druid_compact(&compact_theta(&[h3, h4])).expect("decode JP row"),
    ];
    let country = StringColumnData::from_nullable_values(&[
        Some("US".to_string()),
        Some("US".to_string()),
        Some("JP".to_string()),
    ]);

    let mut columns = HashMap::new();
    columns.insert(
        "__time".to_string(),
        ColumnData::Long(vec![base, base + 60_000, base + 120_000]),
    );
    columns.insert("country".to_string(), ColumnData::String(country));
    columns.insert("users_theta".to_string(), ColumnData::ComplexTheta(rows));

    SegmentData {
        version: 9,
        num_rows: 3,
        interval: ferrodruid_segment::Interval {
            start_millis: base,
            end_millis: base + 86_400_000,
        },
        dimensions: vec!["country".to_string()],
        metrics: vec!["users_theta".to_string()],
        columns,
        time_sorted: true,
    }
}

fn parse_query(v: serde_json::Value) -> DruidQuery {
    serde_json::from_value(v).expect("query JSON must parse")
}

/// Pull the theta envelope's convenience `estimate` field out of a result
/// map entry.
fn theta_estimate(map: &serde_json::Map<String, serde_json::Value>, name: &str) -> f64 {
    let envelope = map.get(name).unwrap_or_else(|| panic!("missing {name}"));
    assert_eq!(
        envelope.get("@sketch").and_then(|v| v.as_str()),
        Some("theta"),
        "aggregation output must be the theta partial-state envelope, got {envelope}"
    );
    envelope
        .get("estimate")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_else(|| panic!("envelope has no estimate: {envelope}"))
}

/// Total distinct users over the whole segment: the timeseries `thetaSketch`
/// aggregation must UNION the three per-row sketches → exactly 4 (a
/// sum-of-rows bug would report 5).
#[test]
fn timeseries_theta_over_migrated_column_unions_rows() {
    let segment = build_theta_segment();
    let query = parse_query(json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "sketch_rollup_day"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        "granularity": "all",
        "aggregations": [
            {"type": "thetaSketch", "name": "uu", "fieldName": "users_theta"}
        ]
    }));
    let QueryResult::Timeseries(results) = execute_query(&query, &segment).expect("execute") else {
        panic!("expected timeseries result");
    };
    assert_eq!(results.len(), 1);
    let est = theta_estimate(&results[0].result, "uu");
    assert!(
        (est - 4.0).abs() < f64::EPSILON,
        "total distinct users must be exactly 4 (union, not sum), got {est}"
    );

    // SQL-path parity: finalizing the same envelope yields the same scalar
    // Druid's `APPROX_COUNT_DISTINCT_DS_THETA` reports.
    let spec: ferrodruid_aggregator::AggregatorSpec = serde_json::from_value(json!(
        {"type": "thetaSketch", "name": "uu", "fieldName": "users_theta"}
    ))
    .expect("agg spec");
    let finalized = ferrodruid_aggregator::finalize_sketch_json(
        &spec,
        results[0].result.get("uu").expect("uu envelope"),
    )
    .expect("finalize");
    assert_eq!(finalized.as_f64(), Some(4.0));
}

/// Per-country distinct users via groupBy: US must union its two rows'
/// overlapping sketches to exactly 2 (not 3), JP is 2 — the same
/// per-country constants the live `sketch_rollup_day` Druid oracle
/// produces.
#[test]
fn groupby_theta_per_country_matches_druid_oracle_constants() {
    let segment = build_theta_segment();
    let query = parse_query(json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "sketch_rollup_day"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        "granularity": "all",
        "dimensions": [
            {"type": "default", "dimension": "country", "outputName": "country",
             "outputType": "STRING"}
        ],
        "aggregations": [
            {"type": "thetaSketch", "name": "uu", "fieldName": "users_theta"}
        ]
    }));
    let QueryResult::GroupBy(results) = execute_query(&query, &segment).expect("execute") else {
        panic!("expected groupBy result");
    };
    assert_eq!(results.len(), 2, "two countries");
    let mut seen = HashMap::new();
    for r in &results {
        let country = r
            .event
            .get("country")
            .and_then(|v| v.as_str())
            .expect("country key")
            .to_string();
        let map: serde_json::Map<String, serde_json::Value> = r.event.clone().into_iter().collect();
        seen.insert(country, theta_estimate(&map, "uu"));
    }
    assert_eq!(
        seen.get("US").copied(),
        Some(2.0),
        "US uu (union of 2 overlapping rows)"
    );
    assert_eq!(seen.get("JP").copied(), Some(2.0), "JP uu");
}

/// A raw value hitting the SAME aggregator after it absorbed Druid-origin
/// sketches must NOT corrupt the estimate by mixing hash spaces: the
/// incompatible raw value is dropped fail-soft (documented union-only
/// limitation of migrated theta columns).
#[test]
fn theta_aggregator_never_mixes_hash_spaces() {
    use ferrodruid_aggregator::{Aggregator, ThetaSketchAggregator, theta_sketch_envelope};

    let druid = ThetaSketch::from_druid_compact(&compact_theta(&[10, 20])).expect("decode");
    let mut agg = ThetaSketchAggregator::new(4096);
    agg.aggregate(Some(&theta_sketch_envelope(&druid)));
    assert!((agg.estimate() - 2.0).abs() < f64::EPSILON);
    // A raw (FNV-hashed) value arrives — mixing would poison the Druid
    // hash space, so it is refused and the estimate stays 2.
    agg.aggregate(Some(&json!("some raw user id")));
    assert!(
        (agg.estimate() - 2.0).abs() < f64::EPSILON,
        "raw values must never mix into a Druid-origin union (got {})",
        agg.estimate()
    );
}
