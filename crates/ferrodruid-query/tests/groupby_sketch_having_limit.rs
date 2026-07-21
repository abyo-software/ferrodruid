// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! GroupBy HAVING + limitSpec over SKETCH aggregations (the symmetric-path
//! twin of the topN metric-rank fix).
//!
//! A sketch aggregation (`hyperUnique` over a decoded Druid column,
//! `thetaSketch`) is carried through groupBy result rows as its
//! partial-state ENVELOPE — a JSON object `{"@sketch": …, "estimate": N,
//! …}` — not a bare number.  Its comparison/ordering value is the
//! envelope's `estimate` (exactly how topN already ranks it).  Pre-fix:
//!
//! * **H1 — HAVING**: `HavingSpec::matches` compared via `as_f64()` only,
//!   so EVERY `HAVING <sketch agg> <op> <threshold>` failed and valid
//!   groups were silently removed (all rows dropped).
//! * **H2 — limitSpec ordering**: the numeric comparator ranked every
//!   envelope as `0.0` and the lexicographic fallback stringified the
//!   envelope JSON, so `ORDER BY <sketch agg> DESC LIMIT k` selected the
//!   WRONG groups (whatever the secondary order / envelope-bytes order
//!   happened to be).
//!
//! Both are pinned here for hyperUnique AND theta, multi-group (4 groups
//! each), asserting the exact retained/selected groups and their order.

use std::collections::HashMap;

use ferrodruid_query::{DruidQuery, QueryResult, execute_query};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::{ColumnData, DruidHyperUnique, StringColumnData, ThetaSketch};
use serde_json::json;

// ---------------------------------------------------------------------------
// Segment builders
// ---------------------------------------------------------------------------

/// Build a DataSketches compact Theta image (little-endian, preLongs 2,
/// exact mode): the on-disk per-row form of a Druid `thetaSketch` metric.
/// Hashes must be strictly ascending.
fn compact_theta(hashes: &[u64]) -> Vec<u8> {
    let mut buf = vec![2u8, 3, 3, 12, 13, 0, 0x1E, 0x93];
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    buf.extend_from_slice(&(hashes.len() as i32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    for &h in hashes {
        buf.extend_from_slice(&h.to_le_bytes());
    }
    buf
}

/// An exact-mode theta sketch of `count` distinct hashes (exact-mode theta
/// estimates are EXACTLY the entry count).
fn theta_of(count: u64) -> ThetaSketch {
    let hashes: Vec<u64> = (1..=count).map(|i| i * 1_000).collect();
    ThetaSketch::from_druid_compact(&compact_theta(&hashes)).expect("decode synthetic theta")
}

/// Build a decoded Druid `hyperUnique` sketch whose register page carries
/// `page_bytes.len()` bytes of `0x11` starting at `first_page_byte` — i.e.
/// `2 × len` non-zero registers (each `0x11` page byte sets both of its
/// nibble registers to 1).  The blob is the dense on-disk form
/// `DruidHyperUnique::from_druid_blob` accepts.
fn hyper_unique_of(first_page_byte: usize, num_page_bytes: usize) -> DruidHyperUnique {
    let non_zero_registers = 2 * num_page_bytes;
    #[allow(clippy::cast_possible_truncation)]
    let declared = non_zero_registers as u16;
    let mut blob = Vec::with_capacity(7 + 1024);
    blob.push(0x01); // version
    blob.push(0x00); // register offset (only 0 supported)
    blob.extend_from_slice(&declared.to_be_bytes()); // declared non-zero count
    blob.extend_from_slice(&[0, 0, 0]); // no overflow
    let mut page = [0u8; 1024];
    for b in page.iter_mut().skip(first_page_byte).take(num_page_bytes) {
        *b = 0x11;
    }
    blob.extend_from_slice(&page);
    DruidHyperUnique::from_druid_blob(&blob).expect("decode synthetic hyperUnique blob")
}

/// One row per country.  `country_rows` supplies `(country, metric column
/// cell)` pairs in row order; `metric_name` names the sketch column.
fn build_segment(country_rows: Vec<(&str, ColumnData)>) -> SegmentData {
    // `country_rows` carries one single-row ColumnData PER ROW only for
    // call-site readability; splice them into one column here.
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("parse base ts")
        .timestamp_millis();
    let num_rows = country_rows.len();
    let countries: Vec<Option<String>> = country_rows
        .iter()
        .map(|(c, _)| Some((*c).to_string()))
        .collect();
    let mut theta_rows: Vec<ThetaSketch> = Vec::new();
    let mut hu_rows: Vec<DruidHyperUnique> = Vec::new();
    for (_, cell) in country_rows {
        match cell {
            ColumnData::ComplexTheta(mut rows) => theta_rows.append(&mut rows),
            ColumnData::ComplexHyperUnique(mut rows) => hu_rows.append(&mut rows),
            other => panic!("unsupported cell column: {other:?}"),
        }
    }
    let (metric_name, metric_col) = if theta_rows.is_empty() {
        assert_eq!(hu_rows.len(), num_rows);
        ("uu_col", ColumnData::ComplexHyperUnique(hu_rows))
    } else {
        assert_eq!(theta_rows.len(), num_rows);
        ("uu_col", ColumnData::ComplexTheta(theta_rows))
    };

    let mut columns = HashMap::new();
    #[allow(clippy::cast_possible_wrap)]
    columns.insert(
        "__time".to_string(),
        ColumnData::Long((0..num_rows).map(|i| base + (i as i64) * 60_000).collect()),
    );
    columns.insert(
        "country".to_string(),
        ColumnData::String(StringColumnData::from_nullable_values(&countries)),
    );
    columns.insert(metric_name.to_string(), metric_col);

    SegmentData {
        version: 9,
        num_rows,
        interval: ferrodruid_segment::Interval {
            start_millis: base,
            end_millis: base + 86_400_000,
        },
        dimensions: vec!["country".to_string()],
        metrics: vec![metric_name.to_string()],
        columns,
        time_sorted: true,
    }
}

/// 4 theta groups with EXACT estimates: bb=12, dd=7, aa=4, cc=2.
/// Row order (== country lex order aa,bb,cc,dd is deliberately DIFFERENT
/// from estimate order) so a comparator that degrades to the secondary /
/// insertion order picks provably wrong groups.
fn theta_segment() -> SegmentData {
    build_segment(vec![
        ("aa", ColumnData::ComplexTheta(vec![theta_of(4)])),
        ("bb", ColumnData::ComplexTheta(vec![theta_of(12)])),
        ("cc", ColumnData::ComplexTheta(vec![theta_of(2)])),
        ("dd", ColumnData::ComplexTheta(vec![theta_of(7)])),
    ])
}

/// 4 hyperUnique groups.  Estimates rise with the non-zero register count
/// (linear-counting regime): bb (40 regs) > dd (26) > aa (10) > cc (6).
/// The register POSITIONS are arranged so the serialized-envelope BYTES
/// order (cc first at page byte 0, then bb at 50, dd at 150, aa at 250)
/// INVERTS the estimate order — a comparator that stringifies the envelope
/// JSON picks provably wrong groups.
fn hyper_unique_segment() -> SegmentData {
    build_segment(vec![
        (
            "aa",
            ColumnData::ComplexHyperUnique(vec![hyper_unique_of(250, 5)]),
        ),
        (
            "bb",
            ColumnData::ComplexHyperUnique(vec![hyper_unique_of(50, 20)]),
        ),
        (
            "cc",
            ColumnData::ComplexHyperUnique(vec![hyper_unique_of(0, 3)]),
        ),
        (
            "dd",
            ColumnData::ComplexHyperUnique(vec![hyper_unique_of(150, 13)]),
        ),
    ])
}

// ---------------------------------------------------------------------------
// Query plumbing
// ---------------------------------------------------------------------------

fn parse_query(v: serde_json::Value) -> DruidQuery {
    serde_json::from_value(v).expect("query JSON must parse")
}

/// Execute a groupBy and return `(country, uu envelope)` per result row, in
/// result order.  Also asserts every `uu` output IS a sketch envelope, so
/// these tests can never silently pass by comparing bare numbers.
fn run_groupby(
    query: serde_json::Value,
    segment: &SegmentData,
) -> Vec<(String, serde_json::Value)> {
    let query = parse_query(query);
    let QueryResult::GroupBy(results) = execute_query(&query, segment).expect("execute") else {
        panic!("expected groupBy result");
    };
    results
        .iter()
        .map(|r| {
            let country = r
                .event
                .get("country")
                .and_then(|v| v.as_str())
                .expect("country key")
                .to_string();
            let uu = r.event.get("uu").expect("uu output").clone();
            assert!(
                uu.get("@sketch").is_some(),
                "test premise: the uu aggregation output must be a sketch \
                 partial-state envelope, got {uu}"
            );
            (country, uu)
        })
        .collect()
}

fn countries(rows: &[(String, serde_json::Value)]) -> Vec<&str> {
    rows.iter().map(|(c, _)| c.as_str()).collect()
}

fn base_query(aggregation: serde_json::Value) -> serde_json::Value {
    json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "sketchy"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        "granularity": "all",
        "dimensions": [
            {"type": "default", "dimension": "country", "outputName": "country",
             "outputType": "STRING"}
        ],
        "aggregations": [aggregation]
    })
}

fn theta_agg() -> serde_json::Value {
    json!({"type": "thetaSketch", "name": "uu", "fieldName": "uu_col"})
}

fn hyper_unique_agg() -> serde_json::Value {
    json!({"type": "hyperUnique", "name": "uu", "fieldName": "uu_col"})
}

// ---------------------------------------------------------------------------
// H1 — HAVING over a sketch aggregation
// ---------------------------------------------------------------------------

/// `HAVING uu > 5` over theta estimates {aa:4, bb:12, cc:2, dd:7} must
/// retain EXACTLY {bb, dd}.  Pre-fix: `as_f64()` on the envelope is None,
/// every comparison fails, ALL groups are silently dropped.
#[test]
fn having_greater_than_theta_estimate_retains_exceeding_groups() {
    let mut query = base_query(theta_agg());
    query["having"] = json!({"type": "greaterThan", "aggregation": "uu", "value": 5.0});
    let rows = run_groupby(query, &theta_segment());
    let mut got = countries(&rows);
    got.sort_unstable();
    assert_eq!(
        got,
        vec!["bb", "dd"],
        "HAVING uu > 5 must keep exactly the groups whose theta ESTIMATE \
         exceeds 5 (bb=12, dd=7), not drop/keep groups by envelope-vs-number \
         confusion"
    );
}

/// `HAVING uu < 5` over the same theta groups retains exactly {aa, cc}.
#[test]
fn having_less_than_theta_estimate_retains_lower_groups() {
    let mut query = base_query(theta_agg());
    query["having"] = json!({"type": "lessThan", "aggregation": "uu", "value": 5.0});
    let rows = run_groupby(query, &theta_segment());
    let mut got = countries(&rows);
    got.sort_unstable();
    assert_eq!(
        got,
        vec!["aa", "cc"],
        "HAVING uu < 5 must keep aa=4 and cc=2"
    );
}

/// `HAVING uu > 15` over hyperUnique estimates (bb≈40.4, dd≈26.2, aa≈10.0,
/// cc≈6.0) must retain EXACTLY {bb, dd}.  The threshold separations are
/// asserted from the decoded sketches themselves so the fixture can never
/// silently drift.
#[test]
fn having_greater_than_hyper_unique_estimate_retains_exceeding_groups() {
    // Pin the fixture premise: estimates straddle the 15.0 threshold.
    assert!(
        hyper_unique_of(50, 20).estimate() > 15.0,
        "bb above threshold"
    );
    assert!(
        hyper_unique_of(150, 13).estimate() > 15.0,
        "dd above threshold"
    );
    assert!(
        hyper_unique_of(250, 5).estimate() < 15.0,
        "aa below threshold"
    );
    assert!(
        hyper_unique_of(0, 3).estimate() < 15.0,
        "cc below threshold"
    );

    let mut query = base_query(hyper_unique_agg());
    query["having"] = json!({"type": "greaterThan", "aggregation": "uu", "value": 15.0});
    let rows = run_groupby(query, &hyper_unique_segment());
    let mut got = countries(&rows);
    got.sort_unstable();
    assert_eq!(
        got,
        vec!["bb", "dd"],
        "HAVING uu > 15 must compare 15 against each group's hyperUnique \
         ESTIMATE and keep exactly bb/dd"
    );
}

/// Composite having (AND of two sketch comparisons) resolves the estimate
/// in every leaf: `uu > 5 AND NOT(uu > 10)` keeps exactly {dd} (7).
#[test]
fn having_composite_over_theta_estimate() {
    let mut query = base_query(theta_agg());
    query["having"] = json!({
        "type": "and",
        "havingSpecs": [
            {"type": "greaterThan", "aggregation": "uu", "value": 5.0},
            {"type": "not",
             "havingSpec": {"type": "greaterThan", "aggregation": "uu", "value": 10.0}}
        ]
    });
    let rows = run_groupby(query, &theta_segment());
    assert_eq!(countries(&rows), vec!["dd"], "5 < uu <= 10 keeps only dd=7");
}

// ---------------------------------------------------------------------------
// H2 — limitSpec ordering over a sketch aggregation
// ---------------------------------------------------------------------------

/// `ORDER BY uu DESC (numeric), country ASC LIMIT 2` over theta estimates
/// must return [bb=12, dd=7] in that order.  Pre-fix the numeric
/// comparator ranked every envelope as 0.0, the country tiebreaker took
/// over, and the limit selected [aa, bb] — the wrong groups.
#[test]
fn limit_spec_numeric_desc_theta_selects_highest_estimates() {
    let mut query = base_query(theta_agg());
    query["limitSpec"] = json!({
        "type": "default",
        "limit": 2,
        "columns": [
            {"dimension": "uu", "direction": "descending", "dimensionOrder": "numeric"},
            {"dimension": "country", "direction": "ascending",
             "dimensionOrder": "lexicographic"}
        ]
    });
    let rows = run_groupby(query, &theta_segment());
    assert_eq!(
        countries(&rows),
        vec!["bb", "dd"],
        "ORDER BY uu DESC LIMIT 2 must pick the two highest theta estimates \
         (bb=12, dd=7) in descending-estimate order"
    );
}

/// Ascending numeric ordering is the mirror: LIMIT 2 returns [cc=2, aa=4].
#[test]
fn limit_spec_numeric_asc_theta_selects_lowest_estimates() {
    let mut query = base_query(theta_agg());
    query["limitSpec"] = json!({
        "type": "default",
        "limit": 2,
        "columns": [
            {"dimension": "uu", "direction": "ascending", "dimensionOrder": "numeric"},
            {"dimension": "country", "direction": "ascending",
             "dimensionOrder": "lexicographic"}
        ]
    });
    let rows = run_groupby(query, &theta_segment());
    assert_eq!(
        countries(&rows),
        vec!["cc", "aa"],
        "ORDER BY uu ASC LIMIT 2 must pick the two lowest theta estimates"
    );
}

/// `ORDER BY uu DESC (numeric), country ASC LIMIT 2` over hyperUnique
/// estimates must return [bb≈40.4, dd≈26.2] in that order.
#[test]
fn limit_spec_numeric_desc_hyper_unique_selects_highest_estimates() {
    let mut query = base_query(hyper_unique_agg());
    query["limitSpec"] = json!({
        "type": "default",
        "limit": 2,
        "columns": [
            {"dimension": "uu", "direction": "descending", "dimensionOrder": "numeric"},
            {"dimension": "country", "direction": "ascending",
             "dimensionOrder": "lexicographic"}
        ]
    });
    let rows = run_groupby(query, &hyper_unique_segment());
    assert_eq!(
        countries(&rows),
        vec!["bb", "dd"],
        "ORDER BY uu DESC LIMIT 2 must pick the two highest hyperUnique \
         estimates in descending-estimate order"
    );
}

/// The same DESC/LIMIT over hyperUnique WITHOUT `dimensionOrder: numeric`
/// (the default lexicographic branch).  A sketch cell must STILL order by
/// its estimate.  Pre-fix the lex fallback stringified the envelope JSON,
/// ordering by serialized register bytes — the fixture's register
/// positions make that order [cc, bb, dd, aa], so the pre-fix limit
/// selected [cc, bb]: provably wrong groups, deterministically.
#[test]
fn limit_spec_default_order_hyper_unique_still_orders_by_estimate() {
    let mut query = base_query(hyper_unique_agg());
    query["limitSpec"] = json!({
        "type": "default",
        "limit": 2,
        "columns": [
            {"dimension": "uu", "direction": "descending"}
        ]
    });
    let rows = run_groupby(query, &hyper_unique_segment());
    assert_eq!(
        countries(&rows),
        vec!["bb", "dd"],
        "a sketch aggregation must order by its ESTIMATE even without \
         dimensionOrder=numeric (never by stringified envelope JSON)"
    );
}

/// HAVING and limitSpec combined end-to-end: keep uu > 5 then take the
/// single highest — exactly [bb].
#[test]
fn having_plus_limit_spec_compose_over_theta() {
    let mut query = base_query(theta_agg());
    query["having"] = json!({"type": "greaterThan", "aggregation": "uu", "value": 5.0});
    query["limitSpec"] = json!({
        "type": "default",
        "limit": 1,
        "columns": [
            {"dimension": "uu", "direction": "descending", "dimensionOrder": "numeric"}
        ]
    });
    let rows = run_groupby(query, &theta_segment());
    assert_eq!(countries(&rows), vec!["bb"]);
}
