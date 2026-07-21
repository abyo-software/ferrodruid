// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Multi-segment topN over SKETCH aggregations — the BROKER-fold twin of
//! the single-segment topN metric-rank fix.
//!
//! A sketch aggregation (`hyperUnique` over a decoded Druid column,
//! `thetaSketch`) is carried through per-shard topN partials as its
//! partial-state ENVELOPE — a JSON object `{"@sketch": …, "estimate": N,
//! …}` — not a bare number, so the broker can union the shards' sketches
//! exactly.  Its ranking value is the envelope's `estimate` (exactly how
//! the per-shard topN already ranks it).  Pre-fix,
//! `sort_topn_merged`'s Numeric arm read every merged cell via
//! `as_f64()`, so EVERY envelope ranked as `0.0` and the dimension
//! tiebreaker took over: a topN over a sketch aggregation across two or
//! more segments/shards (the exact v1.1.1 multi-segment bug class)
//! returned the alphabetically-first groups instead of the
//! highest-cardinality ones — even though the same query over one
//! segment was correct.
//!
//! Pinned here for hyperUnique AND theta through the REAL fold:
//! `Broker::execute_local` over two Historicals (2 partials →
//! `merge_topn` → `sort_topn_merged`), with one group (`ee`) split
//! ACROSS the shards so the winning rank exists only after the
//! cross-shard sketch union.

use std::collections::HashMap;

use ferrodruid_broker::Broker;
use ferrodruid_historical::Historical;
use ferrodruid_query::{DruidQuery, QueryResult};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::{ColumnData, DruidHyperUnique, StringColumnData, ThetaSketch};
use serde_json::json;

// ---------------------------------------------------------------------------
// Sketch fixtures (same builders as the query-side sketch ordering tests)
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

/// An exact-mode theta sketch of `count` distinct hashes starting at
/// `start * 1_000` (exact-mode theta estimates are EXACTLY the entry
/// count; distinct `start` ranges give DISJOINT hash sets so cross-shard
/// unions genuinely grow).
fn theta_of_range(start: u64, count: u64) -> ThetaSketch {
    let hashes: Vec<u64> = (start..start + count).map(|i| (i + 1) * 1_000).collect();
    ThetaSketch::from_druid_compact(&compact_theta(&hashes)).expect("decode synthetic theta")
}

/// Build a decoded Druid `hyperUnique` sketch whose register page carries
/// `num_page_bytes` bytes of `0x11` starting at `first_page_byte` — i.e.
/// `2 × num_page_bytes` non-zero registers (linear-counting regime:
/// estimates rise with the non-zero register count).  Disjoint page
/// ranges union (register-wise max) into strictly larger estimates.
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

// ---------------------------------------------------------------------------
// Segment / broker plumbing
// ---------------------------------------------------------------------------

/// One row per country with the given sketch metric column cells.
fn build_segment(countries: &[&str], metric_col: ColumnData) -> SegmentData {
    let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("parse base ts")
        .timestamp_millis();
    let num_rows = countries.len();
    let country_values: Vec<Option<String>> =
        countries.iter().map(|c| Some((*c).to_string())).collect();

    let mut columns = HashMap::new();
    #[allow(clippy::cast_possible_wrap)]
    columns.insert(
        "__time".to_string(),
        ColumnData::Long((0..num_rows).map(|i| base + (i as i64) * 60_000).collect()),
    );
    columns.insert(
        "country".to_string(),
        ColumnData::String(StringColumnData::from_nullable_values(&country_values)),
    );
    columns.insert("uu_col".to_string(), metric_col);

    SegmentData {
        version: 9,
        num_rows,
        interval: ferrodruid_segment::Interval {
            start_millis: base,
            end_millis: base + 86_400_000,
        },
        dimensions: vec!["country".to_string()],
        metrics: vec!["uu_col".to_string()],
        columns,
        time_sorted: true,
    }
}

fn topn_query(aggregation: serde_json::Value) -> DruidQuery {
    serde_json::from_value(json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "sketchy"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        "granularity": "all",
        "dimension": {"type": "default", "dimension": "country",
                      "outputName": "country", "outputType": "STRING"},
        "threshold": 3,
        "metric": {"type": "numeric", "metric": "uu"},
        "aggregations": [aggregation]
    }))
    .expect("query JSON must parse")
}

/// Load `seg_a` / `seg_b` into two Historicals and run the topN through
/// the real broker fold (`execute_local` over 2 shards → `merge_topn`).
/// Returns `(country, resolved uu estimate)` per merged result row, in
/// result order.
fn run_multiseg_topn(
    query: &DruidQuery,
    seg_a: SegmentData,
    seg_b: SegmentData,
) -> Vec<(String, f64)> {
    let dir_a = tempfile::tempdir().expect("tempdir a");
    let dir_b = tempfile::tempdir().expect("tempdir b");

    let hist_a = Historical::new(dir_a.path().to_path_buf(), 1_000_000);
    hist_a.load_segment("seg_a", seg_a).expect("load seg_a");
    hist_a
        .set_segment_datasource("seg_a", "sketchy")
        .expect("ds seg_a");

    let hist_b = Historical::new(dir_b.path().to_path_buf(), 1_000_000);
    hist_b.load_segment("seg_b", seg_b).expect("load seg_b");
    hist_b
        .set_segment_datasource("seg_b", "sketchy")
        .expect("ds seg_b");

    let broker = Broker::new();
    let merged = broker
        .execute_local(query, &[&hist_a, &hist_b])
        .expect("execute_local");

    let QueryResult::TopN(results) = merged.result else {
        panic!("expected topN result");
    };
    assert_eq!(results.len(), 1, "granularity=all → one time bucket");
    results[0]
        .result
        .iter()
        .map(|row| {
            let country = row
                .get("country")
                .and_then(|v| v.as_str())
                .expect("country key")
                .to_string();
            let uu = row.get("uu").expect("uu output");
            // Resolve the ranking value the way a client would: a bare
            // number, or a sketch envelope's `estimate`.
            let estimate = uu
                .as_f64()
                .or_else(|| uu.get("estimate").and_then(serde_json::Value::as_f64))
                .unwrap_or_else(|| panic!("uu output must resolve numerically, got {uu}"));
            (country, estimate)
        })
        .collect()
}

fn countries(rows: &[(String, f64)]) -> Vec<&str> {
    rows.iter().map(|(c, _)| c.as_str()).collect()
}

// ---------------------------------------------------------------------------
// Theta — broker fold must rank on the unioned estimates
// ---------------------------------------------------------------------------

/// Shard A holds {aa:4, bb:7, ee:3}, shard B holds {cc:2, dd:6, ee:5}
/// (ee's hash sets are DISJOINT across the shards, so its union is 8 and
/// it wins only after the cross-shard merge).  threshold=3 must return
/// the 3 highest-cardinality groups [ee=8, bb=7, dd=6] in estimate
/// order.  Pre-fix the broker ranked every envelope as 0.0 and the
/// dimension tiebreaker returned [aa, bb, cc].
#[test]
fn multiseg_topn_theta_ranks_by_unioned_estimate() {
    let seg_a = build_segment(
        &["aa", "bb", "ee"],
        ColumnData::ComplexTheta(vec![
            theta_of_range(0, 4),
            theta_of_range(100, 7),
            theta_of_range(200, 3),
        ]),
    );
    let seg_b = build_segment(
        &["cc", "dd", "ee"],
        ColumnData::ComplexTheta(vec![
            theta_of_range(300, 2),
            theta_of_range(400, 6),
            theta_of_range(500, 5),
        ]),
    );

    let query = topn_query(json!({
        "type": "thetaSketch", "name": "uu", "fieldName": "uu_col"
    }));
    let rows = run_multiseg_topn(&query, seg_a, seg_b);

    assert_eq!(
        countries(&rows),
        vec!["ee", "bb", "dd"],
        "multi-segment topN over thetaSketch must return the 3 highest \
         UNIONED cardinalities in estimate order (ee=3+5=8, bb=7, dd=6), \
         not the 0.0-ranked dimension-tiebreak order; got {rows:?}"
    );
    let estimates: Vec<f64> = rows.iter().map(|(_, e)| *e).collect();
    assert_eq!(
        estimates,
        vec![8.0, 7.0, 6.0],
        "exact-mode theta estimates are exact counts; ee must be the \
         cross-shard union (3 + 5 disjoint hashes = 8)"
    );
}

// ---------------------------------------------------------------------------
// hyperUnique — broker fold must rank on the register-union estimates
// ---------------------------------------------------------------------------

/// Same shape with decoded-Druid hyperUnique registers: shard A holds
/// {aa:10 regs, bb:20 regs, ee:8 regs}, shard B holds {cc:6 regs,
/// dd:18 regs, ee:16 regs} with ee on DISJOINT register pages, so its
/// register-wise-max union has 24 non-zero registers and the largest
/// estimate.  threshold=3 must return [ee, bb, dd] in estimate order.
#[test]
fn multiseg_topn_hyper_unique_ranks_by_unioned_estimate() {
    // Pin the fixture premise from the sketches themselves, so the
    // expected order can never silently drift: union(ee) > bb > dd >
    // aa > cc.
    let ee_union = hyper_unique_of(300, 4).merged(&hyper_unique_of(400, 8));
    let bb = hyper_unique_of(50, 10).estimate();
    let dd = hyper_unique_of(150, 9).estimate();
    let aa = hyper_unique_of(250, 5).estimate();
    let cc = hyper_unique_of(0, 3).estimate();
    assert!(ee_union.estimate() > bb, "premise: ee union ranks first");
    assert!(bb > dd, "premise: bb ranks second");
    assert!(dd > aa, "premise: dd ranks third");
    assert!(aa > cc, "premise: aa ranks fourth");

    let seg_a = build_segment(
        &["aa", "bb", "ee"],
        ColumnData::ComplexHyperUnique(vec![
            hyper_unique_of(250, 5),
            hyper_unique_of(50, 10),
            hyper_unique_of(300, 4),
        ]),
    );
    let seg_b = build_segment(
        &["cc", "dd", "ee"],
        ColumnData::ComplexHyperUnique(vec![
            hyper_unique_of(0, 3),
            hyper_unique_of(150, 9),
            hyper_unique_of(400, 8),
        ]),
    );

    let query = topn_query(json!({
        "type": "hyperUnique", "name": "uu", "fieldName": "uu_col"
    }));
    let rows = run_multiseg_topn(&query, seg_a, seg_b);

    assert_eq!(
        countries(&rows),
        vec!["ee", "bb", "dd"],
        "multi-segment topN over hyperUnique must return the 3 highest \
         register-union cardinalities in estimate order, not the \
         0.0-ranked dimension-tiebreak order; got {rows:?}"
    );
    // The winning estimate must BE the cross-shard union's estimate —
    // not either shard's partial.
    let (_, ee_estimate) = &rows[0];
    assert!(
        (ee_estimate - ee_union.estimate()).abs() < 1e-9,
        "ee must rank on its cross-shard register union \
         (expected {}, got {ee_estimate})",
        ee_union.estimate()
    );
}
