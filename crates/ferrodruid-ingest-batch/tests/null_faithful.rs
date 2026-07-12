// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Null-faithful batch ingestion — failing-test-first battery for
//! Defects A/B/C/D (measured 2026-07-11 against apache/druid:35.0.1,
//! default null handling; Defect D measured against Druid 36).
//!
//! Ground-truth dataset (7 rows):
//!
//! | row | site   | value | device_id |
//! |-----|--------|-------|-----------|
//! | 0   | site_a | 10    | d1        |
//! | 1   | site_a | 20    | d2        |
//! | 2   | site_a | null  | d2        |
//! | 3   | site_b | 30    | d1        |
//! | 4   | site_b | null  | null      |
//! | 5   | site_b | null  | d3        |
//! | 6   | site_c | null  | d3        |
//!
//! Druid 35 behavior (measured):
//! * Scan returns `"value":null` / `"device_id":null` for null inputs.
//! * `SUM("value")` per site → 30 / 30 / null.
//! * `COUNT("value")` → 2 / 1 / 0. `AVG` → 15.0 / 30.0 / null.

use ferrodruid_aggregator::{Aggregator, DoubleSumAggregator};
use ferrodruid_bitmap::DruidBitmap;
use ferrodruid_ingest_batch::{
    BatchIngester, DimensionSchema, DimensionType, parse_dimension_entries,
};
use ferrodruid_query::scan::ScanQuery;
use ferrodruid_segment::ColumnData;
use ferrodruid_segment::segment::SegmentData;

/// The 7-row ground-truth dataset with strictly increasing timestamps so
/// post-sort row order is deterministic and matches the table above.
fn ground_truth_rows() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"__time": 1000, "site": "site_a", "value": 10.0, "device_id": "d1"}),
        serde_json::json!({"__time": 2000, "site": "site_a", "value": 20.0, "device_id": "d2"}),
        serde_json::json!({"__time": 3000, "site": "site_a", "value": null, "device_id": "d2"}),
        serde_json::json!({"__time": 4000, "site": "site_b", "value": 30.0, "device_id": "d1"}),
        serde_json::json!({"__time": 5000, "site": "site_b", "value": null, "device_id": null}),
        serde_json::json!({"__time": 6000, "site": "site_b", "value": null, "device_id": "d3"}),
        serde_json::json!({"__time": 7000, "site": "site_c", "value": null, "device_id": "d3"}),
    ]
}

fn double_col<'a>(seg: &'a SegmentData, name: &str) -> &'a [f64] {
    match seg.column(name) {
        Some(ColumnData::Double(v)) => v,
        other => panic!("expected DOUBLE column for `{name}`, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Defect B — metric nulls must not be 0-filled (rollup=false)
// ---------------------------------------------------------------------------

/// With `metricsSpec: [{"type":"doubleSum","name":"value","fieldName":"value"}]`
/// and rollup=false, a null input must be stored as NULL (NaN in the
/// in-memory DOUBLE column), not 0-filled.
#[test]
fn defect_b_metric_nulls_stored_as_null_not_zero() {
    let ingester = BatchIngester::new(
        "gt".into(),
        "__time".into(),
        vec!["site".into(), "device_id".into()],
        vec![serde_json::json!({
            "type": "doubleSum", "name": "value", "fieldName": "value"
        })],
    );
    let seg = ingester
        .ingest(ground_truth_rows())
        .expect("ingest")
        .segment_data;

    let vals = double_col(&seg, "value");
    assert_eq!(vals.len(), 7);
    // Non-null inputs keep their values.
    assert_eq!(vals[0], 10.0);
    assert_eq!(vals[1], 20.0);
    assert_eq!(vals[3], 30.0);
    // Null inputs must be stored as NULL (NaN), NOT 0.0 (Defect B).
    for idx in [2usize, 4, 5, 6] {
        assert!(
            vals[idx].is_nan(),
            "row {idx}: null metric input must be stored as NULL (NaN), got {}",
            vals[idx]
        );
    }
}

// ---------------------------------------------------------------------------
// Defect C — string-dimension nulls must stay distinct from ""
// ---------------------------------------------------------------------------

/// A null string-dimension input must be stored as null — distinct from the
/// empty string — via the trailing null-row bitmap on the string column
/// (`bitmap_indexes.len() == dictionary.len() + 1`, last bitmap = null rows).
#[test]
fn defect_c_string_dim_null_distinct_from_empty() {
    let ingester = BatchIngester::new(
        "gt".into(),
        "__time".into(),
        vec!["site".into(), "device_id".into()],
        vec![],
    );
    let seg = ingester
        .ingest(ground_truth_rows())
        .expect("ingest")
        .segment_data;

    let sc = match seg.column("device_id") {
        Some(ColumnData::String(sc)) => sc,
        other => panic!("expected STRING column, got {other:?}"),
    };

    // The segment must know row 4's device_id is null: one *extra* trailing
    // bitmap past the dictionary cardinality holds the null rows.
    assert_eq!(
        sc.bitmap_indexes.len(),
        sc.dictionary.len() + 1,
        "null-bearing string column must carry a trailing null-row bitmap \
         (Defect C: today null is silently mapped to \"\")"
    );
    let null_bm: &DruidBitmap = sc
        .bitmap_indexes
        .last()
        .expect("trailing bitmap must exist");
    assert!(null_bm.contains(4), "row 4 device_id is null");
    assert_eq!(null_bm.len(), 1, "exactly one null device_id row");

    // The dictionary entry the null rows point at must NOT claim the null
    // row in its own value bitmap (a selector filter on that value must not
    // match null rows).
    let ord_row4 = sc.encoded_values[4] as usize;
    assert!(
        (ord_row4) < sc.dictionary.len(),
        "null rows must encode to an in-range ordinal (dense groupBy safety)"
    );
    assert!(
        !sc.bitmap_indexes[ord_row4].contains(4),
        "the dictionary-value bitmap must exclude null rows"
    );

    // No real "" value exists in this dataset, so no non-null row may claim
    // the "" placeholder entry.
    if let Some(empty_ord) = (0..sc.dictionary.len()).find(|&i| sc.dictionary.get(i) == Some("")) {
        assert!(
            sc.bitmap_indexes[empty_ord].is_empty(),
            "\"\" placeholder bitmap must be empty when no real \"\" rows exist"
        );
    }

    // site column has no nulls: representation must be unchanged
    // (backward-compatible: no trailing bitmap).
    let site = match seg.column("site") {
        Some(ColumnData::String(sc)) => sc,
        other => panic!("expected STRING column, got {other:?}"),
    };
    assert_eq!(site.bitmap_indexes.len(), site.dictionary.len());
}

// ---------------------------------------------------------------------------
// Defect D — `count`-type metric must count raw rows, not 0-fill
// ---------------------------------------------------------------------------

/// With `metricsSpec: [{"type":"count","name":"count"}]` and rollup=false,
/// every stored row aggregates exactly one raw row, so the stored `count`
/// must be 1 for every row (Druid 36 measured behavior) — not 0.
#[test]
fn defect_d_count_metric_is_one_per_raw_row() {
    let ingester = BatchIngester::new(
        "gt".into(),
        "__time".into(),
        vec!["site".into()],
        vec![serde_json::json!({"type": "count", "name": "count"})],
    );
    // Note: raw rows do NOT contain a "count" field — Druid's count
    // aggregator has no fieldName and counts input rows.
    let seg = ingester
        .ingest(ground_truth_rows())
        .expect("ingest")
        .segment_data;

    match seg.column("count") {
        Some(ColumnData::Long(v)) => {
            assert_eq!(v.len(), 7);
            assert!(
                v.iter().all(|&c| c == 1),
                "count metric must store 1 per raw row at rollup=false, got {v:?}"
            );
        }
        other => panic!("count metric must be a LONG column of 1s, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Defect A — typed dimension entries must produce numeric columns
// ---------------------------------------------------------------------------

/// `dimensionsSpec.dimensions` object entries with `{"type":"double"}` must
/// produce a numeric DOUBLE column (with nulls preserved as NaN), not a
/// dictionary-encoded string column of `"10.0"` strings.
///
/// Failing-first note: this defect is only expressible through the new
/// typed-schema API (the legacy `BatchIngester::new` path takes bare names
/// and cannot carry a type), so its RED state was a compile failure rather
/// than an assert failure — the pre-fix ingester had no typed-dimension
/// support at all and ingested `value` as a STRING dimension (measured live:
/// scan showed `"value":"10.0"`).
#[test]
fn defect_a_typed_double_dimension_is_numeric() {
    let schemas = parse_dimension_entries(&[
        serde_json::json!("site"),
        serde_json::json!("device_id"),
        serde_json::json!({"type": "double", "name": "value"}),
    ])
    .expect("parse dimensionsSpec entries");
    assert_eq!(schemas[0].dim_type, DimensionType::String);
    assert_eq!(schemas[1].dim_type, DimensionType::String);
    assert_eq!(schemas[2].dim_type, DimensionType::Double);

    let ingester = BatchIngester::with_schemas("gt".into(), "__time".into(), schemas, vec![]);
    let seg = ingester
        .ingest(ground_truth_rows())
        .expect("ingest")
        .segment_data;

    assert_eq!(
        seg.dimensions,
        vec!["site", "device_id", "value"],
        "typed dimension must be registered as a dimension"
    );
    let vals = double_col(&seg, "value");
    assert_eq!(vals[0], 10.0);
    assert_eq!(vals[1], 20.0);
    assert_eq!(vals[3], 30.0);
    for idx in [2usize, 4, 5, 6] {
        assert!(vals[idx].is_nan(), "row {idx} must be NULL (NaN)");
    }
}

/// Typed `long` dimensions: a null-free batch stores a true LONG column; a
/// null-bearing batch falls back to DOUBLE with NaN nulls (documented — an
/// `i64` has no in-band NULL marker).
#[test]
fn typed_long_dimension_null_handling() {
    let schemas = vec![DimensionSchema::new("code", DimensionType::Long)];

    // Null-free: LONG.
    let ingester =
        BatchIngester::with_schemas("gt".into(), "__time".into(), schemas.clone(), vec![]);
    let rows = vec![
        serde_json::json!({"__time": 1000, "code": 7}),
        serde_json::json!({"__time": 2000, "code": -3}),
    ];
    let seg = ingester.ingest(rows).expect("ingest").segment_data;
    match seg.column("code") {
        Some(ColumnData::Long(v)) => assert_eq!(v, &[7, -3]),
        other => panic!("null-free long dim must be LONG, got {other:?}"),
    }

    // Null-bearing: DOUBLE fallback with NaN.
    let ingester = BatchIngester::with_schemas("gt".into(), "__time".into(), schemas, vec![]);
    let rows = vec![
        serde_json::json!({"__time": 1000, "code": 7}),
        serde_json::json!({"__time": 2000, "code": null}),
    ];
    let seg = ingester.ingest(rows).expect("ingest").segment_data;
    match seg.column("code") {
        Some(ColumnData::Double(v)) => {
            assert_eq!(v[0], 7.0);
            assert!(v[1].is_nan(), "null long input must be NULL (NaN)");
        }
        other => panic!("null-bearing long dim must fall back to DOUBLE, got {other:?}"),
    }
}

/// Unsupported / malformed dimension entries are rejected.
#[test]
fn parse_dimension_entries_rejects_bad_shapes() {
    let err = parse_dimension_entries(&[serde_json::json!({"type": "double"})]).unwrap_err();
    assert!(err.to_string().contains("name"), "missing name: {err}");

    let err =
        parse_dimension_entries(&[serde_json::json!({"type": "nested", "name": "x"})]).unwrap_err();
    assert!(
        err.to_string().contains("unsupported dimension type"),
        "unsupported type: {err}"
    );

    let err = parse_dimension_entries(&[serde_json::json!(42)]).unwrap_err();
    assert!(
        err.to_string().contains("must be a string or an object"),
        "bad entry: {err}"
    );

    // `type` defaults to string when absent.
    let schemas = parse_dimension_entries(&[serde_json::json!({"name": "plain"})]).expect("parse");
    assert_eq!(schemas[0].dim_type, DimensionType::String);
}

// ---------------------------------------------------------------------------
// Read-path verification: scan renders stored numeric nulls as JSON null,
// and the aggregator layer skips them in sums.
// ---------------------------------------------------------------------------

fn scan_all(seg: &SegmentData) -> Vec<std::collections::HashMap<String, serde_json::Value>> {
    let query: ScanQuery = serde_json::from_value(serde_json::json!({
        "queryType": "scan",
        "dataSource": "gt",
        "intervals": ["1970-01-01T00:00:00.000Z/2000-01-01T00:00:00.000Z"],
        "resultFormat": "list"
    }))
    .expect("scan query json");
    query.execute(seg).expect("scan execute").events
}

/// The real Scan read path must render stored numeric NULLs as JSON null
/// (Druid 35: `"value":null`) and non-null values as numbers — with the
/// query crate completely unmodified.
#[test]
fn scan_renders_stored_numeric_nulls_as_json_null() {
    let schemas = parse_dimension_entries(&[
        serde_json::json!("site"),
        serde_json::json!("device_id"),
        serde_json::json!({"type": "double", "name": "value"}),
    ])
    .expect("parse");
    let ingester = BatchIngester::with_schemas(
        "gt".into(),
        "__time".into(),
        schemas,
        vec![serde_json::json!({"type": "count", "name": "count"})],
    );
    let seg = ingester
        .ingest(ground_truth_rows())
        .expect("ingest")
        .segment_data;

    let events = scan_all(&seg);
    assert_eq!(events.len(), 7);

    // Rows are time-ordered (ts 1000..7000) — same order as the fixture.
    assert_eq!(events[0]["value"], serde_json::json!(10.0));
    assert_eq!(events[1]["value"], serde_json::json!(20.0));
    assert_eq!(events[3]["value"], serde_json::json!(30.0));
    for idx in [2usize, 4, 5, 6] {
        assert_eq!(
            events[idx]["value"],
            serde_json::Value::Null,
            "row {idx}: stored numeric NULL must scan as JSON null"
        );
    }
    // Defect D end-to-end: count metric scans as 1 on every row.
    for (idx, ev) in events.iter().enumerate() {
        assert_eq!(
            ev["count"],
            serde_json::json!(1),
            "row {idx}: count metric must scan as 1"
        );
    }
    // Known read-side gap (out-of-scope query crate, reported): string
    // dimension NULL still renders as "" in scan because the row renderer
    // resolves every in-range ordinal through the dictionary.  The segment
    // itself DOES know the row is null:
    match seg.column("device_id") {
        Some(ColumnData::String(sc)) => {
            assert!(
                sc.is_null_row(4),
                "segment must know device_id row 4 is null"
            );
            assert!(!sc.is_null_row(0));
        }
        other => panic!("expected STRING column, got {other:?}"),
    }
}

/// The aggregator layer must skip stored NULLs in sums: per-site
/// `SUM(value)` computed by feeding real scan output through the real
/// `DoubleSumAggregator` yields 30 / 30 / null — the aggregator itself now
/// tracks whether any non-null input contributed and reports SQL null for
/// an all-null group, matching Druid 35 default null handling.
#[test]
fn aggregator_skips_stored_nulls_in_sums() {
    let schemas = parse_dimension_entries(&[
        serde_json::json!("site"),
        serde_json::json!({"type": "double", "name": "value"}),
    ])
    .expect("parse");
    let ingester = BatchIngester::with_schemas("gt".into(), "__time".into(), schemas, vec![]);
    let seg = ingester
        .ingest(ground_truth_rows())
        .expect("ingest")
        .segment_data;

    let events = scan_all(&seg);
    let sum_for_site = |site: &str| -> serde_json::Value {
        let mut agg = DoubleSumAggregator::new();
        for ev in events
            .iter()
            .filter(|e| e["site"] == serde_json::json!(site))
        {
            agg.aggregate(Some(&ev["value"]));
        }
        agg.get()
    };

    assert_eq!(
        sum_for_site("site_a"),
        serde_json::json!(30.0),
        "10+20, null skipped"
    );
    assert_eq!(
        sum_for_site("site_b"),
        serde_json::json!(30.0),
        "30, nulls skipped"
    );
    assert_eq!(
        sum_for_site("site_c"),
        serde_json::Value::Null,
        "an all-null group's SUM is SQL null at the aggregator level \
         (Druid 35 default null handling)"
    );
}

// ---------------------------------------------------------------------------
// On-disk round-trips: null information must survive v9 and FDX.
// ---------------------------------------------------------------------------

fn assert_null_faithful(seg: &SegmentData) {
    let vals = double_col(seg, "value");
    assert_eq!(vals[0], 10.0);
    assert_eq!(vals[3], 30.0);
    for idx in [2usize, 4, 5, 6] {
        assert!(
            vals[idx].is_nan(),
            "row {idx} NULL must survive the round-trip"
        );
    }
    match seg.column("device_id") {
        Some(ColumnData::String(sc)) => {
            assert!(
                sc.is_null_row(4),
                "null-row bitmap must survive the round-trip"
            );
            assert_eq!(sc.null_rows().map(DruidBitmap::len), Some(1));
        }
        other => panic!("expected STRING column, got {other:?}"),
    }
    match seg.column("count") {
        Some(ColumnData::Long(v)) => assert!(v.iter().all(|&c| c == 1)),
        other => panic!("expected LONG count column, got {other:?}"),
    }
}

fn build_ground_truth_segment() -> SegmentData {
    let schemas = parse_dimension_entries(&[
        serde_json::json!("site"),
        serde_json::json!("device_id"),
        serde_json::json!({"type": "double", "name": "value"}),
    ])
    .expect("parse");
    let ingester = BatchIngester::with_schemas(
        "gt".into(),
        "__time".into(),
        schemas,
        vec![serde_json::json!({"type": "count", "name": "count"})],
    );
    ingester
        .ingest(ground_truth_rows())
        .expect("ingest")
        .segment_data
}

/// v9 disk round-trip: write to a real directory, read back, nulls intact.
#[test]
fn null_segment_v9_disk_roundtrip() {
    let seg = build_ground_truth_segment();
    let dir = tempfile::tempdir().expect("tempdir");
    let seg_dir = dir.path().join("seg");
    ferrodruid_segment::write_segment_v9(&seg, &seg_dir).expect("write v9");
    let read_back = SegmentData::open(&seg_dir).expect("read v9");
    assert_null_faithful(&read_back);
}

/// FDX in-memory round-trip: nulls intact through the FDX writer/reader.
#[test]
fn null_segment_fdx_memory_roundtrip() {
    let seg = build_ground_truth_segment();
    let (meta, chunks) = ferrodruid_segment::write_segment_fdx_to_memory(&seg).expect("write FDX");
    let smoosh = ferrodruid_segment::SmooshReader::from_parts(&meta, chunks).expect("smoosh");
    let read_back = SegmentData::from_smoosh(&smoosh).expect("read FDX");
    assert_eq!(read_back.version, 10);
    assert_null_faithful(&read_back);
}

/// Null-free data keeps the historical representation bit-for-bit: no
/// trailing bitmap, exact metric values, LONG count.
#[test]
fn null_free_ingest_representation_unchanged() {
    let ingester = BatchIngester::new(
        "ds".into(),
        "__time".into(),
        vec!["site".into()],
        vec![serde_json::json!({"type": "doubleSum", "name": "value", "fieldName": "value"})],
    );
    let rows = vec![
        serde_json::json!({"__time": 1000, "site": "a", "value": 1.5}),
        serde_json::json!({"__time": 2000, "site": "b", "value": 2.5}),
    ];
    let seg = ingester.ingest(rows).expect("ingest").segment_data;
    match seg.column("site") {
        Some(ColumnData::String(sc)) => {
            assert_eq!(sc.bitmap_indexes.len(), sc.dictionary.len());
            assert!(sc.null_rows().is_none());
        }
        other => panic!("expected STRING column, got {other:?}"),
    }
    assert_eq!(double_col(&seg, "value"), &[1.5, 2.5]);
}

/// Under rollup, sums skip null inputs and an all-null group stores NULL.
#[test]
fn rollup_sum_skips_nulls_and_all_null_group_stores_null() {
    let ingester = BatchIngester::new(
        "gt".into(),
        "__time".into(),
        vec!["site".into()],
        vec![serde_json::json!({
            "type": "doubleSum", "name": "value", "fieldName": "value"
        })],
    );
    let seg = ingester
        .ingest_with_rollup(ground_truth_rows(), "day")
        .expect("rollup")
        .segment_data;

    // 3 groups: site_a / site_b / site_c.
    assert_eq!(seg.num_rows, 3);
    let events = scan_all(&seg);
    let value_of = |site: &str| -> serde_json::Value {
        events
            .iter()
            .find(|e| e["site"] == serde_json::json!(site))
            .map(|e| e["value"].clone())
            .expect("group present")
    };
    assert_eq!(
        value_of("site_a"),
        serde_json::json!(30.0),
        "10+20, null skipped"
    );
    assert_eq!(
        value_of("site_b"),
        serde_json::json!(30.0),
        "30, nulls skipped"
    );
    assert_eq!(
        value_of("site_c"),
        serde_json::Value::Null,
        "all-null rollup group must store NULL, not 0"
    );
}

/// Under rollup, the `count` metric stores the number of raw rows merged
/// into each rolled-up row — honoring the metric's *spec name* (not a
/// hardcoded `"count"`).
#[test]
fn defect_d_count_metric_rollup_merged_counts() {
    let ingester = BatchIngester::new(
        "gt".into(),
        "__time".into(),
        vec!["site".into()],
        vec![serde_json::json!({"type": "count", "name": "cnt"})],
    );
    // All 7 rows collapse into 3 groups (site_a / site_b / site_c) at day
    // granularity (all timestamps are within the same day).
    let seg = ingester
        .ingest_with_rollup(ground_truth_rows(), "day")
        .expect("rollup ingest")
        .segment_data;

    assert_eq!(seg.num_rows, 3);
    match seg.column("cnt") {
        Some(ColumnData::Long(v)) => {
            let mut counts = v.to_vec();
            counts.sort_unstable();
            assert_eq!(counts, vec![1, 3, 3], "site_a=3, site_b=3, site_c=1");
        }
        other => panic!("cnt metric must be a LONG column, got {other:?}"),
    }
}

/// Codex-review High (2026-07-11): an unsigned JSON integer above
/// `i64::MAX` must NOT silently wrap into a negative long — it is stored
/// as NULL (the long column falls back to DOUBLE+NaN), never a corrupted
/// value. Same for float inputs outside the i64 range.
#[test]
fn out_of_range_long_input_stores_null_not_wrapped() {
    let schemas = vec![DimensionSchema::new("code", DimensionType::Long)];
    let ingester = BatchIngester::with_schemas("gt".into(), "__time".into(), schemas, vec![]);
    let rows = vec![
        serde_json::json!({"__time": 1000, "code": 7}),
        // u64::from(i64::MAX) + 1 — representable in JSON, not in i64.
        serde_json::json!({"__time": 2000, "code": 9_223_372_036_854_775_808_u64}),
        // A float far outside the i64 range.
        serde_json::json!({"__time": 3000, "code": 1.0e19}),
    ];
    let seg = ingester.ingest(rows).expect("ingest").segment_data;
    match seg.column("code") {
        Some(ColumnData::Double(v)) => {
            assert_eq!(v[0], 7.0);
            assert!(
                v[1].is_nan(),
                "u64 above i64::MAX must store NULL, got {} (wrap corruption)",
                v[1]
            );
            assert!(
                v[2].is_nan(),
                "float outside i64 range must store NULL, got {}",
                v[2]
            );
        }
        other => panic!("null-bearing long column must be DOUBLE, got {other:?}"),
    }
}

/// codex-review r2 (2026-07-11): under rollup, a NULL dimension value must
/// form its own group, distinct from a real `""` group — previously both
/// coerced to `""` and merged (Druid keeps them distinct).
#[test]
fn rollup_null_dim_group_distinct_from_empty_string() {
    let ingester = BatchIngester::new(
        "gt".into(),
        "__time".into(),
        vec!["site".into()],
        vec![
            serde_json::json!({"type": "count", "name": "cnt"}),
            serde_json::json!({"type": "doubleSum", "name": "value", "fieldName": "value"}),
        ],
    );
    let rows = vec![
        serde_json::json!({"__time": 1000, "site": null, "value": 1.0}),
        serde_json::json!({"__time": 2000, "site": "",   "value": 2.0}),
        serde_json::json!({"__time": 3000, "site": null, "value": 4.0}),
    ];
    let seg = ingester
        .ingest_with_rollup(rows, "day")
        .expect("rollup ingest")
        .segment_data;

    // Two groups: the null group (rows 1+3, sum 5) and the "" group (row 2).
    assert_eq!(
        seg.num_rows, 2,
        "null and \"\" must be DISTINCT rollup groups"
    );
    let sc = match seg.column("site") {
        Some(ColumnData::String(sc)) => sc,
        other => panic!("expected STRING column, got {other:?}"),
    };
    let nulls = sc.null_rows().expect("null-bearing rollup dim");
    assert_eq!(nulls.len(), 1, "exactly one stored row is the null group");
    // The null group carries cnt=2 / value=5; the "" group cnt=1 / value=2.
    let (cnts, vals) = match (seg.column("cnt"), seg.column("value")) {
        (Some(ColumnData::Long(c)), Some(ColumnData::Double(v))) => (c.clone(), v.clone()),
        other => panic!("unexpected column shapes: {other:?}"),
    };
    let null_row = (0..2).find(|&i| sc.is_null_row(i)).expect("null row");
    let empty_row = 1 - null_row;
    assert_eq!(cnts[null_row], 2);
    assert_eq!(vals[null_row], 5.0);
    assert_eq!(cnts[empty_row], 1);
    assert_eq!(vals[empty_row], 2.0);
}

/// codex-review r6 (2026-07-11): a null-bearing long-typed column falls back
/// to DOUBLE — exact only within +/-2^53. A batch that combines NULLs with
/// values OUTSIDE that range must fail closed (silent f64 rounding would
/// corrupt adjacent IDs, e.g. 9007199254740993 -> ...992), never ingest
/// corrupted values. In-range null-bearing batches and null-free big-value
/// batches keep working.
#[test]
fn long_nulls_with_beyond_2p53_values_fail_closed() {
    let mk = || {
        BatchIngester::with_schemas(
            "gt".into(),
            "__time".into(),
            vec![DimensionSchema::new("id", DimensionType::Long)],
            vec![],
        )
    };

    // NULL + beyond-2^53 value: must ERROR, not round silently.
    let rows = vec![
        serde_json::json!({"__time": 1000, "id": 9_007_199_254_740_993_i64}),
        serde_json::json!({"__time": 2000, "id": null}),
    ];
    let err = mk().ingest(rows).expect_err("must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("2^53") || msg.contains("9007199254740992") || msg.contains("precision"),
        "error must explain the precision limit: {msg}"
    );

    // NULL-free big values stay a true LONG column (exact).
    let rows = vec![
        serde_json::json!({"__time": 1000, "id": 9_007_199_254_740_993_i64}),
        serde_json::json!({"__time": 2000, "id": 9_007_199_254_740_995_i64}),
    ];
    let seg = mk().ingest(rows).expect("null-free ingest").segment_data;
    match seg.column("id") {
        Some(ColumnData::Long(v)) => {
            assert_eq!(v, &[9_007_199_254_740_993, 9_007_199_254_740_995]);
        }
        other => panic!("null-free long column must stay LONG, got {other:?}"),
    }

    // NULL + in-range values keep the exact DOUBLE fallback.
    let rows = vec![
        serde_json::json!({"__time": 1000, "id": 42}),
        serde_json::json!({"__time": 2000, "id": null}),
    ];
    let seg = mk().ingest(rows).expect("in-range ingest").segment_data;
    match seg.column("id") {
        Some(ColumnData::Double(v)) => {
            assert_eq!(v[0], 42.0);
            assert!(v[1].is_nan());
        }
        other => panic!("in-range null-bearing long must be DOUBLE, got {other:?}"),
    }
}

/// codex-review r7 (2026-07-11): rollup grouping must key numeric-typed
/// dimensions by their NUMERIC value, not their JSON text — `1` and `1.0`
/// are the same double and must merge into one rolled row (previously the
/// string forms "1" vs "1.0" produced two rows, both storing x=1.0).
#[test]
fn rollup_numeric_dim_keys_by_value_not_json_text() {
    let ingester = BatchIngester::with_schemas(
        "gt".into(),
        "__time".into(),
        vec![DimensionSchema::new("x", DimensionType::Double)],
        vec![serde_json::json!({"type": "count", "name": "cnt"})],
    );
    let rows = vec![
        serde_json::json!({"__time": 1000, "x": 1}),
        serde_json::json!({"__time": 2000, "x": 1.0}),
        serde_json::json!({"__time": 3000, "x": 2.5}),
    ];
    let seg = ingester
        .ingest_with_rollup(rows, "day")
        .expect("rollup ingest")
        .segment_data;
    assert_eq!(
        seg.num_rows, 2,
        "1 and 1.0 must merge into one rolled group (plus the 2.5 group)"
    );
    match seg.column("cnt") {
        Some(ColumnData::Long(v)) => {
            let mut counts = v.to_vec();
            counts.sort_unstable();
            assert_eq!(counts, vec![1, 2], "merged group carries cnt=2");
        }
        other => panic!("cnt must be LONG, got {other:?}"),
    }
}

/// codex-review r9 (2026-07-11): rollup keys must match STORAGE precision.
/// A FLOAT dimension stores f32 — 16777216.0 and 16777217.0 are the same
/// f32, so they must merge into one rolled group (previously keyed at f64
/// precision -> two rows with identical stored values). And -0.0 keys the
/// same as 0.0 (numerically equal).
#[test]
fn rollup_float_dim_keys_at_storage_precision_and_negzero_merges() {
    let mk = |t: DimensionType| {
        BatchIngester::with_schemas(
            "gt".into(),
            "__time".into(),
            vec![DimensionSchema::new("x", t)],
            vec![serde_json::json!({"type": "count", "name": "cnt"})],
        )
    };
    // f32 precision collapse: one group, cnt=2.
    let rows = vec![
        serde_json::json!({"__time": 1000, "x": 16777216.0}),
        serde_json::json!({"__time": 2000, "x": 16777217.0}),
    ];
    let seg = mk(DimensionType::Float)
        .ingest_with_rollup(rows, "day")
        .expect("rollup")
        .segment_data;
    assert_eq!(
        seg.num_rows, 1,
        "f32-identical values must merge into one rolled group"
    );

    // -0.0 vs 0.0 (double): numerically equal, one group.
    let rows = vec![
        serde_json::json!({"__time": 1000, "x": 0.0}),
        serde_json::json!({"__time": 2000, "x": -0.0}),
    ];
    let seg = mk(DimensionType::Double)
        .ingest_with_rollup(rows, "day")
        .expect("rollup")
        .segment_data;
    assert_eq!(seg.num_rows, 1, "-0.0 and 0.0 must key identically");
}

/// codex-review r11 (2026-07-11): a metric spec whose `name` differs from
/// its `fieldName` ({"name":"sum_value","fieldName":"value"}) must read the
/// SOURCE field — previously the builder read `row.get(name)` and stored
/// NULL (post null-faithful work; 0.0 before it) for every renamed metric.
/// Covers rollup=false storage and the rollup sum path.
#[test]
fn renamed_metric_reads_field_name_not_output_name() {
    let mk = || {
        BatchIngester::with_schemas(
            "gt".into(),
            "__time".into(),
            vec![DimensionSchema::string("site")],
            vec![serde_json::json!({
                "type": "doubleSum", "name": "sum_value", "fieldName": "value"
            })],
        )
    };
    let rows = vec![
        serde_json::json!({"__time": 1000, "site": "a", "value": 10.0}),
        serde_json::json!({"__time": 2000, "site": "a", "value": 20.0}),
    ];
    // rollup=false: stored under the OUTPUT name, values read from `value`.
    let seg = mk().ingest(rows.clone()).expect("ingest").segment_data;
    match seg.column("sum_value") {
        Some(ColumnData::Double(v)) => assert_eq!(v, &[10.0, 20.0]),
        other => panic!("renamed metric must store source values, got {other:?}"),
    }
    // rollup: one group with the rolled sum 30.0.
    let seg = mk()
        .ingest_with_rollup(rows, "day")
        .expect("rollup")
        .segment_data;
    assert_eq!(seg.num_rows, 1);
    match seg.column("sum_value") {
        Some(ColumnData::Double(v)) => assert_eq!(v, &[30.0]),
        other => panic!("rolled renamed metric must be 30.0, got {other:?}"),
    }
}

/// codex-review r13 (2026-07-11): non-finite numeric inputs — the string
/// forms "Infinity"/"-Infinity"/"NaN" (accepted by Rust's f64 parser) and
/// float-typed values overflowing f32 — must store SQL NULL, not a
/// non-finite value that renders as null on the wire while grouping as a
/// phantom second "null" group.
#[test]
fn non_finite_numeric_inputs_store_null() {
    let mk = |t: DimensionType| {
        BatchIngester::with_schemas(
            "gt".into(),
            "__time".into(),
            vec![DimensionSchema::new("x", t)],
            vec![serde_json::json!({"type": "count", "name": "cnt"})],
        )
    };
    let rows = vec![
        serde_json::json!({"__time": 1000, "x": "Infinity"}),
        serde_json::json!({"__time": 2000, "x": "NaN"}),
        serde_json::json!({"__time": 3000, "x": null}),
        serde_json::json!({"__time": 4000, "x": 1.5}),
    ];
    let seg = mk(DimensionType::Double)
        .ingest(rows.clone())
        .expect("ingest")
        .segment_data;
    match seg.column("x") {
        Some(ColumnData::Double(v)) => {
            assert!(v[0].is_nan(), "\"Infinity\" must store NULL, got {}", v[0]);
            assert!(v[1].is_nan(), "\"NaN\" must store NULL, got {}", v[1]);
            assert!(v[2].is_nan());
            assert_eq!(v[3], 1.5);
        }
        other => panic!("expected DOUBLE, got {other:?}"),
    }
    // All three null-ish rows share ONE rollup group (no phantom groups).
    let seg = mk(DimensionType::Double)
        .ingest_with_rollup(rows, "day")
        .expect("rollup")
        .segment_data;
    assert_eq!(seg.num_rows, 2, "null/Infinity/NaN = one group, 1.5 = one");

    // Float overflow: a finite f64 above f32::MAX must store NULL, not inf.
    let rows = vec![serde_json::json!({"__time": 1000, "x": 1.0e39})];
    let seg = mk(DimensionType::Float)
        .ingest(rows)
        .expect("ingest")
        .segment_data;
    match seg.column("x") {
        Some(ColumnData::Float(v)) => {
            assert!(v[0].is_nan(), "f32 overflow must store NULL, got {}", v[0]);
        }
        other => panic!("expected FLOAT, got {other:?}"),
    }
}
