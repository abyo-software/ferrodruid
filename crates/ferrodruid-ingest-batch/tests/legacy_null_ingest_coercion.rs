// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-B legacy null mode — INGEST-side coercion.
//!
//! **This test binary latches the process-global legacy-null mode ON** in
//! every test; no ANSI-mode test may be added here (own-process latch).
//!
//! Oracle basis: the Druid 27.0.0 legacy segment captured under
//! `tests/segment-compat/fixtures/legacy_null_druid27/segment/` —
//! `dump-segment --dump metadata` shows the all-null LONG `x` stored as
//! literal `0` on every row with **`hasNulls: false`** (no null bitmap
//! exists), and `strcol` stores `''` and absent rows as the SAME
//! dictionary id (cardinality 4, minValue `""`).  A legacy writer emits
//! NO null markers of any kind.
//!
//! Under the latch the REAL `BatchIngester` must therefore produce:
//!
//! * LONG dims: plain `ColumnData::Long` (0 at absent rows), never
//!   `LongNullable` / never a null bitmap;
//! * DOUBLE/FLOAT dims: literal `0.0` (never the in-band NaN null);
//! * STRING dims: absent/null rows coerced to `""` — no trailing
//!   null-row bitmap (`null_rows()` is `None`), and the `""` dictionary
//!   entry's value bitmap covers both the real-`''` and the coerced rows.

use ferrodruid_ingest_batch::{BatchIngester, parse_dimension_entries};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::ColumnData;
use serde_json::{Value, json};

fn latch_legacy() {
    assert!(
        ferrodruid_common::null_mode::init_legacy_null_mode(true),
        "this test binary requires the legacy-null latch"
    );
}

fn base_millis() -> i64 {
    chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .expect("base ts")
        .timestamp_millis()
}

/// Ingest the 6-row legacy_null_compat fixture through the real ingester.
fn ingest_fixture() -> SegmentData {
    latch_legacy();
    let base = base_millis();
    let t = |i: i64| base + i * 3_600_000;
    let rows: Vec<Value> = vec![
        json!({"__time": t(0), "tag": "r0", "strcol": "a", "y": 10}),
        json!({"__time": t(1), "tag": "r1", "strcol": "b"}),
        json!({"__time": t(2), "tag": "r2", "strcol": "", "y": 20}),
        json!({"__time": t(3), "tag": "r3"}),
        json!({"__time": t(4), "tag": "r4", "strcol": ""}),
        json!({"__time": t(5), "tag": "r5", "strcol": "c", "y": 30}),
    ];
    let dims = json!([
        {"type": "string", "name": "tag"},
        {"type": "string", "name": "strcol"},
        {"type": "long", "name": "x"},
        {"type": "long", "name": "y"}
    ]);
    let ingester = BatchIngester::with_schemas(
        "legacy_null_compat".to_string(),
        "__time".to_string(),
        parse_dimension_entries(dims.as_array().expect("array")).expect("schemas"),
        Vec::new(),
    );
    ingester.ingest(rows).expect("ingest").segment_data
}

#[test]
fn long_dims_store_literal_zeros_with_no_null_markers() {
    let segment = ingest_fixture();
    match segment.columns.get("x").expect("x column") {
        ColumnData::Long(values) => {
            assert_eq!(values, &vec![0, 0, 0, 0, 0, 0], "all-null x coerces to 0s");
        }
        other => panic!("legacy x must be plain Long (no null bitmap), got {other:?}"),
    }
    match segment.columns.get("y").expect("y column") {
        ColumnData::Long(values) => {
            assert_eq!(
                values,
                &vec![10, 0, 20, 0, 0, 30],
                "absent y rows coerce to 0"
            );
        }
        other => panic!("legacy y must be plain Long (no null bitmap), got {other:?}"),
    }
}

#[test]
fn string_dims_coerce_null_to_empty_with_no_null_bitmap() {
    let segment = ingest_fixture();
    let ColumnData::String(sc) = segment.columns.get("strcol").expect("strcol column") else {
        panic!("strcol must be a single-value string column");
    };
    assert!(
        sc.null_rows().is_none(),
        "a legacy-written string column carries NO null-row bitmap"
    );
    // Dictionary = ["", "a", "b", "c"] (the legacy dump's cardinality-4,
    // minValue-"" shape) and the coerced rows resolve to "".
    let resolved: Vec<String> = (0..6)
        .map(|i| {
            sc.dictionary
                .get(sc.encoded_values[i] as usize)
                .expect("in-dictionary ordinal")
                .to_string()
        })
        .collect();
    assert_eq!(resolved, vec!["a", "b", "", "", "", "c"]);
    // The "" entry's value bitmap covers real-'' AND coerced rows alike
    // (rows 2, 3, 4) — on disk they are the same dictionary id.
    let empty_ord = (0..sc.dictionary.len())
        .find(|&i| sc.dictionary.get(i) == Some(""))
        .expect("\"\" dictionary entry exists");
    let bitmap = &sc.bitmap_indexes[empty_ord];
    let rows: Vec<u32> = (0..6u32).filter(|&r| bitmap.contains(r)).collect();
    assert_eq!(rows, vec![2, 3, 4], "'' bitmap covers coerced + real rows");
}

#[test]
fn double_float_dims_coerce_null_to_zero_not_nan() {
    latch_legacy();
    let base = base_millis();
    let t = |i: i64| base + i * 3_600_000;
    let rows: Vec<Value> = vec![
        json!({"__time": t(0), "tag": "e0", "d": 1.5, "f": 2.5}),
        json!({"__time": t(1), "tag": "e1"}),
        json!({"__time": t(2), "tag": "e2", "d": 3.5}),
        json!({"__time": t(3), "tag": "e3", "f": 4.5}),
    ];
    let dims = json!([
        {"type": "string", "name": "tag"},
        {"type": "double", "name": "d"},
        {"type": "float", "name": "f"}
    ]);
    let ingester = BatchIngester::with_schemas(
        "legacy_null_ext".to_string(),
        "__time".to_string(),
        parse_dimension_entries(dims.as_array().expect("array")).expect("schemas"),
        Vec::new(),
    );
    let segment = ingester.ingest(rows).expect("ingest").segment_data;
    match segment.columns.get("d").expect("d column") {
        ColumnData::Double(values) => {
            assert_eq!(
                values,
                &vec![1.5, 0.0, 3.5, 0.0],
                "absent d rows coerce to literal 0.0 (never NaN)"
            );
            assert!(values.iter().all(|v| !v.is_nan()));
        }
        other => panic!("d must be a Double column, got {other:?}"),
    }
    match segment.columns.get("f").expect("f column") {
        ColumnData::Float(values) => {
            assert_eq!(values, &vec![2.5, 0.0, 0.0, 4.5]);
            assert!(values.iter().all(|v| !v.is_nan()));
        }
        other => panic!("f must be a Float column, got {other:?}"),
    }
}

/// Rollup under legacy: a null/absent dimension keys as its coercion
/// default, so ''/null rows MERGE into one rolled group with genuine
/// default-valued rows (a legacy writer never distinguishes them).
/// Derived from the pinned per-column coercion semantics; a
/// rollup-specific oracle fixture is a follow-on.
#[test]
fn rollup_merges_null_and_default_dimension_keys() {
    latch_legacy();
    let base = base_millis();
    let rows: Vec<Value> = vec![
        json!({"__time": base, "site": "a", "v": 1.0}),
        json!({"__time": base + 1000, "site": null, "v": 2.0}),
        json!({"__time": base + 2000, "site": "", "v": 3.0}),
        json!({"__time": base + 3000, "v": 4.0}),
    ];
    let ingester = BatchIngester::new(
        "legacy_rollup".to_string(),
        "__time".to_string(),
        vec!["site".to_string()],
        vec![json!({"type": "doubleSum", "name": "v", "fieldName": "v"})],
    );
    let segment = ingester
        .ingest_with_rollup(rows, "day")
        .expect("rollup ingest")
        .segment_data;
    // Two rolled rows: the merged ''/null/absent group (v = 2+3+4 = 9)
    // and the "a" group (v = 1).
    assert_eq!(
        segment.num_rows, 2,
        "''/null/absent must roll into ONE group"
    );
    let ColumnData::String(sc) = segment.columns.get("site").expect("site column") else {
        panic!("site must be a single-value string column");
    };
    assert!(sc.null_rows().is_none(), "no null markers under legacy");
    let mut got: Vec<(String, f64)> = (0..segment.num_rows)
        .map(|i| {
            let site = sc
                .dictionary
                .get(sc.encoded_values[i] as usize)
                .expect("ord")
                .to_string();
            let v = match segment.columns.get("v").expect("v column") {
                ColumnData::Double(vals) => vals[i],
                other => panic!("v must be Double, got {other:?}"),
            };
            (site, v)
        })
        .collect();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got.len(), 2);
    assert_eq!(got[0], (String::new(), 9.0), "merged '' group sums 2+3+4");
    assert_eq!(got[1], ("a".to_string(), 1.0));
}

/// Metric columns (e.g. a doubleSum metric fed by an absent field) coerce
/// to 0.0 as well — a legacy writer stores plain zeros everywhere.
#[test]
fn metric_columns_coerce_null_to_zero() {
    latch_legacy();
    let base = base_millis();
    let rows: Vec<Value> = vec![
        json!({"__time": base, "tag": "m0", "v": 2.0}),
        json!({"__time": base + 1000, "tag": "m1"}),
    ];
    let ingester = BatchIngester::new(
        "legacy_metric".to_string(),
        "__time".to_string(),
        vec!["tag".to_string()],
        vec![json!({"type": "doubleSum", "name": "v", "fieldName": "v"})],
    );
    let segment = ingester.ingest(rows).expect("ingest").segment_data;
    match segment.columns.get("v").expect("v column") {
        ColumnData::Double(values) => {
            assert_eq!(values, &vec![2.0, 0.0], "absent metric coerces to 0.0");
            assert!(values.iter().all(|v| !v.is_nan()));
        }
        other => panic!("v must be a Double metric column, got {other:?}"),
    }
}
