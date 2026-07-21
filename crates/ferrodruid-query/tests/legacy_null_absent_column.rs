// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-B legacy null mode — physically-absent columns (H1).
//!
//! **This test binary latches the process-global legacy-null mode ON** in
//! every test; no ANSI-mode test may be added here (own-process latch).
//!
//! ONE invariant: on a schema-evolution segment that LACKS a column
//! entirely, every legacy read must be indistinguishable from a segment
//! where the column is present with every cell null —
//!
//! * a missing NUMERIC column behaves as `0`/`0.0` (aggregations,
//!   numeric-`outputType` grouping);
//! * a missing STRING column behaves as `''` (== the ONE merged
//!   canonical null), so approximate `COUNT DISTINCT` counts it;
//! * the shared canonicalizer
//!   (`ferrodruid_common::null_mode::legacy_canonical_cell`) and the
//!   single-binary read point (`column_value_at`) agree cell-for-cell,
//!   so the role-split path (which uses the shared rule directly) can
//!   never diverge from the single-binary path.

use ferrodruid_common::null_mode::{LegacyColumnKind, legacy_canonical_cell};
use ferrodruid_query::{TimeseriesQuery, column_value_at, numeric_agg_cell};
use ferrodruid_segment::{SegmentData, SegmentDataBuilder};
use serde_json::json;

fn latch_legacy() {
    assert!(
        ferrodruid_common::null_mode::init_legacy_null_mode(true),
        "this test binary requires the legacy-null latch"
    );
}

const INTERVAL: &str = "1970-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z";

/// 3 rows; the queried columns `m` (numeric) / `sm` (string) are ABSENT.
fn segment_absent() -> SegmentData {
    SegmentDataBuilder::new()
        .add_timestamp_column(vec![0, 0, 0])
        .add_string_column("s", vec!["a".into(), "b".into(), "c".into()])
        .build()
        .expect("segment builds")
}

/// Twin: `m` PRESENT with every cell null, `sm` PRESENT with every cell
/// `''` (the legacy null string).
fn segment_null_twin() -> SegmentData {
    SegmentDataBuilder::new()
        .add_timestamp_column(vec![0, 0, 0])
        .add_string_column("s", vec!["a".into(), "b".into(), "c".into()])
        .add_long_column_nullable("m", true, vec![None, None, None])
        .add_string_column("sm", vec![String::new(), String::new(), String::new()])
        .build()
        .expect("twin segment builds")
}

fn timeseries(aggs: serde_json::Value) -> TimeseriesQuery {
    serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": aggs,
    }))
    .expect("timeseries query parses")
}

/// Approximate COUNT DISTINCT over the ABSENT string column `sm` must
/// count the ONE merged ''/null value — identically to the twin where
/// every cell is `''` — instead of the HLL feed exiting on `None` and
/// reporting 0.
#[test]
fn hll_over_absent_string_column_counts_merged_null() {
    latch_legacy();
    let q = timeseries(json!([
        {"type": "HLLSketchBuild", "name": "u", "fieldName": "sm"}
    ]));
    let absent = q.execute(&segment_absent()).expect("absent executes");
    let twin = q.execute(&segment_null_twin()).expect("twin executes");
    assert_eq!(absent.len(), 1);
    assert_eq!(twin.len(), 1);
    let est_absent = absent[0]
        .result
        .get("u")
        .and_then(numeric_agg_cell)
        .expect("estimate present");
    let est_twin = twin[0].result.get("u").and_then(numeric_agg_cell);
    // HLL is approximate: assert the ONE distinct value within sketch
    // tolerance (pre-fix the feed exited on `None` and estimated 0).
    assert!(
        (est_absent - 1.0).abs() < 0.01,
        "legacy HLL over an ABSENT string column must count the merged ''/null value \
         (~1 distinct), got {est_absent}: {absent:?}"
    );
    assert_eq!(
        Some(est_absent),
        est_twin,
        "absent column and present-all-'' column must be indistinguishable"
    );
}

/// SUM / MIN over the ABSENT numeric column `m` must read the legacy
/// default 0 per row — identically to the twin where every cell is null
/// (stored as the coerced 0 defaults).
#[test]
fn numeric_aggs_over_absent_column_read_zero() {
    latch_legacy();
    let q = timeseries(json!([
        {"type": "longSum", "name": "total", "fieldName": "m"},
        {"type": "longMin", "name": "lo", "fieldName": "m"},
        {"type": "count", "name": "cnt"}
    ]));
    let absent = q.execute(&segment_absent()).expect("absent executes");
    let twin = q.execute(&segment_null_twin()).expect("twin executes");
    assert_eq!(absent.len(), 1);
    assert_eq!(
        absent[0].result.get("total"),
        Some(&json!(0)),
        "legacy SUM over an ABSENT numeric column is 0: {absent:?}"
    );
    assert_eq!(
        absent[0].result.get("lo"),
        Some(&json!(0)),
        "legacy MIN over an ABSENT numeric column is 0 (fed per-row defaults): {absent:?}"
    );
    assert_eq!(absent[0].result.get("cnt"), Some(&json!(3)));
    assert_eq!(
        absent[0].result, twin[0].result,
        "absent column and present-all-null column must aggregate identically"
    );
}

/// Grouping the ABSENT column `m` with a numeric `outputType` must key
/// every row as the legacy default 0 (one group, key = JSON number 0) —
/// identically to the twin — instead of preserving the null key.
#[test]
fn numeric_output_type_grouping_over_absent_column_keys_zero() {
    latch_legacy();
    let q: ferrodruid_query::GroupByQuery = serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [
            {"type": "default", "dimension": "m", "outputName": "m", "outputType": "LONG"}
        ],
        "aggregations": [{"type": "count", "name": "cnt"}],
    }))
    .expect("groupBy query parses");
    let absent = q.execute(&segment_absent()).expect("absent executes");
    let twin = q.execute(&segment_null_twin()).expect("twin executes");
    assert_eq!(
        absent.len(),
        1,
        "one merged legacy-default group expected: {absent:?}"
    );
    assert_eq!(
        absent[0].event.get("m"),
        Some(&json!(0)),
        "legacy numeric grouping over an ABSENT column keys as 0: {absent:?}"
    );
    assert_eq!(absent[0].event.get("cnt"), Some(&json!(3)));
    assert_eq!(
        absent[0].event, twin[0].event,
        "absent column and present-all-null column must group identically"
    );
}

/// The SHARED-rule identity: `legacy_canonical_cell` (the rule the
/// role-split executor applies to raw JSON rows) agrees cell-for-cell
/// with `column_value_at` (the single-binary read of the equivalent
/// typed column), for every kind × cell shape.
#[test]
fn shared_canonicalizer_matches_column_value_at() {
    latch_legacy();
    let seg = SegmentDataBuilder::new()
        .add_timestamp_column(vec![0, 0])
        .add_string_column("s", vec!["a".into(), String::new()])
        .add_long_column_nullable("l", true, vec![Some(5), None])
        .add_double_column_nullable("d", true, vec![Some(1.5), None])
        .build()
        .expect("segment builds");

    let col = |name: &str| seg.columns.get(name).expect("column exists");

    // LONG: present value passes through; null/missing reads 0.
    assert_eq!(
        column_value_at(col("l"), 0),
        legacy_canonical_cell(LegacyColumnKind::Long, Some(&json!(5)))
    );
    assert_eq!(
        column_value_at(col("l"), 1),
        legacy_canonical_cell(LegacyColumnKind::Long, None)
    );
    assert_eq!(
        legacy_canonical_cell(LegacyColumnKind::Long, Some(&serde_json::Value::Null)),
        json!(0)
    );

    // DOUBLE: present value passes through; null/missing reads 0.0.
    assert_eq!(
        column_value_at(col("d"), 0),
        legacy_canonical_cell(LegacyColumnKind::Double, Some(&json!(1.5)))
    );
    assert_eq!(
        column_value_at(col("d"), 1),
        legacy_canonical_cell(LegacyColumnKind::Double, None)
    );

    // STRING: `''`, null, and missing are the ONE canonical null.
    assert_eq!(
        column_value_at(col("s"), 0),
        legacy_canonical_cell(LegacyColumnKind::String, Some(&json!("a")))
    );
    assert_eq!(
        column_value_at(col("s"), 1),
        legacy_canonical_cell(LegacyColumnKind::String, Some(&json!("")))
    );
    assert_eq!(
        legacy_canonical_cell(LegacyColumnKind::String, None),
        serde_json::Value::Null
    );
    assert_eq!(
        legacy_canonical_cell(LegacyColumnKind::String, Some(&serde_json::Value::Null)),
        serde_json::Value::Null
    );
}
