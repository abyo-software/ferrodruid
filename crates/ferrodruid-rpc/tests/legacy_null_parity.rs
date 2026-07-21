// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-B legacy null mode — role-split ⇄ single-binary PARITY (H1–H4).
//!
//! **This test binary latches the process-global legacy-null mode ON** in
//! every test; no ANSI-mode test may be added here (own-process latch).
//!
//! ONE invariant: the role-split executor (`native_query` /
//! `sql_bridge`) must return the SAME legacy answer as the single-binary
//! path (`ferrodruid-query`) for the same logical data — including on a
//! schema-evolution segment that lacks a column entirely.  Every test
//! drives BOTH paths over twin fixtures (a typed `SegmentData` and its
//! JSON-Lines wire `Segment` mirror) and asserts value-level identity,
//! and pins the oracle legacy answer explicitly.
//!
//! Covered legs: numeric equality filter over null/missing cells (H2),
//! numeric groupBy key merging (H2), scan typed defaults (H2),
//! `IS NULL` through the SQL bridge — string AND numeric (H3),
//! empty-match timeseries bucket emission (H4), and the
//! physically-absent column (H1) on grouping and filtering.
//!
//! Approximate `COUNT DISTINCT` has NO role-split counterpart (the wire
//! aggregation surface is count/longSum/doubleSum, and the SQL bridge
//! rejects HLL shapes), so its absent-column legacy behavior is pinned
//! single-binary-only in
//! `ferrodruid-query/tests/legacy_null_absent_column.rs`.

use ferrodruid_deep_storage::Segment;
use ferrodruid_query::{DruidQuery, GroupByQuery, ScanQuery, TimeseriesQuery};
use ferrodruid_rpc::native_query::{
    Aggregation, DimensionOutputType, DimensionRef, EqualsFilter, GroupBySpec, NativeQuery,
    NativeQueryResult, ScanSpec, TimeseriesSpec, legacy_fill_empty_timeseries, merge_group_by,
    merge_timeseries, merge_top_n,
};
use ferrodruid_rpc::sql_bridge::{sql_response_rows, translate_query, translate_sql};
use ferrodruid_segment::{SegmentData, SegmentDataBuilder};
use ferrodruid_sql::planner::{ColumnSchema, DataSourceSchema};
use ferrodruid_sql::{parse_druid_sql, planner::plan_sql};
use serde_json::json;

fn latch_legacy() {
    assert!(
        ferrodruid_common::null_mode::init_legacy_null_mode(true),
        "this test binary requires the legacy-null latch"
    );
}

const INTERVAL: &str = "1970-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z";

/// Single-binary twin: 3 rows.
///
/// | row | `__time` | `s`  | `y`      |
/// |-----|----------|------|----------|
/// | 0   | 0        | "a"  | 0        |
/// | 1   | 60000    | ""   | null     |
/// | 2   | 120000   | ""   | 7        |
///
/// Column `m` is ABSENT (schema evolution).
fn single_binary_segment() -> SegmentData {
    SegmentDataBuilder::new()
        .add_timestamp_column(vec![0, 60_000, 120_000])
        .add_string_column("s", vec!["a".into(), String::new(), String::new()])
        .add_long_column_nullable("y", true, vec![Some(0), None, Some(7)])
        .build()
        .expect("single-binary twin builds")
}

/// Role-split twin: the SAME logical rows as JSON-Lines (`''` stays
/// `""`; the null `y` cell is an omitted field; `m` is absent from
/// header AND rows).
fn wire_segment() -> Segment {
    let jsonl = concat!(
        r#"{"segmentId":"s1","dataSource":"ds","columns":[{"name":"__time","type":"long"},{"name":"s","type":"string"},{"name":"y","type":"long"}]}"#,
        "\n",
        r#"{"__time":0,"s":"a","y":0}"#,
        "\n",
        r#"{"__time":60000,"s":""}"#,
        "\n",
        r#"{"__time":120000,"s":"","y":7}"#,
        "\n",
    );
    Segment::parse_jsonl(jsonl).expect("wire twin parses")
}

/// The SQL schema both paths' SQL layers would see for this datasource.
fn schema() -> DataSourceSchema {
    DataSourceSchema {
        name: "ds".to_string(),
        dimensions: vec![ColumnSchema {
            name: "s".to_string(),
            column_type: ferrodruid_common::types::ColumnType::String,
        }],
        metrics: vec![ColumnSchema {
            name: "y".to_string(),
            column_type: ferrodruid_common::types::ColumnType::Long,
        }],
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

fn single_binary_count(filter: serde_json::Value) -> i64 {
    let q: TimeseriesQuery = serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "granularity": "all",
        "filter": filter,
        "aggregations": [{"type": "count", "name": "cnt"}],
    }))
    .expect("timeseries parses");
    let results = q
        .execute(&single_binary_segment())
        .expect("single-binary executes");
    assert_eq!(results.len(), 1, "one bucket expected: {results:?}");
    results[0]
        .result
        .get("cnt")
        .and_then(serde_json::Value::as_i64)
        .expect("cnt is an integer")
}

/// Role-split count over the wire twin, driven through the FULL
/// role-split path shape: per-segment execute → broker merge →
/// post-merge legacy empty-result fill (W-B H2: the empty bucket is a
/// BROKER synthesis, anchored to the query interval start — a segment
/// with no matching rows returns `[]`).
fn role_split_count(filter: EqualsFilter) -> i64 {
    let spec = TimeseriesSpec {
        data_source: "ds".to_string(),
        granularity_ms: 0,
        aggregations: vec![Aggregation::Count {
            name: "cnt".to_string(),
        }],
        filter: Some(filter),
        intervals: vec![INTERVAL.to_string()],
    };
    let q = NativeQuery::Timeseries(spec.clone());
    let NativeQueryResult::Timeseries(part) = q.execute(&wire_segment()) else {
        panic!("timeseries result expected");
    };
    let merged = merge_timeseries(vec![part], &spec.aggregations);
    let buckets = legacy_fill_empty_timeseries(merged, &spec);
    assert_eq!(buckets.len(), 1, "one bucket expected: {buckets:?}");
    buckets[0]
        .result
        .get("cnt")
        .and_then(serde_json::Value::as_i64)
        .expect("cnt is an integer")
}

// ---------------------------------------------------------------------------
// H2 — numeric equality filter
// ---------------------------------------------------------------------------

/// `y = 0` under legacy matches the stored 0 AND the null/missing cell
/// (coerced default 0) on BOTH paths.  Oracle legacy answer: 2.
#[test]
fn numeric_equality_filter_matches_null_as_zero_on_both_paths() {
    latch_legacy();
    let single = single_binary_count(json!({
        "type": "selector", "dimension": "y", "value": 0
    }));
    let split = role_split_count(EqualsFilter {
        dimension: "y".to_string(),
        value: "0".to_string(),
    });
    assert_eq!(single, 2, "oracle legacy answer: y=0 matches 2 rows");
    assert_eq!(
        split, single,
        "role-split numeric equality must equal the single-binary answer"
    );
}

/// `y = 7` stays exact on both paths (the null→0 coercion must not
/// widen matching).  Oracle legacy answer: 1.
#[test]
fn numeric_equality_filter_nonzero_stays_exact_on_both_paths() {
    latch_legacy();
    let single = single_binary_count(json!({
        "type": "selector", "dimension": "y", "value": 7
    }));
    let split = role_split_count(EqualsFilter {
        dimension: "y".to_string(),
        value: "7".to_string(),
    });
    assert_eq!(single, 1);
    assert_eq!(split, single);
}

// ---------------------------------------------------------------------------
// H2 — numeric groupBy key merging + emitted key values
// ---------------------------------------------------------------------------

/// Grouping the numeric column `y`: the null/missing cell keys as the
/// coerced default 0 and MERGES with the stored-0 row on both paths;
/// the emitted key is the JSON number.  Oracle legacy answer:
/// `{y: 0, cnt: 2}` and `{y: 7, cnt: 1}`.
#[test]
fn numeric_group_by_merges_null_into_zero_group_on_both_paths() {
    latch_legacy();
    // Single-binary.
    let q: GroupByQuery = serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["y"],
        "aggregations": [{"type": "count", "name": "cnt"}],
    }))
    .expect("groupBy parses");
    let single = q
        .execute(&single_binary_segment())
        .expect("single-binary executes");
    let mut single_rows: Vec<(serde_json::Value, i64)> = single
        .iter()
        .map(|r| {
            (
                r.event.get("y").cloned().unwrap_or(serde_json::Value::Null),
                r.event
                    .get("cnt")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(-1),
            )
        })
        .collect();
    single_rows.sort_by_key(|(v, _)| v.to_string());

    // Role-split.
    let q = NativeQuery::GroupBy(GroupBySpec {
        data_source: "ds".to_string(),
        dimensions: vec!["y".into()],
        aggregations: vec![Aggregation::Count {
            name: "cnt".to_string(),
        }],
        filter: None,
        having: None,
        sort: None,
        limit: None,
    });
    let NativeQueryResult::GroupBy(rows) = q.execute(&wire_segment()) else {
        panic!("groupBy result expected");
    };
    let mut split_rows: Vec<(serde_json::Value, i64)> = rows
        .iter()
        .map(|r| {
            (
                r.get("y").cloned().unwrap_or(serde_json::Value::Null),
                r.get("cnt")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(-1),
            )
        })
        .collect();
    split_rows.sort_by_key(|(v, _)| v.to_string());

    assert_eq!(
        single_rows,
        vec![(json!(0), 2), (json!(7), 1)],
        "oracle legacy answer: null merges into the 0 group"
    );
    assert_eq!(
        split_rows, single_rows,
        "role-split groupBy must equal the single-binary answer"
    );
}

// ---------------------------------------------------------------------------
// H1 — physically-absent column (grouping + filtering) on both paths
// ---------------------------------------------------------------------------

/// Grouping the ABSENT column `m` (no `outputType` on the wire → string
/// semantics): ONE merged null group on both paths, with the dimension
/// key EMITTED as JSON null.  Oracle legacy answer: `{m: null, cnt: 3}`.
#[test]
fn absent_column_group_by_emits_one_null_group_on_both_paths() {
    latch_legacy();
    let q: GroupByQuery = serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["m"],
        "aggregations": [{"type": "count", "name": "cnt"}],
    }))
    .expect("groupBy parses");
    let single = q
        .execute(&single_binary_segment())
        .expect("single-binary executes");
    assert_eq!(single.len(), 1);
    assert_eq!(single[0].event.get("m"), Some(&serde_json::Value::Null));
    assert_eq!(single[0].event.get("cnt"), Some(&json!(3)));

    let q = NativeQuery::GroupBy(GroupBySpec {
        data_source: "ds".to_string(),
        dimensions: vec!["m".into()],
        aggregations: vec![Aggregation::Count {
            name: "cnt".to_string(),
        }],
        filter: None,
        having: None,
        sort: None,
        limit: None,
    });
    let NativeQueryResult::GroupBy(rows) = q.execute(&wire_segment()) else {
        panic!("groupBy result expected");
    };
    assert_eq!(rows.len(), 1, "one merged null group expected: {rows:?}");
    assert_eq!(
        rows[0].get("m"),
        Some(&serde_json::Value::Null),
        "role-split must EMIT the absent dimension as JSON null (single-binary parity): {rows:?}"
    );
    assert_eq!(rows[0].get("cnt"), Some(&json!(3)));
}

/// Filtering the ABSENT column `m`: the `''` (null) selector matches
/// every row on both paths (an absent column IS a null string column —
/// Druid's missing-column semantics); a numeric literal matches nothing
/// on both paths.  Oracle legacy answers: 3 and 0.
#[test]
fn absent_column_filters_agree_on_both_paths() {
    latch_legacy();
    // '' (null) selector — matches all rows.
    let single = single_binary_count(json!({
        "type": "selector", "dimension": "m", "value": null
    }));
    let split = role_split_count(EqualsFilter {
        dimension: "m".to_string(),
        value: String::new(),
    });
    assert_eq!(single, 3, "oracle: absent column IS the null string column");
    assert_eq!(split, single);

    // Numeric literal — never equals the null string; H4 keeps the
    // empty-match bucket comparable (cnt 0) instead of [].
    let single = single_binary_count(json!({
        "type": "selector", "dimension": "m", "value": 0
    }));
    let split = role_split_count(EqualsFilter {
        dimension: "m".to_string(),
        value: "0".to_string(),
    });
    assert_eq!(
        single, 0,
        "oracle: numeric literal never matches the null string"
    );
    assert_eq!(split, single);
}

// ---------------------------------------------------------------------------
// H3 — IS NULL through the SQL bridge
// ---------------------------------------------------------------------------

/// `WHERE s IS NULL` must TRANSLATE under the latch (string IS NULL is
/// the `''`-equality selector) and answer identically to the
/// single-binary null filter.  Oracle legacy answer: 2 (`''` rows).
#[test]
fn sql_bridge_string_is_null_translates_and_matches_merged_nulls() {
    latch_legacy();
    let bridged = translate_sql("SELECT COUNT(*) AS cnt FROM ds WHERE s IS NULL", &schema())
        .expect("legacy string IS NULL must translate");
    let NativeQuery::Timeseries(spec) = bridged.native_query else {
        panic!("timeseries expected, got {:?}", bridged.native_query);
    };
    let filter = spec.filter.clone().expect("filter present");
    assert_eq!(filter.dimension, "s");
    assert_eq!(filter.value, "", "IS NULL is the ''-equality selector");

    let NativeQueryResult::Timeseries(buckets) =
        NativeQuery::Timeseries(spec).execute(&wire_segment())
    else {
        panic!("timeseries result expected");
    };
    let split = buckets[0]
        .result
        .get("cnt")
        .and_then(serde_json::Value::as_i64)
        .expect("cnt");

    let single = single_binary_count(json!({"type": "null", "column": "s"}));
    assert_eq!(single, 2, "oracle legacy answer: '' rows are the null rows");
    assert_eq!(split, single);
}

/// `WHERE y IS NULL` (numeric) must also translate — and match NO rows
/// on both paths (numeric cells are never null under the latch; they
/// read as the coerced 0 defaults).  Oracle legacy answer: 0.
#[test]
fn sql_bridge_numeric_is_null_translates_and_matches_nothing() {
    latch_legacy();
    let bridged = translate_sql("SELECT COUNT(*) AS cnt FROM ds WHERE y IS NULL", &schema())
        .expect("legacy numeric IS NULL must translate");
    let NativeQuery::Timeseries(spec) = bridged.native_query else {
        panic!("timeseries expected, got {:?}", bridged.native_query);
    };
    let NativeQueryResult::Timeseries(part) =
        NativeQuery::Timeseries(spec.clone()).execute(&wire_segment())
    else {
        panic!("timeseries result expected");
    };
    // W-B H2: the empty-match bucket is a BROKER synthesis (post-merge
    // fill), not a per-segment one.
    let buckets =
        legacy_fill_empty_timeseries(merge_timeseries(vec![part], &spec.aggregations), &spec);
    assert_eq!(
        buckets.len(),
        1,
        "H4: the empty match set still emits the bucket: {buckets:?}"
    );
    let split = buckets[0]
        .result
        .get("cnt")
        .and_then(serde_json::Value::as_i64)
        .expect("cnt");

    let single = single_binary_count(json!({"type": "null", "column": "y"}));
    assert_eq!(
        single, 0,
        "oracle legacy answer: numeric IS NULL matches nothing"
    );
    assert_eq!(split, single);
}

// ---------------------------------------------------------------------------
// H4 — empty-match timeseries bucket
// ---------------------------------------------------------------------------

/// A filter matching NOTHING still emits the single required bucket
/// with the legacy empty-aggregate defaults (count 0 / longSum 0 /
/// doubleSum 0.0) on both paths — granularity `all` AND bucketed.
#[test]
fn empty_match_timeseries_emits_legacy_default_bucket_on_both_paths() {
    latch_legacy();
    // Single-binary (granularity all).
    let q: TimeseriesQuery = serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "granularity": "all",
        "filter": {"type": "selector", "dimension": "s", "value": "zzz"},
        "aggregations": [
            {"type": "count", "name": "cnt"},
            {"type": "longSum", "name": "total", "fieldName": "y"},
            {"type": "doubleSum", "name": "dtotal", "fieldName": "y"}
        ],
    }))
    .expect("timeseries parses");
    let single = q
        .execute(&single_binary_segment())
        .expect("single-binary executes");
    assert_eq!(single.len(), 1, "single-binary emits the empty bucket");

    // Role-split (granularity all AND bucketed), driven through the full
    // path shape: per-segment execute (returns `[]` on an empty match —
    // W-B H2) → broker merge → post-merge legacy empty-result fill.
    for granularity_ms in [0_i64, 60_000] {
        let spec = TimeseriesSpec {
            data_source: "ds".to_string(),
            granularity_ms,
            aggregations: vec![
                Aggregation::Count {
                    name: "cnt".to_string(),
                },
                Aggregation::LongSum {
                    name: "total".to_string(),
                    field_name: "y".to_string(),
                },
                Aggregation::DoubleSum {
                    name: "dtotal".to_string(),
                    field_name: "y".to_string(),
                },
            ],
            filter: Some(EqualsFilter {
                dimension: "s".to_string(),
                value: "zzz".to_string(),
            }),
            intervals: vec![INTERVAL.to_string()],
        };
        let q = NativeQuery::Timeseries(spec.clone());
        let NativeQueryResult::Timeseries(part) = q.execute(&wire_segment()) else {
            panic!("timeseries result expected");
        };
        let buckets =
            legacy_fill_empty_timeseries(merge_timeseries(vec![part], &spec.aggregations), &spec);
        assert_eq!(
            buckets.len(),
            1,
            "H4: role-split must emit the required empty bucket (gran {granularity_ms}): {buckets:?}"
        );
        assert_eq!(buckets[0].timestamp_ms, 0);
        for (name, expect) in [
            ("cnt", json!(0)),
            ("total", json!(0)),
            ("dtotal", json!(0.0)),
        ] {
            let split_v = buckets[0].result.get(name).cloned();
            let single_v = single[0].result.get(name).cloned();
            assert_eq!(
                split_v.as_ref().and_then(serde_json::Value::as_f64),
                Some(expect.as_f64().unwrap_or(f64::NAN)),
                "legacy empty-agg default for {name} (gran {granularity_ms}): {buckets:?}"
            );
            assert_eq!(
                split_v.as_ref().and_then(serde_json::Value::as_f64),
                single_v.as_ref().and_then(serde_json::Value::as_f64),
                "role-split empty bucket must equal the single-binary bucket for {name}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// H2 — scan typed defaults for declared-but-missing fields
// ---------------------------------------------------------------------------

/// A projected column the segment declares emits its canonical legacy
/// read even when the row omits it: numeric default 0, canonical null
/// for strings — matching the single-binary scan of the twin.
#[test]
fn scan_emits_typed_defaults_for_declared_missing_fields_on_both_paths() {
    latch_legacy();
    // Single-binary scan.
    let q: ScanQuery = serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "columns": ["s", "y"],
    }))
    .expect("scan parses");
    let single = q
        .execute(&single_binary_segment())
        .expect("single-binary executes");
    assert_eq!(single.events.len(), 3);

    // Role-split scan.
    let q = NativeQuery::Scan(ScanSpec {
        data_source: "ds".to_string(),
        columns: Some(vec!["s".to_string(), "y".to_string()]),
        limit: None,
        filter: None,
    });
    let NativeQueryResult::Scan(rows) = q.execute(&wire_segment()) else {
        panic!("scan result expected");
    };
    assert_eq!(rows.len(), 3);

    // Row 1 (the null-`y` row): oracle legacy read is s=null, y=0.
    assert_eq!(rows[1].get("s"), Some(&serde_json::Value::Null));
    assert_eq!(
        rows[1].get("y"),
        Some(&json!(0)),
        "H2: a declared-but-missing numeric field must emit the typed default 0: {rows:?}"
    );
    // Value-level identity with the single-binary scan, row by row.
    for (i, row) in rows.iter().enumerate() {
        for col in ["s", "y"] {
            let single_v = single.events[i].get(col).cloned();
            let split_v = row.get(col).cloned();
            assert_eq!(
                split_v, single_v,
                "scan cell ({i}, {col}) must be identical on both paths"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// H1 — physically-absent column WITH a declared `outputType`
//
// The DimensionSpec's consuming `outputType` must survive the wire (the
// SQL bridge used to reduce every dimension to a bare name), so the
// role-split executor canonicalizes an absent grouping column by its
// DECLARED kind (LONG → 0, DOUBLE → 0.0) exactly like the single-binary
// coercion (`dim_spec::coerce_group_key_to_output_type` under the
// latch).  Invariant: identical group key on both paths, and absent
// rows NEVER split from real-zero rows.
// ---------------------------------------------------------------------------

/// GroupBy over the ABSENT column `m` with an explicit `outputType`.
fn absent_group_by_query(output_type: &str) -> GroupByQuery {
    serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{"type": "default", "dimension": "m", "outputType": output_type}],
        "aggregations": [{"type": "count", "name": "cnt"}],
    }))
    .expect("groupBy parses")
}

/// `outputType: LONG` over the ABSENT column `m`: the declared kind
/// coerces the missing cell to the legacy default 0 on BOTH paths.
/// Oracle legacy answer: ONE group `{m: 0, cnt: 3}`.
#[test]
fn absent_column_long_output_type_groups_as_zero_on_both_paths() {
    latch_legacy();
    let q = absent_group_by_query("LONG");
    let single = q
        .execute(&single_binary_segment())
        .expect("single-binary executes");
    assert_eq!(single.len(), 1, "one coerced-default group: {single:?}");
    assert_eq!(
        single[0].event.get("m"),
        Some(&json!(0)),
        "oracle legacy answer: absent LONG column keys as 0"
    );
    assert_eq!(single[0].event.get("cnt"), Some(&json!(3)));

    let wire = translate_query(&DruidQuery::GroupBy(q)).expect("bridge translates");
    let NativeQueryResult::GroupBy(rows) = wire.execute(&wire_segment()) else {
        panic!("groupBy result expected");
    };
    assert_eq!(rows.len(), 1, "one merged zero group expected: {rows:?}");
    assert_eq!(
        rows[0].get("m"),
        Some(&json!(0)),
        "role-split must key the absent LONG column as 0 (single-binary parity): {rows:?}"
    );
    assert_eq!(rows[0].get("cnt"), Some(&json!(3)));
}

/// `outputType: DOUBLE` over the ABSENT column `m`: legacy default 0.0
/// on BOTH paths.  Oracle legacy answer: ONE group `{m: 0.0, cnt: 3}`.
#[test]
fn absent_column_double_output_type_groups_as_zero_on_both_paths() {
    latch_legacy();
    let q = absent_group_by_query("DOUBLE");
    let single = q
        .execute(&single_binary_segment())
        .expect("single-binary executes");
    assert_eq!(single.len(), 1, "one coerced-default group: {single:?}");
    assert_eq!(
        single[0].event.get("m"),
        Some(&json!(0.0)),
        "oracle legacy answer: absent DOUBLE column keys as 0.0"
    );
    assert_eq!(single[0].event.get("cnt"), Some(&json!(3)));

    let wire = translate_query(&DruidQuery::GroupBy(q)).expect("bridge translates");
    let NativeQueryResult::GroupBy(rows) = wire.execute(&wire_segment()) else {
        panic!("groupBy result expected");
    };
    assert_eq!(rows.len(), 1, "one merged zero group expected: {rows:?}");
    assert_eq!(
        rows[0].get("m"),
        Some(&json!(0.0)),
        "role-split must key the absent DOUBLE column as 0.0 (single-binary parity): {rows:?}"
    );
    assert_eq!(rows[0].get("cnt"), Some(&json!(3)));
}

/// Schema evolution across segments: a segment DECLARING `m` (stored
/// zeros) and a segment LACKING `m` must fold into ONE broker-merged
/// group under `outputType: LONG` — absent rows never split from
/// real-zero rows.  Oracle legacy answer: `{m: 0, cnt: 5}`.
#[test]
fn absent_column_long_output_type_merges_with_real_zero_rows_at_broker() {
    latch_legacy();
    let declaring = Segment::parse_jsonl(concat!(
        r#"{"segmentId":"s2","dataSource":"ds","columns":[{"name":"__time","type":"long"},{"name":"m","type":"long"}]}"#,
        "\n",
        r#"{"__time":0,"m":0}"#,
        "\n",
        r#"{"__time":60000,"m":0}"#,
        "\n",
    ))
    .expect("declaring twin parses");

    let q = absent_group_by_query("LONG");
    let NativeQuery::GroupBy(spec) =
        translate_query(&DruidQuery::GroupBy(q)).expect("bridge translates")
    else {
        panic!("groupBy wire query expected");
    };
    let NativeQueryResult::GroupBy(part_absent) =
        NativeQuery::GroupBy(spec.clone()).execute(&wire_segment())
    else {
        panic!("groupBy result expected");
    };
    let NativeQueryResult::GroupBy(part_zero) =
        NativeQuery::GroupBy(spec.clone()).execute(&declaring)
    else {
        panic!("groupBy result expected");
    };
    let merged = merge_group_by(vec![part_absent, part_zero], &spec);
    assert_eq!(
        merged.len(),
        1,
        "absent-column rows must MERGE with real-zero rows, never split: {merged:?}"
    );
    assert_eq!(merged[0].get("m"), Some(&json!(0)));
    assert_eq!(merged[0].get("cnt"), Some(&json!(5)));
}

/// NO `outputType` (the wire default — indistinguishable from an
/// explicit STRING): the existing header-kind/String fallback must be
/// PRESERVED — ONE null group on both paths (the ANSI-parity default,
/// same oracle as `absent_column_group_by_emits_one_null_group_on_both_paths`).
#[test]
fn absent_column_without_output_type_keeps_null_group_through_bridge() {
    latch_legacy();
    let q: GroupByQuery = serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": ["m"],
        "aggregations": [{"type": "count", "name": "cnt"}],
    }))
    .expect("groupBy parses");
    let single = q
        .execute(&single_binary_segment())
        .expect("single-binary executes");
    assert_eq!(single.len(), 1);
    assert_eq!(single[0].event.get("m"), Some(&serde_json::Value::Null));

    let wire = translate_query(&DruidQuery::GroupBy(q)).expect("bridge translates");
    let NativeQueryResult::GroupBy(rows) = wire.execute(&wire_segment()) else {
        panic!("groupBy result expected");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("m"),
        Some(&serde_json::Value::Null),
        "no outputType → the existing String/null default must be preserved: {rows:?}"
    );
    assert_eq!(rows[0].get("cnt"), Some(&json!(3)));
}

// ---------------------------------------------------------------------------
// W-B role-split H1 — the broker's SQL formatter renders the merged
// ''/null STRING as "" (Druid's SQL-wire face), identical to the
// single-binary SQL formatter
//
// Both formatters now share ONE canonicalization point
// (`ferrodruid_sql::legacy_string_cell`).  The single-binary side of the
// identity is pinned against the measured Druid 27 oracle in
// `ferrodruid-rest/tests/legacy_null_sql_e2e.rs`
// (`sql_group_by_merges_null_and_empty` renders the merged group as `""`
// per `group_strcol.json`; `sql_projection_renders_empty_string_and_zeros`
// per `select_all_rows.json`).  These tests pin the SAME oracle cells on
// the role-split formatter over the wire twin.
// ---------------------------------------------------------------------------

/// GroupBy over the string column `s`: the merged ''/null group's key
/// cell must render `""` on the role-split SQL wire (native surfaces
/// keep JSON null — that contrast is pinned by the role-split executor
/// tests).  Oracle SQL answer: `("", 2)` and `("a", 1)`.
#[test]
fn role_split_sql_group_by_renders_merged_null_string_as_empty_string() {
    latch_legacy();
    let bridged = translate_sql("SELECT s, COUNT(*) AS cnt FROM ds GROUP BY s", &schema())
        .expect("groupBy translates");
    let NativeQuery::GroupBy(spec) = bridged.native_query.clone() else {
        panic!(
            "groupBy wire query expected, got {:?}",
            bridged.native_query
        );
    };
    let NativeQueryResult::GroupBy(part) =
        NativeQuery::GroupBy(spec.clone()).execute(&wire_segment())
    else {
        panic!("groupBy result expected");
    };
    let merged = NativeQueryResult::GroupBy(merge_group_by(vec![part], &spec));
    let (columns, rows) = sql_response_rows(&bridged, &merged);
    assert_eq!(columns, vec!["s".to_string(), "cnt".to_string()]);
    let mut cells: Vec<(serde_json::Value, i64)> = rows
        .iter()
        .map(|r| {
            (
                r.first().cloned().unwrap_or(serde_json::Value::Null),
                r.get(1).and_then(serde_json::Value::as_i64).unwrap_or(-1),
            )
        })
        .collect();
    cells.sort_by_key(|(v, _)| v.to_string());
    assert_eq!(
        cells,
        vec![(json!(""), 2), (json!("a"), 1)],
        "role-split SQL must render the merged ''/null group key as \"\" \
         (single-binary + Druid 27 `group_strcol.json` oracle), never JSON null"
    );
}

/// Scan projection over `s`, `y`: the merged ''/null `s` cells render
/// `""` on the role-split SQL wire and the null-`y` cell renders the
/// coerced `0` — the same cells the single-binary SQL wire pins per
/// `select_all_rows.json`.
#[test]
fn role_split_sql_scan_renders_merged_null_string_as_empty_string() {
    latch_legacy();
    let bridged = translate_sql("SELECT s, y FROM ds", &schema()).expect("scan translates");
    let NativeQuery::Scan(spec) = bridged.native_query.clone() else {
        panic!("scan wire query expected, got {:?}", bridged.native_query);
    };
    let NativeQueryResult::Scan(part) = NativeQuery::Scan(spec.clone()).execute(&wire_segment())
    else {
        panic!("scan result expected");
    };
    let merged = NativeQueryResult::Scan(ferrodruid_rpc::native_query::merge_scan(
        vec![part],
        spec.limit,
    ));
    let (columns, rows) = sql_response_rows(&bridged, &merged);
    assert_eq!(columns, vec!["s".to_string(), "y".to_string()]);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], json!("a"));
    for (i, row) in rows.iter().enumerate().skip(1) {
        assert_eq!(
            row[0],
            json!(""),
            "role-split SQL scan row {i} must render the merged ''/null `s` as \"\": {rows:?}"
        );
    }
    assert_eq!(
        rows[1][1],
        json!(0),
        "the null `y` cell keeps its coerced legacy 0 on the SQL wire"
    );
}

// ---------------------------------------------------------------------------
// W-B role-split H2 — the legacy empty-RESULT bucket is a BROKER
// synthesis (post-merge, interval-start-anchored), never per segment
// ---------------------------------------------------------------------------

/// 2024-01-01T00:00:00Z.
const JAN1_MS: i64 = 1_704_067_200_000;
const JAN_INTERVAL: &str = "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z";

/// Wire shard twin: two rows tagged `tag` at `base` / `base + 60s`
/// with y = 5 / 7.
fn wire_shard_2024(tag: &str, base: i64) -> Segment {
    let header = concat!(
        r#"{"segmentId":"s2024","dataSource":"ds","columns":[{"name":"__time","type":"long"},"#,
        r#"{"name":"tag","type":"string"},{"name":"y","type":"long"}]}"#,
    );
    let jsonl = format!(
        "{header}\n{{\"__time\":{t0},\"tag\":\"{tag}\",\"y\":5}}\n\
         {{\"__time\":{t1},\"tag\":\"{tag}\",\"y\":7}}\n",
        t0 = base,
        t1 = base + 60_000,
    );
    Segment::parse_jsonl(&jsonl).expect("2024 wire shard parses")
}

/// Single-binary twin of BOTH shards' rows in ONE segment (same logical
/// datasource content, single-binary sharding).
fn single_binary_2024_union() -> ferrodruid_segment::SegmentData {
    ferrodruid_segment::SegmentDataBuilder::new()
        .add_timestamp_column(vec![
            JAN1_MS,
            JAN1_MS + 60_000,
            JAN1_MS + 2 * 3_600_000,
            JAN1_MS + 2 * 3_600_000 + 60_000,
        ])
        .add_string_column(
            "tag",
            ["a", "a", "b", "b"]
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
        .add_long_column("y", true, vec![5, 7, 5, 7])
        .build()
        .expect("2024 single-binary twin builds")
}

fn hour_spec_2024(filter_value: &str) -> TimeseriesSpec {
    TimeseriesSpec {
        data_source: "ds".to_string(),
        granularity_ms: 3_600_000,
        aggregations: vec![
            Aggregation::Count {
                name: "cnt".to_string(),
            },
            Aggregation::LongSum {
                name: "total".to_string(),
                field_name: "y".to_string(),
            },
        ],
        filter: Some(EqualsFilter {
            dimension: "tag".to_string(),
            value: filter_value.to_string(),
        }),
        intervals: vec![JAN_INTERVAL.to_string()],
    }
}

fn single_binary_hour_2024(filter_value: &str) -> Vec<ferrodruid_query::TimeseriesResult> {
    let q: TimeseriesQuery = serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [JAN_INTERVAL],
        "granularity": "hour",
        "filter": {"type": "selector", "dimension": "tag", "value": filter_value},
        "aggregations": [
            {"type": "count", "name": "cnt"},
            {"type": "longSum", "name": "total", "fieldName": "y"}
        ],
    }))
    .expect("timeseries parses");
    q.execute(&single_binary_2024_union())
        .expect("single-binary executes")
}

/// A PARTIALLY-empty scatter (shard A matches, shard B doesn't) keeps
/// ONLY the real buckets — the empty shard must not plant a spurious
/// 1970 (epoch-0) bucket next to them.  Oracle: the single-binary query
/// over the SAME rows in one segment emits exactly one 2024 bucket.
#[test]
fn partially_empty_scatter_keeps_only_real_buckets_on_both_paths() {
    latch_legacy();
    let spec = hour_spec_2024("a");
    let shard_a = wire_shard_2024("a", JAN1_MS);
    let shard_b = wire_shard_2024("b", JAN1_MS + 2 * 3_600_000);
    let mut parts = Vec::new();
    for shard in [&shard_a, &shard_b] {
        let NativeQueryResult::Timeseries(part) =
            NativeQuery::Timeseries(spec.clone()).execute(shard)
        else {
            panic!("timeseries result expected");
        };
        parts.push(part);
    }
    let merged = legacy_fill_empty_timeseries(merge_timeseries(parts, &spec.aggregations), &spec);
    assert_eq!(
        merged.len(),
        1,
        "H2: the empty shard must contribute NO bucket — a spurious epoch-0 \
         bucket next to the real 2024 bucket is the R3 per-segment-synthesis \
         bug: {merged:?}"
    );
    assert_eq!(merged[0].timestamp_ms, JAN1_MS);
    assert_eq!(merged[0].result.get("cnt"), Some(&json!(2)));
    assert_eq!(merged[0].result.get("total"), Some(&json!(12)));

    // Single-binary oracle over the same logical rows (one segment).
    let single = single_binary_hour_2024("a");
    assert_eq!(single.len(), 1, "single-binary emits only the real bucket");
    assert_eq!(
        ferrodruid_query::parse_iso_millis(&single[0].timestamp),
        Some(merged[0].timestamp_ms),
        "role-split bucket timestamp must equal the single-binary bucket"
    );
    for key in ["cnt", "total"] {
        assert_eq!(
            merged[0]
                .result
                .get(key)
                .and_then(serde_json::Value::as_f64),
            single[0]
                .result
                .get(key)
                .and_then(serde_json::Value::as_f64),
            "role-split {key} must equal the single-binary value"
        );
    }
}

/// An ALL-empty scatter emits exactly ONE bucket anchored at the query
/// INTERVAL START (not epoch 0) with the legacy empty-agg defaults —
/// identical to the single-binary empty-bucket fill.
#[test]
fn all_empty_scatter_anchors_bucket_at_interval_start_on_both_paths() {
    latch_legacy();
    let spec = hour_spec_2024("zzz");
    let shard_a = wire_shard_2024("a", JAN1_MS);
    let shard_b = wire_shard_2024("b", JAN1_MS + 2 * 3_600_000);
    let mut parts = Vec::new();
    for shard in [&shard_a, &shard_b] {
        let NativeQueryResult::Timeseries(part) =
            NativeQuery::Timeseries(spec.clone()).execute(shard)
        else {
            panic!("timeseries result expected");
        };
        assert!(
            part.is_empty(),
            "H2: a shard with no matching rows returns NO buckets (the empty-\
             result bucket is a broker synthesis): {part:?}"
        );
        parts.push(part);
    }
    let merged = legacy_fill_empty_timeseries(merge_timeseries(parts, &spec.aggregations), &spec);
    assert_eq!(
        merged.len(),
        1,
        "exactly one synthesized bucket: {merged:?}"
    );
    assert_eq!(
        merged[0].timestamp_ms, JAN1_MS,
        "the legacy empty-result bucket anchors at the query interval start, \
         not epoch 0: {merged:?}"
    );
    assert_eq!(merged[0].result.get("cnt"), Some(&json!(0)));
    assert_eq!(merged[0].result.get("total"), Some(&json!(0)));

    // Single-binary oracle: same query, no matching rows.
    let single = single_binary_hour_2024("zzz");
    assert_eq!(single.len(), 1, "single-binary emits the empty fill bucket");
    assert_eq!(
        ferrodruid_query::parse_iso_millis(&single[0].timestamp),
        Some(merged[0].timestamp_ms),
        "role-split empty-bucket anchor must equal the single-binary anchor"
    );
    for key in ["cnt", "total"] {
        assert_eq!(
            merged[0]
                .result
                .get(key)
                .and_then(serde_json::Value::as_f64),
            single[0]
                .result
                .get(key)
                .and_then(serde_json::Value::as_f64),
            "role-split empty-bucket {key} must equal the single-binary value"
        );
    }
}

/// Granularity `all` keeps its epoch-0 anchor on BOTH paths even under a
/// 2024 interval (the single-binary `bucket_timestamp(_, all)` floors to
/// epoch 0 — `bucket_floor(_, 0)` mirrors it on the wire).
#[test]
fn all_empty_scatter_granularity_all_keeps_epoch_anchor_on_both_paths() {
    latch_legacy();
    let mut spec = hour_spec_2024("zzz");
    spec.granularity_ms = 0;
    let NativeQueryResult::Timeseries(part) =
        NativeQuery::Timeseries(spec.clone()).execute(&wire_shard_2024("a", JAN1_MS))
    else {
        panic!("timeseries result expected");
    };
    let merged =
        legacy_fill_empty_timeseries(merge_timeseries(vec![part], &spec.aggregations), &spec);
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged[0].timestamp_ms, 0,
        "granularity all floors to epoch 0"
    );

    let q: TimeseriesQuery = serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [JAN_INTERVAL],
        "granularity": "all",
        "filter": {"type": "selector", "dimension": "tag", "value": "zzz"},
        "aggregations": [
            {"type": "count", "name": "cnt"},
            {"type": "longSum", "name": "total", "fieldName": "y"}
        ],
    }))
    .expect("timeseries parses");
    let single = q
        .execute(&single_binary_2024_union())
        .expect("single-binary executes");
    assert_eq!(single.len(), 1);
    assert_eq!(
        ferrodruid_query::parse_iso_millis(&single[0].timestamp),
        Some(0),
        "single-binary `all` empty bucket sits at epoch 0"
    );
    assert_eq!(merged[0].result.get("cnt"), Some(&json!(0)));
}

// ---------------------------------------------------------------------------
// Role-split dimension ALIASING — input (read) vs output (emit) name
//
// `SELECT y AS alias, COUNT(*) FROM ds GROUP BY y` plans a DimensionSpec
// reading the PHYSICAL column `y` and emitting the alias.  The wire
// DimensionRef must carry BOTH names: the executor READS segment rows by
// the input name and EMITS the group key under the output name.  A
// DimensionRef that collapses to the output name alone makes the
// role-split executor read a NONEXISTENT column — under the legacy latch
// every row then canonicalizes to the missing-column default and ALL real
// groups silently collapse into one.
// ---------------------------------------------------------------------------

/// Plan `sql`, execute the planned single-binary groupBy on the typed
/// twin, and return its `(dimension value, cnt)` pairs keyed by `dim_key`
/// (sorted for order-independent comparison).
fn single_binary_group_rows(sql: &str, dim_key: &str) -> Vec<(serde_json::Value, i64)> {
    let stmt = parse_druid_sql(sql).expect("SQL parses");
    let planned = plan_sql(&stmt, &schema()).expect("SQL plans");
    let DruidQuery::GroupBy(q) = &planned.native_query else {
        panic!("groupBy plan expected, got {:?}", planned.native_query);
    };
    let single = q
        .execute(&single_binary_segment())
        .expect("single-binary executes");
    let mut rows: Vec<(serde_json::Value, i64)> = single
        .iter()
        .map(|r| {
            (
                r.event
                    .get(dim_key)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                r.event
                    .get("cnt")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(-1),
            )
        })
        .collect();
    rows.sort_by_key(|(v, _)| v.to_string());
    rows
}

/// Translate `sql` onto the wire, execute it on the wire twin, broker-merge,
/// and return the merged `(dimension value, cnt)` pairs keyed by `dim_key`
/// plus the SQL-formatted `(columns, rows)`.
#[allow(clippy::type_complexity)]
fn role_split_group_rows(
    sql: &str,
    dim_key: &str,
) -> (
    Vec<(serde_json::Value, i64)>,
    Vec<String>,
    Vec<Vec<serde_json::Value>>,
) {
    let bridged = translate_sql(sql, &schema()).expect("bridge translates");
    let NativeQuery::GroupBy(spec) = bridged.native_query.clone() else {
        panic!(
            "groupBy wire query expected, got {:?}",
            bridged.native_query
        );
    };
    let NativeQueryResult::GroupBy(part) =
        NativeQuery::GroupBy(spec.clone()).execute(&wire_segment())
    else {
        panic!("groupBy result expected");
    };
    let merged = merge_group_by(vec![part], &spec);
    let mut rows: Vec<(serde_json::Value, i64)> = merged
        .iter()
        .map(|r| {
            (
                r.get(dim_key).cloned().unwrap_or(serde_json::Value::Null),
                r.get("cnt")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(-1),
            )
        })
        .collect();
    rows.sort_by_key(|(v, _)| v.to_string());
    let (columns, sql_rows) = sql_response_rows(&bridged, &NativeQueryResult::GroupBy(merged));
    (rows, columns, sql_rows)
}

/// NUMERIC alias: `SELECT y AS alias … GROUP BY y` must return one group
/// per distinct `y` on BOTH paths, keyed/labeled `alias`.  Oracle legacy
/// answer: `{alias: 0, cnt: 2}` (null merges into 0) and `{alias: 7, cnt: 1}`
/// — NEVER a single collapsed group.
#[test]
fn aliased_numeric_group_by_keeps_real_groups_on_both_paths() {
    latch_legacy();
    let sql = "SELECT y AS alias, COUNT(*) AS cnt FROM ds GROUP BY y";
    let single_rows = single_binary_group_rows(sql, "alias");
    assert_eq!(
        single_rows,
        vec![(json!(0), 2), (json!(7), 1)],
        "oracle legacy answer: two real groups under the alias key"
    );

    let (split_rows, columns, sql_rows) = role_split_group_rows(sql, "alias");
    assert_eq!(
        split_rows, single_rows,
        "role-split aliased groupBy must equal the single-binary answer \
         (the executor must READ `y`, not the alias) — a single collapsed \
         group means the input name was lost on the wire"
    );

    // The SQL response labels the dimension column with the alias.
    assert_eq!(columns, vec!["alias".to_string(), "cnt".to_string()]);
    let mut cells: Vec<(serde_json::Value, i64)> = sql_rows
        .iter()
        .map(|r| {
            (
                r.first().cloned().unwrap_or(serde_json::Value::Null),
                r.get(1).and_then(serde_json::Value::as_i64).unwrap_or(-1),
            )
        })
        .collect();
    cells.sort_by_key(|(v, _)| v.to_string());
    assert_eq!(
        cells,
        vec![(json!(0), 2), (json!(7), 1)],
        "role-split SQL must surface both groups under the alias column"
    );
}

/// H1 ANSI-restore guard (the latch-side inverse): under the LEGACY
/// latch the bridge keeps the R9 rich `DimensionRef` — physical READ
/// name + output alias + declared `outputType` — and the Druid-style
/// object wire spelling.  The ANSI (latch-off) bridge maps to the
/// pre-W-B bare selected name instead (pinned in the `sql_bridge` unit
/// tests, which run unlatched).
#[test]
fn legacy_bridge_dimension_carries_input_output_and_type() {
    latch_legacy();
    let bridged = translate_sql(
        "SELECT y AS alias, COUNT(*) AS cnt FROM ds GROUP BY y",
        &schema(),
    )
    .expect("bridge translates");
    let NativeQuery::GroupBy(ref spec) = bridged.native_query else {
        panic!(
            "groupBy wire query expected, got {:?}",
            bridged.native_query
        );
    };
    assert_eq!(
        spec.dimensions,
        vec![DimensionRef {
            name: "y".to_string(),
            output_name: Some("alias".to_string()),
            output_type: Some(DimensionOutputType::Long),
        }],
        "the legacy latch must keep the input/output/type split (R9)"
    );
    let wire = serde_json::to_string(&bridged.native_query).expect("ser");
    assert!(
        wire.contains(r#"{"dimension":"y","outputName":"alias","outputType":"LONG"}"#),
        "the legacy wire must carry the Druid-style object spelling: {wire}"
    );
}

/// Latch-leak-audit guard (the latch-side inverse of the ANSI
/// `ansi_timeseries_wire_omits_intervals` pin): the LEGACY wire
/// timeseries carries the planner's query intervals — the broker's
/// post-merge empty-bucket anchor (H2) — while ANSI keeps the pre-W-B
/// wire bytes (no `intervals` key).
#[test]
fn legacy_timeseries_wire_carries_intervals() {
    latch_legacy();
    let bridged = translate_sql("SELECT COUNT(*) AS cnt FROM ds", &schema()).expect("translates");
    let NativeQuery::Timeseries(ref spec) = bridged.native_query else {
        panic!(
            "timeseries wire query expected, got {:?}",
            bridged.native_query
        );
    };
    assert!(
        !spec.intervals.is_empty(),
        "the legacy wire must carry the planner intervals for the \
         broker's empty-bucket anchor"
    );
}

/// STRING alias: `SELECT s AS label … GROUP BY s` — the merged ''/null
/// group and the `"a"` group both survive on BOTH paths under the alias.
#[test]
fn aliased_string_group_by_keeps_real_groups_on_both_paths() {
    latch_legacy();
    let sql = "SELECT s AS label, COUNT(*) AS cnt FROM ds GROUP BY s";
    let single_rows = single_binary_group_rows(sql, "label");
    assert_eq!(
        single_rows,
        vec![(json!("a"), 1), (serde_json::Value::Null, 2)],
        "oracle legacy answer: the 'a' group plus the merged ''/null group"
    );

    let (split_rows, columns, sql_rows) = role_split_group_rows(sql, "label");
    assert_eq!(
        split_rows, single_rows,
        "role-split aliased string groupBy must equal the single-binary \
         answer (READ `s`, EMIT `label`)"
    );

    assert_eq!(columns, vec!["label".to_string(), "cnt".to_string()]);
    let mut cells: Vec<(serde_json::Value, i64)> = sql_rows
        .iter()
        .map(|r| {
            (
                r.first().cloned().unwrap_or(serde_json::Value::Null),
                r.get(1).and_then(serde_json::Value::as_i64).unwrap_or(-1),
            )
        })
        .collect();
    cells.sort_by_key(|(v, _)| v.to_string());
    assert_eq!(
        cells,
        vec![(json!(""), 2), (json!("a"), 1)],
        "role-split SQL renders the merged ''/null group as \"\" under the alias"
    );
}

/// topN alias: `SELECT s AS page … GROUP BY s ORDER BY cnt DESC LIMIT 5`
/// plans a native topN whose dimension reads `s` and emits `page`.  Both
/// paths must rank the SAME two groups — never one collapsed group of 3.
#[test]
fn aliased_top_n_keeps_real_groups_on_both_paths() {
    latch_legacy();
    let sql = "SELECT s AS page, COUNT(*) AS cnt FROM ds GROUP BY s ORDER BY cnt DESC LIMIT 5";
    let stmt = parse_druid_sql(sql).expect("SQL parses");
    let planned = plan_sql(&stmt, &schema()).expect("SQL plans");
    let DruidQuery::TopN(q) = &planned.native_query else {
        panic!("topN plan expected, got {:?}", planned.native_query);
    };
    let single = q
        .execute(&single_binary_segment())
        .expect("single-binary executes");
    assert_eq!(single.len(), 1, "one time bucket expected: {single:?}");
    let single_rows: Vec<(serde_json::Value, i64)> = single[0]
        .result
        .iter()
        .map(|r| {
            (
                r.get("page").cloned().unwrap_or(serde_json::Value::Null),
                r.get("cnt")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(-1),
            )
        })
        .collect();
    assert_eq!(
        single_rows,
        vec![(serde_json::Value::Null, 2), (json!("a"), 1)],
        "oracle legacy answer: merged ''/null group (2) ranks above 'a' (1)"
    );

    let bridged = translate_sql(sql, &schema()).expect("bridge translates");
    let NativeQuery::TopN(spec) = bridged.native_query.clone() else {
        panic!("topN wire query expected, got {:?}", bridged.native_query);
    };
    let NativeQueryResult::TopN(part) = NativeQuery::TopN(spec.clone()).execute(&wire_segment())
    else {
        panic!("topN result expected");
    };
    let merged = merge_top_n(vec![part], &spec);
    let split_rows: Vec<(serde_json::Value, i64)> = merged
        .iter()
        .map(|r| {
            (
                r.get("page").cloned().unwrap_or(serde_json::Value::Null),
                r.get("cnt")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(-1),
            )
        })
        .collect();
    assert_eq!(
        split_rows, single_rows,
        "role-split aliased topN must equal the single-binary ranking \
         (READ `s`, EMIT `page`) — one collapsed group means the input \
         name was lost on the wire"
    );
}
