// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-B legacy null mode — role-split historical executor (H4).
//!
//! **This test binary latches the process-global legacy-null mode ON** in
//! every test; no ANSI-mode test may be added here (own-process latch).
//!
//! The single-binary query path reads every row value through ONE
//! read-side canonicalization point
//! (`ferrodruid-query::helpers::column_value_at`: legacy `''` IS the
//! null value).  The role-split historical executes
//! `ferrodruid-rpc::native_query` against raw JSON-Lines rows instead —
//! these tests pin that the SAME legacy semantics apply on the
//! scatter/role-split path:
//!
//! * a `''` (null) equality filter matches BOTH `""` rows and
//!   null/missing rows (the merged ''/null value);
//! * scan / groupBy output renders a stored `""` as canonical JSON null
//!   (Druid 27 renders both `''` and absent rows as null in every
//!   native surface).

use ferrodruid_deep_storage::Segment;
use ferrodruid_rpc::native_query::{
    Aggregation, EqualsFilter, NativeQuery, NativeQueryResult, ScanSpec, TimeseriesSpec,
};

fn latch_legacy() {
    assert!(
        ferrodruid_common::null_mode::init_legacy_null_mode(true),
        "this test binary requires the legacy-null latch"
    );
}

/// 3 rows: a real string, an empty string, and a missing value — under
/// legacy the latter two are ONE merged ''/null value.
fn fixture() -> Segment {
    let jsonl = concat!(
        r#"{"segmentId":"s1","dataSource":"ds","columns":[{"name":"__time","type":"long"},{"name":"s","type":"string"},{"name":"y","type":"long"}]}"#,
        "\n",
        r#"{"__time":0,"s":"a","y":10}"#,
        "\n",
        r#"{"__time":0,"s":"","y":20}"#,
        "\n",
        r#"{"__time":0,"y":30}"#,
        "\n",
    );
    Segment::parse_jsonl(jsonl).expect("fixture parses")
}

/// The `''` selector is the null selector under legacy: it matches the
/// `""` row AND the missing row (merged ''/null), so the scan returns 2
/// rows, not 1.
#[test]
fn scan_empty_string_filter_matches_merged_null_rows() {
    latch_legacy();
    let q = NativeQuery::Scan(ScanSpec {
        data_source: "ds".to_string(),
        columns: Some(vec!["y".to_string()]),
        limit: None,
        filter: Some(EqualsFilter {
            dimension: "s".to_string(),
            value: String::new(),
        }),
    });
    let NativeQueryResult::Scan(rows) = q.execute(&fixture()) else {
        panic!("scan result expected");
    };
    let ys: Vec<i64> = rows
        .iter()
        .filter_map(|r| r.get("y").and_then(serde_json::Value::as_i64))
        .collect();
    assert_eq!(
        ys,
        vec![20, 30],
        "legacy '' filter must match BOTH the \"\" row and the missing row"
    );
}

/// Legacy scan output renders a stored `""` as canonical JSON null —
/// exactly what the single-binary `column_value_at` read produces.
#[test]
fn scan_renders_empty_string_as_canonical_null() {
    latch_legacy();
    let q = NativeQuery::Scan(ScanSpec {
        data_source: "ds".to_string(),
        columns: Some(vec!["s".to_string(), "y".to_string()]),
        limit: None,
        filter: None,
    });
    let NativeQueryResult::Scan(rows) = q.execute(&fixture()) else {
        panic!("scan result expected");
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("s"), Some(&serde_json::json!("a")));
    assert_eq!(
        rows[1].get("s"),
        Some(&serde_json::Value::Null),
        "legacy scan must render the stored \"\" as canonical null"
    );
}

/// The same '' ≡ null equivalence through the timeseries filter path.
#[test]
fn timeseries_count_with_empty_string_filter_counts_merged_nulls() {
    latch_legacy();
    let q = NativeQuery::Timeseries(TimeseriesSpec {
        data_source: "ds".to_string(),
        granularity_ms: 0,
        aggregations: vec![Aggregation::Count {
            name: "cnt".to_string(),
        }],
        filter: Some(EqualsFilter {
            dimension: "s".to_string(),
            value: String::new(),
        }),
        intervals: Vec::new(),
    });
    let NativeQueryResult::Timeseries(buckets) = q.execute(&fixture()) else {
        panic!("timeseries result expected");
    };
    assert_eq!(buckets.len(), 1);
    assert_eq!(
        buckets[0].result.get("cnt"),
        Some(&serde_json::json!(2)),
        "legacy count over the '' (null) filter must see the merged ''/null rows"
    );
}
