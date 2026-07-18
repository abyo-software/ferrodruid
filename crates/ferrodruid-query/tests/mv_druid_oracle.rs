// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! compat-11 multi-value (MV) string dimension oracle-twin tests.
//!
//! These tests drive the FULL FerroDruid pipeline — JSON rows →
//! [`BatchIngester`] (which builds a genuine `StringMulti` column) →
//! native queries — and assert the EXACT results real Apache Druid 31
//! returns for the same fixture.  The live-Druid side of the contract is
//! `tests/segment-compat/binary_diff_v9.rs::druid_oracle_multivalue_queries`
//! (`#[ignore]`d; needs a Druid container), which ingests the SAME rows
//! (`multiValueHandling: ARRAY`, matching FerroDruid's order-preserving
//! default) and asserts the SAME constants against Druid's native API.
//! Keep the two fixtures and constant sets in sync.
//!
//! ## The shared 4-row fixture (2024-01-01, rows 1 h apart)
//!
//! | row | tags        | m  |
//! |-----|-------------|----|
//! | 0   | ["a","b"]   | 10 |
//! | 1   | "a"         | 20 |
//! | 2   | []          | 30 |
//! | 3   | ["c","a"]   | 40 |
//!
//! ## The Druid-semantics constants asserted below
//!
//! * groupBy on `tags` EXPLODES rows across elements:
//!   null:{cnt 1, sum 30}, a:{3, 70}, b:{1, 10}, c:{1, 40};
//! * selector `tags = "a"` matches ANY element → cnt 3 / sum 70;
//!   IN `{b, c}` → cnt 2 / sum 50;
//! * a filter selects the ROW but groupBy still explodes ALL its
//!   elements: selector `tags = "b"` + groupBy → a:{1,10} AND b:{1,10};
//! * a NON-grouping timeseries does NOT explode: SUM(m) = 100 (4 rows);
//! * scan renders `["a","b"]` (array), `"a"` (scalar), null (empty MV),
//!   `["c","a"]` (within-row order preserved);
//! * segmentMetadata reports `hasMultipleValues: true` for `tags`.

use ferrodruid_ingest_batch::BatchIngester;
use ferrodruid_ingest_batch::{DimensionSchema, DimensionType};
use ferrodruid_query::{GroupByQuery, ScanQuery, SegmentMetadataQuery, TimeseriesQuery, TopNQuery};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::ColumnData;

/// The shared fixture rows (see the module docs).
fn fixture_rows() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"__time": "2024-01-01T00:00:00Z", "tags": ["a", "b"], "m": 10}),
        serde_json::json!({"__time": "2024-01-01T01:00:00Z", "tags": "a", "m": 20}),
        serde_json::json!({"__time": "2024-01-01T02:00:00Z", "tags": [], "m": 30}),
        serde_json::json!({"__time": "2024-01-01T03:00:00Z", "tags": ["c", "a"], "m": 40}),
    ]
}

/// Ingest the fixture through the REAL batch-ingest path (tags string,
/// m long) and return the built segment.
fn ingest_fixture() -> SegmentData {
    let ingester = BatchIngester::with_schemas(
        "mv_compat".into(),
        "__time".into(),
        vec![
            DimensionSchema::string("tags"),
            DimensionSchema::new("m", DimensionType::Long),
        ],
        vec![],
    );
    let result = ingester.ingest(fixture_rows()).expect("ingest fixture");
    result.segment_data
}

const INTERVAL: &str = "2024-01-01T00:00:00Z/2024-01-02T00:00:00Z";

/// Collect `tags -> (cnt, sum_m)` from groupBy results.
fn collect_groups(
    results: &[ferrodruid_query::GroupByResult],
) -> std::collections::HashMap<Option<String>, (i64, i64)> {
    results
        .iter()
        .map(|r| {
            let tag = match r.event.get("tags").expect("tags key") {
                serde_json::Value::Null => None,
                serde_json::Value::String(s) => Some(s.clone()),
                other => panic!("tags group key must be a string or null, got {other}"),
            };
            let cnt = r.event.get("cnt").and_then(|v| v.as_i64()).expect("cnt");
            let sum = r
                .event
                .get("sum_m")
                .and_then(|v| v.as_i64())
                .expect("sum_m");
            (tag, (cnt, sum))
        })
        .collect()
}

fn groupby_json(filter: Option<serde_json::Value>) -> GroupByQuery {
    let mut q = serde_json::json!({
        "queryType": "groupBy",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{
            "type": "default",
            "dimension": "tags",
            "outputName": "tags",
            "outputType": "STRING"
        }],
        "aggregations": [
            {"type": "count", "name": "cnt"},
            {"type": "longSum", "name": "sum_m", "fieldName": "m"}
        ]
    });
    if let Some(f) = filter {
        q["filter"] = f;
    }
    serde_json::from_value(q).expect("parse groupBy")
}

/// The ingest path builds a GENUINE `StringMulti` column for `tags`
/// (array rows keep both elements in order; `[]` is the null row) while
/// the long dimension stays a plain `Long`.
#[test]
fn mv_fixture_ingests_as_string_multi() {
    let segment = ingest_fixture();
    match segment.column("tags").expect("tags") {
        ColumnData::StringMulti(mc) => {
            assert_eq!(mc.num_rows(), 4);
            assert_eq!(mc.row_values(0), vec!["a", "b"]);
            assert_eq!(mc.row_values(1), vec!["a"]);
            assert!(mc.is_null_row(2));
            assert_eq!(mc.row_values(3), vec!["c", "a"], "order preserved");
        }
        other => panic!("expected StringMulti, got {other:?}"),
    }
    assert!(matches!(segment.column("m"), Some(ColumnData::Long(_))));
}

/// ORACLE: `GROUP BY tags` explodes each row across its elements —
/// null:{1,30}, a:{3,70}, b:{1,10}, c:{1,40} (confirmed against real
/// Druid 31 by the ignored segment-compat twin).
#[test]
fn mv_groupby_explodes_rows_across_elements() {
    let segment = ingest_fixture();
    let results = groupby_json(None).execute(&segment).expect("groupBy");
    let groups = collect_groups(&results);
    assert_eq!(groups.get(&None), Some(&(1, 30)), "empty [] groups as null");
    assert_eq!(
        groups.get(&Some("a".into())),
        Some(&(3, 70)),
        "a: rows 0,1,3"
    );
    assert_eq!(groups.get(&Some("b".into())), Some(&(1, 10)), "b: row 0");
    assert_eq!(groups.get(&Some("c".into())), Some(&(1, 40)), "c: row 3");
    assert_eq!(groups.len(), 4, "exactly 4 groups: {results:?}");
}

/// ORACLE: selector `tags = "a"` matches a row when ANY element equals
/// "a" (rows 0, 1, 3) and IN `{b, c}` matches rows 0 and 3.
#[test]
fn mv_selector_and_in_match_any_element() {
    let segment = ingest_fixture();

    let ts_a: TimeseriesQuery = serde_json::from_value(serde_json::json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "filter": {"type": "selector", "dimension": "tags", "value": "a"},
        "aggregations": [
            {"type": "count", "name": "cnt"},
            {"type": "longSum", "name": "sum_m", "fieldName": "m"}
        ]
    }))
    .expect("parse timeseries");
    let res = ts_a.execute(&segment).expect("timeseries");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].result.get("cnt").and_then(|v| v.as_i64()), Some(3));
    assert_eq!(
        res[0].result.get("sum_m").and_then(|v| v.as_i64()),
        Some(70)
    );

    let ts_in: TimeseriesQuery = serde_json::from_value(serde_json::json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "filter": {"type": "in", "dimension": "tags", "values": ["b", "c"]},
        "aggregations": [
            {"type": "count", "name": "cnt"},
            {"type": "longSum", "name": "sum_m", "fieldName": "m"}
        ]
    }))
    .expect("parse timeseries");
    let res = ts_in.execute(&segment).expect("timeseries");
    assert_eq!(res[0].result.get("cnt").and_then(|v| v.as_i64()), Some(2));
    assert_eq!(
        res[0].result.get("sum_m").and_then(|v| v.as_i64()),
        Some(50)
    );
}

/// compat-11 R2: a `bloomFilter` filter matches a row when ANY element
/// probes into the bloom (same any-element rule as selector/IN) — a bloom
/// containing only "a" matches rows 0 `["a","b"]`, 1 `"a"`, 3 `["c","a"]`
/// but NOT the empty row 2.  Pre-fix the JSON-array TEXT was probed, so
/// `["a","b"]` was rejected.  (FerroDruid bloom envelope — not part of
/// the live-Druid twin's constant set, which cannot produce this
/// envelope; the any-element rule itself is the oracle-verified
/// selector/IN rule.)
#[test]
fn mv_bloom_filter_matches_any_element() {
    let segment = ingest_fixture();
    let mut bloom = ferrodruid_aggregator::BloomFilter::for_entries(8);
    bloom.add("a");
    let b64 = ferrodruid_aggregator::encode_bloom_filter(&bloom);
    let ts: TimeseriesQuery = serde_json::from_value(serde_json::json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "filter": {"type": "bloomFilter", "dimension": "tags", "base64Filter": b64},
        "aggregations": [
            {"type": "count", "name": "cnt"},
            {"type": "longSum", "name": "sum_m", "fieldName": "m"}
        ]
    }))
    .expect("parse timeseries");
    let res = ts.execute(&segment).expect("timeseries");
    assert_eq!(res.len(), 1);
    assert_eq!(
        res[0].result.get("cnt").and_then(|v| v.as_i64()),
        Some(3),
        "rows 0, 1, 3 hold element \"a\""
    );
    assert_eq!(
        res[0].result.get("sum_m").and_then(|v| v.as_i64()),
        Some(70)
    );
}

/// ORACLE (Druid's documented MV surprise): a filter selects the ROW, and
/// groupBy still explodes ALL of that row's elements — selector
/// `tags = "b"` keeps only row 0 (`["a","b"]`), whose groupBy yields BOTH
/// a:{1,10} and b:{1,10}.
#[test]
fn mv_filtered_groupby_still_explodes_all_elements() {
    let segment = ingest_fixture();
    let q = groupby_json(Some(serde_json::json!({
        "type": "selector", "dimension": "tags", "value": "b"
    })));
    let results = q.execute(&segment).expect("groupBy");
    let groups = collect_groups(&results);
    assert_eq!(groups.get(&Some("a".into())), Some(&(1, 10)));
    assert_eq!(groups.get(&Some("b".into())), Some(&(1, 10)));
    assert_eq!(groups.len(), 2, "row 0 only, exploded into a and b");
}

/// ORACLE: a NON-grouping query does NOT explode — the metric is summed
/// once per row (SUM(m) = 100 over 4 rows, not the exploded 130).
#[test]
fn mv_non_grouping_timeseries_does_not_explode() {
    let segment = ingest_fixture();
    let ts: TimeseriesQuery = serde_json::from_value(serde_json::json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "aggregations": [
            {"type": "count", "name": "cnt"},
            {"type": "longSum", "name": "sum_m", "fieldName": "m"}
        ]
    }))
    .expect("parse timeseries");
    let res = ts.execute(&segment).expect("timeseries");
    assert_eq!(res[0].result.get("cnt").and_then(|v| v.as_i64()), Some(4));
    assert_eq!(
        res[0].result.get("sum_m").and_then(|v| v.as_i64()),
        Some(100),
        "no explosion without an MV grouping dim"
    );
}

/// ORACLE: topN on `tags` ranked by SUM(m) explodes like groupBy:
/// a:70, c:40, null:30, b:10.
#[test]
fn mv_topn_explodes_and_ranks() {
    let segment = ingest_fixture();
    let q: TopNQuery = serde_json::from_value(serde_json::json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimension": {
            "type": "default",
            "dimension": "tags",
            "outputName": "tags",
            "outputType": "STRING"
        },
        "threshold": 10,
        "metric": "sum_m",
        "aggregations": [
            {"type": "longSum", "name": "sum_m", "fieldName": "m"}
        ]
    }))
    .expect("parse topN");
    let res = q.execute(&segment).expect("topN");
    assert_eq!(res.len(), 1);
    let ranked: Vec<(serde_json::Value, i64)> = res[0]
        .result
        .iter()
        .map(|m| {
            (
                m.get("tags").cloned().unwrap_or(serde_json::Value::Null),
                m.get("sum_m").and_then(|v| v.as_i64()).expect("sum_m"),
            )
        })
        .collect();
    assert_eq!(
        ranked,
        vec![
            (serde_json::json!("a"), 70),
            (serde_json::json!("c"), 40),
            (serde_json::Value::Null, 30),
            (serde_json::json!("b"), 10),
        ],
        "topN explode + ranking"
    );
}

/// ORACLE: scan renders an MV row as a JSON array (order preserved), a
/// 1-element row as the scalar string, and an empty MV row as null.
#[test]
fn mv_scan_renders_arrays_scalars_and_null() {
    let segment = ingest_fixture();
    let q: ScanQuery = serde_json::from_value(serde_json::json!({
        "queryType": "scan",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL],
        "columns": ["__time", "tags", "m"],
        "resultFormat": "list",
        "order": "ascending"
    }))
    .expect("parse scan");
    let res = q.execute(&segment).expect("scan");
    let tags: Vec<serde_json::Value> = res
        .events
        .iter()
        .map(|e| e.get("tags").cloned().unwrap_or(serde_json::Value::Null))
        .collect();
    assert_eq!(
        tags,
        vec![
            serde_json::json!(["a", "b"]),
            serde_json::json!("a"),
            serde_json::Value::Null,
            serde_json::json!(["c", "a"]),
        ],
        "scan MV rendering"
    );
}

/// ORACLE: segmentMetadata reports `hasMultipleValues: true` for the MV
/// column and `false` for every single-value column.
#[test]
fn mv_segment_metadata_reports_has_multiple_values() {
    let segment = ingest_fixture();
    let q: SegmentMetadataQuery = serde_json::from_value(serde_json::json!({
        "queryType": "segmentMetadata",
        "dataSource": {"type": "table", "name": "mv_compat"},
        "intervals": [INTERVAL]
    }))
    .expect("parse segmentMetadata");
    let res = q.execute(&segment).expect("segmentMetadata");
    assert_eq!(res.len(), 1);
    let tags_meta = res[0].columns.get("tags").expect("tags metadata");
    assert!(tags_meta.has_multiple_values, "tags is MV");
    assert_eq!(tags_meta.typ, "STRING");
    let m_meta = res[0].columns.get("m").expect("m metadata");
    assert!(!m_meta.has_multiple_values, "m is single-value");
}

/// The MV column survives persist → reload on BOTH on-disk formats (FDX
/// and v9), and the reloaded segment answers the groupBy oracle with the
/// same exploded constants — the round-trip the batch-ingest durability
/// path relies on.
#[test]
fn mv_on_disk_roundtrip_preserves_query_results() {
    let segment = ingest_fixture();

    type WriteFn = fn(&SegmentData, &std::path::Path) -> ferrodruid_common::error::Result<()>;
    let formats: [(&str, WriteFn); 2] = [
        ("fdx", ferrodruid_segment::write_segment_fdx),
        ("v9", ferrodruid_segment::write_segment_v9),
    ];
    for (label, write) in formats {
        let dir = tempfile::tempdir().expect("tempdir");
        let seg_dir = dir.path().join(label);
        write(&segment, &seg_dir).unwrap_or_else(|e| panic!("write {label}: {e}"));
        // `from_smoosh` auto-detects v9 vs FDX from version.bin.
        let smoosh = ferrodruid_segment::SmooshReader::open(&seg_dir)
            .unwrap_or_else(|e| panic!("open {label}: {e}"));
        let reloaded =
            SegmentData::from_smoosh(&smoosh).unwrap_or_else(|e| panic!("reload {label}: {e}"));

        match reloaded.column("tags").expect("tags") {
            ColumnData::StringMulti(mc) => {
                assert_eq!(mc.row_values(0), vec!["a", "b"], "{label}");
                assert!(mc.is_null_row(2), "{label}");
                assert_eq!(mc.row_values(3), vec!["c", "a"], "{label}: order");
            }
            other => panic!("{label}: expected StringMulti after reload, got {other:?}"),
        }

        let results = groupby_json(None).execute(&reloaded).expect("groupBy");
        let groups = collect_groups(&results);
        assert_eq!(groups.get(&None), Some(&(1, 30)), "{label}");
        assert_eq!(groups.get(&Some("a".into())), Some(&(3, 70)), "{label}");
        assert_eq!(groups.get(&Some("b".into())), Some(&(1, 10)), "{label}");
        assert_eq!(groups.get(&Some("c".into())), Some(&(1, 40)), "{label}");
    }
}
