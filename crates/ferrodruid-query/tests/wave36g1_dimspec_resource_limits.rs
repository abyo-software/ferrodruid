// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 36-G1 — regression tests for the 3 Wave 37B query Highs:
//!
//! 1. **Unbounded per-key memory in TopN/GroupBy** — DoS via a
//!    high-cardinality dimension (Wave 37B query Top-1).
//! 2. **TopN ignores `DimensionSpec` extraction / filter wrappers**
//!    (Wave 37B query High #2).
//! 3. **GroupBy ignores `DimensionSpec` extraction / filter wrappers**
//!    (Wave 37B query High #3).

use ferrodruid_aggregator::AggregatorSpec;
use ferrodruid_common::error::DruidError;
use ferrodruid_common::types::{ColumnType, DataSource, DimensionSpec, ExtractionFunction};
use ferrodruid_query::{
    GranularitySpec, GroupByQuery, TopNMetricSpec, TopNQuery, groupby::DEFAULT_GROUPBY_MAX_KEYS,
    topn::DEFAULT_TOPN_MAX_INFLIGHT,
};
use ferrodruid_segment::{SegmentData, SegmentDataBuilder};

// ---------------------------------------------------------------------------
// Tiny segment builder — N rows, every row a distinct dimension value.
// ---------------------------------------------------------------------------

fn build_high_card_segment(n: usize, dim_name: &str) -> SegmentData {
    let values: Vec<String> = (0..n).map(|i| format!("k{i}")).collect();
    let timestamps: Vec<i64> = (0..n as i64).collect();
    SegmentDataBuilder::new()
        .add_timestamp_column(timestamps)
        .add_string_column(dim_name, values)
        .build()
        .expect("build segment")
}

// ---------------------------------------------------------------------------
// Deliverable 1 — Resource-limit guard fires on high-cardinality input.
// ---------------------------------------------------------------------------

#[test]
fn topn_rejects_query_exceeding_inflight_threshold() {
    // 50 distinct dimension values; cap = 10 fires before the 11th key.
    let segment = build_high_card_segment(50, "page");
    let q = TopNQuery {
        data_source: DataSource::Table {
            name: "test".into(),
        },
        intervals: vec!["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z".into()],
        granularity: GranularitySpec::Simple("all".into()),
        dimension: DimensionSpec::Default {
            dimension: "page".into(),
            output_name: "page".into(),
            output_type: ColumnType::String,
        },
        threshold: 5,
        metric: TopNMetricSpec::Numeric {
            metric: "cnt".into(),
        },
        filter: None,
        virtual_columns: None,
        aggregations: vec![AggregatorSpec::Count { name: "cnt".into() }],
        post_aggregations: None,
        context: None,
    };

    let err = q
        .execute_with_limit(&segment, 10)
        .expect_err("expected ResourceLimit");
    match err {
        DruidError::ResourceLimit {
            kind,
            limit,
            observed,
        } => {
            assert_eq!(kind, "topN.maxIntermediateRows");
            assert_eq!(limit, 10);
            assert!(observed >= 10, "observed {observed} should be >= limit 10");
        }
        other => panic!("wrong error variant: {other:?}"),
    }

    // With the default cap (`DEFAULT_TOPN_MAX_INFLIGHT` = 100k) the same
    // query succeeds — sanity-check the fast path is not regressed.
    let _ = q
        .execute_with_limit(&segment, DEFAULT_TOPN_MAX_INFLIGHT)
        .expect("default cap should not fire on 50 keys");
}

#[test]
fn groupby_rejects_query_exceeding_max_keys() {
    let segment = build_high_card_segment(50, "page");
    let q = GroupByQuery {
        data_source: DataSource::Table {
            name: "test".into(),
        },
        intervals: vec!["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z".into()],
        granularity: GranularitySpec::Simple("all".into()),
        dimensions: vec![DimensionSpec::Default {
            dimension: "page".into(),
            output_name: "page".into(),
            output_type: ColumnType::String,
        }],
        filter: None,
        virtual_columns: None,
        aggregations: vec![AggregatorSpec::Count { name: "cnt".into() }],
        post_aggregations: None,
        subtotals_spec: None,
        having: None,
        limit_spec: None,
        context: None,
    };

    let err = q
        .execute_with_limit(&segment, 10)
        .expect_err("expected ResourceLimit");
    match err {
        DruidError::ResourceLimit {
            kind,
            limit,
            observed,
        } => {
            assert_eq!(kind, "groupBy.maxResults");
            assert_eq!(limit, 10);
            assert!(observed >= 10, "observed {observed} should be >= limit 10");
        }
        other => panic!("wrong error variant: {other:?}"),
    }

    // Default cap (1M) should not fire.
    let _ = q
        .execute_with_limit(&segment, DEFAULT_GROUPBY_MAX_KEYS)
        .expect("default cap should not fire");
}

// ---------------------------------------------------------------------------
// Deliverable 2 — TopN honours `extraction` extraction-fn.
// ---------------------------------------------------------------------------

#[test]
fn topn_with_extraction_fn_yields_extracted_values() {
    // 4 rows: "FOO", "FOO", "BAR", "BAR".  With `lower` extraction, the
    // result must group on "foo" / "bar" (lower-cased), not the raw
    // "FOO" / "BAR" the buggy code would have used.
    let segment = SegmentDataBuilder::new()
        .add_timestamp_column((0..4).collect())
        .add_string_column(
            "page",
            vec!["FOO".into(), "FOO".into(), "BAR".into(), "BAR".into()],
        )
        .build()
        .expect("build");

    let q = TopNQuery {
        data_source: DataSource::Table { name: "t".into() },
        intervals: vec!["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z".into()],
        granularity: GranularitySpec::Simple("all".into()),
        dimension: DimensionSpec::Extraction {
            dimension: "page".into(),
            output_name: "page_lc".into(),
            extraction_fn: ExtractionFunction::Lower { locale: None },
        },
        threshold: 5,
        metric: TopNMetricSpec::Numeric {
            metric: "cnt".into(),
        },
        filter: None,
        virtual_columns: None,
        aggregations: vec![AggregatorSpec::Count { name: "cnt".into() }],
        post_aggregations: None,
        context: None,
    };

    let result = q.execute(&segment).expect("execute");
    assert_eq!(result.len(), 1);
    let entries = &result[0].result;
    // Two distinct keys: "foo" and "bar" (each count 2).
    assert_eq!(entries.len(), 2);
    let keys: Vec<String> = entries
        .iter()
        .filter_map(|m| m.get("page_lc").and_then(|v| v.as_str()).map(str::to_owned))
        .collect();
    assert!(
        keys.contains(&"foo".into()),
        "expected lowered key 'foo', got {keys:?}"
    );
    assert!(
        keys.contains(&"bar".into()),
        "expected lowered key 'bar', got {keys:?}"
    );
    // None of the *raw* upper-case values should leak through.
    assert!(!keys.iter().any(|k| k == "FOO" || k == "BAR"));
}

// ---------------------------------------------------------------------------
// Deliverable 3 — GroupBy honours `listFiltered` whitelist.
// ---------------------------------------------------------------------------

#[test]
fn groupby_with_filtered_dim_excludes_filtered_rows() {
    // 6 rows: A, B, A, C, B, C — whitelist {A, B}; "C" rows must be
    // dropped from the group set.
    let segment = SegmentDataBuilder::new()
        .add_timestamp_column((0..6).collect())
        .add_string_column(
            "dim",
            vec![
                "A".into(),
                "B".into(),
                "A".into(),
                "C".into(),
                "B".into(),
                "C".into(),
            ],
        )
        .build()
        .expect("build");

    let q = GroupByQuery {
        data_source: DataSource::Table { name: "t".into() },
        intervals: vec!["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z".into()],
        granularity: GranularitySpec::Simple("all".into()),
        dimensions: vec![DimensionSpec::ListFiltered {
            delegate: Box::new(DimensionSpec::Default {
                dimension: "dim".into(),
                output_name: "dim".into(),
                output_type: ColumnType::String,
            }),
            values: vec!["A".into(), "B".into()],
            is_whitelist: true,
        }],
        filter: None,
        virtual_columns: None,
        aggregations: vec![AggregatorSpec::Count { name: "cnt".into() }],
        post_aggregations: None,
        subtotals_spec: None,
        having: None,
        limit_spec: None,
        context: None,
    };

    let result = q.execute(&segment).expect("execute");
    // After filtering, only {A, B} remain — 2 distinct group keys.
    assert_eq!(result.len(), 2);
    let dim_values: Vec<String> = result
        .iter()
        .filter_map(|r| {
            r.event
                .get("dim")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        })
        .collect();
    assert!(dim_values.contains(&"A".into()));
    assert!(dim_values.contains(&"B".into()));
    assert!(
        !dim_values.contains(&"C".into()),
        "C should be filtered out"
    );
    // Each remaining group has 2 rows.
    for r in &result {
        let cnt = r.event.get("cnt").and_then(|v| v.as_u64()).unwrap_or(0);
        assert_eq!(cnt, 2, "expected 2 rows per surviving group");
    }
}
