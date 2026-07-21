// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-A (v1.5.0): cross-engine decode of REAL Apache-Druid-written
//! `hyperUnique` complex metric columns, asserted against the captured
//! Druid 31.0.2 oracle (`tests/segment-compat/fixtures/hyperunique_druid31`
//! — real segments, real `dump-segment` output, real query answers).
//!
//! The regression bar is the oracle's EXACT doubles: the native
//! `hyperUnique` estimate is `12.03529418544122` for the 12-distinct
//! dataset across ALL THREE segmentations (rollup=false, rollup=true, and
//! the 6-segment multi-shard layout — the v1.1.1 broker-fold bug-class
//! shape), per-day `8.015665809687173` / `10.024493827539368`, per-country
//! JP `6.008806266444944` / US `8.015665809687173`, and the dense
//! 1200-distinct variant `1190.8281757103275` total with
//! `694.502272279783` (JP) / `707.1747087253059` (US).
//!
//! The MULTI-SEGMENT shape is the DEFAULT here: every 12-distinct
//! assertion runs against `hu_multiseg` (6 segments), exercising both the
//! intra-segment fold (rows → one sketch) and the inter-segment broker
//! fold (`merge_json_by_spec` + post-agg re-evaluation, mirrored from the
//! broker's own sequence).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use ferrodruid_aggregator::{AggregatorSpec, PostAggregatorSpec, merge_json_by_spec};
use ferrodruid_query::{DruidQuery, QueryResult, execute_query, finalize_native_wire_outputs};
use ferrodruid_segment::SegmentData;
use serde_json::json;

// ---------------------------------------------------------------------------
// Oracle constants (captured 2026-07-20 from real Druid 31.0.2)
// ---------------------------------------------------------------------------

const ORACLE_TOTAL_12: f64 = 12.035_294_185_441_22;
const ORACLE_DAY_1: f64 = 8.015_665_809_687_173;
const ORACLE_DAY_2: f64 = 10.024_493_827_539_368;
const ORACLE_JP: f64 = 6.008_806_266_444_944;
const ORACLE_US: f64 = 8.015_665_809_687_173;
const ORACLE_EVENTS_PER_UU: f64 = 1.994_134_886_127_849_8;
const ORACLE_DENSE_TOTAL: f64 = 1_190.828_175_710_327_5;
const ORACLE_DENSE_JP: f64 = 694.502_272_279_783;
const ORACLE_DENSE_US: f64 = 707.174_708_725_305_9;

// ---------------------------------------------------------------------------
// Fixture loading
// ---------------------------------------------------------------------------

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/segment-compat/fixtures/hyperunique_druid31")
}

/// Open every captured segment of a datasource, in shard order.
fn open_segments(ds: &str) -> Vec<SegmentData> {
    let dir = fixture_root().join("segments").join(ds);
    let mut seg_dirs: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("fixture dir {}: {e}", dir.display()))
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("segment_"))
        })
        .collect();
    seg_dirs.sort();
    assert!(!seg_dirs.is_empty(), "no segments under {}", dir.display());
    seg_dirs
        .iter()
        .map(|p| {
            SegmentData::open(p).unwrap_or_else(|e| {
                panic!(
                    "real Druid hyperUnique segment {} must open strict: {e}",
                    p.display()
                )
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Broker-fold simulation (mirrors crates/ferrodruid-broker merge sequence:
// per-shard execute → merge agg fields by spec → re-evaluate post-aggs)
// ---------------------------------------------------------------------------

fn parse_query(v: serde_json::Value) -> DruidQuery {
    serde_json::from_value(v).expect("query JSON must parse")
}

/// Merge one shard's result map into the accumulated map, dispatching every
/// declared aggregation through `merge_json_by_spec` (dimension fields keep
/// the destination value — the broker's passthrough rule).
fn merge_shard_map(
    dst: &mut serde_json::Map<String, serde_json::Value>,
    src: &serde_json::Map<String, serde_json::Value>,
    aggregations: &[AggregatorSpec],
) {
    let spec_by_name: HashMap<&str, &AggregatorSpec> =
        aggregations.iter().map(|s| (s.name(), s)).collect();
    for (key, src_val) in src {
        if let Some(dst_val) = dst.get_mut(key) {
            if let Some(&spec) = spec_by_name.get(key.as_str()) {
                *dst_val = merge_json_by_spec(spec, dst_val, src_val);
            }
        } else {
            dst.insert(key.clone(), src_val.clone());
        }
    }
}

/// Re-evaluate post-aggregations on a merged map (the broker's
/// `reapply_post_aggs` sequence).
fn reapply_post_aggs(
    post_aggs: &[PostAggregatorSpec],
    map: &mut serde_json::Map<String, serde_json::Value>,
) {
    let agg_results: HashMap<String, serde_json::Value> =
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    for pa in post_aggs {
        let json_val = pa
            .evaluate(&agg_results)
            .and_then(serde_json::Number::from_f64)
            .map_or(serde_json::Value::Null, serde_json::Value::Number);
        map.insert(pa.name().to_string(), json_val);
    }
}

/// Execute a timeseries query per segment and broker-fold every bucket into
/// one map per distinct timestamp; returns `(timestamp, merged map)` pairs
/// in ascending timestamp order with post-aggs re-applied.
fn timeseries_folded(
    query_json: serde_json::Value,
    segments: &[SegmentData],
) -> Vec<(String, serde_json::Map<String, serde_json::Value>)> {
    let query = parse_query(query_json);
    let aggregations: Vec<AggregatorSpec> = match &query {
        DruidQuery::Timeseries(q) => q.aggregations.clone(),
        other => panic!("expected timeseries query, got {other:?}"),
    };
    let post_aggs: Vec<PostAggregatorSpec> = match &query {
        DruidQuery::Timeseries(q) => q.post_aggregations.clone().unwrap_or_default(),
        _ => Vec::new(),
    };
    let mut buckets: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
    for segment in segments {
        let QueryResult::Timeseries(results) = execute_query(&query, segment).expect("execute")
        else {
            panic!("expected timeseries result");
        };
        for entry in results {
            let ts = entry.timestamp.clone();
            match buckets.iter_mut().find(|(t, _)| *t == ts) {
                Some((_, dst)) => merge_shard_map(dst, &entry.result, &aggregations),
                None => buckets.push((ts, entry.result.clone())),
            }
        }
    }
    buckets.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (_, map) in &mut buckets {
        reapply_post_aggs(&post_aggs, map);
    }
    buckets
}

/// Execute a groupBy query per segment and broker-fold rows by their single
/// grouping dimension; returns `dimension value → merged map` with
/// post-aggs re-applied.
fn groupby_folded(
    query_json: serde_json::Value,
    segments: &[SegmentData],
    dim: &str,
) -> HashMap<String, serde_json::Map<String, serde_json::Value>> {
    let query = parse_query(query_json);
    let aggregations: Vec<AggregatorSpec> = match &query {
        DruidQuery::GroupBy(q) => q.aggregations.clone(),
        other => panic!("expected groupBy query, got {other:?}"),
    };
    let post_aggs: Vec<PostAggregatorSpec> = match &query {
        DruidQuery::GroupBy(q) => q.post_aggregations.clone().unwrap_or_default(),
        _ => Vec::new(),
    };
    let mut groups: HashMap<String, serde_json::Map<String, serde_json::Value>> = HashMap::new();
    for segment in segments {
        let QueryResult::GroupBy(results) = execute_query(&query, segment).expect("execute") else {
            panic!("expected groupBy result");
        };
        for row in results {
            let key = row
                .event
                .get(dim)
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("missing group key `{dim}`"))
                .to_string();
            let src: serde_json::Map<String, serde_json::Value> =
                row.event.clone().into_iter().collect();
            match groups.get_mut(&key) {
                Some(dst) => merge_shard_map(dst, &src, &aggregations),
                None => {
                    groups.insert(key, src);
                }
            }
        }
    }
    for map in groups.values_mut() {
        reapply_post_aggs(&post_aggs, map);
    }
    groups
}

/// Extract the finalized native double for aggregation `name` by running
/// the merged envelope through the same finalize path the native wire uses.
fn finalized_estimate(
    spec_json: serde_json::Value,
    map: &serde_json::Map<String, serde_json::Value>,
    name: &str,
) -> f64 {
    let spec: AggregatorSpec = serde_json::from_value(spec_json).expect("agg spec");
    let envelope = map.get(name).unwrap_or_else(|| panic!("missing {name}"));
    let finalized = ferrodruid_aggregator::finalize_sketch_json(&spec, envelope)
        .unwrap_or_else(|| panic!("finalize must produce a scalar for {envelope}"));
    finalized
        .as_f64()
        .unwrap_or_else(|| panic!("finalized value must be a number, got {finalized}"))
}

/// Assert two doubles are BIT-identical (the oracle bar is exact estimator
/// arithmetic, not approximate closeness).
#[track_caller]
fn assert_bits(got: f64, want: f64, what: &str) {
    assert!(
        got.to_bits() == want.to_bits(),
        "{what}: got {got:?} ({:#018x}), oracle {want:?} ({:#018x})",
        got.to_bits(),
        want.to_bits()
    );
}

fn hyper_unique_ts_query() -> serde_json::Value {
    json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "hu"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-03T00:00:00Z"],
        "granularity": "all",
        "aggregations": [
            {"type": "hyperUnique", "name": "uu", "fieldName": "uu"},
            {"type": "longSum", "name": "events", "fieldName": "events"}
        ],
        "postAggregations": [
            {"type": "hyperUniqueCardinality", "name": "uu_card", "fieldName": "uu"},
            {"type": "arithmetic", "name": "events_per_uu", "fn": "/",
             "fields": [
                {"type": "fieldAccess", "name": "e", "fieldName": "events"},
                {"type": "fieldAccess", "name": "u", "fieldName": "uu"}
             ]}
        ]
    })
}

// ---------------------------------------------------------------------------
// The W-A flip test: real Druid hyperUnique segments decode + query
// ---------------------------------------------------------------------------

/// FIRST W-A assertion: the captured multi-shard Druid segments (6
/// segments, the v1.1.1 broker-fold bug-class shape) must OPEN strict and
/// a native `hyperUnique` timeseries folded across all 6 must reproduce
/// the oracle double EXACTLY.  Before W-A this fails at `SegmentData::open`
/// ("unsupported complex column typeName `hyperUnique`").
#[test]
fn multiseg_hyperunique_timeseries_matches_oracle_exactly() {
    let segments = open_segments("hu_multiseg");
    assert_eq!(segments.len(), 6, "oracle captured 3 shards/day × 2 days");
    let buckets = timeseries_folded(hyper_unique_ts_query(), &segments);
    assert_eq!(buckets.len(), 1, "granularity=all folds to one bucket");
    let map = &buckets[0].1;

    let uu = finalized_estimate(
        json!({"type": "hyperUnique", "name": "uu", "fieldName": "uu"}),
        map,
        "uu",
    );
    assert_bits(uu, ORACLE_TOTAL_12, "hu_multiseg native uu");

    // hyperUniqueCardinality post-agg ≡ the finalized aggregation value
    // (oracle: `uu_card == uu` in every captured query).
    let uu_card = map
        .get("uu_card")
        .and_then(serde_json::Value::as_f64)
        .expect("uu_card");
    assert_bits(uu_card, ORACLE_TOTAL_12, "hu_multiseg uu_card post-agg");

    // events=24 and the arithmetic post-agg over the finalized estimate.
    assert_eq!(
        map.get("events").and_then(serde_json::Value::as_i64),
        Some(24)
    );
    let per = map
        .get("events_per_uu")
        .and_then(serde_json::Value::as_f64)
        .expect("events_per_uu");
    assert_bits(per, ORACLE_EVENTS_PER_UU, "hu_multiseg events_per_uu");
}

/// Per-day fold (granularity=day) across the 6 shards: day buckets merge
/// 3 shards each and must reproduce the oracle's per-day doubles.
#[test]
fn multiseg_hyperunique_per_day_matches_oracle_exactly() {
    let segments = open_segments("hu_multiseg");
    let mut query = hyper_unique_ts_query();
    query["granularity"] = json!("day");
    let buckets = timeseries_folded(query, &segments);
    assert_eq!(buckets.len(), 2, "two day buckets");
    let spec = json!({"type": "hyperUnique", "name": "uu", "fieldName": "uu"});
    assert_bits(
        finalized_estimate(spec.clone(), &buckets[0].1, "uu"),
        ORACLE_DAY_1,
        "day 1 uu",
    );
    assert_bits(
        finalized_estimate(spec, &buckets[1].1, "uu"),
        ORACLE_DAY_2,
        "day 2 uu",
    );
}

/// Per-country groupBy folded across the 6 shards.
#[test]
fn multiseg_hyperunique_groupby_country_matches_oracle_exactly() {
    let segments = open_segments("hu_multiseg");
    let groups = groupby_folded(
        json!({
            "queryType": "groupBy",
            "dataSource": {"type": "table", "name": "hu"},
            "intervals": ["2024-01-01T00:00:00Z/2024-01-03T00:00:00Z"],
            "granularity": "all",
            "dimensions": [
                {"type": "default", "dimension": "country", "outputName": "country",
                 "outputType": "STRING"}
            ],
            "aggregations": [
                {"type": "hyperUnique", "name": "uu", "fieldName": "uu"}
            ],
            "postAggregations": [
                {"type": "hyperUniqueCardinality", "name": "uu_card", "fieldName": "uu"}
            ]
        }),
        &segments,
        "country",
    );
    let spec = json!({"type": "hyperUnique", "name": "uu", "fieldName": "uu"});
    let jp = groups.get("JP").expect("JP group");
    let us = groups.get("US").expect("US group");
    assert_bits(
        finalized_estimate(spec.clone(), jp, "uu"),
        ORACLE_JP,
        "JP uu",
    );
    assert_bits(finalized_estimate(spec, us, "uu"), ORACLE_US, "US uu");
    // Post-agg parity per group.
    assert_bits(
        jp.get("uu_card")
            .and_then(serde_json::Value::as_f64)
            .expect("JP uu_card"),
        ORACLE_JP,
        "JP uu_card",
    );
    assert_bits(
        us.get("uu_card")
            .and_then(serde_json::Value::as_f64)
            .expect("US uu_card"),
        ORACLE_US,
        "US uu_card",
    );
}

/// Folding must be invariant to segmentation (the oracle's key property):
/// rollup=false single-user blobs, rollup=true pre-folded blobs, and the
/// 6-shard layout all reproduce the SAME estimate.
#[test]
fn estimates_invariant_across_all_three_segmentations() {
    let spec = json!({"type": "hyperUnique", "name": "uu", "fieldName": "uu"});
    for ds in ["hu_rollup_false", "hu_rollup_day", "hu_multiseg"] {
        let segments = open_segments(ds);
        let buckets = timeseries_folded(hyper_unique_ts_query(), &segments);
        assert_eq!(buckets.len(), 1, "{ds}: one bucket");
        let uu = finalized_estimate(spec.clone(), &buckets[0].1, "uu");
        assert_bits(uu, ORACLE_TOTAL_12, ds);
    }
}

/// Dense-regime (1200 distinct, 1031-byte blobs, one with the max-overflow
/// header) totals and per-country estimates.
#[test]
fn dense_hyperunique_matches_oracle_exactly() {
    let segments = open_segments("hu_dense");
    let spec = json!({"type": "hyperUnique", "name": "uu", "fieldName": "uu"});

    let buckets = timeseries_folded(hyper_unique_ts_query(), &segments);
    assert_eq!(buckets.len(), 1);
    assert_bits(
        finalized_estimate(spec.clone(), &buckets[0].1, "uu"),
        ORACLE_DENSE_TOTAL,
        "dense total uu",
    );

    let groups = groupby_folded(
        json!({
            "queryType": "groupBy",
            "dataSource": {"type": "table", "name": "hu"},
            "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
            "granularity": "all",
            "dimensions": [
                {"type": "default", "dimension": "country", "outputName": "country",
                 "outputType": "STRING"}
            ],
            "aggregations": [
                {"type": "hyperUnique", "name": "uu", "fieldName": "uu"}
            ]
        }),
        &segments,
        "country",
    );
    assert_bits(
        finalized_estimate(spec.clone(), groups.get("JP").expect("JP"), "uu"),
        ORACLE_DENSE_JP,
        "dense JP uu",
    );
    assert_bits(
        finalized_estimate(spec, groups.get("US").expect("US"), "uu"),
        ORACLE_DENSE_US,
        "dense US uu",
    );
}

/// topN by the hyperUnique metric (dense variant, one segment): US ranks
/// first with the exact oracle doubles after wire finalization.
#[test]
fn dense_hyperunique_topn_ranks_by_estimate() {
    let segments = open_segments("hu_dense");
    assert_eq!(segments.len(), 1);
    let query = parse_query(json!({
        "queryType": "topN",
        "dataSource": {"type": "table", "name": "hu"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
        "granularity": "all",
        "dimension": {"type": "default", "dimension": "country",
                      "outputName": "country", "outputType": "STRING"},
        "metric": "uu",
        "threshold": 2,
        "aggregations": [
            {"type": "hyperUnique", "name": "uu", "fieldName": "uu"}
        ]
    }));
    let mut result = execute_query(&query, &segments[0]).expect("execute");
    finalize_native_wire_outputs(&query, &mut result);
    let QueryResult::TopN(entries) = result else {
        panic!("expected topN result");
    };
    assert_eq!(entries.len(), 1);
    let rows = &entries[0].result;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("country").and_then(|v| v.as_str()),
        Some("US"),
        "US (707.17) must rank above JP (694.50)"
    );
    assert_bits(
        rows[0]
            .get("uu")
            .and_then(serde_json::Value::as_f64)
            .expect("US uu"),
        ORACLE_DENSE_US,
        "topN US uu",
    );
    assert_bits(
        rows[1]
            .get("uu")
            .and_then(serde_json::Value::as_f64)
            .expect("JP uu"),
        ORACLE_DENSE_JP,
        "topN JP uu",
    );
}

/// The native wire finalization path (`finalize_native_wire_outputs`, the
/// default `finalize=true` shape Druid returns) renders the estimate
/// double directly in the timeseries result.
#[test]
fn native_wire_finalize_renders_estimate_double() {
    let segments = open_segments("hu_rollup_day");
    let query = parse_query(json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "hu"},
        "intervals": ["2024-01-01T00:00:00Z/2024-01-03T00:00:00Z"],
        "granularity": "all",
        "aggregations": [
            {"type": "hyperUnique", "name": "uu", "fieldName": "uu"}
        ]
    }));
    // Single-segment day 1: the folded (day, country) sketches of day 1.
    let mut result = execute_query(&query, &segments[0]).expect("execute");
    finalize_native_wire_outputs(&query, &mut result);
    let QueryResult::Timeseries(entries) = result else {
        panic!("expected timeseries result");
    };
    assert_eq!(entries.len(), 1);
    let uu = entries[0]
        .result
        .get("uu")
        .and_then(serde_json::Value::as_f64)
        .expect("finalized uu");
    assert_bits(uu, ORACLE_DAY_1, "day-1 segment finalized uu");
}

/// STRICT no-mix: a `hyperUnique` aggregation over a NON-hyperUnique
/// column (raw strings) must never fabricate an estimate — the aggregator
/// poisons loudly (an error envelope, finalized to no scalar), never a
/// silent wrong number.
#[test]
fn hyperunique_agg_over_raw_column_fails_loud_not_silent() {
    use ferrodruid_segment::SegmentDataBuilder;

    let segment = SegmentDataBuilder::new()
        .add_timestamp_column(vec![100, 200])
        .add_string_column("user", vec!["a".into(), "b".into()])
        .build()
        .expect("segment");
    let query = parse_query(json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "t"},
        "intervals": ["1970-01-01T00:00:00Z/1970-01-02T00:00:00Z"],
        "granularity": "all",
        "aggregations": [
            {"type": "hyperUnique", "name": "uu", "fieldName": "user"}
        ]
    }));
    let QueryResult::Timeseries(entries) = execute_query(&query, &segment).expect("execute") else {
        panic!("expected timeseries result");
    };
    assert_eq!(entries.len(), 1);
    let value = entries[0].result.get("uu").expect("uu present");
    // The output is an ERROR envelope — visibly not an estimate.
    assert_eq!(
        value.get("@sketch").and_then(|v| v.as_str()),
        Some("mixError"),
        "raw values into a merge-only hyperUnique aggregation must poison \
         loudly, got {value}"
    );
    // Finalization refuses to produce a scalar from the poisoned state.
    let spec: AggregatorSpec =
        serde_json::from_value(json!({"type": "hyperUnique", "name": "uu", "fieldName": "user"}))
            .expect("spec");
    assert_eq!(
        ferrodruid_aggregator::finalize_sketch_json(&spec, value),
        None
    );
}
