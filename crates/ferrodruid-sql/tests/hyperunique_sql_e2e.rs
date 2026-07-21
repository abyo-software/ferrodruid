// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-A (v1.5.0) SQL end-to-end: `APPROX_COUNT_DISTINCT` over a REAL
//! Apache-Druid-written `COMPLEX<hyperUnique>` column, asserted against
//! the captured Druid 31.0.2 oracle answers
//! (`tests/segment-compat/fixtures/hyperunique_druid31/oracle/`).
//!
//! Pipeline under test: SQL text → `parse_druid_sql` → `plan_sql` (which
//! lowers `APPROX_COUNT_DISTINCT` to a hidden `HLLSketchBuild` + a rounded
//! `HLLSketchEstimate` post-agg) → per-segment native execution → the
//! broker fold (`merge_json_by_spec` + post-agg re-evaluation) across the
//! 6-shard multi-segment layout — the v1.1.1 broker-fold bug-class shape.
//! The native accumulator ADOPTS the decoded-hyperUnique feed (empty-
//! neutral rule) and the finalized value comes from the Druid-parity
//! estimator, so the SQL integers must EXACT-match the oracle: total 12,
//! JP 6 / US 8, per-day 8 / 10, dense 1191 with JP 695 / US 707.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use ferrodruid_aggregator::{AggregatorSpec, PostAggregatorSpec, merge_json_by_spec};
use ferrodruid_common::types::ColumnType;
use ferrodruid_query::{DruidQuery, QueryResult, execute_query};
use ferrodruid_segment::SegmentData;
use ferrodruid_sql::parser::parse_druid_sql;
use ferrodruid_sql::planner::{ColumnSchema, DataSourceSchema, plan_sql};

// ---------------------------------------------------------------------------
// Fixture + schema
// ---------------------------------------------------------------------------

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/segment-compat/fixtures/hyperunique_druid31")
}

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
        .map(|p| SegmentData::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display())))
        .collect()
}

fn hu_schema(name: &str) -> DataSourceSchema {
    DataSourceSchema {
        name: name.to_string(),
        dimensions: vec![ColumnSchema {
            name: "country".to_string(),
            column_type: ColumnType::String,
        }],
        metrics: vec![
            ColumnSchema {
                name: "uu".to_string(),
                column_type: ColumnType::Complex("hyperUnique".to_string()),
            },
            ColumnSchema {
                name: "events".to_string(),
                column_type: ColumnType::Long,
            },
        ],
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

fn plan(sql: &str, ds: &str) -> DruidQuery {
    let stmt = parse_druid_sql(sql).expect("parse");
    plan_sql(&stmt, &hu_schema(ds)).expect("plan").native_query
}

// ---------------------------------------------------------------------------
// Broker-fold simulation (merge agg fields by spec, re-apply post-aggs —
// the broker's own sequence)
// ---------------------------------------------------------------------------

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

/// Execute a planned SQL query per segment, broker-fold the results by
/// group key (the values of `key_fields`, JSON-encoded), and return
/// `group key → merged map` with post-aggs re-applied.
fn execute_folded(
    query: &DruidQuery,
    segments: &[SegmentData],
    key_fields: &[&str],
) -> HashMap<String, serde_json::Map<String, serde_json::Value>> {
    let (aggregations, post_aggs): (Vec<AggregatorSpec>, Vec<PostAggregatorSpec>) = match query {
        DruidQuery::Timeseries(q) => (
            q.aggregations.clone(),
            q.post_aggregations.clone().unwrap_or_default(),
        ),
        DruidQuery::GroupBy(q) => (
            q.aggregations.clone(),
            q.post_aggregations.clone().unwrap_or_default(),
        ),
        other => panic!("expected timeseries/groupBy plan, got {other:?}"),
    };
    let mut groups: HashMap<String, serde_json::Map<String, serde_json::Value>> = HashMap::new();
    for segment in segments {
        let rows: Vec<(String, serde_json::Map<String, serde_json::Value>)> =
            match execute_query(query, segment).expect("execute") {
                QueryResult::Timeseries(entries) => {
                    entries
                        .into_iter()
                        .map(|e| {
                            // A TIME_FLOOR group-by lowers to a granular
                            // timeseries: the bucket identity is the entry
                            // TIMESTAMP (plus any named fields).
                            let mut key: Vec<serde_json::Value> =
                                vec![serde_json::Value::String(e.timestamp.clone())];
                            key.extend(key_fields.iter().map(|f| {
                                e.result.get(*f).cloned().unwrap_or(serde_json::Value::Null)
                            }));
                            (serde_json::to_string(&key).expect("key"), e.result)
                        })
                        .collect()
                }
                QueryResult::GroupBy(entries) => entries
                    .into_iter()
                    .map(|e| {
                        let key: Vec<serde_json::Value> = key_fields
                            .iter()
                            .map(|f| e.event.get(*f).cloned().unwrap_or(serde_json::Value::Null))
                            .collect();
                        (
                            serde_json::to_string(&key).expect("key"),
                            e.event.into_iter().collect(),
                        )
                    })
                    .collect(),
                other => panic!("unexpected result shape {other:?}"),
            };
        for (key, src) in rows {
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

#[track_caller]
fn assert_exact_int(map: &serde_json::Map<String, serde_json::Value>, name: &str, want: i64) {
    let got = map
        .get(name)
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_else(|| panic!("`{name}` must be a number, got {:?}", map.get(name)));
    #[allow(clippy::cast_precision_loss)]
    let want_f = want as f64;
    assert!(
        got.to_bits() == want_f.to_bits(),
        "{name}: got {got:?}, oracle {want}"
    );
}

// ---------------------------------------------------------------------------
// Oracle-parity SQL tests (multi-segment DEFAULT shape)
// ---------------------------------------------------------------------------

/// `SELECT APPROX_COUNT_DISTINCT(uu)` across the 6-shard layout → 12.
#[test]
fn sql_approx_count_distinct_total_matches_oracle() {
    let segments = open_segments("hu_multiseg");
    assert_eq!(segments.len(), 6);
    let query = plan("SELECT APPROX_COUNT_DISTINCT(uu) AS uu FROM hu", "hu");
    let groups = execute_folded(&query, &segments, &[]);
    assert_eq!(groups.len(), 1);
    let map = groups.values().next().expect("one bucket");
    assert_exact_int(map, "uu", 12);
}

/// Per-country: JP 6 / US 8 (oracle `sql_by_country`).
#[test]
fn sql_approx_count_distinct_by_country_matches_oracle() {
    let segments = open_segments("hu_multiseg");
    let query = plan(
        "SELECT country, APPROX_COUNT_DISTINCT(uu) AS uu FROM hu GROUP BY country",
        "hu",
    );
    let groups = execute_folded(&query, &segments, &["country"]);
    assert_eq!(groups.len(), 2);
    assert_exact_int(groups.get(r#"["JP"]"#).expect("JP"), "uu", 6);
    assert_exact_int(groups.get(r#"["US"]"#).expect("US"), "uu", 8);
}

/// Per-day via `TIME_FLOOR` (the oracle's own `sql_by_day` query): 8 / 10.
#[test]
fn sql_approx_count_distinct_by_day_matches_oracle() {
    let segments = open_segments("hu_multiseg");
    let query = plan(
        "SELECT TIME_FLOOR(__time, 'P1D') AS d, APPROX_COUNT_DISTINCT(uu) AS uu \
         FROM hu GROUP BY 1",
        "hu",
    );
    let groups = execute_folded(&query, &segments, &["d"]);
    assert_eq!(groups.len(), 2, "two day groups, got {groups:?}");
    let mut day_values: Vec<(String, f64)> = groups
        .iter()
        .map(|(k, m)| {
            (
                k.clone(),
                m.get("uu").and_then(serde_json::Value::as_f64).expect("uu"),
            )
        })
        .collect();
    day_values.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(day_values[0].1.to_bits(), 8.0_f64.to_bits(), "day 1");
    assert_eq!(day_values[1].1.to_bits(), 10.0_f64.to_bits(), "day 2");
}

/// The estimates are segmentation-invariant through SQL too.
#[test]
fn sql_total_invariant_across_segmentations() {
    for ds in ["hu_rollup_false", "hu_rollup_day", "hu_multiseg"] {
        let segments = open_segments(ds);
        let query = plan("SELECT APPROX_COUNT_DISTINCT(uu) AS uu FROM hu", "hu");
        let groups = execute_folded(&query, &segments, &[]);
        let map = groups.values().next().expect("one bucket");
        assert_exact_int(map, "uu", 12);
    }
}

/// Dense variant (1200 distinct, overflow header present): 1191 total,
/// JP 695 / US 707 (oracle `sql_total` / `sql_by_country`).
#[test]
fn sql_dense_matches_oracle() {
    let segments = open_segments("hu_dense");
    let total = plan("SELECT APPROX_COUNT_DISTINCT(uu) AS uu FROM hu", "hu");
    let groups = execute_folded(&total, &segments, &[]);
    assert_exact_int(groups.values().next().expect("bucket"), "uu", 1191);

    let by_country = plan(
        "SELECT country, APPROX_COUNT_DISTINCT(uu) AS uu FROM hu GROUP BY country",
        "hu",
    );
    let groups = execute_folded(&by_country, &segments, &["country"]);
    assert_exact_int(groups.get(r#"["JP"]"#).expect("JP"), "uu", 695);
    assert_exact_int(groups.get(r#"["US"]"#).expect("US"), "uu", 707);
}

/// STRICT no-mix at the SQL/broker layer: folding a decoded-hyperUnique
/// shard with a NON-empty native-FNV shard (schema drift — the same
/// column is a raw string in one segment) must produce a LOUD error
/// envelope and a NULL SQL output, never a silently wrong estimate.
#[test]
fn sql_mixed_column_kinds_fail_loud_not_silent() {
    use ferrodruid_segment::SegmentDataBuilder;

    let hu_segments = open_segments("hu_rollup_day");
    // A drifted segment where `uu` is a plain string column: the hidden
    // HLLSketchBuild hashes raw values into a NON-empty native FNV HLL.
    let drifted = SegmentDataBuilder::new()
        .add_timestamp_column(vec![1_704_067_200_000, 1_704_067_260_000])
        .add_string_column("country", vec!["US".into(), "JP".into()])
        .add_string_column("uu", vec!["u01".into(), "u05".into()])
        .build()
        .expect("segment");

    let query = plan("SELECT APPROX_COUNT_DISTINCT(uu) AS uu FROM hu", "hu");
    let mut segments = hu_segments;
    segments.push(drifted);
    let groups = execute_folded(&query, &segments, &[]);
    let map = groups.values().next().expect("bucket");
    // The SQL output value must be NULL (the estimate post-agg refuses to
    // numerify the error), and the hidden agg field must carry the loud
    // mix-error envelope.
    assert!(
        map.get("uu").is_some_and(serde_json::Value::is_null),
        "mixed feeds must not produce a number, got {:?}",
        map.get("uu")
    );
    let hidden = map
        .values()
        .find(|v| {
            v.get("@sketch").and_then(serde_json::Value::as_str)
                == Some(ferrodruid_aggregator::SKETCH_MIX_ERROR_TAG)
        })
        .unwrap_or_else(|| panic!("expected a loud mix-error envelope in {map:?}"));
    assert!(
        hidden
            .get("message")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|m| m.contains("no-mix")),
        "error envelope must explain the mix, got {hidden}"
    );
}
