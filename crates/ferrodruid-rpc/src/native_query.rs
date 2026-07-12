// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 41.OO + 42.RR native query subset.
//!
//! For Wave 5/6 of the v1.0 plan we ship a small, focused subset of
//! Druid's native query JSON shape so the cross-role wire can carry
//! real query bodies. Four query types are supported here:
//!
//! - [`NativeQuery::Timeseries`] — time-bucketed aggregate over a
//!   metric column (count or sum). Wave 41.OO.
//! - [`NativeQuery::Scan`] — row-by-row dump with optional column
//!   projection, optional row limit, and optional equality filter.
//!   Wave 41.OO.
//! - [`NativeQuery::GroupBy`] — group rows by N dimensions, fold via
//!   N aggregations, optional `having` predicate + sort + limit.
//!   Wave 42.RR.
//! - [`NativeQuery::TopN`] — top-N rows over a single dimension
//!   ranked by a single metric. Wave 42.RR.
//!
//! Other native query shapes (`search`, `segmentMetadata`,
//! `dataSourceMetadata`, `timeBoundary`, full SQL) live in the richer
//! [`ferrodruid-query`](../../ferrodruid_query) crate used by the
//! single-binary path; wiring them through the cross-role HTTP
//! boundary remains deferred.
//!
//! ## Why this lives in `ferrodruid-rpc` and not `ferrodruid-query`
//!
//! The role-split path keeps the historical / broker binaries small
//! and dependency-light. Pulling in the full `ferrodruid-query` crate
//! (which transitively pulls in `ferrodruid-segment`,
//! `ferrodruid-aggregator`, `ferrodruid-bitmap`, `ferrodruid-dict`,
//! ...) would defeat the role-split goal of independent deployability.
//! The Wave 41.OO subset is a tiny, hand-written executor that operates
//! directly against [`Segment`] artifacts.

use std::collections::HashMap;

use ferrodruid_deep_storage::Segment;
use serde::{Deserialize, Serialize};

/// Top-level native-query envelope carried over the broker→historical
/// HTTP wire. Tagged by the `queryType` JSON field, mirroring Druid's
/// native query shape so an existing Druid client targeting this
/// surface gets a recognisable response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "queryType", rename_all = "camelCase")]
pub enum NativeQuery {
    /// Time-bucketed aggregate over a metric column.
    Timeseries(TimeseriesSpec),
    /// Row-by-row dump with optional projection / limit / filter.
    Scan(ScanSpec),
    /// Group rows by N dimensions, fold via N aggregations.
    GroupBy(GroupBySpec),
    /// Top-N over a single dimension ranked by one metric.
    TopN(TopNSpec),
}

/// Time-bucketed aggregate.
///
/// Buckets are keyed by `timestamp_ms / granularity_ms * granularity_ms`
/// — i.e. floor-aligned to the granularity. The empty-bucket fill rule
/// is **not** applied (deferred to W6) so a query over a sparse
/// interval returns only the buckets that have at least one matching
/// row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TimeseriesSpec {
    /// Datasource the query targets.
    pub data_source: String,
    /// Granularity in milliseconds. `0` is treated as "single bucket".
    /// Druid's `all` granularity maps to `0`; `minute` to `60_000`;
    /// `hour` to `3_600_000`; `day` to `86_400_000`.
    pub granularity_ms: i64,
    /// Aggregations to compute, in projection order. The result for
    /// each bucket carries one entry per aggregation, keyed by the
    /// aggregation's `name`.
    pub aggregations: Vec<Aggregation>,
    /// Optional equality filter. When present, only rows whose
    /// `dimension == value` participate in the bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<EqualsFilter>,
}

/// Row-by-row dump.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScanSpec {
    /// Datasource the query targets.
    pub data_source: String,
    /// Optional column projection. When `None` or empty, every column
    /// in the row is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// Optional row limit. When `None`, every matching row is
    /// returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Optional equality filter. Same semantics as for
    /// [`TimeseriesSpec`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<EqualsFilter>,
}

/// Wave 42.RR `groupBy` query spec.
///
/// Rows are grouped by the tuple of dimension values pulled from each
/// row at the columns named in `dimensions`. For each group, the
/// per-aggregation result is folded over the rows that fell into it.
/// An optional [`HavingClause`] filters groups *after* aggregation,
/// optional [`SortSpec`] entries order the surviving groups, and an
/// optional `limit` truncates the head of the sorted list.
///
/// The Wave 42.RR `groupBy` is intentionally simple: no extraction
/// functions, no pre-aggregation filter beyond the W5 equality filter,
/// no nested groupBy. Druid's full surface lands in a later wave; this
/// subset is what the cross-role demo needs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GroupBySpec {
    /// Datasource the query targets.
    pub data_source: String,
    /// Columns to group by. The result row carries one entry per
    /// dimension keyed by the dimension's column name. Order matters
    /// because it controls the default sort key tie-break order.
    pub dimensions: Vec<String>,
    /// Aggregations to compute per group, in projection order.
    pub aggregations: Vec<Aggregation>,
    /// Optional equality filter applied *before* grouping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<EqualsFilter>,
    /// Optional `having` predicate evaluated against each post-fold
    /// group. Groups for which the predicate is false are dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having: Option<HavingClause>,
    /// Optional sort. When `None` or empty, the result order is
    /// stable but unspecified beyond "groups appear in encounter
    /// order"; tests should always pass an explicit sort when
    /// asserting on order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<SortSpec>>,
    /// Optional limit on the number of post-sort groups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Wave 42.RR `topN` query spec.
///
/// `topN` is the cardinality-friendly degenerate of `groupBy`: a
/// single dimension, ranked by a single metric. The result is the
/// top `threshold` rows ordered descending by `metric` (numeric
/// types) — Druid's "high" sort. We intentionally omit the
/// "low" / "alphaNumeric" / "lexicographic" variants the full Druid
/// surface ships; this subset is enough for the cross-role demo.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopNSpec {
    /// Datasource the query targets.
    pub data_source: String,
    /// The single dimension to group by.
    pub dimension: String,
    /// Aggregations to compute per group. The `metric` column must
    /// match one of these aggregation names.
    pub aggregations: Vec<Aggregation>,
    /// Aggregation name to rank by. Higher = better (Druid's "high"
    /// sort). When the metric isn't numeric the row is sorted as 0.
    pub metric: String,
    /// Maximum number of result rows to keep after the rank-and-cap.
    pub threshold: usize,
    /// Optional pre-grouping equality filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<EqualsFilter>,
}

/// Wave 42.RR `having` predicate. We support equality + the four
/// standard numeric inequalities. Combinators (AND / OR / NOT) and
/// extraction functions are deferred.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum HavingClause {
    /// `aggregation == value` (numeric).
    Equal {
        /// Aggregation name to evaluate.
        aggregation: String,
        /// Numeric literal.
        value: f64,
    },
    /// `aggregation > value`.
    GreaterThan {
        /// Aggregation name to evaluate.
        aggregation: String,
        /// Numeric literal.
        value: f64,
    },
    /// `aggregation >= value`.
    GreaterThanOrEqual {
        /// Aggregation name to evaluate.
        aggregation: String,
        /// Numeric literal.
        value: f64,
    },
    /// `aggregation < value`.
    LessThan {
        /// Aggregation name to evaluate.
        aggregation: String,
        /// Numeric literal.
        value: f64,
    },
    /// `aggregation <= value`.
    LessThanOrEqual {
        /// Aggregation name to evaluate.
        aggregation: String,
        /// Numeric literal.
        value: f64,
    },
}

impl HavingClause {
    /// True iff the result row satisfies the predicate.
    #[must_use]
    pub fn matches(&self, row: &serde_json::Map<String, serde_json::Value>) -> bool {
        let (key, value, op) = match self {
            HavingClause::Equal { aggregation, value } => (aggregation, *value, HavingOp::Eq),
            HavingClause::GreaterThan { aggregation, value } => (aggregation, *value, HavingOp::Gt),
            HavingClause::GreaterThanOrEqual { aggregation, value } => {
                (aggregation, *value, HavingOp::Ge)
            }
            HavingClause::LessThan { aggregation, value } => (aggregation, *value, HavingOp::Lt),
            HavingClause::LessThanOrEqual { aggregation, value } => {
                (aggregation, *value, HavingOp::Le)
            }
        };
        let actual = row.get(key).and_then(serde_json::Value::as_f64);
        match actual {
            Some(a) => match op {
                HavingOp::Eq => (a - value).abs() < f64::EPSILON,
                HavingOp::Gt => a > value,
                HavingOp::Ge => a >= value,
                HavingOp::Lt => a < value,
                HavingOp::Le => a <= value,
            },
            None => false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum HavingOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

/// Sort specification for a [`GroupBySpec`].
///
/// `dimension` selects the result-row key (either a dimension name or
/// an aggregation name); `direction` controls ascending vs descending.
/// Numeric columns sort numerically; everything else sorts as a
/// string. Missing keys sort as the smallest possible value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SortSpec {
    /// Result-row key to sort on.
    pub dimension: String,
    /// Sort direction.
    #[serde(default)]
    pub direction: SortDirection,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    /// Ascending order.
    #[default]
    Ascending,
    /// Descending order.
    Descending,
}

/// Single equality predicate `dimension == value`. The Wave 41.OO
/// surface intentionally only supports equality; richer filters
/// (range, IN, NOT, AND/OR) are deferred.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EqualsFilter {
    /// Column name to match on.
    pub dimension: String,
    /// Expected literal value (string-compared, mirroring Druid's
    /// dimension-aware equality).
    pub value: String,
}

impl EqualsFilter {
    /// True iff `row[self.dimension]` stringifies to `self.value`.
    #[must_use]
    pub fn matches(&self, row: &serde_json::Map<String, serde_json::Value>) -> bool {
        match row.get(&self.dimension) {
            Some(serde_json::Value::String(s)) => *s == self.value,
            Some(serde_json::Value::Number(n)) => n.to_string() == self.value,
            Some(serde_json::Value::Bool(b)) => b.to_string() == self.value,
            Some(serde_json::Value::Null) | None => false,
            // Arrays / objects never match a scalar literal in Wave
            // 41.OO. Druid would route these via dedicated typed
            // filters; we deliberately reject them here so the
            // semantics stay narrow and predictable.
            Some(_) => false,
        }
    }
}

/// Druid-aligned aggregation spec. Wave 41.OO covers the two
/// aggregations the cross-role demo touches: row-count and metric sum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Aggregation {
    /// Row count. The `field_name` is ignored.
    Count {
        /// Result key.
        name: String,
    },
    /// Sum of a numeric column over the bucket. Non-numeric / missing
    /// values are skipped.
    LongSum {
        /// Result key.
        name: String,
        /// Source column.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Sum of a numeric column as a `f64`. Non-numeric / missing
    /// values are skipped.
    DoubleSum {
        /// Result key.
        name: String,
        /// Source column.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
}

impl Aggregation {
    /// Result-map key the aggregation writes to.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Aggregation::Count { name }
            | Aggregation::LongSum { name, .. }
            | Aggregation::DoubleSum { name, .. } => name,
        }
    }
}

/// Result of a native-query execution against a single segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "queryType", rename_all = "camelCase")]
pub enum NativeQueryResult {
    /// Bucketed timeseries result. Buckets are sorted ascending.
    Timeseries(Vec<TimeseriesBucket>),
    /// Scan row dump.
    Scan(Vec<serde_json::Map<String, serde_json::Value>>),
    /// `groupBy` row dump. Each row carries one entry per dimension
    /// keyed by the dimension's column name plus one entry per
    /// aggregation keyed by the aggregation's `name`.
    GroupBy(Vec<serde_json::Map<String, serde_json::Value>>),
    /// `topN` row dump. Identical wire shape to `GroupBy` but ranked
    /// and capped to a small `threshold`.
    TopN(Vec<serde_json::Map<String, serde_json::Value>>),
}

/// One bucket of a timeseries result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimeseriesBucket {
    /// Bucket-floor timestamp in milliseconds.
    pub timestamp_ms: i64,
    /// Aggregation results for this bucket, keyed by aggregation name.
    pub result: serde_json::Map<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

impl NativeQuery {
    /// Execute the query against a single in-memory segment.
    ///
    /// Wave 41.OO does not validate the segment's `data_source` against
    /// the query's `data_source` — the broker is expected to scatter
    /// only to historicals that hold the relevant segment. The
    /// historical's segment store may filter by `data_source` before
    /// calling this, but execution itself is segment-local.
    #[must_use]
    pub fn execute(&self, segment: &Segment) -> NativeQueryResult {
        match self {
            NativeQuery::Timeseries(spec) => {
                NativeQueryResult::Timeseries(execute_timeseries(spec, segment))
            }
            NativeQuery::Scan(spec) => NativeQueryResult::Scan(execute_scan(spec, segment)),
            NativeQuery::GroupBy(spec) => {
                NativeQueryResult::GroupBy(execute_group_by(spec, segment))
            }
            NativeQuery::TopN(spec) => NativeQueryResult::TopN(execute_top_n(spec, segment)),
        }
    }
}

/// Merge two timeseries result vectors by bucket timestamp.
///
/// Rows with identical `timestamp_ms` have their aggregation entries
/// summed. The result is sorted ascending by timestamp.
///
/// This is the per-broker reducer used to combine per-historical
/// scatter responses into a single tier-wide answer.
#[must_use]
pub fn merge_timeseries(
    parts: Vec<Vec<TimeseriesBucket>>,
    aggs: &[Aggregation],
) -> Vec<TimeseriesBucket> {
    let mut by_ts: HashMap<i64, serde_json::Map<String, serde_json::Value>> = HashMap::new();
    for part in parts {
        for bucket in part {
            let entry = by_ts.entry(bucket.timestamp_ms).or_default();
            for agg in aggs {
                let key = agg.name();
                let incoming = bucket
                    .result
                    .get(key)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let prev = entry
                    .get(key)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let total = prev + incoming;
                let value = match agg {
                    Aggregation::DoubleSum { .. } => json_number(total),
                    _ => {
                        // Count / LongSum: store as integer when integral.
                        if total.fract() == 0.0 && total.is_finite() {
                            serde_json::Value::Number(serde_json::Number::from(total as i64))
                        } else {
                            json_number(total)
                        }
                    }
                };
                entry.insert(key.to_string(), value);
            }
        }
    }
    let mut keys: Vec<i64> = by_ts.keys().copied().collect();
    keys.sort_unstable();
    keys.into_iter()
        .map(|ts| TimeseriesBucket {
            timestamp_ms: ts,
            result: by_ts.remove(&ts).unwrap_or_default(),
        })
        .collect()
}

/// Merge two scan result vectors by concatenation, applying the
/// supplied per-broker limit. The merge is order-preserving with
/// respect to the input fragment order.
#[must_use]
pub fn merge_scan(
    parts: Vec<Vec<serde_json::Map<String, serde_json::Value>>>,
    limit: Option<usize>,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    for part in parts {
        for row in part {
            if let Some(cap) = limit
                && out.len() >= cap
            {
                return out;
            }
            out.push(row);
        }
    }
    out
}

fn execute_timeseries(spec: &TimeseriesSpec, segment: &Segment) -> Vec<TimeseriesBucket> {
    let mut buckets: HashMap<i64, BucketState> = HashMap::new();

    for row in &segment.rows {
        if let Some(filter) = &spec.filter
            && !filter.matches(row)
        {
            continue;
        }
        let ts = row
            .get("__time")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let key = bucket_floor(ts, spec.granularity_ms);
        let state = buckets
            .entry(key)
            .or_insert_with(|| BucketState::new(&spec.aggregations));
        state.feed(row, &spec.aggregations);
    }

    let mut keys: Vec<i64> = buckets.keys().copied().collect();
    keys.sort_unstable();
    keys.into_iter()
        .map(|ts| {
            let state = buckets
                .remove(&ts)
                .unwrap_or_else(|| BucketState::new(&spec.aggregations));
            TimeseriesBucket {
                timestamp_ms: ts,
                result: state.finish(&spec.aggregations),
            }
        })
        .collect()
}

/// Merge two groupBy result vectors by dimension tuple.
///
/// Per-segment fragments are grouped by the dimensions key tuple
/// (taken from `dimensions` in spec order); aggregations are folded
/// across fragments using the same sum-fold rule as `merge_timeseries`.
/// The `having` predicate, sort, and limit are then applied to the
/// merged set so they always see the cluster-wide totals (not the
/// per-segment partial folds).
#[must_use]
pub fn merge_group_by(
    parts: Vec<Vec<serde_json::Map<String, serde_json::Value>>>,
    spec: &GroupBySpec,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    // Key by dimension-tuple (stringified) to keep it Hash-able.
    let mut groups: HashMap<Vec<String>, serde_json::Map<String, serde_json::Value>> =
        HashMap::new();
    let mut order: Vec<Vec<String>> = Vec::new();
    for part in parts {
        for row in part {
            let key: Vec<String> = spec
                .dimensions
                .iter()
                .map(|d| stringify_dim_value(row.get(d)))
                .collect();
            let entry = groups.entry(key.clone()).or_insert_with(|| {
                order.push(key.clone());
                let mut m = serde_json::Map::new();
                for d in &spec.dimensions {
                    if let Some(v) = row.get(d) {
                        m.insert(d.clone(), v.clone());
                    }
                }
                m
            });
            for agg in &spec.aggregations {
                let k = agg.name();
                let prev = entry
                    .get(k)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let incoming = row
                    .get(k)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let total = prev + incoming;
                entry.insert(k.to_string(), agg_value(agg, total));
            }
        }
    }
    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = order
        .into_iter()
        .filter_map(|k| groups.remove(&k))
        .collect();
    apply_having_sort_limit(&mut out, spec);
    out
}

/// Merge two topN result vectors by dimension value.
///
/// Same fold rule as [`merge_group_by`] but with a single dimension,
/// then ranked descending by the `metric` aggregation and capped at
/// `threshold`.
#[must_use]
pub fn merge_top_n(
    parts: Vec<Vec<serde_json::Map<String, serde_json::Value>>>,
    spec: &TopNSpec,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut groups: HashMap<String, serde_json::Map<String, serde_json::Value>> = HashMap::new();
    for part in parts {
        for row in part {
            let key = stringify_dim_value(row.get(&spec.dimension));
            let entry = groups.entry(key).or_insert_with(|| {
                let mut m = serde_json::Map::new();
                if let Some(v) = row.get(&spec.dimension) {
                    m.insert(spec.dimension.clone(), v.clone());
                }
                m
            });
            for agg in &spec.aggregations {
                let k = agg.name();
                let prev = entry
                    .get(k)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let incoming = row
                    .get(k)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let total = prev + incoming;
                entry.insert(k.to_string(), agg_value(agg, total));
            }
        }
    }
    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = groups.into_values().collect();
    rank_top_n(&mut out, spec);
    out
}

fn execute_scan(
    spec: &ScanSpec,
    segment: &Segment,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    for row in &segment.rows {
        if let Some(filter) = &spec.filter
            && !filter.matches(row)
        {
            continue;
        }
        let projected = match &spec.columns {
            Some(cols) if !cols.is_empty() => {
                let mut m = serde_json::Map::with_capacity(cols.len());
                for c in cols {
                    if let Some(v) = row.get(c) {
                        m.insert(c.clone(), v.clone());
                    }
                }
                m
            }
            _ => row.clone(),
        };
        out.push(projected);
        if let Some(cap) = spec.limit
            && out.len() >= cap
        {
            break;
        }
    }
    out
}

fn execute_group_by(
    spec: &GroupBySpec,
    segment: &Segment,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut groups: HashMap<Vec<String>, GroupState> = HashMap::new();
    let mut order: Vec<Vec<String>> = Vec::new();

    for row in &segment.rows {
        if let Some(filter) = &spec.filter
            && !filter.matches(row)
        {
            continue;
        }
        let key: Vec<String> = spec
            .dimensions
            .iter()
            .map(|d| stringify_dim_value(row.get(d)))
            .collect();
        let entry = groups.entry(key.clone()).or_insert_with(|| {
            order.push(key.clone());
            GroupState::new(spec, row)
        });
        entry.feed(row, &spec.aggregations);
    }

    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = order
        .into_iter()
        .filter_map(|k| groups.remove(&k).map(|g| g.finish(&spec.aggregations)))
        .collect();
    apply_having_sort_limit(&mut out, spec);
    out
}

fn execute_top_n(
    spec: &TopNSpec,
    segment: &Segment,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut groups: HashMap<String, GroupState> = HashMap::new();
    let dim_keys = std::slice::from_ref(&spec.dimension);

    for row in &segment.rows {
        if let Some(filter) = &spec.filter
            && !filter.matches(row)
        {
            continue;
        }
        let key = stringify_dim_value(row.get(&spec.dimension));
        let entry = groups
            .entry(key)
            .or_insert_with(|| GroupState::new_for_dims(dim_keys, row));
        entry.feed(row, &spec.aggregations);
    }

    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = groups
        .into_values()
        .map(|g| g.finish(&spec.aggregations))
        .collect();
    rank_top_n(&mut out, spec);
    out
}

fn apply_having_sort_limit(
    rows: &mut Vec<serde_json::Map<String, serde_json::Value>>,
    spec: &GroupBySpec,
) {
    if let Some(having) = &spec.having {
        rows.retain(|r| having.matches(r));
    }
    if let Some(sort) = &spec.sort
        && !sort.is_empty()
    {
        rows.sort_by(|a, b| compare_rows(a, b, sort));
    }
    if let Some(limit) = spec.limit
        && rows.len() > limit
    {
        rows.truncate(limit);
    }
}

fn rank_top_n(rows: &mut Vec<serde_json::Map<String, serde_json::Value>>, spec: &TopNSpec) {
    rows.sort_by(|a, b| {
        let av = a
            .get(&spec.metric)
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        let bv = b
            .get(&spec.metric)
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        // Descending by metric (Druid "high" sort).
        bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
    });
    if rows.len() > spec.threshold {
        rows.truncate(spec.threshold);
    }
}

fn compare_rows(
    a: &serde_json::Map<String, serde_json::Value>,
    b: &serde_json::Map<String, serde_json::Value>,
    sort: &[SortSpec],
) -> std::cmp::Ordering {
    for s in sort {
        let av = a.get(&s.dimension);
        let bv = b.get(&s.dimension);
        let ord = compare_json(av, bv);
        if ord != std::cmp::Ordering::Equal {
            return match s.direction {
                SortDirection::Ascending => ord,
                SortDirection::Descending => ord.reverse(),
            };
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_json(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        // Missing keys sort smallest.
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(av), Some(bv)) => {
            // Prefer numeric compare when both sides parse as numbers.
            if let (Some(an), Some(bn)) = (av.as_f64(), bv.as_f64()) {
                an.partial_cmp(&bn).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                stringify_dim_value(Some(av)).cmp(&stringify_dim_value(Some(bv)))
            }
        }
    }
}

fn stringify_dim_value(v: Option<&serde_json::Value>) -> String {
    match v {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::Bool(b)) => b.to_string(),
        Some(serde_json::Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn agg_value(agg: &Aggregation, v: f64) -> serde_json::Value {
    match agg {
        Aggregation::DoubleSum { .. } => json_number(v),
        _ => {
            if v.fract() == 0.0 && v.is_finite() {
                serde_json::Value::Number(serde_json::Number::from(v as i64))
            } else {
                json_number(v)
            }
        }
    }
}

#[derive(Debug)]
struct GroupState {
    /// Row holding the dimension values (cloned at first encounter).
    base: serde_json::Map<String, serde_json::Value>,
    /// One slot per aggregation, in spec order.
    slots: Vec<f64>,
}

impl GroupState {
    fn new(spec: &GroupBySpec, row: &serde_json::Map<String, serde_json::Value>) -> Self {
        Self::new_for_dims(spec.dimensions.as_slice(), row)
    }

    fn new_for_dims(dims: &[String], row: &serde_json::Map<String, serde_json::Value>) -> Self {
        let mut base = serde_json::Map::with_capacity(dims.len());
        for d in dims {
            if let Some(v) = row.get(d) {
                base.insert(d.clone(), v.clone());
            }
        }
        Self {
            base,
            slots: Vec::new(),
        }
    }

    fn feed(&mut self, row: &serde_json::Map<String, serde_json::Value>, aggs: &[Aggregation]) {
        if self.slots.len() != aggs.len() {
            self.slots = vec![0.0; aggs.len()];
        }
        for (idx, agg) in aggs.iter().enumerate() {
            match agg {
                Aggregation::Count { .. } => {
                    self.slots[idx] += 1.0;
                }
                Aggregation::LongSum { field_name, .. }
                | Aggregation::DoubleSum { field_name, .. } => {
                    if let Some(v) = row.get(field_name).and_then(serde_json::Value::as_f64) {
                        self.slots[idx] += v;
                    }
                }
            }
        }
    }

    fn finish(self, aggs: &[Aggregation]) -> serde_json::Map<String, serde_json::Value> {
        let mut out = self.base;
        for (idx, agg) in aggs.iter().enumerate() {
            let v = self.slots.get(idx).copied().unwrap_or(0.0);
            out.insert(agg.name().to_string(), agg_value(agg, v));
        }
        out
    }
}

#[derive(Debug)]
struct BucketState {
    /// One slot per aggregation, in spec order.
    slots: Vec<f64>,
}

impl BucketState {
    fn new(aggs: &[Aggregation]) -> Self {
        Self {
            slots: vec![0.0; aggs.len()],
        }
    }

    fn feed(&mut self, row: &serde_json::Map<String, serde_json::Value>, aggs: &[Aggregation]) {
        for (idx, agg) in aggs.iter().enumerate() {
            match agg {
                Aggregation::Count { .. } => {
                    self.slots[idx] += 1.0;
                }
                Aggregation::LongSum { field_name, .. }
                | Aggregation::DoubleSum { field_name, .. } => {
                    if let Some(v) = row.get(field_name).and_then(serde_json::Value::as_f64) {
                        self.slots[idx] += v;
                    }
                }
            }
        }
    }

    fn finish(self, aggs: &[Aggregation]) -> serde_json::Map<String, serde_json::Value> {
        let mut out = serde_json::Map::with_capacity(aggs.len());
        for (idx, agg) in aggs.iter().enumerate() {
            let v = self.slots[idx];
            let value = match agg {
                Aggregation::DoubleSum { .. } => json_number(v),
                _ => {
                    if v.fract() == 0.0 && v.is_finite() {
                        serde_json::Value::Number(serde_json::Number::from(v as i64))
                    } else {
                        json_number(v)
                    }
                }
            };
            out.insert(agg.name().to_string(), value);
        }
        out
    }
}

fn bucket_floor(ts: i64, granularity_ms: i64) -> i64 {
    if granularity_ms <= 0 {
        // `all` granularity: every row goes into the timestamp=0 bucket.
        return 0;
    }
    // Saturating to avoid overflow on pathological inputs.
    let g = granularity_ms;
    if ts >= 0 {
        (ts / g) * g
    } else {
        // Floor for negative timestamps.
        let q = ts / g;
        let r = ts % g;
        if r == 0 { q * g } else { (q - 1) * g }
    }
}

fn json_number(v: f64) -> serde_json::Value {
    serde_json::Number::from_f64(v)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_deep_storage::Segment;

    fn fixture() -> Segment {
        let text = r#"{"segmentId":"wiki_v0_0","dataSource":"wikipedia","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"},{"name":"count","type":"long"}]}
{"__time":1714694400000,"page":"home","count":3}
{"__time":1714694460000,"page":"about","count":1}
{"__time":1714694520000,"page":"home","count":2}
{"__time":1714694580000,"page":"home","count":5}
"#;
        Segment::parse_jsonl(text).expect("parse fixture")
    }

    #[test]
    fn timeseries_count_all_granularity() {
        let q = NativeQuery::Timeseries(TimeseriesSpec {
            data_source: "wikipedia".into(),
            granularity_ms: 0,
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: None,
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Timeseries(buckets) => {
                assert_eq!(buckets.len(), 1);
                assert_eq!(buckets[0].timestamp_ms, 0);
                assert_eq!(buckets[0].result.get("rows"), Some(&serde_json::json!(4)));
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }

    #[test]
    fn timeseries_long_sum_with_filter() {
        let q = NativeQuery::Timeseries(TimeseriesSpec {
            data_source: "wikipedia".into(),
            granularity_ms: 0,
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            filter: Some(EqualsFilter {
                dimension: "page".into(),
                value: "home".into(),
            }),
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Timeseries(buckets) => {
                // 3 + 2 + 5 = 10 (only "home" rows).
                assert_eq!(buckets[0].result.get("total"), Some(&serde_json::json!(10)));
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }

    #[test]
    fn timeseries_minute_granularity_yields_per_minute_buckets() {
        let q = NativeQuery::Timeseries(TimeseriesSpec {
            data_source: "wikipedia".into(),
            granularity_ms: 60_000,
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: None,
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Timeseries(buckets) => {
                assert_eq!(buckets.len(), 4);
                for b in &buckets {
                    assert_eq!(b.result.get("rows"), Some(&serde_json::json!(1)));
                }
                // Sorted ascending.
                let mut prev = i64::MIN;
                for b in &buckets {
                    assert!(b.timestamp_ms >= prev);
                    prev = b.timestamp_ms;
                }
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }

    #[test]
    fn scan_returns_full_rows_when_no_projection() {
        let q = NativeQuery::Scan(ScanSpec {
            data_source: "wikipedia".into(),
            columns: None,
            limit: None,
            filter: None,
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Scan(rows) => {
                assert_eq!(rows.len(), 4);
                assert!(rows[0].contains_key("__time"));
                assert!(rows[0].contains_key("page"));
                assert!(rows[0].contains_key("count"));
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn scan_projects_columns_and_applies_limit() {
        let q = NativeQuery::Scan(ScanSpec {
            data_source: "wikipedia".into(),
            columns: Some(vec!["page".into()]),
            limit: Some(2),
            filter: None,
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Scan(rows) => {
                assert_eq!(rows.len(), 2);
                for r in &rows {
                    assert_eq!(r.len(), 1);
                    assert!(r.contains_key("page"));
                }
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn scan_filter_skips_non_matching_rows() {
        let q = NativeQuery::Scan(ScanSpec {
            data_source: "wikipedia".into(),
            columns: None,
            limit: None,
            filter: Some(EqualsFilter {
                dimension: "page".into(),
                value: "about".into(),
            }),
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Scan(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("about")));
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn merge_timeseries_sums_buckets_by_timestamp() {
        let aggs = vec![Aggregation::Count {
            name: "rows".into(),
        }];
        let part_a = vec![
            TimeseriesBucket {
                timestamp_ms: 1714694400000,
                result: serde_json::json!({"rows": 2})
                    .as_object()
                    .expect("obj")
                    .clone(),
            },
            TimeseriesBucket {
                timestamp_ms: 1714694460000,
                result: serde_json::json!({"rows": 1})
                    .as_object()
                    .expect("obj")
                    .clone(),
            },
        ];
        let part_b = vec![TimeseriesBucket {
            timestamp_ms: 1714694400000,
            result: serde_json::json!({"rows": 5})
                .as_object()
                .expect("obj")
                .clone(),
        }];

        let merged = merge_timeseries(vec![part_a, part_b], &aggs);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].timestamp_ms, 1714694400000);
        assert_eq!(merged[0].result.get("rows"), Some(&serde_json::json!(7)));
        assert_eq!(merged[1].timestamp_ms, 1714694460000);
        assert_eq!(merged[1].result.get("rows"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn merge_scan_concatenates_and_caps() {
        let part_a = vec![
            serde_json::json!({"page": "home"})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        let part_b = vec![
            serde_json::json!({"page": "about"})
                .as_object()
                .expect("obj")
                .clone(),
            serde_json::json!({"page": "kb"})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        let merged = merge_scan(vec![part_a, part_b], Some(2));
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].get("page"), Some(&serde_json::json!("home")));
        assert_eq!(merged[1].get("page"), Some(&serde_json::json!("about")));
    }

    #[test]
    fn native_query_round_trips_via_serde_json() {
        let q = NativeQuery::Timeseries(TimeseriesSpec {
            data_source: "wikipedia".into(),
            granularity_ms: 60_000,
            aggregations: vec![
                Aggregation::Count {
                    name: "rows".into(),
                },
                Aggregation::LongSum {
                    name: "sum_count".into(),
                    field_name: "count".into(),
                },
            ],
            filter: Some(EqualsFilter {
                dimension: "page".into(),
                value: "home".into(),
            }),
        });
        let s = serde_json::to_string(&q).expect("ser");
        let back: NativeQuery = serde_json::from_str(&s).expect("de");
        assert_eq!(back, q);
        assert!(s.contains("\"queryType\":\"timeseries\""), "{s}");
    }

    #[test]
    fn bucket_floor_negative_timestamp_floors_correctly() {
        assert_eq!(bucket_floor(-1, 1000), -1000);
        assert_eq!(bucket_floor(-1000, 1000), -1000);
        assert_eq!(bucket_floor(-1001, 1000), -2000);
    }

    // ---------------------------------------------------------------
    // Wave 42.RR — groupBy + topN executor / merge tests
    // ---------------------------------------------------------------

    #[test]
    fn group_by_single_dimension_sums_metric_per_group() {
        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            filter: None,
            having: None,
            sort: Some(vec![SortSpec {
                dimension: "page".into(),
                direction: SortDirection::Ascending,
            }]),
            limit: None,
        });
        match q.execute(&fixture()) {
            NativeQueryResult::GroupBy(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("about")));
                assert_eq!(rows[0].get("total"), Some(&serde_json::json!(1)));
                assert_eq!(rows[1].get("page"), Some(&serde_json::json!("home")));
                // 3 + 2 + 5 = 10
                assert_eq!(rows[1].get("total"), Some(&serde_json::json!(10)));
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn group_by_having_filters_post_aggregation() {
        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            filter: None,
            having: Some(HavingClause::GreaterThan {
                aggregation: "total".into(),
                value: 5.0,
            }),
            sort: None,
            limit: None,
        });
        match q.execute(&fixture()) {
            NativeQueryResult::GroupBy(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("total"), Some(&serde_json::json!(10)));
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn group_by_sort_descending_by_metric_with_limit() {
        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: None,
            having: None,
            sort: Some(vec![SortSpec {
                dimension: "rows".into(),
                direction: SortDirection::Descending,
            }]),
            limit: Some(1),
        });
        match q.execute(&fixture()) {
            NativeQueryResult::GroupBy(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("rows"), Some(&serde_json::json!(3)));
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn group_by_filter_skips_rows_pre_aggregation() {
        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: Some(EqualsFilter {
                dimension: "page".into(),
                value: "home".into(),
            }),
            having: None,
            sort: None,
            limit: None,
        });
        match q.execute(&fixture()) {
            NativeQueryResult::GroupBy(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("rows"), Some(&serde_json::json!(3)));
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn top_n_ranks_descending_by_metric_with_threshold() {
        let q = NativeQuery::TopN(TopNSpec {
            data_source: "wikipedia".into(),
            dimension: "page".into(),
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            metric: "total".into(),
            threshold: 1,
            filter: None,
        });
        match q.execute(&fixture()) {
            NativeQueryResult::TopN(rows) => {
                assert_eq!(rows.len(), 1);
                // home wins: 3+2+5=10 vs about=1.
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("total"), Some(&serde_json::json!(10)));
            }
            other => panic!("expected topN, got {other:?}"),
        }
    }

    #[test]
    fn top_n_returns_all_groups_when_threshold_exceeds_cardinality() {
        let q = NativeQuery::TopN(TopNSpec {
            data_source: "wikipedia".into(),
            dimension: "page".into(),
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            metric: "rows".into(),
            threshold: 100,
            filter: None,
        });
        match q.execute(&fixture()) {
            NativeQueryResult::TopN(rows) => {
                assert_eq!(rows.len(), 2);
                // First row is the higher-count "home".
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("rows"), Some(&serde_json::json!(3)));
                assert_eq!(rows[1].get("page"), Some(&serde_json::json!("about")));
                assert_eq!(rows[1].get("rows"), Some(&serde_json::json!(1)));
            }
            other => panic!("expected topN, got {other:?}"),
        }
    }

    #[test]
    fn merge_group_by_combines_per_segment_partial_folds() {
        let spec = GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            filter: None,
            having: None,
            sort: Some(vec![SortSpec {
                dimension: "page".into(),
                direction: SortDirection::Ascending,
            }]),
            limit: None,
        };
        let part_a = vec![
            serde_json::json!({"page":"home","total":3})
                .as_object()
                .expect("obj")
                .clone(),
            serde_json::json!({"page":"about","total":1})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        let part_b = vec![
            serde_json::json!({"page":"home","total":7})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        let merged = merge_group_by(vec![part_a, part_b], &spec);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].get("page"), Some(&serde_json::json!("about")));
        assert_eq!(merged[0].get("total"), Some(&serde_json::json!(1)));
        assert_eq!(merged[1].get("page"), Some(&serde_json::json!("home")));
        // 3 + 7 = 10 cluster-wide.
        assert_eq!(merged[1].get("total"), Some(&serde_json::json!(10)));
    }

    #[test]
    fn merge_top_n_re_ranks_after_combining_segments() {
        let spec = TopNSpec {
            data_source: "wikipedia".into(),
            dimension: "page".into(),
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            metric: "total".into(),
            threshold: 2,
            filter: None,
        };
        // Per-segment: a=5, b=4 → segment 1 winner is "a".
        let part_a = vec![
            serde_json::json!({"page":"a","total":5})
                .as_object()
                .expect("obj")
                .clone(),
            serde_json::json!({"page":"b","total":4})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        // Per-segment: b=10 → segment 2 contributes 10 to "b".
        let part_b = vec![
            serde_json::json!({"page":"b","total":10})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        // Cluster-wide: b=14, a=5 → ordering flips.
        let merged = merge_top_n(vec![part_a, part_b], &spec);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].get("page"), Some(&serde_json::json!("b")));
        assert_eq!(merged[0].get("total"), Some(&serde_json::json!(14)));
        assert_eq!(merged[1].get("page"), Some(&serde_json::json!("a")));
        assert_eq!(merged[1].get("total"), Some(&serde_json::json!(5)));
    }

    #[test]
    fn group_by_round_trips_via_serde_json() {
        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: None,
            having: Some(HavingClause::GreaterThanOrEqual {
                aggregation: "rows".into(),
                value: 2.0,
            }),
            sort: Some(vec![SortSpec {
                dimension: "rows".into(),
                direction: SortDirection::Descending,
            }]),
            limit: Some(5),
        });
        let s = serde_json::to_string(&q).expect("ser");
        let back: NativeQuery = serde_json::from_str(&s).expect("de");
        assert_eq!(back, q);
        assert!(s.contains("\"queryType\":\"groupBy\""), "{s}");
    }

    #[test]
    fn top_n_round_trips_via_serde_json() {
        let q = NativeQuery::TopN(TopNSpec {
            data_source: "wikipedia".into(),
            dimension: "page".into(),
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            metric: "rows".into(),
            threshold: 10,
            filter: None,
        });
        let s = serde_json::to_string(&q).expect("ser");
        let back: NativeQuery = serde_json::from_str(&s).expect("de");
        assert_eq!(back, q);
        assert!(s.contains("\"queryType\":\"topN\""), "{s}");
    }
}
