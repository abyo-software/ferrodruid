// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! TopN query type — find the top N dimension values by a metric.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use ferrodruid_aggregator::{AggregatorSpec, PostAggregatorSpec};
use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_common::types::{DataSource, DimensionSpec};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::ColumnData;

use crate::context::QueryContext;
use crate::dim_spec::{CompiledDimSpec, GroupKey};
use crate::filter::FilterSpec;
use crate::helpers::{
    GranularitySpec, bucket_timestamp, build_row_prealloc, build_row_update_only,
    deserialize_intervals, feed_aggregator_row_from_map, finalize_agg_value, parse_intervals,
    pruned_row_range, substitute_cardinality_partials, validate_granularity,
};
use crate::timeseries::format_epoch_millis;
use crate::virtual_columns::{VirtualColumnSpec, VirtualColumns};

/// Default in-flight per-key cap for TopN queries.  Mirrors
/// `QueryLimitsConfig::topn_max_inflight_threshold` and is used when the
/// caller invokes `execute` without an explicit limit (e.g. from a
/// non-REST integration path).  Wave 36-G1 (Wave 37B query Top-1 DoS).
pub const DEFAULT_TOPN_MAX_INFLIGHT: usize = 100_000;

// ---------------------------------------------------------------------------
// TopNMetricSpec
// ---------------------------------------------------------------------------

/// Specifies how to rank dimension values in a TopN query.
///
/// Wave 47-D §4 closes the divergence where FerroDruid required the
/// verbose tagged form: Druid also accepts a bare-string shorthand
/// (`"metric": "cnt"`) for `Numeric { metric: "cnt" }`.  Both variants
/// are now decoded; serialisation always emits the canonical tagged
/// form.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TopNMetricSpec {
    /// Rank by a numeric metric value.
    #[serde(rename = "numeric")]
    Numeric {
        /// The aggregation output name to sort by.
        metric: String,
    },
    /// Rank by dimension value ordering.
    #[serde(rename = "dimension")]
    Dimension {
        /// Ordering type (e.g. `"lexicographic"`, `"numeric"`).
        #[serde(default)]
        ordering: Option<String>,
        /// Optional previous-stop for pagination.
        #[serde(default, rename = "previousStop")]
        previous_stop: Option<String>,
    },
    /// Inverted metric (bottom-N).
    #[serde(rename = "inverted")]
    Inverted {
        /// The metric spec to invert.
        metric: Box<TopNMetricSpec>,
    },
}

impl<'de> Deserialize<'de> for TopNMetricSpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "camelCase")]
        enum Tagged {
            #[serde(rename = "numeric")]
            Numeric { metric: String },
            #[serde(rename = "dimension")]
            Dimension {
                #[serde(default)]
                ordering: Option<String>,
                #[serde(default, rename = "previousStop")]
                previous_stop: Option<String>,
            },
            #[serde(rename = "inverted")]
            Inverted { metric: Box<TopNMetricSpec> },
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            // String shorthand — Druid accepts `"metric":"cnt"` as
            // `{"type":"numeric","metric":"cnt"}`.
            Bare(String),
            Tagged(Tagged),
        }

        Ok(match Either::deserialize(deserializer)? {
            Either::Bare(name) => TopNMetricSpec::Numeric { metric: name },
            Either::Tagged(Tagged::Numeric { metric }) => TopNMetricSpec::Numeric { metric },
            Either::Tagged(Tagged::Dimension {
                ordering,
                previous_stop,
            }) => TopNMetricSpec::Dimension {
                ordering,
                previous_stop,
            },
            Either::Tagged(Tagged::Inverted { metric }) => TopNMetricSpec::Inverted { metric },
        })
    }
}

// ---------------------------------------------------------------------------
// Query spec
// ---------------------------------------------------------------------------

/// A Druid TopN query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopNQuery {
    /// The data source to query.
    pub data_source: DataSource,
    /// Time intervals to query over.
    ///
    /// Accepts both a single ISO `"start/end"` string and an array of
    /// such strings (TG-4-finding-001, W2-D pydruid/druid-go compat).
    #[serde(deserialize_with = "deserialize_intervals")]
    pub intervals: Vec<String>,
    /// Granularity for time bucketing.
    pub granularity: GranularitySpec,
    /// The dimension to compute TopN over.
    pub dimension: DimensionSpec,
    /// Number of top values to return.
    pub threshold: usize,
    /// The metric used to rank dimension values.
    pub metric: TopNMetricSpec,
    /// Optional filter.
    #[serde(default)]
    pub filter: Option<FilterSpec>,
    /// Optional virtual columns (derived columns computed by expression).
    #[serde(default)]
    pub virtual_columns: Option<Vec<VirtualColumnSpec>>,
    /// Aggregators to compute.
    pub aggregations: Vec<AggregatorSpec>,
    /// Optional post-aggregators.
    #[serde(default)]
    pub post_aggregations: Option<Vec<PostAggregatorSpec>>,
    /// Optional query context.
    #[serde(default)]
    pub context: Option<QueryContext>,
}

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

/// A single TopN result entry (one per time bucket).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopNResult {
    /// The bucket timestamp.
    pub timestamp: String,
    /// Ranked dimension values with their aggregation results.
    pub result: Vec<serde_json::Map<String, serde_json::Value>>,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

impl TopNQuery {
    /// Execute this TopN query against a segment using the default
    /// in-flight key cap (`DEFAULT_TOPN_MAX_INFLIGHT`).
    ///
    /// Use [`Self::execute_with_limit`] to override the cap from a
    /// `QueryLimitsConfig`.
    pub fn execute(&self, segment: &SegmentData) -> Result<Vec<TopNResult>> {
        self.execute_with_limit(segment, DEFAULT_TOPN_MAX_INFLIGHT)
    }

    /// DD R43 (Finding 7): validate that every numeric metric referenced by
    /// the metric spec names a real aggregation or post-aggregation output.
    /// A dimension-ordered metric ranks by the dimension value itself and so
    /// references no aggregation.
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Query`] when a numeric metric names an output
    /// that is not produced by this query's aggregations / post-aggregations.
    fn validate_metric(&self) -> Result<()> {
        fn check(metric: &TopNMetricSpec, valid: &[String]) -> Result<()> {
            match metric {
                TopNMetricSpec::Numeric { metric: name } => {
                    if valid.iter().any(|v| v == name) {
                        Ok(())
                    } else {
                        Err(DruidError::Query(format!(
                            "TopN metric `{name}` does not name any aggregation or \
                             post-aggregation of this query"
                        )))
                    }
                }
                TopNMetricSpec::Inverted { metric: inner } => check(inner, valid),
                // Dimension ordering ranks by the dimension value, not a metric.
                TopNMetricSpec::Dimension { .. } => Ok(()),
            }
        }

        let mut valid: Vec<String> = self
            .aggregations
            .iter()
            .map(|a| a.name().to_owned())
            .collect();
        if let Some(ref post_aggs) = self.post_aggregations {
            valid.extend(post_aggs.iter().map(|p| p.name().to_owned()));
        }
        check(&self.metric, &valid)
    }

    /// Execute this TopN query with an explicit in-flight key cap.
    ///
    /// `max_inflight_threshold` bounds the **summed** number of distinct
    /// dimension values retained across all time buckets *before*
    /// truncation to `self.threshold`.  Pass `0` to disable the guard
    /// (not recommended in production).  When the guard fires the query
    /// returns [`DruidError::ResourceLimit`], which the REST layer
    /// translates into `429 Too Many Keys`.
    ///
    /// Wave 36-G1 closes Wave 37B query Top-1 (DoS via high-cardinality
    /// dimensions) and Wave 37B query High #2 (extraction / list-filtered
    /// / regex-filtered / prefix-filtered wrappers were silently
    /// dropped).
    pub fn execute_with_limit(
        &self,
        segment: &SegmentData,
        max_inflight_threshold: usize,
    ) -> Result<Vec<TopNResult>> {
        // Validate post-aggregators up front so unsupported variants error
        // cleanly rather than silently dropping the derived field.
        if let Some(ref post_aggs) = self.post_aggregations {
            for pa in post_aggs {
                pa.validate_supported()?;
            }
        }

        // DD R43 (Finding 7): a numeric metric naming an aggregation that does
        // not exist read `unwrap_or(0.0)` per row, so every row tied at 0.0 and
        // a wrong top-N was returned silently. Validate that every numeric
        // metric the spec references resolves to one of the query's
        // aggregation / post-aggregation output names before execution.
        self.validate_metric()?;

        // DD R40: reject a malformed expression filter up front instead of
        // letting it silently match every row (fail-open data exposure).
        if let Some(ref filter) = self.filter {
            filter.validate()?;
        }
        // DD R48: reject a duration granularity with periodMs == 0 / out-of-i64.
        validate_granularity(&self.granularity)?;

        let intervals = parse_intervals(&self.intervals)?;
        let timestamps = segment.timestamp_column()?;
        let dim_name = dimension_spec_name(&self.dimension);
        let virtual_columns = VirtualColumns::compile(&self.virtual_columns)?;
        // Wave 45-F: compile every regex inside the dimension spec exactly
        // once at plan time.  Malformed patterns surface here as
        // [`DruidError::Query`] instead of becoming silent per-row no-
        // matches in the loop below.
        let compiled_dim = CompiledDimSpec::new(&self.dimension)?;

        // Vectorized single-thread fast path for the common shape (granularity
        // "all", a plain Default dimension over a Long/String column, typed
        // sum/count aggregators, no virtual columns, typed-decidable filter).
        // Replaces the per-row row-map build + boxed serde_json aggregator
        // dispatch and hands off to the same ranking/threshold logic below.
        if let Some(fast) = self.try_vectorized_topn(
            segment,
            &virtual_columns,
            &intervals,
            timestamps,
            max_inflight_threshold,
        )? {
            return Ok(fast);
        }

        // bucket_key -> { dim_value -> Vec<aggregators> }
        //
        // Wave 40-B (Wave 39 [High] [NEW-VARIANT] topn.rs:166-173): keyed by
        // typed `GroupKey` rather than the previous string-coerced form, so
        // numeric/boolean/null dimensions no longer collide with their
        // stringified representations during the per-bucket aggregation.
        type DimAggs = HashMap<GroupKey, Vec<Box<dyn ferrodruid_aggregator::Aggregator>>>;
        let mut buckets: HashMap<i64, DimAggs> = HashMap::new();
        let mut inflight: usize = 0;

        // W3-SL1-B step 1: hoist row-map allocation out of the loop.
        let mut row = build_row_prealloc(segment);

        for (row_idx, &ts) in timestamps.iter().enumerate().take(segment.num_rows()) {
            if !intervals.is_empty()
                && !intervals
                    .iter()
                    .any(|(start, end)| ts >= *start && ts < *end)
            {
                continue;
            }

            // W3-SL1-B step 3 (Task #31): typed fast-path — see
            // `timeseries.rs` for the design note. Same pattern.
            let fast_rejected = self.filter.as_ref().is_some_and(|filter| {
                virtual_columns.is_empty()
                    && matches!(filter.matches_typed(segment, row_idx), Some(false))
            });
            if fast_rejected {
                continue;
            }
            build_row_update_only(segment, row_idx, &mut row);
            virtual_columns.augment_row(&mut row);
            if let Some(filter) = &self.filter {
                let fast_accepted = virtual_columns.is_empty()
                    && matches!(filter.matches_typed(segment, row_idx), Some(true));
                if !fast_accepted && !filter.matches(&row) {
                    continue;
                }
            }

            let bucket_key = bucket_timestamp(ts, &self.granularity);
            // Resolve the dimension value from the augmented row so an
            // expression virtual column can be the TopN dimension.
            let dv = row
                .get(dim_name)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            // Apply DimensionSpec wrappers (extraction / listFiltered /
            // regexFiltered / prefixFiltered) — Wave 37B High #2.  A
            // wrapper that rejects the value drops the row from the
            // per-bucket aggregation, matching Druid semantics.
            //
            // Wave 40-B: `apply_dim_spec_typed` preserves the JSON type
            // tag for `Default` dimensions so numeric/boolean dims do not
            // collide with their stringified form.
            let Some(dv_key) = compiled_dim.apply_typed(&dv) else {
                continue;
            };

            let dim_aggs = buckets.entry(bucket_key).or_default();
            let key_was_new = !dim_aggs.contains_key(&dv_key);
            if key_was_new && max_inflight_threshold > 0 && inflight >= max_inflight_threshold {
                return Err(DruidError::ResourceLimit {
                    kind: "topN.maxIntermediateRows",
                    limit: max_inflight_threshold,
                    observed: inflight + 1,
                });
            }
            let aggs = dim_aggs
                .entry(dv_key)
                .or_insert_with(|| self.aggregations.iter().map(|spec| spec.create()).collect());
            if key_was_new {
                inflight += 1;
            }

            for (i, spec) in self.aggregations.iter().enumerate() {
                feed_aggregator_row_from_map(segment, row_idx, &row, spec, aggs[i].as_mut());
            }
        }

        let mut bucket_keys: Vec<i64> = buckets.keys().copied().collect();
        bucket_keys.sort();

        // Wave 47-D §5: for `granularity=all`, Druid stamps the single
        // emitted bucket with the start of the requested interval rather
        // than the synthetic `0` epoch produced by `bucket_timestamp`.
        // Use the earliest interval start when the query supplies one;
        // otherwise leave the bucket key as-is (no intervals → no
        // anchor, fall through to 1970 to keep parity with empty-result
        // shape).
        let all_granularity_anchor: Option<i64> = if is_all_granularity(&self.granularity) {
            intervals.iter().map(|(start, _)| *start).min()
        } else {
            None
        };

        let mut results = Vec::with_capacity(bucket_keys.len());
        for key in bucket_keys {
            let dim_aggs = buckets.get(&key).expect("bucket exists");
            // Sort uses the typed key's lex-form (Wave 40-B); we keep a
            // parallel `String` for the secondary tiebreaker in
            // `sort_topn_entries`.  The third tuple slot carries the live
            // aggregator vector so exact-cardinality outputs can be
            // envelope-substituted AFTER ranking/threshold truncation
            // (multi-shard exact union, 2026-07-11).
            type EntryAggs<'a> = &'a Vec<Box<dyn ferrodruid_aggregator::Aggregator>>;
            let mut entries: Vec<(
                String,
                serde_json::Map<String, serde_json::Value>,
                EntryAggs<'_>,
            )> = Vec::with_capacity(dim_aggs.len());

            for (dv, aggs) in dim_aggs {
                let mut m = serde_json::Map::new();
                let out_name = dimension_spec_output_name(&self.dimension);
                m.insert(out_name.to_string(), dv.to_json());
                for (i, spec) in self.aggregations.iter().enumerate() {
                    // Fail-closed (2026-07-11): a saturated exact-cardinality
                    // aggregator must error out here, never finalize to a
                    // silently capped count (see `finalize_agg_value`).
                    m.insert(
                        spec.name().to_string(),
                        finalize_agg_value(aggs[i].as_ref())?,
                    );
                }
                // Post-aggregations
                if let Some(ref post_aggs) = self.post_aggregations {
                    let agg_results: HashMap<String, serde_json::Value> =
                        m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    for pa in post_aggs {
                        // A null post-aggregation result — evaluate() None (e.g. an
                        // expression over a missing/null operand or IEEE x/0) or a
                        // non-finite value — emits an explicit JSON null under the output
                        // name, matching Druid. Omitting the key entirely (pre codex-r3)
                        // silently dropped the column from native result rows.
                        let json_val = pa
                            .evaluate(&agg_results)
                            .and_then(serde_json::Number::from_f64)
                            .map_or(serde_json::Value::Null, serde_json::Value::Number);
                        m.insert(pa.name().to_string(), json_val);
                    }
                }
                entries.push((dv.as_sort_key(), m, aggs));
            }

            // Sort by metric.
            sort_topn_entries(&mut entries, &self.metric);

            // Apply `previousStop` pagination filter (Wave 45-A, Wave 37B
            // query Medium #1).  When the query is dimension-ordered the
            // caller may pass a `previousStop` cursor to resume after a
            // prior page; entries that are not strictly past the cursor
            // (per the dimension's ordering) are dropped before truncation.
            apply_previous_stop(&mut entries, &self.metric);

            // Truncate to threshold.
            entries.truncate(self.threshold);

            // Multi-shard exact union (2026-07-11): swap exact-cardinality
            // outputs for their full-set envelopes AFTER the metric ranking
            // and threshold truncation ran on the exact per-segment counts.
            // The broker merge unions the surviving entries' envelopes
            // across segments and re-ranks on the exact union counts.
            for (_, m, aggs) in &mut entries {
                substitute_cardinality_partials(&self.aggregations, aggs, m);
            }

            let bucket_ts = all_granularity_anchor.unwrap_or(key);
            results.push(TopNResult {
                timestamp: format_epoch_millis(bucket_ts),
                result: entries.into_iter().map(|(_, m, _)| m).collect(),
            });
        }

        Ok(results)
    }

    /// Vectorized single-thread fast path for [`Self::execute_with_limit`].
    /// See the call site for the eligibility summary. Returns
    /// `Ok(Some(results))` when handled, `Ok(None)` to fall back. Accumulates
    /// over typed column slices with an integer-keyed map (no per-row row-map
    /// build or boxed `serde_json` dispatch), then reuses the exact
    /// ranking/threshold/emit logic of the row-oriented path.
    #[allow(clippy::too_many_lines)]
    fn try_vectorized_topn(
        &self,
        segment: &SegmentData,
        virtual_columns: &VirtualColumns,
        intervals: &[(i64, i64)],
        timestamps: &[i64],
        max_inflight_threshold: usize,
    ) -> Result<Option<Vec<TopNResult>>> {
        if !virtual_columns.is_empty()
            || !is_all_granularity(&self.granularity)
            || !matches!(self.dimension, DimensionSpec::Default { .. })
        {
            return Ok(None);
        }

        enum DimKind<'a> {
            Long(&'a [i64]),
            Str(&'a ferrodruid_segment::column::StringColumnData),
        }
        let dim = match segment.columns.get(dimension_spec_name(&self.dimension)) {
            Some(ColumnData::Long(v)) => DimKind::Long(v.as_slice()),
            Some(ColumnData::String(sc)) => {
                // Null gate (D3): this path keys groups on raw dictionary
                // codes, and a SQL-NULL row shares the `""` placeholder
                // ordinal — it would silently merge the null group into the
                // `""` group. Null-bearing dims take the general path (which
                // keys `GroupKey::Null` via the row map). O(1) layout check;
                // null-free columns are unaffected.
                if sc.null_rows().is_some() {
                    return Ok(None);
                }
                DimKind::Str(sc)
            }
            _ => return Ok(None),
        };

        enum AggPlan<'a> {
            Count,
            LongSum(&'a [i64]),
            DoubleSum(&'a [f64]),
        }
        let mut plans: Vec<AggPlan> = Vec::with_capacity(self.aggregations.len());
        for spec in &self.aggregations {
            plans.push(match spec {
                AggregatorSpec::Count { .. } => AggPlan::Count,
                AggregatorSpec::LongSum { field_name, .. } => match segment.columns.get(field_name)
                {
                    Some(ColumnData::Long(v)) => AggPlan::LongSum(v.as_slice()),
                    _ => return Ok(None),
                },
                AggregatorSpec::DoubleSum { field_name, .. } => {
                    match segment.columns.get(field_name) {
                        Some(ColumnData::Double(v)) => AggPlan::DoubleSum(v.as_slice()),
                        _ => return Ok(None),
                    }
                }
                _ => return Ok(None),
            });
        }

        let num_rows = segment.num_rows().min(timestamps.len());
        if let Some(f) = &self.filter
            && num_rows > 0
            && f.matches_typed(segment, 0).is_none()
        {
            return Ok(None);
        }

        // Double sums carry a `seen` flag (any non-null contribution) so an
        // all-null group emits SQL null, matching Druid. Long sums need no
        // flag: `ColumnData::Long` has no in-band null (null-bearing long
        // input is stored as Double/NaN and falls off this fast path).
        #[derive(Clone, Copy)]
        enum Acc {
            Count(u64),
            Long(i64),
            Double(f64, bool),
        }
        let make_accs = || -> Vec<Acc> {
            plans
                .iter()
                .map(|p| match p {
                    AggPlan::Count => Acc::Count(0),
                    AggPlan::LongSum(_) => Acc::Long(0),
                    AggPlan::DoubleSum(_) => Acc::Double(0.0, false),
                })
                .collect()
        };
        let filter = self.filter.as_ref();
        // Compile the filter once (borrowed column slices + pre-parsed bounds)
        // so the per-row test is a branch-light predicate, no HashMap lookup or
        // string parse. Falls back to `matches_typed` for shapes it can't
        // resolve. Q3 carries no filter, so this is None there.
        let compiled_filter = filter.and_then(|f| f.compile_typed(segment));
        // Time-interval pruning: on a sorted __time column, iterate only the
        // row range inside the interval (Q3's interval is one month of ~7
        // years of data — ~85x fewer rows than a full scan).
        let (scan_lo, scan_hi, check_interval) =
            match pruned_row_range(timestamps, intervals, segment.time_sorted) {
                Some((lo, hi)) => (lo, hi.min(num_rows), false),
                None => (0, num_rows, !intervals.is_empty()),
            };

        // Accumulate the [start, end) row range into a dim-keyed group map
        // (key = Long dim value, or String dict code, both as i64).
        let accumulate = |start: usize, end: usize| -> HashMap<i64, Vec<Acc>> {
            let mut groups: HashMap<i64, Vec<Acc>> = HashMap::new();
            for row in start..end {
                if check_interval {
                    let ts = timestamps[row];
                    if !intervals.iter().any(|(s, e)| ts >= *s && ts < *e) {
                        continue;
                    }
                }
                if let Some(cf) = &compiled_filter {
                    if !cf.eval(row) {
                        continue;
                    }
                } else if let Some(f) = filter
                    && f.matches_typed(segment, row) != Some(true)
                {
                    continue;
                }
                let key = match &dim {
                    DimKind::Long(v) => v[row],
                    DimKind::Str(sc) => i64::from(sc.encoded_values[row]),
                };
                let accs = groups.entry(key).or_insert_with(&make_accs);
                for (k, plan) in plans.iter().enumerate() {
                    match (plan, &mut accs[k]) {
                        (AggPlan::Count, Acc::Count(c)) => *c += 1,
                        (AggPlan::LongSum(col), Acc::Long(s)) => {
                            if let Some(&x) = col.get(row) {
                                *s = s.wrapping_add(x);
                            }
                        }
                        (AggPlan::DoubleSum(col), Acc::Double(s, seen)) => {
                            // NaN = SQL NULL (in-band marker): skip so a null
                            // never poisons the sum (Druid: SUM ignores nulls).
                            // `seen` records ≥1 non-null contribution
                            // (all-null ⇒ SQL null on emit).
                            if let Some(&x) = col.get(row)
                                && !x.is_nan()
                            {
                                *s += x;
                                *seen = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
            groups
        };

        // Merge `src` group accumulators into `dst`.
        let merge = |dst: &mut HashMap<i64, Vec<Acc>>, src: HashMap<i64, Vec<Acc>>| {
            for (key, sv) in src {
                match dst.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        for (d, s) in e.get_mut().iter_mut().zip(sv) {
                            match (d, s) {
                                (Acc::Count(a), Acc::Count(b)) => *a += b,
                                (Acc::Long(a), Acc::Long(b)) => *a = a.wrapping_add(b),
                                (Acc::Double(a, sa), Acc::Double(b, sb)) => {
                                    *a += b;
                                    *sa |= sb;
                                }
                                _ => {}
                            }
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(sv);
                    }
                }
            }
        };

        // Parallel dense-scan: split the pruned range across rayon's shared
        // pool, build per-thread group maps, then merge. High-cardinality
        // dimensions (Q3: ~180k distinct orderkeys) make the HashMap probing
        // the dominant cost, which parallelizes cleanly. f64 sums become
        // order-non-deterministic (matches Druid parallel aggregation, per the
        // product decision). Small scans stay single-threaded so unit tests are
        // byte-identical.
        const PARALLEL_MIN_ROWS: usize = 250_000;
        let scan_len = scan_hi.saturating_sub(scan_lo);
        let groups: HashMap<i64, Vec<Acc>> =
            if scan_len >= PARALLEL_MIN_ROWS && rayon::current_num_threads() > 1 {
                use rayon::iter::{IntoParallelIterator, ParallelIterator};
                let n = rayon::current_num_threads().max(1);
                let chunk = scan_len.div_ceil(n * 4).max(1 << 14);
                let starts: Vec<usize> = (scan_lo..scan_hi).step_by(chunk).collect();
                starts
                    .into_par_iter()
                    .map(|start| accumulate(start, (start + chunk).min(scan_hi)))
                    .reduce(HashMap::new, |mut a, b| {
                        if a.len() >= b.len() {
                            merge(&mut a, b);
                            a
                        } else {
                            let mut b = b;
                            merge(&mut b, a);
                            b
                        }
                    })
            } else {
                accumulate(scan_lo, scan_hi)
            };

        // Enforce the intermediate-rows resource limit on the merged
        // cardinality (distinct dimension values seen).
        if max_inflight_threshold > 0 && groups.len() > max_inflight_threshold {
            return Err(DruidError::ResourceLimit {
                kind: "topN.maxIntermediateRows",
                limit: max_inflight_threshold,
                observed: groups.len(),
            });
        }

        let out_name = dimension_spec_output_name(&self.dimension);
        let make_dv = |key: i64| -> GroupKey {
            match &dim {
                DimKind::Long(_) => GroupKey::Long(key),
                DimKind::Str(sc) => usize::try_from(key)
                    .ok()
                    .and_then(|o| sc.dictionary.get(o))
                    .map_or(GroupKey::Null, |s| GroupKey::String(s.to_string())),
            }
        };
        let agg_value = |acc: Acc| -> serde_json::Value {
            match acc {
                Acc::Count(c) => serde_json::Value::Number(serde_json::Number::from(c)),
                Acc::Long(s) => serde_json::Value::Number(serde_json::Number::from(s)),
                // No non-null contribution ⇒ SQL null (Druid: SUM over an
                // all-null group is null, not 0).
                Acc::Double(_, false) => serde_json::Value::Null,
                Acc::Double(s, true) => serde_json::to_value(s).unwrap_or(serde_json::Value::Null),
            }
        };

        // Fast top-K: for a numeric metric naming an aggregator (no post-aggs),
        // rank groups by that aggregator's value with the dim-sort tiebreak
        // (mirroring `sort_topn_entries`), then build output maps for only the
        // top `threshold` survivors. Avoids a `serde_json::Map` per group — the
        // dominant cost once a high-cardinality dimension is interval-pruned.
        let no_post = self.post_aggregations.as_ref().is_none_or(Vec::is_empty);
        if no_post
            && let TopNMetricSpec::Numeric { metric: mname } = &self.metric
            && let Some(mi) = self.aggregations.iter().position(|a| a.name() == mname)
        {
            let mut ranked: Vec<(f64, String, i64)> = groups
                .iter()
                .map(|(&key, accs)| {
                    let mv = match accs[mi] {
                        Acc::Count(c) => c as f64,
                        Acc::Long(s) => s as f64,
                        // An unseen (all-null ⇒ SQL null) sum ranks as 0.0 —
                        // exactly how `sort_topn_entries` ranks a JSON null
                        // (`as_f64().unwrap_or(0.0)`), keeping fast/general
                        // path ordering identical. The unseen sum is 0.0.
                        Acc::Double(s, _) => s,
                    };
                    (mv, make_dv(key).as_sort_key(), key)
                })
                .collect();
            ranked.sort_by(|a, b| {
                b.0.partial_cmp(&a.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.1.cmp(&b.1))
            });
            ranked.truncate(self.threshold);
            let result: Vec<serde_json::Map<String, serde_json::Value>> = ranked
                .iter()
                .map(|&(_, _, key)| {
                    let accs = &groups[&key];
                    let mut m = serde_json::Map::new();
                    m.insert(out_name.to_string(), make_dv(key).to_json());
                    for (k, spec) in self.aggregations.iter().enumerate() {
                        m.insert(spec.name().to_string(), agg_value(accs[k]));
                    }
                    m
                })
                .collect();
            let anchor = intervals.iter().map(|(s, _)| *s).min().unwrap_or(0);
            return Ok(Some(vec![TopNResult {
                timestamp: format_epoch_millis(anchor),
                result,
            }]));
        }

        // Emit — build entries then reuse the exact ranking/threshold logic.
        // Unit payload: this path never plans cardinality aggregators, so
        // there is nothing to envelope-substitute after ranking.
        let mut entries: Vec<(String, serde_json::Map<String, serde_json::Value>, ())> =
            Vec::with_capacity(groups.len());
        for (key, accs) in &groups {
            let dv = match &dim {
                DimKind::Long(_) => GroupKey::Long(*key),
                DimKind::Str(sc) => usize::try_from(*key)
                    .ok()
                    .and_then(|o| sc.dictionary.get(o))
                    .map_or(GroupKey::Null, |s| GroupKey::String(s.to_string())),
            };
            let mut m = serde_json::Map::new();
            m.insert(out_name.to_string(), dv.to_json());
            for (k, spec) in self.aggregations.iter().enumerate() {
                m.insert(spec.name().to_string(), agg_value(accs[k]));
            }
            if let Some(ref post_aggs) = self.post_aggregations {
                let agg_results: HashMap<String, serde_json::Value> =
                    m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                for pa in post_aggs {
                    // A null post-aggregation result — evaluate() None (e.g. an
                    // expression over a missing/null operand or IEEE x/0) or a
                    // non-finite value — emits an explicit JSON null under the output
                    // name, matching Druid. Omitting the key entirely (pre codex-r3)
                    // silently dropped the column from native result rows.
                    let json_val = pa
                        .evaluate(&agg_results)
                        .and_then(serde_json::Number::from_f64)
                        .map_or(serde_json::Value::Null, serde_json::Value::Number);
                    m.insert(pa.name().to_string(), json_val);
                }
            }
            entries.push((dv.as_sort_key(), m, ()));
        }
        sort_topn_entries(&mut entries, &self.metric);
        apply_previous_stop(&mut entries, &self.metric);
        entries.truncate(self.threshold);
        let anchor = intervals.iter().map(|(s, _)| *s).min().unwrap_or(0);
        Ok(Some(vec![TopNResult {
            timestamp: format_epoch_millis(anchor),
            result: entries.into_iter().map(|(_, m, ())| m).collect(),
        }]))
    }
}

/// Returns `true` if the granularity collapses every row into a single
/// bucket — Druid's `"all"` granularity.  The interval start is then
/// used as the bucket timestamp instead of the `0` epoch returned by
/// the global `bucket_timestamp` helper.
fn is_all_granularity(g: &GranularitySpec) -> bool {
    matches!(g, GranularitySpec::Simple(s) if s.eq_ignore_ascii_case("all"))
}

/// Sort TopN entries according to the metric spec.
///
/// Generic over a per-entry payload `T` (the row-oriented path carries the
/// live aggregator vector for post-ranking envelope substitution; the
/// vectorized path carries `()`).
fn sort_topn_entries<T>(
    entries: &mut [(String, serde_json::Map<String, serde_json::Value>, T)],
    metric: &TopNMetricSpec,
) {
    match metric {
        TopNMetricSpec::Numeric { metric: name } => {
            entries.sort_by(|(da, a, _), (db, b, _)| {
                let va = a.get(name).and_then(|v| v.as_f64()).unwrap_or(0.0);
                let vb = b.get(name).and_then(|v| v.as_f64()).unwrap_or(0.0);
                // Primary: descending by metric.  Secondary tiebreaker:
                // dimension lexicographic ascending, matching Druid 30
                // TopN semantics for stable ties.
                vb.partial_cmp(&va)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| da.cmp(db))
            });
        }
        TopNMetricSpec::Dimension { ordering, .. } => {
            let numeric = ordering.as_deref() == Some("numeric");
            entries.sort_by(|(a, _, _), (b, _, _)| {
                if numeric {
                    let va: f64 = a.parse().unwrap_or(0.0);
                    let vb: f64 = b.parse().unwrap_or(0.0);
                    va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    a.cmp(b)
                }
            });
        }
        TopNMetricSpec::Inverted { metric: inner } => {
            sort_topn_entries(entries, inner);
            entries.reverse();
        }
    }
}

/// Apply the `previousStop` cursor of a dimension-ordered TopN metric
/// spec to the post-sort entry list.
///
/// Wave 45-A (Wave 37B query Medium #1): `previousStop` was parsed from
/// JSON but never enforced, so paginated dimension-ordered queries
/// could repeat or skip rows.  This helper drops entries that are not
/// strictly past the cursor according to the same ordering used during
/// `sort_topn_entries`:
///
/// * `Dimension { ordering: Some("numeric"), previous_stop }` — keep
///   entries whose key parses as `f64` and is `> previous_stop` (also
///   parsed as `f64`).  Unparseable keys are kept (safe default — a
///   malformed cursor must not silently truncate the page).
/// * `Dimension { ordering: _, previous_stop }` — lexicographic; keep
///   entries whose key is `> previous_stop`.
/// * `Numeric` / `Inverted` — `previousStop` is not part of those
///   variants, so no filtering is applied.  An `Inverted { Dimension { … } }`
///   recursion would change the ordering direction, which is why the
///   inverted arm only reverses but does not re-apply the cursor; this
///   matches Druid's documented semantics where `previousStop` is only
///   meaningful for plain dimension ordering.
fn apply_previous_stop<T>(
    entries: &mut Vec<(String, serde_json::Map<String, serde_json::Value>, T)>,
    metric: &TopNMetricSpec,
) {
    let TopNMetricSpec::Dimension {
        ordering,
        previous_stop: Some(stop),
    } = metric
    else {
        return;
    };
    let numeric = ordering.as_deref() == Some("numeric");
    if numeric {
        let stop_f = stop.parse::<f64>();
        entries.retain(|(key, _, _)| match (&stop_f, key.parse::<f64>()) {
            (Ok(s), Ok(k)) => k > *s,
            // Either the cursor or the entry is not numeric.  Be
            // conservative: keep the entry (do not silently drop rows
            // because of a malformed cursor).
            _ => true,
        });
    } else {
        entries.retain(|(key, _, _)| key.as_str() > stop.as_str());
    }
}

/// Extract the input dimension name from a DimensionSpec.
fn dimension_spec_name(spec: &DimensionSpec) -> &str {
    match spec {
        DimensionSpec::Default { dimension, .. } => dimension,
        DimensionSpec::Extraction { dimension, .. } => dimension,
        DimensionSpec::ListFiltered { delegate, .. } => dimension_spec_name(delegate),
        DimensionSpec::RegexFiltered { delegate, .. } => dimension_spec_name(delegate),
        DimensionSpec::PrefixFiltered { delegate, .. } => dimension_spec_name(delegate),
    }
}

/// Extract the output column name from a DimensionSpec.
fn dimension_spec_output_name(spec: &DimensionSpec) -> &str {
    match spec {
        DimensionSpec::Default { output_name, .. } => output_name,
        DimensionSpec::Extraction { output_name, .. } => output_name,
        DimensionSpec::ListFiltered { delegate, .. } => dimension_spec_output_name(delegate),
        DimensionSpec::RegexFiltered { delegate, .. } => dimension_spec_output_name(delegate),
        DimensionSpec::PrefixFiltered { delegate, .. } => dimension_spec_output_name(delegate),
    }
}

// (Wave 40-B) `json_value_to_string` removed — typed `GroupKey` keys via
// `apply_dim_spec_typed` replace the previous string-coercion path.

// ---------------------------------------------------------------------------
// Wave 40-B regression tests (Wave 39 [High] [NEW-VARIANT] topn.rs:166-173)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_aggregator::AggregatorSpec;
    use ferrodruid_common::types::{ColumnType, DataSource};
    use ferrodruid_segment::Interval;
    use ferrodruid_segment::SegmentData;
    use ferrodruid_segment::column::ColumnData;

    /// TopN over a Double-typed dimension with multiple NaN feeds.  Pre-
    /// W40-B both NaN rows would be conflated with the literal string
    /// `"NaN"` bucket; the typed path uses [`ordered_float::OrderedFloat`]
    /// so all NaN representations hash to the same `GroupKey::Double(NaN)`,
    /// producing exactly 1 NaN bucket.
    #[test]
    fn topn_double_dimension_keys_handle_nan_consistently() {
        let mut columns = std::collections::HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![100, 200, 300, 400]),
        );
        // 4 rows: NaN, NaN, 1.0, 1.0  — typed path collapses to 2 buckets.
        columns.insert(
            "ratio".to_string(),
            ColumnData::Double(vec![f64::NAN, f64::NAN, 1.0, 1.0]),
        );
        let segment = SegmentData {
            version: 9,
            num_rows: 4,
            interval: Interval {
                start_millis: 0,
                end_millis: 1000,
            },
            dimensions: vec!["ratio".to_string()],
            metrics: vec![],
            columns,
            time_sorted: false,
        };

        let q = TopNQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec!["1970-01-01T00:00:00Z/1970-01-01T00:00:01Z".into()],
            granularity: GranularitySpec::Simple("all".into()),
            dimension: ferrodruid_common::types::DimensionSpec::Default {
                dimension: "ratio".into(),
                output_name: "ratio".into(),
                output_type: ColumnType::Double,
            },
            threshold: 10,
            metric: TopNMetricSpec::Numeric {
                metric: "cnt".into(),
            },
            filter: None,
            virtual_columns: None,
            aggregations: vec![AggregatorSpec::Count { name: "cnt".into() }],
            post_aggregations: None,
            context: None,
        };

        let results = q.execute(&segment).expect("execute");
        assert_eq!(results.len(), 1);
        let entries = &results[0].result;
        // We expect 2 distinct buckets: the NaN one (count 2) and the 1.0
        // one (count 2).  Note: column_value_at returns Null for NaN
        // because serde_json::Number::from_f64 rejects NaN — so the NaN
        // bucket's `ratio` field comes back as JSON null rather than a
        // numeric value.  The key invariant Wave 40-B asserts is that
        // both NaN rows landed in *one* bucket (count == 2), not two
        // distinct stringified buckets.
        assert_eq!(
            entries.len(),
            2,
            "NaN must collapse to a single typed bucket (Wave 40-B)"
        );
        let total_cnt: i64 = entries
            .iter()
            .map(|e| e.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0))
            .sum();
        assert_eq!(total_cnt, 4, "all 4 rows accounted for");
        let max_cnt = entries
            .iter()
            .map(|e| e.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0))
            .max()
            .unwrap_or(0);
        assert_eq!(
            max_cnt, 2,
            "each bucket must collect exactly 2 rows — NaN must hash consistently"
        );
    }

    /// DD R43 (Finding 7): a numeric TopN metric naming an aggregation that
    /// does not exist read `unwrap_or(0.0)` per row, so every row tied at 0.0
    /// and a wrong top-N was returned. The query must now be rejected before
    /// execution.
    #[test]
    fn topn_rejects_unknown_metric() {
        let mut columns = std::collections::HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![100, 200, 300]));
        columns.insert("code".to_string(), ColumnData::Long(vec![1, 2, 1]));
        let segment = SegmentData {
            version: 9,
            num_rows: 3,
            interval: Interval {
                start_millis: 0,
                end_millis: 1000,
            },
            dimensions: vec!["code".to_string()],
            metrics: vec![],
            columns,
            time_sorted: false,
        };
        let base = TopNQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec!["1970-01-01T00:00:00Z/1970-01-01T00:00:01Z".into()],
            granularity: GranularitySpec::Simple("all".into()),
            dimension: ferrodruid_common::types::DimensionSpec::Default {
                dimension: "code".into(),
                output_name: "code".into(),
                output_type: ColumnType::Long,
            },
            threshold: 10,
            metric: TopNMetricSpec::Numeric {
                metric: "does_not_exist".into(),
            },
            filter: None,
            virtual_columns: None,
            aggregations: vec![AggregatorSpec::Count { name: "cnt".into() }],
            post_aggregations: None,
            context: None,
        };
        let err = base
            .execute(&segment)
            .expect_err("unknown metric must reject");
        match err {
            DruidError::Query(msg) => assert!(msg.contains("does not name any"), "msg = {msg}"),
            other => panic!("expected DruidError::Query, got {other:?}"),
        }

        // The metric that names the real aggregation still executes; an
        // inverted wrapper around the real metric is also accepted.
        let mut ok = base.clone();
        ok.metric = TopNMetricSpec::Numeric {
            metric: "cnt".into(),
        };
        assert!(ok.execute(&segment).is_ok(), "valid metric must execute");
        let mut inv = base.clone();
        inv.metric = TopNMetricSpec::Inverted {
            metric: Box::new(TopNMetricSpec::Numeric {
                metric: "cnt".into(),
            }),
        };
        assert!(
            inv.execute(&segment).is_ok(),
            "inverted valid metric must execute"
        );
    }

    /// Wave 45-A regression for Wave 37B query Medium #1: dimension-ordered
    /// TopN with `previousStop` must skip entries strictly `<=` the cursor
    /// so page 2 of a paginated query does not repeat page 1's last value.
    ///
    /// Uses a `Long` dimension so the lex compare runs on stringified
    /// numerics (e.g. `"10"` < `"2"` lexicographically); with
    /// `previousStop="2"` the cursor sits between `"10"` and `"2"`/`"3"`
    /// /`"4"` so the surviving keys must be `>"2"` lexicographically.
    #[test]
    fn topn_dimension_previous_stop_lex_skips_prior_page() {
        let mut columns = std::collections::HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![100, 200, 300, 400, 500]),
        );
        // 5 distinct dim values: 1, 2, 3, 4, 10.  Lex order:
        // "1" < "10" < "2" < "3" < "4".  previousStop="2" must keep
        // "3" and "4" only.
        columns.insert("code".to_string(), ColumnData::Long(vec![1, 2, 3, 4, 10]));
        let segment = SegmentData {
            version: 9,
            num_rows: 5,
            interval: Interval {
                start_millis: 0,
                end_millis: 1000,
            },
            dimensions: vec!["code".to_string()],
            metrics: vec![],
            columns,
            time_sorted: false,
        };

        let q = TopNQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec!["1970-01-01T00:00:00Z/1970-01-01T00:00:01Z".into()],
            granularity: GranularitySpec::Simple("all".into()),
            dimension: ferrodruid_common::types::DimensionSpec::Default {
                dimension: "code".into(),
                output_name: "code".into(),
                output_type: ColumnType::Long,
            },
            threshold: 10,
            metric: TopNMetricSpec::Dimension {
                ordering: Some("lexicographic".into()),
                previous_stop: Some("2".into()),
            },
            filter: None,
            virtual_columns: None,
            aggregations: vec![AggregatorSpec::Count { name: "cnt".into() }],
            post_aggregations: None,
            context: None,
        };

        let results = q.execute(&segment).expect("execute");
        assert_eq!(results.len(), 1);
        let entries = &results[0].result;
        // Sort stability: emit the surviving sort keys (lex form) for the
        // assertion.  Pre-W45A the page contained all five keys.
        let codes: Vec<i64> = entries
            .iter()
            .filter_map(|m| m.get("code").and_then(|v| v.as_i64()))
            .collect();
        assert_eq!(
            codes,
            vec![3_i64, 4_i64],
            "lex `previousStop=\"2\"` must keep only \"3\" and \"4\" (Wave 45-A); pre-fix returned all 5 codes"
        );
    }

    /// Wave 45-A: numeric-ordered `previousStop` parses cursor as `f64`
    /// and keeps entries strictly greater.  Tolerates a malformed cursor
    /// by keeping all entries (safer than silently returning empty).
    #[test]
    fn topn_dimension_previous_stop_numeric_skips_lower_values() {
        let mut columns = std::collections::HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![100, 200, 300, 400]),
        );
        columns.insert("code".to_string(), ColumnData::Long(vec![1, 2, 3, 10]));
        let segment = SegmentData {
            version: 9,
            num_rows: 4,
            interval: Interval {
                start_millis: 0,
                end_millis: 1000,
            },
            dimensions: vec!["code".to_string()],
            metrics: vec![],
            columns,
            time_sorted: false,
        };

        let q = TopNQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec!["1970-01-01T00:00:00Z/1970-01-01T00:00:01Z".into()],
            granularity: GranularitySpec::Simple("all".into()),
            dimension: ferrodruid_common::types::DimensionSpec::Default {
                dimension: "code".into(),
                output_name: "code".into(),
                output_type: ColumnType::Long,
            },
            threshold: 10,
            metric: TopNMetricSpec::Dimension {
                ordering: Some("numeric".into()),
                previous_stop: Some("2".into()),
            },
            filter: None,
            virtual_columns: None,
            aggregations: vec![AggregatorSpec::Count { name: "cnt".into() }],
            post_aggregations: None,
            context: None,
        };

        let results = q.execute(&segment).expect("execute");
        let codes: Vec<i64> = results[0]
            .result
            .iter()
            .filter_map(|m| m.get("code").and_then(|v| v.as_i64()))
            .collect();
        // Numeric sort + previousStop=2.0 → keep [3, 10].  Pre-W45A
        // returned [1, 2, 3, 10].
        assert_eq!(
            codes,
            vec![3_i64, 10_i64],
            "numeric `previousStop=2` must keep only codes > 2.0 (Wave 45-A)"
        );
    }

    /// Wave 47-D §4: Druid accepts `"metric":"cnt"` as shorthand for
    /// `{"type":"numeric","metric":"cnt"}`.  FerroDruid now decodes both
    /// shapes; serialisation always emits the canonical tagged form.
    #[test]
    fn topn_metric_spec_accepts_string_shorthand() {
        let parsed: TopNMetricSpec = serde_json::from_str("\"cnt\"").expect("bare string");
        match parsed {
            TopNMetricSpec::Numeric { metric } => assert_eq!(metric, "cnt"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    /// Smoke test: a full TopN query JSON payload using string-form
    /// `dataSource` / `dimension` / `metric` deserialises identically
    /// to the verbose tagged form (Wave 47-D §2-4).
    #[test]
    fn topn_query_accepts_all_shorthands() {
        let shorthand = r#"{
            "queryType": "topN",
            "dataSource": "wikipedia",
            "intervals": ["2024-01-01T00:00:00Z/2024-01-04T00:00:00Z"],
            "granularity": "all",
            "dimension": "page",
            "metric": "cnt",
            "threshold": 5,
            "aggregations": [{"type":"count","name":"cnt"}]
        }"#;
        let q: TopNQuery = serde_json::from_str(shorthand).expect("shorthand parse");
        assert!(matches!(
            q.data_source,
            ferrodruid_common::types::DataSource::Table { .. }
        ));
        assert!(matches!(
            q.dimension,
            ferrodruid_common::types::DimensionSpec::Default { .. }
        ));
        assert!(matches!(q.metric, TopNMetricSpec::Numeric { .. }));
    }

    /// Wave 47-D §5: TopN with `granularity=all` must stamp the result
    /// bucket with the start of the requested interval, not the 1970
    /// epoch produced by `bucket_timestamp("all", _) = 0`.  Druid 30-36
    /// all return `timestamp == intervals[0].start`.
    #[test]
    fn topn_granularity_all_uses_interval_start_as_timestamp() {
        use ferrodruid_dict::FrontCodedDictionary;
        use ferrodruid_segment::column::StringColumnData;

        // Build a tiny segment with three rows in 2024-01-02.
        let mut columns = std::collections::HashMap::new();
        let base_ms = 1_704_153_600_000_i64; // 2024-01-02T00:00:00Z
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![base_ms, base_ms + 3_600_000, base_ms + 7_200_000]),
        );
        columns.insert(
            "page".to_string(),
            ColumnData::String(StringColumnData {
                dictionary: FrontCodedDictionary::from_sorted(vec!["a".into(), "b".into()]),
                encoded_values: vec![0, 1, 0],
                bitmap_indexes: vec![],
            }),
        );
        let segment = SegmentData {
            version: 9,
            num_rows: 3,
            interval: Interval {
                start_millis: base_ms,
                end_millis: base_ms + 86_400_000,
            },
            dimensions: vec!["page".into()],
            metrics: vec![],
            columns,
            time_sorted: false,
        };

        let q = TopNQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            // Wider interval than the data — interval start is what must
            // surface in the response timestamp, not the data's earliest
            // row time.
            intervals: vec!["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z".into()],
            granularity: GranularitySpec::Simple("all".into()),
            dimension: ferrodruid_common::types::DimensionSpec::Default {
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

        let results = q.execute(&segment).expect("execute");
        assert_eq!(results.len(), 1, "granularity=all yields a single bucket");
        assert_eq!(
            results[0].timestamp, "2024-01-01T00:00:00.000Z",
            "granularity=all must surface the interval start, not 1970-01-01"
        );
    }

    /// A virtual column must be usable as the TopN dimension and the
    /// metric aggregator must be able to read another virtual column as
    /// its `fieldName`.  `big = val > 10` over rows val = {5, 12, 18, 23}
    /// gives groups {false, true, true, true}.  `val2 = val * 2`, summed
    /// per `big`: false→10, true→(12+18+23)*2=106.  TopN by `s` ranks
    /// true (106) then false (10).
    #[test]
    fn topn_virtual_column_as_dimension_and_metric() {
        use ferrodruid_segment::SegmentDataBuilder;
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200, 300, 400])
            .add_double_column("val", true, vec![5.0, 12.0, 18.0, 23.0])
            .build()
            .expect("build segment");

        let q: TopNQuery = serde_json::from_str(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z"],
                "granularity": "all",
                "dimension": "big",
                "threshold": 10,
                "metric": {"type":"numeric","metric":"s"},
                "virtualColumns": [
                    {"type":"expression","name":"big","expression":"val > 10"},
                    {"type":"expression","name":"val2","expression":"val * 2"}
                ],
                "aggregations": [
                    {"type":"doubleSum","name":"s","fieldName":"val2"}
                ]
            }"#,
        )
        .expect("parse topN");
        let results = q.execute(&segment).expect("execute");
        assert_eq!(results.len(), 1);
        let entries = &results[0].result;
        // 2 distinct buckets (true / false).
        assert_eq!(entries.len(), 2);
        // Ranked descending by `s`: true (s=106) then false (s=10).
        let s_values: Vec<f64> = entries
            .iter()
            .filter_map(|m| m.get("s").and_then(serde_json::Value::as_f64))
            .collect();
        assert_eq!(s_values, vec![106.0, 10.0]);
        // The dimension value is the boolean virtual column.
        assert_eq!(entries[0].get("big"), Some(&serde_json::json!(true)));
        assert_eq!(entries[1].get("big"), Some(&serde_json::json!(false)));
    }
}
