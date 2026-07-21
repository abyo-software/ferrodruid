// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Timeseries query type — aggregate metrics over time buckets.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use ferrodruid_aggregator::{AggregatorSpec, PostAggregatorSpec};
use ferrodruid_common::error::Result;
use ferrodruid_common::types::DataSource;
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::ColumnData;

use crate::context::QueryContext;
use crate::filter::FilterSpec;
use crate::helpers::{
    GranularitySpec, bucket_timestamp, build_row_prealloc, build_row_update_only,
    compile_vectorized_filter, deserialize_intervals, ensure_aggregations_not_multi_value,
    feed_aggregator_row_from_map, finalize_agg_value, parse_intervals, pruned_row_range,
    substitute_cardinality_partials, validate_granularity,
};
use crate::virtual_columns::{VirtualColumnSpec, VirtualColumns};

// ---------------------------------------------------------------------------
// Query spec
// ---------------------------------------------------------------------------

/// A Druid timeseries query.
///
/// Timeseries queries compute aggregated metrics over time buckets for all rows
/// matching the filter within the given intervals.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeseriesQuery {
    /// The data source to query.
    pub data_source: DataSource,
    /// Time intervals to query over (ISO-8601 strings).
    ///
    /// Accepts both a single ISO `"start/end"` string and an array of
    /// such strings (TG-4-finding-001, W2-D pydruid/druid-go compat).
    #[serde(deserialize_with = "deserialize_intervals")]
    pub intervals: Vec<String>,
    /// Granularity for time bucketing.
    pub granularity: GranularitySpec,
    /// Optional filter to apply.
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
    /// Whether to return results in descending time order.
    #[serde(default)]
    pub descending: Option<bool>,
    /// Optional query context.
    #[serde(default)]
    pub context: Option<QueryContext>,
}

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

/// A single timeseries result entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesResult {
    /// The bucket timestamp (ISO-8601).
    pub timestamp: String,
    /// Aggregation results for this bucket.
    pub result: serde_json::Map<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

impl TimeseriesQuery {
    /// Execute this timeseries query against a segment.
    pub fn execute(&self, segment: &SegmentData) -> Result<Vec<TimeseriesResult>> {
        // Validate post-aggregators up front so unsupported variants error
        // cleanly rather than silently dropping the derived field.
        if let Some(post_aggs) = &self.post_aggregations {
            for pa in post_aggs {
                pa.validate_supported()?;
            }
        }
        // DD R40: reject a malformed expression filter up front instead of
        // letting it silently match every row (fail-open data exposure).
        if let Some(ref filter) = self.filter {
            filter.validate()?;
        }
        // DD R48: reject a duration granularity with periodMs == 0 (divide-by-
        // zero panic) or an out-of-i64 period up front.
        validate_granularity(&self.granularity)?;

        let virtual_columns = VirtualColumns::compile(&self.virtual_columns)?;
        // compat-11 MV fail-loud: an expression or aggregator over a
        // genuine multi-value (`StringMulti`) column has no element-wise
        // semantics yet — error once at plan time instead of silently
        // stringifying each row's array.
        virtual_columns.ensure_no_multi_value_refs(segment)?;
        ensure_aggregations_not_multi_value(segment, &self.aggregations, &virtual_columns)?;
        // compat-11 R2: reject non-element-aware filters over an MV column
        // at plan time (comprehensive guard — see
        // `FilterSpec::ensure_multi_value_supported`).
        if let Some(ref filter) = self.filter {
            filter.ensure_multi_value_supported(segment, &virtual_columns)?;
        }
        let intervals = parse_intervals(&self.intervals)?;
        let timestamps = segment.timestamp_column()?;

        // Vectorized + parallel fast path for the common single-bucket
        // (`granularity: all`) shape with typed sum/count aggregators and a
        // filter that compiles to a total typed predicate (typed-decidable
        // for every row). Mirrors the groupBy fast path: no per-row row-map
        // build, no boxed serde_json aggregator dispatch. Falls back to the
        // row loop below for any unsupported shape.
        if let Some(fast) = self.try_vectorized_all(segment, &virtual_columns, &intervals)? {
            return Ok(fast);
        }

        // Group rows into time buckets.
        // bucket_key -> Vec<aggregator instances>
        let mut buckets: HashMap<i64, Vec<Box<dyn ferrodruid_aggregator::Aggregator>>> =
            HashMap::new();

        // W3-SL1-B step 1: hoist row-map allocation out of the loop.
        let mut row = build_row_prealloc(segment);

        for (row_idx, &ts) in timestamps.iter().enumerate().take(segment.num_rows()) {
            // Check interval membership.
            if !intervals.is_empty()
                && !intervals
                    .iter()
                    .any(|(start, end)| ts >= *start && ts < *end)
            {
                continue;
            }

            // W3-SL1-B step 3 (Task #31): typed fast-path — try to
            // evaluate the filter against typed columns before
            // materialising the row map. Q6's `And { Bound(l_quantity,
            // numeric), Bound(l_discount, numeric) }` takes the fast
            // path; ~99 % of rows are rejected here without paying the
            // `build_row_update_only` cost. Fast path is skipped when
            // any virtual column exists (they need the row map to
            // materialise) or when `matches_typed` returns `None`
            // (unsupported filter variant) — in those cases we fall
            // through to the slow (row-map) path.
            let fast_rejected = self.filter.as_ref().is_some_and(|filter| {
                virtual_columns.is_empty()
                    && matches!(filter.matches_typed(segment, row_idx), Some(false))
            });
            if fast_rejected {
                continue;
            }

            // Row map is required from here on for aggregator field
            // access. Build it once, augment virtual columns, and — if
            // the fast path didn't already resolve the filter — re-check
            // the filter against the materialised row.
            build_row_update_only(segment, row_idx, &mut row);
            virtual_columns.augment_row(&mut row);
            if let Some(filter) = &self.filter {
                let fast_accepted = virtual_columns.is_empty()
                    && matches!(filter.matches_typed(segment, row_idx), Some(true));
                if !fast_accepted && !filter.matches(&row) {
                    continue;
                }
            }

            // Determine bucket key.
            let bucket_key = bucket_timestamp(ts, &self.granularity);

            // Get or create aggregators for this bucket.
            let aggs = buckets
                .entry(bucket_key)
                .or_insert_with(|| self.aggregations.iter().map(|spec| spec.create()).collect());

            // Feed values into aggregators (virtual columns resolved via `row`).
            for (i, spec) in self.aggregations.iter().enumerate() {
                feed_aggregator_row_from_map(segment, row_idx, &row, spec, aggs[i].as_mut());
            }
        }

        // Respect skipEmptyBuckets context parameter.
        let skip_empty = self
            .context
            .as_ref()
            .is_some_and(|ctx| ctx.skip_empty_buckets());

        // Wave 47-D §6: Druid's TIMESERIES emits a row for every gran-aligned
        // bucket between the first and last non-empty bucket within each
        // segment-granularity macro-block (with `null` aggregator values for
        // empty buckets). With `skipEmptyBuckets:true` Druid (and FerroDruid)
        // skip those empties; default `false` fills them.
        //
        // FerroDruid currently builds one segment per ingestion task rather
        // than chunking by `segmentGranularity`, so we approximate Druid's
        // per-segment fill by grouping non-empty buckets under the next-coarser
        // natural granularity (hour→day, minute→hour, ...) and filling each
        // group from its own min to max.  This exactly reproduces Druid's
        // 35-row hour-over-3-day shape for the harness and keeps day-grain
        // queries (each day is its own group) at parity.
        let fill_buckets: BTreeMap<i64, Option<&Vec<Box<dyn ferrodruid_aggregator::Aggregator>>>> =
            if skip_empty {
                buckets.iter().map(|(k, aggs)| (*k, Some(aggs))).collect()
            } else {
                expand_with_empty_buckets(&buckets, &self.granularity, &intervals)
            };

        let mut bucket_keys: Vec<i64> = fill_buckets.keys().copied().collect();
        if self.descending.unwrap_or(false) {
            bucket_keys.reverse();
        }

        let mut results = Vec::with_capacity(bucket_keys.len());
        for key in bucket_keys {
            let aggs_opt = fill_buckets.get(&key).copied().flatten();
            let mut result_map = serde_json::Map::new();
            for (i, spec) in self.aggregations.iter().enumerate() {
                // Fail-closed (2026-07-11): a saturated exact-cardinality
                // aggregator must error out here, never finalize to a
                // silently capped count (see `finalize_agg_value`).
                let value = match aggs_opt {
                    Some(aggs) => finalize_agg_value(aggs[i].as_ref())?,
                    None => empty_bucket_value(spec),
                };
                result_map.insert(spec.name().to_string(), value);
            }
            apply_post_aggs(&self.post_aggregations, &mut result_map);
            // Multi-shard exact union (2026-07-11): swap each exact-
            // cardinality output's bare count for its full-set envelope
            // AFTER post-aggregations were computed on the exact
            // per-segment count, so the broker can union across segments.
            // The broker's finalization pass collapses the envelope back
            // to a bare count (and re-applies post-aggregations) before
            // anything reaches a client.
            if let Some(aggs) = aggs_opt {
                substitute_cardinality_partials(&self.aggregations, aggs, &mut result_map);
            }
            results.push(TimeseriesResult {
                timestamp: format_epoch_millis(key),
                result: result_map,
            });
        }

        Ok(results)
    }

    /// Vectorized + parallel single-bucket (`granularity: all`) fast path.
    /// Handles typed `count`/`longSum`/`doubleSum` aggregators with no
    /// virtual columns and a filter that fully compiles to a typed predicate
    /// (typed-decidable for EVERY row — see `compile_vectorized_filter`).
    /// Returns `Ok(Some(results))` when it handled the query, `Ok(None)` to
    /// fall back to the row-oriented loop. Output matches that loop for
    /// accepted shapes.
    #[allow(clippy::too_many_lines)]
    fn try_vectorized_all(
        &self,
        segment: &SegmentData,
        virtual_columns: &VirtualColumns,
        intervals: &[(i64, i64)],
    ) -> Result<Option<Vec<TimeseriesResult>>> {
        if !virtual_columns.is_empty()
            || !matches!(&self.granularity, GranularitySpec::Simple(s) if s.eq_ignore_ascii_case("all"))
        {
            return Ok(None);
        }
        // W-B legacy null mode: the typed accumulators below (NaN-skip
        // double sums with a `seen`→null flag) implement ANSI semantics on
        // raw slices; legacy needs the coerced-default reads and the
        // 0-identity empty sum.  Legacy is a migration-compat mode, not a
        // perf mode — take the row-oriented loop, which reads through
        // `column_value_at` and the legacy-aware aggregators.  Perf twins
        // are a follow-on.
        if ferrodruid_common::legacy_null_mode() {
            return Ok(None);
        }

        enum AggPlan<'a> {
            Count,
            LongSum(&'a [i64]),
            DoubleSum(&'a [f64]),
        }
        let mut plans: Vec<AggPlan> = Vec::with_capacity(self.aggregations.len());
        for spec in &self.aggregations {
            let plan = match spec {
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
            };
            plans.push(plan);
        }

        let timestamps = segment.timestamp_column()?;
        let num_rows = segment.num_rows().min(timestamps.len());
        // The filter must be typed-decidable for EVERY row, which only the
        // structural whole-tree compile guarantees — see
        // `compile_vectorized_filter`. (A row-0 `matches_typed` probe is NOT
        // sound: `And`/`Or` short-circuit, so decidability is row-dependent
        // and a mid-scan undecidable row would be silently dropped.) The
        // compiled form doubles as the per-row predicate below: borrowed
        // column slices + pre-parsed bounds, no HashMap lookup or string
        // parse per row.
        let Some(compiled_filter) = compile_vectorized_filter(self.filter.as_ref(), segment) else {
            return Ok(None);
        };

        // Double sums carry a `seen` flag (any non-null contribution) so an
        // all-null input emits SQL null, matching Druid. Long sums need no
        // flag: `ColumnData::Long` has no in-band null (null-bearing long
        // input is stored as Double/NaN and falls off this fast path), so
        // every matched row contributes.
        #[derive(Clone, Copy)]
        enum Acc {
            Count(u64),
            Long(i64),
            Double(f64, bool),
        }
        let init: Vec<Acc> = plans
            .iter()
            .map(|p| match p {
                AggPlan::Count => Acc::Count(0),
                AggPlan::LongSum(_) => Acc::Long(0),
                AggPlan::DoubleSum(_) => Acc::Double(0.0, false),
            })
            .collect();

        // Time-interval pruning: on a sorted __time column, scan only the row
        // range inside the interval instead of all rows + a per-row check.
        let (scan_lo, scan_hi, check_interval) =
            match pruned_row_range(timestamps, intervals, segment.time_sorted) {
                Some((lo, hi)) => (lo, hi.min(num_rows), false),
                None => (0, num_rows, !intervals.is_empty()),
            };
        let scan_len = scan_hi.saturating_sub(scan_lo);

        let accumulate = |range: std::ops::Range<usize>| -> (Vec<Acc>, u64) {
            let mut acc = init.clone();
            let mut matched: u64 = 0;
            for row in range {
                if check_interval {
                    let ts = timestamps[row];
                    if !intervals.iter().any(|(s, e)| ts >= *s && ts < *e) {
                        continue;
                    }
                }
                // `compiled_filter` covers the WHOLE filter tree (the fast
                // path bailed out above otherwise), so `eval` is a total
                // per-row predicate — no `matches_typed` fallback that could
                // mistake an undecidable row for a non-match.
                if let Some(cf) = &compiled_filter
                    && !cf.eval(row)
                {
                    continue;
                }
                matched += 1;
                for (k, plan) in plans.iter().enumerate() {
                    match (plan, &mut acc[k]) {
                        (AggPlan::Count, Acc::Count(c)) => *c += 1,
                        (AggPlan::LongSum(col), Acc::Long(s)) => {
                            if let Some(&x) = col.get(row) {
                                *s = s.wrapping_add(x);
                            }
                        }
                        (AggPlan::DoubleSum(col), Acc::Double(s, seen)) => {
                            // NaN = SQL NULL (in-band marker): skip so a null
                            // never poisons the sum (Druid: SUM ignores
                            // nulls). A predictable branch — null-free
                            // columns take it 100%-predicted. The `seen`
                            // write inside the taken branch is a register
                            // store; it marks that ≥1 non-null contributed
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
            (acc, matched)
        };

        const PAR_MIN: usize = 250_000;
        let (acc, matched) = if scan_len >= PAR_MIN && rayon::current_num_threads() > 1 {
            use rayon::iter::{IntoParallelIterator, ParallelIterator};
            let n = rayon::current_num_threads().max(1);
            let chunk = scan_len.div_ceil(n * 4).max(1 << 14);
            let starts: Vec<usize> = (scan_lo..scan_hi).step_by(chunk).collect();
            starts
                .into_par_iter()
                .map(|start| accumulate(start..(start + chunk).min(scan_hi)))
                .reduce(
                    || (init.clone(), 0u64),
                    |mut a, b| {
                        for (av, bv) in a.0.iter_mut().zip(b.0.iter()) {
                            match (av, bv) {
                                (Acc::Count(x), Acc::Count(y)) => *x += y,
                                (Acc::Long(x), Acc::Long(y)) => *x = x.wrapping_add(*y),
                                (Acc::Double(x, sx), Acc::Double(y, sy)) => {
                                    *x += y;
                                    *sx |= sy;
                                }
                                _ => {}
                            }
                        }
                        (a.0, a.1 + b.1)
                    },
                )
        } else {
            accumulate(scan_lo..scan_hi)
        };

        // granularity "all": no matching rows -> empty result set (matches the
        // row-oriented path, which produces no bucket).
        if matched == 0 {
            return Ok(Some(Vec::new()));
        }

        let mut result_map = serde_json::Map::new();
        for (k, spec) in self.aggregations.iter().enumerate() {
            let value = match acc[k] {
                Acc::Count(c) => serde_json::Value::Number(serde_json::Number::from(c)),
                Acc::Long(s) => serde_json::Value::Number(serde_json::Number::from(s)),
                // No non-null contribution ⇒ SQL null (Druid: SUM over an
                // all-null input is null, not 0).
                Acc::Double(_, false) => serde_json::Value::Null,
                Acc::Double(s, true) => serde_json::to_value(s).unwrap_or(serde_json::Value::Null),
            };
            result_map.insert(spec.name().to_string(), value);
        }
        apply_post_aggs(&self.post_aggregations, &mut result_map);
        Ok(Some(vec![TimeseriesResult {
            timestamp: format_epoch_millis(0),
            result: result_map,
        }]))
    }
}

/// Default value reported for an aggregator that received no rows in a bucket.
///
/// Druid's TIMESERIES wire format emits `0` for `count` (the only aggregator
/// whose initial state is the meaningful empty answer) and `null` for every
/// other built-in aggregator on an empty bucket. `Filtered` defers to the
/// inner spec.
fn empty_bucket_value(spec: &AggregatorSpec) -> serde_json::Value {
    // W-B legacy null mode: an empty bucket carries every aggregator's
    // legacy NO-INPUT value — exactly what a fresh aggregator's `get()`
    // reports under the latch (SUM → 0, longMin/Max → the i64 sentinels,
    // doubleMin/Max → the ±Infinity strings; oracle native_ts_empty.json).
    // In ANSI mode `create().get()` would ALSO reproduce the mapping below
    // (count → 0, everything else → null), but the explicit match is kept
    // so the long-standing ANSI path stays byte-identical and
    // allocation-free.
    if ferrodruid_common::legacy_null_mode() {
        return spec.create().get();
    }
    match spec {
        AggregatorSpec::Count { .. } => serde_json::Value::Number(serde_json::Number::from(0)),
        AggregatorSpec::Filtered { aggregator, .. } => empty_bucket_value(aggregator),
        _ => serde_json::Value::Null,
    }
}

/// Expand the non-empty bucket set with empty-bucket placeholders to match
/// Druid's TIMESERIES emission rule (Wave 47-D §6).
///
/// For each non-empty bucket, find its containing macro-bucket (one
/// granularity step coarser than the query). Within every macro-bucket that
/// holds at least one non-empty entry, fill all gran-aligned buckets between
/// that group's min and max non-empty entry inclusive.  Macro-buckets with no
/// data are not filled (so a query interval ending well past the last event
/// does not produce a long trailing run of nulls — matching Druid).
///
/// Returns a sorted map keyed by bucket-start millis. The value is `Some(aggs)`
/// for non-empty buckets (so the caller can read aggregator state) and `None`
/// for synthetic empty buckets.
fn expand_with_empty_buckets<'a>(
    buckets: &'a HashMap<i64, Vec<Box<dyn ferrodruid_aggregator::Aggregator>>>,
    granularity: &GranularitySpec,
    intervals: &[(i64, i64)],
) -> BTreeMap<i64, Option<&'a Vec<Box<dyn ferrodruid_aggregator::Aggregator>>>> {
    let mut out: BTreeMap<i64, Option<&'a Vec<Box<dyn ferrodruid_aggregator::Aggregator>>>> =
        BTreeMap::new();
    for (k, aggs) in buckets {
        out.insert(*k, Some(aggs));
    }

    // No fill for `all` / `none` (single-bucket-or-per-row behaviour) or for
    // non-uniform calendar grans where there is no clean "next bucket" step.
    let Some(period_ms) = uniform_period_ms(granularity) else {
        // For `all`-style grans with no non-empty buckets, emit a single
        // synthetic bucket per interval so the response is shape-stable.
        if out.is_empty() && is_all_granularity(granularity) {
            for (start, _) in intervals {
                let bucket_key = bucket_timestamp(*start, granularity);
                out.entry(bucket_key).or_insert(None);
            }
        }
        return out;
    };

    if out.is_empty() {
        // No matching rows at all: emit a single empty bucket at each interval
        // start (preserves the previous behaviour callers relied on for
        // per-interval shape stability).
        for (start, _) in intervals {
            let bucket_key = bucket_timestamp(*start, granularity);
            out.entry(bucket_key).or_insert(None);
        }
        return out;
    }

    let macro_period_ms = next_coarser_period_ms(period_ms);
    let mut groups: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    for &k in buckets.keys() {
        let macro_key = floor_div(k, macro_period_ms) * macro_period_ms;
        let entry = groups.entry(macro_key).or_insert((k, k));
        if k < entry.0 {
            entry.0 = k;
        }
        if k > entry.1 {
            entry.1 = k;
        }
    }

    for (_macro_key, (min_k, max_k)) in groups {
        let mut k = min_k;
        while k <= max_k {
            out.entry(k).or_insert(None);
            k += period_ms;
        }
    }

    out
}

/// Floor division that handles negative dividends consistently (i.e. always
/// rounds toward `-∞`), unlike Rust's default `/` which truncates toward zero.
fn floor_div(a: i64, b: i64) -> i64 {
    let q = a / b;
    let r = a % b;
    if (r != 0) && ((r < 0) != (b < 0)) {
        q - 1
    } else {
        q
    }
}

/// Return the bucket period in milliseconds for uniform granularities, or
/// `None` for `all` / `none` / calendar-based (`month` / `quarter` / `year`)
/// granularities where empty-bucket fill is skipped.
fn uniform_period_ms(g: &GranularitySpec) -> Option<i64> {
    match g {
        GranularitySpec::Simple(s) => match s.to_lowercase().as_str() {
            "second" => Some(1_000),
            "minute" => Some(60_000),
            "five_minute" => Some(300_000),
            "ten_minute" => Some(600_000),
            "fifteen_minute" => Some(900_000),
            "thirty_minute" => Some(1_800_000),
            "hour" => Some(3_600_000),
            "six_hour" => Some(21_600_000),
            "day" => Some(86_400_000),
            "week" => Some(604_800_000),
            _ => None,
        },
        GranularitySpec::Full(g) => match g {
            ferrodruid_common::types::Granularity::Second => Some(1_000),
            ferrodruid_common::types::Granularity::Minute => Some(60_000),
            ferrodruid_common::types::Granularity::FiveMinute => Some(300_000),
            ferrodruid_common::types::Granularity::TenMinute => Some(600_000),
            ferrodruid_common::types::Granularity::FifteenMinute => Some(900_000),
            ferrodruid_common::types::Granularity::ThirtyMinute => Some(1_800_000),
            ferrodruid_common::types::Granularity::Hour => Some(3_600_000),
            ferrodruid_common::types::Granularity::SixHour => Some(21_600_000),
            ferrodruid_common::types::Granularity::Day => Some(86_400_000),
            ferrodruid_common::types::Granularity::Week => Some(604_800_000),
            ferrodruid_common::types::Granularity::Duration { period_ms, .. } => {
                i64::try_from(*period_ms).ok()
            }
            _ => None,
        },
    }
}

/// `true` when the granularity is `all` (a single bucket regardless of input).
fn is_all_granularity(g: &GranularitySpec) -> bool {
    match g {
        GranularitySpec::Simple(s) => matches!(s.to_lowercase().as_str(), "all"),
        GranularitySpec::Full(_) => false,
    }
}

/// Pick a sensible "macro" bucket size one step coarser than the query period.
/// This is the proxy for Druid's `segmentGranularity` — events are grouped
/// under their macro-bucket and only buckets within a macro-bucket that has
/// at least one event are filled.  Multipliers chosen to match the natural
/// ratios in Druid's gran ladder (sub-hour→hour, sub-day→day, sub-week→week,
/// week-or-larger→identity so the entire range becomes one group).
fn next_coarser_period_ms(period_ms: i64) -> i64 {
    const HOUR: i64 = 3_600_000;
    const DAY: i64 = 86_400_000;
    const WEEK: i64 = 604_800_000;
    if period_ms < HOUR {
        HOUR
    } else if period_ms < DAY {
        DAY
    } else if period_ms < WEEK {
        WEEK
    } else {
        period_ms
    }
}

/// Apply post-aggregators to a result map.
fn apply_post_aggs(
    post_aggs: &Option<Vec<PostAggregatorSpec>>,
    result_map: &mut serde_json::Map<String, serde_json::Value>,
) {
    if let Some(post_aggs) = post_aggs {
        let agg_results: HashMap<String, serde_json::Value> = result_map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
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
            result_map.insert(pa.name().to_string(), json_val);
        }
    }
}

/// Format epoch millis as ISO-8601 UTC string.
pub(crate) fn format_epoch_millis(millis: i64) -> String {
    use chrono::{DateTime, Utc};
    let dt = DateTime::<Utc>::from_timestamp_millis(millis);
    match dt {
        Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        None => format!("{millis}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_segment::SegmentDataBuilder;

    fn iso_millis(s: &str) -> i64 {
        chrono::DateTime::parse_from_rfc3339(s)
            .expect("parse")
            .timestamp_millis()
    }

    /// Build the §6 harness segment: 10 rows over `2024-01-01..2024-01-03T08`,
    /// matching the `wikipedia_compat` sample but trimmed to the columns the
    /// hour-grain `namespace=Main` query touches.  This is the segment shape
    /// the Druid v32/v36 diff harness exercises.
    fn build_harness_segment() -> ferrodruid_segment::SegmentData {
        let timestamps = vec![
            iso_millis("2024-01-01T00:00:00Z"),
            iso_millis("2024-01-01T01:00:00Z"),
            iso_millis("2024-01-01T02:00:00Z"),
            iso_millis("2024-01-01T03:00:00Z"),
            iso_millis("2024-01-01T12:00:00Z"),
            iso_millis("2024-01-02T00:00:00Z"),
            iso_millis("2024-01-02T06:00:00Z"),
            iso_millis("2024-01-02T12:00:00Z"),
            iso_millis("2024-01-03T00:00:00Z"),
            iso_millis("2024-01-03T08:00:00Z"),
        ];
        let added = vec![
            100.0_f64, 50.0, 200.0, 150.0, 75.0, 120.0, 300.0, 180.0, 90.0, 110.0,
        ];
        let namespaces = [
            "Main", "Talk", "Main", "Main", "Main", "Main", "Portal", "Main", "Main", "Main",
        ];
        SegmentDataBuilder::new()
            .add_timestamp_column(timestamps)
            .add_double_column("added", true, added)
            .add_string_column(
                "namespace",
                namespaces.iter().map(|s| (*s).to_string()).collect(),
            )
            .build()
            .expect("build segment")
    }

    fn parse_query(json: &str) -> TimeseriesQuery {
        serde_json::from_str(json).expect("parse timeseries query")
    }

    /// Wave 47-D §6: hour granularity over a 3-day interval with
    /// `skipEmptyBuckets` unset must emit Druid's per-day fill of 35 buckets
    /// — 13 + 13 + 9 — with `null` for the empty hour buckets.
    #[test]
    fn hour_granularity_emits_per_day_filled_empty_buckets() {
        let segment = build_harness_segment();
        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wikipedia_compat"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "hour",
                "filter": {"type":"selector","dimension":"namespace","value":"Main"},
                "aggregations": [{"type":"doubleSum","name":"total_added","fieldName":"added"}]
            }"#,
        );

        let results = query.execute(&segment).expect("execute");
        assert_eq!(
            results.len(),
            35,
            "expected 35 hour buckets (13 + 13 + 9 per Druid v32/v36), got {}: {:?}",
            results.len(),
            results.iter().map(|r| &r.timestamp).collect::<Vec<_>>()
        );

        // Spot-check the run starts and ends.
        assert_eq!(results[0].timestamp, "2024-01-01T00:00:00.000Z");
        assert_eq!(
            results[0].result.get("total_added"),
            Some(&serde_json::json!(100.0))
        );
        assert_eq!(results[34].timestamp, "2024-01-03T08:00:00.000Z");
        assert_eq!(
            results[34].result.get("total_added"),
            Some(&serde_json::json!(110.0))
        );

        // Empty bucket between T00 and T02 of day 1 must be `null`, matching
        // Druid's wire format for `doubleSum` on a no-data bucket.
        let empty_t04_d1 = results
            .iter()
            .find(|r| r.timestamp == "2024-01-01T04:00:00.000Z")
            .expect("T04 of day 1 emitted");
        assert_eq!(
            empty_t04_d1.result.get("total_added"),
            Some(&serde_json::Value::Null)
        );
    }

    /// `skipEmptyBuckets:true` must preserve the historical behaviour of
    /// emitting only buckets that received events (8 rows for the harness).
    #[test]
    fn skip_empty_buckets_true_preserves_compact_output() {
        let segment = build_harness_segment();
        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wikipedia_compat"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "hour",
                "filter": {"type":"selector","dimension":"namespace","value":"Main"},
                "aggregations": [{"type":"doubleSum","name":"total_added","fieldName":"added"}],
                "context": {"skipEmptyBuckets": true}
            }"#,
        );

        let results = query.execute(&segment).expect("execute");
        assert_eq!(results.len(), 8, "expected 8 non-empty hour buckets");
        for r in &results {
            assert!(
                !matches!(r.result.get("total_added"), Some(serde_json::Value::Null)),
                "skipEmptyBuckets=true must not emit null buckets, got {r:?}"
            );
        }
    }

    /// Day-grain queries already deep-matched Druid before §6 (every day in
    /// the harness has data); regression-guard the count aggregator at day
    /// gran so the new fill logic does not perturb the count=0 default.
    #[test]
    fn day_granularity_count_unaffected_by_fill_logic() {
        let segment = build_harness_segment();
        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wikipedia_compat"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "day",
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        );

        let results = query.execute(&segment).expect("execute");
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].timestamp, "2024-01-01T00:00:00.000Z");
        assert_eq!(results[0].result.get("cnt"), Some(&serde_json::json!(5)));
        assert_eq!(results[1].timestamp, "2024-01-02T00:00:00.000Z");
        assert_eq!(results[1].result.get("cnt"), Some(&serde_json::json!(3)));
        assert_eq!(results[2].timestamp, "2024-01-03T00:00:00.000Z");
        assert_eq!(results[2].result.get("cnt"), Some(&serde_json::json!(2)));
    }

    /// A fixed-period `duration` granularity (the native form Superset's
    /// PT5S / PT30S grains lower to) buckets on epoch-anchored 5-second
    /// boundaries. Rows at :00, :02, :04, :07 and :11 seconds must land in
    /// the [:00, :05) / [:05, :10) / [:10, :15) buckets.
    #[test]
    fn duration_granularity_five_second_buckets() {
        let timestamps = vec![
            iso_millis("2024-01-01T00:00:00Z"),
            iso_millis("2024-01-01T00:00:02Z"),
            iso_millis("2024-01-01T00:00:04Z"),
            iso_millis("2024-01-01T00:00:07Z"),
            iso_millis("2024-01-01T00:00:11Z"),
        ];
        let added = vec![1.0_f64, 2.0, 4.0, 8.0, 16.0];
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(timestamps)
            .add_double_column("added", true, added)
            .build()
            .expect("build segment");

        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-01T00:01:00.000Z"],
                "granularity": {"type":"duration","period_ms":5000,"origin":"1970-01-01T00:00:00Z"},
                "aggregations": [
                    {"type":"count","name":"cnt"},
                    {"type":"doubleSum","name":"total","fieldName":"added"}
                ],
                "context": {"skipEmptyBuckets": true}
            }"#,
        );

        let results = query.execute(&segment).expect("execute");
        assert_eq!(
            results.len(),
            3,
            "expected 3 non-empty 5s buckets, got {:?}",
            results.iter().map(|r| &r.timestamp).collect::<Vec<_>>()
        );
        assert_eq!(results[0].timestamp, "2024-01-01T00:00:00.000Z");
        assert_eq!(results[0].result.get("cnt"), Some(&serde_json::json!(3)));
        assert_eq!(
            results[0].result.get("total"),
            Some(&serde_json::json!(7.0))
        );
        assert_eq!(results[1].timestamp, "2024-01-01T00:00:05.000Z");
        assert_eq!(results[1].result.get("cnt"), Some(&serde_json::json!(1)));
        assert_eq!(
            results[1].result.get("total"),
            Some(&serde_json::json!(8.0))
        );
        assert_eq!(results[2].timestamp, "2024-01-01T00:00:10.000Z");
        assert_eq!(results[2].result.get("cnt"), Some(&serde_json::json!(1)));
        assert_eq!(
            results[2].result.get("total"),
            Some(&serde_json::json!(16.0))
        );
    }

    /// `count` aggregator on an empty bucket must report `0`, not `null` —
    /// matches Druid's wire format and is the only built-in aggregator with
    /// a meaningful zero default.
    #[test]
    fn count_aggregator_empty_bucket_reports_zero() {
        let segment = build_harness_segment();
        // Filter to a namespace that exists but only on day 1 + day 3, so the
        // hour fill range still runs end-to-end and we exercise both empty
        // and non-empty count buckets.
        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wikipedia_compat"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "hour",
                "filter": {"type":"selector","dimension":"namespace","value":"Main"},
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        );
        let results = query.execute(&segment).expect("execute");
        let empty = results
            .iter()
            .find(|r| r.timestamp == "2024-01-01T04:00:00.000Z")
            .expect("T04 of day 1 emitted");
        assert_eq!(
            empty.result.get("cnt"),
            Some(&serde_json::json!(0)),
            "count default for empty bucket must be 0, not null"
        );
    }

    /// `descending: true` must reverse the filled set, including empty
    /// buckets — first emitted row is the latest gran-aligned bucket.
    #[test]
    fn descending_reverses_filled_buckets() {
        let segment = build_harness_segment();
        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wikipedia_compat"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "hour",
                "filter": {"type":"selector","dimension":"namespace","value":"Main"},
                "aggregations": [{"type":"doubleSum","name":"total_added","fieldName":"added"}],
                "descending": true
            }"#,
        );
        let results = query.execute(&segment).expect("execute");
        assert_eq!(results.len(), 35);
        assert_eq!(results[0].timestamp, "2024-01-03T08:00:00.000Z");
        assert_eq!(results[34].timestamp, "2024-01-01T00:00:00.000Z");
    }

    /// A virtual column (`added * 2`) must be computed per row and be usable
    /// as an aggregator `fieldName`.  Sum over the harness `Main` rows of
    /// `added` is 100+200+150+75+120+180+90+110 = 1025, so `added2` sums to
    /// 2050.
    #[test]
    fn virtual_column_usable_as_aggregator_field() {
        let segment = build_harness_segment();
        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wikipedia_compat"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "all",
                "filter": {"type":"selector","dimension":"namespace","value":"Main"},
                "virtualColumns": [
                    {"type":"expression","name":"added2","expression":"added * 2"}
                ],
                "aggregations": [
                    {"type":"doubleSum","name":"sum_added2","fieldName":"added2"}
                ]
            }"#,
        );
        let results = query.execute(&segment).expect("execute");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].result.get("sum_added2"),
            Some(&serde_json::json!(2050.0))
        );
    }

    /// A virtual column must be usable in a `filter`: filter `added > 150`
    /// over Main rows keeps {200, 180} → doubleSum = 380.
    #[test]
    fn virtual_column_usable_in_filter() {
        let segment = build_harness_segment();
        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wikipedia_compat"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "all",
                "virtualColumns": [
                    {"type":"expression","name":"big","expression":"added > 150"}
                ],
                "filter": {"type":"and","fields":[
                    {"type":"selector","dimension":"namespace","value":"Main"},
                    {"type":"selector","dimension":"big","value":true}
                ]},
                "aggregations": [
                    {"type":"doubleSum","name":"sum_added","fieldName":"added"}
                ]
            }"#,
        );
        let results = query.execute(&segment).expect("execute");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].result.get("sum_added"),
            Some(&serde_json::json!(380.0))
        );
    }

    /// An unsupported post-aggregator must surface a clean error via
    /// `validate_supported()` rather than silently dropping the derived
    /// field.  `expression` is now evaluatable for the supported grammar
    /// subset, so a *parseable* expression executes fine and the fail-loud
    /// contract is checked with an expression using an unsupported function.
    #[test]
    fn unsupported_post_agg_errors_cleanly() {
        let segment = build_harness_segment();
        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wikipedia_compat"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [{"type":"count","name":"cnt"}],
                "postAggregations": [
                    {"type":"expression","name":"bad","expression":"concat(cnt, 1)"}
                ]
            }"#,
        );
        let err = query.execute(&segment).expect_err("must reject");
        match err {
            ferrodruid_common::error::DruidError::Query(msg) => {
                assert!(msg.contains("expression"), "{msg}")
            }
            other => panic!("expected Query error, got {other:?}"),
        }

        // A parseable expression over an aggregator output now executes and
        // produces the derived field.
        let ok_query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wikipedia_compat"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [{"type":"count","name":"cnt"}],
                "postAggregations": [
                    {"type":"expression","name":"cnt_plus_one","expression":"cnt + 1"}
                ]
            }"#,
        );
        let results = ok_query.execute(&segment).expect("execute");
        assert_eq!(results.len(), 1);
        let cnt = results[0]
            .result
            .get("cnt")
            .and_then(serde_json::Value::as_f64)
            .expect("cnt");
        assert_eq!(
            results[0].result.get("cnt_plus_one"),
            Some(&serde_json::json!(cnt + 1.0))
        );
    }

    /// Build the row-dependent-decidability repro segment: `price` (Double)
    /// = [5, 20, 30], `cat` (String, null-free) = [a, b, b], `__time` =
    /// [100, 200, 300].
    fn build_mixed_filter_segment() -> ferrodruid_segment::SegmentData {
        SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200, 300])
            .add_double_column("price", true, vec![5.0, 20.0, 30.0])
            .add_string_column("cat", vec!["a".into(), "b".into(), "b".into()])
            .build()
            .expect("build segment")
    }

    /// Regression (2026-07-19 High): `And`/`Or` short-circuiting makes
    /// `matches_typed` decidability ROW-DEPENDENT, so a row-0 probe must
    /// never admit a filter to the vectorized `granularity=all` fast path.
    ///
    /// Filter `And[Bound(price > 10, numeric), Selector(cat = b)]` over
    /// the repro segment: row 0 fails the Bound, so
    /// `matches_typed(segment, 0)` short-circuits to `Some(false)`
    /// WITHOUT visiting the (typed-undecidable) Selector.  The old row-0
    /// probe then certified the whole filter as typed-decidable, and the
    /// accumulate loop silently dropped rows 1-2 (both `None`-undecided),
    /// returning an EMPTY result.  The correct Druid answer is one bucket
    /// with cnt = 2 (rows (20, b) and (30, b)).
    #[test]
    fn vectorized_all_row_dependent_and_filter_falls_back_correctly() {
        let segment = build_mixed_filter_segment();
        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/1970-01-01T00:00:01.000Z"],
                "granularity": "all",
                "filter": {"type":"and","fields":[
                    {"type":"bound","dimension":"price","lower":"10","lowerStrict":true,"ordering":"numeric"},
                    {"type":"selector","dimension":"cat","value":"b"}
                ]},
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        );

        let results = query.execute(&segment).expect("execute");
        assert_eq!(
            results.len(),
            1,
            "one granularity=all bucket expected, got {results:?}"
        );
        assert_eq!(
            results[0].result.get("cnt"),
            Some(&serde_json::json!(2)),
            "price>10 AND cat=b matches exactly rows 1-2, got {results:?}"
        );
    }

    /// Or-dual of the row-dependent-decidability regression: row 0 PASSES
    /// the typed Bound clause, so `Or` short-circuits to `Some(true)` at
    /// row 0 without visiting the undecidable Selector; rows that fail
    /// the Bound but match the Selector were then silently dropped.
    ///
    /// `price = [20, 5, 30]`, `cat = [a, a, b]`, filter
    /// `Or[Bound(price > 10, numeric), Selector(cat = a)]`: every row
    /// matches (rows 0/2 via the Bound, row 1 via the Selector) → cnt = 3.
    #[test]
    fn vectorized_all_row_dependent_or_filter_falls_back_correctly() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200, 300])
            .add_double_column("price", true, vec![20.0, 5.0, 30.0])
            .add_string_column("cat", vec!["a".into(), "a".into(), "b".into()])
            .build()
            .expect("build segment");
        let query = parse_query(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/1970-01-01T00:00:01.000Z"],
                "granularity": "all",
                "filter": {"type":"or","fields":[
                    {"type":"bound","dimension":"price","lower":"10","lowerStrict":true,"ordering":"numeric"},
                    {"type":"selector","dimension":"cat","value":"a"}
                ]},
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        );

        let results = query.execute(&segment).expect("execute");
        assert_eq!(
            results.len(),
            1,
            "one granularity=all bucket expected, got {results:?}"
        );
        assert_eq!(
            results[0].result.get("cnt"),
            Some(&serde_json::json!(3)),
            "price>10 OR cat=a matches all 3 rows, got {results:?}"
        );
    }
}
