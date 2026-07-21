// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! GroupBy query type — group rows by one or more dimensions and aggregate.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use ferrodruid_aggregator::{AggregatorSpec, CardinalityState, PostAggregatorSpec};
use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_common::types::{ColumnType, DataSource, DimensionSpec};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::{ColumnData, is_null_double, is_null_long_row};
use ordered_float::OrderedFloat;

use crate::context::QueryContext;
use crate::dim_spec::{CompiledDimSpec, GroupKey};
use crate::filter::FilterSpec;
use crate::helpers::{
    GranularitySpec, bucket_timestamp, build_row_prealloc, build_row_update_only, column_value_at,
    deserialize_intervals, ensure_aggregations_not_multi_value,
    ensure_dimension_specs_not_multi_value_coerced, feed_aggregator_row,
    feed_aggregator_row_from_map, finalize_agg_value, is_sketch_envelope, numeric_agg_cell,
    parse_intervals, pruned_row_range, substitute_cardinality_partials, validate_granularity,
};
use crate::timeseries::format_epoch_millis;
use crate::virtual_columns::{VirtualColumnSpec, VirtualColumns};

/// Default in-flight key cap for GroupBy queries.  Mirrors
/// `QueryLimitsConfig::groupby_max_keys` and is used when the caller
/// invokes `execute` without an explicit limit.  Wave 36-G1 (Wave 37B
/// query Top-1 DoS).
pub const DEFAULT_GROUPBY_MAX_KEYS: usize = 1_000_000;

// ---------------------------------------------------------------------------
// HavingSpec
// ---------------------------------------------------------------------------

/// A having clause for filtering GroupBy results after aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum HavingSpec {
    /// Keep rows where the aggregation value is greater than the threshold.
    #[serde(rename = "greaterThan")]
    GreaterThan {
        /// Aggregation output name.
        aggregation: String,
        /// Threshold value.
        value: f64,
    },
    /// Keep rows where the aggregation value is less than the threshold.
    #[serde(rename = "lessThan")]
    LessThan {
        /// Aggregation output name.
        aggregation: String,
        /// Threshold value.
        value: f64,
    },
    /// Keep rows where the aggregation value equals the threshold.
    #[serde(rename = "equalTo")]
    EqualTo {
        /// Aggregation output name.
        aggregation: String,
        /// Threshold value.
        value: f64,
    },
    /// Logical AND of multiple having clauses.
    #[serde(rename = "and")]
    And {
        /// Child having specs.  Druid spells the wire field `havingSpecs`
        /// (the enum-level `rename_all` only touches variant names, so the
        /// field silently stayed snake_case pre-fix and every composite
        /// HAVING from a real Druid client failed to parse); the old
        /// snake_case spelling stays accepted as an alias.
        #[serde(rename = "havingSpecs", alias = "having_specs")]
        having_specs: Vec<HavingSpec>,
    },
    /// Logical OR of multiple having clauses.
    #[serde(rename = "or")]
    Or {
        /// Child having specs (wire: `havingSpecs` — see [`Self::And`]).
        #[serde(rename = "havingSpecs", alias = "having_specs")]
        having_specs: Vec<HavingSpec>,
    },
    /// Logical NOT of a having clause.
    #[serde(rename = "not")]
    Not {
        /// The having spec to negate (wire: `havingSpec` — see
        /// [`Self::And`]).
        #[serde(rename = "havingSpec", alias = "having_spec")]
        having_spec: Box<HavingSpec>,
    },
}

impl HavingSpec {
    /// Evaluate this having spec against a result row.
    ///
    /// A referenced SKETCH aggregation (decoded-Druid hyperUnique, theta,
    /// HLL) sits in the row as its partial-state ENVELOPE — a JSON object,
    /// not a number — so every comparison resolves the cell through
    /// [`numeric_agg_cell`], which reads the envelope's `estimate` (the
    /// same extraction topN ranking and limitSpec ordering use).  Pre-fix,
    /// the raw `as_f64()` returned `None` for every envelope and a
    /// `HAVING <sketch agg> > threshold` silently removed EVERY group.  A
    /// cell that is neither a number nor an estimate-bearing envelope
    /// (JSON null, a string, a mix-error envelope) still fails the
    /// comparison, as it always did.
    pub fn matches(&self, row: &serde_json::Map<String, serde_json::Value>) -> bool {
        match self {
            Self::GreaterThan { aggregation, value } => row
                .get(aggregation)
                .and_then(numeric_agg_cell)
                .is_some_and(|v| v > *value),
            Self::LessThan { aggregation, value } => row
                .get(aggregation)
                .and_then(numeric_agg_cell)
                .is_some_and(|v| v < *value),
            Self::EqualTo { aggregation, value } => row
                .get(aggregation)
                .and_then(numeric_agg_cell)
                .is_some_and(|v| (v - *value).abs() < f64::EPSILON),
            Self::And { having_specs } => having_specs.iter().all(|h| h.matches(row)),
            Self::Or { having_specs } => having_specs.iter().any(|h| h.matches(row)),
            Self::Not { having_spec } => !having_spec.matches(row),
        }
    }
}

// ---------------------------------------------------------------------------
// LimitSpec
// ---------------------------------------------------------------------------

/// Specifies ordering and row limits on GroupBy results.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitSpec {
    /// Limit type (usually `"default"`).
    #[serde(rename = "type")]
    pub typ: String,
    /// Maximum number of result rows.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Column ordering specifications.
    #[serde(default)]
    pub columns: Option<Vec<OrderByColumnSpec>>,
}

/// A column ordering specification within a LimitSpec.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderByColumnSpec {
    /// Column name to order by.
    pub dimension: String,
    /// Direction: `"ascending"` or `"descending"`.
    #[serde(default)]
    pub direction: Option<String>,
    /// Ordering type: `"lexicographic"`, `"numeric"`, etc.
    #[serde(default, rename = "dimensionOrder")]
    pub dimension_order: Option<String>,
}

/// Sort GroupBy result rows by a LimitSpec's ordering columns.
///
/// This is THE limitSpec comparator: per-segment execution
/// ([`GroupByQuery::execute`]) and the broker's multi-segment merge
/// (W-C S3) both call it, so the merged group order can never diverge
/// from the single-segment order.  Each column sort is stable and the
/// columns are applied in reverse declaration order, so the final order
/// is lexicographic over the declared columns with pre-sort order as
/// the last tiebreak.
///
/// Semantics preserved from the per-segment path:
///
/// * **Exact-cardinality envelopes** (multi-shard exact union,
///   2026-07-11): outputs may be envelope-shaped
///   [`CardinalityState`] partials at this point (per-segment: substituted
///   after HAVING; broker: already collapsed to bare counts by the
///   finalize pass, in which case the probe is a no-op), so ordering on
///   such a column reads the envelope's exact count, not the envelope
///   object (which would sort as 0 / stringify as JSON).  The unchecked
///   probe is clone-free and O(1); peer envelopes are validated at the
///   merge/finalize trust boundaries instead.
/// * **W1-J finding-C**: Druid/Calcite default ordering is NULLS FIRST
///   in ASC (and therefore NULLS LAST in DESC).  Without an explicit
///   null branch the lex `to_string()` fallback stringifies `null` to
///   `"null"`, which sorted GROUPING SETS / CUBE / ROLLUP null-groups
///   after real values whose first byte is less than `'n'` — diverging
///   from Druid.  A missing key (`None`) is treated the same as
///   `Value::Null` because subtotal rows store omitted dimensions as
///   JSON null but may also omit the key entirely.
/// * **W-A sketch cells**: a SKETCH aggregation cell (hyperUnique /
///   theta / HLL) is its partial-state envelope here, so ordering on
///   such a column compares the envelope's `estimate` (via
///   [`numeric_agg_cell`] — the same extraction topN ranking and HAVING
///   use), regardless of `dimensionOrder`.  Pre-fix, the numeric branch
///   ranked every envelope as 0.0 and the lex fallback stringified the
///   envelope JSON — both made `ORDER BY <sketch agg> … LIMIT k` select
///   the WRONG groups.  A mix-error envelope (no estimate) ranks 0.0,
///   exactly as topN ranks it.
/// * **Wave 40-B**: typed dimensions emit their JSON form (Number /
///   Bool) rather than String, so a lex sort falls through to
///   `to_string()` rather than only honouring `as_str()`.
pub fn sort_by_order_columns(results: &mut [GroupByResult], columns: &[OrderByColumnSpec]) {
    for col_spec in columns.iter().rev() {
        let desc = col_spec
            .direction
            .as_deref()
            .is_some_and(|d| d == "descending");
        let numeric = col_spec
            .dimension_order
            .as_deref()
            .is_some_and(|o| o == "numeric");
        let name = col_spec.dimension.clone();
        results.sort_by(|a, b| {
            let va = a.event.get(&name);
            let vb = b.event.get(&name);
            // Exact-cardinality envelope probe (see doc comment).
            let ca = va
                .and_then(CardinalityState::peek_json_unchecked)
                .map(|(_, c)| serde_json::Value::from(c));
            let cb = vb
                .and_then(CardinalityState::peek_json_unchecked)
                .map(|(_, c)| serde_json::Value::from(c));
            let va = ca.as_ref().or(va);
            let vb = cb.as_ref().or(vb);
            // W1-J finding-C: NULLS FIRST in ASC / LAST in DESC (see doc
            // comment).
            let na = va.is_none_or(serde_json::Value::is_null);
            let nb = vb.is_none_or(serde_json::Value::is_null);
            let ord = match (na, nb) {
                (true, true) => std::cmp::Ordering::Equal,
                // null < non-null in ASC ⇒ matches Druid NULLS FIRST.
                // The trailing `if desc { reverse }` flips this so DESC
                // puts nulls last as Druid does.
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                (false, false) => {
                    // W-A sketch envelope ranking (see doc comment).
                    let sketch =
                        va.is_some_and(is_sketch_envelope) || vb.is_some_and(is_sketch_envelope);
                    if numeric || sketch {
                        let fa = va.and_then(numeric_agg_cell).unwrap_or(0.0);
                        let fb = vb.and_then(numeric_agg_cell).unwrap_or(0.0);
                        fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
                    } else {
                        // Wave 40-B typed-dimension lex fallback (see doc
                        // comment).  Nulls were short-circuited above.
                        let sa = va
                            .and_then(|v| v.as_str().map(str::to_owned))
                            .or_else(|| va.map(serde_json::Value::to_string))
                            .unwrap_or_default();
                        let sb = vb
                            .and_then(|v| v.as_str().map(str::to_owned))
                            .or_else(|| vb.map(serde_json::Value::to_string))
                            .unwrap_or_default();
                        sa.cmp(&sb)
                    }
                }
            };
            if desc { ord.reverse() } else { ord }
        });
    }
}

// ---------------------------------------------------------------------------
// Query spec
// ---------------------------------------------------------------------------

/// A Druid GroupBy query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupByQuery {
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
    /// Dimensions to group by.
    pub dimensions: Vec<DimensionSpec>,
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
    /// Optional subtotals specification.
    ///
    /// Each entry is a subset of the query's output dimension names; the
    /// query produces one hierarchical result set per entry, with the
    /// omitted dimensions nulled.  `[]` (the empty subset) is the grand
    /// total over all rows.
    #[serde(default, rename = "subtotalsSpec")]
    pub subtotals_spec: Option<Vec<Vec<String>>>,
    /// Optional having clause.
    #[serde(default)]
    pub having: Option<HavingSpec>,
    /// Optional limit/order specification.
    #[serde(default)]
    pub limit_spec: Option<LimitSpec>,
    /// Optional query context.
    #[serde(default)]
    pub context: Option<QueryContext>,
}

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

/// A single GroupBy result row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupByResult {
    /// Version string (always `"v1"`).
    pub version: String,
    /// The bucket timestamp.
    pub timestamp: String,
    /// The event data (dimension values + aggregation results).
    pub event: serde_json::Map<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

impl GroupByQuery {
    /// Execute this GroupBy query against a segment using the default
    /// per-key cap (`DEFAULT_GROUPBY_MAX_KEYS`).
    ///
    /// Use [`Self::execute_with_limit`] to override the cap from a
    /// `QueryLimitsConfig`.
    pub fn execute(&self, segment: &SegmentData) -> Result<Vec<GroupByResult>> {
        self.execute_with_limit(segment, DEFAULT_GROUPBY_MAX_KEYS)
    }

    /// Execute this GroupBy query with an explicit per-key cap.
    ///
    /// `max_keys` bounds the total number of distinct group keys
    /// (timestamp + dimension tuple) retained in memory.  Pass `0` to
    /// disable.  When the guard fires the query returns
    /// [`DruidError::ResourceLimit`] (REST: `429 Too Many Keys`).
    ///
    /// Wave 36-G1 closes Wave 37B query Top-1 (DoS) and Wave 37B query
    /// High #3 (extraction / list-filtered / regex-filtered /
    /// prefix-filtered DimensionSpec wrappers were silently dropped from
    /// the group key construction).
    pub fn execute_with_limit(
        &self,
        segment: &SegmentData,
        max_keys: usize,
    ) -> Result<Vec<GroupByResult>> {
        // Validate post-aggregators up front so unsupported variants error
        // cleanly rather than silently dropping the derived field.
        if let Some(ref post_aggs) = self.post_aggregations {
            for pa in post_aggs {
                pa.validate_supported()?;
            }
        }

        // Wave 45-F: compile every regex inside every dimension spec
        // exactly once at plan time.  Malformed patterns surface here as
        // [`DruidError::Query`] instead of becoming silent per-row no-
        // matches in the loop below.
        let compiled_dims: Vec<CompiledDimSpec> = self
            .dimensions
            .iter()
            .map(CompiledDimSpec::new)
            .collect::<Result<Vec<_>>>()?;

        let virtual_columns = VirtualColumns::compile(&self.virtual_columns)?;
        // compat-11 MV fail-loud: an expression or aggregator over a
        // genuine multi-value (`StringMulti`) column has no element-wise
        // semantics yet — error once at plan time instead of silently
        // stringifying each row's array.  (GROUPING on an MV column stays
        // fully working: it explodes, oracle-verified.)
        virtual_columns.ensure_no_multi_value_refs(segment)?;
        ensure_aggregations_not_multi_value(segment, &self.aggregations, &virtual_columns)?;
        // compat-11 R3: a grouping DimensionSpec over an MV column with a
        // non-STRING outputType or an extractionFn silently diverges from
        // Druid's per-element coercion — reject at plan time (the plain-
        // STRING explosion stays untouched; see
        // `ensure_dimension_specs_not_multi_value_coerced`).
        ensure_dimension_specs_not_multi_value_coerced(
            segment,
            &self.dimensions,
            &virtual_columns,
        )?;
        // compat-11 R2: reject non-element-aware filters over an MV column
        // at plan time (comprehensive guard — see
        // `FilterSpec::ensure_multi_value_supported`).
        if let Some(ref filter) = self.filter {
            filter.ensure_multi_value_supported(segment, &virtual_columns)?;
        }

        // Determine the set of aggregation "groupings".  Without a
        // `subtotalsSpec`, there is exactly one grouping over every
        // dimension.  With a `subtotalsSpec`, each entry is a subset of the
        // output dimension names and produces its own hierarchical result
        // set (omitted dimensions nulled).
        let groupings: Vec<Vec<usize>> = match &self.subtotals_spec {
            None => vec![(0..self.dimensions.len()).collect()],
            Some(subsets) => {
                let mut out = Vec::with_capacity(subsets.len());
                for subset in subsets {
                    out.push(self.resolve_subtotal_indices(subset)?);
                }
                out
            }
        };

        let mut results: Vec<GroupByResult> = Vec::new();
        for active in &groupings {
            self.aggregate_active_dims(
                segment,
                &compiled_dims,
                &virtual_columns,
                active,
                max_keys,
                &mut results,
            )?;
        }

        // Sort by timestamp, then dimension values.
        results.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        // Apply limit spec (the ordering comparator is shared with the
        // broker merge — W-C S3 — so per-segment and merged ordering can
        // never diverge).
        if let Some(ref limit_spec) = self.limit_spec {
            if let Some(ref columns) = limit_spec.columns {
                sort_by_order_columns(&mut results, columns);
            }
            if let Some(limit) = limit_spec.limit {
                results.truncate(limit);
            }
        }

        Ok(results)
    }

    /// Resolve a `subtotalsSpec` subset (a list of output dimension names)
    /// to the indices of the matching entries in `self.dimensions`,
    /// preserving the order in which the dimensions are declared on the
    /// query (matching Druid's subtotal semantics).
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Query`] when a subset names a dimension that is
    /// not one of the query's output dimensions.
    fn resolve_subtotal_indices(&self, subset: &[String]) -> Result<Vec<usize>> {
        let mut indices = Vec::with_capacity(subset.len());
        for (idx, dim_spec) in self.dimensions.iter().enumerate() {
            if subset
                .iter()
                .any(|name| name == dim_spec_output_name(dim_spec))
            {
                indices.push(idx);
            }
        }
        // Every name in the subset must resolve to a declared dimension.
        for name in subset {
            if !self
                .dimensions
                .iter()
                .any(|d| dim_spec_output_name(d) == name)
            {
                return Err(DruidError::Query(format!(
                    "subtotalsSpec references unknown dimension '{name}'"
                )));
            }
        }
        Ok(indices)
    }

    /// Aggregate the segment grouping only by the dimensions whose indices
    /// appear in `active`, appending one [`GroupByResult`] per group to
    /// `results`.  Dimensions not in `active` are emitted as JSON `null`
    /// (the Druid subtotal convention).  When `active` is every dimension
    /// index this reproduces the plain (non-subtotal) grouping.
    #[allow(clippy::too_many_arguments)]
    fn aggregate_active_dims(
        &self,
        segment: &SegmentData,
        compiled_dims: &[CompiledDimSpec],
        virtual_columns: &VirtualColumns,
        active: &[usize],
        max_keys: usize,
        results: &mut Vec<GroupByResult>,
    ) -> Result<()> {
        // DD R40: reject a malformed expression filter up front instead of
        // letting it silently match every row (fail-open data exposure).
        if let Some(ref filter) = self.filter {
            filter.validate()?;
        }
        // DD R48: reject a duration granularity with periodMs == 0 / out-of-i64.
        validate_granularity(&self.granularity)?;

        let intervals = parse_intervals(&self.intervals)?;

        // Vectorized columnar fast path (the W3-SL1-C "dict-ID + columnar-agg"
        // follow-up): for `Default` dims over dictionary/long columns with
        // count/longSum/doubleSum aggregators and no filter/virtual columns,
        // this replaces the per-row `String` group key and the per-row boxed
        // `serde_json::Value` aggregator dispatch with integer dict-code keys
        // and monomorphized accumulation over typed column slices. Returns
        // `Ok(true)` when it handled the query; falls back to the row loop
        // below otherwise.
        if self.try_vectorized_active_dims(
            segment,
            virtual_columns,
            active,
            &intervals,
            max_keys,
            results,
        )? {
            return Ok(());
        }

        let timestamps = segment.timestamp_column()?;

        // W3-SL1-D step (Task #34): encode the composite key as a
        // single `Vec<GroupKey>` whose first element is
        // `GroupKey::Long(bucket_key)` and the rest are the active
        // dim keys.  This unlocks a `HashMap::get_mut(&[GroupKey])`
        // borrow-based lookup on hit — since `Vec<T>: Borrow<[T]>`
        // — so the per-row `dim_key` buffer is reused (clear + push)
        // instead of allocated per row (was `Vec::with_capacity(n)`
        // + drop on every one of the ~6 M SF1 / 60 M SF10 Q1 rows).
        // Only the ~24 unique-key misses pay the `.clone()` cost.
        // The pre-fix tuple key `(i64, Vec<GroupKey>)` had no
        // `Borrow<(i64, &[GroupKey])>` in std, which forced consuming
        // `entry(...)` and therefore a per-row alloc.
        type CompositeKey = Vec<GroupKey>;
        type AggVec = Vec<Box<dyn ferrodruid_aggregator::Aggregator>>;
        let mut groups: HashMap<CompositeKey, AggVec> = HashMap::new();

        // W3-SL1-B step 1: hoist the row-map allocation out of the hot
        // loop and reuse it every iteration via `build_row_prealloc, build_row_update_only`. This
        // kills one HashMap header alloc + drop per row (~60 M for
        // SF10 Q1 — see W2-C-perf-Q1 findings).
        let mut row = build_row_prealloc(segment);

        // W3-SL1-B step 2.5 (Task #30): reusable `Value::Null` sentinel
        // so `row.get(name).unwrap_or(&NULL)` skips the per-dim JSON
        // `Value::clone()` from the pre-fix `.cloned().unwrap_or(...)`
        // path (that clone was a heap allocation per string dimension
        // per row on the Q1 hot path). The `dim_key` `Vec<GroupKey>`
        // stays inside the loop because moving it into the composite
        // HashMap key ties its lifetime to per-row insertion; a
        // hoist-with-clone experiment saved 0 % net because the
        // per-row `dim_key.clone()` cost matched the `Value::clone()`
        // savings. Real dim-key allocation elimination is a W3-SL1-C
        // dict-ID key refactor, filed as a follow-up.
        let null_val = serde_json::Value::Null;

        // W3-SL1-B step 4 (Task #32): row-map-free fast path for the
        // common case (no filter + no virtual columns). Skips the
        // per-row `build_row_update_only` entirely and reads dim values
        // + aggregator field values from `segment.columns` directly via
        // `column_value_at` + `feed_aggregator_row`. Q1 (TPC-H Pricing
        // Summary, no filter, no virtual columns) enters this path and
        // saves 13 of the 15 per-row column reads plus 15 per-row
        // HashMap slot updates that the slow path pays for every row.
        let row_map_free = self.filter.is_none() && virtual_columns.is_empty();

        // W3-SL1-D step (Task #34): reusable composite-key buffer.
        // Sized `active.len() + 1` because index 0 is the encoded
        // `GroupKey::Long(bucket_key)` and indices 1.. are the dim
        // keys.  We reuse this Vec every row via `.clear()` +
        // `.push(...)` and only `.clone()` it on hashmap miss (i.e.
        // ~24 times for Q1 SF10 vs ~60 M per-row allocs pre-fix).
        let composite_len = active.len() + 1;
        let mut dim_key: Vec<GroupKey> = Vec::with_capacity(composite_len);

        // compat-11: a grouping dimension backed by a MULTI-VALUE string
        // column EXPLODES — the row contributes one group key per element
        // (Druid semantics).  Such groupings take the dedicated row-map
        // explode path below and SKIP the single-value loop entirely
        // (`scan_rows = 0`); single-value groupings keep every unchanged
        // fast path.  Detection over-approximates safely: a virtual column
        // shadowing the name still resolves through the row map inside the
        // explode path, which handles scalar values identically.
        let mv_grouping = active.iter().any(|&idx| {
            matches!(
                segment
                    .columns
                    .get(dim_spec_input_name(&self.dimensions[idx])),
                Some(ColumnData::StringMulti(_))
            )
        });
        if mv_grouping {
            self.accumulate_mv_rows(
                segment,
                compiled_dims,
                virtual_columns,
                active,
                &intervals,
                max_keys,
                &mut groups,
            )?;
        }
        let scan_rows = if mv_grouping { 0 } else { segment.num_rows() };

        'rows: for (row_idx, &ts) in timestamps.iter().enumerate().take(scan_rows) {
            if !intervals.is_empty()
                && !intervals
                    .iter()
                    .any(|(start, end)| ts >= *start && ts < *end)
            {
                continue;
            }

            let bucket_key = bucket_timestamp(ts, &self.granularity);
            if row_map_free {
                // W3-SL1-B step 4 fast path: no filter + no virtual
                // columns → skip build_row_update_only + go directly
                // to segment for both dim keys and aggregator fields.
                //
                // W3-SL1-C (Task #33) sub-step: for `DimensionSpec::Default`
                // dims (i.e. no extraction / filter wrapper — the plain
                // Q1 case), skip the intermediate `Value::String`
                // construction and go directly from the typed column to
                // `GroupKey`. Halves the string allocations per row
                // (one per string dim per row instead of two).
                dim_key.clear();
                dim_key.push(GroupKey::Long(bucket_key));
                for &idx in active {
                    let dim_spec = &self.dimensions[idx];
                    let compiled = &compiled_dims[idx];
                    let name = dim_spec_input_name(dim_spec);
                    let group_key = if let DimensionSpec::Default { output_type, .. } = dim_spec {
                        // Re-audit Medium (2026-07-19): a Default spec's
                        // outputType coerces the key per value BEFORE
                        // grouping (Druid convertObjectTo{Long,Float,
                        // Double}) so e.g. "01"/"1" merge into the numeric
                        // group 1.  STRING (the wire default) is the
                        // identity, so the plain path pays only a
                        // non-allocating enum match per dim per row.
                        let raw = segment.columns.get(name).map_or(GroupKey::Null, |col| {
                            direct_column_to_group_key(col, row_idx)
                        });
                        crate::dim_spec::coerce_group_key_to_output_type(raw, output_type)
                    } else {
                        // Extraction / filter wrapper: keep the Value
                        // round-trip so `apply_typed` gets its expected
                        // shape.
                        let val_owned = segment
                            .columns
                            .get(name)
                            .map(|col| column_value_at(col, row_idx));
                        let val_ref = val_owned.as_ref().unwrap_or(&null_val);
                        let Some(transformed) = compiled.apply_typed(val_ref) else {
                            continue 'rows;
                        };
                        transformed
                    };
                    dim_key.push(group_key);
                }

                // W3-SL1-D borrow-based lookup: on hit, reuse the
                // buffer; on miss, `.clone()` for the owned key.
                if let Some(aggs) = groups.get_mut(dim_key.as_slice()) {
                    for (i, spec) in self.aggregations.iter().enumerate() {
                        feed_aggregator_row(segment, row_idx, spec, aggs[i].as_mut());
                    }
                } else {
                    if max_keys > 0 && groups.len() >= max_keys {
                        return Err(DruidError::ResourceLimit {
                            kind: "groupBy.maxResults",
                            limit: max_keys,
                            observed: groups.len() + 1,
                        });
                    }
                    let owned_key = dim_key.clone();
                    let aggs = groups.entry(owned_key).or_insert_with(|| {
                        self.aggregations.iter().map(|spec| spec.create()).collect()
                    });
                    for (i, spec) in self.aggregations.iter().enumerate() {
                        feed_aggregator_row(segment, row_idx, spec, aggs[i].as_mut());
                    }
                }
                continue;
            }

            // W3-SL1-B step 3 (Task #31): typed fast-path — see
            // `timeseries.rs` for the design note.
            let fast_rejected = self.filter.as_ref().is_some_and(|filter| {
                virtual_columns.is_empty()
                    && matches!(filter.matches_typed(segment, row_idx), Some(false))
            });
            if fast_rejected {
                continue;
            }

            // Build the row map and materialise virtual columns so they are
            // visible to the filter, to dimension resolution, and to
            // aggregator `fieldName` access.
            build_row_update_only(segment, row_idx, &mut row);
            virtual_columns.augment_row(&mut row);

            if let Some(filter) = &self.filter {
                let fast_accepted = virtual_columns.is_empty()
                    && matches!(filter.matches_typed(segment, row_idx), Some(true));
                if !fast_accepted && !filter.matches(&row) {
                    continue;
                }
            }

            // Build dim key over only the active dimensions, applying
            // extraction / filter wrappers per dimension.  A rejecting
            // wrapper drops the row (Druid `listFiltered` etc. semantics).
            // Dimension values are resolved from the augmented `row` so
            // expression virtual columns can be grouped on.
            //
            // W3-SL1-D (Task #34): reuse `dim_key` buffer via clear +
            // push, borrow-based hashmap lookup; clone only on miss.
            dim_key.clear();
            dim_key.push(GroupKey::Long(bucket_key));
            for &idx in active {
                let dim_spec = &self.dimensions[idx];
                let compiled = &compiled_dims[idx];
                let name = dim_spec_input_name(dim_spec);
                let val = row.get(name).unwrap_or(&null_val);
                let Some(transformed) = compiled.apply_typed(val) else {
                    continue 'rows;
                };
                dim_key.push(transformed);
            }

            if let Some(aggs) = groups.get_mut(dim_key.as_slice()) {
                for (i, spec) in self.aggregations.iter().enumerate() {
                    feed_aggregator_row_from_map(segment, row_idx, &row, spec, aggs[i].as_mut());
                }
            } else {
                if max_keys > 0 && groups.len() >= max_keys {
                    return Err(DruidError::ResourceLimit {
                        kind: "groupBy.maxResults",
                        limit: max_keys,
                        observed: groups.len() + 1,
                    });
                }
                let owned_key = dim_key.clone();
                let aggs = groups.entry(owned_key).or_insert_with(|| {
                    self.aggregations.iter().map(|spec| spec.create()).collect()
                });
                for (i, spec) in self.aggregations.iter().enumerate() {
                    feed_aggregator_row_from_map(segment, row_idx, &row, spec, aggs[i].as_mut());
                }
            }
        }

        for (composite, aggs) in &groups {
            // W3-SL1-D key layout: composite[0] = GroupKey::Long(bucket_key),
            // composite[1..] = active dim keys in `active` order.
            let bucket_key = match composite.first() {
                Some(GroupKey::Long(b)) => *b,
                _ => {
                    debug_assert!(
                        false,
                        "composite key first element must be GroupKey::Long(bucket)"
                    );
                    0
                }
            };
            let dim_key: &[GroupKey] = composite.get(1..).unwrap_or(&[]);
            let mut event = serde_json::Map::new();

            // Emit every declared output dimension.  Active dimensions take
            // their grouped value (preserving the JSON type tag the typed
            // `GroupKey` carries, Wave 40-B); omitted dimensions are nulled.
            let mut active_pos = 0usize;
            for (i, dim_spec) in self.dimensions.iter().enumerate() {
                let out_name = dim_spec_output_name(dim_spec);
                let value = if active.contains(&i) {
                    let v = dim_key[active_pos].to_json();
                    active_pos += 1;
                    v
                } else {
                    serde_json::Value::Null
                };
                event.insert(out_name.to_string(), value);
            }

            // Insert aggregation results.  CL-4 / W1-H R7: the GROUPING(...)
            // indicator depends only on which dimensions are in the active
            // subtotals subset; finalize its value here from the active
            // dim-name list rather than from per-row accumulation.
            let active_dim_names: Vec<String> = active
                .iter()
                .map(|&i| dim_spec_output_name(&self.dimensions[i]).to_string())
                .collect();
            for (i, spec) in self.aggregations.iter().enumerate() {
                let value = if let AggregatorSpec::Grouping {
                    fields,
                    group_by_dims,
                    ..
                } = spec
                {
                    let g = ferrodruid_aggregator::GroupingAggregator::new(
                        fields.clone(),
                        group_by_dims.clone(),
                    );
                    serde_json::Value::from(g.compute_bitmask(&active_dim_names))
                } else {
                    // Fail-closed (2026-07-11): a saturated exact-cardinality
                    // aggregator must error out here, never finalize to a
                    // silently capped count (see `finalize_agg_value`).
                    finalize_agg_value(aggs[i].as_ref())?
                };
                event.insert(spec.name().to_string(), value);
            }

            // Post-aggregations.
            if let Some(ref post_aggs) = self.post_aggregations {
                let agg_results: HashMap<String, serde_json::Value> =
                    event.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
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
                    event.insert(pa.name().to_string(), json_val);
                }
            }

            // Having filter.
            if let Some(having) = &self.having
                && !having.matches(&event)
            {
                continue;
            }

            // Multi-shard exact union (2026-07-11): swap exact-cardinality
            // outputs for their full-set envelopes AFTER post-aggregations
            // and HAVING evaluated on the exact per-segment count.  The
            // later limitSpec ordering in `execute_with_limit` reads the
            // envelope's count via `CardinalityState::peek_json`; the
            // broker merge unions the envelopes across segments.
            substitute_cardinality_partials(&self.aggregations, aggs, &mut event);

            results.push(GroupByResult {
                version: "v1".to_string(),
                timestamp: format_epoch_millis(bucket_key),
                event,
            });
        }

        Ok(())
    }

    /// Row-accumulation path for groupings that include a MULTI-VALUE
    /// string dimension (compat-11 explode semantics, matching Druid):
    ///
    /// * a row whose MV dim holds `["a","b"]` contributes to BOTH group
    ///   `"a"` and group `"b"` — every aggregator accumulates the row once
    ///   per exploded key (COUNT counts it in both groups, SUM adds its
    ///   metric to both);
    /// * an empty (`[]`) / null MV row groups as the null dimension value;
    /// * with several MV grouping dims the row contributes the cartesian
    ///   product of its element keys.
    ///
    /// Deliberately row-map based and slow — the vectorized/SIMD paths
    /// stay `String`-only and are never entered for MV dims, keeping
    /// single-value groupBy byte-for-byte on its unchanged fast paths.
    #[allow(clippy::too_many_arguments)]
    fn accumulate_mv_rows(
        &self,
        segment: &SegmentData,
        compiled_dims: &[CompiledDimSpec],
        virtual_columns: &VirtualColumns,
        active: &[usize],
        intervals: &[(i64, i64)],
        max_keys: usize,
        groups: &mut HashMap<Vec<GroupKey>, Vec<Box<dyn ferrodruid_aggregator::Aggregator>>>,
    ) -> Result<()> {
        let timestamps = segment.timestamp_column()?;
        let mut row = build_row_prealloc(segment);
        let null_val = serde_json::Value::Null;

        for (row_idx, &ts) in timestamps.iter().enumerate().take(segment.num_rows()) {
            if !intervals.is_empty()
                && !intervals
                    .iter()
                    .any(|(start, end)| ts >= *start && ts < *end)
            {
                continue;
            }
            build_row_update_only(segment, row_idx, &mut row);
            virtual_columns.augment_row(&mut row);
            if let Some(filter) = &self.filter
                && !filter.matches(&row)
            {
                continue;
            }

            let bucket_key = bucket_timestamp(ts, &self.granularity);

            // Per active dim: the element group keys this row contributes
            // (a singleton list for single-value dims).
            let mut per_dim_keys: Vec<Vec<GroupKey>> = Vec::with_capacity(active.len());
            let mut row_dropped = false;
            for &idx in active {
                let compiled = &compiled_dims[idx];
                let name = dim_spec_input_name(&self.dimensions[idx]);
                let val = row.get(name).unwrap_or(&null_val);
                let keys = explode_dim_value(val, compiled);
                if keys.is_empty() {
                    // Every element was rejected by a filtered-dimension
                    // wrapper: the row contributes nothing for this
                    // grouping (mirrors the single-value row-drop
                    // semantics of a rejecting wrapper).
                    row_dropped = true;
                    break;
                }
                per_dim_keys.push(keys);
            }
            if row_dropped {
                continue;
            }

            // Cartesian product over the per-dim key lists (odometer).
            // Per-row work is the product of the row's element counts —
            // the same blow-up shape Druid accepts for MV groupBy; the
            // `max_keys` guard still caps total retained groups.
            let mut cursor = vec![0usize; per_dim_keys.len()];
            let mut done = false;
            while !done {
                let mut dim_key: Vec<GroupKey> = Vec::with_capacity(per_dim_keys.len() + 1);
                dim_key.push(GroupKey::Long(bucket_key));
                for (d, keys) in per_dim_keys.iter().enumerate() {
                    dim_key.push(keys[cursor[d]].clone());
                }
                if let Some(aggs) = groups.get_mut(dim_key.as_slice()) {
                    for (i, spec) in self.aggregations.iter().enumerate() {
                        feed_aggregator_row_from_map(
                            segment,
                            row_idx,
                            &row,
                            spec,
                            aggs[i].as_mut(),
                        );
                    }
                } else {
                    if max_keys > 0 && groups.len() >= max_keys {
                        return Err(DruidError::ResourceLimit {
                            kind: "groupBy.maxResults",
                            limit: max_keys,
                            observed: groups.len() + 1,
                        });
                    }
                    let aggs = groups.entry(dim_key).or_insert_with(|| {
                        self.aggregations.iter().map(|spec| spec.create()).collect()
                    });
                    for (i, spec) in self.aggregations.iter().enumerate() {
                        feed_aggregator_row_from_map(
                            segment,
                            row_idx,
                            &row,
                            spec,
                            aggs[i].as_mut(),
                        );
                    }
                }
                // Advance the odometer (rightmost dim fastest); a full
                // wrap means every combination has been visited.
                done = true;
                for d in (0..per_dim_keys.len()).rev() {
                    cursor[d] += 1;
                    if cursor[d] < per_dim_keys[d].len() {
                        done = false;
                        break;
                    }
                    cursor[d] = 0;
                }
            }
        }
        Ok(())
    }

    /// Vectorized columnar fast path for [`Self::aggregate_active_dims`].
    ///
    /// Handles the common analytic shape — `DimensionSpec::Default` dims over
    /// dictionary (`String`) or `Long` columns, `count`/`longSum`/`doubleSum`
    /// aggregators bound to a matching typed column, and no row filter or
    /// virtual columns — by keying groups on integer dictionary codes (no
    /// per-row `String` allocation) and accumulating over typed column slices
    /// with monomorphized ops (no per-row boxed `serde_json` dispatch).
    ///
    /// Returns `Ok(true)` when it fully handled the query (results pushed) or
    /// `Ok(false)` to fall back to the row-oriented loop. Output is
    /// byte-identical to that loop for the shapes it accepts.
    #[allow(clippy::too_many_lines, clippy::cast_possible_truncation)]
    fn try_vectorized_active_dims(
        &self,
        segment: &SegmentData,
        virtual_columns: &VirtualColumns,
        active: &[usize],
        intervals: &[(i64, i64)],
        max_keys: usize,
        results: &mut Vec<GroupByResult>,
    ) -> Result<bool> {
        if self.filter.is_some() || !virtual_columns.is_empty() {
            return Ok(false);
        }
        // W-B legacy null mode: every vectorized strategy below reads raw
        // dictionary codes / column slices, bypassing the legacy
        // canonicalization (`''`≡null merged group keys, numeric nulls as
        // 0).  Legacy is a migration-compat mode, not a perf mode — take
        // the general row path, which reads through `column_value_at` /
        // `direct_column_to_group_key`.  Perf twins are a follow-on.
        if ferrodruid_common::legacy_null_mode() {
            return Ok(false);
        }

        enum DimCol<'a> {
            Str(&'a ferrodruid_segment::column::StringColumnData),
            Long(&'a [i64]),
        }
        let mut dim_cols: Vec<DimCol> = Vec::with_capacity(active.len());
        for &idx in active {
            let dim_spec = &self.dimensions[idx];
            let DimensionSpec::Default { output_type, .. } = dim_spec else {
                return Ok(false);
            };
            match segment.columns.get(dim_spec_input_name(dim_spec)) {
                Some(ColumnData::String(sc)) => {
                    // Null gate (D3): every vectorized strategy below keys
                    // groups on raw dictionary codes, and a SQL-NULL row
                    // shares the `""` placeholder ordinal — it would silently
                    // merge the null group into the `""` group. Null-bearing
                    // dims take the general path (which keys `GroupKey::Null`
                    // via the null-row bitmap). `null_rows()` is an O(1)
                    // layout check, so null-free columns pay nothing and take
                    // exactly the same code path as before.
                    if sc.null_rows().is_some() {
                        return Ok(false);
                    }
                    // Re-audit Medium (2026-07-19): this path keys and emits
                    // raw dictionary STRINGS, so only the pass-through STRING
                    // outputType is honoured here.  A coercing (numeric)
                    // outputType must MERGE distinct strings into one numeric
                    // group ("01"/"1" → 1, Druid convertObjectToLong) — those
                    // shapes fall back to the row path, which coerces via
                    // `CompiledDimSpec::apply_typed`.
                    if !matches!(output_type, ColumnType::String) {
                        return Ok(false);
                    }
                    dim_cols.push(DimCol::Str(sc));
                }
                Some(ColumnData::Long(v)) => {
                    // LONG outputType over a LONG column is the identity
                    // coercion and STRING is the typed pass-through — both
                    // key/emit the raw i64 (so the SQL planner's
                    // outputType=column-type specs keep this fast path).
                    // FLOAT/DOUBLE require re-keying at float/double
                    // precision: row path.
                    if !matches!(output_type, ColumnType::String | ColumnType::Long) {
                        return Ok(false);
                    }
                    dim_cols.push(DimCol::Long(v.as_slice()));
                }
                _ => return Ok(false),
            }
        }
        // Re-audit Low (2026-07-19): the sparse strategy below tracks
        // missing-row dims in a per-key i64 bitmask (one bit per active
        // dim) so a genuine i64::MIN Long value stays distinct from the
        // missing-row sentinel.  Cap the vectorized path at 63 dims;
        // wider queries (never seen in practice) take the row path.
        if dim_cols.len() > 63 {
            return Ok(false);
        }

        enum AggPlan<'a> {
            Count,
            LongSum(&'a [i64]),
            DoubleSum(&'a [f64]),
        }
        let mut agg_plans: Vec<AggPlan> = Vec::with_capacity(self.aggregations.len());
        for spec in &self.aggregations {
            let plan = match spec {
                AggregatorSpec::Count { .. } => AggPlan::Count,
                AggregatorSpec::LongSum { field_name, .. } => {
                    match segment.columns.get(field_name) {
                        Some(ColumnData::Long(v)) => AggPlan::LongSum(v.as_slice()),
                        _ => return Ok(false),
                    }
                }
                AggregatorSpec::DoubleSum { field_name, .. } => {
                    match segment.columns.get(field_name) {
                        Some(ColumnData::Double(v)) => AggPlan::DoubleSum(v.as_slice()),
                        _ => return Ok(false),
                    }
                }
                _ => return Ok(false),
            };
            agg_plans.push(plan);
        }

        // Double sums carry a parallel `seen` vec (any non-null contribution
        // per group) so an all-null group emits SQL null, matching Druid.
        // Long sums need no flag: `ColumnData::Long` has no in-band null
        // (null-bearing long input is stored as Double/NaN and falls off this
        // fast path), so every matched row contributes.
        enum Acc {
            Count(Vec<u64>),
            Long(Vec<i64>),
            Double(Vec<f64>, Vec<bool>),
        }
        let timestamps = segment.timestamp_column()?;
        let num_rows = segment.num_rows().min(timestamps.len());
        let mut accs: Vec<Acc> = agg_plans
            .iter()
            .map(|p| match p {
                AggPlan::Count => Acc::Count(Vec::new()),
                AggPlan::LongSum(_) => Acc::Long(Vec::new()),
                AggPlan::DoubleSum(_) => Acc::Double(Vec::new(), Vec::new()),
            })
            .collect();
        // Hoist the granularity bucket for the constant "all" case: its
        // `to_lowercase()` otherwise allocates a String on every row.
        // "none" is NOT constant — it buckets per millisecond (ts_millis),
        // exactly like the object form `{"type":"none"}` (re-audit Medium
        // 2026-07-19: the pre-fix hoist collapsed the string spelling into
        // one epoch bucket, diverging from Druid and from the object
        // spelling); it takes the per-row `bucket_timestamp` path below.
        let const_bucket: Option<i64> = match &self.granularity {
            GranularitySpec::Simple(s) if s.eq_ignore_ascii_case("all") => Some(0),
            _ => None,
        };

        // Group-id assignment strategy. When every active dim is a dictionary
        // (String) column, the bucket is constant, and the product of dict
        // cardinalities is bounded, index a dense `slot -> gid` array by the
        // combined dict code — direct array index, no per-row hashing.
        // Otherwise fall back to an integer-keyed HashMap probe.
        const DENSE_MAX_SLOTS: usize = 1 << 20;
        let dense: Option<(Vec<usize>, usize)> = const_bucket.and_then(|_| {
            let mut strides = vec![0usize; dim_cols.len()];
            let mut slots: usize = 1;
            for j in (0..dim_cols.len()).rev() {
                strides[j] = slots;
                let card = match &dim_cols[j] {
                    DimCol::Str(sc) => {
                        // Re-audit Low (2026-07-19): both dense arms index
                        // `slot_to_gid` / `hits` directly with this dim's
                        // codes, so every scanned row needs a genuine
                        // in-dictionary ordinal.  A dim slice shorter than
                        // the segment (corrupt / hand-built / foreign
                        // attach) would surface the i64::MIN missing-row
                        // sentinel, whose usize cast wraps into an
                        // out-of-bounds panic (request-triggered DoS).
                        // Short dims fall off the dense strategy to the
                        // sparse path, which tolerates the sentinel and
                        // emits those rows' dims as null — same as the
                        // row-oriented loop.
                        if sc.encoded_values.len() < num_rows {
                            return None;
                        }
                        sc.dictionary.len()
                    }
                    DimCol::Long(_) => return None,
                };
                slots = slots.checked_mul(card).filter(|&s| s <= DENSE_MAX_SLOTS)?;
            }
            Some((strides, slots))
        });

        // Parallel dense path: above the unit-test scale, split rows across
        // rayon's shared global pool and merge per-slot partials. f64 sums are
        // order-non-deterministic here (matches Druid's parallel aggregation,
        // per an explicit product decision); count/longSum stay exact. Small
        // queries fall through to the deterministic single-thread path below,
        // so unit tests (10K-row) remain byte-identical.
        const PARALLEL_MIN_ROWS: usize = 250_000;
        const PARALLEL_MAX_SLOTS: usize = 1 << 13;
        if let Some((strides, num_slots)) = &dense {
            let num_slots = *num_slots;
            if num_rows >= PARALLEL_MIN_ROWS
                && num_slots <= PARALLEL_MAX_SLOTS
                && rayon::current_num_threads() > 1
            {
                use rayon::iter::{IntoParallelIterator, ParallelIterator};

                // Double sums carry a parallel per-slot `seen` array (any
                // non-null contribution) so an all-null group emits SQL
                // null. The arrays are `num_slots` bools (≤ 8K) — L1-hot
                // next to the sum arrays; the OR-write stays inside the
                // existing NaN-skip branch, keeping the loops vectorizable.
                enum AccArr {
                    Count(Vec<u64>),
                    Long(Vec<i64>),
                    Double(Vec<f64>, Vec<bool>),
                }
                let make_partial = || -> (Vec<AccArr>, Vec<bool>) {
                    let a = agg_plans
                        .iter()
                        .map(|p| match p {
                            AggPlan::Count => AccArr::Count(vec![0u64; num_slots]),
                            AggPlan::LongSum(_) => AccArr::Long(vec![0i64; num_slots]),
                            AggPlan::DoubleSum(_) => {
                                AccArr::Double(vec![0f64; num_slots], vec![false; num_slots])
                            }
                        })
                        .collect::<Vec<_>>();
                    (a, vec![false; num_slots])
                };
                // Pre-extract the dict-code slices (dense dims are all String)
                // and clamp the row count to the shortest slice (dims *and* agg
                // source columns) so the inner loops index without a per-access
                // bounds check or Option unwrap.
                let dim_slices: Vec<&[u32]> = dim_cols
                    .iter()
                    .map(|dc| match dc {
                        DimCol::Str(sc) => sc.encoded_values.as_slice(),
                        DimCol::Long(_) => &[][..],
                    })
                    .collect();
                let mut rows = dim_slices
                    .iter()
                    .map(|s| s.len())
                    .min()
                    .map_or(num_rows, |m| num_rows.min(m));
                for p in &agg_plans {
                    match p {
                        AggPlan::LongSum(c) => rows = rows.min(c.len()),
                        AggPlan::DoubleSum(c) => rows = rows.min(c.len()),
                        AggPlan::Count => {}
                    }
                }
                // Interval pruning: on a sorted __time column with a single
                // interval, binary-search the row range and drop the per-row
                // interval test entirely — Q1's full-range interval elides to a
                // straight scan with zero timestamp loads/compares.
                let (scan_lo, scan_hi, check_interval) =
                    match pruned_row_range(timestamps, intervals, segment.time_sorted) {
                        Some((lo, hi)) => (lo, hi.min(rows), false),
                        None => (0, rows, !intervals.is_empty()),
                    };
                let scan_len = scan_hi.saturating_sub(scan_lo);
                let n_threads = rayon::current_num_threads().max(1);
                let chunk = scan_len.div_ceil(n_threads * 4).max(1 << 14);
                let chunk_starts: Vec<usize> = (scan_lo..scan_hi).step_by(chunk).collect();
                let dim_slices = &dim_slices;
                let (accs_arr, hit) = chunk_starts
                    .into_par_iter()
                    .map(|start| {
                        let end = (start + chunk).min(scan_hi);
                        let (mut accs, mut hits) = make_partial();
                        if check_interval {
                            // General path (multi-interval or unsorted __time):
                            // per-row interval test + enum-dispatch accumulate.
                            for row in start..end {
                                let ts = timestamps[row];
                                if !intervals.iter().any(|(s, e)| ts >= *s && ts < *e) {
                                    continue;
                                }
                                let mut flat = 0usize;
                                for (j, sl) in dim_slices.iter().enumerate() {
                                    flat += (sl[row] as usize) * strides[j];
                                }
                                hits[flat] = true;
                                for (k, plan) in agg_plans.iter().enumerate() {
                                    match (plan, &mut accs[k]) {
                                        (AggPlan::Count, AccArr::Count(v)) => v[flat] += 1,
                                        (AggPlan::LongSum(col), AccArr::Long(v)) => {
                                            if let Some(&x) = col.get(row) {
                                                v[flat] = v[flat].wrapping_add(x);
                                            }
                                        }
                                        (AggPlan::DoubleSum(col), AccArr::Double(v, seen)) => {
                                            // NaN = SQL NULL (in-band marker):
                                            // skip so a null never poisons the
                                            // sum (Druid: SUM ignores nulls).
                                            // `seen` records ≥1 non-null
                                            // contribution (all-null ⇒ null).
                                            if let Some(&x) = col.get(row)
                                                && !x.is_nan()
                                            {
                                                v[flat] += x;
                                                seen[flat] = true;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            return (accs, hits);
                        }
                        // Fast path: no per-row interval test. Two passes with
                        // group-id computation (pass 1) hoisted out of the per-agg
                        // enum dispatch (pass 2), tiled so the flat-index buffer
                        // stays L1-resident. A full-chunk index buffer spills L2
                        // back to DRAM at SF10 scale (~50% extra memory traffic on
                        // a bandwidth-bound scan); tiling streams each aggregation
                        // column from DRAM exactly once with the index buffer hot.
                        const TILE: usize = 8192;
                        let mut flatbuf = [0u32; TILE];
                        let mut b0 = start;
                        while b0 < end {
                            let b1 = (b0 + TILE).min(end);
                            let fb = &mut flatbuf[..b1 - b0];
                            match dim_slices.len() {
                                1 => {
                                    let s0 = &dim_slices[0][b0..b1];
                                    for (dst, &a) in fb.iter_mut().zip(s0) {
                                        *dst = a;
                                        hits[a as usize] = true;
                                    }
                                }
                                2 => {
                                    let st0 = strides[0];
                                    let s0 = &dim_slices[0][b0..b1];
                                    let s1 = &dim_slices[1][b0..b1];
                                    for (dst, (&a, &b)) in fb.iter_mut().zip(s0.iter().zip(s1)) {
                                        let f = (a as usize) * st0 + b as usize;
                                        *dst = f as u32;
                                        hits[f] = true;
                                    }
                                }
                                _ => {
                                    for (i, dst) in fb.iter_mut().enumerate() {
                                        let row = b0 + i;
                                        let mut f = 0usize;
                                        for (j, sl) in dim_slices.iter().enumerate() {
                                            f += (sl[row] as usize) * strides[j];
                                        }
                                        *dst = f as u32;
                                        hits[f] = true;
                                    }
                                }
                            }
                            let fb = &flatbuf[..b1 - b0];
                            for (k, plan) in agg_plans.iter().enumerate() {
                                match (plan, &mut accs[k]) {
                                    (AggPlan::Count, AccArr::Count(v)) => {
                                        for &f in fb {
                                            v[f as usize] += 1;
                                        }
                                    }
                                    (AggPlan::LongSum(col), AccArr::Long(v)) => {
                                        for (&f, &x) in fb.iter().zip(&col[b0..b1]) {
                                            let s = f as usize;
                                            v[s] = v[s].wrapping_add(x);
                                        }
                                    }
                                    (AggPlan::DoubleSum(col), AccArr::Double(v, seen)) => {
                                        // NaN = SQL NULL: the `!is_nan` guard
                                        // is a predictable per-element branch
                                        // (`x == x` compare) that keeps the
                                        // tiled loop vectorizable; null-free
                                        // columns take it 100%-predicted. The
                                        // predicated `seen` byte-store hits
                                        // the same ≤8K L1-resident slot range
                                        // as the sum write (all-null ⇒ null).
                                        for (&f, &x) in fb.iter().zip(&col[b0..b1]) {
                                            if !x.is_nan() {
                                                v[f as usize] += x;
                                                seen[f as usize] = true;
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            b0 = b1;
                        }
                        (accs, hits)
                    })
                    .reduce(make_partial, |mut a, b| {
                        for (av, bv) in a.0.iter_mut().zip(b.0.iter()) {
                            match (av, bv) {
                                (AccArr::Count(x), AccArr::Count(y)) => {
                                    for (xi, yi) in x.iter_mut().zip(y) {
                                        *xi += *yi;
                                    }
                                }
                                (AccArr::Long(x), AccArr::Long(y)) => {
                                    for (xi, yi) in x.iter_mut().zip(y) {
                                        *xi = xi.wrapping_add(*yi);
                                    }
                                }
                                (AccArr::Double(x, sx), AccArr::Double(y, sy)) => {
                                    for (xi, yi) in x.iter_mut().zip(y) {
                                        *xi += *yi;
                                    }
                                    for (si, sj) in sx.iter_mut().zip(sy) {
                                        *si |= *sj;
                                    }
                                }
                                _ => {}
                            }
                        }
                        for (hi, hj) in a.1.iter_mut().zip(b.1.iter()) {
                            *hi |= *hj;
                        }
                        a
                    });

                let n_groups = hit.iter().filter(|&&h| h).count();
                if max_keys > 0 && n_groups > max_keys {
                    return Err(DruidError::ResourceLimit {
                        kind: "groupBy.maxResults",
                        limit: max_keys,
                        observed: n_groups,
                    });
                }
                let bucket_key = const_bucket.unwrap_or(0);
                for flat in 0..num_slots {
                    if !hit[flat] {
                        continue;
                    }
                    let mut event = serde_json::Map::new();
                    let mut active_pos = 0usize;
                    for (i, dim_spec) in self.dimensions.iter().enumerate() {
                        let out_name = dim_spec_output_name(dim_spec);
                        let value = if active.contains(&i) {
                            let st = strides[active_pos];
                            // Mixed-radix decode: ordinal for this dim.
                            let above = if active_pos == 0 {
                                num_slots
                            } else {
                                strides[active_pos - 1]
                            };
                            let ord = (flat / st) % (above / st);
                            let v = match &dim_cols[active_pos] {
                                DimCol::Str(sc) => {
                                    sc.dictionary.get(ord).map_or(serde_json::Value::Null, |s| {
                                        serde_json::Value::String(s.to_string())
                                    })
                                }
                                DimCol::Long(_) => serde_json::Value::Null,
                            };
                            active_pos += 1;
                            v
                        } else {
                            serde_json::Value::Null
                        };
                        event.insert(out_name.to_string(), value);
                    }
                    for (k, spec) in self.aggregations.iter().enumerate() {
                        let value = match &accs_arr[k] {
                            AccArr::Count(v) => {
                                serde_json::Value::Number(serde_json::Number::from(v[flat]))
                            }
                            AccArr::Long(v) => {
                                serde_json::Value::Number(serde_json::Number::from(v[flat]))
                            }
                            // No non-null contribution ⇒ SQL null (Druid:
                            // SUM over an all-null group is null, not 0).
                            AccArr::Double(_, seen) if !seen[flat] => serde_json::Value::Null,
                            AccArr::Double(v, _) => {
                                serde_json::to_value(v[flat]).unwrap_or(serde_json::Value::Null)
                            }
                        };
                        event.insert(spec.name().to_string(), value);
                    }
                    if let Some(ref post_aggs) = self.post_aggregations {
                        let agg_results: HashMap<String, serde_json::Value> =
                            event.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
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
                            event.insert(pa.name().to_string(), json_val);
                        }
                    }
                    if let Some(having) = &self.having
                        && !having.matches(&event)
                    {
                        continue;
                    }
                    results.push(GroupByResult {
                        version: "v1".to_string(),
                        timestamp: format_epoch_millis(bucket_key),
                        event,
                    });
                }
                return Ok(true);
            }
        }

        let mut slot_to_gid: Vec<u32> = match &dense {
            Some((_, slots)) => vec![u32::MAX; *slots],
            None => Vec::new(),
        };
        let mut keymap: HashMap<Vec<i64>, u32> = HashMap::new();
        // gid -> key ([bucket, dim_code_0, .., missing_mask]); indexes `accs`
        // and drives emit.
        let mut group_keys: Vec<Vec<i64>> = Vec::new();
        // Group key = [bucket, dim_code_0, .., dim_code_{k-1}, missing_mask]
        // (all i64).  Re-audit Low (2026-07-19): i64::MIN doubles as the
        // "dim column shorter than the segment" missing-row marker AND as a
        // legitimate Long dimension value, and the pre-fix emit rendered
        // BOTH as JSON null while the row-oriented path emits the genuine
        // value as the number -9223372036854775808 — path-dependent output.
        // The trailing `missing_mask` (bit j = dim j had no row) carries
        // missing-ness out-of-band, so a genuine-MIN group keys and emits
        // as the number while a missing-row group keys and emits as null —
        // the same two groups the row-oriented path produces.
        let mut key_buf: Vec<i64> = Vec::with_capacity(dim_cols.len() + 2);

        for (row, &ts) in timestamps.iter().enumerate().take(num_rows) {
            if !intervals.is_empty() && !intervals.iter().any(|(s, e)| ts >= *s && ts < *e) {
                continue;
            }
            key_buf.clear();
            key_buf.push(const_bucket.unwrap_or_else(|| bucket_timestamp(ts, &self.granularity)));
            let mut missing_mask: i64 = 0;
            for (j, dc) in dim_cols.iter().enumerate() {
                let code = match dc {
                    DimCol::Str(sc) => match sc.encoded_values.get(row) {
                        Some(&c) => i64::from(c),
                        None => {
                            missing_mask |= 1 << j;
                            i64::MIN
                        }
                    },
                    DimCol::Long(v) => match v.get(row) {
                        Some(&x) => x,
                        None => {
                            missing_mask |= 1 << j;
                            i64::MIN
                        }
                    },
                };
                key_buf.push(code);
            }
            key_buf.push(missing_mask);
            let gid = if let Some((strides, _)) = &dense {
                // Dense: flat index = Σ code_j · stride_j (codes are valid
                // ordinals < cardinality by construction, so flat < slots).
                let mut flat = 0usize;
                for (j, &st) in strides.iter().enumerate() {
                    flat += (key_buf[1 + j] as usize) * st;
                }
                let slot = slot_to_gid[flat];
                if slot == u32::MAX {
                    if max_keys > 0 && group_keys.len() >= max_keys {
                        return Err(DruidError::ResourceLimit {
                            kind: "groupBy.maxResults",
                            limit: max_keys,
                            observed: group_keys.len() + 1,
                        });
                    }
                    let g = group_keys.len() as u32;
                    slot_to_gid[flat] = g;
                    group_keys.push(key_buf.clone());
                    for a in &mut accs {
                        match a {
                            Acc::Count(v) => v.push(0),
                            Acc::Long(v) => v.push(0),
                            Acc::Double(v, seen) => {
                                v.push(0.0);
                                seen.push(false);
                            }
                        }
                    }
                    g as usize
                } else {
                    slot as usize
                }
            } else if let Some(&g) = keymap.get(key_buf.as_slice()) {
                g as usize
            } else {
                if max_keys > 0 && keymap.len() >= max_keys {
                    return Err(DruidError::ResourceLimit {
                        kind: "groupBy.maxResults",
                        limit: max_keys,
                        observed: keymap.len() + 1,
                    });
                }
                let g = keymap.len() as u32;
                keymap.insert(key_buf.clone(), g);
                group_keys.push(key_buf.clone());
                for a in &mut accs {
                    match a {
                        Acc::Count(v) => v.push(0),
                        Acc::Long(v) => v.push(0),
                        Acc::Double(v, seen) => {
                            v.push(0.0);
                            seen.push(false);
                        }
                    }
                }
                g as usize
            };
            for (k, plan) in agg_plans.iter().enumerate() {
                match (plan, &mut accs[k]) {
                    (AggPlan::Count, Acc::Count(v)) => v[gid] += 1,
                    (AggPlan::LongSum(col), Acc::Long(v)) => {
                        if let Some(&x) = col.get(row) {
                            v[gid] = v[gid].wrapping_add(x);
                        }
                    }
                    (AggPlan::DoubleSum(col), Acc::Double(v, seen)) => {
                        // NaN = SQL NULL: skip so a null never poisons the
                        // sum; `seen` records ≥1 non-null contribution
                        // (all-null ⇒ SQL null on emit).
                        if let Some(&x) = col.get(row)
                            && !x.is_nan()
                        {
                            v[gid] += x;
                            seen[gid] = true;
                        }
                    }
                    _ => unreachable!("agg plan / accumulator constructed in lockstep"),
                }
            }
        }

        // Emit — mirrors the row-oriented emit block exactly.
        for (gid, key) in group_keys.iter().enumerate() {
            let bucket_key = key[0];
            // Trailing missing-row bitmask (see the key-layout note above);
            // `.get` keeps this total even if a legacy-shaped key appeared.
            let missing_mask = key.get(1 + dim_cols.len()).copied().unwrap_or(0);
            let mut event = serde_json::Map::new();
            let mut active_pos = 0usize;
            for (i, dim_spec) in self.dimensions.iter().enumerate() {
                let out_name = dim_spec_output_name(dim_spec);
                let value = if active.contains(&i) {
                    let code = key[1 + active_pos];
                    let v = match &dim_cols[active_pos] {
                        DimCol::Str(sc) => usize::try_from(code)
                            .ok()
                            .and_then(|ord| sc.dictionary.get(ord))
                            .map_or(serde_json::Value::Null, |s| {
                                serde_json::Value::String(s.to_string())
                            }),
                        DimCol::Long(_) => {
                            // Re-audit Low (2026-07-19): only a MISSING row
                            // (mask bit set) emits null; a genuine i64::MIN
                            // dimension value emits the number, matching
                            // the row-oriented path.
                            if missing_mask & (1 << active_pos) != 0 {
                                serde_json::Value::Null
                            } else {
                                serde_json::Value::Number(serde_json::Number::from(code))
                            }
                        }
                    };
                    active_pos += 1;
                    v
                } else {
                    serde_json::Value::Null
                };
                event.insert(out_name.to_string(), value);
            }
            for (k, spec) in self.aggregations.iter().enumerate() {
                let value = match &accs[k] {
                    Acc::Count(v) => serde_json::Value::Number(serde_json::Number::from(v[gid])),
                    Acc::Long(v) => serde_json::Value::Number(serde_json::Number::from(v[gid])),
                    // No non-null contribution ⇒ SQL null.
                    Acc::Double(_, seen) if !seen[gid] => serde_json::Value::Null,
                    Acc::Double(v, _) => {
                        serde_json::to_value(v[gid]).unwrap_or(serde_json::Value::Null)
                    }
                };
                event.insert(spec.name().to_string(), value);
            }
            if let Some(ref post_aggs) = self.post_aggregations {
                let agg_results: HashMap<String, serde_json::Value> =
                    event.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
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
                    event.insert(pa.name().to_string(), json_val);
                }
            }
            if let Some(having) = &self.having
                && !having.matches(&event)
            {
                continue;
            }
            results.push(GroupByResult {
                version: "v1".to_string(),
                timestamp: format_epoch_millis(bucket_key),
                event,
            });
        }

        Ok(true)
    }
}

/// **W3-SL1-C sub-step (Task #33)** — direct typed-column → GroupKey
/// conversion for the plain `DimensionSpec::Default` fast path in
/// `aggregate_active_dims`. Skips the intermediate `Value::String`
/// (which allocates a per-row `String`) produced by
/// [`crate::helpers::column_value_at`] followed by another `String::clone()`
/// in `json_to_group_key`. For Q1's 2 string dims × 60 M rows this
/// halves the string-heap traffic (120 M `String` allocs eliminated).
///
/// Semantics-equivalent to
/// `json_to_group_key(&column_value_at(col, row_idx))` for the
/// column types this handles; returns `GroupKey::Null` for
/// `ColumnData::Complex` (which serialises to `Value::Null` in
/// `column_value_at`) and for out-of-range indices.
#[must_use]
fn direct_column_to_group_key(col: &ColumnData, row_idx: usize) -> GroupKey {
    // W-B legacy null mode: keep the documented equivalence with
    // `json_to_group_key(&column_value_at(col, row_idx))` — under legacy
    // that composition yields the coerced-default keys (numeric NULL →
    // `Long(0)`/`Double(0.0)`, string `''`/null → the ONE merged
    // `GroupKey::Null` that native output renders as JSON null), all
    // oracle-measured (group_y.json: `0` group; ext_group_d.json: `0.0`
    // group; native_groupby_strcol.json: merged null group).
    let legacy = ferrodruid_common::legacy_null_mode();
    match col {
        ColumnData::Long(v) => v
            .get(row_idx)
            .copied()
            .map_or(GroupKey::Null, GroupKey::Long),
        // Nullable long: the null-row bitmap decides NULL (the value vector
        // stores 0 at NULL rows); non-null rows key on the EXACT i64 —
        // `GroupKey::Long` — so values beyond ±2^53 group exactly.  Legacy
        // mode skips the bitmap: the stored 0 IS the coerced key.
        ColumnData::LongNullable(v, nulls) => v.get(row_idx).copied().map_or(GroupKey::Null, |x| {
            if !legacy && is_null_long_row(nulls, row_idx) {
                GroupKey::Null
            } else {
                GroupKey::Long(x)
            }
        }),
        // NaN doubles/floats are the in-band SQL-NULL marker
        // (`ferrodruid_segment::column::NULL_DOUBLE`); `column_value_at`
        // renders them as JSON null (`Number::from_f64(NaN)` is `None`), so
        // the documented equivalence requires `GroupKey::Null` here — never a
        // `GroupKey::Double(NaN)` group key.  Legacy: the NaN null keys as
        // the coerced `0.0`.
        ColumnData::Double(v) => v.get(row_idx).copied().map_or(GroupKey::Null, |x| {
            if is_null_double(x) {
                if legacy {
                    GroupKey::Double(OrderedFloat(0.0))
                } else {
                    GroupKey::Null
                }
            } else {
                GroupKey::Double(OrderedFloat(x))
            }
        }),
        ColumnData::Float(v) => v.get(row_idx).copied().map_or(GroupKey::Null, |x| {
            if is_null_double(f64::from(x)) {
                if legacy {
                    GroupKey::Double(OrderedFloat(0.0))
                } else {
                    GroupKey::Null
                }
            } else {
                GroupKey::Double(OrderedFloat(f64::from(x)))
            }
        }),
        ColumnData::String(sc) => {
            // SQL-NULL rows point at the `""` placeholder ordinal; check the
            // null-row bitmap before dictionary resolution (see
            // `column_value_at`) so null groups key as `GroupKey::Null`,
            // distinct from the `""` group.  Legacy: a resolved `''` value
            // ALSO keys as `GroupKey::Null` — the merged ''/null group.
            if sc.is_null_row(row_idx) {
                GroupKey::Null
            } else {
                let ord = sc.encoded_values.get(row_idx).copied().unwrap_or(0) as usize;
                match sc.dictionary.get(ord) {
                    Some(s) if legacy && s.is_empty() => GroupKey::Null,
                    Some(s) => GroupKey::String(s.to_string()),
                    None => GroupKey::Null,
                }
            }
        }
        // Multi-value string dimension (compat-11): a multi-element row has
        // NO single group key — Druid explodes it into one key per element,
        // which `aggregate_active_dims` does on its dedicated MV path
        // (`accumulate_mv_rows`), so this arm is not reached for MV
        // grouping dims.  For the documented equivalence with
        // `json_to_group_key(&column_value_at(col, row_idx))` we mirror
        // that composition exactly: empty row → Null, 1-element row → the
        // scalar string, multi-element row → the JSON-array text (what
        // `json_to_group_key` yields for `Value::Array`).
        ColumnData::StringMulti(_) => {
            crate::dim_spec::json_to_group_key(&column_value_at(col, row_idx))
        }
        ColumnData::Complex(_) => GroupKey::Null,
        // Decoded theta column (compat-8 sketch #2): grouping BY a sketch
        // has no meaningful identity, but the documented equivalence with
        // `json_to_group_key(&column_value_at(col, row_idx))` must hold
        // across code paths — `column_value_at` renders the row as its
        // partial-state envelope object, so mirror that composition
        // exactly (an object keys as its JSON text, same as StringMulti's
        // multi-element rows).
        ColumnData::ComplexTheta(_) => {
            crate::dim_spec::json_to_group_key(&column_value_at(col, row_idx))
        }
        // Decoded hyperUnique column (W-A, v1.5.0): same rule as
        // ComplexTheta — grouping BY a sketch has no meaningful identity,
        // but the documented `json_to_group_key(&column_value_at(..))`
        // equivalence must hold across code paths.
        ColumnData::ComplexHyperUnique(_) => {
            crate::dim_spec::json_to_group_key(&column_value_at(col, row_idx))
        }
    }
}

/// Explode one row's dimension VALUE into its per-element group keys
/// (compat-11 MV semantics).
///
/// * A JSON array (a multi-element MV row as materialised by
///   `column_value_at`) yields one key per element, each transformed
///   through the dimension spec; a wrapper that rejects an element drops
///   that ELEMENT (Druid's filtered-dimension-on-MV element filtering).
/// * An empty array yields the single null key (empty MV groups as null).
/// * Any scalar (including a 1-element MV row, which materialises as the
///   scalar string) yields its single transformed key; `None` from a
///   rejecting wrapper yields an empty vec and the caller drops the row —
///   unchanged single-value semantics.
pub(crate) fn explode_dim_value(
    val: &serde_json::Value,
    compiled: &CompiledDimSpec,
) -> Vec<GroupKey> {
    match val {
        serde_json::Value::Array(elems) => {
            if elems.is_empty() {
                return vec![GroupKey::Null];
            }
            elems
                .iter()
                .filter_map(|e| compiled.apply_typed(e))
                .collect()
        }
        scalar => compiled.apply_typed(scalar).into_iter().collect(),
    }
}

/// Extract input dimension name from a DimensionSpec.
fn dim_spec_input_name(spec: &DimensionSpec) -> &str {
    match spec {
        DimensionSpec::Default { dimension, .. } => dimension,
        DimensionSpec::Extraction { dimension, .. } => dimension,
        DimensionSpec::ListFiltered { delegate, .. } => dim_spec_input_name(delegate),
        DimensionSpec::RegexFiltered { delegate, .. } => dim_spec_input_name(delegate),
        DimensionSpec::PrefixFiltered { delegate, .. } => dim_spec_input_name(delegate),
    }
}

/// Extract output dimension name from a DimensionSpec.
fn dim_spec_output_name(spec: &DimensionSpec) -> &str {
    match spec {
        DimensionSpec::Default { output_name, .. } => output_name,
        DimensionSpec::Extraction { output_name, .. } => output_name,
        DimensionSpec::ListFiltered { delegate, .. } => dim_spec_output_name(delegate),
        DimensionSpec::RegexFiltered { delegate, .. } => dim_spec_output_name(delegate),
        DimensionSpec::PrefixFiltered { delegate, .. } => dim_spec_output_name(delegate),
    }
}

// (Wave 40-B) `json_to_string` removed — typed `GroupKey` keys via
// `apply_dim_spec_typed` replace the previous string-coercion path.

// ---------------------------------------------------------------------------
// Wave 40-B regression tests (Wave 39 [High] [NEW-VARIANT] groupby.rs:247-259)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_aggregator::AggregatorSpec;
    use ferrodruid_common::types::{ColumnType, DataSource};
    use ferrodruid_segment::Interval;
    use ferrodruid_segment::SegmentData;
    use ferrodruid_segment::column::ColumnData;

    fn build_long_dim_segment() -> SegmentData {
        // Timestamps and a `code` Long column with values [1, 1, 2].
        // The pre-W40-B groupby would first stringify them, so
        // `code = 1` (Long) and a hypothetical `code = "1"` (String)
        // would have collided — we directly assert the typed key path
        // returns 2 distinct groups (1 and 2).
        let mut columns = std::collections::HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![100, 200, 300]));
        columns.insert("code".to_string(), ColumnData::Long(vec![1, 1, 2]));
        SegmentData {
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
        }
    }

    #[test]
    fn groupby_long_dimension_keys_distinguish_typed_values_not_string() {
        // GroupBy on a Long-typed dimension must produce typed-Number keys
        // in the result (NOT stringified).  Pre-W40-B the result event
        // had `"code": "1"` (string); the typed path emits `"code": 1`.
        let segment = build_long_dim_segment();
        let q = GroupByQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec!["1970-01-01T00:00:00Z/1970-01-01T00:00:01Z".into()],
            granularity: GranularitySpec::Simple("all".into()),
            dimensions: vec![ferrodruid_common::types::DimensionSpec::Default {
                dimension: "code".into(),
                output_name: "code".into(),
                output_type: ColumnType::Long,
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
        let mut results = q.execute(&segment).expect("execute");
        results.sort_by(|a, b| {
            a.event
                .get("code")
                .and_then(|v| v.as_i64())
                .cmp(&b.event.get("code").and_then(|v| v.as_i64()))
        });
        assert_eq!(results.len(), 2, "must produce 2 distinct numeric groups");
        // Critical assertion: the typed key path emits a JSON Number,
        // not a string — `as_i64()` returns Some(_), `as_str()` is None.
        for r in &results {
            let v = r.event.get("code").expect("code");
            assert!(
                v.as_i64().is_some(),
                "Wave 40-B: GroupBy on Long dimension must emit JSON Number, got {v}"
            );
            assert!(
                v.as_str().is_none(),
                "Wave 40-B: GroupBy on Long dimension must NOT stringify the key (got {v})"
            );
        }
        let counts: Vec<i64> = results
            .iter()
            .map(|r| r.event.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0))
            .collect();
        assert_eq!(counts, vec![2, 1], "code=1 has 2 rows, code=2 has 1 row");
    }

    /// Granularity `"none"` (the STRING spelling — the form most clients
    /// emit) must bucket per distinct millisecond exactly like the object
    /// form `{"type":"none"}` (re-audit Medium 2026-07-19; pre-fix the
    /// string spelling collapsed every row into one 1970-01-01 bucket
    /// like `"all"`, while the object spelling bucketed per millisecond —
    /// internally inconsistent and silently wrong vs Druid).
    #[test]
    fn groupby_granularity_none_string_buckets_per_millisecond() {
        let segment = build_long_dim_segment(); // __time 100/200/300, code 1/1/2
        let make = |granularity: GranularitySpec| GroupByQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec!["1970-01-01T00:00:00Z/1970-01-01T00:00:01Z".into()],
            granularity,
            dimensions: vec![ferrodruid_common::types::DimensionSpec::Default {
                dimension: "code".into(),
                output_name: "code".into(),
                output_type: ColumnType::Long,
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
        let string_form = make(GranularitySpec::Simple("none".into()))
            .execute(&segment)
            .expect("string form");
        let object_form = make(GranularitySpec::Full(
            ferrodruid_common::types::Granularity::None,
        ))
        .execute(&segment)
        .expect("object form");

        // One bucket per distinct millisecond, each holding its single row.
        assert_eq!(
            string_form.len(),
            3,
            "granularity \"none\" must produce one bucket per distinct __time"
        );
        let timestamps: Vec<&str> = string_form.iter().map(|r| r.timestamp.as_str()).collect();
        assert_eq!(
            timestamps,
            vec![
                "1970-01-01T00:00:00.100Z",
                "1970-01-01T00:00:00.200Z",
                "1970-01-01T00:00:00.300Z",
            ]
        );
        for r in &string_form {
            assert_eq!(r.event.get("cnt").and_then(|v| v.as_i64()), Some(1));
        }
        // Both spellings must agree exactly.
        let render = |rows: &[GroupByResult]| -> Vec<(String, String)> {
            rows.iter()
                .map(|r| {
                    (
                        r.timestamp.clone(),
                        serde_json::Value::Object(r.event.clone()).to_string(),
                    )
                })
                .collect()
        };
        assert_eq!(
            render(&string_form),
            render(&object_form),
            "string and object spellings of `none` must produce identical results"
        );
    }

    /// `{"type":"default","dimension":"code","outputType":"LONG"}` over a
    /// single-value STRING column: Druid coerces each value to LONG
    /// (`convertObjectToLong`) and MERGES `"01"` and `"1"` into ONE
    /// numeric group `1` with number-typed keys (re-audit Medium
    /// 2026-07-19; pre-fix `outputType` had no consumer outside the MV
    /// plan-time guard, so FerroDruid silently kept two distinct
    /// string-typed groups — wrong group count and wrong key type).
    #[test]
    fn groupby_output_type_long_coerces_and_merges_string_column() {
        use ferrodruid_segment::SegmentDataBuilder;
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200, 300])
            .add_string_column("code", vec!["01".into(), "1".into(), "2".into()])
            .build()
            .expect("build segment");
        // Parse from JSON so the wire `outputType` decode is exercised too.
        let q = parse_groupby(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": [
                    {"type":"default","dimension":"code","outputName":"code","outputType":"LONG"}
                ],
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        );
        let mut results = q.execute(&segment).expect("execute");
        results.sort_by_key(|r| r.event.get("code").and_then(|v| v.as_i64()));
        assert_eq!(
            results.len(),
            2,
            "\"01\" and \"1\" must merge into the single numeric group 1"
        );
        assert_eq!(
            results[0].event.get("code"),
            Some(&serde_json::json!(1)),
            "the merged group keys as the JSON NUMBER 1"
        );
        assert_eq!(
            results[0].event.get("cnt").and_then(|v| v.as_i64()),
            Some(2),
            "both coerced rows aggregate into the merged group"
        );
        assert_eq!(results[1].event.get("code"), Some(&serde_json::json!(2)));
        assert_eq!(
            results[1].event.get("cnt").and_then(|v| v.as_i64()),
            Some(1)
        );
        for r in &results {
            assert!(
                r.event.get("code").and_then(|v| v.as_str()).is_none(),
                "keys must be number-typed, not strings"
            );
        }
    }

    /// An unparseable string under a numeric `outputType` coerces to the
    /// SQL-null group (Druid `convertObjectToLong` yields null), never a
    /// string-typed group.
    #[test]
    fn groupby_output_type_long_unparseable_string_groups_as_null() {
        use ferrodruid_segment::SegmentDataBuilder;
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200])
            .add_string_column("code", vec!["not-a-number".into(), "1".into()])
            .build()
            .expect("build segment");
        let q = parse_groupby(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": [
                    {"type":"default","dimension":"code","outputName":"code","outputType":"LONG"}
                ],
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        );
        let results = q.execute(&segment).expect("execute");
        assert_eq!(results.len(), 2);
        let null_group = results
            .iter()
            .find(|r| r.event.get("code") == Some(&serde_json::Value::Null))
            .expect("unparseable value must land in the null group");
        assert_eq!(
            null_group.event.get("cnt").and_then(|v| v.as_i64()),
            Some(1)
        );
        let one_group = results
            .iter()
            .find(|r| r.event.get("code") == Some(&serde_json::json!(1)))
            .expect("parseable value keys numerically");
        assert_eq!(one_group.event.get("cnt").and_then(|v| v.as_i64()), Some(1));
    }

    /// Druid-compat regression (High, 2026-07-19): under `outputType:
    /// LONG` a NON-INTEGRAL string like `"1.9"` must land in the
    /// SQL-null group and an out-of-range string must too — Druid 31's
    /// `getExactLongFromDecimalString` is `BigDecimal(str)
    /// .longValueExact()`, which admits only exact integers in i64
    /// range.  Pre-fix, FerroDruid fell back to an f64 parse and cast,
    /// truncating `"1.9"` into the unrelated integer group 1 and
    /// saturating huge strings to an i64 bound — wrong merge vs Druid.
    #[test]
    fn groupby_output_type_long_nonintegral_and_out_of_range_strings_group_as_null() {
        use ferrodruid_segment::SegmentDataBuilder;
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200, 300, 400])
            .add_string_column(
                "code",
                vec![
                    "1".into(),
                    "1.9".into(),
                    "2".into(),
                    "9999999999999999999999".into(),
                ],
            )
            .build()
            .expect("build segment");
        let q = parse_groupby(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": [
                    {"type":"default","dimension":"code","outputName":"code","outputType":"LONG"}
                ],
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        );
        let results = q.execute(&segment).expect("execute");
        assert_eq!(
            results.len(),
            3,
            "expected groups null / 1 / 2 — \"1.9\" must NOT merge into group 1"
        );
        let count_of = |key: &serde_json::Value| {
            results
                .iter()
                .find(|r| r.event.get("code") == Some(key))
                .and_then(|r| r.event.get("cnt"))
                .and_then(serde_json::Value::as_i64)
        };
        assert_eq!(
            count_of(&serde_json::Value::Null),
            Some(2),
            "\"1.9\" (non-integral) and the out-of-range string both key as SQL-null"
        );
        assert_eq!(
            count_of(&serde_json::json!(1)),
            Some(1),
            "group 1 must hold ONLY the exact \"1\" row (no truncated \"1.9\")"
        );
        assert_eq!(count_of(&serde_json::json!(2)), Some(1));
        assert!(
            !results
                .iter()
                .any(|r| r.event.get("code") == Some(&serde_json::json!(i64::MAX))),
            "out-of-range string must not saturate into an i64::MAX group"
        );
    }

    /// A String dim column SHORTER than the segment row count (corrupt /
    /// hand-built / foreign-attach segment — every `SegmentData` field is
    /// pub) must not panic the vectorized dense strategy (re-audit Low
    /// 2026-07-19: the i64::MIN missing-row sentinel wrapped through the
    /// dense flat-index usize cast and indexed `slot_to_gid` out of
    /// bounds).  Short dims now fall off the dense strategy to the sparse
    /// path, which groups the missing rows' dim as null — the same shape
    /// the row-oriented path produces.
    #[test]
    fn groupby_short_string_dim_column_does_not_panic() {
        use ferrodruid_segment::column::StringColumnData;
        let mut columns = std::collections::HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![100, 200, 300, 400]),
        );
        // 2-row string column against a 4-row segment.
        columns.insert(
            "city".to_string(),
            ColumnData::String(StringColumnData::from_nullable_values(&[
                Some("A".to_string()),
                Some("A".to_string()),
            ])),
        );
        let segment = SegmentData {
            version: 9,
            num_rows: 4,
            interval: Interval {
                start_millis: 0,
                end_millis: 1000,
            },
            dimensions: vec!["city".to_string()],
            metrics: vec![],
            columns,
            time_sorted: false,
        };
        let q = GroupByQuery {
            data_source: DataSource::Table { name: "t".into() },
            intervals: vec!["1970-01-01T00:00:00Z/1970-01-01T00:00:01Z".into()],
            granularity: GranularitySpec::Simple("all".into()),
            dimensions: vec![ferrodruid_common::types::DimensionSpec::Default {
                dimension: "city".into(),
                output_name: "city".into(),
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
        let results = q
            .execute(&segment)
            .expect("must not panic on a short dim column");
        assert_eq!(results.len(), 2, "one real group + one missing-row group");
        let a_group = results
            .iter()
            .find(|r| r.event.get("city").and_then(|v| v.as_str()) == Some("A"))
            .expect("A group");
        assert_eq!(a_group.event.get("cnt").and_then(|v| v.as_i64()), Some(2));
        let null_group = results
            .iter()
            .find(|r| r.event.get("city") == Some(&serde_json::Value::Null))
            .expect("missing rows group as null, like the row-oriented path");
        assert_eq!(
            null_group.event.get("cnt").and_then(|v| v.as_i64()),
            Some(2)
        );
    }

    /// A genuine `i64::MIN` Long dimension value must key and emit as the
    /// NUMBER -9223372036854775808 on the vectorized path — matching the
    /// row-oriented path — and must stay a SEPARATE group from rows whose
    /// dim column is simply missing (short column), which emit null
    /// (re-audit Low 2026-07-19: the i64::MIN missing-row sentinel
    /// conflated the two, so the same query emitted null or the number
    /// depending on which internal path executed).
    #[test]
    fn groupby_long_min_dimension_value_distinct_from_missing_rows() {
        let mut columns = std::collections::HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![100, 200, 300]));
        // Rows 0-1 hold the genuine boundary value; row 2 is MISSING
        // (column shorter than the segment).
        columns.insert(
            "code".to_string(),
            ColumnData::Long(vec![i64::MIN, i64::MIN]),
        );
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
        let make = |virtual_columns: Option<Vec<VirtualColumnSpec>>| GroupByQuery {
            data_source: DataSource::Table { name: "t".into() },
            intervals: vec!["1970-01-01T00:00:00Z/1970-01-01T00:00:01Z".into()],
            granularity: GranularitySpec::Simple("all".into()),
            dimensions: vec![ferrodruid_common::types::DimensionSpec::Default {
                dimension: "code".into(),
                output_name: "code".into(),
                output_type: ColumnType::Long,
            }],
            filter: None,
            virtual_columns,
            aggregations: vec![AggregatorSpec::Count { name: "cnt".into() }],
            post_aggregations: None,
            subtotals_spec: None,
            having: None,
            limit_spec: None,
            context: None,
        };
        // No VC → vectorized path; an unused constant VC forces the
        // row-oriented path (vectorized bails on any virtual column).
        let vectorized = make(None).execute(&segment).expect("vectorized path");
        let unused_vc: VirtualColumnSpec = serde_json::from_value(serde_json::json!({
            "type": "expression", "name": "v_unused", "expression": "1 + 1"
        }))
        .expect("vc");
        let row_path = make(Some(vec![unused_vc]))
            .execute(&segment)
            .expect("row-oriented path");

        let render = |rows: &[GroupByResult]| -> Vec<(Option<i64>, Option<i64>)> {
            let mut out: Vec<(Option<i64>, Option<i64>)> = rows
                .iter()
                .map(|r| {
                    (
                        r.event.get("code").and_then(serde_json::Value::as_i64),
                        r.event.get("cnt").and_then(serde_json::Value::as_i64),
                    )
                })
                .collect();
            out.sort();
            out
        };
        let expected = vec![
            (None, Some(1)),           // missing row → null group
            (Some(i64::MIN), Some(2)), // genuine boundary value → NUMBER group
        ];
        assert_eq!(
            render(&vectorized),
            expected,
            "vectorized path must keep genuine i64::MIN distinct from missing rows"
        );
        assert_eq!(
            render(&row_path),
            expected,
            "row-oriented path parity: same two groups"
        );
        // The genuine-MIN group's key must be the JSON NUMBER, not null.
        let min_group = vectorized
            .iter()
            .find(|r| r.event.get("code") == Some(&serde_json::json!(i64::MIN)))
            .expect("genuine i64::MIN emits as a number on the vectorized path");
        assert_eq!(min_group.event.get("cnt").and_then(|v| v.as_i64()), Some(2));
    }

    /// DD R10 A#7: a non-finite post-agg result (here +Inf from overflowing
    /// multiplication) must finalize to JSON null, not a silent 0.
    #[test]
    fn groupby_non_finite_post_agg_emits_null() {
        use ferrodruid_aggregator::PostAggregatorSpec;

        let segment = build_long_dim_segment();
        let big = serde_json::Number::from_f64(1e308).expect("finite constant");
        let q = GroupByQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec!["1970-01-01T00:00:00Z/1970-01-01T00:00:01Z".into()],
            granularity: GranularitySpec::Simple("all".into()),
            dimensions: vec![ferrodruid_common::types::DimensionSpec::Default {
                dimension: "code".into(),
                output_name: "code".into(),
                output_type: ColumnType::Long,
            }],
            filter: None,
            virtual_columns: None,
            aggregations: vec![AggregatorSpec::Count { name: "cnt".into() }],
            // 1e308 * 1e308 overflows f64 to +Inf.
            post_aggregations: Some(vec![PostAggregatorSpec::Arithmetic {
                name: "overflow".into(),
                fn_name: "*".into(),
                fields: vec![
                    PostAggregatorSpec::Constant {
                        name: "a".into(),
                        value: big.clone(),
                    },
                    PostAggregatorSpec::Constant {
                        name: "b".into(),
                        value: big,
                    },
                ],
            }]),
            subtotals_spec: None,
            having: None,
            limit_spec: None,
            context: None,
        };
        let results = q.execute(&segment).expect("execute");
        assert!(!results.is_empty());
        for r in &results {
            let v = r.event.get("overflow").expect("overflow field present");
            assert_eq!(
                v,
                &serde_json::Value::Null,
                "non-finite post-agg must be null, not {v}"
            );
        }
    }

    /// Build a segment with a String `city` dim, a String `kind` dim and a
    /// Double `val` metric for the subtotals / virtual-column tests.
    fn build_subtotal_segment() -> SegmentData {
        use ferrodruid_segment::SegmentDataBuilder;
        // 4 rows:
        //   (Tokyo, A, 10) (Tokyo, B, 20) (Osaka, A, 30) (Osaka, A, 40)
        SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200, 300, 400])
            .add_string_column(
                "city",
                vec![
                    "Tokyo".into(),
                    "Tokyo".into(),
                    "Osaka".into(),
                    "Osaka".into(),
                ],
            )
            .add_string_column("kind", vec!["A".into(), "B".into(), "A".into(), "A".into()])
            .add_double_column("val", true, vec![10.0, 20.0, 30.0, 40.0])
            .build()
            .expect("build subtotal segment")
    }

    fn parse_groupby(json: &str) -> GroupByQuery {
        serde_json::from_str(json).expect("parse groupBy query")
    }

    /// A virtual column must be computable per-row and usable both as a
    /// group dimension and as an aggregator `fieldName`.
    #[test]
    fn virtual_column_usable_in_dimension_and_aggregator() {
        let segment = build_subtotal_segment();
        // `val2 = val * 2`, group by city, sum val2.
        // Tokyo: (10+20)*2 = 60 ; Osaka: (30+40)*2 = 140.
        let q = parse_groupby(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": ["city"],
                "virtualColumns": [
                    {"type":"expression","name":"val2","expression":"val * 2"}
                ],
                "aggregations": [
                    {"type":"doubleSum","name":"sum_val2","fieldName":"val2"}
                ]
            }"#,
        );
        let results = q.execute(&segment).expect("execute");
        let mut by_city: HashMap<String, f64> = HashMap::new();
        for r in &results {
            let city = r.event.get("city").and_then(|v| v.as_str()).unwrap_or("");
            let sum = r
                .event
                .get("sum_val2")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            by_city.insert(city.to_string(), sum);
        }
        assert_eq!(by_city.get("Tokyo"), Some(&60.0));
        assert_eq!(by_city.get("Osaka"), Some(&140.0));

        // Group BY the virtual column itself.
        let q2 = parse_groupby(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": ["val2"],
                "virtualColumns": [
                    {"type":"expression","name":"val2","expression":"val * 2"}
                ],
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        );
        let results2 = q2.execute(&segment).expect("execute");
        // val2 distinct values: 20, 40, 60, 80 → 4 groups, each count 1.
        assert_eq!(results2.len(), 4);
        for r in &results2 {
            assert_eq!(r.event.get("cnt").and_then(|v| v.as_i64()), Some(1));
            assert!(r.event.get("val2").and_then(|v| v.as_f64()).is_some());
        }
    }

    /// `subtotalsSpec` must produce a hierarchical result set per subset,
    /// nulling the omitted dimensions, including the `[]` grand total.
    #[test]
    fn subtotals_spec_hierarchical_rows_including_grand_total() {
        let segment = build_subtotal_segment();
        let q = parse_groupby(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": ["city", "kind"],
                "aggregations": [{"type":"doubleSum","name":"s","fieldName":"val"}],
                "subtotalsSpec": [["city","kind"], ["city"], []]
            }"#,
        );
        let results = q.execute(&segment).expect("execute");

        // Subset [city, kind]: (Tokyo,A)=10 (Tokyo,B)=20 (Osaka,A)=70 → 3
        // Subset [city]: Tokyo=30, Osaka=70 → 2 (kind nulled)
        // Subset []: grand total 100 → 1 (both nulled)
        // Total rows = 6.
        assert_eq!(results.len(), 6, "got {results:#?}");

        // Grand total row: city == null, kind == null, s == 100.
        let grand = results
            .iter()
            .find(|r| {
                r.event.get("city") == Some(&serde_json::Value::Null)
                    && r.event.get("kind") == Some(&serde_json::Value::Null)
            })
            .expect("grand total row present");
        assert_eq!(
            grand.event.get("s").and_then(serde_json::Value::as_f64),
            Some(100.0)
        );

        // city-only subtotal: kind nulled, two cities.
        let city_only: Vec<&GroupByResult> = results
            .iter()
            .filter(|r| {
                r.event.get("kind") == Some(&serde_json::Value::Null)
                    && r.event.get("city").is_some_and(|v| !v.is_null())
            })
            .collect();
        assert_eq!(city_only.len(), 2);
        let tokyo = city_only
            .iter()
            .find(|r| r.event.get("city").and_then(|v| v.as_str()) == Some("Tokyo"))
            .expect("Tokyo subtotal");
        assert_eq!(
            tokyo.event.get("s").and_then(serde_json::Value::as_f64),
            Some(30.0)
        );

        // Full [city, kind] grouping: 3 rows, all dims non-null.
        let full: Vec<&GroupByResult> = results
            .iter()
            .filter(|r| {
                r.event.get("city").is_some_and(|v| !v.is_null())
                    && r.event.get("kind").is_some_and(|v| !v.is_null())
            })
            .collect();
        assert_eq!(full.len(), 3);
    }

    /// `subtotalsSpec` referencing an unknown dimension errors cleanly.
    #[test]
    fn subtotals_spec_unknown_dimension_errors() {
        let segment = build_subtotal_segment();
        let q = parse_groupby(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": ["city"],
                "aggregations": [{"type":"count","name":"cnt"}],
                "subtotalsSpec": [["nope"]]
            }"#,
        );
        let err = q.execute(&segment).expect_err("must reject unknown dim");
        match err {
            DruidError::Query(msg) => assert!(msg.contains("nope"), "{msg}"),
            other => panic!("expected Query error, got {other:?}"),
        }
    }

    /// groupBy with a `variance` aggregator plus `variance` and `stddev`
    /// post-aggregators must return the finalized scalar numbers.
    ///
    /// Tokyo rows have `val` = {10, 20}.  Population variance of {10,20} is
    /// ((10-15)^2 + (20-15)^2)/2 = (25+25)/2 = 25; population stddev = 5.
    #[test]
    fn variance_and_stddev_post_aggregators_finalize() {
        let segment = build_subtotal_segment();
        let q = parse_groupby(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": ["city"],
                "filter": {"type":"selector","dimension":"city","value":"Tokyo"},
                "aggregations": [
                    {"type":"variance","name":"v","fieldName":"val"}
                ],
                "postAggregations": [
                    {"type":"variance","name":"var",
                     "field":{"type":"fieldAccess","name":"fa","fieldName":"v"}},
                    {"type":"stddev","name":"sd",
                     "field":{"type":"fieldAccess","name":"fa","fieldName":"v"}}
                ]
            }"#,
        );
        let results = q.execute(&segment).expect("execute");
        assert_eq!(results.len(), 1);
        let event = &results[0].event;
        let var = event
            .get("var")
            .and_then(serde_json::Value::as_f64)
            .expect("var present");
        let sd = event
            .get("sd")
            .and_then(serde_json::Value::as_f64)
            .expect("sd present");
        assert!(
            (var - 25.0).abs() < 1e-9,
            "population variance = 25, got {var}"
        );
        assert!((sd - 5.0).abs() < 1e-9, "population stddev = 5, got {sd}");
    }

    /// An unsupported post-aggregator must error cleanly at execute time.
    /// (`hyperUniqueCardinality`, the historical exemplar here, became
    /// implemented in W-A v1.5.0 — a malformed `expression` is the
    /// remaining validate-rejected shape.)
    #[test]
    fn groupby_unsupported_post_agg_errors() {
        let segment = build_subtotal_segment();
        let q = parse_groupby(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": ["1970-01-01T00:00:00.000Z/2099-01-01T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": ["city"],
                "aggregations": [{"type":"count","name":"cnt"}],
                "postAggregations": [
                    {"type":"expression","name":"bad","expression":"concat(("}
                ]
            }"#,
        );
        let err = q.execute(&segment).expect_err("must reject");
        match err {
            DruidError::Query(_) => {}
            other => panic!("expected Query error, got {other:?}"),
        }
    }

    /// W1-J finding-C: Druid / Calcite default ordering puts nulls
    /// FIRST in ASC sorts (and therefore LAST in DESC).  Before the
    /// fix the lex `to_string()` fallback stringified `null` to
    /// `"null"` and sorted GROUPING SETS / CUBE / ROLLUP null-groups
    /// in lex-of-"null" position, diverging from Druid on every
    /// harness query that touches CUBE / ROLLUP / GROUPING SETS.
    ///
    /// This regression test drives the LimitSpec sort path directly
    /// with a mixed null+non-null `city` column and asserts the null
    /// row comes first in ASC and last in DESC.
    #[test]
    fn limit_spec_sort_puts_nulls_first_in_asc() {
        // Hand-build results that look like a CUBE / ROLLUP output
        // (one null-group + several non-null groups).
        let mut results: Vec<GroupByResult> = vec![
            GroupByResult {
                version: "v1".to_string(),
                timestamp: "1970-01-01T00:00:00.000Z".to_string(),
                event: serde_json::json!({"city": "Hauptseite", "cnt": 1})
                    .as_object()
                    .expect("obj")
                    .clone(),
            },
            GroupByResult {
                version: "v1".to_string(),
                timestamp: "1970-01-01T00:00:00.000Z".to_string(),
                event: serde_json::json!({"city": serde_json::Value::Null, "cnt": 10})
                    .as_object()
                    .expect("obj")
                    .clone(),
            },
            GroupByResult {
                version: "v1".to_string(),
                timestamp: "1970-01-01T00:00:00.000Z".to_string(),
                event: serde_json::json!({"city": "Tokyo", "cnt": 4})
                    .as_object()
                    .expect("obj")
                    .clone(),
            },
        ];

        // Drive THE LimitSpec sort path (`sort_by_order_columns` — since
        // W-C S3 the extracted comparator shared with the broker merge)
        // for an ASC string dimension, so a future regression in the
        // comparator is immediately visible.
        let col_spec = OrderByColumnSpec {
            dimension: "city".to_string(),
            direction: Some("ascending".to_string()),
            dimension_order: Some("lexicographic".to_string()),
        };
        sort_by_order_columns(&mut results, std::slice::from_ref(&col_spec));

        // Order MUST be: null, Hauptseite, Tokyo.  The pre-fix
        // comparator returned [Hauptseite, Tokyo, null] because
        // `"null"` > `"Tokyo"` lex.
        let cities: Vec<Option<&str>> = results
            .iter()
            .map(|r| r.event.get("city").and_then(|v| v.as_str()))
            .collect();
        assert_eq!(
            cities,
            vec![None, Some("Hauptseite"), Some("Tokyo")],
            "ASC sort must put nulls first (Druid / Calcite default)"
        );
    }
}
