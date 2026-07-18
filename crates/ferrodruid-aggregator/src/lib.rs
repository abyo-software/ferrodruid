// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid-compatible aggregation engine (count, sum, min, max, first/last, filtered, post-aggregators).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod bloom;
mod cardinality;
mod count;
mod filtered;
mod first_last;
mod grouping;
mod minmax;
mod post_agg;
mod postagg_expr;
mod sketch;
mod string_agg;
mod sum;
mod variance;

pub use cardinality::{
    CARDINALITY_CROSS_SHARD_MERGE_LIMIT_KIND, CARDINALITY_EXACT_SET_LIMIT_KIND,
    CARDINALITY_MALFORMED_STATE_KIND, CARDINALITY_STATE_TAG, CardinalityAggregator,
    CardinalityState, MalformedCardinalityState, exact_cardinality_partial,
    exact_cardinality_set_cap, set_exact_cardinality_cap_for_tests,
};
pub use count::CountAggregator;
pub use filtered::FilteredAggregator;
pub use first_last::{
    DoubleFirstAggregator, DoubleLastAggregator, FloatFirstAggregator, FloatLastAggregator,
    LongFirstAggregator, LongLastAggregator, StringFirstAggregator, StringLastAggregator,
};
pub use minmax::{
    DoubleMaxAggregator, DoubleMinAggregator, FloatMaxAggregator, FloatMinAggregator,
    LongMaxAggregator, LongMinAggregator,
};
pub use sketch::{
    HLL_SKETCH_TAG, HllSketchAggregator, QUANTILES_SKETCH_TAG, QuantilesSketchAggregator,
    THETA_SKETCH_TAG, ThetaSketchAggregator, finalize_sketch_json, merge_sketch_json,
    theta_sketch_envelope,
};
pub use sum::{DoubleSumAggregator, FloatSumAggregator, LongSumAggregator};
pub use variance::{VarianceAggregator, VarianceEstimator, merge_variance_json};

pub use bloom::{
    BLOOM_FILTER_TAG, BloomFilter, BloomFilterAggregator, decode_bloom_filter, encode_bloom_filter,
};
pub use grouping::GroupingAggregator;
pub use string_agg::{
    ArrayAggAggregator, DEFAULT_ARRAY_AGG_LIMIT, DEFAULT_STRING_AGG_BYTE_LIMIT, StringAggAggregator,
};

use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;

// ---------------------------------------------------------------------------
// Aggregator trait
// ---------------------------------------------------------------------------

/// Why an aggregator's finalized [`Aggregator::get`] value would be inexact:
/// a resource cap clipped its internal state (e.g. the exact-cardinality
/// set hit [`MAX_CARDINALITY_SET_SIZE`]).
///
/// Fail-closed (2026-07-11): finalization layers (the query executors)
/// consult [`Aggregator::saturation`] before reading `get()` and FAIL the
/// query with `DruidError::ResourceLimit` when this is `Some`, instead of
/// returning a silently under-counted scalar — matching Druid, which never
/// silently returns a wrong exact-distinct count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregatorSaturation {
    /// Which limit fired. Used verbatim as `DruidError::ResourceLimit::kind`
    /// and therefore surfaced in the client-visible error message, so it
    /// names both the limit and the recommended remedy.
    pub kind: &'static str,
    /// The configured upper bound that fired.
    pub limit: usize,
    /// The observed state size at finalization (>= `limit` once saturated;
    /// the true value is strictly larger but is unknown past the cap).
    pub observed: usize,
}

/// A Druid aggregator that accumulates values over rows.
pub trait Aggregator: Send + Sync + fmt::Debug {
    /// Process a single row value. `value` is JSON-compatible.
    fn aggregate(&mut self, value: Option<&serde_json::Value>);

    /// Process a single row value alongside an explicit `timestamp`.
    ///
    /// Default implementation discards `timestamp` and forwards to
    /// [`Aggregator::aggregate`].  Aggregators whose semantics depend on a
    /// per-row timestamp (first/last by time, including the W1-H CL-4 R6
    /// non-`__time` time column variant) override this to honour the
    /// timestamp directly rather than relying on insertion order.
    ///
    /// CL-4 / W1-H R6 (`AggregatorSpec::DoubleFirst { time_column: Some(_) }`
    /// et al.): the query layer extracts the time-column value from the
    /// row map and dispatches through this method so first/last
    /// aggregators that name a non-`__time` time column see the actual
    /// timestamp values instead of a synthetic insertion-order counter.
    fn aggregate_with_time(&mut self, _timestamp: i64, value: Option<&serde_json::Value>) {
        self.aggregate(value);
    }

    /// Process all configured field values for a single row.
    ///
    /// The default implementation forwards each value through
    /// [`Aggregator::aggregate`] in order, which matches the historical
    /// "one value per row" contract for all single-field aggregators.
    /// Aggregators whose semantics depend on knowing the *whole* row
    /// (currently just [`crate::CardinalityAggregator`], which supports
    /// multi-field union and tuple modes) override this method.
    ///
    /// Wave 45-E (Wave 37B Medium #3 `aggregator/lib.rs:183-191`,
    /// `cardinality.rs:40-67`): the query layer dispatches per-row
    /// aggregation through this method so multi-field cardinality specs
    /// honour the spec instead of silently discarding extra fields.
    fn aggregate_multi(&mut self, values: &[Option<&serde_json::Value>]) {
        for v in values {
            self.aggregate(*v);
        }
    }

    /// Get the current aggregated result.
    fn get(&self) -> serde_json::Value;

    /// Report whether a resource cap has made this aggregator's
    /// [`get`](Aggregator::get) value inexact.
    ///
    /// The default implementation returns `None` ("the value is exact").
    /// Capped aggregators (currently [`CardinalityAggregator`]) override
    /// this; wrappers ([`FilteredAggregator`]) delegate to their inner
    /// aggregator. Finalization layers fail closed on `Some` — see
    /// [`AggregatorSaturation`].
    fn saturation(&self) -> Option<AggregatorSaturation> {
        None
    }

    /// Merge another aggregator's state (for scatter/gather).
    fn merge(&mut self, other: &dyn Aggregator);

    /// Reset state for reuse.
    fn reset(&mut self);

    /// Clone as boxed trait object.
    fn clone_box(&self) -> Box<dyn Aggregator>;

    /// Downcast support so that aggregators with rich internal state (e.g.
    /// the [`CardinalityAggregator`] backing `HashSet`) can be merged
    /// without going through their lossy [`Aggregator::get`] JSON form.
    ///
    /// Default implementation returns `None`; concrete aggregators that
    /// support typed merge should override.
    fn as_any(&self) -> Option<&dyn Any> {
        None
    }
}

impl Clone for Box<dyn Aggregator> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// ---------------------------------------------------------------------------
// JSON-level merge dispatcher (broker scatter/gather)
// ---------------------------------------------------------------------------

/// Merge two aggregator results in their JSON form according to the
/// supplied [`AggregatorSpec`] kind.
///
/// The broker collects per-shard partials as JSON; this helper dispatches
/// to the correct merge rule (sum / min / max / first-wins / last-wins /
/// cardinality-additive) instead of blindly summing every numeric field.
///
/// Wave 36-G2 (Wave 37B High `broker/lib.rs:462-483`): the previous
/// `merge_agg_maps` used `dst + src` for any numeric pair, corrupting
/// min/max/first/last/avg/cardinality results across shards.
#[must_use]
pub fn merge_json_by_spec(
    spec: &AggregatorSpec,
    dst: &serde_json::Value,
    src: &serde_json::Value,
) -> serde_json::Value {
    match spec {
        AggregatorSpec::Count { .. }
        | AggregatorSpec::LongSum { .. }
        | AggregatorSpec::DoubleSum { .. }
        | AggregatorSpec::FloatSum { .. } => merge_sum(dst, src),

        AggregatorSpec::LongMin { .. }
        | AggregatorSpec::DoubleMin { .. }
        | AggregatorSpec::FloatMin { .. } => merge_min(dst, src),

        AggregatorSpec::LongMax { .. }
        | AggregatorSpec::DoubleMax { .. }
        | AggregatorSpec::FloatMax { .. } => merge_max(dst, src),

        // First/Last across shards is ambiguous in the broker layer because
        // the published JSON shape is a bare value with no timestamp. The
        // closest safe semantics: first-wins keeps the existing value
        // (idempotent); last-wins takes the new value. This matches
        // single-binary execution where shards emit results in a stable
        // order. A timestamp-aware shape would require a wire change and
        // is tracked separately.
        AggregatorSpec::LongFirst { .. }
        | AggregatorSpec::DoubleFirst { .. }
        | AggregatorSpec::FloatFirst { .. }
        | AggregatorSpec::StringFirst { .. } => merge_first(dst, src),

        AggregatorSpec::LongLast { .. }
        | AggregatorSpec::DoubleLast { .. }
        | AggregatorSpec::FloatLast { .. }
        | AggregatorSpec::StringLast { .. } => merge_last(dst, src),

        AggregatorSpec::Cardinality { .. } => merge_cardinality(dst, src),

        AggregatorSpec::Variance { .. } => variance::merge_variance_json(dst, src),

        AggregatorSpec::HllSketchBuild { .. }
        | AggregatorSpec::HllSketchMerge { .. }
        | AggregatorSpec::ThetaSketch { .. }
        | AggregatorSpec::QuantilesDoublesSketch { .. } => sketch::merge_sketch_json(dst, src),

        AggregatorSpec::ArrayAgg { distinct, .. } => {
            string_agg::merge_array_agg_json(*distinct, dst, src)
        }
        AggregatorSpec::StringAgg { separator, .. } => {
            string_agg::merge_string_agg_json(separator, dst, src)
        }
        AggregatorSpec::BloomFilter { .. } => bloom::merge_bloom_json(dst, src),
        AggregatorSpec::Grouping { .. } => {
            // Grouping bitmasks are deterministic per-row given the active
            // subtotal set; cross-shard merge picks the source value if the
            // destination is null (defensive — both sides should produce the
            // same bitmask for the same grouping anyway).
            if dst.is_null() {
                src.clone()
            } else {
                dst.clone()
            }
        }

        AggregatorSpec::Filtered { aggregator, .. } => merge_json_by_spec(aggregator, dst, src),
    }
}

fn merge_sum(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    match (dst.as_f64(), src.as_f64()) {
        (Some(d), Some(s)) => {
            let sum = d + s;
            let dst_is_int = dst.is_i64() || dst.is_u64();
            let src_is_int = src.is_i64() || src.is_u64();
            if dst_is_int && src_is_int && sum == sum.trunc() {
                #[allow(clippy::cast_possible_truncation)]
                let as_int = sum as i64;
                serde_json::json!(as_int)
            } else {
                serde_json::json!(sum)
            }
        }
        (None, Some(_)) => src.clone(),
        _ => dst.clone(),
    }
}

fn merge_min(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    match (dst.as_f64(), src.as_f64()) {
        (Some(d), Some(s)) => {
            if s < d {
                src.clone()
            } else {
                dst.clone()
            }
        }
        (None, Some(_)) => src.clone(),
        _ => dst.clone(),
    }
}

fn merge_max(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    match (dst.as_f64(), src.as_f64()) {
        (Some(d), Some(s)) => {
            if s > d {
                src.clone()
            } else {
                dst.clone()
            }
        }
        (None, Some(_)) => src.clone(),
        _ => dst.clone(),
    }
}

fn merge_first(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    if dst.is_null() {
        src.clone()
    } else {
        dst.clone()
    }
}

fn merge_last(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    if src.is_null() {
        dst.clone()
    } else {
        src.clone()
    }
}

fn merge_cardinality(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    use cardinality::CardinalityState;

    // Wave 40-B (Wave 39 [High] [NEW-VARIANT]): when *either* side is
    // shipped as a typed [`CardinalityState`] envelope, perform a true
    // HashSet union and re-emit a state envelope.  This closes the
    // overlapping-shard over-count that the previous saturating-add did.
    //
    // Untrusted-peer hardening (2026-07-12, Codex HIGH findings 1+4+5):
    // partials are hostile input.  Each side classifies as one of
    //
    // * `State`  — a VALIDATED envelope (`from_json` checks the count/
    //   values/saturated invariants and the wire bounds);
    // * `Count`  — a bare u64 count (legacy / zero-identity wire shape);
    // * `Poison` — tagged as an envelope but malformed or invariant-
    //   violating, or a bare value that is neither an envelope nor a u64.
    //
    // A poisoned side must never be dropped (the old code mapped it to a
    // bare 0 and silently LOST the shard) nor trusted (its count may be
    // forged): the merge degrades to a *saturated* envelope carrying only
    // the counts of the trusted sides, and the broker finalization pass
    // fails the query closed.
    //
    // Merge rules for well-formed sides:
    //
    // * `state ⊕ state`  -> `CardinalityState::union` (exact when both
    //   non-saturated, saturating-add when either saturated).
    // * `state ⊕ count`  -> a zero on either side (bare 0, or a non-
    //   saturated empty envelope) is the exact identity; otherwise the
    //   count side upgrades to a saturated state and the union degrades
    //   (a bare count carries no per-key information, so the merge has to
    //   assume worst-case overlap).
    // * `count ⊕ count`  -> saturating-add of counts, emitted as a
    //   *saturated* state envelope (fail-closed 2026-07-11: the sum is
    //   only an upper bound; a zero side is the exact identity and passes
    //   the other side through).
    //
    // Any caller that wants exact union must emit `CardinalityState`
    // envelopes (see [`CardinalityAggregator::into_state`]).
    enum Side {
        State(CardinalityState),
        Count(u64),
        Poison,
    }
    fn classify(v: &serde_json::Value) -> Side {
        match CardinalityState::from_json(v) {
            Ok(Some(state)) => Side::State(state),
            Ok(None) => match v.as_u64() {
                Some(n) => Side::Count(n),
                None => Side::Poison,
            },
            Err(_) => Side::Poison,
        }
    }
    fn is_exact_empty(s: &CardinalityState) -> bool {
        !s.saturated && s.count == 0 && s.values.is_empty()
    }

    match (classify(dst), classify(src)) {
        (Side::State(d), Side::State(s)) => CardinalityState::union(&d, &s).to_json(),
        (Side::State(d), Side::Count(n)) => {
            if n == 0 {
                d.to_json()
            } else if is_exact_empty(&d) {
                src.clone()
            } else {
                CardinalityState::union(&d, &CardinalityState::saturated_with_count(n)).to_json()
            }
        }
        (Side::Count(n), Side::State(s)) => {
            if n == 0 {
                s.to_json()
            } else if is_exact_empty(&s) {
                dst.clone()
            } else {
                CardinalityState::union(&CardinalityState::saturated_with_count(n), &s).to_json()
            }
        }
        (Side::Count(d), Side::Count(s)) => {
            if d == 0 {
                src.clone()
            } else if s == 0 {
                dst.clone()
            } else {
                CardinalityState::saturated_with_count(d.saturating_add(s)).to_json()
            }
        }
        // Poison: fail closed.  Only trusted sides contribute to the
        // carried (inexact) bound; the saturated flag is what makes the
        // broker reject the query.
        (Side::Poison, Side::Poison) => CardinalityState::saturated_with_count(0).to_json(),
        (Side::Poison, Side::State(t)) | (Side::State(t), Side::Poison) => {
            CardinalityState::saturated_with_count(t.count).to_json()
        }
        (Side::Poison, Side::Count(n)) | (Side::Count(n), Side::Poison) => {
            CardinalityState::saturated_with_count(n).to_json()
        }
    }
}

// ---------------------------------------------------------------------------
// AggregatorSpec
// ---------------------------------------------------------------------------

/// JSON-deserializable aggregator specification matching the Druid Native Query format.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum AggregatorSpec {
    /// Counts the number of rows.
    Count {
        /// Output name for this aggregation.
        name: String,
    },
    /// Computes the sum of a long (i64) column.
    LongSum {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Computes the sum of a double (f64) column.
    DoubleSum {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Computes the sum of a float (f32) column.
    FloatSum {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Computes the minimum of a long (i64) column.
    LongMin {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Computes the maximum of a long (i64) column.
    LongMax {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Computes the minimum of a double (f64) column.
    DoubleMin {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Computes the maximum of a double (f64) column.
    DoubleMax {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Computes the minimum of a float (f32) column.
    FloatMin {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Computes the maximum of a float (f32) column.
    FloatMax {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Returns the first long (i64) value by timestamp.
    LongFirst {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Optional non-`__time` time column to order by (CL-4 / W1-H R6).
        /// `None` keeps the historical `__time` / insertion-order semantics.
        #[serde(
            default,
            rename = "timeColumn",
            skip_serializing_if = "Option::is_none"
        )]
        time_column: Option<String>,
    },
    /// Returns the last long (i64) value by timestamp.
    LongLast {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Optional non-`__time` time column to order by (CL-4 / W1-H R6).
        #[serde(
            default,
            rename = "timeColumn",
            skip_serializing_if = "Option::is_none"
        )]
        time_column: Option<String>,
    },
    /// Returns the first double (f64) value by timestamp.
    DoubleFirst {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Optional non-`__time` time column to order by (CL-4 / W1-H R6).
        #[serde(
            default,
            rename = "timeColumn",
            skip_serializing_if = "Option::is_none"
        )]
        time_column: Option<String>,
    },
    /// Returns the last double (f64) value by timestamp.
    DoubleLast {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Optional non-`__time` time column to order by (CL-4 / W1-H R6).
        #[serde(
            default,
            rename = "timeColumn",
            skip_serializing_if = "Option::is_none"
        )]
        time_column: Option<String>,
    },
    /// Returns the first float (f32) value by timestamp.
    FloatFirst {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Optional non-`__time` time column to order by (CL-4 / W1-H R6).
        #[serde(
            default,
            rename = "timeColumn",
            skip_serializing_if = "Option::is_none"
        )]
        time_column: Option<String>,
    },
    /// Returns the last float (f32) value by timestamp.
    FloatLast {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Optional non-`__time` time column to order by (CL-4 / W1-H R6).
        #[serde(
            default,
            rename = "timeColumn",
            skip_serializing_if = "Option::is_none"
        )]
        time_column: Option<String>,
    },
    /// Returns the first string value by timestamp.
    StringFirst {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Maximum byte length for the stored string.
        #[serde(default, rename = "maxStringBytes")]
        max_string_bytes: Option<usize>,
        /// Optional non-`__time` time column to order by (CL-4 / W1-H R6).
        #[serde(
            default,
            rename = "timeColumn",
            skip_serializing_if = "Option::is_none"
        )]
        time_column: Option<String>,
    },
    /// Returns the last string value by timestamp.
    StringLast {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Maximum byte length for the stored string.
        #[serde(default, rename = "maxStringBytes")]
        max_string_bytes: Option<usize>,
        /// Optional non-`__time` time column to order by (CL-4 / W1-H R6).
        #[serde(
            default,
            rename = "timeColumn",
            skip_serializing_if = "Option::is_none"
        )]
        time_column: Option<String>,
    },
    /// Computes exact cardinality (distinct count) using a HashSet.
    Cardinality {
        /// Output name for this aggregation.
        name: String,
        /// Column(s) to compute cardinality over.
        fields: Vec<String>,
        /// If true, combine field values per row into a composite key.
        #[serde(default, rename = "byRow")]
        by_row: Option<bool>,
    },
    /// Variance aggregator (Welford online algorithm).
    ///
    /// Druid's `variance` aggregator.  `estimator` selects `population`
    /// (default) or `sample`; the result of [`AggregatorSpec::create`] is a
    /// partial-state object — use a `variance` / `stddev` post-aggregator (or
    /// [`VarianceAggregator::finalize`]) to obtain the scalar.
    Variance {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Estimator selection: `"population"` (default) or `"sample"`.
        #[serde(default)]
        estimator: Option<String>,
    },
    /// `HLLSketchBuild` aggregator — build a HyperLogLog sketch from raw
    /// column values.
    #[serde(rename = "HLLSketchBuild")]
    HllSketchBuild {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Log-base-2 of the number of HLL buckets (Druid `lgK`).  Clamped to
        /// `4..=18`; default 14.
        #[serde(default, rename = "lgK")]
        lg_k: Option<u8>,
        /// Per-aggregator finalize opt-out (Druid `shouldFinalize`, P1-#3
        /// hardening): `false` keeps the INTERMEDIATE sketch on the native
        /// wire even when the query-context `finalize` is in effect (the
        /// default).  Absent / `true` follows the context default.
        #[serde(
            default,
            rename = "shouldFinalize",
            skip_serializing_if = "Option::is_none"
        )]
        should_finalize: Option<bool>,
    },
    /// `HLLSketchMerge` aggregator — merge pre-built HyperLogLog sketches.
    #[serde(rename = "HLLSketchMerge")]
    HllSketchMerge {
        /// Output name for this aggregation.
        name: String,
        /// Column carrying serialized HLL sketches.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Log-base-2 of the number of HLL buckets (Druid `lgK`).  Clamped to
        /// `4..=18`; default 14.
        #[serde(default, rename = "lgK")]
        lg_k: Option<u8>,
        /// Per-aggregator finalize opt-out (Druid `shouldFinalize`): `false`
        /// keeps the intermediate sketch even under context finalize;
        /// absent / `true` follows the context default (P1-#3 hardening).
        #[serde(
            default,
            rename = "shouldFinalize",
            skip_serializing_if = "Option::is_none"
        )]
        should_finalize: Option<bool>,
    },
    /// `thetaSketch` aggregator — build a Theta sketch from column values for
    /// set-operation cardinality.
    ThetaSketch {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Maximum number of retained hashes (Druid `size`).  Default 4096.
        #[serde(default)]
        size: Option<usize>,
        /// Per-aggregator finalize opt-out (Druid `shouldFinalize`): `false`
        /// keeps the intermediate sketch even under context finalize;
        /// absent / `true` follows the context default (P1-#3 hardening).
        #[serde(
            default,
            rename = "shouldFinalize",
            skip_serializing_if = "Option::is_none"
        )]
        should_finalize: Option<bool>,
    },
    /// `quantilesDoublesSketch` aggregator — build a quantiles (T-digest)
    /// sketch from numeric column values.
    QuantilesDoublesSketch {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Compression / accuracy budget (Druid `k`).  Default 200.
        #[serde(default, rename = "k")]
        k: Option<usize>,
        /// Per-aggregator finalize opt-out (Druid `shouldFinalize`): `false`
        /// keeps the intermediate sketch even under context finalize;
        /// absent / `true` follows the context default (P1-#3 hardening).
        #[serde(
            default,
            rename = "shouldFinalize",
            skip_serializing_if = "Option::is_none"
        )]
        should_finalize: Option<bool>,
    },
    /// `ARRAY_AGG(expr [, sizeLimit])` aggregator — collect values into an
    /// ordered array (CL-4 / W1-H R1).
    ArrayAgg {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Whether to deduplicate (DISTINCT).
        #[serde(default, rename = "distinct")]
        distinct: bool,
        /// Optional accumulator size cap (number of elements collected).
        /// Druid defaults to a maxSizeBytes-derived cap; FerroDruid honours
        /// `size_limit` directly as a per-aggregator element budget.
        #[serde(default, rename = "sizeLimit", skip_serializing_if = "Option::is_none")]
        size_limit: Option<usize>,
    },
    /// `LISTAGG(expr, sep)` / `STRING_AGG(expr, sep)` aggregator — concatenate
    /// values with a separator (CL-4 / W1-H R2 + R3).  Druid 33+ aliases
    /// `LISTAGG` to `STRING_AGG`; this spec carries the same shape for both.
    StringAgg {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Separator string (default `","`).
        #[serde(default = "default_separator")]
        separator: String,
        /// Optional accumulator size cap (number of bytes in the final
        /// concatenated string).  `None` means no cap.
        #[serde(default, rename = "sizeLimit", skip_serializing_if = "Option::is_none")]
        size_limit: Option<usize>,
    },
    /// `BLOOM_FILTER(expr, numEntries)` aggregator — build a base64-encoded
    /// bloom filter over input values (CL-4 / W1-H R4).
    ///
    /// The native wire format is a self-contained FerroDruid bloom filter
    /// envelope (see [`crate::bloom`] for the encoding).  Strict byte-eq
    /// with Apache Hive's `BloomKFilter` is a residual tracked in
    /// `docs/known-limitations.md`; FerroDruid round-trip via
    /// `BLOOM_FILTER` ↔ `BLOOM_FILTER_TEST` is guaranteed.
    BloomFilter {
        /// Output name for this aggregation.
        name: String,
        /// Column to aggregate.
        #[serde(rename = "fieldName")]
        field_name: String,
        /// Estimated number of distinct entries; sizes the underlying bit
        /// array.  Clamped to `[8, 1 << 26]` to avoid pathological inputs.
        #[serde(rename = "numEntries")]
        num_entries: u64,
    },
    /// `GROUPING(c1, c2, ...)` indicator aggregator — emits a bitmask
    /// identifying which referenced columns are *absent* from the current
    /// grouping subset (CL-4 / W1-H R7).  Bit `(n-1-i)` is set when
    /// `c_i` is being aggregated (not grouped); bit cleared when `c_i`
    /// is in the active subtotal set.  The value is finalised at row
    /// emit time by the [`crate::Aggregator`] consumer (typically the
    /// groupBy executor) which has visibility into the active set.
    Grouping {
        /// Output name for this aggregation.
        name: String,
        /// Output column names referenced by the `GROUPING(...)` call,
        /// in left-to-right argument order.
        fields: Vec<String>,
        /// The full ordered list of GROUP BY dimension output names the
        /// enclosing query carries.  The bitmask is computed against
        /// indices into this list.
        #[serde(rename = "groupByDims")]
        group_by_dims: Vec<String>,
    },
    /// Wraps another aggregator with a filter predicate.
    Filtered {
        /// The filter definition (stored as opaque JSON for now).
        filter: serde_json::Value,
        /// The inner aggregator specification.
        aggregator: Box<AggregatorSpec>,
    },
}

fn default_separator() -> String {
    ",".to_string()
}

impl AggregatorSpec {
    /// Returns the output name of this aggregation.
    pub fn name(&self) -> &str {
        match self {
            Self::Count { name }
            | Self::LongSum { name, .. }
            | Self::DoubleSum { name, .. }
            | Self::FloatSum { name, .. }
            | Self::LongMin { name, .. }
            | Self::LongMax { name, .. }
            | Self::DoubleMin { name, .. }
            | Self::DoubleMax { name, .. }
            | Self::FloatMin { name, .. }
            | Self::FloatMax { name, .. }
            | Self::LongFirst { name, .. }
            | Self::LongLast { name, .. }
            | Self::DoubleFirst { name, .. }
            | Self::DoubleLast { name, .. }
            | Self::FloatFirst { name, .. }
            | Self::FloatLast { name, .. }
            | Self::StringFirst { name, .. }
            | Self::StringLast { name, .. }
            | Self::Cardinality { name, .. }
            | Self::Variance { name, .. }
            | Self::HllSketchBuild { name, .. }
            | Self::HllSketchMerge { name, .. }
            | Self::ThetaSketch { name, .. }
            | Self::QuantilesDoublesSketch { name, .. }
            | Self::ArrayAgg { name, .. }
            | Self::StringAgg { name, .. }
            | Self::BloomFilter { name, .. }
            | Self::Grouping { name, .. } => name,
            Self::Filtered { aggregator, .. } => aggregator.name(),
        }
    }

    /// Returns the field name of this aggregation, if applicable.
    pub fn field_name(&self) -> Option<&str> {
        match self {
            Self::Count { .. } | Self::Cardinality { .. } | Self::Grouping { .. } => None,
            Self::LongSum { field_name, .. }
            | Self::DoubleSum { field_name, .. }
            | Self::FloatSum { field_name, .. }
            | Self::LongMin { field_name, .. }
            | Self::LongMax { field_name, .. }
            | Self::DoubleMin { field_name, .. }
            | Self::DoubleMax { field_name, .. }
            | Self::FloatMin { field_name, .. }
            | Self::FloatMax { field_name, .. }
            | Self::LongFirst { field_name, .. }
            | Self::LongLast { field_name, .. }
            | Self::DoubleFirst { field_name, .. }
            | Self::DoubleLast { field_name, .. }
            | Self::FloatFirst { field_name, .. }
            | Self::FloatLast { field_name, .. }
            | Self::StringFirst { field_name, .. }
            | Self::StringLast { field_name, .. }
            | Self::Variance { field_name, .. }
            | Self::HllSketchBuild { field_name, .. }
            | Self::HllSketchMerge { field_name, .. }
            | Self::ThetaSketch { field_name, .. }
            | Self::QuantilesDoublesSketch { field_name, .. }
            | Self::ArrayAgg { field_name, .. }
            | Self::StringAgg { field_name, .. }
            | Self::BloomFilter { field_name, .. } => Some(field_name),
            Self::Filtered { aggregator, .. } => aggregator.field_name(),
        }
    }

    /// Returns the non-`__time` time column this aggregator orders by, if any.
    ///
    /// Only the first / last family (CL-4 / W1-H R6 two-argument forms)
    /// returns `Some`; every other aggregator returns `None`.  The query
    /// layer uses this to extract the per-row timestamp from the row map
    /// and dispatch through [`Aggregator::aggregate_with_time`].
    pub fn time_column(&self) -> Option<&str> {
        match self {
            Self::LongFirst { time_column, .. }
            | Self::LongLast { time_column, .. }
            | Self::DoubleFirst { time_column, .. }
            | Self::DoubleLast { time_column, .. }
            | Self::FloatFirst { time_column, .. }
            | Self::FloatLast { time_column, .. }
            | Self::StringFirst { time_column, .. }
            | Self::StringLast { time_column, .. } => time_column.as_deref(),
            Self::Filtered { aggregator, .. } => aggregator.time_column(),
            _ => None,
        }
    }

    /// Returns the per-aggregator `shouldFinalize` flag (P1-#3 hardening).
    ///
    /// Druid semantics: `Some(false)` opts THIS aggregator out of
    /// finalization — it keeps its intermediate sketch even when the
    /// query-context `finalize` is in effect (the native default).
    /// `None` / `Some(true)` follow the context default.  Only the sketch
    /// aggregators carry the flag; a `filtered` wrapper delegates to its
    /// inner aggregator (Druid finalizes through the wrapper); every other
    /// aggregator returns `None`.
    pub fn should_finalize(&self) -> Option<bool> {
        match self {
            Self::HllSketchBuild {
                should_finalize, ..
            }
            | Self::HllSketchMerge {
                should_finalize, ..
            }
            | Self::ThetaSketch {
                should_finalize, ..
            }
            | Self::QuantilesDoublesSketch {
                should_finalize, ..
            } => *should_finalize,
            Self::Filtered { aggregator, .. } => aggregator.should_finalize(),
            _ => None,
        }
    }

    /// Creates a live aggregator instance from this specification.
    pub fn create(&self) -> Box<dyn Aggregator> {
        match self {
            Self::Count { .. } => Box::new(CountAggregator::new()),
            Self::LongSum { .. } => Box::new(LongSumAggregator::new()),
            Self::DoubleSum { .. } => Box::new(DoubleSumAggregator::new()),
            Self::FloatSum { .. } => Box::new(FloatSumAggregator::new()),
            Self::LongMin { .. } => Box::new(LongMinAggregator::new()),
            Self::LongMax { .. } => Box::new(LongMaxAggregator::new()),
            Self::DoubleMin { .. } => Box::new(DoubleMinAggregator::new()),
            Self::DoubleMax { .. } => Box::new(DoubleMaxAggregator::new()),
            Self::FloatMin { .. } => Box::new(FloatMinAggregator::new()),
            Self::FloatMax { .. } => Box::new(FloatMaxAggregator::new()),
            Self::LongFirst { .. } => Box::new(LongFirstAggregator::new()),
            Self::LongLast { .. } => Box::new(LongLastAggregator::new()),
            Self::DoubleFirst { .. } => Box::new(DoubleFirstAggregator::new()),
            Self::DoubleLast { .. } => Box::new(DoubleLastAggregator::new()),
            Self::FloatFirst { .. } => Box::new(FloatFirstAggregator::new()),
            Self::FloatLast { .. } => Box::new(FloatLastAggregator::new()),
            Self::StringFirst {
                max_string_bytes, ..
            } => Box::new(StringFirstAggregator::new(max_string_bytes.unwrap_or(1024))),
            Self::StringLast {
                max_string_bytes, ..
            } => Box::new(StringLastAggregator::new(max_string_bytes.unwrap_or(1024))),
            Self::ArrayAgg {
                size_limit,
                distinct,
                ..
            } => Box::new(string_agg::ArrayAggAggregator::new(
                *distinct,
                size_limit.unwrap_or(string_agg::DEFAULT_ARRAY_AGG_LIMIT),
            )),
            Self::StringAgg {
                separator,
                size_limit,
                ..
            } => Box::new(string_agg::StringAggAggregator::new(
                separator.clone(),
                size_limit.unwrap_or(string_agg::DEFAULT_STRING_AGG_BYTE_LIMIT),
            )),
            Self::BloomFilter { num_entries, .. } => {
                Box::new(bloom::BloomFilterAggregator::new(*num_entries))
            }
            Self::Grouping {
                fields,
                group_by_dims,
                ..
            } => Box::new(grouping::GroupingAggregator::new(
                fields.clone(),
                group_by_dims.clone(),
            )),
            Self::Cardinality { by_row, fields, .. } => Box::new(
                CardinalityAggregator::with_fields(by_row.unwrap_or(false), fields.clone()),
            ),
            Self::Variance { estimator, .. } => Box::new(VarianceAggregator::new(
                VarianceEstimator::from_opt(estimator.as_deref()),
            )),
            Self::HllSketchBuild { lg_k, .. } => {
                Box::new(HllSketchAggregator::build(lg_k.unwrap_or(14)))
            }
            Self::HllSketchMerge { lg_k, .. } => {
                Box::new(HllSketchAggregator::merge(lg_k.unwrap_or(14)))
            }
            Self::ThetaSketch { size, .. } => {
                Box::new(ThetaSketchAggregator::new(size.unwrap_or(4096)))
            }
            Self::QuantilesDoublesSketch { k, .. } => {
                Box::new(QuantilesSketchAggregator::new(k.unwrap_or(200)))
            }
            Self::Filtered {
                filter, aggregator, ..
            } => Box::new(FilteredAggregator::new(filter.clone(), aggregator.create())),
        }
    }
}

// ---------------------------------------------------------------------------
// PostAggregatorSpec
// ---------------------------------------------------------------------------

/// JSON-deserializable post-aggregator specification.
///
/// Post-aggregators compute derived values from the results of aggregators.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum PostAggregatorSpec {
    /// Arithmetic computation on sub-post-aggregators.
    Arithmetic {
        /// Output name.
        name: String,
        /// Operation: `+`, `-`, `*`, `/`, or `quotient`.
        #[serde(rename = "fn")]
        fn_name: String,
        /// Operand post-aggregators.
        fields: Vec<PostAggregatorSpec>,
    },
    /// Access the result of a named aggregator.
    FieldAccess {
        /// Output name.
        name: String,
        /// Name of the aggregator whose result to access.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// A constant numeric value.
    Constant {
        /// Output name.
        name: String,
        /// The constant value.
        value: serde_json::Number,
    },
    /// An expression-based post-aggregator.
    #[serde(rename = "expression")]
    Expression {
        /// Output name.
        name: String,
        /// The expression string.
        expression: String,
    },
    /// HyperUnique cardinality post-aggregator.
    HyperUniqueCardinality {
        /// Output name.
        name: String,
        /// Name of the HyperUnique aggregator field.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// `thetaSketchEstimate` — distinct-count estimate of a theta sketch
    /// produced by a `thetaSketch` aggregator.
    ThetaSketchEstimate {
        /// Output name.
        name: String,
        /// Field-access referencing the theta-sketch aggregator's output.
        field: Box<PostAggregatorSpec>,
    },
    /// `HLLSketchEstimate` — distinct-count estimate of an HLL sketch produced
    /// by an `HLLSketchBuild` / `HLLSketchMerge` aggregator.
    #[serde(rename = "HLLSketchEstimate")]
    HllSketchEstimate {
        /// Output name.
        name: String,
        /// Field-access referencing the HLL-sketch aggregator's output.
        field: Box<PostAggregatorSpec>,
        /// When `Some(true)`, round the estimate to the nearest integer
        /// (Druid's `"round": true`; used by SQL `APPROX_COUNT_DISTINCT` /
        /// `COUNT(DISTINCT)` whose result type is BIGINT).  Absent on the
        /// wire when unset so serialization of pre-existing specs is
        /// byte-identical.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        round: Option<bool>,
    },
    /// `quantilesDoublesSketchToQuantile` — read a single quantile out of a
    /// quantiles (T-digest) sketch.
    QuantilesDoublesSketchToQuantile {
        /// Output name.
        name: String,
        /// Field-access referencing the quantiles-sketch aggregator's output.
        field: Box<PostAggregatorSpec>,
        /// Fraction in `[0, 1]` to read from the sketch.
        fraction: f64,
    },
    /// `variance` post-aggregator — finalize a `variance` aggregator's partial
    /// state to the scalar variance per its configured estimator.
    Variance {
        /// Output name.
        name: String,
        /// Field-access referencing the variance aggregator's output.
        field: Box<PostAggregatorSpec>,
    },
    /// `stddev` post-aggregator — square root of a `variance` aggregator's
    /// finalized variance.
    Stddev {
        /// Output name.
        name: String,
        /// Field-access referencing the variance aggregator's output.
        field: Box<PostAggregatorSpec>,
    },
}

impl PostAggregatorSpec {
    /// Returns the output name of this post-aggregation.
    pub fn name(&self) -> &str {
        match self {
            Self::Arithmetic { name, .. }
            | Self::FieldAccess { name, .. }
            | Self::Constant { name, .. }
            | Self::Expression { name, .. }
            | Self::HyperUniqueCardinality { name, .. }
            | Self::ThetaSketchEstimate { name, .. }
            | Self::HllSketchEstimate { name, .. }
            | Self::QuantilesDoublesSketchToQuantile { name, .. }
            | Self::Variance { name, .. }
            | Self::Stddev { name, .. } => name,
        }
    }

    /// Validate that this post-aggregator (and any nested children) is in
    /// a supported, evaluatable form.  Returns an explicit error for
    /// variants whose runtime evaluation is not yet implemented, instead
    /// of silently producing `None` at query time.
    ///
    /// Wave 45-B closure of Wave 37B `aggregator` Medium #5 (Codex
    /// `lib.rs:320-333,411-415`): `Expression` and `HyperUniqueCardinality`
    /// were accepted on the wire but `evaluate()` returned `None`, so a
    /// query that referenced them silently dropped the derived field
    /// from results.  Callers should run `validate_supported()` at
    /// query-spec parse time and propagate the error to the client.
    /// `Expression` is now evaluatable (see the `postagg_expr` module docs
    /// for the supported grammar subset); validation parses the expression up
    /// front so malformed or unsupported expressions are rejected loudly at
    /// query-spec parse time rather than yielding `None` per row.
    ///
    /// # Errors
    ///
    /// Returns [`ferrodruid_common::DruidError::Query`] when a sub-tree
    /// uses `HyperUniqueCardinality` (not yet wired through the aggregator
    /// engine), or an `Expression` whose expression string does not parse
    /// under the supported grammar subset.
    pub fn validate_supported(&self) -> ferrodruid_common::Result<()> {
        match self {
            Self::FieldAccess { .. } | Self::Constant { .. } => Ok(()),
            Self::Arithmetic { fields, .. } => {
                for f in fields {
                    f.validate_supported()?;
                }
                Ok(())
            }
            Self::ThetaSketchEstimate { field, .. }
            | Self::HllSketchEstimate { field, .. }
            | Self::QuantilesDoublesSketchToQuantile { field, .. }
            | Self::Variance { field, .. }
            | Self::Stddev { field, .. } => field.validate_supported(),
            Self::Expression { name, expression } => match postagg_expr::parse(expression) {
                Ok(_) => Ok(()),
                Err(e) => Err(ferrodruid_common::DruidError::Query(format!(
                    "post-aggregator '{name}': unsupported or malformed expression \
                     '{expression}': {e}"
                ))),
            },
            Self::HyperUniqueCardinality { name, .. } => {
                Err(ferrodruid_common::DruidError::Query(format!(
                    "post-aggregator '{name}' uses type 'hyperUniqueCardinality' which is not \
                     yet implemented; evaluation would silently produce a missing result"
                )))
            }
        }
    }

    /// Evaluates this post-aggregator against a map of aggregator results.
    ///
    /// Returns `None` if the evaluation cannot produce a numeric result.
    pub fn evaluate(&self, agg_results: &HashMap<String, serde_json::Value>) -> Option<f64> {
        match self {
            Self::FieldAccess { field_name, .. } => {
                let val = agg_results.get(field_name)?;
                value_to_f64(val)
            }
            Self::Constant { value, .. } => value.as_f64(),
            Self::Arithmetic {
                fn_name, fields, ..
            } => {
                if fields.is_empty() {
                    return Some(0.0);
                }
                let mut values = Vec::with_capacity(fields.len());
                for f in fields {
                    values.push(f.evaluate(agg_results)?);
                }
                let result = match fn_name.as_str() {
                    "+" => values.iter().sum(),
                    "-" => {
                        let first = values[0];
                        first - values[1..].iter().sum::<f64>()
                    }
                    "*" => values.iter().product(),
                    "/" => {
                        let mut acc = values[0];
                        for &v in &values[1..] {
                            if v == 0.0 {
                                return Some(0.0);
                            }
                            acc /= v;
                        }
                        acc
                    }
                    "quotient" => {
                        let mut acc = values[0];
                        for &v in &values[1..] {
                            if v == 0.0 {
                                return Some(0.0);
                            }
                            acc /= v;
                        }
                        acc
                    }
                    _ => return None,
                };
                Some(result)
            }
            Self::ThetaSketchEstimate { field, .. } => {
                let v = field.resolve_value(agg_results)?;
                let bytes = sketch_field_bytes(&v, sketch::THETA_SKETCH_TAG)?;
                ferrodruid_sketches::ThetaSketch::deserialize(&bytes)
                    .ok()
                    .map(|s| s.estimate())
            }
            Self::HllSketchEstimate { field, round, .. } => {
                let v = field.resolve_value(agg_results)?;
                let bytes = sketch_field_bytes(&v, sketch::HLL_SKETCH_TAG)?;
                let est = ferrodruid_sketches::HllSketch::deserialize(&bytes)
                    .ok()
                    .map(|s| s.estimate())?;
                // Druid's HLLSketchEstimate `"round": true` rounds the
                // estimate to the nearest long (SQL COUNT(DISTINCT) is
                // BIGINT).  thetaSketchEstimate has no documented round
                // field, so ThetaSketchEstimate above stays unrounded.
                Some(if *round == Some(true) {
                    est.round()
                } else {
                    est
                })
            }
            Self::QuantilesDoublesSketchToQuantile {
                field, fraction, ..
            } => {
                let v = field.resolve_value(agg_results)?;
                let bytes = sketch_field_bytes(&v, sketch::QUANTILES_SKETCH_TAG)?;
                ferrodruid_sketches::TDigest::deserialize(&bytes)
                    .ok()
                    .and_then(|d| d.quantile(*fraction).ok())
            }
            Self::Variance { field, .. } => {
                let v = field.resolve_value(agg_results)?;
                variance::VarianceAggregator::from_state_json(&v).and_then(|a| a.finalize())
            }
            Self::Stddev { field, .. } => {
                let v = field.resolve_value(agg_results)?;
                variance::VarianceAggregator::from_state_json(&v).and_then(|a| a.finalize_stddev())
            }
            Self::Expression { expression, .. } => {
                // Fail-closed: parse errors or unsupported constructs yield
                // `None` (the spec should have been rejected up front by
                // `validate_supported`).  Single-shot parse per evaluation;
                // parse caching is a known perf follow-up (see the
                // `postagg_expr` module docs).
                postagg_expr::parse(expression).ok()?.evaluate(agg_results)
            }
            Self::HyperUniqueCardinality { .. } => {
                // HyperUnique cardinality is not yet implemented.
                None
            }
        }
    }

    /// Resolve this post-aggregator to its raw JSON value (rather than a
    /// coerced `f64`).  Used by the sketch / variance finalizers, whose input
    /// is a partial-state envelope object, not a scalar number.  Only the
    /// `FieldAccess` and `Constant` leaves can resolve to a raw value; any
    /// other shape returns `None`.
    fn resolve_value(
        &self,
        agg_results: &HashMap<String, serde_json::Value>,
    ) -> Option<serde_json::Value> {
        match self {
            Self::FieldAccess { field_name, .. } => agg_results.get(field_name).cloned(),
            Self::Constant { value, .. } => Some(serde_json::Value::Number(value.clone())),
            _ => None,
        }
    }
}

/// Decode the base64 sketch bytes out of a partial-state envelope value,
/// checking the `@sketch` tag matches `expected_tag`.
fn sketch_field_bytes(value: &serde_json::Value, expected_tag: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let obj = value.as_object()?;
    if obj.get("@sketch").and_then(serde_json::Value::as_str)? != expected_tag {
        return None;
    }
    let b64 = obj.get("bytes")?.as_str()?;
    base64::engine::general_purpose::STANDARD.decode(b64).ok()
}

/// Extracts a numeric f64 from a JSON value.
fn value_to_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- AggregatorSpec deserialization ---

    #[test]
    fn deser_count() {
        let j = r#"{"type":"count","name":"cnt"}"#;
        let spec: AggregatorSpec = serde_json::from_str(j).expect("deser count");
        assert_eq!(spec.name(), "cnt");
        assert!(spec.field_name().is_none());
    }

    #[test]
    fn deser_long_sum() {
        let j = r#"{"type":"longSum","name":"total","fieldName":"amount"}"#;
        let spec: AggregatorSpec = serde_json::from_str(j).expect("deser longSum");
        assert_eq!(spec.name(), "total");
        assert_eq!(spec.field_name(), Some("amount"));
    }

    #[test]
    fn deser_double_min() {
        let j = r#"{"type":"doubleMin","name":"lo","fieldName":"price"}"#;
        let spec: AggregatorSpec = serde_json::from_str(j).expect("deser doubleMin");
        assert_eq!(spec.name(), "lo");
    }

    #[test]
    fn deser_string_first() {
        let j = r#"{"type":"stringFirst","name":"sf","fieldName":"city","maxStringBytes":512}"#;
        let spec: AggregatorSpec = serde_json::from_str(j).expect("deser stringFirst");
        assert_eq!(spec.name(), "sf");
        if let AggregatorSpec::StringFirst {
            max_string_bytes, ..
        } = &spec
        {
            assert_eq!(*max_string_bytes, Some(512));
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn deser_filtered() {
        let j = r#"{
            "type": "filtered",
            "filter": {"type":"selector","dimension":"country","value":"US"},
            "aggregator": {"type":"count","name":"us_count"}
        }"#;
        let spec: AggregatorSpec = serde_json::from_str(j).expect("deser filtered");
        assert_eq!(spec.name(), "us_count");
    }

    #[test]
    fn round_trip_all_specs() {
        let specs = vec![
            json!({"type":"count","name":"c"}),
            json!({"type":"longSum","name":"s","fieldName":"x"}),
            json!({"type":"doubleSum","name":"ds","fieldName":"y"}),
            json!({"type":"floatSum","name":"fs","fieldName":"z"}),
            json!({"type":"longMin","name":"lmin","fieldName":"a"}),
            json!({"type":"longMax","name":"lmax","fieldName":"b"}),
            json!({"type":"doubleMin","name":"dmin","fieldName":"c"}),
            json!({"type":"doubleMax","name":"dmax","fieldName":"d"}),
            json!({"type":"floatMin","name":"fmin","fieldName":"e"}),
            json!({"type":"floatMax","name":"fmax","fieldName":"f"}),
            json!({"type":"longFirst","name":"lf","fieldName":"g"}),
            json!({"type":"longLast","name":"ll","fieldName":"h"}),
            json!({"type":"doubleFirst","name":"df","fieldName":"i"}),
            json!({"type":"doubleLast","name":"dl","fieldName":"j"}),
            json!({"type":"floatFirst","name":"ff","fieldName":"k"}),
            json!({"type":"floatLast","name":"fl","fieldName":"l"}),
            json!({"type":"stringFirst","name":"sf","fieldName":"m"}),
            json!({"type":"stringLast","name":"sl","fieldName":"n"}),
        ];
        for s in &specs {
            let spec: AggregatorSpec = serde_json::from_value(s.clone()).expect("deser");
            let re = serde_json::to_value(&spec).expect("ser");
            let spec2: AggregatorSpec = serde_json::from_value(re).expect("round-trip deser");
            assert_eq!(spec.name(), spec2.name());
        }
    }

    // --- Count ---

    #[test]
    fn count_empty() {
        let agg = CountAggregator::new();
        assert_eq!(agg.get(), json!(0));
    }

    #[test]
    fn count_single() {
        let mut agg = CountAggregator::new();
        agg.aggregate(Some(&json!(42)));
        assert_eq!(agg.get(), json!(1));
    }

    #[test]
    fn count_multiple_and_null() {
        let mut agg = CountAggregator::new();
        agg.aggregate(Some(&json!(1)));
        agg.aggregate(None);
        agg.aggregate(Some(&json!("hello")));
        assert_eq!(agg.get(), json!(3));
    }

    #[test]
    fn count_merge() {
        let mut a = CountAggregator::new();
        a.aggregate(Some(&json!(1)));
        a.aggregate(Some(&json!(2)));
        let mut b = CountAggregator::new();
        b.aggregate(Some(&json!(3)));
        a.merge(&b);
        assert_eq!(a.get(), json!(3));
    }

    #[test]
    fn count_reset() {
        let mut a = CountAggregator::new();
        a.aggregate(Some(&json!(1)));
        a.reset();
        assert_eq!(a.get(), json!(0));
    }

    // --- LongSum ---

    #[test]
    fn long_sum_basic() {
        let mut agg = LongSumAggregator::new();
        agg.aggregate(Some(&json!(10)));
        agg.aggregate(Some(&json!(20)));
        agg.aggregate(None);
        agg.aggregate(Some(&json!(30)));
        assert_eq!(agg.get(), json!(60));
    }

    #[test]
    fn long_sum_merge() {
        let mut a = LongSumAggregator::new();
        a.aggregate(Some(&json!(100)));
        let mut b = LongSumAggregator::new();
        b.aggregate(Some(&json!(200)));
        a.merge(&b);
        assert_eq!(a.get(), json!(300));
    }

    // --- DoubleSum ---

    #[test]
    fn double_sum_basic() {
        let mut agg = DoubleSumAggregator::new();
        agg.aggregate(Some(&json!(1.5)));
        agg.aggregate(Some(&json!(2.5)));
        agg.aggregate(None);
        assert_eq!(agg.get(), json!(4.0));
    }

    // --- LongMin/LongMax ---

    #[test]
    fn long_min_basic() {
        let mut agg = LongMinAggregator::new();
        agg.aggregate(Some(&json!(50)));
        agg.aggregate(Some(&json!(10)));
        agg.aggregate(Some(&json!(30)));
        assert_eq!(agg.get(), json!(10));
    }

    #[test]
    fn long_max_basic() {
        let mut agg = LongMaxAggregator::new();
        agg.aggregate(Some(&json!(50)));
        agg.aggregate(Some(&json!(100)));
        agg.aggregate(Some(&json!(30)));
        assert_eq!(agg.get(), json!(100));
    }

    // Druid 35 default null handling (measured 2026-07-11): an EMPTY
    // min/max is SQL null on the wire, not the `i64::MAX`/`i64::MIN`
    // fold-identity sentinel it used to leak.
    #[test]
    fn long_min_empty() {
        let agg = LongMinAggregator::new();
        assert_eq!(agg.get(), serde_json::Value::Null);
    }

    #[test]
    fn long_max_empty() {
        let agg = LongMaxAggregator::new();
        assert_eq!(agg.get(), serde_json::Value::Null);
    }

    #[test]
    fn long_min_merge() {
        let mut a = LongMinAggregator::new();
        a.aggregate(Some(&json!(50)));
        let mut b = LongMinAggregator::new();
        b.aggregate(Some(&json!(10)));
        a.merge(&b);
        assert_eq!(a.get(), json!(10));
    }

    // --- DoubleMin/DoubleMax ---

    #[test]
    fn double_min_basic() {
        let mut agg = DoubleMinAggregator::new();
        agg.aggregate(Some(&json!(std::f64::consts::PI)));
        agg.aggregate(Some(&json!(2.71)));
        assert_eq!(agg.get(), json!(2.71));
    }

    #[test]
    fn double_max_basic() {
        let mut agg = DoubleMaxAggregator::new();
        agg.aggregate(Some(&json!(std::f64::consts::PI)));
        agg.aggregate(Some(&json!(2.71)));
        assert_eq!(agg.get(), json!(std::f64::consts::PI));
    }

    // --- FloatMin/FloatMax ---

    #[test]
    fn float_min_basic() {
        let mut agg = FloatMinAggregator::new();
        agg.aggregate(Some(&json!(3.0)));
        agg.aggregate(Some(&json!(1.5)));
        assert_eq!(agg.get(), json!(1.5));
    }

    // --- StringFirst/StringLast ---

    #[test]
    fn string_first_empty() {
        let agg = StringFirstAggregator::new(1024);
        assert_eq!(agg.get(), serde_json::Value::Null);
    }

    #[test]
    fn string_first_basic() {
        let mut agg = StringFirstAggregator::new(1024);
        // Simulate rows with timestamps: aggregate takes (timestamp, value) pairs
        // For simplicity, first_last aggregators track insertion order (timestamp via separate method)
        agg.aggregate_with_time(100, Some(&json!("alpha")));
        agg.aggregate_with_time(200, Some(&json!("beta")));
        agg.aggregate_with_time(50, Some(&json!("gamma")));
        assert_eq!(agg.get(), json!("gamma"));
    }

    #[test]
    fn string_last_basic() {
        let mut agg = StringLastAggregator::new(1024);
        agg.aggregate_with_time(100, Some(&json!("alpha")));
        agg.aggregate_with_time(200, Some(&json!("beta")));
        agg.aggregate_with_time(50, Some(&json!("gamma")));
        assert_eq!(agg.get(), json!("beta"));
    }

    #[test]
    fn string_first_truncation() {
        let mut agg = StringFirstAggregator::new(3);
        agg.aggregate_with_time(1, Some(&json!("abcdef")));
        assert_eq!(agg.get(), json!("abc"));
    }

    // --- Filtered ---

    #[test]
    fn filtered_delegates_when_active() {
        let inner = Box::new(CountAggregator::new()) as Box<dyn Aggregator>;
        let mut agg = FilteredAggregator::new(json!({"type":"selector"}), inner);
        // FilteredAggregator always delegates in our current impl (filter eval is a placeholder)
        agg.aggregate(Some(&json!(1)));
        agg.aggregate(Some(&json!(2)));
        assert_eq!(agg.get(), json!(2));
    }

    // --- PostAggregator ---

    #[test]
    fn post_agg_field_access() {
        let mut results = HashMap::new();
        results.insert("total".to_string(), json!(42));
        let spec = PostAggregatorSpec::FieldAccess {
            name: "out".into(),
            field_name: "total".into(),
        };
        assert_eq!(spec.evaluate(&results), Some(42.0));
    }

    #[test]
    fn post_agg_constant() {
        let results = HashMap::new();
        let spec = PostAggregatorSpec::Constant {
            name: "pi".into(),
            value: serde_json::Number::from_f64(3.125).expect("f64"),
        };
        assert_eq!(spec.evaluate(&results), Some(3.125));
    }

    #[test]
    fn post_agg_arithmetic_add() {
        let mut results = HashMap::new();
        results.insert("a".to_string(), json!(10));
        results.insert("b".to_string(), json!(20));
        let spec = PostAggregatorSpec::Arithmetic {
            name: "sum".into(),
            fn_name: "+".into(),
            fields: vec![
                PostAggregatorSpec::FieldAccess {
                    name: "fa".into(),
                    field_name: "a".into(),
                },
                PostAggregatorSpec::FieldAccess {
                    name: "fb".into(),
                    field_name: "b".into(),
                },
            ],
        };
        assert_eq!(spec.evaluate(&results), Some(30.0));
    }

    #[test]
    fn post_agg_arithmetic_subtract() {
        let mut results = HashMap::new();
        results.insert("a".to_string(), json!(50));
        results.insert("b".to_string(), json!(20));
        let spec = PostAggregatorSpec::Arithmetic {
            name: "diff".into(),
            fn_name: "-".into(),
            fields: vec![
                PostAggregatorSpec::FieldAccess {
                    name: "fa".into(),
                    field_name: "a".into(),
                },
                PostAggregatorSpec::FieldAccess {
                    name: "fb".into(),
                    field_name: "b".into(),
                },
            ],
        };
        assert_eq!(spec.evaluate(&results), Some(30.0));
    }

    #[test]
    fn post_agg_arithmetic_multiply() {
        let mut results = HashMap::new();
        results.insert("a".to_string(), json!(6));
        results.insert("b".to_string(), json!(7));
        let spec = PostAggregatorSpec::Arithmetic {
            name: "prod".into(),
            fn_name: "*".into(),
            fields: vec![
                PostAggregatorSpec::FieldAccess {
                    name: "fa".into(),
                    field_name: "a".into(),
                },
                PostAggregatorSpec::FieldAccess {
                    name: "fb".into(),
                    field_name: "b".into(),
                },
            ],
        };
        assert_eq!(spec.evaluate(&results), Some(42.0));
    }

    #[test]
    fn post_agg_arithmetic_divide() {
        let mut results = HashMap::new();
        results.insert("a".to_string(), json!(100));
        results.insert("b".to_string(), json!(4));
        let spec = PostAggregatorSpec::Arithmetic {
            name: "quot".into(),
            fn_name: "/".into(),
            fields: vec![
                PostAggregatorSpec::FieldAccess {
                    name: "fa".into(),
                    field_name: "a".into(),
                },
                PostAggregatorSpec::FieldAccess {
                    name: "fb".into(),
                    field_name: "b".into(),
                },
            ],
        };
        assert_eq!(spec.evaluate(&results), Some(25.0));
    }

    #[test]
    fn post_agg_division_by_zero() {
        let mut results = HashMap::new();
        results.insert("a".to_string(), json!(100));
        results.insert("b".to_string(), json!(0));
        let spec = PostAggregatorSpec::Arithmetic {
            name: "bad".into(),
            fn_name: "/".into(),
            fields: vec![
                PostAggregatorSpec::FieldAccess {
                    name: "fa".into(),
                    field_name: "a".into(),
                },
                PostAggregatorSpec::FieldAccess {
                    name: "fb".into(),
                    field_name: "b".into(),
                },
            ],
        };
        assert_eq!(spec.evaluate(&results), Some(0.0));
    }

    #[test]
    fn post_agg_deser_arithmetic() {
        let j = r#"{
            "type": "arithmetic",
            "name": "avg",
            "fn": "+",
            "fields": [
                {"type":"fieldAccess","name":"fa","fieldName":"total"},
                {"type":"constant","name":"c","value":1}
            ]
        }"#;
        let spec: PostAggregatorSpec = serde_json::from_str(j).expect("deser");
        assert_eq!(spec.name(), "avg");
    }

    #[test]
    fn post_agg_deser_expression() {
        let j = r#"{"type":"expression","name":"e","expression":"x + y"}"#;
        let spec: PostAggregatorSpec = serde_json::from_str(j).expect("deser");
        assert_eq!(spec.name(), "e");
    }

    /// `expression` post-aggregators are now evaluatable for the supported
    /// grammar subset: a parseable expression must validate...
    #[test]
    fn post_agg_validate_accepts_parseable_expression() {
        let spec = PostAggregatorSpec::Expression {
            name: "e".into(),
            expression: "round(\"s\" / \"c\", 1)".into(),
        };
        spec.validate_supported()
            .expect("parseable expression must validate");
    }

    /// ...while a malformed or unsupported expression must still be rejected
    /// loudly at validation time (Wave 45-B fail-loud contract preserved:
    /// query results never silently drop the derived field).
    #[test]
    fn post_agg_validate_rejects_malformed_expression() {
        let spec = PostAggregatorSpec::Expression {
            name: "e".into(),
            expression: "concat(x, y)".into(), // unsupported function
        };
        let err = spec
            .validate_supported()
            .expect_err("unsupported expression must not validate");
        let msg = format!("{err}");
        assert!(
            msg.contains("expression") && msg.contains("concat"),
            "error must mention the unsupported construct: {msg}"
        );
    }

    #[test]
    fn post_agg_validate_rejects_hyper_unique_cardinality() {
        let spec = PostAggregatorSpec::HyperUniqueCardinality {
            name: "h".into(),
            field_name: "u".into(),
        };
        let err = spec
            .validate_supported()
            .expect_err("hyperUniqueCardinality must not validate");
        let msg = format!("{err}");
        assert!(
            msg.contains("hyperUniqueCardinality"),
            "error must mention the unsupported variant: {msg}"
        );
    }

    #[test]
    fn post_agg_validate_recurses_into_arithmetic_children() {
        // An arithmetic post-aggregator that nests an invalid expression
        // child must itself fail validation, not just the leaf.
        let spec = PostAggregatorSpec::Arithmetic {
            name: "ratio".into(),
            fn_name: "/".into(),
            fields: vec![
                PostAggregatorSpec::FieldAccess {
                    name: "fa".into(),
                    field_name: "n".into(),
                },
                PostAggregatorSpec::Expression {
                    name: "denom".into(),
                    expression: "x * ".into(), // malformed
                },
            ],
        };
        let err = spec
            .validate_supported()
            .expect_err("nested malformed expression must propagate failure");
        let msg = format!("{err}");
        assert!(msg.contains("expression"), "msg = {msg}");
    }

    #[test]
    fn post_agg_validate_accepts_supported_tree() {
        let spec = PostAggregatorSpec::Arithmetic {
            name: "sum".into(),
            fn_name: "+".into(),
            fields: vec![
                PostAggregatorSpec::FieldAccess {
                    name: "fa".into(),
                    field_name: "a".into(),
                },
                PostAggregatorSpec::Constant {
                    name: "c".into(),
                    value: serde_json::Number::from(1),
                },
            ],
        };
        spec.validate_supported()
            .expect("supported tree must validate");
    }

    // --- AggregatorSpec::create ---

    #[test]
    fn spec_create_and_aggregate() {
        let spec: AggregatorSpec =
            serde_json::from_str(r#"{"type":"longSum","name":"s","fieldName":"v"}"#)
                .expect("deser");
        let mut agg = spec.create();
        agg.aggregate(Some(&json!(5)));
        agg.aggregate(Some(&json!(10)));
        assert_eq!(agg.get(), json!(15));
    }

    // --- New aggregator spec deserialization (variance + sketches) ---

    #[test]
    fn deser_variance_default_estimator() {
        let j = r#"{"type":"variance","name":"v","fieldName":"x"}"#;
        let spec: AggregatorSpec = serde_json::from_str(j).expect("deser variance");
        assert_eq!(spec.name(), "v");
        assert_eq!(spec.field_name(), Some("x"));
        assert!(matches!(
            spec,
            AggregatorSpec::Variance {
                estimator: None,
                ..
            }
        ));
    }

    #[test]
    fn deser_variance_sample_estimator() {
        let j = r#"{"type":"variance","name":"v","fieldName":"x","estimator":"sample"}"#;
        let spec: AggregatorSpec = serde_json::from_str(j).expect("deser variance sample");
        if let AggregatorSpec::Variance { estimator, .. } = &spec {
            assert_eq!(estimator.as_deref(), Some("sample"));
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn deser_hll_sketch_build_and_merge() {
        let build: AggregatorSpec = serde_json::from_str(
            r#"{"type":"HLLSketchBuild","name":"h","fieldName":"u","lgK":12}"#,
        )
        .expect("deser HLLSketchBuild");
        assert!(matches!(
            build,
            AggregatorSpec::HllSketchBuild { lg_k: Some(12), .. }
        ));
        let merge: AggregatorSpec =
            serde_json::from_str(r#"{"type":"HLLSketchMerge","name":"h","fieldName":"u"}"#)
                .expect("deser HLLSketchMerge");
        assert!(matches!(merge, AggregatorSpec::HllSketchMerge { .. }));
    }

    #[test]
    fn deser_theta_and_quantiles_sketch() {
        let theta: AggregatorSpec = serde_json::from_str(
            r#"{"type":"thetaSketch","name":"t","fieldName":"u","size":2048}"#,
        )
        .expect("deser thetaSketch");
        assert!(matches!(
            theta,
            AggregatorSpec::ThetaSketch {
                size: Some(2048),
                ..
            }
        ));
        let q: AggregatorSpec = serde_json::from_str(
            r#"{"type":"quantilesDoublesSketch","name":"q","fieldName":"latency","k":128}"#,
        )
        .expect("deser quantilesDoublesSketch");
        assert!(matches!(
            q,
            AggregatorSpec::QuantilesDoublesSketch { k: Some(128), .. }
        ));
    }

    #[test]
    fn variance_spec_create_and_finalize() {
        let spec: AggregatorSpec =
            serde_json::from_str(r#"{"type":"variance","name":"v","fieldName":"x"}"#)
                .expect("deser");
        let mut agg = spec.create();
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            agg.aggregate(Some(&json!(v)));
        }
        let state = agg.get();
        let recon = VarianceAggregator::from_state_json(&state).expect("state");
        assert!((recon.finalize().expect("var") - 4.0).abs() < 1e-9);
    }

    #[test]
    fn hll_spec_create_estimates_distinct() {
        let spec: AggregatorSpec =
            serde_json::from_str(r#"{"type":"HLLSketchBuild","name":"h","fieldName":"u"}"#)
                .expect("deser");
        let mut agg = spec.create();
        for i in 0_u32..1000 {
            agg.aggregate(Some(&json!(i)));
        }
        let est = agg
            .get()
            .get("estimate")
            .and_then(serde_json::Value::as_f64)
            .expect("estimate");
        assert!((est - 1000.0).abs() / 1000.0 < 0.05, "est={est}");
    }

    #[test]
    fn round_trip_new_specs() {
        let specs = vec![
            json!({"type":"variance","name":"v","fieldName":"x","estimator":"sample"}),
            json!({"type":"HLLSketchBuild","name":"h","fieldName":"u","lgK":12}),
            json!({"type":"HLLSketchMerge","name":"hm","fieldName":"u"}),
            json!({"type":"thetaSketch","name":"t","fieldName":"u","size":4096}),
            json!({"type":"quantilesDoublesSketch","name":"q","fieldName":"l","k":200}),
        ];
        for s in &specs {
            let spec: AggregatorSpec = serde_json::from_value(s.clone()).expect("deser");
            let re = serde_json::to_value(&spec).expect("ser");
            let spec2: AggregatorSpec = serde_json::from_value(re).expect("round-trip");
            assert_eq!(spec.name(), spec2.name());
        }
    }

    // --- New post-aggregators (sketch estimate / quantile / variance / stddev) ---

    #[test]
    fn post_agg_variance_and_stddev_finalize() {
        let mut agg = VarianceAggregator::new(VarianceEstimator::Population);
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            agg.add(v);
        }
        let mut results = HashMap::new();
        results.insert("vstate".to_string(), agg.get());

        let var_pa = PostAggregatorSpec::Variance {
            name: "var".into(),
            field: Box::new(PostAggregatorSpec::FieldAccess {
                name: "fa".into(),
                field_name: "vstate".into(),
            }),
        };
        let sd_pa = PostAggregatorSpec::Stddev {
            name: "sd".into(),
            field: Box::new(PostAggregatorSpec::FieldAccess {
                name: "fa".into(),
                field_name: "vstate".into(),
            }),
        };
        let var = var_pa.evaluate(&results).expect("var");
        let sd = sd_pa.evaluate(&results).expect("sd");
        assert!((var - 4.0).abs() < 1e-9, "var={var}");
        assert!((sd - 2.0).abs() < 1e-9, "sd={sd}");
        var_pa.validate_supported().expect("variance pa valid");
        sd_pa.validate_supported().expect("stddev pa valid");
    }

    #[test]
    fn post_agg_theta_and_hll_estimate() {
        let mut hll = HllSketchAggregator::build(14);
        let mut theta = ThetaSketchAggregator::new(4096);
        for i in 0_u32..1000 {
            hll.aggregate(Some(&json!(i)));
            theta.aggregate(Some(&json!(i)));
        }
        let mut results = HashMap::new();
        results.insert("h".to_string(), hll.get());
        results.insert("t".to_string(), theta.get());

        let hll_pa = PostAggregatorSpec::HllSketchEstimate {
            name: "he".into(),
            field: Box::new(PostAggregatorSpec::FieldAccess {
                name: "fa".into(),
                field_name: "h".into(),
            }),
            round: None,
        };
        let theta_pa = PostAggregatorSpec::ThetaSketchEstimate {
            name: "te".into(),
            field: Box::new(PostAggregatorSpec::FieldAccess {
                name: "fa".into(),
                field_name: "t".into(),
            }),
        };
        let he = hll_pa.evaluate(&results).expect("he");
        let te = theta_pa.evaluate(&results).expect("te");
        assert!((he - 1000.0).abs() / 1000.0 < 0.05, "he={he}");
        assert!((te - 1000.0).abs() / 1000.0 < 0.05, "te={te}");
    }

    /// `HLLSketchEstimate` with `"round": true` returns an integral estimate
    /// (Druid SQL `COUNT(DISTINCT)` is BIGINT); for a small-cardinality
    /// column the rounded estimate is the exact distinct count.
    #[test]
    fn post_agg_hll_estimate_round_true_is_integral_and_exact_for_small_n() {
        let mut hll = HllSketchAggregator::build(14);
        for city in ["tokyo", "osaka", "kyoto", "tokyo", "osaka"] {
            hll.aggregate(Some(&json!(city)));
        }
        let mut results = HashMap::new();
        results.insert("h".to_string(), hll.get());
        let rounded = PostAggregatorSpec::HllSketchEstimate {
            name: "he".into(),
            field: Box::new(PostAggregatorSpec::FieldAccess {
                name: "fa".into(),
                field_name: "h".into(),
            }),
            round: Some(true),
        };
        let est = rounded.evaluate(&results).expect("estimate");
        assert!(
            (est - est.round()).abs() < f64::EPSILON,
            "round=true must produce an integral value, got {est}"
        );
        assert!(
            (est - 3.0).abs() < f64::EPSILON,
            "expected exactly 3, got {est}"
        );
    }

    /// Wire-format compatibility: an `HLLSketchEstimate` spec without `round`
    /// must serialize byte-identically to the pre-`round` format, and
    /// `"round": true` must survive a serde round-trip.
    #[test]
    fn post_agg_hll_estimate_round_wire_compat() {
        let legacy = r#"{"type":"HLLSketchEstimate","name":"he","field":{"type":"fieldAccess","name":"fa","fieldName":"h"}}"#;
        let spec: PostAggregatorSpec = serde_json::from_str(legacy).expect("deser legacy");
        let re = serde_json::to_string(&spec).expect("ser legacy");
        assert_eq!(
            re, legacy,
            "serialization without round must be byte-identical"
        );

        let with_round = r#"{"type":"HLLSketchEstimate","name":"he","field":{"type":"fieldAccess","name":"fa","fieldName":"h"},"round":true}"#;
        let spec: PostAggregatorSpec = serde_json::from_str(with_round).expect("deser round");
        assert!(matches!(
            &spec,
            PostAggregatorSpec::HllSketchEstimate {
                round: Some(true),
                ..
            }
        ));
        let re = serde_json::to_string(&spec).expect("ser round");
        assert_eq!(re, with_round, "round:true must survive a serde round-trip");
    }

    /// `expression` post-aggregator end-to-end through
    /// `PostAggregatorSpec::evaluate`: `round("s" / "c", 1)` over sum/count
    /// outputs, null propagation, and fail-closed parse errors.
    #[test]
    fn post_agg_expression_evaluates() {
        let spec: PostAggregatorSpec = serde_json::from_str(
            r#"{"type":"expression","name":"avg_r","expression":"round(\"s\" / \"c\", 1)"}"#,
        )
        .expect("deser expression");
        spec.validate_supported().expect("expression validates");

        let mut results = HashMap::new();
        results.insert("s".to_string(), json!(22.0));
        results.insert("c".to_string(), json!(7));
        let v = spec.evaluate(&results).expect("value");
        assert!(
            (v - 3.1).abs() < f64::EPSILON,
            "22/7 rounded to 1dp = 3.1, got {v}"
        );

        // Null / missing field propagation: whole expression -> None.
        let mut with_null = HashMap::new();
        with_null.insert("s".to_string(), json!(null));
        with_null.insert("c".to_string(), json!(7));
        assert_eq!(
            spec.evaluate(&with_null),
            None,
            "null field must yield None"
        );
        let missing: HashMap<String, serde_json::Value> = HashMap::new();
        assert_eq!(
            spec.evaluate(&missing),
            None,
            "missing field must yield None"
        );

        // Division by zero -> None (IEEE inf filtered), unlike the
        // arithmetic post-agg's documented divide-by-zero -> 0.
        let mut zero_c = HashMap::new();
        zero_c.insert("s".to_string(), json!(22.0));
        zero_c.insert("c".to_string(), json!(0));
        assert_eq!(spec.evaluate(&zero_c), None, "x/0 must yield None");

        // Fail-closed on a malformed expression at evaluate time.
        let bad = PostAggregatorSpec::Expression {
            name: "b".into(),
            expression: "s +".into(),
        };
        assert_eq!(bad.evaluate(&results), None);
    }

    #[test]
    fn post_agg_quantiles_to_quantile() {
        let mut q = QuantilesSketchAggregator::new(200);
        for i in 1..=1000 {
            q.aggregate(Some(&json!(i)));
        }
        let mut results = HashMap::new();
        results.insert("q".to_string(), q.get());
        let p95_pa = PostAggregatorSpec::QuantilesDoublesSketchToQuantile {
            name: "p95".into(),
            field: Box::new(PostAggregatorSpec::FieldAccess {
                name: "fa".into(),
                field_name: "q".into(),
            }),
            fraction: 0.95,
        };
        let p95 = p95_pa.evaluate(&results).expect("p95");
        assert!((p95 - 950.0).abs() / 950.0 < 0.05, "p95={p95}");
    }

    #[test]
    fn post_agg_new_variants_deserialize() {
        let theta: PostAggregatorSpec = serde_json::from_str(
            r#"{"type":"thetaSketchEstimate","name":"te","field":{"type":"fieldAccess","name":"fa","fieldName":"t"}}"#,
        )
        .expect("deser thetaSketchEstimate");
        assert_eq!(theta.name(), "te");
        let q: PostAggregatorSpec = serde_json::from_str(
            r#"{"type":"quantilesDoublesSketchToQuantile","name":"p","field":{"type":"fieldAccess","name":"fa","fieldName":"q"},"fraction":0.5}"#,
        )
        .expect("deser quantilesDoublesSketchToQuantile");
        assert_eq!(q.name(), "p");
        let sd: PostAggregatorSpec = serde_json::from_str(
            r#"{"type":"stddev","name":"s","field":{"type":"fieldAccess","name":"fa","fieldName":"v"}}"#,
        )
        .expect("deser stddev");
        assert_eq!(sd.name(), "s");
    }

    #[test]
    fn merge_json_by_spec_routes_variance_and_sketches() {
        // Variance partial states merge correctly through the dispatcher.
        let mut a = VarianceAggregator::new(VarianceEstimator::Population);
        for v in [2.0, 4.0, 4.0, 4.0] {
            a.add(v);
        }
        let mut b = VarianceAggregator::new(VarianceEstimator::Population);
        for v in [5.0, 5.0, 7.0, 9.0] {
            b.add(v);
        }
        let vspec = AggregatorSpec::Variance {
            name: "v".into(),
            field_name: "x".into(),
            estimator: None,
        };
        let merged = merge_json_by_spec(&vspec, &a.get(), &b.get());
        let var = VarianceAggregator::from_state_json(&merged)
            .expect("state")
            .finalize()
            .expect("var");
        assert!((var - 4.0).abs() < 1e-9, "dispatcher-merged var={var}");

        // HLL sketches merge through the dispatcher.
        let mut ha = HllSketchAggregator::build(12);
        let mut hb = HllSketchAggregator::build(12);
        for i in 0_u32..500 {
            ha.aggregate(Some(&json!(i)));
        }
        for i in 500_u32..1000 {
            hb.aggregate(Some(&json!(i)));
        }
        let hspec = AggregatorSpec::HllSketchBuild {
            name: "h".into(),
            field_name: "u".into(),
            lg_k: Some(12),
            should_finalize: None,
        };
        let hmerged = merge_json_by_spec(&hspec, &ha.get(), &hb.get());
        let est = hmerged
            .get("estimate")
            .and_then(serde_json::Value::as_f64)
            .expect("estimate");
        assert!(
            (est - 1000.0).abs() / 1000.0 < 0.10,
            "dispatcher HLL est={est}"
        );
    }

    #[test]
    fn spec_create_count() {
        let spec: AggregatorSpec =
            serde_json::from_str(r#"{"type":"count","name":"c"}"#).expect("deser");
        let mut agg = spec.create();
        agg.aggregate(Some(&json!(null)));
        agg.aggregate(Some(&json!(1)));
        assert_eq!(agg.get(), json!(2));
    }

    // -----------------------------------------------------------------------
    // Wave 48 — proptest hardening (aggregator algebraic invariants)
    //
    // * `prop_long_sum_commutative` — feeding `[a, b]` and `[b, a]` into
    //   two `LongSumAggregator`s yields identical results
    //   (commutativity of integer wrapping_add).
    // * `prop_long_sum_associative_via_merge` — `(a ⊕ b) ⊕ c == a ⊕ (b ⊕ c)`
    //   under shard-merge semantics.
    // * `prop_long_min_associative` — min is associative under any
    //   partition (a ⊕ b ⊕ c == min(a, b, c)).
    // * `prop_count_identity_empty` — a fresh CountAggregator returns 0.
    // * `prop_long_max_idempotent_under_self_merge` — `a ⊕ a == a`.
    // -----------------------------------------------------------------------

    mod proptests {
        use super::super::*;
        use proptest::prelude::*;
        use serde_json::json;

        fn long_sum_of(xs: &[i64]) -> i64 {
            let mut agg = LongSumAggregator::new();
            for x in xs {
                agg.aggregate(Some(&json!(*x)));
            }
            agg.get().as_i64().unwrap_or(0)
        }

        fn long_min_of(xs: &[i64]) -> i64 {
            let mut agg = LongMinAggregator::new();
            for x in xs {
                agg.aggregate(Some(&json!(*x)));
            }
            agg.get().as_i64().unwrap_or(i64::MAX)
        }

        proptest! {
            /// Commutativity: feeding values in different orders to a
            /// long-sum aggregator must produce the same total.
            #[test]
            fn prop_long_sum_commutative(
                a in prop::collection::vec(any::<i64>(), 0..100),
                b in prop::collection::vec(any::<i64>(), 0..100),
            ) {
                let s_ab = long_sum_of(&[a.as_slice(), b.as_slice()].concat());
                let s_ba = long_sum_of(&[b.as_slice(), a.as_slice()].concat());
                prop_assert_eq!(s_ab, s_ba);
            }

            /// Associativity under shard-merge: building two partial
            /// aggregators and merging is equivalent to a single pass.
            /// (Uses wrapping i64 addition as defined by `LongSumAggregator`.)
            #[test]
            fn prop_long_sum_associative_via_merge(
                a in prop::collection::vec(any::<i64>(), 0..50),
                b in prop::collection::vec(any::<i64>(), 0..50),
                c in prop::collection::vec(any::<i64>(), 0..50),
            ) {
                let mut left = LongSumAggregator::new();
                for v in a.iter().chain(b.iter()) {
                    left.aggregate(Some(&json!(*v)));
                }
                let mut right = LongSumAggregator::new();
                for v in c.iter() {
                    right.aggregate(Some(&json!(*v)));
                }
                left.merge(&right);

                let single = long_sum_of(&[a.as_slice(), b.as_slice(), c.as_slice()].concat());
                prop_assert_eq!(left.get().as_i64().unwrap_or(0), single);
            }

            /// Min is associative: `min(a, b, c)` is the same regardless
            /// of how the values are partitioned across shards.
            #[test]
            fn prop_long_min_associative(
                a in prop::collection::vec(any::<i64>(), 1..30),
                b in prop::collection::vec(any::<i64>(), 1..30),
            ) {
                let combined = long_min_of(&[a.as_slice(), b.as_slice()].concat());
                let m_a = long_min_of(&a);
                let m_b = long_min_of(&b);
                let merged = m_a.min(m_b);
                prop_assert_eq!(combined, merged);
            }

            /// Identity: a fresh count aggregator has count 0 regardless
            /// of how it is constructed.
            #[test]
            fn prop_count_identity_empty(_dummy in 0u8..1) {
                let agg = CountAggregator::new();
                prop_assert_eq!(agg.get(), json!(0));
            }

            /// Idempotence under self-merge: max(a, a) == a.
            #[test]
            fn prop_long_max_idempotent_under_self_merge(
                xs in prop::collection::vec(any::<i64>(), 1..50),
            ) {
                let mut a = LongMaxAggregator::new();
                let mut b = LongMaxAggregator::new();
                for v in &xs {
                    a.aggregate(Some(&json!(*v)));
                    b.aggregate(Some(&json!(*v)));
                }
                let before = a.get();
                a.merge(&b);
                prop_assert_eq!(a.get(), before);
            }
        }
    }
}
