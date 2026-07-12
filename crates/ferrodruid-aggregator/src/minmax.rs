// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Min/Max aggregators for long, double, and float types.
//!
//! SQL-null contract (Druid 35 default null handling, measured 2026-07-11):
//! a min/max that has seen NO non-null input reports SQL `null` from
//! [`Aggregator::get`], not its fold-identity sentinel (`i64::MAX` for
//! `longMin`, `i64::MIN` for `longMax`, `f64::MAX`/`f64::MIN` and the `f32`
//! analogues for the floating widths). Each aggregator tracks a `seen` flag
//! set by the first non-null contribution — the same pattern the sum family
//! carries in [`crate::sum`]. `merge` reads the other side's `get()`: an
//! empty partial merges as JSON null (no number extracts), so it never folds
//! its sentinel into a real result, and merging a real partial into an empty
//! accumulator adopts the value and flips `seen`.

use crate::Aggregator;

macro_rules! define_minmax_aggregator {
    (
        $name:ident, $doc:expr, $ty:ty,
        $init:expr, $cmp:tt, $json_extract:ident,
        $to_json:expr
    ) => {
        #[doc = $doc]
        #[doc = ""]
        #[doc = "With NO non-null input the result is SQL null (see the module doc)."]
        #[derive(Debug, Clone)]
        pub struct $name {
            value: $ty,
            seen: bool,
        }

        impl $name {
            /// Creates a new aggregator in the no-input state: the running
            /// value holds the fold-identity sentinel and `get()` reports
            /// SQL null until the first non-null input arrives.
            pub fn new() -> Self {
                Self {
                    value: $init,
                    seen: false,
                }
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl Aggregator for $name {
            fn aggregate(&mut self, value: Option<&serde_json::Value>) {
                if let Some(v) = value.and_then(|v| v.$json_extract()) {
                    #[allow(clippy::cast_possible_truncation)]
                    let typed: $ty = v as $ty;
                    // `!self.seen` adopts the very first input even when it
                    // equals the sentinel (e.g. an actual `i64::MAX` fed to
                    // a min) — the sentinel is fold identity, not state.
                    if !self.seen || typed $cmp self.value {
                        self.value = typed;
                    }
                    self.seen = true;
                }
            }

            fn get(&self) -> serde_json::Value {
                if !self.seen {
                    return serde_json::Value::Null;
                }
                #[allow(clippy::redundant_closure_call)]
                ($to_json)(self.value)
            }

            fn merge(&mut self, other: &dyn Aggregator) {
                // An empty partial's get() is JSON null → no number extracts
                // → no-op, so an empty partial never folds its sentinel into
                // a real result and never nullifies one either.
                if let Some(v) = other.get().$json_extract() {
                    #[allow(clippy::cast_possible_truncation)]
                    let typed: $ty = v as $ty;
                    if !self.seen || typed $cmp self.value {
                        self.value = typed;
                    }
                    self.seen = true;
                }
            }

            fn reset(&mut self) {
                self.value = $init;
                self.seen = false;
            }

            fn clone_box(&self) -> Box<dyn Aggregator> {
                Box::new(self.clone())
            }
        }
    };
}

fn i64_to_json(v: i64) -> serde_json::Value {
    serde_json::Value::Number(serde_json::Number::from(v))
}

fn f64_to_json(v: f64) -> serde_json::Value {
    serde_json::to_value(v).unwrap_or(serde_json::Value::Null)
}

fn f32_to_json(v: f32) -> serde_json::Value {
    serde_json::to_value(f64::from(v)).unwrap_or(serde_json::Value::Null)
}

define_minmax_aggregator!(
    LongMinAggregator,
    "Tracks the minimum long (i64) value. Initial value is `i64::MAX`.",
    i64,
    i64::MAX,
    <,
    as_i64,
    i64_to_json
);

define_minmax_aggregator!(
    LongMaxAggregator,
    "Tracks the maximum long (i64) value. Initial value is `i64::MIN`.",
    i64,
    i64::MIN,
    >,
    as_i64,
    i64_to_json
);

define_minmax_aggregator!(
    DoubleMinAggregator,
    "Tracks the minimum double (f64) value. Initial value is `f64::MAX`.",
    f64,
    f64::MAX,
    <,
    as_f64,
    f64_to_json
);

define_minmax_aggregator!(
    DoubleMaxAggregator,
    "Tracks the maximum double (f64) value. Initial value is `f64::MIN`.",
    f64,
    f64::MIN,
    >,
    as_f64,
    f64_to_json
);

define_minmax_aggregator!(
    FloatMinAggregator,
    "Tracks the minimum float (f32) value. Initial value is `f32::MAX`.",
    f32,
    f32::MAX,
    <,
    as_f64,
    f32_to_json
);

define_minmax_aggregator!(
    FloatMaxAggregator,
    "Tracks the maximum float (f32) value. Initial value is `f32::MIN`.",
    f32,
    f32::MIN,
    >,
    as_f64,
    f32_to_json
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Aggregator;
    use serde_json::json;

    /// SQL-null semantics (Druid 35 default null handling, measured
    /// 2026-07-11): a min/max that has seen NO non-null input reports SQL
    /// null from `get()`, not its init sentinel (`i64::MAX` for `longMin`,
    /// `i64::MIN` for `longMax`, etc.). Same contract the sum family got
    /// in `sum.rs`.
    #[test]
    fn all_null_input_minmax_report_null() {
        let aggs: Vec<Box<dyn Aggregator>> = vec![
            Box::new(LongMinAggregator::new()),
            Box::new(LongMaxAggregator::new()),
            Box::new(DoubleMinAggregator::new()),
            Box::new(DoubleMaxAggregator::new()),
            Box::new(FloatMinAggregator::new()),
            Box::new(FloatMaxAggregator::new()),
        ];
        for mut agg in aggs {
            agg.aggregate(None);
            agg.aggregate(Some(&serde_json::Value::Null));
            assert_eq!(
                agg.get(),
                serde_json::Value::Null,
                "a min/max with no non-null input must be SQL null, got {:?}",
                agg.get()
            );
        }
    }

    /// A single non-null input flips the aggregator from null to that value
    /// — even when the value equals the old init sentinel (`i64::MAX` fed to
    /// `longMin` is a legitimate observed minimum, not the empty state).
    #[test]
    fn single_input_flips_from_null_even_at_sentinel() {
        let mut min = LongMinAggregator::new();
        min.aggregate(Some(&json!(i64::MAX)));
        assert_eq!(min.get(), json!(i64::MAX));

        let mut max = LongMaxAggregator::new();
        max.aggregate(Some(&json!(i64::MIN)));
        assert_eq!(max.get(), json!(i64::MIN));

        let mut dmin = DoubleMinAggregator::new();
        dmin.aggregate(Some(&json!(0.0)));
        assert_eq!(dmin.get(), json!(0.0));
    }

    /// Plain folding still works once inputs arrive; nulls interleaved with
    /// values are skipped without disturbing the running value.
    #[test]
    fn minmax_fold_skips_nulls() {
        let mut min = LongMinAggregator::new();
        let mut max = LongMaxAggregator::new();
        for v in [json!(5), serde_json::Value::Null, json!(-3), json!(9)] {
            min.aggregate(Some(&v));
            max.aggregate(Some(&v));
        }
        assert_eq!(min.get(), json!(-3));
        assert_eq!(max.get(), json!(9));
    }

    /// Merge semantics with the null contract:
    /// * non-empty ⊕ empty  → unchanged (an empty partial must NOT fold its
    ///   sentinel into a real result)
    /// * empty ⊕ non-empty  → adopts the value and un-nulls
    /// * empty ⊕ empty      → still null
    /// * reset()            → back to the null (no-input) state
    #[test]
    fn merge_preserves_null_contract() {
        let empty = DoubleMinAggregator::new();
        let mut full = DoubleMinAggregator::new();
        full.aggregate(Some(&json!(7.5)));

        let mut lhs = full.clone();
        lhs.merge(&empty);
        assert_eq!(lhs.get(), json!(7.5), "empty partial must not disturb");

        let mut lhs = DoubleMinAggregator::new();
        lhs.merge(&full);
        assert_eq!(lhs.get(), json!(7.5), "merging into empty adopts");

        let mut lhs = DoubleMinAggregator::new();
        lhs.merge(&empty);
        assert_eq!(lhs.get(), serde_json::Value::Null, "empty ⊕ empty is null");

        let mut r = full.clone();
        r.reset();
        assert_eq!(r.get(), serde_json::Value::Null, "reset returns to null");
    }

    /// Cross-partial merge still folds real values on both sides.
    #[test]
    fn merge_folds_real_partials() {
        let mut a = LongMaxAggregator::new();
        a.aggregate(Some(&json!(10)));
        let mut b = LongMaxAggregator::new();
        b.aggregate(Some(&json!(25)));
        a.merge(&b);
        assert_eq!(a.get(), json!(25));

        let mut c = LongMinAggregator::new();
        c.aggregate(Some(&json!(10)));
        let mut d = LongMinAggregator::new();
        d.aggregate(Some(&json!(-2)));
        c.merge(&d);
        assert_eq!(c.get(), json!(-2));
    }
}
