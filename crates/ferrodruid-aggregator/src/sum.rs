// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Sum aggregators — LongSum (i64), DoubleSum (f64), FloatSum (f32).
//!
//! SQL-null contract (Druid 35 default null handling, measured 2026-07-11):
//! a sum that has seen NO non-null input reports SQL `null` from [`Aggregator::get`],
//! not `0`. Each aggregator tracks a `seen` flag set by the first non-null
//! contribution. `merge` reads the other side's `get()` — an empty partial
//! merges as a no-op (JSON null coerces to no number), so merging an empty
//! accumulator into a non-empty one never nullifies, and merging a non-empty
//! one into an empty one adopts its value and flips `seen`.

use crate::Aggregator;

/// Coerce a JSON numeric value to `i64`.
///
/// Druid's `longSum` aggregator accepts any numeric column, including
/// metric columns ingested as `double` / `float` (e.g. `100.0`).  Pre
/// Wave 47-D §7 the implementation only matched `as_i64()`, so a column
/// stored as `Double` silently summed to `0` because every JSON value
/// arrived as a non-integral `Number`.  This helper falls back to
/// `as_f64()` and truncates toward zero, which mirrors Druid's
/// `Numbers.parseLong` semantics for sum aggregators.
fn json_as_i64_lossy(v: &serde_json::Value) -> Option<i64> {
    if let Some(i) = v.as_i64() {
        return Some(i);
    }
    if let Some(u) = v.as_u64() {
        return Some(u as i64);
    }
    let f = v.as_f64()?;
    if !f.is_finite() {
        return None;
    }
    #[allow(clippy::cast_possible_truncation)]
    let truncated = f.trunc() as i64;
    Some(truncated)
}

// ---------------------------------------------------------------------------
// LongSum
// ---------------------------------------------------------------------------

/// Computes the sum of long (i64) values. Null/missing values are skipped;
/// with NO non-null input the result is SQL null (see the module doc).
#[derive(Debug, Clone)]
pub struct LongSumAggregator {
    sum: i64,
    seen: bool,
}

impl LongSumAggregator {
    /// Creates a new long sum aggregator initialized to the no-input state.
    pub fn new() -> Self {
        Self {
            sum: 0,
            seen: false,
        }
    }
}

impl Default for LongSumAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl Aggregator for LongSumAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        if let Some(v) = value.and_then(json_as_i64_lossy) {
            self.sum = self.sum.wrapping_add(v);
            self.seen = true;
        }
    }

    fn get(&self) -> serde_json::Value {
        if !self.seen {
            return serde_json::Value::Null;
        }
        serde_json::Value::Number(serde_json::Number::from(self.sum))
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        // An empty partial's get() is JSON null → no number → no-op, so an
        // empty accumulator never nullifies a non-empty one.
        if let Some(n) = json_as_i64_lossy(&other.get()) {
            self.sum = self.sum.wrapping_add(n);
            self.seen = true;
        }
    }

    fn reset(&mut self) {
        self.sum = 0;
        self.seen = false;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }
}

// ---------------------------------------------------------------------------
// DoubleSum
// ---------------------------------------------------------------------------

/// Computes the sum of double (f64) values. Null/missing values are skipped;
/// with NO non-null input the result is SQL null (see the module doc).
#[derive(Debug, Clone)]
pub struct DoubleSumAggregator {
    sum: f64,
    seen: bool,
}

impl DoubleSumAggregator {
    /// Creates a new double sum aggregator initialized to the no-input state.
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            seen: false,
        }
    }
}

impl Default for DoubleSumAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl Aggregator for DoubleSumAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        if let Some(v) = value.and_then(|v| v.as_f64()) {
            self.sum += v;
            self.seen = true;
        }
    }

    fn get(&self) -> serde_json::Value {
        if !self.seen {
            return serde_json::Value::Null;
        }
        serde_json::to_value(self.sum).unwrap_or(serde_json::Value::Null)
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        // Empty partial → get() null → no-op (never nullifies non-empty).
        if let Some(n) = other.get().as_f64() {
            self.sum += n;
            self.seen = true;
        }
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.seen = false;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }
}

// ---------------------------------------------------------------------------
// FloatSum
// ---------------------------------------------------------------------------

/// Computes the sum of float (f32) values. Null/missing values are skipped;
/// with NO non-null input the result is SQL null (see the module doc).
#[derive(Debug, Clone)]
pub struct FloatSumAggregator {
    sum: f32,
    seen: bool,
}

impl FloatSumAggregator {
    /// Creates a new float sum aggregator initialized to the no-input state.
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            seen: false,
        }
    }
}

impl Default for FloatSumAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl Aggregator for FloatSumAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        if let Some(v) = value.and_then(|v| v.as_f64()) {
            #[allow(clippy::cast_possible_truncation)]
            let fv = v as f32;
            self.sum += fv;
            self.seen = true;
        }
    }

    fn get(&self) -> serde_json::Value {
        if !self.seen {
            return serde_json::Value::Null;
        }
        serde_json::to_value(f64::from(self.sum)).unwrap_or(serde_json::Value::Null)
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        // Empty partial → get() null → no-op (never nullifies non-empty).
        if let Some(n) = other.get().as_f64() {
            #[allow(clippy::cast_possible_truncation)]
            let fv = n as f32;
            self.sum += fv;
            self.seen = true;
        }
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.seen = false;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Wave 47-D §7: `longSum` against a metric column ingested as
    /// `Double` (the common Druid case where ingestion specs declare a
    /// numeric metric and the segment store keeps it as `f64`) used to
    /// silently sum to `0` because `serde_json::Value::as_i64()` fails
    /// on `100.0`.  The aggregator now coerces via `as_f64` and
    /// truncates toward zero, matching Druid's `Numbers.parseLong`.
    #[test]
    fn long_sum_accepts_floating_point_metric_column() {
        let mut agg = LongSumAggregator::new();
        agg.aggregate(Some(&json!(100.0)));
        agg.aggregate(Some(&json!(200.0)));
        agg.aggregate(Some(&json!(75.0)));
        assert_eq!(agg.get(), json!(375));
    }

    /// Mixed integer + floating point inputs.  Integers take the
    /// `as_i64` fast path; floats are truncated.
    #[test]
    fn long_sum_mixed_int_and_float() {
        let mut agg = LongSumAggregator::new();
        agg.aggregate(Some(&json!(10_i64)));
        agg.aggregate(Some(&json!(2.7_f64)));
        agg.aggregate(Some(&json!(-1.9_f64)));
        // 10 + 2 (trunc 2.7) + -1 (trunc -1.9) = 11
        assert_eq!(agg.get(), json!(11));
    }

    /// Non-finite floats (NaN, +Inf, -Inf) are skipped.
    #[test]
    fn long_sum_skips_non_finite_floats() {
        let mut agg = LongSumAggregator::new();
        agg.aggregate(Some(&json!(5_i64)));
        agg.aggregate(Some(&serde_json::Value::Null));
        let nan_val = serde_json::Number::from_f64(f64::NAN).map(serde_json::Value::Number);
        agg.aggregate(nan_val.as_ref());
        assert_eq!(agg.get(), json!(5));
    }

    /// SQL-null semantics (Druid 35 default null handling): a sum that has
    /// seen NO non-null input reports SQL null — for all three widths.
    #[test]
    fn all_null_input_sums_report_null() {
        let mut l = LongSumAggregator::new();
        let mut d = DoubleSumAggregator::new();
        let mut f = FloatSumAggregator::new();
        for agg in [
            &mut l as &mut dyn Aggregator,
            &mut d as &mut dyn Aggregator,
            &mut f as &mut dyn Aggregator,
        ] {
            agg.aggregate(None);
            agg.aggregate(Some(&serde_json::Value::Null));
            assert_eq!(
                agg.get(),
                serde_json::Value::Null,
                "a sum with no non-null input must be SQL null"
            );
        }
        // A single non-null input flips each to a number — even 0.
        l.aggregate(Some(&json!(0_i64)));
        d.aggregate(Some(&json!(0.0)));
        f.aggregate(Some(&json!(0.0)));
        assert_eq!(l.get(), json!(0));
        assert_eq!(d.get(), json!(0.0));
        assert_eq!(f.get(), json!(0.0));
    }

    /// Merge semantics with the null contract:
    /// * empty ⊕ non-empty  → non-empty value (merge INTO empty)
    /// * non-empty ⊕ empty  → unchanged (an empty partial must NOT nullify)
    /// * empty ⊕ empty      → still null
    #[test]
    fn merge_preserves_null_contract() {
        let empty = DoubleSumAggregator::new();
        let mut full = DoubleSumAggregator::new();
        full.aggregate(Some(&json!(7.5)));

        // non-empty ⊕ empty: merging an empty accumulator is a no-op.
        let mut lhs = full.clone();
        lhs.merge(&empty);
        assert_eq!(lhs.get(), json!(7.5), "empty partial must not nullify");

        // empty ⊕ non-empty: the merged value survives and un-nulls.
        let mut lhs = DoubleSumAggregator::new();
        lhs.merge(&full);
        assert_eq!(lhs.get(), json!(7.5));

        // empty ⊕ empty stays null.
        let mut lhs = DoubleSumAggregator::new();
        lhs.merge(&empty);
        assert_eq!(lhs.get(), serde_json::Value::Null);

        // reset() returns to the null (no-input) state.
        let mut r = full.clone();
        r.reset();
        assert_eq!(r.get(), serde_json::Value::Null);
    }
}
