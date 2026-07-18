// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `GROUPING(c1, c2, ...)` indicator function (CL-4 / W1-H R7).
//!
//! `GROUPING(col)` returns `1` when `col` is being aggregated by the
//! current grouping subset (i.e., its value in the result row is the
//! aggregate-over-everything null), and `0` when `col` is in the active
//! grouping subset.  `GROUPING(c1, c2, ..., cn)` returns the bitmask
//! `sum_i (g_i * 2^(n-1-i))` where `g_i = 1` iff `c_i` is aggregated.
//!
//! Implementation note: the GROUPING bitmask is *not* row-accumulated —
//! it depends only on the current subtotals subset.  The groupBy
//! executor finalises the value at row emit time (it has visibility into
//! the active subset list); this aggregator's [`Aggregator::aggregate`]
//! method is a no-op.
//!
//! The [`GroupingAggregator::compute_bitmask`] helper is exposed so the
//! executor can compute the value without instantiating an aggregator,
//! and so unit tests can exercise the bitmask logic directly.

use crate::Aggregator;

/// A `GROUPING(...)` indicator aggregator.
///
/// The aggregator carries the per-call configuration (the list of
/// columns referenced by `GROUPING(...)` and the enclosing query's full
/// GROUP BY column list); the actual bitmask is computed by the
/// executor at row emit time and written into the result event.
#[derive(Debug, Clone)]
pub struct GroupingAggregator {
    /// Columns referenced by the `GROUPING(...)` call (left-to-right).
    fields: Vec<String>,
    /// Full list of the enclosing query's GROUP BY output column names.
    group_by_dims: Vec<String>,
}

impl GroupingAggregator {
    /// Construct a new aggregator.
    #[must_use]
    pub fn new(fields: Vec<String>, group_by_dims: Vec<String>) -> Self {
        Self {
            fields,
            group_by_dims,
        }
    }

    /// Borrow the configured fields list.
    #[must_use]
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Borrow the configured GROUP BY dimensions list.
    #[must_use]
    pub fn group_by_dims(&self) -> &[String] {
        &self.group_by_dims
    }

    /// Compute the bitmask for the current grouping subset.
    ///
    /// `active_dim_names` are the GROUP BY output column names present
    /// in the *current* subtotals subset (i.e., the dimensions that
    /// have a real grouped value in the emitted row).  Any field
    /// referenced by `GROUPING(...)` that is NOT in `active_dim_names`
    /// contributes a `1` bit; fields IN `active_dim_names` contribute
    /// `0`.  The leftmost argument is the most significant bit.
    ///
    /// Fields referenced by `GROUPING(...)` that are not in the
    /// enclosing query's GROUP BY at all are treated as "always
    /// aggregated" (bit set) — a defensive choice that matches what an
    /// equivalent unqualified SELECT would have produced.
    #[must_use]
    pub fn compute_bitmask(&self, active_dim_names: &[String]) -> i64 {
        let n = self.fields.len();
        if n == 0 {
            return 0;
        }
        let mut mask: i64 = 0;
        for (i, col) in self.fields.iter().enumerate() {
            let bit_pos = n - 1 - i;
            let is_grouped = active_dim_names.iter().any(|name| name == col);
            if !is_grouped {
                mask |= 1i64 << bit_pos;
            }
        }
        mask
    }
}

impl Aggregator for GroupingAggregator {
    fn aggregate(&mut self, _value: Option<&serde_json::Value>) {
        // GROUPING is computed at emit time; per-row accumulation is a no-op.
    }

    fn get(&self) -> serde_json::Value {
        // The default emit value is 0 (every field grouped) — the
        // executor must override via `compute_bitmask` for any row
        // produced by a partial subtotals subset.  Returning 0 keeps
        // callers that bypass the executor finalisation honest about
        // the missing context (rather than silently producing a
        // nonsense bitmask).
        serde_json::Value::from(0_i64)
    }

    fn merge(&mut self, _other: &dyn Aggregator) {
        // GROUPING does not merge — every shard produces the same per-row
        // bitmask for the same grouping subset.
    }

    fn reset(&mut self) {}

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn grouping_single_arg_grouped_returns_zero() {
        let agg = GroupingAggregator::new(s(&["city"]), s(&["city", "country"]));
        assert_eq!(agg.compute_bitmask(&s(&["city", "country"])), 0);
    }

    #[test]
    fn grouping_single_arg_not_grouped_returns_one() {
        let agg = GroupingAggregator::new(s(&["city"]), s(&["city", "country"]));
        // Subset omits `city` -> city is aggregated -> bit 0 set.
        assert_eq!(agg.compute_bitmask(&s(&["country"])), 1);
    }

    #[test]
    fn grouping_two_arg_left_msb() {
        let agg = GroupingAggregator::new(s(&["city", "country"]), s(&["city", "country"]));
        // both grouped -> 0
        assert_eq!(agg.compute_bitmask(&s(&["city", "country"])), 0);
        // city absent, country present -> bit (n-1-0) = bit 1 -> 2
        assert_eq!(agg.compute_bitmask(&s(&["country"])), 0b10);
        // city present, country absent -> bit (n-1-1) = bit 0 -> 1
        assert_eq!(agg.compute_bitmask(&s(&["city"])), 0b01);
        // both absent (grand total) -> 11 = 3
        assert_eq!(agg.compute_bitmask(&[]), 0b11);
    }

    /// Edge case: empty argument list returns 0 (no bits to set).
    #[test]
    fn grouping_no_args_returns_zero() {
        let agg = GroupingAggregator::new(Vec::new(), s(&["x"]));
        assert_eq!(agg.compute_bitmask(&s(&["x"])), 0);
    }

    #[test]
    fn grouping_aggregate_is_no_op() {
        let mut agg = GroupingAggregator::new(vec!["a".to_string()], vec!["a".to_string()]);
        agg.aggregate(Some(&serde_json::json!("anything")));
        // Default `get()` returns 0; the executor must call
        // `compute_bitmask` for any row that comes from a subtotals subset.
        assert_eq!(agg.get(), serde_json::json!(0));
    }
}
