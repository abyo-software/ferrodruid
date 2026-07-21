// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Variance / standard-deviation aggregator.
//!
//! Implements Druid's `variance` aggregator using Welford's online algorithm.
//! The aggregator tracks the running `(count, mean, m2)` triple where `m2` is
//! the sum of squared deviations from the mean.  This form is numerically
//! stable for streaming input and supports an exact parallel merge (the
//! Chan / Golub / LeVeque parallel-variance combination), so per-shard partial
//! states can be combined without loss.
//!
//! Two estimators are supported, matching Druid:
//!
//! * `population` (default) — divides `m2` by `count`.
//! * `sample` — divides `m2` by `count - 1` (Bessel's correction).
//!
//! The aggregator's partial state (`get`) is a JSON object
//! `{"count":…,"sum":…,"m2":…,"estimator":…}` so that scatter/gather merge
//! across the broker can reconstruct and combine states exactly.  A
//! [`finalize`](VarianceAggregator::finalize) helper turns the state into the
//! scalar variance, and the [`PostAggregatorSpec`](crate::PostAggregatorSpec)
//! `stddev` variant takes its square root.

use std::any::Any;

use crate::Aggregator;

/// Variance estimator selection (Bessel's correction or not).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarianceEstimator {
    /// Population variance: divide the sum of squared deviations by `n`.
    Population,
    /// Sample variance: divide by `n - 1` (Bessel's correction).
    Sample,
}

impl VarianceEstimator {
    /// Parse a Druid estimator string.  Unknown / absent values default to
    /// [`VarianceEstimator::Population`], matching Druid's default.
    #[must_use]
    pub fn from_opt(s: Option<&str>) -> Self {
        match s {
            Some("sample") => Self::Sample,
            _ => Self::Population,
        }
    }

    /// The canonical Druid wire string for this estimator.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Population => "population",
            Self::Sample => "sample",
        }
    }
}

/// Online variance aggregator (Welford / parallel-merge).
#[derive(Debug, Clone)]
pub struct VarianceAggregator {
    count: u64,
    mean: f64,
    m2: f64,
    estimator: VarianceEstimator,
}

impl VarianceAggregator {
    /// Create a new variance aggregator with the given estimator.
    #[must_use]
    pub fn new(estimator: VarianceEstimator) -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            estimator,
        }
    }

    /// Number of values aggregated so far.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Running mean of the aggregated values.
    #[must_use]
    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Sum of squared deviations from the mean (the `M2` accumulator).
    #[must_use]
    pub fn m2(&self) -> f64 {
        self.m2
    }

    /// The configured estimator.
    #[must_use]
    pub fn estimator(&self) -> VarianceEstimator {
        self.estimator
    }

    /// Feed a single floating-point value (Welford update).
    pub fn add(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        #[allow(clippy::cast_precision_loss)]
        let count_f = self.count as f64;
        self.mean += delta / count_f;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }

    /// Merge another variance state into this one using the parallel
    /// (Chan / Golub / LeVeque) combination, which is exact regardless of how
    /// the values were partitioned across the two states.
    pub fn merge_state(&mut self, other: &VarianceAggregator) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            self.count = other.count;
            self.mean = other.mean;
            self.m2 = other.m2;
            return;
        }
        #[allow(clippy::cast_precision_loss)]
        let n_a = self.count as f64;
        #[allow(clippy::cast_precision_loss)]
        let n_b = other.count as f64;
        let n_ab = n_a + n_b;
        let delta = other.mean - self.mean;
        let new_mean = self.mean + delta * (n_b / n_ab);
        let new_m2 = self.m2 + other.m2 + delta * delta * (n_a * n_b / n_ab);
        self.count += other.count;
        self.mean = new_mean;
        self.m2 = new_m2;
    }

    /// Finalize the state to the scalar variance per the configured
    /// estimator.  Returns `None` when there are not enough values to produce
    /// a variance (`count == 0`, or `count == 1` for the `sample` estimator).
    #[must_use]
    pub fn finalize(&self) -> Option<f64> {
        match self.estimator {
            VarianceEstimator::Population => {
                if self.count == 0 {
                    return None;
                }
                #[allow(clippy::cast_precision_loss)]
                let denom = self.count as f64;
                Some(self.m2 / denom)
            }
            VarianceEstimator::Sample => {
                if self.count < 2 {
                    return None;
                }
                #[allow(clippy::cast_precision_loss)]
                let denom = (self.count - 1) as f64;
                Some(self.m2 / denom)
            }
        }
    }

    /// Finalize to the standard deviation (square root of the variance).
    #[must_use]
    pub fn finalize_stddev(&self) -> Option<f64> {
        self.finalize().map(f64::sqrt)
    }

    /// Build a partial-state JSON object for scatter/gather merge.
    #[must_use]
    pub fn state_json(&self) -> serde_json::Value {
        serde_json::json!({
            "count": self.count,
            "sum": self.mean * (self.count as f64),
            "mean": self.mean,
            "m2": self.m2,
            "estimator": self.estimator.as_str(),
        })
    }

    /// Reconstruct a partial state from a JSON object previously produced by
    /// [`state_json`](Self::state_json).  Returns `None` when the value is not
    /// a recognised variance state object.
    #[must_use]
    pub fn from_state_json(value: &serde_json::Value) -> Option<Self> {
        let obj = value.as_object()?;
        let count = obj.get("count")?.as_u64()?;
        let m2 = obj.get("m2")?.as_f64()?;
        let mean = match obj.get("mean").and_then(serde_json::Value::as_f64) {
            Some(m) => m,
            None => {
                let sum = obj.get("sum")?.as_f64()?;
                if count == 0 {
                    0.0
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    let denom = count as f64;
                    sum / denom
                }
            }
        };
        let estimator =
            VarianceEstimator::from_opt(obj.get("estimator").and_then(serde_json::Value::as_str));
        Some(Self {
            count,
            mean,
            m2,
            estimator,
        })
    }
}

impl Aggregator for VarianceAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        if let Some(v) = value.and_then(serde_json::Value::as_f64) {
            self.add(v);
        }
    }

    fn get(&self) -> serde_json::Value {
        self.state_json()
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        if let Some(any) = other.as_any()
            && let Some(other_var) = any.downcast_ref::<VarianceAggregator>()
        {
            self.merge_state(other_var);
            return;
        }
        // Fallback: `other` published its state as JSON via `get()`.
        if let Some(state) = VarianceAggregator::from_state_json(&other.get()) {
            self.merge_state(&state);
        }
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }
}

/// Merge two variance partial-state JSON objects (broker scatter/gather).
///
/// Both sides must be objects produced by
/// [`VarianceAggregator::state_json`].  When either side is not a recognised
/// state object the other side is returned unchanged; if neither is, `dst` is
/// returned.
#[must_use]
pub fn merge_variance_json(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    match (
        VarianceAggregator::from_state_json(dst),
        VarianceAggregator::from_state_json(src),
    ) {
        (Some(mut d), Some(s)) => {
            d.merge_state(&s);
            d.state_json()
        }
        (Some(d), None) => d.state_json(),
        (None, Some(s)) => s.state_json(),
        (None, None) => dst.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Exact population variance of a small hand-computed data set.
    /// data = [2, 4, 4, 4, 5, 5, 7, 9]; mean = 5; population variance = 4.0.
    #[test]
    fn population_variance_exact() {
        let mut agg = VarianceAggregator::new(VarianceEstimator::Population);
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            agg.add(v);
        }
        let var = agg.finalize().expect("variance");
        assert!((var - 4.0).abs() < 1e-9, "population variance = {var}");
    }

    /// Sample variance of the same data: m2 = 32, n-1 = 7 → 32/7 ≈ 4.5714286.
    #[test]
    fn sample_variance_exact() {
        let mut agg = VarianceAggregator::new(VarianceEstimator::Sample);
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            agg.add(v);
        }
        let var = agg.finalize().expect("variance");
        assert!(
            (var - 32.0 / 7.0).abs() < 1e-9,
            "sample variance = {var}, expected {}",
            32.0 / 7.0
        );
    }

    /// Population vs sample differ for the same data.
    #[test]
    fn population_vs_sample_differ() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        let mut pop = VarianceAggregator::new(VarianceEstimator::Population);
        let mut samp = VarianceAggregator::new(VarianceEstimator::Sample);
        for &v in &data {
            pop.add(v);
            samp.add(v);
        }
        let pv = pop.finalize().expect("pop");
        let sv = samp.finalize().expect("samp");
        // population = 2.0, sample = 2.5.
        assert!((pv - 2.0).abs() < 1e-9, "pop = {pv}");
        assert!((sv - 2.5).abs() < 1e-9, "samp = {sv}");
        assert!(sv > pv, "sample variance must exceed population variance");
    }

    /// Standard deviation is the square root of the variance.
    #[test]
    fn stddev_is_sqrt_of_variance() {
        let mut agg = VarianceAggregator::new(VarianceEstimator::Population);
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            agg.add(v);
        }
        let sd = agg.finalize_stddev().expect("stddev");
        assert!((sd - 2.0).abs() < 1e-9, "stddev = {sd}");
    }

    /// Parallel-merge equivalence: variance of [A ∪ B] computed by merging two
    /// partial states must equal variance computed on the concatenation.
    #[test]
    fn parallel_merge_equivalence() {
        let part_a = [2.0, 4.0, 4.0, 4.0];
        let part_b = [5.0, 5.0, 7.0, 9.0];

        // Single-pass over the concatenation.
        let mut single = VarianceAggregator::new(VarianceEstimator::Population);
        for &v in part_a.iter().chain(part_b.iter()) {
            single.add(v);
        }

        // Two partials merged.
        let mut a = VarianceAggregator::new(VarianceEstimator::Population);
        for &v in &part_a {
            a.add(v);
        }
        let mut b = VarianceAggregator::new(VarianceEstimator::Population);
        for &v in &part_b {
            b.add(v);
        }
        a.merge_state(&b);

        assert_eq!(a.count(), single.count());
        assert!(
            (a.mean() - single.mean()).abs() < 1e-9,
            "merged mean {} != single {}",
            a.mean(),
            single.mean()
        );
        assert!(
            (a.m2() - single.m2()).abs() < 1e-6,
            "merged m2 {} != single {}",
            a.m2(),
            single.m2()
        );
        assert!(
            (a.finalize().expect("a") - single.finalize().expect("single")).abs() < 1e-9,
            "merged variance must equal single-pass variance"
        );
    }

    /// Merging an empty state is a no-op; merging into an empty state adopts
    /// the other side.
    #[test]
    fn merge_with_empty_states() {
        let mut a = VarianceAggregator::new(VarianceEstimator::Population);
        let empty = VarianceAggregator::new(VarianceEstimator::Population);
        a.add(1.0);
        a.add(3.0);
        let before = a.finalize().expect("var");
        a.merge_state(&empty);
        assert!((a.finalize().expect("var") - before).abs() < 1e-12);

        let mut e = VarianceAggregator::new(VarianceEstimator::Population);
        e.merge_state(&a);
        assert_eq!(e.count(), a.count());
        assert!((e.mean() - a.mean()).abs() < 1e-12);
    }

    /// JSON partial-state round-trips through the broker merge helper.
    #[test]
    fn state_json_round_trip_and_merge() {
        let mut a = VarianceAggregator::new(VarianceEstimator::Population);
        for v in [2.0, 4.0, 4.0, 4.0] {
            a.add(v);
        }
        let mut b = VarianceAggregator::new(VarianceEstimator::Population);
        for v in [5.0, 5.0, 7.0, 9.0] {
            b.add(v);
        }
        let merged = merge_variance_json(&a.state_json(), &b.state_json());
        let recon = VarianceAggregator::from_state_json(&merged).expect("recon");
        assert!((recon.finalize().expect("var") - 4.0).abs() < 1e-9);
    }

    /// The trait `merge` path uses the typed `as_any` downcast.
    #[test]
    fn trait_merge_uses_typed_path() {
        let mut a = VarianceAggregator::new(VarianceEstimator::Population);
        a.add(2.0);
        a.add(4.0);
        let mut b = VarianceAggregator::new(VarianceEstimator::Population);
        b.add(4.0);
        b.add(4.0);
        let single = {
            let mut s = VarianceAggregator::new(VarianceEstimator::Population);
            for v in [2.0, 4.0, 4.0, 4.0] {
                s.add(v);
            }
            s.finalize().expect("single")
        };
        Aggregator::merge(&mut a, &b);
        let var = VarianceAggregator::from_state_json(&a.get())
            .expect("state")
            .finalize()
            .expect("var");
        assert!((var - single).abs() < 1e-9);
    }

    /// Sample estimator with a single value yields no variance.
    #[test]
    fn sample_single_value_is_none() {
        let mut agg = VarianceAggregator::new(VarianceEstimator::Sample);
        agg.add(42.0);
        assert!(agg.finalize().is_none());
    }

    /// Empty aggregator yields no variance for either estimator.
    #[test]
    fn empty_is_none() {
        let pop = VarianceAggregator::new(VarianceEstimator::Population);
        let samp = VarianceAggregator::new(VarianceEstimator::Sample);
        assert!(pop.finalize().is_none());
        assert!(samp.finalize().is_none());
    }

    /// Non-numeric / null values are ignored by `aggregate`.
    #[test]
    fn aggregate_ignores_non_numeric() {
        let mut agg = VarianceAggregator::new(VarianceEstimator::Population);
        agg.aggregate(Some(&json!("hello")));
        agg.aggregate(None);
        agg.aggregate(Some(&json!(2.0)));
        agg.aggregate(Some(&json!(4.0)));
        assert_eq!(agg.count(), 2);
    }
}
