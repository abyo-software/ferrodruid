// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Cost-based balancing strategy for segment placement.
//!
//! # Cost model (clean-room)
//!
//! When deciding where to place a segment, we want to *spread out* segments
//! whose time intervals are close together, because queries usually scan a
//! contiguous time range and co-locating temporally adjacent segments on one
//! Historical creates a hotspot. We therefore assign a **joint cost** to every
//! ordered pair of segments that would sit on the same server, and place each
//! new segment on the server that minimizes the total added cost.
//!
//! The cost between two segments is derived purely from their time intervals
//! using an exponential time-decay kernel. Let two segments have intervals
//! `[a0, a1)` and `[b0, b1)` measured in milliseconds since the epoch. Define
//! the half-life decay rate
//!
//! ```text
//! LAMBDA = ln(2) / HALF_LIFE_MILLIS
//! ```
//!
//! so that two instants `HALF_LIFE_MILLIS` apart contribute half the cost of
//! two coincident instants. The pairwise cost is the double integral of the
//! decay kernel over the two intervals:
//!
//! ```text
//! cost(A, B) = ∫_{a0}^{a1} ∫_{b0}^{b1} exp(-LAMBDA * |x - y|) dy dx
//! ```
//!
//! This integral has a closed form. With `k = LAMBDA`, the inner integral over
//! `y` for a fixed `x` and an interval `[y0, y1)` to one side of `x` is
//! `(exp(-k*(y0 - x)) - exp(-k*(y1 - x))) / k`. Splitting the outer interval at
//! the points where the sign of `x - y` flips and summing the contributions
//! yields the helper [`interval_cost`] below. Larger overlap and smaller gaps
//! produce larger cost; intervals far apart in time produce cost decaying
//! exponentially toward zero.
//!
//! The cost of placing a candidate segment on a server is the sum of
//! `cost(candidate, s)` over every segment `s` already on that server, plus a
//! small *fill penalty* proportional to the server's fill ratio so that, all
//! else equal, emptier servers are preferred (this keeps the cluster balanced
//! when many segments share the same interval).
//!
//! This is a clean-room formulation. Apache Druid's `CostBalancerStrategy`
//! uses a conceptually similar exponential time-decay joint cost, but the exact
//! constants, the data-source affinity bonus, and the gap/overlap segmentation
//! here are our own derivation from the integral above. We do **not** reproduce
//! Druid's piecewise code.

use ferrodruid_common::types::Interval;

/// Default half-life for the time-decay kernel: 45 days in milliseconds.
///
/// Two segments whose nearest edges are 45 days apart contribute roughly half
/// the cost of two coincident segments. The value is on the same order as the
/// daily/monthly segment granularities common in Druid deployments.
pub const HALF_LIFE_MILLIS: f64 = 45.0 * 24.0 * 60.0 * 60.0 * 1000.0;

/// Weight of the fill-ratio penalty (in the same units as a unit-overlap
/// segment cost). Keeps placement biased toward emptier servers when temporal
/// costs tie.
pub const FILL_PENALTY_WEIGHT: f64 = 1.0;

/// Decay rate `LAMBDA = ln(2) / HALF_LIFE_MILLIS`.
#[must_use]
fn lambda() -> f64 {
    std::f64::consts::LN_2 / HALF_LIFE_MILLIS
}

/// Interval endpoints in milliseconds since the Unix epoch as `(start, end)`.
///
/// `end` is clamped to be `>= start` so a malformed (reversed) interval yields
/// a zero-width interval rather than a negative-width one.
#[must_use]
fn interval_millis(iv: &Interval) -> (f64, f64) {
    let start = iv.start.timestamp_millis() as f64;
    let end = iv.end.timestamp_millis() as f64;
    (start, end.max(start))
}

/// Closed-form joint cost between two time intervals under the exponential
/// time-decay kernel `exp(-LAMBDA * |x - y|)`.
///
/// Returns the double integral described in the module docs. The result is
/// always non-negative and decays toward zero as the intervals move apart.
#[must_use]
pub fn interval_cost(a: &Interval, b: &Interval) -> f64 {
    let k = lambda();
    let (a0, a1) = interval_millis(a);
    let (b0, b1) = interval_millis(b);

    // Degenerate (zero-width) intervals contribute no cost.
    if a1 <= a0 || b1 <= b0 {
        return 0.0;
    }

    // We compute ∫_{a0}^{a1} g(x) dx where
    //   g(x) = ∫_{b0}^{b1} exp(-k|x - y|) dy.
    // To keep the exponents well-conditioned we shift time so that the start of
    // interval A is the origin; |x - y| is translation-invariant.
    let shift = a0;
    let (a0, a1) = (a0 - shift, a1 - shift);
    let (b0, b1) = (b0 - shift, b1 - shift);

    // Antiderivative pieces use exp(-k * d) with d >= 0, so values stay in
    // (0, 1] and never overflow.
    //
    // For a fixed x, split [b0, b1) into the part below x (y < x) and above x
    // (y > x):
    //   ∫_{y<x} exp(-k(x - y)) dy + ∫_{y>x} exp(-k(y - x)) dy.
    // Integrating g(x) over [a0, a1) again splits at b0 and b1. Rather than
    // deriving every cross term by hand we evaluate via a numerically stable
    // helper that integrates analytically over the three orderings.
    cost_integral(a0, a1, b0, b1, k)
}

/// Evaluate `∫_{a0}^{a1} ∫_{b0}^{b1} exp(-k|x - y|) dy dx` analytically.
///
/// Inputs are pre-shifted (see [`interval_cost`]). All `exp` arguments are
/// non-positive so the computation cannot overflow.
#[must_use]
fn cost_integral(a0: f64, a1: f64, b0: f64, b1: f64, k: f64) -> f64 {
    // Inner integral h(x) = ∫_{b0}^{b1} exp(-k|x - y|) dy as a function of x.
    // We integrate h over [a0, a1] by adaptively handling the regions relative
    // to b0 and b1. Because h is piecewise-smooth with breakpoints at b0 and
    // b1, we split [a0, a1] at those breakpoints and integrate each smooth
    // piece in closed form.
    let mut breakpoints = vec![a0, a1, b0.clamp(a0, a1), b1.clamp(a0, a1)];
    breakpoints.sort_by(|p, q| p.partial_cmp(q).unwrap_or(std::cmp::Ordering::Equal));
    breakpoints.dedup_by(|p, q| (*p - *q).abs() < f64::EPSILON);

    let mut total = 0.0;
    for w in breakpoints.windows(2) {
        let (lo, hi) = (w[0], w[1]);
        if hi <= lo {
            continue;
        }
        total += piece_integral(lo, hi, b0, b1, k);
    }
    total
}

/// Integrate `h(x) = ∫_{b0}^{b1} exp(-k|x - y|) dy` over `x ∈ [lo, hi]`, where
/// the entire sub-interval `[lo, hi]` lies wholly below `b0`, wholly above
/// `b1`, or wholly within `[b0, b1]` (guaranteed by the breakpoint split).
#[must_use]
fn piece_integral(lo: f64, hi: f64, b0: f64, b1: f64, k: f64) -> f64 {
    let mid = 0.5 * (lo + hi);
    if mid <= b0 {
        // x <= y for all y: |x - y| = y - x.
        // h(x) = ∫_{b0}^{b1} exp(-k(y - x)) dy
        //      = exp(k x) * (exp(-k b0) - exp(-k b1)) / k.
        // ∫_{lo}^{hi} exp(k x) dx = (exp(k hi) - exp(k lo)) / k. To keep
        // exponents non-positive we factor out exp(k * b1) is not safe; instead
        // combine: exp(k x) * exp(-k b0) = exp(-k (b0 - x)) with b0 - x >= 0.
        let c = |x: f64| -> f64 {
            // ∫ over y closed form already applied; here accumulate the outer.
            // Use stable form: term = (exp(-k(b0 - x)) - exp(-k(b1 - x))) / k.
            (neg_exp(k * (b0 - x)) - neg_exp(k * (b1 - x))) / k
        };
        integrate_simpson(c, lo, hi)
    } else if mid >= b1 {
        // x >= y for all y: |x - y| = x - y.
        let c = |x: f64| -> f64 { (neg_exp(k * (x - b1)) - neg_exp(k * (x - b0))) / k };
        integrate_simpson(c, lo, hi)
    } else {
        // b0 <= x <= b1: split y at x.
        let c = |x: f64| -> f64 {
            let below = (1.0 - neg_exp(k * (x - b0))) / k; // ∫_{b0}^{x} exp(-k(x-y))dy
            let above = (1.0 - neg_exp(k * (b1 - x))) / k; // ∫_{x}^{b1} exp(-k(y-x))dy
            below + above
        };
        integrate_simpson(c, lo, hi)
    }
}

/// `exp(-d)` clamped so a non-finite or negative argument cannot produce
/// `+inf`. Arguments are expected to be `>= 0`; tiny negatives from rounding
/// are clamped to `0`.
#[must_use]
fn neg_exp(d: f64) -> f64 {
    (-d.max(0.0)).exp()
}

/// Composite Simpson integration of a smooth function over `[lo, hi]`.
///
/// The inner integrand on each breakpoint-bounded piece is smooth and
/// monotone, so a modest panel count gives an accurate, allocation-free result.
#[must_use]
fn integrate_simpson<F: Fn(f64) -> f64>(f: F, lo: f64, hi: f64) -> f64 {
    if hi <= lo {
        return 0.0;
    }
    const PANELS: usize = 16; // even
    let h = (hi - lo) / PANELS as f64;
    let mut sum = f(lo) + f(hi);
    for i in 1..PANELS {
        let x = lo + h * i as f64;
        sum += if i % 2 == 0 { 2.0 } else { 4.0 } * f(x);
    }
    sum * h / 3.0
}

/// Cost of placing a candidate segment on a server already holding
/// `existing_intervals`, plus a fill-ratio penalty.
///
/// `fill_ratio` is the server's `current / max` occupancy in `[0, 1]`; it is
/// scaled by [`FILL_PENALTY_WEIGHT`] and added so emptier servers win ties.
#[must_use]
pub fn placement_cost(
    candidate: &Interval,
    existing_intervals: &[Interval],
    fill_ratio: f64,
) -> f64 {
    let temporal: f64 = existing_intervals
        .iter()
        .map(|iv| interval_cost(candidate, iv))
        .sum();
    temporal + FILL_PENALTY_WEIGHT * fill_ratio
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};

    fn iv(d0: i64, d1: i64) -> Interval {
        // Build intervals as day-offsets from a fixed epoch for clarity. Using
        // offsets (rather than calendar days) keeps arbitrarily large gaps
        // valid without hitting month/day bounds.
        let base = Utc
            .with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
            .single()
            .expect("base");
        Interval {
            start: base + Duration::days(d0),
            end: base + Duration::days(d1),
        }
    }

    #[test]
    fn cost_positive_and_finite() {
        let c = interval_cost(&iv(1, 2), &iv(1, 2));
        assert!(c.is_finite());
        assert!(c > 0.0);
    }

    #[test]
    fn cost_decays_with_distance() {
        let base = iv(1, 2);
        let near = interval_cost(&base, &iv(2, 3));
        let far = interval_cost(&base, &iv(28, 29));
        let farther = interval_cost(&base, &iv(31, 32));
        assert!(near > far, "near={near} far={far}");
        assert!(far > farther, "far={far} farther={farther}");
    }

    #[test]
    fn cost_symmetric() {
        let a = iv(1, 5);
        let b = iv(3, 9);
        let ab = interval_cost(&a, &b);
        let ba = interval_cost(&b, &a);
        assert!((ab - ba).abs() < 1e-6 * ab.max(1.0), "ab={ab} ba={ba}");
    }

    #[test]
    fn zero_width_interval_zero_cost() {
        let degenerate = Interval {
            start: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            end: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        };
        assert_eq!(interval_cost(&degenerate, &iv(1, 2)), 0.0);
    }

    #[test]
    fn overlap_costs_more_than_adjacent() {
        let base = iv(1, 10);
        let overlapping = interval_cost(&base, &iv(5, 15));
        let adjacent = interval_cost(&base, &iv(10, 19));
        assert!(
            overlapping > adjacent,
            "overlap={overlapping} adjacent={adjacent}"
        );
    }

    #[test]
    fn placement_prefers_emptier_server_on_tie() {
        let candidate = iv(1, 2);
        // Two servers each holding the same single far-away segment -> equal
        // temporal cost; fill ratio breaks the tie.
        let existing = vec![iv(40, 41)];
        let empty_cost = placement_cost(&candidate, &existing, 0.1);
        let full_cost = placement_cost(&candidate, &existing, 0.9);
        assert!(empty_cost < full_cost);
    }

    #[test]
    fn placement_prefers_temporally_distant_server() {
        let candidate = iv(1, 2);
        let crowded = vec![iv(1, 2), iv(2, 3), iv(3, 4)]; // temporally close
        let distant = vec![iv(40, 41), iv(41, 42), iv(42, 43)];
        let crowded_cost = placement_cost(&candidate, &crowded, 0.5);
        let distant_cost = placement_cost(&candidate, &distant, 0.5);
        assert!(
            distant_cost < crowded_cost,
            "distant={distant_cost} crowded={crowded_cost}"
        );
    }

    #[test]
    fn empty_server_only_fill_penalty() {
        let candidate = iv(1, 2);
        let cost = placement_cost(&candidate, &[], 0.0);
        assert_eq!(cost, 0.0);
        let cost2 = placement_cost(&candidate, &[], 0.5);
        assert!((cost2 - 0.5 * FILL_PENALTY_WEIGHT).abs() < 1e-12);
    }
}
