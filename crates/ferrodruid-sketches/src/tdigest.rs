// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! T-digest for streaming quantile (percentile) estimation.
//!
//! Implements the merging algorithm described in Dunning & Ertl (2019).
//! Centroids near the tails (q ≈ 0 or q ≈ 1) are kept small so that
//! extreme quantiles (p99, p999) are estimated accurately, while centroids
//! near the median are allowed to grow larger to save space.

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

use crate::{Result, SketchError};

/// Current serialization format version.
const VERSION: u8 = 1;

/// A centroid in the T-digest.
#[derive(Debug, Clone)]
pub struct Centroid {
    /// Weighted mean of the values in this centroid.
    pub mean: f64,
    /// Number of values represented by this centroid.
    pub count: u64,
}

/// T-digest for streaming quantile estimation.
///
/// The `compression` parameter (often called *delta*) controls the trade-off
/// between accuracy and memory.  A typical default is 100, which keeps at
/// most ~100 centroids.
#[derive(Debug, Clone)]
pub struct TDigest {
    centroids: Vec<Centroid>,
    total_count: u64,
    max_centroids: usize,
    min_val: f64,
    max_val: f64,
}

impl TDigest {
    /// Create a new, empty T-digest with the given compression parameter.
    pub fn new(compression: usize) -> Self {
        Self {
            centroids: Vec::new(),
            total_count: 0,
            max_centroids: compression,
            min_val: f64::INFINITY,
            max_val: f64::NEG_INFINITY,
        }
    }

    /// Add a single value with weight 1.
    pub fn add(&mut self, value: f64) {
        self.add_weighted(value, 1);
    }

    /// Add a value with an explicit count (weight).
    pub fn add_weighted(&mut self, value: f64, count: u64) {
        if count == 0 {
            return;
        }
        if value < self.min_val {
            self.min_val = value;
        }
        if value > self.max_val {
            self.max_val = value;
        }
        self.total_count += count;

        // Insert into sorted position.
        let pos = self
            .centroids
            .binary_search_by(|c| {
                c.mean
                    .partial_cmp(&value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap_or_else(|p| p);
        self.centroids.insert(pos, Centroid { mean: value, count });

        if self.centroids.len() > self.max_centroids * 2 {
            self.compress();
        }
    }

    /// Estimate the value at the given quantile.
    ///
    /// `q` must be in `[0.0, 1.0]`.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::Empty`] if no values have been added, or
    /// [`SketchError::InvalidQuantile`] if `q` is outside `[0.0, 1.0]`.
    pub fn quantile(&self, q: f64) -> Result<f64> {
        if !(0.0..=1.0).contains(&q) {
            return Err(SketchError::InvalidQuantile(q));
        }
        if self.centroids.is_empty() {
            return Err(SketchError::Empty);
        }
        if self.centroids.len() == 1 {
            return Ok(self.centroids[0].mean);
        }

        // Edge cases.
        if q <= 0.0 {
            return Ok(self.min_val);
        }
        if q >= 1.0 {
            return Ok(self.max_val);
        }

        let target = q * self.total_count as f64;
        let mut cumulative: f64 = 0.0;

        for i in 0..self.centroids.len() {
            let c = &self.centroids[i];
            let half = c.count as f64 / 2.0;

            if cumulative + half >= target {
                // Interpolate within this centroid.
                if i == 0 {
                    // Between min and first centroid mean.
                    let width = self.centroids[0].mean - self.min_val;
                    if half > 0.0 {
                        let frac = target / half;
                        return Ok(self.min_val + width * frac.min(1.0));
                    }
                    return Ok(self.centroids[0].mean);
                }
                // Between previous centroid mean and this centroid mean.
                let prev = &self.centroids[i - 1];
                let prev_half = prev.count as f64 / 2.0;
                let left = cumulative - prev_half;
                let right = cumulative + half;
                if (right - left).abs() < f64::EPSILON {
                    return Ok((prev.mean + c.mean) / 2.0);
                }
                let frac = (target - left) / (right - left);
                return Ok(prev.mean + (c.mean - prev.mean) * frac.clamp(0.0, 1.0));
            }

            cumulative += c.count as f64;

            if cumulative >= target {
                // Between this centroid mean and the next.
                if i + 1 < self.centroids.len() {
                    let next = &self.centroids[i + 1];
                    let left = cumulative - half;
                    let next_half = next.count as f64 / 2.0;
                    let right = cumulative + next_half;
                    if (right - left).abs() < f64::EPSILON {
                        return Ok((c.mean + next.mean) / 2.0);
                    }
                    let frac = (target - left) / (right - left);
                    return Ok(c.mean + (next.mean - c.mean) * frac.clamp(0.0, 1.0));
                }
                // Last centroid — interpolate towards max.
                let width = self.max_val - c.mean;
                let remaining = self.total_count as f64 - cumulative + half;
                if remaining > 0.0 {
                    let frac = (target - (cumulative - half)) / remaining;
                    return Ok(c.mean + width * frac.clamp(0.0, 1.0));
                }
                return Ok(c.mean);
            }
        }

        Ok(self.max_val)
    }

    /// Merge another T-digest into this one.
    pub fn merge(&mut self, other: &TDigest) {
        if other.centroids.is_empty() {
            return;
        }
        if other.min_val < self.min_val {
            self.min_val = other.min_val;
        }
        if other.max_val > self.max_val {
            self.max_val = other.max_val;
        }
        self.total_count += other.total_count;

        // Merge all centroids.
        let mut all: Vec<Centroid> = self
            .centroids
            .drain(..)
            .chain(other.centroids.iter().cloned())
            .collect();
        all.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        self.centroids = all;
        self.compress();
    }

    /// Return the total count of values added.
    pub fn count(&self) -> u64 {
        self.total_count
    }

    /// Return the minimum value seen.
    pub fn min(&self) -> f64 {
        self.min_val
    }

    /// Return the maximum value seen.
    pub fn max(&self) -> f64 {
        self.max_val
    }

    /// Serialize the T-digest to bytes.
    ///
    /// Format: `[version: u8][max_centroids: u32 LE][total_count: u64 LE]
    ///          [min: f64 LE][max: f64 LE][n_centroids: u32 LE]
    ///          [mean_0: f64 LE][count_0: u64 LE]...`
    pub fn serialize(&self) -> Vec<u8> {
        let n = self.centroids.len() as u32;
        let cap = 1 + 4 + 8 + 8 + 8 + 4 + (self.centroids.len() * 16);
        let mut buf = Vec::with_capacity(cap);
        let _ = buf.write_u8(VERSION);
        let _ = buf.write_u32::<LittleEndian>(self.max_centroids as u32);
        let _ = buf.write_u64::<LittleEndian>(self.total_count);
        let _ = buf.write_f64::<LittleEndian>(self.min_val);
        let _ = buf.write_f64::<LittleEndian>(self.max_val);
        let _ = buf.write_u32::<LittleEndian>(n);
        for c in &self.centroids {
            let _ = buf.write_f64::<LittleEndian>(c.mean);
            let _ = buf.write_u64::<LittleEndian>(c.count);
        }
        buf
    }

    /// Deserialize a T-digest from bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::Serialization`] on invalid data.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let min_header = 1 + 4 + 8 + 8 + 8 + 4; // 33 bytes
        if data.len() < min_header {
            return Err(SketchError::Serialization(
                "data too short for TDigest header".into(),
            ));
        }
        let mut cursor = Cursor::new(data);
        let version = cursor
            .read_u8()
            .map_err(|e| SketchError::Serialization(e.to_string()))?;
        if version != VERSION {
            return Err(SketchError::Serialization(format!(
                "unsupported TDigest version {version}"
            )));
        }
        let max_centroids = cursor
            .read_u32::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))?
            as usize;
        let total_count = cursor
            .read_u64::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))?;
        let min_val = cursor
            .read_f64::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))?;
        let max_val = cursor
            .read_f64::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))?;
        let n = cursor
            .read_u32::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))? as usize;

        // DD R32: compute the expected length with CHECKED arithmetic. `n` is an
        // untrusted u32; `n * 16` (or the `+ min_header`) can wrap on 32-bit
        // targets, making `expected` collapse below `data.len()` so the guard
        // passes and `Vec::with_capacity(n)` then reserves a huge buffer from a
        // tiny input. With the overflow rejected, the length guard bounds `n` to
        // the actual payload size (16 bytes/centroid), so the reservation is safe.
        let expected = n
            .checked_mul(16)
            .and_then(|body| min_header.checked_add(body))
            .ok_or_else(|| {
                SketchError::Serialization(
                    "TDigest centroid count overflows the addressable length".to_string(),
                )
            })?;
        if data.len() < expected {
            return Err(SketchError::Serialization(format!(
                "expected at least {expected} bytes but got {}",
                data.len()
            )));
        }

        let mut centroids = Vec::with_capacity(n);
        for _ in 0..n {
            let mean = cursor
                .read_f64::<LittleEndian>()
                .map_err(|e| SketchError::Serialization(e.to_string()))?;
            let count = cursor
                .read_u64::<LittleEndian>()
                .map_err(|e| SketchError::Serialization(e.to_string()))?;
            centroids.push(Centroid { mean, count });
        }

        Ok(Self {
            centroids,
            total_count,
            max_centroids,
            min_val,
            max_val,
        })
    }

    /// Compress centroids to stay within the budget.
    ///
    /// Uses a scale function that gives smaller centroids near the tails
    /// (q ≈ 0 and q ≈ 1) for better extreme-quantile accuracy.
    fn compress(&mut self) {
        if self.centroids.len() <= self.max_centroids {
            return;
        }
        // Sort by mean (should already be sorted, but ensure it).
        self.centroids.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let delta = self.max_centroids as f64;
        let total = self.total_count as f64;
        let mut result: Vec<Centroid> = Vec::with_capacity(self.max_centroids);
        let mut cumulative: f64 = 0.0;

        for c in self.centroids.drain(..) {
            if result.is_empty() {
                cumulative += c.count as f64;
                result.push(c);
                continue;
            }

            let last = result.last().expect("result is non-empty");
            let q = cumulative / total;
            // Scale function k(q) = delta / 2 * (asin(2q - 1) / pi + 0.5)
            // Weight limit at quantile q:
            let limit = weight_limit(q, delta, total);

            if last.count + c.count <= limit as u64 {
                // Merge into the last centroid.
                let merged_count = last.count + c.count;
                let merged_mean =
                    (last.mean * last.count as f64 + c.mean * c.count as f64) / merged_count as f64;
                let last_mut = result.last_mut().expect("result is non-empty");
                last_mut.mean = merged_mean;
                last_mut.count = merged_count;
            } else {
                result.push(c);
            }
            cumulative = result.iter().map(|r| r.count as f64).sum();
        }

        self.centroids = result;
    }
}

/// Compute the maximum weight a centroid is allowed to have at quantile `q`.
fn weight_limit(q: f64, delta: f64, total: f64) -> f64 {
    // k(q) = (delta / (2 * pi)) * asin(2q - 1) — derivative gives the
    // limit.  A simpler approach: 4 * total * q * (1 - q) / delta, which
    // is the parabolic scale function and works well in practice.
    let limit = 4.0 * total * q * (1.0 - q) / delta;
    // Ensure at least 1.
    limit.max(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_quantile_error() {
        let td = TDigest::new(100);
        assert!(matches!(td.quantile(0.5), Err(SketchError::Empty)));
    }

    #[test]
    fn invalid_quantile() {
        let mut td = TDigest::new(100);
        td.add(1.0);
        assert!(matches!(
            td.quantile(-0.1),
            Err(SketchError::InvalidQuantile(_))
        ));
        assert!(matches!(
            td.quantile(1.1),
            Err(SketchError::InvalidQuantile(_))
        ));
    }

    #[test]
    fn single_value_all_quantiles() {
        let mut td = TDigest::new(100);
        td.add(42.0);
        for &q in &[0.0, 0.25, 0.5, 0.75, 1.0] {
            let v = td.quantile(q).expect("should succeed");
            assert!(
                (v - 42.0).abs() < f64::EPSILON,
                "q={q}: expected 42.0, got {v}"
            );
        }
    }

    #[test]
    fn uniform_thousand_values() {
        let mut td = TDigest::new(100);
        for i in 1..=1000 {
            td.add(i as f64);
        }

        let p50 = td.quantile(0.50).expect("p50");
        let p95 = td.quantile(0.95).expect("p95");
        let p99 = td.quantile(0.99).expect("p99");

        // Expected: p50=500, p95=950, p99=990.  Allow 5% error.
        assert!(
            (p50 - 500.0).abs() / 500.0 < 0.05,
            "p50={p50}, expected ~500"
        );
        assert!(
            (p95 - 950.0).abs() / 950.0 < 0.05,
            "p95={p95}, expected ~950"
        );
        assert!(
            (p99 - 990.0).abs() / 990.0 < 0.05,
            "p99={p99}, expected ~990"
        );
    }

    #[test]
    fn q_zero_returns_min() {
        let mut td = TDigest::new(100);
        for i in 1..=100 {
            td.add(i as f64);
        }
        let v = td.quantile(0.0).expect("q=0");
        assert!(
            (v - 1.0).abs() < f64::EPSILON,
            "q=0 should return min=1.0, got {v}"
        );
    }

    #[test]
    fn q_one_returns_max() {
        let mut td = TDigest::new(100);
        for i in 1..=100 {
            td.add(i as f64);
        }
        let v = td.quantile(1.0).expect("q=1");
        assert!(
            (v - 100.0).abs() < f64::EPSILON,
            "q=1 should return max=100.0, got {v}"
        );
    }

    #[test]
    fn merge_two_digests() {
        let mut a = TDigest::new(100);
        let mut b = TDigest::new(100);
        for i in 1..=500 {
            a.add(i as f64);
        }
        for i in 501..=1000 {
            b.add(i as f64);
        }
        a.merge(&b);
        assert_eq!(a.count(), 1000);

        let p50 = a.quantile(0.50).expect("p50");
        assert!(
            (p50 - 500.0).abs() / 500.0 < 0.10,
            "merged p50={p50}, expected ~500"
        );
    }

    #[test]
    fn serialize_deserialize_round_trip() {
        let mut td = TDigest::new(50);
        for i in 1..=200 {
            td.add(i as f64);
        }
        let bytes = td.serialize();
        let restored = TDigest::deserialize(&bytes).expect("deserialize");
        assert_eq!(td.count(), restored.count());
        let orig_p50 = td.quantile(0.5).expect("p50");
        let rest_p50 = restored.quantile(0.5).expect("p50");
        assert!(
            (orig_p50 - rest_p50).abs() < f64::EPSILON,
            "round-trip p50 mismatch: {orig_p50} vs {rest_p50}"
        );
    }

    #[test]
    fn deserialize_too_short() {
        assert!(TDigest::deserialize(&[]).is_err());
    }

    #[test]
    fn deserialize_rejects_huge_centroid_count() {
        // DD R32: a small buffer that declares a huge centroid count must be
        // rejected (checked length arithmetic), never reach a huge
        // Vec::with_capacity. The centroid-count `n` is the last u32 of the
        // 33-byte header (offset 29..33).
        let mut td = TDigest::new(50);
        for i in 1..=10 {
            td.add(i as f64);
        }
        let mut bytes = td.serialize();
        // Overwrite `n` with a value whose *16 length would overflow a 32-bit
        // usize (and is absurdly larger than the buffer on 64-bit).
        bytes[29..33].copy_from_slice(&0x1000_0000u32.to_le_bytes());
        let err = TDigest::deserialize(&bytes)
            .expect_err("a huge declared centroid count must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("expected at least") || msg.contains("overflows"),
            "expected a length/overflow error, got: {msg}"
        );
    }

    #[test]
    fn min_max_tracking() {
        let mut td = TDigest::new(100);
        td.add(10.0);
        td.add(5.0);
        td.add(20.0);
        assert!((td.min() - 5.0).abs() < f64::EPSILON);
        assert!((td.max() - 20.0).abs() < f64::EPSILON);
    }
}
