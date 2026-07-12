// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Theta sketch for set-operation cardinality estimation.
//!
//! A Theta sketch retains a set of hash values below a dynamically adjusted
//! threshold *theta*.  Because theta is shared across sketches, set operations
//! (union, intersection, difference) can be performed directly on the retained
//! hash sets.

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::BTreeSet;
use std::io::Cursor;

use crate::{Result, SketchError};

/// Current serialization format version.
const VERSION: u8 = 1;

/// Theta sketch for set-operation cardinality estimation.
#[derive(Debug, Clone)]
pub struct ThetaSketch {
    /// Threshold in `(0.0, 1.0]`.  Only hashes whose normalised value is
    /// strictly below theta are retained.
    theta: f64,
    /// Maximum number of retained hashes before theta is lowered.
    max_size: usize,
    /// Retained hash values (sorted via `BTreeSet`).
    hashes: BTreeSet<u64>,
}

impl ThetaSketch {
    /// Create a new Theta sketch with the given maximum retained-hash size.
    pub fn new(max_size: usize) -> Self {
        Self {
            theta: 1.0,
            max_size,
            hashes: BTreeSet::new(),
        }
    }

    /// Create a new Theta sketch with the default size of 4096.
    pub fn default_size() -> Self {
        Self::new(4096)
    }

    /// Add a value by hashing it internally.
    pub fn add(&mut self, value: &[u8]) {
        let hash = hash64(value);
        self.add_hash(hash);
    }

    /// Add a pre-computed 64-bit hash.
    pub fn add_hash(&mut self, hash: u64) {
        let norm = normalise(hash);
        if norm >= self.theta {
            return;
        }
        self.hashes.insert(hash);
        self.trim();
    }

    /// Estimate the cardinality.
    ///
    /// When theta is still 1.0 (sketch has not yet reached capacity) the
    /// estimate is simply the number of retained hashes.  Otherwise it is
    /// `retained / theta`.
    pub fn estimate(&self) -> f64 {
        if self.hashes.is_empty() {
            return 0.0;
        }
        if (self.theta - 1.0).abs() < f64::EPSILON {
            return self.hashes.len() as f64;
        }
        self.hashes.len() as f64 / self.theta
    }

    /// Return the union of two sketches.
    pub fn union(&self, other: &ThetaSketch) -> ThetaSketch {
        let min_theta = self.theta.min(other.theta);
        let max_size = self.max_size.max(other.max_size);
        let mut result = ThetaSketch {
            theta: min_theta,
            max_size,
            hashes: BTreeSet::new(),
        };
        for &h in self.hashes.iter().chain(other.hashes.iter()) {
            if normalise(h) < min_theta {
                result.hashes.insert(h);
            }
        }
        result.trim();
        result
    }

    /// Return the intersection of two sketches.
    pub fn intersect(&self, other: &ThetaSketch) -> ThetaSketch {
        let min_theta = self.theta.min(other.theta);
        let max_size = self.max_size.max(other.max_size);
        let mut result = ThetaSketch {
            theta: min_theta,
            max_size,
            hashes: BTreeSet::new(),
        };
        for &h in &self.hashes {
            if other.hashes.contains(&h) && normalise(h) < min_theta {
                result.hashes.insert(h);
            }
        }
        result
    }

    /// Return the set difference (A not B).
    pub fn difference(&self, other: &ThetaSketch) -> ThetaSketch {
        let min_theta = self.theta.min(other.theta);
        let max_size = self.max_size;
        let mut result = ThetaSketch {
            theta: min_theta,
            max_size,
            hashes: BTreeSet::new(),
        };
        for &h in &self.hashes {
            if !other.hashes.contains(&h) && normalise(h) < min_theta {
                result.hashes.insert(h);
            }
        }
        result
    }

    /// Serialize the sketch to bytes.
    ///
    /// Format: `[version: u8][theta: f64 LE][max_size: u32 LE][count: u32 LE][hashes: count × u64 LE]`
    pub fn serialize(&self) -> Vec<u8> {
        let count = self.hashes.len() as u32;
        let mut buf = Vec::with_capacity(1 + 8 + 4 + 4 + self.hashes.len() * 8);
        let _ = buf.write_u8(VERSION);
        let _ = buf.write_f64::<LittleEndian>(self.theta);
        let _ = buf.write_u32::<LittleEndian>(self.max_size as u32);
        let _ = buf.write_u32::<LittleEndian>(count);
        for &h in &self.hashes {
            let _ = buf.write_u64::<LittleEndian>(h);
        }
        buf
    }

    /// Deserialize a sketch from bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::Serialization`] on invalid data.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let min_header = 1 + 8 + 4 + 4; // version + theta + max_size + count
        if data.len() < min_header {
            return Err(SketchError::Serialization(
                "data too short for Theta header".into(),
            ));
        }
        let mut cursor = Cursor::new(data);
        let version = cursor
            .read_u8()
            .map_err(|e| SketchError::Serialization(e.to_string()))?;
        if version != VERSION {
            return Err(SketchError::Serialization(format!(
                "unsupported Theta version {version}"
            )));
        }
        let theta = cursor
            .read_f64::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))?;
        let max_size = cursor
            .read_u32::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))?
            as usize;
        let count = cursor
            .read_u32::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))? as usize;

        // DD R32: checked length arithmetic so an untrusted `count` cannot wrap
        // `count * 8` (32-bit) and bypass the length guard.
        let expected_len = count
            .checked_mul(8)
            .and_then(|body| min_header.checked_add(body))
            .ok_or_else(|| {
                SketchError::Serialization(
                    "theta sketch hash count overflows the addressable length".to_string(),
                )
            })?;
        if data.len() < expected_len {
            return Err(SketchError::Serialization(format!(
                "expected at least {expected_len} bytes but got {}",
                data.len()
            )));
        }
        let mut hashes = BTreeSet::new();
        for _ in 0..count {
            let h = cursor
                .read_u64::<LittleEndian>()
                .map_err(|e| SketchError::Serialization(e.to_string()))?;
            hashes.insert(h);
        }
        Ok(Self {
            theta,
            max_size,
            hashes,
        })
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Trim the set to `max_size` by lowering theta.
    fn trim(&mut self) {
        while self.hashes.len() > self.max_size {
            // Remove the largest hash and update theta.
            if let Some(&largest) = self.hashes.iter().next_back() {
                self.hashes.remove(&largest);
                self.theta = normalise(largest);
            }
        }
    }
}

/// Normalise a `u64` hash to `[0.0, 1.0)`.
fn normalise(hash: u64) -> f64 {
    hash as f64 / u64::MAX as f64
}

/// 64-bit FNV-1a hash (deterministic).
fn hash64(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sketch_estimate_zero() {
        let sketch = ThetaSketch::default_size();
        assert!((sketch.estimate() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn five_thousand_values() {
        let mut sketch = ThetaSketch::default_size();
        for i in 0_u32..5000 {
            sketch.add(&i.to_le_bytes());
        }
        let est = sketch.estimate();
        let error = (est - 5000.0).abs() / 5000.0;
        assert!(
            error < 0.10,
            "estimate {est} should be within 10% of 5000 (error={error})"
        );
    }

    #[test]
    fn union_disjoint_sets() {
        let mut a = ThetaSketch::new(4096);
        let mut b = ThetaSketch::new(4096);
        for i in 0_u32..2000 {
            a.add(&i.to_le_bytes());
        }
        for i in 2000_u32..4000 {
            b.add(&i.to_le_bytes());
        }
        let u = a.union(&b);
        let est = u.estimate();
        let error = (est - 4000.0).abs() / 4000.0;
        assert!(
            error < 0.15,
            "union estimate {est} should be near 4000 (error={error})"
        );
    }

    #[test]
    fn intersection_of_overlapping_sets() {
        let mut a = ThetaSketch::new(4096);
        let mut b = ThetaSketch::new(4096);
        // A = 0..3000, B = 1000..4000, overlap = 1000..3000 (2000 items)
        for i in 0_u32..3000 {
            a.add(&i.to_le_bytes());
        }
        for i in 1000_u32..4000 {
            b.add(&i.to_le_bytes());
        }
        let inter = a.intersect(&b);
        let est = inter.estimate();
        // Intersection should be approximately 2000.  Theta-sketch
        // intersection can have higher variance, so we allow 30%.
        let error = (est - 2000.0).abs() / 2000.0;
        assert!(
            error < 0.30,
            "intersection estimate {est} should be near 2000 (error={error})"
        );
    }

    #[test]
    fn difference_of_sets() {
        let mut a = ThetaSketch::new(4096);
        let mut b = ThetaSketch::new(4096);
        // A = 0..3000, B = 2000..4000 → A \ B ≈ 2000
        for i in 0_u32..3000 {
            a.add(&i.to_le_bytes());
        }
        for i in 2000_u32..4000 {
            b.add(&i.to_le_bytes());
        }
        let diff = a.difference(&b);
        let est = diff.estimate();
        let error = (est - 2000.0).abs() / 2000.0;
        assert!(
            error < 0.30,
            "difference estimate {est} should be near 2000 (error={error})"
        );
    }

    #[test]
    fn serialize_deserialize_round_trip() {
        let mut sketch = ThetaSketch::new(512);
        for i in 0_u32..1000 {
            sketch.add(&i.to_le_bytes());
        }
        let bytes = sketch.serialize();
        let restored = ThetaSketch::deserialize(&bytes).expect("deserialize should succeed");
        assert!((sketch.estimate() - restored.estimate()).abs() < f64::EPSILON);
    }

    #[test]
    fn deserialize_too_short() {
        assert!(ThetaSketch::deserialize(&[]).is_err());
    }
}
