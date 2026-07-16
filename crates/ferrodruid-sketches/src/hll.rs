// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! HyperLogLog sketch for cardinality estimation.
//!
//! Implements the standard HyperLogLog algorithm with bias correction
//! (linear counting for small cardinalities, harmonic mean with alpha
//! correction factor for larger ones).

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

use crate::{Result, SketchError};

/// Current serialization format version.
const VERSION: u8 = 1;

/// HyperLogLog sketch for cardinality estimation.
///
/// Uses `2^precision` registers (each one byte) to probabilistically
/// count distinct elements.  Precision must be in `4..=18`; the default
/// is 14 (16 384 registers, ~0.8 % standard error).
#[derive(Debug, Clone)]
pub struct HllSketch {
    precision: u8,
    registers: Vec<u8>,
}

impl HllSketch {
    /// Create a new HLL sketch with the given precision.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidPrecision`] if `precision` is outside
    /// the valid range `4..=18`.
    pub fn new(precision: u8) -> Result<Self> {
        if !(4..=18).contains(&precision) {
            return Err(SketchError::InvalidPrecision(precision));
        }
        let m = 1_usize << precision;
        Ok(Self {
            precision,
            registers: vec![0u8; m],
        })
    }

    /// Create a new HLL sketch with the default precision of 14.
    pub fn default_precision() -> Self {
        // p=14 is always valid.
        Self {
            precision: 14,
            registers: vec![0u8; 1 << 14],
        }
    }

    /// Add a value by hashing it internally.
    pub fn add(&mut self, value: &[u8]) {
        let hash = hash64(value);
        self.add_hash(hash);
    }

    /// Add a pre-computed 64-bit hash.
    pub fn add_hash(&mut self, hash: u64) {
        let p = self.precision as u32;
        let idx = (hash >> (64 - p)) as usize;
        // Take the remaining (64-p) bits, counting leading zeros.
        let remaining = if p < 64 {
            (hash << p) | (1_u64 << (p - 1))
        } else {
            1_u64
        };
        let run = (remaining.leading_zeros() + 1) as u8;
        if run > self.registers[idx] {
            self.registers[idx] = run;
        }
    }

    /// Estimate the cardinality.
    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let alpha = alpha_m(self.registers.len());

        // Harmonic mean of 2^(-register).
        let mut sum: f64 = 0.0;
        let mut zeros: usize = 0;
        for &r in &self.registers {
            sum += f64::from(2_u32).powi(-(i32::from(r)));
            if r == 0 {
                zeros += 1;
            }
        }

        let raw_estimate = alpha * m * m / sum;

        // Small-range correction (linear counting).
        if raw_estimate <= 2.5 * m && zeros > 0 {
            return m * (m / zeros as f64).ln();
        }

        // Large-range correction (for 32-bit hash space — not needed for
        // 64-bit hashes, but included for completeness).
        let two_32: f64 = (1_u64 << 32) as f64;
        if raw_estimate > two_32 / 30.0 {
            return -two_32 * (1.0 - raw_estimate / two_32).ln();
        }

        raw_estimate
    }

    /// Merge another HLL sketch into this one.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::PrecisionMismatch`] if precisions differ.
    pub fn merge(&mut self, other: &HllSketch) -> Result<()> {
        if self.precision != other.precision {
            return Err(SketchError::PrecisionMismatch(
                self.precision,
                other.precision,
            ));
        }
        for (dst, &src) in self.registers.iter_mut().zip(other.registers.iter()) {
            if src > *dst {
                *dst = src;
            }
        }
        Ok(())
    }

    /// Serialize the sketch to bytes.
    ///
    /// Format: `[version: u8][precision: u8][registers: M bytes]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + self.registers.len());
        // These writes to a Vec cannot fail.
        let _ = buf.write_u8(VERSION);
        let _ = buf.write_u8(self.precision);
        buf.extend_from_slice(&self.registers);
        buf
    }

    /// Deserialize a sketch from bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::Serialization`] on invalid data and
    /// [`SketchError::InvalidPrecision`] if the stored precision is out of
    /// range.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 2 {
            return Err(SketchError::Serialization(
                "data too short for HLL header".into(),
            ));
        }
        let mut cursor = Cursor::new(data);
        let version = cursor
            .read_u8()
            .map_err(|e| SketchError::Serialization(e.to_string()))?;
        if version != VERSION {
            return Err(SketchError::Serialization(format!(
                "unsupported HLL version {version}"
            )));
        }
        let precision = cursor
            .read_u8()
            .map_err(|e| SketchError::Serialization(e.to_string()))?;
        if !(4..=18).contains(&precision) {
            return Err(SketchError::InvalidPrecision(precision));
        }
        let expected = 1_usize << precision;
        let registers = &data[2..];
        if registers.len() != expected {
            return Err(SketchError::Serialization(format!(
                "expected {} register bytes but got {}",
                expected,
                registers.len()
            )));
        }
        Ok(Self {
            precision,
            registers: registers.to_vec(),
        })
    }

    /// Return the precision parameter.
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Return the number of registers (`2^precision`).
    pub fn num_registers(&self) -> usize {
        self.registers.len()
    }
}

/// Alpha constant used in the harmonic-mean estimator.
fn alpha_m(m: usize) -> f64 {
    match m {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / m as f64),
    }
}

/// 64-bit FNV-1a hash (deterministic, no randomness).
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
    fn empty_sketch_estimate_near_zero() {
        let sketch = HllSketch::default_precision();
        assert!(sketch.estimate() < 1.0, "empty sketch should estimate ~0");
    }

    #[test]
    fn thousand_unique_values() {
        let mut sketch = HllSketch::default_precision();
        for i in 0_u32..1000 {
            sketch.add(&i.to_le_bytes());
        }
        let est = sketch.estimate();
        let error = (est - 1000.0).abs() / 1000.0;
        assert!(
            error < 0.05,
            "estimate {est} should be within 5% of 1000 (error={error})"
        );
    }

    #[test]
    fn duplicate_values_estimate_one() {
        let mut sketch = HllSketch::default_precision();
        for _ in 0..1000 {
            sketch.add(b"same-value");
        }
        let est = sketch.estimate();
        assert!(
            est < 2.0,
            "adding same value 1000 times should estimate ~1, got {est}"
        );
    }

    #[test]
    fn merge_non_overlapping() {
        let mut a = HllSketch::new(10).expect("valid precision");
        let mut b = HllSketch::new(10).expect("valid precision");
        for i in 0_u32..500 {
            a.add(&i.to_le_bytes());
        }
        for i in 500_u32..1000 {
            b.add(&i.to_le_bytes());
        }
        a.merge(&b).expect("merge should succeed");
        let est = a.estimate();
        let error = (est - 1000.0).abs() / 1000.0;
        assert!(
            error < 0.10,
            "merged estimate {est} should be within 10% of 1000 (error={error})"
        );
    }

    #[test]
    fn serialize_deserialize_round_trip() {
        let mut sketch = HllSketch::new(8).expect("valid precision");
        for i in 0_u32..200 {
            sketch.add(&i.to_le_bytes());
        }
        let bytes = sketch.serialize();
        let restored = HllSketch::deserialize(&bytes).expect("deserialize should succeed");
        assert_eq!(sketch.precision(), restored.precision());
        assert!((sketch.estimate() - restored.estimate()).abs() < f64::EPSILON);
    }

    #[test]
    fn invalid_precision_low() {
        assert!(matches!(
            HllSketch::new(3),
            Err(SketchError::InvalidPrecision(3))
        ));
    }

    #[test]
    fn invalid_precision_high() {
        assert!(matches!(
            HllSketch::new(19),
            Err(SketchError::InvalidPrecision(19))
        ));
    }

    #[test]
    fn merge_precision_mismatch() {
        let mut a = HllSketch::new(10).expect("ok");
        let b = HllSketch::new(12).expect("ok");
        assert!(matches!(
            a.merge(&b),
            Err(SketchError::PrecisionMismatch(10, 12))
        ));
    }
}
