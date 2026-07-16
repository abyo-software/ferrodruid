// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Probabilistic sketch data structures for FerroDruid.
//!
//! This crate provides three core approximate data structures used for
//! streaming aggregations:
//!
//! - [`HllSketch`] — HyperLogLog for cardinality estimation
//! - [`ThetaSketch`] — Theta sketch for set-operation cardinality
//! - [`TDigest`] — T-digest for quantile (percentile) estimation

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod hll;
pub mod tdigest;
pub mod theta;

pub use hll::HllSketch;
pub use tdigest::TDigest;
pub use theta::ThetaSketch;

use ferrodruid_common::DruidError;

/// The type of sketch data structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SketchType {
    /// HyperLogLog for cardinality estimation.
    HyperLogLog,
    /// Theta sketch for set-operation cardinality.
    Theta,
    /// T-digest for quantile estimation.
    TDigest,
}

impl std::fmt::Display for SketchType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HyperLogLog => write!(f, "HyperLogLog"),
            Self::Theta => write!(f, "Theta"),
            Self::TDigest => write!(f, "T-Digest"),
        }
    }
}

/// Sketch-specific error type.
#[derive(Debug, thiserror::Error)]
pub enum SketchError {
    /// Invalid precision parameter.
    #[error("invalid precision {0}: must be between 4 and 18 inclusive")]
    InvalidPrecision(u8),

    /// Precision mismatch when merging sketches.
    #[error("precision mismatch: cannot merge p={0} with p={1}")]
    PrecisionMismatch(u8, u8),

    /// Invalid quantile parameter.
    #[error("invalid quantile {0}: must be between 0.0 and 1.0 inclusive")]
    InvalidQuantile(f64),

    /// Sketch is empty and cannot produce an estimate.
    #[error("sketch is empty")]
    Empty,

    /// Serialization or deserialization error.
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl From<SketchError> for DruidError {
    fn from(e: SketchError) -> Self {
        DruidError::Internal(e.to_string())
    }
}

/// Convenience result alias for sketch operations.
pub type Result<T> = std::result::Result<T, SketchError>;
