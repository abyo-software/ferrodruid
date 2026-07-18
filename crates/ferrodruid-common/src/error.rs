// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Error types for FerroDruid.

use thiserror::Error;

/// Unified error type for all FerroDruid operations.
#[derive(Error, Debug)]
pub enum DruidError {
    /// A query-processing error.
    #[error("query error: {0}")]
    Query(String),

    /// A segment-level error (read, write, or corrupt data).
    #[error("segment error: {0}")]
    Segment(String),

    /// An ingestion pipeline error.
    #[error("ingestion error: {0}")]
    Ingestion(String),

    /// A metadata store error.
    #[error("metadata error: {0}")]
    Metadata(String),

    /// An authentication / authorisation error.
    #[error("auth error: {0}")]
    Auth(String),

    /// A configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// An I/O error.
    #[error("io error: {source}")]
    Io {
        /// The underlying I/O error.
        #[from]
        source: std::io::Error,
    },

    /// A JSON serialization / deserialization error.
    #[error("json error: {source}")]
    Json {
        /// The underlying serde_json error.
        #[from]
        source: serde_json::Error,
    },

    /// An internal (unexpected) error.
    #[error("internal error: {0}")]
    Internal(String),

    /// A resource-limit guard fired before a query could complete.
    ///
    /// Typed as a separate variant so callers (e.g. the REST layer) can
    /// translate this into a `429 Too Many Keys` rather than a generic
    /// `500 Internal Server Error`.  Wave 36-G1 (Wave 37B query Top-1
    /// finding): high-cardinality TopN/GroupBy queries previously could
    /// drive historical memory to OOM via an unbounded per-key
    /// `HashMap<String, Vec<Box<dyn Aggregator>>>` insertion path.
    #[error("resource limit exceeded: {kind} (limit={limit}, observed={observed})")]
    ResourceLimit {
        /// Which limit fired (e.g. `"groupBy.maxResults"`,
        /// `"topN.maxIntermediateRows"`).
        kind: &'static str,
        /// The configured upper bound.
        limit: usize,
        /// The observed value at the moment the guard fired.
        observed: usize,
    },
}

/// Convenience alias used throughout FerroDruid crates.
pub type Result<T> = std::result::Result<T, DruidError>;
