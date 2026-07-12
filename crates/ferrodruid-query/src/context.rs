// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid query context parameters.
//!
//! The `context` object is an optional part of every Druid native query that
//! controls execution behaviour such as timeouts, cache usage, and
//! timeseries-specific options like `skipEmptyBuckets`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Druid query context parameters.
///
/// Any field not explicitly modeled lands in the `extra` catch-all map so that
/// unknown context keys are preserved during round-trip serialization.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryContext {
    /// Query timeout in milliseconds.
    #[serde(default)]
    pub timeout: Option<u64>,
    /// Query priority (lower = higher priority).
    #[serde(default)]
    pub priority: Option<i32>,
    /// Skip empty buckets in timeseries results.
    #[serde(default, rename = "skipEmptyBuckets")]
    pub skip_empty_buckets: Option<bool>,
    /// Finalize aggregations.
    #[serde(default)]
    pub finalize: Option<bool>,
    /// Use cache for this query.
    #[serde(default, rename = "useCache")]
    pub use_cache: Option<bool>,
    /// Populate cache with results of this query.
    #[serde(default, rename = "populateCache")]
    pub populate_cache: Option<bool>,
    /// Caller-supplied query identifier.
    #[serde(default, rename = "queryId")]
    pub query_id: Option<String>,
    /// Max scatter-gather bytes queued before back-pressure.
    #[serde(default, rename = "maxScatterGatherBytes")]
    pub max_scatter_gather_bytes: Option<u64>,
    /// Vectorize query processing.
    #[serde(default)]
    pub vectorize: Option<bool>,
    /// Additional context params (catch-all).
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl QueryContext {
    /// Returns `true` if `skipEmptyBuckets` is explicitly set to `true`.
    pub fn skip_empty_buckets(&self) -> bool {
        self.skip_empty_buckets.unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_context() {
        let ctx: QueryContext = serde_json::from_str("{}").expect("parse empty");
        assert_eq!(ctx.timeout, None);
        assert_eq!(ctx.priority, None);
        assert!(!ctx.skip_empty_buckets());
        assert!(ctx.extra.is_empty());
    }

    #[test]
    fn parse_full_context() {
        let json = r#"{
            "timeout": 30000,
            "priority": -1,
            "skipEmptyBuckets": true,
            "finalize": true,
            "useCache": false,
            "populateCache": false,
            "queryId": "abc-123",
            "maxScatterGatherBytes": 1073741824,
            "vectorize": true,
            "customKey": "customValue"
        }"#;
        let ctx: QueryContext = serde_json::from_str(json).expect("parse full");
        assert_eq!(ctx.timeout, Some(30000));
        assert_eq!(ctx.priority, Some(-1));
        assert!(ctx.skip_empty_buckets());
        assert_eq!(ctx.finalize, Some(true));
        assert_eq!(ctx.use_cache, Some(false));
        assert_eq!(ctx.populate_cache, Some(false));
        assert_eq!(ctx.query_id.as_deref(), Some("abc-123"));
        assert_eq!(ctx.max_scatter_gather_bytes, Some(1_073_741_824));
        assert_eq!(ctx.vectorize, Some(true));
        assert_eq!(
            ctx.extra.get("customKey"),
            Some(&serde_json::Value::String("customValue".to_string()))
        );
    }

    #[test]
    fn roundtrip_context() {
        let ctx = QueryContext {
            timeout: Some(5000),
            skip_empty_buckets: Some(true),
            query_id: Some("q1".to_string()),
            ..Default::default()
        };
        let json = serde_json::to_string(&ctx).expect("serialize");
        let ctx2: QueryContext = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(ctx2.timeout, Some(5000));
        assert!(ctx2.skip_empty_buckets());
        assert_eq!(ctx2.query_id.as_deref(), Some("q1"));
    }
}
