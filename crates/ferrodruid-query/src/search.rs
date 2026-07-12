// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Search query type — find dimension values matching a search specification.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use ferrodruid_common::error::Result;
use ferrodruid_common::types::{DataSource, SearchQuerySpec};
use ferrodruid_segment::SegmentData;

use crate::context::QueryContext;
use crate::filter::FilterSpec;
use crate::helpers::{
    GranularitySpec, bucket_timestamp, build_row_prealloc, build_row_update_only, column_value_at,
    deserialize_intervals, parse_intervals, validate_granularity,
};
use crate::timeseries::format_epoch_millis;

// ---------------------------------------------------------------------------
// Query spec
// ---------------------------------------------------------------------------

/// A Druid Search query.
///
/// Search queries return the set of dimension values that match a search
/// specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchQuery {
    /// The data source to query.
    pub data_source: DataSource,
    /// Time intervals to query over.
    ///
    /// Accepts both a single ISO `"start/end"` string and an array of
    /// such strings (TG-4-finding-001, W2-D pydruid/druid-go compat).
    #[serde(deserialize_with = "deserialize_intervals")]
    pub intervals: Vec<String>,
    /// Granularity for time bucketing.
    #[serde(default = "default_granularity")]
    pub granularity: GranularitySpec,
    /// The search specification.
    pub query: SearchQuerySpec,
    /// Dimensions to search (if empty, search all string dimensions).
    #[serde(default)]
    pub search_dimensions: Option<Vec<String>>,
    /// Optional filter.
    #[serde(default)]
    pub filter: Option<FilterSpec>,
    /// Maximum number of results.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Sort specification.
    #[serde(default)]
    pub sort: Option<SearchSortSpec>,
    /// Optional query context.
    #[serde(default)]
    pub context: Option<QueryContext>,
}

/// Default granularity for search queries: all.
fn default_granularity() -> GranularitySpec {
    GranularitySpec::Simple("all".to_string())
}

/// Sort specification for search results.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchSortSpec {
    /// Sort type: `"lexicographic"` or `"strlen"`.
    #[serde(rename = "type")]
    pub typ: String,
}

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

/// A single search result entry (one per time bucket).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// The bucket timestamp.
    pub timestamp: String,
    /// Matching dimension values.
    pub result: Vec<SearchHit>,
}

/// A single search hit — a dimension name + value pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    /// The dimension name.
    pub dimension: String,
    /// The matching value.
    pub value: String,
    /// The count of matching rows.
    pub count: u64,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

impl SearchQuery {
    /// Execute this Search query against a segment.
    pub fn execute(&self, segment: &SegmentData) -> Result<Vec<SearchResult>> {
        // DD R40: reject a malformed expression filter up front instead of
        // letting it silently match every row (fail-open data exposure).
        if let Some(ref filter) = self.filter {
            filter.validate()?;
        }
        // DD R48: reject a duration granularity with periodMs == 0 / out-of-i64.
        validate_granularity(&self.granularity)?;

        let intervals = parse_intervals(&self.intervals)?;
        let timestamps = segment.timestamp_column()?;

        // Determine which dimensions to search.
        let dims: Vec<String> = match &self.search_dimensions {
            Some(d) if !d.is_empty() => d.clone(),
            _ => segment.dimensions.clone(),
        };

        // bucket_key -> set of (dimension, value)
        let mut buckets: HashMap<i64, BTreeSet<(String, String)>> = HashMap::new();
        // W3-SL1-B step 1: hoist row-map allocation out of the loop.
        let mut row = build_row_prealloc(segment);

        for (row_idx, &ts) in timestamps.iter().enumerate().take(segment.num_rows()) {
            if !intervals.is_empty()
                && !intervals
                    .iter()
                    .any(|(start, end)| ts >= *start && ts < *end)
            {
                continue;
            }

            if let Some(ref filter) = self.filter {
                build_row_update_only(segment, row_idx, &mut row);
                if !filter.matches(&row) {
                    continue;
                }
            }

            let bucket_key = bucket_timestamp(ts, &self.granularity);

            for dim in &dims {
                let val = segment
                    .columns
                    .get(dim)
                    .map(|col| column_value_at(col, row_idx))
                    .unwrap_or(serde_json::Value::Null);
                let val_str = match &val {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Null => continue,
                    v => v.to_string(),
                };

                if search_matches(&self.query, &val_str) {
                    buckets
                        .entry(bucket_key)
                        .or_default()
                        .insert((dim.clone(), val_str));
                }
            }
        }

        let mut bucket_keys: Vec<i64> = buckets.keys().copied().collect();
        bucket_keys.sort();

        let mut results = Vec::with_capacity(bucket_keys.len());
        for key in bucket_keys {
            let hits = buckets.get(&key).expect("bucket exists");
            let mut hit_list: Vec<SearchHit> = hits
                .iter()
                .map(|(dim, val)| SearchHit {
                    dimension: dim.clone(),
                    value: val.clone(),
                    count: 1,
                })
                .collect();

            // Apply sort.
            match self.sort.as_ref().map(|s| s.typ.as_str()) {
                Some("strlen") => hit_list.sort_by(|a, b| {
                    a.value
                        .len()
                        .cmp(&b.value.len())
                        .then(a.value.cmp(&b.value))
                }),
                _ => hit_list.sort_by(|a, b| a.value.cmp(&b.value)),
            }

            // Apply limit.
            if let Some(limit) = self.limit {
                hit_list.truncate(limit);
            }

            results.push(SearchResult {
                timestamp: format_epoch_millis(key),
                result: hit_list,
            });
        }

        Ok(results)
    }
}

/// Check if a value matches a search query specification.
fn search_matches(query: &SearchQuerySpec, val: &str) -> bool {
    match query {
        SearchQuerySpec::Contains { value } => val.contains(value.as_str()),
        SearchQuerySpec::InsensitiveContains { value } => {
            val.to_lowercase().contains(&value.to_lowercase())
        }
        SearchQuerySpec::Fragment {
            values,
            case_sensitive,
        } => {
            if *case_sensitive {
                values.iter().all(|frag| val.contains(frag.as_str()))
            } else {
                let lower = val.to_lowercase();
                values
                    .iter()
                    .all(|frag| lower.contains(&frag.to_lowercase()))
            }
        }
        SearchQuerySpec::Regex { pattern } => regex::Regex::new(pattern)
            .map(|re| re.is_match(val))
            .unwrap_or(false),
    }
}
