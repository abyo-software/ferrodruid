// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Search query type — find dimension values matching a search specification.

use std::collections::{BTreeMap, HashMap};

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
            // compat-11 R2: reject non-element-aware filters over an MV
            // column at plan time (comprehensive guard — see
            // `FilterSpec::ensure_multi_value_supported`).  Search queries
            // carry no virtual columns, so nothing shadows the segment.
            filter.ensure_multi_value_supported(
                segment,
                &crate::virtual_columns::VirtualColumns::compile(&None)?,
            )?;
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

        // bucket_key -> (dimension, value) -> matching occurrence count.
        // Re-audit Low (2026-07-19): Druid reports the actual number of
        // matching rows/value occurrences per (dimension, value) hit
        // (cursor strategy: one increment per matching element per row);
        // the pre-fix BTreeSet discarded multiplicity and stamped every
        // hit `count: 1`, silently flattening any client that ranks by
        // frequency (the standard autocomplete use).
        type BucketHits = BTreeMap<(String, String), u64>;
        let mut buckets: HashMap<i64, BucketHits> = HashMap::new();
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
                // compat-11: a multi-value row materialises as a JSON array
                // — Druid's search query matches (and reports) individual
                // ELEMENTS, so probe each element; scalar rows keep the
                // unchanged single-candidate path.
                let candidates: Vec<String> = match &val {
                    serde_json::Value::String(s) => vec![s.clone()],
                    serde_json::Value::Null => continue,
                    serde_json::Value::Array(elems) => elems
                        .iter()
                        .filter(|e| !e.is_null())
                        .map(|e| match e {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .collect(),
                    v => vec![v.to_string()],
                };

                for val_str in candidates {
                    if search_matches(&self.query, &val_str) {
                        *buckets
                            .entry(bucket_key)
                            .or_default()
                            .entry((dim.clone(), val_str))
                            .or_insert(0) += 1;
                    }
                }
            }
        }

        let mut bucket_entries: Vec<(i64, BucketHits)> = buckets.into_iter().collect();
        bucket_entries.sort_by_key(|&(key, _)| key);

        let mut results = Vec::with_capacity(bucket_entries.len());
        for (key, hits) in bucket_entries {
            let mut hit_list: Vec<SearchHit> = hits
                .into_iter()
                .map(|((dimension, value), count)| SearchHit {
                    dimension,
                    value,
                    count,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_segment::SegmentDataBuilder;

    fn search_query(spec: SearchQuerySpec) -> SearchQuery {
        SearchQuery {
            data_source: DataSource::Table { name: "t".into() },
            intervals: vec!["1970-01-01T00:00:00Z/2099-01-01T00:00:00Z".into()],
            granularity: default_granularity(),
            query: spec,
            search_dimensions: Some(vec!["page".into()]),
            filter: None,
            limit: None,
            sort: None,
            context: None,
        }
    }

    /// Re-audit Low (2026-07-19): `SearchHit.count` must report the
    /// actual number of matching occurrences per (dimension, value) —
    /// Druid returns e.g. `count: 3` for a value present in 3 rows; the
    /// pre-fix set-based dedup stamped every hit `count: 1`.
    #[test]
    fn search_hit_counts_report_matching_occurrences() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200, 300, 400])
            .add_string_column(
                "page",
                vec![
                    "Main_Page".into(),
                    "Main_Page".into(),
                    "Main_Street".into(),
                    "Other".into(),
                ],
            )
            .build()
            .expect("build segment");

        let q = search_query(SearchQuerySpec::Contains {
            value: "Main".into(),
        });
        let results = q.execute(&segment).expect("execute");
        assert_eq!(results.len(), 1, "granularity all → one bucket");
        let hits = &results[0].result;
        assert_eq!(hits.len(), 2, "two distinct matching values");
        let count_of = |value: &str| -> u64 {
            hits.iter()
                .find(|h| h.value == value)
                .map_or(0, |h| h.count)
        };
        assert_eq!(
            count_of("Main_Page"),
            2,
            "Main_Page appears in 2 rows — count must be 2, not 1"
        );
        assert_eq!(count_of("Main_Street"), 1);
        // Lexicographic default sort is preserved.
        assert_eq!(hits[0].value, "Main_Page");
        assert_eq!(hits[1].value, "Main_Street");
    }
}
