// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Metadata query types: segmentMetadata, dataSourceMetadata, and timeBoundary.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use ferrodruid_common::error::Result;
use ferrodruid_common::types::DataSource;
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::ColumnData;

use crate::context::QueryContext;
use crate::helpers::deserialize_optional_intervals;
use crate::timeseries::format_epoch_millis;

// ===========================================================================
// SegmentMetadata
// ===========================================================================

/// A Druid segmentMetadata query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentMetadataQuery {
    /// The data source to query.
    pub data_source: DataSource,
    /// Time intervals to query over.
    ///
    /// Accepts both a single ISO `"start/end"` string and an array of
    /// such strings (TG-4-finding-001, W2-D pydruid/druid-go compat).
    #[serde(default, deserialize_with = "deserialize_optional_intervals")]
    pub intervals: Option<Vec<String>>,
    /// Optional query context.
    #[serde(default)]
    pub context: Option<QueryContext>,
}

/// The result of a segmentMetadata query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentMetadataResult {
    /// Segment identifier string.
    pub id: String,
    /// Time intervals covered.
    pub intervals: Vec<String>,
    /// Per-column metadata.
    pub columns: HashMap<String, ColumnMetadata>,
    /// Total number of rows.
    pub num_rows: usize,
}

/// Metadata for a single column in a segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMetadata {
    /// The column value type (e.g. `"LONG"`, `"STRING"`).
    #[serde(rename = "type")]
    pub typ: String,
    /// Whether the column has multiple values per row.
    pub has_multiple_values: bool,
    /// Estimated size in bytes.
    pub size: u64,
    /// Cardinality (number of unique values), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<u64>,
}

impl SegmentMetadataQuery {
    /// Execute this segmentMetadata query against a segment.
    pub fn execute(&self, segment: &SegmentData) -> Result<Vec<SegmentMetadataResult>> {
        let interval_str = format!(
            "{}/{}",
            format_epoch_millis(segment.interval.start_millis),
            format_epoch_millis(segment.interval.end_millis),
        );

        let mut columns = HashMap::new();
        for (name, col) in &segment.columns {
            let (typ, cardinality) = match col {
                ColumnData::Long(v) => ("LONG".to_string(), Some(v.len() as u64)),
                // Nullable longs are still LONG-typed to callers (the null
                // bitmap is a storage detail, not a type change).
                ColumnData::LongNullable(v, _) => ("LONG".to_string(), Some(v.len() as u64)),
                ColumnData::Float(v) => ("FLOAT".to_string(), Some(v.len() as u64)),
                ColumnData::Double(v) => ("DOUBLE".to_string(), Some(v.len() as u64)),
                ColumnData::String(sc) => ("STRING".to_string(), Some(sc.dictionary.len() as u64)),
                // Multi-value string dimension (compat-11): STRING-typed
                // with element-dictionary cardinality, reported to callers
                // via `hasMultipleValues: true` below (Druid convention).
                ColumnData::StringMulti(mc) => {
                    ("STRING".to_string(), Some(mc.dictionary.len() as u64))
                }
                ColumnData::Complex(_) => ("COMPLEX".to_string(), None),
                // Decoded theta column (compat-8 sketch #2): report the
                // parameterized complex type the way modern Druid's
                // segmentMetadata does.
                ColumnData::ComplexTheta(_) => ("COMPLEX<thetaSketch>".to_string(), None),
            };
            columns.insert(
                name.clone(),
                ColumnMetadata {
                    typ,
                    has_multiple_values: matches!(col, ColumnData::StringMulti(_)),
                    size: col.num_rows().unwrap_or(0) as u64 * 8,
                    cardinality,
                },
            );
        }

        Ok(vec![SegmentMetadataResult {
            id: "segment_0".to_string(),
            intervals: vec![interval_str],
            columns,
            num_rows: segment.num_rows(),
        }])
    }
}

// ===========================================================================
// DataSourceMetadata
// ===========================================================================

/// A Druid dataSourceMetadata query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataSourceMetadataQuery {
    /// The data source to query.
    pub data_source: DataSource,
    /// Optional query context.
    #[serde(default)]
    pub context: Option<QueryContext>,
}

/// The result of a dataSourceMetadata query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceMetadataResult {
    /// The timestamp of the latest data point.
    pub timestamp: String,
    /// Result containing the max ingested event time.
    pub result: DataSourceMetadataResultInner,
}

/// Inner result for dataSourceMetadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataSourceMetadataResultInner {
    /// Maximum ingested event timestamp (ISO-8601).
    pub max_ingested_event_time: String,
}

impl DataSourceMetadataQuery {
    /// Execute this dataSourceMetadata query against a segment.
    pub fn execute(&self, segment: &SegmentData) -> Result<Vec<DataSourceMetadataResult>> {
        let timestamps = segment.timestamp_column()?;
        let max_ts = timestamps.iter().copied().max().unwrap_or(0);
        Ok(vec![DataSourceMetadataResult {
            timestamp: format_epoch_millis(max_ts),
            result: DataSourceMetadataResultInner {
                max_ingested_event_time: format_epoch_millis(max_ts),
            },
        }])
    }
}

// ===========================================================================
// TimeBoundary
// ===========================================================================

/// A Druid timeBoundary query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeBoundaryQuery {
    /// The data source to query.
    pub data_source: DataSource,
    /// Optional bound: `"maxTime"`, `"minTime"`, or both if absent.
    #[serde(default)]
    pub bound: Option<String>,
    /// Optional query context.
    #[serde(default)]
    pub context: Option<QueryContext>,
}

/// The result of a timeBoundary query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeBoundaryResult {
    /// A timestamp (typically the min time).
    pub timestamp: String,
    /// The result object containing min/max time.
    pub result: serde_json::Map<String, serde_json::Value>,
}

impl TimeBoundaryQuery {
    /// Execute this timeBoundary query against a segment.
    pub fn execute(&self, segment: &SegmentData) -> Result<Vec<TimeBoundaryResult>> {
        let timestamps = segment.timestamp_column()?;
        let min_ts = timestamps.iter().copied().min().unwrap_or(0);
        let max_ts = timestamps.iter().copied().max().unwrap_or(0);

        let mut result = serde_json::Map::new();
        match self.bound.as_deref() {
            Some("maxTime") => {
                result.insert(
                    "maxTime".to_string(),
                    serde_json::Value::String(format_epoch_millis(max_ts)),
                );
            }
            Some("minTime") => {
                result.insert(
                    "minTime".to_string(),
                    serde_json::Value::String(format_epoch_millis(min_ts)),
                );
            }
            _ => {
                result.insert(
                    "minTime".to_string(),
                    serde_json::Value::String(format_epoch_millis(min_ts)),
                );
                result.insert(
                    "maxTime".to_string(),
                    serde_json::Value::String(format_epoch_millis(max_ts)),
                );
            }
        }

        Ok(vec![TimeBoundaryResult {
            timestamp: format_epoch_millis(min_ts),
            result,
        }])
    }
}
