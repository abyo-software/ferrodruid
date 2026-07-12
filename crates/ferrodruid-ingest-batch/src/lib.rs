// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Native batch ingestion for FerroDruid.
//!
//! The [`BatchIngester`] takes JSON rows (inline or from files), parses
//! timestamps and dimensions, builds dictionary-encoded string columns with
//! bitmap indexes (plus typed numeric dimension columns for
//! `dimensionsSpec.dimensions` object entries), and produces a
//! [`SegmentData`] ready for deep-storage persistence.
//!
//! # Null handling (Druid default / SQL-compatible mode)
//!
//! Null or absent input values are preserved as SQL NULL, matching Druid's
//! default null handling:
//!
//! * numeric columns (typed dimensions and rollup=false metrics) store NULL
//!   via the in-band NaN marker
//!   ([`ferrodruid_segment::column::NULL_DOUBLE`]);
//! * string dimensions store NULL distinct from `""` via the trailing
//!   null-row bitmap (see
//!   [`ferrodruid_segment::column::StringColumnData::null_rows`]);
//! * `count`-type metrics count raw input rows (1 per stored row at
//!   rollup=false; the merged group count under rollup).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod avro_format;
pub mod parquet_format;

use std::collections::{BTreeMap, HashMap};
use std::io::BufRead;
use std::path::Path;

use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_segment::column::{ColumnData, NULL_DOUBLE, NULL_FLOAT, StringColumnData};
use ferrodruid_segment::segment::{Interval, SegmentData};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// TransformSpec
// ---------------------------------------------------------------------------

/// Transform spec for ingestion (Druid-compatible).
///
/// Allows computing derived columns and filtering rows during ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransformSpec {
    /// Transforms to apply to each row, computing new column values.
    #[serde(default)]
    pub transforms: Vec<Transform>,
    /// Optional filter to exclude rows during ingestion.
    #[serde(default)]
    pub filter: Option<serde_json::Value>,
}

/// A single column transform that computes a new column from an expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transform {
    /// Output column name.
    pub name: String,
    /// Expression to compute the column value (simple field reference for now).
    pub expression: String,
}

// ---------------------------------------------------------------------------
// InputFormat
// ---------------------------------------------------------------------------

/// Rollup key: (truncated timestamp, dimension values).
///
/// A dimension value is `None` for SQL NULL — kept distinct from
/// `Some("")` so a null group never merges with a real empty-string
/// group (codex-review r2, 2026-07-11; Druid keeps them distinct).
type RollupKey = (i64, Vec<Option<String>>);

/// Rollup accumulator: (row count, metric sums).
type RollupAccum = (usize, HashMap<String, f64>);

/// Input format for batch ingestion data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum InputFormat {
    /// JSON format (one object per row).
    #[serde(rename = "json")]
    Json,
    /// CSV format with explicit column names.
    #[serde(rename = "csv")]
    Csv {
        /// Column names in order.
        columns: Vec<String>,
        /// Field delimiter (default: comma).
        #[serde(default = "default_delimiter")]
        delimiter: String,
        /// Number of header rows to skip.
        #[serde(default)]
        skip_header_rows: usize,
    },
    /// TSV format with explicit column names.
    #[serde(rename = "tsv")]
    Tsv {
        /// Column names in order.
        columns: Vec<String>,
    },
    /// Parquet format (Druid wire name `parquet`).
    ///
    /// Top-level columns are read into rows. `flattenSpec` JSON-path
    /// extraction of nested columns is not supported; nested columns are
    /// skipped. See [`crate::parquet_format`].
    #[serde(rename = "parquet")]
    Parquet {
        /// Optional flatten spec (accepted for wire compatibility but not
        /// applied; nested extraction is unsupported).
        #[serde(default, rename = "flattenSpec")]
        flatten_spec: Option<serde_json::Value>,
    },
    /// Avro Object Container File (Druid wire name `avro_ocf`).
    ///
    /// The schema is embedded in the file header. See [`crate::avro_format`].
    #[serde(rename = "avro_ocf")]
    AvroOcf {
        /// Optional flatten spec (accepted for wire compatibility but not
        /// applied).
        #[serde(default, rename = "flattenSpec")]
        flatten_spec: Option<serde_json::Value>,
    },
    /// Avro stream format (Druid wire name `avro_stream`).
    ///
    /// Bare Avro datums that depend on an externally-supplied reader schema.
    /// Recognised for wire compatibility but not decoded by this crate (no
    /// embedded schema available); see crate limitations.
    #[serde(rename = "avro_stream")]
    AvroStream {
        /// Optional flatten spec (accepted for wire compatibility but not
        /// applied).
        #[serde(default, rename = "flattenSpec")]
        flatten_spec: Option<serde_json::Value>,
    },
}

impl InputFormat {
    /// Parse a raw byte buffer into ingestion rows according to this format.
    ///
    /// JSON is parsed as newline-delimited objects; CSV / TSV use their
    /// configured columns; Parquet and Avro OCF decode self-describing binary
    /// buffers. `avro_stream` returns an error because it requires an external
    /// reader schema not carried in the buffer.
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Ingestion`] if the buffer cannot be parsed under
    /// this format, or if the format is not byte-buffer decodable.
    pub fn parse_bytes(&self, data: &[u8]) -> Result<Vec<serde_json::Value>> {
        match self {
            InputFormat::Json => parse_json_lines(data),
            InputFormat::Csv {
                columns, delimiter, ..
            } => {
                let text = std::str::from_utf8(data).map_err(|e| {
                    DruidError::Ingestion(format!("CSV buffer is not valid UTF-8: {e}"))
                })?;
                let delim = delimiter.chars().next().unwrap_or(',');
                parse_delimited(text, columns, delim)
            }
            InputFormat::Tsv { columns } => {
                let text = std::str::from_utf8(data).map_err(|e| {
                    DruidError::Ingestion(format!("TSV buffer is not valid UTF-8: {e}"))
                })?;
                parse_delimited(text, columns, '\t')
            }
            InputFormat::Parquet { .. } => parquet_format::parse_parquet_bytes(data.to_vec()),
            InputFormat::AvroOcf { .. } => avro_format::parse_avro_ocf_bytes(data),
            InputFormat::AvroStream { .. } => Err(DruidError::Ingestion(
                "avro_stream requires an external reader schema and is not decodable from a \
                 self-contained buffer"
                    .to_string(),
            )),
        }
    }
}

/// Parse a newline-delimited JSON buffer into rows.
fn parse_json_lines(data: &[u8]) -> Result<Vec<serde_json::Value>> {
    let text = std::str::from_utf8(data)
        .map_err(|e| DruidError::Ingestion(format!("JSON buffer is not valid UTF-8: {e}")))?;
    let mut rows = Vec::new();
    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
            DruidError::Ingestion(format!("JSON parse error at line {}: {e}", line_num + 1))
        })?;
        rows.push(value);
    }
    Ok(rows)
}

/// Parse a delimited (CSV / TSV) text buffer into JSON object rows.
fn parse_delimited(
    data: &str,
    columns: &[String],
    delimiter: char,
) -> Result<Vec<serde_json::Value>> {
    let mut rows = Vec::new();
    for (line_num, line) in data.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let fields: Vec<&str> = trimmed.split(delimiter).collect();
        if fields.len() != columns.len() {
            return Err(DruidError::Ingestion(format!(
                "delimited line {} has {} fields, expected {}",
                line_num + 1,
                fields.len(),
                columns.len()
            )));
        }
        let mut obj = serde_json::Map::new();
        for (col, val) in columns.iter().zip(fields.iter()) {
            if let Ok(n) = val.parse::<i64>() {
                obj.insert(
                    col.clone(),
                    serde_json::Value::Number(serde_json::Number::from(n)),
                );
            } else if let Ok(n) = val.parse::<f64>() {
                match serde_json::Number::from_f64(n) {
                    Some(num) => {
                        obj.insert(col.clone(), serde_json::Value::Number(num));
                    }
                    None => {
                        obj.insert(col.clone(), serde_json::Value::String(val.to_string()));
                    }
                }
            } else {
                obj.insert(col.clone(), serde_json::Value::String(val.to_string()));
            }
        }
        rows.push(serde_json::Value::Object(obj));
    }
    Ok(rows)
}

fn default_delimiter() -> String {
    ",".to_string()
}

// ---------------------------------------------------------------------------
// IngestedSegment
// ---------------------------------------------------------------------------

/// Result of a batch ingestion: a segment ready for persistence.
#[derive(Debug)]
pub struct IngestedSegment {
    /// Data source name.
    pub data_source: String,
    /// Time interval covered by this segment.
    pub interval: Interval,
    /// Number of rows ingested.
    pub num_rows: usize,
    /// The built segment data.
    pub segment_data: SegmentData,
    /// Segment version string (ISO-8601 timestamp).
    pub version: String,
}

// ---------------------------------------------------------------------------
// InputSource
// ---------------------------------------------------------------------------

/// Source of input data for batch ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum InputSource {
    /// Read from local filesystem (JSON lines files).
    #[serde(rename = "local")]
    Local {
        /// Base directory containing input files.
        base_dir: String,
        /// Optional glob filter for file names.
        filter: Option<String>,
    },
    /// Inline JSON data embedded in the spec.
    #[serde(rename = "inline")]
    Inline {
        /// The rows of data.
        data: Vec<serde_json::Value>,
    },
}

// ---------------------------------------------------------------------------
// DimensionSchema (typed dimensions)
// ---------------------------------------------------------------------------

/// Value type of a dimension column (Druid `dimensionsSpec.dimensions`
/// object-entry `type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DimensionType {
    /// Dictionary-encoded string dimension (Druid default).
    String,
    /// 64-bit integer dimension.
    ///
    /// `i64` has no in-band NULL marker, so a batch whose long-typed column
    /// contains nulls is stored as `DOUBLE` with NaN nulls (null-faithful,
    /// but integral values render as `10.0` and the column reports `DOUBLE`
    /// in metadata).  Null-free batches produce a true `LONG` column.
    Long,
    /// 32-bit floating-point dimension (NULL stored as NaN).
    Float,
    /// 64-bit floating-point dimension (NULL stored as NaN).
    Double,
}

/// A single dimension schema entry: column name plus value type.
#[derive(Debug, Clone)]
pub struct DimensionSchema {
    /// Output column name.
    pub name: String,
    /// Value type of the column.
    pub dim_type: DimensionType,
}

impl DimensionSchema {
    /// A typed dimension schema entry.
    pub fn new(name: impl Into<String>, dim_type: DimensionType) -> Self {
        Self {
            name: name.into(),
            dim_type,
        }
    }

    /// A plain string dimension (the Druid default for bare-name entries).
    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, DimensionType::String)
    }
}

/// Parse Druid `dimensionsSpec.dimensions` entries into typed schemas.
///
/// Plain-string entries (`"site"`) are string dimensions.  Object entries
/// (`{"type": "double", "name": "value"}`) are typed; `type` defaults to
/// `"string"` when absent.  Supported types: `string`, `long`, `float`,
/// `double`.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] for an object entry without a `name`,
/// an unsupported `type`, or a non-string / non-object entry.
pub fn parse_dimension_entries(entries: &[serde_json::Value]) -> Result<Vec<DimensionSchema>> {
    let mut schemas = Vec::with_capacity(entries.len());
    for entry in entries {
        match entry {
            serde_json::Value::String(name) => schemas.push(DimensionSchema::string(name.clone())),
            serde_json::Value::Object(map) => {
                let name = map
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DruidError::Ingestion(format!(
                            "dimensionsSpec entry is missing `name`: {entry}"
                        ))
                    })?
                    .to_string();
                let type_str = map.get("type").and_then(|v| v.as_str()).unwrap_or("string");
                let dim_type = match type_str {
                    "string" => DimensionType::String,
                    "long" => DimensionType::Long,
                    "float" => DimensionType::Float,
                    "double" => DimensionType::Double,
                    other => {
                        return Err(DruidError::Ingestion(format!(
                            "unsupported dimension type `{other}` for dimension `{name}` \
                             (supported: string, long, float, double)"
                        )));
                    }
                };
                schemas.push(DimensionSchema::new(name, dim_type));
            }
            other => {
                return Err(DruidError::Ingestion(format!(
                    "dimensionsSpec entry must be a string or an object, got: {other}"
                )));
            }
        }
    }
    Ok(schemas)
}

// ---------------------------------------------------------------------------
// JSON value coercion helpers
// ---------------------------------------------------------------------------

/// Coerce a JSON value to `f64` per Druid numeric-column ingestion rules:
/// numbers pass through, numeric strings are parsed, everything else
/// (including JSON null) is NULL (`None`).
fn json_to_f64(v: &serde_json::Value) -> Option<f64> {
    // Non-finite results are NULL (codex r13): Rust's f64 parser accepts
    // "Infinity"/"-Infinity"/"NaN" strings, and a stored non-finite value
    // renders as JSON null on the wire while grouping as a phantom second
    // "null" key. NaN is the in-band NULL marker, so nothing non-finite may
    // pass this choke point.
    let parsed = match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    };
    parsed.filter(|x| x.is_finite())
}

/// Coerce a JSON value to `i64` per Druid long-column ingestion rules:
/// integers pass through, floats and numeric strings truncate toward zero,
/// everything else (including JSON null) is NULL (`None`).
fn json_to_i64(v: &serde_json::Value) -> Option<i64> {
    #[allow(clippy::cast_possible_truncation)]
    fn f64_to_i64(f: f64) -> Option<i64> {
        // Reject values outside the exactly-representable i64 range instead
        // of relying on Rust's saturating float→int cast — an out-of-range
        // input must become NULL, never a silently clamped extreme value.
        // (2^63 as f64 is exact; anything >= it, or < -2^63, is out of range.)
        const I64_RANGE_HI: f64 = 9_223_372_036_854_775_808.0; // 2^63
        (f.is_finite() && (-I64_RANGE_HI..I64_RANGE_HI).contains(&f.trunc()))
            .then(|| f.trunc() as i64)
    }
    match v {
        // `as_i64` covers every in-range value, including u64s <= i64::MAX;
        // a u64 above i64::MAX must become NULL, not wrap (codex-review High,
        // 2026-07-11: `u as i64` stored 9223372036854775808 as a negative
        // long — silent corruption from untrusted ingest input).
        serde_json::Value::Number(n) => n.as_i64().or_else(|| n.as_f64().and_then(f64_to_i64)),
        serde_json::Value::String(s) => {
            let t = s.trim();
            t.parse::<i64>()
                .ok()
                .or_else(|| t.parse::<f64>().ok().and_then(f64_to_i64))
        }
        _ => None,
    }
}

/// Whether a metric aggregator spec is Druid's `count` type (no `fieldName`;
/// counts raw input rows rather than reading a column).
fn is_count_metric_spec(spec: &serde_json::Value) -> bool {
    spec.get("type").and_then(|v| v.as_str()) == Some("count")
}

// ---------------------------------------------------------------------------
// BatchIngester
// ---------------------------------------------------------------------------

/// Processes JSON rows into a Druid segment.
pub struct BatchIngester {
    data_source: String,
    timestamp_column: String,
    dim_schemas: Vec<DimensionSchema>,
    metrics_specs: Vec<serde_json::Value>,
}

impl BatchIngester {
    /// Create a new batch ingester with plain string dimensions.
    ///
    /// * `data_source` — target data source name
    /// * `timestamp_column` — column to extract the primary timestamp from
    ///   (falls back to `__time` if the column is not found)
    /// * `dimensions` — dimension column names to extract (all ingested as
    ///   string dimensions; use [`BatchIngester::with_schemas`] for typed
    ///   dimensions)
    /// * `metrics_specs` — metric aggregator specs (used to identify metric
    ///   column names; each spec should have a `"name"` field)
    pub fn new(
        data_source: String,
        timestamp_column: String,
        dimensions: Vec<String>,
        metrics_specs: Vec<serde_json::Value>,
    ) -> Self {
        let dim_schemas = dimensions
            .into_iter()
            .map(DimensionSchema::string)
            .collect();
        Self::with_schemas(data_source, timestamp_column, dim_schemas, metrics_specs)
    }

    /// Create a new batch ingester with typed dimension schemas
    /// (`dimensionsSpec.dimensions` object entries — see
    /// [`parse_dimension_entries`]).
    pub fn with_schemas(
        data_source: String,
        timestamp_column: String,
        dim_schemas: Vec<DimensionSchema>,
        metrics_specs: Vec<serde_json::Value>,
    ) -> Self {
        Self {
            data_source,
            timestamp_column,
            dim_schemas,
            metrics_specs,
        }
    }

    /// Ingest a batch of JSON rows and produce segment data.
    ///
    /// Null and absent values are preserved as SQL NULL (see the crate-level
    /// null-handling notes).  `count`-type metrics store 1 per raw row.
    pub fn ingest(&self, rows: Vec<serde_json::Value>) -> Result<IngestedSegment> {
        self.ingest_inner(rows, /* count_metrics_from_field */ false)
    }

    /// Shared ingestion core.
    ///
    /// `count_metrics_from_field` is `true` only for the internal
    /// rollup path, whose pre-aggregated rows carry the merged group count
    /// under each `count`-type metric's spec name.  For raw (rollup=false)
    /// rows, a `count` metric is always 1 per row — Druid's `count`
    /// aggregator has no `fieldName` and counts input rows, ignoring any
    /// same-named field in the data.
    fn ingest_inner(
        &self,
        rows: Vec<serde_json::Value>,
        count_metrics_from_field: bool,
    ) -> Result<IngestedSegment> {
        if rows.is_empty() {
            return Err(DruidError::Ingestion("no rows to ingest".to_string()));
        }

        // Step 1: Parse rows into (timestamp_millis, row)
        let mut parsed = Vec::with_capacity(rows.len());
        for row in &rows {
            let ts = self.extract_timestamp(row)?;
            parsed.push((ts, row));
        }

        // Step 2: Sort by timestamp
        parsed.sort_by_key(|(ts, _)| *ts);

        let num_rows = parsed.len();
        let min_ts = parsed[0].0;
        let max_ts = parsed[num_rows - 1].0;

        // Step 3: Build __time column
        let time_values: Vec<i64> = parsed.iter().map(|(ts, _)| *ts).collect();

        // Step 4: Build dimension columns per schema (string dimensions are
        // dictionary-encoded; long/float/double dimensions are numeric).
        let mut columns: HashMap<String, ColumnData> = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(time_values));

        for schema in &self.dim_schemas {
            let col = match schema.dim_type {
                DimensionType::String => self.build_string_column(&schema.name, &parsed)?,
                DimensionType::Double => Self::build_double_column(&schema.name, &parsed),
                DimensionType::Float => Self::build_float_column(&schema.name, &parsed),
                DimensionType::Long => Self::build_long_column(&schema.name, &parsed)?,
            };
            columns.insert(schema.name.clone(), col);
        }

        // Step 5: Build metric columns.  `count`-type metrics count raw rows
        // (LONG); every other aggregator reads its named field as DOUBLE
        // with nulls preserved as NaN.
        let metric_names = self.metric_names();
        for spec in &self.metrics_specs {
            let Some(name) = spec.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            let col = if is_count_metric_spec(spec) {
                let values: Vec<i64> = if count_metrics_from_field {
                    parsed
                        .iter()
                        .map(|(_, row)| row.get(name).and_then(json_to_i64).unwrap_or(1))
                        .collect()
                } else {
                    vec![1_i64; num_rows]
                };
                ColumnData::Long(values)
            } else {
                // codex-review r11: read the SOURCE field (`fieldName`), not
                // the output `name` — a renamed metric
                // ({"name":"sum_value","fieldName":"value"}) previously read
                // the nonexistent output field and stored NULL on every row.
                let field = spec
                    .get("fieldName")
                    .and_then(|v| v.as_str())
                    .unwrap_or(name);
                self.build_metric_column(field, &parsed)
            };
            columns.insert(name.to_string(), col);
        }

        let interval = Interval {
            start_millis: min_ts,
            end_millis: max_ts,
        };

        let version = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

        // Rows were sorted by timestamp above, so __time is ascending; cache it
        // so query-time interval pruning can binary-search without an O(n) scan.
        let time_sorted = matches!(
            columns.get("__time"),
            Some(ferrodruid_segment::column::ColumnData::Long(v)) if v.is_sorted()
        );
        let segment_data = SegmentData {
            version: 9,
            num_rows,
            interval,
            dimensions: self.dimension_names(),
            metrics: metric_names,
            columns,
            time_sorted,
        };

        Ok(IngestedSegment {
            data_source: self.data_source.clone(),
            interval,
            num_rows,
            segment_data,
            version,
        })
    }

    /// Ingest with rollup (pre-aggregation).
    ///
    /// Groups rows by truncated timestamp (at the given granularity) and all
    /// dimension values, then aggregates metrics: `count`-type metrics store
    /// the number of merged raw rows (under their spec name); every other
    /// metric is summed over its **non-null** inputs (a group whose inputs
    /// are all null stores NULL, matching Druid's default null handling).
    ///
    /// A NULL (or absent) dimension value forms its own rollup group,
    /// distinct from a real `""` value, and is stored as SQL NULL in the
    /// rolled segment — matching Druid (codex-review r2 fix, 2026-07-11).
    /// Non-string dimension values are still coerced to their string form
    /// for grouping purposes.
    ///
    /// `rollup_granularity` is one of `"second"`, `"minute"`, `"hour"`, `"day"`,
    /// `"month"`, `"year"`.
    pub fn ingest_with_rollup(
        &self,
        rows: Vec<serde_json::Value>,
        rollup_granularity: &str,
    ) -> Result<IngestedSegment> {
        if rows.is_empty() {
            return Err(DruidError::Ingestion("no rows to ingest".to_string()));
        }

        // Parse all rows into (timestamp_millis, row)
        let mut parsed: Vec<(i64, &serde_json::Value)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let ts = self.extract_timestamp(row)?;
            parsed.push((ts, row));
        }

        // Split metric specs: `count` metrics take the merged group count;
        // all others are summed over non-null inputs.
        let count_names: Vec<String> = self
            .metrics_specs
            .iter()
            .filter(|s| is_count_metric_spec(s))
            .filter_map(|s| s.get("name").and_then(|v| v.as_str()).map(String::from))
            .collect();
        // (output name, source field) pairs — a renamed metric reads its
        // `fieldName` from raw rows and stores under `name` (codex r11).
        let sum_metrics: Vec<(String, String)> = self
            .metrics_specs
            .iter()
            .filter(|s| !is_count_metric_spec(s))
            .filter_map(|s| {
                let name = s.get("name").and_then(|v| v.as_str())?;
                let field = s.get("fieldName").and_then(|v| v.as_str()).unwrap_or(name);
                Some((name.to_string(), field.to_string()))
            })
            .collect();

        // Build rollup key: (truncated_ts, dim1, dim2, ...)
        let mut groups: BTreeMap<RollupKey, RollupAccum> = BTreeMap::new();

        for (ts, row) in &parsed {
            let truncated = truncate_timestamp(*ts, rollup_granularity);
            // NULL (or absent) dimension values key as `None` — a distinct
            // group from a real `""` value, matching Druid. Numeric-typed
            // dimensions key by their canonical NUMERIC value, not the JSON
            // text (codex-review r7: `1` vs `1.0` are the same double and
            // must land in one rolled group).
            let dim_key: Vec<Option<String>> = self
                .dim_schemas
                .iter()
                .map(|schema| {
                    let v = row.get(&schema.name)?;
                    match schema.dim_type {
                        DimensionType::String => match v {
                            serde_json::Value::String(s) => Some(s.clone()),
                            serde_json::Value::Null => None,
                            other => Some(other.to_string()),
                        },
                        DimensionType::Long => json_to_i64(v).map(|x| x.to_string()),
                        DimensionType::Double => {
                            // Canonical f64 formatting: 1, 1.0 and "1.0" all
                            // key as "1"; -0.0 normalises to 0.0 (numerically
                            // equal, must share a group — codex r9); NaN
                            // cannot arise from JSON input.
                            json_to_f64(v).map(|x| {
                                let x = if x == 0.0 { 0.0 } else { x };
                                x.to_string()
                            })
                        }
                        DimensionType::Float => {
                            // Key at STORAGE precision (f32): values that
                            // collapse to the same stored f32 must share a
                            // rolled group (codex r9: 16777216.0 vs
                            // 16777217.0 store identically). -0.0 -> 0.0.
                            // An f32-overflowing value stores NULL, so it
                            // keys as NULL too (codex r13).
                            json_to_f64(v)
                                .map(|x| {
                                    #[allow(clippy::cast_possible_truncation)]
                                    let x = x as f32;
                                    x
                                })
                                .filter(|x| x.is_finite())
                                .map(|x| {
                                    let x = if x == 0.0 { 0.0 } else { x };
                                    x.to_string()
                                })
                        }
                    }
                })
                .collect();

            let entry = groups
                .entry((truncated, dim_key))
                .or_insert_with(|| (0, HashMap::new()));
            entry.0 += 1; // count
            for (m, field) in &sum_metrics {
                // Null-skipping sum: only non-null (coercible) inputs
                // contribute; a group with no non-null input never gets an
                // entry, so the rolled row omits the key and the stored
                // value is NULL. Values are read from the SOURCE field and
                // accumulated under the OUTPUT name (codex r11).
                if let Some(val) = row.get(field).and_then(json_to_f64) {
                    *entry.1.entry(m.clone()).or_insert(0.0) += val;
                }
            }
        }

        // Convert groups back to JSON rows
        let mut rolled_rows: Vec<serde_json::Value> = Vec::with_capacity(groups.len());
        for ((ts, dim_vals), (count, metric_sums)) in &groups {
            let mut obj = serde_json::Map::new();
            obj.insert(
                self.timestamp_column.clone(),
                serde_json::Value::Number(serde_json::Number::from(*ts)),
            );
            for (i, schema) in self.dim_schemas.iter().enumerate() {
                // A `None` key re-emits as JSON null so the re-ingest stores
                // it as a real NULL (null-row bitmap), not the "" entry.
                let v = match &dim_vals[i] {
                    Some(s) => serde_json::Value::String(s.clone()),
                    None => serde_json::Value::Null,
                };
                obj.insert(schema.name.clone(), v);
            }
            // Merged raw-row count for each `count`-type metric, under its
            // spec name (pre-fix this was hardcoded to `"count"` and then
            // 0-filled by the metric-column builder — Defect D).
            for cname in &count_names {
                obj.insert(
                    cname.clone(),
                    serde_json::Value::Number(serde_json::Number::from(*count)),
                );
            }
            for (m, val) in metric_sums {
                if let Some(n) = serde_json::Number::from_f64(*val) {
                    // Emit under the SOURCE fieldName: the re-ingest reads
                    // each metric from its `fieldName` (codex r11), so the
                    // rolled value must live there, not under the output
                    // name. Same-named metrics are unaffected.
                    let field = sum_metrics
                        .iter()
                        .find(|(name, _)| name == m)
                        .map_or(m.as_str(), |(_, field)| field.as_str());
                    obj.insert(field.to_string(), serde_json::Value::Number(n));
                }
            }
            rolled_rows.push(serde_json::Value::Object(obj));
        }

        self.ingest_inner(rolled_rows, /* count_metrics_from_field */ true)
    }

    /// Ingest from CSV data.
    ///
    /// Parses the CSV string using the given column names and delimiter,
    /// converting each row into a JSON object before ingesting.
    pub fn ingest_csv(
        &self,
        data: &str,
        columns: &[String],
        delimiter: char,
    ) -> Result<IngestedSegment> {
        let mut rows = Vec::new();
        for (line_num, line) in data.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let fields: Vec<&str> = trimmed.split(delimiter).collect();
            if fields.len() != columns.len() {
                return Err(DruidError::Ingestion(format!(
                    "CSV line {} has {} fields, expected {}",
                    line_num + 1,
                    fields.len(),
                    columns.len()
                )));
            }
            let mut obj = serde_json::Map::new();
            for (col, val) in columns.iter().zip(fields.iter()) {
                // Try to parse as number first, then fall back to string
                if let Ok(n) = val.parse::<i64>() {
                    obj.insert(
                        col.clone(),
                        serde_json::Value::Number(serde_json::Number::from(n)),
                    );
                } else if let Ok(n) = val.parse::<f64>() {
                    if let Some(num) = serde_json::Number::from_f64(n) {
                        obj.insert(col.clone(), serde_json::Value::Number(num));
                    } else {
                        obj.insert(col.clone(), serde_json::Value::String(val.to_string()));
                    }
                } else {
                    obj.insert(col.clone(), serde_json::Value::String(val.to_string()));
                }
            }
            rows.push(serde_json::Value::Object(obj));
        }

        if rows.is_empty() {
            return Err(DruidError::Ingestion("no rows in CSV data".to_string()));
        }

        self.ingest(rows)
    }

    /// Ingest from a JSON-lines file (one JSON object per line).
    pub fn ingest_file(&self, path: &Path) -> Result<IngestedSegment> {
        let file = std::fs::File::open(path).map_err(|e| {
            DruidError::Ingestion(format!("failed to open {}: {e}", path.display()))
        })?;
        let reader = std::io::BufReader::new(file);
        let mut rows = Vec::new();

        for (line_num, line) in reader.lines().enumerate() {
            let line = line.map_err(|e| {
                DruidError::Ingestion(format!("read error at line {}: {e}", line_num + 1))
            })?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
                DruidError::Ingestion(format!("JSON parse error at line {}: {e}", line_num + 1))
            })?;
            rows.push(value);
        }

        self.ingest(rows)
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Extract epoch milliseconds from a row.
    fn extract_timestamp(&self, row: &serde_json::Value) -> Result<i64> {
        // Try the configured timestamp column first, then __time
        let ts_val = row
            .get(&self.timestamp_column)
            .or_else(|| row.get("__time"));

        match ts_val {
            Some(serde_json::Value::Number(n)) => n
                .as_i64()
                .ok_or_else(|| DruidError::Ingestion(format!("timestamp is not an integer: {n}"))),
            Some(serde_json::Value::String(s)) => {
                // Try ISO-8601 parse
                let dt = s.parse::<DateTime<Utc>>().map_err(|e| {
                    DruidError::Ingestion(format!("failed to parse timestamp '{s}': {e}"))
                })?;
                Ok(dt.timestamp_millis())
            }
            Some(other) => Err(DruidError::Ingestion(format!(
                "unsupported timestamp type: {other}"
            ))),
            None => Err(DruidError::Ingestion(format!(
                "timestamp column '{}' not found in row",
                self.timestamp_column
            ))),
        }
    }

    /// Build a dictionary-encoded string column with bitmap indexes.
    ///
    /// Null and absent inputs are preserved as SQL NULL — distinct from
    /// `""` — via the trailing null-row bitmap (see
    /// [`StringColumnData::null_rows`]).
    fn build_string_column(
        &self,
        dim_name: &str,
        rows: &[(i64, &serde_json::Value)],
    ) -> Result<ColumnData> {
        let values: Vec<Option<String>> = rows
            .iter()
            .map(|(_, row)| match row.get(dim_name) {
                Some(serde_json::Value::String(s)) => Some(s.clone()),
                Some(serde_json::Value::Number(n)) => Some(n.to_string()),
                Some(serde_json::Value::Bool(b)) => Some(b.to_string()),
                Some(serde_json::Value::Null) | None => None,
                Some(other) => Some(other.to_string()),
            })
            .collect();

        Ok(ColumnData::String(StringColumnData::from_nullable_values(
            &values,
        )))
    }

    /// Build a `DOUBLE` column for a typed dimension; nulls stored as NaN.
    fn build_double_column(name: &str, rows: &[(i64, &serde_json::Value)]) -> ColumnData {
        let values: Vec<f64> = rows
            .iter()
            .map(|(_, row)| row.get(name).and_then(json_to_f64).unwrap_or(NULL_DOUBLE))
            .collect();
        ColumnData::Double(values)
    }

    /// Build a `FLOAT` column for a typed dimension; nulls stored as NaN.
    fn build_float_column(name: &str, rows: &[(i64, &serde_json::Value)]) -> ColumnData {
        #[allow(clippy::cast_possible_truncation)]
        let values: Vec<f32> = rows
            .iter()
            .map(|(_, row)| {
                row.get(name)
                    .and_then(json_to_f64)
                    // A finite f64 above f32::MAX casts to inf — store NULL
                    // instead of a non-finite value (codex r13).
                    .map(|v| v as f32)
                    .filter(|v| v.is_finite())
                    .map_or(NULL_FLOAT, |v| v)
            })
            .collect();
        ColumnData::Float(values)
    }

    /// Build a `LONG` column for a typed dimension.
    ///
    /// `i64` has no in-band NULL marker, so if any input is null the column
    /// falls back to `DOUBLE` with NaN nulls (null-faithful; see
    /// [`DimensionType::Long`]).  Null-free inputs produce a true `LONG`
    /// column.
    fn build_long_column(name: &str, rows: &[(i64, &serde_json::Value)]) -> Result<ColumnData> {
        /// Largest magnitude exactly representable in f64 (2^53).
        const F64_EXACT_MAX: i64 = 9_007_199_254_740_992;
        let values: Vec<Option<i64>> = rows
            .iter()
            .map(|(_, row)| row.get(name).and_then(json_to_i64))
            .collect();
        if values.iter().all(Option::is_some) {
            return Ok(ColumnData::Long(values.into_iter().flatten().collect()));
        }
        // codex-review r6 (2026-07-11): the DOUBLE fallback is exact only
        // within +/-2^53. A batch that combines NULLs with values beyond
        // that range must FAIL CLOSED — silently rounding e.g.
        // 9007199254740993 to ...992 corrupts high-range IDs and can merge
        // distinct adjacent values.
        if let Some(bad) = values
            .iter()
            .flatten()
            .find(|v| v.unsigned_abs() > F64_EXACT_MAX as u64)
        {
            return Err(DruidError::Ingestion(format!(
                "long dimension `{name}` mixes NULLs with value {bad}, which exceeds the \
                 f64-exact range (+/-2^53 = +/-{F64_EXACT_MAX}); the null-bearing long \
                 fallback would silently lose precision. Make the column non-null, or \
                 ingest it as a string dimension"
            )));
        }
        #[allow(clippy::cast_precision_loss)]
        let doubles: Vec<f64> = values
            .into_iter()
            .map(|v| v.map_or(NULL_DOUBLE, |x| x as f64))
            .collect();
        Ok(ColumnData::Double(doubles))
    }

    /// Build a numeric (double) column for a non-`count` metric; nulls,
    /// absent fields, and non-coercible values are stored as NULL (NaN) —
    /// never 0-filled.
    fn build_metric_column(
        &self,
        metric_name: &str,
        rows: &[(i64, &serde_json::Value)],
    ) -> ColumnData {
        let values: Vec<f64> = rows
            .iter()
            .map(|(_, row)| {
                row.get(metric_name)
                    .and_then(json_to_f64)
                    .unwrap_or(NULL_DOUBLE)
            })
            .collect();
        ColumnData::Double(values)
    }

    /// Extract metric column names from the metric specs.
    fn metric_names(&self) -> Vec<String> {
        self.metrics_specs
            .iter()
            .filter_map(|spec| spec.get("name").and_then(|v| v.as_str()).map(String::from))
            .collect()
    }

    /// Dimension column names in schema order.
    fn dimension_names(&self) -> Vec<String> {
        self.dim_schemas.iter().map(|s| s.name.clone()).collect()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Truncate epoch millis to the floor of the given granularity.
fn truncate_timestamp(ts_millis: i64, granularity: &str) -> i64 {
    let dt = DateTime::from_timestamp_millis(ts_millis).unwrap_or_default();
    let naive = dt.naive_utc();

    let truncated = match granularity {
        "second" => NaiveDateTime::new(
            naive.date(),
            chrono::NaiveTime::from_hms_opt(naive.hour(), naive.minute(), naive.second())
                .unwrap_or_default(),
        ),
        "minute" => NaiveDateTime::new(
            naive.date(),
            chrono::NaiveTime::from_hms_opt(naive.hour(), naive.minute(), 0).unwrap_or_default(),
        ),
        "hour" => NaiveDateTime::new(
            naive.date(),
            chrono::NaiveTime::from_hms_opt(naive.hour(), 0, 0).unwrap_or_default(),
        ),
        "day" => NaiveDateTime::new(
            naive.date(),
            chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap_or_default(),
        ),
        "month" => NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(naive.year(), naive.month(), 1).unwrap_or_default(),
            chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap_or_default(),
        ),
        "year" => NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(naive.year(), 1, 1).unwrap_or_default(),
            chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap_or_default(),
        ),
        // Unknown granularity: no truncation
        _ => naive,
    };

    DateTime::<Utc>::from_naive_utc_and_offset(truncated, Utc).timestamp_millis()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_rows() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({
                "__time": "2024-01-01T00:00:03Z",
                "region": "us",
                "count": 10
            }),
            serde_json::json!({
                "__time": "2024-01-01T00:00:01Z",
                "region": "eu",
                "count": 5
            }),
            serde_json::json!({
                "__time": "2024-01-01T00:00:02Z",
                "region": "us",
                "count": 3
            }),
        ]
    }

    #[test]
    fn ingest_basic() {
        let ingester = BatchIngester::new(
            "test_ds".into(),
            "__time".into(),
            vec!["region".into()],
            vec![serde_json::json!({"type": "doubleSum", "name": "count"})],
        );

        let result = ingester.ingest(sample_rows()).expect("ingest");
        assert_eq!(result.data_source, "test_ds");
        assert_eq!(result.num_rows, 3);

        let seg = &result.segment_data;
        assert_eq!(seg.num_rows, 3);
        assert_eq!(seg.dimensions, vec!["region"]);
        assert_eq!(seg.metrics, vec!["count"]);

        // Timestamps should be sorted
        let ts = seg.timestamp_column().expect("__time");
        assert!(ts[0] <= ts[1]);
        assert!(ts[1] <= ts[2]);
    }

    #[test]
    fn ingest_sorts_by_timestamp() {
        let ingester =
            BatchIngester::new("ds".into(), "__time".into(), vec!["region".into()], vec![]);

        let result = ingester.ingest(sample_rows()).expect("ingest");
        let ts = result.segment_data.timestamp_column().expect("ts");
        // The original order is 03, 01, 02 — sorted should be 01, 02, 03
        assert!(ts.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn ingest_builds_dictionary_and_bitmaps() {
        let ingester =
            BatchIngester::new("ds".into(), "__time".into(), vec!["region".into()], vec![]);

        let result = ingester.ingest(sample_rows()).expect("ingest");
        let col = result.segment_data.column("region").expect("region column");
        match col {
            ColumnData::String(sc) => {
                // Dictionary should have 2 entries: "eu", "us" (sorted)
                assert_eq!(sc.dictionary.len(), 2);
                assert_eq!(sc.dictionary.get(0), Some("eu"));
                assert_eq!(sc.dictionary.get(1), Some("us"));

                // 3 rows
                assert_eq!(sc.encoded_values.len(), 3);

                // 2 bitmaps
                assert_eq!(sc.bitmap_indexes.len(), 2);
            }
            other => panic!("expected String column, got {other:?}"),
        }
    }

    #[test]
    fn ingest_metric_column() {
        let ingester = BatchIngester::new(
            "ds".into(),
            "__time".into(),
            vec![],
            vec![serde_json::json!({"type": "doubleSum", "name": "count"})],
        );

        let result = ingester.ingest(sample_rows()).expect("ingest");
        let col = result.segment_data.column("count").expect("count");
        match col {
            ColumnData::Double(vals) => {
                assert_eq!(vals.len(), 3);
                // Sorted by time: eu(5), us(3), us(10)
                assert!((vals[0] - 5.0).abs() < f64::EPSILON);
                assert!((vals[1] - 3.0).abs() < f64::EPSILON);
                assert!((vals[2] - 10.0).abs() < f64::EPSILON);
            }
            other => panic!("expected Double column, got {other:?}"),
        }
    }

    #[test]
    fn ingest_numeric_timestamps() {
        let rows = vec![
            serde_json::json!({"__time": 2000, "dim": "a"}),
            serde_json::json!({"__time": 1000, "dim": "b"}),
        ];
        let ingester = BatchIngester::new("ds".into(), "__time".into(), vec!["dim".into()], vec![]);
        let result = ingester.ingest(rows).expect("ingest");
        let ts = result.segment_data.timestamp_column().expect("ts");
        assert_eq!(ts, &[1000_i64, 2000]);
    }

    #[test]
    fn ingest_custom_timestamp_column() {
        let rows = vec![
            serde_json::json!({"ts": "2024-01-01T00:00:02Z", "dim": "a"}),
            serde_json::json!({"ts": "2024-01-01T00:00:01Z", "dim": "b"}),
        ];
        let ingester = BatchIngester::new("ds".into(), "ts".into(), vec!["dim".into()], vec![]);
        let result = ingester.ingest(rows).expect("ingest");
        assert_eq!(result.num_rows, 2);
    }

    #[test]
    fn ingest_empty_rows_error() {
        let ingester = BatchIngester::new("ds".into(), "__time".into(), vec![], vec![]);
        let err = ingester.ingest(vec![]).unwrap_err();
        assert!(err.to_string().contains("no rows"));
    }

    #[test]
    fn ingest_missing_timestamp_error() {
        let rows = vec![serde_json::json!({"dim": "a"})];
        let ingester = BatchIngester::new("ds".into(), "__time".into(), vec![], vec![]);
        let err = ingester.ingest(rows).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn ingest_file_jsonl() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("data.jsonl");
        std::fs::write(
            &path,
            r#"{"__time": 1000, "city": "tokyo", "val": 1.0}
{"__time": 2000, "city": "osaka", "val": 2.0}
{"__time": 3000, "city": "tokyo", "val": 3.0}
"#,
        )
        .expect("write");

        let ingester = BatchIngester::new(
            "ds".into(),
            "__time".into(),
            vec!["city".into()],
            vec![serde_json::json!({"type": "doubleSum", "name": "val"})],
        );
        let result = ingester.ingest_file(&path).expect("ingest_file");
        assert_eq!(result.num_rows, 3);
        assert_eq!(result.segment_data.dimensions, vec!["city"]);
    }

    #[test]
    fn ingest_file_not_found() {
        let ingester = BatchIngester::new("ds".into(), "__time".into(), vec![], vec![]);
        let err = ingester
            .ingest_file(Path::new("/nonexistent/data.jsonl"))
            .unwrap_err();
        assert!(err.to_string().contains("failed to open"));
    }

    #[test]
    fn ingest_interval_covers_data() {
        let ingester = BatchIngester::new("ds".into(), "__time".into(), vec![], vec![]);
        let rows = vec![
            serde_json::json!({"__time": 3000}),
            serde_json::json!({"__time": 1000}),
            serde_json::json!({"__time": 2000}),
        ];
        let result = ingester.ingest(rows).expect("ingest");
        assert_eq!(result.interval.start_millis, 1000);
        assert_eq!(result.interval.end_millis, 3000);
    }

    // --- Rollup tests ---

    #[test]
    fn rollup_reduces_rows() {
        // 100 rows with duplicates across 2 regions and 2 days
        let mut rows = Vec::new();
        for i in 0..100 {
            let region = if i % 2 == 0 { "us" } else { "eu" };
            // Two days: first 50 on day 1, next 50 on day 2
            let day = if i < 50 {
                "2024-01-01T12:00:00Z"
            } else {
                "2024-01-02T12:00:00Z"
            };
            rows.push(serde_json::json!({
                "__time": day,
                "region": region,
                "revenue": 10.0
            }));
        }

        let ingester = BatchIngester::new(
            "ds".into(),
            "__time".into(),
            vec!["region".into()],
            vec![serde_json::json!({"type": "doubleSum", "name": "revenue"})],
        );

        let result = ingester
            .ingest_with_rollup(rows, "day")
            .expect("rollup ingest");

        // 2 days x 2 regions = 4 rolled-up rows (much fewer than 100)
        assert_eq!(result.num_rows, 4);
    }

    #[test]
    fn rollup_empty_rows_error() {
        let ingester = BatchIngester::new("ds".into(), "__time".into(), vec![], vec![]);
        let err = ingester.ingest_with_rollup(vec![], "day").unwrap_err();
        assert!(err.to_string().contains("no rows"));
    }

    // --- CSV tests ---

    #[test]
    fn csv_ingestion() {
        let csv_data = "1000,tokyo,1.5\n2000,osaka,2.5\n3000,tokyo,3.5\n";
        let columns = vec!["__time".into(), "city".into(), "val".into()];

        let ingester = BatchIngester::new(
            "ds".into(),
            "__time".into(),
            vec!["city".into()],
            vec![serde_json::json!({"type": "doubleSum", "name": "val"})],
        );

        let result = ingester
            .ingest_csv(csv_data, &columns, ',')
            .expect("csv ingest");
        assert_eq!(result.num_rows, 3);
        assert_eq!(result.segment_data.dimensions, vec!["city"]);
    }

    #[test]
    fn csv_field_count_mismatch() {
        let csv_data = "1000,tokyo\n";
        let columns = vec!["__time".into(), "city".into(), "val".into()];

        let ingester = BatchIngester::new("ds".into(), "__time".into(), vec![], vec![]);
        let err = ingester.ingest_csv(csv_data, &columns, ',').unwrap_err();
        assert!(err.to_string().contains("fields"));
    }

    // --- TransformSpec tests ---

    #[test]
    fn transform_spec_parse() {
        let json = r#"{
            "transforms": [
                {"name": "fullName", "expression": "concat(first, ' ', last)"}
            ],
            "filter": {"type": "selector", "dimension": "region", "value": "us"}
        }"#;
        let spec: TransformSpec = serde_json::from_str(json).expect("parse");
        assert_eq!(spec.transforms.len(), 1);
        assert_eq!(spec.transforms[0].name, "fullName");
        assert!(spec.filter.is_some());
    }

    #[test]
    fn transform_spec_empty() {
        let json = r#"{}"#;
        let spec: TransformSpec = serde_json::from_str(json).expect("parse");
        assert!(spec.transforms.is_empty());
        assert!(spec.filter.is_none());
    }

    // --- InputFormat tests ---

    #[test]
    fn input_format_json_roundtrip() {
        let format = InputFormat::Json;
        let json = serde_json::to_string(&format).expect("ser");
        let parsed: InputFormat = serde_json::from_str(&json).expect("deser");
        assert!(matches!(parsed, InputFormat::Json));
    }

    #[test]
    fn input_format_csv_roundtrip() {
        let format = InputFormat::Csv {
            columns: vec!["a".into(), "b".into()],
            delimiter: ",".into(),
            skip_header_rows: 1,
        };
        let json = serde_json::to_string(&format).expect("ser");
        let parsed: InputFormat = serde_json::from_str(&json).expect("deser");
        match parsed {
            InputFormat::Csv {
                columns,
                delimiter,
                skip_header_rows,
            } => {
                assert_eq!(columns, vec!["a", "b"]);
                assert_eq!(delimiter, ",");
                assert_eq!(skip_header_rows, 1);
            }
            other => panic!("expected Csv, got {other:?}"),
        }
    }

    #[test]
    fn input_format_tsv_roundtrip() {
        let format = InputFormat::Tsv {
            columns: vec!["x".into()],
        };
        let json = serde_json::to_string(&format).expect("ser");
        let parsed: InputFormat = serde_json::from_str(&json).expect("deser");
        assert!(matches!(parsed, InputFormat::Tsv { .. }));
    }

    #[test]
    fn input_format_parquet_wire_name() {
        let parsed: InputFormat = serde_json::from_str(r#"{"type":"parquet"}"#).expect("deser");
        assert!(matches!(parsed, InputFormat::Parquet { .. }));
    }

    #[test]
    fn input_format_avro_ocf_wire_name() {
        let parsed: InputFormat = serde_json::from_str(r#"{"type":"avro_ocf"}"#).expect("deser");
        assert!(matches!(parsed, InputFormat::AvroOcf { .. }));
    }

    #[test]
    fn input_format_avro_stream_wire_name() {
        let parsed: InputFormat = serde_json::from_str(r#"{"type":"avro_stream"}"#).expect("deser");
        assert!(matches!(parsed, InputFormat::AvroStream { .. }));
    }

    #[test]
    fn input_format_parquet_accepts_flatten_spec() {
        let parsed: InputFormat =
            serde_json::from_str(r#"{"type":"parquet","flattenSpec":{"fields":[]}}"#)
                .expect("deser");
        match parsed {
            InputFormat::Parquet { flatten_spec } => assert!(flatten_spec.is_some()),
            other => panic!("expected Parquet, got {other:?}"),
        }
    }

    #[test]
    fn parse_bytes_json_lines() {
        let fmt = InputFormat::Json;
        let rows = fmt
            .parse_bytes(b"{\"__time\":1000,\"a\":1}\n{\"__time\":2000,\"a\":2}\n")
            .expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["a"], serde_json::json!(1));
    }

    #[test]
    fn parse_bytes_csv() {
        let fmt = InputFormat::Csv {
            columns: vec!["__time".into(), "city".into(), "val".into()],
            delimiter: ",".into(),
            skip_header_rows: 0,
        };
        let rows = fmt.parse_bytes(b"1000,tokyo,1.5\n").expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["city"], serde_json::json!("tokyo"));
        assert_eq!(rows[0]["val"], serde_json::json!(1.5));
    }

    #[test]
    fn parse_bytes_avro_stream_errors() {
        let fmt = InputFormat::AvroStream { flatten_spec: None };
        let err = fmt.parse_bytes(&[0u8, 1, 2]).unwrap_err();
        assert!(err.to_string().contains("avro_stream"));
    }
}
