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

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::io::BufRead;
use std::path::Path;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
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
        /// Number of leading rows to skip (Druid `skipHeaderRows`).
        ///
        /// The enum-level `rename_all = "camelCase"` renames only VARIANTS,
        /// so the Druid wire spelling needs an explicit field rename here —
        /// without it, a real spec's `skipHeaderRows` silently defaulted to
        /// 0 (re-audit Medium fix). The snake_case alias keeps any stored
        /// legacy specs readable.
        #[serde(default, rename = "skipHeaderRows", alias = "skip_header_rows")]
        skip_header_rows: usize,
        /// Multi-value cell delimiter (Druid `listDelimiter`, compat-11).
        /// A cell containing this delimiter splits into a multi-value
        /// (JSON array) row value.  Absent = Druid's default `\u{1}`
        /// (Ctrl-A), matching upstream behaviour.
        #[serde(default, rename = "listDelimiter")]
        list_delimiter: Option<String>,
    },
    /// TSV format with explicit column names.
    #[serde(rename = "tsv")]
    Tsv {
        /// Column names in order.
        columns: Vec<String>,
        /// Number of leading rows to skip (Druid `skipHeaderRows`; same
        /// silent-drop species as the CSV field — see its comment).
        #[serde(default, rename = "skipHeaderRows", alias = "skip_header_rows")]
        skip_header_rows: usize,
        /// Multi-value cell delimiter (Druid `listDelimiter`, compat-11).
        /// Absent = Druid's default `\u{1}` (Ctrl-A).
        #[serde(default, rename = "listDelimiter")]
        list_delimiter: Option<String>,
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
                columns,
                delimiter,
                skip_header_rows,
                list_delimiter,
            } => {
                let text = std::str::from_utf8(data).map_err(|e| {
                    DruidError::Ingestion(format!("CSV buffer is not valid UTF-8: {e}"))
                })?;
                let delim = delimiter.chars().next().unwrap_or(',');
                // CSV parses with RFC 4180 quoting (Druid uses opencsv's
                // RFC4180Parser); TSV below does a plain split — Druid's
                // `tsv`/delimited format has NO quote handling.
                parse_delimited(
                    text,
                    columns,
                    delim,
                    list_delimiter.as_deref(),
                    *skip_header_rows,
                    Quoting::Rfc4180,
                )
            }
            InputFormat::Tsv {
                columns,
                skip_header_rows,
                list_delimiter,
            } => {
                let text = std::str::from_utf8(data).map_err(|e| {
                    DruidError::Ingestion(format!("TSV buffer is not valid UTF-8: {e}"))
                })?;
                parse_delimited(
                    text,
                    columns,
                    '\t',
                    list_delimiter.as_deref(),
                    *skip_header_rows,
                    Quoting::None,
                )
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

/// Druid's default `listDelimiter` for delimited (CSV / TSV) input:
/// `\u{1}` (Ctrl-A).
const DEFAULT_LIST_DELIMITER: &str = "\u{1}";

/// Field-quoting mode for [`parse_delimited`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Quoting {
    /// RFC 4180 double-quote handling — Druid's `csv` inputFormat parses
    /// each line with opencsv's `RFC4180Parser`.
    Rfc4180,
    /// No quote handling (plain split) — Druid's `tsv`/delimited
    /// inputFormat splits on the delimiter with no quoting.
    None,
}

/// Split one physical line into its field values under RFC 4180 quoting,
/// matching Druid's `csv` inputFormat (opencsv `RFC4180Parser`): a `"`
/// opens a quoted run in which the delimiter and any further text are
/// literal and `""` is an escaped literal quote; the enclosing quotes are
/// stripped from the value. Druid parses each physical line independently
/// (`TextReader` iterates lines, so quoted fields cannot span lines); an
/// unterminated quote consumes the rest of the line, as opencsv does.
///
/// PERF: only invoked for lines that actually CONTAIN a `"` — a quote-free
/// line is byte-identical under this state machine and a plain
/// `split(delimiter)` (the machine only diverges on a quote character), so
/// [`split_line`] routes quote-free lines through the zero-copy split and
/// this owned-`String`-per-cell path stays off the common unquoted hot path.
fn split_rfc4180_line(line: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"'); // escaped literal quote
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else if c == '"' {
            in_quotes = true;
        } else if c == delimiter {
            fields.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    fields.push(cur);
    fields
}

/// Split one physical line into cells under the given quoting mode,
/// BORROWING from the line wherever the semantics permit (perf recovery,
/// 2026-07-19 — the RFC 4180 correctness fix had routed every cell through
/// an owned `String`, adding a heap allocation per cell on the common path):
///
/// * [`Quoting::None`] (TSV): always a zero-copy `split` — Druid's
///   `tsv`/delimited inputFormat has NO quote handling, so every cell
///   borrows and quotes stay literal.
/// * [`Quoting::Rfc4180`] (CSV): a line with NO `"` anywhere is
///   byte-identical under the state machine and a plain split, so the
///   overwhelmingly-common unquoted line also borrows; only a line that
///   actually contains `"` runs the allocating [`split_rfc4180_line`]
///   state machine (quotes stripped, `""` unescaped, quoted delimiter
///   literal — semantics unchanged).
fn split_line<'a>(line: &'a str, delimiter: char, quoting: Quoting) -> Vec<Cow<'a, str>> {
    match quoting {
        Quoting::None => line.split(delimiter).map(Cow::Borrowed).collect(),
        Quoting::Rfc4180 => {
            if line.contains('"') {
                split_rfc4180_line(line, delimiter)
                    .into_iter()
                    .map(Cow::Owned)
                    .collect()
            } else {
                line.split(delimiter).map(Cow::Borrowed).collect()
            }
        }
    }
}

/// Parse a delimited (CSV / TSV) text buffer into JSON object rows.
///
/// The first `skip_rows` PHYSICAL lines are skipped unparsed (Druid
/// `skipHeaderRows`: opencsv skips the leading lines of each file before
/// reading data). Lines are NOT trimmed before splitting — leading /
/// trailing whitespace belongs to the first / last cell, as in Druid —
/// though fully-blank lines are still skipped.
///
/// compat-11: a cell containing the `list_delimiter` (explicit
/// `listDelimiter`, or Druid's default `\u{1}` when absent) splits into a
/// JSON ARRAY of its string parts — the ingester then builds a genuine
/// multi-value string row from it.  Cells without the delimiter keep the
/// historical scalar (numeric-probing) conversion unchanged.
fn parse_delimited(
    data: &str,
    columns: &[String],
    delimiter: char,
    list_delimiter: Option<&str>,
    skip_rows: usize,
    quoting: Quoting,
) -> Result<Vec<serde_json::Value>> {
    let list_delim = match list_delimiter {
        Some(ld) if !ld.is_empty() => ld,
        Some(_) => DEFAULT_LIST_DELIMITER, // empty declared delimiter: fall back
        None => DEFAULT_LIST_DELIMITER,
    };
    let mut rows = Vec::new();
    for (line_num, line) in data.lines().enumerate() {
        if line_num < skip_rows {
            continue; // skipHeaderRows: leading lines are never parsed
        }
        if line.trim().is_empty() {
            continue;
        }
        let fields = split_line(line, delimiter, quoting);
        if fields.len() != columns.len() {
            return Err(DruidError::Ingestion(format!(
                "delimited line {} has {} fields, expected {}",
                line_num + 1,
                fields.len(),
                columns.len()
            )));
        }
        let mut obj = serde_json::Map::new();
        // Allocation discipline (perf recovery): numeric probing parses from
        // the borrowed slice (0 allocs for a borrowed numeric cell); a cell
        // that actually becomes a string value is materialised via
        // `Cow::into_owned` — 1 alloc for a borrowed cell, a plain MOVE (0
        // extra allocs) for a cell the RFC 4180 path already owns.
        for (col, val) in columns.iter().zip(fields) {
            if val.contains(list_delim) {
                // Multi-value cell: split into string elements, in order
                // (Druid keeps listDelimiter parts as strings).
                let parts: Vec<serde_json::Value> = val
                    .split(list_delim)
                    .map(|p| serde_json::Value::String(p.to_string()))
                    .collect();
                obj.insert(col.clone(), serde_json::Value::Array(parts));
            } else if let Ok(n) = val.parse::<i64>() {
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
                        obj.insert(col.clone(), serde_json::Value::String(val.into_owned()));
                    }
                }
            } else {
                obj.insert(col.clone(), serde_json::Value::String(val.into_owned()));
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
    /// contains nulls is stored as `LongNullable` (exact `i64` values plus
    /// an explicit null-row bitmap — null-faithful AND exact beyond ±2^53;
    /// the column still reports `LONG` in metadata).  Null-free batches
    /// produce a true `LONG` column with the historical layout.
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

/// Convert a chrono-parsed timestamp to epoch-millis, dead-lettering a leap
/// second.
///
/// chrono ACCEPTS a `:60` leap second by folding it into a `nanosecond()`
/// value `>= 1_000_000_000`, and `timestamp_millis()` then silently
/// normalizes it to the following minute — storing a DIFFERENT instant than
/// the input. Druid (Joda-Time) rejects `:60` outright, so a value carrying
/// the leap marker must be rejected here rather than shifted (Codex R10).
/// Both the offset-aware (`DateTime<Utc>`) and naive (`NaiveDateTime`)
/// branches share this behavior, so both route through this guard.
fn millis_rejecting_leap<T: Timelike>(parsed: &T, millis: i64, raw: &str) -> Result<i64> {
    if parsed.nanosecond() >= 1_000_000_000 {
        return Err(DruidError::Ingestion(format!(
            "leap-second timestamp not supported: '{raw}'"
        )));
    }
    Ok(millis)
}

/// Whether a trailing token in an `auto` timestamp is a time-zone name Druid
/// recognises.
///
/// Druid STRIPS a recognised zone and parses the remaining wall time as UTC —
/// it does NOT shift the instant (a Joda `withZone` quirk: changing the
/// display zone preserves the instant). So this only decides accept-vs-reject,
/// never an offset. The recognised set is the standard-tzdata abbreviations
/// Druid/Joda list: `UTC`/`GMT`/`UT`/`Z` + the STANDARD US zones `EST`/`CST`/
/// `MST`/`PST`. The DAYLIGHT abbreviations `EDT`/`CDT`/`MDT`/`PDT` are NOT in
/// standard OpenJDK tzdata, so Druid rejects them — and so do we (verified via
/// the Codex clean-room behavioral oracle, 2026-07-13).
///
/// DOCUMENTED RESIDUAL (Codex R15): a full IANA `Area/Location` id in the
/// timestamp string (`… America/Los_Angeles`, `… Etc/GMT+5`) is dead-lettered
/// rather than recognised. Exact IANA membership needs a bundled tz database
/// (chrono-tz); a shape heuristic both DROPPED valid ids (`Etc/GMT+5`) and
/// ACCEPTED fictitious ones (`Mars/Olympus`), so it was removed in favour of
/// the exact, safe abbreviation set. Such timestamps are exceedingly rare in
/// real data.
fn is_recognized_zone(token: &str) -> bool {
    matches!(
        token.to_ascii_uppercase().as_str(),
        "UTC" | "GMT" | "UT" | "Z" | "EST" | "CST" | "MST" | "PST"
    )
}

/// Parse a ZONE-LESS ISO wall-clock date/date-time as a UTC instant, at
/// full-second (with optional fractional), minute, or hour precision —
/// including a FRACTIONAL hour or minute (`22.5` == 22:30:00, `22:13.5` ==
/// 22:13:30, `.` or `,` separator, sub-millisecond truncated) — or a
/// date-only / year-month / trailing-`T` (`2023-11-14T`) value (all UTC
/// midnight). The separator must already be the ISO `T`. Rejects a leap
/// second. Returns `None` if no precision matches. Druid `auto` accepts all
/// of these forms (Codex clean-room oracle, 2026-07-13).
fn parse_wallclock_utc_millis(s: &str, raw: &str) -> Option<Result<i64>> {
    if let Ok(naive) = s.parse::<NaiveDateTime>() {
        return Some(millis_rejecting_leap(
            &naive,
            naive.and_utc().timestamp_millis(),
            raw,
        ));
    }
    // Reduced precision: omitted lower fields zero-fill (no leap possible).
    // Minute precision (`…T HH:MM`) — chrono defaults the second to 0.
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M") {
        return Some(Ok(naive.and_utc().timestamp_millis()));
    }
    // Hour precision (`…T HH`) — chrono's `parse_from_str` requires a minute,
    // so build it from the date + hour with minute/second = 0. The hour must be
    // 1–2 ASCII DIGITS: Rust's int parser accepts a leading `+`/`-`, so a plain
    // `.parse::<u32>()` would wrongly accept `…T+1` (which Druid rejects) and
    // store a wrong instant (Codex R15).
    if let Some((date_str, hour_str)) = s.split_once('T')
        && (1..=2).contains(&hour_str.len())
        && hour_str.bytes().all(|b| b.is_ascii_digit())
        && let Ok(date) = date_str.parse::<NaiveDate>()
        && let Ok(hour) = hour_str.parse::<u32>()
        && let Some(naive) = date.and_hms_opt(hour, 0, 0)
    {
        return Some(Ok(naive.and_utc().timestamp_millis()));
    }
    // FRACTIONAL hour / minute (Codex behavioral oracle, 2026-07-13): Joda's
    // ISO time element lets a fraction follow the hour or the minute directly
    // (`…T22.5` == 22:30:00, `…T22:13.5` == 22:13:30), with `.` OR `,` as the
    // separator. The integer part reuses the digit-guarded reduced-precision
    // parsing above; the fraction scales to milliseconds and TRUNCATES below
    // 1 ms (Joda truncates, never rounds). A fraction after full SECONDS is
    // NOT handled here (`HH:MM:SS` never matches the `HH`/`HH:MM` shapes), so
    // an over-long seconds fraction chrono rejected above stays rejected.
    if let Some((date_str, time_str)) = s.split_once('T')
        && let Some(sep) = time_str.find(['.', ','])
    {
        let (int_part, frac_part) = time_str.split_at(sep);
        let frac_digits = &frac_part[1..];
        if !frac_digits.is_empty() && frac_digits.bytes().all(|b| b.is_ascii_digit()) {
            // EXACT integer scaling (Codex R19): routing the fraction through
            // f64 lost 1 ms on inputs like `.29` of an hour (0.29 × 3.6e6 =
            // 1_043_999.999… → 1_043_999). digits×unit/10^len in u64 with
            // floor division is exact for every decimal fraction. Digits are
            // capped at 9 (Joda's own fraction cap; beyond that contributes
            // < 1 ms and is truncation-irrelevant except pathological
            // crafted carries — documented residual).
            let scale_frac = |digits: &str, unit_ms: u64| -> i64 {
                let d = &digits[..digits.len().min(9)];
                let val: u64 = d.parse().unwrap_or(0);
                let pow = 10u64.pow(d.len() as u32);
                #[allow(clippy::cast_possible_wrap)]
                ((u128::from(val) * u128::from(unit_ms) / u128::from(pow)) as i64)
            };
            // Minute precision integer part (`HH:MM`) → fraction of a minute.
            if let Ok(naive) =
                NaiveDateTime::parse_from_str(&format!("{date_str}T{int_part}"), "%Y-%m-%dT%H:%M")
            {
                let frac_millis = scale_frac(frac_digits, 60_000);
                return Some(Ok(naive.and_utc().timestamp_millis() + frac_millis));
            }
            // Hour precision integer part (`HH`) → fraction of an hour. The
            // same 1–2 ASCII-DIGIT guard as the integral hour branch above.
            if (1..=2).contains(&int_part.len())
                && int_part.bytes().all(|b| b.is_ascii_digit())
                && let Ok(date) = date_str.parse::<NaiveDate>()
                && let Ok(hour) = int_part.parse::<u32>()
                && let Some(naive) = date.and_hms_opt(hour, 0, 0)
            {
                let frac_millis = scale_frac(frac_digits, 3_600_000);
                return Some(Ok(naive.and_utc().timestamp_millis() + frac_millis));
            }
        }
    }
    // Trailing-`T` date (`YYYY-MM-DDT`, no time fields): the auto grammar
    // permits it and defaults to UTC midnight (Codex behavioral oracle,
    // 2026-07-13).
    if let Some(date_str) = s.strip_suffix('T')
        && let Ok(date) = date_str.parse::<NaiveDate>()
    {
        return Some(Ok(date
            .and_time(NaiveTime::MIN)
            .and_utc()
            .timestamp_millis()));
    }
    if let Ok(date) = s.parse::<NaiveDate>() {
        return Some(Ok(date
            .and_time(NaiveTime::MIN)
            .and_utc()
            .timestamp_millis()));
    }
    // Reduced calendar date: year-month (`YYYY-MM`) → first of month, UTC
    // midnight, which Druid `auto` accepts (Codex R15). chrono's `NaiveDate`
    // needs a day, so append `-01`. Guard the shape (`NNNN-NN`) so it is not
    // confused with a time fragment.
    if let Some((y, m)) = s.split_once('-')
        && !s.contains('T')
        && y.len() == 4
        && y.bytes().all(|b| b.is_ascii_digit())
        && m.len() == 2
        && m.bytes().all(|b| b.is_ascii_digit())
        && let Ok(date) = format!("{s}-01").parse::<NaiveDate>()
    {
        return Some(Ok(date
            .and_time(NaiveTime::MIN)
            .and_utc()
            .timestamp_millis()));
    }
    None
}

/// Split a trailing explicit UTC offset — `Z`/`z` or `±HH:MM` — off an ISO
/// timestamp string, returning the wall-clock prefix and the offset in
/// signed milliseconds.
///
/// Used for REDUCED-precision forms (`…T22:13Z`, `…T22+09:00`) that chrono's
/// full-precision RFC3339 parse rejects: Druid accepts them and applies the
/// offset (Codex clean-room oracle, 2026-07-13). Only the colon form is
/// recognised here — a colon-less offset (`-0800`) on a reduced-precision
/// value is a documented residual (dead-lettered), and out-of-range
/// `HH`/`MM` digits fail the match rather than produce a wrong instant. The
/// CALLER must verify the prefix still contains a time-of-day: Druid rejects
/// an offset directly after a date-only value (`2023-11-14Z` → REJECT).
fn split_trailing_utc_offset(s: &str) -> Option<(&str, i64)> {
    if let Some(prefix) = s.strip_suffix(['Z', 'z']) {
        return Some((prefix, 0));
    }
    let b = s.as_bytes();
    let n = b.len();
    // `±HH:MM` is 6 bytes; require at least 1 byte of wall-clock prefix.
    if n < 7 {
        return None;
    }
    let sign = match b[n - 6] {
        b'+' => 1_i64,
        b'-' => -1_i64,
        _ => return None,
    };
    if !(b[n - 5].is_ascii_digit()
        && b[n - 4].is_ascii_digit()
        && b[n - 3] == b':'
        && b[n - 2].is_ascii_digit()
        && b[n - 1].is_ascii_digit())
    {
        return None;
    }
    let hours = i64::from(b[n - 5] - b'0') * 10 + i64::from(b[n - 4] - b'0');
    let minutes = i64::from(b[n - 2] - b'0') * 10 + i64::from(b[n - 1] - b'0');
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some((&s[..n - 6], sign * (hours * 3600 + minutes * 60) * 1000))
}

/// Extract epoch-millis from a row's configured `timestamp_column` (exactly,
/// with no `__time` fallback — Druid reads only `timestampSpec.column`).
///
/// Emulates Druid's `auto` timestampSpec grammar. Accepts, in order: a JSON
/// number epoch-millis (integral or floating, truncated toward zero per
/// `Number.longValue`); a numeric-string epoch (`"1700000000000"`); an
/// RFC3339/ISO-8601 string WITH an offset (`…Z`, `…+09:00`), at full OR
/// reduced (minute/hour) precision — the offset is applied, but an offset
/// directly after a value with no time-of-day (`2023-11-14Z`) is rejected;
/// a datetime/date with a trailing RECOGNISED named zone (`… PST`, `… UTC`),
/// which Druid STRIPS and reads as UTC WITHOUT shifting the instant (IANA
/// `Area/Location` ids are a documented residual — dead-lettered); and a
/// zone-less ISO date-time (`T` or space separator; full/minute/hour
/// precision, including a fractional hour or minute `…T22.5`/`…T22:13.5`),
/// or a date-only / year-month / trailing-`T` (`2023-11-14T`) value, all
/// interpreted as UTC.
///
/// A leap second (`:60`) is rejected. This is the single source of truth for
/// a row's timestamp: the batch ingester uses it during `ingest`, and
/// streaming ingestion uses it to pre-validate (dead-letter) a record BEFORE
/// buffering, so one row with a missing / unparseable timestamp cannot fail
/// the whole segment build. NOTE: the parse is `auto`-flavored (the declared
/// format is not threaded), so it is a lenient superset for strict `iso` /
/// `millis`; exotic Joda forms (ordinal/week dates, colon-less offsets) are a
/// documented residual.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] when the timestamp column is absent,
/// null, a non-integer number, an unparseable string, or a non-scalar type.
pub fn extract_row_timestamp_millis(
    row: &serde_json::Value,
    timestamp_column: &str,
) -> Result<i64> {
    extract_row_timestamp_millis_fmt(row, timestamp_column, TsFormat::Auto)
}

/// Declared `timestampSpec.format`, threaded from the validated spec down to
/// row extraction.
///
/// The three formats genuinely DIFFER (Codex clean-room oracle): `auto` reads
/// an all-digit string as epoch-millis; `iso` reads it as an ISO YEAR
/// (`"2023"` → 2023-01-01T00:00:00Z) with a signed-integer-millis fallback
/// only where ISO parsing fails (≥10-digit values); `millis` accepts ONLY
/// numeric input. Before the format was threaded, a declared `iso` stored
/// `"2023"` as 2023 ms — a WRONG instant (Fable audit).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TsFormat {
    /// Druid `auto`: millis-or-ISO detection (`T` or space separator, named
    /// zones, reduced precision).
    #[default]
    Auto,
    /// Strict ISO-8601: `T` separator only, no named zones; bare numbers are
    /// ISO years, falling back to integer millis only when ISO rejects them.
    Iso,
    /// Epoch milliseconds only: a signed-integer string or a number.
    Millis,
}

impl TsFormat {
    /// Map a declared `timestampSpec.format` string (case-insensitive) onto
    /// the implemented grammar. This is the SINGLE shared spec→[`TsFormat`]
    /// mapping for every ingestion path (Kafka / Kinesis supervisors and
    /// native batch), so a format the extractor does not implement is
    /// rejected identically everywhere instead of silently mis-parsed as
    /// `auto` (compat-9 P0: a declared `posix`/`nano`/custom pattern read
    /// as `auto` stores a WRONG instant — e.g. a posix-seconds value read
    /// as millis lands in 1970).
    ///
    /// # Errors
    ///
    /// Returns the client-facing message for an unimplemented format as a
    /// plain `String` so each caller can wrap it in its own error type
    /// (`DruidError::Ingestion`, `KafkaIngestError::Deserialization`, …).
    pub fn from_spec_format(format: &str) -> std::result::Result<Self, String> {
        match format.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "iso" => Ok(Self::Iso),
            "millis" => Ok(Self::Millis),
            _ => Err(format!(
                "unsupported timestampSpec.format {format:?} (supported: auto, iso, millis; \
                 \"posix\"/\"nano\"/custom patterns would be silently mis-parsed as \
                 milliseconds)"
            )),
        }
    }
}

/// [`extract_row_timestamp_millis`] with the DECLARED `timestampSpec.format`
/// threaded through (see [`TsFormat`]).
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] when the timestamp column is absent,
/// null, or its value is not acceptable under the declared format.
pub fn extract_row_timestamp_millis_fmt(
    row: &serde_json::Value,
    timestamp_column: &str,
    format: TsFormat,
) -> Result<i64> {
    // Read ONLY the configured timestamp column — Druid reads exactly
    // `timestampSpec.column` and does NOT fall back to `__time` (Codex R17).
    let ts_val = row.get(timestamp_column);
    if matches!(ts_val, None | Some(serde_json::Value::Null)) {
        return Err(DruidError::Ingestion(format!(
            "timestamp column '{timestamp_column}' is absent or null"
        )));
    }
    let v = ts_val.expect("checked above");
    match format {
        TsFormat::Auto => extract_auto_timestamp(v, timestamp_column),
        TsFormat::Iso => extract_iso_timestamp(v),
        TsFormat::Millis => extract_millis_timestamp(v),
    }
}

/// Druid `millis` format: a signed-integer STRING or a number
/// (`Number.longValue` semantics — floats truncate toward zero). ISO text is
/// rejected (oracle: `millis` + `"2023-11-14"` → REJECT).
fn extract_millis_timestamp(v: &serde_json::Value) -> Result<i64> {
    match v {
        serde_json::Value::Number(_) => json_to_i64(v).ok_or_else(|| {
            DruidError::Ingestion(format!("timestamp number is out of the i64 range: {v}"))
        }),
        serde_json::Value::String(s) => {
            let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
            if !digits.is_empty()
                && digits.bytes().all(|b| b.is_ascii_digit())
                && let Ok(millis) = s.parse::<i64>()
            {
                Ok(millis)
            } else {
                Err(DruidError::Ingestion(format!(
                    "format \"millis\" requires a signed-integer epoch, got '{s}'"
                )))
            }
        }
        other => Err(DruidError::Ingestion(format!(
            "unsupported timestamp type: {other}"
        ))),
    }
}

/// A bare signed integer under Druid `iso`: Joda parses it as an ISO YEAR
/// first (`"2023"` → Jan 1 UTC, `"-1"` → year −1); values Joda's ISO parser
/// REJECTS (≥10-digit years overflow its year field) fall back to signed
/// integer epoch-millis (oracle: `"1700000000000"` → millis, `"-1"` → year).
/// Joda-valid years chrono cannot represent (|year| in 262 144..=999 999 999)
/// fail CLOSED — dead-letter, never a wrong instant.
fn iso_integer_to_millis(t: &str) -> Result<i64> {
    let digits = t.strip_prefix(['+', '-']).unwrap_or(t);
    if let Ok(y) = t.parse::<i32>()
        && (-262_143..=262_142).contains(&y)
        && let Some(d) = NaiveDate::from_ymd_opt(y, 1, 1)
    {
        return Ok(d.and_time(NaiveTime::MIN).and_utc().timestamp_millis());
    }
    if digits.len() >= 10 {
        t.parse::<i64>().map_err(|_| {
            DruidError::Ingestion(format!("iso timestamp is out of the i64 range: '{t}'"))
        })
    } else {
        Err(DruidError::Ingestion(format!(
            "iso year '{t}' is outside the representable range (±262143); larger \
             Joda-valid years are a documented fail-closed residual"
        )))
    }
}

/// Druid `iso` format: strict ISO-8601 — `T` separator only (no space forms),
/// no named zones; bare integers are ISO years with an integer-millis
/// fallback (see [`iso_integer_to_millis`]); floating-point numbers are
/// rejected (oracle: `iso` + `1.7e12` → REJECT).
fn extract_iso_timestamp(v: &serde_json::Value) -> Result<i64> {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                iso_integer_to_millis(&i.to_string())
            } else {
                Err(DruidError::Ingestion(format!(
                    "format \"iso\" rejects this numeric timestamp: {n}"
                )))
            }
        }
        serde_json::Value::String(s) => {
            // Joda tolerates surrounding whitespace (Codex R16: `" 2023 "`).
            let t = s.trim();
            if t.is_empty() {
                return Err(DruidError::Ingestion(format!(
                    "failed to parse iso timestamp '{s}'"
                )));
            }
            let digits = t.strip_prefix(['+', '-']).unwrap_or(t);
            if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
                return iso_integer_to_millis(t);
            }
            // Strict ISO: no space separator, no named-zone extraction
            // (oracle: iso + `"2023-11-14 22:13:20"` / `"… PST"` → REJECT).
            if t.contains(' ') {
                return Err(DruidError::Ingestion(format!(
                    "format \"iso\" requires the 'T' separator (no spaces): '{s}'"
                )));
            }
            if let Ok(dt) = t.parse::<DateTime<Utc>>() {
                return millis_rejecting_leap(&dt, dt.timestamp_millis(), s);
            }
            if let Some(res) = parse_wallclock_utc_millis(t, s) {
                return res;
            }
            if let Some((wall, offset_millis)) = split_trailing_utc_offset(t)
                && let Some((_, time_part)) = wall.split_once('T')
                && !time_part.is_empty()
                && let Some(res) = parse_wallclock_utc_millis(wall, s)
            {
                return res.map(|millis| millis - offset_millis);
            }
            Err(DruidError::Ingestion(format!(
                "failed to parse iso timestamp '{s}'"
            )))
        }
        other => Err(DruidError::Ingestion(format!(
            "unsupported timestamp type: {other}"
        ))),
    }
}

/// Druid `auto` format (see [`extract_row_timestamp_millis`] for the accepted
/// grammar).
fn extract_auto_timestamp(v: &serde_json::Value, timestamp_column: &str) -> Result<i64> {
    let ts_val = Some(v);
    match ts_val {
        // A JSON number is epoch-millis; a float truncates toward zero and an
        // out-of-i64-range value is rejected (Codex R14 oracle), via the same
        // coercion the long-column path uses. ADJUDICATED (FG-8, Fable audit
        // #8): Druid wraps a value > i64::MAX via Java `Number.longValue`
        // (two's-complement, a garbage NEGATIVE instant); we deliberately
        // fail CLOSED instead of emulating the wrap.
        Some(v @ serde_json::Value::Number(_)) => json_to_i64(v).ok_or_else(|| {
            DruidError::Ingestion(format!("timestamp number is out of the i64 range: {v}"))
        }),
        Some(serde_json::Value::String(s)) => {
            //   1. numeric-string epoch-millis: the RAW (un-trimmed) string must
            //      be ALL-ASCII-DIGITS, because Druid `auto` classifies the
            //      input as-is — `" 2023 "` has spaces, so it is NOT the millis
            //      path but ISO year 2023 (Codex R16). Trimming first would
            //      store the WRONG instant (2023 ms). A SIGNED string (`"-1"`)
            //      is likewise not all-digits → Druid reads ISO year −1 and
            //      Rust's `parse::<i64>()` would store `-1` ms (Codex R15). Both
            //      fall through to the datetime grammar (and dead-letter if not
            //      a form we implement — safer than a wrong instant; signed /
            //      ISO-year-only / whitespace-padded numeric strings are a
            //      documented residual).
            if !s.is_empty()
                && s.bytes().all(|b| b.is_ascii_digit())
                && let Ok(millis) = s.parse::<i64>()
            {
                return Ok(millis);
            }
            let trimmed = s.trim();
            //   2. RFC3339 / ISO-8601 WITH an explicit offset (`…Z`, `…+09:00`).
            //      The offset IS applied (unlike a trailing NAMED zone below).
            if let Ok(dt) = trimmed.parse::<DateTime<Utc>>() {
                return millis_rejecting_leap(&dt, dt.timestamp_millis(), s);
            }
            //   3. A trailing RECOGNISED named zone (`… PST`,
            //      `… America/Los_Angeles`). Druid STRIPS it and parses the
            //      remainder as UTC — it does NOT shift the instant (Joda
            //      `withZone` quirk; corrected from an earlier shifting impl
            //      via the Codex clean-room oracle). Unrecognised zones (incl.
            //      EDT/CDT/MDT/PDT) are not stripped → they fall through to the
            //      reject below.
            if let Some((dt_part, zone)) = trimmed.rsplit_once(' ')
                && is_recognized_zone(zone)
                && let Some(res) =
                    parse_wallclock_utc_millis(&dt_part.trim_end().replacen(' ', "T", 1), s)
            {
                return res;
            }
            //   4. Zone-less wall-clock: `T` OR space separator, at full/
            //      minute/hour precision, or date-only — all interpreted as
            //      UTC (Druid `auto`). Normalise the first space to `T` so the
            //      space form reuses the same parser (Codex R11).
            if let Some(res) = parse_wallclock_utc_millis(&trimmed.replacen(' ', "T", 1), s) {
                return res;
            }
            //   5. REDUCED-precision wall-clock WITH an explicit offset
            //      (`…T22:13Z`, `…T22+09:00`): chrono's step 2 only parses
            //      FULL-precision offset forms, so these land here. Strip the
            //      offset, parse the remainder through the same wallclock
            //      grammar (leap-second rejection included), then SUBTRACT
            //      the offset — the wall time is local to it. Druid rejects
            //      an offset directly after a value with NO time-of-day
            //      (`2023-11-14Z`, `2023-11-14+09:00` → REJECT, oracle), so
            //      the remainder must have a non-empty time part.
            if let Some((wall, offset_millis)) = split_trailing_utc_offset(trimmed) {
                let wall_t = wall.replacen(' ', "T", 1);
                if let Some((_, time_part)) = wall_t.split_once('T')
                    && !time_part.is_empty()
                    && let Some(res) = parse_wallclock_utc_millis(&wall_t, s)
                {
                    return res.map(|millis| millis - offset_millis);
                }
            }
            Err(DruidError::Ingestion(format!(
                "failed to parse timestamp '{s}': not epoch-millis, RFC3339, a recognised \
                 named zone, or ISO-8601 date/date-time"
            )))
        }
        Some(serde_json::Value::Null) | None => Err(DruidError::Ingestion(format!(
            "timestamp column '{timestamp_column}' is absent or null"
        ))),
        Some(other) => Err(DruidError::Ingestion(format!(
            "unsupported timestamp type: {other}"
        ))),
    }
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

/// Largest magnitude exactly representable in `f64` (2^53).
///
/// Historical role: while null-bearing long columns degraded to a NaN-null
/// `DOUBLE` (pre 2026-07), a batch mixing NULLs with a value beyond this
/// range could not be stored exactly.  The batch path now stores such
/// columns as `LongNullable` (i64-exact), so this bound only backs the
/// STREAMING pre-flight classifier [`long_dim_value_class`], which keeps
/// its fail-closed dead-letter behavior until streaming is rewired onto
/// the nullable-long layout (follow-on).
const F64_EXACT_MAX: i64 = 9_007_199_254_740_992;

/// Classify a row's value for a LONG dimension, for streaming's exact-storage
/// pre-flight check.
///
/// Returns `None` when the value is NULL / absent / non-coercible, `Some(true)`
/// when the coerced integer is OUTSIDE the f64-exact range (±2^53), and
/// `Some(false)` when in range. Streaming uses this to dead-letter the
/// CONFLICTING record rather than losing the whole segment (Codex R14).
/// NOTE: the BATCH path no longer fails closed on this mix —
/// [`build_long_column`] stores null-bearing longs i64-exactly as
/// `LongNullable`; the streaming guard is deliberately left fail-closed
/// until its wire-up is revisited (follow-on).
#[must_use]
pub fn long_dim_value_class(row: &serde_json::Value, dimension: &str) -> Option<bool> {
    json_to_i64(row.get(dimension)?).map(|v| v.unsigned_abs() > F64_EXACT_MAX as u64)
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
            match classify_plain_numeric(t) {
                PlainNumeric::Value(v) => Some(v),
                // Fail CLOSED (Codex R19): a plain-shaped overflow must not
                // reach the f64 fallback, which would round it into range.
                PlainNumeric::Overflow => None,
                PlainNumeric::NotPlain => t.parse::<f64>().ok().and_then(f64_to_i64),
            }
        }
        _ => None,
    }
}

/// Tri-state classification of a PLAIN numeric string
/// (`[+-]?digits[.digits]`), never routing through `f64`.
enum PlainNumeric {
    /// Exact value, truncated toward zero.
    Value(i64),
    /// Recognised plain-numeric shape whose integral part OVERFLOWS i64.
    /// Fails CLOSED (NULL): the f64 fallback must NOT run — it would round
    /// the value back INTO range (e.g. `"-9223372036854775809.0"` →
    /// `i64::MIN`) and store a wrong long (Codex R19).
    Overflow,
    /// Not a plain numeric shape (scientific notation etc.); the lossy f64
    /// fallback may apply (documented residual).
    NotPlain,
}

/// Classify a trimmed string per [`PlainNumeric`]. Plain-decimal truncation
/// toward zero equals the integral part exactly across the whole i64 range
/// (no f64 round-trip — `"9007199254740993.0"` must stay …993, Codex R18).
fn classify_plain_numeric(t: &str) -> PlainNumeric {
    let (int_part, frac) = match t.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (t, None),
    };
    if let Some(f) = frac
        && !f.bytes().all(|b| b.is_ascii_digit())
    {
        return PlainNumeric::NotPlain; // "1.5e3" etc.
    }
    let unsigned = int_part.strip_prefix(['+', '-']).unwrap_or(int_part);
    if !unsigned.bytes().all(|b| b.is_ascii_digit()) {
        return PlainNumeric::NotPlain;
    }
    if unsigned.is_empty() {
        // ".5"/"-.5" truncate to 0; a bare "."/"-."/"" is not a number.
        return match frac {
            Some(f) if !f.is_empty() => PlainNumeric::Value(0),
            _ => PlainNumeric::NotPlain,
        };
    }
    match int_part.parse::<i64>() {
        Ok(v) => PlainNumeric::Value(v),
        // All-digit integral part that i64 cannot hold: unstorable as a
        // long — NULL, never a rounded/wrapped stand-in.
        Err(_) => PlainNumeric::Overflow,
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
    /// Declared `timestampSpec.format` (default [`TsFormat::Auto`]); threaded
    /// down to row extraction because the formats genuinely differ (a
    /// declared `iso` reads `"2023"` as the YEAR 2023, not 2023 ms — Fable
    /// audit).
    timestamp_format: TsFormat,
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
            timestamp_format: TsFormat::Auto,
        }
    }

    /// Set the declared `timestampSpec.format` (default [`TsFormat::Auto`]).
    /// Streaming ingestion threads the validated spec's format through so a
    /// declared `iso`/`millis` is honored at extraction time (Fable audit).
    #[must_use]
    pub fn with_timestamp_format(mut self, format: TsFormat) -> Self {
        self.timestamp_format = format;
        self
    }

    /// Ingest a batch of JSON rows and produce segment data.
    ///
    /// Null and absent values are preserved as SQL NULL (see the crate-level
    /// null-handling notes).  `count`-type metrics store 1 per raw row.
    pub fn ingest(&self, rows: Vec<serde_json::Value>) -> Result<IngestedSegment> {
        if rows.is_empty() {
            return Err(DruidError::Ingestion("no rows to ingest".to_string()));
        }

        // Parse rows into (timestamp_millis, row) under the DECLARED
        // `timestampSpec.format`.
        let mut parsed = Vec::with_capacity(rows.len());
        for row in rows {
            let ts = self.extract_timestamp(&row)?;
            parsed.push((ts, row));
        }
        self.ingest_parsed(parsed, /* rolled */ false)
    }

    /// Shared ingestion core over timestamp-resolved rows.
    ///
    /// `rolled` is `true` only for the internal rollup path, whose
    /// pre-aggregated rows carry EVERY metric value — the merged group
    /// count of a `count`-type metric and each rolled sum alike — under
    /// the metric's OUTPUT `name`.  For raw (rollup=false) rows, a `count`
    /// metric is always 1 per row — Druid's `count` aggregator has no
    /// `fieldName` and counts input rows, ignoring any same-named field in
    /// the data — and every other metric reads its SOURCE `fieldName`
    /// (codex r11).  Rolled rows must NOT be read back through
    /// `fieldName`: Druid permits a metric's `fieldName` to name a
    /// dimension (only OUTPUT-name collisions are rejected), so a rolled
    /// sum round-tripped under the source field would collide with the
    /// dimension value (re-audit Medium fix).
    fn ingest_parsed(
        &self,
        parsed_rows: Vec<(i64, serde_json::Value)>,
        rolled: bool,
    ) -> Result<IngestedSegment> {
        if parsed_rows.is_empty() {
            return Err(DruidError::Ingestion("no rows to ingest".to_string()));
        }

        let mut parsed: Vec<(i64, &serde_json::Value)> =
            parsed_rows.iter().map(|(ts, row)| (*ts, row)).collect();

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
                let values: Vec<i64> = if rolled {
                    parsed
                        .iter()
                        .map(|(_, row)| row.get(name).and_then(json_to_i64).unwrap_or(1))
                        .collect()
                } else {
                    vec![1_i64; num_rows]
                };
                ColumnData::Long(values)
            } else {
                let field = if rolled {
                    // Rolled rows carry each sum under its OUTPUT name
                    // (see `ingest_with_rollup`) — never re-read through
                    // `fieldName`, which may legally name a dimension.
                    name
                } else {
                    // codex-review r11: raw rows read the SOURCE field
                    // (`fieldName`), not the output `name` — a renamed
                    // metric ({"name":"sum_value","fieldName":"value"})
                    // previously read the nonexistent output field and
                    // stored NULL on every row.
                    spec.get("fieldName")
                        .and_then(|v| v.as_str())
                        .unwrap_or(name)
                };
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
            let mut dim_key: Vec<Option<String>> = Vec::with_capacity(self.dim_schemas.len());
            for schema in &self.dim_schemas {
                let value = row.get(&schema.name);
                // compat-11 MV fail-loud: rollup has no element-wise
                // multi-value semantics yet.  Pre-fix, an array dimension
                // value was scalarised into the rollup grouping key as JSON
                // text (`["a","b"]` → `"[\"a\",\"b\"]"`), so a later
                // groupBy returned ONE corrupt array-text group instead of
                // exploded element groups.  Error loud instead — rollup
                // stays DISABLED for MV data until MV rollup lands
                // (`rollup:false` + MV is fully working, oracle-verified).
                if let Some(serde_json::Value::Array(_)) = value {
                    return Err(DruidError::Ingestion(format!(
                        "rollup over a multi-value dimension `{}` is not supported yet \
                         — ingest with rollup disabled, or upgrade when MV rollup lands",
                        schema.name
                    )));
                }
                let key = value.and_then(|v| match schema.dim_type {
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
                });
                // W-B legacy null mode: a null/absent dimension keys as its
                // coercion DEFAULT (`""` / `0`), merging with genuine
                // default-valued rows at rollup time — a legacy writer never
                // distinguishes them.  (Derived from the pinned per-column
                // coercion; a rollup-specific oracle fixture is a follow-on.)
                let key = match key {
                    None if ferrodruid_common::legacy_null_mode() => Some(match schema.dim_type {
                        DimensionType::String => String::new(),
                        // Matches the canonical numeric key formatting above
                        // (`0`/`0.0` both key as "0").
                        DimensionType::Long => 0i64.to_string(),
                        DimensionType::Double | DimensionType::Float => 0.0f64.to_string(),
                    }),
                    k => k,
                };
                dim_key.push(key);
            }

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

        // Convert groups back to (timestamp_millis, row) pairs. The
        // truncated timestamp travels OUT-OF-BAND — it is already epoch
        // millis. Re-emitting it into the row and re-parsing under the
        // DECLARED `timestampSpec.format` mis-read numeric millis as ISO
        // YEARS under format=iso (|v| <= 262142 -> the year, 6-9-digit
        // values -> hard error), and a metric whose `fieldName` named the
        // timestamp column clobbered it (re-audit Low + Medium fix).
        let mut rolled_rows: Vec<(i64, serde_json::Value)> = Vec::with_capacity(groups.len());
        for ((ts, dim_vals), (count, metric_sums)) in &groups {
            let mut obj = serde_json::Map::new();
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
                    // Emit under the OUTPUT name — the rolled re-ingest
                    // reads each metric back from `name` (see
                    // `ingest_parsed`). The previous round-trip through
                    // the SOURCE `fieldName` overwrote the dimension (or
                    // timestamp) value whenever `fieldName` named one:
                    // Druid permits a dimension to double as an aggregator
                    // input, e.g. dimensions ["price"] + doubleSum
                    // {name:"total", fieldName:"price"} must store the
                    // group's dimension value under `price` and the sum
                    // under `total` (re-audit Medium fix).
                    obj.insert(m.clone(), serde_json::Value::Number(n));
                }
            }
            rolled_rows.push((*ts, serde_json::Value::Object(obj)));
        }

        self.ingest_parsed(rolled_rows, /* rolled */ true)
    }

    /// Ingest from CSV data.
    ///
    /// Parses the CSV string using the given column names and delimiter —
    /// with RFC 4180 quote handling, via the same parser as
    /// [`InputFormat::parse_bytes`] (a private duplicate here previously
    /// kept the naive-split quote bug alive on this path) — converting
    /// each row into a JSON object before ingesting.
    pub fn ingest_csv(
        &self,
        data: &str,
        columns: &[String],
        delimiter: char,
    ) -> Result<IngestedSegment> {
        let rows = parse_delimited(data, columns, delimiter, None, 0, Quoting::Rfc4180)?;
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

    /// Extract epoch milliseconds from a row under the declared format.
    fn extract_timestamp(&self, row: &serde_json::Value) -> Result<i64> {
        extract_row_timestamp_millis_fmt(row, &self.timestamp_column, self.timestamp_format)
    }

    /// Build a dictionary-encoded string column with bitmap indexes.
    ///
    /// Null and absent inputs are preserved as SQL NULL — distinct from
    /// `""` — via the trailing null-row bitmap (see
    /// [`StringColumnData::null_rows`]).
    ///
    /// # Multi-value dimensions (compat-11)
    ///
    /// A JSON ARRAY value makes the row a multi-value row (its elements,
    /// in order), and the column becomes
    /// [`ferrodruid_segment::column::StringMultiColumnData`]
    /// (`ColumnData::StringMulti`) iff ANY row carries an explicit array.
    /// A column whose every row is a scalar/null builds the historical
    /// single-value [`StringColumnData`] byte-for-byte (ADDITIVE
    /// strategy).  An empty array stores the row as SQL NULL (Druid's
    /// `[]`/`null` equivalence for MV string dimensions).
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Ingestion`] when an array element is itself
    /// null, an array, or an object — Druid's null-element and nested
    /// shapes are documented follow-ons, and silently coercing them would
    /// be wrong.
    fn build_string_column(
        &self,
        dim_name: &str,
        rows: &[(i64, &serde_json::Value)],
    ) -> Result<ColumnData> {
        let any_array = rows
            .iter()
            .any(|(_, row)| matches!(row.get(dim_name), Some(serde_json::Value::Array(_))));

        if !any_array {
            // Single-value path — UNCHANGED (byte-for-byte historical
            // layout for every pre-existing input shape).
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|(_, row)| match row.get(dim_name) {
                    Some(serde_json::Value::String(s)) => Some(s.clone()),
                    Some(serde_json::Value::Number(n)) => Some(n.to_string()),
                    Some(serde_json::Value::Bool(b)) => Some(b.to_string()),
                    // W-B legacy null mode: coerce null/absent to `""` at
                    // ingest — a legacy writer stores NO null markers, and
                    // `''` and absent land on the SAME dictionary id
                    // (oracle: the captured Druid 27 segment's dump shows
                    // exactly that shape).
                    Some(serde_json::Value::Null) | None => {
                        if ferrodruid_common::legacy_null_mode() {
                            Some(String::new())
                        } else {
                            None
                        }
                    }
                    Some(other) => Some(other.to_string()),
                })
                .collect();

            return Ok(ColumnData::String(StringColumnData::from_nullable_values(
                &values,
            )));
        }

        // Multi-value path: every row becomes an ordered element list;
        // null/absent and `[]` become the empty (SQL NULL) row.
        let mut mv_rows: Vec<Vec<String>> = Vec::with_capacity(rows.len());
        for (_, row) in rows {
            let elements = match row.get(dim_name) {
                Some(serde_json::Value::Array(elems)) => {
                    let mut out = Vec::with_capacity(elems.len());
                    for e in elems {
                        match e {
                            serde_json::Value::String(s) => out.push(s.clone()),
                            serde_json::Value::Number(n) => out.push(n.to_string()),
                            serde_json::Value::Bool(b) => out.push(b.to_string()),
                            other => {
                                return Err(DruidError::Ingestion(format!(
                                    "multi-value dimension '{dim_name}': unsupported array \
                                     element {other} (null elements and nested arrays/objects \
                                     inside a multi-value row are not yet supported)"
                                )));
                            }
                        }
                    }
                    out
                }
                Some(serde_json::Value::String(s)) => vec![s.clone()],
                Some(serde_json::Value::Number(n)) => vec![n.to_string()],
                Some(serde_json::Value::Bool(b)) => vec![b.to_string()],
                Some(serde_json::Value::Null) | None => Vec::new(),
                Some(other) => vec![other.to_string()],
            };
            mv_rows.push(elements);
        }

        Ok(ColumnData::StringMulti(
            ferrodruid_segment::column::StringMultiColumnData::from_rows(&mv_rows),
        ))
    }

    /// Build a `DOUBLE` column for a typed dimension; nulls stored as NaN
    /// (W-B legacy null mode: as the literal `0.0` default — a legacy
    /// writer stores plain zeros, oracle `ext_native_scan.json`).
    fn build_double_column(name: &str, rows: &[(i64, &serde_json::Value)]) -> ColumnData {
        let null_marker = if ferrodruid_common::legacy_null_mode() {
            0.0
        } else {
            NULL_DOUBLE
        };
        let values: Vec<f64> = rows
            .iter()
            .map(|(_, row)| row.get(name).and_then(json_to_f64).unwrap_or(null_marker))
            .collect();
        ColumnData::Double(values)
    }

    /// Build a `FLOAT` column for a typed dimension; nulls stored as NaN
    /// (W-B legacy null mode: as the literal `0.0` default).
    fn build_float_column(name: &str, rows: &[(i64, &serde_json::Value)]) -> ColumnData {
        let null_marker = if ferrodruid_common::legacy_null_mode() {
            0.0
        } else {
            NULL_FLOAT
        };
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
                    .map_or(null_marker, |v| v)
            })
            .collect();
        ColumnData::Float(values)
    }

    /// Build a `LONG` column for a typed dimension.
    ///
    /// `i64` has no in-band NULL marker, so if any input is null the column
    /// is stored as [`ColumnData::LongNullable`]: the exact `i64` values
    /// (with `0` at NULL rows) plus an explicit null-row bitmap.  Values
    /// beyond ±2^53 are preserved exactly (null-faithful AND i64-exact; see
    /// [`DimensionType::Long`]).  Null-free inputs produce a true `LONG`
    /// column with the historical layout byte-for-byte.
    ///
    /// (History: before 2026-07 the null-bearing case degraded to a NaN-null
    /// `DOUBLE`, and a batch mixing NULLs with values beyond ±2^53 failed
    /// closed — codex-review r6, 2026-07-11.  The `LongNullable` layout
    /// removes both the precision loss and the refusal.)
    fn build_long_column(name: &str, rows: &[(i64, &serde_json::Value)]) -> Result<ColumnData> {
        let values: Vec<Option<i64>> = rows
            .iter()
            .map(|(_, row)| row.get(name).and_then(json_to_i64))
            .collect();
        // W-B legacy null mode: coerce null/absent to the literal `0` and
        // keep the plain `LONG` layout — a legacy writer emits NO null
        // bitmap (oracle: Druid 27 dump-segment reports `hasNulls: false`
        // on the all-null `x`).
        if ferrodruid_common::legacy_null_mode() {
            return Ok(ColumnData::Long(
                values.into_iter().map(|v| v.unwrap_or(0)).collect(),
            ));
        }
        if values.iter().all(Option::is_some) {
            return Ok(ColumnData::Long(values.into_iter().flatten().collect()));
        }
        let (vals, nulls) = ferrodruid_segment::column::long_nullable_parts(&values);
        Ok(ColumnData::LongNullable(vals, nulls))
    }

    /// Build a numeric (double) column for a non-`count` metric; nulls,
    /// absent fields, and non-coercible values are stored as NULL (NaN) —
    /// never 0-filled.
    fn build_metric_column(
        &self,
        metric_name: &str,
        rows: &[(i64, &serde_json::Value)],
    ) -> ColumnData {
        let null_marker = if ferrodruid_common::legacy_null_mode() {
            // W-B legacy null mode: a legacy writer 0-fills missing metric
            // inputs (no NaN-null marker exists on disk).
            0.0
        } else {
            NULL_DOUBLE
        };
        let values: Vec<f64> = rows
            .iter()
            .map(|(_, row)| {
                row.get(metric_name)
                    .and_then(json_to_f64)
                    .unwrap_or(null_marker)
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

    // -----------------------------------------------------------------------
    // compat-11: multi-value string dimensions
    // -----------------------------------------------------------------------

    /// JSON `{tag:["a","b"]}, {tag:"c"}, {tag:[]}` ingests as a genuine
    /// `StringMulti` column: the array row keeps BOTH elements in order,
    /// the scalar row is a 1-element row, and `[]` is the null row.
    #[test]
    fn ingest_json_array_builds_string_multi() {
        let rows = vec![
            serde_json::json!({"__time": 1000, "tag": ["a", "b"]}),
            serde_json::json!({"__time": 2000, "tag": "c"}),
            serde_json::json!({"__time": 3000, "tag": []}),
        ];
        let ingester = BatchIngester::new("ds".into(), "__time".into(), vec!["tag".into()], vec![]);
        let result = ingester.ingest(rows).expect("ingest");
        match result.segment_data.column("tag").expect("tag") {
            ColumnData::StringMulti(mc) => {
                assert_eq!(mc.num_rows(), 3);
                assert_eq!(mc.row_values(0), vec!["a", "b"]);
                assert_eq!(mc.row_values(1), vec!["c"]);
                assert!(mc.is_null_row(2), "[] ingests as the null row");
                // Dictionary: a, b, c sorted.
                assert_eq!(mc.dictionary.len(), 3);
            }
            other => panic!("expected StringMulti column, got {other:?}"),
        }
    }

    /// A purely single-value column stays `String` — byte-for-byte the
    /// historical layout (ADDITIVE guarantee), even when another dimension
    /// in the same batch is multi-value.
    #[test]
    fn ingest_single_value_column_stays_string() {
        let rows = vec![
            serde_json::json!({"__time": 1000, "tag": ["a", "b"], "city": "tokyo"}),
            serde_json::json!({"__time": 2000, "tag": "c", "city": "osaka"}),
        ];
        let ingester = BatchIngester::new(
            "ds".into(),
            "__time".into(),
            vec!["tag".into(), "city".into()],
            vec![],
        );
        let result = ingester.ingest(rows).expect("ingest");
        match result.segment_data.column("city").expect("city") {
            ColumnData::String(sc) => {
                assert_eq!(sc.dictionary.len(), 2);
                assert_eq!(sc.encoded_values.len(), 2);
                assert_eq!(sc.bitmap_indexes.len(), 2, "historical layout");
            }
            other => panic!("single-value column must stay String, got {other:?}"),
        }
        assert!(matches!(
            result.segment_data.column("tag"),
            Some(ColumnData::StringMulti(_))
        ));
    }

    /// A 1-element explicit array still triggers the MV column shape
    /// (explicit-array rule), but the row itself holds one element.
    #[test]
    fn ingest_single_element_array_still_multi_column() {
        let rows = vec![
            serde_json::json!({"__time": 1000, "tag": ["only"]}),
            serde_json::json!({"__time": 2000, "tag": "plain"}),
        ];
        let ingester = BatchIngester::new("ds".into(), "__time".into(), vec!["tag".into()], vec![]);
        let result = ingester.ingest(rows).expect("ingest");
        match result.segment_data.column("tag").expect("tag") {
            ColumnData::StringMulti(mc) => {
                assert_eq!(mc.row_values(0), vec!["only"]);
                assert_eq!(mc.row_values(1), vec!["plain"]);
            }
            other => panic!("expected StringMulti column, got {other:?}"),
        }
    }

    /// Null / nested elements inside an MV array FAIL LOUD (documented
    /// follow-on), never silently coerce.
    #[test]
    fn ingest_mv_array_null_element_fails_loud() {
        let rows = vec![serde_json::json!({"__time": 1000, "tag": ["a", null]})];
        let ingester = BatchIngester::new("ds".into(), "__time".into(), vec!["tag".into()], vec![]);
        let err = ingester.ingest(rows).expect_err("null element must fail");
        assert!(
            err.to_string().contains("multi-value dimension"),
            "expected loud MV-element error, got: {err}"
        );
    }

    /// CSV `listDelimiter` splits a cell into a multi-value row; unsplit
    /// cells stay scalars.
    #[test]
    fn parse_delimited_list_delimiter_splits_cells() {
        let fmt = InputFormat::Csv {
            columns: vec!["__time".into(), "tags".into()],
            delimiter: ",".into(),
            skip_header_rows: 0,
            list_delimiter: Some("|".into()),
        };
        let rows = fmt.parse_bytes(b"1000,a|b\n2000,c\n").expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["tags"], serde_json::json!(["a", "b"]));
        assert_eq!(rows[1]["tags"], serde_json::json!("c"));

        // End-to-end: the split cell ingests as a StringMulti row.
        let ingester =
            BatchIngester::new("ds".into(), "__time".into(), vec!["tags".into()], vec![]);
        let result = ingester.ingest(rows).expect("ingest");
        match result.segment_data.column("tags").expect("tags") {
            ColumnData::StringMulti(mc) => {
                assert_eq!(mc.row_values(0), vec!["a", "b"]);
                assert_eq!(mc.row_values(1), vec!["c"]);
            }
            other => panic!("expected StringMulti column, got {other:?}"),
        }

        // Druid's DEFAULT list delimiter (\u{1}) applies when the spec
        // declares none.
        let fmt = InputFormat::Tsv {
            columns: vec!["__time".into(), "tags".into()],
            skip_header_rows: 0,
            list_delimiter: None,
        };
        let rows = fmt
            .parse_bytes("1000\tx\u{1}y\n".as_bytes())
            .expect("parse");
        assert_eq!(rows[0]["tags"], serde_json::json!(["x", "y"]));
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
        assert!(err.to_string().contains("absent or null"), "err = {err}");
    }

    #[test]
    fn extract_timestamp_reads_only_configured_column() {
        // Druid reads ONLY timestampSpec.column. A row that lacks the configured
        // column but carries __time must DEAD-LETTER — not be published at
        // __time's instant via a fallback (Codex R17).
        let row = serde_json::json!({"__time": 1234, "other": "x"});
        assert!(
            extract_row_timestamp_millis(&row, "event_time").is_err(),
            "must not fall back to __time when the configured column is absent"
        );
        // The configured column is read when present.
        let row = serde_json::json!({"event_time": 5678, "__time": 1234});
        assert_eq!(
            extract_row_timestamp_millis(&row, "event_time").expect("configured column"),
            5678
        );
    }

    #[test]
    fn extract_timestamp_accepts_numeric_string_epoch() {
        // Druid "auto" accepts a numeric-string epoch-millis, not only ISO.
        let row = serde_json::json!({"__time": "1700000000000"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("numeric string"),
            1_700_000_000_000
        );
        // ISO still works.
        let row = serde_json::json!({"__time": "2023-11-14T22:13:20Z"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("iso"),
            1_700_000_000_000
        );
    }

    #[test]
    fn extract_timestamp_accepts_timezoneless_iso() {
        // Druid `auto`/`iso` accept timezone-less ISO-8601 timestamps and
        // interpret them in the default zone (UTC). chrono's `DateTime<Utc>`
        // parser REQUIRES an explicit offset, so these forms must be handled
        // via a naive-datetime fallback or every such row is dropped
        // (Codex R9, 2026-07-13).
        //
        // Timezone-less date-time (no `Z`, no offset) == the UTC instant.
        let row = serde_json::json!({"__time": "2023-11-14T22:13:20"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("tz-less datetime"),
            1_700_000_000_000
        );
        // ... with fractional seconds.
        let row = serde_json::json!({"__time": "2023-11-14T22:13:20.500"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("tz-less datetime frac"),
            1_700_000_000_500
        );
        // Date-only ISO → UTC midnight.
        let row = serde_json::json!({"__time": "2023-11-14"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("date-only"),
            1_699_920_000_000
        );
        // Druid `auto` also accepts a SPACE separator instead of 'T' (Codex
        // R11); it must resolve to the same instant as the 'T' form.
        let row = serde_json::json!({"__time": "2023-11-14 22:13:20"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("space-separated"),
            1_700_000_000_000
        );
        // ... including with an explicit offset after the space.
        let row = serde_json::json!({"__time": "2023-11-14 22:13:20Z"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("space + Z"),
            1_700_000_000_000
        );
        // A genuinely unparseable string is still rejected (not silently 0).
        let row = serde_json::json!({"__time": "not-a-timestamp"});
        assert!(extract_row_timestamp_millis(&row, "__time").is_err());
    }

    #[test]
    fn extract_timestamp_named_zone_stripped_not_shifted() {
        // Druid STRIPS a recognised trailing zone and reads the remaining wall
        // time as UTC — it does NOT shift the instant (a Joda `withZone`
        // quirk). The Codex clean-room behavioral oracle (2026-07-13) corrected
        // an earlier impl that wrongly shifted by a US offset. Read as UTC,
        // 2009-02-13T23:31:30 is the classic unix ts 1234567890.
        for ts in [
            "2009-02-13 23:31:30 PST",
            "2009-02-13 23:31:30 EST",
            "2009-02-13 23:31:30 UTC",
            "2009-02-13 23:31:30 GMT",
            "2009-02-13T23:31:30 PST", // 'T' separator too
        ] {
            let row = serde_json::json!({ "__time": ts });
            assert_eq!(
                extract_row_timestamp_millis(&row, "__time").expect(ts),
                1_234_567_890_000,
                "zone must be stripped WITHOUT shifting: {ts:?}"
            );
        }
        // Date-only + recognised zone → UTC midnight (NOT midnight in the zone).
        let row = serde_json::json!({"__time": "2009-02-13 PST"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("date-only PST"),
            1_234_483_200_000
        );
        // DAYLIGHT abbreviations are not standard-tzdata ids; an unknown token,
        // and (documented residual, Codex R15) a full IANA id or fictitious
        // `Area/Location` shape, are ALL dead-lettered — never stored wrong.
        for ts in [
            "2009-02-13T23:31:30 EDT",
            "2009-02-13 23:31:30 XYZ",
            "2009-02-13 23:31:30 America/Los_Angeles", // IANA: residual, dead-lettered
            "2009-02-13 23:31:30 Mars/Olympus",        // fictitious: no longer over-accepted
        ] {
            let row = serde_json::json!({ "__time": ts });
            assert!(
                extract_row_timestamp_millis(&row, "__time").is_err(),
                "unrecognised zone must be rejected: {ts:?}"
            );
        }
        // A leap second with a recognised zone is still rejected.
        let row = serde_json::json!({"__time": "2009-02-13 23:31:60 PST"});
        assert!(extract_row_timestamp_millis(&row, "__time").is_err());
    }

    #[test]
    fn extract_timestamp_r15_grammar_corrections() {
        // #3: a numeric-string epoch is ALL-DIGITS only under Druid `auto`. A
        // SIGNED string is not a millis epoch (Druid reads "-1" as ISO year -1),
        // and Rust's int parser would accept the sign and store a WRONG instant
        // (-1 ms). Restrict to unsigned digits and dead-letter signed forms.
        for bad in ["-1", "+1", "-1700000000000"] {
            let row = serde_json::json!({ "__time": bad });
            assert!(
                extract_row_timestamp_millis(&row, "__time").is_err(),
                "signed numeric string {bad:?} must dead-letter, not store -1 ms"
            );
        }
        // All-digit strings still parse as epoch-millis.
        let row = serde_json::json!({"__time": "2023"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("2023"),
            2023
        );

        // #1a: an hour-precision field must be all-digits; "+1" (accepted by
        // Rust's int parser) must NOT be read as hour 1 (Druid rejects it).
        let row = serde_json::json!({"__time": "2023-11-14T+1"});
        assert!(
            extract_row_timestamp_millis(&row, "__time").is_err(),
            "T+1 must dead-letter, not store 01:00"
        );
        // Genuine hour precision still works.
        let row = serde_json::json!({"__time": "2023-11-14T22"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("hour"),
            1_699_999_200_000
        );

        // #1b: year-month (YYYY-MM) → first of month, UTC midnight (Druid auto).
        let row = serde_json::json!({"__time": "2023-11"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("year-month"),
            1_698_796_800_000
        );

        // #R16: the millis path checks the RAW (un-trimmed) string, so a
        // whitespace-padded number (`" 2023 "`) is NOT read as 2023 ms (Druid
        // reads ISO year 2023; we dead-letter — the iso-year residual — rather
        // than store the wrong instant). A clean all-digit string still parses.
        let row = serde_json::json!({"__time": " 2023 "});
        assert!(
            extract_row_timestamp_millis(&row, "__time").is_err(),
            "whitespace-padded number must not store 2023 ms"
        );
        let row = serde_json::json!({"__time": "2023"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("clean 2023"),
            2023
        );
    }

    #[test]
    fn extract_timestamp_accepts_float_epoch_millis() {
        // A JSON floating-point number is epoch-millis, truncated toward zero
        // (Druid Number.longValue); Codex R14 oracle.
        for (row, expected) in [
            (serde_json::json!({"__time": 1.7e12}), 1_700_000_000_000_i64),
            (
                serde_json::json!({"__time": 1_700_000_000_000.0}),
                1_700_000_000_000,
            ),
            (
                serde_json::json!({"__time": 1_700_000_000_000.9}),
                1_700_000_000_000,
            ),
            (
                serde_json::json!({"__time": -1_700_000_000_000.9}),
                -1_700_000_000_000,
            ),
        ] {
            assert_eq!(
                extract_row_timestamp_millis(&row, "__time").expect("float epoch"),
                expected
            );
        }
        // A float STRING is NOT a millis epoch under auto (only all-digit is).
        let row = serde_json::json!({"__time": "1700000000000.0"});
        assert!(extract_row_timestamp_millis(&row, "__time").is_err());
    }

    #[test]
    fn extract_timestamp_accepts_reduced_precision() {
        // Druid `auto` accepts minute and hour precision, zero-filling omitted
        // fields (Codex R14 oracle). 22:13:20 UTC == 1_700_000_000_000.
        let cases = [
            ("2023-11-14T22:13", 1_699_999_980_000_i64), // minute → :00
            ("2023-11-14 22:13", 1_699_999_980_000),     // space + minute
            ("2023-11-14T22", 1_699_999_200_000),        // hour → :00:00
        ];
        for (ts, expected) in cases {
            let row = serde_json::json!({ "__time": ts });
            assert_eq!(
                extract_row_timestamp_millis(&row, "__time").expect(ts),
                expected,
                "reduced precision {ts:?}"
            );
        }
    }

    #[test]
    fn extract_timestamp_accepts_fractional_hour_and_minute() {
        // Joda's ISO time element lets a fraction follow the HOUR or the
        // MINUTE directly: 22.5 h == 22:30:00 and 22:13.5 == 22:13:30, with
        // `.` OR `,` as the fraction separator; sub-millisecond precision
        // truncates (Codex clean-room oracle, 2026-07-13:
        // "2023-11-14T22.5" → 1700001000000, "2023-11-14T22:13.5" →
        // 1700000010000).
        let cases = [
            ("2023-11-14T22.5", 1_700_001_000_000_i64), // fractional hour
            ("2023-11-14T22,5", 1_700_001_000_000),     // comma separator
            ("2023-11-14 22.5", 1_700_001_000_000),     // space-separated form
            ("2023-11-14T22:13.5", 1_700_000_010_000),  // fractional minute
            ("2023-11-14T22:13,5", 1_700_000_010_000),  // comma separator
            // Multi-digit fraction of a minute: 0.505 × 60_000 = 30_300 ms.
            ("2023-11-14T22:13.505", 1_700_000_010_300),
            // Sub-ms truncates: 0.5000001 × 3_600_000 = 1_800_000.36 → 1_800_000.
            ("2023-11-14T22.5000001", 1_700_001_000_000),
        ];
        for (ts, expected) in cases {
            let row = serde_json::json!({ "__time": ts });
            assert_eq!(
                extract_row_timestamp_millis(&row, "__time").expect(ts),
                expected,
                "fractional hour/minute {ts:?}"
            );
        }
        // An empty or non-digit fraction is not a fractional field — reject.
        for bad in ["2023-11-14T22.", "2023-11-14T22.5x", "2023-11-14T22:13."] {
            let row = serde_json::json!({ "__time": bad });
            assert!(
                extract_row_timestamp_millis(&row, "__time").is_err(),
                "malformed fraction {bad:?} must dead-letter"
            );
        }
    }

    #[test]
    fn extract_timestamp_accepts_trailing_t_date() {
        // The auto grammar permits a trailing `T` with no time fields and
        // defaults to UTC midnight (Codex clean-room oracle, 2026-07-13:
        // "2023-11-14T" → 1699920000000).
        let row = serde_json::json!({"__time": "2023-11-14T"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("trailing T"),
            1_699_920_000_000
        );
        // The prefix must still be a real calendar date.
        for bad in ["2023-02-29T", "2023-11-14TT", "T"] {
            let row = serde_json::json!({ "__time": bad });
            assert!(
                extract_row_timestamp_millis(&row, "__time").is_err(),
                "invalid trailing-T form {bad:?} must dead-letter"
            );
        }
    }

    #[test]
    fn extract_timestamp_reduced_precision_with_offset() {
        // Reduced-precision wall clock + explicit offset: chrono's full-
        // precision RFC3339 parse (step 2) rejects these, so the offset must
        // be stripped and SUBTRACTED from the wall time (Codex clean-room
        // oracle, 2026-07-13: "2023-11-14T22:13Z" → 1699999980000,
        // "2023-11-14T22Z" → 1699999200000).
        let cases = [
            ("2023-11-14T22:13Z", 1_699_999_980_000_i64),
            ("2023-11-14T22Z", 1_699_999_200_000),
            // +09:00 → wall time minus nine hours.
            ("2023-11-14T22:13+09:00", 1_699_967_580_000),
            // -08:00 → wall time plus eight hours.
            ("2023-11-14T22:13-08:00", 1_700_028_780_000),
        ];
        for (ts, expected) in cases {
            let row = serde_json::json!({ "__time": ts });
            assert_eq!(
                extract_row_timestamp_millis(&row, "__time").expect(ts),
                expected,
                "reduced precision + offset {ts:?}"
            );
        }
        // An offset directly after a value with NO time-of-day is rejected by
        // Druid's grammar (oracle: "2023-11-14Z" and "2023-11-14+09:00" both
        // REJECT); trailing-T + offset is conservatively dead-lettered too.
        // A colon-less offset (`-0800`) on reduced precision stays a
        // documented residual.
        for bad in [
            "2023-11-14Z",
            "2023-11-14+09:00",
            "2023-11-14TZ",
            "2023-11-14T22:13-0800", // colon-less offset: residual
        ] {
            let row = serde_json::json!({ "__time": bad });
            assert!(
                extract_row_timestamp_millis(&row, "__time").is_err(),
                "{bad:?} must dead-letter"
            );
        }
        // Leap-second rejection must hold on the offset paths too.
        let row = serde_json::json!({"__time": "2023-11-14T23:59:60Z"});
        assert!(extract_row_timestamp_millis(&row, "__time").is_err());
    }

    #[test]
    fn ts_format_from_spec_format_maps_and_rejects() {
        // The shared spec→TsFormat mapping (compat-9): the three
        // implemented grammars map case-insensitively…
        assert_eq!(TsFormat::from_spec_format("auto"), Ok(TsFormat::Auto));
        assert_eq!(TsFormat::from_spec_format("iso"), Ok(TsFormat::Iso));
        assert_eq!(TsFormat::from_spec_format("millis"), Ok(TsFormat::Millis));
        assert_eq!(TsFormat::from_spec_format("MILLIS"), Ok(TsFormat::Millis));
        assert_eq!(TsFormat::from_spec_format("Iso"), Ok(TsFormat::Iso));
        // …and everything else fails LOUDLY with the client-facing
        // message, never a silent `auto` fallback.
        for bad in ["posix", "nano", "ruby", "yyyy-MM-dd HH:mm:ss", ""] {
            let err = TsFormat::from_spec_format(bad).expect_err("unsupported format");
            assert!(
                err.contains("unsupported timestampSpec.format"),
                "message must name the field: {err}"
            );
        }
    }

    #[test]
    fn extract_timestamp_format_is_threaded() {
        // The declared timestampSpec.format genuinely changes semantics
        // (Codex oracle); un-threaded, a declared `iso` stored "2023" as
        // 2023 ms — a WRONG instant (Fable audit).
        use TsFormat::{Auto, Iso, Millis};
        let ext = |v: serde_json::Value, f: TsFormat| {
            extract_row_timestamp_millis_fmt(&serde_json::json!({ "__time": v }), "__time", f)
        };
        // iso: bare digits are ISO YEARS…
        assert_eq!(
            ext(serde_json::json!("2023"), Iso).expect("iso year"),
            1_672_531_200_000
        );
        assert_eq!(
            ext(serde_json::json!("-1"), Iso).expect("iso year -1"),
            -62_198_755_200_000
        );
        // …and ≥10-digit values (which Joda's year field rejects) fall back
        // to signed integer epoch-millis (oracle).
        assert_eq!(
            ext(serde_json::json!("1700000000000"), Iso).expect("iso millis fallback"),
            1_700_000_000_000
        );
        assert_eq!(
            ext(serde_json::json!("-1700000000000"), Iso).expect("iso signed fallback"),
            -1_700_000_000_000
        );
        // iso numbers: an integer is a year; floats are rejected (oracle).
        assert_eq!(
            ext(serde_json::json!(-1), Iso).expect("iso number year"),
            -62_198_755_200_000
        );
        assert!(ext(serde_json::json!(1.7e12), Iso).is_err());
        // iso is strict: space separator and named zones reject; 'T' forms,
        // date-only, and reduced-precision+offset are accepted.
        assert!(ext(serde_json::json!("2023-11-14 22:13:20"), Iso).is_err());
        assert!(ext(serde_json::json!("2023-11-14 22:13:20 PST"), Iso).is_err());
        assert_eq!(
            ext(serde_json::json!("2023-11-14T22:13:20"), Iso).expect("iso T"),
            1_700_000_000_000
        );
        assert_eq!(
            ext(serde_json::json!("2023-11-14"), Iso).expect("iso date"),
            1_699_920_000_000
        );
        assert_eq!(
            ext(serde_json::json!("2023-11-14T22:13Z"), Iso).expect("iso reduced+offset"),
            1_699_999_980_000
        );
        // millis: signed-integer strings and numbers only; ISO text rejects.
        assert_eq!(ext(serde_json::json!("-1"), Millis).expect("millis -1"), -1);
        assert_eq!(
            ext(serde_json::json!("2023"), Millis).expect("millis 2023"),
            2023
        );
        assert_eq!(
            ext(serde_json::json!(1.7e12), Millis).expect("millis float"),
            1_700_000_000_000
        );
        assert!(ext(serde_json::json!("2023-11-14"), Millis).is_err());
        // auto is unchanged: all-digit strings are epoch-millis.
        assert_eq!(ext(serde_json::json!("2023"), Auto).expect("auto"), 2023);
    }

    #[test]
    fn long_dim_value_class_classifies_null_over_and_in_range() {
        let row = serde_json::json!({
            "id": 42,
            "big": 9_007_199_254_740_993_i64, // 2^53 + 1
            "n": null
        });
        assert_eq!(long_dim_value_class(&row, "id"), Some(false)); // in range
        assert_eq!(long_dim_value_class(&row, "big"), Some(true)); // over ±2^53
        assert_eq!(long_dim_value_class(&row, "n"), None); // null
        assert_eq!(long_dim_value_class(&row, "absent"), None); // absent
    }

    #[test]
    fn fractional_time_scaling_is_exact_integer_math() {
        // Codex R19: f64 scaling lost 1 ms ("T22.29" -> …999). Exact integer
        // digits×unit/10^len must yield the exact decimal value.
        // 2023-11-14T22:00Z = 1_699_999_200_000; 0.29 h = 1_044_000 ms.
        let row = serde_json::json!({"__time": "2023-11-14T22.29"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("frac hour"),
            1_700_000_244_000
        );
        // 22:13 = 1_699_999_980_000; 0.0021 min = 126 ms exactly.
        let row = serde_json::json!({"__time": "2023-11-14T22:13.0021"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect("frac minute"),
            1_699_999_980_126
        );
    }

    #[test]
    fn plain_numeric_overflow_fails_closed_not_f64_rounded() {
        // Codex R19: a plain decimal/integer string whose integral part
        // overflows i64 must become NULL — the f64 fallback would round
        // "-9223372036854775809[.0]" to exactly i64::MIN and store it.
        for v in [
            "-9223372036854775809.0",
            "-9223372036854775809",
            "9223372036854775808",
            "9223372036854775808.5",
        ] {
            let row = serde_json::json!({ "id": v });
            assert_eq!(
                long_dim_value_class(&row, "id"),
                None,
                "overflowing plain numeric {v:?} must be NULL, not a rounded long"
            );
        }
        // The exact boundary values still convert.
        let row = serde_json::json!({"id": "-9223372036854775808"});
        assert_eq!(long_dim_value_class(&row, "id"), Some(true)); // i64::MIN, |v| > 2^53
        let row = serde_json::json!({"id": "9223372036854775807.9"});
        assert_eq!(long_dim_value_class(&row, "id"), Some(true)); // i64::MAX
    }

    #[test]
    fn long_dim_decimal_string_is_exact_not_f64_rounded() {
        // A plain-decimal STRING for a long dimension must convert exactly,
        // truncating toward zero — NOT via an f64 round-trip, which silently
        // rounds integral values beyond ±2^53 (e.g. "9007199254740993.0" →
        // …992: corrupted high-range IDs, Codex R18).
        let ingester = BatchIngester::with_schemas(
            "ds".into(),
            "__time".into(),
            vec![DimensionSchema::new("id", DimensionType::Long)],
            vec![],
        );
        let rows = vec![
            serde_json::json!({"__time": 1, "id": "9007199254740993.0"}), // 2^53+1, exact
            serde_json::json!({"__time": 2, "id": "1.5"}),                // trunc toward zero → 1
            serde_json::json!({"__time": 3, "id": "-2.5"}),               // trunc toward zero → -2
            serde_json::json!({"__time": 4, "id": "9223372036854775807.0"}), // i64::MAX exact
        ];
        let result = ingester.ingest(rows).expect("ingest");
        match result.segment_data.column("id").expect("id column") {
            ColumnData::Long(vals) => assert_eq!(
                vals,
                &[9_007_199_254_740_993, 1, -2, i64::MAX],
                "decimal strings must convert exactly (no f64 rounding)"
            ),
            other => panic!("expected Long column, got {other:?}"),
        }
        // The streaming pre-flight classifier must see the EXACT value too:
        // 2^53+1 as a decimal string is OVER the f64-exact range.
        let row = serde_json::json!({"id": "9007199254740993.0"});
        assert_eq!(long_dim_value_class(&row, "id"), Some(true));
    }

    #[test]
    fn extract_timestamp_rejects_leap_second() {
        // Druid (Joda) rejects a `:60` leap second. chrono ACCEPTS it, folding
        // it into a nanosecond value >= 1e9, and `timestamp_millis()` then
        // silently normalizes it to the next minute — storing a DIFFERENT
        // instant than the input. Both the offset-aware and naive branches
        // share this behavior, so both must dead-letter it (Codex R10).
        for ts in [
            "2023-11-14T22:13:60.500",  // naive branch
            "2023-11-14T22:13:60.500Z", // offset branch
            "2023-11-14T22:13:60",      // whole leap second, naive
            "2023-11-14 22:13:60.500",  // space-separated leap (Codex R11 path)
        ] {
            let row = serde_json::json!({ "__time": ts });
            let got = extract_row_timestamp_millis(&row, "__time");
            assert!(
                got.is_err(),
                "leap second {ts:?} must be rejected, got {got:?}"
            );
        }
        // The ordinary :59 second at the same sub-second is still accepted.
        let row = serde_json::json!({"__time": "2023-11-14T22:13:59.500"});
        assert_eq!(
            extract_row_timestamp_millis(&row, "__time").expect(":59 is valid"),
            1_700_000_039_500
        );
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

    /// compat-11 MV fail-loud: `rollup:true` with a multi-value (JSON
    /// array) dimension value must ERROR.  Pre-fix the array was
    /// scalarised into the rollup grouping key as JSON text
    /// (`["a","b"]` → `"[\"a\",\"b\"]"`), so a later groupBy returned ONE
    /// corrupt array-text group instead of exploded element groups.
    /// (`rollup:false` + MV stays fully working — `mv_druid_oracle.rs`.)
    #[test]
    fn rollup_multi_value_dimension_fails_loud() {
        let ingester = BatchIngester::new(
            "ds".into(),
            "__time".into(),
            vec!["tags".into()],
            vec![serde_json::json!({"type": "doubleSum", "name": "revenue"})],
        );
        let rows = vec![
            serde_json::json!({
                "__time": "2024-01-01T00:00:00Z",
                "tags": ["a", "b"],
                "revenue": 10.0
            }),
            serde_json::json!({
                "__time": "2024-01-01T01:00:00Z",
                "tags": "a",
                "revenue": 20.0
            }),
        ];
        let err = ingester
            .ingest_with_rollup(rows, "day")
            .expect_err("rollup over an MV dimension must fail loud");
        let msg = err.to_string();
        assert!(msg.contains("multi-value"), "{msg}");
        assert!(msg.contains("tags"), "{msg}");
        assert!(msg.contains("rollup"), "{msg}");
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
            list_delimiter: None,
        };
        let json = serde_json::to_string(&format).expect("ser");
        let parsed: InputFormat = serde_json::from_str(&json).expect("deser");
        match parsed {
            InputFormat::Csv {
                columns,
                delimiter,
                skip_header_rows,
                ..
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
            skip_header_rows: 0,
            list_delimiter: None,
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
            list_delimiter: None,
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

    // --- Re-audit fixes: skipHeaderRows / RFC 4180 / rollup emission ---

    /// Druid's wire spelling is camelCase `skipHeaderRows`; the enum-level
    /// `rename_all` renames only VARIANTS, so pre-fix this bound to
    /// nothing and the value silently defaulted to 0.
    #[test]
    fn input_format_binds_druid_camelcase_skip_header_rows() {
        let parsed: InputFormat = serde_json::from_str(
            r#"{"type":"csv","columns":["ts","page","value"],"skipHeaderRows":1}"#,
        )
        .expect("deser");
        match parsed {
            InputFormat::Csv {
                skip_header_rows, ..
            } => assert_eq!(skip_header_rows, 1),
            other => panic!("expected Csv, got {other:?}"),
        }
        // TSV takes the same Druid field (same silent-drop species).
        let parsed: InputFormat =
            serde_json::from_str(r#"{"type":"tsv","columns":["a"],"skipHeaderRows":2}"#)
                .expect("deser");
        match parsed {
            InputFormat::Tsv {
                skip_header_rows, ..
            } => assert_eq!(skip_header_rows, 2),
            other => panic!("expected Tsv, got {other:?}"),
        }
    }

    /// `skipHeaderRows: 1` must skip the header line — Druid ingests this
    /// file as ONE data row; pre-fix the header line was parsed as data
    /// (and its `ts` cell then failed timestamp extraction downstream).
    #[test]
    fn parse_bytes_csv_applies_skip_header_rows() {
        let fmt = InputFormat::Csv {
            columns: vec!["ts".into(), "page".into(), "value".into()],
            delimiter: ",".into(),
            skip_header_rows: 1,
            list_delimiter: None,
        };
        let rows = fmt
            .parse_bytes(b"ts,page,value\n1000,a,2\n")
            .expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["ts"], serde_json::json!(1000));
        assert_eq!(rows[0]["page"], serde_json::json!("a"));
    }

    /// RFC 4180 quoting exactly as Druid's `csv` inputFormat (opencsv)
    /// parses it: enclosing quotes are stripped, a quoted delimiter is
    /// literal text, and `""` is an escaped literal quote. Pre-fix
    /// `"tokyo"` kept its quote characters (silently divergent dimension
    /// values) and `"New York, NY"` mis-split and failed the row.
    #[test]
    fn parse_bytes_csv_rfc4180_quoting_matches_druid() {
        let fmt = InputFormat::Csv {
            columns: vec!["ts".into(), "city".into(), "val".into()],
            delimiter: ",".into(),
            skip_header_rows: 0,
            list_delimiter: None,
        };
        // Druid stores `tokyo`, NOT `"tokyo"`.
        let rows = fmt.parse_bytes(b"1000,\"tokyo\",2\n").expect("parse");
        assert_eq!(rows[0]["city"], serde_json::json!("tokyo"));

        // Druid ingests this as 3 fields; the quoted comma is data.
        let rows = fmt
            .parse_bytes(b"1000,\"New York, NY\",2\n")
            .expect("quoted delimiter");
        assert_eq!(rows[0]["city"], serde_json::json!("New York, NY"));
        assert_eq!(rows[0]["val"], serde_json::json!(2));

        // Druid unescapes `""` to a literal quote: `say "hi"`.
        let rows = fmt
            .parse_bytes(b"1000,\"say \"\"hi\"\"\",2\n")
            .expect("escaped quote");
        assert_eq!(rows[0]["city"], serde_json::json!("say \"hi\""));
    }

    /// Druid's `tsv` (delimited) inputFormat does a plain split with NO
    /// quote handling — quotes must stay literal on the TSV path.
    #[test]
    fn parse_bytes_tsv_keeps_quotes_literal() {
        let fmt = InputFormat::Tsv {
            columns: vec!["ts".into(), "city".into(), "val".into()],
            skip_header_rows: 0,
            list_delimiter: None,
        };
        let rows = fmt.parse_bytes(b"1000\t\"tokyo\"\t2\n").expect("parse");
        assert_eq!(rows[0]["city"], serde_json::json!("\"tokyo\""));
    }

    /// Allocation-routing pins (perf recovery, 2026-07-19): TSV and
    /// quote-free CSV lines must take the zero-copy BORROWED split path;
    /// only a CSV line actually containing `"` may run the allocating
    /// RFC 4180 state machine — and its values must stay RFC 4180-correct.
    #[test]
    fn split_line_routes_zero_copy_vs_rfc4180() {
        // (a) TSV: always borrowed, quotes stay literal (no RFC 4180).
        let cells = split_line("1000\t\"tokyo\"\t2", '\t', Quoting::None);
        assert!(
            cells.iter().all(|c| matches!(c, Cow::Borrowed(_))),
            "TSV cells must borrow from the line"
        );
        assert_eq!(cells, vec!["1000", "\"tokyo\"", "2"]);

        // (b) unquoted CSV: borrowed fast path, values identical to the
        // plain split (the no-alloc route).
        let line = "1000,tokyo,3.5,x y,";
        let cells = split_line(line, ',', Quoting::Rfc4180);
        assert!(
            cells.iter().all(|c| matches!(c, Cow::Borrowed(_))),
            "quote-free CSV cells must borrow from the line"
        );
        let plain: Vec<&str> = line.split(',').collect();
        assert_eq!(cells, plain);

        // The fast-path precondition itself: for a quote-free line the
        // RFC 4180 state machine and the plain split are byte-identical.
        let machine = split_rfc4180_line(line, ',');
        assert_eq!(machine, plain);

        // (c) quoted CSV: owned RFC 4180 path — enclosing quotes stripped,
        // `""` unescaped, quoted delimiter literal.
        let cells = split_line("1000,\"say \"\"hi\"\", NY\",2", ',', Quoting::Rfc4180);
        assert!(
            cells.iter().all(|c| matches!(c, Cow::Owned(_))),
            "a quoted CSV line routes through the owned state machine"
        );
        assert_eq!(cells, vec!["1000", "say \"hi\", NY", "2"]);
    }

    /// Cells keep their own whitespace (Druid/opencsv does not trim);
    /// pre-fix the whole line was `trim()`ed, silently altering the
    /// first/last cells.
    #[test]
    fn parse_bytes_csv_preserves_cell_whitespace() {
        let fmt = InputFormat::Csv {
            columns: vec!["a".into(), "b".into()],
            delimiter: ",".into(),
            skip_header_rows: 0,
            list_delimiter: None,
        };
        let rows = fmt.parse_bytes(b" x,y \n").expect("parse");
        assert_eq!(rows[0]["a"], serde_json::json!(" x"));
        assert_eq!(rows[0]["b"], serde_json::json!("y "));
    }

    /// Druid permits a metric whose `fieldName` names a dimension
    /// (dimensions ["price"] + doubleSum {name:"total", fieldName:"price"};
    /// only OUTPUT-name collisions are rejected). The rolled group must
    /// keep dimension `price` = "5" and store the sum 10 under `total` —
    /// pre-fix the rolled sum was re-emitted under the SOURCE fieldName,
    /// overwriting the dimension with "10".
    #[test]
    fn rollup_metric_source_field_naming_a_dimension_keeps_dimension_value() {
        let ingester = BatchIngester::new(
            "ds".into(),
            "__time".into(),
            vec!["price".into()],
            vec![serde_json::json!({
                "type": "doubleSum", "name": "total", "fieldName": "price"
            })],
        );
        let rows = vec![
            serde_json::json!({"__time": 1000, "price": 5}),
            serde_json::json!({"__time": 2000, "price": 5}),
        ];
        let seg = ingester
            .ingest_with_rollup(rows, "day")
            .expect("rollup")
            .segment_data;
        assert_eq!(seg.num_rows, 1, "one queryGranularity group");
        match seg.column("price") {
            Some(ColumnData::String(sc)) => {
                assert_eq!(sc.dictionary.len(), 1);
                assert_eq!(
                    sc.dictionary.get(0),
                    Some("5"),
                    "dimension value must survive rollup (Druid keeps \"5\")"
                );
            }
            other => panic!("expected String dimension, got {other:?}"),
        }
        match seg.column("total") {
            Some(ColumnData::Double(v)) => assert_eq!(v, &[10.0], "doubleSum(price)"),
            other => panic!("expected Double metric, got {other:?}"),
        }
    }

    /// Same clobber species for the timestamp: a metric may read the raw
    /// timestamp input column (`fieldName` == timestampSpec.column, legal
    /// in Druid). The rolled segment must keep the truncated bucket
    /// instant while the sum of the RAW timestamp values lands under the
    /// OUTPUT name — pre-fix the re-emitted sum overwrote the row time.
    #[test]
    fn rollup_metric_source_field_naming_timestamp_column_keeps_bucket_time() {
        let ingester = BatchIngester::new(
            "ds".into(),
            "t".into(),
            vec![],
            vec![serde_json::json!({
                "type": "doubleSum", "name": "tsum", "fieldName": "t"
            })],
        );
        let rows = vec![
            serde_json::json!({"t": 1000}),
            serde_json::json!({"t": 2000}),
        ];
        let seg = ingester.ingest_with_rollup(rows, "day").expect("rollup");
        assert_eq!(seg.num_rows, 1);
        assert_eq!(
            seg.segment_data.interval.start_millis, 0,
            "bucket instant 1970-01-01T00:00:00Z, not the metric sum"
        );
        match seg.segment_data.column("tsum") {
            Some(ColumnData::Double(v)) => assert_eq!(v, &[3000.0], "doubleSum(raw t)"),
            other => panic!("expected Double metric, got {other:?}"),
        }
    }

    /// The internal rollup round-trip must not re-parse the truncated
    /// numeric timestamp under the DECLARED format: with format=iso Druid
    /// still rolls 1970-01-01T00:02:xxZ into minute bucket 120000 ms —
    /// pre-fix the numeric 120000 was re-read as ISO YEAR 120000 AD.
    #[test]
    fn rollup_iso_format_keeps_truncated_millis_not_iso_years() {
        let ingester = BatchIngester::new(
            "ds".into(),
            "t".into(),
            vec![],
            vec![serde_json::json!({"type": "count", "name": "cnt"})],
        )
        .with_timestamp_format(TsFormat::Iso);
        let rows = vec![
            serde_json::json!({"t": "1970-01-01T00:02:05Z"}),
            serde_json::json!({"t": "1970-01-01T00:02:30Z"}),
        ];
        let seg = ingester.ingest_with_rollup(rows, "minute").expect("rollup");
        assert_eq!(seg.num_rows, 1, "one minute bucket");
        assert_eq!(
            seg.segment_data.interval.start_millis, 120_000,
            "truncated instant must stay 1970-01-01T00:02:00Z"
        );
    }
}
