// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Common types for FerroDruid.

use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Column types
// ---------------------------------------------------------------------------

/// Druid column value types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ColumnType {
    /// 64-bit signed integer.
    Long,
    /// 32-bit IEEE-754 float.
    Float,
    /// 64-bit IEEE-754 double.
    Double,
    /// UTF-8 string (dictionary-encoded in segments).
    String,
    /// Complex / opaque type identified by its name (e.g. `"hyperUnique"`).
    #[serde(untagged)]
    Complex(std::string::String),
}

// ---------------------------------------------------------------------------
// Granularity
// ---------------------------------------------------------------------------

/// Druid query granularity.
///
/// Simple granularities are serialized as their lowercase name.
/// `Duration` is serialized as an object with `type: "duration"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Granularity {
    /// No bucketing — every row is its own bucket.
    None,
    /// 1-second buckets.
    Second,
    /// 1-minute buckets.
    Minute,
    /// 5-minute buckets.
    #[serde(rename = "five_minute")]
    FiveMinute,
    /// 10-minute buckets.
    #[serde(rename = "ten_minute")]
    TenMinute,
    /// 15-minute buckets.
    #[serde(rename = "fifteen_minute")]
    FifteenMinute,
    /// 30-minute buckets.
    #[serde(rename = "thirty_minute")]
    ThirtyMinute,
    /// 1-hour buckets.
    Hour,
    /// 6-hour buckets.
    #[serde(rename = "six_hour")]
    SixHour,
    /// 1-day buckets.
    Day,
    /// 1-week buckets.
    Week,
    /// 1-month buckets.
    Month,
    /// 1-quarter (3-month) buckets.
    Quarter,
    /// 1-year buckets.
    Year,
    /// Fixed-duration buckets with an optional origin.
    #[serde(rename = "duration")]
    Duration {
        /// Duration of each bucket in milliseconds.
        period_ms: u64,
        /// Origin timestamp for bucket alignment.
        origin: DateTime<Utc>,
    },
}

// ---------------------------------------------------------------------------
// Interval
// ---------------------------------------------------------------------------

/// An ISO-8601 time interval `[start, end)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interval {
    /// Inclusive start of the interval.
    pub start: DateTime<Utc>,
    /// Exclusive end of the interval.
    pub end: DateTime<Utc>,
}

impl Interval {
    /// Returns `true` if `ts` falls within `[start, end)`.
    pub fn contains(&self, ts: &DateTime<Utc>) -> bool {
        *ts >= self.start && *ts < self.end
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}",
            self.start.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            self.end.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        )
    }
}

// ---------------------------------------------------------------------------
// Data source
// ---------------------------------------------------------------------------

/// Reference to a Druid data source.
///
/// Druid's native query API accepts two equivalent forms for `dataSource`:
/// the tagged-object form (`{"type":"table","name":"wiki"}`) and the
/// string shorthand (`"wiki"`, equivalent to a table).  Wave 47-D §2
/// closed the divergence where FerroDruid only accepted the tagged form
/// — the bare-string shape now also deserialises to
/// [`DataSource::Table`].  Serialisation always emits the tagged form
/// for stability.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DataSource {
    /// A regular table data source.
    #[serde(rename = "table")]
    Table {
        /// Name of the data source.
        name: String,
    },
    /// Union of multiple table data sources.
    #[serde(rename = "union")]
    Union {
        /// Names of the data sources to union.
        #[serde(rename = "dataSources")]
        data_sources: Vec<String>,
    },
    /// An inline sub-query.
    #[serde(rename = "query")]
    Query {
        /// The sub-query definition.
        query: Box<serde_json::Value>,
    },
    /// A lookup data source.
    #[serde(rename = "lookup")]
    Lookup {
        /// Name of the registered lookup.
        lookup: String,
    },
    /// Inline row data embedded in the query.
    #[serde(rename = "inline")]
    Inline {
        /// Column names for each row.
        #[serde(rename = "columnNames")]
        column_names: Vec<String>,
        /// Row values (each inner Vec has one value per column).
        rows: Vec<Vec<serde_json::Value>>,
    },
}

impl<'de> Deserialize<'de> for DataSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Same enum, but the tagged-object spelling — Druid `"type":"table"` form.
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "camelCase")]
        enum Tagged {
            #[serde(rename = "table")]
            Table { name: String },
            #[serde(rename = "union")]
            Union {
                #[serde(rename = "dataSources", alias = "data_sources")]
                data_sources: Vec<String>,
            },
            #[serde(rename = "query")]
            Query { query: Box<serde_json::Value> },
            #[serde(rename = "lookup")]
            Lookup { lookup: String },
            #[serde(rename = "inline")]
            Inline {
                #[serde(rename = "columnNames", alias = "column_names")]
                column_names: Vec<String>,
                rows: Vec<Vec<serde_json::Value>>,
            },
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            // String shorthand — Druid accepts `"dataSource":"wiki"` as
            // equivalent to `{"type":"table","name":"wiki"}`.
            Bare(String),
            Tagged(Tagged),
        }

        Ok(match Either::deserialize(deserializer)? {
            Either::Bare(name) => DataSource::Table { name },
            Either::Tagged(Tagged::Table { name }) => DataSource::Table { name },
            Either::Tagged(Tagged::Union { data_sources }) => DataSource::Union { data_sources },
            Either::Tagged(Tagged::Query { query }) => DataSource::Query { query },
            Either::Tagged(Tagged::Lookup { lookup }) => DataSource::Lookup { lookup },
            Either::Tagged(Tagged::Inline { column_names, rows }) => {
                DataSource::Inline { column_names, rows }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Segment identifier
// ---------------------------------------------------------------------------

/// Uniquely identifies a Druid segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentId {
    /// Data source name.
    pub data_source: String,
    /// Time interval covered by this segment.
    pub interval: Interval,
    /// Version string (typically an ISO-8601 timestamp).
    pub version: String,
    /// Partition number within the interval + version.
    pub partition_num: i32,
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}_{}_{}_{}",
            self.data_source, self.interval, self.version, self.partition_num,
        )
    }
}

// ---------------------------------------------------------------------------
// Dimension spec
// ---------------------------------------------------------------------------

/// Druid dimension specification for groupBy / topN queries.
///
/// `rename_all_fields = "camelCase"` is required because `tag = "type"`
/// suppresses the field-rename effect of `rename_all` on struct variants —
/// without it, `output_name` / `output_type` were emitted as snake_case
/// rather than Druid's canonical `outputName` / `outputType`. The
/// per-field `alias = "output_name"` etc. preserves backward compat with
/// existing fixtures that were written against the old snake_case
/// behaviour, so deserialization accepts both shapes.
///
/// Wave 47-D §3 closes the divergence where FerroDruid required the
/// verbose tagged form: Druid also accepts a bare-string shorthand
/// (`"page"`) for `Default { dimension: "page", output_name: "page",
/// output_type: STRING }` and an object that omits `outputName` /
/// `outputType` (defaulting to the dimension name and `STRING`).  Both
/// variants are now decoded; serialisation always emits the canonical
/// tagged form.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DimensionSpec {
    /// Default dimension — pass through with optional type coercion.
    #[serde(rename = "default")]
    Default {
        /// Input dimension name.
        dimension: String,
        /// Output column name.
        #[serde(alias = "output_name")]
        output_name: String,
        /// Output value type.
        #[serde(alias = "output_type")]
        output_type: ColumnType,
    },
    /// Extraction-function based dimension.
    #[serde(rename = "extraction")]
    Extraction {
        /// Input dimension name.
        dimension: String,
        /// Output column name.
        #[serde(alias = "output_name")]
        output_name: String,
        /// The extraction function to apply.
        #[serde(alias = "extraction_fn")]
        extraction_fn: ExtractionFunction,
    },
    /// List-filtered dimension (whitelist / blacklist).
    #[serde(rename = "listFiltered")]
    ListFiltered {
        /// Delegate dimension spec to filter.
        delegate: Box<DimensionSpec>,
        /// Allowed (or denied) values.
        values: Vec<String>,
        /// If `true`, `values` is a whitelist; otherwise a blacklist.
        is_whitelist: bool,
    },
    /// Regex-filtered dimension.
    #[serde(rename = "regexFiltered")]
    RegexFiltered {
        /// Delegate dimension spec to filter.
        delegate: Box<DimensionSpec>,
        /// Regular expression pattern.
        pattern: String,
    },
    /// Prefix-filtered dimension.
    #[serde(rename = "prefixFiltered")]
    PrefixFiltered {
        /// Delegate dimension spec to filter.
        delegate: Box<DimensionSpec>,
        /// Required prefix string.
        prefix: String,
    },
}

impl<'de> Deserialize<'de> for DimensionSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // The tagged-object spelling — Druid `"type":"default"` form.
        // `outputName` / `outputType` are optional in upstream Druid:
        // `outputName` defaults to `dimension` and `outputType` to
        // `STRING` when omitted (Wave 47-D §3).
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "camelCase")]
        enum Tagged {
            #[serde(rename = "default")]
            Default {
                dimension: String,
                #[serde(default, alias = "output_name")]
                output_name: Option<String>,
                #[serde(default, alias = "output_type")]
                output_type: Option<ColumnType>,
            },
            #[serde(rename = "extraction")]
            Extraction {
                dimension: String,
                #[serde(default, alias = "output_name")]
                output_name: Option<String>,
                #[serde(alias = "extraction_fn")]
                extraction_fn: ExtractionFunction,
            },
            #[serde(rename = "listFiltered")]
            ListFiltered {
                delegate: Box<DimensionSpec>,
                values: Vec<String>,
                is_whitelist: bool,
            },
            #[serde(rename = "regexFiltered")]
            RegexFiltered {
                delegate: Box<DimensionSpec>,
                pattern: String,
            },
            #[serde(rename = "prefixFiltered")]
            PrefixFiltered {
                delegate: Box<DimensionSpec>,
                prefix: String,
            },
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            // String shorthand — Druid accepts `["page"]` as
            // `[{"type":"default","dimension":"page","outputName":"page","outputType":"STRING"}]`.
            Bare(String),
            Tagged(Tagged),
        }

        Ok(match Either::deserialize(deserializer)? {
            Either::Bare(name) => DimensionSpec::Default {
                output_name: name.clone(),
                dimension: name,
                output_type: ColumnType::String,
            },
            Either::Tagged(Tagged::Default {
                dimension,
                output_name,
                output_type,
            }) => DimensionSpec::Default {
                output_name: output_name.unwrap_or_else(|| dimension.clone()),
                dimension,
                output_type: output_type.unwrap_or(ColumnType::String),
            },
            Either::Tagged(Tagged::Extraction {
                dimension,
                output_name,
                extraction_fn,
            }) => DimensionSpec::Extraction {
                output_name: output_name.unwrap_or_else(|| dimension.clone()),
                dimension,
                extraction_fn,
            },
            Either::Tagged(Tagged::ListFiltered {
                delegate,
                values,
                is_whitelist,
            }) => DimensionSpec::ListFiltered {
                delegate,
                values,
                is_whitelist,
            },
            Either::Tagged(Tagged::RegexFiltered { delegate, pattern }) => {
                DimensionSpec::RegexFiltered { delegate, pattern }
            }
            Either::Tagged(Tagged::PrefixFiltered { delegate, prefix }) => {
                DimensionSpec::PrefixFiltered { delegate, prefix }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Extraction functions
// ---------------------------------------------------------------------------

/// Druid extraction functions applied to dimension values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ExtractionFunction {
    /// Regex extraction.
    #[serde(rename = "regex")]
    Regex {
        /// Regular expression.
        expr: String,
        /// Capture group index. `None` selects the whole match (group 0)
        /// per the Wave 45-F query-engine close; a numeric value selects
        /// the corresponding numbered capture (`$N`).
        #[serde(default)]
        index: Option<usize>,
        /// Replace missing values with a substitute.
        #[serde(default)]
        replace_missing_value: bool,
        /// The substitute value when `replace_missing_value` is true.
        #[serde(default)]
        replace_missing_value_with: Option<String>,
        /// Optional named capture-group selector (FerroDruid extension —
        /// Wave 45-F).  When set this takes precedence over `index` and
        /// resolves to the regex `(?P<name>…)` group.
        #[serde(default, rename = "groupName")]
        group_name: Option<String>,
    },
    /// Partial regex match — retains only the matched portion.
    #[serde(rename = "partial")]
    Partial {
        /// Regular expression.
        expr: String,
    },
    /// Search-query based extraction.
    #[serde(rename = "searchQuery")]
    SearchQuery {
        /// The search query to apply.
        query: SearchQuerySpec,
    },
    /// String-length extraction — emits the length of the value.
    #[serde(rename = "strlen")]
    Strlen,
    /// Time-formatting extraction.
    #[serde(rename = "timeFormat")]
    TimeFormat {
        /// Output format string (Java SimpleDateFormat / Joda).
        #[serde(default)]
        format: Option<String>,
        /// Timezone for output.
        #[serde(default)]
        time_zone: Option<String>,
        /// Locale for output.
        #[serde(default)]
        locale: Option<String>,
        /// Granularity to bucket to before formatting.
        #[serde(default)]
        granularity: Option<Box<Granularity>>,
        /// If true, emit epoch millis instead of formatted string.
        #[serde(default)]
        as_millis: bool,
    },
    /// Legacy time-parsing extraction.
    #[serde(rename = "time")]
    Time {
        /// Input format.
        time_format: String,
        /// Output format.
        result_format: String,
    },
    /// Substring extraction.
    #[serde(rename = "substring")]
    Substring {
        /// Start index (0-based).
        index: usize,
        /// Maximum length (if `None`, to end of string).
        #[serde(default)]
        length: Option<usize>,
    },
    /// Convert to upper-case.
    #[serde(rename = "upper")]
    Upper {
        /// Optional locale for case conversion.
        #[serde(default)]
        locale: Option<String>,
    },
    /// Convert to lower-case.
    #[serde(rename = "lower")]
    Lower {
        /// Optional locale for case conversion.
        #[serde(default)]
        locale: Option<String>,
    },
    /// Numeric bucketing.
    #[serde(rename = "bucket")]
    Bucket {
        /// Bucket size.
        #[serde(default = "default_bucket_size")]
        size: f64,
        /// Bucket offset.
        #[serde(default)]
        offset: f64,
    },
    /// Cascading chain of extraction functions applied in order.
    #[serde(rename = "cascade")]
    Cascade {
        /// Ordered list of extraction functions.
        extraction_fns: Vec<ExtractionFunction>,
    },
    /// Printf-style string formatting.
    #[serde(rename = "stringFormat")]
    StringFormat {
        /// How to handle null values.
        null_handling: NullHandling,
        /// Format string (Java `String.format` style).
        format: String,
    },
    /// Map-based lookup extraction.
    #[serde(rename = "lookup")]
    Lookup {
        /// The lookup definition.
        lookup: LookupSpec,
        /// If true, retain the original value when no match is found.
        #[serde(default)]
        retain_missing_value: bool,
        /// Whether the mapping is injective (1-to-1).
        #[serde(default)]
        injective: bool,
        /// Replacement value when the key is missing and retain is false.
        #[serde(default)]
        replace_missing_value_with: Option<String>,
    },
    /// Registered (named) lookup extraction.
    #[serde(rename = "registeredLookup")]
    RegisteredLookup {
        /// Name of the registered lookup.
        lookup: String,
        /// If true, retain the original value when no match is found.
        #[serde(default)]
        retain_missing_value: bool,
    },
}

fn default_bucket_size() -> f64 {
    1.0
}

// ---------------------------------------------------------------------------
// Null handling
// ---------------------------------------------------------------------------

/// How null dimension values are treated in string-format extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NullHandling {
    /// Represent nulls as the literal string `"null"`.
    NullString,
    /// Represent nulls as an empty string.
    EmptyString,
    /// Propagate nulls (return null).
    ReturnNull,
}

// ---------------------------------------------------------------------------
// Search query spec
// ---------------------------------------------------------------------------

/// Search query specifications used in search-query extraction and search
/// queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SearchQuerySpec {
    /// Case-sensitive contains.
    #[serde(rename = "contains")]
    Contains {
        /// The value to search for.
        value: String,
    },
    /// Case-insensitive contains.
    #[serde(rename = "insensitive_contains")]
    InsensitiveContains {
        /// The value to search for.
        value: String,
    },
    /// Fragment-based search (all fragments must match).
    #[serde(rename = "fragment")]
    Fragment {
        /// Fragment strings that must all be present.
        values: Vec<String>,
        /// Whether the search is case-sensitive.
        #[serde(default)]
        case_sensitive: bool,
    },
    /// Regex-based search.
    #[serde(rename = "regex")]
    Regex {
        /// Regular expression pattern.
        pattern: String,
    },
}

// ---------------------------------------------------------------------------
// Lookup spec
// ---------------------------------------------------------------------------

/// A map-based lookup definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupSpec {
    /// Lookup type identifier (e.g. `"map"`).
    #[serde(rename = "type")]
    pub typ: String,
    /// Key → value mapping.
    pub map: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn interval_contains() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();
        let iv = Interval { start, end };

        assert!(iv.contains(&Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap()));
        assert!(iv.contains(&start));
        assert!(!iv.contains(&end)); // exclusive
        assert!(!iv.contains(&Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap()));
    }

    #[test]
    fn interval_display() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();
        let iv = Interval { start, end };
        let s = iv.to_string();
        assert!(s.contains('/'));
        assert!(s.starts_with("2024-01-01"));
    }

    #[test]
    fn segment_id_display() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();
        let sid = SegmentId {
            data_source: "wiki".into(),
            interval: Interval { start, end },
            version: "2024-01-01T00:00:00.000Z".into(),
            partition_num: 0,
        };
        let s = sid.to_string();
        assert!(s.starts_with("wiki_"));
    }

    #[test]
    fn datasource_table_json() {
        let ds = DataSource::Table {
            name: "wikipedia".into(),
        };
        let json = serde_json::to_string(&ds).expect("serialize");
        assert!(json.contains("\"type\":\"table\""));
        let back: DataSource = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, ds);
    }

    #[test]
    fn dimension_spec_default_json() {
        let dim = DimensionSpec::Default {
            dimension: "page".into(),
            output_name: "page".into(),
            output_type: ColumnType::String,
        };
        let json = serde_json::to_string(&dim).expect("serialize");
        assert!(json.contains("\"type\":\"default\""));
        let back: DimensionSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, dim);
    }

    /// Wire serialization MUST emit Druid's canonical camelCase field names —
    /// snake_case would break interop with real Druid clients and brokers.
    /// Deserialization keeps a snake_case alias so existing fixtures continue
    /// to load.
    #[test]
    fn dimension_spec_serializes_camelcase_and_accepts_snake_alias() {
        let dim = DimensionSpec::Default {
            dimension: "page".into(),
            output_name: "page_out".into(),
            output_type: ColumnType::String,
        };
        let json = serde_json::to_string(&dim).expect("serialize");
        assert!(
            json.contains("\"outputName\":\"page_out\""),
            "expected canonical Druid camelCase outputName, got: {json}"
        );
        assert!(
            json.contains("\"outputType\""),
            "expected canonical Druid camelCase outputType, got: {json}"
        );
        assert!(
            !json.contains("\"output_name\""),
            "snake_case output_name leaked into wire output: {json}"
        );

        // Backward compat: snake_case alias still parses.
        let snake =
            r#"{"type":"default","dimension":"x","output_name":"y","output_type":"STRING"}"#;
        let parsed: DimensionSpec = serde_json::from_str(snake).expect("snake alias");
        match parsed {
            DimensionSpec::Default {
                dimension,
                output_name,
                output_type,
            } => {
                assert_eq!(dimension, "x");
                assert_eq!(output_name, "y");
                assert_eq!(output_type, ColumnType::String);
            }
            other => panic!("expected Default variant, got {other:?}"),
        }
    }

    #[test]
    fn extraction_fn_regex_json() {
        let ef = ExtractionFunction::Regex {
            expr: "(\\w+)".into(),
            index: Some(1),
            replace_missing_value: false,
            replace_missing_value_with: None,
            group_name: None,
        };
        let json = serde_json::to_string(&ef).expect("serialize");
        assert!(json.contains("\"type\":\"regex\""));
        let back: ExtractionFunction = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, ef);
    }

    #[test]
    fn search_query_spec_json() {
        let sq = SearchQuerySpec::Contains {
            value: "hello".into(),
        };
        let json = serde_json::to_string(&sq).expect("serialize");
        assert!(json.contains("\"type\":\"contains\""));
    }

    #[test]
    fn null_handling_json() {
        let nh = NullHandling::EmptyString;
        let json = serde_json::to_string(&nh).expect("serialize");
        assert_eq!(json, "\"emptyString\"");
    }

    /// Wave 47-D §2: Druid accepts `"dataSource":"wikipedia"` as
    /// shorthand for `{"type":"table","name":"wikipedia"}`.  FerroDruid
    /// must parse both shapes and serialise to the canonical tagged form.
    #[test]
    fn datasource_accepts_string_shorthand() {
        let parsed: DataSource = serde_json::from_str("\"wikipedia\"").expect("bare string");
        assert_eq!(
            parsed,
            DataSource::Table {
                name: "wikipedia".into()
            }
        );

        // Round-trip through the canonical tagged form continues to work.
        let parsed_full: DataSource =
            serde_json::from_str(r#"{"type":"table","name":"wikipedia"}"#).expect("tagged");
        assert_eq!(parsed_full, parsed);
    }

    /// Wave 47-D §3: Druid accepts a bare-string dimension shorthand
    /// `"page"` as equivalent to `{"type":"default","dimension":"page",
    /// "outputName":"page","outputType":"STRING"}`.  Object form with
    /// missing `outputName` / `outputType` defaults the same way.
    #[test]
    fn dimension_spec_accepts_string_shorthand() {
        let parsed: DimensionSpec = serde_json::from_str("\"page\"").expect("bare string");
        assert_eq!(
            parsed,
            DimensionSpec::Default {
                dimension: "page".into(),
                output_name: "page".into(),
                output_type: ColumnType::String,
            }
        );

        // outputName / outputType omitted on the default object.
        let parsed_partial: DimensionSpec =
            serde_json::from_str(r#"{"type":"default","dimension":"page"}"#)
                .expect("partial default object");
        assert_eq!(parsed_partial, parsed);
    }

    /// Round-trip through `Vec<DimensionSpec>` so that
    /// `"dimensions":["a","b"]` decodes the way Druid emits it.
    #[test]
    fn dimension_spec_vec_accepts_string_shorthand_list() {
        let parsed: Vec<DimensionSpec> =
            serde_json::from_str(r#"["page","language"]"#).expect("string list");
        assert_eq!(parsed.len(), 2);
        match &parsed[0] {
            DimensionSpec::Default {
                dimension,
                output_name,
                output_type,
            } => {
                assert_eq!(dimension, "page");
                assert_eq!(output_name, "page");
                assert_eq!(*output_type, ColumnType::String);
            }
            other => panic!("expected Default, got {other:?}"),
        }
    }
}
