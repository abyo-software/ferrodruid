// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Kafka indexing service for FerroDruid.
//!
//! Provides Kafka supervisor spec parsing, lifecycle management, and
//! status reporting compatible with the Druid supervisor API.
//! Actual Kafka I/O requires the `kafka-io` feature flag (depends on `rdkafka`).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod checkpoint;
pub mod consumer;
pub mod eos_writer;
pub mod index_task;
pub mod partitions;
pub mod runtime;
pub mod source;
pub mod streaming;
pub mod supervisor;
pub mod topic_id;

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors from Kafka ingestion.
#[derive(Debug, Error)]
pub enum KafkaIngestError {
    /// Failed to connect to Kafka broker.
    #[error("kafka connection failed: {0}")]
    Connection(String),
    /// Deserialization error.
    #[error("kafka deserialization error: {0}")]
    Deserialization(String),
    /// Segment build error.
    #[error("segment build error: {0}")]
    SegmentBuild(String),
    /// Invalid supervisor state transition.
    #[error("invalid state transition: {0}")]
    InvalidState(String),
}

/// Kafka supervisor spec (Druid-compatible JSON format).
///
/// Wave 45-B closure of Wave 37B `ingest_kafka_tail` Medium #3
/// (Codex `lib.rs:40`): `#[serde(deny_unknown_fields)]` rejects
/// misspelled top-level keys (e.g. `iOConfig`, `tuningConfg`) so a
/// typo in an operator-supplied spec does not silently produce a
/// supervisor running with default values.
///
/// The overlord's `spawn_kafka_supervisor` strips real-Druid wrapper keys
/// (`id`, `suspended`, `context`, and the `{"spec": …}` envelope) BEFORE
/// deserializing into this struct, so this top-level strictness catches
/// typos without rejecting a genuine Druid supervisor POST body. The
/// nested [`KafkaIoConfig`] / [`KafkaTuningConfig`] are Druid-lenient
/// (they ignore unknown fields) because real Druid populates them with
/// many fields this crate does not model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KafkaSupervisorSpec {
    /// Spec type, must be `"kafka"`.
    #[serde(rename = "type")]
    pub spec_type: String,
    /// Data schema describing the target datasource and its columns.
    pub data_schema: DataSchema,
    /// Kafka I/O configuration.
    pub io_config: KafkaIoConfig,
    /// Optional tuning parameters.
    pub tuning_config: Option<KafkaTuningConfig>,
}

/// Maximum task replicas this supervisor will accept.
///
/// Wave 42-A: bound `replicas` and `task_count` to defend against
/// pathologically large `usize` values that downstream code treats as
/// loop bounds or pre-allocations. The cap is generous (1024) so
/// real production fan-outs are unaffected.
const MAX_TASK_REPLICAS: usize = 1024;
/// Maximum tuning row counts this supervisor will accept (1 billion).
const MAX_ROWS: usize = 1_000_000_000;

/// Metric aggregator `type`s streaming ingestion materialises correctly.
/// The ingester stores every numeric metric as a **DOUBLE** column, so only
/// `count` and the `double*` aggregators are store/query type-consistent;
/// `long*` / `float*` aggregators would read the DOUBLE column and return
/// null on the query side (Codex R7). They are rejected at validation —
/// declare numeric streaming metrics as `doubleSum`/`doubleMin`/`doubleMax`.
const SUPPORTED_METRIC_TYPES: &[&str] = &["count", "doubleSum", "doubleMin", "doubleMax"];

impl KafkaSupervisorSpec {
    /// Validate the spec contents against semantic rules that serde
    /// alone cannot enforce.
    ///
    /// Closes Wave 37B `ingest_kafka_tail` Mediums:
    ///   * **Medium #1** — `spec_type` must equal `"kafka"`. Pre-fix
    ///     any string was accepted and echoed back via `status()`.
    ///   * **Medium #4** — numeric tuning/task fields must be in a
    ///     reasonable, non-zero range. Pre-fix `0` and pathologically
    ///     large `usize` values were accepted, producing inert
    ///     supervisors or downstream allocation hazards.
    ///
    /// Returns `Ok(())` when the spec is acceptable; otherwise a
    /// [`KafkaIngestError::Deserialization`] describing the offence.
    pub fn validate(&self) -> Result<(), KafkaIngestError> {
        if self.spec_type != "kafka" {
            return Err(KafkaIngestError::Deserialization(format!(
                "supervisor spec.type must be \"kafka\", got {:?}",
                self.spec_type
            )));
        }
        if self.data_schema.data_source.trim().is_empty() {
            return Err(KafkaIngestError::Deserialization(
                "dataSchema.dataSource must not be empty".to_owned(),
            ));
        }
        if self.io_config.topic.trim().is_empty() {
            return Err(KafkaIngestError::Deserialization(
                "ioConfig.topic must not be empty".to_owned(),
            ));
        }
        // `^`-prefixed topic names are REGEX subscriptions to librdkafka
        // (Codex R28): `rd_kafka_subscribe` matches any `^`-prefixed name
        // against the cluster's full topic list and subscribes to EVERY
        // match. Druid's `ioConfig.topic` is a literal name, so passing the
        // value through unchecked would (a) ingest multiple topics into one
        // datasource, (b) defeat the string-based (datasource, topic)
        // pair-uniqueness guard (a literal `orders-prod` supervisor beside
        // `^orders-.*` double-ingests), and (c) break topic-stamped
        // provenance and replay-cleanup semantics, which assume one literal
        // topic. Reject loudly instead.
        if self.io_config.topic.starts_with('^') {
            return Err(KafkaIngestError::Deserialization(format!(
                "ioConfig.topic {:?} starts with '^', which librdkafka's subscribe \
                 interprets as a REGEX pattern over all cluster topics — incompatible \
                 with Druid's literal `topic` semantics. Regex multi-topic ingestion \
                 (Druid's `topicPattern`) is not supported; use a literal topic name",
                self.io_config.topic
            )));
        }
        // Kafka topic-name GRAMMAR (Codex R39): the literal `topic` is handed
        // straight to `rd_kafka_subscribe`, which only special-cases the
        // `^`-regex prefix (rejected above) — a syntactically INVALID name
        // (leading/trailing/internal space, control char, disallowed
        // punctuation, over-long, or the reserved `.`/`..`) sails through both
        // validate() and subscribe(), and the broker then answers the
        // subscription's metadata request with a PERMANENT
        // `INVALID_TOPIC_EXCEPTION` (protocol error 17,
        // `RDKafkaErrorCode::InvalidTopic`). librdkafka retries that forever,
        // so the supervisor is ACKNOWLEDGED yet ingests NOTHING until the
        // records expire at Kafka retention — and an earliest re-create would
        // first drop the pair's prior rows trusting a replay that never runs
        // (permanent loss). Enforce Apache Kafka's own topic-name rules
        // (org.apache.kafka.common.internals.Topic.validate): non-empty, at
        // most 249 characters, only `[A-Za-z0-9._-]`, and not the reserved
        // `.` / `..`. (`streaming::consume_error_is_fatal` additionally
        // classifies a broker-side `InvalidTopic` fatal, so a name this grammar
        // accepts but a stricter broker policy rejects still fails loud rather
        // than idling.)
        const MAX_KAFKA_TOPIC_LEN: usize = 249;
        let topic = self.io_config.topic.as_str();
        if topic == "." || topic == ".." {
            return Err(KafkaIngestError::Deserialization(format!(
                "ioConfig.topic {topic:?} is a reserved name — \".\" and \"..\" are not \
                 legal Kafka topic names"
            )));
        }
        if let Some(bad) = topic
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
        {
            return Err(KafkaIngestError::Deserialization(format!(
                "ioConfig.topic {topic:?} contains an illegal character {bad:?}: Kafka \
                 topic names allow only ASCII letters, digits, '.', '_' and '-' \
                 (no spaces or control characters)"
            )));
        }
        let char_len = topic.chars().count();
        if char_len > MAX_KAFKA_TOPIC_LEN {
            return Err(KafkaIngestError::Deserialization(format!(
                "ioConfig.topic is {char_len} characters, exceeding Kafka's maximum \
                 topic-name length of {MAX_KAFKA_TOPIC_LEN}"
            )));
        }
        // Druid's `topicPattern` (regex multi-topic, Druid 28+) is not
        // supported: reject it loudly rather than let the Druid-lenient
        // ioConfig silently ignore it (Codex R28) — silent ignoring would
        // leave the operator believing the regex subscription is live while
        // the consumer reads only the literal `topic`.
        if self.io_config.topic_pattern.is_some() {
            return Err(KafkaIngestError::Deserialization(
                "ioConfig.topicPattern (regex multi-topic ingestion) is not supported \
                 by streaming ingestion; supply a single literal `topic` instead"
                    .to_owned(),
            ));
        }
        // bootstrap.servers is REQUIRED (Codex R7): without it the runtime
        // would fall back to `localhost:9092` and silently ingest from a
        // random local broker instead of the intended cluster.
        if !self
            .io_config
            .consumer_properties
            .get("bootstrap.servers")
            .is_some_and(|v| !v.trim().is_empty())
        {
            return Err(KafkaIngestError::Deserialization(
                "ioConfig.consumerProperties must set a non-empty \"bootstrap.servers\"".to_owned(),
            ));
        }
        // `topic.blacklist` is a librdkafka regex PATTERN LIST that hides
        // matching topics from broker metadata "as if the topics did not
        // exist" (librdkafka 2.12.1 rdkafka_conf.c / CONFIGURATION.md). A
        // supervisor subscribes to exactly ONE literal topic, so the key has
        // no legitimate use here — and a pattern matching that own topic
        // silently blanks the subscription (zero assigned partitions).
        // Worse than a visibly idle supervisor (Codex R29): an earliest
        // RE-CREATE first deletes the pair's prior rows trusting the replay
        // to rebuild them, and a blanked replay rebuilds NOTHING — permanent
        // loss. Incompatible with the replay-rebuild guarantee; reject
        // loudly.
        if self
            .io_config
            .consumer_properties
            .contains_key("topic.blacklist")
        {
            return Err(KafkaIngestError::Deserialization(
                "ioConfig.consumerProperties \"topic.blacklist\" is not supported: this \
                 supervisor subscribes to a single literal topic, and a blacklist pattern \
                 matching that topic would silently blank the subscription — an earliest \
                 re-create would then delete prior rows that its replay can never rebuild"
                    .to_owned(),
            ));
        }
        // The ingester honours only numeric-epoch-millis and ISO-8601
        // timestamps; a declared `posix` (seconds) / `nano` / custom
        // format would be SILENTLY mis-interpreted as millis (e.g. a
        // 2023 posix value stored in 1970). Reject formats we do not
        // implement instead of storing wrong timestamps.
        let ts_format = self.data_schema.timestamp_spec.format.to_ascii_lowercase();
        if !matches!(ts_format.as_str(), "auto" | "iso" | "millis") {
            return Err(KafkaIngestError::Deserialization(format!(
                "dataSchema.timestampSpec.format {:?} is not supported \
                 (only \"auto\", \"iso\", \"millis\"); \"posix\"/\"nano\"/custom \
                 would be mis-interpreted as milliseconds",
                self.data_schema.timestamp_spec.format
            )));
        }
        // Rollup + queryGranularity: streaming ingests each flushed batch
        // RAW — no pre-aggregation and no timestamp truncation. Druid
        // DEFAULTS `rollup` to true and truncates timestamps to
        // `queryGranularity`, so a spec relying on those defaults would be
        // silently mis-ingested. Require BOTH to be explicit and disabling:
        // `rollup:false` and `queryGranularity:"NONE"` (Codex R7).
        //
        // An ABSENT granularitySpec is REJECTED (Codex R14 oracle): Druid
        // constructs a default uniform spec with rollup=TRUE when the whole
        // field is missing, which streaming cannot honour — so we require it to
        // be present and explicit rather than silently ingesting raw where
        // Druid would roll up.
        let Some(g) = &self.data_schema.granularity_spec else {
            return Err(KafkaIngestError::Deserialization(
                "dataSchema.granularitySpec is required for streaming ingestion: Druid \
                 defaults rollup=true when it is absent, but streaming ingests raw — supply \
                 an explicit granularitySpec with rollup=false and queryGranularity=NONE"
                    .to_owned(),
            ));
        };
        // Only "uniform" is supported for streaming (Druid's default;
        // "arbitrary" needs explicit intervals). Fail loud on anything
        // else rather than silently mis-bucketing (Codex R12).
        if !g.spec_type.eq_ignore_ascii_case("uniform") {
            return Err(KafkaIngestError::Deserialization(format!(
                "dataSchema.granularitySpec.type {:?} is not supported by streaming \
                 ingestion (only \"uniform\")",
                g.spec_type
            )));
        }
        if g.rollup != Some(false) {
            return Err(KafkaIngestError::Deserialization(
                "dataSchema.granularitySpec.rollup must be explicitly false for streaming \
                 ingestion (it ingests raw; Druid defaults rollup to true)"
                    .to_owned(),
            ));
        }
        if !g.query_granularity.eq_ignore_ascii_case("none") {
            return Err(KafkaIngestError::Deserialization(format!(
                "dataSchema.granularitySpec.queryGranularity {:?} is not supported by \
                 streaming ingestion (only \"NONE\"; it does not truncate timestamps)",
                g.query_granularity
            )));
        }
        // Dimensions: streaming materialises only the EXPLICITLY-listed
        // dimensions. Reject schema auto-discovery / an empty list, which
        // would silently drop every un-listed column.
        if self.data_schema.dimensions_spec.use_schema_discovery == Some(true) {
            return Err(KafkaIngestError::Deserialization(
                "dataSchema.dimensionsSpec.useSchemaDiscovery is not supported \
                 (list dimensions explicitly)"
                    .to_owned(),
            ));
        }
        if self.data_schema.dimensions_spec.dimensions.is_empty() {
            return Err(KafkaIngestError::Deserialization(
                "dataSchema.dimensionsSpec.dimensions must be a non-empty explicit \
                 list (schema auto-discovery is not supported)"
                    .to_owned(),
            ));
        }
        // Dimension types: only string/long/float/double are materialised
        // correctly. A `json`/`array`/complex dimension would be silently
        // stored as its stringified form — reject unsupported types.
        for d in &self.data_schema.dimensions_spec.dimensions {
            if let DimensionEntry::Typed { name, dim_type } = d
                && !matches!(dim_type.as_str(), "string" | "long" | "float" | "double")
            {
                return Err(KafkaIngestError::Deserialization(format!(
                    "dataSchema.dimensionsSpec dimension {name:?} has unsupported type \
                     {dim_type:?} (only string/long/float/double)"
                )));
            }
        }
        // transformSpec (filters / derived columns) is silently dropped by
        // lenient `DataSchema` deserialization and NOT applied by streaming
        // ingestion — reject it rather than ingest unfiltered/untransformed.
        if self.data_schema.transform_spec.is_some() {
            return Err(KafkaIngestError::Deserialization(
                "dataSchema.transformSpec is not supported by streaming ingestion".to_owned(),
            ));
        }
        // Input format: the consumer parses only the raw JSON object. Reject
        // a non-JSON format AND any non-trivial JSON option (e.g.
        // `flattenSpec`), which would silently reshape/skip records if
        // honoured — accept only a bare `{"type":"json"}`.
        if let Some(fmt) = &self.io_config.input_format {
            let t = fmt.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if !t.eq_ignore_ascii_case("json") {
                return Err(KafkaIngestError::Deserialization(format!(
                    "ioConfig.inputFormat.type {t:?} is not supported (only \"json\")"
                )));
            }
            if let Some(obj) = fmt.as_object()
                && obj.keys().any(|k| k != "type")
            {
                return Err(KafkaIngestError::Deserialization(
                    "ioConfig.inputFormat options beyond {\"type\":\"json\"} \
                     (e.g. flattenSpec) are not supported"
                        .to_owned(),
                ));
            }
        }
        // metricsSpec: only numeric sum/min/max (+ count) aggregators are
        // materialised, each needing a non-empty `name` (and, for non-count,
        // a `fieldName`). Anything else (theta/HLL sketches, string
        // first/last, complex) would be silently coerced to a garbage DOUBLE
        // or (on a malformed `name`) omitted entirely — reject it.
        // (Caveat, documented: `longSum`/`longMin`/`longMax` are stored as
        // DOUBLE, so magnitudes above 2^53 lose integer precision — a
        // pre-existing batch-ingest limitation shared with `index_parallel`.)
        for m in &self.data_schema.metrics_spec {
            let t = m.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if !SUPPORTED_METRIC_TYPES.contains(&t) {
                return Err(KafkaIngestError::Deserialization(format!(
                    "dataSchema.metricsSpec type {t:?} is not supported by streaming \
                     ingestion (supported: {SUPPORTED_METRIC_TYPES:?})"
                )));
            }
            let name = m.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if name.trim().is_empty() {
                return Err(KafkaIngestError::Deserialization(format!(
                    "dataSchema.metricsSpec entry of type {t:?} needs a non-empty string `name`"
                )));
            }
            if t != "count"
                && !m
                    .get("fieldName")
                    .and_then(|v| v.as_str())
                    .is_some_and(|f| !f.trim().is_empty())
            {
                return Err(KafkaIngestError::Deserialization(format!(
                    "dataSchema.metricsSpec {name:?} (type {t:?}) needs a non-empty string `fieldName`"
                )));
            }
        }
        // Column-name collisions (Codex R7): a dimension and metric sharing a
        // name (or either named the reserved `__time`) would silently
        // overwrite a column / the canonical timestamp. Reject duplicates
        // within and across dimensions/metrics, and any use of `__time`.
        {
            let mut seen = std::collections::HashSet::new();
            let dim_names = self
                .data_schema
                .dimensions_spec
                .dimensions
                .iter()
                .map(|d| match d {
                    DimensionEntry::String(s) => s.as_str(),
                    DimensionEntry::Typed { name, .. } => name.as_str(),
                });
            let metric_names = self
                .data_schema
                .metrics_spec
                .iter()
                .filter_map(|m| m.get("name").and_then(|v| v.as_str()));
            for name in dim_names.chain(metric_names) {
                if name == "__time" {
                    return Err(KafkaIngestError::Deserialization(
                        "a dimension/metric may not be named the reserved \"__time\"".to_owned(),
                    ));
                }
                if !seen.insert(name) {
                    return Err(KafkaIngestError::Deserialization(format!(
                        "duplicate column name {name:?} across dimensions/metrics"
                    )));
                }
            }
        }
        if let Some(n) = self.io_config.task_count
            && (n == 0 || n > MAX_TASK_REPLICAS)
        {
            return Err(KafkaIngestError::Deserialization(format!(
                "ioConfig.taskCount must be in 1..={MAX_TASK_REPLICAS}, got {n}"
            )));
        }
        if let Some(n) = self.io_config.replicas
            && (n == 0 || n > MAX_TASK_REPLICAS)
        {
            return Err(KafkaIngestError::Deserialization(format!(
                "ioConfig.replicas must be in 1..={MAX_TASK_REPLICAS}, got {n}"
            )));
        }
        if let Some(tc) = &self.tuning_config {
            if let Some(n) = tc.max_rows_in_memory
                && (n == 0 || n > MAX_ROWS)
            {
                return Err(KafkaIngestError::Deserialization(format!(
                    "tuningConfig.maxRowsInMemory must be in 1..={MAX_ROWS}, got {n}"
                )));
            }
            if let Some(n) = tc.max_rows_per_segment
                && (n == 0 || n > MAX_ROWS)
            {
                return Err(KafkaIngestError::Deserialization(format!(
                    "tuningConfig.maxRowsPerSegment must be in 1..={MAX_ROWS}, got {n}"
                )));
            }
            if let Some(n) = tc.max_total_rows
                && n > MAX_ROWS
            {
                return Err(KafkaIngestError::Deserialization(format!(
                    "tuningConfig.maxTotalRows must be <= {MAX_ROWS}, got {n}"
                )));
            }
        }
        Ok(())
    }
}

/// Data schema for an ingestion spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
// deny_unknown_fields (Codex R7): any unmodeled data-shaping field
// (e.g. `transformSpec` beyond the captured one) is rejected loudly rather
// than silently dropped — streaming ingestion supports only the fields below.
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataSchema {
    /// Target datasource name.
    pub data_source: String,
    /// How to extract the primary timestamp.
    pub timestamp_spec: TimestampSpec,
    /// Dimension columns.
    pub dimensions_spec: DimensionsSpec,
    /// Metric aggregation specs.
    #[serde(default)]
    pub metrics_spec: Vec<serde_json::Value>,
    /// Segment granularity configuration.
    pub granularity_spec: Option<GranularitySpec>,
    /// Row transform / filter spec. Captured so validation can REJECT it:
    /// streaming ingestion applies no transforms/filters, so accepting a
    /// `transformSpec` would silently ingest unfiltered/untransformed rows.
    #[serde(default)]
    pub transform_spec: Option<serde_json::Value>,
}

/// Timestamp extraction configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
// deny_unknown_fields (Codex R7): rejects unmodeled timestamp options
// (e.g. `missingValue`) that streaming would otherwise ignore.
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimestampSpec {
    /// Column name containing the timestamp.
    pub column: String,
    /// Timestamp format string (default `"auto"`).
    #[serde(default = "default_ts_format")]
    pub format: String,
}

fn default_ts_format() -> String {
    "auto".to_owned()
}

/// Dimension columns specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
// deny_unknown_fields (Codex R7): rejects unmodeled dimension options
// (e.g. `includeAllDimensions`, `spatialDimensions`) that would silently
// drop columns if honoured.
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DimensionsSpec {
    /// List of dimension entries.
    pub dimensions: Vec<DimensionEntry>,
    /// Columns to exclude from auto-detection.
    #[serde(default)]
    pub dimension_exclusions: Vec<String>,
    /// Druid schema-auto-discovery flag. Captured so validation can REJECT
    /// it: streaming ingestion materialises only the EXPLICITLY-listed
    /// dimensions, so `useSchemaDiscovery:true` (or an empty `dimensions`)
    /// would silently drop every discovered column.
    #[serde(default)]
    pub use_schema_discovery: Option<bool>,
}

/// A dimension can be a simple string name or a typed descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DimensionEntry {
    /// Dimension specified as a plain column name (string type implied).
    String(String),
    /// Dimension with explicit type information.
    ///
    /// NOTE: serde does not support `deny_unknown_fields` on an untagged
    /// enum variant, so unmodeled per-dimension options
    /// (`multiValueHandling`, `maxStringLength`, `createBitmapIndex`, …) are
    /// IGNORED rather than rejected here. Documented residual: streaming
    /// materialises string/long/float/double scalar dimensions only; array
    /// (multi-value) dimension values are stored as their scalar JSON text.
    Typed {
        /// Column name.
        name: String,
        /// Dimension type (e.g. "string", "long", "double").
        #[serde(rename = "type")]
        dim_type: String,
    },
}

/// Segment granularity configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
// deny_unknown_fields (Codex R7): rejects unmodeled granularity options.
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GranularitySpec {
    /// Granularity spec type (Druid default `"uniform"`).
    ///
    /// Druid makes `type` OPTIONAL and defaults it to `"uniform"`, so a
    /// standard granularitySpec that omits it (just `segmentGranularity` /
    /// `queryGranularity` / `rollup`) MUST still parse — otherwise a valid
    /// supervisor is rejected and, on a feature transition, a previously
    /// persisted spec is skipped and consumes zero records (Codex R12).
    #[serde(
        rename = "type",
        default = "default_granularity_type",
        deserialize_with = "de_granularity_type"
    )]
    pub spec_type: String,
    /// Segment granularity (e.g. "DAY", "HOUR"). Druid defaults this to "day"
    /// when omitted OR explicitly null (Codex R14/R15 oracle), so a spec that
    /// supplies only `rollup` — or an explicit `null` — must still parse.
    #[serde(
        default = "default_segment_granularity",
        deserialize_with = "de_segment_granularity"
    )]
    pub segment_granularity: String,
    /// Query granularity (e.g. "MINUTE", "NONE"). Druid defaults this to "none"
    /// when omitted OR explicitly null — which is exactly what streaming
    /// requires, so a spec omitting it (or passing null) validates.
    #[serde(
        default = "default_query_granularity",
        deserialize_with = "de_query_granularity"
    )]
    pub query_granularity: String,
    /// Whether to roll up rows with identical dimensions and timestamp.
    pub rollup: Option<bool>,
}

/// Druid's default `granularitySpec.type` when the field is omitted.
fn default_granularity_type() -> String {
    "uniform".to_owned()
}

/// Deserialize a granularity field, mapping an EXPLICIT JSON `null` to the
/// Druid default (serde's `default` only covers a MISSING field, not `null`;
/// Druid resolves a null field to its default — Codex R15).
fn de_granularity_type<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Ok(Option::<String>::deserialize(d)?.unwrap_or_else(default_granularity_type))
}

/// See [`de_granularity_type`]: null → `segmentGranularity` default.
fn de_segment_granularity<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Ok(Option::<String>::deserialize(d)?.unwrap_or_else(default_segment_granularity))
}

/// See [`de_granularity_type`]: null → `queryGranularity` default.
fn de_query_granularity<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Ok(Option::<String>::deserialize(d)?.unwrap_or_else(default_query_granularity))
}

/// Druid's default `granularitySpec.segmentGranularity` when omitted.
fn default_segment_granularity() -> String {
    "DAY".to_owned()
}

/// Druid's default `granularitySpec.queryGranularity` when omitted.
fn default_query_granularity() -> String {
    "NONE".to_owned()
}

/// Kafka consumer I/O configuration.
///
/// Real Druid supervisor `ioConfig` objects carry additional fields this
/// struct does not model (`type: "kafka"`, `inputFormat`, `pollTimeout`,
/// `startDelay`, `period`, …). To accept a genuine Druid Kafka supervisor
/// POST body — the whole point of the wired streaming path — unknown
/// fields are IGNORED (Druid-lenient) rather than rejected. Typos in the
/// fields we DO require (`topic`, `consumerProperties`) still fail loudly
/// as missing-required-field errors. (This relaxes the earlier Wave 45-B
/// `deny_unknown_fields` hardening, which rejected every real Druid spec;
/// see `spawn_kafka_supervisor` in ferrodruid-overlord.)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaIoConfig {
    /// Kafka topic to consume from. A LITERAL topic name, exactly like
    /// Druid's `ioConfig.topic` — [`KafkaSupervisorSpec::validate`] REJECTS
    /// a `^`-prefixed value because librdkafka's `subscribe` regex-matches
    /// any `^`-prefixed name against the cluster's full topic list
    /// (librdkafka 2.12.1, `rd_kafka_subscribe`), which would silently turn
    /// one supervisor into a multi-topic subscription (Codex R28).
    pub topic: String,
    /// Druid's regex multi-topic field (Druid 28+), mutually exclusive with
    /// `topic` upstream. Captured so [`KafkaSupervisorSpec::validate`] can
    /// REJECT it: streaming ingestion subscribes to a single literal topic,
    /// and the Druid-lenient `ioConfig` would otherwise silently DROP the
    /// pattern — the operator would believe a regex subscription is live
    /// while only the literal `topic` is consumed (Codex R28).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_pattern: Option<String>,
    /// Kafka consumer properties (e.g. `bootstrap.servers`).
    pub consumer_properties: HashMap<String, String>,
    /// Record input format (`{"type": "json"}`). Captured so
    /// [`KafkaSupervisorSpec::validate`] can REJECT non-JSON formats: the
    /// consumer only parses JSON, so a `csv`/`avro`/… spec would be
    /// accepted but silently skip every record.
    #[serde(default)]
    pub input_format: Option<serde_json::Value>,
    /// Number of indexing tasks.
    #[serde(default)]
    pub task_count: Option<usize>,
    /// Replication factor for indexing tasks.
    #[serde(default)]
    pub replicas: Option<usize>,
    /// Task duration before handoff (e.g. "PT1H").
    #[serde(default)]
    pub task_duration: Option<String>,
    /// Whether to start from the earliest available offset.
    pub use_earliest_offset: Option<bool>,
}

/// Tuning parameters for Kafka ingestion.
///
/// Like [`KafkaIoConfig`], real Druid `tuningConfig` objects carry many
/// fields this struct does not model (`type: "kafka"`, `maxBytesInMemory`,
/// `maxPendingPersists`, `handoffConditionTimeout`, …). Unknown fields are
/// IGNORED (Druid-lenient) so a genuine Druid supervisor spec parses; every
/// field here is optional, so this whole section may be omitted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTuningConfig {
    /// Maximum rows to hold in memory before persisting.
    pub max_rows_in_memory: Option<usize>,
    /// Maximum rows per segment.
    pub max_rows_per_segment: Option<usize>,
    /// Maximum total rows across all segments.
    pub max_total_rows: Option<usize>,
    /// How often to persist intermediate data (e.g. "PT10M").
    pub intermediate_persist_period: Option<String>,
}

/// Kafka supervisor runtime state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SupervisorState {
    /// Supervisor created but not yet running.
    Pending,
    /// Supervisor is actively consuming and indexing.
    Running,
    /// Supervisor is suspended (paused).
    Suspended,
    /// Supervisor is shutting down.
    Stopping,
    /// Supervisor encountered an unrecoverable error.
    Unhealthy,
}

/// Kafka supervisor runtime instance.
pub struct KafkaSupervisor {
    /// Unique supervisor identifier.
    pub id: String,
    /// The supervisor specification.
    pub spec: KafkaSupervisorSpec,
    /// Current runtime state.
    pub state: RwLock<SupervisorState>,
    /// Target datasource name (cached from spec).
    pub data_source: String,
}

impl KafkaSupervisor {
    /// Create a new Kafka supervisor from a spec.
    ///
    /// **Note**: this constructor does *not* validate the spec —
    /// callers that need spec validation (Wave 37B Medium #1 + #4
    /// closure) must use [`Self::try_new`] instead. `new` is retained
    /// for callers (notably tests) that intentionally exercise
    /// pre-validation behaviour.
    pub fn new(id: String, spec: KafkaSupervisorSpec) -> Self {
        let data_source = spec.data_schema.data_source.clone();
        Self {
            id,
            spec,
            state: RwLock::new(SupervisorState::Pending),
            data_source,
        }
    }

    /// Create a new Kafka supervisor, validating the spec first.
    ///
    /// Returns [`KafkaIngestError::Deserialization`] when
    /// [`KafkaSupervisorSpec::validate`] rejects the spec — this
    /// closes Wave 37B `ingest_kafka_tail` Medium #1 (spec_type must
    /// equal `"kafka"`) and Medium #4 (numeric ranges).
    pub fn try_new(id: String, spec: KafkaSupervisorSpec) -> Result<Self, KafkaIngestError> {
        spec.validate()?;
        Ok(Self::new(id, spec))
    }

    /// Returns `true` if the internal state lock has been poisoned
    /// by a previous panic.
    ///
    /// Wave 42-B (Wave 37B `ingest_kafka_tail` Low closure): exposes
    /// poisoning explicitly so callers can distinguish a real
    /// `Unhealthy` state from an internal-corruption-induced
    /// pseudo-`Unhealthy`.  Pre-fix, a poisoned lock was silently
    /// downgraded to `Unhealthy` by [`Self::state`] and silently
    /// ignored by [`Self::suspend`] / [`Self::resume`].
    #[must_use]
    pub fn is_state_poisoned(&self) -> bool {
        self.state.is_poisoned()
    }

    /// Get the current supervisor state.
    ///
    /// Wave 42-B (Wave 37B `ingest_kafka_tail` Low closure): if the
    /// internal `RwLock` is poisoned, we now log an explicit
    /// `tracing::error!` so operators can detect the corruption
    /// rather than seeing an indistinguishable `Unhealthy` and
    /// continuing to assume the supervisor is in a normal terminal
    /// state.  Callers needing programmatic detection should use
    /// [`Self::is_state_poisoned`].
    pub fn state(&self) -> SupervisorState {
        match self.state.read() {
            Ok(s) => s.clone(),
            Err(poisoned) => {
                tracing::error!(
                    id = %self.id,
                    "supervisor state lock poisoned — a previous panic left the state unreadable; reporting Unhealthy",
                );
                // Best-effort recovery: the inner value is still
                // structurally valid, just untrusted.  Read it
                // anyway so callers see the last persisted state
                // rather than always defaulting to `Unhealthy`.
                let _ = poisoned;
                SupervisorState::Unhealthy
            }
        }
    }

    /// Suspend the supervisor (pause consumption).
    ///
    /// Wave 42-B (Wave 37B `ingest_kafka_tail` Low closure): a
    /// poisoned lock previously caused this call to silently no-op,
    /// hiding a failed state transition from the operator.  We now
    /// log an explicit `tracing::error!` so the failure is visible
    /// in supervisor logs and on the metrics surface.
    pub fn suspend(&self) {
        match self.state.write() {
            Ok(mut s) => {
                if *s == SupervisorState::Running || *s == SupervisorState::Pending {
                    *s = SupervisorState::Suspended;
                    tracing::info!(id = %self.id, "supervisor suspended");
                }
            }
            Err(_poisoned) => {
                tracing::error!(
                    id = %self.id,
                    "supervisor suspend ignored — state lock poisoned by previous panic",
                );
            }
        }
    }

    /// Resume the supervisor from a suspended state.
    ///
    /// Wave 42-B (Wave 37B `ingest_kafka_tail` Low closure): see
    /// [`Self::suspend`] for the lock-poison handling rationale.
    pub fn resume(&self) {
        match self.state.write() {
            Ok(mut s) => {
                if *s == SupervisorState::Suspended || *s == SupervisorState::Pending {
                    *s = SupervisorState::Running;
                    tracing::info!(id = %self.id, "supervisor resumed");
                }
            }
            Err(_poisoned) => {
                tracing::error!(
                    id = %self.id,
                    "supervisor resume ignored — state lock poisoned by previous panic",
                );
            }
        }
    }

    /// Get the target datasource name.
    pub fn data_source(&self) -> &str {
        &self.data_source
    }

    /// Get the supervisor spec.
    pub fn spec(&self) -> &KafkaSupervisorSpec {
        &self.spec
    }

    /// Get supervisor status in Druid API format.
    pub fn status(&self) -> serde_json::Value {
        let state = self.state();
        let healthy = state == SupervisorState::Running || state == SupervisorState::Pending;

        serde_json::json!({
            "id": self.id,
            "state": state,
            "detailedState": state,
            "healthy": healthy,
            "spec": {
                "type": self.spec.spec_type,
                "dataSource": self.data_source,
                "topic": self.spec.io_config.topic,
            },
            "source": {
                "type": "kafka",
                "topic": self.spec.io_config.topic,
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_spec_json() -> &'static str {
        r#"{
            "type": "kafka",
            "dataSchema": {
                "dataSource": "wiki-events",
                "timestampSpec": {
                    "column": "__time",
                    "format": "iso"
                },
                "dimensionsSpec": {
                    "dimensions": [
                        "page",
                        {"name": "user", "type": "string"},
                        {"name": "delta", "type": "long"}
                    ],
                    "dimensionExclusions": ["__time"]
                },
                "metricsSpec": [
                    {"type": "count", "name": "count"},
                    {"type": "doubleSum", "name": "added", "fieldName": "added"}
                ],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": "NONE",
                    "rollup": false
                }
            },
            "ioConfig": {
                "topic": "wikipedia",
                "consumerProperties": {
                    "bootstrap.servers": "kafka:9092"
                },
                "taskCount": 2,
                "replicas": 1,
                "taskDuration": "PT1H",
                "useEarliestOffset": true
            },
            "tuningConfig": {
                "maxRowsInMemory": 75000,
                "maxRowsPerSegment": 5000000,
                "intermediatePersistPeriod": "PT10M"
            }
        }"#
    }

    #[test]
    fn parse_full_spec() {
        let spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse spec");

        assert_eq!(spec.spec_type, "kafka");
        assert_eq!(spec.data_schema.data_source, "wiki-events");
        assert_eq!(spec.data_schema.timestamp_spec.column, "__time");
        assert_eq!(spec.data_schema.timestamp_spec.format, "iso");
        assert_eq!(spec.data_schema.dimensions_spec.dimensions.len(), 3);
        assert_eq!(spec.data_schema.metrics_spec.len(), 2);

        let gran = spec.data_schema.granularity_spec.as_ref().expect("gran");
        assert_eq!(gran.segment_granularity, "DAY");
        assert_eq!(gran.query_granularity, "NONE");
        assert_eq!(gran.rollup, Some(false));

        assert_eq!(spec.io_config.topic, "wikipedia");
        assert_eq!(
            spec.io_config.consumer_properties.get("bootstrap.servers"),
            Some(&"kafka:9092".to_owned())
        );
        assert_eq!(spec.io_config.task_count, Some(2));
        assert_eq!(spec.io_config.replicas, Some(1));
        assert_eq!(spec.io_config.task_duration, Some("PT1H".to_owned()));
        assert_eq!(spec.io_config.use_earliest_offset, Some(true));

        let tuning = spec.tuning_config.as_ref().expect("tuning");
        assert_eq!(tuning.max_rows_in_memory, Some(75000));
        assert_eq!(tuning.max_rows_per_segment, Some(5_000_000));
        assert_eq!(tuning.intermediate_persist_period, Some("PT10M".to_owned()));
    }

    #[test]
    fn parse_minimal_spec() {
        let json = r#"{
            "type": "kafka",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "ts"},
                "dimensionsSpec": {"dimensions": ["a", "b"]}
            },
            "ioConfig": {
                "topic": "my-topic",
                "consumerProperties": {"bootstrap.servers": "localhost:9092"}
            }
        }"#;
        let spec: KafkaSupervisorSpec = serde_json::from_str(json).expect("parse");
        assert_eq!(spec.data_schema.data_source, "events");
        assert_eq!(spec.data_schema.timestamp_spec.format, "auto");
        assert!(spec.tuning_config.is_none());
        assert!(spec.data_schema.granularity_spec.is_none());
    }

    #[test]
    fn dimension_entry_variants() {
        let json = r#"["page", {"name": "delta", "type": "long"}]"#;
        let dims: Vec<DimensionEntry> = serde_json::from_str(json).expect("parse");
        assert_eq!(dims.len(), 2);
        match &dims[0] {
            DimensionEntry::String(s) => assert_eq!(s, "page"),
            _ => panic!("expected string variant"),
        }
        match &dims[1] {
            DimensionEntry::Typed { name, dim_type } => {
                assert_eq!(name, "delta");
                assert_eq!(dim_type, "long");
            }
            _ => panic!("expected typed variant"),
        }
    }

    #[test]
    fn supervisor_lifecycle() {
        let spec: KafkaSupervisorSpec = serde_json::from_str(sample_spec_json()).expect("parse");
        let sup = KafkaSupervisor::new("wiki-kafka".to_owned(), spec);

        assert_eq!(sup.state(), SupervisorState::Pending);
        assert_eq!(sup.data_source(), "wiki-events");
        assert_eq!(sup.id, "wiki-kafka");

        // Resume from Pending -> Running.
        sup.resume();
        assert_eq!(sup.state(), SupervisorState::Running);

        // Suspend -> Suspended.
        sup.suspend();
        assert_eq!(sup.state(), SupervisorState::Suspended);

        // Resume -> Running.
        sup.resume();
        assert_eq!(sup.state(), SupervisorState::Running);
    }

    #[test]
    fn suspend_from_pending() {
        let spec: KafkaSupervisorSpec = serde_json::from_str(sample_spec_json()).expect("parse");
        let sup = KafkaSupervisor::new("test".to_owned(), spec);

        assert_eq!(sup.state(), SupervisorState::Pending);
        sup.suspend();
        assert_eq!(sup.state(), SupervisorState::Suspended);
    }

    #[test]
    fn status_format() {
        let spec: KafkaSupervisorSpec = serde_json::from_str(sample_spec_json()).expect("parse");
        let sup = KafkaSupervisor::new("wiki-kafka".to_owned(), spec);
        sup.resume();

        let status = sup.status();
        assert_eq!(status["id"], "wiki-kafka");
        assert_eq!(status["state"], "RUNNING");
        assert_eq!(status["healthy"], true);
        assert_eq!(status["source"]["type"], "kafka");
        assert_eq!(status["source"]["topic"], "wikipedia");
        assert_eq!(status["spec"]["dataSource"], "wiki-events");
    }

    #[test]
    fn status_unhealthy_when_suspended() {
        let spec: KafkaSupervisorSpec = serde_json::from_str(sample_spec_json()).expect("parse");
        let sup = KafkaSupervisor::new("test".to_owned(), spec);
        sup.resume();
        sup.suspend();

        let status = sup.status();
        assert_eq!(status["state"], "SUSPENDED");
        assert_eq!(status["healthy"], false);
    }

    #[test]
    fn state_serde_screaming_snake() {
        let json = serde_json::to_string(&SupervisorState::Running).expect("ser");
        assert_eq!(json, "\"RUNNING\"");
        let json = serde_json::to_string(&SupervisorState::Pending).expect("ser");
        assert_eq!(json, "\"PENDING\"");
        let json = serde_json::to_string(&SupervisorState::Unhealthy).expect("ser");
        assert_eq!(json, "\"UNHEALTHY\"");

        let parsed: SupervisorState = serde_json::from_str("\"SUSPENDED\"").expect("deser");
        assert_eq!(parsed, SupervisorState::Suspended);
    }

    #[test]
    fn spec_roundtrip() {
        let spec: KafkaSupervisorSpec = serde_json::from_str(sample_spec_json()).expect("parse");
        let json = serde_json::to_string(&spec).expect("ser");
        let _: KafkaSupervisorSpec = serde_json::from_str(&json).expect("roundtrip");
    }

    /// W37B ingest_kafka Medium #1: `try_new` must reject any spec
    /// whose `type` is not `"kafka"`. Pre-fix the supervisor would
    /// have started with whatever string the caller supplied and
    /// echoed it back via `status()`.
    #[test]
    fn try_new_rejects_non_kafka_spec_type() {
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.spec_type = "kinesis".to_owned();
        let err = match KafkaSupervisor::try_new("test".to_owned(), spec) {
            Ok(_) => panic!("expected validation to reject non-kafka spec type"),
            Err(e) => e,
        };
        match err {
            KafkaIngestError::Deserialization(msg) => {
                assert!(msg.contains("must be \"kafka\""), "msg = {msg}");
            }
            other => panic!("expected Deserialization, got {other:?}"),
        }
    }

    /// W37B ingest_kafka Medium #4: numeric tuning fields must be in
    /// a sensible range. `taskCount = 0` produces an inert supervisor
    /// pre-fix; `maxRowsInMemory = usize::MAX` is a downstream
    /// allocation hazard.
    #[test]
    fn try_new_rejects_pathological_numeric_fields() {
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");

        // Zero task_count rejected.
        spec.io_config.task_count = Some(0);
        assert!(spec.validate().is_err());

        // Restore + try replicas.
        spec.io_config.task_count = Some(2);
        spec.io_config.replicas = Some(0);
        assert!(spec.validate().is_err());

        // Restore + try huge max_rows_in_memory.
        spec.io_config.replicas = Some(1);
        if let Some(tc) = spec.tuning_config.as_mut() {
            tc.max_rows_in_memory = Some(usize::MAX);
        }
        assert!(spec.validate().is_err());

        // A clean spec passes validation.
        if let Some(tc) = spec.tuning_config.as_mut() {
            tc.max_rows_in_memory = Some(75_000);
        }
        spec.validate().expect("clean spec");
        let sup = match KafkaSupervisor::try_new("test".to_owned(), spec) {
            Ok(s) => s,
            Err(e) => panic!("clean spec must validate, got {e}"),
        };
        assert_eq!(sup.state(), SupervisorState::Pending);
    }

    /// W37B ingest_kafka Medium #3 (Wave 45-B closure): a typo in a
    /// supervisor-spec field name must be rejected by serde, not
    /// silently dropped.  Pre-fix `consumerProperties` mistyped as
    /// `consumerPropeties` deserialized into an empty map and the
    /// supervisor came up without any broker address — the operator
    /// would have no signal that their config was wrong.
    #[test]
    fn deny_unknown_fields_rejects_top_level_typo() {
        let json = r#"{
            "type": "kafka",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "ts"},
                "dimensionsSpec": {"dimensions": ["a"]}
            },
            "ioConfig": {
                "topic": "t",
                "consumerProperties": {"bootstrap.servers": "x:9092"}
            },
            "extraGarbage": "boom"
        }"#;
        let err = serde_json::from_str::<KafkaSupervisorSpec>(json)
            .expect_err("deny_unknown_fields must reject extraGarbage");
        let msg = format!("{err}");
        assert!(
            msg.contains("extraGarbage") || msg.contains("unknown field"),
            "msg = {msg}"
        );
    }

    #[test]
    fn io_config_typo_in_required_field_still_rejected() {
        // The sub-configs are now Druid-lenient (they accept the many extra
        // fields real Druid populates), BUT a typo in a REQUIRED field
        // (`consumerPropeties` → `consumerProperties` missing) still fails
        // loudly as a missing-required-field error — the important half of
        // the Wave 45-B typo protection survives.
        let json = r#"{
            "type": "kafka",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "ts"},
                "dimensionsSpec": {"dimensions": ["a"]}
            },
            "ioConfig": {
                "topic": "t",
                "consumerPropeties": {"bootstrap.servers": "x:9092"}
            }
        }"#;
        let err = serde_json::from_str::<KafkaSupervisorSpec>(json)
            .expect_err("typo dropping a required field must still reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("consumerProperties") || msg.contains("missing field"),
            "msg = {msg}"
        );
    }

    #[test]
    fn tuning_config_ignores_unknown_and_extra_fields() {
        // Druid-lenient (relaxes the earlier `deny_unknown_fields`): a real
        // Druid `tuningConfig` carries `type` plus many fields this crate
        // does not model, and every modelled field is optional — so a
        // tuningConfig with unknown/extra keys must parse cleanly (else no
        // real Druid supervisor spec could ever start). The trade-off: a
        // typo in an OPTIONAL tuning field is silently ignored.
        let json = r#"{
            "type": "kafka",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "ts"},
                "dimensionsSpec": {"dimensions": ["a"]}
            },
            "ioConfig": {
                "type": "kafka",
                "topic": "t",
                "inputFormat": {"type": "json"},
                "consumerProperties": {"bootstrap.servers": "x:9092"}
            },
            "tuningConfig": {
                "type": "kafka",
                "maxRowsPerSegment": 1000,
                "maxBytesInMemory": 12345,
                "maxRowsInMmory": 1000
            }
        }"#;
        let spec = serde_json::from_str::<KafkaSupervisorSpec>(json)
            .expect("Druid-shaped spec with extra ioConfig/tuningConfig fields must parse");
        // Modelled fields still bind; unknown ones are dropped.
        assert_eq!(spec.io_config.topic, "t");
        assert_eq!(
            spec.tuning_config.and_then(|t| t.max_rows_per_segment),
            Some(1000)
        );
    }

    /// W37B ingest_kafka Medium #1 sanity: the original sample spec
    /// must continue to validate cleanly.
    #[test]
    fn try_new_accepts_well_formed_spec() {
        let spec: KafkaSupervisorSpec = serde_json::from_str(sample_spec_json()).expect("parse");
        let sup = match KafkaSupervisor::try_new("wiki-kafka".to_owned(), spec) {
            Ok(s) => s,
            Err(e) => panic!("well-formed spec must validate, got {e}"),
        };
        assert_eq!(sup.id, "wiki-kafka");
    }

    #[test]
    fn granularity_type_defaults_to_uniform_and_rejects_other() {
        // Druid makes granularitySpec.type OPTIONAL (default "uniform"); a
        // spec omitting it must parse AND validate — otherwise a standard
        // supervisor is rejected and a feature transition skips a persisted
        // spec, consuming zero records (Codex R12).
        let json = r#"{
            "type": "kafka",
            "dataSchema": {
                "dataSource": "ds",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"segmentGranularity": "DAY", "queryGranularity": "NONE", "rollup": false}
            },
            "ioConfig": {"topic": "t", "consumerProperties": {"bootstrap.servers": "kafka:9092"}}
        }"#;
        let spec: KafkaSupervisorSpec =
            serde_json::from_str(json).expect("parse without granularity type");
        assert_eq!(
            spec.data_schema
                .granularity_spec
                .as_ref()
                .expect("gran")
                .spec_type,
            "uniform"
        );
        spec.validate()
            .expect("validate with defaulted uniform type");

        // An explicit non-uniform (e.g. "arbitrary") type is rejected loudly.
        let arbitrary = json.replace(
            r#""granularitySpec": {"segmentGranularity""#,
            r#""granularitySpec": {"type": "arbitrary", "segmentGranularity""#,
        );
        let spec2: KafkaSupervisorSpec = serde_json::from_str(&arbitrary).expect("parse arbitrary");
        let err = spec2.validate().expect_err("non-uniform must be rejected");
        assert!(format!("{err}").contains("uniform"), "err = {err}");
    }

    #[test]
    fn granularity_segment_and_query_fields_default() {
        // Druid defaults segmentGranularity=day and queryGranularity=none when
        // omitted (Codex R14 oracle), so a granularitySpec supplying only
        // rollup:false must parse AND validate.
        let json = r#"{
            "type": "kafka",
            "dataSchema": {
                "dataSource": "ds",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {"topic": "t", "consumerProperties": {"bootstrap.servers": "kafka:9092"}}
        }"#;
        let spec: KafkaSupervisorSpec = serde_json::from_str(json).expect("parse rollup-only gran");
        let g = spec.data_schema.granularity_spec.as_ref().expect("gran");
        assert_eq!(g.spec_type, "uniform");
        assert_eq!(g.segment_granularity, "DAY");
        assert_eq!(g.query_granularity, "NONE");
        spec.validate()
            .expect("validate with defaulted granularity fields");
    }

    #[test]
    fn absent_granularity_spec_is_rejected() {
        // Druid defaults rollup=TRUE when granularitySpec is entirely absent
        // (Codex R14 oracle); streaming ingests raw and cannot honour that, so
        // an absent granularitySpec must be rejected — not silently ingested
        // raw where Druid would roll up.
        let json = r#"{
            "type": "kafka",
            "dataSchema": {
                "dataSource": "ds",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]}
            },
            "ioConfig": {"topic": "t", "consumerProperties": {"bootstrap.servers": "kafka:9092"}}
        }"#;
        let spec: KafkaSupervisorSpec = serde_json::from_str(json).expect("parse no gran");
        let err = spec
            .validate()
            .expect_err("absent granularitySpec must be rejected");
        assert!(
            format!("{err}").contains("granularitySpec is required"),
            "err = {err}"
        );
    }

    #[test]
    fn granularity_explicit_null_fields_resolve_to_defaults() {
        // Druid resolves an EXPLICIT null granularity field to its default
        // (Codex R15); serde's `default` only covers a MISSING field, so a
        // deserialize_with maps null -> default. A spec with all-null fields
        // (but explicit rollup:false) must parse AND validate.
        let json = r#"{
            "type": "kafka",
            "dataSchema": {
                "dataSource": "ds",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"type": null, "segmentGranularity": null, "queryGranularity": null, "rollup": false}
            },
            "ioConfig": {"topic": "t", "consumerProperties": {"bootstrap.servers": "kafka:9092"}}
        }"#;
        let spec: KafkaSupervisorSpec =
            serde_json::from_str(json).expect("parse null granularity fields");
        let g = spec.data_schema.granularity_spec.as_ref().expect("gran");
        assert_eq!(g.spec_type, "uniform");
        assert_eq!(g.segment_granularity, "DAY");
        assert_eq!(g.query_granularity, "NONE");
        spec.validate()
            .expect("validate with null-resolved defaults");
    }

    #[test]
    fn validate_rejects_non_json_input_format() {
        // The consumer only parses JSON; a CSV/Avro/etc spec must be
        // rejected loudly, not accepted and then skip every record.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.io_config.input_format = Some(serde_json::json!({"type": "csv"}));
        let err = spec
            .validate()
            .expect_err("non-JSON inputFormat must reject");
        assert!(format!("{err}").contains("inputFormat"), "err = {err}");
        // JSON (and absent) are accepted.
        spec.io_config.input_format = Some(serde_json::json!({"type": "json"}));
        spec.validate().expect("json inputFormat ok");
    }

    /// Codex R28 HIGH: librdkafka's `subscribe` treats any topic name
    /// prefixed with `^` as a REGEX pattern matched against the cluster's
    /// full topic list (librdkafka 2.12.1 `rd_kafka_subscribe` docs), while
    /// Druid's `ioConfig.topic` is a LITERAL name — regex lives in the
    /// separate (unsupported) `topicPattern` field. Pre-fix `"^orders-.*"`
    /// sailed through validation and would (a) ingest MULTIPLE topics into
    /// one datasource, (b) let a literal `orders-prod` second supervisor
    /// pass the string-based pair-uniqueness guard and double-ingest, and
    /// (c) break the literal-topic assumptions of provenance stamping and
    /// replay cleanup.
    #[test]
    fn validate_rejects_regex_topic() {
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.io_config.topic = "^orders-.*".to_owned();
        let err = spec
            .validate()
            .expect_err("'^'-prefixed topic must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains('^'), "msg must explain the '^' trigger: {msg}");
        assert!(
            msg.contains("topicPattern"),
            "msg must point at topicPattern: {msg}"
        );
        // A literal topic stays accepted.
        spec.io_config.topic = "orders-prod".to_owned();
        spec.validate().expect("literal topic ok");
    }

    /// Codex R28 HIGH companion: a spec carrying Druid's `topicPattern`
    /// (regex multi-topic ingestion, Druid 28+) must be REJECTED loudly,
    /// not silently ignored by the Druid-lenient `ioConfig` — silently
    /// dropping it would leave the operator believing the regex
    /// subscription is live while the consumer reads only the literal
    /// `topic`.
    #[test]
    fn validate_rejects_topic_pattern_field() {
        let json = r#"{
            "type": "kafka",
            "dataSchema": {
                "dataSource": "ds",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {
                "topic": "orders-prod",
                "topicPattern": "orders-.*",
                "consumerProperties": {"bootstrap.servers": "kafka:9092"}
            }
        }"#;
        let spec: KafkaSupervisorSpec =
            serde_json::from_str(json).expect("parse topicPattern spec");
        let err = spec.validate().expect_err("topicPattern must be rejected");
        assert!(format!("{err}").contains("topicPattern"), "err = {err}");
    }

    /// Codex R39 HIGH: a syntactically INVALID Kafka topic name must be
    /// rejected at validation. Pre-fix a trailing-space name (`"orders "`)
    /// passed both `validate` and `rd_kafka_subscribe`, but the broker returns
    /// a PERMANENT `INVALID_TOPIC_EXCEPTION`, leaving an acknowledged
    /// supervisor idle (ingesting nothing) until the records expire at Kafka
    /// retention. Enforce Apache Kafka's own topic-name grammar
    /// (`org.apache.kafka.common.internals.Topic`): non-empty, <= 249 chars,
    /// only `[A-Za-z0-9._-]`, and not the reserved `.` / `..`.
    #[test]
    fn validate_rejects_malformed_topic_names() {
        let base: KafkaSupervisorSpec = serde_json::from_str(sample_spec_json()).expect("parse");
        for bad in [
            "orders ",        // trailing space (the exact finding)
            " orders",        // leading space
            "or ders",        // internal space
            "",               // empty
            ".",              // reserved
            "..",             // reserved
            "orders$",        // illegal punctuation
            "orders\ttab",    // control character (TAB)
            "orders\u{0}end", // NUL
            "naïve",          // non-ASCII
        ] {
            let mut spec = base.clone();
            spec.io_config.topic = bad.to_owned();
            assert!(
                spec.validate().is_err(),
                "malformed topic {bad:?} must be rejected"
            );
        }
        // Length: 250 chars (> Kafka's 249 max) rejected, exactly 249 accepted.
        let mut spec = base.clone();
        spec.io_config.topic = "a".repeat(250);
        assert!(
            spec.validate().is_err(),
            "250-char topic must be rejected (Kafka max is 249)"
        );
        spec.io_config.topic = "a".repeat(249);
        spec.validate().expect("249-char topic must validate");
        // Legitimate names spanning the full legal charset stay accepted.
        for ok in [
            "orders",
            "orders-prod",
            "orders.v2",
            "a_b-1.2",
            "t",
            "_internal",
        ] {
            let mut spec = base.clone();
            spec.io_config.topic = ok.to_owned();
            spec.validate()
                .unwrap_or_else(|e| panic!("legal topic {ok:?} must validate, got {e}"));
        }
    }

    /// Codex R29 F1: a `consumerProperties["topic.blacklist"]` — librdkafka's
    /// regex PATTERN LIST that hides matching topics from broker metadata "as
    /// if the topics did not exist" — must be REJECTED at validation. A
    /// pattern matching the supervisor's own literal `topic` silently blanks
    /// the subscription (zero assigned partitions), and on an earliest
    /// RE-CREATE the pair's prior rows are deleted trusting the replay to
    /// rebuild them — a blanked replay rebuilds NOTHING (permanent loss). A
    /// single-literal-topic supervisor has no legitimate use for the key.
    #[test]
    fn validate_rejects_topic_blacklist_consumer_property() {
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.io_config
            .consumer_properties
            .insert("topic.blacklist".to_owned(), "wiki.*".to_owned());
        let err = spec
            .validate()
            .expect_err("topic.blacklist must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("topic.blacklist"),
            "msg must name the offending key: {msg}"
        );
        // Removing the key restores a valid spec.
        spec.io_config.consumer_properties.remove("topic.blacklist");
        spec.validate().expect("spec without topic.blacklist ok");
    }

    #[test]
    fn validate_rejects_unsupported_metric_type() {
        // A sketch/complex aggregator would be silently stored as a garbage
        // double — reject it. Numeric sum/min/max + count stay accepted.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.data_schema.metrics_spec = vec![
            serde_json::json!({"type": "longSum", "name": "a", "fieldName": "a"}),
            serde_json::json!({"type": "thetaSketch", "name": "u", "fieldName": "u"}),
        ];
        let err = spec.validate().expect_err("sketch metric must reject");
        assert!(format!("{err}").contains("metricsSpec"), "err = {err}");
        // Drop the sketch → passes.
        spec.data_schema.metrics_spec =
            vec![serde_json::json!({"type": "doubleSum", "name": "v", "fieldName": "v"})];
        spec.validate().expect("numeric metrics ok");
    }

    #[test]
    fn validate_rejects_malformed_metric_name_or_field() {
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        // Typo'd `name` key → BatchIngester would silently omit the metric.
        spec.data_schema.metrics_spec =
            vec![serde_json::json!({"type": "doubleSum", "naem": "total", "fieldName": "v"})];
        assert!(spec.validate().is_err(), "missing metric name must reject");
        // Non-count metric without fieldName.
        spec.data_schema.metrics_spec = vec![serde_json::json!({"type": "longSum", "name": "x"})];
        assert!(spec.validate().is_err(), "missing fieldName must reject");
    }

    #[test]
    fn validate_rejects_rollup_and_schema_discovery() {
        // rollup:true — streaming ingests raw.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        if let Some(g) = spec.data_schema.granularity_spec.as_mut() {
            g.rollup = Some(true);
        }
        assert!(spec.validate().is_err(), "rollup:true must reject");

        // useSchemaDiscovery:true.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.data_schema.dimensions_spec.use_schema_discovery = Some(true);
        assert!(spec.validate().is_err(), "useSchemaDiscovery must reject");

        // Empty dimensions.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.data_schema.dimensions_spec.dimensions.clear();
        assert!(spec.validate().is_err(), "empty dimensions must reject");
    }

    #[test]
    fn validate_rejects_unsupported_dim_type_transform_and_blank_field() {
        // Unsupported dimension type (e.g. `json`).
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.data_schema.dimensions_spec.dimensions = vec![DimensionEntry::Typed {
            name: "attrs".into(),
            dim_type: "json".into(),
        }];
        assert!(spec.validate().is_err(), "json dim type must reject");

        // transformSpec present.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.data_schema.transform_spec = Some(serde_json::json!({"filter": {"type": "selector"}}));
        assert!(spec.validate().is_err(), "transformSpec must reject");

        // Blank (empty) fieldName on a non-count metric.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.data_schema.metrics_spec =
            vec![serde_json::json!({"type": "doubleSum", "name": "total", "fieldName": ""})];
        assert!(spec.validate().is_err(), "blank fieldName must reject");
    }

    #[test]
    fn deny_unknown_fields_rejects_unmodeled_nested_spec_features() {
        // Any unmodeled data-shaping field is rejected at PARSE (deny_unknown_fields).
        let bad_dataschema = r#"{"type":"kafka",
            "dataSchema":{"dataSource":"e","timestampSpec":{"column":"__time"},
                "dimensionsSpec":{"dimensions":["p"]},"unknownFeature":true},
            "ioConfig":{"topic":"t","consumerProperties":{"bootstrap.servers":"x:9092"}}}"#;
        assert!(serde_json::from_str::<KafkaSupervisorSpec>(bad_dataschema).is_err());
        let bad_dims = r#"{"type":"kafka",
            "dataSchema":{"dataSource":"e","timestampSpec":{"column":"__time"},
                "dimensionsSpec":{"dimensions":["p"],"includeAllDimensions":true}},
            "ioConfig":{"topic":"t","consumerProperties":{"bootstrap.servers":"x:9092"}}}"#;
        assert!(serde_json::from_str::<KafkaSupervisorSpec>(bad_dims).is_err());
        let bad_ts = r#"{"type":"kafka",
            "dataSchema":{"dataSource":"e","timestampSpec":{"column":"__time","missingValue":"2020"},
                "dimensionsSpec":{"dimensions":["p"]}},
            "ioConfig":{"topic":"t","consumerProperties":{"bootstrap.servers":"x:9092"}}}"#;
        assert!(serde_json::from_str::<KafkaSupervisorSpec>(bad_ts).is_err());
    }

    #[test]
    fn validate_requires_bootstrap_servers_and_rejects_bad_granularity_and_collisions() {
        // Missing bootstrap.servers.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.io_config.consumer_properties.clear();
        assert!(
            spec.validate().is_err(),
            "missing bootstrap.servers must reject"
        );

        // Omitted rollup (Druid defaults true) must be rejected.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        if let Some(g) = spec.data_schema.granularity_spec.as_mut() {
            g.rollup = None;
        }
        assert!(spec.validate().is_err(), "omitted rollup must reject");

        // Non-NONE queryGranularity must be rejected.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        if let Some(g) = spec.data_schema.granularity_spec.as_mut() {
            g.query_granularity = "MINUTE".into();
        }
        assert!(
            spec.validate().is_err(),
            "queryGranularity MINUTE must reject"
        );

        // A metric named `__time`, and a dim/metric name collision.
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.data_schema.metrics_spec =
            vec![serde_json::json!({"type": "doubleSum", "name": "__time", "fieldName": "v"})];
        assert!(spec.validate().is_err(), "__time metric must reject");

        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.data_schema.dimensions_spec.dimensions = vec![DimensionEntry::String("dup".into())];
        spec.data_schema.metrics_spec =
            vec![serde_json::json!({"type": "doubleSum", "name": "dup", "fieldName": "v"})];
        assert!(
            spec.validate().is_err(),
            "dim/metric name collision must reject"
        );
    }

    #[test]
    fn validate_rejects_long_and_float_metric_types() {
        // Numeric metrics are materialised as DOUBLE, so only count + double*
        // are store/query-consistent; long*/float* are rejected.
        for t in ["longSum", "longMin", "floatSum", "floatMax"] {
            let mut spec: KafkaSupervisorSpec =
                serde_json::from_str(sample_spec_json()).expect("parse");
            spec.data_schema.metrics_spec =
                vec![serde_json::json!({"type": t, "name": "m", "fieldName": "v"})];
            assert!(
                spec.validate().is_err(),
                "{t} must reject (materialised as double)"
            );
        }
    }

    #[test]
    fn validate_rejects_input_format_with_flatten_spec() {
        let mut spec: KafkaSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse");
        spec.io_config.input_format = Some(serde_json::json!({
            "type": "json",
            "flattenSpec": {"fields": [{"type": "path", "name": "ts", "expr": "$.t"}]}
        }));
        assert!(
            spec.validate().is_err(),
            "inputFormat with flattenSpec must reject"
        );
    }

    /// Wave 42-B regression for Wave 37B `ingest_kafka_tail` Low
    /// (`lib.rs:203` lock poisoning).  Pre-fix, a poisoned state lock
    /// was indistinguishable from a real `Unhealthy` transition and
    /// suspend/resume silently no-op'd.  Post-fix:
    ///
    /// 1. [`KafkaSupervisor::is_state_poisoned`] reports `true` after
    ///    a writer thread panicked while holding the lock.
    /// 2. [`KafkaSupervisor::state`] still returns `Unhealthy` for
    ///    backward-compat with existing callers but emits an explicit
    ///    `tracing::error!` (not asserted here — verified by
    ///    inspection in the source under Wave 42-B).
    #[test]
    fn poisoned_state_lock_is_detectable() {
        use std::sync::Arc;

        let spec: KafkaSupervisorSpec = serde_json::from_str(sample_spec_json()).expect("parse");
        let sup = Arc::new(KafkaSupervisor::new("poison-test".to_owned(), spec));
        assert!(
            !sup.is_state_poisoned(),
            "freshly constructed supervisor must not be poisoned"
        );

        // Poison the lock by panicking inside a write guard on a
        // separate thread.  The panic is contained — joining the
        // thread returns `Err`, and the lock remains poisoned for
        // the rest of the test.
        let sup_clone = Arc::clone(&sup);
        let handle = std::thread::spawn(move || {
            let _guard = sup_clone.state.write().expect("acquire write lock");
            panic!("intentional panic to poison the RwLock");
        });
        let join_result = handle.join();
        assert!(
            join_result.is_err(),
            "spawned thread must panic to poison the lock"
        );

        // The poisoning must now be observable.
        assert!(
            sup.is_state_poisoned(),
            "RwLock must be poisoned after the writer thread panicked"
        );

        // `state()` must still return `Unhealthy` (the documented
        // graceful-degradation contract) rather than panicking.
        assert_eq!(sup.state(), SupervisorState::Unhealthy);

        // `suspend()` and `resume()` must not panic on a poisoned
        // lock — they log and no-op.  We just call them and assert
        // the supervisor is still marked poisoned.
        sup.suspend();
        sup.resume();
        assert!(sup.is_state_poisoned());
    }
}
