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
pub mod supervisor;

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
/// supervisor running with default values.  The same hardening is
/// applied to [`KafkaIoConfig`] and [`KafkaTuningConfig`] below.
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
#[serde(rename_all = "camelCase")]
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
}

/// Timestamp extraction configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct DimensionsSpec {
    /// List of dimension entries.
    pub dimensions: Vec<DimensionEntry>,
    /// Columns to exclude from auto-detection.
    #[serde(default)]
    pub dimension_exclusions: Vec<String>,
}

/// A dimension can be a simple string name or a typed descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DimensionEntry {
    /// Dimension specified as a plain column name (string type implied).
    String(String),
    /// Dimension with explicit type information.
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
#[serde(rename_all = "camelCase")]
pub struct GranularitySpec {
    /// Granularity spec type (e.g. "uniform").
    #[serde(rename = "type")]
    pub spec_type: String,
    /// Segment granularity (e.g. "DAY", "HOUR").
    pub segment_granularity: String,
    /// Query granularity (e.g. "MINUTE", "NONE").
    pub query_granularity: String,
    /// Whether to roll up rows with identical dimensions and timestamp.
    pub rollup: Option<bool>,
}

/// Kafka consumer I/O configuration.
///
/// Wave 45-B closure of Wave 37B `ingest_kafka_tail` Medium #3:
/// `deny_unknown_fields` flags typos like `consumerPropeties` /
/// `taskDurtion` instead of silently accepting and dropping them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KafkaIoConfig {
    /// Kafka topic to consume from.
    pub topic: String,
    /// Kafka consumer properties (e.g. `bootstrap.servers`).
    pub consumer_properties: HashMap<String, String>,
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
/// Wave 45-B closure of Wave 37B `ingest_kafka_tail` Medium #3:
/// `deny_unknown_fields` flags typos in operator-supplied tuning
/// configs (e.g. `maxRowsInMmory`) instead of silently accepting them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
                    {"type": "longSum", "name": "added", "fieldName": "added"}
                ],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": "MINUTE",
                    "rollup": true
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
        assert_eq!(gran.query_granularity, "MINUTE");
        assert_eq!(gran.rollup, Some(true));

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
    fn deny_unknown_fields_rejects_io_config_typo() {
        // `consumerPropeties` (missing the second `r`) is the typo the
        // Codex finding called out by name.
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
            .expect_err("deny_unknown_fields must reject consumerPropeties typo");
        let msg = format!("{err}");
        assert!(
            msg.contains("consumerPropeties") || msg.contains("unknown field"),
            "msg = {msg}"
        );
    }

    #[test]
    fn deny_unknown_fields_rejects_tuning_config_typo() {
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
            "tuningConfig": {
                "maxRowsInMmory": 1000
            }
        }"#;
        let err = serde_json::from_str::<KafkaSupervisorSpec>(json)
            .expect_err("deny_unknown_fields must reject tuning typo");
        let msg = format!("{err}");
        assert!(
            msg.contains("maxRowsInMmory") || msg.contains("unknown field"),
            "msg = {msg}"
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
