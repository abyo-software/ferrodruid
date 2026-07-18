// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Kinesis indexing service for FerroDruid.
//!
//! Provides Kinesis supervisor spec parsing (Druid supervisor API
//! compatible) plus the reusable, AWS-optional building blocks the
//! overlord drives to actually consume a stream:
//!
//! * [`source`] — the transport-abstracted [`KinesisSource`] trait
//!   (list shards / shard iterators / get-records / stream identity)
//!   with a distinct, matchable expired-iterator error;
//! * [`mock`] — a deterministic in-memory [`MockKinesisSource`] usable
//!   from OTHER crates' tests (no AWS, no docker, no feature);
//! * [`frontier`] — the sequence-number resume frontier (opaque
//!   bignum-string positions, per-shard span folding, fold from the
//!   durable segment payload's `kinesisSequences`);
//! * [`decode`] — per-record JSON decode with loud dead-lettering;
//! * [`aws`] (behind the `kinesis-io` feature, default OFF) — the real
//!   `aws-sdk-kinesis` adapter. The DEFAULT build carries no AWS
//!   dependency.
//!
//! The consume loop / publish tail / supervisor lifecycle live in the
//! overlord (mirroring the Kafka split); [`KinesisSupervisor::run`] here
//! remains a stub.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(feature = "kinesis-io")]
pub mod aws;
pub mod decode;
pub mod frontier;
pub mod mock;
pub mod source;

pub use decode::{DeadLetter, DecodedBatch, RecordDecodeError, decode_batch, decode_record};
pub use frontier::{
    FrontierError, FrontierRowEvidence, KINESIS_STREAMING_KIND, KinesisResumeFrontier,
    SEQUENCES_PAYLOAD_KEY, STREAM_CREATION_PAYLOAD_KEY, STREAM_PAYLOAD_KEY, SeqNum, SeqSpan,
    ShardResume, ShardSequences, fold_resume_frontier, fold_spans, sequences_to_payload,
};
pub use mock::MockKinesisSource;
pub use source::{
    GetRecordsOutput, KinesisRecord, KinesisSource, KinesisSourceError, ShardId, ShardIterator,
    StartPosition, StreamIdentity,
};

#[cfg(feature = "kinesis-io")]
pub use aws::AwsKinesisSource;

/// Errors from Kinesis ingestion.
#[derive(Debug, Error)]
pub enum KinesisIngestError {
    /// Failed to connect to Kinesis.
    #[error("kinesis connection failed: {0}")]
    Connection(String),
    /// Deserialization error.
    #[error("kinesis deserialization error: {0}")]
    Deserialization(String),
    /// Transport / API error from the underlying [`KinesisSource`].
    #[error(transparent)]
    Source(#[from] KinesisSourceError),
}

/// Kinesis supervisor spec (Druid-compatible JSON format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KinesisSupervisorSpec {
    /// Spec type, must be `"kinesis"`.
    #[serde(rename = "type")]
    pub spec_type: String,
    /// Data schema describing the target datasource and its columns.
    pub data_schema: DataSchema,
    /// Kinesis I/O configuration.
    pub io_config: KinesisIoConfig,
    /// Optional tuning parameters.
    pub tuning_config: Option<KinesisTuningConfig>,
}

/// Data schema for a Kinesis ingestion spec.
///
/// Reuses the same structure as the Kafka `DataSchema`.
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
        /// Dimension type (e.g. `"string"`, `"long"`, `"double"`).
        #[serde(rename = "type")]
        dim_type: String,
    },
}

/// Segment granularity configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GranularitySpec {
    /// Granularity spec type (e.g. `"uniform"`).
    #[serde(rename = "type")]
    pub spec_type: String,
    /// Segment granularity (e.g. `"DAY"`, `"HOUR"`).
    pub segment_granularity: String,
    /// Query granularity (e.g. `"MINUTE"`, `"NONE"`).
    pub query_granularity: String,
    /// Whether to roll up rows with identical dimensions and timestamp.
    pub rollup: Option<bool>,
}

/// Kinesis stream I/O configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KinesisIoConfig {
    /// Kinesis stream name.
    pub stream: String,
    /// Optional custom Kinesis endpoint (e.g. for localstack).
    pub endpoint: Option<String>,
    /// AWS region hosting the stream. Druid specs carry this (as
    /// `region`); defaults to `us-east-1` when absent so older stored
    /// specs keep parsing.
    #[serde(default = "default_region")]
    pub region: String,
    /// Optional IAM role ARN to assume for stream access. PARSED but
    /// NOT implemented in v1 (env/default credential chain only) — see
    /// [`log_unsupported_options`](Self::log_unsupported_options).
    pub aws_assumed_role_arn: Option<String>,
    /// Number of indexing tasks. v1 runs a SINGLE consumer task per
    /// supervisor regardless of this value.
    #[serde(default)]
    pub task_count: Option<usize>,
    /// Whether to start reading from the earliest sequence number
    /// (`TRIM_HORIZON`); default `false` (`LATEST`).
    pub use_earliest_sequence_number: Option<bool>,
}

fn default_region() -> String {
    "us-east-1".to_owned()
}

impl KinesisIoConfig {
    /// The spec-derived start position for shards with NO durable
    /// resume evidence: `TRIM_HORIZON` when
    /// `useEarliestSequenceNumber=true`, else `LATEST` (the Druid
    /// default).
    #[must_use]
    pub fn start_position(&self) -> StartPosition {
        if self.use_earliest_sequence_number == Some(true) {
            StartPosition::TrimHorizon
        } else {
            StartPosition::Latest
        }
    }

    /// Warn (loudly, once at supervisor start) about spec options that
    /// parse but are NOT implemented in v1: `awsAssumedRoleArn` (STS
    /// assume-role — the default credential chain is used instead) and
    /// `taskCount > 1` (a single consumer task polls all shards).
    pub fn log_unsupported_options(&self) {
        if let Some(arn) = &self.aws_assumed_role_arn {
            tracing::warn!(
                stream = %self.stream,
                aws_assumed_role_arn = %arn,
                "kinesis spec sets awsAssumedRoleArn, but STS assume-role is NOT \
                 implemented in v1 — the default AWS credential chain (env / \
                 profile / instance role) is used instead; ensure it can read \
                 the stream directly"
            );
        }
        if self.task_count.is_some_and(|n| n > 1) {
            tracing::warn!(
                stream = %self.stream,
                task_count = self.task_count,
                "kinesis spec sets taskCount > 1, but v1 runs a SINGLE consumer \
                 task per supervisor polling all shards; the value is ignored"
            );
        }
    }
}

/// Tuning parameters for Kinesis ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KinesisTuningConfig {
    /// Maximum rows to hold in memory before persisting.
    pub max_rows_in_memory: Option<usize>,
    /// Maximum rows per segment.
    pub max_rows_per_segment: Option<usize>,
}

/// Kinesis indexing supervisor (stub).
#[derive(Debug)]
pub struct KinesisSupervisor {
    _spec: KinesisSupervisorSpec,
}

impl KinesisSupervisor {
    /// Create a new Kinesis supervisor.
    pub fn new(spec: KinesisSupervisorSpec) -> Self {
        Self { _spec: spec }
    }

    /// Start consuming and indexing (stub).
    pub async fn run(&self) -> Result<(), KinesisIngestError> {
        tracing::info!(stream = %self._spec.io_config.stream, "kinesis supervisor stub — not yet implemented");
        Ok(())
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
            "type": "kinesis",
            "dataSchema": {
                "dataSource": "clickstream",
                "timestampSpec": {
                    "column": "event_time",
                    "format": "iso"
                },
                "dimensionsSpec": {
                    "dimensions": [
                        "user_id",
                        {"name": "page", "type": "string"},
                        {"name": "duration", "type": "long"}
                    ]
                },
                "metricsSpec": [
                    {"type": "count", "name": "count"},
                    {"type": "doubleSum", "name": "revenue", "fieldName": "revenue"}
                ],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": "MINUTE",
                    "rollup": false
                }
            },
            "ioConfig": {
                "stream": "my-kinesis-stream",
                "endpoint": "http://localhost:4566",
                "awsAssumedRoleArn": "arn:aws:iam::123456789:role/druid-kinesis",
                "taskCount": 3,
                "useEarliestSequenceNumber": true
            },
            "tuningConfig": {
                "maxRowsInMemory": 50000,
                "maxRowsPerSegment": 3000000
            }
        }"#
    }

    #[test]
    fn parse_full_spec() {
        let spec: KinesisSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse spec");

        assert_eq!(spec.spec_type, "kinesis");
        assert_eq!(spec.data_schema.data_source, "clickstream");
        assert_eq!(spec.data_schema.timestamp_spec.column, "event_time");
        assert_eq!(spec.data_schema.timestamp_spec.format, "iso");
        assert_eq!(spec.data_schema.dimensions_spec.dimensions.len(), 3);
        assert_eq!(spec.data_schema.metrics_spec.len(), 2);

        let gran = spec.data_schema.granularity_spec.as_ref().expect("gran");
        assert_eq!(gran.segment_granularity, "HOUR");
        assert_eq!(gran.query_granularity, "MINUTE");
        assert_eq!(gran.rollup, Some(false));

        assert_eq!(spec.io_config.stream, "my-kinesis-stream");
        assert_eq!(
            spec.io_config.endpoint.as_deref(),
            Some("http://localhost:4566")
        );
        assert_eq!(
            spec.io_config.aws_assumed_role_arn.as_deref(),
            Some("arn:aws:iam::123456789:role/druid-kinesis")
        );
        assert_eq!(spec.io_config.task_count, Some(3));
        assert_eq!(spec.io_config.use_earliest_sequence_number, Some(true));

        let tuning = spec.tuning_config.as_ref().expect("tuning");
        assert_eq!(tuning.max_rows_in_memory, Some(50000));
        assert_eq!(tuning.max_rows_per_segment, Some(3_000_000));
    }

    #[test]
    fn parse_minimal_spec() {
        let json = r#"{
            "type": "kinesis",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "ts"},
                "dimensionsSpec": {"dimensions": ["a"]}
            },
            "ioConfig": {
                "stream": "my-stream"
            }
        }"#;
        let spec: KinesisSupervisorSpec = serde_json::from_str(json).expect("parse");
        assert_eq!(spec.data_schema.data_source, "events");
        assert_eq!(spec.io_config.stream, "my-stream");
        assert!(spec.io_config.endpoint.is_none());
        assert!(spec.io_config.aws_assumed_role_arn.is_none());
        assert!(spec.tuning_config.is_none());
        assert!(spec.data_schema.granularity_spec.is_none());
        assert_eq!(spec.data_schema.timestamp_spec.format, "auto");
    }

    #[test]
    fn spec_roundtrip() {
        let spec: KinesisSupervisorSpec = serde_json::from_str(sample_spec_json()).expect("parse");
        let json = serde_json::to_string(&spec).expect("ser");
        let roundtripped: KinesisSupervisorSpec = serde_json::from_str(&json).expect("roundtrip");
        assert_eq!(roundtripped.spec_type, "kinesis");
        assert_eq!(roundtripped.io_config.stream, "my-kinesis-stream");
        assert_eq!(
            roundtripped.data_schema.data_source,
            spec.data_schema.data_source
        );
    }

    #[test]
    fn region_defaults_to_us_east_1_when_absent() {
        // Neither sample spec carries `region` — older stored specs and
        // minimal Druid specs must keep parsing, with the documented
        // default.
        let spec: KinesisSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse spec");
        assert_eq!(spec.io_config.region, "us-east-1");
    }

    #[test]
    fn region_parses_and_roundtrips_when_present() {
        let json = r#"{
            "type": "kinesis",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "ts"},
                "dimensionsSpec": {"dimensions": ["a"]}
            },
            "ioConfig": {
                "stream": "my-stream",
                "region": "ap-northeast-1"
            }
        }"#;
        let spec: KinesisSupervisorSpec = serde_json::from_str(json).expect("parse");
        assert_eq!(spec.io_config.region, "ap-northeast-1");
        let ser = serde_json::to_string(&spec).expect("ser");
        let back: KinesisSupervisorSpec = serde_json::from_str(&ser).expect("roundtrip");
        assert_eq!(back.io_config.region, "ap-northeast-1");
    }

    #[test]
    fn start_position_follows_use_earliest_sequence_number() {
        let mut spec: KinesisSupervisorSpec =
            serde_json::from_str(sample_spec_json()).expect("parse spec");
        // Sample sets useEarliestSequenceNumber=true.
        assert_eq!(spec.io_config.start_position(), StartPosition::TrimHorizon);
        spec.io_config.use_earliest_sequence_number = Some(false);
        assert_eq!(spec.io_config.start_position(), StartPosition::Latest);
        spec.io_config.use_earliest_sequence_number = None;
        assert_eq!(
            spec.io_config.start_position(),
            StartPosition::Latest,
            "Druid default is LATEST"
        );
    }

    #[test]
    fn dimension_entry_variants() {
        let json = r#"["user_id", {"name": "duration", "type": "long"}]"#;
        let dims: Vec<DimensionEntry> = serde_json::from_str(json).expect("parse");
        assert_eq!(dims.len(), 2);
        match &dims[0] {
            DimensionEntry::String(s) => assert_eq!(s, "user_id"),
            _ => panic!("expected string variant"),
        }
        match &dims[1] {
            DimensionEntry::Typed { name, dim_type } => {
                assert_eq!(name, "duration");
                assert_eq!(dim_type, "long");
            }
            _ => panic!("expected typed variant"),
        }
    }
}
