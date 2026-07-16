// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid metadata schema compatibility for FerroDruid.
//!
//! This crate provides strongly-typed Rust representations of the JSON
//! structures stored in Druid's metadata database (segments, rules,
//! supervisors, coordinator dynamic config).  It also exposes a
//! [`validate_druid_metadata`] function that checks whether a given SQLite
//! metadata database contains the expected Druid tables and is readable by
//! FerroDruid.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;

use ferrodruid_common::{DruidError, Result};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

// ---------------------------------------------------------------------------
// Segment payload
// ---------------------------------------------------------------------------

/// Druid segment payload (stored as JSON in `druid_segments.payload`).
///
/// Every segment written by a Druid indexer carries this structure.  The
/// exact shape is dictated by the Druid metadata protocol; field names use
/// `camelCase` on the wire and are mapped to idiomatic Rust names via serde
/// rename attributes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentPayload {
    /// Data source this segment belongs to.
    pub data_source: String,
    /// ISO-8601 interval in `start/end` form.
    pub interval: String,
    /// Version string (typically an ISO-8601 timestamp).
    pub version: String,
    /// Where the segment data lives on deep storage.
    pub load_spec: LoadSpec,
    /// Comma-separated list of dimension column names.
    pub dimensions: String,
    /// Comma-separated list of metric column names.
    pub metrics: String,
    /// Optional sharding specification (called `shardSpec` on the wire).
    #[serde(rename = "shardSpec")]
    pub sharding_spec: Option<ShardingSpec>,
    /// Binary format version of the segment file.
    pub binary_version: Option<i32>,
    /// Size of the segment in bytes.
    pub size: i64,
    /// Canonical segment identifier string.
    pub identifier: String,
}

// ---------------------------------------------------------------------------
// Load spec
// ---------------------------------------------------------------------------

/// Where a segment's data is stored on deep storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum LoadSpec {
    /// Segment stored on the local filesystem.
    #[serde(rename = "local")]
    Local {
        /// Filesystem path to the segment data.
        path: String,
    },
    /// Segment stored in an S3 zip archive.
    #[serde(rename = "s3_zip")]
    S3Zip {
        /// S3 bucket name.
        bucket: String,
        /// S3 object key.
        key: String,
    },
    /// Segment stored on HDFS.
    #[serde(rename = "hdfs")]
    Hdfs {
        /// HDFS path to the segment data.
        path: String,
    },
    /// Segment stored on Google Cloud Storage.
    #[serde(rename = "google")]
    Google {
        /// GCS bucket name.
        bucket: String,
        /// GCS object path.
        path: String,
    },
    /// Segment stored on Azure Blob Storage.
    #[serde(rename = "azure")]
    Azure {
        /// Azure container name.
        container: String,
        /// Azure blob path.
        blob: String,
    },
}

// ---------------------------------------------------------------------------
// Sharding spec
// ---------------------------------------------------------------------------

/// Describes how a segment is partitioned (the `shardSpec` field in the
/// segment payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ShardingSpec {
    /// Numbered sharding: a simple N-of-M scheme.
    #[serde(rename = "numbered")]
    Numbered {
        /// Zero-based partition index.
        #[serde(rename = "partitionNum")]
        partition_num: i32,
        /// Total partition count.
        partitions: i32,
    },
    /// Hash-based sharding on selected dimensions.
    #[serde(rename = "hashed")]
    Hashed {
        /// Zero-based partition index.
        #[serde(rename = "partitionNum")]
        partition_num: i32,
        /// Total partition count.
        partitions: i32,
        /// Subset of dimensions used for hashing (all if absent).
        #[serde(rename = "partitionDimensions")]
        partition_dimensions: Option<Vec<String>>,
    },
    /// Single-partition sharding (the entire interval in one segment).
    #[serde(rename = "single")]
    Single {
        /// Partition number (always 0).
        #[serde(rename = "partitionNum")]
        partition_num: i32,
    },
    /// Linear sharding (incrementally numbered, unknown total).
    #[serde(rename = "linear")]
    Linear {
        /// Zero-based partition index.
        #[serde(rename = "partitionNum")]
        partition_num: i32,
    },
    /// No explicit sharding.
    #[serde(rename = "none")]
    None {},
}

// ---------------------------------------------------------------------------
// Supervisor spec
// ---------------------------------------------------------------------------

/// Druid supervisor spec format (as stored in `druid_supervisors.payload`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorSpec {
    /// Supervisor type, e.g. `"kafka"`, `"kinesis"`.
    #[serde(rename = "type")]
    pub spec_type: String,
    /// The full supervisor specification body (schema varies by type).
    pub spec: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Load rules
// ---------------------------------------------------------------------------

/// Druid load / drop / broadcast rule format (as stored in
/// `druid_rules.payload`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DruidRule {
    /// Retain segments forever with the given replication.
    #[serde(rename = "loadForever")]
    LoadForever {
        /// Tier name → replica count.
        #[serde(rename = "tieredReplicants")]
        tier_replicants: HashMap<String, usize>,
    },
    /// Retain segments whose interval falls within the given interval.
    #[serde(rename = "loadByInterval")]
    LoadByInterval {
        /// ISO-8601 interval to match.
        interval: String,
        /// Tier name → replica count.
        #[serde(rename = "tieredReplicants")]
        tier_replicants: HashMap<String, usize>,
    },
    /// Retain segments whose interval falls within a sliding period window.
    #[serde(rename = "loadByPeriod")]
    LoadByPeriod {
        /// ISO-8601 period, e.g. `"P1M"`.
        period: String,
        /// Whether to include segments in the future.
        #[serde(rename = "includeFuture", default)]
        include_future: bool,
        /// Tier name → replica count.
        #[serde(rename = "tieredReplicants")]
        tier_replicants: HashMap<String, usize>,
    },
    /// Drop all segments unconditionally.
    #[serde(rename = "dropForever")]
    DropForever {},
    /// Drop segments whose interval matches.
    #[serde(rename = "dropByInterval")]
    DropByInterval {
        /// ISO-8601 interval to match.
        interval: String,
    },
    /// Drop segments older than the given period.
    #[serde(rename = "dropByPeriod")]
    DropByPeriod {
        /// ISO-8601 period, e.g. `"P6M"`.
        period: String,
        /// Whether to include segments in the future.
        #[serde(rename = "includeFuture", default)]
        include_future: bool,
    },
    /// Broadcast segments to all servers (for lookup data sources).
    #[serde(rename = "broadcastForever")]
    BroadcastForever {},
}

// ---------------------------------------------------------------------------
// Coordinator dynamic config
// ---------------------------------------------------------------------------

/// Druid coordinator dynamic configuration (stored in `druid_config` under
/// the key `"coordinator.config"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinatorDynamicConfig {
    /// Maximum number of segments to move per coordinator run.
    #[serde(default)]
    pub max_segments_to_move: i32,
    /// Throttle limit for replication (max segments replicating concurrently).
    #[serde(default)]
    pub replication_throttle_limit: i32,
    /// Number of threads for balancer cost computation.
    #[serde(default)]
    pub balancer_compute_threads: i32,
    /// Data sources whose unused segments may be killed.
    #[serde(default)]
    pub kill_data_source_whitelist: Vec<String>,
    /// Data sources whose pending segments should not be killed.
    #[serde(default)]
    pub kill_pending_segments_skip_list: Vec<String>,
    /// Maximum segments allowed in a historical's loading queue.
    #[serde(default)]
    pub max_segments_in_node_loading_queue: i32,
    /// Byte limit for segment merge tasks.
    #[serde(default)]
    pub merge_bytes_limit: i64,
    /// Segment count limit for segment merge tasks.
    #[serde(default)]
    pub merge_segments_limit: i32,
    /// Whether smart (cost-based) segment loading is enabled.
    #[serde(default)]
    pub smart_segment_loading: bool,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// The set of tables that a Druid-compatible metadata database must contain.
const EXPECTED_TABLES: &[&str] = &[
    "druid_segments",
    "druid_rules",
    "druid_supervisors",
    "druid_config",
    "druid_audit",
    "druid_tasklogs",
    "druid_tasklocks",
];

/// Result of validating a Druid metadata database.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// Tables found in the database.
    pub tables_found: Vec<String>,
    /// Expected tables that are missing.
    pub tables_missing: Vec<String>,
    /// Number of segment rows in `druid_segments`.
    pub segment_count: usize,
    /// Number of distinct data sources across segments.
    pub datasource_count: usize,
    /// `true` when all expected tables are present.
    pub is_compatible: bool,
}

/// Validate that a SQLite database contains the Druid metadata schema.
///
/// This queries `sqlite_master` for the expected table names and counts
/// segments / data sources to populate the report.
pub async fn validate_druid_metadata(pool: &SqlitePool) -> Result<ValidationReport> {
    // Discover which tables exist.
    let rows = sqlx::query("SELECT name FROM sqlite_master WHERE type = 'table'")
        .fetch_all(pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("list tables: {e}")))?;

    let existing: Vec<String> = rows
        .iter()
        .map(|r| r.try_get::<String, _>("name").unwrap_or_default())
        .collect();

    let mut tables_found = Vec::new();
    let mut tables_missing = Vec::new();
    for &tbl in EXPECTED_TABLES {
        if existing.iter().any(|n| n == tbl) {
            tables_found.push(tbl.to_string());
        } else {
            tables_missing.push(tbl.to_string());
        }
    }

    let is_compatible = tables_missing.is_empty();

    // Count segments and data sources (only if the table exists).
    let (segment_count, datasource_count) = if tables_found.contains(&"druid_segments".to_string())
    {
        let seg_row = sqlx::query("SELECT COUNT(*) AS cnt FROM druid_segments")
            .fetch_one(pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("count segments: {e}")))?;
        let seg_count: i64 = seg_row
            .try_get("cnt")
            .map_err(|e| DruidError::Metadata(format!("decode count: {e}")))?;

        let ds_row = sqlx::query("SELECT COUNT(DISTINCT dataSource) AS cnt FROM druid_segments")
            .fetch_one(pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("count data sources: {e}")))?;
        let ds_count: i64 = ds_row
            .try_get("cnt")
            .map_err(|e| DruidError::Metadata(format!("decode count: {e}")))?;

        (seg_count as usize, ds_count as usize)
    } else {
        (0, 0)
    };

    Ok(ValidationReport {
        tables_found,
        tables_missing,
        segment_count,
        datasource_count,
        is_compatible,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -- SegmentPayload -------------------------------------------------------

    #[test]
    fn parse_real_druid_segment_payload_local() {
        let json = r#"{
            "dataSource": "wikipedia",
            "interval": "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z",
            "version": "2024-01-01T00:00:00.000Z",
            "loadSpec": {
                "type": "local",
                "path": "/segments/wikipedia/2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z/2024-01-01T00:00:00.000Z/0/index.zip"
            },
            "dimensions": "page,user,language,city",
            "metrics": "count,added,deleted",
            "shardSpec": {"type": "numbered", "partitionNum": 0, "partitions": 1},
            "binaryVersion": 9,
            "size": 12345678,
            "identifier": "wikipedia_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_2024-01-01T00:00:00.000Z"
        }"#;
        let payload: SegmentPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.data_source, "wikipedia");
        assert_eq!(
            payload.interval,
            "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"
        );
        assert_eq!(payload.version, "2024-01-01T00:00:00.000Z");
        assert_eq!(payload.dimensions, "page,user,language,city");
        assert_eq!(payload.metrics, "count,added,deleted");
        assert_eq!(payload.binary_version, Some(9));
        assert_eq!(payload.size, 12345678);
        assert!(matches!(payload.load_spec, LoadSpec::Local { .. }));
        if let LoadSpec::Local { path } = &payload.load_spec {
            assert!(path.ends_with("index.zip"));
        }
        assert!(payload.sharding_spec.is_some());
        if let Some(ShardingSpec::Numbered {
            partition_num,
            partitions,
        }) = &payload.sharding_spec
        {
            assert_eq!(*partition_num, 0);
            assert_eq!(*partitions, 1);
        } else {
            panic!("expected Numbered sharding spec");
        }
    }

    #[test]
    fn parse_segment_payload_s3_zip() {
        let json = r#"{
            "dataSource": "clicks",
            "interval": "2024-06-01T00:00:00.000Z/2024-06-02T00:00:00.000Z",
            "version": "2024-06-01T12:00:00.000Z",
            "loadSpec": {
                "type": "s3_zip",
                "bucket": "druid-deep-storage",
                "key": "clicks/2024-06-01/0/index.zip"
            },
            "dimensions": "url,referrer,country",
            "metrics": "count,duration",
            "shardSpec": {"type": "hashed", "partitionNum": 2, "partitions": 4, "partitionDimensions": ["country"]},
            "binaryVersion": 9,
            "size": 98765432,
            "identifier": "clicks_2024-06-01T00:00:00.000Z_2024-06-02T00:00:00.000Z_2024-06-01T12:00:00.000Z_2"
        }"#;
        let payload: SegmentPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.data_source, "clicks");
        if let LoadSpec::S3Zip { bucket, key } = &payload.load_spec {
            assert_eq!(bucket, "druid-deep-storage");
            assert_eq!(key, "clicks/2024-06-01/0/index.zip");
        } else {
            panic!("expected S3Zip load spec");
        }
        if let Some(ShardingSpec::Hashed {
            partition_num,
            partitions,
            partition_dimensions,
        }) = &payload.sharding_spec
        {
            assert_eq!(*partition_num, 2);
            assert_eq!(*partitions, 4);
            assert_eq!(
                partition_dimensions.as_ref().unwrap(),
                &vec!["country".to_string()]
            );
        } else {
            panic!("expected Hashed sharding spec");
        }
    }

    #[test]
    fn parse_segment_payload_hdfs() {
        let json = r#"{
            "dataSource": "events",
            "interval": "2024-03-15T00:00:00.000Z/2024-03-16T00:00:00.000Z",
            "version": "2024-03-15T00:00:00.000Z",
            "loadSpec": {
                "type": "hdfs",
                "path": "hdfs://namenode:8020/druid/segments/events/2024-03-15/0/index.zip"
            },
            "dimensions": "event_type,user_id",
            "metrics": "count",
            "binaryVersion": 9,
            "size": 5555555,
            "identifier": "events_2024-03-15_v1_0"
        }"#;
        let payload: SegmentPayload = serde_json::from_str(json).unwrap();
        assert!(payload.sharding_spec.is_none());
        if let LoadSpec::Hdfs { path } = &payload.load_spec {
            assert!(path.starts_with("hdfs://"));
        } else {
            panic!("expected Hdfs load spec");
        }
    }

    #[test]
    fn parse_segment_payload_google_loadspec() {
        let json = r#"{
            "dataSource": "logs",
            "interval": "2024-04-01T00:00:00.000Z/2024-04-02T00:00:00.000Z",
            "version": "2024-04-01T00:00:00.000Z",
            "loadSpec": {
                "type": "google",
                "bucket": "druid-gcs-bucket",
                "path": "logs/2024-04-01/0/index.zip"
            },
            "dimensions": "level,service",
            "metrics": "count",
            "size": 1234567,
            "identifier": "logs_2024-04-01_v1_0"
        }"#;
        let payload: SegmentPayload = serde_json::from_str(json).unwrap();
        if let LoadSpec::Google { bucket, path } = &payload.load_spec {
            assert_eq!(bucket, "druid-gcs-bucket");
            assert_eq!(path, "logs/2024-04-01/0/index.zip");
        } else {
            panic!("expected Google load spec");
        }
    }

    #[test]
    fn parse_segment_payload_azure_loadspec() {
        let json = r#"{
            "dataSource": "metrics",
            "interval": "2024-05-01T00:00:00.000Z/2024-05-02T00:00:00.000Z",
            "version": "2024-05-01T00:00:00.000Z",
            "loadSpec": {
                "type": "azure",
                "container": "druid-container",
                "blob": "metrics/2024-05-01/0/index.zip"
            },
            "dimensions": "host,metric_name",
            "metrics": "value",
            "size": 7654321,
            "identifier": "metrics_2024-05-01_v1_0"
        }"#;
        let payload: SegmentPayload = serde_json::from_str(json).unwrap();
        if let LoadSpec::Azure { container, blob } = &payload.load_spec {
            assert_eq!(container, "druid-container");
            assert_eq!(blob, "metrics/2024-05-01/0/index.zip");
        } else {
            panic!("expected Azure load spec");
        }
    }

    // -- ShardingSpec variants ------------------------------------------------

    #[test]
    fn parse_sharding_spec_single() {
        let json = r#"{"type": "single", "partitionNum": 0}"#;
        let spec: ShardingSpec = serde_json::from_str(json).unwrap();
        assert!(matches!(spec, ShardingSpec::Single { partition_num: 0 }));
    }

    #[test]
    fn parse_sharding_spec_linear() {
        let json = r#"{"type": "linear", "partitionNum": 3}"#;
        let spec: ShardingSpec = serde_json::from_str(json).unwrap();
        assert!(matches!(spec, ShardingSpec::Linear { partition_num: 3 }));
    }

    #[test]
    fn parse_sharding_spec_none() {
        let json = r#"{"type": "none"}"#;
        let spec: ShardingSpec = serde_json::from_str(json).unwrap();
        assert!(matches!(spec, ShardingSpec::None {}));
    }

    // -- SupervisorSpec -------------------------------------------------------

    #[test]
    fn parse_supervisor_spec_kafka() {
        let json = r#"{
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": "wiki-events",
                    "timestampSpec": {"column": "timestamp", "format": "auto"},
                    "dimensionsSpec": {"dimensions": ["page", "user"]}
                },
                "ioConfig": {
                    "topic": "wiki-events",
                    "consumerProperties": {"bootstrap.servers": "kafka:9092"},
                    "taskCount": 1,
                    "replicas": 1
                },
                "tuningConfig": {"type": "kafka", "maxRowsPerSegment": 5000000}
            }
        }"#;
        let spec: SupervisorSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.spec_type, "kafka");
        assert_eq!(spec.spec["ioConfig"]["topic"], "wiki-events");
    }

    #[test]
    fn parse_supervisor_spec_kinesis() {
        let json = r#"{
            "type": "kinesis",
            "spec": {
                "dataSchema": {"dataSource": "events"},
                "ioConfig": {
                    "stream": "events-stream",
                    "endpoint": "kinesis.us-east-1.amazonaws.com"
                }
            }
        }"#;
        let spec: SupervisorSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.spec_type, "kinesis");
    }

    // -- DruidRule variants ---------------------------------------------------

    #[test]
    fn parse_rule_load_forever() {
        let json = r#"{
            "type": "loadForever",
            "tieredReplicants": {"_default_tier": 2, "hot": 1}
        }"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::LoadForever { tier_replicants } = &rule {
            assert_eq!(tier_replicants["_default_tier"], 2);
            assert_eq!(tier_replicants["hot"], 1);
        } else {
            panic!("expected LoadForever");
        }
    }

    #[test]
    fn parse_rule_load_by_interval() {
        let json = r#"{
            "type": "loadByInterval",
            "interval": "2024-01-01/2024-07-01",
            "tieredReplicants": {"_default_tier": 1}
        }"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::LoadByInterval {
            interval,
            tier_replicants,
        } = &rule
        {
            assert_eq!(interval, "2024-01-01/2024-07-01");
            assert_eq!(tier_replicants["_default_tier"], 1);
        } else {
            panic!("expected LoadByInterval");
        }
    }

    #[test]
    fn parse_rule_load_by_period() {
        let json = r#"{
            "type": "loadByPeriod",
            "period": "P3M",
            "includeFuture": true,
            "tieredReplicants": {"_default_tier": 2}
        }"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::LoadByPeriod {
            period,
            include_future,
            tier_replicants,
        } = &rule
        {
            assert_eq!(period, "P3M");
            assert!(include_future);
            assert_eq!(tier_replicants["_default_tier"], 2);
        } else {
            panic!("expected LoadByPeriod");
        }
    }

    #[test]
    fn parse_rule_load_by_period_default_include_future() {
        let json = r#"{
            "type": "loadByPeriod",
            "period": "P1M",
            "tieredReplicants": {"_default_tier": 1}
        }"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::LoadByPeriod { include_future, .. } = &rule {
            assert!(!include_future, "includeFuture should default to false");
        } else {
            panic!("expected LoadByPeriod");
        }
    }

    #[test]
    fn parse_rule_drop_forever() {
        let json = r#"{"type": "dropForever"}"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        assert!(matches!(rule, DruidRule::DropForever {}));
    }

    #[test]
    fn parse_rule_drop_by_interval() {
        let json = r#"{"type": "dropByInterval", "interval": "2020-01-01/2021-01-01"}"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::DropByInterval { interval } = &rule {
            assert_eq!(interval, "2020-01-01/2021-01-01");
        } else {
            panic!("expected DropByInterval");
        }
    }

    #[test]
    fn parse_rule_drop_by_period() {
        let json = r#"{"type": "dropByPeriod", "period": "P6M", "includeFuture": false}"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::DropByPeriod {
            period,
            include_future,
        } = &rule
        {
            assert_eq!(period, "P6M");
            assert!(!include_future);
        } else {
            panic!("expected DropByPeriod");
        }
    }

    #[test]
    fn parse_rule_broadcast_forever() {
        let json = r#"{"type": "broadcastForever"}"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        assert!(matches!(rule, DruidRule::BroadcastForever {}));
    }

    #[test]
    fn parse_rule_array() {
        let json = r#"[
            {"type": "loadByPeriod", "period": "P1M", "tieredReplicants": {"_default_tier": 2}},
            {"type": "dropForever"}
        ]"#;
        let rules: Vec<DruidRule> = serde_json::from_str(json).unwrap();
        assert_eq!(rules.len(), 2);
        assert!(matches!(rules[0], DruidRule::LoadByPeriod { .. }));
        assert!(matches!(rules[1], DruidRule::DropForever {}));
    }

    // -- CoordinatorDynamicConfig ---------------------------------------------

    #[test]
    fn parse_coordinator_dynamic_config_full() {
        let json = r#"{
            "maxSegmentsToMove": 100,
            "replicationThrottleLimit": 500,
            "balancerComputeThreads": 4,
            "killDataSourceWhitelist": ["old_events"],
            "killPendingSegmentsSkipList": ["important_ds"],
            "maxSegmentsInNodeLoadingQueue": 500,
            "mergeBytesLimit": 536870912,
            "mergeSegmentsLimit": 100,
            "smartSegmentLoading": true
        }"#;
        let cfg: CoordinatorDynamicConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_segments_to_move, 100);
        assert_eq!(cfg.replication_throttle_limit, 500);
        assert_eq!(cfg.balancer_compute_threads, 4);
        assert_eq!(cfg.kill_data_source_whitelist, vec!["old_events"]);
        assert_eq!(cfg.kill_pending_segments_skip_list, vec!["important_ds"]);
        assert_eq!(cfg.max_segments_in_node_loading_queue, 500);
        assert_eq!(cfg.merge_bytes_limit, 536_870_912);
        assert_eq!(cfg.merge_segments_limit, 100);
        assert!(cfg.smart_segment_loading);
    }

    #[test]
    fn parse_coordinator_dynamic_config_defaults() {
        let json = r#"{}"#;
        let cfg: CoordinatorDynamicConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_segments_to_move, 0);
        assert_eq!(cfg.replication_throttle_limit, 0);
        assert!(cfg.kill_data_source_whitelist.is_empty());
        assert!(!cfg.smart_segment_loading);
    }

    // -- Round-trip serialization ---------------------------------------------

    #[test]
    fn segment_payload_round_trip() {
        let original = SegmentPayload {
            data_source: "test_ds".to_string(),
            interval: "2024-01-01/2024-01-02".to_string(),
            version: "2024-01-01T00:00:00.000Z".to_string(),
            load_spec: LoadSpec::Local {
                path: "/seg/test/0/index.zip".to_string(),
            },
            dimensions: "dim1,dim2".to_string(),
            metrics: "met1".to_string(),
            sharding_spec: Some(ShardingSpec::Single { partition_num: 0 }),
            binary_version: Some(9),
            size: 1024,
            identifier: "test_ds_2024-01-01_v1_0".to_string(),
        };
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: SegmentPayload = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.data_source, original.data_source);
        assert_eq!(deserialized.size, original.size);
        assert_eq!(deserialized.identifier, original.identifier);
    }

    #[test]
    fn druid_rule_round_trip() {
        let rules = vec![
            DruidRule::LoadByPeriod {
                period: "P1M".to_string(),
                include_future: true,
                tier_replicants: HashMap::from([("_default_tier".to_string(), 2)]),
            },
            DruidRule::DropForever {},
        ];
        let serialized = serde_json::to_string(&rules).unwrap();
        let deserialized: Vec<DruidRule> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.len(), 2);
    }

    // -- Serialized field names verify camelCase on the wire -------------------

    #[test]
    fn segment_payload_field_names_are_camel_case() {
        let payload = SegmentPayload {
            data_source: "ds".to_string(),
            interval: "2024-01-01/2024-01-02".to_string(),
            version: "v1".to_string(),
            load_spec: LoadSpec::Local {
                path: "/x".to_string(),
            },
            dimensions: "d".to_string(),
            metrics: "m".to_string(),
            sharding_spec: None,
            binary_version: None,
            size: 0,
            identifier: "id".to_string(),
        };
        let val: serde_json::Value = serde_json::to_value(&payload).unwrap();
        let obj = val.as_object().unwrap();
        assert!(obj.contains_key("dataSource"), "expected camelCase key");
        assert!(obj.contains_key("loadSpec"), "expected camelCase key");
        assert!(
            !obj.contains_key("data_source"),
            "should not have snake_case key"
        );
    }

    // -- validate_druid_metadata ----------------------------------------------

    #[tokio::test]
    async fn validate_empty_db() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        let report = validate_druid_metadata(&pool).await.unwrap();
        assert!(!report.is_compatible);
        assert_eq!(report.tables_missing.len(), 7);
        assert_eq!(report.tables_found.len(), 0);
    }

    #[tokio::test]
    async fn validate_initialized_db() {
        let store = ferrodruid_metadata::MetadataStore::new_in_memory()
            .await
            .unwrap();
        store.initialize().await.unwrap();

        // The MetadataStore uses a single-connection pool for in-memory DBs,
        // so we need to get its pool.  Since we cannot access it directly, we
        // create a second pool on a file-backed temp DB.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let file_store = ferrodruid_metadata::MetadataStore::new_sqlite(db_path.to_str().unwrap())
            .await
            .unwrap();
        file_store.initialize().await.unwrap();

        // Insert a segment so counts are non-zero.
        let seg = ferrodruid_metadata::SegmentMetadataRow {
            id: "seg1".to_string(),
            data_source: "wiki".to_string(),
            created_date: "2024-01-01T00:00:00Z".to_string(),
            start: "2024-01-01T00:00:00Z".to_string(),
            end: "2024-01-02T00:00:00Z".to_string(),
            version: "v1".to_string(),
            used: true,
            payload: json!({}),
        };
        file_store.insert_segment(&seg).await.unwrap();

        // Now validate via a raw pool.
        let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}", db_path.display()))
            .await
            .unwrap();
        let report = validate_druid_metadata(&pool).await.unwrap();
        assert!(report.is_compatible);
        assert_eq!(report.tables_missing.len(), 0);
        assert_eq!(report.tables_found.len(), 7);
        assert_eq!(report.segment_count, 1);
        assert_eq!(report.datasource_count, 1);
    }
}
