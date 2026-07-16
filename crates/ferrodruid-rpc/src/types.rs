// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wire types exchanged across the cross-role HTTP boundary.
//!
//! The shapes here are intentionally **subset-aligned** with Apache
//! Druid's REST surface so an existing Druid client can hit a
//! FerroDruid broker / middleManager and get a recognisable shape
//! back. They are not the full Druid schemas — the missing fields
//! (e.g. `intervals`, `granularity`, full `IngestSpec`) will land in
//! W4 alongside the real query / ingestion execution.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A SQL query forwarded from the router to a broker.
///
/// Mirrors the shape Druid's `POST /druid/v2/sql` accepts: a `query`
/// string, an optional `resultFormat`, and an optional `context`
/// blob. The `tier_hint` field is a FerroDruid extension used by the
/// router to bias broker selection — Druid clients that do not set it
/// get the default tier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SqlQuery {
    /// SQL text to execute. Required.
    pub query: String,
    /// Desired result envelope. Defaults to [`SqlResultFormat::Object`]
    /// to match Druid's default.
    #[serde(rename = "resultFormat", default)]
    pub result_format: SqlResultFormat,
    /// Opaque per-query context (timeouts, query ID overrides, etc.).
    /// Forwarded verbatim to the broker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
    /// FerroDruid extension: which tier the router should prefer when
    /// picking a broker. Druid clients omit this; the router fills in
    /// [`TierHint::Default`] when absent.
    #[serde(rename = "tierHint", default, skip_serializing_if = "Option::is_none")]
    pub tier_hint: Option<TierHint>,
}

impl SqlQuery {
    /// Convenience constructor for tests and binary entry points that
    /// have a literal SQL string and no extra context.
    #[must_use]
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            result_format: SqlResultFormat::Object,
            context: None,
            tier_hint: None,
        }
    }
}

/// Druid-compatible result-envelope hint.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SqlResultFormat {
    /// One JSON object per row, keyed by column name. Druid's default.
    #[default]
    Object,
    /// One JSON array per row, columns in projection order.
    Array,
    /// Single CSV document.
    Csv,
}

/// Tier hint propagated from the router to the broker selection
/// policy. The default tier is the only tier that exists today;
/// `Hot` / `Cold` are reserved for the v1.0 multi-tier topology.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TierHint {
    /// Use whichever broker is closest / least loaded. Today this is
    /// always the only broker the router knows about.
    #[default]
    Default,
    /// Prefer brokers serving the hot tier.
    Hot,
    /// Prefer brokers serving the cold tier.
    Cold,
}

/// Response shape returned by the broker for a `POST /druid/v2/sql`
/// call.
///
/// In Wave 39.HH the broker echoes the query back as a single-row
/// result so the cross-process wire is observable end to end. Real
/// execution lands in W4.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SqlResponse {
    /// Per-query identifier the broker assigns. The router relays
    /// this to the client so Druid-aligned correlation works.
    #[serde(rename = "queryId")]
    pub query_id: String,
    /// Column names in projection order.
    pub columns: Vec<String>,
    /// One row per element; each row's length matches `columns.len()`.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Wall-clock time the broker reports it spent on the query.
    /// Wave 39.HH always reports 0 since execution is canned.
    #[serde(rename = "elapsedMs", default)]
    pub elapsed_ms: u64,
}

/// Lightweight broker introspection payload returned by
/// `GET /druid/v2/info`. Used by the router to discover broker tier
/// and version without parsing the full Druid metadata API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerInfo {
    /// FerroDruid version this broker is running.
    pub version: String,
    /// Logical role (always `"broker"` for this endpoint, but kept on
    /// the wire so a router that hits the wrong role gets a clear
    /// signal instead of a parse error).
    pub role: String,
    /// Tier this broker serves. Today every broker reports
    /// `"default"`; multi-tier discovery is W4+.
    pub tier: String,
    /// Cluster-unique broker identifier.
    #[serde(rename = "brokerId")]
    pub broker_id: String,
}

/// Indexing task spec dispatched from the overlord to a middleManager.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskAssignment {
    /// Stable task identifier. The overlord assigns this before
    /// dispatch so the middleManager and the overlord agree on the
    /// task name even if the network call retries.
    #[serde(rename = "taskId")]
    pub task_id: String,
    /// Druid-style task type label. Wave 39.HH only uses this for
    /// logging; W4 wires it to a real executor registry.
    #[serde(rename = "taskKind")]
    pub task_kind: TaskKind,
    /// Datasource the task targets. Mirrors Druid's `dataSource`
    /// field on `IngestSpec`.
    #[serde(rename = "dataSource")]
    pub data_source: String,
    /// Opaque task body. Today the middleManager only stores it; W4
    /// will type-narrow this into the real ingestion spec graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<serde_json::Value>,
}

impl TaskAssignment {
    /// Generate a fresh task id and wrap the supplied spec.
    #[must_use]
    pub fn new(task_kind: TaskKind, data_source: impl Into<String>) -> Self {
        Self {
            task_id: format!("task-{}", Uuid::new_v4()),
            task_kind,
            data_source: data_source.into(),
            spec: None,
        }
    }
}

/// Druid-compatible indexing task type labels supported by Wave 39.HH.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    /// Batch index task (Druid `index_parallel` family analogue).
    Index,
    /// Streaming Kafka ingestion task.
    Kafka,
    /// Streaming Kinesis ingestion task.
    Kinesis,
    /// Compaction task (segment merge).
    Compact,
}

/// Lifecycle state of a dispatched task. Mirrors Druid's
/// `TaskStatus.Status` enum.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    /// Accepted by the middleManager but the worker has not started.
    Pending,
    /// Worker is actively executing the task.
    Running,
    /// Task finished successfully.
    Success,
    /// Task finished with a failure.
    Failed,
    /// Task is unknown to this middleManager (e.g. wrong worker
    /// queried, or the worker restarted and lost the task).
    Unknown,
}

/// Status report returned by the middleManager when the overlord
/// polls `GET /druid/v1/middlemanager/task/{id}/status`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskStatus {
    /// Task identifier the report applies to.
    #[serde(rename = "taskId")]
    pub task_id: String,
    /// Lifecycle state.
    pub state: TaskState,
    /// Human-readable status message — empty when no detail is
    /// available. Capped at 1 KiB on the server side so a misbehaving
    /// task cannot bloat the overlord's memory.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// Per-segment query fragment forwarded from the broker to a
/// historical via `POST /druid/v2/native`. The shape is
/// subset-aligned with Druid's native query JSON: a `query` body, a
/// target `segment_id` the historical is expected to be serving, and
/// an optional opaque `context` blob for per-query options.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentQuery {
    /// Native / SQL query body. Today the Wave 40.LL handler echoes
    /// this back; W5 wires it to `ferrodruid-query` for real
    /// per-segment execution.
    pub query: String,
    /// Segment identifier the broker is fanning out to. The
    /// historical asserts it has the segment loaded before answering;
    /// in Wave 40.LL the assertion is stubbed (any segment id is
    /// accepted).
    #[serde(rename = "segmentId")]
    pub segment_id: String,
    /// Opaque per-query context (timeouts, query ID overrides, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
}

impl SegmentQuery {
    /// Convenience constructor for tests and binary entry points.
    #[must_use]
    pub fn new(query: impl Into<String>, segment_id: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            segment_id: segment_id.into(),
            context: None,
        }
    }
}

/// Response shape returned by the historical for a
/// `POST /druid/v2/native` call.
///
/// In Wave 40.LL the historical echoes the query back as a single-row
/// result so the cross-process wire is observable end to end. Real
/// per-segment execution lands in W5.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentQueryResponse {
    /// Segment identifier this response applies to.
    #[serde(rename = "segmentId")]
    pub segment_id: String,
    /// One row per element; each row's length matches the projection
    /// the broker requested.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Wall-clock time the historical reports it spent on the query.
    /// Wave 40.LL always reports 0 since execution is canned.
    #[serde(rename = "elapsedMs", default)]
    pub elapsed_ms: u64,
}

/// Segment-load command dispatched from the coordinator to a
/// historical via `POST /druid/v1/historical/load`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentLoadCommand {
    /// Stable segment identifier (typically `<dataSource>_<interval>_<version>_<partitionNum>`
    /// in Druid; FerroDruid mirrors the shape).
    #[serde(rename = "segmentId")]
    pub segment_id: String,
    /// Datasource the segment belongs to.
    #[serde(rename = "dataSource")]
    pub data_source: String,
    /// Deep-storage URI the historical fetches from. Today the Wave
    /// 40.LL handler accepts the URI but does not actually fetch; W5
    /// wires the real `ferrodruid-deep-storage` integration.
    #[serde(rename = "deepStorageUri")]
    pub deep_storage_uri: String,
    /// Tier the historical is being asked to serve this segment from.
    /// Defaults to `"default"` when omitted.
    #[serde(default = "default_tier", skip_serializing_if = "is_default_tier")]
    pub tier: String,
    /// Opaque coordinator-side metadata attached to the load command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl SegmentLoadCommand {
    /// Convenience constructor for tests and binary entry points.
    #[must_use]
    pub fn new(
        segment_id: impl Into<String>,
        data_source: impl Into<String>,
        deep_storage_uri: impl Into<String>,
    ) -> Self {
        Self {
            segment_id: segment_id.into(),
            data_source: data_source.into(),
            deep_storage_uri: deep_storage_uri.into(),
            tier: "default".into(),
            metadata: None,
        }
    }
}

fn default_tier() -> String {
    "default".into()
}

fn is_default_tier(t: &String) -> bool {
    t == "default"
}

/// Segment-drop command dispatched from the coordinator to a
/// historical via `POST /druid/v1/historical/drop`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentDropCommand {
    /// Segment identifier to drop.
    #[serde(rename = "segmentId")]
    pub segment_id: String,
}

impl SegmentDropCommand {
    /// Convenience constructor.
    #[must_use]
    pub fn new(segment_id: impl Into<String>) -> Self {
        Self {
            segment_id: segment_id.into(),
        }
    }
}

/// Lifecycle state of a segment from the historical's point of view.
/// Mirrors the subset of Druid's `SegmentLoadInfo` states FerroDruid
/// exposes today.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SegmentLoadState {
    /// Coordinator has issued a load command; historical is fetching
    /// the segment from deep storage.
    Loading,
    /// Segment is fully loaded and queryable.
    Loaded,
    /// Segment was loaded but a drop command has since landed.
    Dropped,
    /// Load attempt failed (e.g. deep-storage I/O error).
    Failed,
    /// Historical has never been told about this segment.
    Unknown,
}

/// Status report returned by the historical for the `load` / `drop`
/// commands, and embedded in the `loadstatus` table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadStatusReport {
    /// Segment identifier this report applies to.
    #[serde(rename = "segmentId")]
    pub segment_id: String,
    /// Lifecycle state.
    pub state: SegmentLoadState,
    /// Human-readable status message. Empty when no detail is
    /// available. Capped at 1 KiB on the server side.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_query_round_trips_via_serde_json() {
        let q = SqlQuery::new("SELECT 1");
        let s = serde_json::to_string(&q).expect("serialize");
        let back: SqlQuery = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(back, q);
    }

    #[test]
    fn sql_query_default_format_is_object() {
        let q = SqlQuery::new("SELECT 1");
        assert_eq!(q.result_format, SqlResultFormat::Object);
    }

    #[test]
    fn sql_response_carries_query_id_and_rows() {
        let r = SqlResponse {
            query_id: "q1".into(),
            columns: vec!["a".into()],
            rows: vec![vec![serde_json::Value::from(1)]],
            elapsed_ms: 0,
        };
        let s = serde_json::to_string(&r).expect("serialize");
        assert!(s.contains("\"queryId\":\"q1\""), "{s}");
        assert!(s.contains("\"columns\":[\"a\"]"), "{s}");
    }

    #[test]
    fn task_assignment_generates_unique_id() {
        let a = TaskAssignment::new(TaskKind::Index, "ds");
        let b = TaskAssignment::new(TaskKind::Index, "ds");
        assert_ne!(a.task_id, b.task_id);
        assert!(a.task_id.starts_with("task-"));
    }

    #[test]
    fn task_status_omits_empty_message() {
        let s = TaskStatus {
            task_id: "t1".into(),
            state: TaskState::Pending,
            message: String::new(),
        };
        let j = serde_json::to_string(&s).expect("serialize");
        assert!(!j.contains("message"), "{j}");
    }

    #[test]
    fn task_state_enum_round_trips_lowercase() {
        for state in [
            TaskState::Pending,
            TaskState::Running,
            TaskState::Success,
            TaskState::Failed,
            TaskState::Unknown,
        ] {
            let s = serde_json::to_string(&state).expect("ser");
            let back: TaskState = serde_json::from_str(&s).expect("de");
            assert_eq!(back, state);
        }
    }

    #[test]
    fn tier_hint_defaults_to_default() {
        assert_eq!(TierHint::default(), TierHint::Default);
    }

    #[test]
    fn segment_query_round_trips_via_serde_json() {
        let q = SegmentQuery::new("SELECT 1", "seg-1");
        let s = serde_json::to_string(&q).expect("serialize");
        let back: SegmentQuery = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(back, q);
        assert!(s.contains("\"segmentId\":\"seg-1\""), "{s}");
    }

    #[test]
    fn segment_load_command_default_tier_is_omitted_on_wire() {
        let cmd = SegmentLoadCommand::new("seg-1", "ds-A", "deepstore://A/seg-1");
        let s = serde_json::to_string(&cmd).expect("serialize");
        // Default tier should not appear in the wire body.
        assert!(!s.contains("\"tier\""), "{s}");
        // But round-trip should restore the default value.
        let back: SegmentLoadCommand = serde_json::from_str(&s).expect("de");
        assert_eq!(back.tier, "default");
        assert_eq!(back.segment_id, "seg-1");
    }

    #[test]
    fn segment_load_command_custom_tier_is_emitted() {
        let mut cmd = SegmentLoadCommand::new("seg-1", "ds-A", "deepstore://A/seg-1");
        cmd.tier = "hot".into();
        let s = serde_json::to_string(&cmd).expect("serialize");
        assert!(s.contains("\"tier\":\"hot\""), "{s}");
    }

    #[test]
    fn segment_load_state_round_trips_lowercase() {
        for state in [
            SegmentLoadState::Loading,
            SegmentLoadState::Loaded,
            SegmentLoadState::Dropped,
            SegmentLoadState::Failed,
            SegmentLoadState::Unknown,
        ] {
            let s = serde_json::to_string(&state).expect("ser");
            let back: SegmentLoadState = serde_json::from_str(&s).expect("de");
            assert_eq!(back, state);
        }
    }

    #[test]
    fn load_status_report_omits_empty_message() {
        let r = LoadStatusReport {
            segment_id: "seg-1".into(),
            state: SegmentLoadState::Loaded,
            message: String::new(),
        };
        let j = serde_json::to_string(&r).expect("serialize");
        assert!(!j.contains("message"), "{j}");
        assert!(j.contains("\"segmentId\":\"seg-1\""), "{j}");
    }
}
