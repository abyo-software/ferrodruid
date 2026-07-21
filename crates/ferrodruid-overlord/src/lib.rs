// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Ingestion task assignment for FerroDruid.
//!
//! The [`Overlord`] manages the lifecycle of ingestion tasks (Kafka
//! supervisors, batch ingestion jobs) and their assignment to
//! MiddleManager/Indexer nodes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(feature = "kafka-io")]
mod kafka;
// The Kinesis wiring (compat-5) is transport-abstracted: the whole
// consume/publish/resume pipeline is generic over the feature-free
// `KinesisSource` trait, so the module compiles (and its integration
// tests run, mock-driven) WITHOUT `kinesis-io`. Only the real
// aws-sdk-kinesis source construction — in `create_supervisor` /
// `resume_kinesis_supervisors`, not here — needs the feature, so the
// module is omitted from a default non-test build (where nothing could
// reach it) rather than sprinkled with per-item gates.
#[cfg(any(test, feature = "kinesis-io"))]
mod kinesis;
mod persist;
mod task;

pub use task::{
    Interval, LockDecision, LockType, RetryPolicy, TaskLock, TaskState, Worker,
    WorkerSelectStrategy, WorkerSelector, evaluate_lock_request,
};

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Utc};
use ferrodruid_common::{DruidError, Result};
use ferrodruid_deep_storage::DeepStorage;
use ferrodruid_historical::{Historical, SegmentSwapEntry};
use ferrodruid_ingest_batch::{BatchIngester, TsFormat};
use ferrodruid_metadata::{MetadataStore, SegmentMetadataRow, SupervisorRow, TaskLockRow, TaskRow};
use ferrodruid_segment::SegmentData;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::persist::persist_segment;

// ---------------------------------------------------------------------------
// Helpers for the Druid `index_parallel` + inline-data path
// ---------------------------------------------------------------------------

/// Format epoch milliseconds as a Druid-style ISO-8601 UTC string with
/// millisecond precision, e.g. `2024-01-01T00:00:00.000Z`.
fn format_epoch_millis_iso(millis: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(millis).map_or_else(
        || millis.to_string(),
        |dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
    )
}

/// Best-effort cleanup of a Phase-P deep-storage blob that a publish failure
/// left ORPHANED (compat-3 durability, H5/H8), shared by the batch and Kafka
/// publish tails so the delete DECISION lives in exactly one place.
///
/// The blob is deleted ONLY when it is safe: `persisted` (a blob really was
/// uploaded) AND `metadata_removed` (no metadata row references it anymore —
/// the row was never committed for an H8 metadata-transaction failure, or its
/// rollback SUCCEEDED for an H5 swap failure). When a rollback FAILED the row
/// still points at the blob, so `metadata_removed` is false and the blob is
/// KEPT: deleting a still-referenced blob would create a phantom (a metadata
/// row → a missing blob) that the bootstrap reload treats as data loss.
/// `delete_segment` is idempotent, so an actual delete error is only logged.
///
/// Returns whether a delete was attempted (diagnostics / tests).
pub(crate) async fn cleanup_orphan_blob(
    deep_storage: Option<&dyn DeepStorage>,
    data_source: &str,
    segment_id: &str,
    persisted: bool,
    metadata_removed: bool,
) -> bool {
    if !(persisted && metadata_removed) {
        return false;
    }
    let Some(ds) = deep_storage else {
        return false;
    };
    if let Err(del_err) = ds.delete_segment(data_source, segment_id).await {
        tracing::warn!(
            data_source = %data_source,
            segment_id = %segment_id,
            error = %del_err,
            "could not delete an orphan deep-storage blob after a publish failure \
             (unreferenced; storage waste only)",
        );
    }
    true
}

/// Allocate a collision-free segment id against every existing metadata
/// row (used or unused) and every loaded segment.
///
/// Shared implementation behind [`Overlord::allocate_segment_id`] and the
/// streaming Kafka publish tail so there is exactly ONE id-allocation
/// path (Codex 2026-07-12 round-2 HIGH #4). Callers MUST hold the
/// datasource's publish lock: it is what keeps the exists-check and the
/// subsequent insert race-free.
async fn allocate_segment_id_inner(
    metadata: &MetadataStore,
    historical: &Historical,
    ds_name: &str,
    start_iso: &str,
    end_iso: &str,
    version: &str,
) -> Result<String> {
    /// Bound on same-base-id publications; reaching it means the store
    /// holds this many rows for one `(interval, version)` and almost
    /// certainly indicates a bug or abuse rather than real ingestion.
    const MAX_ID_SUFFIX: u32 = 10_000;

    let base = format!("{ds_name}_{start_iso}_{end_iso}_{version}");
    let mut candidate = base.clone();
    for suffix in 1..=MAX_ID_SUFFIX {
        let taken =
            metadata.segment_exists(&candidate).await? || historical.has_segment(&candidate);
        if !taken {
            return Ok(candidate);
        }
        candidate = format!("{base}_{suffix}");
    }
    Err(DruidError::Ingestion(format!(
        "could not allocate a unique segment id after {MAX_ID_SUFFIX} attempts \
         (base id '{base}'); refusing to overwrite an existing segment"
    )))
}

/// Derive a [`TaskLocation`] from a worker id of the form `host:port`.
fn worker_location(worker_id: &str) -> Option<TaskLocation> {
    let (host, port) = worker_id.rsplit_once(':')?;
    let port: u16 = port.parse().ok()?;
    Some(TaskLocation {
        host: host.to_string(),
        port,
        tls_port: None,
    })
}

/// Parse an ISO-8601 `"start/end"` interval string into epoch-millis bounds.
fn parse_iso_interval(s: &str) -> Result<(i64, i64)> {
    let (start_s, end_s) = s
        .split_once('/')
        .ok_or_else(|| DruidError::Ingestion(format!("interval missing '/': {s}")))?;
    let start = parse_interval_bound_millis(start_s.trim())
        .map_err(|e| DruidError::Ingestion(format!("interval start '{start_s}': {e}")))?;
    let end = parse_interval_bound_millis(end_s.trim())
        .map_err(|e| DruidError::Ingestion(format!("interval end '{end_s}': {e}")))?;
    if start >= end {
        return Err(DruidError::Ingestion(format!(
            "interval start must be before end: {s}"
        )));
    }
    Ok((start, end))
}

/// Parse one interval bound to epoch millis, accepting both RFC3339 timestamps
/// and the bare `YYYY-MM-DD` date form (UTC midnight) common in Druid batch
/// ingestion specs (DD R47).
fn parse_interval_bound_millis(s: &str) -> std::result::Result<i64, String> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        && let Some(naive) = date.and_hms_opt(0, 0, 0)
    {
        return Ok(naive.and_utc().timestamp_millis());
    }
    Err(format!("not an RFC3339 timestamp or YYYY-MM-DD date: {s}"))
}

/// Whether `spec` is a Kafka supervisor (top-level `type: "kafka"`).
///
/// Ungated (available without `kafka-io`) so `create_supervisor` can derive
/// a stable id-less Kafka supervisor id in EVERY build — a default build
/// still persists Kafka specs, and inconsistent ids across a feature
/// transition would resume as duplicate consumers (Codex R11).
pub(crate) fn is_kafka_typed(spec: &serde_json::Value) -> bool {
    spec.get("type").and_then(|v| v.as_str()) == Some("kafka")
}

/// Whether `spec` is a Kinesis supervisor (top-level `type: "kinesis"`).
/// Ungated for the same build-parity reasons as [`is_kafka_typed`]
/// (stable id derivation + validate-before-persist in EVERY build).
pub(crate) fn is_kinesis_typed(spec: &serde_json::Value) -> bool {
    spec.get("type").and_then(|v| v.as_str()) == Some("kinesis")
}

/// The `dataSchema.dataSource` of a supervisor spec (flattened or
/// enveloped). Druid derives an id-less supervisor's id from this, so
/// `create_supervisor` uses it as the STABLE id when `id` is omitted —
/// otherwise reposting the same id-less spec would generate a fresh
/// synthetic id and start a duplicate consumer (Codex R5/R11). Ungated for
/// the same reason as [`is_kafka_typed`].
pub(crate) fn datasource_of(spec: &serde_json::Value) -> Option<String> {
    fn ds(v: &serde_json::Value) -> Option<String> {
        v.get("dataSchema")?
            .get("dataSource")?
            .as_str()
            .map(str::to_owned)
    }
    ds(spec).or_else(|| spec.get("spec").and_then(ds))
}

/// Coerce a supervisor POST's `suspended` flag (top-level or inside the
/// `{"spec": …}` envelope) the way Druid's Jackson scalar coercion does:
/// booleans pass through and the STRINGS "true"/"false" (ASCII
/// case-insensitive) coerce; `null`/absent mean not-suspended. Anything else
/// is a LOUD error — a junk flag must never silently pick the
/// running-vs-suspended lifecycle state (Fable audit: `"suspended": "true"`
/// from shell/YAML templating used to start a consumer Druid would have kept
/// suspended). Ungated so both builds accept/reject identically.
pub(crate) fn kafka_suspended_flag(spec: &serde_json::Value) -> Result<bool> {
    fn coerce(v: &serde_json::Value) -> Result<bool> {
        match v {
            serde_json::Value::Bool(b) => Ok(*b),
            serde_json::Value::String(s) if s.eq_ignore_ascii_case("true") => Ok(true),
            serde_json::Value::String(s) if s.eq_ignore_ascii_case("false") => Ok(false),
            serde_json::Value::Null => Ok(false),
            other => Err(DruidError::Ingestion(format!(
                "supervisor `suspended` flag must be a boolean or \"true\"/\"false\", got: {other}"
            ))),
        }
    }
    let top = spec
        .get("suspended")
        .map(coerce)
        .transpose()?
        .unwrap_or(false);
    let inner = spec
        .get("spec")
        .and_then(|s| s.get("suspended"))
        .map(coerce)
        .transpose()?
        .unwrap_or(false);
    Ok(top || inner)
}

/// Top-level keys real Druid includes in a supervisor POST body that are NOT
/// part of the strict `KafkaSupervisorSpec` (they trip its
/// `deny_unknown_fields`). Stripped before deserializing.
const KAFKA_WRAPPER_KEYS: &[&str] = &["id", "suspended", "context"];

/// Parse a supervisor POST body into a `KafkaSupervisorSpec`, accepting the
/// flattened (`{type,dataSchema,ioConfig}`) and enveloped (`{type,spec:{…}}`)
/// Druid shapes. Returns `None` for anything that is not a strict Kafka spec.
///
/// Ungated (out of the kafka-io-only `kafka` module) so `create_supervisor`
/// can validate a Kafka POST BEFORE persisting it in EVERY build — the
/// default build otherwise acknowledged+persisted an invalid spec that a
/// later kafka-io resume silently skipped (Codex R14).
pub(crate) fn parse_kafka_supervisor_spec(
    spec: &serde_json::Value,
) -> Option<ferrodruid_ingest_kafka::KafkaSupervisorSpec> {
    fn strip(mut v: serde_json::Value) -> serde_json::Value {
        if let Some(obj) = v.as_object_mut() {
            for k in KAFKA_WRAPPER_KEYS {
                obj.remove(*k);
            }
        }
        v
    }
    if let Ok(parsed) =
        serde_json::from_value::<ferrodruid_ingest_kafka::KafkaSupervisorSpec>(strip(spec.clone()))
    {
        return Some(parsed);
    }
    // Enveloped form: lift the inner `spec` and carry the outer `type` down.
    let mut inner = strip(spec.get("spec")?.clone());
    if let Some(obj) = inner.as_object_mut()
        && !obj.contains_key("type")
        && let Some(outer_type) = spec.get("type")
    {
        obj.insert("type".to_string(), outer_type.clone());
    }
    serde_json::from_value(inner).ok()
}

/// Parse AND semantically validate a Kafka supervisor spec, returning a
/// client-facing error so `create_supervisor` rejects an invalid spec BEFORE
/// persisting it (in EVERY build, not just kafka-io; Codex R14).
pub(crate) fn validate_kafka_spec(
    spec: &serde_json::Value,
) -> Result<ferrodruid_ingest_kafka::KafkaSupervisorSpec> {
    let parsed = parse_kafka_supervisor_spec(spec).ok_or_else(|| {
        DruidError::Ingestion(
            "invalid Kafka supervisor spec: could not parse dataSchema / ioConfig \
             (a runnable spec needs both)"
                .to_string(),
        )
    })?;
    parsed
        .validate()
        .map_err(|e| DruidError::Ingestion(format!("invalid Kafka supervisor spec: {e}")))?;
    Ok(parsed)
}

/// Parse a supervisor POST body into a `KinesisSupervisorSpec`, accepting
/// the flattened and enveloped (`{type, spec:{…}}`) Druid shapes — the
/// Kinesis analogue of [`parse_kafka_supervisor_spec`]. Ungated so a
/// Kinesis POST is validated before persist in EVERY build (compat-5,
/// mirroring the kafka Codex R14 posture).
pub(crate) fn parse_kinesis_supervisor_spec(
    spec: &serde_json::Value,
) -> Option<ferrodruid_ingest_kinesis::KinesisSupervisorSpec> {
    fn strip(mut v: serde_json::Value) -> serde_json::Value {
        if let Some(obj) = v.as_object_mut() {
            for k in KAFKA_WRAPPER_KEYS {
                obj.remove(*k);
            }
        }
        v
    }
    if let Ok(parsed) = serde_json::from_value::<ferrodruid_ingest_kinesis::KinesisSupervisorSpec>(
        strip(spec.clone()),
    ) {
        return Some(parsed);
    }
    // Enveloped form: lift the inner `spec`, carry the outer `type` down.
    let mut inner = strip(spec.get("spec")?.clone());
    if let Some(obj) = inner.as_object_mut()
        && !obj.contains_key("type")
        && let Some(outer_type) = spec.get("type")
    {
        obj.insert("type".to_string(), outer_type.clone());
    }
    serde_json::from_value(inner).ok()
}

/// Parse AND semantically validate a Kinesis supervisor spec, returning a
/// client-facing error so `create_supervisor` rejects an invalid spec
/// BEFORE persisting it — in EVERY build (compat-5). The semantic checks
/// live here (the ingest crate carries no `validate()`), mirroring the
/// runnable-spec essentials of the Kafka `validate()`: a non-empty
/// datasource + stream, a supported `timestampSpec.format`
/// (auto/iso/millis — the only grammars the shared extraction speaks),
/// and non-zero tuning row limits.
pub(crate) fn validate_kinesis_spec(
    spec: &serde_json::Value,
) -> Result<ferrodruid_ingest_kinesis::KinesisSupervisorSpec> {
    let parsed = parse_kinesis_supervisor_spec(spec).ok_or_else(|| {
        DruidError::Ingestion(
            "invalid Kinesis supervisor spec: could not parse dataSchema / ioConfig \
             (a runnable spec needs both, with ioConfig.stream)"
                .to_string(),
        )
    })?;
    if parsed.spec_type != "kinesis" {
        return Err(DruidError::Ingestion(format!(
            "invalid Kinesis supervisor spec: spec.type must be \"kinesis\", got {:?}",
            parsed.spec_type
        )));
    }
    if parsed.data_schema.data_source.trim().is_empty() {
        return Err(DruidError::Ingestion(
            "invalid Kinesis supervisor spec: dataSchema.dataSource must not be empty".to_string(),
        ));
    }
    if parsed.io_config.stream.trim().is_empty() {
        return Err(DruidError::Ingestion(
            "invalid Kinesis supervisor spec: ioConfig.stream must not be empty".to_string(),
        ));
    }
    // Shared spec→TsFormat mapping (compat-9): the same helper the batch
    // parse uses, so an unimplemented format is rejected with one message
    // everywhere.
    if let Err(msg) = TsFormat::from_spec_format(&parsed.data_schema.timestamp_spec.format) {
        return Err(DruidError::Ingestion(format!(
            "invalid Kinesis supervisor spec: {msg}"
        )));
    }
    if let Some(tuning) = &parsed.tuning_config {
        if tuning.max_rows_per_segment == Some(0) {
            return Err(DruidError::Ingestion(
                "invalid Kinesis supervisor spec: tuningConfig.maxRowsPerSegment must be > 0"
                    .to_string(),
            ));
        }
        if tuning.max_rows_in_memory == Some(0) {
            return Err(DruidError::Ingestion(
                "invalid Kinesis supervisor spec: tuningConfig.maxRowsInMemory must be > 0"
                    .to_string(),
            ));
        }
    }
    Ok(parsed)
}

/// Run one WHOLE supervisor-lifecycle operation body uncancellably: spawn
/// it, park the join handle in `ops` (the Overlord's per-transport
/// lifecycle-op registry), and relay its result to the caller over a
/// oneshot. Shared by the Kafka AND Kinesis wiring (hoisted out of the
/// kafka-io-gated module for compat-5; semantics unchanged).
///
/// Cancellation safety (Codex R20 → R22 → R23): a lifecycle op chains
/// destructive-then-restorative steps a cancelled caller (an HTTP client
/// disconnect dropping the axum handler future) must never tear apart —
/// spawning the whole tail removes that boundary by construction: a
/// cancelled caller either ran nothing or the full op runs to completion
/// in the background.
///
/// The registry lock is taken BEFORE the spawn and released after the
/// push, with no await point in between, so caller cancellation cannot
/// lose a spawned op's handle. Every lifecycle entry point drains the
/// registry right after taking the lifecycle lock. The result travels
/// back on a oneshot; a receive error (the op panicked or was aborted
/// before sending) is a fail-closed `Err` naming `label`. Op bodies take
/// only the datasource publish lock, metadata handles, and the consumer
/// registries — never an ops registry or the lifecycle lock — so the
/// drain cannot deadlock.
#[cfg(any(feature = "kafka-io", feature = "kinesis-io"))]
pub(crate) async fn run_lifecycle_op<T, F>(
    ops: &Mutex<Vec<tokio::task::JoinHandle<()>>>,
    label: &str,
    op: F,
) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let (result_tx, result_rx) = tokio::sync::oneshot::channel::<Result<T>>();
    {
        // Registry lock BEFORE the spawn: spawn→push has no await point,
        // so caller cancellation cannot lose a spawned op's handle.
        let mut ops_guard = ops.lock().await;
        let handle = tokio::spawn(async move {
            // A cancelled caller dropped the receiver; the registry drain
            // then owns this op's completion — ignore the send error.
            let _ = result_tx.send(op.await);
        });
        ops_guard.push(handle);
    }
    match result_rx.await {
        Ok(result) => result,
        // Sender dropped without a result: the op panicked or was aborted.
        Err(_) => Err(DruidError::Ingestion(format!(
            "{label} did not run to completion (panicked or was aborted); \
             refusing to report success"
        ))),
    }
}

/// Segment-granularity bucketing used to compute the interval an
/// `appendToExisting: false` (Druid's batch default) task overwrites.
///
/// Druid's replace semantics operate on **segmentGranularity buckets**, not
/// on the raw data range: a DAY-granularity task whose rows land anywhere in
/// `2026-01-01` overshadows every existing segment of that day, even ones
/// whose row timestamps do not intersect the new rows'.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentGranularity {
    /// `ALL`: a single bucket covering all time (the task replaces the
    /// datasource's entire used-segment set).
    All,
    /// Fixed-length buckets of `millis` anchored at epoch offset `anchor`
    /// (everything from `NONE`/`SECOND` up to `DAY`; `WEEK` uses a Monday
    /// anchor for ISO-week alignment).
    Fixed {
        /// Bucket length in milliseconds.
        millis: i64,
        /// Epoch-millis offset the bucket grid is aligned to.
        anchor: i64,
    },
    /// Calendar months (variable length, UTC).
    Month,
    /// Calendar quarters (Jan/Apr/Jul/Oct starts, UTC).
    Quarter,
    /// Calendar years (UTC).
    Year,
}

/// Milliseconds in one UTC day.
const MILLIS_PER_DAY: i64 = 86_400_000;

/// Epoch-millis of Monday 1969-12-29T00:00:00Z — anchors `WEEK` buckets so
/// they start on Mondays (ISO weeks), matching Druid's `WEEK` granularity.
const WEEK_ANCHOR_MILLIS: i64 = -3 * MILLIS_PER_DAY;

/// `druid_config` key prefix registering every datasource that ever held a
/// task lock (`{prefix}{dataSource}` → `true`), stamped by `persist_lock`
/// and read back by
/// [`reconcile_orphaned_task_locks`](Overlord::reconcile_orphaned_task_locks)
/// so crash-orphaned locks are enumerable even when their datasource has
/// neither task rows nor segments (ship2 H10). Entries are never removed —
/// one idempotent row per datasource, and a stale entry only costs one
/// empty lock query at startup.
const LOCK_DS_CONFIG_PREFIX: &str = "overlord.lock_datasource.";

/// Parse a Druid `segmentGranularity` JSON value.
///
/// Druid's `UniformGranularitySpec` defaults `segmentGranularity` to **DAY**
/// when the field (or the whole `granularitySpec`) is absent. Supported wire
/// shapes: the simple-string granularity names (case-insensitive) and the
/// `{"type": "period", "period": "P1D"}` / `{"type": "all"}` object forms.
/// Unsupported shapes fail closed — the caller only invokes this on the
/// `appendToExisting: false` path, where guessing the replace scope wrong
/// would silently duplicate or destroy data.
fn parse_segment_granularity(v: Option<&serde_json::Value>) -> Result<SegmentGranularity> {
    const DAY: SegmentGranularity = SegmentGranularity::Fixed {
        millis: MILLIS_PER_DAY,
        anchor: 0,
    };
    let Some(v) = v else { return Ok(DAY) };
    match v {
        serde_json::Value::Null => Ok(DAY),
        serde_json::Value::String(s) => segment_granularity_from_name(s),
        serde_json::Value::Object(o) => {
            let gtype = o.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match gtype {
                "all" => Ok(SegmentGranularity::All),
                "period" => {
                    let period = o.get("period").and_then(|p| p.as_str()).ok_or_else(|| {
                        DruidError::Ingestion(
                            "segmentGranularity period object is missing its 'period' string"
                                .to_string(),
                        )
                    })?;
                    segment_granularity_from_period(period)
                }
                other => Err(DruidError::Ingestion(format!(
                    "unsupported segmentGranularity object type '{other}' for \
                     appendToExisting=false (replace) ingestion (supported: all, period)"
                ))),
            }
        }
        other => Err(DruidError::Ingestion(format!(
            "segmentGranularity must be a string or object, got: {other}"
        ))),
    }
}

/// Map a simple-string granularity name (case-insensitive) onto
/// [`SegmentGranularity`].
fn segment_granularity_from_name(s: &str) -> Result<SegmentGranularity> {
    let fixed = |millis| SegmentGranularity::Fixed { millis, anchor: 0 };
    match s.to_ascii_lowercase().as_str() {
        "none" => Ok(fixed(1)),
        "second" => Ok(fixed(1_000)),
        "minute" => Ok(fixed(60_000)),
        "five_minute" => Ok(fixed(300_000)),
        "ten_minute" => Ok(fixed(600_000)),
        "fifteen_minute" => Ok(fixed(900_000)),
        "thirty_minute" => Ok(fixed(1_800_000)),
        "hour" => Ok(fixed(3_600_000)),
        "six_hour" => Ok(fixed(21_600_000)),
        "eight_hour" => Ok(fixed(28_800_000)),
        "day" => Ok(fixed(MILLIS_PER_DAY)),
        "week" => Ok(SegmentGranularity::Fixed {
            millis: 7 * MILLIS_PER_DAY,
            anchor: WEEK_ANCHOR_MILLIS,
        }),
        "month" => Ok(SegmentGranularity::Month),
        "quarter" => Ok(SegmentGranularity::Quarter),
        "year" => Ok(SegmentGranularity::Year),
        "all" => Ok(SegmentGranularity::All),
        other => Err(DruidError::Ingestion(format!(
            "unsupported segmentGranularity '{other}' for appendToExisting=false \
             (replace) ingestion"
        ))),
    }
}

/// Map a period-object `segmentGranularity` (`P1D`, `PT1H`, ...) onto
/// [`SegmentGranularity`].
fn segment_granularity_from_period(p: &str) -> Result<SegmentGranularity> {
    match p.to_ascii_uppercase().as_str() {
        "PT1S" => segment_granularity_from_name("second"),
        "PT1M" => segment_granularity_from_name("minute"),
        "PT5M" => segment_granularity_from_name("five_minute"),
        "PT10M" => segment_granularity_from_name("ten_minute"),
        "PT15M" => segment_granularity_from_name("fifteen_minute"),
        "PT30M" => segment_granularity_from_name("thirty_minute"),
        "PT1H" => segment_granularity_from_name("hour"),
        "PT6H" => segment_granularity_from_name("six_hour"),
        "PT8H" => segment_granularity_from_name("eight_hour"),
        "P1D" => segment_granularity_from_name("day"),
        "P1W" | "P7D" => segment_granularity_from_name("week"),
        "P1M" => Ok(SegmentGranularity::Month),
        "P3M" => Ok(SegmentGranularity::Quarter),
        "P1Y" => Ok(SegmentGranularity::Year),
        other => Err(DruidError::Ingestion(format!(
            "unsupported segmentGranularity period '{other}' for \
             appendToExisting=false (replace) ingestion"
        ))),
    }
}

/// Clamp an `i128` intermediate back into `i64` bucket arithmetic.
fn clamp_millis_i64(v: i128) -> i64 {
    let clamped = v.clamp(i128::from(i64::MIN), i128::from(i64::MAX));
    // Infallible after the clamp; saturate defensively rather than unwrap.
    i64::try_from(clamped).unwrap_or(if v < 0 { i64::MIN } else { i64::MAX })
}

/// `(year, month0)` of a UTC timestamp for calendar-bucket arithmetic.
fn year_month0(ts: i64) -> Result<(i32, i32)> {
    let dt = DateTime::<Utc>::from_timestamp_millis(ts).ok_or_else(|| {
        DruidError::Ingestion(format!("timestamp out of range for calendar bucket: {ts}"))
    })?;
    let month0 = i32::try_from(dt.month0())
        .map_err(|e| DruidError::Ingestion(format!("month out of range: {e}")))?;
    Ok((dt.year(), month0))
}

/// Epoch-millis of `year`/`month0` (0-based month, may exceed 11 to roll
/// into later years) at UTC midnight of day 1.
fn ym_start_millis(year: i32, month0: i32) -> Result<i64> {
    let y = year + month0.div_euclid(12);
    let m = u32::try_from(month0.rem_euclid(12) + 1)
        .map_err(|e| DruidError::Ingestion(format!("month out of range: {e}")))?;
    chrono::NaiveDate::from_ymd_opt(y, m, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|n| n.and_utc().timestamp_millis())
        .ok_or_else(|| {
            DruidError::Ingestion(format!("calendar bucket out of range: year {y} month {m}"))
        })
}

/// Compute the half-open `[start, end)` epoch-millis window that an
/// `appendToExisting: false` task overwrites: the union of the
/// segment-granularity buckets covering the ingested data's
/// `[min_ts, max_ts]` (both bounds inclusive row timestamps).
fn replace_scope(granularity: SegmentGranularity, min_ts: i64, max_ts: i64) -> Result<(i64, i64)> {
    match granularity {
        SegmentGranularity::All => Ok((i64::MIN, i64::MAX)),
        SegmentGranularity::Fixed { millis, anchor } => {
            let millis_w = i128::from(millis);
            let anchor_w = i128::from(anchor);
            let bucket_start =
                |t: i64| anchor_w + (i128::from(t) - anchor_w).div_euclid(millis_w) * millis_w;
            Ok((
                clamp_millis_i64(bucket_start(min_ts)),
                clamp_millis_i64(bucket_start(max_ts) + millis_w),
            ))
        }
        SegmentGranularity::Month => {
            let (ys, ms) = year_month0(min_ts)?;
            let (ye, me) = year_month0(max_ts)?;
            Ok((ym_start_millis(ys, ms)?, ym_start_millis(ye, me + 1)?))
        }
        SegmentGranularity::Quarter => {
            let (ys, ms) = year_month0(min_ts)?;
            let (ye, me) = year_month0(max_ts)?;
            let qs = ms.div_euclid(3) * 3;
            let qe = me.div_euclid(3) * 3;
            Ok((ym_start_millis(ys, qs)?, ym_start_millis(ye, qe + 3)?))
        }
        SegmentGranularity::Year => {
            let (ys, _) = year_month0(min_ts)?;
            let (ye, _) = year_month0(max_ts)?;
            Ok((ym_start_millis(ys, 0)?, ym_start_millis(ye, 12)?))
        }
    }
}

/// Convert a [`TaskLock`] into its persisted [`TaskLockRow`] form.
fn lock_to_lock_row(lock: &TaskLock) -> Result<TaskLockRow> {
    let start_iso = format_epoch_millis_iso(lock.interval.start_millis);
    let end_iso = format_epoch_millis_iso(lock.interval.end_millis);
    let payload = serde_json::to_value(lock)
        .map_err(|e| DruidError::Metadata(format!("serialize lock: {e}")))?;
    Ok(TaskLockRow {
        id: lock.id.clone(),
        task_id: lock.task_id.clone(),
        data_source: lock.data_source.clone(),
        interval_start: start_iso,
        interval_end: end_iso,
        lock_type: lock.lock_type.as_str().to_string(),
        priority: lock.priority,
        revoked: lock.revoked,
        payload,
    })
}

/// Reconstruct a [`TaskLock`] from its persisted [`TaskLockRow`] form.
fn lock_row_to_lock(row: &TaskLockRow) -> Result<TaskLock> {
    let start = DateTime::parse_from_rfc3339(&row.interval_start)
        .map_err(|e| DruidError::Metadata(format!("lock interval_start: {e}")))?
        .timestamp_millis();
    let end = DateTime::parse_from_rfc3339(&row.interval_end)
        .map_err(|e| DruidError::Metadata(format!("lock interval_end: {e}")))?
        .timestamp_millis();
    let lock_type = match row.lock_type.as_str() {
        "SHARED" => LockType::Shared,
        "EXCLUSIVE" => LockType::Exclusive,
        other => {
            return Err(DruidError::Metadata(format!("unknown lock type: {other}")));
        }
    };
    Ok(TaskLock {
        id: row.id.clone(),
        task_id: row.task_id.clone(),
        data_source: row.data_source.clone(),
        interval: Interval::new(start, end)?,
        lock_type,
        priority: row.priority,
        revoked: row.revoked,
    })
}

/// Parsed `index_parallel` spec ready to feed [`BatchIngester`].
#[derive(Debug)]
struct ParsedIndexSpec {
    data_source: String,
    timestamp_column: String,
    /// Declared `timestampSpec.format`, mapped through the SHARED
    /// [`TsFormat::from_spec_format`] helper (the same one the
    /// Kafka/Kinesis supervisor validation uses) and threaded to
    /// [`BatchIngester::with_timestamp_format`]. Pre-fix (compat-9 P0)
    /// the field was never read and every batch task parsed as `auto`,
    /// so a declared `iso` spec silently stored `"2023"` as 2023
    /// MILLISECONDS (1970-01-01T00:00:02.023Z) instead of the year
    /// 2023; unimplemented formats (posix/nano/custom) were silently
    /// mis-parsed instead of rejected.
    timestamp_format: TsFormat,
    /// Typed dimension schemas: `dimensionsSpec.dimensions` object entries
    /// (`{"type":"double","name":...}`) keep their type instead of being
    /// flattened to bare names (which silently ingested numeric dimensions
    /// as strings — the typed-dimension half of the null-faithful work).
    dimensions: Vec<ferrodruid_ingest_batch::DimensionSchema>,
    metrics_specs: Vec<serde_json::Value>,
    rows: Vec<serde_json::Value>,
    /// Whether ingestion-time rollup is enabled.  Druid's
    /// `UniformGranularitySpec` defaults `rollup` to **true** when the
    /// field is absent, and the default `granularitySpec` itself (when the
    /// whole object is absent) is `{rollup: true, queryGranularity: none}`
    /// — pre-fix this was never read and every spec silently ingested raw
    /// un-rolled rows (2026-07-11 v1.1.0 build-verification bug).
    rollup: bool,
    /// Truncation grain for rollup grouping, in the form
    /// [`BatchIngester::ingest_with_rollup`] expects (`"second"` /
    /// `"minute"` / `"hour"` / `"day"` / `"month"` / `"year"`), or
    /// `"none"` for exact-millisecond grouping (Druid's rollup with
    /// `queryGranularity=none` still merges identical (timestamp, dims)
    /// rows).  Only meaningful when `rollup` is true.
    query_granularity: String,
    /// `ioConfig.appendToExisting`.  Druid's native-batch default is
    /// **false**: the task REPLACES the used segments of the
    /// segmentGranularity buckets it writes into.  `true` preserves the
    /// pre-existing append behavior (segments coexist).  Pre-fix
    /// (P1-#1, 2026-07-12) this field was never read and every task
    /// appended — re-ingesting an interval silently doubled COUNT(*).
    append_to_existing: bool,
    /// Raw `granularitySpec.segmentGranularity` value, validated lazily on
    /// the `appendToExisting: false` path only (an append-only spec with an
    /// exotic granularity must not fail parse — mirroring the
    /// `rollup: false` leniency for `queryGranularity`).
    segment_granularity: Option<serde_json::Value>,
}

/// Map a Druid `queryGranularity` simple-string form (case-insensitive)
/// onto the truncation grain [`BatchIngester::ingest_with_rollup`]
/// expects.  Grains the batch ingester cannot truncate to
/// (`fifteen_minute`, `thirty_minute`, `week`, `quarter`, `all`) fail
/// closed with an ingestion error rather than silently mis-bucketing
/// rolled rows.
fn map_granularity_name(s: &str) -> Result<String> {
    let lower = s.to_ascii_lowercase();
    match lower.as_str() {
        "none" | "second" | "minute" | "hour" | "day" | "month" | "year" => Ok(lower),
        other => Err(DruidError::Ingestion(format!(
            "unsupported queryGranularity '{other}' for rollup ingestion \
             (supported: none/second/minute/hour/day/month/year, \
             or the period object form PT1S/PT1M/PT1H/P1D/P1M/P1Y)"
        ))),
    }
}

/// Map a Druid period-object `queryGranularity` (`{"type": "period",
/// "period": "PT1H"}`) onto the batch ingester's truncation grain.
fn map_granularity_period(p: &str) -> Result<String> {
    match p.to_ascii_uppercase().as_str() {
        "PT1S" => Ok("second".to_string()),
        "PT1M" => Ok("minute".to_string()),
        "PT1H" => Ok("hour".to_string()),
        "P1D" => Ok("day".to_string()),
        "P1M" => Ok("month".to_string()),
        "P1Y" => Ok("year".to_string()),
        other => Err(DruidError::Ingestion(format!(
            "unsupported queryGranularity period '{other}' for rollup ingestion \
             (supported periods: PT1S/PT1M/PT1H/P1D/P1M/P1Y)"
        ))),
    }
}

/// Parse a Druid `queryGranularity` JSON value onto the truncation grain
/// string used for rollup grouping.
///
/// Supported wire shapes:
/// * absent / `null` → `"none"` (Druid's `UniformGranularitySpec` default);
/// * simple strings, case-insensitive: `"none"`, `"second"`, `"minute"`,
///   `"hour"`, `"day"`, `"month"`, `"year"` (the fixture files use the
///   uppercase forms `"NONE"` / `"HOUR"`);
/// * object forms `{"type": "none"}`, `{"type": "all"}` (rejected — see
///   below) and `{"type": "period", "period": "PT1H"}` for the six
///   mappable periods.
///
/// `"none"` maps to exact-millisecond grouping: Druid's rollup with
/// `queryGranularity=none` still merges rows whose (timestamp, dims) are
/// identical, which is exactly what the batch ingester does for the
/// `"none"` grain (its timestamp truncation leaves it untouched).
fn parse_query_granularity(v: Option<&serde_json::Value>) -> Result<String> {
    let Some(v) = v else {
        return Ok("none".to_string());
    };
    match v {
        serde_json::Value::Null => Ok("none".to_string()),
        serde_json::Value::String(s) => map_granularity_name(s),
        serde_json::Value::Object(o) => {
            let gtype = o.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match gtype {
                "none" => Ok("none".to_string()),
                "period" => {
                    let period = o.get("period").and_then(|p| p.as_str()).ok_or_else(|| {
                        DruidError::Ingestion(
                            "queryGranularity period object is missing its 'period' string"
                                .to_string(),
                        )
                    })?;
                    map_granularity_period(period)
                }
                other => Err(DruidError::Ingestion(format!(
                    "unsupported queryGranularity object type '{other}' for rollup ingestion \
                     (supported: none, period)"
                ))),
            }
        }
        other => Err(DruidError::Ingestion(format!(
            "queryGranularity must be a string or object, got: {other}"
        ))),
    }
}

/// Parse `dataSchema.granularitySpec` into `(rollup, query_granularity)`.
///
/// Druid semantics (its `UniformGranularitySpec` defaults, matched
/// faithfully): an absent `granularitySpec` object means
/// `{rollup: true, queryGranularity: none}`, and inside a present spec an
/// absent `rollup` field defaults to **true** and an absent
/// `queryGranularity` to `none`.
///
/// The grain is only validated when rollup is enabled: with
/// `rollup: false` the batch path ingests raw rows and never consults the
/// grain, so an exotic grain must not fail a spec that ingests fine
/// (honest limitation: Druid truncates `__time` by `queryGranularity`
/// even with rollup disabled; FerroDruid's rollup=false path currently
/// stores exact timestamps).
fn parse_granularity_spec(data_schema: &serde_json::Value) -> Result<(bool, String)> {
    let gspec = data_schema.get("granularitySpec");
    let rollup = match gspec.and_then(|g| g.get("rollup")) {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(other) => {
            return Err(DruidError::Ingestion(format!(
                "granularitySpec.rollup must be a boolean, got: {other}"
            )));
        }
    };
    let query_granularity = if rollup {
        parse_query_granularity(gspec.and_then(|g| g.get("queryGranularity")))?
    } else {
        "none".to_string()
    };
    Ok((rollup, query_granularity))
}

/// Minimal wildcard matcher for `local` inputSource `filter` patterns.
///
/// Supports `*` (any run of characters, including empty), `?` (exactly one
/// character), and literal characters, matched against the whole file NAME
/// (never a path). This covers Druid's common `*.json`-style filters
/// without pulling in a glob dependency; character classes (`[...]`) are
/// not supported and match literally.
fn glob_matches(pattern: &str, name: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = name.chars().collect();
    let (mut p, mut t) = (0usize, 0usize);
    // Position of the most recent `*` and the text index just past the run
    // it currently swallows (classic greedy matcher with backtracking).
    let mut star: Option<(usize, usize)> = None;
    while t < txt.len() {
        if p < pat.len() && (pat[p] == '?' || pat[p] == txt[t]) {
            p += 1;
            t += 1;
        } else if p < pat.len() && pat[p] == '*' {
            star = Some((p, t));
            p += 1;
        } else if let Some((sp, st)) = star {
            // Backtrack: let the last `*` swallow one more character.
            p = sp + 1;
            t = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    while p < pat.len() && pat[p] == '*' {
        p += 1;
    }
    p == pat.len()
}

/// Read a local input file race-safely: refuse to follow a symlink, refuse
/// to block on a FIFO, and require the opened inode to be the exact regular
/// file validated as inside `baseDir` at enumeration.
///
/// `expected` is the enumeration-time metadata of the canonical file. The
/// read closes the whole check-to-open TOCTOU family (Codex compat-4 R1
/// H2 + R2 H1/H2):
/// - `O_NOFOLLOW` — a final-component symlink swap fails the open (`ELOOP`)
///   instead of being followed outside `baseDir`.
/// - `O_NONBLOCK` — a FIFO/device swapped in opens immediately instead of
///   blocking read-only, so submission can't hang with locks held.
/// - `is_file()` on the opened handle rejects a non-regular swap-in.
/// - `(device, inode)` identity — `O_NOFOLLOW` only guards the FINAL
///   component, so an ANCESTOR directory swapped for a symlink (e.g.
///   `baseDir` itself) could resolve `path` to a different inode OUTSIDE
///   the tree; requiring the opened inode to match the enumerated one
///   closes that escape, since no external file can share the validated
///   inode's identity.
///
/// Uses only the safe `OpenOptionsExt::custom_flags` / `MetadataExt`, so
/// `#![forbid(unsafe_code)]` is preserved.
#[cfg(unix)]
fn read_regular_file_checked(
    path: &std::path::Path,
    expected: &std::fs::Metadata,
) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "input file is not a regular file",
        ));
    }
    // The opened inode must be byte-for-byte the file enumerated inside
    // baseDir:
    // - (device, inode): the ancestor-symlink identity gate (R2 H1) —
    //   `O_NOFOLLOW` guards only the final component, so an ancestor dir
    //   swapped for a symlink would resolve to a different inode outside
    //   the tree.
    // - (size, mtime, ctime): content stability (R3 H2 + R4 H1) — the
    //   (dev,inode) match alone does NOT prove the CONTENT is unchanged; a
    //   truncate/append, or an in-place rewrite that PRESERVES the size,
    //   still bumps mtime/ctime, so any post-enumeration mutation of that
    //   inode is rejected instead of silently publishing mutated input.
    if meta.dev() != expected.dev()
        || meta.ino() != expected.ino()
        || meta.size() != expected.size()
        || meta.mtime() != expected.mtime()
        || meta.mtime_nsec() != expected.mtime_nsec()
        || meta.ctime() != expected.ctime()
        || meta.ctime_nsec() != expected.ctime_nsec()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "input file changed between validation and read \
             (identity/size/mtime/ctime mismatch — possible symlink, rename, \
             truncation, or in-place rewrite race)",
        ));
    }
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    // Re-stat AFTER the read: a mutation DURING the read — e.g. rewriting
    // not-yet-read bytes of a large file — can leave the pre-read stat and
    // the total byte count unchanged, but the write still bumps mtime/ctime
    // (Codex R5 H3). Reject any change since the pre-read stat, and require
    // the byte count to still match the enumerated size.
    let after = file.metadata()?;
    if buf.len() as u64 != expected.len()
        || after.size() != meta.size()
        || after.mtime() != meta.mtime()
        || after.mtime_nsec() != meta.mtime_nsec()
        || after.ctime() != meta.ctime()
        || after.ctime_nsec() != meta.ctime_nsec()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "input file changed during read (possible concurrent mutation)",
        ));
    }
    Ok(buf)
}

/// Non-unix fallback: no `O_NOFOLLOW`/inode identity, so the
/// canonical-containment check in [`read_local_input_source`] is the
/// (best-effort, non-race-safe) guard.
#[cfg(not(unix))]
fn read_regular_file_checked(
    path: &std::path::Path,
    _expected: &std::fs::Metadata,
) -> std::io::Result<Vec<u8>> {
    std::fs::read(path)
}

/// Releases a task's durable locks if `submit_task` is DROPPED (its request
/// future cancelled — e.g. the HTTP client disconnects) after the lock was
/// granted but before the task committed.
///
/// Without it the lock leaks durably and blocks the datasource forever,
/// because nothing else releases a lock for a task that has no persisted
/// record (Codex compat-4 R4 H2). On a normal completion the guard is
/// disarmed — `run_with_retry`'s terminal paths already released the locks
/// and the task row is persisted.
struct SubmitLockGuard {
    armed: bool,
    task_id: String,
    metadata: Arc<MetadataStore>,
}

impl SubmitLockGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SubmitLockGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let task_id = std::mem::take(&mut self.task_id);
        let metadata = Arc::clone(&self.metadata);
        // Drop can't await, so the release runs as a detached task. The
        // submit future was cancelled before committing, so no other path
        // will release these locks. Guard against being dropped outside a
        // runtime (e.g. during shutdown).
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                match metadata.get_locks_for_task(&task_id).await {
                    Ok(rows) => {
                        for r in rows {
                            let _ = metadata.delete_lock(&r.id).await;
                        }
                    }
                    Err(e) => tracing::warn!(
                        task_id = %task_id,
                        error = %e,
                        "failed to list locks for cancellation release"
                    ),
                }
            });
        }
    }
}

/// Flips a batch task's durable RUNNING row to FAILED if `submit_task`
/// is DROPPED (its request future cancelled — e.g. the HTTP client
/// disconnects) after the row was persisted but before the detached
/// execute+publish tail was registered and spawned.
///
/// The ASYNC submit contract persists the RUNNING row BEFORE returning
/// the id (Druid parity: the accepted task is durably pollable), and the
/// registration+spawn block after it is fully synchronous — so a
/// cancellation can only land parked at the insert-join await or at the
/// `running_tasks` write-lock acquisition right after it, both
/// strictly BEFORE any tail exists. Without this guard such a drop
/// stranded a durable RUNNING row that nothing in the live process would
/// ever resolve (no in-memory record, no tail, no fence): a poller of
/// that id read RUNNING forever until a restart's
/// [`reconcile_stale_running_batch_tasks`](Overlord::reconcile_stale_running_batch_tasks)
/// pass. FAILED is truthful here — no tail was spawned, so nothing
/// executed and nothing can ever commit; a resubmission is safe. The
/// companion [`SubmitLockGuard`] releases the interval locks.
///
/// Torn-insert ordering (D4): the RUNNING-row insert runs in its OWN
/// spawned (uncancellable) task whose join handle lives on this guard, and
/// the cancellation cleanup AWAITS that join before upserting FAILED — so
/// the FAILED flip is ordered strictly after the RUNNING insert has truly
/// resolved. Pre-fix the detached FAILED upsert raced the cancelled
/// insert's store-side landing; when RUNNING landed last it won, stranding
/// a durable non-terminal row with no live owner until a restart's
/// bootstrap reconcile. Residual (documented): if the cleanup task cannot
/// be spawned at all (guard dropped outside a live runtime — process
/// shutdown), the row falls back to that same recoverable crash-residual
/// shape, with nothing executed.
struct SubmitRowGuard {
    armed: bool,
    /// The task's row pre-serialized with FAILED status (`Drop` can
    /// neither await nor serialize fallibly).
    failed_row: TaskRow,
    metadata: Arc<MetadataStore>,
    /// Join handle of the UNCANCELLABLE spawned RUNNING-row insert (D4):
    /// `Some` from [`attach_running_insert`] until
    /// [`await_running_insert`] has driven it to completion. If the
    /// submit future is cancelled while awaiting the join, the handle
    /// stays here so the `Drop` cleanup can ORDER the FAILED flip
    /// strictly AFTER the insert has truly resolved — the interleaving
    /// that used to strand a late-landing RUNNING row past the cleanup.
    ///
    /// [`attach_running_insert`]: SubmitRowGuard::attach_running_insert
    /// [`await_running_insert`]: SubmitRowGuard::await_running_insert
    running_insert: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl SubmitRowGuard {
    /// Build an ARMED guard for `record`, pre-serializing its FAILED row.
    fn armed(record: &TaskRecord, metadata: Arc<MetadataStore>) -> Result<Self> {
        let mut failed = record.clone();
        failed.state = TaskState::Failed;
        Ok(Self {
            armed: true,
            failed_row: failed.to_row()?,
            metadata,
            running_insert: None,
        })
    }

    /// Attach the spawned RUNNING-row insert's join handle (called
    /// synchronously right after the spawn — no cancellation point can
    /// land between the spawn and this attach).
    fn attach_running_insert(&mut self, handle: tokio::task::JoinHandle<Result<()>>) {
        self.running_insert = Some(handle);
    }

    /// Await the spawned RUNNING-row insert to completion, returning its
    /// result. The insert task itself is UNCANCELLABLE (a spawned task);
    /// only this await is — a cancellation parked here leaves the handle
    /// with the guard, whose `Drop` cleanup then finishes the ordering.
    ///
    /// On a definite store refusal (`Ok(Err)`) the guard DISARMS itself:
    /// the store rejected the row, so there is nothing durable to flip
    /// (and should a torn write have landed anyway, the next bootstrap
    /// reconcile recovers it — the pre-existing contract). On a join
    /// error (the insert task died — forbidden-by-policy panic) the
    /// guard STAYS ARMED: whether the row landed is unknown, and an
    /// idempotent FAILED upsert is truthful either way.
    async fn await_running_insert(&mut self) -> Result<()> {
        let Some(handle) = self.running_insert.as_mut() else {
            return Ok(());
        };
        let joined = handle.await;
        self.running_insert = None;
        match joined {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                self.disarm();
                Err(e)
            }
            Err(join_err) => Err(DruidError::Metadata(format!(
                "RUNNING-row insert task for {} did not run to completion: {join_err}",
                self.failed_row.id
            ))),
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SubmitRowGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let row = self.failed_row.clone();
        let metadata = Arc::clone(&self.metadata);
        let running_insert = self.running_insert.take();
        // Drop can't await, so the cleanup runs as a detached task (the
        // same shape as SubmitLockGuard). Guard against being dropped
        // outside a runtime (process shutdown).
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                // D4 ordering: the RUNNING insert runs in its own
                // UNCANCELLABLE spawned task, so awaiting its join here
                // guarantees the FAILED upsert below lands strictly
                // AFTER the RUNNING row has truly resolved (landed or
                // definitively refused). Pre-fix the two writes raced:
                // when the FAILED cleanup completed BEFORE a torn
                // RUNNING insert landed, RUNNING won and the row was
                // stranded non-terminal with no live owner until a
                // restart's bootstrap reconcile. The join result itself
                // is irrelevant — an idempotent FAILED upsert is
                // truthful whether or not the row exists.
                if let Some(insert) = running_insert {
                    let _ = insert.await;
                }
                match metadata.insert_task(&row).await {
                    Ok(()) => tracing::warn!(
                        task_id = %row.id,
                        "submit request future was cancelled before its batch \
                         tail was spawned; durable task row finalized FAILED \
                         (nothing executed, resubmission is safe)"
                    ),
                    Err(e) => tracing::warn!(
                        task_id = %row.id,
                        error = %e,
                        "failed to finalize the durable task row of a \
                         cancelled submit as FAILED; a stale RUNNING row (if \
                         the insert landed) is reconciled at the next bootstrap"
                    ),
                }
            });
        }
    }
}

/// State carried by a publish fence: the DURABLE-COMMIT truth of one
/// shielded publication-critical section, as far as it is known.
///
/// The verdict question every finalizer asks is NOT "did the publish
/// function return `Ok`" but "is the task's DATA durably committed, such
/// that re-running/resubmitting the same input would double count" (F1).
/// Those diverge on two real paths: the append swap-failed +
/// rollback-failed residual RETAINS a committed row + blob yet returns
/// `Err`, and a death (panic or publish-deadline cancellation, F3) after
/// Phase M leaves a committed durable row with no `Ok` ever produced.
/// The section therefore keeps the fence's value honest AS IT GOES:
/// watchers that outlive the sender read the last state to classify.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FenceState {
    /// Publication in flight; nothing is durably committed yet (or a
    /// completed rollback un-committed it). A death here is NOT
    /// committed: nothing survives a restart, so a resubmission is safe.
    InFlight,
    /// Phase M committed a DURABLE (blob-backed) `appendToExisting` row
    /// and no rollback has succeeded since: the row + blob survive a
    /// restart's bootstrap reload, so a death from here on MUST read as
    /// committed — reporting FAILED would invite an append resubmission
    /// that double counts after the reload. Replace-mode publishes never
    /// enter this state: a replace resubmission re-plans its victims
    /// from used rows and is idempotent for the interval, so failing
    /// closed to NOT-committed both is safe and preserves the
    /// retry-heals-visibility behavior (F1: replace mode not weakened).
    DurableAppendCommitted,
    /// Final verdict: the section resolved normally.
    Resolved {
        /// Whether the publish's data is durably committed (swap landed,
        /// or the append durable-retained residual kept row + blob).
        committed: bool,
    },
}

/// One REGISTERED publish fence: the verdict channel of a shielded
/// publication-critical section plus the context every finalizer needs
/// to resolve a fence that closes WITHOUT a `Resolved` verdict (D1) —
/// the publish mode gating the durable-state resolution, and the gate
/// over the section's uncancellable in-flight store ops (D2) so no
/// point-in-time durable read can race a still-landing commit.
///
/// Cloned out of the registry by `shutdown_task` and the
/// [`BatchTailGuard`] recovery finalizer; consumed by
/// [`Overlord::await_registered_fence_verdict`].
#[derive(Clone)]
struct PublishFenceEntry {
    /// The fence's verdict channel (see [`FenceState`]).
    verdict: tokio::sync::watch::Receiver<FenceState>,
    /// Whether the publish is `appendToExisting` — the mode gate of
    /// [`Overlord::resolve_interrupted_publish_verdict`] (replace mode
    /// stays fail-closed NOT-committed: a replace resubmission is
    /// idempotent for the interval).
    append_to_existing: bool,
    /// Gate held by any UNCANCELLABLE tracked store op of the section
    /// ([`run_tracked_store_op`]) for the op's true duration — including
    /// past a deadline drop of the section itself. Finalizers bound-wait
    /// on it before reading durable state.
    store_ops: Arc<tokio::sync::Mutex<()>>,
}

/// Run one publication store op as its OWN spawned task, holding `gate`
/// for the op's TRUE duration (D2). The caller awaits the join handle;
/// if the surrounding section future is dropped at that await (publish
/// deadline, F3), the op itself keeps running to completion — it is
/// never torn mid-call into a "may still land later" write — and the
/// gate is released only when the op has truly resolved, so the
/// deadline path (and every finalizer) can bound-wait for the real
/// outcome before reading durable state and before releasing the
/// datasource publish lock. An op-task panic (forbidden by policy)
/// surfaces as an `Err` here; its unwind releases the gate.
async fn run_tracked_store_op<T>(
    gate: &Arc<tokio::sync::Mutex<()>>,
    op: impl std::future::Future<Output = Result<T>> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    let held = Arc::clone(gate).lock_owned().await;
    let handle = tokio::spawn(async move {
        let _held = held;
        op.await
    });
    match handle.await {
        Ok(result) => result,
        Err(join_err) => Err(DruidError::Ingestion(format!(
            "tracked publication store op did not run to completion: {join_err}"
        ))),
    }
}

/// Classify a fence's LAST state after its sender is gone (panic or
/// deadline cancellation): committed iff the section had durably
/// committed by the point it died. See [`FenceState`]. LAST-RESORT
/// fallback only (D1): every closed-without-verdict path first resolves
/// from durable state
/// ([`resolve_interrupted_publish_verdict`](Overlord::resolve_interrupted_publish_verdict))
/// and reaches this provisional classification only when the store
/// cannot answer at all.
fn fence_last_state_committed(fence: &tokio::sync::watch::Receiver<FenceState>) -> bool {
    matches!(
        *fence.borrow(),
        FenceState::DurableAppendCommitted | FenceState::Resolved { committed: true }
    )
}

/// Positive segment-kind marker stamped into every BATCH-published
/// segment payload (R9-F2). Streaming publishes stamp their own kinds
/// (`"kafka-streaming"` / `"kinesis-streaming"`), so a segment row's
/// batch-vs-streaming provenance is decidable from the row alone.
const BATCH_SEGMENT_KIND: &str = "batch";

/// Whether a segment metadata row's payload carries BATCH provenance
/// (R9-F2): the positive `kind == "batch"` marker, or NO `kind` at all —
/// legacy batch rows (published before the marker existed) carry no
/// `kind`, while every streaming publish (kafka/kinesis) has always
/// stamped one. Any OTHER `kind` (streaming, unknown-future, or a
/// malformed non-string) fails closed: task/supervisor ids are
/// user-controlled, so a coincidental `taskId` match on a non-batch row
/// must never count as batch-commit proof — it would mark an uncommitted
/// batch task SUCCESS and silently lose its data (the client never
/// resubmits a SUCCESS).
fn segment_payload_is_batch(payload: &serde_json::Value) -> bool {
    match payload.get("kind") {
        None => true,
        Some(kind) => kind.as_str() == Some(BATCH_SEGMENT_KIND),
    }
}

/// State of the live DETACHED batch execute+publish tails spawned by
/// [`Overlord::submit_task`] — their abort handles (review High on ship2
/// H9: a detached tail must stay killable) AND the publish fences of any
/// SHIELDED publication-critical sections currently in flight (the
/// append double-count fix on the H9 abort). One struct under ONE mutex
/// on purpose: `shutdown_task`'s "is a shielded publish in flight?"
/// decision and `execute_index_parallel`'s fence-registration/spawn are
/// linearized by the same lock, so an abort can never slip between "no
/// fence seen → mark FAILED" and "publication task spawned anyway".
#[derive(Default)]
struct BatchTails {
    /// Abort handles of the live outer batch tails, keyed by task id.
    tails: HashMap<String, tokio::task::AbortHandle>,
    /// Publish fences, keyed by task id: present exactly while (or
    /// after) that task's SHIELDED publication-critical section is/was
    /// in flight, carrying a [`FenceState`] — in-flight/provisional
    /// while publishing, `Resolved { committed }` once the section
    /// resolves (`committed == true` only for a DURABLY COMMITTED
    /// publish; see [`FenceState`] for the death shapes). `shutdown_task`
    /// clones the receiver and awaits the verdict BEFORE finalizing the
    /// task's terminal status + releasing its interval locks, so an
    /// abort landing inside the shielded window can never mark an
    /// append task FAILED (inviting a double-appending resubmission of
    /// input that actually committed). Fence LIFETIME = the publish's
    /// finalization, NOT the outer tail's: an entry stays discoverable
    /// until the truthful terminal status has been WRITTEN TO THE
    /// IN-MEMORY TABLE (`running_tasks` — the state every `shutdown_task`
    /// phase-1 decision reads), so a SECOND concurrent shutdown racing
    /// the first one's finalizer still finds the fence and awaits the
    /// verdict instead of eagerly failing a task whose publish may
    /// commit.
    ///
    /// INVARIANT (the whole registry's self-check): a fence is retired
    /// ONLY atomically with a truthful terminal in-memory state write —
    /// under the `running_tasks` write lock, in
    /// [`Overlord::finalize_batch_terminal`] or on the tail's persisted
    /// normal exit — so "no fence found" always means "the in-memory
    /// task state already tells the truth about the publish". Entries
    /// are removed (a) by the tail body on EVERY exit of its own code —
    /// normal completion via `persist_and_store`, or a failed terminal
    /// persist via `finalize_batch_terminal` (the committed-but-
    /// unpersisted case flips the in-memory state to SUCCESS first and
    /// flushes the durable row in a BOUNDED background retry — a fence
    /// is never parked waiting for a later explicit shutdown); (b) by a
    /// shutdown finalizer, atomically with its verdict-derived state
    /// write; or (c) by the [`BatchTailGuard`] recovery finalizer when
    /// the tail DIED (abort/panic) with its fence still registered. So
    /// entries can never accumulate (R3) on ANY path.
    ///
    /// Each entry is a [`PublishFenceEntry`]: the verdict receiver plus
    /// the publish mode and the in-flight store-op gate a finalizer
    /// needs to resolve a fence that closes WITHOUT a verdict from
    /// DURABLE state (D1/D2) instead of its provisional snapshot.
    publish_fences: HashMap<String, PublishFenceEntry>,
    /// Task ids with a LIVE bounded terminal-persist background retry
    /// ([`Overlord::spawn_terminal_persist_retry`]): a marker is
    /// inserted before the retry task spawns and removed when the loop
    /// exits (durable success OR attempt-cap exhaustion — both bounded),
    /// so concurrent finalizers (tail exit, shutdown, guard recovery)
    /// dedupe to at most ONE loop per task and the registry stays
    /// accumulation-free (R3).
    persist_retries: std::collections::HashSet<String>,
}

impl BatchTails {
    /// True when no tail is live, no publish fence is outstanding, and no
    /// terminal-persist retry is in flight — the R3 no-accumulation
    /// invariant, asserted by tests (test-only: production code only ever
    /// touches individual entries).
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.tails.is_empty() && self.publish_fences.is_empty() && self.persist_retries.is_empty()
    }
}

/// Registry of [`BatchTails`], keyed by task id. A `std::sync::Mutex` on
/// purpose: every access is a short synchronous insert/remove with no
/// `.await` under the guard, and the sync lock is what lets
/// `submit_task` register the record + abort handle around the spawn
/// with NO intervening await point — so a caller cancellation can never
/// strike between "tail spawned" and "tail registered". Lock order
/// everywhere: `running_tasks` (when taken) → this registry; nothing
/// awaits under the guard.
type BatchTailRegistry = std::sync::Mutex<BatchTails>;

/// Lock the batch-tail registry, recovering from poisoning: the maps'
/// entries are independent per-task pairs (no multi-step invariant a
/// panicking holder could have half-applied), so the state inside a
/// poisoned lock is still valid to use — and refusing here would make
/// every later task unkillable, the exact defect this registry fixes.
fn lock_batch_tails(registry: &BatchTailRegistry) -> std::sync::MutexGuard<'_, BatchTails> {
    registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Removes a batch tail's [`BatchTailRegistry`] abort-handle entry when
/// the tail finishes — on ANY exit: normal completion, a panic (the
/// unwind drops it), or a [`shutdown_task`](Overlord::shutdown_task)
/// abort (dropping the tail's future drops it). Held as the first local
/// of the spawned tail so the registry can never accumulate entries for
/// dead tails, no matter how the tail dies.
///
/// The publish fence is deliberately NOT removed synchronously here. Its
/// lifetime is the PUBLISH's (and its finalization's), not the outer
/// tail's: removing it on an ABORT drop opened a window — aborted tail
/// dropped, first shutdown's finalizer not yet landed — in which a
/// SECOND concurrent `shutdown_task` found neither tail nor fence, took
/// the eager path, and durably marked a task FAILED (releasing its
/// interval locks) while the shielded publish was still committing: a
/// client observing that FAILED could resubmit an append and permanently
/// double count. A fence is retired only atomically with a truthful
/// terminal in-memory state (see [`BatchTails::publish_fences`]).
///
/// A fence still registered when this guard drops therefore means the
/// tail's own body did NOT reach its exit path (an abort landed mid-body,
/// or a — forbidden-by-policy — panic unwound it). For that case the drop
/// spawns a detached RECOVERY finalizer that awaits the publish verdict
/// (resolving a fence that closed WITHOUT a verdict from DURABLE state,
/// D1) and finalizes truthfully via
/// [`finalize_batch_terminal`](Overlord::finalize_batch_terminal). On an
/// abort this is redundant with (and idempotent against — same verdict,
/// same state write, deduped persist retry) the aborting shutdown's own
/// finalizer; on a panic it is the ONLY thing standing between a
/// committed publish and a fence leaked until some later explicit
/// shutdown, so no exit of the tail can strand a fence (R3).
///
/// NO fence registered + a still NON-terminal task means the tail died
/// (panic/abort) strictly BEFORE fence registration (D3): no publish was
/// ever spawned — the registration re-checks this guard's registry reap
/// under the same mutex and refuses afterwards — so nothing committed
/// and nothing ever will. Pre-fix this shape was simply ignored, leaving
/// the task RUNNING forever (in memory AND in the durable store) with no
/// finalizer anywhere; the drop now spawns a recovery that finalizes it
/// FAILED and releases its locks, so a LIVE finalizer covers the WHOLE
/// tail lifetime, pre- and post-fence. On every normal exit the tail
/// finalized itself before this guard drops (the in-memory state is
/// already terminal), so that recovery no-ops without touching the
/// store.
struct BatchTailGuard {
    task_id: String,
    overlord: Overlord,
}

impl Drop for BatchTailGuard {
    fn drop(&mut self) {
        let orphaned_fence = {
            let mut registry = lock_batch_tails(&self.overlord.batch_tails);
            registry.tails.remove(&self.task_id);
            registry.publish_fences.get(&self.task_id).cloned()
        };
        // Recovery runs in a detached task (Drop cannot await). Guard
        // against being dropped outside a live runtime (process
        // shutdown) — the registry is process-local, so there is nothing
        // to leak then; a durable RUNNING residual is reconciled at the
        // next bootstrap.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let overlord = self.overlord.clone_handle();
        let task_id = std::mem::take(&mut self.task_id);
        match orphaned_fence {
            Some(fence) => {
                // The tail died with its fence registered: recover the
                // publish verdict.
                handle.spawn(async move {
                    // Resolution or death, the verdict is definite: a
                    // fence closed WITHOUT a `Resolved` verdict (panic /
                    // publish deadline) is resolved from DURABLE state
                    // (D1) — a durably-committed append reads COMMITTED
                    // (F1), everything else fails closed.
                    let committed = overlord
                        .await_registered_fence_verdict(&task_id, fence)
                        .await;
                    if let Err(e) = overlord
                        .finalize_batch_terminal(&task_id, committed, None)
                        .await
                    {
                        tracing::warn!(
                            task_id = %task_id,
                            error = %e,
                            "batch tail died with its publish fence registered; the \
                             recovery finalizer resolved the verdict but could not \
                             persist the terminal status (bounded background retry \
                             spawned)"
                        );
                    }
                });
            }
            None => {
                // D3: NO fence registered. On every NORMAL tail exit the
                // in-memory state is already terminal by the time this
                // guard drops (the tail body finalizes before returning),
                // so the check below no-ops. A NON-terminal state here
                // means the tail DIED (panic, or an abort — whose
                // shutdown wrote FAILED under the same lock before this
                // task can observe the state) strictly BEFORE its fence
                // was registered: no publish was ever spawned (the
                // registration re-checks this guard's registry reap under
                // the same mutex and refuses), so nothing committed and
                // nothing ever will — pre-fix such a task sat RUNNING
                // forever (in memory AND in the durable store), its
                // JoinHandle unobserved, with no finalizer anywhere.
                // Finalize it FAILED (truthful: nothing published) and
                // release its locks, idempotently against any concurrent
                // shutdown finalizer.
                handle.spawn(async move {
                    let non_terminal = {
                        let tasks = overlord.running_tasks.read().await;
                        tasks.get(&task_id).is_some_and(|t| !t.state.is_terminal())
                    };
                    if !non_terminal {
                        return;
                    }
                    tracing::warn!(
                        task_id = %task_id,
                        "batch tail died before its publish fence was \
                         registered (no publish spawned, nothing committed); \
                         finalizing the task FAILED and releasing its locks"
                    );
                    if let Err(e) = overlord
                        .finalize_batch_terminal(&task_id, false, None)
                        .await
                    {
                        tracing::warn!(
                            task_id = %task_id,
                            error = %e,
                            "pre-fence tail-death recovery finalized the task \
                             in-memory but could not persist the terminal \
                             status (bounded background retry spawned)"
                        );
                    }
                });
            }
        }
    }
}

/// Outcome of one shielded publication-critical section
/// ([`Overlord::run_publish_critical_section`]), separating the
/// DURABLE-COMMIT truth from the function-level result (F1): `committed`
/// is the fence's ground truth ("would re-running this input double
/// count?"), `result` is what the section's control flow produced. They
/// diverge exactly on the append swap-failed + rollback-failed residual
/// (row + blob durably retained, function-level `Err`) — and on the
/// timeout/panic reconstructions in `execute_index_parallel`.
struct PublishSectionOutcome {
    /// The [`Overlord::execute_index_parallel`] `retry_suppressed`
    /// OUT-contract (`true` ONLY when re-running could double count).
    retry_suppressed: bool,
    /// Whether the publish's data is DURABLY COMMITTED: the swap landed,
    /// or the append durable-retained residual kept its row + blob (a
    /// restart's bootstrap reload serves them). Ground truth for the
    /// task's terminal verdict: committed ⇒ SUCCESS. Replace-mode
    /// swap+rollback double failures stay `false` on purpose — a replace
    /// resubmission is idempotent for the interval and the retry heals
    /// the visibility gap (F1: replace behavior not weakened).
    committed: bool,
    /// The function-level result (logged, and surfaced when not
    /// committed).
    result: Result<()>,
}

/// Owned inputs of [`Overlord::run_publish_critical_section`], bundled so
/// the shielded publication task can be spawned as a plain `'static`
/// future (review High on ship2 H9) instead of threading a dozen loose
/// parameters through `tokio::spawn`.
struct PublishArgs {
    task_id: String,
    ds_name: String,
    segment_id: String,
    victims: Vec<SegmentMetadataRow>,
    append_to_existing: bool,
    segment_data: SegmentData,
    num_rows: usize,
    start_iso: String,
    end_iso: String,
    version: String,
}

/// Read a `local` inputSource: enumerate the regular files DIRECTLY under
/// `inputSource.baseDir` (non-recursive) whose file name matches the
/// optional `inputSource.filter` glob (default `"*"`), decode each with
/// `ioConfig.inputFormat`, and return the concatenated rows (files are
/// processed in sorted order for determinism).
///
/// Path safety: `baseDir` is canonicalized and every matched entry must
/// canonicalize to a path that stays UNDER it — a symlink (or any other
/// resolution) escaping `baseDir` is a hard error, so a spec can never
/// read files outside the directory it names. Zero matched files is a
/// hard error too (mirroring the inline empty-data behavior) so a typo'd
/// filter fails loud instead of publishing nothing.
fn read_local_input_source(
    input_source: &serde_json::Value,
    io_config: &serde_json::Value,
) -> Result<Vec<serde_json::Value>> {
    let format_value = io_config.get("inputFormat").ok_or_else(|| {
        DruidError::Ingestion(
            "ioConfig.inputFormat is required for the local inputSource".to_string(),
        )
    })?;
    let input_format: ferrodruid_ingest_batch::InputFormat =
        serde_json::from_value(format_value.clone())
            .map_err(|e| DruidError::Ingestion(format!("invalid ioConfig.inputFormat: {e}")))?;

    let base_dir = input_source
        .get("baseDir")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            DruidError::Ingestion(
                "inputSource.baseDir (string) is required for the local inputSource".to_string(),
            )
        })?;
    // An ABSENT filter defaults to match-all; a PRESENT-but-non-string
    // filter is a malformed spec (Codex R1 H3) — fail loud rather than
    // silently degrading to "*" and ingesting every file under baseDir.
    let filter = match input_source.get("filter") {
        None => "*",
        Some(serde_json::Value::String(s)) => s.as_str(),
        Some(other) => {
            return Err(DruidError::Ingestion(format!(
                "inputSource.filter must be a string glob, got: {other}"
            )));
        }
    };

    let canonical_base = std::fs::canonicalize(base_dir).map_err(|e| {
        DruidError::Ingestion(format!(
            "inputSource.baseDir {base_dir:?} is not readable: {e}"
        ))
    })?;
    if !canonical_base.is_dir() {
        return Err(DruidError::Ingestion(format!(
            "inputSource.baseDir {base_dir:?} is not a directory"
        )));
    }

    let entries = std::fs::read_dir(&canonical_base).map_err(|e| {
        DruidError::Ingestion(format!(
            "failed to list inputSource.baseDir {base_dir:?}: {e}"
        ))
    })?;
    let mut files: Vec<(std::path::PathBuf, std::fs::Metadata)> = Vec::new();
    for entry in entries {
        let entry = entry
            .map_err(|e| DruidError::Ingestion(format!("failed to read a baseDir entry: {e}")))?;
        // A non-UTF-8 file name cannot be reliably glob-matched, and
        // silently skipping it would drop input the user expects to ingest
        // (Codex R1 H4). Fail loud rather than publish incomplete data.
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            return Err(DruidError::Ingestion(format!(
                "inputSource.baseDir {base_dir:?} contains a non-UTF-8 file name \
                 ({name:?}); refusing to enumerate to avoid silently dropping input"
            )));
        };
        if !glob_matches(filter, name_str) {
            continue;
        }
        let path = entry.path();
        // Resolve symlinks; the result must stay under the canonical
        // baseDir or the spec is reading outside the directory it names.
        let resolved = std::fs::canonicalize(&path).map_err(|e| {
            DruidError::Ingestion(format!("failed to resolve input file {path:?}: {e}"))
        })?;
        if !resolved.starts_with(&canonical_base) {
            return Err(DruidError::Ingestion(format!(
                "input file {path:?} resolves outside baseDir {base_dir:?} \
                 (symlink/traversal escape rejected)"
            )));
        }
        // Capture the enumeration-time metadata of the CANONICAL file so
        // the read can require the opened inode to match (Codex R2 H1
        // ancestor-symlink identity gate). `resolved` is canonical, so
        // `symlink_metadata` == `metadata` here.
        let resolved_meta = std::fs::symlink_metadata(&resolved).map_err(|e| {
            DruidError::Ingestion(format!("failed to stat input file {resolved:?}: {e}"))
        })?;
        if !resolved_meta.is_file() {
            // Sub-directories and other non-files never match.
            continue;
        }
        files.push((resolved, resolved_meta));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    // Two entries that canonicalize to the same file (e.g. a real file and
    // an in-base symlink aliasing it) would otherwise be read — and thus
    // ingested — twice; dedupe the resolved set so the row count is stable.
    files.dedup_by(|a, b| a.0 == b.0);

    if files.is_empty() {
        return Err(DruidError::Ingestion(format!(
            "no input files matched filter {filter:?} under inputSource.baseDir {base_dir:?}"
        )));
    }

    let mut rows = Vec::new();
    for (path, expected_meta) in &files {
        // Race-safe read (unix): O_NOFOLLOW + O_NONBLOCK + regular-file
        // recheck + (dev,inode) identity match against the enumerated file,
        // so a symlink/FIFO/ancestor-symlink swapped in after the
        // containment check cannot be followed outside baseDir, block, or
        // change the inode read (Codex R1 H2 + R2 H1/H2).
        let bytes = read_regular_file_checked(path, expected_meta).map_err(|e| {
            DruidError::Ingestion(format!("failed to read input file {path:?}: {e}"))
        })?;
        rows.extend(input_format.parse_bytes(&bytes).map_err(|e| {
            DruidError::Ingestion(format!("failed to parse input file {path:?}: {e}"))
        })?);
    }
    Ok(rows)
}

/// Whether a `dataSchema.transformSpec` value is semantically EMPTY — JSON
/// null, `{}`, or an object whose only keys are an empty/null `transforms`
/// and a null `filter` (the no-op shape Druid tooling serializes). Anything
/// else carries filter/transform content that native-batch ingestion does
/// not apply and must therefore reject loudly (compat-9): pre-fix a
/// transformSpec was silently dropped and rows were ingested
/// unfiltered/untransformed.
fn transform_spec_is_inert(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Null => true,
        serde_json::Value::Object(obj) => obj.iter().all(|(k, val)| match k.as_str() {
            "transforms" => val.is_null() || val.as_array().is_some_and(Vec::is_empty),
            "filter" => val.is_null(),
            _ => false,
        }),
        _ => false,
    }
}

/// Parse a Druid native-batch (`index` / `index_parallel`) ingestion spec
/// into the form [`BatchIngester`] consumes.
///
/// Returns `Ok(None)` if the spec is well-formed but not a batch shape at
/// all (missing `dataSchema` / `ioConfig` / `inputSource`); the caller
/// should fall back to stub behavior in that case so the wire envelope
/// stays honest.  A recognised batch spec with an UNSUPPORTED
/// `inputSource` type (s3/http/hdfs/...) is an `Err` instead — the task
/// must terminate FAILED (releasing its locks) rather than park PENDING
/// with locks held.
fn parse_index_parallel_spec(spec: &serde_json::Value) -> Result<Option<ParsedIndexSpec>> {
    let inner = spec.get("spec").unwrap_or(spec);

    let data_schema = match inner.get("dataSchema") {
        Some(v) => v,
        None => return Ok(None),
    };

    let data_source = data_schema
        .get("dataSource")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DruidError::Ingestion("dataSchema.dataSource is required".to_string()))?
        .to_string();

    let timestamp_column = data_schema
        .get("timestampSpec")
        .and_then(|t| t.get("column"))
        .and_then(|v| v.as_str())
        .unwrap_or("__time")
        .to_string();

    // Declared `timestampSpec.format`, mapped through the SHARED
    // `TsFormat::from_spec_format` (same helper as the Kafka/Kinesis
    // supervisor validation) so batch honors `iso`/`millis` and rejects
    // unimplemented formats LOUDLY — pre-fix (compat-9 P0) the format was
    // never read and everything parsed as `auto`, silently storing wrong
    // instants for a declared `iso` (`"2023"` → 2023 ms) and for
    // posix/nano/custom patterns. Absent/null defaults to `auto`
    // (Druid's default); a non-string fails closed rather than guessing.
    let timestamp_format = match data_schema
        .get("timestampSpec")
        .and_then(|t| t.get("format"))
    {
        None | Some(serde_json::Value::Null) => TsFormat::Auto,
        Some(serde_json::Value::String(s)) => {
            TsFormat::from_spec_format(s).map_err(DruidError::Ingestion)?
        }
        Some(other) => {
            return Err(DruidError::Ingestion(format!(
                "dataSchema.timestampSpec.format must be a string, got: {other}"
            )));
        }
    };

    // transformSpec (row filter / derived columns) is NOT applied by
    // native-batch ingestion: honoring the pre-fix silent drop would
    // ingest rows unfiltered/untransformed — a correctness divergence.
    // Reject any transformSpec with content (mirroring the Kafka
    // streaming rejection); a semantically-empty one (null / `{}` /
    // `{"transforms": [], "filter": null}` — the no-op shape Druid
    // tooling emits) is inert and stays accepted.
    if let Some(ts) = data_schema.get("transformSpec")
        && !transform_spec_is_inert(ts)
    {
        return Err(DruidError::Ingestion(
            "dataSchema.transformSpec is not supported by native-batch ingestion \
             (its filter/transforms would be silently ignored, ingesting rows \
             unfiltered/untransformed)"
                .to_string(),
        ));
    }

    let dimensions: Vec<ferrodruid_ingest_batch::DimensionSchema> = data_schema
        .get("dimensionsSpec")
        .and_then(|d| d.get("dimensions"))
        .and_then(|v| v.as_array())
        .map(|arr| ferrodruid_ingest_batch::parse_dimension_entries(arr))
        .transpose()?
        .unwrap_or_default();

    let metrics_specs: Vec<serde_json::Value> = data_schema
        .get("metricsSpec")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let (rollup, query_granularity) = parse_granularity_spec(data_schema)?;
    let segment_granularity = data_schema
        .get("granularitySpec")
        .and_then(|g| g.get("segmentGranularity"))
        .cloned();

    let io_config = match inner.get("ioConfig") {
        Some(v) => v,
        None => return Ok(None),
    };

    // Druid's native-batch `ioConfig` defaults `appendToExisting` to FALSE
    // (replace).  A non-boolean value fails closed: silently coercing it
    // would pick replace-vs-append semantics by guesswork.
    let append_to_existing = match io_config.get("appendToExisting") {
        None | Some(serde_json::Value::Null) => false,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(other) => {
            return Err(DruidError::Ingestion(format!(
                "ioConfig.appendToExisting must be a boolean, got: {other}"
            )));
        }
    };
    let input_source = match io_config.get("inputSource") {
        Some(v) => v,
        None => return Ok(None),
    };
    let src_type = input_source.get("type").and_then(|v| v.as_str());
    let rows: Vec<serde_json::Value> = match src_type {
        // Inline data — Druid accepts either a JSONL string or a JSON
        // array of row objects.  We support both.
        Some("inline") => {
            let raw_data = input_source.get("data").ok_or_else(|| {
                DruidError::Ingestion("inputSource.data is required for inline source".to_string())
            })?;
            match raw_data {
                serde_json::Value::String(s) => s
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .map(|line| {
                        serde_json::from_str::<serde_json::Value>(line).map_err(|e| {
                            DruidError::Ingestion(format!("inline JSONL parse error: {e}"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                serde_json::Value::Array(arr) => arr.clone(),
                other => {
                    return Err(DruidError::Ingestion(format!(
                        "inputSource.data must be a JSONL string or array, got: {other}"
                    )));
                }
            }
        }
        // Local files under a base directory, decoded per
        // ioConfig.inputFormat.
        Some("local") => read_local_input_source(input_source, io_config)?,
        // Anything else (s3/http/hdfs/druid/...) is a hard error, NOT
        // `Ok(None)`: the stub fallback reverts the task to PENDING with
        // its locks still held, which read as accepted-but-hung forever.
        // An error terminates the task FAILED and releases its locks.
        other => {
            return Err(DruidError::Ingestion(format!(
                "unsupported inputSource type \"{}\" (batch ingestion supports: inline, local)",
                other.unwrap_or("<absent>")
            )));
        }
    };

    Ok(Some(ParsedIndexSpec {
        data_source,
        timestamp_column,
        timestamp_format,
        dimensions,
        metrics_specs,
        rows,
        rollup,
        query_granularity,
        append_to_existing,
        segment_granularity,
    }))
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Status of an ingestion task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskStatus {
    /// Waiting for a worker slot.
    Pending,
    /// Currently executing.
    Running,
    /// Completed successfully.
    Success,
    /// Completed with failure.
    Failed,
}

/// Location of a running task on a worker node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLocation {
    /// Worker hostname.
    pub host: String,
    /// Worker HTTP port.
    pub port: u16,
    /// Optional TLS port.
    pub tls_port: Option<u16>,
}

/// Information about a single ingestion task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    /// Unique task identifier.
    pub id: String,
    /// Task type (e.g. `"index_kafka"`, `"index_parallel"`).
    pub task_type: String,
    /// Target data source name.
    pub data_source: String,
    /// Current status (legacy 4-state view; `WAITING` is surfaced here as
    /// `PENDING` for backward compatibility — use [`TaskInfo::state`] for the
    /// full lifecycle state).
    pub status: TaskStatus,
    /// When the task was created.
    pub created_time: DateTime<Utc>,
    /// Where the task is running (populated once `Running`).
    pub location: Option<TaskLocation>,
    /// Full lifecycle state (`WAITING`/`PENDING`/`RUNNING`/`SUCCESS`/`FAILED`).
    #[serde(default = "default_task_state")]
    pub state: TaskState,
    /// Number of execution attempts made so far.
    #[serde(default)]
    pub attempt: u32,
    /// Worker id (`host:port`) this task is currently assigned to, if any.
    #[serde(default)]
    pub worker: Option<String>,
}

fn default_task_state() -> TaskState {
    TaskState::Pending
}

impl TaskStatus {
    /// Project a full [`TaskState`] onto the legacy 4-state [`TaskStatus`].
    ///
    /// `WAITING` collapses to `Pending` so existing callers that filter on
    /// `Pending` for "waiting tasks" keep working.
    fn from_state(state: TaskState) -> Self {
        match state {
            TaskState::Waiting | TaskState::Pending => TaskStatus::Pending,
            TaskState::Running => TaskStatus::Running,
            TaskState::Success => TaskStatus::Success,
            TaskState::Failed => TaskStatus::Failed,
        }
    }
}

// ---------------------------------------------------------------------------
// Overlord
// ---------------------------------------------------------------------------

/// Internal lifecycle record for a task.
#[derive(Debug, Clone)]
struct TaskRecord {
    id: String,
    task_type: String,
    data_source: String,
    state: TaskState,
    created_time: DateTime<Utc>,
    location: Option<TaskLocation>,
    /// Number of execution attempts made so far.
    attempt: u32,
    /// Worker id this task is assigned to (`host:port`), if any.
    worker: Option<String>,
}

impl TaskRecord {
    fn to_info(&self) -> TaskInfo {
        TaskInfo {
            id: self.id.clone(),
            task_type: self.task_type.clone(),
            data_source: self.data_source.clone(),
            status: TaskStatus::from_state(self.state),
            created_time: self.created_time,
            location: self.location.clone(),
            state: self.state,
            attempt: self.attempt,
            worker: self.worker.clone(),
        }
    }

    fn to_row(&self) -> Result<TaskRow> {
        let info = self.to_info();
        let payload = serde_json::to_value(&info)
            .map_err(|e| DruidError::Metadata(format!("serialize task info: {e}")))?;
        Ok(TaskRow {
            id: self.id.clone(),
            task_type: self.task_type.clone(),
            data_source: self.data_source.clone(),
            status: self.state.as_str().to_string(),
            created_date: self.created_time.to_rfc3339(),
            attempt: i64::from(self.attempt),
            worker: self.worker.clone(),
            payload,
        })
    }
}

/// Attempts for a transient bootstrap-reload step (existence-check error,
/// download, open, or load) before a DURABLE (`loadSpec`) row's failure is
/// treated as a hard durability error (H4).
const BOOTSTRAP_RELOAD_ATTEMPTS: usize = 3;

/// Backoff between bootstrap-reload retry attempts (H4).
const BOOTSTRAP_RELOAD_BACKOFF: std::time::Duration = std::time::Duration::from_millis(200);

/// Attempts for the BOUNDED background retry of a batch task's failed
/// terminal-status persist (the publish-fence leak fix): a transient
/// metadata-store outage is flushed on recovery instead of leaving the
/// durable task row RUNNING until an explicit `shutdown_task` or restart.
/// After the cap the loop gives up LOUDLY (see
/// [`Overlord::spawn_terminal_persist_retry`]): the in-memory status
/// already carries the truth, and the stale durable RUNNING row converges
/// to the same documented crash-between-publish-and-terminal-persist
/// residual that `submit_task` already accepts.
const TERMINAL_PERSIST_RETRY_ATTEMPTS: u32 = 10;

/// Base backoff between terminal-persist retry attempts, doubled per
/// attempt up to [`TERMINAL_PERSIST_RETRY_MAX_BACKOFF`]. Test builds use
/// short delays so the bounded-exhaustion path is exercisable in-process;
/// the loop's LOGIC is identical in both builds.
#[cfg(not(test))]
const TERMINAL_PERSIST_RETRY_BASE_BACKOFF: std::time::Duration =
    std::time::Duration::from_millis(100);
#[cfg(test)]
const TERMINAL_PERSIST_RETRY_BASE_BACKOFF: std::time::Duration =
    std::time::Duration::from_millis(20);

/// Ceiling for the doubling terminal-persist retry backoff.
#[cfg(not(test))]
const TERMINAL_PERSIST_RETRY_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(test)]
const TERMINAL_PERSIST_RETRY_MAX_BACKOFF: std::time::Duration =
    std::time::Duration::from_millis(40);

/// Per-ATTEMPT timeout on a terminal-status persist await (both the
/// finalizer's direct attempt and every bounded-retry attempt): a HUNG
/// metadata op — as opposed to one that fails fast — must not park an
/// attempt forever, or the attempt cap never exhausts and the retry
/// marker is never removed (the F4 hang-leak). A timed-out attempt is
/// counted exactly like a failed one; the dropped store call is
/// cancelled at its await point. Test builds use short timeouts so the
/// hung-attempt path is exercisable in-process; the LOGIC is identical.
#[cfg(not(test))]
const TERMINAL_PERSIST_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
#[cfg(test)]
const TERMINAL_PERSIST_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

/// Default deadline for ONE shielded publication-critical section
/// (deep-storage upload → metadata transaction → swap, plus every
/// failure-path compensation). Without a deadline a single hung upload
/// or metadata op kept the publish fence's sender alive FOREVER, so
/// every shutdown/guard finalizer awaiting the verdict parked forever
/// and each hang accumulated one fence + one task (the F3 leak).
/// Overridable via [`Overlord::with_publish_deadline`] (tests use
/// millisecond deadlines). Generous on purpose: it only exists to bound
/// a genuine hang, not to race a slow-but-working upload.
const PUBLISH_DEADLINE_DEFAULT: std::time::Duration = std::time::Duration::from_secs(600);

/// Bound on the wait for a deadline-dropped publication's UNCANCELLABLE
/// in-flight store op ([`run_tracked_store_op`]) to truly resolve before
/// the interrupted verdict is read from durable state (D2). The deadline
/// task holds the datasource publish lock across this wait, so a
/// subsequent publisher cannot interleave with a still-landing commit,
/// and the point-in-time durable read cannot contradict what actually
/// lands. Generous on purpose: the section's own deadline already fired,
/// and this bound only exists so a metadata store that is GENUINELY hung
/// (not merely slow) cannot park the deadline path forever. Documented
/// residual: if the op outlives even this bound, the lock is released
/// while the op may still land — the verdict then degrades to the same
/// point-in-time-read residual class as a crash between publish and
/// persist (and the op task itself leaks until the store resolves it).
const PUBLISH_OP_RESOLVE_BOUND: std::time::Duration = std::time::Duration::from_secs(30);

/// Default bound on how long a lock-conflicted batch submission may
/// queue WAITING for its interval lock before its waiter finalizes it
/// FAILED (truthful: it never ran, nothing committed, a resubmission is
/// safe). Druid parity twice over: a lock-blocked task queues until the
/// lock frees (the waiter re-attempts acquisition with bounded backoff),
/// and the 5-minute default mirrors Apache Druid's `taskLockTimeout`
/// default (300 s). Overridable via
/// [`Overlord::with_lock_wait_deadline`] (tests use millisecond
/// deadlines). The bound exists so an accepted task can never hang
/// forever with no resolution behind a lock that never frees.
const LOCK_WAIT_DEADLINE_DEFAULT: std::time::Duration = std::time::Duration::from_secs(300);

/// Base backoff between a WAITING waiter's interval-lock re-acquisition
/// attempts (doubles up to [`LOCK_WAIT_RETRY_MAX_BACKOFF`]).
#[cfg(not(test))]
const LOCK_WAIT_RETRY_BASE_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);
/// Test override of [`LOCK_WAIT_RETRY_BASE_BACKOFF`]: fast retries so a
/// released lock is picked up within milliseconds.
#[cfg(test)]
const LOCK_WAIT_RETRY_BASE_BACKOFF: std::time::Duration = std::time::Duration::from_millis(10);

/// Cap on the waiter's re-acquisition backoff.
#[cfg(not(test))]
const LOCK_WAIT_RETRY_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);
/// Test override of [`LOCK_WAIT_RETRY_MAX_BACKOFF`].
#[cfg(test)]
const LOCK_WAIT_RETRY_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_millis(20);

/// Outcome of a SINGLE bootstrap-reload attempt for one used metadata row
/// (H4). Copy so the retry loop can re-inspect the last outcome after the
/// loop ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReloadAttempt {
    /// Downloaded, opened, and loaded into the Historical.
    Loaded,
    /// `segment_exists` returned `Ok(false)` — the blob is DETERMINISTICALLY
    /// absent (not a transient error), so retrying cannot recover it.
    BlobMissing,
    /// The downloaded blob's content hash does NOT match the hash recorded in
    /// the durable metadata row (H2): the blob was swapped for a different
    /// valid v9 artifact or silently corrupted. DETERMINISTIC — retrying the
    /// same durable bytes cannot recover it.
    ContentMismatch,
    /// A transient/corrupt failure (existence-check error, download, open, or
    /// load) that MAY succeed on a retry.
    Transient,
}

/// Attempt ONE bootstrap reload of a single used segment: existence check →
/// download → content-hash verify (H2) → open → load into the Historical
/// (H4). Distinguishes a deterministically-missing blob
/// ([`ReloadAttempt::BlobMissing`]) and a content-identity mismatch
/// ([`ReloadAttempt::ContentMismatch`]) from a transient/corrupt failure
/// ([`ReloadAttempt::Transient`]) so the caller retries only the latter and
/// fails loud on a durable row's persistent or deterministic failure.
///
/// `expected_hash` is the content hash recorded in the metadata row's
/// `payload.loadSpec.sha256`, if any. When present, the downloaded blob is
/// re-hashed and compared BEFORE it is opened or loaded, so a swapped /
/// silently-corrupted durable segment is rejected rather than served (H2). A
/// row with NO recorded hash (a legacy / pre-compat-3-hash durable row) skips
/// the check for backward compatibility.
async fn attempt_bootstrap_reload(
    historical: &Historical,
    deep_storage: &dyn DeepStorage,
    ds: &str,
    id: &str,
    expected_hash: Option<&str>,
) -> ReloadAttempt {
    match deep_storage.segment_exists(ds, id).await {
        Ok(true) => {}
        Ok(false) => return ReloadAttempt::BlobMissing,
        Err(e) => {
            tracing::warn!(
                data_source = %ds, segment_id = %id, error = %e,
                "bootstrap reload: deep-storage existence check failed (transient)",
            );
            return ReloadAttempt::Transient;
        }
    }
    let staging = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                segment_id = %id, error = %e,
                "bootstrap reload: could not create staging tempdir (transient)",
            );
            return ReloadAttempt::Transient;
        }
    };
    let dest = staging.path().join("v9");
    if let Err(e) = deep_storage.download_segment(ds, id, &dest).await {
        tracing::warn!(
            data_source = %ds, segment_id = %id, error = %e,
            "bootstrap reload: download failed (transient)",
        );
        return ReloadAttempt::Transient;
    }
    // H2: verify the downloaded blob's content identity against the hash
    // recorded in the durable metadata row BEFORE opening or loading it. A
    // blob swapped for a different valid v9 artifact — or silently corrupted
    // so it still decodes — changes this hash and is rejected fail-loud rather
    // than served. A hashing error (e.g. the staging dir vanished) is treated
    // as transient. A row with no recorded hash (legacy) skips the check.
    if let Some(expected) = expected_hash {
        match crate::persist::blob_content_hash(&dest) {
            Ok(actual) if actual == expected => {}
            Ok(actual) => {
                tracing::error!(
                    data_source = %ds, segment_id = %id,
                    expected = %expected, actual = %actual,
                    "bootstrap reload: downloaded blob content hash MISMATCH — durable segment \
                     swapped or silently corrupted (H2)",
                );
                return ReloadAttempt::ContentMismatch;
            }
            Err(e) => {
                tracing::warn!(
                    data_source = %ds, segment_id = %id, error = %e,
                    "bootstrap reload: could not hash downloaded blob (transient)",
                );
                return ReloadAttempt::Transient;
            }
        }
    }
    let seg = match SegmentData::open(&dest) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                data_source = %ds, segment_id = %id, error = %e,
                "bootstrap reload: could not open downloaded segment (transient/corrupt)",
            );
            return ReloadAttempt::Transient;
        }
    };
    // `load_segment_with_datasource` routes through `load_spilled` under FG-7
    // spill mode automatically, so bootstrap works in both residency modes.
    if let Err(e) = historical.load_segment_with_datasource(id, ds, seg) {
        tracing::warn!(
            data_source = %ds, segment_id = %id, error = %e,
            "bootstrap reload: load into Historical failed (transient)",
        );
        return ReloadAttempt::Transient;
    }
    ReloadAttempt::Loaded
}

/// The Overlord manages the full lifecycle of ingestion tasks and
/// supervisor specs.
///
/// Tasks are stored in-memory for Phase 1; supervisor specs are persisted
/// to the [`MetadataStore`].
///
/// When constructed via [`Overlord::with_executor`], `submit_task` will
/// drive `index_parallel` + `inputSource.type=inline` ingestion specs all
/// the way through:
///
/// 1. Parse `dataSchema` (timestamp/dimensions/metrics) and the inline
///    JSONL or JSON-array data.
/// 2. Invoke [`BatchIngester::ingest`] to produce a `SegmentData`.
/// 3. With `ioConfig.appendToExisting: false` (Druid's batch default),
///    REPLACE: every used segment of the datasource whose interval
///    overlaps — and is fully contained in — the segmentGranularity
///    buckets covering the new data is swapped out for the new segment.
///    If an overlapping segment extends beyond those buckets the task
///    FAILS CLOSED instead of silently dropping its out-of-scope rows.
///    `true` appends (segments coexist). The whole publish sequence is
///    serialized per datasource on the shared
///    [`MetadataStore::datasource_publish_lock`] — the same mutex the
///    Coordinator's used-flag mutations take, so an admin disable can
///    never interleave with a publish. The segment id is allocated
///    collision-free under that lock (same-millisecond publications get a
///    numeric suffix instead of silently overwriting each other).
/// 4. Persist the metadata changes first as ONE atomic transaction
///    ([`MetadataStore::replace_segments_txn`]: victims marked unused AND
///    the new [`SegmentMetadataRow`] inserted, so coordinator endpoints
///    surface it); a metadata failure — or a crash mid-transaction —
///    leaves metadata untouched and aborts before the query path is
///    touched.
/// 5. Apply the query-visible change as ONE atomic
///    [`Historical::replace_segments`] swap (victims out, new segment +
///    datasource mapping in), so a concurrent query observes either the
///    full old segment set or the full new one — never a partial mix,
///    a double-counted interval, or an unregistered orphan. A swap
///    failure is compensated by one
///    [`MetadataStore::rollback_replace_txn`] transaction under the
///    still-held publish lock.
///
/// Constructed via [`Overlord::new`] (no executor) the task is recorded
/// as `Pending` only — the original Phase-1 stub behavior — to preserve
/// backwards compatibility with unit-test setups that do not own a
/// Historical.
pub struct Overlord {
    metadata: Arc<MetadataStore>,
    historical: Option<Arc<Historical>>,
    /// Deep-storage backend used to PERSIST every published segment before
    /// its metadata row is committed (compat-3 stage 1). `None` keeps the
    /// pre-persistence behavior (segments memory-resident only), so unit
    /// setups that do not own a backend still work. When set, both publish
    /// tails order the sequence persist → metadata → swap and
    /// [`bootstrap_reload_segments`](Overlord::bootstrap_reload_segments)
    /// re-downloads every used segment on startup.
    deep_storage: Option<Arc<dyn DeepStorage>>,
    running_tasks: Arc<RwLock<HashMap<String, TaskRecord>>>,
    /// Abort handles for the live detached batch execute+publish tails
    /// (the ship2-H9 spawns), keyed by task id — review High on H9: the
    /// spawn that made the tail survive a client disconnect must NOT also
    /// make it unkillable. `submit_task` registers the handle (together
    /// with the RUNNING record in `running_tasks`) BEFORE the tail can be
    /// observed, [`shutdown_task`](Overlord::shutdown_task) uses it to
    /// abort a live tail, and the tail's own [`BatchTailGuard`] removes
    /// the entry on every exit (completion, panic, abort) so entries can
    /// never accumulate. Also carries the publish fences
    /// ([`BatchTails::publish_fences`]) `shutdown_task` awaits so an
    /// abort landing during a shielded publication can never mark an
    /// append task FAILED whose publish actually committed (the append
    /// double-count fix).
    batch_tails: Arc<BatchTailRegistry>,
    /// Monotonically increasing counter for generating task IDs.
    task_counter: Arc<std::sync::atomic::AtomicU64>,
    /// Pluggable worker selector (registered workers + strategy).
    workers: Arc<Mutex<WorkerSelector>>,
    /// Retry policy for failed/lost tasks.
    retry_policy: RetryPolicy,
    /// Per-datasource acquisition mutexes serializing the whole
    /// read-active-locks → evaluate-conflicts → revoke → insert sequence
    /// in [`try_acquire_lock`]. Without this, two concurrent overlapping
    /// EXCLUSIVE requests could both read an empty active set and both be
    /// granted. The outer map is itself guarded so that the per-datasource
    /// mutex is created/looked up atomically; the inner `Arc<Mutex<()>>` is
    /// then held across the entire acquire sequence for that datasource.
    ///
    /// The segment-publication critical section of
    /// [`execute_index_parallel`] uses a DIFFERENT lock: the store-level
    /// [`MetadataStore::datasource_publish_lock`], shared with the
    /// Coordinator's used-flag mutations (`disable_segment` /
    /// `disable_datasource` / `enable_datasource`) so a publish and an
    /// admin disable are mutually exclusive per datasource (Codex
    /// 2026-07-12 round-2 HIGH #2/#3). It moved off this struct precisely
    /// so that lock lives where every mutator of the shared metadata can
    /// reach it; the single-process scope caveat is documented on
    /// [`MetadataStore::datasource_publish_lock`].
    ///
    /// [`execute_index_parallel`]: Overlord::execute_index_parallel
    lock_acquire_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    /// One-way gate closing the bootstrap-only orphaned-lock reconcile
    /// window (review High on ship2 H10). `false` (open) = this process
    /// has never begun a task-lock acquisition, so every persisted lock
    /// row belongs to a PRIOR process; `true` (closed, permanent) = at
    /// least one local acquisition has begun, so a persisted lock row may
    /// belong to a live local task that is not (yet) in `running_tasks`
    /// — `submit_task` grants the lock BEFORE `persist_and_store` inserts
    /// the record — and can no longer be told apart from a crash orphan.
    ///
    /// [`try_acquire_lock`](Overlord::try_acquire_lock) stamps it `true`
    /// under this mutex BEFORE touching any acquisition state;
    /// [`reconcile_orphaned_task_locks`](Overlord::reconcile_orphaned_task_locks)
    /// holds the same mutex for its WHOLE reap and refuses (no-op) once
    /// closed. Holding one mutex for both gives a hard happens-before:
    /// either a reap completes before any local lock exists, or the
    /// stamp wins and every later reap refuses — a live local lock can
    /// never be deleted. Shared (`Arc`) across
    /// [`clone_handle`](Overlord::clone_handle) copies so the spawned
    /// batch/publish tails observe the same window.
    task_lock_reconcile_gate: Arc<Mutex<bool>>,
    /// Deadline for one shielded publication-critical section (F3). A
    /// publish exceeding it is cancelled at its current await point and
    /// resolved from DURABLE state (R9-F1/D1, with the publish lock held
    /// and any uncancellable tracked store op awaited first, D2; the
    /// fence's provisional state — see [`FenceState`] — is only the
    /// store-unreachable fallback) — never left as an unresolved fence
    /// that parks shutdown/guard finalizers forever. Immutable config,
    /// copied by [`clone_handle`](Overlord::clone_handle); defaults to
    /// [`PUBLISH_DEADLINE_DEFAULT`], overridable via
    /// [`with_publish_deadline`](Overlord::with_publish_deadline).
    publish_deadline: std::time::Duration,
    /// Bound on how long a lock-conflicted batch submission's WAITING
    /// waiter queues for its interval lock before finalizing the task
    /// FAILED (see [`run_lock_waiter`](Overlord::run_lock_waiter)).
    /// Immutable config, copied by
    /// [`clone_handle`](Overlord::clone_handle); defaults to
    /// [`LOCK_WAIT_DEADLINE_DEFAULT`], overridable via
    /// [`with_lock_wait_deadline`](Overlord::with_lock_wait_deadline).
    lock_wait_deadline: std::time::Duration,
    /// Test-only fault injection: while `true`, the atomic publish
    /// metadata transaction in [`execute_index_parallel`] fails with an
    /// injected [`DruidError::Metadata`] before touching the store,
    /// exercising the publication failure path (Codex 2026-07-12 HIGH #2,
    /// re-pointed at the round-2 single-transaction publish). Sticky until
    /// cleared so every retry attempt of a task observes the same fault.
    ///
    /// [`execute_index_parallel`]: Overlord::execute_index_parallel
    #[cfg(test)]
    inject_insert_segment_failure: Arc<std::sync::atomic::AtomicBool>,
    /// Test-only fault injection: while `true`, the COMPENSATING rollback
    /// transaction after a failed Phase-3 swap in
    /// [`execute_index_parallel`] fails with an injected
    /// [`DruidError::Metadata`] before touching the store, exercising the
    /// swap-failed + rollback-failed residual (Codex R23 H1: committed
    /// post-replace metadata with the query-visible state unreconciled).
    /// Sticky until cleared.
    ///
    /// [`execute_index_parallel`]: Overlord::execute_index_parallel
    #[cfg(test)]
    inject_rollback_replace_failure: Arc<std::sync::atomic::AtomicBool>,
    /// Test-only fault injection: the number of upcoming TERMINAL-status
    /// task persists ([`persist_task_row_checked`]) that fail with an
    /// injected [`DruidError::Metadata`] — a stand-in for a transient
    /// metadata-store outage at exactly the point that used to leak a
    /// committed publish's fence. A counter (not a sticky bool) so a test
    /// can fail the first N persists and let the bounded background
    /// retry succeed on recovery.
    ///
    /// [`persist_task_row_checked`]: Overlord::persist_task_row_checked
    #[cfg(test)]
    inject_terminal_persist_failures: Arc<std::sync::atomic::AtomicU32>,
    /// Test-only fault injection: the number of upcoming terminal-status
    /// task persists ([`persist_task_row_checked`]) that HANG (park on a
    /// never-resolving future) instead of failing fast — a stand-in for
    /// a hung metadata op. The caller's per-attempt timeout
    /// ([`TERMINAL_PERSIST_ATTEMPT_TIMEOUT`], F4) must cancel each hung
    /// attempt so the attempt cap still advances and the retry marker is
    /// still removed. Consumed AFTER
    /// [`inject_terminal_persist_failures`], so a test can fail the
    /// first N persists fast and hang the rest.
    ///
    /// [`persist_task_row_checked`]: Overlord::persist_task_row_checked
    /// [`inject_terminal_persist_failures`]: Overlord::inject_terminal_persist_failures
    #[cfg(test)]
    inject_terminal_persist_hangs: Arc<std::sync::atomic::AtomicU32>,
    /// Test-only pause hook: while a test holds this mutex, every
    /// shielded publication-critical section
    /// ([`run_publish_critical_section`]) parks at its entry — publish
    /// lock held, section genuinely in flight — so a test can land a
    /// [`shutdown_task`](Overlord::shutdown_task) abort deterministically
    /// INSIDE the shielded window (the append double-count fix).
    ///
    /// [`run_publish_critical_section`]: Overlord::run_publish_critical_section
    #[cfg(test)]
    test_publish_pause: Arc<Mutex<()>>,
    /// Test-only observability for [`test_publish_pause`]: flips `true`
    /// the moment a shielded publication section has STARTED (and is
    /// about to park on the pause mutex), so a test knows the abort it
    /// is about to send lands during the shielded window.
    ///
    /// [`test_publish_pause`]: Overlord::test_publish_pause
    #[cfg(test)]
    test_publish_entered: Arc<std::sync::atomic::AtomicBool>,
    /// Test-only pause hook (R9-F1): while a test holds this mutex, every
    /// shielded publication-critical section parks JUST AFTER its Phase-M
    /// metadata commit (durable row committed, fence provisionally
    /// [`FenceState::DurableAppendCommitted`] for a durable append, swap
    /// NOT yet applied), so a test can deterministically drop the section
    /// at the publish deadline inside the committed-but-unresolved window
    /// and exercise the durable-truth re-derivation of the verdict.
    #[cfg(test)]
    test_post_commit_pause: Arc<Mutex<()>>,
    /// Test-only observability for [`test_post_commit_pause`]: flips
    /// `true` the moment a section has COMMITTED its Phase-M metadata
    /// transaction and is about to park on the post-commit pause mutex.
    ///
    /// [`test_post_commit_pause`]: Overlord::test_post_commit_pause
    #[cfg(test)]
    test_post_commit_entered: Arc<std::sync::atomic::AtomicBool>,
    /// Test-only fault injection (D3): while `true`, the batch tail
    /// PANICS at the top of [`execute_index_parallel`] — strictly BEFORE
    /// the publish fence is registered — so a test can exercise the
    /// tail-died-with-no-fence recovery of [`BatchTailGuard`] (a panic
    /// there used to strand the task RUNNING forever: no fence, no
    /// finalizer, unobserved `JoinHandle`).
    ///
    /// [`execute_index_parallel`]: Overlord::execute_index_parallel
    #[cfg(test)]
    inject_pre_fence_panic: Arc<std::sync::atomic::AtomicBool>,
    /// Test-only pause hook (D4): while a test holds this mutex, the
    /// spawned RUNNING-row insert task of every batch `submit_task` parks
    /// BEFORE touching the store — simulating a torn/slow insert whose
    /// store-side landing is delayed past a submit cancellation — so a
    /// test can deterministically order "cancellation cleanup runs" vs.
    /// "RUNNING insert lands".
    #[cfg(test)]
    test_running_insert_pause: Arc<Mutex<()>>,
    /// Test-only observability for [`test_running_insert_pause`]: flips
    /// `true` the moment a spawned RUNNING-row insert task has started
    /// (and is about to park on the pause mutex).
    ///
    /// [`test_running_insert_pause`]: Overlord::test_running_insert_pause
    #[cfg(test)]
    test_running_insert_entered: Arc<std::sync::atomic::AtomicBool>,
    /// Test-only pause hook (D2): while a test holds this mutex, every
    /// Phase-M publish metadata commit ([`publish_replace_metadata`])
    /// parks BEFORE touching the store — a stand-in for a mid-commit
    /// store op still in flight when the publish deadline fires — so a
    /// test can deterministically drop the section at the deadline while
    /// the commit is "still landing".
    ///
    /// [`publish_replace_metadata`]: Overlord::publish_replace_metadata
    #[cfg(test)]
    test_commit_op_pause: Arc<Mutex<()>>,
    /// Test-only observability for [`test_commit_op_pause`]: flips `true`
    /// the moment a Phase-M commit has started (and is about to park on
    /// the commit-op pause mutex).
    ///
    /// [`test_commit_op_pause`]: Overlord::test_commit_op_pause
    #[cfg(test)]
    test_commit_op_entered: Arc<std::sync::atomic::AtomicBool>,
    /// Running Kafka supervisor consumer tasks, keyed by supervisor id, so
    /// [`shutdown_supervisor`](Overlord::shutdown_supervisor) can stop the
    /// one it owns. Present only with the `kafka-io` feature (real Kafka
    /// I/O); without it, supervisor specs are persisted but no consumer
    /// runs. Behind an `Arc` because the uncancellable spawned lifecycle
    /// ops ([`kafka::run_lifecycle_op`], Codex R23) register the consumer
    /// they start from inside the `'static` op body.
    #[cfg(feature = "kafka-io")]
    kafka_supervisors: Arc<Mutex<HashMap<String, kafka::KafkaSupervisorHandle>>>,
    /// Serializes the whole persist→spawn→register (create) and
    /// tombstone→stop→remove (shutdown) supervisor-lifecycle transitions so
    /// a concurrent create and shutdown for the same id cannot interleave.
    /// Without it, a shutdown landing between a create's spec-persist and
    /// its consumer-spawn would tombstone the metadata row yet leave an
    /// orphan consumer running (Codex 2026-07-13). Present in EVERY build
    /// (Codex R27 F4): the default (no-kafka-io) build spawns no consumers,
    /// but its `create_supervisor` still runs the persisted-layer
    /// (dataSource, topic) pair-uniqueness check
    /// ([`refuse_persisted_kafka_pair_conflict`](Overlord::refuse_persisted_kafka_pair_conflict))
    /// as check-then-act across await points — two concurrent POSTs for the
    /// same pair under different ids could both pass the check and both be
    /// persisted (the later kafka-io resume then warn-skips one, silently
    /// disabling it). Lock order everywhere: `supervisor_lifecycle` →
    /// `kafka_lifecycle_ops` (drain) → `kafka_supervisors` / datasource
    /// publish locks; nothing ever takes this lock while holding a later
    /// one.
    supervisor_lifecycle: Arc<Mutex<()>>,
    /// Spawned supervisor-lifecycle operations ([`kafka::run_lifecycle_op`])
    /// whose join handles must gate the NEXT lifecycle operation. Each op
    /// runs a WHOLE destructive-then-restorative tail — (earliest-replay)
    /// cleanup → spec persist → consumer start → registration — in one
    /// spawned task, so a cancelled caller (HTTP disconnect) can never tear
    /// the cleanup apart from the consumer start that replays what it
    /// deleted (Codex R23; supersedes the R22 cleanup-only registry). But
    /// that same cancellation releases `supervisor_lifecycle` while the op
    /// keeps running (an ORPHAN escaping the lifecycle serialization —
    /// Codex R22), so every op handle is pushed here at spawn and
    /// [`create_supervisor`](Overlord::create_supervisor),
    /// [`resume_kafka_supervisors`](Overlord::resume_kafka_supervisors) AND
    /// [`shutdown_supervisor`](Overlord::shutdown_supervisor) drain (await)
    /// it right after taking the lifecycle lock: no lifecycle operation
    /// proceeds while any prior op is still in flight. Shutdown must drain
    /// too (R23): an in-flight op may be about to register the very
    /// consumer the shutdown exists to stop — without the drain it would
    /// report "not found" and the op would then register a consumer nothing
    /// ever stops. The drain also reaps completed handles. Op bodies take
    /// only the datasource publish lock, metadata handles, and the
    /// `kafka_supervisors` lock — never this registry or the lifecycle
    /// lock — so the drain cannot deadlock.
    #[cfg(feature = "kafka-io")]
    kafka_lifecycle_ops: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    /// Whether the BACKGROUND resume-retry task (Codex R6 H3) is currently
    /// running, so at most ONE such task ever exists per Overlord
    /// (idempotence: `resume_kafka_supervisors` may be called again while a
    /// retry loop is still working through earlier failures). Set by
    /// [`spawn_kafka_resume_retry`](Overlord::spawn_kafka_resume_retry) via
    /// compare-and-swap, cleared by the task itself when a retry pass
    /// reports zero remaining failures.
    #[cfg(feature = "kafka-io")]
    kafka_resume_retry_active: Arc<std::sync::atomic::AtomicBool>,
    /// Running Kinesis supervisor consumer tasks, keyed by supervisor id
    /// (compat-5) — the Kinesis mirror of
    /// [`kafka_supervisors`](Self::kafka_supervisors), with the pair key
    /// `(data_source, stream)`. Present only with `kinesis-io`; without
    /// it, Kinesis specs are validated + persisted but no consumer runs.
    #[cfg(feature = "kinesis-io")]
    kinesis_supervisors: Arc<Mutex<HashMap<String, kinesis::KinesisSupervisorHandle>>>,
    /// Spawned Kinesis supervisor-lifecycle operations
    /// ([`run_lifecycle_op`]) — the Kinesis mirror of
    /// [`kafka_lifecycle_ops`](Self::kafka_lifecycle_ops), drained (with
    /// it) by every lifecycle entry point right after taking the
    /// lifecycle lock, so no lifecycle operation proceeds while any prior
    /// op of EITHER transport is still in flight.
    #[cfg(feature = "kinesis-io")]
    kinesis_lifecycle_ops: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    /// Whether the background Kinesis resume-retry task is running (the
    /// mirror of
    /// [`kafka_resume_retry_active`](Self::kafka_resume_retry_active)).
    #[cfg(feature = "kinesis-io")]
    kinesis_resume_retry_active: Arc<std::sync::atomic::AtomicBool>,
}

impl Overlord {
    /// Create a new Overlord backed by the given metadata store.
    ///
    /// Tasks submitted via this Overlord are recorded as `Pending`
    /// without driving an ingestion executor.  Use [`with_executor`]
    /// for the production path that actually produces segments.
    ///
    /// [`with_executor`]: Overlord::with_executor
    pub fn new(metadata: Arc<MetadataStore>) -> Self {
        Self {
            metadata,
            historical: None,
            deep_storage: None,
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            batch_tails: Arc::new(std::sync::Mutex::new(BatchTails::default())),
            task_counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            workers: Arc::new(Mutex::new(WorkerSelector::new(
                Vec::new(),
                WorkerSelectStrategy::RoundRobin,
            ))),
            retry_policy: RetryPolicy::default(),
            lock_acquire_locks: Arc::new(Mutex::new(HashMap::new())),
            task_lock_reconcile_gate: Arc::new(Mutex::new(false)),
            publish_deadline: PUBLISH_DEADLINE_DEFAULT,
            lock_wait_deadline: LOCK_WAIT_DEADLINE_DEFAULT,
            #[cfg(test)]
            inject_insert_segment_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            inject_rollback_replace_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            inject_terminal_persist_failures: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            #[cfg(test)]
            inject_terminal_persist_hangs: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            #[cfg(test)]
            test_publish_pause: Arc::new(Mutex::new(())),
            #[cfg(test)]
            test_publish_entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            test_post_commit_pause: Arc::new(Mutex::new(())),
            #[cfg(test)]
            test_post_commit_entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            inject_pre_fence_panic: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            test_running_insert_pause: Arc::new(Mutex::new(())),
            #[cfg(test)]
            test_running_insert_entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            test_commit_op_pause: Arc::new(Mutex::new(())),
            #[cfg(test)]
            test_commit_op_entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "kafka-io")]
            kafka_supervisors: Arc::new(Mutex::new(HashMap::new())),
            supervisor_lifecycle: Arc::new(Mutex::new(())),
            #[cfg(feature = "kafka-io")]
            kafka_lifecycle_ops: Arc::new(Mutex::new(Vec::new())),
            #[cfg(feature = "kafka-io")]
            kafka_resume_retry_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "kinesis-io")]
            kinesis_supervisors: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "kinesis-io")]
            kinesis_lifecycle_ops: Arc::new(Mutex::new(Vec::new())),
            #[cfg(feature = "kinesis-io")]
            kinesis_resume_retry_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Create a new Overlord wired to a [`Historical`] for in-process
    /// segment publication.
    ///
    /// `index_parallel` specs with an inline input source will be fully
    /// executed: the batch ingester runs synchronously inside
    /// `submit_task`, the resulting segment is loaded into the supplied
    /// Historical, and the metadata store is updated so coordinator and
    /// SQL paths can find it.
    pub fn with_executor(metadata: Arc<MetadataStore>, historical: Arc<Historical>) -> Self {
        Self {
            metadata,
            historical: Some(historical),
            deep_storage: None,
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            batch_tails: Arc::new(std::sync::Mutex::new(BatchTails::default())),
            task_counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            workers: Arc::new(Mutex::new(WorkerSelector::new(
                Vec::new(),
                WorkerSelectStrategy::RoundRobin,
            ))),
            retry_policy: RetryPolicy::default(),
            lock_acquire_locks: Arc::new(Mutex::new(HashMap::new())),
            task_lock_reconcile_gate: Arc::new(Mutex::new(false)),
            publish_deadline: PUBLISH_DEADLINE_DEFAULT,
            lock_wait_deadline: LOCK_WAIT_DEADLINE_DEFAULT,
            #[cfg(test)]
            inject_insert_segment_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            inject_rollback_replace_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            inject_terminal_persist_failures: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            #[cfg(test)]
            inject_terminal_persist_hangs: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            #[cfg(test)]
            test_publish_pause: Arc::new(Mutex::new(())),
            #[cfg(test)]
            test_publish_entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            test_post_commit_pause: Arc::new(Mutex::new(())),
            #[cfg(test)]
            test_post_commit_entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            inject_pre_fence_panic: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            test_running_insert_pause: Arc::new(Mutex::new(())),
            #[cfg(test)]
            test_running_insert_entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            test_commit_op_pause: Arc::new(Mutex::new(())),
            #[cfg(test)]
            test_commit_op_entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "kafka-io")]
            kafka_supervisors: Arc::new(Mutex::new(HashMap::new())),
            supervisor_lifecycle: Arc::new(Mutex::new(())),
            #[cfg(feature = "kafka-io")]
            kafka_lifecycle_ops: Arc::new(Mutex::new(Vec::new())),
            #[cfg(feature = "kafka-io")]
            kafka_resume_retry_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "kinesis-io")]
            kinesis_supervisors: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "kinesis-io")]
            kinesis_lifecycle_ops: Arc::new(Mutex::new(Vec::new())),
            #[cfg(feature = "kinesis-io")]
            kinesis_resume_retry_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Override the retry policy (builder-style; consumes and returns self).
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Attach a deep-storage backend (builder-style; consumes and returns
    /// self).
    ///
    /// Once set, every published segment (batch and streaming) is persisted
    /// to deep storage BEFORE its metadata row is committed, and
    /// [`bootstrap_reload_segments`](Overlord::bootstrap_reload_segments)
    /// re-downloads every used segment on startup so the datasource survives
    /// a restart.
    #[must_use]
    pub fn with_deep_storage(mut self, deep_storage: Arc<dyn DeepStorage>) -> Self {
        self.deep_storage = Some(deep_storage);
        self
    }

    /// Override the shielded-publish deadline (builder-style; consumes
    /// and returns self).
    ///
    /// Bounds ONE publication-critical section (deep-storage upload →
    /// metadata transaction → swap + compensations, F3): a publish that
    /// exceeds the deadline is cancelled at its current await point and
    /// resolved to a definite verdict by QUERYING DURABLE STATE
    /// (committed iff a durable batch segment row names the task — see
    /// [`resolve_interrupted_publish_verdict`](Overlord::resolve_interrupted_publish_verdict),
    /// R9-F1; otherwise NOT-committed / FAILED), so shutdown and guard
    /// finalizers are never parked forever behind a hung upload or
    /// metadata op. Defaults to [`PUBLISH_DEADLINE_DEFAULT`]; tests use
    /// millisecond deadlines so a hung publish resolves fast.
    #[must_use]
    pub fn with_publish_deadline(mut self, deadline: std::time::Duration) -> Self {
        self.publish_deadline = deadline;
        self
    }

    /// Override the lock-wait deadline (builder-style; consumes and
    /// returns self).
    ///
    /// Bounds how long a lock-conflicted batch (`index` /
    /// `index_parallel`) submission may queue WAITING for its interval
    /// lock: its tracked waiter re-attempts acquisition with bounded
    /// backoff until the conflicting lock frees (the task then runs the
    /// normal execute→publish→terminal path) or this deadline expires
    /// (the task is finalized FAILED — truthful: it never ran, nothing
    /// committed, a resubmission is safe). Defaults to
    /// [`LOCK_WAIT_DEADLINE_DEFAULT`] (300 s, Apache Druid's
    /// `taskLockTimeout` default); tests use millisecond deadlines.
    #[must_use]
    pub fn with_lock_wait_deadline(mut self, deadline: std::time::Duration) -> Self {
        self.lock_wait_deadline = deadline;
        self
    }

    /// Override the worker-selection strategy and pre-register workers
    /// (builder-style; consumes and returns self).
    #[must_use]
    pub fn with_workers(self, workers: Vec<Worker>, strategy: WorkerSelectStrategy) -> Self {
        self.workers
            .try_lock()
            .map(|mut guard| *guard = WorkerSelector::new(workers, strategy))
            .ok();
        self
    }

    /// Cheap handle onto the SAME overlord state (every field is a shared
    /// `Arc` handle or immutable config), so the batch execute + publish
    /// tail can be spawned as a DETACHED `'static` task (ship2 H9, the
    /// batch mirror of [`run_lifecycle_op`]'s spawn): a client disconnect
    /// dropping the HTTP request future must never cancel a P→M→swap
    /// publish mid-flight nor strand a RUNNING task row forever. The tail
    /// is not unkillable, though (review High on H9): its abort handle is
    /// registered in `batch_tails` so `shutdown_task` can stop a stalled
    /// tail, with only the publication-critical section shielded. The
    /// handle observes and mutates the identical `running_tasks` /
    /// `workers` / lock registries — it is NOT a second overlord.
    fn clone_handle(&self) -> Self {
        Self {
            metadata: Arc::clone(&self.metadata),
            historical: self.historical.clone(),
            deep_storage: self.deep_storage.clone(),
            running_tasks: Arc::clone(&self.running_tasks),
            batch_tails: Arc::clone(&self.batch_tails),
            task_counter: Arc::clone(&self.task_counter),
            workers: Arc::clone(&self.workers),
            retry_policy: self.retry_policy,
            lock_acquire_locks: Arc::clone(&self.lock_acquire_locks),
            task_lock_reconcile_gate: Arc::clone(&self.task_lock_reconcile_gate),
            publish_deadline: self.publish_deadline,
            lock_wait_deadline: self.lock_wait_deadline,
            #[cfg(test)]
            inject_insert_segment_failure: Arc::clone(&self.inject_insert_segment_failure),
            #[cfg(test)]
            inject_rollback_replace_failure: Arc::clone(&self.inject_rollback_replace_failure),
            #[cfg(test)]
            inject_terminal_persist_failures: Arc::clone(&self.inject_terminal_persist_failures),
            #[cfg(test)]
            inject_terminal_persist_hangs: Arc::clone(&self.inject_terminal_persist_hangs),
            #[cfg(test)]
            test_publish_pause: Arc::clone(&self.test_publish_pause),
            #[cfg(test)]
            test_publish_entered: Arc::clone(&self.test_publish_entered),
            #[cfg(test)]
            test_post_commit_pause: Arc::clone(&self.test_post_commit_pause),
            #[cfg(test)]
            test_post_commit_entered: Arc::clone(&self.test_post_commit_entered),
            #[cfg(test)]
            inject_pre_fence_panic: Arc::clone(&self.inject_pre_fence_panic),
            #[cfg(test)]
            test_running_insert_pause: Arc::clone(&self.test_running_insert_pause),
            #[cfg(test)]
            test_running_insert_entered: Arc::clone(&self.test_running_insert_entered),
            #[cfg(test)]
            test_commit_op_pause: Arc::clone(&self.test_commit_op_pause),
            #[cfg(test)]
            test_commit_op_entered: Arc::clone(&self.test_commit_op_entered),
            #[cfg(feature = "kafka-io")]
            kafka_supervisors: Arc::clone(&self.kafka_supervisors),
            supervisor_lifecycle: Arc::clone(&self.supervisor_lifecycle),
            #[cfg(feature = "kafka-io")]
            kafka_lifecycle_ops: Arc::clone(&self.kafka_lifecycle_ops),
            #[cfg(feature = "kafka-io")]
            kafka_resume_retry_active: Arc::clone(&self.kafka_resume_retry_active),
            #[cfg(feature = "kinesis-io")]
            kinesis_supervisors: Arc::clone(&self.kinesis_supervisors),
            #[cfg(feature = "kinesis-io")]
            kinesis_lifecycle_ops: Arc::clone(&self.kinesis_lifecycle_ops),
            #[cfg(feature = "kinesis-io")]
            kinesis_resume_retry_active: Arc::clone(&self.kinesis_resume_retry_active),
        }
    }

    // ----- Bootstrap -------------------------------------------------------

    /// Reload every used segment from deep storage into the Historical at
    /// startup (compat-3 stage 1, eager v1).
    ///
    /// Segments are held fully in-heap (or spilled to this instance's
    /// private spill root under FG-7 spill mode), so a restart starts with an
    /// EMPTY Historical. Deep storage is the durable source: this
    /// re-downloads each `used = TRUE` metadata row's blob and loads it (with
    /// its datasource mapping), so the datasource is query-visible again
    /// without replaying any upstream source.
    ///
    /// The Historical's initial-load flag is held `false` for the duration so
    /// `/status/health` does not advertise readiness before the sweep
    /// finishes.
    ///
    /// **Fail-closed on a durable segment (H4).** A used row is DURABLE when
    /// its payload carries a `loadSpec` (it was published under persistence).
    /// Such a row's blob is expected to exist and load; a transient
    /// download/open/load failure is RETRIED
    /// ([`BOOTSTRAP_RELOAD_ATTEMPTS`]) and, if it still cannot be reloaded — or
    /// its blob is deterministically MISSING — the bootstrap FAILS (returns
    /// `Err`, leaving the initial-load flag `false`) rather than advertise
    /// readiness with committed data SILENTLY ABSENT. Unlike a streaming
    /// segment, a batch segment is not recoverable by Kafka replay, so a silent
    /// skip would be permanent data loss on every query. A LEGACY row (no
    /// `loadSpec`, published before persistence) whose blob is missing is still
    /// warn-skipped — it cannot be reconstructed, so refusing to start would be
    /// worse than a documented partial reload.
    ///
    /// A no-op returning `Ok(0)` when either a Historical or a deep-storage
    /// backend is absent (e.g. an Overlord built without
    /// [`with_deep_storage`](Overlord::with_deep_storage)).
    ///
    /// **Stage-1 scope (honest limitation):** this does NOT change Kafka
    /// offset/resume behavior. A restart can therefore both reload a
    /// streaming segment here AND replay its records from Kafka — a transient
    /// double-count resolved in stage 2 (offset commit + resume re-audit).
    /// Callers MUST run this BEFORE
    /// [`resume_kafka_supervisors`](Overlord::resume_kafka_supervisors).
    pub async fn bootstrap_reload_segments(&self) -> Result<usize> {
        // Reconcile crash-orphaned durable task locks FIRST (ship2 H10),
        // BEFORE the no-backend early return below: the lock table is
        // durable in every configuration, and an orphan blocks its
        // datasource interval whether or not segments can be reloaded.
        // Non-fatal: a reconcile failure must not stop the segment reload
        // (the orphan then simply persists until the next startup).
        if let Err(e) = self.reconcile_orphaned_task_locks().await {
            tracing::warn!(
                error = %e,
                "startup task-lock reconciliation failed; task locks orphaned \
                 by a previous process (if any) may still block their \
                 datasource intervals until the next restart"
            );
        }
        // Recover the durable VERDICT of stale RUNNING batch rows (F2),
        // also before the no-backend early return: the task table is
        // durable in every configuration. Non-fatal for the same reason
        // as above.
        if let Err(e) = self.reconcile_stale_running_batch_tasks().await {
            tracing::warn!(
                error = %e,
                "startup stale-RUNNING batch-task reconciliation failed; a \
                 committed pre-restart task may keep reporting RUNNING (and \
                 invite a duplicate resubmission) until the next restart"
            );
        }
        let (Some(historical), Some(deep_storage)) =
            (self.historical.as_ref(), self.deep_storage.as_ref())
        else {
            return Ok(0);
        };

        historical.set_initial_load_complete(false);
        let mut reloaded = 0usize;
        let used = self.metadata.get_used_segments_all().await?;
        for row in used {
            let ds = row.data_source.as_str();
            let id = row.id.as_str();
            // A row that carries a `loadSpec` was published UNDER persistence
            // (durable). Silently skipping it and then advertising readiness
            // would serve queries with committed data SILENTLY ABSENT, with no
            // replay to reconstruct a batch segment (H4).
            let durable = row.payload.get("loadSpec").is_some();
            // Content-identity hash recorded at persist time (H2), if any. Only
            // durable rows carry a `loadSpec`; a `loadSpec` without a `sha256`
            // is a legacy / pre-compat-3-hash row whose blob is not verified.
            let expected_hash = row
                .payload
                .get("loadSpec")
                .and_then(|ls| ls.get("sha256"))
                .and_then(serde_json::Value::as_str);

            // Retry only TRANSIENT failures; a deterministically-missing blob
            // (existence == Ok(false)) or a content-hash mismatch is not retried.
            let mut outcome = ReloadAttempt::Transient;
            for attempt in 1..=BOOTSTRAP_RELOAD_ATTEMPTS {
                outcome = attempt_bootstrap_reload(
                    historical,
                    deep_storage.as_ref(),
                    ds,
                    id,
                    expected_hash,
                )
                .await;
                match outcome {
                    ReloadAttempt::Loaded
                    | ReloadAttempt::BlobMissing
                    | ReloadAttempt::ContentMismatch => break,
                    ReloadAttempt::Transient => {
                        if attempt < BOOTSTRAP_RELOAD_ATTEMPTS {
                            tokio::time::sleep(BOOTSTRAP_RELOAD_BACKOFF).await;
                        }
                    }
                }
            }

            match outcome {
                ReloadAttempt::Loaded => reloaded += 1,
                ReloadAttempt::BlobMissing => {
                    if durable {
                        // Fail-closed: return BEFORE set_initial_load_complete(true)
                        // so the node never advertises readiness with a durable
                        // segment missing (H4).
                        return Err(DruidError::Segment(format!(
                            "bootstrap reload: durable segment {ds}/{id} (a loadSpec metadata \
                             row) is MISSING from deep storage — refusing to start with \
                             committed data silently absent; restore the blob or mark the row \
                             unused (H4/H7)"
                        )));
                    }
                    tracing::warn!(
                        data_source = %ds, segment_id = %id,
                        "bootstrap reload: legacy used row (no loadSpec) has no deep-storage \
                         blob (pre-persistence or manually deleted) — skipping; not \
                         query-visible until re-ingested",
                    );
                }
                ReloadAttempt::ContentMismatch => {
                    // Deterministic and only reachable for a durable row that
                    // recorded a content hash: the deep-storage blob no longer
                    // matches what was committed. Fail-closed (return BEFORE
                    // set_initial_load_complete(true)) rather than open and
                    // serve different data than the metadata attests to (H2).
                    return Err(DruidError::Segment(format!(
                        "bootstrap reload: durable segment {ds}/{id} (a loadSpec metadata row) \
                         FAILED its content-hash integrity check — the deep-storage blob was \
                         swapped or silently corrupted; refusing to start and serve different \
                         data than was committed; restore the correct blob or mark the row \
                         unused (H2/H7)"
                    )));
                }
                ReloadAttempt::Transient => {
                    if durable {
                        return Err(DruidError::Segment(format!(
                            "bootstrap reload: durable segment {ds}/{id} (a loadSpec metadata \
                             row) could not be reloaded after {BOOTSTRAP_RELOAD_ATTEMPTS} \
                             attempts (download/open/load failing) — refusing to start with \
                             committed data silently absent (H4/H7)"
                        )));
                    }
                    tracing::warn!(
                        data_source = %ds, segment_id = %id,
                        "bootstrap reload: legacy used row (no loadSpec) failed to reload after \
                         retries — skipping (best-effort partial reload)",
                    );
                }
            }
        }
        historical.set_initial_load_complete(true);
        tracing::info!(reloaded, "bootstrap reload complete");
        Ok(reloaded)
    }

    // ----- Tasks -----------------------------------------------------------

    /// Submit a new ingestion task from its JSON spec — ASYNCHRONOUSLY,
    /// matching Apache Druid's contract: the task id is returned as soon
    /// as the task is durably accepted, and the caller learns the verdict
    /// by POLLING the task status ([`get_task`] / the REST
    /// `GET /druid/indexer/v1/task/{id}/status`).
    ///
    /// For an Overlord built with [`with_executor`] and a native-batch
    /// spec (`index` / `index_parallel`, inline or local inputSource)
    /// this synchronously acquires the spec's interval locks, persists
    /// the durable RUNNING task row, registers + spawns the DETACHED
    /// execute→publish→terminal-persist tail, and returns the id — it
    /// does NOT await the tail. For an Overlord built with [`new`] it
    /// falls back to stub behavior (record as Pending, return ID).
    ///
    /// The only errors are PRE-EXECUTION ones (malformed lock request,
    /// lock/store failures before the tail exists) — nothing has run and
    /// nothing can commit when `Err` is returned. Once `Ok(id)` is
    /// returned, no failure is ever reported through this call: the
    /// polled status (written by the fence-derived finalizers) is the
    /// single source of the verdict, so a submitter can never observe a
    /// failure for a publish that actually commits (the R10 class).
    ///
    /// Returns the auto-generated task identifier.
    ///
    /// [`get_task`]: Overlord::get_task
    /// [`with_executor`]: Overlord::with_executor
    /// [`new`]: Overlord::new
    pub async fn submit_task(&self, spec: serde_json::Value) -> Result<String> {
        // The dataSource for ID generation may live at the top level
        // (legacy Phase-1 form) or under spec.spec.dataSchema.dataSource
        // (Druid `index_parallel`).
        let task_type = spec
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let mut data_source = spec
            .get("dataSource")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if data_source.is_empty() {
            data_source = spec
                .pointer("/spec/dataSchema/dataSource")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
        }

        // Generate an id that is collision-free ACROSS RESTARTS (ship2 H8):
        // the in-process counter restarts at 1 every process start while
        // task rows are DURABLE, and `MetadataStore::insert_task` is an
        // UPSERT on id — a reused id would silently clobber a pre-restart
        // task's persisted status row (a client polling that id via the
        // R3-H1 restart fallback then sees the NEW task's state, concludes
        // its completed ingestion failed, and resubmits a duplicate). Skip
        // over any persisted id; the atomic counter keeps concurrent
        // in-process submissions unique, so this loop only walks past ids
        // persisted by PREVIOUS processes (bounded by their row count).
        let id = loop {
            let seq = self
                .task_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let candidate = format!("{task_type}_{data_source}_{seq}");
            if self.metadata.get_task(&candidate).await?.is_none() {
                break candidate;
            }
        };

        // Every task starts WAITING (blocked on lock acquisition).
        let mut record = TaskRecord {
            id: id.clone(),
            task_type: task_type.clone(),
            data_source: data_source.clone(),
            state: TaskState::Waiting,
            created_time: Utc::now(),
            location: None,
            attempt: 0,
            worker: None,
        };

        // Acquire an interval lock if the spec carries one. A failure to
        // acquire (blocked by a higher/equal-priority holder) leaves the task
        // WAITING — the caller can retry submission or poll.
        //
        // The lock must target the datasource the publish tail actually
        // WRITES: `dataSchema.dataSource`, resolved exactly as
        // `parse_index_parallel_spec` resolves it (`spec.spec` when
        // present, else the root). The ID-derivation `data_source` above
        // prefers the legacy TOP-LEVEL `dataSource` field, which nothing
        // publishes to — pre-fix the lock was acquired for that field, so
        // a spec carrying both with different values held its exclusive
        // interval lock on the wrong datasource and two tasks writing the
        // same real datasource + interval never conflicted. Specs with no
        // `dataSchema.dataSource` (stub shapes that never publish) keep
        // the ID-derivation datasource.
        let lock_data_source = spec
            .get("spec")
            .unwrap_or(&spec)
            .pointer("/dataSchema/dataSource")
            .and_then(|v| v.as_str())
            .unwrap_or(&data_source)
            .to_string();
        let lock_req = self.parse_lock_request(&id, &lock_data_source, &spec)?;
        // Armed once a lock is actually granted; releases the durable lock
        // if this future is dropped (cancelled) before it commits (R4 H2).
        let mut lock_guard: Option<SubmitLockGuard> = None;
        if let Some(req) = lock_req {
            match self.try_acquire_lock(req.clone()).await? {
                true => {
                    lock_guard = Some(SubmitLockGuard {
                        armed: true,
                        task_id: id.clone(),
                        metadata: Arc::clone(&self.metadata),
                    });
                }
                false => {
                    // Blocked by an equal/higher-priority holder. For an
                    // executor-backed batch task, queue-on-lock (Druid
                    // parity): the accepted task is WAITING, owned by a
                    // LIVE tracked waiter that re-attempts acquisition
                    // until the lock frees (then runs the normal
                    // execute→publish→terminal tail) or the lock-wait
                    // deadline finalizes it FAILED. Pre-fix this arm
                    // persisted the WAITING row and returned with NO
                    // owner anywhere — no tail, no finalizer, no retry —
                    // so releasing the conflicting lock resumed nothing
                    // and the accepted task stayed WAITING forever
                    // (bootstrap ignores WAITING rows: a permanently
                    // stranded accepted task).
                    if self.historical.is_some()
                        && (task_type == "index" || task_type == "index_parallel")
                    {
                        return self.spawn_lock_waiter(record, spec, req).await;
                    }
                    // Stub path (no executor, or a non-batch type that
                    // nothing would ever run): persist WAITING and
                    // return the id, as before.
                    self.persist_and_store(record).await?;
                    return Ok(id);
                }
            }
        }

        // Locks (if any) granted: WAITING -> PENDING.
        record.state = TaskState::Pending;

        // If this Overlord owns a Historical and the task is a native-batch
        // type (Druid's serial `index` or parallel `index_parallel` — both
        // carry the same dataSchema/ioConfig shape), start the full
        // ingestion ASYNCHRONOUSLY and return the id (Druid parity: the
        // client polls the task status for the verdict).  Otherwise leave
        // it PENDING (the original Phase-1 stub behavior).  Pre-compat-4
        // only `index_parallel` matched, so a serial `index` task was
        // accepted and then parked PENDING forever.
        if self.historical.is_some() && (task_type == "index" || task_type == "index_parallel") {
            record.state = TaskState::Running;
            // ASYNC SUBMIT CONTRACT (the R10-class root fix). The old
            // contract awaited the detached tail's verdict over a oneshot
            // and handed the FINAL status to the HTTP caller. That
            // synchronous coupling was the root of a bug class: any path
            // that delivered the WAITING SUBMITTER a failure while the
            // shielded publish still committed (an abort tearing down the
            // result channel, an explicit shutdown mid-publish) invited a
            // resubmission of already-committed input — the permanent
            // append double count. Now the submitter only ever receives
            // the task id; the ONLY verdict source is the polled task
            // status, written by the fence-derived finalizers
            // (`finalize_batch_terminal` / the tail's own persisted
            // exit), so no submitter can ever observe a failure for a
            // publish that commits — and a dropped HTTP response holds
            // nothing that could tear or mis-report anything.
            //
            // Persist the RUNNING row FIRST, awaited by THIS request
            // future (insert_task is an upsert, so the tail's terminal
            // persist updates it): the id handed back is durably
            // pollable, a successfully executed task can never
            // "disappear", and published data with no task row would 404
            // on /status and invite a duplicate resubmit (Codex R5 H2).
            // Residual at-least-once: a crash strictly between
            // publication and the terminal update leaves the row RUNNING
            // until the NEXT bootstrap's
            // `reconcile_stale_running_batch_tasks` (F2) recovers its
            // verdict from the committed segment rows; a default
            // replace-mode retry is idempotent for the interval,
            // appendToExisting retries are not
            // (documented). A LIVE process whose terminal update fails
            // converges to the same shape only after the bounded
            // background persist retry (`spawn_terminal_persist_retry`)
            // exhausts its budget — and keeps reporting the truthful
            // terminal status from memory either way.
            //
            // Cancellation of THIS request future (client disconnect
            // before the id is delivered) can only land parked at the
            // insert-join await below or at the `running_tasks`
            // write-lock acquisition after it — everything from there to
            // `Ok(id)` is synchronous. Both windows are strictly BEFORE
            // the tail exists, and both are guarded: `SubmitRowGuard`
            // flips the RUNNING row to FAILED — ORDERED after the
            // uncancellable insert has truly resolved (D4), so a torn
            // insert landing late can never outlive the FAILED cleanup —
            // and `SubmitLockGuard` releases the interval locks; nothing
            // executed, so FAILED is the truth and a resubmission is
            // safe. A client cancelling AFTER the registration block
            // holds nothing: the tail runs to completion on its own.
            // Serialize the row BEFORE arming the guard: a (theoretical)
            // serialization failure then returns plainly instead of
            // firing the guard for a row that never existed.
            let running_row = record.to_row()?;
            let mut row_guard = SubmitRowGuard::armed(&record, Arc::clone(&self.metadata))?;
            // D4: run the RUNNING-row insert in its OWN spawned task so
            // the STORE-side write is uncancellable — a submit
            // cancellation can only drop the JOIN await, never tear the
            // insert itself into a "may land later" torn write. The
            // guard holds the join handle, so the cancellation cleanup
            // can order its FAILED flip strictly AFTER the insert has
            // truly resolved (see `SubmitRowGuard`). The guard is armed
            // BEFORE the spawn (synchronous, no cancellation point
            // between them): were the spawn first, a guard-arming
            // failure would leak an unguarded RUNNING insert.
            let insert_task = {
                let metadata = Arc::clone(&self.metadata);
                #[cfg(test)]
                let pause = Arc::clone(&self.test_running_insert_pause);
                #[cfg(test)]
                let entered = Arc::clone(&self.test_running_insert_entered);
                tokio::spawn(async move {
                    #[cfg(test)]
                    {
                        // Test-only pause point (D4): a torn/slow insert
                        // whose store-side landing is delayed.
                        entered.store(true, std::sync::atomic::Ordering::SeqCst);
                        let _pause = pause.lock().await;
                    }
                    metadata.insert_task(&running_row).await
                })
            };
            row_guard.attach_running_insert(insert_task);
            // On an insert error: the store refused the row (guard
            // disarmed inside `await_running_insert` — nothing durable
            // to flip) or the insert task died (guard stays armed; its
            // idempotent FAILED cleanup is truthful whether or not the
            // row landed). The armed SubmitLockGuard drop releases the
            // interval locks on the error return.
            row_guard.await_running_insert().await?;
            // Run the execute → P→M→swap publish → terminal-persist tail
            // in a DETACHED spawned task (ship2 H9, the batch mirror of
            // [`run_lifecycle_op`]'s spawn): nothing about the tail's
            // fate ever depends on this request future again.
            //
            // Review High on H9: the detached tail must stay VISIBLE and
            // KILLABLE. The RUNNING record and the tail's abort handle
            // are registered in the block below, which has no await point
            // between them and the spawn, so from the instant the tail
            // exists `get_running_tasks` / `get_tasks_by_datasource` see
            // it and `shutdown_task` can abort it. Only the P→M→swap
            // publication-critical section inside `execute_index_parallel`
            // is shielded from that abort (see the nested spawn there), so
            // an abort can kill a stalled tail yet can never tear a
            // publish mid-swap.
            let this = self.clone_handle();
            {
                // Atomic registration: everything between the write-lock
                // acquisition and the end of this block is synchronous,
                // so the record, the spawned tail, and its abort handle
                // become visible together — `shutdown_task` (which takes
                // the same locks in the same order) can never observe a
                // registered record without its killable tail. Holding
                // the sync registry guard across the spawn also orders
                // the tail's own `BatchTailGuard` removal after the
                // insert here — a tail that finishes instantly still
                // leaves no stale registry entry. Neither guard is held
                // across an await.
                let mut tasks = self.running_tasks.write().await;
                let mut tails = lock_batch_tails(&self.batch_tails);
                tasks.insert(id.clone(), record.clone());
                let tail_guard = BatchTailGuard {
                    task_id: id.clone(),
                    overlord: self.clone_handle(),
                };
                let handle = tokio::spawn(async move {
                    // Deregisters this tail on EVERY exit (completion,
                    // panic, abort) — and, should this body die with its
                    // publish fence still registered (abort/panic
                    // mid-body), spawns the verdict-driven recovery
                    // finalizer (see BatchTailGuard). `_`-prefixed but
                    // BOUND (not `let _ =`): it must live to the end of
                    // this scope, dropping only after the fence has been
                    // retired by the body's exit paths.
                    let _tail_guard = tail_guard;
                    this.run_batch_tail_body(record, spec, lock_guard).await;
                });
                tails.tails.insert(id.clone(), handle.abort_handle());
                // Tail spawned AND registered: the submit-scope row guard
                // retires here (still inside the synchronous block — no
                // cancellation point can land between the spawn and this
                // disarm). From here on the row's fate belongs to the
                // tail and its finalizers.
                row_guard.disarm();
            }
            return Ok(id);
        }

        self.persist_and_store(record).await?;
        // Committed: the task row is now persisted, so disarm the
        // cancellation guard (it must only fire on a dropped future).
        if let Some(mut guard) = lock_guard {
            guard.disarm();
        }
        Ok(id)
    }

    /// The detached batch tail BODY: execute → P→M→swap publish →
    /// terminal-persist, then retire the publish fence (or finalize via
    /// [`finalize_batch_terminal`](Self::finalize_batch_terminal) when
    /// the terminal persist fails) and disarm the lock guard once the
    /// record is terminal. Shared verbatim by the normal (lock-granted)
    /// submit spawn and by the lock-queued WAITING waiter once it
    /// acquires its interval lock, so both paths get the identical
    /// fence/finalizer treatment. MUST run inside a spawned task that
    /// holds a [`BatchTailGuard`] (registered in `batch_tails`) for its
    /// whole lifetime — every invariant documented on the guard assumes
    /// it.
    ///
    /// Publish-fence retirement (concurrent-shutdown fix + the
    /// fence-leak fix): the fence outlives this tail's abort-handle
    /// entry and is retired on this body's every exit —
    ///  * `Ok`: the terminal status is durably persisted AND mirrored
    ///    in-memory (`persist_and_store` updated the table under the
    ///    same `running_tasks` lock `shutdown_task` checks), so a
    ///    shutdown finding no fence always finds a truthful terminal
    ///    task; plain removal suffices.
    ///  * `Err` (the terminal persist failed fast OR timed out — the
    ///    initial attempt is time-bounded by
    ///    [`TERMINAL_PERSIST_ATTEMPT_TIMEOUT`] like every other
    ///    terminal-persist site, F4, so a hung metadata op cannot park
    ///    this tail forever): the in-memory table still shows a
    ///    non-terminal state, so the fence may NOT simply be dropped
    ///    (losing the
    ///    committed verdict) nor retained (the historical HIGH leak: a
    ///    committed-but-unpersisted fence parked forever waiting for an
    ///    explicit shutdown, one leaked entry per transient store
    ///    failure). `finalize_batch_terminal` resolves it in bounded
    ///    time: truthful in-memory terminal state (SUCCESS for a
    ///    committed publish — never a resubmit-inviting FAILED), fence
    ///    retired atomically with it, and the durable row flushed by a
    ///    bounded background retry.
    ///
    /// On an ABORT none of this runs; the shutdown finalizer and the
    /// tail guard's recovery retire the fence instead, idempotently.
    ///
    /// No report to any submitter: under the async submit contract the
    /// tail's verdict lives ONLY in the task status (in-memory + durable
    /// row), where pollers read the fence-derived truth. The old oneshot
    /// back to the awaiting submit — and every path that could hand that
    /// submitter a failure for a committed publish — is gone by
    /// construction.
    async fn run_batch_tail_body(
        &self,
        mut record: TaskRecord,
        spec: serde_json::Value,
        mut lock_guard: Option<SubmitLockGuard>,
    ) {
        self.run_with_retry(&mut record, &spec).await;
        // Per-attempt timeout (F4) on the tail's INITIAL terminal persist
        // too: this write sits AFTER the publish committed, and a HUNG
        // (not failing-fast) metadata op here used to park this tail
        // forever — control never reached `finalize_batch_terminal`, so
        // the in-memory task stayed RUNNING and the live tail + resolved
        // fence stayed registered indefinitely, with NO deadline anywhere
        // (unlike the finalizer's attempt and every bounded-retry
        // attempt). A timed-out attempt is routed to the SAME truthful
        // recovery as a fast failure — the `Err` arm below finalizes via
        // `finalize_batch_terminal` (fence retired, locks released,
        // bounded background persist retry), so a committed publish still
        // converges to SUCCESS. The cancelled attempt may or may not have
        // landed store-side; the finalizer's persist is an idempotent
        // upsert of the same terminal row either way.
        let result = match tokio::time::timeout(
            TERMINAL_PERSIST_ATTEMPT_TIMEOUT,
            self.persist_and_store(record.clone()),
        )
        .await
        {
            Ok(persisted) => persisted,
            Err(_) => Err(DruidError::Metadata(format!(
                "initial terminal-status persist for {} timed out after \
                 {TERMINAL_PERSIST_ATTEMPT_TIMEOUT:?} (hung metadata op \
                 cancelled); finalizing via the bounded-retry path",
                record.id
            ))),
        };
        match &result {
            Ok(()) => {
                lock_batch_tails(&self.batch_tails)
                    .publish_fences
                    .remove(&record.id);
            }
            Err(_) => {
                // The executed truth: `record.state` as run_with_retry
                // left it (SUCCESS for a committed publish, FAILED
                // otherwise) — defensively FAILED should it ever be
                // non-terminal here.
                if !record.state.is_terminal() {
                    record.state = TaskState::Failed;
                }
                record.worker = None;
                record.location = None;
                let committed = record.state == TaskState::Success;
                // Self-check: the record-derived truth and the fence
                // verdict (when one is still registered and resolved)
                // always agree — `Resolved { committed: true }` implies
                // run_with_retry returned SUCCESS and vice versa.
                #[cfg(debug_assertions)]
                {
                    let verdict = lock_batch_tails(&self.batch_tails)
                        .publish_fences
                        .get(&record.id)
                        .map(|entry| *entry.verdict.borrow());
                    if let Some(FenceState::Resolved {
                        committed: resolved,
                    }) = verdict
                    {
                        debug_assert_eq!(
                            resolved, committed,
                            "fence verdict and executed record disagree"
                        );
                    }
                }
                if let Err(e) = self
                    .finalize_batch_terminal(&record.id, committed, Some(record.clone()))
                    .await
                {
                    tracing::warn!(
                        task_id = %record.id,
                        error = %e,
                        "batch tail could not persist its terminal \
                         status; finalized in-memory (fence retired) \
                         with a bounded background persist retry"
                    );
                }
            }
        }
        // Terminal handling complete: run_with_retry's terminal paths
        // (and finalize_batch_terminal on the Err path) already released
        // any locks, so disarm the cancellation guard whenever the
        // record reached a terminal state. On a never-ran failure the
        // guard (if armed) drops right here — and on an abort or panic
        // it drops with the future — releasing the task's durable locks,
        // the same terminal behavior as the pre-spawn inline path
        // (repeat-safe against the finalizer's own release).
        if (result.is_ok() || record.state.is_terminal())
            && let Some(guard) = lock_guard.as_mut()
        {
            guard.disarm();
        }
        drop(lock_guard);
    }

    /// Accept a lock-conflicted executor-batch submission by handing the
    /// WAITING task to a LIVE tracked owner (the queue-on-lock fix): the
    /// WAITING row is durably persisted (same D4 uncancellable-insert +
    /// [`SubmitRowGuard`] shape as the RUNNING path — a submit
    /// cancellation can never strand a durable ownerless row), then the
    /// record and the spawned waiter's abort handle are registered
    /// ATOMICALLY (the same synchronous `running_tasks` → `batch_tails`
    /// block as the normal tail spawn), so from the instant the waiter
    /// exists it is visible (`get_task` / `get_waiting_tasks`), killable
    /// (`shutdown_task` aborts it), and covered by the
    /// [`BatchTailGuard`] whole-lifetime finalizer (an abort/panic
    /// finalizes the task FAILED instead of stranding it). Returns the
    /// accepted task id.
    async fn spawn_lock_waiter(
        &self,
        record: TaskRecord,
        spec: serde_json::Value,
        req: TaskLock,
    ) -> Result<String> {
        let id = record.id.clone();
        // Serialize the row BEFORE arming the guard (same ordering
        // rationale as the RUNNING path).
        let waiting_row = record.to_row()?;
        let mut row_guard = SubmitRowGuard::armed(&record, Arc::clone(&self.metadata))?;
        let insert_task = {
            let metadata = Arc::clone(&self.metadata);
            tokio::spawn(async move { metadata.insert_task(&waiting_row).await })
        };
        row_guard.attach_running_insert(insert_task);
        row_guard.await_running_insert().await?;
        let this = self.clone_handle();
        {
            // Atomic registration, mirroring the normal tail spawn: no
            // await point between the write-lock acquisition and the
            // guard disarm, so the record, the spawned waiter, and its
            // abort handle become visible together and a caller
            // cancellation can never strike between "waiter spawned"
            // and "waiter registered".
            let mut tasks = self.running_tasks.write().await;
            let mut tails = lock_batch_tails(&self.batch_tails);
            tasks.insert(id.clone(), record.clone());
            let tail_guard = BatchTailGuard {
                task_id: id.clone(),
                overlord: self.clone_handle(),
            };
            let handle = tokio::spawn(async move {
                // Same whole-lifetime coverage as the normal tail: the
                // guard deregisters the waiter on EVERY exit and
                // finalizes a died-non-terminal task FAILED.
                let _tail_guard = tail_guard;
                this.run_lock_waiter(record, spec, req).await;
            });
            tails.tails.insert(id.clone(), handle.abort_handle());
            row_guard.disarm();
        }
        Ok(id)
    }

    /// Body of the WAITING waiter (queue-on-lock, Druid parity: a
    /// lock-blocked task queues until the lock frees): re-attempt the
    /// interval-lock acquisition with bounded backoff until it is
    /// granted — the task then goes WAITING → RUNNING (in-memory +
    /// durable row) and runs the shared
    /// [`run_batch_tail_body`](Self::run_batch_tail_body) — or until the
    /// lock-wait deadline ([`lock_wait_deadline`](Self::with_lock_wait_deadline),
    /// default [`LOCK_WAIT_DEADLINE_DEFAULT`]) expires, which finalizes
    /// the task FAILED (truthful: it never ran, nothing committed, a
    /// resubmission is safe) via
    /// [`finalize_batch_terminal`](Self::finalize_batch_terminal).
    ///
    /// Composition with the existing invariants:
    ///  * no double acquisition — every attempt runs the full serialized
    ///    [`try_acquire_lock`](Self::try_acquire_lock) evaluation, and a
    ///    task never conflicts with its own persisted locks, so an
    ///    ambiguous store error between attempts cannot self-block (any
    ///    duplicate own-row is released with the task's locks);
    ///  * `shutdown_task` aborts a still-WAITING waiter through the same
    ///    registry as any tail: phase 1 writes FAILED (no fence exists
    ///    yet) and aborts; nothing was acquired, so its
    ///    `release_task_locks` is a no-op scan — and if the abort lands
    ///    inside an acquisition that just persisted the lock row, that
    ///    same scan (task-id-keyed) releases it;
    ///  * once the lock is granted, a [`SubmitLockGuard`] is armed
    ///    synchronously (no await between grant and arming), so an
    ///    abort landing later releases the acquired locks exactly like a
    ///    cancelled submit;
    ///  * the WAITING → RUNNING durable flip happens BEFORE execution,
    ///    so the crash-between-publish-and-terminal-persist residual
    ///    stays covered by the F2 bootstrap reconcile (which correlates
    ///    RUNNING rows against committed segments);
    ///  * a store failure during a re-acquisition attempt is retried
    ///    until the deadline (warn-logged), never silently dropped.
    ///
    /// Residual (documented): a process crash while the task queues
    /// WAITING leaves a durable WAITING row whose waiter died with the
    /// process; the next bootstrap's
    /// [`reconcile_stale_running_batch_tasks`](Self::reconcile_stale_running_batch_tasks)
    /// finalizes such ownerless batch WAITING rows FAILED (they never
    /// ran, so FAILED is truthful and a resubmission is safe).
    async fn run_lock_waiter(
        &self,
        mut record: TaskRecord,
        spec: serde_json::Value,
        req: TaskLock,
    ) {
        let deadline = tokio::time::Instant::now() + self.lock_wait_deadline;
        let mut backoff = LOCK_WAIT_RETRY_BASE_BACKOFF;
        let acquired = loop {
            match self.try_acquire_lock(req.clone()).await {
                Ok(true) => break true,
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        task_id = %record.id,
                        error = %e,
                        "lock-wait re-acquisition attempt failed; retrying \
                         until the lock-wait deadline"
                    );
                }
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break false;
            }
            tokio::time::sleep(backoff.min(deadline - now)).await;
            backoff = (backoff * 2).min(LOCK_WAIT_RETRY_MAX_BACKOFF);
        };
        if !acquired {
            // Deadline expired with the lock still held elsewhere:
            // truthful terminal rejection. Nothing ran and nothing was
            // acquired, so FAILED + the (no-op) lock release + the
            // durable persist (bounded background retry on failure) is
            // the whole cleanup.
            record.state = TaskState::Failed;
            record.worker = None;
            record.location = None;
            tracing::warn!(
                task_id = %record.id,
                deadline_ms = u64::try_from(self.lock_wait_deadline.as_millis())
                    .unwrap_or(u64::MAX),
                "batch task could not acquire its interval lock within the \
                 lock-wait deadline; finalized FAILED (it never ran — a \
                 resubmission is safe)"
            );
            if let Err(e) = self
                .finalize_batch_terminal(&record.id, false, Some(record.clone()))
                .await
            {
                tracing::warn!(
                    task_id = %record.id,
                    error = %e,
                    "lock-wait deadline finalizer could not persist the \
                     terminal status; finalized in-memory with a bounded \
                     background persist retry"
                );
            }
            return;
        }
        // Lock granted: arm the cancellation guard SYNCHRONOUSLY (no
        // await point between the grant returning and this arming), so
        // an abort from here on releases the acquired locks exactly like
        // a cancelled submit (R4 H2).
        let mut lock_guard = SubmitLockGuard {
            armed: true,
            task_id: record.id.clone(),
            metadata: Arc::clone(&self.metadata),
        };
        // WAITING → RUNNING, in-memory AND durable, BEFORE execution:
        // the async submit contract persists the RUNNING row ahead of
        // the tail precisely so a crash between publication and the
        // terminal persist is recovered by the F2 bootstrap reconcile —
        // executing under a durable WAITING row would leave that crash
        // residual unrecoverable (bootstrap ignores WAITING for commit
        // correlation). If the flip cannot be made durable, refuse to
        // execute: FAILED is truthful (nothing ran) and releases the
        // just-acquired locks.
        record.state = TaskState::Running;
        // Per-attempt timeout (F4) on this pre-execution flip too: a HUNG
        // store op here used to park the just-dequeued waiter forever
        // (in-memory WAITING, live registered tail, just-acquired locks
        // held — the lock-wait deadline only bounds ACQUISITION, so no
        // deadline covered this write). A timed-out flip is handled
        // exactly like a fast failure below: refuse to execute, finalize
        // FAILED (truthful — nothing ran), release the just-acquired
        // locks. The cancelled attempt may or may not have landed a
        // RUNNING row store-side; the refusal path's terminal persist is
        // an upsert that overwrites it with FAILED either way.
        let flip = match tokio::time::timeout(
            TERMINAL_PERSIST_ATTEMPT_TIMEOUT,
            self.persist_and_store(record.clone()),
        )
        .await
        {
            Ok(persisted) => persisted,
            Err(_) => Err(DruidError::Metadata(format!(
                "WAITING->RUNNING persist for {} timed out after \
                 {TERMINAL_PERSIST_ATTEMPT_TIMEOUT:?} (hung metadata op \
                 cancelled); refusing to execute",
                record.id
            ))),
        };
        if let Err(e) = flip {
            tracing::warn!(
                task_id = %record.id,
                error = %e,
                "queued batch task acquired its interval lock but could not \
                 persist the RUNNING row; refusing to execute (finalized \
                 FAILED, locks released — a resubmission is safe)"
            );
            record.state = TaskState::Failed;
            record.worker = None;
            record.location = None;
            if let Err(e2) = self
                .finalize_batch_terminal(&record.id, false, Some(record.clone()))
                .await
            {
                tracing::warn!(
                    task_id = %record.id,
                    error = %e2,
                    "queued-task RUNNING-persist-failure finalizer could not \
                     persist the terminal status; finalized in-memory with a \
                     bounded background persist retry"
                );
            }
            // finalize_batch_terminal released the task's locks; the
            // guard must not fire a second (redundant) release.
            lock_guard.disarm();
            return;
        }
        self.run_batch_tail_body(record, spec, Some(lock_guard))
            .await;
    }

    /// Run an `index_parallel` task through the RUNNING -> terminal portion of
    /// the lifecycle, retrying on failure per the configured [`RetryPolicy`].
    ///
    /// Records that reach `SUCCESS` release their locks; records that exhaust
    /// the retry budget end `FAILED` (also releasing locks). A record that the
    /// spec does not match (`Ok(false)`) is left `PENDING` — the stub path.
    async fn run_with_retry(&self, record: &mut TaskRecord, spec: &serde_json::Value) {
        let Some(historical) = self.historical.as_ref() else {
            return;
        };
        loop {
            // Assign a worker if any are registered; bookkeeping only.
            record.worker = self.assign_worker(&record.id).await;
            record.location = record.worker.as_ref().and_then(|w| worker_location(w));
            record.state = TaskState::Running;
            record.attempt = record.attempt.saturating_add(1);

            // OUT-param: set true only for the durable-retained append
            // residual (Codex R23 H1 append variant), where retrying would
            // append a SECOND durable row and double count after the next
            // restart's bootstrap reload. Reset before every attempt.
            let mut retry_suppressed = false;
            match self
                .execute_index_parallel(
                    &record.id,
                    &record.data_source,
                    spec,
                    historical,
                    &mut retry_suppressed,
                )
                .await
            {
                Ok(true) => {
                    record.state = TaskState::Success;
                    self.release_task_locks(&record.id).await;
                    return;
                }
                Ok(false) => {
                    // A recognised native-batch task (this loop only runs for
                    // `index`/`index_parallel`) whose spec is not runnable —
                    // a missing/`Ok(None)` dataSchema/ioConfig/inputSource —
                    // can never become runnable, so it terminates FAILED and
                    // RELEASES its locks. Reverting to PENDING with the locks
                    // still held stranded the task and blocked its datasource
                    // forever (Codex compat-4 R1 H1). Nothing here polls a
                    // PENDING task back to life, so PENDING == a permanent
                    // accept-then-hang with a leaked lock.
                    record.state = TaskState::Failed;
                    record.worker = None;
                    record.location = None;
                    self.release_task_locks(&record.id).await;
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        task_id = %record.id,
                        attempt = record.attempt,
                        error = %e,
                        retry_suppressed,
                        "index_parallel ingestion attempt failed"
                    );
                    // A durable-retained append residual must NOT be retried
                    // (it is reloaded on restart; re-appending duplicates it).
                    if !retry_suppressed && self.retry_policy.can_retry(record.attempt) {
                        // Worker (notionally) released between attempts.
                        record.worker = None;
                        record.location = None;
                        record.state = TaskState::Pending;
                        // Backoff is computed but not slept on in-process
                        // (the detached tail retries promptly); it is
                        // surfaced for schedulers via
                        // `RetryPolicy::backoff_millis`.
                        continue;
                    }
                    record.state = TaskState::Failed;
                    self.release_task_locks(&record.id).await;
                    return;
                }
            }
        }
    }

    /// Persist a task record and store it in the in-memory table.
    async fn persist_and_store(&self, record: TaskRecord) -> Result<()> {
        let row = record.to_row()?;
        self.persist_task_row_checked(&row).await?;
        let mut tasks = self.running_tasks.write().await;
        tasks.insert(record.id.clone(), record);
        Ok(())
    }

    /// Upsert a task row into the metadata store — the single choke point
    /// for every terminal-status persist of the batch state machine
    /// (`persist_and_store`, [`persist_terminal_row`], and through them
    /// [`finalize_batch_terminal`] and the bounded background retry).
    ///
    /// Split out so tests can inject a transient store failure here — and
    /// ONLY here; the initial RUNNING-row insert and all segment-metadata
    /// writes keep using the real store — to exercise the
    /// committed-publish-with-failed-terminal-persist path that used to
    /// leak the publish fence.
    ///
    /// [`persist_terminal_row`]: Overlord::persist_terminal_row
    /// [`finalize_batch_terminal`]: Overlord::finalize_batch_terminal
    async fn persist_task_row_checked(&self, row: &TaskRow) -> Result<()> {
        #[cfg(test)]
        if self.take_injected_terminal_persist_failure() {
            return Err(DruidError::Metadata(
                "injected terminal-persist failure (test fault hook)".to_string(),
            ));
        }
        #[cfg(test)]
        if self.take_injected_terminal_persist_hang() {
            // A HUNG (not failing-fast) metadata op: park on a
            // never-resolving future. The caller's per-attempt timeout
            // (F4) must drop this future so the attempt cap advances.
            std::future::pending::<()>().await;
        }
        self.metadata.insert_task(row).await
    }

    /// Consume one injected terminal-persist failure, if any are armed.
    #[cfg(test)]
    fn take_injected_terminal_persist_failure(&self) -> bool {
        self.inject_terminal_persist_failures
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| n.checked_sub(1),
            )
            .is_ok()
    }

    /// Consume one injected terminal-persist HANG, if any are armed.
    #[cfg(test)]
    fn take_injected_terminal_persist_hang(&self) -> bool {
        self.inject_terminal_persist_hangs
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| n.checked_sub(1),
            )
            .is_ok()
    }

    /// Persist the current IN-MEMORY state of a task as an UPSERT
    /// ([`MetadataStore::insert_task`], via the checked choke point).
    ///
    /// The terminal-persist twin of [`persist_existing`]: upsert (not
    /// UPDATE) on purpose, so a truthful terminal row lands even when the
    /// task's initial RUNNING-row insert itself failed (an UPDATE would
    /// silently write nothing and report success). No-op for a task id
    /// with no in-memory record.
    ///
    /// [`persist_existing`]: Overlord::persist_existing
    async fn persist_terminal_row(&self, task_id: &str) -> Result<()> {
        let row = {
            let tasks = self.running_tasks.read().await;
            match tasks.get(task_id) {
                Some(r) => r.to_row()?,
                None => return Ok(()),
            }
        };
        self.persist_task_row_checked(&row).await
    }

    /// Truthfully finalize a batch task from its publish verdict: write
    /// the terminal in-memory state (infallible), retire the publish
    /// fence ATOMICALLY with that write, release the task's interval
    /// locks, and persist the terminal row — spawning a BOUNDED
    /// background retry ([`spawn_terminal_persist_retry`]) when the
    /// persist fails, so a transient store outage can never park a fence
    /// (or a lying non-terminal status) until some later explicit
    /// shutdown.
    ///
    /// Shared by every finalizer of the publish-fence state machine — the
    /// tail's own failed-terminal-persist exit, `shutdown_task`'s
    /// verdict finalizer, and the [`BatchTailGuard`] died-with-fence
    /// recovery — so all of them are idempotent against each other by
    /// construction: same verdict in, same state written, repeat-safe
    /// lock release, deduped persist retry.
    ///
    /// `committed == true` (the publish verdict resolved committed) forces
    /// SUCCESS even over an already-terminal FAILED: the fence verdict is
    /// ground truth, and a FAILED status on a committed append invites
    /// the double-appending resubmission (R4/R5). Otherwise an existing
    /// terminal state is left untouched and a non-terminal one becomes
    /// FAILED.
    ///
    /// `executed`, when supplied (the tail's own exit knows it), is the
    /// tail's executed record — attempt count and worker included — and
    /// replaces a still-non-terminal in-memory record before the state
    /// rules above apply, so the persisted row carries full fidelity. An
    /// already-terminal in-memory record is never overwritten by it (a
    /// concurrent finalizer's truth stands; the two always agree on
    /// SUCCESS-ness, see the `debug_assert` at the tail call site).
    ///
    /// Error contract: `Err` means ONLY "the durable persist did not land
    /// yet" — the in-memory state, fence retirement, and lock release
    /// have all already happened, and the bounded retry is running.
    ///
    /// [`spawn_terminal_persist_retry`]: Overlord::spawn_terminal_persist_retry
    async fn finalize_batch_terminal(
        &self,
        task_id: &str,
        committed: bool,
        executed: Option<TaskRecord>,
    ) -> Result<()> {
        {
            let mut tasks = self.running_tasks.write().await;
            if let Some(rec) = executed
                && let Some(existing) = tasks.get_mut(task_id)
                && !existing.state.is_terminal()
            {
                *existing = rec;
            }
            if let Some(task) = tasks.get_mut(task_id) {
                if committed {
                    // The publish genuinely landed: SUCCESS, even over a
                    // FAILED a concurrent shutdown/transition may have
                    // written — the fence verdict is ground truth, and a
                    // FAILED status would invite the double-appending
                    // resubmission.
                    task.state = TaskState::Success;
                    task.worker = None;
                    task.location = None;
                } else if !task.state.is_terminal() {
                    task.state = TaskState::Failed;
                    task.worker = None;
                    task.location = None;
                }
                // Self-check: finalization must leave a terminal state,
                // and a committed publish must never read anything but
                // SUCCESS from here on.
                debug_assert!(task.state.is_terminal());
                debug_assert!(!committed || task.state == TaskState::Success);
            }
            // Retire the fence in the SAME `running_tasks` critical
            // section as the truth write (lock order: `running_tasks` →
            // registry, no await under either): any shutdown that
            // subsequently finds no fence reads an already-truthful
            // terminal state under the same lock.
            lock_batch_tails(&self.batch_tails)
                .publish_fences
                .remove(task_id);
        }
        self.release_task_locks(task_id).await;
        // Per-attempt timeout (F4): a HUNG metadata op must not park this
        // finalizer (and whatever awaits it) forever — a timed-out
        // attempt is handed to the bounded background retry exactly like
        // a fast failure.
        match tokio::time::timeout(
            TERMINAL_PERSIST_ATTEMPT_TIMEOUT,
            self.persist_terminal_row(task_id),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                self.spawn_terminal_persist_retry(task_id);
                Err(e)
            }
            Err(_) => {
                self.spawn_terminal_persist_retry(task_id);
                Err(DruidError::Metadata(format!(
                    "terminal-status persist for {task_id} timed out after \
                     {TERMINAL_PERSIST_ATTEMPT_TIMEOUT:?} (hung metadata op \
                     cancelled); bounded background retry spawned"
                )))
            }
        }
    }

    /// Spawn the BOUNDED background retry of a failed terminal-status
    /// persist: up to [`TERMINAL_PERSIST_RETRY_ATTEMPTS`] attempts of
    /// [`persist_terminal_row`](Overlord::persist_terminal_row) with
    /// doubling backoff, deduped per task id via
    /// [`BatchTails::persist_retries`] (concurrent finalizers spawn at
    /// most one loop). The in-memory state is already truthful when this
    /// is called, so the loop is pure durability flushing — during a
    /// transient store outage the held markers are bounded by the tasks
    /// whose terminal persist failed, and they are flushed on recovery.
    ///
    /// Every attempt is additionally TIME-bounded
    /// ([`TERMINAL_PERSIST_ATTEMPT_TIMEOUT`], F4): a hung metadata op is
    /// cancelled and counted like a fast failure, so the cap always
    /// exhausts and the marker always leaves the registry.
    ///
    /// Attempt-cap exhaustion gives up LOUDLY (error log): the durable
    /// row then stays RUNNING — exactly the documented
    /// crash-between-publication-and-terminal-persist residual of
    /// `submit_task` — while the in-memory status keeps reporting the
    /// truth for this process's lifetime; after a restart the bootstrap
    /// reload re-serves the committed (published + durable) segment and
    /// [`reconcile_stale_running_batch_tasks`](Overlord::reconcile_stale_running_batch_tasks)
    /// recovers the row's terminal verdict (F2).
    fn spawn_terminal_persist_retry(&self, task_id: &str) {
        {
            let mut registry = lock_batch_tails(&self.batch_tails);
            if !registry.persist_retries.insert(task_id.to_string()) {
                // A bounded retry loop is already running for this task.
                return;
            }
        }
        let this = self.clone_handle();
        let task_id = task_id.to_string();
        tokio::spawn(async move {
            let mut backoff = TERMINAL_PERSIST_RETRY_BASE_BACKOFF;
            let mut landed = false;
            for attempt in 1..=TERMINAL_PERSIST_RETRY_ATTEMPTS {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(TERMINAL_PERSIST_RETRY_MAX_BACKOFF);
                // Per-attempt timeout (F4): the loop is attempt-bounded,
                // but each attempt must be TIME-bounded too — one hung
                // metadata op would otherwise block cap exhaustion and
                // marker removal forever. A timed-out attempt advances
                // the counter exactly like a fast failure.
                match tokio::time::timeout(
                    TERMINAL_PERSIST_ATTEMPT_TIMEOUT,
                    this.persist_terminal_row(&task_id),
                )
                .await
                {
                    Ok(Ok(())) => {
                        landed = true;
                        break;
                    }
                    Ok(Err(e)) => tracing::warn!(
                        task_id = %task_id,
                        attempt,
                        error = %e,
                        "terminal-status persist retry failed"
                    ),
                    Err(_) => tracing::warn!(
                        task_id = %task_id,
                        attempt,
                        "terminal-status persist retry attempt timed out after \
                         {TERMINAL_PERSIST_ATTEMPT_TIMEOUT:?} (hung metadata op \
                         cancelled); the attempt cap still advances"
                    ),
                }
            }
            // Marker removal on EVERY loop exit — the retry can never
            // outlive its bounded budget in the registry (R3).
            lock_batch_tails(&this.batch_tails)
                .persist_retries
                .remove(&task_id);
            if !landed {
                tracing::error!(
                    task_id = %task_id,
                    attempts = TERMINAL_PERSIST_RETRY_ATTEMPTS,
                    "terminal-status persist exhausted its bounded retry \
                     budget; the durable task row stays RUNNING (the \
                     documented crash-residual shape) while the in-memory \
                     status carries the truthful terminal state — the \
                     published segment itself is durable and is re-served \
                     by the bootstrap reload after a restart"
                );
            }
        });
    }

    /// Drive the native-batch (`index` / `index_parallel`) ingestion path.
    ///
    /// Returns `Ok(true)` when a segment was produced and registered,
    /// `Ok(false)` when the spec is not a batch shape at all (missing
    /// dataSchema/ioConfig/inputSource — the stub fallback), and `Err(_)`
    /// for hard failures, including unsupported inputSource types.
    ///
    /// `retry_suppressed` is an OUT-parameter set to `true` (from a `false`
    /// default the caller resets before every attempt) ONLY for the
    /// `appendToExisting` swap-failed + rollback-failed durable-retained
    /// residual (Codex R23 H1 append variant): that outcome committed a
    /// durable metadata row + blob that the next restart reloads, so
    /// re-running this attempt would append a SECOND durable row over the
    /// same interval and double count after that reload. [`run_with_retry`]
    /// must NOT retry when this is set (streaming R14/H2 `RetainedDurable`
    /// parity). Every other failure leaves it `false` (retryable per the
    /// [`RetryPolicy`]).
    ///
    /// [`run_with_retry`]: Overlord::run_with_retry
    async fn execute_index_parallel(
        &self,
        task_id: &str,
        data_source: &str,
        spec: &serde_json::Value,
        historical: &Arc<Historical>,
        retry_suppressed: &mut bool,
    ) -> Result<bool> {
        // Default: a failure is retryable unless a branch below proves it
        // must not be. Every early `?` return therefore stays retryable.
        *retry_suppressed = false;
        #[cfg(test)]
        {
            // Test-only fault hook (D3): panic strictly BEFORE the publish
            // fence exists, exercising the tail-died-with-no-fence
            // recovery path of `BatchTailGuard`.
            assert!(
                !self
                    .inject_pre_fence_panic
                    .load(std::sync::atomic::Ordering::SeqCst),
                "injected pre-fence batch-tail panic (test fault hook)"
            );
        }
        let parsed = match parse_index_parallel_spec(spec)? {
            Some(p) => p,
            None => return Ok(false),
        };
        let ParsedIndexSpec {
            data_source: ds_name,
            timestamp_column,
            timestamp_format,
            dimensions,
            metrics_specs,
            rows,
            rollup,
            query_granularity,
            append_to_existing,
            segment_granularity,
        } = parsed;

        if ds_name != data_source {
            tracing::debug!(
                task_id,
                "spec dataSource '{ds_name}' differs from header '{data_source}'; using spec"
            );
        }
        if rows.is_empty() {
            return Err(DruidError::Ingestion(
                "inputSource produced no rows (empty input data)".to_string(),
            ));
        }

        // Thread the DECLARED timestampSpec.format down to row extraction
        // (compat-9 P0): pre-fix this construction defaulted to `auto`, so
        // a declared `iso`/`millis` spec silently stored wrong instants.
        let ingester = BatchIngester::with_schemas(
            ds_name.clone(),
            timestamp_column,
            dimensions,
            metrics_specs,
        )
        .with_timestamp_format(timestamp_format);
        // Rollup enabled (Druid's default) pre-aggregates: rows sharing a
        // (queryGranularity-truncated timestamp, dimension values) key merge
        // into one stored row with summed metrics and `count`-type metrics
        // holding the merged raw-row count.  Pre-fix this branch did not
        // exist and `ingest_with_rollup` was dead code in the server binary.
        let ingested = if rollup {
            ingester.ingest_with_rollup(rows, &query_granularity)?
        } else {
            ingester.ingest(rows)?
        };

        let start_iso = format_epoch_millis_iso(ingested.interval.start_millis);
        // Use a half-open end one millisecond past the max so the
        // segment fully covers the data range.
        let end_iso = format_epoch_millis_iso(ingested.interval.end_millis.saturating_add(1));

        let version = ingested.version.clone();
        let num_rows = ingested.num_rows;

        // Serialize the whole publication sequence (id allocation →
        // replace scan → metadata transaction → segment swap → rollback)
        // per datasource so two concurrent tasks cannot interleave: each
        // publish observes the previous one's full effect. The lock lives
        // on the shared [`MetadataStore`] (round-2 HIGH #2/#3) so the
        // Coordinator's used-flag mutations (`disable_segment` /
        // `disable_datasource` / `enable_datasource`) are mutually
        // exclusive with this critical section too: an admin disable can
        // only land fully before it (then the segment is simply not a
        // victim) or fully after it (then the disable wins) — never in
        // between, where a rollback would resurrect the disable or a
        // just-disabled segment would become query-visible.
        // The guard is OWNED (review High on ship2 H9) so it can move into
        // the shielded publication WRAPPER task spawned below: if
        // `shutdown_task` aborts this (outer) tail while the publication
        // is in flight, the nested task — not this dropped stack — holds
        // the lock until the P→M→swap sequence reaches its normal
        // completion or rollback. The WRAPPER (deadline) task — not the
        // section future it drops on expiry — owns the guard (D2), so a
        // deadline cancellation keeps the lock held until the
        // interrupted verdict has been resolved against durable state
        // (bound-waiting for any uncancellable in-flight store op): a
        // subsequent publisher can never interleave with a still-landing
        // commit. An abort landing BEFORE that spawn (id allocation /
        // replace planning, both read-only) simply drops the guard here:
        // the lock is released and neither metadata nor the
        // query-visible state was touched — a consistent no-op.
        let publish_guard = self
            .metadata
            .datasource_publish_lock(&ds_name)
            .await
            .lock_owned()
            .await;

        // Allocate the segment id UNDER the lock (round-2 HIGH #4): the
        // base id embeds a millisecond-resolution version generated at
        // ingest time, so two tasks covering the same interval in the
        // same millisecond would collide and one task's rows would be
        // silently discarded on insert. `allocate_segment_id` uniquifies
        // with a Druid-style numeric suffix against every existing
        // metadata row (used or unused) and every loaded segment.
        let segment_id = self
            .allocate_segment_id(historical, &ds_name, &start_iso, &end_iso, &version)
            .await?;

        // --- Publication (Codex 2026-07-12 HIGH #1/#2/#3 + round-2 #1-#4) --
        //
        // Druid-faithful replace (P1-#1): with `appendToExisting: false`
        // (Druid's batch default) the task overwrites the
        // segmentGranularity buckets it writes into. The sequence is
        // ordered so that no failure, crash, or concurrent query can
        // observe data loss, a partial replace, or an unregistered orphan
        // segment:
        //
        //   1. Plan the replace read-only, FAILING CLOSED if any
        //      overlapping used segment extends beyond the replace scope
        //      (dropping it wholesale would delete out-of-scope rows).
        //   2. Write metadata as ONE atomic SQLite transaction
        //      ([`MetadataStore::replace_segments_txn`]: victims → unused
        //      AND the new row inserted). A failure — or a process crash
        //      mid-transaction — leaves metadata exactly as it was
        //      (round-2 HIGH #1); the query path has not been touched.
        //   3. Apply the query-visible change LAST as ONE atomic swap
        //      ([`Historical::replace_segments`]), so a concurrent query
        //      sees either the full old segment set or the full new one.
        //      If the swap fails, one compensating transaction
        //      ([`MetadataStore::rollback_replace_txn`]) restores the
        //      pre-publish metadata under the still-held publish lock.
        //
        // Durability note (round-2 HIGH #1 residual, documented): a crash
        // BETWEEN steps 2 and 3 leaves the committed post-replace metadata
        // with the swap unapplied. That is still a consistent recovery
        // point, not a partial one: segment data is in-memory only in the
        // current product, so a restart reloads nothing either way and
        // metadata remains the single durable record of the completed
        // replace — no victim is half-retired and no interval is
        // attributed to two segment sets.

        // Phase 1 — plan (read-only, fail-closed on partial overlap).
        let victims = if append_to_existing {
            Vec::new()
        } else {
            self.plan_replace_victims(
                &ds_name,
                segment_granularity.as_ref(),
                (ingested.interval.start_millis, ingested.interval.end_millis),
                &segment_id,
            )
            .await?
        };

        // ----- Shielded publication-critical section (review High on ship2
        // H9). Phase P → Phase M → swap (+ rollback / reconcile / orphan
        // cleanup on failure) runs in its OWN spawned task
        // ([`run_publish_critical_section`](Self::run_publish_critical_section)),
        // holding the publish-lock guard, and is awaited here. If
        // `shutdown_task` aborts the outer batch tail at this await, the
        // nested `JoinHandle` is dropped, which DETACHES the publication
        // task rather than cancelling it: the publication always reaches
        // its normal completion or rollback (the H9 no-torn-publish
        // guarantee) and only then releases the publish lock — nothing is
        // ever torn between the committed metadata transaction and the
        // query-visible swap.
        // The fence channel is created FIRST so its sender can move into
        // the section, which keeps the fence's value honest as the
        // publish progresses (see [`FenceState`]) — a death mid-section
        // (panic, or the publish-deadline cancellation below) then leaves
        // the last provisional state readable by every watcher.
        let (fence_tx, fence_rx) = tokio::sync::watch::channel(FenceState::InFlight);
        // Probe retained by THIS stack (and cloned into the wrapper) to
        // classify a dropped-sender death from the fence's last state.
        let fence_probe = fence_rx.clone();
        // D2: gate over the section's UNCANCELLABLE tracked store ops
        // (Phase-M commit, compensating rollback). Held by each op task
        // for the op's TRUE duration — including past a deadline drop of
        // the section — and bound-waited by every interrupted-verdict
        // resolution before it reads durable state.
        let store_ops = Arc::new(tokio::sync::Mutex::new(()));
        let section = self.clone_handle().run_publish_critical_section(
            Arc::clone(historical),
            fence_tx,
            Arc::clone(&store_ops),
            PublishArgs {
                task_id: task_id.to_string(),
                ds_name,
                segment_id,
                victims,
                append_to_existing,
                segment_data: ingested.segment_data,
                num_rows,
                start_iso,
                end_iso,
                version,
            },
        );
        // Register the PUBLISH FENCE before the shielded task can exist,
        // under the same registry mutex `shutdown_task` inspects (append
        // double-count fix): in-flight/provisional while publishing,
        // `Resolved { committed }` sent by the section the moment it
        // resolves. The two critical sections linearize, so exactly one
        // of the following holds — (a) shutdown already reaped this
        // tail's abort-handle entry (its abort is about to land, and it
        // saw NO fence and marked the task FAILED): REFUSE to start the
        // publication, so "marked FAILED, locks released" can never
        // coexist with a publish that still commits; or (b) the fence is
        // registered first: shutdown will see it and AWAIT the verdict
        // before finalizing status/locks.
        let deadline = self.publish_deadline;
        let publish_tail = {
            let mut registry = lock_batch_tails(&self.batch_tails);
            if !registry.tails.contains_key(task_id) {
                // Case (a): the un-spawned `section` and the still-local
                // publish guard are both dropped by this return with
                // nothing touched — a consistent no-op. Suppress the
                // (doomed) retry: the abort lands at this tail's next
                // await either way, and shutdown has already finalized
                // the task as FAILED.
                *retry_suppressed = true;
                return Err(DruidError::Ingestion(format!(
                    "task {task_id} was shut down before its publication started"
                )));
            }
            registry.publish_fences.insert(
                task_id.to_string(),
                PublishFenceEntry {
                    verdict: fence_rx,
                    append_to_existing,
                    store_ops: Arc::clone(&store_ops),
                },
            );
            let probe = fence_probe.clone();
            let tid = task_id.to_string();
            let overlord = self.clone_handle();
            let append_mode = append_to_existing;
            let ops_gate = Arc::clone(&store_ops);
            tokio::spawn(async move {
                // D2: the datasource publish-lock guard is owned by THIS
                // wrapper task (not the droppable section future), bound
                // to a local so it is held until the section completes
                // normally OR the deadline arm below has fully resolved
                // the interrupted verdict — never released while a
                // dropped store op may still land.
                let _publish_guard = publish_guard;
                // DEADLINE (F3): a hung upload / metadata op must not
                // keep the fence sender alive forever (parking every
                // shutdown/guard finalizer and accumulating one fence +
                // task per hang). On expiry the section future is
                // DROPPED: it is cancelled at its current await point,
                // the fence sender drops with the last honest
                // provisional state — and any TRACKED store op keeps
                // running uncancelled in its own task (D2), holding the
                // `store_ops` gate until it truly resolves.
                match tokio::time::timeout(deadline, section).await {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        // R9-F1: the drop cancelled the section at an
                        // await point; a tracked commit/rollback may
                        // still be landing in its own task. Resolve the
                        // verdict by QUERYING DURABLE STATE (the F2
                        // segment⇄taskId correlation, batch provenance
                        // required) AFTER bound-waiting for the in-flight
                        // op to truly resolve (D2, inside
                        // `resolve_interrupted_publish_verdict`); the
                        // fence is only the bounded fallback when the
                        // store cannot answer. The publish lock stays
                        // held across all of it.
                        let committed = overlord
                            .resolve_interrupted_publish_verdict(
                                &tid,
                                append_mode,
                                &probe,
                                &ops_gate,
                            )
                            .await;
                        tracing::error!(
                            task_id = %tid,
                            deadline_ms = u64::try_from(deadline.as_millis())
                                .unwrap_or(u64::MAX),
                            committed,
                            "shielded publication exceeded its deadline and was \
                             cancelled at its current await point; verdict \
                             resolved from DURABLE state (committed iff a \
                             durable batch segment row names this task), not \
                             from the fence's provisional state — a dropped \
                             store op that lands after the drop must not \
                             invert the real durable outcome (R9-F1)"
                        );
                        PublishSectionOutcome {
                            // Unknown which op hung: fail closed, like the
                            // panic path — an append retry over unknown
                            // durable state could double count.
                            retry_suppressed: true,
                            committed,
                            result: Err(DruidError::Ingestion(format!(
                                "publication for {tid} exceeded its \
                                 {deadline:?} deadline and was cancelled; \
                                 verdict resolved from durable state"
                            ))),
                        }
                    }
                }
            })
        };
        let outcome = match publish_tail.await {
            Ok(outcome) => outcome,
            // The nested publication task PANICKED (nothing ever aborts
            // it): resolve from DURABLE state (R9-F1, same species as the
            // deadline — an unwind can drop a `commit().await` mid-call
            // exactly like a cancellation, so the provisional fence state
            // can invert the durable outcome). A panic AFTER a durable
            // append commit reads COMMITTED (the row + blob survive and
            // are reloaded on restart; a FAILED verdict would invite a
            // double-counting resubmit). The retry is suppressed either
            // way (an append retry over unknown durable state could
            // double count, Codex R23 H1 append variant). Residual
            // (documented): the wrapper's unwind released the publish
            // lock, so the D2 lock-held guarantee does not extend to this
            // forbidden-by-policy panic shape — the bound-wait on the ops
            // gate below still orders the durable read after any tracked
            // op that survived the unwind.
            Err(join_err) => PublishSectionOutcome {
                retry_suppressed: true,
                committed: self
                    .resolve_interrupted_publish_verdict(
                        task_id,
                        append_to_existing,
                        &fence_probe,
                        &store_ops,
                    )
                    .await,
                result: Err(DruidError::Ingestion(format!(
                    "publication task for {task_id} did not run to completion: {join_err}"
                ))),
            },
        };
        *retry_suppressed = outcome.retry_suppressed;
        if outcome.committed {
            // F1: the DATA is durably committed — that is the verdict,
            // even when a later rollback/cleanup step (or a post-commit
            // death) made the function-level result an `Err`. Reporting
            // anything but success here records a resubmit-inviting
            // FAILED for input that a restart reloads: the permanent
            // append double count.
            if let Err(e) = &outcome.result {
                tracing::warn!(
                    task_id,
                    error = %e,
                    "publish DURABLY COMMITTED although a later step \
                     failed; reporting SUCCESS (the fence's ground truth — \
                     a FAILED status would invite a double-appending \
                     resubmission of already-committed input)"
                );
            }
            return Ok(true);
        }
        outcome.result?;
        Ok(true)
    }

    /// The publication-critical section of
    /// [`execute_index_parallel`](Self::execute_index_parallel): Phase P
    /// (deep-storage persist) → Phase M (one atomic metadata transaction)
    /// → the query-visible swap, plus every failure-path compensation
    /// (metadata rollback, victim-drop reconcile, orphan-blob cleanup,
    /// append retry suppression). Spawned as its OWN task with the
    /// datasource publish-lock guard moved in (review High on ship2 H9):
    /// the OUTER batch tail is abortable by
    /// [`shutdown_task`](Self::shutdown_task), and dropping the outer
    /// tail's await of this task DETACHES it instead of cancelling it, so
    /// an abort can kill a stalled ingestion or lock-wait yet can never
    /// tear a publish mid-swap — this section always runs to its normal
    /// completion or rollback and only then releases the publish lock.
    ///
    /// Returns a [`PublishSectionOutcome`]: the `retry_suppressed`
    /// OUT-contract (`true` ONLY for the `appendToExisting` swap-failed +
    /// rollback-failed durable-retained residual and the unknown-state
    /// reconstructions), the DURABLE-COMMIT truth, and the
    /// function-level result.
    ///
    /// Owns the fence sender: the body keeps the fence's provisional
    /// state honest as the publish progresses (see [`FenceState`]), and
    /// this wrapper sends the FINAL `Resolved { committed }` before
    /// returning — so a watcher either observes the final verdict or,
    /// when the sender drops mid-section (panic / deadline
    /// cancellation), the last honest provisional state.
    ///
    /// The datasource publish-lock guard is deliberately NOT owned here
    /// (D2): the WRAPPER task that awaits this section with the deadline
    /// owns it, so a deadline drop of this future cannot release the
    /// lock while a tracked store op is still landing. `store_ops` is
    /// the gate those tracked ops hold (see [`run_tracked_store_op`]).
    async fn run_publish_critical_section(
        self,
        historical: Arc<Historical>,
        fence_tx: tokio::sync::watch::Sender<FenceState>,
        store_ops: Arc<tokio::sync::Mutex<()>>,
        args: PublishArgs,
    ) -> PublishSectionOutcome {
        let outcome = self
            .publish_critical_section_body(&historical, &fence_tx, &store_ops, args)
            .await;
        // Deliver the final verdict BEFORE the join can be observed: any
        // finalizer awaiting the fence wakes with the truth (a
        // receiver-less send is fine — nothing is watching).
        let _ = fence_tx.send(FenceState::Resolved {
            committed: outcome.committed,
        });
        outcome
    }

    /// Body of [`run_publish_critical_section`](Self::run_publish_critical_section)
    /// (split out so the wrapper can guarantee the final fence send on
    /// every return path). Sends PROVISIONAL fence states at the points
    /// where the durable truth changes; the wrapper sends the final one.
    async fn publish_critical_section_body(
        &self,
        historical: &Arc<Historical>,
        fence_tx: &tokio::sync::watch::Sender<FenceState>,
        store_ops: &Arc<tokio::sync::Mutex<()>>,
        args: PublishArgs,
    ) -> PublishSectionOutcome {
        // The datasource publish lock is held by the surrounding wrapper
        // task for this whole section (and, on a deadline drop, until the
        // interrupted verdict is resolved — D2).
        #[cfg(test)]
        {
            // Test-only pause point: signal that the shielded section has
            // STARTED, then park on the pause mutex while a test lands a
            // `shutdown_task` abort mid-section (publish lock held — the
            // real in-flight state).
            self.test_publish_entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let _pause = self.test_publish_pause.lock().await;
        }
        let PublishArgs {
            task_id,
            ds_name,
            segment_id,
            victims,
            append_to_existing,
            segment_data,
            num_rows,
            start_iso,
            end_iso,
            version,
        } = args;
        let task_id = task_id.as_str();
        // OUT-value: the caller's `retry_suppressed` contract.
        let mut suppress = false;

        // Phase P (persist) — upload the segment to deep storage BEFORE any
        // metadata is committed (compat-3 stage 1). Crash-consistency
        // invariant: a metadata row is only ever committed AFTER a durable
        // upload, so a restart's bootstrap reload always has a blob to
        // re-download. An upload failure aborts here — BEFORE Phase M — so
        // nothing is committed and the existing publish-failure path
        // handles it (no new failure plumbing). Batch has no offset
        // dimension, so the sequence is simply P → M → swap. Skipped (no
        // loadSpec) when no deep-storage backend is configured, preserving
        // the pre-persistence memory-resident behavior.
        let load_spec = match self.deep_storage.as_deref() {
            Some(ds) => match persist_segment(ds, &ds_name, &segment_id, &segment_data).await {
                Ok(spec) => Some(spec),
                Err(e) => {
                    return PublishSectionOutcome {
                        retry_suppressed: suppress,
                        committed: false,
                        result: Err(e),
                    };
                }
            },
            None => None,
        };

        // Phase 2 (M) — ONE atomic metadata transaction (victims -> unused
        // AND the new row inserted). On failure nothing was written — there
        // is no partial state to roll back, and queries kept serving the old
        // segments the whole time.
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        let mut payload = serde_json::json!({
            "dataSource": ds_name,
            "numRows": num_rows,
            "taskId": task_id,
            // R9-F2: positive BATCH-provenance marker, so the stale-task
            // reconcile and the deadline-verdict resolution can REQUIRE
            // batch provenance instead of trusting a bare (user-
            // controllable) taskId coincidence with a streaming row.
            "kind": BATCH_SEGMENT_KIND,
        });
        if let Some(spec) = &load_spec {
            payload["loadSpec"] = spec.to_json();
        }
        let row = SegmentMetadataRow {
            id: segment_id.clone(),
            data_source: ds_name.clone(),
            created_date: now_iso,
            start: start_iso,
            end: end_iso,
            version,
            used: true,
            payload,
        };
        let victim_ids: Vec<String> = victims.iter().map(|v| v.id.clone()).collect();
        // If the metadata transaction fails AFTER Phase P uploaded a blob, that
        // blob is now unreferenced (no row was committed) — best-effort delete
        // it before surfacing the error so repeated failures do not leak orphan
        // storage (H8).
        //
        // The commit runs as a TRACKED store op (D2): its own spawned
        // task holds the `store_ops` gate for the op's true duration, so
        // a publish-deadline drop of THIS future at the join await never
        // tears the transaction into a "may still land later" write —
        // the op runs to its real conclusion and the deadline path
        // bound-waits on the gate before reading durable state (with
        // the publish lock still held).
        let commit_result = {
            let this = self.clone_handle();
            let victim_ids = victim_ids.clone();
            let row = row.clone();
            run_tracked_store_op(store_ops, async move {
                this.publish_replace_metadata(&victim_ids, &row).await
            })
            .await
        };
        if let Err(e) = commit_result {
            cleanup_orphan_blob(
                self.deep_storage.as_deref(),
                &ds_name,
                &segment_id,
                load_spec.is_some(),
                true, // no row was ever committed
            )
            .await;
            return PublishSectionOutcome {
                retry_suppressed: suppress,
                committed: false,
                result: Err(e),
            };
        }

        // Phase M COMMITTED. For a DURABLE append row, record that on the
        // fence NOW (F1): from this instant a death (panic / deadline
        // cancellation) leaves a committed row + blob that the next
        // restart reloads, so any watcher reading the dropped-sender
        // fence must classify it COMMITTED — a FAILED verdict would
        // invite an append resubmission that double counts after the
        // reload. A later SUCCESSFUL rollback (swap-failure path below)
        // downgrades the verdict before any further await. Replace mode
        // deliberately never sets this: a replace resubmission is
        // idempotent for the interval (fail-closed is safe and preserves
        // the retry-heals-visibility behavior).
        if append_to_existing && load_spec.is_some() {
            let _ = fence_tx.send(FenceState::DurableAppendCommitted);
        }
        #[cfg(test)]
        {
            // Test-only pause point (R9-F1): park AFTER the Phase-M commit
            // (and its provisional fence update) so a test can drop this
            // section at the publish deadline inside the
            // committed-but-unresolved window.
            self.test_post_commit_entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let _pause = self.test_post_commit_pause.lock().await;
        }

        // Phase 3 — one atomic query-visible swap: victims out, new
        // segment (with its datasource mapping) in, under a single
        // write-lock acquisition.
        match historical.replace_segments(
            &victim_ids,
            vec![SegmentSwapEntry {
                id: segment_id.clone(),
                data: Arc::new(segment_data),
                datasource: Some(ds_name.clone()),
            }],
        ) {
            Ok(removed) => {
                // A victim with a metadata row but no loaded segment (e.g.
                // after a restart) is tolerated: its row was already
                // flipped to unused above.
                for victim in &victims {
                    if removed.iter().any(|r| r.id == victim.id) {
                        tracing::info!(
                            task_id,
                            data_source = %ds_name,
                            segment_id = %victim.id,
                            "appendToExisting=false: replaced (dropped + marked unused) \
                             overlapping segment"
                        );
                    } else {
                        tracing::warn!(
                            task_id,
                            segment_id = %victim.id,
                            "replaced segment was not loaded in the historical; \
                             marked unused in metadata only"
                        );
                    }
                }
            }
            Err(e) => {
                // Reachable when the Historical's lock is poisoned, an
                // undeclared segment-id collision is detected, or — in
                // SPILL residency mode (FG-7) — the swap's spill write
                // fails on a transient disk error (disk full / EACCES /
                // fsync); the swap mutates nothing on failure. Undo the
                // committed metadata transaction with ONE compensating
                // transaction — delete the new row, restore the victim
                // snapshots verbatim — so metadata keeps matching the
                // unchanged query-visible state. The publish lock is still
                // held, so no admin disable can have landed in between for
                // the restore to overwrite (round-2 HIGH #2). A rollback
                // failure (the metadata store broken right after a
                // successful commit) is logged and the original error
                // surfaced; the committed post-replace metadata then stays,
                // and the query-visible state is RECONCILED to it below
                // (Codex R23 H1) instead of being left to double count on
                // a retry.
                // The compensating rollback is a TRACKED store op too
                // (D2): a deadline drop mid-rollback must not tear it —
                // the op resolves in its own task and the interrupted
                // verdict is read only after it truly did.
                let rollback_result = {
                    let this = self.clone_handle();
                    let segment_id = segment_id.clone();
                    let victims = victims.clone();
                    run_tracked_store_op(store_ops, async move {
                        this.rollback_replace_metadata(&segment_id, &victims).await
                    })
                    .await
                };
                let rolled_back = match rollback_result {
                    Ok(()) => true,
                    Err(restore_err) => {
                        tracing::error!(
                            task_id,
                            segment_id = %segment_id,
                            error = %restore_err,
                            "rollback could not un-publish the metadata after a \
                             historical swap failure"
                        );
                        false
                    }
                };
                // Codex R23 H1: the rollback failed, so metadata is
                // irrevocably committed as {victims → unused, new row →
                // used} while the Historical still serves the victims and
                // not the new segment. Left alone, a retry (the default
                // RetryPolicy auto-retries; a manual resubmission behaves
                // identically) plans its victims from USED metadata rows
                // only, so it selects the unloaded new row and never drops
                // the still-loaded old victims — its own segment then
                // loads NEXT TO them and every query double counts the
                // interval until a restart. Reconcile the query-visible
                // state to the committed metadata instead: drop the old
                // victims NOW. A drop-only swap spills nothing, so the
                // very failure class that broke the swap cannot break the
                // reconcile; its sole failure mode is a poisoned lock, in
                // which case every subsequent swap fails too — log it and
                // surface the original error (unchanged behavior). The
                // interval's data is temporarily ABSENT (a gap the retry —
                // or a restart's bootstrap reload of the durable new row —
                // heals) rather than double-counted: gap → eventual
                // consistency over a silently wrong answer.
                if !rolled_back && !victim_ids.is_empty() {
                    match historical.replace_segments(&victim_ids, vec![]) {
                        Ok(_) => {
                            tracing::warn!(
                                task_id,
                                data_source = %ds_name,
                                segment_id = %segment_id,
                                victims = victim_ids.len(),
                                "swap AND rollback failed: dropped the replace victims \
                                 to match the committed metadata (victims unused, new \
                                 row used). The interval is a temporary gap until a \
                                 retry republishes it or a restart reloads the durable \
                                 new row — this prevents a retry from double counting \
                                 the interval (Codex R23 H1)"
                            );
                        }
                        Err(reconcile_err) => {
                            tracing::error!(
                                task_id,
                                segment_id = %segment_id,
                                error = %reconcile_err,
                                "swap AND rollback failed, and the victim-drop \
                                 reconcile ALSO failed (poisoned lock): the old \
                                 victims stay loaded with unused metadata rows — a \
                                 retry may double count the interval until the \
                                 process restarts"
                            );
                        }
                    }
                }
                // Codex R23 H1 (append variant): with `appendToExisting`
                // there are NO victims, so the drop-reconcile above does
                // not apply. If the rollback ALSO failed AND the row is
                // DURABLE (a deep-storage blob backs it), the attempt's
                // used row + blob are RETAINED — the next restart's
                // bootstrap reload makes them query-visible — while the
                // segment is NOT visible in THIS session. An auto-retry
                // (or a manual resubmission) would append a SECOND durable
                // row for the same input under a fresh id (append never
                // selects the first as a victim, unlike a replace whose
                // next plan picks the retained row up and marks it
                // unused), and the restart reload would then load BOTH →
                // a permanent duplicate / over-count. SUPPRESS the retry
                // so exactly ONE durable row survives, exactly mirroring
                // the streaming `RetainedDurable` guard (R14/H2): the data
                // is durable and reloaded on restart (no loss), and no
                // second copy is ever appended (no duplicate). Honest
                // limitation: the interval is a gap until that restart. A
                // memory-only row (no `loadSpec`/blob) is NOT reloaded, so
                // it stays a normal retryable failure — re-running is the
                // only way to land the data and creates no restart
                // duplicate (parity with streaming's `ReSeek`).
                // F1: this durable-retained append residual IS a durable
                // commit — the row + blob survive and the next restart's
                // bootstrap reload serves them. The verdict is therefore
                // COMMITTED (⇒ terminal SUCCESS), never a
                // resubmit-inviting FAILED, and the retry stays
                // suppressed (re-running would append a second durable
                // copy). Replace mode stays NOT-committed on purpose: a
                // replace resubmission is idempotent and the retry heals
                // the visibility gap.
                let committed = append_to_existing && !rolled_back && load_spec.is_some();
                if committed {
                    suppress = true;
                    tracing::warn!(
                        task_id,
                        data_source = %ds_name,
                        segment_id = %segment_id,
                        "appendToExisting: the query-visible swap FAILED and the \
                         metadata rollback ALSO failed, so the durable segment row + \
                         deep-storage blob are RETAINED and will be reloaded on the \
                         next restart. The publish is DURABLY COMMITTED (terminal \
                         SUCCESS; a FAILED status would invite a resubmission that \
                         double counts after the restart reload) and the retry is \
                         suppressed so the same input is NOT appended again under a \
                         new id. Honest limitation: the segment is NOT query-visible \
                         in THIS session — it becomes visible after the next \
                         restart's bootstrap reload (Codex R23 H1 append variant, \
                         streaming R14/H2 RetainedDurable parity)"
                    );
                }
                // The verdict is now determined: resolve the fence BEFORE
                // the cleanup await below, so a death inside it (panic /
                // deadline cancellation) can no longer change how a
                // watcher classifies this publish. In particular a
                // SUCCESSFUL rollback downgrades a provisional
                // `DurableAppendCommitted` to NOT-committed here.
                let _ = fence_tx.send(FenceState::Resolved { committed });
                // Delete the orphan blob we uploaded in Phase P ONLY when the
                // rollback SUCCEEDED — i.e. the metadata row that referenced it
                // is gone (H5). If the rollback FAILED the blob is KEPT (its row
                // still points at it; deleting would create a phantom).
                cleanup_orphan_blob(
                    self.deep_storage.as_deref(),
                    &ds_name,
                    &segment_id,
                    load_spec.is_some(),
                    rolled_back,
                )
                .await;
                return PublishSectionOutcome {
                    retry_suppressed: suppress,
                    committed,
                    result: Err(e),
                };
            }
        }

        tracing::info!(
            task_id,
            data_source = %ds_name,
            segment_id = %segment_id,
            num_rows,
            replaced = victim_ids.len(),
            "index_parallel ingestion completed"
        );
        PublishSectionOutcome {
            retry_suppressed: suppress,
            committed: true,
            result: Ok(()),
        }
    }

    /// Apply the publish metadata change (victims → unused + new row) as
    /// one atomic transaction via [`MetadataStore::replace_segments_txn`].
    ///
    /// Split out so tests can inject a metadata failure here — and ONLY
    /// here; any rollback writes must keep using the real store — to
    /// exercise the publication failure path (Codex 2026-07-12 HIGH #2).
    /// Atomicity of a failure BETWEEN the transaction's writes is locked
    /// in at the store level
    /// (`replace_txn_failure_between_writes_leaves_no_partial_state`).
    async fn publish_replace_metadata(
        &self,
        victim_ids: &[String],
        row: &SegmentMetadataRow,
    ) -> Result<()> {
        #[cfg(test)]
        {
            // Test-only pause point (D2): park BEFORE the store commit so
            // a test can drop the section at the publish deadline while
            // the Phase-M commit is genuinely "still landing".
            self.test_commit_op_entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let _pause = self.test_commit_op_pause.lock().await;
        }
        #[cfg(test)]
        if self
            .inject_insert_segment_failure
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(DruidError::Metadata(
                "injected publish-metadata failure (test fault hook)".to_string(),
            ));
        }
        self.metadata.replace_segments_txn(victim_ids, row).await
    }

    /// Apply the COMPENSATING rollback (delete the new row, restore the
    /// victim snapshots verbatim) via
    /// [`MetadataStore::rollback_replace_txn`].
    ///
    /// Split out so tests can inject a rollback failure here — and ONLY
    /// here; the forward publish keeps using
    /// [`publish_replace_metadata`](Self::publish_replace_metadata) — to
    /// exercise the swap-failed + rollback-failed residual (Codex R23 H1)
    /// end to end.
    async fn rollback_replace_metadata(
        &self,
        segment_id: &str,
        victims: &[SegmentMetadataRow],
    ) -> Result<()> {
        #[cfg(test)]
        if self
            .inject_rollback_replace_failure
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(DruidError::Metadata(
                "injected rollback-metadata failure (test fault hook)".to_string(),
            ));
        }
        self.metadata
            .rollback_replace_txn(segment_id, victims)
            .await
    }

    /// Allocate a collision-free segment id under the datasource's publish
    /// lock (Codex 2026-07-12 round-2 HIGH #4).
    ///
    /// The base id `{ds}_{start}_{end}_{version}` embeds a
    /// millisecond-resolution version, so two tasks publishing over the
    /// same interval in the same millisecond produce the SAME id; pre-fix
    /// the second `INSERT OR REPLACE` + `load_segment` silently discarded
    /// the first task's rows. If the base id is taken — by any metadata
    /// row, used or unused, or any loaded segment — a Druid-style numeric
    /// suffix `_1`, `_2`, … is appended until the id is free. The plain
    /// `INSERT` inside [`MetadataStore::replace_segments_txn`] and the
    /// collision check in [`Historical::replace_segments`] remain as
    /// fail-closed backstops should an id slip through anyway.
    ///
    /// Callers MUST hold the datasource's publish lock: it is what keeps
    /// the exists-check and the subsequent insert race-free (every other
    /// writer of this datasource's rows serializes on the same lock).
    async fn allocate_segment_id(
        &self,
        historical: &Arc<Historical>,
        ds_name: &str,
        start_iso: &str,
        end_iso: &str,
        version: &str,
    ) -> Result<String> {
        allocate_segment_id_inner(
            &self.metadata,
            historical,
            ds_name,
            start_iso,
            end_iso,
            version,
        )
        .await
    }

    /// Plan the replace step for an `appendToExisting: false` task: return
    /// every used segment of `ds_name` whose interval overlaps the
    /// segmentGranularity buckets covering the new data (`data_bounds` =
    /// inclusive `(min, max)` row-timestamp millis), so the caller can
    /// mark them unused and swap them out of the Historical atomically.
    ///
    /// **Fail-closed containment check (Codex 2026-07-12 HIGH #1):** the
    /// batch ingester publishes one raw `[min, max+1)` segment per task,
    /// so an existing segment can extend OUTSIDE the replace scope — e.g.
    /// a segment holding Jan-1 *and* Jan-2 rows when a later task
    /// re-ingests only Jan-1 at DAY granularity. Dropping such a victim
    /// wholesale would silently delete its out-of-scope rows, so this
    /// method ABORTS the ingestion instead; only victims fully contained
    /// in the replace scope are ever returned. Segment splitting is
    /// deliberately not attempted — re-ingest data covering the full
    /// spanned interval (or use `appendToExisting: true`) to proceed.
    ///
    /// Read-only; callers must hold the datasource's publish lock so the
    /// plan cannot go stale before it is applied.
    async fn plan_replace_victims(
        &self,
        ds_name: &str,
        segment_granularity: Option<&serde_json::Value>,
        data_bounds: (i64, i64),
        new_segment_id: &str,
    ) -> Result<Vec<SegmentMetadataRow>> {
        let granularity = parse_segment_granularity(segment_granularity)?;
        let (scope_start, scope_end) = replace_scope(granularity, data_bounds.0, data_bounds.1)?;

        let existing = self.metadata.get_used_segments(ds_name).await?;
        let mut victims = Vec::new();
        for seg in existing {
            // Never treat the segment being published as its own victim.
            // With collision-free id allocation (`allocate_segment_id`,
            // round-2 HIGH #4) a live row can no longer share the new id,
            // so this guard is defense-in-depth only.
            if seg.id == new_segment_id {
                continue;
            }
            let seg_start = parse_interval_bound_millis(&seg.start).map_err(|e| {
                DruidError::Ingestion(format!(
                    "cannot parse existing segment '{}' interval start '{}' for \
                     replace-overlap check: {e}",
                    seg.id, seg.start
                ))
            })?;
            let seg_end = parse_interval_bound_millis(&seg.end).map_err(|e| {
                DruidError::Ingestion(format!(
                    "cannot parse existing segment '{}' interval end '{}' for \
                     replace-overlap check: {e}",
                    seg.id, seg.end
                ))
            })?;
            if seg_start >= scope_end || seg_end <= scope_start {
                continue; // no overlap with the replaced buckets
            }
            if seg_start < scope_start || seg_end > scope_end {
                return Err(DruidError::Ingestion(format!(
                    "replace would drop data outside the ingested interval: existing \
                     segment '{}' [{}/{}] spans beyond the replace scope [{}/{}] \
                     computed from the ingested rows' segmentGranularity buckets; \
                     aborting instead of losing the out-of-scope rows (re-ingest data \
                     covering the segment's full interval, or use appendToExisting: true)",
                    seg.id,
                    seg.start,
                    seg.end,
                    format_epoch_millis_iso(scope_start),
                    format_epoch_millis_iso(scope_end),
                )));
            }
            victims.push(seg);
        }
        Ok(victims)
    }

    /// Look up a task by its identifier.
    ///
    /// Falls back to the persisted task row when the id is not in the
    /// in-memory table (Codex compat-4 R3 H1): after a restart the
    /// in-memory map is empty, but a task that completed before the
    /// restart still has a durable metadata row — reporting it (instead of
    /// a 404 for a known id) is what stops a client from resubmitting a
    /// duplicate. The row's `payload` is a serialized [`TaskInfo`].
    pub async fn get_task(&self, id: &str) -> Result<Option<TaskInfo>> {
        {
            let tasks = self.running_tasks.read().await;
            if let Some(record) = tasks.get(id) {
                return Ok(Some(record.to_info()));
            }
        }
        match self.metadata.get_task(id).await? {
            Some(row) => {
                let info: TaskInfo = serde_json::from_value(row.payload).map_err(|e| {
                    DruidError::Metadata(format!("deserialize persisted task {id}: {e}"))
                })?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    /// Return all tasks that are currently in `RUNNING` state.
    pub async fn get_running_tasks(&self) -> Vec<TaskInfo> {
        let tasks = self.running_tasks.read().await;
        tasks
            .values()
            .filter(|t| t.state == TaskState::Running)
            .map(TaskRecord::to_info)
            .collect()
    }

    /// Return all tasks that have completed (`SUCCESS` or `FAILED`).
    pub async fn get_complete_tasks(&self) -> Vec<TaskInfo> {
        let tasks = self.running_tasks.read().await;
        tasks
            .values()
            .filter(|t| t.state.is_terminal())
            .map(TaskRecord::to_info)
            .collect()
    }

    /// Return all tasks that are waiting (`WAITING` or `PENDING`).
    pub async fn get_waiting_tasks(&self) -> Vec<TaskInfo> {
        let tasks = self.running_tasks.read().await;
        tasks
            .values()
            .filter(|t| matches!(t.state, TaskState::Waiting | TaskState::Pending))
            .map(TaskRecord::to_info)
            .collect()
    }

    /// Gracefully shut down a task by transitioning it to `FAILED`, releasing
    /// any locks it holds.
    ///
    /// `RUNNING`/`PENDING`/`WAITING` tasks transition to `FAILED`; tasks that
    /// are already terminal are left unchanged (idempotent).
    ///
    /// A batch task whose detached execute+publish tail is still live is
    /// ABORTED (review High on ship2 H9): the tail's future is dropped at
    /// its next await point, so a stalled ingestion or lock-wait stops
    /// consuming resources, and its interval locks are released both here
    /// and by the tail's own `SubmitLockGuard` drop. The abort cannot
    /// tear a publish: the P→M→swap publication-critical section runs in
    /// its own shielded task (see `execute_index_parallel`) that always
    /// runs to its normal completion or rollback.
    ///
    /// Abort DURING the shielded section (append double-count fix): when
    /// the publish fence shows a shielded publication in flight, this
    /// method AWAITS its verdict — bounded, because the section always
    /// completes or rolls back — and only then finalizes: a COMMITTED
    /// publish ends the task `SUCCESS` (the data is genuinely published;
    /// reporting FAILED would invite an `appendToExisting` resubmission
    /// of input that already landed — a permanent double count, since
    /// append accumulates where replace overwrites), anything else ends
    /// it `FAILED`, and the interval locks are released only once the
    /// publish has truly resolved. A tail aborted BEFORE its publication
    /// spawned still fails fast exactly as before (no publish can start
    /// afterwards: the spawn re-checks this method's registry reap under
    /// the same mutex and refuses). CONCURRENT shutdowns of the same
    /// task are safe: the fence stays registered until a finalizer (or
    /// the tail's own exit) has WRITTEN the truthful terminal status to
    /// the in-memory table — it is NOT removed when the aborted tail's
    /// future drops — so a second shutdown racing the first one's
    /// finalizer still finds the fence and awaits the same verdict
    /// instead of eagerly marking the task FAILED mid-publish; the
    /// finalizers (all funneled through
    /// [`finalize_batch_terminal`](Self::finalize_batch_terminal),
    /// including the aborted tail guard's own recovery) are idempotent
    /// (a committed publish is always SUCCESS, lock release and
    /// persistence are repeat-safe), and once the fence is gone the task
    /// is already terminal-and-truthful, so the eager path only ever
    /// re-persists the truth. An `Err` from the fence path means only
    /// that the finalizer's DURABLE persist did not land: the in-memory
    /// state, fence retirement, and lock release have all happened, and
    /// a bounded background retry is flushing the row — re-invoking this
    /// method (or just waiting) converges. Residual (documented): the
    /// aborted tail's own `SubmitLockGuard` may release the interval
    /// locks while the shielded publish drains — the terminal status,
    /// which is what drives resubmission decisions, still tells the
    /// truth.
    pub async fn shutdown_task(&self, id: &str) -> Result<()> {
        self.abort_tail_and_finalize(id).await
    }

    /// The ONE registry-linearized abort + fence-aware finalizer behind
    /// EVERY manual terminal path of a batch task — [`shutdown_task`],
    /// a terminal [`transition_task`], and [`lose_worker`] retry-budget
    /// exhaustion all route here, so no administrative path can persist
    /// a terminal status (or release interval locks) while a shielded
    /// publication is still deciding the truth. All behavior documented
    /// on [`shutdown_task`] (the original sole caller) applies verbatim.
    ///
    /// [`shutdown_task`]: Overlord::shutdown_task
    /// [`transition_task`]: Overlord::transition_task
    /// [`lose_worker`]: Overlord::lose_worker
    async fn abort_tail_and_finalize(&self, id: &str) -> Result<()> {
        // Phase 1 — synchronous under `running_tasks` → registry (the
        // same lock order as `submit_task`): abort the live tail and
        // decide whether a SHIELDED publication is in flight.
        let fence = {
            let mut tasks = self.running_tasks.write().await;
            let task = tasks
                .get_mut(id)
                .ok_or_else(|| DruidError::Metadata(format!("task not found: {id}")))?;
            if task.state.is_terminal() {
                None
            } else {
                // Abort the detached batch tail, if one is live, and
                // clone its publish fence, if one is registered — both
                // under ONE registry acquisition, so this linearizes
                // against the fence-registration in
                // `execute_index_parallel`: either the reap of the
                // abort-handle entry lands first (the tail then REFUSES
                // to spawn its publication → no publish can ever
                // contradict the FAILED verdict below) or the fence is
                // already visible here (→ its verdict is awaited before
                // any finalization). Removing the abort-handle entry is
                // belt-and-braces only — the aborted tail's
                // `BatchTailGuard` also removes it when the future is
                // dropped. The registry guard is released at the `let`
                // statement's end, BEFORE `abort()` runs: should the
                // abort ever drop the tail inline, the tail's guard can
                // re-take the registry lock without self-deadlocking.
                let (tail, fence) = {
                    let mut registry = lock_batch_tails(&self.batch_tails);
                    (
                        registry.tails.remove(id),
                        registry.publish_fences.get(id).cloned(),
                    )
                };
                if let Some(handle) = tail {
                    handle.abort();
                }
                // Only a task with NO publish in flight may be failed
                // immediately; one inside the shielded section is
                // finalized by the fence verdict below.
                if fence.is_none() {
                    task.state = TaskState::Failed;
                    task.worker = None;
                    task.location = None;
                }
                fence
            }
        };

        let Some(fence) = fence else {
            // No shielded publication in flight (or the task was already
            // terminal): finalize exactly as before.
            self.release_task_locks(id).await;
            self.persist_existing(id).await?;
            return Ok(());
        };

        // Phase 2 — a shielded publication is in flight. Await its
        // verdict and finalize in a DETACHED task (mirroring the H9
        // spawn): a caller dropping THIS request future mid-await must
        // not strand the task un-finalized. The await is bounded: the
        // publication completes or rolls back, and a HUNG one is
        // cancelled at the publish deadline (F3) — either way the fence
        // resolves and this finalizer is released.
        let this = self.clone_handle();
        let task_id = id.to_string();
        let finalizer = tokio::spawn(async move {
            // Resolution or death (panic / deadline cancellation), the
            // verdict is definite: a fence that closes WITHOUT a
            // `Resolved` verdict is resolved from DURABLE state (D1) —
            // the same truth the deadline path derives — never persisted
            // from its provisional snapshot when the store can answer.
            let committed = this.await_registered_fence_verdict(&task_id, fence).await;
            // Truth write + fence retirement + lock release + persist
            // (bounded background retry on a persist failure), shared
            // with — and idempotent against — the tail's own exit
            // finalizer and the tail guard's recovery.
            this.finalize_batch_terminal(&task_id, committed, None)
                .await
        });
        match finalizer.await {
            Ok(result) => result,
            // Nothing aborts the finalizer; a JoinError means it
            // panicked — fail closed rather than report a clean shutdown.
            Err(join_err) => Err(DruidError::Ingestion(format!(
                "shutdown finalizer for {id} did not run to completion: {join_err}"
            ))),
        }
    }

    /// Return all tasks (any state) for a given data source.
    pub async fn get_tasks_by_datasource(&self, ds: &str) -> Vec<TaskInfo> {
        let tasks = self.running_tasks.read().await;
        tasks
            .values()
            .filter(|t| t.data_source == ds)
            .map(TaskRecord::to_info)
            .collect()
    }

    /// Transition a task to a new legacy [`TaskStatus`].
    ///
    /// This is the backward-compatible entrypoint used by the REST layer. It
    /// maps the 4-state status onto the lifecycle state machine and rejects
    /// transitions that are invalid for the current state.
    pub async fn update_task_status(&self, id: &str, status: TaskStatus) -> Result<()> {
        let target = match status {
            TaskStatus::Pending => TaskState::Pending,
            TaskStatus::Running => TaskState::Running,
            TaskStatus::Success => TaskState::Success,
            TaskStatus::Failed => TaskState::Failed,
        };
        self.transition_task(id, target).await
    }

    /// Transition a task to an explicit lifecycle [`TaskState`], enforcing the
    /// state-machine's valid-edge rules.
    ///
    /// Returns an error if the task does not exist or the transition is
    /// invalid for its current state. Reaching a terminal state releases the
    /// task's locks.
    ///
    /// A TERMINAL target on a batch (`index`/`index_parallel`) task with a
    /// LIVE tracked tail or registered publish fence must not bypass the
    /// tail/fence protocol (append double-count fix, manual-path variant):
    ///
    /// * `FAILED` routes through the SAME registry-linearized abort +
    ///   fence-aware finalizer as [`shutdown_task`](Self::shutdown_task)
    ///   ([`abort_tail_and_finalize`](Self::abort_tail_and_finalize)): the
    ///   tail is aborted, an in-flight shielded publication's verdict is
    ///   awaited, and the task ends with the fence-derived TRUTH — a
    ///   committed append ends **SUCCESS** (the requested FAILED is
    ///   overridden: a durable FAILED on committed input invites a
    ///   double-appending resubmission), anything else ends FAILED. Locks
    ///   are released only once the publish has truly resolved.
    /// * `SUCCESS` is REJECTED (`Err`): nothing may claim success while
    ///   the execute/publish tail is live — the publish could still fail
    ///   or roll back, and a durable SUCCESS would silently LOSE the
    ///   batch (a client trusting it never resubmits). The verdict
    ///   belongs to the publish fence; poll the task status instead (or
    ///   abort via `shutdown_task`).
    ///
    /// Tasks with no live tail/fence (streaming, stub-path, manually
    /// driven, or already terminal) keep the direct transition: no
    /// publish can be in flight for them — a fence is only ever
    /// registered by a live tracked tail, and fence registration refuses
    /// once the tail's registry entry is gone.
    pub async fn transition_task(&self, id: &str, target: TaskState) -> Result<()> {
        let route_to_finalizer = {
            let mut tasks = self.running_tasks.write().await;
            let task = tasks
                .get_mut(id)
                .ok_or_else(|| DruidError::Metadata(format!("task not found: {id}")))?;
            if task.state == target {
                return Ok(());
            }
            if !task.state.can_transition_to(target) {
                return Err(DruidError::Ingestion(format!(
                    "invalid task transition for {id}: {} -> {}",
                    task.state.as_str(),
                    target.as_str()
                )));
            }
            // Same lock order as `submit_task`/`shutdown_task`
            // (`running_tasks` → registry, no await under either): a live
            // tail/fence observed here cannot finalize out from under us
            // before we route, and one absent here can never spawn a
            // publication that contradicts the direct write below (fence
            // registration re-checks the tails registry under this same
            // mutex).
            let live_batch_tail = target.is_terminal()
                && matches!(task.task_type.as_str(), "index" | "index_parallel")
                && {
                    let registry = lock_batch_tails(&self.batch_tails);
                    registry.tails.contains_key(id) || registry.publish_fences.contains_key(id)
                };
            if live_batch_tail && target == TaskState::Success {
                return Err(DruidError::Ingestion(format!(
                    "cannot manually transition batch task {id} to SUCCESS while its \
                     execute/publish tail is live: success is decided by the publish \
                     verdict (poll the task status, or abort via task shutdown)"
                )));
            }
            if !live_batch_tail {
                task.state = target;
                if matches!(target, TaskState::Pending | TaskState::Waiting) {
                    task.worker = None;
                    task.location = None;
                }
            }
            live_batch_tail
        };
        if route_to_finalizer {
            // Manual FAILED on a live batch tail: the shared finalizer
            // aborts the tail and writes the fence-derived truth
            // (idempotent + re-checked under the same locks, so any race
            // with the tail's own finalization converges on the truth).
            return self.abort_tail_and_finalize(id).await;
        }
        if target.is_terminal() {
            self.release_task_locks(id).await;
        }
        self.persist_existing(id).await?;
        Ok(())
    }

    /// Persist the current in-memory state of an existing task to metadata.
    async fn persist_existing(&self, id: &str) -> Result<()> {
        let row = {
            let tasks = self.running_tasks.read().await;
            match tasks.get(id) {
                Some(r) => r.to_row()?,
                None => return Ok(()),
            }
        };
        self.metadata.update_task_status(&row).await?;
        Ok(())
    }

    // ----- Worker management -----------------------------------------------

    /// Register (or replace) a worker available for task assignment.
    pub async fn register_worker(&self, worker: Worker) {
        let mut guard = self.workers.lock().await;
        guard.register(worker);
    }

    /// Deregister a worker and reschedule any of its RUNNING tasks.
    ///
    /// Each task currently assigned to the lost worker is moved back to
    /// `PENDING` (re-assignable) if it still has retry budget, otherwise it
    /// transitions to `FAILED`. Returns the list of affected task ids.
    ///
    /// Retry-budget exhaustion on a batch (`index`/`index_parallel`) task
    /// with a LIVE tracked tail or registered publish fence must not write
    /// that FAILED directly (append double-count fix, manual-path
    /// variant): the tail may be inside its shielded publication, and a
    /// durable FAILED + released locks invites a resubmission of input
    /// that still commits — a permanent double count. Such tasks route
    /// through the SAME registry-linearized abort + fence-aware finalizer
    /// as [`shutdown_task`](Self::shutdown_task)
    /// ([`abort_tail_and_finalize`](Self::abort_tail_and_finalize)): the
    /// tail is aborted, an in-flight publish verdict is awaited, a
    /// committed append ends **SUCCESS** (never a resubmit-inviting
    /// FAILED), anything else ends FAILED, and locks release only once
    /// the publish has resolved. Tasks with no live tail/fence keep the
    /// direct path (no publish can be in flight for them). An `Err`
    /// return follows the shutdown contract: for routed tasks it means
    /// only that the durable persist has not landed yet (the in-memory
    /// truth is written and a bounded background retry is flushing).
    pub async fn lose_worker(&self, worker_id: &str) -> Result<Vec<String>> {
        {
            let mut guard = self.workers.lock().await;
            guard.deregister(worker_id);
        }
        let mut affected = Vec::new();
        let mut to_fail = Vec::new();
        let mut to_finalize = Vec::new();
        {
            let mut tasks = self.running_tasks.write().await;
            for task in tasks.values_mut() {
                if task.worker.as_deref() == Some(worker_id) && task.state == TaskState::Running {
                    affected.push(task.id.clone());
                    if self.retry_policy.can_retry(task.attempt) {
                        task.worker = None;
                        task.location = None;
                        task.state = TaskState::Pending;
                    } else if matches!(task.task_type.as_str(), "index" | "index_parallel") && {
                        // Same lock order as `shutdown_task`
                        // (`running_tasks` → registry, no await): a
                        // tail/fence absent here can never spawn a
                        // contradicting publication later.
                        let registry = lock_batch_tails(&self.batch_tails);
                        registry.tails.contains_key(&task.id)
                            || registry.publish_fences.contains_key(&task.id)
                    } {
                        // Do NOT write FAILED here: the terminal state
                        // (and the worker/location clearing) is written
                        // by the fence-aware finalizer below, from the
                        // publish-derived truth.
                        to_finalize.push(task.id.clone());
                    } else {
                        task.worker = None;
                        task.location = None;
                        task.state = TaskState::Failed;
                        to_fail.push(task.id.clone());
                    }
                }
            }
        }
        // Route the live-tail terminal tasks FIRST (the correctness-
        // critical part): abort + fence-derived truth via the shared
        // shutdown machinery. Attempt every task even if one finalizer
        // reports a (persist-retry-pending) error.
        let mut first_err = None;
        for id in &to_finalize {
            if let Err(e) = self.abort_tail_and_finalize(id).await {
                // The in-memory truth is written, the fence retired, and
                // the locks released; only the durable persist is still
                // flushing (bounded background retry).
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        for id in &to_fail {
            self.release_task_locks(id).await;
        }
        for id in &affected {
            if to_finalize.contains(id) {
                // Persisted by the fence-aware finalizer.
                continue;
            }
            self.persist_existing(id).await?;
        }
        Ok(affected)
    }

    /// Current number of registered workers.
    pub async fn worker_count(&self) -> usize {
        self.workers.lock().await.len()
    }

    /// Assign a worker to a task using the configured selection strategy.
    ///
    /// Load is computed from the authoritative in-memory assignment table.
    /// Returns the chosen worker id, or `None` if no worker has free capacity
    /// (the task then runs unassigned — in-process execution still proceeds).
    async fn assign_worker(&self, _task_id: &str) -> Option<String> {
        let load_map = {
            let tasks = self.running_tasks.read().await;
            let mut m: HashMap<String, u32> = HashMap::new();
            for t in tasks.values() {
                if t.state == TaskState::Running
                    && let Some(w) = &t.worker
                {
                    *m.entry(w.clone()).or_insert(0) += 1;
                }
            }
            m
        };
        let mut guard = self.workers.lock().await;
        if guard.is_empty() {
            return None;
        }
        let load = |id: &str| *load_map.get(id).unwrap_or(&0);
        guard.select(&load).map(|w| w.id())
    }

    // ----- Task locks ------------------------------------------------------

    /// Request a TimeChunk lock on behalf of a task.
    ///
    /// Returns `Ok(Some(lock))` when granted (after preempting any strictly
    /// lower-priority conflicting locks), or `Ok(None)` when blocked by an
    /// equal-or-higher-priority holder.
    pub async fn acquire_lock(
        &self,
        task_id: &str,
        data_source: &str,
        interval: Interval,
        lock_type: LockType,
        priority: i64,
    ) -> Result<Option<TaskLock>> {
        let req = TaskLock {
            id: String::new(),
            task_id: task_id.to_string(),
            data_source: data_source.to_string(),
            interval,
            lock_type,
            priority,
            revoked: false,
        };
        if self.try_acquire_lock(req.clone()).await? {
            // Re-read to recover the persisted id.
            let locks = self.metadata.get_locks_for_task(task_id).await?;
            let granted = locks
                .into_iter()
                .filter_map(|r| lock_row_to_lock(&r).ok())
                .find(|l| {
                    l.data_source == req.data_source
                        && l.interval == req.interval
                        && l.lock_type == req.lock_type
                        && !l.revoked
                });
            Ok(granted)
        } else {
            Ok(None)
        }
    }

    /// Return the active (non-revoked) locks for a data source.
    pub async fn locks_for_datasource(&self, data_source: &str) -> Result<Vec<TaskLock>> {
        let rows = self
            .metadata
            .get_locks_for_datasource(data_source, false)
            .await?;
        rows.iter().map(lock_row_to_lock).collect()
    }

    /// Core lock acquisition against persisted state. Returns whether the lock
    /// was granted (preempting strictly-lower-priority holders if needed).
    async fn try_acquire_lock(&self, req: TaskLock) -> Result<bool> {
        // Permanently close the bootstrap orphaned-lock reconcile window
        // BEFORE any acquisition state is touched (review High on ship2
        // H10): the lock row this call may persist belongs to a task that
        // is NOT yet in `running_tasks` (`submit_task` inserts the record
        // only after the grant), so from this point on a reconcile pass
        // could no longer tell a live local lock from a crash orphan.
        // Taking the gate mutex here also ORDERS this acquisition against
        // an in-flight bootstrap reap: either the reap finished before
        // this lock exists, or this stamp wins and the reap refuses. No
        // other guard is held while waiting, so no lock-order cycle.
        {
            let mut gate = self.task_lock_reconcile_gate.lock().await;
            *gate = true;
        }
        // Serialize the entire read → evaluate → revoke → insert sequence on
        // a per-datasource mutex. Two concurrent overlapping EXCLUSIVE
        // requests must not both observe an empty active set and both be
        // granted; holding this guard across the whole critical section makes
        // the conflict evaluation see the effect of any concurrent grant.
        let acquire_guard = {
            let mut map = self.lock_acquire_locks.lock().await;
            Arc::clone(
                map.entry(req.data_source.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let _serialized = acquire_guard.lock().await;

        let existing_rows = self
            .metadata
            .get_locks_for_datasource(&req.data_source, false)
            .await?;
        let existing: Vec<TaskLock> = existing_rows
            .iter()
            .map(lock_row_to_lock)
            .collect::<Result<Vec<_>>>()?;

        match evaluate_lock_request(&req, &existing) {
            LockDecision::Blocked => Ok(false),
            LockDecision::Preempt(ids) => {
                for id in ids {
                    self.metadata.revoke_lock(&id).await?;
                }
                self.persist_lock(&req).await?;
                Ok(true)
            }
            LockDecision::Granted => {
                self.persist_lock(&req).await?;
                Ok(true)
            }
        }
    }

    async fn persist_lock(&self, lock: &TaskLock) -> Result<()> {
        let row = lock_to_lock_row(lock)?;
        self.metadata.insert_task_lock(&row).await?;
        // Durable registry of every datasource that ever held a task lock
        // (ship2 H10): the store enumerates lock rows per datasource only,
        // so startup reconciliation derives its candidate set from task
        // rows + segment rows + THIS registry. Without it, a lock whose
        // owner crashed before its first task-row persist — on a
        // datasource with no segments yet — would be un-enumerable and
        // block that interval forever. Idempotent per-datasource upsert
        // (no read-modify-write races); best-effort: on failure the
        // task-row candidates still cover any orphan that actually blocks
        // a submission (the parked submission persists a WAITING row
        // naming the datasource), one restart later.
        if let Err(e) = self
            .metadata
            .set_config(
                &format!("{LOCK_DS_CONFIG_PREFIX}{}", lock.data_source),
                &serde_json::Value::Bool(true),
            )
            .await
        {
            tracing::warn!(
                data_source = %lock.data_source,
                error = %e,
                "failed to register the lock datasource for startup lock \
                 reconciliation (the lock itself is persisted)"
            );
        }
        Ok(())
    }

    /// Release (delete) all locks held by a task.
    async fn release_task_locks(&self, task_id: &str) {
        match self.metadata.get_locks_for_task(task_id).await {
            Ok(rows) => {
                for r in rows {
                    if let Err(e) = self.metadata.delete_lock(&r.id).await {
                        tracing::warn!(task_id, lock_id = %r.id, error = %e, "failed to delete lock");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(task_id, error = %e, "failed to list locks for release");
            }
        }
    }

    /// Release durable task locks orphaned by a process crash (ship2 H10).
    ///
    /// Task locks are persisted (`druid_tasklocks`) but every release path
    /// is in-process — `run_with_retry`'s terminal paths, `shutdown_task`
    /// / `transition_task`, and the `SubmitLockGuard` cancellation drop —
    /// so a crash while any task held an interval lock left the row behind
    /// forever: after a restart `try_acquire_lock` kept re-reading it,
    /// every equal-priority submission overlapping the interval parked
    /// WAITING with no error surfaced, and `shutdown_task` (in-memory
    /// lookup only) offered no API path to clear it. This reconciles the
    /// persisted lock rows against the tasks LIVE in THIS process — every
    /// lock whose owner is not a live (non-terminal, in-memory) task is
    /// deleted — and is invoked from
    /// [`bootstrap_reload_segments`](Overlord::bootstrap_reload_segments)
    /// at startup, when the in-memory table is empty and therefore every
    /// persisted lock is by definition a pre-restart orphan.
    ///
    /// **Bootstrap-only, ENFORCED (review High).** A task being submitted
    /// persists its granted lock BEFORE its record enters the in-memory
    /// table ([`submit_task`](Overlord::submit_task) calls
    /// `try_acquire_lock` ahead of `persist_and_store`), so a reconcile
    /// running concurrently with — or any time after — a local
    /// acquisition could mistake that live lock for an orphan, delete it,
    /// and let an overlapping equal-priority task acquire + publish
    /// concurrently (a later metadata swap would then mark the other
    /// task's segment unused = lost rows). This is therefore gated, not
    /// merely documented: the reap runs holding the
    /// `task_lock_reconcile_gate` mutex — the same mutex
    /// `try_acquire_lock` stamps closed before ANY local lock is
    /// persisted — and permanently degrades to a warn + `Ok(0)` no-op
    /// once any local acquisition has begun. While a reap is in flight, a
    /// concurrent acquisition blocks on the gate until the reap
    /// completes; a lock granted by this process can NEVER be deleted
    /// here. The cost is honest and bounded: a lock leaked mid-serving
    /// (e.g. a panicked publish tail) is reaped at the NEXT restart's
    /// bootstrap rather than live.
    ///
    /// **Single-live-overlord scope (honest limitation).** This reap
    /// assumes it is the ONLY live Overlord against this metadata store —
    /// FerroDruid's supported topology is the single-binary, single-node
    /// deployment (the store-level `datasource_publish_lock` is likewise
    /// process-local). A SECOND concurrently-live overlord sharing the
    /// store (e.g. a rolling restart with overlap) is indistinguishable
    /// from a crashed predecessor: its live tasks' locks WOULD be reaped
    /// as orphans here, re-opening the concurrent-overlapping-publish
    /// row-loss race. The lock row carries no owner-liveness/heartbeat
    /// field, so another live instance cannot be detected cheaply — do
    /// not run two live overlords against one metadata store.
    ///
    /// Lock rows are enumerated per datasource; the candidate set is
    /// drawn from persisted task rows, segment rows, and the durable
    /// lock-datasource registry `persist_lock` stamps
    /// ([`LOCK_DS_CONFIG_PREFIX`]), so a lock is enumerable even when its
    /// owner crashed before its first task-row persist on a datasource
    /// with no segments. Residual: if that registry stamp itself failed
    /// (warn-logged in `persist_lock`) the orphan is found one restart
    /// later, once the first blocked submission has persisted a WAITING
    /// task row naming the datasource.
    ///
    /// Returns the number of locks released.
    ///
    /// # Errors
    /// Propagates metadata-store failures enumerating tasks, datasources,
    /// or lock rows (individual delete failures are logged and skipped).
    pub async fn reconcile_orphaned_task_locks(&self) -> Result<usize> {
        // Hold the gate for the WHOLE reap: while held, no local lock can
        // be granted (`try_acquire_lock` stamps under this same mutex);
        // once any local acquisition has stamped it, refuse permanently —
        // a persisted lock row may now belong to a live local task absent
        // from `running_tasks`, indistinguishable from a crash orphan.
        let gate = self.task_lock_reconcile_gate.lock().await;
        if *gate {
            tracing::warn!(
                "skipping orphaned-task-lock reconciliation: this process \
                 has already begun acquiring task locks, so a persisted \
                 lock row may belong to a live local task (a submission's \
                 lock is granted before its record enters the in-memory \
                 table) and cannot be distinguished from a crash orphan; \
                 reconciliation only runs at bootstrap, before any local \
                 lock activity — a genuinely orphaned lock is reaped at \
                 the next restart"
            );
            return Ok(0);
        }
        // Live = non-terminal tasks in the in-memory table. With the gate
        // OPEN this process has never acquired a lock, so no live local
        // task can own any persisted row; the filter below is retained as
        // defense-in-depth only.
        let live: std::collections::HashSet<String> = {
            let tasks = self.running_tasks.read().await;
            tasks
                .values()
                .filter(|t| !t.state.is_terminal())
                .map(|t| t.id.clone())
                .collect()
        };
        let mut data_sources: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        for task in self.metadata.get_all_tasks().await? {
            data_sources.insert(task.data_source);
        }
        for ds in self.metadata.get_all_data_sources().await? {
            data_sources.insert(ds);
        }
        // The durable lock-datasource registry stamped by `persist_lock`
        // covers locks whose owner never persisted a task row on a
        // datasource that has no segments (see `persist_lock`).
        for (name, _) in self.metadata.get_all_config().await? {
            if let Some(ds) = name.strip_prefix(LOCK_DS_CONFIG_PREFIX) {
                data_sources.insert(ds.to_string());
            }
        }
        let mut released = 0usize;
        for ds in &data_sources {
            for lock in self.metadata.get_locks_for_datasource(ds, true).await? {
                if live.contains(&lock.task_id) {
                    continue;
                }
                match self.metadata.delete_lock(&lock.id).await {
                    Ok(()) => {
                        released += 1;
                        tracing::warn!(
                            task_id = %lock.task_id,
                            lock_id = %lock.id,
                            data_source = %ds,
                            interval_start = %lock.interval_start,
                            interval_end = %lock.interval_end,
                            "released a task lock orphaned by a previous process \
                             (its owning task is not running); it would otherwise \
                             block equal-priority ingestion on this interval \
                             forever; assumes this is the ONLY live overlord on \
                             this metadata store (the supported single-binary \
                             topology) — a second live overlord's locks are \
                             indistinguishable from crash orphans"
                        );
                    }
                    Err(e) => tracing::warn!(
                        task_id = %lock.task_id,
                        lock_id = %lock.id,
                        error = %e,
                        "failed to delete an orphaned task lock"
                    ),
                }
            }
        }
        Ok(released)
    }

    /// True when a DURABLE (`loadSpec`-bearing) segment row of BATCH
    /// provenance names `task_id` as its producer — the same
    /// segment⇄taskId correlation
    /// [`reconcile_stale_running_batch_tasks`](Overlord::reconcile_stale_running_batch_tasks)
    /// uses as commit proof (used OR unused: a later replace retiring
    /// the segment does not un-commit the task).
    ///
    /// # Errors
    /// Propagates metadata-store failures enumerating segments.
    async fn durable_batch_commit_for_task(&self, task_id: &str) -> Result<bool> {
        Ok(self.metadata.get_all_segments().await?.iter().any(|seg| {
            seg.payload
                .get("taskId")
                .and_then(serde_json::Value::as_str)
                == Some(task_id)
                && seg.payload.get("loadSpec").is_some()
                && segment_payload_is_batch(&seg.payload)
        }))
    }

    /// Resolve the verdict of a publication whose shielded section died
    /// WITHOUT resolving its own fence (publish-deadline cancellation,
    /// or a panic reconstructed from a `JoinError`) by querying DURABLE
    /// STATE (R9-F1).
    ///
    /// The fence's last provisional state is NOT authoritative for these
    /// deaths: the section was dropped at an await point, and a metadata
    /// commit/rollback whose future was dropped MID-CALL may still land
    /// in the database afterwards. Classifying from the provisional
    /// state alone can then produce the OPPOSITE of the durable outcome:
    ///
    /// * `InFlight` + a commit that landed anyway → terminal FAILED for
    ///   durably committed input — the client resubmits and the append
    ///   double counts permanently (the F2 reconcile cannot recover it:
    ///   it only rescans RUNNING rows, and this row is terminal);
    /// * `DurableAppendCommitted` + a rollback that landed anyway →
    ///   SUCCESS over a removed row — the batch is silently lost (the
    ///   client never resubmits a SUCCESS).
    ///
    /// The durable truth is the same correlation the F2 reconcile uses
    /// ([`durable_batch_commit_for_task`](Overlord::durable_batch_commit_for_task)):
    /// committed iff a DURABLE segment row of BATCH provenance names
    /// this task as its producer — so a streaming row with a
    /// coincidental `taskId` cannot spoof it (R9-F2). Replace mode stays
    /// fail-closed NOT-committed (a replace resubmission re-plans its
    /// victims and is idempotent for the interval — the unchanged F1
    /// replace semantics). The store query is BOUNDED by
    /// [`TERMINAL_PERSIST_ATTEMPT_TIMEOUT`] — the deadline may have
    /// fired precisely BECAUSE the metadata store is hung, and this
    /// resolution must not park the publish tail forever — and on a
    /// query failure/timeout it falls back to the fence's last
    /// provisional state (the pre-existing classification: no worse than
    /// before, and the store being unreachable makes a just-landed
    /// contradicting write unlikely).
    ///
    /// Before the read, this BOUND-WAITS on `store_ops` — the gate every
    /// UNCANCELLABLE tracked store op of the section holds for its true
    /// duration ([`run_tracked_store_op`], D2) — so a commit/rollback
    /// whose join await was dropped has truly resolved before the
    /// durable state is consulted: the read observes the op's REAL
    /// outcome instead of racing it.
    ///
    /// Honest residual: this is a point-in-time read. An op that outlives
    /// even [`PUBLISH_OP_RESOLVE_BOUND`] (a genuinely hung store) can
    /// still land after this read and diverge from the verdict — the
    /// same class as the documented crash-between-publish-and-persist
    /// window, now shrunk to the pathological hung-op tail.
    async fn resolve_interrupted_publish_verdict(
        &self,
        task_id: &str,
        append_to_existing: bool,
        fence: &tokio::sync::watch::Receiver<FenceState>,
        store_ops: &tokio::sync::Mutex<()>,
    ) -> bool {
        // The section resolved its own verdict before dying: trust it —
        // a `Resolved` fence is the section's final, post-compensation
        // truth, not a provisional snapshot (and its tracked ops all
        // completed before it resolved).
        if let FenceState::Resolved { committed } = *fence.borrow() {
            return committed;
        }
        // D2: wait (bounded) for any in-flight tracked store op to truly
        // resolve, so the durable read below cannot race a still-landing
        // commit/rollback. The acquired guard is dropped immediately —
        // only the "op has resolved" edge matters (the section is dead,
        // so no NEW op can start).
        if tokio::time::timeout(PUBLISH_OP_RESOLVE_BOUND, store_ops.lock())
            .await
            .is_err()
        {
            tracing::error!(
                task_id,
                "an uncancellable publication store op did not resolve \
                 within {PUBLISH_OP_RESOLVE_BOUND:?}; resolving the verdict \
                 from a point-in-time durable read that the hung op may \
                 still contradict if it eventually lands (documented \
                 residual)"
            );
        }
        if !append_to_existing {
            return false;
        }
        match tokio::time::timeout(
            TERMINAL_PERSIST_ATTEMPT_TIMEOUT,
            self.durable_batch_commit_for_task(task_id),
        )
        .await
        {
            Ok(Ok(present)) => present,
            Ok(Err(e)) => {
                tracing::error!(
                    task_id,
                    error = %e,
                    "durable-state resolution of an interrupted publish \
                     verdict failed; falling back to the fence's last \
                     provisional state"
                );
                fence_last_state_committed(fence)
            }
            Err(_) => {
                tracing::error!(
                    task_id,
                    "durable-state resolution of an interrupted publish \
                     verdict timed out (metadata store hung); falling back \
                     to the fence's last provisional state"
                );
                fence_last_state_committed(fence)
            }
        }
    }

    /// Await a REGISTERED publish fence's verdict, classifying EVERY
    /// termination shape — the shared logic of the `shutdown_task`
    /// finalizer and the [`BatchTailGuard`] recovery finalizer:
    ///
    /// * a normal resolution yields the section's own final verdict;
    /// * a fence that closes WITHOUT a `Resolved` verdict (the section
    ///   PANICKED, or the publish deadline cancelled it mid-await, F3)
    ///   is resolved from DURABLE STATE (D1) via
    ///   [`resolve_interrupted_publish_verdict`] — the same
    ///   batch-provenance-gated, bounded resolution the deadline path
    ///   itself uses, after bound-waiting for any uncancellable
    ///   in-flight store op of the section (D2). Pre-fix these
    ///   finalizers classified the closed fence from its last
    ///   PROVISIONAL state and persisted that — so a commit that landed
    ///   anyway was recorded FAILED (double-count invitation) and a
    ///   rollback that landed anyway was recorded SUCCESS (silent data
    ///   loss); the deadline task's own durable resolution could never
    ///   reach them. No finalizer may persist a provisional verdict when
    ///   the durable truth is knowable; the fence's provisional state
    ///   remains only the LAST fallback when the store cannot answer at
    ///   all.
    ///
    /// [`resolve_interrupted_publish_verdict`]: Overlord::resolve_interrupted_publish_verdict
    async fn await_registered_fence_verdict(
        &self,
        task_id: &str,
        entry: PublishFenceEntry,
    ) -> bool {
        let mut fence = entry.verdict;
        let resolved = fence
            .wait_for(|state| matches!(state, FenceState::Resolved { .. }))
            .await
            .map(|state| matches!(*state, FenceState::Resolved { committed: true }));
        match resolved {
            Ok(committed) => committed,
            Err(_) => {
                self.resolve_interrupted_publish_verdict(
                    task_id,
                    entry.append_to_existing,
                    &fence,
                    &entry.store_ops,
                )
                .await
            }
        }
    }

    /// Recover the durable VERDICT of stale RUNNING batch task rows at
    /// bootstrap (F2 — retry exhaustion loses the durable verdict across
    /// a restart).
    ///
    /// A batch task whose terminal-status persist failed past the bounded
    /// retry budget (or whose process crashed between publication and the
    /// terminal persist) leaves its durable row RUNNING while the
    /// truthful verdict lived only in the dead process's memory. After a
    /// restart, a client polling the id then saw a non-terminal state
    /// forever — and resubmitting a committed `appendToExisting` input
    /// appends it a SECOND time (the restart reload serves BOTH: a
    /// permanent double count).
    ///
    /// This reconciles every RUNNING (and crash-orphaned WAITING — see
    /// below) `index`/`index_parallel` row not
    /// LIVE in this process against the segment table, using the
    /// `taskId` each publish stamps into its segment row's payload —
    /// restricted to rows of BATCH provenance (R9-F2, see
    /// [`segment_payload_is_batch`]): streaming (kafka/kinesis) segments
    /// also carry a `taskId` and ids are user-controlled, so a
    /// coincidental match must never prove a batch commit:
    ///
    /// * a row with a DURABLE committed segment (any segment row naming
    ///   the task with a `loadSpec` — used OR unused, since a later
    ///   replace may have retired it) becomes **SUCCESS**: the publish
    ///   committed, the blob is restart-reloadable, and the recovered
    ///   verdict stops the duplicate resubmission;
    /// * a row whose committed segment rows are all MEMORY-ONLY (no
    ///   `loadSpec`) becomes **FAILED**: the restart lost the data (the
    ///   pre-persistence configuration is memory-resident by design), so
    ///   SUCCESS would claim data that is not served — while a resubmit
    ///   is safe (nothing reloads, so nothing double counts);
    /// * a row with NO committed segment becomes **FAILED**: the publish
    ///   never committed (or was rolled back), nothing is durable, and a
    ///   resubmission is safe.
    ///
    /// Either way the row leaves the un-recoverable RUNNING lie, its
    /// payload (served by the [`get_task`](Overlord::get_task) restart
    /// fallback) is updated to match, and its interval locks are
    /// released (repeat-safe with
    /// [`reconcile_orphaned_task_locks`](Overlord::reconcile_orphaned_task_locks)).
    ///
    /// **Single-live-overlord scope**, like the lock reconcile: rows
    /// belonging to tasks live in THIS process are skipped via the
    /// in-memory table. Under the async submit contract the durable
    /// RUNNING row is persisted just BEFORE the in-memory record is
    /// registered, so a row can exist for a live-but-not-yet-registered
    /// task for one instant — harmless here because this reconcile runs
    /// only during bootstrap, before task submission begins (and a
    /// SubmitRowGuard resolves the cancelled-submit shape in-process).
    /// A SECOND concurrently live overlord's RUNNING rows are
    /// indistinguishable from crash residue — do not run two live
    /// overlords on one metadata store.
    /// Streaming task types are never touched. Batch **WAITING** rows
    /// (a queued lock-waiter whose process crashed before the lock was
    /// ever granted — the waiter dies with the process) are reconciled
    /// by the same correlation: they never carry a committed publish, so
    /// they become FAILED (truthful: nothing ran, a resubmission is
    /// safe) instead of stranding a pollable WAITING row forever.
    /// Residual: PENDING rows are left as-is (the stub path's parked
    /// shape; an executor-backed batch task is never durably PENDING),
    /// and per-row persist failures are logged and skipped (retried at
    /// the next bootstrap).
    ///
    /// Returns the number of rows reconciled.
    ///
    /// # Errors
    /// Propagates metadata-store failures enumerating tasks or segments.
    async fn reconcile_stale_running_batch_tasks(&self) -> Result<usize> {
        let live: std::collections::HashSet<String> = {
            let tasks = self.running_tasks.read().await;
            tasks
                .values()
                .filter(|t| !t.state.is_terminal())
                .map(|t| t.id.clone())
                .collect()
        };
        let stale: Vec<TaskRow> = self
            .metadata
            .get_active_tasks()
            .await?
            .into_iter()
            .filter(|t| {
                t.status == TaskState::Running.as_str() || t.status == TaskState::Waiting.as_str()
            })
            .filter(|t| t.task_type == "index" || t.task_type == "index_parallel")
            .filter(|t| !live.contains(&t.id))
            .collect();
        if stale.is_empty() {
            return Ok(0);
        }
        // Correlate against EVERY segment row (used or unused: a later
        // replace retiring the segment does not un-commit the task).
        let mut committed: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut committed_durable: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for seg in self.metadata.get_all_segments().await? {
            // R9-F2: only a segment of BATCH provenance can prove a BATCH
            // task's commit. Streaming (kafka/kinesis) segments ALSO carry
            // a `taskId`, and task/supervisor ids are user-controlled, so
            // a coincidental id match on a streaming row would mark an
            // UNCOMMITTED stale batch task SUCCESS — silently losing its
            // data (the client never resubmits a SUCCESS). Batch rows
            // stamp `kind: "batch"` (legacy pre-marker rows carry no
            // `kind`; streaming rows always stamp theirs); any other kind
            // fails closed. See [`segment_payload_is_batch`].
            if !segment_payload_is_batch(&seg.payload) {
                continue;
            }
            if let Some(tid) = seg
                .payload
                .get("taskId")
                .and_then(serde_json::Value::as_str)
            {
                committed.insert(tid.to_string());
                if seg.payload.get("loadSpec").is_some() {
                    committed_durable.insert(tid.to_string());
                }
            }
        }
        let mut reconciled = 0usize;
        for mut row in stale {
            let verdict = if committed_durable.contains(&row.id) {
                TaskState::Success
            } else {
                TaskState::Failed
            };
            row.status = verdict.as_str().to_string();
            row.worker = None;
            // Keep the payload (the `get_task` restart-fallback view)
            // truthful. Mutated as raw JSON so a payload from an older
            // build never fails the whole reconcile on a deserialize.
            if let Some(obj) = row.payload.as_object_mut() {
                obj.insert(
                    "status".to_string(),
                    serde_json::Value::String(verdict.as_str().to_string()),
                );
                obj.insert(
                    "state".to_string(),
                    serde_json::Value::String(verdict.as_str().to_string()),
                );
                obj.insert("worker".to_string(), serde_json::Value::Null);
                obj.insert("location".to_string(), serde_json::Value::Null);
            }
            match self.metadata.update_task_status(&row).await {
                Ok(()) => {
                    reconciled += 1;
                    tracing::warn!(
                        task_id = %row.id,
                        data_source = %row.data_source,
                        verdict = verdict.as_str(),
                        committed_durable = committed_durable.contains(&row.id),
                        committed_memory_only =
                            committed.contains(&row.id) && !committed_durable.contains(&row.id),
                        "reconciled a stale RUNNING/WAITING batch task row left \
                         by a previous process: SUCCESS when its publish \
                         committed a durable (restart-reloadable) segment, \
                         FAILED otherwise — either verdict is recoverable and \
                         resubmit-safe, unlike the non-terminal lie it replaces"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        task_id = %row.id,
                        error = %e,
                        "failed to persist a reconciled terminal status for a \
                         stale RUNNING/WAITING batch task row; retried at the \
                         next bootstrap"
                    );
                    continue;
                }
            }
            self.release_task_locks(&row.id).await;
        }
        Ok(reconciled)
    }

    /// Parse an optional lock request out of a task spec.
    ///
    /// We look for `spec.spec.ioConfig.intervals` (an array of ISO-8601
    /// interval strings `"start/end"`) and acquire an EXCLUSIVE lock covering
    /// their union at the spec's `priority` (default 0). Absence of intervals
    /// means the task does not need a lock (returns `Ok(None)`).
    fn parse_lock_request(
        &self,
        task_id: &str,
        data_source: &str,
        spec: &serde_json::Value,
    ) -> Result<Option<TaskLock>> {
        let intervals = spec
            .pointer("/spec/ioConfig/intervals")
            .or_else(|| spec.pointer("/ioConfig/intervals"))
            .and_then(|v| v.as_array());
        let Some(intervals) = intervals else {
            return Ok(None);
        };
        if intervals.is_empty() {
            return Ok(None);
        }
        let priority = spec
            .pointer("/spec/tuningConfig/priority")
            .or_else(|| spec.pointer("/priority"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);

        let mut min_start = i64::MAX;
        let mut max_end = i64::MIN;
        for iv in intervals {
            // DD R46: a non-string interval entry was silently skipped, so a spec
            // with `intervals: [123]` acquired NO lock and bypassed conflict
            // prevention / priority preemption / the exclusive-lock guard. Reject
            // any non-string entry rather than fail open.
            let Some(s) = iv.as_str() else {
                return Err(DruidError::Ingestion(format!(
                    "ingestion spec ioConfig.intervals entry is not a string: {iv}"
                )));
            };
            let (start, end) = parse_iso_interval(s)?;
            min_start = min_start.min(start);
            max_end = max_end.max(end);
        }
        // A present, non-empty `intervals` that yields no valid interval is a
        // malformed spec — fail closed rather than treat it as "no lock needed".
        if min_start == i64::MAX || max_end == i64::MIN {
            return Err(DruidError::Ingestion(
                "ingestion spec ioConfig.intervals is present but yielded no valid interval"
                    .to_string(),
            ));
        }
        let interval = Interval::new(min_start, max_end)?;
        Ok(Some(TaskLock {
            id: String::new(),
            task_id: task_id.to_string(),
            data_source: data_source.to_string(),
            interval,
            lock_type: LockType::Exclusive,
            priority,
            revoked: false,
        }))
    }

    // ----- Supervisors -----------------------------------------------------

    /// Await every registered supervisor-lifecycle op (Codex R22/R23) —
    /// including ORPHANS whose caller was cancelled mid-operation — so the
    /// lifecycle operation about to run observes only COMPLETED ops: no
    /// still-running cleanup that could drop a new consumer's rows (R22),
    /// and no in-flight create whose consumer would be registered after a
    /// shutdown already looked (R23). Must be called with the
    /// `supervisor_lifecycle` lock held, immediately after acquiring it, by
    /// ALL THREE lifecycle entry points (create / resume / shutdown).
    /// Completed handles resolve instantly, so this doubles as the
    /// registry's reaper. A `JoinError` (panicked or aborted op) is logged,
    /// not propagated: the op is gone either way, and a failed op already
    /// failed closed for ITS caller.
    ///
    /// Cancellation safety of the DRAIN ITSELF (Codex R24 F1): the drain is
    /// caller-context code, so it can be cancelled at any of its awaits. The
    /// previous `mem::take` emptied the registry BEFORE awaiting, so a
    /// cancel mid-drain detached every still-running op — the next drain saw
    /// an empty Vec and the next lifecycle operation proceeded while an
    /// orphan was still in flight (the R22 serialization escape reintroduced
    /// one level up; a stale LATEST op could even register last and replace
    /// the replaying consumer). Instead the registry guard is held across
    /// the WHOLE drain and each handle is awaited IN PLACE (`&mut` — a
    /// `JoinHandle` is `Unpin`); it is removed from the Vec only right after
    /// its await completes, synchronously (no await point in between), so:
    ///   * a cancel during a handle's await leaves that handle — and every
    ///     later one — still registered (nothing is lost);
    ///   * a completed handle is removed in the same poll that observed the
    ///     completion, so it is never re-polled.
    ///
    /// Holding a `tokio::sync::Mutex` guard across the awaits is deliberate
    /// (it is an async-aware lock). Deadlock-free: op bodies take only the
    /// datasource publish lock, metadata handles, blocking-pool tasks, and
    /// the `kafka_supervisors` lock — never this registry or the lifecycle
    /// lock — and drains only ever run under the `supervisor_lifecycle`
    /// lock, so no two drains contend for the registry either.
    #[cfg(feature = "kafka-io")]
    async fn drain_kafka_lifecycle_ops(&self) {
        let mut ops = self.kafka_lifecycle_ops.lock().await;
        while !ops.is_empty() {
            // In-place await (FIFO): on cancellation the un-completed handle
            // stays in the Vec for the next drain to resume waiting on.
            let result = (&mut ops[0]).await;
            // Completed → remove synchronously, before any other await, so
            // this handle can never be polled again.
            drop(ops.remove(0));
            if let Err(e) = result {
                tracing::warn!(
                    error = %e,
                    "a prior supervisor-lifecycle op panicked or was aborted",
                );
            }
        }
    }

    /// Drain the Kinesis lifecycle-op registry — the mirror of
    /// [`drain_kafka_lifecycle_ops`](Self::drain_kafka_lifecycle_ops),
    /// with the same FIFO in-place-await discipline. Every lifecycle
    /// entry point runs BOTH drains (each feature-gated), so no
    /// lifecycle operation proceeds while any prior op of either
    /// transport is in flight.
    #[cfg(feature = "kinesis-io")]
    async fn drain_kinesis_lifecycle_ops(&self) {
        let mut ops = self.kinesis_lifecycle_ops.lock().await;
        while !ops.is_empty() {
            let result = (&mut ops[0]).await;
            drop(ops.remove(0));
            if let Err(e) = result {
                tracing::warn!(
                    error = %e,
                    "a prior kinesis supervisor-lifecycle op panicked or was aborted",
                );
            }
        }
    }

    /// Enforce the one-supervisor-per-`(dataSource, topic)`-pair invariant
    /// at the PERSISTED layer (Codex R25): refuse to create Kafka
    /// supervisor `spec_id` while a DIFFERENT persisted supervisor id's
    /// latest generation is a Kafka spec claiming the same pair.
    ///
    /// The kafka-io live-handle guards in
    /// [`create_supervisor`](Overlord::create_supervisor) only see consumers
    /// REGISTERED in this process; a default (no-kafka-io) build has no
    /// handles at all, and even a kafka-io build has none right after a
    /// restart (before
    /// [`resume_kafka_supervisors`](Overlord::resume_kafka_supervisors)
    /// runs). Without this check a default build accepted a second id for
    /// an occupied pair, and the later kafka-io resume warn-skipped one of
    /// the two rows (derived id preferred) — the losing supervisor, though
    /// legitimately created, was silently disabled: nobody ever consumed
    /// its records. Ungated so BOTH builds refuse identically at POST time.
    ///
    /// Which persisted rows claim a pair (mirrors what resume would run,
    /// fail-safe on junk):
    /// * tombstoned (shut down, latest generation has no `type`) or
    ///   non-Kafka latest generation → does NOT claim: the pair is free;
    /// * suspended → DOES claim: suspension is a reversible pause (the
    ///   operator intends to resume), so handing its pair to another id
    ///   would turn that later resume into a refusal trap — shut the
    ///   supervisor down to release the pair;
    /// * malformed `suspended` flag → warn + skip, does NOT claim: resume
    ///   also warn-skips such a row (it can never become a consumer), so
    ///   treating it as an owner would let junk legacy data block the pair
    ///   forever with nothing actually consuming;
    /// * unparseable (legacy) Kafka spec → skip: no pair can be derived,
    ///   and resume warn-skips it too.
    ///
    /// `spec_id` itself is excluded — replacing/suspending one's own
    /// registration is the same-id live guard's domain.
    async fn refuse_persisted_kafka_pair_conflict(
        &self,
        spec_id: &str,
        data_source: &str,
        topic: &str,
    ) -> Result<()> {
        let mut ids: Vec<String> = self
            .metadata
            .get_all_supervisors()
            .await?
            .into_iter()
            .map(|row| row.spec_id)
            .collect();
        ids.sort();
        ids.dedup();
        for other_id in ids {
            if other_id == spec_id {
                continue;
            }
            let Some(latest) = self.metadata.get_supervisor(&other_id).await? else {
                continue;
            };
            if !is_kafka_typed(&latest) {
                continue; // tombstone / non-Kafka: claims no pair
            }
            if let Err(e) = kafka_suspended_flag(&latest) {
                tracing::warn!(
                    supervisor_id = %other_id, error = %e,
                    "persisted pair check: skipping a supervisor with a malformed \
                     `suspended` flag — resume can never run it, so it does not \
                     claim its (datasource, topic) pair",
                );
                continue;
            }
            let Some(parsed) = parse_kafka_supervisor_spec(&latest) else {
                continue; // unparseable legacy row: resume warn-skips it too
            };
            if parsed.data_schema.data_source == data_source && parsed.io_config.topic == topic {
                return Err(DruidError::Ingestion(format!(
                    "a persisted Kafka supervisor '{other_id}' already claims datasource \
                     '{data_source}' / topic '{topic}'; shut it down before creating \
                     '{spec_id}' (two supervisors on one pair would ingest every \
                     record twice)"
                )));
            }
        }
        Ok(())
    }

    /// Enforce the one-supervisor-per-`(dataSource, stream)`-pair
    /// invariant at the PERSISTED layer for Kinesis supervisors — the
    /// compat-5 mirror of
    /// [`refuse_persisted_kafka_pair_conflict`](Self::refuse_persisted_kafka_pair_conflict),
    /// with identical claim rules (tombstoned/non-Kinesis latest
    /// generation claims nothing; suspended claims; malformed
    /// `suspended` flag or unparseable spec warn-skips). Ungated so both
    /// builds refuse identically at POST time: a default (no-kinesis-io)
    /// build still persists specs a later kinesis-io resume runs, and
    /// two persisted supervisors on one pair would ingest every record
    /// twice.
    async fn refuse_persisted_kinesis_pair_conflict(
        &self,
        spec_id: &str,
        data_source: &str,
        stream: &str,
    ) -> Result<()> {
        let mut ids: Vec<String> = self
            .metadata
            .get_all_supervisors()
            .await?
            .into_iter()
            .map(|row| row.spec_id)
            .collect();
        ids.sort();
        ids.dedup();
        for other_id in ids {
            if other_id == spec_id {
                continue;
            }
            let Some(latest) = self.metadata.get_supervisor(&other_id).await? else {
                continue;
            };
            if !is_kinesis_typed(&latest) {
                continue; // tombstone / non-Kinesis: claims no pair
            }
            if let Err(e) = kafka_suspended_flag(&latest) {
                tracing::warn!(
                    supervisor_id = %other_id, error = %e,
                    "persisted kinesis pair check: skipping a supervisor with a \
                     malformed `suspended` flag — resume can never run it, so it does \
                     not claim its (datasource, stream) pair",
                );
                continue;
            }
            let Some(parsed) = parse_kinesis_supervisor_spec(&latest) else {
                continue; // unparseable legacy row: resume warn-skips it too
            };
            if parsed.data_schema.data_source == data_source && parsed.io_config.stream == stream {
                return Err(DruidError::Ingestion(format!(
                    "a persisted Kinesis supervisor '{other_id}' already claims \
                     datasource '{data_source}' / stream '{stream}'; shut it down \
                     before creating '{spec_id}' (two supervisors on one pair would \
                     ingest every record twice)"
                )));
            }
        }
        Ok(())
    }

    /// Create (persist) a supervisor spec and return its spec identifier.
    ///
    /// The spec should contain `"id"` at the top level; if absent, a
    /// synthetic identifier is generated.
    pub async fn create_supervisor(&self, spec: serde_json::Value) -> Result<String> {
        // Hold the lifecycle lock across the whole validate→prepare→spawn-op
        // sequence so a concurrent shutdown cannot slip in between the
        // metadata write and the consumer spawn (Codex 2026-07-13). Taken in
        // EVERY build (Codex R27 F4): the default build's persisted-layer
        // pair-uniqueness check is check-then-act across awaits, so two
        // concurrent same-pair creates could otherwise both pass it and
        // both persist.
        let _lifecycle = self.supervisor_lifecycle.lock().await;
        // Wait out every prior lifecycle op — a cancelled caller's orphan
        // must not still be running when this operation's consumer starts
        // publishing (Codex R22/R23).
        #[cfg(feature = "kafka-io")]
        self.drain_kafka_lifecycle_ops().await;
        #[cfg(feature = "kinesis-io")]
        self.drain_kinesis_lifecycle_ops().await;

        // For a Kafka spec with no explicit `id`, Druid derives the id from
        // `dataSchema.dataSource`; use that STABLE id so reposting the same
        // id-less spec hits the live-supervisor guard instead of generating
        // a fresh synthetic id and starting a duplicate consumer (Codex R5).
        //
        // Derive it in ALL builds, not just kafka-io (Codex R11): a default
        // (no-kafka-io) build still PERSISTS supervisor specs, so a fresh
        // synthetic `supervisor_N` per repost would accumulate DISTINCT rows;
        // a later kafka-io build then resumes EACH persisted spec_id as its
        // own consumer (resume dedups by spec_id) and ingests every record
        // multiple times. A stable datasource-derived id makes reposts
        // collapse onto one spec_id regardless of which build persisted them.
        let derived_id = if spec.get("id").and_then(serde_json::Value::as_str).is_none()
            && (is_kafka_typed(&spec) || is_kinesis_typed(&spec))
        {
            // No explicit STRING id (absent OR null/non-string → treat as
            // omitted, Codex R7) → derive the stable id from the datasource.
            datasource_of(&spec)
        } else {
            None
        };

        let spec_id = spec
            .get("id")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or(derived_id)
            .unwrap_or_else(|| {
                let seq = self
                    .task_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                format!("supervisor_{seq}")
            });

        // Supervisor-lifecycle handling for kafka-io builds (Codex 2026-07-13):
        //   * drop a handle whose consumer task has already exited (dead
        //     handles must not block a repost);
        //   * refuse to replace a LIVE consumer with anything other than an
        //     explicit suspend of the SAME Kafka supervisor — including a
        //     non-Kafka spec, which would otherwise orphan the running
        //     consumer against metadata describing a different supervisor;
        //   * validate a Kafka spec BEFORE persisting so an invalid one is
        //     rejected rather than acknowledged as running.
        //   * for a runnable Kafka spec, CREATE + SUBSCRIBE the consumer
        //     BEFORE persisting, so a bad client config fails without leaving
        //     a persisted-but-broken supervisor row (Codex R4).
        #[cfg(feature = "kafka-io")]
        let mut suspend_kafka = false;
        #[cfg(feature = "kafka-io")]
        let prepared: Option<(kafka::PreparedKafkaConsumer, Arc<Historical>)> = {
            let is_kafka = is_kafka_typed(&spec);
            // Coerced Druid-faithfully; a junk flag is a loud reject BEFORE
            // any lifecycle decision or persist (Fable audit).
            let suspended = is_kafka && kafka_suspended_flag(&spec)?;
            // Resolve — under ONE held lock (Codex R33 review) — BOTH the
            // fate of any handle registered under this id AND whether a LIVE
            // consumer blocks this create. Two separate observations let a
            // consumer that finished in between slip a non-suspend
            // replacement past the "already running" guard while its
            // un-harvested replay obligation was silently dropped.
            //
            // A REPLAY-REQUIRED sentinel (Codex R30 F3) is a FINISHED handle
            // left registered with its metadata still ACTIVE, so a topic
            // replay can still rebuild the rows a failed drain lost. Pre-fix
            // an `is_finished()` reap removed it and a non-suspend repost
            // persisted its replacement as the latest generation — the lost
            // rows became permanently unrecoverable (Codex R33). It may be
            // superseded ONLY by a re-create that ACTUALLY replays them (the
            // SAME datasource/topic pair, SAME cluster, EARLIEST offset,
            // UNCHANGED schema — `recoverable_by`), and even then it is LEFT
            // in place so only the recovering op's final `insert` overwrites
            // it ON SUCCESS (reaping here would reopen the hole if a later
            // step — prepare/cleanup/persist — or a caller cancellation
            // aborted the recovery). Anything else (non-Kafka, a different
            // pair, a re-point at different brokers, a LATEST tail re-create,
            // or a schema change) is a loud refuse.
            //
            // On a SUSPEND the handle is deliberately left untouched here: the
            // suspend op below drives it through `shutdown()` to observe any
            // replay obligation (Codex R30 F3), and `live` is irrelevant on
            // that branch (`live && !suspended`).
            let live = if suspended {
                false
            } else {
                let mut sups = self.kafka_supervisors.lock().await;
                // Observe liveness EXACTLY ONCE (Codex R33 review): the
                // registry lock does not freeze the consumer TASK, so
                // `is_finished()` can flip running→finished between two reads —
                // reading it twice let a consumer that finished in between be
                // misclassified as a reapable clean handle, dropping its replay
                // obligation. `Some(true)` = finished, `Some(false)` = running,
                // `None` = no handle. `finished` is monotonic (a finished task
                // never un-finishes), so this one capture is authoritative.
                let finished = sups
                    .get(&spec_id)
                    .map(kafka::KafkaSupervisorHandle::is_finished);
                // Classify from that single observation. A running consumer
                // blocks a non-suspend replacement; a finished handle is
                // HARVESTED (below) then treated as a sentinel or reaped; no
                // handle / a recoverable sentinel left in place / a reaped
                // clean handle all leave this create free to proceed. `?` on
                // an invalid Kafka spec surfaces the validation error and
                // leaves the sentinel in place.
                enum Registered {
                    /// No blocker: absent, a recoverable sentinel LEFT in
                    /// place, or a clean handle to reap.
                    Free {
                        /// Whether a clean, stale finished handle must be
                        /// reaped before proceeding.
                        reap: bool,
                    },
                    /// A RUNNING consumer blocks a non-suspend replacement.
                    Live,
                    /// A replay-required sentinel this repost cannot recover.
                    Refuse,
                }
                let state = match finished {
                    None => Registered::Free { reap: false },
                    // Observed RUNNING — never drained here, always blocks (even
                    // if it finishes 1ns later, refusing is the safe outcome).
                    Some(false) => Registered::Live,
                    Some(true) => {
                        // HARVEST an undrained fatal/panic outcome so it is not
                        // mistaken for a clean stale handle: a consumer that
                        // self-terminated carries its replay obligation only in
                        // the un-joined task's stats (Codex R33 review). The
                        // task is already finished, so the join is instant.
                        if let Some(handle) = sups.get_mut(&spec_id)
                            && !handle.replay_required()
                        {
                            let _ = handle.shutdown().await;
                        }
                        // Re-read only the (now harvested) obligation — NOT
                        // liveness — to classify the finished handle.
                        match sups.get(&spec_id) {
                            Some(h) if h.replay_required() => {
                                if is_kafka && h.recoverable_by(&validate_kafka_spec(&spec)?) {
                                    // Recoverable: leave the sentinel; the op
                                    // below replays and overwrites it on
                                    // success. FG-6 caveat (unchanged by R33):
                                    // "supersede" means the earliest consumer
                                    // STARTS replaying, not that the replay has
                                    // COMPLETED — with in-memory segments a
                                    // durable replay-to-completion guarantee is
                                    // FG-7 (deep storage). R33 only narrows
                                    // WHICH reposts may supersede it.
                                    Registered::Free { reap: false }
                                } else {
                                    Registered::Refuse
                                }
                            }
                            // Finished, harvested, still clean → stale, reap it.
                            _ => Registered::Free { reap: true },
                        }
                    }
                };
                match state {
                    Registered::Live => true,
                    Registered::Free { reap } => {
                        if reap {
                            sups.remove(&spec_id);
                        }
                        false
                    }
                    Registered::Refuse => {
                        return Err(DruidError::Ingestion(format!(
                            "supervisor '{spec_id}' is in an UNRECOVERED replay-required state: \
                             its previous Kafka consumer stopped with buffered rows that never \
                             became queryable, and its metadata is still ACTIVE so a topic \
                             replay can still rebuild them. This POST would replace the \
                             supervisor WITHOUT replaying those rows, losing them permanently. \
                             To RECOVER, repost the SAME Kafka spec — same datasource + topic, \
                             same bootstrap.servers, useEarliestOffset=true, unchanged \
                             ingestion schema — so an earliest replay (or a restart/resume of \
                             that spec) rebuilds the lost rows. Until then every \
                             shutdown/suspend also keeps refusing, so the pair stays protected"
                        )));
                    }
                }
            };

            // Any non-suspend replacement of a live consumer is refused (no
            // durable offsets → a fresh consumer would re-ingest from the
            // start and duplicate rows; a non-Kafka replacement would orphan
            // the consumer entirely). Shut it down first.
            if live && !suspended {
                return Err(DruidError::Ingestion(format!(
                    "supervisor '{spec_id}' is already running a Kafka consumer; \
                     shut it down before reposting or replacing it"
                )));
            }

            if is_kafka {
                let parsed = validate_kafka_spec(&spec)?;
                if suspended {
                    // Even a suspended registration claims its (datasource,
                    // topic) pair, so it must not collide with another id's
                    // persisted claim (Codex R25; see
                    // `refuse_persisted_kafka_pair_conflict`).
                    self.refuse_persisted_kafka_pair_conflict(
                        &spec_id,
                        &parsed.data_schema.data_source,
                        &parsed.io_config.topic,
                    )
                    .await?;
                    suspend_kafka = true;
                    None
                } else if let Some(historical) = self.historical.clone() {
                    // Cross-id duplicate-consumer guard (Fable audit): a
                    // running consumer for the SAME (datasource, topic) under
                    // a DIFFERENT spec_id — e.g. a legacy synthetic
                    // `supervisor_N` row spawned at resume — would ingest
                    // every record TWICE. The same-id live-guard above cannot
                    // see it, so refuse by pair here.
                    {
                        let sups = self.kafka_supervisors.lock().await;
                        if let Some(other_id) = sups.iter().find_map(|(k, h)| {
                            (*k != spec_id
                                && !h.is_finished()
                                && h.data_source == parsed.data_schema.data_source
                                && h.topic == parsed.io_config.topic)
                                .then(|| k.clone())
                        }) {
                            return Err(DruidError::Ingestion(format!(
                                "a Kafka consumer for datasource '{}' / topic '{}' is \
                                 already running under supervisor '{other_id}'; shut it \
                                 down before creating '{spec_id}' (two consumers on one \
                                 pair would ingest every record twice)",
                                parsed.data_schema.data_source, parsed.io_config.topic,
                            )));
                        }
                    }
                    // Persisted-layer pair guard (Codex R25), AFTER the live
                    // guard so a live conflict keeps its more specific
                    // "already running" refusal. The live guard alone leaves
                    // a hole right after a restart: no handles are registered
                    // yet (resume has not run), so only the persisted rows
                    // can witness the claim.
                    self.refuse_persisted_kafka_pair_conflict(
                        &spec_id,
                        &parsed.data_schema.data_source,
                        &parsed.io_config.topic,
                    )
                    .await?;
                    // Create the consumer NOW (fail-fast, before persist).
                    Some((kafka::prepare_kafka_consumer(&spec_id, parsed)?, historical))
                } else {
                    // No Historical to publish into → persist only. Still a
                    // registration, so the persisted pair claim applies
                    // (Codex R25).
                    self.refuse_persisted_kafka_pair_conflict(
                        &spec_id,
                        &parsed.data_schema.data_source,
                        &parsed.io_config.topic,
                    )
                    .await?;
                    None
                }
            } else {
                None
            }
        };

        // Validate a Kafka spec in the DEFAULT (no-kafka-io) build too, BEFORE
        // persisting. The kafka-io build validates in the prepare block above;
        // the default build otherwise acknowledged + persisted an invalid spec
        // (missing timestampSpec / dimensionsSpec / bootstrap.servers, …) that a
        // later kafka-io resume then silently skipped, consuming zero records
        // (Codex R14). Suspended/tombstone specs are not Kafka-typed here, so
        // they are unaffected.
        #[cfg(not(feature = "kafka-io"))]
        if is_kafka_typed(&spec) {
            // Same loud junk-`suspended` rejection as the kafka-io build
            // (Fable audit) — build parity on what gets persisted.
            kafka_suspended_flag(&spec)?;
            let parsed = validate_kafka_spec(&spec)?;
            // Persisted-layer (datasource, topic) pair guard (Codex R25):
            // this build has no live handles at all, so the persisted rows
            // are its ONLY witness. Pre-fix it accepted a second id for an
            // occupied pair, and the later kafka-io resume warn-skipped one
            // of the two rows — a legitimately created supervisor silently
            // ingested nothing.
            self.refuse_persisted_kafka_pair_conflict(
                &spec_id,
                &parsed.data_schema.data_source,
                &parsed.io_config.topic,
            )
            .await?;
            // Documented divergence (FG-9): librdkafka property VALUES cannot
            // be validated without kafka-io. The kafka-io build fail-fasts a
            // bad `consumerProperties` value at POST; this build persists it
            // and a later kafka-io resume will warn-skip the supervisor at
            // every startup. Make that visible at create time.
            tracing::warn!(
                supervisor_id = %spec_id,
                "kafka spec accepted WITHOUT librdkafka property-value validation \
                 (default build): a bad consumerProperties value will only surface \
                 at kafka-io startup (FG-9)",
            );
        }

        // Kinesis supervisor-lifecycle handling (compat-5), mirroring the
        // Kafka block above: same-id live-consumer guard (checked for ANY
        // spec type, so a cross-type repost cannot orphan a running Kinesis
        // consumer), replay-required sentinel harvesting, validate-before-
        // persist, and the duplicate-(datasource, stream)-pair guards.
        #[cfg(feature = "kinesis-io")]
        let mut suspend_kinesis = false;
        #[cfg(feature = "kinesis-io")]
        let kinesis_prepared: Option<(
            ferrodruid_ingest_kinesis::KinesisSupervisorSpec,
            Arc<Historical>,
        )> = {
            let is_kinesis = is_kinesis_typed(&spec);
            // Same Druid-faithful coercion as the kafka path; a junk flag is
            // a loud reject BEFORE any lifecycle decision or persist.
            let suspended = is_kinesis && kafka_suspended_flag(&spec)?;
            // Resolve — under ONE held registry lock — both the fate of any
            // handle registered under this id and whether a LIVE consumer
            // blocks this create (the kafka R33 single-observation
            // discipline: `is_finished` is monotonic, so one capture is
            // authoritative).
            let live = if suspended {
                false
            } else {
                let mut sups = self.kinesis_supervisors.lock().await;
                let finished = sups
                    .get(&spec_id)
                    .map(kinesis::KinesisSupervisorHandle::is_finished);
                match finished {
                    None => false,
                    // Observed RUNNING — always blocks a non-suspend
                    // replacement (even if it finishes 1ns later, refusing
                    // is the safe outcome).
                    Some(false) => true,
                    Some(true) => {
                        // HARVEST an undrained fatal/panic outcome so it is
                        // not mistaken for a clean stale handle (the task is
                        // already finished, so the join is instant).
                        if let Some(handle) = sups.get_mut(&spec_id)
                            && !handle.replay_required()
                        {
                            let _ = handle.shutdown().await;
                        }
                        match sups.get(&spec_id) {
                            Some(h) if h.replay_required() => {
                                // A same-pair Kinesis re-create RECOVERS the
                                // sentinel inherently: its consumer resumes
                                // from the durable frontier, which never
                                // advanced past the lost rows, so they are
                                // re-consumed regardless of the spec's start
                                // position (no committed offset exists that
                                // could skip them — simpler than kafka).
                                // Leave the sentinel in place; the op below
                                // overwrites it on success. Anything else is
                                // a loud refuse.
                                if is_kinesis && h.recoverable_by(&validate_kinesis_spec(&spec)?) {
                                    false
                                } else {
                                    return Err(DruidError::Ingestion(format!(
                                        "supervisor '{spec_id}' is in an UNRECOVERED \
                                         replay-required state: its previous Kinesis \
                                         consumer stopped with buffered rows that never \
                                         became queryable, and its metadata is still \
                                         ACTIVE so a stream re-consume can rebuild them \
                                         (the durable sequence frontier never advanced \
                                         past them). This POST would replace the \
                                         supervisor WITHOUT re-consuming those rows. To \
                                         RECOVER, repost a Kinesis spec for the SAME \
                                         datasource + stream (its consumer resumes from \
                                         the durable frontier and re-consumes the lost \
                                         span)"
                                    )));
                                }
                            }
                            // Finished, harvested, still clean → stale; reap.
                            _ => {
                                sups.remove(&spec_id);
                                false
                            }
                        }
                    }
                }
            };
            // Any non-suspend replacement of a live Kinesis consumer is
            // refused (a fresh same-pair consumer would double-consume; a
            // cross-type replacement would orphan the running consumer).
            if live && !suspended {
                return Err(DruidError::Ingestion(format!(
                    "supervisor '{spec_id}' is already running a Kinesis consumer; \
                     shut it down before reposting or replacing it"
                )));
            }
            if is_kinesis {
                let parsed = validate_kinesis_spec(&spec)?;
                if suspended {
                    // Even a suspended registration claims its pair.
                    self.refuse_persisted_kinesis_pair_conflict(
                        &spec_id,
                        &parsed.data_schema.data_source,
                        &parsed.io_config.stream,
                    )
                    .await?;
                    suspend_kinesis = true;
                    None
                } else if let Some(historical) = self.historical.clone() {
                    // Cross-id duplicate-consumer guard: a running consumer
                    // for the SAME (datasource, stream) under a DIFFERENT
                    // spec_id would ingest every record twice.
                    {
                        let sups = self.kinesis_supervisors.lock().await;
                        if let Some(other_id) = sups.iter().find_map(|(k, h)| {
                            (*k != spec_id
                                && !h.is_finished()
                                && h.data_source == parsed.data_schema.data_source
                                && h.stream == parsed.io_config.stream)
                                .then(|| k.clone())
                        }) {
                            return Err(DruidError::Ingestion(format!(
                                "a Kinesis consumer for datasource '{}' / stream '{}' \
                                 is already running under supervisor '{other_id}'; \
                                 shut it down before creating '{spec_id}' (two \
                                 consumers on one pair would ingest every record \
                                 twice)",
                                parsed.data_schema.data_source, parsed.io_config.stream,
                            )));
                        }
                    }
                    // Persisted-layer pair guard, AFTER the live guard so a
                    // live conflict keeps its more specific refusal.
                    self.refuse_persisted_kinesis_pair_conflict(
                        &spec_id,
                        &parsed.data_schema.data_source,
                        &parsed.io_config.stream,
                    )
                    .await?;
                    Some((parsed, historical))
                } else {
                    // No Historical to publish into → persist only; still a
                    // registration, so the persisted pair claim applies.
                    self.refuse_persisted_kinesis_pair_conflict(
                        &spec_id,
                        &parsed.data_schema.data_source,
                        &parsed.io_config.stream,
                    )
                    .await?;
                    None
                }
            } else {
                None
            }
        };

        // Validate a Kinesis spec in the DEFAULT (no-kinesis-io) build too,
        // BEFORE persisting (build parity with the kafka Codex R14 posture:
        // an invalid spec must be rejected, not acknowledged + persisted for
        // a later kinesis-io resume to silently skip). The accepted spec is
        // persisted but NO consumer runs — warn loudly instead of silently
        // no-opping.
        #[cfg(not(feature = "kinesis-io"))]
        if is_kinesis_typed(&spec) {
            kafka_suspended_flag(&spec)?;
            let parsed = validate_kinesis_spec(&spec)?;
            self.refuse_persisted_kinesis_pair_conflict(
                &spec_id,
                &parsed.data_schema.data_source,
                &parsed.io_config.stream,
            )
            .await?;
            tracing::warn!(
                supervisor_id = %spec_id,
                stream = %parsed.io_config.stream,
                "kinesis spec accepted and persisted, but this build was compiled \
                 WITHOUT the `kinesis-io` feature: NO consumer will run and NO \
                 records will be ingested until a kinesis-io build resumes this \
                 supervisor",
            );
        }

        // Runnable Kafka path: the ENTIRE remaining tail — (earliest-replay)
        // cleanup → spec persist → consumer start → registration — runs
        // inside ONE uncancellable spawned lifecycle op (Codex R23). With
        // only the cleanup spawned (the R20/R22 design), a caller cancelled
        // at the await boundary AFTER the cleanup completed but BEFORE the
        // consumer start left the prior rows deleted with nothing bound to
        // replay them: a follow-up LATEST create for the same (datasource,
        // topic, bootstrap) drained the finished cleanup and consumed from
        // the topic tail — the deleted rows were rebuilt by nobody (silent
        // permanent loss). Spawning the whole tail removes that boundary by
        // construction: the cleanup and the consumer start are indivisible.
        //
        // Caller-cancellation semantics: an HTTP client that disconnected
        // sees no response, but the operation still COMPLETES in the
        // background (spec persisted, consumer registered) — a retried POST
        // is then refused by the live-consumer guard, which is the correct
        // signal that the create already took effect.
        //
        // Intermediate failures are fail-closed through the oneshot: a
        // cleanup failure persists nothing and starts nothing; a persist
        // failure after the cleanup starts NO consumer and leaves no spec
        // row — the prior rows are then gone until the next EARLIEST
        // create/restart replays the topic and rebuilds them (the FG-6
        // Kafka-is-the-durable-log posture; a latest create would not
        // rebuild them, exactly as documented for every earliest drop).
        #[cfg(feature = "kafka-io")]
        if let Some((prepared, historical)) = prepared {
            let metadata = Arc::clone(&self.metadata);
            let deep_storage = self.deep_storage.clone();
            let supervisors = Arc::clone(&self.kafka_supervisors);
            let op_spec_id = spec_id.clone();
            kafka::run_lifecycle_op(
                &self.kafka_lifecycle_ops,
                "the supervisor-create lifecycle op",
                async move {
                    // Resolve the source cluster's IDENTITY first (Codex R24
                    // F2), before anything destructive: the broker-side
                    // cluster id keys both the pre-start drop's provenance
                    // match and the provenance stamped on every segment this
                    // consumer publishes. `None` = unknown identity, handled
                    // fail-safe by the drop's match rules.
                    let (prepared, cluster_id) = kafka::resolve_cluster_id(prepared).await?;
                    // Topic GENERATION identity (Codex R7 H1): the KIP-516
                    // topic id, resolved best-effort from EVERY bootstrap
                    // broker (tri-state, Codex R28 H1): Agreed = definite id;
                    // Disagreed = recreation-SUSPECTED (the resume floors,
                    // never skips); Unresolved = fail-safe cluster-gating
                    // fallback.
                    let topic_probe = kafka::resolve_topic_id(&prepared).await;
                    // Durable resume FRONTIER (compat-3 stage 2): the
                    // per-partition offset to resume PAST, derived from the
                    // durable segment set's `payload.kafkaOffsets`. Seeking the
                    // consumer past it (in `start_prepared`) means
                    // already-persisted records — reloaded from deep storage at
                    // bootstrap — are neither replayed nor double-counted.
                    // Empty for a genuinely new pair (first start →
                    // `useEarliestOffset` / committed offsets govern, as Druid).
                    // ALL rows are fetched (Codex R18 C1+C2): the numeric
                    // frontier still uses only `used = TRUE` rows with
                    // `kafkaOffsets` (the pure derivation filters them, R3
                    // H5), but the topic-RECREATION guard needs the topicId
                    // evidence of disabled and kafkaOffsets-less rows too —
                    // after a delete+recreate the group's committed offsets
                    // name the DEAD generation's offset space, and resuming
                    // from them would silently skip new-generation records.
                    let resume_frontier = kafka::derive_resume_frontier(
                        &metadata.get_all_segments().await?,
                        prepared.data_source(),
                        prepared.topic(),
                        cluster_id.as_deref(),
                        &topic_probe,
                        |id| historical.has_segment(id),
                    );
                    // Earliest path: `earliest_replay_cleanup` now DROPS only
                    // LEGACY (non-durable, pre-stage-1) prior segments and
                    // replays them; DURABLE segments are KEPT (resumed past the
                    // frontier, never dropped — a drop with no replay to
                    // rebuild them would be pure loss). It still runs the R26 F1
                    // schema-change refusal + the readiness probes. Latest: keep
                    // all prior segments (a tail resume never redelivers them).
                    let prepared = if prepared.use_earliest_offset() {
                        kafka::earliest_replay_cleanup(
                            prepared,
                            &metadata,
                            &historical,
                            cluster_id.as_deref(),
                        )
                        .await?
                    } else {
                        tracing::info!(
                            supervisor_id = %op_spec_id,
                            data_source = prepared.data_source(),
                            topic = prepared.topic(),
                            "useEarliestOffset=false (latest): keeping existing streaming \
                             segments; the consumer resumes past the durable frontier",
                        );
                        prepared
                    };
                    metadata.insert_supervisor(&op_spec_id, &spec).await?;
                    let handle = kafka::start_prepared(
                        prepared,
                        Arc::clone(&metadata),
                        historical,
                        deep_storage,
                        cluster_id,
                        topic_probe.into_agreed(),
                        resume_frontier,
                    );
                    supervisors.lock().await.insert(op_spec_id, handle);
                    Ok(())
                },
            )
            .await?;
            return Ok(spec_id);
        }

        // An explicit suspend of a (possibly) RUNNING consumer is a
        // deregister → stop(drain) → persist transition with the same
        // tearing hazard as shutdown (Codex R24 F3): a caller cancelled
        // mid-transition must not leave a GHOST — so the whole transition
        // rides a spawned lifecycle op (the drain above already forced
        // prior in-flight ops to register their consumers, so the remove
        // sees them). A cancelled caller gets no response but the suspend
        // still takes effect — a retried POST is a harmless idempotent
        // re-suspend.
        //
        // Order (Codex R26 F2): stop + DRAIN the consumer FIRST, and only
        // persist the suspended spec if the drain SUCCEEDED. The previous
        // persist-first order recorded the supervisor as suspended even
        // when the final flush of its residual buffer failed — those rows
        // never became queryable, and a suspended supervisor is never
        // replayed, so they were silently lost. On a drain failure nothing
        // is persisted (the ACTIVE spec stays the latest) and the Err is
        // returned: a restart/resume — or an earliest re-create of the
        // pair — replays the topic and rebuilds the rows (FG-6).
        //
        // Deregistration is ON SUCCESS ONLY (Codex R30 F3): a failed drain
        // KEEPS the (now finished) handle in the registry as a
        // replay-required sentinel, so a RETRIED suspend — or a shutdown —
        // re-observes the failure through the same handle and keeps
        // refusing to persist, instead of finding an empty registry and
        // recording the lossy stop as clean. The sentinel is superseded
        // only by a replay path (restart resume / earliest re-create).
        #[cfg(feature = "kafka-io")]
        if suspend_kafka {
            let metadata = Arc::clone(&self.metadata);
            let supervisors = Arc::clone(&self.kafka_supervisors);
            let op_spec_id = spec_id.clone();
            kafka::run_lifecycle_op(
                &self.kafka_lifecycle_ops,
                "the supervisor-suspend lifecycle op",
                async move {
                    {
                        let mut sups = supervisors.lock().await;
                        if let Some(running) = sups.get_mut(&op_spec_id) {
                            running.shutdown().await?;
                            sups.remove(&op_spec_id);
                        }
                    }
                    metadata.insert_supervisor(&op_spec_id, &spec).await?;
                    Ok(())
                },
            )
            .await?;
            return Ok(spec_id);
        }

        // Runnable Kinesis path (compat-5): the remaining tail — source
        // construction → spec persist → consumer start → registration —
        // runs inside ONE uncancellable spawned lifecycle op, exactly like
        // the kafka tail above (R23: a cancelled caller either ran nothing
        // or the full op completes in the background). There is no
        // pre-start destructive cleanup to protect here: Kinesis segments
        // are durable-or-absent and the resume frontier (not a replay
        // drop) reconciles prior rows, so the op is purely restorative.
        #[cfg(feature = "kinesis-io")]
        if let Some((parsed, historical)) = kinesis_prepared {
            let metadata = Arc::clone(&self.metadata);
            let deep_storage = self.deep_storage.clone();
            let supervisors = Arc::clone(&self.kinesis_supervisors);
            let op_spec_id = spec_id.clone();
            run_lifecycle_op(
                &self.kinesis_lifecycle_ops,
                "the kinesis supervisor-create lifecycle op",
                async move {
                    // Build the real AWS-backed source (default credential
                    // chain, spec region, optional endpoint override — e.g.
                    // LocalStack). Client construction performs no stream
                    // I/O; an unreachable endpoint surfaces inside the
                    // consume loop, which retries with backoff (lazy-connect
                    // parity with the kafka consumer).
                    let source = ferrodruid_ingest_kinesis::AwsKinesisSource::connect(
                        &parsed.io_config.region,
                        parsed.io_config.endpoint.as_deref(),
                    )
                    .await;
                    metadata.insert_supervisor(&op_spec_id, &spec).await?;
                    let handle = kinesis::start_kinesis_consumer(
                        &op_spec_id,
                        Box::new(source),
                        &parsed,
                        Arc::clone(&metadata),
                        historical,
                        deep_storage,
                    );
                    supervisors.lock().await.insert(op_spec_id, handle);
                    Ok(())
                },
            )
            .await?;
            return Ok(spec_id);
        }

        // Explicit suspend of a (possibly) running Kinesis consumer: stop +
        // DRAIN first, persist the suspended spec only on drain success, and
        // deregister on success only — the failed handle stays registered as
        // a replay-required sentinel so a retried suspend keeps refusing
        // (the kafka R26 F2 / R30 F3 ordering, mirrored).
        #[cfg(feature = "kinesis-io")]
        if suspend_kinesis {
            let metadata = Arc::clone(&self.metadata);
            let supervisors = Arc::clone(&self.kinesis_supervisors);
            let op_spec_id = spec_id.clone();
            run_lifecycle_op(
                &self.kinesis_lifecycle_ops,
                "the kinesis supervisor-suspend lifecycle op",
                async move {
                    {
                        let mut sups = supervisors.lock().await;
                        if let Some(running) = sups.get_mut(&op_spec_id) {
                            running.shutdown().await?;
                            sups.remove(&op_spec_id);
                        }
                    }
                    metadata.insert_supervisor(&op_spec_id, &spec).await?;
                    Ok(())
                },
            )
            .await?;
            return Ok(spec_id);
        }

        // Non-runnable paths (non-Kafka/non-Kinesis / no Historical / any
        // spec in the default build): nothing destructive happens and no
        // consumer can be running, so the persist stays in the caller's
        // context.
        self.metadata.insert_supervisor(&spec_id, &spec).await?;

        Ok(spec_id)
    }

    /// Get the latest supervisor spec by identifier.
    pub async fn get_supervisor(&self, id: &str) -> Result<Option<serde_json::Value>> {
        self.metadata.get_supervisor(id).await
    }

    /// Return all persisted supervisor rows.
    pub async fn get_all_supervisors(&self) -> Result<Vec<SupervisorRow>> {
        self.metadata.get_all_supervisors().await
    }

    /// Shut down a supervisor by removing its latest spec from in-memory
    /// tracking.  The persisted row is retained for audit purposes.
    pub async fn shutdown_supervisor(&self, id: &str) -> Result<()> {
        // Serialize against create_supervisor so a shutdown cannot land
        // between a create's persist and spawn (Codex 2026-07-13). Taken in
        // EVERY build (Codex R27 F4) so the default build's persist-side
        // transitions (tombstone vs create's pair check + persist) are
        // serialized under the same lock as the kafka-io build's.
        let _lifecycle = self.supervisor_lifecycle.lock().await;
        // Wait out every in-flight lifecycle op (Codex R23): a cancelled
        // create's spawned op may be about to persist + register the very
        // consumer this shutdown exists to stop — without the drain, the
        // existence check below would say "not found" (nothing persisted
        // yet) and the op would then register a consumer that nothing ever
        // stops, running against a spec row that was never tombstoned.
        #[cfg(feature = "kafka-io")]
        self.drain_kafka_lifecycle_ops().await;
        #[cfg(feature = "kinesis-io")]
        self.drain_kinesis_lifecycle_ops().await;

        // Verify it exists.
        let existing = self.metadata.get_supervisor(id).await?;
        if existing.is_none() {
            return Err(DruidError::Metadata(format!("supervisor not found: {id}")));
        }

        // The tombstone entry indicating shutdown.
        let tombstone = serde_json::json!({
            "id": id,
            "suspended": true,
            "shutdownTime": Utc::now().to_rfc3339(),
        });

        // Deregister, stop(drain) the consumer, and only THEN persist the
        // tombstone, as ONE spawned lifecycle op (Codex R24 F3): in the
        // caller's context a cancel mid-transition left a GHOST — the op
        // survives cancellation, so a cancelled caller gets no response but
        // the shutdown still completes in the background; a retry is a
        // harmless idempotent re-tombstone.
        //
        // Order (Codex R26 F2): the drain runs FIRST and the tombstone is
        // persisted only on drain SUCCESS. The previous persist-first order
        // recorded the supervisor as shut down even when the final flush of
        // its residual buffer failed — those rows never became queryable,
        // and a tombstoned supervisor is never replayed, so they were
        // silently lost. On a drain failure: nothing is persisted (the
        // ACTIVE spec stays the latest — restart/resume replays the topic
        // and rebuilds the rows, FG-6) and the Err surfaces to the
        // operator, who can re-create the pair (earliest) to recover
        // in-process. The crash-shape of the new order is equally safe:
        // dying between the stop and the persist leaves the spec active →
        // replay on restart.
        //
        // Deregistration is ON SUCCESS ONLY (Codex R30 F3): pre-fix the op
        // REMOVED the handle before observing the drain failure, so a
        // RETRIED shutdown found no handle, skipped the drain check, and
        // persisted the tombstone — permanently foreclosing the replay the
        // first refusal existed to protect. The failed handle now stays in
        // the registry as a replay-required sentinel
        // (`KafkaSupervisorHandle::replay_error`): every retry re-observes
        // the cached failure through `shutdown()` and keeps refusing the
        // tombstone until a replay path (restart resume / earliest
        // re-create) supersedes the handle.
        #[cfg(any(feature = "kafka-io", feature = "kinesis-io"))]
        {
            let metadata = Arc::clone(&self.metadata);
            #[cfg(feature = "kafka-io")]
            let kafka_supervisors = Arc::clone(&self.kafka_supervisors);
            #[cfg(feature = "kinesis-io")]
            let kinesis_supervisors = Arc::clone(&self.kinesis_supervisors);
            let op_id = id.to_string();
            // The op is hosted in ONE registry (kafka's when built, else
            // kinesis's); every lifecycle entry point drains BOTH, so the
            // serialization invariant is unaffected by which hosts it.
            #[cfg(feature = "kafka-io")]
            let ops = &self.kafka_lifecycle_ops;
            #[cfg(all(feature = "kinesis-io", not(feature = "kafka-io")))]
            let ops = &self.kinesis_lifecycle_ops;
            return run_lifecycle_op(ops, "the supervisor-shutdown lifecycle op", async move {
                // Stop + drain whichever transport's consumer runs under
                // this id; a drain failure (`?`) refuses the tombstone so
                // the lost rows stay replayable (kafka R26 F2 / R30 F3,
                // shared by the kinesis handle).
                #[cfg(feature = "kafka-io")]
                {
                    let mut sups = kafka_supervisors.lock().await;
                    if let Some(running) = sups.get_mut(&op_id) {
                        running.shutdown().await?;
                        sups.remove(&op_id);
                    }
                }
                #[cfg(feature = "kinesis-io")]
                {
                    let mut sups = kinesis_supervisors.lock().await;
                    if let Some(running) = sups.get_mut(&op_id) {
                        running.shutdown().await?;
                        sups.remove(&op_id);
                    }
                }
                metadata.insert_supervisor(&op_id, &tombstone).await?;
                Ok(())
            })
            .await;
        }

        // Default build: no consumer can be running, so the persist alone
        // completes the shutdown.
        #[cfg(not(any(feature = "kafka-io", feature = "kinesis-io")))]
        {
            self.metadata.insert_supervisor(id, &tombstone).await?;
            Ok(())
        }
    }

    /// Base delay of the background resume-retry loop (Codex R6 H3), doubled
    /// after every failed pass up to [`Self::KAFKA_RESUME_RETRY_CAP`]. Short
    /// in tests.
    #[cfg(all(feature = "kafka-io", not(test)))]
    const KAFKA_RESUME_RETRY_BASE: std::time::Duration = std::time::Duration::from_secs(10);
    #[cfg(all(feature = "kafka-io", test))]
    const KAFKA_RESUME_RETRY_BASE: std::time::Duration = std::time::Duration::from_millis(50);

    /// Ceiling on the resume-retry backoff (Codex R6 H3): the loop keeps
    /// retrying at this cadence until every persisted supervisor either
    /// resumes or stops being a candidate — a broker outage longer than any
    /// fixed retry budget must not permanently starve a persisted
    /// supervisor (the very failure mode the retry exists to remove), so
    /// the loop is deliberately unbounded-in-attempts but loud on every
    /// failed pass. Short in tests.
    #[cfg(all(feature = "kafka-io", not(test)))]
    const KAFKA_RESUME_RETRY_CAP: std::time::Duration = std::time::Duration::from_secs(300);
    #[cfg(all(feature = "kafka-io", test))]
    const KAFKA_RESUME_RETRY_CAP: std::time::Duration = std::time::Duration::from_millis(200);

    /// Resume persisted, non-suspended Kafka supervisors after a restart.
    ///
    /// Because segments are memory-resident only, a restart has lost all
    /// previously ingested data; re-spawning each supervisor re-consumes
    /// from Kafka (per `auto.offset.reset` — `earliest` fully rebuilds the
    /// in-memory segments, `latest` resumes at the tail). Suspended /
    /// tombstoned specs are skipped. Best-effort: a spec that no longer
    /// validates is logged and skipped, not fatal. Returns the number of
    /// consumers started.
    ///
    /// A candidate whose startup LIFECYCLE OP fails (consumer init,
    /// cluster-id resolution task, metadata read, or a fail-close
    /// pre-cleanup probe against a temporarily unreachable broker) is no
    /// longer abandoned until the next process restart (Codex R6 H3 — a
    /// transiently unreachable broker used to permanently starve the
    /// supervisor's partitions, silently losing everything retention
    /// expired in the meantime): a BACKGROUND task retries the resume pass
    /// with capped exponential backoff
    /// ([`Self::KAFKA_RESUME_RETRY_BASE`] → [`Self::KAFKA_RESUME_RETRY_CAP`])
    /// until a pass reports zero remaining failures, warning loudly on
    /// every failed pass. Each retry pass re-reads the persisted specs and
    /// re-runs the SAME idempotent pass as startup (already-running
    /// handles and live `(datasource, topic)` pairs are skipped; specs
    /// tombstoned/suspended in the meantime drop out), and at most one
    /// retry task ever runs per Overlord
    /// ([`spawn_kafka_resume_retry`](Self::spawn_kafka_resume_retry)).
    /// PERMANENT per-spec failures (a spec that no longer validates, a
    /// malformed `suspended` flag) are still warn-skipped without retry —
    /// only an operator can fix those, and re-posting the spec goes through
    /// `create_supervisor`.
    ///
    /// Call once at startup (takes `Arc<Self>` so the background retry can
    /// outlive the call). Requires the `kafka-io` feature and a Historical
    /// to publish into.
    #[cfg(feature = "kafka-io")]
    pub async fn resume_kafka_supervisors(self: Arc<Self>) -> Result<usize> {
        match self.resume_kafka_supervisors_once().await {
            Ok((started, failed)) => {
                if failed > 0 {
                    tracing::warn!(
                        started,
                        failed,
                        "some persisted Kafka supervisors failed their startup lifecycle op; \
                         scheduling a background resume retry so a transiently unreachable \
                         broker cannot permanently starve them (R6 H3)",
                    );
                    Self::spawn_kafka_resume_retry(&self);
                }
                Ok(started)
            }
            // Codex R14 H1: a WHOLE-PASS error (e.g. the metadata enumeration
            // `get_all_supervisors` / `get_supervisor` failing transiently at
            // startup) previously propagated straight out — main.rs merely
            // warns and continues, so NO background retry was scheduled and
            // every persisted supervisor stayed starved until the next process
            // restart, silently losing everything Kafka retention expired in
            // the meantime. Schedule the SAME R6 H3 retry the failed-candidate
            // path uses: its loop treats a whole-pass `Err` like a failed
            // candidate and keeps retrying (capped cadence) until a pass
            // enumerates cleanly, so a transient store/broker outage heals
            // itself. The error is still returned so the startup warn survives;
            // the retry is idempotent (compare-and-swap), so it never
            // double-spawns even if the caller retries.
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "the persisted-Kafka-supervisor resume pass failed WHOLESALE (metadata \
                     enumeration error?); scheduling a background resume retry so a transient \
                     failure at startup cannot permanently starve every supervisor (R14 H1)",
                );
                Self::spawn_kafka_resume_retry(&self);
                Err(e)
            }
        }
    }

    /// One idempotent resume pass (Codex R6 H3 factored this out of
    /// [`resume_kafka_supervisors`](Self::resume_kafka_supervisors) so the
    /// background retry re-runs EXACTLY the startup logic): returns
    /// `(started, failed)` where `failed` counts candidates whose startup
    /// lifecycle op returned an error — the retryable class. Permanent
    /// skips (invalid spec, malformed flag, duplicate pair, already
    /// running) are not failures.
    #[cfg(feature = "kafka-io")]
    async fn resume_kafka_supervisors_once(&self) -> Result<(usize, usize)> {
        let Some(historical) = self.historical.clone() else {
            return Ok((0, 0));
        };
        let _lifecycle = self.supervisor_lifecycle.lock().await;
        // Wait out every prior lifecycle op — a cancelled caller's orphan
        // must not still be running when a resumed consumer starts
        // publishing (Codex R22/R23).
        self.drain_kafka_lifecycle_ops().await;
        #[cfg(feature = "kinesis-io")]
        self.drain_kinesis_lifecycle_ops().await;

        // Distinct supervisor ids (the store may hold spec history per id).
        let mut ids: Vec<String> = self
            .metadata
            .get_all_supervisors()
            .await?
            .into_iter()
            .map(|row| row.spec_id)
            .collect();
        ids.sort();
        ids.dedup();

        // Pass 1: collect the runnable candidates (kafka-typed, not
        // suspended, not already running, spec validates).
        let mut candidates: Vec<(String, ferrodruid_ingest_kafka::KafkaSupervisorSpec)> =
            Vec::new();
        for id in ids {
            let Some(spec) = self.metadata.get_supervisor(&id).await? else {
                continue;
            };
            // Skip non-Kafka, suspended, and tombstoned (no `type`) specs.
            // A malformed `suspended` flag on a persisted (legacy) row is
            // fail-safe: warn + skip, never guess "running" (Fable audit).
            if !is_kafka_typed(&spec) {
                continue;
            }
            match kafka_suspended_flag(&spec) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        supervisor_id = %id, error = %e,
                        "skipping persisted supervisor with a malformed `suspended` flag",
                    );
                    continue;
                }
            }
            {
                let sups = self.kafka_supervisors.lock().await;
                if sups
                    .get(&id)
                    .is_some_and(|h| !kafka::KafkaSupervisorHandle::is_finished(h))
                {
                    continue; // already running
                }
            }
            match validate_kafka_spec(&spec) {
                Ok(parsed) => candidates.push((id, parsed)),
                Err(e) => tracing::warn!(
                    supervisor_id = %id,
                    error = %e,
                    "skipping unresumable persisted supervisor spec",
                ),
            }
        }

        // Pass 2: spawn ONE consumer per (datasource, topic) pair (Fable
        // audit: legacy synthetic `supervisor_N` rows + a datasource-derived
        // row for the same pair would otherwise EACH consume the whole topic
        // and double every stored record). Datasource-DERIVED ids (id ==
        // dataSource, the shape the current binary persists) are preferred
        // over synthetic/legacy ids; within a class the order is
        // deterministic (lexicographic).
        candidates.sort_by(|(a_id, a), (b_id, b)| {
            let a_derived = *a_id == a.data_schema.data_source;
            let b_derived = *b_id == b.data_schema.data_source;
            b_derived.cmp(&a_derived).then_with(|| a_id.cmp(b_id))
        });
        let mut live_pairs: std::collections::HashSet<(String, String)> = {
            let sups = self.kafka_supervisors.lock().await;
            sups.values()
                .filter(|h| !h.is_finished())
                .map(|h| (h.data_source.clone(), h.topic.clone()))
                .collect()
        };

        let mut started = 0usize;
        let mut failed = 0usize;
        for (id, parsed) in candidates {
            let pair = (
                parsed.data_schema.data_source.clone(),
                parsed.io_config.topic.clone(),
            );
            if live_pairs.contains(&pair) {
                tracing::warn!(
                    supervisor_id = %id,
                    data_source = %pair.0,
                    topic = %pair.1,
                    "skipping resume: another supervisor already consumes this \
                     (datasource, topic) pair — a second consumer would ingest \
                     every record twice (legacy data: creating this state anew \
                     is refused at POST time since Codex R25, so this duplicate \
                     row predates that guard; shut it down or delete it)",
                );
                continue;
            }
            let use_earliest = parsed.io_config.use_earliest_offset.unwrap_or(false);
            // Whole per-supervisor tail — consumer init → cluster-identity
            // resolution → (earliest) cleanup → consumer start →
            // registration — in ONE uncancellable spawned lifecycle op, the
            // same mechanism as create_supervisor (Codex R23).
            // Resume is a startup path with far less cancellation exposure
            // than an HTTP handler, but a dropped/timed-out startup future
            // could still tear a completed cleanup apart from its replaying
            // consumer; riding the same registry keeps the invariant
            // single-sourced and lets shutdown's drain cover these ops too.
            // Fail-safe on error (either step): the consumer is never
            // registered and this candidate is warn-skipped, like before.
            let metadata = Arc::clone(&self.metadata);
            let op_historical = Arc::clone(&historical);
            let op_deep_storage = self.deep_storage.clone();
            let supervisors = Arc::clone(&self.kafka_supervisors);
            let op_id = id.clone();
            let result = kafka::run_lifecycle_op(
                &self.kafka_lifecycle_ops,
                "the supervisor-resume lifecycle op",
                async move {
                    // Create + subscribe the consumer, then resolve the
                    // source cluster's IDENTITY (Codex R24 F2) — both before
                    // anything destructive, exactly like the create path.
                    let prepared = kafka::prepare_kafka_consumer(&op_id, parsed)?;
                    let (prepared, cluster_id) = kafka::resolve_cluster_id(prepared).await?;
                    // Topic GENERATION identity (Codex R7 H1): detects a
                    // topic deleted+recreated across the restart — the exact
                    // case where the durable frontier's offsets name records
                    // of a DELETED log and must not be seeked past. Tri-state
                    // (Codex R28 H1): a broker DISAGREEMENT on the id is
                    // recreation-SUSPECTED and floors the resume.
                    let topic_probe = kafka::resolve_topic_id(&prepared).await;
                    // Durable resume (compat-3 stage 2): the routine restart
                    // path. The supervisor's prior DURABLE segments were
                    // reloaded from deep storage at bootstrap and MUST NOT be
                    // dropped (the pre-stage-2 earliest-replay drop of durable
                    // rows would be pure loss now — resuming past the frontier
                    // does not rebuild them). Derive the per-partition resume
                    // frontier from the durable rows' `payload.kafkaOffsets`;
                    // the consumer is seeked past it (in `start_prepared`) so
                    // already-persisted records are neither replayed nor
                    // double-counted against the reloaded segments. ALL rows
                    // are fetched (Codex R18 C1+C2): the numeric frontier
                    // still uses only `used = TRUE` rows with `kafkaOffsets`
                    // (the pure derivation filters them, R3 H5), but the
                    // topic-RECREATION guard needs the topicId evidence of
                    // disabled and kafkaOffsets-less rows too — after a
                    // delete+recreate the group's committed offsets name the
                    // DEAD generation's offset space, and resuming from them
                    // would silently skip new-generation records.
                    let resume_frontier = kafka::derive_resume_frontier(
                        &metadata.get_all_segments().await?,
                        prepared.data_source(),
                        prepared.topic(),
                        cluster_id.as_deref(),
                        &topic_probe,
                        |id| op_historical.has_segment(id),
                    );
                    // Earliest path: `earliest_replay_cleanup` drops only
                    // LEGACY (non-durable) prior segments and replays them
                    // (durable rows are kept), and runs the R26 F1
                    // schema-change refusal + readiness probes. Latest: keep
                    // all prior segments. `useEarliestOffset` now governs only
                    // a FIRST start (empty frontier + no committed offset).
                    let prepared = if use_earliest {
                        kafka::earliest_replay_cleanup(
                            prepared,
                            &metadata,
                            &op_historical,
                            cluster_id.as_deref(),
                        )
                        .await?
                    } else {
                        prepared
                    };
                    let handle = kafka::start_prepared(
                        prepared,
                        Arc::clone(&metadata),
                        op_historical,
                        op_deep_storage,
                        cluster_id,
                        topic_probe.into_agreed(),
                        resume_frontier,
                    );
                    supervisors.lock().await.insert(op_id, handle);
                    Ok(())
                },
            )
            .await;
            match result {
                Ok(()) => {
                    live_pairs.insert(pair);
                    started += 1;
                    tracing::info!(supervisor_id = %id, "resumed Kafka supervisor after restart");
                }
                Err(e) => {
                    // Counted as FAILED (not skipped) so the caller schedules
                    // the background retry (Codex R6 H3): a transient broker
                    // outage at startup must not permanently starve this
                    // supervisor's partitions.
                    failed += 1;
                    tracing::warn!(
                        supervisor_id = %id, error = %e,
                        "resume failed: the supervisor's lifecycle op failed \
                         (consumer init, cluster-id resolution, or prior-segment \
                         drop) — it will be retried in the background (R6 H3)",
                    );
                }
            }
        }
        Ok((started, failed))
    }

    /// Spawn the at-most-one BACKGROUND resume-retry task (Codex R6 H3):
    /// after a capped-exponential-backoff sleep it re-runs
    /// [`resume_kafka_supervisors_once`](Self::resume_kafka_supervisors_once)
    /// until a pass reports zero remaining failures (every candidate either
    /// resumed — records start flowing the moment its consumer registers —
    /// or stopped being a candidate: tombstoned, suspended, or superseded),
    /// then clears the active flag and exits. A whole-pass error (e.g. a
    /// transient metadata-store failure) is retried like a failed
    /// candidate. Idempotent under repeated calls via compare-and-swap on
    /// the `kafka_resume_retry_active` flag: a second caller while a task
    /// is live is a no-op — the live task's next pass re-reads the
    /// persisted specs and covers whatever the second caller saw. The loop
    /// is deliberately UNBOUNDED in attempts (see
    /// [`Self::KAFKA_RESUME_RETRY_CAP`]): any fixed attempt budget would
    /// re-create the permanent-starvation failure mode for outages longer
    /// than the budget; a genuinely permanent lifecycle failure keeps
    /// warning at the capped cadence — loud and bounded — until the
    /// operator fixes or removes the spec.
    #[cfg(feature = "kafka-io")]
    fn spawn_kafka_resume_retry(this: &Arc<Self>) {
        use std::sync::atomic::Ordering;
        if this
            .kafka_resume_retry_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // A retry task is already working through the failures; its next
            // pass re-reads the persisted specs, so nothing is lost.
            return;
        }
        let overlord = Arc::clone(this);
        tokio::spawn(async move {
            let mut delay = Self::KAFKA_RESUME_RETRY_BASE;
            loop {
                tokio::time::sleep(delay).await;
                match overlord.resume_kafka_supervisors_once().await {
                    Ok((started, 0)) => {
                        if started > 0 {
                            tracing::info!(
                                started,
                                "background resume retry recovered previously failed \
                                 Kafka supervisors (R6 H3)",
                            );
                        }
                        // Clear the flag BEFORE exiting the loop: a caller
                        // whose own pass fails concurrently must be able to
                        // CAS a fresh retry task the instant this one is
                        // done deciding — clearing after `break` would let
                        // that caller's failure fall between the exit
                        // decision and the flag store, retried by nobody.
                        // The worst case of this ordering is one extra,
                        // briefly-overlapping task, and every pass is
                        // idempotent.
                        overlord
                            .kafka_resume_retry_active
                            .store(false, Ordering::SeqCst);
                        break;
                    }
                    Ok((started, failed)) => {
                        if started > 0 {
                            tracing::info!(
                                started,
                                "background resume retry recovered some previously \
                                 failed Kafka supervisors (R6 H3)",
                            );
                        }
                        tracing::warn!(
                            failed,
                            retry_in_secs = delay.min(Self::KAFKA_RESUME_RETRY_CAP).as_secs(),
                            "background resume retry: some persisted Kafka supervisors \
                             still fail their startup lifecycle op (retrying — a broker \
                             outage heals here; a permanent config failure needs the \
                             operator to fix or remove the spec)",
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "background resume retry: the resume pass itself failed \
                             (metadata store unavailable?); retrying",
                        );
                    }
                }
                delay = (delay * 2).min(Self::KAFKA_RESUME_RETRY_CAP);
            }
        });
    }

    /// Test-only: whether the supervisor `id` has a LIVE (not finished)
    /// consumer handle registered.
    #[cfg(all(test, feature = "kafka-io"))]
    pub(crate) async fn kafka_supervisor_live_for_tests(&self, id: &str) -> bool {
        self.kafka_supervisors
            .lock()
            .await
            .get(id)
            .is_some_and(|h| !h.is_finished())
    }

    /// Test-only: whether the background resume-retry task is currently
    /// running (Codex R6 H3).
    #[cfg(all(test, feature = "kafka-io"))]
    pub(crate) fn kafka_resume_retry_active_for_tests(&self) -> bool {
        self.kafka_resume_retry_active
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Base delay of the background Kinesis resume-retry loop (mirror of
    /// [`Self::KAFKA_RESUME_RETRY_BASE`]). Short in tests.
    #[cfg(all(feature = "kinesis-io", not(test)))]
    const KINESIS_RESUME_RETRY_BASE: std::time::Duration = std::time::Duration::from_secs(10);
    #[cfg(all(feature = "kinesis-io", test))]
    const KINESIS_RESUME_RETRY_BASE: std::time::Duration = std::time::Duration::from_millis(50);

    /// Ceiling on the Kinesis resume-retry backoff (mirror of
    /// [`Self::KAFKA_RESUME_RETRY_CAP`]): deliberately unbounded in
    /// attempts, loud on every failed pass.
    #[cfg(all(feature = "kinesis-io", not(test)))]
    const KINESIS_RESUME_RETRY_CAP: std::time::Duration = std::time::Duration::from_secs(300);
    #[cfg(all(feature = "kinesis-io", test))]
    const KINESIS_RESUME_RETRY_CAP: std::time::Duration = std::time::Duration::from_millis(200);

    /// Resume persisted, non-suspended Kinesis supervisors after a restart
    /// (compat-5) — the Kinesis mirror of
    /// [`resume_kafka_supervisors`](Self::resume_kafka_supervisors).
    ///
    /// One consumer is spawned per `(datasource, stream)` pair from the
    /// persisted specs; each consumer derives its per-shard resume start
    /// from the DURABLE sequence frontier (the segment rows'
    /// `payload.kinesisSequences`), so already-persisted records —
    /// reloaded from deep storage at bootstrap — are neither replayed nor
    /// double-counted, and everything above the frontier is re-consumed
    /// (zero loss). Callers must run
    /// [`bootstrap_reload_segments`](Self::bootstrap_reload_segments)
    /// FIRST so the frontier's loaded-evidence check sees the reloaded
    /// blobs. A candidate whose startup lifecycle op fails transiently is
    /// retried by an at-most-one background task with capped exponential
    /// backoff (the kafka R6 H3 discipline). Call once at startup; takes
    /// `Arc<Self>` so the background retry can outlive the call. Requires
    /// `kinesis-io` and a Historical to publish into.
    #[cfg(feature = "kinesis-io")]
    pub async fn resume_kinesis_supervisors(self: Arc<Self>) -> Result<usize> {
        match self.resume_kinesis_supervisors_once().await {
            Ok((started, failed)) => {
                if failed > 0 {
                    tracing::warn!(
                        started,
                        failed,
                        "some persisted Kinesis supervisors failed their startup \
                         lifecycle op; scheduling a background resume retry so a \
                         transiently unreachable endpoint cannot permanently starve \
                         them",
                    );
                    Self::spawn_kinesis_resume_retry(&self);
                }
                Ok(started)
            }
            Err(e) => {
                // A WHOLE-PASS error must schedule the retry too (the kafka
                // R14 H1 lesson): main.rs merely warns and continues, so
                // without this every persisted supervisor would stay starved
                // until the next process restart.
                tracing::warn!(
                    error = %e,
                    "the persisted-Kinesis-supervisor resume pass failed WHOLESALE \
                     (metadata enumeration error?); scheduling a background resume \
                     retry",
                );
                Self::spawn_kinesis_resume_retry(&self);
                Err(e)
            }
        }
    }

    /// One idempotent Kinesis resume pass: returns `(started, failed)`
    /// where `failed` counts candidates whose startup lifecycle op
    /// errored (the retryable class); permanent skips (invalid spec,
    /// malformed flag, duplicate pair, already running) are not failures.
    /// The mirror of
    /// [`resume_kafka_supervisors_once`](Self::resume_kafka_supervisors_once).
    #[cfg(feature = "kinesis-io")]
    async fn resume_kinesis_supervisors_once(&self) -> Result<(usize, usize)> {
        let Some(historical) = self.historical.clone() else {
            return Ok((0, 0));
        };
        let _lifecycle = self.supervisor_lifecycle.lock().await;
        #[cfg(feature = "kafka-io")]
        self.drain_kafka_lifecycle_ops().await;
        self.drain_kinesis_lifecycle_ops().await;

        let mut ids: Vec<String> = self
            .metadata
            .get_all_supervisors()
            .await?
            .into_iter()
            .map(|row| row.spec_id)
            .collect();
        ids.sort();
        ids.dedup();

        // Pass 1: runnable candidates (kinesis-typed, not suspended, not
        // already running, spec validates).
        let mut candidates: Vec<(String, ferrodruid_ingest_kinesis::KinesisSupervisorSpec)> =
            Vec::new();
        for id in ids {
            let Some(spec) = self.metadata.get_supervisor(&id).await? else {
                continue;
            };
            if !is_kinesis_typed(&spec) {
                continue;
            }
            match kafka_suspended_flag(&spec) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        supervisor_id = %id, error = %e,
                        "skipping persisted kinesis supervisor with a malformed \
                         `suspended` flag",
                    );
                    continue;
                }
            }
            {
                let sups = self.kinesis_supervisors.lock().await;
                if sups
                    .get(&id)
                    .is_some_and(|h| !kinesis::KinesisSupervisorHandle::is_finished(h))
                {
                    continue; // already running
                }
            }
            match validate_kinesis_spec(&spec) {
                Ok(parsed) => candidates.push((id, parsed)),
                Err(e) => tracing::warn!(
                    supervisor_id = %id,
                    error = %e,
                    "skipping unresumable persisted kinesis supervisor spec",
                ),
            }
        }

        // Pass 2: ONE consumer per (datasource, stream) pair, preferring
        // datasource-derived ids over synthetic/legacy ids (the kafka
        // resume discipline).
        candidates.sort_by(|(a_id, a), (b_id, b)| {
            let a_derived = *a_id == a.data_schema.data_source;
            let b_derived = *b_id == b.data_schema.data_source;
            b_derived.cmp(&a_derived).then_with(|| a_id.cmp(b_id))
        });
        let mut live_pairs: std::collections::HashSet<(String, String)> = {
            let sups = self.kinesis_supervisors.lock().await;
            sups.values()
                .filter(|h| !h.is_finished())
                .map(|h| (h.data_source.clone(), h.stream.clone()))
                .collect()
        };

        let mut started = 0usize;
        let mut failed = 0usize;
        for (id, parsed) in candidates {
            let pair = (
                parsed.data_schema.data_source.clone(),
                parsed.io_config.stream.clone(),
            );
            if live_pairs.contains(&pair) {
                tracing::warn!(
                    supervisor_id = %id,
                    data_source = %pair.0,
                    stream = %pair.1,
                    "skipping kinesis resume: another supervisor already consumes \
                     this (datasource, stream) pair — a second consumer would ingest \
                     every record twice (creating this state anew is refused at POST \
                     time; shut the duplicate down or delete it)",
                );
                continue;
            }
            let metadata = Arc::clone(&self.metadata);
            let op_historical = Arc::clone(&historical);
            let op_deep_storage = self.deep_storage.clone();
            let supervisors = Arc::clone(&self.kinesis_supervisors);
            let op_id = id.clone();
            let result = run_lifecycle_op(
                &self.kinesis_lifecycle_ops,
                "the kinesis supervisor-resume lifecycle op",
                async move {
                    let source = ferrodruid_ingest_kinesis::AwsKinesisSource::connect(
                        &parsed.io_config.region,
                        parsed.io_config.endpoint.as_deref(),
                    )
                    .await;
                    let handle = kinesis::start_kinesis_consumer(
                        &op_id,
                        Box::new(source),
                        &parsed,
                        metadata,
                        op_historical,
                        op_deep_storage,
                    );
                    supervisors.lock().await.insert(op_id, handle);
                    Ok(())
                },
            )
            .await;
            match result {
                Ok(()) => {
                    live_pairs.insert(pair);
                    started += 1;
                    tracing::info!(supervisor_id = %id, "resumed Kinesis supervisor after restart");
                }
                Err(e) => {
                    failed += 1;
                    tracing::warn!(
                        supervisor_id = %id, error = %e,
                        "kinesis resume failed: the supervisor's lifecycle op failed — \
                         it will be retried in the background",
                    );
                }
            }
        }
        Ok((started, failed))
    }

    /// Spawn the at-most-one background Kinesis resume-retry task — the
    /// mirror of [`spawn_kafka_resume_retry`](Self::spawn_kafka_resume_retry),
    /// idempotent via compare-and-swap on
    /// [`kinesis_resume_retry_active`](Self::kinesis_resume_retry_active),
    /// unbounded in attempts, loud on every failed pass.
    #[cfg(feature = "kinesis-io")]
    fn spawn_kinesis_resume_retry(this: &Arc<Self>) {
        use std::sync::atomic::Ordering;
        if this
            .kinesis_resume_retry_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let overlord = Arc::clone(this);
        tokio::spawn(async move {
            let mut delay = Self::KINESIS_RESUME_RETRY_BASE;
            loop {
                tokio::time::sleep(delay).await;
                match overlord.resume_kinesis_supervisors_once().await {
                    Ok((started, 0)) => {
                        if started > 0 {
                            tracing::info!(
                                started,
                                "background resume retry recovered previously failed \
                                 Kinesis supervisors",
                            );
                        }
                        // Clear BEFORE exiting (see the kafka twin for why).
                        overlord
                            .kinesis_resume_retry_active
                            .store(false, Ordering::SeqCst);
                        break;
                    }
                    Ok((started, failed)) => {
                        if started > 0 {
                            tracing::info!(
                                started,
                                "background resume retry recovered some previously \
                                 failed Kinesis supervisors",
                            );
                        }
                        tracing::warn!(
                            failed,
                            retry_in_secs = delay.min(Self::KINESIS_RESUME_RETRY_CAP).as_secs(),
                            "background resume retry: some persisted Kinesis \
                             supervisors still fail their startup lifecycle op \
                             (retrying)",
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "background kinesis resume retry: the resume pass itself \
                             failed (metadata store unavailable?); retrying",
                        );
                    }
                }
                delay = (delay * 2).min(Self::KINESIS_RESUME_RETRY_CAP);
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn setup() -> (Arc<MetadataStore>, Overlord) {
        let store = MetadataStore::new_in_memory().await.expect("create store");
        store.initialize().await.expect("init schema");
        let store = Arc::new(store);
        let overlord = Overlord::new(Arc::clone(&store));
        (store, overlord)
    }

    /// Poll a task to its terminal state under the ASYNC submit contract
    /// (submit returns the id immediately; the ONLY verdict source is
    /// the polled status). Panics if the task never terminates within
    /// the deadline.
    async fn await_task_terminal(overlord: &Overlord, id: &str) -> TaskState {
        for _ in 0..1500 {
            if let Some(info) = overlord.get_task(id).await.expect("poll task")
                && info.state.is_terminal()
            {
                return info.state;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("task {id} never reached a terminal state");
    }

    /// Submit under the async contract and poll to the SUCCESS verdict —
    /// the test-side replacement for the removed synchronous submit
    /// (submit → id → poll status → assert SUCCESS). Returns the id.
    async fn submit_ok(overlord: &Overlord, spec: serde_json::Value) -> String {
        let id = overlord.submit_task(spec).await.expect("submit");
        assert_eq!(
            await_task_terminal(overlord, &id).await,
            TaskState::Success,
            "task {id} must reach SUCCESS"
        );
        id
    }

    /// Submit under the async contract and poll to the FAILED verdict
    /// (execution failures no longer surface through submit — the id is
    /// returned and the polled status carries the failure). Returns the id.
    async fn submit_failed(overlord: &Overlord, spec: serde_json::Value) -> String {
        let id = overlord.submit_task(spec).await.expect("submit");
        assert_eq!(
            await_task_terminal(overlord, &id).await,
            TaskState::Failed,
            "task {id} must reach FAILED"
        );
        id
    }

    /// `dimensionsSpec.dimensions` object entries keep their declared type
    /// through spec parsing — previously they were flattened to bare names,
    /// so a `{"type":"double"}` dimension was silently ingested as a STRING
    /// column and aggregations over it returned 0.
    #[test]
    fn parse_spec_preserves_typed_dimensions() {
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "t",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": [
                        "site_id",
                        {"type": "double", "name": "value"}
                    ]},
                    "metricsSpec": []
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"site_id\":\"a\",\"value\":1.5}"
                    }
                }
            }
        });
        let parsed = parse_index_parallel_spec(&spec)
            .expect("parse ok")
            .expect("supported shape");
        assert_eq!(parsed.dimensions.len(), 2);
        assert_eq!(parsed.dimensions[0].name, "site_id");
        assert_eq!(
            parsed.dimensions[0].dim_type,
            ferrodruid_ingest_batch::DimensionType::String
        );
        assert_eq!(parsed.dimensions[1].name, "value");
        assert_eq!(
            parsed.dimensions[1].dim_type,
            ferrodruid_ingest_batch::DimensionType::Double
        );
    }

    #[tokio::test]
    async fn submit_and_get_task() {
        let (_store, overlord) = setup().await;
        let spec = json!({
            "type": "index_kafka",
            "dataSource": "wiki"
        });
        let id = overlord.submit_task(spec).await.expect("submit");
        assert!(id.contains("index_kafka"));
        assert!(id.contains("wiki"));

        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.data_source, "wiki");
    }

    #[tokio::test]
    async fn filter_by_datasource() {
        let (_store, overlord) = setup().await;
        overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit wiki");
        overlord
            .submit_task(json!({"type": "index", "dataSource": "clicks"}))
            .await
            .expect("submit clicks");
        overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit wiki2");

        let wiki_tasks = overlord.get_tasks_by_datasource("wiki").await;
        assert_eq!(wiki_tasks.len(), 2);

        let clicks_tasks = overlord.get_tasks_by_datasource("clicks").await;
        assert_eq!(clicks_tasks.len(), 1);
    }

    #[tokio::test]
    async fn status_transitions() {
        let (_store, overlord) = setup().await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");

        // Pending -> Running
        overlord
            .update_task_status(&id, TaskStatus::Running)
            .await
            .expect("to running");
        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.status, TaskStatus::Running);

        // Running -> Success
        overlord
            .update_task_status(&id, TaskStatus::Success)
            .await
            .expect("to success");
        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.status, TaskStatus::Success);
    }

    #[tokio::test]
    async fn get_running_tasks_filters() {
        let (_store, overlord) = setup().await;
        let id1 = overlord
            .submit_task(json!({"type": "index", "dataSource": "a"}))
            .await
            .expect("submit");
        let _id2 = overlord
            .submit_task(json!({"type": "index", "dataSource": "b"}))
            .await
            .expect("submit");

        // Initially nothing is Running.
        assert!(overlord.get_running_tasks().await.is_empty());

        overlord
            .update_task_status(&id1, TaskStatus::Running)
            .await
            .expect("to running");

        let running = overlord.get_running_tasks().await;
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].id, id1);
    }

    #[tokio::test]
    async fn update_nonexistent_task_errors() {
        let (_store, overlord) = setup().await;
        let result = overlord
            .update_task_status("no_such_task", TaskStatus::Failed)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_get_shutdown_supervisor() {
        let (_store, overlord) = setup().await;
        // Generic supervisor CRUD test — uses an unknown type so neither
        // the kafka nor the kinesis dispatch (validation / consumer spawn)
        // applies in ANY feature combination. Since compat-5, `"kinesis"`
        // is a RUNNABLE type (validated before persist) and can no longer
        // stand in for "generic persist-only spec" — its behavior is
        // covered by the kinesis-specific tests below and in `kinesis::`.
        let spec = json!({
            "id": "wiki-custom",
            "type": "custom",
            "dataSchema": {"dataSource": "wiki"},
            "ioConfig": {"topic": "wiki-events"}
        });

        let spec_id = overlord.create_supervisor(spec).await.expect("create");
        assert_eq!(spec_id, "wiki-custom");

        let got = overlord
            .get_supervisor("wiki-custom")
            .await
            .expect("get")
            .expect("some");
        assert_eq!(got["type"], "custom");

        // List all.
        let all = overlord.get_all_supervisors().await.expect("all");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].spec_id, "wiki-custom");

        // Shutdown.
        overlord
            .shutdown_supervisor("wiki-custom")
            .await
            .expect("shutdown");

        // After shutdown, the latest spec is the tombstone.
        let got = overlord
            .get_supervisor("wiki-custom")
            .await
            .expect("get")
            .expect("some");
        assert_eq!(got["suspended"], true);
    }

    /// compat-5: a Kinesis spec is VALIDATED before persist in every
    /// build — an invalid one (junk `dataSchema` / missing
    /// `ioConfig.stream`) is rejected, not acknowledged + persisted as an
    /// opaque no-op row.
    #[tokio::test]
    async fn kinesis_spec_is_validated_before_persist() {
        let (_store, overlord) = setup().await;
        // The pre-compat-5 shape (kafka-ish ioConfig, no stream) must now
        // be refused.
        let invalid = json!({
            "id": "bad-kinesis",
            "type": "kinesis",
            "dataSchema": {"dataSource": "wiki"},
            "ioConfig": {"topic": "wiki-events"}
        });
        let err = overlord
            .create_supervisor(invalid)
            .await
            .expect_err("an invalid kinesis spec must be rejected");
        assert!(
            format!("{err}").contains("Kinesis"),
            "error should name the kinesis validation: {err}"
        );
        assert!(
            overlord
                .get_supervisor("bad-kinesis")
                .await
                .expect("get")
                .is_none(),
            "a rejected spec must not be persisted"
        );

        // A junk `suspended` flag is a loud reject (build parity with
        // kafka), never a silent lifecycle guess.
        let junk_flag = json!({
            "id": "junk-kinesis",
            "type": "kinesis",
            "suspended": "maybe",
            "dataSchema": {
                "dataSource": "wiki",
                "timestampSpec": {"column": "ts"},
                "dimensionsSpec": {"dimensions": ["page"]}
            },
            "ioConfig": {"stream": "wiki-stream"}
        });
        assert!(overlord.create_supervisor(junk_flag).await.is_err());

        // A VALID kinesis spec is accepted; this setup has no Historical,
        // so in every build it persists without a consumer.
        let valid = json!({
            "id": "good-kinesis",
            "type": "kinesis",
            "dataSchema": {
                "dataSource": "wiki",
                "timestampSpec": {"column": "ts"},
                "dimensionsSpec": {"dimensions": ["page"]}
            },
            "ioConfig": {"stream": "wiki-stream"}
        });
        let id = overlord.create_supervisor(valid).await.expect("create");
        assert_eq!(id, "good-kinesis");
        assert!(
            overlord
                .get_supervisor("good-kinesis")
                .await
                .expect("get")
                .is_some()
        );
    }

    /// compat-5: an id-less Kinesis spec derives its STABLE id from the
    /// datasource (like kafka — reposting must collapse onto one spec_id,
    /// not accumulate synthetic `supervisor_N` rows a later resume would
    /// run as duplicate consumers), and a second id claiming the same
    /// (datasource, stream) pair is refused at the persisted layer.
    #[tokio::test]
    async fn kinesis_spec_derives_stable_id_and_pair_conflict_is_refused() {
        let (_store, overlord) = setup().await;
        let spec = |id: Option<&str>, ds: &str| {
            let mut v = json!({
                "type": "kinesis",
                "dataSchema": {
                    "dataSource": ds,
                    "timestampSpec": {"column": "ts"},
                    "dimensionsSpec": {"dimensions": ["page"]}
                },
                "ioConfig": {"stream": "clicks-stream"}
            });
            if let Some(id) = id {
                v["id"] = json!(id);
            }
            v
        };
        let id = overlord
            .create_supervisor(spec(None, "clicks"))
            .await
            .expect("create");
        assert_eq!(
            id, "clicks",
            "id-less kinesis spec derives id from dataSource"
        );

        // A DIFFERENT id claiming the same (datasource, stream) pair is
        // refused in every build (two supervisors on one pair would ingest
        // every record twice).
        let err = overlord
            .create_supervisor(spec(Some("other-id"), "clicks"))
            .await
            .expect_err("persisted pair conflict must refuse");
        assert!(format!("{err}").contains("already claims"), "err = {err}");
    }

    #[tokio::test]
    async fn shutdown_nonexistent_supervisor_errors() {
        let (_store, overlord) = setup().await;
        let result = overlord.shutdown_supervisor("no_such").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn submit_index_parallel_executes_and_loads_segment() {
        // Build a real Historical and a metadata store.
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "wiki",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page", "language"]},
                    "metricsSpec": [
                        {"type": "count", "name": "count"},
                        {"type": "longSum", "name": "added", "fieldName": "added"}
                    ]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\",\"language\":\"en\",\"added\":100}\n{\"timestamp\":\"2024-01-01T01:00:00Z\",\"page\":\"Talk\",\"language\":\"en\",\"added\":50}"
                    }
                }
            }
        });

        // Async contract: submit returns the id; the polled status is
        // the verdict.
        let _id = submit_ok(&overlord, spec).await;

        // Historical should now report a loaded segment for `wiki`.
        let segs = historical.loaded_segments();
        assert_eq!(segs.len(), 1, "expected 1 segment loaded, got {segs:?}");
        let seg_id = &segs[0];
        assert_eq!(
            historical.segment_datasource(seg_id).as_deref(),
            Some("wiki"),
        );

        // MetadataStore should have the segment row.
        let rows = metadata.get_used_segments("wiki").await.expect("get rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].data_source, "wiki");
    }

    /// Ship2 H8: task ids are `{type}_{ds}_{seq}` with the seq counter
    /// reset to 1 on every process start, while task rows are DURABLE and
    /// `MetadataStore::insert_task` is an UPSERT on id. Pre-fix, the first
    /// post-restart submission for the same type+datasource regenerated a
    /// pre-restart task's id and silently clobbered its persisted status
    /// row — a client polling the old id (the R3-H1 restart fallback
    /// exists precisely for that) then saw the NEW task's state, concluded
    /// its completed ingestion failed, and resubmitted a duplicate.
    #[tokio::test]
    async fn restart_task_id_never_clobbers_persisted_prior_task_row() {
        let store = Arc::new(MetadataStore::new_in_memory().await.expect("create store"));
        store.initialize().await.expect("init schema");

        // Process 1: submit + complete a task; its SUCCESS row is durable.
        let overlord_a = Overlord::new(Arc::clone(&store));
        let old_id = overlord_a
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit A");
        overlord_a
            .update_task_status(&old_id, TaskStatus::Running)
            .await
            .expect("A running");
        overlord_a
            .update_task_status(&old_id, TaskStatus::Success)
            .await
            .expect("A success");
        drop(overlord_a); // restart: in-memory state gone, counter reset

        // Process 2: same type + datasource is the common resubmit case.
        let overlord_b = Overlord::new(Arc::clone(&store));
        let new_id = overlord_b
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit B");
        assert_ne!(
            new_id, old_id,
            "a post-restart submission must never reuse a persisted task id"
        );

        // The pre-restart task's durable row must be intact: polling the
        // OLD id still reports ITS terminal state, not the new task's.
        let info = overlord_b
            .get_task(&old_id)
            .await
            .expect("get old id")
            .expect("pre-restart task row must survive the new submission");
        assert_eq!(
            info.status,
            TaskStatus::Success,
            "pre-restart task's persisted SUCCESS was clobbered by the new task"
        );
    }

    /// THE ROOT FIX (the R10 class dissolved): batch submit is ASYNC —
    /// `submit_task` returns the task id IMMEDIATELY (Druid's
    /// `POST /druid/indexer/v1/task` contract) and never awaits the
    /// execute+publish tail, so a client that "disconnects" (drops the
    /// submit response) during the SHIELDED publish holds nothing: no
    /// oneshot to tear down, no future whose cancellation could hand the
    /// submitter a failure while the publish commits. The only verdict
    /// source is the POLLED task status, which is the fence-derived
    /// truth.
    ///
    /// RED under the old synchronous contract: submit parked on the
    /// tail's verdict (here: on the paused shielded section), so the
    /// timeout below expired — and the exact R10 scenario (submitter
    /// handed FAILURE, publish commits, client resubmits, permanent
    /// double count) was reachable. GREEN now: the id returns while the
    /// publish is still in flight, the status never shows a lying
    /// terminal state mid-publish, and the polled verdict ends SUCCESS
    /// with the data ingested exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_submitter_during_shielded_publish_polls_success_no_double_count() {
        // File-backed store (4-connection pool): the parked publish tail
        // can hold a connection while the observer polls below run.
        let db_dir = tempfile::tempdir().expect("dbdir");
        let db_path = db_dir.path().join("meta.sqlite");
        let metadata = Arc::new(
            MetadataStore::new_sqlite(db_path.to_str().expect("utf8 path"))
                .await
                .expect("create"),
        );
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // Park the SHIELDED publication section on the test pause hook:
        // the tail signals `test_publish_entered`, then blocks WHILE
        // holding the datasource publish lock — the real in-flight state
        // a disconnect used to race.
        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "r10_root_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "appendToExisting": true
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_r10_root_ds_1";

        // ASYNC CONTRACT: the id must come back while the publish is
        // still parked — the old synchronous submit blocked here forever
        // (until the pause was released) and this timeout expired.
        let id = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            overlord.submit_task(spec),
        )
        .await
        .expect(
            "async submit contract violated: submit_task must return the \
             task id immediately instead of awaiting the publish verdict",
        )
        .expect("submit");
        assert_eq!(id, expected_id);
        // The client now "disconnects": the submit response is dropped.
        // Under the async contract that drop is a no-op by construction —
        // the caller holds no channel, no future, nothing coupled to the
        // tail.
        drop(id);

        // Wait until the shielded section has genuinely STARTED (publish
        // in flight, publish lock held).
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_publish_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "shielded publication section never started");

        // While the publish is in flight NO terminal status may be
        // observable — the old contract's FAILURE-while-committing lie
        // (the resubmit invitation) must be impossible to poll.
        for _ in 0..30 {
            let info = overlord
                .get_task(expected_id)
                .await
                .expect("poll status")
                .expect("record");
            assert!(
                !info.state.is_terminal(),
                "a terminal status ({:?}) was pollable while the shielded \
                 publish was still in flight",
                info.state
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Release the pause: the detached tail runs the publish to its
        // commit and finalizes — with no submitter attached at all.
        drop(pause_guard);

        // The POLLED status is the verdict: SUCCESS, in-memory and
        // durable.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "polled status must report the committed publish"
        );
        let mut durable_status = String::new();
        for _ in 0..400 {
            if let Some(row) = metadata.get_task(expected_id).await.expect("get task") {
                durable_status.clone_from(&row.status);
                if durable_status == "SUCCESS" {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(durable_status, "SUCCESS", "durable row must reach SUCCESS");

        // NO double count: the disconnected client was never handed a
        // failure, so nothing invites a resubmission — and the data
        // landed exactly once, query-visible exactly once.
        let rows = metadata
            .get_used_segments("r10_root_ds")
            .await
            .expect("used segments");
        assert_eq!(rows.len(), 1, "expected exactly one published segment");
        let segs = historical.loaded_segments();
        assert_eq!(segs.len(), 1, "segment must be loaded exactly once");
        assert_eq!(
            historical.segment_datasource(&segs[0]).as_deref(),
            Some("r10_root_ds")
        );
        // Hygiene: registry quiescent, interval locks released.
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence entries may outlive the finalized task"
        );
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks");
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
    }

    /// The submit-scope cancellation guards: a client disconnect that
    /// drops the `submit_task` future BEFORE the tail is registered and
    /// spawned (parked inside the durable RUNNING-row insert or at the
    /// registration lock) must strand NOTHING — under the async contract
    /// the RUNNING row is persisted synchronously before the tail
    /// exists, so an unguarded drop there would leave a durable RUNNING
    /// row that no tail, finalizer, or live-process path ever resolves.
    /// The `SubmitRowGuard` flips it to FAILED (truthful: nothing
    /// executed) and the `SubmitLockGuard` releases the interval locks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_submit_future_before_tail_spawn_leaves_no_stranded_running_row() {
        use std::future::Future;

        // File-backed store (4-connection pool): the suspended submit
        // future can hold a connection at its cancellation point while
        // the observer polls below run.
        let db_dir = tempfile::tempdir().expect("dbdir");
        let db_path = db_dir.path().join("meta.sqlite");
        let metadata = Arc::new(
            MetadataStore::new_sqlite(db_path.to_str().expect("utf8 path"))
                .await
                .expect("create"),
        );
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "cancel_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"]
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_cancel_ds_1";

        // Hand-poll the submit future and DROP it as soon as the durable
        // RUNNING row is visible: the drop then lands with the future
        // parked strictly before the registration block (the row insert's
        // final poll or the `running_tasks` write acquisition) — the
        // exact guarded window. If the future instead completes between
        // observer polls (the registration block ran), the task is live
        // and simply runs to completion — both outcomes are asserted.
        let completed = {
            let fut = overlord.submit_task(spec);
            tokio::pin!(fut);
            let mut outcome = None;
            for _ in 0..20_000 {
                let step =
                    std::future::poll_fn(|cx| std::task::Poll::Ready(fut.as_mut().poll(cx))).await;
                if let std::task::Poll::Ready(result) = step {
                    result.expect("submit ok");
                    outcome = Some(true);
                    break;
                }
                if metadata
                    .get_task(expected_id)
                    .await
                    .expect("poll task row")
                    .is_some()
                {
                    outcome = Some(false);
                    break;
                }
            }
            outcome.expect("RUNNING task row never appeared")
        }; // <- an un-completed submit future is dropped HERE (disconnect)

        if completed {
            // The submit finished before the drop: the tail owns the task
            // and must run it to a truthful terminal state.
            let _ = await_task_terminal(&overlord, expected_id).await;
        } else {
            // The drop landed in the guarded window: NOTHING may stay
            // RUNNING. The durable row must converge to FAILED (the
            // SubmitRowGuard) — never sit RUNNING forever in a live
            // process.
            let mut status = String::new();
            for _ in 0..400 {
                if let Some(row) = metadata.get_task(expected_id).await.expect("get task") {
                    status = row.status;
                    if status == "FAILED" {
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert_eq!(
                status, "FAILED",
                "a cancelled submit stranded its durable task row \
                 non-terminal (RUNNING lie) instead of FAILED"
            );
            // No tail was spawned: nothing executed, nothing published.
            assert!(
                metadata
                    .get_used_segments("cancel_ds")
                    .await
                    .expect("used")
                    .is_empty(),
                "no publish may happen for a task whose submit was \
                 cancelled before its tail was spawned"
            );
            assert!(historical.loaded_segments().is_empty());
        }

        // Either way: interval locks released (SubmitLockGuard or the
        // tail's terminal path) and the registry quiescent.
        let mut locks = Vec::new();
        for _ in 0..400 {
            locks = metadata
                .get_locks_for_task(expected_id)
                .await
                .expect("locks");
            if locks.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence entries may outlive a cancelled submit"
        );
    }

    /// D4 — the SubmitRowGuard torn-insert strand: a pre-spawn submit
    /// cancellation whose RUNNING-row insert is still IN FLIGHT
    /// store-side must NEVER leave a durable RUNNING row that outlives
    /// the FAILED cleanup. Pre-fix the guard's detached FAILED upsert
    /// raced the torn insert — when FAILED landed first and the RUNNING
    /// insert landed after, RUNNING won and the row was stranded
    /// non-terminal (no in-memory record, no tail, no fence) until a
    /// restart. The fix orders the cleanup strictly AFTER the
    /// (uncancellable) insert has truly resolved, so every interleaving
    /// converges to FAILED.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_submit_failed_cleanup_orders_after_torn_running_insert() {
        use std::future::Future;

        // File-backed store (4-connection pool): the parked insert task
        // and the observer polls below can hold connections concurrently.
        let db_dir = tempfile::tempdir().expect("dbdir");
        let db_path = db_dir.path().join("meta.sqlite");
        let metadata = Arc::new(
            MetadataStore::new_sqlite(db_path.to_str().expect("utf8 path"))
                .await
                .expect("create"),
        );
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // Park the RUNNING-row insert BEFORE it reaches the store: the
        // deterministic stand-in for a torn insert that lands late.
        let pause = Arc::clone(&overlord.test_running_insert_pause);
        let pause_guard = pause.lock().await;

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "torn_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"]
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_torn_ds_1";

        // Hand-poll the submit future until its insert task is parked
        // (the submit is then awaiting the join), and DROP it there: the
        // cancellation lands with the RUNNING insert genuinely in flight.
        {
            let fut = overlord.submit_task(spec);
            tokio::pin!(fut);
            let mut parked = false;
            for _ in 0..20_000 {
                let step =
                    std::future::poll_fn(|cx| std::task::Poll::Ready(fut.as_mut().poll(cx))).await;
                assert!(
                    !matches!(step, std::task::Poll::Ready(_)),
                    "submit must not complete while its RUNNING insert is parked"
                );
                if overlord
                    .test_running_insert_entered
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    parked = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            assert!(parked, "the RUNNING-row insert task never started");
        } // <- the submit future (with its cancellation guards) drops HERE

        // Give the (unordered, pre-fix) detached FAILED cleanup every
        // chance to run FIRST — the exact losing interleaving.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        // ... and only then let the torn RUNNING insert land store-side.
        drop(pause_guard);

        // The durable row must CONVERGE to FAILED: the cleanup is ordered
        // after the insert, so RUNNING can never win the race and strand
        // a non-terminal row with no live owner.
        let mut status = String::new();
        for _ in 0..400 {
            if let Some(row) = metadata.get_task(expected_id).await.expect("get task") {
                status = row.status;
                if status == "FAILED" {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            status, "FAILED",
            "a cancelled submit's torn RUNNING insert outlived the FAILED \
             cleanup — the durable row is stranded non-terminal"
        );
        // Stability: FAILED must be the FINAL state (nothing flips it back).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let row = metadata
            .get_task(expected_id)
            .await
            .expect("get task")
            .expect("row must exist");
        assert_eq!(row.status, "FAILED", "the FAILED cleanup must be final");

        // Nothing executed, nothing published, locks released, registry
        // quiescent.
        assert!(
            metadata
                .get_used_segments("torn_ds")
                .await
                .expect("used")
                .is_empty(),
            "no publish may happen for a cancelled submit"
        );
        let mut locks = Vec::new();
        for _ in 0..400 {
            locks = metadata
                .get_locks_for_task(expected_id)
                .await
                .expect("locks");
            if locks.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence entries may outlive a cancelled submit"
        );
    }

    /// Review High on ship2 H9: a detached batch tail must stay VISIBLE
    /// and KILLABLE — the RUNNING record and the tail's abort handle are
    /// registered atomically with the spawn, so a stalled ingestion is
    /// visible in `running_tasks` and `shutdown_task` aborts it,
    /// releases its interval locks, and moves the row to FAILED. Under
    /// the ASYNC submit contract the submitter got its id long ago and
    /// holds nothing; the tail here is stalled at the datasource
    /// publish-lock acquisition (held by the test — a stand-in for any
    /// hung ingestion), i.e. BEFORE the shielded P→M→swap section:
    /// releasing the stall after the shutdown must publish NOTHING (the
    /// abort genuinely killed the tail, it did not merely hide it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_aborts_a_stalled_batch_tail() {
        // File-backed store (4-connection pool), same rationale as the H9
        // test: the stalled tail can hold a connection while the observer
        // polls below run.
        let db_dir = tempfile::tempdir().expect("dbdir");
        let db_path = db_dir.path().join("meta.sqlite");
        let metadata = Arc::new(
            MetadataStore::new_sqlite(db_path.to_str().expect("utf8 path"))
                .await
                .expect("create"),
        );
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // Stall the tail mid-flight: hold the datasource publish lock so
        // the spawned tail blocks at its publish-lock acquisition.
        let publish_lock = metadata.datasource_publish_lock("stall_ds").await;
        let stall_guard = publish_lock.lock().await;

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "stall_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"]
                }
            }
        });

        // ASYNC contract: the id returns immediately even though the
        // tail is stalled behind the held publish lock.
        let expected_id = overlord.submit_task(spec).await.expect("submit");
        assert_eq!(expected_id, "index_parallel_stall_ds_1");
        let expected_id = expected_id.as_str();

        // The stalled tail must be VISIBLE in the in-memory table...
        let running = overlord.get_running_tasks().await;
        assert!(
            running.iter().any(|t| t.id == expected_id),
            "a live disconnected batch tail must be visible in \
             running_tasks (got: {running:?})"
        );
        // ...with its interval lock held (nothing released it yet)...
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks mid-flight");
        assert_eq!(
            locks.len(),
            1,
            "the stalled tail's interval lock must be held mid-flight"
        );

        // ...and shutdown_task must FIND it, abort the tail, release its
        // locks, and move the row to a terminal state.
        overlord
            .shutdown_task(expected_id)
            .await
            .expect("shutdown_task must find the running tail");
        let info = overlord
            .get_task(expected_id)
            .await
            .expect("get")
            .expect("record");
        assert_eq!(
            info.status,
            TaskStatus::Failed,
            "an aborted tail's task must be terminal"
        );
        for _ in 0..400 {
            if metadata
                .get_locks_for_task(expected_id)
                .await
                .expect("locks after shutdown")
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks after shutdown");
        assert!(
            locks.is_empty(),
            "shutdown must release the aborted tail's interval locks: {locks:?}"
        );

        // The abort must actually KILL the stalled tail: releasing the
        // stall must NOT let a zombie tail acquire the publish lock and
        // publish afterwards.
        drop(stall_guard);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            historical.loaded_segments().is_empty(),
            "an aborted tail must not publish after shutdown"
        );
        let used = metadata
            .get_used_segments("stall_ds")
            .await
            .expect("used segments");
        assert!(
            used.is_empty(),
            "an aborted tail must not commit segment metadata after \
             shutdown: {used:?}"
        );
        // No unbounded accumulation: the aborted tail's abort handle must
        // be gone from the registry (removed by shutdown_task and by the
        // tail's own BatchTailGuard drop).
        assert!(
            lock_batch_tails(&overlord.batch_tails).is_empty(),
            "aborted tail must deregister its abort handle"
        );
    }

    /// Append double-count on an abort landing DURING the shielded
    /// publication section (High): `shutdown_task` aborts the outer batch
    /// tail, but the P→M→swap section is a detached nested task that
    /// still runs to its COMMIT. Pre-fix shutdown immediately marked the
    /// task FAILED and released its interval locks — for
    /// `appendToExisting: true` that FAILED status invites a
    /// resubmission of the same input, which appends the same rows a
    /// SECOND time (replace mode overwrites and is idempotent; append
    /// accumulates — a permanent double count). Post-fix shutdown
    /// observes the in-flight publish fence, AWAITS the shielded
    /// outcome (bounded: the section always completes or rolls back),
    /// and reports the committed publish as SUCCESS — so nothing invites
    /// the duplicate append and the locks are only released once the
    /// publish has truly resolved.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_during_shielded_append_publish_must_not_invite_double_count() {
        // File-backed store (4-connection pool), same rationale as the
        // other H9 tests: the paused publish tail can hold a connection
        // while the observer polls below run.
        let db_dir = tempfile::tempdir().expect("dbdir");
        let db_path = db_dir.path().join("meta.sqlite");
        let metadata = Arc::new(
            MetadataStore::new_sqlite(db_path.to_str().expect("utf8 path"))
                .await
                .expect("create"),
        );
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // Park the SHIELDED publication section on the test pause hook:
        // the section signals `test_publish_entered`, then blocks at its
        // entry WHILE holding the datasource publish lock — exactly the
        // "abort lands during the shielded section" window.
        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "append_abort_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "appendToExisting": true
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_append_abort_ds_1";

        // ASYNC contract: the submitter gets its id immediately and
        // holds nothing further — the tail proceeds to the (paused)
        // shielded section on its own.
        let id = overlord.submit_task(spec.clone()).await.expect("submit");
        assert_eq!(id, expected_id);

        // Wait until the shielded section has genuinely STARTED.
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_publish_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "shielded publication section never started");

        // Clone the live tail's abort handle BEFORE the shutdown reaps
        // its registry entry, purely as an observer of the abort.
        let tail_handle = lock_batch_tails(&overlord.batch_tails)
            .tails
            .get(expected_id)
            .cloned()
            .expect("live tail must be registered");

        // Land the shutdown abort DURING the shielded section.
        let shutdown = tokio::spawn({
            let overlord = overlord.clone_handle();
            async move {
                overlord
                    .shutdown_task("index_parallel_append_abort_ds_1")
                    .await
            }
        });
        // Deterministic proof the abort landed while the section was
        // still parked: the aborted OUTER tail's future finishes (its
        // BatchTailGuard drop has run) BEFORE the pause is released —
        // the detached shielded section is still parked inside.
        let mut aborted = false;
        for _ in 0..1000 {
            if tail_handle.is_finished() {
                aborted = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            aborted,
            "the outer tail must be aborted while the shielded section is parked"
        );

        // Release the pause: the DETACHED shielded section now runs to
        // its commit, and the shutdown resolves.
        drop(pause_guard);
        shutdown
            .await
            .expect("join shutdown")
            .expect("shutdown_task");

        // The shielded publish must have committed (exactly once) — poll,
        // because pre-fix the shutdown returns without awaiting it.
        let mut used = Vec::new();
        for _ in 0..1000 {
            used = metadata
                .get_used_segments("append_abort_ds")
                .await
                .expect("used segments");
            if !used.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            used.len(),
            1,
            "the shielded append publish must commit exactly once"
        );

        // THE BUG: a FAILED status here is a lie that invites a
        // resubmission of the already-committed input. Act as that
        // naive-but-correct client FIRST, so the pre-fix failure
        // demonstrates the actual double count.
        let info = overlord
            .get_task(expected_id)
            .await
            .expect("get")
            .expect("record");
        if info.status == TaskStatus::Failed {
            // A client resubmitting its "failed" append input: pre-fix
            // this acquires the released lock and appends the same rows
            // AGAIN. Run it to its terminal state so the double count
            // (if any) is visible to the assertions below.
            let dup = overlord
                .submit_task(spec)
                .await
                .expect("append resubmission after a reported failure");
            let _ = await_task_terminal(&overlord, &dup).await;
        }
        let used = metadata
            .get_used_segments("append_abort_ds")
            .await
            .expect("used segments");
        assert_eq!(
            used.len(),
            1,
            "append double count: the aborted task's shielded publish committed, \
             yet a resubmission (invited by the FAILED status) appended the same \
             input again"
        );
        assert_eq!(
            historical.loaded_segments().len(),
            1,
            "the committed append segment must be query-visible exactly once"
        );
        assert_eq!(
            info.status,
            TaskStatus::Success,
            "shutdown during the shielded section must report the COMMITTED publish"
        );
        // R3 hygiene: nothing accumulates past the resolved shutdown.
        assert!(
            lock_batch_tails(&overlord.batch_tails).is_empty(),
            "no tail/fence entries may outlive the resolved shutdown"
        );
        // Interval-lock hygiene: released once the publish resolved.
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks after shutdown");
        assert!(
            locks.is_empty(),
            "locks must be released once the shutdown resolved: {locks:?}"
        );
    }

    /// Append double-count through the MANUAL worker-loss path (High):
    /// `lose_worker` on a batch task with no retry budget left used to
    /// write FAILED + release the interval locks DIRECTLY, bypassing the
    /// tail/fence protocol `shutdown_task` honors — while the shielded
    /// P→M→swap section was still committing. The durable FAILED invites
    /// a resubmission of the `appendToExisting` input; the original
    /// shielded publication then commits anyway, so the resubmitted
    /// append lands the same rows a SECOND time (a permanent double
    /// count). Post-fix worker loss routes such a task through the SAME
    /// registry-linearized abort + fence-aware finalizer as
    /// `shutdown_task`: no terminal status is pollable mid-publish, the
    /// committed publish ends the task SUCCESS, and the locks are only
    /// released once the publish has truly resolved.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lose_worker_during_shielded_append_publish_must_not_invite_double_count() {
        // File-backed store (4-connection pool), same rationale as the
        // other H9 tests: parked publish tails can hold connections while
        // the observer polls below run.
        let db_dir = tempfile::tempdir().expect("dbdir");
        let db_path = db_dir.path().join("meta.sqlite");
        let metadata = Arc::new(
            MetadataStore::new_sqlite(db_path.to_str().expect("utf8 path"))
                .await
                .expect("create"),
        );
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        // ONE attempt of budget: the lost worker's task is out of
        // retries, so worker loss means a terminal verdict.
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_retry_policy(RetryPolicy {
                max_attempts: 1,
                base_delay_millis: 1,
                max_delay_millis: 1,
            });
        overlord
            .register_worker(Worker {
                host: "w1".into(),
                port: 8100,
                capacity: 1,
            })
            .await;

        // Park the SHIELDED publication section on the test pause hook.
        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "append_lw_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "appendToExisting": true
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_append_lw_ds_1";
        let id = overlord.submit_task(spec.clone()).await.expect("submit");
        assert_eq!(id, expected_id);

        // Wait until the shielded section has genuinely STARTED (publish
        // fence registered, publish lock held).
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_publish_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "shielded publication section never started");

        // Attach the worker's bookkeeping to the RUNNING record — the
        // shape `lose_worker` matches — with the retry budget already
        // consumed, so losing the worker takes the terminal path.
        {
            let mut tasks = overlord.running_tasks.write().await;
            let t = tasks.get_mut(expected_id).expect("task");
            t.worker = Some("w1:8100".to_string());
            t.attempt = 1;
        }

        // Land the worker loss DURING the shielded section. Post-fix it
        // parks on the publish verdict (spawned so the test keeps driving
        // the pause); pre-fix it returned immediately with a durable
        // FAILED already persisted.
        let lose = tokio::spawn({
            let overlord = overlord.clone_handle();
            async move { overlord.lose_worker("w1:8100").await }
        });

        // Bounded observation window: NO terminal status may become
        // pollable while the shielded publish is still parked. Pre-fix
        // the direct FAILED shows up here — act as the naive-but-correct
        // client and resubmit the "failed" append input FIRST, so the
        // pre-fix failure demonstrates the actual double count.
        let mut dup_id = None;
        for _ in 0..30 {
            let info = overlord
                .get_task(expected_id)
                .await
                .expect("poll status")
                .expect("record");
            if info.state == TaskState::Failed {
                let dup = overlord
                    .submit_task(spec.clone())
                    .await
                    .expect("append resubmission after a reported failure");
                dup_id = Some(dup);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Release the pause: every parked shielded section (the original,
        // and pre-fix the invited duplicate) runs to its commit.
        drop(pause_guard);
        let affected = lose.await.expect("join lose_worker").expect("lose_worker");
        assert!(
            affected.iter().any(|t| t == expected_id),
            "the lost worker's task must be reported affected: {affected:?}"
        );
        if let Some(dup) = dup_id {
            let _ = await_task_terminal(&overlord, &dup).await;
        }

        // The committed publish must end the task SUCCESS — never a
        // resubmit-inviting FAILED.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "worker loss during the shielded section must report the COMMITTED publish"
        );
        // Rows once: the append landed exactly one segment.
        let used = metadata
            .get_used_segments("append_lw_ds")
            .await
            .expect("used segments");
        assert_eq!(
            used.len(),
            1,
            "append double count: the lost worker's shielded publish committed, \
             yet a resubmission (invited by the durable FAILED) appended the \
             same input again"
        );
        assert_eq!(
            historical.loaded_segments().len(),
            1,
            "the committed append segment must be query-visible exactly once"
        );
        // Hygiene: registry quiescent, interval locks released.
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence entries may outlive the finalized task"
        );
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks");
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
    }

    /// Append double-count through the MANUAL state-machine path (High):
    /// a direct `transition_task(id, FAILED)` on a RUNNING batch task
    /// used to persist FAILED + release the interval locks DIRECTLY,
    /// bypassing the tail/fence protocol `shutdown_task` honors — while
    /// the shielded publication was still committing. Exactly the same
    /// resubmit-inviting lie as the worker-loss variant above. Post-fix
    /// the manual FAILED routes through the shared registry-linearized
    /// abort + fence-aware finalizer: the committed publish ends the
    /// task SUCCESS (the fence verdict is ground truth — the requested
    /// FAILED is overridden), rows land exactly once, and locks are
    /// released only once the publish has resolved.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transition_to_failed_during_shielded_append_publish_must_not_invite_double_count() {
        // File-backed store (4-connection pool), same rationale as the
        // other H9 tests.
        let db_dir = tempfile::tempdir().expect("dbdir");
        let db_path = db_dir.path().join("meta.sqlite");
        let metadata = Arc::new(
            MetadataStore::new_sqlite(db_path.to_str().expect("utf8 path"))
                .await
                .expect("create"),
        );
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // Park the SHIELDED publication section on the test pause hook.
        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "append_tt_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "appendToExisting": true
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_append_tt_ds_1";
        let id = overlord.submit_task(spec.clone()).await.expect("submit");
        assert_eq!(id, expected_id);

        // Wait until the shielded section has genuinely STARTED.
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_publish_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "shielded publication section never started");

        // Land the manual RUNNING -> FAILED DURING the shielded section.
        // Post-fix it parks on the publish verdict (spawned so the test
        // keeps driving the pause); pre-fix it returned immediately with
        // a durable FAILED already persisted.
        let transition = tokio::spawn({
            let overlord = overlord.clone_handle();
            async move {
                overlord
                    .transition_task("index_parallel_append_tt_ds_1", TaskState::Failed)
                    .await
            }
        });

        // Bounded observation window: NO terminal status may become
        // pollable while the shielded publish is still parked. Pre-fix
        // the direct FAILED shows up here — act as the naive-but-correct
        // client and resubmit the "failed" append input FIRST.
        let mut dup_id = None;
        for _ in 0..30 {
            let info = overlord
                .get_task(expected_id)
                .await
                .expect("poll status")
                .expect("record");
            if info.state == TaskState::Failed {
                let dup = overlord
                    .submit_task(spec.clone())
                    .await
                    .expect("append resubmission after a reported failure");
                dup_id = Some(dup);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Release the pause: every parked shielded section runs to its
        // commit, and the routed transition resolves.
        drop(pause_guard);
        transition
            .await
            .expect("join transition")
            .expect("transition_task");
        if let Some(dup) = dup_id {
            let _ = await_task_terminal(&overlord, &dup).await;
        }

        // The committed publish must end the task SUCCESS — never a
        // resubmit-inviting FAILED.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "manual FAILED during the shielded section must report the COMMITTED publish"
        );
        // Rows once: the append landed exactly one segment.
        let used = metadata
            .get_used_segments("append_tt_ds")
            .await
            .expect("used segments");
        assert_eq!(
            used.len(),
            1,
            "append double count: the manually-failed task's shielded publish \
             committed, yet a resubmission (invited by the durable FAILED) \
             appended the same input again"
        );
        assert_eq!(
            historical.loaded_segments().len(),
            1,
            "the committed append segment must be query-visible exactly once"
        );
        // Hygiene: registry quiescent, interval locks released.
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence entries may outlive the finalized task"
        );
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks");
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
    }

    /// A manual RUNNING -> SUCCESS on a batch task whose execute/publish
    /// tail is live must be REJECTED: nothing has been published yet (the
    /// shielded section is still in flight), so a manual SUCCESS would
    /// durably claim data that may never commit — the batch is silently
    /// LOST when the client trusts the status and moves on. The verdict
    /// belongs to the publish fence; the caller polls the status (or
    /// aborts via `shutdown_task`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transition_to_success_on_live_batch_tail_is_rejected() {
        let db_dir = tempfile::tempdir().expect("dbdir");
        let db_path = db_dir.path().join("meta.sqlite");
        let metadata = Arc::new(
            MetadataStore::new_sqlite(db_path.to_str().expect("utf8 path"))
                .await
                .expect("create"),
        );
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // Park the SHIELDED publication section: the publish is genuinely
        // in flight when the manual SUCCESS lands.
        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "append_ts_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "appendToExisting": true
                }
            }
        });
        let expected_id = "index_parallel_append_ts_ds_1";
        let id = overlord.submit_task(spec).await.expect("submit");
        assert_eq!(id, expected_id);
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_publish_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "shielded publication section never started");

        // The manual SUCCESS must be rejected while the publish is live —
        // a silent-loss claim otherwise.
        let err = overlord
            .transition_task(expected_id, TaskState::Success)
            .await
            .expect_err("manual SUCCESS on a live batch tail must be rejected");
        assert!(
            err.to_string().contains("publish"),
            "rejection must explain the live publish: {err}"
        );
        // The task is untouched: still RUNNING, still finishing on its own.
        let info = overlord
            .get_task(expected_id)
            .await
            .expect("poll")
            .expect("record");
        assert_eq!(info.state, TaskState::Running);

        // Release the pause: the tail commits and reports its own SUCCESS.
        drop(pause_guard);
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success
        );
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence entries may outlive the finalized task"
        );
    }

    /// Concurrent-shutdown bypass of the publish fence (High): the FIRST
    /// `shutdown_task` lands during the shielded P→M→swap section, clones
    /// the publish fence, and parks its finalizer on the verdict — but the
    /// ABORTED outer tail's `BatchTailGuard::drop` used to remove the
    /// fence from the registry along with the abort handle. A SECOND
    /// concurrent `shutdown_task` arriving in that window (tail dropped,
    /// first finalizer not yet landed) found neither tail nor fence, took
    /// the eager path, durably persisted FAILED, and released the interval
    /// locks WHILE the shielded publish was still committing. A client
    /// polling task status then observed a durable FAILED for an
    /// `appendToExisting` input that actually committed — resubmitting it
    /// appends the same rows a SECOND time (a permanent double count)
    /// before the first finalizer overwrites the status back to SUCCESS.
    /// Post-fix the fence's lifetime is the publish finalization's, not
    /// the tail's: it stays discoverable until a truthful terminal status
    /// has been persisted, so the second shutdown also awaits the verdict
    /// and NO terminal status is ever observable while the publish is in
    /// flight.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_shutdowns_during_shielded_publish_must_not_expose_transient_failed() {
        // File-backed store (4-connection pool), same rationale as the
        // other H9 tests: parked tails can hold connections while the
        // observer polls below run.
        let db_dir = tempfile::tempdir().expect("dbdir");
        let db_path = db_dir.path().join("meta.sqlite");
        let metadata = Arc::new(
            MetadataStore::new_sqlite(db_path.to_str().expect("utf8 path"))
                .await
                .expect("create"),
        );
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // Park the SHIELDED publication section on the test pause hook:
        // it signals `test_publish_entered`, then blocks at its entry
        // while holding the datasource publish lock — the real in-flight
        // state.
        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "append_race_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "appendToExisting": true
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_append_race_ds_1";

        // ASYNC contract: the submitter gets its id immediately; the
        // tail proceeds to the (paused) shielded section on its own.
        let id = overlord.submit_task(spec.clone()).await.expect("submit");
        assert_eq!(id, expected_id);

        // Wait until the shielded section has genuinely STARTED.
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_publish_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "shielded publication section never started");

        // Clone the live tail's abort handle BEFORE the first shutdown
        // reaps its registry entry, purely as an observer of the abort.
        let tail_handle = lock_batch_tails(&overlord.batch_tails)
            .tails
            .get(expected_id)
            .cloned()
            .expect("live tail must be registered");

        // FIRST shutdown: aborts the outer tail, clones the fence, and
        // parks its finalizer on the (unresolved) verdict.
        let shutdown_first = tokio::spawn({
            let overlord = overlord.clone_handle();
            async move {
                overlord
                    .shutdown_task("index_parallel_append_race_ds_1")
                    .await
            }
        });

        // Deterministic proof the aborted tail's future has DROPPED (its
        // `BatchTailGuard` has run — the exact instant the pre-fix code
        // lost the fence): the outer tail's task finishes while the
        // detached shielded section is still parked inside.
        let mut aborted = false;
        for _ in 0..1000 {
            if tail_handle.is_finished() {
                aborted = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            aborted,
            "the outer tail must be aborted (guard dropped) while the \
             shielded section is parked"
        );

        // SECOND concurrent shutdown, inside the bug window: the tail's
        // registry entries are gone, the first finalizer is still parked
        // (the pause is held). Pre-fix this found neither tail nor fence
        // and eagerly persisted FAILED mid-publish.
        let shutdown_second = tokio::spawn({
            let overlord = overlord.clone_handle();
            async move {
                overlord
                    .shutdown_task("index_parallel_append_race_ds_1")
                    .await
            }
        });

        // THE BUG WINDOW: while the shielded publish is STILL parked, no
        // durable terminal status may be observable — any terminal
        // verdict now is a lie about a publish that has not resolved.
        // Watch the client-visible status through the window; pre-fix the
        // second shutdown lands FAILED here almost immediately.
        let mut lied_status = None;
        for _ in 0..50 {
            if let Some(info) = overlord.get_task(expected_id).await.expect("get")
                && matches!(info.status, TaskStatus::Failed | TaskStatus::Success)
            {
                lied_status = Some(info.status);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Act as the naive-but-correct client FIRST (so the pre-fix
        // failure demonstrates the actual harm): a durable FAILED on an
        // append task invites resubmitting the same input. The
        // resubmitted tail queues on the datasource publish lock behind
        // the parked publish, so it is joined only after the pause is
        // released.
        let resubmission = if lied_status == Some(TaskStatus::Failed) {
            Some(tokio::spawn({
                let overlord = overlord.clone_handle();
                let spec = spec.clone();
                async move { overlord.submit_task(spec).await }
            }))
        } else {
            None
        };

        // Release the pause: the shielded publish commits, both shutdown
        // finalizers observe the verdict and resolve.
        drop(pause_guard);
        shutdown_first
            .await
            .expect("join first shutdown")
            .expect("first shutdown_task");
        shutdown_second
            .await
            .expect("join second shutdown")
            .expect("second shutdown_task");
        if let Some(handle) = resubmission {
            let dup = handle
                .await
                .expect("join resubmission")
                .expect("append resubmission after a reported failure");
            // Run the invited duplicate to its terminal state so the
            // double count (if any) is visible to the assertions below.
            let _ = await_task_terminal(&overlord, &dup).await;
        }

        // Exactly once: the committed shielded append must be the ONLY
        // publish — pre-fix the invited resubmission appends the same
        // input again.
        let used = metadata
            .get_used_segments("append_race_ds")
            .await
            .expect("used segments");
        assert_eq!(
            used.len(),
            1,
            "concurrent-shutdown double count: the transient FAILED invited a \
             resubmission that appended the same input again"
        );
        assert_eq!(
            historical.loaded_segments().len(),
            1,
            "the committed append segment must be query-visible exactly once"
        );

        // No lie was ever observable while the publish was in flight.
        assert_eq!(
            lied_status, None,
            "a durable terminal status was observable while the shielded \
             publish was still in flight"
        );

        // The committed publish reports SUCCESS (idempotent across both
        // finalizers).
        let info = overlord
            .get_task(expected_id)
            .await
            .expect("get")
            .expect("record");
        assert_eq!(
            info.status,
            TaskStatus::Success,
            "concurrent shutdowns during the shielded section must report \
             the COMMITTED publish"
        );
        // R3 hygiene: no tail/fence entry outlives the resolved shutdowns.
        assert!(
            lock_batch_tails(&overlord.batch_tails).is_empty(),
            "no tail/fence entries may outlive the resolved shutdowns"
        );
        // Interval-lock hygiene: released once the publish resolved.
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks after shutdown");
        assert!(
            locks.is_empty(),
            "locks must be released once the shutdowns resolved: {locks:?}"
        );
    }

    /// Poll the batch-tail registry until it is fully quiescent (no tail,
    /// no fence, no live persist-retry marker) or the deadline expires.
    async fn await_registry_quiescence(overlord: &Overlord) -> bool {
        for _ in 0..500 {
            if lock_batch_tails(&overlord.batch_tails).is_empty() {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        false
    }

    /// THE fence leak (High): a COMMITTED publish whose terminal-status
    /// persist FAILS used to retain its resolved fence INDEFINITELY —
    /// the outer tail exits, no background finalizer existed, and only a
    /// future explicit `shutdown_task` would remove it. During a
    /// transient metadata-store failure window this leaked one fence per
    /// occurrence: unbounded registry growth, violating the R3
    /// no-accumulation invariant. Post-fix the tail finalizes in bounded
    /// time: truthful SUCCESS in-memory (never a resubmit-inviting
    /// FAILED — the R4/R5 guarantee), fence retired atomically with it,
    /// durable row flushed by the bounded background retry once the
    /// store recovers, and the submit call reports the committed
    /// ingestion as the success it is.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn committed_publish_with_failed_terminal_persist_retires_fence_and_reports_success() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // Transient store outage at exactly the terminal persist: the
        // tail's own persist AND the finalizer's immediate re-attempt
        // fail; the FIRST bounded background retry then succeeds (the
        // store "recovers").
        overlord
            .inject_terminal_persist_failures
            .store(2, std::sync::atomic::Ordering::SeqCst);

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "fence_leak_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "appendToExisting": true
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_fence_leak_ds_1";

        let id = overlord.submit_task(spec.clone()).await.expect("submit");
        assert_eq!(id, expected_id);

        // NO FENCE LEAK AT QUIESCENCE (the High): the committed-but-
        // unpersisted fence must be retired by the tail's own bounded
        // finalization — NOT parked until some later explicit shutdown.
        assert!(
            await_registry_quiescence(&overlord).await,
            "fence leak: a committed publish whose terminal persist failed \
             left its fence (or retry marker) in the registry at quiescence"
        );

        // The committed append is a SUCCESS — reported as such to status
        // pollers, the async contract's ONLY verdict channel (a FAILED
        // here invites the double-appending resubmission).
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "committed publish must report SUCCESS, not invite a resubmit"
        );

        // The durable row was flushed to SUCCESS by the bounded
        // background retry once the injected outage cleared.
        let mut durable_status = String::new();
        for _ in 0..500 {
            if let Some(row) = metadata.get_task(expected_id).await.expect("get row") {
                durable_status.clone_from(&row.status);
                if durable_status == "SUCCESS" {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            durable_status, "SUCCESS",
            "the bounded background retry must flush the terminal row on \
             store recovery"
        );

        // No double count: exactly one committed segment, query-visible
        // exactly once, and a truthful SUCCESS means no client resubmits.
        let used = metadata
            .get_used_segments("fence_leak_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), 1, "exactly one committed append segment");
        assert_eq!(historical.loaded_segments().len(), 1);
        // Interval-lock hygiene.
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks");
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
    }

    /// Bounded-retry EXHAUSTION (the persist never succeeds): the fence
    /// and the retry marker must STILL leave the registry in bounded time
    /// — the cap is the leak backstop, not an accumulation point — while
    /// the in-memory status keeps reporting the committed truth and the
    /// durable row stays RUNNING (the documented
    /// crash-between-publication-and-terminal-persist residual; the
    /// segment itself is committed and the bootstrap reload re-serves it
    /// after a restart).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_persist_retry_exhaustion_still_retires_fence_and_reports_truth() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // A terminal-persist outage that NEVER clears.
        overlord
            .inject_terminal_persist_failures
            .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "persist_cap_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "appendToExisting": true
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_persist_cap_ds_1";

        let id = overlord.submit_task(spec).await.expect("submit");
        assert_eq!(id, expected_id);
        // Polled verdict: SUCCESS regardless of the persist outage (the
        // in-memory truth the async contract serves).
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "committed publish reports success regardless of the persist outage"
        );

        // Bounded: fence retired immediately, retry marker gone once the
        // capped loop exhausts — nothing accumulates even when the store
        // never recovers.
        assert!(
            await_registry_quiescence(&overlord).await,
            "registry must quiesce in bounded time even when every persist \
             attempt fails"
        );

        // In-memory truth: SUCCESS for this process's lifetime.
        let info = overlord
            .get_task(expected_id)
            .await
            .expect("get")
            .expect("record");
        assert_eq!(info.status, TaskStatus::Success);

        // Honest residual: the DURABLE row stays RUNNING (the documented
        // crash-residual shape) — the retry budget is exhausted, not
        // silently retried forever.
        let row = metadata
            .get_task(expected_id)
            .await
            .expect("get row")
            .expect("row present");
        assert_eq!(
            row.status, "RUNNING",
            "with the store permanently failing the durable row keeps the \
             documented RUNNING residual shape"
        );
        // The committed segment is durable state either way.
        let used = metadata
            .get_used_segments("persist_cap_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), 1);
    }

    /// A ROLLED-BACK publish whose terminal persist also fails: pre-fix
    /// the tail dropped the fence but left the in-memory record RUNNING
    /// forever (a status endpoint lie: clients wait on a task that
    /// already failed; only an explicit shutdown ever resolved it).
    /// Post-fix the tail finalizes FAILED in-memory in bounded time and
    /// the background retry flushes the durable FAILED row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_publish_with_failed_terminal_persist_finalizes_failed_not_stuck_running() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // Publish metadata fails on every attempt (sticky) -> the task
        // exhausts its retries and ends FAILED; the terminal persist of
        // that FAILED row fails twice, then the background retry lands.
        overlord
            .inject_insert_segment_failure
            .store(true, std::sync::atomic::Ordering::SeqCst);
        overlord
            .inject_terminal_persist_failures
            .store(2, std::sync::atomic::Ordering::SeqCst);

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "fail_persist_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    }
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_fail_persist_ds_1";

        let id = overlord.submit_task(spec).await.expect("submit");
        assert_eq!(id, expected_id);

        // Truthful terminal state IN-MEMORY, in bounded time — not a
        // stuck-RUNNING status lie. A rolled-back publish is a genuine
        // failure, and the polled status (the async contract's only
        // verdict channel) must say so.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Failed,
            "a failed task whose terminal persist failed must still read \
             FAILED, not RUNNING forever"
        );

        // Registry quiescent; durable FAILED row flushed by the retry.
        assert!(await_registry_quiescence(&overlord).await);
        let mut durable_status = String::new();
        for _ in 0..500 {
            if let Some(row) = metadata.get_task(expected_id).await.expect("get row") {
                durable_status.clone_from(&row.status);
                if durable_status == "FAILED" {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(durable_status, "FAILED");
        // Nothing committed, nothing loaded.
        assert!(
            metadata
                .get_used_segments("fail_persist_ds")
                .await
                .expect("used")
                .is_empty()
        );
        assert!(historical.loaded_segments().is_empty());
        // Interval-lock hygiene despite both failure injections.
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks");
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
    }

    /// A tail that DIES (abort/panic) with its publish fence still
    /// registered must not strand the fence until some later explicit
    /// shutdown: `BatchTailGuard::drop` spawns a verdict-driven recovery
    /// finalizer. Driven directly (registry + guard, no real panic
    /// machinery): one task whose orphaned fence resolves COMMITTED must
    /// finalize SUCCESS (and upsert a truthful durable row even though
    /// the task never persisted one), and one whose publish died
    /// verdict-less (sender dropped) must fail closed to FAILED. Both
    /// leave the registry quiescent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tail_guard_drop_with_live_fence_spawns_recovery_finalizer() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));

        for (task_id, verdict, expected_state, expected_row) in [
            (
                "guard_recover_committed",
                Some(true),
                TaskState::Success,
                "SUCCESS",
            ),
            (
                "guard_recover_verdictless",
                None,
                TaskState::Failed,
                "FAILED",
            ),
        ] {
            // A RUNNING in-memory record, as registered by submit_task.
            {
                let mut tasks = overlord.running_tasks.write().await;
                tasks.insert(
                    task_id.to_string(),
                    TaskRecord {
                        id: task_id.to_string(),
                        task_type: "index_parallel".to_string(),
                        data_source: "guard_ds".to_string(),
                        state: TaskState::Running,
                        created_time: Utc::now(),
                        location: None,
                        attempt: 1,
                        worker: None,
                    },
                );
            }
            // A registered in-flight fence, as registered by
            // execute_index_parallel (append mode, idle op gate — the
            // verdictless case then exercises the D1 durable-state
            // resolution against an empty store: NOT committed → FAILED).
            let (fence_tx, fence_rx) = tokio::sync::watch::channel(FenceState::InFlight);
            {
                let mut registry = lock_batch_tails(&overlord.batch_tails);
                registry.publish_fences.insert(
                    task_id.to_string(),
                    PublishFenceEntry {
                        verdict: fence_rx,
                        append_to_existing: true,
                        store_ops: Arc::new(tokio::sync::Mutex::new(())),
                    },
                );
            }
            // The tail dies with the fence registered (abort/panic drop).
            drop(BatchTailGuard {
                task_id: task_id.to_string(),
                overlord: overlord.clone_handle(),
            });
            // Resolve (or kill) the publish: the recovery finalizer must
            // pick the verdict up.
            match verdict {
                Some(v) => {
                    fence_tx
                        .send(FenceState::Resolved { committed: v })
                        .expect("recovery holds a receiver");
                    // Keep the sender alive until the verdict is seen.
                    let mut resolved = false;
                    for _ in 0..500 {
                        if overlord
                            .get_task(task_id)
                            .await
                            .expect("get")
                            .is_some_and(|t| t.state == expected_state)
                        {
                            resolved = true;
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    assert!(resolved, "recovery finalizer never resolved {task_id}");
                    drop(fence_tx);
                }
                None => drop(fence_tx),
            }

            // Truthful terminal state + full registry quiescence.
            assert!(
                await_registry_quiescence(&overlord).await,
                "orphaned fence for {task_id} must be retired by the \
                 recovery finalizer, not parked for a later shutdown"
            );
            let info = overlord
                .get_task(task_id)
                .await
                .expect("get")
                .expect("record");
            assert_eq!(info.state, expected_state, "task {task_id}");
            // The recovery persists a truthful durable row (upsert: the
            // task never had one).
            let mut durable_status = String::new();
            for _ in 0..500 {
                if let Some(row) = metadata.get_task(task_id).await.expect("get row") {
                    durable_status.clone_from(&row.status);
                    if durable_status == expected_row {
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert_eq!(durable_status, expected_row, "task {task_id}");
        }
    }

    /// D3 — a tail that PANICS strictly BEFORE its publish fence is
    /// registered must still be finalized: FAILED (nothing published),
    /// interval locks released, durable row terminal. Pre-fix the
    /// `BatchTailGuard` only launched recovery when a fence existed, so
    /// a pre-fence panic left the task RUNNING permanently — in memory
    /// AND in the durable store — with its `JoinHandle` unobserved and
    /// no finalizer anywhere.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tail_panic_before_fence_registration_finalizes_failed_not_running_forever() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // The injected fault: the tail panics at the top of
        // execute_index_parallel — BEFORE any fence exists.
        overlord
            .inject_pre_fence_panic
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "prefence_panic_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"]
                }
            }
        });
        // Deterministic: fresh store + first submission for this key space.
        let expected_id = "index_parallel_prefence_panic_ds_1";

        let id = overlord.submit_task(spec).await.expect("submit");
        assert_eq!(id, expected_id);

        // The panicked tail must be finalized FAILED in bounded time —
        // never RUNNING forever.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Failed,
            "a pre-fence tail panic published nothing: the truthful \
             terminal state is FAILED"
        );
        // The DURABLE row must reach FAILED too (pre-fix it stayed
        // RUNNING until a restart's bootstrap reconcile).
        let mut durable_status = String::new();
        for _ in 0..500 {
            if let Some(row) = metadata.get_task(expected_id).await.expect("get row") {
                durable_status.clone_from(&row.status);
                if durable_status == "FAILED" {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            durable_status, "FAILED",
            "the durable task row must be finalized FAILED, not left RUNNING"
        );
        // Nothing was published; the interval locks are released.
        assert!(
            metadata
                .get_used_segments("prefence_panic_ds")
                .await
                .expect("used")
                .is_empty(),
            "a pre-fence panic must publish nothing"
        );
        let mut locks = Vec::new();
        for _ in 0..500 {
            locks = metadata
                .get_locks_for_task(expected_id)
                .await
                .expect("locks");
            if locks.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
        // Registry hygiene: the dead tail leaves no entry behind.
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence/retry entry may outlive the panicked tail"
        );
    }

    /// Ship2 H10: a durable EXCLUSIVE task lock orphaned by a process
    /// crash (persisted, but its owner died with the process) must be
    /// RELEASED by startup reconciliation — pre-fix nothing reaped it:
    /// `try_acquire_lock` kept re-reading the orphan row, every
    /// equal-priority submission overlapping the interval parked WAITING
    /// forever, and `shutdown_task` (in-memory lookup only) offered no
    /// API path to clear it.
    #[tokio::test]
    async fn bootstrap_releases_crash_orphaned_task_locks() {
        let store = Arc::new(MetadataStore::new_in_memory().await.expect("create store"));
        store.initialize().await.expect("init schema");

        // Process 1: a task acquires a durable interval lock, then the
        // process CRASHES (drop without any release path running).
        let overlord_a = Overlord::new(Arc::clone(&store));
        let interval = Interval::new(1_704_067_200_000, 1_704_153_600_000).expect("interval");
        let granted = overlord_a
            .acquire_lock(
                "index_parallel_lock_ds_7",
                "lock_ds",
                interval,
                LockType::Exclusive,
                0,
            )
            .await
            .expect("acquire");
        assert!(granted.is_some(), "lock must be granted and persisted");
        drop(overlord_a); // crash: the lock row is now orphaned

        // Process 2 (restart): bootstrap must reconcile the orphan away.
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord_b = Overlord::with_executor(Arc::clone(&store), Arc::clone(&historical));
        overlord_b
            .bootstrap_reload_segments()
            .await
            .expect("bootstrap");
        let locks = store
            .get_locks_for_datasource("lock_ds", true)
            .await
            .expect("locks");
        assert!(
            locks.is_empty(),
            "crash-orphaned task lock must be released on startup: {locks:?}"
        );

        // And an equal-priority (default 0) submission over the same
        // interval now runs to SUCCESS instead of parking WAITING forever.
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "lock_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"]
                }
            }
        });
        let id = overlord_b.submit_task(spec).await.expect("submit");
        assert_eq!(
            await_task_terminal(&overlord_b, &id).await,
            TaskState::Success,
            "submission overlapping the orphaned lock's interval must run, \
             not park WAITING behind a dead task's lock"
        );
    }

    /// Review High on ship2 H10: `submit_task` persists a task's granted
    /// lock BEFORE the record enters the in-memory `running_tasks` table
    /// (`try_acquire_lock` runs ahead of `persist_and_store`), so an
    /// orphaned-lock reconcile pass overlapping a live local acquisition
    /// — or invoked any time after serving starts — would see a persisted
    /// lock whose owner is absent from `running_tasks`, misread it as a
    /// crash orphan, delete it, and let an overlapping equal-priority
    /// task acquire + publish concurrently (a later metadata swap then
    /// marks the other task's segment unused = lost rows).
    ///
    /// Once ANY local lock acquisition has happened, reconciliation must
    /// be INERT: release nothing, delete no row — not even a row that
    /// LOOKS orphaned (unknown owner), because after local lock activity
    /// an untracked owner is indistinguishable from a live in-flight
    /// submission. Genuine crash orphans are reaped exclusively by the
    /// NEXT restart's bootstrap, when this process provably holds none.
    ///
    /// Pre-fix this test fails: reconcile released BOTH rows (the live
    /// task's lock included) because neither owner was in `running_tasks`.
    #[tokio::test]
    async fn reconcile_is_inert_once_a_local_lock_was_acquired() {
        let store = Arc::new(MetadataStore::new_in_memory().await.expect("create store"));
        store.initialize().await.expect("init schema");

        // A PRIOR process acquired a lock and crashed: a genuine orphan.
        let overlord_prior = Overlord::new(Arc::clone(&store));
        let ghost_interval = Interval::new(1_704_067_200_000, 1_704_153_600_000).expect("interval");
        overlord_prior
            .acquire_lock(
                "index_parallel_ghost_ds_9",
                "ghost_ds",
                ghost_interval,
                LockType::Exclusive,
                0,
            )
            .await
            .expect("acquire ghost");
        drop(overlord_prior); // crash

        // THIS process: a live local acquisition whose owning task is NOT
        // in `running_tasks` — exactly the in-flight submission window.
        let overlord = Overlord::new(Arc::clone(&store));
        let live_interval = Interval::new(1_704_067_200_000, 1_704_153_600_000).expect("interval");
        let granted = overlord
            .acquire_lock(
                "index_parallel_live_ds_1",
                "live_ds",
                live_interval,
                LockType::Exclusive,
                0,
            )
            .await
            .expect("acquire live");
        assert!(granted.is_some(), "live lock must be granted");

        // Reconcile after local lock activity must refuse outright.
        let released = overlord
            .reconcile_orphaned_task_locks()
            .await
            .expect("reconcile");
        assert_eq!(
            released, 0,
            "reconcile must be inert once this process has acquired a lock"
        );
        let live_locks = store
            .get_locks_for_datasource("live_ds", true)
            .await
            .expect("live locks");
        assert_eq!(
            live_locks.len(),
            1,
            "the live in-flight task's lock must NOT be reaped as an \
             orphan: {live_locks:?}"
        );
        // Honest trade-off: the genuine prior-process orphan is ALSO left
        // in place (it is reaped at the next restart's bootstrap instead)
        // — after local activity the two are indistinguishable.
        let ghost_locks = store
            .get_locks_for_datasource("ghost_ds", true)
            .await
            .expect("ghost locks");
        assert_eq!(
            ghost_locks.len(),
            1,
            "post-activity reconcile must not touch ANY row: {ghost_locks:?}"
        );
    }

    /// A minimal runnable `index_parallel` spec whose ioConfig carries an
    /// explicit interval (so submission requests an EXCLUSIVE interval
    /// lock over 2024-01-01/2024-01-02).
    fn lock_queue_spec(ds: &str) -> serde_json::Value {
        json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": ds,
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    },
                    "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"]
                }
            }
        })
    }

    /// The High strand fix (queue-on-lock): a batch submission whose
    /// interval lock is HELD by an equal-priority task must be accepted
    /// WAITING with a LIVE owner — a tracked waiter registered in
    /// `batch_tails` exactly like a normal batch tail — and once the
    /// conflicting lock is RELEASED it must acquire the lock and run the
    /// normal execute→publish→terminal path to SUCCESS (Druid parity: a
    /// lock-blocked task queues until the lock frees).
    ///
    /// Pre-fix this test fails at the owner assertion: `submit_task`
    /// persisted the WAITING row and returned with NO tail, NO finalizer
    /// and NO lock retry anywhere — releasing the conflicting lock
    /// resumed nothing and the accepted task stayed WAITING forever
    /// (bootstrap reconciliation ignores WAITING rows, so not even a
    /// restart recovered a verdict).
    #[tokio::test]
    async fn lock_conflicted_batch_submission_queues_and_runs_when_lock_frees() {
        let (metadata, _historical, overlord, _dir) = setup_executor().await;

        // Another task holds the interval EXCLUSIVE at equal priority.
        let interval = Interval::new(1_704_067_200_000, 1_704_153_600_000).expect("interval");
        let blocker = overlord
            .acquire_lock("blocker_task", "lockq_ds", interval, LockType::Exclusive, 0)
            .await
            .expect("acquire blocker")
            .expect("blocker granted");

        let id = overlord
            .submit_task(lock_queue_spec("lockq_ds"))
            .await
            .expect("submit");

        // Accepted + visible/pollable as WAITING…
        let info = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(info.state, TaskState::Waiting, "queued task is WAITING");
        // …and OWNED: a live waiter tail is registered (the strand
        // witness — pre-fix nothing owned the accepted task).
        assert!(
            lock_batch_tails(&overlord.batch_tails)
                .tails
                .contains_key(&id),
            "a lock-conflicted accepted batch task must have a LIVE \
             registered owner (waiter tail) — an ownerless WAITING row \
             strands forever"
        );

        // Release the conflicting lock: the waiter must acquire it and
        // drive the task to SUCCESS.
        metadata
            .delete_lock(&blocker.id)
            .await
            .expect("release blocker");
        assert_eq!(
            await_task_terminal(&overlord, &id).await,
            TaskState::Success,
            "once the conflicting lock frees, the queued task must run \
             to SUCCESS — never park WAITING forever"
        );

        // Durable verdict + the data genuinely published.
        let row = metadata.get_task(&id).await.expect("row").expect("some");
        assert_eq!(row.status, "SUCCESS", "durable row carries the verdict");
        assert!(
            !metadata
                .get_used_segments("lockq_ds")
                .await
                .expect("used")
                .is_empty(),
            "the queued task's segment must be published"
        );

        // Quiescence: no tail/fence/retry entry and no lingering locks.
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence/retry entry may outlive the completed waiter"
        );
        let mut locks = Vec::new();
        for _ in 0..500 {
            locks = metadata.get_locks_for_task(&id).await.expect("locks");
            if locks.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
    }

    /// The lock-wait deadline (bounded queue-on-lock): a queued batch
    /// task whose conflicting lock is NEVER released must not wait
    /// forever with no resolution — after the configurable
    /// (test-overridable) lock-wait deadline its waiter finalizes it
    /// FAILED (truthful: it never ran, nothing published), the waiter
    /// and registry entries are reaped, the task holds no locks, and
    /// the conflicting holder's lock is untouched.
    #[tokio::test]
    async fn lock_wait_deadline_finalizes_queued_batch_task_failed() {
        let (metadata, _historical, overlord, _dir) = setup_executor().await;
        let overlord = overlord.with_lock_wait_deadline(std::time::Duration::from_millis(100));

        let interval = Interval::new(1_704_067_200_000, 1_704_153_600_000).expect("interval");
        overlord
            .acquire_lock(
                "blocker_task",
                "lockq_deadline_ds",
                interval,
                LockType::Exclusive,
                0,
            )
            .await
            .expect("acquire blocker")
            .expect("blocker granted");

        let id = overlord
            .submit_task(lock_queue_spec("lockq_deadline_ds"))
            .await
            .expect("submit");

        // The deadline must resolve the queued task FAILED in bounded
        // time (never WAITING forever behind a lock that never frees).
        assert_eq!(
            await_task_terminal(&overlord, &id).await,
            TaskState::Failed,
            "an unacquirable lock must finalize the queued task FAILED \
             at the lock-wait deadline"
        );
        // Durable verdict lands too (poll: the terminal persist may be
        // a beat behind the in-memory flip only on the retry path).
        let mut durable_status = String::new();
        for _ in 0..500 {
            if let Some(row) = metadata.get_task(&id).await.expect("row") {
                durable_status.clone_from(&row.status);
                if durable_status == "FAILED" {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(durable_status, "FAILED", "durable row carries the verdict");
        // It never ran: nothing published, no locks held by the task.
        assert!(
            metadata
                .get_used_segments("lockq_deadline_ds")
                .await
                .expect("used")
                .is_empty(),
            "a deadline-failed queued task must never publish"
        );
        assert!(
            metadata
                .get_locks_for_task(&id)
                .await
                .expect("locks")
                .is_empty(),
            "a deadline-failed queued task must hold no locks"
        );
        // The conflicting holder's lock is untouched; registry quiescent.
        assert_eq!(
            metadata
                .get_locks_for_datasource("lockq_deadline_ds", true)
                .await
                .expect("blocker locks")
                .len(),
            1,
            "the deadline must NOT release the conflicting holder's lock"
        );
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence/retry entry may outlive the deadline-failed waiter"
        );
    }

    /// `shutdown_task` on a still-WAITING lock-queued task: the waiter
    /// is a live registered owner (abortable like any batch tail), so a
    /// shutdown terminates it FAILED promptly, releases nothing it never
    /// acquired, leaves the CONFLICTING holder's lock untouched, and
    /// leaks no registry entry. Pre-fix there was no owner to abort at
    /// all (the pre-fix `shutdown_task` only flipped the in-memory row).
    #[tokio::test]
    async fn shutdown_of_lock_queued_waiting_task_terminates_and_leaks_nothing() {
        let (metadata, _historical, overlord, _dir) = setup_executor().await;

        let interval = Interval::new(1_704_067_200_000, 1_704_153_600_000).expect("interval");
        overlord
            .acquire_lock(
                "blocker_task",
                "lockq_shutdown_ds",
                interval,
                LockType::Exclusive,
                0,
            )
            .await
            .expect("acquire blocker")
            .expect("blocker granted");

        let id = overlord
            .submit_task(lock_queue_spec("lockq_shutdown_ds"))
            .await
            .expect("submit");
        let info = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(info.state, TaskState::Waiting, "queued task is WAITING");
        // The strand witness (pre-fix RED): a live owner must exist.
        assert!(
            lock_batch_tails(&overlord.batch_tails)
                .tails
                .contains_key(&id),
            "a WAITING lock-queued task must have a live abortable waiter"
        );

        overlord.shutdown_task(&id).await.expect("shutdown");
        let info = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(
            info.state,
            TaskState::Failed,
            "shutdown of a WAITING task terminates it FAILED"
        );
        let row = metadata.get_task(&id).await.expect("row").expect("some");
        assert_eq!(row.status, "FAILED", "durable row carries the verdict");

        // The aborted waiter leaks nothing: registry quiescent, no locks
        // held by the shut-down task, the blocker's lock untouched.
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence/retry entry may outlive the aborted waiter"
        );
        assert!(
            metadata
                .get_locks_for_task(&id)
                .await
                .expect("locks")
                .is_empty(),
            "the WAITING task never acquired a lock; nothing to release"
        );
        assert_eq!(
            metadata
                .get_locks_for_datasource("lockq_shutdown_ds", true)
                .await
                .expect("blocker locks")
                .len(),
            1,
            "shutdown of the queued task must NOT release the \
             conflicting holder's lock"
        );

        // And nothing resurrects the shut-down task: release the blocker,
        // give any (wrongly) surviving waiter time to retry, and confirm
        // the verdict stands with nothing published.
        for lock in metadata
            .get_locks_for_task("blocker_task")
            .await
            .expect("blocker rows")
        {
            metadata.delete_lock(&lock.id).await.expect("release");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let info = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(
            info.state,
            TaskState::Failed,
            "a shut-down queued task must stay FAILED after the lock frees"
        );
        assert!(
            metadata
                .get_used_segments("lockq_shutdown_ds")
                .await
                .expect("used")
                .is_empty(),
            "a shut-down queued task must never publish"
        );
    }

    /// Crash residual of the queue-on-lock design: a durable batch
    /// WAITING row whose owning process died (its waiter dies with the
    /// process) must be reconciled FAILED by the next bootstrap — it
    /// never ran and can never resume, so leaving it WAITING would
    /// strand a pollable accepted task forever (pre-fix the F2 bootstrap
    /// reconcile skipped WAITING rows entirely).
    #[tokio::test]
    async fn bootstrap_reconciles_crash_orphaned_waiting_batch_row_failed() {
        let store = Arc::new(MetadataStore::new_in_memory().await.expect("create store"));
        store.initialize().await.expect("init schema");

        // Process 1: a lock-conflicted batch submission persists a
        // WAITING row, then the process CRASHES (drop; the row's owner —
        // stub here, waiter in the executor path — dies with it).
        let overlord_a = Overlord::new(Arc::clone(&store));
        let interval = Interval::new(1_704_067_200_000, 1_704_153_600_000).expect("interval");
        overlord_a
            .acquire_lock(
                "blocker_task",
                "lockq_crash_ds",
                interval,
                LockType::Exclusive,
                0,
            )
            .await
            .expect("acquire blocker")
            .expect("blocker granted");
        let id = overlord_a
            .submit_task(lock_queue_spec("lockq_crash_ds"))
            .await
            .expect("submit");
        let row = store.get_task(&id).await.expect("row").expect("some");
        assert_eq!(row.status, "WAITING", "pre-crash shape: durable WAITING");
        drop(overlord_a); // crash

        // Process 2 (restart): bootstrap must finalize the ownerless
        // WAITING batch row FAILED (truthful: it never ran; a
        // resubmission is safe) instead of leaving it stranded.
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord_b = Overlord::with_executor(Arc::clone(&store), historical);
        overlord_b
            .bootstrap_reload_segments()
            .await
            .expect("bootstrap");
        let row = store.get_task(&id).await.expect("row").expect("some");
        assert_eq!(
            row.status, "FAILED",
            "a crash-orphaned WAITING batch row must be reconciled FAILED"
        );
        // The restart-fallback poll view agrees.
        let info = overlord_b.get_task(&id).await.expect("get").expect("some");
        assert_eq!(
            info.state,
            TaskState::Failed,
            "pollers must see the reconciled terminal verdict"
        );
    }

    /// The 2026-07-11 rollup bug: `parse_index_parallel_spec` never read
    /// `granularitySpec.rollup` / `queryGranularity`, so the REST
    /// `/druid/indexer/v1/task` path ALWAYS ingested raw un-rolled rows and
    /// `BatchIngester::ingest_with_rollup` was dead code in the server
    /// binary.  A spec with `rollup: true` + `queryGranularity: "hour"`
    /// whose rows share an (hour, dims) key must store MERGED rows with
    /// summed metrics and the merged raw-row count in the `count`-type
    /// metric — exactly what Druid does.
    ///
    /// Pre-fix this test fails: 4 raw rows are stored (`num_rows == 4`,
    /// `cnt == [1, 1, 1, 1]`).
    #[tokio::test]
    async fn submit_index_parallel_rollup_merges_rows() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // 4 raw rows; rows 1+2 share (hour 00, site_a) so rollup at "hour"
        // merges them: 4 raw -> 3 stored rows.
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "rollup_ds",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["site_id"]},
                    "metricsSpec": [
                        {"type": "count", "name": "cnt"},
                        {"type": "longSum", "name": "value_sum", "fieldName": "value"}
                    ],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "DAY",
                        "queryGranularity": "hour",
                        "rollup": true
                    }
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:05:00Z\",\"site_id\":\"site_a\",\"value\":10}\n{\"timestamp\":\"2024-01-01T00:35:00Z\",\"site_id\":\"site_a\",\"value\":5}\n{\"timestamp\":\"2024-01-01T00:20:00Z\",\"site_id\":\"site_b\",\"value\":7}\n{\"timestamp\":\"2024-01-01T01:10:00Z\",\"site_id\":\"site_a\",\"value\":3}"
                    }
                }
            }
        });

        let _id = submit_ok(&overlord, spec).await;

        let segs = historical.loaded_segments();
        assert_eq!(segs.len(), 1, "expected 1 segment loaded, got {segs:?}");
        let seg = historical
            .get_segment(&segs[0])
            .expect("get_segment")
            .expect("segment data readable");

        // 4 raw rows roll up to 3 stored rows (site_a hour-00 pair merges).
        assert_eq!(seg.num_rows, 3, "rollup must merge the hour-00 pair");

        // Stored rows sorted by (truncated __time, dims):
        //   (00:00, site_a): cnt=2, value_sum=15
        //   (00:00, site_b): cnt=1, value_sum=7
        //   (01:00, site_a): cnt=1, value_sum=3
        match seg.columns.get("cnt") {
            Some(ferrodruid_segment::column::ColumnData::Long(v)) => {
                assert_eq!(v, &vec![2, 1, 1], "count metric = merged raw-row count");
            }
            other => panic!("cnt column should be Long, got {other:?}"),
        }
        match seg.columns.get("value_sum") {
            Some(ferrodruid_segment::column::ColumnData::Double(v)) => {
                assert_eq!(v, &vec![15.0, 7.0, 3.0], "metric summed within groups");
            }
            other => panic!("value_sum column should be Double, got {other:?}"),
        }
        match seg.columns.get("__time") {
            Some(ferrodruid_segment::column::ColumnData::Long(v)) => {
                // 2024-01-01T00:00:00Z / T01:00:00Z — truncated to the hour.
                assert_eq!(
                    v,
                    &vec![1_704_067_200_000, 1_704_067_200_000, 1_704_070_800_000]
                );
            }
            other => panic!("__time column should be Long, got {other:?}"),
        }
    }

    /// `granularitySpec` parsing is Druid-faithful:
    /// * an absent `rollup` field inside a present spec defaults to true
    ///   (Druid's `UniformGranularitySpec` default);
    /// * grain strings are case-insensitive (the fixture files use
    ///   `"HOUR"` / `"NONE"`);
    /// * the period object form maps `PT1H` -> `hour`;
    /// * an entirely absent `granularitySpec` means Druid's default
    ///   `{rollup: true, queryGranularity: none}`;
    /// * `rollup: false` is honoured.
    #[test]
    fn parse_spec_reads_rollup_and_query_granularity() {
        let base = |gspec: serde_json::Value| {
            let mut data_schema = json!({
                "dataSource": "t",
                "timestampSpec": {"column": "timestamp", "format": "iso"},
                "dimensionsSpec": {"dimensions": ["d"]},
                "metricsSpec": []
            });
            if !gspec.is_null()
                && let Some(obj) = data_schema.as_object_mut()
            {
                obj.insert("granularitySpec".to_string(), gspec);
            }
            json!({
                "type": "index_parallel",
                "spec": {
                    "dataSchema": data_schema,
                    "ioConfig": {
                        "inputSource": {
                            "type": "inline",
                            "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"d\":\"x\"}"
                        }
                    }
                }
            })
        };
        let parse = |gspec: serde_json::Value| {
            parse_index_parallel_spec(&base(gspec))
                .expect("parse ok")
                .expect("supported shape")
        };

        // rollup absent inside a present spec -> true; "HOUR" -> "hour".
        let p = parse(json!({"type": "uniform", "queryGranularity": "HOUR"}));
        assert!(p.rollup, "absent rollup defaults to true (Druid)");
        assert_eq!(p.query_granularity, "hour");

        // Period object form.
        let p = parse(
            json!({"rollup": true, "queryGranularity": {"type": "period", "period": "PT1H"}}),
        );
        assert!(p.rollup);
        assert_eq!(p.query_granularity, "hour");

        // Entirely absent granularitySpec -> Druid's default spec.
        let p = parse(serde_json::Value::Null);
        assert!(p.rollup, "absent granularitySpec means rollup=true (Druid)");
        assert_eq!(p.query_granularity, "none");

        // rollup: false honoured; "NONE" case-insensitive.
        let p = parse(json!({"rollup": false, "queryGranularity": "NONE"}));
        assert!(!p.rollup);

        // Unsupported grain fails closed when rollup is on...
        let err = parse_index_parallel_spec(&base(
            json!({"rollup": true, "queryGranularity": "fifteen_minute"}),
        ))
        .expect_err("unsupported rollup grain must be rejected");
        assert!(format!("{err}").contains("unsupported queryGranularity"));

        // ...but is ignored when rollup is off (the raw path never
        // truncates, so the spec must not fail).
        let p = parse(json!({"rollup": false, "queryGranularity": "fifteen_minute"}));
        assert!(!p.rollup);
    }

    // ----- batch inputSource execution (compat-4) --------------------------

    /// The minimal `*`/`?` glob used by the `local` inputSource `filter`.
    #[test]
    fn glob_matches_wildcards() {
        // Literals.
        assert!(glob_matches("data.json", "data.json"));
        assert!(!glob_matches("data.json", "data.jsonl"));
        assert!(!glob_matches("data.json", "data_json"));
        // `*` runs (including empty).
        assert!(glob_matches("*", "anything.at.all"));
        assert!(glob_matches("*", ""));
        assert!(glob_matches("*.json", "a.json"));
        assert!(glob_matches("*.json", ".json"));
        assert!(!glob_matches("*.json", "a.jsonl"));
        assert!(!glob_matches("*.json", "a.csv"));
        assert!(glob_matches("a*b*c", "aXXbYYc"));
        assert!(glob_matches("a*b*c", "abc"));
        assert!(!glob_matches("a*b*c", "acb"));
        assert!(glob_matches("wiki-*.json", "wiki-2024-01-01.json"));
        // `?` is exactly one character.
        assert!(glob_matches("?.json", "a.json"));
        assert!(!glob_matches("?.json", "ab.json"));
        assert!(!glob_matches("?.json", ".json"));
        // Multi-byte characters count as one `?`.
        assert!(glob_matches("?.json", "\u{3042}.json"));
        // Empty pattern matches only the empty name.
        assert!(glob_matches("", ""));
        assert!(!glob_matches("", "a"));
    }

    /// Boilerplate for executor-backed batch tests: in-memory metadata,
    /// tempdir-cached Historical, executor Overlord. The returned `TempDir`
    /// must be held alive by the caller for the Historical cache to stay
    /// valid.
    async fn executor_setup() -> (
        Arc<MetadataStore>,
        Arc<ferrodruid_historical::Historical>,
        tempfile::TempDir,
        Overlord,
    ) {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        (metadata, historical, cache_dir, overlord)
    }

    /// Build a native-batch spec with a `local` inputSource.
    fn local_batch_spec(
        data_source: &str,
        base_dir: &std::path::Path,
        filter: Option<&str>,
        input_format: serde_json::Value,
    ) -> serde_json::Value {
        let mut input_source = json!({
            "type": "local",
            "baseDir": base_dir.to_str().expect("utf-8 tempdir path"),
        });
        if let Some(f) = filter {
            input_source["filter"] = json!(f);
        }
        json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": data_source,
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [
                        {"type": "count", "name": "count"},
                        {"type": "longSum", "name": "added", "fieldName": "added"}
                    ],
                    "granularitySpec": {"rollup": false}
                },
                "ioConfig": {
                    "inputSource": input_source,
                    "inputFormat": input_format
                }
            }
        })
    }

    /// compat-4 core: a `local` inputSource with JSONL files actually
    /// EXECUTES (pre-fix it parked PENDING forever). Two files, all rows
    /// land in one loaded segment with a `used=true` metadata row.
    #[tokio::test]
    async fn submit_local_json_input_source_executes() {
        let (metadata, historical, _cache, overlord) = executor_setup().await;
        let data_dir = tempfile::tempdir().expect("data dir");
        std::fs::write(
            data_dir.path().join("a.json"),
            "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\",\"added\":100}\n\
             {\"timestamp\":\"2024-01-01T01:00:00Z\",\"page\":\"Talk\",\"added\":50}\n",
        )
        .expect("write a.json");
        std::fs::write(
            data_dir.path().join("b.json"),
            "{\"timestamp\":\"2024-01-01T02:00:00Z\",\"page\":\"Help\",\"added\":25}\n",
        )
        .expect("write b.json");

        let spec = local_batch_spec(
            "wiki",
            data_dir.path(),
            Some("*.json"),
            json!({"type": "json"}),
        );
        // Async contract: submit yields the id; the polled status must
        // reach SUCCESS (not park).
        let _id = submit_ok(&overlord, spec).await;

        let segs = historical.loaded_segments();
        assert_eq!(segs.len(), 1, "expected 1 loaded segment, got {segs:?}");
        assert_eq!(
            historical.segment_datasource(&segs[0]).as_deref(),
            Some("wiki")
        );
        let seg = historical
            .get_segment(&segs[0])
            .expect("get_segment")
            .expect("segment data readable");
        assert_eq!(seg.num_rows, 3, "2 rows from a.json + 1 from b.json");

        let rows = metadata.get_used_segments("wiki").await.expect("used rows");
        assert_eq!(rows.len(), 1, "one used=true metadata row");
        assert_eq!(rows[0].data_source, "wiki");
    }

    /// `local` + CSV inputFormat: rows decode via the declared columns.
    #[tokio::test]
    async fn submit_local_csv_input_source_executes() {
        let (_metadata, historical, _cache, overlord) = executor_setup().await;
        let data_dir = tempfile::tempdir().expect("data dir");
        std::fs::write(
            data_dir.path().join("data.csv"),
            "2024-01-01T00:00:00Z,Main,100\n2024-01-01T01:00:00Z,Talk,50\n",
        )
        .expect("write csv");
        let spec = local_batch_spec(
            "wiki_csv",
            data_dir.path(),
            Some("*.csv"),
            json!({"type": "csv", "columns": ["timestamp", "page", "added"]}),
        );
        let _id = submit_ok(&overlord, spec).await;
        let segs = historical.loaded_segments();
        assert_eq!(segs.len(), 1);
        let seg = historical
            .get_segment(&segs[0])
            .expect("get_segment")
            .expect("segment data readable");
        assert_eq!(seg.num_rows, 2);
    }

    /// `local` + TSV inputFormat: tab-delimited rows decode via the
    /// declared columns.
    #[tokio::test]
    async fn submit_local_tsv_input_source_executes() {
        let (_metadata, historical, _cache, overlord) = executor_setup().await;
        let data_dir = tempfile::tempdir().expect("data dir");
        std::fs::write(
            data_dir.path().join("data.tsv"),
            "2024-01-01T00:00:00Z\tMain\t100\n2024-01-01T01:00:00Z\tTalk\t50\n",
        )
        .expect("write tsv");
        let spec = local_batch_spec(
            "wiki_tsv",
            data_dir.path(),
            Some("*.tsv"),
            json!({"type": "tsv", "columns": ["timestamp", "page", "added"]}),
        );
        let _id = submit_ok(&overlord, spec).await;
        let segs = historical.loaded_segments();
        assert_eq!(segs.len(), 1);
        let seg = historical
            .get_segment(&segs[0])
            .expect("get_segment")
            .expect("segment data readable");
        assert_eq!(seg.num_rows, 2);
    }

    /// Glob filtering: only `*.json` is picked from a mixed dir — the
    /// `.csv` sibling is NOT read (its content is not valid JSONL, so
    /// reading it would fail the task; SUCCESS proves the filter excluded
    /// it).
    #[tokio::test]
    async fn submit_local_glob_filter_selects_only_matching_files() {
        let (_metadata, historical, _cache, overlord) = executor_setup().await;
        let data_dir = tempfile::tempdir().expect("data dir");
        std::fs::write(
            data_dir.path().join("a.json"),
            "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\",\"added\":100}\n",
        )
        .expect("write a.json");
        std::fs::write(data_dir.path().join("b.csv"), "not,valid,jsonl\n").expect("write b.csv");
        let spec = local_batch_spec(
            "wiki_mixed",
            data_dir.path(),
            Some("*.json"),
            json!({"type": "json"}),
        );
        // SUCCESS proves the filter excluded the invalid sibling.
        let _id = submit_ok(&overlord, spec).await;
        let segs = historical.loaded_segments();
        assert_eq!(segs.len(), 1);
        let seg = historical
            .get_segment(&segs[0])
            .expect("get_segment")
            .expect("segment data readable");
        assert_eq!(seg.num_rows, 1, "only a.json's row, b.csv filtered out");
    }

    /// PATH SAFETY: a symlink inside baseDir pointing OUTSIDE it must be
    /// rejected — the task terminates FAILED (never reads the target,
    /// never parks PENDING).
    #[cfg(unix)]
    #[tokio::test]
    async fn submit_local_symlink_escape_fails_terminally() {
        let (_metadata, historical, _cache, overlord) = executor_setup().await;
        let overlord = overlord.with_retry_policy(RetryPolicy {
            max_attempts: 1,
            base_delay_millis: 1,
            max_delay_millis: 1,
        });
        let outside = tempfile::tempdir().expect("outside dir");
        std::fs::write(
            outside.path().join("evil.json"),
            "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"X\",\"added\":1}\n",
        )
        .expect("write outside file");
        let data_dir = tempfile::tempdir().expect("data dir");
        std::os::unix::fs::symlink(
            outside.path().join("evil.json"),
            data_dir.path().join("leak.json"),
        )
        .expect("symlink");

        let spec = local_batch_spec(
            "wiki_leak",
            data_dir.path(),
            Some("*.json"),
            json!({"type": "json"}),
        );
        // Execution failures no longer surface through submit: the id
        // returns and the polled status must terminate FAILED.
        let _id = submit_failed(&overlord, spec).await;
        assert!(historical.loaded_segments().is_empty(), "nothing ingested");
    }

    /// Codex R1 H1: a recognised native-batch task (`index_parallel`) whose
    /// spec is malformed — valid dataSchema + ioConfig but NO inputSource —
    /// must terminate FAILED and release its locks, never park PENDING with
    /// the lock held forever (the accept-then-hang the whole change fixes).
    #[tokio::test]
    async fn submit_index_parallel_missing_input_source_fails_and_releases_locks() {
        let (_metadata, historical, _cache, overlord) = executor_setup().await;
        let overlord = overlord.with_retry_policy(RetryPolicy {
            max_attempts: 1,
            base_delay_millis: 1,
            max_delay_millis: 1,
        });
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": {"intervals": ["2024-01-01T00:00:00Z/2024-02-01T00:00:00Z"]}
            }
        });
        // Must FAIL terminally (polled), not park PENDING.
        let _id = submit_failed(&overlord, spec).await;
        let locks = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert!(
            locks.is_empty(),
            "terminal failure must release its locks: {locks:?}"
        );
        assert!(historical.loaded_segments().is_empty());
    }

    /// Codex R1 H2 + R2 H1: `read_regular_file_checked` reads only the exact
    /// regular inode enumerated inside baseDir — a symlink final component
    /// (O_NOFOLLOW) and any OTHER inode (the ancestor-swap identity gate)
    /// are both refused, while the matching real file still reads.
    #[cfg(unix)]
    #[test]
    fn read_regular_file_checked_enforces_symlink_and_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real = dir.path().join("real.json");
        std::fs::write(&real, b"{}\n").expect("write real file");
        let real_meta = std::fs::symlink_metadata(&real).expect("stat real");
        assert!(
            read_regular_file_checked(&real, &real_meta).is_ok(),
            "a real regular file matching its enumerated identity must read"
        );

        let link = dir.path().join("link.json");
        std::os::unix::fs::symlink(&real, &link).expect("plant symlink");
        assert!(
            read_regular_file_checked(&link, &real_meta).is_err(),
            "a symlink final component must be refused (O_NOFOLLOW)"
        );

        // A different inode than the one enumerated (the state an
        // ancestor-directory symlink swap would produce) is refused by the
        // (device, inode) identity gate.
        let other = dir.path().join("other.json");
        std::fs::write(&other, b"{}\n").expect("write other file");
        assert!(
            read_regular_file_checked(&other, &real_meta).is_err(),
            "an inode different from the enumerated file must be refused (identity gate)"
        );
    }

    /// Codex R2 H2: a FIFO swapped in must be rejected WITHOUT blocking —
    /// `O_NONBLOCK` makes the read-only open return promptly (the test
    /// completing at all proves it did not hang) and the regular-file
    /// recheck fails it.
    #[cfg(unix)]
    #[test]
    fn read_regular_file_checked_rejects_fifo_without_blocking() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fifo = dir.path().join("pipe.json");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !made {
            return; // mkfifo unavailable on this host; skip.
        }
        let meta = std::fs::symlink_metadata(&fifo).expect("stat fifo");
        assert!(
            read_regular_file_checked(&fifo, &meta).is_err(),
            "a FIFO must be rejected (not a regular file), not block the read"
        );
    }

    /// Codex R3 H2: the (dev,inode) gate proves the SAME inode but not the
    /// same content — a truncate/append to that inode after enumeration
    /// must be refused by the size check, not silently publish partial or
    /// extended input.
    #[cfg(unix)]
    #[test]
    fn read_regular_file_checked_rejects_size_change() {
        let dir = tempfile::tempdir().expect("tempdir");
        let f = dir.path().join("a.json");
        std::fs::write(&f, b"line1\nline2\n").expect("write two lines");
        let enumerated = std::fs::symlink_metadata(&f).expect("stat");
        // Truncate in place (same inode, smaller size).
        std::fs::write(&f, b"line1\n").expect("truncate to one line");
        assert!(
            read_regular_file_checked(&f, &enumerated).is_err(),
            "a size change since enumeration must be refused, not silently publish partial input"
        );
    }

    /// Codex R4 H1: an in-place rewrite that PRESERVES the size (same
    /// device/inode/length) still bumps mtime/ctime, so the content-stability
    /// check must refuse it rather than publish the mutated bytes.
    #[cfg(unix)]
    #[test]
    fn read_regular_file_checked_rejects_same_size_rewrite() {
        let dir = tempfile::tempdir().expect("tempdir");
        let f = dir.path().join("a.json");
        std::fs::write(&f, b"{\"id\":1}\n").expect("write v1");
        let enumerated = std::fs::symlink_metadata(&f).expect("stat");
        // Guarantee a distinct mtime/ctime tick, then rewrite EQUAL length.
        std::thread::sleep(std::time::Duration::from_millis(10));
        std::fs::write(&f, b"{\"id\":2}\n").expect("rewrite v2 (same length)");
        assert!(
            read_regular_file_checked(&f, &enumerated).is_err(),
            "an equal-length in-place rewrite must be refused (mtime/ctime changed), \
             not silently publish mutated input"
        );
    }

    /// Codex R3 H1: a task that completed before a restart must still be
    /// reported by `get_task` (via the persisted metadata row), not a 404
    /// that would prompt a client to resubmit a duplicate. A fresh Overlord
    /// over the SAME metadata store models the restart (empty in-memory
    /// table); an unknown id is still `None`.
    #[tokio::test]
    async fn get_task_survives_restart_via_metadata_fallback() {
        let (metadata, historical, _cache, overlord) = executor_setup().await;
        let data_dir = tempfile::tempdir().expect("data dir");
        std::fs::write(
            data_dir.path().join("a.json"),
            "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"M\",\"added\":1}\n",
        )
        .expect("write a.json");
        let spec = local_batch_spec(
            "wiki",
            data_dir.path(),
            Some("*.json"),
            json!({"type": "json"}),
        );
        let id = submit_ok(&overlord, spec).await;

        let restarted = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        let after = restarted
            .get_task(&id)
            .await
            .expect("get")
            .expect("a completed task must survive restart, not 404");
        assert_eq!(after.id, id);
        assert_eq!(
            after.state,
            TaskState::Success,
            "the persisted terminal state must be reported after restart"
        );
        assert!(
            restarted
                .get_task("no-such-task-id")
                .await
                .expect("get")
                .is_none(),
            "a genuinely unknown id is still a 404 (None)"
        );
    }

    /// Codex R4 H2: if a submission is dropped (cancelled) after its lock is
    /// granted but before it commits, the durable lock must be released, not
    /// leaked forever. A no-executor overlord leaves a real held lock on a
    /// submitted task; an armed `SubmitLockGuard` dropped for that task id
    /// must release it (as the dropped submit future would).
    #[tokio::test]
    async fn submit_lock_guard_releases_locks_on_drop() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("md"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": {"intervals": ["2024-01-01T00:00:00Z/2024-02-01T00:00:00Z"]}
            }
        });
        let id = overlord.submit_task(spec).await.expect("submit");
        assert!(
            !metadata
                .get_locks_for_task(&id)
                .await
                .expect("locks")
                .is_empty(),
            "the submitted task must hold a durable lock to release"
        );

        {
            let _guard = SubmitLockGuard {
                armed: true,
                task_id: id.clone(),
                metadata: Arc::clone(&metadata),
            };
        } // dropped here -> detached release

        let mut released = false;
        for _ in 0..100 {
            tokio::task::yield_now().await;
            if metadata
                .get_locks_for_task(&id)
                .await
                .expect("locks")
                .is_empty()
            {
                released = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert!(
            released,
            "a dropped armed guard must release the task's durable locks"
        );
    }

    /// Codex R1 H3: a PRESENT-but-non-string `filter` is a malformed spec —
    /// it must fail terminally, never silently degrade to `"*"` and ingest
    /// every file under baseDir.
    #[tokio::test]
    async fn submit_local_non_string_filter_fails_terminally() {
        let (_metadata, historical, _cache, overlord) = executor_setup().await;
        let overlord = overlord.with_retry_policy(RetryPolicy {
            max_attempts: 1,
            base_delay_millis: 1,
            max_delay_millis: 1,
        });
        let data_dir = tempfile::tempdir().expect("data dir");
        std::fs::write(
            data_dir.path().join("a.json"),
            "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"M\",\"added\":1}\n",
        )
        .expect("write a.json");
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "wiki",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}],
                    "granularitySpec": {"rollup": false}
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "local",
                        "baseDir": data_dir.path().to_str().expect("utf-8 path"),
                        "filter": 7
                    },
                    "inputFormat": {"type": "json"}
                }
            }
        });
        // Must fail terminally (polled), not silently ingest all files.
        let _id = submit_failed(&overlord, spec).await;
        assert!(historical.loaded_segments().is_empty());
    }

    /// Codex R1 H4: a non-UTF-8 file name under baseDir must fail the
    /// enumeration loudly, never be silently skipped (which would publish
    /// incomplete data from the remaining files).
    #[cfg(unix)]
    #[tokio::test]
    async fn submit_local_non_utf8_filename_fails_terminally() {
        use std::os::unix::ffi::OsStrExt as _;

        let (_metadata, historical, _cache, overlord) = executor_setup().await;
        let overlord = overlord.with_retry_policy(RetryPolicy {
            max_attempts: 1,
            base_delay_millis: 1,
            max_delay_millis: 1,
        });
        let data_dir = tempfile::tempdir().expect("data dir");
        std::fs::write(
            data_dir.path().join("a.json"),
            "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"M\",\"added\":1}\n",
        )
        .expect("write valid file");
        let mut bad = std::ffi::OsString::from("bad");
        bad.push(std::ffi::OsStr::from_bytes(&[0x80]));
        bad.push(".json");
        std::fs::write(data_dir.path().join(&bad), "{}\n").expect("write non-utf8-named file");

        let spec = local_batch_spec("wiki", data_dir.path(), Some("*"), json!({"type": "json"}));
        // Must fail loud (polled FAILED), not silently drop input.
        let _id = submit_failed(&overlord, spec).await;
        assert!(
            historical.loaded_segments().is_empty(),
            "nothing may be published when enumeration fails loud"
        );
    }

    /// An unsupported inputSource (s3) must terminate FAILED and RELEASE
    /// its interval locks — pre-fix it reverted to PENDING with the locks
    /// still held (the accept-then-hang). Replaces the old
    /// `submit_unknown_input_source_falls_back_to_pending` behavior.
    #[tokio::test]
    async fn submit_unsupported_input_source_fails_and_releases_locks() {
        let (_metadata, historical, _cache, overlord) = executor_setup().await;
        let overlord = overlord.with_retry_policy(RetryPolicy {
            max_attempts: 1,
            base_delay_millis: 1,
            max_delay_millis: 1,
        });
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": {
                    "inputSource": {"type": "s3", "uris": ["s3://bucket/file.json"]},
                    "inputFormat": {"type": "json"},
                    "intervals": ["2024-01-01T00:00:00Z/2024-02-01T00:00:00Z"]
                }
            }
        });
        // Unsupported inputSource must FAIL terminally (polled), not
        // park PENDING.
        let _id = submit_failed(&overlord, spec).await;
        let locks = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert!(
            locks.is_empty(),
            "terminal failure must release its locks: {locks:?}"
        );
        assert!(historical.loaded_segments().is_empty());
    }

    /// Gate 1: Druid's serial-native `type: "index"` routes through the
    /// same executor as `index_parallel` (pre-fix it parked PENDING).
    #[tokio::test]
    async fn submit_serial_index_task_executes() {
        let (metadata, historical, _cache, overlord) = executor_setup().await;
        let spec = json!({
            "type": "index",
            "spec": {
                "dataSchema": {
                    "dataSource": "wiki_serial",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [{"type": "count", "name": "count"}]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\"}"
                    }
                }
            }
        });
        let id = submit_ok(&overlord, spec).await;
        assert!(id.starts_with("index_"), "id carries the task type: {id}");
        assert_eq!(historical.loaded_segments().len(), 1);
        let rows = metadata
            .get_used_segments("wiki_serial")
            .await
            .expect("used rows");
        assert_eq!(rows.len(), 1);
    }

    /// Zero matched input files is a hard error → terminal FAILED
    /// (mirroring the inline empty-data behavior in
    /// `retry_exhaustion_ends_failed`) — never a silent success and never
    /// a PENDING park.
    #[tokio::test]
    async fn submit_local_no_matching_files_fails_terminally() {
        let (_metadata, historical, _cache, overlord) = executor_setup().await;
        let overlord = overlord.with_retry_policy(RetryPolicy {
            max_attempts: 1,
            base_delay_millis: 1,
            max_delay_millis: 1,
        });
        let data_dir = tempfile::tempdir().expect("data dir");
        let spec = local_batch_spec(
            "wiki_nomatch",
            data_dir.path(),
            Some("*.json"),
            json!({"type": "json"}),
        );
        // Zero matched files must FAIL terminally (polled).
        let _id = submit_failed(&overlord, spec).await;
        assert!(historical.loaded_segments().is_empty());
    }

    #[tokio::test]
    async fn supervisor_auto_id() {
        let (_store, overlord) = setup().await;
        // Unknown type so the synthetic-id fallback is exercised under
        // every feature set. Since compat-5 an id-less KINESIS spec — like
        // a kafka one — derives its STABLE id from dataSource instead
        // (covered by `kinesis_spec_derives_stable_id_...`), so it can no
        // longer exercise this fallback.
        let spec = json!({
            "type": "custom",
            "dataSchema": {"dataSource": "clicks"}
        });
        let spec_id = overlord.create_supervisor(spec).await.expect("create");
        assert!(spec_id.starts_with("supervisor_"));
    }

    #[test]
    fn datasource_id_derivation_helpers() {
        // The ungated helpers behind stable id-less Kafka supervisor ids
        // (hoisted out of the kafka-io-gated module in Codex R11 so they run
        // in every build).
        assert!(is_kafka_typed(&json!({"type": "kafka"})));
        assert!(!is_kafka_typed(&json!({"type": "kinesis"})));
        assert!(is_kinesis_typed(&json!({"type": "kinesis"})));
        assert!(!is_kinesis_typed(&json!({"type": "kafka"})));
        assert_eq!(
            datasource_of(&json!({"dataSchema": {"dataSource": "ds"}})).as_deref(),
            Some("ds")
        );
        // Enveloped `{"spec": {...}}` form.
        assert_eq!(
            datasource_of(&json!({"spec": {"dataSchema": {"dataSource": "ds2"}}})).as_deref(),
            Some("ds2")
        );
        assert_eq!(datasource_of(&json!({"type": "kafka"})), None);
    }

    #[cfg(not(feature = "kafka-io"))]
    #[tokio::test]
    async fn default_build_id_less_kafka_spec_uses_stable_datasource_id() {
        // Codex R11: even in a default (no-kafka-io) build, create_supervisor
        // must derive an id-less Kafka supervisor's id from
        // dataSchema.dataSource — NOT a fresh synthetic `supervisor_N`.
        // Otherwise repeated POSTs of the same id-less spec accumulate
        // DISTINCT rows that a later kafka-io build resumes as duplicate
        // consumers (each ingesting every record). This test guards the
        // default build specifically, where the bug lived.
        let (_store, overlord) = setup().await;
        // A VALID Kafka spec (default build now validates before persist —
        // Codex R14 #3) with no explicit id.
        let spec = json!({
            "type": "kafka",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {"topic": "t", "consumerProperties": {"bootstrap.servers": "kafka:9092"}}
        });
        let id1 = overlord
            .create_supervisor(spec.clone())
            .await
            .expect("create1");
        assert_eq!(
            id1, "events",
            "id-less kafka spec must derive the datasource id"
        );
        let id2 = overlord.create_supervisor(spec).await.expect("create2");
        assert_eq!(
            id2, "events",
            "repost must reuse the derived id (dedup), not a fresh supervisor_N"
        );
    }

    #[tokio::test]
    async fn create_rejects_malformed_suspended_flag() {
        // Fable audit: `suspended` must be a bool or the strings
        // "true"/"false" (Jackson scalar coercion). Any other value must be
        // rejected LOUDLY in EVERY build — a silently-ignored junk flag would
        // pick the running-vs-suspended lifecycle state by accident.
        let (store, overlord) = setup().await;
        let spec = json!({
            "type": "kafka",
            "suspended": "maybe",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {"topic": "t", "consumerProperties": {"bootstrap.servers": "kafka:9092"}}
        });
        let err = overlord
            .create_supervisor(spec)
            .await
            .expect_err("junk suspended flag must be rejected");
        assert!(format!("{err}").contains("suspended"), "err = {err}");
        assert!(
            store.get_all_supervisors().await.expect("rows").is_empty(),
            "a rejected spec must not be persisted"
        );
    }

    #[cfg(not(feature = "kafka-io"))]
    #[tokio::test]
    async fn default_build_rejects_invalid_kafka_spec_before_persist() {
        // Codex R14 #3: the default (no-kafka-io) build must VALIDATE a Kafka
        // supervisor spec before persisting — otherwise it acknowledges +
        // persists an invalid spec (here: missing timestampSpec /
        // dimensionsSpec / bootstrap.servers) that a later kafka-io resume
        // silently skips, consuming zero records.
        let (store, overlord) = setup().await;
        let bad = json!({
            "type": "kafka",
            "dataSchema": {"dataSource": "events"},
            "ioConfig": {"topic": "t"}
        });
        let err = overlord
            .create_supervisor(bad)
            .await
            .expect_err("invalid kafka spec must be rejected");
        assert!(
            format!("{err}").contains("invalid Kafka supervisor spec"),
            "err = {err}"
        );
        // And nothing was persisted.
        assert!(
            store.get_all_supervisors().await.expect("rows").is_empty(),
            "an invalid spec must not be persisted"
        );
    }

    #[cfg(not(feature = "kafka-io"))]
    #[tokio::test]
    async fn default_build_refuses_second_id_for_persisted_kafka_pair() {
        // Codex R25: the default (no-kafka-io) build has no live consumer
        // handles, so the PERSISTED layer is its only chance to enforce the
        // one-supervisor-per-(dataSource, topic) invariant. Pre-fix it
        // accepted a second id for an occupied pair; a later kafka-io resume
        // then preferred the derived id and warn-skipped the other row —
        // that legitimately created supervisor was silently disabled (nobody
        // ever consumed its records).
        let (store, overlord) = setup().await;
        let spec = |id: &str, topic: &str| {
            json!({
                "type": "kafka",
                "id": id,
                "dataSchema": {
                    "dataSource": "events",
                    "timestampSpec": {"column": "__time", "format": "auto"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "granularitySpec": {"rollup": false}
                },
                "ioConfig": {
                    "topic": topic,
                    "consumerProperties": {"bootstrap.servers": "kafka:9092"}
                }
            })
        };
        assert_eq!(
            overlord
                .create_supervisor(spec("a", "t"))
                .await
                .expect("create a"),
            "a"
        );
        // The same (dataSource, topic) under a NEW id → loud refusal naming
        // the persisted owner.
        let err = overlord
            .create_supervisor(spec("b", "t"))
            .await
            .expect_err("a second id for an occupied (datasource, topic) pair must be refused");
        let msg = format!("{err}");
        assert!(
            msg.contains("'a'"),
            "the refusal must name the owning supervisor: {msg}"
        );
        // A DIFFERENT topic on the same datasource is a different pair.
        assert_eq!(
            overlord
                .create_supervisor(spec("c", "t2"))
                .await
                .expect("create c"),
            "c"
        );
        // Only the accepted supervisors were persisted.
        let mut ids: Vec<String> = store
            .get_all_supervisors()
            .await
            .expect("rows")
            .into_iter()
            .map(|r| r.spec_id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "c".to_string()]);
    }

    #[cfg(not(feature = "kafka-io"))]
    #[tokio::test]
    async fn default_build_concurrent_creates_cannot_both_claim_a_pair() {
        // Codex R27 F4: without the (previously kafka-io-only) lifecycle
        // lock, the default build's persisted-pair uniqueness check is
        // check-then-act across await points (TOCTOU): two concurrent
        // POSTs for the same (dataSource, topic) under different ids can
        // BOTH pass `refuse_persisted_kafka_pair_conflict` before either
        // persists, and both get persisted — the later kafka-io resume
        // then warn-skips one of them, silently disabling a legitimately
        // created supervisor. Exactly ONE of two concurrent creates may
        // succeed. (Several rounds on fresh pairs so a lucky interleaving
        // cannot hide the race; on a current-thread runtime the futures
        // interleave deterministically at their metadata awaits.)
        let (store, overlord) = setup().await;
        let spec = |id: &str, topic: &str| {
            json!({
                "type": "kafka",
                "id": id,
                "dataSchema": {
                    "dataSource": "events",
                    "timestampSpec": {"column": "__time", "format": "auto"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "granularitySpec": {"rollup": false}
                },
                "ioConfig": {
                    "topic": topic,
                    "consumerProperties": {"bootstrap.servers": "kafka:9092"}
                }
            })
        };
        for i in 0..10_u32 {
            let topic = format!("race-topic-{i}");
            let (r1, r2) = tokio::join!(
                overlord.create_supervisor(spec(&format!("x{i}"), &topic)),
                overlord.create_supervisor(spec(&format!("y{i}"), &topic)),
            );
            let ok_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
            assert_eq!(
                ok_count, 1,
                "round {i}: exactly one concurrent create may claim the pair \
                 (r1={r1:?}, r2={r2:?})"
            );
            // The loser must be the pair refusal, naming the winner.
            let loser = if r1.is_err() { &r1 } else { &r2 };
            let msg = format!("{}", loser.as_ref().expect_err("one must lose"));
            assert!(
                msg.contains("already claims"),
                "round {i}: the loser must be refused by the pair guard: {msg}"
            );
        }
        // Exactly the 10 winners were persisted — no doubled pair rows.
        let mut ids: Vec<String> = store
            .get_all_supervisors()
            .await
            .expect("rows")
            .into_iter()
            .map(|r| r.spec_id)
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(
            ids.len(),
            10,
            "one persisted supervisor per pair, never two: {ids:?}"
        );
    }

    // ----- lifecycle state machine via the Overlord API -----

    #[tokio::test]
    async fn invalid_transition_is_rejected() {
        let (_store, overlord) = setup().await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        // The stub task is PENDING. PENDING -> SUCCESS is invalid.
        let err = overlord
            .transition_task(&id, TaskState::Success)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("invalid task transition"));

        // PENDING -> RUNNING -> SUCCESS is valid.
        overlord
            .transition_task(&id, TaskState::Running)
            .await
            .expect("to running");
        overlord
            .transition_task(&id, TaskState::Success)
            .await
            .expect("to success");
        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.state, TaskState::Success);
    }

    #[tokio::test]
    async fn legacy_update_status_maps_to_machine() {
        let (_store, overlord) = setup().await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        // Legacy API: PENDING -> RUNNING valid, RUNNING -> PENDING valid.
        overlord
            .update_task_status(&id, TaskStatus::Running)
            .await
            .expect("running");
        let t = overlord.get_task(&id).await.expect("g").expect("s");
        assert_eq!(t.status, TaskStatus::Running);
        assert_eq!(t.state, TaskState::Running);
    }

    // ----- locks via the Overlord -----

    #[tokio::test]
    async fn overlord_lock_grant_and_conflict() {
        let (_store, overlord) = setup().await;
        let interval = Interval::new(0, 1000).expect("iv");
        let granted = overlord
            .acquire_lock("t1", "wiki", interval, LockType::Exclusive, 5)
            .await
            .expect("acquire");
        assert!(granted.is_some(), "first lock should grant");

        // Overlapping exclusive at equal priority is blocked.
        let blocked = overlord
            .acquire_lock(
                "t2",
                "wiki",
                Interval::new(500, 1500).expect("iv"),
                LockType::Exclusive,
                5,
            )
            .await
            .expect("acquire");
        assert!(blocked.is_none(), "equal-priority overlap is blocked");

        let active = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert_eq!(active.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_exclusive_lock_grants_at_most_one() {
        // DD R10 #1: without serialization of the read → evaluate → insert
        // sequence, two concurrent overlapping EXCLUSIVE requests can both read
        // an empty active set and both be granted. Fire many concurrent rounds
        // to make the race highly likely to manifest if unserialized.
        for round in 0..32 {
            let (_store, overlord) = setup().await;
            let overlord = Arc::new(overlord);
            let o1 = Arc::clone(&overlord);
            let o2 = Arc::clone(&overlord);
            let iv_a = Interval::new(0, 1000).expect("iv");
            let iv_b = Interval::new(500, 1500).expect("iv");
            let h1 = tokio::spawn(async move {
                o1.acquire_lock("t1", "wiki", iv_a, LockType::Exclusive, 5)
                    .await
            });
            let h2 = tokio::spawn(async move {
                o2.acquire_lock("t2", "wiki", iv_b, LockType::Exclusive, 5)
                    .await
            });
            let (r1, r2) = tokio::join!(h1, h2);
            let g1 = r1.expect("join1").expect("acquire1").is_some();
            let g2 = r2.expect("join2").expect("acquire2").is_some();
            assert!(
                g1 ^ g2,
                "round {round}: exactly one overlapping exclusive lock must be granted (g1={g1}, g2={g2})"
            );
            let active = overlord.locks_for_datasource("wiki").await.expect("locks");
            assert_eq!(
                active.len(),
                1,
                "round {round}: only one active exclusive lock expected"
            );
        }
    }

    #[tokio::test]
    async fn overlord_lock_preemption() {
        let (_store, overlord) = setup().await;
        // Low-priority lock first.
        overlord
            .acquire_lock(
                "low",
                "wiki",
                Interval::new(0, 1000).expect("iv"),
                LockType::Exclusive,
                1,
            )
            .await
            .expect("low")
            .expect("granted");

        // High-priority overlapping lock preempts it.
        let high = overlord
            .acquire_lock(
                "high",
                "wiki",
                Interval::new(0, 1000).expect("iv"),
                LockType::Exclusive,
                9,
            )
            .await
            .expect("high");
        assert!(high.is_some(), "higher priority preempts");

        // Only the high lock is active; the low one is revoked.
        let active = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].task_id, "high");
    }

    #[tokio::test]
    async fn shared_locks_coexist() {
        let (_store, overlord) = setup().await;
        let a = overlord
            .acquire_lock(
                "a",
                "wiki",
                Interval::new(0, 1000).expect("iv"),
                LockType::Shared,
                5,
            )
            .await
            .expect("a");
        let b = overlord
            .acquire_lock(
                "b",
                "wiki",
                Interval::new(0, 1000).expect("iv"),
                LockType::Shared,
                5,
            )
            .await
            .expect("b");
        assert!(a.is_some() && b.is_some(), "two shared locks coexist");
        let active = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert_eq!(active.len(), 2);
    }

    #[tokio::test]
    async fn terminal_releases_locks() {
        let (_store, overlord) = setup().await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        overlord
            .acquire_lock(
                &id,
                "wiki",
                Interval::new(0, 1000).expect("iv"),
                LockType::Exclusive,
                5,
            )
            .await
            .expect("lock")
            .expect("granted");
        assert_eq!(
            overlord
                .locks_for_datasource("wiki")
                .await
                .expect("l")
                .len(),
            1
        );

        // Drive to a terminal state; the lock must be released.
        overlord
            .transition_task(&id, TaskState::Running)
            .await
            .expect("running");
        overlord
            .transition_task(&id, TaskState::Failed)
            .await
            .expect("failed");
        assert!(
            overlord
                .locks_for_datasource("wiki")
                .await
                .expect("l")
                .is_empty(),
            "terminal task releases its locks"
        );
    }

    // ----- retry exhaustion -----

    /// A spec whose inline data is malformed JSONL causes ingestion to error
    /// on every attempt, so a small retry budget must end in FAILED with the
    /// attempt counter at the budget.
    #[tokio::test]
    async fn retry_exhaustion_ends_failed() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), historical)
            .with_retry_policy(RetryPolicy {
                max_attempts: 3,
                base_delay_millis: 1,
                max_delay_millis: 1,
            });

        // Empty inline data is a hard ingestion error.
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": {"inputSource": {"type": "inline", "data": "   "}}
            }
        });
        let id = submit_failed(&overlord, spec).await;
        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.attempt, 3, "exhausted the 3-attempt budget");
    }

    // ----- worker assignment + loss -----

    #[tokio::test]
    async fn worker_assignment_and_loss_reassigns() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let (_store, overlord) = (Arc::clone(&metadata), Overlord::new(metadata));
        overlord
            .register_worker(Worker {
                host: "w1".into(),
                port: 8100,
                capacity: 4,
            })
            .await;
        assert_eq!(overlord.worker_count().await, 1);

        // Manually drive a task to RUNNING on the worker via the lifecycle.
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        overlord
            .transition_task(&id, TaskState::Running)
            .await
            .expect("running");
        // Assign the worker explicitly (in-process bookkeeping).
        {
            let mut tasks = overlord.running_tasks.write().await;
            let t = tasks.get_mut(&id).expect("task");
            t.worker = Some("w1:8100".to_string());
        }

        // Lose the worker; the running task is re-queued to PENDING (budget ok).
        let affected = overlord.lose_worker("w1:8100").await.expect("lose");
        assert_eq!(affected, vec![id.clone()]);
        let t = overlord.get_task(&id).await.expect("g").expect("s");
        assert_eq!(t.state, TaskState::Pending);
        assert!(t.worker.is_none());
        assert_eq!(overlord.worker_count().await, 0);
    }

    #[tokio::test]
    async fn worker_loss_without_budget_fails() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(metadata).with_retry_policy(RetryPolicy {
            max_attempts: 1,
            base_delay_millis: 1,
            max_delay_millis: 1,
        });
        overlord
            .register_worker(Worker {
                host: "w1".into(),
                port: 8100,
                capacity: 1,
            })
            .await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        overlord
            .transition_task(&id, TaskState::Running)
            .await
            .expect("running");
        {
            let mut tasks = overlord.running_tasks.write().await;
            let t = tasks.get_mut(&id).expect("task");
            t.worker = Some("w1:8100".to_string());
            t.attempt = 1; // budget already consumed
        }
        let affected = overlord.lose_worker("w1:8100").await.expect("lose");
        assert_eq!(affected, vec![id.clone()]);
        let t = overlord.get_task(&id).await.expect("g").expect("s");
        assert_eq!(t.state, TaskState::Failed, "no budget left -> FAILED");
    }

    // ----- persistence round-trip -----

    #[tokio::test]
    async fn task_persisted_to_metadata() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");

        // Persisted as PENDING and visible via active-task query.
        let row = metadata.get_task(&id).await.expect("get").expect("some");
        assert_eq!(row.status, "PENDING");
        assert_eq!(row.data_source, "wiki");
        let active = metadata.get_active_tasks().await.expect("active");
        assert_eq!(active.len(), 1);

        // Transition updates the persisted status.
        overlord
            .transition_task(&id, TaskState::Running)
            .await
            .expect("running");
        let row = metadata.get_task(&id).await.expect("get").expect("some");
        assert_eq!(row.status, "RUNNING");
    }

    #[tokio::test]
    async fn submit_with_intervals_acquires_lock() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": {
                    "intervals": ["2024-01-01T00:00:00Z/2024-02-01T00:00:00Z"]
                }
            }
        });
        let _id = overlord.submit_task(spec).await.expect("submit");
        let locks = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert_eq!(locks.len(), 1, "interval spec should acquire one lock");
        assert_eq!(locks[0].lock_type, LockType::Exclusive);
    }

    #[tokio::test]
    async fn submit_with_date_only_interval_acquires_lock() {
        // DD R47: bare YYYY-MM-DD interval bounds (common in batch ingestion
        // specs) must acquire a lock, not be rejected by an RFC3339-only parser.
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": { "intervals": ["2024-01-01/2024-02-01"] }
            }
        });
        overlord
            .submit_task(spec)
            .await
            .expect("submit date-only interval");
        let locks = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert_eq!(
            locks.len(),
            1,
            "date-only interval spec should acquire a lock"
        );
    }

    #[tokio::test]
    async fn submit_with_non_string_interval_fails_closed() {
        // DD R46: a non-string ioConfig.intervals entry previously was silently
        // skipped, so the task acquired NO interval lock (bypassing conflict
        // prevention). It must now fail closed.
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": { "intervals": [123] }
            }
        });
        assert!(
            overlord.submit_task(spec).await.is_err(),
            "a non-string interval must be rejected, not run without a lock"
        );
        // No lock leaked from the rejected submit.
        let locks = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert!(locks.is_empty(), "rejected submit must not acquire a lock");
    }

    #[tokio::test]
    async fn interval_lock_targets_published_datasource_not_id_header() {
        // Re-audit Low: a spec carrying BOTH a legacy top-level
        // `dataSource` and `spec.dataSchema.dataSource` publishes to the
        // LATTER (the field `parse_index_parallel_spec` resolves), so the
        // exclusive interval lock must land there too. Pre-fix the lock
        // was acquired for the ID-derivation datasource (the top-level
        // field when present), so two tasks writing the same REAL
        // datasource + interval never conflicted — the lock guarded a
        // datasource nothing writes.
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));
        let id1 = overlord
            .submit_task(json!({
                "type": "index_parallel",
                "dataSource": "legacy_header_ds",
                "spec": {
                    "dataSchema": {"dataSource": "published_ds"},
                    "ioConfig": {
                        "intervals": ["2024-01-01T00:00:00Z/2024-02-01T00:00:00Z"]
                    }
                }
            }))
            .await
            .expect("submit first");

        // The lock lands on the datasource the publish actually writes...
        let published_locks = overlord
            .locks_for_datasource("published_ds")
            .await
            .expect("locks");
        assert_eq!(
            published_locks.len(),
            1,
            "the interval lock must target the PUBLISHED datasource"
        );
        assert_eq!(published_locks[0].task_id, id1);
        // ...and NOT on the ID-derivation header datasource.
        let header_locks = overlord
            .locks_for_datasource("legacy_header_ds")
            .await
            .expect("locks");
        assert!(
            header_locks.is_empty(),
            "no lock may land on the unwritten header datasource: {header_locks:?}"
        );

        // A second task writing the SAME published datasource over an
        // overlapping interval genuinely conflicts (stub path: persisted
        // WAITING, no second lock granted).
        let id2 = overlord
            .submit_task(json!({
                "type": "index_parallel",
                "spec": {
                    "dataSchema": {"dataSource": "published_ds"},
                    "ioConfig": {
                        "intervals": ["2024-01-15T00:00:00Z/2024-01-20T00:00:00Z"]
                    }
                }
            }))
            .await
            .expect("submit second");
        let t2 = overlord.get_task(&id2).await.expect("get").expect("some");
        assert_eq!(
            t2.state,
            TaskState::Waiting,
            "an overlapping task on the published datasource must be blocked"
        );
        let locks_after = overlord
            .locks_for_datasource("published_ds")
            .await
            .expect("locks");
        assert_eq!(
            locks_after.len(),
            1,
            "still only the first task's lock: {locks_after:?}"
        );
    }

    // ----- appendToExisting replace/append semantics (P1-#1, 2026-07-12) -----

    /// `parse_index_parallel_spec` reads `ioConfig.appendToExisting` with
    /// Druid's native-batch default of FALSE, and fails closed on
    /// non-boolean values.
    #[test]
    fn parse_spec_reads_append_to_existing() {
        let base = |io_extra: serde_json::Value| {
            let mut io_config = json!({
                "inputSource": {
                    "type": "inline",
                    "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"d\":\"x\"}"
                }
            });
            if let (Some(obj), Some(extra)) = (io_config.as_object_mut(), io_extra.as_object()) {
                for (k, v) in extra {
                    obj.insert(k.clone(), v.clone());
                }
            }
            json!({
                "type": "index_parallel",
                "spec": {
                    "dataSchema": {
                        "dataSource": "t",
                        "timestampSpec": {"column": "timestamp", "format": "iso"},
                        "dimensionsSpec": {"dimensions": ["d"]},
                        "metricsSpec": []
                    },
                    "ioConfig": io_config
                }
            })
        };

        // Absent -> false (Druid's ioConfig default for native batch).
        let p = parse_index_parallel_spec(&base(json!({})))
            .expect("parse")
            .expect("supported");
        assert!(!p.append_to_existing, "absent appendToExisting -> false");

        // Explicit true / false honored.
        let p = parse_index_parallel_spec(&base(json!({"appendToExisting": true})))
            .expect("parse")
            .expect("supported");
        assert!(p.append_to_existing);
        let p = parse_index_parallel_spec(&base(json!({"appendToExisting": false})))
            .expect("parse")
            .expect("supported");
        assert!(!p.append_to_existing);

        // Non-boolean fails closed.
        let err = parse_index_parallel_spec(&base(json!({"appendToExisting": "yes"})))
            .expect_err("non-bool appendToExisting must be rejected");
        assert!(format!("{err}").contains("appendToExisting"));
    }

    // ----- timestampSpec.format threading (compat-9 P0, 2026-07-18) -----

    /// Build a minimal inline native-batch spec: `ts_format` (when `Some`)
    /// becomes `timestampSpec.format`, and `schema_extra` object keys are
    /// merged into `dataSchema` (used by the format / transformSpec tests).
    fn fmt_spec(
        ts_format: Option<serde_json::Value>,
        schema_extra: serde_json::Value,
    ) -> serde_json::Value {
        let mut ts_spec = json!({"column": "timestamp"});
        if let Some(f) = ts_format {
            ts_spec["format"] = f;
        }
        let mut data_schema = json!({
            "dataSource": "t",
            "timestampSpec": ts_spec,
            "dimensionsSpec": {"dimensions": ["d"]},
            "metricsSpec": []
        });
        if let (Some(obj), Some(extra)) = (data_schema.as_object_mut(), schema_extra.as_object()) {
            for (k, v) in extra {
                obj.insert(k.clone(), v.clone());
            }
        }
        json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": data_schema,
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"d\":\"x\"}"
                    }
                }
            }
        })
    }

    /// Formats the extractor does not implement (posix / nano / ruby /
    /// custom Joda patterns) fail LOUDLY at spec-parse time, exactly like
    /// the Kafka/Kinesis streaming validation. Pre-fix they were silently
    /// parsed as `auto`, storing WRONG instants (a posix-seconds value
    /// read as millis lands in 1970).
    #[test]
    fn parse_spec_rejects_unsupported_timestamp_format() {
        for bad in ["posix", "nano", "ruby", "yyyy-MM-dd HH:mm:ss"] {
            let err = parse_index_parallel_spec(&fmt_spec(Some(json!(bad)), json!({})))
                .expect_err(&format!("format {bad:?} must be rejected"));
            let msg = format!("{err}");
            assert!(
                msg.contains("unsupported timestampSpec.format"),
                "error must name the unsupported format: {msg}"
            );
        }
        // A non-string format is malformed — fail closed, never guess.
        let err = parse_index_parallel_spec(&fmt_spec(Some(json!(5)), json!({})))
            .expect_err("non-string timestampSpec.format must be rejected");
        assert!(format!("{err}").contains("timestampSpec.format"));
    }

    /// The declared `timestampSpec.format` lands in
    /// `ParsedIndexSpec.timestamp_format` (threaded to the
    /// `BatchIngester` at the execute site) — `iso`/`millis` map to their
    /// strict grammars, absent/null defaults to Druid's `auto`, and the
    /// mapping is case-insensitive like the streaming paths.
    #[test]
    fn parse_spec_threads_timestamp_format() {
        let parse = |f: Option<serde_json::Value>| {
            parse_index_parallel_spec(&fmt_spec(f, json!({})))
                .expect("parse ok")
                .expect("supported shape")
                .timestamp_format
        };
        assert_eq!(parse(Some(json!("iso"))), TsFormat::Iso);
        assert_eq!(parse(Some(json!("millis"))), TsFormat::Millis);
        assert_eq!(parse(Some(json!("MILLIS"))), TsFormat::Millis);
        assert_eq!(parse(Some(json!("auto"))), TsFormat::Auto);
        assert_eq!(parse(None), TsFormat::Auto, "absent format -> auto");
        assert_eq!(parse(Some(json!(null))), TsFormat::Auto, "null -> auto");
    }

    /// A `transformSpec` with content is NOT applied by native-batch
    /// ingestion — pre-fix it was silently dropped, so rows were ingested
    /// unfiltered/untransformed. Reject it loudly (mirroring the Kafka
    /// streaming rejection); a semantically-EMPTY transformSpec is inert
    /// and stays accepted so real-world no-op specs keep working.
    #[test]
    fn parse_spec_rejects_transform_spec() {
        let filtering = json!({
            "transformSpec": {
                "filter": {"type": "selector", "dimension": "d", "value": "x"}
            }
        });
        let err = parse_index_parallel_spec(&fmt_spec(None, filtering))
            .expect_err("a transformSpec with content must be rejected");
        assert!(format!("{err}").contains("transformSpec"));
        let transforming = json!({
            "transformSpec": {
                "transforms": [{"type": "expression", "name": "d2", "expression": "upper(d)"}]
            }
        });
        let err = parse_index_parallel_spec(&fmt_spec(None, transforming))
            .expect_err("a transformSpec with transforms must be rejected");
        assert!(format!("{err}").contains("transformSpec"));
        // Inert shapes: null / {} / the empty form Druid tooling emits.
        for inert in [
            json!(null),
            json!({}),
            json!({"transforms": [], "filter": null}),
        ] {
            parse_index_parallel_spec(&fmt_spec(None, json!({"transformSpec": inert})))
                .expect("inert transformSpec parses")
                .expect("supported shape");
        }
    }

    /// compat-9 P0 (E2E): a declared `format: "iso"` is honored by batch
    /// EXECUTION — `"2023"` is the ISO YEAR 2023. Pre-fix the format was
    /// ignored (always `auto`), which read `"2023"` as 2023 MILLISECONDS
    /// past the epoch and silently stored 1970-01-01T00:00:02.023Z.
    #[tokio::test]
    async fn batch_iso_format_reads_bare_year_as_year() {
        let (metadata, historical, _cache, overlord) = executor_setup().await;
        let spec = json!({
            "type": "index",
            "spec": {
                "dataSchema": {
                    "dataSource": "fmt_iso",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "metricsSpec": [],
                    "granularitySpec": {"rollup": false}
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2023\",\"page\":\"Main\"}"
                    }
                }
            }
        });
        let _id = submit_ok(&overlord, spec).await;
        assert_eq!(historical.loaded_segments().len(), 1);
        let rows = metadata
            .get_used_segments("fmt_iso")
            .await
            .expect("used rows");
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0].start.starts_with("2023-01-01T00:00:00"),
            "format iso must read \"2023\" as the YEAR 2023, got start {}",
            rows[0].start
        );
    }

    /// Segment-granularity parsing + replace-scope bucket math.
    #[test]
    fn segment_granularity_replace_scope_math() {
        let day = MILLIS_PER_DAY;
        // 2026-01-01T00:00:01Z .. 2026-01-01T00:00:08Z
        let t1 = 1_767_225_601_000;
        let t8 = 1_767_225_608_000;
        let day_start = 1_767_225_600_000; // 2026-01-01T00:00:00Z

        // Default (absent) -> DAY buckets.
        let g = parse_segment_granularity(None).expect("default");
        assert_eq!(
            replace_scope(g, t1, t8).expect("scope"),
            (day_start, day_start + day),
            "absent segmentGranularity defaults to DAY"
        );

        // Case-insensitive string + period-object forms.
        let g = parse_segment_granularity(Some(&json!("DAY"))).expect("DAY");
        assert_eq!(replace_scope(g, t1, t8).expect("scope").0, day_start);
        let g = parse_segment_granularity(Some(&json!({"type": "period", "period": "P1D"})))
            .expect("P1D");
        assert_eq!(replace_scope(g, t1, t8).expect("scope").0, day_start);

        // HOUR: data crossing an hour boundary widens the scope.
        let g = parse_segment_granularity(Some(&json!("hour"))).expect("hour");
        let t_0130 = day_start + 5_400_000; // 01:30
        assert_eq!(
            replace_scope(g, t1, t_0130).expect("scope"),
            (day_start, day_start + 7_200_000),
            "00:00:01..01:30 covers the 00:00 and 01:00 hour buckets"
        );

        // WEEK: 2026-01-01 is a Thursday; its ISO week starts Mon 2025-12-29.
        let g = parse_segment_granularity(Some(&json!("WEEK"))).expect("week");
        let monday = day_start - 3 * day; // 2025-12-29T00:00:00Z
        assert_eq!(
            replace_scope(g, t1, t8).expect("scope"),
            (monday, monday + 7 * day)
        );

        // MONTH / QUARTER / YEAR calendar buckets.
        let feb_start = 1_769_904_000_000; // 2026-02-01T00:00:00Z
        let g = parse_segment_granularity(Some(&json!("MONTH"))).expect("month");
        assert_eq!(
            replace_scope(g, t1, t8).expect("scope"),
            (day_start, feb_start)
        );
        let apr_start = 1_775_001_600_000; // 2026-04-01T00:00:00Z
        let g = parse_segment_granularity(Some(&json!("QUARTER"))).expect("quarter");
        assert_eq!(
            replace_scope(g, t1, t8).expect("scope"),
            (day_start, apr_start)
        );
        let y2027 = 1_798_761_600_000; // 2027-01-01T00:00:00Z
        let g = parse_segment_granularity(Some(&json!("YEAR"))).expect("year");
        assert_eq!(replace_scope(g, t1, t8).expect("scope"), (day_start, y2027));

        // ALL covers everything.
        let g = parse_segment_granularity(Some(&json!("ALL"))).expect("all");
        assert_eq!(
            replace_scope(g, t1, t8).expect("scope"),
            (i64::MIN, i64::MAX)
        );

        // Unsupported shapes fail closed.
        assert!(parse_segment_granularity(Some(&json!("fortnight"))).is_err());
        assert!(parse_segment_granularity(Some(&json!(42))).is_err());
        assert!(
            parse_segment_granularity(Some(&json!({"type": "period", "period": "P2D"}))).is_err()
        );
    }

    /// Build an executor-backed Overlord plus its Historical + metadata.
    async fn setup_executor() -> (
        Arc<MetadataStore>,
        Arc<ferrodruid_historical::Historical>,
        Overlord,
        tempfile::TempDir,
    ) {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        (metadata, historical, overlord, cache_dir)
    }

    /// An `index_parallel` spec shaped like the 2026-07-12 audit fixtures:
    /// DAY segment granularity, rollup off, explicit `appendToExisting`.
    fn batch_spec(
        ds: &str,
        append_to_existing: serde_json::Value,
        data: &str,
    ) -> serde_json::Value {
        let mut io_config = json!({
            "type": "index_parallel",
            "inputSource": {"type": "inline", "data": data},
            "inputFormat": {"type": "json"}
        });
        if !append_to_existing.is_null()
            && let Some(obj) = io_config.as_object_mut()
        {
            obj.insert("appendToExisting".to_string(), append_to_existing);
        }
        json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": ds,
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["grp", "uid"]},
                    "metricsSpec": [{"type": "longSum", "name": "a", "fieldName": "a"}],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "DAY",
                        "queryGranularity": "NONE",
                        "rollup": false
                    }
                },
                "ioConfig": io_config
            }
        })
    }

    /// 4 rows at 2026-01-01T00:00:01..04Z (the audit's specA data).
    fn rows_first() -> &'static str {
        "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"grp\":\"x\",\"uid\":\"u1\",\"a\":10}\n\
         {\"timestamp\":\"2026-01-01T00:00:02Z\",\"grp\":\"x\",\"uid\":\"u2\",\"a\":20}\n\
         {\"timestamp\":\"2026-01-01T00:00:03Z\",\"grp\":\"y\",\"uid\":\"u1\",\"a\":30}\n\
         {\"timestamp\":\"2026-01-01T00:00:04Z\",\"grp\":\"y\",\"uid\":\"u2\",\"a\":40}"
    }

    /// 4 rows at 2026-01-01T00:00:05..08Z — same DAY bucket as
    /// [`rows_first`] but a disjoint raw-millisecond range (the audit's
    /// specB data), so a Druid-faithful replace must be bucket-scoped,
    /// not raw-data-interval-scoped.
    fn rows_second() -> &'static str {
        "{\"timestamp\":\"2026-01-01T00:00:05Z\",\"grp\":\"x\",\"uid\":\"u2\",\"a\":100}\n\
         {\"timestamp\":\"2026-01-01T00:00:06Z\",\"grp\":\"x\",\"uid\":\"u3\",\"a\":200}\n\
         {\"timestamp\":\"2026-01-01T00:00:07Z\",\"grp\":\"y\",\"uid\":\"u4\",\"a\":300}\n\
         {\"timestamp\":\"2026-01-01T00:00:08Z\",\"grp\":\"y\",\"uid\":\"u2\",\"a\":400}"
    }

    /// Run a `count` timeseries over the full time range through the real
    /// query path ([`Historical::execute_query`]) and sum across segments —
    /// this is what a SQL `COUNT(*)` ultimately observes, so it proves
    /// replaced rows are gone from query results, not just from metadata.
    fn queried_row_count(historical: &ferrodruid_historical::Historical, ds: &str) -> i64 {
        let query: ferrodruid_query::DruidQuery = serde_json::from_value(json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": ds},
            "intervals": ["2000-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z"],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("build count query");
        let results = historical
            .execute_query(&query)
            .expect("execute count query");
        results
            .iter()
            .map(|r| match r {
                ferrodruid_query::QueryResult::Timeseries(ts) => ts
                    .iter()
                    .map(|row| {
                        row.result
                            .get("cnt")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0)
                    })
                    .sum::<i64>(),
                _ => 0,
            })
            .sum()
    }

    /// P1-#1 (2026-07-12): re-ingesting an interval with
    /// `appendToExisting: false` (Druid's batch default) must REPLACE the
    /// interval's existing segments, not silently duplicate them.
    ///
    /// Measured live against Druid 36: two same-DAY-bucket tasks with
    /// `appendToExisting: false` leave COUNT(*) = 4 (second task's rows
    /// only); pre-fix FerroDruid kept 8 rows across 2 coexisting segments.
    ///
    /// The second task's rows live at 00:00:05..08Z while the first task's
    /// are at 00:00:01..04Z — disjoint raw ranges inside the same DAY
    /// bucket — so this also locks in segmentGranularity-bucket scoping.
    #[tokio::test]
    async fn replace_semantics_same_day_bucket_overwrites() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        // Sequential submit-and-poll keeps the replace ordering
        // deterministic under the async contract.
        let _id1 = submit_ok(
            &overlord,
            batch_spec("replace_ds", json!(false), rows_first()),
        )
        .await;
        assert_eq!(queried_row_count(&historical, "replace_ds"), 4);

        let _id2 = submit_ok(
            &overlord,
            batch_spec("replace_ds", json!(false), rows_second()),
        )
        .await;

        // Druid semantics: the second appendToExisting:false task replaces
        // the DAY bucket — 4 rows, 1 segment, and only the new rows.
        assert_eq!(
            queried_row_count(&historical, "replace_ds"),
            4,
            "replace must not double-count: expected 4 rows (second task only)"
        );
        assert_eq!(
            historical.segment_count(),
            1,
            "old segment must be dropped from the Historical"
        );

        // Metadata: exactly one used segment; the old one is retained as
        // used=false (Druid keeps replaced segments as unused rows).
        let used = metadata
            .get_used_segments("replace_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), 1, "exactly one used segment after replace");
        let all = metadata.get_all_segments().await.expect("all segments");
        assert_eq!(all.len(), 2, "replaced segment row is kept (unused)");
        assert_eq!(
            all.iter().filter(|s| !s.used).count(),
            1,
            "exactly one unused (replaced) segment row"
        );

        // The surviving segment holds the SECOND task's rows (sum(a)=1000).
        let seg = historical
            .get_segment(&historical.loaded_segments()[0])
            .expect("get_segment")
            .expect("segment data");
        match seg.columns.get("a") {
            Some(ferrodruid_segment::column::ColumnData::Double(v)) => {
                assert_eq!(
                    v.iter().sum::<f64>(),
                    1000.0,
                    "second task's rows survive (100+200+300+400)"
                );
            }
            other => panic!("metric column 'a' should be Double, got {other:?}"),
        }
    }

    /// `appendToExisting: true` keeps the pre-existing behavior: both
    /// segments coexist and COUNT doubles (Druid parity: 8 rows).
    #[tokio::test]
    async fn append_true_keeps_both_segments() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        submit_ok(
            &overlord,
            batch_spec("append_ds", json!(false), rows_first()),
        )
        .await;
        submit_ok(
            &overlord,
            batch_spec("append_ds", json!(true), rows_second()),
        )
        .await;

        assert_eq!(queried_row_count(&historical, "append_ds"), 8);
        assert_eq!(historical.segment_count(), 2);
        let used = metadata
            .get_used_segments("append_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), 2, "append keeps both segments used");
    }

    /// Replace is scoped to the overlapping segmentGranularity buckets:
    /// an `appendToExisting: false` task for a DIFFERENT day must not
    /// touch the first day's segment.
    #[tokio::test]
    async fn replace_disjoint_day_buckets_keeps_both() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        submit_ok(&overlord, batch_spec("days_ds", json!(false), rows_first())).await;
        let day2 = "{\"timestamp\":\"2026-01-02T00:00:01Z\",\"grp\":\"x\",\"uid\":\"u1\",\"a\":1}\n\
             {\"timestamp\":\"2026-01-02T00:00:02Z\",\"grp\":\"y\",\"uid\":\"u2\",\"a\":2}";
        submit_ok(&overlord, batch_spec("days_ds", json!(false), day2)).await;

        assert_eq!(
            queried_row_count(&historical, "days_ds"),
            6,
            "disjoint DAY buckets must both survive an appendToExisting:false task"
        );
        assert_eq!(historical.segment_count(), 2);
        assert_eq!(
            metadata
                .get_used_segments("days_ds")
                .await
                .expect("used")
                .len(),
            2
        );
    }

    /// Replace is scoped per datasource: an `appendToExisting: false` task
    /// for datasource B must never drop datasource A's segments even when
    /// the intervals coincide exactly.
    #[tokio::test]
    async fn replace_does_not_cross_datasources() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        submit_ok(&overlord, batch_spec("ds_a", json!(false), rows_first())).await;
        submit_ok(&overlord, batch_spec("ds_b", json!(false), rows_first())).await;

        assert_eq!(queried_row_count(&historical, "ds_a"), 4);
        assert_eq!(queried_row_count(&historical, "ds_b"), 4);
        assert_eq!(historical.segment_count(), 2);
        assert_eq!(
            metadata.get_used_segments("ds_a").await.expect("a").len(),
            1
        );
        assert_eq!(
            metadata.get_used_segments("ds_b").await.expect("b").len(),
            1
        );
    }

    /// An absent `appendToExisting` defaults to FALSE (Druid's native-batch
    /// `ioConfig` default) — re-submitting the same-bucket spec without the
    /// field must replace, not append.
    #[tokio::test]
    async fn append_to_existing_defaults_to_false() {
        let (_metadata, historical, overlord, _dir) = setup_executor().await;

        submit_ok(
            &overlord,
            batch_spec("default_ds", serde_json::Value::Null, rows_first()),
        )
        .await;
        submit_ok(
            &overlord,
            batch_spec("default_ds", serde_json::Value::Null, rows_second()),
        )
        .await;

        assert_eq!(
            queried_row_count(&historical, "default_ds"),
            4,
            "absent appendToExisting must default to false (replace)"
        );
        assert_eq!(historical.segment_count(), 1);
    }

    /// A non-boolean `appendToExisting` fails closed instead of being
    /// silently coerced.
    #[tokio::test]
    async fn append_to_existing_non_bool_fails_closed() {
        let (_metadata, historical, overlord, _dir) = setup_executor().await;
        // Non-boolean appendToExisting must fail the task (polled).
        let _id = submit_failed(&overlord, batch_spec("bad_ds", json!("yes"), rows_first())).await;
        assert!(historical.loaded_segments().is_empty());
    }

    #[tokio::test]
    async fn shutdown_idempotent_on_terminal() {
        let (_store, overlord) = setup().await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        overlord.shutdown_task(&id).await.expect("shutdown");
        let t = overlord.get_task(&id).await.expect("g").expect("s");
        assert_eq!(t.state, TaskState::Failed);
        // Second shutdown is a no-op (still found, still FAILED).
        overlord.shutdown_task(&id).await.expect("shutdown again");
        let t = overlord.get_task(&id).await.expect("g").expect("s");
        assert_eq!(t.state, TaskState::Failed);
    }

    /// Codex 2026-07-12 HIGH #1: the ingester publishes ONE raw
    /// `[min, max+1)` segment even when its rows span multiple
    /// segmentGranularity buckets, so an existing segment can extend
    /// OUTSIDE a later task's replace scope. Pre-fix, such a victim was
    /// dropped WHOLESALE: replacing only Jan-1 silently deleted the
    /// victim's Jan-2 rows. The fix fails the task CLOSED and drops
    /// nothing.
    #[tokio::test]
    async fn replace_partial_overlap_fails_closed_and_preserves_data() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        // One task whose rows span TWO DAY buckets -> one raw segment
        // [2026-01-01T00:00:01, 2026-01-02T00:00:02.001) crossing the
        // bucket boundary.
        let two_day_rows = "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"grp\":\"x\",\"uid\":\"u1\",\"a\":10}\n\
             {\"timestamp\":\"2026-01-01T00:00:02Z\",\"grp\":\"x\",\"uid\":\"u2\",\"a\":20}\n\
             {\"timestamp\":\"2026-01-02T00:00:01Z\",\"grp\":\"y\",\"uid\":\"u3\",\"a\":30}\n\
             {\"timestamp\":\"2026-01-02T00:00:02Z\",\"grp\":\"y\",\"uid\":\"u4\",\"a\":40}";
        let _id1 = submit_ok(&overlord, batch_spec("span_ds", json!(false), two_day_rows)).await;
        assert_eq!(queried_row_count(&historical, "span_ds"), 4);
        let original_used = metadata
            .get_used_segments("span_ds")
            .await
            .expect("used segments");
        assert_eq!(original_used.len(), 1);
        let original_id = original_used[0].id.clone();

        // Replace ONLY Jan-1: the scope is [Jan-1, Jan-2) but the existing
        // segment extends into Jan-2 — a partial overlap.
        let jan1_only = "{\"timestamp\":\"2026-01-01T00:00:05Z\",\"grp\":\"x\",\"uid\":\"u5\",\"a\":100}\n\
             {\"timestamp\":\"2026-01-01T00:00:06Z\",\"grp\":\"x\",\"uid\":\"u6\",\"a\":200}\n\
             {\"timestamp\":\"2026-01-01T00:00:07Z\",\"grp\":\"y\",\"uid\":\"u7\",\"a\":300}";
        // A partial-overlap replace must fail closed (polled FAILED).
        let _id2 = submit_failed(&overlord, batch_spec("span_ds", json!(false), jan1_only)).await;

        // Nothing was dropped: all 4 original rows — including the
        // out-of-scope Jan-2 rows — are still queryable, the segment is
        // still loaded, and its metadata row is still used.
        assert_eq!(
            queried_row_count(&historical, "span_ds"),
            4,
            "out-of-scope rows must survive a failed partial-overlap replace"
        );
        assert_eq!(historical.segment_count(), 1);
        let used = metadata
            .get_used_segments("span_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), 1, "victim must stay used after the abort");
        assert_eq!(used[0].id, original_id);

        // Remediation: re-ingesting data covering the FULL spanned interval
        // replaces cleanly (the victim is then fully contained in scope).
        let full_span = "{\"timestamp\":\"2026-01-01T00:00:05Z\",\"grp\":\"x\",\"uid\":\"u5\",\"a\":100}\n\
             {\"timestamp\":\"2026-01-02T00:00:05Z\",\"grp\":\"y\",\"uid\":\"u6\",\"a\":200}";
        let _id3 = submit_ok(&overlord, batch_spec("span_ds", json!(false), full_span)).await;
        assert_eq!(queried_row_count(&historical, "span_ds"), 2);
        assert_eq!(historical.segment_count(), 1);
    }

    /// Codex 2026-07-12 HIGH #2: pre-fix the publication sequence was
    /// drop victims -> mark unused -> load new -> insert metadata row, so
    /// an `insert_segment` failure left the new segment query-visible but
    /// unregistered (an orphan) and the victims dropped + unused (a lost
    /// interval). The fix writes metadata first and rolls it back on
    /// failure; the query-visible state is never touched on the failure
    /// path.
    #[tokio::test]
    async fn metadata_insert_failure_rolls_back_without_orphan() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        // Two coexisting segments in the same DAY bucket -> the replace
        // below has TWO victims.
        submit_ok(&overlord, batch_spec("rb_ds", json!(false), rows_first())).await;
        submit_ok(&overlord, batch_spec("rb_ds", json!(true), rows_second())).await;
        assert_eq!(queried_row_count(&historical, "rb_ds"), 8);
        let victims_before: std::collections::HashSet<String> = metadata
            .get_used_segments("rb_ds")
            .await
            .expect("used segments")
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(victims_before.len(), 2);

        // Inject: every new-segment metadata insert fails (sticky across
        // the task's retry attempts).
        overlord
            .inject_insert_segment_failure
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let replacement = "{\"timestamp\":\"2026-01-01T00:00:09Z\",\"grp\":\"z\",\"uid\":\"u9\",\"a\":1}\n\
             {\"timestamp\":\"2026-01-01T00:00:10Z\",\"grp\":\"z\",\"uid\":\"u10\",\"a\":2}";
        // Task must fail (polled) when segment registration fails.
        let _id = submit_failed(&overlord, batch_spec("rb_ds", json!(false), replacement)).await;

        // No query-visible orphan and no lost interval: the historical
        // still serves exactly the ORIGINAL 8 rows from 2 segments, and
        // the victims' metadata rows are restored to used=true.
        assert_eq!(
            queried_row_count(&historical, "rb_ds"),
            8,
            "a failed publication must not lose the replaced interval or \
             expose an unregistered orphan segment"
        );
        assert_eq!(historical.segment_count(), 2);
        let used_after: std::collections::HashSet<String> = metadata
            .get_used_segments("rb_ds")
            .await
            .expect("used segments")
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(
            used_after, victims_before,
            "victims must be restored to used=true after the rollback"
        );

        // Recovery: clear the fault and re-submit — the replace completes
        // cleanly, proving the rollback left no poisoned state behind.
        overlord
            .inject_insert_segment_failure
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let _id2 = submit_ok(&overlord, batch_spec("rb_ds", json!(false), replacement)).await;
        assert_eq!(queried_row_count(&historical, "rb_ds"), 2);
        assert_eq!(historical.segment_count(), 1);
        assert_eq!(
            metadata
                .get_used_segments("rb_ds")
                .await
                .expect("used segments")
                .len(),
            1
        );
    }

    /// Codex 2026-07-12 HIGH #3: a multi-victim replace must be atomic to
    /// concurrent queries. Pre-fix, victims were dropped one lock
    /// acquisition at a time (with awaits between) before the replacement
    /// loaded, so a concurrent count could observe only SOME of the old
    /// segments. With `Historical::replace_segments` the whole swap
    /// happens under one write-lock acquisition, and `execute_query`
    /// holds the read lock for its entire run — so a reader hammering
    /// counts across repeated append+replace cycles may only ever observe
    /// the full pre-state (7 rows: 4 + 3 appended) or the full post-state
    /// (4 rows), never a partial mix (0, 3, 8, 11, ...).
    #[tokio::test]
    async fn multi_victim_replace_never_shows_partial_state_to_queries() {
        let (_metadata, historical, overlord, _dir) = setup_executor().await;

        // Seed: 4 rows -> count 4.
        submit_ok(
            &overlord,
            batch_spec("atomic_ds", json!(false), rows_first()),
        )
        .await;

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let violations = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
        let reader = {
            let hist = Arc::clone(&historical);
            let stop = Arc::clone(&stop);
            let violations = Arc::clone(&violations);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    let n = queried_row_count(&hist, "atomic_ds");
                    if n != 4 && n != 7 {
                        violations.lock().expect("violations lock").push(n);
                    }
                }
            })
        };

        // 3 rows in the same DAY bucket: each append makes the datasource
        // a two-victim (4-row + 3-row) target for the following replace.
        let append3 = "{\"timestamp\":\"2026-01-01T00:01:00Z\",\"grp\":\"p\",\"uid\":\"v1\",\"a\":1}\n\
             {\"timestamp\":\"2026-01-01T00:01:01Z\",\"grp\":\"p\",\"uid\":\"v2\",\"a\":2}\n\
             {\"timestamp\":\"2026-01-01T00:01:02Z\",\"grp\":\"p\",\"uid\":\"v3\",\"a\":3}";
        for _ in 0..15 {
            submit_ok(&overlord, batch_spec("atomic_ds", json!(true), append3)).await;
            submit_ok(
                &overlord,
                batch_spec("atomic_ds", json!(false), rows_second()),
            )
            .await;
        }
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        reader.join().expect("reader thread");
        let seen = violations.lock().expect("violations lock").clone();
        assert!(
            seen.is_empty(),
            "concurrent queries observed partial replace states: {seen:?}"
        );
        assert_eq!(queried_row_count(&historical, "atomic_ds"), 4);
    }

    /// Codex 2026-07-12 round-2 HIGH #2/#3: the publication critical
    /// section must serialize on the SHARED store-level datasource publish
    /// lock — the same mutex the Coordinator's `disable_segment` /
    /// `disable_datasource` take. While that lock is held (here directly;
    /// in production by an in-flight admin mutation) a publish must BLOCK
    /// rather than interleave. Pre-fix the Overlord used a private lock
    /// map, so this publish would complete while the store lock was held
    /// and the timeout assertion below would fail.
    #[tokio::test]
    async fn publish_blocks_while_shared_datasource_lock_held() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        let lock = metadata.datasource_publish_lock("lk_ds").await;
        let guard = lock.lock().await;

        // Async contract: the id returns immediately; the detached tail
        // then parks on the held publish lock.
        let id = overlord
            .submit_task(batch_spec("lk_ds", json!(false), rows_first()))
            .await
            .expect("submit");

        // Deterministic: a held tokio::Mutex can never be acquired, so
        // the publish cannot complete — the polled status must stay
        // non-terminal and nothing may become visible while the lock is
        // held.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let info = overlord.get_task(&id).await.expect("get").expect("some");
        assert!(
            !info.state.is_terminal(),
            "the task must block (non-terminal) while the datasource's \
             shared publish lock is held: {info:?}"
        );
        assert_eq!(
            historical.segment_count(),
            0,
            "nothing may become query-visible while the lock is held"
        );
        assert!(
            metadata
                .get_used_segments("lk_ds")
                .await
                .expect("used")
                .is_empty(),
            "no metadata row may be written while the lock is held"
        );

        // Release the lock: the publish completes normally.
        drop(guard);
        assert_eq!(
            await_task_terminal(&overlord, &id).await,
            TaskState::Success,
            "publish succeeds after the lock is released"
        );
        assert_eq!(queried_row_count(&historical, "lk_ds"), 4);
    }

    /// Codex 2026-07-12 round-2 HIGH #4 (deterministic unit): segment-id
    /// allocation must skip ids taken by ANY metadata row (used or
    /// unused) or any loaded segment, appending a numeric suffix. Pre-fix
    /// the id was `format!`ed unconditionally, so both same-millisecond
    /// tasks got the same id and one silently overwrote the other.
    #[tokio::test]
    async fn allocate_segment_id_uniquifies_on_collision() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        let base = "u_ds_2026-01-01T00:00:00.000Z_2026-01-02T00:00:00.000Z_v1";
        let alloc = || {
            overlord.allocate_segment_id(
                &historical,
                "u_ds",
                "2026-01-01T00:00:00.000Z",
                "2026-01-02T00:00:00.000Z",
                "v1",
            )
        };

        // Free -> base id.
        assert_eq!(alloc().await.expect("alloc"), base);

        // Base taken by a USED metadata row -> first suffix.
        let mut row = SegmentMetadataRow {
            id: base.to_string(),
            data_source: "u_ds".to_string(),
            created_date: "2026-01-01T00:00:00Z".to_string(),
            start: "2026-01-01T00:00:00.000Z".to_string(),
            end: "2026-01-02T00:00:00.000Z".to_string(),
            version: "v1".to_string(),
            used: true,
            payload: json!({}),
        };
        metadata.insert_segment(&row).await.expect("seed base");
        assert_eq!(alloc().await.expect("alloc"), format!("{base}_1"));

        // An UNUSED row still occupies its id (INSERT would clobber it).
        row.id = format!("{base}_1");
        row.used = false;
        metadata.insert_segment(&row).await.expect("seed _1");
        assert_eq!(alloc().await.expect("alloc"), format!("{base}_2"));

        // A segment loaded in the Historical without a metadata row (e.g.
        // mid-publish of another task) is also skipped.
        historical
            .load_segment(&format!("{base}_2"), {
                // Any segment data works; reuse the ingester for brevity.
                let ingester = ferrodruid_ingest_batch::BatchIngester::new(
                    "u_ds".to_string(),
                    "timestamp".to_string(),
                    vec!["grp".to_string()],
                    vec![],
                );
                ingester
                    .ingest(vec![json!({
                        "timestamp": "2026-01-01T00:00:00Z",
                        "grp": "x"
                    })])
                    .expect("ingest")
                    .segment_data
            })
            .expect("load");
        assert_eq!(alloc().await.expect("alloc"), format!("{base}_3"));
    }

    /// Codex 2026-07-12 round-2 HIGH #4 (end-to-end): rapid appends over
    /// the same interval — many of which land in the same millisecond and
    /// pre-fix produced COLLIDING segment ids whose rows were silently
    /// discarded — must all survive with distinct ids.
    #[tokio::test]
    async fn rapid_same_interval_appends_lose_no_rows() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        const N: usize = 12;
        let one_row =
            "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"grp\":\"x\",\"uid\":\"u1\",\"a\":1}";
        for _ in 0..N {
            submit_ok(&overlord, batch_spec("rapid_ds", json!(true), one_row)).await;
        }

        assert_eq!(
            queried_row_count(&historical, "rapid_ds"),
            N as i64,
            "every appended row must survive (no same-millisecond id collision loss)"
        );
        assert_eq!(historical.segment_count(), N);
        let used = metadata
            .get_used_segments("rapid_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), N);
        let distinct: std::collections::HashSet<&str> =
            used.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(distinct.len(), N, "all segment ids must be distinct");
    }

    /// Codex 2026-07-12 round-2 HIGH #2 (disposition lock-in): an
    /// admin-disabled segment must never be resurrected by a failing
    /// publish's rollback. With the shared publish lock the disable can
    /// only land fully BEFORE the publish critical section (the disabled
    /// segment is then not read as a victim, hence not restored) or fully
    /// AFTER it — the mid-publish interleaving that caused resurrection
    /// is structurally impossible, which the blocking tests above pin
    /// down. This test locks in the before-ordering end state.
    #[tokio::test]
    async fn disabled_segment_not_resurrected_by_failing_publish_rollback() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        // Two coexisting segments in the same DAY bucket.
        submit_ok(&overlord, batch_spec("dis_ds", json!(false), rows_first())).await;
        submit_ok(&overlord, batch_spec("dis_ds", json!(true), rows_second())).await;
        let used = metadata.get_used_segments("dis_ds").await.expect("used");
        assert_eq!(used.len(), 2);
        let disabled_id = used[0].id.clone();
        let surviving_id = used[1].id.clone();

        // Admin disables one segment (serialized via the shared lock —
        // this is the Coordinator's disable_segment path).
        {
            let lock = metadata.datasource_publish_lock("dis_ds").await;
            let _guard = lock.lock().await;
            metadata
                .mark_segment_unused(&disabled_id)
                .await
                .expect("disable");
        }

        // A replace whose metadata transaction fails: its rollback must
        // NOT touch the admin-disabled row (it was never a victim).
        overlord
            .inject_insert_segment_failure
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _id = submit_failed(&overlord, batch_spec("dis_ds", json!(false), rows_second())).await;
        overlord
            .inject_insert_segment_failure
            .store(false, std::sync::atomic::Ordering::SeqCst);

        let used_after: Vec<String> = metadata
            .get_used_segments("dis_ds")
            .await
            .expect("used")
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert!(
            !used_after.contains(&disabled_id),
            "the admin-disabled segment must NOT be resurrected by the \
             failing publish's rollback, used set: {used_after:?}"
        );
        assert_eq!(
            used_after,
            vec![surviving_id],
            "only the untouched victim remains used"
        );
        assert_eq!(historical.segment_count(), 2, "query state untouched");
    }

    // =======================================================================
    // compat-3 stage 1: deep-storage persistence + bootstrap reload
    // =======================================================================

    /// A [`DeepStorage`] wrapper whose `upload_segment` always fails,
    /// modelling a deep-storage outage during the Phase-P persist so the
    /// crash-order test can assert nothing downstream is committed.
    struct FailingUploadDeepStorage;

    #[async_trait::async_trait]
    impl DeepStorage for FailingUploadDeepStorage {
        async fn list_segments(
            &self,
            _data_source: &str,
        ) -> ferrodruid_deep_storage::Result<Vec<String>> {
            Ok(Vec::new())
        }
        async fn download_segment(
            &self,
            data_source: &str,
            segment_id: &str,
            _dest: &std::path::Path,
        ) -> ferrodruid_deep_storage::Result<()> {
            Err(ferrodruid_deep_storage::DeepStorageError::NotFound {
                data_source: data_source.to_string(),
                segment_id: segment_id.to_string(),
            })
        }
        async fn upload_segment(
            &self,
            _data_source: &str,
            _segment_id: &str,
            _src: &std::path::Path,
        ) -> ferrodruid_deep_storage::Result<()> {
            Err(ferrodruid_deep_storage::DeepStorageError::Other(
                "injected upload failure (test fault hook)".to_string(),
            ))
        }
        async fn delete_segment(
            &self,
            _data_source: &str,
            _segment_id: &str,
        ) -> ferrodruid_deep_storage::Result<()> {
            Ok(())
        }
        async fn segment_exists(
            &self,
            _data_source: &str,
            _segment_id: &str,
        ) -> ferrodruid_deep_storage::Result<bool> {
            Ok(false)
        }
    }

    /// A [`DeepStorage`] that RECORDS every `delete_segment` call (all other
    /// ops are no-ops), so the orphan-blob cleanup decision (H5/H8) is
    /// observable in a unit test.
    #[derive(Default)]
    struct DeleteRecordingDeepStorage {
        deleted: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl DeepStorage for DeleteRecordingDeepStorage {
        async fn list_segments(
            &self,
            _data_source: &str,
        ) -> ferrodruid_deep_storage::Result<Vec<String>> {
            Ok(Vec::new())
        }
        async fn download_segment(
            &self,
            _data_source: &str,
            _segment_id: &str,
            _dest: &std::path::Path,
        ) -> ferrodruid_deep_storage::Result<()> {
            Ok(())
        }
        async fn upload_segment(
            &self,
            _data_source: &str,
            _segment_id: &str,
            _src: &std::path::Path,
        ) -> ferrodruid_deep_storage::Result<()> {
            Ok(())
        }
        async fn delete_segment(
            &self,
            _data_source: &str,
            segment_id: &str,
        ) -> ferrodruid_deep_storage::Result<()> {
            self.deleted
                .lock()
                .expect("lock")
                .push(segment_id.to_string());
            Ok(())
        }
        async fn segment_exists(
            &self,
            _data_source: &str,
            _segment_id: &str,
        ) -> ferrodruid_deep_storage::Result<bool> {
            Ok(false)
        }
    }

    /// H5/H8: the orphan-blob cleanup deletes the Phase-P blob EXACTLY when it
    /// is unreferenced (persisted AND the metadata row is gone), and NEVER when
    /// a rollback left the row still pointing at it (which would create a
    /// phantom).
    #[tokio::test]
    async fn cleanup_orphan_blob_gates_delete_on_metadata_removed() {
        let ds = DeleteRecordingDeepStorage::default();
        let dyn_ds: &dyn DeepStorage = &ds;

        // H8 (metadata txn failed → row never committed) / H5 (rollback OK):
        // persisted + metadata_removed → delete.
        assert!(cleanup_orphan_blob(Some(dyn_ds), "d", "seg_ok", true, true).await);
        // H5 rollback FAILED: the row still references the blob → KEEP it.
        assert!(!cleanup_orphan_blob(Some(dyn_ds), "d", "seg_rollback_fail", true, false).await);
        // No deep-storage backend → no-op.
        assert!(!cleanup_orphan_blob(None, "d", "seg_no_backend", true, true).await);
        // Nothing was persisted → no-op.
        assert!(!cleanup_orphan_blob(Some(dyn_ds), "d", "seg_not_persisted", false, true).await);

        assert_eq!(
            *ds.deleted.lock().expect("lock"),
            vec!["seg_ok".to_string()],
            "only the safe (unreferenced) blob is deleted"
        );
    }

    /// Executor-backed Overlord wired to a real [`LocalDeepStorage`], plus
    /// the storage handle and both backing tempdirs (returned so they
    /// outlive the test).
    async fn setup_executor_with_deep_storage() -> (
        Arc<MetadataStore>,
        Arc<ferrodruid_historical::Historical>,
        Overlord,
        Arc<dyn DeepStorage>,
        tempfile::TempDir,
        tempfile::TempDir,
    ) {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage));
        (
            metadata,
            historical,
            overlord,
            deep_storage,
            cache_dir,
            ds_dir,
        )
    }

    /// Batch parity: a batch task with a deep-storage backend PERSISTS the
    /// segment (blob present) and stamps a `loadSpec` marker into the
    /// metadata row's payload.
    #[tokio::test]
    async fn batch_persist_writes_blob_and_loadspec() {
        let (metadata, historical, overlord, deep_storage, _c, _d) =
            setup_executor_with_deep_storage().await;

        let _id = submit_ok(&overlord, batch_spec("bp_ds", json!(false), rows_first())).await;
        assert_eq!(queried_row_count(&historical, "bp_ds"), 4);

        let used = metadata
            .get_used_segments("bp_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), 1);
        let seg_id = used[0].id.clone();

        // loadSpec marker present with the right shape.
        let load_spec = used[0]
            .payload
            .get("loadSpec")
            .expect("payload carries a loadSpec after persist");
        assert_eq!(
            load_spec.get("type").and_then(|v| v.as_str()),
            Some("local")
        );
        assert_eq!(
            load_spec.get("dataSource").and_then(|v| v.as_str()),
            Some("bp_ds")
        );
        assert_eq!(
            load_spec.get("segmentId").and_then(|v| v.as_str()),
            Some(seg_id.as_str())
        );

        // Blob is really in deep storage.
        assert!(
            deep_storage
                .segment_exists("bp_ds", &seg_id)
                .await
                .expect("exists")
        );
    }

    /// Codex R23 H1: in SPILL residency mode (FG-7) a batch replace whose
    /// Phase-3 swap fails on a TRANSIENT spill-write error (disk full /
    /// EACCES / fsync — [`Historical::replace_segments`] admission does a
    /// real disk write) AND whose compensating metadata rollback ALSO
    /// fails leaves metadata committed as {victims → unused, new row →
    /// used} while the query-visible state still serves the victims and
    /// not the new segment. A retry (the default [`RetryPolicy`]
    /// auto-retries in-process; a manual resubmission behaves identically)
    /// plans its victims from USED metadata rows only
    /// (`plan_replace_victims`), so it selects the unloaded new row and
    /// never drops the still-loaded old victims — its own segment then
    /// loads NEXT TO them and every query double counts the interval
    /// until a restart.
    ///
    /// The fix reconciles the Historical to the irrevocably-committed
    /// metadata when the rollback fails: the old victims are dropped (a
    /// drop-only `replace_segments` spills nothing, so the very failure
    /// class that broke the swap cannot break the reconcile). The interval
    /// is then temporarily ABSENT (a gap the retry — or a restart's
    /// bootstrap reload of the durable new row — heals) rather than
    /// double-counted: gap → eventual consistency is the safe side of the
    /// trade against a silently wrong answer.
    #[tokio::test]
    async fn spill_swap_rollback_double_failure_must_not_double_count_on_retry() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        // Spill residency: every admission writes the segment's v9 bytes
        // under `cache_dir/spill/<pid>-<nonce>/` — a fallible disk write.
        let historical = Arc::new(ferrodruid_historical::Historical::with_options(
            cache_dir.path().to_path_buf(),
            10_000_000,
            false,
            true,
        ));
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );
        // max_attempts = 1 keeps the in-process auto-retry from firing
        // while the injected faults are still armed; the resubmission
        // below IS the retry (the default policy's automatic second
        // attempt takes exactly the same path).
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage))
            .with_retry_policy(RetryPolicy {
                max_attempts: 1,
                ..RetryPolicy::default()
            });

        // Seed: segment A (4 rows), spilled + loaded + used.
        let _seed = submit_ok(&overlord, batch_spec("dc_ds", json!(false), rows_first())).await;
        assert_eq!(queried_row_count(&historical, "dc_ds"), 4);
        let victim_id = {
            let used = metadata.get_used_segments("dc_ds").await.expect("used");
            assert_eq!(used.len(), 1);
            used[0].id.clone()
        };

        // Transient spill-write fault: the instance's private spill root
        // becomes read-only, so the swap's spill write fails with EACCES
        // (same failure class as disk-full/quota); restoring the
        // permissions below is what makes it "transient".
        let spill_root = cache_dir.path().join("spill");
        let instance_dir = std::fs::read_dir(&spill_root)
            .expect("spill root")
            .filter_map(std::result::Result::ok)
            .find(|e| e.path().is_dir())
            .expect("spill instance dir")
            .path();
        let writable = std::fs::metadata(&instance_dir)
            .expect("instance dir metadata")
            .permissions();
        let mut read_only = writable.clone();
        read_only.set_readonly(true);
        std::fs::set_permissions(&instance_dir, read_only).expect("chmod read-only");

        // Arm the rollback failure: swap Err + rollback Err is the R23 H1
        // residual (committed metadata, unreconciled query state).
        overlord
            .inject_rollback_replace_failure
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Swap + rollback double failure must fail the task (polled).
        let _failed =
            submit_failed(&overlord, batch_spec("dc_ds", json!(false), rows_second())).await;

        // The committed, un-rolled-back metadata: the victim row is unused
        // and the new row is used + durable (its blob is KEPT because the
        // rollback failed — H5 — so a restart's bootstrap reload can serve
        // it).
        let used_after_failure = metadata.get_used_segments("dc_ds").await.expect("used");
        assert_eq!(used_after_failure.len(), 1);
        let retained_id = used_after_failure[0].id.clone();
        assert_ne!(retained_id, victim_id, "the new row stayed committed");
        assert!(
            deep_storage
                .segment_exists("dc_ds", &retained_id)
                .await
                .expect("exists"),
            "the durable blob is retained while its row still references it"
        );

        // Reconcile (the fix): with the metadata irrevocably committed,
        // the query-visible state must match it — the old victim is
        // dropped (temporary gap, healed by the retry below or a
        // restart's bootstrap reload) instead of staying loaded next to
        // whatever a retry publishes.
        assert!(
            !historical.has_segment(&victim_id),
            "an unused victim must not stay query-visible after a failed rollback"
        );
        assert_eq!(
            queried_row_count(&historical, "dc_ds"),
            0,
            "the interval is a GAP until the retry republishes it — never a \
             double count"
        );

        // The fault was transient: the spill root is writable again and
        // the metadata store is healthy again.
        std::fs::set_permissions(&instance_dir, writable).expect("chmod writable");
        overlord
            .inject_rollback_replace_failure
            .store(false, std::sync::atomic::Ordering::SeqCst);

        // The retry. Its victim planning reads USED metadata rows only, so
        // it selects the (unloaded) retained row and NEVER the old victim
        // (already unused) — pre-fix the victim therefore stayed loaded
        // and the interval double-counted.
        let _retry = submit_ok(&overlord, batch_spec("dc_ds", json!(false), rows_second())).await;
        assert_eq!(
            queried_row_count(&historical, "dc_ds"),
            4,
            "the retry must serve ONLY its own rows — a still-loaded old victim \
             next to the retry's segment double counts the interval (Codex R23 H1)"
        );
        assert_eq!(
            historical.segment_count(),
            1,
            "exactly the retry's segment may be loaded — a lingering old victim \
             is the double-count (Codex R23 H1)"
        );
    }

    /// Codex R23 H1 (append variant): with `appendToExisting: true` there
    /// are NO victims, so the R23 drop-reconcile does not apply. In SPILL
    /// residency mode a Phase-3 swap that fails on a transient spill-write
    /// error AND a compensating rollback that ALSO fails leave the
    /// attempt's DURABLE row + blob committed (used, reloaded on restart)
    /// while the segment is not query-visible this session. The default
    /// [`RetryPolicy`] (`max_attempts = 3`) then auto-retries, and — because
    /// append never selects the retained row as a victim — each attempt
    /// appends ANOTHER durable row for the same input under a fresh id. The
    /// next restart's bootstrap reload loads EVERY durable row → a permanent
    /// duplicate / over-count.
    ///
    /// The fix SUPPRESSES the retry for this durable-retained append
    /// residual (streaming R14/H2 `RetainedDurable` parity): exactly one
    /// durable row survives, so the restart reload serves the interval once
    /// (no duplicate) and the data is not lost (it is durable). Honest
    /// limitation: the segment is a gap until that restart.
    #[tokio::test]
    async fn append_swap_rollback_double_failure_must_not_duplicate_on_restart() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        // Spill residency: the swap's spill write is a fallible disk op.
        let historical = Arc::new(ferrodruid_historical::Historical::with_options(
            cache_dir.path().to_path_buf(),
            10_000_000,
            false,
            true,
        ));
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );
        // DEFAULT policy (max_attempts = 3): the auto-retry loop is exactly
        // what must be suppressed — pre-fix each sticky-fault attempt commits
        // ANOTHER durable row.
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage));

        // Seed: append A (4 rows), persisted + loaded + used (fault unarmed).
        let _seed = submit_ok(&overlord, batch_spec("ap_ds", json!(true), rows_first())).await;
        assert_eq!(queried_row_count(&historical, "ap_ds"), 4);

        // Arm the STICKY double-fault: the spill instance dir is read-only
        // (every swap's spill write fails with EACCES) and the metadata
        // rollback fails.
        let spill_root = cache_dir.path().join("spill");
        let instance_dir = std::fs::read_dir(&spill_root)
            .expect("spill root")
            .filter_map(std::result::Result::ok)
            .find(|e| e.path().is_dir())
            .expect("spill instance dir")
            .path();
        let writable = std::fs::metadata(&instance_dir)
            .expect("instance dir metadata")
            .permissions();
        let mut read_only = writable.clone();
        read_only.set_readonly(true);
        std::fs::set_permissions(&instance_dir, read_only).expect("chmod read-only");
        overlord
            .inject_rollback_replace_failure
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Append B (disjoint rows): swap fails + rollback fails every attempt.
        let failed = overlord
            .submit_task(batch_spec("ap_ds", json!(true), rows_second()))
            .await
            .expect("submission recorded");
        assert_eq!(
            await_task_terminal(&overlord, &failed).await,
            TaskState::Success,
            "append swap + rollback double failure RETAINS a durably \
             committed row + blob — the task ends SUCCESS (F1: a FAILED \
             verdict on committed input invites a double-counting \
             resubmission), with the retry suppressed either way"
        );

        // The fault was transient; the disk + metadata store recover.
        std::fs::set_permissions(&instance_dir, writable).expect("chmod writable");
        overlord
            .inject_rollback_replace_failure
            .store(false, std::sync::atomic::Ordering::SeqCst);

        // Simulate a RESTART: a fresh, empty heap-mode Historical sharing the
        // SAME metadata + deep storage; bootstrap reload re-materializes every
        // used durable row. Pre-fix the extra retry-appended B rows reload too
        // → over-count.
        let cache2 = tempfile::tempdir().expect("cache2");
        let hist2 = Arc::new(ferrodruid_historical::Historical::new(
            cache2.path().to_path_buf(),
            10_000_000,
        ));
        let ovl2 = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist2))
            .with_deep_storage(Arc::clone(&deep_storage));
        let reloaded = ovl2.bootstrap_reload_segments().await.expect("bootstrap");
        assert_eq!(
            reloaded, 2,
            "exactly the seed A row and ONE durable B row may reload — a second \
             B row is the retry-appended duplicate (Codex R23 H1 append variant)"
        );
        assert_eq!(
            queried_row_count(&hist2, "ap_ds"),
            8,
            "the restart must serve A (4) + B (4) exactly once — a retained-plus-\
             reappended B over-counts the interval pre-fix; that this equals 8 \
             (not 4) also proves the suppressed retry lost NO data (B is durable \
             and reloaded)"
        );

        // Confirm the durable footprint directly: one A row + exactly one B row.
        let used = metadata.get_used_segments("ap_ds").await.expect("used");
        assert_eq!(
            used.len(),
            2,
            "retry suppression keeps exactly ONE durable B row — pre-fix each \
             auto-retry attempt appended another durable row: {used:?}"
        );
    }

    /// F1 — a durably-committed append must never be recorded FAILED. The
    /// append swap-failed + rollback-failed residual deliberately RETAINS
    /// the committed metadata row + blob (the data IS durably committed
    /// and the next restart's bootstrap reload serves it), yet pre-fix the
    /// publish path returned `Err`, the fence verdict read `is_ok()` =
    /// false, and the task was recorded FAILED. A FAILED verdict on
    /// durably-committed append input is a resubmit invitation: a client
    /// resubmitting its "failed" append lands a SECOND durable row and the
    /// next restart reloads BOTH — a permanent double count. The fence
    /// verdict must reflect whether the DATA durably committed, not
    /// whether the function returned `Ok`: this task must end SUCCESS.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_rollback_failed_durable_commit_ends_success_not_failed() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        // Spill residency: the swap's spill write is a fallible disk op.
        let historical = Arc::new(ferrodruid_historical::Historical::with_options(
            cache_dir.path().to_path_buf(),
            10_000_000,
            false,
            true,
        ));
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage));

        // Seed: append A (4 rows), persisted + loaded + used (fault unarmed).
        let _seed = submit_ok(&overlord, batch_spec("f1_ds", json!(true), rows_first())).await;

        // Arm the double fault: read-only spill instance dir (swap fails
        // with EACCES) + failing metadata rollback — the durable-retained
        // append residual.
        let spill_root = cache_dir.path().join("spill");
        let instance_dir = std::fs::read_dir(&spill_root)
            .expect("spill root")
            .filter_map(std::result::Result::ok)
            .find(|e| e.path().is_dir())
            .expect("spill instance dir")
            .path();
        let writable = std::fs::metadata(&instance_dir)
            .expect("instance dir metadata")
            .permissions();
        let mut read_only = writable.clone();
        read_only.set_readonly(true);
        std::fs::set_permissions(&instance_dir, read_only).expect("chmod read-only");
        overlord
            .inject_rollback_replace_failure
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Append B: swap fails, rollback fails, row + blob RETAINED — the
        // data is durably committed, so the POLLED status (the async
        // contract's only verdict channel) must report the truth.
        let failed_id = overlord
            .submit_task(batch_spec("f1_ds", json!(true), rows_second()))
            .await
            .expect("submit");
        assert_eq!(
            await_task_terminal(&overlord, &failed_id).await,
            TaskState::Success,
            "a durably-committed append (row + blob retained) must end \
             SUCCESS — FAILED invites a resubmission that double counts \
             after the restart reload"
        );
        std::fs::set_permissions(&instance_dir, writable).expect("chmod writable");
        overlord
            .inject_rollback_replace_failure
            .store(false, std::sync::atomic::Ordering::SeqCst);

        // Exactly the seed row + ONE durable B row (SUCCESS also means no
        // auto-retry ran).
        let used = metadata.get_used_segments("f1_ds").await.expect("used");
        assert_eq!(
            used.len(),
            2,
            "exactly one durable B row may exist: {used:?}"
        );

        // Restart: both durable rows reload; B is served exactly once (no
        // double count, no loss) — the SUCCESS verdict was the truth.
        let cache2 = tempfile::tempdir().expect("cache2");
        let hist2 = Arc::new(ferrodruid_historical::Historical::new(
            cache2.path().to_path_buf(),
            10_000_000,
        ));
        let ovl2 = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist2))
            .with_deep_storage(Arc::clone(&deep_storage));
        let reloaded = ovl2.bootstrap_reload_segments().await.expect("bootstrap");
        assert_eq!(reloaded, 2, "seed A + exactly one B reload");
        assert_eq!(
            queried_row_count(&hist2, "f1_ds"),
            8,
            "A (4) + B (4) exactly once after the restart"
        );
    }

    /// F2 — retry exhaustion must not lose the durable verdict across a
    /// restart. When the bounded terminal-persist retry caps out, the
    /// durable task row stays RUNNING while the truthful SUCCESS lives
    /// only in this process's memory; pre-fix a restart then had NO
    /// recoverable verdict for a committed append — a client polling the
    /// id saw a non-terminal state forever and resubmitting duplicated
    /// the input. Post-fix the bootstrap reconciles a committed-but-
    /// RUNNING batch row (correlated via the segment payload's `taskId` +
    /// `loadSpec`) to a durable SUCCESS.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bootstrap_reconciles_committed_running_task_row_to_success() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage));

        // A terminal-persist outage that NEVER clears: the bounded retry
        // exhausts and the durable row is left RUNNING (the residual).
        overlord
            .inject_terminal_persist_failures
            .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);

        let expected_id = "index_parallel_f2_ds_1";
        let id = overlord
            .submit_task(batch_spec("f2_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "committed publish polls SUCCESS"
        );
        assert!(
            await_registry_quiescence(&overlord).await,
            "registry must quiesce after retry exhaustion"
        );
        // Precondition (the documented residual this finding is about):
        // the durable row is stuck RUNNING although the publish committed.
        let row = metadata
            .get_task(expected_id)
            .await
            .expect("get row")
            .expect("row present");
        assert_eq!(row.status, "RUNNING", "precondition: stuck-RUNNING row");

        // RESTART: a fresh Overlord on the same metadata + deep storage.
        let cache2 = tempfile::tempdir().expect("cache2");
        let hist2 = Arc::new(ferrodruid_historical::Historical::new(
            cache2.path().to_path_buf(),
            10_000_000,
        ));
        let ovl2 = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist2))
            .with_deep_storage(Arc::clone(&deep_storage));
        let reloaded = ovl2.bootstrap_reload_segments().await.expect("bootstrap");
        assert_eq!(reloaded, 1, "the committed durable segment reloads");

        // THE FIX: the committed-but-RUNNING row is reconciled to a
        // durable SUCCESS at bootstrap — the verdict survives the restart
        // instead of inviting a duplicate resubmission.
        let row = metadata
            .get_task(expected_id)
            .await
            .expect("get row")
            .expect("row present");
        assert_eq!(
            row.status, "SUCCESS",
            "bootstrap must reconcile a committed-but-RUNNING batch row \
             to SUCCESS (its segment row is published + durable): {row:?}"
        );
        // A polling client gets the recoverable verdict (via the durable
        // fallback), not a resubmit invitation.
        let info = ovl2
            .get_task(expected_id)
            .await
            .expect("get")
            .expect("record");
        assert_eq!(
            info.status,
            TaskStatus::Success,
            "the restart-recovered verdict must be SUCCESS: {info:?}"
        );
        // Lock hygiene: nothing held by the reconciled task.
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks");
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
    }

    /// F3 (part 1) — a shielded publish that HANGS must hit the deadline
    /// and resolve to a definite verdict instead of parking the submit
    /// tail (and the fence) forever. Nothing was durably committed at the
    /// hang point, so the verdict is NOT-committed / FAILED, and the
    /// registry must fully quiesce while the hang persists.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hung_shielded_publish_deadline_fails_task_and_quiesces() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_publish_deadline(std::time::Duration::from_millis(100));

        // The hang: the shielded section parks at its entry while the test
        // holds the pause mutex — held for the WHOLE test, so every
        // resolution below happens while the "upload" is still hung.
        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let expected_id = "index_parallel_f3_hang_ds_1";
        let id = overlord
            .submit_task(batch_spec("f3_hang_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);

        // The polled status must resolve in bounded time — pre-fix the
        // fence (and everything awaiting it) parked forever behind the
        // hung section; the deadline now cancels it and the definite
        // FAILED verdict lands in the task status.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Failed,
            "timed-out publish (nothing committed) must be FAILED"
        );
        // The fence is resolved and retired; nothing accumulates per hang.
        assert!(
            await_registry_quiescence(&overlord).await,
            "one hung publish must not leave a fence/tail/retry entry"
        );
        // Nothing was committed by the cancelled section.
        assert!(
            metadata
                .get_all_segments()
                .await
                .expect("all segments")
                .is_empty(),
            "a publish cancelled at its (pre-P) hang point commits nothing"
        );
        drop(pause_guard);
    }

    /// F3 (part 2) — a `shutdown_task` finalizer awaiting the fence of a
    /// HUNG shielded publish must be RELEASED by the deadline with a
    /// definite verdict. Pre-fix the fence sender lived forever inside
    /// the hung section, so the shutdown parked forever (one leaked
    /// finalizer + fence + task per hang).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hung_shielded_publish_deadline_releases_shutdown_finalizer() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_publish_deadline(std::time::Duration::from_millis(100));

        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let expected_id = "index_parallel_f3_fin_ds_1";
        let id = overlord
            .submit_task(batch_spec("f3_fin_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);
        // Wait until the shielded section has genuinely STARTED (fence
        // registered, publish lock held) so the shutdown finds the fence.
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_publish_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "shielded publication section never started");

        // The shutdown awaits the fence verdict — it must be released by
        // the deadline, not parked forever behind the hang.
        let shutdown_result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            overlord.shutdown_task(expected_id),
        )
        .await
        .expect("the deadline must release the shutdown finalizer");
        assert!(
            shutdown_result.is_ok(),
            "released finalizer persists the truthful verdict: {shutdown_result:?}"
        );

        let info = overlord
            .get_task(expected_id)
            .await
            .expect("get")
            .expect("record");
        assert!(
            info.state.is_terminal(),
            "the finalized task must be terminal: {info:?}"
        );
        assert_eq!(
            info.status,
            TaskStatus::Failed,
            "nothing durably committed at the hang point: {info:?}"
        );
        assert!(
            await_registry_quiescence(&overlord).await,
            "fence + tail + finalizer must all be released despite the hang"
        );
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks");
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
        drop(pause_guard);
    }

    /// R9-F1 (forward inversion) — a deadline-interrupted publish whose
    /// metadata COMMIT lands anyway (the dropped `commit().await` had
    /// already reached the database) must resolve to the verdict of the
    /// DURABLE row: SUCCESS, not FAILED. Pre-fix the verdict was
    /// classified from the fence's last provisional state (`InFlight`) →
    /// terminal FAILED → the client resubmits input that is durably
    /// committed and reloaded on restart: a permanent append double
    /// count (and F2 cannot recover it, because F2 only scans RUNNING
    /// rows and this row is terminal FAILED).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_interrupted_publish_commit_landed_resolves_success() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_publish_deadline(std::time::Duration::from_millis(100));

        // Park the section at its entry for the WHOLE test: the deadline
        // drops it there, exactly like a hung store op.
        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let expected_id = "index_parallel_f1a_dl_ds_1";
        // The commit that "lands exactly as the deadline fires": by the
        // time the interrupted verdict is resolved, a DURABLE batch
        // segment row correlated to this task is present in the store —
        // the same row `replace_segments_txn` would have committed.
        metadata
            .insert_segment(&SegmentMetadataRow {
                id: "f1a_dl_ds_2026-01-01T00:00:00.000Z_2026-01-02T00:00:00.000Z_v1".to_string(),
                data_source: "f1a_dl_ds".to_string(),
                created_date: "2026-01-01T00:00:00.000Z".to_string(),
                start: "2026-01-01T00:00:00.000Z".to_string(),
                end: "2026-01-02T00:00:00.000Z".to_string(),
                version: "v1".to_string(),
                used: true,
                payload: json!({
                    "dataSource": "f1a_dl_ds",
                    "numRows": 4,
                    "taskId": expected_id,
                    "kind": "batch",
                    "loadSpec": {"type": "local", "path": "/blob"}
                }),
            })
            .await
            .expect("insert landed-commit row");

        let id = overlord
            .submit_task(batch_spec("f1a_dl_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);

        // The deadline must resolve the interrupted publish and the
        // POLLED verdict must be the durable truth.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "a deadline-interrupted publish whose commit LANDED must \
             resolve to the durable truth (SUCCESS) — FAILED invites a \
             resubmission that double counts the committed input"
        );
        // No double count: the SUCCESS verdict invites no resubmission and
        // the suppressed retry appended no second row — exactly ONE
        // durable row exists for the input.
        let all = metadata.get_all_segments().await.expect("all segments");
        assert_eq!(
            all.len(),
            1,
            "exactly the landed-commit row may exist: {all:?}"
        );
        assert!(
            await_registry_quiescence(&overlord).await,
            "no fence/tail/retry entry may leak"
        );
        drop(pause_guard);
    }

    /// R9-F1 (reverse inversion) — a deadline-interrupted publish whose
    /// ROLLBACK lands anyway (the row committed in Phase M was removed
    /// while the fence still read `DurableAppendCommitted`) must resolve
    /// to the verdict of the DURABLE state: FAILED, not SUCCESS. Pre-fix
    /// the provisional fence state produced SUCCESS over a removed row —
    /// the client never resubmits and the batch is silently lost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_interrupted_publish_rollback_landed_resolves_failed() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage))
            .with_publish_deadline(std::time::Duration::from_millis(2000));

        // Park the section right AFTER its Phase-M commit (fence
        // provisionally DurableAppendCommitted): the deadline drops it in
        // the committed-but-unresolved window.
        let pause = Arc::clone(&overlord.test_post_commit_pause);
        let pause_guard = pause.lock().await;

        let expected_id = "index_parallel_f1b_dl_ds_1";
        let id = overlord
            .submit_task(batch_spec("f1b_dl_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);
        // Wait until the section has genuinely committed Phase M and is
        // parked at the post-commit pause.
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_post_commit_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "section never reached its post-commit point");
        let committed_row = metadata
            .get_all_segments()
            .await
            .expect("all segments")
            .into_iter()
            .find(|s| {
                s.payload.get("taskId").and_then(serde_json::Value::as_str) == Some(expected_id)
            })
            .expect("Phase M committed a row for the task");
        // The rollback that "lands exactly as the deadline fires": the
        // committed row is REMOVED while the fence still provisionally
        // reads DurableAppendCommitted.
        metadata
            .delete_segments(std::slice::from_ref(&committed_row.id))
            .await
            .expect("landed rollback removes the row");

        // The deadline must resolve the interrupted publish; the polled
        // verdict must follow the durable truth.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Failed,
            "a deadline-interrupted publish whose rollback LANDED must \
             resolve to the durable truth (FAILED) — SUCCESS over a \
             removed row silently loses the batch (the client never \
             resubmits)"
        );
        assert!(
            await_registry_quiescence(&overlord).await,
            "no fence/tail/retry entry may leak"
        );
        drop(pause_guard);
    }

    /// D1 (forward) — a `shutdown_task` finalizer awaiting a fence that
    /// the publish DEADLINE closes without a `Resolved` verdict must
    /// resolve from DURABLE STATE, exactly like the deadline path
    /// itself: a landed commit reads SUCCESS. Pre-fix the finalizer
    /// classified the closed fence from its last PROVISIONAL state
    /// (`InFlight` → FAILED) and persisted that — the deadline task's
    /// later durable-state resolution could never reach it — so a
    /// committed append was marked FAILED and a client resubmission
    /// double counted the committed input permanently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_finalizer_on_deadline_closed_fence_resolves_commit_to_success() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_publish_deadline(std::time::Duration::from_millis(800));

        // Park the section at its entry for the WHOLE test: the deadline
        // drops it there, closing the fence with `InFlight` provisional.
        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let expected_id = "index_parallel_d1a_ds_1";
        // The commit that "landed anyway": a DURABLE batch segment row
        // correlated to this task is present when the verdict resolves.
        metadata
            .insert_segment(&SegmentMetadataRow {
                id: "d1a_ds_2026-01-01T00:00:00.000Z_2026-01-02T00:00:00.000Z_v1".to_string(),
                data_source: "d1a_ds".to_string(),
                created_date: "2026-01-01T00:00:00.000Z".to_string(),
                start: "2026-01-01T00:00:00.000Z".to_string(),
                end: "2026-01-02T00:00:00.000Z".to_string(),
                version: "v1".to_string(),
                used: true,
                payload: json!({
                    "dataSource": "d1a_ds",
                    "numRows": 4,
                    "taskId": expected_id,
                    "kind": "batch",
                    "loadSpec": {"type": "local", "path": "/blob"}
                }),
            })
            .await
            .expect("insert landed-commit row");

        let id = overlord
            .submit_task(batch_spec("d1a_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);
        // Wait until the shielded section has genuinely STARTED (fence
        // registered) so the shutdown awaits the fence verdict.
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_publish_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "shielded publication section never started");

        // The shutdown finalizer awaits the fence; the deadline then
        // closes it WITHOUT a Resolved verdict.
        let shutdown = {
            let this = overlord.clone_handle();
            let tid = expected_id.to_string();
            tokio::spawn(async move { this.shutdown_task(&tid).await })
        };
        let shutdown_result = tokio::time::timeout(std::time::Duration::from_secs(15), shutdown)
            .await
            .expect("the deadline must release the shutdown finalizer")
            .expect("finalizer must not panic");
        assert!(
            shutdown_result.is_ok(),
            "released finalizer persists the truthful verdict: {shutdown_result:?}"
        );

        // The finalizer must resolve from DURABLE state: the landed
        // commit reads SUCCESS — never a resubmit-inviting FAILED.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "a shutdown finalizer observing a deadline-closed fence must \
             resolve the verdict from durable state (landed commit → \
             SUCCESS); classifying the provisional InFlight as FAILED \
             invites a double-counting resubmission"
        );
        // The durable row too.
        let mut durable_status = String::new();
        for _ in 0..500 {
            if let Some(row) = metadata.get_task(expected_id).await.expect("get row") {
                durable_status.clone_from(&row.status);
                if durable_status == "SUCCESS" {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(durable_status, "SUCCESS", "durable row must reach SUCCESS");
        // No double count: exactly the landed-commit row exists.
        let all = metadata.get_all_segments().await.expect("all segments");
        assert_eq!(all.len(), 1, "exactly the landed-commit row: {all:?}");
        assert!(
            await_registry_quiescence(&overlord).await,
            "no fence/tail/retry entry may leak"
        );
        drop(pause_guard);
    }

    /// D1 (reverse) — the same shutdown-finalizer path with a fence the
    /// deadline closes at provisional `DurableAppendCommitted` while the
    /// ROLLBACK landed anyway (the committed row was removed): the
    /// finalizer must resolve FAILED from durable state. Pre-fix it
    /// classified the provisional state as committed and persisted
    /// SUCCESS over a removed row — the batch was silently lost (a
    /// client never resubmits a SUCCESS).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_finalizer_on_deadline_closed_fence_resolves_rollback_to_failed() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage))
            .with_publish_deadline(std::time::Duration::from_millis(1500));

        // Park the section right AFTER its Phase-M commit: the fence is
        // provisionally DurableAppendCommitted when the deadline drops it.
        let pause = Arc::clone(&overlord.test_post_commit_pause);
        let pause_guard = pause.lock().await;

        let expected_id = "index_parallel_d1b_ds_1";
        let id = overlord
            .submit_task(batch_spec("d1b_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);
        // Wait until the section has genuinely committed Phase M.
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_post_commit_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "section never reached its post-commit point");
        let committed_row = metadata
            .get_all_segments()
            .await
            .expect("all segments")
            .into_iter()
            .find(|s| {
                s.payload.get("taskId").and_then(serde_json::Value::as_str) == Some(expected_id)
            })
            .expect("Phase M committed a row for the task");
        // The rollback that "landed anyway": the committed row is REMOVED
        // while the fence still provisionally reads DurableAppendCommitted.
        metadata
            .delete_segments(std::slice::from_ref(&committed_row.id))
            .await
            .expect("landed rollback removes the row");

        // The shutdown finalizer awaits the fence; the deadline closes it
        // at DurableAppendCommitted without a Resolved verdict.
        let shutdown = {
            let this = overlord.clone_handle();
            let tid = expected_id.to_string();
            tokio::spawn(async move { this.shutdown_task(&tid).await })
        };
        let shutdown_result = tokio::time::timeout(std::time::Duration::from_secs(15), shutdown)
            .await
            .expect("the deadline must release the shutdown finalizer")
            .expect("finalizer must not panic");
        assert!(
            shutdown_result.is_ok(),
            "released finalizer persists the truthful verdict: {shutdown_result:?}"
        );

        // The finalizer must resolve from DURABLE state: rollback landed →
        // FAILED, never SUCCESS over a removed row.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Failed,
            "a shutdown finalizer observing a deadline-closed fence must \
             resolve the verdict from durable state (landed rollback → \
             FAILED); persisting the provisional DurableAppendCommitted \
             as SUCCESS silently loses the batch"
        );
        assert!(
            await_registry_quiescence(&overlord).await,
            "no fence/tail/retry entry may leak"
        );
        drop(pause_guard);
    }

    /// D2 — the publish deadline must NOT release the datasource publish
    /// lock while a deadline-dropped Phase-M commit may still land: a
    /// subsequent publisher could interleave with the landing commit
    /// (stale victim planning / lost publication), and the point-in-time
    /// durable verdict could contradict what actually lands. Post-fix
    /// the commit runs as an UNCANCELLABLE tracked sub-task, the
    /// deadline task keeps the lock held while it bound-waits for the
    /// op to truly resolve, and only then classifies from durable state
    /// — so the verdict matches what landed and no publisher interleaves
    /// mid-commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_dropped_commit_holds_publish_lock_and_verdict_matches_durable_truth() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage))
            .with_publish_deadline(std::time::Duration::from_millis(500));

        // Park the Phase-M commit BEFORE it reaches the store: the
        // deadline then fires while the commit is genuinely mid-flight.
        let pause = Arc::clone(&overlord.test_commit_op_pause);
        let pause_guard = pause.lock().await;

        let expected_id = "index_parallel_d2_ds_1";
        let id = overlord
            .submit_task(batch_spec("d2_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);
        // Wait until the commit op is genuinely in flight (parked).
        let mut entered = false;
        for _ in 0..1000 {
            if overlord
                .test_commit_op_entered
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                entered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(entered, "the Phase-M commit never started");
        // Let the deadline fire (and the section be dropped) while the
        // commit is still unresolved.
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        // D2 core: while the dropped commit is STILL RESOLVING, the
        // datasource publish lock must be HELD — a subsequent publisher
        // on the same datasource must not be able to interleave.
        let publish_lock = metadata.datasource_publish_lock("d2_ds").await;
        assert!(
            publish_lock.try_lock().is_err(),
            "the publish lock was released while a deadline-dropped \
             commit is still resolving — a subsequent publisher can \
             interleave with the landing commit"
        );
        // ... and no verdict may be persisted from a point-in-time read
        // that races the landing commit.
        let info = overlord
            .get_task(expected_id)
            .await
            .expect("get")
            .expect("record");
        assert!(
            !info.state.is_terminal(),
            "a verdict was persisted while the dropped commit was still \
             resolving: {info:?}"
        );

        // Release the commit: the uncancellable op lands its durable row.
        drop(pause_guard);

        // The verdict must MATCH the durable truth: the commit landed, so
        // the (append-mode) task ends SUCCESS — a FAILED verdict would
        // invite a resubmission that double counts after a restart
        // reload.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "the deadline verdict must match what the dropped commit \
             actually landed"
        );
        // Exactly ONE durable committed row for the input (no duplicate,
        // no loss).
        let all = metadata.get_all_segments().await.expect("all segments");
        assert_eq!(all.len(), 1, "exactly one committed row: {all:?}");
        assert_eq!(
            all[0]
                .payload
                .get("taskId")
                .and_then(serde_json::Value::as_str),
            Some(expected_id),
            "the committed row must be the dropped commit's own"
        );
        // The publish lock is released once the op truly resolved.
        let mut lock_free = false;
        for _ in 0..500 {
            if metadata
                .datasource_publish_lock("d2_ds")
                .await
                .try_lock()
                .is_ok()
            {
                lock_free = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            lock_free,
            "the publish lock must be released after the dropped commit \
             resolved"
        );
        assert!(
            await_registry_quiescence(&overlord).await,
            "no fence/tail/retry entry may leak"
        );
    }

    /// R9-F2 provenance applied to the R9-F1 inline durable check — a
    /// STREAMING (kafka/kinesis) segment row whose `taskId` coincides
    /// with the interrupted batch task must NOT count as commit proof:
    /// the deadline verdict stays FAILED. Guards the check against the
    /// same coincidental-id spoof as the reconcile.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_inline_durable_check_ignores_streaming_segments() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_publish_deadline(std::time::Duration::from_millis(100));

        let pause = Arc::clone(&overlord.test_publish_pause);
        let pause_guard = pause.lock().await;

        let expected_id = "index_parallel_f1c_dl_ds_1";
        // A DURABLE row with a coincidentally matching taskId — but of
        // STREAMING provenance. It must not flip the verdict.
        metadata
            .insert_segment(&SegmentMetadataRow {
                id: "f1c_dl_ds_2026-01-01T00:00:00.000Z_2026-01-02T00:00:00.000Z_v1".to_string(),
                data_source: "f1c_dl_ds".to_string(),
                created_date: "2026-01-01T00:00:00.000Z".to_string(),
                start: "2026-01-01T00:00:00.000Z".to_string(),
                end: "2026-01-02T00:00:00.000Z".to_string(),
                version: "v1".to_string(),
                used: true,
                payload: json!({
                    "dataSource": "f1c_dl_ds",
                    "numRows": 4,
                    "taskId": expected_id,
                    "kind": "kafka-streaming",
                    "topic": "coincidence",
                    "loadSpec": {"type": "local", "path": "/blob"}
                }),
            })
            .await
            .expect("insert streaming row");

        let id = overlord
            .submit_task(batch_spec("f1c_dl_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);
        // Deadline resolution: the polled verdict must NOT read the
        // streaming row as batch-commit proof.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Failed,
            "a streaming segment must not prove a batch commit"
        );
        drop(pause_guard);
    }

    /// R9-F2 — reconciliation must not attribute a STREAMING segment to a
    /// batch task. A stale RUNNING batch task whose id coincidentally
    /// matches the `taskId` of a kafka/kinesis segment row (supervisor and
    /// task ids are user-controlled) must reconcile to FAILED — pre-fix it
    /// reconciled to SUCCESS and the batch's (uncommitted) data was
    /// silently lost, because the client never resubmits a SUCCESS. Real
    /// batch provenance (the `kind: "batch"` marker, or a legacy row
    /// without any `kind`) must still reconcile to SUCCESS.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconcile_requires_batch_provenance_for_commit_proof() {
        let (metadata, overlord) = setup().await;

        let mk_task = |id: &str, ds: &str| TaskRow {
            id: id.to_string(),
            task_type: "index_parallel".to_string(),
            data_source: ds.to_string(),
            status: "RUNNING".to_string(),
            created_date: "2026-01-01T00:00:00.000Z".to_string(),
            attempt: 1,
            worker: Some("overlord".to_string()),
            payload: json!({"id": id, "status": "RUNNING"}),
        };
        let mk_seg = |id: &str, ds: &str, payload: serde_json::Value| SegmentMetadataRow {
            id: id.to_string(),
            data_source: ds.to_string(),
            created_date: "2026-01-01T00:00:00.000Z".to_string(),
            start: "2026-01-01T00:00:00.000Z".to_string(),
            end: "2026-01-02T00:00:00.000Z".to_string(),
            version: "v1".to_string(),
            used: true,
            payload,
        };

        // (1) Stale batch task whose id coincides with a KAFKA segment.
        metadata
            .insert_task(&mk_task("stale_kafka_coincide", "r9_kafka_ds"))
            .await
            .expect("task 1");
        metadata
            .insert_segment(&mk_seg(
                "r9_kafka_seg",
                "r9_kafka_ds",
                json!({
                    "dataSource": "r9_kafka_ds",
                    "numRows": 7,
                    "taskId": "stale_kafka_coincide",
                    "kind": "kafka-streaming",
                    "topic": "t",
                    "loadSpec": {"type": "local", "path": "/k"}
                }),
            ))
            .await
            .expect("seg 1");
        // (2) Stale batch task whose id coincides with a KINESIS segment.
        metadata
            .insert_task(&mk_task("stale_kinesis_coincide", "r9_kinesis_ds"))
            .await
            .expect("task 2");
        metadata
            .insert_segment(&mk_seg(
                "r9_kinesis_seg",
                "r9_kinesis_ds",
                json!({
                    "dataSource": "r9_kinesis_ds",
                    "numRows": 7,
                    "taskId": "stale_kinesis_coincide",
                    "kind": "kinesis-streaming",
                    "stream": "s",
                    "loadSpec": {"type": "local", "path": "/n"}
                }),
            ))
            .await
            .expect("seg 2");
        // (3) Stale batch task whose id coincides with an UNKNOWN-kind
        // segment: fail-closed, not commit proof.
        metadata
            .insert_task(&mk_task("stale_unknown_coincide", "r9_unknown_ds"))
            .await
            .expect("task 3");
        metadata
            .insert_segment(&mk_seg(
                "r9_unknown_seg",
                "r9_unknown_ds",
                json!({
                    "dataSource": "r9_unknown_ds",
                    "numRows": 7,
                    "taskId": "stale_unknown_coincide",
                    "kind": "future-streaming",
                    "loadSpec": {"type": "local", "path": "/u"}
                }),
            ))
            .await
            .expect("seg 3");
        // (4) Stale batch task with a REAL batch segment (positive marker).
        metadata
            .insert_task(&mk_task("stale_batch_marked", "r9_batch_ds"))
            .await
            .expect("task 4");
        metadata
            .insert_segment(&mk_seg(
                "r9_batch_seg",
                "r9_batch_ds",
                json!({
                    "dataSource": "r9_batch_ds",
                    "numRows": 7,
                    "taskId": "stale_batch_marked",
                    "kind": "batch",
                    "loadSpec": {"type": "local", "path": "/b"}
                }),
            ))
            .await
            .expect("seg 4");
        // (5) Stale batch task with a LEGACY batch segment (no `kind` —
        // rows published before the marker existed; streaming rows always
        // carry a `kind`, so absence is batch provenance).
        metadata
            .insert_task(&mk_task("stale_batch_legacy", "r9_legacy_ds"))
            .await
            .expect("task 5");
        metadata
            .insert_segment(&mk_seg(
                "r9_legacy_seg",
                "r9_legacy_ds",
                json!({
                    "dataSource": "r9_legacy_ds",
                    "numRows": 7,
                    "taskId": "stale_batch_legacy",
                    "loadSpec": {"type": "local", "path": "/l"}
                }),
            ))
            .await
            .expect("seg 5");

        let reconciled = overlord
            .reconcile_stale_running_batch_tasks()
            .await
            .expect("reconcile");
        assert_eq!(reconciled, 5, "every stale RUNNING batch row reconciles");

        let status_of = |id: &'static str| {
            let metadata = Arc::clone(&metadata);
            async move {
                metadata
                    .get_task(id)
                    .await
                    .expect("get task")
                    .expect("task row present")
                    .status
            }
        };
        assert_eq!(
            status_of("stale_kafka_coincide").await,
            "FAILED",
            "a kafka segment with a coincidental taskId is NOT commit \
             proof for a batch task — SUCCESS silently loses the batch"
        );
        assert_eq!(
            status_of("stale_kinesis_coincide").await,
            "FAILED",
            "a kinesis segment with a coincidental taskId is NOT commit \
             proof for a batch task"
        );
        assert_eq!(
            status_of("stale_unknown_coincide").await,
            "FAILED",
            "an unknown-kind segment fails closed (not commit proof)"
        );
        assert_eq!(
            status_of("stale_batch_marked").await,
            "SUCCESS",
            "a genuinely batch-produced durable segment still proves the \
             commit"
        );
        assert_eq!(
            status_of("stale_batch_legacy").await,
            "SUCCESS",
            "a legacy (pre-marker, no-kind) batch segment still proves \
             the commit"
        );
    }

    /// F4 — a HUNG terminal-persist attempt (as opposed to one that fails
    /// fast) must still advance the bounded retry's attempt cap and remove
    /// the retry marker. Pre-fix each attempt awaited
    /// `persist_terminal_row` with no timeout, so ONE hung metadata op
    /// parked the finalizer and the retry loop forever — the cap never
    /// exhausted and the marker never left the registry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hung_terminal_persist_attempts_still_exhaust_cap_and_remove_marker() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // First terminal persist (the tail's own) fails FAST, routing the
        // tail into `finalize_batch_terminal`; every subsequent persist
        // (the finalizer's direct attempt + all bounded-retry attempts)
        // HANGS.
        overlord
            .inject_terminal_persist_failures
            .store(1, std::sync::atomic::Ordering::SeqCst);
        overlord
            .inject_terminal_persist_hangs
            .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);

        let expected_id = "index_parallel_f4_ds_1";
        let id = overlord
            .submit_task(batch_spec("f4_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);
        // The polled status must reach the truthful terminal SUCCESS in
        // bounded time — a hung terminal persist must not park the tail's
        // finalization (F4).
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "the committed publish reports success regardless of the hung persist"
        );

        // The cap must exhaust and the marker must leave the registry in
        // bounded time even though every attempt hangs.
        assert!(
            await_registry_quiescence(&overlord).await,
            "hung persist attempts must still exhaust the attempt cap and \
             remove the retry marker"
        );

        // In-memory truth: SUCCESS for this process's lifetime.
        let info = overlord
            .get_task(expected_id)
            .await
            .expect("get")
            .expect("record");
        assert_eq!(info.status, TaskStatus::Success);
        // Honest residual: with the store hung for every attempt the
        // durable row keeps the documented RUNNING crash-residual shape
        // (recovered at the next bootstrap by the F2 reconcile).
        let row = metadata
            .get_task(expected_id)
            .await
            .expect("get row")
            .expect("row present");
        assert_eq!(row.status, "RUNNING");
    }

    /// The missed F4 site #1 (High): the tail's INITIAL terminal persist
    /// — the HAPPY-path write at the end of `run_batch_tail_body` — had
    /// no per-attempt timeout, unlike every other terminal-persist site
    /// (the finalizer's direct attempt and every bounded-retry attempt,
    /// F4). One HUNG (not failing-fast) metadata op AFTER the publish
    /// committed therefore parked the tail forever: the in-memory task
    /// stayed RUNNING and the live tail + resolved fence stayed
    /// registered indefinitely — a strand/leak with NO deadline anywhere
    /// (control never reached `finalize_batch_terminal`). Post-fix the
    /// attempt is time-bounded and a timeout routes to the SAME truthful
    /// recovery as a fast failure: SUCCESS in-memory (the fence-derived
    /// verdict — never a resubmit-inviting FAILED), fence retired, locks
    /// released, durable row flushed by the finalizer's own bounded
    /// persist, registry quiescent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hung_initial_terminal_persist_still_finalizes_committed_publish() {
        let (metadata, _historical, overlord, _dir) = setup_executor().await;

        // ONLY the tail's initial terminal persist hangs (it is the first
        // choke-point call — the submit-side RUNNING-row insert bypasses
        // the hook); every subsequent persist (the finalizer's bounded
        // attempt) succeeds, so the durable verdict can land.
        overlord
            .inject_terminal_persist_hangs
            .store(1, std::sync::atomic::Ordering::SeqCst);

        let expected_id = "index_parallel_hang_initial_ds_1";
        let id = overlord
            .submit_task(batch_spec("hang_initial_ds", json!(true), rows_first()))
            .await
            .expect("submit");
        assert_eq!(id, expected_id);

        // RED (pre-fix): the tail parks forever on the hung persist — the
        // task never leaves RUNNING and this poll panics. GREEN: the
        // bounded attempt times out and the committed publish finalizes
        // SUCCESS.
        assert_eq!(
            await_task_terminal(&overlord, expected_id).await,
            TaskState::Success,
            "a committed publish whose INITIAL terminal persist hangs must \
             still finalize SUCCESS in bounded time (never strand RUNNING)"
        );

        // No strand/leak at quiescence: the live tail, the resolved
        // fence, and any retry marker must all leave the registry.
        assert!(
            await_registry_quiescence(&overlord).await,
            "the live tail / resolved fence must not stay registered \
             behind a hung initial terminal persist"
        );

        // The durable verdict lands via the finalizer's bounded persist
        // (the injected hang was consumed by the initial attempt).
        let mut durable_status = String::new();
        for _ in 0..500 {
            if let Some(row) = metadata.get_task(expected_id).await.expect("get row") {
                durable_status.clone_from(&row.status);
                if durable_status == "SUCCESS" {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(durable_status, "SUCCESS", "durable row carries the verdict");

        // Interval-lock hygiene at quiescence.
        let locks = metadata
            .get_locks_for_task(expected_id)
            .await
            .expect("locks");
        assert!(locks.is_empty(), "locks must be released: {locks:?}");
    }

    /// The missed F4 site #2 (High): the queued waiter's
    /// WAITING->RUNNING flip (`run_lock_waiter`) had no per-attempt
    /// timeout either — a HUNG store op on that pre-execution persist
    /// parked the just-dequeued waiter forever (in-memory WAITING, live
    /// registered tail, just-acquired interval locks held, no deadline
    /// anywhere: the lock-wait deadline only bounds ACQUISITION, and the
    /// fast-failure refusal path was never reached). Post-fix the flip is
    /// time-bounded and a timeout takes the SAME refusal path as a fast
    /// failure: the waiter finalizes FAILED (truthful — it never
    /// executed, a resubmission is safe), releases the just-acquired
    /// locks, publishes nothing, and quiesces the registry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hung_waiting_to_running_persist_finalizes_queued_waiter_failed() {
        let (metadata, _historical, overlord, _dir) = setup_executor().await;

        // Another task holds the interval EXCLUSIVE: the submission
        // queues WAITING behind it.
        let interval = Interval::new(1_704_067_200_000, 1_704_153_600_000).expect("interval");
        let blocker = overlord
            .acquire_lock(
                "blocker_task",
                "lockq_hang_ds",
                interval,
                LockType::Exclusive,
                0,
            )
            .await
            .expect("acquire blocker")
            .expect("blocker granted");

        let id = overlord
            .submit_task(lock_queue_spec("lockq_hang_ds"))
            .await
            .expect("submit");
        let info = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(info.state, TaskState::Waiting, "queued task is WAITING");

        // Arm ONE hang: the FIRST choke-point call from here on is the
        // waiter's WAITING->RUNNING flip (the durable WAITING insert at
        // submit bypassed the hook); the refusal path's terminal persist
        // then succeeds, so the durable FAILED verdict can land.
        overlord
            .inject_terminal_persist_hangs
            .store(1, std::sync::atomic::Ordering::SeqCst);

        // Free the lock: the waiter acquires it and hits the hung flip.
        metadata
            .delete_lock(&blocker.id)
            .await
            .expect("release blocker");

        // RED (pre-fix): the waiter parks forever on the hung flip — the
        // task never leaves WAITING and this poll panics. GREEN: the
        // bounded flip times out and the waiter refuses to execute.
        assert_eq!(
            await_task_terminal(&overlord, &id).await,
            TaskState::Failed,
            "a queued waiter whose WAITING->RUNNING persist hangs must \
             finalize FAILED in bounded time (it never executed), not \
             strand"
        );

        // It never ran: nothing may be published.
        assert!(
            metadata
                .get_used_segments("lockq_hang_ds")
                .await
                .expect("used")
                .is_empty(),
            "a refused waiter must never publish"
        );
        // Durable verdict lands via the refusal path's bounded persist.
        let mut durable_status = String::new();
        for _ in 0..500 {
            if let Some(row) = metadata.get_task(&id).await.expect("row") {
                durable_status.clone_from(&row.status);
                if durable_status == "FAILED" {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(durable_status, "FAILED", "durable row carries the verdict");
        // The just-acquired locks are released (poll: the release runs
        // inside the finalizer, a beat behind the in-memory flip).
        let mut locks = Vec::new();
        for _ in 0..500 {
            locks = metadata.get_locks_for_task(&id).await.expect("locks");
            if locks.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            locks.is_empty(),
            "the just-acquired locks must be released: {locks:?}"
        );
        assert!(
            await_registry_quiescence(&overlord).await,
            "no tail/fence/retry entry may outlive the refused waiter"
        );
    }

    /// Crash-order (P fails): an upload failure in Phase P aborts BEFORE any
    /// metadata is committed and BEFORE the query-visible swap — no metadata
    /// row, no loaded segment, no data.
    #[tokio::test]
    async fn persist_upload_failure_blocks_metadata_and_swap() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(FailingUploadDeepStorage);
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(deep_storage);

        // An upload failure must fail the task (polled).
        let _id = submit_failed(&overlord, batch_spec("fail_ds", json!(false), rows_first())).await;

        // P failed → M never ran: no metadata rows at all.
        assert!(
            metadata
                .get_all_segments()
                .await
                .expect("all segments")
                .is_empty(),
            "no metadata row may be committed when the persist fails"
        );
        // Swap never ran: nothing query-visible.
        assert_eq!(queried_row_count(&historical, "fail_ds"), 0);
        assert_eq!(historical.segment_count(), 0);
    }

    /// Crash-order (swap fails after persist): a historical swap failure
    /// (forced via a 1-byte cache budget) rolls the committed metadata back
    /// AND best-effort deletes the orphan deep-storage blob — no orphan is
    /// left in either store.
    #[tokio::test]
    async fn swap_failure_rolls_back_metadata_and_deletes_blob() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        // 1-byte cache budget: the query-visible add in `replace_segments`
        // exceeds it and fails, exercising the post-persist swap-failure path.
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            1,
        ));
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage));

        // A swap failure must fail the task (polled).
        let _id = submit_failed(&overlord, batch_spec("swap_ds", json!(false), rows_first())).await;

        // Metadata rolled back: no rows survive.
        assert!(
            metadata
                .get_all_segments()
                .await
                .expect("all segments")
                .is_empty(),
            "metadata must be rolled back after a swap failure"
        );
        // No orphan blob: every uploaded blob was best-effort deleted.
        assert!(
            deep_storage
                .list_segments("swap_ds")
                .await
                .expect("list")
                .is_empty(),
            "the orphan deep-storage blob must be deleted after a swap failure"
        );
        assert_eq!(queried_row_count(&historical, "swap_ds"), 0);
    }

    /// Bootstrap reload: after a "restart" (a fresh, empty Historical sharing
    /// the same metadata + deep storage), `bootstrap_reload_segments`
    /// re-downloads the used segment and makes it query-visible again.
    #[tokio::test]
    async fn bootstrap_reload_restores_segments_from_deep_storage() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );

        // "First boot": ingest a segment (persisted to deep storage + metadata).
        {
            let cache1 = tempfile::tempdir().expect("cache1");
            let hist1 = Arc::new(ferrodruid_historical::Historical::new(
                cache1.path().to_path_buf(),
                10_000_000,
            ));
            let ovl1 = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist1))
                .with_deep_storage(Arc::clone(&deep_storage));
            submit_ok(&ovl1, batch_spec("boot_ds", json!(false), rows_first())).await;
            assert_eq!(queried_row_count(&hist1, "boot_ds"), 4);
        }

        // "Restart": fresh empty Historical, same metadata + deep storage.
        let cache2 = tempfile::tempdir().expect("cache2");
        let hist2 = Arc::new(ferrodruid_historical::Historical::new(
            cache2.path().to_path_buf(),
            10_000_000,
        ));
        assert_eq!(
            queried_row_count(&hist2, "boot_ds"),
            0,
            "a fresh historical starts empty"
        );
        let ovl2 = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist2))
            .with_deep_storage(Arc::clone(&deep_storage));

        let reloaded = ovl2.bootstrap_reload_segments().await.expect("bootstrap");
        assert_eq!(reloaded, 1, "exactly one used segment reloaded");
        assert!(
            hist2.is_initial_load_complete(),
            "initial-load flag restored to true after the sweep"
        );
        assert_eq!(
            queried_row_count(&hist2, "boot_ds"),
            4,
            "the reloaded segment is query-visible again"
        );
    }

    /// Bootstrap reload tolerates a phantom used row whose blob is missing
    /// from deep storage (legacy/pre-persistence): it is warn-skipped, not
    /// fatal, and the sweep still completes.
    #[tokio::test]
    async fn bootstrap_reload_skips_phantom_rows_without_blob() {
        let (metadata, historical, overlord, _deep_storage, _c, _d) =
            setup_executor_with_deep_storage().await;

        // A used metadata row that has NO corresponding deep-storage blob.
        let phantom = SegmentMetadataRow {
            id: "phantom_seg".to_string(),
            data_source: "ph_ds".to_string(),
            created_date: "2026-01-01T00:00:00.000Z".to_string(),
            start: "2026-01-01T00:00:00.000Z".to_string(),
            end: "2026-01-02T00:00:00.000Z".to_string(),
            version: "2026-01-01T00:00:00.000Z".to_string(),
            used: true,
            payload: serde_json::json!({ "dataSource": "ph_ds" }),
        };
        metadata
            .replace_segments_txn(&[], &phantom)
            .await
            .expect("insert phantom used row");

        let reloaded = overlord
            .bootstrap_reload_segments()
            .await
            .expect("bootstrap tolerates a phantom row");
        assert_eq!(reloaded, 0, "the phantom row is skipped, not reloaded");
        assert!(historical.is_initial_load_complete());
        assert_eq!(queried_row_count(&historical, "ph_ds"), 0);
    }

    /// Bootstrap reload works when the Historical runs in FG-7 spill mode:
    /// the reloaded segment is written to this instance's private spill root
    /// and is query-visible.
    #[tokio::test]
    async fn bootstrap_reload_works_in_spill_mode() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );

        // First boot (heap mode) populates deep storage + metadata.
        {
            let cache1 = tempfile::tempdir().expect("cache1");
            let hist1 = Arc::new(ferrodruid_historical::Historical::new(
                cache1.path().to_path_buf(),
                10_000_000,
            ));
            let ovl1 = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist1))
                .with_deep_storage(Arc::clone(&deep_storage));
            submit_ok(
                &ovl1,
                batch_spec("spill_boot_ds", json!(false), rows_first()),
            )
            .await;
            assert_eq!(queried_row_count(&hist1, "spill_boot_ds"), 4);
        }

        // Restart into a SPILL-mode Historical.
        let cache2 = tempfile::tempdir().expect("cache2");
        let hist_spill = Arc::new(ferrodruid_historical::Historical::with_options(
            cache2.path().to_path_buf(),
            10_000_000,
            false,
            true,
        ));
        let ovl2 = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist_spill))
            .with_deep_storage(Arc::clone(&deep_storage));

        let reloaded = ovl2
            .bootstrap_reload_segments()
            .await
            .expect("bootstrap (spill)");
        assert_eq!(reloaded, 1);
        assert_eq!(
            queried_row_count(&hist_spill, "spill_boot_ds"),
            4,
            "spill-mode reload is query-visible"
        );
    }

    /// A no-deep-storage Overlord's bootstrap reload is a no-op (returns 0)
    /// and does not disturb the initial-load flag.
    #[tokio::test]
    async fn bootstrap_reload_no_op_without_deep_storage() {
        let (_metadata, _historical, overlord, _dir) = setup_executor().await;
        let reloaded = overlord
            .bootstrap_reload_segments()
            .await
            .expect("bootstrap no-op");
        assert_eq!(reloaded, 0);
    }

    /// A [`DeepStorage`] whose `segment_exists` is always `Ok(true)` but whose
    /// `download_segment` always FAILS, counting the download attempts — so a
    /// durable row's transient-failure retry + fail-loud (H4) is observable.
    #[derive(Default)]
    struct ExistsButDownloadFailsDeepStorage {
        downloads: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DeepStorage for ExistsButDownloadFailsDeepStorage {
        async fn list_segments(
            &self,
            _data_source: &str,
        ) -> ferrodruid_deep_storage::Result<Vec<String>> {
            Ok(Vec::new())
        }
        async fn download_segment(
            &self,
            _data_source: &str,
            _segment_id: &str,
            _dest: &std::path::Path,
        ) -> ferrodruid_deep_storage::Result<()> {
            self.downloads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(ferrodruid_deep_storage::DeepStorageError::Other(
                "injected download failure (test fault hook)".to_string(),
            ))
        }
        async fn upload_segment(
            &self,
            _data_source: &str,
            _segment_id: &str,
            _src: &std::path::Path,
        ) -> ferrodruid_deep_storage::Result<()> {
            Ok(())
        }
        async fn delete_segment(
            &self,
            _data_source: &str,
            _segment_id: &str,
        ) -> ferrodruid_deep_storage::Result<()> {
            Ok(())
        }
        async fn segment_exists(
            &self,
            _data_source: &str,
            _segment_id: &str,
        ) -> ferrodruid_deep_storage::Result<bool> {
            Ok(true)
        }
    }

    /// Build a used metadata row that carries a `loadSpec` (durable) or not
    /// (legacy), with NO corresponding deep-storage blob.
    fn used_row(id: &str, ds: &str, with_load_spec: bool) -> SegmentMetadataRow {
        let mut payload = serde_json::json!({ "dataSource": ds });
        if with_load_spec {
            payload["loadSpec"] = serde_json::json!({
                "type": "local",
                "dataSource": ds,
                "segmentId": id,
            });
        }
        SegmentMetadataRow {
            id: id.to_string(),
            data_source: ds.to_string(),
            created_date: "2026-01-01T00:00:00.000Z".to_string(),
            start: "2026-01-01T00:00:00.000Z".to_string(),
            end: "2026-01-02T00:00:00.000Z".to_string(),
            version: "2026-01-01T00:00:00.000Z".to_string(),
            used: true,
            payload,
        }
    }

    /// H4: a DURABLE row (carries a `loadSpec`) whose blob is deterministically
    /// MISSING from deep storage must FAIL the bootstrap loudly — never a
    /// silent skip that then marks the node ready.
    #[tokio::test]
    async fn bootstrap_reload_fails_loud_on_durable_row_missing_blob() {
        let (metadata, historical, overlord, _deep_storage, _c, _d) =
            setup_executor_with_deep_storage().await;

        metadata
            .replace_segments_txn(&[], &used_row("durable_gone", "dur_ds", true))
            .await
            .expect("insert durable row without a blob");

        let err = overlord.bootstrap_reload_segments().await.expect_err(
            "a durable (loadSpec) row with a missing blob must fail the bootstrap (H4)",
        );
        assert!(
            format!("{err}").contains("durable segment"),
            "fail-loud error should name the durable segment, got: {err}"
        );
        assert!(
            !historical.is_initial_load_complete(),
            "the node must NOT advertise readiness with a durable segment silently absent (H4)"
        );
    }

    /// H4: a DURABLE row whose blob EXISTS but keeps failing to download is
    /// RETRIED [`BOOTSTRAP_RELOAD_ATTEMPTS`] times and then fails the bootstrap
    /// loudly (transient/corrupt → fail-closed), never a silent ready state.
    #[tokio::test]
    async fn bootstrap_reload_retries_then_fails_loud_on_durable_download_failure() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache = tempfile::tempdir().expect("cache");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache.path().to_path_buf(),
            10_000_000,
        ));
        let ds = Arc::new(ExistsButDownloadFailsDeepStorage::default());
        let deep: Arc<dyn DeepStorage> = Arc::clone(&ds) as Arc<dyn DeepStorage>;
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(deep);

        metadata
            .replace_segments_txn(&[], &used_row("dl_fail", "dl_ds", true))
            .await
            .expect("insert durable row");

        let err = overlord
            .bootstrap_reload_segments()
            .await
            .expect_err("a durable row that never downloads must fail loud after retries (H4)");
        assert!(
            format!("{err}").contains("could not be reloaded"),
            "error should describe the exhausted retries, got: {err}"
        );
        assert!(!historical.is_initial_load_complete());
        assert_eq!(
            ds.downloads.load(std::sync::atomic::Ordering::SeqCst),
            BOOTSTRAP_RELOAD_ATTEMPTS,
            "the transient download failure is retried exactly BOOTSTRAP_RELOAD_ATTEMPTS times"
        );
    }

    /// H4 regression: a LEGACY row (NO `loadSpec`) whose blob is missing is
    /// still warn-skipped and the sweep completes ready — only durable rows
    /// fail loud.
    #[tokio::test]
    async fn bootstrap_reload_legacy_missing_blob_still_skips_and_readies() {
        let (metadata, historical, overlord, _deep_storage, _c, _d) =
            setup_executor_with_deep_storage().await;

        metadata
            .replace_segments_txn(&[], &used_row("legacy_gone", "leg_ds", false))
            .await
            .expect("insert legacy row without a blob");

        let reloaded = overlord
            .bootstrap_reload_segments()
            .await
            .expect("legacy missing-blob row is tolerated");
        assert_eq!(reloaded, 0, "the legacy row is skipped, not reloaded");
        assert!(
            historical.is_initial_load_complete(),
            "a legacy (no-loadSpec) missing blob still readies the node"
        );
    }

    /// H2: a durable segment whose deep-storage blob was SWAPPED for a
    /// DIFFERENT but perfectly valid v9 artifact (so `SegmentData::open`
    /// still succeeds, yet the content is not what was committed) must FAIL
    /// the bootstrap loudly via the content-hash integrity check — never
    /// silently serve the swapped data with the metadata's offsets still
    /// committed.
    #[tokio::test]
    async fn bootstrap_reload_fails_loud_on_swapped_blob() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );

        // First boot: persist TWO datasources with DIFFERENT data, producing
        // two valid but byte-different v9 blobs on disk.
        let (victim_id, decoy_id) = {
            let cache1 = tempfile::tempdir().expect("cache1");
            let hist1 = Arc::new(ferrodruid_historical::Historical::new(
                cache1.path().to_path_buf(),
                10_000_000,
            ));
            let ovl1 = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist1))
                .with_deep_storage(Arc::clone(&deep_storage));
            submit_ok(&ovl1, batch_spec("victim_ds", json!(false), rows_first())).await;
            submit_ok(&ovl1, batch_spec("decoy_ds", json!(false), rows_second())).await;
            let victim = metadata.get_used_segments("victim_ds").await.expect("v")[0]
                .id
                .clone();
            let decoy = metadata.get_used_segments("decoy_ds").await.expect("d")[0]
                .id
                .clone();
            (victim, decoy)
        };

        // Swap: overwrite the victim's blob files with the decoy's — a
        // DIFFERENT yet perfectly valid v9 artifact, so decode passes but the
        // content identity differs.
        let victim_dir = ds_dir.path().join("victim_ds").join(&victim_id);
        let decoy_dir = ds_dir.path().join("decoy_ds").join(&decoy_id);
        for entry in std::fs::read_dir(&victim_dir).expect("read victim dir") {
            std::fs::remove_file(entry.expect("entry").path()).expect("rm victim file");
        }
        for entry in std::fs::read_dir(&decoy_dir).expect("read decoy dir") {
            let src = entry.expect("entry").path();
            let dst = victim_dir.join(src.file_name().expect("file name"));
            std::fs::copy(&src, &dst).expect("copy decoy file");
        }

        // Restart: fresh Historical, same metadata + (tampered) deep storage.
        let cache2 = tempfile::tempdir().expect("cache2");
        let hist2 = Arc::new(ferrodruid_historical::Historical::new(
            cache2.path().to_path_buf(),
            10_000_000,
        ));
        let ovl2 = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist2))
            .with_deep_storage(Arc::clone(&deep_storage));

        let err = ovl2
            .bootstrap_reload_segments()
            .await
            .expect_err("a durable segment whose blob was swapped must fail the bootstrap (H2)");
        assert!(
            format!("{err}").contains("content-hash integrity check"),
            "fail-loud error should name the integrity check, got: {err}"
        );
        assert!(
            !hist2.is_initial_load_complete(),
            "the node must NOT advertise readiness after a swapped durable blob (H2)"
        );
        assert_eq!(
            queried_row_count(&hist2, "victim_ds"),
            0,
            "the swapped segment must NOT be query-visible"
        );
    }

    /// H2 backward compat: a durable row published BEFORE compat-3 added the
    /// content hash (a `loadSpec` present, but no `sha256`) skips the integrity
    /// check and reloads its intact blob as before — the check is opt-in on the
    /// recorded hash, never a hard requirement that would strand legacy data.
    #[tokio::test]
    async fn bootstrap_reload_durable_row_without_hash_still_reloads() {
        use ferrodruid_ingest_batch::BatchIngester;

        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep_storage: Arc<dyn DeepStorage> = Arc::new(
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_dir.path().to_path_buf()),
        );

        // Upload a real, intact blob for (leg_ds, leg_seg).
        let ingester = BatchIngester::new(
            "leg_ds".to_string(),
            "__time".to_string(),
            vec!["page".to_string()],
            vec![],
        );
        const BASE_MS: i64 = 1_700_000_000_000;
        let seg = ingester
            .ingest(vec![
                json!({ "__time": BASE_MS + 1, "page": "a" }),
                json!({ "__time": BASE_MS + 2, "page": "b" }),
            ])
            .expect("ingest")
            .segment_data;
        persist_segment(deep_storage.as_ref(), "leg_ds", "leg_seg", &seg)
            .await
            .expect("persist real blob");

        // Insert a DURABLE row whose loadSpec carries NO `sha256` (legacy shape).
        let row = used_row("leg_seg", "leg_ds", true);
        assert!(
            row.payload["loadSpec"].get("sha256").is_none(),
            "legacy fixture row must not carry a sha256"
        );
        metadata
            .replace_segments_txn(&[], &row)
            .await
            .expect("insert legacy durable row");

        // Restart and reload: no recorded hash → check skipped → segment reloads.
        let cache = tempfile::tempdir().expect("cache");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage));

        let reloaded = overlord
            .bootstrap_reload_segments()
            .await
            .expect("legacy durable row without hash reloads");
        assert_eq!(reloaded, 1, "the intact legacy blob is reloaded");
        assert!(historical.is_initial_load_complete());
    }
}
