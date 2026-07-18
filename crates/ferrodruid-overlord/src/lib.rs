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
    running_tasks: RwLock<HashMap<String, TaskRecord>>,
    /// Monotonically increasing counter for generating task IDs.
    task_counter: std::sync::atomic::AtomicU64,
    /// Pluggable worker selector (registered workers + strategy).
    workers: Mutex<WorkerSelector>,
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
    lock_acquire_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Test-only fault injection: while `true`, the atomic publish
    /// metadata transaction in [`execute_index_parallel`] fails with an
    /// injected [`DruidError::Metadata`] before touching the store,
    /// exercising the publication failure path (Codex 2026-07-12 HIGH #2,
    /// re-pointed at the round-2 single-transaction publish). Sticky until
    /// cleared so every retry attempt of a task observes the same fault.
    ///
    /// [`execute_index_parallel`]: Overlord::execute_index_parallel
    #[cfg(test)]
    inject_insert_segment_failure: std::sync::atomic::AtomicBool,
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
    inject_rollback_replace_failure: std::sync::atomic::AtomicBool,
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
    supervisor_lifecycle: Mutex<()>,
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
    kafka_lifecycle_ops: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Whether the BACKGROUND resume-retry task (Codex R6 H3) is currently
    /// running, so at most ONE such task ever exists per Overlord
    /// (idempotence: `resume_kafka_supervisors` may be called again while a
    /// retry loop is still working through earlier failures). Set by
    /// [`spawn_kafka_resume_retry`](Overlord::spawn_kafka_resume_retry) via
    /// compare-and-swap, cleared by the task itself when a retry pass
    /// reports zero remaining failures.
    #[cfg(feature = "kafka-io")]
    kafka_resume_retry_active: std::sync::atomic::AtomicBool,
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
    kinesis_lifecycle_ops: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Whether the background Kinesis resume-retry task is running (the
    /// mirror of
    /// [`kafka_resume_retry_active`](Self::kafka_resume_retry_active)).
    #[cfg(feature = "kinesis-io")]
    kinesis_resume_retry_active: std::sync::atomic::AtomicBool,
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
            running_tasks: RwLock::new(HashMap::new()),
            task_counter: std::sync::atomic::AtomicU64::new(1),
            workers: Mutex::new(WorkerSelector::new(
                Vec::new(),
                WorkerSelectStrategy::RoundRobin,
            )),
            retry_policy: RetryPolicy::default(),
            lock_acquire_locks: Mutex::new(HashMap::new()),
            #[cfg(test)]
            inject_insert_segment_failure: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            inject_rollback_replace_failure: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "kafka-io")]
            kafka_supervisors: Arc::new(Mutex::new(HashMap::new())),
            supervisor_lifecycle: Mutex::new(()),
            #[cfg(feature = "kafka-io")]
            kafka_lifecycle_ops: Mutex::new(Vec::new()),
            #[cfg(feature = "kafka-io")]
            kafka_resume_retry_active: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "kinesis-io")]
            kinesis_supervisors: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "kinesis-io")]
            kinesis_lifecycle_ops: Mutex::new(Vec::new()),
            #[cfg(feature = "kinesis-io")]
            kinesis_resume_retry_active: std::sync::atomic::AtomicBool::new(false),
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
            running_tasks: RwLock::new(HashMap::new()),
            task_counter: std::sync::atomic::AtomicU64::new(1),
            workers: Mutex::new(WorkerSelector::new(
                Vec::new(),
                WorkerSelectStrategy::RoundRobin,
            )),
            retry_policy: RetryPolicy::default(),
            lock_acquire_locks: Mutex::new(HashMap::new()),
            #[cfg(test)]
            inject_insert_segment_failure: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            inject_rollback_replace_failure: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "kafka-io")]
            kafka_supervisors: Arc::new(Mutex::new(HashMap::new())),
            supervisor_lifecycle: Mutex::new(()),
            #[cfg(feature = "kafka-io")]
            kafka_lifecycle_ops: Mutex::new(Vec::new()),
            #[cfg(feature = "kafka-io")]
            kafka_resume_retry_active: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "kinesis-io")]
            kinesis_supervisors: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "kinesis-io")]
            kinesis_lifecycle_ops: Mutex::new(Vec::new()),
            #[cfg(feature = "kinesis-io")]
            kinesis_resume_retry_active: std::sync::atomic::AtomicBool::new(false),
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

    /// Submit a new ingestion task from its JSON spec.
    ///
    /// For an Overlord built with [`with_executor`] this drives the
    /// native-batch path (`index` / `index_parallel`, inline or local
    /// inputSource) end-to-end (parse spec, run batch ingester, load
    /// segment into Historical, register in metadata).  For an Overlord
    /// built with [`new`] it falls back to stub behavior (record as
    /// Pending, return ID).
    ///
    /// Returns the auto-generated task identifier.
    ///
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

        let seq = self
            .task_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = format!("{task_type}_{data_source}_{seq}");

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
        let lock_req = self.parse_lock_request(&id, &data_source, &spec)?;
        // Armed once a lock is actually granted; releases the durable lock
        // if this future is dropped (cancelled) before it commits (R4 H2).
        let mut lock_guard: Option<SubmitLockGuard> = None;
        if let Some(req) = lock_req {
            match self.try_acquire_lock(req).await? {
                true => {
                    lock_guard = Some(SubmitLockGuard {
                        armed: true,
                        task_id: id.clone(),
                        metadata: Arc::clone(&self.metadata),
                    });
                }
                false => {
                    // Stay WAITING; persist and return the id.
                    self.persist_and_store(record).await?;
                    return Ok(id);
                }
            }
        }

        // Locks (if any) granted: WAITING -> PENDING.
        record.state = TaskState::Pending;

        // If this Overlord owns a Historical and the task is a native-batch
        // type (Druid's serial `index` or parallel `index_parallel` — both
        // carry the same dataSchema/ioConfig shape), run the full ingestion
        // through RUNNING -> terminal.  Otherwise leave it PENDING (the
        // original Phase-1 stub behavior).  Pre-compat-4 only
        // `index_parallel` matched, so a serial `index` task was accepted
        // and then parked PENDING forever.
        if self.historical.is_some() && (task_type == "index" || task_type == "index_parallel") {
            // Persist a RUNNING row BEFORE publication (insert_task is an
            // upsert, so the terminal persist below updates it) so a
            // successfully executed task can never "disappear": published
            // data with no task row would 404 on /status and invite a
            // duplicate resubmit (Codex R5 H2). Residual at-least-once: a
            // crash strictly between publication and the terminal update
            // leaves the row RUNNING; a default replace-mode retry is
            // idempotent for the interval, appendToExisting retries are not
            // (documented).
            record.state = TaskState::Running;
            self.metadata.insert_task(&record.to_row()?).await?;
            self.run_with_retry(&mut record, &spec).await;
        }

        self.persist_and_store(record).await?;
        // Committed: run_with_retry's terminal paths already released any
        // locks and the task row is now persisted, so disarm the
        // cancellation guard (it must only fire on a dropped future).
        if let Some(mut guard) = lock_guard {
            guard.disarm();
        }
        Ok(id)
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
                        // Backoff is computed but not slept on in-process to
                        // keep submission synchronous; it is surfaced for
                        // schedulers via `RetryPolicy::backoff_millis`.
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
        self.metadata.insert_task(&row).await?;
        let mut tasks = self.running_tasks.write().await;
        tasks.insert(record.id.clone(), record);
        Ok(())
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
        let publish_lock = self.metadata.datasource_publish_lock(&ds_name).await;
        let _publish_guard = publish_lock.lock().await;

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

        // Phase P (persist) — upload the segment to deep storage BEFORE any
        // metadata is committed (compat-3 stage 1). Crash-consistency
        // invariant: a metadata row is only ever committed AFTER a durable
        // upload, so a restart's bootstrap reload always has a blob to
        // re-download. An upload failure aborts here via `?` — BEFORE Phase
        // M — so nothing is committed and the existing publish-failure path
        // handles it (no new failure plumbing). Batch has no offset
        // dimension, so the sequence is simply P → M → swap. Skipped (no
        // loadSpec) when no deep-storage backend is configured, preserving
        // the pre-persistence memory-resident behavior.
        let load_spec = match self.deep_storage.as_deref() {
            Some(ds) => {
                Some(persist_segment(ds, &ds_name, &segment_id, &ingested.segment_data).await?)
            }
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
        if let Err(e) = self.publish_replace_metadata(&victim_ids, &row).await {
            cleanup_orphan_blob(
                self.deep_storage.as_deref(),
                &ds_name,
                &segment_id,
                load_spec.is_some(),
                true, // no row was ever committed
            )
            .await;
            return Err(e);
        }

        // Phase 3 — one atomic query-visible swap: victims out, new
        // segment (with its datasource mapping) in, under a single
        // write-lock acquisition.
        match historical.replace_segments(
            &victim_ids,
            vec![SegmentSwapEntry {
                id: segment_id.clone(),
                data: Arc::new(ingested.segment_data),
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
                let rolled_back = match self.rollback_replace_metadata(&segment_id, &victims).await
                {
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
                if append_to_existing && !rolled_back && load_spec.is_some() {
                    *retry_suppressed = true;
                    tracing::warn!(
                        task_id,
                        data_source = %ds_name,
                        segment_id = %segment_id,
                        "appendToExisting: the query-visible swap FAILED and the \
                         metadata rollback ALSO failed, so the durable segment row + \
                         deep-storage blob are RETAINED and will be reloaded on the \
                         next restart. Suppressing retry so the same input is NOT \
                         appended again under a new id (which would double count \
                         after the restart reload). Honest limitation: the segment \
                         is NOT query-visible in THIS session — it becomes visible \
                         after the next restart's bootstrap reload (Codex R23 H1 \
                         append variant, streaming R14/H2 RetainedDurable parity)"
                    );
                }
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
                return Err(e);
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
        Ok(true)
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
    pub async fn shutdown_task(&self, id: &str) -> Result<()> {
        {
            let mut tasks = self.running_tasks.write().await;
            let task = tasks
                .get_mut(id)
                .ok_or_else(|| DruidError::Metadata(format!("task not found: {id}")))?;
            if !task.state.is_terminal() {
                task.state = TaskState::Failed;
                task.worker = None;
                task.location = None;
            }
        }
        self.release_task_locks(id).await;
        self.persist_existing(id).await?;
        Ok(())
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
    pub async fn transition_task(&self, id: &str, target: TaskState) -> Result<()> {
        {
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
            task.state = target;
            if matches!(target, TaskState::Pending | TaskState::Waiting) {
                task.worker = None;
                task.location = None;
            }
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
    pub async fn lose_worker(&self, worker_id: &str) -> Result<Vec<String>> {
        {
            let mut guard = self.workers.lock().await;
            guard.deregister(worker_id);
        }
        let mut affected = Vec::new();
        let mut to_fail = Vec::new();
        {
            let mut tasks = self.running_tasks.write().await;
            for task in tasks.values_mut() {
                if task.worker.as_deref() == Some(worker_id) && task.state == TaskState::Running {
                    affected.push(task.id.clone());
                    task.worker = None;
                    task.location = None;
                    if self.retry_policy.can_retry(task.attempt) {
                        task.state = TaskState::Pending;
                    } else {
                        task.state = TaskState::Failed;
                        to_fail.push(task.id.clone());
                    }
                }
            }
        }
        for id in &to_fail {
            self.release_task_locks(id).await;
        }
        for id in &affected {
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

        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.status,
            TaskStatus::Success,
            "task should complete successfully: {task:?}"
        );

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

        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.status,
            TaskStatus::Success,
            "rollup task should complete successfully: {task:?}"
        );

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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.status,
            TaskStatus::Success,
            "local batch task must execute to SUCCESS, not park: {task:?}"
        );

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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(task.status, TaskStatus::Success, "csv local task: {task:?}");
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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(task.status, TaskStatus::Success, "tsv local task: {task:?}");
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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.status,
            TaskStatus::Success,
            "only the .json file must be read: {task:?}"
        );
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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.state,
            TaskState::Failed,
            "symlink escape must fail terminally: {task:?}"
        );
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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.state,
            TaskState::Failed,
            "a batch spec with no inputSource must FAIL terminally, not park PENDING: {task:?}"
        );
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
        let id = overlord.submit_task(spec).await.expect("submit");
        assert_eq!(
            overlord
                .get_task(&id)
                .await
                .expect("get")
                .expect("present")
                .state,
            TaskState::Success
        );

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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.state,
            TaskState::Failed,
            "a non-string filter must fail terminally, not silently ingest all files: {task:?}"
        );
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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.state,
            TaskState::Failed,
            "a non-UTF-8 file name must fail loud, not silently drop input: {task:?}"
        );
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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.state,
            TaskState::Failed,
            "unsupported inputSource must FAIL terminally, not park PENDING: {task:?}"
        );
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
        let id = overlord.submit_task(spec).await.expect("submit");
        assert!(id.starts_with("index_"), "id carries the task type: {id}");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.status,
            TaskStatus::Success,
            "serial index task must execute: {task:?}"
        );
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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.state,
            TaskState::Failed,
            "zero matched files must FAIL terminally: {task:?}"
        );
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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.state, TaskState::Failed);
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
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(task.status, TaskStatus::Success, "batch task: {task:?}");
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

        let id1 = overlord
            .submit_task(batch_spec("replace_ds", json!(false), rows_first()))
            .await
            .expect("submit first");
        let t1 = overlord.get_task(&id1).await.expect("get").expect("some");
        assert_eq!(t1.status, TaskStatus::Success, "first task: {t1:?}");
        assert_eq!(queried_row_count(&historical, "replace_ds"), 4);

        let id2 = overlord
            .submit_task(batch_spec("replace_ds", json!(false), rows_second()))
            .await
            .expect("submit second");
        let t2 = overlord.get_task(&id2).await.expect("get").expect("some");
        assert_eq!(t2.status, TaskStatus::Success, "second task: {t2:?}");

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

        overlord
            .submit_task(batch_spec("append_ds", json!(false), rows_first()))
            .await
            .expect("submit first");
        overlord
            .submit_task(batch_spec("append_ds", json!(true), rows_second()))
            .await
            .expect("submit second");

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

        overlord
            .submit_task(batch_spec("days_ds", json!(false), rows_first()))
            .await
            .expect("submit day 1");
        let day2 = "{\"timestamp\":\"2026-01-02T00:00:01Z\",\"grp\":\"x\",\"uid\":\"u1\",\"a\":1}\n\
             {\"timestamp\":\"2026-01-02T00:00:02Z\",\"grp\":\"y\",\"uid\":\"u2\",\"a\":2}";
        overlord
            .submit_task(batch_spec("days_ds", json!(false), day2))
            .await
            .expect("submit day 2");

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

        overlord
            .submit_task(batch_spec("ds_a", json!(false), rows_first()))
            .await
            .expect("submit ds_a");
        overlord
            .submit_task(batch_spec("ds_b", json!(false), rows_first()))
            .await
            .expect("submit ds_b");

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

        overlord
            .submit_task(batch_spec(
                "default_ds",
                serde_json::Value::Null,
                rows_first(),
            ))
            .await
            .expect("submit first");
        overlord
            .submit_task(batch_spec(
                "default_ds",
                serde_json::Value::Null,
                rows_second(),
            ))
            .await
            .expect("submit second");

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
        let id = overlord
            .submit_task(batch_spec("bad_ds", json!("yes"), rows_first()))
            .await
            .expect("submit records the task");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(
            t.state,
            TaskState::Failed,
            "non-boolean appendToExisting must fail the task, got {t:?}"
        );
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
        let id1 = overlord
            .submit_task(batch_spec("span_ds", json!(false), two_day_rows))
            .await
            .expect("submit spanning ingest");
        let t1 = overlord.get_task(&id1).await.expect("get").expect("some");
        assert_eq!(t1.status, TaskStatus::Success, "first task: {t1:?}");
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
        let id2 = overlord
            .submit_task(batch_spec("span_ds", json!(false), jan1_only))
            .await
            .expect("submission itself is recorded");
        let t2 = overlord.get_task(&id2).await.expect("get").expect("some");
        assert_eq!(
            t2.state,
            TaskState::Failed,
            "a partial-overlap replace must fail closed, got {t2:?}"
        );

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
        let id3 = overlord
            .submit_task(batch_spec("span_ds", json!(false), full_span))
            .await
            .expect("submit full-span replace");
        let t3 = overlord.get_task(&id3).await.expect("get").expect("some");
        assert_eq!(t3.status, TaskStatus::Success, "full-span replace: {t3:?}");
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
        overlord
            .submit_task(batch_spec("rb_ds", json!(false), rows_first()))
            .await
            .expect("seed ingest");
        overlord
            .submit_task(batch_spec("rb_ds", json!(true), rows_second()))
            .await
            .expect("append ingest");
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
        let id = overlord
            .submit_task(batch_spec("rb_ds", json!(false), replacement))
            .await
            .expect("submission itself is recorded");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(
            t.state,
            TaskState::Failed,
            "task must fail when segment registration fails, got {t:?}"
        );

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
        let id2 = overlord
            .submit_task(batch_spec("rb_ds", json!(false), replacement))
            .await
            .expect("resubmit");
        let t2 = overlord.get_task(&id2).await.expect("get").expect("some");
        assert_eq!(t2.status, TaskStatus::Success, "recovery replace: {t2:?}");
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
        overlord
            .submit_task(batch_spec("atomic_ds", json!(false), rows_first()))
            .await
            .expect("seed ingest");

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
            overlord
                .submit_task(batch_spec("atomic_ds", json!(true), append3))
                .await
                .expect("append");
            overlord
                .submit_task(batch_spec("atomic_ds", json!(false), rows_second()))
                .await
                .expect("replace");
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
        let overlord = Arc::new(overlord);

        let lock = metadata.datasource_publish_lock("lk_ds").await;
        let guard = lock.lock().await;

        let mut publishing = {
            let overlord = Arc::clone(&overlord);
            tokio::spawn(async move {
                overlord
                    .submit_task(batch_spec("lk_ds", json!(false), rows_first()))
                    .await
            })
        };

        // Deterministic: a held tokio::Mutex can never be acquired, so the
        // publish cannot complete within the timeout.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(150), &mut publishing)
                .await
                .is_err(),
            "a publish must block while the datasource's shared publish lock is held"
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
        let id = publishing
            .await
            .expect("join publish task")
            .expect("publish succeeds after the lock is released");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(t.status, TaskStatus::Success, "task: {t:?}");
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
            let id = overlord
                .submit_task(batch_spec("rapid_ds", json!(true), one_row))
                .await
                .expect("submit append");
            let t = overlord.get_task(&id).await.expect("get").expect("some");
            assert_eq!(t.status, TaskStatus::Success, "append task: {t:?}");
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
        overlord
            .submit_task(batch_spec("dis_ds", json!(false), rows_first()))
            .await
            .expect("seed ingest");
        overlord
            .submit_task(batch_spec("dis_ds", json!(true), rows_second()))
            .await
            .expect("append ingest");
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
        let id = overlord
            .submit_task(batch_spec("dis_ds", json!(false), rows_second()))
            .await
            .expect("submission recorded");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(t.state, TaskState::Failed, "task: {t:?}");
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

        let id = overlord
            .submit_task(batch_spec("bp_ds", json!(false), rows_first()))
            .await
            .expect("submit");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(t.status, TaskStatus::Success, "task: {t:?}");
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
        let seed = overlord
            .submit_task(batch_spec("dc_ds", json!(false), rows_first()))
            .await
            .expect("seed ingest");
        let t = overlord.get_task(&seed).await.expect("get").expect("some");
        assert_eq!(t.status, TaskStatus::Success, "seed: {t:?}");
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

        let failed = overlord
            .submit_task(batch_spec("dc_ds", json!(false), rows_second()))
            .await
            .expect("submission itself is recorded");
        let t = overlord
            .get_task(&failed)
            .await
            .expect("get")
            .expect("some");
        assert_eq!(
            t.state,
            TaskState::Failed,
            "swap + rollback double failure must fail the task: {t:?}"
        );

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
        let retry = overlord
            .submit_task(batch_spec("dc_ds", json!(false), rows_second()))
            .await
            .expect("retry");
        let t = overlord.get_task(&retry).await.expect("get").expect("some");
        assert_eq!(t.status, TaskStatus::Success, "retry: {t:?}");
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
        let seed = overlord
            .submit_task(batch_spec("ap_ds", json!(true), rows_first()))
            .await
            .expect("seed append");
        let t = overlord.get_task(&seed).await.expect("get").expect("some");
        assert_eq!(t.status, TaskStatus::Success, "seed: {t:?}");
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
        let t = overlord
            .get_task(&failed)
            .await
            .expect("get")
            .expect("some");
        assert_eq!(
            t.state,
            TaskState::Failed,
            "append swap + rollback double failure fails the task: {t:?}"
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

        let id = overlord
            .submit_task(batch_spec("fail_ds", json!(false), rows_first()))
            .await
            .expect("submission itself is recorded");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(
            t.state,
            TaskState::Failed,
            "an upload failure must fail the task, got {t:?}"
        );

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

        let id = overlord
            .submit_task(batch_spec("swap_ds", json!(false), rows_first()))
            .await
            .expect("submission itself is recorded");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(
            t.state,
            TaskState::Failed,
            "a swap failure must fail the task, got {t:?}"
        );

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
            ovl1.submit_task(batch_spec("boot_ds", json!(false), rows_first()))
                .await
                .expect("ingest");
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
            ovl1.submit_task(batch_spec("spill_boot_ds", json!(false), rows_first()))
                .await
                .expect("ingest");
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
            ovl1.submit_task(batch_spec("victim_ds", json!(false), rows_first()))
                .await
                .expect("ingest victim");
            ovl1.submit_task(batch_spec("decoy_ds", json!(false), rows_second()))
                .await
                .expect("ingest decoy");
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
