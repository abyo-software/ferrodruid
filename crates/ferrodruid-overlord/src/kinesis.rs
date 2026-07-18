// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Kinesis supervisor runtime wiring (compat-5).
//!
//! Mirrors the Kafka wiring in `kafka.rs`: `create_supervisor` persists a
//! Kinesis supervisor spec; this module turns it into a running consumer
//! that polls the stream through the transport-abstracted
//! [`KinesisSource`] trait and publishes rolled segments into the shared
//! metadata store + [`Historical`] — so a Kinesis-fed datasource becomes
//! queryable exactly like a batch- or Kafka-ingested one.
//!
//! The whole consume / roll / publish / checkpoint / resume pipeline is
//! GENERIC over [`KinesisSource`], so it is fully exercised by the
//! deterministic in-memory
//! [`MockKinesisSource`](ferrodruid_ingest_kinesis::MockKinesisSource)
//! in ordinary `cargo test` — no AWS, no docker, no `kinesis-io`
//! feature. Only the construction of the real
//! `AwsKinesisSource` (in `create_supervisor` / the resume path) is
//! behind `kinesis-io`, exactly as kafka's rdkafka consumer is gated
//! while its `StreamingBuffer` is not.
//!
//! **Durability model (same P→M→swap discipline as compat-3 Kafka):**
//! each rolled segment is persisted to deep storage BEFORE its metadata
//! row is committed, and the row's payload carries the per-shard
//! sequence spans it covers (`payload.kinesisSequences`) plus the stream
//! generation marker (`streamCreationTimestampMillis`). Kinesis has no
//! server-side consumer checkpoint, so the durable segment set is the
//! ONLY checkpoint store: on restart the resume frontier is folded from
//! the durable rows ([`fold_resume_frontier`]) and each shard resumes
//! past its chain-intact durable coverage — zero loss, bounded
//! at-least-once duplication. A memory-only sink (no deep-storage
//! backend) must NOT stamp sequences (mirroring the kafka C3 guard): a
//! restart would otherwise resume past records whose only copy vanished
//! with the process. A memory-only supervisor additionally warns LOUDLY
//! at start that ingestion is non-durable and forces its no-evidence
//! resume default to `TRIM_HORIZON` (never `LATEST`), so a restart
//! re-consumes the retention window instead of seeking past the wiped
//! rows — duplication over loss, made true (R1-C2).
//!
//! **v1 limitations (documented, mirrored in the FG catalogue):**
//! resharding (shard split/merge) is unsupported — a closed shard
//! (`next_iterator = None`) stops being polled with a warning and the
//! new child shards are only picked up by a supervisor restart;
//! `taskCount` is ignored (one consumer task polls all shards); STS
//! assume-role and enhanced fan-out are not implemented.

use std::sync::Arc;
use std::time::Duration;

use ferrodruid_common::{DruidError, Result};
use ferrodruid_deep_storage::DeepStorage;
use ferrodruid_historical::{Historical, SegmentSwapEntry};
use ferrodruid_ingest_batch::IngestedSegment;
use ferrodruid_ingest_batch::{DimensionSchema, DimensionType, TsFormat};
use ferrodruid_ingest_kafka::consumer::KafkaConsumerConfig;
use ferrodruid_ingest_kafka::streaming::{
    SegmentSink, SegmentSinkError, StreamingBuffer, StreamingStats,
};
use ferrodruid_ingest_kinesis::{
    DimensionEntry, KINESIS_STREAMING_KIND, KinesisSource, KinesisSourceError,
    KinesisSupervisorSpec, SEQUENCES_PAYLOAD_KEY, STREAM_CREATION_PAYLOAD_KEY, STREAM_PAYLOAD_KEY,
    SeqNum, SeqSpan, ShardId, ShardIterator, ShardSequences, StartPosition, StreamIdentity,
    decode_batch, fold_resume_frontier, sequences_to_payload,
};
use ferrodruid_ingest_kinesis::{FrontierRowEvidence, KinesisResumeFrontier};
use ferrodruid_metadata::{MetadataStore, SegmentMetadataRow};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::persist::persist_segment;
use crate::{allocate_segment_id_inner, format_epoch_millis_iso};

/// Backoff between retries of a TRANSIENT Kinesis failure (unreachable
/// endpoint, throttling, a failed `DescribeStream` at consumer start).
/// Short in tests (which script errors deterministically).
#[cfg(not(test))]
const KINESIS_ERROR_BACKOFF: Duration = Duration::from_secs(2);
#[cfg(test)]
const KINESIS_ERROR_BACKOFF: Duration = Duration::from_millis(10);

/// Idle backoff between poll passes when every open shard returned an
/// empty page (Kinesis returns empty pages routinely; per-shard
/// `GetRecords` is rate-limited to 5/s on the real service, so a tight
/// empty-page spin would be throttled anyway).
#[cfg(not(test))]
const KINESIS_IDLE_BACKOFF: Duration = Duration::from_millis(500);
#[cfg(test)]
const KINESIS_IDLE_BACKOFF: Duration = Duration::from_millis(2);

/// Minimum interval between `GetRecords` polls of a shard that RETURNED
/// records (R3-H1 per-shard pacing). The real service caps `GetRecords`
/// at 5 calls/sec/shard; 250 ms keeps a safety margin under that cap so
/// a hot shard is drained as fast as the service allows without earning
/// throttles — and, because every shard is paced INDIVIDUALLY, a hot
/// shard can no longer drag an empty/caught-up sibling into a
/// hot-polled (throttled) spin. Tiny in tests to keep them fast.
#[cfg(not(test))]
const KINESIS_MIN_POLL_INTERVAL: Duration = Duration::from_millis(250);
#[cfg(test)]
const KINESIS_MIN_POLL_INTERVAL: Duration = Duration::from_millis(2);

/// Per-call deadline for a single Kinesis source RPC (Codex R5 H1). A hung
/// `GetRecords` — a source/proxy that accepts the call but never responds
/// — must NOT block the sequential poll loop and thus its sibling shards,
/// the flush timer, and shutdown indefinitely (the call is a bare `.await`
/// with no shutdown escape, and backlog past the retention window is lost).
/// On the deadline the call is abandoned, the iterator retained, and the
/// shard backed off like any transient failure. The real `aws-sdk-kinesis`
/// client ALSO carries an operation timeout (see `aws.rs`); this loop-level
/// guard is transport-agnostic (it bounds any `KinesisSource` hang).
#[cfg(not(test))]
const KINESIS_RPC_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const KINESIS_RPC_TIMEOUT: Duration = Duration::from_millis(50);

// ---------------------------------------------------------------------------
// Supervisor handle
// ---------------------------------------------------------------------------

/// Handle to a running Kinesis supervisor consumer task, kept by the
/// [`Overlord`](crate::Overlord) so `shutdown_supervisor` can stop it.
/// Mirrors `KafkaSupervisorHandle` (drain discipline, sticky replay
/// obligation) with the Kinesis pair key `(data_source, stream)`.
///
/// The replay-recovery story is SIMPLER than Kafka's: Kinesis has no
/// committed offset that could skip past lost rows — the resume frontier
/// is derived from the durable segment set alone, and rows that were
/// consumed but never published never advanced it. ANY restart / resume /
/// re-create of the same `(data_source, stream)` pair therefore
/// re-consumes exactly the lost span (see
/// [`recoverable_by`](Self::recoverable_by)), so no schema/broker
/// fingerprints are needed to prove a repost recovers the sentinel.
pub(crate) struct KinesisSupervisorHandle {
    shutdown_tx: mpsc::Sender<()>,
    /// The consumer task. Taken (→ `None`) the first time a drain joins
    /// it; from then on the drain OUTCOME lives in
    /// [`replay_error`](Self::replay_error) — a tokio [`JoinHandle`] must
    /// not be polled again after completion.
    handle: Option<JoinHandle<StreamingStats>>,
    /// Sticky replay obligation (the kafka R30 F3 discipline): `Some`
    /// once a completed drain reported lost rows. The failed handle stays
    /// registered as a sentinel so every retried shutdown/suspend
    /// re-reports the failure and keeps refusing the tombstone until a
    /// replay path (restart resume / same-pair re-create) supersedes it.
    replay_error: Option<String>,
    /// Target datasource (pair key half 1).
    pub(crate) data_source: String,
    /// Source stream (pair key half 2).
    pub(crate) stream: String,
}

impl KinesisSupervisorHandle {
    /// Signal the consumer to flush its residual buffer and exit, then
    /// wait for it to drain, RETURNING the stop outcome. `Err` when the
    /// stats report lost rows (failed mid-stream/final publish, a fatal
    /// consume stop, or a panicked task): the caller must NOT record the
    /// supervisor as cleanly stopped (tombstone/suspended) — keeping the
    /// metadata ACTIVE lets a restart/resume re-consume the lost span
    /// from the stream (the durable frontier never advanced past it).
    /// `&mut self`: the outcome is CACHED so a retried drain re-observes
    /// the same failure (the obligation cannot be forgotten).
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            let _ = self.shutdown_tx.send(()).await;
            match handle.await {
                Ok(stats) if !stats.replay_required() => {}
                Ok(stats) => {
                    let mut causes: Vec<&str> = Vec::new();
                    if stats.mid_stream_flush_failed {
                        causes.push("FAILED at least one mid-stream segment publish");
                    }
                    if stats.final_flush_failed {
                        causes.push("FAILED its final drain");
                    }
                    if stats.fatal_consume_error {
                        causes.push(
                            "STOPPED on a fatal consume error (auth failure or a \
                             deleted stream) without delivering the stream's records",
                        );
                    }
                    self.replay_error = Some(format!(
                        "the Kinesis consumer for datasource '{}' / stream '{}' {}: the \
                         affected rows never became queryable (consumed {}, published {} \
                         segments). The supervisor was NOT recorded as stopped — its \
                         metadata stays active so a restart/resume (or a re-create of the \
                         same pair) re-consumes the lost span from the stream (the \
                         durable sequence frontier never advanced past it)",
                        self.data_source,
                        self.stream,
                        causes.join(" AND "),
                        stats.total_consumed,
                        stats.total_published,
                    ));
                }
                Err(e) => {
                    self.replay_error = Some(format!(
                        "the Kinesis consumer task for datasource '{}' / stream '{}' did \
                         not run to completion ({e}); its residual buffer state is \
                         unknown, so the final drain is treated as FAILED and the \
                         supervisor was NOT recorded as stopped (a restart/resume \
                         re-consumes from the durable frontier)",
                        self.data_source, self.stream,
                    ));
                }
            }
        }
        match &self.replay_error {
            None => Ok(()),
            Some(reason) => Err(DruidError::Ingestion(reason.clone())),
        }
    }

    /// Whether the consumer task has already exited — or was already
    /// joined by a drain. A finished handle is stale and must not block a
    /// repost, count as a live pair, or dedup a resume.
    pub(crate) fn is_finished(&self) -> bool {
        self.handle.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Whether this handle is a REPLAY-REQUIRED sentinel: a completed
    /// drain reported lost rows and cached the reason, while the
    /// supervisor's metadata was deliberately LEFT ACTIVE so a stream
    /// re-consume can rebuild them.
    pub(crate) fn replay_required(&self) -> bool {
        self.replay_error.is_some()
    }

    /// Whether `parsed` would RECOVER this replay-required sentinel: any
    /// spec targeting the SAME `(data_source, stream)` pair qualifies —
    /// its consumer resumes from the durable frontier, and the lost rows
    /// (consumed but never published) are ABOVE that frontier, so they
    /// are re-consumed regardless of the spec's start position. A
    /// different pair (or a non-Kinesis spec) cannot, and is refused.
    pub(crate) fn recoverable_by(&self, parsed: &KinesisSupervisorSpec) -> bool {
        parsed.data_schema.data_source == self.data_source && parsed.io_config.stream == self.stream
    }
}

// ---------------------------------------------------------------------------
// Publish tail (kinesis provenance over the SHARED primitives)
// ---------------------------------------------------------------------------

/// [`SegmentSink`] that appends each rolled Kinesis segment into the
/// shared metadata + [`Historical`], through the SAME P→M→swap publish
/// primitives as the Kafka sink ([`persist_segment`],
/// [`allocate_segment_id_inner`], `replace_segments_txn`, the Historical
/// swap, orphan-blob cleanup) — only the stamped provenance differs:
/// `kind = "kinesis-streaming"`, `stream` + `streamArn` +
/// `streamCreationTimestampMillis` (not topic/cluster), and the
/// per-shard checkpoint under `payload.kinesisSequences`.
pub(crate) struct OverlordKinesisSink {
    pub(crate) metadata: Arc<MetadataStore>,
    pub(crate) historical: Arc<Historical>,
    /// Deep-storage backend: when configured, each rolled segment is
    /// PERSISTED before its metadata row is committed so a restart's
    /// bootstrap reload rebuilds it durably. `None` keeps the
    /// memory-resident behavior — and (CRITICAL, the kafka C3 guard
    /// mirrored) suppresses the `kinesisSequences` stamp, because a
    /// restart must never resume past records whose only copy vanished
    /// with the process.
    pub(crate) deep_storage: Option<Arc<dyn DeepStorage>>,
    /// Publishing supervisor id (`taskId` in the payload; diagnostics).
    pub(crate) task_id: String,
    /// Source stream name — the `payload.stream` the resume frontier
    /// matches on.
    pub(crate) stream: String,
    /// Stream identity resolved at consumer start (`DescribeStream`).
    /// `streamCreationTimestampMillis` is the generation marker the
    /// frontier requires to MATCH before advancing a resume (the ARN is
    /// reused on a same-name recreate, so it is diagnostics only).
    /// `None` = unresolved: sequences are NOT stamped (identity-less
    /// checkpoints must never seed a resume frontier — after a
    /// delete+recreate they would name a dead generation's records).
    pub(crate) identity: Option<StreamIdentity>,
}

impl OverlordKinesisSink {
    /// Publish one rolled segment ALONG WITH the per-shard sequence
    /// spans it covers, so the durable row carries the resume
    /// checkpoint. The loop treats `Ok` as "the covered spans are
    /// durable" — there is no separate commit step (Kinesis has no
    /// broker-side checkpoint; the durable row IS the checkpoint).
    pub(crate) async fn publish_with_sequences(
        &self,
        segment: IngestedSegment,
        sequences: &ShardSequences,
    ) -> std::result::Result<(), SegmentSinkError> {
        // `RetainedDurable` maps to `Ok` (the durable row reloads on the
        // next restart, so re-consuming/re-publishing it would
        // double-count) and a genuine failure to `Err` (the loop drops
        // the batch and the broken span chain re-consumes it on restart:
        // at-least-once, no loss) — the kafka R14 H2 discipline.
        match publish_kinesis_segment_persisted(
            &self.metadata,
            &self.historical,
            self.deep_storage.as_deref(),
            &self.task_id,
            &self.stream,
            self.identity.as_ref(),
            sequences,
            segment,
        )
        .await
        {
            Ok(_id) | Err(KinesisPublishError::RetainedDurable(_id)) => Ok(()),
            Err(KinesisPublishError::Failed(e)) => Err(SegmentSinkError(e.to_string())),
        }
    }
}

impl SegmentSink for OverlordKinesisSink {
    async fn publish(&self, segment: IngestedSegment) -> std::result::Result<(), SegmentSinkError> {
        self.publish_with_sequences(segment, &ShardSequences::new())
            .await
    }

    fn commits_offsets(&self) -> bool {
        // Checkpoints are durable exactly when segments are persisted —
        // the same C3 rule as the Kafka sink.
        self.deep_storage.is_some()
    }
}

/// Failure outcome of [`publish_kinesis_segment_persisted`] — the same
/// two-class split as the Kafka publish tail (R14 H2): `Failed` leaves
/// nothing durable (re-consume the batch), `RetainedDurable` means the
/// swap failed AND the rollback failed, so the durable row + blob
/// survive and reload on restart (the checkpoint is effectively durable;
/// do NOT re-consume, or the restart reload double-counts).
#[derive(Debug)]
enum KinesisPublishError {
    /// Genuine publish failure with nothing durable left behind.
    Failed(DruidError),
    /// Swap failed and rollback failed: durable row + blob retained
    /// (reloaded on restart; not query-visible this session). Carries
    /// the retained segment id (diagnostics).
    RetainedDurable(String),
}

impl From<DruidError> for KinesisPublishError {
    fn from(e: DruidError) -> Self {
        Self::Failed(e)
    }
}

/// Publish one rolled Kinesis segment as an APPEND: **P (persist) → M
/// (metadata) → swap**, under the datasource publish lock, with the
/// same failure discipline as the Kafka tail (rollback the metadata row
/// on swap failure; best-effort delete the orphan blob only when the
/// rollback succeeded; classify a swap+rollback double failure as
/// retained-durable).
///
/// Provenance stamped: `kind = "kinesis-streaming"`, `stream`, and —
/// when the stream identity was resolved — `streamArn` +
/// `streamCreationTimestampMillis` (the generation marker). The
/// per-shard checkpoint `kinesisSequences` is stamped ONLY when the
/// segment is DURABLE (`load_spec` present) AND the identity is
/// resolved (mirror of kafka.rs's C3 + R4 H4 guard): a memory-only or
/// identity-less checkpoint must never seed a resume frontier.
#[allow(clippy::too_many_arguments)]
async fn publish_kinesis_segment_persisted(
    metadata: &Arc<MetadataStore>,
    historical: &Arc<Historical>,
    deep_storage: Option<&dyn DeepStorage>,
    task_id: &str,
    stream: &str,
    identity: Option<&StreamIdentity>,
    sequences: &ShardSequences,
    ingested: IngestedSegment,
) -> std::result::Result<String, KinesisPublishError> {
    let ds_name = ingested.data_source.clone();
    let start_iso = format_epoch_millis_iso(ingested.interval.start_millis);
    // Half-open end one millisecond past the max so the segment fully
    // covers the data range (matches the batch + kafka paths).
    let end_iso = format_epoch_millis_iso(ingested.interval.end_millis.saturating_add(1));
    let version = ingested.version.clone();
    let num_rows = ingested.num_rows;

    // Serialize the whole publication per datasource on the shared
    // publish lock (id alloc must be race-free with the insert).
    let publish_lock = metadata.datasource_publish_lock(&ds_name).await;
    let _guard = publish_lock.lock().await;

    let segment_id = allocate_segment_id_inner(
        metadata, historical, &ds_name, &start_iso, &end_iso, &version,
    )
    .await?;

    // Phase P — upload BEFORE any metadata is committed (crash
    // consistency: a metadata row only ever follows a durable upload).
    let load_spec = match deep_storage {
        Some(ds) => Some(persist_segment(ds, &ds_name, &segment_id, &ingested.segment_data).await?),
        None => None,
    };

    let now_iso = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    let mut payload = serde_json::json!({
        "dataSource": ds_name,
        "numRows": num_rows,
        "taskId": task_id,
        "kind": KINESIS_STREAMING_KIND,
        STREAM_PAYLOAD_KEY: stream,
    });
    if let Some(id) = identity {
        payload["streamArn"] = serde_json::Value::String(id.stream_arn.clone());
        payload[STREAM_CREATION_PAYLOAD_KEY] =
            serde_json::json!(id.stream_creation_timestamp_millis);
    }
    if let Some(spec) = &load_spec {
        payload["loadSpec"] = spec.to_json();
    }
    // Durable resume checkpoint. Stamped ONLY when the segment is
    // actually DURABLE (`load_spec` present): a memory-only segment must
    // not seed a resume frontier, or a restart would seek past records
    // whose only copy vanished with the process (loss) — the kafka C3
    // guard, verbatim. And ONLY alongside a RESOLVED stream identity:
    // sequence numbers are only meaningful within one stream GENERATION,
    // so an identity-less checkpoint could, after a delete+recreate,
    // make a dead generation's positions look skippable.
    if load_spec.is_some() && !sequences.is_empty() {
        if identity.is_some() {
            payload[SEQUENCES_PAYLOAD_KEY] = sequences_to_payload(sequences);
        } else {
            tracing::warn!(
                data_source = %ds_name,
                stream,
                segment_id = %segment_id,
                "durable kinesis streaming segment published WITHOUT kinesisSequences: \
                 the stream identity was not resolved at consumer start, and \
                 identity-less checkpoints must never seed a resume frontier (they \
                 could name a dead generation's records after a stream recreate). \
                 A restart re-consumes this segment's span from the stream \
                 (at-least-once duplication, never loss)",
            );
        }
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

    // Phase M — one atomic metadata transaction (append: empty victim
    // set). On failure the Phase-P blob is now unreferenced; best-effort
    // delete it before surfacing the error (H8).
    if let Err(e) = metadata.replace_segments_txn(&[], &row).await {
        crate::cleanup_orphan_blob(
            deep_storage,
            &ds_name,
            &segment_id,
            load_spec.is_some(),
            true, // no row was ever committed
        )
        .await;
        return Err(e.into());
    }

    // Query-visible swap LAST. On failure: roll the metadata row back
    // under the still-held publish lock, clean the orphan blob only if
    // the rollback removed the row (H5), and classify the residual (H2).
    match historical.replace_segments(
        &[],
        vec![SegmentSwapEntry {
            id: segment_id.clone(),
            data: Arc::new(ingested.segment_data),
            datasource: Some(ds_name.clone()),
        }],
    ) {
        Ok(_) => {
            tracing::info!(
                task_id,
                data_source = %ds_name,
                segment_id = %segment_id,
                num_rows,
                "kinesis streaming segment published",
            );
            Ok(segment_id)
        }
        Err(e) => {
            let rolled_back = match metadata.rollback_replace_txn(&segment_id, &[]).await {
                Ok(()) => true,
                Err(restore_err) => {
                    tracing::error!(
                        task_id,
                        segment_id = %segment_id,
                        error = %restore_err,
                        "rollback could not un-publish kinesis segment metadata after \
                         swap failure",
                    );
                    false
                }
            };
            crate::cleanup_orphan_blob(
                deep_storage,
                &ds_name,
                &segment_id,
                load_spec.is_some(),
                rolled_back,
            )
            .await;
            if !rolled_back && load_spec.is_some() {
                tracing::warn!(
                    task_id,
                    data_source = %ds_name,
                    segment_id = %segment_id,
                    error = %e,
                    "kinesis streaming publish: the query-visible swap FAILED and the \
                     metadata rollback ALSO failed, so the durable segment row + blob \
                     are RETAINED and reload on the next restart. Treating its \
                     sequence checkpoint as durable so the loop does NOT re-consume \
                     and re-publish it (which would double-count after the restart \
                     reload). Honest limitation: not query-visible in THIS session",
                );
                Err(KinesisPublishError::RetainedDurable(segment_id))
            } else {
                Err(KinesisPublishError::Failed(e))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Spec → shared streaming-buffer config
// ---------------------------------------------------------------------------

/// Map a Kinesis dimension entry onto the shared typed dimension schema
/// (mirror of the kafka runtime's `dimension_entry_to_schema`).
fn dimension_entry_to_schema(entry: &DimensionEntry) -> DimensionSchema {
    match entry {
        DimensionEntry::String(name) => DimensionSchema::string(name.clone()),
        DimensionEntry::Typed { name, dim_type } => {
            let dt = match dim_type.as_str() {
                "long" => DimensionType::Long,
                "float" => DimensionType::Float,
                "double" => DimensionType::Double,
                _ => DimensionType::String,
            };
            DimensionSchema::new(name.clone(), dt)
        }
    }
}

/// Build the SHARED streaming-buffer schema config from a (validated)
/// Kinesis supervisor spec, so the consume loop reuses the existing
/// [`StreamingBuffer`] / `BatchIngester` pipeline unchanged. The
/// Kafka-transport fields (`brokers`, `group_id`,
/// `additional_properties`) are unused placeholders; `topic` carries the
/// stream name for log parity.
pub(crate) fn kinesis_buffer_config(
    supervisor_id: &str,
    spec: &KinesisSupervisorSpec,
) -> KafkaConsumerConfig {
    let dim_schemas: Vec<DimensionSchema> = spec
        .data_schema
        .dimensions_spec
        .dimensions
        .iter()
        .map(dimension_entry_to_schema)
        .collect();
    let max_rows = spec
        .tuning_config
        .as_ref()
        .and_then(|t| t.max_rows_per_segment)
        .unwrap_or(5_000_000);
    // Shared spec→TsFormat mapping (compat-9). The spec was already
    // validated (`validate_kinesis_spec` rejects unimplemented formats
    // before persist), so the Err branch is unreachable here; fall back
    // to `auto` defensively, matching the pre-helper behavior.
    let timestamp_format =
        TsFormat::from_spec_format(&spec.data_schema.timestamp_spec.format).unwrap_or_default();
    KafkaConsumerConfig {
        brokers: String::new(),
        topic: spec.io_config.stream.clone(),
        group_id: format!("ferrodruid-{supervisor_id}"),
        data_source: spec.data_schema.data_source.clone(),
        timestamp_column: spec.data_schema.timestamp_spec.column.clone(),
        timestamp_format,
        dim_schemas,
        metrics_specs: spec.data_schema.metrics_spec.clone(),
        max_rows_per_segment: max_rows,
        segment_flush_interval_ms: 10_000,
        use_earliest_offset: spec.io_config.use_earliest_sequence_number == Some(true),
        additional_properties: std::collections::HashMap::new(),
        output_dir: None,
    }
}

// ---------------------------------------------------------------------------
// The consume loop (generic over KinesisSource)
// ---------------------------------------------------------------------------

/// Parameters of one supervisor's consume loop, decoupled from the spec
/// so tests can shrink the flush interval etc.
pub(crate) struct KinesisLoopParams {
    /// Supervisor id (stamped as `taskId`, used in logs).
    pub supervisor_id: String,
    /// Source stream name.
    pub stream: String,
    /// Spec-derived start position for shards with NO durable resume
    /// evidence (`TRIM_HORIZON` / `LATEST`). Honored only on the
    /// DURABLE path: a memory-only supervisor (no deep storage) forces
    /// this to `TRIM_HORIZON` inside the loop (R1-C2 — a `LATEST`
    /// default would skip records whose only copy a restart wiped).
    pub default_start: StartPosition,
    /// Shared streaming-buffer schema config (see
    /// [`kinesis_buffer_config`]); its `segment_flush_interval_ms`
    /// drives the time-based roll.
    pub buffer_config: KafkaConsumerConfig,
}

/// Per-shard consume cursor: the live iterator, the resume/seek
/// position, and the sequence bookkeeping that becomes the durable
/// checkpoint span.
struct ShardCursor {
    shard: ShardId,
    /// The live iterator; `None` = needs a (re-)seek (fresh start,
    /// after an expired-iterator error, or after a failed seek).
    iterator: Option<ShardIterator>,
    /// The position used for the INITIAL seek (resume-derived), reused
    /// verbatim when a re-seek happens before anything was consumed.
    start: StartPosition,
    /// Last sequence number consumed in THIS session — or, at start,
    /// the durable frontier's resume point for an
    /// `AfterSequenceNumber` resume, so the first rolled span's
    /// `prev_last` CHAINS onto the durable coverage (without the link
    /// the resume walk would refuse to skip the new span and every
    /// restart would re-consume it: duplication, though never loss).
    last_consumed: Option<SeqNum>,
    /// Sequence span covered by records consumed since the last roll.
    /// Taken at roll time into the published row's `kinesisSequences`;
    /// dropped (chain break → restart re-consumes) when a roll/publish
    /// fails.
    pending: Option<SeqSpan>,
    /// A closed shard (resharding — `next_iterator = None` after the
    /// last record): polled no further in v1.
    closed: bool,
    /// Earliest instant this shard may be polled again (R3-H1 per-shard
    /// pacing): records → `+KINESIS_MIN_POLL_INTERVAL`, empty page →
    /// `+KINESIS_IDLE_BACKOFF`, transient error → `+KINESIS_ERROR_BACKOFF`
    /// — recorded here instead of sleeping inline, so one shard's
    /// backoff never stalls the others and a hot shard never drags a
    /// caught-up sibling into a hot-polled (throttled) spin.
    next_poll_at: tokio::time::Instant,
}

impl ShardCursor {
    fn new(shard: ShardId, start: StartPosition) -> Self {
        let last_consumed = match &start {
            StartPosition::AfterSequenceNumber(s) => Some(s.clone()),
            _ => None,
        };
        Self {
            shard,
            iterator: None,
            start,
            last_consumed,
            pending: None,
            closed: false,
            // Due immediately: the first poll must not wait.
            next_poll_at: tokio::time::Instant::now(),
        }
    }

    /// Record one consumed record's sequence number (dead-lettered
    /// records included — the resume frontier advances past
    /// consumed-but-unstored records exactly as Druid commits past
    /// unparseable ones).
    fn note_consumed(&mut self, seq: SeqNum) {
        match &mut self.pending {
            None => {
                self.pending = Some(SeqSpan::new(
                    seq.clone(),
                    seq.clone(),
                    self.last_consumed.clone(),
                ));
            }
            Some(span) => span.last = seq.clone(),
        }
        self.last_consumed = Some(seq);
    }

    /// Where a RE-seek must start: after the last consumed sequence
    /// (nothing lost, nothing re-read), or the original start position
    /// when nothing was consumed yet.
    fn reseek_position(&self) -> StartPosition {
        match &self.last_consumed {
            Some(s) => StartPosition::AfterSequenceNumber(s.clone()),
            None => self.start.clone(),
        }
    }
}

/// Move every shard's pending span out (for stamping into the rolled
/// segment's metadata payload).
fn take_pending(cursors: &mut [ShardCursor]) -> ShardSequences {
    let mut sequences = ShardSequences::new();
    for cursor in cursors.iter_mut() {
        if let Some(span) = cursor.pending.take() {
            sequences.insert(cursor.shard.as_str().to_owned(), span);
        }
    }
    sequences
}

/// Fold the durable resume frontier for `(data_source, stream)` from the
/// used segment rows: a row contributes only if its payload carries the
/// Kinesis streaming provenance for THIS stream, and only
/// identity-confirmed rows whose blob actually reloaded
/// (`historical.has_segment`) may ADVANCE the resume (the rest floor
/// it) — the whole gating lives in [`fold_resume_frontier`].
async fn derive_kinesis_resume_frontier(
    metadata: &MetadataStore,
    historical: &Historical,
    data_source: &str,
    stream: &str,
    current_creation_millis: i64,
) -> Result<KinesisResumeFrontier> {
    let rows = metadata.get_used_segments(data_source).await?;
    let evidence: Vec<FrontierRowEvidence<'_>> = rows
        .iter()
        .map(|row| FrontierRowEvidence {
            payload: &row.payload,
            loaded: historical.has_segment(&row.id),
        })
        .collect();
    Ok(fold_resume_frontier(
        &evidence,
        stream,
        Some(current_creation_millis),
    ))
}

/// Fetch a shard iterator at `position`, falling back to `TRIM_HORIZON`
/// when a RESUME position (`At`/`AfterSequenceNumber`) is rejected by
/// the service (`Api` error — on real Kinesis a retention-trimmed
/// sequence raises `InvalidArgumentException`): re-consuming the
/// retained log is bounded at-least-once duplication, never loss,
/// whereas refusing to seek would stall the shard forever. Transient
/// errors (transport/throttle) are NOT downgraded — the caller retries
/// the same position later.
async fn get_iterator_with_trim_fallback(
    source: &dyn KinesisSource,
    stream: &str,
    shard: &ShardId,
    position: &StartPosition,
) -> std::result::Result<ShardIterator, KinesisSourceError> {
    match source.get_shard_iterator(stream, shard, position).await {
        Ok(it) => Ok(it),
        Err(KinesisSourceError::Api(detail))
            if matches!(
                position,
                StartPosition::AtSequenceNumber(_) | StartPosition::AfterSequenceNumber(_)
            ) =>
        {
            tracing::warn!(
                stream,
                shard = %shard,
                error = %detail,
                "kinesis resume position rejected by the service (retention-trimmed \
                 sequence?): falling back to TRIM_HORIZON — the shard's retained log \
                 is re-consumed (bounded at-least-once duplication, never loss)",
            );
            source
                .get_shard_iterator(stream, shard, &StartPosition::TrimHorizon)
                .await
        }
        Err(e) => Err(e),
    }
}

/// Roll the buffer and publish through the sink, moving the covered
/// shard spans into the durable row. Returns `false` on a lost batch
/// (roll or publish failure): the rolled rows are gone from memory and
/// their spans are DROPPED, so the span chain breaks and a restart
/// re-consumes exactly that batch (at-least-once, no loss). An empty
/// roll retains the pending spans for the next non-empty roll
/// (commit-past-dead-letters, the kafka behavior).
async fn flush_buffer(
    buffer: &mut StreamingBuffer,
    cursors: &mut [ShardCursor],
    sink: &OverlordKinesisSink,
    stats: &mut StreamingStats,
    published: &mut u64,
    is_final: bool,
) -> bool {
    match buffer.roll() {
        Ok(None) => true,
        Ok(Some(segment)) => {
            let sequences = take_pending(cursors);
            match sink.publish_with_sequences(segment, &sequences).await {
                Ok(()) => {
                    *published += 1;
                    true
                }
                Err(e) => {
                    tracing::error!(
                        supervisor_id = %sink.task_id,
                        stream = %sink.stream,
                        error = %e,
                        is_final,
                        "kinesis streaming publish FAILED: the rolled rows are dropped \
                         from memory and their sequence spans are NOT checkpointed, so \
                         a restart re-consumes them from the stream (at-least-once, \
                         no loss)",
                    );
                    if is_final {
                        stats.final_flush_failed = true;
                    } else {
                        stats.mid_stream_flush_failed = true;
                    }
                    false
                }
            }
        }
        Err(e) => {
            // The buffer drained its rows before the ingest failed —
            // they are lost from memory. Drop the covered spans too so
            // the chain breaks and a restart re-consumes them.
            let _ = take_pending(cursors);
            tracing::error!(
                supervisor_id = %sink.task_id,
                stream = %sink.stream,
                error = %e,
                is_final,
                "kinesis streaming segment roll FAILED: the buffered rows are dropped \
                 and their sequence spans are NOT checkpointed (a restart re-consumes \
                 them)",
            );
            if is_final {
                stats.final_flush_failed = true;
            } else {
                stats.mid_stream_flush_failed = true;
            }
            false
        }
    }
}

/// Retry a startup step (`DescribeStream` / metadata read /
/// `ListShards`) until it succeeds, a FATAL error surfaces (`Ok(None)` =
/// stop the consumer), or shutdown is requested (`Err(())`).
macro_rules! retry_startup_step {
    ($stats:ident, $shutdown_rx:ident, $label:expr, $attempt:expr) => {
        loop {
            match $attempt {
                Ok(v) => break v,
                Err(KinesisSourceError::Auth(detail)) => {
                    tracing::error!(
                        error = %detail,
                        concat!("kinesis consumer: FATAL auth failure during ", $label,
                                " — stopping (fix credentials and re-create/restart)"),
                    );
                    $stats.fatal_consume_error = true;
                    return $stats;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        concat!("kinesis consumer: transient failure during ", $label,
                                " — retrying"),
                    );
                    tokio::select! {
                        _ = $shutdown_rx.recv() => return $stats,
                        () = tokio::time::sleep(KINESIS_ERROR_BACKOFF) => {}
                    }
                }
            }
        }
    };
}

/// The Kinesis consume loop: resolve the stream identity → derive the
/// per-shard durable resume frontier → snapshot the shard set →
/// `GetRecords` across all open shards, each at its OWN cadence
/// (`next_poll_at` per-shard pacing: min-interval when records flow,
/// idle backoff on empty pages, error backoff on transient failures —
/// never an inline sleep, so no shard can starve or stall another) →
/// decode → buffer → roll → publish (durable, sequence-stamped) — until
/// shutdown, then final-drain. Fully generic over [`KinesisSource`]
/// (mock-drivable in every build); mandatory expired-iterator recovery
/// included.
pub(crate) async fn run_kinesis_streaming_loop(
    source: Box<dyn KinesisSource>,
    params: KinesisLoopParams,
    metadata: Arc<MetadataStore>,
    historical: Arc<Historical>,
    deep_storage: Option<Arc<dyn DeepStorage>>,
    mut shutdown_rx: mpsc::Receiver<()>,
) -> StreamingStats {
    let mut stats = StreamingStats::default();
    let stream = params.stream.clone();
    let data_source = params.buffer_config.data_source.clone();

    // R1-C2: a memory-only supervisor (no deep-storage backend) never
    // stamps checkpoints, so a restart's frontier is EMPTY and every
    // shard would use the spec default verbatim — a LATEST default
    // would then seek PAST retained records whose only copy was the
    // in-heap segments the restart wiped: silent loss. Memory-only
    // therefore (a) warns LOUDLY that ingestion is non-durable and (b)
    // forces the no-evidence resume default to TRIM_HORIZON, so a
    // restart re-consumes the retention window instead of skipping it —
    // the honest non-durable trade: bounded at-least-once duplication,
    // never loss. The durable path is untouched: the spec default
    // (LATEST / TRIM_HORIZON) is honored, and the durable frontier
    // governs whenever evidence exists.
    let default_start = if deep_storage.is_some() {
        params.default_start.clone()
    } else {
        tracing::warn!(
            supervisor_id = %params.supervisor_id,
            stream,
            data_source = %data_source,
            spec_default = ?params.default_start,
            "kinesis ingestion is NON-DURABLE for this supervisor: no deep-storage \
             backend is configured, so rolled segments live only in process memory \
             and every consumed row is LOST on restart (deep storage is REQUIRED \
             for durability). The no-evidence resume default is forced to \
             TRIM_HORIZON (never LATEST) so a restart re-consumes the stream's \
             retention window instead of seeking past records whose only copy \
             vanished with the process — duplication over loss",
        );
        StartPosition::TrimHorizon
    };

    // 1. Stream identity (generation marker). Retried until reachable:
    //    like kafka's lazy connect, a temporarily unreachable endpoint
    //    must not kill a legitimately created supervisor.
    let identity: StreamIdentity = retry_startup_step!(
        stats,
        shutdown_rx,
        "DescribeStream",
        source.describe_stream(&stream).await
    );

    // 2. Durable resume frontier from the used segment rows.
    let frontier = retry_startup_step!(
        stats,
        shutdown_rx,
        "resume-frontier derivation",
        derive_kinesis_resume_frontier(
            &metadata,
            &historical,
            &data_source,
            &stream,
            identity.stream_creation_timestamp_millis,
        )
        .await
        .map_err(|e| KinesisSourceError::Transport(e.to_string()))
    );

    // 3. Shard set, snapshotted ONCE (resharding unsupported in v1).
    let shard_ids: Vec<ShardId> = retry_startup_step!(
        stats,
        shutdown_rx,
        "ListShards",
        source.list_shards(&stream).await
    );
    if shard_ids.is_empty() {
        tracing::warn!(
            supervisor_id = %params.supervisor_id,
            stream,
            "kinesis stream has no shards to consume (the shard set is snapshotted \
             at consumer start; restart the supervisor after resharding)",
        );
    }

    let mut cursors: Vec<ShardCursor> = shard_ids
        .into_iter()
        .map(|shard| {
            let start = frontier.start_position_for(shard.as_str(), default_start.clone());
            tracing::info!(
                supervisor_id = %params.supervisor_id,
                stream,
                shard = %shard,
                start = ?start,
                stream_recreated = frontier.stream_recreated,
                "kinesis shard start position derived from the durable frontier",
            );
            ShardCursor::new(shard, start)
        })
        .collect();

    let mut buffer = StreamingBuffer::from_config(&params.buffer_config);
    let sink = OverlordKinesisSink {
        metadata,
        historical,
        deep_storage,
        task_id: params.supervisor_id.clone(),
        stream: stream.clone(),
        identity: Some(identity),
    };
    let flush_interval =
        Duration::from_millis(params.buffer_config.segment_flush_interval_ms.max(1));
    let mut next_flush = tokio::time::Instant::now() + flush_interval;
    let mut published = 0u64;

    'main: loop {
        // Shutdown is level-checked between shard polls so a stop request
        // never waits on a slow endpoint longer than one call.
        match shutdown_rx.try_recv() {
            Ok(()) | Err(mpsc::error::TryRecvError::Disconnected) => break 'main,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }
        // Poll only DUE shards (R3-H1): each shard is paced by its own
        // `next_poll_at`, so a hot shard never forces an extra poll of a
        // caught-up one and one shard's backoff never delays another.
        let due_now = tokio::time::Instant::now();
        for i in 0..cursors.len() {
            if cursors[i].closed || cursors[i].next_poll_at > due_now {
                continue;
            }
            if cursors[i].iterator.is_none() {
                let pos = cursors[i].reseek_position();
                match get_iterator_with_trim_fallback(
                    source.as_ref(),
                    &stream,
                    &cursors[i].shard,
                    &pos,
                )
                .await
                {
                    Ok(it) => cursors[i].iterator = Some(it),
                    Err(KinesisSourceError::Auth(detail)) => {
                        tracing::error!(
                            stream, error = %detail,
                            "kinesis consumer: FATAL auth failure fetching a shard \
                             iterator — stopping",
                        );
                        stats.fatal_consume_error = true;
                        break 'main;
                    }
                    Err(e) => {
                        tracing::warn!(
                            stream, shard = %cursors[i].shard, error = %e,
                            "kinesis consumer: could not fetch a shard iterator \
                             (retried after the error backoff)",
                        );
                        // Backoff via the schedule, NOT an inline sleep:
                        // the other shards keep their own cadence.
                        cursors[i].next_poll_at =
                            tokio::time::Instant::now() + KINESIS_ERROR_BACKOFF;
                        continue;
                    }
                }
            }
            let Some(iterator) = cursors[i].iterator.clone() else {
                continue;
            };
            let out_result = match tokio::time::timeout(
                KINESIS_RPC_TIMEOUT,
                source.get_records(&iterator),
            )
            .await
            {
                Ok(result) => result,
                Err(_elapsed) => {
                    // Hung RPC (Codex R5 H1): abandon the call, keep the
                    // iterator, back the shard off like a transient error,
                    // and move on — a stuck GetRecords must not freeze the
                    // loop, its siblings, the flush timer, or shutdown.
                    tracing::warn!(
                        stream, shard = %cursors[i].shard,
                        "kinesis GetRecords did not respond within the per-call RPC \
                         deadline; abandoning the call, keeping the iterator, and \
                         backing off (a hung source must not block the poll loop or \
                         shutdown)",
                    );
                    cursors[i].next_poll_at = tokio::time::Instant::now() + KINESIS_ERROR_BACKOFF;
                    continue;
                }
            };
            match out_result {
                Ok(out) => {
                    // Schedule the shard's next poll from the outcome:
                    // records → the throttle-safe minimum interval (drain
                    // fast, stay under 5 GetRecords/s/shard); empty page
                    // → the idle backoff (the shard is caught up).
                    cursors[i].next_poll_at = tokio::time::Instant::now()
                        + if out.records.is_empty() {
                            KINESIS_IDLE_BACKOFF
                        } else {
                            KINESIS_MIN_POLL_INTERVAL
                        };
                    for record in &out.records {
                        // Sequence bookkeeping FIRST — even a
                        // dead-lettered record was consumed, and the
                        // frontier advances past it (Druid's
                        // commit-past-unparseable behavior).
                        match SeqNum::parse(&record.sequence_number) {
                            Ok(seq) => cursors[i].note_consumed(seq),
                            Err(e) => tracing::warn!(
                                stream, shard = %cursors[i].shard, error = %e,
                                "kinesis record carries an unparseable sequence number; \
                                 it is ingested but cannot advance the durable \
                                 checkpoint (a restart may re-consume it: duplication, \
                                 never loss)",
                            ),
                        }
                        // Loud, provenance-carrying decode pre-filter
                        // (JSON-object records only in v1).
                        let decoded =
                            decode_batch(cursors[i].shard.as_str(), std::slice::from_ref(record));
                        if decoded.rows.is_empty() {
                            continue; // dead letters already warn-logged
                        }
                        // Single source of ingestion validation: the
                        // shared buffer re-parses and enforces timestamp
                        // presence, depth/size bounds, and the exact-long
                        // storage guard, exactly as the kafka path.
                        match buffer.push_payload(&record.data) {
                            Ok(should_roll) => {
                                if should_roll {
                                    flush_buffer(
                                        &mut buffer,
                                        &mut cursors,
                                        &sink,
                                        &mut stats,
                                        &mut published,
                                        false,
                                    )
                                    .await;
                                    next_flush = tokio::time::Instant::now() + flush_interval;
                                }
                            }
                            Err(e) => tracing::warn!(
                                stream,
                                shard = %cursors[i].shard,
                                sequence_number = %record.sequence_number,
                                partition_key = %record.partition_key,
                                error = %e,
                                "kinesis record failed ingest validation — dead-lettered \
                                 (not ingested, not retried)",
                            ),
                        }
                    }
                    match out.next_iterator {
                        Some(it) => cursors[i].iterator = Some(it),
                        None => {
                            tracing::warn!(
                                stream, shard = %cursors[i].shard,
                                "kinesis shard CLOSED by a resharding split/merge (live \
                                 resharding is NOT supported in v1): this parent shard is \
                                 fully consumed and no longer polled, but its CHILD shards \
                                 are NOT discovered by this consumer — a SUPERVISOR RESTART \
                                 is REQUIRED to pick them up. DATA-LOSS RISK: if the \
                                 supervisor is not restarted before the stream's retention \
                                 window expires, the child shards' records are trimmed and \
                                 PERMANENTLY LOST",
                            );
                            cursors[i].iterator = None;
                            cursors[i].closed = true;
                        }
                    }
                }
                Err(e) if e.is_expired_iterator() => {
                    // MANDATORY recovery: re-seek AFTER the last consumed
                    // sequence (or the original start) and continue — an
                    // unhandled expiry stalls the shard forever.
                    tracing::info!(
                        stream, shard = %cursors[i].shard,
                        "kinesis shard iterator expired (5-minute TTL): re-seeking \
                         after the last consumed sequence and continuing",
                    );
                    cursors[i].iterator = None;
                    // Re-seek promptly on the next pass: the expiry is
                    // not a throttle, so no pacing penalty.
                    cursors[i].next_poll_at = tokio::time::Instant::now();
                }
                Err(KinesisSourceError::Auth(detail)) => {
                    tracing::error!(
                        stream, error = %detail,
                        "kinesis consumer: FATAL auth failure on GetRecords — stopping \
                         (the residual buffer is final-drained; the supervisor is NOT \
                         recorded as cleanly stopped)",
                    );
                    stats.fatal_consume_error = true;
                    break 'main;
                }
                Err(KinesisSourceError::StreamNotFound(detail)) => {
                    tracing::error!(
                        stream, error = %detail,
                        "kinesis consumer: the stream disappeared mid-session (deleted \
                         or recreated) — stopping; a restart re-resolves the stream \
                         identity and, on a detected recreate, re-consumes from \
                         TRIM_HORIZON",
                    );
                    stats.fatal_consume_error = true;
                    break 'main;
                }
                Err(e) => {
                    // Throttled / transport / api: transient — back THIS
                    // shard off via its schedule (no inline sleep: a
                    // throttled shard must never stall the whole
                    // round-robin, R3-H1), keep the iterator, retry.
                    tracing::warn!(
                        stream, shard = %cursors[i].shard, error = %e,
                        "kinesis GetRecords failed transiently; backing off",
                    );
                    cursors[i].next_poll_at = tokio::time::Instant::now() + KINESIS_ERROR_BACKOFF;
                }
            }
        }
        if tokio::time::Instant::now() >= next_flush {
            flush_buffer(
                &mut buffer,
                &mut cursors,
                &sink,
                &mut stats,
                &mut published,
                false,
            )
            .await;
            next_flush = tokio::time::Instant::now() + flush_interval;
        }
        // Sleep until the EARLIEST due deadline — the soonest shard
        // `next_poll_at` (over non-closed shards; if every shard is
        // closed, the flush timer alone) capped by `next_flush` — so the
        // loop wakes exactly when a shard or a flush is due: no
        // hot-spin, no cross-shard stall. An already-due deadline makes
        // `sleep_until` return immediately (a plain yield), and the
        // select keeps shutdown responsive under a continuous flow.
        let wake_at = cursors
            .iter()
            .filter(|c| !c.closed)
            .map(|c| c.next_poll_at)
            .min()
            .map_or(next_flush, |earliest| earliest.min(next_flush));
        tokio::select! {
            _ = shutdown_rx.recv() => break 'main,
            () = tokio::time::sleep_until(wake_at) => {}
        }
    }

    // Final drain: roll + publish the residual buffer. A failure marks
    // `final_flush_failed`, which the handle surfaces as a NON-clean stop
    // (the tombstone is refused so a restart re-consumes the lost span).
    flush_buffer(
        &mut buffer,
        &mut cursors,
        &sink,
        &mut stats,
        &mut published,
        true,
    )
    .await;
    stats.total_consumed = buffer.total_consumed();
    stats.total_published = published;
    stats
}

// ---------------------------------------------------------------------------
// Consumer start (handle construction)
// ---------------------------------------------------------------------------

/// Spawn [`run_kinesis_streaming_loop`] with explicit params and return
/// its supervisor handle (the seam tests drive with a
/// [`MockKinesisSource`](ferrodruid_ingest_kinesis::MockKinesisSource)
/// and a shrunk flush interval).
pub(crate) fn start_kinesis_consumer_with_params(
    source: Box<dyn KinesisSource>,
    params: KinesisLoopParams,
    metadata: Arc<MetadataStore>,
    historical: Arc<Historical>,
    deep_storage: Option<Arc<dyn DeepStorage>>,
) -> KinesisSupervisorHandle {
    let data_source = params.buffer_config.data_source.clone();
    let stream = params.stream.clone();
    let supervisor_id = params.supervisor_id.clone();
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
    tracing::info!(
        supervisor_id = %supervisor_id,
        data_source = %data_source,
        stream = %stream,
        durable = deep_storage.is_some(),
        "starting Kinesis consumer: segments are persisted and the per-shard \
         sequence checkpoint is stamped into each durable row; a restart resumes \
         past the durable frontier (at-least-once). Without a deep-storage \
         backend, segments are memory-only, NO checkpoint is stamped, and the \
         resume default is forced to TRIM_HORIZON (see the non-durability \
         warning at loop start)",
    );
    let handle = tokio::spawn(async move {
        let stats = run_kinesis_streaming_loop(
            source,
            params,
            metadata,
            historical,
            deep_storage,
            shutdown_rx,
        )
        .await;
        if stats.replay_required() {
            tracing::error!(
                supervisor_id = %supervisor_id,
                consumed = stats.total_consumed,
                published = stats.total_published,
                mid_stream_flush_failed = stats.mid_stream_flush_failed,
                final_flush_failed = stats.final_flush_failed,
                fatal_consume_error = stats.fatal_consume_error,
                "kinesis supervisor consumer stopped WITH LOST BUFFERED ROWS: those \
                 rows never became queryable — the durable frontier never advanced \
                 past them, so a restart/resume of this (datasource, stream) pair \
                 re-consumes them from the stream",
            );
        } else {
            tracing::info!(
                supervisor_id = %supervisor_id,
                consumed = stats.total_consumed,
                published = stats.total_published,
                "kinesis supervisor consumer stopped",
            );
        }
        stats
    });
    KinesisSupervisorHandle {
        shutdown_tx,
        handle: Some(handle),
        replay_error: None,
        data_source,
        stream,
    }
}

/// Spawn a Kinesis consumer for an ALREADY-validated supervisor spec:
/// derives the shared buffer config + start position from the spec,
/// warns once about the parse-only options (`awsAssumedRoleArn`,
/// `taskCount > 1`), and delegates to
/// [`start_kinesis_consumer_with_params`].
pub(crate) fn start_kinesis_consumer(
    supervisor_id: &str,
    source: Box<dyn KinesisSource>,
    spec: &KinesisSupervisorSpec,
    metadata: Arc<MetadataStore>,
    historical: Arc<Historical>,
    deep_storage: Option<Arc<dyn DeepStorage>>,
) -> KinesisSupervisorHandle {
    spec.io_config.log_unsupported_options();
    let params = KinesisLoopParams {
        supervisor_id: supervisor_id.to_string(),
        stream: spec.io_config.stream.clone(),
        default_start: spec.io_config.start_position(),
        buffer_config: kinesis_buffer_config(supervisor_id, spec),
    };
    start_kinesis_consumer_with_params(source, params, metadata, historical, deep_storage)
}

// ---------------------------------------------------------------------------
// Tests — mock-driven, NO AWS / docker / kinesis-io feature required.
// The real-AWS/LocalStack end-to-end lives outside this crate.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use ferrodruid_deep_storage::LocalDeepStorage;
    use ferrodruid_ingest_batch::BatchIngester;
    use ferrodruid_ingest_kinesis::MockKinesisSource;
    use serde_json::json;

    async fn setup() -> (Arc<MetadataStore>, Arc<Historical>, tempfile::TempDir) {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create store"));
        metadata.initialize().await.expect("init schema");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(Historical::new(cache_dir.path().to_path_buf(), 10_000_000));
        (metadata, historical, cache_dir)
    }

    /// Sum a `count` timeseries over all time through the real query
    /// path — what a SQL `COUNT(*)` observes, proving the published rows
    /// are query-visible (not merely present in metadata).
    fn queried_row_count(historical: &Historical, ds: &str) -> i64 {
        let query: ferrodruid_query::DruidQuery = serde_json::from_value(json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": ds},
            "intervals": ["2000-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z"],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("build count query");
        historical
            .execute_query(&query)
            .expect("execute count query")
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
                    .sum(),
                _ => 0,
            })
            .sum()
    }

    /// Poll the real query path until `ds` serves exactly `expect` rows.
    async fn wait_for_count(historical: &Historical, ds: &str, expect: i64) {
        for _ in 0..400 {
            if queried_row_count(historical, ds) == expect {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!(
            "timed out waiting for {expect} query-visible rows in {ds}; have {}",
            queried_row_count(historical, ds)
        );
    }

    /// Realistic epoch-millis base (2023-11-14) inside the 2000..2100
    /// query window.
    const T0: i64 = 1_700_000_000_000;

    fn row(off: i64, page: &str) -> serde_json::Value {
        json!({"__time": T0 + off, "page": page})
    }

    fn loop_params(supervisor_id: &str, stream: &str, ds: &str) -> KinesisLoopParams {
        KinesisLoopParams {
            supervisor_id: supervisor_id.to_string(),
            stream: stream.to_string(),
            default_start: StartPosition::TrimHorizon,
            buffer_config: KafkaConsumerConfig {
                brokers: String::new(),
                topic: stream.to_string(),
                group_id: format!("ferrodruid-{supervisor_id}"),
                data_source: ds.to_string(),
                timestamp_column: "__time".to_string(),
                timestamp_format: TsFormat::Auto,
                dim_schemas: vec![DimensionSchema::string("page".to_string())],
                metrics_specs: vec![],
                max_rows_per_segment: 5_000_000,
                segment_flush_interval_ms: 100,
                use_earliest_offset: true,
                additional_properties: std::collections::HashMap::new(),
                output_dir: None,
            },
        }
    }

    /// Build an [`IngestedSegment`] from JSON rows via the same
    /// `BatchIngester` the streaming buffer uses.
    fn segment(ds: &str, rows: &[serde_json::Value]) -> IngestedSegment {
        let ingester = BatchIngester::new(
            ds.to_string(),
            "__time".to_string(),
            vec!["page".to_string()],
            vec![],
        );
        ingester.ingest(rows.to_vec()).expect("ingest rows")
    }

    fn seq(s: &str) -> SeqNum {
        SeqNum::parse(s).expect("valid seq")
    }

    // -- sink guards (the CRITICAL durability stamps) -----------------------

    /// A MEMORY-ONLY sink (no deep-storage backend) must NOT stamp
    /// `kinesisSequences` — a restart would otherwise resume past records
    /// whose only copy vanished with the process (loss). Mirror of the
    /// kafka C3 guard.
    #[tokio::test]
    async fn memory_only_sink_never_stamps_sequences() {
        let (metadata, historical, _dir) = setup().await;
        let sink = OverlordKinesisSink {
            metadata: Arc::clone(&metadata),
            historical: Arc::clone(&historical),
            deep_storage: None,
            task_id: "sup-k".to_string(),
            stream: "st".to_string(),
            identity: Some(StreamIdentity {
                stream_arn: "arn:aws:kinesis:us-east-1:0:stream/st".to_string(),
                stream_creation_timestamp_millis: 111,
            }),
        };
        let mut seqs = ShardSequences::new();
        seqs.insert(
            "shard-a".to_string(),
            SeqSpan::new(seq("10"), seq("20"), None),
        );
        sink.publish_with_sequences(segment("kin_mem", &[row(0, "a")]), &seqs)
            .await
            .expect("publish");
        let rows = metadata.get_used_segments("kin_mem").await.expect("rows");
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0].payload.get(SEQUENCES_PAYLOAD_KEY).is_none(),
            "memory-only publish must NOT stamp kinesisSequences: {:?}",
            rows[0].payload
        );
        assert!(rows[0].payload.get("loadSpec").is_none());
        assert_eq!(
            rows[0]
                .payload
                .get("kind")
                .and_then(serde_json::Value::as_str),
            Some(KINESIS_STREAMING_KIND)
        );
        // The trait impl (`SegmentSink::publish`) publishes without
        // sequences and reports non-durable checkpointing.
        assert!(!sink.commits_offsets());
        SegmentSink::publish(&sink, segment("kin_mem", &[row(1, "b")]))
            .await
            .expect("trait publish");
        assert_eq!(queried_row_count(&historical, "kin_mem"), 2);
    }

    /// A DURABLE publish with an UNRESOLVED stream identity must stamp
    /// `loadSpec` (the segment IS durable) but NOT `kinesisSequences`
    /// (identity-less checkpoints could name a dead generation's records
    /// after a stream recreate). With identity resolved, everything is
    /// stamped and folds back into a resume frontier.
    #[tokio::test]
    async fn durable_sink_stamps_sequences_only_with_identity() {
        let (metadata, historical, _dir) = setup().await;
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep: Arc<dyn DeepStorage> =
            Arc::new(LocalDeepStorage::new(ds_dir.path().to_path_buf()));
        let mut seqs = ShardSequences::new();
        seqs.insert(
            "shard-a".to_string(),
            SeqSpan::new(seq("10"), seq("20"), None),
        );

        // Identity UNRESOLVED → durable, but no checkpoint.
        let sink = OverlordKinesisSink {
            metadata: Arc::clone(&metadata),
            historical: Arc::clone(&historical),
            deep_storage: Some(Arc::clone(&deep)),
            task_id: "sup-k".to_string(),
            stream: "st".to_string(),
            identity: None,
        };
        sink.publish_with_sequences(segment("kin_dur", &[row(0, "a")]), &seqs)
            .await
            .expect("publish");
        let rows = metadata.get_used_segments("kin_dur").await.expect("rows");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].payload.get("loadSpec").is_some(), "durable");
        assert!(
            rows[0].payload.get(SEQUENCES_PAYLOAD_KEY).is_none(),
            "identity-less durable publish must NOT stamp kinesisSequences"
        );

        // Identity RESOLVED → checkpoint stamped, and it folds back into
        // an advancing resume frontier.
        let sink = OverlordKinesisSink {
            identity: Some(StreamIdentity {
                stream_arn: "arn:aws:kinesis:us-east-1:0:stream/st".to_string(),
                stream_creation_timestamp_millis: 111,
            }),
            ..sink
        };
        assert!(sink.commits_offsets());
        sink.publish_with_sequences(segment("kin_dur", &[row(1, "b")]), &seqs)
            .await
            .expect("publish");
        let rows = metadata.get_used_segments("kin_dur").await.expect("rows");
        let stamped: Vec<_> = rows
            .iter()
            .filter(|r| r.payload.get(SEQUENCES_PAYLOAD_KEY).is_some())
            .collect();
        assert_eq!(stamped.len(), 1);
        assert_eq!(
            stamped[0]
                .payload
                .get(STREAM_CREATION_PAYLOAD_KEY)
                .and_then(serde_json::Value::as_i64),
            Some(111)
        );
        assert!(stamped[0].payload.get("streamArn").is_some());
        let evidence: Vec<FrontierRowEvidence<'_>> = rows
            .iter()
            .map(|r| FrontierRowEvidence {
                payload: &r.payload,
                loaded: true,
            })
            .collect();
        let frontier = fold_resume_frontier(&evidence, "st", Some(111));
        assert_eq!(
            frontier.start_position_for("shard-a", StartPosition::Latest),
            StartPosition::AfterSequenceNumber(seq("20")),
            "the stamped checkpoint must round-trip into an advancing resume"
        );
    }

    // -- the consume loop, end to end (mock source) -------------------------

    /// THE compat-5 key proof. Phase 1: a mock stream (2 shards, 6
    /// records) is consumed by the real loop with a real LocalDeepStorage
    /// + MetadataStore + Historical; the rows become query-visible
    /// through the real query path and every published row carries the
    /// durable `kinesisSequences` checkpoint + generation stamp. Phase 2
    /// (RESTART): a fresh Historical + fresh loop over the SAME metadata
    /// + deep storage, with the mock still holding every record
    /// (TRIM_HORIZON would redeliver all 6) plus 2 NEW records — the
    /// durable frontier resumes each shard PAST its published coverage,
    /// so the outcome is exactly 8 rows: zero loss (the new records
    /// arrive) and no duplication (the old ones are not re-consumed).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mock_consume_restart_resumes_zero_loss_no_duplication() {
        let (metadata, hist1, _dir1) = setup().await;
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep: Arc<dyn DeepStorage> =
            Arc::new(LocalDeepStorage::new(ds_dir.path().to_path_buf()));

        let mock = MockKinesisSource::new("st");
        mock.add_shard("shardId-000000000000");
        mock.add_shard("shardId-000000000001");
        for i in 0..3 {
            mock.push_json("shardId-000000000000", "pk", &row(i, "a"));
            mock.push_json("shardId-000000000001", "pk", &row(100 + i, "b"));
        }

        // Phase 1: consume + publish durable segments.
        let mut handle = start_kinesis_consumer_with_params(
            Box::new(mock.clone()),
            loop_params("kin-sup", "st", "kin_ds"),
            Arc::clone(&metadata),
            Arc::clone(&hist1),
            Some(Arc::clone(&deep)),
        );
        wait_for_count(&hist1, "kin_ds", 6).await;
        handle.shutdown().await.expect("clean phase-1 stop");
        assert!(!handle.replay_required());

        // Every published row is durable and checkpoint-stamped.
        let rows = metadata.get_used_segments("kin_ds").await.expect("rows");
        assert!(!rows.is_empty());
        for r in &rows {
            assert_eq!(
                r.payload.get("kind").and_then(serde_json::Value::as_str),
                Some(KINESIS_STREAMING_KIND)
            );
            assert_eq!(
                r.payload
                    .get(STREAM_PAYLOAD_KEY)
                    .and_then(serde_json::Value::as_str),
                Some("st")
            );
            assert_eq!(
                r.payload
                    .get(STREAM_CREATION_PAYLOAD_KEY)
                    .and_then(serde_json::Value::as_i64),
                Some(1_700_000_000_000),
                "generation stamp: {:?}",
                r.payload
            );
            assert!(
                r.payload.get("loadSpec").is_some(),
                "durable: {:?}",
                r.payload
            );
            assert!(
                r.payload.get(SEQUENCES_PAYLOAD_KEY).is_some(),
                "checkpoint: {:?}",
                r.payload
            );
        }

        // RESTART: fresh empty Historical; bootstrap reload re-downloads
        // the durable segments (query-visible again with zero replay).
        let cache2 = tempfile::tempdir().expect("cache2");
        let hist2 = Arc::new(Historical::new(cache2.path().to_path_buf(), 10_000_000));
        let ovl2 = crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist2))
            .with_deep_storage(Arc::clone(&deep));
        ovl2.bootstrap_reload_segments().await.expect("bootstrap");
        assert_eq!(queried_row_count(&hist2, "kin_ds"), 6);

        // The mock still retains ALL records (a TRIM_HORIZON consumer
        // would redeliver them); add 2 genuinely new ones.
        mock.push_json("shardId-000000000000", "pk", &row(10, "a"));
        mock.push_json("shardId-000000000001", "pk", &row(110, "b"));

        // Phase 2: a fresh consume loop over the SAME metadata + deep
        // storage. The durable frontier must resume each shard PAST its
        // published coverage: only the 2 new records are ingested.
        let mut handle2 = start_kinesis_consumer_with_params(
            Box::new(mock.clone()),
            loop_params("kin-sup", "st", "kin_ds"),
            Arc::clone(&metadata),
            Arc::clone(&hist2),
            Some(Arc::clone(&deep)),
        );
        wait_for_count(&hist2, "kin_ds", 8).await;
        // Settle for several flush intervals: the count must STAY 8 —
        // any re-consumption of phase-1 records would push it past 8.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            queried_row_count(&hist2, "kin_ds"),
            8,
            "zero loss (the 2 new records) AND no duplication (the 6 \
             published records are not re-consumed)"
        );
        handle2.shutdown().await.expect("clean phase-2 stop");
    }

    /// R1-C2: a MEMORY-ONLY supervisor (no deep-storage backend) never
    /// stamps checkpoints, so on restart its frontier is empty and the
    /// spec default would be used verbatim — a LATEST default would seek
    /// PAST retained records whose only copy was the in-heap segments
    /// the restart wiped (silent loss). The loop must force the
    /// no-evidence default to TRIM_HORIZON: the pre-pushed records below
    /// are reachable ONLY from TRIM_HORIZON, so consuming them proves
    /// the override (re-consuming the retained log each restart is the
    /// honest non-durable trade: duplication, never loss).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_only_consumer_defaults_to_trim_horizon_never_latest() {
        let (metadata, historical, _dir) = setup().await;
        let mock = MockKinesisSource::new("st");
        for i in 0..3 {
            mock.push_json("s1", "pk", &row(i, "a"));
        }
        let mut params = loop_params("kin-mem-trim", "st", "kin_mem_trim");
        params.default_start = StartPosition::Latest; // spec default
        let mut handle = start_kinesis_consumer_with_params(
            Box::new(mock.clone()),
            params,
            Arc::clone(&metadata),
            Arc::clone(&historical),
            None, // memory-only: no deep storage
        );
        wait_for_count(&historical, "kin_mem_trim", 3).await;
        handle.shutdown().await.expect("clean stop");
    }

    /// The DURABLE path is untouched by the R1-C2 memory-only override:
    /// with a deep-storage backend and an empty frontier, the spec's
    /// LATEST default is honored — pre-existing records are skipped and
    /// only records produced after the seek arrive. (The frontier-based
    /// resume on the durable path is proven by
    /// `mock_consume_restart_resumes_zero_loss_no_duplication`.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_consumer_honors_spec_latest_default() {
        let (metadata, historical, _dir) = setup().await;
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep: Arc<dyn DeepStorage> =
            Arc::new(LocalDeepStorage::new(ds_dir.path().to_path_buf()));
        let mock = MockKinesisSource::new("st");
        for i in 0..3 {
            mock.push_json("s1", "pk", &row(i, "old"));
        }
        let mut params = loop_params("kin-lat", "st", "kin_lat");
        params.default_start = StartPosition::Latest;
        let mut handle = start_kinesis_consumer_with_params(
            Box::new(mock.clone()),
            params,
            Arc::clone(&metadata),
            Arc::clone(&historical),
            Some(deep),
        );
        // Wait until the LATEST iterator has been taken and polled at
        // least once (so the pre-pushed records are provably behind it),
        // then push new records: only those may arrive.
        for _ in 0..400 {
            if mock.get_records_calls() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(mock.get_records_calls() >= 1, "consumer never polled");
        mock.push_json("s1", "pk", &row(10, "new"));
        mock.push_json("s1", "pk", &row(11, "new"));
        wait_for_count(&historical, "kin_lat", 2).await;
        // Settle: the count must STAY 2 — consuming any of the 3
        // pre-existing records would mean LATEST was not honored.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            queried_row_count(&historical, "kin_lat"),
            2,
            "spec LATEST honored on the durable path: pre-existing records \
             are skipped, new ones arrive"
        );
        handle.shutdown().await.expect("clean stop");
    }

    /// Stream delete+recreate across a restart: the durable rows are
    /// stamped with the OLD creation timestamp, so the frontier marks
    /// the stream recreated and every shard re-consumes from
    /// TRIM_HORIZON — records pushed BEFORE the phase-2 loop started are
    /// picked up (a LATEST start would have missed them; a trusted stale
    /// frontier would have seeked a dead generation's positions).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recreated_stream_reconsumes_from_trim_horizon() {
        let (metadata, hist1, _dir1) = setup().await;
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep: Arc<dyn DeepStorage> =
            Arc::new(LocalDeepStorage::new(ds_dir.path().to_path_buf()));

        let mock = MockKinesisSource::new("st");
        for i in 0..4 {
            mock.push_json("s1", "pk", &row(i, "a"));
        }
        let mut handle = start_kinesis_consumer_with_params(
            Box::new(mock.clone()),
            loop_params("kin-rc", "st", "kin_rc"),
            Arc::clone(&metadata),
            Arc::clone(&hist1),
            Some(Arc::clone(&deep)),
        );
        wait_for_count(&hist1, "kin_rc", 4).await;
        handle.shutdown().await.expect("clean stop");

        // Delete+recreate: records wiped, NEW creation timestamp (the
        // ARN stays the same — it cannot detect this). Then 3 new
        // records, pushed BEFORE the restarted loop exists.
        mock.recreate_stream(1_800_000_000_000);
        for i in 0..3 {
            mock.push_json("s1", "pk", &row(50 + i, "n"));
        }

        let cache2 = tempfile::tempdir().expect("cache2");
        let hist2 = Arc::new(Historical::new(cache2.path().to_path_buf(), 10_000_000));
        let ovl2 = crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&hist2))
            .with_deep_storage(Arc::clone(&deep));
        ovl2.bootstrap_reload_segments().await.expect("bootstrap");
        assert_eq!(queried_row_count(&hist2, "kin_rc"), 4);

        // Use a LATEST default so consuming the pre-pushed records can
        // ONLY be explained by the recreation → TRIM_HORIZON escalation.
        let mut params = loop_params("kin-rc", "st", "kin_rc");
        params.default_start = StartPosition::Latest;
        let mut handle2 = start_kinesis_consumer_with_params(
            Box::new(mock.clone()),
            params,
            Arc::clone(&metadata),
            Arc::clone(&hist2),
            Some(Arc::clone(&deep)),
        );
        wait_for_count(&hist2, "kin_rc", 7).await; // 4 reloaded + 3 new
        handle2.shutdown().await.expect("clean stop");
        // The new generation's rows are stamped with the NEW marker.
        let rows = metadata.get_used_segments("kin_rc").await.expect("rows");
        assert!(rows.iter().any(|r| {
            r.payload
                .get(STREAM_CREATION_PAYLOAD_KEY)
                .and_then(serde_json::Value::as_i64)
                == Some(1_800_000_000_000)
        }));
    }

    /// The MANDATORY expired-iterator recovery: mid-consume, every
    /// outstanding iterator dies (the real 5-minute TTL); the loop must
    /// re-fetch AFTER the last consumed sequence and continue — no
    /// stall, no loss, no duplication.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_iterator_recovers_without_loss_or_duplication() {
        let (metadata, historical, _dir) = setup().await;
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep: Arc<dyn DeepStorage> =
            Arc::new(LocalDeepStorage::new(ds_dir.path().to_path_buf()));

        let mock = MockKinesisSource::new("st");
        mock.push_json("s1", "pk", &row(0, "a"));
        mock.push_json("s1", "pk", &row(1, "a"));
        let mut handle = start_kinesis_consumer_with_params(
            Box::new(mock.clone()),
            loop_params("kin-exp", "st", "kin_exp"),
            Arc::clone(&metadata),
            Arc::clone(&historical),
            Some(deep),
        );
        wait_for_count(&historical, "kin_exp", 2).await;

        // Kill every outstanding iterator, then produce more records:
        // the loop's next GetRecords fails with ExpiredIterator and must
        // recover by re-seeking after the last consumed sequence.
        mock.expire_iterators();
        mock.push_json("s1", "pk", &row(2, "a"));
        mock.push_json("s1", "pk", &row(3, "a"));
        wait_for_count(&historical, "kin_exp", 4).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            queried_row_count(&historical, "kin_exp"),
            4,
            "recovery must neither lose nor duplicate records"
        );
        handle.shutdown().await.expect("clean stop");
    }

    /// R3-H1 regression (per-shard `GetRecords` pacing): shard A is HOT
    /// (a deep backlog delivered ONE record per call, so every poll of A
    /// returns records for the whole window) while shard B exists but
    /// stays EMPTY (caught up). The un-paced loop never slept while ANY
    /// shard returned records, so B was hot-polled every round-robin
    /// pass — on real Kinesis that exceeds the 5 GetRecords/s/shard cap
    /// and gets B throttled. With per-shard pacing, no shard may be
    /// polled before its `next_poll_at`, so the TOTAL call count over a
    /// fixed wall-clock window is hard-bounded (the mock exposes only a
    /// GLOBAL `get_records_calls` counter, so the bound is asserted on
    /// the total): 500 ms at the 2 ms test-mode floor admits ~251
    /// polls/shard (~502 total); 2_000 leaves generous scheduling slack
    /// while the un-paced loop needed >= 5_000 calls just to drain A's
    /// backlog inside the window (measured RED pre-fix). Progress on A
    /// proves pacing throttles the CALL RATE, not consumption.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hot_shard_does_not_hot_poll_caught_up_shard() {
        let (metadata, historical, _dir) = setup().await;
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep: Arc<dyn DeepStorage> =
            Arc::new(LocalDeepStorage::new(ds_dir.path().to_path_buf()));

        let mock = MockKinesisSource::new("st");
        mock.add_shard("shard-hot");
        mock.add_shard("shard-empty");
        mock.set_records_per_call(1);
        for i in 0..5_000 {
            mock.push_json("shard-hot", "pk", &row(i, "a"));
        }

        let mut handle = start_kinesis_consumer_with_params(
            Box::new(mock.clone()),
            loop_params("kin-pace", "st", "kin_pace"),
            Arc::clone(&metadata),
            Arc::clone(&historical),
            Some(deep),
        );
        // Fixed wall-clock window with shard A continuously hot.
        tokio::time::sleep(Duration::from_millis(500)).await;
        handle.shutdown().await.expect("clean stop");

        // (b) B is NOT hot-polled: the total is hard-bounded by the
        // per-shard pacing floor (2 shards, one caught up), nowhere near
        // the >= 5_000 calls the un-paced hot spin burned.
        let calls = mock.get_records_calls();
        assert!(
            calls < 2_000,
            "GetRecords must be paced per shard, not hot-polled: {calls} calls \
             in a ~500ms window"
        );
        // (a) the hot shard made progress and its rows are
        // query-visible (mid-run flushes + the final drain published).
        let count = queried_row_count(&historical, "kin_pace");
        assert!(
            count >= 5,
            "the hot shard must keep consuming under pacing, got {count} rows"
        );
    }

    /// A `KinesisSource` whose `GetRecords` NEVER completes — models a
    /// source/proxy that accepts the call but never responds. The startup
    /// RPCs return promptly so the loop reaches the hanging `get_records`.
    struct HangingGetRecords;

    #[async_trait::async_trait]
    impl KinesisSource for HangingGetRecords {
        async fn describe_stream(
            &self,
            _stream: &str,
        ) -> std::result::Result<StreamIdentity, KinesisSourceError> {
            Ok(StreamIdentity {
                stream_arn: "arn:hang".to_owned(),
                stream_creation_timestamp_millis: 1,
            })
        }
        async fn list_shards(
            &self,
            _stream: &str,
        ) -> std::result::Result<Vec<ShardId>, KinesisSourceError> {
            Ok(vec![ShardId::new("shard-0")])
        }
        async fn get_shard_iterator(
            &self,
            _stream: &str,
            _shard: &ShardId,
            _position: &StartPosition,
        ) -> std::result::Result<ShardIterator, KinesisSourceError> {
            Ok(ShardIterator::new("iter-0"))
        }
        async fn get_records(
            &self,
            _iterator: &ShardIterator,
        ) -> std::result::Result<ferrodruid_ingest_kinesis::GetRecordsOutput, KinesisSourceError>
        {
            // Never resolves: the loop's KINESIS_RPC_TIMEOUT must abandon it.
            std::future::pending().await
        }
    }

    /// Codex R5 H1: a hung `GetRecords` must not block the poll loop or its
    /// shutdown. `handle.shutdown()` must COMPLETE (the per-call RPC timeout
    /// abandons the stuck call) rather than hang forever.
    #[tokio::test]
    async fn hung_get_records_does_not_block_shutdown() {
        let (metadata, historical, _dir) = setup().await;
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep: Arc<dyn DeepStorage> =
            Arc::new(LocalDeepStorage::new(ds_dir.path().to_path_buf()));

        let mut handle = start_kinesis_consumer_with_params(
            Box::new(HangingGetRecords),
            loop_params("kin-hang", "st", "kin_hang"),
            Arc::clone(&metadata),
            Arc::clone(&historical),
            Some(deep),
        );
        // Let the loop start and reach the hanging get_records.
        tokio::time::sleep(Duration::from_millis(200)).await;
        // The key assertion: shutdown completes despite the hung RPC. Bound
        // it well above KINESIS_RPC_TIMEOUT (50ms in tests) but far below
        // "forever"; a pre-fix loop (no RPC timeout) never returns here.
        let stopped = tokio::time::timeout(Duration::from_secs(5), handle.shutdown()).await;
        assert!(
            stopped.is_ok(),
            "a hung GetRecords must not block the consumer's shutdown"
        );
    }

    /// The spec-driven entry point (`start_kinesis_consumer`) maps a
    /// real Druid-shaped Kinesis spec onto the loop: schema/dimensions
    /// from `dataSchema`, TRIM_HORIZON from `useEarliestSequenceNumber`,
    /// and the parse-only options warn instead of failing. The default
    /// 10s flush never fires — the rows become queryable via the FINAL
    /// drain on shutdown, proving the drain path publishes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spec_driven_consumer_ingests_and_final_drain_publishes() {
        let (metadata, historical, _dir) = setup().await;
        let ds_dir = tempfile::tempdir().expect("deep-storage dir");
        let deep: Arc<dyn DeepStorage> =
            Arc::new(LocalDeepStorage::new(ds_dir.path().to_path_buf()));
        let spec: KinesisSupervisorSpec = serde_json::from_value(json!({
            "type": "kinesis",
            "dataSchema": {
                "dataSource": "kin_spec_ds",
                "timestampSpec": {"column": "__time", "format": "millis"},
                "dimensionsSpec": {"dimensions": [
                    "page", {"name": "n", "type": "long"}
                ]}
            },
            "ioConfig": {
                "stream": "st",
                "useEarliestSequenceNumber": true,
                "taskCount": 3,
                "awsAssumedRoleArn": "arn:aws:iam::1:role/unsupported"
            }
        }))
        .expect("spec");

        let mock = MockKinesisSource::new("st");
        for i in 0..5 {
            mock.push_json("s1", "pk", &json!({"__time": T0 + i, "page": "p", "n": i}));
        }
        let mut handle = start_kinesis_consumer(
            "kin-spec",
            Box::new(mock.clone()),
            &spec,
            Arc::clone(&metadata),
            Arc::clone(&historical),
            Some(deep),
        );
        // Wait until the loop has certainly consumed the seeded page
        // (call 1 reads all 5; call 2 proves the loop advanced past it),
        // then stop: the FINAL drain must publish the buffered rows.
        for _ in 0..400 {
            if mock.get_records_calls() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        handle.shutdown().await.expect("clean stop");
        assert_eq!(queried_row_count(&historical, "kin_spec_ds"), 5);
    }

    // -- spec → buffer-config mapping ---------------------------------------

    #[test]
    fn buffer_config_maps_spec_schema() {
        let spec: KinesisSupervisorSpec = serde_json::from_value(json!({
            "type": "kinesis",
            "dataSchema": {
                "dataSource": "clicks",
                "timestampSpec": {"column": "event_time", "format": "iso"},
                "dimensionsSpec": {"dimensions": [
                    "user", {"name": "dur", "type": "long"}
                ]},
                "metricsSpec": [{"type": "count", "name": "count"}]
            },
            "ioConfig": {"stream": "clicks-stream", "useEarliestSequenceNumber": true},
            "tuningConfig": {"maxRowsPerSegment": 1234}
        }))
        .expect("spec");
        let cfg = kinesis_buffer_config("sup-1", &spec);
        assert_eq!(cfg.data_source, "clicks");
        assert_eq!(cfg.topic, "clicks-stream");
        assert_eq!(cfg.timestamp_column, "event_time");
        assert_eq!(cfg.timestamp_format, TsFormat::Iso);
        assert_eq!(cfg.dim_schemas.len(), 2);
        assert_eq!(cfg.dim_schemas[1].dim_type, DimensionType::Long);
        assert_eq!(cfg.metrics_specs.len(), 1);
        assert_eq!(cfg.max_rows_per_segment, 1234);
        assert!(cfg.use_earliest_offset);
        assert_eq!(spec.io_config.start_position(), StartPosition::TrimHorizon);
    }

    // -- handle drain discipline --------------------------------------------

    /// A consumer that stopped with lost rows (failed final drain) makes
    /// `shutdown()` return `Err` — STICKY across retries — and marks the
    /// handle replay-required; only a SAME-pair Kinesis spec counts as a
    /// recovery (its consumer resumes from the durable frontier, which
    /// never advanced past the lost rows).
    #[tokio::test]
    async fn lossy_stop_is_sticky_and_recoverable_by_same_pair_only() {
        let (tx, _rx) = mpsc::channel::<()>(1);
        let mut handle = KinesisSupervisorHandle {
            shutdown_tx: tx,
            handle: Some(tokio::spawn(async {
                StreamingStats {
                    total_consumed: 5,
                    total_published: 0,
                    final_flush_failed: true,
                    mid_stream_flush_failed: false,
                    fatal_consume_error: false,
                    cluster_id_drifted: false,
                }
            })),
            replay_error: None,
            data_source: "events".to_string(),
            stream: "st".to_string(),
        };
        assert!(!handle.replay_required());
        let err = handle.shutdown().await.expect_err("lossy stop refuses");
        assert!(
            format!("{err}").contains("NOT recorded as stopped"),
            "{err}"
        );
        assert!(handle.replay_required());
        assert!(handle.is_finished());
        // Sticky: a retried drain re-reports the same obligation.
        assert!(handle.shutdown().await.is_err());

        let spec = |ds: &str, stream: &str| -> KinesisSupervisorSpec {
            serde_json::from_value(json!({
                "type": "kinesis",
                "dataSchema": {
                    "dataSource": ds,
                    "timestampSpec": {"column": "ts"},
                    "dimensionsSpec": {"dimensions": ["a"]}
                },
                "ioConfig": {"stream": stream}
            }))
            .expect("spec")
        };
        assert!(handle.recoverable_by(&spec("events", "st")));
        assert!(!handle.recoverable_by(&spec("events", "other")));
        assert!(!handle.recoverable_by(&spec("other", "st")));
    }

    /// A trim-horizon fallback happens ONLY for a service-rejected
    /// resume position; and the resume floor logic feeds the loop the
    /// right positions (unit check of the helper against the mock).
    #[tokio::test]
    async fn trimmed_resume_position_falls_back_to_trim_horizon() {
        let mock = MockKinesisSource::new("st");
        mock.push_record("s1", "pk", "100", br#"{}"#.to_vec(), None);
        // The mock rejects an iterator for an UNKNOWN shard with an Api
        // error; a resume position on it therefore exercises the
        // fallback path returning the (still failing) TrimHorizon
        // attempt — while a valid shard succeeds directly.
        let ok = get_iterator_with_trim_fallback(
            &mock,
            "st",
            &ShardId::new("s1"),
            &StartPosition::AfterSequenceNumber(seq("50")),
        )
        .await;
        assert!(ok.is_ok());
        let err = get_iterator_with_trim_fallback(
            &mock,
            "st",
            &ShardId::new("missing"),
            &StartPosition::AtSequenceNumber(seq("50")),
        )
        .await;
        assert!(
            err.is_err(),
            "TrimHorizon retry on a missing shard still fails"
        );
    }
}
