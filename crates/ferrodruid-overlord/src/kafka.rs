// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Kafka supervisor runtime wiring.
//!
//! `create_supervisor` persists a supervisor spec; this module turns a
//! *Kafka* supervisor spec into a running streaming consumer that polls the
//! topic and publishes rolled segments into the shared metadata store +
//! [`Historical`] — so a Kafka-fed datasource becomes queryable exactly
//! like a batch-ingested one.
//!
//! Published segments go through [`publish_streaming_segment`], which reuses
//! the same collision-safe id allocation ([`allocate_segment_id_inner`]),
//! atomic metadata transaction ([`MetadataStore::replace_segments_txn`]),
//! and query-visible swap ([`Historical::replace_segments`]) as batch
//! `index_parallel` — minus the replace/victim planning, because streaming
//! only ever appends.
//!
//! **Durability model (compat-3, 2026-07-16):** streaming ingestion is
//! durable in a `kafka-io` build. Published segments persist to deep
//! storage (fsync + SHA-256 content hash) and reload at bootstrap, and the
//! consumer commits Kafka offsets durably so a restart RESUMES from the
//! committed position rather than replaying from `auto.offset.reset`. A
//! restart therefore neither replays already-ingested data nor loses it:
//! segments reload from deep storage and consumption resumes at the
//! committed offset. Persisted, non-suspended supervisors are auto-resumed
//! on startup
//! ([`Overlord::resume_kafka_supervisors`](crate::Overlord::resume_kafka_supervisors)).
//! Residual limitations (unresolved cluster identity, multi-broker
//! topic-id windows, topic delete→recreate) are catalogued as FG-6/FG-10
//! in the published limitations. This is a doc note only; the runtime
//! logic below is unchanged.

use std::sync::Arc;

use ferrodruid_common::{DruidError, Result};
use ferrodruid_deep_storage::DeepStorage;
use ferrodruid_historical::{Historical, SegmentSwapEntry};
use ferrodruid_ingest_batch::IngestedSegment;
use ferrodruid_ingest_kafka::KafkaSupervisorSpec;
use ferrodruid_ingest_kafka::eos_writer::OffsetSpan;
use ferrodruid_ingest_kafka::runtime::KafkaSupervisorRuntime;
use ferrodruid_ingest_kafka::streaming::{
    KafkaStreamConsumer, PartitionOffsets, PartitionResume, ResumeFrontier, SegmentSink,
    SegmentSinkError, StreamingStats, TopicRecords, create_stream_consumer, run_streaming_loop,
    schema_fingerprint, schema_fp_is_current_version,
};
use ferrodruid_ingest_kafka::topic_id::TopicIdProbe;
use ferrodruid_metadata::{MetadataStore, SegmentMetadataRow};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::persist::persist_segment;
use crate::{allocate_segment_id_inner, format_epoch_millis_iso};

/// Handle to a running Kafka supervisor consumer task, kept by the
/// [`Overlord`](crate::Overlord) so `shutdown_supervisor` can stop it.
///
/// Carries the consumer's `(data_source, topic)` pair so create/resume can
/// enforce the ONE-consumer-per-pair invariant across DIFFERENT spec_ids
/// (legacy synthetic `supervisor_N` rows vs the datasource-derived id) —
/// otherwise every record is silently ingested once per consumer (Fable
/// audit). The same pair invariant is ALSO enforced at the PERSISTED layer
/// at create time in EVERY build (Codex R25,
/// `Overlord::refuse_persisted_kafka_pair_conflict`): live handles only
/// witness consumers registered in this process, so a default (no-kafka-io)
/// build — and a kafka-io build right after restart, before resume — would
/// otherwise accept a second id whose row the next resume then warn-skips
/// (a legitimately created supervisor silently ingesting nothing).
///
/// String equality on `topic` is a sound uniqueness key ONLY because
/// `KafkaSupervisorSpec::validate` rejects `^`-prefixed topics (Codex R28):
/// librdkafka regex-subscribes such names, so an unvalidated `^orders-.*`
/// beside a literal `orders-prod` would pass this guard yet double-ingest.
pub(crate) struct KafkaSupervisorHandle {
    shutdown_tx: mpsc::Sender<()>,
    /// The consumer task. Taken (→ `None`) the first time a drain joins
    /// it; from then on the drain OUTCOME lives in
    /// [`replay_error`](Self::replay_error) — a tokio [`JoinHandle`] must
    /// not be polled again after completion.
    handle: Option<JoinHandle<StreamingStats>>,
    /// Sticky replay obligation (Codex R30 F3): `Some(reason)` once a
    /// completed drain reported lost rows (failed mid-stream/final flush,
    /// fatal consume stop, or a panicked task). Pre-R30 the lifecycle ops
    /// REMOVED the handle from the registry before observing the drain
    /// failure, so a retried shutdown/suspend found no handle, skipped the
    /// drain check entirely, and persisted the tombstone — permanently
    /// foreclosing the very replay the first refusal protected. The failed
    /// handle now STAYS registered as a sentinel carrying this reason:
    /// every retried shutdown/suspend re-reports it (and keeps refusing
    /// the tombstone) until a replay path — restart resume or an earliest
    /// re-create — supersedes the handle.
    replay_error: Option<String>,
    pub(crate) data_source: String,
    pub(crate) topic: String,
    /// Ingestion-schema fingerprint ([`schema_fingerprint`], Codex R26 F1)
    /// of the spec this consumer was started from. Retained so a re-create
    /// that wants to SUPERSEDE a replay-required sentinel (Codex R33) can be
    /// verified to carry the SAME schema — only then can its earliest replay
    /// rebuild exactly the rows the failed drain lost (see
    /// [`recoverable_by`](Self::recoverable_by)).
    schema_fp: String,
    /// `bootstrap.servers` of the source cluster this consumer was started
    /// from. Also part of [`recoverable_by`](Self::recoverable_by) (Codex
    /// R33): a repost that RE-POINTS the pair at a DIFFERENT broker set
    /// cannot replay the ORIGINAL cluster's lost rows, so it must not
    /// supersede the sentinel. This is a bootstrap-STRING check (the strongest
    /// signal available synchronously, before a lifecycle op resolves the
    /// broker-side cluster id); a same-string DNS repoint remains the
    /// documented ambiguity the cleanup's cluster-id match guards downstream.
    brokers: String,
}

impl KafkaSupervisorHandle {
    /// Signal the consumer to flush its residual buffer and exit, then wait
    /// for it to drain, RETURNING the stop outcome. A send error is ignored
    /// (a closed channel means the consumer is already on its way out), but
    /// the stats are not: `Err` when the stats report that ANY buffered
    /// rows were dropped without becoming queryable — a failed FINAL drain
    /// (Codex R26 F2) and/or a failed MID-STREAM publish earlier in the
    /// consumer's life (Codex R27 F2; sticky, so a later clean — typically
    /// empty — final drain cannot launder it) — or that the consumer
    /// stopped itself on a FATAL consume error without delivering the
    /// topic's records (Codex R30 F2). Either way the caller must NOT
    /// record the supervisor as cleanly stopped (tombstone/suspended),
    /// because a stopped-and-tombstoned supervisor is never replayed and
    /// the rows would be silently lost. Keeping the metadata ACTIVE instead
    /// lets a restart/resume (or an earliest re-create) replay the topic
    /// and rebuild them — the FG-6 Kafka-is-the-durable-log recovery model.
    /// A consumer task that panicked/was aborted is fail-closed `Err` too:
    /// its buffer state is unknown.
    ///
    /// `&mut self`, not `self` (Codex R30 F3): the outcome is CACHED on the
    /// handle, so a caller that keeps a failed handle registered gets the
    /// same `Err` from every retry — the drain obligation cannot be
    /// forgotten by consuming the handle.
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
                            "STOPPED on a fatal consume error (authorization / \
                             librdkafka-fatal / payload-integrity) without delivering \
                             the topic's records",
                        );
                    }
                    if stats.cluster_id_drifted {
                        causes.push(
                            "DETECTED a Kafka cluster-identity DRIFT (a broker hostname \
                             was repointed to a different cluster mid-session) and refused \
                             to publish the possibly-mis-attributed buffered rows",
                        );
                    }
                    self.replay_error = Some(format!(
                        "the Kafka consumer for datasource '{}' / topic '{}' {}: the \
                         affected rows never became queryable (consumed {}, \
                         published {} segments). The supervisor was NOT recorded as \
                         stopped — its metadata stays active so a restart/resume (or an \
                         earliest re-create of the same pair) replays the topic and \
                         rebuilds the rows",
                        self.data_source,
                        self.topic,
                        causes.join(" AND "),
                        stats.total_consumed,
                        stats.total_published,
                    ));
                }
                Err(e) => {
                    self.replay_error = Some(format!(
                        "the Kafka consumer task for datasource '{}' / topic '{}' did not run \
                         to completion ({e}); its residual buffer state is unknown, so the \
                         final drain is treated as FAILED and the supervisor was NOT recorded \
                         as stopped (a restart/resume replays the topic)",
                        self.data_source, self.topic,
                    ));
                }
            }
        }
        match &self.replay_error {
            None => Ok(()),
            Some(reason) => Err(DruidError::Ingestion(reason.clone())),
        }
    }

    /// Whether the consumer task has already exited (e.g. it hit a fatal
    /// error) — or was already joined by a drain. A finished handle is
    /// stale and must not block a repost, count as a live pair, or dedup a
    /// resume; a REPLAY-REQUIRED finished handle (see
    /// [`replay_error`](Self::replay_error)) is still finished by this
    /// definition — only the shutdown/suspend transitions consult the
    /// obligation, via [`shutdown`](Self::shutdown).
    pub(crate) fn is_finished(&self) -> bool {
        self.handle.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Whether this handle is a REPLAY-REQUIRED sentinel (Codex R30 F3): a
    /// completed drain reported lost buffered rows and cached the reason in
    /// [`replay_error`](Self::replay_error), while the supervisor's metadata
    /// was deliberately LEFT ACTIVE (no tombstone) so a topic replay can
    /// still rebuild them. Such a finished handle must NOT be silently
    /// reaped/replaced by a create that cannot replay those rows (Codex
    /// R33) — the create path consults this before reaping a finished
    /// handle, and only [`recoverable_by`](Self::recoverable_by) reposts may
    /// supersede it.
    pub(crate) fn replay_required(&self) -> bool {
        self.replay_error.is_some()
    }

    /// Whether `parsed` (an already-validated incoming Kafka spec) would
    /// RECOVER this replay-required sentinel by replaying the lost rows: it
    /// must target the SAME `(data_source, topic)` pair, the SAME source
    /// cluster (`bootstrap.servers`), start from the EARLIEST offset, and
    /// carry the SAME ingestion-schema fingerprint ([`schema_fingerprint`])
    /// — the exact conditions under which the earliest-replay cleanup
    /// ([`earliest_replay_cleanup`]) then drops the pair's prior rows and the
    /// replay rebuilds them (including the ones the failed drain lost). A
    /// non-Kafka spec, a DIFFERENT pair, a re-point at DIFFERENT brokers, a
    /// LATEST (tail) re-create, or a schema change cannot rebuild the lost
    /// rows, so it must be refused rather than allowed to supersede the
    /// sentinel (Codex R33). `parsed` is already validated, so its
    /// `bootstrap.servers` is present and non-empty (Codex R7).
    pub(crate) fn recoverable_by(&self, parsed: &KafkaSupervisorSpec) -> bool {
        parsed.data_schema.data_source == self.data_source
            && parsed.io_config.topic == self.topic
            && parsed.io_config.use_earliest_offset.unwrap_or(false)
            && schema_fingerprint(parsed) == self.schema_fp
            && parsed
                .io_config
                .consumer_properties
                .get("bootstrap.servers")
                .map(String::as_str)
                == Some(self.brokers.as_str())
    }
}

/// [`SegmentSink`] that appends each rolled Kafka segment into the shared
/// metadata + [`Historical`].
struct OverlordKafkaSink {
    metadata: Arc<MetadataStore>,
    historical: Arc<Historical>,
    /// Deep-storage backend (compat-3 stage 1): when configured, each rolled
    /// streaming segment is PERSISTED before its metadata row is committed
    /// (persist → metadata → swap) so a restart's bootstrap reload rebuilds
    /// it from durable storage rather than relying solely on a Kafka replay.
    /// `None` keeps the pre-persistence memory-resident behavior.
    deep_storage: Option<Arc<dyn DeepStorage>>,
    task_id: String,
    /// Source topic, stamped into each published segment's provenance so the
    /// respawn cleanup can identify EXACTLY the rows a topic replay rebuilds
    /// (Codex R19: an untyped `taskId` match could delete unreplayable
    /// foreign data or miss a prior supervisor id's rows).
    topic: String,
    /// `bootstrap.servers` of the source CLUSTER, stamped alongside `topic`
    /// (Codex R21): topic names are only unique WITHIN a cluster, so a
    /// cleanup keyed on (kind, topic) alone would drop another cluster's
    /// same-named topic's rows — which that consumer's replay can never
    /// rebuild (permanent loss).
    bootstrap: String,
    /// The source cluster's Kafka **cluster id**, stamped when it could be
    /// resolved at consumer start (Codex R24 F2). Since Codex R26 F3 this
    /// is the ONLY identity the respawn cleanup matches on (see
    /// [`drop_streaming_segments_task`]'s match rules); `bootstrap` stays
    /// stamped for diagnostics only.
    ///
    /// Stamping the START-time value for the consumer's whole life is
    /// sound ONLY because the consumer pins
    /// `metadata.recovery.strategy=none` (Codex R30 F1, see
    /// `build_client_config` in the streaming module): librdkafka 2.12.1
    /// otherwise re-bootstraps by default when all known brokers vanish,
    /// and a bootstrap DNS name repointed to a DIFFERENT cluster would
    /// silently migrate the live session while rows kept being stamped
    /// with the old id — a later earliest re-create would then drop/keep
    /// exactly the wrong rows. With rebootstrap off, one session ⇔ one
    /// cluster identity, so stamping (here) and cleanup matching (the id
    /// resolved by the same session at create/resume) share one source.
    cluster_id: Option<String>,
    /// The source topic's KIP-516 **topic id** (topic UUID), stamped when it
    /// could be resolved at consumer start ([`resolve_topic_id`], Codex R7
    /// H1). A topic deleted+recreated under the same name REUSES the offset
    /// space, so `kafkaOffsets` are only meaningful within ONE topic
    /// GENERATION — the resume frontier treats a definite topic-id MISMATCH
    /// as a detected recreation (the dead generation's rows are excluded and
    /// a partition with no live evidence re-consumes the retained log from
    /// low), while an unresolved/unstamped id leaves the cluster-identity
    /// gating in charge (Codex R8 H1/H2 tri-state — the id is an
    /// enhancement, never a downgrade trigger). Start-time stamping is sound
    /// for the same R30 F1 reason as `cluster_id`; the honest residual — a
    /// MID-SESSION recreation stamps the old generation's id on
    /// new-generation rows until restart — only ever costs bounded
    /// duplication on the next resume (the stale-id rows read as a dead
    /// generation and their span is re-consumed from the retained log),
    /// never loss.
    topic_id: Option<String>,
    /// Fingerprint of the supervisor's ingestion schema
    /// ([`schema_fingerprint`], Codex R26 F1), stamped so the respawn
    /// cleanup can refuse to drop rows a re-created supervisor's replay
    /// would rebuild under a DIFFERENT schema (= unrebuildable).
    schema_fp: String,
}

impl SegmentSink for OverlordKafkaSink {
    async fn publish(&self, segment: IngestedSegment) -> std::result::Result<(), SegmentSinkError> {
        self.publish_with_offsets(segment, &PartitionOffsets::new())
            .await
    }

    async fn publish_with_offsets(
        &self,
        segment: IngestedSegment,
        offsets: &PartitionOffsets,
    ) -> std::result::Result<(), SegmentSinkError> {
        // A `RetainedDurable` failure (swap failed but the durable row survived
        // a failed rollback) maps to `Ok(())` so the loop commits the offset and
        // never re-consumes/re-publishes the segment (H2 dup guard); a genuine
        // failure maps to `Err` so the loop re-consumes (at-least-once).
        sink_result_from_publish(
            publish_streaming_segment_persisted(
                &self.metadata,
                &self.historical,
                self.deep_storage.as_deref(),
                &StreamingProvenance {
                    task_id: &self.task_id,
                    topic: &self.topic,
                    bootstrap: &self.bootstrap,
                    cluster_id: self.cluster_id.as_deref(),
                    topic_id: self.topic_id.as_deref(),
                    schema_fp: &self.schema_fp,
                },
                offsets,
                segment,
            )
            .await,
        )
    }

    fn commits_offsets(&self) -> bool {
        // Offsets are only committed when this sink actually PERSISTS segments
        // to deep storage (compat-3 durability, C3): with no backend the
        // published segment is memory-only, so committing its offset would let
        // a restart skip records whose only copy vanished with the process
        // (loss). A configured backend + an `Ok` publish means the blob is
        // durable (a failed persist aborts the publish with `Err`).
        self.deep_storage.is_some()
    }
}

/// Provenance stamped into every published streaming segment's payload —
/// everything the respawn cleanup ([`drop_streaming_segments_task`]) needs
/// to decide whether a later replay REBUILDS the row (and everything an
/// operator needs to trace where it came from).
struct StreamingProvenance<'a> {
    /// Publishing supervisor id (`taskId` in the payload; diagnostics —
    /// the cleanup matches by provenance, never by task id, Codex R19).
    task_id: &'a str,
    /// Source topic (`topic`): names the replay source.
    topic: &'a str,
    /// `bootstrap.servers` of the source cluster (`bootstrap`, normalized).
    /// Diagnostics only since Codex R26 F3: bootstrap equality is neither
    /// necessary nor sufficient for cluster identity, so the cleanup no
    /// longer consults it.
    bootstrap: &'a str,
    /// Broker-side Kafka cluster id (`clusterId`) when resolvable at
    /// consumer start — the ONLY identity the cleanup matches on.
    cluster_id: Option<&'a str>,
    /// KIP-516 topic id (`topicId`) of the source topic when resolvable at
    /// consumer start (Codex R7 H1) — the identity that survives a topic
    /// delete+recreate (which reuses topic NAME and offset space). The
    /// resume frontier requires a definite match before skipping past this
    /// row's `kafkaOffsets`, and reads a definite MISMATCH as a detected
    /// recreation (re-consume the retained log from low).
    topic_id: Option<&'a str>,
    /// Ingestion-schema fingerprint (`schemaFp`, [`schema_fingerprint`]) —
    /// lets the cleanup refuse a same-pair re-create whose changed schema
    /// could not rebuild these rows from a replay (Codex R26 F1).
    schema_fp: &'a str,
}

/// Normalize a `bootstrap.servers` string for provenance stamping: split
/// on commas, strip ALL whitespace (spaces around commas are cosmetic to
/// librdkafka), drop empty entries, SORT, dedup, and re-join. Sorting
/// absorbs reordering (Codex R24 F2 — `a:9092,b:9092` and `b:9092,a:9092`
/// are one cluster: the list is an unordered seed set), so stamped values
/// stay comparable for an OPERATOR inspecting provenance. Since Codex R26
/// F3 the stamped bootstrap is diagnostics only — the respawn cleanup
/// ([`drop_streaming_segments_task`]) matches exclusively on the Kafka
/// cluster id, never on bootstrap equality (a DNS repoint can make one
/// string name two clusters over time). Idempotent.
fn normalize_bootstrap(bootstrap: &str) -> String {
    let mut hosts: Vec<String> = bootstrap
        .split(',')
        .map(|h| h.chars().filter(|c| !c.is_whitespace()).collect::<String>())
        .filter(|h| !h.is_empty())
        .collect();
    hosts.sort_unstable();
    hosts.dedup();
    hosts.join(",")
}

/// Failure outcome of [`publish_streaming_segment_persisted`].
///
/// The happy path returns `Ok(segment_id)` — the segment is durable (when a
/// backend is configured), its metadata row is committed, AND it is
/// query-visible in THIS session. Everything else is an error, split into two
/// classes the streaming sink must treat DIFFERENTLY (Codex R14 H2):
///
/// * [`Failed`](Self::Failed) — nothing durable survives (persist failed, the
///   metadata transaction failed, or the swap failed AND the rollback removed
///   the row). No offset is durable, so the sink surfaces the failure and the
///   streaming loop re-consumes the batch (at-least-once, no loss).
/// * [`RetainedDurable`](Self::RetainedDurable) — the swap failed AND the
///   metadata rollback ALSO failed, so the durable metadata row + deep-storage
///   blob are RETAINED and reloaded on the next restart. The offset is
///   therefore EFFECTIVELY durable; the sink reports success so the loop
///   COMMITS it and does NOT re-consume/re-publish the segment (which would
///   double-count once the restart reload re-materialises the retained row).
#[derive(Debug)]
enum StreamingPublishError {
    /// A genuine publish failure with nothing durable left behind — re-consume.
    Failed(DruidError),
    /// Swap failed and rollback failed: the durable row + blob survive and are
    /// reloaded on restart, so the offset is durable (do NOT re-consume). Not
    /// query-visible this session. Carries the retained segment id (diagnostics).
    RetainedDurable(String),
}

impl From<DruidError> for StreamingPublishError {
    fn from(e: DruidError) -> Self {
        Self::Failed(e)
    }
}

/// Disposition of a streaming publish whose query-visible swap FAILED, decided
/// PURELY from whether the metadata rollback removed the just-committed row
/// (`rolled_back`) and whether the segment is DURABLE (a deep-storage blob
/// backs it, `durable`).
///
/// Split out so the H2 dup-guard decision is unit-testable without injecting a
/// rollback failure: with real SQLite a streaming rollback (`restore = &[]`,
/// just a `DELETE`) always succeeds, so the `RetainDurable` branch — reachable
/// only when the metadata store breaks BETWEEN the insert and the rollback —
/// cannot be driven end-to-end from a test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SwapFailureDisposition {
    /// The row was rolled back (gone) OR the segment was never durable: the
    /// offset is NOT durable, so the streaming loop must re-consume the batch.
    ReSeek,
    /// The row survived a FAILED rollback AND is durable (blob-backed): it is
    /// reloaded on restart, so the offset is durable and must be committed
    /// rather than re-consumed (H2). Not query-visible this session.
    RetainDurable,
}

/// Decide how a swap failure is surfaced (H2): only a durable row that
/// survived a FAILED rollback is treated as durably committed; every other
/// case re-consumes (at-least-once).
fn classify_swap_failure(rolled_back: bool, durable: bool) -> SwapFailureDisposition {
    if !rolled_back && durable {
        SwapFailureDisposition::RetainDurable
    } else {
        SwapFailureDisposition::ReSeek
    }
}

/// Map a [`publish_streaming_segment_persisted`] result to the [`SegmentSink`]
/// result the streaming loop consumes (Codex R14 H2).
///
/// A live publish and a [`RetainedDurable`](StreamingPublishError::RetainedDurable)
/// failure BOTH report `Ok(())`, so the loop commits the covered offset and
/// advances its contiguous frontier — never re-seeking to re-consume and
/// re-publish a segment whose durable row will simply reload on restart (that
/// would permanently double-count). Only a genuine
/// [`Failed`](StreamingPublishError::Failed) becomes a [`SegmentSinkError`], so
/// the loop re-consumes the dropped batch (at-least-once, no loss).
fn sink_result_from_publish(
    outcome: std::result::Result<String, StreamingPublishError>,
) -> std::result::Result<(), SegmentSinkError> {
    match outcome {
        Ok(_id) | Err(StreamingPublishError::RetainedDurable(_id)) => Ok(()),
        Err(StreamingPublishError::Failed(e)) => Err(SegmentSinkError(e.to_string())),
    }
}

/// Publish one rolled streaming segment as an APPEND (no victims): PERSIST
/// it to deep storage (compat-3 stage 1) when a backend is configured,
/// allocate a collision-free id under the datasource publish lock, commit one
/// atomic metadata transaction, then apply the query-visible swap. On success
/// returns the allocated segment id; on failure returns a
/// [`StreamingPublishError`] whose variant tells the sink whether the offset is
/// durable ([`RetainedDurable`](StreamingPublishError::RetainedDurable), H2) or
/// must be re-consumed ([`Failed`](StreamingPublishError::Failed)).
///
/// The sequence is **P (persist) → M (metadata) → swap**: the segment is
/// uploaded to deep storage BEFORE any metadata row is committed, so a
/// restart's bootstrap reload always has a durable blob to re-download. An
/// upload failure aborts BEFORE the metadata transaction, riding the existing
/// publish-failure path (which the streaming loop records as a lost-rows
/// mid-stream/final-drain failure). Streaming has no victims (append-only),
/// so — unlike the batch tail — there is no replace planning; there is also
/// no offset key in the payload (offset commit is stage-2 work).
///
/// On swap failure the just-committed metadata row is rolled back under the
/// still-held publish lock AND the orphan deep-storage blob is best-effort
/// deleted, so neither metadata nor storage keeps a dangling reference — the
/// same failure discipline as `execute_index_parallel`.
///
/// `deep_storage: None` keeps the pre-persistence memory-resident behavior
/// (no upload, no `loadSpec`); it is the path unit tests and any
/// Overlord-without-a-backend take.
async fn publish_streaming_segment_persisted(
    metadata: &Arc<MetadataStore>,
    historical: &Arc<Historical>,
    deep_storage: Option<&dyn DeepStorage>,
    prov: &StreamingProvenance<'_>,
    offsets: &PartitionOffsets,
    ingested: IngestedSegment,
) -> std::result::Result<String, StreamingPublishError> {
    let task_id = prov.task_id;
    let ds_name = ingested.data_source.clone();
    let start_iso = format_epoch_millis_iso(ingested.interval.start_millis);
    // Half-open end one millisecond past the max so the segment fully
    // covers the data range (matches the batch path).
    let end_iso = format_epoch_millis_iso(ingested.interval.end_millis.saturating_add(1));
    let version = ingested.version.clone();
    let num_rows = ingested.num_rows;

    // Serialize the whole publication per datasource on the shared publish
    // lock (id alloc must be race-free with the subsequent insert).
    let publish_lock = metadata.datasource_publish_lock(&ds_name).await;
    let _guard = publish_lock.lock().await;

    let segment_id = allocate_segment_id_inner(
        metadata, historical, &ds_name, &start_iso, &end_iso, &version,
    )
    .await?;

    // Phase P (persist) — upload BEFORE any metadata is committed. An upload
    // failure aborts via `?` before the metadata transaction, so the
    // crash-consistency invariant (a metadata row only ever follows a
    // durable upload) holds. Skipped when no backend is configured.
    let load_spec = match deep_storage {
        Some(ds) => Some(persist_segment(ds, &ds_name, &segment_id, &ingested.segment_data).await?),
        None => None,
    };

    let now_iso = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    // Streaming provenance (Codex R19): `kind` marks the row as
    // rebuilt-by-replay, `topic` names the replay source. The respawn
    // cleanup drops exactly the rows whose provenance names the SAME replay
    // source (see `drop_streaming_segments_task`) — never a batch/foreign
    // row (whose taskId might coincide), and regardless of which supervisor
    // id originally published it. Cluster identity (Codex R21 → R24 F2 →
    // R26 F3): topic names are only unique within a cluster, so the payload
    // carries the broker-side `clusterId` when it was resolvable at
    // consumer start — since R26 F3 the ONLY identity the cleanup matches
    // on — plus the normalized `bootstrap` (diagnostics only). `schemaFp`
    // (R26 F1) records the ingestion schema the rows were built under, so a
    // later same-pair re-create with a CHANGED schema is refused rather
    // than allowed to drop rows its replay cannot rebuild.
    let mut payload = serde_json::json!({
        "dataSource": ds_name,
        "numRows": num_rows,
        "taskId": task_id,
        "kind": "kafka-streaming",
        "topic": prov.topic,
        "bootstrap": normalize_bootstrap(prov.bootstrap),
        "schemaFp": prov.schema_fp,
    });
    if let Some(cid) = prov.cluster_id {
        payload["clusterId"] = serde_json::Value::String(cid.to_string());
    }
    // KIP-516 topic id (Codex R7 H1): the identity of the topic GENERATION
    // whose offset space `kafkaOffsets` lives in. Stamped whenever it was
    // resolvable at consumer start; the resume frontier requires a definite
    // match to skip past this row and treats a mismatch as a detected
    // recreation.
    if let Some(tid) = prov.topic_id {
        payload["topicId"] = serde_json::Value::String(tid.to_string());
    }
    if let Some(spec) = &load_spec {
        payload["loadSpec"] = spec.to_json();
    }
    // Kafka resume offsets (compat-3 durability): the per-partition `[start,
    // next)` span this segment covers, keyed by partition id. On restart the
    // overlord derives the resume frontier from the durable segment set (see
    // `compute_resume_frontier`) and reconciles it with the committed offset so
    // already-persisted records are not replayed — the durable rows are the
    // self-consistent offset store (no separate file can drift). Stamped ONLY
    // when the segment is actually DURABLE (`load_spec.is_some()`, C3): a
    // memory-only segment (no deep-storage backend) must not seed a resume
    // frontier, or a restart would seek past records whose only copy vanished
    // with the process (loss). Empty on the no-offset path (unit tests).
    //
    // Stamped ONLY alongside a resolved cluster identity (Codex R4 H4): the
    // invariant is "a kafkaOffsets-carrying durable row always names its
    // cluster (`clusterId`)". Offsets are only meaningful within ONE
    // cluster's offset space — after a bootstrap DNS name is repointed from
    // cluster A to cluster B, seeking B's partitions with A's identity-less
    // offsets would skip B's distinct records permanently — so an
    // identity-less row must never look skippable (`compute_resume_frontier`
    // requires a DEFINITE identity match to ADVANCE the resume; since R5 H2
    // an unconfirmed row still FLOORS it, loss prevention only). With an
    // unresolved identity the offsets are OMITTED: the segment stays
    // durable + queryable + reloadable, a restart just resumes from the
    // committed offsets alone (documented degraded mode, warned below —
    // the phantom-row stale-commit protection needs the frontier).
    if load_spec.is_some() && !offsets.is_empty() {
        if prov.cluster_id.is_some() {
            let mut ko = serde_json::Map::new();
            for (partition, span) in offsets {
                ko.insert(
                    partition.to_string(),
                    serde_json::json!({ "start": span.start, "next": span.next }),
                );
            }
            payload["kafkaOffsets"] = serde_json::Value::Object(ko);
            if prov.topic_id.is_none() {
                tracing::warn!(
                    data_source = %ds_name,
                    topic = %prov.topic,
                    segment_id = %segment_id,
                    "durable streaming segment stamped WITHOUT a topicId (the KIP-516 \
                     topic id could not be resolved at consumer start — non-PLAINTEXT \
                     listener, pre-2.8 broker, or an unreachable probe): on resume this \
                     row is gated by the CLUSTER identity alone (Codex R8 H2 — the \
                     topic id only tightens the frontier on a positively detected \
                     recreation), so a recreation spanning this row's lifetime is \
                     undetectable until the id becomes resolvable",
                );
            }
        } else {
            tracing::warn!(
                data_source = %ds_name,
                topic = %prov.topic,
                segment_id = %segment_id,
                "durable streaming segment published WITHOUT kafkaOffsets: the Kafka \
                 cluster identity could not be resolved at consumer start, and \
                 identity-less offsets must never seed a resume frontier (Codex R4 \
                 H4 — after a bootstrap repoint they would seek the NEW cluster past \
                 its own records = permanent loss). A restart resumes from the \
                 committed offsets alone; resolve the broker's cluster id (KIP-78) \
                 to restore frontier-based resume",
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

    // Append: empty victim set. One atomic metadata transaction inserts the
    // new row; a failure (or crash) leaves metadata untouched. If it fails
    // AFTER Phase P uploaded a blob, that blob is now unreferenced — best-effort
    // delete it before surfacing the error so repeated failures do not leak
    // orphan storage (H8).
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

    // Query-visible swap LAST: add the new segment WITH its datasource
    // mapping (via `load_segment_with_datasource` semantics inside
    // `replace_segments`) so default-deny isolation makes it visible to
    // table queries. On failure, one compensating transaction un-publishes.
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
                "kafka streaming segment published",
            );
            Ok(segment_id)
        }
        Err(e) => {
            // Roll the just-committed metadata row back under the still-held
            // publish lock. Track whether the row was ACTUALLY removed: the
            // orphan-blob delete below AND the durable-offset decision (H2) are
            // gated on it.
            let rolled_back = match metadata.rollback_replace_txn(&segment_id, &[]).await {
                Ok(()) => true,
                Err(restore_err) => {
                    tracing::error!(
                        task_id,
                        segment_id = %segment_id,
                        error = %restore_err,
                        "rollback could not un-publish kafka segment metadata after swap failure",
                    );
                    false
                }
            };
            // Delete the orphan blob ONLY when the rollback SUCCEEDED, i.e. the
            // metadata row that referenced it is gone (H5). If the rollback
            // FAILED the row still references the blob — deleting it would
            // create a phantom (metadata pointing at a missing blob), exactly
            // what the bootstrap reload warns-and-skips as data loss.
            crate::cleanup_orphan_blob(
                deep_storage,
                &ds_name,
                &segment_id,
                load_spec.is_some(),
                rolled_back,
            )
            .await;
            // H2 (Codex R14): if the swap failed AND the rollback ALSO failed,
            // the durable metadata row + blob are RETAINED and reloaded on the
            // next restart — so the offset is effectively DURABLE. Reporting a
            // plain failure here would make the streaming loop re-consume and
            // re-publish the segment under a NEW id; on restart BOTH the
            // retained row and the re-published row reload → permanent double
            // count. Signal `RetainedDurable` so the offset is committed and
            // NOT re-consumed. A memory-only row (no blob) is not reloaded, so
            // it stays a genuine failure (re-consume; offsets are never
            // committed for a non-durable sink anyway).
            match classify_swap_failure(rolled_back, load_spec.is_some()) {
                SwapFailureDisposition::RetainDurable => {
                    tracing::warn!(
                        task_id,
                        data_source = %ds_name,
                        segment_id = %segment_id,
                        error = %e,
                        "kafka streaming publish: the query-visible swap FAILED and the \
                         metadata rollback ALSO failed, so the durable segment row + \
                         deep-storage blob are RETAINED and will be reloaded on the next \
                         restart. Treating its Kafka offset as durable-committed so the \
                         streaming loop does NOT re-consume and re-publish it (which would \
                         double-count after the restart reload). Honest limitation: the \
                         segment is NOT query-visible in THIS session — it becomes visible \
                         after the next restart's bootstrap reload (H2)",
                    );
                    Err(StreamingPublishError::RetainedDurable(segment_id))
                }
                SwapFailureDisposition::ReSeek => Err(StreamingPublishError::Failed(e)),
            }
        }
    }
}

/// No-deep-storage convenience over [`publish_streaming_segment_persisted`]
/// used only by unit tests (and conceptually any Overlord built without a
/// deep-storage backend): publishes append-only with no persist and no
/// `loadSpec`. The production consumer path always goes through
/// [`OverlordKafkaSink`], which threads its configured backend into
/// [`publish_streaming_segment_persisted`] directly.
#[cfg(test)]
async fn publish_streaming_segment(
    metadata: &Arc<MetadataStore>,
    historical: &Arc<Historical>,
    prov: &StreamingProvenance<'_>,
    ingested: IngestedSegment,
) -> Result<String> {
    publish_streaming_segment_persisted(
        metadata,
        historical,
        None,
        prov,
        &PartitionOffsets::new(),
        ingested,
    )
    .await
    .map_err(|e| match e {
        StreamingPublishError::Failed(inner) => inner,
        StreamingPublishError::RetainedDurable(id) => DruidError::Ingestion(format!(
            "streaming publish swap failed but durable row '{id}' was retained"
        )),
    })
}

// `run_lifecycle_op` — the uncancellable spawned-lifecycle-op runner
// (Codex R20 → R22 → R23: a cancelled caller must never tear a
// destructive cleanup apart from the consumer start that replays it) —
// moved VERBATIM to the crate root for compat-5, so the Kinesis wiring
// shares the SAME single implementation instead of forking it.
// Re-exported here so `kafka::run_lifecycle_op` call sites and this
// module's tests are unchanged.
pub(crate) use crate::run_lifecycle_op;

/// Drop every segment a topic replay will REBUILD — rows for `data_source`
/// whose payload carries the streaming provenance `kind == "kafka-streaming"
/// && topic == <topic>` AND whose cluster identity matches (see the match
/// rules below) — from BOTH the query-visible [`Historical`] and metadata,
/// atomically, under the datasource publish lock.
///
/// Called before a consumer (re)starts — but ONLY when the spec replays
/// from the EARLIEST offset (Codex R20): segments are memory-resident and
/// offsets are never committed, so an `useEarliestOffset:true` consumer
/// replays the topic from the beginning and rebuilds exactly this
/// provenance set; leaving any of it loaded would store those records TWICE
/// (Fable audit + Codex R19). A `useEarliestOffset:false` (latest — the
/// spec default) consumer starts at the topic TAIL instead: the old records
/// are never redelivered, so the prior segments neither duplicate nor can
/// be rebuilt — the callers must SKIP this drop or they permanently lose
/// that data.
///
/// The provenance match is what makes this safe and complete:
///   * a batch/foreign row (no `kind`) is NEVER touched, even when its
///     `taskId` coincides with a supervisor id (Codex R19: replay cannot
///     rebuild foreign data);
///   * rows published under a PRIOR supervisor id for the same topic ARE
///     dropped (Codex R19: pair uniqueness must outlive the old handle;
///     since Codex R25 the pair is also enforced at CREATE time against
///     the persisted rows, in every build — see
///     `Overlord::refuse_persisted_kafka_pair_conflict`);
///   * rows from a DIFFERENT topic are kept (a topic change does not replay
///     them, so dropping them would lose data);
///   * rows from the same topic NAME on a DIFFERENT CLUSTER are kept (Codex
///     R21): topic names are only unique within a cluster, and a replay
///     against cluster B can never rebuild cluster A's rows — dropping them
///     would be permanent loss;
///   * `used` is ignored — an unused-but-loaded row would double just the
///     same (Codex R19).
///
/// Cluster identity (Codex R21 → R24 F2 → R26 F3): the broker-side Kafka
/// **cluster id** is the ONLY matching identity. `bootstrap.servers`
/// equality is neither necessary (two orderings / aliases of one list name
/// one cluster) nor sufficient (a DNS repoint can make one string name two
/// clusters over time) — R26 F3 removed the R21-era bootstrap FALLBACK
/// after it was shown to claim a bootstrap-only row across a DNS repoint:
/// the row was stamped in a session that could not resolve the cluster id,
/// the name was later repointed to a different cluster, and the "matching"
/// re-create dropped rows its replay could never rebuild. With
/// `cluster_id` = the id resolved from the (re)starting consumer (`None` =
/// could not be resolved), the rules are — fail-safe throughout: a skipped
/// drop costs at worst DUPLICATED rows (rebuilt by the replay), a wrong
/// drop is PERMANENT loss, so every unknown identity is a non-match:
///   * row stamped with a `clusterId` AND `cluster_id` is `Some`: drop on
///     **cluster-id equality alone** (bootstrap is NOT consulted — the id
///     survives reordering, aliasing, and DNS repoints);
///   * row stamped with a `clusterId` but `cluster_id` is `None`: **never
///     dropped** — the current consumer's identity is unknown, and claiming
///     the row on bootstrap equality could destroy another cluster's data
///     that this replay can never rebuild;
///   * row WITHOUT a `clusterId` (legacy R21-era rows, or rows published
///     while the id was unobtainable): **never auto-dropped** either — its
///     provenance can name a repointed cluster just the same. Kept rows
///     are counted in a WARN so the operator can see the duplication
///     residual an earliest replay then produces (documented default:
///     duplication over loss; delete such rows explicitly if their origin
///     is known).
///
/// Ingestion-schema guard (Codex R26 F1, tightened by R27 F1/F3): every
/// victim CANDIDATE (a row the rules above would drop) additionally has
/// its stamped `schemaFp` inspected against `schema_fp` — the (re)starting
/// spec's [`schema_fingerprint`], always of the CURRENT canonicalisation
/// version:
///   * stamped with the CURRENT version and EQUAL → a victim (the replay
///     provably rebuilds it under the same ingestion + record-selection
///     semantics);
///   * stamped with the current version but DIFFERENT → the whole
///     operation is REFUSED before anything is deleted: the replay would
///     re-read the old records under the NEW schema (a renamed timestamp
///     column dead-letters every one of them; a `read_committed` replay
///     never redelivers a `read_uncommitted` generation's aborted
///     transactional records — R27 F1), so the dropped rows would not be
///     rebuilt. Changing a pair's schema therefore requires a NEW
///     datasource name (or explicitly deleting the prior rows);
///   * `schemaFp` missing / non-string / of an UNKNOWN or OLDER version
///     (an unprefixed R26-era stamp): the row is UNVERIFIABLE — it is
///     KEPT + warn-counted and does NOT block (R27 F3; pre-R27 a missing
///     fingerprint was dropped, which silently lost the rows whenever the
///     schema had in fact changed since). Same fail-safe trade as the
///     cluster-identity rules: keeping risks only DUPLICATION on the
///     earliest replay, a wrong drop is permanent loss.
///
/// Failure atomicity (Codex R22, supersedes the R19 compensation): the
/// metadata rows are deleted FIRST, in ONE transaction. A failure there is
/// fail-closed — nothing has been dropped anywhere, the error propagates,
/// the caller must NOT start the consumer, and a retry sees the full victim
/// set again. Only after that commit is the query-visible drop applied,
/// synchronously, via a drop-only [`Historical::replace_segments`] call —
/// which only SHRINKS the cache, so cache-limit admission cannot reject it;
/// its sole failure mode is a poisoned segment-map lock. The previous order
/// (Historical drop first + compensating re-ADD on a metadata failure) was
/// rejected by Codex R22: the compensation was an ADD and therefore subject
/// to cache-limit admission — another datasource consuming the freed bytes
/// between the drop and the failed delete made the compensation itself
/// fail, freezing the half-state.
///
/// Honest residual (lock poisoning only): if the drop-only swap fails after
/// the metadata commit, the victim rows stay loaded (query-visible) with no
/// metadata row — a later earliest re-create would not find them as victims
/// while the replay re-ingests their records (duplication). But a poisoned
/// segment-map lock means another thread PANICKED holding it: the process
/// is already broken and must be restarted, and a restart clears the
/// memory-resident segments — erasing the residual by construction.
///
/// Cancellation safety (Codex R20/R22/R23): destructive by design, so this
/// must only ever run INSIDE an uncancellable spawned lifecycle op
/// ([`run_lifecycle_op`]) — and in the SAME op as the consumer start whose
/// replay rebuilds the dropped rows, never in a separate task from it (the
/// R23 seam). It takes only the datasource publish lock, so the op it runs
/// in cannot deadlock the registry drain.
/// Outcome of [`select_streaming_victims`]: the victim ids the
/// earliest-replay cleanup would drop, plus the fail-safe KEEP counters
/// surfaced to the operator (see [`drop_streaming_segments_task`]'s match
/// rules).
#[derive(Debug)]
struct StreamingVictimScan {
    victims: Vec<String>,
    kept_no_cluster_id: usize,
    kept_identity_unknown: usize,
    kept_schema_unverifiable: usize,
    /// Same-pair, same-cluster, same-schema rows that ARE DURABLE (carry
    /// `payload.kafkaOffsets`) and so are KEPT, never dropped (compat-3
    /// stage 2): they are reloaded from deep storage at bootstrap and the
    /// consumer resumes PAST their offsets, so dropping them (with no replay
    /// to rebuild them) would be pure loss. The schema-change refusal still
    /// fires on them first — only the DROP is suppressed.
    kept_durable: usize,
}

/// Apply [`drop_streaming_segments_task`]'s victim match rules (provenance
/// kind/topic → cluster-id identity → schema-fingerprint gate) to `rows`,
/// WITHOUT deleting anything. Split out (Codex R30 F2) so the lifecycle ops
/// can ask "would this cleanup drop anything?" BEFORE running the
/// destructive pass — the topic-readiness probe only gates a cleanup that
/// has victims. The schema-change refusal (R26 F1) lives here, so both the
/// read-only scan and the destructive pass refuse identically.
///
/// # Errors
///
/// The R26 F1 refusal: a same-cluster row stamped with a CURRENT-version
/// but DIFFERENT schema fingerprint fails the whole (re)create before
/// anything is deleted.
fn select_streaming_victims(
    rows: Vec<SegmentMetadataRow>,
    data_source: &str,
    topic: &str,
    cluster_id: Option<&str>,
    schema_fp: &str,
) -> Result<StreamingVictimScan> {
    let mut scan = StreamingVictimScan {
        victims: Vec::new(),
        // Same-pair rows KEPT because their cluster identity cannot be
        // confirmed (R26 F3) — surfaced in the caller's WARN so the
        // duplication residual of an earliest replay is operator-visible.
        kept_no_cluster_id: 0,
        kept_identity_unknown: 0,
        // Cluster-matched rows KEPT because their ingestion-schema
        // fingerprint cannot be verified (missing, non-string, or of an
        // unknown/older canonicalisation version — R27 F1/F3).
        kept_schema_unverifiable: 0,
        // Cluster-matched, same-schema rows KEPT because they are DURABLE
        // (compat-3 stage 2) — resumed past their offsets, not dropped.
        kept_durable: 0,
    };
    for r in rows {
        if r.data_source != data_source
            || r.payload.get("kind").and_then(serde_json::Value::as_str) != Some("kafka-streaming")
            || r.payload.get("topic").and_then(serde_json::Value::as_str) != Some(topic)
        {
            continue;
        }
        // Cluster identity (Codex R21 → R24 F2 → R26 F3), fail-safe — see
        // the match-rule table in `drop_streaming_segments_task`'s doc.
        match (
            r.payload
                .get("clusterId")
                .and_then(serde_json::Value::as_str),
            cluster_id,
        ) {
            // Both identities known and EQUAL: the row is a victim
            // candidate — gate it on the ingestion-schema fingerprint
            // (R26 F1 / R27 F1/F3) BEFORE anything is deleted. Fail-closed:
            // one same-version-mismatching row refuses the whole
            // (re)create; an unverifiable fingerprint keeps the row.
            (Some(row_cid), Some(cid)) if row_cid == cid => {
                match r
                    .payload
                    .get("schemaFp")
                    .and_then(serde_json::Value::as_str)
                {
                    Some(row_fp) if schema_fp_is_current_version(row_fp) => {
                        if row_fp == schema_fp {
                            // DURABLE rows (compat-3 stage 2): carry
                            // `kafkaOffsets`, are reloaded from deep storage at
                            // bootstrap, and are resumed PAST via the frontier
                            // seek — so they are KEPT, never dropped (a drop
                            // with no replay to rebuild them would be pure
                            // loss). The schema-change refusal above still
                            // fires on them first. Only legacy, non-durable
                            // (pre-stage-1) streaming rows remain victims.
                            if r.payload.get("kafkaOffsets").is_some() {
                                scan.kept_durable += 1;
                            } else {
                                scan.victims.push(r.id);
                            }
                        } else {
                            return Err(DruidError::Ingestion(format!(
                                "refusing to (re)start the consumer for datasource \
                                 '{data_source}' / topic '{topic}': prior streaming segment \
                                 '{}' was built under a DIFFERENT ingestion schema (stamped \
                                 schemaFp {row_fp}, current {schema_fp}). Replaying the topic \
                                 under the changed schema cannot rebuild those rows (e.g. a \
                                 renamed timestamp column dead-letters every old record), so \
                                 dropping them would be permanent loss. To change the \
                                 ingestion schema, create the supervisor under a new \
                                 datasource name — or explicitly delete the prior streaming \
                                 rows first. Nothing was dropped.",
                                r.id
                            )));
                        }
                    }
                    // Missing / non-string / unknown-or-older fingerprint
                    // version: the row cannot be verified as rebuildable
                    // under the current schema — keep + warn, never drop,
                    // never block (R27 F1/F3; see the doc's match rules).
                    _ => scan.kept_schema_unverifiable += 1,
                }
            }
            // Both known, different: another cluster's rows — a replay
            // against THIS cluster can never rebuild them.
            (Some(_), Some(_)) => {}
            // The row names a cluster id but the current consumer's is
            // unknown: never claim it.
            (Some(_), None) => scan.kept_identity_unknown += 1,
            // Row without a cluster id: never auto-dropped (R26 F3 — its
            // bootstrap may be a repointed name for a different cluster).
            (None, _) => scan.kept_no_cluster_id += 1,
        }
    }
    Ok(scan)
}

/// Derive the per-partition durable RESUME evidence (compat-3 durability) from
/// the durable segment set: for every metadata row of this `(data_source,
/// topic)` pair on the SAME cluster, fold its `payload.kafkaOffsets` spans
/// into a [`PartitionResume`] per partition.
///
/// Crucially this distinguishes rows that ACTUALLY reloaded into the Historical
/// (`is_loaded(id)` true) from PHANTOM rows whose blob is missing (skipped at
/// bootstrap, C2). Only loaded spans of DEFINITE-identity rows (see below) seed
/// the skippable coverage; a phantom row still contributes its `start` to
/// `min_start`, pulling the resume point BELOW the hole so its records are
/// re-consumed rather than skipped past (which would be loss). [`resume_offset`](ferrodruid_ingest_kafka::streaming::resume_offset)
/// then reconciles this with the partition's committed Kafka offset — the
/// durable rows are the self-consistent offset store, so no separate checkpoint
/// can drift from the segments
/// ([`EosSegmentWriter::resume_offsets`](ferrodruid_ingest_kafka::eos_writer::EosSegmentWriter::resume_offsets)
/// model). This REPLACES the routine earliest-replay drop
/// ([`earliest_replay_cleanup`]) — under durability the reloaded segments must
/// be KEPT (a drop with no replay to rebuild them would be pure loss).
///
/// Cluster identity (Codex R3 C1 → R4 H4 → R5 H2): a row's `kafkaOffsets`
/// are only meaningful within the offset space of the cluster that produced
/// them — but the span plays TWO separable roles, and identity gates them
/// DIFFERENTLY (R5 H2):
///
/// * **FLOOR** (`min_start` — the resume never skips below it, the
///   loss-prevention / phantom-recovery role, R1 C2): applied
///   CONSERVATIVELY, to every durable row EXCEPT a definite mismatch. A
///   phantom row (blob missing) whose identity is unknowable must still pull
///   the resume below its hole — otherwise the committed offset (already
///   past the span) is trusted and the missing data is skipped forever
///   (loss). Flooring an actually-foreign row merely re-consumes from an
///   early offset (bounded at-least-once duplication, never loss).
/// * **ADVANCE** (the `loaded` spans — offsets the resume may skip PAST, the
///   dedup role, R3 H6): applied ONLY on a DEFINITE identity match (row
///   `clusterId` == the current consumer's resolved id). Skipping past an
///   unproven span could skip a different cluster's distinct records
///   (permanent loss — the R4 H4 finding); that guarantee is unchanged.
///
/// The identity pairings:
///
/// * **definite match** (both known, equal): floor + advance (when the blob
///   actually reloaded);
/// * **definite mismatch** (both known, different): NEITHER — another
///   cluster's offset space; folding it in either role would be meaningless
///   here (its records cannot exist in this cluster's offset space);
/// * **current identity unresolved** (row stamped, consumer id `None`):
///   floor only, loudly — a match cannot be confirmed, so the span never
///   advances the resume, but its start still guards against phantom loss;
/// * **row unstamped** (kafkaOffsets present, no `clusterId`): legacy pre-H4
///   residue (the publish path now ALWAYS stamps `clusterId` alongside
///   `kafkaOffsets`, and omits the offsets entirely when the identity is
///   unresolved). Floor only, loudly — an unknowable origin is never
///   treated as authoritative for a skip.
///
/// The R3 C1 concern (a suppressed frontier stranding a durable partition on
/// `auto.offset.reset`) is answered upstream by the stamp invariant
/// (same-cluster rows are always stamped); the R5 H2 floor additionally
/// guarantees that even unconfirmable rows anchor the resume at/below their
/// data. Floor-only rows themselves stay queryable and are never dropped;
/// cluster-id provenance for DROPS remains the CLEANUP's concern
/// ([`select_streaming_victims`]).
///
/// **Topic identity (Codex R7 H1 → R8 H1/H2, KIP-516):** offsets are only
/// meaningful within ONE topic *generation* — a topic deleted+recreated under
/// the same name on the same cluster REUSES the offset numbers for DIFFERENT
/// records, and when the new generation has been produced past the durable
/// frontier the stale target is IN-range, so the R5-H1 watermark clamp cannot
/// catch it. The row's stamped `topicId` gates the span TRI-STATE, as an
/// *enhancement* over the cluster gate — it only ever tightens the frontier
/// on POSITIVE evidence of a recreation, never on the absence of evidence:
///
/// * **definite topic-id match** (row stamped, current resolved, equal):
///   the row is generation-confirmed — advance-eligible (still requires the
///   definite cluster match + a reloaded blob);
/// * **definite topic-id MISMATCH** (both known, different): the topic was
///   RECREATED — the row belongs to a DEAD generation whose offsets are
///   meaningless in the live topic's offset space, so it is EXCLUDED from
///   BOTH roles, exactly like a cluster mismatch. Every partition such a row
///   names is FLAGGED `recreated` in its frontier entry (Codex R9 C1+C2) and
///   resumes on the unified recreation model: **floor = the retained log's
///   `low` watermark** — never the dead generation's committed offset (it
///   survives the recreate in the group coordinator but names a dead-space
///   record, R9 C1) and never the live rows' `min_start` (live evidence must
///   not lift the floor past a never-durable `[low, live_start)` publish
///   gap, R9 C2) — and **advance = the live generation's coverage walked
///   contiguously from `low`** (`recreated_resume_target`). This keeps the
///   two earlier guarantees simultaneously: no new-generation record is ever
///   skipped (R7 H1 — a gap below live coverage is re-consumed), and
///   coverage contiguous from `low` is skipped so nothing is re-published on
///   every restart (Codex R8 H1: the R7 shape floored dead rows at 0, and
///   when live coverage started at a retention-advanced `low` > 0 the
///   floor-0 never connected to it — permanent unbounded double count). The
///   dead rows themselves stay queryable (they hold the old generation's
///   data; nothing can rebuild them). A partition left with ONLY
///   dead-generation evidence — the first restart after the recreation,
///   before anything was re-published — gets a floor-0 entry with no
///   coverage, which the same derivation turns into "re-consume the retained
///   log from `low`";
/// * **current topic id unresolved** (row stamped, probe failed —
///   TLS/SASL listener, pre-2.8 broker) or **row unstamped** (no `topicId`):
///   the topic id decides NOTHING — the cluster-identity gating above
///   governs alone, the pre-R7 behavior (Codex R8 H2: downgrading these
///   rows to floor-only pinned the resume at the OLDEST durable span start,
///   so every TLS/SASL deployment re-consumed and re-published its ENTIRE
///   retained log on EVERY restart — unbounded duplication and durable
///   segment growth). The honest residual: a recreation that happens while
///   the id is unresolvable is undetectable (exactly the pre-R7 exposure),
///   accepted in exchange for bounded steady-state behavior; the loud
///   per-session warns below flag the degraded-detection mode.
///
/// Administratively DISABLED rows — `used == false` — are excluded (Codex R3
/// H5): they are not reloaded at bootstrap, so their span would read as an
/// unloaded hole that forces the resume below it, replaying and RE-PUBLISHING
/// data an operator explicitly removed. Rows without `kafkaOffsets` (legacy /
/// pre-offset rows) contribute nothing. A partition with no durable row at
/// all is ABSENT from the map, so the consumer leaves it to
/// `auto.offset.reset` (never seeks it). **Both filters run BEFORE the
/// topic-id check above, so a recreation whose only evidence lives in a
/// disabled or `kafkaOffsets`-less row is invisible HERE — that evidence is
/// covered by the topic-level guard [`detect_topic_recreation`] (Codex R18
/// C1+C2), which scans ALL rows; callers must use [`derive_resume_frontier`]
/// to get both.**
///
/// The ingestion-schema guard (Codex R26 F1) is preserved on the earliest
/// path by [`select_streaming_victims`]; this derivation is deliberately pure
/// so it can run on BOTH the earliest and latest paths.
pub(crate) fn compute_resume_frontier(
    rows: &[SegmentMetadataRow],
    data_source: &str,
    topic: &str,
    cluster_id: Option<&str>,
    topic_id: Option<&str>,
    is_loaded: impl Fn(&str) -> bool,
) -> std::collections::BTreeMap<i32, PartitionResume> {
    use serde_json::Value;
    // Per partition: the min start across ALL durable rows (loaded or phantom),
    // and the spans of the rows that actually reloaded.
    let mut min_start: std::collections::BTreeMap<i32, i64> = std::collections::BTreeMap::new();
    let mut loaded: std::collections::BTreeMap<i32, Vec<OffsetSpan>> =
        std::collections::BTreeMap::new();
    // Warn counters — no exclusion/downgrade may be silent (Codex R3 C1 /
    // R4 H4 / R5 H2 / R7 H1 / R8 H1+H2).
    let mut excluded_mismatch = 0usize;
    let mut floor_only_identity_unresolved = 0usize;
    let mut floor_only_unstamped = 0usize;
    let mut recreated_generation = 0usize;
    let mut advance_topic_unresolved = 0usize;
    let mut advance_topic_unstamped = 0usize;
    // Codex R16 H1/H2: structurally corrupt payload data — never silent
    // (same discipline as the exclusions above). A malformed kafkaOffsets
    // span (H1) is floored but never advanced; a malformed clusterId /
    // topicId (H2, present but null/empty/non-string) is floor-only, never
    // conflated with a genuinely unstamped (field-absent) row.
    let mut malformed_span = 0usize;
    let mut floor_only_malformed_cluster_id = 0usize;
    let mut floor_only_malformed_topic_id = 0usize;
    // Partitions named by DEAD-generation rows (definite topic-id mismatch,
    // Codex R8 H1): excluded from floor+advance, but a partition with NO
    // live evidence at all must still seek the retained log's `low`.
    let mut dead_gen_partitions: std::collections::BTreeSet<i32> =
        std::collections::BTreeSet::new();
    for r in rows {
        if r.data_source != data_source
            || r.payload.get("kind").and_then(Value::as_str) != Some("kafka-streaming")
            || r.payload.get("topic").and_then(Value::as_str) != Some(topic)
        {
            continue;
        }
        // Administratively disabled rows never enter the frontier (H5).
        if !r.used {
            continue;
        }
        // Only DURABLE rows (with `kafkaOffsets`) can contribute — and only
        // they are worth warn-counting below.
        let Some(offsets) = r.payload.get("kafkaOffsets").and_then(Value::as_object) else {
            continue;
        };
        // Cluster identity (Codex R4 H4 → R5 H2): the span's two roles are
        // gated separately — FLOOR (min_start, loss prevention) applies to
        // every pairing except a definite mismatch; ADVANCE (skippable
        // coverage) requires a DEFINITE match. See the doc comment.
        let cluster_advance = match (classify_payload_stamp(&r.payload, "clusterId"), cluster_id) {
            (PayloadStamp::Present(row_cid), Some(cid)) if row_cid == cid => true,
            (PayloadStamp::Present(_), Some(_)) => {
                excluded_mismatch += 1;
                continue; // another cluster's offset space: neither role
            }
            (PayloadStamp::Present(_), None) => {
                floor_only_identity_unresolved += 1;
                false
            }
            (PayloadStamp::Absent, _) => {
                floor_only_unstamped += 1;
                false
            }
            (PayloadStamp::Malformed, _) => {
                // Codex R16 H2 (clusterId consistency): a clusterId that is
                // PRESENT but corrupt (null / empty / non-string) is not a
                // usable identity. Treat it as floor-only — never a definite
                // match that could advance, and (unlike a real mismatch) never
                // dropped from the loss-preventing FLOOR either.
                floor_only_malformed_cluster_id += 1;
                false
            }
        };
        // Topic identity (Codex R7 H1 → R8 H1/H2 → R16 H2, KIP-516):
        // FOUR-state — the id only ever BITES on positive evidence of a
        // recreation. A definite MISMATCH marks a DEAD generation: the row
        // is excluded from BOTH roles (its offsets are meaningless in the
        // live generation's space, exactly like a cluster mismatch —
        // flooring it at 0, the R7 shape, wedged the resume below live
        // coverage that starts at a retention-advanced `low` > 0 and
        // re-consumed it on EVERY restart, Codex R8 H1), remembering its
        // partitions so a partition with NO live evidence still seeks the
        // retained log's `low`. An ABSENT (field-missing) or unresolved
        // pairing decides NOTHING: the cluster gate above governs alone
        // (the R7 floor-only downgrade made every TLS/SASL deployment —
        // where the PLAINTEXT probe can never resolve an id — re-consume
        // its whole retained log on every restart, Codex R8 H2). A
        // MALFORMED stamp (field PRESENT but null / empty / non-string) is
        // NOT an unstamped row — it is a CORRUPT stamp: advancing it on the
        // cluster fallback would, after a recreation whose current id IS
        // resolvable and different, let a DEAD generation's row skip the
        // live generation's records = loss (Codex R16 H2). It is forced
        // floor-only.
        let topic_advance_ok = match (classify_payload_stamp(&r.payload, "topicId"), topic_id) {
            (PayloadStamp::Present(row_tid), Some(tid)) if row_tid != tid => {
                recreated_generation += 1;
                for pk in offsets.keys() {
                    if let Ok(partition) = pk.parse::<i32>() {
                        dead_gen_partitions.insert(partition);
                    }
                }
                continue; // dead generation: neither floor nor advance
            }
            (PayloadStamp::Present(_), Some(_)) => true, // definite match: advance-eligible
            (PayloadStamp::Present(_), None) => {
                // Current id unresolved: cluster fallback. Counted only
                // where the row is actually advance-eligible (the warn is
                // about advancing with recreation detection unavailable).
                if cluster_advance {
                    advance_topic_unresolved += 1;
                }
                true
            }
            (PayloadStamp::Absent, _) => {
                if cluster_advance {
                    advance_topic_unstamped += 1;
                }
                true
            }
            (PayloadStamp::Malformed, _) => {
                // Codex R16 H2: a corrupt topicId is untrustworthy — never
                // advance-eligible (floor-only). The valid span still floors
                // the resume below; only the skip-past is withheld
                // (fail-closed). Counted only where the row is otherwise
                // advance-eligible (a cluster-confirmed row whose advance the
                // malformed stamp actually withholds).
                if cluster_advance {
                    floor_only_malformed_topic_id += 1;
                }
                false
            }
        };
        // Only definite-cluster rows with a TRUSTWORTHY topic stamp
        // (non-malformed, non-dead-generation — Codex R16 H2) whose blob
        // ACTUALLY reloaded may seed the skippable coverage. A phantom
        // (unloaded) blob never advances.
        let row_advances = cluster_advance && topic_advance_ok && is_loaded(&r.id);
        for (pk, span) in offsets {
            let Ok(partition) = pk.parse::<i32>() else {
                continue;
            };
            // FLOOR (C2 / R5 H2): the span's `start` floors the resume point
            // whenever it is a valid, NON-NEGATIVE Kafka offset — never skip
            // below durable evidence, whoever produced it. A non-integer or
            // negative start is a corrupt stamp naming no real offset, so it
            // can neither floor nor advance (Codex R16 H1 — dropped entirely).
            let start = match span.get("start").and_then(Value::as_i64) {
                Some(s) if s >= 0 => s,
                _ => {
                    malformed_span += 1;
                    continue;
                }
            };
            // Phantom OR loaded, definite match OR unknown identity: the
            // start floors the resume point.
            min_start
                .entry(partition)
                .and_modify(|e| *e = (*e).min(start))
                .or_insert(start);
            // ADVANCE (H6) — Codex R16 H1: the span's `next` may only skip
            // past records when it is STRUCTURALLY sound: a valid i64,
            // `next >= start` (a backwards `next < start` span is corrupt),
            // and `next >= 1`. A malformed `next` (missing / non-integer /
            // backwards / non-positive) OVERSTATES durable coverage — trusting
            // it to advance would skip [real_next, stamped_next) forever
            // (loss). The start already floored above; only the skip-past is
            // withheld (fail-closed). Well-formed spans are unaffected — their
            // provenance is still trusted; the honest limitation is a
            // well-formed but numerically over-large `next`, undetectable
            // without offset provenance in the blob.
            let Some(next) = span.get("next").and_then(Value::as_i64) else {
                malformed_span += 1;
                continue;
            };
            if next < start || next < 1 {
                malformed_span += 1;
                continue;
            }
            if row_advances {
                loaded
                    .entry(partition)
                    .or_default()
                    .push(OffsetSpan::new(start, next));
            }
        }
    }
    // Recreation-detected partitions (Codex R8 H1 → R9 C1/C2): EVERY
    // partition named by a dead-generation row resumes on the unified
    // recreation model — the frontier entry is flagged `recreated`, so the
    // consumer floors it at the retained log's `low` watermark (never the
    // dead generation's committed offset, never the live rows' `min_start`)
    // and advances only through live coverage CONTIGUOUS from `low`
    // (`recreated_resume_target`). Pre-R9, a partition with ANY live
    // evidence dropped out of the dead-only floor: its resume started at
    // the live `min_start` and a never-durable `[low, live_start)` publish
    // gap was skipped forever (R9 C2 — loss). A dead-ONLY partition (the
    // first restart after a recreation, before anything was re-published)
    // additionally needs a floor-0 entry so it appears in the frontier at
    // all (no new-generation record is ever skipped — the R7 H1 guarantee).
    let mut dead_only_partitions = 0usize;
    for partition in &dead_gen_partitions {
        if !min_start.contains_key(partition) {
            min_start.insert(*partition, 0);
            dead_only_partitions += 1;
        }
    }
    if excluded_mismatch > 0 {
        tracing::warn!(
            data_source,
            topic,
            excluded_mismatch,
            "resume frontier: excluding same-pair durable rows stamped with a DIFFERENT \
             cluster id — their offsets belong to another cluster's offset space. Their \
             records are NOT resumed past; delete the rows explicitly if the pair was \
             deliberately repointed (Codex R3 C1: exclusion is loud, never a silent reset)",
        );
    }
    if floor_only_identity_unresolved > 0 {
        tracing::warn!(
            data_source,
            topic,
            floor_only_identity_unresolved,
            "resume frontier: same-pair durable rows FLOOR the resume but never \
             ADVANCE it because the CURRENT consumer's cluster identity could not \
             be resolved — a definite identity match is required before stamped \
             offsets may be skipped past (Codex R4 H4: after a bootstrap repoint, \
             another cluster's offsets would seek THIS cluster past its own records \
             = permanent loss), while the floor still guards their spans against a \
             stale-high committed offset (Codex R5 H2: no skip below durable \
             evidence = no loss; re-consuming the spans is bounded at-least-once \
             duplication). Resolve the broker's cluster id (KIP-78) to restore \
             full frontier-based resume",
        );
    }
    if floor_only_unstamped > 0 {
        tracing::warn!(
            data_source,
            topic,
            floor_only_unstamped,
            "resume frontier: same-pair durable rows carrying kafkaOffsets WITHOUT a \
             stamped clusterId (legacy pre-H4 rows — the publish path now always \
             stamps both together) FLOOR the resume but never ADVANCE it: their \
             origin cluster is unknowable, so their spans are never skipped past \
             (Codex R4 H4), while the floor still guards them against a stale-high \
             committed offset (Codex R5 H2: re-consume, bounded duplication, never \
             loss). Delete the rows explicitly if their origin is known foreign",
        );
    }
    if recreated_generation > 0 {
        tracing::warn!(
            data_source,
            topic,
            recreated_generation,
            dead_only_partitions,
            current_topic_id = topic_id.unwrap_or(""),
            "resume frontier: TOPIC RECREATION DETECTED (Codex R7 H1 / R8 H1 / \
             R9) — durable rows are stamped with a DIFFERENT KIP-516 topicId than \
             the topic currently carries, so their offsets name records of a \
             DELETED topic generation (offset numbers are REUSED across a \
             delete+recreate). The dead generation's rows are EXCLUDED from the \
             frontier, and every partition they name resumes on the unified \
             recreation model: floor = the retained log's LOW watermark (the dead \
             generation's committed offset is never trusted and is overwritten at \
             the floor seek), advance = the live generation's durable coverage \
             walked CONTIGUOUSLY from low — a [low, coverage) publish gap is \
             re-consumed (bounded at-least-once duplication), coverage contiguous \
             from low is skipped (no per-restart re-publish), and no record of the \
             new generation is ever skipped (never loss). The old-generation rows \
             stay queryable; delete them explicitly if the recreation made them \
             obsolete",
        );
    }
    if advance_topic_unresolved > 0 {
        tracing::warn!(
            data_source,
            topic,
            advance_topic_unresolved,
            "resume frontier: the CURRENT topic's KIP-516 topicId could not be \
             resolved (non-PLAINTEXT listener, pre-2.8 broker, or an unreachable \
             probe), so topic-RECREATION detection is unavailable this session: \
             cluster-confirmed durable rows advance the resume on the cluster \
             identity alone (Codex R8 H2 — refusing to advance here re-consumed \
             and re-published the ENTIRE retained log on every restart; the topic \
             id only ever tightens the frontier on a positively detected \
             recreation). If the topic was in fact deleted+recreated while the id \
             was unresolvable, new-generation records below the durable frontier \
             may be skipped until the id becomes resolvable — make the broker's \
             PLAINTEXT listener reachable or upgrade past Kafka 2.8 to restore \
             detection",
        );
    }
    if advance_topic_unstamped > 0 {
        tracing::warn!(
            data_source,
            topic,
            advance_topic_unstamped,
            "resume frontier: cluster-confirmed durable rows carry kafkaOffsets \
             WITHOUT a stamped topicId (published while the topic id was \
             unresolvable, or pre-R7 legacy) — their topic GENERATION is \
             unknowable, so they advance the resume on the cluster identity alone \
             (Codex R8 H2, the pre-R7 gating). A recreation spanning those rows' \
             lifetime is undetectable; delete the rows explicitly if the topic is \
             known to have been recreated since they were published",
        );
    }
    if malformed_span > 0 {
        tracing::warn!(
            data_source,
            topic,
            malformed_span,
            "resume frontier: durable rows carried STRUCTURALLY INVALID \
             kafkaOffsets spans (a non-integer / negative `start`, or a `next` \
             that is missing / non-integer / < start / non-positive) — Codex \
             R16 H1. A valid `start` still FLOORS the resume (loss prevention), \
             but the corrupt `next` was NOT trusted to ADVANCE it: a span whose \
             `next` overstates the durable coverage would skip [real, stamped) \
             forever = permanent loss. Re-consuming the span is bounded \
             at-least-once duplication. Inspect the rows; delete them explicitly \
             if they were hand-edited or corrupted",
        );
    }
    if floor_only_malformed_cluster_id > 0 {
        tracing::warn!(
            data_source,
            topic,
            floor_only_malformed_cluster_id,
            "resume frontier: durable rows carried a MALFORMED clusterId (present \
             but null / empty / non-string) — Codex R16 H2. A corrupt identity \
             cannot be a definite match, so its spans FLOOR the resume but never \
             ADVANCE it (like an unstamped row, R4 H4 / R5 H2); it is also NOT \
             treated as a definite MISMATCH (which would drop the loss-preventing \
             floor). Delete the rows explicitly if their origin is known foreign",
        );
    }
    if floor_only_malformed_topic_id > 0 {
        tracing::warn!(
            data_source,
            topic,
            floor_only_malformed_topic_id,
            current_topic_id = topic_id.unwrap_or(""),
            "resume frontier: cluster-confirmed durable rows carried a MALFORMED \
             topicId (present but null / empty / non-string) — Codex R16 H2. A \
             corrupt topic stamp is NOT an unstamped row: it is untrustworthy, so \
             it is forced FLOOR-ONLY (never advance-eligible). Advancing it on the \
             cluster fallback would, after a topic delete+recreate whose current \
             id IS resolvable and different, let a DEAD generation's row skip the \
             live generation's records = permanent loss. The valid span still \
             floors the resume (bounded at-least-once duplication). Delete the \
             rows explicitly if they were hand-edited or corrupted",
        );
    }
    min_start
        .into_iter()
        .map(|(partition, ms)| {
            let resume = PartitionResume::new(ms, loaded.remove(&partition).unwrap_or_default());
            let resume = if dead_gen_partitions.contains(&partition) {
                // R9: dead-generation evidence exists → the consumer resumes
                // this partition on the unified recreation model (floor =
                // `low`, live-coverage advance, consumed_next-driven
                // restore) regardless of live evidence.
                resume.mark_recreated()
            } else {
                resume
            };
            (partition, resume)
        })
        .collect()
}

/// Topic-level RECREATION detection over the FULL metadata row set (Codex
/// R18 C1+C2): whether ANY row of this `(data_source, kind=kafka-streaming,
/// topic)` pair — **used or administratively disabled, with or without
/// `kafkaOffsets`** — carries a stamped `topicId` in DEFINITE mismatch with
/// the CURRENT resolved topic id (both present, different).
///
/// This exists because [`compute_resume_frontier`]'s per-row recreation
/// handling only ever sees rows that survive its `used` + `kafkaOffsets`
/// filters, so two real evidence carriers were invisible (the R18 findings):
///
/// * **C1 — disabled rows**: with EVERY durable row of the pair disabled
///   (`used = false`), the frontier is empty and the resume no-ops — yet the
///   consumer group's committed offsets SURVIVE a topic delete+recreate and
///   name the DEAD generation's offset space, so resuming from them silently
///   skips every new-generation record below them (loss). The disabled rows
///   still hold the dead `topicId` — the only recreation evidence there is.
/// * **C2 — `kafkaOffsets`-less durable rows**: a row published while the
///   cluster identity was transiently unresolved carries a `topicId` but no
///   `kafkaOffsets` (the R4 H4 stamp invariant omits them together with the
///   `clusterId`), while the consumer still manual-committed its offsets.
///   The row names no partition, so its partition resumes from the stale
///   committed offset after a recreation — the same silent skip. This is
///   covered ONLY when the offsetless row's OWN `clusterId` positively
///   confirms the same cluster (Codex R26 H1): a clusterId-ABSENT offsetless
///   row is ambiguous (see the gates) and is a degraded-mode residual, NOT
///   evidence.
///
/// Gates, kept consistent with the frontier's tri-state logic:
///
/// * a topicId mismatch is recreation evidence ONLY on a row whose OWN
///   `clusterId` is PRESENT and == the current resolved id (SAME cluster
///   confirmed, Codex R26 H1). A same-cluster recreation and a bootstrap
///   cluster REPOINT A→B both present as a topicId mismatch and are told
///   apart only by the clusterId, so a clusterId-ABSENT (or MALFORMED) row is
///   AMBIGUOUS and never fires — firing on a repoint's foreign rows would
///   flood every partition of the new cluster to `low` and double-count its
///   already-committed records;
/// * a row whose `clusterId` is PRESENT but DIFFERENT is another cluster's
///   row — its different `topicId` is that cluster's topic, NOT evidence of a
///   recreation here (skipped, exactly like the frontier's cluster gate);
/// * a MALFORMED `topicId` (present but null / empty / non-string, Codex
///   R16 H2) is never treated as a mismatch — corrupt stamps stay
///   floor-only, they do not fire the guard;
/// * `topic_id == None` (current id unresolvable: non-PLAINTEXT listener,
///   pre-2.8 broker) detects NOTHING — the honest documented residual
///   (Codex R8 H2): a recreation is undetectable until the id becomes
///   resolvable;
/// * `cluster_id == None` (current cluster id unresolved) detects NOTHING
///   either (Codex R24 H1): a foreign-cluster row cannot be positively
///   excluded, so firing on the ambiguous case would double-count a
///   repointed cluster's records on every restart.
///
/// The verdict is FLOOR-only in effect (see
/// [`ResumeFrontier::topic_recreated`]): on detection the consumer floors
/// every assigned partition at the retained log's `low` watermark — bounded
/// at-least-once re-consumption of the retained log, never a skip. On a
/// definite MATCH (or any non-definite pairing) nothing changes: the normal
/// resume path is untouched, so there is no spurious re-consume.
pub(crate) fn detect_topic_recreation(
    rows: &[SegmentMetadataRow],
    data_source: &str,
    topic: &str,
    cluster_id: Option<&str>,
    topic_id: Option<&str>,
) -> bool {
    use serde_json::Value;
    let Some(tid) = topic_id else {
        // Unresolvable current id: detection unavailable this session (the
        // documented R8 H2 residual). Never fires on absence of evidence.
        return false;
    };
    // R24 H1: a topicId mismatch alone does NOT prove a recreation — a topic
    // delete+recreate (SAME cluster, new KIP-516 id) and a bootstrap CLUSTER
    // REPOINT A→B (DIFFERENT cluster, whose topic simply has a different id)
    // both present as "row topicId != current topicId". They are told apart
    // only by the CLUSTER identity. With the current cluster id UNRESOLVED we
    // cannot distinguish them, and firing on the ambiguous case treats a
    // repoint's foreign rows as a recreation — flooring every partition to the
    // retained log's `low` on EVERY restart while the id stays unresolvable, so
    // the new cluster's already-committed records are re-consumed and
    // permanently double-counted. Stay silent instead: the per-row frontier
    // gating still floors conservatively (loss-prevention), and a genuine
    // recreation whose current id is unresolvable is the pre-existing
    // documented R8 H2 residual (unchanged). Detection requires a RESOLVED
    // current cluster id so a foreign-cluster row can be positively excluded.
    let Some(cid) = cluster_id else {
        return false;
    };
    let mut evidence_rows = 0usize;
    for r in rows {
        if r.data_source != data_source
            || r.payload.get("kind").and_then(Value::as_str) != Some("kafka-streaming")
            || r.payload.get("topic").and_then(Value::as_str) != Some(topic)
        {
            continue;
        }
        // R26 H1: a topicId mismatch is recreation evidence ONLY on a row
        // whose OWN clusterId DEFINITELY confirms this cluster (PRESENT and
        // == the current resolved id). A same-cluster topic delete+recreate
        // (a new KIP-516 id, the OLD row keeping the dead one) and a bootstrap
        // CLUSTER REPOINT A→B (a DIFFERENT cluster whose topic simply has a
        // different id) BOTH present as "row topicId != current topicId", and
        // are told apart ONLY by the row's clusterId. A clusterId-ABSENT row
        // — an offsetless row published while the cluster id was transiently
        // unresolved, since the R4 H4 invariant omits clusterId and
        // kafkaOffsets TOGETHER — is therefore AMBIGUOUS: firing on it treats
        // a repoint's foreign rows as a recreation and floors every partition
        // of cluster B to `low` on restart, re-consuming and permanently
        // double-counting B's already-committed (bootstrap-loaded) records
        // (this needs no lost async commit — strictly more reachable than the
        // R25 residual). A MALFORMED clusterId is likewise not a definite
        // match. Skip both — only a same-cluster-CONFIRMED row's topicId
        // mismatch counts. Downgrade: a genuine same-cluster recreation whose
        // ONLY evidence is a clusterId-absent offsetless row is now a
        // cluster-id-unresolved DEGRADED-MODE residual (the row names no
        // partition, so the numeric frontier cannot floor it either — see
        // `derive_resume_frontier`'s honest limitation); it never arises on a
        // healthy PLAINTEXT / Kafka 2.8+ broker where the clusterId is always
        // stamped.
        let PayloadStamp::Present(row_cid) = classify_payload_stamp(&r.payload, "clusterId") else {
            continue; // clusterId Absent / Malformed → ambiguous, not evidence
        };
        if row_cid != cid {
            continue; // a DIFFERENT cluster's row (foreign repoint) — never our recreation
        }
        // DEFINITE topic-id mismatch only (both present, different): a
        // malformed stamp is not a mismatch (R16 H2), an absent stamp is no
        // evidence.
        if let PayloadStamp::Present(row_tid) = classify_payload_stamp(&r.payload, "topicId")
            && row_tid != tid
        {
            evidence_rows += 1;
        }
    }
    if evidence_rows > 0 {
        tracing::warn!(
            data_source,
            topic,
            evidence_rows,
            current_topic_id = tid,
            "TOPIC RECREATION DETECTED from the full metadata row set (Codex \
             R18 C1+C2): rows of this pair — including administratively \
             disabled rows and durable rows without kafkaOffsets, which never \
             enter the numeric resume frontier — are stamped with a DIFFERENT \
             KIP-516 topicId than the topic currently carries. The consumer \
             group's committed offsets survive a delete+recreate but name the \
             DEAD generation's offset space, so NO committed offset is trusted: \
             every assigned partition resumes from the retained log's LOW \
             watermark, advanced only through live-generation durable coverage \
             (bounded at-least-once duplication, never loss). Delete the \
             old-generation rows explicitly once they are obsolete",
        );
        true
    } else {
        false
    }
}

/// Derive the complete durable-resume directive for a (re)starting consumer
/// (compat-3 durability): the per-partition numeric frontier
/// ([`compute_resume_frontier`] — used + `kafkaOffsets` rows only, exactly
/// as before) PLUS the topic-level recreation verdict
/// ([`detect_topic_recreation`] — over ALL rows, Codex R18 C1+C2), keyed on
/// the multi-broker topic-id probe's tri-state verdict (Codex R28 H1):
///
/// * [`TopicIdProbe::Agreed`] — the definite current id drives the R7/R18
///   match/mismatch logic exactly as before;
/// * [`TopicIdProbe::Unresolved`] — no id: detection unavailable, the
///   cluster gating governs alone (the documented R8 H2 residual — exactly
///   the pre-R28 `None`);
/// * [`TopicIdProbe::Disagreed`] — bootstrap brokers CONFLICT on the id: a
///   recreation's metadata propagation window. This is POSITIVE evidence
///   that a recreation may have just happened with NO definite current id
///   to compare against, so the derivation goes conservative on BOTH axes:
///   `topic_recreated` is set (every partition floors at the retained log's
///   `low`, committed offsets untrusted) AND every frontier entry's
///   coverage is STRIPPED — with no current id, no row can be
///   generation-confirmed, and a dead-generation span left in `loaded`
///   would let `recreated_resume_target` walk past new-generation records
///   from `low` (the exact skip this floor exists to prevent). Bounded
///   at-least-once re-consumption of the retained log, never a skip; a
///   restart after the metadata converges resumes normally.
///
/// `rows` must therefore be the FULL row set of the datasource (used AND
/// disabled — [`MetadataStore::get_all_segments`]), not just the used rows:
/// disabled rows are still excluded from the numeric derivation by the
/// frontier's own `used` filter (Codex R3 H5), but their `topicId` is
/// recreation EVIDENCE the guard must see (C1).
///
/// On a detected recreation every frontier entry is recreation-flagged: the
/// committed offsets of partitions WITH live durable evidence are just as
/// untrusted as the rest (the dead generation's commit may still be the
/// group's last landed value), so each is resumed on the unified R9 model —
/// floor = the retained log's `low`, advance = live coverage contiguous from
/// `low` — never `min(committed, min_start)`, which could sit above a
/// never-durable `[low, live_start)` publish gap (the R9 C2 shape).
/// Partitions with NO entry are floored at `low` by the consumer's
/// synthesis ([`ResumeFrontier::synthesize_recreated`]).
///
/// **Honest limitation (Codex R26 H1, cluster-id-unresolved degraded mode):**
/// a genuine same-cluster recreation whose ONLY surviving evidence is a
/// clusterId-ABSENT offsetless row (published while the cluster id was
/// transiently unresolved — the R4 H4 invariant omits clusterId and
/// kafkaOffsets together) is NOT detected: such a row is indistinguishable
/// from a bootstrap cluster repoint (see [`detect_topic_recreation`]), and
/// its partition — carrying no `kafkaOffsets` — is also absent from the
/// numeric frontier, so nothing floors it and it resumes from the stale
/// committed offset (a post-recreation skip = loss). This is the same
/// degraded-mode residual bucket as Codex R25 / R8 H2 and NEVER arises on a
/// healthy PLAINTEXT / Kafka 2.8+ broker, where the clusterId is always
/// stamped and the recreation is detected on the same-cluster-confirmed row.
///
/// **Honest limitation (Codex R28 H1, propagation-window residual):** the
/// disagreement floor only helps when the probe SEES the conflict. If every
/// responsive broker still serves the STALE id (all lagging, or the fresh
/// brokers unreachable), or the bootstrap names a single broker (no
/// agreement concept), the probe returns a unanimous — stale — `Agreed` id
/// that matches the durable rows' stamps and the recreation stays
/// undetected, exactly the pre-R28 exposure. Multi-broker agreement NARROWS
/// the window; a metadata-based detection cannot eliminate it. PLAINTEXT
/// probe only, as before (non-PLAINTEXT is `Unresolved`, the R8 H2
/// residual).
pub(crate) fn derive_resume_frontier(
    rows: &[SegmentMetadataRow],
    data_source: &str,
    topic: &str,
    cluster_id: Option<&str>,
    topic_probe: &TopicIdProbe,
    is_loaded: impl Fn(&str) -> bool,
) -> ResumeFrontier {
    let topic_id = topic_probe.agreed_id();
    let disagreed = topic_probe.is_disagreed();
    let topic_recreated =
        disagreed || detect_topic_recreation(rows, data_source, topic, cluster_id, topic_id);
    let mut partitions =
        compute_resume_frontier(rows, data_source, topic, cluster_id, topic_id, is_loaded);
    if disagreed {
        // Codex R28 H1: brokers disagree on the current id, so NO row can be
        // generation-confirmed — a span computed on the cluster fallback may
        // belong to the DEAD generation, and once every entry is
        // recreation-flagged below, `recreated_resume_target` would walk
        // that span from `low` and seek PAST new-generation records (skip =
        // loss). Strip ALL coverage: every partition floors at `low` with
        // nothing to advance through (bounded re-consumption, never a skip).
        let stripped_coverage: usize = partitions
            .values()
            .filter(|resume| !resume.loaded.is_empty())
            .count();
        partitions = partitions
            .into_iter()
            .map(|(p, resume)| (p, PartitionResume::new(resume.min_start, Vec::new())))
            .collect();
        tracing::warn!(
            data_source,
            topic,
            stripped_coverage,
            "resume frontier: TOPIC-ID DISAGREEMENT across bootstrap brokers \
             (Codex R28 H1) — treating the topic as RECREATION-SUSPECTED: \
             committed offsets are untrusted, durable coverage is NOT skipped \
             past (no row can be generation-confirmed without a definite \
             current id), and every assigned partition resumes from the \
             retained log's LOW watermark. Re-consuming the retained log is \
             bounded at-least-once duplication, never loss; a restart after \
             the brokers' metadata converges resumes normally",
        );
    }
    if topic_recreated {
        partitions = partitions
            .into_iter()
            .map(|(p, resume)| (p, resume.mark_recreated()))
            .collect();
    }
    ResumeFrontier {
        partitions,
        topic_recreated,
    }
}

/// Classification of an OPTIONAL identity/generation stamp in a segment
/// metadata payload (Codex R16 H2) — distinguishing a field that is genuinely
/// ABSENT from one that is PRESENT but corrupt. The two must never be
/// conflated: an absent stamp is a legitimately unstamped row (legacy / an
/// unresolved probe) that may still advance the resume on the cluster
/// fallback, whereas a malformed stamp is untrustworthy and is forced
/// floor-only.
enum PayloadStamp<'a> {
    /// The field is not present at all (a genuinely unstamped row).
    Absent,
    /// The field is present but not a usable value (null, empty string, or a
    /// non-string type) — a corrupt stamp.
    Malformed,
    /// The field is present and a non-empty string.
    Present(&'a str),
}

/// Classify `payload[key]` (Codex R16 H2): missing → [`PayloadStamp::Absent`],
/// a non-empty string → [`PayloadStamp::Present`], anything else (null / empty
/// string / non-string type) → [`PayloadStamp::Malformed`].
fn classify_payload_stamp<'a>(payload: &'a serde_json::Value, key: &str) -> PayloadStamp<'a> {
    match payload.get(key) {
        None => PayloadStamp::Absent,
        Some(serde_json::Value::String(s)) if !s.is_empty() => PayloadStamp::Present(s),
        Some(_) => PayloadStamp::Malformed,
    }
}

pub(crate) async fn drop_streaming_segments_task(
    metadata: &Arc<MetadataStore>,
    historical: &Arc<Historical>,
    data_source: &str,
    topic: &str,
    cluster_id: Option<&str>,
    schema_fp: &str,
) -> Result<usize> {
    let publish_lock = metadata.datasource_publish_lock(data_source).await;
    let _guard = publish_lock.lock().await;

    let StreamingVictimScan {
        victims,
        kept_no_cluster_id,
        kept_identity_unknown,
        kept_schema_unverifiable,
        kept_durable,
    } = select_streaming_victims(
        metadata.get_all_segments().await?,
        data_source,
        topic,
        cluster_id,
        schema_fp,
    )?;
    if kept_durable > 0 {
        tracing::info!(
            data_source,
            topic,
            kept_durable,
            "keeping same-pair, same-cluster DURABLE streaming segments (compat-3 \
             stage 2): they are reloaded from deep storage and the consumer resumes \
             past their committed offsets, so the earliest-replay drop is suppressed \
             for them (dropping them would be pure loss)",
        );
    }
    if kept_no_cluster_id > 0 || kept_identity_unknown > 0 {
        tracing::warn!(
            data_source,
            topic,
            kept_no_cluster_id,
            kept_identity_unknown,
            "keeping same-pair streaming rows whose cluster identity cannot be \
             confirmed (no stamped clusterId / current identity unresolved): an \
             earliest replay may DUPLICATE their records — the documented fail-safe \
             (duplication over loss). Delete these rows explicitly if their origin \
             is known",
        );
    }
    if kept_schema_unverifiable > 0 {
        tracing::warn!(
            data_source,
            topic,
            kept_schema_unverifiable,
            "keeping same-pair, same-cluster streaming rows whose ingestion-schema \
             fingerprint is missing or of an unknown/older version (R27 F1/F3): they \
             cannot be verified as rebuildable under the current schema, so they are \
             never auto-dropped — an earliest replay may DUPLICATE their records \
             (duplication over loss). Delete these rows explicitly if their origin \
             is known",
        );
    }
    if victims.is_empty() {
        return Ok(0);
    }

    // Metadata delete FIRST, in ONE transaction (Codex R22): a failure here
    // is fail-closed — NOTHING has been dropped anywhere, so no compensation
    // exists to fail; the caller must not start the consumer and a retry
    // sees the full victim set again.
    if let Err(e) = metadata.delete_segments(&victims).await {
        return Err(DruidError::Ingestion(format!(
            "refusing to (re)start the consumer for datasource '{data_source}' / topic \
             '{topic}': could not delete its prior streaming segments' metadata rows \
             (nothing was dropped; a replaying consumer would duplicate every row): {e}"
        )));
    }

    // Query-visible drop SECOND, synchronously (no cancellation point between
    // the commit and this call — we are inside the spawned lifecycle op). A
    // drop-only `replace_segments` only shrinks the cache, so cache-limit
    // admission cannot reject it; its sole failure mode is a poisoned lock
    // (see the honest residual in this function's doc). Ids that are not
    // currently loaded are tolerated (e.g. an unused row never re-loaded).
    if let Err(e) = historical.replace_segments(&victims, vec![]) {
        tracing::error!(
            data_source,
            topic,
            error = %e,
            "query-visible drop failed AFTER the metadata delete committed \
             (poisoned lock): the victim rows stay loaded without metadata rows; \
             the process must be restarted, which clears the in-memory residual",
        );
        return Err(DruidError::Ingestion(format!(
            "refusing to (re)start the consumer for datasource '{data_source}' / topic \
             '{topic}': prior streaming segments' metadata rows were deleted but the \
             query-visible drop failed (poisoned lock — restart the process): {e}"
        )));
    }
    tracing::info!(
        data_source,
        topic,
        dropped = victims.len(),
        "dropped prior streaming segments before (re)start; the topic replay rebuilds them",
    );
    Ok(victims.len())
}

// `crate::parse_kafka_supervisor_spec`, `crate::validate_kafka_spec`, `is_kafka_typed`, and
// `datasource_of` were hoisted to the crate root (ungated) so
// `create_supervisor` can derive a stable id-less Kafka supervisor id AND
// validate a Kafka spec before persisting, in the default (no-`kafka-io`)
// build too — see `crate::parse_kafka_supervisor_spec` /
// `crate::validate_kafka_spec` / `crate::is_kafka_typed` /
// `crate::datasource_of` (Codex R11/R14).

// The `suspended`-flag handling was hoisted to the crate root as
// `crate::kafka_suspended_flag` (ungated, Druid-faithful string coercion,
// loud reject of junk values — Fable audit) so both builds treat the
// lifecycle flag identically.

/// A Kafka consumer that has been CREATED + SUBSCRIBED but not yet started.
/// Produced by [`prepare_kafka_consumer`] BEFORE the supervisor spec is
/// persisted, so a bad client config fails without leaving a persisted-but-
/// broken supervisor row (Codex R4). Started with [`start_prepared`].
pub(crate) struct PreparedKafkaConsumer {
    consumer: KafkaStreamConsumer,
    config: ferrodruid_ingest_kafka::consumer::KafkaConsumerConfig,
    supervisor_id: String,
    /// Ingestion-schema fingerprint of the VALIDATED spec this consumer was
    /// prepared from ([`schema_fingerprint`], Codex R26 F1). Threaded into
    /// the pre-start owned-segment drop (to refuse a schema-changing
    /// re-create whose replay could not rebuild the dropped rows) and into
    /// every published segment's provenance.
    schema_fp: String,
}

impl PreparedKafkaConsumer {
    /// Target datasource of the prepared consumer (for the pre-start
    /// owned-segment drop).
    pub(crate) fn data_source(&self) -> &str {
        &self.config.data_source
    }

    /// Source topic of the prepared consumer (for the pre-start
    /// owned-segment drop's provenance match).
    pub(crate) fn topic(&self) -> &str {
        &self.config.topic
    }

    /// Ingestion-schema fingerprint of the spec this consumer was prepared
    /// from (for the pre-start owned-segment drop's schema-change refusal —
    /// Codex R26 F1).
    pub(crate) fn schema_fp(&self) -> &str {
        &self.schema_fp
    }

    /// Whether the prepared consumer replays from the EARLIEST offset.
    /// Already defaulted (`Option<bool>` → `unwrap_or(false)`, the Druid
    /// spec default) by `build_consumer_config`, so this is exactly what
    /// the consumer will do. Gates the pre-start owned-segment drop (Codex
    /// R20): a latest consumer never redelivers the old records, so its
    /// prior segments must be KEPT, not dropped.
    pub(crate) fn use_earliest_offset(&self) -> bool {
        self.config.use_earliest_offset
    }
}

/// How long to wait for the broker-side Kafka cluster id when a lifecycle
/// op resolves a prepared consumer's cluster identity
/// ([`resolve_cluster_id`]). Bounded: an unreachable broker costs at most
/// this once per create/resume, on the blocking pool. Tests use unroutable
/// brokers, so the test build keeps the wait short.
#[cfg(not(test))]
const CLUSTER_ID_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(test)]
const CLUSTER_ID_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

/// Resolve the prepared consumer's Kafka **cluster id** — the first-class
/// cluster identity for the pre-start owned-segment drop and for the
/// provenance stamped on every segment it will publish (Codex R24 F2).
///
/// Runs [`ferrodruid_ingest_kafka::streaming::fetch_cluster_id`] (which
/// BLOCKS up to [`CLUSTER_ID_FETCH_TIMEOUT`] waiting for broker metadata)
/// on the blocking pool, moving the prepared consumer through it and back.
/// `None` means the identity is UNKNOWN (unreachable broker / no id):
/// callers proceed with the documented fail-safe matching (clusterId-
/// stamped rows are then never dropped) and stamp bootstrap-only
/// provenance. `Err` only if the blocking task itself failed (cancelled
/// runtime shutdown or a panic in librdkafka) — fail-closed, since the
/// prepared consumer is lost with it.
pub(crate) async fn resolve_cluster_id(
    prepared: PreparedKafkaConsumer,
) -> Result<(PreparedKafkaConsumer, Option<String>)> {
    tokio::task::spawn_blocking(move || {
        let cluster_id = ferrodruid_ingest_kafka::streaming::fetch_cluster_id(
            &prepared.consumer,
            CLUSTER_ID_FETCH_TIMEOUT,
        );
        if cluster_id.is_none() {
            tracing::warn!(
                supervisor_id = %prepared.supervisor_id,
                data_source = %prepared.config.data_source,
                topic = %prepared.config.topic,
                "could not resolve the Kafka cluster id: proceeding with UNKNOWN \
                 cluster identity — prior rows stamped with a clusterId will NOT be \
                 dropped (fail-safe; an earliest replay may then duplicate them), \
                 published rows carry bootstrap-only provenance WITHOUT kafkaOffsets \
                 (Codex R4 H4: identity-less offsets never seed a resume frontier), \
                 and prior durable rows do NOT steer the resume seek (the committed \
                 offsets govern instead)",
            );
        }
        (prepared, cluster_id)
    })
    .await
    .map_err(|e| {
        DruidError::Ingestion(format!(
            "the cluster-id resolution task did not run to completion: {e}"
        ))
    })
}

/// How long [`resolve_topic_id`]'s wire probe may block resolving the
/// KIP-516 topic id. Bounded like [`CLUSTER_ID_FETCH_TIMEOUT`], and short in
/// tests (which use unroutable brokers).
#[cfg(not(test))]
const TOPIC_ID_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(test)]
const TOPIC_ID_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

/// Resolve the source topic's KIP-516 **topic id** (topic UUID) — the
/// identity that survives a topic delete+recreate (Codex R7 H1) — via the
/// safe-Rust wire probe
/// ([`ferrodruid_ingest_kafka::topic_id::fetch_topic_id`]: `ApiVersions` +
/// `Metadata v10+` over plain TCP against EVERY bootstrap broker; librdkafka
/// only exposes topic ids through Admin-API FFI, unreachable under
/// `#![forbid(unsafe_code)]`), returning the tri-state agreement verdict
/// (Codex R28 H1):
///
/// * [`TopicIdProbe::Agreed`] — the definite current id (stamped into
///   published provenance and compared on resume);
/// * [`TopicIdProbe::Disagreed`] — brokers CONFLICT on the id (a
///   recreation's metadata propagation window): recreation-SUSPECTED, the
///   resume derivation floors the topic (never skips) and nothing is
///   stamped;
/// * [`TopicIdProbe::Unresolved`] — best-effort fail-safe (a non-PLAINTEXT
///   `security.protocol`, a pre-2.8 broker, unreachable/misbehaving
///   brokers, or a timeout): published rows are stamped without `topicId`
///   and the resume frontier falls back to the cluster-identity gating
///   alone (Codex R8 H2: recreation detection is unavailable, but the
///   resume still advances past cluster-confirmed durable spans — the R6
///   behavior).
///
/// Runs the blocking socket I/O on the blocking pool; a join failure
/// degrades to `Unresolved` (unlike [`resolve_cluster_id`] nothing is moved
/// through the task, so nothing is lost with it).
pub(crate) async fn resolve_topic_id(prepared: &PreparedKafkaConsumer) -> TopicIdProbe {
    let security = prepared
        .config
        .additional_properties
        .get("security.protocol")
        .map(String::as_str);
    if !ferrodruid_ingest_kafka::topic_id::plaintext_probe_supported(security) {
        tracing::warn!(
            supervisor_id = %prepared.supervisor_id,
            data_source = %prepared.config.data_source,
            topic = %prepared.config.topic,
            security_protocol = security.unwrap_or(""),
            "cannot resolve the KIP-516 topic id over a non-PLAINTEXT listener \
             (the safe-Rust wire probe speaks no TLS/SASL): proceeding with an \
             UNRESOLVED topic identity — durable rows are stamped without a \
             topicId and the resume frontier is gated by the cluster identity \
             alone (Codex R8 H2): topic-RECREATION detection is unavailable \
             for this session, but restarts still resume past the durable \
             frontier (no per-restart re-consumption)",
        );
        return TopicIdProbe::Unresolved;
    }
    let brokers = prepared.config.brokers.clone();
    let topic = prepared.config.topic.clone();
    let resolved = tokio::task::spawn_blocking(move || {
        ferrodruid_ingest_kafka::topic_id::fetch_topic_id(&brokers, &topic, TOPIC_ID_FETCH_TIMEOUT)
    })
    .await
    .unwrap_or(TopicIdProbe::Unresolved);
    match &resolved {
        TopicIdProbe::Agreed(_) => {}
        TopicIdProbe::Disagreed => {
            tracing::warn!(
                supervisor_id = %prepared.supervisor_id,
                data_source = %prepared.config.data_source,
                topic = %prepared.config.topic,
                "bootstrap brokers DISAGREE on the topic's KIP-516 id (Codex \
                 R28 H1) — the cluster's metadata is in flux, exactly a topic \
                 delete+recreate propagation window. The topic is treated as \
                 RECREATION-SUSPECTED for this start: no committed offset and \
                 no durable coverage is trusted, every assigned partition is \
                 floored at the retained log's LOW watermark (bounded \
                 at-least-once re-consumption, never a skip), and published \
                 rows are stamped without a topicId. A restart after the \
                 metadata converges resumes normally",
            );
        }
        TopicIdProbe::Unresolved => {
            tracing::warn!(
                supervisor_id = %prepared.supervisor_id,
                data_source = %prepared.config.data_source,
                topic = %prepared.config.topic,
                "could not resolve the KIP-516 topic id (unreachable broker, or a \
                 pre-2.8 broker without Metadata v10): proceeding with an UNRESOLVED \
                 topic identity — durable rows are stamped without a topicId and the \
                 resume frontier is gated by the cluster identity alone (Codex R8 \
                 H2): topic-RECREATION detection is unavailable for this session, \
                 but restarts still resume past the durable frontier (no per-restart \
                 re-consumption)",
            );
        }
    }
    resolved
}

/// How long the pre-cleanup topic-readiness probe
/// ([`verify_topic_readable`]) may block waiting for topic metadata.
/// Bounded like [`CLUSTER_ID_FETCH_TIMEOUT`], and short in tests (which
/// use unroutable brokers).
#[cfg(not(test))]
const TOPIC_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(test)]
const TOPIC_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

/// How long the pre-cleanup group-join probe ([`prove_group_joinable`]) may
/// poll the prepared consumer waiting for a permanent JoinGroup/config
/// rejection to surface (Codex R35). Bounded like [`TOPIC_PROBE_TIMEOUT`],
/// and short in tests (which use unroutable brokers — the probe then times
/// out transiently and PROCEEDS). A permanent rejection is queued shortly
/// after `subscribe`, so it normally surfaces on the first poll well within
/// this; only an empty/unknown reachable topic waits the whole window.
#[cfg(not(test))]
const GROUP_JOIN_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(test)]
const GROUP_JOIN_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

/// Prove the prepared consumer can READ (in principle) its topic — broker
/// answers a topic-metadata request, the topic exists without a
/// topic-level error (authorization/unknown-topic arrive there), and it
/// has partitions — BEFORE the caller runs anything destructive (Codex R30
/// F2). Runs [`ferrodruid_ingest_kafka::streaming::probe_topic_metadata`]
/// (blocking, bounded by [`TOPIC_PROBE_TIMEOUT`]) on the blocking pool,
/// moving the prepared consumer through it and back.
///
/// Unlike [`resolve_cluster_id`], a probe failure here is a fail-CLOSE
/// `Err`, not a warn-and-proceed: the caller is about to drop this pair's
/// prior segments on the promise that an earliest replay rebuilds them,
/// and a consumer that cannot read the topic can never keep that promise
/// (the R30 F2 loss chain: ACL kept for cluster metadata but lost for the
/// topic → cleanup deletes everything → recv fails forever → nothing
/// rebuilt). Honest limitation (see the probe's doc): metadata proves
/// DESCRIBE, not READ — a READ-only denial passes the probe and is caught
/// fail-loud at consume time instead
/// ([`consume_error_is_fatal`](ferrodruid_ingest_kafka::streaming::consume_error_is_fatal)
/// stops the consumer and the non-clean stop refuses the tombstone).
///
/// On success it ALSO returns the probe's [`TopicRecords`] verdict (Codex
/// R31): whether the source topic currently holds records an earliest replay
/// could rebuild from. [`earliest_replay_cleanup`] uses it to KEEP (not
/// drop) the pair's prior segments when the topic is proven empty — the
/// deleted+recreated / retention-expired topic that a replay can never
/// rebuild (duplication over permanent loss).
pub(crate) async fn verify_topic_readable(
    prepared: PreparedKafkaConsumer,
) -> Result<(PreparedKafkaConsumer, TopicRecords)> {
    let (prepared, outcome) = tokio::task::spawn_blocking(move || {
        let outcome = ferrodruid_ingest_kafka::streaming::probe_topic_metadata(
            &prepared.consumer,
            &prepared.config.topic,
            TOPIC_PROBE_TIMEOUT,
        );
        (prepared, outcome)
    })
    .await
    .map_err(|e| {
        DruidError::Ingestion(format!(
            "the topic-readiness probe task did not run to completion: {e}"
        ))
    })?;
    match outcome {
        Err(e) => Err(DruidError::Ingestion(format!(
            "refusing to (re)start the consumer for datasource '{}' / topic '{}': the \
             topic-readiness probe failed BEFORE the earliest-replay cleanup ({e}). \
             NOTHING was dropped — the prior streaming segments are preserved \
             (fail-close, Codex R30 F2: a replay that cannot read the topic would \
             never rebuild them)",
            prepared.config.data_source, prepared.config.topic,
        ))),
        Ok(records) => Ok((prepared, records)),
    }
}

/// Prove the prepared consumer can actually be ADMITTED to its consumer
/// group — polling it briefly to surface any PERMANENT JoinGroup/config
/// rejection — BEFORE the caller runs the destructive earliest-replay drop
/// (Codex R35). Runs
/// [`ferrodruid_ingest_kafka::streaming::probe_group_joinable`] (async
/// `recv()` bounded by [`GROUP_JOIN_PROBE_TIMEOUT`]) on `&prepared.consumer`.
///
/// This closes the R35 loss chain that the R30 F2 metadata probe alone left
/// open: a locally-valid but broker-rejected consumer setting (e.g.
/// `session.timeout.ms` outside the broker's
/// `group.min/max.session.timeout.ms`) passes consumer creation AND
/// [`verify_topic_readable`] (the topic IS describable) yet is refused at
/// every JoinGroup. Without this step [`earliest_replay_cleanup`] would drop
/// the pair's prior segments, then the streaming loop would warn-retry the
/// un-healable JoinGroup forever and rebuild NOTHING — permanent loss.
///
/// The probe polls in a bounded loop and REFUSES (`Err`, prior segments kept,
/// consumer never starts) ONLY on a PERMANENT group/config rejection — which
/// is validated when the JoinGroup is received and so surfaces FAST, before
/// the broker's `group.initial.rebalance.delay.ms`. Otherwise it PROCEEDS
/// (`Ok`): on positive join evidence (a delivered record — seeked back inside
/// the probe so the reused consumer's loop re-reads it — or a non-empty
/// assignment), or on the deadline elapsing without a permanent rejection.
/// Proceeding on a silent deadline (Codex R35 review) is deliberate: a
/// permanent rejection would already have surfaced, so a silent deadline is
/// dominated by a healthy join still inside that broker-side delay (unknowable
/// to the client, and RESET every spawn since the group id is unique) —
/// fail-closing would permanently block earliest re-creates on clusters whose
/// delay exceeds the window. The narrow residual (a permanent failure emitting
/// no classifiable error in-window) stays non-silent via the poll loop's own
/// fatal classification once the consumer runs; FG-6/FG-7 durable segments are
/// the real fix.
///
/// Honest limitation (same as R30 F2 / R31): the real permanent-rejection
/// refuse (with its specific message) and the assignment-confirmed PROCEED
/// both need a real broker and are real-broker E2E residuals; the fatal
/// DECISION rule (a fatal poll error ⇒ refuse) is the shared
/// [`consume_error_is_fatal`](ferrodruid_ingest_kafka::streaming::consume_error_is_fatal)
/// classification unit-pinned in the ingest-kafka crate.
pub(crate) async fn prove_group_joinable(
    prepared: PreparedKafkaConsumer,
) -> Result<PreparedKafkaConsumer> {
    match ferrodruid_ingest_kafka::streaming::probe_group_joinable(
        &prepared.consumer,
        GROUP_JOIN_PROBE_TIMEOUT,
    )
    .await
    {
        Ok(()) => Ok(prepared),
        Err(e) => Err(DruidError::Ingestion(format!(
            "refusing to (re)start the consumer for datasource '{}' / topic '{}': a \
             PERMANENT group-join / config rejection surfaced BEFORE the earliest-replay \
             cleanup ({e}). NOTHING was dropped — the prior streaming segments are \
             preserved (fail-close, Codex R35: the broker will never admit this consumer \
             to its group as configured, so a replay could never rebuild the dropped rows). \
             Fix the rejected consumer setting and re-create the supervisor from earliest",
            prepared.config.data_source, prepared.config.topic,
        ))),
    }
}

/// Whether the empty-topic guard (Codex R31) must SUPPRESS the destructive
/// earliest-replay drop for a given [`TopicRecords`] probe verdict.
///
/// Only a POSITIVE emptiness sighting ([`TopicRecords::Empty`]) suppresses
/// it: an earliest replay of a topic that currently holds no records rebuilds
/// nothing, so dropping the pair's prior segments would be PERMANENT loss
/// (the trap when a topic is deleted+recreated — or its retention expires —
/// under the same name/cluster/schema, indistinguishable from the original
/// log without a topic UUID that librdkafka's consumer metadata does not
/// expose). [`TopicRecords::HasRecords`] lets the drop proceed (the replay
/// rebuilds them — the normal restart/resume case), and
/// [`TopicRecords::Unknown`] (watermarks unprovable) never suppresses on a
/// guess. Fail-safe throughout: suppression only ever ADDS a KEEP
/// (duplication over loss), never a drop.
fn empty_topic_suppresses_drop(records: TopicRecords) -> bool {
    matches!(records, TopicRecords::Empty)
}

/// The EARLIEST-replay pre-start cleanup, probe-gated (Codex R30 F2): the
/// single seam both `create_supervisor` and `resume_kafka_supervisors` run
/// when `useEarliestOffset` is set, inside their uncancellable lifecycle
/// ops.
///
/// Order:
///   1. **Read-only victim scan** ([`select_streaming_victims`] over the
///      current metadata): decides whether the cleanup would drop anything
///      at all, and raises the R26 F1 schema-change refusal early. No
///      victims → the destructive pass is a no-op, so the probe is
///      SKIPPED: a fresh supervisor keeps the deliberate lazy-connect
///      semantics (a temporarily unreachable broker does not refuse the
///      create; librdkafka retries in the background).
///   2. **Topic-readiness probe** ([`verify_topic_readable`]) — only when
///      step 1 found victims. Fail-close `Err` BEFORE anything is
///      destroyed: prior segments survive, the consumer never starts.
///   3. **Empty-topic guard** (Codex R31): the same probe reports whether
///      the source topic currently holds any replayable records. If it is
///      proven EMPTY ([`TopicRecords::Empty`]) the destructive drop is
///      SKIPPED and the prior segments are KEPT — a topic deleted+recreated
///      (or whose retention expired) under the same name/cluster/schema
///      would otherwise be dropped-then-not-rebuilt (permanent loss), and
///      without a topic UUID (unavailable in librdkafka's consumer
///      metadata — see [`TopicRecords`]) it is indistinguishable from the
///      original log. Fail-safe: keeping risks at worst DUPLICATION on the
///      earliest replay, a wrong drop is permanent loss. `HasRecords` /
///      `Unknown` proceed to the drop (never suppress on a guess).
///   4. **Group-join probe** ([`prove_group_joinable`], Codex R35) — only
///      when step 3 did NOT suppress the drop. Polls the prepared consumer in
///      a bounded loop to force a JoinGroup attempt while the prior segments
///      are still intact: a PERMANENT group/config rejection (a broker-rejected
///      `session.timeout.ms`, `group.id`, assignor, or API version — see
///      [`consume_error_is_fatal`](ferrodruid_ingest_kafka::streaming::consume_error_is_fatal))
///      passes creation + the metadata probe yet consumes NOTHING, so it
///      would let the drop destroy the prior segments and rebuild none. Such a
///      rejection surfaces FAST (before any initial-rebalance delay) and is
///      fail-close `Err` BEFORE the drop; a healthy or still-joining consumer
///      (no permanent rejection in-window) proceeds, so a broker whose
///      initial-rebalance delay exceeds the window is never permanently
///      blocked.
///   5. **Destructive drop** ([`drop_streaming_segments_task`]) — the
///      authoritative victim selection re-runs under the datasource
///      publish lock. Lifecycle ops are fully serialized (lifecycle lock +
///      registry drain), and the not-yet-started consumer publishes
///      nothing, so the step-1 scan cannot go stale in between for this
///      pair; other pairs' publishes never select into it.
pub(crate) async fn earliest_replay_cleanup(
    prepared: PreparedKafkaConsumer,
    metadata: &Arc<MetadataStore>,
    historical: &Arc<Historical>,
    cluster_id: Option<&str>,
) -> Result<PreparedKafkaConsumer> {
    let would_drop = !select_streaming_victims(
        metadata.get_all_segments().await?,
        prepared.data_source(),
        prepared.topic(),
        cluster_id,
        prepared.schema_fp(),
    )?
    .victims
    .is_empty();
    if !would_drop {
        // No victims: the destructive pass is a no-op, so the probe is
        // SKIPPED (unchanged lazy-connect semantics — a fresh supervisor is
        // not refused by a temporarily unreachable broker). Re-run the drop
        // task for parity/logging; it re-scans and drops nothing.
        drop_streaming_segments_task(
            metadata,
            historical,
            prepared.data_source(),
            prepared.topic(),
            cluster_id,
            prepared.schema_fp(),
        )
        .await?;
        return Ok(prepared);
    }
    // Would drop → prove the topic is readable FIRST (fail-close, R30 F2),
    // and learn whether it currently holds replayable records (R31).
    let (prepared, records) = verify_topic_readable(prepared).await?;
    if empty_topic_suppresses_drop(records) {
        // Empty-topic guard (Codex R31): the topic currently holds NO
        // replayable records but prior streaming segments for this pair
        // exist. An earliest replay would rebuild NOTHING, so dropping them
        // would be PERMANENT loss (deleted+recreated topic, or retention
        // expired past the data). KEEP them and start the consumer without
        // the destructive pass — duplication over loss.
        tracing::warn!(
            data_source = prepared.data_source(),
            topic = prepared.topic(),
            "the source topic currently holds NO replayable records (every partition \
             watermark is empty) yet prior streaming segments for this pair exist: the \
             earliest-replay cleanup is SKIPPED and those segments are KEPT (Codex R31 \
             — duplication over permanent loss). Without a topic UUID (not exposed by \
             librdkafka's consumer metadata) a deleted+recreated or retention-expired \
             topic cannot be told apart from the original log; an earliest replay of an \
             empty topic would rebuild nothing, so dropping would be irreversible. If \
             the topic was intentionally recreated, use a NEW datasource name or delete \
             these rows explicitly",
        );
        return Ok(prepared);
    }
    // About to DROP (has victims, topic not proven empty) → prove the
    // consumer can actually be admitted to its group FIRST (Codex R35): a
    // permanent JoinGroup/config rejection (e.g. a `session.timeout.ms`
    // outside the broker's group.min/max range) passes consumer creation AND
    // the metadata probe yet consumes NOTHING, so dropping the pair's prior
    // segments would lose them with no replay to rebuild them. Such a
    // rejection surfaces fast; the `?` then propagates that refusal BEFORE the
    // destructive drop, keeping the prior segments. A healthy/still-joining
    // consumer (no permanent rejection in-window) proceeds.
    let prepared = prove_group_joinable(prepared).await?;
    drop_streaming_segments_task(
        metadata,
        historical,
        prepared.data_source(),
        prepared.topic(),
        cluster_id,
        prepared.schema_fp(),
    )
    .await?;
    Ok(prepared)
}

/// Build + subscribe a Kafka consumer from an ALREADY-validated spec,
/// returning a [`PreparedKafkaConsumer`]. Fails FAST (`Err`) on a bad
/// librdkafka property / unroutable broker — before the caller persists or
/// acknowledges the supervisor — instead of a detached task dying
/// immediately and leaving a dead handle + a persisted spec.
pub(crate) fn prepare_kafka_consumer(
    supervisor_id: &str,
    parsed: KafkaSupervisorSpec,
) -> Result<PreparedKafkaConsumer> {
    // Fingerprint the ingestion schema BEFORE the spec is consumed by the
    // runtime: the pre-start drop and every published segment's provenance
    // need it (Codex R26 F1).
    let schema_fp = schema_fingerprint(&parsed);
    let runtime = KafkaSupervisorRuntime::new(supervisor_id.to_string(), parsed);
    let config = runtime.build_consumer_config();
    let consumer = create_stream_consumer(&config)
        .map_err(|e| DruidError::Ingestion(format!("kafka consumer init failed: {e}")))?;
    Ok(PreparedKafkaConsumer {
        consumer,
        config,
        supervisor_id: supervisor_id.to_string(),
        schema_fp,
    })
}

/// Start a [`PreparedKafkaConsumer`]'s poll loop and return its handle.
/// Infallible (the consumer already exists). `cluster_id` (from
/// [`resolve_cluster_id`]) and `topic_id` (the [`resolve_topic_id`] probe's
/// AGREED id — [`TopicIdProbe::into_agreed`]; a Disagreed/Unresolved probe
/// stamps nothing, Codex R7 H1 / R28 H1) are stamped into every published
/// segment's provenance when known.
pub(crate) fn start_prepared(
    prepared: PreparedKafkaConsumer,
    metadata: Arc<MetadataStore>,
    historical: Arc<Historical>,
    deep_storage: Option<Arc<dyn DeepStorage>>,
    cluster_id: Option<String>,
    topic_id: Option<String>,
    resume_frontier: ResumeFrontier,
) -> KafkaSupervisorHandle {
    let PreparedKafkaConsumer {
        consumer,
        config,
        supervisor_id,
        schema_fp,
    } = prepared;
    // Retained on the handle so a later re-create can be checked as a
    // same-schema, same-cluster recovery of a replay-required sentinel
    // (Codex R33).
    let handle_schema_fp = schema_fp.clone();
    let handle_brokers = config.brokers.clone();

    // Operator notice (compat-3 stage 2): published segments are DURABLE
    // (persisted to deep storage; reloaded at bootstrap) and the consumer
    // manual-commits offsets after each successful publish, so a restart
    // resumes PAST the durable frontier instead of replaying the whole topic.
    // Residual: at-least-once (a hard kill between publish and commit can
    // re-consume the last un-committed roll — bounded duplication, never
    // loss); exactly-once is stage 3.
    tracing::info!(
        supervisor_id = %supervisor_id,
        data_source = %config.data_source,
        use_earliest_offset = config.use_earliest_offset,
        resume_frontier_partitions = resume_frontier.partitions.len(),
        topic_recreated = resume_frontier.topic_recreated,
        "starting Kafka consumer: segments are durable + offsets are committed \
         after each persisted publish; a restart resumes past the durable frontier \
         (at-least-once)",
    );
    // The cluster id the sink STAMPS onto every published segment (resolved at
    // start). The poll loop also needs it to detect a live cluster-identity
    // DRIFT before publishing (Codex R37 F3): a broker hostname repointed to a
    // different cluster (which librdkafka follows on RECONNECT — a path the R30
    // F1 rebootstrap pin does not cover) would otherwise let cluster B's rows be
    // stamped with cluster A's id. Clone it for the loop; the sink takes the
    // original.
    let expected_cluster_id = cluster_id.clone();
    let sink = OverlordKafkaSink {
        metadata,
        historical,
        deep_storage,
        task_id: supervisor_id.clone(),
        topic: config.topic.clone(),
        bootstrap: config.brokers.clone(),
        cluster_id,
        topic_id,
        schema_fp,
    };
    let data_source = config.data_source.clone();
    let topic = config.topic.clone();
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
    // The task RETURNS its stats so `KafkaSupervisorHandle::shutdown` can
    // observe a failed final drain (Codex R26 F2) — or a sticky mid-stream
    // publish failure (Codex R27 F2) — instead of the failure dying in a
    // log line while the caller tombstones the supervisor.
    let handle = tokio::spawn(async move {
        let stats = run_streaming_loop(
            consumer,
            config,
            sink,
            expected_cluster_id,
            resume_frontier,
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
                cluster_id_drifted = stats.cluster_id_drifted,
                "kafka supervisor consumer stopped WITH LOST BUFFERED ROWS \
                 (mid-stream / final-drain publish failure — R26 F2 / R27 F2, a \
                 fatal consume error — R30 F2, and/or a cluster-identity drift — \
                 R37 F3): those rows never became queryable and only a Kafka replay \
                 (restart / earliest re-create) can rebuild them",
            );
        } else {
            tracing::info!(
                supervisor_id = %supervisor_id,
                consumed = stats.total_consumed,
                published = stats.total_published,
                "kafka supervisor consumer stopped",
            );
        }
        stats
    });
    KafkaSupervisorHandle {
        shutdown_tx,
        handle: Some(handle),
        replay_error: None,
        data_source,
        topic,
        schema_fp: handle_schema_fp,
        brokers: handle_brokers,
    }
}

// ---------------------------------------------------------------------------
// Tests (compiled only with `kafka-io` + `cfg(test)`; none need a broker —
// the real-broker end-to-end lives in `tests/kafka_supervisor_e2e.rs`).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use ferrodruid_ingest_batch::BatchIngester;
    use serde_json::json;
    use tokio::sync::Mutex;

    async fn setup() -> (Arc<MetadataStore>, Arc<Historical>, tempfile::TempDir) {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create store"));
        metadata.initialize().await.expect("init schema");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(Historical::new(cache_dir.path().to_path_buf(), 10_000_000));
        (metadata, historical, cache_dir)
    }

    /// Build an [`IngestedSegment`] from JSON rows via the same
    /// [`BatchIngester`] the streaming buffer uses.
    fn segment(ds: &str, rows: &[serde_json::Value]) -> IngestedSegment {
        let ingester = BatchIngester::new(
            ds.to_string(),
            "__time".to_string(),
            vec!["page".to_string()],
            vec![],
        );
        ingester.ingest(rows.to_vec()).expect("ingest rows")
    }

    /// Realistic epoch-millis base (2023-11-14) so rows land inside the
    /// 2000..2100 query window; `off` offsets a few ms within the segment.
    fn row(off: i64, page: &str) -> serde_json::Value {
        const BASE_MS: i64 = 1_700_000_000_000;
        json!({ "__time": BASE_MS + off, "page": page })
    }

    /// Shorthand for the provenance stamped by [`publish_streaming_segment`]
    /// in tests. `"v2:fp-test"` is the schema fingerprint used everywhere a
    /// test does not exercise the schema-change refusal (R26 F1); the topic
    /// id defaults to the resolved `"tid-1"` (tests exercising the R7 H1
    /// topic-generation matrix use [`prov_tid`]).
    fn prov<'a>(
        task_id: &'a str,
        topic: &'a str,
        bootstrap: &'a str,
        cluster_id: Option<&'a str>,
        schema_fp: &'a str,
    ) -> StreamingProvenance<'a> {
        prov_tid(
            task_id,
            topic,
            bootstrap,
            cluster_id,
            Some("tid-1"),
            schema_fp,
        )
    }

    /// [`prov`] with full control over the stamped KIP-516 topic id (Codex
    /// R7 H1).
    fn prov_tid<'a>(
        task_id: &'a str,
        topic: &'a str,
        bootstrap: &'a str,
        cluster_id: Option<&'a str>,
        topic_id: Option<&'a str>,
        schema_fp: &'a str,
    ) -> StreamingProvenance<'a> {
        StreamingProvenance {
            task_id,
            topic,
            bootstrap,
            cluster_id,
            topic_id,
            schema_fp,
        }
    }

    /// Build a synthetic streaming metadata row for the resume-frontier /
    /// durable-exclusion tests (compat-3 stage 2). `offsets` is a list of
    /// `(partition, start, next)`; `None` produces a LEGACY (non-durable)
    /// row without `kafkaOffsets`. The stamped topic id defaults to
    /// `"tid-1"` — the current-generation id the frontier tests resolve —
    /// so the cluster-identity matrix keeps deciding those tests; the R7 H1
    /// topic-generation matrix uses [`streaming_row_gen`].
    fn streaming_row(
        id: &str,
        ds: &str,
        topic: &str,
        cluster: Option<&str>,
        schema_fp: &str,
        offsets: Option<&[(i32, i64, i64)]>,
    ) -> SegmentMetadataRow {
        streaming_row_gen(id, ds, topic, cluster, Some("tid-1"), schema_fp, offsets)
    }

    /// [`streaming_row`] with full control over the stamped KIP-516 topic
    /// id (`None` = an unstamped/pre-R7 row).
    fn streaming_row_gen(
        id: &str,
        ds: &str,
        topic: &str,
        cluster: Option<&str>,
        topic_id: Option<&str>,
        schema_fp: &str,
        offsets: Option<&[(i32, i64, i64)]>,
    ) -> SegmentMetadataRow {
        let mut payload = json!({
            "dataSource": ds,
            "numRows": 1,
            "kind": "kafka-streaming",
            "topic": topic,
            "schemaFp": schema_fp,
        });
        if let Some(c) = cluster {
            payload["clusterId"] = json!(c);
        }
        if let Some(t) = topic_id {
            payload["topicId"] = json!(t);
        }
        if let Some(offs) = offsets {
            let mut ko = serde_json::Map::new();
            for (p, s, n) in offs {
                ko.insert(p.to_string(), json!({ "start": s, "next": n }));
            }
            payload["kafkaOffsets"] = serde_json::Value::Object(ko);
        }
        SegmentMetadataRow {
            id: id.to_string(),
            data_source: ds.to_string(),
            created_date: "2026-01-01T00:00:00.000Z".to_string(),
            start: "2023-11-14T00:00:00.000Z".to_string(),
            end: "2023-11-15T00:00:00.000Z".to_string(),
            version: "v1".to_string(),
            used: true,
            payload,
        }
    }

    #[test]
    fn resume_frontier_takes_max_next_per_partition_same_cluster() {
        use ferrodruid_ingest_kafka::streaming::resume_offset;
        // With every row LOADED and contiguous, the reconciled resume offset is
        // max(next) per partition over the durable rows of the SAME pair on the
        // SAME cluster; other clusters / other topics / rows without
        // kafkaOffsets contribute nothing.
        let rows = vec![
            streaming_row(
                "s0",
                "events",
                "t",
                Some("kc-A"),
                "v2:fp",
                Some(&[(0, 0, 250)]),
            ),
            streaming_row(
                "s1",
                "events",
                "t",
                Some("kc-A"),
                "v2:fp",
                Some(&[(0, 250, 500), (1, 0, 30)]),
            ),
            // A different cluster: excluded (its replay could never rebuild here).
            streaming_row(
                "s2",
                "events",
                "t",
                Some("kc-B"),
                "v2:fp",
                Some(&[(0, 0, 9999)]),
            ),
            // A different topic: excluded.
            streaming_row(
                "s3",
                "events",
                "u",
                Some("kc-A"),
                "v2:fp",
                Some(&[(0, 0, 9999)]),
            ),
            // A legacy non-durable row (no kafkaOffsets): contributes nothing.
            streaming_row("s4", "events", "t", Some("kc-A"), "v2:fp", None),
        ];
        let f =
            compute_resume_frontier(&rows, "events", "t", Some("kc-A"), Some("tid-1"), |_| true);
        assert_eq!(f.len(), 2);
        // All spans loaded + contiguous from 0 → resume reconciles to max(next).
        assert_eq!(resume_offset(Some(0), f.get(&0).expect("p0")), 500);
        assert_eq!(resume_offset(Some(0), f.get(&1).expect("p1")), 30);
    }

    #[test]
    fn resume_frontier_phantom_row_pulls_resume_below_the_hole() {
        use ferrodruid_ingest_kafka::streaming::resume_offset;
        // C2: partition 0 has TWO durable rows, [0,100) and [100,200), and a
        // prior session committed 200. If the [0,100) blob is PHANTOM (not
        // reloaded), the resume must NOT skip to 200 (that would lose 0-99) —
        // min_start (0) floors it and the hole at [0,100) stops the walk, so
        // the reconciled resume is 0 (re-consume everything, no loss).
        let rows = vec![
            streaming_row(
                "lo",
                "events",
                "t",
                Some("kc-A"),
                "v2:fp",
                Some(&[(0, 0, 100)]),
            ),
            streaming_row(
                "hi",
                "events",
                "t",
                Some("kc-A"),
                "v2:fp",
                Some(&[(0, 100, 200)]),
            ),
        ];
        // "lo" phantom (blob missing), "hi" loaded.
        let f = compute_resume_frontier(&rows, "events", "t", Some("kc-A"), Some("tid-1"), |id| {
            id == "hi"
        });
        let p0 = f.get(&0).expect("p0");
        assert_eq!(p0.min_start, 0, "phantom start floors the resume");
        assert_eq!(resume_offset(Some(200), p0), 0, "no skip past the hole");

        // When BOTH rows are loaded + contiguous, the committed 200 reconciles
        // to 200 (nothing to re-consume).
        let f2 =
            compute_resume_frontier(&rows, "events", "t", Some("kc-A"), Some("tid-1"), |_| true);
        assert_eq!(resume_offset(Some(200), f2.get(&0).expect("p0")), 200);
    }

    // NOTE (Codex R4 H4 → R5 H2): the R3-C1 test `resume_frontier_survives_
    // unknown_cluster_identity` asserted that UNKNOWN identities are INCLUDED
    // in the frontier; R4 H4 swung to EXCLUDING them entirely; R5 H2 settles
    // on the split: unknown identities FLOOR the resume (loss prevention,
    // phantom recovery) but never ADVANCE it (no unproven skip) — see
    // `resume_frontier_requires_definite_cluster_match_to_advance` and
    // `resume_frontier_unknown_identity_floors_but_never_advances` for the
    // full matrix. The R3-C1 loss concern (a suppressed frontier stranding a
    // durable partition on `auto.offset.reset`) is answered by the stamp
    // invariant plus the H2 floor, never by trusting unknown offsets for a
    // skip.

    #[test]
    fn resume_frontier_excludes_definite_cluster_mismatch() {
        // A DEFINITE mismatch (both identities known and different) excludes
        // a row from BOTH roles — its offsets live in another cluster's
        // offset space, so folding them into THIS cluster's frontier (floor
        // or advance) would anchor the seek to a meaningless position.
        // (Non-definite pairings floor-but-never-advance since R5 H2 — see
        // `resume_frontier_requires_definite_cluster_match_to_advance`.)
        let rows = vec![streaming_row(
            "s0",
            "events",
            "t",
            Some("kc-B"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        )];
        assert!(
            compute_resume_frontier(&rows, "events", "t", Some("kc-A"), Some("tid-1"), |_| true)
                .is_empty(),
            "a known-different cluster's rows must stay excluded"
        );
    }

    #[test]
    fn resume_frontier_excludes_disabled_rows() {
        use ferrodruid_ingest_kafka::streaming::resume_offset;
        // Codex R3 H5: administratively DISABLED (used=false) durable rows
        // must not enter the resume frontier. Pre-fix, a disabled row's span
        // was folded in; because disabled rows are not reloaded at bootstrap,
        // its span became an unloaded hole that forced the resume BELOW it —
        // replaying and RE-PUBLISHING data an operator had explicitly removed.
        let mut disabled = streaming_row(
            "dis",
            "events",
            "t",
            Some("kc-A"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        disabled.used = false;
        let used_row = streaming_row(
            "use",
            "events",
            "t",
            Some("kc-A"),
            "v2:fp",
            Some(&[(0, 500, 600)]),
        );
        let f = compute_resume_frontier(
            &[disabled.clone(), used_row],
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |id| id == "use",
        );
        let p0 = f.get(&0).expect("p0");
        assert_eq!(
            p0.min_start, 500,
            "a disabled row must not floor the resume below the used rows (H5)"
        );
        assert_eq!(
            resume_offset(None, p0),
            600,
            "the disabled [0,500) span must not force a replay that re-publishes \
             administratively-removed data (H5)"
        );
        // Only disabled rows → NO frontier at all (first-start semantics).
        let f = compute_resume_frontier(
            &[disabled],
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| false,
        );
        assert!(
            f.is_empty(),
            "disabled rows alone must not create a frontier (H5)"
        );
    }

    #[test]
    fn resume_frontier_requires_definite_cluster_match_to_advance() {
        use ferrodruid_ingest_kafka::streaming::resume_offset;
        // Codex R4 H4 (refined by R5 H2): a durable row's span may ADVANCE
        // the resume (be skipped past) ONLY on a DEFINITE cluster-identity
        // match. Every non-definite pairing never advances: seeking THIS
        // consumer's partitions past offsets from an unconfirmed offset space
        // (a bootstrap name repointed from cluster A to cluster B) would skip
        // cluster B's distinct records permanently. Since R5 H2 the
        // non-definite pairings still FLOOR the resume (loss prevention) —
        // they anchor the seek at/below their data instead of vanishing from
        // the frontier. Floor-only rows stay queryable and are never dropped.
        let stamped = vec![streaming_row(
            "s0",
            "events",
            "t",
            Some("kc-A"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        )];
        // Definite match → floor + advance.
        let f =
            compute_resume_frontier(&stamped, "events", "t", Some("kc-A"), Some("tid-1"), |_| {
                true
            });
        assert_eq!(
            f.len(),
            1,
            "a definite cluster-identity match seeds the frontier"
        );
        assert_eq!(
            resume_offset(Some(0), f.get(&0).expect("p0")),
            500,
            "a definite match ADVANCES the resume past its loaded span"
        );
        // Stamped row, but the CURRENT identity is unresolved → the match
        // cannot be confirmed: the span must not be skipped past (H4), but
        // it still floors the resume (R5 H2).
        let f = compute_resume_frontier(&stamped, "events", "t", None, Some("tid-1"), |_| true);
        let p0 = f.get(&0).expect("floor survives an unresolved identity");
        assert!(
            p0.loaded.is_empty(),
            "an unresolved current identity must not be treated as a match (H4)"
        );
        assert_eq!(
            resume_offset(None, p0),
            0,
            "the unconfirmed span is re-consumed, never skipped"
        );
        // UNSTAMPED (legacy pre-H4) row with kafkaOffsets: the publish path
        // now always stamps clusterId alongside kafkaOffsets, so an unstamped
        // durable row is legacy residue whose origin cluster is unknowable —
        // never authoritative for a skip (H4), still a floor (R5 H2).
        let unstamped = vec![streaming_row(
            "s0",
            "events",
            "t",
            None,
            "v2:fp",
            Some(&[(0, 0, 500)]),
        )];
        let f = compute_resume_frontier(
            &unstamped,
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        let p0 = f.get(&0).expect("floor survives an unstamped row");
        assert!(
            p0.loaded.is_empty(),
            "a legacy unstamped durable row must not advance the resume (H4)"
        );
        // Both identities unknown → still never a definite match: floor only.
        let f = compute_resume_frontier(&unstamped, "events", "t", None, Some("tid-1"), |_| true);
        assert!(
            f.get(&0).expect("p0").loaded.is_empty(),
            "two unknown identities are never a definite match (H4)"
        );
    }

    #[test]
    fn resume_frontier_unknown_identity_floors_but_never_advances() {
        use ferrodruid_ingest_kafka::streaming::resume_offset;
        // Codex R5 H2: R4 H4 excluded unresolved/unstamped rows from the
        // frontier ENTIRELY — but a durable row whose blob is MISSING (a
        // phantom, R1 C2) must still pull the resume BELOW its hole even when
        // its cluster identity is unknowable, or the committed offset (already
        // past the span) is trusted and the missing data is skipped forever
        // (loss). The span's two roles separate: FLOOR (min_start — never
        // skip below it) applies to every durable row except a DEFINITE
        // mismatch; ADVANCE (skip-past of loaded coverage) stays
        // definite-match-only (the H4 guarantee).

        // (1) Stamped row, CURRENT identity unresolved, blob PHANTOM,
        //     committed already past the span: the floor must force a
        //     re-consume from the hole (0), not trust committed (600).
        let stamped = vec![streaming_row(
            "s0",
            "events",
            "t",
            Some("kc-A"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        )];
        let f = compute_resume_frontier(&stamped, "events", "t", None, Some("tid-1"), |_| false);
        let p0 = f
            .get(&0)
            .expect("an unknown-identity durable row must FLOOR the frontier (H2)");
        assert_eq!(p0.min_start, 0, "the phantom's start floors the resume");
        assert!(
            p0.loaded.is_empty(),
            "an unproven identity must never seed skippable coverage (H4)"
        );
        assert_eq!(
            resume_offset(Some(600), p0),
            0,
            "committed past the phantom span must NOT be trusted — re-consume \
             from the floor (no loss, H2)"
        );

        // (2) Even when the row's blob IS loaded, an unknown identity never
        //     ADVANCES the resume (its origin offset space is unproven —
        //     re-consume is bounded duplication, skip-past could be loss).
        let f = compute_resume_frontier(&stamped, "events", "t", None, Some("tid-1"), |_| true);
        let p0 = f.get(&0).expect("p0");
        assert!(
            p0.loaded.is_empty(),
            "loaded-but-unproven rows floor, never advance"
        );
        assert_eq!(resume_offset(Some(600), p0), 0);

        // (3) UNSTAMPED legacy row (kafkaOffsets, no clusterId): same
        //     floor-only treatment, whichever side is unknown.
        let unstamped = vec![streaming_row(
            "s0",
            "events",
            "t",
            None,
            "v2:fp",
            Some(&[(0, 0, 500)]),
        )];
        let f = compute_resume_frontier(
            &unstamped,
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        let p0 = f
            .get(&0)
            .expect("an unstamped legacy durable row must floor the frontier (H2)");
        assert!(p0.loaded.is_empty());
        assert_eq!(resume_offset(Some(600), p0), 0);
        let f = compute_resume_frontier(&unstamped, "events", "t", None, Some("tid-1"), |_| true);
        assert_eq!(
            resume_offset(Some(600), f.get(&0).expect("p0")),
            0,
            "both identities unknown still floors (no loss)"
        );

        // (4) DEFINITE mismatch: NEITHER floor nor advance — another
        //     cluster's offsets are meaningless in this offset space.
        let mismatch = vec![streaming_row(
            "s0",
            "events",
            "t",
            Some("kc-B"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        )];
        assert!(
            compute_resume_frontier(
                &mismatch,
                "events",
                "t",
                Some("kc-A"),
                Some("tid-1"),
                |_| true
            )
            .is_empty(),
            "a definite mismatch must not floor the resume (its offsets are \
             another cluster's)"
        );

        // (5) Mixed: a definite-match loaded span advances the walk; an
        //     unproven span above it stops the walk (gated re-consume), and
        //     the committed offset past BOTH is never trusted.
        let mixed = vec![
            streaming_row(
                "match",
                "events",
                "t",
                Some("kc-A"),
                "v2:fp",
                Some(&[(0, 0, 500)]),
            ),
            streaming_row(
                "legacy",
                "events",
                "t",
                None,
                "v2:fp",
                Some(&[(0, 500, 800)]),
            ),
        ];
        let f =
            compute_resume_frontier(&mixed, "events", "t", Some("kc-A"), Some("tid-1"), |_| true);
        let p0 = f.get(&0).expect("p0");
        assert_eq!(p0.min_start, 0);
        assert_eq!(
            resume_offset(Some(800), p0),
            500,
            "advance through the definite-match coverage, then STOP before \
             the unproven span — re-consume [500,800) instead of trusting \
             committed (H2)"
        );
    }

    #[test]
    fn resume_frontier_detects_topic_recreation_and_reconsumes_from_low() {
        use ferrodruid_ingest_kafka::streaming::{clamp_resume_target, resume_offset};
        // Codex R7 H1: durable row [0,500) was published under the ORIGINAL
        // topic generation (topicId "tid-old"); the topic is then DELETED and
        // RECREATED on the SAME cluster (current topicId "tid-new") and >500
        // records are produced, so the new watermark window is [0, 800) — the
        // resume target 500 is IN-RANGE and the R5-H1 clamp alone cannot
        // catch the recreation. Offset 500 of the NEW generation names a
        // DIFFERENT record than the durable rows cover (KIP-516 offset
        // reuse), so seeking 500 would permanently skip the new topic's
        // [0,500) — loss. The topic-id mismatch must detect the recreation:
        // the dead generation's rows are EXCLUDED, and the partition — left
        // with no live evidence — is floored at 0 (never advances), so the
        // reconciled + clamped seek re-consumes the retained log from `low`
        // (R7 H1, dead-only scoping per R8 H1).
        let rows = vec![streaming_row_gen(
            "s0",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-old"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        )];
        let f =
            compute_resume_frontier(&rows, "events", "t", Some("kc-A"), Some("tid-new"), |_| {
                true
            });
        let p0 = f
            .get(&0)
            .expect("recreated-generation rows must still gate the resume");
        assert!(
            p0.loaded.is_empty(),
            "a prior-generation span must never ADVANCE the resume — its \
             offsets name different records in the recreated topic"
        );
        assert_eq!(
            p0.min_start, 0,
            "a dead-only partition must floor at 0, not at the stale \
             generation's own start (R8 H1: the dead rows themselves are \
             excluded; the floor-0 comes from the dead-only low-seek)"
        );
        let target = resume_offset(Some(500), p0);
        assert_eq!(
            clamp_resume_target(target, 0, 800),
            0,
            "an in-range post-recreation resume must re-consume from low \
             (seeking the durable frontier would skip the new topic's \
             [0,500) records = permanent loss)"
        );
        // Mixed generations: rows re-published AFTER the recreation (stamped
        // with the CURRENT id) govern the partition alone — the dead
        // generation's rows are excluded (R8 H1) — so nothing below the live
        // contiguous coverage is skipped, and nothing above it is.
        let mixed = vec![
            streaming_row_gen(
                "old-gen",
                "events",
                "t",
                Some("kc-A"),
                Some("tid-old"),
                "v2:fp",
                Some(&[(0, 0, 500)]),
            ),
            streaming_row_gen(
                "new-gen",
                "events",
                "t",
                Some("kc-A"),
                Some("tid-new"),
                "v2:fp",
                Some(&[(0, 0, 300)]),
            ),
        ];
        let f =
            compute_resume_frontier(&mixed, "events", "t", Some("kc-A"), Some("tid-new"), |_| {
                true
            });
        assert_eq!(
            resume_offset(Some(500), f.get(&0).expect("p0")),
            300,
            "the new generation's own durable coverage advances from the \
             recreation floor; the old generation's span is ignored"
        );
    }

    #[test]
    fn topic_recreation_detected_from_disabled_rows() {
        // Codex R18 C1: every durable row of the pair is administratively
        // DISABLED (used=false), then the topic is deleted+recreated (same
        // name, same cluster) and the process restarts. Pre-R18 the frontier
        // derivation dropped disabled rows before the topic-id check, so the
        // resume saw an EMPTY frontier, `apply_resume_seek` no-op'd, and
        // every partition resumed from the DEAD generation's stale committed
        // offset — silently skipping every new-generation record below it
        // (loss). The disabled rows still carry the dead topicId: the
        // topic-level guard must surface it, and the consumer must floor
        // EVERY assigned partition at the retained log's `low` watermark.
        use ferrodruid_ingest_kafka::streaming::{recreated_resume_target, resume_offset};
        let mut disabled = streaming_row_gen(
            "dead-dis",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-old"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        disabled.used = false;
        let mut f = derive_resume_frontier(
            &[disabled],
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-new".to_string()),
            |_| false,
        );
        assert!(
            f.topic_recreated,
            "recreation evidence carried ONLY by disabled rows must still \
             reach the resume path — an unflagged empty frontier resumes \
             every partition from the dead generation's committed offset (loss)"
        );
        assert!(
            f.partitions.is_empty(),
            "disabled rows still never enter the NUMERIC frontier (R3 H5): \
             their spans must not force a replay of administratively-removed \
             data — the recreation verdict rides the topic-level flag instead"
        );
        // Streaming side: the consumer synthesizes a floor-low recreation
        // entry for every assigned partition (here 0 and 1 — note partition
        // 1 never had ANY durable row). The dead generation's stale-high
        // committed offset (500) never gates the resume; the seek-time
        // derivation lands on the retained log's `low`.
        assert_eq!(f.synthesize_recreated([0, 1]), 2);
        for p in [0, 1] {
            let resume = f.partitions.get(&p).expect("synthesized entry");
            assert!(resume.recreated);
            assert_eq!(
                resume_offset(Some(500), resume),
                0,
                "the dead generation's committed offset must never gate the walk"
            );
            assert_eq!(
                recreated_resume_target(120, 900, resume),
                120,
                "the partition re-consumes the retained log from `low` — \
                 records are re-consumed (bounded duplication), never skipped"
            );
        }
    }

    #[test]
    fn topic_recreation_offsetless_row_needs_confirmed_cluster() {
        // Codex R18 C2, refined by R26 H1: an offsetless durable row
        // (published while the cluster identity was transiently unresolved —
        // the R4 H4 invariant omits kafkaOffsets, and with an unresolved id
        // ALSO the clusterId) carries a topicId but no kafkaOffsets, and the
        // consumer still manual-committed its offsets. Whether its topicId
        // mismatch is recreation evidence depends on the row's OWN clusterId:
        //
        //  * clusterId ABSENT → AMBIGUOUS (a same-cluster recreation and a
        //    bootstrap repoint are indistinguishable from it, R26 H1) → NOT
        //    evidence; a genuine recreation whose only witness is such a row
        //    is a cluster-id-unresolved DEGRADED-MODE residual (never on a
        //    healthy PLAINTEXT / 2.8+ broker);
        //  * clusterId PRESENT and == current → SAME cluster confirmed → the
        //    topicId mismatch fires (genuine recreation, even without
        //    kafkaOffsets); the partition, absent from the numeric frontier,
        //    is low-floored by the consumer's synthesis so its dead committed
        //    offset is never trusted.
        use ferrodruid_ingest_kafka::streaming::{recreated_resume_target, resume_offset};

        // clusterId ABSENT → does NOT fire (R26 H1 degraded-mode residual).
        let ambiguous = streaming_row_gen(
            "no-cid",
            "events",
            "t",
            None,
            Some("tid-old"),
            "v2:fp",
            None,
        );
        let f = derive_resume_frontier(
            std::slice::from_ref(&ambiguous),
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-new".to_string()),
            |_| true,
        );
        assert!(
            !f.topic_recreated,
            "a clusterId-ABSENT offsetless row is ambiguous (recreation vs \
             bootstrap repoint) and must NOT fire the guard — the honest \
             cluster-id-unresolved degraded-mode residual (Codex R26 H1)"
        );
        assert!(f.partitions.is_empty());

        // clusterId PRESENT and confirming the same cluster → fires (genuine
        // recreation evidence even without kafkaOffsets).
        let confirmed = streaming_row_gen(
            "cid-old",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-old"),
            "v2:fp",
            None,
        );
        let mut f = derive_resume_frontier(
            std::slice::from_ref(&confirmed),
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-new".to_string()),
            |_| true,
        );
        assert!(
            f.topic_recreated,
            "a same-cluster-CONFIRMED offsetless row's topicId mismatch is \
             genuine recreation evidence (Codex R18 C2 preserved for the \
             non-ambiguous case)"
        );
        assert!(
            f.partitions.is_empty(),
            "an offset-less row names no partition — the numeric frontier \
             stays empty; the verdict rides the topic-level flag"
        );
        // The partition the row's records actually came from is only known
        // at assignment time: the synthesized entry floors it at `low`.
        assert_eq!(f.synthesize_recreated([0]), 1);
        let resume = f.partitions.get(&0).expect("synthesized entry");
        assert_eq!(resume_offset(Some(700), resume), 0);
        assert_eq!(recreated_resume_target(50, 800, resume), 50);
    }

    #[test]
    fn topic_recreation_ignores_clusterid_absent_ambiguous_mismatch() {
        // Codex R26 H1 (the RED repro): a bootstrap REPOINT A→B where the
        // cluster id was transiently unresolved at publish time on BOTH
        // clusters, so each cluster published an offsetless durable row with
        // the clusterId OMITTED (R4 H4 omits clusterId + kafkaOffsets
        // together) but the topicId stamped. B's Kafka commits landed. On
        // restart B's cluster id resolves (current cluster_id = Some("kc-B"),
        // topic_id = Some("tid-B")).
        //
        // Row A: clusterId ABSENT, topicId "tid-A". Row B: clusterId ABSENT,
        // topicId "tid-B". Pre-fix the foreign-skip only skipped rows whose
        // clusterId was PRESENT-and-different, so row A's ABSENT clusterId
        // slipped through and its "tid-A" != "tid-B" mismatch was counted as
        // recreation evidence → topic_recreated=true (WRONG): every B
        // partition floored to `low`, re-consuming and permanently
        // double-counting B's bootstrap-loaded, already committed records (no
        // lost async commit required — strictly more reachable than R25).
        let row_a = streaming_row_gen("a", "events", "t", None, Some("tid-A"), "v2:fp", None);
        let row_b = streaming_row_gen("b", "events", "t", None, Some("tid-B"), "v2:fp", None);
        assert!(
            !detect_topic_recreation(&[row_a, row_b], "events", "t", Some("kc-B"), Some("tid-B"),),
            "a clusterId-ABSENT row's topicId mismatch is ambiguous (a \
             same-cluster recreation vs a bootstrap repoint) and must not fire \
             the recreation guard — firing double-counts a repointed cluster's \
             already-committed records (Codex R26 H1)"
        );
    }

    #[test]
    fn topic_recreation_flags_live_frontier_partitions_too() {
        // Codex R18 (generalization): when the recreation evidence lives in
        // a DISABLED row, a partition that ALSO has live (current-generation)
        // durable coverage must still be recreation-flagged — the group's
        // last landed commit may still be the dead generation's stale-high
        // value, and the pre-R18 `min(committed, min_start)` floor could sit
        // above a never-durable [low, live_start) publish gap (the R9 C2
        // shape). The unified model floors at `low` and advances only
        // through live coverage contiguous from `low`.
        use ferrodruid_ingest_kafka::streaming::recreated_resume_target;
        let mut disabled = streaming_row_gen(
            "dead-dis",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-old"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        disabled.used = false;
        let live = streaming_row_gen(
            "new-gen",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-new"),
            "v2:fp",
            Some(&[(0, 400, 800)]),
        );
        let f = derive_resume_frontier(
            &[disabled, live],
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-new".to_string()),
            |_| true,
        );
        assert!(f.topic_recreated);
        let p0 = f.partitions.get(&0).expect("live evidence keeps the entry");
        assert!(
            p0.recreated,
            "topic-level recreation must flag EVERY frontier entry — a live \
             row must not leave its partition trusting the dead committed offset"
        );
        assert_eq!(
            recreated_resume_target(300, 800, p0),
            300,
            "the [300,400) publish gap below the live coverage is re-consumed \
             from `low`, never skipped (R9 C2 semantics under the R18 guard)"
        );
        assert_eq!(
            recreated_resume_target(400, 800, p0),
            800,
            "live coverage contiguous from `low` is still skipped — no \
             per-restart re-publish (R8 H1 preserved)"
        );
    }

    #[test]
    fn topic_recreation_guard_never_fires_without_definite_mismatch() {
        // R18 regressions: the guard fires ONLY on a DEFINITE topic-id
        // mismatch — the normal path stays byte-identical (no spurious
        // low re-consume).
        let mut disabled_match = streaming_row_gen(
            "dis-match",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        disabled_match.used = false;
        // Matching topicId → no recreation, and disabled rows alone still
        // mean NO frontier (first-start semantics, R3 H5).
        let f = derive_resume_frontier(
            &[disabled_match.clone()],
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-1".to_string()),
            |_| false,
        );
        assert!(!f.topic_recreated);
        assert!(f.partitions.is_empty());

        // Current topic id UNRESOLVABLE (None): detection is unavailable —
        // the documented R8 H2 residual — even with a dead-looking stamp.
        let mut disabled_old = streaming_row_gen(
            "dis-old",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-old"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        disabled_old.used = false;
        let f = derive_resume_frontier(
            &[disabled_old.clone()],
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Unresolved,
            |_| false,
        );
        assert!(
            !f.topic_recreated,
            "an unresolvable current topic id detects nothing (honest \
             residual, R8 H2): the guard needs BOTH ids present"
        );

        // R24 H1: current CLUSTER id UNRESOLVABLE (None) with a resolved topic
        // id and a cluster-STAMPED row whose topicId differs. This is exactly a
        // bootstrap A→B REPOINT read while the new cluster's id cannot be
        // resolved — the old cluster's rows have a different topicId, but that
        // is a foreign cluster, NOT a recreation of ours. Firing here would
        // flood every partition to `low` on every restart = permanent
        // double-count. Detection must stay silent without a resolved current
        // cluster id.
        let mut foreign_unresolved = streaming_row_gen(
            "repoint-A",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-old"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        foreign_unresolved.used = false;
        let f = derive_resume_frontier(
            &[foreign_unresolved],
            "events",
            "t",
            None, // current cluster id UNRESOLVED (repeated resolution failure)
            &TopicIdProbe::Agreed("tid-new".to_string()),
            |_| false,
        );
        assert!(
            !f.topic_recreated,
            "an unresolvable current cluster id cannot tell a recreation from \
             an A→B repoint — the guard must not fire (R24 H1)"
        );

        // A definite FOREIGN-cluster row's different topicId is that
        // cluster's topic, not a recreation of ours.
        let mut foreign = streaming_row_gen(
            "foreign-dis",
            "events",
            "t",
            Some("kc-B"),
            Some("tid-old"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        foreign.used = false;
        let f = derive_resume_frontier(
            &[foreign],
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-new".to_string()),
            |_| false,
        );
        assert!(
            !f.topic_recreated,
            "another cluster's rows must not fire the recreation guard"
        );

        // A MALFORMED topicId (present but null / empty) is a corrupt stamp,
        // never a mismatch (R16 H2).
        let mut malformed = streaming_row_gen(
            "mal",
            "events",
            "t",
            Some("kc-A"),
            None,
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        malformed.payload["topicId"] = serde_json::Value::Null;
        malformed.used = false;
        let mut malformed_empty =
            streaming_row_gen("mal2", "events", "t", Some("kc-A"), None, "v2:fp", None);
        malformed_empty.payload["topicId"] = json!("");
        let f = derive_resume_frontier(
            &[malformed, malformed_empty],
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-new".to_string()),
            |_| false,
        );
        assert!(
            !f.topic_recreated,
            "malformed topicId stamps are floor-only corrupt data, never \
             recreation evidence (R16 H2)"
        );

        // Other datasource / other topic / non-streaming rows: no evidence.
        let mut other_topic = streaming_row_gen(
            "other-topic",
            "events",
            "u",
            Some("kc-A"),
            Some("tid-old"),
            "v2:fp",
            None,
        );
        other_topic.used = false;
        let mut other_ds = streaming_row_gen(
            "other-ds",
            "clicks",
            "t",
            Some("kc-A"),
            Some("tid-old"),
            "v2:fp",
            None,
        );
        other_ds.used = false;
        let f = derive_resume_frontier(
            &[other_topic, other_ds],
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-new".to_string()),
            |_| false,
        );
        assert!(!f.topic_recreated);

        // MATCH on a normal live pair: derive == compute, nothing flagged
        // (the normal restart path is untouched).
        let live = streaming_row(
            "live",
            "events",
            "t",
            Some("kc-A"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        let f = derive_resume_frontier(
            std::slice::from_ref(&live),
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-1".to_string()),
            |_| true,
        );
        assert!(!f.topic_recreated);
        assert_eq!(
            f.partitions,
            compute_resume_frontier(&[live], "events", "t", Some("kc-A"), Some("tid-1"), |_| {
                true
            }),
            "without a detected recreation the derived frontier is exactly \
             the numeric frontier — no spurious recreation flags"
        );
        assert!(!f.partitions.get(&0).expect("p0").recreated);
        // And synthesis is inert on the normal path.
        let mut f = f;
        assert_eq!(
            f.synthesize_recreated([0, 7]),
            0,
            "no synthesized entries without a detected recreation — a \
             frontier-less partition keeps first-start semantics"
        );
        assert!(!f.partitions.contains_key(&7));
    }

    #[test]
    fn topic_id_disagreement_floors_resume_no_skip() {
        // Codex R28 H1 (the GREEN half of the RED→GREEN pair; the probe-side
        // RED lives in ferrodruid-ingest-kafka
        // `topic_id::tests::fetch_topic_id_broker_disagreement_is_never_a_definite_id`):
        // a topic is deleted+recreated and the process restarts inside the
        // metadata propagation window — a lagging broker still serves the
        // OLD generation's topic id, a fresh one the NEW id.
        use ferrodruid_ingest_kafka::streaming::{recreated_resume_target, resume_offset};
        // Durable rows of the pair: cluster-confirmed, stamped with the OLD
        // generation's topicId, coverage [0,500) on partition 0, blob loaded.
        let rows = [streaming_row_gen(
            "dur",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-old"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        )];

        // The pre-R28 loss shape this guard closes: the probe trusted the
        // FIRST responder, so a lagging broker resolved `tid-old` — a
        // definite MATCH against the durable rows' stamp. No recreation is
        // detected and the frontier advances to 500; on a recreated log
        // regrown past the stale frontier that seek skips the NEW
        // generation's records [0,500) forever.
        let trusted = derive_resume_frontier(
            &rows,
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-old".to_string()),
            |_| true,
        );
        assert!(!trusted.topic_recreated);
        assert_eq!(
            resume_offset(Some(500), trusted.partitions.get(&0).expect("p0")),
            500,
            "a stale unanimously-Agreed id still advances (the honest R28 \
             residual: agreement among lagging responders is undetectable) — \
             this pins the loss shape the DISAGREEMENT verdict must prevent"
        );

        // R28: the probe SEES the conflict → Disagreed → recreation-
        // SUSPECTED. Committed offsets and durable coverage are both
        // untrusted; the partition floors at the retained log's `low`.
        let f = derive_resume_frontier(
            &rows,
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Disagreed,
            |_| true,
        );
        assert!(
            f.topic_recreated,
            "a broker disagreement on the topic id is positive evidence of a \
             recreation propagation window: the resume must go conservative"
        );
        let p0 = f.partitions.get(&0).expect("p0");
        assert!(p0.recreated);
        assert!(
            p0.loaded.is_empty(),
            "no row can be generation-confirmed without a definite current \
             id: coverage must be STRIPPED — a possibly-dead span left in \
             `loaded` would let recreated_resume_target walk past \
             new-generation records from `low` (skip = loss)"
        );
        assert_eq!(
            resume_offset(Some(500), p0),
            0,
            "the (possibly dead) committed offset never gates the resume"
        );
        assert_eq!(
            recreated_resume_target(0, 800, p0),
            0,
            "the regrown log is re-consumed from `low`: records [0,500) are \
             re-read (bounded at-least-once duplication), never skipped"
        );
        assert_eq!(
            recreated_resume_target(120, 900, p0),
            120,
            "a retention-advanced `low` floors the walk, exactly like a \
             detected recreation"
        );

        // A partition with NO durable evidence at all is floored through the
        // consumer's synthesis, like every detected recreation.
        let mut f = f;
        assert_eq!(f.synthesize_recreated([0, 3]), 1);
        let p3 = f.partitions.get(&3).expect("synthesized entry");
        assert!(p3.recreated);
        assert_eq!(recreated_resume_target(40, 900, p3), 40);
    }

    #[test]
    fn topic_id_disagreement_regressions_normal_paths_unchanged() {
        // R28 regressions: Agreed keeps the pre-R28 `Some(id)` behavior and
        // Unresolved keeps the pre-R28 `None` (R8 H2 cluster-fallback)
        // behavior — the conservative floor fires ONLY on a disagreement (no
        // spurious whole-log re-consumption, the R8 H2 lesson).
        use ferrodruid_ingest_kafka::streaming::resume_offset;
        let rows = [streaming_row_gen(
            "dur",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        )];

        let agreed = derive_resume_frontier(
            &rows,
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Agreed("tid-1".to_string()),
            |_| true,
        );
        assert!(!agreed.topic_recreated);
        assert_eq!(
            resume_offset(Some(200), agreed.partitions.get(&0).expect("p0")),
            500,
            "a unanimous id with a definite match still advances past the \
             durable coverage (single-broker bootstraps and healthy clusters \
             are unchanged)"
        );

        let unresolved = derive_resume_frontier(
            &rows,
            "events",
            "t",
            Some("kc-A"),
            &TopicIdProbe::Unresolved,
            |_| true,
        );
        assert!(!unresolved.topic_recreated);
        assert_eq!(
            resume_offset(Some(200), unresolved.partitions.get(&0).expect("p0")),
            500,
            "Unresolved keeps the cluster-fallback advance (Codex R8 H2): \
             absence of evidence never floors"
        );
    }

    #[test]
    fn resume_frontier_recreation_dead_rows_do_not_wedge_below_live_coverage() {
        use ferrodruid_ingest_kafka::streaming::{
            clamp_resume_target, recreated_resume_target, resume_offset,
        };
        // Codex R8 H1 (re-stated on the R9 unified recreation model): a
        // recreation was detected on the PREVIOUS restart and the retained
        // log re-consumed from `low` = 300 (the new generation's head was
        // already retention-deleted), so the re-published rows cover
        // [300, 800) under the NEW topic id — while the dead generation's
        // rows are still in the metadata. R7 floored every dead row at 0:
        // the live coverage starting at 300 never connected to it — the
        // resume stayed 0, the clamp sought `low`, and [300, 800) was
        // re-consumed and RE-PUBLISHED on EVERY subsequent restart
        // (permanent, unbounded double count). Under R9 the partition is
        // recreation-FLAGGED and its seek target is derived as
        // `recreated_resume_target(low, high)`: floor = low = 300, and the
        // live coverage — CONTIGUOUS from low — advances it to 800: steady
        // across restarts, nothing re-published, and (unlike the pre-R9
        // shape) a coverage that did NOT reach down to `low` would re-consume
        // the gap instead of skipping it.
        let rows = vec![
            streaming_row_gen(
                "old-gen",
                "events",
                "t",
                Some("kc-A"),
                Some("tid-old"),
                "v2:fp",
                Some(&[(0, 0, 500)]),
            ),
            streaming_row_gen(
                "new-gen",
                "events",
                "t",
                Some("kc-A"),
                Some("tid-new"),
                "v2:fp",
                Some(&[(0, 300, 800)]),
            ),
        ];
        let f =
            compute_resume_frontier(&rows, "events", "t", Some("kc-A"), Some("tid-new"), |_| {
                true
            });
        let p0 = f.get(&0).expect("p0");
        assert!(
            p0.recreated,
            "dead-generation evidence must flag the partition recreated (R9)"
        );
        assert_eq!(
            recreated_resume_target(300, 800, p0),
            800,
            "live coverage CONTIGUOUS from low advances the recreation floor \
             to the live frontier — not wedged at 0, nothing re-consumed or \
             re-published on later restarts (R8 H1 preserved under R9)"
        );

        // Mixed partitions: p0 has live coverage contiguous from low
        // (advances); p1 has ONLY dead-generation evidence — it must still
        // be present, recreation-flagged with no coverage, so the derivation
        // re-consumes its retained log from `low` (no new-generation record
        // is ever skipped).
        let rows = vec![
            streaming_row_gen(
                "old-gen",
                "events",
                "t",
                Some("kc-A"),
                Some("tid-old"),
                "v2:fp",
                Some(&[(0, 0, 500), (1, 0, 400)]),
            ),
            streaming_row_gen(
                "new-gen",
                "events",
                "t",
                Some("kc-A"),
                Some("tid-new"),
                "v2:fp",
                Some(&[(0, 300, 800)]),
            ),
        ];
        let f =
            compute_resume_frontier(&rows, "events", "t", Some("kc-A"), Some("tid-new"), |_| {
                true
            });
        assert_eq!(
            recreated_resume_target(300, 800, f.get(&0).expect("p0")),
            800
        );
        let p1 = f
            .get(&1)
            .expect("a dead-only partition must stay in the frontier (no-loss)");
        assert!(p1.recreated);
        assert_eq!(p1.min_start, 0);
        assert!(p1.loaded.is_empty());
        assert_eq!(
            recreated_resume_target(250, 900, p1),
            250,
            "dead-only partitions re-consume the retained log from low"
        );
        assert_eq!(
            clamp_resume_target(resume_offset(Some(400), p1), 250, 900),
            250,
            "the floor-0 placeholder path agrees: dead committed offsets \
             never gate the seek"
        );

        // A FOREIGN-cluster row that also carries a mismatched topic id is
        // excluded by the CLUSTER gate alone: it must not leave a dead-only
        // low-seek behind (its offsets are another cluster's entirely).
        let foreign = vec![streaming_row_gen(
            "foreign",
            "events",
            "t",
            Some("kc-B"),
            Some("tid-old"),
            "v2:fp",
            Some(&[(0, 0, 500)]),
        )];
        assert!(
            compute_resume_frontier(
                &foreign,
                "events",
                "t",
                Some("kc-A"),
                Some("tid-new"),
                |_| true
            )
            .is_empty(),
            "another cluster's dead rows must not seed even a low-seek"
        );
    }

    #[test]
    fn recreated_partition_live_rows_do_not_lift_the_low_floor() {
        use ferrodruid_ingest_kafka::streaming::{clamp_resume_target, resume_offset};
        // Codex R9 C2: after a detected recreation the consumer re-consumed
        // the new generation, the publish of [300,400) FAILED (no durable
        // row) and [400,800) SUCCEEDED. Retention holds the retained window
        // at [low=300, high=800). The partition now carries BOTH a
        // dead-generation row AND live new-generation coverage [400,800) —
        // pre-R9 the live row disqualified the partition from the dead-only
        // floor-0, so the resume started at the live min_start=400 and
        // advanced to 800: the never-durable [300,400) records were skipped
        // FOREVER (loss). A recreation-detected partition must keep its
        // floor at `low` regardless of live evidence; live coverage may only
        // advance the resume CONTIGUOUSLY from `low`, and a gap
        // [low, live_start) forces re-consumption from `low`.
        let rows = vec![
            streaming_row_gen(
                "old-gen",
                "events",
                "t",
                Some("kc-A"),
                Some("tid-old"),
                "v2:fp",
                Some(&[(0, 0, 500)]),
            ),
            streaming_row_gen(
                "new-gen",
                "events",
                "t",
                Some("kc-A"),
                Some("tid-new"),
                "v2:fp",
                Some(&[(0, 400, 800)]),
            ),
        ];
        let f =
            compute_resume_frontier(&rows, "events", "t", Some("kc-A"), Some("tid-new"), |_| {
                true
            });
        let p0 = f.get(&0).expect("p0");
        // The dead generation's committed offset (500) survives the
        // recreation in the group coordinator; it must not gate anything.
        let target = resume_offset(Some(500), p0);
        assert_eq!(
            clamp_resume_target(target, 300, 800),
            300,
            "a recreation-detected partition must re-consume from `low`: the \
             live row [400,800) must NOT lift the floor above the undurable \
             publish gap [300,400) — seeking past it is permanent loss (R9 C2)"
        );
        // The unified model end-to-end (R9): floor = low, advance = live
        // coverage contiguous from low, restore = the resume_floor alone.
        use ferrodruid_ingest_kafka::streaming::{recreated_resume_target, restore_target};
        assert!(p0.recreated, "live evidence must not clear the flag (R9)");
        assert_eq!(
            recreated_resume_target(300, 800, p0),
            300,
            "gap [300,400) below the live coverage → resume at low (no loss); \
             the [400,800) coverage above the gap is re-consumed once \
             (bounded at-least-once duplication, accepted)"
        );
        assert_eq!(
            recreated_resume_target(400, 800, p0),
            800,
            "retention later advances low to 400 → the coverage is contiguous \
             from low and is skipped (no unbounded per-restart re-publish)"
        );
        assert_eq!(
            recreated_resume_target(0, 350, p0),
            0,
            "the log shrank again below the coverage (second truncation): \
             the walked target re-clamps to low (R5 H1)"
        );
        assert_eq!(
            restore_target(p0.recreated, Some(500), 420),
            420,
            "a rebalance restore on the recreated partition is resume_floor \
             driven — the dead committed 500 is never mixed in (R9 C1)"
        );
        // Control: a live-only partition (no dead-generation row) keeps the
        // pre-R9 committed/min_start-governed derivation.
        let live_only = vec![streaming_row_gen(
            "new-gen",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-new"),
            "v2:fp",
            Some(&[(0, 400, 800)]),
        )];
        let f = compute_resume_frontier(
            &live_only,
            "events",
            "t",
            Some("kc-A"),
            Some("tid-new"),
            |_| true,
        );
        assert!(
            !f.get(&0).expect("p0").recreated,
            "no dead-generation evidence → not recreation-flagged"
        );
    }

    #[test]
    fn resume_frontier_topic_id_unresolved_advances_on_cluster_identity() {
        use ferrodruid_ingest_kafka::streaming::resume_offset;
        // Codex R8 H2 (supersedes the R7 floor-only matrix): the KIP-516
        // topic id is an ENHANCEMENT — it tightens the frontier only on
        // POSITIVE evidence of a recreation (a definite mismatch). When it
        // cannot be resolved at all (TLS/SASL listener the PLAINTEXT probe
        // cannot speak to, a pre-2.8 broker, a failed probe) or the row was
        // never stamped, the cluster-identity gating alone governs (the R6
        // behavior). R7's downgrade-to-floor-only instead pinned the resume
        // at the OLDEST durable span start on EVERY restart: the whole
        // retained log was re-consumed and RE-PUBLISHED each time — unbounded
        // duplication + unbounded durable-segment growth on every TLS/SASL
        // deployment.
        //
        // (1) Row stamped, CURRENT topic id unresolved, cluster definite
        //     match, blob reloaded: ADVANCES (no restart re-consumption).
        let stamped = vec![streaming_row_gen(
            "s0",
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            "v2:fp",
            Some(&[(0, 100, 500)]),
        )];
        let f = compute_resume_frontier(&stamped, "events", "t", Some("kc-A"), None, |_| true);
        let p0 = f.get(&0).expect("p0");
        assert_eq!(p0.min_start, 100);
        assert!(
            !p0.loaded.is_empty(),
            "a cluster-confirmed row must ADVANCE when the topic id is \
             unresolvable — the id is an enhancement, not a downgrade trigger (H2)"
        );
        assert_eq!(
            resume_offset(Some(500), p0),
            500,
            "restart must resume PAST the durable frontier, not re-consume it (H2)"
        );
        // (2) Row UNSTAMPED (published while the id was unresolvable, or
        //     pre-R7 legacy), current resolved: same cluster fallback.
        let unstamped = vec![streaming_row_gen(
            "s0",
            "events",
            "t",
            Some("kc-A"),
            None,
            "v2:fp",
            Some(&[(0, 100, 500)]),
        )];
        let f = compute_resume_frontier(
            &unstamped,
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        assert_eq!(resume_offset(Some(500), f.get(&0).expect("p0")), 500);
        // (3) Both unknown: still the cluster fallback.
        let f = compute_resume_frontier(&unstamped, "events", "t", Some("kc-A"), None, |_| true);
        assert_eq!(resume_offset(Some(500), f.get(&0).expect("p0")), 500);
        // (4) The CLUSTER gate is NOT weakened by the fallback: a topic-id
        //     match with an UNRESOLVED cluster identity stays floor-only.
        let f = compute_resume_frontier(&stamped, "events", "t", None, Some("tid-1"), |_| true);
        assert!(
            f.get(&0).expect("p0").loaded.is_empty(),
            "advance still requires a DEFINITE cluster match (R4 H4/R5 H2)"
        );
        // (5) A PHANTOM blob still floors under the fallback (is_loaded
        //     gates coverage exactly as before).
        let f = compute_resume_frontier(&stamped, "events", "t", Some("kc-A"), None, |_| false);
        assert_eq!(
            resume_offset(Some(600), f.get(&0).expect("p0")),
            100,
            "a phantom row floors the resume below its hole (C2), fallback or not"
        );
        // (6) Definite match (the default helpers): advances (control).
        let matched = vec![streaming_row(
            "s0",
            "events",
            "t",
            Some("kc-A"),
            "v2:fp",
            Some(&[(0, 100, 500)]),
        )];
        let f =
            compute_resume_frontier(&matched, "events", "t", Some("kc-A"), Some("tid-1"), |_| {
                true
            });
        assert_eq!(resume_offset(Some(100), f.get(&0).expect("p0")), 500);
    }

    #[test]
    fn resume_frontier_malformed_span_floors_but_never_advances() {
        use ferrodruid_ingest_kafka::streaming::resume_offset;
        // Codex R16 H1: a durable row's kafkaOffsets span is STRUCTURALLY
        // validated before its `next` may ADVANCE (skip past) the resume. A
        // corrupt span — `next < start`, a non-positive `next`, a non-integer
        // `next`, or a negative `start` — overstates durable coverage;
        // trusting it to skip would lose [real, stamped) forever. The valid
        // `start` still FLOORS the resume; only the skip-past is withheld
        // (fail-closed). Well-formed spans are unaffected (regression below).
        // The row is cluster-matched, topic-matched, and LOADED, so the ONLY
        // thing that can withhold the advance is the span validation.

        // (1) `next < start` (backwards): start=100 floors, next=50 must NOT
        //     advance. Committed already past it → re-consume from the floor.
        let mut backwards = streaming_row("bad", "events", "t", Some("kc-A"), "v2:fp", None);
        backwards.payload["kafkaOffsets"] = json!({ "0": { "start": 100, "next": 50 } });
        let f = compute_resume_frontier(
            &[backwards],
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        let p0 = f.get(&0).expect("a valid start still floors the frontier");
        assert_eq!(p0.min_start, 100, "the valid start floors the resume");
        assert!(
            p0.loaded.is_empty(),
            "a backwards (next < start) span must never advance the resume (H1)"
        );
        assert_eq!(
            resume_offset(Some(400), p0),
            100,
            "committed past a corrupt span is not trusted — re-consume from the floor"
        );

        // (2) non-positive `next`: floor-only.
        let mut nonpos = streaming_row("np", "events", "t", Some("kc-A"), "v2:fp", None);
        nonpos.payload["kafkaOffsets"] = json!({ "0": { "start": 0, "next": 0 } });
        let f = compute_resume_frontier(
            &[nonpos],
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        assert!(
            f.get(&0).expect("p0").loaded.is_empty(),
            "a non-positive next must never advance the resume (H1)"
        );

        // (3) non-integer `next` (string): the valid start still floors,
        //     advance withheld.
        let mut nonint = streaming_row("ni", "events", "t", Some("kc-A"), "v2:fp", None);
        nonint.payload["kafkaOffsets"] = json!({ "0": { "start": 100, "next": "500" } });
        let f = compute_resume_frontier(
            &[nonint],
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        let p0 = f
            .get(&0)
            .expect("a valid start floors even when next is non-integer");
        assert_eq!(p0.min_start, 100);
        assert!(
            p0.loaded.is_empty(),
            "a non-integer next must never advance the resume (H1)"
        );

        // (4) negative `start`: not a real offset — dropped from BOTH roles.
        let mut negstart = streaming_row("neg", "events", "t", Some("kc-A"), "v2:fp", None);
        negstart.payload["kafkaOffsets"] = json!({ "0": { "start": -5, "next": 100 } });
        let f = compute_resume_frontier(
            &[negstart],
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        assert!(
            !f.contains_key(&0),
            "a negative start names no real offset — dropped from the frontier entirely (H1)"
        );

        // (5) REGRESSION: a well-formed span still ADVANCES exactly as before.
        let good = vec![streaming_row(
            "ok",
            "events",
            "t",
            Some("kc-A"),
            "v2:fp",
            Some(&[(0, 100, 500)]),
        )];
        let f =
            compute_resume_frontier(&good, "events", "t", Some("kc-A"), Some("tid-1"), |_| true);
        assert_eq!(
            resume_offset(Some(100), f.get(&0).expect("p0")),
            500,
            "a well-formed span must still advance past its durable coverage"
        );
    }

    #[test]
    fn resume_frontier_malformed_topic_id_is_floor_only_not_unstamped() {
        use ferrodruid_ingest_kafka::streaming::resume_offset;
        // Codex R16 H2: a topicId that is PRESENT but corrupt (null / empty /
        // non-string) must be distinguished from an ABSENT (field-missing,
        // genuinely unstamped) topicId. An absent id keeps the R8 H2 cluster
        // fallback (advance-eligible — refusing it re-consumes the whole
        // retained log on every restart for TLS/SASL deployments). A MALFORMED
        // id is untrustworthy: forced FLOOR-ONLY — after a recreation (current
        // id resolvable and DIFFERENT), a dead generation's row with a corrupt
        // topicId would otherwise advance and skip the live generation (loss).
        // Every fixture is cluster-matched + LOADED; current id is "tid-1".

        // (1) topicId = null → MALFORMED → floor-only.
        let mut null_id = streaming_row_gen(
            "s0",
            "events",
            "t",
            Some("kc-A"),
            None,
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        null_id.payload["topicId"] = json!(null);
        let f = compute_resume_frontier(
            &[null_id],
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        let p0 = f
            .get(&0)
            .expect("a malformed-topicId row still FLOORS the frontier");
        assert_eq!(p0.min_start, 0, "the valid span floors the resume");
        assert!(
            p0.loaded.is_empty(),
            "a null topicId is corrupt, not unstamped — never advance-eligible (H2)"
        );
        assert_eq!(
            resume_offset(Some(500), p0),
            0,
            "committed past the corrupt-stamp span is not trusted — re-consume from the floor"
        );

        // (2) topicId = number (non-string) → MALFORMED → floor-only.
        let mut num_id = streaming_row_gen(
            "s0",
            "events",
            "t",
            Some("kc-A"),
            None,
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        num_id.payload["topicId"] = json!(12_345);
        let f = compute_resume_frontier(
            &[num_id],
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        assert!(
            f.get(&0).expect("p0").loaded.is_empty(),
            "a non-string topicId is corrupt — floor-only (H2)"
        );

        // (3) topicId = "" (empty string) → MALFORMED → floor-only.
        let mut empty_id = streaming_row_gen(
            "s0",
            "events",
            "t",
            Some("kc-A"),
            None,
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        empty_id.payload["topicId"] = json!("");
        let f = compute_resume_frontier(
            &[empty_id],
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        assert!(
            f.get(&0).expect("p0").loaded.is_empty(),
            "an empty topicId is corrupt — floor-only (H2)"
        );

        // (4) REGRESSION (R8 H2): an ABSENT topicId (field truly missing)
        //     keeps the cluster fallback and ADVANCES — the malformed handling
        //     must not have collapsed this legitimate unstamped case (which
        //     would re-introduce the TLS/SASL unbounded-replay regression).
        let absent = streaming_row_gen(
            "s0",
            "events",
            "t",
            Some("kc-A"),
            None,
            "v2:fp",
            Some(&[(0, 0, 500)]),
        );
        assert!(
            absent.payload.get("topicId").is_none(),
            "the absent-id fixture must genuinely omit topicId"
        );
        let f = compute_resume_frontier(
            &[absent],
            "events",
            "t",
            Some("kc-A"),
            Some("tid-1"),
            |_| true,
        );
        assert_eq!(
            resume_offset(Some(500), f.get(&0).expect("p0")),
            500,
            "an ABSENT topicId still advances on the cluster fallback (R8 H2 regression)"
        );
    }

    #[test]
    fn select_streaming_victims_keeps_durable_rows_drops_legacy() {
        // Durable (kafkaOffsets-carrying) rows are KEPT (resumed past the
        // frontier), never dropped; legacy non-durable same-pair rows are still
        // victims (dropped + earliest-replayed).
        let rows = vec![
            streaming_row(
                "durable",
                "events",
                "t",
                Some("kc-A"),
                "v2:fp",
                Some(&[(0, 0, 500)]),
            ),
            streaming_row("legacy", "events", "t", Some("kc-A"), "v2:fp", None),
        ];
        let scan =
            select_streaming_victims(rows, "events", "t", Some("kc-A"), "v2:fp").expect("scan");
        assert_eq!(scan.victims, vec!["legacy".to_string()]);
        assert_eq!(scan.kept_durable, 1);
    }

    #[test]
    fn select_streaming_victims_refuses_schema_change_even_for_durable() {
        // The R26 F1 schema-change refusal fires on a durable row too (before
        // the keep decision): a changed schema cannot share the datasource.
        let rows = vec![streaming_row(
            "durable",
            "events",
            "t",
            Some("kc-A"),
            "v2:OLD",
            Some(&[(0, 0, 500)]),
        )];
        let err = select_streaming_victims(rows, "events", "t", Some("kc-A"), "v2:NEW")
            .expect_err("a durable row with a changed schema must refuse");
        assert!(
            format!("{err}").contains("DIFFERENT ingestion schema"),
            "err = {err}"
        );
    }

    /// A consumer handle whose task has ALREADY exited reporting a failed
    /// final drain (lost buffered rows) — models the aftermath of a broken
    /// publish tail for the R26 F2 / R30 F3 tombstone-refusal tests.
    fn lossy_handle(
        shutdown_tx: mpsc::Sender<()>,
        data_source: &str,
        topic: &str,
    ) -> KafkaSupervisorHandle {
        KafkaSupervisorHandle {
            shutdown_tx,
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
            data_source: data_source.to_string(),
            topic: topic.to_string(),
            // Fingerprint + brokers of `flattened_spec` — the (events,
            // wiki-events) spec every sentinel test recovers with — so a
            // same-schema, same-cluster earliest re-create is accepted as
            // recovery (Codex R33).
            schema_fp: schema_fingerprint(
                &crate::validate_kafka_spec(&flattened_spec()).expect("valid flattened spec"),
            ),
            brokers: "127.0.0.1:9092".to_string(),
        }
    }

    /// Sum a `count` timeseries over all time through the real query path —
    /// this is what a SQL `COUNT(*)` observes, so it proves the published
    /// rows are query-visible (not merely present in metadata).
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

    #[tokio::test]
    async fn publish_makes_streaming_rows_queryable() {
        let (metadata, historical, _dir) = setup().await;
        let seg = segment(
            "events",
            &[row(1_000, "a"), row(2_000, "b"), row(3_000, "a")],
        );

        let id = publish_streaming_segment(
            &metadata,
            &historical,
            &prov("sup-1", "t", "127.0.0.1:9092", None, "v2:fp-test"),
            seg,
        )
        .await
        .expect("publish");
        assert!(id.starts_with("events_"), "id={id}");

        // Query-visible.
        assert_eq!(queried_row_count(&historical, "events"), 3);
        // Metadata row exists and is used.
        assert!(metadata.segment_exists(&id).await.expect("exists"));
    }

    #[tokio::test]
    async fn sink_publish_appends_across_segments() {
        let (metadata, historical, _dir) = setup().await;
        let sink = OverlordKafkaSink {
            metadata: Arc::clone(&metadata),
            historical: Arc::clone(&historical),
            deep_storage: None,
            task_id: "sup-append".to_string(),
            topic: "t".to_string(),
            bootstrap: "127.0.0.1:9092".to_string(),
            cluster_id: None,
            topic_id: None,
            schema_fp: "v2:fp-test".to_string(),
        };

        sink.publish(segment("events", &[row(1_000, "a"), row(2_000, "b")]))
            .await
            .expect("first publish");
        sink.publish(segment("events", &[row(3_000, "c")]))
            .await
            .expect("second publish");

        // Append semantics: both segments contribute; no overwrite.
        assert_eq!(queried_row_count(&historical, "events"), 3);
    }

    /// compat-3 stage 1: with a deep-storage backend configured, a streaming
    /// publish PERSISTS the segment (blob present, re-openable) and stamps a
    /// `loadSpec` marker into the metadata row's payload — while staying
    /// query-visible. Offsets are untouched (stage 2).
    #[tokio::test]
    async fn streaming_publish_persists_blob_and_loadspec() {
        let (metadata, historical, _dir) = setup().await;
        let ds_base = tempfile::tempdir().expect("deep-storage dir");
        let deep_storage =
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_base.path().to_path_buf());
        let ds_ref: &dyn DeepStorage = &deep_storage;

        let seg = segment(
            "events",
            &[row(1_000, "a"), row(2_000, "b"), row(3_000, "a")],
        );
        let id = publish_streaming_segment_persisted(
            &metadata,
            &historical,
            Some(ds_ref),
            &prov(
                "sup-1",
                "wiki-events",
                "127.0.0.1:9092",
                Some("cid-1"),
                "v2:fp-test",
            ),
            &PartitionOffsets::new(),
            seg,
        )
        .await
        .expect("publish");

        // Query-visible.
        assert_eq!(queried_row_count(&historical, "events"), 3);

        // Blob persisted + re-openable with the same row count.
        assert!(
            deep_storage
                .segment_exists("events", &id)
                .await
                .expect("exists")
        );
        let dl = tempfile::tempdir().expect("dl dir");
        let dest = dl.path().join("seg");
        deep_storage
            .download_segment("events", &id, &dest)
            .await
            .expect("download");
        let reloaded = ferrodruid_segment::SegmentData::open(&dest).expect("reopen");
        assert_eq!(reloaded.num_rows, 3);

        // Metadata row carries the loadSpec marker.
        let all = metadata.get_all_segments().await.expect("all segments");
        let row_meta = all.iter().find(|s| s.id == id).expect("metadata row");
        let load_spec = row_meta
            .payload
            .get("loadSpec")
            .expect("streaming payload carries a loadSpec");
        assert_eq!(
            load_spec.get("type").and_then(|v| v.as_str()),
            Some("local")
        );
        assert_eq!(
            load_spec.get("segmentId").and_then(|v| v.as_str()),
            Some(id.as_str())
        );
        // Stage-1 invariant: no offset key is stamped by the publish tail.
        assert!(row_meta.payload.get("offset").is_none());
    }

    /// Codex R14 H2 (pure decision): a swap failure only treats the offset as
    /// durable — committing it instead of re-consuming — when the metadata row
    /// survived a FAILED rollback AND the segment is durable (blob-backed). Any
    /// other combination re-consumes (at-least-once). Pre-fix the publish tail
    /// unconditionally surfaced a failure here, so the streaming loop always
    /// re-seeked/re-published — double-counting the durable-retained segment on
    /// restart.
    #[test]
    fn classify_swap_failure_retains_only_when_durable_and_rollback_failed() {
        use SwapFailureDisposition::{ReSeek, RetainDurable};
        // The H2 case: rollback FAILED (row survives) + durable (reloads on
        // restart) → commit the offset, do NOT re-consume.
        assert_eq!(classify_swap_failure(false, true), RetainDurable);
        // Rollback SUCCEEDED (row gone) → offset not durable → re-consume.
        assert_eq!(classify_swap_failure(true, true), ReSeek);
        // Memory-only (no blob): a lingering row is not reloaded on restart, so
        // it is not durable → re-consume (offsets never committed anyway).
        assert_eq!(classify_swap_failure(false, false), ReSeek);
        assert_eq!(classify_swap_failure(true, false), ReSeek);
    }

    /// Codex R14 H2 (sink mapping): a live publish AND a `RetainedDurable`
    /// failure both report `Ok(())` to the streaming loop, so it COMMITS the
    /// covered offset and never re-seeks to re-consume/re-publish a durable row
    /// that will simply reload on restart (that would permanently double
    /// count). Only a genuine `Failed` becomes a `SegmentSinkError` (re-consume,
    /// at-least-once). Pre-fix there was no `RetainedDurable` variant, so a
    /// swap-failed-but-retained segment mapped to `Err` and the loop re-seeked.
    #[test]
    fn retained_durable_publish_maps_to_sink_ok_so_offset_is_committed() {
        assert!(
            sink_result_from_publish(Ok("events_seg-live".to_string())).is_ok(),
            "a live publish commits its offset"
        );
        assert!(
            sink_result_from_publish(Err(StreamingPublishError::RetainedDurable(
                "events_seg-retained".to_string()
            )))
            .is_ok(),
            "a durable-retained segment's offset must be committed (H2), not re-consumed"
        );
        assert!(
            sink_result_from_publish(Err(StreamingPublishError::Failed(DruidError::Ingestion(
                "persist failed".to_string()
            ))))
            .is_err(),
            "a genuine failure must re-consume (at-least-once)"
        );
    }

    /// Codex R14 H2 (end-to-end, ReSeek direction): drive a REAL swap failure
    /// through the durable publish tail — a 1-byte Historical cache rejects any
    /// add, so `replace_segments` fails deterministically without poisoning a
    /// lock. With a real SQLite store the rollback SUCCEEDS (a `restore=&[]`
    /// rollback is just a `DELETE`), so the offset is NOT durable: the publish
    /// surfaces a `Failed` (the loop re-consumes) and the metadata row is gone
    /// (no phantom, no double count). The RetainedDurable branch is covered by
    /// the pure `classify_swap_failure` test above — with real SQLite a
    /// rollback cannot be made to fail deterministically.
    #[tokio::test]
    async fn streaming_swap_failure_with_successful_rollback_surfaces_failed_and_removes_row() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("store"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("cache dir");
        // 1-byte cache: any segment add exceeds the limit → the query-visible
        // swap fails, exercising the swap-failure branch on the durable path.
        let historical = Arc::new(Historical::new(cache_dir.path().to_path_buf(), 1));
        let ds_base = tempfile::tempdir().expect("deep-storage dir");
        let deep_storage =
            ferrodruid_deep_storage::LocalDeepStorage::new(ds_base.path().to_path_buf());
        let ds_ref: &dyn DeepStorage = &deep_storage;
        let mut offsets = PartitionOffsets::new();
        offsets.insert(0, OffsetSpan::new(0, 3));

        let res = publish_streaming_segment_persisted(
            &metadata,
            &historical,
            Some(ds_ref),
            &prov(
                "sup-swapfail",
                "t",
                "127.0.0.1:9092",
                Some("cid-1"),
                "v2:fp",
            ),
            &offsets,
            segment(
                "events",
                &[row(1_000, "a"), row(2_000, "b"), row(3_000, "c")],
            ),
        )
        .await;

        assert!(
            matches!(res, Err(StreamingPublishError::Failed(_))),
            "swap failure + successful rollback must surface a Failed publish (re-consume), \
             got {res:?}"
        );
        assert!(
            metadata
                .get_all_segments()
                .await
                .expect("all segments")
                .is_empty(),
            "a successful rollback must leave NO metadata row (re-consume, never a retained \
             phantom that would double-count on restart)"
        );
    }

    /// C3: `kafkaOffsets` is stamped ONLY when the segment is actually DURABLE
    /// (a deep-storage backend is configured). A memory-only publish must NOT
    /// seed a resume frontier, or a restart would seek past records whose only
    /// copy vanished with the process (loss). Passing non-empty offsets does
    /// not change this on the no-backend path.
    #[tokio::test]
    async fn streaming_publish_stamps_offsets_only_when_durable() {
        let mut offsets = PartitionOffsets::new();
        offsets.insert(0, OffsetSpan::new(0, 3));

        // Memory-only (deep_storage=None): NO kafkaOffsets, even with offsets.
        {
            let (metadata, historical, _dir) = setup().await;
            let id = publish_streaming_segment_persisted(
                &metadata,
                &historical,
                None,
                &prov("sup-mem", "t", "127.0.0.1:9092", Some("cid-1"), "v2:fp"),
                &offsets,
                segment(
                    "events",
                    &[row(1_000, "a"), row(2_000, "b"), row(3_000, "c")],
                ),
            )
            .await
            .expect("publish");
            let all = metadata.get_all_segments().await.expect("all");
            let m = all.iter().find(|s| s.id == id).expect("row");
            assert!(
                m.payload.get("kafkaOffsets").is_none(),
                "memory-only publish must NOT stamp kafkaOffsets (C3)"
            );
            assert!(m.payload.get("loadSpec").is_none());
        }

        // Durable (deep_storage=Some): kafkaOffsets IS stamped.
        {
            let (metadata, historical, _dir) = setup().await;
            let ds_base = tempfile::tempdir().expect("ds dir");
            let deep_storage =
                ferrodruid_deep_storage::LocalDeepStorage::new(ds_base.path().to_path_buf());
            let ds_ref: &dyn DeepStorage = &deep_storage;
            let id = publish_streaming_segment_persisted(
                &metadata,
                &historical,
                Some(ds_ref),
                &prov("sup-dur", "t", "127.0.0.1:9092", Some("cid-1"), "v2:fp"),
                &offsets,
                segment(
                    "events",
                    &[row(1_000, "a"), row(2_000, "b"), row(3_000, "c")],
                ),
            )
            .await
            .expect("publish");
            let all = metadata.get_all_segments().await.expect("all");
            let m = all.iter().find(|s| s.id == id).expect("row");
            let ko = m
                .payload
                .get("kafkaOffsets")
                .and_then(|v| v.as_object())
                .expect("durable publish stamps kafkaOffsets (C3)");
            assert_eq!(
                ko.get("0")
                    .and_then(|s| s.get("next"))
                    .and_then(|v| v.as_i64()),
                Some(3)
            );
        }
    }

    /// Codex R4 H4: `kafkaOffsets` is stamped ONLY together with a resolved
    /// `clusterId` — the invariant "a frontier-eligible durable row always
    /// names its cluster". A durable publish whose consumer could not resolve
    /// the cluster identity at start OMITS `kafkaOffsets` (the segment stays
    /// durable + queryable + reloadable; it just never seeds a resume
    /// frontier), so an identity-less row can never be mistaken for
    /// authoritative seek evidence after a bootstrap-name repoint.
    #[tokio::test]
    async fn durable_publish_stamps_offsets_only_with_cluster_identity() {
        let mut offsets = PartitionOffsets::new();
        offsets.insert(0, OffsetSpan::new(0, 3));

        // Durable + resolved identity: BOTH kafkaOffsets and clusterId stamped.
        {
            let (metadata, historical, _dir) = setup().await;
            let ds_base = tempfile::tempdir().expect("ds dir");
            let deep_storage =
                ferrodruid_deep_storage::LocalDeepStorage::new(ds_base.path().to_path_buf());
            let ds_ref: &dyn DeepStorage = &deep_storage;
            let id = publish_streaming_segment_persisted(
                &metadata,
                &historical,
                Some(ds_ref),
                &prov("sup-cid", "t", "127.0.0.1:9092", Some("kc-A"), "v2:fp"),
                &offsets,
                segment(
                    "events",
                    &[row(1_000, "a"), row(2_000, "b"), row(3_000, "c")],
                ),
            )
            .await
            .expect("publish");
            let all = metadata.get_all_segments().await.expect("all");
            let m = all.iter().find(|s| s.id == id).expect("row");
            assert!(m.payload.get("kafkaOffsets").is_some());
            assert_eq!(
                m.payload.get("clusterId").and_then(|v| v.as_str()),
                Some("kc-A"),
                "kafkaOffsets must always be accompanied by clusterId (H4)"
            );
        }

        // Durable but UNRESOLVED identity: kafkaOffsets OMITTED (fail-safe:
        // the row must never look frontier-eligible without a definite
        // identity), while the segment itself stays durable (loadSpec kept).
        {
            let (metadata, historical, _dir) = setup().await;
            let ds_base = tempfile::tempdir().expect("ds dir");
            let deep_storage =
                ferrodruid_deep_storage::LocalDeepStorage::new(ds_base.path().to_path_buf());
            let ds_ref: &dyn DeepStorage = &deep_storage;
            let id = publish_streaming_segment_persisted(
                &metadata,
                &historical,
                Some(ds_ref),
                &prov("sup-nocid", "t", "127.0.0.1:9092", None, "v2:fp"),
                &offsets,
                segment(
                    "events",
                    &[row(1_000, "a"), row(2_000, "b"), row(3_000, "c")],
                ),
            )
            .await
            .expect("publish");
            let all = metadata.get_all_segments().await.expect("all");
            let m = all.iter().find(|s| s.id == id).expect("row");
            assert!(
                m.payload.get("kafkaOffsets").is_none(),
                "an identity-less durable row must NOT carry kafkaOffsets (H4)"
            );
            assert!(
                m.payload.get("loadSpec").is_some(),
                "the segment itself stays durable/reloadable"
            );
        }
    }

    /// Codex R7 H1: a durable publish stamps the resolved KIP-516 `topicId`
    /// alongside `clusterId`/`kafkaOffsets` (the generation identity the
    /// resume frontier compares to detect a topic recreation); with the id
    /// UNRESOLVED the row is stamped WITHOUT `topicId` — offsets kept (the
    /// floor role survives) but never skippable (unstamped = floor-only).
    #[tokio::test]
    async fn durable_publish_stamps_topic_id_alongside_offsets() {
        let mut offsets = PartitionOffsets::new();
        offsets.insert(0, OffsetSpan::new(0, 3));

        // Resolved topic id → stamped.
        {
            let (metadata, historical, _dir) = setup().await;
            let ds_base = tempfile::tempdir().expect("ds dir");
            let deep_storage =
                ferrodruid_deep_storage::LocalDeepStorage::new(ds_base.path().to_path_buf());
            let ds_ref: &dyn DeepStorage = &deep_storage;
            let id = publish_streaming_segment_persisted(
                &metadata,
                &historical,
                Some(ds_ref),
                &prov_tid(
                    "sup-tid",
                    "t",
                    "127.0.0.1:9092",
                    Some("kc-A"),
                    Some("tid-9"),
                    "v2:fp",
                ),
                &offsets,
                segment("events", &[row(1_000, "a")]),
            )
            .await
            .expect("publish");
            let all = metadata.get_all_segments().await.expect("all");
            let m = all.iter().find(|s| s.id == id).expect("row");
            assert_eq!(
                m.payload.get("topicId").and_then(|v| v.as_str()),
                Some("tid-9"),
                "the resolved topic id must be stamped (R7 H1)"
            );
            assert!(m.payload.get("kafkaOffsets").is_some());
        }

        // Unresolved topic id → row stamped WITHOUT topicId, offsets kept.
        {
            let (metadata, historical, _dir) = setup().await;
            let ds_base = tempfile::tempdir().expect("ds dir");
            let deep_storage =
                ferrodruid_deep_storage::LocalDeepStorage::new(ds_base.path().to_path_buf());
            let ds_ref: &dyn DeepStorage = &deep_storage;
            let id = publish_streaming_segment_persisted(
                &metadata,
                &historical,
                Some(ds_ref),
                &prov_tid(
                    "sup-notid",
                    "t",
                    "127.0.0.1:9092",
                    Some("kc-A"),
                    None,
                    "v2:fp",
                ),
                &offsets,
                segment("events", &[row(1_000, "a")]),
            )
            .await
            .expect("publish");
            let all = metadata.get_all_segments().await.expect("all");
            let m = all.iter().find(|s| s.id == id).expect("row");
            assert!(
                m.payload.get("topicId").is_none(),
                "an unresolved topic id must not be fabricated"
            );
            assert!(
                m.payload.get("kafkaOffsets").is_some(),
                "offsets stay stamped — the resume frontier gates a \
                 topicId-less row on the cluster identity alone (R8 H2)"
            );
        }
    }

    /// C3: the overlord sink only commits offsets when it can persist durably.
    #[tokio::test]
    async fn sink_commits_offsets_iff_deep_storage_present() {
        let (metadata, historical, _dir) = setup().await;
        let mut sink = OverlordKafkaSink {
            metadata: Arc::clone(&metadata),
            historical: Arc::clone(&historical),
            deep_storage: None,
            task_id: "s".to_string(),
            topic: "t".to_string(),
            bootstrap: "127.0.0.1:9092".to_string(),
            cluster_id: None,
            topic_id: None,
            schema_fp: "v2:fp".to_string(),
        };
        assert!(!sink.commits_offsets(), "no backend → no commit (C3)");
        let ds_base = tempfile::tempdir().expect("ds dir");
        sink.deep_storage = Some(Arc::new(ferrodruid_deep_storage::LocalDeepStorage::new(
            ds_base.path().to_path_buf(),
        )));
        assert!(sink.commits_offsets(), "backend present → commit");
    }

    fn flattened_spec() -> serde_json::Value {
        json!({
            "type": "kafka",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {
                "topic": "wiki-events",
                "consumerProperties": {"bootstrap.servers": "127.0.0.1:9092"},
                "useEarliestOffset": true
            }
        })
    }

    /// [`flattened_spec`] with `useEarliestOffset:false` — which is also the
    /// spec DEFAULT (`Option<bool>` → `unwrap_or(false)` in
    /// `build_consumer_config`): the consumer starts at the topic TAIL and
    /// never redelivers previously produced records.
    fn latest_spec() -> serde_json::Value {
        let mut spec = flattened_spec();
        spec["ioConfig"]["useEarliestOffset"] = json!(false);
        spec
    }

    /// An id-less EARLIEST Kafka spec for an arbitrary (datasource, topic)
    /// pair, for tests that need several non-conflicting supervisors.
    fn pair_spec(ds: &str, topic: &str) -> serde_json::Value {
        json!({
            "type": "kafka",
            "dataSchema": {
                "dataSource": ds,
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {
                "topic": topic,
                "consumerProperties": {"bootstrap.servers": "127.0.0.1:59092"},
                "useEarliestOffset": true
            }
        })
    }

    #[test]
    fn parse_accepts_flattened_and_enveloped() {
        // Flattened.
        let p = crate::parse_kafka_supervisor_spec(&flattened_spec()).expect("flattened parses");
        assert_eq!(p.data_schema.data_source, "events");
        assert_eq!(p.io_config.topic, "wiki-events");

        // Enveloped (modern Druid POST shape): inner `spec`, outer `type`.
        let enveloped = json!({
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": "events",
                    "timestampSpec": {"column": "__time", "format": "auto"},
                    "dimensionsSpec": {"dimensions": ["page"]}
                },
                "ioConfig": {
                    "topic": "wiki-events",
                    "consumerProperties": {"bootstrap.servers": "127.0.0.1:9092"}
                }
            }
        });
        let p = crate::parse_kafka_supervisor_spec(&enveloped).expect("enveloped parses");
        assert_eq!(p.data_schema.data_source, "events");
        assert_eq!(p.io_config.topic, "wiki-events");
    }

    #[test]
    fn parse_accepts_real_druid_shaped_spec() {
        // A realistic Druid supervisor POST: top-level `id`/`suspended`,
        // enveloped `spec`, ioConfig with `type`/`inputFormat`, and a
        // tuningConfig with `type` + fields this crate does not model.
        let druid = json!({
            "type": "kafka",
            "id": "events-supervisor",
            "suspended": false,
            "spec": {
                "dataSchema": {
                    "dataSource": "events",
                    "timestampSpec": {"column": "__time", "format": "auto"},
                    "dimensionsSpec": {"dimensions": [
                        "page",
                        {"type": "long", "name": "added"}
                    ]}
                },
                "ioConfig": {
                    "type": "kafka",
                    "topic": "wiki-events",
                    "inputFormat": {"type": "json"},
                    "consumerProperties": {"bootstrap.servers": "kafka:9092"},
                    "useEarliestOffset": true,
                    "taskCount": 2
                },
                "tuningConfig": {
                    "type": "kafka",
                    "maxRowsPerSegment": 500000,
                    "maxBytesInMemory": 134217728,
                    "maxPendingPersists": 0
                }
            }
        });
        let p = crate::parse_kafka_supervisor_spec(&druid).expect("real Druid spec must parse");
        assert_eq!(p.data_schema.data_source, "events");
        assert_eq!(p.io_config.topic, "wiki-events");
        assert_eq!(
            p.tuning_config.and_then(|t| t.max_rows_per_segment),
            Some(500_000)
        );
    }

    #[test]
    fn parse_rejects_non_kafka_shapes() {
        // Bare tombstone/echo shape (strips to just `{type}` — no dataSchema).
        assert!(crate::parse_kafka_supervisor_spec(&json!({"id": "x", "type": "kafka"})).is_none());
        // Missing required ioConfig.
        assert!(
            crate::parse_kafka_supervisor_spec(&json!({"type": "kafka", "dataSchema": {}}))
                .is_none()
        );
        // A typo in a MODELLED top-level key still fails loudly (top-level
        // deny_unknown_fields survives wrapper stripping).
        assert!(
            crate::parse_kafka_supervisor_spec(&json!({
                "type": "kafka",
                "dataSchemaX": {},
                "ioConfig": {"topic": "t", "consumerProperties": {}}
            }))
            .is_none()
        );
    }

    #[test]
    fn validate_rejects_non_runnable_and_bad_format() {
        // Bare tombstone/echo (no dataSchema/ioConfig) → not runnable.
        assert!(crate::validate_kafka_spec(&json!({"id": "wiki", "type": "kafka"})).is_err());
        // Unsupported timestamp format is rejected by validate().
        assert!(
            crate::validate_kafka_spec(&json!({
                "type": "kafka",
                "dataSchema": {
                    "dataSource": "events",
                    "timestampSpec": {"column": "ts", "format": "posix"},
                    "dimensionsSpec": {"dimensions": ["a"]}
                },
                "ioConfig": {"topic": "t", "consumerProperties": {"bootstrap.servers": "x:9092"}}
            }))
            .is_err()
        );
    }

    #[test]
    fn detects_kafka_type_and_suspension() {
        assert!(crate::is_kafka_typed(&flattened_spec()));
        assert!(!crate::is_kafka_typed(&json!({"type": "kinesis"})));
        // Suspend flag: bool (top-level and enveloped), Jackson-style string
        // coercion, and a LOUD reject of junk values (Fable audit).
        let flag = |v: &serde_json::Value| crate::kafka_suspended_flag(v);
        assert!(flag(&json!({"type": "kafka", "suspended": true})).expect("bool"));
        assert!(flag(&json!({"type": "kafka", "spec": {"suspended": true}})).expect("enveloped"));
        assert!(flag(&json!({"type": "kafka", "suspended": "true"})).expect("string true"));
        assert!(!flag(&json!({"type": "kafka", "suspended": "False"})).expect("string False"));
        assert!(!flag(&json!({"type": "kafka", "suspended": null})).expect("null = not suspended"));
        assert!(!flag(&flattened_spec()).expect("absent = not suspended"));
        assert!(flag(&json!({"type": "kafka", "suspended": "maybe"})).is_err());
        assert!(flag(&json!({"type": "kafka", "suspended": 1})).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repost_or_cross_type_replacement_of_running_is_refused() {
        let (metadata, historical, _dir) = setup().await;
        let overlord = crate::Overlord::with_executor(metadata, historical);
        let spec = json!({
            "id": "sup-x", "type": "kafka",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {
                "topic": "t",
                "consumerProperties": {"bootstrap.servers": "127.0.0.1:59092"},
                "useEarliestOffset": true
            }
        });
        overlord
            .create_supervisor(spec.clone())
            .await
            .expect("first create runs");

        // Reposting the SAME running Kafka supervisor is refused (would
        // duplicate rows via an earliest replay).
        let err = overlord
            .create_supervisor(spec.clone())
            .await
            .expect_err("repost of a running supervisor must be refused");
        assert!(format!("{err}").contains("already running"), "err = {err}");

        // A DIFFERENT-type spec reusing the id is also refused (would orphan
        // the running Kafka consumer) — and must not be persisted. Since
        // compat-5 a kinesis spec is itself runnable+validated, so use a
        // VALID one: the same-id LIVE-consumer guard must still fire FIRST
        // (the refusal is "already running", not a validation error).
        let kinesis = json!({
            "id": "sup-x", "type": "kinesis",
            "dataSchema": {
                "dataSource": "events_kinesis",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]}
            },
            "ioConfig": {"stream": "s"}
        });
        let err2 = overlord
            .create_supervisor(kinesis)
            .await
            .expect_err("cross-type replacement of a running supervisor must be refused");
        assert!(
            format!("{err2}").contains("already running"),
            "err = {err2}"
        );

        // After an explicit shutdown, re-creating the same id works again.
        overlord
            .shutdown_supervisor("sup-x")
            .await
            .expect("shutdown");
        overlord
            .create_supervisor(spec)
            .await
            .expect("re-create after shutdown");
        overlord
            .shutdown_supervisor("sup-x")
            .await
            .expect("final shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn id_less_kafka_spec_derives_stable_id_from_datasource() {
        let (metadata, historical, _dir) = setup().await;
        let overlord = crate::Overlord::with_executor(metadata, historical);
        // No `id`: Druid derives it from dataSchema.dataSource.
        let spec = json!({
            "type": "kafka",
            "dataSchema": {
                "dataSource": "events_ds",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {
                "topic": "t",
                "consumerProperties": {"bootstrap.servers": "127.0.0.1:59092"},
                "useEarliestOffset": true
            }
        });
        let id = overlord
            .create_supervisor(spec.clone())
            .await
            .expect("create");
        assert_eq!(
            id, "events_ds",
            "id-less kafka spec must use the datasource"
        );
        // Reposting the SAME id-less spec hits the live guard (no duplicate).
        assert!(
            overlord.create_supervisor(spec).await.is_err(),
            "id-less repost must be refused (stable derived id)"
        );
        overlord
            .shutdown_supervisor("events_ds")
            .await
            .expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn null_id_is_treated_as_omitted_and_derives_from_datasource() {
        let (metadata, historical, _dir) = setup().await;
        let overlord = crate::Overlord::with_executor(metadata, historical);
        // `"id": null` must be treated as omitted (not generate supervisor_N).
        let spec = json!({
            "id": null,
            "type": "kafka",
            "dataSchema": {
                "dataSource": "nulls_ds",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {
                "topic": "t",
                "consumerProperties": {"bootstrap.servers": "127.0.0.1:59092"},
                "useEarliestOffset": true
            }
        });
        let id = overlord.create_supervisor(spec).await.expect("create");
        assert_eq!(id, "nulls_ds", "null id must derive from datasource");
        overlord
            .shutdown_supervisor("nulls_ds")
            .await
            .expect("shutdown");
    }

    /// SEMANTICS CHANGED by Codex R26 F3 (was
    /// `recreate_after_shutdown_drops_prior_streaming_segments`): the rows
    /// here carry no `clusterId` (and the re-created consumer's identity is
    /// unresolvable — no broker), so the pre-start cleanup must now KEEP
    /// every one of them. Pre-R26 the bootstrap fallback claimed the
    /// same-pair rows; after a DNS repoint that same match could destroy
    /// rows the replay can never rebuild, so unconfirmed identity now means
    /// "keep + warn" (duplication over loss). The drop selectivity that
    /// this test used to pin (same-pair across ids / other-topic / batch)
    /// lives on under CONFIRMED identity in
    /// `drop_claims_same_cluster_rows_across_supervisor_ids_only`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recreate_with_unknown_cluster_identity_keeps_streaming_segments() {
        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        overlord
            .create_supervisor(flattened_spec())
            .await
            .expect("create");
        let owned_current = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "wiki-events",
                "127.0.0.1:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a"), row(2_000, "b")]),
        )
        .await
        .expect("owned segment (current id)");
        let owned_legacy = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "sup-old",
                "wiki-events",
                "127.0.0.1:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(3_000, "c")]),
        )
        .await
        .expect("owned segment (legacy id)");
        assert_eq!(queried_row_count(&historical, "events"), 3);

        // Shutdown keeps the data queryable (documented), …
        overlord
            .shutdown_supervisor("events")
            .await
            .expect("shutdown");
        assert_eq!(queried_row_count(&historical, "events"), 3);

        // … and RE-CREATING the supervisor — whose cluster identity cannot
        // be resolved (unroutable broker) — must keep them too (R26 F3):
        // neither row's cluster can be confirmed as THIS cluster, and a
        // wrong drop is permanent loss. The earliest replay may duplicate
        // them — the documented fail-safe residual.
        overlord
            .create_supervisor(flattened_spec())
            .await
            .expect("re-create");
        assert_eq!(
            queried_row_count(&historical, "events"),
            3,
            "unconfirmed-identity rows must be KEPT on re-create (R26 F3)"
        );
        assert!(
            metadata
                .segment_exists(&owned_current)
                .await
                .expect("exists"),
            "current-id streaming row must survive"
        );
        assert!(
            metadata
                .segment_exists(&owned_legacy)
                .await
                .expect("exists"),
            "legacy-id streaming row must survive"
        );
        let _ = overlord.shutdown_supervisor("events").await;
    }

    /// The R19 provenance SELECTIVITY under the R26 F3 rules: with the
    /// cluster identity CONFIRMED on both sides (`clusterId` equality), the
    /// cleanup drops exactly the same-(datasource, topic) streaming rows —
    /// regardless of which supervisor id published them — and never a
    /// different topic's row, a different cluster's row, or a batch row
    /// whose `taskId` merely coincides. (Formerly pinned through the
    /// overlord re-create path, which can no longer confirm identity in a
    /// brokerless test — see
    /// `recreate_with_unknown_cluster_identity_keeps_streaming_segments`.)
    #[tokio::test]
    async fn drop_claims_same_cluster_rows_across_supervisor_ids_only() {
        let (metadata, historical, _dir) = setup().await;
        let owned_current = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "wiki-events",
                "127.0.0.1:9092",
                Some("kc-A"),
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a"), row(2_000, "b")]),
        )
        .await
        .expect("owned segment (current id)");
        let owned_legacy = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "sup-old",
                "wiki-events",
                "127.0.0.1:9092",
                Some("kc-A"),
                "v2:fp-test",
            ),
            segment("events", &[row(3_000, "c")]),
        )
        .await
        .expect("owned segment (legacy id — R19: cross-id ownership)");
        let other_topic = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "other-topic",
                "127.0.0.1:9092",
                Some("kc-A"),
                "v2:fp-test",
            ),
            segment("events", &[row(4_000, "d")]),
        )
        .await
        .expect("other-topic segment");
        // A true BATCH row whose taskId COINCIDES with the supervisor id
        // (R19: replay cannot rebuild foreign data — must survive).
        let batch_seg = segment("events", &[row(5_000, "e")]);
        let batch_row = SegmentMetadataRow {
            id: "events_batch_1".to_string(),
            data_source: "events".to_string(),
            created_date: "2026-07-14T00:00:00.000Z".to_string(),
            start: "2023-11-14T00:00:00.000Z".to_string(),
            end: "2023-11-15T00:00:00.000Z".to_string(),
            version: "v1".to_string(),
            used: true,
            payload: json!({"dataSource": "events", "numRows": 1, "taskId": "events"}),
        };
        metadata
            .insert_segment(&batch_row)
            .await
            .expect("insert batch row");
        historical
            .replace_segments(
                &[],
                vec![SegmentSwapEntry {
                    id: batch_row.id.clone(),
                    data: Arc::new(batch_seg.segment_data),
                    datasource: Some("events".to_string()),
                }],
            )
            .expect("load batch row");
        assert_eq!(queried_row_count(&historical, "events"), 5);

        let dropped = drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "wiki-events",
            Some("kc-A"),
            "v2:fp-test",
        )
        .await
        .expect("drop");
        assert_eq!(dropped, 2, "exactly the same-pair, same-cluster rows");
        assert!(
            !metadata
                .segment_exists(&owned_current)
                .await
                .expect("exists"),
            "current-id streaming metadata row must be gone"
        );
        assert!(
            !metadata
                .segment_exists(&owned_legacy)
                .await
                .expect("exists"),
            "legacy-id streaming metadata row must be gone (R19 cross-id)"
        );
        assert!(
            metadata.segment_exists(&other_topic).await.expect("exists"),
            "another topic's streaming row must survive"
        );
        assert!(
            metadata
                .segment_exists(&batch_row.id)
                .await
                .expect("exists"),
            "the batch row must survive a coincidental taskId (R19 provenance)"
        );
        // 2 query-visible rows left: the other-topic row + the batch row
        // (the dropped segments held 3 of the original 5 rows).
        assert_eq!(queried_row_count(&historical, "events"), 2);
    }

    /// SEMANTICS CHANGED by Codex R26 F3 (was
    /// `resume_drops_prior_streaming_segments`): a resumed consumer whose
    /// cluster identity cannot be resolved (no broker) must KEEP the
    /// supervisor's clusterId-less prior rows — the pre-R26 bootstrap
    /// fallback dropped the owned one, which after a DNS repoint could be
    /// another cluster's unrebuildable data. Resume still starts the
    /// consumer; the earliest replay may duplicate (documented residual).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_with_unknown_cluster_identity_keeps_prior_streaming_segments() {
        let (metadata, historical, _dir) = setup().await;
        metadata
            .insert_supervisor("events", &flattened_spec())
            .await
            .expect("persist supervisor");
        publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "wiki-events",
                "127.0.0.1:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("owned segment");
        publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "other-task",
                "another-topic",
                "127.0.0.1:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(2_000, "b")]),
        )
        .await
        .expect("foreign segment (different topic)");

        let overlord = Arc::new(crate::Overlord::with_executor(
            Arc::clone(&metadata),
            Arc::clone(&historical),
        ));
        assert_eq!(
            Arc::clone(&overlord)
                .resume_kafka_supervisors()
                .await
                .expect("resume"),
            1
        );
        assert_eq!(
            queried_row_count(&historical, "events"),
            2,
            "unconfirmed-identity rows must be KEPT on resume (R26 F3)"
        );
        let _ = overlord.shutdown_supervisor("events").await;
    }

    /// Codex R24 F2 (now the ONLY matching rule — R26 F3): when BOTH the
    /// row and the (re)starting consumer carry a Kafka cluster id, that id
    /// ALONE decides the match — bootstrap is not consulted. Two rows on
    /// the same `bootstrap.servers` string can come from DIFFERENT clusters
    /// (a DNS repoint over time), and one cluster's rows can be stamped
    /// under a DIFFERENT bootstrap string (aliases / reordered lists /
    /// added brokers): a replay rebuilds exactly the rows of ITS cluster
    /// id, nothing else.
    #[tokio::test]
    async fn drop_cluster_id_decides_over_bootstrap() {
        let (metadata, historical, _dir) = setup().await;
        // Same cluster id as the consumer, same bootstrap → dropped.
        let same_both = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "sup-a",
                "topic-x",
                "cluster-a:9092",
                Some("kc-A"),
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish same-cluster row");
        // DIFFERENT cluster id on the SAME bootstrap string (DNS repointed
        // since) → must SURVIVE: this replay can never rebuild it.
        let repointed = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "sup-b",
                "topic-x",
                "cluster-a:9092",
                Some("kc-B"),
                "v2:fp-test",
            ),
            segment("events", &[row(2_000, "b")]),
        )
        .await
        .expect("publish repointed row");
        // SAME cluster id under a DIFFERENT bootstrap string (alias / added
        // broker) → dropped: the replay rebuilds it (bootstrap not
        // consulted).
        let aliased = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "sup-c",
                "topic-x",
                "alias-of-a:19092",
                Some("kc-A"),
                "v2:fp-test",
            ),
            segment("events", &[row(3_000, "c")]),
        )
        .await
        .expect("publish aliased row");
        assert_eq!(queried_row_count(&historical, "events"), 3);

        let dropped = drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "topic-x",
            Some("kc-A"),
            "v2:fp-test",
        )
        .await
        .expect("drop");
        assert_eq!(
            dropped, 2,
            "exactly the kc-A rows are rebuilt by the replay"
        );
        assert!(!metadata.segment_exists(&same_both).await.expect("exists"));
        assert!(
            !metadata.segment_exists(&aliased).await.expect("exists"),
            "same cluster id under another bootstrap alias must be dropped"
        );
        assert!(
            metadata.segment_exists(&repointed).await.expect("exists"),
            "a different cluster id must survive even on an identical bootstrap \
             string (dropping it would be permanent loss)"
        );
        assert_eq!(queried_row_count(&historical, "events"), 1);
    }

    /// Codex R24 F2, tightened by R26 F3: when the (re)starting consumer's
    /// cluster id could NOT be resolved, NO same-pair row may be claimed —
    /// neither a clusterId-stamped one (R24: bootstrap equality is not
    /// cluster identity) nor a bootstrap-only one (R26 F3: the pre-R26
    /// bootstrap fallback dropped it here, which after a DNS repoint could
    /// be another cluster's unrebuildable data). SEMANTICS CHANGED: the
    /// bootstrap-only row used to be dropped via the fallback; it now
    /// survives, warn-counted, and an earliest replay may duplicate it
    /// (documented residual — duplication over loss).
    #[tokio::test]
    async fn drop_keeps_cluster_id_rows_when_current_identity_unknown() {
        let (metadata, historical, _dir) = setup().await;
        let id_stamped = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "sup-a",
                "topic-x",
                "cluster-a:9092",
                Some("kc-A"),
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish clusterId-stamped row");
        let bootstrap_only = publish_streaming_segment(
            &metadata,
            &historical,
            &prov("sup-b", "topic-x", "cluster-a:9092", None, "v2:fp-test"),
            segment("events", &[row(2_000, "b")]),
        )
        .await
        .expect("publish bootstrap-only row");

        let dropped = drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "topic-x",
            None, // the current consumer's cluster id is UNKNOWN
            "v2:fp-test",
        )
        .await
        .expect("drop");
        assert_eq!(dropped, 0, "no row's identity can be confirmed → keep all");
        assert!(
            metadata.segment_exists(&id_stamped).await.expect("exists"),
            "a clusterId-stamped row must survive an unknown-identity drop \
             (fail-safe: bootstrap equality is not cluster identity)"
        );
        assert!(
            metadata
                .segment_exists(&bootstrap_only)
                .await
                .expect("exists"),
            "the bootstrap-only row must survive too (R26 F3: no fallback)"
        );
        assert_eq!(queried_row_count(&historical, "events"), 2);
    }

    /// Codex R21 finding 2 (fail-safe; subsumed by the R26 F3 rule that a
    /// clusterId-less row is never auto-dropped): a streaming row WITHOUT a
    /// stamped `bootstrap` (pre-provenance dev data — this branch is
    /// unreleased) has an unknowable cluster identity, so the cleanup must
    /// NOT claim it: never drop what the replay may not rebuild.
    #[tokio::test]
    async fn drop_skips_rows_without_bootstrap_provenance() {
        let (metadata, historical, _dir) = setup().await;
        // Simulate a pre-R21 row: kind+topic provenance, no bootstrap.
        let seg = segment("events", &[row(1_000, "a")]);
        let legacy_row = SegmentMetadataRow {
            id: "events_legacy_1".to_string(),
            data_source: "events".to_string(),
            created_date: "2026-07-14T00:00:00.000Z".to_string(),
            start: "2023-11-14T00:00:00.000Z".to_string(),
            end: "2023-11-15T00:00:00.000Z".to_string(),
            version: "v1".to_string(),
            used: true,
            payload: json!({
                "dataSource": "events",
                "numRows": 1,
                "taskId": "sup-old",
                "kind": "kafka-streaming",
                "topic": "topic-x",
            }),
        };
        metadata
            .insert_segment(&legacy_row)
            .await
            .expect("insert legacy row");
        historical
            .replace_segments(
                &[],
                vec![SegmentSwapEntry {
                    id: legacy_row.id.clone(),
                    data: Arc::new(seg.segment_data),
                    datasource: Some("events".to_string()),
                }],
            )
            .expect("load legacy row");

        let dropped = drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "topic-x",
            None,
            "v2:fp-test",
        )
        .await
        .expect("drop");
        assert_eq!(dropped, 0, "a bootstrap-less legacy row must not match");
        assert!(
            metadata
                .segment_exists(&legacy_row.id)
                .await
                .expect("exists"),
            "the legacy row must survive (fail-safe)"
        );
        assert_eq!(queried_row_count(&historical, "events"), 1);
    }

    /// Codex R20 finding 1: cancelling the CALLER's future mid-drop (an HTTP
    /// client disconnect drops the axum handler future at an await point)
    /// must not strand a half-state — with the R22 fail-closed order, the
    /// metadata-deleted / still-query-visible window between the commit and
    /// the Historical drop. The drop runs inside a spawned lifecycle op
    /// ([`run_lifecycle_op`], the R23 generalization) that survives caller
    /// cancellation, so once the metadata delete has begun the Historical
    /// drop always completes too — without any retry. (Both sides gone is
    /// asserted at the end; R22 removed the compensation, so completion of
    /// the spawned op is the ONLY thing upholding this.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drop_streaming_segments_survives_caller_cancellation() {
        use std::future::Future;
        use std::task::Poll;

        let (metadata, historical, _dir) = setup().await;
        // Several victims → the metadata delete transaction spans several
        // await points, so the cancellation window genuinely exists.
        let mut ids = Vec::new();
        for off in [1_000_i64, 2_000, 3_000] {
            ids.push(
                publish_streaming_segment(
                    &metadata,
                    &historical,
                    // clusterId stamped + matched below: post-R26-F3 the
                    // drop only ever claims identity-confirmed rows.
                    &prov(
                        "events",
                        "wiki-events",
                        "127.0.0.1:9092",
                        Some("kc-A"),
                        "v2:fp-test",
                    ),
                    segment("events", &[row(off, "a")]),
                )
                .await
                .expect("publish"),
            );
        }
        assert_eq!(queried_row_count(&historical, "events"), 3);

        // Drive the drop future BY HAND and abandon it at an observable
        // mid-flight point: the query-visible rows are already gone (with
        // the R22 order that means the spawned op finished its work) but
        // the CALLER's future is still pending — a deterministic model of a
        // caller cancelled at that await point. If no such window shows up,
        // completing before it also upholds the invariant.
        let lifecycle_ops = Mutex::new(Vec::new());
        {
            let op_metadata = Arc::clone(&metadata);
            let op_historical = Arc::clone(&historical);
            let mut fut = Box::pin(run_lifecycle_op(
                &lifecycle_ops,
                "the streaming-segment drop op",
                async move {
                    drop_streaming_segments_task(
                        &op_metadata,
                        &op_historical,
                        "events",
                        "wiki-events",
                        Some("kc-A"),
                        "v2:fp-test",
                    )
                    .await
                },
            ));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let step = std::future::poll_fn(|cx| {
                    Poll::Ready(match fut.as_mut().poll(cx) {
                        Poll::Ready(r) => Some(r),
                        Poll::Pending => None,
                    })
                })
                .await;
                match step {
                    Some(result) => {
                        // Ran to completion before the hostile window became
                        // observable — nothing left to cancel; completion
                        // itself upholds the invariant.
                        result.expect("drop failed");
                        eprintln!("cancellation test: drop completed before the window");
                        break;
                    }
                    None if queried_row_count(&historical, "events") == 0 => {
                        eprintln!("cancellation test: cancelling mid-flight");
                        break; // query-visible drop happened → CANCEL here
                    }
                    None => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "drop future made no observable progress"
                        );
                        tokio::task::yield_now().await;
                    }
                }
            }
            // `fut` dropped here == the caller was cancelled.
        }

        // The drop had already begun, so it must still run to COMPLETION:
        // both sides end dropped, with no retry issued by anyone.
        let mut consistent = false;
        for _ in 0..250_u32 {
            let mut meta_rows = 0_usize;
            for id in &ids {
                if metadata.segment_exists(id).await.expect("exists") {
                    meta_rows += 1;
                }
            }
            if meta_rows == 0 && queried_row_count(&historical, "events") == 0 {
                consistent = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            consistent,
            "cancelled drop stranded an inconsistent half-state: the spawned task \
             must always finish BOTH the metadata delete and the query-visible drop"
        );
    }

    /// Codex R22 finding 2: a caller cancelled while awaiting its spawned
    /// lifecycle op (R23: the whole cleanup→persist→start→register tail)
    /// releases the lifecycle lock, but the op keeps running as an ORPHAN.
    /// The NEXT lifecycle operation must WAIT for that orphan (the
    /// `kafka_lifecycle_ops` drain) before proceeding — otherwise a new
    /// consumer for the same (datasource, topic, bootstrap) can start
    /// publishing and the still-running orphan's cleanup then drops the NEW
    /// rows, whose offsets are already consumed (never re-ingested = silent
    /// permanent loss).
    ///
    /// Deterministic model: the orphan is parked on the `events` publish
    /// lock (held by the test), and the follow-up create targets a
    /// DIFFERENT datasource+topic — so nothing but the drain can make it
    /// wait. Pre-fix, create #2 completes while the orphan has not even
    /// scanned its victims (the lock is still held) — the lifecycle
    /// serialization was escaped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_ops_drain_orphaned_cleanup_before_proceeding() {
        use std::future::Future;
        use std::task::Poll;

        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // One prior streaming row for (events, wiki-events). The orphan's
        // cleanup parks on this datasource's publish lock; the row itself
        // SURVIVES the whole test (R26 F3: the re-created consumer's
        // cluster identity is unresolvable here, so nothing may be
        // claimed) — completion is witnessed by the registered consumer.
        let old = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "wiki-events",
                "127.0.0.1:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish prior row");

        // Park the (future) cleanup: hold the events publish lock, so the
        // spawned task blocks at its first step and stays in flight.
        let publish_lock = metadata.datasource_publish_lock("events").await;
        let gate = publish_lock.lock().await;

        // Create #1 (earliest, events/wiki-events): drive BY HAND until its
        // cleanup task is spawned + registered, then DROP the future — the
        // modelled client disconnect. The lifecycle lock is released; the
        // cleanup lives on as an orphan blocked on `gate`.
        {
            let mut fut = Box::pin(overlord.create_supervisor(flattened_spec()));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let done =
                    std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_ready())).await;
                assert!(
                    !done,
                    "create #1 must not complete while its cleanup is gated on the \
                     events publish lock"
                );
                if overlord.kafka_lifecycle_ops.lock().await.len() == 1 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "create #1 never registered its cleanup task"
                );
                tokio::task::yield_now().await;
            }
            // `fut` dropped here == the caller was cancelled mid-cleanup.
        }

        // Create #2 for a DIFFERENT (datasource, topic): it shares no
        // publish lock with the orphan, so only the registry drain can
        // (and must) make it wait.
        let other = json!({
            "type": "kafka",
            "dataSchema": {
                "dataSource": "other_ds",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {
                "topic": "other-topic",
                "consumerProperties": {"bootstrap.servers": "127.0.0.1:59092"},
                "useEarliestOffset": true
            }
        });
        let mut fut2 = Box::pin(overlord.create_supervisor(other));
        for _ in 0..25_u32 {
            let done =
                std::future::poll_fn(|cx| Poll::Ready(fut2.as_mut().poll(cx).is_ready())).await;
            assert!(
                !done,
                "create #2 completed while the orphaned cleanup was still running \
                 (R22: the lifecycle serialization was escaped — a new consumer \
                 could publish rows the orphan then drops)"
            );
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        // The orphan is still gated: its victim must still exist.
        assert!(
            metadata.segment_exists(&old).await.expect("exists"),
            "the orphan must not have run while the publish lock is held"
        );

        // Release the gate → the orphan completes → the drain unblocks
        // create #2, which must then run to completion.
        drop(gate);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let id2 = loop {
            let step = std::future::poll_fn(|cx| {
                Poll::Ready(match fut2.as_mut().poll(cx) {
                    Poll::Ready(r) => Some(r),
                    Poll::Pending => None,
                })
            })
            .await;
            match step {
                Some(result) => break result.expect("create #2 succeeds after the drain"),
                None => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "create #2 never completed after the orphan was released"
                    );
                    tokio::task::yield_now().await;
                }
            }
        };
        assert_eq!(id2, "other_ds");
        // Ordering witness: by the time create #2 completed, the orphan had
        // finished its WHOLE job — its LAST step is registering the events
        // consumer. (The prior row is untouched: R26 F3 keeps it under an
        // unconfirmed cluster identity.)
        assert!(
            overlord
                .kafka_supervisors
                .lock()
                .await
                .contains_key("events"),
            "the orphaned op must have completed (registered its consumer) \
             before create #2 proceeded"
        );
        assert!(
            metadata.segment_exists(&old).await.expect("exists"),
            "R26 F3: the unconfirmed-identity row must survive the orphan's cleanup"
        );
        assert_eq!(queried_row_count(&historical, "events"), 1);
        // R23: the orphaned WHOLE-op also persisted + registered the events
        // consumer after the gate lifted — stop it too.
        let _ = overlord.shutdown_supervisor("events").await;
        let _ = overlord.shutdown_supervisor("other_ds").await;
    }

    /// Codex R24 F1: the registry DRAIN is itself a cancellation surface.
    /// Pre-fix it `mem::take`-emptied the registry BEFORE awaiting the
    /// handles, so a caller cancelled MID-DRAIN (client disconnect while
    /// waiting out an orphan) detached every still-running op from the
    /// registry — the NEXT lifecycle operation then drained an empty Vec and
    /// sailed through while the orphan was still in flight: the exact R22
    /// serialization escape, reintroduced one level up (e.g. a stale LATEST
    /// op registering last and replacing the replaying consumer).
    ///
    /// Deterministic model: op #1 is parked on the events publish lock
    /// (orphaned create), create #2 is cancelled while ITS drain awaits that
    /// orphan, and create #3 (a third, disjoint pair — no shared publish
    /// lock, no shared (datasource, topic)) must STILL be gated by the
    /// orphan; only the drain can make it wait. Pre-fix, create #3 completes
    /// while the orphan has not even scanned its victims.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_drain_keeps_orphaned_ops_gating_later_lifecycle_ops() {
        use std::future::Future;
        use std::task::Poll;

        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // A prior streaming row: the orphan parks on its datasource's
        // publish lock. The row survives (R26 F3, unconfirmed identity);
        // the registered consumer is the completion witness.
        let old = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "wiki-events",
                "127.0.0.1:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish prior row");

        // Park the (future) op #1 on the events publish lock.
        let publish_lock = metadata.datasource_publish_lock("events").await;
        let gate = publish_lock.lock().await;

        // Create #1: cancel the caller once its op is registered — the op
        // lives on as an orphan blocked on `gate`.
        {
            let mut fut = Box::pin(overlord.create_supervisor(flattened_spec()));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let done =
                    std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_ready())).await;
                assert!(!done, "create #1 must not complete while gated");
                if overlord.kafka_lifecycle_ops.lock().await.len() == 1 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "create #1 never registered its lifecycle op"
                );
                tokio::task::yield_now().await;
            }
        }

        // Create #2 (disjoint pair): poll it INTO its drain — it parks on
        // the orphan — then cancel it MID-DRAIN.
        {
            let mut fut2 = Box::pin(overlord.create_supervisor(pair_spec("other_ds", "other-t")));
            for _ in 0..25_u32 {
                let done =
                    std::future::poll_fn(|cx| Poll::Ready(fut2.as_mut().poll(cx).is_ready())).await;
                assert!(
                    !done,
                    "create #2 must be parked in its drain while the orphan runs"
                );
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            // `fut2` dropped here == the DRAIN ITSELF was cancelled mid-await.
        }

        // Create #3 (yet another disjoint pair) must STILL be gated by the
        // orphan. Pre-fix the cancelled drain had emptied the registry and
        // detached the orphan, so create #3 completes here — RED.
        let mut fut3 = Box::pin(overlord.create_supervisor(pair_spec("third_ds", "third-t")));
        for _ in 0..100_u32 {
            let done =
                std::future::poll_fn(|cx| Poll::Ready(fut3.as_mut().poll(cx).is_ready())).await;
            assert!(
                !done,
                "create #3 completed while the orphaned create #1 op was still in \
                 flight (R24 F1: a cancelled drain must not detach registered ops)"
            );
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        // The orphan is still gated: its victim must still exist.
        assert!(
            metadata.segment_exists(&old).await.expect("exists"),
            "the orphan must not have run while the publish lock is held"
        );

        // Release the gate → the orphan completes → create #3 completes.
        drop(gate);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let id3 = loop {
            let step = std::future::poll_fn(|cx| {
                Poll::Ready(match fut3.as_mut().poll(cx) {
                    Poll::Ready(r) => Some(r),
                    Poll::Pending => None,
                })
            })
            .await;
            match step {
                Some(result) => break result.expect("create #3 succeeds after the drain"),
                None => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "create #3 never completed after the orphan was released"
                    );
                    tokio::task::yield_now().await;
                }
            }
        };
        assert_eq!(id3, "third_ds");
        // Ordering witness: the orphan finished its whole job first — its
        // last step registers the events consumer. (The prior row survives:
        // R26 F3 keeps unconfirmed-identity rows.)
        assert!(
            overlord
                .kafka_supervisors
                .lock()
                .await
                .contains_key("events"),
            "the orphaned op must have completed before create #3 proceeded"
        );
        assert!(
            metadata.segment_exists(&old).await.expect("exists"),
            "R26 F3: the unconfirmed-identity row must survive the orphan's cleanup"
        );
        // The orphan registered the events consumer; create #2 never got far
        // enough to persist anything (its op was never spawned).
        let _ = overlord.shutdown_supervisor("events").await;
        assert!(
            overlord.shutdown_supervisor("other_ds").await.is_err(),
            "the cancelled create #2 must not have persisted its spec"
        );
        let _ = overlord.shutdown_supervisor("third_ds").await;
    }

    /// Codex R22 finding 1: the cleanup must be FAIL-CLOSED — the metadata
    /// rows are deleted FIRST (one transaction), and only then are the rows
    /// dropped from the query-visible [`Historical`]. The reverse order
    /// (Historical first + compensating re-ADD on a metadata failure) had a
    /// real failure mode: the compensation is an ADD and thus subject to
    /// cache-limit admission, so another datasource consuming the freed
    /// cache between the drop and a failed delete made the compensation
    /// itself fail, sticking the half-state.
    ///
    /// Pinned structurally: the task body is driven BY HAND (not spawned),
    /// so between polls nothing can advance it — any `Pending` is a genuine
    /// intermediate state, and at every one of them the rows must still be
    /// query-visible (the Historical drop is synchronous AFTER the commit,
    /// with no await point in between, so a mid-flight vanish is exactly the
    /// forbidden old order).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drop_deletes_metadata_before_dropping_query_visible_rows() {
        use std::future::Future;
        use std::task::Poll;

        let (metadata, historical, _dir) = setup().await;
        let mut ids = Vec::new();
        for off in [1_000_i64, 2_000, 3_000] {
            ids.push(
                publish_streaming_segment(
                    &metadata,
                    &historical,
                    // clusterId stamped + matched below (post-R26-F3 rule).
                    &prov(
                        "events",
                        "wiki-events",
                        "127.0.0.1:9092",
                        Some("kc-A"),
                        "v2:fp-test",
                    ),
                    segment("events", &[row(off, "a")]),
                )
                .await
                .expect("publish"),
            );
        }
        assert_eq!(queried_row_count(&historical, "events"), 3);

        let mut fut = Box::pin(drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "wiki-events",
            Some("kc-A"),
            "v2:fp-test",
        ));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let step = std::future::poll_fn(|cx| {
                Poll::Ready(match fut.as_mut().poll(cx) {
                    Poll::Ready(r) => Some(r),
                    Poll::Pending => None,
                })
            })
            .await;
            match step {
                Some(result) => {
                    assert_eq!(result.expect("drop"), 3);
                    break;
                }
                None => {
                    assert_eq!(
                        queried_row_count(&historical, "events"),
                        3,
                        "rows vanished from the query-visible Historical while the \
                         cleanup was still pending: the Historical drop ran BEFORE \
                         the metadata delete committed (R22 fail-closed order)"
                    );
                    assert!(
                        std::time::Instant::now() < deadline,
                        "drop future made no observable progress"
                    );
                    tokio::task::yield_now().await;
                }
            }
        }
        // Completion drops BOTH sides.
        assert_eq!(queried_row_count(&historical, "events"), 0);
        for id in &ids {
            assert!(
                !metadata.segment_exists(id).await.expect("exists"),
                "metadata row '{id}' must be gone after the drop"
            );
        }
    }

    /// Codex R20 finding 2 (create path): `useEarliestOffset:false` — the
    /// spec DEFAULT — consumes from the topic TAIL. A re-created consumer
    /// never redelivers the old records, so the prior segments cannot
    /// duplicate AND cannot be rebuilt: dropping them is permanent data
    /// loss. They must be KEPT.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recreate_with_latest_offset_keeps_streaming_segments() {
        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        overlord
            .create_supervisor(latest_spec())
            .await
            .expect("create");
        let seg = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "wiki-events",
                "127.0.0.1:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a"), row(2_000, "b")]),
        )
        .await
        .expect("owned segment");
        overlord
            .shutdown_supervisor("events")
            .await
            .expect("shutdown");

        overlord
            .create_supervisor(latest_spec())
            .await
            .expect("re-create");
        assert_eq!(
            queried_row_count(&historical, "events"),
            2,
            "a latest (tail) consumer never replays the old records: prior \
             streaming segments must be KEPT (dropping them = permanent loss)"
        );
        assert!(
            metadata.segment_exists(&seg).await.expect("exists"),
            "the metadata row must survive a latest re-create"
        );
        let _ = overlord.shutdown_supervisor("events").await;
    }

    /// Codex R20 finding 2 (resume path): same as the create path — a
    /// resumed `useEarliestOffset:false` consumer starts at the tail, so
    /// the supervisor's prior segments must be kept, not dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_with_latest_offset_keeps_streaming_segments() {
        let (metadata, historical, _dir) = setup().await;
        metadata
            .insert_supervisor("events", &latest_spec())
            .await
            .expect("persist supervisor");
        let seg = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "wiki-events",
                "127.0.0.1:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("owned segment");

        let overlord = Arc::new(crate::Overlord::with_executor(
            Arc::clone(&metadata),
            Arc::clone(&historical),
        ));
        assert_eq!(
            Arc::clone(&overlord)
                .resume_kafka_supervisors()
                .await
                .expect("resume"),
            1
        );
        assert_eq!(
            queried_row_count(&historical, "events"),
            1,
            "a latest resume must keep the prior streaming segment"
        );
        assert!(metadata.segment_exists(&seg).await.expect("exists"));
        let _ = overlord.shutdown_supervisor("events").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_dedups_legacy_rows_by_datasource_topic() {
        // Fable audit: pre-branch binaries persisted a fresh synthetic
        // `supervisor_N` row per id-less POST. Two legacy rows holding the
        // SAME (datasource, topic) spec must not EACH spawn a consumer on
        // resume — that silently doubles every stored record.
        let (metadata, historical, _dir) = setup().await;
        metadata
            .insert_supervisor("supervisor_1", &flattened_spec())
            .await
            .expect("legacy row 1");
        metadata
            .insert_supervisor("supervisor_2", &flattened_spec())
            .await
            .expect("legacy row 2");
        let overlord = Arc::new(crate::Overlord::with_executor(metadata, historical));
        let started = Arc::clone(&overlord)
            .resume_kafka_supervisors()
            .await
            .expect("resume");
        assert_eq!(
            started, 1,
            "one (datasource, topic) pair must spawn exactly one consumer"
        );
        let _ = overlord.shutdown_supervisor("supervisor_1").await;
        let _ = overlord.shutdown_supervisor("supervisor_2").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_refuses_cross_id_duplicate_consumer() {
        // Fable audit: with a LEGACY consumer running under a synthetic id,
        // an id-less repost derives a DIFFERENT spec_id, slips past the
        // same-id live-guard, and starts a SECOND consumer for the same
        // (datasource, topic) — double ingestion. The guard must check the
        // pair across ids.
        let (metadata, historical, _dir) = setup().await;
        metadata
            .insert_supervisor("supervisor_legacy", &flattened_spec())
            .await
            .expect("legacy row");
        let overlord = Arc::new(crate::Overlord::with_executor(metadata, historical));
        assert_eq!(
            Arc::clone(&overlord)
                .resume_kafka_supervisors()
                .await
                .expect("resume"),
            1
        );
        let err = overlord
            .create_supervisor(flattened_spec())
            .await
            .expect_err("cross-id duplicate consumer must be refused");
        assert!(
            format!("{err}").contains("supervisor_legacy"),
            "the refusal must name the running consumer: {err}"
        );
        let _ = overlord.shutdown_supervisor("supervisor_legacy").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_prefers_derived_id_over_synthetic() {
        // When both a synthetic legacy row and the datasource-derived row
        // exist for one (datasource, topic), resume must prefer the DERIVED
        // id ('events') even when the synthetic id sorts first.
        let (metadata, historical, _dir) = setup().await;
        metadata
            .insert_supervisor("aaa_legacy", &flattened_spec())
            .await
            .expect("synthetic row (sorts before 'events')");
        metadata
            .insert_supervisor("events", &flattened_spec())
            .await
            .expect("derived row");
        let overlord = Arc::new(crate::Overlord::with_executor(metadata, historical));
        assert_eq!(
            Arc::clone(&overlord)
                .resume_kafka_supervisors()
                .await
                .expect("resume"),
            1
        );
        // The live consumer must be the derived one: reposting the id-less
        // spec derives 'events' and must hit the SAME-id guard (which never
        // mentions the synthetic id).
        let err = overlord
            .create_supervisor(flattened_spec())
            .await
            .expect_err("pair is live");
        assert!(
            !format!("{err}").contains("aaa_legacy"),
            "derived id must be the live one, not the synthetic: {err}"
        );
        let _ = overlord.shutdown_supervisor("events").await;
        let _ = overlord.shutdown_supervisor("aaa_legacy").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn suspended_string_flag_is_honored_not_ignored() {
        // "suspended": "true" as a JSON STRING (shell/YAML templating output)
        // must be coerced like Druid's Jackson scalar coercion — NOT silently
        // treated as running (Fable audit). Observable: a suspended create
        // starts no consumer, so an identical repost is NOT refused as
        // "already running", and resume skips the spec.
        let (metadata, historical, _dir) = setup().await;
        let overlord = Arc::new(crate::Overlord::with_executor(metadata, historical));
        let mut spec = flattened_spec();
        let obj = spec.as_object_mut().expect("object");
        obj.insert("id".into(), json!("s-str"));
        obj.insert("suspended".into(), json!("true"));
        overlord
            .create_supervisor(spec.clone())
            .await
            .expect("suspended-by-string create persists");
        overlord
            .create_supervisor(spec)
            .await
            .expect("suspended repost must not be blocked by a live consumer");
        let started = Arc::clone(&overlord)
            .resume_kafka_supervisors()
            .await
            .expect("resume");
        assert_eq!(started, 0, "suspended-by-string spec must not resume");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_refuses_persisted_pair_claim_without_live_handle() {
        // Codex R25: right after a restart (resume not yet run) there are NO
        // live handles, so the live pair guard sees nothing. The PERSISTED
        // layer must still refuse a new id for an occupied (datasource,
        // topic) pair — otherwise the next resume prefers the derived id
        // and warn-skips the newly created supervisor: silently disabled,
        // nobody ever consumes its records.
        let (metadata, historical, _dir) = setup().await;
        metadata
            .insert_supervisor("supervisor_legacy", &flattened_spec())
            .await
            .expect("persisted row (no live handle: models a fresh restart)");
        let overlord = crate::Overlord::with_executor(metadata, historical);
        let err = overlord
            .create_supervisor(flattened_spec())
            .await
            .expect_err("a persisted pair claim must be refused even without a live handle");
        assert!(
            format!("{err}").contains("supervisor_legacy"),
            "the refusal must name the persisted owner: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn suspended_supervisor_still_claims_its_pair() {
        // R25 judgment call: a SUSPENDED supervisor still owns its
        // (datasource, topic) pair. Suspension is a reversible pause —
        // Druid's suspend/resume APIs keep the supervisor registered — so
        // handing the pair to another id would turn the later resume into a
        // refusal trap. Shutting the supervisor down (tombstone) is the
        // loud, explicit way to release the pair.
        let (metadata, historical, _dir) = setup().await;
        let overlord = crate::Overlord::with_executor(metadata, historical);
        let mut suspended = flattened_spec();
        suspended["id"] = json!("first");
        suspended["suspended"] = json!(true);
        overlord
            .create_supervisor(suspended)
            .await
            .expect("suspended create persists");

        let mut second = flattened_spec();
        second["id"] = json!("second");
        let err = overlord
            .create_supervisor(second.clone())
            .await
            .expect_err("a suspended supervisor's pair must not be handed to a new id");
        assert!(
            format!("{err}").contains("first"),
            "the refusal must name the suspended owner: {err}"
        );

        // Shutdown (tombstone) releases the pair.
        overlord
            .shutdown_supervisor("first")
            .await
            .expect("shutdown first");
        overlord
            .create_supervisor(second)
            .await
            .expect("a tombstoned pair must be free for a new id");
        let _ = overlord.shutdown_supervisor("second").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tombstoned_supervisor_frees_its_pair_for_a_new_id() {
        // R25: shutdown tombstones the row (its latest generation has no
        // `type`), and a tombstone does NOT claim its former (datasource,
        // topic) pair — a NEW id may take it over afterwards.
        let (metadata, historical, _dir) = setup().await;
        let overlord = crate::Overlord::with_executor(metadata, historical);
        let mut first = flattened_spec();
        first["id"] = json!("first");
        overlord
            .create_supervisor(first)
            .await
            .expect("create first");
        overlord
            .shutdown_supervisor("first")
            .await
            .expect("shutdown first");
        let mut second = flattened_spec();
        second["id"] = json!("second");
        overlord
            .create_supervisor(second)
            .await
            .expect("a tombstoned pair must be free for a new supervisor id");
        let _ = overlord.shutdown_supervisor("second").await;
    }

    /// Codex R23 (HIGH): an EARLIEST create whose caller is cancelled while
    /// the operation is in flight must not be able to complete the pre-start
    /// cleanup (existing rows deleted) WITHOUT also reaching the consumer
    /// start — the deleted rows are only ever rebuilt by that earliest
    /// replay. Pre-fix, the cleanup ran in a spawned (uncancellable) task
    /// but persist + start + register still ran in the CALLER: a cancel at
    /// the boundary left the rows deleted with no consumer and no spec row,
    /// and a follow-up LATEST create for the same pair then consumed from
    /// the topic tail — the deleted rows were rebuilt by nobody (silent
    /// permanent loss). Post-fix the WHOLE runnable tail (cleanup → persist
    /// → start → register) is ONE spawned lifecycle op: dropping the caller
    /// future at ANY await point still runs it to completion, so the class
    /// is gone by construction (structural witness — there is no
    /// caller-side await boundary left to cancel at).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_create_op_completes_cleanup_persist_and_register() {
        use std::future::Future;
        use std::task::Poll;

        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // A prior streaming row. The cleanup parks on its datasource's
        // publish lock; under R26 F3 the row itself is KEPT (the consumer's
        // cluster identity is unresolvable here), so the R23 witnesses are
        // the registered consumer + the persisted spec.
        let old = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "wiki-events",
                "127.0.0.1:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish prior row");

        // Park the op at its first step (the earliest cleanup takes the
        // events publish lock), so the caller is deterministically dropped
        // while the op is still IN FLIGHT.
        let publish_lock = metadata.datasource_publish_lock("events").await;
        let gate = publish_lock.lock().await;

        {
            let mut fut = Box::pin(overlord.create_supervisor(flattened_spec()));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let done =
                    std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_ready())).await;
                assert!(
                    !done,
                    "create must not complete while its op is gated on the \
                     events publish lock"
                );
                if overlord.kafka_lifecycle_ops.lock().await.len() == 1 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "create never registered its lifecycle op"
                );
                tokio::task::yield_now().await;
            }
            // `fut` dropped here == the caller was cancelled mid-operation.
        }
        drop(gate);

        // The whole tail must still complete: the earliest consumer
        // registered AND the real spec persisted — the replay-capable state
        // in which nothing is lost.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let live = {
                let sups = overlord.kafka_supervisors.lock().await;
                sups.get("events").is_some_and(|h| !h.is_finished())
            };
            if live {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "R23: the cancelled create never registered its consumer — the \
                 cleanup ran as an orphan and the deleted rows have no \
                 replaying consumer to rebuild them (permanent loss)"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            metadata.segment_exists(&old).await.expect("exists"),
            "R26 F3: the unconfirmed-identity row must survive the cleanup"
        );
        let persisted = overlord
            .get_supervisor("events")
            .await
            .expect("get")
            .expect("the spec row must be persisted despite the cancel");
        assert!(
            crate::is_kafka_typed(&persisted),
            "the REAL spec must be persisted, not a tombstone: {persisted}"
        );

        // The R23 loss sequence is now impossible: a follow-up LATEST create
        // for the same pair cannot land on a torn state — the earliest
        // consumer is live, so the repost is refused.
        let err = overlord
            .create_supervisor(latest_spec())
            .await
            .expect_err("the pair is live; a latest repost must be refused");
        assert!(format!("{err}").contains("already running"), "err = {err}");
        let _ = overlord.shutdown_supervisor("events").await;
    }

    /// The lifecycle-op registry drain must also gate `shutdown_supervisor`
    /// (Codex R23): with the whole create tail in a spawned op, a cancelled
    /// caller can leave the op still running. A shutdown arriving in that
    /// window must WAIT for the op — otherwise it observes "supervisor not
    /// found" (the op has not persisted yet) and returns, after which the op
    /// registers a consumer that nothing will ever stop, running against a
    /// spec row that was never tombstoned.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_in_flight_lifecycle_op() {
        use std::future::Future;
        use std::task::Poll;

        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        let old = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "wiki-events",
                "127.0.0.1:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish prior row");

        // Gate the op on the events publish lock, cancel the create caller
        // once the op is registered (same model as the create test above).
        let publish_lock = metadata.datasource_publish_lock("events").await;
        let gate = publish_lock.lock().await;
        {
            let mut fut = Box::pin(overlord.create_supervisor(flattened_spec()));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let done =
                    std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_ready())).await;
                assert!(!done, "create must not complete while gated");
                if overlord.kafka_lifecycle_ops.lock().await.len() == 1 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "create never registered its lifecycle op"
                );
                tokio::task::yield_now().await;
            }
        }

        // Shutdown must WAIT for the in-flight op (registry drain) — not
        // fail "not found" and let the op register an unstoppable consumer.
        let mut fut = Box::pin(overlord.shutdown_supervisor("events"));
        for _ in 0..25_u32 {
            let done =
                std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_ready())).await;
            assert!(
                !done,
                "shutdown completed while the create op was still in flight \
                 (R23: it must drain the registry first)"
            );
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        drop(gate);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let step = std::future::poll_fn(|cx| {
                Poll::Ready(match fut.as_mut().poll(cx) {
                    Poll::Ready(r) => Some(r),
                    Poll::Pending => None,
                })
            })
            .await;
            match step {
                Some(result) => {
                    result.expect("shutdown succeeds after the drain");
                    break;
                }
                None => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "shutdown never completed after the op was released"
                    );
                    tokio::task::yield_now().await;
                }
            }
        }

        // The op's consumer was registered, then stopped BY this shutdown.
        assert!(
            overlord.kafka_supervisors.lock().await.is_empty(),
            "the drained op's consumer must have been stopped by shutdown"
        );
        // The spec row ends tombstoned (suspended), not live.
        let spec = overlord
            .get_supervisor("events")
            .await
            .expect("get")
            .expect("row");
        assert_eq!(
            spec.get("suspended").and_then(serde_json::Value::as_bool),
            Some(true),
            "shutdown must leave the tombstone as the latest spec: {spec}"
        );
        // And the op ran to completion before shutdown proceeded; its
        // cleanup KEPT the unconfirmed-identity row (R26 F3).
        assert!(metadata.segment_exists(&old).await.expect("exists"));
    }

    /// Codex R24 F3, transition order updated by R26 F2: the shutdown
    /// transition — now deregister → stop(drain) → tombstone persist — must
    /// ride a spawned lifecycle op so a cancelled caller cannot tear it.
    /// The R24-era hostile window (tombstone committed, consumer still
    /// running = a GHOST publishing against a suspended spec) no longer
    /// exists by construction: the persist comes LAST, and the crash-shape
    /// of the new order (stopped but not yet persisted) recovers by replay
    /// (active metadata). What remains to pin: a caller cancelled right
    /// after the op registers must still get the WHOLE transition —
    /// consumer stopped + deregistered AND (the drain here succeeding —
    /// empty buffer) the tombstone persisted.
    ///
    /// Deterministic model: the test holds the `kafka_supervisors`
    /// registration lock, so the transition parks at its FIRST step (the
    /// handle remove); the caller is dropped once the op is registered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_shutdown_still_stops_the_consumer() {
        use std::future::Future;
        use std::task::Poll;

        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        overlord
            .create_supervisor(flattened_spec())
            .await
            .expect("create");
        assert!(!overlord.kafka_supervisors.lock().await.is_empty());
        // Reap the completed create op so the registry marker below can
        // only be the shutdown's own op.
        overlord.drain_kafka_lifecycle_ops().await;
        assert!(overlord.kafka_lifecycle_ops.lock().await.is_empty());

        // Park the transition at its first step (the handle remove).
        let gate = overlord.kafka_supervisors.lock().await;
        {
            let mut fut = Box::pin(overlord.shutdown_supervisor("events"));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let done =
                    std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_ready())).await;
                assert!(
                    !done,
                    "shutdown must not complete while the registration lock is held"
                );
                if overlord.kafka_lifecycle_ops.lock().await.len() == 1 {
                    break; // op registered → cancel the caller NOW
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the shutdown never registered its lifecycle op"
                );
                tokio::task::yield_now().await;
            }
            // `fut` dropped here == the caller was cancelled mid-transition.
        }
        drop(gate);

        // The transition must still run to completion: the consumer is
        // stopped and deregistered (no ghost), …
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if overlord.kafka_supervisors.lock().await.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "R24 F3: the cancelled shutdown left a GHOST consumer — the \
                 consumer was never stopped"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // … and the tombstone lands AFTER the successful drain (R26 F2
        // order: stop first, persist second) — poll for it.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let tombstoned = overlord
                .get_supervisor("events")
                .await
                .expect("get")
                .expect("row")
                .get("suspended")
                .and_then(serde_json::Value::as_bool)
                == Some(true);
            if tombstoned {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the tombstone must be persisted once the drain succeeded"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// Codex R24 F3 (suspend seam): an explicit suspend (`suspended:true`
    /// repost) of a RUNNING consumer is the same persist → deregister →
    /// stop transition as shutdown and must ride a spawned lifecycle op
    /// too. Structural witness: the suspend must REGISTER a lifecycle op
    /// (pre-fix it never does — the transition runs caller-cancellable and
    /// this loop sees the future complete with the registry still empty);
    /// cancelling the caller right after registration must still end with
    /// the consumer stopped, the handle deregistered, and the suspended
    /// spec persisted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_suspend_create_still_stops_the_consumer() {
        use std::future::Future;
        use std::task::Poll;

        let (metadata, historical, _dir) = setup().await;
        let overlord = Arc::new(crate::Overlord::with_executor(
            Arc::clone(&metadata),
            Arc::clone(&historical),
        ));
        overlord
            .create_supervisor(flattened_spec())
            .await
            .expect("create");
        // Reap the completed create op so the registry marker below can
        // only be the suspend's own op.
        overlord.drain_kafka_lifecycle_ops().await;
        assert!(overlord.kafka_lifecycle_ops.lock().await.is_empty());

        let mut suspend_spec = flattened_spec();
        suspend_spec["suspended"] = json!(true);
        {
            let mut fut = Box::pin(overlord.create_supervisor(suspend_spec));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let done =
                    std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_ready())).await;
                if overlord.kafka_lifecycle_ops.lock().await.len() == 1 {
                    break; // registered → cancel the caller NOW
                }
                assert!(
                    !done,
                    "the suspend transition completed without ever registering a \
                     lifecycle op (R24 F3: persist → stop ran caller-cancellable)"
                );
                assert!(
                    std::time::Instant::now() < deadline,
                    "the suspend never registered its lifecycle op"
                );
                tokio::task::yield_now().await;
            }
            // `fut` dropped here == the caller was cancelled mid-transition.
        }

        // The orphaned op must complete the whole transition.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if overlord.kafka_supervisors.lock().await.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "R24 F3: the cancelled suspend left a GHOST consumer — the \
                 consumer was never stopped"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // The suspended spec lands AFTER the successful drain (R26 F2
        // order: stop first, persist second) — poll for it; then resume
        // agrees.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let spec = overlord
                .get_supervisor("events")
                .await
                .expect("get")
                .expect("row");
            if crate::kafka_suspended_flag(&spec).expect("flag") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the suspended spec must be persisted once the drain succeeded: {spec}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            Arc::clone(&overlord)
                .resume_kafka_supervisors()
                .await
                .expect("resume"),
            0,
            "a suspended supervisor must not resume"
        );
    }

    /// Codex R26 F1: re-creating the same (datasource, topic) pair on the
    /// SAME cluster with a CHANGED ingestion schema (e.g. a renamed
    /// timestamp column) must be REFUSED, not allowed to drop the prior
    /// rows: the earliest replay then re-reads the old records under the
    /// NEW schema — records without the new timestamp column dead-letter —
    /// so the dropped rows are not rebuilt (silent loss). The cleanup
    /// compares the `schemaFp` stamped at publish time against the
    /// (re)starting spec's fingerprint and fails CLOSED before deleting
    /// anything.
    #[tokio::test]
    async fn drop_refuses_schema_fingerprint_mismatch() {
        let (metadata, historical, _dir) = setup().await;
        let victim = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "topic-x",
                "cluster-a:9092",
                Some("kc-A"),
                "v2:fp-old",
            ),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish under the OLD schema");

        // Same pair, same cluster, DIFFERENT schema fingerprint → refuse.
        let err = drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "topic-x",
            Some("kc-A"),
            "v2:fp-new",
        )
        .await
        .expect_err("a schema-changing re-create must be refused (R26 F1)");
        let msg = format!("{err}");
        assert!(
            msg.contains("DIFFERENT ingestion schema"),
            "refusal must explain the schema change: {msg}"
        );
        assert!(
            msg.contains("new datasource"),
            "refusal must point at the safe alternatives: {msg}"
        );
        // Fail-closed: NOTHING was dropped.
        assert!(
            metadata.segment_exists(&victim).await.expect("exists"),
            "the old-schema row must survive the refusal"
        );
        assert_eq!(queried_row_count(&historical, "events"), 1);

        // The SAME schema fingerprint drops as before (no false refusal).
        let dropped = drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "topic-x",
            Some("kc-A"),
            "v2:fp-old",
        )
        .await
        .expect("an unchanged schema must keep dropping");
        assert_eq!(dropped, 1);
        assert_eq!(queried_row_count(&historical, "events"), 0);
    }

    /// SEMANTICS CHANGED by Codex R27 F3 (was
    /// `drop_does_not_block_on_fingerprintless_candidate_rows`, which
    /// pinned the pre-R27 "still dropped" behaviour): a cluster-matched row
    /// WITHOUT a stamped `schemaFp` (a post-clusterId / pre-schemaFp
    /// generation of this unreleased branch) cannot be verified as
    /// rebuildable under the CURRENT schema — if the schema changed in
    /// between, the replay dead-letters every old record and the dropped
    /// rows are never rebuilt (silent loss). Such rows must now be KEPT
    /// (+ warn, duplication over loss) while the create itself still
    /// SUCCEEDS (they must not block either — same rule as an
    /// unknown-version fingerprint, Codex R27 F1).
    #[tokio::test]
    async fn drop_keeps_fingerprintless_cluster_matched_rows() {
        let (metadata, historical, _dir) = setup().await;
        // Simulate a pre-R26 row: full provenance EXCEPT schemaFp.
        let seg = segment("events", &[row(1_000, "a")]);
        let legacy_row = SegmentMetadataRow {
            id: "events_pre_r26_1".to_string(),
            data_source: "events".to_string(),
            created_date: "2026-07-14T00:00:00.000Z".to_string(),
            start: "2023-11-14T00:00:00.000Z".to_string(),
            end: "2023-11-15T00:00:00.000Z".to_string(),
            version: "v1".to_string(),
            used: true,
            payload: json!({
                "dataSource": "events",
                "numRows": 1,
                "taskId": "events",
                "kind": "kafka-streaming",
                "topic": "topic-x",
                "bootstrap": "cluster-a:9092",
                "clusterId": "kc-A",
            }),
        };
        metadata
            .insert_segment(&legacy_row)
            .await
            .expect("insert legacy row");
        historical
            .replace_segments(
                &[],
                vec![SegmentSwapEntry {
                    id: legacy_row.id.clone(),
                    data: Arc::new(seg.segment_data),
                    datasource: Some("events".to_string()),
                }],
            )
            .expect("load legacy row");

        let dropped = drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "topic-x",
            Some("kc-A"),
            "v2:fp-current",
        )
        .await
        .expect("a fingerprint-less row must not refuse the create");
        assert_eq!(
            dropped, 0,
            "a fingerprint-less cluster-matched row must be KEPT (R27 F3): \
             a schema change in between would make its replay dead-letter"
        );
        assert!(
            metadata
                .segment_exists(&legacy_row.id)
                .await
                .expect("exists"),
            "the fingerprint-less row must survive"
        );
        assert_eq!(queried_row_count(&historical, "events"), 1);
    }

    /// Codex R27 F1/F3 (version rule): a cluster-matched row stamped with a
    /// fingerprint of an UNKNOWN/OLDER canonicalisation version (here the
    /// unprefixed R26-era form) is unverifiable exactly like a missing
    /// fingerprint — it must be KEPT and must NOT refuse the create. A
    /// blocking refusal is reserved for a SAME-version mismatch.
    #[tokio::test]
    async fn drop_keeps_unknown_version_fingerprint_rows_without_refusing() {
        let (metadata, historical, _dir) = setup().await;
        // Publish with an R26-era UNPREFIXED fingerprint stamp.
        let old_version = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "events",
                "topic-x",
                "cluster-a:9092",
                Some("kc-A"),
                "0123456789abcdef",
            ),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish row with an old-version fingerprint");

        // Current fingerprint differs — pre-R27 this REFUSED the create
        // (raw string mismatch); the version rule keeps + proceeds instead.
        let dropped = drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "topic-x",
            Some("kc-A"),
            "v2:fp-current",
        )
        .await
        .expect("an unknown-version fingerprint must not refuse the create (R27 F1)");
        assert_eq!(
            dropped, 0,
            "an unknown-version-fingerprint row must be KEPT, not dropped"
        );
        assert!(
            metadata.segment_exists(&old_version).await.expect("exists"),
            "the old-version-fingerprint row must survive"
        );
        assert_eq!(queried_row_count(&historical, "events"), 1);
    }

    /// Codex R26 F2: a shutdown whose FINAL drain failed (residual buffered
    /// rows never became queryable) must FAIL — no tombstone, metadata left
    /// ACTIVE — so a restart/resume or an earliest re-create replays the
    /// topic and rebuilds the rows. Pre-fix the drain failure died in a log
    /// line: shutdown reported success and persisted the tombstone, after
    /// which nothing ever replayed the lost rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_final_drain_fails_shutdown_and_keeps_supervisor_active() {
        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        metadata
            .insert_supervisor("events", &flattened_spec())
            .await
            .expect("persist the active spec");

        // A consumer handle whose task reports a FAILED final drain —
        // models a broken publish tail at the moment the residual buffer
        // was flushed (the streaming side is pinned separately in
        // `ferrodruid_ingest_kafka::streaming::tests::publish_rolled_propagates_sink_failure`).
        let (shutdown_tx, _shutdown_rx) = mpsc::channel::<()>(1);
        let handle = lossy_handle(shutdown_tx, "events", "wiki-events");
        overlord
            .kafka_supervisors
            .lock()
            .await
            .insert("events".to_string(), handle);

        let err = overlord
            .shutdown_supervisor("events")
            .await
            .expect_err("a failed final drain must fail the shutdown (R26 F2)");
        assert!(
            format!("{err}").contains("final drain"),
            "the error must name the drain failure: {err}"
        );

        // NO tombstone: the latest persisted spec must still be the ACTIVE
        // Kafka spec, so restart/resume replays the topic (FG-6 recovery).
        let spec = overlord
            .get_supervisor("events")
            .await
            .expect("get")
            .expect("row");
        assert!(
            crate::is_kafka_typed(&spec),
            "the active spec must remain the latest (no tombstone): {spec}"
        );
        assert!(
            !crate::kafka_suspended_flag(&spec).expect("flag"),
            "the supervisor must NOT be recorded as suspended: {spec}"
        );
        // SEMANTICS CHANGED by Codex R30 F3 (was: "must still be
        // deregistered"): deregistering on failure is exactly what let a
        // RETRIED shutdown skip the drain check and tombstone the pair. The
        // stopped consumer's handle now STAYS registered as a
        // replay-required sentinel (task finished, obligation cached) until
        // a replay path supersedes it.
        {
            let sups = overlord.kafka_supervisors.lock().await;
            let sentinel = sups.get("events").expect(
                "the failed-drain handle must STAY registered as a replay-required \
                 sentinel (R30 F3)",
            );
            assert!(
                sentinel.is_finished(),
                "the sentinel's consumer task is finished (it neither blocks a \
                 re-create nor counts as a live pair)"
            );
        }

        // Recovery path: an earliest re-create of the same pair succeeds
        // and replays the topic (cleanup + replay), rebuilding the rows the
        // failed drain lost.
        overlord
            .create_supervisor(flattened_spec())
            .await
            .expect("an earliest re-create must recover the pair");
        overlord
            .shutdown_supervisor("events")
            .await
            .expect("a clean (empty-buffer) shutdown succeeds");
    }

    /// Codex R27 F2: a MID-STREAM publish failure (threshold/timer flush
    /// whose rows never became queryable) must fail a LATER shutdown even
    /// when the final drain itself succeeded (typically on an EMPTY
    /// buffer) — no tombstone, metadata left ACTIVE — so a restart/resume
    /// or an earliest re-create replays the topic and rebuilds the rows.
    /// Pre-fix the mid-stream failure died in a log line, the empty final
    /// drain reported a clean stop, and the tombstone foreclosed every
    /// replay: the rows were silently lost. (The loop-side stickiness of
    /// the flag is pinned in
    /// `ferrodruid_ingest_kafka::streaming::tests::mid_stream_publish_failure_is_sticky`.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mid_stream_publish_failure_fails_shutdown_and_keeps_supervisor_active() {
        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        metadata
            .insert_supervisor("events", &flattened_spec())
            .await
            .expect("persist the active spec");

        // A consumer handle whose task reports a mid-stream publish failure
        // and a CLEAN final drain — the exact "later empty drain launders
        // the loss" shape of R27 F2.
        let (shutdown_tx, _shutdown_rx) = mpsc::channel::<()>(1);
        let handle = KafkaSupervisorHandle {
            shutdown_tx,
            handle: Some(tokio::spawn(async {
                StreamingStats {
                    total_consumed: 7,
                    total_published: 1,
                    final_flush_failed: false,
                    mid_stream_flush_failed: true,
                    fatal_consume_error: false,
                    cluster_id_drifted: false,
                }
            })),
            replay_error: None,
            data_source: "events".to_string(),
            topic: "wiki-events".to_string(),
            schema_fp: schema_fingerprint(
                &crate::validate_kafka_spec(&flattened_spec()).expect("valid flattened spec"),
            ),
            brokers: "127.0.0.1:9092".to_string(),
        };
        overlord
            .kafka_supervisors
            .lock()
            .await
            .insert("events".to_string(), handle);

        let err = overlord
            .shutdown_supervisor("events")
            .await
            .expect_err("a forgotten mid-stream publish failure must fail the shutdown (R27 F2)");
        assert!(
            format!("{err}").contains("mid-stream"),
            "the error must name the mid-stream failure: {err}"
        );

        // NO tombstone: the latest persisted spec must still be the ACTIVE
        // Kafka spec, so restart/resume replays the topic (FG-6 recovery).
        let spec = overlord
            .get_supervisor("events")
            .await
            .expect("get")
            .expect("row");
        assert!(
            crate::is_kafka_typed(&spec),
            "the active spec must remain the latest (no tombstone): {spec}"
        );
        assert!(
            !crate::kafka_suspended_flag(&spec).expect("flag"),
            "the supervisor must NOT be recorded as suspended: {spec}"
        );
        // SEMANTICS CHANGED by Codex R30 F3 (was: "must still be
        // deregistered"): the lossy-stop handle stays registered as a
        // replay-required sentinel so a retried shutdown/suspend keeps
        // refusing the tombstone (see
        // `retried_shutdown_after_failed_drain_still_refuses_tombstone`).
        assert!(
            overlord
                .kafka_supervisors
                .lock()
                .await
                .contains_key("events"),
            "the lossy-stop handle must stay registered as a sentinel (R30 F3)"
        );
    }

    /// Codex R30 F2: when the earliest-replay cleanup WOULD drop victims,
    /// topic readability must be proven FIRST — a consumer whose principal
    /// can resolve cluster metadata but cannot read the topic would let the
    /// cleanup destroy the pair's prior segments and then rebuild NOTHING.
    /// Broker unreachable ≈ readability unprovable: the create must fail
    /// CLOSED with every prior segment intact. (A real authorization
    /// failure needs a broker with ACLs — real-broker E2E residual; the
    /// topic-level-error branch of the probe is the same code path.)
    #[tokio::test]
    async fn earliest_cleanup_probes_topic_before_destructive_drop() {
        let (metadata, historical, _dir) = setup().await;
        let spec = pair_spec("events", "topic-x");
        // The fingerprint the (re)starting spec compares against.
        let fp = schema_fingerprint(&crate::validate_kafka_spec(&spec).expect("valid spec"));
        // A genuine victim row: same pair, same cluster id, same
        // current-version schema fp — exactly what the cleanup would drop.
        let seg = publish_streaming_segment(
            &metadata,
            &historical,
            &prov("sup-old", "topic-x", "127.0.0.1:59092", Some("kc-A"), &fp),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish victim row");
        assert_eq!(queried_row_count(&historical, "events"), 1);

        let prepared =
            prepare_kafka_consumer("events", crate::validate_kafka_spec(&spec).expect("valid"))
                .expect("lazy create+subscribe succeeds");
        let err = match earliest_replay_cleanup(prepared, &metadata, &historical, Some("kc-A"))
            .await
        {
            Ok(_) => panic!("an unprovable topic must refuse the cleanup BEFORE the drop (R30 F2)"),
            Err(e) => e,
        };
        assert!(
            format!("{err}").contains("topic-readiness"),
            "the refusal must name the probe: {err}"
        );
        assert!(
            format!("{err}").contains("NOTHING was dropped"),
            "the refusal must state fail-close: {err}"
        );
        // Fail-close: the prior segment survives in metadata AND stays
        // query-visible.
        assert!(metadata.segment_exists(&seg).await.expect("exists"));
        assert_eq!(queried_row_count(&historical, "events"), 1);
    }

    /// Codex R30 F2, no-victims fast path: a cleanup that would drop
    /// NOTHING is not destructive, so the probe must not gate it — the
    /// deliberate lazy-connect create semantics survive (a temporarily
    /// unreachable broker does not refuse a fresh supervisor; librdkafka
    /// retries in the background). Kept-by-fail-safe rows (unconfirmable /
    /// foreign identity) are not victims and stay untouched.
    #[tokio::test]
    async fn earliest_cleanup_skips_probe_when_nothing_to_drop() {
        let (metadata, historical, _dir) = setup().await;
        let spec = pair_spec("events", "topic-x");
        // Same-pair rows that are NOT victims under the R26 F3 rules:
        // no stamped clusterId / a different cluster's id.
        publish_streaming_segment(
            &metadata,
            &historical,
            &prov("sup-old", "topic-x", "127.0.0.1:59092", None, "v2:fp-test"),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish clusterId-less row");
        publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "sup-old",
                "topic-x",
                "127.0.0.1:59092",
                Some("kc-OTHER"),
                "v2:fp-test",
            ),
            segment("events", &[row(2_000, "b")]),
        )
        .await
        .expect("publish other-cluster row");

        let prepared =
            prepare_kafka_consumer("events", crate::validate_kafka_spec(&spec).expect("valid"))
                .expect("lazy create+subscribe succeeds");
        // No victims → no probe → the unroutable broker must NOT refuse.
        let prepared = earliest_replay_cleanup(prepared, &metadata, &historical, Some("kc-A"))
            .await
            .expect("a victimless cleanup must not be probe-gated");
        drop(prepared);
        assert_eq!(
            queried_row_count(&historical, "events"),
            2,
            "the kept (non-victim) rows stay untouched"
        );
    }

    /// Codex R35 (review-hardened): the pre-cleanup group-join probe wrapper
    /// ([`prove_group_joinable`]) must PROCEED (`Ok`) when no PERMANENT
    /// group/config rejection surfaces within the window — it must not
    /// spuriously refuse a legitimate earliest re-create on a merely
    /// inconclusive poll (a transient/unreachable broker, or a join still
    /// inside `group.initial.rebalance.delay.ms`). It REFUSES only on a fatal
    /// poll error (which surfaces fast); the `?` in `earliest_replay_cleanup`
    /// then propagates that refusal BEFORE `drop_streaming_segments_task`, so a
    /// refused probe SKIPS the cleanup and preserves the prior segments (the
    /// structural "poll refuse → cleanup skip" order, and the analogous
    /// fail-close ordering already witnessed for the metadata probe by
    /// `earliest_cleanup_probes_topic_before_destructive_drop`). The PERMANENT
    /// `InvalidSessionTimeout` refuse needs a real broker and is a real-broker
    /// E2E residual; its DECISION rule is the shared `consume_error_is_fatal`
    /// classification unit-pinned in the ingest-kafka crate
    /// (`permanent_group_config_recv_errors_are_fatal`).
    #[tokio::test]
    async fn prove_group_joinable_proceeds_without_permanent_rejection() {
        let spec = pair_spec("events", "topic-x"); // 127.0.0.1:59092 (unreachable)
        let prepared =
            prepare_kafka_consumer("events", crate::validate_kafka_spec(&spec).expect("valid"))
                .expect("lazy create+subscribe succeeds");
        let prepared = prove_group_joinable(prepared)
            .await
            .expect("no PERMANENT rejection ⇒ PROCEED (Ok), never spuriously refuse a re-create");
        drop(prepared);
    }

    /// Codex R31: the empty-topic guard's decision must fire ONLY on a
    /// proven-empty topic. A topic delete→recreate under the same
    /// name/cluster/schema (indistinguishable from the original log without a
    /// topic UUID, which librdkafka's consumer metadata does not expose)
    /// would otherwise let the earliest-replay cleanup drop the pair's prior
    /// segments that an empty replay can never rebuild — permanent loss. The
    /// verdict maps fail-safe: `Empty` ⇒ suppress the drop (keep, duplication
    /// over loss); `HasRecords` (normal restart/resume) and `Unknown`
    /// (watermarks unprovable) ⇒ proceed, never suppress on a guess.
    ///
    /// (The live end-to-end — recreate an empty topic and assert the prior
    /// segments survive — needs a real broker and lives in the Kafka E2E; the
    /// unreachable-broker path is already covered by
    /// `earliest_cleanup_probes_topic_before_destructive_drop`, where the
    /// probe fails CLOSED before this verdict is ever reached.)
    #[test]
    fn empty_topic_guard_suppresses_drop_only_when_proven_empty() {
        assert!(
            empty_topic_suppresses_drop(TopicRecords::Empty),
            "a proven-empty topic must suppress the destructive drop (keep prior segments)"
        );
        assert!(
            !empty_topic_suppresses_drop(TopicRecords::HasRecords),
            "a topic that still holds records must let the drop proceed (replay rebuilds)"
        );
        assert!(
            !empty_topic_suppresses_drop(TopicRecords::Unknown),
            "an unprovable topic must NOT suppress on a guess (preserve prior behavior)"
        );
    }

    /// Codex R30 F3: after a FAILED drain the shutdown op used to REMOVE
    /// the handle from the map before observing the failure, so a RETRIED
    /// shutdown found no handle, skipped the drain check entirely, and
    /// persisted the tombstone — permanently foreclosing the replay the
    /// first refusal existed to protect. The retry must keep refusing the
    /// tombstone until a replay path (earliest re-create / restart resume)
    /// actually recovers the pair.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retried_shutdown_after_failed_drain_still_refuses_tombstone() {
        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        metadata
            .insert_supervisor("events", &flattened_spec())
            .await
            .expect("persist the active spec");

        // A consumer handle whose task reports a FAILED final drain (same
        // shape as `failed_final_drain_fails_shutdown_and_keeps_supervisor_active`).
        let (shutdown_tx, _shutdown_rx) = mpsc::channel::<()>(1);
        let handle = lossy_handle(shutdown_tx, "events", "wiki-events");
        overlord
            .kafka_supervisors
            .lock()
            .await
            .insert("events".to_string(), handle);

        // First shutdown observes the failed drain and refuses (R26 F2).
        overlord
            .shutdown_supervisor("events")
            .await
            .expect_err("the first shutdown must observe the failed drain");

        // R30 F3: the RETRY must refuse the tombstone too — pre-fix the
        // handle was already gone, so this call tombstoned the pair and the
        // lost rows became permanently unreplayable.
        let err = overlord
            .shutdown_supervisor("events")
            .await
            .expect_err("a RETRIED shutdown after a failed drain must still refuse (R30 F3)");
        assert!(
            format!("{err}").contains("replay"),
            "the retry refusal must explain the outstanding replay: {err}"
        );
        let spec = overlord
            .get_supervisor("events")
            .await
            .expect("get")
            .expect("row");
        assert!(
            crate::is_kafka_typed(&spec),
            "the active spec must SURVIVE the retried shutdown (no tombstone): {spec}"
        );

        // Recovery: an earliest re-create of the pair replays the topic and
        // rebuilds the lost rows; a clean shutdown may then tombstone.
        overlord
            .create_supervisor(flattened_spec())
            .await
            .expect("an earliest re-create must recover the pair");
        overlord
            .shutdown_supervisor("events")
            .await
            .expect("a clean (empty-buffer) shutdown succeeds after recovery");
        let spec = overlord
            .get_supervisor("events")
            .await
            .expect("get")
            .expect("row");
        assert!(
            !crate::is_kafka_typed(&spec),
            "after recovery the clean shutdown must tombstone normally: {spec}"
        );
    }

    /// Codex R30 F3, suspend route: a suspended-spec POST for a consumer
    /// whose task ended with LOST buffered rows must be refused exactly
    /// like a shutdown — pre-fix the create path reaped the finished handle
    /// without ever draining it, the suspend op then found no handle, and
    /// the suspended spec was persisted: a suspended supervisor is never
    /// replayed, so the rows were silently gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn suspend_of_lossy_finished_consumer_is_refused() {
        let (metadata, historical, _dir) = setup().await;
        let overlord =
            crate::Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        metadata
            .insert_supervisor("events", &flattened_spec())
            .await
            .expect("persist the active spec");

        let (shutdown_tx, _shutdown_rx) = mpsc::channel::<()>(1);
        let handle = lossy_handle(shutdown_tx, "events", "wiki-events");
        overlord
            .kafka_supervisors
            .lock()
            .await
            .insert("events".to_string(), handle);

        // POST the same spec with `suspended: true` (id derives to "events").
        let mut suspend_spec = flattened_spec();
        suspend_spec["suspended"] = json!(true);
        let err = overlord
            .create_supervisor(suspend_spec.clone())
            .await
            .expect_err("suspending a consumer that lost buffered rows must be refused (R30 F3)");
        assert!(
            format!("{err}").contains("replay"),
            "the refusal must explain the outstanding replay: {err}"
        );
        let spec = overlord
            .get_supervisor("events")
            .await
            .expect("get")
            .expect("row");
        assert!(
            !crate::kafka_suspended_flag(&spec).expect("flag"),
            "the supervisor must NOT be recorded as suspended: {spec}"
        );

        // The RETRIED suspend is refused just the same (the obligation is
        // remembered, R30 F3).
        overlord
            .create_supervisor(suspend_spec)
            .await
            .expect_err("a retried suspend must still be refused");

        // Recovery: an earliest re-create replays the topic; a clean
        // suspend then persists normally.
        overlord
            .create_supervisor(flattened_spec())
            .await
            .expect("an earliest re-create must recover the pair");
        let mut suspend_spec = flattened_spec();
        suspend_spec["suspended"] = json!(true);
        overlord
            .create_supervisor(suspend_spec)
            .await
            .expect("a clean suspend succeeds after recovery");
        let spec = overlord
            .get_supervisor("events")
            .await
            .expect("get")
            .expect("row");
        assert!(
            crate::kafka_suspended_flag(&spec).expect("flag"),
            "after recovery the clean suspend must persist: {spec}"
        );
    }

    /// Register a replay-required sentinel on id `"events"` (task finished,
    /// obligation CACHED) with the ACTIVE `flattened_spec` persisted — the
    /// exact state a failed shutdown/suspend drain leaves (Codex R30 F3): a
    /// `lossy_handle` is inserted and drained once via a shutdown that FAILS,
    /// so its `replay_error` is now set and the handle stays registered.
    /// Shared setup for the Codex R33 sentinel-squash regression tests. The
    /// returned `TempDir` backs the historical cache and must be kept alive
    /// by the caller for the overlord's lifetime.
    async fn sentinel_overlord() -> (crate::Overlord, tempfile::TempDir) {
        let (metadata, historical, dir) = setup().await;
        let overlord = crate::Overlord::with_executor(Arc::clone(&metadata), historical);
        metadata
            .insert_supervisor("events", &flattened_spec())
            .await
            .expect("persist the active spec");
        let (shutdown_tx, _shutdown_rx) = mpsc::channel::<()>(1);
        let handle = lossy_handle(shutdown_tx, "events", "wiki-events");
        overlord
            .kafka_supervisors
            .lock()
            .await
            .insert("events".to_string(), handle);
        // Drain once: the failed drain caches `replay_error` on the handle
        // (which STAYS registered, R30 F3), turning it into a true sentinel.
        overlord
            .shutdown_supervisor("events")
            .await
            .expect_err("the first shutdown must observe the failed drain and refuse");
        (overlord, dir)
    }

    /// Assert the replay-required sentinel on `"events"` SURVIVED an
    /// offending POST: the handle is still registered (finished, so it
    /// neither blocks a real recovery nor counts as a live pair), the
    /// metadata is still the ACTIVE `wiki-events` Kafka spec (no tombstone,
    /// no replacement), and a shutdown STILL refuses — the replay obligation
    /// is intact (Codex R33).
    async fn assert_sentinel_intact(overlord: &crate::Overlord) {
        {
            let sups = overlord.kafka_supervisors.lock().await;
            let sentinel = sups
                .get("events")
                .expect("the replay-required sentinel must still be registered (R33)");
            assert!(
                sentinel.is_finished(),
                "the surviving sentinel's task is finished"
            );
            assert!(
                sentinel.replay_required(),
                "the surviving handle must still carry the replay obligation"
            );
        }
        let spec = overlord
            .get_supervisor("events")
            .await
            .expect("get")
            .expect("row");
        assert!(
            crate::is_kafka_typed(&spec) && !crate::kafka_suspended_flag(&spec).expect("flag"),
            "the ACTIVE Kafka spec must be unchanged (no tombstone/replacement): {spec}"
        );
        assert_eq!(
            spec.get("ioConfig")
                .and_then(|c| c.get("topic"))
                .and_then(serde_json::Value::as_str),
            Some("wiki-events"),
            "the persisted spec must NOT have been overwritten by the offending POST: {spec}"
        );
        // The replay protection is still enforced end-to-end: a shutdown
        // re-observes the obligation and keeps refusing the tombstone.
        overlord
            .shutdown_supervisor("events")
            .await
            .expect_err("shutdown must still refuse while the replay obligation stands");
    }

    /// Codex R33: a REPLAY-REQUIRED sentinel (Codex R30 F3) left after a
    /// failed shutdown/suspend drain must NOT be silently reaped by a
    /// NON-Kafka repost of the same id. Pre-fix `create_supervisor` reaped
    /// any finished handle before looking at the spec, so a non-Kafka POST
    /// removed the sentinel and PERSISTED itself as the latest generation:
    /// the lost rows became permanently unreplayable (the ACTIVE Kafka spec
    /// they needed was gone). The repost must be refused, the sentinel and
    /// the active spec left intact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn nonkafka_repost_refused_while_replay_required() {
        let (overlord, _dir) = sentinel_overlord().await;
        // A non-Kafka spec addressed to the SAME id ("events").
        let non_kafka = json!({
            "id": "events",
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "events"},
                "ioConfig": {"inputSource": {"type": "inline", "data": "{}"}}
            }
        });
        let err = overlord
            .create_supervisor(non_kafka)
            .await
            .expect_err("a non-Kafka repost must not squash a replay-required sentinel (R33)");
        assert!(
            format!("{err}").contains("replay"),
            "the refusal must explain the outstanding replay: {err}"
        );
        assert_sentinel_intact(&overlord).await;
    }

    /// Codex R33: a Kafka repost for a DIFFERENT (datasource, topic) pair —
    /// here a different TOPIC under the datasource-derived id "events" — must
    /// not reap the sentinel either. Pre-fix it did, then `insert_supervisor`
    /// OVERWROTE the active `wiki-events` spec with the new topic's spec: the
    /// original pair's lost rows were orphaned with no replay source. A
    /// different pair cannot replay them, so the repost is refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn different_pair_kafka_repost_refused_while_replay_required() {
        let (overlord, _dir) = sentinel_overlord().await;
        // Same datasource (so the id derives to "events") but a DIFFERENT
        // topic — a distinct pair whose replay cannot rebuild wiki-events.
        let other_topic = pair_spec("events", "some-other-topic");
        let err = overlord
            .create_supervisor(other_topic)
            .await
            .expect_err("a different-pair Kafka repost must not squash the sentinel (R33)");
        assert!(
            format!("{err}").contains("replay"),
            "the refusal must explain the outstanding replay: {err}"
        );
        assert_sentinel_intact(&overlord).await;
    }

    /// Codex R33: a same-pair repost that would NOT actually replay the lost
    /// rows — a LATEST (tail) re-create, or a SCHEMA change — is refused too.
    /// A latest consumer never redelivers the old records, and a
    /// schema-changed replay's `earliest_replay_cleanup` refuses to drop the
    /// prior rows (unrebuildable under the new schema, Codex R26 F1); either
    /// way the sentinel's rows would be lost, so neither may supersede it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn latest_or_schema_change_repost_refused_while_replay_required() {
        // (a) LATEST re-create of the SAME pair (useEarliestOffset=false).
        let (overlord, _dir) = sentinel_overlord().await;
        let err = overlord
            .create_supervisor(latest_spec())
            .await
            .expect_err("a LATEST re-create cannot replay the lost rows (R33)");
        assert!(
            format!("{err}").contains("replay"),
            "the latest refusal must explain the replay: {err}"
        );
        assert_sentinel_intact(&overlord).await;

        // (b) SAME pair + earliest but a CHANGED schema (extra dimension →
        // different `schema_fingerprint`).
        let (overlord2, _dir2) = sentinel_overlord().await;
        let mut schema_changed = flattened_spec();
        schema_changed["dataSchema"]["dimensionsSpec"]["dimensions"] =
            json!(["page", "added_dimension"]);
        let err = overlord2
            .create_supervisor(schema_changed)
            .await
            .expect_err("a schema-changed re-create cannot rebuild the lost rows (R33)");
        assert!(
            format!("{err}").contains("replay"),
            "the schema-change refusal must explain the replay: {err}"
        );
        assert_sentinel_intact(&overlord2).await;
    }

    /// Codex R33, the ALLOW path: a repost of the SAME (datasource, topic)
    /// pair, SAME cluster, EARLIEST offset, and UNCHANGED schema is the ONE
    /// re-create permitted to supersede a replay-required sentinel — it is the
    /// only shape whose `earliest_replay_cleanup` + replay CAN rebuild the
    /// lost rows. This pins the ALLOW gate: the sentinel is replaced by a
    /// fresh live earliest consumer, and a subsequent clean shutdown may
    /// tombstone.
    ///
    /// HONEST LIMITATION (FG-6, unchanged by R33): "supersede" means the
    /// earliest consumer has STARTED replaying, NOT that the replay has
    /// COMPLETED. Segments are in-memory only, so if the topic is unreachable
    /// at recovery (and there are no cleanup victims to force the readiness
    /// probe) the consumer registers anyway and an immediate shutdown drains
    /// an empty buffer cleanly — the lost rows are not rebuilt until Kafka is
    /// back AND the consumer runs. This is the same FG-6 "Kafka is the durable
    /// log" posture the whole sentinel mechanism sits inside; a durable
    /// replay-to-completion guarantee is FG-7 (deep-storage persistence). R33
    /// only narrows WHICH reposts may supersede the sentinel; it does not (and
    /// cannot, in-memory) guarantee the replay finishes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_pair_earliest_same_schema_repost_supersedes_sentinel() {
        let (overlord, _dir) = sentinel_overlord().await;
        overlord
            .create_supervisor(flattened_spec())
            .await
            .expect("a same-pair earliest same-schema repost may supersede the sentinel (R33)");
        // The sentinel was superseded by a fresh LIVE earliest consumer (not a
        // finished sentinel): the replay obligation is now carried by a
        // running consumer replaying the topic, not a stuck handle.
        {
            let sups = overlord.kafka_supervisors.lock().await;
            let running = sups
                .get("events")
                .expect("a live consumer replaced the sentinel");
            assert!(
                !running.is_finished() && !running.replay_required(),
                "the recovering repost must register a fresh live consumer, not a sentinel"
            );
        }
        // The sentinel no longer blocks lifecycle ops: a clean shutdown of the
        // fresh consumer tombstones (FG-6 caveat above notwithstanding).
        overlord
            .shutdown_supervisor("events")
            .await
            .expect("a clean shutdown of the fresh consumer succeeds");
        let spec = overlord
            .get_supervisor("events")
            .await
            .expect("get")
            .expect("row");
        assert!(
            !crate::is_kafka_typed(&spec),
            "the clean shutdown of the fresh consumer must tombstone normally: {spec}"
        );
    }

    /// Codex R33 (review F1): a repost of the same pair / earliest / schema
    /// but a re-point at DIFFERENT brokers is NOT a recovery — the ORIGINAL
    /// cluster's lost rows can only be replayed from the ORIGINAL cluster, so
    /// a different broker set must not supersede the sentinel. `recoverable_by`
    /// compares `bootstrap.servers` (the strongest signal available before a
    /// lifecycle op resolves the broker-side cluster id).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn different_broker_repost_refused_while_replay_required() {
        let (overlord, _dir) = sentinel_overlord().await;
        // Same datasource + topic + earliest + schema, DIFFERENT brokers.
        let mut different_brokers = flattened_spec();
        different_brokers["ioConfig"]["consumerProperties"]["bootstrap.servers"] =
            json!("different-cluster:9092");
        let err = overlord
            .create_supervisor(different_brokers)
            .await
            .expect_err("a different-broker repost cannot replay the original cluster (R33)");
        assert!(
            format!("{err}").contains("replay"),
            "the refusal must explain the outstanding replay: {err}"
        );
        assert_sentinel_intact(&overlord).await;
    }

    /// Codex R33 (review F2): a repost that IS a valid recovery
    /// (`recoverable_by`) but whose create then FAILS partway must NOT have
    /// already reaped the sentinel — otherwise the ACTIVE metadata is left
    /// with no replay guard and a later shutdown tombstones the pair (the
    /// exact R30 F3 hole, reopened). The sentinel is LEFT registered until the
    /// spawned op's registration overwrites it ON SUCCESS. Here the recovery
    /// is aborted by a persisted-pair conflict from a legacy duplicate row
    /// under another id (a deterministic mid-create failure).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_recovery_leaves_replay_sentinel_registered() {
        let (overlord, _dir) = sentinel_overlord().await;
        // A LEGACY duplicate row (pre-R25) claiming the SAME (events,
        // wiki-events) pair under a DIFFERENT id — persisted directly to
        // bypass the create-time pair guard, exactly the state
        // `refuse_persisted_kafka_pair_conflict` exists to catch.
        overlord
            .metadata
            .insert_supervisor("legacy-events", &flattened_spec())
            .await
            .expect("persist a conflicting legacy pair row");

        // A genuine recovery spec (same pair / cluster / earliest / schema):
        // `recoverable_by` passes, so the sentinel is LEFT in place — but the
        // create then fails at the persisted-pair conflict.
        let err = overlord
            .create_supervisor(flattened_spec())
            .await
            .expect_err("the recovery must fail at the persisted-pair conflict");
        assert!(
            format!("{err}").contains("already claims"),
            "the failure must be the persisted-pair conflict: {err}"
        );
        // The sentinel must have SURVIVED the aborted recovery (not reaped
        // before success), so the replay obligation still stands.
        assert_sentinel_intact(&overlord).await;
    }

    /// Codex R33 (review): a consumer that SELF-TERMINATED on a fatal consume
    /// error (or panicked) and was never drained is finished but carries its
    /// replay obligation only in the un-joined task's stats — `replay_error`
    /// is not cached until a drain joins it. A non-recovery repost must NOT
    /// mistake it for a clean stale handle and reap it (which, combined with
    /// the finished/live TOCTOU, silently dropped the obligation). The create
    /// path HARVESTS the finished handle's outcome under the same lock it
    /// classifies liveness, so the fatal stop becomes a replay-required
    /// sentinel and a LATEST (non-replaying) repost is refused, not accepted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unobserved_fatal_stop_is_harvested_not_reaped_on_repost() {
        let (metadata, historical, _dir) = setup().await;
        let overlord = crate::Overlord::with_executor(Arc::clone(&metadata), historical);
        metadata
            .insert_supervisor("events", &flattened_spec())
            .await
            .expect("persist the active spec");

        // A finished-but-UNDRAINED fatal stop: handle still `Some(finished
        // task)`, `replay_error` not yet cached.
        let (shutdown_tx, _shutdown_rx) = mpsc::channel::<()>(1);
        let handle = KafkaSupervisorHandle {
            shutdown_tx,
            handle: Some(tokio::spawn(async {
                StreamingStats {
                    total_consumed: 4,
                    total_published: 0,
                    final_flush_failed: false,
                    mid_stream_flush_failed: false,
                    fatal_consume_error: true,
                    cluster_id_drifted: false,
                }
            })),
            replay_error: None,
            data_source: "events".to_string(),
            topic: "wiki-events".to_string(),
            schema_fp: schema_fingerprint(
                &crate::validate_kafka_spec(&flattened_spec()).expect("valid flattened spec"),
            ),
            brokers: "127.0.0.1:9092".to_string(),
        };
        // Let the trivial task finish so the handle is observably finished but
        // still un-harvested (no cached `replay_error`).
        for _ in 0..10_000 {
            if handle.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            handle.is_finished() && !handle.replay_required(),
            "precondition: the handle is finished but its fatal outcome is un-harvested"
        );
        overlord
            .kafka_supervisors
            .lock()
            .await
            .insert("events".to_string(), handle);

        // A LATEST (non-replaying) repost of the same pair must be refused:
        // the create harvests the fatal stop into a sentinel first.
        let err = overlord
            .create_supervisor(latest_spec())
            .await
            .expect_err("a latest repost must not reap an un-harvested fatal stop (R33 review)");
        assert!(
            format!("{err}").contains("replay"),
            "the refusal must explain the outstanding replay: {err}"
        );
        assert_sentinel_intact(&overlord).await;
    }

    /// Codex R37 F3 (structural witness): a consumer that stopped on a detected
    /// cluster-identity DRIFT (`cluster_id_drifted`) must make `shutdown` REFUSE
    /// the tombstone — the loop deliberately did NOT publish the possibly-
    /// mis-attributed buffer, so recording a clean stop would foreclose the only
    /// recovery (a replay). Drives the real `KafkaSupervisorHandle::shutdown`
    /// drain path; the live broker-reconnect drift itself is a real-broker E2E
    /// residual (the DECISION rule is the `cluster_id_is_drift` classifier
    /// unit-pinned in the ingest-kafka crate).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cluster_id_drift_stop_refuses_tombstone() {
        let (shutdown_tx, _shutdown_rx) = mpsc::channel::<()>(1);
        let mut handle = KafkaSupervisorHandle {
            shutdown_tx,
            handle: Some(tokio::spawn(async {
                StreamingStats {
                    total_consumed: 9,
                    total_published: 2,
                    final_flush_failed: false,
                    mid_stream_flush_failed: false,
                    fatal_consume_error: false,
                    cluster_id_drifted: true,
                }
            })),
            replay_error: None,
            data_source: "events".to_string(),
            topic: "wiki-events".to_string(),
            schema_fp: schema_fingerprint(
                &crate::validate_kafka_spec(&flattened_spec()).expect("valid flattened spec"),
            ),
            brokers: "127.0.0.1:9092".to_string(),
        };

        let err = handle
            .shutdown()
            .await
            .expect_err("a cluster-identity drift stop must refuse the tombstone (R37 F3)");
        let msg = format!("{err}");
        assert!(
            msg.contains("DRIFT") && msg.contains("replay"),
            "the refusal must name the cluster-identity drift and the outstanding replay: {msg}"
        );
        // The obligation is cached (sticky): a retried shutdown re-reports it.
        let err2 = handle
            .shutdown()
            .await
            .expect_err("a retried shutdown must still refuse (cached replay obligation)");
        assert!(format!("{err2}").contains("DRIFT"), "err2 = {err2}");
    }

    /// Codex R26 F3: a streaming row WITHOUT a stamped `clusterId` (legacy
    /// R21-era rows, or rows published while the cluster id was
    /// unresolvable) must NEVER be auto-dropped — not even when its stamped
    /// `bootstrap` equals the (re)starting consumer's. Bootstrap equality
    /// is not cluster identity: after a DNS repoint the same string names a
    /// DIFFERENT cluster, whose replay can never rebuild the dropped rows
    /// (permanent loss). Keeping them risks only DUPLICATION on an earliest
    /// replay — the documented fail-safe default (duplication over loss).
    #[tokio::test]
    async fn drop_never_claims_clusterid_less_rows() {
        let (metadata, historical, _dir) = setup().await;
        // Bootstrap-only rows (no clusterId), including a reordered form of
        // the same list — both previously matched via the bootstrap
        // fallback.
        let exact = publish_streaming_segment(
            &metadata,
            &historical,
            &prov("sup-a", "topic-x", "cluster-a:9092", None, "v2:fp-test"),
            segment("events", &[row(1_000, "a")]),
        )
        .await
        .expect("publish bootstrap-only row");
        let reordered = publish_streaming_segment(
            &metadata,
            &historical,
            &prov(
                "sup-b",
                "topic-x",
                "cluster-b:9092,cluster-a:9092",
                None,
                "v2:fp-test",
            ),
            segment("events", &[row(2_000, "b")]),
        )
        .await
        .expect("publish reordered bootstrap-only row");
        assert_eq!(queried_row_count(&historical, "events"), 2);

        // Current identity KNOWN (Some): the id-less rows still must not be
        // claimed — the pre-R26 bootstrap fallback dropped `exact` here.
        let dropped = drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "topic-x",
            Some("kc-A"),
            "v2:fp-test",
        )
        .await
        .expect("drop scan");
        assert_eq!(
            dropped, 0,
            "clusterId-less rows must never be auto-dropped (R26 F3)"
        );
        // Current identity UNKNOWN (None): same fail-safe.
        let dropped = drop_streaming_segments_task(
            &metadata,
            &historical,
            "events",
            "topic-x",
            None,
            "v2:fp-test",
        )
        .await
        .expect("drop scan");
        assert_eq!(dropped, 0);
        assert!(metadata.segment_exists(&exact).await.expect("exists"));
        assert!(metadata.segment_exists(&reordered).await.expect("exists"));
        assert_eq!(queried_row_count(&historical, "events"), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_and_shutdown_lifecycle() {
        // A validated spec spawns a consumer (no broker needed: the
        // StreamConsumer connects lazily and simply waits for records), and
        // shutdown drains it cleanly without hanging.
        let (metadata, historical, _dir) = setup().await;
        let spec = json!({
            "type": "kafka",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {
                "topic": "wiki-events",
                "consumerProperties": {"bootstrap.servers": "127.0.0.1:59092"},
                "useEarliestOffset": true
            }
        });
        let parsed = crate::validate_kafka_spec(&spec).expect("valid spec");
        let prepared = prepare_kafka_consumer("events-sup", parsed)
            .expect("consumer create+subscribe succeeds (lazy connect)");
        let mut handle = start_prepared(
            prepared,
            metadata,
            historical,
            None,
            None,
            None,
            ResumeFrontier::default(),
        );
        // Shutdown must complete promptly (empty buffer → flush is a no-op)
        // and report a CLEAN drain (R26 F2: the Result is now meaningful).
        tokio::time::timeout(std::time::Duration::from_secs(10), handle.shutdown())
            .await
            .expect("shutdown did not hang")
            .expect("an empty-buffer drain must report success");
    }

    /// Codex R6 H3: a persisted supervisor whose startup LIFECYCLE OP fails
    /// (here: librdkafka refuses the consumer config value at creation —
    /// the same warn-path a transiently unreachable broker's fail-close
    /// probe takes) must NOT be abandoned until the next process restart:
    /// a background task keeps retrying the resume pass, and once the
    /// condition clears (here: the operator re-persists a healthy spec —
    /// for a broker outage, the broker coming back) the consumer STARTS.
    /// Pre-R6, the candidate was warn-skipped once and its partitions were
    /// permanently starved — everything retention expired in the meantime
    /// was silently lost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_retries_failed_supervisor_startup_in_background() {
        let (metadata, historical, _dir) = setup().await;
        // Passes spec VALIDATION (so it is a runnable candidate, not a
        // permanent skip) but fails consumer CREATION: librdkafka validates
        // property values at instantiation time.
        let mut bad = flattened_spec();
        bad["ioConfig"]["consumerProperties"]["session.timeout.ms"] = json!("not-a-number");
        metadata
            .insert_supervisor("events", &bad)
            .await
            .expect("persist failing spec");
        let overlord = Arc::new(crate::Overlord::with_executor(
            Arc::clone(&metadata),
            Arc::clone(&historical),
        ));
        assert_eq!(
            Arc::clone(&overlord)
                .resume_kafka_supervisors()
                .await
                .expect("the resume pass itself succeeds"),
            0,
            "the failing lifecycle op must start nothing on the first pass"
        );
        // Heal the condition (models the broker coming back / the operator
        // fixing the config): the latest persisted spec generation wins.
        metadata
            .insert_supervisor("events", &flattened_spec())
            .await
            .expect("persist healed spec");
        // The background retry (R6 H3) must pick the healed spec up and
        // start the consumer WITHOUT any new resume call.
        let mut live = false;
        for _ in 0..100 {
            if overlord.kafka_supervisor_live_for_tests("events").await {
                live = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            live,
            "the background resume retry must start the supervisor once the \
             transient startup failure clears (R6 H3)"
        );
        let _ = overlord.shutdown_supervisor("events").await;
    }

    /// Codex R6 H3, loop hygiene: the background retry task must EXIT (flag
    /// cleared) once nothing remains to resume — here the failing spec is
    /// tombstoned, so the next pass has zero candidates and zero failures.
    /// Also pins the double-spawn idempotence: a second resume call while
    /// the retry task is live must not wedge anything (compare-and-swap
    /// no-op).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_retry_task_exits_when_nothing_left_and_never_double_spawns() {
        let (metadata, historical, _dir) = setup().await;
        let mut bad = flattened_spec();
        bad["ioConfig"]["consumerProperties"]["session.timeout.ms"] = json!("bogus");
        metadata
            .insert_supervisor("events", &bad)
            .await
            .expect("persist failing spec");
        let overlord = Arc::new(crate::Overlord::with_executor(
            Arc::clone(&metadata),
            Arc::clone(&historical),
        ));
        assert_eq!(
            Arc::clone(&overlord)
                .resume_kafka_supervisors()
                .await
                .expect("resume pass"),
            0
        );
        assert!(
            overlord.kafka_resume_retry_active_for_tests(),
            "a failed candidate must schedule the background retry (R6 H3)"
        );
        // A second resume call while the retry task is live: CAS no-op.
        assert_eq!(
            Arc::clone(&overlord)
                .resume_kafka_supervisors()
                .await
                .expect("second resume pass"),
            0
        );
        // Remove the supervisor: the next retry pass sees no candidates and
        // the task exits, clearing the flag.
        overlord
            .shutdown_supervisor("events")
            .await
            .expect("tombstone the never-started supervisor");
        let mut cleared = false;
        for _ in 0..100 {
            if !overlord.kafka_resume_retry_active_for_tests() {
                cleared = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            cleared,
            "the retry task must exit once nothing remains to resume (R6 H3)"
        );
    }

    /// Codex R14 H1: a WHOLE-PASS resume error (not `failed > 0` — the entire
    /// pass returns `Err`, e.g. the startup metadata enumeration fails
    /// transiently) must ALSO schedule the background retry. Here an
    /// UNINITIALIZED metadata store makes `get_all_supervisors` fail, so
    /// `resume_kafka_supervisors_once` returns a whole-pass `Err`. Pre-fix the
    /// `?` propagated straight out and NO retry was scheduled (the flag stayed
    /// false) — every persisted supervisor was starved until the next restart.
    /// Now the retry is scheduled; once the transient condition heals (the
    /// schema is created), the retry's next clean pass exits and clears the
    /// flag.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_whole_pass_error_schedules_background_retry_and_recovers() {
        // Deliberately NOT initialized: `get_all_supervisors` hits a missing
        // table → the resume pass fails WHOLESALE (not a per-candidate failure).
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("store"));
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let historical = Arc::new(Historical::new(cache_dir.path().to_path_buf(), 10_000_000));
        let overlord = Arc::new(crate::Overlord::with_executor(
            Arc::clone(&metadata),
            Arc::clone(&historical),
        ));

        let res = Arc::clone(&overlord).resume_kafka_supervisors().await;
        assert!(
            res.is_err(),
            "an uninitialized store must make the resume pass fail WHOLESALE"
        );
        assert!(
            overlord.kafka_resume_retry_active_for_tests(),
            "a WHOLE-PASS resume Err must schedule the background retry (R14 H1) — pre-fix \
             the `?` propagated and nothing was scheduled"
        );

        // Heal the transient condition: create the schema. The background
        // retry's next pass now enumerates cleanly (zero supervisors) and the
        // task exits, clearing the flag — proving the transient whole-pass
        // failure recovers instead of permanently starving.
        metadata.initialize().await.expect("init schema");
        let mut cleared = false;
        for _ in 0..200 {
            if !overlord.kafka_resume_retry_active_for_tests() {
                cleared = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            cleared,
            "once the transient whole-pass failure heals, the background retry exits \
             (R14 H1 / R6 H3)"
        );
    }
}
