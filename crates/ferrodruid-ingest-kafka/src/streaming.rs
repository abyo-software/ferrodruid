// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Streaming Kafka ingestion that publishes rolled segments through a
//! caller-supplied [`SegmentSink`].
//!
//! This is the glue that turns the (previously inert)
//! [`KafkaConsumerTask`](crate::consumer::KafkaConsumerTask) building
//! block into a real supervisor: records are polled from Kafka,
//! accumulated in a [`StreamingBuffer`], and — when the buffer fills or a
//! wall-clock flush interval elapses — rolled into an
//! [`IngestedSegment`] and handed to the sink.
//!
//! **Offset policy (compat-3 stage 2 — durable, at-least-once):** the
//! consumer runs with `enable.auto.commit=false` and MANUAL-commits the
//! per-partition max offset ONLY AFTER a rolled segment's publish returns
//! `Ok` (segment durably persisted to deep storage + metadata committed —
//! stage 1's P→M order). A failed publish commits nothing, so a restart
//! re-consumes exactly the un-persisted records (at-least-once: loss
//! impossible; duplication bounded by one un-committed roll). Each published
//! segment ALSO stamps the `[start, next)` span it covers into
//! `payload.kafkaOffsets`, so the durable segment set is a self-consistent
//! resume-frontier source that no separate offset store can drift from — the
//! [`EosSegmentWriter::resume_offsets`](crate::eos_writer::EosSegmentWriter::resume_offsets)
//! model. On restart the overlord derives the per-partition frontier from
//! those durable rows and (after the group assignment settles) SEEKS each
//! partition past it, so already-persisted records are neither replayed nor
//! double-counted against the segments reloaded from deep storage.
//! Exactly-once and removal of the now-redundant earliest-replay machinery
//! are stage 3.
//!
//! **Rebalance protocol (Codex R29):** because nothing is ever committed,
//! the consumer additionally pins
//! `partition.assignment.strategy=cooperative-sticky` — under the eager
//! default, any rebalance (e.g. a partition-count increase) would revoke
//! every partition and re-consume it from `auto.offset.reset`, duplicating
//! already-published rows in-process. The pin is asserted by unit test on
//! the assembled config; the live add-partitions rebalance itself needs a
//! real broker and remains a real-broker E2E residual.
//!
//! The [`StreamingBuffer`] (accumulate + roll) is available without the
//! `kafka-io` feature so it is unit-testable; only the actual rdkafka
//! poll loop in [`run_streaming_consumer`] requires `kafka-io`.

use std::collections::BTreeMap;

use ferrodruid_ingest_batch::{
    BatchIngester, DimensionSchema, DimensionType, IngestedSegment, TsFormat, long_dim_value_class,
};

use crate::consumer::{ConsumerError, KafkaConsumerConfig, check_record_bounds};
use crate::eos_writer::OffsetSpan;
use crate::{DimensionEntry, KafkaSupervisorSpec};

/// Per-partition Kafka offset spans `[start, next)` covered by ONE rolled
/// segment (compat-3 stage 2), keyed by partition id (the topic is fixed
/// per consumer). Reuses [`OffsetSpan`](crate::eos_writer::OffsetSpan) so the
/// durable resume-frontier derivation shares one span type with the EOS
/// writer. Stamped into the published segment's `payload.kafkaOffsets` and —
/// on `Ok` publish — manual-committed to Kafka (`enable.auto.commit=false`),
/// so a restart resumes PAST the durable frontier instead of replaying
/// already-persisted records.
pub type PartitionOffsets = BTreeMap<i32, OffsetSpan>;

/// Per-partition durable-resume evidence (compat-3 durability), derived by
/// the overlord from the durable segment set and consumed by
/// [`apply_resume_seek`] together with the partition's COMMITTED Kafka offset
/// to compute where the consumer must resume.
///
/// * `min_start` is the lowest `[start,…)` claimed by ANY durable metadata row
///   for this partition — whether or not its blob actually reloaded, and
///   whether or not its cluster identity could be confirmed (everything except
///   a DEFINITE cluster mismatch or a DEFINITE topic-generation mismatch
///   floors; Codex R5 H2 / R8 H1 — a dead generation's rows are excluded
///   entirely, and a partition with ONLY dead-generation evidence gets
///   `min_start = 0` with no coverage so the watermark clamp re-consumes the
///   retained log from `low`) — the floor that pulls the resume point BELOW a
///   stale-high committed offset when a blob went missing (a phantom row).
/// * `loaded` are the `[start,next)` spans of the durable rows that ACTUALLY
///   reloaded into the Historical AND whose cluster identity DEFINITELY
///   matches the current consumer's (Codex R4 H4), merged and sorted by
///   `start`. Only offsets contiguously covered by these are safe to skip; a
///   hole (a phantom row, a never-durable failed roll, or an
///   unconfirmed-cluster-identity row) stops the resume so its records are
///   re-consumed rather than skipped (no loss).
/// * `recreated` marks a partition on which a TOPIC RECREATION was positively
///   detected (a durable row stamped with a DIFFERENT KIP-516 `topicId` than
///   the topic currently carries — Codex R7 H1 / R8 H1 / R9 C1+C2). The old
///   generation's offset space is DEAD: its committed offset survives the
///   recreation in the group coordinator but names a different record, and
///   its rows' spans are meaningless. Such a partition is resumed on the
///   unified recreation model — FLOOR = the retained log's `low` watermark
///   (never `min_start`, never the committed offset), ADVANCE = the live
///   generation's `loaded` coverage walked contiguously from `low`
///   ([`recreated_resume_target`]), and a rebalance restore driven by the
///   `resume_floor` (durable-OR-buffered frontier) alone ([`restore_target`]).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartitionResume {
    /// Lowest durable-row start for the partition (loaded or phantom,
    /// definite-match or unconfirmed identity). IGNORED when `recreated`
    /// (the floor is the live log's `low` watermark instead — R9 C2).
    pub min_start: i64,
    /// Merged, `start`-sorted spans of the DEFINITE-identity rows that
    /// reloaded into Historical (the only spans safe to skip past).
    pub loaded: Vec<OffsetSpan>,
    /// A topic recreation was positively detected on this partition (Codex
    /// R9): dead-generation evidence exists, so the committed offset and
    /// `min_start` are untrusted and the resume is `low`-floored.
    pub recreated: bool,
}

impl PartitionResume {
    /// Build from the earliest durable start and the reloaded spans, merging
    /// the spans into non-overlapping, `start`-sorted intervals so
    /// [`resume_offset`] can walk them once.
    #[must_use]
    pub fn new(min_start: i64, mut loaded: Vec<OffsetSpan>) -> Self {
        loaded.sort_by_key(|s| (s.start, s.next));
        let mut merged: Vec<OffsetSpan> = Vec::with_capacity(loaded.len());
        for span in loaded {
            if span.next <= span.start {
                continue; // empty/degenerate
            }
            match merged.last_mut() {
                // Overlapping or adjacent → extend the previous interval.
                Some(prev) if span.start <= prev.next => {
                    if span.next > prev.next {
                        prev.next = span.next;
                    }
                }
                _ => merged.push(span),
            }
        }
        Self {
            min_start,
            loaded: merged,
            recreated: false,
        }
    }

    /// Mark this partition as RECREATION-DETECTED (Codex R9): dead-generation
    /// evidence exists, so the resume is driven by the unified recreation
    /// model (floor = `low`, advance = live coverage contiguous from `low`,
    /// restore = `resume_floor` alone). Builder-style so the overlord's
    /// frontier derivation composes it with [`new`](Self::new).
    #[must_use]
    pub fn mark_recreated(mut self) -> Self {
        self.recreated = true;
        self
    }
}

/// The complete durable-resume directive the overlord derives on (re)start
/// (compat-3 durability): the per-partition [`PartitionResume`] evidence PLUS
/// the topic-level recreation verdict (Codex R18 C1+C2).
///
/// `topic_recreated` exists because the per-partition map alone cannot carry
/// a recreation whose only evidence lives in rows OUTSIDE the numeric
/// frontier: administratively DISABLED rows (used=false, excluded per R3 H5 —
/// C1: with every durable row disabled the map is EMPTY) and durable rows
/// stamped with a `topicId` but WITHOUT `kafkaOffsets` (published while the
/// cluster identity was transiently unresolved, R4 H4 — C2: the row names no
/// partition). In both cases the DEAD generation's committed offsets survive
/// the delete+recreate in the group coordinator, so resuming any partition
/// from its committed offset silently skips every new-generation record below
/// it (loss). When `topic_recreated` is set the consumer trusts NO committed
/// offset: every assigned partition — frontier entry or not — is floored at
/// the retained log's `low` watermark (the entries in `partitions` are all
/// recreation-flagged by the deriving side; partitions with no entry get one
/// synthesized via [`synthesize_recreated`](Self::synthesize_recreated)).
/// FLOOR-only by construction: a synthesized entry has no coverage, so the
/// resume can never advance/skip — re-consuming the retained log is bounded
/// at-least-once duplication, never loss.
///
/// Honest residual (unchanged from R8 H2): a recreation is only detectable
/// while the CURRENT topic id is resolvable — with a non-PLAINTEXT listener
/// or a pre-2.8 broker the flag stays `false` and the pre-R18 exposure
/// remains, exactly as documented on the frontier derivation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResumeFrontier {
    /// Per-partition durable resume evidence (see [`PartitionResume`]). When
    /// `topic_recreated` is set, every entry is recreation-flagged.
    pub partitions: BTreeMap<i32, PartitionResume>,
    /// A topic recreation was positively detected from ANY metadata row of
    /// the pair (used or disabled, with or without `kafkaOffsets`): a row's
    /// stamped `topicId` and the CURRENT resolved topic id are both present
    /// and DIFFERENT (Codex R18 C1+C2). Committed offsets are untrusted for
    /// EVERY partition of the topic while set.
    pub topic_recreated: bool,
}

impl ResumeFrontier {
    /// Under a DETECTED topic recreation, ensure every listed (assigned)
    /// partition has a frontier entry: partitions with no durable evidence
    /// get a synthesized floor-low entry (`min_start = 0`, no coverage,
    /// recreation-flagged) so their resume seek is derived as the retained
    /// log's `low` watermark ([`recreated_resume_target`]) instead of
    /// trusting the DEAD generation's committed offset (Codex R18 C1+C2).
    /// Existing entries are never touched. Returns the number of entries
    /// synthesized; a no-op (0) unless `topic_recreated` — the normal path
    /// never gains spurious entries.
    ///
    /// FLOOR-only invariant: a synthesized entry carries NO coverage, so
    /// [`resume_offset`] returns the floor-0 placeholder and the seek-time
    /// derivation lands exactly on `low` — the resume is never advanced past
    /// a record that was not durably covered (re-consume, never skip).
    pub fn synthesize_recreated(&mut self, partitions: impl IntoIterator<Item = i32>) -> usize {
        if !self.topic_recreated {
            return 0;
        }
        let mut synthesized = 0usize;
        for partition in partitions {
            self.partitions.entry(partition).or_insert_with(|| {
                synthesized += 1;
                PartitionResume::new(0, Vec::new()).mark_recreated()
            });
        }
        synthesized
    }
}

/// Walk `point` forward through the CONTIGUOUS prefix of the merged,
/// `start`-sorted `loaded` coverage: a span at/below `point` extends it to
/// the span's `next`; the first hole ABOVE `point` stops the walk so the
/// missing records are re-consumed (never skipped — the no-loss priority all
/// resume derivations share).
fn advance_contiguous(mut point: i64, loaded: &[OffsetSpan]) -> i64 {
    for span in loaded {
        if span.next <= point {
            continue; // fully below the walk position
        }
        if span.start <= point {
            point = span.next; // contiguous → extend
        } else {
            break; // a hole above `point` → stop (re-consume from here)
        }
    }
    point
}

/// Compute the offset a partition must RESUME consuming from on restart
/// (compat-3 durability), given its OPTIONAL committed Kafka offset and its
/// durable [`PartitionResume`]. Pure so the correctness core (C1/C2/H6) is
/// unit-testable without a broker.
///
/// A [`PartitionResume`] only ever exists for a partition that HAS durable
/// rows (the overlord builds the frontier map solely from the durable segment
/// set), so the durable frontier is **authoritative** and this ALWAYS returns
/// a seek target — it never downgrades to `auto.offset.reset`. The committed
/// offset is only an OPTIONAL lower bound; its ABSENCE (expired retention, or
/// an async commit that never landed) must NOT fall back to
/// `auto.offset.reset`, which would either skip the durable span (latest →
/// loss of any post-frontier crash residue) or replay all of it (earliest →
/// duplication) (C1).
///
/// The walk starts at `floor`:
///
/// * `floor = min(committed, min_start)` when a commit exists — `committed`
///   pulls it down to catch a **never-durable gap** below the durable rows (a
///   failed roll left no row, so `min_start` is above it, but the contiguous
///   commit kept `committed` at/below it), and `min_start` pulls it down to
///   catch a **phantom row** whose blob is missing (C2: `committed` stale-high,
///   `min_start` at/below the hole);
/// * `floor = min_start` when there is NO commit — the durable frontier alone
///   governs (C1).
///
/// then walks the contiguous reloaded coverage:
///
/// * a hole in `loaded` stops the walk, so the missing records are re-consumed
///   (never skipped) — at-least-once duplication of any durable rows above the
///   hole is accepted (no-loss priority);
/// * fully-covered coverage advances the resume PAST already-reloaded records
///   so they are not double-counted (H6 — the async-commit-lagged case).
///
/// **Recreation-detected partitions** (`resume.recreated`, Codex R9 C1+C2)
/// use the unified recreation model instead: the floor is **0** — NEITHER
/// the committed offset (it survives the recreation but names a DEAD
/// generation's record) NOR `min_start` (live rows may start ABOVE the
/// retained log's `low`, and lifting the floor to them skips any
/// never-durable `[low, live_start)` gap forever) may gate the walk. The
/// returned target is a floor-0 placeholder that the seek path re-derives
/// against the REAL watermark window ([`recreated_resume_target`]) before
/// seeking; when `low == 0` the two derivations agree exactly.
#[must_use]
pub fn resume_offset(committed: Option<i64>, resume: &PartitionResume) -> i64 {
    // Durable rows exist for this partition (it is in the frontier map), so the
    // durable frontier is authoritative even when there is NO committed offset
    // (C1): `committed` can only pull the floor DOWN, never gate the seek.
    let point = if resume.recreated {
        // R9: dead-generation committed offsets and live-row min_start are
        // both untrusted — floor at 0, clamped to `low` at seek time.
        0
    } else {
        match committed {
            Some(c) => c.min(resume.min_start),
            None => resume.min_start,
        }
    };
    advance_contiguous(point, &resume.loaded)
}

/// The authoritative resume target for a RECREATION-DETECTED partition
/// (Codex R9 C2), derived against the partition's LIVE watermark window
/// `[low, high]`: **FLOOR = `low`** (the dead generation's committed offset
/// and the live rows' `min_start` are both untrusted — see
/// [`PartitionResume::recreated`]), **ADVANCE = the live generation's
/// `loaded` coverage walked contiguously from `low`**. A gap
/// `[low, live_start)` — a publish that failed after the recreation was
/// detected — keeps the target at `low` so the gap is re-consumed (bounded
/// at-least-once duplication of any durable coverage above it, never loss);
/// coverage contiguous from `low` is skipped past (no re-publish of durable
/// rows on every restart — the R8 H1 guarantee, now preserved WITHOUT
/// letting live rows lift the floor). A walked target past `high` (the log
/// shrank again — a second truncation/recreation) re-clamps to `low`
/// ([`clamp_resume_target`], R5 H1). Pure so the R9 C2 core is unit-testable
/// without a broker.
#[must_use]
pub fn recreated_resume_target(low: i64, high: i64, resume: &PartitionResume) -> i64 {
    clamp_resume_target(advance_contiguous(low, &resume.loaded), low, high)
}

/// Resolve the post-rebalance RESTORE target (Codex R6 H1 → R9 C1 → R12 H1
/// → R27 H1), given whether the partition's COMMITTED offset is UNTRUSTED.
/// `resume_floor` is the durable-OR-buffered frontier
/// ([`safe_resume_frontier`]) — NOT the raw consumed-forward point (which
/// includes a dropped failed batch, R12 H1):
///
/// * `committed_untrusted == false`: [`rebalance_restore_target`]'s
///   `max(committed, resume_floor)` — the committed offset is live
///   durable group progress and seeking below it re-publishes it (R6 H1);
/// * `committed_untrusted == true`: **`resume_floor` alone**. The committed
///   offset names a record of a DEAD offset numbering, and with the log
///   produced past it again the stale value is IN-range — the watermark
///   clamp cannot catch it, and `max()`ing it in seeks the restore PAST
///   unconsumed records that were never published anywhere (permanent
///   loss). Two detections arm this:
///
///   * **topic recreation** (`PartitionResume::recreated`, R9 C1): the
///     committed offset survives a topic delete+recreate (same group id)
///     but names the dead generation's offset space. The dead value is
///     additionally OVERWRITTEN at the recreation-floor seek (the commit in
///     the activation pass — `try_activate_pending`) but that commit is
///     async and may not have landed by the time a rebalance fires — this
///     resolution never reads it;
///   * **same-topic-id truncation**
///     ([`ResumeSeekGate::offset_space_reset_detected`], R27 H1): an
///     unclean leader election (or a recreation the topic-id probe could
///     not see) SHRANK the offset space under the durable frontier — the
///     startup seek was `target > high`-clamped to `low` — and the log
///     later REGREW past the stale committed offset with records re-using
///     the truncated offset numbers. `recreated` never arms here (no topic
///     id changed), so the seek-time clamp history is the only witness.
///
///   In both cases `resume_floor` is always live-numbering progress:
///   records only ever flow on such a partition after its floor/clamped
///   seek landed. Honest residual (documented, at-least-once): in a
///   multi-member group another member's live committed progress is
///   ignored too, so the restore may re-consume spans that member already
///   published — bounded duplication, chosen over any loss window.
#[must_use]
pub fn restore_target(committed_untrusted: bool, committed: Option<i64>, resume_floor: i64) -> i64 {
    if committed_untrusted {
        resume_floor
    } else {
        rebalance_restore_target(committed, resume_floor)
    }
}

/// Whether a FAILED resume seek to the durable frontier `target` may be safely
/// ignored, given the partition's OPTIONAL `committed` Kafka offset (compat-3
/// durability, C1/C2/H6). Pure so the fail-closed decision is unit-testable
/// without a broker.
///
/// The seek returned by [`resume_offset`] is AUTHORITATIVE: `target` is the
/// exact offset a partition with durable rows must resume from. If the seek
/// FAILS, letting the consumer proceed from its natural position is safe ONLY
/// when that position already equals `target` (`committed == Some(target)`, a
/// redundant seek). EVERY other outcome corrupts the stream and must instead
/// PAUSE the partition (fail-closed; a restart re-seeks):
///
/// * `target > committed` (forward / skip): replaying from the stale committed
///   offset re-consumes and RE-PUBLISHES already-durable rows = permanent
///   double count (H6);
/// * `target < committed` (backward / re-consume): staying at the stale-high
///   committed offset SKIPS the phantom/gap span the seek had to re-consume =
///   loss (C2);
/// * `committed == None` (expired retention / async commit not landed):
///   `auto.offset.reset` governs = skip the whole durable span (latest → loss)
///   or replay it (earliest → duplication) — a durable partition must never
///   fall back to it (C1).
#[must_use]
pub fn resume_seek_failure_is_safe(committed: Option<i64>, target: i64) -> bool {
    committed == Some(target)
}

/// Per-partition progress of the authoritative resume seek (compat-3
/// durability, Codex R3 C2/C3/C4/H6) — see [`ResumeSeekGate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekGateState {
    /// The authoritative seek target is not resolved yet (the committed-offset
    /// fetch is pending or failed). Consumption is suppressed.
    NeedsTarget,
    /// The partition must be sought to `target` before ANY of its records may
    /// be consumed. Consumption is suppressed.
    NeedsSeek {
        /// The authoritative offset the partition must resume from.
        target: i64,
    },
    /// The partition was revoked+reassigned mid-session with in-session
    /// progress (Codex R6 H1): its restore seek target must be resolved at
    /// ACTIVATION time as `max(fresh committed offset, resume_floor)`
    /// ([`rebalance_restore_target`]) — never a frozen local point, which
    /// knows nothing about progress OTHER group members committed while this
    /// consumer did not own the partition (seeking below the committed offset
    /// re-consumes and RE-PUBLISHES their durable records = permanent double
    /// count). Consumption is suppressed until that seek succeeds.
    NeedsRestore {
        /// The in-session RESUME FLOOR — the durable-OR-buffered frontier
        /// ([`safe_resume_frontier`], Codex R12 H1): the offset up to which
        /// this consumer's records are either durably published OR still
        /// buffered (recoverable). Restoring here NEVER skips a DROPPED
        /// (failed-publish) batch — those offsets are absent from both the
        /// durable frontier and the buffer, so the floor stays below them and
        /// they are re-consumed (no loss) — and NEVER re-consumes an
        /// already-durable/still-buffered record (the buffered tail is
        /// re-published from the buffer, dup-free). The group's committed
        /// offset (fetched fresh at activation) may exceed it and then wins —
        /// unless the committed offset is UNTRUSTED (recreation R9 C1 /
        /// offset-space reset R27 H1): then the floor drives alone
        /// ([`restore_target`]).
        resume_floor: i64,
    },
    /// The authoritative seek succeeded (or the partition never needed one):
    /// records flow.
    Active,
}

/// Resolve the post-rebalance RESTORE seek target for a partition with
/// in-session progress (compat-3 durability, Codex R6 H1 → R12 H1):
/// `max(committed, resume_floor)` — and NEVER below the committed offset.
///
/// `resume_floor` is the durable-OR-buffered frontier
/// ([`safe_resume_frontier`]): the highest offset up to which this consumer's
/// records are either durably published OR still buffered (recoverable). It is
/// NOT the raw consumed-forward point — a DROPPED (failed-publish) batch is
/// absent from both the durable frontier and the buffer, so it never lifts the
/// floor past itself (which would seek over records that exist nowhere = loss,
/// R12 H1).
///
/// `committed` is the group's durable progress: it only ever advances after
/// some member's segment was durably published (the manual-commit
/// discipline), so any offset below it names records that are ALREADY
/// queryable — seeking there re-consumes and re-publishes them (permanent
/// double count). While THIS consumer did not own the partition, another
/// member may have advanced it far past this consumer's local
/// `resume_floor`, which is why the local value alone must never be the
/// restore point. `resume_floor` still wins when it is HIGHER — the
/// continuous-owner / async-commit-lag case: records in `[committed,
/// resume_floor)` are already published by (or still buffered in) THIS
/// consumer, so seeking back to `committed` would double-count against
/// itself. A `None` committed offset (fetch succeeded, nothing ever
/// committed) leaves `resume_floor` as the only floor.
///
/// Residual (documented, at-least-once): a member whose durable publish
/// landed but whose ASYNC commit did not advances nothing here — the
/// reassigned consumer may re-consume that span (bounded duplication, never
/// loss). Pure so the invariant is unit-testable without a broker.
#[must_use]
pub fn rebalance_restore_target(committed: Option<i64>, resume_floor: i64) -> i64 {
    committed.map_or(resume_floor, |c| c.max(resume_floor))
}

/// Per-partition state machine that ENFORCES the authoritative resume seek
/// (compat-3 durability, Codex R3): no partition with a durable frontier — or
/// with join-drive records to rewind to — is ever consumed at a non-frontier
/// offset. The poll loop consults [`record_delivered`](Self::record_delivered)
/// for EVERY received record, so the enforcement holds on every failure path:
///
/// * **C2** — the group assignment never settling in-window no longer falls
///   back to consuming from committed offsets / `auto.offset.reset`: the
///   frontier partitions stay gated ([`SeekGateState::NeedsTarget`]) and are
///   sought once the assignment appears;
/// * **C3** — a failed seek whose safety PAUSE also failed (e.g. mid
///   rebalance) is still suppressed: the gate — not the broker-side pause —
///   is what keeps records from being consumed, so a pause failure can no
///   longer leak loss/double-count consumption;
/// * **C4** — a failed rewind of join-drive-discarded records gates the
///   partition at the first discarded offset instead of dropping the records
///   forever;
/// * **H6** — paused partitions remain in [`pending`](Self::pending), are
///   re-sought on a timer, and are RESUMED on success — never left paused
///   until restart.
///
/// Dropping a suppressed record is always safe: its offset is never fed to
/// the buffer or the commit frontier, so nothing is committed past it, and
/// the successful authoritative seek re-delivers from `target` (records
/// below `target` are covered by reloaded durable segments). Kept free of
/// Kafka I/O so the enforcement core is unit-testable without a broker.
#[derive(Debug, Default)]
pub struct ResumeSeekGate {
    /// Per-partition seek progress. Absent partitions are Active (partitions
    /// without a durable frontier or rewind obligation consume freely).
    states: BTreeMap<i32, SeekGateState>,
    /// Partitions whose records were delivered while suppressed (their fetch
    /// position advanced past the natural start), or whose join-drive records
    /// were discarded: the `committed == target` failed-seek no-op carve-out
    /// ([`resume_seek_failure_is_safe`]) is invalid for them.
    tainted: std::collections::BTreeSet<i32>,
    /// Partitions currently (best-effort) paused broker-side; a successful
    /// seek must `resume()` them before consumption restarts.
    paused: std::collections::BTreeSet<i32>,
    /// Partitions whose CURRENTLY PENDING seek is a RECREATION-FLOOR seek
    /// (Codex R9): its target must be re-derived from the live watermark
    /// window (`floor = low`, advance = live coverage —
    /// [`recreated_resume_target`]) instead of validating the floor-0
    /// placeholder, and the first successful such seek must OVERWRITE the
    /// group's dead-generation committed offset with the seek position
    /// (R9 C1, so `max(committed, …)` semantics become safe again for
    /// every later reader). Armed when a recreation-detected partition's
    /// `NeedsTarget` resolves; disarmed on the successful floor seek and on
    /// activation. A RESTORE seek (`resume_floor`-driven) is never armed —
    /// committing its target would claim durability for buffered rows.
    recreation_floor: std::collections::BTreeSet<i32>,
    /// Partitions whose offset space was observed to have SHRUNK under the
    /// authoritative seek target (`target > high` at watermark-clamp time —
    /// a log truncation such as an unclean leader election, or a topic
    /// recreation the topic-id probe could not see; Codex R27 H1). The
    /// group's committed offset on such a partition predates the reset and
    /// names records of the DEAD offset numbering — and once the log REGROWS
    /// past it (new records re-using the truncated offset numbers) the stale
    /// value is IN-range again, so the watermark clamp alone can never catch
    /// it. This memory is what lets a LATER rebalance restore ignore the
    /// committed offset ([`restore_target`] with `committed_untrusted`,
    /// exactly like a recreation-detected partition) instead of `max()`ing a
    /// stale-high value in and seeking PAST unconsumed regrown records
    /// (permanent loss). Deliberately NEVER cleared for the life of the
    /// session — not by [`mark_active`](Self::mark_active) (the loss window
    /// is a rebalance long AFTER the clamped seek activated) and not by a
    /// later in-range validation (in-range is exactly what a regrown stale
    /// offset looks like). Residual: every restore on such a partition is
    /// `resume_floor`-driven, so another member's committed progress is
    /// re-consumed — bounded duplication, chosen over any loss window.
    offset_space_reset: std::collections::BTreeSet<i32>,
    /// Partitions whose CURRENTLY PENDING seek must be FLOORED at the live
    /// `low` watermark because its target was derived from now-untrusted
    /// sources — a stale committed offset or old-numbering durable coverage —
    /// after an offset-space reset was witnessed this session (Codex R27 H1
    /// (a)/(b)). Distinct from the persistent [`offset_space_reset`] memory:
    /// this is the PER-SEEK arming (mirrors [`recreation_floor`]) that
    /// switches the watermark clamp from "trust the target if in-range" to
    /// "floor at `low`", so a REGROWN stale target — which is back in-range
    /// and invisible to the plain clamp — can never forward-seek past regrown
    /// records (loss). Armed when the reset is first detected (its pending
    /// [`SeekGateState::NeedsSeek`] target is the stale one — case (a)) and
    /// when a rebalance re-derives a `NeedsTarget` on a reset partition before
    /// it ever activated (case (b)); DISARMED on activation
    /// ([`mark_active`](Self::mark_active)), where the seek has landed on the
    /// live `low` and the target is authoritative again. NOT armed for a
    /// `NeedsRestore` seek — that target is the trusted `resume_floor` (live
    /// progress), so the primary R27 restore path is left unchanged.
    reset_floor: std::collections::BTreeSet<i32>,
}

impl ResumeSeekGate {
    /// A gate with no gated partitions (everything consumes freely).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Gate `partition` until its authoritative target is resolved AND the
    /// seek to it succeeds (a frontier partition whose committed offset is
    /// not known yet).
    pub fn require_target(&mut self, partition: i32) {
        self.states.insert(partition, SeekGateState::NeedsTarget);
    }

    /// Gate `partition` on a seek to the RESOLVED `target` (a frontier
    /// partition whose reconciled resume offset is known, or a non-frontier
    /// partition that must be rewound to its first join-drive record).
    pub fn set_target(&mut self, partition: i32, target: i64) {
        self.states
            .insert(partition, SeekGateState::NeedsSeek { target });
    }

    /// Gate `partition` on a post-rebalance RESTORE seek (Codex R6 H1): the
    /// final target is resolved at activation time as
    /// [`rebalance_restore_target`]`(fresh committed, resume_floor)`, so a
    /// stale local consume point can never seek BELOW the offset the group
    /// durably committed while this consumer did not own the partition.
    /// `resume_floor` is the durable-OR-buffered frontier
    /// ([`safe_resume_frontier`], Codex R12 H1) — never the raw
    /// consumed-forward point, which would skip a dropped failed batch (loss).
    pub fn set_restore(&mut self, partition: i32, resume_floor: i64) {
        self.states
            .insert(partition, SeekGateState::NeedsRestore { resume_floor });
    }

    /// The partition's current seek progress (Active when never gated).
    #[must_use]
    pub fn state(&self, partition: i32) -> SeekGateState {
        self.states
            .get(&partition)
            .copied()
            .unwrap_or(SeekGateState::Active)
    }

    /// Whether every gated partition has been activated.
    #[must_use]
    pub fn is_clear(&self) -> bool {
        self.states
            .values()
            .all(|s| matches!(s, SeekGateState::Active))
    }

    /// The partitions still awaiting a successful authoritative seek (the
    /// retry set: [`SeekGateState::NeedsTarget`] / [`SeekGateState::NeedsSeek`],
    /// paused or not).
    #[must_use]
    pub fn pending(&self) -> Vec<i32> {
        self.states
            .iter()
            .filter(|(_, s)| !matches!(s, SeekGateState::Active))
            .map(|(&p, _)| p)
            .collect()
    }

    /// Verdict for a record delivered on `partition`: `true` = consume it;
    /// `false` = the partition's authoritative seek has not succeeded yet —
    /// the record must be DROPPED without recording its offset anywhere (the
    /// successful seek re-delivers from the target). A suppressed delivery
    /// additionally taints the partition (its fetch position advanced), which
    /// invalidates the [`may_noop_failed_seek`](Self::may_noop_failed_seek)
    /// carve-out.
    pub fn record_delivered(&mut self, partition: i32) -> bool {
        match self.state(partition) {
            SeekGateState::Active => true,
            SeekGateState::NeedsTarget
            | SeekGateState::NeedsSeek { .. }
            | SeekGateState::NeedsRestore { .. } => {
                self.tainted.insert(partition);
                false
            }
        }
    }

    /// Mark `partition`'s fetch position as moved past its natural start
    /// (records were delivered-and-discarded for it, e.g. during the join
    /// drive) — the failed-seek no-op carve-out is invalid from here on.
    pub fn taint(&mut self, partition: i32) {
        self.tainted.insert(partition);
    }

    /// The authoritative seek (and, if needed, the broker-side resume)
    /// succeeded: consumption restarts and the position is authoritative
    /// again (the taint is cleared). Returns `true` when the partition was
    /// activated.
    ///
    /// REFUSED (`false`, state unchanged) while the partition is recorded
    /// broker-side PAUSED (Codex R4 H1): an "Active while paused" partition
    /// would never receive another record AND — having left
    /// [`pending`](Self::pending) — would never be retried by the seek-retry
    /// timer: a silent permanent stall. Callers must `resume()` the partition
    /// first ([`note_unpaused`](Self::note_unpaused)) and only then activate;
    /// a refused activation keeps the partition gated + retried.
    #[must_use]
    pub fn mark_active(&mut self, partition: i32) -> bool {
        if self.paused.contains(&partition) {
            return false;
        }
        self.states.insert(partition, SeekGateState::Active);
        self.tainted.remove(&partition);
        // An Active partition has no pending seek, so no pending
        // recreation-floor derivation either (R9). Idempotent.
        self.recreation_floor.remove(&partition);
        // `offset_space_reset` is deliberately NOT cleared (R27 H1): the
        // truncation memory must outlive activation — the loss it prevents
        // is a rebalance restore trusting the stale committed offset long
        // after the clamped seek landed.
        //
        // The PER-SEEK `reset_floor` arming IS cleared (R27 H1 (a)/(b)): the
        // seek has now landed on the live `low` and the position is
        // authoritative again — a later rebalance re-derives a fresh
        // obligation (and re-arms if the partition is still reset-dirty).
        self.reset_floor.remove(&partition);
        true
    }

    /// Arm `partition`'s pending seek as a RECREATION-FLOOR seek (Codex R9):
    /// called when a recreation-detected partition's `NeedsTarget` resolves.
    /// See the `recreation_floor` field for the semantics it switches on.
    pub fn arm_recreation_floor(&mut self, partition: i32) {
        self.recreation_floor.insert(partition);
    }

    /// Whether `partition`'s pending seek is a recreation-floor seek.
    #[must_use]
    pub fn recreation_floor_armed(&self, partition: i32) -> bool {
        self.recreation_floor.contains(&partition)
    }

    /// Disarm the recreation-floor marker after the floor seek SUCCEEDED;
    /// returns whether it was armed (the caller then overwrites the dead
    /// committed offset exactly once per arming — R9 C1).
    pub fn disarm_recreation_floor(&mut self, partition: i32) -> bool {
        self.recreation_floor.remove(&partition)
    }

    /// Record that `partition`'s offset space was observed to have SHRUNK
    /// under an authoritative seek target (`target > high` at watermark-clamp
    /// time — log truncation / recreation missed by the topic-id probe, Codex
    /// R27 H1): its committed offset predates the reset and must never drive
    /// a rebalance restore again this session. See the `offset_space_reset`
    /// field for why the memory is never cleared.
    pub fn note_offset_space_reset(&mut self, partition: i32) {
        self.offset_space_reset.insert(partition);
    }

    /// Whether an offset-space reset (truncation) was ever observed on
    /// `partition` this session (Codex R27 H1) — if so, its committed offset
    /// is untrusted and a rebalance restore is `resume_floor`-driven alone.
    #[must_use]
    pub fn offset_space_reset_detected(&self, partition: i32) -> bool {
        self.offset_space_reset.contains(&partition)
    }

    /// Arm `partition`'s pending seek as a RESET-FLOOR seek (Codex R27 H1
    /// (a)/(b)): its target was derived from now-untrusted sources (a stale
    /// committed offset or old-numbering coverage) after an offset-space
    /// reset, so the watermark clamp must floor it at `low` instead of
    /// trusting it (a regrown stale target is back in-range and would
    /// forward-seek past regrown records = loss). See the `reset_floor`
    /// field. Idempotent; disarmed on activation.
    pub fn arm_reset_floor(&mut self, partition: i32) {
        self.reset_floor.insert(partition);
    }

    /// Whether `partition`'s pending seek is a reset-floor seek (its target
    /// must be floored at the live `low` watermark — Codex R27 H1 (a)/(b)).
    #[must_use]
    pub fn reset_floor_armed(&self, partition: i32) -> bool {
        self.reset_floor.contains(&partition)
    }

    /// Record that the partition was successfully PAUSED broker-side after a
    /// failed seek: a later successful seek must `resume()` it.
    pub fn note_paused(&mut self, partition: i32) {
        self.paused.insert(partition);
    }

    /// Record that the partition was successfully RESUMED broker-side.
    pub fn note_unpaused(&mut self, partition: i32) {
        self.paused.remove(&partition);
    }

    /// Whether the partition is currently recorded as broker-side paused.
    #[must_use]
    pub fn is_paused(&self, partition: i32) -> bool {
        self.paused.contains(&partition)
    }

    /// Whether a FAILED seek to `target` may be treated as a harmless no-op:
    /// requires `committed == Some(target)` ([`resume_seek_failure_is_safe`])
    /// AND that no record was ever delivered/discarded on the partition (an
    /// untouched fetch position — otherwise the position is past `committed`
    /// and skipping the seek would skip records).
    #[must_use]
    pub fn may_noop_failed_seek(
        &self,
        partition: i32,
        committed: Option<i64>,
        target: i64,
    ) -> bool {
        resume_seek_failure_is_safe(committed, target) && !self.tainted.contains(&partition)
    }
}

/// Clamp an authoritative resume-seek `target` into the partition's CURRENT
/// broker-side watermark window `[low, high]` (compat-3 durability, Codex R4
/// H3 → R5 H1). Pure so the bound logic is unit-testable without a broker.
///
/// rdkafka's `seek` only repositions the LOCAL fetch position — it reports
/// success without the broker validating the offset. If `target` is OUTSIDE
/// the retained log, the first fetch later fails broker-side with
/// OFFSET_OUT_OF_RANGE and librdkafka silently falls back to
/// `auto.offset.reset` — under `latest` that SKIPS every retained
/// post-frontier record (permanent loss); under `earliest` it replays the
/// whole log (unbounded duplication). Clamping the target into the retained
/// window before seeking removes the fallback by construction. BOTH
/// out-of-range directions clamp to `low` (no-loss priority):
///
/// * `target < low` (retention already deleted past the durable frontier):
///   seek `low` — the `[target, low)` records no longer exist anywhere, and
///   everything retained from `low` on is consumed (any already-durable
///   prefix re-consumed this way is bounded at-least-once duplication, never
///   loss);
/// * `target > high` (the log was truncated/recreated below the durable
///   frontier — the offset space SHRANK): seek `low`, NOT `high` (Codex R5
///   H1): every retained record `[low, high)` lives in the NEW offset space
///   (the durable segments that produced `target` cover DIFFERENT, old-space
///   records), so seeking `high` would skip ALL of them — permanent loss of
///   the recreated topic's data — while re-consuming from `low` is no-loss
///   and no-double-count;
/// * `target == high` is IN range (the consumer is caught up and waits at
///   the log end for new records);
/// * degenerate `high < low` (never reported by a sane broker): collapse to
///   `low` rather than propagating a nonsense window.
#[must_use]
pub fn clamp_resume_target(target: i64, low: i64, high: i64) -> i64 {
    if high < low {
        // Degenerate broker window: never propagate it.
        return low;
    }
    if target < low || target > high {
        // Out of range in EITHER direction → re-consume the retained log
        // from `low` (bounded duplication at worst, never loss — R5 H1).
        return low;
    }
    target
}

/// Resolve the validated resume-seek target inside a partition's CURRENT
/// watermark window `[low, high]`, applying the two override signals the
/// activation pass carries (compat-3 durability). Returns `(validated
/// target, offset space SHRANK)`; the boolean is `true` only when a plain
/// clamp had to pull the target down from ABOVE `high` (a freshly-detected
/// truncation the caller must remember — Codex R27 H1). Pure so the R9 C2 /
/// R27 H1 seek derivations are unit-testable without a broker.
///
/// * `recreation == Some(resume)` (R9 C2): the incoming `target` is only a
///   floor-0 placeholder — the real target is [`recreated_resume_target`]
///   (floor `low`, advanced contiguously through the live generation's
///   coverage). The committed offset is already untrusted on a recreation,
///   so no separate reset is signalled;
/// * `reset_floor == true` (R27 H1 (a)/(b)): an offset-space reset was
///   witnessed this session and the incoming `target` was derived from
///   now-untrusted sources (a stale committed offset, or old-numbering
///   durable coverage). After a REGROWTH the stale target lands back
///   IN-range, so a plain clamp would forward-seek PAST regrown records
///   (loss). Floor at `low` unconditionally — re-consume, bounded
///   duplication, NEVER a skip. No reset is signalled (already recorded);
/// * otherwise: [`clamp_resume_target`] into the window, signalling
///   `target > high` so the caller records the first-seen truncation.
#[must_use]
pub fn resolve_seek_target(
    recreation: Option<&PartitionResume>,
    reset_floor: bool,
    target: i64,
    low: i64,
    high: i64,
) -> (i64, bool) {
    if let Some(resume) = recreation {
        return (recreated_resume_target(low, high, resume), false);
    }
    if reset_floor {
        // Floor at `low`, validated against the (possibly degenerate) window.
        return (clamp_resume_target(low, low, high), false);
    }
    (clamp_resume_target(target, low, high), target > high)
}

/// Partition-level rebalance activity observed since the last drain
/// (compat-3 durability, Codex R4 H2), as recorded by the consumer's
/// rebalance callbacks and consumed by [`rearm_gate_after_rebalance`].
/// Pure data so the re-arm logic is unit-testable without a broker.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RebalanceEvents {
    /// Partitions REVOKED from this consumer (their librdkafka fetch state —
    /// position and app-level pause flag — died with the revoked toppar).
    pub revoked: std::collections::BTreeSet<i32>,
    /// Partitions (re-)ASSIGNED to this consumer (their fetch position was
    /// re-initialised from the COMMITTED offset, which may lag the durable
    /// frontier when an async commit never landed).
    pub assigned: std::collections::BTreeSet<i32>,
    /// A rebalance ERROR was reported (rdkafka unassigns EVERYTHING on it):
    /// every tracked partition must be treated as revoked+reassigned.
    pub lost_all: bool,
}

impl RebalanceEvents {
    /// Whether nothing was recorded (the common steady-state case).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.lost_all && self.revoked.is_empty() && self.assigned.is_empty()
    }
}

/// Shared, thread-safe accumulator of [`RebalanceEvents`] — written by the
/// consumer's rebalance callbacks (which run on the polling thread) and
/// drained by the streaming loop (Codex R4 H2). Cloning shares the same
/// underlying accumulator.
#[derive(Debug, Default, Clone)]
pub struct RebalanceNotices {
    inner: std::sync::Arc<std::sync::Mutex<RebalanceEvents>>,
}

impl RebalanceNotices {
    /// A fresh, empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock the accumulator, recovering (rather than propagating) a poisoned
    /// lock: the guarded state is plain partition-id sets whose worst
    /// inconsistency is an extra re-arm — strictly safe — while a propagated
    /// poison would take down the poll loop.
    fn lock(&self) -> std::sync::MutexGuard<'_, RebalanceEvents> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Record partitions revoked from the consumer.
    pub fn record_revoked(&self, partitions: impl IntoIterator<Item = i32>) {
        self.lock().revoked.extend(partitions);
    }

    /// Record partitions (re-)assigned to the consumer.
    pub fn record_assigned(&self, partitions: impl IntoIterator<Item = i32>) {
        self.lock().assigned.extend(partitions);
    }

    /// Record a rebalance ERROR (rdkafka unassigns everything on it).
    pub fn record_lost_all(&self) {
        self.lock().lost_all = true;
    }

    /// Drain everything recorded so far, leaving the accumulator empty.
    #[must_use]
    pub fn take(&self) -> RebalanceEvents {
        std::mem::take(&mut *self.lock())
    }
}

/// Re-arm the [`ResumeSeekGate`] for every partition touched by a rebalance
/// (compat-3 durability, Codex R4 H2), so NO partition is ever consumed at
/// the post-rebalance position without a fresh authoritative seek.
///
/// The finding: a revocation (e.g. a `max.poll.interval.ms` overrun during a
/// long publish) followed by a reassignment restarts the partition from its
/// COMMITTED offset — which lags the true consume position whenever the
/// manual async commit did not land (or simply because records were consumed
/// since the last durable publish). An `Active` gate would let that stale
/// position flow: already-published durable rows are re-consumed and
/// RE-PUBLISHED (permanent double count). Re-arming forces a seek first:
///
/// * a partition with in-session consume progress (its entry in the
///   `resume_floor` map — the durable-OR-buffered frontier,
///   [`safe_resume_frontier`]) is re-armed to a RESTORE seek
///   ([`ResumeSeekGate::set_restore`], Codex R6 H1 → R12 H1) whose final
///   target is resolved at activation time as
///   `max(fresh committed offset, resume_floor)`
///   ([`rebalance_restore_target`]): `resume_floor` alone is this consumer's
///   LOCAL progress and knows nothing about offsets OTHER group members
///   durably published + committed while this consumer did not own the
///   partition — freezing the seek there (the pre-R6 shape) seeks BACKWARD
///   below the group's committed progress and re-publishes those members'
///   records (permanent double count). The max keeps both directions safe:
///   nothing below the group's durable commit is ever re-consumed, and
///   nothing this consumer durably published or still holds buffered
///   (possibly above a lagging async commit) is re-delivered. Crucially the
///   floor is the durable-OR-buffered frontier, NOT the raw consumed-forward
///   point: a DROPPED (failed-publish) batch is absent from it, so the
///   restore re-consumes that batch instead of skipping it (Codex R12 H1 —
///   the raw point would seek past records that exist nowhere = loss);
/// * an untouched partition WITH a durable frontier is re-armed to
///   [`ResumeSeekGate::require_target`]: its authoritative target is
///   re-derived from the committed offset + durable evidence, exactly like
///   the restart path (nothing was consumed in-session, so the startup
///   derivation is still sound);
/// * an untouched partition WITHOUT a frontier keeps its existing state — a
///   still-gated join-drive rewind keeps its pending seek obligation (C4),
///   and a free partition resumes from its unchanged committed offset /
///   `auto.offset.reset`, the normal first-start semantics;
/// * every REVOKED partition's broker-side pause marker is cleared
///   ([`ResumeSeekGate::note_unpaused`]): the app-level pause flag lived on
///   the revoked toppar and a reassigned toppar starts UNPAUSED, so keeping
///   the marker would wrongly refuse activation (H1) against broker reality.
///
/// Returns the number of partitions re-armed (0 = nothing to do). Pure (no
/// Kafka I/O) so the enforcement core is unit-testable without a broker.
pub fn rearm_gate_after_rebalance(
    gate: &mut ResumeSeekGate,
    frontier: &BTreeMap<i32, PartitionResume>,
    resume_floor: &BTreeMap<i32, i64>,
    events: &RebalanceEvents,
) -> usize {
    if events.is_empty() {
        return 0;
    }
    let mut affected: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
    affected.extend(events.revoked.iter().copied());
    affected.extend(events.assigned.iter().copied());
    if events.lost_all {
        // A rebalance error unassigns EVERYTHING: treat every partition this
        // session tracks (durable frontier or in-session progress) as
        // revoked+reassigned.
        affected.extend(frontier.keys().copied());
        affected.extend(resume_floor.keys().copied());
    }
    let mut rearmed = 0usize;
    for &partition in &affected {
        if events.lost_all || events.revoked.contains(&partition) {
            // The broker-side pause died with the revoked toppar; a
            // reassigned toppar starts unpaused. Clear the marker so a later
            // successful seek can activate (H1) without a spurious resume.
            gate.note_unpaused(partition);
        }
        if let Some(&floor) = resume_floor.get(&partition) {
            // NOT a frozen `set_target(floor)` (Codex R6 H1): the restore
            // target is resolved against the FRESH committed offset at
            // activation time, so a stale local consume point can never
            // seek below what the group durably committed in the meantime.
            // `floor` is the durable-OR-buffered frontier (Codex R12 H1) —
            // never the raw consumed-forward point, which would skip a
            // dropped failed batch (loss).
            gate.set_restore(partition, floor);
            rearmed += 1;
        } else if frontier.contains_key(&partition) {
            gate.require_target(partition);
            rearmed += 1;
        }
        // else: keep the existing state (gated rewind keeps its obligation;
        // an untouched free partition stays free).
    }
    rearmed
}

/// Per-partition tracker that keeps the COMMITTED Kafka offset CONTIGUOUS
/// with the durable segment set (compat-3 durability, C1): it never lets a
/// commit advance PAST the first gap left by a failed publish, so a restart
/// resuming from the committed offset re-consumes the dropped rows instead of
/// skipping them (loss).
///
/// Kept free of Kafka I/O so it is unit-testable without a broker.
#[derive(Debug, Default)]
pub struct ContiguousFrontier {
    /// Per-partition next offset to commit — the end of the run of durably
    /// published spans that is CONTIGUOUS from the consume base. Seeded lazily
    /// to a partition's base (its first CONSUMED offset) so a first publish
    /// that starts ABOVE the base (its earlier roll failed) is detected as a
    /// gap rather than silently committed over.
    next: BTreeMap<i32, i64>,
}

impl ContiguousFrontier {
    /// A fresh tracker (no partitions seen yet).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Note that a Kafka message at `(partition, offset)` was consumed, seeding
    /// the partition's contiguous base to the FIRST offset seen (messages
    /// arrive in increasing offset order per partition, so the first is the
    /// base). Called for every consumed message, including dead-lettered ones.
    pub fn note_consumed(&mut self, partition: i32, offset: i64) {
        self.next.entry(partition).or_insert(offset);
    }

    /// FORCE the partition's contiguous-commit base to `offset` — called
    /// exactly when an authoritative seek was CLAMPED into the retained
    /// watermark window (Codex R6 H2), i.e. when the consume position was
    /// moved somewhere the pre-clamp base can NEVER reconnect to:
    ///
    /// * retention deleted past the old base (`target < low`, clamped up to
    ///   `low`): the `[base, low)` records no longer exist anywhere, so a
    ///   base pinned below `low` blocks EVERY future commit — the offset
    ///   never advances, and each restart re-consumes + re-publishes the
    ///   whole retained log (repeating durable duplication, the R6 H2
    ///   never-commit);
    /// * the log was truncated/recreated below the old base
    ///   (`target > high`, clamped down to `low`, R5 H1): the old base
    ///   belongs to a DEAD offset space — new-space spans sit entirely
    ///   below it and look "already committed", so nothing is ever
    ///   committed in the new space either.
    ///
    /// In both cases the skipped-over span is unobtainable by construction
    /// (the records are gone from Kafka), so re-basing surrenders nothing
    /// the pin could still protect. Deliberately NOT called for in-range
    /// seeks: a restore seek ABOVE the base (e.g. back to the `resume_floor`
    /// past rows still buffered but not yet durable) must keep the old base,
    /// or a failed publish of those buffered rows followed by a successful
    /// later publish would commit past the gap — exactly the C1 loss the
    /// tracker exists to prevent.
    pub fn reset_base(&mut self, partition: i32, offset: i64) {
        self.next.insert(partition, offset);
    }

    /// Fold a just-published DURABLE segment's covered spans in and return the
    /// per-partition offsets that are now safe to COMMIT (the contiguous
    /// frontier), or `None`/empty when a gap blocks every partition.
    ///
    /// For each partition, the span is contiguous iff it starts exactly at the
    /// current frontier (`start == next`): then the frontier advances to
    /// `span.next` and that value is committable. A span that starts ABOVE the
    /// frontier (a prior roll for `[next, start)` failed and was dropped)
    /// leaves the frontier PINNED at the gap — nothing past it is committed, so
    /// a restart re-consumes from the gap.
    pub fn record_durable(&mut self, spans: &PartitionOffsets) -> BTreeMap<i32, i64> {
        let mut committable = BTreeMap::new();
        for (&partition, span) in spans {
            let frontier = self.next.entry(partition).or_insert(span.start);
            if span.start <= *frontier && span.next > *frontier {
                *frontier = span.next;
                committable.insert(partition, *frontier);
            }
            // A span entirely below the frontier (already committed) or above
            // it (a gap) advances nothing and commits nothing.
        }
        committable
    }

    /// The per-partition durable-contiguous frontier — the offset up to which
    /// each seen partition's records have been durably published CONTIGUOUSLY
    /// from its consume base. Exposed so the rebalance restore
    /// ([`safe_resume_frontier`], Codex R12 H1) and the publish-failure re-seek
    /// ([`arm_publish_failure_reseek`], Codex R12 H2) can floor their targets
    /// at the durable frontier instead of the raw consumed-forward point:
    /// unlike that point, this EXCLUDES records that were consumed but whose
    /// publish FAILED (dropped, never durable — seeking past them is loss).
    #[must_use]
    pub fn durable_next_map(&self) -> &BTreeMap<i32, i64> {
        &self.next
    }

    /// The durable-contiguous frontier for ONE partition (`None` before its
    /// consume base is seeded). See [`durable_next_map`](Self::durable_next_map).
    #[must_use]
    pub fn durable_next(&self, partition: i32) -> Option<i64> {
        self.next.get(&partition).copied()
    }
}

/// FNV-1a 64-bit over `bytes` (offset basis `0xcbf29ce484222325`, prime
/// `0x100000001b3`) — a fixed, dependency-free, platform-stable hash.
/// Used by [`schema_fingerprint`]; see there for why FNV (accident
/// prevention, not tamper resistance) is sufficient.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Append a JSON string literal (with `serde_json`'s stable escaping) to
/// `out`.
fn push_json_str(s: &str, out: &mut String) {
    // `Display` on a JSON value is infallible and deterministic.
    out.push_str(&serde_json::Value::String(s.to_owned()).to_string());
}

/// Append `v` to `out` as CANONICAL JSON: object keys in sorted order,
/// array elements in order, no insignificant whitespace, scalars via
/// `serde_json`'s own (stable) formatting. The workspace enables
/// `serde_json/preserve_order`, so a plain `to_string` would leak the
/// POSTed key order into the fingerprint — two semantically identical
/// `metricsSpec` entries with reordered keys would then fingerprint
/// differently and spuriously refuse a re-create.
fn write_canonical_json(v: &serde_json::Value, out: &mut String) {
    match v {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&str> = map.keys().map(String::as_str).collect();
            keys.sort_unstable();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_json_str(k, out);
                out.push(':');
                if let Some(child) = map.get(*k) {
                    write_canonical_json(child, out);
                }
            }
            out.push('}');
        }
        serde_json::Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical_json(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// Version prefix stamped on every [`schema_fingerprint`] (Codex R27 F1).
///
/// The prefix names the CANONICALISATION version, so a consumer of a
/// stamped `schemaFp` can tell "produced by a different (older/newer)
/// canonicalisation" apart from "same canonicalisation, genuinely
/// different schema". Comparison rule (kept consistent between the
/// overlord's cleanup and this crate): only a SAME-version mismatch is a
/// blocking refusal; a missing fingerprint or one of an unknown/older
/// version is UNVERIFIABLE — such rows are kept + warned about, never
/// auto-dropped and never a refusal. Bump the version whenever the
/// canonical form below changes ("v2" added `isolation.level` to the
/// R26-era unprefixed form).
pub const SCHEMA_FP_VERSION_PREFIX: &str = "v2:";

/// Whether `fp` was produced by the CURRENT fingerprint canonicalisation
/// version (see [`SCHEMA_FP_VERSION_PREFIX`]). `false` means the stamp is
/// unverifiable (older/unknown canonicalisation): callers must treat the
/// row like one with NO fingerprint — keep + warn — never as a match and
/// never as a blocking mismatch (Codex R27 F1/F3).
#[must_use]
pub fn schema_fp_is_current_version(fp: &str) -> bool {
    fp.starts_with(SCHEMA_FP_VERSION_PREFIX)
}

/// Stable fingerprint of the parts of a supervisor spec that determine how
/// a Kafka record is rebuilt into stored rows — the **ingestion semantics**
/// of `dataSchema` (Codex R26 F1) plus the **record-selection semantics**
/// of the effective consumer config (Codex R27 F1).
///
/// The overlord stamps this on every published streaming segment
/// (`schemaFp` in the payload) and compares it before the earliest-replay
/// cleanup: prior rows built under a DIFFERENT ingestion schema cannot be
/// reconstructed by replaying the topic under the new one (e.g. a renamed
/// timestamp column dead-letters every old record), so a create/resume
/// that would drop such rows must refuse instead of silently losing them.
///
/// Covered (canonical form, in this order): `dimensionsSpec.dimensions`
/// (names + EFFECTIVE materialised types, declared order preserved — order
/// changes the stored segment), `granularitySpec` (`rollup`,
/// `queryGranularity` lowercased), the EFFECTIVE `isolation.level` (from
/// `ioConfig.consumerProperties`, defaulting to `read_committed` — the
/// pinned librdkafka 2.12.1 default applied when the property is
/// unspecified), `metricsSpec` (each entry as canonical JSON, declared
/// order), and `timestampSpec` (column + the EFFECTIVE extraction grammar
/// `auto`/`iso`/`millis`, exactly as `build_consumer_config` maps it).
///
/// `isolation.level` is included because it changes WHICH records a replay
/// delivers, not how they are rebuilt: a `read_uncommitted` generation
/// also ingested aborted transactional records, which a `read_committed`
/// replay NEVER redelivers — an identical-`dataSchema` re-create would
/// otherwise drop those rows as "rebuilt by the replay" and lose them
/// (Codex R27 F1). It is the only forwarded consumer property that selects
/// among the subscribed topic's records (librdkafka 2.12.1 CONFIGURATION
/// sweep): the offset/commit/group/rebalance keys are runtime-managed,
/// the fetch/queue keys only size transport buffers, `topic.blacklist` is
/// REJECTED at validation (Codex R29 — it could blank the subscription and
/// starve an earliest re-create's replay), and `allow.auto.create.topics`
/// can only blank out the topic wholesale (a supervisor that visibly
/// ingests nothing), never deliver a subset.
///
/// Deliberately EXCLUDED: `dataSource` and the rest of `ioConfig` (the
/// cleanup is already keyed on the (datasource, topic) pair and cluster
/// identity is separate provenance; `useEarliestOffset` gates whether the
/// cleanup runs at all), tuning / flush sizing (affects segment
/// BOUNDARIES, not row reconstruction), and `dimensionExclusions` (not
/// honoured by streaming ingestion). A plain `"page"` dimension and
/// `{"name":"page","type":"string"}` fingerprint identically — they
/// materialise identically.
///
/// Hash choice: FNV-1a 64-bit over the canonical string, hex-encoded,
/// prefixed with [`SCHEMA_FP_VERSION_PREFIX`]. FNV is chosen over the
/// workspace's `sha2` because neither this crate nor the overlord
/// currently depends on it (adding the edge is out of scope) and the
/// fingerprint defends against operator ACCIDENTS (a schema edit on the
/// same pair), not adversarial collisions — the spec author and the
/// provenance author are the same trust domain. Any change to this
/// canonicalisation must bump the version prefix: stamps of other versions
/// are then treated as UNVERIFIABLE (keep + warn), never dropped and never
/// a blocking refusal (Codex R27 F1/F3).
#[must_use]
pub fn schema_fingerprint(spec: &KafkaSupervisorSpec) -> String {
    let ds = &spec.data_schema;
    let mut canon = String::from("{\"dims\":[");
    for (i, d) in ds.dimensions_spec.dimensions.iter().enumerate() {
        if i > 0 {
            canon.push(',');
        }
        let (name, eff_type) = match d {
            DimensionEntry::String(name) => (name.as_str(), "string"),
            DimensionEntry::Typed { name, dim_type } => (
                name.as_str(),
                match dim_type.as_str() {
                    "long" => "long",
                    "float" => "float",
                    "double" => "double",
                    // Mirror `dimension_entry_to_schema`: any other declared
                    // type is materialised as a string dimension.
                    _ => "string",
                },
            ),
        };
        canon.push('[');
        push_json_str(name, &mut canon);
        canon.push_str(",\"");
        canon.push_str(eff_type);
        canon.push_str("\"]");
    }
    canon.push_str("],\"gran\":");
    match &ds.granularity_spec {
        Some(g) => {
            canon.push_str("{\"qg\":");
            push_json_str(&g.query_granularity.to_ascii_lowercase(), &mut canon);
            canon.push_str(",\"rollup\":");
            canon.push_str(match g.rollup {
                Some(true) => "true",
                Some(false) => "false",
                None => "null",
            });
            canon.push('}');
        }
        None => canon.push_str("null"),
    }
    // EFFECTIVE record-selection semantics (Codex R27 F1): the isolation
    // level the consumer will actually run with. Unspecified means the
    // librdkafka default, `read_committed` (pinned: rdkafka-sys
    // 4.10.0+2.12.1 / librdkafka 2.12.1, rdkafka_conf.c `.vdef =
    // RD_KAFKA_READ_COMMITTED`), stamped EXPLICITLY so an unspecified spec
    // and a spelled-out `read_committed` fingerprint identically.
    canon.push_str(",\"isolation\":");
    push_json_str(
        spec.io_config
            .consumer_properties
            .get("isolation.level")
            .map_or("read_committed", String::as_str),
        &mut canon,
    );
    canon.push_str(",\"metrics\":[");
    for (i, m) in ds.metrics_spec.iter().enumerate() {
        if i > 0 {
            canon.push(',');
        }
        write_canonical_json(m, &mut canon);
    }
    canon.push_str("],\"ts\":[");
    push_json_str(&ds.timestamp_spec.column, &mut canon);
    // The EFFECTIVE extraction grammar (validate() restricts the format to
    // these three; the same mapping `build_consumer_config` applies).
    canon.push_str(
        match ds.timestamp_spec.format.to_ascii_lowercase().as_str() {
            "iso" => ",\"iso\"]}",
            "millis" => ",\"millis\"]}",
            _ => ",\"auto\"]}",
        },
    );
    format!(
        "{SCHEMA_FP_VERSION_PREFIX}{:016x}",
        fnv1a_64(canon.as_bytes())
    )
}

/// [`ConsumerContext`](rdkafka::consumer::ConsumerContext) that records
/// partition revocations/assignments into a shared [`RebalanceNotices`]
/// accumulator (compat-3 durability, Codex R4 H2), so the streaming loop can
/// re-arm the [`ResumeSeekGate`] for rebalance-affected partitions
/// ([`rearm_gate_after_rebalance`]) BEFORE consuming any post-rebalance
/// record. It changes nothing about rdkafka's default rebalance handling
/// (the incremental assign/unassign for the pinned `cooperative-sticky`
/// protocol still runs via the default
/// [`ConsumerContext::rebalance`](rdkafka::consumer::ConsumerContext::rebalance));
/// it only OBSERVES.
///
/// Ordering guarantee this relies on (rdkafka 0.37): rebalance events and
/// records flow through the SAME consumer event queue, and the callbacks run
/// inline on the polling thread (base_consumer.rs `poll_queue` →
/// `handle_rebalance_event` → `ConsumerContext::rebalance`), so a
/// (re-)assignment is always recorded here BEFORE the first post-rebalance
/// record of that partition can be returned by `recv()` — the loop's
/// drain-then-verdict order can therefore never consume a record the re-arm
/// should have gated.
///
/// Requires the `kafka-io` feature.
#[cfg(feature = "kafka-io")]
#[derive(Debug)]
pub struct GateConsumerContext {
    /// The subscribed topic; rebalance entries for other topics (impossible
    /// under the single-topic subscribe, defensive) are ignored.
    topic: String,
    /// Shared accumulator drained by the streaming loop.
    notices: RebalanceNotices,
}

#[cfg(feature = "kafka-io")]
impl GateConsumerContext {
    /// Context observing rebalances of `topic`.
    #[must_use]
    pub fn new(topic: String) -> Self {
        Self {
            topic,
            notices: RebalanceNotices::new(),
        }
    }

    /// A handle onto the shared accumulator (clones share state).
    #[must_use]
    pub fn notices(&self) -> RebalanceNotices {
        self.notices.clone()
    }

    /// The partition ids of `tpl` entries belonging to the observed topic.
    fn own_partitions(&self, tpl: &rdkafka::TopicPartitionList) -> Vec<i32> {
        tpl.elements()
            .iter()
            .filter(|e| e.topic() == self.topic)
            .map(rdkafka::topic_partition_list::TopicPartitionListElem::partition)
            .collect()
    }
}

#[cfg(feature = "kafka-io")]
impl rdkafka::client::ClientContext for GateConsumerContext {}

#[cfg(feature = "kafka-io")]
impl rdkafka::consumer::ConsumerContext for GateConsumerContext {
    // Revocations are recorded PRE (before the incremental unassign), and
    // assignments POST (after the incremental assign has been applied), so a
    // drained notice always describes a state librdkafka has already
    // reached.
    fn pre_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        if let rdkafka::consumer::Rebalance::Revoke(tpl) = rebalance {
            self.notices.record_revoked(self.own_partitions(tpl));
        }
    }

    fn post_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        match rebalance {
            rdkafka::consumer::Rebalance::Assign(tpl) => {
                self.notices.record_assigned(self.own_partitions(tpl));
            }
            rdkafka::consumer::Rebalance::Revoke(_) => {}
            rdkafka::consumer::Rebalance::Error(e) => {
                tracing::warn!(
                    topic = %self.topic, error = %e,
                    "rebalance ERROR: rdkafka unassigns everything — every tracked \
                     partition will be re-armed for a fresh authoritative seek (H2)",
                );
                self.notices.record_lost_all();
            }
        }
    }
}

/// The concrete Kafka consumer type produced by [`create_stream_consumer`],
/// re-exported so dependents (e.g. the overlord) can name it without a
/// direct `rdkafka` dependency. Carries the [`GateConsumerContext`] rebalance
/// observer (Codex R4 H2). Requires the `kafka-io` feature.
#[cfg(feature = "kafka-io")]
pub type KafkaStreamConsumer = rdkafka::consumer::StreamConsumer<GateConsumerContext>;

/// Error returned by a [`SegmentSink`] when it cannot make a rolled
/// segment queryable.
#[derive(Debug, thiserror::Error)]
#[error("segment sink publish failed: {0}")]
pub struct SegmentSinkError(pub String);

/// A destination for rolled segments produced by streaming ingestion.
///
/// The overlord implements this by running the same collision-safe
/// publish tail as batch `index_parallel` (allocate id → metadata
/// transaction → query-visible swap), so a Kafka-fed datasource becomes
/// queryable exactly like a batch-ingested one.
pub trait SegmentSink: Send + Sync {
    /// Publish one rolled segment, making its rows queryable. `Ok(())`
    /// means the segment is durable + live and the loop commits its offsets
    /// (compat-3 stage 2); `Err` is logged, the rolled rows are dropped from
    /// memory, and NO offset is committed — so those records are re-consumed
    /// on restart (at-least-once, no loss). A failure during the FINAL drain
    /// on shutdown additionally surfaces in
    /// [`StreamingStats::final_flush_failed`] (Codex R26 F2) so the caller
    /// does not record a lossy stop as clean.
    fn publish(
        &self,
        segment: IngestedSegment,
    ) -> impl std::future::Future<Output = Result<(), SegmentSinkError>> + Send;

    /// Publish one rolled segment ALONG WITH the per-partition Kafka offset
    /// spans it covers (compat-3 stage 2), so a durable sink can stamp them
    /// into the segment's `payload.kafkaOffsets` for resume-frontier
    /// derivation. The default IGNORES the offsets and delegates to
    /// [`publish`](Self::publish) — a test / memory-only sink need not
    /// override it; the overlord's durable sink does. The poll loop commits
    /// the offsets to Kafka only after THIS returns `Ok`.
    fn publish_with_offsets(
        &self,
        segment: IngestedSegment,
        _offsets: &PartitionOffsets,
    ) -> impl std::future::Future<Output = Result<(), SegmentSinkError>> + Send {
        self.publish(segment)
    }

    /// Whether a successful [`publish`](Self::publish) of this sink is DURABLE
    /// enough to commit its offsets to Kafka (compat-3 durability, C3).
    ///
    /// `false` (the default, taken by memory-only test sinks AND by the real
    /// overlord sink when NO deep-storage backend is configured) means a
    /// published segment is NOT persisted, so committing its offset would let
    /// a restart skip records whose only copy vanished with the process
    /// (loss). The poll loop therefore commits NOTHING for such a sink and
    /// relies on `auto.offset.reset` replay instead — non-durable, but
    /// loss-free. The overlord's durable sink returns `true` exactly when its
    /// deep-storage backend is present (a failed persist already aborts the
    /// publish with `Err`, so `Ok` + `true` ⇒ the blob is durable).
    fn commits_offsets(&self) -> bool {
        false
    }
}

/// Row accumulator that rolls buffered Kafka records into an
/// [`IngestedSegment`] on demand.
///
/// Kept free of any Kafka I/O so it can be exercised in ordinary
/// `cargo test` without a broker or the `kafka-io` feature.
pub struct StreamingBuffer {
    data_source: String,
    timestamp_column: String,
    /// Declared `timestampSpec.format`, honored at push-time validation AND
    /// at roll-time extraction (both must agree or a record could validate
    /// under one grammar and be stored under another — Fable audit).
    timestamp_format: TsFormat,
    dim_schemas: Vec<DimensionSchema>,
    metrics_specs: Vec<serde_json::Value>,
    max_rows_per_segment: usize,
    rows: Vec<serde_json::Value>,
    /// Running sum of accepted record payload bytes since the last roll,
    /// used to trigger a size-based flush well before `maxRowsPerSegment`
    /// (which can legitimately be up to 1e9) lets the heap blow up.
    buffered_bytes: usize,
    total_consumed: u64,
    total_published: u64,
    /// Names of the LONG-typed dimensions, for the exact-storage pre-flight.
    long_dims: Vec<String>,
    /// Per-[`long_dims`](Self::long_dims) `(seen_null, seen_over_±2^53)` since
    /// the last roll. A record that would make any long column mix a NULL with
    /// an out-of-±2^53 value cannot be stored exactly (the batch ingester fails
    /// the WHOLE segment), so such a record is dead-lettered (Codex R14).
    long_dim_flags: Vec<(bool, bool)>,
    /// Per-partition `[start, next)` Kafka offset span covered by the records
    /// buffered since the last roll (compat-3 stage 2). Fed by
    /// [`record_offset`](Self::record_offset) as each message is consumed and
    /// moved out at [`roll_with_offsets`](Self::roll_with_offsets) so the
    /// rolled segment carries exactly the offsets it materialised — the durable
    /// resume-frontier source. Includes the offsets of dead-lettered records
    /// (their message WAS consumed), matching Druid's "commit past
    /// unparseable" behaviour.
    offset_spans: PartitionOffsets,
}

/// Byte ceiling on the un-rolled buffer, measured on RAW record bytes.
/// Independent of the row count, a supervisor with a huge
/// `maxRowsPerSegment` could otherwise accumulate GiBs before the flush
/// timer fires and OOM the shared process. The cap is on raw input bytes;
/// parsed `serde_json::Value` rows can be several× larger, so this is set
/// conservatively at 32 MiB (Codex R7) to bound the worst-case parsed heap
/// to a few hundred MiB rather than GiBs. Rolls regardless of row count.
const MAX_BUFFERED_BYTES: usize = 32 * 1024 * 1024;

impl StreamingBuffer {
    /// Build a buffer from a consumer config (the segment schema and the
    /// roll threshold are taken from it).
    #[must_use]
    pub fn from_config(config: &KafkaConsumerConfig) -> Self {
        let long_dims: Vec<String> = config
            .dim_schemas
            .iter()
            .filter(|d| d.dim_type == DimensionType::Long)
            .map(|d| d.name.clone())
            .collect();
        let long_dim_flags = vec![(false, false); long_dims.len()];
        Self {
            data_source: config.data_source.clone(),
            timestamp_column: config.timestamp_column.clone(),
            timestamp_format: config.timestamp_format,
            dim_schemas: config.dim_schemas.clone(),
            metrics_specs: config.metrics_specs.clone(),
            // `max(1)` is pure defense: `KafkaSupervisorSpec::validate`
            // already rejects a 0 `maxRowsPerSegment` and
            // `build_consumer_config` defaults it to 5_000_000, so this is
            // never 0 in practice. If one slipped through, rolling every
            // record is far safer than a 0 that would `>=`-trigger on the
            // first push anyway — it just keeps segments tiny, never
            // unbounded.
            max_rows_per_segment: config.max_rows_per_segment.max(1),
            rows: Vec::new(),
            buffered_bytes: 0,
            total_consumed: 0,
            total_published: 0,
            long_dims,
            long_dim_flags,
            offset_spans: PartitionOffsets::new(),
        }
    }

    /// Record that a Kafka message at `(partition, offset)` was consumed,
    /// extending that partition's `[start, next)` span to cover it (compat-3
    /// stage 2). Mirrors
    /// [`PendingBatch::record`](crate::eos_writer::PendingBatch::record): the
    /// span `next` advances to `offset + 1` (the value manual-committed to
    /// Kafka on a successful publish). Called once per Kafka MESSAGE — a
    /// multi-object record shares one offset — and even for a dead-lettered
    /// record, so the resume frontier advances past consumed-but-unstored
    /// messages exactly as Druid commits past unparseable ones.
    pub fn record_offset(&mut self, partition: i32, offset: i64) {
        // The span `next` is offset+1; saturate so a record at i64::MAX (a
        // protocol-valid signed offset) cannot overflow — a raw `offset + 1`
        // panics in debug and in release wraps to i64::MIN, persisting an
        // uncommittable `[i64::MAX, i64::MIN)` span that is re-published every
        // restart (unbounded double-count). saturating_add(1) leaves next at
        // i64::MAX so the record commits normally (mirrors the durable
        // frontier's own saturating advance).
        let next = offset.saturating_add(1);
        let entry = self
            .offset_spans
            .entry(partition)
            .or_insert_with(|| OffsetSpan::new(offset, next));
        if offset < entry.start {
            entry.start = offset;
        }
        if next > entry.next {
            entry.next = next;
        }
    }

    /// Bounds-check, parse, validate, and buffer one Kafka record payload.
    ///
    /// Returns `Ok(true)` when the buffer has reached
    /// `max_rows_per_segment` and the caller should [`roll`](Self::roll);
    /// `Ok(false)` when more room remains. A malformed / oversized /
    /// over-deep payload — OR a row that is not a JSON object / is missing
    /// its timestamp column — yields `Err` and is NOT buffered, so the
    /// caller can dead-letter that ONE record instead of letting it poison
    /// the whole batch's [`roll`](Self::roll) (which fails atomically if
    /// any row lacks a timestamp). See [`check_record_bounds`].
    pub fn push_payload(&mut self, payload: &[u8]) -> Result<bool, ConsumerError> {
        check_record_bounds(payload)?;
        // A single Kafka record may carry MULTIPLE whitespace-separated (or
        // concatenated) JSON objects; Druid's `json` inputFormat reads each as
        // its own row. Parse and VALIDATE every object up front, then append
        // them all atomically — a single malformed / non-object / timestamp-
        // less object dead-letters the WHOLE record rather than ingesting a
        // partial prefix (each object is validated with the SAME function the
        // ingester uses at `roll()` time, so every buffered row survives the
        // segment build).
        let mut parsed = Vec::new();
        let mut stream =
            serde_json::Deserializer::from_slice(payload).into_iter::<serde_json::Value>();
        for item in &mut stream {
            let row = item.map_err(|e| ConsumerError::ParseError(e.to_string()))?;
            if !row.is_object() {
                return Err(ConsumerError::ParseError(
                    "record is not a JSON object".to_string(),
                ));
            }
            ferrodruid_ingest_batch::extract_row_timestamp_millis_fmt(
                &row,
                &self.timestamp_column,
                self.timestamp_format,
            )
            .map_err(|e| ConsumerError::ParseError(e.to_string()))?;
            parsed.push(row);
        }
        if parsed.is_empty() {
            return Err(ConsumerError::ParseError(
                "record contained no JSON value".to_string(),
            ));
        }
        // Exact-storage pre-flight (Codex R14): a segment whose LONG column
        // mixes a NULL with an out-of-±2^53 value cannot be stored exactly and
        // would fail the WHOLE roll (losing every buffered row). Simulate this
        // record over the running per-long-dim state; if it would introduce
        // that conflict, dead-letter the record atomically instead of poisoning
        // the batch. Committed only after ALL long dims pass. No long dims →
        // zero cost (the loop body never runs).
        if !self.long_dims.is_empty() {
            let mut flags = self.long_dim_flags.clone();
            for (i, dim) in self.long_dims.iter().enumerate() {
                for row in &parsed {
                    match long_dim_value_class(row, dim) {
                        None if flags[i].1 => {
                            return Err(ConsumerError::ParseError(format!(
                                "long dimension '{dim}': record mixes a NULL with an \
                                 out-of-±2^53 value in the same segment (not exactly storable)"
                            )));
                        }
                        None => flags[i].0 = true,
                        Some(true) if flags[i].0 => {
                            return Err(ConsumerError::ParseError(format!(
                                "long dimension '{dim}': record mixes an out-of-±2^53 value \
                                 with a NULL in the same segment (not exactly storable)"
                            )));
                        }
                        Some(true) => flags[i].1 = true,
                        Some(false) => {}
                    }
                }
            }
            self.long_dim_flags = flags;
        }
        let added = parsed.len() as u64;
        self.rows.append(&mut parsed);
        self.buffered_bytes = self.buffered_bytes.saturating_add(payload.len());
        self.total_consumed += added;
        // Roll on EITHER the row-count threshold OR the byte ceiling, so a
        // very large `maxRowsPerSegment` cannot let the heap grow unbounded.
        Ok(self.rows.len() >= self.max_rows_per_segment
            || self.buffered_bytes >= MAX_BUFFERED_BYTES)
    }

    /// Whether the buffer currently holds no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Number of un-rolled rows currently buffered.
    #[must_use]
    pub fn pending_rows(&self) -> usize {
        self.rows.len()
    }

    /// The per-partition `[start, next)` Kafka offset spans of the records
    /// CURRENTLY buffered (consumed since the last roll, not yet durable) —
    /// the in-memory, still-RECOVERABLE tail above the durable frontier
    /// (Codex R12 H1). A rebalance restore floors its target at the
    /// durable-OR-buffered frontier ([`safe_resume_frontier`]): these buffered
    /// rows are re-published from the buffer on the next roll, so a restore
    /// that SKIPS past them is dup-free — whereas a DROPPED (failed-publish)
    /// batch is absent here (a failed roll drains this map), so it can never
    /// lift the floor past it (that would be loss). Includes dead-lettered
    /// records' offsets (their message was consumed), matching the
    /// commit-past-unparseable resume semantics.
    #[must_use]
    pub fn buffered_offset_spans(&self) -> &PartitionOffsets {
        &self.offset_spans
    }

    /// Roll the buffered rows into one [`IngestedSegment`], emptying the
    /// buffer. Returns `Ok(None)` when the buffer is empty. Convenience over
    /// [`roll_with_offsets`](Self::roll_with_offsets) that discards the
    /// covered Kafka offset spans (used by tests that do not exercise offset
    /// tracking).
    pub fn roll(&mut self) -> Result<Option<IngestedSegment>, ConsumerError> {
        Ok(self.roll_with_offsets()?.map(|(segment, _offsets)| segment))
    }

    /// Roll the buffered rows into one [`IngestedSegment`] AND the
    /// per-partition Kafka offset spans it covers (compat-3 stage 2),
    /// emptying both the row buffer and the offset spans. Returns `Ok(None)`
    /// when the buffer holds no rows (the offset spans are then retained so a
    /// later non-empty roll still covers any dead-lettered messages consumed
    /// in the meantime).
    pub fn roll_with_offsets(
        &mut self,
    ) -> Result<Option<(IngestedSegment, PartitionOffsets)>, ConsumerError> {
        if self.rows.is_empty() {
            return Ok(None);
        }
        let rows = std::mem::take(&mut self.rows);
        let offsets = std::mem::take(&mut self.offset_spans);
        self.buffered_bytes = 0;
        // The exact-storage conflict is per-segment; reset the long-dim state
        // for the next segment (Codex R14).
        self.long_dim_flags
            .iter_mut()
            .for_each(|f| *f = (false, false));
        let ingester = BatchIngester::with_schemas(
            self.data_source.clone(),
            self.timestamp_column.clone(),
            self.dim_schemas.clone(),
            self.metrics_specs.clone(),
        )
        .with_timestamp_format(self.timestamp_format);
        let segment = ingester
            .ingest(rows)
            .map_err(|e| ConsumerError::IngestionError(e.to_string()))?;
        self.total_published += 1;
        Ok(Some((segment, offsets)))
    }

    /// Total records accepted into the buffer over its lifetime.
    #[must_use]
    pub fn total_consumed(&self) -> u64 {
        self.total_consumed
    }

    /// Total segments successfully rolled over the buffer's lifetime.
    #[must_use]
    pub fn total_published(&self) -> u64 {
        self.total_published
    }
}

/// Summary returned when a streaming consumer stops.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamingStats {
    /// Records consumed from Kafka and buffered.
    pub total_consumed: u64,
    /// Segments rolled and published to the sink.
    pub total_published: u64,
    /// Whether the FINAL drain — the roll + publish of the residual buffer
    /// when the shutdown signal arrived — failed (Codex R26 F2). The final
    /// drain runs on the way OUT: if the caller then tombstones/suspends
    /// the supervisor as "cleanly stopped", nothing ever replays the lost
    /// rows. The overlord therefore treats `true` as a FAILED shutdown and
    /// keeps the supervisor metadata active so a restart/re-create replays
    /// the topic and rebuilds the rows.
    pub final_flush_failed: bool,
    /// Whether ANY mid-stream (row-threshold or flush-timer) roll/publish
    /// failed over the consumer's lifetime — STICKY: once set it is never
    /// cleared by later successes (Codex R27 F2). Mid-stream failures are
    /// non-fatal for the poll LOOP (offsets are never committed, so the
    /// dropped rows are re-consumed on restart), but the rows are only
    /// actually rebuilt if a replay HAPPENS: a subsequent clean — often
    /// EMPTY — final drain must not launder the loss into a "cleanly
    /// stopped" shutdown, or the tombstone/suspend forecloses every replay
    /// and the rows are silently gone. The overlord treats `true` exactly
    /// like [`final_flush_failed`](Self::final_flush_failed): the shutdown
    /// FAILS and the metadata stays active (see
    /// [`replay_required`](Self::replay_required)).
    pub mid_stream_flush_failed: bool,
    /// Whether the consumer STOPPED ITSELF on a fatal consume error
    /// (authorization failure / librdkafka fatal error — Codex R30 F2, see
    /// [`consume_error_is_fatal`]) instead of delivering the topic's
    /// records. From that moment on nothing was consumed, so the consumer
    /// cannot claim it made the topic's data queryable — in the worst case
    /// it had already earliest-dropped the pair's prior segments and then
    /// rebuilt NONE of them. A tombstone/suspend recorded off a stop with
    /// this set would foreclose the only recovery (a replay), so the
    /// overlord treats it exactly like a flush failure (see
    /// [`replay_required`](Self::replay_required)). Pre-R30 the recv error
    /// was warn-looped forever and an empty-buffer shutdown then looked
    /// perfectly clean.
    pub fatal_consume_error: bool,
    /// Whether the consumer STOPPED ITSELF because its live Kafka **cluster
    /// id** DRIFTED away from the one resolved and stamped at start (Codex
    /// R37 F3). The R30 F1 `metadata.recovery.strategy=none` pin stops a
    /// re-BOOTSTRAP from migrating clusters, but a plain broker RECONNECT is
    /// a different path: if a known broker hostname is repointed to a
    /// DIFFERENT cluster, librdkafka reconnects (warn only) and refreshes the
    /// cached cluster id to the new one, while the sink keeps stamping the
    /// START-time id — mis-attributing cluster B's rows as cluster A. A later
    /// earliest re-create against A would then DROP those rows (its replay
    /// can never rebuild B's data) — permanent loss. When the loop detects
    /// the live id no longer matches the stamped one — on the periodic
    /// tripwire OR on the R6 H4 pre-publish identity check that guards
    /// every non-empty publish (so a drift between ticks cannot be durably
    /// stamped) — it refuses to publish the (possibly-B) buffered rows and
    /// stops fail-loud: the stop is replay-required, and a restart
    /// re-resolves the identity and rebuilds under the correct cluster.
    /// Only ever set when the start-time identity was KNOWN (`Some`); an
    /// unresolvable live id is NOT treated as drift (fail-safe — never
    /// block a publish on a transient metadata gap).
    pub cluster_id_drifted: bool,
}

impl StreamingStats {
    /// Whether this consumer's stop must NOT be recorded as clean: buffered
    /// rows were dropped without ever becoming queryable — by a failed
    /// final drain (Codex R26 F2) and/or a failed mid-stream publish (Codex
    /// R27 F2) — and/or the consumer aborted on a fatal consume error
    /// without having delivered the topic's records (Codex R30 F2) — and/or
    /// it stopped on a detected cluster-identity DRIFT and refused to publish
    /// the mis-attributed buffer (Codex R37 F3). Either
    /// way ONLY a Kafka replay (restart/resume or an earliest re-create)
    /// can rebuild the missing rows. Callers recording this consumer's stop
    /// MUST fail the stop (no tombstone/suspend persist) while this is
    /// `true`, keeping the supervisor metadata active so that replay
    /// happens.
    #[must_use]
    pub fn replay_required(&self) -> bool {
        self.final_flush_failed
            || self.mid_stream_flush_failed
            || self.fatal_consume_error
            || self.cluster_id_drifted
    }
}

/// Whether a freshly re-fetched live Kafka cluster id represents a DRIFT away
/// from the `expected` (start-time) identity (Codex R37 F3).
///
/// Fail-safe polarity: drift is claimed ONLY when the live id is KNOWN
/// (`Some`) and DIFFERS from `expected`. An unresolvable live id (`None` —
/// a transient metadata gap, all brokers momentarily down) is NEVER drift, so
/// the drift guard can only ever cost a publish being deferred to the next
/// roll (when the identity resolves again), never a spurious fail-loud stop.
/// Kept pure and un-gated so the polarity is unit-testable without a broker.
#[must_use]
pub fn cluster_id_is_drift(live: Option<&str>, expected: &str) -> bool {
    matches!(live, Some(l) if l != expected)
}

/// Whether a PUBLISH must be refused because the live cluster identity has
/// confirmably drifted away from the identity the sink stamps (Codex R6 H4).
///
/// The sink stamps the START-time cluster id (`clusterId` + `kafkaOffsets`)
/// onto every durable segment; the R37 F3 tripwire only re-checks the live
/// id on a 60 s cadence, so a segment flushed right after a broker repoint
/// (before the next tick) would durably stamp the NEW cluster's records
/// with the OLD cluster's provenance — a later earliest re-create against
/// the old cluster then drops rows its replay can never rebuild. This
/// verdict runs immediately BEFORE each non-empty publish: only a
/// CONFIRMED drift (live id known and different) refuses, exactly the
/// [`cluster_id_is_drift`] polarity —
///
/// * `expected == None`: nothing was resolved at start, so nothing is
///   stamped (`kafkaOffsets` are omitted, the cleanup keeps such rows) —
///   there is no identity to protect, publish;
/// * live unresolvable (`None`): NOT drift (fail-safe — a transient
///   metadata gap can defer detection, never wedge publishing);
/// * live known and equal: publish (the stable path);
/// * live known and DIFFERENT: refuse — the caller stops fail-loud and
///   discards the (possibly other-cluster) buffer rather than durably
///   mis-stamping it.
///
/// Pure so the verdict is unit-testable without a broker. Honest residual
/// (KIP-516): a repoint the broker hides (no cluster id in metadata, or a
/// drift landing in the instants between this check and the metadata
/// write) is undetectable under `#![forbid(unsafe_code)]` rdkafka — durable
/// per-cluster provenance (FG-7) is the real fix.
#[must_use]
pub fn publish_blocked_by_identity_drift(live: Option<&str>, expected: Option<&str>) -> bool {
    match expected {
        Some(e) => cluster_id_is_drift(live, e),
        None => false,
    }
}

/// Build, create, and subscribe a Kafka [`StreamConsumer`] SYNCHRONOUSLY,
/// so a bad config (e.g. an invalid librdkafka property in
/// `additional_properties`, or an unroutable broker string) fails HERE —
/// before the supervisor is acknowledged as running — rather than inside a
/// detached task that would exit immediately leaving a dead handle
/// (Codex 2026-07-13).
///
/// **Offset policy (compat-3 stage 2, see the module header):** the consumer
/// runs with `enable.auto.commit=false` and MANUAL-commits the per-partition
/// offset only AFTER a rolled segment is durably published (deep storage +
/// metadata), so a failed publish commits nothing and its records are
/// re-consumed on restart (at-least-once). The group id is STABLE
/// (`ferrodruid-{supervisor_id}`), so Kafka returns the committed offset on
/// resume and the consumer continues past the durable frontier;
/// `auto.offset.reset` (`earliest` when `useEarliestOffset`) governs only a
/// FIRST start with no committed offset. The correctness-critical client
/// properties (offset/commit/group keys and the `cooperative-sticky`
/// rebalance pin — Codex R29) are applied AFTER any caller-supplied
/// `additional_properties` so a spec cannot re-enable auto-commit, repoint the
/// group, or revert to an eager rebalance.
///
/// Requires the `kafka-io` feature (pulls in `rdkafka` / librdkafka).
#[cfg(feature = "kafka-io")]
pub fn create_stream_consumer(
    config: &KafkaConsumerConfig,
) -> Result<KafkaStreamConsumer, ConsumerError> {
    use rdkafka::consumer::Consumer;

    // The rebalance observer (Codex R4 H2): revocations/assignments are
    // recorded into the context's shared `RebalanceNotices`, which the
    // streaming loop reads back via `consumer.client().context().notices()`.
    let consumer: KafkaStreamConsumer = build_client_config(config)
        .create_with_context(GateConsumerContext::new(config.topic.clone()))
        .map_err(|e| ConsumerError::KafkaError(e.to_string()))?;
    consumer
        .subscribe(&[&config.topic])
        .map_err(|e| ConsumerError::KafkaError(e.to_string()))?;
    Ok(consumer)
}

/// Lower bound the runtime enforces on `max.poll.interval.ms` (Codex R36):
/// librdkafka's own default (rdkafka-sys 4.10.0+2.12.1 CONFIGURATION.md,
/// range 1..86400000). A caller may raise the interval above this (e.g. to
/// keep it `>= session.timeout.ms`, which librdkafka requires) but not lower
/// it — a value below librdkafka's ~500 ms check cadence would let a
/// mid-publish block revoke every partition and force a duplicating
/// earliest re-consume. See [`build_client_config`].
#[cfg(feature = "kafka-io")]
const MAX_POLL_INTERVAL_FLOOR_MS: u64 = 300_000;

/// Upper bound the runtime enforces on `max.poll.interval.ms` — librdkafka's
/// documented maximum (rdkafka-sys 4.10.0+2.12.1 CONFIGURATION.md, range
/// 1..86400000). It is NOT cosmetic: librdkafka parses the key with
/// `(int)strtol(...)` before range-checking, so a value above the C `int`
/// range narrows (e.g. `4294967297` → `1`) and would silently restore the
/// pathological interval the floor blocks. Clamping the value we `.set` to
/// this maximum keeps it inside the `int` range so it can never wrap.
#[cfg(feature = "kafka-io")]
const MAX_POLL_INTERVAL_CEIL_MS: u64 = 86_400_000;

/// Ceiling (and the safe default) the runtime enforces on
/// `topic.metadata.refresh.interval.ms` (Codex R37 F2): librdkafka's own
/// default (rdkafka-sys 4.10.0+2.12.1 CONFIGURATION.md, range -1..3600000,
/// default 300000). The periodic metadata timer is what discovers partitions
/// ADDED to the topic after start; `-1` DISABLES it (records on new partitions
/// are never assigned and are lost at retention). The runtime therefore never
/// lets the effective interval be slower than this default (a slower refresh
/// re-opens the discovery-lag loss on short-retention topics) — `-1` / `0` /
/// unparseable / any value above it collapse to this ceiling.
#[cfg(feature = "kafka-io")]
const METADATA_REFRESH_CEIL_MS: u64 = 300_000;

/// Floor the runtime enforces on a POSITIVE caller
/// `topic.metadata.refresh.interval.ms` (Codex R37 F2 / round-3): a spec may
/// legitimately set a FASTER refresh than the default (short-retention topics
/// that add partitions need prompt discovery), and that value is PRESERVED —
/// but not below this floor, so a spec cannot make librdkafka hammer the broker
/// with metadata requests (a self-inflicted load DoS). 1000 ms is far faster
/// than any real discovery need yet safely above pathological.
#[cfg(feature = "kafka-io")]
const METADATA_REFRESH_FLOOR_MS: u64 = 1_000;

/// Assemble the librdkafka [`ClientConfig`](rdkafka::config::ClientConfig)
/// for [`create_stream_consumer`]: caller-supplied `additional_properties`
/// FIRST (security.protocol, SASL/SSL, …), then the runtime-managed,
/// correctness-critical keys LAST so they win over anything a spec tried
/// to set (defence-in-depth: `build_consumer_config` already strips most of
/// these from `additional_properties`; `max.poll.interval.ms` is forwarded
/// and FLOORED here so a legitimately-large caller value survives). Split
/// out so the pinned keys are unit-testable without a broker.
#[cfg(feature = "kafka-io")]
fn build_client_config(config: &KafkaConsumerConfig) -> rdkafka::config::ClientConfig {
    let mut client = rdkafka::config::ClientConfig::new();
    // Caller-supplied properties FIRST (security.protocol, SASL/SSL, …)…
    for (k, v) in &config.additional_properties {
        client.set(k, v);
    }
    // `max.poll.interval.ms` FLOOR + CEILING (Codex R36): compute the effective
    // value as `caller.clamp(300000, 86400000)` from the caller's own property
    // (forwarded above), then re-set it LAST so it wins. A caller value inside
    // librdkafka's documented range is PRESERVED (a spec that legitimately
    // raises the interval — e.g. to keep it >= `session.timeout.ms`, which
    // librdkafka REQUIRES — still starts); an absent / unparseable / below-floor
    // value (the finding's `1`) is raised to the default. The CEILING is not
    // cosmetic: librdkafka parses this key with `(int)strtol(...)` BEFORE its
    // range check, so a `u64` above the C `int` range would NARROW inside
    // librdkafka — e.g. `4294967297` wraps to `1`, silently restoring the
    // pathological interval the floor exists to block. Clamping to the
    // documented maximum keeps the value we `.set` within the `int` range so it
    // can never wrap. See the `.set` below for why.
    let max_poll_ms = config
        .additional_properties
        .get("max.poll.interval.ms")
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map_or(MAX_POLL_INTERVAL_FLOOR_MS, |v| {
            v.clamp(MAX_POLL_INTERVAL_FLOOR_MS, MAX_POLL_INTERVAL_CEIL_MS)
        })
        .to_string();
    // `topic.metadata.refresh.interval.ms` CLAMP (Codex R37 F2 / round-3):
    // forwarded (above), then re-set LAST so it wins. `-1` DISABLES partition
    // discovery, so `-1` / `0` / negative / unparseable / a value SLOWER than
    // the default all collapse to the safe ceiling (`METADATA_REFRESH_CEIL_MS`);
    // a POSITIVE, FASTER caller value is PRESERVED (short-retention topics that
    // add partitions need prompt discovery) but not below the floor (so a spec
    // cannot hammer the broker). An exact pin would override a legitimate faster
    // setting and re-open the discovery-lag loss the finding closes. Parsed as
    // `i64` so the sentinel `-1` is recognised (not silently treated as huge).
    let metadata_refresh_ms = config
        .additional_properties
        .get("topic.metadata.refresh.interval.ms")
        .and_then(|s| s.trim().parse::<i64>().ok())
        .filter(|&v| v > 0)
        .map_or(METADATA_REFRESH_CEIL_MS, |v| {
            u64::try_from(v)
                .unwrap_or(METADATA_REFRESH_CEIL_MS)
                .clamp(METADATA_REFRESH_FLOOR_MS, METADATA_REFRESH_CEIL_MS)
        })
        .to_string();
    // …then the runtime-managed, correctness-critical keys LAST so they win
    // over anything a spec tried to set.
    client
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false")
        .set(
            "auto.offset.reset",
            if config.use_earliest_offset {
                "earliest"
            } else {
                "latest"
            },
        )
        // Rebalance protocol (Codex R29): pin the COOPERATIVE (incremental)
        // assignor. librdkafka's default `range,roundrobin` (rdkafka-sys
        // 4.10.0+2.12.1 rdkafka_conf.c `.sdef`) is EAGER: any group
        // rebalance — e.g. the operator ADDS PARTITIONS to the topic —
        // first revokes ALL owned partitions, discarding librdkafka's
        // in-memory fetch positions, then re-assigns them, restarting every
        // partition from its committed offset / `auto.offset.reset` and
        // re-delivering rows consumed since the last commit as IN-PROCESS
        // duplicates (no restart involved). Under `cooperative-sticky` a
        // rebalance never revokes unaffected partitions (librdkafka
        // rdkafka.h: the cooperative rebalance callback "will never be
        // called if the set of partitions being revoked is empty"), so
        // existing positions survive and only genuinely NEW partitions start
        // from `auto.offset.reset`. rdkafka 0.37's default event handling
        // drives the matching incremental assign/unassign automatically
        // (`ConsumerContext::rebalance` branches on `rebalance_protocol()`);
        // the `GateConsumerContext` installed by `create_stream_consumer`
        // (Codex R4 H2) only OBSERVES those callbacks — it records the
        // affected partitions so the poll loop re-seeks them before
        // consuming, and leaves the assign/unassign handling to the default.
        // `build_consumer_config` manages `group.protocol` alongside this
        // pin: librdkafka rejects a set strategy under
        // `group.protocol=consumer` (KIP-848), so the consumer always runs
        // the classic protocol.
        .set("partition.assignment.strategy", "cooperative-sticky")
        // Cluster-identity stability (Codex R30 F1): librdkafka 2.12.1
        // defaults `metadata.recovery.strategy` to `rebootstrap` (rdkafka-sys
        // 4.10.0+2.12.1 rdkafka_conf.c:454 / CONFIGURATION.md — "If set to
        // `none`, the client doesn't re-bootstrap"): when every known broker
        // becomes unavailable — or a broker requests it in a metadata
        // response (KIP-1102) — the client silently repeats the bootstrap
        // process from `bootstrap.servers`. If that DNS name was repointed
        // to a DIFFERENT cluster in the meantime, the live consumer migrates
        // clusters MID-SESSION, while the overlord sink keeps stamping the
        // cluster id resolved at consumer START onto rows that now come from
        // the other cluster. Mislabeled provenance corrupts the earliest
        // re-create cleanup in both directions: connected to the new cluster
        // it KEEPS rows stamped with the old id (duplication), and a later
        // re-create against the OLD cluster DROPS the mislabeled rows its
        // replay can never rebuild (permanent loss). Pinning `none` removes
        // the class by construction: one consumer session can never abandon
        // the cluster whose identity was resolved and stamped at start, so
        // start-time stamping and cleanup matching stay one identity. The
        // trade (documented): if the whole cluster becomes permanently
        // unreachable, this consumer stalls (transient recv warnings)
        // instead of re-bootstrapping — a restart/re-create re-resolves the
        // identity and recovers, exactly the pre-librdkafka-2.11 behaviour.
        .set("metadata.recovery.strategy", "none")
        // Payload INTEGRITY (Codex R37 F1): pin `check.crcs=true`. librdkafka
        // DEFAULTS it to `false` (rdkafka-sys 4.10.0+2.12.1 CONFIGURATION.md:
        // `check.crcs | C | true,false | false`), so a record whose bytes were
        // flipped on the wire or on disk into DIFFERENT-but-still-valid JSON is
        // published with a WRONG value, silently. With CRC verification ON, a
        // corrupted record is rejected by librdkafka as `RD_KAFKA_RESP_ERR__BAD_MSG`
        // (`RDKafkaErrorCode::BadMessage`), which `consume_error_is_fatal` now
        // classifies replay-required (fail-loud) — so a detected corruption
        // stops the consumer instead of being silently skipped or mis-valued.
        // The documented cost is "slightly increased CPU usage".
        .set("check.crcs", "true")
        // Protocol-downgrade / isolation bypass (Codex R38): pin the ApiVersion
        // negotiation ON and floor the fallback to a `read_committed`-capable
        // broker version. `isolation.level=read_committed` (R27, the pinned
        // librdkafka default) is only carried by Fetch **v4** (KIP-98, Kafka
        // 0.11.0.0); a pre-v4 Fetch has no isolation flag and a
        // down-conversion-enabled 2.x/3.x broker serves it as READ_UNCOMMITTED,
        // silently delivering ABORTED transactional records. librdkafka chooses
        // the Fetch version from ApiVersion NEGOTIATION, or — when
        // `api.version.request=false`, or the request fails/times out — from
        // `broker.version.fallback` (rdkafka-sys 4.10.0+2.12.1 CONFIGURATION.md:
        // `api.version.request | true,false | true`; `broker.version.fallback |
        // | 0.10.0`, valid downgrade values `0.9.0`/`0.8.x`, "Any other value
        // >= 0.10 … enables ApiVersionRequests"). A spec setting
        // `api.version.request=false` + `broker.version.fallback=0.9.0` would
        // force a pre-v4 Fetch and bypass the pinned read_committed. Pin
        // `api.version.request=true` so the broker's REAL feature set is always
        // negotiated (a modern broker advertises Fetch v4+), and pin
        // `broker.version.fallback=0.11.0.0` — the FIRST transactional-Fetch
        // release — so even the residual "negotiation genuinely fails / a small
        // `api.version.request.timeout.ms` times it out" path assumes a
        // read_committed-capable broker rather than downgrading. Both are also
        // dropped from `additional_properties` upstream (`MANAGED_CONSUMER_KEYS`
        // in runtime.rs, alongside the deprecated `api.version.fallback.ms`);
        // re-setting them LAST here is defence-in-depth. Harmless against
        // supported (>= 0.11) brokers: when negotiation succeeds the fallback is
        // ignored entirely, and pre-0.11 brokers have no transactions to read.
        .set("api.version.request", "true")
        .set("broker.version.fallback", "0.11.0.0")
        // Partition DISCOVERY / completeness (Codex R37 F2 / round-3): set the
        // CLAMPED `topic.metadata.refresh.interval.ms` computed above. The
        // periodic metadata timer discovers partitions ADDED after start (range
        // -1..3600000, default 300000, CONFIGURATION.md); `-1` DISABLES it, so a
        // new partition is never assigned and its records are lost at retention.
        // The clamp collapses `-1`/`0`/invalid/slower-than-default to the safe
        // ceiling while PRESERVING a legitimately FASTER caller value (a
        // short-retention topic that adds partitions needs prompt discovery) —
        // an exact pin would override that and re-open the loss.
        // `metadata.max.age.ms` (cache age) stays caller-tunable: discovery is
        // driven by THIS periodic refresh, not the cache age.
        .set("topic.metadata.refresh.interval.ms", &metadata_refresh_ms)
        // Topic completeness: pin `allow.auto.create.topics=false` (also the
        // librdkafka consumer default). `true` makes a subscribe to a MISSING
        // topic AUTO-CREATE an empty one on a permissive broker, so a topic
        // typo silently ingests from an empty log instead of failing loudly —
        // and after an earliest cleanup the empty replay rebuilds nothing.
        .set("allow.auto.create.topics", "false")
        // Max-poll FLOOR (Codex R36): set the effective `max.poll.interval.ms`
        // computed above = `max(caller, 300000 ms)`. librdkafka checks the
        // interval TWICE A SECOND (rdkafka-sys 4.10.0+2.12.1 CONFIGURATION.md:
        // range 1..86400000, default 300000, "checked two times per second").
        // The finding (pre-durability shape): a spec-set pathological value
        // (`max.poll.interval.ms=1`) below that cadence makes ANY publish
        // that out-blocks ~500 ms — e.g. a long Historical query holding a
        // read lock while this consumer is mid-publish — exceed the interval
        // and revoke every partition; the rejoin then restarted from
        // committed offsets / `auto.offset.reset` and RE-DELIVERED
        // already-published rows in-process (an in-session duplicate). A FLOOR
        // (not an exact pin) removes the caller's ability to go BELOW the safe
        // default while PRESERVING a legitimately larger caller value — a spec
        // may need `max.poll.interval.ms >= session.timeout.ms` (librdkafka
        // rejects the client otherwise), so an exact pin would break valid
        // large-session configs; the floor does not.
        //
        // Since Codex R4 H2 the rejoin duplication itself is also closed at
        // the enforcement layer: the rebalance observer (`GateConsumerContext`
        // → `rearm_gate_after_rebalance`) re-arms the resume-seek gate for
        // every revoked/reassigned partition, so post-rejoin records are
        // suppressed until a fresh authoritative seek restores the exact
        // pre-revoke position (or re-derives the durable frontier target) —
        // a genuine max-poll overrun now costs a seek round-trip, not a
        // replay. The floor is kept as defence-in-depth: constant revocation
        // churn would still stall ingestion behind endless re-seeks. An
        // offset-only in-session HIGH-WATER dedup remains REJECTED: it cannot
        // distinguish an earliest-reset REPLAY of the same log (offsets to
        // skip) from an offset-REUSE — a topic delete/recreate, or an
        // unclean-leader-election truncation — that rewrites the same offsets
        // with NEW data (offsets that must NOT be skipped), because consumer
        // metadata carries no KIP-516 topic UUID or leader epoch under
        // `#![forbid(unsafe_code)]` (same identity limit as Codex R31); the
        // H2 restore point is immune to that trap because it only ever seeks
        // BACK to a position this session itself reached.
        .set("max.poll.interval.ms", &max_poll_ms)
        // Bound librdkafka's own prefetch queue so a busy/adversarial broker
        // cannot make it hold GiBs of records before they reach our per-record
        // (1 MiB) and buffer caps (Codex R4/R7). librdkafka's default
        // `queued.max.messages.kbytes` is ~1 GiB; pin the queue + per-fetch +
        // per-partition-fetch limits conservatively so aliases can't raise
        // the effective size.
        .set("queued.max.messages.kbytes", "65536") // 64 MiB queue
        .set("fetch.max.bytes", "16777216") // 16 MiB per fetch
        .set("max.partition.fetch.bytes", "16777216"); // 16 MiB per partition
    client
}

/// Fetch the Kafka **cluster id** (the broker-side `cluster.id`, a UUID
/// minted once when the cluster is first formatted) from an
/// already-created consumer, blocking up to `timeout`.
///
/// This is the first-class cluster IDENTITY for provenance stamping:
/// `bootstrap.servers` strings are only an unordered seed LIST — two
/// different strings can name one cluster, and (after a DNS repoint) one
/// string can name two different clusters over time — so equality on them
/// is neither necessary nor sufficient (Codex R24). `None` means the id
/// could not be obtained within `timeout` (broker unreachable, or a
/// pre-KIP-78 broker that reports no id) and callers must treat the
/// identity as UNKNOWN, never as a match.
///
/// Wraps rdkafka's [`Client::fetch_cluster_id`] (librdkafka
/// `rd_kafka_clusterid`), reached through the consumer's
/// [`Consumer::client`]. **This call BLOCKS the current thread** up to
/// `timeout` while librdkafka waits for broker metadata — run it on a
/// blocking-capable thread (e.g. `tokio::task::spawn_blocking`), not
/// directly on an async worker.
///
/// Requires the `kafka-io` feature.
///
/// [`Client::fetch_cluster_id`]: rdkafka::client::Client::fetch_cluster_id
/// [`Consumer::client`]: rdkafka::consumer::Consumer::client
#[cfg(feature = "kafka-io")]
#[must_use]
pub fn fetch_cluster_id(
    consumer: &KafkaStreamConsumer,
    timeout: std::time::Duration,
) -> Option<String> {
    use rdkafka::consumer::Consumer as _;
    consumer.client().fetch_cluster_id(timeout)
}

/// How often the poll loop runs the cluster-identity drift TRIPWIRE
/// ([`cluster_identity_drifted`], Codex R37 F3). NOT per-publish: each check
/// re-fetches the live cluster id, and rdkafka 0.37's `fetch_cluster_id`
/// LEAKS the `rd_kafka_clusterid`-allocated string (it never calls
/// `rd_kafka_mem_free`; the same upstream bug the one-shot start-time
/// `resolve_cluster_id` already hits), so a per-publish check would leak
/// per segment. A fixed low cadence bounds that leak to one small (~UUID-sized)
/// allocation per interval — negligible (< ~1 MiB/month/supervisor) — while
/// still catching a broker-reconnect drift within one interval. Short in tests.
#[cfg(not(test))]
#[cfg(feature = "kafka-io")]
const CLUSTER_DRIFT_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
#[cfg(test)]
#[cfg(feature = "kafka-io")]
const CLUSTER_DRIFT_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// How long a single drift check may block on broker metadata. Bounded; a
/// timeout just yields `None` (fail-safe — no drift, retried next interval).
#[cfg(feature = "kafka-io")]
const CLUSTER_DRIFT_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Re-fetch the consumer's live Kafka cluster id and report whether it has
/// DRIFTED away from the `expected` (start-time) identity (Codex R37 F3).
///
/// The R30 F1 `metadata.recovery.strategy=none` pin blocks a re-BOOTSTRAP
/// cluster migration, but a broker RECONNECT to a repointed hostname (now
/// serving a DIFFERENT cluster) is a separate path — librdkafka reconnects
/// with only a warning and swaps its cached cluster id, while the sink keeps
/// stamping the start-time id. Comparing the live id to the stamped one
/// catches that.
///
/// FORCES a fresh metadata fetch (`fetch_metadata`) FIRST, so the subsequent
/// `fetch_cluster_id` reflects the broker we are CURRENTLY connected to rather
/// than a possibly-stale cached value — `rd_kafka_clusterid` alone returns the
/// cached id and would otherwise lag until librdkafka's own periodic refresh
/// (the finding's staleness window). A failed metadata fetch is ignored: the
/// cluster-id read then falls back to whatever is cached and, if unresolvable,
/// yields `None`.
///
/// Fail-safe (see [`cluster_id_is_drift`]): a live id that cannot be resolved
/// (`None`) is NOT drift, so a transient metadata gap can only defer the check
/// to the next interval, never fabricate a stop. **Blocks up to
/// ~2× [`CLUSTER_DRIFT_CHECK_TIMEOUT`]**, which is why it runs on the low-cadence
/// [`CLUSTER_DRIFT_CHECK_INTERVAL`] tripwire, not per publish.
///
/// Leaks one small `fetch_cluster_id` allocation per call (upstream rdkafka
/// 0.37 bug — see [`CLUSTER_DRIFT_CHECK_INTERVAL`]); the cadence bounds it.
#[cfg(feature = "kafka-io")]
#[must_use]
fn cluster_identity_drifted(consumer: &KafkaStreamConsumer, topic: &str, expected: &str) -> bool {
    use rdkafka::consumer::Consumer as _;
    // Force the cached cluster id to reflect the CURRENT broker before reading
    // it (closes the staleness window); the result itself is unused.
    let _ = consumer
        .client()
        .fetch_metadata(Some(topic), CLUSTER_DRIFT_CHECK_TIMEOUT);
    publish_blocked_by_identity_drift(
        fetch_cluster_id(consumer, CLUSTER_DRIFT_CHECK_TIMEOUT).as_deref(),
        Some(expected),
    )
}

/// Async wrapper around [`cluster_identity_drifted`] shared by the periodic
/// R37 F3 tripwire AND the R6 H4 pre-publish identity check: runs the
/// BLOCKING librdkafka metadata + cluster-id fetch on `spawn_blocking` so it
/// never stalls the async worker. `expected == None` (no identity resolved
/// at start ⇒ nothing is stamped) and a failed blocking-task join both
/// degrade to `false` — fail-safe, never a spurious stop.
#[cfg(feature = "kafka-io")]
async fn live_identity_drifted(
    consumer: &std::sync::Arc<KafkaStreamConsumer>,
    topic: &str,
    expected: Option<&str>,
) -> bool {
    let Some(expected) = expected else {
        return false;
    };
    let c = std::sync::Arc::clone(consumer);
    let topic_owned = topic.to_owned();
    let expected_owned = expected.to_owned();
    tokio::task::spawn_blocking(move || {
        cluster_identity_drifted(c.as_ref(), &topic_owned, &expected_owned)
    })
    .await
    .unwrap_or(false)
}

/// One fail-loud record of a detected cluster-identity drift (Codex R37 F3
/// tripwire / R6 H4 pre-publish check), shared by every stop site so the
/// operator-facing explanation stays in one place. `trigger` names which
/// check caught it.
#[cfg(feature = "kafka-io")]
fn log_identity_drift_stop(topic: &str, expected: Option<&str>, trigger: &str) {
    tracing::error!(
        topic = %topic,
        expected_cluster_id = expected.unwrap_or(""),
        trigger,
        "FATAL: the Kafka cluster identity DRIFTED away from the id resolved \
         at start (a known broker hostname was repointed to a DIFFERENT \
         cluster and librdkafka reconnected to it). Discarding the in-flight \
         buffer (its rows may belong to the other cluster and would be \
         durably mis-stamped with the old cluster's provenance — R6 H4) and \
         stopping the consumer. The stop reports replay-required so the \
         supervisor is not recorded as cleanly stopped; a restart re-resolves \
         the identity and rebuilds under the correct cluster (R37 F3 / R6 H4)",
    );
}

/// Whether a `recv()` error means this consumer will NEVER make progress
/// without operator action, so the poll loop must stop fail-LOUD instead
/// of warn-looping forever (Codex R30 F2, extended R35).
///
/// Fatal classes (rdkafka 0.37 `error.rs` / rdkafka-sys 4.10.0+2.12.1
/// `types.rs`):
///   * [`KafkaError::MessageConsumptionFatal`] — librdkafka has marked the
///     CLIENT INSTANCE fatally failed (`rd_kafka_fatal_error`); no retry on
///     this consumer can ever succeed. Some of the group/config rejections
///     below ALSO surface here when librdkafka flags them fatal — this arm
///     catches them regardless of code.
///   * [`KafkaError::MessageConsumption`] with an AUTHORIZATION code
///     (`TopicAuthorizationFailed` 29 / `GroupAuthorizationFailed` 30 /
///     `ClusterAuthorizationFailed` 31): librdkafka retries these forever,
///     so the supervisor looks alive while consuming NOTHING — after an
///     earliest re-create's cleanup that silence means the dropped rows
///     are never rebuilt. ACL restoration alone cannot be waited on
///     in-process (the operator must resume/re-create, which also
///     re-verifies the topic), so fail-loud is strictly better.
///   * [`KafkaError::MessageConsumption`] with a PERMANENT GROUP-JOIN /
///     CONFIG / PROTOCOL rejection (Codex R35) — a JoinGroup/SyncGroup the
///     broker refuses for a reason a plain retry can never heal, only an
///     operator changing the rejected setting can: `InvalidSessionTimeout`
///     (26 — `session.timeout.ms` outside the broker's
///     `group.min/max.session.timeout.ms`, the exact R35 scenario: locally
///     valid, broker-rejected), `InvalidGroupId` (24 — the `group.id`
///     violates the broker's rules), `InconsistentGroupProtocol` (23 — the
///     offered `partition.assignment.strategy` / protocol metadata is
///     rejected; this consumer joins a UNIQUE single-member group per spawn,
///     so it is a config rejection, never a transient mixed-member
///     negotiation), and `UnsupportedVersion` (35 — the broker does not
///     support an API version the client requires; a retry re-sends the same
///     request).
///
///   * [`KafkaError::MessageConsumption`] with a PAYLOAD INTEGRITY / DECODE
///     code (Codex R37 F1) — `BadMessage` (-199, `RD_KAFKA_RESP_ERR__BAD_MSG`:
///     a record that failed the pinned `check.crcs=true` verification) or
///     `BadCompression` (-198: a batch that could not be decompressed). Left
///     transient, librdkafka's default handling would SKIP the corrupt record
///     and advance, so a bit-flip is dropped silently (after an earliest
///     cleanup that is permanent loss) — and without the CRC pin a flip into
///     valid-but-wrong JSON is published as a WRONG value. Fail-loud: the stop
///     is replay-required, and because Kafka is the durable log a restart
///     re-fetches a CLEAN copy (wire corruption self-heals) or surfaces
///     genuine on-disk corruption to the operator instead of ingesting it.
///
///   * [`KafkaError::MessageConsumption`] with a PERMANENT INVALID-TOPIC-NAME
///     rejection (Codex R39) — `InvalidTopic` (17, `INVALID_TOPIC_EXCEPTION`):
///     the broker refuses a syntactically invalid or broker-policy-rejected
///     topic NAME (e.g. a trailing space that slipped past a lenient caller).
///     `validate()` now rejects a bad literal name up front, so this arm is
///     defence-in-depth for a name that grammar accepts but a stricter broker
///     policy refuses. A retry re-sends the same name and is rejected
///     identically, so librdkafka would retry forever while the acknowledged
///     supervisor consumes NOTHING until the records expire at retention —
///     after an earliest re-create that is permanent loss. Only an operator
///     re-create with a corrected topic can heal it, so fail-loud.
///
/// The permanent group/config and integrity rejections make the stop
/// replay-required, so a
/// shutdown/suspend is refused for as long as the setting stays rejected (the
/// Codex R30 F3 sentinel can STUCK-refuse here). That is the correct
/// fail-LOUD — strictly better than silently losing the rows an earliest
/// cleanup may already have dropped — and it clears the moment the operator
/// fixes the rejected setting and re-creates from earliest: the pre-cleanup
/// group-join probe ([`probe_group_joinable`]) then either passes (setting
/// fixed) or refuses BEFORE any destructive drop.
///
/// Everything else is TRANSIENT and stays a warn-and-retry — including the
/// group-coordinator conditions librdkafka heals by itself: `AllBrokersDown`
/// (-187) and transport errors recover on reconnection; `UnknownTopicOrPartition`
/// (3) and the local `UnknownTopic` (-188) name a topic that does not YET
/// exist — the topic-creation / metadata-propagation race, where a brief
/// warn-and-retry is exactly right (kept transient, NOT swept in with the R39
/// `InvalidTopic` fix; a genuinely-nonexistent valid name is caught fail-loud
/// later by the pre-cleanup `probe_topic_metadata`, not here). The
/// admin/CreateTopics-only codes `InvalidPartitions` (37) /
/// `InvalidReplicationFactor` (38) / `InvalidReplicaAssignment` (39) /
/// `TopicAlreadyExists` (36) never reach a subscribe/consume path, so they are
/// deliberately NOT added either. `OperationTimedOut` / poll timeouts are
/// routine; `CoordinatorNotAvailable`
/// (15) / `NotCoordinator` (16) / `CoordinatorLoadInProgress` (14) resolve
/// once the coordinator settles; `RebalanceInProgress` (27) /
/// `IllegalGeneration` (22) / `UnknownMemberId` (25) / `MemberIdRequired`
/// (79) are the normal rejoin handshake. `FencedInstanceId` (82) /
/// `GroupMaxSizeReached` (81) are unreachable here (no static
/// `group.instance.id`; a unique single-member group per spawn) so they are
/// deliberately NOT added.
///
/// [`KafkaError::MessageConsumptionFatal`]: rdkafka::error::KafkaError::MessageConsumptionFatal
/// [`KafkaError::MessageConsumption`]: rdkafka::error::KafkaError::MessageConsumption
#[cfg(feature = "kafka-io")]
#[must_use]
pub fn consume_error_is_fatal(err: &rdkafka::error::KafkaError) -> bool {
    use rdkafka::error::KafkaError;
    use rdkafka::types::RDKafkaErrorCode;
    match err {
        KafkaError::MessageConsumptionFatal(_) => true,
        KafkaError::MessageConsumption(code) => matches!(
            code,
            // Authorization (Codex R30 F2): librdkafka retries these forever.
            RDKafkaErrorCode::TopicAuthorizationFailed
                | RDKafkaErrorCode::GroupAuthorizationFailed
                | RDKafkaErrorCode::ClusterAuthorizationFailed
                // Permanent group-join / config / protocol rejections
                // (Codex R35): retry cannot heal them, only fixing the
                // rejected consumer setting can.
                | RDKafkaErrorCode::InvalidSessionTimeout
                | RDKafkaErrorCode::InvalidGroupId
                | RDKafkaErrorCode::InconsistentGroupProtocol
                | RDKafkaErrorCode::UnsupportedVersion
                // Permanent INVALID-TOPIC-NAME rejection (Codex R39): the
                // broker answers the subscription's metadata request with
                // `INVALID_TOPIC_EXCEPTION` (protocol error 17) for a
                // syntactically invalid or broker-policy-rejected topic name.
                // A retry re-sends the SAME name and is rejected identically,
                // so librdkafka would otherwise retry forever while the
                // supervisor looks alive and consumes NOTHING — after an
                // earliest re-create's cleanup that silence is permanent loss
                // of the dropped rows (the same idle hazard as the R30 F2
                // authorization codes). `validate()` now rejects a bad literal
                // name up front; this is defence-in-depth for a name it accepts
                // but a stricter broker policy refuses. Only fixing the topic
                // (an operator re-create) can heal it, so fail-loud.
                | RDKafkaErrorCode::InvalidTopic
                // Payload INTEGRITY / decode (Codex R37 F1): with the pinned
                // `check.crcs=true`, a record that fails CRC verification (or a
                // batch that cannot be decompressed) surfaces here as
                // `BadMessage` / `BadCompression`. librdkafka would otherwise
                // let the poll loop treat these as transient and SKIP the
                // corrupt record, silently advancing past it — after an
                // earliest cleanup that dropped the pair's prior rows, a
                // skipped record is permanent loss, and without CRCs a
                // bit-flip is published as a WRONG value. Fail-LOUD instead:
                // the stop is replay-required, and because Kafka is the durable
                // log a restart re-fetches a CLEAN copy (on-the-wire
                // corruption self-heals) or surfaces the on-disk corruption for
                // the operator rather than ingesting garbage.
                | RDKafkaErrorCode::BadMessage
                | RDKafkaErrorCode::BadCompression
        ),
        _ => false,
    }
}

/// Whether a topic currently holds records an earliest replay could rebuild
/// from, as observed by the pre-cleanup readiness probe
/// ([`probe_topic_metadata`]).
///
/// Motivation (Codex R31): librdkafka's consumer metadata exposes only the
/// topic NAME, never its KIP-516 topic UUID (vendor: rdkafka 0.37
/// `MetadataTopic` = name/partitions/error only; the underlying
/// `rd_kafka_metadata_topic` C struct carries no id; the UUID exists solely
/// behind the Admin `DescribeTopics` FFI, unreachable under
/// `#![forbid(unsafe_code)]`). So a topic deleted and recreated under the
/// SAME name on the SAME cluster with the SAME ingestion schema is
/// indistinguishable from the original log, and the overlord's
/// earliest-replay cleanup would drop the pair's prior segments believing an
/// earliest replay rebuilds them — but a recreated/empty topic replays
/// NOTHING, so those rows would be permanently lost. When the probe can
/// PROVE the topic currently holds no records, the cleanup keeps the prior
/// segments instead (duplication over permanent loss). This is a proximity
/// guard, not a cure: a recreated topic that has since been RE-PRODUCED
/// reads as `HasRecords` and the residual remains — closed for good only by
/// durable deep-storage segments (FG-6/FG-7), after which the cleanup no
/// longer depends on a Kafka replay at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicRecords {
    /// At least one partition reported `high > low`: the topic holds records
    /// an earliest replay can rebuild from (the normal restart/resume case).
    HasRecords,
    /// EVERY partition reported `high == low`: the topic is currently empty,
    /// so an earliest replay would rebuild nothing (deleted+recreated topic,
    /// or retention expired past all the data).
    Empty,
    /// At least one partition's watermarks could not be fetched and none
    /// showed records: emptiness is UNPROVEN. Callers must fall back to their
    /// default behavior — never suppress a destructive drop on a guess.
    Unknown,
}

/// Reduce per-partition `(low, high)` watermark observations to a
/// [`TopicRecords`] verdict. Each element is `Some((low, high))` for a
/// successful fetch or `None` for a failed one.
///
/// * ANY partition with `high > low` ⇒ [`TopicRecords::HasRecords`]
///   (short-circuits — a single sighting proves the topic is non-empty);
/// * ALL partitions fetched AND every one empty (`high == low`) ⇒
///   [`TopicRecords::Empty`];
/// * otherwise (a failed fetch, or an empty partition slice) ⇒
///   [`TopicRecords::Unknown`] — emptiness is never claimed on partial data,
///   so a transient watermark hiccup can only cost a duplication-avoiding
///   KEEP being SKIPPED, never a wrong drop.
#[must_use]
pub fn classify_watermarks(per_partition: &[Option<(i64, i64)>]) -> TopicRecords {
    let mut any_unknown = false;
    for wm in per_partition {
        match wm {
            Some((low, high)) if high > low => return TopicRecords::HasRecords,
            Some(_) => {}
            None => any_unknown = true,
        }
    }
    if any_unknown || per_partition.is_empty() {
        TopicRecords::Unknown
    } else {
        TopicRecords::Empty
    }
}

/// Prove that `topic` is READABLE-in-principle through an already-created
/// consumer, blocking up to `timeout`: the broker must answer a metadata
/// request for the topic, the topic must exist WITHOUT a topic-level error
/// (`TopicAuthorizationFailed` / `UnknownTopicOrPartition` arrive here),
/// and it must have at least one partition.
///
/// Used by the overlord BEFORE the destructive earliest-replay cleanup
/// (Codex R30 F2): a principal that kept its cluster-metadata ACL but lost
/// topic READ would otherwise pass consumer creation + cluster-id
/// resolution, let the cleanup drop every prior segment, and then consume
/// nothing — the dropped rows would never be rebuilt. Fail-close: any
/// probe failure must abort the caller BEFORE anything is dropped.
///
/// Honest limitation: a metadata fetch requires (and therefore proves)
/// topic DESCRIBE, not READ. A principal holding DESCRIBE but not READ
/// passes this probe; that residual is caught fail-loud at consume time
/// instead ([`consume_error_is_fatal`] stops the consumer, and the failed
/// stop refuses the tombstone). Vendor: rdkafka 0.37
/// `Consumer::fetch_metadata` (consumer/mod.rs:377 → client.rs:438,
/// `rd_kafka_metadata`), topic-level error via `MetadataTopic::error()`.
///
/// On success it ALSO reports whether the topic currently holds any
/// replayable records ([`TopicRecords`], Codex R31): after the readiness
/// checks it fetches each partition's `(low, high)` watermark
/// (`Consumer::fetch_watermarks`, consumer/mod.rs:383) and reduces them via
/// [`classify_watermarks`]. This lets the overlord's earliest-replay cleanup
/// decline to drop a pair's prior segments when the source topic is proven
/// empty — the deleted+recreated / retention-expired topic that a replay
/// could never rebuild (there is no topic UUID in consumer metadata to
/// detect the recreation directly — see [`TopicRecords`]). A watermark fetch
/// failure yields [`TopicRecords::Unknown`], never a false `Empty`.
///
/// **BLOCKS the current thread** up to `timeout` — run it on a
/// blocking-capable thread, like [`fetch_cluster_id`]. The watermark fetches
/// run only AFTER a successful (hence responsive-broker) metadata fetch and
/// only when the caller is about to drop (i.e. rarely), so the worst-case
/// added wall-clock is bounded by `partitions × timeout` on the blocking
/// pool.
///
/// # Errors
///
/// [`ConsumerError::KafkaError`] describing exactly which readiness check
/// failed (metadata fetch error, topic absent, topic-level error code, or
/// zero partitions). A watermark-fetch failure is NOT an error — it degrades
/// the records verdict to [`TopicRecords::Unknown`].
#[cfg(feature = "kafka-io")]
pub fn probe_topic_metadata(
    consumer: &KafkaStreamConsumer,
    topic: &str,
    timeout: std::time::Duration,
) -> Result<TopicRecords, ConsumerError> {
    use rdkafka::consumer::Consumer as _;
    use rdkafka::types::RDKafkaErrorCode;

    let metadata = consumer
        .client()
        .fetch_metadata(Some(topic), timeout)
        .map_err(|e| {
            ConsumerError::KafkaError(format!("topic-readiness probe: metadata fetch failed: {e}"))
        })?;
    let entry = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| {
            ConsumerError::KafkaError(format!(
                "topic-readiness probe: broker metadata does not mention topic '{topic}'"
            ))
        })?;
    if let Some(err) = entry.error() {
        return Err(ConsumerError::KafkaError(format!(
            "topic-readiness probe: topic '{topic}' reported {:?}",
            RDKafkaErrorCode::from(err)
        )));
    }
    if entry.partitions().is_empty() {
        return Err(ConsumerError::KafkaError(format!(
            "topic-readiness probe: topic '{topic}' has no partitions"
        )));
    }
    // Records probe (Codex R31): a partition with `high > low` still holds
    // records an earliest replay can rebuild from. A failed watermark fetch
    // is left as `None` so `classify_watermarks` degrades to `Unknown`
    // rather than ever claiming a false `Empty`.
    //
    // Short-circuit on the FIRST record sighting (Codex R31 review, Medium):
    // one non-empty partition already proves `HasRecords`, so the common
    // restart case (a full topic) costs a single watermark fetch instead of
    // one per partition — bounding this uncancellable, globally-serialized
    // lifecycle step. Proving `Empty` still requires every partition, so the
    // per-partition results are accumulated and reduced by
    // `classify_watermarks` (which the unit test pins).
    let mut per_partition: Vec<Option<(i64, i64)>> = Vec::with_capacity(entry.partitions().len());
    for p in entry.partitions() {
        let wm = consumer.fetch_watermarks(topic, p.id(), timeout).ok();
        if let Some((low, high)) = wm
            && high > low
        {
            return Ok(TopicRecords::HasRecords);
        }
        per_partition.push(wm);
    }
    Ok(classify_watermarks(&per_partition))
}

/// Prove — BEFORE the overlord's destructive earliest-replay cleanup — that
/// this ALREADY-subscribed consumer can actually be ADMITTED to its consumer
/// group, by polling it briefly and surfacing any PERMANENT JoinGroup/config
/// rejection (Codex R35).
///
/// The R30 F2 metadata probe ([`probe_topic_metadata`]) proves the topic is
/// DESCRIBE-able, but a `session.timeout.ms` outside the broker's
/// `group.min/max.session.timeout.ms` (or another permanent group/config
/// rejection — see [`consume_error_is_fatal`]) is locally valid and passes
/// consumer creation AND the metadata probe, yet the broker refuses every
/// JoinGroup. Without this probe the overlord would delete the pair's prior
/// segments, then the streaming loop would warn-retry the un-healable
/// JoinGroup forever, rebuilding NOTHING — permanent loss of the dropped
/// rows. Polling here forces the join to be attempted while the prior
/// segments are still intact, so a permanent rejection aborts the cleanup
/// fail-CLOSE (`Err`) instead.
///
/// Because a single poll can return an inconclusive TRANSIENT event (the
/// coordinator is still being found, a reconnect is in flight) BEFORE a
/// permanent rejection is dequeued, this polls in a bounded LOOP (in short
/// steps) until one of these outcomes is reached — it never decides off a
/// single inconclusive poll (Codex R35 review):
///   * a PERMANENT group/config error (see [`consume_error_is_fatal`]) is
///     dequeued ⇒ `Err` — the caller REFUSES the cleanup and keeps the prior
///     segments. Such a rejection is validated when the JoinGroup is
///     RECEIVED, so it surfaces FAST — before the broker's
///     `group.initial.rebalance.delay.ms` — which is why the loop reliably
///     sees it;
///   * POSITIVE join evidence ⇒ `Ok(())`: a delivered RECORD (join succeeded
///     and produced data) or a NON-EMPTY partition
///     [`assignment`](rdkafka::consumer::Consumer::assignment)
///     (JoinGroup+SyncGroup completed, even on a quiet topic). A delivered
///     record must not be swallowed — it is SEEK-ed back to its own offset so
///     the streaming loop (which REUSES this consumer) re-reads it; this probe
///     only runs on the earliest-replay path, so re-reading from that offset
///     is exactly the intended replay. A seek failure is fail-CLOSE `Err`.
///   * the DEADLINE elapses WITHOUT a permanent error ⇒ `Ok(())` (PROCEED,
///     with a warning if the assignment was not yet confirmed). A permanent
///     rejection is delivered promptly (above), so a silent deadline is
///     DOMINATED by the benign case — a healthy join still inside the
///     broker-side `group.initial.rebalance.delay.ms` (unknowable to the
///     client, and RESET on every spawn because this consumer's `group.id` is
///     unique). Fail-closing here would permanently block earliest re-creates
///     with victims on any cluster whose initial-rebalance delay exceeds this
///     window — a worse, unrecoverable failure than the narrow residual it
///     would guard. That residual — a join that PERMANENTLY fails yet emits no
///     classifiable error within the window — is still made NON-SILENT once
///     the consumer runs: the poll loop's own [`consume_error_is_fatal`]
///     classification then flags it replay-required (no clean tombstone), and
///     durable deep-storage segments (FG-6/FG-7) are the real fix.
///
/// **Runs async `recv()` in a bounded loop** — call it from the overlord's
/// async lifecycle op (NOT the blocking `spawn_blocking` style of
/// [`probe_topic_metadata`]). A permanent JoinGroup rejection is queued as an
/// error event shortly after `subscribe`, so it normally surfaces on the
/// first poll; a healthy join asserts its assignment within a rebalance.
///
/// # Errors
///
/// [`ConsumerError::KafkaError`] when a permanent group/config rejection is
/// surfaced, or when a probe-consumed record could not be seeked back.
///
/// Requires the `kafka-io` feature.
#[cfg(feature = "kafka-io")]
pub async fn probe_group_joinable(
    consumer: &KafkaStreamConsumer,
    timeout: std::time::Duration,
) -> Result<(), ConsumerError> {
    use rdkafka::Message as _;
    use rdkafka::consumer::Consumer as _;

    // Non-empty partition assignment ⇒ the JoinGroup+SyncGroup completed. A
    // local, non-blocking query; `Err` (unknowable) is treated as "not yet
    // joined" so it can only DELAY a proceed, never fabricate one.
    let joined = |c: &KafkaStreamConsumer| c.assignment().map(|a| a.count() > 0).unwrap_or(false);

    // Poll in short steps so a completed assignment (which appears only AFTER
    // the broker's group.initial.rebalance.delay.ms) is noticed promptly
    // instead of only at the full deadline.
    let step = std::time::Duration::from_millis(500);
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining.min(step), consumer.recv()).await {
            // The join SUCCEEDED and delivered a record. Do NOT swallow it:
            // seek its partition back to that offset so the streaming loop
            // that reuses this consumer re-reads it (earliest-replay path).
            Ok(Ok(msg)) => {
                let topic = msg.topic().to_owned();
                let partition = msg.partition();
                let offset = msg.offset();
                drop(msg);
                consumer
                    .seek(
                        &topic,
                        partition,
                        rdkafka::Offset::Offset(offset),
                        remaining.min(step),
                    )
                    .map_err(|e| {
                        ConsumerError::KafkaError(format!(
                            "group-join probe consumed one record from {topic}[{partition}]@\
                             {offset} while proving the consumer can join its group, but could \
                             not seek it back for the streaming loop to re-read (refusing the \
                             cleanup rather than dropping the record): {e}"
                        ))
                    })?;
                return Ok(());
            }
            // PERMANENT group/config rejection — surfaces FAST (before the
            // initial-rebalance delay), so the loop catches it even after
            // transient noise. Fail-CLOSE.
            Ok(Err(e)) if consume_error_is_fatal(&e) => {
                return Err(ConsumerError::KafkaError(format!(
                    "group-join probe: the broker PERMANENTLY refused to admit the consumer to \
                     its group ({e}); a plain retry cannot heal this — fix the rejected consumer \
                     setting (e.g. session.timeout.ms within the broker's \
                     group.min/max.session.timeout.ms) and re-create the supervisor from earliest"
                )));
            }
            // TRANSIENT error (coordinator/rebalance/unreachable) OR a per-step
            // timeout: if the rebalance has ALREADY assigned partitions the
            // join is done — proceed with positive evidence; otherwise keep
            // polling until the deadline.
            Ok(Err(_)) | Err(_) => {
                if joined(consumer) {
                    return Ok(());
                }
            }
        }
    }
    // Deadline WITHOUT a permanent rejection: PROCEED. A permanent group/config
    // rejection would have surfaced as a fatal error by now (it is validated
    // when the JoinGroup is received, before the broker's initial-rebalance
    // delay), so a silent deadline is dominated by a healthy join still inside
    // that broker-side delay — fail-closing here would permanently block
    // earliest re-creates on clusters whose delay exceeds this window (the
    // group id is unique per spawn, so the delay never converges). The narrow
    // residual (a permanent failure emitting no classifiable error in-window)
    // is caught non-silently by the poll loop's own fatal classification once
    // the consumer runs; FG-6/FG-7 durable segments are the real fix.
    if !joined(consumer) {
        tracing::warn!(
            "group-join probe: proceeding without a confirmed partition assignment within the \
             probe window — a permanent group/config rejection would have surfaced as a fatal \
             error by now, so this is treated as a still-joining broker (e.g. \
             group.initial.rebalance.delay.ms), not a refusal",
        );
    }
    Ok(())
}

/// The per-partition RESUME FLOOR the rebalance restore (Codex R12 H1) and the
/// publish-failure re-seek (Codex R12 H2) target: the durable-contiguous
/// frontier ([`ContiguousFrontier`]) EXTENDED by the currently-buffered
/// (in-memory, still-RECOVERABLE) tail ([`StreamingBuffer::buffered_offset_spans`]).
///
/// For each in-session partition the floor is
/// `max(durable_next, buffered_span.next)`. This is the ONLY offset it is safe
/// to resume PAST:
///
/// * records below `durable_next` are durable (skipping them is dup-free);
/// * records in `[durable_next, buffered_span.next)` are still in the buffer
///   and are re-published from it on the next roll, so skipping them is
///   ALSO dup-free (the 2f fenced-rebalance invariant — buffered rows must not
///   be double-counted);
/// * a DROPPED (failed-publish) batch is absent from BOTH — a failed roll
///   drains the buffer's spans and never advances the durable frontier — so
///   the floor stays BELOW it and it is re-consumed, never skipped (Codex R12
///   H1: the raw consumed-forward point WOULD skip it, and once retention
///   advances those records are lost).
///
/// Pure (no Kafka I/O) so the floor derivation is unit-testable without a
/// broker.
#[must_use]
pub fn safe_resume_frontier(
    contiguous: &ContiguousFrontier,
    buffer: &StreamingBuffer,
) -> BTreeMap<i32, i64> {
    let mut floor = contiguous.durable_next_map().clone();
    for (&partition, span) in buffer.buffered_offset_spans() {
        floor
            .entry(partition)
            .and_modify(|f| *f = (*f).max(span.next))
            .or_insert(span.next);
    }
    floor
}

/// Re-arm the gate to RE-SEEK every currently-ACTIVE in-session partition back
/// to its durable-contiguous frontier after a publish FAILURE (Codex R12 H2),
/// returning the `(partition, target)` pairs it armed.
///
/// A failed roll/publish DRAINED the shared buffer and DROPPED its rows,
/// leaving a gap between each covered partition's durable frontier and the
/// records the loop had consumed forward of it. Left alone, the loop would
/// keep consuming and publishing FORWARD of that gap — but the
/// contiguous-commit frontier is pinned AT the gap, so none of those later
/// segments can ever be committed, and every one of them is RE-PUBLISHED from
/// the gap on the next restart (unbounded double count, worse the longer the
/// healthy run). Re-seeking each affected partition back to its durable
/// frontier makes the loop re-consume the dropped records and re-publish them
/// CONTIGUOUSLY instead — closing the gap rather than accumulating
/// uncommittable segments beyond it.
///
/// * Only ACTIVE partitions are re-armed: a still-gated partition consumed
///   nothing into the buffer, so it contributed no rows to the failed batch
///   and its own pending seek already governs.
/// * The target is the durable frontier (the buffer is EMPTY right after a
///   failed roll, so [`safe_resume_frontier`] reduces to `durable_next`).
/// * Each re-armed partition is TAINTED: the consumer's fetch position moved
///   PAST the durable frontier (it consumed the now-dropped rows), so a failed
///   backward re-seek must NOT be treated as a harmless no-op
///   ([`ResumeSeekGate::may_noop_failed_seek`]) — that would skip the
///   re-consume (loss).
///
/// Pure (no Kafka I/O — the actual seek is [`try_activate_pending`]) so the
/// selection + arming is unit-testable without a broker.
pub fn arm_publish_failure_reseek(
    gate: &mut ResumeSeekGate,
    contiguous: &ContiguousFrontier,
) -> Vec<(i32, i64)> {
    let targets: Vec<(i32, i64)> = contiguous
        .durable_next_map()
        .iter()
        .filter(|(partition, _)| matches!(gate.state(**partition), SeekGateState::Active))
        .map(|(&partition, &durable)| (partition, durable))
        .collect();
    for &(partition, durable) in &targets {
        gate.set_target(partition, durable);
        gate.taint(partition);
    }
    targets
}

/// Drain the [`RebalanceNotices`] and re-arm the gate for every affected
/// partition ([`rearm_gate_after_rebalance`], Codex R4 H2), logging what was
/// re-armed. A cheap no-op (one uncontended mutex lock) when no rebalance
/// happened — the steady state. On a real rebalance the in-session partitions'
/// restore floors are the durable-OR-buffered frontier ([`safe_resume_frontier`],
/// Codex R12 H1).
///
/// Under a DETECTED topic recreation (`frontier.topic_recreated`, Codex R18
/// C1+C2) a newly assigned partition with no durable frontier entry first
/// gets one synthesized ([`ResumeFrontier::synthesize_recreated`]), so the
/// re-arm gates it (`require_target`) and its seek is derived as the retained
/// log's `low` watermark — instead of the pre-R18 "untouched free partition"
/// path, which would consume it from the DEAD generation's committed offset
/// (silently skipping every new-generation record below it — loss).
#[cfg(feature = "kafka-io")]
fn drain_rebalance_and_rearm(
    notices: &RebalanceNotices,
    gate: &mut ResumeSeekGate,
    frontier: &mut ResumeFrontier,
    contiguous: &ContiguousFrontier,
    buffer: &StreamingBuffer,
    topic: &str,
) {
    let events = notices.take();
    if events.is_empty() {
        return;
    }
    // R18 C1/C2: low-floor every (re)assigned partition when the topic was
    // recreated — see the doc comment. A no-op unless `topic_recreated`.
    let synthesized = frontier.synthesize_recreated(events.assigned.iter().copied());
    if synthesized > 0 {
        tracing::warn!(
            topic,
            synthesized,
            "topic recreation detected (Codex R18 C1/C2): synthesized floor-low \
             resume entries for rebalance-assigned partitions with NO durable \
             frontier evidence — their committed offsets name the DEAD \
             generation's offset space and are never trusted; each partition is \
             gated until its low-watermark floor seek lands",
        );
    }
    // Only built on a REAL rebalance (rare) — clones the durable frontier map
    // and folds in the buffered tail (Codex R12 H1).
    let resume_floor = safe_resume_frontier(contiguous, buffer);
    let rearmed = rearm_gate_after_rebalance(gate, &frontier.partitions, &resume_floor, &events);
    if rearmed > 0 {
        tracing::warn!(
            topic,
            revoked = ?events.revoked,
            assigned = ?events.assigned,
            lost_all = events.lost_all,
            rearmed,
            "rebalance detected: re-armed the resume-seek gate for the affected \
             partitions — each is suppressed until a fresh authoritative seek \
             restores its position (in-session progress) or re-derives the durable \
             frontier target, so a stale committed offset can never re-publish \
             durable rows (H2)",
        );
    } else {
        // Nothing to re-arm: an initial assignment on a first start (no
        // durable frontier, nothing consumed yet) or a rebalance touching
        // only partitions with no obligations — normal, not a warning.
        tracing::debug!(
            topic,
            revoked = ?events.revoked,
            assigned = ?events.assigned,
            lost_all = events.lost_all,
            "rebalance observed with nothing to re-arm (no durable frontier or \
             in-session progress on the affected partitions)",
        );
    }
}

/// Run the streaming poll loop on an already-created [`StreamConsumer`]
/// (from [`create_stream_consumer`]) until a shutdown signal is received.
///
/// Buffers records, rolling a segment when either `max_rows_per_segment`
/// rows accumulate OR `segment_flush_interval_ms` elapses; each rolled
/// segment is published through `sink`. On shutdown the residual buffer is
/// flushed before returning; a failure of THAT final flush is reported in
/// [`StreamingStats::final_flush_failed`] (Codex R26 F2) because the rows
/// it dropped will never be replayed once the supervisor is recorded as
/// cleanly stopped. A failed MID-stream flush is recorded STICKILY in
/// [`StreamingStats::mid_stream_flush_failed`] (Codex R27 F2, via
/// [`publish_rolled_mid_stream`]) for the same reason: a later clean —
/// typically empty — final drain must not launder those dropped rows into
/// a "cleanly stopped" record. Does NOT commit offsets (see
/// [`create_stream_consumer`] / the module header).
///
/// `expected_cluster_id` is the Kafka cluster id resolved and stamped at
/// consumer START (`None` when it could not be resolved). A low-cadence
/// TRIPWIRE ([`CLUSTER_DRIFT_CHECK_INTERVAL`]) re-fetches the live cluster id
/// and compares (Codex R37 F3): if it has DRIFTED — a broker hostname
/// repointed to a DIFFERENT cluster, which librdkafka follows on RECONNECT (a
/// path the R30 F1 `rebootstrap` pin does NOT cover) — the loop discards the
/// in-flight buffer and stops fail-loud with
/// [`StreamingStats::cluster_id_drifted`] set. It runs on a cadence (not per
/// publish) because each check re-fetches the cluster id, which leaks a small
/// upstream allocation and forces a metadata round-trip; see
/// [`cluster_identity_drifted`] and the in-body comment for the deliberate
/// residuals (bounded pre-detection window; buffer discard over silent
/// cross-cluster contamination). Fail-safe: with `None` (nothing is stamped,
/// so the cleanup keeps such rows anyway) or an unresolvable live id it is a
/// no-op.
///
/// `resume_frontier` (compat-3 stage 2) is the per-partition offset the
/// consumer must resume PAST — the max `next` derived by the overlord from the
/// durable segment set's `payload.kafkaOffsets`. When non-empty, the loop
/// SEEKS each partition to its reconciled frontier ([`apply_resume_seek`]) so
/// records already persisted (and reloaded from deep storage at bootstrap)
/// are neither replayed nor double-counted — and ENFORCES it through a
/// [`ResumeSeekGate`] (Codex R3): a partition whose authoritative seek has
/// not succeeded (assignment timeout — C2, seek+pause failure — C3, rewind
/// failure — C4) has its records dropped un-consumed while the seek is
/// retried on a timer; a paused partition is resumed on a successful retry
/// (H6). Empty on a first start (no durable rows) — `auto.offset.reset` /
/// committed offsets then govern, exactly as Druid.
///
/// When the overlord positively detected a TOPIC RECREATION
/// ([`ResumeFrontier::topic_recreated`], Codex R18 C1+C2), committed offsets
/// are untrusted for EVERY partition of the topic: assigned partitions with
/// no durable frontier entry get a floor-low recreation entry synthesized
/// (at startup in [`apply_resume_seek`], on later rebalances in
/// `drain_rebalance_and_rearm`) and are gated until their seek to the
/// retained log's `low` watermark lands — never resumed from the DEAD
/// generation's committed offset (which survives the delete+recreate and
/// would silently skip every new-generation record below it).
#[cfg(feature = "kafka-io")]
pub async fn run_streaming_loop<S: SegmentSink>(
    consumer: KafkaStreamConsumer,
    config: KafkaConsumerConfig,
    sink: S,
    expected_cluster_id: Option<String>,
    mut resume_frontier: ResumeFrontier,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
) -> StreamingStats {
    use std::sync::Arc;
    use std::time::Duration;

    use rdkafka::Message;
    use rdkafka::consumer::Consumer as _;

    let topic = config.topic.clone();
    let mut buffer = StreamingBuffer::from_config(&config);
    // Contiguous-commit tracker (compat-3 durability, C1): keeps the committed
    // offset from ever advancing past a gap left by a failed publish. Only the
    // per-partition offsets it returns are ever committed, and only when the
    // sink is durable (`commits_offsets`, C3).
    let mut frontier = ContiguousFrontier::new();
    let commits_offsets = sink.commits_offsets();
    // Shared so the low-cadence drift tripwire (Codex R37 F3) can run its
    // BLOCKING librdkafka metadata/cluster-id fetch on `spawn_blocking` instead
    // of stalling the async worker (the same pattern the overlord's
    // `resolve_cluster_id` / `verify_topic_readable` use). `recv()` still works
    // through the `Arc` deref, and the tripwire only runs while `recv()` is not
    // being polled (a different select! arm won), so there is no concurrent use.
    let consumer = Arc::new(consumer);

    // Rebalance observer (Codex R4 H2): the consumer's context records every
    // revoke/assign into this shared accumulator; the loop drains it BEFORE
    // every gate verdict / retry pass and re-arms the seek gate for the
    // affected partitions, so a post-rebalance stale-committed position can
    // never flow without a fresh authoritative seek.
    let rebalance_notices = consumer.client().context().notices();
    // The per-partition restore floor a rebalance re-arm seeks back to (Codex
    // R4 H2 → R12 H1) is NOT tracked as a raw consumed-forward point anymore:
    // that would include a DROPPED (failed-publish) batch and seek PAST it =
    // loss. It is derived on demand as the durable-OR-buffered frontier
    // (`safe_resume_frontier(&frontier, &buffer)`, inside
    // `drain_rebalance_and_rearm`) — durable-contiguous progress extended by
    // the still-recoverable buffered tail, excluding any dropped batch.

    // Durable resume (compat-3 stage 2, hardened by Codex R3): before
    // consuming, seek each assigned partition PAST the durable frontier so
    // already-persisted records (loaded from deep storage at bootstrap) are
    // not replayed. The returned GATE carries every partition whose
    // authoritative seek has not succeeded yet (assignment timeout, seek or
    // pause failure, rewind failure): their records are DROPPED un-consumed
    // below and the seek is retried on `seek_retry_timer` until it lands —
    // no failure path consumes a partition at a non-frontier offset
    // (C2/C3/C4) and no gated partition stalls until restart (H6). A no-op
    // on a first start (empty frontier).
    let mut seek_gate = apply_resume_seek(
        consumer.as_ref(),
        &topic,
        &mut resume_frontier,
        &rebalance_notices,
        &mut frontier,
    )
    .await;

    // Retry cadence for gated partitions (Codex R3 H6). Ticks are cheap
    // no-ops once the gate is clear (the common case: an empty gate on a
    // first start, or every seek landing in `apply_resume_seek`).
    let mut seek_retry_timer = tokio::time::interval(RESUME_SEEK_RETRY_INTERVAL);
    seek_retry_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    seek_retry_timer.tick().await; // first tick fires immediately; skip it

    let flush_ms = config.segment_flush_interval_ms.max(1);
    let mut flush_timer = tokio::time::interval(Duration::from_millis(flush_ms));
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick fires immediately; skip it so an empty buffer is not
    // "flushed" before any data arrives.
    flush_timer.tick().await;

    // Set by the shutdown branch (Codex R26 F2): a failed FINAL drain must
    // surface in the returned stats, not vanish into a log line — the
    // caller is about to record this consumer as "cleanly stopped". Default
    // `false` so the new drift/stop break paths (Codex R37 F3) can exit the
    // loop without publishing and without a spurious failed-drain record.
    let mut final_flush_failed = false;
    // STICKY mid-stream failure record (Codex R27 F2): set by any failed
    // threshold/timer flush, never cleared — see `publish_rolled_mid_stream`.
    let mut mid_stream_flush_failed = false;
    // Fatal consume error (Codex R30 F2): the loop stopped ITSELF because
    // recv() reported an error no retry can heal (see
    // `consume_error_is_fatal`) — surfaced in the stats so the stop is
    // never recorded as clean.
    let mut fatal_consume_error = false;
    // Cluster-identity drift (Codex R37 F3): set when the live cluster id no
    // longer matches `expected_cluster_id`, at which point the buffered rows
    // are NOT published (they may belong to the other cluster) and the loop
    // stops fail-loud.
    let mut cluster_id_drifted = false;

    // Cluster-identity drift TRIPWIRE (Codex R37 F3) + pre-publish identity
    // check (Codex R6 H4). The tripwire runs on a fixed LOW cadence (each
    // check re-fetches the live cluster id, which leaks a small upstream
    // allocation — see `CLUSTER_DRIFT_CHECK_INTERVAL` — and forces a metadata
    // round-trip) and catches a drift while the topic is IDLE; since R6 H4
    // every NON-EMPTY publish additionally re-confirms the identity
    // immediately before stamping, so a drift landing between ticks can no
    // longer be durably stamped with the old cluster's provenance. Publish
    // cadence is bounded below by the flush interval, so the added leak/RTT
    // stays the same order as the tripwire's. Only meaningful when the
    // start-time identity was resolved (`Some`): with `None` nothing is
    // stamped and the respawn cleanup keeps such rows anyway, so there is
    // nothing to mis-attribute. Deliberate residuals (documented, this is a
    // proximity guard like the R31 empty-topic guard — the true fix is
    // durable per-cluster provenance, FG-7):
    //   * the pre-publish check is check-then-stamp, not atomic: a repoint
    //     landing in the instants between the confirmation and the metadata
    //     write — or one the broker HIDES (live id unresolvable at check
    //     time, the fail-safe polarity) — can still stamp the old id
    //     (narrowed from the pre-R6 up-to-one-interval window, not zero);
    //   * on detection the in-flight buffer is DISCARDED, not published — by
    //     the time a drift is noticed the buffer holds only rows consumed since
    //     the last flush (≤ one flush interval), and those are the MOST likely
    //     to belong to the NEW cluster, so publishing them stamped with the old
    //     id would be silent cross-cluster contamination that a later same-pair
    //     earliest re-create turns into loss. A LOUD replay-required discard of
    //     ≤ one flush interval (recoverable if the operator restores the
    //     bootstrap DNS) is the deliberate trade — loud bounded refusal over
    //     silent corruption;
    //   * the check can only OBSERVE a drift the broker reports: if the
    //     repointed cluster returns metadata WITHOUT a cluster id (a pre-KIP-78
    //     / misconfigured broker), librdkafka keeps the cached start-time id and
    //     the tripwire cannot distinguish it — the same "no durable cluster
    //     identity under #![forbid(unsafe_code)]" limit as R31/R33 (FG-7 fix);
    //   * the drift stop's replay-required sentinel is recovered by a re-create
    //     via the R33 `recoverable_by` bootstrap-STRING match, which — like all
    //     bootstrap-string identity in this crate — cannot tell a same-string
    //     DNS repoint apart from the original cluster, so a re-create while the
    //     name still resolves to the OTHER cluster can supersede the obligation
    //     (documented R26 F3 / R33 ambiguity; FG-7's durable per-cluster
    //     provenance is the real fix).
    let expected_cluster = expected_cluster_id.as_deref();
    let mut drift_timer = tokio::time::interval(CLUSTER_DRIFT_CHECK_INTERVAL);
    drift_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // First tick fires immediately; skip it (nothing has drifted at start).
    drift_timer.tick().await;

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                // Pre-publish identity check (Codex R6 H4): the final drain
                // stamps the start-time cluster id, so a drift that happened
                // since the last tripwire tick must refuse the publish
                // rather than durably mis-stamp the buffer.
                if !buffer.is_empty()
                    && live_identity_drifted(&consumer, &topic, expected_cluster).await
                {
                    log_identity_drift_stop(&topic, expected_cluster, "pre-publish (final drain)");
                    cluster_id_drifted = true;
                    break;
                }
                // Final drain: publish the residual buffer and, on Ok, commit
                // its offsets (compat-3 stage 2). An Err records a failed final
                // drain (Codex R26 F2) and commits nothing.
                match publish_rolled(&sink, &topic, &mut buffer).await {
                    Ok(Some(offsets)) => {
                        commit_contiguous(
                            commits_offsets, &mut frontier, consumer.as_ref(), &topic, &offsets,
                        );
                    }
                    Ok(None) => {}
                    Err(_) => final_flush_failed = true,
                }
                break;
            }
            _ = drift_timer.tick() => {
                // Low-cadence drift tripwire (Codex R37 F3). A confirmed drift
                // stops the consumer fail-loud and DISCARDS the in-flight
                // buffer (see the block comment above). Fail-safe: an
                // unresolved / unchanged identity is a no-op. The BLOCKING
                // metadata + cluster-id fetch runs on `spawn_blocking` (inside
                // `live_identity_drifted`) so it never stalls the async worker
                // (a slow/unreachable broker would otherwise block
                // ingestion/shutdown for up to the fetch timeouts); a join
                // failure degrades to "no drift".
                if live_identity_drifted(&consumer, &topic, expected_cluster).await {
                    log_identity_drift_stop(&topic, expected_cluster, "periodic tripwire");
                    cluster_id_drifted = true;
                    break;
                }
            }
            _ = flush_timer.tick() => {
                // Pre-publish identity check (Codex R6 H4), see above.
                if !buffer.is_empty()
                    && live_identity_drifted(&consumer, &topic, expected_cluster).await
                {
                    log_identity_drift_stop(&topic, expected_cluster, "pre-publish (flush timer)");
                    cluster_id_drifted = true;
                    break;
                }
                // Mid-stream: non-fatal for the LOOP, recorded stickily on
                // failure (Codex R27 F2). On Ok commit the covered offsets;
                // on Err re-seek the affected partitions back to their durable
                // frontier so the dropped batch is re-consumed rather than
                // consumed-forward-past (Codex R12 H2).
                flush_and_commit_mid_stream(
                    &sink, consumer.as_ref(), &topic, &mut buffer, commits_offsets,
                    &mut frontier, &mut seek_gate, &resume_frontier.partitions,
                    &mut mid_stream_flush_failed,
                ).await;
            }
            _ = seek_retry_timer.tick() => {
                // Rebalance re-arm FIRST (Codex R4 H2), so partitions a
                // revoke/reassign affected re-enter the retry set and are
                // re-sought below even when no record arrives to trigger the
                // recv-arm drain.
                drain_rebalance_and_rearm(
                    &rebalance_notices, &mut seek_gate, &mut resume_frontier,
                    &frontier, &buffer, &topic,
                );
                // Retry the authoritative seek for gated partitions (Codex R3
                // H6): a transient failure pauses/gates a partition, this
                // re-seeks it every interval, and a success resumes +
                // activates it. A cheap no-op when the gate is clear.
                if !seek_gate.is_clear() {
                    try_activate_pending(
                        consumer.as_ref(), &topic, &mut seek_gate, &resume_frontier.partitions,
                        &mut frontier,
                    );
                }
            }
            msg = consumer.recv() => {
                match msg {
                    Ok(m) => {
                        // Rebalance re-arm BEFORE the gate verdict (Codex R4
                        // H2): the context's callbacks run inline on this
                        // polling thread and share its event queue with the
                        // records, so a (re-)assignment notice is always
                        // recorded before the first post-rebalance record is
                        // returned — draining here guarantees that record
                        // meets an already re-armed gate and is suppressed
                        // until the fresh authoritative seek lands.
                        drain_rebalance_and_rearm(
                            &rebalance_notices, &mut seek_gate, &mut resume_frontier,
                            &frontier, &buffer, &topic,
                        );
                        // Seek-gate verdict (Codex R3 C2/C3/C4): a record
                        // on a partition whose authoritative resume seek has
                        // not succeeded yet must be DROPPED — consuming it
                        // would replay durable rows (double count) or commit
                        // past a gap (loss). Its offset is recorded NOWHERE
                        // (neither the buffer nor the commit frontier), so the
                        // successful seek re-delivers from the authoritative
                        // target and nothing is lost.
                        if !seek_gate.record_delivered(m.partition()) {
                            tracing::debug!(
                                topic = %topic,
                                partition = m.partition(),
                                offset = m.offset(),
                                "dropping record on a seek-gated partition (the \
                                 authoritative resume seek has not succeeded yet)",
                            );
                            continue;
                        }
                        // ACCEPTED. The restore point a rebalance re-arm seeks
                        // back to is derived from the durable frontier + the
                        // buffered tail (Codex R4 H2 → R12 H1), not tracked as
                        // a raw consumed-forward point — the offset is fed to
                        // the buffer + contiguous frontier just below.
                        // Feed the message's (partition, offset) to the buffer
                        // FIRST (compat-3 stage 2) so a roll triggered by THIS
                        // message covers its offset; recorded even for a
                        // dead-lettered payload (its message was consumed).
                        buffer.record_offset(m.partition(), m.offset());
                        // Seed this partition's contiguous-commit base to its
                        // first consumed offset (compat-3 durability, C1/H4).
                        frontier.note_consumed(m.partition(), m.offset());
                        if let Some(payload) = m.payload() {
                            match buffer.push_payload(payload) {
                                Ok(true) => {
                                    // Pre-publish identity check (Codex R6
                                    // H4), see the shutdown arm.
                                    if live_identity_drifted(
                                        &consumer, &topic, expected_cluster,
                                    ).await {
                                        log_identity_drift_stop(
                                            &topic, expected_cluster,
                                            "pre-publish (row threshold)",
                                        );
                                        cluster_id_drifted = true;
                                        break;
                                    }
                                    // Mid-stream: non-fatal, recorded on
                                    // failure, as above; commit on Ok, re-seek
                                    // the affected partitions on Err (Codex
                                    // R12 H2).
                                    flush_and_commit_mid_stream(
                                        &sink, consumer.as_ref(), &topic,
                                        &mut buffer, commits_offsets,
                                        &mut frontier, &mut seek_gate,
                                        &resume_frontier.partitions,
                                        &mut mid_stream_flush_failed,
                                    ).await;
                                }
                                Ok(false) => {}
                                Err(e) => {
                                    tracing::warn!(
                                        topic = %topic,
                                        partition = m.partition(),
                                        offset = m.offset(),
                                        error = %e,
                                        "skipping unparseable/invalid Kafka record",
                                    );
                                }
                            }
                        }
                    }
                    Err(e) if consume_error_is_fatal(&e) => {
                        // Fail-LOUD (Codex R30 F2): an authorization /
                        // librdkafka-fatal error never heals in-process, so
                        // warn-looping would starve ingestion while looking
                        // alive — after an earliest re-create's cleanup that
                        // silence permanently loses the dropped rows. Drain
                        // what was already consumed (real data — its publish
                        // outcome is accounted like a final drain), then stop;
                        // the fatal flag makes the stop non-clean so a
                        // tombstone/suspend is refused and a replay
                        // (resume / earliest re-create) stays possible.
                        tracing::error!(
                            topic = %topic, error = %e,
                            "FATAL Kafka consume error: stopping the consumer (no retry \
                             can heal it); the stop reports replay-required so the \
                             supervisor cannot be recorded as cleanly stopped",
                        );
                        fatal_consume_error = true;
                        // Pre-publish identity check (Codex R6 H4): even this
                        // last-gasp drain must not durably mis-stamp a
                        // drifted buffer.
                        if !buffer.is_empty()
                            && live_identity_drifted(&consumer, &topic, expected_cluster).await
                        {
                            log_identity_drift_stop(
                                &topic, expected_cluster, "pre-publish (fatal-error drain)",
                            );
                            cluster_id_drifted = true;
                            break;
                        }
                        match publish_rolled(&sink, &topic, &mut buffer).await {
                            Ok(Some(offsets)) => {
                                commit_contiguous(
                                    commits_offsets, &mut frontier,
                                    consumer.as_ref(), &topic, &offsets,
                                );
                            }
                            Ok(None) => {}
                            Err(_) => final_flush_failed = true,
                        }
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                            topic = %topic, error = %e,
                            "transient Kafka recv error (will retry)",
                        );
                    }
                }
            }
        }
    }

    StreamingStats {
        total_consumed: buffer.total_consumed(),
        total_published: buffer.total_published(),
        final_flush_failed,
        mid_stream_flush_failed,
        fatal_consume_error,
        cluster_id_drifted,
    }
}

/// How long [`apply_resume_seek`] polls for the group assignment to settle
/// before handing enforcement over to the poll loop (compat-3 stage 2).
/// Bounded; short in tests. A miss is NOT a fall-back to committed offsets /
/// `auto.offset.reset` (Codex R3 C2): the frontier partitions stay GATED in
/// the returned [`ResumeSeekGate`] — no record of theirs is consumed until
/// the authoritative seek succeeds on a later retry.
#[cfg(all(not(test), feature = "kafka-io"))]
const RESUME_SEEK_ASSIGN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
#[cfg(all(test, feature = "kafka-io"))]
const RESUME_SEEK_ASSIGN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// How long a single [`apply_resume_seek`] `seek` may block on librdkafka.
#[cfg(feature = "kafka-io")]
const RESUME_SEEK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Cadence at which the poll loop RETRIES the authoritative seek for gated
/// partitions ([`ResumeSeekGate::pending`], Codex R3 H6): a transient seek
/// failure (e.g. mid rebalance) pauses/gates the partition but must never
/// stall it until restart — every interval the pending partitions currently
/// in the assignment are re-sought, and a success resumes + activates them.
#[cfg(all(not(test), feature = "kafka-io"))]
const RESUME_SEEK_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
#[cfg(all(test, feature = "kafka-io"))]
const RESUME_SEEK_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(150);

/// Fetch the committed offset for ONE partition (`Some(committed)` on a
/// successful fetch — `committed` itself is `None` when nothing was ever
/// committed — and `None` when the fetch FAILED, i.e. the value is unknown).
/// The distinction matters (Codex R3): an unknown committed offset must keep
/// the partition gated (the authoritative target cannot be computed without
/// it — a commit BELOW `min_start` marks a never-durable gap the resume must
/// re-consume), while a known-absent one legitimately resolves the target
/// from the durable rows alone.
#[cfg(feature = "kafka-io")]
fn fetch_committed_offset(
    consumer: &KafkaStreamConsumer,
    topic: &str,
    partition: i32,
) -> Option<Option<i64>> {
    use rdkafka::consumer::Consumer as _;
    use rdkafka::{Offset, TopicPartitionList};

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, partition);
    match consumer.committed_offsets(tpl, RESUME_SEEK_TIMEOUT) {
        Ok(c) => Some(
            match c.find_partition(topic, partition).map(|e| e.offset()) {
                Some(Offset::Offset(v)) => Some(v),
                _ => None,
            },
        ),
        Err(e) => {
            tracing::warn!(
                topic, partition, error = %e,
                "resume seek: committed-offset fetch failed — the partition stays gated \
                 until the authoritative target can be computed (retried)",
            );
            None
        }
    }
}

/// Best-effort broker-side pause of ONE partition; `true` on success. The
/// gate — not this pause — is what suppresses consumption (Codex R3 C3): the
/// pause only stops librdkafka from fetching records the loop would drop.
#[cfg(feature = "kafka-io")]
fn pause_partition(consumer: &KafkaStreamConsumer, topic: &str, partition: i32) -> bool {
    use rdkafka::TopicPartitionList;
    use rdkafka::consumer::Consumer as _;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, partition);
    match consumer.pause(&tpl) {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(
                topic, partition, error = %e,
                "resume seek: safety pause failed — the seek gate still suppresses this \
                 partition's records (C3), and the seek is retried",
            );
            false
        }
    }
}

/// Resume `partition` broker-side if it is recorded paused, then activate it
/// — the ONLY path to [`SeekGateState::Active`] (Codex R4 H1). Returns `true`
/// when the partition was activated; `false` leaves it gated (+ paused) and
/// retried on the timer.
///
/// The invariant this enforces: **a partition is never Active while
/// broker-side paused**. Pre-H1, the failed-seek no-op carve-out
/// (`committed == target`) could activate a partition whose earlier failure
/// had paused it — Active meant it left the retry set, paused meant no
/// record would ever arrive: a silent permanent stall (no data flows, no
/// retry fires, until restart). [`ResumeSeekGate::mark_active`] itself now
/// refuses that transition; this helper performs the resume-first ordering
/// every activation path must take.
#[cfg(feature = "kafka-io")]
fn resume_and_activate(
    consumer: &KafkaStreamConsumer,
    topic: &str,
    gate: &mut ResumeSeekGate,
    partition: i32,
) -> bool {
    use rdkafka::TopicPartitionList;
    use rdkafka::consumer::Consumer as _;

    if gate.is_paused(partition) {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(topic, partition);
        match consumer.resume(&tpl) {
            Ok(()) => gate.note_unpaused(partition),
            Err(e) => {
                tracing::warn!(
                    topic, partition, error = %e,
                    "broker-side resume failed — the partition stays gated (never \
                     activated while paused, H1) and is retried",
                );
                return false;
            }
        }
    }
    gate.mark_active(partition)
}

/// Clamp the authoritative seek `target` into the partition's CURRENT
/// broker-side watermark window (Codex R4 H3): fetch `(low, high)` and apply
/// [`clamp_resume_target`]. Returns `(validated target, offset space
/// SHRANK)` — the second component is `true` exactly when `target > high`
/// (the log was truncated/recreated below the durable frontier, Codex R27
/// H1): the caller must RECORD that fact on the gate
/// ([`ResumeSeekGate::note_offset_space_reset`]) so a LATER rebalance
/// restore never trusts the partition's pre-truncation committed offset —
/// once the log regrows past it, the stale value is in-range again and this
/// clamp can no longer catch it. `None` when the watermark fetch failed —
/// the caller must keep the partition gated and retry, because seeking an
/// unvalidated target risks the exact OFFSET_OUT_OF_RANGE →
/// `auto.offset.reset` fallback (latest = silent loss of every retained
/// post-frontier record) the clamp exists to prevent. **Blocks up to
/// [`RESUME_SEEK_TIMEOUT`]** (like the adjacent committed-offset fetch /
/// seek calls).
///
/// `recreation` is `Some(resume)` when the pending seek is a
/// RECREATION-FLOOR seek (Codex R9 C2, [`ResumeSeekGate`]'s
/// `recreation_floor`): the incoming `target` is only a floor-0 placeholder
/// (the frontier derivation cannot know `low`), so the REAL target is
/// derived here — where the live watermarks are in hand — as
/// [`recreated_resume_target`]`(low, high, resume)`: floor `low`, advanced
/// contiguously through the live generation's coverage. This is what keeps a
/// live row starting ABOVE `low` from lifting the floor past a never-durable
/// `[low, live_start)` gap (loss), while still skipping coverage contiguous
/// from `low` (no per-restart re-publish — R8 H1).
///
/// `reset_floor` is `true` when the pending seek is a RESET-FLOOR seek (Codex
/// R27 H1 (a)/(b), [`ResumeSeekGate`]'s `reset_floor`): an offset-space reset
/// was already witnessed this session and the incoming `target` came from
/// now-untrusted sources (a stale committed offset, or old-numbering
/// coverage), so it is floored at `low` unconditionally — a regrown stale
/// target is back in-range and a plain clamp would forward-seek past regrown
/// records (loss).
#[cfg(feature = "kafka-io")]
fn clamped_seek_target(
    consumer: &KafkaStreamConsumer,
    topic: &str,
    partition: i32,
    target: i64,
    recreation: Option<&PartitionResume>,
    reset_floor: bool,
) -> Option<(i64, bool)> {
    use rdkafka::consumer::Consumer as _;

    match consumer.fetch_watermarks(topic, partition, RESUME_SEEK_TIMEOUT) {
        Ok((low, high)) => {
            let (resolved, offset_space_shrank) =
                resolve_seek_target(recreation, reset_floor, target, low, high);
            if recreation.is_some() {
                tracing::warn!(
                    topic,
                    partition,
                    placeholder_target = target,
                    derived = resolved,
                    low_watermark = low,
                    high_watermark = high,
                    "resume seek: RECREATION-FLOOR target derived against the live \
                     watermark window (R9 C2) — floor = low (the dead generation's \
                     committed offset and the live rows' min_start are both \
                     untrusted), advanced contiguously through the live \
                     generation's durable coverage; any [low, coverage) gap is \
                     re-consumed (bounded at-least-once duplication, never loss)",
                );
            } else if reset_floor {
                tracing::warn!(
                    topic,
                    partition,
                    untrusted_target = target,
                    floored = resolved,
                    low_watermark = low,
                    high_watermark = high,
                    "resume seek: RESET-FLOOR target floored at the live LOW \
                     watermark (R27 H1 (a)/(b)) — an offset-space reset was \
                     witnessed this session, so the pending target (from a stale \
                     committed offset or old-numbering coverage) is UNTRUSTED; a \
                     regrown stale target lands back in-range and a plain clamp \
                     would forward-seek past regrown records (loss). Re-consumed \
                     from low instead (bounded at-least-once duplication)",
                );
            } else if resolved != target {
                tracing::warn!(
                    topic,
                    partition,
                    target,
                    clamped = resolved,
                    low_watermark = low,
                    high_watermark = high,
                    "resume seek target is OUTSIDE the retained log — clamped to the \
                     LOW watermark so the seek can never trigger the broker-side \
                     OFFSET_OUT_OF_RANGE → auto.offset.reset fallback (latest would \
                     silently skip retained post-frontier records; earliest would \
                     replay the whole log). target < low means retention already \
                     deleted records below the durable frontier (bounded re-consume \
                     of the retained durable prefix = at-least-once duplication, \
                     never loss, H3); target > high means the log was truncated or \
                     recreated below the durable frontier — the retained records \
                     live in a NEW offset space, so they are re-consumed from low \
                     rather than skipped by a seek to the log end (R5 H1)",
                );
            }
            // `offset_space_shrank` (a first-seen `target > high`) tells the
            // caller to REMEMBER the reset (R27 H1) — after the log regrows,
            // the stale committed offset is in-range again and this clamp can
            // never catch it.
            Some((resolved, offset_space_shrank))
        }
        Err(e) => {
            tracing::warn!(
                topic, partition, target, error = %e,
                "resume seek: watermark fetch failed — the seek target cannot be \
                 validated against the retained log, so the partition stays gated \
                 and is retried (H3)",
            );
            None
        }
    }
}

/// One activation attempt for every gated partition currently ASSIGNED to the
/// consumer (Codex R3 C2/C3/C4/H6): resolve the authoritative target if still
/// unknown (committed-offset fetch + [`resume_offset`] for a frontier
/// partition, [`rebalance_restore_target`] for a rebalance-restore one — R6
/// H1), seek to it, and on success resume (if paused) + activate. Every
/// failure path leaves the partition GATED (suppressed via
/// [`ResumeSeekGate::record_delivered`]) and best-effort paused, to be
/// retried on the next call — never consumed at a non-frontier offset, never
/// permanently stalled.
///
/// `contiguous` is the loop's commit tracker: when the seek had to be
/// CLAMPED into the retained watermark window, the partition's
/// contiguous-commit base is re-based to the actual seek position
/// ([`ContiguousFrontier::reset_base`], Codex R6 H2) — the pre-clamp base
/// points at records that no longer exist, so keeping it would block every
/// future commit (never-commit → repeated durable duplication on every
/// restart). An UN-clamped seek never re-bases (see `reset_base` for why a
/// restore seek above the base must keep it).
///
/// Unassigned pending partitions are skipped silently (a seek/pause cannot
/// succeed on them); they are retried once the assignment includes them.
#[cfg(feature = "kafka-io")]
fn try_activate_pending(
    consumer: &KafkaStreamConsumer,
    topic: &str,
    gate: &mut ResumeSeekGate,
    frontier: &BTreeMap<i32, PartitionResume>,
    contiguous: &mut ContiguousFrontier,
) {
    use rdkafka::Offset;
    use rdkafka::consumer::Consumer as _;

    if gate.is_clear() {
        return;
    }
    let assigned: std::collections::BTreeSet<i32> = match consumer.assignment() {
        Ok(a) => a
            .elements()
            .iter()
            .filter(|e| e.topic() == topic)
            .map(rdkafka::topic_partition_list::TopicPartitionListElem::partition)
            .collect(),
        Err(e) => {
            tracing::warn!(
                topic, error = %e,
                "resume seek: could not read the assignment — gated partitions stay \
                 suppressed and are retried",
            );
            return;
        }
    };
    for partition in gate.pending() {
        if !assigned.contains(&partition) {
            continue;
        }
        // Resolve the authoritative target if it is still unknown. The
        // committed offset is fetched fresh here (and again below for the
        // failed-seek carve-out) — a failed fetch keeps the partition gated.
        let target = match gate.state(partition) {
            SeekGateState::Active => continue,
            SeekGateState::NeedsSeek { target } => target,
            // Post-rebalance restore (Codex R6 H1 → R12 H1): resolve against
            // the FRESH committed offset — `max(committed, resume_floor)`,
            // never below the group's durable progress. `resume_floor` is the
            // durable-OR-buffered frontier (never a raw consumed-forward point
            // that includes a dropped batch — R12 H1). Resolution downgrades
            // the state to a plain `NeedsSeek`; a LATER rebalance re-arms it
            // with a fresh `resume_floor` (and hence a fresh committed fetch),
            // so the resolved value can only go stale while this consumer owns
            // the partition — during which no other member can advance the
            // commit.
            //
            // COMMITTED-UNTRUSTED partitions never read the committed offset
            // here — it names a DEAD offset numbering, and with the log
            // produced past it again the stale value is IN-range —
            // `max()`ing it in seeks the restore past unconsumed records
            // (permanent loss). `resume_floor` alone drives
            // ([`restore_target`]); it is always live-numbering progress
            // because records only flow after the floor/clamped seek landed.
            // Two witnesses arm this:
            //
            // * RECREATION-DETECTED (Codex R9 C1): the committed offset
            //   survives the delete+recreate in the group coordinator but
            //   names the dead generation's record;
            // * OFFSET-SPACE RESET (Codex R27 H1): a same-topic-id
            //   truncation (unclean leader election) shrank the log under
            //   the durable frontier — witnessed by a `target > high`
            //   watermark clamp this session — and the log has since
            //   regrown over the reused offset numbers. `recreated` never
            //   arms here (no topic id changed), so the gate's clamp
            //   history is the only detector.
            SeekGateState::NeedsRestore { resume_floor } => {
                let recreated = frontier.get(&partition).is_some_and(|r| r.recreated);
                if recreated || gate.offset_space_reset_detected(partition) {
                    let target = restore_target(true, None, resume_floor);
                    tracing::warn!(
                        topic,
                        partition,
                        resume_floor,
                        recreated,
                        offset_space_reset = gate.offset_space_reset_detected(partition),
                        "rebalance restore on a COMMITTED-UNTRUSTED partition \
                         (recreation R9 C1 / offset-space reset R27 H1): the group's \
                         committed offset is (or may still be) the dead numbering's \
                         and is NOT consulted — restoring to this consumer's own \
                         live progress instead",
                    );
                    gate.set_target(partition, target);
                    target
                } else {
                    match fetch_committed_offset(consumer, topic, partition) {
                        Some(committed) => {
                            let target = restore_target(false, committed, resume_floor);
                            gate.set_target(partition, target);
                            target
                        }
                        None => {
                            // Unknown committed offset ⇒ the restore floor
                            // cannot be validated against the group's progress.
                            // Keep the partition gated + best-effort paused;
                            // retried.
                            if pause_partition(consumer, topic, partition) {
                                gate.note_paused(partition);
                            }
                            continue;
                        }
                    }
                }
            }
            SeekGateState::NeedsTarget => {
                let Some(resume) = frontier.get(&partition) else {
                    // Invariant: NeedsTarget is only ever set for frontier
                    // partitions. Defensive: without durable evidence there
                    // is nothing to seek to. Resume-first (H1): if a prior
                    // failure paused it, a failed resume keeps it gated.
                    let _ = resume_and_activate(consumer, topic, gate, partition);
                    continue;
                };
                if resume.recreated {
                    // R9: the committed offset is untrusted on a
                    // recreation-detected partition (dead generation), so
                    // resolution neither fetches nor waits on it. The
                    // floor-0 placeholder from `resume_offset` is re-derived
                    // against the live watermark window at clamp time
                    // (`recreated_resume_target` — floor = low, advance =
                    // live coverage); arming the recreation floor switches
                    // the clamp to that derivation AND schedules the
                    // dead-committed overwrite on the successful seek (C1).
                    gate.arm_recreation_floor(partition);
                    let target = resume_offset(None, resume);
                    gate.set_target(partition, target);
                    target
                } else if gate.offset_space_reset_detected(partition) {
                    // R27 H1 (b): a rebalance re-derived this partition's
                    // target via `require_target` BEFORE its truncation-clamped
                    // seek ever activated (no in-session progress ⇒ no restore
                    // floor ⇒ `require_target`, not `set_restore`). The
                    // committed offset and the old-numbering durable coverage
                    // are BOTH untrusted after the offset-space reset — and
                    // once the log regrew past the stale committed offset,
                    // `resume_offset(committed, resume)` lands IN-range and a
                    // plain clamp would forward-seek past regrown records
                    // (loss). Arm the reset floor so the seek is pinned at the
                    // live `low` (re-consume, bounded dup); the committed fetch
                    // is skipped entirely (its value is meaningless here). The
                    // `target` is a placeholder re-derived at clamp time.
                    gate.arm_reset_floor(partition);
                    let target = resume_offset(None, resume);
                    gate.set_target(partition, target);
                    tracing::warn!(
                        topic,
                        partition,
                        "resume seek: RESET-FLOOR armed on a rebalance-rederived \
                         target (R27 H1 (b)) — the offset space reset earlier this \
                         session, so the committed offset + old-numbering coverage \
                         are untrusted; the seek is pinned at the live low watermark \
                         (re-consume, never a forward skip past regrown records)",
                    );
                    target
                } else {
                    match fetch_committed_offset(consumer, topic, partition) {
                        Some(committed) => {
                            let target = resume_offset(committed, resume);
                            gate.set_target(partition, target);
                            target
                        }
                        None => {
                            // Unknown committed offset ⇒ the authoritative target
                            // cannot be computed. Keep the partition gated +
                            // best-effort paused; retried.
                            if pause_partition(consumer, topic, partition) {
                                gate.note_paused(partition);
                            }
                            continue;
                        }
                    }
                }
            }
        };
        // Validate the target against the retained log BEFORE seeking (H3):
        // an out-of-range seek "succeeds" locally, then the first fetch hits
        // OFFSET_OUT_OF_RANGE broker-side and auto.offset.reset takes over —
        // under `latest` every retained post-frontier record is skipped
        // (loss). An unvalidatable target keeps the partition gated+paused.
        // A pending RECREATION-FLOOR seek (R9 C2) is instead re-derived
        // against the watermarks fetched here (floor = low, advance = live
        // coverage — see `clamped_seek_target`).
        let recreation = gate
            .recreation_floor_armed(partition)
            .then(|| frontier.get(&partition))
            .flatten();
        // R27 H1 (a)/(b): a partition whose reset floor is armed (its target
        // came from now-untrusted committed / old-numbering coverage) is
        // pinned at the live `low`, so a REGROWN stale target can never
        // forward-seek past regrown records (loss). Recreation takes
        // precedence (its own live-coverage derivation).
        let reset_floor = recreation.is_none() && gate.reset_floor_armed(partition);
        let Some((target, offset_space_shrank)) =
            clamped_seek_target(consumer, topic, partition, target, recreation, reset_floor)
        else {
            if pause_partition(consumer, topic, partition) {
                gate.note_paused(partition);
            }
            continue;
        };
        if offset_space_shrank && !gate.offset_space_reset_detected(partition) {
            // Recorded at DETECTION time (before the seek attempt, R27 H1):
            // even if the clamped seek fails, the truncation was witnessed —
            // this partition's committed offset predates the offset-space
            // reset and must never drive a rebalance restore again this
            // session (once the log regrows, the stale value is in-range
            // and undetectable by the watermark clamp).
            gate.note_offset_space_reset(partition);
            // (a) Arm the PER-SEEK reset floor too: this pending seek's gate
            // target is still the PRE-clamp stale value, so if THIS seek fails
            // and a regrowth happens before the retry, the NeedsSeek retry
            // would otherwise re-clamp the now-in-range stale target and
            // forward-seek past regrown records (loss). Arming pins every
            // retry at `low` until the seek activates (disarmed there).
            gate.arm_reset_floor(partition);
            tracing::warn!(
                topic,
                partition,
                target,
                "offset space SHRANK under the durable frontier (log truncation / \
                 recreation invisible to the topic-id probe) — recorded (R27 H1): \
                 this partition's committed offset predates the reset, so every \
                 later rebalance restore this session is resume_floor-driven \
                 alone, and every retry of THIS seek is pinned at the low \
                 watermark (bounded re-consume, never a skip past regrown records)",
            );
        }
        match consumer.seek(
            topic,
            partition,
            Offset::Offset(target),
            RESUME_SEEK_TIMEOUT,
        ) {
            Ok(()) => {
                // A CLAMPED seek moved the consume position somewhere the
                // pre-clamp contiguous-commit base can never reconnect to
                // (the in-between records were deleted by retention, or the
                // whole offset space was recreated): re-base the commit
                // tracker to the actual seek position (Codex R6 H2), or no
                // offset would ever be committed again and every restart
                // would re-consume + re-publish the retained log. The
                // gate's recorded target is aligned too, so a retry after a
                // failed resume re-seeks the SAME validated position. A
                // RESET-FLOOR seek (R27 H1 (a)/(b)) ALWAYS re-bases: its
                // `target` is the live `low` of a NEW offset numbering, so an
                // old-numbering base would sit above every new span and pin
                // the commit frontier forever (R6 H2 never-commit), even when
                // the placeholder gate target happens to equal `low`.
                if reset_floor
                    || matches!(gate.state(partition),
                        SeekGateState::NeedsSeek { target: t } if t != target)
                {
                    contiguous.reset_base(partition, target);
                    gate.set_target(partition, target);
                }
                // R9 C1 (recreation resume, step 1): the group's committed
                // offset still points into the DEAD generation's offset
                // space — OVERWRITE it with the floor-seek position NOW, so
                // every later committed-offset reader (the R6 H1 restore
                // max, a post-restart derivation after the operator deletes
                // the dead rows, the failed-seek carve-out) sees a
                // new-generation value instead of the dead one. Sound
                // unconditionally: `target` is `low` advanced only through
                // DURABLE live coverage, so the commit never claims
                // durability for anything that is not either durable or
                // already deleted by retention. Async + best-effort — if it
                // never lands, the resume_floor-driven restore (step 2)
                // and the restart re-derivation still hold the no-loss
                // invariant.
                if gate.disarm_recreation_floor(partition) {
                    let overwrite: BTreeMap<i32, i64> =
                        std::iter::once((partition, target)).collect();
                    commit_offsets(consumer, topic, &overwrite);
                    tracing::warn!(
                        topic,
                        partition,
                        target,
                        "recreation floor seek landed (R9 C1): overwriting the DEAD \
                         generation's committed offset with the new-generation floor \
                         position (async; the restore path never trusts the old value \
                         either way)",
                    );
                }
                // Resume-first activation (H1/H6): a paused partition is
                // resumed BEFORE activating, or no record would ever flow
                // again (a silent permanent stall). A failed resume keeps
                // the partition gated; the next retry re-seeks (harmless —
                // same target) and re-tries the resume.
                if resume_and_activate(consumer, topic, gate, partition) {
                    tracing::info!(
                        topic,
                        partition,
                        target,
                        "resume: sought partition to the reconciled durable frontier \
                         (resumed first if paused, H1/H6)",
                    );
                }
            }
            Err(e) => {
                // The committed==target carve-out is only sound while the
                // partition's fetch position is untouched (no record was ever
                // delivered/discarded on it) — [`ResumeSeekGate::taint`].
                let committed = fetch_committed_offset(consumer, topic, partition);
                if let Some(c) = committed
                    && gate.may_noop_failed_seek(partition, c, target)
                {
                    // Resume-first here too (H1): the pre-H1 shape activated
                    // a still-paused partition on this carve-out — Active +
                    // paused + out of the retry set = data stopped flowing
                    // forever. A failed resume keeps it gated + retried.
                    if resume_and_activate(consumer, topic, gate, partition) {
                        tracing::warn!(
                            topic, partition, target, error = %e,
                            "resume seek failed but the consumer is still exactly at the \
                             durable frontier (committed == target, untouched position) — \
                             harmless no-op (resumed first if paused, H1)",
                        );
                    }
                    continue;
                }
                // A failed authoritative seek must NOT let the consumer
                // consume from its natural position in ANY direction: forward
                // (skip) would replay + re-publish durable rows = double
                // count (H6); backward (re-consume) would stay at the
                // stale-high committed offset and SKIP the phantom/gap span =
                // loss (C2); no commit would fall back to auto.offset.reset =
                // skip/replay the whole durable span (C1). The gate keeps the
                // partition suppressed even if the pause fails (C3), and the
                // retry timer re-seeks it (H6).
                if pause_partition(consumer, topic, partition) {
                    gate.note_paused(partition);
                }
                tracing::error!(
                    topic, partition, target, committed = ?committed, error = %e,
                    paused = gate.is_paused(partition),
                    "resume seek to the durable frontier FAILED — the partition is gated \
                     (its records are dropped un-consumed) until a retried seek succeeds \
                     (C1/C2/C3/H6)",
                );
            }
        }
    }
}

/// Establish the per-partition resume-seek GATE after a restart (compat-3
/// durability): every partition with a durable frontier starts gated, the
/// group join is driven in a bounded poll loop, and one activation pass
/// ([`try_activate_pending`]) seeks the assigned partitions to their
/// authoritative `resume_offset(committed, resume)` targets. The returned
/// [`ResumeSeekGate`] carries whatever could not be activated — the poll loop
/// suppresses those partitions' records and retries the seek on a timer, so
/// NO failure path (assignment timeout — C2, seek+pause failure — C3, rewind
/// failure — C4, transient pause — H6) ever consumes a partition at a
/// non-frontier offset or stalls it until restart.
///
/// Records delivered during the join drive are NOT lost: a frontier
/// partition's records are re-delivered by its authoritative seek (the drive
/// only TAINTS it, invalidating the no-op carve-out), and a NON-frontier
/// partition is rewound to the earliest discarded offset (H4) — a failed
/// rewind gates the partition at that offset instead of dropping the records
/// (C4).
///
/// **Topic recreation** (`frontier.topic_recreated`, Codex R18 C1+C2): the
/// DEAD generation's committed offsets survive the delete+recreate, so NO
/// partition may resume from its committed offset — not even one with no
/// durable frontier entry (C1: every durable row disabled → empty map; C2: a
/// topicId-stamped row without `kafkaOffsets` names no partition). The drive
/// therefore (a) does NOT no-op on an empty map, and (b) synthesizes a
/// floor-low recreation entry ([`ResumeFrontier::synthesize_recreated`]) for
/// every assigned partition missing from the map — gating it until its
/// low-watermark floor seek lands, exactly like a dead-only R9 partition.
/// Later (re)assignments are covered the same way by
/// [`drain_rebalance_and_rearm`].
///
/// Fail-safe: an empty frontier WITHOUT a detected recreation is a no-op
/// (first start — committed offsets / `auto.offset.reset` govern, as Druid).
#[cfg(feature = "kafka-io")]
async fn apply_resume_seek(
    consumer: &KafkaStreamConsumer,
    topic: &str,
    frontier: &mut ResumeFrontier,
    notices: &RebalanceNotices,
    contiguous: &mut ContiguousFrontier,
) -> ResumeSeekGate {
    use rdkafka::consumer::Consumer as _;

    let mut gate = ResumeSeekGate::new();
    // Nothing to reconcile AND nothing can have been discarded (the drive only
    // runs when there is a durable frontier to seek to) — unless a recreation
    // was detected (R18 C1): then the committed offsets are untrusted even
    // with no durable frontier entry, and the drive must run to floor every
    // assigned partition at `low`.
    if frontier.partitions.is_empty() && !frontier.topic_recreated {
        return gate;
    }
    // EVERY frontier partition starts gated (C2): no record of theirs is
    // consumed until its authoritative seek succeeds — regardless of when (or
    // whether) the assignment settles.
    for &p in frontier.partitions.keys() {
        gate.require_target(p);
    }
    let step = std::time::Duration::from_millis(200);
    let deadline = tokio::time::Instant::now() + RESUME_SEEK_ASSIGN_TIMEOUT;
    // Earliest offset delivered-and-discarded per NON-frontier partition
    // during the join drive, so it can be rewound (H4).
    let mut discarded: BTreeMap<i32, i64> = BTreeMap::new();
    let mut assignment_seen = true;
    loop {
        if consumer
            .assignment()
            .map(|a| a.count() > 0)
            .unwrap_or(false)
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(
                topic,
                gated = gate.pending().len(),
                "resume seek: no partition assignment within the window — the frontier \
                 partitions STAY GATED (their records are not consumed) until the \
                 authoritative seek succeeds on a later retry (C2)",
            );
            assignment_seen = false;
            break;
        }
        if let Ok(Ok(m)) = tokio::time::timeout(step, consumer.recv()).await {
            use rdkafka::Message as _;
            let p = m.partition();
            let o = m.offset();
            if frontier.partitions.contains_key(&p) {
                // A frontier partition's pre-seek record: dropped, and the
                // partition TAINTED so a failed seek can never no-op past it.
                gate.record_delivered(p);
            } else if frontier.topic_recreated {
                // R18 C1/C2: under a detected recreation a partition with no
                // durable evidence must NOT take the non-frontier
                // discard+rewind path — its committed offset (and this
                // record's offset) may name the DEAD generation's space.
                // Synthesize its floor-low recreation entry, gate it, and
                // drop the record (the low-floor seek re-delivers it).
                frontier.synthesize_recreated([p]);
                gate.require_target(p);
                gate.record_delivered(p);
            } else {
                discarded
                    .entry(p)
                    .and_modify(|e| *e = (*e).min(o))
                    .or_insert(o);
            }
        }
    }
    // Non-frontier partitions whose records were discarded during the drive
    // must be rewound to the earliest discarded offset (H4). Gating them
    // enforces it: a failed rewind seek keeps them suppressed + retried
    // instead of silently dropping the records (C4).
    for (&p, &first) in &discarded {
        gate.set_target(p, first);
        gate.taint(p);
    }
    // Consume the rebalance notices accumulated during the join drive —
    // including the INITIAL assignment — BEFORE the activation pass (Codex
    // R4 H2). At this point nothing has been consumed (the restore-floor map
    // is empty) and every frontier partition is still gated, so the initial
    // assignment re-arms to the state the gate is already in (idempotent),
    // while a genuine revoke+reassign during the drive is re-armed exactly
    // like a mid-session one. Draining here also keeps the loop's first
    // drain from re-gating partitions this activation pass just activated.
    let drive_events = notices.take();
    // R18 C1/C2: under a detected recreation, EVERY assigned partition —
    // with or without a durable frontier entry — must be low-floored before
    // it may consume (its committed offset may name the DEAD generation's
    // offset space). The initial assignment is recorded in the drive notices
    // (the rebalance callback runs inline on the polling thread before the
    // first record is returned), so synthesizing here covers every partition
    // the rearm below then gates. A no-op unless `topic_recreated`.
    let synthesized = frontier.synthesize_recreated(drive_events.assigned.iter().copied());
    if synthesized > 0 {
        tracing::warn!(
            topic,
            synthesized,
            "topic recreation detected (Codex R18 C1/C2): synthesized floor-low \
             resume entries for assigned partitions with NO durable frontier \
             evidence — their committed offsets survive the delete+recreate but \
             name the DEAD generation's offset space, so each partition is gated \
             until its seek to the retained log's LOW watermark lands \
             (re-consuming the retained log is bounded at-least-once \
             duplication, never loss)",
        );
    }
    let _ = rearm_gate_after_rebalance(
        &mut gate,
        &frontier.partitions,
        &BTreeMap::new(),
        &drive_events,
    );
    if assignment_seen {
        try_activate_pending(consumer, topic, &mut gate, &frontier.partitions, contiguous);
    }
    gate
}

/// Commit the CONTIGUOUS frontier for a just-published durable segment
/// (compat-3 durability): the segment's raw covered spans are folded into the
/// [`ContiguousFrontier`], and only the per-partition offsets that remain
/// contiguous from the consume base are committed — never past a gap left by a
/// failed publish (C1). A no-op when the sink is not durable (`commits_offsets
/// == false`, C3) or when nothing advanced (a gap pins every partition).
#[cfg(feature = "kafka-io")]
fn commit_contiguous(
    commits_offsets: bool,
    frontier: &mut ContiguousFrontier,
    consumer: &KafkaStreamConsumer,
    topic: &str,
    spans: &PartitionOffsets,
) {
    if !commits_offsets {
        return;
    }
    let committable = frontier.record_durable(spans);
    commit_offsets(consumer, topic, &committable);
}

/// Manual-commit per-partition `next` offsets to Kafka (compat-3 durability).
/// The consumer runs `enable.auto.commit=false`, so this is the ONLY offset
/// advance — and it commits only the CONTIGUOUS frontier
/// ([`ContiguousFrontier`]), so a failed publish's gap is never committed
/// over (C1). `CommitMode::Async` keeps the poll loop non-blocking; a commit
/// that never reaches the broker (e.g. a hard kill) is normally covered by the
/// durable resume reconciliation on restart (the segment's `kafkaOffsets`
/// metadata frontier advances the consumer past the reloaded segment), so it
/// only costs bounded duplication.
///
/// EXCEPTION (Codex R25 H1, documented degraded-mode residual): when the
/// segment was published while the broker CLUSTER identity was UNRESOLVED
/// (pre-2.8 broker with no KIP-78 id, or a transient metadata-fetch failure),
/// the R4 H4 invariant OMITS `kafkaOffsets` from that durable row — an
/// identity-less offset must never seed a resume frontier (flooring it would
/// re-consume the whole retained log every restart, R8 H2; advancing it could
/// skip a repointed cluster's distinct records, R4 H4). Such a segment then
/// has NO metadata frontier to cover a lost async commit, so if THIS commit is
/// also lost to a hard kill the reloaded segment's records are re-consumed and
/// double-counted on restart. This is the inherent price of durability without
/// a cluster identity; it never affects a resolvable (PLAINTEXT / 2.8+) broker,
/// where `kafkaOffsets` are stamped and the metadata frontier covers the loss.
/// Resolving the broker's cluster id (KIP-78) restores full coverage.
#[cfg(feature = "kafka-io")]
fn commit_offsets(consumer: &KafkaStreamConsumer, topic: &str, offsets: &BTreeMap<i32, i64>) {
    use rdkafka::consumer::{CommitMode, Consumer as _};
    use rdkafka::{Offset, TopicPartitionList};

    if offsets.is_empty() {
        return;
    }
    let mut tpl = TopicPartitionList::new();
    for (partition, next) in offsets {
        if let Err(e) = tpl.add_partition_offset(topic, *partition, Offset::Offset(*next)) {
            tracing::warn!(
                topic, partition, next, error = %e,
                "resume-offset commit: could not add a partition to the commit list \
                 (skipping the commit; records re-consumed on restart)",
            );
            return;
        }
    }
    if let Err(e) = consumer.commit(&tpl, CommitMode::Async) {
        tracing::warn!(
            topic, error = %e,
            "resume-offset commit failed (at-least-once: the affected records are \
             re-consumed on restart)",
        );
    }
}

/// Re-seek every ACTIVE in-session partition back to its durable-contiguous
/// frontier after a publish FAILURE (Codex R12 H2), then seek them there
/// immediately so the re-consume starts on the next poll rather than waiting
/// for the retry timer. See [`arm_publish_failure_reseek`] for why forward
/// progress past the gap must stop (unbounded double count) and why the
/// re-armed partitions are tainted. A failed seek keeps the partition gated +
/// paused and is retried by the `seek_retry_timer` (H6). A no-op when nothing
/// is active/in-session (e.g. a roll failure before anything was consumed).
#[cfg(feature = "kafka-io")]
fn reseek_active_after_publish_failure(
    consumer: &KafkaStreamConsumer,
    topic: &str,
    gate: &mut ResumeSeekGate,
    resume_frontier: &BTreeMap<i32, PartitionResume>,
    contiguous: &mut ContiguousFrontier,
) {
    let targets = arm_publish_failure_reseek(gate, contiguous);
    if targets.is_empty() {
        return;
    }
    tracing::warn!(
        topic,
        partitions = ?targets.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
        "publish failed: re-seeking the affected partitions back to their \
         durable-contiguous frontier so the DROPPED rows are re-consumed and \
         re-published contiguously — never consume forward past an \
         uncommittable gap (Codex R12 H2: that piles up durable segments the \
         commit frontier can never advance past, re-published every restart)",
    );
    // Seek the re-armed partitions back NOW (rather than waiting for the retry
    // timer) so the re-consume begins on the next poll; a failed seek stays
    // gated + paused and is retried on the timer (H6).
    try_activate_pending(consumer, topic, gate, resume_frontier, contiguous);
}

/// Mid-stream (row-threshold / flush-timer) flush used by the poll loop
/// (Codex R12 H2): roll + publish the buffer, and on the outcome —
///
/// * `Ok(Some)` — commit the covered contiguous frontier ([`commit_contiguous`]);
/// * `Ok(None)` — an empty buffer, nothing to do;
/// * `Err` — record the failure STICKILY in `mid_stream_flush_failed` (Codex
///   R27 F2, so a later clean drain cannot launder the loss) AND re-seek the
///   affected partitions back to their durable frontier
///   ([`reseek_active_after_publish_failure`], Codex R12 H2) so the dropped
///   batch is re-consumed instead of the loop consuming forward past the gap.
#[cfg(feature = "kafka-io")]
#[allow(clippy::too_many_arguments)]
async fn flush_and_commit_mid_stream<S: SegmentSink>(
    sink: &S,
    consumer: &KafkaStreamConsumer,
    topic: &str,
    buffer: &mut StreamingBuffer,
    commits_offsets: bool,
    contiguous: &mut ContiguousFrontier,
    gate: &mut ResumeSeekGate,
    resume_frontier: &BTreeMap<i32, PartitionResume>,
    mid_stream_flush_failed: &mut bool,
) {
    match publish_rolled(sink, topic, buffer).await {
        Ok(Some(offsets)) => {
            commit_contiguous(commits_offsets, contiguous, consumer, topic, &offsets);
        }
        Ok(None) => {}
        Err(_) => {
            *mid_stream_flush_failed = true;
            reseek_active_after_publish_failure(consumer, topic, gate, resume_frontier, contiguous);
        }
    }
}

/// Mid-stream (row-threshold / flush-timer) wrapper around
/// [`publish_rolled`]: a failure is recorded STICKILY in
/// `mid_stream_flush_failed` — set on `Err`, NEVER cleared by later
/// successes (Codex R27 F2).
///
/// Mid-stream failures are deliberately non-fatal for the poll loop
/// (offsets are never committed, so the dropped rows are re-consumed on
/// restart), but they must survive into [`StreamingStats`]: the rows are
/// only actually rebuilt if a replay HAPPENS, and the decision that allows
/// a tombstone/suspend — which forecloses every replay — is taken from the
/// stats at shutdown. Pre-R27, a mid-stream failure died in a log line, a
/// subsequent (typically EMPTY) final drain succeeded, and the supervisor
/// was recorded as cleanly stopped: nothing ever redelivered the dropped
/// rows.
///
/// Available without the `kafka-io` feature so the stickiness is
/// unit-testable without a broker.
///
/// Returns the per-partition offset spans of the segment it published
/// (`Some` on a successful non-empty roll, `None` on an empty no-op) so the
/// poll loop can manual-commit them to Kafka (compat-3 stage 2); a FAILURE
/// returns `None` and commits nothing (the offsets stay un-committed and the
/// records are re-consumed on restart — the #9 invariant).
pub async fn publish_rolled_mid_stream<S: SegmentSink>(
    sink: &S,
    topic: &str,
    buffer: &mut StreamingBuffer,
    mid_stream_flush_failed: &mut bool,
) -> Option<PartitionOffsets> {
    match publish_rolled(sink, topic, buffer).await {
        Ok(offsets) => offsets,
        Err(_) => {
            *mid_stream_flush_failed = true;
            None
        }
    }
}

/// Roll the buffer (if non-empty) and publish it through the sink,
/// RETURNING the outcome (Codex R26 F2 — previously fire-and-forget).
///
/// The consumer loop's mid-stream call sites deliberately treat an `Err`
/// as NON-FATAL for the loop (both failure modes are logged here and the
/// affected rows are dropped from memory): killing the whole persisted
/// supervisor on one bad batch would silently stop all future ingestion,
/// and because offsets are never committed the dropped rows are re-consumed
/// from Kafka on restart. The failure is still RECORDED stickily in
/// [`StreamingStats::mid_stream_flush_failed`] via
/// [`publish_rolled_mid_stream`] (Codex R27 F2), so a later
/// tombstone/suspend — which would foreclose that replay — fails instead.
/// The FINAL drain on shutdown propagates the `Err` into
/// [`StreamingStats::final_flush_failed`] the same way (Codex R26 F2).
///
/// A roll failure (a batch that cannot be built into a segment) is
/// reported through the same error type: either way the buffered rows did
/// not reach the sink. An empty buffer is a successful no-op. Does not
/// commit offsets (see [`run_streaming_consumer`]).
///
/// # Errors
///
/// The sink's publish error, or a wrapped roll error; in both cases the
/// rolled rows were dropped from memory and only a Kafka replay (restart /
/// earliest re-create) can rebuild them.
///
/// Available without the `kafka-io` feature so the failure propagation is
/// unit-testable without a broker.
pub async fn publish_rolled<S: SegmentSink>(
    sink: &S,
    topic: &str,
    buffer: &mut StreamingBuffer,
) -> Result<Option<PartitionOffsets>, SegmentSinkError> {
    let (segment, offsets) = match buffer.roll_with_offsets() {
        Ok(Some(rolled)) => rolled,
        Ok(None) => return Ok(None),
        Err(e) => {
            tracing::error!(
                topic = %topic, error = %e,
                "failed to roll Kafka batch into a segment; dropping batch (re-consumed on restart)",
            );
            return Err(SegmentSinkError(format!("roll failed before publish: {e}")));
        }
    };
    let num_rows = segment.num_rows;
    match sink.publish_with_offsets(segment, &offsets).await {
        Ok(()) => {
            tracing::info!(topic = %topic, num_rows, "published Kafka segment");
            // The rolled segment is durable (persist → metadata succeeded);
            // returning its covered offsets lets the poll loop manual-commit
            // them to Kafka (compat-3 stage 2). A publish FAILURE returns
            // `Err` and commits NOTHING, so those records are re-consumed on
            // restart (at-least-once — the #9 invariant).
            Ok(Some(offsets))
        }
        Err(e) => {
            tracing::error!(
                topic = %topic, num_rows, error = %e,
                "segment publish failed; dropping batch (re-consumed on restart)",
            );
            Err(e)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn config(max_rows: usize) -> KafkaConsumerConfig {
        KafkaConsumerConfig {
            brokers: "localhost:9092".to_string(),
            topic: "t".to_string(),
            group_id: "g".to_string(),
            data_source: "events".to_string(),
            timestamp_column: "__time".to_string(),
            timestamp_format: TsFormat::Auto,
            dim_schemas: vec![DimensionSchema::string("page")],
            metrics_specs: vec![],
            max_rows_per_segment: max_rows,
            segment_flush_interval_ms: 10_000,
            use_earliest_offset: true,
            additional_properties: std::collections::HashMap::new(),
            output_dir: None,
        }
    }

    fn payload(ts: i64, page: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "__time": ts, "page": page })).expect("json")
    }

    #[test]
    fn buffer_rolls_at_threshold() {
        let mut buf = StreamingBuffer::from_config(&config(3));
        assert!(!buf.push_payload(&payload(1, "a")).expect("push"));
        assert!(!buf.push_payload(&payload(2, "b")).expect("push"));
        // Third row hits the threshold.
        assert!(buf.push_payload(&payload(3, "c")).expect("push"));
        assert_eq!(buf.pending_rows(), 3);

        let seg = buf.roll().expect("roll").expect("segment");
        assert_eq!(seg.num_rows, 3);
        assert_eq!(seg.data_source, "events");
        assert!(buf.is_empty());
        assert_eq!(buf.total_consumed(), 3);
        assert_eq!(buf.total_published(), 1);
    }

    #[test]
    fn roll_empty_is_none() {
        let mut buf = StreamingBuffer::from_config(&config(10));
        assert!(buf.roll().expect("roll").is_none());
        assert_eq!(buf.total_published(), 0);
    }

    #[test]
    fn time_based_partial_flush_rolls_all_buffered_rows() {
        // Fewer rows than the count threshold still roll on demand — this
        // is the path the wall-clock flush timer drives in the real loop.
        let mut buf = StreamingBuffer::from_config(&config(1_000));
        for i in 0..4 {
            assert!(!buf.push_payload(&payload(1_000 + i, "x")).expect("push"));
        }
        let seg = buf.roll().expect("roll").expect("segment");
        assert_eq!(seg.num_rows, 4);
        assert!(buf.is_empty());
    }

    #[test]
    fn malformed_payload_is_rejected_not_buffered() {
        let mut buf = StreamingBuffer::from_config(&config(10));
        let err = buf.push_payload(b"not json").expect_err("must reject");
        assert!(matches!(err, ConsumerError::ParseError(_)));
        assert!(buf.is_empty());
        assert_eq!(buf.total_consumed(), 0);
    }

    #[test]
    fn oversized_payload_is_rejected_before_parse() {
        let mut buf = StreamingBuffer::from_config(&config(10));
        let mut big = Vec::new();
        big.extend_from_slice(br#"{"k":""#);
        big.resize(crate::consumer::MAX_RECORD_PAYLOAD_BYTES + 32, b'a');
        big.extend_from_slice(br#""}"#);
        let err = buf.push_payload(&big).expect_err("must reject");
        assert!(matches!(err, ConsumerError::PayloadTooLarge { .. }));
        assert!(buf.is_empty());
    }

    #[test]
    fn record_missing_timestamp_is_rejected_individually() {
        // A syntactically valid JSON object without the timestamp column
        // must be rejected at push time (dead-lettered) so it never reaches
        // roll() — where a missing timestamp fails the WHOLE segment build
        // and previously killed the supervisor.
        let mut buf = StreamingBuffer::from_config(&config(10));
        let no_ts = serde_json::to_vec(&serde_json::json!({ "page": "a" })).expect("json");
        let err = buf
            .push_payload(&no_ts)
            .expect_err("must reject missing timestamp");
        assert!(matches!(err, ConsumerError::ParseError(_)));
        assert!(buf.is_empty());

        // Good rows around it still buffer and roll cleanly.
        assert!(!buf.push_payload(&payload(1, "a")).expect("push good"));
        assert!(!buf.push_payload(&payload(2, "b")).expect("push good"));
        let seg = buf.roll().expect("roll").expect("segment");
        assert_eq!(seg.num_rows, 2);
    }

    #[test]
    fn unparseable_timestamp_is_rejected_at_push_not_at_roll() {
        // A present-but-unparseable timestamp (non-ISO string) must be
        // caught at push time — matching the ingester's own timestamp
        // parsing — so it can never poison the batch roll.
        let mut buf = StreamingBuffer::from_config(&config(10));
        let bad = serde_json::to_vec(&serde_json::json!({ "__time": "not-a-date", "page": "a" }))
            .expect("json");
        let err = buf
            .push_payload(&bad)
            .expect_err("must reject bad timestamp");
        assert!(matches!(err, ConsumerError::ParseError(_)));
        assert!(buf.is_empty());

        // And a good batch around it still rolls cleanly.
        assert!(!buf.push_payload(&payload(1, "a")).expect("push"));
        let seg = buf.roll().expect("roll").expect("segment");
        assert_eq!(seg.num_rows, 1);
    }

    #[test]
    fn record_with_timezoneless_iso_timestamp_is_accepted() {
        // A timezone-less ISO-8601 `__time` (`2023-11-14T22:13:20`, and the
        // date-only form) is a valid Druid `auto`/`iso` timestamp. Before the
        // Codex-R9 fix the shared `extract_row_timestamp_millis` used chrono's
        // offset-REQUIRING `DateTime<Utc>` parser, so every such record was
        // dead-lettered at push time and dropped. It must now buffer and roll.
        let mut buf = StreamingBuffer::from_config(&config(10));
        let tz_less = serde_json::to_vec(
            &serde_json::json!({ "__time": "2023-11-14T22:13:20", "page": "a" }),
        )
        .expect("json");
        assert!(
            !buf.push_payload(&tz_less)
                .expect("tz-less datetime buffers")
        );
        let date_only =
            serde_json::to_vec(&serde_json::json!({ "__time": "2023-11-14", "page": "b" }))
                .expect("json");
        assert!(!buf.push_payload(&date_only).expect("date-only buffers"));
        // Druid `auto` also accepts a SPACE separator instead of 'T' (R11).
        let space_sep = serde_json::to_vec(
            &serde_json::json!({ "__time": "2023-11-14 22:13:20", "page": "c" }),
        )
        .expect("json");
        assert!(
            !buf.push_payload(&space_sep)
                .expect("space-separated buffers")
        );
        let seg = buf.roll().expect("roll").expect("segment");
        assert_eq!(seg.num_rows, 3);
    }

    #[test]
    fn record_with_leap_second_timestamp_is_dead_lettered() {
        // A `:60` leap second is rejected by Druid but ACCEPTED (and silently
        // shifted to the next minute) by chrono. It must be dead-lettered at
        // push time so a wrong-instant record never reaches roll()/publish
        // (Codex R10).
        let mut buf = StreamingBuffer::from_config(&config(10));
        let leap = serde_json::to_vec(
            &serde_json::json!({ "__time": "2023-11-14T22:13:60.500", "page": "a" }),
        )
        .expect("json");
        let err = buf
            .push_payload(&leap)
            .expect_err("leap second must be rejected");
        assert!(matches!(err, ConsumerError::ParseError(_)));
        assert!(buf.is_empty());
    }

    #[test]
    fn record_with_named_timezone_timestamp_is_accepted() {
        // Druid `auto` accepts a trailing RECOGNISED named zone (`… PST`),
        // which it strips and reads as UTC (no shift); it must buffer, while an
        // unrecognised zone dead-letters rather than being guessed into a wrong
        // instant (Codex R12; no-shift semantics corrected via the R14 oracle).
        let mut buf = StreamingBuffer::from_config(&config(10));
        let pst = serde_json::to_vec(
            &serde_json::json!({ "__time": "2009-02-13 23:31:30 PST", "page": "a" }),
        )
        .expect("json");
        assert!(!buf.push_payload(&pst).expect("named-zone buffers"));
        let bad_zone = serde_json::to_vec(
            &serde_json::json!({ "__time": "2009-02-13 23:31:30 XYZ", "page": "b" }),
        )
        .expect("json");
        assert!(
            buf.push_payload(&bad_zone).is_err(),
            "unknown zone must reject"
        );
        let seg = buf.roll().expect("roll").expect("segment");
        assert_eq!(seg.num_rows, 1);
    }

    #[test]
    fn declared_format_is_threaded_to_validation_and_roll() {
        // Fable audit: with a declared format:"iso", "2023" is the ISO YEAR
        // 2023 in Druid — the un-threaded parser stored 2023 ms (a wrong
        // instant). The format must govern BOTH push-time validation and
        // roll-time extraction.
        let mut cfg = config(10);
        cfg.timestamp_format = TsFormat::Iso;
        let mut buf = StreamingBuffer::from_config(&cfg);
        let rec = serde_json::to_vec(&serde_json::json!({ "__time": "2023", "page": "a" }))
            .expect("json");
        assert!(!buf.push_payload(&rec).expect("iso year buffers"));
        let seg = buf.roll().expect("roll").expect("segment");
        assert_eq!(
            seg.segment_data.timestamp_column().expect("ts"),
            &[1_672_531_200_000_i64],
            "iso '2023' must be stored as the YEAR 2023, not 2023 ms"
        );

        // And a declared millis format dead-letters ISO text at push time.
        let mut cfg = config(10);
        cfg.timestamp_format = TsFormat::Millis;
        let mut buf = StreamingBuffer::from_config(&cfg);
        let bad = serde_json::to_vec(&serde_json::json!({ "__time": "2023-11-14", "page": "a" }))
            .expect("json");
        assert!(
            buf.push_payload(&bad).is_err(),
            "millis must reject ISO text"
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn record_with_multiple_json_objects_is_split_into_rows() {
        // Druid's json inputFormat reads every JSON object in a record as its
        // own row; a multi-object payload must produce multiple rows, not be
        // discarded as "trailing characters" (Codex R13).
        let mut buf = StreamingBuffer::from_config(&config(10));
        let two = b"{\"__time\":1,\"page\":\"a\"}\n{\"__time\":2,\"page\":\"b\"}";
        assert!(!buf.push_payload(two).expect("two-object record buffers"));
        assert_eq!(buf.pending_rows(), 2);
        assert_eq!(buf.total_consumed(), 2);
        // Concatenated (no separator) also works.
        let concat = b"{\"__time\":3,\"page\":\"c\"}{\"__time\":4,\"page\":\"d\"}";
        assert!(
            !buf.push_payload(concat)
                .expect("concatenated record buffers")
        );
        assert_eq!(buf.pending_rows(), 4);
        let seg = buf.roll().expect("roll").expect("segment");
        assert_eq!(seg.num_rows, 4);
    }

    #[test]
    fn multi_object_record_dead_letters_atomically_on_one_bad_object() {
        // If ANY object in a multi-object record is invalid, the WHOLE record
        // is dead-lettered — no partial prefix is buffered (Codex R13).
        let mut buf = StreamingBuffer::from_config(&config(10));
        // Second object lacks the timestamp column.
        let mixed = b"{\"__time\":1,\"page\":\"a\"}\n{\"page\":\"b\"}";
        assert!(
            buf.push_payload(mixed).is_err(),
            "one bad object rejects all"
        );
        assert!(buf.is_empty(), "no partial prefix buffered");
        assert_eq!(buf.total_consumed(), 0);
    }

    #[test]
    fn long_dim_null_plus_over_2p53_record_is_dead_lettered_not_whole_batch() {
        // A LONG column that mixes a NULL with an out-of-±2^53 value cannot be
        // stored exactly and would fail the WHOLE roll (losing every buffered
        // row). The conflicting record must be dead-lettered while the batch
        // stays ingestable (Codex R14).
        let mut cfg = config(100);
        cfg.dim_schemas = vec![DimensionSchema::new("id", DimensionType::Long)];
        let mut buf = StreamingBuffer::from_config(&cfg);
        let rec = |v: serde_json::Value| {
            serde_json::to_vec(&serde_json::json!({ "__time": 1, "id": v })).expect("json")
        };
        let over = 9_007_199_254_740_993_i64; // 2^53 + 1

        // A NULL id buffers.
        assert!(
            !buf.push_payload(&rec(serde_json::Value::Null))
                .expect("null buffers")
        );
        // An out-of-range id in the SAME segment conflicts → dead-lettered.
        let err = buf
            .push_payload(&rec(serde_json::json!(over)))
            .expect_err("null+over conflict rejected");
        assert!(matches!(err, ConsumerError::ParseError(_)));
        assert_eq!(buf.pending_rows(), 1, "batch preserved (null row kept)");
        // An in-range id still buffers alongside the null.
        assert!(
            !buf.push_payload(&rec(serde_json::json!(42)))
                .expect("in-range buffers")
        );
        let seg = buf.roll().expect("roll").expect("segment");
        assert_eq!(seg.num_rows, 2);
        // A FRESH segment can take the out-of-range value (no null yet).
        assert!(
            !buf.push_payload(&rec(serde_json::json!(over)))
                .expect("over-range in a fresh segment")
        );
        assert_eq!(buf.pending_rows(), 1);
    }

    #[test]
    fn non_object_record_is_rejected() {
        let mut buf = StreamingBuffer::from_config(&config(10));
        let arr = b"[1,2,3]";
        let err = buf.push_payload(arr).expect_err("array is not a record");
        assert!(matches!(err, ConsumerError::ParseError(_)));
        assert!(buf.is_empty());
    }

    #[test]
    fn typed_dimension_is_preserved_through_roll() {
        use ferrodruid_ingest_batch::DimensionType;
        let mut cfg = config(10);
        cfg.dim_schemas = vec![DimensionSchema::new("count", DimensionType::Long)];
        let mut buf = StreamingBuffer::from_config(&cfg);
        for i in 0..3 {
            let p = serde_json::to_vec(&serde_json::json!({ "__time": 1_000 + i, "count": i }))
                .expect("json");
            buf.push_payload(&p).expect("push");
        }
        // A `long`-typed dimension must NOT be silently stringified.
        let seg = buf.roll().expect("roll").expect("segment");
        assert_eq!(seg.num_rows, 3);
    }

    #[test]
    fn zero_threshold_clamped_to_one() {
        // Defensive clamp: a 0 `maxRowsPerSegment` (which validate()
        // normally rejects) must not panic or misbehave; it is clamped to
        // 1 so the first record rolls rather than triggering degenerate
        // behaviour.
        let mut buf = StreamingBuffer::from_config(&config(0));
        assert!(buf.push_payload(&payload(1, "a")).expect("push"));
    }

    // -----------------------------------------------------------------
    // publish_rolled failure propagation (Codex R26 F2)
    // -----------------------------------------------------------------

    /// A sink that always fails, modelling a broken publish tail at the
    /// worst moment: the final drain on shutdown.
    struct FailingSink;
    impl SegmentSink for FailingSink {
        async fn publish(&self, _segment: IngestedSegment) -> Result<(), SegmentSinkError> {
            Err(SegmentSinkError("injected sink failure".to_string()))
        }
    }

    /// A sink that fails the first `fail_first` publishes, then succeeds —
    /// models a transiently broken publish tail mid-stream (Codex R27 F2).
    struct FailNTimesSink {
        fail_first: usize,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl SegmentSink for FailNTimesSink {
        async fn publish(&self, _segment: IngestedSegment) -> Result<(), SegmentSinkError> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < self.fail_first {
                Err(SegmentSinkError("injected mid-stream failure".to_string()))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn mid_stream_publish_failure_is_sticky() {
        // Codex R27 F2: one failed mid-stream flush must keep
        // `mid_stream_flush_failed` set through ANY number of later
        // successful flushes — including the empty no-op flush the loop's
        // timer fires — so the shutdown-time stats still report the loss.
        // This drives `publish_rolled_mid_stream`, the exact function both
        // mid-stream call sites of `run_streaming_loop` use.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio rt");
        rt.block_on(async {
            let sink = FailNTimesSink {
                fail_first: 1,
                calls: std::sync::atomic::AtomicUsize::new(0),
            };
            let mut buf = StreamingBuffer::from_config(&config(10));
            let mut mid_stream_flush_failed = false;

            // First flush fails → flag set, rows dropped from the buffer.
            assert!(!buf.push_payload(&payload(1, "a")).expect("push"));
            publish_rolled_mid_stream(&sink, "t", &mut buf, &mut mid_stream_flush_failed).await;
            assert!(mid_stream_flush_failed, "a failed flush must set the flag");
            assert!(buf.is_empty(), "the failed batch is dropped from memory");

            // A later SUCCESSFUL non-empty flush must not clear it…
            assert!(!buf.push_payload(&payload(2, "b")).expect("push"));
            publish_rolled_mid_stream(&sink, "t", &mut buf, &mut mid_stream_flush_failed).await;
            assert!(
                mid_stream_flush_failed,
                "a later successful publish must NOT clear the sticky flag"
            );
            // …and neither must a successful EMPTY flush (the shape of the
            // final drain that pre-fix laundered the loss).
            assert!(buf.is_empty());
            publish_rolled_mid_stream(&sink, "t", &mut buf, &mut mid_stream_flush_failed).await;
            assert!(
                mid_stream_flush_failed,
                "an empty no-op flush must NOT clear the sticky flag"
            );

            // And the flag feeds the shutdown decision.
            let stats = StreamingStats {
                total_consumed: 2,
                total_published: 1,
                final_flush_failed: false,
                mid_stream_flush_failed,
                fatal_consume_error: false,
                cluster_id_drifted: false,
            };
            assert!(stats.replay_required());
        });
    }

    #[test]
    fn publish_rolled_propagates_sink_failure() {
        // Codex R26 F2: `publish_rolled` was fire-and-forget, so the FINAL
        // drain on shutdown could fail (residual valid rows never became
        // queryable) while the consumer still reported a clean stop — the
        // overlord then tombstoned the supervisor and nothing ever replayed
        // the lost rows. The failure must be RETURNED so the shutdown
        // branch can surface it in `StreamingStats::final_flush_failed`.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio rt");
        rt.block_on(async {
            let mut buf = StreamingBuffer::from_config(&config(10));
            assert!(!buf.push_payload(&payload(1, "a")).expect("push"));
            let err = publish_rolled(&FailingSink, "t", &mut buf)
                .await
                .expect_err("a failed publish of buffered rows must propagate");
            assert!(
                format!("{err}").contains("injected sink failure"),
                "err = {err}"
            );
            // An EMPTY buffer is a successful no-op even on a broken sink —
            // the normal-path shutdown (everything already flushed) must
            // keep succeeding.
            assert!(buf.is_empty());
            assert!(
                publish_rolled(&FailingSink, "t", &mut buf)
                    .await
                    .expect("empty flush is a no-op")
                    .is_none(),
                "an empty flush publishes nothing, so it returns no offsets to commit",
            );
        });
    }

    // -----------------------------------------------------------------
    // Offset tracking + commit-after-Ok (compat-3 stage 2)
    // -----------------------------------------------------------------

    /// A sink that always succeeds — models a healthy durable publish tail so
    /// the offset-return path (which the poll loop commits) is exercised.
    struct OkSink;
    impl SegmentSink for OkSink {
        async fn publish(&self, _segment: IngestedSegment) -> Result<(), SegmentSinkError> {
            Ok(())
        }
    }

    #[test]
    fn buffer_records_per_partition_offset_spans() {
        // record_offset extends each partition's [start, next) span; a roll
        // hands the spans to the segment and clears them for the next one.
        let mut buf = StreamingBuffer::from_config(&config(100));
        buf.record_offset(0, 10);
        assert!(!buf.push_payload(&payload(1, "a")).expect("push"));
        buf.record_offset(0, 11);
        assert!(!buf.push_payload(&payload(2, "b")).expect("push"));
        buf.record_offset(1, 4);
        assert!(!buf.push_payload(&payload(3, "c")).expect("push"));

        let (seg, offsets) = buf
            .roll_with_offsets()
            .expect("roll")
            .expect("segment + offsets");
        assert_eq!(seg.num_rows, 3);
        // Kafka's committed offset is the NEXT to consume = max + 1.
        assert_eq!(offsets.get(&0), Some(&OffsetSpan::new(10, 12)));
        assert_eq!(offsets.get(&1), Some(&OffsetSpan::new(4, 5)));

        // Spans reset after a roll.
        buf.record_offset(0, 20);
        assert!(!buf.push_payload(&payload(4, "d")).expect("push"));
        let (_seg2, offsets2) = buf.roll_with_offsets().expect("roll2").expect("segment2");
        assert_eq!(offsets2.get(&0), Some(&OffsetSpan::new(20, 21)));
        assert!(
            !offsets2.contains_key(&1),
            "partition 1 span did not carry over"
        );
    }

    #[test]
    fn record_offset_saturates_at_i64_max_without_overflow() {
        // A record at the maximum protocol-valid signed offset must not overflow
        // (Codex R11): a raw offset+1 panics in debug and wraps to i64::MIN in
        // release, persisting an uncommittable [i64::MAX, i64::MIN) span that is
        // re-published every restart. saturating_add(1) leaves next at i64::MAX.
        let mut buf = StreamingBuffer::from_config(&config(100));
        buf.record_offset(0, i64::MAX);
        assert!(!buf.push_payload(&payload(1, "a")).expect("push"));
        let (_seg, offsets) = buf
            .roll_with_offsets()
            .expect("roll")
            .expect("segment + offsets");
        assert_eq!(
            offsets.get(&0),
            Some(&OffsetSpan::new(i64::MAX, i64::MAX)),
            "next must saturate at i64::MAX, never wrap to i64::MIN"
        );
    }

    #[test]
    fn empty_roll_retains_offset_spans_of_dead_letters() {
        // A dead-lettered record's offset is still recorded (its message was
        // consumed); if no rows are buffered the roll is a no-op but the span
        // is RETAINED so a later non-empty roll covers it (commit past
        // unparseable, like Druid).
        let mut buf = StreamingBuffer::from_config(&config(100));
        buf.record_offset(0, 7); // a message that dead-lettered (no push)
        assert!(buf.roll_with_offsets().expect("roll").is_none());
        // Now a good record at a later offset.
        buf.record_offset(0, 8);
        assert!(!buf.push_payload(&payload(1, "a")).expect("push"));
        let (_seg, offsets) = buf.roll_with_offsets().expect("roll").expect("segment");
        // The span spans the dead-letter (7) through the good record (8).
        assert_eq!(offsets.get(&0), Some(&OffsetSpan::new(7, 9)));
    }

    #[test]
    fn publish_rolled_returns_offsets_on_ok_and_none_on_failure() {
        // The #9 invariant (compat-3 stage 2): a SUCCESSFUL publish returns
        // the covered offsets so the poll loop can commit them; a FAILED
        // publish returns Err and NO offsets — nothing is committed, so the
        // records are re-consumed on restart (at-least-once, no loss).
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio rt");
        rt.block_on(async {
            let mut buf = StreamingBuffer::from_config(&config(100));
            buf.record_offset(0, 41);
            assert!(!buf.push_payload(&payload(1, "a")).expect("push"));
            let offsets = publish_rolled(&OkSink, "t", &mut buf)
                .await
                .expect("ok publish")
                .expect("a successful publish returns its offsets to commit");
            assert_eq!(offsets.get(&0), Some(&OffsetSpan::new(41, 42)));

            // A failing publish returns Err (→ the loop commits nothing).
            buf.record_offset(0, 42);
            assert!(!buf.push_payload(&payload(2, "b")).expect("push"));
            publish_rolled(&FailingSink, "t", &mut buf)
                .await
                .expect_err("a failed publish returns Err, not committable offsets");

            // publish_rolled_mid_stream mirrors this: Some on Ok, None + sticky
            // flag on failure.
            let mut sticky = false;
            buf.record_offset(0, 43);
            assert!(!buf.push_payload(&payload(3, "c")).expect("push"));
            let mid = publish_rolled_mid_stream(&OkSink, "t", &mut buf, &mut sticky).await;
            assert_eq!(
                mid.and_then(|o| o.get(&0).cloned()),
                Some(OffsetSpan::new(43, 44))
            );
            assert!(
                !sticky,
                "a successful mid-stream flush leaves the flag clear"
            );

            buf.record_offset(0, 44);
            assert!(!buf.push_payload(&payload(4, "d")).expect("push"));
            let mid_fail =
                publish_rolled_mid_stream(&FailingSink, "t", &mut buf, &mut sticky).await;
            assert!(
                mid_fail.is_none(),
                "a failed mid-stream flush returns no committable offsets"
            );
            assert!(sticky, "and sets the sticky failure flag");
        });
    }

    // -----------------------------------------------------------------
    // compat-3 durability: contiguous commit (C1) + resume walk (C1/C2/H6)
    // -----------------------------------------------------------------

    fn spans(pairs: &[(i32, i64, i64)]) -> PartitionOffsets {
        pairs
            .iter()
            .map(|&(p, s, n)| (p, OffsetSpan::new(s, n)))
            .collect()
    }

    #[test]
    fn contiguous_frontier_does_not_commit_past_a_failed_roll_gap() {
        // C1: base 0; the [0,100) roll FAILS (never durable, so record_durable
        // is never called for it) and the [100,200) roll succeeds. Committing
        // 200 would let a restart skip 0-99 (loss). The contiguous frontier
        // stays pinned at the gap: the successful [100,200) publish commits
        // NOTHING, so on restart the committed offset never advanced past 0.
        let mut f = ContiguousFrontier::new();
        f.note_consumed(0, 0); // first consumed offset = base
        // ... consume through 199 (the failed roll's messages were consumed) ...
        f.note_consumed(0, 100);
        // The failed roll [0,100) is dropped: no record_durable. The next
        // successful publish covers [100,200):
        let committable = f.record_durable(&spans(&[(0, 100, 200)]));
        assert!(
            !committable.contains_key(&0),
            "a publish that starts ABOVE the base (a prior roll failed) must NOT \
             commit past the gap"
        );

        // The normal (contiguous) case DOES advance and commit.
        let mut f2 = ContiguousFrontier::new();
        f2.note_consumed(0, 0);
        assert_eq!(
            f2.record_durable(&spans(&[(0, 0, 100)])).get(&0),
            Some(&100)
        );
        assert_eq!(
            f2.record_durable(&spans(&[(0, 100, 200)])).get(&0),
            Some(&200),
            "contiguous publishes advance the committed frontier with no gap"
        );
    }

    #[test]
    fn contiguous_frontier_tracks_new_partition_base() {
        // H4 (commit side): a brand-new partition that joins mid-stream and is
        // NOT in the resume frontier is committed contiguously from its OWN
        // first-consumed base, never from 0 or from a stale assumption.
        let mut f = ContiguousFrontier::new();
        f.note_consumed(7, 500); // new partition's base is 500
        // Its first durable roll [500,600) is contiguous from the base 500.
        assert_eq!(
            f.record_durable(&spans(&[(7, 500, 600)])).get(&7),
            Some(&600)
        );
        // A hole for the new partition pins it too.
        let mut f2 = ContiguousFrontier::new();
        f2.note_consumed(7, 500);
        assert!(
            !f2.record_durable(&spans(&[(7, 550, 600)])).contains_key(&7),
            "a first publish above the new partition's base is a gap"
        );
    }

    #[test]
    fn resume_offset_reconciles_committed_and_durable() {
        // C1 FIX: no committed offset but durable rows EXIST → seek the durable
        // frontier authoritatively (NOT auto.offset.reset). loaded [100,200),
        // min_start=100 → resume at 200 (skip the already-durable span, and
        // re-consume any post-frontier crash residue; no loss, no dup).
        let r = PartitionResume::new(100, vec![OffsetSpan::new(100, 200)]);
        assert_eq!(
            resume_offset(None, &r),
            200,
            "no commit + durable rows → seek the durable frontier (C1), never \
             auto.offset.reset"
        );

        // C1 restart with a committed base BELOW the durable rows: a hole
        // between the committed base and the durable coverage stops the walk.
        // committed=50, durable [100,200) loaded → resume from 50 (re-consume
        // the failed [50,100), then the durable [100,200) is re-consumed = dup).
        let r_c1 = PartitionResume::new(100, vec![OffsetSpan::new(100, 200)]);
        assert_eq!(resume_offset(Some(50), &r_c1), 50);

        // C2 phantom: committed=200 (stale-high), min_start=0 (phantom row),
        // loaded [100,200) → the hole at [0,100) stops the walk at 0.
        let r_c2 = PartitionResume::new(0, vec![OffsetSpan::new(100, 200)]);
        assert_eq!(resume_offset(Some(200), &r_c2), 0);

        // H6: fully durable + loaded [0,200), committed lagged at 150 → skip the
        // already-durable [150,200), resume at 200 (no double count).
        let r_h6 = PartitionResume::new(0, vec![OffsetSpan::new(0, 200)]);
        assert_eq!(resume_offset(Some(150), &r_h6), 200);

        // Normal: committed already at the durable end → no re-consumption.
        assert_eq!(resume_offset(Some(200), &r_h6), 200);

        // Merged, out-of-order, contiguous spans still walk correctly.
        let r_merge =
            PartitionResume::new(0, vec![OffsetSpan::new(100, 200), OffsetSpan::new(0, 100)]);
        assert_eq!(resume_offset(Some(0), &r_merge), 200);

        // A genuine hole inside the durable coverage stops the walk.
        let r_hole =
            PartitionResume::new(0, vec![OffsetSpan::new(0, 100), OffsetSpan::new(200, 300)]);
        assert_eq!(resume_offset(Some(0), &r_hole), 100);
    }

    #[test]
    fn resume_offset_committed_none_never_falls_back_to_reset() {
        // C1: durable rows exist but the committed offset is GONE (expired
        // retention, or an async commit that never landed). The resume MUST
        // derive from the durable rows, not auto.offset.reset:
        //   * `latest` would skip post-frontier crash residue (loss);
        //   * `earliest` would replay the whole loaded span (dup).
        // Fully-loaded [0,500) contiguous → frontier 500 (skip durable rows,
        // re-consume anything after them).
        let full = PartitionResume::new(0, vec![OffsetSpan::new(0, 500)]);
        assert_eq!(resume_offset(None, &full), 500);

        // A hole at [200,300) (phantom) with the commit gone → stop at the
        // first gap (200) and re-consume from there, rather than skipping past
        // it (no loss).
        let holed =
            PartitionResume::new(0, vec![OffsetSpan::new(0, 200), OffsetSpan::new(300, 400)]);
        assert_eq!(resume_offset(None, &holed), 200);
    }

    #[test]
    fn resume_seek_failure_unsafe_in_both_directions() {
        // C2 FIX: a failed authoritative seek is safe to ignore ONLY when the
        // consumer is already at the durable frontier (committed == target).
        assert!(
            resume_seek_failure_is_safe(Some(200), 200),
            "already-at-frontier seek-failure is a harmless no-op"
        );
        // Backward re-consume (target < committed): staying at committed would
        // SKIP the phantom/gap span it had to re-consume = loss (C2).
        assert!(
            !resume_seek_failure_is_safe(Some(200), 0),
            "backward re-consume seek-failure is UNSAFE (loss, C2)"
        );
        // Forward skip (target > committed): replaying from committed
        // double-counts already-durable rows (H6).
        assert!(
            !resume_seek_failure_is_safe(Some(150), 200),
            "forward skip seek-failure is UNSAFE (double count, H6)"
        );
        // No committed offset: auto.offset.reset would govern a durable
        // partition — skip (latest) or replay (earliest) the whole span (C1).
        assert!(
            !resume_seek_failure_is_safe(None, 200),
            "no-commit seek-failure is UNSAFE (auto.offset.reset skip/replay, C1)"
        );
    }

    #[test]
    fn non_committing_sink_default() {
        // C3: the default sink does NOT commit offsets (memory-only). A durable
        // sink must opt in. `OkSink` is a memory-only test sink.
        assert!(!OkSink.commits_offsets());
    }

    // -----------------------------------------------------------------
    // ResumeSeekGate — the per-partition seek-enforcement state machine
    // (compat-3 durability, Codex R3 C2/C3/C4/H6)
    // -----------------------------------------------------------------

    #[test]
    fn gate_blocks_consumption_until_activated() {
        // Codex R3 C2: a partition with a durable frontier must NOT be
        // consumed until its authoritative seek SUCCEEDED — an assignment
        // timeout (or any other failure to seek) must gate consumption, never
        // fall through to `auto.offset.reset` consumption.
        let mut gate = ResumeSeekGate::new();
        gate.require_target(0);
        assert!(
            !gate.record_delivered(0),
            "a frontier partition without a resolved target must be suppressed (C2)"
        );
        // Resolving the target alone is not enough — the seek must succeed.
        gate.set_target(0, 500);
        assert!(
            !gate.record_delivered(0),
            "a frontier partition with an un-applied seek must stay suppressed (C2)"
        );
        assert_eq!(gate.pending(), vec![0]);
        assert!(!gate.is_clear());
        // Only a successful seek activates consumption.
        assert!(gate.mark_active(0), "unpaused partition activates");
        assert!(gate.record_delivered(0), "an activated partition consumes");
        assert!(gate.is_clear());
        // Partitions the gate never gated (no durable frontier) are Active by
        // default — first-start semantics are unchanged.
        assert!(gate.record_delivered(7));
    }

    #[test]
    fn gate_seek_and_pause_failure_still_suppresses() {
        // Codex R3 C3: when BOTH the seek and the safety pause fail (e.g. mid
        // rebalance), the poll loop must still not consume the partition —
        // the gate (not the broker-side pause) is the enforcement.
        let mut gate = ResumeSeekGate::new();
        gate.set_target(3, 200);
        // Seek failed, pause failed → NOT recorded as paused; still suppressed.
        assert!(!gate.is_paused(3));
        assert!(
            !gate.record_delivered(3),
            "seek+pause both failed: records must be dropped, not consumed (C3)"
        );
        assert_eq!(
            gate.pending(),
            vec![3],
            "the partition stays pending for retry"
        );
    }

    #[test]
    fn gate_enforces_rewind_of_join_drive_records() {
        // Codex R3 C4: a record delivered-and-discarded during the join drive
        // for a NON-frontier partition must be rewound to; if that rewind seek
        // FAILS, the partition is gated (suppressed + retried) instead of the
        // record being silently dropped forever.
        let mut gate = ResumeSeekGate::new();
        // The rewind failed → the partition must be gated at the first
        // discarded offset and suppressed until the seek lands.
        gate.set_target(1, 42);
        gate.taint(1);
        assert!(
            !gate.record_delivered(1),
            "a partition whose join-drive rewind failed must not consume past \
             the discarded record (C4)"
        );
        // Retry succeeds → consumption resumes from the rewound offset.
        assert!(gate.mark_active(1), "retried rewind seek activates");
        assert!(gate.record_delivered(1));
    }

    #[test]
    fn gate_paused_partition_retry_resume_cycle() {
        // Codex R3 H6: a partition paused after a transient seek failure must
        // be RETRIED and — once the seek succeeds — RESUMED, never left
        // paused until restart.
        let mut gate = ResumeSeekGate::new();
        gate.set_target(2, 100);
        // Transient failure: seek failed, pause succeeded.
        gate.note_paused(2);
        assert!(gate.is_paused(2));
        assert!(!gate.record_delivered(2), "paused partition is suppressed");
        assert_eq!(
            gate.pending(),
            vec![2],
            "a paused partition must stay in the retry set (H6)"
        );
        // Retry: the seek now succeeds → the loop resumes the partition and
        // activates it.
        gate.note_unpaused(2);
        assert!(gate.mark_active(2), "resumed partition activates");
        assert!(!gate.is_paused(2));
        assert!(
            gate.record_delivered(2),
            "recovered partition consumes again"
        );
        assert!(gate.is_clear());
    }

    #[test]
    fn gate_refuses_activation_while_paused() {
        // Codex R4 H1: "Active while broker-side PAUSED" is an invariant
        // violation — no record would ever flow on the partition again, and
        // because it leaves the retry set (`pending`) the seek-retry timer
        // stops touching it: a silent PERMANENT stall. The gate itself must
        // refuse the transition; callers resume() first (note_unpaused) and
        // only then activate.
        let mut gate = ResumeSeekGate::new();
        gate.set_target(0, 200);
        gate.note_paused(0);
        // The no-op carve-out path (seek failed, committed == target,
        // untouched position) must NOT be able to activate a paused partition.
        assert!(
            !gate.mark_active(0),
            "activation must be refused while the partition is paused (H1)"
        );
        assert!(
            !gate.record_delivered(0),
            "a paused partition stays suppressed after a refused activation"
        );
        assert_eq!(
            gate.pending(),
            vec![0],
            "a refused activation must keep the partition in the retry set (H1)"
        );
        // Once the broker-side resume lands, activation succeeds.
        gate.note_unpaused(0);
        assert!(gate.mark_active(0), "resumed partition may activate");
        assert!(gate.record_delivered(0));
        assert!(gate.is_clear());
    }

    #[test]
    fn gate_noop_carveout_requires_untouched_position() {
        // The committed==target "failed seek is a harmless no-op" carve-out is
        // only sound while the consumer's position still equals the committed
        // offset. Once ANY record was delivered (and suppressed) on the
        // partition, the position has advanced past it — the carve-out must
        // be refused and the seek enforced (C2/C3).
        let mut gate = ResumeSeekGate::new();
        gate.set_target(0, 200);
        assert!(
            gate.may_noop_failed_seek(0, Some(200), 200),
            "untouched position + committed==target → safe no-op"
        );
        assert!(
            !gate.may_noop_failed_seek(0, Some(150), 200),
            "committed != target is never a safe no-op"
        );
        assert!(
            !gate.may_noop_failed_seek(0, None, 200),
            "no committed offset is never a safe no-op (C1)"
        );
        // A suppressed delivery taints the partition's position.
        assert!(!gate.record_delivered(0));
        assert!(
            !gate.may_noop_failed_seek(0, Some(200), 200),
            "a delivered-and-suppressed record invalidates the carve-out"
        );
        // Activation clears the taint (the position is authoritative again).
        assert!(gate.mark_active(0), "activation clears the taint");
        assert!(gate.record_delivered(0));
    }

    // -----------------------------------------------------------------
    // Rebalance re-arm (Codex R4 H2) + watermark clamp (Codex R4 H3)
    // -----------------------------------------------------------------

    #[test]
    fn rebalance_rearm_regates_affected_partitions() {
        // Codex R4 H2: after a revoke+reassign the partition restarts from
        // its COMMITTED offset, which lags the true position whenever the
        // async commit did not land (or rows were consumed since the last
        // publish). An Active gate would let that stale position flow —
        // already-published durable rows re-consumed and RE-PUBLISHED
        // (permanent double count). The re-arm must force a fresh seek:
        //  * consumed partitions are re-armed to their exact restore point;
        //  * untouched frontier partitions re-derive their target;
        //  * untouched non-frontier gated partitions keep their obligation.
        let mut frontier = BTreeMap::new();
        frontier.insert(0, PartitionResume::new(0, vec![OffsetSpan::new(0, 500)]));
        frontier.insert(1, PartitionResume::new(0, vec![OffsetSpan::new(0, 300)]));
        let mut gate = ResumeSeekGate::new();
        // p0: frontier, activated, then consumed up to (but excluding) 700.
        gate.require_target(0);
        assert!(gate.mark_active(0));
        // p1: frontier, activated, nothing consumed yet.
        gate.require_target(1);
        assert!(gate.mark_active(1));
        // p2: non-frontier join-drive rewind still pending (C4).
        gate.set_target(2, 42);
        gate.taint(2);
        // In-session RESUME FLOORS (durable-OR-buffered frontier, R12 H1): p0
        // and the free p3.
        let mut resume_floor = BTreeMap::new();
        resume_floor.insert(0, 700);
        resume_floor.insert(3, 90);

        let mut events = RebalanceEvents::default();
        events.revoked.extend([0, 1, 2, 3]);
        events.assigned.extend([0, 1, 2, 3]);
        let rearmed = rearm_gate_after_rebalance(&mut gate, &frontier, &resume_floor, &events);
        assert_eq!(rearmed, 3, "p0/p1/p3 re-armed; p2 keeps its pending seek");

        // p0: restore obligation carrying the in-session floor — the final
        // seek target is resolved at activation as max(fresh committed, 700)
        // (R6 H1), so nothing this consumer durably published or still holds
        // buffered is re-delivered AND nothing another member committed
        // meanwhile is re-published.
        assert_eq!(
            gate.state(0),
            SeekGateState::NeedsRestore { resume_floor: 700 }
        );
        assert!(
            !gate.record_delivered(0),
            "a stale-committed post-rebalance record must be suppressed (H2)"
        );
        // p1: untouched frontier → target re-derived exactly like the
        // restart path (its committed offset cannot have moved in-session).
        assert_eq!(gate.state(1), SeekGateState::NeedsTarget);
        assert!(!gate.record_delivered(1));
        // p2: the pending rewind obligation is preserved (C4).
        assert_eq!(gate.state(2), SeekGateState::NeedsSeek { target: 42 });
        // p3: free partition WITH progress → restore obligation too (its
        // committed offset lags by up to one un-published flush).
        assert_eq!(
            gate.state(3),
            SeekGateState::NeedsRestore { resume_floor: 90 }
        );
    }

    #[test]
    fn rebalance_rearm_clears_stale_pause_markers() {
        // Codex R4 H2/H1 interplay: the app-level pause flag lives on the
        // toppar and DIES with the revoke — a reassigned toppar starts
        // UNPAUSED. A stale `paused` marker would make the H1 invariant
        // refuse activation forever (mark_active → false with no broker-side
        // pause left to resume). Revocation must clear the marker.
        let mut gate = ResumeSeekGate::new();
        gate.set_target(5, 100);
        gate.note_paused(5);
        let mut resume_floor = BTreeMap::new();
        resume_floor.insert(5, 100);
        let mut events = RebalanceEvents::default();
        events.revoked.insert(5);
        events.assigned.insert(5);
        let _ = rearm_gate_after_rebalance(&mut gate, &BTreeMap::new(), &resume_floor, &events);
        assert!(
            !gate.is_paused(5),
            "revocation clears the broker-side pause marker (the toppar died)"
        );
        assert!(
            gate.mark_active(5),
            "the re-armed partition can activate once its restore seek lands"
        );
    }

    #[test]
    fn rebalance_error_rearms_every_tracked_partition() {
        // Codex R4 H2: a rebalance ERROR makes rdkafka unassign EVERYTHING —
        // every tracked partition (durable frontier or in-session progress)
        // must be treated as revoked+reassigned even though no partition
        // list was delivered with the error.
        let mut frontier = BTreeMap::new();
        frontier.insert(0, PartitionResume::new(0, vec![]));
        let mut gate = ResumeSeekGate::new();
        gate.require_target(0);
        assert!(gate.mark_active(0));
        let mut resume_floor = BTreeMap::new();
        resume_floor.insert(7, 30);
        let events = RebalanceEvents {
            lost_all: true,
            ..RebalanceEvents::default()
        };
        let rearmed = rearm_gate_after_rebalance(&mut gate, &frontier, &resume_floor, &events);
        assert_eq!(rearmed, 2);
        assert_eq!(gate.state(0), SeekGateState::NeedsTarget);
        assert_eq!(
            gate.state(7),
            SeekGateState::NeedsRestore { resume_floor: 30 }
        );
    }

    #[test]
    fn rebalance_notices_accumulate_and_drain() {
        // The context's callbacks WRITE (possibly across several rebalance
        // rounds) and the loop DRAINS; a drain leaves the accumulator empty
        // and clones share state (the consumer context holds one clone, the
        // loop another).
        let notices = RebalanceNotices::new();
        assert!(notices.take().is_empty(), "fresh accumulator is empty");
        notices.record_revoked([1, 2]);
        notices.record_assigned([2, 3]);
        let ev = notices.take();
        assert_eq!(ev.revoked.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(ev.assigned.iter().copied().collect::<Vec<_>>(), vec![2, 3]);
        assert!(!ev.lost_all);
        assert!(!ev.is_empty());
        assert!(notices.take().is_empty(), "take drains");
        notices.record_lost_all();
        let ev = notices.take();
        assert!(ev.lost_all && !ev.is_empty());
        let writer = notices.clone();
        writer.record_assigned([9]);
        assert!(
            notices.take().assigned.contains(&9),
            "clones share the accumulator"
        );
    }

    #[test]
    fn rebalance_rearm_noop_cases() {
        // Empty events, and partitions unknown to both the frontier and the
        // in-session progress map: nothing is re-armed — a genuinely NEW
        // partition (discovered by the metadata refresh) keeps the normal
        // `auto.offset.reset` first-start semantics.
        let mut gate = ResumeSeekGate::new();
        let empty = RebalanceEvents::default();
        assert_eq!(
            rearm_gate_after_rebalance(&mut gate, &BTreeMap::new(), &BTreeMap::new(), &empty),
            0
        );
        let mut events = RebalanceEvents::default();
        events.assigned.insert(11); // newly discovered partition
        assert_eq!(
            rearm_gate_after_rebalance(&mut gate, &BTreeMap::new(), &BTreeMap::new(), &events),
            0
        );
        assert!(
            gate.record_delivered(11),
            "a genuinely new partition consumes freely"
        );
    }

    #[test]
    fn rebalance_restore_floor_is_durable_or_buffered_not_dropped() {
        // Codex R12 H1: after a rebalance, the restore floor must be the
        // durable-OR-BUFFERED frontier — NEVER the raw consumed-forward point,
        // which includes a DROPPED (failed-publish) batch. Restoring PAST a
        // dropped batch seeks over records that exist nowhere (dropped from
        // memory, never durable); once retention advances they are lost.
        //
        // FAILED-PUBLISH case: the consumer consumed [0,100), the roll FAILED
        // and drained the buffer (rows dropped), so the durable frontier stays
        // at the base 0 and the buffer is EMPTY. The safe resume floor is 0
        // (re-consume the dropped batch) — NOT the consumed-forward 100 the
        // pre-fix loop tracked.
        let mut contiguous = ContiguousFrontier::new();
        contiguous.note_consumed(0, 0); // base 0 (records [0,100) were consumed)
        // The [0,100) roll FAILED → no record_durable → the durable frontier
        // stays pinned at the base; the failed roll also drained the buffer.
        let empty_buffer = StreamingBuffer::from_config(&config(100));
        let floor = safe_resume_frontier(&contiguous, &empty_buffer);
        assert_eq!(
            floor.get(&0),
            Some(&0),
            "a DROPPED batch (failed publish emptied the buffer) never lifts the \
             restore floor above the durable frontier — it is re-consumed (H1); \
             the raw consumed-forward point (100) would seek past it = loss"
        );
        // And feeding that floor through the rebalance re-arm restores to it.
        let mut gate = ResumeSeekGate::new();
        gate.require_target(0);
        assert!(gate.mark_active(0));
        let mut events = RebalanceEvents::default();
        events.revoked.insert(0);
        events.assigned.insert(0);
        assert_eq!(
            rearm_gate_after_rebalance(&mut gate, &BTreeMap::new(), &floor, &events),
            1
        );
        assert_eq!(
            gate.state(0),
            SeekGateState::NeedsRestore { resume_floor: 0 },
            "the re-arm carries the durable floor (0), not the dropped-batch \
             consumed-forward point (100)"
        );
        // max(committed, floor): with the commit ALSO pinned at the gap (0),
        // the resolved restore is 0 — the failed [0,100) is re-consumed.
        assert_eq!(restore_target(false, Some(0), 0), 0);

        // BUFFERED case: the SAME consume, but the rows are still BUFFERED (no
        // failure). The buffer's span [0,100) lifts the floor to 100, so the
        // buffered rows are re-published from the buffer (dup-free) and the
        // restore skips PAST them — the 2f fenced-rebalance invariant.
        let mut buffered = StreamingBuffer::from_config(&config(1_000));
        for o in 0..100 {
            buffered.record_offset(0, o);
            assert!(!buffered.push_payload(&payload(1, "x")).expect("push"));
        }
        let floor_buf = safe_resume_frontier(&contiguous, &buffered);
        assert_eq!(
            floor_buf.get(&0),
            Some(&100),
            "still-buffered rows lift the restore floor (re-published from the \
             buffer, so skipping past them is dup-free — 2f)"
        );

        // ASYNC-COMMIT-LAG regression: Ok-published [0,100) → durable frontier
        // 100, buffer empty, Kafka commit lagged. The floor is 100 (durable),
        // so the restore does NOT re-consume the already-durable span.
        let mut durable = ContiguousFrontier::new();
        durable.note_consumed(0, 0);
        durable.record_durable(&spans(&[(0, 0, 100)]));
        let floor_lag = safe_resume_frontier(&durable, &empty_buffer);
        assert_eq!(
            floor_lag.get(&0),
            Some(&100),
            "Ok-published-but-commit-lagged: the durable frontier reflects the \
             publish, so the restore does not re-consume it (consumed_next's \
             one real benefit, preserved)"
        );
    }

    #[test]
    fn publish_failure_reseeks_active_partitions_to_durable_frontier() {
        // Codex R12 H2: a transient publish failure dropped the rolled batch,
        // leaving a gap. The loop must NOT keep consuming forward — that piles
        // up durable segments the contiguous-commit frontier can never advance
        // past (pinned at the gap), each RE-PUBLISHED from the gap on every
        // restart (unbounded double count). Instead each ACTIVE in-session
        // partition is re-armed to a re-seek at its durable frontier, TAINTED
        // so a failed backward re-seek cannot no-op past the gap.
        let mut contiguous = ContiguousFrontier::new();
        // p0: published [0,50) durably, then consumed forward with the
        // [50,..) roll dropped by the failed publish → durable frontier 50.
        contiguous.note_consumed(0, 0);
        contiguous.record_durable(&spans(&[(0, 0, 50)]));
        contiguous.note_consumed(0, 50); // (base already 0; forward consume)
        assert_eq!(contiguous.durable_next(0), Some(50));
        // p7: brand-new active partition, base 500, nothing durable yet → the
        // whole buffered range was dropped, so the frontier is its base 500.
        contiguous.note_consumed(7, 500);
        assert_eq!(contiguous.durable_next(7), Some(500));
        // p3: has an in-session frontier entry but is currently GATED (a
        // pending rewind) — it consumed nothing into the failed batch, so it
        // must NOT be re-armed by the failure path.
        contiguous.note_consumed(3, 42);
        let mut gate = ResumeSeekGate::new();
        gate.set_target(3, 42);

        let targets = arm_publish_failure_reseek(&mut gate, &contiguous);
        assert_eq!(
            targets,
            vec![(0, 50), (7, 500)],
            "only ACTIVE in-session partitions are re-sought, each to its \
             durable frontier (never the consumed-forward point)"
        );
        // p0/p7 are now gated on a re-seek to the durable frontier + tainted.
        assert_eq!(gate.state(0), SeekGateState::NeedsSeek { target: 50 });
        assert_eq!(gate.state(7), SeekGateState::NeedsSeek { target: 500 });
        assert!(
            !gate.record_delivered(0),
            "forward records are dropped until the re-seek lands"
        );
        // The taint disables the failed-seek no-op carve-out, so a failed
        // backward re-seek can never skip the re-consume of the dropped batch.
        assert!(
            !gate.may_noop_failed_seek(0, Some(50), 50),
            "a tainted re-seek must not be treated as a harmless no-op (that \
             would skip the dropped [50,..) records = loss)"
        );
        // p3's pending obligation is untouched (it fed no rows to the batch).
        assert_eq!(gate.state(3), SeekGateState::NeedsSeek { target: 42 });
    }

    #[test]
    fn clamp_resume_target_stays_inside_watermarks() {
        // Codex R4 H3: rdkafka's seek only moves the LOCAL fetch position;
        // an out-of-range target "succeeds" locally, then the first broker
        // fetch hits OFFSET_OUT_OF_RANGE and auto.offset.reset takes over —
        // `latest` skips every retained post-frontier record (loss). The
        // target must therefore be clamped into [low, high] BEFORE seeking.
        assert_eq!(
            clamp_resume_target(451, 0, 1000),
            451,
            "an in-range target is untouched"
        );
        assert_eq!(
            clamp_resume_target(451, 451, 1000),
            451,
            "target == low is in range"
        );
        assert_eq!(
            clamp_resume_target(1000, 0, 1000),
            1000,
            "target == high (log end, caught up) is in range — the consumer \
             waits there for NEW records"
        );
        assert_eq!(
            clamp_resume_target(100, 300, 1000),
            300,
            "a below-retention target is clamped UP to the low watermark — \
             the [100,300) records no longer exist; seeking 300 consumes \
             everything retained (bounded duplication, never reset-to-latest \
             loss, H3)"
        );
        assert_eq!(
            clamp_resume_target(1500, 300, 1000),
            300,
            "a past-end target is clamped to the LOW watermark, never high — \
             the offset space shrank (recreate/truncate), so every retained \
             record [low, high) is NEW and must be consumed; seeking high \
             would skip ALL of them (Codex R5 H1)"
        );
        assert_eq!(
            clamp_resume_target(500, 700, 600),
            700,
            "a degenerate (high < low) window collapses to low"
        );
    }

    #[test]
    fn clamp_resume_target_past_end_reconsumes_from_low() {
        // Codex R5 H1: a recreated/truncated topic SHRINKS the offset space —
        // the durable frontier (e.g. 1500) can exceed the current high
        // watermark (e.g. 1000). Clamping such a target to `high` (the R4 H3
        // behaviour) seeks to the log END and SKIPS every retained record
        // [low, high) — permanent loss of the recreated topic's data. The
        // reused offsets belong to a NEW offset space (the old durable
        // segments cover DIFFERENT records), so re-consuming from `low` is
        // no-loss and no-double-count. BOTH out-of-range directions therefore
        // clamp to `low`.
        assert_eq!(
            clamp_resume_target(1500, 0, 1000),
            0,
            "durable frontier past the recreated log's end must re-consume \
             the WHOLE retained log from low, not seek high and skip it (H1)"
        );
        assert_eq!(
            clamp_resume_target(1001, 0, 1000),
            0,
            "one past the end is already out of range → low"
        );
        assert_eq!(
            clamp_resume_target(1000, 0, 1000),
            1000,
            "target == high is NOT out of range — the consumer is caught up \
             and waits at the log end for new records"
        );
        // Empty partition (low == high): everything collapses to that point.
        assert_eq!(
            clamp_resume_target(5, 0, 0),
            0,
            "empty partition, past-end target → low (== high)"
        );
        assert_eq!(clamp_resume_target(0, 0, 0), 0, "empty partition, at end");
        assert_eq!(
            clamp_resume_target(7, 3, 3),
            3,
            "empty compacted partition, past-end target → low"
        );
        assert_eq!(
            clamp_resume_target(3, 3, 3),
            3,
            "empty compacted partition, at end"
        );
        assert_eq!(
            clamp_resume_target(1, 3, 3),
            3,
            "empty compacted partition, below-retention target → low"
        );
        // Degenerate high < low (never reported by a sane broker): low, for
        // any target position relative to the nonsense window.
        assert_eq!(clamp_resume_target(650, 700, 600), 700);
        assert_eq!(clamp_resume_target(9999, 700, 600), 700);
        assert_eq!(clamp_resume_target(-1, 700, 600), 700);
    }

    // -----------------------------------------------------------------
    // schema_fingerprint (Codex R26 F1)
    // -----------------------------------------------------------------

    /// A minimal valid streaming spec for fingerprint tests.
    fn fp_spec() -> KafkaSupervisorSpec {
        serde_json::from_value(serde_json::json!({
            "type": "kafka",
            "dataSchema": {
                "dataSource": "events",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page", {"name": "added", "type": "long"}]},
                "metricsSpec": [{"type": "doubleSum", "name": "bytes", "fieldName": "bytes"}],
                "granularitySpec": {"rollup": false, "queryGranularity": "NONE"}
            },
            "ioConfig": {
                "topic": "t",
                "consumerProperties": {"bootstrap.servers": "a:9092"},
                "useEarliestOffset": true
            }
        }))
        .expect("spec parses")
    }

    #[test]
    fn schema_fingerprint_is_stable_and_ignores_non_ingestion_fields() {
        let base = schema_fingerprint(&fp_spec());
        assert_eq!(
            base.len(),
            SCHEMA_FP_VERSION_PREFIX.len() + 16,
            "version prefix + 16-hex-char FNV-1a 64: {base}"
        );
        // Deterministic.
        assert_eq!(base, schema_fingerprint(&fp_spec()));
        // Non-ingestion-semantics changes must NOT change the fingerprint:
        // the same rows are rebuilt by a replay regardless of these.
        let mut spec = fp_spec();
        spec.io_config.topic = "another-topic".to_string();
        spec.io_config.use_earliest_offset = Some(false);
        spec.io_config
            .consumer_properties
            .insert("bootstrap.servers".to_string(), "b:9092".to_string());
        spec.tuning_config = Some(crate::KafkaTuningConfig {
            max_rows_in_memory: Some(10),
            max_rows_per_segment: Some(10),
            max_total_rows: None,
            intermediate_persist_period: None,
        });
        assert_eq!(base, schema_fingerprint(&spec));
        // A plain-string dimension and its explicit `type: string` form
        // materialise identically → identical fingerprints.
        let mut spec = fp_spec();
        spec.data_schema.dimensions_spec.dimensions[0] = DimensionEntry::Typed {
            name: "page".to_string(),
            dim_type: "string".to_string(),
        };
        assert_eq!(base, schema_fingerprint(&spec));
    }

    #[test]
    fn schema_fingerprint_changes_with_ingestion_semantics() {
        let base = schema_fingerprint(&fp_spec());
        // Renamed timestamp column: old records lack the new column — the
        // exact R26 F1 loss scenario.
        let mut spec = fp_spec();
        spec.data_schema.timestamp_spec.column = "new_ts".to_string();
        assert_ne!(base, schema_fingerprint(&spec));
        // Changed timestamp format grammar.
        let mut spec = fp_spec();
        spec.data_schema.timestamp_spec.format = "millis".to_string();
        assert_ne!(base, schema_fingerprint(&spec));
        // Dimension ORDER changes the stored segment layout.
        let mut spec = fp_spec();
        spec.data_schema.dimensions_spec.dimensions.reverse();
        assert_ne!(base, schema_fingerprint(&spec));
        // Dimension TYPE change.
        let mut spec = fp_spec();
        spec.data_schema.dimensions_spec.dimensions[1] = DimensionEntry::Typed {
            name: "added".to_string(),
            dim_type: "double".to_string(),
        };
        assert_ne!(base, schema_fingerprint(&spec));
        // Metric added.
        let mut spec = fp_spec();
        spec.data_schema
            .metrics_spec
            .push(serde_json::json!({"type": "count", "name": "cnt"}));
        assert_ne!(base, schema_fingerprint(&spec));
    }

    #[test]
    fn schema_fingerprint_covers_isolation_level_and_is_versioned() {
        // Codex R27 F1: `isolation.level` decides WHICH records an earliest
        // replay redelivers (read_uncommitted also delivers aborted
        // transactional records; read_committed never does), so it is part
        // of the fingerprint that gates the pre-replay cleanup — and every
        // fingerprint carries a canonicalisation-version prefix so
        // unknown/older stamps can be told apart from a genuine mismatch.
        let base = schema_fingerprint(&fp_spec());
        assert!(
            schema_fp_is_current_version(&base),
            "fingerprints must carry the canonicalisation version prefix: {base}"
        );
        // An unprefixed (R26-era) or unknown-version stamp is NOT current.
        assert!(!schema_fp_is_current_version("0123456789abcdef"));
        assert!(!schema_fp_is_current_version("v1:0123456789abcdef"));
        // Explicit read_committed == the librdkafka 2.12.1 default applied
        // when the property is unspecified → SAME fingerprint (no spurious
        // refusal for spelling out the default).
        let mut spec = fp_spec();
        spec.io_config
            .consumer_properties
            .insert("isolation.level".to_string(), "read_committed".to_string());
        assert_eq!(
            base,
            schema_fingerprint(&spec),
            "explicit read_committed must fingerprint like the default"
        );
        // read_uncommitted selects a DIFFERENT record set → different fp.
        let mut spec = fp_spec();
        spec.io_config.consumer_properties.insert(
            "isolation.level".to_string(),
            "read_uncommitted".to_string(),
        );
        assert_ne!(
            base,
            schema_fingerprint(&spec),
            "isolation.level changes record selection and must change the fingerprint"
        );
    }

    /// Codex R31: [`classify_watermarks`] is the pure core of the
    /// empty-topic guard — a topic proven empty must NOT let the
    /// earliest-replay cleanup drop the pair's prior segments (a recreated /
    /// retention-expired topic replays nothing). Fail-safe polarity: any
    /// single record sighting wins (`HasRecords`), all-empty-and-fetched is
    /// `Empty`, and ANY unfetched partition (or no partitions) degrades to
    /// `Unknown` — emptiness is never claimed on partial data, so a
    /// transient hiccup can only skip a duplication-avoiding KEEP, never
    /// cause a wrong drop.
    #[test]
    fn classify_watermarks_is_fail_safe() {
        // A single non-empty partition proves the topic holds records, even
        // if others are empty or unfetched.
        assert_eq!(
            classify_watermarks(&[Some((0, 0)), Some((5, 12)), None]),
            TopicRecords::HasRecords,
        );
        assert_eq!(
            classify_watermarks(&[Some((100, 250))]),
            TopicRecords::HasRecords,
        );
        // Every partition fetched AND empty (low == high, whether at 0 for a
        // fresh/recreated topic or advanced for a fully-consumed+expired one)
        // ⇒ Empty.
        assert_eq!(
            classify_watermarks(&[Some((0, 0)), Some((0, 0))]),
            TopicRecords::Empty,
        );
        assert_eq!(
            classify_watermarks(&[Some((900, 900))]),
            TopicRecords::Empty,
        );
        // Any unfetched partition without a record sighting ⇒ Unknown
        // (never claim empty on partial data).
        assert_eq!(
            classify_watermarks(&[Some((0, 0)), None]),
            TopicRecords::Unknown,
        );
        assert_eq!(classify_watermarks(&[None]), TopicRecords::Unknown);
        // An empty partition slice is also Unknown, not Empty.
        assert_eq!(classify_watermarks(&[]), TopicRecords::Unknown);
    }

    /// Codex R29 F2: the consumer must run the COOPERATIVE (incremental)
    /// rebalance protocol. librdkafka's default strategy list
    /// (`range,roundrobin`, rdkafka-sys 4.10.0+2.12.1 rdkafka_conf.c
    /// `.sdef`) is EAGER: any rebalance — e.g. the operator ADDS PARTITIONS
    /// to the topic — first revokes ALL owned partitions, discarding the
    /// in-memory fetch positions; because this consumer never commits
    /// offsets and its group id is unique per spawn, the re-assign then
    /// restarts every partition from `auto.offset.reset`, re-delivering
    /// already-published rows as in-process duplicates (no restart
    /// involved). `cooperative-sticky` never revokes unaffected partitions,
    /// so existing positions survive and only genuinely NEW partitions
    /// start from `auto.offset.reset`. This pins the config; the live
    /// rebalance behaviour needs a real broker (E2E residual, see the
    /// module header).
    #[cfg(feature = "kafka-io")]
    #[test]
    fn client_config_pins_cooperative_sticky_rebalance() {
        let mut cfg = config(10);
        // Defence-in-depth: even a strategy that somehow survived into
        // additional_properties must lose to the pin (applied LAST).
        cfg.additional_properties.insert(
            "partition.assignment.strategy".to_string(),
            "range".to_string(),
        );
        let client = build_client_config(&cfg);
        assert_eq!(
            client.get("partition.assignment.strategy"),
            Some("cooperative-sticky"),
            "the cooperative-sticky strategy must be pinned last"
        );
        // The neighbouring correctness pins stay intact.
        assert_eq!(client.get("enable.auto.commit"), Some("false"));
        assert_eq!(client.get("auto.offset.reset"), Some("earliest"));
        assert_eq!(client.get("queued.max.messages.kbytes"), Some("65536"));
    }

    /// Codex R37: the data-integrity + completeness EXACT pins win over any
    /// caller value (applied LAST, defence-in-depth even if one survived into
    /// `additional_properties`): `check.crcs=true` (F1 — verify every record's
    /// CRC so a bit-flip cannot be published as a wrong value) and
    /// `allow.auto.create.topics=false` (a caller `true` must NOT let a topic
    /// typo auto-create an empty log). `topic.metadata.refresh.interval.ms` is
    /// covered separately by [`client_config_clamps_metadata_refresh`] because
    /// it is a CLAMP (a faster caller value survives), not an exact pin.
    #[cfg(feature = "kafka-io")]
    #[test]
    fn client_config_pins_r37_integrity_and_completeness() {
        let mut cfg = config(10);
        // The exact pathological caller values from the R37 findings.
        cfg.additional_properties
            .insert("check.crcs".to_string(), "false".to_string());
        cfg.additional_properties
            .insert("allow.auto.create.topics".to_string(), "true".to_string());
        let client = build_client_config(&cfg);
        assert_eq!(
            client.get("check.crcs"),
            Some("true"),
            "check.crcs must be pinned true so a corrupt record fails CRC (R37 F1)"
        );
        assert_eq!(
            client.get("allow.auto.create.topics"),
            Some("false"),
            "a caller's true must NOT let a topic typo auto-create an empty log (R37)"
        );
        // The neighbouring correctness pins stay intact.
        assert_eq!(client.get("enable.auto.commit"), Some("false"));
        assert_eq!(client.get("metadata.recovery.strategy"), Some("none"));
    }

    /// Codex R38: the protocol-version negotiation pins win over any caller
    /// value (applied LAST, defence-in-depth even if one survived into
    /// `additional_properties`). `api.version.request=false` +
    /// `broker.version.fallback=0.9.0` would make librdkafka speak a pre-v4
    /// Fetch that a down-conversion-enabled 2.x/3.x broker serves as
    /// READ_UNCOMMITTED, bypassing the pinned `read_committed` and silently
    /// publishing aborted transactional rows (KIP-98). `api.version.request` is
    /// pinned `true` (always negotiate the broker's real feature set) and
    /// `broker.version.fallback` is pinned to the first transactional-Fetch
    /// release `0.11.0.0`, so even the residual "negotiation genuinely fails /
    /// times out" path assumes a `read_committed`-capable broker.
    #[cfg(feature = "kafka-io")]
    #[test]
    fn client_config_pins_r38_protocol_negotiation() {
        let mut cfg = config(10);
        // The exact protocol-downgrade caller values from the R38 finding.
        cfg.additional_properties
            .insert("api.version.request".to_string(), "false".to_string());
        cfg.additional_properties
            .insert("broker.version.fallback".to_string(), "0.9.0".to_string());
        let client = build_client_config(&cfg);
        assert_eq!(
            client.get("api.version.request"),
            Some("true"),
            "api.version.request must be pinned true so negotiation is never disabled (R38)"
        );
        assert_eq!(
            client.get("broker.version.fallback"),
            Some("0.11.0.0"),
            "broker.version.fallback must be floored to a read_committed-capable version (R38)"
        );
        // The neighbouring correctness pins stay intact.
        assert_eq!(client.get("enable.auto.commit"), Some("false"));
        assert_eq!(client.get("check.crcs"), Some("true"));
    }

    /// Codex R37 F2 (round-3): `topic.metadata.refresh.interval.ms` is CLAMPED
    /// to `[floor, 300000]`, applied LAST. `-1` (disable), `0`, and any value
    /// SLOWER than the default collapse to the safe ceiling (discovery can
    /// never be switched off or slowed past the default), but a legitimately
    /// FASTER caller value is PRESERVED (a short-retention topic that adds
    /// partitions needs prompt discovery — an exact pin would override it and
    /// re-open the loss). A below-floor value is raised (no broker hammering).
    #[cfg(feature = "kafka-io")]
    #[test]
    fn client_config_clamps_metadata_refresh() {
        let effective = |v: &str| {
            let mut cfg = config(10);
            cfg.additional_properties.insert(
                "topic.metadata.refresh.interval.ms".to_string(),
                v.to_string(),
            );
            build_client_config(&cfg)
                .get("topic.metadata.refresh.interval.ms")
                .map(String::from)
        };
        // Disable / invalid / slower-than-default → the safe ceiling.
        assert_eq!(effective("-1").as_deref(), Some("300000"), "-1 (disable)");
        assert_eq!(effective("0").as_deref(), Some("300000"), "0");
        assert_eq!(effective("not-a-number").as_deref(), Some("300000"), "junk");
        assert_eq!(
            effective("600000").as_deref(),
            Some("300000"),
            "slower than the default is capped to the default"
        );
        // A legitimately faster refresh is PRESERVED.
        assert_eq!(
            effective("30000").as_deref(),
            Some("30000"),
            "a faster caller value must survive (short-retention topics)"
        );
        // Below the floor is raised (no broker hammering).
        assert_eq!(
            effective("1").as_deref(),
            Some("1000"),
            "below floor raised"
        );
        // Absent → the safe ceiling default.
        let base = build_client_config(&config(10));
        assert_eq!(
            base.get("topic.metadata.refresh.interval.ms"),
            Some("300000"),
            "absent ⇒ the safe default"
        );
    }

    /// Codex R36: `max.poll.interval.ms` is FLOORED to `max(caller, 300000)`,
    /// applied LAST so it wins. librdkafka checks the interval twice a second,
    /// so a pathological `1` would let any publish out-blocking ~500 ms revoke
    /// every partition; the never-committing, unique-group consumer then
    /// re-consumes already-published rows from `auto.offset.reset`. A FLOOR
    /// (not an exact pin) blocks going BELOW the safe default while PRESERVING
    /// a legitimately larger caller value — critical because librdkafka
    /// requires `max.poll.interval.ms >= session.timeout.ms`, so an exact pin
    /// would break a valid large-session config.
    #[cfg(feature = "kafka-io")]
    #[test]
    fn client_config_floors_max_poll_interval() {
        // A pathologically small caller value is RAISED to the floor.
        let mut small = config(10);
        small
            .additional_properties
            .insert("max.poll.interval.ms".to_string(), "1".to_string());
        assert_eq!(
            build_client_config(&small).get("max.poll.interval.ms"),
            Some("300000"),
            "a below-floor caller value must be raised to 300000 (R36)"
        );

        // A legitimately LARGER caller value is PRESERVED (would otherwise
        // break a `session.timeout.ms=600000` config that needs max.poll >= it).
        let mut large = config(10);
        large
            .additional_properties
            .insert("max.poll.interval.ms".to_string(), "900000".to_string());
        assert_eq!(
            build_client_config(&large).get("max.poll.interval.ms"),
            Some("900000"),
            "a legitimately larger caller value must be preserved by the floor (R36)"
        );

        // Absent → the default floor. Unparseable garbage → floored too.
        assert_eq!(
            build_client_config(&config(10)).get("max.poll.interval.ms"),
            Some("300000"),
            "an absent value defaults to the floor"
        );
        let mut junk = config(10);
        junk.additional_properties.insert(
            "max.poll.interval.ms".to_string(),
            "not-a-number".to_string(),
        );
        assert_eq!(
            build_client_config(&junk).get("max.poll.interval.ms"),
            Some("300000"),
            "an unparseable value is replaced by the floor (never left for librdkafka to reject)"
        );

        // A value ABOVE librdkafka's documented max must be clamped to the
        // ceiling, NOT passed through: librdkafka narrows it with
        // `(int)strtol(...)`, so `4294967297` (2^32 + 1) would wrap to `1` —
        // silently restoring the pathological interval the floor blocks (R36).
        let mut oversized = config(10);
        oversized
            .additional_properties
            .insert("max.poll.interval.ms".to_string(), "4294967297".to_string());
        assert_eq!(
            build_client_config(&oversized).get("max.poll.interval.ms"),
            Some("86400000"),
            "an above-max value must be clamped to librdkafka's documented ceiling so it \
             cannot narrow to a pathological interval (R36)"
        );

        // The neighbouring correctness pins stay intact.
        assert_eq!(
            build_client_config(&config(10)).get("enable.auto.commit"),
            Some("false")
        );
    }

    /// Codex R30 F2: a stop caused by a fatal consume error must report
    /// replay-required even when every flush succeeded — the consumer
    /// stopped DELIVERING, so it cannot claim the topic's records became
    /// queryable (worst case: right after an earliest cleanup dropped the
    /// pair's prior segments).
    #[test]
    fn stats_fatal_consume_error_requires_replay() {
        let stats = StreamingStats {
            total_consumed: 3,
            total_published: 1,
            final_flush_failed: false,
            mid_stream_flush_failed: false,
            fatal_consume_error: true,
            cluster_id_drifted: false,
        };
        assert!(
            stats.replay_required(),
            "a fatal consume error must make the stop non-clean (R30 F2)"
        );
        let clean = StreamingStats {
            fatal_consume_error: false,
            ..stats
        };
        assert!(!clean.replay_required());
    }

    /// Codex R37 F3: a stop caused by a detected cluster-identity DRIFT must
    /// report replay-required even when every flush succeeded — the loop
    /// deliberately did NOT publish the possibly-mis-attributed buffer, so a
    /// replay is the only way those rows become queryable under the right
    /// cluster.
    #[test]
    fn stats_cluster_id_drift_requires_replay() {
        let drift = StreamingStats {
            total_consumed: 9,
            total_published: 2,
            final_flush_failed: false,
            mid_stream_flush_failed: false,
            fatal_consume_error: false,
            cluster_id_drifted: true,
        };
        assert!(
            drift.replay_required(),
            "a cluster-identity drift must make the stop non-clean (R37 F3)"
        );
        let clean = StreamingStats {
            cluster_id_drifted: false,
            ..drift
        };
        assert!(!clean.replay_required());
    }

    /// Codex R37 F3: the pure drift classifier is fail-safe. Only a KNOWN live
    /// id that DIFFERS from the stamped one is drift; an unresolvable live id
    /// (`None`) is never drift, so a transient metadata gap can only defer the
    /// check, never fabricate a fail-loud stop.
    #[test]
    fn cluster_id_drift_classification_is_fail_safe() {
        // Confirmed different cluster id ⇒ drift.
        assert!(cluster_id_is_drift(Some("cluster-B"), "cluster-A"));
        // Same id ⇒ not drift.
        assert!(!cluster_id_is_drift(Some("cluster-A"), "cluster-A"));
        // Unresolvable live id ⇒ never drift (fail-safe).
        assert!(!cluster_id_is_drift(None, "cluster-A"));
    }

    /// Codex R30 F2: the recv-error classification — authorization errors
    /// and librdkafka-fatal errors stop the consumer fail-loud; transient
    /// broker/topic conditions keep the warn-and-retry behaviour.
    #[cfg(feature = "kafka-io")]
    #[test]
    fn fatal_consume_error_classification() {
        use rdkafka::error::KafkaError;
        use rdkafka::types::RDKafkaErrorCode;

        // Fatal: the principal lost READ (the exact R30 F2 scenario), the
        // group/cluster authz variants, a librdkafka fatal error, and the
        // R37 F1 payload-integrity / decode codes (a CRC-failed record under
        // the pinned check.crcs, or an undecodable batch).
        for fatal in [
            KafkaError::MessageConsumption(RDKafkaErrorCode::TopicAuthorizationFailed),
            KafkaError::MessageConsumption(RDKafkaErrorCode::GroupAuthorizationFailed),
            KafkaError::MessageConsumption(RDKafkaErrorCode::ClusterAuthorizationFailed),
            KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::Fatal),
            KafkaError::MessageConsumption(RDKafkaErrorCode::BadMessage),
            KafkaError::MessageConsumption(RDKafkaErrorCode::BadCompression),
        ] {
            assert!(
                consume_error_is_fatal(&fatal),
                "must be fatal (fail-loud): {fatal}"
            );
        }
        // Transient: recoverable broker/topic conditions must keep the
        // consumer alive and retrying.
        for transient in [
            KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown),
            KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition),
            KafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut),
            KafkaError::NoMessageReceived,
        ] {
            assert!(
                !consume_error_is_fatal(&transient),
                "must stay transient (warn + retry): {transient}"
            );
        }
    }

    /// Codex R35: a PERMANENT group-join / config / protocol rejection —
    /// a JoinGroup/SyncGroup the broker refuses for a reason no retry can
    /// heal (the exact `InvalidSessionTimeout` scenario: a locally-valid but
    /// broker-rejected `session.timeout.ms`) — must be classified FATAL so
    /// the poll loop stops fail-loud (and the same rule gates the pre-cleanup
    /// group-join probe). The TRANSIENT group-coordinator conditions
    /// librdkafka heals by itself (`CoordinatorNotAvailable` / `NotCoordinator`
    /// / `RebalanceInProgress` / `CoordinatorLoadInProgress`, and the normal
    /// rejoin handshake `IllegalGeneration` / `UnknownMemberId` /
    /// `MemberIdRequired`) must stay warn-and-retry — turning them fatal would
    /// permanently refuse a shutdown on an ordinary rebalance.
    #[cfg(feature = "kafka-io")]
    #[test]
    fn permanent_group_config_recv_errors_are_fatal() {
        use rdkafka::error::KafkaError;
        use rdkafka::types::RDKafkaErrorCode;

        // PERMANENT (config/protocol) — retry cannot heal, so fail-loud.
        for fatal in [
            RDKafkaErrorCode::InvalidSessionTimeout,
            RDKafkaErrorCode::InvalidGroupId,
            RDKafkaErrorCode::InconsistentGroupProtocol,
            RDKafkaErrorCode::UnsupportedVersion,
        ] {
            assert!(
                consume_error_is_fatal(&KafkaError::MessageConsumption(fatal)),
                "permanent group/config rejection must be fatal (fail-loud): {fatal:?}"
            );
        }
        // TRANSIENT group-coordinator / rejoin conditions — librdkafka heals
        // them; they must NOT be fatal or a routine rebalance would refuse
        // every shutdown.
        for transient in [
            RDKafkaErrorCode::CoordinatorNotAvailable,
            RDKafkaErrorCode::NotCoordinator,
            RDKafkaErrorCode::CoordinatorLoadInProgress,
            RDKafkaErrorCode::RebalanceInProgress,
            RDKafkaErrorCode::IllegalGeneration,
            RDKafkaErrorCode::UnknownMemberId,
            RDKafkaErrorCode::MemberIdRequired,
        ] {
            assert!(
                !consume_error_is_fatal(&KafkaError::MessageConsumption(transient)),
                "transient group condition must stay warn+retry: {transient:?}"
            );
        }
    }

    /// Codex R39: a PERMANENT invalid-topic-name rejection (`InvalidTopic`,
    /// protocol error 17) must be classified FATAL so the poll loop stops
    /// fail-loud instead of retrying forever while the acknowledged supervisor
    /// consumes nothing until retention. The topic-creation-race code
    /// `UnknownTopicOrPartition` (3, R30) and the admin/CreateTopics-only
    /// `InvalidPartitions` (37) must NOT be swept in — turning them fatal would
    /// refuse shutdown on a routine metadata-propagation delay / an unreachable
    /// code path.
    #[cfg(feature = "kafka-io")]
    #[test]
    fn permanent_invalid_topic_recv_error_is_fatal() {
        use rdkafka::error::KafkaError;
        use rdkafka::types::RDKafkaErrorCode;

        assert!(
            consume_error_is_fatal(&KafkaError::MessageConsumption(
                RDKafkaErrorCode::InvalidTopic
            )),
            "InvalidTopic (permanent bad topic name) must be fatal (fail-loud)"
        );
        // Must NOT over-broaden: the topic-creation-race code stays transient
        // (R30), and the admin-only CreateTopics error is unreachable on the
        // consume path — both stay warn+retry.
        for transient in [
            RDKafkaErrorCode::UnknownTopicOrPartition,
            RDKafkaErrorCode::InvalidPartitions,
        ] {
            assert!(
                !consume_error_is_fatal(&KafkaError::MessageConsumption(transient)),
                "must stay transient (not swept into the R39 InvalidTopic fix): {transient:?}"
            );
        }
    }

    /// Codex R30 F2: the topic-readiness probe must FAIL (bounded) against
    /// an unreachable broker — the overlord runs it before the destructive
    /// earliest-replay cleanup, so `Err` here is what preserves the prior
    /// segments. (The authorization-failure shape needs a real broker with
    /// ACLs and stays a real-broker E2E residual; the topic-level error
    /// branch is the same code path.)
    #[cfg(feature = "kafka-io")]
    #[test]
    fn probe_topic_metadata_fails_on_unreachable_broker() {
        // A StreamConsumer needs a Tokio runtime CONTEXT to be created; the
        // probe itself is deliberately blocking (called via spawn_blocking
        // in production), so it runs outside async.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio rt");
        let _guard = rt.enter();
        let mut cfg = config(10);
        cfg.brokers = "127.0.0.1:59092".to_string(); // nothing listens here
        let consumer = create_stream_consumer(&cfg).expect("lazy create+subscribe succeeds");
        let err = probe_topic_metadata(&consumer, "t", std::time::Duration::from_millis(250))
            .expect_err("an unreachable broker must fail the readiness probe");
        assert!(
            format!("{err}").contains("topic-readiness probe"),
            "the error must name the probe: {err}"
        );
    }

    /// Codex R35 (review-hardened): the pre-cleanup group-join probe must NOT
    /// spuriously refuse on a merely inconclusive poll — against a broker that
    /// delivers no PERMANENT group/config rejection (here: a temporarily
    /// unreachable broker) it must PROCEED (`Ok`) after the bounded window, so
    /// a broker hiccup or a still-in-progress join (e.g. inside
    /// `group.initial.rebalance.delay.ms`) never permanently blocks a
    /// legitimate earliest re-create. It REFUSES only on a fatal poll error,
    /// which surfaces fast; that PERMANENT `InvalidSessionTimeout` refuse
    /// needs a real broker and is a real-broker E2E residual, and its DECISION
    /// rule is the `consume_error_is_fatal` classification pinned by
    /// `permanent_group_config_recv_errors_are_fatal`.
    #[cfg(feature = "kafka-io")]
    #[test]
    fn group_join_probe_proceeds_without_permanent_rejection() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio rt");
        rt.block_on(async {
            let mut cfg = config(10);
            cfg.brokers = "127.0.0.1:59092".to_string(); // nothing listens here
            let consumer = create_stream_consumer(&cfg).expect("lazy create+subscribe succeeds");
            probe_group_joinable(&consumer, std::time::Duration::from_millis(250))
                .await
                .expect(
                    "no PERMANENT rejection ⇒ PROCEED (Ok): a transient/unreachable broker or an \
                     in-progress join must never spuriously refuse the earliest re-create",
                );
        });
    }

    /// Codex R30 F1: the consumer must NOT silently re-bootstrap onto a
    /// different cluster. librdkafka 2.12.1 defaults
    /// `metadata.recovery.strategy` to `rebootstrap` (rdkafka-sys
    /// 4.10.0+2.12.1 rdkafka_conf.c:454 / CONFIGURATION.md): when every
    /// known broker becomes unavailable, the client repeats the bootstrap
    /// process from `bootstrap.servers` — so a DNS repoint of that name to
    /// a DIFFERENT cluster silently migrates the live consumer mid-session,
    /// while the sink keeps stamping the START-time cluster id onto rows
    /// that now come from the other cluster (mislabeled provenance → a
    /// later earliest re-create drops/keeps the wrong rows). Pinning the
    /// strategy to `none` removes the class: one consumer session can never
    /// abandon its resolved cluster identity, so the start-time stamp stays
    /// truthful for the session's whole life.
    #[cfg(feature = "kafka-io")]
    #[test]
    fn client_config_pins_metadata_recovery_none() {
        let mut cfg = config(10);
        // Defence-in-depth: a spec-smuggled rebootstrap must lose to the
        // pin (applied LAST).
        cfg.additional_properties.insert(
            "metadata.recovery.strategy".to_string(),
            "rebootstrap".to_string(),
        );
        let client = build_client_config(&cfg);
        assert_eq!(
            client.get("metadata.recovery.strategy"),
            Some("none"),
            "rebootstrap must be pinned OFF (R30 F1): a re-bootstrap can silently \
             migrate the session to a different cluster and stale-stamp provenance"
        );
        // The neighbouring correctness pins stay intact.
        assert_eq!(client.get("enable.auto.commit"), Some("false"));
        assert_eq!(
            client.get("partition.assignment.strategy"),
            Some("cooperative-sticky")
        );
    }

    #[test]
    fn rebalance_rearm_must_not_pin_restore_below_group_committed() {
        // Codex R6 H1: the `resume_floor` is THIS consumer's local progress —
        // it knows nothing about the rest of the GROUP. If member A lost the
        // partition at durable floor 100 and member B consumed, durably
        // published, and COMMITTED up to 200 before A is re-assigned, pinning
        // A's restore seek at its stale floor=100 seeks BACKWARD below the
        // group's committed progress and re-consumes + RE-PUBLISHES B's
        // [100,200) rows (permanent double count). The re-arm must therefore
        // NOT freeze a `NeedsSeek` at the floor: the restore target has to be
        // resolved against the FRESH committed offset at activation time
        // (max(committed, resume_floor); never below committed).
        let mut gate = ResumeSeekGate::new();
        let mut resume_floor = BTreeMap::new();
        resume_floor.insert(0, 100);
        let mut events = RebalanceEvents::default();
        events.revoked.insert(0);
        events.assigned.insert(0);
        let rearmed =
            rearm_gate_after_rebalance(&mut gate, &BTreeMap::new(), &resume_floor, &events);
        assert_eq!(rearmed, 1);
        assert_ne!(
            gate.state(0),
            SeekGateState::NeedsSeek { target: 100 },
            "the restore point must not be FIXED at the local resume_floor: \
             the group's committed offset (unknowable at re-arm time) may be \
             ahead of it, and seeking below committed re-publishes another \
             member's durable records (R6 H1)"
        );
        // The partition must still be SUPPRESSED until the fresh seek lands.
        assert!(
            !gate.record_delivered(0),
            "a post-rebalance record must stay suppressed until the \
             committed-floor restore seek succeeds"
        );
    }

    #[test]
    fn rebalance_restore_target_never_seeks_below_committed() {
        // Codex R6 H1, the resolution rule: restore = max(committed,
        // resume_floor). `committed` is durable group progress (only ever
        // advanced AFTER a durable publish), so seeking below it re-publishes
        // records some member already published; `resume_floor` is this
        // consumer's own durable-OR-buffered frontier, so seeking below IT
        // re-delivers records already published by (or still buffered in)
        // this consumer. The max of the two avoids both duplications.
        assert_eq!(
            rebalance_restore_target(Some(200), 100),
            200,
            "another member advanced the group to 200 while we were revoked \
             at floor 100: resume at 200, never below committed (R6 H1)"
        );
        assert_eq!(
            rebalance_restore_target(Some(200), 300),
            300,
            "continuous owner with async-commit lag (resume_floor=300 > \
             committed=200): resume at our own durable/buffered frontier — \
             records below it are already published by (or buffered in) US"
        );
        assert_eq!(
            rebalance_restore_target(None, 150),
            150,
            "no commit ever landed: the local durable/buffered floor is the \
             only floor available"
        );
        assert_eq!(
            rebalance_restore_target(Some(70), 70),
            70,
            "equal is a plain position restore"
        );
    }

    #[test]
    fn recreated_partition_restore_is_resume_floor_driven() {
        // Codex R9 C1: the group's committed offset SURVIVES a topic
        // delete+recreate (same group id), but it names a record of the DEAD
        // offset space. Session: recreation detected → the partition sought
        // `low` (0) → the new generation consumed to durable floor 100 → a
        // rebalance revokes+reassigns. The pre-R9 restore resolved
        // `max(committed=800, floor=100) = 800`, and with the new
        // generation produced past 800 the target is IN-range — the clamp
        // cannot catch it — so the new generation's [100,800) is skipped
        // forever (loss). A recreation-detected partition must resolve its
        // restore from `resume_floor` ALONE (always new-generation: records
        // only ever flow after the recreation-floor seek) and never mix the
        // dead committed offset in.
        assert_eq!(
            restore_target(true, Some(800), 100),
            100,
            "dead-generation committed offset must not drive the restore \
             (R9 C1: seeking 800 skips the new generation's [100,800))"
        );
        assert_eq!(
            restore_target(true, None, 100),
            100,
            "recreated + no committed offset: the resume_floor drives"
        );
        // Non-recreated partitions keep the R6 H1 rule unchanged.
        assert_eq!(
            restore_target(false, Some(800), 100),
            800,
            "a live-generation committed offset still wins (R6 H1)"
        );
        assert_eq!(restore_target(false, Some(70), 90), 90);
        // The frontier must EXPOSE the recreation signal the restore needs:
        // a PartitionResume for a recreation-detected partition carries it.
        let resume = PartitionResume::new(0, vec![]);
        assert!(
            !resume.recreated,
            "a plain frontier entry is not recreation-flagged"
        );
        assert!(resume.mark_recreated().recreated);
    }

    #[test]
    fn truncation_reset_restore_ignores_stale_committed_after_regrowth() {
        // Codex R27 H1 — the SAME-topic-id truncation twin of R9 C1: an
        // unclean leader election truncates the log (offset space SHRINKS
        // under the durable frontier) and the topic is later re-produced past
        // the stale committed offset (the offset space REGROWS, re-using the
        // truncated offset numbers). No topic id changes, so `recreated`
        // never arms — yet the group's committed offset predates the
        // truncation and names records of the DEAD offset numbering.
        let partition = 7;

        // Startup: the resume seek target (derived from the stale committed
        // offset / durable evidence) fell PAST the truncated log's end — the
        // R5 H1 clamp catches it and re-consumes from `low`.
        assert_eq!(
            clamp_resume_target(1000, 0, 100),
            0,
            "startup truncation clamp (target > high → low, R5 H1)"
        );
        // Session: the consumer re-consumed the retained log to a
        // durable-OR-buffered floor of 40 (new offset numbering), then the
        // log REGREW to high=2000. The stale committed offset (1000) is back
        // IN-range — the watermark clamp alone can no longer catch it.
        assert_eq!(
            clamp_resume_target(1000, 0, 2000),
            1000,
            "post-regrowth the stale committed offset is in-range: the \
             watermark clamp cannot detect the truncation any more"
        );
        // A rebalance fires. Truncation never set `recreated` (that flag is
        // topic-id evidence only), so the restore MUST get the truncation
        // history from the seek-time clamp instead: the startup `target >
        // high` clamp arms the gate's offset-space-reset memory, it survives
        // activation, and the restore resolution ignores the stale committed
        // offset — `resume_floor` alone drives (re-consume, never skip).
        let mut gate = ResumeSeekGate::new();
        gate.note_offset_space_reset(partition);
        assert!(
            gate.mark_active(partition),
            "the truncation-clamped seek lands and the partition activates"
        );
        assert!(
            gate.offset_space_reset_detected(partition),
            "the truncation memory must SURVIVE activation — the loss window \
             is a rebalance long after the startup seek"
        );
        gate.set_restore(partition, 40);
        let committed_untrusted = gate.offset_space_reset_detected(partition);
        let restored = restore_target(committed_untrusted, Some(1000), 40);
        assert!(
            restored <= 40,
            "restore must not seek past the regrown records [40,1000) — \
             skipping them stamps them durable without ever consuming them \
             (permanent loss, R27 H1); got {restored}"
        );
        assert_eq!(restored, 40, "resume_floor alone drives (bounded dup)");
        // Per-partition isolation: a partition that never saw a truncation
        // clamp keeps the R6 H1 rule unchanged (committed offset wins).
        assert!(!gate.offset_space_reset_detected(8));
        assert_eq!(
            restore_target(gate.offset_space_reset_detected(8), Some(1000), 40),
            1000,
            "no truncation history → live committed progress still wins"
        );
    }

    #[test]
    fn offset_space_reset_memory_survives_rearm_and_activation_cycles() {
        // Codex R27 H1, the gate-state machine: the truncation memory must
        // survive everything the session throws at it — activation, the
        // rebalance re-arm, another activation — because the loss it
        // prevents can be an arbitrary number of rebalances downstream.
        let mut gate = ResumeSeekGate::new();
        gate.note_offset_space_reset(3);

        // Startup: clamped seek lands, partition activates.
        assert!(gate.mark_active(3));
        assert!(gate.offset_space_reset_detected(3));

        // Rebalance: the real re-arm path (revoke+reassign with in-session
        // progress) re-gates the partition to a RESTORE seek — and must not
        // touch the truncation memory.
        let frontier: BTreeMap<i32, PartitionResume> = BTreeMap::new();
        let mut floors = BTreeMap::new();
        floors.insert(3, 40_i64);
        floors.insert(9, 500_i64);
        let events = RebalanceEvents {
            revoked: [3, 9].into_iter().collect(),
            assigned: [3, 9].into_iter().collect(),
            lost_all: false,
        };
        assert_eq!(
            rearm_gate_after_rebalance(&mut gate, &frontier, &floors, &events),
            2
        );
        assert_eq!(
            gate.state(3),
            SeekGateState::NeedsRestore { resume_floor: 40 }
        );
        assert!(
            gate.offset_space_reset_detected(3),
            "rearm must not clear the truncation memory"
        );
        // The untouched partition is unaffected (per-partition isolation).
        assert!(!gate.offset_space_reset_detected(9));

        // The restore resolution (as `try_activate_pending` computes it):
        // recreated is false for both — only the reset partition ignores the
        // stale committed offset.
        let recreated = false;
        assert_eq!(
            restore_target(
                recreated || gate.offset_space_reset_detected(3),
                Some(1000),
                40
            ),
            40,
            "reset partition: resume_floor alone (never a forward skip)"
        );
        assert_eq!(
            restore_target(
                recreated || gate.offset_space_reset_detected(9),
                Some(1000),
                500
            ),
            1000,
            "normal partition: R6 H1 max(committed, resume_floor) unchanged"
        );

        // A second activation cycle still keeps the memory.
        assert!(gate.mark_active(3));
        assert!(gate.offset_space_reset_detected(3));
        // Idempotent re-recording is harmless.
        gate.note_offset_space_reset(3);
        assert!(gate.offset_space_reset_detected(3));
    }

    #[test]
    fn resolve_seek_target_reset_floor_pins_low_recreation_and_normal_unchanged() {
        // Codex R27 H1 (a)/(b), the pure decision core: the seek-target
        // resolver applied inside `clamped_seek_target`.
        //
        // RESET FLOOR (the fix): an offset-space reset was witnessed this
        // session, the incoming `target` (1000) came from a now-untrusted
        // committed offset / old-numbering coverage, and the log REGREW to
        // high=2000 so 1000 is back IN-range. A plain clamp would trust it —
        let stale = 1000;
        assert_eq!(
            clamp_resume_target(stale, 0, 2000),
            stale,
            "documented loss (pre-fix): the regrown stale target is in-range, \
             so a plain clamp forward-seeks past the regrown [0,1000)"
        );
        // — but the reset-floor resolver pins the seek at the live `low`:
        let (target, shrank) = resolve_seek_target(None, true, stale, 0, 2000);
        assert_eq!(
            target, 0,
            "reset-floor seek is pinned at low: re-consume [0,2000), never a \
             forward skip past regrown records (R27 H1 (a)/(b))"
        );
        assert!(
            !shrank,
            "an already-recorded reset does not re-signal offset_space_shrank"
        );
        // Degenerate window still collapses safely to low.
        assert_eq!(
            resolve_seek_target(None, true, stale, 700, 600),
            (700, false)
        );

        // RECREATION (R9 C2) takes precedence and is unchanged: floor low +
        // advance through live coverage, regardless of `reset_floor`.
        let rec = PartitionResume::new(400, vec![OffsetSpan::new(400, 800)]).mark_recreated();
        assert_eq!(
            resolve_seek_target(Some(&rec), true, stale, 400, 800),
            (800, false),
            "recreation derivation wins over reset_floor and is unchanged"
        );

        // NORMAL path (no reset, no recreation) is unchanged: plain clamp,
        // and a first-seen `target > high` signals the truncation to record.
        assert_eq!(
            resolve_seek_target(None, false, 1500, 0, 1000),
            (0, true),
            "first-seen truncation: clamp to low AND signal offset_space_shrank"
        );
        assert_eq!(
            resolve_seek_target(None, false, 451, 0, 1000),
            (451, false),
            "in-range target on a healthy log is trusted (normal path)"
        );
    }

    #[test]
    fn reset_floor_arming_survives_failed_seek_and_disarms_at_activation() {
        // Codex R27 H1 (a): after a truncation-clamped seek FAILS, the gate
        // retains its PRE-clamp stale NeedsSeek target — so on a regrowth the
        // per-seek reset floor is what keeps the retry pinned at low. The
        // arming must therefore SURVIVE a failed (un-activated) seek and be
        // cleared only when the seek finally lands (activation).
        let stale = 1000;
        let mut gate = ResumeSeekGate::new();
        // Initial derivation produced a NeedsSeek{stale}; the clamp detected
        // the shrink and armed both the persistent memory and the per-seek
        // reset floor (as `try_activate_pending` does at detection time).
        gate.set_target(7, stale);
        gate.note_offset_space_reset(7);
        gate.arm_reset_floor(7);
        assert!(gate.reset_floor_armed(7), "armed at detection");

        // The seek FAILED: the partition is still gated at the stale target
        // (a failed seek never rewrites the gate) and the floor stays armed,
        // so the NEXT retry consults it (call site: reset_floor =
        // recreation.is_none() && reset_floor_armed).
        assert_eq!(gate.state(7), SeekGateState::NeedsSeek { target: stale });
        assert!(
            gate.reset_floor_armed(7),
            "the per-seek floor MUST survive a failed seek — else a regrowth \
             before the retry would let the stale target forward-seek (loss)"
        );
        // Post-regrowth the retry is pinned at low (the fix), never the stale
        // in-range value.
        assert_eq!(resolve_seek_target(None, true, stale, 0, 2000).0, 0);

        // Activation lands the low seek → the per-seek floor disarms, but the
        // persistent truncation memory (for the restore path) stays.
        assert!(gate.mark_active(7));
        assert!(
            !gate.reset_floor_armed(7),
            "per-seek floor disarms on activation"
        );
        assert!(
            gate.offset_space_reset_detected(7),
            "the persistent restore-path memory outlives activation (primary R27)"
        );
    }

    #[test]
    fn reset_floor_arms_on_rebalance_rederivation_before_activation() {
        // Codex R27 H1 (b): a rebalance fires BEFORE the truncation-clamped
        // seek ever activated. With no in-session progress the partition has
        // no restore floor, so `rearm_gate_after_rebalance` re-derives it via
        // `require_target` (→ NeedsTarget), NOT `set_restore`. The NeedsTarget
        // re-derivation would walk the OLD-numbering coverage + stale
        // committed offset; after a regrowth that lands in-range and a plain
        // clamp forward-seeks past regrown records (loss).
        let mut gate = ResumeSeekGate::new();
        gate.note_offset_space_reset(4); // reset witnessed earlier this session

        // Frontier has the partition (durable evidence exists), but nothing
        // was consumed in-session yet ⇒ empty resume-floor map ⇒ the rearm
        // path takes `require_target`.
        let mut frontier: BTreeMap<i32, PartitionResume> = BTreeMap::new();
        frontier.insert(4, PartitionResume::new(0, vec![OffsetSpan::new(0, 900)]));
        let floors: BTreeMap<i32, i64> = BTreeMap::new();
        let events = RebalanceEvents {
            revoked: [4].into_iter().collect(),
            assigned: [4].into_iter().collect(),
            lost_all: false,
        };
        assert_eq!(
            rearm_gate_after_rebalance(&mut gate, &frontier, &floors, &events),
            1
        );
        assert_eq!(
            gate.state(4),
            SeekGateState::NeedsTarget,
            "no in-session progress ⇒ require_target (not a restore)"
        );
        // The persistent reset memory survives the rearm — this is what the
        // NeedsTarget branch consults to arm the per-seek reset floor.
        assert!(gate.offset_space_reset_detected(4));

        // Model the NeedsTarget-branch decision: because the reset is on
        // record, the branch arms the per-seek reset floor and does NOT trust
        // the committed offset / old coverage.
        assert!(
            gate.offset_space_reset_detected(4),
            "the NeedsTarget branch keys the reset-floor arming off this flag"
        );
        gate.arm_reset_floor(4);
        assert!(gate.reset_floor_armed(4));
        // Old-numbering coverage would have advanced to 900; after a regrowth
        // to high=3000 that is in-range and a plain clamp would forward-seek
        // (loss). The reset floor pins low instead.
        let old_coverage_target = advance_contiguous(0, &frontier[&4].loaded); // = 900
        assert_eq!(old_coverage_target, 900);
        assert_eq!(
            clamp_resume_target(old_coverage_target, 0, 3000),
            900,
            "documented loss (pre-fix): old-coverage target is in-range after \
             regrowth → forward skip of [0,900)"
        );
        assert_eq!(
            resolve_seek_target(None, true, old_coverage_target, 0, 3000).0,
            0,
            "reset floor pins the pre-activation rebalance re-derivation at low"
        );

        // A partition with NO reset on record keeps the normal require_target
        // behavior (control): its target is trusted.
        assert!(!gate.reset_floor_armed(9));
        assert_eq!(resolve_seek_target(None, false, 900, 0, 3000).0, 900);
    }

    #[test]
    fn recreated_resume_target_floors_at_low_and_advances_live_coverage() {
        // Codex R9 C2, the pure core: recreation detected, live coverage
        // [400,800) (the [300,400) publish FAILED after the recreation).
        let r = PartitionResume::new(400, vec![OffsetSpan::new(400, 800)]).mark_recreated();
        assert_eq!(
            recreated_resume_target(300, 800, &r),
            300,
            "a [low, coverage) gap pins the resume at low — the live row must \
             not lift the floor past the never-durable [300,400) (loss)"
        );
        assert_eq!(
            recreated_resume_target(400, 800, &r),
            800,
            "coverage CONTIGUOUS from low is skipped — no per-restart \
             re-consume + re-publish of durable rows (R8 H1 preserved)"
        );
        assert_eq!(
            recreated_resume_target(0, 800, &r),
            0,
            "low = 0 with coverage starting at 400: re-consume from 0"
        );
        // Multi-span coverage: contiguous prefix advances, first hole stops.
        let r = PartitionResume::new(
            0,
            vec![OffsetSpan::new(100, 300), OffsetSpan::new(350, 500)],
        )
        .mark_recreated();
        assert_eq!(
            recreated_resume_target(100, 900, &r),
            300,
            "advance through [100,300), stop at the [300,350) hole"
        );
        // No live coverage (dead-only partition): resume at low.
        let r = PartitionResume::new(0, vec![]).mark_recreated();
        assert_eq!(recreated_resume_target(250, 900, &r), 250);
        // The log shrank AGAIN below the walked target (second
        // truncation/recreation): re-clamp to low (R5 H1 semantics).
        let r = PartitionResume::new(0, vec![OffsetSpan::new(0, 900)]).mark_recreated();
        assert_eq!(recreated_resume_target(0, 600, &r), 0);
        // Caught-up boundary: coverage ending exactly at high is in-range.
        let r = PartitionResume::new(0, vec![OffsetSpan::new(0, 600)]).mark_recreated();
        assert_eq!(recreated_resume_target(0, 600, &r), 600);
    }

    #[test]
    fn resume_offset_recreated_ignores_committed_and_min_start() {
        // Codex R9: the floor-0 placeholder — neither the (possibly dead)
        // committed offset nor the live min_start may gate the walk.
        let r = PartitionResume::new(400, vec![OffsetSpan::new(400, 800)]).mark_recreated();
        assert_eq!(
            resume_offset(Some(800), &r),
            0,
            "dead committed 800 and live min_start 400 are both untrusted: \
             the placeholder floors at 0 (clamped to low at seek time)"
        );
        assert_eq!(resume_offset(None, &r), 0);
        // Coverage contiguous from 0 still advances the placeholder (equal
        // to the low=0 derivation).
        let r = PartitionResume::new(0, vec![OffsetSpan::new(0, 300)]).mark_recreated();
        assert_eq!(resume_offset(Some(500), &r), 300);
        // Control: the same shape WITHOUT the flag keeps the C1/C2 rule.
        let r = PartitionResume::new(400, vec![OffsetSpan::new(400, 800)]);
        assert_eq!(resume_offset(Some(800), &r), 800);
        assert_eq!(resume_offset(Some(200), &r), 200);
    }

    #[test]
    fn synthesize_recreated_floors_evidence_less_partitions_at_low() {
        // Codex R18 C1/C2 (streaming half): under a DETECTED topic
        // recreation, an assigned partition with NO durable frontier entry
        // must be low-floored — its committed offset survives the
        // delete+recreate but names the DEAD generation's offset space.
        let mut f = ResumeFrontier {
            partitions: BTreeMap::new(),
            topic_recreated: true,
        };
        assert_eq!(f.synthesize_recreated([0, 3]), 2);
        for p in [0, 3] {
            let r = f.partitions.get(&p).expect("synthesized");
            assert!(r.recreated);
            assert!(r.loaded.is_empty(), "FLOOR-only: no coverage to skip past");
            assert_eq!(
                resume_offset(Some(9_999), r),
                0,
                "the stale committed offset never gates the placeholder"
            );
            assert_eq!(
                recreated_resume_target(250, 900, r),
                250,
                "the seek-time derivation lands exactly on `low` — the \
                 retained log is re-consumed (bounded duplication), never skipped"
            );
        }
        // Idempotent + never clobbers real evidence.
        let live = PartitionResume::new(400, vec![OffsetSpan::new(400, 800)]).mark_recreated();
        f.partitions.insert(5, live.clone());
        assert_eq!(f.synthesize_recreated([3, 5]), 0);
        assert_eq!(f.partitions.get(&5), Some(&live));

        // Control (normal path unchanged): without a detected recreation the
        // synthesis is inert — a frontier-less partition keeps its committed
        // offset / auto.offset.reset first-start semantics.
        let mut normal = ResumeFrontier::default();
        assert_eq!(normal.synthesize_recreated([0, 1]), 0);
        assert!(normal.partitions.is_empty());
    }

    #[test]
    fn rebalance_assign_under_recreation_gates_evidence_less_partitions() {
        // Codex R18 C1/C2: a partition (re-)assigned mid-session with no
        // durable frontier entry. Pre-R18 the rearm left it as an "untouched
        // free partition" — it consumed straight from the DEAD generation's
        // committed offset, silently skipping every new-generation record
        // below it. With the synthesis step it is gated (NeedsTarget) until
        // its low-watermark floor seek lands, and its records are suppressed
        // meanwhile.
        let mut f = ResumeFrontier {
            partitions: BTreeMap::new(),
            topic_recreated: true,
        };
        let events = RebalanceEvents {
            revoked: std::collections::BTreeSet::new(),
            assigned: [7].into_iter().collect(),
            lost_all: false,
        };
        let mut gate = ResumeSeekGate::new();
        // The drain path synthesizes FIRST, then re-arms against the map.
        assert_eq!(f.synthesize_recreated(events.assigned.iter().copied()), 1);
        let rearmed =
            rearm_gate_after_rebalance(&mut gate, &f.partitions, &BTreeMap::new(), &events);
        assert_eq!(rearmed, 1);
        assert!(
            matches!(gate.state(7), SeekGateState::NeedsTarget),
            "the assigned partition must be gated for a fresh authoritative \
             (recreation-floor) seek"
        );
        assert!(
            !gate.record_delivered(7),
            "its records are suppressed until the low-floor seek lands"
        );

        // Control (normal path unchanged): no recreation → no synthesis →
        // the rearm leaves the evidence-less partition free, exactly the
        // pre-R18 first-start semantics.
        let mut f = ResumeFrontier::default();
        let mut gate = ResumeSeekGate::new();
        assert_eq!(f.synthesize_recreated(events.assigned.iter().copied()), 0);
        let rearmed =
            rearm_gate_after_rebalance(&mut gate, &f.partitions, &BTreeMap::new(), &events);
        assert_eq!(rearmed, 0);
        assert!(matches!(gate.state(7), SeekGateState::Active));
        assert!(gate.record_delivered(7));
    }

    #[test]
    fn gate_recreation_floor_arms_and_disarms_once() {
        // Codex R9 C1: the dead-committed overwrite must fire EXACTLY once
        // per arming — on the successful floor seek — and a restore seek
        // must never inherit the marker.
        let mut gate = ResumeSeekGate::new();
        gate.require_target(0);
        assert!(!gate.recreation_floor_armed(0));
        gate.arm_recreation_floor(0);
        assert!(gate.recreation_floor_armed(0));
        assert!(
            gate.disarm_recreation_floor(0),
            "first disarm reports armed → the caller overwrites the dead \
             committed offset"
        );
        assert!(
            !gate.disarm_recreation_floor(0),
            "second disarm is a no-op → no repeated overwrite commits"
        );
        // Activation clears a still-armed marker (e.g. the failed-seek
        // committed==target carve-out activated without a floor seek): an
        // Active partition has no pending seek to re-derive.
        gate.arm_recreation_floor(0);
        assert!(gate.mark_active(0));
        assert!(!gate.recreation_floor_armed(0));
        // A later rebalance re-arms via NeedsRestore: resume_floor-driven,
        // never the floor marker.
        gate.set_restore(0, 120);
        assert!(!gate.recreation_floor_armed(0));
        assert_eq!(
            gate.state(0),
            SeekGateState::NeedsRestore { resume_floor: 120 }
        );
    }

    #[test]
    fn contiguous_frontier_clamped_seek_resets_base_or_never_commits() {
        // Codex R6 H2 (retention direction): the partition consumed from 100
        // and committed 150; retention then deleted [150,300) and the
        // authoritative re-seek was CLAMPED to low=300. Without re-basing,
        // every new durable span starts at 300 — ABOVE the stale base 150 —
        // so `record_durable` reads it as a gap forever: nothing is EVER
        // committed again, and every restart re-consumes + re-publishes the
        // whole retained log (repeating durable duplication). The [150,300)
        // records no longer exist ANYWHERE, so there is nothing for the pin
        // to protect: the base must move to the clamped seek position.
        let mut f = ContiguousFrontier::new();
        f.note_consumed(0, 100);
        let mut spans = PartitionOffsets::new();
        spans.insert(0, OffsetSpan::new(100, 150));
        assert_eq!(
            f.record_durable(&spans).get(&0),
            Some(&150),
            "pre-clamp publish commits normally"
        );
        // Reproduce the R6 H2 never-commit: post-clamp spans are a permanent
        // gap against the stale base.
        let mut post = PartitionOffsets::new();
        post.insert(0, OffsetSpan::new(300, 400));
        assert!(
            f.record_durable(&post).is_empty(),
            "without re-basing, the clamped consume position never commits \
             (the R6 H2 bug this fix removes)"
        );
        // The fix: the clamped seek resets the base to the actual seek
        // position, so consumption from there commits contiguously again.
        f.reset_base(0, 300);
        let mut healed = PartitionOffsets::new();
        healed.insert(0, OffsetSpan::new(300, 400));
        assert_eq!(
            f.record_durable(&healed).get(&0),
            Some(&400),
            "after the clamp re-base, the new durable span is contiguous \
             from the clamped seek position and commits (R6 H2)"
        );
    }

    #[test]
    fn contiguous_frontier_recreate_clamp_rebases_downward() {
        // Codex R6 H2 (recreate direction): the durable/committed frontier
        // sat at 1500 when the topic was deleted+recreated — the offset
        // space SHRANK and the clamp seeks `low`=0 (R5 H1). The stale base
        // 1500 is then ABOVE every new-space span: `[0,100)` has
        // `next <= base`, so it looks "already committed" and nothing is
        // ever committed in the new offset space. Re-basing to the clamped
        // seek position (0) restores contiguous commits for the new space.
        let mut f = ContiguousFrontier::new();
        f.note_consumed(0, 1_400);
        let mut spans = PartitionOffsets::new();
        spans.insert(0, OffsetSpan::new(1_400, 1_500));
        assert_eq!(f.record_durable(&spans).get(&0), Some(&1_500));
        let mut new_space = PartitionOffsets::new();
        new_space.insert(0, OffsetSpan::new(0, 100));
        assert!(
            f.record_durable(&new_space).is_empty(),
            "new-space spans below the stale base never commit (R6 H2 repro)"
        );
        f.reset_base(0, 0);
        let mut healed = PartitionOffsets::new();
        healed.insert(0, OffsetSpan::new(0, 100));
        assert_eq!(
            f.record_durable(&healed).get(&0),
            Some(&100),
            "after the recreate-clamp re-base, new-space spans commit"
        );
    }

    #[test]
    fn publish_identity_gate_blocks_only_confirmed_drift() {
        // Codex R6 H4: the sink stamps the START-time cluster id on every
        // durable segment, so the moment before a publish is the last chance
        // to notice that the live cluster is no longer the one the stamp
        // names. The pre-publish verdict must block ONLY a CONFIRMED drift:
        //   * live known and != expected → refuse (stopping beats stamping
        //     cluster B's rows with cluster A's provenance);
        //   * live known and == expected → publish (the stable-path
        //     regression);
        //   * live unresolvable → publish (fail-safe, same polarity as the
        //     R37 F3 tripwire — a transient metadata gap must never wedge
        //     publishing);
        //   * nothing stamped at start (expected None) → publish (there is
        //     no identity to protect; the cleanup keeps such rows anyway).
        assert!(
            publish_blocked_by_identity_drift(Some("kc-B"), Some("kc-A")),
            "confirmed drift must refuse the publish (R6 H4)"
        );
        assert!(
            !publish_blocked_by_identity_drift(Some("kc-A"), Some("kc-A")),
            "stable identity must keep publishing + stamping (regression)"
        );
        assert!(
            !publish_blocked_by_identity_drift(None, Some("kc-A")),
            "unresolvable live id is NOT drift (fail-safe polarity)"
        );
        assert!(
            !publish_blocked_by_identity_drift(Some("kc-B"), None),
            "an unstamped session has no identity to protect"
        );
        assert!(
            !publish_blocked_by_identity_drift(None, None),
            "nothing known on either side → publish"
        );
    }

    #[test]
    fn schema_fingerprint_canonicalizes_metric_key_order() {
        // The workspace enables serde_json/preserve_order, so two
        // semantically identical metricsSpec entries POSTed with different
        // key orders would leak that order into a naive serialization —
        // spuriously refusing a re-create. The canonical writer must sort
        // object keys.
        let base = fp_spec();
        let mut reordered = fp_spec();
        reordered.data_schema.metrics_spec = vec![serde_json::json!({
            "fieldName": "bytes", "name": "bytes", "type": "doubleSum"
        })];
        assert_eq!(
            schema_fingerprint(&base),
            schema_fingerprint(&reordered),
            "metric key order must not affect the fingerprint"
        );
    }
}
