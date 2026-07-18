// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Sequence-number resume frontier (compat-5 durability).
//!
//! Mirrors the Kafka `ResumeFrontier` / `PartitionResume` / `OffsetSpan`
//! model with the two Kinesis-specific retypes:
//!
//! 1. **Positions are opaque decimal strings**, not `i64`: a real
//!    Kinesis sequence number is ~56 decimal digits — too large for
//!    `u128` (39 digits max) — and ordered only WITHIN a shard.
//!    [`SeqNum`] compares them as arbitrary-precision non-negative
//!    integers (length-then-lexicographic on the canonical digits — no
//!    bignum dependency needed).
//! 2. **Adjacency is undecidable in sequence space**: Kafka offsets are
//!    dense (`[start, next)` arithmetic detects holes); Kinesis sequence
//!    numbers are sparse, so "no records between span A and span B" can
//!    NEVER be inferred from the numbers alone. Each [`SeqSpan`]
//!    therefore carries an explicit chain link — `prev_last`, the last
//!    sequence number consumed BEFORE the span's first record — and the
//!    resume walk only advances through coverage whose chain is intact
//!    AND rooted at a genuine ANCHOR (`prev_last == None`: the first
//!    batch ever read on the shard, proving nothing below it was ever
//!    consumed). A broken chain (a failed roll's dropped batch, a
//!    phantom row whose blob vanished) stops the walk, and an
//!    UNANCHORED walk falls back to `TRIM_HORIZON`, so the missing
//!    records are RE-CONSUMED rather than skipped: at-least-once
//!    duplication, never loss — the same no-loss priority every Kafka
//!    resume derivation shares.
//!
//! The durable checkpoint lives in the segment metadata row's
//! `payload.kinesisSequences` (the analogue of `payload.kafkaOffsets`):
//! Kinesis has NO server-side per-consumer checkpoint (no committed
//! offset), so the durable segment set is the ONLY checkpoint store and
//! [`fold_resume_frontier`] always operates with `committed = None` in
//! Kafka terms — the durable frontier alone is authoritative.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::source::StartPosition;

/// Payload key carrying the per-shard [`SeqSpan`]s a durable streaming
/// segment covers (the Kinesis analogue of `kafkaOffsets`).
pub const SEQUENCES_PAYLOAD_KEY: &str = "kinesisSequences";
/// The `payload.kind` value stamped on Kinesis streaming segments.
pub const KINESIS_STREAMING_KIND: &str = "kinesis-streaming";
/// Payload key naming the source stream.
pub const STREAM_PAYLOAD_KEY: &str = "stream";
/// Payload key carrying the stream-generation marker
/// ([`StreamIdentity::stream_creation_timestamp_millis`](crate::source::StreamIdentity)) —
/// the recreation detector (the ARN is REUSED on same-name recreate, so
/// it cannot serve this role).
pub const STREAM_CREATION_PAYLOAD_KEY: &str = "streamCreationTimestampMillis";

/// Errors from frontier / sequence-number handling.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrontierError {
    /// A sequence number string was empty or contained a non-digit.
    #[error("invalid kinesis sequence number {0:?}: must be a non-empty decimal digit string")]
    InvalidSequenceNumber(String),
}

/// An opaque Kinesis sequence number, compared as an arbitrary-precision
/// non-negative decimal integer.
///
/// Stored in CANONICAL form (leading zeros stripped), so derived
/// equality/hashing agree with numeric ordering. Serializes as the
/// canonical decimal string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct SeqNum(String);

impl SeqNum {
    /// Parse a decimal digit string into a sequence number.
    ///
    /// # Errors
    /// [`FrontierError::InvalidSequenceNumber`] if `s` is empty or
    /// contains a non-digit character (Kinesis sequence numbers are
    /// non-negative decimals; `-`, `+`, whitespace all rejected).
    pub fn parse(s: &str) -> Result<Self, FrontierError> {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(FrontierError::InvalidSequenceNumber(s.to_owned()));
        }
        let canonical = s.trim_start_matches('0');
        let canonical = if canonical.is_empty() { "0" } else { canonical };
        Ok(Self(canonical.to_owned()))
    }

    /// The canonical decimal string (what gets sent back to the Kinesis
    /// API as `StartingSequenceNumber` and stamped into payloads).
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Ord for SeqNum {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Canonical digits: a longer number is strictly larger; equal
        // lengths compare lexicographically == numerically.
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.cmp(&other.0))
    }
}

impl PartialOrd for SeqNum {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for SeqNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for SeqNum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// The INCLUSIVE sequence span `[start, last]` one rolled segment covers
/// on one shard, plus the explicit chain link that makes contiguity
/// decidable (see the module docs — sequence space is sparse, so
/// arithmetic adjacency does not exist).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeqSpan {
    /// First sequence number included (inclusive).
    pub start: SeqNum,
    /// Last sequence number included (inclusive) — resuming AFTER this
    /// is safe only if the chain from a genuine anchor is intact.
    pub last: SeqNum,
    /// The last sequence number the consumer had consumed BEFORE this
    /// span's first record: `None` when the span began at the consumer's
    /// start position (TRIM_HORIZON / LATEST), `Some(prev)` when it
    /// chains onto earlier consumption (the previous roll's `last`, or
    /// the resume point). A resume walk only advances across span
    /// boundaries whose `prev_last` equals the walk point — anything
    /// else is a potential dropped-batch hole and is re-consumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_last: Option<SeqNum>,
}

impl SeqSpan {
    /// Construct a span covering `[start, last]` with its chain link.
    #[must_use]
    pub fn new(start: SeqNum, last: SeqNum, prev_last: Option<SeqNum>) -> Self {
        Self {
            start,
            last,
            prev_last,
        }
    }
}

/// Per-shard sequence spans covered by ONE rolled segment, keyed by
/// shard id — the value serialized under
/// [`SEQUENCES_PAYLOAD_KEY`] in the published segment's metadata payload
/// (the analogue of Kafka's `PartitionOffsets`).
pub type ShardSequences = BTreeMap<String, SeqSpan>;

/// Serialize a segment's [`ShardSequences`] to the JSON value stamped
/// under [`SEQUENCES_PAYLOAD_KEY`]. Round-trips through
/// [`fold_resume_frontier`].
#[must_use]
pub fn sequences_to_payload(sequences: &ShardSequences) -> serde_json::Value {
    // ShardSequences is (String → SeqSpan) with only string/None fields:
    // serialization cannot fail.
    serde_json::to_value(sequences).unwrap_or(serde_json::Value::Null)
}

/// Per-shard durable-resume evidence (the Kinesis `PartitionResume`):
/// the folded spans of the rows that actually reloaded — the only
/// coverage that can EVER be skipped past, and only when it chains back
/// to a genuine anchor (see
/// [`resume_start_position`](Self::resume_start_position)).
///
/// The mere EXISTENCE of a `ShardResume` for a shard also matters: it
/// records that durable evidence exists for the shard, so
/// [`KinesisResumeFrontier::start_position_for`] never falls back to
/// the spec default (a `LATEST` default could seek past re-consumable
/// records) — a shard with evidence but no anchored coverage resumes
/// from `TRIM_HORIZON`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardResume {
    /// The shard this evidence belongs to.
    pub shard_id: String,
    /// Folded, `start`-sorted spans of the rows that ACTUALLY reloaded
    /// (identity-confirmed). Only an unbroken chain of these from a
    /// genuine anchor is skipped past.
    pub loaded: Vec<SeqSpan>,
}

impl ShardResume {
    /// Build from the reloaded spans, folding them (see [`fold_spans`])
    /// so [`resume_start_position`](Self::resume_start_position) can
    /// walk them once.
    #[must_use]
    pub fn new(shard_id: impl Into<String>, loaded: Vec<SeqSpan>) -> Self {
        Self {
            shard_id: shard_id.into(),
            loaded: fold_spans(loaded),
        }
    }

    /// Compute where the shard must RESUME consuming, from the durable
    /// evidence alone (Kinesis has no committed offset — the durable
    /// frontier is always authoritative, the Kafka `committed = None`
    /// case).
    ///
    /// **Loss-safety invariant (Codex R1-C1):**
    /// [`StartPosition::AfterSequenceNumber`]`(P)` is returned ONLY when
    /// an UNBROKEN chain of loaded spans runs from a genuine ANCHOR — a
    /// span with `prev_last == None`, i.e. the first batch ever read on
    /// this shard (from `TRIM_HORIZON` / `LATEST`), which proves the
    /// consumer never consumed anything below it — up to the covered
    /// point `P`. A durable span that is NOT chained back to the genuine
    /// beginning of consumption cannot license skipping records below
    /// it: its `prev_last = Some(_)` proves records WERE consumed below
    /// it that no durable span covers (a failed leading roll's dropped
    /// batch). In every such case — unanchored lowest span, phantom-only
    /// evidence, no loaded spans at all — the shard resumes from
    /// [`StartPosition::TrimHorizon`], re-consuming the retained log:
    /// bounded at-least-once duplication, NEVER loss.
    ///
    /// A genuine anchor span is itself durable and persists across
    /// restarts, so normal multi-restart resume still returns
    /// `AfterSequenceNumber` efficiently (the anchor is always in the
    /// folded frontier); only the failed-leading-roll / broken-chain /
    /// phantom cases pay the `TRIM_HORIZON` re-consume.
    #[must_use]
    pub fn resume_start_position(&self) -> StartPosition {
        // Walk the folded coverage from the anchor. `point` = the last
        // sequence number proven durably covered by an anchored chain.
        let mut point: Option<&SeqNum> = None;
        for span in &self.loaded {
            let chains = match point {
                // The first interval must be a genuine ANCHOR: nothing
                // was ever consumed before it, so nothing below it can
                // be unpublished. A lowest span with `prev_last =
                // Some(_)` sits ABOVE consumed-but-never-durable
                // records — trusting it would skip them (loss).
                None => span.prev_last.is_none(),
                // Later intervals must either chain in RECORD space
                // (their `prev_last` IS the walk point) or overlap
                // numerically; a mismatched link is a potential
                // dropped-batch hole and stops the walk.
                Some(p) => span.prev_last.as_ref() == Some(p) || span.start <= *p,
            };
            if !chains {
                break;
            }
            if point.is_none_or(|p| span.last > *p) {
                point = Some(&span.last);
            }
        }
        match point {
            None => StartPosition::TrimHorizon,
            Some(p) => StartPosition::AfterSequenceNumber(p.clone()),
        }
    }
}

/// Fold spans into non-overlapping, `start`-sorted intervals, merging
/// (a) numeric overlaps and (b) CHAIN-adjacent spans (`b.prev_last ==
/// Some(a.last)`), so a resume walk visits each interval once.
/// Degenerate spans (`last < start`) are dropped.
#[must_use]
pub fn fold_spans(spans: Vec<SeqSpan>) -> Vec<SeqSpan> {
    let mut spans: Vec<SeqSpan> = spans
        .into_iter()
        .filter(|s| s.last >= s.start) // drop degenerate
        .collect();
    spans.sort_by(|a, b| a.start.cmp(&b.start).then_with(|| a.last.cmp(&b.last)));
    let mut merged: Vec<SeqSpan> = Vec::with_capacity(spans.len());
    for span in spans {
        match merged.last_mut() {
            // Numeric overlap, or an intact chain link in record space
            // (`span.prev_last` IS the previous interval's `last`) →
            // extend the previous interval; the merged interval keeps
            // the HEAD's `prev_last` (its own upstream link).
            Some(prev)
                if span.start <= prev.last || span.prev_last.as_ref() == Some(&prev.last) =>
            {
                if span.last > prev.last {
                    prev.last = span.last;
                }
            }
            _ => merged.push(span),
        }
    }
    merged
}

/// One segment-metadata row's worth of fold input: the row's JSON
/// `payload` plus whether its blob actually reloaded into the
/// Historical (`loaded`). Rows that did not reload FLOOR the frontier
/// but never advance it (phantom-row protection).
#[derive(Debug, Clone, Copy)]
pub struct FrontierRowEvidence<'a> {
    /// The segment row's `payload` JSON.
    pub payload: &'a serde_json::Value,
    /// Whether the segment's blob is confirmed reloaded and queryable.
    pub loaded: bool,
}

/// The complete durable-resume directive for one `(datasource, stream)`
/// pair (the Kinesis `ResumeFrontier`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KinesisResumeFrontier {
    /// Per-shard durable resume evidence, keyed by shard id.
    pub shards: BTreeMap<String, ShardResume>,
    /// A stream delete+recreate was POSITIVELY detected: some durable
    /// row is stamped with a creation timestamp DIFFERENT from the
    /// current stream's. Every sequence number in the durable evidence
    /// then names a DEAD generation's record — the whole frontier is
    /// untrusted and every shard resumes from `TRIM_HORIZON`
    /// (re-consuming the retained log: bounded at-least-once
    /// duplication, never loss).
    pub stream_recreated: bool,
    /// A durable checkpoint row for this stream was PRESENT but its
    /// `kinesisSequences` value was not an object (corrupt / unreadable),
    /// so the shards it named cannot be enumerated. Rather than let those
    /// shards fall back to the spec `LATEST` default (and skip records
    /// after the last durable segment), the WHOLE frontier is condemned
    /// to `TRIM_HORIZON` — the same all-shards floor as `stream_recreated`
    /// (Codex R3 C1).
    pub checkpoint_corrupt: bool,
}

impl KinesisResumeFrontier {
    /// The start position shard `shard_id` must use, falling back to
    /// `default` (the spec-derived position) for shards with no durable
    /// evidence. Under a detected recreation EVERY shard — evidence or
    /// not — starts at `TRIM_HORIZON` (see
    /// [`stream_recreated`](Self::stream_recreated)).
    #[must_use]
    pub fn start_position_for(&self, shard_id: &str, default: StartPosition) -> StartPosition {
        if self.stream_recreated || self.checkpoint_corrupt {
            return StartPosition::TrimHorizon;
        }
        self.shards
            .get(shard_id)
            .map_or(default, ShardResume::resume_start_position)
    }
}

/// Fold a [`KinesisResumeFrontier`] from durable segment-metadata
/// payloads (the rows the overlord loads for the datasource; only
/// `used` rows must be passed — administratively disabled rows never
/// enter the frontier, mirroring the Kafka discipline).
///
/// Row admission (mirrors `compute_resume_frontier`'s gating, folded
/// down to the Kinesis identity model):
///
/// * rows whose `payload.kind` != [`KINESIS_STREAMING_KIND`] or whose
///   `payload.stream` != `stream` are ignored (another pipeline's rows);
/// * a row stamped with a creation timestamp EQUAL to
///   `current_creation_millis` is identity-confirmed: it ADMITS the
///   shard into the frontier and, if `loaded`, its spans may ADVANCE
///   the resume (when they chain from a genuine anchor — see
///   [`ShardResume::resume_start_position`]);
/// * a row stamped with a DIFFERENT creation timestamp is a DEAD
///   generation's: excluded from BOTH roles and the whole frontier is
///   marked [`stream_recreated`](KinesisResumeFrontier::stream_recreated);
/// * an UNSTAMPED row, or an unresolved current identity
///   (`current_creation_millis = None`), or a MALFORMED stamp is
///   evidence-only — it admits the shard (so the spec default is never
///   used) but grants NO skip permission: without anchored loaded
///   coverage the shard re-consumes from `TRIM_HORIZON`;
/// * a malformed span (missing / non-decimal `start` or `last`) is
///   evidence-only via whatever parses; if nothing parses the row is
///   skipped loudly (warn), never silently.
#[must_use]
pub fn fold_resume_frontier(
    rows: &[FrontierRowEvidence<'_>],
    stream: &str,
    current_creation_millis: Option<i64>,
) -> KinesisResumeFrontier {
    use serde_json::Value;
    // Shards with ANY durable evidence: they must never fall back to
    // the spec default (a LATEST default could seek past re-consumable
    // records) — without anchored loaded coverage they TRIM_HORIZON.
    let mut evidenced: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut loaded_spans: BTreeMap<String, Vec<SeqSpan>> = BTreeMap::new();
    let mut stream_recreated = false;
    let mut checkpoint_corrupt = false;
    // Warn counters — no exclusion/downgrade may be silent (the Kafka
    // frontier discipline, Codex R3 C1 / R5 H2 / R16 H1 lineage).
    let mut dead_generation_rows = 0usize;
    let mut floor_only_identity = 0usize;
    let mut malformed_spans = 0usize;
    let mut skipped_unparseable = 0usize;

    for row in rows {
        let p = row.payload;
        if p.get("kind").and_then(Value::as_str) != Some(KINESIS_STREAMING_KIND)
            || p.get(STREAM_PAYLOAD_KEY).and_then(Value::as_str) != Some(stream)
        {
            continue; // another pipeline's / another stream's row
        }
        // Only rows carrying the durable checkpoint contribute. Distinguish
        // ABSENT (a legit non-checkpoint kinesis row — skip) from
        // PRESENT-but-not-an-object (a corrupt durable checkpoint whose
        // shards we cannot enumerate — Codex R3 C1: condemn the whole
        // frontier to TrimHorizon rather than let its shards fall back to
        // the spec LATEST default).
        let seqs = match p.get(SEQUENCES_PAYLOAD_KEY) {
            None => continue,
            // A NON-EMPTY object is the only enumerable checkpoint. A
            // non-object (R3 C1) OR an EMPTY object (R4 C1) is a durable
            // checkpoint that names no usable shards — unenumerable, so it
            // floors the WHOLE frontier to TrimHorizon rather than let its
            // shards fall back to the spec LATEST default (a durable
            // streaming segment always covers ≥1 shard, so `{}` is anomalous).
            Some(Value::Object(o)) if !o.is_empty() => o,
            Some(_) => {
                checkpoint_corrupt = true;
                skipped_unparseable += 1;
                continue;
            }
        };
        // Identity gate (creation timestamp = generation marker): a
        // DEFINITE mismatch is a dead generation — excluded from BOTH
        // roles and the whole frontier is condemned to TRIM_HORIZON. An
        // unstamped row / unresolved current identity / malformed stamp
        // is evidence-only: loss prevention without skip permission.
        let advance_identity_ok =
            match (p.get(STREAM_CREATION_PAYLOAD_KEY), current_creation_millis) {
                (Some(stamp), Some(current)) => match stamp.as_i64() {
                    Some(ts) if ts == current => true,
                    Some(_) => {
                        stream_recreated = true;
                        dead_generation_rows += 1;
                        continue;
                    }
                    None => {
                        // Present but malformed — a corrupt stamp is NOT an
                        // unstamped row, but it grants no skip permission
                        // either.
                        floor_only_identity += 1;
                        false
                    }
                },
                _ => {
                    floor_only_identity += 1;
                    false
                }
            };
        for (shard, span_value) in seqs {
            let parse_field = |key: &str| {
                span_value
                    .get(key)
                    .and_then(Value::as_str)
                    .and_then(|s| SeqNum::parse(s).ok())
            };
            let start = parse_field("start");
            let last = parse_field("last");
            let Some(start) = start else {
                // A durable checkpoint NAMES this shard, so the shard key
                // alone is floor-only evidence (Codex R2 C1): admit it even
                // when `start` is unparseable, or it would fall back to the
                // spec LATEST default and could skip an unpublished leading
                // hole. Loud (counted + warned below), never silent, never a
                // skip permission.
                skipped_unparseable += 1;
                evidenced.insert(shard.clone());
                continue;
            };
            // EVIDENCE: any durable span admits the shard into the
            // frontier (loss prevention — the spec default is never
            // used for it), whether or not it grants skip permission.
            evidenced.insert(shard.clone());
            // ADVANCE: only identity-confirmed, actually-reloaded rows
            // with a fully-parsed span may be skipped past.
            let Some(last) = last else {
                malformed_spans += 1;
                continue;
            };
            if advance_identity_ok && row.loaded {
                // `prevLast` decides anchoring (Codex R1/R2): an ABSENT (or
                // null) `prevLast` is a genuine ANCHOR (the first batch read
                // from Trim/Latest — nothing consumed below it). A PRESENT
                // but MALFORMED `prevLast` must NOT collapse to `None`: that
                // would falsely promote an unanchored span to an anchor and
                // skip the records below it (Codex R2 C2). The span is
                // dropped to floor-only (the shard stays `evidenced` above ⇒
                // TrimHorizon) instead of pushed as loaded.
                let prev_last = match span_value.get("prevLast") {
                    None | Some(Value::Null) => None,
                    Some(v) => match v.as_str().and_then(|s| SeqNum::parse(s).ok()) {
                        Some(p) => Some(p),
                        None => {
                            malformed_spans += 1;
                            continue;
                        }
                    },
                };
                loaded_spans
                    .entry(shard.clone())
                    .or_default()
                    .push(SeqSpan::new(start, last, prev_last));
            }
        }
    }

    if stream_recreated {
        tracing::warn!(
            stream,
            dead_generation_rows,
            "kinesis stream delete+recreate POSITIVELY detected (durable rows \
             stamped with a different stream creation timestamp): the durable \
             sequence frontier names a dead generation's records, so EVERY \
             shard resumes from TRIM_HORIZON (re-consuming the retained log — \
             bounded at-least-once duplication, never loss)"
        );
    }
    if checkpoint_corrupt {
        tracing::warn!(
            stream,
            "kinesis durable checkpoint row had a non-object or EMPTY \
             kinesisSequences value (corrupt/unreadable): its shards cannot be \
             enumerated, so EVERY shard resumes from TRIM_HORIZON rather than \
             risk the spec LATEST default skipping records after the last \
             durable segment (bounded at-least-once duplication, never loss)"
        );
    }
    if floor_only_identity > 0 {
        tracing::warn!(
            stream,
            floor_only_identity,
            "kinesis resume frontier: durable rows whose stream identity could \
             not be CONFIRMED (unstamped row, unresolved current identity, or \
             malformed stamp) contribute evidence-only (no skip permission) — \
             without anchored loaded coverage their shards re-consume from \
             TRIM_HORIZON rather than skip"
        );
    }
    if malformed_spans > 0 || skipped_unparseable > 0 {
        tracing::warn!(
            stream,
            malformed_spans,
            skipped_unparseable,
            "kinesis resume frontier: structurally corrupt kinesisSequences \
             spans encountered (evidence-only where a start parsed; skipped \
             where nothing parsed) — the affected coverage is re-consumed"
        );
    }

    let mut shards = BTreeMap::new();
    for shard in evidenced {
        let spans = loaded_spans.remove(&shard).unwrap_or_default();
        shards.insert(shard.clone(), ShardResume::new(shard, spans));
    }
    KinesisResumeFrontier {
        shards,
        stream_recreated,
        checkpoint_corrupt,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(s: &str) -> SeqNum {
        SeqNum::parse(s).expect("valid seq")
    }

    fn span(start: &str, last: &str, prev: Option<&str>) -> SeqSpan {
        SeqSpan::new(seq(start), seq(last), prev.map(seq))
    }

    // -- SeqNum ordering ----------------------------------------------------

    #[test]
    fn seqnum_orders_numerically_not_lexicographically() {
        assert!(seq("9") < seq("10"));
        assert!(seq("99") < seq("100"));
        assert!(seq("2") > seq("1"));
        // Realistic 56-digit Kinesis sequence numbers (beyond u128).
        let a = seq("49590338271490256608559692538361571095921575989136588898");
        let b = seq("49590338271490256608559692538361571095921575989136588899");
        let c = seq("149590338271490256608559692538361571095921575989136588898");
        assert!(a < b);
        assert!(b < c);
        assert_eq!(a.cmp(&a), std::cmp::Ordering::Equal);
    }

    #[test]
    fn seqnum_leading_zeros_are_canonicalized() {
        assert_eq!(seq("007"), seq("7"));
        assert_eq!(seq("007").as_str(), "7");
        assert_eq!(seq("000"), seq("0"));
        assert!(seq("0099") < seq("100"));
    }

    #[test]
    fn seqnum_rejects_invalid() {
        for bad in ["", "12a3", "-5", "+5", " 42", "42 ", "4.2"] {
            assert!(SeqNum::parse(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn seqnum_serde_roundtrip_and_validation() {
        let s = seq("49590338271490256608559692538361571095921575989136588898");
        let json = serde_json::to_string(&s).expect("ser");
        assert_eq!(
            json,
            "\"49590338271490256608559692538361571095921575989136588898\""
        );
        let back: SeqNum = serde_json::from_str(&json).expect("de");
        assert_eq!(back, s);
        assert!(serde_json::from_str::<SeqNum>("\"12a\"").is_err());
    }

    // -- span folding -------------------------------------------------------

    #[test]
    fn fold_spans_merges_numeric_overlap() {
        let folded = fold_spans(vec![
            span("10", "20", None),
            span("15", "30", None), // overlaps [10,20]
        ]);
        assert_eq!(folded, vec![span("10", "30", None)]);
    }

    #[test]
    fn fold_spans_merges_chain_adjacent() {
        // [10,20] then [25,30] whose prev_last == 20: contiguous in
        // RECORD space (sparse sequence numbers) → one interval.
        let folded = fold_spans(vec![span("10", "20", None), span("25", "30", Some("20"))]);
        assert_eq!(folded.len(), 1);
        assert_eq!(folded[0].start, seq("10"));
        assert_eq!(folded[0].last, seq("30"));
        assert_eq!(folded[0].prev_last, None); // keeps the head's link
    }

    #[test]
    fn fold_spans_keeps_unchained_gap_separate() {
        // prev_last (17) != previous last (20) → potential dropped-batch
        // hole → intervals must NOT merge.
        let folded = fold_spans(vec![span("10", "20", None), span("25", "30", Some("17"))]);
        assert_eq!(folded.len(), 2);
    }

    #[test]
    fn fold_spans_drops_degenerate_and_sorts() {
        let folded = fold_spans(vec![
            span("30", "40", Some("20")),
            span("50", "40", None), // degenerate: last < start
            span("10", "20", None),
        ]);
        assert_eq!(folded.len(), 1, "chain 10-20 → 30-40 merges: {folded:?}");
        assert_eq!(folded[0].start, seq("10"));
        assert_eq!(folded[0].last, seq("40"));
    }

    // -- resume walk --------------------------------------------------------

    #[test]
    fn resume_advances_through_chained_coverage() {
        // A clean chain from a genuine anchor (prev_last == None): the
        // resume skips past the whole covered range.
        let r = ShardResume::new(
            "shardId-000000000000",
            vec![span("10", "20", None), span("25", "30", Some("20"))],
        );
        assert_eq!(
            r.resume_start_position(),
            StartPosition::AfterSequenceNumber(seq("30"))
        );
    }

    #[test]
    fn resume_multi_span_chain_from_anchor_reaches_chain_end() {
        // Three rolls, each chained onto the previous roll's last, the
        // first a genuine anchor: an unbroken multi-span chain resumes
        // AFTER the chain end.
        let r = ShardResume::new(
            "s",
            vec![
                span("10", "20", None),
                span("25", "30", Some("20")),
                span("41", "55", Some("30")),
            ],
        );
        assert_eq!(
            r.resume_start_position(),
            StartPosition::AfterSequenceNumber(seq("55"))
        );
    }

    #[test]
    fn resume_stops_at_chain_break() {
        // Roll 2's batch was dropped after a failed publish: roll 3
        // chains onto the DROPPED batch's last (40), not roll 1's (20).
        // The walk must stop at 20 so the dropped records re-consume.
        // This is SAFE (not TrimHorizon-worthy): the anchor at 10
        // proves from-the-beginning coverage up to 20, so resuming
        // After(20) skips nothing unpublished.
        let r = ShardResume::new(
            "s",
            vec![span("10", "20", None), span("45", "50", Some("40"))],
        );
        assert_eq!(
            r.resume_start_position(),
            StartPosition::AfterSequenceNumber(seq("20"))
        );
    }

    #[test]
    fn resume_unanchored_coverage_trims() {
        // The only loaded coverage [25,30] chains onto consumption at
        // 20 that no durable span covers (e.g. its row is a phantom
        // whose blob vanished): nothing proves records below 25 are
        // durable → re-consume the retained log, never skip.
        let r = ShardResume::new("s", vec![span("25", "30", Some("20"))]);
        assert_eq!(r.resume_start_position(), StartPosition::TrimHorizon);
    }

    #[test]
    fn resume_with_no_loaded_coverage_trims() {
        // Durable evidence exists (the shard has a ShardResume) but
        // nothing reloaded: no skip permission at all → TRIM_HORIZON
        // (never the spec default, which could be LATEST).
        let r = ShardResume::new("s", Vec::new());
        assert_eq!(r.resume_start_position(), StartPosition::TrimHorizon);
    }

    #[test]
    fn resume_r1c1_unanchored_span_trims_never_after() {
        // R1-C1 repro (Codex compat-5 review): on a shard, the roll of
        // span [10,20] FAILED to publish (dropped, never durable); the
        // loop read forward and durably published [30,40] with
        // prevLast=20. The only durable row is [30,40], so the lowest
        // loaded span does NOT anchor (prev_last = Some(20) proves
        // records were consumed below it that no durable span covers).
        // Resuming After(40) would permanently lose [10,20]; the ONLY
        // loss-safe resume is TRIM_HORIZON (bounded at-least-once
        // duplication, never loss).
        let r = ShardResume::new("s", vec![span("30", "40", Some("20"))]);
        assert_eq!(r.resume_start_position(), StartPosition::TrimHorizon);
    }

    #[test]
    fn resume_advance_is_monotonic() {
        // Adding more CHAINED coverage never moves the resume backwards.
        let base = ShardResume::new("s", vec![span("10", "20", None)]);
        let more = ShardResume::new(
            "s",
            vec![span("10", "20", None), span("22", "31", Some("20"))],
        );
        let StartPosition::AfterSequenceNumber(a) = base.resume_start_position() else {
            panic!("base should advance");
        };
        let StartPosition::AfterSequenceNumber(b) = more.resume_start_position() else {
            panic!("more should advance");
        };
        assert!(b >= a);
    }

    #[test]
    fn resume_chains_from_anchor_across_sparse_gap() {
        // First roll AFTER a resume: the span starts far above the
        // anchor's coverage numerically but chains onto the durable
        // frontier in RECORD space (prev_last == a loaded last) —
        // sequence numbers are sparse, so the numeric gap is fine.
        let r = ShardResume::new(
            "s",
            vec![span("10", "20", None), span("100", "110", Some("20"))],
        );
        assert_eq!(
            r.resume_start_position(),
            StartPosition::AfterSequenceNumber(seq("110"))
        );
    }

    // -- fold from payload --------------------------------------------------

    fn payload(stream: &str, creation: Option<i64>, seqs: serde_json::Value) -> serde_json::Value {
        let mut p = serde_json::json!({
            "kind": KINESIS_STREAMING_KIND,
            "stream": stream,
            SEQUENCES_PAYLOAD_KEY: seqs,
        });
        if let Some(c) = creation {
            p[STREAM_CREATION_PAYLOAD_KEY] = serde_json::json!(c);
        }
        p
    }

    #[test]
    fn fold_from_payload_round_trips() {
        // Stamp two segments' ShardSequences, fold them back, and check
        // the resume walk sees the chain.
        let mut seg1 = ShardSequences::new();
        seg1.insert("shard-a".to_owned(), span("10", "20", None));
        seg1.insert("shard-b".to_owned(), span("5", "9", None));
        let mut seg2 = ShardSequences::new();
        seg2.insert("shard-a".to_owned(), span("31", "40", Some("20")));

        let p1 = payload("st", Some(111), sequences_to_payload(&seg1));
        let p2 = payload("st", Some(111), sequences_to_payload(&seg2));
        let rows = [
            FrontierRowEvidence {
                payload: &p1,
                loaded: true,
            },
            FrontierRowEvidence {
                payload: &p2,
                loaded: true,
            },
        ];
        let f = fold_resume_frontier(&rows, "st", Some(111));
        assert!(!f.stream_recreated);
        assert_eq!(f.shards.len(), 2);
        assert_eq!(
            f.start_position_for("shard-a", StartPosition::Latest),
            StartPosition::AfterSequenceNumber(seq("40"))
        );
        assert_eq!(
            f.start_position_for("shard-b", StartPosition::Latest),
            StartPosition::AfterSequenceNumber(seq("9"))
        );
        // No evidence → spec default.
        assert_eq!(
            f.start_position_for("shard-zz", StartPosition::Latest),
            StartPosition::Latest
        );
    }

    #[test]
    fn fold_r1c1_failed_leading_roll_resumes_trim_horizon() {
        // R1-C1 at the fold level: the ONLY durable, loaded,
        // identity-confirmed row covers [30,40] with prevLast=20 (the
        // leading roll [10,20] failed to publish and was dropped). The
        // durable evidence proves records BELOW 30 were consumed but
        // never durably published, so the resume must be TRIM_HORIZON —
        // NOT AfterSequenceNumber(40), which would permanently lose
        // [10,20].
        let mut seg = ShardSequences::new();
        seg.insert("shard-a".to_owned(), span("30", "40", Some("20")));
        let p = payload("st", Some(111), sequences_to_payload(&seg));
        let rows = [FrontierRowEvidence {
            payload: &p,
            loaded: true,
        }];
        let f = fold_resume_frontier(&rows, "st", Some(111));
        assert_eq!(
            f.start_position_for("shard-a", StartPosition::Latest),
            StartPosition::TrimHorizon,
            "an unanchored durable span must never license a skip below itself"
        );
    }

    #[test]
    fn fold_r2c1_malformed_start_is_floor_only_not_latest() {
        // R2-C1: a durable row NAMES shard-a but its `start` is unparseable.
        // The shard key alone is floor-only evidence — it must resolve to
        // TrimHorizon, never fall back to the spec LATEST default (which
        // could skip an unpublished leading hole).
        let seqs = serde_json::json!({"shard-a": {"start": "bad", "last": "40", "prevLast": "20"}});
        let p = payload("st", Some(111), seqs);
        let rows = [FrontierRowEvidence {
            payload: &p,
            loaded: true,
        }];
        let f = fold_resume_frontier(&rows, "st", Some(111));
        assert_eq!(
            f.start_position_for("shard-a", StartPosition::Latest),
            StartPosition::TrimHorizon,
            "a durable row naming the shard must floor it to TrimHorizon even with a malformed start"
        );
    }

    #[test]
    fn fold_r2c2_malformed_prev_last_is_floor_only_not_false_anchor() {
        // R2-C2: durable [30,40] with a PRESENT-but-malformed prevLast must
        // NOT collapse to None (a false anchor) and resume After(40) — that
        // would skip the unpublished [10,20] below it. Floor-only ⇒
        // TrimHorizon.
        let seqs = serde_json::json!({"shard-a": {"start": "30", "last": "40", "prevLast": "bad"}});
        let p = payload("st", Some(111), seqs);
        let rows = [FrontierRowEvidence {
            payload: &p,
            loaded: true,
        }];
        let f = fold_resume_frontier(&rows, "st", Some(111));
        assert_eq!(
            f.start_position_for("shard-a", StartPosition::Latest),
            StartPosition::TrimHorizon,
            "a malformed prevLast must be floor-only, never a false anchor licensing a skip"
        );
    }

    #[test]
    fn fold_r3c1_non_object_sequences_floors_all_shards() {
        // R3-C1: a durable row for this stream whose `kinesisSequences` is
        // PRESENT but not an object (corrupt) cannot be enumerated. It must
        // condemn the WHOLE frontier to TrimHorizon — never let an unknown
        // shard fall back to the spec LATEST default and skip records after
        // the last durable segment.
        let mut p = payload("st", Some(111), serde_json::Value::Null);
        p[SEQUENCES_PAYLOAD_KEY] = serde_json::json!("corrupt");
        let rows = [FrontierRowEvidence {
            payload: &p,
            loaded: true,
        }];
        let f = fold_resume_frontier(&rows, "st", Some(111));
        assert!(
            f.checkpoint_corrupt,
            "a non-object checkpoint must be flagged"
        );
        assert_eq!(
            f.start_position_for("any-shard", StartPosition::Latest),
            StartPosition::TrimHorizon,
            "a corrupt durable checkpoint must floor every shard, even unknown ones"
        );
        // An ABSENT kinesisSequences (a legit non-checkpoint row) must NOT
        // trip the corrupt flag.
        let p2 = serde_json::json!({"kind": KINESIS_STREAMING_KIND, "stream": "st"});
        let rows2 = [FrontierRowEvidence {
            payload: &p2,
            loaded: true,
        }];
        let f2 = fold_resume_frontier(&rows2, "st", Some(111));
        assert!(!f2.checkpoint_corrupt);
        assert_eq!(
            f2.start_position_for("any-shard", StartPosition::Latest),
            StartPosition::Latest,
            "an absent checkpoint (non-durable row) honors the spec default"
        );

        // R4-C1: a PRESENT but EMPTY object names no shards — a durable
        // streaming segment always covers >=1 shard, so `{}` is anomalous
        // and must floor every shard, never fall back to LATEST.
        let p3 = payload("st", Some(111), serde_json::json!({}));
        let rows3 = [FrontierRowEvidence {
            payload: &p3,
            loaded: true,
        }];
        let f3 = fold_resume_frontier(&rows3, "st", Some(111));
        assert!(
            f3.checkpoint_corrupt,
            "an empty kinesisSequences object must be flagged corrupt"
        );
        assert_eq!(
            f3.start_position_for("any-shard", StartPosition::Latest),
            StartPosition::TrimHorizon,
            "an empty durable checkpoint must floor every shard to TrimHorizon"
        );
    }

    #[test]
    fn fold_unloaded_row_trims_and_never_advances() {
        let mut seg = ShardSequences::new();
        seg.insert("a".to_owned(), span("10", "20", None));
        let p = payload("st", Some(1), sequences_to_payload(&seg));
        let rows = [FrontierRowEvidence {
            payload: &p,
            loaded: false, // phantom: metadata row exists, blob missing
        }];
        let f = fold_resume_frontier(&rows, "st", Some(1));
        assert_eq!(
            f.start_position_for("a", StartPosition::Latest),
            StartPosition::TrimHorizon,
            "a phantom row grants no skip permission and must suppress the \
             spec default (LATEST would skip its re-consumable records): \
             re-consume the retained log, never advance"
        );
    }

    #[test]
    fn fold_identity_mismatch_marks_recreated() {
        let mut seg = ShardSequences::new();
        seg.insert("a".to_owned(), span("10", "20", None));
        let p = payload("st", Some(111), sequences_to_payload(&seg));
        let rows = [FrontierRowEvidence {
            payload: &p,
            loaded: true,
        }];
        // Current stream was created at a DIFFERENT time → the durable
        // evidence belongs to a dead generation.
        let f = fold_resume_frontier(&rows, "st", Some(222));
        assert!(f.stream_recreated);
        // EVERY shard trims — evidence or not.
        assert_eq!(
            f.start_position_for("a", StartPosition::Latest),
            StartPosition::TrimHorizon
        );
        assert_eq!(
            f.start_position_for("other", StartPosition::Latest),
            StartPosition::TrimHorizon
        );
    }

    #[test]
    fn fold_unstamped_or_unresolved_identity_never_skips() {
        let mut seg = ShardSequences::new();
        seg.insert("a".to_owned(), span("10", "20", None));
        // Row unstamped (no creation timestamp).
        let p_unstamped = payload("st", None, sequences_to_payload(&seg));
        let rows = [FrontierRowEvidence {
            payload: &p_unstamped,
            loaded: true,
        }];
        let f = fold_resume_frontier(&rows, "st", Some(1));
        assert!(!f.stream_recreated);
        assert_eq!(
            f.start_position_for("a", StartPosition::Latest),
            StartPosition::TrimHorizon,
            "unstamped identity: evidence-only, never skip (and never the \
             spec default)"
        );
        // Row stamped but CURRENT identity unresolved.
        let p_stamped = payload("st", Some(1), sequences_to_payload(&seg));
        let rows = [FrontierRowEvidence {
            payload: &p_stamped,
            loaded: true,
        }];
        let f = fold_resume_frontier(&rows, "st", None);
        assert!(!f.stream_recreated);
        assert_eq!(
            f.start_position_for("a", StartPosition::Latest),
            StartPosition::TrimHorizon
        );
    }

    #[test]
    fn fold_malformed_span_never_skips() {
        // `last` is not a decimal → the span admits the shard via
        // `start` (evidence-only) but must never advance a skip.
        let p = payload(
            "st",
            Some(1),
            serde_json::json!({"a": {"start": "10", "last": "xx"}}),
        );
        let rows = [FrontierRowEvidence {
            payload: &p,
            loaded: true,
        }];
        let f = fold_resume_frontier(&rows, "st", Some(1));
        assert_eq!(
            f.start_position_for("a", StartPosition::Latest),
            StartPosition::TrimHorizon
        );
        // `start` is unparseable, but the shard KEY in a durable checkpoint
        // is itself floor-only evidence (Codex R2 C1): the shard is admitted
        // and resolves to TrimHorizon, never the spec LATEST default.
        let p2 = payload("st", Some(1), serde_json::json!({"a": {"start": true}}));
        let rows2 = [FrontierRowEvidence {
            payload: &p2,
            loaded: true,
        }];
        let f2 = fold_resume_frontier(&rows2, "st", Some(1));
        assert_eq!(
            f2.start_position_for("a", StartPosition::Latest),
            StartPosition::TrimHorizon
        );
    }

    #[test]
    fn fold_ignores_foreign_kind_and_stream() {
        let mut seg = ShardSequences::new();
        seg.insert("a".to_owned(), span("10", "20", None));
        let other_stream = payload("other", Some(1), sequences_to_payload(&seg));
        let kafka_kind = serde_json::json!({
            "kind": "kafka-streaming",
            "stream": "st",
            SEQUENCES_PAYLOAD_KEY: sequences_to_payload(&seg),
        });
        let rows = [
            FrontierRowEvidence {
                payload: &other_stream,
                loaded: true,
            },
            FrontierRowEvidence {
                payload: &kafka_kind,
                loaded: true,
            },
        ];
        let f = fold_resume_frontier(&rows, "st", Some(1));
        assert!(f.shards.is_empty());
        assert!(!f.stream_recreated);
    }

    #[test]
    fn fold_phantom_below_unanchored_coverage_trims() {
        // A phantom row's span [9,20] (blob missing) sits below loaded
        // but UNANCHORED coverage [50,60] (prev_last = Some(45)): the
        // walk cannot root at an anchor → nothing may be skipped, the
        // whole retained log is re-consumed.
        let mut seg1 = ShardSequences::new();
        seg1.insert("a".to_owned(), span("50", "60", Some("45")));
        let mut seg2 = ShardSequences::new();
        seg2.insert("a".to_owned(), span("9", "20", None));
        let p1 = payload("st", Some(1), sequences_to_payload(&seg1));
        let p2 = payload("st", Some(1), sequences_to_payload(&seg2));
        let rows = [
            FrontierRowEvidence {
                payload: &p1,
                loaded: true,
            },
            FrontierRowEvidence {
                payload: &p2,
                loaded: false, // phantom: evidence-only
            },
        ];
        let f = fold_resume_frontier(&rows, "st", Some(1));
        let r = f.shards.get("a").expect("shard evidence");
        assert_eq!(r.resume_start_position(), StartPosition::TrimHorizon);
    }

    #[test]
    fn fold_anchor_is_trusted_above_a_phantom_row() {
        // Documented trade of the anchor model: loaded ANCHORED coverage
        // [50,60] (prev_last = None — the first batch that consumer
        // session ever read, so NOTHING below 50 was consumed and left
        // unpublished by it) resumes After(60) even though a phantom
        // row's span [9,20] sits below. The phantom's records were
        // durably PUBLISHED once (P-before-M) and only their blob
        // vanished — a deep-storage recovery concern, not a
        // consume-resume loss: an anchor at 50 can only arise from an
        // operator-chosen LATEST start or a retention trim, in neither
        // of which a TrimHorizon re-consume could restore [9,20] anyway.
        let mut seg1 = ShardSequences::new();
        seg1.insert("a".to_owned(), span("50", "60", None));
        let mut seg2 = ShardSequences::new();
        seg2.insert("a".to_owned(), span("9", "20", None));
        let p1 = payload("st", Some(1), sequences_to_payload(&seg1));
        let p2 = payload("st", Some(1), sequences_to_payload(&seg2));
        let rows = [
            FrontierRowEvidence {
                payload: &p1,
                loaded: true,
            },
            FrontierRowEvidence {
                payload: &p2,
                loaded: false, // phantom: evidence-only
            },
        ];
        let f = fold_resume_frontier(&rows, "st", Some(1));
        let r = f.shards.get("a").expect("shard evidence");
        assert_eq!(
            r.resume_start_position(),
            StartPosition::AfterSequenceNumber(seq("60"))
        );
    }
}
