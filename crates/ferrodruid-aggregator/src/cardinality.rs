// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Cardinality aggregator — exact cardinality using [`HashSet`].
//!
//! Counts the number of distinct values across one or more fields.

use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

use crate::{Aggregator, AggregatorSaturation};

// ---------------------------------------------------------------------------
// CardinalityState — typed wire form for broker scatter/gather merge
// ---------------------------------------------------------------------------

/// JSON envelope used to ship a [`CardinalityAggregator`]'s exact-set
/// contents from a per-shard worker (e.g. historical) to the merging broker.
///
/// Wave 40-B (Wave 39 [High] [NEW-VARIANT] `broker/lib.rs:566-587 +
/// aggregator/lib.rs:88-129`): the broker previously merged cardinality
/// results by saturating-add of the per-shard *counts*, which over-counted
/// any keys that overlapped across shards.  Shipping the actual key set on
/// the wire lets the broker run a true `HashSet` union when both sides have
/// `saturated == false`.  When either side is saturated the broker keeps
/// the additive upper bound (also saturating) — this is the same
/// degradation mode `merge_cardinality` already used.
///
/// Multi-shard exact union (2026-07-11): the envelope carries the FULL
/// exact set up to the same [`MAX_CARDINALITY_SET_SIZE`] bound that governs
/// the aggregator itself — the former separate 1,000-key wire cap
/// (`MAX_WIRE_VALUES`) is gone.  The supported product is single-binary
/// mode (`Broker::execute_local`, historicals in-process), where the
/// "wire" is an in-process `serde_json::Value`, so the envelope size is
/// bounded by the per-aggregator DoS cap rather than a network budget.
/// A shard whose aggregator saturated ships `saturated = true` (keys
/// dropped) and the merge degrades to a saturating-add upper bound, which
/// the broker finalization pass fails closed.  Classic-distributed
/// deployments should note the consequence: an exact-mode partial can ship
/// up to [`MAX_CARDINALITY_SET_SIZE`] keys per shard per group (exact mode
/// is opt-in; `APPROX_COUNT_DISTINCT` / HLL remains the wire-cheap path).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CardinalityState {
    /// Sentinel JSON tag so the broker can distinguish a state envelope
    /// from a bare numeric count using only `serde_json::from_value`.
    /// Must be the literal string `"cardinality_state"`.
    #[serde(rename = "@type")]
    pub typ: String,
    /// Canonical-string set contents (`value_to_key`-encoded, identical to
    /// `CardinalityAggregator::iter_keys`).  Empty when `saturated == true`.
    pub values: Vec<String>,
    /// `true` once any shard saturated; the merge degrades to saturating-add.
    pub saturated: bool,
    /// Pre-computed count.  Always present so a broker that doesn't know
    /// the state shape can fall back to a count read.
    pub count: u64,
}

// (2026-07-11) The former `MAX_WIRE_VALUES` (1,000) envelope cap was folded
// into `MAX_CARDINALITY_SET_SIZE`: a separate, tighter wire cap made any
// shard with >1,000 distinct values un-unionable and forced the broker
// merge to fail closed even though both sides held exact sets.  With the
// bound unified, single-segment AND multi-segment exact distinct counts are
// exact up to the same 1,000,000-key cap, and only genuinely over-cap
// unions fail closed.

/// JSON `@type` tag used by [`CardinalityState`].
pub const CARDINALITY_STATE_TAG: &str = "cardinality_state";

/// Error returned when a JSON value TAGGED as a [`CardinalityState`]
/// envelope (`"@type": "cardinality_state"`) fails to parse or violates
/// the envelope invariants (see [`CardinalityState::from_json`]).
///
/// Untrusted-peer hardening (2026-07-12, Codex HIGH findings 1+4+5):
/// envelopes are exchanged between shards and must be treated as hostile
/// input.  A tagged-but-malformed value used to be indistinguishable from
/// a legacy bare count (`from_json` returned `None` for both), so the
/// merge path silently treated the shard as an empty 0 and DROPPED it.
/// This type makes "tagged but bad" a distinct, fail-closed outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedCardinalityState {
    /// Static description of the violated invariant.  Deliberately carries
    /// no peer-controlled data (nothing to echo back to an attacker).
    pub reason: &'static str,
}

impl std::fmt::Display for MalformedCardinalityState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "malformed cardinality state envelope: {}", self.reason)
    }
}

impl std::error::Error for MalformedCardinalityState {}

/// Upper bound (in bytes) accepted for any single canonical key arriving
/// in a peer envelope's `values` array.
///
/// A legitimate producer clips every key to [`MAX_CARDINALITY_KEY_BYTES`]
/// (plus, for keys written by pre-2026-07-12 builds, the 8-byte `…trunc`
/// marker), so anything longer can only be forged or corrupt — and
/// accepting it would let a peer bypass the per-key memory bound.
pub const MAX_WIRE_VALUE_BYTES: usize = MAX_CARDINALITY_KEY_BYTES + "…trunc".len();

impl CardinalityState {
    /// Construct an empty saturated state with the given count (used when
    /// the source aggregator is over-cap or when a count-only legacy wire
    /// shape needs to be promoted into a state for further union).
    #[must_use]
    pub fn saturated_with_count(count: u64) -> Self {
        Self {
            typ: CARDINALITY_STATE_TAG.to_owned(),
            values: Vec::new(),
            saturated: true,
            count,
        }
    }

    /// Try to deserialize a JSON value as a `CardinalityState`.
    ///
    /// Returns:
    /// * `Ok(None)` — the value is *not* the typed envelope (typically a
    ///   bare numeric count emitted by an old or in-process path);
    /// * `Ok(Some(state))` — a well-formed, invariant-checked envelope;
    /// * `Err(_)` — the value is TAGGED as an envelope
    ///   (`"@type": "cardinality_state"`) but is malformed or violates the
    ///   envelope invariants.  Callers MUST fail closed (reject or
    ///   saturate), never treat this as an empty/zero shard (Codex HIGH
    ///   finding 1: the old `Option` return silently dropped such shards).
    ///
    /// Validated invariants (Codex HIGH findings 4+5 — envelopes are
    /// untrusted peer input):
    /// * `saturated` is a bool, `count` is a u64, `values` is an array of
    ///   strings;
    /// * `values.len()` ≤ [`MAX_CARDINALITY_SET_SIZE`] and every value is
    ///   ≤ [`MAX_WIRE_VALUE_BYTES`] bytes (peer-triggered OOM bound; the
    ///   check runs on the *borrowed* JSON before anything is cloned);
    /// * a NON-saturated state's `count` must equal the number of distinct
    ///   `values` — the peer-supplied count is never trusted over the
    ///   actual set (a forged `values=["x"], count=1000000` used to
    ///   finalize as an exact 1,000,000);
    /// * a saturated state ships no keys (`values` empty); its `count` is
    ///   accepted as an inexact bound (finalization fails closed on it).
    ///
    /// # Errors
    /// [`MalformedCardinalityState`] when the value carries the envelope
    /// tag but any of the above fails.
    pub fn from_json(value: &serde_json::Value) -> Result<Option<Self>, MalformedCardinalityState> {
        let Some(obj) = value.as_object() else {
            return Ok(None);
        };
        match obj.get("@type").and_then(|v| v.as_str()) {
            Some(t) if t == CARDINALITY_STATE_TAG => {}
            _ => return Ok(None),
        }
        // Validate on the borrowed value BEFORE the clone so a hostile
        // over-cap envelope is rejected without duplicating its payload.
        validate_tagged_obj(obj, MAX_CARDINALITY_SET_SIZE)?;
        match serde_json::from_value(value.clone()) {
            Ok(state) => Ok(Some(state)),
            Err(_) => Err(MalformedCardinalityState {
                reason: "tagged envelope failed to deserialize",
            }),
        }
    }

    /// Serialize this state as a JSON value carrying the `@type` tag.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// Clone-free, VALIDATING probe of a JSON value for the envelope
    /// shape: returns `(saturated, count)` when `value` is a well-formed
    /// [`CardinalityState`] envelope, `Ok(None)` when it is not an
    /// envelope at all, and `Err` when it is tagged as one but malformed
    /// or invariant-violating (same rules as [`Self::from_json`]; the
    /// caller must fail closed).  Unlike `from_json` this never clones the
    /// `values` array — the invariant check walks it borrowed — so it
    /// stays the cheap probe for the broker finalization path, which is
    /// exactly the path a forged `count` would otherwise reach unchecked
    /// (Codex HIGH finding 4).
    ///
    /// # Errors
    /// [`MalformedCardinalityState`] when the value carries the envelope
    /// tag but fails validation.
    pub fn peek_json(
        value: &serde_json::Value,
    ) -> Result<Option<(bool, u64)>, MalformedCardinalityState> {
        let Some(obj) = value.as_object() else {
            return Ok(None);
        };
        match obj.get("@type").and_then(|v| v.as_str()) {
            Some(t) if t == CARDINALITY_STATE_TAG => {}
            _ => return Ok(None),
        }
        validate_tagged_obj(obj, MAX_CARDINALITY_SET_SIZE).map(Some)
    }

    /// Clone-free, NON-validating probe: `(saturated, count)` if `value`
    /// looks like an envelope, `None` otherwise.
    ///
    /// Only for TRUSTED, executor-local envelopes — e.g. limitSpec
    /// ordering over partials the executor itself emitted a few lines
    /// earlier — where the O(values) invariant walk of
    /// [`Self::peek_json`] inside an `O(n log n)` sort comparator would
    /// be wasted work.  Anything that can carry a *peer* envelope (merge,
    /// broker finalization) must use the validating [`Self::peek_json`] /
    /// [`Self::from_json`].
    #[must_use]
    pub fn peek_json_unchecked(value: &serde_json::Value) -> Option<(bool, u64)> {
        let obj = value.as_object()?;
        if obj.get("@type")?.as_str()? != CARDINALITY_STATE_TAG {
            return None;
        }
        let saturated = obj.get("saturated")?.as_bool()?;
        let count = obj.get("count")?.as_u64()?;
        Some((saturated, count))
    }

    /// Union two states, producing a new state.  When either side is
    /// saturated, the result is saturated and its `count` is the
    /// saturating-add of the inputs (matching the existing degraded-mode
    /// merge).  When both sides have values, the result holds the exact
    /// union, subject to the exact-set cap
    /// ([`MAX_CARDINALITY_SET_SIZE`], or the test-lowered
    /// [`exact_cardinality_set_cap`]); an over-cap union saturates and the
    /// broker finalization pass fails the query closed.
    #[must_use]
    pub fn union(a: &Self, b: &Self) -> Self {
        Self::union_with_cap(a, b, exact_cardinality_set_cap())
    }

    /// [`Self::union`] with an explicit exact-set cap (separated out so
    /// unit tests can drive the over-cap saturation path without touching
    /// the process-wide test override).
    fn union_with_cap(a: &Self, b: &Self, cap: usize) -> Self {
        // Fail-closed (2026-07-12, Codex HIGH finding 6): the saturation
        // check runs BEFORE the empty-side identity.  A saturated operand
        // is inexact regardless of its (possibly empty) value set — the
        // old order let a hostile `saturated=true, count=0, values=[]`
        // peer state slip through the identity shortcut and produce an
        // exact-looking unsaturated union.  (No legitimate producer emits
        // a saturated count-0 state: a saturated aggregator's count is its
        // capped set size, ≥ 1.)
        if a.saturated || b.saturated {
            return Self::saturated_with_count(a.count.saturating_add(b.count));
        }
        // Union with a NON-saturated empty side is the exact identity.
        // This keeps e.g. an all-null shard (a `filtered` not-null
        // cardinality that matched no rows, count 0) from needlessly
        // degrading the merge to an inexact add.
        if b.count == 0 && b.values.is_empty() {
            return a.clone();
        }
        if a.count == 0 && a.values.is_empty() {
            return b.clone();
        }
        // Pre-allocation is bounded by the cap: a peer cannot force an
        // allocation larger than the exact-set bound plus the one probe
        // slot that detects overflow (Codex HIGH finding 5).
        let mut merged: HashSet<String> = HashSet::with_capacity(
            (a.values.len().saturating_add(b.values.len())).min(cap.saturating_add(1)),
        );
        for v in a.values.iter().chain(b.values.iter()) {
            merged.insert(v.clone());
            if merged.len() > cap {
                #[allow(clippy::cast_possible_truncation)]
                let count = merged.len() as u64;
                return Self::saturated_with_count(count);
            }
        }
        let values: Vec<String> = merged.into_iter().collect();
        #[allow(clippy::cast_possible_truncation)]
        let count = values.len() as u64;
        Self {
            typ: CARDINALITY_STATE_TAG.to_owned(),
            values,
            saturated: false,
            count,
        }
    }
}

/// Validate a JSON object already known to carry the
/// [`CARDINALITY_STATE_TAG`] against the envelope invariants (see
/// [`CardinalityState::from_json`] for the list).  Returns the verified
/// `(saturated, count)` pair.  `set_cap` is a parameter (production
/// callers pass [`MAX_CARDINALITY_SET_SIZE`]) so unit tests can drive the
/// over-cap rejection without materializing a million-entry array.
fn validate_tagged_obj(
    obj: &serde_json::Map<String, serde_json::Value>,
    set_cap: usize,
) -> Result<(bool, u64), MalformedCardinalityState> {
    let Some(saturated) = obj.get("saturated").and_then(serde_json::Value::as_bool) else {
        return Err(MalformedCardinalityState {
            reason: "`saturated` missing or not a bool",
        });
    };
    let Some(count) = obj.get("count").and_then(serde_json::Value::as_u64) else {
        return Err(MalformedCardinalityState {
            reason: "`count` missing or not a u64",
        });
    };
    let Some(values) = obj.get("values").and_then(serde_json::Value::as_array) else {
        return Err(MalformedCardinalityState {
            reason: "`values` missing or not an array",
        });
    };
    if values.len() > set_cap {
        return Err(MalformedCardinalityState {
            reason: "`values` exceeds the exact-set cap",
        });
    }
    if saturated {
        // A saturated producer ships no keys; its count is an accepted
        // inexact bound (finalization fails closed on the flag alone).
        if !values.is_empty() {
            return Err(MalformedCardinalityState {
                reason: "saturated state must ship no values",
            });
        }
        return Ok((true, count));
    }
    // Non-saturated: the count must equal the number of DISTINCT values —
    // never trust a peer-supplied count over the actual set.  The walk is
    // borrowed (no clones); duplicate or non-string entries fail too.
    let mut distinct: HashSet<&str> = HashSet::with_capacity(values.len());
    for v in values {
        let Some(s) = v.as_str() else {
            return Err(MalformedCardinalityState {
                reason: "`values` entry is not a string",
            });
        };
        if s.len() > MAX_WIRE_VALUE_BYTES {
            return Err(MalformedCardinalityState {
                reason: "`values` entry exceeds the per-key wire bound",
            });
        }
        if !distinct.insert(s) {
            return Err(MalformedCardinalityState {
                reason: "`values` entries are not distinct",
            });
        }
    }
    if usize::try_from(count) != Ok(distinct.len()) {
        return Err(MalformedCardinalityState {
            reason: "`count` does not match the distinct value set",
        });
    }
    Ok((false, count))
}

/// Maximum number of distinct keys held by an exact-cardinality aggregator.
///
/// Wave 36-G2 (Wave 37B High `cardinality.rs:17-57`): the previous
/// implementation kept an unbounded `HashSet<String>` that an attacker could
/// inflate by sending a high-cardinality column (a UUID-style key per row),
/// exhausting Historical RAM before query timeouts fire. We saturate at
/// this cap: once `seen.len() == MAX_CARDINALITY_SET_SIZE`, additional keys
/// stop being inserted and [`CardinalityAggregator::saturated`] returns
/// `true`.
///
/// Fail-closed (2026-07-11): a saturated exact set means the finalized
/// count would silently under-count, so finalization layers (the query
/// executors, via [`Aggregator::saturation`]) now FAIL the query with a
/// resource-limit error instead of returning the capped number. Callers
/// that need distinct counts beyond this bound must use an approximate
/// sketch (`APPROX_COUNT_DISTINCT` / HLL), which has no such cap.
pub const MAX_CARDINALITY_SET_SIZE: usize = 1_000_000;

/// `DruidError::ResourceLimit::kind` string used when an exact-cardinality
/// set hits its per-aggregator cap ([`MAX_CARDINALITY_SET_SIZE`]) and the
/// finalized count would silently under-count. Carried verbatim into the
/// REST error envelope, so it names both the limit and the remedy.
pub const CARDINALITY_EXACT_SET_LIMIT_KIND: &str = "cardinality.maxExactSetSize (exact COUNT(DISTINCT)/cardinality exceeded the exact \
     distinct-value set limit and the result would silently under-count; use \
     APPROX_COUNT_DISTINCT / an approximate HLL aggregator for unbounded-cardinality columns)";

/// `DruidError::ResourceLimit::kind` string used when exact-cardinality
/// partial results cannot be union-merged exactly across segments/shards
/// and the merged count would be an over-counting saturating-add upper
/// bound (see the broker's cardinality finalization pass).
pub const CARDINALITY_CROSS_SHARD_MERGE_LIMIT_KIND: &str = "cardinality.crossShardExactMerge (exact COUNT(DISTINCT)/cardinality partial results \
     could not be union-merged exactly across segments/shards — either the exact \
     distinct-value union exceeded the exact set limit, or a partial carried a bare \
     count with no key set — so the merged count would be an inexact saturating-add \
     upper bound; use APPROX_COUNT_DISTINCT / an approximate HLL aggregator for \
     distinct counts beyond the exact set limit)";

/// `DruidError::ResourceLimit::kind` string used when a partial result
/// tagged as an exact-cardinality [`CardinalityState`] envelope is
/// malformed or violates the envelope invariants (peer-supplied `count`
/// disagreeing with the value set, over-cap set, oversized key, wrong
/// field types).  The merged count cannot be verified exact, so the query
/// fails closed rather than trusting hostile/corrupt peer input.
pub const CARDINALITY_MALFORMED_STATE_KIND: &str = "cardinality.malformedState (a partial result tagged as an exact \
     COUNT(DISTINCT)/cardinality state envelope was malformed or violated the envelope \
     invariants, so the merged count could not be verified exact; the query fails closed \
     rather than trusting a peer-supplied count)";

/// Test-only override for the exact-set cap. `0` means "no override" (use
/// [`MAX_CARDINALITY_SET_SIZE`]). See
/// [`set_exact_cardinality_cap_for_tests`].
static EXACT_SET_CAP_OVERRIDE: AtomicUsize = AtomicUsize::new(0);

/// TEST-ONLY: lower the exact-cardinality set cap for every aggregator
/// constructed afterwards in this process.
///
/// Driving the real [`MAX_CARDINALITY_SET_SIZE`] (1,000,000 keys) in a test
/// is infeasible, so integration/e2e tests lower the cap to a small value
/// to exercise the fail-closed saturation path. The override is clamped to
/// `1..=MAX_CARDINALITY_SET_SIZE` — it can only LOWER the cap, never raise
/// it, so the Wave 36-G2 DoS bound holds even if this is called in
/// production by mistake. Passing `0` clears the override (back to the
/// production default). Production code must never call this.
pub fn set_exact_cardinality_cap_for_tests(cap: usize) {
    EXACT_SET_CAP_OVERRIDE.store(cap.min(MAX_CARDINALITY_SET_SIZE), Ordering::Relaxed);
}

/// The exact-set cap in effect for newly constructed aggregators:
/// [`MAX_CARDINALITY_SET_SIZE`] unless a test lowered it via
/// [`set_exact_cardinality_cap_for_tests`].
#[must_use]
pub fn exact_cardinality_set_cap() -> usize {
    match EXACT_SET_CAP_OVERRIDE.load(Ordering::Relaxed) {
        0 => MAX_CARDINALITY_SET_SIZE,
        v => v.max(1),
    }
}

/// Length cap (in bytes) for any single canonical key stored in the set.
///
/// Wave 36-G2: protects against a single very-long string value blowing out
/// memory even when distinct-count stays low.
///
/// Exactness fix (2026-07-12, Codex HIGH finding 3): keys beyond this
/// length are no longer prefix-truncated (two distinct values sharing a
/// 4096-byte prefix silently counted as ONE, with `saturated = false` — a
/// silent under-count).  An over-long key is now replaced by
/// `…sha256:<hex of SHA-256(full key)>`, which is both far below this
/// bound (74 bytes) and collision-free for all practical purposes
/// (2⁻¹²⁸ birthday bound), so distinct values map to distinct keys while
/// the per-key memory bound holds.  The digest is a stable function of the
/// full key bytes, so the same value observed on different shards still
/// unions to one key.
pub const MAX_CARDINALITY_KEY_BYTES: usize = 4096;

/// Aggregator that computes exact cardinality using a `HashSet`.
///
/// Each unique JSON value (serialized to string) is tracked. The result is the
/// count of distinct values, capped at [`MAX_CARDINALITY_SET_SIZE`].
///
/// Wave 45-E (Wave 37B Medium #3 `aggregator/lib.rs:183-191`,
/// `cardinality.rs:40-67`): a Druid `cardinality` aggregator may be configured
/// with multiple fields.  The aggregator now stores the configured field
/// list and offers [`aggregate_row_values`](Self::aggregate_row_values) to
/// feed a whole row of per-field values in one call.  Two semantics are
/// supported, matching the upstream Druid spec:
///
/// * `by_row = false` — per-field tagged distincts summed across fields:
///   each field contributes its own distinct-set independently of every
///   other field, so `cardinality(a, b)` = `|distinct(a)| + |distinct(b)|`
///   when the per-field sets are disjoint, and the per-field sets are kept
///   distinct in the underlying `HashSet` by tagging every key with a
///   4-byte big-endian field index prefix (`f<idx_be>:<value_to_key>`).
///   This matches Druid's `cardinality(byRow=false)` semantics where a value
///   `"x"` observed in column `a` is a different distinct from the same
///   `"x"` observed in column `b` (Wave 54-A; closes Wave 52 STILL-OPEN
///   `aggregator/cardinality.rs:253-259`).
/// * `by_row = true` — tuple semantics: the per-row contribution is a single
///   length-prefixed composite of all field values, so
///   `cardinality_by_row(a, b)` = `|distinct (a, b) tuples|`.  Tuple
///   composition already encodes field position via the row layout, so no
///   additional per-field tag is required.
#[derive(Debug, Clone)]
pub struct CardinalityAggregator {
    /// Unique values observed so far (stored as canonical string representations).
    seen: HashSet<String>,
    /// Whether to compute cardinality by row (combining field values) or per-field.
    by_row: bool,
    /// Configured field list this aggregator was constructed with.  Empty for
    /// the legacy single-field call site that drives the
    /// [`Aggregator::aggregate`] trait method directly with one value at a
    /// time.  When non-empty, callers should use
    /// [`aggregate_row_values`](Self::aggregate_row_values) and supply one
    /// `Option<Value>` per configured field.
    fields: Vec<String>,
    /// Accumulated values for the current row when `by_row` is true.
    row_parts: Vec<String>,
    /// True once the cap has been hit at least once.
    saturated: bool,
    /// Per-instance exact-set cap. Always [`MAX_CARDINALITY_SET_SIZE`] in
    /// production; tests lower it via [`Self::with_cap_for_tests`] or the
    /// process-wide [`set_exact_cardinality_cap_for_tests`] override.
    cap: usize,
}

impl CardinalityAggregator {
    /// Create a new cardinality aggregator with no configured field list.
    ///
    /// Equivalent to `with_fields(by_row, vec![])`.  The aggregator behaves
    /// like the legacy single-field aggregator: callers feed values one at
    /// a time through [`Aggregator::aggregate`], and (when `by_row = true`)
    /// each call is treated as a single-field row.
    pub fn new(by_row: bool) -> Self {
        Self::with_fields(by_row, Vec::new())
    }

    /// Create a new cardinality aggregator that knows its configured field
    /// list.
    ///
    /// `fields` is the list of source-column names this aggregator was
    /// configured to read.  It is consulted by
    /// [`aggregate_row_values`](Self::aggregate_row_values) when the query
    /// layer feeds a whole row at once; the field list itself is opaque to
    /// the aggregator (the query layer is responsible for supplying values
    /// in the same order as `fields`).
    ///
    /// When `fields` is empty the aggregator falls back to the legacy
    /// single-field, one-call-per-row contract used by
    /// [`Aggregator::aggregate`].
    pub fn with_fields(by_row: bool, fields: Vec<String>) -> Self {
        Self {
            seen: HashSet::new(),
            by_row,
            fields,
            row_parts: Vec::new(),
            saturated: false,
            cap: exact_cardinality_set_cap(),
        }
    }

    /// TEST-ONLY: construct an aggregator with a lowered per-instance
    /// exact-set cap so unit tests can drive saturation without inserting
    /// [`MAX_CARDINALITY_SET_SIZE`] keys. The cap is clamped to
    /// `1..=MAX_CARDINALITY_SET_SIZE` (it can only lower the production
    /// bound, never raise it). Production code must use
    /// [`Self::new`] / [`Self::with_fields`], which apply the production
    /// default.
    #[must_use]
    pub fn with_cap_for_tests(by_row: bool, fields: Vec<String>, cap: usize) -> Self {
        let mut agg = Self::with_fields(by_row, fields);
        agg.cap = cap.clamp(1, MAX_CARDINALITY_SET_SIZE);
        agg
    }

    /// Returns the configured field list (may be empty for the legacy
    /// single-field call site).
    #[must_use]
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Feed all field values for a single row.
    ///
    /// `values` must contain exactly one entry per configured field, in the
    /// same order as `fields`.  Behaviour:
    ///
    /// * `by_row = false`: each value is inserted into the set tagged with
    ///   its position-in-`values` field index, so the cardinality is
    ///   `SUM_i |distinct(field_i)|` (per-field distincts summed across
    ///   fields).  Wave 54-A: a 4-byte big-endian field-index prefix is
    ///   added to each canonical key so that the same byte-level value
    ///   observed in different fields stays distinct (closes Wave 52
    ///   STILL-OPEN `cardinality.rs:253-259` — without the tag, a value
    ///   `"x"` in field `a` and the same `"x"` in field `b` collapsed to a
    ///   single set entry, under-counting by up to a factor of `fields.len()`).
    /// * `by_row = true`: the values are length-prefix encoded into a single
    ///   composite key (matching the wire-safe encoding documented on
    ///   [`finish_row`](Self::finish_row)) and inserted as one entry, so the
    ///   cardinality is `|distinct (v1, …, vk) tuples|`.
    ///
    /// The set cap [`MAX_CARDINALITY_SET_SIZE`] continues to apply globally
    /// regardless of how many fields are configured.
    pub fn aggregate_row_values(&mut self, values: &[Option<&serde_json::Value>]) {
        for (idx, v) in values.iter().enumerate() {
            // SAFETY: `idx` comes from `enumerate` over a slice; a slice
            // length always fits in `usize`, and we cast to `u32` because
            // `aggregate_field_at` takes a 32-bit index (a Druid query with
            // > 2^32 cardinality fields is not a real workload).  The
            // `try_into` clamp protects against the pathological case at
            // `u32::MAX` instead of panicking.
            #[allow(clippy::cast_possible_truncation)]
            let field_idx = u32::try_from(idx).unwrap_or(u32::MAX);
            self.aggregate_field_at(field_idx, *v);
        }
        if self.by_row {
            self.finish_row();
        }
    }

    /// Feed a single field value for the current row.
    ///
    /// For `by_row = false`, this immediately inserts the value into the set
    /// (subject to [`MAX_CARDINALITY_SET_SIZE`]).
    /// For `by_row = true`, call this for each field, then call
    /// [`finish_row`](Self::finish_row) to combine them.
    ///
    /// This single-field entry point is the legacy single-column call site
    /// (no field index is known) and inserts the value with no field-index
    /// tag; multi-field rows must go through
    /// [`aggregate_row_values`](Self::aggregate_row_values) (which calls
    /// [`aggregate_field_at`](Self::aggregate_field_at) per position) so
    /// each field's distinct-set is kept separate in the underlying
    /// `HashSet`.
    pub fn aggregate_field(&mut self, value: Option<&serde_json::Value>) {
        let key = value_to_key(value);
        if self.by_row {
            self.row_parts.push(key);
        } else {
            self.try_insert(key);
        }
    }

    /// Feed a single field value for the current row, tagged with its
    /// position-in-row field index.
    ///
    /// Wave 54-A (closes Wave 52 STILL-OPEN `cardinality.rs:253-259`): in
    /// `by_row = false` mode the per-field distinct-set must be kept
    /// separate per field, otherwise the same byte-level value observed in
    /// two different columns collapses to a single set entry and the
    /// cardinality under-counts.  This entry point prefixes the canonical
    /// key with `f<field_idx_be>:` (4 big-endian bytes of the index, then
    /// the literal `:`) so each field has its own subspace inside the
    /// shared `HashSet`.
    ///
    /// In `by_row = true` mode the field-index tag is unnecessary — the
    /// composite tuple already encodes per-field position via the row layout
    /// — so the per-part value is pushed onto `row_parts` without a tag and
    /// is finalised by [`finish_row`](Self::finish_row).
    ///
    /// Wave 57 (closes Wave 56 NEW-W56 Low — tag-bypass of per-key cap):
    /// `value_to_key` clips the raw value to [`MAX_CARDINALITY_KEY_BYTES`],
    /// but the 10-byte `f<8hex>:` field-index tag is appended *after* that
    /// clip.  Without a second clip the tagged key can exceed the documented
    /// per-key cap by `10 + "…trunc".len()` bytes.  We re-clip the tagged
    /// key so the bound holds for every key inserted into `seen`, matching
    /// the [`finish_row`](Self::finish_row) composite-cap contract.
    pub fn aggregate_field_at(&mut self, field_idx: u32, value: Option<&serde_json::Value>) {
        let raw = value_to_key(value);
        if self.by_row {
            // Tuple semantics — composite key is built in `finish_row`,
            // which already encodes positional information via the
            // length-prefix layout.
            self.row_parts.push(raw);
        } else {
            // Per-field union semantics — tag the key with the 4-byte BE
            // field index so the same `value_to_key` output observed in
            // different fields does not collide in `seen`.
            let bytes = field_idx.to_be_bytes();
            // Encode the 4 raw bytes as 8 hex chars so the combined key
            // stays a `String` (the `seen: HashSet<String>` and
            // `CardinalityState.values: Vec<String>` wire shape are
            // unchanged).  Hex keeps the prefix self-delimited (`f` + 8
            // hex digits + `:`) so it cannot be confused with any
            // `value_to_key` prefix (`s:` / `n:` / `b:` / `j:` / `__null__`).
            let mut tagged = String::with_capacity(10 + raw.len());
            tagged.push('f');
            for b in bytes {
                use std::fmt::Write as _;
                // `write!` to a `String` cannot fail; ignore the
                // `fmt::Result` to keep the path `unwrap`-free.
                let _ = write!(tagged, "{b:02x}");
            }
            tagged.push(':');
            tagged.push_str(&raw);
            // Wave 57: re-clip the *full* tagged key (tag + value) so the
            // 10-byte tag cannot push the inserted key past the cap that
            // `value_to_key` only enforces on the value portion.
            let capped = clip_to_key_cap(tagged);
            self.try_insert(capped);
        }
    }

    /// When `by_row = true`, finalize the current row by combining accumulated
    /// field values into a single composite key and inserting it.
    ///
    /// Wave 36-G2 (Wave 37B Medium `cardinality.rs:52-57`): composite keys
    /// are now length-prefix encoded (`<len>:<part>` per field) so that
    /// field values containing the previous `\x00` delimiter cannot be
    /// confused with a different tuple decomposition.
    ///
    /// Wave 54-A (closes Wave 52 NEW-W45 Low — composite key length cap
    /// bypass): per-part values are clipped to
    /// [`MAX_CARDINALITY_KEY_BYTES`] inside [`value_to_key`], but a wide
    /// multi-field composite (e.g. 50 string fields each near the per-part
    /// cap) could still produce a final key well above the documented
    /// per-key cap.  We therefore also clip the *composite* key.
    /// Exactness fix (2026-07-12, Codex HIGH finding 3): the clip is now a
    /// full-key SHA-256 digest, so two distinct composites never collapse
    /// to one set entry (see [`clip_to_key_cap`]) while the per-key memory
    /// bound still holds.
    pub fn finish_row(&mut self) {
        if self.by_row && !self.row_parts.is_empty() {
            let mut composite = String::new();
            for part in &self.row_parts {
                composite.push_str(&part.len().to_string());
                composite.push(':');
                composite.push_str(part);
                composite.push('\x00');
            }
            let capped = clip_to_key_cap(composite);
            self.try_insert(capped);
            self.row_parts.clear();
        }
    }

    /// Returns `true` once any insert has been refused due to the size cap.
    pub fn saturated(&self) -> bool {
        self.saturated
    }

    /// Number of distinct entries currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Returns `true` when no entries have been inserted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    /// Iterate over the canonical keys currently tracked. Used by the typed
    /// merge path.
    pub fn iter_keys(&self) -> impl Iterator<Item = &String> {
        self.seen.iter()
    }

    /// Serialize this aggregator's exact set into a [`CardinalityState`]
    /// suitable for shipping to a broker that will union with other
    /// shards' states.
    ///
    /// Multi-shard exact union (2026-07-11): the envelope carries the FULL
    /// exact set (up to this aggregator's own cap — the production
    /// [`MAX_CARDINALITY_SET_SIZE`]).  It degrades to `saturated = true`
    /// only when this aggregator itself saturated (per-shard cap hit), in
    /// which case the keys are dropped and the merge falls back to a
    /// saturating-add upper bound that the broker fails closed.  The
    /// former 1,000-key wire cap is gone; see the [`CardinalityState`]
    /// docs for the single-binary vs classic-distributed rationale.
    ///
    /// Wave 40-B closes Wave 39 [High] [NEW-VARIANT]
    /// `aggregator/lib.rs:88-129` — the broker can compute a true
    /// union over per-shard sets instead of saturating-add of counts.
    #[must_use]
    pub fn into_state(&self) -> CardinalityState {
        #[allow(clippy::cast_possible_truncation)]
        let count = self.seen.len() as u64;
        if self.saturated {
            return CardinalityState::saturated_with_count(count);
        }
        CardinalityState {
            typ: CARDINALITY_STATE_TAG.to_owned(),
            values: self.seen.iter().cloned().collect(),
            saturated: false,
            count,
        }
    }

    /// Hydrate a [`CardinalityState`] back into a `CardinalityAggregator`
    /// so it can be merged via the typed [`Aggregator::merge`] path.  The
    /// `by_row` flag is irrelevant for an already-finalized state — it is
    /// only consulted at row-feeding time, which a hydrated aggregator
    /// will not see.
    ///
    /// Untrusted-peer hardening (2026-07-12, Codex HIGH findings 4+5): the
    /// state may be peer-supplied, so hydration fails closed on anything
    /// inconsistent instead of trusting it:
    ///
    /// * values beyond the exact-set cap are not inserted (bounding both
    ///   memory and the pre-allocation) and force `saturated = true` — an
    ///   over-cap set can never claim to be exact;
    /// * a non-saturated state whose `count` disagrees with its actual
    ///   distinct value set forces `saturated = true` — the peer-supplied
    ///   count is never presented as exact.
    #[must_use]
    pub fn from_state(state: &CardinalityState) -> Self {
        Self::from_state_with_cap(state, exact_cardinality_set_cap())
    }

    /// [`Self::from_state`] with an explicit exact-set cap (separated out
    /// so unit tests can drive the over-cap fail-closed path without
    /// materializing [`MAX_CARDINALITY_SET_SIZE`] keys).
    fn from_state_with_cap(state: &CardinalityState, cap: usize) -> Self {
        let mut seen: HashSet<String> = HashSet::with_capacity(state.values.len().min(cap));
        let mut saturated = state.saturated;
        for v in &state.values {
            if seen.len() >= cap {
                if !seen.contains(v) {
                    // Fail closed: the set exceeds the cap, so an exact
                    // count can no longer be claimed (Codex finding 5 —
                    // the old hydration carried an over-cap set with
                    // `saturated = false`, bypassing finalization).
                    saturated = true;
                }
                continue;
            }
            seen.insert(v.clone());
        }
        // Fail closed on a forged count: for a state claiming exactness
        // the count must equal the actual distinct set (Codex finding 4).
        if !saturated && usize::try_from(state.count) != Ok(seen.len()) {
            saturated = true;
        }
        Self {
            seen,
            by_row: false,
            fields: Vec::new(),
            row_parts: Vec::new(),
            saturated,
            cap,
        }
    }

    /// Insert respecting the cap.
    fn try_insert(&mut self, key: String) {
        // Always allow updating existing keys (no-op insert) so saturated
        // aggregators can still observe duplicates without flipping the flag.
        if self.seen.contains(&key) {
            return;
        }
        if self.seen.len() >= self.cap {
            self.saturated = true;
            return;
        }
        self.seen.insert(key);
    }
}

impl Aggregator for CardinalityAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        // Single-field shortcut: treat each call as one field, one row.
        self.aggregate_field(value);
        if self.by_row {
            self.finish_row();
        }
    }

    fn aggregate_multi(&mut self, values: &[Option<&serde_json::Value>]) {
        // Wave 45-E: route multi-field rows through the row-aware path so
        // both `by_row` modes honour the configured field list.
        self.aggregate_row_values(values);
    }

    fn get(&self) -> serde_json::Value {
        #[allow(clippy::cast_possible_truncation)]
        let count = self.seen.len() as u64;
        serde_json::Value::Number(serde_json::Number::from(count))
    }

    fn saturation(&self) -> Option<AggregatorSaturation> {
        // Fail-closed (2026-07-11): once the exact set has refused an
        // insert, `get()` would return a silently under-counted number.
        // Finalization layers consult this and fail the query instead.
        if self.saturated {
            Some(AggregatorSaturation {
                kind: CARDINALITY_EXACT_SET_LIMIT_KIND,
                limit: self.cap,
                observed: self.seen.len(),
            })
        } else {
            None
        }
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        // Wave 36-G2 (Wave 37B High `cardinality.rs:73-90`): the previous
        // implementation read `other.get()` and inserted synthetic
        // `__merge_{i}` keys that double-counted overlapping shards. The
        // correct merge is a set union over the underlying `seen` HashSet.
        //
        // Path A — both sides are `CardinalityAggregator`: union the sets.
        // A `FilteredAggregator` wrapper (the E16 exact COUNT(DISTINCT)
        // lowering) is unwrapped first so filtered(cardinality) merges
        // stay exact set unions instead of degrading to Path B.
        // Path B — `other` is some unknown `dyn Aggregator` (only happens
        // when callers mix aggregator types in a scatter/gather pipeline);
        // its count is an opaque per-shard number that cannot be exactly
        // de-duplicated against our distinct population.
        if let Some(other_any) = other.as_any() {
            if let Some(other_card) = other_any.downcast_ref::<CardinalityAggregator>() {
                for k in &other_card.seen {
                    self.try_insert(k.clone());
                }
                if other_card.saturated {
                    self.saturated = true;
                }
                return;
            }
            if let Some(filtered) = other_any.downcast_ref::<crate::FilteredAggregator>() {
                self.merge(filtered.inner());
                return;
            }
        }

        // Path B fallback: bump our running count by `other.get()`'s count
        // up to the cap, using opaque sentinel keys keyed by the donor's
        // identity hash so repeated merges of the same `other` are
        // idempotent (no re-injection on a second merge call).
        //
        // Fail-closed (2026-07-12, Codex HIGH finding 2): the opaque keys
        // may overlap the real distinct population, so the additive result
        // is only an upper bound — the merge marks itself `saturated` so
        // finalization fails closed instead of presenting a plausible
        // exact count.  A zero contribution is the exact identity and
        // stays exact; a donor whose value is not a u64 count is
        // fail-closed too (its contribution is unknowable).
        let other_count = other.get();
        match other_count.as_u64() {
            Some(0) => {}
            Some(n) => {
                self.saturated = true;
                // Use a stable prefix derived from the donor's data pointer
                // so different `other` aggregators produce different keys
                // but the same `other` merged twice does not double-count.
                // Casting a wide trait-object pointer to `usize` requires
                // going through a thin pointer first.
                let thin: *const () = (other as *const dyn Aggregator).cast::<()>();
                let prefix = thin as usize;
                for i in 0..n {
                    if self.seen.len() >= self.cap {
                        break;
                    }
                    self.try_insert(format!("__opaque:{prefix:x}:{i}"));
                }
            }
            None => {
                self.saturated = true;
            }
        }
    }

    fn reset(&mut self) {
        self.seen.clear();
        self.row_parts.clear();
        self.saturated = false;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }
}

/// Extract the mergeable partial form for an exact-cardinality aggregator:
/// the full-set [`CardinalityState`] envelope (as JSON), unwrapping
/// [`FilteredAggregator`](crate::FilteredAggregator) layers (the E16 exact
/// `COUNT(DISTINCT)` SQL lowering wraps `cardinality` in a not-null
/// `filtered`).  Returns `None` for every other aggregator kind, so callers
/// can use it as a "is this an exact-cardinality partial?" probe.
///
/// Multi-shard exact union (2026-07-11): the query executors emit this
/// envelope — instead of the bare `get()` count — as the per-segment
/// partial for exact-cardinality outputs, so the broker merge can compute
/// the true cross-segment set union.  Callers must run the fail-closed
/// saturation check (`Aggregator::saturation`) BEFORE emitting; a
/// saturated aggregator never reaches this point on the executor paths.
#[must_use]
pub fn exact_cardinality_partial(agg: &dyn Aggregator) -> Option<serde_json::Value> {
    let any = agg.as_any()?;
    if let Some(card) = any.downcast_ref::<CardinalityAggregator>() {
        return Some(card.into_state().to_json());
    }
    if let Some(filtered) = any.downcast_ref::<crate::FilteredAggregator>() {
        return exact_cardinality_partial(filtered.inner());
    }
    None
}

/// Convert a JSON value to a canonical string key, clipped to
/// [`MAX_CARDINALITY_KEY_BYTES`] via the collision-free digest form of
/// [`clip_to_key_cap`] to bound memory.
fn value_to_key(value: Option<&serde_json::Value>) -> String {
    // Codex round-2: for a LARGE string value, do NOT build the full
    // `format!("s:{s}")` copy before clipping — the value is already
    // materialized (bounded upstream), and the extra full-length copy doubles
    // the transient allocation. Hash the tag + the value's bytes DIRECTLY when
    // the value already exceeds the stored-key cap, so no oversized `String`
    // is ever built. Exactness is preserved (the hash is over the full bytes).
    match value {
        None | Some(serde_json::Value::Null) => "__null__".to_string(),
        Some(serde_json::Value::String(s)) if s.len() > MAX_CARDINALITY_KEY_BYTES => {
            sha_key_of(b"s:", s.as_bytes())
        }
        Some(serde_json::Value::String(s)) => clip_to_key_cap(format!("s:{s}")),
        Some(serde_json::Value::Number(n)) => clip_to_key_cap(format!("n:{n}")),
        Some(serde_json::Value::Bool(b)) => clip_to_key_cap(format!("b:{b}")),
        Some(other) => clip_to_key_cap(format!("j:{other}")),
    }
}

/// Stored-key SHA-256 of `tag || bytes`, in the same `…sha256:<hex>` form
/// [`clip_to_key_cap`] emits — used to key an over-cap value WITHOUT first
/// materializing an oversized `tag+value` copy (Codex round-2 OOM guard).
fn sha_key_of(tag: &[u8], bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(tag);
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(74);
    out.push_str("…sha256:");
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Clip an arbitrary key string to [`MAX_CARDINALITY_KEY_BYTES`].  Used
/// both by [`value_to_key`] for per-part values and by
/// [`CardinalityAggregator::finish_row`] for the composite tuple key
/// (Wave 54-A — closes Wave 52 NEW-W45 Low).
///
/// Exactness fix (2026-07-12, Codex HIGH finding 3): the previous
/// prefix-truncation (`<4096-byte prefix>…trunc`) collapsed two DISTINCT
/// values sharing a long prefix into ONE key — a silent under-count with
/// `saturated = false`, violating the exact-count contract.  An over-cap
/// key is now replaced by the hex SHA-256 digest of the FULL key bytes
/// (74-byte stored form `…sha256:<64 hex>`), so distinct values map to
/// distinct keys (collision odds ≈ 2⁻¹²⁸ per pair — cryptographically
/// negligible) while memory stays bounded.  The `…` (U+2026) lead-in
/// cannot collide with any unclipped key: every unclipped canonical key
/// starts with a `value_to_key` type prefix (`s:`/`n:`/`b:`/`j:`/
/// `__null__`), a field tag (`f<8 hex>:`), or a length digit (composite
/// tuples).  The digest is a pure function of the key bytes — no
/// process-local state — so shards clip identically and cross-shard
/// unions still de-duplicate the same underlying value.
fn clip_to_key_cap(raw: String) -> String {
    use sha2::{Digest, Sha256};
    if raw.len() <= MAX_CARDINALITY_KEY_BYTES {
        raw
    } else {
        let digest = Sha256::digest(raw.as_bytes());
        let mut clipped = String::with_capacity(74);
        clipped.push_str("…sha256:");
        for b in digest {
            use std::fmt::Write as _;
            // `write!` to a `String` cannot fail; ignore the `fmt::Result`
            // to keep the path `unwrap`-free.
            let _ = write!(clipped, "{b:02x}");
        }
        clipped
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cardinality_empty() {
        let agg = CardinalityAggregator::new(false);
        assert_eq!(agg.get(), json!(0));
    }

    #[test]
    fn cardinality_distinct_strings() {
        let mut agg = CardinalityAggregator::new(false);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(Some(&json!("b")));
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(Some(&json!("c")));
        assert_eq!(agg.get(), json!(3));
    }

    #[test]
    fn cardinality_distinct_numbers() {
        let mut agg = CardinalityAggregator::new(false);
        agg.aggregate(Some(&json!(1)));
        agg.aggregate(Some(&json!(2)));
        agg.aggregate(Some(&json!(1)));
        agg.aggregate(Some(&json!(3)));
        assert_eq!(agg.get(), json!(3));
    }

    #[test]
    fn cardinality_with_nulls() {
        let mut agg = CardinalityAggregator::new(false);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(None);
        agg.aggregate(Some(&json!(null)));
        agg.aggregate(Some(&json!("b")));
        // null and None map to the same key
        assert_eq!(agg.get(), json!(3)); // "a", null, "b"
    }

    #[test]
    fn cardinality_mixed_types() {
        let mut agg = CardinalityAggregator::new(false);
        agg.aggregate(Some(&json!("1")));
        agg.aggregate(Some(&json!(1)));
        // String "1" and number 1 are distinct.
        assert_eq!(agg.get(), json!(2));
    }

    #[test]
    fn cardinality_reset() {
        let mut agg = CardinalityAggregator::new(false);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(Some(&json!("b")));
        assert_eq!(agg.get(), json!(2));
        agg.reset();
        assert_eq!(agg.get(), json!(0));
    }

    #[test]
    fn cardinality_by_row() {
        let mut agg = CardinalityAggregator::new(true);

        // Row 1: fields "a", "x"
        agg.aggregate_field(Some(&json!("a")));
        agg.aggregate_field(Some(&json!("x")));
        agg.finish_row();

        // Row 2: fields "b", "y"
        agg.aggregate_field(Some(&json!("b")));
        agg.aggregate_field(Some(&json!("y")));
        agg.finish_row();

        // Row 3: fields "a", "x" — same as row 1
        agg.aggregate_field(Some(&json!("a")));
        agg.aggregate_field(Some(&json!("x")));
        agg.finish_row();

        assert_eq!(agg.get(), json!(2)); // 2 unique combinations
    }

    #[test]
    fn cardinality_by_row_via_aggregate() {
        // When using the generic aggregate() with by_row=true, each call is
        // treated as a single-field row.
        let mut agg = CardinalityAggregator::new(true);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(Some(&json!("b")));
        agg.aggregate(Some(&json!("a")));
        assert_eq!(agg.get(), json!(2));
    }

    #[test]
    fn cardinality_clone_box() {
        let mut agg = CardinalityAggregator::new(false);
        agg.aggregate(Some(&json!("x")));
        agg.aggregate(Some(&json!("y")));
        let cloned = agg.clone_box();
        assert_eq!(cloned.get(), json!(2));
    }

    // -----------------------------------------------------------------------
    // Wave 36-G2 regression tests
    // -----------------------------------------------------------------------

    #[test]
    fn cardinality_merge_unions_sets_not_fabricates() {
        // Wave 37B High `cardinality.rs:73-90`: the previous implementation
        // synthesized `__merge_{i}` keys, producing a count equal to the
        // sum of both sides regardless of overlap. The correct semantics is
        // a set union.
        let mut a = CardinalityAggregator::new(false);
        for v in 0..10u64 {
            a.aggregate(Some(&json!(v)));
        }
        let mut b = CardinalityAggregator::new(false);
        for v in 5..15u64 {
            b.aggregate(Some(&json!(v)));
        }
        // Disjoint values: 0..10 ∪ 5..15 = 0..15 = 15 distinct entries.
        a.merge(&b);
        let count = a.get().as_u64().expect("u64 result");
        assert_eq!(
            count, 15,
            "merge must union sets; got {count}, expected 15 (10 ∪ 10 with 5 overlap)"
        );
        // Strictly NOT 20 (sum-with-fabrication).
        assert!(count < 20, "must not fabricate phantom keys");
        // Strictly NOT 2 (the failure mode the prompt warned about).
        assert!(count > 2);
    }

    #[test]
    fn cardinality_merge_disjoint_unions_to_full_sum() {
        let mut a = CardinalityAggregator::new(false);
        for v in 0..10u64 {
            a.aggregate(Some(&json!(format!("a-{v}"))));
        }
        let mut b = CardinalityAggregator::new(false);
        for v in 0..10u64 {
            b.aggregate(Some(&json!(format!("b-{v}"))));
        }
        a.merge(&b);
        assert_eq!(a.get(), json!(20));
    }

    #[test]
    fn cardinality_merge_idempotent_on_same_other() {
        let mut a = CardinalityAggregator::new(false);
        a.aggregate(Some(&json!("x")));
        let mut b = CardinalityAggregator::new(false);
        b.aggregate(Some(&json!("y")));
        a.merge(&b);
        let after_first = a.get();
        a.merge(&b);
        let after_second = a.get();
        assert_eq!(
            after_first, after_second,
            "merging the same set twice must be idempotent"
        );
        assert_eq!(after_first, json!(2));
    }

    #[test]
    fn cardinality_rejects_when_unique_count_exceeds_cap() {
        let mut agg = CardinalityAggregator::new(false);
        for v in 0..1024u64 {
            agg.aggregate(Some(&json!(v)));
        }
        assert_eq!(agg.len(), 1024);
        assert!(!agg.saturated());

        // Verify the per-key length cap is wired.
        let huge: String = "x".repeat(10_000);
        agg.aggregate(Some(&json!(huge)));
        let max_len = agg.iter_keys().map(String::len).max().unwrap_or(0);
        assert!(
            max_len <= MAX_CARDINALITY_KEY_BYTES + "…trunc".len(),
            "key length {max_len} exceeds bound"
        );
    }

    #[test]
    fn cardinality_byrow_length_prefix_disambiguates_nul_in_value() {
        // Wave 37B Medium `cardinality.rs:52-57`: the previous join("\x00")
        // could conflate distinct field tuples when a value contained NUL.
        // With length-prefix encoding, ("a\x00b", "c") and ("a", "b\x00c")
        // must hash distinctly.
        let mut agg = CardinalityAggregator::new(true);

        agg.aggregate_field(Some(&json!("a\x00b")));
        agg.aggregate_field(Some(&json!("c")));
        agg.finish_row();

        agg.aggregate_field(Some(&json!("a")));
        agg.aggregate_field(Some(&json!("b\x00c")));
        agg.finish_row();

        assert_eq!(
            agg.get(),
            json!(2),
            "length-prefix encoding must keep NUL-bearing values distinct"
        );
    }

    #[test]
    fn cardinality_clips_long_values_to_avoid_oom_without_colliding() {
        let mut agg = CardinalityAggregator::new(false);
        let big_a: String = "a".repeat(100_000);
        let big_b: String = "a".repeat(100_001);
        agg.aggregate(Some(&json!(big_a.clone())));
        agg.aggregate(Some(&json!(big_b)));
        // Exactness fix (2026-07-12, Codex HIGH finding 3): the clip is a
        // full-key digest, so the two DISTINCT long values count as 2 (the
        // old prefix truncation silently collapsed them to 1) while the
        // stored keys stay bounded.
        assert_eq!(agg.get(), json!(2));
        // The same value fed twice still counts once.
        agg.aggregate(Some(&json!(big_a)));
        assert_eq!(agg.get(), json!(2));
        let max_len = agg.iter_keys().map(String::len).max().unwrap_or(0);
        assert!(
            max_len <= MAX_CARDINALITY_KEY_BYTES,
            "clipped keys must stay within the per-key bound, got {max_len}"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 40-B: typed CardinalityState round-trip + merge
    // (Wave 39 [High] [NEW-VARIANT] aggregator/lib.rs:88-129)
    // -----------------------------------------------------------------------

    #[test]
    fn cardinality_state_round_trip_preserves_set() {
        let mut agg = CardinalityAggregator::new(false);
        for v in 0..20u64 {
            agg.aggregate(Some(&json!(format!("k{v}"))));
        }
        let state = agg.into_state();
        assert!(!state.saturated);
        assert_eq!(state.count, 20);
        // JSON round-trip preserves the @type tag.
        let json = state.to_json();
        let back = CardinalityState::from_json(&json)
            .expect("well-formed envelope")
            .expect("tagged envelope");
        assert_eq!(state, back);
    }

    /// Multi-shard exact union (2026-07-11): the former 1,000-key wire cap
    /// is gone — a non-saturated set above 1,000 keys ships its FULL exact
    /// set (this is what makes a single segment with e.g. 5,000 distinct
    /// values stay exact through the broker's envelope finalization).
    #[test]
    fn cardinality_state_ships_full_set_above_former_wire_cap() {
        let mut agg = CardinalityAggregator::new(false);
        for v in 0..1_500u64 {
            agg.aggregate(Some(&json!(format!("u{v}"))));
        }
        let state = agg.into_state();
        assert!(
            !state.saturated,
            "a non-saturated exact set must ship exact regardless of size"
        );
        assert_eq!(state.values.len(), 1_500);
        assert_eq!(state.count, 1_500);
    }

    /// A shard whose aggregator saturated (per-instance cap refused an
    /// insert) still ships a saturated envelope with the keys dropped.
    #[test]
    fn cardinality_state_saturates_when_aggregator_saturated() {
        let mut agg = CardinalityAggregator::with_cap_for_tests(false, Vec::new(), 4);
        for v in 0..6u64 {
            agg.aggregate(Some(&json!(format!("u{v}"))));
        }
        assert!(agg.saturated());
        let state = agg.into_state();
        assert!(
            state.saturated,
            "saturated aggregator must saturate the envelope"
        );
        assert!(state.values.is_empty(), "saturated state drops values");
        assert_eq!(state.count, 4, "count reflects the capped set size");
    }

    /// A union whose exact result exceeds the exact-set cap saturates (the
    /// broker finalization pass then fails the query closed).  Driven via
    /// the explicit-cap variant so the process-wide test override is not
    /// touched (it would race parallel tests in this binary).
    #[test]
    fn cardinality_state_union_over_cap_saturates() {
        let mk = |range: std::ops::Range<u64>| {
            let mut agg = CardinalityAggregator::new(false);
            for v in range {
                agg.aggregate(Some(&json!(v)));
            }
            agg.into_state()
        };
        // 0..4 ∪ 2..6 = 0..6 = 6 distinct > cap 5.
        let a = mk(0..4);
        let b = mk(2..6);
        let merged = CardinalityState::union_with_cap(&a, &b, 5);
        assert!(merged.saturated, "over-cap union must saturate");
        assert!(merged.values.is_empty());
        assert_eq!(merged.count, 6, "count carries the point of overflow");
        // The same union under a sufficient cap is exact.
        let merged = CardinalityState::union_with_cap(&a, &b, 6);
        assert!(!merged.saturated);
        assert_eq!(merged.count, 6);
    }

    /// `peek_json` reads (saturated, count) without deserializing the
    /// values array, and rejects non-envelope shapes.  The non-validating
    /// `peek_json_unchecked` variant agrees on all well-formed shapes.
    #[test]
    fn cardinality_state_peek_json_matches_from_json() {
        let mut agg = CardinalityAggregator::new(false);
        for v in 0..7u64 {
            agg.aggregate(Some(&json!(v)));
        }
        let state = agg.into_state();
        let json = state.to_json();
        assert_eq!(CardinalityState::peek_json(&json), Ok(Some((false, 7))));
        let sat = CardinalityState::saturated_with_count(9).to_json();
        assert_eq!(CardinalityState::peek_json(&sat), Ok(Some((true, 9))));
        assert_eq!(CardinalityState::peek_json(&json!(42)), Ok(None));
        assert_eq!(CardinalityState::peek_json(&json!({"count": 3})), Ok(None));
        assert_eq!(
            CardinalityState::peek_json_unchecked(&json),
            Some((false, 7))
        );
        assert_eq!(CardinalityState::peek_json_unchecked(&sat), Some((true, 9)));
        assert_eq!(CardinalityState::peek_json_unchecked(&json!(42)), None);
    }

    /// `exact_cardinality_partial` produces the envelope for a bare
    /// cardinality aggregator AND through a `FilteredAggregator` wrapper
    /// (the E16 exact COUNT(DISTINCT) lowering), and `None` for other
    /// aggregator kinds.
    #[test]
    fn exact_cardinality_partial_unwraps_filtered_and_rejects_others() {
        let mut card = CardinalityAggregator::new(false);
        card.aggregate(Some(&json!("a")));
        card.aggregate(Some(&json!("b")));
        let direct = exact_cardinality_partial(&card).expect("bare cardinality");
        assert_eq!(CardinalityState::peek_json(&direct), Ok(Some((false, 2))));

        let filtered = crate::FilteredAggregator::new(
            json!({"type": "not", "field": {"type": "selector", "dimension": "d", "value": null}}),
            Box::new(card),
        );
        let wrapped = exact_cardinality_partial(&filtered).expect("filtered(cardinality)");
        assert_eq!(CardinalityState::peek_json(&wrapped), Ok(Some((false, 2))));

        let spec: crate::AggregatorSpec =
            serde_json::from_value(json!({"type": "count", "name": "cnt"})).expect("spec");
        let count_agg = spec.create();
        assert!(
            exact_cardinality_partial(count_agg.as_ref()).is_none(),
            "non-cardinality aggregators must not produce envelopes"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 45-E: multi-field cardinality (close W37B aggregator Medium #3)
    // -----------------------------------------------------------------------

    /// `by_row = false` over multiple fields means: per-field tagged
    /// distincts summed across fields.  Wave 54-A (closes Wave 52
    /// STILL-OPEN — Codex DD R5 v1+v2 independently confirmed Druid's
    /// `cardinality(byRow=false)` keeps each field's distinct-set
    /// separate; the previous implementation un-tagged the per-field
    /// values and collapsed e.g. `a="x"` and `b="x"` to one entry,
    /// under-counting by up to a factor of `fields.len()`).  Rows
    /// `(a=1,b=1), (a=2,b=1), (a=2,b=2)` now contribute the per-field
    /// tagged set
    /// `{(idx=0, 1), (idx=0, 2), (idx=1, 1), (idx=1, 2)}` → 4 distinct
    /// entries — i.e. `|distinct(a)| + |distinct(b)| = 2 + 2 = 4`.
    #[test]
    fn cardinality_multi_field_default_per_field_tagged_distincts() {
        let mut agg = CardinalityAggregator::with_fields(false, vec!["a".into(), "b".into()]);
        agg.aggregate_row_values(&[Some(&json!(1)), Some(&json!(1))]);
        agg.aggregate_row_values(&[Some(&json!(2)), Some(&json!(1))]);
        agg.aggregate_row_values(&[Some(&json!(2)), Some(&json!(2))]);
        // Per-field tagged distincts:
        //   field a sees {1, 2}              → 2 distinct
        //   field b sees {1, 2}              → 2 distinct
        //   sum                              → 4
        assert_eq!(
            agg.get(),
            json!(4),
            "by_row=false multi-field must keep per-field distinct sets separate"
        );
    }

    /// Wave 54-A: a value `"x"` observed in field `a` and the same value
    /// `"x"` observed in field `b` must NOT collapse to a single set
    /// entry.  The previous untagged code lost the field origin and
    /// returned 1 for two rows `(a="x", b="y"), (a="y", b="x")`; the
    /// per-field-tagged code returns 4 (two distinct values in each of
    /// the two fields).
    #[test]
    fn cardinality_multi_field_same_value_in_two_fields_does_not_collapse() {
        let mut agg = CardinalityAggregator::with_fields(false, vec!["a".into(), "b".into()]);
        agg.aggregate_row_values(&[Some(&json!("x")), Some(&json!("y"))]);
        agg.aggregate_row_values(&[Some(&json!("y")), Some(&json!("x"))]);
        // field a sees {"x", "y"} → 2; field b sees {"x", "y"} → 2;
        // per-field tagged sum → 4.  The pre-Wave-54-A code returned 2
        // (untagged union of {"x", "y"}).
        assert_eq!(
            agg.get(),
            json!(4),
            "field-position must be part of the key in by_row=false"
        );
    }

    /// Wave 54-A: a single-field aggregator (legacy call site,
    /// `aggregate(value)` directly) MUST keep the simple union semantics
    /// — there is only one field, so the per-field tag would be constant
    /// and add no information.  The legacy `aggregate_field` (no field
    /// index) path stays untagged so the wire shape and existing
    /// per-aggregator behaviour are preserved.
    #[test]
    fn cardinality_single_field_legacy_path_unchanged_by_w54a() {
        let mut agg = CardinalityAggregator::new(false);
        agg.aggregate(Some(&json!("x")));
        agg.aggregate(Some(&json!("y")));
        agg.aggregate(Some(&json!("x"))); // dup
        assert_eq!(agg.get(), json!(2));
    }

    /// Wave 54-A — closes Wave 52 NEW-W45 Low (composite key length cap
    /// bypass): a wide multi-field `by_row=true` cardinality query that
    /// concatenates many per-part values must NOT produce a final
    /// composite key larger than `MAX_CARDINALITY_KEY_BYTES`.
    #[test]
    fn cardinality_finish_row_caps_oversized_composite_at_max_bytes() {
        let mut agg = CardinalityAggregator::new(true);
        // Build a row of ~50 string parts of ~200 chars each.  Each part
        // is well below the per-part cap (so `value_to_key` does not
        // truncate it), but the concatenation is ~10K bytes which is far
        // beyond `MAX_CARDINALITY_KEY_BYTES` (4K).
        for i in 0..50u32 {
            let part = format!("part_{i:04}_{}", "x".repeat(200));
            agg.aggregate_field(Some(&json!(part)));
        }
        agg.finish_row();
        let max_len = agg.iter_keys().map(String::len).max().unwrap_or(0);
        assert!(
            max_len <= MAX_CARDINALITY_KEY_BYTES + "…trunc".len(),
            "composite key length {max_len} exceeds documented bound \
             MAX_CARDINALITY_KEY_BYTES={MAX_CARDINALITY_KEY_BYTES}"
        );
        assert_eq!(agg.len(), 1);
    }

    /// Wave 57 — closes Wave 56 NEW-W56 Low (tag-bypass of per-key cap):
    /// `aggregate_field_at` (the `by_row=false` multi-field path) appends
    /// a 10-byte `f<8hex>:` tag after `value_to_key` already clipped the
    /// value to `MAX_CARDINALITY_KEY_BYTES`.  Without a second cap pass
    /// the tagged key can exceed the documented per-key bound.  This test
    /// pins the post-fix invariant: every key in `seen` is bounded by
    /// `MAX_CARDINALITY_KEY_BYTES + "…trunc".len()`, matching the
    /// composite-key cap in `finish_row`.
    #[test]
    fn field_tagged_key_clips_to_max_total_bytes() {
        let mut agg =
            CardinalityAggregator::with_fields(false, vec!["f0".into(), "f1".into(), "f2".into()]);
        // Per-field value sized at exactly `MAX_CARDINALITY_KEY_BYTES`
        // (so `value_to_key`'s clip already kicks in) — without the W57
        // fix the appended tag would push the stored key past the cap.
        let oversized = "x".repeat(MAX_CARDINALITY_KEY_BYTES + 100);
        agg.aggregate_row_values(&[
            Some(&json!(oversized.clone())),
            Some(&json!(oversized.clone())),
            Some(&json!(oversized)),
        ]);
        let max_len = agg.iter_keys().map(String::len).max().unwrap_or(0);
        assert!(
            max_len <= MAX_CARDINALITY_KEY_BYTES + "…trunc".len(),
            "tagged key length {max_len} exceeds documented bound \
             MAX_CARDINALITY_KEY_BYTES={MAX_CARDINALITY_KEY_BYTES} \
             + '…trunc' marker; tag-bypass regression",
        );
    }

    /// `by_row = true` over multiple fields means: count distinct row
    /// tuples.  The same input as the union test produces 3 distinct
    /// tuples `(1,1), (2,1), (2,2)`.
    #[test]
    fn cardinality_multi_field_byrow_concatenation() {
        let mut agg = CardinalityAggregator::with_fields(true, vec!["a".into(), "b".into()]);
        agg.aggregate_row_values(&[Some(&json!(1)), Some(&json!(1))]);
        agg.aggregate_row_values(&[Some(&json!(2)), Some(&json!(1))]);
        agg.aggregate_row_values(&[Some(&json!(2)), Some(&json!(2))]);
        assert_eq!(
            agg.get(),
            json!(3),
            "by_row=true multi-field must count distinct row tuples"
        );
    }

    /// A `null` field value must be treated as a real value distinct from
    /// other values; in `by_row = true` mode it must hash differently from
    /// a row where the same field is missing entirely (caller passes
    /// `None` vs `Some(&Value::Null)`), but must hash **identically** to
    /// an explicit `null` since `value_to_key` collapses both onto the
    /// `__null__` sentinel for `by_row = false`.  The test below pins the
    /// `by_row = true` behaviour: rows that differ only in a `null`
    /// position must collapse to the same tuple.
    #[test]
    fn cardinality_multi_field_handles_null_values() {
        let mut agg = CardinalityAggregator::with_fields(true, vec!["a".into(), "b".into()]);
        // Row with explicit null on b
        agg.aggregate_row_values(&[Some(&json!("x")), Some(&json!(null))]);
        // Row with missing b (None)
        agg.aggregate_row_values(&[Some(&json!("x")), None]);
        // Both rows hash to the same tuple `(x, __null__)`.
        assert_eq!(
            agg.get(),
            json!(1),
            "explicit null and missing field must collapse to one tuple"
        );

        // A genuinely different b must produce a second tuple.
        agg.aggregate_row_values(&[Some(&json!("x")), Some(&json!("y"))]);
        assert_eq!(agg.get(), json!(2));
    }

    /// Length-prefix encoding must keep a value containing a literal
    /// field-separator NUL byte distinct from a different decomposition of
    /// the same byte stream — the existing single-field test
    /// [`cardinality_byrow_length_prefix_disambiguates_nul_in_value`]
    /// already covers `aggregate_field` + `finish_row`; this test pins the
    /// same property when the row is fed through the multi-field
    /// `aggregate_row_values` entry point.
    #[test]
    fn cardinality_multi_field_length_prefix_disambiguates_nul_in_value() {
        let mut agg = CardinalityAggregator::with_fields(true, vec!["a".into(), "b".into()]);
        agg.aggregate_row_values(&[Some(&json!("a\x00b")), Some(&json!("c"))]);
        agg.aggregate_row_values(&[Some(&json!("a")), Some(&json!("b\x00c"))]);
        assert_eq!(
            agg.get(),
            json!(2),
            "multi-field length-prefix encoding must keep NUL-bearing values distinct"
        );
    }

    /// Back-compat: a `with_fields(by_row, vec![])` aggregator (or the
    /// equivalent `new(by_row)`) must continue to behave as the legacy
    /// single-field aggregator when fed via `Aggregator::aggregate`.
    #[test]
    fn cardinality_back_compat_empty_fields_uses_single_value_path() {
        let mut new_style = CardinalityAggregator::with_fields(false, Vec::new());
        let mut legacy = CardinalityAggregator::new(false);
        for v in 0..5u64 {
            new_style.aggregate(Some(&json!(v)));
            legacy.aggregate(Some(&json!(v)));
        }
        assert_eq!(new_style.get(), legacy.get());
        assert_eq!(new_style.fields(), Vec::<String>::new().as_slice());
    }

    /// The trait-level `aggregate_multi` override must dispatch to the
    /// row-aware path: feeding a 2-tuple at once must produce the same
    /// cardinality as the explicit `aggregate_row_values` call.
    #[test]
    fn cardinality_multi_field_trait_aggregate_multi_matches_row_values() {
        let mut a = CardinalityAggregator::with_fields(true, vec!["x".into(), "y".into()]);
        let mut b = CardinalityAggregator::with_fields(true, vec!["x".into(), "y".into()]);

        let v1 = json!(1);
        let v2 = json!(2);

        // Trait dispatch
        Aggregator::aggregate_multi(&mut a, &[Some(&v1), Some(&v2)]);
        Aggregator::aggregate_multi(&mut a, &[Some(&v2), Some(&v1)]);

        // Direct row-aware call
        b.aggregate_row_values(&[Some(&v1), Some(&v2)]);
        b.aggregate_row_values(&[Some(&v2), Some(&v1)]);

        assert_eq!(a.get(), b.get());
        assert_eq!(a.get(), json!(2), "two distinct (x,y) tuples expected");
    }

    // -----------------------------------------------------------------------
    // Fail-closed exact-cardinality program (2026-07-11)
    // -----------------------------------------------------------------------

    /// A saturated aggregator must REPORT its saturation through the
    /// trait-level probe so finalization layers can fail closed instead of
    /// reading the silently capped `get()` value.
    #[test]
    fn saturation_reported_once_cap_hit() {
        let mut agg = CardinalityAggregator::with_cap_for_tests(false, Vec::new(), 3);
        for v in 0..3u64 {
            agg.aggregate(Some(&json!(v)));
        }
        assert!(
            agg.saturation().is_none(),
            "at-cap set is still exact (cap refused nothing yet)"
        );
        agg.aggregate(Some(&json!(99)));
        let sat = agg.saturation().expect("over-cap insert must saturate");
        assert_eq!(sat.limit, 3);
        assert_eq!(sat.observed, 3);
        assert!(
            sat.kind.contains("cardinality.maxExactSetSize"),
            "kind must name the limit, got: {}",
            sat.kind
        );
        assert!(
            sat.kind.contains("APPROX_COUNT_DISTINCT"),
            "kind must point at the approximate remedy, got: {}",
            sat.kind
        );
        // The capped `get()` value still exists (callers that go through
        // `saturation()` never read it) and stays at the cap.
        assert_eq!(agg.get(), json!(3));
    }

    /// The test-only cap constructor may only LOWER the production bound.
    #[test]
    fn test_cap_cannot_raise_production_bound() {
        let agg = CardinalityAggregator::with_cap_for_tests(
            false,
            Vec::new(),
            MAX_CARDINALITY_SET_SIZE * 10,
        );
        assert!(agg.saturation().is_none());
        // Drive one insert to prove the aggregator is usable; the clamp
        // itself is private state, pinned indirectly by the saturation
        // limit below.
        let mut agg = CardinalityAggregator::with_cap_for_tests(false, Vec::new(), 0);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(Some(&json!("b")));
        let sat = agg
            .saturation()
            .expect("cap clamps up to 1, so 2nd insert saturates");
        assert_eq!(sat.limit, 1, "cap of 0 must clamp to 1, never unbounded");
    }

    /// `FilteredAggregator` (the E16 exact COUNT(DISTINCT) wrapper) must
    /// propagate the inner aggregator's saturation report.
    #[test]
    fn filtered_wrapper_propagates_saturation() {
        let inner = CardinalityAggregator::with_cap_for_tests(false, Vec::new(), 2);
        let mut filtered = crate::FilteredAggregator::new(
            json!({"type": "not", "field": {"type": "selector", "dimension": "d", "value": null}}),
            Box::new(inner),
        );
        for v in 0..5u64 {
            filtered.aggregate(Some(&json!(v)));
        }
        let sat = filtered
            .saturation()
            .expect("wrapper must surface inner saturation");
        assert_eq!(sat.limit, 2);
    }

    /// Union with a NON-saturated empty side is the identity — a
    /// saturated other side stays saturated with its count as the carried
    /// bound (since the 2026-07-12 finding-6 fix the saturated flag is
    /// checked first, but adding an exact empty side never changes the
    /// result either way).
    #[test]
    fn state_union_with_empty_side_is_identity() {
        let saturated = CardinalityState::saturated_with_count(1234);
        let empty = CardinalityState {
            typ: CARDINALITY_STATE_TAG.to_owned(),
            values: Vec::new(),
            saturated: false,
            count: 0,
        };
        let merged = CardinalityState::union(&saturated, &empty);
        assert_eq!(merged, saturated, "∅ on the right must be identity");
        let merged = CardinalityState::union(&empty, &saturated);
        assert_eq!(merged, saturated, "∅ on the left must be identity");
    }

    /// `merge_cardinality` over two bare counts (the production per-shard
    /// wire shape) must emit a *saturated* envelope carrying the additive
    /// upper bound — never a bare (silently wrong) number — except when a
    /// zero side makes the merge exact.
    #[test]
    fn merge_cardinality_bare_counts_emit_saturated_envelope() {
        let spec: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "cardinality", "name": "uniq", "fields": ["d"]
        }))
        .expect("spec");
        let merged = crate::merge_json_by_spec(&spec, &json!(7), &json!(11));
        let state = CardinalityState::from_json(&merged)
            .expect("bare-count merge must produce a well-formed envelope")
            .expect("bare-count merge must produce a state envelope");
        assert!(
            state.saturated,
            "additive upper bound must be marked inexact"
        );
        assert_eq!(state.count, 18);

        // Zero on either side is exact and must pass the other through.
        assert_eq!(
            crate::merge_json_by_spec(&spec, &json!(0), &json!(11)),
            json!(11)
        );
        assert_eq!(
            crate::merge_json_by_spec(&spec, &json!(7), &json!(0)),
            json!(7)
        );
    }

    // -----------------------------------------------------------------------
    // Codex HIGH findings (2026-07-12): exactness + untrusted peer envelopes
    // -----------------------------------------------------------------------

    /// Finding 1: a value TAGGED as a `CardinalityState` envelope that fails
    /// to parse must poison the merge (fail closed), never be treated as a
    /// bare 0 and silently dropped (lost shard).
    #[test]
    fn merge_tagged_malformed_envelope_fails_closed_not_dropped() {
        let spec: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "cardinality", "name": "uniq", "fields": ["d"]
        }))
        .expect("spec");
        let mut agg = CardinalityAggregator::new(false);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(Some(&json!("b")));
        let valid = agg.into_state().to_json();
        // Tagged as a state envelope but structurally malformed (`values`
        // is not an array).
        let malformed = json!({
            "@type": "cardinality_state",
            "values": "not-an-array",
            "saturated": false,
            "count": 2
        });
        for (d, s) in [(&valid, &malformed), (&malformed, &valid)] {
            let merged = crate::merge_json_by_spec(&spec, d, s);
            assert_eq!(
                merged.get("saturated").and_then(serde_json::Value::as_bool),
                Some(true),
                "a tagged-but-malformed peer state must poison the merge \
                 (fail closed), not be silently dropped as a bare 0; got {merged}"
            );
        }
    }

    /// Finding 2: merging an opaque non-cardinality count cannot be
    /// de-duplicated against the existing distinct population, so the
    /// result is inexact and must report saturation (fail closed at
    /// finalize) — never a plausible exact count.
    #[test]
    fn merge_opaque_count_marks_result_inexact() {
        let mut card = CardinalityAggregator::new(false);
        card.aggregate(Some(&json!("a")));
        card.aggregate(Some(&json!("b")));
        card.aggregate(Some(&json!("c")));
        let spec: crate::AggregatorSpec =
            serde_json::from_value(json!({"type": "count", "name": "cnt"})).expect("spec");
        let mut other = spec.create();
        other.aggregate(Some(&json!(1)));
        other.aggregate(Some(&json!(1)));
        card.merge(other.as_ref());
        assert!(
            card.saturation().is_some(),
            "an opaque per-shard count may overlap the existing distinct \
             set — the merged result is inexact and must fail closed, got \
             an exact-looking {}",
            card.get()
        );
    }

    /// Finding 2 (identity edge): merging an opaque count of 0 contributes
    /// nothing and is exact — it must NOT force saturation.
    #[test]
    fn merge_opaque_zero_count_stays_exact() {
        let mut card = CardinalityAggregator::new(false);
        card.aggregate(Some(&json!("a")));
        let spec: crate::AggregatorSpec =
            serde_json::from_value(json!({"type": "count", "name": "cnt"})).expect("spec");
        let other = spec.create();
        card.merge(other.as_ref());
        assert!(card.saturation().is_none(), "zero contribution is exact");
        assert_eq!(card.get(), json!(1));
    }

    /// Finding 3: two DISTINCT values longer than the per-key byte cap that
    /// share a >4096-byte prefix must still count as 2 (or fail closed) —
    /// never silently collapse to 1 with `saturated = false`.  The memory
    /// bound must hold at the same time.
    #[test]
    fn long_distinct_values_do_not_collide_at_key_cap() {
        let mut agg = CardinalityAggregator::new(false);
        let prefix = "p".repeat(MAX_CARDINALITY_KEY_BYTES + 100);
        let a = format!("{prefix}-first");
        let b = format!("{prefix}-second");
        agg.aggregate(Some(&json!(a)));
        agg.aggregate(Some(&json!(b)));
        assert!(
            agg.saturation().is_none(),
            "two long values are far below the set cap — must stay exact"
        );
        assert_eq!(
            agg.get(),
            json!(2),
            "distinct >key-cap values must map to distinct keys"
        );
        let max_len = agg.iter_keys().map(String::len).max().unwrap_or(0);
        assert!(
            max_len <= MAX_CARDINALITY_KEY_BYTES + "…trunc".len(),
            "per-key memory bound must still hold, got {max_len}"
        );
    }

    /// Finding 3 (determinism): the clipped form of a long value must be
    /// deterministic so the same value observed on two shards unions to
    /// ONE key, not two.
    #[test]
    fn long_value_clip_is_deterministic_across_aggregators() {
        let long = "q".repeat(MAX_CARDINALITY_KEY_BYTES * 2);
        let mut a = CardinalityAggregator::new(false);
        a.aggregate(Some(&json!(long.clone())));
        let mut b = CardinalityAggregator::new(false);
        b.aggregate(Some(&json!(long)));
        let merged = CardinalityState::union(&a.into_state(), &b.into_state());
        assert!(!merged.saturated);
        assert_eq!(
            merged.count, 1,
            "same long value on two shards must union to one key"
        );
    }

    /// Findings 4+5: hydrating a state whose `count` does not match its
    /// actual distinct value set must mark the aggregator inexact (fail
    /// closed) — the peer-supplied count is never trusted over the set.
    #[test]
    fn from_state_forged_count_fails_closed() {
        let forged = CardinalityState {
            typ: CARDINALITY_STATE_TAG.to_owned(),
            values: vec!["s:x".to_owned()],
            saturated: false,
            count: 1_000_000,
        };
        let hydrated = CardinalityAggregator::from_state(&forged);
        assert!(
            hydrated.saturation().is_some(),
            "count/values mismatch must fail closed, never present an \
             exact-looking count"
        );
    }

    /// Finding 4 (merge path): a forged unsaturated envelope whose `count`
    /// disagrees with its `values` must poison the merge, not contribute
    /// a trusted count.
    #[test]
    fn merge_forged_count_envelope_fails_closed() {
        let spec: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "cardinality", "name": "uniq", "fields": ["d"]
        }))
        .expect("spec");
        let forged = json!({
            "@type": "cardinality_state",
            "values": ["s:x"],
            "saturated": false,
            "count": 1_000_000u64
        });
        // Merged with a bare exact count: the pre-fix state⊕count path
        // saturating-added the forged 1,000,000 into the upper bound.
        let merged = crate::merge_json_by_spec(&spec, &forged, &json!(7));
        assert_eq!(
            merged.get("saturated").and_then(serde_json::Value::as_bool),
            Some(true),
            "forged envelope must poison the merge (fail closed); got {merged}"
        );
        let carried = merged.get("count").and_then(serde_json::Value::as_u64);
        assert!(
            carried.is_some_and(|c| c < 1_000_000),
            "the forged count must NOT be trusted into the carried bound, \
             got {merged}"
        );
    }

    /// Finding 5: a peer envelope value beyond the per-key wire bound can
    /// never be a legit canonical key (the aggregator clips all keys) —
    /// accepting it would bypass the per-key memory bound, so the merge
    /// must fail closed.
    #[test]
    fn merge_envelope_with_oversized_value_fails_closed() {
        let spec: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "cardinality", "name": "uniq", "fields": ["d"]
        }))
        .expect("spec");
        let mut agg = CardinalityAggregator::new(false);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(Some(&json!("b")));
        let valid = agg.into_state().to_json();
        let oversized = json!({
            "@type": "cardinality_state",
            "values": [format!("s:{}", "x".repeat(3 * MAX_CARDINALITY_KEY_BYTES))],
            "saturated": false,
            "count": 1
        });
        let merged = crate::merge_json_by_spec(&spec, &valid, &oversized);
        assert_eq!(
            merged.get("saturated").and_then(serde_json::Value::as_bool),
            Some(true),
            "an over-bound peer key must fail closed, got {merged}"
        );
    }

    /// Finding 6: a saturated operand makes the union saturated regardless
    /// of its (possibly empty) value set.  A malformed/hostile peer
    /// `saturated=true, count=0, values=[]` must NOT be dropped as an
    /// empty identity.
    #[test]
    fn saturated_empty_peer_state_poisons_union() {
        let mut agg = CardinalityAggregator::new(false);
        for v in 0..3u64 {
            agg.aggregate(Some(&json!(v)));
        }
        let exact = agg.into_state();
        let hostile = CardinalityState {
            typ: CARDINALITY_STATE_TAG.to_owned(),
            values: Vec::new(),
            saturated: true,
            count: 0,
        };
        let merged = CardinalityState::union(&exact, &hostile);
        assert!(
            merged.saturated,
            "saturated ∪ anything must be saturated (right operand)"
        );
        let merged = CardinalityState::union(&hostile, &exact);
        assert!(
            merged.saturated,
            "saturated ∪ anything must be saturated (left operand)"
        );
    }

    /// Findings 1+4+5 unit matrix: `from_json` / `peek_json` distinguish
    /// "not an envelope" (`Ok(None)`), "well-formed envelope"
    /// (`Ok(Some)`) and "tagged but malformed / invariant-violating"
    /// (`Err` — the caller must fail closed).
    #[test]
    fn from_json_and_peek_json_validation_matrix() {
        // Not tagged → Ok(None) (legacy bare count / unrelated shapes).
        assert_eq!(CardinalityState::from_json(&json!(42)), Ok(None));
        assert_eq!(CardinalityState::from_json(&json!({"count": 3})), Ok(None));
        // Well-formed → Ok(Some).
        let good = json!({
            "@type": "cardinality_state",
            "values": ["s:a", "s:b"],
            "saturated": false,
            "count": 2
        });
        let state = CardinalityState::from_json(&good)
            .expect("well-formed")
            .expect("tagged");
        assert_eq!(state.count, 2);
        assert_eq!(CardinalityState::peek_json(&good), Ok(Some((false, 2))));
        // Tagged + malformed / invariant-violating → Err (fail closed).
        let bad = [
            // `values` not an array.
            json!({"@type": "cardinality_state", "values": "x", "saturated": false, "count": 1}),
            // `saturated` missing.
            json!({"@type": "cardinality_state", "values": [], "count": 0}),
            // `count` not a u64.
            json!({"@type": "cardinality_state", "values": [], "saturated": false, "count": -1}),
            // Forged count (≠ distinct values) — finding 4.
            json!({"@type": "cardinality_state", "values": ["s:x"], "saturated": false,
                   "count": 1_000_000u64}),
            // Duplicate values inflating the claimed count.
            json!({"@type": "cardinality_state", "values": ["s:x", "s:x"], "saturated": false,
                   "count": 2}),
            // Non-string value entry.
            json!({"@type": "cardinality_state", "values": [7], "saturated": false, "count": 1}),
            // Saturated state shipping values (contract: keys dropped).
            json!({"@type": "cardinality_state", "values": ["s:x"], "saturated": true,
                   "count": 1}),
            // Oversized value entry (per-key wire bound) — finding 5.
            json!({
                "@type": "cardinality_state",
                "values": [format!("s:{}", "x".repeat(2 * MAX_CARDINALITY_KEY_BYTES))],
                "saturated": false,
                "count": 1
            }),
        ];
        for v in &bad {
            assert!(
                CardinalityState::from_json(v).is_err(),
                "from_json must reject: {v}"
            );
            assert!(
                CardinalityState::peek_json(v).is_err(),
                "peek_json must reject: {v}"
            );
        }
        // A saturated state with no values and any count is an accepted
        // (inexact) bound — finalization fails closed on the flag.
        let sat =
            json!({"@type": "cardinality_state", "values": [], "saturated": true, "count": 999});
        assert_eq!(CardinalityState::peek_json(&sat), Ok(Some((true, 999))));
    }

    /// Finding 5: hydrating an over-cap value set must saturate (fail
    /// closed) and must not hold more than `cap` keys — driven via the
    /// internal cap parameter so the test does not materialize
    /// [`MAX_CARDINALITY_SET_SIZE`] keys.
    #[test]
    fn from_state_over_cap_hydration_saturates() {
        let state = CardinalityState {
            typ: CARDINALITY_STATE_TAG.to_owned(),
            values: (0..10).map(|i| format!("s:v{i}")).collect(),
            saturated: false,
            count: 10,
        };
        let hydrated = CardinalityAggregator::from_state_with_cap(&state, 4);
        assert!(
            hydrated.saturation().is_some(),
            "over-cap hydration must fail closed"
        );
        assert!(
            hydrated.len() <= 4,
            "hydration must not hold more than cap keys, got {}",
            hydrated.len()
        );
        // Below-cap hydration of the same (consistent) state stays exact.
        let hydrated = CardinalityAggregator::from_state_with_cap(&state, 16);
        assert!(hydrated.saturation().is_none());
        assert_eq!(hydrated.get(), json!(10));
    }

    /// Finding 5: the wire validation rejects a `values` array longer
    /// than the set cap BEFORE anything is cloned — driven via the
    /// internal cap parameter.
    #[test]
    fn validate_rejects_values_array_over_set_cap() {
        let obj = json!({
            "@type": "cardinality_state",
            "values": ["s:a", "s:b", "s:c"],
            "saturated": false,
            "count": 3
        });
        let map = obj.as_object().expect("object");
        assert!(
            validate_tagged_obj(map, 2).is_err(),
            "3 values over cap 2 must be rejected"
        );
        assert_eq!(validate_tagged_obj(map, 3), Ok((false, 3)));
    }

    #[test]
    fn cardinality_state_union_overlapping_shards_does_not_overcount() {
        // Shard A has 0..10, Shard B has 5..15 — union must be 0..15 = 15.
        let mut a = CardinalityAggregator::new(false);
        for v in 0..10u64 {
            a.aggregate(Some(&json!(v)));
        }
        let mut b = CardinalityAggregator::new(false);
        for v in 5..15u64 {
            b.aggregate(Some(&json!(v)));
        }
        let s_a = a.into_state();
        let s_b = b.into_state();
        let merged = CardinalityState::union(&s_a, &s_b);
        assert!(!merged.saturated);
        assert_eq!(
            merged.count, 15,
            "union must not overcount overlapping shard keys"
        );

        // Multi-shard exact union (2026-07-11): the same property must hold
        // ABOVE the former 1,000-key wire cap — two overlapping shards of
        // 800 keys each union to the exact 1,200 (previously: saturated
        // envelope, fail-closed).
        let mut a = CardinalityAggregator::new(false);
        for v in 0..800u64 {
            a.aggregate(Some(&json!(v)));
        }
        let mut b = CardinalityAggregator::new(false);
        for v in 400..1_200u64 {
            b.aggregate(Some(&json!(v)));
        }
        let merged = CardinalityState::union(&a.into_state(), &b.into_state());
        assert!(!merged.saturated, "1,200-key union is far below the 1M cap");
        assert_eq!(
            merged.count, 1_200,
            "overlapping >1,000-key shards must union exactly"
        );
    }
}
