// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Sketch aggregators that route Druid's approximate-aggregation wire types
//! through the real implementations in the `ferrodruid-sketches` crate.
//!
//! Three families are wired here, each reusing the sketch math from
//! `ferrodruid-sketches` (no internals are re-implemented):
//!
//! * [`HllSketchAggregator`] — Druid `HLLSketchBuild` / `HLLSketchMerge`
//!   (HyperLogLog cardinality), backed by [`ferrodruid_sketches::HllSketch`].
//! * [`ThetaSketchAggregator`] — Druid `thetaSketch` (set-operation
//!   cardinality), backed by [`ferrodruid_sketches::ThetaSketch`].
//! * [`QuantilesSketchAggregator`] — Druid `quantilesDoublesSketch`
//!   (approximate quantiles), backed by [`ferrodruid_sketches::TDigest`].
//!
//! ## Partial-state wire form
//!
//! For scatter/gather across the broker, each aggregator's [`Aggregator::get`]
//! emits a JSON object carrying a base64-encoded serialized sketch under a
//! `@sketch` tag, e.g.
//! `{"@sketch":"hll","bytes":"…base64…","estimate":1234.0}`.  The `estimate`
//! field is a convenience for consumers that only want the scalar; the
//! `bytes` field lets a merging broker reconstruct the exact sketch and union
//! it with other shards' sketches without precision loss.  A
//! [`merge_sketch_json`] helper performs that broker-side merge.
//!
//! The default sketch parameters follow the existing `ferrodruid-sketches`
//! defaults (HLL precision 14, Theta size 4096, T-digest compression 200);
//! Druid's `lgK` / `size` / `k` knobs are accepted on the wire and mapped onto
//! these structures on a best-effort basis (documented per aggregator).

use std::any::Any;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

use ferrodruid_sketches::{DruidHyperUnique, HllSketch, TDigest, ThetaSketch};

use crate::Aggregator;

/// Default T-digest compression for the quantiles sketch aggregator.
const DEFAULT_QUANTILES_COMPRESSION: usize = 200;

/// JSON tag used to mark an HLL sketch partial-state envelope.
pub const HLL_SKETCH_TAG: &str = "hll";
/// JSON tag used to mark a Theta sketch partial-state envelope.
pub const THETA_SKETCH_TAG: &str = "theta";
/// JSON tag used to mark a quantiles (T-digest) sketch partial-state envelope.
pub const QUANTILES_SKETCH_TAG: &str = "quantiles";
/// JSON tag used to mark a DECODED Druid `hyperUnique` partial-state
/// envelope (W-A, v1.5.0).  Distinct from [`HLL_SKETCH_TAG`] — the two
/// register states live in unrelated hash spaces and must never merge —
/// and from the `"HLLSketch"` DataSketches family, which stays undecodable.
pub const DRUID_HYPER_UNIQUE_TAG: &str = "druidHyperUnique";
/// JSON tag marking a LOUD sketch-mix error envelope: emitted instead of
/// an estimate when incompatible sketch feeds meet (e.g. a decoded Druid
/// hyperUnique and a native FNV HLL, or raw values into a merge-only
/// hyperUnique aggregation).  The envelope carries a human-readable
/// `message`, absorbs every further merge, and finalizes to NO scalar —
/// the query output visibly carries the error object, never a silently
/// wrong number.
pub const SKETCH_MIX_ERROR_TAG: &str = "mixError";

/// Encode a serialized sketch into a tagged partial-state JSON envelope.
fn sketch_envelope(tag: &str, bytes: &[u8], estimate: f64) -> serde_json::Value {
    serde_json::json!({
        "@sketch": tag,
        "bytes": BASE64.encode(bytes),
        "estimate": estimate,
    })
}

/// Build the theta partial-state envelope for an already-materialized
/// sketch — the same `{"@sketch":"theta","bytes":…,"estimate":…}` shape
/// [`ThetaSketchAggregator`]'s [`Aggregator::get`] emits.  Used by the
/// query layer to feed the per-row decoded sketches of a migrated Druid
/// `thetaSketch` column (compat-8 sketch #2) into the aggregator's merge
/// path, where they are unioned exactly.
#[must_use]
pub fn theta_sketch_envelope(sketch: &ThetaSketch) -> serde_json::Value {
    sketch_envelope(THETA_SKETCH_TAG, &sketch.serialize(), sketch.estimate())
}

/// Build the partial-state envelope for a decoded Druid `hyperUnique`
/// sketch — the shape [`HyperUniqueAggregator`]'s [`Aggregator::get`]
/// emits.  Used by the query layer to feed the per-row decoded sketches of
/// a migrated Druid `hyperUnique` column (W-A, v1.5.0) into the
/// aggregator's merge path, where they fold by register-wise max.  The
/// `estimate` convenience field carries the Druid-parity estimator value.
#[must_use]
pub fn druid_hyper_unique_envelope(sketch: &DruidHyperUnique) -> serde_json::Value {
    sketch_envelope(
        DRUID_HYPER_UNIQUE_TAG,
        &sketch.serialize(),
        sketch.estimate(),
    )
}

/// Build a LOUD mix-error envelope (see [`SKETCH_MIX_ERROR_TAG`]).
fn mix_error_envelope(message: &str) -> serde_json::Value {
    serde_json::json!({
        "@sketch": SKETCH_MIX_ERROR_TAG,
        "message": message,
    })
}

/// The LOUD error message for a correctly-TAGGED sketch envelope whose
/// payload FAILED TO DECODE (missing/invalid base64, or malformed sketch
/// bytes).  Such an envelope is CORRUPT partial state; dropping it
/// silently would omit an entire row's/shard's contribution — and do so
/// order-dependently (the reverse arrival order retains the corrupt
/// envelope instead) — so every aggregation and broker-merge path fails
/// LOUD with the absorbing mix-error envelope: no scalar, a visible
/// error object, identical in both arrival orders.
fn corrupt_envelope_message(tag: &str) -> String {
    format!(
        "corrupt {tag} sketch envelope: the tagged payload failed to decode \
         (invalid base64 or malformed sketch bytes); silently dropping it \
         would omit an entire row's/shard's contribution (an order-dependent \
         undercount), so the aggregation fails loud instead"
    )
}

/// Build the LOUD corrupt-envelope error envelope for `tag` (see
/// [`corrupt_envelope_message`]).
fn corrupt_envelope_error(tag: &str) -> serde_json::Value {
    mix_error_envelope(&corrupt_envelope_message(tag))
}

/// The `@sketch` tag of a partial-state envelope, if `value` is one.
fn envelope_tag(value: &serde_json::Value) -> Option<&str> {
    value.as_object()?.get("@sketch")?.as_str()
}

/// Extract the raw serialized sketch bytes from a tagged envelope, checking
/// the `@sketch` tag matches `expected_tag`.
fn envelope_bytes(value: &serde_json::Value, expected_tag: &str) -> Option<Vec<u8>> {
    let obj = value.as_object()?;
    let tag = obj.get("@sketch")?.as_str()?;
    if tag != expected_tag {
        return None;
    }
    let b64 = obj.get("bytes")?.as_str()?;
    BASE64.decode(b64).ok()
}

// ---------------------------------------------------------------------------
// Value hashing helper
// ---------------------------------------------------------------------------

/// Convert a JSON value into the byte representation a sketch should hash.
/// Numbers use their canonical decimal text so that `1` and `1.0` hash the
/// same; strings use their UTF-8 bytes; other shapes use their JSON text.
fn value_bytes(value: &serde_json::Value) -> Vec<u8> {
    match value {
        serde_json::Value::String(s) => s.clone().into_bytes(),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string().into_bytes()
            } else if let Some(u) = n.as_u64() {
                u.to_string().into_bytes()
            } else if let Some(f) = n.as_f64() {
                f.to_string().into_bytes()
            } else {
                n.to_string().into_bytes()
            }
        }
        serde_json::Value::Bool(b) => {
            if *b {
                b"true".to_vec()
            } else {
                b"false".to_vec()
            }
        }
        other => other.to_string().into_bytes(),
    }
}

/// Strongly-mixed 64-bit hash of a JSON value.
///
/// The `ferrodruid-sketches` `add(&[u8])` entry point uses an internal FNV-1a
/// hash whose high bits are poorly distributed for short, structurally-similar
/// inputs (e.g. small decimal strings).  Because HLL/Theta derive the bucket
/// index from the *top* bits of the hash, that weak avalanche skews estimates.
/// We instead compute a well-avalanched 64-bit hash here and feed it through
/// the sketches' `add_hash` entry point, so the registers see a near-uniform
/// hash regardless of input shape.  FNV-1a supplies the input-mixing; a
/// SplitMix64 finalizer supplies the avalanche.
fn mixed_hash(value: &serde_json::Value) -> u64 {
    let bytes = value_bytes(value);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in &bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    // SplitMix64 finalizer for strong bit avalanche.
    let mut z = h.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// ---------------------------------------------------------------------------
// Uniform sketch-cell classification (state-matrix hardening)
// ---------------------------------------------------------------------------
//
// EVERY merge/aggregate path — the broker-side [`merge_sketch_json`], every
// aggregator's `aggregate` feed, and every dyn-merge fallback — routes its
// inputs through ONE classifier ([`classify_sketch_cell`]) and applies ONE
// precedence, so the per-species arms can never diverge again:
//
//   1. `mixError` on either side  → propagate (absorbing).
//   2. Corrupt on either side (correctly TAGGED, payload FAILS TO DECODE,
//      ANY family) → LOUD corrupt poison, checked BEFORE any species or
//      family matching — `valid theta ⊕ corrupt hll` is loud in BOTH
//      orders, never clears the poison, never retains the raw object,
//      never hashes the envelope text as a raw value.
//   3. Null (JSON `null`) on a side → identity; both null → null.
//   4. Empty (decodes to an EMPTY sketch, ANY species) on a side →
//      identity (adopt the other side); both empty → the DETERMINISTIC
//      canonical empty.
//   5. Two NonEmpty SAME species → normal union.  A DECODABLE but
//      parameter-incompatible SAME-species union (theta hash-space/seed,
//      HLL precision) stays the documented fail-soft drop — the ONLY
//      surviving fail-soft case.
//   6. Two NonEmpty DIFFERENT species → LOUD `mixError`, in BOTH orders.

/// The sketch species (families) covered by the uniform merge rule, in the
/// FIXED priority order used for deterministic tie-breaks (the both-empty
/// canonical species and the both-corrupt error tag).  The order is
/// arbitrary but frozen: changing it changes merge outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SketchSpecies {
    /// Native FNV HLL ([`HLL_SKETCH_TAG`]).
    Hll,
    /// Theta ([`THETA_SKETCH_TAG`]).
    Theta,
    /// Quantiles T-digest ([`QUANTILES_SKETCH_TAG`]).
    Quantiles,
    /// Decoded Druid hyperUnique ([`DRUID_HYPER_UNIQUE_TAG`]).
    DruidHyperUnique,
}

impl SketchSpecies {
    /// The wire `@sketch` tag of this species.
    fn tag(self) -> &'static str {
        match self {
            Self::Hll => HLL_SKETCH_TAG,
            Self::Theta => THETA_SKETCH_TAG,
            Self::Quantiles => QUANTILES_SKETCH_TAG,
            Self::DruidHyperUnique => DRUID_HYPER_UNIQUE_TAG,
        }
    }

    /// Reverse of [`Self::tag`].
    fn from_tag(tag: &str) -> Option<Self> {
        match tag {
            HLL_SKETCH_TAG => Some(Self::Hll),
            THETA_SKETCH_TAG => Some(Self::Theta),
            QUANTILES_SKETCH_TAG => Some(Self::Quantiles),
            DRUID_HYPER_UNIQUE_TAG => Some(Self::DruidHyperUnique),
            _ => None,
        }
    }

    /// The DETERMINISTIC canonical EMPTY envelope of this species
    /// (default parameters), returned when BOTH merge sides are empty.
    /// Canonicalizing the parameters is safe because an empty partial is
    /// the merge IDENTITY on every path (rule 4): it is adopted-over or
    /// kept verbatim, never actually unioned, so its parameters can no
    /// longer influence any later merge.
    fn canonical_empty_envelope(self) -> serde_json::Value {
        match self {
            Self::Hll => {
                let s = HllSketch::default_precision();
                sketch_envelope(HLL_SKETCH_TAG, &s.serialize(), s.estimate())
            }
            Self::Theta => {
                let s = ThetaSketch::default_size();
                sketch_envelope(THETA_SKETCH_TAG, &s.serialize(), s.estimate())
            }
            Self::Quantiles => {
                let d = TDigest::new(DEFAULT_QUANTILES_COMPRESSION);
                let median = d.quantile(0.5).unwrap_or(f64::NAN);
                sketch_envelope(QUANTILES_SKETCH_TAG, &d.serialize(), median)
            }
            Self::DruidHyperUnique => druid_hyper_unique_envelope(&DruidHyperUnique::empty()),
        }
    }
}

/// A decoded sketch payload (the `NonEmpty` cell's cargo).
#[derive(Debug)]
enum DecodedSketch {
    /// Native FNV HLL.
    Hll(HllSketch),
    /// Theta.
    Theta(ThetaSketch),
    /// Quantiles T-digest.
    Quantiles(TDigest),
    /// Decoded Druid hyperUnique.
    DruidHyperUnique(DruidHyperUnique),
}

impl DecodedSketch {
    /// Which species this payload belongs to.
    fn species(&self) -> SketchSpecies {
        match self {
            Self::Hll(_) => SketchSpecies::Hll,
            Self::Theta(_) => SketchSpecies::Theta,
            Self::Quantiles(_) => SketchSpecies::Quantiles,
            Self::DruidHyperUnique(_) => SketchSpecies::DruidHyperUnique,
        }
    }

    /// Whether the decoded sketch carries NO values (the merge identity).
    fn is_empty(&self) -> bool {
        match self {
            Self::Hll(s) => s.estimate() == 0.0,
            Self::Theta(s) => s.retained() == 0,
            Self::Quantiles(d) => d.count() == 0,
            Self::DruidHyperUnique(s) => s.is_empty(),
        }
    }
}

/// The classification of ONE side of a sketch merge — the shared state
/// space every merge/aggregate path routes through (see the module-level
/// precedence above).
#[derive(Debug)]
enum SketchCell {
    /// JSON `null` — the empty-bucket identity (rule 3).
    Null,
    /// An absorbing LOUD error envelope, carrying its message (rule 1).
    MixError(String),
    /// A correctly-TAGGED envelope whose payload FAILED TO DECODE
    /// (missing/invalid base64, or malformed sketch bytes) — rule 2.
    Corrupt(SketchSpecies),
    /// A valid envelope that decodes to an EMPTY sketch — the merge
    /// identity in every state (rule 4).
    Empty(SketchSpecies),
    /// A valid envelope carrying a NON-empty decoded sketch (rules 5/6).
    NonEmpty(DecodedSketch),
    /// Outside the sketch-cell state space: a raw value, or an envelope
    /// of a family the uniform rule does not govern (e.g. bloom).  Each
    /// path keeps its own documented handling for these (raw hashing in
    /// build mode, legacy fail-soft passthrough at the broker, the
    /// no-raw-add poison on the merge-only hyperUnique aggregator).
    Foreign,
}

/// Classify one side of a sketch merge into its [`SketchCell`].
fn classify_sketch_cell(value: &serde_json::Value) -> SketchCell {
    if value.is_null() {
        return SketchCell::Null;
    }
    let Some(tag) = envelope_tag(value) else {
        return SketchCell::Foreign;
    };
    if tag == SKETCH_MIX_ERROR_TAG {
        let message = value
            .get("message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("upstream sketch mix error")
            .to_string();
        return SketchCell::MixError(message);
    }
    let Some(species) = SketchSpecies::from_tag(tag) else {
        return SketchCell::Foreign;
    };
    let Some(bytes) = envelope_bytes(value, species.tag()) else {
        return SketchCell::Corrupt(species);
    };
    let decoded = match species {
        SketchSpecies::Hll => HllSketch::deserialize(&bytes).ok().map(DecodedSketch::Hll),
        SketchSpecies::Theta => ThetaSketch::deserialize(&bytes)
            .ok()
            .map(DecodedSketch::Theta),
        SketchSpecies::Quantiles => TDigest::deserialize(&bytes)
            .ok()
            .map(DecodedSketch::Quantiles),
        SketchSpecies::DruidHyperUnique => DruidHyperUnique::deserialize(&bytes)
            .ok()
            .map(DecodedSketch::DruidHyperUnique),
    };
    match decoded {
        None => SketchCell::Corrupt(species),
        Some(d) if d.is_empty() => SketchCell::Empty(species),
        Some(d) => SketchCell::NonEmpty(d),
    }
}

/// The LOUD deterministic cross-species error message (rule 6).  The two
/// species are SORTED so BOTH arrival orders — and every path — produce
/// the identical envelope.
fn cross_species_message(a: SketchSpecies, b: SketchSpecies) -> String {
    let (x, y) = if a <= b { (a, b) } else { (b, a) };
    format!(
        "cannot merge non-empty {x} and {y} sketch partials: the families \
         live in unrelated hash/value spaces, so combining them would \
         corrupt the result (strict no-mix, fail-loud — never a silently \
         wrong estimate)",
        x = x.tag(),
        y = y.tag(),
    )
}

// ---------------------------------------------------------------------------
// HLL sketch aggregator
// ---------------------------------------------------------------------------

/// Aggregator backing Druid's `HLLSketchBuild` and `HLLSketchMerge`.
///
/// In *build* mode each input value is hashed and added to the HLL registers.
/// In *merge* mode each input is expected to be a serialized HLL sketch (the
/// `bytes`/base64 partial-state form emitted by [`Aggregator::get`]) which is
/// deserialized and merged register-wise.  Both modes share the same
/// register-union merge for scatter/gather, so the distinction only affects
/// how raw rows are consumed.  An input TAGGED as an `hll` envelope is
/// treated as partial state in either mode; a tagged-but-UNDECODABLE
/// envelope is CORRUPT and poisons the accumulator loudly (see
/// [`corrupt_envelope_message`] — never a silent order-dependent drop,
/// never a phantom raw-value hash).
///
/// # Decoded-Druid `hyperUnique` adoption (W-A, v1.5.0)
///
/// SQL `APPROX_COUNT_DISTINCT` lowers to this aggregator, so a migrated
/// `COMPLEX<hyperUnique>` column feeds it [`DRUID_HYPER_UNIQUE_TAG`]
/// envelopes.  Mirroring the theta empty-neutral rules: an EMPTY native
/// accumulator ADOPTS the first decoded-hyperUnique feed and becomes a
/// register-max merger of decoded sketches; a NON-empty native accumulator
/// (or an adopted one receiving native/raw input) has genuinely mixed hash
/// spaces and POISONS itself loudly — [`Aggregator::get`] then emits a
/// [`SKETCH_MIX_ERROR_TAG`] envelope that finalizes to NO scalar, so the
/// query output carries a visible error object, never a silently wrong
/// estimate.
///
/// The no-mix rule keys on NON-EMPTINESS, not on species alone (R6): an
/// EMPTY valid partial of EITHER species carries no distinct values, so
/// it is the merge IDENTITY in every accumulator state — it never
/// poisons, never overrides, and never flips the accumulator's mode,
/// regardless of arrival order.  (Pre-R6 the adopted-Druid guard ran
/// before the native-envelope decode, so `dhu ⊕ empty-hll` poisoned
/// while the reverse order adopted cleanly — an order-dependent poison;
/// and an empty decoded feed was adopted, poisoning later native input.)
#[derive(Debug, Clone)]
pub struct HllSketchAggregator {
    sketch: HllSketch,
    merge_mode: bool,
    /// The adopted decoded-Druid state, once a [`DRUID_HYPER_UNIQUE_TAG`]
    /// feed reached an empty native accumulator (see the struct docs).
    ///
    /// INVARIANT: when `Some`, the sketch is NON-empty — an empty decoded
    /// feed is the merge identity and is never adopted (adopting it would
    /// flip the accumulator into decoded mode and poison later native
    /// input, the reverse-order half of the R6 order-dependent poison).
    adopted: Option<DruidHyperUnique>,
    /// Loud no-mix poison: once set, every input is absorbed and
    /// [`Aggregator::get`] emits the mix-error envelope.
    mix_error: Option<String>,
}

impl HllSketchAggregator {
    /// Create a build-mode HLL aggregator with the given `lg_k` precision.
    ///
    /// `lg_k` is clamped to the `4..=18` range supported by
    /// [`ferrodruid_sketches::HllSketch`]; an out-of-range value falls back to
    /// the default precision (14).
    #[must_use]
    pub fn build(lg_k: u8) -> Self {
        let sketch = HllSketch::new(lg_k).unwrap_or_else(|_| HllSketch::default_precision());
        Self {
            sketch,
            merge_mode: false,
            adopted: None,
            mix_error: None,
        }
    }

    /// Create a merge-mode HLL aggregator with the given `lg_k` precision.
    #[must_use]
    pub fn merge(lg_k: u8) -> Self {
        let mut s = Self::build(lg_k);
        s.merge_mode = true;
        s
    }

    /// Current cardinality estimate: the native FNV-HLL estimate, or the
    /// Druid-parity estimate after a decoded-hyperUnique adoption.  A
    /// poisoned (mixed-feed) accumulator returns NaN — deliberately not a
    /// fabricated number ([`Aggregator::get`] carries the loud error).
    #[must_use]
    pub fn estimate(&self) -> f64 {
        if self.mix_error.is_some() {
            return f64::NAN;
        }
        match &self.adopted {
            Some(adopted) => adopted.estimate(),
            None => self.sketch.estimate(),
        }
    }

    /// Access the underlying native sketch (for typed merge).  Stays the
    /// native FNV sketch even after a decoded-hyperUnique adoption (which
    /// only happens on an EMPTY native sketch).
    #[must_use]
    pub fn sketch(&self) -> &HllSketch {
        &self.sketch
    }

    /// Whether the native FNV accumulator is still EMPTY (nothing hashed
    /// or merged into it) — the state in which a decoded-hyperUnique feed
    /// may be adopted.  An empty HLL estimates exactly 0.0 and any
    /// occupied register makes the estimate positive.
    fn native_is_empty(&self) -> bool {
        self.sketch.estimate() == 0.0
    }

    /// Poison this accumulator loudly (see the struct docs).
    fn poison(&mut self, message: String) {
        if self.mix_error.is_none() {
            self.mix_error = Some(message);
        }
    }

    /// Route ONE classified sketch cell (see [`classify_sketch_cell`])
    /// into this accumulator — shared by the `aggregate` feed and the
    /// dyn-merge fallback so the state matrix can never diverge between
    /// paths.  [`SketchCell::Foreign`] (raw input) is handled by the
    /// CALLERS: the build-mode row feed hashes it; every other path keeps
    /// its legacy behavior.
    fn consume_envelope_cell(&mut self, cell: SketchCell) {
        match cell {
            SketchCell::Null | SketchCell::Foreign => {}
            // Rule 1: an upstream loud error propagates (absorbs this
            // accumulator).
            SketchCell::MixError(message) => self.poison(message),
            // Rule 2: a tagged-but-undecodable envelope of ANY family —
            // even one this accumulator would refuse when valid — poisons
            // LOUDLY, before any species matching.  Never a silent
            // order-dependent drop, never a phantom raw-value hash.
            SketchCell::Corrupt(species) => self.poison(corrupt_envelope_message(species.tag())),
            // Rule 4 (R6): an EMPTY valid partial of ANY species carries
            // no distinct values — the merge IDENTITY in every
            // accumulator state.  It never poisons, never overrides, and
            // never flips the accumulator's mode.
            SketchCell::Empty(_) => {}
            SketchCell::NonEmpty(DecodedSketch::Hll(other)) => {
                if self.adopted.is_some() {
                    // Rule 6 / W-A strict no-mix: native registers cannot
                    // fold into an adopted decoded-Druid state.
                    self.poison(cross_species_message(
                        SketchSpecies::Hll,
                        SketchSpecies::DruidHyperUnique,
                    ));
                } else if self.native_is_empty() {
                    // Rule 4: the empty side is the identity — ADOPT the
                    // partial wholesale, so a same-species merge into an
                    // empty accumulator can never be lost to a precision
                    // mismatch.
                    self.sketch = other;
                } else {
                    // Rule 5: same-species union.  A precision-incompatible
                    // merge stays the documented fail-soft drop (the ONLY
                    // surviving fail-soft case).
                    let _ = self.sketch.merge(&other);
                }
            }
            SketchCell::NonEmpty(DecodedSketch::DruidHyperUnique(other)) => {
                // Field-disjoint emptiness check (the `native_is_empty`
                // helper would borrow all of `self` across the
                // `adopted.as_mut()` borrow).
                match self.adopted.as_mut() {
                    Some(adopted) => adopted.merge_in_place(&other),
                    None if self.sketch.estimate() == 0.0 => self.adopted = Some(other),
                    // Rule 6 / W-A strict no-mix — the SAME sorted message
                    // as the reverse pairing above, so both arrival orders
                    // produce the identical loud envelope.
                    None => self.poison(cross_species_message(
                        SketchSpecies::Hll,
                        SketchSpecies::DruidHyperUnique,
                    )),
                }
            }
            // Rule 6: any other non-empty species (theta/quantiles) is a
            // LOUD cross-species mix — keyed on this aggregator's DECLARED
            // species (not its adoption state) so both arrival orders
            // produce the identical poison.  Pre-fix the build path hashed
            // such an envelope's JSON text as a phantom distinct value.
            SketchCell::NonEmpty(other) => {
                self.poison(cross_species_message(SketchSpecies::Hll, other.species()));
            }
        }
    }
}

impl Aggregator for HllSketchAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let Some(v) = value else { return };
        // A poisoned accumulator absorbs everything (the error is final).
        if self.mix_error.is_some() {
            return;
        }
        // W-B legacy null mode: a JSON-null feed IS the merged ''/null
        // value — a REAL distinct value, hashed as `""` (oracle
        // count_distinct_strcol.json: legacy approximate COUNT DISTINCT
        // counts it; ANSI skips it).  Rewriting BEFORE classification
        // routes it through the uniform Foreign (raw-input) path, so the
        // adopted-state no-mix poison and the merge-mode skip apply to it
        // exactly like any other raw value.
        let legacy_empty_string;
        let v = if v.is_null() && ferrodruid_common::legacy_null_mode() {
            legacy_empty_string = serde_json::Value::String(String::new());
            &legacy_empty_string
        } else {
            v
        };
        let cell = classify_sketch_cell(v);
        if matches!(cell, SketchCell::Foreign) {
            // RAW (or out-of-scope) input.  After adoption the accumulator
            // is a decoded-Druid register state: raw input can never mix
            // into it (strict no-mix, loud).  Tagged partials never reach
            // this guard — they were classified above, so an empty-valid
            // partial cannot poison order-dependently.
            if self.adopted.is_some() {
                self.poison(
                    "cannot feed native/raw input into an HLL accumulator that adopted \
                     a decoded Druid hyperUnique sketch: the register states live in \
                     unrelated hash spaces (strict no-mix)"
                        .to_string(),
                );
                return;
            }
            if self.merge_mode {
                // Merge mode consumes only partial-state envelopes; any
                // other input shape is skipped (unchanged).
                return;
            }
            self.sketch.add_hash(mixed_hash(v));
            return;
        }
        self.consume_envelope_cell(cell);
    }

    fn get(&self) -> serde_json::Value {
        if let Some(message) = &self.mix_error {
            return mix_error_envelope(message);
        }
        if let Some(adopted) = &self.adopted {
            return druid_hyper_unique_envelope(adopted);
        }
        sketch_envelope(
            HLL_SKETCH_TAG,
            &self.sketch.serialize(),
            self.sketch.estimate(),
        )
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        if self.mix_error.is_some() {
            return;
        }
        if let Some(any) = other.as_any()
            && let Some(o) = any.downcast_ref::<HllSketchAggregator>()
        {
            if let Some(message) = &o.mix_error {
                self.poison(message.clone());
                return;
            }
            match (&mut self.adopted, &o.adopted) {
                (Some(adopted), Some(other_adopted)) => {
                    adopted.merge_in_place(other_adopted);
                }
                (Some(_), None) => {
                    // Peer is native: empty = identity (rule 4), non-empty
                    // = the strict no-mix loud (rule 6, sorted message).
                    if !o.native_is_empty() {
                        self.poison(cross_species_message(
                            SketchSpecies::Hll,
                            SketchSpecies::DruidHyperUnique,
                        ));
                    }
                }
                (None, Some(other_adopted)) => {
                    if self.native_is_empty() {
                        self.adopted = Some(other_adopted.clone());
                    } else {
                        self.poison(cross_species_message(
                            SketchSpecies::Hll,
                            SketchSpecies::DruidHyperUnique,
                        ));
                    }
                }
                (None, None) => {
                    if self.native_is_empty() {
                        // Rule 4: adopt wholesale — never lose a peer to a
                        // precision mismatch against an EMPTY accumulator.
                        self.sketch = o.sketch.clone();
                    } else {
                        // Rule 5: documented same-species precision
                        // fail-soft.
                        let _ = self.sketch.merge(&o.sketch);
                    }
                }
            }
            return;
        }
        // Dyn-merge fallback: classify the peer's JSON partial through the
        // SAME cell rules as the row feed (empty = identity even against
        // an adopted accumulator, non-empty cross = loud no-mix, corrupt =
        // loud corrupt — never a silent drop).
        let other_value = other.get();
        let cell = classify_sketch_cell(&other_value);
        if matches!(cell, SketchCell::Foreign) {
            // Legacy: a non-sketch peer is ignored — EXCEPT into an
            // adopted accumulator, where any native state is a strict
            // no-mix.
            if self.adopted.is_some() {
                self.poison(
                    "cannot merge native HLL state into an accumulator that \
                     adopted a decoded Druid hyperUnique sketch (strict no-mix)"
                        .to_string(),
                );
            }
            return;
        }
        self.consume_envelope_cell(cell);
    }

    fn reset(&mut self) {
        let precision = self.sketch.precision();
        self.sketch = HllSketch::new(precision).unwrap_or_else(|_| HllSketch::default_precision());
        self.adopted = None;
        self.mix_error = None;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }
}

// ---------------------------------------------------------------------------
// Theta sketch aggregator
// ---------------------------------------------------------------------------

/// Aggregator backing Druid's `thetaSketch` (build mode).
///
/// Each input value is hashed and added.  The partial state is a serialized
/// Theta sketch so the broker can union shards exactly (the union retains the
/// minimum theta and the combined hash set).
///
/// A theta-TAGGED envelope whose payload fails to decode is CORRUPT
/// partial state and POISONS the accumulator loudly ([`Aggregator::get`]
/// then emits a [`SKETCH_MIX_ERROR_TAG`] envelope that finalizes to NO
/// scalar) — see [`corrupt_envelope_message`] for why the former
/// fail-soft drop ("undercount never over-count") was wrong: the drop
/// was ORDER-DEPENDENT (`valid ⊕ corrupt` kept the valid side's lone
/// estimate while the reverse order retained the corrupt envelope), a
/// silent undercount either way.  A DECODABLE but hash-space/seed
/// incompatible sketch is still dropped fail-soft by the documented
/// union guard (that case carries no corruption signal, only a refusal
/// to mix).
#[derive(Debug, Clone)]
pub struct ThetaSketchAggregator {
    sketch: ThetaSketch,
    /// The configured Druid `size` (nominal retained-hash budget), kept so
    /// [`Aggregator::reset`] rebuilds a parameter-faithful sketch — the
    /// HLL aggregator preserves its precision and the quantiles aggregator
    /// its compression across `reset` the same way.  (Pre-fix, `reset`
    /// fell back to the 4096 default, silently coarsening any executor
    /// that reuses the aggregator across groups/buckets.)
    size: usize,
    /// Loud corrupt-envelope poison: once set, every input is absorbed
    /// and [`Aggregator::get`] emits the mix-error envelope (same
    /// absorbing pattern as [`HllSketchAggregator`]).
    mix_error: Option<String>,
}

impl ThetaSketchAggregator {
    /// Create a Theta sketch aggregator retaining up to `size` hashes.
    #[must_use]
    pub fn new(size: usize) -> Self {
        let size = if size == 0 { 4096 } else { size };
        Self {
            sketch: ThetaSketch::new(size),
            size,
            mix_error: None,
        }
    }

    /// Current cardinality estimate.  A poisoned (corrupt-envelope)
    /// accumulator returns NaN — deliberately not a fabricated number
    /// ([`Aggregator::get`] carries the loud error).
    #[must_use]
    pub fn estimate(&self) -> f64 {
        if self.mix_error.is_some() {
            return f64::NAN;
        }
        self.sketch.estimate()
    }

    /// Access the underlying sketch (for typed merge / set ops).
    #[must_use]
    pub fn sketch(&self) -> &ThetaSketch {
        &self.sketch
    }

    /// Poison this accumulator loudly (see the struct docs).
    fn poison(&mut self, message: String) {
        if self.mix_error.is_none() {
            self.mix_error = Some(message);
        }
    }

    /// Route ONE classified sketch cell into this accumulator — shared by
    /// the `aggregate` feed and the dyn-merge fallback (see
    /// [`classify_sketch_cell`] and the module-level precedence).
    /// [`SketchCell::Foreign`] (raw input) is handled by the callers.
    fn consume_envelope_cell(&mut self, cell: SketchCell) {
        match cell {
            SketchCell::Null | SketchCell::Foreign => {}
            // Rule 1: an upstream loud error propagates.
            SketchCell::MixError(message) => self.poison(message),
            // Rule 2: a tagged-but-undecodable envelope of ANY family
            // poisons LOUDLY before species matching — never a silent
            // drop, never a phantom raw-value hash of the envelope text.
            SketchCell::Corrupt(species) => self.poison(corrupt_envelope_message(species.tag())),
            // Rule 4: an EMPTY valid partial of ANY species is the merge
            // identity.
            SketchCell::Empty(_) => {}
            // Rule 5: same-species union.  A DECODABLE but incompatible
            // sketch (native-FNV × Druid-MurmurHash3 hash spaces, or
            // cross-seed Druid origins) is dropped fail-soft by the
            // documented union guard — the ONLY surviving fail-soft case.
            SketchCell::NonEmpty(DecodedSketch::Theta(other)) => {
                if let Ok(u) = self.sketch.union(&other) {
                    self.sketch = u;
                }
            }
            // Rule 6: a non-empty envelope of a DIFFERENT species is a
            // LOUD cross-species mix (pre-fix it fell through to
            // `add_hash` — a phantom distinct value).
            SketchCell::NonEmpty(other) => {
                self.poison(cross_species_message(SketchSpecies::Theta, other.species()));
            }
        }
    }
}

impl Aggregator for ThetaSketchAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let Some(v) = value else { return };
        // A poisoned accumulator absorbs everything (the error is final).
        if self.mix_error.is_some() {
            return;
        }
        let cell = classify_sketch_cell(v);
        if matches!(cell, SketchCell::Foreign) {
            // RAW value: hash and add.  A raw add into a Druid-origin
            // union is refused by the sketch itself (documented fail-soft
            // union-only limitation of migrated theta columns).
            let _ = self.sketch.add_hash(mixed_hash(v));
            return;
        }
        self.consume_envelope_cell(cell);
    }

    fn get(&self) -> serde_json::Value {
        if let Some(message) = &self.mix_error {
            return mix_error_envelope(message);
        }
        sketch_envelope(
            THETA_SKETCH_TAG,
            &self.sketch.serialize(),
            self.sketch.estimate(),
        )
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        if self.mix_error.is_some() {
            return;
        }
        // Cross-hash-space unions are refused by the sketch (see
        // `aggregate`); the DECODABLE-but-incompatible side is dropped
        // fail-soft, the same way an HLL precision-mismatch merge is.  A
        // poisoned peer or a tagged-but-undecodable peer envelope poisons
        // this side loudly.
        if let Some(any) = other.as_any()
            && let Some(o) = any.downcast_ref::<ThetaSketchAggregator>()
        {
            if let Some(message) = &o.mix_error {
                self.poison(message.clone());
                return;
            }
            if let Ok(u) = self.sketch.union(&o.sketch) {
                self.sketch = u;
            }
            return;
        }
        // Dyn-merge fallback: classify the peer's JSON partial through
        // the SAME cell rules as the row feed.  A non-sketch (Foreign)
        // peer is ignored (legacy — never raw-hashed on a merge path).
        let other_value = other.get();
        let cell = classify_sketch_cell(&other_value);
        if !matches!(cell, SketchCell::Foreign) {
            self.consume_envelope_cell(cell);
        }
    }

    fn reset(&mut self) {
        // Parameter-faithful reset: rebuild with the CONFIGURED size, not
        // the 4096 default (see the `size` field docs).
        self.sketch = ThetaSketch::new(self.size);
        self.mix_error = None;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }
}

// ---------------------------------------------------------------------------
// Decoded Druid hyperUnique aggregator (W-A, v1.5.0)
// ---------------------------------------------------------------------------

/// Aggregator backing Druid's native `hyperUnique` aggregation over a
/// MIGRATED `COMPLEX<hyperUnique>` column (W-A, v1.5.0).
///
/// Strictly MERGE-mode: each input must be a [`DRUID_HYPER_UNIQUE_TAG`]
/// partial-state envelope (the per-row form `column_value_at` renders for
/// a decoded column, and the form peer partials emit); sketches fold by
/// register-wise max, exactly like Druid's own fold.  There is NO raw-add
/// mode — the underlying [`DruidHyperUnique`] type has no `add` at all
/// (Druid's raw-value hash function is unknown to this clean-room
/// implementation), so a `hyperUnique` aggregation pointed at a
/// non-hyperUnique column POISONS itself loudly: [`Aggregator::get`] emits
/// a [`SKETCH_MIX_ERROR_TAG`] envelope that finalizes to NO scalar, never
/// a silently wrong estimate.
///
/// Even this merge-only aggregator classifies its inputs through the
/// UNIFORM state-matrix rule ([`classify_sketch_cell`]) rather than
/// blanket-poisoning every foreign envelope: an EMPTY valid envelope of
/// ANY species is the merge identity (it carries no values, so honoring
/// it cannot violate the no-raw-add invariant), while corrupt envelopes
/// (any family), NON-empty cross-species envelopes, and raw values all
/// stay LOUD.
#[derive(Debug, Clone, Default)]
pub struct HyperUniqueAggregator {
    sketch: DruidHyperUnique,
    /// Loud no-mix poison (see the struct docs).
    mix_error: Option<String>,
}

impl HyperUniqueAggregator {
    /// Create an empty decoded-hyperUnique merger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current Druid-parity cardinality estimate (NaN when poisoned — the
    /// loud error lives in [`Aggregator::get`], never a fabricated number).
    #[must_use]
    pub fn estimate(&self) -> f64 {
        if self.mix_error.is_some() {
            return f64::NAN;
        }
        self.sketch.estimate()
    }

    /// Access the underlying decoded sketch (for typed merge / tests).
    #[must_use]
    pub fn sketch(&self) -> &DruidHyperUnique {
        &self.sketch
    }

    fn poison(&mut self, message: String) {
        if self.mix_error.is_none() {
            self.mix_error = Some(message);
        }
    }

    /// Route ONE classified sketch cell into this merger (see
    /// [`classify_sketch_cell`] and the module-level precedence).  The
    /// uniform rule is applied in full rather than the former blanket
    /// "any foreign envelope poisons": an EMPTY valid envelope of ANY
    /// species is the identity — it carries no values, so honoring it
    /// cannot violate the documented no-raw-add invariant — while corrupt
    /// (any family) and non-empty-cross envelopes stay LOUD, and raw
    /// values keep the merge-only poison (there is no raw-add path).
    fn consume_envelope_cell(&mut self, cell: SketchCell) {
        match cell {
            SketchCell::Null => {}
            // Rule 1: an upstream loud error propagates.
            SketchCell::MixError(message) => self.poison(message),
            // Rule 2: tagged-but-undecodable, ANY family → loud corrupt
            // (pre-fix a corrupt hyperUnique envelope was dropped
            // fail-soft — an order-dependent silent undercount — and a
            // corrupt FOREIGN envelope got the generic raw-input message).
            SketchCell::Corrupt(species) => self.poison(corrupt_envelope_message(species.tag())),
            // Rule 4: empty valid partial of ANY species = identity.
            SketchCell::Empty(_) => {}
            // Rule 5: same-species register-wise max fold (Druid's fold).
            SketchCell::NonEmpty(DecodedSketch::DruidHyperUnique(other)) => {
                self.sketch.merge_in_place(&other);
            }
            // Rule 6: a non-empty envelope of a DIFFERENT species is a
            // LOUD cross-species mix (sorted message, order-independent).
            SketchCell::NonEmpty(other) => {
                self.poison(cross_species_message(
                    SketchSpecies::DruidHyperUnique,
                    other.species(),
                ));
            }
            // Raw values can never fold into a decoded-Druid register
            // state: poison loudly (strict no-mix; a merge-only
            // aggregation has no raw-add path).
            SketchCell::Foreign => self.poison(
                "hyperUnique aggregation received non-hyperUnique input (a raw \
                 value or an incompatible sketch): a migrated hyperUnique column \
                 is merge-only, and FerroDruid cannot hash raw values into \
                 Druid's register space (strict no-mix, fail-loud)"
                    .to_string(),
            ),
        }
    }
}

impl Aggregator for HyperUniqueAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let Some(v) = value else { return };
        if self.mix_error.is_some() {
            return;
        }
        self.consume_envelope_cell(classify_sketch_cell(v));
    }

    fn get(&self) -> serde_json::Value {
        if let Some(message) = &self.mix_error {
            return mix_error_envelope(message);
        }
        druid_hyper_unique_envelope(&self.sketch)
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        if self.mix_error.is_some() {
            return;
        }
        if let Some(any) = other.as_any()
            && let Some(o) = any.downcast_ref::<HyperUniqueAggregator>()
        {
            match &o.mix_error {
                Some(message) => self.poison(message.clone()),
                None => self.sketch.merge_in_place(&o.sketch),
            }
            return;
        }
        // JSON fallback: route the peer's envelope through the same rules
        // as the row feed.
        let other_value = other.get();
        self.aggregate(Some(&other_value));
    }

    fn reset(&mut self) {
        self.sketch = DruidHyperUnique::empty();
        self.mix_error = None;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }
}

// ---------------------------------------------------------------------------
// Quantiles (T-digest) sketch aggregator
// ---------------------------------------------------------------------------

/// Aggregator backing Druid's `quantilesDoublesSketch` (build + merge).
///
/// Each numeric input is added to a [`ferrodruid_sketches::TDigest`].  The
/// partial state serializes the digest so shards can be merged exactly via
/// T-digest centroid merging.  Druid's `k` parameter maps onto the digest's
/// compression budget.
///
/// A quantiles-TAGGED envelope whose payload fails to decode is CORRUPT
/// partial state and POISONS the accumulator loudly, the same uniform
/// rule every tagged sketch envelope follows (see
/// [`corrupt_envelope_message`]): [`Aggregator::get`] then emits a
/// [`SKETCH_MIX_ERROR_TAG`] envelope that finalizes to NO scalar.
#[derive(Debug, Clone)]
pub struct QuantilesSketchAggregator {
    digest: TDigest,
    compression: usize,
    /// Loud corrupt-envelope poison (same absorbing pattern as
    /// [`HllSketchAggregator`]).
    mix_error: Option<String>,
}

impl QuantilesSketchAggregator {
    /// Create a quantiles sketch aggregator with the given compression budget
    /// (`k`).  A zero value falls back to [`DEFAULT_QUANTILES_COMPRESSION`].
    #[must_use]
    pub fn new(compression: usize) -> Self {
        let compression = if compression == 0 {
            DEFAULT_QUANTILES_COMPRESSION
        } else {
            compression
        };
        Self {
            digest: TDigest::new(compression),
            compression,
            mix_error: None,
        }
    }

    /// Estimate the value at quantile `q` (`q` in `[0, 1]`).  Returns `None`
    /// when the digest is empty, `q` is out of range, or the accumulator is
    /// poisoned (a corrupt envelope was fed — [`Aggregator::get`] carries
    /// the loud error, never a fabricated quantile).
    #[must_use]
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.mix_error.is_some() {
            return None;
        }
        self.digest.quantile(q).ok()
    }

    /// Poison this accumulator loudly (see the struct docs).
    fn poison(&mut self, message: String) {
        if self.mix_error.is_none() {
            self.mix_error = Some(message);
        }
    }

    /// Number of values added.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.digest.count()
    }

    /// Access the underlying digest (for typed merge).
    #[must_use]
    pub fn digest(&self) -> &TDigest {
        &self.digest
    }

    /// Route ONE classified sketch cell into this accumulator — shared by
    /// the `aggregate` feed and the dyn-merge fallback (see
    /// [`classify_sketch_cell`] and the module-level precedence).
    /// [`SketchCell::Foreign`] (raw numeric input) is handled by the
    /// callers.
    fn consume_envelope_cell(&mut self, cell: SketchCell) {
        match cell {
            SketchCell::Null | SketchCell::Foreign => {}
            // Rule 1: an upstream loud error propagates.
            SketchCell::MixError(message) => self.poison(message),
            // Rule 2: tagged-but-undecodable, ANY family → loud corrupt
            // (pre-fix a corrupt FOREIGN-family envelope was silently
            // skipped by the numeric fallback).
            SketchCell::Corrupt(species) => self.poison(corrupt_envelope_message(species.tag())),
            // Rule 4: empty valid partial of ANY species = identity.
            SketchCell::Empty(_) => {}
            // Rule 5: same-species centroid merge (exact count).
            SketchCell::NonEmpty(DecodedSketch::Quantiles(other)) => self.digest.merge(&other),
            // Rule 6: a non-empty envelope of a DIFFERENT species is a
            // LOUD cross-species mix (pre-fix it was silently skipped —
            // a silent whole-shard drop).
            SketchCell::NonEmpty(other) => {
                self.poison(cross_species_message(
                    SketchSpecies::Quantiles,
                    other.species(),
                ));
            }
        }
    }
}

impl Aggregator for QuantilesSketchAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let Some(v) = value else { return };
        // A poisoned accumulator absorbs everything (the error is final).
        if self.mix_error.is_some() {
            return;
        }
        let cell = classify_sketch_cell(v);
        if matches!(cell, SketchCell::Foreign) {
            // RAW input: numeric values are added; anything else is
            // skipped (unchanged legacy rule).
            if let Some(f) = v.as_f64() {
                self.digest.add(f);
            }
            return;
        }
        self.consume_envelope_cell(cell);
    }

    fn get(&self) -> serde_json::Value {
        if let Some(message) = &self.mix_error {
            return mix_error_envelope(message);
        }
        // The convenience `estimate` field carries the median (p50) for
        // quantiles sketches; consumers that want a specific quantile use the
        // post-aggregator path on the reconstructed digest.
        let median = self.digest.quantile(0.5).unwrap_or(f64::NAN);
        sketch_envelope(QUANTILES_SKETCH_TAG, &self.digest.serialize(), median)
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        if self.mix_error.is_some() {
            return;
        }
        if let Some(any) = other.as_any()
            && let Some(o) = any.downcast_ref::<QuantilesSketchAggregator>()
        {
            match &o.mix_error {
                Some(message) => self.poison(message.clone()),
                None => self.digest.merge(&o.digest),
            }
            return;
        }
        // Dyn-merge fallback: classify the peer's JSON partial through
        // the SAME cell rules as the row feed.  A non-sketch (Foreign)
        // peer is ignored (legacy — never added as a raw value on a merge
        // path).
        let other_value = other.get();
        let cell = classify_sketch_cell(&other_value);
        if !matches!(cell, SketchCell::Foreign) {
            self.consume_envelope_cell(cell);
        }
    }

    fn reset(&mut self) {
        self.digest = TDigest::new(self.compression);
        self.mix_error = None;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }
}

// ---------------------------------------------------------------------------
// Native-wire finalization for sketch partial states
// ---------------------------------------------------------------------------

/// Finalize one sketch aggregator output for the native query wire, the way
/// Apache Druid's `finalize` context semantics do (P1-#3).
///
/// Measured against real Apache Druid 36.0.0 (2026-07-12, local Docker,
/// `wikipedia_compat` fixture) — a native query with a RAW sketch
/// aggregation and `finalize` in effect (the default) returns:
///
/// * `HLLSketchBuild` / `HLLSketchMerge` — the unrounded double estimate
///   (e.g. `8.000000139077509`);
/// * `thetaSketch` — the double estimate (e.g. `8.0`);
/// * `quantilesDoublesSketch` — the sketch's value count `n` as an integer
///   (e.g. `10`; NOT a quantile);
/// * an empty timeseries bucket — the empty-sketch scalar (`0.0` for
///   HLL/theta, `0` for quantiles), while non-sketch aggregators keep
///   their own empty-bucket values.
///
/// Returns `Some(scalar)` when `spec` is a sketch aggregation and `value`
/// is its partial-state envelope (or JSON null for an empty bucket).
/// Returns `None` — caller keeps the value untouched — for every other
/// aggregator kind and for any undecodable value (fail-soft, mirroring
/// [`merge_sketch_json`]).  Notably `bloomFilter` returns `None` on
/// purpose: Druid's finalized bloom aggregation IS the filter itself, so
/// finalization must not collapse it to a scalar.
///
/// Per-aggregator opt-out (codex-review HIGH finding D on P1-#3): a spec
/// carrying `"shouldFinalize": false` (including through a `filtered`
/// wrapper) returns `None` — it keeps its intermediate sketch even when
/// the query-context `finalize` put the caller on the finalizing path,
/// matching Druid, where the per-aggregator flag overrides the context
/// default for that aggregator.
///
/// The estimate is computed by the same deserialize-and-estimate calls the
/// `HLLSketchEstimate` / `thetaSketchEstimate` post-aggregators use, so a
/// finalized native value always matches the SQL path's estimate for the
/// same sketch bytes.
#[must_use]
pub fn finalize_sketch_json(
    spec: &crate::AggregatorSpec,
    value: &serde_json::Value,
) -> Option<serde_json::Value> {
    // Per-aggregator `shouldFinalize: false` keeps the intermediate
    // (`should_finalize` recurses through `filtered` wrappers).
    if spec.should_finalize() == Some(false) {
        return None;
    }
    match spec {
        crate::AggregatorSpec::HllSketchBuild { .. }
        | crate::AggregatorSpec::HllSketchMerge { .. } => {
            let est = if value.is_null() {
                // Measured: Druid 36 finalizes an empty-bucket HLL to `0.0`.
                0.0
            } else {
                let bytes = envelope_bytes(value, HLL_SKETCH_TAG)?;
                HllSketch::deserialize(&bytes).ok()?.estimate()
            };
            serde_json::Number::from_f64(est).map(serde_json::Value::Number)
        }
        crate::AggregatorSpec::ThetaSketch { .. } => {
            let est = if value.is_null() {
                // Measured: Druid 36 finalizes an empty-bucket theta to `0.0`.
                0.0
            } else {
                let bytes = envelope_bytes(value, THETA_SKETCH_TAG)?;
                ThetaSketch::deserialize(&bytes).ok()?.estimate()
            };
            serde_json::Number::from_f64(est).map(serde_json::Value::Number)
        }
        crate::AggregatorSpec::QuantilesDoublesSketch { .. } => {
            let n = if value.is_null() {
                // Measured: Druid 36 finalizes an empty-bucket quantiles
                // sketch to integer `0`.
                0
            } else {
                let bytes = envelope_bytes(value, QUANTILES_SKETCH_TAG)?;
                TDigest::deserialize(&bytes).ok()?.count()
            };
            Some(serde_json::Value::Number(serde_json::Number::from(n)))
        }
        crate::AggregatorSpec::HyperUnique { round, .. } => {
            // A LOUD mix-error envelope must stay visible on the wire —
            // never collapse it into a fabricated scalar.
            if envelope_tag(value) == Some(SKETCH_MIX_ERROR_TAG) {
                return None;
            }
            let sketch = if value.is_null() {
                // An empty bucket finalizes like an empty sketch (estimate
                // exactly 0.0 — the linear-counting value of an untouched
                // register page).
                DruidHyperUnique::empty()
            } else {
                let bytes = envelope_bytes(value, DRUID_HYPER_UNIQUE_TAG)?;
                DruidHyperUnique::deserialize(&bytes).ok()?
            };
            if *round == Some(true) {
                // Druid's `"round": true` renders the nearest long (Java
                // `Math.round` semantics — the SQL BIGINT rendering).
                Some(serde_json::Value::Number(serde_json::Number::from(
                    sketch.estimate_rounded(),
                )))
            } else {
                // Native default: the RAW unrounded estimator double
                // (oracle: `12.03529418544122`, never `12`).
                serde_json::Number::from_f64(sketch.estimate()).map(serde_json::Value::Number)
            }
        }
        crate::AggregatorSpec::Filtered { aggregator, .. } => {
            finalize_sketch_json(aggregator, value)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Broker-side JSON merge for sketch partial states
// ---------------------------------------------------------------------------

/// Merge two sketch partial-state JSON envelopes by the UNIFORM
/// state-matrix rule (see [`classify_sketch_cell`] and the precedence
/// documented on it):
///
/// 1. A [`SKETCH_MIX_ERROR_TAG`] envelope on either side absorbs the
///    merge.
/// 2. A CORRUPT side (correctly tagged, payload fails to decode — ANY
///    family) merges into the LOUD corrupt-envelope error
///    ([`corrupt_envelope_message`]), checked BEFORE any species or null
///    handling, so `valid theta ⊕ corrupt hll` is loud in BOTH orders —
///    pre-fix a cross-family corrupt src was silently discarded (or,
///    reversed, the corrupt envelope was retained and the valid shard
///    discarded): an order-dependent silent undercount.
/// 3. A JSON `null` side is the empty-bucket IDENTITY (the shape
///    timeseries executors emit for synthetic empty buckets) and adopts
///    the other side.
/// 4. An EMPTY valid side of ANY species is the identity and adopts the
///    other side, whatever its species (emptiness, not species, decides
///    — R6); BOTH sides empty merge to the DETERMINISTIC canonical empty
///    ([`SketchSpecies::canonical_empty_envelope`] of the priority
///    species), identical in both arrival orders.
/// 5. Two NON-empty SAME-species sides union normally.  A DECODABLE but
///    parameter-incompatible union (theta hash-space/cross-seed, HLL
///    precision) keeps `dst` unchanged — the documented fail-soft drop,
///    the ONLY surviving fail-soft case (no corruption signal, only a
///    refusal to mix).
/// 6. Two NON-empty DIFFERENT-species sides merge into the LOUD
///    [`SKETCH_MIX_ERROR_TAG`] envelope (sorted deterministic message),
///    in BOTH orders — this includes the W-A decoded-Druid-hyperUnique ×
///    native-HLL strict no-mix, and (new with the state-matrix
///    hardening) every other species pairing that formerly fail-softed.
///
/// Values OUTSIDE the sketch state space (raw values, unknown/bloom
/// envelopes) keep the legacy fail-soft passthrough: `dst` is returned
/// unchanged.
#[must_use]
pub fn merge_sketch_json(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    merge_sketch_cells(
        dst,
        classify_sketch_cell(dst),
        src,
        classify_sketch_cell(src),
    )
}

/// The precedence engine behind [`merge_sketch_json`]: merge two
/// CLASSIFIED sketch cells.  The original JSON values are threaded
/// through so identity outcomes return the untouched envelope (byte-
/// preserving) rather than a re-serialization.
fn merge_sketch_cells(
    dst: &serde_json::Value,
    dst_cell: SketchCell,
    src: &serde_json::Value,
    src_cell: SketchCell,
) -> serde_json::Value {
    match (dst_cell, src_cell) {
        // Rule 1: a loud error absorbs everything it merges with.
        (SketchCell::MixError(_), _) => dst.clone(),
        (_, SketchCell::MixError(_)) => src.clone(),
        // Rule 2: corrupt is LOUD before species/null/empty handling.
        // Both-corrupt picks the priority species' tag so both arrival
        // orders produce the identical envelope.
        (SketchCell::Corrupt(a), SketchCell::Corrupt(b)) => corrupt_envelope_error(a.min(b).tag()),
        (SketchCell::Corrupt(a), _) => corrupt_envelope_error(a.tag()),
        (_, SketchCell::Corrupt(b)) => corrupt_envelope_error(b.tag()),
        // Rule 3: JSON `null` is the empty-bucket identity (the v1.1.1
        // broker-fold class: a shard with no rows must stay neutral).
        (SketchCell::Null, _) => src.clone(),
        (_, SketchCell::Null) => dst.clone(),
        // Out-of-scope values (raw / bloom / unknown tags): the legacy
        // fail-soft passthrough — keep dst unchanged.
        (SketchCell::Foreign, _) | (_, SketchCell::Foreign) => dst.clone(),
        // Rule 4: an EMPTY valid side of ANY species is the identity;
        // both-empty canonicalizes deterministically on the priority
        // species (identical in both arrival orders).
        (SketchCell::Empty(a), SketchCell::Empty(b)) => a.min(b).canonical_empty_envelope(),
        (SketchCell::Empty(_), SketchCell::NonEmpty(_)) => src.clone(),
        (SketchCell::NonEmpty(_), SketchCell::Empty(_)) => dst.clone(),
        // Rules 5/6: two NON-empty sides.
        (SketchCell::NonEmpty(d), SketchCell::NonEmpty(s)) => union_non_empty_sketches(dst, d, &s),
    }
}

/// Union two NON-empty decoded sketches (rules 5/6 of
/// [`merge_sketch_json`]): same species → normal union (with the
/// documented fail-soft drop for a DECODABLE-but-incompatible pair);
/// different species → the LOUD deterministic cross-species error.
fn union_non_empty_sketches(
    dst: &serde_json::Value,
    d: DecodedSketch,
    s: &DecodedSketch,
) -> serde_json::Value {
    match (d, s) {
        (DecodedSketch::Hll(mut d), DecodedSketch::Hll(s)) => {
            if d.merge(s).is_err() {
                // Decodable but precision-incompatible: the documented
                // fail-soft drop (no corruption signal, only a refusal to
                // mix).
                return dst.clone();
            }
            sketch_envelope(HLL_SKETCH_TAG, &d.serialize(), d.estimate())
        }
        (DecodedSketch::Theta(d), DecodedSketch::Theta(s)) => match d.union(s) {
            Ok(u) => sketch_envelope(THETA_SKETCH_TAG, &u.serialize(), u.estimate()),
            // A cross-hash-space union (native FNV vs Druid MurmurHash3
            // origin) or a cross-seed union (Druid-origin sketches decoded
            // with different update seeds) is refused by the sketch; keep
            // `dst` unchanged (the documented fail-soft drop).
            Err(_) => dst.clone(),
        },
        (DecodedSketch::Quantiles(mut d), DecodedSketch::Quantiles(s)) => {
            d.merge(s);
            let median = d.quantile(0.5).unwrap_or(f64::NAN);
            sketch_envelope(QUANTILES_SKETCH_TAG, &d.serialize(), median)
        }
        (DecodedSketch::DruidHyperUnique(mut d), DecodedSketch::DruidHyperUnique(s)) => {
            d.merge_in_place(s);
            druid_hyper_unique_envelope(&d)
        }
        // Rule 6: LOUD cross-species error, sorted so both arrival orders
        // produce the identical envelope.
        (d, s) => mix_error_envelope(&cross_species_message(d.species(), s.species())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- HLL ---

    #[test]
    fn hll_estimates_distinct_count_within_error() {
        let mut agg = HllSketchAggregator::build(14);
        for i in 0_u32..1000 {
            agg.aggregate(Some(&json!(i)));
        }
        let est = agg.estimate();
        let err = (est - 1000.0).abs() / 1000.0;
        assert!(err < 0.05, "HLL estimate {est} not within 5% of 1000");
    }

    #[test]
    fn hll_duplicates_estimate_one() {
        let mut agg = HllSketchAggregator::build(14);
        for _ in 0..500 {
            agg.aggregate(Some(&json!("same")));
        }
        assert!(agg.estimate() < 2.0);
    }

    #[test]
    fn hll_typed_merge_unions_registers() {
        let mut a = HllSketchAggregator::build(12);
        let mut b = HllSketchAggregator::build(12);
        for i in 0_u32..500 {
            a.aggregate(Some(&json!(i)));
        }
        for i in 500_u32..1000 {
            b.aggregate(Some(&json!(i)));
        }
        a.merge(&b);
        let err = (a.estimate() - 1000.0).abs() / 1000.0;
        assert!(err < 0.10, "merged HLL estimate {} off", a.estimate());
    }

    #[test]
    fn hll_json_merge_round_trip() {
        let mut a = HllSketchAggregator::build(12);
        let mut b = HllSketchAggregator::build(12);
        for i in 0_u32..500 {
            a.aggregate(Some(&json!(i)));
        }
        for i in 500_u32..1000 {
            b.aggregate(Some(&json!(i)));
        }
        let merged = merge_sketch_json(&a.get(), &b.get());
        let est = merged
            .get("estimate")
            .and_then(serde_json::Value::as_f64)
            .expect("estimate");
        let err = (est - 1000.0).abs() / 1000.0;
        assert!(err < 0.10, "json-merged HLL estimate {est} off");
    }

    #[test]
    fn hll_merge_mode_consumes_serialized_sketches() {
        // Build two partial sketches, then feed their serialized forms into a
        // merge-mode aggregator one row at a time.
        let mut a = HllSketchAggregator::build(12);
        let mut b = HllSketchAggregator::build(12);
        for i in 0_u32..500 {
            a.aggregate(Some(&json!(i)));
        }
        for i in 500_u32..1000 {
            b.aggregate(Some(&json!(i)));
        }
        let mut merger = HllSketchAggregator::merge(12);
        merger.aggregate(Some(&a.get()));
        merger.aggregate(Some(&b.get()));
        let err = (merger.estimate() - 1000.0).abs() / 1000.0;
        assert!(err < 0.10, "merge-mode estimate {} off", merger.estimate());
    }

    // --- Theta ---

    #[test]
    fn theta_estimates_distinct_count() {
        let mut agg = ThetaSketchAggregator::new(4096);
        for i in 0_u32..5000 {
            agg.aggregate(Some(&json!(i)));
        }
        let err = (agg.estimate() - 5000.0).abs() / 5000.0;
        assert!(err < 0.10, "theta estimate {} off", agg.estimate());
    }

    #[test]
    fn theta_typed_merge_unions() {
        let mut a = ThetaSketchAggregator::new(4096);
        let mut b = ThetaSketchAggregator::new(4096);
        for i in 0_u32..2000 {
            a.aggregate(Some(&json!(i)));
        }
        for i in 2000_u32..4000 {
            b.aggregate(Some(&json!(i)));
        }
        a.merge(&b);
        let err = (a.estimate() - 4000.0).abs() / 4000.0;
        assert!(err < 0.15, "theta merge estimate {} off", a.estimate());
    }

    /// Build a DataSketches compact theta image (preLongs 2, exact mode)
    /// with an explicit 16-bit seed hash in preamble bytes 6-7.
    fn druid_compact_image(seed_hash: u16, hashes: &[u64]) -> Vec<u8> {
        let seed = seed_hash.to_le_bytes();
        let mut buf = vec![2u8, 3, 3, 12, 13, 0, seed[0], seed[1]];
        buf.extend_from_slice(
            &i32::try_from(hashes.len())
                .expect("test count fits i32")
                .to_le_bytes(),
        );
        buf.extend_from_slice(&0u32.to_le_bytes());
        for &h in hashes {
            buf.extend_from_slice(&h.to_le_bytes());
        }
        buf
    }

    /// Two Druid-origin sketches decoded with DIFFERENT update seeds must
    /// never union into a wrong estimate: the incompatible side is DROPPED
    /// fail-soft (undercount, never a corrupt over-count) — the same
    /// treatment as the existing hash-space mismatch.  Same-seed sketches
    /// union normally.
    #[test]
    fn theta_cross_seed_envelopes_drop_fail_soft() {
        let a = ThetaSketch::from_druid_compact(&druid_compact_image(0x931E, &[1, 2]))
            .expect("decode a");
        let b = ThetaSketch::from_druid_compact(&druid_compact_image(0xBEEF, &[1, 2]))
            .expect("decode b");
        // Aggregator feed: the first envelope is adopted; the cross-seed
        // second is dropped (estimate stays 2 — never the double-counted 4).
        let mut agg = ThetaSketchAggregator::new(4096);
        agg.aggregate(Some(&theta_sketch_envelope(&a)));
        agg.aggregate(Some(&theta_sketch_envelope(&b)));
        assert!((agg.estimate() - 2.0).abs() < f64::EPSILON);
        // Broker-side JSON merge likewise keeps dst unchanged.
        let merged = merge_sketch_json(&theta_sketch_envelope(&a), &theta_sketch_envelope(&b));
        assert_eq!(merged, theta_sketch_envelope(&a));
        // Same seed → unions normally (A→B semantics preserved).
        let c = ThetaSketch::from_druid_compact(&druid_compact_image(0x931E, &[3, 4]))
            .expect("decode c");
        let merged = merge_sketch_json(&theta_sketch_envelope(&a), &theta_sketch_envelope(&c));
        let est = merged
            .get("estimate")
            .and_then(serde_json::Value::as_f64)
            .expect("estimate");
        assert!(
            (est - 4.0).abs() < f64::EPSILON,
            "same-seed union estimate {est}"
        );
    }

    /// A `tag`-tagged envelope whose payload is valid base64 but
    /// undecodable sketch bytes (rejected by every family's
    /// `deserialize` on version/length).
    fn corrupt_envelope(tag: &str) -> serde_json::Value {
        use base64::Engine as _;
        let b64 =
            base64::engine::general_purpose::STANDARD.encode([0xDEu8, 0xAD, 0xBE, 0xEF, 0x01]);
        json!({"@sketch": tag, "bytes": b64})
    }

    /// A `tag`-tagged envelope whose payload is not even base64.
    fn bad_base64_envelope(tag: &str) -> serde_json::Value {
        json!({"@sketch": tag, "bytes": "%%%not-base64%%%"})
    }

    /// Assert `value` is the LOUD mix-error envelope.
    fn assert_loud(value: &serde_json::Value, context: &str) {
        assert_eq!(
            value.get("@sketch").and_then(serde_json::Value::as_str),
            Some(SKETCH_MIX_ERROR_TAG),
            "{context}: expected the LOUD error envelope, got {value}"
        );
    }

    /// A value TAGGED as a theta envelope whose payload is undecodable
    /// (corrupt sketch bytes, or bytes that are not even base64) must
    /// poison the accumulator LOUDLY, in BOTH arrival orders.  Pre-fix
    /// it was silently dropped, so `valid ⊕ corrupt` finalized to the
    /// valid side's lone estimate — a plausible number that OMITS an
    /// entire row's/shard's contribution — an ORDER-DEPENDENT silent
    /// undercount.  (Older still, it fell through to `add_hash`,
    /// counting the envelope's JSON text as a phantom distinct value —
    /// still guarded: the estimate must never gain a phantom +1.)
    #[test]
    fn theta_corrupt_envelope_poisons_loud_both_orders() {
        for corrupt in [
            corrupt_envelope(THETA_SKETCH_TAG),
            bad_base64_envelope(THETA_SKETCH_TAG),
        ] {
            let mut valid_first = ThetaSketchAggregator::new(4096);
            valid_first.aggregate(Some(&json!("a")));
            valid_first.aggregate(Some(&json!("b")));
            valid_first.aggregate(Some(&corrupt));

            let mut corrupt_first = ThetaSketchAggregator::new(4096);
            corrupt_first.aggregate(Some(&corrupt));
            corrupt_first.aggregate(Some(&json!("a")));
            corrupt_first.aggregate(Some(&json!("b")));

            for agg in [&valid_first, &corrupt_first] {
                assert_loud(&agg.get(), "theta corrupt envelope");
                assert!(
                    agg.estimate().is_nan(),
                    "poisoned theta must not fabricate an estimate, got {}",
                    agg.estimate()
                );
            }
            assert_eq!(
                valid_first.get(),
                corrupt_first.get(),
                "both arrival orders must yield the identical loud error"
            );
        }
    }

    // --- Corrupt tagged envelopes are LOUD on every path (this fix) ---

    /// The HIGH order-dependent silent-undercount bug: a correctly-TAGGED
    /// `druidHyperUnique` envelope whose payload FAILS TO DECODE must
    /// poison the SQL accumulator (`HllSketchAggregator`) LOUDLY in BOTH
    /// arrival orders.  Pre-fix `consume_druid_hyper_unique` silently
    /// dropped it, so `valid ⊕ corrupt` emitted the valid side's lone
    /// estimate — a plausible number omitting a whole row/shard.
    #[test]
    fn hll_corrupt_druid_hyper_unique_envelope_poisons_loud_both_orders() {
        let valid = druid_hyper_unique_envelope(&hyper_unique(1, &[(10, 0x02)]));
        for corrupt in [
            corrupt_envelope(DRUID_HYPER_UNIQUE_TAG),
            bad_base64_envelope(DRUID_HYPER_UNIQUE_TAG),
        ] {
            let mut valid_first = HllSketchAggregator::build(14);
            valid_first.aggregate(Some(&valid));
            valid_first.aggregate(Some(&corrupt));

            let mut corrupt_first = HllSketchAggregator::build(14);
            corrupt_first.aggregate(Some(&corrupt));
            corrupt_first.aggregate(Some(&valid));

            for agg in [&valid_first, &corrupt_first] {
                assert_loud(&agg.get(), "hll ⟵ corrupt druidHyperUnique");
                assert!(agg.estimate().is_nan());
            }
            assert_eq!(
                valid_first.get(),
                corrupt_first.get(),
                "both arrival orders must yield the identical loud error"
            );
        }
    }

    /// A corrupt tagged Druid envelope followed by NATIVE input must not
    /// silently produce an ordinary FNV-HLL result — pre-fix the corrupt
    /// envelope was ignored, the native rows hashed, and `get` emitted a
    /// plausible plain `hll` envelope (the intended fail-loud vanished).
    #[test]
    fn hll_corrupt_druid_envelope_then_native_feed_stays_loud() {
        let mut agg = HllSketchAggregator::build(14);
        agg.aggregate(Some(&corrupt_envelope(DRUID_HYPER_UNIQUE_TAG)));
        agg.aggregate(Some(&json!("u1")));
        agg.aggregate(Some(&json!("u2")));
        assert_loud(&agg.get(), "corrupt druidHyperUnique then native feed");
        assert!(agg.estimate().is_nan());
    }

    /// The same rule on the merge-only `HyperUniqueAggregator`: a tagged
    /// `druidHyperUnique` envelope that fails to decode poisons loudly in
    /// BOTH arrival orders, and finalization refuses to fabricate a
    /// scalar.
    #[test]
    fn hyper_unique_corrupt_envelope_poisons_loud_both_orders() {
        let valid = druid_hyper_unique_envelope(&hyper_unique(1, &[(10, 0x02)]));
        for corrupt in [
            corrupt_envelope(DRUID_HYPER_UNIQUE_TAG),
            bad_base64_envelope(DRUID_HYPER_UNIQUE_TAG),
        ] {
            let mut valid_first = HyperUniqueAggregator::new();
            valid_first.aggregate(Some(&valid));
            valid_first.aggregate(Some(&corrupt));

            let mut corrupt_first = HyperUniqueAggregator::new();
            corrupt_first.aggregate(Some(&corrupt));
            corrupt_first.aggregate(Some(&valid));

            for agg in [&valid_first, &corrupt_first] {
                assert_loud(&agg.get(), "hyperUnique corrupt envelope");
                assert!(agg.estimate().is_nan());
                assert_eq!(
                    finalize_sketch_json(&hyper_unique_spec(false), &agg.get()),
                    None,
                    "a poisoned aggregation must never finalize to a scalar"
                );
            }
            assert_eq!(valid_first.get(), corrupt_first.get());
        }
    }

    /// The same rule for the NATIVE `hll` tag: a merge-mode accumulator
    /// receiving a tagged-but-undecodable native partial poisons loud in
    /// both orders (pre-fix: silent drop → the valid shard's lone
    /// estimate).
    #[test]
    fn hll_corrupt_native_envelope_poisons_loud_both_orders() {
        let mut shard = HllSketchAggregator::build(12);
        shard.aggregate(Some(&json!("a")));
        let valid = shard.get();
        for corrupt in [
            corrupt_envelope(HLL_SKETCH_TAG),
            bad_base64_envelope(HLL_SKETCH_TAG),
        ] {
            let mut valid_first = HllSketchAggregator::merge(12);
            valid_first.aggregate(Some(&valid));
            valid_first.aggregate(Some(&corrupt));

            let mut corrupt_first = HllSketchAggregator::merge(12);
            corrupt_first.aggregate(Some(&corrupt));
            corrupt_first.aggregate(Some(&valid));

            for agg in [&valid_first, &corrupt_first] {
                assert_loud(&agg.get(), "hll corrupt native envelope");
                assert!(agg.estimate().is_nan());
            }
            assert_eq!(valid_first.get(), corrupt_first.get());
        }
    }

    /// … and for the `quantiles` tag (uniform rule for every tagged
    /// sketch envelope): tagged-but-undecodable poisons loud, both
    /// orders.
    #[test]
    fn quantiles_corrupt_envelope_poisons_loud_both_orders() {
        let mut shard = QuantilesSketchAggregator::new(100);
        for i in 1..=10 {
            shard.aggregate(Some(&json!(i)));
        }
        let valid = shard.get();
        for corrupt in [
            corrupt_envelope(QUANTILES_SKETCH_TAG),
            bad_base64_envelope(QUANTILES_SKETCH_TAG),
        ] {
            let mut valid_first = QuantilesSketchAggregator::new(100);
            valid_first.aggregate(Some(&valid));
            valid_first.aggregate(Some(&corrupt));

            let mut corrupt_first = QuantilesSketchAggregator::new(100);
            corrupt_first.aggregate(Some(&corrupt));
            corrupt_first.aggregate(Some(&valid));

            for agg in [&valid_first, &corrupt_first] {
                assert_loud(&agg.get(), "quantiles corrupt envelope");
                assert_eq!(
                    agg.quantile(0.5),
                    None,
                    "poisoned quantiles must not fabricate a quantile"
                );
            }
            assert_eq!(valid_first.get(), corrupt_first.get());
        }
    }

    /// Broker-side JSON merge: `valid ⊕ corrupt` must merge into the
    /// LOUD error envelope in BOTH arrival orders for EVERY tagged
    /// sketch family.  Pre-fix, the fallback `return dst` silently
    /// DISCARDED the corrupt src (a plausible estimate omitting an
    /// entire shard) or — in the reverse arrival order — RETAINED the
    /// corrupt envelope and discarded the valid shard instead: an
    /// order-dependent silent undercount.
    #[test]
    fn merge_sketch_json_corrupt_tagged_envelope_is_loud_both_orders() {
        let mut h = HllSketchAggregator::build(12);
        h.aggregate(Some(&json!("x")));
        let mut t = ThetaSketchAggregator::new(4096);
        t.aggregate(Some(&json!("x")));
        let mut q = QuantilesSketchAggregator::new(100);
        q.aggregate(Some(&json!(1.0)));
        let dhu = druid_hyper_unique_envelope(&hyper_unique(1, &[(10, 0x02)]));
        let cases = [
            (HLL_SKETCH_TAG, h.get()),
            (THETA_SKETCH_TAG, t.get()),
            (QUANTILES_SKETCH_TAG, q.get()),
            (DRUID_HYPER_UNIQUE_TAG, dhu),
        ];
        for (tag, valid) in cases {
            for corrupt in [corrupt_envelope(tag), bad_base64_envelope(tag)] {
                let vc = merge_sketch_json(&valid, &corrupt);
                let cv = merge_sketch_json(&corrupt, &valid);
                assert_loud(&vc, &format!("{tag}: valid ⊕ corrupt"));
                assert_loud(&cv, &format!("{tag}: corrupt ⊕ valid"));
                assert_eq!(vc, cv, "{tag}: both orders must be identical");
                assert_ne!(vc, valid, "{tag}: never the valid side's lone estimate");
                // The loud result keeps absorbing further merges, and a
                // null (empty synthetic bucket) stays neutral around it.
                assert_eq!(merge_sketch_json(&vc, &valid), vc);
                assert_eq!(merge_sketch_json(&vc, &serde_json::Value::Null), vc);
                assert_eq!(merge_sketch_json(&serde_json::Value::Null, &vc), vc);
            }
        }
    }

    /// The CRITICAL distinction: an EMPTY-but-VALID envelope (decodes to
    /// an empty sketch) stays the identity/empty — the corrupt-envelope
    /// rule keys on DECODE FAILURE, not on emptiness — and JSON `null`
    /// keeps its empty-bucket-identity behavior.
    #[test]
    fn empty_valid_envelopes_stay_identity_not_error() {
        // theta: an empty valid envelope unions as a no-op, both sides.
        let mut t = ThetaSketchAggregator::new(4096);
        t.aggregate(Some(&json!("a")));
        let valid = t.get();
        let empty = ThetaSketchAggregator::new(4096).get();
        for merged in [
            merge_sketch_json(&valid, &empty),
            merge_sketch_json(&empty, &valid),
        ] {
            assert_eq!(
                merged.get("estimate").and_then(serde_json::Value::as_f64),
                Some(1.0),
                "empty-valid theta envelope must stay neutral, got {merged}"
            );
        }
        // theta aggregator feed: empty envelope then raw values.
        let mut t2 = ThetaSketchAggregator::new(4096);
        t2.aggregate(Some(&empty));
        t2.aggregate(Some(&json!("a")));
        assert!((t2.estimate() - 1.0).abs() < f64::EPSILON);

        // druidHyperUnique: an empty valid envelope folds as identity.
        let a = hyper_unique(1, &[(10, 0x02)]);
        let empty_dhu = druid_hyper_unique_envelope(&DruidHyperUnique::empty());
        let mut hu = HyperUniqueAggregator::new();
        hu.aggregate(Some(&empty_dhu));
        hu.aggregate(Some(&druid_hyper_unique_envelope(&a)));
        assert_eq!(hu.estimate().to_bits(), a.estimate().to_bits());
        assert_eq!(
            hu.get().get("@sketch").and_then(serde_json::Value::as_str),
            Some(DRUID_HYPER_UNIQUE_TAG)
        );
    }

    /// `reset` must preserve the configured Druid `size` parameter, the
    /// way the HLL aggregator preserves precision and the quantiles
    /// aggregator preserves compression — pre-fix it reset to the 4096
    /// default, silently coarsening any executor that reuses the
    /// aggregator across groups/buckets.
    #[test]
    fn theta_reset_preserves_configured_size() {
        // A deliberately tiny retention budget so the size visibly shapes
        // the estimate (500 distinct >> 16 retained ⇒ approximate mode).
        let mut fresh = ThetaSketchAggregator::new(16);
        for i in 0_u32..500 {
            fresh.aggregate(Some(&json!(i)));
        }
        let fresh_estimate = fresh.estimate();
        assert!(
            (fresh_estimate - 500.0).abs() > f64::EPSILON,
            "size 16 must estimate approximately, got exact {fresh_estimate}"
        );

        // Reset, then replay the identical feed: hashing is deterministic,
        // so a parameter-faithful reset reproduces the estimate EXACTLY.
        // (Pre-fix, the post-reset sketch had the 4096 default budget and
        // returned exactly 500.0 instead.)
        fresh.reset();
        assert!(fresh.estimate().abs() < f64::EPSILON, "reset must empty");
        for i in 0_u32..500 {
            fresh.aggregate(Some(&json!(i)));
        }
        assert!(
            (fresh.estimate() - fresh_estimate).abs() < f64::EPSILON,
            "post-reset estimate {} must match the pre-reset configured-size \
             estimate {fresh_estimate}",
            fresh.estimate()
        );
    }

    #[test]
    fn theta_json_merge_round_trip() {
        let mut a = ThetaSketchAggregator::new(4096);
        let mut b = ThetaSketchAggregator::new(4096);
        for i in 0_u32..2000 {
            a.aggregate(Some(&json!(i)));
        }
        for i in 2000_u32..4000 {
            b.aggregate(Some(&json!(i)));
        }
        let merged = merge_sketch_json(&a.get(), &b.get());
        let est = merged
            .get("estimate")
            .and_then(serde_json::Value::as_f64)
            .expect("estimate");
        let err = (est - 4000.0).abs() / 4000.0;
        assert!(err < 0.15, "json-merged theta estimate {est} off");
    }

    // --- Quantiles ---

    #[test]
    fn quantiles_p50_p95_within_tolerance() {
        let mut agg = QuantilesSketchAggregator::new(200);
        for i in 1..=1000 {
            agg.aggregate(Some(&json!(i)));
        }
        let p50 = agg.quantile(0.50).expect("p50");
        let p95 = agg.quantile(0.95).expect("p95");
        assert!((p50 - 500.0).abs() / 500.0 < 0.05, "p50={p50}");
        assert!((p95 - 950.0).abs() / 950.0 < 0.05, "p95={p95}");
    }

    #[test]
    fn quantiles_typed_merge() {
        let mut a = QuantilesSketchAggregator::new(200);
        let mut b = QuantilesSketchAggregator::new(200);
        for i in 1..=500 {
            a.aggregate(Some(&json!(i)));
        }
        for i in 501..=1000 {
            b.aggregate(Some(&json!(i)));
        }
        a.merge(&b);
        assert_eq!(a.count(), 1000);
        let p50 = a.quantile(0.5).expect("p50");
        assert!((p50 - 500.0).abs() / 500.0 < 0.10, "merged p50={p50}");
    }

    #[test]
    fn quantiles_json_merge_round_trip() {
        let mut a = QuantilesSketchAggregator::new(200);
        let mut b = QuantilesSketchAggregator::new(200);
        for i in 1..=500 {
            a.aggregate(Some(&json!(i)));
        }
        for i in 501..=1000 {
            b.aggregate(Some(&json!(i)));
        }
        let merged = merge_sketch_json(&a.get(), &b.get());
        let bytes = envelope_bytes(&merged, QUANTILES_SKETCH_TAG).expect("bytes");
        let digest = TDigest::deserialize(&bytes).expect("deser");
        assert_eq!(digest.count(), 1000);
        let p50 = digest.quantile(0.5).expect("p50");
        assert!((p50 - 500.0).abs() / 500.0 < 0.10, "json-merged p50={p50}");
    }

    #[test]
    fn reset_clears_state() {
        let mut hll = HllSketchAggregator::build(12);
        hll.aggregate(Some(&json!(1)));
        hll.reset();
        assert!(hll.estimate() < 1.0);

        let mut theta = ThetaSketchAggregator::new(512);
        theta.aggregate(Some(&json!(1)));
        theta.reset();
        assert!((theta.estimate() - 0.0).abs() < f64::EPSILON);

        let mut q = QuantilesSketchAggregator::new(100);
        q.aggregate(Some(&json!(1.0)));
        q.reset();
        assert_eq!(q.count(), 0);
    }

    /// UPDATED (state-matrix hardening; formerly
    /// `envelope_tag_mismatch_is_fail_soft`): two EMPTY valid envelopes of
    /// DIFFERENT species no longer keep whichever side happened to arrive
    /// as `dst` (an order-dependent result) — both-empty merges to the
    /// DETERMINISTIC canonical empty of the priority species (uniform
    /// rule 4), identical in both arrival orders.
    #[test]
    fn cross_family_both_empty_merges_to_deterministic_empty() {
        let hll = HllSketchAggregator::build(14).get();
        let theta = ThetaSketchAggregator::new(4096).get();
        let fwd = merge_sketch_json(&hll, &theta);
        let bwd = merge_sketch_json(&theta, &hll);
        // Priority order: hll < theta — the canonical empty is the
        // default-parameter native HLL envelope.
        assert_eq!(fwd, hll, "both-empty cross-family must canonicalize");
        assert_eq!(fwd, bwd, "both orders must be identical");
    }

    // --- Decoded Druid hyperUnique (W-A, v1.5.0) ---

    /// Build a decoded sketch from a sparse Druid blob with the given
    /// `(byte position, packed value byte)` pairs.
    fn hyper_unique(non_zero: u16, pairs: &[(u16, u8)]) -> DruidHyperUnique {
        let mut blob = vec![0x01, 0x00];
        blob.extend_from_slice(&non_zero.to_be_bytes());
        blob.extend_from_slice(&[0, 0, 0]);
        for &(pos, val) in pairs {
            blob.extend_from_slice(&pos.to_be_bytes());
            blob.push(val);
        }
        DruidHyperUnique::from_druid_blob(&blob).expect("decode blob")
    }

    fn hyper_unique_spec(round: bool) -> crate::AggregatorSpec {
        serde_json::from_value(json!({
            "type": "hyperUnique", "name": "uu", "fieldName": "uu", "round": round
        }))
        .expect("hyperUnique spec")
    }

    /// The merge-only hyperUnique aggregator folds envelope feeds by
    /// register-wise max and finalizes through the Druid-parity estimator
    /// (raw double by default, Java-`Math.round` long under `round`).
    #[test]
    fn hyper_unique_aggregator_folds_and_finalizes() {
        let a = hyper_unique(1, &[(10, 0x02)]);
        let b = hyper_unique(2, &[(10, 0x01), (20, 0x30)]);
        let mut agg = HyperUniqueAggregator::new();
        agg.aggregate(Some(&druid_hyper_unique_envelope(&a)));
        agg.aggregate(Some(&druid_hyper_unique_envelope(&b)));
        agg.aggregate(Some(&serde_json::Value::Null)); // null rows are skipped
        let expected = a.merged(&b);
        assert_eq!(agg.estimate().to_bits(), expected.estimate().to_bits());

        let fin = finalize_sketch_json(&hyper_unique_spec(false), &agg.get()).expect("finalize");
        assert_eq!(
            fin.as_f64().map(f64::to_bits),
            Some(expected.estimate().to_bits())
        );
        let rounded =
            finalize_sketch_json(&hyper_unique_spec(true), &agg.get()).expect("finalize rounded");
        assert_eq!(rounded.as_i64(), Some(expected.estimate_rounded()));

        // Empty-bucket null finalizes to the empty-sketch scalar.
        assert_eq!(
            finalize_sketch_json(&hyper_unique_spec(false), &serde_json::Value::Null)
                .and_then(|v| v.as_f64()),
            Some(0.0)
        );
    }

    /// Raw values (and foreign sketch envelopes) into the merge-only
    /// hyperUnique aggregator poison it LOUDLY: `get` emits the mix-error
    /// envelope and finalization refuses to produce a scalar.
    #[test]
    fn hyper_unique_aggregator_poisons_on_raw_input() {
        let mut agg = HyperUniqueAggregator::new();
        agg.aggregate(Some(&druid_hyper_unique_envelope(&hyper_unique(
            1,
            &[(8, 0x01)],
        ))));
        agg.aggregate(Some(&json!("raw user id")));
        let out = agg.get();
        assert_eq!(
            out.get("@sketch").and_then(serde_json::Value::as_str),
            Some(SKETCH_MIX_ERROR_TAG)
        );
        assert!(agg.estimate().is_nan());
        assert_eq!(finalize_sketch_json(&hyper_unique_spec(false), &out), None);
        // The poison is final: later valid envelopes do not resurrect it.
        agg.aggregate(Some(&druid_hyper_unique_envelope(&hyper_unique(
            1,
            &[(9, 0x01)],
        ))));
        assert_eq!(
            agg.get().get("@sketch").and_then(serde_json::Value::as_str),
            Some(SKETCH_MIX_ERROR_TAG)
        );
    }

    /// The SQL accumulator (`HllSketchAggregator`) ADOPTS a decoded
    /// hyperUnique feed while EMPTY, keeps folding decoded feeds, and its
    /// envelope finalizes through the Druid-parity estimator — while a
    /// NON-empty native accumulator (or an adopted one fed raw values)
    /// poisons loudly instead of mixing hash spaces.
    #[test]
    fn hll_aggregator_adopts_druid_hyper_unique_when_empty_and_poisons_on_mix() {
        let a = hyper_unique(1, &[(10, 0x02)]);
        let b = hyper_unique(1, &[(20, 0x30)]);

        // Empty accumulator adopts, then folds.
        let mut agg = HllSketchAggregator::build(14);
        agg.aggregate(Some(&druid_hyper_unique_envelope(&a)));
        agg.aggregate(Some(&druid_hyper_unique_envelope(&b)));
        let expected = a.merged(&b);
        assert_eq!(agg.estimate().to_bits(), expected.estimate().to_bits());
        assert_eq!(
            agg.get().get("@sketch").and_then(serde_json::Value::as_str),
            Some(DRUID_HYPER_UNIQUE_TAG)
        );

        // Adopted accumulator refuses raw values loudly.
        agg.aggregate(Some(&json!("raw value")));
        assert_eq!(
            agg.get().get("@sketch").and_then(serde_json::Value::as_str),
            Some(SKETCH_MIX_ERROR_TAG)
        );

        // Non-empty native accumulator refuses the decoded feed loudly.
        let mut native = HllSketchAggregator::build(14);
        native.aggregate(Some(&json!("v1")));
        native.aggregate(Some(&druid_hyper_unique_envelope(&a)));
        assert_eq!(
            native
                .get()
                .get("@sketch")
                .and_then(serde_json::Value::as_str),
            Some(SKETCH_MIX_ERROR_TAG)
        );
        assert!(native.estimate().is_nan());
    }

    /// Typed merge between accumulators follows the same adoption /
    /// no-mix rules.
    #[test]
    fn hll_aggregator_typed_merge_adoption_rules() {
        let a = hyper_unique(1, &[(10, 0x02)]);

        // adopted ⟵ empty-native: no-op.
        let mut adopted = HllSketchAggregator::build(14);
        adopted.aggregate(Some(&druid_hyper_unique_envelope(&a)));
        let empty = HllSketchAggregator::build(14);
        adopted.merge(&empty);
        assert_eq!(adopted.estimate().to_bits(), a.estimate().to_bits());

        // empty-native ⟵ adopted: adopts.
        let mut empty2 = HllSketchAggregator::build(14);
        empty2.merge(&adopted);
        assert_eq!(empty2.estimate().to_bits(), a.estimate().to_bits());

        // non-empty native ⟵ adopted: loud poison.
        let mut native = HllSketchAggregator::build(14);
        native.aggregate(Some(&json!("v1")));
        native.merge(&adopted);
        assert_eq!(
            native
                .get()
                .get("@sketch")
                .and_then(serde_json::Value::as_str),
            Some(SKETCH_MIX_ERROR_TAG)
        );

        // adopted ⟵ non-empty native: loud poison.
        let mut adopted2 = HllSketchAggregator::build(14);
        adopted2.aggregate(Some(&druid_hyper_unique_envelope(&a)));
        let mut native2 = HllSketchAggregator::build(14);
        native2.aggregate(Some(&json!("v1")));
        adopted2.merge(&native2);
        assert_eq!(
            adopted2
                .get()
                .get("@sketch")
                .and_then(serde_json::Value::as_str),
            Some(SKETCH_MIX_ERROR_TAG)
        );
    }

    /// R6: an EMPTY-but-VALID native `hll` partial is the merge IDENTITY
    /// in BOTH arrival orders around a decoded-hyperUnique adoption.
    /// Pre-fix, the adopted-Druid guard in `aggregate` ran BEFORE the
    /// native-HLL envelope decode, so `dhu ⊕ empty-hll` POISONED while
    /// the reverse order adopted cleanly — an order-dependent poison of
    /// a partial that carries no distinct values at all.
    #[test]
    fn hll_aggregator_empty_hll_partial_is_identity_both_orders() {
        let a = hyper_unique(1, &[(10, 0x02)]);
        let dhu = druid_hyper_unique_envelope(&a);
        let empty_hll = HllSketchAggregator::build(14).get();

        // Order 1 (pre-fix POISON): adopt the decoded sketch, then feed
        // the empty-valid native partial.
        let mut adopted_first = HllSketchAggregator::build(14);
        adopted_first.aggregate(Some(&dhu));
        adopted_first.aggregate(Some(&empty_hll));
        // Order 2 (already correct): empty-valid native partial first.
        let mut empty_first = HllSketchAggregator::build(14);
        empty_first.aggregate(Some(&empty_hll));
        empty_first.aggregate(Some(&dhu));

        for (agg, ctx) in [
            (&adopted_first, "dhu ⊕ empty-hll"),
            (&empty_first, "empty-hll ⊕ dhu"),
        ] {
            let out = agg.get();
            assert_eq!(
                out.get("@sketch").and_then(serde_json::Value::as_str),
                Some(DRUID_HYPER_UNIQUE_TAG),
                "{ctx}: the empty partial must be identity, not poison; got {out}"
            );
            assert_eq!(
                agg.estimate().to_bits(),
                a.estimate().to_bits(),
                "{ctx}: must keep the adopted estimate"
            );
        }
        assert_eq!(
            adopted_first.get(),
            empty_first.get(),
            "both arrival orders must be identical"
        );
    }

    /// R6 mirror: an EMPTY-but-VALID decoded `druidHyperUnique` partial
    /// is the merge IDENTITY in BOTH arrival orders around native-HLL
    /// content.  Pre-fix BOTH orders were wrong: fed to a NON-empty
    /// native accumulator it poisoned (the no-mix arm keyed on species,
    /// not emptiness), and fed FIRST it was ADOPTED, flipping the
    /// accumulator into decoded mode so the later native partial (or raw
    /// value) poisoned instead.
    #[test]
    fn hll_aggregator_empty_dhu_partial_is_identity_both_orders() {
        let empty_dhu = druid_hyper_unique_envelope(&DruidHyperUnique::empty());
        let mut src = HllSketchAggregator::build(14);
        src.aggregate(Some(&json!("v1")));
        src.aggregate(Some(&json!("v2")));
        let hll_env = src.get();
        let expected = src.estimate();

        // Order 1 (pre-fix POISON): non-empty native partial, then the
        // empty decoded partial.
        let mut native_first = HllSketchAggregator::build(14);
        native_first.aggregate(Some(&hll_env));
        native_first.aggregate(Some(&empty_dhu));
        // Order 2 (pre-fix POISON via phantom adoption): empty decoded
        // partial first, then the non-empty native partial.
        let mut empty_first = HllSketchAggregator::build(14);
        empty_first.aggregate(Some(&empty_dhu));
        empty_first.aggregate(Some(&hll_env));

        for (agg, ctx) in [
            (&native_first, "hll ⊕ empty-dhu"),
            (&empty_first, "empty-dhu ⊕ hll"),
        ] {
            let out = agg.get();
            assert_eq!(
                out.get("@sketch").and_then(serde_json::Value::as_str),
                Some(HLL_SKETCH_TAG),
                "{ctx}: the empty partial must be identity, not poison; got {out}"
            );
            assert_eq!(
                agg.estimate().to_bits(),
                expected.to_bits(),
                "{ctx}: must keep the native estimate"
            );
        }
        assert_eq!(
            native_first.get(),
            empty_first.get(),
            "both arrival orders must be identical"
        );

        // Raw-value flavor of order 2: the empty decoded partial must
        // not block a later raw add either.
        let mut raw_after_empty = HllSketchAggregator::build(14);
        raw_after_empty.aggregate(Some(&empty_dhu));
        raw_after_empty.aggregate(Some(&json!("v1")));
        assert_eq!(
            raw_after_empty
                .get()
                .get("@sketch")
                .and_then(serde_json::Value::as_str),
            Some(HLL_SKETCH_TAG),
            "empty-dhu ⊕ raw value must stay a native accumulator"
        );
        assert!(raw_after_empty.estimate() > 0.0);
    }

    /// The strict no-mix loudness is UNTOUCHED by the empty-identity
    /// rule: two NON-empty partials of different species still poison
    /// loudly, in BOTH arrival orders.
    #[test]
    fn hll_aggregator_non_empty_cross_species_still_poisons_both_orders() {
        let dhu = druid_hyper_unique_envelope(&hyper_unique(1, &[(10, 0x02)]));
        let mut src = HllSketchAggregator::build(14);
        src.aggregate(Some(&json!("v1")));
        let hll_env = src.get();

        for order in [[&dhu, &hll_env], [&hll_env, &dhu]] {
            let mut agg = HllSketchAggregator::build(14);
            agg.aggregate(Some(order[0]));
            agg.aggregate(Some(order[1]));
            assert_loud(&agg.get(), "non-empty hll × non-empty dhu");
            assert!(agg.estimate().is_nan());
        }
    }

    /// The dyn-merge fallback (a FOREIGN aggregator with no typed
    /// downcast whose `get` emits a native `hll` envelope) follows the
    /// same classification: empty = identity even against an adopted
    /// accumulator, non-empty = loud no-mix, corrupt = loud corrupt.
    #[test]
    fn hll_aggregator_dyn_merge_fallback_classifies_hll_envelope() {
        /// Minimal foreign aggregator replaying a fixed envelope.
        #[derive(Debug, Clone)]
        struct FixedEnvelope(serde_json::Value);
        impl Aggregator for FixedEnvelope {
            fn aggregate(&mut self, _value: Option<&serde_json::Value>) {}
            fn get(&self) -> serde_json::Value {
                self.0.clone()
            }
            fn merge(&mut self, _other: &dyn Aggregator) {}
            fn reset(&mut self) {}
            fn clone_box(&self) -> Box<dyn Aggregator> {
                Box::new(self.clone())
            }
        }

        let a = hyper_unique(1, &[(10, 0x02)]);

        // Empty foreign hll partial into an adopted accumulator: identity.
        let mut adopted = HllSketchAggregator::build(14);
        adopted.aggregate(Some(&druid_hyper_unique_envelope(&a)));
        adopted.merge(&FixedEnvelope(HllSketchAggregator::build(14).get()));
        assert_eq!(
            adopted
                .get()
                .get("@sketch")
                .and_then(serde_json::Value::as_str),
            Some(DRUID_HYPER_UNIQUE_TAG),
            "empty foreign hll partial must be identity, got {}",
            adopted.get()
        );
        assert_eq!(adopted.estimate().to_bits(), a.estimate().to_bits());

        // Non-empty foreign hll partial into an adopted accumulator:
        // genuine cross-species mix, loud.
        let mut src = HllSketchAggregator::build(14);
        src.aggregate(Some(&json!("v1")));
        adopted.merge(&FixedEnvelope(src.get()));
        assert_loud(&adopted.get(), "non-empty foreign hll × adopted");

        // Corrupt foreign hll partial: loud corrupt poison (R5).
        let mut fresh = HllSketchAggregator::build(14);
        fresh.merge(&FixedEnvelope(corrupt_envelope(HLL_SKETCH_TAG)));
        assert_loud(&fresh.get(), "corrupt foreign hll envelope");
    }

    /// R6 at the broker-JSON layer: an EMPTY valid partial of EITHER
    /// species is the identity in BOTH directions of
    /// [`merge_sketch_json`]; two NON-empty species still merge loud.
    /// Pre-fix, `non-empty hll ⊕ empty dhu` (and the reverse direction)
    /// merged into the mix error — the no-mix arms keyed on species, not
    /// emptiness.
    #[test]
    fn merge_sketch_json_empty_species_is_identity_both_orders() {
        let a = hyper_unique(1, &[(10, 0x02)]);
        let ea = druid_hyper_unique_envelope(&a);
        let empty_dhu = druid_hyper_unique_envelope(&DruidHyperUnique::empty());
        let empty_hll = HllSketchAggregator::build(14).get();
        let mut src = HllSketchAggregator::build(14);
        src.aggregate(Some(&json!("v1")));
        let hll_env = src.get();

        // empty dhu is identity around a non-empty native hll partial.
        assert_eq!(merge_sketch_json(&hll_env, &empty_dhu), hll_env);
        assert_eq!(merge_sketch_json(&empty_dhu, &hll_env), hll_env);
        // empty hll is identity around a non-empty decoded partial
        // (pinned; was already correct).
        assert_eq!(merge_sketch_json(&ea, &empty_hll), ea);
        assert_eq!(merge_sketch_json(&empty_hll, &ea), ea);
        // Two NON-empty species: still the loud no-mix error.
        assert_loud(&merge_sketch_json(&hll_env, &ea), "hll ⊕ dhu");
        assert_loud(&merge_sketch_json(&ea, &hll_env), "dhu ⊕ hll");
    }

    /// Broker-layer JSON merges: decoded envelopes fold; an EMPTY native
    /// HLL side is neutral (adopts / keeps); a NON-empty native side is
    /// the loud mix error; an error envelope absorbs every merge.
    #[test]
    fn merge_sketch_json_druid_hyper_unique_rules() {
        let a = hyper_unique(1, &[(10, 0x02)]);
        let b = hyper_unique(1, &[(20, 0x30)]);
        let ea = druid_hyper_unique_envelope(&a);
        let eb = druid_hyper_unique_envelope(&b);

        // dhu + dhu → folded.
        let merged = merge_sketch_json(&ea, &eb);
        assert_eq!(
            merged.get("estimate").and_then(serde_json::Value::as_f64),
            Some(a.merged(&b).estimate())
        );

        // empty-hll + dhu → adopt (the v1.1.1 broker-fold class: a shard
        // with no rows in the bucket must stay neutral).
        let empty_hll = HllSketchAggregator::build(14).get();
        assert_eq!(merge_sketch_json(&empty_hll, &ea), ea);
        // dhu + empty-hll → keep.
        assert_eq!(merge_sketch_json(&ea, &empty_hll), ea);

        // non-empty hll + dhu (either direction) → LOUD error.
        let mut native = HllSketchAggregator::build(14);
        native.aggregate(Some(&json!("v1")));
        let native_env = native.get();
        for (dst, src) in [(&native_env, &ea), (&ea, &native_env)] {
            let merged = merge_sketch_json(dst, src);
            assert_eq!(
                merged.get("@sketch").and_then(serde_json::Value::as_str),
                Some(SKETCH_MIX_ERROR_TAG),
                "non-empty native × decoded must be loud, got {merged}"
            );
        }

        // The error envelope absorbs both directions.
        let err = merge_sketch_json(&native_env, &ea);
        assert_eq!(merge_sketch_json(&err, &ea), err);
        assert_eq!(merge_sketch_json(&ea, &err), err);
    }

    /// The v1.1.1 broker-fold bug class, JSON-`null` flavor: timeseries
    /// executors emit JSON `null` for synthetic empty buckets, and the
    /// broker folds shard partials in ARRIVAL ORDER through
    /// [`crate::merge_json_by_spec`].  A `null` partial is the
    /// empty-sketch IDENTITY — it must never override a real envelope on
    /// either side (pre-fix, a `null` dst carried no `@sketch` tag, hit
    /// the fallback arm, and silently DISCARDED the src sketch, so the
    /// bucket finalized to 0 / SQL NULL whenever the empty shard arrived
    /// first — shard-order-dependent results).  Both arrival orders of a
    /// multi-shard fold must finalize to the same oracle estimate.
    #[test]
    fn merge_sketch_json_null_partial_is_identity_both_orders() {
        let null = serde_json::Value::Null;

        // --- druidHyperUnique: shards [null, A, B] vs [B, A, null] ---
        let a = hyper_unique(1, &[(10, 0x02)]);
        let b = hyper_unique(2, &[(10, 0x01), (20, 0x30)]);
        let ea = druid_hyper_unique_envelope(&a);
        let eb = druid_hyper_unique_envelope(&b);
        let hu_oracle = a.merged(&b).estimate();
        let hu_spec = hyper_unique_spec(false);
        for shards in [[&null, &ea, &eb], [&eb, &ea, &null]] {
            let mut acc = shards[0].clone();
            for src in &shards[1..] {
                acc = crate::merge_json_by_spec(&hu_spec, &acc, src);
            }
            let fin = finalize_sketch_json(&hu_spec, &acc)
                .and_then(|v| v.as_f64())
                .expect("finalized hyperUnique estimate");
            assert_eq!(
                fin.to_bits(),
                hu_oracle.to_bits(),
                "hyperUnique fold order {shards:?} finalized to {fin}, oracle {hu_oracle}"
            );
        }

        // --- thetaSketch: same multi-shard shape, both orders ---
        let theta_spec: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "thetaSketch", "name": "tt", "fieldName": "user"
        }))
        .expect("theta spec");
        let mut ta = ThetaSketchAggregator::new(4096);
        for i in 0_u32..100 {
            ta.aggregate(Some(&json!(i)));
        }
        let mut tb = ThetaSketchAggregator::new(4096);
        for i in 50_u32..150 {
            tb.aggregate(Some(&json!(i)));
        }
        let eta = ta.get();
        let etb = tb.get();
        // Typed-merge oracle: 0..150 distinct, exact below the retention cap.
        let mut oracle_agg = ta.clone();
        oracle_agg.merge(&tb);
        let theta_oracle = oracle_agg.estimate();
        assert!((theta_oracle - 150.0).abs() < f64::EPSILON);
        for shards in [[&null, &eta, &etb], [&etb, &eta, &null]] {
            let mut acc = shards[0].clone();
            for src in &shards[1..] {
                acc = crate::merge_json_by_spec(&theta_spec, &acc, src);
            }
            let fin = finalize_sketch_json(&theta_spec, &acc)
                .and_then(|v| v.as_f64())
                .expect("finalized theta estimate");
            assert_eq!(
                fin.to_bits(),
                theta_oracle.to_bits(),
                "theta fold order {shards:?} finalized to {fin}, oracle {theta_oracle}"
            );
        }

        // Pure identity: null ⊕ null stays null (an all-empty bucket still
        // finalizes to the empty-sketch scalar downstream).
        assert!(merge_sketch_json(&null, &null).is_null());

        // The strict guards are untouched by null-identity: a loud
        // mix-error envelope still absorbs a null on either side …
        let mut native = HllSketchAggregator::build(14);
        native.aggregate(Some(&json!("v1")));
        let err = merge_sketch_json(&native.get(), &ea);
        assert_eq!(
            err.get("@sketch").and_then(serde_json::Value::as_str),
            Some(SKETCH_MIX_ERROR_TAG)
        );
        assert_eq!(merge_sketch_json(&null, &err), err);
        assert_eq!(merge_sketch_json(&err, &null), err);
        // … and two DIFFERENT non-null species still fail loud (no-mix).
        let mixed = merge_sketch_json(&native.get(), &ea);
        assert_eq!(
            mixed.get("@sketch").and_then(serde_json::Value::as_str),
            Some(SKETCH_MIX_ERROR_TAG)
        );
    }

    // --- Native-wire finalization (P1-#3) ---

    fn hll_spec() -> crate::AggregatorSpec {
        serde_json::from_value(json!({
            "type": "HLLSketchBuild", "name": "uu", "fieldName": "user"
        }))
        .expect("hll spec")
    }

    #[test]
    fn finalize_hll_envelope_to_double_estimate() {
        let mut agg = HllSketchAggregator::build(14);
        for i in 0_u32..100 {
            agg.aggregate(Some(&json!(i)));
        }
        let fin = finalize_sketch_json(&hll_spec(), &agg.get()).expect("finalized");
        let est = fin.as_f64().expect("number");
        assert!(
            (est - 100.0).abs() / 100.0 < 0.05,
            "finalized HLL estimate {est} not within 5% of 100"
        );
        // Same number the HllSketchEstimate post-aggregator computes for
        // the same sketch bytes (SQL-path parity).
        assert!((est - agg.estimate()).abs() < f64::EPSILON);
    }

    #[test]
    fn finalize_theta_envelope_to_double_estimate() {
        let spec: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "thetaSketch", "name": "tt", "fieldName": "user"
        }))
        .expect("theta spec");
        let mut agg = ThetaSketchAggregator::new(4096);
        for i in 0_u32..8 {
            agg.aggregate(Some(&json!(i)));
        }
        let fin = finalize_sketch_json(&spec, &agg.get()).expect("finalized");
        // Below the retention cap the theta estimate is exact.
        assert_eq!(fin.as_f64(), Some(8.0));
    }

    #[test]
    fn finalize_quantiles_envelope_to_integer_count() {
        let spec: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "quantilesDoublesSketch", "name": "qq", "fieldName": "added"
        }))
        .expect("quantiles spec");
        let mut agg = QuantilesSketchAggregator::new(200);
        for i in 1..=10 {
            agg.aggregate(Some(&json!(i)));
        }
        // Druid 36 (measured) finalizes quantilesDoublesSketch to the value
        // count `n` as an INTEGER, not a quantile.
        assert_eq!(finalize_sketch_json(&spec, &agg.get()), Some(json!(10)));
    }

    #[test]
    fn finalize_null_empty_bucket_to_zero_scalars() {
        let null = serde_json::Value::Null;
        assert_eq!(
            finalize_sketch_json(&hll_spec(), &null),
            Some(json!(0.0)),
            "empty-bucket HLL finalizes to 0.0 (measured Druid 36)"
        );
        let qspec: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "quantilesDoublesSketch", "name": "qq", "fieldName": "added"
        }))
        .expect("quantiles spec");
        assert_eq!(finalize_sketch_json(&qspec, &null), Some(json!(0)));
    }

    #[test]
    fn finalize_recurses_through_filtered_wrapper() {
        let spec: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "filtered",
            "filter": {"type": "selector", "dimension": "d", "value": "x"},
            "aggregator": {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user"}
        }))
        .expect("filtered spec");
        let mut agg = HllSketchAggregator::build(14);
        agg.aggregate(Some(&json!("a")));
        let fin = finalize_sketch_json(&spec, &agg.get()).expect("finalized");
        assert!(fin.is_number(), "filtered-wrapped HLL must finalize");
    }

    #[test]
    fn finalize_leaves_non_sketch_specs_untouched() {
        // Non-sketch aggregators (count, bloomFilter, cardinality, …) are
        // not finalized here: count is already scalar, and Druid's
        // finalized bloom aggregation IS the filter itself.
        let count: crate::AggregatorSpec =
            serde_json::from_value(json!({"type": "count", "name": "c"})).expect("count spec");
        assert_eq!(finalize_sketch_json(&count, &json!(5)), None);
        assert_eq!(finalize_sketch_json(&count, &serde_json::Value::Null), None);

        let bloom: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "bloomFilter", "name": "bf", "fieldName": "user", "numEntries": 64
        }))
        .expect("bloom spec");
        let envelope = json!({"@sketch": crate::BLOOM_FILTER_TAG, "bytes": "AAAA"});
        assert_eq!(finalize_sketch_json(&bloom, &envelope), None);
    }

    /// Codex-review HIGH finding D: `"shouldFinalize": false` keeps the
    /// intermediate — `finalize_sketch_json` returns `None` so the caller
    /// leaves the envelope on the wire — including through a `filtered`
    /// wrapper; an explicit `true` (and absent, covered by the tests
    /// above) still finalizes.
    #[test]
    fn finalize_honors_per_aggregator_should_finalize_false() {
        let mut agg = HllSketchAggregator::build(14);
        agg.aggregate(Some(&json!("a")));
        let value = agg.get();

        let opted_out: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "HLLSketchBuild", "name": "uu", "fieldName": "user",
            "shouldFinalize": false
        }))
        .expect("hll spec");
        assert_eq!(opted_out.should_finalize(), Some(false));
        assert_eq!(finalize_sketch_json(&opted_out, &value), None);
        // The empty-bucket null ALSO stays untouched under the opt-out.
        assert_eq!(
            finalize_sketch_json(&opted_out, &serde_json::Value::Null),
            None
        );

        let filtered: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "filtered",
            "filter": {"type": "selector", "dimension": "d", "value": "x"},
            "aggregator": {"type": "HLLSketchBuild", "name": "uu", "fieldName": "user",
                           "shouldFinalize": false}
        }))
        .expect("filtered spec");
        assert_eq!(filtered.should_finalize(), Some(false));
        assert_eq!(finalize_sketch_json(&filtered, &value), None);

        let explicit_true: crate::AggregatorSpec = serde_json::from_value(json!({
            "type": "thetaSketch", "name": "tt", "fieldName": "user",
            "shouldFinalize": true
        }))
        .expect("theta spec");
        let mut theta = ThetaSketchAggregator::new(4096);
        theta.aggregate(Some(&json!("a")));
        assert!(
            finalize_sketch_json(&explicit_true, &theta.get()).is_some(),
            "shouldFinalize=true must finalize like the default"
        );
    }

    #[test]
    fn finalize_undecodable_envelope_is_fail_soft() {
        // A tag mismatch (theta envelope under an HLL spec) must not
        // fabricate an estimate.
        let theta_env = ThetaSketchAggregator::new(512).get();
        assert_eq!(finalize_sketch_json(&hll_spec(), &theta_env), None);
        // Garbage bytes under the right tag likewise.
        let bad = json!({"@sketch": "hll", "bytes": "!!!not-base64!!!"});
        assert_eq!(finalize_sketch_json(&hll_spec(), &bad), None);
    }

    // -----------------------------------------------------------------
    // COMPREHENSIVE state-matrix hardening: the uniform merge rule.
    //
    // Classify each side of a merge into Null | Corrupt | Empty |
    // NonEmpty(species); then, in precedence order:
    //   1. mixError on either side  → propagate (absorbing)
    //   2. Corrupt on either side   → LOUD corrupt poison, BEFORE any
    //      species/family matching (both arrival orders identical)
    //   3. Null on a side           → identity; both null → null
    //   4. Empty(any) on a side     → identity (adopt the other side);
    //      both empty → deterministic canonical empty
    //   5. Two NonEmpty SAME species → union (a DECODABLE-but-
    //      incompatible SAME-species union stays the documented
    //      fail-soft drop — the ONLY surviving fail-soft case)
    //   6. Two NonEmpty DIFFERENT species → LOUD mixError
    // -----------------------------------------------------------------

    /// The four in-scope species, in the implementation's FIXED priority
    /// order (deterministic tie-breaks for both-empty / both-corrupt).
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    enum MatrixSpecies {
        /// Native FNV HLL.
        Hll,
        /// Theta.
        Theta,
        /// Quantiles T-digest.
        Quantiles,
        /// Decoded Druid hyperUnique.
        Dhu,
    }

    const ALL_MATRIX_SPECIES: [MatrixSpecies; 4] = [
        MatrixSpecies::Hll,
        MatrixSpecies::Theta,
        MatrixSpecies::Quantiles,
        MatrixSpecies::Dhu,
    ];

    fn matrix_tag(s: MatrixSpecies) -> &'static str {
        match s {
            MatrixSpecies::Hll => HLL_SKETCH_TAG,
            MatrixSpecies::Theta => THETA_SKETCH_TAG,
            MatrixSpecies::Quantiles => QUANTILES_SKETCH_TAG,
            MatrixSpecies::Dhu => DRUID_HYPER_UNIQUE_TAG,
        }
    }

    /// Empty-valid envelope with the DEFAULT parameters — the same shape
    /// the implementation's deterministic both-empty canonical uses.
    fn matrix_empty_env(s: MatrixSpecies) -> serde_json::Value {
        match s {
            MatrixSpecies::Hll => HllSketchAggregator::build(14).get(),
            MatrixSpecies::Theta => ThetaSketchAggregator::new(4096).get(),
            MatrixSpecies::Quantiles => QuantilesSketchAggregator::new(200).get(),
            MatrixSpecies::Dhu => druid_hyper_unique_envelope(&DruidHyperUnique::empty()),
        }
    }

    /// Non-empty envelope of each species (default parameters).
    fn matrix_non_empty_env(s: MatrixSpecies) -> serde_json::Value {
        match s {
            MatrixSpecies::Hll => {
                let mut a = HllSketchAggregator::build(14);
                for i in 0_u32..8 {
                    a.aggregate(Some(&json!(i)));
                }
                a.get()
            }
            MatrixSpecies::Theta => {
                let mut a = ThetaSketchAggregator::new(4096);
                for i in 0_u32..8 {
                    a.aggregate(Some(&json!(i)));
                }
                a.get()
            }
            MatrixSpecies::Quantiles => {
                let mut a = QuantilesSketchAggregator::new(200);
                for i in 1..=8 {
                    a.aggregate(Some(&json!(i)));
                }
                a.get()
            }
            MatrixSpecies::Dhu => {
                druid_hyper_unique_envelope(&hyper_unique(2, &[(10, 0x02), (20, 0x30)]))
            }
        }
    }

    /// One cell class of the state matrix.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum MatrixClass {
        Null,
        Empty(MatrixSpecies),
        NonEmpty(MatrixSpecies),
        Corrupt(MatrixSpecies),
    }

    /// All 13 matrix cells: JSON null plus {empty-valid, non-empty,
    /// corrupt} × the four species.
    fn matrix_cells() -> Vec<(MatrixClass, serde_json::Value)> {
        let mut cells = vec![(MatrixClass::Null, serde_json::Value::Null)];
        for s in ALL_MATRIX_SPECIES {
            cells.push((MatrixClass::Empty(s), matrix_empty_env(s)));
            cells.push((MatrixClass::NonEmpty(s), matrix_non_empty_env(s)));
            cells.push((MatrixClass::Corrupt(s), corrupt_envelope(matrix_tag(s))));
        }
        cells
    }

    /// The rule oracle's outcome for one ordered `(dst, src)` pair.
    enum MatrixOutcome {
        /// The merge must return exactly this JSON value.
        Exactly(serde_json::Value),
        /// The merge must return the LOUD mix-error envelope.
        Loud,
        /// The merge must return a same-species union envelope.
        Union(MatrixSpecies),
    }

    /// The uniform rule, restated independently as the test oracle.
    fn matrix_expected(
        dst: (MatrixClass, &serde_json::Value),
        src: (MatrixClass, &serde_json::Value),
    ) -> MatrixOutcome {
        use MatrixClass as MC;
        match (dst.0, src.0) {
            // Rule 2: corrupt is loud BEFORE species/null/empty handling.
            (MC::Corrupt(_), _) | (_, MC::Corrupt(_)) => MatrixOutcome::Loud,
            // Rule 3: null identity.
            (MC::Null, _) => MatrixOutcome::Exactly(src.1.clone()),
            (_, MC::Null) => MatrixOutcome::Exactly(dst.1.clone()),
            // Rule 4: empty identity; both-empty canonicalizes on the
            // priority species.
            (MC::Empty(a), MC::Empty(b)) => MatrixOutcome::Exactly(matrix_empty_env(a.min(b))),
            (MC::Empty(_), _) => MatrixOutcome::Exactly(src.1.clone()),
            (_, MC::Empty(_)) => MatrixOutcome::Exactly(dst.1.clone()),
            // Rules 5/6: non-empty × non-empty.
            (MC::NonEmpty(a), MC::NonEmpty(b)) if a == b => MatrixOutcome::Union(a),
            (MC::NonEmpty(_), MC::NonEmpty(_)) => MatrixOutcome::Loud,
        }
    }

    /// The EXHAUSTIVE state matrix over the broker-side
    /// [`merge_sketch_json`]: every ordered pair of the 13 cells (169
    /// pairs), asserting the uniform rule's outcome AND order-independence.
    /// RED cells pre-fix: `empty theta ⊕ non-empty quantiles` silently
    /// dropped the quantiles side; `valid ⊕ cross-family-corrupt` cleared
    /// the corruption (or retained it, order-dependently); two non-empty
    /// DIFFERENT species kept `dst` instead of failing loud.
    #[test]
    fn merge_sketch_json_state_matrix_uniform_and_order_independent() {
        let cells = matrix_cells();
        for (dc, dv) in &cells {
            for (sc, sv) in &cells {
                let ctx = format!("{dc:?} ⊕ {sc:?}");
                let out = merge_sketch_json(dv, sv);
                let rev = merge_sketch_json(sv, dv);
                match matrix_expected((*dc, dv), (*sc, sv)) {
                    MatrixOutcome::Exactly(expected) => {
                        assert_eq!(out, expected, "{ctx}: wrong merge result");
                        let MatrixOutcome::Exactly(rev_expected) =
                            matrix_expected((*sc, sv), (*dc, dv))
                        else {
                            panic!("{ctx}: oracle must be class-symmetric");
                        };
                        assert_eq!(rev, rev_expected, "{ctx}: wrong reverse-order result");
                        assert_eq!(out, rev, "{ctx}: order-dependent result");
                    }
                    MatrixOutcome::Loud => {
                        assert_loud(&out, &ctx);
                        assert_loud(&rev, &format!("{ctx} (reverse)"));
                        assert_eq!(out, rev, "{ctx}: loud result must be order-independent");
                    }
                    MatrixOutcome::Union(species) => {
                        for v in [&out, &rev] {
                            assert_eq!(
                                envelope_tag(v),
                                Some(matrix_tag(species)),
                                "{ctx}: union must stay a {species:?} envelope, got {v}"
                            );
                        }
                        if species == MatrixSpecies::Quantiles {
                            // T-digest centroid layout is arrival-order
                            // sensitive; the COUNT is exact and must match
                            // in both orders (self-merge doubles it).
                            let count = |v: &serde_json::Value| {
                                envelope_bytes(v, QUANTILES_SKETCH_TAG)
                                    .and_then(|b| TDigest::deserialize(&b).ok())
                                    .map(|d| d.count())
                            };
                            assert_eq!(count(&out), Some(16), "{ctx}: self-merge count");
                            assert_eq!(count(&out), count(&rev), "{ctx}: order-dependent count");
                        } else {
                            // HLL/theta/hyperUnique unions are commutative
                            // register/set folds: bit-identical both orders.
                            assert_eq!(out, rev, "{ctx}: order-dependent union");
                        }
                    }
                }
            }
        }
    }

    /// The SAME state matrix driven through every AGGREGATOR's `aggregate`
    /// path: all ordered two-cell feeds into all four aggregator species,
    /// asserting loudness per the uniform rule and order-independence.
    /// RED pre-fix: build-mode theta/HLL hashed foreign or corrupt
    /// envelopes as raw distinct values (phantom counts); quantiles
    /// silently skipped them; hyperUnique blanket-poisoned even EMPTY
    /// foreign envelopes.
    #[test]
    fn aggregator_state_matrix_uniform_and_order_independent() {
        /// Envelope species an aggregator accepts without a loud mix:
        /// its own, plus the documented W-A decoded-hyperUnique adoption
        /// into the HLL (`APPROX_COUNT_DISTINCT`) accumulator.
        fn accepts(agg: MatrixSpecies, env: MatrixSpecies) -> bool {
            agg == env || (agg == MatrixSpecies::Hll && env == MatrixSpecies::Dhu)
        }
        fn fresh(agg: MatrixSpecies) -> Box<dyn Aggregator> {
            match agg {
                MatrixSpecies::Hll => Box::new(HllSketchAggregator::build(14)),
                MatrixSpecies::Theta => Box::new(ThetaSketchAggregator::new(4096)),
                MatrixSpecies::Quantiles => Box::new(QuantilesSketchAggregator::new(200)),
                MatrixSpecies::Dhu => Box::new(HyperUniqueAggregator::new()),
            }
        }
        /// Whether feeding one cell poisons an `agg`-species accumulator.
        fn loud_trigger(agg: MatrixSpecies, class: MatrixClass) -> bool {
            match class {
                MatrixClass::Corrupt(_) => true,
                MatrixClass::NonEmpty(s) => !accepts(agg, s),
                MatrixClass::Null | MatrixClass::Empty(_) => false,
            }
        }

        let cells = matrix_cells();
        for agg_species in ALL_MATRIX_SPECIES {
            for (c1, v1) in &cells {
                for (c2, v2) in &cells {
                    let ctx = format!("{agg_species:?} aggregator ⟵ [{c1:?}, {c2:?}]");
                    let mut fwd = fresh(agg_species);
                    fwd.aggregate(Some(v1));
                    fwd.aggregate(Some(v2));
                    let mut bwd = fresh(agg_species);
                    bwd.aggregate(Some(v2));
                    bwd.aggregate(Some(v1));

                    let t1 = loud_trigger(agg_species, *c1);
                    let t2 = loud_trigger(agg_species, *c2);
                    // The W-A pair rule: non-empty native HLL and non-empty
                    // decoded hyperUnique are each individually acceptable
                    // to the HLL accumulator, but LOUD together.
                    let wa_pair = agg_species == MatrixSpecies::Hll
                        && matches!(
                            (*c1, *c2),
                            (
                                MatrixClass::NonEmpty(MatrixSpecies::Hll),
                                MatrixClass::NonEmpty(MatrixSpecies::Dhu)
                            ) | (
                                MatrixClass::NonEmpty(MatrixSpecies::Dhu),
                                MatrixClass::NonEmpty(MatrixSpecies::Hll)
                            )
                        );
                    if t1 || t2 || wa_pair {
                        assert_loud(&fwd.get(), &format!("{ctx} (fwd)"));
                        assert_loud(&bwd.get(), &format!("{ctx} (bwd)"));
                        if usize::from(t1) + usize::from(t2) <= 1 {
                            // A single trigger (or the W-A pair) must
                            // produce the IDENTICAL loud envelope in both
                            // arrival orders.  (Two independent triggers
                            // may surface either one's message first —
                            // both loud either way.)
                            assert_eq!(fwd.get(), bwd.get(), "{ctx}: order-dependent loud");
                        }
                    } else {
                        let fed_dhu = matches!(c1, MatrixClass::NonEmpty(MatrixSpecies::Dhu))
                            || matches!(c2, MatrixClass::NonEmpty(MatrixSpecies::Dhu));
                        let expected_tag = match agg_species {
                            MatrixSpecies::Hll if fed_dhu => DRUID_HYPER_UNIQUE_TAG,
                            s => matrix_tag(s),
                        };
                        for out in [fwd.get(), bwd.get()] {
                            assert_eq!(
                                envelope_tag(&out),
                                Some(expected_tag),
                                "{ctx}: wrong envelope, got {out}"
                            );
                        }
                        assert_eq!(fwd.get(), bwd.get(), "{ctx}: order-dependent result");
                    }
                }
            }
        }
    }

    /// Named RED cell: a CROSS-family corrupt envelope must never clear
    /// (or order-dependently retain) the poison — `valid theta ⊕ corrupt
    /// hll` is loud in BOTH orders, on the broker merge AND on every
    /// aggregator feed.
    #[test]
    fn cross_family_corrupt_never_clears_poison() {
        let mut t = ThetaSketchAggregator::new(4096);
        t.aggregate(Some(&json!("a")));
        let valid_theta = t.get();
        let corrupt_hll = corrupt_envelope(HLL_SKETCH_TAG);

        // Broker JSON merge, both orders.
        let vc = merge_sketch_json(&valid_theta, &corrupt_hll);
        let cv = merge_sketch_json(&corrupt_hll, &valid_theta);
        assert_loud(&vc, "valid theta ⊕ corrupt hll");
        assert_loud(&cv, "corrupt hll ⊕ valid theta");
        assert_eq!(vc, cv, "both orders must be identical");

        // Aggregator feed, both orders.
        for order in [[&valid_theta, &corrupt_hll], [&corrupt_hll, &valid_theta]] {
            let mut agg = ThetaSketchAggregator::new(4096);
            agg.aggregate(Some(order[0]));
            agg.aggregate(Some(order[1]));
            assert_loud(&agg.get(), "theta aggregator ⟵ cross-family corrupt");
            assert!(agg.estimate().is_nan());
        }
    }

    /// Named RED cell (the phantom-count species): a build-mode
    /// aggregator must NEVER hash a foreign or corrupt envelope's JSON
    /// text as a raw distinct value.
    #[test]
    fn build_aggregators_never_phantom_hash_foreign_envelopes() {
        // Pre-fix, this corrupt-hll envelope fell through theta's raw-add
        // path and the estimate gained a phantom +1 (2.0 instead of loud).
        let mut theta = ThetaSketchAggregator::new(4096);
        theta.aggregate(Some(&json!("a")));
        theta.aggregate(Some(&corrupt_envelope(HLL_SKETCH_TAG)));
        assert_loud(&theta.get(), "theta ⟵ corrupt foreign envelope");
        assert!(theta.estimate().is_nan(), "phantom-hash must not survive");

        // Pre-fix, a non-empty theta envelope was raw-hashed by the HLL
        // build aggregator (estimate 1.0 phantom instead of loud).
        let mut hll = HllSketchAggregator::build(14);
        hll.aggregate(Some(&matrix_non_empty_env(MatrixSpecies::Theta)));
        assert_loud(&hll.get(), "hll ⟵ non-empty theta envelope");
        assert!(hll.estimate().is_nan());

        // Pre-fix, quantiles silently skipped a non-empty cross-species
        // envelope (a silent shard drop) instead of failing loud.
        let mut q = QuantilesSketchAggregator::new(200);
        q.aggregate(Some(&matrix_non_empty_env(MatrixSpecies::Theta)));
        assert_loud(&q.get(), "quantiles ⟵ non-empty theta envelope");
        assert_eq!(q.quantile(0.5), None);
    }

    /// The merge-only hyperUnique aggregator follows the uniform rule too:
    /// an EMPTY valid envelope of a FOREIGN species is the identity (it
    /// adds no raw values, so the no-raw-add invariant holds), not the
    /// blanket foreign-input poison; corrupt and non-empty-cross stay loud.
    #[test]
    fn hyper_unique_empty_foreign_envelope_is_identity() {
        let a = hyper_unique(1, &[(10, 0x02)]);
        let mut agg = HyperUniqueAggregator::new();
        agg.aggregate(Some(&matrix_empty_env(MatrixSpecies::Hll)));
        agg.aggregate(Some(&druid_hyper_unique_envelope(&a)));
        agg.aggregate(Some(&matrix_empty_env(MatrixSpecies::Quantiles)));
        assert_eq!(
            agg.get().get("@sketch").and_then(serde_json::Value::as_str),
            Some(DRUID_HYPER_UNIQUE_TAG),
            "empty foreign envelopes must be identity, got {}",
            agg.get()
        );
        assert_eq!(agg.estimate().to_bits(), a.estimate().to_bits());

        // Corrupt foreign and non-empty foreign stay loud.
        let mut corrupt = HyperUniqueAggregator::new();
        corrupt.aggregate(Some(&corrupt_envelope(THETA_SKETCH_TAG)));
        assert_loud(&corrupt.get(), "hyperUnique ⟵ corrupt foreign");
        let mut cross = HyperUniqueAggregator::new();
        cross.aggregate(Some(&matrix_non_empty_env(MatrixSpecies::Hll)));
        assert_loud(&cross.get(), "hyperUnique ⟵ non-empty foreign");
    }

    /// PRESERVED fail-soft (the ONLY surviving one): a DECODABLE but
    /// parameter-incompatible SAME-species union — HLL precision mismatch
    /// — still drops the incompatible side without fabricating or
    /// poisoning, on both the broker merge and a NON-empty accumulator.
    /// (An EMPTY accumulator instead ADOPTS the partial — rule 4 — so an
    /// empty-side precision mismatch can no longer lose a whole shard.)
    #[test]
    fn hll_precision_mismatch_union_stays_fail_soft() {
        let mut p12 = HllSketchAggregator::build(12);
        p12.aggregate(Some(&json!("a")));
        let mut p14 = HllSketchAggregator::build(14);
        p14.aggregate(Some(&json!("b")));
        let (e12, e14) = (p12.get(), p14.get());

        // Broker merge: each direction keeps its own dst (documented
        // fail-soft; inherently order-dependent, exempt from the matrix).
        assert_eq!(merge_sketch_json(&e12, &e14), e12);
        assert_eq!(merge_sketch_json(&e14, &e12), e14);

        // NON-empty accumulator: the incompatible partial is dropped.
        let before = p12.estimate();
        p12.aggregate(Some(&e14));
        assert_eq!(p12.estimate().to_bits(), before.to_bits());
        assert_eq!(
            p12.get().get("@sketch").and_then(serde_json::Value::as_str),
            Some(HLL_SKETCH_TAG)
        );

        // EMPTY accumulator: adopts the partial wholesale (rule 4).
        let mut empty = HllSketchAggregator::build(12);
        empty.aggregate(Some(&e14));
        assert_eq!(empty.estimate().to_bits(), p14.estimate().to_bits());
    }
}
