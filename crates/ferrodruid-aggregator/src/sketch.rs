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

use ferrodruid_sketches::{HllSketch, TDigest, ThetaSketch};

use crate::Aggregator;

/// Default T-digest compression for the quantiles sketch aggregator.
const DEFAULT_QUANTILES_COMPRESSION: usize = 200;

/// JSON tag used to mark an HLL sketch partial-state envelope.
pub const HLL_SKETCH_TAG: &str = "hll";
/// JSON tag used to mark a Theta sketch partial-state envelope.
pub const THETA_SKETCH_TAG: &str = "theta";
/// JSON tag used to mark a quantiles (T-digest) sketch partial-state envelope.
pub const QUANTILES_SKETCH_TAG: &str = "quantiles";

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
// HLL sketch aggregator
// ---------------------------------------------------------------------------

/// Aggregator backing Druid's `HLLSketchBuild` and `HLLSketchMerge`.
///
/// In *build* mode each input value is hashed and added to the HLL registers.
/// In *merge* mode each input is expected to be a serialized HLL sketch (the
/// `bytes`/base64 partial-state form emitted by [`Aggregator::get`]) which is
/// deserialized and merged register-wise.  Both modes share the same
/// register-union merge for scatter/gather, so the distinction only affects
/// how raw rows are consumed.
#[derive(Debug, Clone)]
pub struct HllSketchAggregator {
    sketch: HllSketch,
    merge_mode: bool,
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
        }
    }

    /// Create a merge-mode HLL aggregator with the given `lg_k` precision.
    #[must_use]
    pub fn merge(lg_k: u8) -> Self {
        let mut s = Self::build(lg_k);
        s.merge_mode = true;
        s
    }

    /// Current cardinality estimate.
    #[must_use]
    pub fn estimate(&self) -> f64 {
        self.sketch.estimate()
    }

    /// Access the underlying sketch (for typed merge).
    #[must_use]
    pub fn sketch(&self) -> &HllSketch {
        &self.sketch
    }
}

impl Aggregator for HllSketchAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let Some(v) = value else { return };
        if v.is_null() {
            return;
        }
        if self.merge_mode {
            // Merge mode: the input is a serialized partial sketch.
            if let Some(bytes) = envelope_bytes(v, HLL_SKETCH_TAG)
                && let Ok(other) = HllSketch::deserialize(&bytes)
            {
                let _ = self.sketch.merge(&other);
            }
            return;
        }
        self.sketch.add_hash(mixed_hash(v));
    }

    fn get(&self) -> serde_json::Value {
        sketch_envelope(
            HLL_SKETCH_TAG,
            &self.sketch.serialize(),
            self.sketch.estimate(),
        )
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        if let Some(any) = other.as_any()
            && let Some(o) = any.downcast_ref::<HllSketchAggregator>()
        {
            let _ = self.sketch.merge(&o.sketch);
            return;
        }
        if let Some(bytes) = envelope_bytes(&other.get(), HLL_SKETCH_TAG)
            && let Ok(o) = HllSketch::deserialize(&bytes)
        {
            let _ = self.sketch.merge(&o);
        }
    }

    fn reset(&mut self) {
        let precision = self.sketch.precision();
        self.sketch = HllSketch::new(precision).unwrap_or_else(|_| HllSketch::default_precision());
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
#[derive(Debug, Clone)]
pub struct ThetaSketchAggregator {
    sketch: ThetaSketch,
}

impl ThetaSketchAggregator {
    /// Create a Theta sketch aggregator retaining up to `size` hashes.
    #[must_use]
    pub fn new(size: usize) -> Self {
        let size = if size == 0 { 4096 } else { size };
        Self {
            sketch: ThetaSketch::new(size),
        }
    }

    /// Current cardinality estimate.
    #[must_use]
    pub fn estimate(&self) -> f64 {
        self.sketch.estimate()
    }

    /// Access the underlying sketch (for typed merge / set ops).
    #[must_use]
    pub fn sketch(&self) -> &ThetaSketch {
        &self.sketch
    }
}

impl Aggregator for ThetaSketchAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let Some(v) = value else { return };
        if v.is_null() {
            return;
        }
        // If the input is itself a serialized theta sketch (merge feed), union
        // it; otherwise hash the raw value.  Both refusals below are the
        // documented hash-space guard (a Druid-origin sketch is union-only
        // with other SAME-SEED Druid-origin sketches): the incompatible
        // input — cross-space OR cross-seed — is DROPPED fail-soft,
        // mirroring the HLL `merge`-error drop, rather than silently
        // corrupting the estimate by mixing FNV-space and MurmurHash3-space
        // (or differently-seeded MurmurHash3) hashes.
        if let Some(bytes) = envelope_bytes(v, THETA_SKETCH_TAG)
            && let Ok(other) = ThetaSketch::deserialize(&bytes)
        {
            if let Ok(u) = self.sketch.union(&other) {
                self.sketch = u;
            }
            return;
        }
        let _ = self.sketch.add_hash(mixed_hash(v));
    }

    fn get(&self) -> serde_json::Value {
        sketch_envelope(
            THETA_SKETCH_TAG,
            &self.sketch.serialize(),
            self.sketch.estimate(),
        )
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        // Cross-hash-space unions are refused by the sketch (see
        // `aggregate`); the incompatible side is dropped fail-soft, the
        // same way an HLL precision-mismatch merge is.
        if let Some(any) = other.as_any()
            && let Some(o) = any.downcast_ref::<ThetaSketchAggregator>()
        {
            if let Ok(u) = self.sketch.union(&o.sketch) {
                self.sketch = u;
            }
            return;
        }
        if let Some(bytes) = envelope_bytes(&other.get(), THETA_SKETCH_TAG)
            && let Ok(o) = ThetaSketch::deserialize(&bytes)
            && let Ok(u) = self.sketch.union(&o)
        {
            self.sketch = u;
        }
    }

    fn reset(&mut self) {
        self.sketch = ThetaSketch::default_size();
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
#[derive(Debug, Clone)]
pub struct QuantilesSketchAggregator {
    digest: TDigest,
    compression: usize,
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
        }
    }

    /// Estimate the value at quantile `q` (`q` in `[0, 1]`).  Returns `None`
    /// when the digest is empty or `q` is out of range.
    #[must_use]
    pub fn quantile(&self, q: f64) -> Option<f64> {
        self.digest.quantile(q).ok()
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
}

impl Aggregator for QuantilesSketchAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let Some(v) = value else { return };
        // Merge feed: a serialized quantiles sketch.
        if let Some(bytes) = envelope_bytes(v, QUANTILES_SKETCH_TAG)
            && let Ok(other) = TDigest::deserialize(&bytes)
        {
            self.digest.merge(&other);
            return;
        }
        if let Some(f) = v.as_f64() {
            self.digest.add(f);
        }
    }

    fn get(&self) -> serde_json::Value {
        // The convenience `estimate` field carries the median (p50) for
        // quantiles sketches; consumers that want a specific quantile use the
        // post-aggregator path on the reconstructed digest.
        let median = self.digest.quantile(0.5).unwrap_or(f64::NAN);
        sketch_envelope(QUANTILES_SKETCH_TAG, &self.digest.serialize(), median)
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        if let Some(any) = other.as_any()
            && let Some(o) = any.downcast_ref::<QuantilesSketchAggregator>()
        {
            self.digest.merge(&o.digest);
            return;
        }
        if let Some(bytes) = envelope_bytes(&other.get(), QUANTILES_SKETCH_TAG)
            && let Ok(o) = TDigest::deserialize(&bytes)
        {
            self.digest.merge(&o);
        }
    }

    fn reset(&mut self) {
        self.digest = TDigest::new(self.compression);
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
        crate::AggregatorSpec::Filtered { aggregator, .. } => {
            finalize_sketch_json(aggregator, value)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Broker-side JSON merge for sketch partial states
// ---------------------------------------------------------------------------

/// Merge two sketch partial-state JSON envelopes, dispatching on the
/// `@sketch` tag.  Both sides must carry the same tag; on any mismatch or
/// decode failure the `dst` side is returned unchanged (fail-soft — never
/// fabricates an estimate).
#[must_use]
pub fn merge_sketch_json(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    let tag = dst
        .as_object()
        .and_then(|o| o.get("@sketch"))
        .and_then(serde_json::Value::as_str);
    match tag {
        Some(HLL_SKETCH_TAG) => merge_hll_json(dst, src),
        Some(THETA_SKETCH_TAG) => merge_theta_json(dst, src),
        Some(QUANTILES_SKETCH_TAG) => merge_quantiles_json(dst, src),
        _ => dst.clone(),
    }
}

fn merge_hll_json(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    let (Some(db), Some(sb)) = (
        envelope_bytes(dst, HLL_SKETCH_TAG),
        envelope_bytes(src, HLL_SKETCH_TAG),
    ) else {
        return dst.clone();
    };
    let (Ok(mut d), Ok(s)) = (HllSketch::deserialize(&db), HllSketch::deserialize(&sb)) else {
        return dst.clone();
    };
    if d.merge(&s).is_err() {
        return dst.clone();
    }
    sketch_envelope(HLL_SKETCH_TAG, &d.serialize(), d.estimate())
}

fn merge_theta_json(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    let (Some(db), Some(sb)) = (
        envelope_bytes(dst, THETA_SKETCH_TAG),
        envelope_bytes(src, THETA_SKETCH_TAG),
    ) else {
        return dst.clone();
    };
    let (Ok(d), Ok(s)) = (ThetaSketch::deserialize(&db), ThetaSketch::deserialize(&sb)) else {
        return dst.clone();
    };
    // A cross-hash-space union (native FNV vs Druid MurmurHash3 origin) or
    // a cross-seed union (Druid-origin sketches decoded with different
    // update seeds) is refused by the sketch; keep `dst` unchanged
    // (fail-soft — never fabricates an estimate from mixed hash spaces).
    let Ok(u) = d.union(&s) else {
        return dst.clone();
    };
    sketch_envelope(THETA_SKETCH_TAG, &u.serialize(), u.estimate())
}

fn merge_quantiles_json(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    let (Some(db), Some(sb)) = (
        envelope_bytes(dst, QUANTILES_SKETCH_TAG),
        envelope_bytes(src, QUANTILES_SKETCH_TAG),
    ) else {
        return dst.clone();
    };
    let (Ok(mut d), Ok(s)) = (TDigest::deserialize(&db), TDigest::deserialize(&sb)) else {
        return dst.clone();
    };
    d.merge(&s);
    let median = d.quantile(0.5).unwrap_or(f64::NAN);
    sketch_envelope(QUANTILES_SKETCH_TAG, &d.serialize(), median)
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

    #[test]
    fn envelope_tag_mismatch_is_fail_soft() {
        let hll = HllSketchAggregator::build(12).get();
        let theta = ThetaSketchAggregator::new(512).get();
        // Merging across mismatched tags returns dst unchanged.
        let merged = merge_sketch_json(&hll, &theta);
        assert_eq!(merged, hll);
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
}
