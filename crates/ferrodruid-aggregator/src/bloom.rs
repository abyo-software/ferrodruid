// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `BLOOM_FILTER` aggregator + `BLOOM_FILTER_TEST` decoder (CL-4 / W1-H R4).
//!
//! ## Wire format ([`BLOOM_FILTER_TAG`])
//!
//! A FerroDruid bloom filter is encoded as a base64-encoded byte blob
//! with the following layout (little-endian, network order for the magic):
//!
//! ```text
//!   bytes  0..4    : magic = "FDBF" (FerroDruid Bloom Filter)
//!   bytes  4..8    : version (u32 LE) = 1
//!   bytes  8..12   : k (number of hash functions, u32 LE)
//!   bytes 12..20   : m (number of bits, u64 LE)
//!   bytes 20..N    : bit array, ceil(m / 8) bytes (LE bit packing)
//! ```
//!
//! ## Hashing
//!
//! We use SipHash-2-4 keyed twice (k0, k1) to derive a 128-bit hash from
//! the input value's stringified form.  The two 64-bit halves act as
//! `h1` and `h2` in the standard double-hashing scheme:
//!
//! ```text
//!   bit_position(i) = (h1 + i * h2) mod m
//! ```
//!
//! This matches the *idea* of Apache Hive's `BloomKFilter` (which uses
//! Murmur3 instead of SipHash) but is intentionally a different binary
//! format — strict byte-eq with Druid's Hive `BloomKFilter` is a residual
//! tracked in `docs/known-limitations.md` (CL-E1 R4-bytes).  FerroDruid
//! round-trip via `BLOOM_FILTER(x, n)` → `BLOOM_FILTER_TEST(x, base64)`
//! is byte-stable and conformance-tested below.
//!
//! ## Sizing
//!
//! Given `num_entries n` and a fixed false-positive rate `p = 0.01`:
//!
//! ```text
//!   m = -n * ln(p) / (ln(2)^2)  ≈ n * 9.5851
//!   k = (m / n) * ln(2)          ≈ 6.643
//! ```
//!
//! We clamp `n` to `[8, 1 << 26]`, `k` to `[2, 12]`, and round `m` up
//! to a multiple of 64 bits.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use base64::Engine as _;

use crate::Aggregator;

/// The wire envelope tag for a FerroDruid bloom filter.
pub const BLOOM_FILTER_TAG: &str = "ferrodruid-bloom-v1";

const MAGIC: [u8; 4] = *b"FDBF";
const VERSION: u32 = 1;
const MIN_ENTRIES: u64 = 8;
const MAX_ENTRIES: u64 = 1 << 26;
const FPP: f64 = 0.01;
const MIN_K: u32 = 2;
const MAX_K: u32 = 12;
const SIPHASH_KEY_A: u64 = 0x0123_4567_89ab_cdef;
const SIPHASH_KEY_B: u64 = 0xfedc_ba98_7654_3210;

// ---------------------------------------------------------------------------
// BloomFilter — the raw filter (decoded form)
// ---------------------------------------------------------------------------

/// A decoded bloom filter (in-memory bit array + parameters).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    /// Number of bits in the filter (multiple of 8).
    pub m: u64,
    /// Number of hash functions.
    pub k: u32,
    /// Bit array, little-endian bit ordering inside each byte.
    pub bits: Vec<u8>,
}

impl BloomFilter {
    /// Build a fresh empty filter sized for `num_entries` distinct elements.
    #[must_use]
    pub fn for_entries(num_entries: u64) -> Self {
        let n = num_entries.clamp(MIN_ENTRIES, MAX_ENTRIES);
        // m_bits ≈ n * 9.5851; round up to a multiple of 8.
        #[allow(clippy::cast_precision_loss)]
        let n_f = n as f64;
        let m_bits_raw = -(n_f * FPP.ln()) / (std::f64::consts::LN_2 * std::f64::consts::LN_2);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let mut m_bits = m_bits_raw.ceil() as u64;
        if m_bits < 64 {
            m_bits = 64;
        }
        // Round up to multiple of 8.
        m_bits = (m_bits + 7) & !7;
        let m_bytes = (m_bits / 8) as usize;
        let k_raw = ((m_bits as f64 / n_f) * std::f64::consts::LN_2).round();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let k = (k_raw.max(MIN_K as f64) as u32).min(MAX_K);
        Self {
            m: m_bits,
            k,
            bits: vec![0u8; m_bytes],
        }
    }

    /// Add `value` to the filter.
    pub fn add(&mut self, value: &str) {
        let (h1, h2) = hash_pair(value);
        for i in 0..u64::from(self.k) {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % self.m;
            let byte = (bit / 8) as usize;
            let mask = 1u8 << (bit % 8);
            self.bits[byte] |= mask;
        }
    }

    /// Test whether `value` is (probably) in the filter.
    #[must_use]
    pub fn test(&self, value: &str) -> bool {
        if self.bits.is_empty() || self.m == 0 {
            return false;
        }
        let (h1, h2) = hash_pair(value);
        for i in 0..u64::from(self.k) {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % self.m;
            let byte = (bit / 8) as usize;
            let mask = 1u8 << (bit % 8);
            if self.bits[byte] & mask == 0 {
                return false;
            }
        }
        true
    }

    /// Bitwise-OR another filter into self (cross-shard merge).
    ///
    /// Filters must have identical `m` and `k`; otherwise `merge_in`
    /// silently no-ops (defensive — the caller would have to be mixing
    /// queries with different `num_entries` parameters).
    pub fn merge_in(&mut self, other: &BloomFilter) {
        if self.m != other.m || self.k != other.k || self.bits.len() != other.bits.len() {
            return;
        }
        for (dst, src) in self.bits.iter_mut().zip(other.bits.iter()) {
            *dst |= *src;
        }
    }

    /// Encode this filter into the wire-format byte blob.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(20 + self.bits.len());
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&self.k.to_le_bytes());
        out.extend_from_slice(&self.m.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Decode a filter from a wire-format byte blob.
    ///
    /// # Errors
    ///
    /// Returns `Err` with a human-readable reason when the magic is
    /// wrong, the version is unsupported, or the byte length disagrees
    /// with `m`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 20 {
            return Err(format!(
                "bloom filter blob too short: {} bytes",
                bytes.len()
            ));
        }
        if bytes[..4] != MAGIC {
            return Err(format!("bloom filter magic mismatch: {:?}", &bytes[..4]));
        }
        let version = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes"));
        if version != VERSION {
            return Err(format!("unsupported bloom filter version: {version}"));
        }
        let k = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes"));
        // Reject an out-of-range `k` on the deserialization path.  `test()`
        // and `add()` loop `0..k` once per probed value, so an
        // attacker-supplied envelope with a huge `k` (up to `u32::MAX`) turns a
        // single filtered query into effectively unbounded per-row CPU (a
        // query-amplification DoS).  The construction path (`for_entries`)
        // already clamps `k` into `MIN_K..=MAX_K`; enforce the identical bound
        // here so a hand-crafted blob cannot smuggle a larger value past it.
        if !(MIN_K..=MAX_K).contains(&k) {
            return Err(format!(
                "bloom filter k out of range: {k} (allowed {MIN_K}..={MAX_K})"
            ));
        }
        let m = u64::from_le_bytes(bytes[12..20].try_into().expect("8 bytes"));
        // `m` must be a non-zero multiple of 8.  `m == 0` makes `add()`
        // evaluate `% 0` (panic); a non-multiple-of-8 `m` lets the per-byte
        // index `bit / 8` reach `bits.len()` and panic out of bounds, because
        // the length check below uses floor division and would accept it.
        // Real filters always have `m` a multiple of 8 and `>= 64`
        // (`for_entries`), so this rejects only crafted blobs.
        if m == 0 || m % 8 != 0 {
            return Err(format!(
                "bloom filter m must be a non-zero multiple of 8: {m}"
            ));
        }
        // Compare lengths in u64: `(m / 8) as usize` could truncate on a 32-bit
        // target and let a crafted, oversized `m` pass a mismatched buffer
        // (then panic out of bounds later). The shipped AMIs are 64-bit, but
        // this keeps the check sound on any target.
        let expected_bytes = m / 8;
        if bytes.len() as u64 != 20 + expected_bytes {
            return Err(format!(
                "bloom filter length mismatch: header says m={m} ({expected_bytes} bytes), got {}",
                bytes.len() - 20
            ));
        }
        Ok(Self {
            m,
            k,
            bits: bytes[20..].to_vec(),
        })
    }
}

/// Base64-encode a bloom filter into its wire string form.
#[must_use]
pub fn encode_bloom_filter(filter: &BloomFilter) -> String {
    base64::engine::general_purpose::STANDARD.encode(filter.to_bytes())
}

/// Decode a base64-encoded bloom filter.
///
/// # Errors
///
/// Returns `Err` when base64 decoding fails or the byte blob is malformed.
pub fn decode_bloom_filter(b64: &str) -> Result<BloomFilter, String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| format!("base64 decode failed: {e}"))?;
    BloomFilter::from_bytes(&bytes)
}

// Two SipHash-2-4 instances seeded with distinct keys give us `h1`, `h2`
// for double hashing.  `DefaultHasher` is SipHash-2-4 in std.
fn hash_pair(value: &str) -> (u64, u64) {
    let mut s1 = DefaultHasher::new();
    SIPHASH_KEY_A.hash(&mut s1);
    value.hash(&mut s1);
    let mut s2 = DefaultHasher::new();
    SIPHASH_KEY_B.hash(&mut s2);
    value.hash(&mut s2);
    let h1 = s1.finish();
    let h2 = s2.finish();
    // Avoid h2 == 0 (would collapse all k positions to h1).
    let h2 = if h2 == 0 { 0x9E37_79B9_7F4A_7C15 } else { h2 };
    (h1, h2)
}

// ---------------------------------------------------------------------------
// BloomFilterAggregator
// ---------------------------------------------------------------------------

/// `BLOOM_FILTER` aggregator — accumulates input values into a bloom filter
/// and emits the base64-encoded wire envelope.
#[derive(Debug, Clone)]
pub struct BloomFilterAggregator {
    filter: BloomFilter,
}

impl BloomFilterAggregator {
    /// Construct an aggregator sized for the given expected entry count.
    #[must_use]
    pub fn new(num_entries: u64) -> Self {
        Self {
            filter: BloomFilter::for_entries(num_entries),
        }
    }
}

impl Aggregator for BloomFilterAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let Some(v) = value else { return };
        if v.is_null() {
            return;
        }
        let s = bloom_input_str(v);
        self.filter.add(&s);
    }

    fn get(&self) -> serde_json::Value {
        // Wire shape: a JSON object envelope so it interoperates with the
        // sketch / variance partial-state convention (`@sketch` tag).
        serde_json::json!({
            "@sketch": BLOOM_FILTER_TAG,
            "bytes": encode_bloom_filter(&self.filter),
        })
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        let other_val = other.get();
        let Some(obj) = other_val.as_object() else {
            return;
        };
        if obj.get("@sketch").and_then(serde_json::Value::as_str) != Some(BLOOM_FILTER_TAG) {
            return;
        }
        if let Some(b64) = obj.get("bytes").and_then(serde_json::Value::as_str)
            && let Ok(other_filter) = decode_bloom_filter(b64)
        {
            self.filter.merge_in(&other_filter);
        }
    }

    fn reset(&mut self) {
        let m_bytes = self.filter.bits.len();
        self.filter.bits.clear();
        self.filter.bits.resize(m_bytes, 0);
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }
}

fn bloom_input_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Cross-shard JSON merge for `BLOOM_FILTER`.
#[must_use]
pub fn merge_bloom_json(dst: &serde_json::Value, src: &serde_json::Value) -> serde_json::Value {
    let dst_bytes = extract_bloom_b64(dst);
    let src_bytes = extract_bloom_b64(src);
    match (dst_bytes, src_bytes) {
        (Some(d), Some(s)) => match (decode_bloom_filter(&d), decode_bloom_filter(&s)) {
            (Ok(mut df), Ok(sf)) => {
                df.merge_in(&sf);
                serde_json::json!({
                    "@sketch": BLOOM_FILTER_TAG,
                    "bytes": encode_bloom_filter(&df),
                })
            }
            _ => dst.clone(),
        },
        (Some(_), None) => dst.clone(),
        (None, Some(_)) => src.clone(),
        _ => dst.clone(),
    }
}

fn extract_bloom_b64(v: &serde_json::Value) -> Option<String> {
    let obj = v.as_object()?;
    if obj.get("@sketch").and_then(serde_json::Value::as_str)? != BLOOM_FILTER_TAG {
        return None;
    }
    obj.get("bytes")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bloom_filter_basic_add_then_test() {
        let mut filter = BloomFilter::for_entries(1000);
        filter.add("alpha");
        filter.add("bravo");
        assert!(filter.test("alpha"));
        assert!(filter.test("bravo"));
        assert!(!filter.test("absent-zzz-123"));
    }

    /// Edge case: never-added value must (probably) report absent.  We
    /// pick a value far enough from the inserted set that the test is
    /// statistically reliable at the configured FPP=0.01.
    #[test]
    fn bloom_filter_absent_value_reports_false() {
        let mut filter = BloomFilter::for_entries(100);
        for i in 0..10 {
            filter.add(&format!("k{i}"));
        }
        // Try several never-added values; at least most must report
        // false (FPP=0.01 means ~1% may collide).
        let mut hits = 0;
        for i in 0..50 {
            if filter.test(&format!("absent-{i}-zzz")) {
                hits += 1;
            }
        }
        assert!(hits < 5, "false positive count too high: {hits}/50");
    }

    #[test]
    fn bloom_filter_round_trip_base64_preserves_membership() {
        let mut filter = BloomFilter::for_entries(500);
        for i in 0..100 {
            filter.add(&format!("user-{i}"));
        }
        let b64 = encode_bloom_filter(&filter);
        let decoded = decode_bloom_filter(&b64).expect("decode");
        assert_eq!(filter, decoded);
        for i in 0..100 {
            assert!(decoded.test(&format!("user-{i}")));
        }
    }

    #[test]
    fn bloom_filter_aggregator_accumulates_and_round_trips() {
        let mut agg = BloomFilterAggregator::new(1000);
        for i in 0..50 {
            agg.aggregate(Some(&json!(format!("page-{i}"))));
        }
        let envelope = agg.get();
        let b64 = envelope
            .get("bytes")
            .and_then(serde_json::Value::as_str)
            .expect("bytes");
        let decoded = decode_bloom_filter(b64).expect("decode");
        for i in 0..50 {
            assert!(decoded.test(&format!("page-{i}")));
        }
    }

    /// Edge case: null inputs must not corrupt the filter.
    #[test]
    fn bloom_filter_aggregator_skips_null_and_missing() {
        let mut agg = BloomFilterAggregator::new(100);
        agg.aggregate(None);
        agg.aggregate(Some(&serde_json::Value::Null));
        agg.aggregate(Some(&json!("kept")));
        let envelope = agg.get();
        let b64 = envelope
            .get("bytes")
            .and_then(serde_json::Value::as_str)
            .expect("bytes");
        let decoded = decode_bloom_filter(b64).expect("decode");
        assert!(decoded.test("kept"));
        // Inserting only `"kept"` — the empty string from null should not
        // appear (we skipped it).
        assert!(!decoded.test(""));
    }

    #[test]
    fn bloom_filter_merge_unions_bits() {
        let mut a = BloomFilterAggregator::new(500);
        let mut b = BloomFilterAggregator::new(500);
        for i in 0..30 {
            a.aggregate(Some(&json!(format!("a-{i}"))));
            b.aggregate(Some(&json!(format!("b-{i}"))));
        }
        a.merge(&b);
        let envelope = a.get();
        let b64 = envelope
            .get("bytes")
            .and_then(serde_json::Value::as_str)
            .expect("bytes");
        let decoded = decode_bloom_filter(b64).expect("decode");
        for i in 0..30 {
            assert!(decoded.test(&format!("a-{i}")));
            assert!(decoded.test(&format!("b-{i}")));
        }
    }

    #[test]
    fn merge_bloom_json_unions_envelopes() {
        let mut a = BloomFilterAggregator::new(200);
        let mut b = BloomFilterAggregator::new(200);
        a.aggregate(Some(&json!("a")));
        b.aggregate(Some(&json!("b")));
        let merged = merge_bloom_json(&a.get(), &b.get());
        let b64 = merged
            .get("bytes")
            .and_then(serde_json::Value::as_str)
            .expect("bytes");
        let decoded = decode_bloom_filter(b64).expect("decode");
        assert!(decoded.test("a"));
        assert!(decoded.test("b"));
    }

    #[test]
    fn bloom_filter_from_bytes_rejects_bad_magic() {
        let mut bytes = BloomFilter::for_entries(100).to_bytes();
        bytes[0] = b'X';
        assert!(BloomFilter::from_bytes(&bytes).is_err());
    }

    #[test]
    fn bloom_filter_for_entries_clamps_small_count() {
        let f = BloomFilter::for_entries(1);
        assert!(f.m >= 64);
        assert!(f.k >= MIN_K);
        assert!(f.k <= MAX_K);
    }

    /// Craft a raw wire envelope with explicit `k`, `m`, and body length so
    /// tests can exercise the `from_bytes` validation directly.
    fn craft_blob(k: u32, m: u64, body_len: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(20 + body_len);
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&k.to_le_bytes());
        bytes.extend_from_slice(&m.to_le_bytes());
        bytes.extend_from_slice(&vec![0xFFu8; body_len]);
        bytes
    }

    #[test]
    fn from_bytes_rejects_oversized_k_dos() {
        // The DoS vector: `k = u32::MAX` with an otherwise-valid m=64 body.
        // `test()` would loop ~4.3e9 times per probed row; `from_bytes` must
        // reject it before it is ever used.
        let blob = craft_blob(u32::MAX, 64, 8);
        let err = BloomFilter::from_bytes(&blob).expect_err("u32::MAX k must be rejected");
        assert!(err.contains("k out of range"), "unexpected error: {err}");
    }

    #[test]
    fn from_bytes_rejects_k_below_min() {
        let blob = craft_blob(1, 64, 8);
        assert!(BloomFilter::from_bytes(&blob).is_err());
        let blob0 = craft_blob(0, 64, 8);
        assert!(BloomFilter::from_bytes(&blob0).is_err());
    }

    #[test]
    fn from_bytes_rejects_zero_m() {
        // A 20-byte blob (no body) with m=0 would make `add()` divide by zero.
        let blob = craft_blob(4, 0, 0);
        let err = BloomFilter::from_bytes(&blob).expect_err("m=0 must be rejected");
        assert!(err.contains("multiple of 8"), "unexpected error: {err}");
    }

    #[test]
    fn from_bytes_rejects_non_multiple_of_8_m() {
        // m=63 passes the floor-division length check (63/8 == 7) but lets a
        // bit index of 56..=62 compute byte 7 == bits.len() → out-of-bounds
        // panic in `test()`.  `from_bytes` must reject it up front.
        let blob = craft_blob(4, 63, 7);
        let err = BloomFilter::from_bytes(&blob).expect_err("non-multiple-of-8 m must be rejected");
        assert!(err.contains("multiple of 8"), "unexpected error: {err}");
    }

    #[test]
    fn from_bytes_accepts_legitimate_filter_roundtrip() {
        // A genuinely constructed filter must still round-trip through
        // `from_bytes` unchanged after the new validation.
        let mut f = BloomFilter::for_entries(1000);
        f.add("alpha");
        f.add("bravo");
        let decoded = BloomFilter::from_bytes(&f.to_bytes()).expect("legit filter must decode");
        assert_eq!(decoded, f);
        assert!(decoded.test("alpha"));
        assert!(decoded.test("bravo"));
    }

    #[test]
    fn aggregator_get_is_envelope_tagged() {
        let agg = BloomFilterAggregator::new(100);
        let v = agg.get();
        assert_eq!(
            v.get("@sketch").and_then(serde_json::Value::as_str),
            Some(BLOOM_FILTER_TAG)
        );
    }
}
