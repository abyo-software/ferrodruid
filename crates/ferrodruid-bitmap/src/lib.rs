// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Roaring bitmap with Druid-compatible binary serialization.
//!
//! Druid segments store bitmaps using a one-byte type tag followed by the
//! bitmap payload.  Type `0x00` is the legacy Concise format (converted to
//! Roaring on read); type `0x01` is the standard Roaring portable
//! serialization.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use ferrodruid_common::error::{DruidError, Result};
use roaring::RoaringBitmap;

// ---------------------------------------------------------------------------
// Bitmap type tags used in the Druid segment wire format
// ---------------------------------------------------------------------------

/// Legacy Concise bitmap tag (read-only; we always write Roaring).
const BITMAP_TYPE_CONCISE: u8 = 0x00;
/// Roaring bitmap tag.
const BITMAP_TYPE_ROARING: u8 = 0x01;

// ---------------------------------------------------------------------------
// Bounded-deserialization limits (untrusted segment input)
// ---------------------------------------------------------------------------

/// Hard cap on a single serialized bitmap accepted by the bounded
/// deserializers (64 MiB — mirrors the segment reader's per-bitmap cap;
/// a legitimate row bitmap never comes close).
const MAX_SERIALIZED_BITMAP_BYTES: usize = 64 * 1024 * 1024;

/// Portable-format cookie announcing a payload that may contain run
/// containers (public Roaring interoperable-serialization spec,
/// <https://github.com/RoaringBitmap/RoaringFormatSpec>). The high 16 bits
/// of the cookie word store `container_count - 1`.
const SERIAL_COOKIE: u32 = 12_347;

/// Portable-format cookie announcing a payload without run containers;
/// the following 32-bit little-endian word is the container count.
const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12_346;

/// Number of consecutive row ids covered by one Roaring container (the
/// low 16 bits of the value space).
const CONTAINER_SPAN: u64 = 1 << 16;

/// Read the container count a portable-Roaring payload declares from its
/// header, without deserializing any container.
fn declared_portable_containers(data: &[u8]) -> Result<u64> {
    if data.len() < 4 {
        return Err(DruidError::Segment(format!(
            "portable roaring bitmap truncated: {} bytes, need at least the 4-byte cookie",
            data.len()
        )));
    }
    let cookie = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if (cookie & 0xFFFF) == SERIAL_COOKIE {
        Ok(u64::from(cookie >> 16) + 1)
    } else if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
        if data.len() < 8 {
            return Err(DruidError::Segment(
                "portable roaring bitmap truncated: missing the 4-byte container count".to_string(),
            ));
        }
        Ok(u64::from(u32::from_le_bytes([
            data[4], data[5], data[6], data[7],
        ])))
    } else {
        Err(DruidError::Segment(format!(
            "portable roaring bitmap has unrecognized cookie {cookie:#010x}"
        )))
    }
}

/// A Druid-compatible bitmap that wraps [`RoaringBitmap`].
#[derive(Clone, Debug, PartialEq)]
pub struct DruidBitmap {
    inner: RoaringBitmap,
}

impl Default for DruidBitmap {
    fn default() -> Self {
        Self::new()
    }
}

impl DruidBitmap {
    // -- construction -------------------------------------------------------

    /// Create an empty bitmap.
    pub fn new() -> Self {
        Self {
            inner: RoaringBitmap::new(),
        }
    }

    /// Wrap an existing [`RoaringBitmap`].
    pub fn from_roaring(bitmap: RoaringBitmap) -> Self {
        // Rebuild from live values so capacity retained by a caller that
        // inserted and later removed many containers cannot enter cache
        // accounting invisibly. All other constructors already create fresh
        // roaring storage.
        Self {
            inner: bitmap.iter().collect(),
        }
    }

    /// Wrap a `RoaringBitmap` that was JUST produced by a boolean operation.
    ///
    /// Roaring's `&`/`|`/`-`/complement build right-sized containers, so
    /// there is no stale outer capacity to discard — wrapping directly
    /// avoids the O(cardinality) rebuild that [`from_roaring`] performs for
    /// untrusted caller-owned bitmaps.
    ///
    /// [`from_roaring`]: DruidBitmap::from_roaring
    fn from_fresh_roaring(bitmap: RoaringBitmap) -> Self {
        Self { inner: bitmap }
    }

    /// Consume `self` and return the inner [`RoaringBitmap`].
    pub fn into_inner(self) -> RoaringBitmap {
        self.inner
    }

    /// Borrow the inner [`RoaringBitmap`].
    pub fn as_inner(&self) -> &RoaringBitmap {
        &self.inner
    }

    /// Conservatively estimate heap bytes retained by the roaring bitmap.
    ///
    /// Roaring's statistics report capacity-aware bytes for array, run, and
    /// bitset stores. An additional per-container allowance covers the outer
    /// container vector, enum objects, keys, and allocator slack that those
    /// statistics intentionally omit.
    #[must_use]
    pub fn estimated_heap_bytes(&self) -> u64 {
        let stats = self.inner.statistics();
        let stores = stats
            .n_bytes_array_containers
            .saturating_add(stats.n_bytes_run_containers)
            .saturating_add(stats.n_bytes_bitset_containers);
        // A roaring container is substantially smaller than 128 bytes before
        // its separately-counted store. Doubling the live container count
        // models Vec growth slack while retaining a conservative bound.
        let containers = u64::from(stats.n_containers)
            .saturating_mul(2)
            .saturating_mul(128);
        stores.saturating_add(containers)
    }

    // -- Druid segment serialization ----------------------------------------

    /// Serialize to the Druid bitmap wire format.
    ///
    /// Format: `[1 byte: 0x01 (roaring)] [portable Roaring bytes]`.
    pub fn serialize_druid(&self) -> Result<Vec<u8>> {
        let serial_len = self.inner.serialized_size();
        let mut buf = Vec::with_capacity(1 + serial_len);
        buf.push(BITMAP_TYPE_ROARING);
        self.inner
            .serialize_into(&mut buf)
            .map_err(|e| DruidError::Segment(format!("bitmap serialization failed: {e}")))?;
        Ok(buf)
    }

    /// Serialize to the bare portable Roaring format **without** the 1-byte
    /// Druid type tag.
    ///
    /// This is the exact inverse of [`Self::deserialize_portable`]: segments
    /// written in the upstream Apache Druid layout name the bitmap codec once
    /// in JSON metadata (`bitmapSerdeFactory`, e.g. `{"type":"roaring"}`)
    /// instead of tagging each serialized bitmap, so the per-bitmap payload
    /// is the standard Roaring portable serialization with no leading tag.
    /// [`Self::serialize_druid`] output is exactly `[0x01]` followed by this
    /// function's output.
    pub fn serialize_portable(&self) -> Result<Vec<u8>> {
        let serial_len = self.inner.serialized_size();
        let mut buf = Vec::with_capacity(serial_len);
        self.inner
            .serialize_into(&mut buf)
            .map_err(|e| DruidError::Segment(format!("bitmap serialization failed: {e}")))?;
        Ok(buf)
    }

    /// Deserialize from the Druid bitmap wire format.
    ///
    /// Accepts both Roaring (`0x01`) and Concise (`0x00`) type tags.  Concise
    /// bitmaps are *not* supported; encountering one returns an error.
    ///
    /// **Trusted input only**: this variant applies no resource bounds, so a
    /// malformed payload can allocate far more memory than its own size
    /// (e.g. run-container blow-up). For payloads read from untrusted
    /// segment files use [`Self::deserialize_druid_bounded`] (or
    /// [`Self::deserialize_portable`]) with the owning column's row count.
    pub fn deserialize_druid(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(DruidError::Segment("bitmap data is empty".to_string()));
        }
        match data[0] {
            BITMAP_TYPE_ROARING => {
                let inner = RoaringBitmap::deserialize_from(&data[1..]).map_err(|e| {
                    DruidError::Segment(format!("roaring deserialization failed: {e}"))
                })?;
                Ok(Self { inner })
            }
            BITMAP_TYPE_CONCISE => Err(DruidError::Segment(
                "concise bitmap decoding is not supported; convert to roaring first".to_string(),
            )),
            other => Err(DruidError::Segment(format!(
                "unknown bitmap type tag: 0x{other:02x}"
            ))),
        }
    }

    /// Deserialize a bare portable-format Roaring bitmap **without** the
    /// 1-byte Druid type tag, bounding resource use *before* any container
    /// is materialized.
    ///
    /// Segments written by upstream engines identify the bitmap codec once
    /// in JSON metadata (a `bitmapSerdeFactory` entry, observed as
    /// `{"type":"roaring"}` in real Druid 31.0.2 segments) rather than
    /// tagging each serialized bitmap; the per-bitmap payload is the
    /// standard Roaring portable serialization.
    ///
    /// `max_rows` is the exclusive upper bound on the row ids the caller
    /// can accept (typically the owning column's row count). Because every
    /// Roaring container spans a fixed 65 536-value range and container
    /// keys are distinct, a bitmap whose values are all `< max_rows` can
    /// declare at most `ceil(max_rows / 65536)` containers — the declared
    /// count is checked against that bound (and the payload length against
    /// a hard 64 MiB cap) **before** deserialization, so a sub-MiB
    /// malformed payload declaring 65 536 run containers fails closed
    /// instead of expanding to ~512 MiB of container stores mid-decode.
    ///
    /// The serialized payload must consume the **entire** slice: every
    /// caller passes an exact-length framed slice, so trailing bytes left
    /// over after deserialization mean the frame was mis-sized or the
    /// payload is malformed, and the call returns an error.
    pub fn deserialize_portable(data: &[u8], max_rows: u64) -> Result<Self> {
        Self::check_portable_bounds(data, max_rows)?;
        let mut cursor: &[u8] = data;
        let inner = RoaringBitmap::deserialize_from(&mut cursor).map_err(|e| {
            DruidError::Segment(format!("portable roaring deserialization failed: {e}"))
        })?;
        if !cursor.is_empty() {
            return Err(DruidError::Segment(format!(
                "portable roaring bitmap has {} trailing byte(s) after the serialized payload",
                cursor.len()
            )));
        }
        Ok(Self { inner })
    }

    /// Deserialize from the Druid bitmap wire format (like
    /// [`Self::deserialize_druid`]) with the same pre-deserialization
    /// resource bounds as [`Self::deserialize_portable`].
    ///
    /// Use this whenever the payload comes from an untrusted segment file
    /// and the owning column's row count is known.
    pub fn deserialize_druid_bounded(data: &[u8], max_rows: u64) -> Result<Self> {
        if data.is_empty() {
            return Err(DruidError::Segment("bitmap data is empty".to_string()));
        }
        match data[0] {
            BITMAP_TYPE_ROARING => Self::deserialize_portable(&data[1..], max_rows),
            BITMAP_TYPE_CONCISE => Err(DruidError::Segment(
                "concise bitmap decoding is not supported; convert to roaring first".to_string(),
            )),
            other => Err(DruidError::Segment(format!(
                "unknown bitmap type tag: 0x{other:02x}"
            ))),
        }
    }

    /// Validate the resource bounds of a portable-Roaring payload against
    /// `max_rows` without deserializing it. See
    /// [`Self::deserialize_portable`] for the rationale.
    fn check_portable_bounds(data: &[u8], max_rows: u64) -> Result<()> {
        if data.len() > MAX_SERIALIZED_BITMAP_BYTES {
            return Err(DruidError::Segment(format!(
                "serialized bitmap length {} exceeds cap {MAX_SERIALIZED_BITMAP_BYTES}",
                data.len()
            )));
        }
        let containers = declared_portable_containers(data)?;
        // A single (bitmap-type) container materializes up to 8 KiB (65536
        // bits). Independently of `max_rows`, cap the container count so the
        // worst-case DECOMPRESSED store stays within MAX_SERIALIZED_BITMAP_BYTES
        // — otherwise a `max_rows >= 2^32` caller would admit 65536 containers
        // (~512 MiB) from a sub-MiB payload. 64 MiB / 8 KiB = 8192.
        const MAX_CONTAINER_STORE_BYTES: u64 = 8 * 1024;
        let hard_max_containers = (MAX_SERIALIZED_BITMAP_BYTES as u64) / MAX_CONTAINER_STORE_BYTES;
        let max_containers = max_rows.div_ceil(CONTAINER_SPAN).min(hard_max_containers);
        if containers > max_containers {
            return Err(DruidError::Segment(format!(
                "bitmap declares {containers} containers, but at most {max_containers} are \
                 allowed (row ids below {max_rows}, each container spans {CONTAINER_SPAN} row \
                 ids, decompressed-store cap {hard_max_containers})"
            )));
        }
        Ok(())
    }

    // -- element operations -------------------------------------------------

    /// Insert a value. Returns `true` if the value was newly inserted.
    pub fn insert(&mut self, val: u32) -> bool {
        self.inner.insert(val)
    }

    /// Test membership.
    pub fn contains(&self, val: u32) -> bool {
        self.inner.contains(val)
    }

    /// Number of values in the bitmap.
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// Whether the bitmap is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    // -- set operations -----------------------------------------------------

    /// Intersection (`self AND other`).
    pub fn and(&self, other: &DruidBitmap) -> DruidBitmap {
        DruidBitmap::from_fresh_roaring(&self.inner & &other.inner)
    }

    /// Union (`self OR other`).
    pub fn or(&self, other: &DruidBitmap) -> DruidBitmap {
        DruidBitmap::from_fresh_roaring(&self.inner | &other.inner)
    }

    /// Difference (`self AND NOT other`).
    pub fn and_not(&self, other: &DruidBitmap) -> DruidBitmap {
        DruidBitmap::from_fresh_roaring(&self.inner - &other.inner)
    }

    /// Complement within a universe of `[0, universe_size)`.
    pub fn not(&self, universe_size: u32) -> DruidBitmap {
        let mut universe = RoaringBitmap::new();
        universe.insert_range(0..universe_size);
        // `owned - &borrowed` subtracts in place (retain_mut), so the result
        // keeps the universe's container-vector capacity even when the
        // complement is small. Rebuild so retained capacity is charged.
        DruidBitmap::from_roaring(universe - &self.inner)
    }

    /// Iterate over the values in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.inner.iter()
    }

    // -- bulk operations ----------------------------------------------------

    /// Build a bitmap from a sorted iterator of `u32` values.
    pub fn from_sorted_iter(iter: impl Iterator<Item = u32>) -> Self {
        let mut bm = RoaringBitmap::new();
        for v in iter {
            bm.insert(v);
        }
        DruidBitmap { inner: bm }
    }

    /// Intersect many bitmaps efficiently.
    ///
    /// Returns a full-universe bitmap when the input slice is empty.
    pub fn intersect_many(bitmaps: &[&DruidBitmap]) -> DruidBitmap {
        match bitmaps.len() {
            0 => DruidBitmap::new(),
            1 => bitmaps[0].clone(),
            _ => {
                let mut result = bitmaps[0].inner.clone();
                for bm in &bitmaps[1..] {
                    result &= &bm.inner;
                }
                // Unlike the binary ops, this clones the first bitmap and
                // intersects in place, so `result` can retain the original
                // container-vector capacity after containers are removed.
                // Rebuild via `from_roaring` so retained capacity cannot
                // escape cache accounting.
                DruidBitmap::from_roaring(result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_empty() {
        let bm = DruidBitmap::new();
        let bytes = bm.serialize_druid().expect("serialize");
        let bm2 = DruidBitmap::deserialize_druid(&bytes).expect("deserialize");
        assert_eq!(bm, bm2);
        assert!(bm2.is_empty());
    }

    #[test]
    fn round_trip_single() {
        let mut bm = DruidBitmap::new();
        bm.insert(42);
        let bytes = bm.serialize_druid().expect("serialize");
        let bm2 = DruidBitmap::deserialize_druid(&bytes).expect("deserialize");
        assert_eq!(bm, bm2);
        assert!(bm2.contains(42));
        assert_eq!(bm2.len(), 1);
    }

    #[test]
    fn round_trip_large() {
        let bm = DruidBitmap::from_sorted_iter(0..100_000);
        assert_eq!(bm.len(), 100_000);
        let bytes = bm.serialize_druid().expect("serialize");
        let bm2 = DruidBitmap::deserialize_druid(&bytes).expect("deserialize");
        assert_eq!(bm, bm2);
    }

    #[test]
    fn type_tag_is_roaring() {
        let bm = DruidBitmap::new();
        let bytes = bm.serialize_druid().expect("serialize");
        assert_eq!(bytes[0], BITMAP_TYPE_ROARING);
    }

    /// `serialize_portable` is the exact inverse of `deserialize_portable`
    /// (the native segment writer's value-bitmap path, TG-1 Part 2), and
    /// `serialize_druid` is exactly `[0x01]` + the portable bytes.
    #[test]
    fn serialize_portable_round_trips_and_matches_tagged_form() {
        for values in [vec![], vec![0u32], vec![0, 4, 9], (0..10_000).collect()] {
            let bm = DruidBitmap::from_sorted_iter(values.iter().copied());
            let portable = bm.serialize_portable().expect("serialize portable");
            let max_rows = u64::from(values.iter().max().copied().unwrap_or(0)) + 1;
            let back = DruidBitmap::deserialize_portable(&portable, max_rows).expect("deserialize");
            assert_eq!(bm, back, "portable round-trip for {} values", values.len());

            let tagged = bm.serialize_druid().expect("serialize druid");
            assert_eq!(tagged[0], BITMAP_TYPE_ROARING);
            assert_eq!(
                &tagged[1..],
                portable.as_slice(),
                "serialize_druid must be exactly the tag byte + serialize_portable"
            );
        }
    }

    #[test]
    fn concise_tag_rejected() {
        let data = [BITMAP_TYPE_CONCISE, 0, 0, 0];
        let res = DruidBitmap::deserialize_druid(&data);
        assert!(res.is_err());
    }

    #[test]
    fn unknown_tag_rejected() {
        let data = [0xFF, 0, 0, 0];
        let res = DruidBitmap::deserialize_druid(&data);
        assert!(res.is_err());
    }

    #[test]
    fn empty_data_rejected() {
        let res = DruidBitmap::deserialize_druid(&[]);
        assert!(res.is_err());
    }

    #[test]
    fn set_and() {
        let a = DruidBitmap::from_sorted_iter([1, 2, 3, 4, 5].into_iter());
        let b = DruidBitmap::from_sorted_iter([3, 4, 5, 6, 7].into_iter());
        let c = a.and(&b);
        let vals: Vec<u32> = c.iter().collect();
        assert_eq!(vals, vec![3, 4, 5]);
    }

    #[test]
    fn set_or() {
        let a = DruidBitmap::from_sorted_iter([1, 2, 3].into_iter());
        let b = DruidBitmap::from_sorted_iter([3, 4, 5].into_iter());
        let c = a.or(&b);
        let vals: Vec<u32> = c.iter().collect();
        assert_eq!(vals, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn set_and_not() {
        let a = DruidBitmap::from_sorted_iter([1, 2, 3, 4, 5].into_iter());
        let b = DruidBitmap::from_sorted_iter([3, 4, 5, 6, 7].into_iter());
        let c = a.and_not(&b);
        let vals: Vec<u32> = c.iter().collect();
        assert_eq!(vals, vec![1, 2]);
    }

    #[test]
    fn set_not() {
        let a = DruidBitmap::from_sorted_iter([1, 3].into_iter());
        let c = a.not(5);
        let vals: Vec<u32> = c.iter().collect();
        assert_eq!(vals, vec![0, 2, 4]);
    }

    #[test]
    fn intersect_many_empty_slice() {
        let result = DruidBitmap::intersect_many(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn intersect_many_single() {
        let a = DruidBitmap::from_sorted_iter([10, 20, 30].into_iter());
        let result = DruidBitmap::intersect_many(&[&a]);
        assert_eq!(result, a);
    }

    #[test]
    fn intersect_many_five() {
        let bitmaps: Vec<DruidBitmap> = (0..5)
            .map(|i| {
                // Each bitmap has values [i*10 .. i*10+20)
                // so the intersection should be [40..50) only for i=0..4
                // Actually let me make them overlap on a specific range.
                DruidBitmap::from_sorted_iter((i..100).map(|x| x as u32))
            })
            .collect();
        let refs: Vec<&DruidBitmap> = bitmaps.iter().collect();
        let result = DruidBitmap::intersect_many(&refs);
        // intersection of [0..100), [1..100), [2..100), [3..100), [4..100) = [4..100)
        let expected = DruidBitmap::from_sorted_iter(4..100_u32);
        assert_eq!(result, expected);
    }

    // -- bounded deserialization (untrusted segment input) -------------------

    #[test]
    fn portable_rejects_hostile_run_container_count_before_decode() {
        // Run-format cookie declaring 65 536 containers (the format
        // maximum): fully backed, ~900 KiB of input expands to ~512 MiB of
        // container stores during deserialization. On a 1-row universe at
        // most 1 container is legitimate — the count must be rejected from
        // the 4-byte header alone.
        let cookie: u32 = (0xFFFF << 16) | 12_347;
        let err = DruidBitmap::deserialize_portable(&cookie.to_le_bytes(), 1)
            .expect_err("hostile container count must fail closed");
        assert!(
            err.to_string().contains("containers"),
            "expected the container bound, got: {err}"
        );
    }

    #[test]
    fn portable_rejects_hostile_norun_container_count_before_decode() {
        // No-run cookie followed by a u32::MAX container count.
        let mut data = 12_346u32.to_le_bytes().to_vec();
        data.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = DruidBitmap::deserialize_portable(&data, 1_000_000)
            .expect_err("hostile container count must fail closed");
        assert!(
            err.to_string().contains("containers"),
            "expected the container bound, got: {err}"
        );
    }

    #[test]
    fn portable_rejects_oversized_payload() {
        let data = vec![0u8; MAX_SERIALIZED_BITMAP_BYTES + 1];
        let err = DruidBitmap::deserialize_portable(&data, u64::MAX)
            .expect_err("oversized payload must fail closed");
        assert!(err.to_string().contains("exceeds cap"), "got: {err}");
    }

    #[test]
    fn portable_rejects_unknown_cookie_and_truncation() {
        assert!(DruidBitmap::deserialize_portable(&[], 100).is_err());
        assert!(DruidBitmap::deserialize_portable(&[0x3B, 0x30], 100).is_err());
        // Valid length, bogus cookie.
        let err = DruidBitmap::deserialize_portable(&0xDEAD_BEEFu32.to_le_bytes(), 100)
            .expect_err("unknown cookie must fail closed");
        assert!(err.to_string().contains("cookie"), "got: {err}");
        // Run cookie is fine but the truncated body must still error inside
        // the (now bounded) roaring decoder, not panic.
        let cookie: u32 = 12_347; // 1 container declared
        assert!(DruidBitmap::deserialize_portable(&cookie.to_le_bytes(), 100_000).is_err());
    }

    #[test]
    fn portable_bounded_round_trip_at_container_edges() {
        // Rows in two containers: {0, 65535, 65536, 131071}.
        let bm = DruidBitmap::from_sorted_iter([0u32, 65_535, 65_536, 131_071].into_iter());
        let mut portable = Vec::new();
        bm.as_inner()
            .serialize_into(&mut portable)
            .expect("serialize");
        // max_rows = 131072 allows ceil(131072/65536) = 2 containers.
        let back =
            DruidBitmap::deserialize_portable(&portable, 131_072).expect("bounded deserialize");
        assert_eq!(back, bm);
        // max_rows = 65536 allows only 1 container: the same payload is
        // structurally out of range and must be rejected up front.
        let err = DruidBitmap::deserialize_portable(&portable, 65_536)
            .expect_err("2 containers on a 1-container universe must fail");
        assert!(err.to_string().contains("containers"), "got: {err}");
    }

    #[test]
    fn deserialize_portable_rejects_trailing_bytes() {
        let bm = DruidBitmap::from_sorted_iter([1u32, 7, 42].into_iter());
        let mut portable = Vec::new();
        bm.as_inner()
            .serialize_into(&mut portable)
            .expect("serialize");
        // Exact-length payload still deserializes (no regression).
        let back =
            DruidBitmap::deserialize_portable(&portable, 43).expect("exact-length deserialize");
        assert_eq!(back, bm);
        // Every caller frames the payload to an exact length, so a leftover
        // byte means the frame was mis-sized / malformed: must fail closed.
        let mut extended = portable.clone();
        extended.push(0xEE);
        let err = DruidBitmap::deserialize_portable(&extended, 43)
            .expect_err("trailing byte must fail closed");
        assert!(err.to_string().contains("trailing"), "got: {err}");
    }

    #[test]
    fn deserialize_druid_bounded_round_trip_and_bounds() {
        let bm = DruidBitmap::from_sorted_iter([1u32, 7, 42].into_iter());
        let bytes = bm.serialize_druid().expect("serialize");
        let back = DruidBitmap::deserialize_druid_bounded(&bytes, 43).expect("deserialize");
        assert_eq!(back, bm);
        // Empty bitmap survives a zero-row bound (0 containers declared).
        let empty = DruidBitmap::new().serialize_druid().expect("serialize");
        assert!(DruidBitmap::deserialize_druid_bounded(&empty, 0).is_ok());
        // Tagged hostile payload is rejected before decode.
        let mut hostile = vec![BITMAP_TYPE_ROARING];
        let cookie: u32 = (0xFFFF << 16) | 12_347;
        hostile.extend_from_slice(&cookie.to_le_bytes());
        let err = DruidBitmap::deserialize_druid_bounded(&hostile, 1)
            .expect_err("hostile container count must fail closed");
        assert!(err.to_string().contains("containers"), "got: {err}");
        // Tag dispatch still fails closed on concise/unknown/empty.
        assert!(DruidBitmap::deserialize_druid_bounded(&[], 1).is_err());
        assert!(DruidBitmap::deserialize_druid_bounded(&[BITMAP_TYPE_CONCISE, 0], 1).is_err());
        assert!(DruidBitmap::deserialize_druid_bounded(&[0xFF, 0], 1).is_err());
    }

    #[test]
    fn from_roaring_and_into_inner() {
        let mut rb = RoaringBitmap::new();
        rb.insert(7);
        rb.insert(77);
        let bm = DruidBitmap::from_roaring(rb.clone());
        assert_eq!(bm.as_inner(), &rb);
        let rb2 = bm.into_inner();
        assert_eq!(rb2, rb);
    }

    #[test]
    fn from_roaring_discards_capacity_retained_by_cleared_containers() {
        let mut rb = RoaringBitmap::new();
        for container in 0..1024_u32 {
            rb.insert(container << 16);
        }
        assert_eq!(rb.statistics().n_containers, 1024);
        rb.clear();
        let bm = DruidBitmap::from_roaring(rb);
        assert!(bm.is_empty());
        assert_eq!(bm.estimated_heap_bytes(), 0);
    }

    #[test]
    fn default_is_empty() {
        let bm = DruidBitmap::default();
        assert!(bm.is_empty());
        assert_eq!(bm.len(), 0);
    }
}
