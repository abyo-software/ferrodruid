// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Front-coded dictionary encoding for Druid string columns.
//!
//! Since Druid 28, string columns use **front-coded dictionaries** where
//! consecutive sorted entries share common prefixes.  Entries are grouped into
//! fixed-size *buckets*; the first entry in each bucket is stored in full
//! while subsequent entries record only the prefix length shared with the
//! previous entry and the differing suffix.
//!
//! Two on-disk versions exist and are distinguished by the leading version
//! marker:
//!
//! * **v1** (segment v9): each entry within a bucket is incrementally coded
//!   against the *previous* entry. Random access requires decoding from the
//!   bucket base forward to the target.
//! * **v2** (segment FDX): each entry within a bucket is incrementally coded
//!   against the bucket *base* (its first, fully-stored entry), so random
//!   access only needs the base plus the target entry's record. `bucket_size`
//!   is required to be a power of two.
//!
//! [`FrontCodedDictionary::deserialize`] reads either version by inspecting
//! the marker; v1 round-trips through [`FrontCodedDictionary::serialize`] and
//! v2 through [`FrontCodedDictionary::serialize_v2`].
//!
//! Wire format (v1):
//! ```text
//! [4 bytes LE: version (1)]
//! [4 bytes LE: bucket_size]
//! [4 bytes LE: num_values]
//! <bucket data>
//! [4 bytes LE × num_buckets: offsets to each bucket start]
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use ferrodruid_common::error::{DruidError, Result};

/// Front-coded dictionary format version 1 (used by segment v9).
///
/// In v1 each entry within a bucket is front-coded against the
/// *immediately preceding* entry. Random access to the i-th entry must
/// therefore decode every prior entry of the bucket up to and including i.
const FORMAT_VERSION_V1: u32 = 1;

/// Front-coded dictionary format version 2 (used by segment FDX).
///
/// In v2 every entry within a bucket is front-coded against the bucket's
/// *base* (the first, fully-stored entry of the bucket). Random access to
/// the i-th entry therefore only needs the bucket base plus that one entry's
/// `(prefix_len, suffix)` record — the cost is bounded by `bucket_size`
/// regardless of where the entry sits in the bucket. `bucket_size` is
/// required to be a power of two.
///
/// Wire format:
/// ```text
/// [4 bytes LE: version (2)]
/// [4 bytes LE: bucket_size]   (power of two)
/// [4 bytes LE: num_values]
/// <bucket data>
///   per bucket:
///     base entry:   [vint base_len][base bytes]
///     then for each remaining entry of the bucket:
///       [vint prefix_len][vint suffix_len][suffix bytes]
///       (prefix_len is shared with the BUCKET BASE, not the previous entry)
/// [4 bytes LE × num_buckets: offset to each bucket start, relative to
///  the start of <bucket data>]
/// ```
const FORMAT_VERSION_V2: u32 = 2;

/// Default format version for [`FrontCodedDictionary::serialize`] (v1).
const FORMAT_VERSION: u32 = FORMAT_VERSION_V1;

// ---------------------------------------------------------------------------
// VInt helpers (Lucene-style variable-length integer)
// ---------------------------------------------------------------------------

/// Encode a `u32` as a variable-length integer and append to `buf`.
///
/// Each byte uses the high bit as a continuation flag (1 = more bytes follow).
/// The value is written in little-endian 7-bit groups.
fn write_vint(buf: &mut Vec<u8>, mut val: u32) {
    // Idiomatic LEB128: while more than 7 bits remain, emit the low 7 bits with
    // the continuation flag set, then shift. `val as u8 | 0x80` keeps the real
    // bit 7 (carried by the `>>= 7`) distinct from the forced continuation flag,
    // so flipping the operator (`|` -> `^`) changes the emitted bytes and is
    // caught by the multi-byte round-trip tests (it is not an equivalent mutant).
    while val >= 0x80 {
        buf.push(val as u8 | 0x80);
        val >>= 7;
    }
    buf.push(val as u8);
}

/// Decode a variable-length integer from `data` starting at `*pos`.
///
/// Advances `*pos` past the consumed bytes.
fn read_vint(data: &[u8], pos: &mut usize) -> Result<u32> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return Err(DruidError::Segment(
                "unexpected end of data while reading VInt".to_string(),
            ));
        }
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 35 {
            return Err(DruidError::Segment("VInt exceeds 32-bit range".to_string()));
        }
    }
}

// ---------------------------------------------------------------------------
// FrontCodedDictionary
// ---------------------------------------------------------------------------

/// A front-coded dictionary for Druid string columns.
///
/// Values are stored in sorted order; each value can be retrieved by its
/// zero-based ordinal or looked up via binary search.
#[derive(Clone, Debug)]
pub struct FrontCodedDictionary {
    /// Decoded values in sorted order.
    values: Vec<String>,
}

impl Default for FrontCodedDictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl FrontCodedDictionary {
    /// Create an empty dictionary.
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    /// Build from a pre-sorted list of strings.
    ///
    /// The caller must ensure values are sorted; this constructor does **not**
    /// re-sort.
    pub fn from_sorted(values: Vec<String>) -> Self {
        Self { values }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the dictionary contains no entries.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Estimate heap bytes retained by the decoded dictionary.
    ///
    /// This uses vector and individual string capacities rather than lengths,
    /// so a short value backed by an oversized allocation is fully charged.
    #[must_use]
    pub fn estimated_heap_bytes(&self) -> u64 {
        let slots = u64::try_from(self.values.capacity())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(std::mem::size_of::<String>()).unwrap_or(u64::MAX));
        self.values.iter().fold(slots, |total, value| {
            total.saturating_add(u64::try_from(value.capacity()).unwrap_or(u64::MAX))
        })
    }

    /// Get a value by its ordinal (0-based index).
    pub fn get(&self, ordinal: usize) -> Option<&str> {
        self.values.get(ordinal).map(|s| s.as_str())
    }

    /// Binary-search for the ordinal of `value`.
    pub fn find(&self, value: &str) -> Option<usize> {
        self.values.binary_search_by(|v| v.as_str().cmp(value)).ok()
    }

    /// Find the range of ordinals whose values start with `prefix`.
    ///
    /// Returns an empty range when no values match.
    pub fn find_prefix_range(&self, prefix: &str) -> std::ops::Range<usize> {
        // Rust string ordering is bytewise, so we partition on raw bytes. This
        // keeps the upper bound exact even when the successor is not valid
        // UTF-8 (e.g. incrementing the last byte crosses a UTF-8 boundary).
        let prefix_bytes = prefix.as_bytes();
        // Lower bound: first value >= prefix
        let lo = self.values.partition_point(|v| v.as_bytes() < prefix_bytes);
        // Upper bound: first value that does not start with prefix. A string s
        // does NOT start with prefix iff s >= prefix_successor, the smallest
        // byte sequence greater than every string with that prefix.
        let hi = match prefix_successor(prefix_bytes) {
            Some(succ) => self
                .values
                .partition_point(|v| v.as_bytes() < succ.as_slice()),
            None => {
                // prefix is all 0xFF bytes – everything from lo onwards matches
                self.values.len()
            }
        };
        lo..hi
    }

    /// Iterate all values with their ordinals.
    pub fn iter(&self) -> impl Iterator<Item = (usize, &str)> {
        self.values.iter().enumerate().map(|(i, s)| (i, s.as_str()))
    }

    // -- serialization ------------------------------------------------------

    /// Serialize to the Druid front-coded binary format.
    ///
    /// `bucket_size` controls how many entries share a bucket (typically 4 or
    /// 16).
    pub fn serialize(&self, bucket_size: usize) -> Result<Vec<u8>> {
        if bucket_size == 0 {
            return Err(DruidError::Segment("bucket_size must be > 0".to_string()));
        }

        let num_values = self.values.len();
        let num_buckets = if num_values == 0 {
            0
        } else {
            num_values.div_ceil(bucket_size)
        };

        let mut buf = Vec::new();

        // Header
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(bucket_size as u32).to_le_bytes());
        buf.extend_from_slice(&(num_values as u32).to_le_bytes());

        // Bucket data + collect offsets (relative to start of bucket data)
        let bucket_data_start = buf.len();
        let mut bucket_offsets: Vec<u32> = Vec::with_capacity(num_buckets);

        for bucket_idx in 0..num_buckets {
            let bucket_start = bucket_idx * bucket_size;
            let bucket_end = std::cmp::min(bucket_start + bucket_size, num_values);

            bucket_offsets.push((buf.len() - bucket_data_start) as u32);

            // First entry: full string
            let first = self.values[bucket_start].as_bytes();
            write_vint(&mut buf, first.len() as u32);
            buf.extend_from_slice(first);

            // Subsequent entries: prefix_len + suffix
            let mut prev = &self.values[bucket_start];
            for idx in (bucket_start + 1)..bucket_end {
                let cur = &self.values[idx];
                let prefix_len = common_prefix_len(prev.as_bytes(), cur.as_bytes());
                let suffix = &cur.as_bytes()[prefix_len..];
                write_vint(&mut buf, prefix_len as u32);
                write_vint(&mut buf, suffix.len() as u32);
                buf.extend_from_slice(suffix);
                prev = cur;
            }
        }

        // Offset table
        for off in &bucket_offsets {
            buf.extend_from_slice(&off.to_le_bytes());
        }

        Ok(buf)
    }

    /// Serialize to the **version 2** front-coded binary format used by
    /// segment FDX.
    ///
    /// Unlike [`serialize`](Self::serialize), every entry within a bucket is
    /// front-coded against the bucket *base* (its first entry) rather than the
    /// previous entry. This bounds random access to a single bucket-base plus
    /// one entry record. `bucket_size` must be a non-zero power of two.
    pub fn serialize_v2(&self, bucket_size: usize) -> Result<Vec<u8>> {
        if bucket_size == 0 {
            return Err(DruidError::Segment("bucket_size must be > 0".to_string()));
        }
        if !bucket_size.is_power_of_two() {
            return Err(DruidError::Segment(format!(
                "front-coded v2 bucket_size {bucket_size} must be a power of two"
            )));
        }

        let num_values = self.values.len();
        let num_buckets = if num_values == 0 {
            0
        } else {
            num_values.div_ceil(bucket_size)
        };

        let mut buf = Vec::new();

        // Header
        buf.extend_from_slice(&FORMAT_VERSION_V2.to_le_bytes());
        buf.extend_from_slice(&(bucket_size as u32).to_le_bytes());
        buf.extend_from_slice(&(num_values as u32).to_le_bytes());

        let bucket_data_start = buf.len();
        let mut bucket_offsets: Vec<u32> = Vec::with_capacity(num_buckets);

        for bucket_idx in 0..num_buckets {
            let bucket_start = bucket_idx * bucket_size;
            let bucket_end = std::cmp::min(bucket_start + bucket_size, num_values);

            bucket_offsets.push((buf.len() - bucket_data_start) as u32);

            // First entry: full string — this is the bucket base.
            let base = self.values[bucket_start].as_bytes();
            write_vint(&mut buf, base.len() as u32);
            buf.extend_from_slice(base);

            // Subsequent entries: prefix_len shared with the BUCKET BASE.
            for idx in (bucket_start + 1)..bucket_end {
                let cur = self.values[idx].as_bytes();
                let prefix_len = common_prefix_len(base, cur);
                let suffix = &cur[prefix_len..];
                write_vint(&mut buf, prefix_len as u32);
                write_vint(&mut buf, suffix.len() as u32);
                buf.extend_from_slice(suffix);
            }
        }

        // Offset table
        for off in &bucket_offsets {
            buf.extend_from_slice(&off.to_le_bytes());
        }

        Ok(buf)
    }

    /// Deserialize from the Druid front-coded binary format.
    ///
    /// The leading 4-byte version marker selects the v1 or v2 decoder, so a
    /// single call decodes both segment-v9 (v1) and segment-FDX (v2)
    /// dictionaries.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 12 {
            return Err(DruidError::Segment(
                "front-coded dictionary too short for header".to_string(),
            ));
        }

        let version = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        match version {
            FORMAT_VERSION_V1 => Self::deserialize_v1(data),
            FORMAT_VERSION_V2 => Self::deserialize_v2(data),
            other => Err(DruidError::Segment(format!(
                "unsupported front-coded dictionary version: {other}"
            ))),
        }
    }

    /// Decode a v1 (previous-entry-relative) buffer. The caller has already
    /// validated the length and version marker.
    fn deserialize_v1(data: &[u8]) -> Result<Self> {
        let bucket_size = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let num_values = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;

        if num_values == 0 {
            return Ok(Self::new());
        }
        if bucket_size == 0 {
            return Err(DruidError::Segment(
                "bucket_size is 0 in serialized dictionary".to_string(),
            ));
        }

        // The header is 12 B and a final offset table of `num_buckets × 4`
        // sits at the tail; everything in between is bucket data. The
        // smallest possible entry is a single VInt zero byte (empty
        // string), so `num_values <= data.len() - 12` is a strict upper
        // bound: a header that claims more entries than the input could
        // ever encode is malicious. Reject early so we don't
        // `Vec::with_capacity(4.29 GiB)` from a 20 B input.
        //
        // Reproducer: `fuzz/artifacts/fuzz_dict_deser/oom-cbbbc3c63d…`
        // (20 B, num_values = 0xFFFE_FEFB ≈ 4.29 G, would allocate
        // ~100 GiB of `Vec<String>` headers up front).
        let max_entries = data.len().saturating_sub(12);
        if num_values > max_entries {
            return Err(DruidError::Segment(format!(
                "front-coded dictionary num_values {num_values} exceeds \
                 strict upper bound {max_entries} for {} B input",
                data.len()
            )));
        }

        let num_buckets = num_values.div_ceil(bucket_size);

        // Read offset table (at the end of data). `num_buckets * 4` cannot
        // overflow because `num_buckets ≤ num_values ≤ data.len() - 12`.
        let offsets_byte_len = num_buckets * 4;
        if data.len() < 12 + offsets_byte_len {
            return Err(DruidError::Segment(
                "front-coded dictionary truncated (offset table)".to_string(),
            ));
        }
        let offsets_start = data.len() - offsets_byte_len;
        let mut bucket_offsets = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let base = offsets_start + i * 4;
            let off =
                u32::from_le_bytes([data[base], data[base + 1], data[base + 2], data[base + 3]])
                    as usize;
            bucket_offsets.push(off);
        }

        let bucket_data_start: usize = 12; // right after header
        // DD R17: reject a malformed/ambiguous offset table before decoding.
        validate_bucket_offsets(&bucket_offsets, offsets_start - bucket_data_start)?;

        // `num_values` has been bound-checked above; the following
        // pre-allocation is therefore at most `data.len() - 12` strings,
        // which is always ≤ the original input size.
        let mut values = Vec::with_capacity(num_values);

        for (bucket_idx, &bucket_offset) in bucket_offsets.iter().enumerate() {
            let bucket_entry_start = bucket_idx * bucket_size;
            let bucket_entry_end = std::cmp::min(bucket_entry_start + bucket_size, num_values);
            let count = bucket_entry_end - bucket_entry_start;

            let mut pos = bucket_data_start + bucket_offset;

            // First entry: full string. DD R33: use checked_add (mirroring v2)
            // so a 5-byte VInt length of ~u32::MAX cannot wrap `pos + len` below
            // `offsets_start` on 32-bit and bypass the truncation check.
            let len = read_vint(data, &mut pos)? as usize;
            if pos.checked_add(len).is_none_or(|end| end > offsets_start) {
                return Err(DruidError::Segment(
                    "front-coded dictionary: bucket data overflows".to_string(),
                ));
            }
            let first = String::from_utf8(data[pos..pos + len].to_vec())
                .map_err(|e| DruidError::Segment(format!("invalid UTF-8 in dictionary: {e}")))?;
            pos += len;
            let mut prev = first.clone();
            values.push(first);

            // Subsequent entries
            for _ in 1..count {
                let prefix_len = read_vint(data, &mut pos)? as usize;
                let suffix_len = read_vint(data, &mut pos)? as usize;
                if pos
                    .checked_add(suffix_len)
                    .is_none_or(|end| end > offsets_start)
                {
                    return Err(DruidError::Segment(
                        "front-coded dictionary: entry data overflows".to_string(),
                    ));
                }
                if prefix_len > prev.len() {
                    return Err(DruidError::Segment(format!(
                        "prefix_len {prefix_len} exceeds previous entry length {}",
                        prev.len()
                    )));
                }
                // `prefix_len` is a byte index; slicing into a `&str` panics
                // if the index is not on a UTF-8 char boundary. The 24 h
                // fuzz finding `crash-299fd3c7…` (41 B) tripped this with a
                // 2-byte `Ą` followed by `prefix_len = 1`. Validate the
                // boundary explicitly so a malformed dictionary surfaces as
                // an `Err` rather than a process-killing panic.
                if !prev.is_char_boundary(prefix_len) {
                    return Err(DruidError::Segment(format!(
                        "prefix_len {prefix_len} falls inside a multi-byte UTF-8 \
                         character of the previous entry"
                    )));
                }
                let mut entry = String::with_capacity(prefix_len + suffix_len);
                entry.push_str(&prev[..prefix_len]);
                let suffix = std::str::from_utf8(&data[pos..pos + suffix_len]).map_err(|e| {
                    DruidError::Segment(format!("invalid UTF-8 in dictionary suffix: {e}"))
                })?;
                entry.push_str(suffix);
                pos += suffix_len;
                prev = entry.clone();
                values.push(entry);
            }

            // DD R17: bucket must consume exactly up to the next bucket offset.
            let expected_end = if bucket_idx + 1 < bucket_offsets.len() {
                bucket_data_start + bucket_offsets[bucket_idx + 1]
            } else {
                offsets_start
            };
            if pos != expected_end {
                return Err(DruidError::Segment(format!(
                    "front-coded dictionary: bucket {bucket_idx} ends at byte {pos} but the \
                     next bucket begins at {expected_end} (corrupt offset table)"
                )));
            }
        }

        Ok(Self { values })
    }

    /// Decode a v2 (bucket-base-relative) buffer. The caller has already
    /// validated the length and version marker.
    fn deserialize_v2(data: &[u8]) -> Result<Self> {
        let bucket_size = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let num_values = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;

        if num_values == 0 {
            return Ok(Self::new());
        }
        if bucket_size == 0 {
            return Err(DruidError::Segment(
                "bucket_size is 0 in serialized dictionary".to_string(),
            ));
        }
        if !bucket_size.is_power_of_two() {
            return Err(DruidError::Segment(format!(
                "front-coded v2 bucket_size {bucket_size} must be a power of two"
            )));
        }

        // Same strict upper bound as v1: the smallest possible entry is a
        // single VInt byte, so a header claiming more entries than the input
        // could ever encode is rejected before any large allocation.
        let max_entries = data.len().saturating_sub(12);
        if num_values > max_entries {
            return Err(DruidError::Segment(format!(
                "front-coded dictionary num_values {num_values} exceeds \
                 strict upper bound {max_entries} for {} B input",
                data.len()
            )));
        }

        let num_buckets = num_values.div_ceil(bucket_size);

        let offsets_byte_len = num_buckets * 4;
        if data.len() < 12 + offsets_byte_len {
            return Err(DruidError::Segment(
                "front-coded dictionary truncated (offset table)".to_string(),
            ));
        }
        let offsets_start = data.len() - offsets_byte_len;
        let mut bucket_offsets = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let base = offsets_start + i * 4;
            let off =
                u32::from_le_bytes([data[base], data[base + 1], data[base + 2], data[base + 3]])
                    as usize;
            bucket_offsets.push(off);
        }

        let bucket_data_start: usize = 12; // right after header
        // DD R17: reject a malformed/ambiguous offset table (e.g. reused offsets)
        // before decoding, so a corrupt segment fails fast rather than silently
        // returning wrong values.
        validate_bucket_offsets(&bucket_offsets, offsets_start - bucket_data_start)?;
        let mut values = Vec::with_capacity(num_values);

        for (bucket_idx, &bucket_offset) in bucket_offsets.iter().enumerate() {
            let bucket_entry_start = bucket_idx * bucket_size;
            let bucket_entry_end = std::cmp::min(bucket_entry_start + bucket_size, num_values);
            let count = bucket_entry_end - bucket_entry_start;

            let mut pos = bucket_data_start
                .checked_add(bucket_offset)
                .ok_or_else(|| DruidError::Segment("bucket offset overflows".to_string()))?;

            // First entry: full string — the bucket base.
            let len = read_vint(data, &mut pos)? as usize;
            if pos.checked_add(len).is_none_or(|end| end > offsets_start) {
                return Err(DruidError::Segment(
                    "front-coded dictionary: bucket data overflows".to_string(),
                ));
            }
            let base = String::from_utf8(data[pos..pos + len].to_vec())
                .map_err(|e| DruidError::Segment(format!("invalid UTF-8 in dictionary: {e}")))?;
            pos += len;
            values.push(base.clone());

            // Subsequent entries are front-coded against the BUCKET BASE.
            for _ in 1..count {
                let prefix_len = read_vint(data, &mut pos)? as usize;
                let suffix_len = read_vint(data, &mut pos)? as usize;
                if pos
                    .checked_add(suffix_len)
                    .is_none_or(|end| end > offsets_start)
                {
                    return Err(DruidError::Segment(
                        "front-coded dictionary: entry data overflows".to_string(),
                    ));
                }
                if prefix_len > base.len() {
                    return Err(DruidError::Segment(format!(
                        "prefix_len {prefix_len} exceeds bucket-base length {}",
                        base.len()
                    )));
                }
                if !base.is_char_boundary(prefix_len) {
                    return Err(DruidError::Segment(format!(
                        "prefix_len {prefix_len} falls inside a multi-byte UTF-8 \
                         character of the bucket base"
                    )));
                }
                let mut entry = String::with_capacity(prefix_len + suffix_len);
                entry.push_str(&base[..prefix_len]);
                let suffix = std::str::from_utf8(&data[pos..pos + suffix_len]).map_err(|e| {
                    DruidError::Segment(format!("invalid UTF-8 in dictionary suffix: {e}"))
                })?;
                entry.push_str(suffix);
                pos += suffix_len;
                values.push(entry);
            }

            // DD R17: this bucket must consume exactly up to the next bucket's
            // offset (or the offset table for the final bucket); a gap or
            // overrun means the segment is corrupt/ambiguous and is rejected.
            let expected_end = if bucket_idx + 1 < bucket_offsets.len() {
                bucket_data_start + bucket_offsets[bucket_idx + 1]
            } else {
                offsets_start
            };
            if pos != expected_end {
                return Err(DruidError::Segment(format!(
                    "front-coded dictionary: bucket {bucket_idx} ends at byte {pos} but the \
                     next bucket begins at {expected_end} (corrupt offset table)"
                )));
            }
        }

        Ok(Self { values })
    }

    /// Random-access decode of the `ordinal`-th string directly from a
    /// **version 2** serialized buffer, without materializing the whole
    /// dictionary.
    ///
    /// Only the target entry's bucket is touched: the bucket base is decoded,
    /// then entries up to `ordinal` are skipped (their suffix bytes are not
    /// copied), and the target entry is reconstructed from the base prefix
    /// plus its own suffix. Returns `Ok(None)` when `ordinal` is out of range.
    ///
    /// `data` must be a v2 buffer; a v1 buffer yields an error.
    pub fn get_v2(data: &[u8], ordinal: usize) -> Result<Option<String>> {
        if data.len() < 12 {
            return Err(DruidError::Segment(
                "front-coded dictionary too short for header".to_string(),
            ));
        }
        let version = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if version != FORMAT_VERSION_V2 {
            return Err(DruidError::Segment(format!(
                "get_v2 requires a version-2 buffer, found version {version}"
            )));
        }
        let bucket_size = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let num_values = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        if bucket_size == 0 || !bucket_size.is_power_of_two() {
            return Err(DruidError::Segment(
                "front-coded v2 bucket_size must be a non-zero power of two".to_string(),
            ));
        }
        if ordinal >= num_values {
            return Ok(None);
        }

        let max_entries = data.len().saturating_sub(12);
        if num_values > max_entries {
            return Err(DruidError::Segment(format!(
                "front-coded dictionary num_values {num_values} exceeds \
                 strict upper bound {max_entries} for {} B input",
                data.len()
            )));
        }

        let num_buckets = num_values.div_ceil(bucket_size);
        let offsets_byte_len = num_buckets * 4;
        if data.len() < 12 + offsets_byte_len {
            return Err(DruidError::Segment(
                "front-coded dictionary truncated (offset table)".to_string(),
            ));
        }
        let offsets_start = data.len() - offsets_byte_len;

        // DD R17: validate the whole offset table before trusting any single
        // offset; a reused/reordered offset (e.g. `[0, 0]`) would otherwise make
        // a random-access read decode the wrong bucket and silently return the
        // wrong string. Bounded by `num_buckets ≤ num_values ≤ data.len()`.
        let mut bucket_offsets = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let base = offsets_start + i * 4;
            bucket_offsets.push(u32::from_le_bytes([
                data[base],
                data[base + 1],
                data[base + 2],
                data[base + 3],
            ]) as usize);
        }
        validate_bucket_offsets(&bucket_offsets, offsets_start - 12)?;

        let bucket_idx = ordinal / bucket_size;
        let within = ordinal % bucket_size;

        // DD R18/R19: a strictly-increasing offset can still point INTO an
        // earlier bucket (`[0, 1]`, R18) or declare a boundary inside the target
        // bucket's own record (`[0, 2, 3]`, R19), neither of which
        // `validate_bucket_offsets` can detect. Verify every bucket UP TO AND
        // INCLUDING the target consumes exactly up to its declared next offset
        // (or `offsets_start` for the final bucket), matching the full
        // deserializer's exact-consume invariant, so the random-access decode
        // below cannot fail-open.
        for b in 0..=bucket_idx {
            let entry_start = b * bucket_size;
            let entry_end = std::cmp::min(entry_start + bucket_size, num_values);
            let count = entry_end - entry_start;
            let start = 12 + bucket_offsets[b];
            let end = skip_v2_bucket(data, start, count, offsets_start)?;
            let expected = if b + 1 < num_buckets {
                12 + bucket_offsets[b + 1]
            } else {
                offsets_start
            };
            if end != expected {
                return Err(DruidError::Segment(format!(
                    "front-coded dictionary: bucket {b} ends at byte {end} but the next \
                     bucket begins at {expected} (corrupt offset table)"
                )));
            }
        }

        let bucket_offset = bucket_offsets[bucket_idx];

        let mut pos = 12usize
            .checked_add(bucket_offset)
            .ok_or_else(|| DruidError::Segment("bucket offset overflows".to_string()))?;

        // Decode the bucket base.
        let len = read_vint(data, &mut pos)? as usize;
        if pos.checked_add(len).is_none_or(|end| end > offsets_start) {
            return Err(DruidError::Segment(
                "front-coded dictionary: bucket data overflows".to_string(),
            ));
        }
        let base = std::str::from_utf8(&data[pos..pos + len])
            .map_err(|e| DruidError::Segment(format!("invalid UTF-8 in dictionary: {e}")))?;
        pos += len;

        if within == 0 {
            return Ok(Some(base.to_string()));
        }

        // Skip intervening entries (do not copy suffixes), then decode target.
        for step in 1..=within {
            let prefix_len = read_vint(data, &mut pos)? as usize;
            let suffix_len = read_vint(data, &mut pos)? as usize;
            if pos
                .checked_add(suffix_len)
                .is_none_or(|end| end > offsets_start)
            {
                return Err(DruidError::Segment(
                    "front-coded dictionary: entry data overflows".to_string(),
                ));
            }
            if step == within {
                if prefix_len > base.len() {
                    return Err(DruidError::Segment(format!(
                        "prefix_len {prefix_len} exceeds bucket-base length {}",
                        base.len()
                    )));
                }
                if !base.is_char_boundary(prefix_len) {
                    return Err(DruidError::Segment(format!(
                        "prefix_len {prefix_len} falls inside a multi-byte UTF-8 \
                         character of the bucket base"
                    )));
                }
                let suffix = std::str::from_utf8(&data[pos..pos + suffix_len]).map_err(|e| {
                    DruidError::Segment(format!("invalid UTF-8 in dictionary suffix: {e}"))
                })?;
                let mut entry = String::with_capacity(prefix_len + suffix_len);
                entry.push_str(&base[..prefix_len]);
                entry.push_str(suffix);
                return Ok(Some(entry));
            }
            pos += suffix_len;
        }

        // Unreachable: `within < bucket_size` and the loop returns at `within`.
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute the length of the longest common prefix of two byte slices.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Validate a front-coded dictionary's bucket-offset table (DD R17).
///
/// Each offset is the byte position of a bucket's start, relative to the start
/// of the bucket-data region (immediately after the 12-byte header). A
/// well-formed table must (1) start at `0` — the first bucket begins the data
/// region, (2) be **strictly increasing** — every bucket holds at least one
/// entry that consumes at least one byte, so no two buckets may share or reorder
/// an offset, and (3) stay inside the `bucket_data_len`-byte data region. Without
/// this check a table such as `[0, 0]` decodes a later bucket from an earlier
/// bucket's bytes and silently returns wrong dimension values (e.g. `["a","a"]`
/// instead of `["a","b"]`) rather than failing fast — a data-correctness
/// fail-open that downstream segment checks (ordinal/cardinality/bitmap) cannot
/// catch.
fn validate_bucket_offsets(offsets: &[usize], bucket_data_len: usize) -> Result<()> {
    let Some(&first) = offsets.first() else {
        return Ok(());
    };
    if first != 0 {
        return Err(DruidError::Segment(format!(
            "front-coded dictionary: first bucket offset is {first} (must be 0)"
        )));
    }
    for pair in offsets.windows(2) {
        if pair[1] <= pair[0] {
            return Err(DruidError::Segment(format!(
                "front-coded dictionary: bucket offsets must be strictly increasing, \
                 found {} after {}",
                pair[1], pair[0]
            )));
        }
    }
    if let Some(&last) = offsets.last()
        && last >= bucket_data_len
    {
        return Err(DruidError::Segment(format!(
            "front-coded dictionary: bucket offset {last} is outside the \
             {bucket_data_len}-byte bucket-data region"
        )));
    }
    Ok(())
}

/// Advance past one front-coded **v2** bucket of `count` entries starting at
/// `pos`, validating every embedded length against `offsets_start` but WITHOUT
/// materializing the strings. Returns the byte position just past the bucket.
///
/// DD R18: `get_v2` uses this to verify that every bucket preceding a
/// random-access target consumes exactly up to its declared next offset. A
/// strictly-increasing offset can still point INTO an earlier bucket (e.g.
/// `[0, 1]`), which [`validate_bucket_offsets`] cannot detect; without this the
/// random-access path would decode a fake bucket base and silently return the
/// wrong string. The full deserializers already catch this via their
/// exact-consume check.
fn skip_v2_bucket(
    data: &[u8],
    mut pos: usize,
    count: usize,
    offsets_start: usize,
) -> Result<usize> {
    // First entry: full string — the bucket base.
    let len = read_vint(data, &mut pos)? as usize;
    if pos.checked_add(len).is_none_or(|end| end > offsets_start) {
        return Err(DruidError::Segment(
            "front-coded dictionary: bucket data overflows".to_string(),
        ));
    }
    pos += len;
    // Subsequent entries: prefix vint + suffix vint + suffix bytes.
    for _ in 1..count {
        let _prefix_len = read_vint(data, &mut pos)? as usize;
        let suffix_len = read_vint(data, &mut pos)? as usize;
        if pos
            .checked_add(suffix_len)
            .is_none_or(|end| end > offsets_start)
        {
            return Err(DruidError::Segment(
                "front-coded dictionary: entry data overflows".to_string(),
            ));
        }
        pos += suffix_len;
    }
    Ok(pos)
}

/// Compute the lexicographic successor of `prefix` — the smallest byte
/// sequence that is strictly greater than every byte sequence starting with
/// `prefix`.
///
/// The computation is purely bytewise: trailing `0xFF` bytes are dropped and
/// the last remaining byte is incremented. Working on raw bytes keeps the bound
/// exact even when the increment crosses a UTF-8 boundary (the result may not
/// be valid UTF-8, which is fine because the dictionary's bytewise ordering
/// matches Rust's bytewise string ordering).
///
/// Returns `None` when no such successor exists (prefix is empty or all `0xFF`
/// bytes), in which case every value `>= prefix` shares the prefix.
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bytes = prefix.to_vec();
    // Walk backwards, dropping 0xFF bytes and incrementing the last non-0xFF.
    while let Some(last) = bytes.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(bytes);
        }
        bytes.pop();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- VInt tests ----------------------------------------------------------

    #[test]
    fn vint_zero() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 0);
        assert_eq!(buf, vec![0x00]);
        let mut pos = 0;
        assert_eq!(read_vint(&buf, &mut pos).unwrap(), 0);
        assert_eq!(pos, 1);
    }

    #[test]
    fn vint_127() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 127);
        assert_eq!(buf, vec![0x7F]);
        let mut pos = 0;
        assert_eq!(read_vint(&buf, &mut pos).unwrap(), 127);
    }

    #[test]
    fn vint_128() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 128);
        // 128 = 0b10000000 → low 7 bits = 0 with continuation, then 1
        assert_eq!(buf, vec![0x80, 0x01]);
        let mut pos = 0;
        assert_eq!(read_vint(&buf, &mut pos).unwrap(), 128);
    }

    #[test]
    fn vint_16383() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 16383);
        // 16383 = 0x3FFF → 0b_011_1111_1111111
        // low 7: 1111111 = 0x7F with continuation → 0xFF
        // next 7: 1111111 = 0x7F no continuation → 0x7F
        assert_eq!(buf, vec![0xFF, 0x7F]);
        let mut pos = 0;
        assert_eq!(read_vint(&buf, &mut pos).unwrap(), 16383);
    }

    #[test]
    fn vint_16384() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 16384);
        let mut pos = 0;
        assert_eq!(read_vint(&buf, &mut pos).unwrap(), 16384);
    }

    #[test]
    fn vint_max() {
        let mut buf = Vec::new();
        write_vint(&mut buf, u32::MAX);
        let mut pos = 0;
        assert_eq!(read_vint(&buf, &mut pos).unwrap(), u32::MAX);
    }

    #[test]
    fn vint_continuation_flag_distinct_from_payload() {
        // A value whose first emitted byte has payload bit 7 == 1 once the
        // continuation flag is added (e.g. 0xFF -> low 7 bits 0x7F, +flag = 0xFF,
        // then 0x01). This locks that the continuation flag (`| 0x80`) is OR-ed,
        // not XOR-ed: an XOR would clear the flag here and corrupt the encoding.
        let mut buf = Vec::new();
        write_vint(&mut buf, 0xFF);
        assert_eq!(buf, vec![0xFF, 0x01]);
        let mut pos = 0;
        assert_eq!(read_vint(&buf, &mut pos).unwrap(), 0xFF);
        assert_eq!(pos, 2);

        // u32::MAX must be exactly 5 bytes with continuation set on the first 4.
        let mut buf = Vec::new();
        write_vint(&mut buf, u32::MAX);
        assert_eq!(buf, vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    }

    #[test]
    fn vint_truncated_error() {
        let buf = vec![0x80]; // continuation bit set but no next byte
        let mut pos = 0;
        assert!(read_vint(&buf, &mut pos).is_err());
    }

    // -- FrontCodedDictionary tests ------------------------------------------

    #[test]
    fn empty_dict() {
        let dict = FrontCodedDictionary::new();
        assert!(dict.is_empty());
        assert_eq!(dict.len(), 0);
        assert_eq!(dict.get(0), None);
        assert_eq!(dict.find("anything"), None);
        assert_eq!(dict.find_prefix_range("x"), 0..0);
    }

    #[test]
    fn single_entry() {
        let dict = FrontCodedDictionary::from_sorted(vec!["hello".to_string()]);
        assert_eq!(dict.len(), 1);
        assert_eq!(dict.get(0), Some("hello"));
        assert_eq!(dict.find("hello"), Some(0));
        assert_eq!(dict.find("world"), None);
    }

    #[test]
    fn many_entries_with_common_prefixes() {
        let values = vec![
            "eu-west-1".to_string(),
            "us-east-1".to_string(),
            "us-east-2".to_string(),
            "us-west-1".to_string(),
            "us-west-2".to_string(),
        ];
        let dict = FrontCodedDictionary::from_sorted(values.clone());

        assert_eq!(dict.len(), 5);
        for (i, v) in values.iter().enumerate() {
            assert_eq!(dict.get(i), Some(v.as_str()));
            assert_eq!(dict.find(v), Some(i));
        }
    }

    #[test]
    fn find_prefix_range() {
        let values = vec![
            "eu-west-1".to_string(),
            "us-east-1".to_string(),
            "us-east-2".to_string(),
            "us-west-1".to_string(),
            "us-west-2".to_string(),
        ];
        let dict = FrontCodedDictionary::from_sorted(values);

        assert_eq!(dict.find_prefix_range("us-"), 1..5);
        assert_eq!(dict.find_prefix_range("us-east"), 1..3);
        assert_eq!(dict.find_prefix_range("us-west"), 3..5);
        assert_eq!(dict.find_prefix_range("eu-"), 0..1);
        assert_eq!(dict.find_prefix_range("ap-"), 0..0);
        assert_eq!(dict.find_prefix_range("zz"), 5..5);
    }

    #[test]
    fn find_prefix_range_utf8_increment_boundary() {
        // The prefix "a\u{7f}" is bytes [0x61, 0x7F]; its byte-successor is
        // [0x61, 0x80], where 0x80 is a UTF-8 continuation byte that is not a
        // valid standalone scalar. The bytewise partition must still place the
        // bound exactly. "a\u{80}" encodes as [0x61, 0xC2, 0x80], which sorts
        // strictly after the successor bound [0x61, 0x80] (0xC2 > 0x80), so it
        // is excluded from the "a\u{7f}" prefix range — as is "b".
        let values = vec![
            "a\u{7f}".to_string(),
            "a\u{80}".to_string(),
            "b".to_string(),
        ];
        // Confirm the fixture is byte-sorted (a precondition of the dict).
        let mut sorted = values.clone();
        sorted.sort();
        assert_eq!(values, sorted, "test fixture must be byte-sorted");

        let dict = FrontCodedDictionary::from_sorted(values);
        // Only the first entry starts with "a\u{7f}".
        assert_eq!(dict.find_prefix_range("a\u{7f}"), 0..1);
        // Broader prefix "a" matches both "a*" entries.
        assert_eq!(dict.find_prefix_range("a"), 0..2);
    }

    #[test]
    fn iter_yields_all() {
        let values = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let dict = FrontCodedDictionary::from_sorted(values);
        let collected: Vec<(usize, &str)> = dict.iter().collect();
        assert_eq!(collected, vec![(0, "a"), (1, "b"), (2, "c")]);
    }

    // -- serialization round-trip -------------------------------------------

    #[test]
    fn round_trip_empty() {
        let dict = FrontCodedDictionary::new();
        let bytes = dict.serialize(4).expect("serialize");
        let dict2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert!(dict2.is_empty());
    }

    #[test]
    fn round_trip_single_entry() {
        let dict = FrontCodedDictionary::from_sorted(vec!["hello".to_string()]);
        let bytes = dict.serialize(4).expect("serialize");
        let dict2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert_eq!(dict2.len(), 1);
        assert_eq!(dict2.get(0), Some("hello"));
    }

    #[test]
    fn round_trip_bucket_size_4() {
        let values = vec![
            "eu-west-1".to_string(),
            "us-east-1".to_string(),
            "us-east-2".to_string(),
            "us-west-1".to_string(),
            "us-west-2".to_string(),
        ];
        let dict = FrontCodedDictionary::from_sorted(values.clone());
        let bytes = dict.serialize(4).expect("serialize");
        let dict2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert_eq!(dict2.len(), 5);
        for (i, v) in values.iter().enumerate() {
            assert_eq!(dict2.get(i), Some(v.as_str()), "ordinal {i}");
        }
    }

    #[test]
    fn round_trip_bucket_size_16() {
        let values: Vec<String> = (0..50).map(|i| format!("item-{i:04}")).collect();
        let dict = FrontCodedDictionary::from_sorted(values.clone());
        let bytes = dict.serialize(16).expect("serialize");
        let dict2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert_eq!(dict2.len(), 50);
        for (i, v) in values.iter().enumerate() {
            assert_eq!(dict2.get(i), Some(v.as_str()), "ordinal {i}");
        }
    }

    #[test]
    fn round_trip_preserves_find() {
        let values = vec![
            "alpha".to_string(),
            "alpha-beta".to_string(),
            "alpha-gamma".to_string(),
            "beta".to_string(),
        ];
        let dict = FrontCodedDictionary::from_sorted(values);
        let bytes = dict.serialize(4).expect("serialize");
        let dict2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert_eq!(dict2.find("alpha"), Some(0));
        assert_eq!(dict2.find("alpha-beta"), Some(1));
        assert_eq!(dict2.find("alpha-gamma"), Some(2));
        assert_eq!(dict2.find("beta"), Some(3));
        assert_eq!(dict2.find("alpha-delta"), None);
        assert_eq!(dict2.find_prefix_range("alpha"), 0..3);
    }

    #[test]
    fn serialize_bucket_size_zero_is_error() {
        let dict = FrontCodedDictionary::from_sorted(vec!["x".to_string()]);
        assert!(dict.serialize(0).is_err());
    }

    #[test]
    fn deserialize_truncated_header() {
        assert!(FrontCodedDictionary::deserialize(&[0; 8]).is_err());
    }

    #[test]
    fn deserialize_bad_version() {
        let mut data = vec![0u8; 12];
        // version = 99
        data[0..4].copy_from_slice(&99_u32.to_le_bytes());
        assert!(FrontCodedDictionary::deserialize(&data).is_err());
    }

    #[test]
    fn round_trip_exact_bucket_boundary() {
        // Exactly 4 entries → 1 full bucket, no remainder
        let values: Vec<String> = (0..4).map(|i| format!("val-{i}")).collect();
        let dict = FrontCodedDictionary::from_sorted(values.clone());
        let bytes = dict.serialize(4).expect("serialize");
        let dict2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        for (i, v) in values.iter().enumerate() {
            assert_eq!(dict2.get(i), Some(v.as_str()));
        }
    }

    #[test]
    fn round_trip_many_buckets() {
        // 100 entries with bucket_size=4 → 25 buckets
        let values: Vec<String> = (0..100).map(|i| format!("key-{i:06}")).collect();
        let dict = FrontCodedDictionary::from_sorted(values.clone());
        let bytes = dict.serialize(4).expect("serialize");
        let dict2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert_eq!(dict2.len(), 100);
        for (i, v) in values.iter().enumerate() {
            assert_eq!(dict2.get(i), Some(v.as_str()), "ordinal {i}");
        }
    }

    // -- DoS regression test (24 h fuzz, 2026-05-03) -----------------------

    /// Regression for fuzz finding
    /// `fuzz_dict_deser/oom-cbbbc3c63d0d40cbba94aa7a6c4c8577a9cfb8e8`.
    ///
    /// 20-byte input claiming `num_values = 0xFFFE_FEFB` (≈ 4.29 G) would
    /// drive `Vec::<String>::with_capacity(num_values)` ≈ 100 GiB of
    /// up-front allocation, OOM-killing the process. The fix rejects any
    /// `num_values` that exceeds the strict structural upper bound
    /// `data.len() - 12` (each entry needs at least one VInt byte).
    #[test]
    fn deserialize_rejects_oversized_num_values() {
        let crash: &[u8] = &[
            0x01, 0x00, 0x00, 0x00, // version
            0x01, 0x00, 0x00, 0x00, // bucket_size
            0xfb, 0xfe, 0xfe, 0xff, // num_values ≈ 4.29 G
            0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, // body
        ];
        let result = FrontCodedDictionary::deserialize(crash);
        assert!(
            result.is_err(),
            "expected error for impossibly large num_values, got {result:?}"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("num_values") && msg.contains("strict upper bound"),
            "error should explain the cap: {msg}"
        );
    }

    /// Boundary: exactly `data.len() - 12` claimed entries is allowed
    /// (still rejected later by structural decode, but not pre-OOMed).
    #[test]
    fn deserialize_at_strict_upper_bound_does_not_oom() {
        // 20 B total → 8 B body → up to 8 entries are not pre-rejected.
        let mut buf = vec![
            0x01, 0x00, 0x00, 0x00, // version
            0x01, 0x00, 0x00, 0x00, // bucket_size
            0x08, 0x00, 0x00, 0x00, // num_values = 8
        ];
        buf.extend_from_slice(&[0u8; 8]); // 8 B of garbage body
        let _ = FrontCodedDictionary::deserialize(&buf); // either Ok or Err is fine; must not OOM
    }

    /// Regression for fuzz finding
    /// `fuzz_dict_deser/crash-299fd3c7670bd30fed3dd6eb45581a0f902d8f17`
    /// (41 B, discovered during the post-fix 5 min smoke run, 2026-05-03).
    ///
    /// Bucket layout encoded a 2-byte `Ą` (`0xC4 0x84`) as the first
    /// entry's payload, then a `prefix_len = 1` for the second entry.
    /// `&prev[..1]` is *inside* the 2-byte char and `str::push_str`
    /// panics with "byte index 1 is not a char boundary; it is inside
    /// 'Ą' (bytes 0..2 of string)" — an attacker-controllable process
    /// kill. The fix validates `prefix_len` against
    /// `prev.is_char_boundary` and surfaces an `Err` instead.
    #[test]
    fn deserialize_rejects_prefix_len_inside_multibyte_char() {
        let crash: &[u8] = &[
            0x01, 0x00, 0x00, 0x00, // version
            0x01, 0x00, 0x00, 0x13, // bucket_size = 0x13000001 — already absurd
            0x05, 0x00, 0x00, 0x00, // num_values = 5
            0x04, 0xc4, 0x84, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04,
            0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x00,
        ];
        // The artifact's bucket_size is malformed enough that the
        // strict-upper-bound check rejects it first. Build a smaller,
        // structurally-valid dictionary that *only* triggers the
        // char-boundary panic to pin the fix:
        let _ = FrontCodedDictionary::deserialize(crash); // must not panic

        // Hand-rolled minimal trigger: 1 bucket of size 2 with first
        // entry = "Ą" (2 B) and second entry's prefix_len = 1 (inside
        // the multibyte char).
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // version
        buf.extend_from_slice(&2u32.to_le_bytes()); // bucket_size = 2
        buf.extend_from_slice(&2u32.to_le_bytes()); // num_values = 2
        // bucket 0 starts at offset 0:
        // first entry: vint len=2, payload "Ą" (0xC4 0x84)
        buf.push(0x02);
        buf.extend_from_slice(&[0xc4, 0x84]);
        // second entry: prefix_len=1 (inside multibyte!), suffix_len=0
        buf.push(0x01);
        buf.push(0x00);
        // offset table: 1 bucket, offset 0
        buf.extend_from_slice(&0u32.to_le_bytes());
        let result = FrontCodedDictionary::deserialize(&buf);
        assert!(
            result.is_err(),
            "expected char-boundary rejection, got {result:?}"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("multi-byte"),
            "error should mention the boundary: {msg}"
        );
    }

    // -- front-coded v2 -----------------------------------------------------

    #[test]
    fn v2_round_trip_empty() {
        let dict = FrontCodedDictionary::new();
        let bytes = dict.serialize_v2(4).expect("serialize_v2");
        let dict2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert!(dict2.is_empty());
    }

    #[test]
    fn v2_round_trip_single_entry() {
        let dict = FrontCodedDictionary::from_sorted(vec!["hello".to_string()]);
        let bytes = dict.serialize_v2(4).expect("serialize_v2");
        let dict2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert_eq!(dict2.len(), 1);
        assert_eq!(dict2.get(0), Some("hello"));
    }

    #[test]
    fn v2_round_trip_with_empty_string_and_prefixes() {
        // Includes empty string (sorts first), shared prefixes, and unicode.
        let values = vec![
            String::new(),
            "café".to_string(),
            "cafés".to_string(),
            "naïve".to_string(),
            "naïveté".to_string(),
            "東京".to_string(),
            "東京都".to_string(),
        ];
        for &bs in &[1usize, 2, 4, 8] {
            let dict = FrontCodedDictionary::from_sorted(values.clone());
            let bytes = dict.serialize_v2(bs).expect("serialize_v2");
            // Marker must be version 2.
            assert_eq!(
                u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                2,
                "bucket_size {bs}"
            );
            let dict2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
            assert_eq!(dict2.len(), values.len(), "bucket_size {bs}");
            for (i, v) in values.iter().enumerate() {
                assert_eq!(
                    dict2.get(i),
                    Some(v.as_str()),
                    "bucket_size {bs} ordinal {i}"
                );
            }
        }
    }

    #[test]
    fn v2_single_bucket_and_multi_bucket() {
        // 3 values, bucket_size 4 → single bucket.
        let single = vec!["a".to_string(), "ab".to_string(), "abc".to_string()];
        let d = FrontCodedDictionary::from_sorted(single.clone());
        let b = d.serialize_v2(4).expect("serialize_v2");
        let d2 = FrontCodedDictionary::deserialize(&b).expect("deserialize");
        for (i, v) in single.iter().enumerate() {
            assert_eq!(d2.get(i), Some(v.as_str()));
        }

        // 17 values, bucket_size 4 → 5 buckets.
        let multi: Vec<String> = (0..17).map(|i| format!("region-{i:03}")).collect();
        let d = FrontCodedDictionary::from_sorted(multi.clone());
        let b = d.serialize_v2(4).expect("serialize_v2");
        let d2 = FrontCodedDictionary::deserialize(&b).expect("deserialize");
        assert_eq!(d2.len(), 17);
        for (i, v) in multi.iter().enumerate() {
            assert_eq!(d2.get(i), Some(v.as_str()), "ordinal {i}");
        }
    }

    #[test]
    fn v2_random_access_get_v2_matches_full_decode() {
        let values: Vec<String> = (0..50)
            .map(|i| format!("us-east-{i:04}-suffix-with-shared-prefix"))
            .collect();
        let dict = FrontCodedDictionary::from_sorted(values.clone());
        let bytes = dict.serialize_v2(8).expect("serialize_v2");

        // Random access to arbitrary index must match the values, including
        // across bucket boundaries and within buckets.
        for (i, v) in values.iter().enumerate() {
            let got = FrontCodedDictionary::get_v2(&bytes, i).expect("get_v2");
            assert_eq!(got.as_deref(), Some(v.as_str()), "ordinal {i}");
        }
        // Out of range.
        assert_eq!(
            FrontCodedDictionary::get_v2(&bytes, values.len()).expect("get_v2"),
            None
        );
    }

    #[test]
    fn v2_get_v2_rejects_v1_buffer() {
        let dict = FrontCodedDictionary::from_sorted(vec!["x".to_string()]);
        let v1 = dict.serialize(4).expect("serialize");
        assert!(FrontCodedDictionary::get_v2(&v1, 0).is_err());
    }

    #[test]
    fn v2_bucket_size_must_be_power_of_two() {
        let dict = FrontCodedDictionary::from_sorted(vec!["a".to_string(), "b".to_string()]);
        assert!(dict.serialize_v2(3).is_err());
        assert!(dict.serialize_v2(0).is_err());
        assert!(dict.serialize_v2(4).is_ok());
        assert!(dict.serialize_v2(1).is_ok());
    }

    #[test]
    fn v2_preserves_find_and_prefix_range() {
        let values = vec![
            "alpha".to_string(),
            "alpha-beta".to_string(),
            "alpha-gamma".to_string(),
            "beta".to_string(),
        ];
        let dict = FrontCodedDictionary::from_sorted(values);
        let bytes = dict.serialize_v2(2).expect("serialize_v2");
        let d2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert_eq!(d2.find("alpha-gamma"), Some(2));
        assert_eq!(d2.find("missing"), None);
        assert_eq!(d2.find_prefix_range("alpha"), 0..3);
    }

    /// Hand-verify decode of a small, hand-constructed v2 byte buffer.
    ///
    /// Dictionary: `["ab", "abc", "ax"]`, bucket_size = 4 → one bucket whose
    /// base is "ab". Within the bucket, "abc" shares prefix "ab" (len 2,
    /// suffix "c") and "ax" shares prefix "a" (len 1, suffix "x"), both
    /// relative to the BASE — that is the v2 distinction from v1 (where "ax"
    /// would be coded against "abc").
    #[test]
    fn v2_hand_constructed_buffer_decodes() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes()); // version = 2
        buf.extend_from_slice(&4u32.to_le_bytes()); // bucket_size = 4
        buf.extend_from_slice(&3u32.to_le_bytes()); // num_values = 3
        // bucket 0 at offset 0:
        //   base: vint len=2, "ab"
        buf.push(0x02);
        buf.extend_from_slice(b"ab");
        //   entry 1 "abc": prefix_len=2 (of base "ab"), suffix_len=1, "c"
        buf.push(0x02);
        buf.push(0x01);
        buf.push(b'c');
        //   entry 2 "ax": prefix_len=1 (of base "ab"), suffix_len=1, "x"
        buf.push(0x01);
        buf.push(0x01);
        buf.push(b'x');
        // offset table: 1 bucket at offset 0
        buf.extend_from_slice(&0u32.to_le_bytes());

        let dict = FrontCodedDictionary::deserialize(&buf).expect("deserialize hand buffer");
        assert_eq!(dict.len(), 3);
        assert_eq!(dict.get(0), Some("ab"));
        assert_eq!(dict.get(1), Some("abc"));
        assert_eq!(dict.get(2), Some("ax"));

        // Random access into the same buffer must agree.
        assert_eq!(
            FrontCodedDictionary::get_v2(&buf, 2)
                .expect("get_v2")
                .as_deref(),
            Some("ax")
        );
    }

    #[test]
    fn v2_rejects_non_power_of_two_bucket_size_on_decode() {
        // Header claims bucket_size = 3 (not a power of two).
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&3u32.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0x01);
        buf.push(b'a');
        buf.extend_from_slice(&0u32.to_le_bytes());
        assert!(FrontCodedDictionary::deserialize(&buf).is_err());
    }

    #[test]
    fn v2_rejects_oversized_num_values() {
        let crash: &[u8] = &[
            0x02, 0x00, 0x00, 0x00, // version 2
            0x04, 0x00, 0x00, 0x00, // bucket_size 4
            0xfb, 0xfe, 0xfe, 0xff, // num_values ≈ 4.29 G
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert!(FrontCodedDictionary::deserialize(crash).is_err());
    }

    #[test]
    fn v2_rejects_prefix_len_inside_multibyte_char() {
        // base = "Ą" (0xC4 0x84), then prefix_len = 1 (inside the char).
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes()); // version 2
        buf.extend_from_slice(&2u32.to_le_bytes()); // bucket_size 2
        buf.extend_from_slice(&2u32.to_le_bytes()); // num_values 2
        buf.push(0x02);
        buf.extend_from_slice(&[0xc4, 0x84]);
        buf.push(0x01); // prefix_len = 1 (inside Ą)
        buf.push(0x00); // suffix_len = 0
        buf.extend_from_slice(&0u32.to_le_bytes());
        let result = FrontCodedDictionary::deserialize(&buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("multi-byte"));
    }

    // -- boundary pins (mutation-gap closure) -------------------------------
    //
    // The tests below pin EXACT boundary conditions in the front-coded v1/v2
    // decoders and helpers. Each is designed so that the real comparison
    // passes while a flipped operator (`<` vs `<=`, `>` vs `>=`/`==`, `||` vs
    // `&&`, `+` vs `-`, etc.) would change the observed pass/fail result.

    /// `is_empty` must report `false` for a populated dictionary (pins the
    /// `is_empty -> true` mutant).
    #[test]
    fn is_empty_false_on_nonempty() {
        let dict = FrontCodedDictionary::from_sorted(vec!["x".to_string()]);
        assert!(!dict.is_empty());
        assert!(FrontCodedDictionary::new().is_empty());
    }

    /// `common_prefix_len` must return the true shared length, not 0. A v1
    /// re-encode then decode is exercised so that a `-> 0` mutant (every entry
    /// stored in full) still round-trips byte-for-byte, hence we pin the
    /// helper directly *and* via an encoding that depends on it.
    #[test]
    fn common_prefix_len_is_nonzero_for_shared_prefix() {
        assert_eq!(common_prefix_len(b"abcd", b"abxy"), 2);
        assert_eq!(common_prefix_len(b"abc", b"abc"), 3);
        assert_eq!(common_prefix_len(b"x", b"y"), 0);
        assert_eq!(common_prefix_len(b"", b"abc"), 0);

        // Pin it through serialization: with two entries sharing a 5-byte
        // prefix and bucket_size 4 (both in one bucket), the second entry's
        // encoded prefix_len byte must be 5, not 0. If common_prefix_len were
        // 0 the encoded suffix would be the whole string and the prefix_len
        // byte would be 0.
        let dict =
            FrontCodedDictionary::from_sorted(vec!["share".to_string(), "shared-tail".to_string()]);
        let bytes = dict.serialize(4).expect("serialize");
        // Header is 12 B; bucket 0 starts at offset 12.
        //   base: vint len=5 (0x05), "share"
        //   entry 1: vint prefix_len, vint suffix_len, suffix
        // prefix_len byte is at index 12 + 1 + 5 = 18.
        assert_eq!(bytes[12], 0x05, "base length vint");
        assert_eq!(&bytes[13..18], b"share");
        assert_eq!(
            bytes[18], 0x05,
            "prefix_len must equal true common prefix (5)"
        );
        // And it must round-trip regardless.
        let d2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert_eq!(d2.get(1), Some("shared-tail"));
    }

    /// `prefix_successor` increments the last non-0xFF byte. The `< 0xFF`
    /// guard (line ~735) must use strict `<`: a byte that is exactly `0xFF`
    /// must be dropped (and the preceding byte incremented), not incremented
    /// in place (which would overflow / wrap). The `<= 0xFF` mutant would
    /// treat a `0xFF` byte as incrementable.
    #[test]
    fn prefix_successor_drops_trailing_0xff() {
        // [0x41, 0xFF] -> drop 0xFF, increment 0x41 -> [0x42].
        assert_eq!(prefix_successor(&[0x41, 0xFF]), Some(vec![0x42]));
        // A non-0xFF last byte is simply incremented.
        assert_eq!(prefix_successor(&[0x41, 0x10]), Some(vec![0x41, 0x11]));
        // 0xFE is below 0xFF and must be incremented in place to 0xFF.
        assert_eq!(prefix_successor(&[0xFE]), Some(vec![0xFF]));
        // All-0xFF has no successor.
        assert_eq!(prefix_successor(&[0xFF, 0xFF]), None);
        // Empty has no successor.
        assert_eq!(prefix_successor(&[]), None);
    }

    /// `find_prefix_range` upper bound: a prefix whose last byte is `0xFF`
    /// inside the bytes forces `prefix_successor` to drop and carry. This
    /// exercises the `< 0xFF` boundary end-to-end through the public API.
    #[test]
    fn find_prefix_range_with_trailing_0xff_byte() {
        // Prefix "\u{ff}" encodes as bytes [0xC3, 0xBF]. Its byte successor
        // requires the `< 0xFF` carry logic in `prefix_successor`: the last
        // byte 0xBF (< 0xFF) is incremented to 0xC0, giving successor
        // [0xC3, 0xC0]. A `<= 0xFF` mutant would still produce the same value
        // *here*, so we additionally use a fixture whose UPPER bound lands
        // exactly on a value that begins with the incremented byte sequence,
        // and a `common_prefix_len -> 0` mutant cannot perturb this path.
        //
        // Byte-sorted fixture (verified below):
        //   "\u{ff}a"  = [0xC3, 0xBF, 0x61]
        //   "\u{ff}b"  = [0xC3, 0xBF, 0x62]
        //   "\u{100}"  = [0xC4, 0x80]   (does NOT start with [0xC3,0xBF])
        let values = vec![
            "\u{ff}a".to_string(),
            "\u{ff}b".to_string(),
            "\u{100}".to_string(),
        ];
        let mut sorted = values.clone();
        sorted.sort();
        assert_eq!(values, sorted, "fixture must be byte-sorted");

        let dict = FrontCodedDictionary::from_sorted(values);
        // Exactly the first two entries start with "\u{ff}".
        assert_eq!(dict.find_prefix_range("\u{ff}"), 0..2);
        // The broader single-byte view is consistent.
        for (ord, v) in dict.iter() {
            let in_range = (0..2).contains(&ord);
            assert_eq!(v.starts_with('\u{ff}'), in_range, "ordinal {ord}: {v:?}");
        }
    }

    // ---- get_v2 boundary pins --------------------------------------------

    /// Build a minimal but structurally valid v2 buffer for a single bucket
    /// from the given base/entries (entries are `(prefix_len, suffix)`).
    fn build_v2_single_bucket(
        bucket_size: u32,
        num_values: u32,
        base: &[u8],
        entries: &[(u8, &[u8])],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&bucket_size.to_le_bytes());
        buf.extend_from_slice(&num_values.to_le_bytes());
        // bucket 0 at offset 0
        write_vint(&mut buf, base.len() as u32);
        buf.extend_from_slice(base);
        for (prefix_len, suffix) in entries {
            write_vint(&mut buf, u32::from(*prefix_len));
            write_vint(&mut buf, suffix.len() as u32);
            buf.extend_from_slice(suffix);
        }
        // offset table: 1 bucket at offset 0
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf
    }

    /// `get_v2` header-length guard (`data.len() < 12`). A 12-byte v2 buffer
    /// with `num_values = 0` must be accepted (returns `None` for any
    /// ordinal). The `<= 12` / `== 12` mutants would wrongly reject it.
    #[test]
    fn get_v2_accepts_exact_12_byte_header() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes()); // version
        buf.extend_from_slice(&4u32.to_le_bytes()); // bucket_size (power of two)
        buf.extend_from_slice(&0u32.to_le_bytes()); // num_values = 0
        assert_eq!(buf.len(), 12);
        // ordinal >= num_values (0) → Ok(None); must NOT error on the header
        // length check.
        assert_eq!(FrontCodedDictionary::get_v2(&buf, 0).expect("get_v2"), None);
        // An 11-byte buffer must still error (pins the real guard fires).
        assert!(FrontCodedDictionary::get_v2(&buf[..11], 0).is_err());
    }

    /// `get_v2` bucket_size guard uses `||`: a non-zero, non-power-of-two
    /// bucket_size (3) must be rejected. The `&&` mutant would require BOTH
    /// `== 0` and `!is_power_of_two`, so bucket_size 3 (only the second holds)
    /// would slip through.
    #[test]
    fn get_v2_rejects_non_power_of_two_bucket_size() {
        // bucket_size = 3 (non-zero, not a power of two), num_values = 1.
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&3u32.to_le_bytes()); // bucket_size = 3
        buf.extend_from_slice(&1u32.to_le_bytes()); // num_values = 1
        write_vint(&mut buf, 1);
        buf.push(b'a');
        buf.extend_from_slice(&0u32.to_le_bytes()); // offset table
        let result = FrontCodedDictionary::get_v2(&buf, 0);
        assert!(
            result.is_err(),
            "bucket_size 3 must be rejected, got {result:?}"
        );
        assert!(result.unwrap_err().to_string().contains("power of two"));
        // bucket_size = 0 must also be rejected (the other `||` operand).
        let mut zero = buf.clone();
        zero[4..8].copy_from_slice(&0u32.to_le_bytes());
        assert!(FrontCodedDictionary::get_v2(&zero, 0).is_err());
    }

    /// `get_v2` strict-upper-bound guard (`num_values > max_entries`).
    ///
    /// Pins `>` against `>=` at the EXACT equality point. A decodable buffer
    /// cannot reach `num_values == max_entries` (every entry costs ≥1 body
    /// byte AND a 4-byte offset-table slot per bucket), so we use a buffer
    /// where `num_values == max_entries` exactly but the offset table is
    /// truncated. The original (`>`) lets it past this guard and the *next*
    /// guard reports "truncated"; the `>=` mutant would instead report the
    /// "strict upper bound" error here. Asserting the error message is
    /// "truncated" (not "strict upper bound") distinguishes them.
    #[test]
    fn get_v2_num_values_exactly_at_upper_bound_falls_through() {
        // data.len() = 20 → max_entries = 8. num_values = 8 (== max_entries).
        // bucket_size = 1 → num_buckets = 8 → 32 B table needed, but only
        // 20 B present → the offset-table guard fires (not the bound guard).
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes()); // version 2
        buf.extend_from_slice(&1u32.to_le_bytes()); // bucket_size 1
        buf.extend_from_slice(&8u32.to_le_bytes()); // num_values 8
        buf.extend_from_slice(&[0u8; 8]); // body
        assert_eq!(buf.len(), 20);
        let result = FrontCodedDictionary::get_v2(&buf, 0);
        assert!(
            result.is_err(),
            "must error (truncated table), got {result:?}"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("truncated") && !msg.contains("strict upper bound"),
            "at num_values == max_entries the bound guard must NOT fire; got: {msg}"
        );

        // And a comfortably-bounded buffer decodes (ACCEPT side of the guard).
        let ok = build_v2_single_bucket(1, 1, b"hi", &[]);
        assert_eq!(
            FrontCodedDictionary::get_v2(&ok, 0)
                .expect("get_v2")
                .as_deref(),
            Some("hi")
        );
    }

    /// `get_v2` strict-upper-bound guard reject side: `num_values` larger than
    /// the structural maximum must error (pins `>` against `==`, which would
    /// only fire on exact equality and let a strictly-larger value through).
    #[test]
    fn get_v2_rejects_oversized_num_values() {
        // Tiny buffer, num_values claimed = 1000 (>> data.len()-12).
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&4u32.to_le_bytes());
        buf.extend_from_slice(&1000u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 8]);
        let result = FrontCodedDictionary::get_v2(&buf, 0);
        assert!(
            result.is_err(),
            "oversized num_values must error, got {result:?}"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("strict upper bound")
        );
    }

    /// `get_v2` offset-table truncation guard (`data.len() < 12 + offsets`).
    /// A buffer truncated so the offset table is incomplete must error; a
    /// buffer with a complete table at the same logical size must not error on
    /// this check. Pins `<` against `==`/`<=`.
    #[test]
    fn get_v2_rejects_truncated_offset_table() {
        // num_values = 5, bucket_size = 1 → 5 buckets → 20 B offset table.
        // Provide a buffer too short to hold it.
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes()); // bucket_size 1
        buf.extend_from_slice(&5u32.to_le_bytes()); // num_values 5 → 5 buckets
        // Only 8 body bytes — far short of 12 + 20 = 32 needed, but enough to
        // pass the strict-upper-bound check (max_entries = 8 ≥ 5).
        buf.extend_from_slice(&[0u8; 8]);
        assert_eq!(buf.len(), 20); // < 32
        let result = FrontCodedDictionary::get_v2(&buf, 0);
        assert!(
            result.is_err(),
            "truncated table must error, got {result:?}"
        );
        assert!(result.unwrap_err().to_string().contains("truncated"));
    }

    /// `get_v2` bucket-base overflow guard (`end > offsets_start`): a base
    /// length that runs past the start of the offset table must error. Pins
    /// `>` against `==`/`>=` by making `end` strictly exceed `offsets_start`.
    #[test]
    fn get_v2_rejects_base_overflowing_offset_table() {
        // 1 bucket, base claims len = 100 but only a couple of bytes exist.
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&4u32.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes()); // num_values 1
        write_vint(&mut buf, 100); // base len = 100 (overflows)
        buf.push(b'a'); // only 1 byte of payload
        buf.extend_from_slice(&0u32.to_le_bytes()); // offset table
        let result = FrontCodedDictionary::get_v2(&buf, 0);
        assert!(result.is_err(), "base overflow must error, got {result:?}");
        assert!(result.unwrap_err().to_string().contains("overflows"));
    }

    /// `get_v2` target-entry overflow guard via the base-relative prefix_len
    /// check (`prefix_len > base.len()`). A target entry whose `prefix_len`
    /// exceeds the bucket base length must error. Pins `>` against `==`/`>=`:
    /// `prefix_len == base.len()` (the full base) is VALID and must decode,
    /// while `prefix_len == base.len() + 1` must error.
    #[test]
    fn get_v2_prefix_len_equal_base_len_is_valid() {
        // base "ab" (len 2). Entry uses prefix_len = 2 (== base.len()),
        // suffix "c" → reconstructed "abc". Must decode OK (not error).
        let ok = build_v2_single_bucket(4, 2, b"ab", &[(2, b"c")]);
        assert_eq!(
            FrontCodedDictionary::get_v2(&ok, 1)
                .expect("get_v2")
                .as_deref(),
            Some("abc")
        );
        // prefix_len = 3 > base.len() 2 must error.
        let bad = build_v2_single_bucket(4, 2, b"ab", &[(3, b"c")]);
        let result = FrontCodedDictionary::get_v2(&bad, 1);
        assert!(
            result.is_err(),
            "prefix_len > base.len() must error, got {result:?}"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("exceeds bucket-base length")
        );
    }

    /// `get_v2` ordinal range guard (`ordinal >= num_values`). The last valid
    /// ordinal (`num_values - 1`) must decode; `num_values` itself must return
    /// `None`. This pins the `>=` against `>`/`==` at the exact upper edge.
    #[test]
    fn get_v2_ordinal_at_exact_bounds() {
        let values: Vec<String> = (0..9).map(|i| format!("v-{i}")).collect();
        let dict = FrontCodedDictionary::from_sorted(values.clone());
        let bytes = dict.serialize_v2(4).expect("serialize_v2");
        let last = values.len() - 1;
        // Last valid ordinal decodes to the last value.
        assert_eq!(
            FrontCodedDictionary::get_v2(&bytes, last)
                .expect("get_v2")
                .as_deref(),
            Some(values[last].as_str())
        );
        // Exactly num_values → None.
        assert_eq!(
            FrontCodedDictionary::get_v2(&bytes, values.len()).expect("get_v2"),
            None
        );
        // Ordinal 0 decodes to the first value.
        assert_eq!(
            FrontCodedDictionary::get_v2(&bytes, 0)
                .expect("get_v2")
                .as_deref(),
            Some(values[0].as_str())
        );
        // First ordinal of the SECOND bucket (index == bucket_size) decodes.
        assert_eq!(
            FrontCodedDictionary::get_v2(&bytes, 4)
                .expect("get_v2")
                .as_deref(),
            Some(values[4].as_str())
        );
    }

    // ---- deserialize_v1 / deserialize_v2 boundary pins -------------------

    /// `deserialize_v1` strict-upper-bound guard (`num_values > max_entries`).
    /// A v1 dict where `num_values == max_entries` exactly must decode (pins
    /// `>` against `>=`), while `num_values == max_entries + 1` must error
    /// (pins `>` against `==`).
    #[test]
    fn deserialize_v1_num_values_at_strict_upper_bound() {
        // Build a real v1 dict, then check its num_values vs max_entries.
        // Use single-character entries so each takes exactly the minimum
        // encoding and num_values approaches max_entries.
        // 4 entries "a","b","c","d", bucket_size 1 → 4 buckets.
        let values = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let dict = FrontCodedDictionary::from_sorted(values.clone());
        let bytes = dict.serialize(1).expect("serialize");
        // This decodes fine; num_values (4) < max_entries here. Verify decode.
        let d2 = FrontCodedDictionary::deserialize(&bytes).expect("deserialize");
        assert_eq!(d2.len(), 4);

        // Now craft a buffer where num_values == max_entries exactly and the
        // structure is still decodable. With bucket_size 1, num_buckets =
        // num_values and the offset table is 4*num_values bytes. The smallest
        // body per entry is 1 byte (vint len=0, empty string). So for N empty
        // entries: data.len() = 12 + N (bodies) + 4N (table) = 12 + 5N, and
        // max_entries = data.len() - 12 = 5N. num_values = N. For N>=1,
        // N < 5N, so exact equality is unreachable for a real dict. Instead
        // pin the ACCEPT side: a buffer where num_values is comfortably below
        // the bound must decode (already covered), and pin the REJECT side
        // (num_values > max_entries) explicitly.
        let mut over = vec![
            0x01, 0x00, 0x00, 0x00, // version 1
            0x01, 0x00, 0x00, 0x00, // bucket_size 1
        ];
        over.extend_from_slice(&7u32.to_le_bytes()); // num_values 7
        over.extend_from_slice(&[0u8; 6]); // only 6 body bytes → max_entries 6 < 7
        let result = FrontCodedDictionary::deserialize(&over);
        assert!(
            result.is_err(),
            "num_values 7 > max 6 must error, got {result:?}"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("strict upper bound")
        );

        // Equality point (`num_values == max_entries`) pins `>` against `>=`.
        // data.len() = 20 → max_entries = 8; num_values = 8, bucket_size 1 →
        // 8 buckets → 32 B table needed but absent, so the *truncated-table*
        // guard fires, NOT the strict-upper-bound guard. A `>=` mutant would
        // instead emit "strict upper bound" at the equality point.
        let mut eq = vec![
            0x01, 0x00, 0x00, 0x00, // version 1
            0x01, 0x00, 0x00, 0x00, // bucket_size 1
        ];
        eq.extend_from_slice(&8u32.to_le_bytes()); // num_values 8 == max_entries
        eq.extend_from_slice(&[0u8; 8]);
        assert_eq!(eq.len(), 20);
        let eq_res = FrontCodedDictionary::deserialize(&eq);
        assert!(eq_res.is_err());
        let eq_msg = eq_res.unwrap_err().to_string();
        assert!(
            eq_msg.contains("truncated") && !eq_msg.contains("strict upper bound"),
            "num_values == max_entries must pass the bound guard; got: {eq_msg}"
        );
    }

    /// `deserialize_v1` offset-table truncation guard
    /// (`data.len() < 12 + offsets`): a buffer one byte short of holding the
    /// full offset table must error (pins `<` against `==`/`<=` on the reject
    /// side), while a buffer of exactly the right size for a real dict decodes.
    #[test]
    fn deserialize_v1_truncated_offset_table_errors() {
        // num_values = 3, bucket_size 1 → 3 buckets → 12 B table. Provide a
        // buffer that passes the strict-upper-bound check but is too short for
        // the table.
        let mut buf = vec![
            0x01, 0x00, 0x00, 0x00, // version 1
            0x01, 0x00, 0x00, 0x00, // bucket_size 1
        ];
        buf.extend_from_slice(&3u32.to_le_bytes()); // num_values 3
        // Need 12 + 12 = 24 B; provide only 12 + 8 = 20 (max_entries 8 >= 3).
        buf.extend_from_slice(&[0u8; 8]);
        assert_eq!(buf.len(), 20);
        let result = FrontCodedDictionary::deserialize(&buf);
        assert!(
            result.is_err(),
            "truncated table must error, got {result:?}"
        );
        assert!(result.unwrap_err().to_string().contains("truncated"));
    }

    /// `deserialize_v1` bucket-data / entry overflow guards (`pos + len`,
    /// `pos + suffix_len`). A base length that runs past `offsets_start` must
    /// error; the addition (`+`) is pinned because `pos - len` would not
    /// exceed `offsets_start` and the overflow would go undetected.
    #[test]
    fn deserialize_v1_base_and_suffix_overflow_errors() {
        // Single bucket, base claims len = 50 but data is short.
        let mut buf = vec![
            0x01, 0x00, 0x00, 0x00, // version 1
            0x04, 0x00, 0x00, 0x00, // bucket_size 4
            0x01, 0x00, 0x00, 0x00, // num_values 1
        ];
        write_vint(&mut buf, 50); // base len = 50, far past the data
        buf.push(b'a');
        buf.extend_from_slice(&0u32.to_le_bytes()); // offset table
        let result = FrontCodedDictionary::deserialize(&buf);
        assert!(result.is_err(), "base overflow must error, got {result:?}");
        assert!(result.unwrap_err().to_string().contains("overflows"));

        // Now an entry-suffix overflow: base "ab", second entry suffix_len 50.
        let mut buf2 = vec![
            0x01, 0x00, 0x00, 0x00, // version 1
            0x04, 0x00, 0x00, 0x00, // bucket_size 4
            0x02, 0x00, 0x00, 0x00, // num_values 2
        ];
        write_vint(&mut buf2, 2); // base len 2
        buf2.extend_from_slice(b"ab");
        write_vint(&mut buf2, 1); // prefix_len 1
        write_vint(&mut buf2, 50); // suffix_len 50 (overflows)
        buf2.push(b'x');
        buf2.extend_from_slice(&0u32.to_le_bytes());
        let result2 = FrontCodedDictionary::deserialize(&buf2);
        assert!(
            result2.is_err(),
            "suffix overflow must error, got {result2:?}"
        );
        assert!(result2.unwrap_err().to_string().contains("overflows"));
    }

    /// `deserialize_v2` strict-upper-bound reject side (`num_values >
    /// max_entries`). Pins `>` against `==`/`>=`: a strictly-too-large
    /// num_values must error.
    #[test]
    fn deserialize_v2_rejects_oversized_num_values_tight() {
        let mut buf = vec![
            0x02, 0x00, 0x00, 0x00, // version 2
            0x04, 0x00, 0x00, 0x00, // bucket_size 4
        ];
        buf.extend_from_slice(&9u32.to_le_bytes()); // num_values 9
        buf.extend_from_slice(&[0u8; 8]); // 8 body bytes → max_entries 8 < 9
        let result = FrontCodedDictionary::deserialize(&buf);
        assert!(
            result.is_err(),
            "num_values 9 > max 8 must error, got {result:?}"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("strict upper bound")
        );
        // And a value within the bound (num_values 8) is NOT rejected by THIS
        // guard (it may still fail structurally, but not with the bound msg).
        buf[8..12].copy_from_slice(&8u32.to_le_bytes());
        let within = FrontCodedDictionary::deserialize(&buf);
        if let Err(e) = within {
            assert!(
                !e.to_string().contains("strict upper bound"),
                "num_values == max_entries must pass the bound guard: {e}"
            );
        }
    }

    /// `deserialize_v2` offset-table truncation guard
    /// (`data.len() < 12 + offsets`). A buffer too short for the offset table
    /// must error (pins `<` against `==`/`<=`).
    #[test]
    fn deserialize_v2_truncated_offset_table_errors() {
        let mut buf = vec![
            0x02, 0x00, 0x00, 0x00, // version 2
            0x01, 0x00, 0x00, 0x00, // bucket_size 1 → num_buckets = num_values
        ];
        buf.extend_from_slice(&4u32.to_le_bytes()); // num_values 4 → 16 B table
        buf.extend_from_slice(&[0u8; 8]); // 8 body bytes; need 12+16=28, have 20
        assert_eq!(buf.len(), 20);
        let result = FrontCodedDictionary::deserialize(&buf);
        assert!(
            result.is_err(),
            "truncated v2 table must error, got {result:?}"
        );
        assert!(result.unwrap_err().to_string().contains("truncated"));
    }

    /// Offset-table size guard at the EXACT equality point
    /// (`data.len() == 12 + offsets_byte_len`), pinning the strict `<` in
    /// `deserialize_v1`, `deserialize_v2`, and `get_v2` against a `<=` mutant.
    ///
    /// A 16-byte buffer with `num_values = 1`, `bucket_size = 1` has exactly
    /// one bucket → a 4-byte offset table → `data.len() == 12 + 4`, so the
    /// offset table occupies bytes `[12, 16)` and there are ZERO bucket-data
    /// bytes. No valid dictionary can sit at this point (the bucket base needs
    /// at least its vint length byte), so the buffer must be REJECTED — but
    /// the precise guard that fires distinguishes the operator:
    ///   * original `<`: the offset-table size guard PASSES (len is exactly
    ///     sufficient); the DD R17 offset-table integrity check then rejects the
    ///     sole offset `0` as outside the 0-byte bucket-data region.
    ///   * `<=` mutant: the offset-table size guard itself fires with "truncated".
    ///
    /// Asserting the error is NOT "truncated" (it is the bucket-data "region"
    /// error) kills the `<=` mutant.
    #[test]
    fn offset_table_at_exact_size_boundary_reports_overflow_not_truncation() {
        // v2 variant.
        let mut v2 = Vec::new();
        v2.extend_from_slice(&2u32.to_le_bytes()); // version 2
        v2.extend_from_slice(&1u32.to_le_bytes()); // bucket_size 1
        v2.extend_from_slice(&1u32.to_le_bytes()); // num_values 1
        v2.extend_from_slice(&0u32.to_le_bytes()); // offset table: bucket 0 at 0
        assert_eq!(v2.len(), 16); // == 12 + 4
        let err = FrontCodedDictionary::deserialize(&v2)
            .expect_err("equality-point buffer must be rejected")
            .to_string();
        assert!(
            err.contains("region") && !err.contains("truncated"),
            "original `<` must pass the size guard and fail the offset-table check: {err}"
        );
        // get_v2 takes the same `<` path and must report the same way.
        let gerr = FrontCodedDictionary::get_v2(&v2, 0)
            .expect_err("get_v2 equality-point buffer must be rejected")
            .to_string();
        assert!(
            gerr.contains("region") && !gerr.contains("truncated"),
            "get_v2 original `<` must pass the size guard and fail the offset-table check: {gerr}"
        );

        // v1 variant (only the version marker differs).
        let mut v1 = v2.clone();
        v1[0..4].copy_from_slice(&1u32.to_le_bytes()); // version 1
        let err1 = FrontCodedDictionary::deserialize(&v1)
            .expect_err("v1 equality-point buffer must be rejected")
            .to_string();
        assert!(
            err1.contains("region") && !err1.contains("truncated"),
            "v1 original `<` must pass the size guard and fail the offset-table check: {err1}"
        );
    }

    #[test]
    fn reused_bucket_offset_rejected_v2() {
        // DD R17: a v2 buffer whose offset table reuses an offset (`[0, 0]`)
        // would decode bucket 1 from bucket 0's bytes and silently return
        // `["a", "a"]` instead of `["a", "b"]`. It must be rejected fail-fast.
        let dict = FrontCodedDictionary::from_sorted(vec!["a".to_string(), "b".to_string()]);
        // bucket_size = 1 forces two single-entry buckets -> offset table [0, N].
        let good = dict.serialize_v2(1).expect("serialize");
        // Sanity: the well-formed buffer round-trips correctly.
        let rt = FrontCodedDictionary::deserialize(&good).expect("deserialize good");
        assert_eq!(rt.values, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(
            FrontCodedDictionary::get_v2(&good, 1).expect("get good"),
            Some("b".to_string())
        );

        // Corrupt the second (last) offset to 0 -> table becomes [0, 0].
        let mut bad = good.clone();
        let n = bad.len();
        bad[n - 4..n].copy_from_slice(&0u32.to_le_bytes());

        let derr = FrontCodedDictionary::deserialize(&bad)
            .expect_err("reused-offset buffer must be rejected by deserialize")
            .to_string();
        assert!(
            derr.contains("strictly increasing") || derr.contains("corrupt offset table"),
            "expected an offset-table error, got: {derr}"
        );
        let gerr = FrontCodedDictionary::get_v2(&bad, 1)
            .expect_err("reused-offset buffer must be rejected by get_v2")
            .to_string();
        assert!(
            gerr.contains("strictly increasing") || gerr.contains("corrupt offset table"),
            "expected an offset-table error from get_v2, got: {gerr}"
        );
    }

    #[test]
    fn interior_bucket_offset_rejected_get_v2() {
        // DD R18: a strictly-increasing offset table whose later offset points
        // INTO an earlier bucket (e.g. `[0, 1]`) passes validate_bucket_offsets
        // (start 0, increasing, in-range) but must not let the random-access
        // `get_v2` fail-open by decoding a fake bucket base. bucket_size = 2
        // forces two buckets ([a,b] then [c]).
        let dict = FrontCodedDictionary::from_sorted(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]);
        let good = dict.serialize_v2(2).expect("serialize");
        // Sanity: the well-formed buffer reads bucket 1 correctly.
        assert_eq!(
            FrontCodedDictionary::get_v2(&good, 2).expect("get good"),
            Some("c".to_string())
        );

        // Corrupt the last offset (bucket 1) to 1 -> [0, 1], interior of bucket 0.
        let mut bad = good.clone();
        let n = bad.len();
        bad[n - 4..n].copy_from_slice(&1u32.to_le_bytes());

        let gerr = FrontCodedDictionary::get_v2(&bad, 2)
            .expect_err("interior offset must be rejected by get_v2")
            .to_string();
        assert!(
            gerr.contains("corrupt offset table") || gerr.contains("overflows"),
            "expected an offset-boundary error from get_v2, got: {gerr}"
        );
        // The full deserializer already rejects it via its exact-consume check.
        assert!(FrontCodedDictionary::deserialize(&bad).is_err());
    }

    #[test]
    fn target_bucket_overrun_rejected_get_v2() {
        // DD R19: the target bucket itself must consume exactly to its declared
        // next offset. With bucket_size=1, values ["a","b","c"], a corrupt table
        // `[0, 2, 3]` is strictly increasing and in range, and the buckets before
        // the target verify, but bucket 1's declared next offset (3) lands inside
        // bucket 1's own record (it really ends at 4). get_v2 must reject rather
        // than return a value from a mis-bounded bucket.
        let dict = FrontCodedDictionary::from_sorted(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]);
        let good = dict.serialize_v2(1).expect("serialize");
        // Sanity: well-formed reads are correct for every ordinal.
        assert_eq!(
            FrontCodedDictionary::get_v2(&good, 1).expect("get good"),
            Some("b".to_string())
        );
        assert_eq!(
            FrontCodedDictionary::get_v2(&good, 2).expect("get good last"),
            Some("c".to_string())
        );

        // Overwrite the offset table (last 12 bytes = 3 × u32) with [0, 2, 3].
        let mut bad = good.clone();
        let n = bad.len();
        bad[n - 12..n - 8].copy_from_slice(&0u32.to_le_bytes());
        bad[n - 8..n - 4].copy_from_slice(&2u32.to_le_bytes());
        bad[n - 4..n].copy_from_slice(&3u32.to_le_bytes());

        let gerr = FrontCodedDictionary::get_v2(&bad, 1)
            .expect_err("target-bucket overrun must be rejected by get_v2")
            .to_string();
        assert!(
            gerr.contains("corrupt offset table") || gerr.contains("overflows"),
            "expected a target-bucket boundary error from get_v2, got: {gerr}"
        );
        assert!(FrontCodedDictionary::deserialize(&bad).is_err());
    }
}
