// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Column types and descriptors for Druid segment v9.
//!
//! Each column in a segment has a JSON descriptor (`<col>.column_descriptor.json`)
//! that records the value type and feature flags.  The actual column data is
//! stored in codec-specific binary formats.

use ferrodruid_bitmap::DruidBitmap;
use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_dict::FrontCodedDictionary;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Reader bounds (Wave 36-D / R1: defeat OOM via crafted segment files)
// ---------------------------------------------------------------------------

/// Hard upper bound on the row count of a single string column.
///
/// Druid segments target ~5M rows by convention; we cap at 64M to leave
/// significant headroom while still preventing a malicious 4-byte
/// `u32::MAX` from triggering a multi-GB allocation. See Wave 35 R1
/// (`column.rs:265`).
const MAX_STRING_COLUMN_ROWS: usize = 64 * 1024 * 1024;

/// Hard upper bound on the serialized dictionary blob length (bytes).
///
/// 256 MiB is well past any realistic Druid string dictionary; an attacker
/// supplying `u32::MAX` would otherwise force a 4 GiB slice index attempt.
const MAX_STRING_DICT_BYTES: usize = 256 * 1024 * 1024;

/// Hard upper bound on the number of bitmap indexes attached to a string
/// column. One bitmap exists per dictionary entry, so this cap is also a
/// proxy for "max distinct values in a string column".
const MAX_STRING_BITMAPS: usize = 16 * 1024 * 1024;

/// Hard upper bound on the serialized length of a single bitmap (bytes).
const MAX_BITMAP_BYTES: usize = 64 * 1024 * 1024;

/// Hard upper bound on the row count of a single numeric (long/float/double)
/// column. The string-column path was hardened in Wave 35; the numeric
/// decoders previously trusted the on-disk header and called
/// `Vec::with_capacity(num)` directly, allowing a crafted segment file to
/// force multi-GiB pre-allocations as long as the file was correspondingly
/// large. 256M elements (≈2 GiB at 8 bytes/value) is well above any
/// realistic Druid column while still rejecting `u32::MAX`-style headers.
/// See Wave 37 R1 (`column.rs:145-245`).
const MAX_NUMERIC_COLUMN_VALUES: usize = 256 * 1024 * 1024;

// ---------------------------------------------------------------------------
// ColumnDescriptor
// ---------------------------------------------------------------------------

/// Column descriptor parsed from the JSON metadata inside a smoosh archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnDescriptor {
    /// The value type: `"LONG"`, `"FLOAT"`, `"DOUBLE"`, `"STRING"`, or `"COMPLEX"`.
    pub value_type: String,

    /// Whether the column contains multi-valued rows (string dimensions only).
    #[serde(default)]
    pub has_multiple_values: bool,

    /// Whether bitmap indexes are present (one bitmap per dictionary value).
    #[serde(default)]
    pub has_bitmap_indexes: bool,

    /// Whether spatial indexes are present.
    #[serde(default)]
    pub has_spatial_indexes: bool,
}

impl ColumnDescriptor {
    /// Parse a [`ColumnDescriptor`] from a JSON byte slice.
    pub fn from_json(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data).map_err(|e| {
            DruidError::Segment(format!("failed to parse column descriptor JSON: {e}"))
        })
    }
}

// ---------------------------------------------------------------------------
// Null-value convention (in-memory)
// ---------------------------------------------------------------------------
//
// FerroDruid stores SQL NULL *in band* so that the in-memory column model
// (whose shape is relied upon across the workspace) does not change:
//
// * `DOUBLE` / `FLOAT` columns: NULL is stored as NaN.  JSON input cannot
//   produce NaN, so a NaN value in a column ingested from JSON always means
//   NULL.  (A genuine NaN arriving through a non-JSON path is
//   indistinguishable from NULL — documented limitation.)
// * `LONG` columns cannot represent NULL in band (every `i64` bit pattern is
//   a valid value).  Ingestion therefore stores null-bearing long-typed
//   columns as `DOUBLE` with NaN nulls; a null-free long column stays `LONG`.
// * `STRING` columns: see [`StringColumnData::null_rows`] — a trailing
//   null-row bitmap appended past the dictionary cardinality records which
//   rows are NULL (distinct from `""`).

/// The in-memory NULL marker for `DOUBLE` columns.
pub const NULL_DOUBLE: f64 = f64::NAN;

/// The in-memory NULL marker for `FLOAT` columns.
pub const NULL_FLOAT: f32 = f32::NAN;

/// Whether an in-memory `DOUBLE` value represents SQL NULL (any NaN).
#[must_use]
pub fn is_null_double(v: f64) -> bool {
    v.is_nan()
}

/// Whether an in-memory `FLOAT` value represents SQL NULL (any NaN).
#[must_use]
pub fn is_null_float(v: f32) -> bool {
    v.is_nan()
}

// ---------------------------------------------------------------------------
// ColumnData
// ---------------------------------------------------------------------------

/// A column of data read from a Druid segment.
#[derive(Debug, Clone)]
pub enum ColumnData {
    /// 64-bit signed integer column.
    Long(Vec<i64>),
    /// 32-bit floating-point column.
    Float(Vec<f32>),
    /// 64-bit floating-point column.
    Double(Vec<f64>),
    /// String (dimension) column with dictionary, encoded ordinals, and bitmaps.
    String(StringColumnData),
    /// Complex column (opaque bytes for caller-defined types like sketches).
    Complex(Vec<u8>),
}

impl ColumnData {
    /// Return the number of rows in this column, if known.
    ///
    /// For numeric and string columns the length is derived from the value
    /// arrays.  Complex columns do not carry explicit row count information.
    pub fn num_rows(&self) -> Option<usize> {
        match self {
            Self::Long(v) => Some(v.len()),
            Self::Float(v) => Some(v.len()),
            Self::Double(v) => Some(v.len()),
            Self::String(s) => Some(s.encoded_values.len()),
            Self::Complex(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// StringColumnData
// ---------------------------------------------------------------------------

/// Data for a string (dimension) column.
///
/// # Null representation
///
/// A column that contains SQL NULL rows (distinct from `""`) carries **one
/// extra trailing bitmap** in [`bitmap_indexes`](Self::bitmap_indexes):
/// `bitmap_indexes.len() == dictionary.len() + 1`, where the last bitmap is
/// the set of NULL rows.  NULL rows' `encoded_values` point at the `""`
/// dictionary entry (which is guaranteed to exist in null-bearing columns)
/// so every ordinal stays within dictionary range, but the `""` entry's own
/// value bitmap **excludes** the NULL rows — a selector filter on `""` must
/// not match NULL.  Null-free columns keep the historical 1-bitmap-per-entry
/// layout byte-for-byte, so existing segments are unaffected.
#[derive(Debug, Clone)]
pub struct StringColumnData {
    /// Sorted dictionary of unique values.
    pub dictionary: FrontCodedDictionary,
    /// Per-row ordinals into the dictionary.
    pub encoded_values: Vec<u32>,
    /// One bitmap per dictionary value indicating which rows contain it,
    /// plus (for null-bearing columns only) one trailing null-row bitmap.
    pub bitmap_indexes: Vec<DruidBitmap>,
}

impl StringColumnData {
    /// Build a string column from per-row nullable values.
    ///
    /// `None` entries are stored as SQL NULL: they are recorded in a trailing
    /// null-row bitmap and their ordinals point at a `""` dictionary entry
    /// (inserted if absent) whose value bitmap excludes them.  When no `None`
    /// is present, the output is identical to the historical null-unaware
    /// construction (sorted dictionary, one bitmap per entry).
    #[must_use]
    pub fn from_nullable_values(values: &[Option<String>]) -> Self {
        use std::collections::{BTreeSet, HashMap};

        // Single fused pass for the null flag + the unique value set (a
        // separate `any(is_none)` pre-pass measurably slowed 1M-row ingest).
        let mut has_null = false;
        let mut unique: BTreeSet<&str> = BTreeSet::new();
        for value in values {
            match value {
                Some(s) => {
                    unique.insert(s.as_str());
                }
                None => has_null = true,
            }
        }
        // The "" placeholder gives NULL rows an in-range ordinal to point at.
        if has_null {
            unique.insert("");
        }
        let sorted: Vec<String> = unique.iter().map(|s| (*s).to_string()).collect();
        let ordinal_of: HashMap<&str, u32> = sorted
            .iter()
            .enumerate()
            .map(|(i, v)| (v.as_str(), i as u32))
            .collect();
        // "" sorts before every other string, but resolve it defensively.
        let null_ord: u32 = if has_null { ordinal_of[""] } else { 0 };

        let mut bitmaps: Vec<DruidBitmap> = (0..sorted.len()).map(|_| DruidBitmap::new()).collect();
        let mut null_bitmap = DruidBitmap::new();
        let mut encoded_values: Vec<u32> = Vec::with_capacity(values.len());

        for (row_idx, value) in values.iter().enumerate() {
            match value {
                Some(s) => {
                    let ord = ordinal_of[s.as_str()];
                    encoded_values.push(ord);
                    bitmaps[ord as usize].insert(row_idx as u32);
                }
                None => {
                    encoded_values.push(null_ord);
                    null_bitmap.insert(row_idx as u32);
                }
            }
        }
        if has_null {
            bitmaps.push(null_bitmap);
        }

        Self {
            dictionary: FrontCodedDictionary::from_sorted(sorted),
            encoded_values,
            bitmap_indexes: bitmaps,
        }
    }

    /// The NULL-row bitmap, if this column carries one.
    ///
    /// Returns `Some` iff `bitmap_indexes.len() == dictionary.len() + 1`
    /// (the null-bearing layout); the returned bitmap is the set of row
    /// indexes whose value is SQL NULL.  Null-free columns return `None`.
    #[must_use]
    pub fn null_rows(&self) -> Option<&DruidBitmap> {
        if self.bitmap_indexes.len() == self.dictionary.len() + 1 {
            self.bitmap_indexes.last()
        } else {
            None
        }
    }

    /// Whether the value at `row_idx` is SQL NULL (distinct from `""`).
    #[must_use]
    pub fn is_null_row(&self, row_idx: usize) -> bool {
        match (self.null_rows(), u32::try_from(row_idx)) {
            (Some(bm), Ok(row)) => bm.contains(row),
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Synthetic builders (for testing)
// ---------------------------------------------------------------------------

/// Build a synthetic long-column binary blob.
///
/// Format: `[4 bytes BE: num_values][8 bytes BE × num_values: values]`
pub fn encode_long_column(values: &[i64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + values.len() * 8);
    buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
    for &v in values {
        buf.extend_from_slice(&v.to_be_bytes());
    }
    buf
}

/// Decode a long column from the synthetic binary format.
pub fn decode_long_column(data: &[u8]) -> Result<Vec<i64>> {
    if data.len() < 4 {
        return Err(DruidError::Segment("long column too short".to_string()));
    }
    let num = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if num > MAX_NUMERIC_COLUMN_VALUES {
        return Err(DruidError::Segment(format!(
            "long column: declared length {num} exceeds cap {MAX_NUMERIC_COLUMN_VALUES}"
        )));
    }
    let expected = num
        .checked_mul(8)
        .and_then(|n| n.checked_add(4))
        .ok_or_else(|| DruidError::Segment("long column: declared length overflows".to_string()))?;
    if data.len() < expected {
        return Err(DruidError::Segment(format!(
            "long column truncated: need {expected} bytes, have {}",
            data.len()
        )));
    }
    let mut values = Vec::with_capacity(num);
    for i in 0..num {
        let off = 4 + i * 8;
        let v = i64::from_be_bytes([
            data[off],
            data[off + 1],
            data[off + 2],
            data[off + 3],
            data[off + 4],
            data[off + 5],
            data[off + 6],
            data[off + 7],
        ]);
        values.push(v);
    }
    Ok(values)
}

/// Build a synthetic float-column binary blob.
///
/// Format: `[4 bytes BE: num_values][4 bytes BE × num_values: values]`
pub fn encode_float_column(values: &[f32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + values.len() * 4);
    buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
    for &v in values {
        buf.extend_from_slice(&v.to_be_bytes());
    }
    buf
}

/// Decode a float column from the synthetic binary format.
pub fn decode_float_column(data: &[u8]) -> Result<Vec<f32>> {
    if data.len() < 4 {
        return Err(DruidError::Segment("float column too short".to_string()));
    }
    let num = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if num > MAX_NUMERIC_COLUMN_VALUES {
        return Err(DruidError::Segment(format!(
            "float column: declared length {num} exceeds cap {MAX_NUMERIC_COLUMN_VALUES}"
        )));
    }
    let expected = num
        .checked_mul(4)
        .and_then(|n| n.checked_add(4))
        .ok_or_else(|| {
            DruidError::Segment("float column: declared length overflows".to_string())
        })?;
    if data.len() < expected {
        return Err(DruidError::Segment(format!(
            "float column truncated: need {expected} bytes, have {}",
            data.len()
        )));
    }
    let mut values = Vec::with_capacity(num);
    for i in 0..num {
        let off = 4 + i * 4;
        let v = f32::from_be_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
        values.push(v);
    }
    Ok(values)
}

/// Build a synthetic double-column binary blob.
///
/// Format: `[4 bytes BE: num_values][8 bytes BE × num_values: values]`
pub fn encode_double_column(values: &[f64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + values.len() * 8);
    buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
    for &v in values {
        buf.extend_from_slice(&v.to_be_bytes());
    }
    buf
}

/// Decode a double column from the synthetic binary format.
pub fn decode_double_column(data: &[u8]) -> Result<Vec<f64>> {
    if data.len() < 4 {
        return Err(DruidError::Segment("double column too short".to_string()));
    }
    let num = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if num > MAX_NUMERIC_COLUMN_VALUES {
        return Err(DruidError::Segment(format!(
            "double column: declared length {num} exceeds cap {MAX_NUMERIC_COLUMN_VALUES}"
        )));
    }
    let expected = num
        .checked_mul(8)
        .and_then(|n| n.checked_add(4))
        .ok_or_else(|| {
            DruidError::Segment("double column: declared length overflows".to_string())
        })?;
    if data.len() < expected {
        return Err(DruidError::Segment(format!(
            "double column truncated: need {expected} bytes, have {}",
            data.len()
        )));
    }
    let mut values = Vec::with_capacity(num);
    for i in 0..num {
        let off = 4 + i * 8;
        let v = f64::from_be_bytes([
            data[off],
            data[off + 1],
            data[off + 2],
            data[off + 3],
            data[off + 4],
            data[off + 5],
            data[off + 6],
            data[off + 7],
        ]);
        values.push(v);
    }
    Ok(values)
}

/// Build a synthetic string-column binary blob.
///
/// Format:
/// ```text
/// [4 bytes BE: num_rows]
/// [4 bytes BE × num_rows: ordinals]
/// [4 bytes BE: dict_len]
/// [dict_len bytes: serialized FrontCodedDictionary]
/// [4 bytes BE: num_bitmaps]
/// for each bitmap:
///   [4 bytes BE: bitmap_len]
///   [bitmap_len bytes: DruidBitmap serialized]
/// ```
pub fn encode_string_column(data: &StringColumnData) -> Result<Vec<u8>> {
    let dict_bytes = data.dictionary.serialize(4)?;
    encode_string_column_with_dict(data, &dict_bytes)
}

/// Build a string-column binary blob using the **front-coded v2** dictionary
/// format (segment FDX).
///
/// The outer column layout is identical to [`encode_string_column`]; only the
/// embedded dictionary blob differs (v2 bucket-base-relative coding). The
/// decoder [`decode_string_column`] auto-detects the dictionary version from
/// its leading marker, so both v9 and FDX string columns decode through the
/// same path.
///
/// `bucket_size` must be a non-zero power of two.
pub fn encode_string_column_v2(data: &StringColumnData, bucket_size: usize) -> Result<Vec<u8>> {
    let dict_bytes = data.dictionary.serialize_v2(bucket_size)?;
    encode_string_column_with_dict(data, &dict_bytes)
}

/// Shared body for the v1/v2 string-column encoders: writes ordinals, the
/// already-serialized dictionary blob, and bitmaps.
fn encode_string_column_with_dict(data: &StringColumnData, dict_bytes: &[u8]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    // Encoded ordinals
    buf.extend_from_slice(&(data.encoded_values.len() as u32).to_be_bytes());
    for &ord in &data.encoded_values {
        buf.extend_from_slice(&ord.to_be_bytes());
    }

    // Dictionary
    buf.extend_from_slice(&(dict_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(dict_bytes);

    // Bitmaps
    buf.extend_from_slice(&(data.bitmap_indexes.len() as u32).to_be_bytes());
    for bm in &data.bitmap_indexes {
        let bm_bytes = bm.serialize_druid()?;
        buf.extend_from_slice(&(bm_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(&bm_bytes);
    }

    Ok(buf)
}

/// Decode a string column from the synthetic binary format.
///
/// All length-prefixed sub-fields are bounded *before* any allocation to
/// defeat OOM via a crafted segment file. See Wave 35 R1 (`column.rs:265`).
pub fn decode_string_column(data: &[u8]) -> Result<StringColumnData> {
    let mut pos: usize = 0;

    // Ordinals — bound num_rows.
    let num_rows = read_be_u32(data, &mut pos)? as usize;
    if num_rows > MAX_STRING_COLUMN_ROWS {
        return Err(DruidError::Segment(format!(
            "string column: num_rows {num_rows} exceeds cap {MAX_STRING_COLUMN_ROWS}"
        )));
    }
    // Also reject if there isn't physically enough data to back the claim.
    if num_rows
        .checked_mul(4)
        .and_then(|n| pos.checked_add(n))
        .is_none_or(|end| end > data.len())
    {
        return Err(DruidError::Segment(format!(
            "string column: declared num_rows {num_rows} exceeds remaining data {}",
            data.len().saturating_sub(pos)
        )));
    }
    let mut encoded_values = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        encoded_values.push(read_be_u32(data, &mut pos)?);
    }

    // Dictionary — bound dict_len.
    let dict_len = read_be_u32(data, &mut pos)? as usize;
    if dict_len > MAX_STRING_DICT_BYTES {
        return Err(DruidError::Segment(format!(
            "string column: dict_len {dict_len} exceeds cap {MAX_STRING_DICT_BYTES}"
        )));
    }
    if pos + dict_len > data.len() {
        return Err(DruidError::Segment(
            "string column: dictionary data truncated".to_string(),
        ));
    }
    let dictionary = FrontCodedDictionary::deserialize(&data[pos..pos + dict_len])?;
    pos += dict_len;

    // Bitmaps — bound num_bitmaps and per-bitmap length.
    let num_bitmaps = read_be_u32(data, &mut pos)? as usize;
    if num_bitmaps > MAX_STRING_BITMAPS {
        return Err(DruidError::Segment(format!(
            "string column: num_bitmaps {num_bitmaps} exceeds cap {MAX_STRING_BITMAPS}"
        )));
    }
    let mut bitmap_indexes = Vec::with_capacity(num_bitmaps);
    let mut total_bitmap_rows: u64 = 0;
    for _ in 0..num_bitmaps {
        let bm_len = read_be_u32(data, &mut pos)? as usize;
        if bm_len > MAX_BITMAP_BYTES {
            return Err(DruidError::Segment(format!(
                "string column: bitmap length {bm_len} exceeds cap {MAX_BITMAP_BYTES}"
            )));
        }
        if pos + bm_len > data.len() {
            return Err(DruidError::Segment(
                "string column: bitmap data truncated".to_string(),
            ));
        }
        // Bounded against `num_rows` BEFORE deserialization: a malformed
        // sub-MiB payload declaring ~65 536 run containers would otherwise
        // expand to ~512 MiB of container stores inside the decoder (rows
        // below `num_rows` fit in at most ceil(num_rows / 65536)
        // containers), long before the max-row invariant check below.
        let bm = DruidBitmap::deserialize_druid_bounded(&data[pos..pos + bm_len], num_rows as u64)?;
        pos += bm_len;
        // Bitmap row ids must all be `< num_rows` (each bit position in a
        // bitmap denotes a row ordinal in the column).  We use
        // `RoaringBitmap::max()` via `as_inner()` rather than iterating
        // every set bit so the check stays O(1) per bitmap regardless of
        // cardinality.
        let bm_idx = bitmap_indexes.len();
        if let Some(max_row) = bm.as_inner().max()
            && (max_row as usize) >= num_rows
        {
            return Err(DruidError::Segment(format!(
                "string column: bitmap[{bm_idx}] references row {max_row} \
                 which is out of range for column num_rows {num_rows}"
            )));
        }
        // Single-value columns partition their rows across the value
        // bitmaps (plus at most one null-row bitmap over the same rows), so
        // the total cardinality can never exceed the row count. Enforce it
        // DURING the loop: run-encoded bitmaps that each stay within the
        // per-bitmap bounds (~6 B of input per all-rows container) could
        // otherwise accumulate num_bitmaps × num_rows/8 bytes of stores.
        total_bitmap_rows = total_bitmap_rows.saturating_add(bm.len());
        if total_bitmap_rows > num_rows as u64 {
            return Err(DruidError::Segment(format!(
                "string column: bitmaps claim {total_bitmap_rows} total rows, exceeding \
                 the column's {num_rows} rows (bitmaps must partition the rows)"
            )));
        }
        bitmap_indexes.push(bm);
    }

    // Wave 45-D closure of W37B `segment_tails` Medium #1 (logic):
    // every length-prefix is bounded above (Wave 35/36-D), but the
    // *internal* invariants between ordinals, dictionary cardinality,
    // and bitmap sets are still un-checked.  An attacker with the
    // ability to craft a segment file (or a corrupted on-disk
    // segment) can therefore produce a `StringColumnData` whose
    // ordinals point past the dictionary, whose bitmap count
    // disagrees with the dictionary, or whose bitmap rows reference
    // rows past the column's `num_rows` — every one of which yields
    // wrong-but-not-error reads at query time.  We validate all
    // three invariants here so corruption surfaces as a hard
    // `DruidError::Segment` at decode time rather than as silent
    // wrong answers.
    let dict_card = dictionary.len();
    for (idx, &ord) in encoded_values.iter().enumerate() {
        if (ord as usize) >= dict_card {
            return Err(DruidError::Segment(format!(
                "string column: ordinal at row {idx} = {ord} is out of bounds \
                 for dictionary cardinality {dict_card}"
            )));
        }
    }
    // One bitmap per dictionary entry, plus optionally one trailing
    // null-row bitmap (null-bearing columns, see
    // [`StringColumnData::null_rows`]).  Any other count is corruption.
    if bitmap_indexes.len() != dict_card && bitmap_indexes.len() != dict_card + 1 {
        return Err(DruidError::Segment(format!(
            "string column: bitmap count {bm_count} does not match dictionary \
             cardinality {dict_card} (one bitmap per dictionary entry, plus \
             at most one trailing null-row bitmap, expected)",
            bm_count = bitmap_indexes.len()
        )));
    }
    // (The per-bitmap max-row invariant — every bitmap row id `< num_rows`
    // — is enforced inside the decode loop above, before each bitmap is
    // accepted, so hostile bitmaps fail closed without accumulating.)

    Ok(StringColumnData {
        dictionary,
        encoded_values,
        bitmap_indexes,
    })
}

/// Read a big-endian u32 from `data` at `*pos`, advancing `*pos`.
fn read_be_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > data.len() {
        return Err(DruidError::Segment(
            "unexpected end of data reading u32".to_string(),
        ));
    }
    let v = u32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos += 4;
    Ok(v)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_descriptor_from_json() {
        let json = br#"{"valueType":"STRING","hasMultipleValues":false,"hasBitmapIndexes":true}"#;
        let desc = ColumnDescriptor::from_json(json).unwrap();
        assert_eq!(desc.value_type, "STRING");
        assert!(!desc.has_multiple_values);
        assert!(desc.has_bitmap_indexes);
        assert!(!desc.has_spatial_indexes);
    }

    #[test]
    fn column_descriptor_defaults() {
        let json = br#"{"valueType":"LONG"}"#;
        let desc = ColumnDescriptor::from_json(json).unwrap();
        assert_eq!(desc.value_type, "LONG");
        assert!(!desc.has_multiple_values);
        assert!(!desc.has_bitmap_indexes);
    }

    #[test]
    fn long_column_round_trip() {
        let values = vec![100_i64, -200, 0, i64::MAX, i64::MIN];
        let encoded = encode_long_column(&values);
        let decoded = decode_long_column(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn float_column_round_trip() {
        let values = vec![1.5_f32, -2.0, 0.0, f32::MAX];
        let encoded = encode_float_column(&values);
        let decoded = decode_float_column(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn double_column_round_trip() {
        let values = vec![1.5_f64, -2.0, 0.0, f64::MAX, f64::MIN];
        let encoded = encode_double_column(&values);
        let decoded = decode_double_column(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn string_column_round_trip() {
        let dict = FrontCodedDictionary::from_sorted(vec![
            "bar".to_string(),
            "baz".to_string(),
            "foo".to_string(),
        ]);
        let encoded_values = vec![2, 0, 1, 2, 0]; // foo, bar, baz, foo, bar

        let mut bm0 = DruidBitmap::new();
        bm0.insert(1);
        bm0.insert(4);

        let mut bm1 = DruidBitmap::new();
        bm1.insert(2);

        let mut bm2 = DruidBitmap::new();
        bm2.insert(0);
        bm2.insert(3);

        let col = StringColumnData {
            dictionary: dict,
            encoded_values,
            bitmap_indexes: vec![bm0.clone(), bm1.clone(), bm2.clone()],
        };

        let blob = encode_string_column(&col).unwrap();
        let decoded = decode_string_column(&blob).unwrap();

        assert_eq!(decoded.encoded_values, col.encoded_values);
        assert_eq!(decoded.dictionary.len(), 3);
        assert_eq!(decoded.dictionary.get(0), Some("bar"));
        assert_eq!(decoded.dictionary.get(1), Some("baz"));
        assert_eq!(decoded.dictionary.get(2), Some("foo"));
        assert_eq!(decoded.bitmap_indexes.len(), 3);
        assert_eq!(decoded.bitmap_indexes[0], bm0);
        assert_eq!(decoded.bitmap_indexes[1], bm1);
        assert_eq!(decoded.bitmap_indexes[2], bm2);
    }

    #[test]
    fn string_column_v2_round_trip() {
        let dict = FrontCodedDictionary::from_sorted(vec![
            "bar".to_string(),
            "baz".to_string(),
            "foo".to_string(),
            "foobar".to_string(),
        ]);
        let encoded_values = vec![2, 0, 1, 3, 0];

        let mut bm0 = DruidBitmap::new();
        bm0.insert(1);
        bm0.insert(4);
        let mut bm1 = DruidBitmap::new();
        bm1.insert(2);
        let mut bm2 = DruidBitmap::new();
        bm2.insert(0);
        let mut bm3 = DruidBitmap::new();
        bm3.insert(3);

        let col = StringColumnData {
            dictionary: dict,
            encoded_values,
            bitmap_indexes: vec![bm0, bm1, bm2, bm3],
        };

        // Encode with the v2 dictionary format.
        let blob = encode_string_column_v2(&col, 4).unwrap();
        // The embedded dictionary blob's version marker must be 2. Locate it:
        // [u32 num_rows][num_rows × u32 ordinals][u32 dict_len][dict blob ..].
        let num_rows = col.encoded_values.len();
        let dict_off = 4 + num_rows * 4 + 4; // start of dict blob
        assert_eq!(
            u32::from_le_bytes([
                blob[dict_off],
                blob[dict_off + 1],
                blob[dict_off + 2],
                blob[dict_off + 3],
            ]),
            2,
            "FDX string column must embed a v2 dictionary"
        );

        // Decode must auto-detect v2 and yield the right values.
        let decoded = decode_string_column(&blob).unwrap();
        assert_eq!(decoded.encoded_values, col.encoded_values);
        assert_eq!(decoded.dictionary.len(), 4);
        assert_eq!(decoded.dictionary.get(0), Some("bar"));
        assert_eq!(decoded.dictionary.get(3), Some("foobar"));
    }

    #[test]
    fn long_column_truncated() {
        assert!(decode_long_column(&[0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1]).is_err());
    }

    #[test]
    fn column_data_num_rows() {
        assert_eq!(ColumnData::Long(vec![1, 2, 3]).num_rows(), Some(3));
        assert_eq!(ColumnData::Float(vec![1.0]).num_rows(), Some(1));
        assert_eq!(ColumnData::Double(vec![]).num_rows(), Some(0));
        assert_eq!(ColumnData::Complex(vec![0]).num_rows(), None);
    }

    // -----------------------------------------------------------------------
    // Wave 36-D / R1: bounded reader (defeat OOM via crafted segment file).
    // Internal security review (Wave 35 R1), High: "index.drd trusts
    // attacker-controlled dimension/metric counts".
    // -----------------------------------------------------------------------

    /// A string column whose serialized header claims `u32::MAX` dictionary
    /// bytes must be rejected before we attempt the slice index. The total
    /// bytes allocated by the decoder must stay well under 1 MiB despite the
    /// adversarial input.
    #[test]
    fn oversized_string_dict_len_is_rejected() {
        // Craft: num_rows=0, then dict_len=u32::MAX.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0_u32.to_be_bytes()); // num_rows = 0
        buf.extend_from_slice(&u32::MAX.to_be_bytes()); // dict_len = 4 GiB
        // (we don't bother appending bitmaps; the dict bound trips first)

        let err = decode_string_column(&buf).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("dict_len") && msg.contains("exceeds cap"),
            "expected dict_len cap error, got: {msg}"
        );
        // Sanity: the buffer itself is only 8 bytes.
        assert!(buf.len() < 64);
    }

    /// A string column whose serialized header claims `u32::MAX` rows must
    /// be rejected before allocation. We don't even need to lie about the
    /// data length — the cap fires first.
    #[test]
    fn oversized_string_num_rows_is_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_be_bytes()); // num_rows = u32::MAX

        let err = decode_string_column(&buf).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("num_rows") && msg.contains("exceeds cap"),
            "expected num_rows cap error, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 36-F (Wave 37 R1 medium): numeric column header is now bounded.
    // Internal security review (Wave 37 R1), Medium: "Numeric column
    // decoders still allocate unbounded vectors from file headers".
    // -----------------------------------------------------------------------

    /// A long-column header claiming `u32::MAX` rows must be rejected before
    /// the decoder reaches `Vec::with_capacity`.
    #[test]
    fn oversized_long_column_num_is_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        let err = decode_long_column(&buf).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("long column") && msg.contains("exceeds cap"),
            "expected long column cap error, got: {msg}"
        );
    }

    /// A float-column header claiming `u32::MAX` rows must be rejected.
    #[test]
    fn oversized_float_column_num_is_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        let err = decode_float_column(&buf).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("float column") && msg.contains("exceeds cap"),
            "expected float column cap error, got: {msg}"
        );
    }

    /// A double-column header claiming `u32::MAX` rows must be rejected.
    #[test]
    fn oversized_double_column_num_is_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        let err = decode_double_column(&buf).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("double column") && msg.contains("exceeds cap"),
            "expected double column cap error, got: {msg}"
        );
    }

    /// A column whose num_bitmaps is in-bound but each bitmap claims
    /// `u32::MAX` bytes must be rejected by the per-bitmap length cap.
    #[test]
    fn oversized_bitmap_len_is_rejected() {
        // Craft: num_rows=0, dict_len=0 (empty FrontCodedDictionary
        // deserializes from empty), num_bitmaps=1, bm_len=u32::MAX.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0_u32.to_be_bytes()); // num_rows
        buf.extend_from_slice(&0_u32.to_be_bytes()); // dict_len
        buf.extend_from_slice(&1_u32.to_be_bytes()); // num_bitmaps
        buf.extend_from_slice(&u32::MAX.to_be_bytes()); // bm_len = 4 GiB

        // We may also fail on FrontCodedDictionary::deserialize() of empty —
        // that's fine, it's still a rejection. The important property: no
        // multi-GiB allocation.
        let result = decode_string_column(&buf);
        assert!(result.is_err(), "must reject oversized bitmap");
    }

    // -----------------------------------------------------------------------
    // Wave 45-D regression — W37B `segment_tails` Medium #1
    //
    // Pre-fix: `decode_string_column` checked length-prefix bounds (Wave
    // 36-D) but never validated the *internal* invariants between the
    // ordinal stream, the dictionary cardinality, and the bitmap set.
    // A crafted segment could therefore decode successfully but contain
    // (a) ordinals pointing past the dictionary, (b) a bitmap count
    // disagreeing with the dictionary, or (c) bitmap row-ids outside
    // the column's `num_rows`.  Each yielded silent wrong reads at
    // query time.  Post-fix: every invariant is enforced at decode
    // time and surfaces as `DruidError::Segment`.
    // -----------------------------------------------------------------------

    /// An ordinal pointing past the dictionary cardinality must be
    /// rejected at decode time rather than producing wrong reads later.
    #[test]
    fn string_column_rejects_ordinal_past_dictionary() {
        // Build a healthy column then corrupt one ordinal in the
        // serialized blob.  Dictionary has 2 entries (cardinality 2),
        // so the legal ordinal range is `[0, 1]`; we patch the last
        // ordinal to `99`.
        let dict = FrontCodedDictionary::from_sorted(vec!["a".into(), "b".into()]);
        let bm0 = {
            let mut b = DruidBitmap::new();
            b.insert(0);
            b
        };
        let bm1 = {
            let mut b = DruidBitmap::new();
            b.insert(1);
            b.insert(2);
            b
        };
        let col = StringColumnData {
            dictionary: dict,
            encoded_values: vec![0, 1, 1],
            bitmap_indexes: vec![bm0, bm1],
        };
        let mut blob = encode_string_column(&col).expect("encode");

        // Ordinals start at byte 4 (after the 4-byte u32 num_rows
        // prefix).  Each ordinal is 4 BE bytes; row 2's ordinal sits
        // at offset 4 + 2*4 = 12.  Patch it to a value past the
        // dictionary cardinality.
        let bad_ord = 99_u32.to_be_bytes();
        blob[12..16].copy_from_slice(&bad_ord);

        let err = decode_string_column(&blob).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("ordinal") && msg.contains("out of bounds"),
            "expected ordinal-out-of-bounds rejection, got: {msg}"
        );
    }

    /// A bitmap-count disagreement with dictionary cardinality must be
    /// rejected: Druid invariant is one bitmap per dictionary entry.
    #[test]
    fn string_column_rejects_bitmap_count_mismatch() {
        // Build a 3-entry dictionary but only attach 2 bitmaps.
        let dict = FrontCodedDictionary::from_sorted(vec!["a".into(), "b".into(), "c".into()]);
        let bm0 = {
            let mut b = DruidBitmap::new();
            b.insert(0);
            b
        };
        let bm1 = {
            let mut b = DruidBitmap::new();
            b.insert(1);
            b
        };
        let col = StringColumnData {
            dictionary: dict,
            encoded_values: vec![0, 1],
            bitmap_indexes: vec![bm0, bm1], // 2 bitmaps, dict_card = 3
        };
        let blob = encode_string_column(&col).expect("encode");

        let err = decode_string_column(&blob).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("bitmap count") && msg.contains("does not match dictionary cardinality"),
            "expected bitmap-count mismatch rejection, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 48 — proptest hardening (segment column decoders)
    //
    // Properties exercised below (256 cases each by default):
    // * `prop_long_column_roundtrip`     — decode(encode(xs)) == xs for
    //   any `Vec<i64>` up to 1024 elements, including i64::MIN/MAX edges.
    // * `prop_double_column_roundtrip`   — same for `Vec<f64>`, with bit-
    //   pattern equality (NaN-safe via `to_bits`).
    // * `prop_long_column_arbitrary_header_no_panic` — feeding any
    //   adversarial 4-byte length header followed by random bytes must
    //   either decode successfully or return Err — never panic, never
    //   allocate >2 GiB.
    //
    // These properties fail closed: any panic / wrong roundtrip is a
    // segment-decoder bug (Wave 36 hardening).
    // -----------------------------------------------------------------------

    mod proptests {
        use super::super::*;
        use proptest::prelude::*;

        proptest! {
            /// Roundtrip property: encode then decode any `Vec<i64>` of
            /// bounded length and recover the original.
            #[test]
            fn prop_long_column_roundtrip(
                values in prop::collection::vec(any::<i64>(), 0..1024usize)
            ) {
                let encoded = encode_long_column(&values);
                let decoded = decode_long_column(&encoded).expect("decode must succeed");
                prop_assert_eq!(decoded, values);
            }

            /// Roundtrip property: encode then decode any `Vec<f64>` of
            /// bounded length and recover the original.  Comparison uses
            /// raw bit patterns so NaN payloads are preserved exactly.
            #[test]
            fn prop_double_column_roundtrip(
                values in prop::collection::vec(any::<f64>(), 0..1024usize)
            ) {
                let encoded = encode_double_column(&values);
                let decoded = decode_double_column(&encoded).expect("decode must succeed");
                prop_assert_eq!(decoded.len(), values.len());
                for (got, want) in decoded.iter().zip(values.iter()) {
                    prop_assert_eq!(got.to_bits(), want.to_bits());
                }
            }

            /// Arbitrary-header property: feeding any 4-byte length prefix
            /// followed by random bytes must never panic the long decoder.
            /// Bound the input length to keep proptest fast.
            #[test]
            fn prop_long_column_arbitrary_header_no_panic(
                header in any::<u32>(),
                tail in prop::collection::vec(any::<u8>(), 0..512usize)
            ) {
                let mut buf = Vec::with_capacity(4 + tail.len());
                buf.extend_from_slice(&header.to_be_bytes());
                buf.extend_from_slice(&tail);
                // Result is allowed to be Ok or Err — both are non-panic
                // outcomes.  The cap on `MAX_NUMERIC_COLUMN_VALUES`
                // ensures we never attempt a multi-GiB allocation.
                let _ = decode_long_column(&buf);
            }

            /// Arbitrary-header property for the string column decoder.
            /// Any random byte payload must surface as either Ok or
            /// `DruidError::Segment`, never a panic.
            #[test]
            fn prop_string_column_arbitrary_bytes_no_panic(
                bytes in prop::collection::vec(any::<u8>(), 0..256usize)
            ) {
                let _ = decode_string_column(&bytes);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Null-faithful string columns (2026-07-11): trailing null-row bitmap.
    // -----------------------------------------------------------------------

    /// `from_nullable_values` with no nulls must be byte-identical to the
    /// historical null-unaware construction (backward compatibility of the
    /// on-disk layout for null-free segments).
    #[test]
    fn nullable_constructor_null_free_is_byte_identical() {
        let values = ["us", "eu", "us", "jp"];
        let nullable: Vec<Option<String>> = values.iter().map(|s| Some((*s).to_string())).collect();
        let col = StringColumnData::from_nullable_values(&nullable);

        // Reference: historical layout built by hand (sorted dict, one
        // bitmap per entry).
        assert_eq!(col.dictionary.len(), 3);
        assert_eq!(col.dictionary.get(0), Some("eu"));
        assert_eq!(col.dictionary.get(1), Some("jp"));
        assert_eq!(col.dictionary.get(2), Some("us"));
        assert_eq!(col.encoded_values, vec![2, 0, 2, 1]);
        assert_eq!(col.bitmap_indexes.len(), 3);
        assert!(col.null_rows().is_none());
        assert!(!col.is_null_row(0));

        // Encode/decode round-trip stays on the historical layout.
        let blob = encode_string_column(&col).expect("encode");
        let decoded = decode_string_column(&blob).expect("decode");
        assert_eq!(decoded.bitmap_indexes.len(), decoded.dictionary.len());
    }

    /// Null-bearing construction: nulls are recorded in the trailing bitmap,
    /// point at the `""` placeholder entry, and the placeholder's own value
    /// bitmap excludes them.  Round-trips through encode/decode.
    #[test]
    fn nullable_constructor_preserves_null_vs_empty() {
        let values = vec![
            Some("d1".to_string()),
            None,
            Some(String::new()), // a REAL empty string, distinct from null
            Some("d1".to_string()),
            None,
        ];
        let col = StringColumnData::from_nullable_values(&values);

        // Dictionary: "" and "d1" ("" sorts first).
        assert_eq!(col.dictionary.len(), 2);
        assert_eq!(col.dictionary.get(0), Some(""));
        assert_eq!(col.dictionary.get(1), Some("d1"));

        // Trailing null bitmap holds exactly rows 1 and 4.
        let nulls = col.null_rows().expect("null bitmap");
        assert!(nulls.contains(1) && nulls.contains(4));
        assert_eq!(nulls.len(), 2);
        assert!(col.is_null_row(1) && col.is_null_row(4));
        assert!(!col.is_null_row(2), "a real \"\" row is NOT null");

        // The "" value bitmap holds only the REAL "" row.
        assert!(col.bitmap_indexes[0].contains(2));
        assert!(!col.bitmap_indexes[0].contains(1));
        assert!(!col.bitmap_indexes[0].contains(4));

        // Null rows' ordinals stay in dictionary range (dense-path safety).
        assert!(col.encoded_values.iter().all(|&o| (o as usize) < 2));

        // Encode/decode round-trip preserves the null bitmap.
        let blob = encode_string_column(&col).expect("encode");
        let decoded = decode_string_column(&blob).expect("decode");
        let nulls = decoded.null_rows().expect("null bitmap survives");
        assert!(nulls.contains(1) && nulls.contains(4));
        assert_eq!(nulls.len(), 2);

        // v2 (FDX) dictionary coding round-trips the null bitmap too.
        let blob_v2 = encode_string_column_v2(&col, 4).expect("encode v2");
        let decoded_v2 = decode_string_column(&blob_v2).expect("decode v2");
        assert!(decoded_v2.null_rows().expect("null bitmap").contains(4));
    }

    /// The decoder accepts exactly dict_card or dict_card+1 bitmaps; two or
    /// more extra bitmaps are still corruption.
    #[test]
    fn string_column_rejects_two_extra_bitmaps() {
        let dict = FrontCodedDictionary::from_sorted(vec!["a".into(), "b".into()]);
        let mk_bm = |rows: &[u32]| {
            let mut b = DruidBitmap::new();
            for &r in rows {
                b.insert(r);
            }
            b
        };
        let col = StringColumnData {
            dictionary: dict,
            encoded_values: vec![0, 1],
            // dict_card = 2, but FOUR bitmaps (two trailing) — corruption.
            bitmap_indexes: vec![mk_bm(&[0]), mk_bm(&[1]), mk_bm(&[]), mk_bm(&[])],
        };
        let blob = encode_string_column(&col).expect("encode");
        let err = decode_string_column(&blob).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("bitmap count"),
            "expected bitmap-count rejection, got: {msg}"
        );
    }

    /// The trailing null bitmap is subject to the same row-range validation
    /// as value bitmaps.
    #[test]
    fn trailing_null_bitmap_row_range_still_validated() {
        let dict = FrontCodedDictionary::from_sorted(vec!["a".into()]);
        let mut value_bm = DruidBitmap::new();
        value_bm.insert(0);
        let mut null_bm = DruidBitmap::new();
        null_bm.insert(99); // out of range for a 2-row column
        let col = StringColumnData {
            dictionary: dict,
            encoded_values: vec![0, 0],
            bitmap_indexes: vec![value_bm, null_bm],
        };
        let blob = encode_string_column(&col).expect("encode");
        let err = decode_string_column(&blob).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("out of range"),
            "expected out-of-range rejection for the null bitmap, got: {msg}"
        );
    }

    /// All-null column: dictionary degenerates to the `""` placeholder and
    /// every row is in the null bitmap.
    #[test]
    fn nullable_constructor_all_null() {
        let values: Vec<Option<String>> = vec![None, None, None];
        let col = StringColumnData::from_nullable_values(&values);
        assert_eq!(col.dictionary.len(), 1);
        assert_eq!(col.dictionary.get(0), Some(""));
        assert!(col.bitmap_indexes[0].is_empty(), "no real \"\" rows");
        let nulls = col.null_rows().expect("null bitmap");
        assert_eq!(nulls.len(), 3);
    }

    /// NaN null markers for numeric columns survive the encode/decode
    /// round-trip bit-exactly.
    #[test]
    fn numeric_null_marker_roundtrip() {
        assert!(is_null_double(NULL_DOUBLE));
        assert!(is_null_float(NULL_FLOAT));
        assert!(!is_null_double(0.0));

        let values = vec![1.5, NULL_DOUBLE, -2.0];
        let blob = encode_double_column(&values);
        let decoded = decode_double_column(&blob).expect("decode");
        assert_eq!(decoded[0], 1.5);
        assert!(is_null_double(decoded[1]));
        assert_eq!(decoded[2], -2.0);
    }

    /// A bitmap referencing a row past the column's row count must be
    /// rejected: a query-time bitmap intersection would otherwise
    /// silently include phantom rows.
    #[test]
    fn string_column_rejects_bitmap_row_past_num_rows() {
        // 3-row column, dict cardinality 2, bitmap_indexes[0]
        // references row 99 (>> num_rows=3).  We use `as_inner().max()`
        // for the validator so the test pins that exact path.
        let dict = FrontCodedDictionary::from_sorted(vec!["a".into(), "b".into()]);
        let bm0 = {
            let mut b = DruidBitmap::new();
            b.insert(0);
            b.insert(99); // out of range — column only has 3 rows
            b
        };
        let bm1 = {
            let mut b = DruidBitmap::new();
            b.insert(1);
            b.insert(2);
            b
        };
        let col = StringColumnData {
            dictionary: dict,
            encoded_values: vec![0, 1, 1], // num_rows = 3
            bitmap_indexes: vec![bm0, bm1],
        };
        let blob = encode_string_column(&col).expect("encode");

        let err = decode_string_column(&blob).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("bitmap[0]") && msg.contains("row 99") && msg.contains("out of range"),
            "expected out-of-range bitmap rejection, got: {msg}"
        );
    }

    /// Bitmaps whose total cardinality exceeds the column's row count must
    /// be rejected: single-value columns partition their rows across the
    /// value bitmaps, and accepting overlapping full bitmaps would let
    /// run-encoded payloads (~6 B of input per all-rows container)
    /// accumulate `num_bitmaps × num_rows / 8` bytes of container stores.
    /// (Twin of `druid_native::tests::
    /// bitmap_total_cardinality_beyond_column_rows_is_rejected`, which
    /// demonstrated the pre-fix acceptance for the upstream layout.)
    #[test]
    fn string_column_rejects_bitmap_total_cardinality_past_num_rows() {
        let dict = FrontCodedDictionary::from_sorted(vec!["a".into(), "b".into()]);
        // Both bitmaps claim ALL 4 rows: each alone passes the per-bitmap
        // bounds (max row 3 < 4, 1 container), but together they claim 8
        // rows on a 4-row column.
        let full = DruidBitmap::from_sorted_iter(0..4u32);
        let col = StringColumnData {
            dictionary: dict,
            encoded_values: vec![0, 0, 1, 1], // num_rows = 4
            bitmap_indexes: vec![full.clone(), full],
        };
        let blob = encode_string_column(&col).expect("encode");

        let err = decode_string_column(&blob).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("total rows") && msg.contains("partition"),
            "expected aggregate-cardinality rejection, got: {msg}"
        );
    }
}
