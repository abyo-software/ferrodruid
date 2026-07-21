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

pub use ferrodruid_sketches::DruidHyperUnique;
pub use ferrodruid_sketches::ThetaSketch;

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

/// Hard upper bound on the total element (ordinal) count of a single
/// multi-value string column.  Mirrors `MAX_NUMERIC_COLUMN_VALUES`: 256M
/// elements is far beyond any realistic Druid segment while rejecting a
/// crafted `u32::MAX` offset table before any allocation.
const MAX_STRING_MULTI_ORDINALS: usize = 256 * 1024 * 1024;

/// Hard upper bound on the row count of a single numeric (long/float/double)
/// column. The string-column path was hardened in Wave 35; the numeric
/// decoders previously trusted the on-disk header and called
/// `Vec::with_capacity(num)` directly, allowing a crafted segment file to
/// force multi-GiB pre-allocations as long as the file was correspondingly
/// large. 256M elements (≈2 GiB at 8 bytes/value) is well above any
/// realistic Druid column while still rejecting `u32::MAX`-style headers.
/// See Wave 37 R1 (`column.rs:145-245`).
const MAX_NUMERIC_COLUMN_VALUES: usize = 256 * 1024 * 1024;

/// Hard upper bound on ONE serialized per-row theta-sketch blob (bytes) in
/// a [`ColumnData::ComplexTheta`] column.  A default-k (4096) sketch
/// serializes to ~32 KiB and Druid's largest legal nominal size (2^26) to
/// ~512 MiB — far beyond any real per-row rollup sketch.  64 MiB (~8.4 M
/// retained hashes) keeps a crafted length prefix from driving a multi-GiB
/// allocation while leaving ~2000× headroom over the default sketch size.
const MAX_THETA_BLOB_BYTES: usize = 64 * 1024 * 1024;

/// The descriptor `complexTypeName` under which a decoded per-row theta
/// column round-trips through the FerroDruid v9/FDX writers (mirrors
/// Druid's own complex type name for the metric).
pub const THETA_COMPLEX_TYPE: &str = "thetaSketch";

/// The descriptor `complexTypeName` under which a decoded per-row Druid
/// `hyperUnique` column ([`ColumnData::ComplexHyperUnique`]) round-trips
/// through the FerroDruid v9/FDX writers (mirrors Druid's own complex type
/// name for the metric — W-A, v1.5.0).
pub const HYPER_UNIQUE_COMPLEX_TYPE: &str = "hyperUnique";

/// Hard upper bound on ONE serialized per-row hyperUnique blob (bytes) in
/// a [`ColumnData::ComplexHyperUnique`] column.  The wire image is a FIXED
/// 2052 bytes (`DruidHyperUnique::serialize`), so any larger declared
/// length is corruption; the cap merely keeps a crafted length prefix from
/// driving an oversized slice attempt before the exact-length check.
const MAX_HYPER_UNIQUE_BLOB_BYTES: usize = 4096;

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

    /// Whether the column carries a trailing null-row bitmap (nullable
    /// `LONG` columns only; see [`ColumnData::LongNullable`]).  Absent in
    /// descriptors written before the nullable-long codec existed, so it
    /// defaults to `false` and old segments decode exactly as before; it
    /// is also SKIPPED on serialize when `false`, so descriptors written
    /// for null-free columns stay byte-for-byte identical to the
    /// pre-nullable-long output.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub has_nulls: bool,

    /// The complex type carried by a `COMPLEX` column
    /// ([`THETA_COMPLEX_TYPE`] = a decoded per-row theta column,
    /// [`ColumnData::ComplexTheta`]).  Absent (and SKIPPED on serialize)
    /// for every other column shape, so descriptors written before this
    /// field existed — and all non-theta descriptors after it — stay
    /// byte-for-byte identical.  A `COMPLEX` descriptor without it decodes
    /// as the opaque [`ColumnData::Complex`], exactly as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub complex_type_name: Option<String>,
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
//   a valid value).  A null-bearing long-typed column is therefore stored as
//   [`ColumnData::LongNullable`]: the exact `i64` values (with `0` at NULL
//   rows) plus an explicit null-row bitmap.  A null-free long column stays
//   `LONG`.  (Historical note: before 2026-07 the null-bearing fallback was
//   a NaN-null `DOUBLE`, which silently lost precision beyond ±2^53.)
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
    /// Nullable 64-bit signed integer column: exact per-row `i64` values
    /// plus a null-row bitmap (a set bit means that row is SQL NULL; the
    /// value vector stores `0` at NULL rows).  Built only for null-BEARING
    /// long columns — a null-free long column stays [`Self::Long`], keeping
    /// the historical layout byte-for-byte.  This variant preserves values
    /// beyond ±2^53 exactly (the pre-2026-07 NaN-`DOUBLE` degrade could
    /// not).
    LongNullable(Vec<i64>, DruidBitmap),
    /// 32-bit floating-point column.
    Float(Vec<f32>),
    /// 64-bit floating-point column.
    Double(Vec<f64>),
    /// String (dimension) column with dictionary, encoded ordinals, and bitmaps.
    String(StringColumnData),
    /// Multi-value string (dimension) column: each row holds an ordered
    /// LIST of dictionary elements (Druid `hasMultipleValues: true`).
    ///
    /// ADDITIVE variant (compat-11): a dimension whose every row is a
    /// single scalar still builds [`Self::String`] byte-for-byte; only a
    /// dimension that actually carries an explicit JSON-array (or
    /// `listDelimiter`-split) value in some row becomes `StringMulti`.
    /// Every single-value fast path in the query layer matches
    /// [`Self::String`] only, so multi-value columns always take the
    /// slow per-row-explode path.
    StringMulti(StringMultiColumnData),
    /// Complex column (opaque bytes for caller-defined types like sketches).
    Complex(Vec<u8>),
    /// Per-row DECODED Theta sketches — a Druid `thetaSketch` complex
    /// metric column read from a migrated segment (compat-8 sketch #2).
    /// Each row holds one [`ThetaSketch`] in the Apache DataSketches
    /// (MurmurHash3) hash space, decoded via
    /// [`ThetaSketch::from_druid_compact`]; such sketches are
    /// **union-only** with other Druid-origin sketches (the sketch itself
    /// enforces this — see its module docs).  Other complex types
    /// (hyperUnique / HLLSketch / quantiles) remain undecodable: they stay
    /// the opaque [`Self::Complex`] on the private read path and a loud
    /// rejection on the Druid-native read path.
    ComplexTheta(Vec<ThetaSketch>),
    /// Per-row DECODED Druid `hyperUnique` HyperLogLog sketches — a Druid
    /// `hyperUnique` complex metric column read from a migrated segment
    /// (W-A, v1.5.0).  Each row holds one [`DruidHyperUnique`] register
    /// state decoded via [`DruidHyperUnique::from_druid_blob`]; the type is
    /// MERGE-ONLY (no raw adds exist) and is never mixed with FerroDruid's
    /// FNV-space `HllSketch` or with DataSketches images.  `HLLSketch` /
    /// `quantilesDoublesSketch` complex types remain undecodable: they stay
    /// the opaque [`Self::Complex`] on the private read path and a loud
    /// rejection on the Druid-native read path.
    ComplexHyperUnique(Vec<DruidHyperUnique>),
}

impl ColumnData {
    /// Return the number of rows in this column, if known.
    ///
    /// For numeric and string columns the length is derived from the value
    /// arrays.  Complex columns do not carry explicit row count information.
    pub fn num_rows(&self) -> Option<usize> {
        match self {
            Self::Long(v) => Some(v.len()),
            Self::LongNullable(v, _) => Some(v.len()),
            Self::Float(v) => Some(v.len()),
            Self::Double(v) => Some(v.len()),
            Self::String(s) => Some(s.encoded_values.len()),
            Self::StringMulti(s) => Some(s.num_rows()),
            Self::Complex(_) => None,
            Self::ComplexTheta(v) => Some(v.len()),
            Self::ComplexHyperUnique(v) => Some(v.len()),
        }
    }
}

/// Whether `row_idx` is marked SQL NULL in a nullable-long null-row bitmap
/// (see [`ColumnData::LongNullable`]).  Row indexes beyond `u32::MAX` cannot
/// exist in a column (row counts are capped well below it), so they report
/// `false`.
#[must_use]
pub fn is_null_long_row(nulls: &DruidBitmap, row_idx: usize) -> bool {
    u32::try_from(row_idx).is_ok_and(|row| nulls.contains(row))
}

/// Build the `(values, null_bitmap)` parts of a [`ColumnData::LongNullable`]
/// from per-row optional values: `None` rows store `0` in the value vector
/// and set the corresponding bit in the null bitmap.
#[must_use]
pub fn long_nullable_parts(values: &[Option<i64>]) -> (Vec<i64>, DruidBitmap) {
    let mut out = Vec::with_capacity(values.len());
    let mut nulls = DruidBitmap::new();
    for (row_idx, value) in values.iter().enumerate() {
        match value {
            Some(v) => out.push(*v),
            None => {
                out.push(0);
                // Row counts are capped far below u32::MAX (see
                // `MAX_NUMERIC_COLUMN_VALUES`), so the cast is total in
                // practice; saturate defensively rather than wrap.
                nulls.insert(u32::try_from(row_idx).unwrap_or(u32::MAX));
            }
        }
    }
    (out, nulls)
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
// StringMultiColumnData (compat-11: genuine multi-value string dimensions)
// ---------------------------------------------------------------------------

/// Data for a multi-value string (dimension) column.
///
/// Druid-style layout: a flat [`ordinals`](Self::ordinals) array holding
/// every row's elements concatenated in row order, sliced per row by
/// [`row_offsets`](Self::row_offsets) — row `i`'s elements are
/// `ordinals[row_offsets[i]..row_offsets[i+1]]`, and
/// `row_offsets.len() == num_rows + 1`.  Element order within a row is
/// preserved as ingested (per-dimension `multiValueHandling` sort/dedup
/// modes are a documented follow-on).
///
/// # Null representation
///
/// A row with ZERO elements (`row_offsets[i] == row_offsets[i+1]`)
/// represents SQL NULL — Druid treats an ingested `[]` and `null`
/// identically for multi-value string dimensions (both group and render
/// as the null value).  Mirroring [`StringColumnData`]'s convention, a
/// column containing such rows carries **one extra trailing bitmap** in
/// [`bitmap_indexes`](Self::bitmap_indexes)
/// (`bitmap_indexes.len() == dictionary.len() + 1`) recording the
/// null/empty rows; a column whose every row has at least one element
/// keeps the plain one-bitmap-per-entry layout.
///
/// Unlike the single-value column, the per-value bitmaps do NOT
/// partition the rows: a row containing `["a","b"]` appears in BOTH the
/// `"a"` and `"b"` bitmaps (Druid's any-element filter semantics).
#[derive(Debug, Clone)]
pub struct StringMultiColumnData {
    /// Sorted dictionary of unique element values.
    pub dictionary: FrontCodedDictionary,
    /// Per-row offsets into [`ordinals`](Self::ordinals);
    /// `row_offsets.len() == num_rows + 1`, monotonic non-decreasing,
    /// starting at 0 and ending at `ordinals.len()`.
    pub row_offsets: Vec<u32>,
    /// Flat element ordinals into the dictionary, rows concatenated in
    /// row order with within-row element order preserved.
    pub ordinals: Vec<u32>,
    /// One bitmap per dictionary value = the set of rows containing that
    /// value as ANY element, plus (for columns with null/empty rows only)
    /// one trailing bitmap of the null/empty rows.
    pub bitmap_indexes: Vec<DruidBitmap>,
}

impl StringMultiColumnData {
    /// Build a multi-value column from per-row element lists.
    ///
    /// An empty list represents SQL NULL (the Druid `[]`/`null`
    /// equivalence for multi-value string dimensions).  Element order
    /// within a row is preserved; duplicate elements within a row are
    /// preserved too (default `ARRAY`-like handling — the `SORTED_ARRAY`
    /// / `SORTED_SET` modes are a documented follow-on).
    #[must_use]
    pub fn from_rows(rows: &[Vec<String>]) -> Self {
        use std::collections::{BTreeSet, HashMap};

        let mut has_empty = false;
        let mut unique: BTreeSet<&str> = BTreeSet::new();
        for row in rows {
            if row.is_empty() {
                has_empty = true;
            }
            for v in row {
                unique.insert(v.as_str());
            }
        }
        let sorted: Vec<String> = unique.iter().map(|s| (*s).to_string()).collect();
        let ordinal_of: HashMap<&str, u32> = sorted
            .iter()
            .enumerate()
            .map(|(i, v)| (v.as_str(), i as u32))
            .collect();

        let mut bitmaps: Vec<DruidBitmap> = (0..sorted.len()).map(|_| DruidBitmap::new()).collect();
        let mut null_bitmap = DruidBitmap::new();
        let total: usize = rows.iter().map(Vec::len).sum();
        let mut ordinals: Vec<u32> = Vec::with_capacity(total);
        let mut row_offsets: Vec<u32> = Vec::with_capacity(rows.len() + 1);
        row_offsets.push(0);

        for (row_idx, row) in rows.iter().enumerate() {
            if row.is_empty() {
                null_bitmap.insert(row_idx as u32);
            }
            for v in row {
                let ord = ordinal_of[v.as_str()];
                ordinals.push(ord);
                bitmaps[ord as usize].insert(row_idx as u32);
            }
            row_offsets.push(ordinals.len() as u32);
        }
        if has_empty {
            bitmaps.push(null_bitmap);
        }

        Self {
            dictionary: FrontCodedDictionary::from_sorted(sorted),
            row_offsets,
            ordinals,
            bitmap_indexes: bitmaps,
        }
    }

    /// Number of rows in this column.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.row_offsets.len().saturating_sub(1)
    }

    /// The element ordinals of `row_idx`, or `None` when out of range.
    /// An empty slice means the row is SQL NULL (ingested `[]`/`null`).
    #[must_use]
    pub fn row_ordinals(&self, row_idx: usize) -> Option<&[u32]> {
        let start = *self.row_offsets.get(row_idx)? as usize;
        let end = *self.row_offsets.get(row_idx + 1)? as usize;
        self.ordinals.get(start..end)
    }

    /// The element strings of `row_idx` resolved through the dictionary,
    /// in stored order.  Out-of-range rows and unresolvable ordinals
    /// yield an empty list (decode-time validation makes the latter
    /// unreachable for columns read from disk).
    #[must_use]
    pub fn row_values(&self, row_idx: usize) -> Vec<&str> {
        self.row_ordinals(row_idx)
            .map(|ords| {
                ords.iter()
                    .filter_map(|&ord| self.dictionary.get(ord as usize))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether `row_idx` is SQL NULL (zero elements — an ingested `[]`
    /// or `null`).
    #[must_use]
    pub fn is_null_row(&self, row_idx: usize) -> bool {
        self.row_ordinals(row_idx).is_some_and(<[u32]>::is_empty)
    }

    /// The NULL/empty-row bitmap, if this column carries one
    /// (`bitmap_indexes.len() == dictionary.len() + 1`).
    #[must_use]
    pub fn null_rows(&self) -> Option<&DruidBitmap> {
        if self.bitmap_indexes.len() == self.dictionary.len() + 1 {
            self.bitmap_indexes.last()
        } else {
            None
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

/// Decode the leading plain-long-column section of `data`, returning the
/// values and the number of bytes consumed.  Shared prefix parser: the
/// public [`decode_long_column`] additionally enforces exact consumption,
/// while [`decode_long_column_nullable`] continues past the prefix into
/// the trailing null-bitmap section.
fn decode_long_column_prefix(data: &[u8]) -> Result<(Vec<i64>, usize)> {
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
    Ok((values, expected))
}

/// Decode a long column from the synthetic binary format.
///
/// The blob must be consumed exactly — trailing bytes are corruption,
/// never skipped (same discipline as every other column decoder).  In
/// particular, a nullable-long blob (values + trailing null-bitmap
/// section) read under a descriptor that lost its `hasNulls` flag used to
/// decode "successfully" here with the null bitmap silently ignored:
/// every SQL NULL row was served as `0`.  That mismatch is now a loud
/// error.
pub fn decode_long_column(data: &[u8]) -> Result<Vec<i64>> {
    let (values, consumed) = decode_long_column_prefix(data)?;
    if consumed != data.len() {
        return Err(DruidError::Segment(format!(
            "long column: {} trailing bytes after the declared values",
            data.len() - consumed
        )));
    }
    Ok(values)
}

/// Build a synthetic nullable-long-column binary blob.
///
/// Format: the plain long-column layout ([`encode_long_column`]) followed by
/// a trailing null-row bitmap section, mirroring the string column's
/// trailing null bitmap:
///
/// ```text
/// [4 bytes BE: num_values]
/// [8 bytes BE × num_values: values (0 at NULL rows)]
/// [4 bytes BE: bitmap_len]
/// [bitmap_len bytes: DruidBitmap serialized (the NULL row set)]
/// ```
///
/// # Errors
///
/// Returns an error if the null bitmap fails to serialize.
pub fn encode_long_column_nullable(values: &[i64], nulls: &DruidBitmap) -> Result<Vec<u8>> {
    let mut buf = encode_long_column(values);
    let bm_bytes = nulls.serialize_druid()?;
    buf.extend_from_slice(&(bm_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(&bm_bytes);
    Ok(buf)
}

/// Decode a nullable long column from the synthetic binary format
/// (see [`encode_long_column_nullable`]).
///
/// All length prefixes are bounded before allocation (same discipline as
/// the string-column decoder), the bitmap is deserialized with a row-count
/// bound, every set bit must reference a row `< num_values`, and the blob
/// must be consumed exactly — trailing bytes are corruption, never skipped.
///
/// # Errors
///
/// Returns [`DruidError::Segment`] on any truncation, cap violation,
/// out-of-range null row, or trailing bytes.
pub fn decode_long_column_nullable(data: &[u8]) -> Result<(Vec<i64>, DruidBitmap)> {
    let (values, mut pos) = decode_long_column_prefix(data)?;

    let bm_len = read_be_u32(data, &mut pos)? as usize;
    if bm_len > MAX_BITMAP_BYTES {
        return Err(DruidError::Segment(format!(
            "nullable long column: null bitmap length {bm_len} exceeds cap {MAX_BITMAP_BYTES}"
        )));
    }
    if pos + bm_len > data.len() {
        return Err(DruidError::Segment(
            "nullable long column: null bitmap truncated".to_string(),
        ));
    }
    // Bounded against the row count BEFORE deserialization (defeats
    // container-store OOM via a crafted payload, same as string bitmaps).
    let nulls =
        DruidBitmap::deserialize_druid_bounded(&data[pos..pos + bm_len], values.len() as u64)?;
    pos += bm_len;
    // Every null row must exist in the column.
    if let Some(max_row) = nulls.as_inner().max()
        && (max_row as usize) >= values.len()
    {
        return Err(DruidError::Segment(format!(
            "nullable long column: null bitmap references row {max_row} which is out of \
             range for column num_rows {}",
            values.len()
        )));
    }
    // The null section is the last thing in the layout; trailing bytes are
    // a layout we do not understand — fail loud, never guess.
    if pos != data.len() {
        return Err(DruidError::Segment(format!(
            "nullable long column: {} trailing bytes after the null bitmap",
            data.len() - pos
        )));
    }
    Ok((values, nulls))
}

/// Build a per-row theta-sketch column binary blob
/// ([`ColumnData::ComplexTheta`] — the FerroDruid-private round-trip form
/// of a decoded Druid `thetaSketch` metric column).
///
/// Format:
/// ```text
/// [4 bytes BE: num_rows]
/// for each row:
///   [4 bytes BE: blob_len]
///   [blob_len bytes: ThetaSketch::serialize()]
/// ```
///
/// # Errors
///
/// Returns [`DruidError::Segment`] when the row count or a per-row blob
/// length does not fit its `u32` wire field (the historical `as u32` cast
/// truncated silently — a wrapped count/length would corrupt every later
/// read of the column).  Unreachable for columns produced by the bounded
/// decode paths, which cap rows and blob sizes far below `u32::MAX`.
pub fn encode_theta_column(rows: &[ThetaSketch]) -> Result<Vec<u8>> {
    let num_rows = u32::try_from(rows.len()).map_err(|_| {
        DruidError::Segment(format!(
            "theta column: {} rows exceed the u32 row-count field",
            rows.len()
        ))
    })?;
    let mut buf = Vec::new();
    buf.extend_from_slice(&num_rows.to_be_bytes());
    for (row_idx, sketch) in rows.iter().enumerate() {
        let bytes = sketch.serialize();
        let blob_len = u32::try_from(bytes.len()).map_err(|_| {
            DruidError::Segment(format!(
                "theta column: row {row_idx} sketch blob of {} bytes exceeds the u32 \
                 length field",
                bytes.len()
            ))
        })?;
        buf.extend_from_slice(&blob_len.to_be_bytes());
        buf.extend_from_slice(&bytes);
    }
    Ok(buf)
}

/// Decode a per-row theta-sketch column (see [`encode_theta_column`]).
///
/// Fully bounded, fail-loud decode (same discipline as the other column
/// decoders): the row count is capped and backed by remaining bytes before
/// allocation, every per-row length prefix is capped
/// ([`MAX_THETA_BLOB_BYTES`]) and bounds-checked before slicing, each blob
/// must deserialize as a [`ThetaSketch`], and the blob must be consumed
/// exactly — trailing bytes are corruption, never skipped.
///
/// # Errors
///
/// Returns [`DruidError::Segment`] on any truncation, cap violation,
/// undecodable sketch blob, or trailing bytes.
pub fn decode_theta_column(data: &[u8]) -> Result<Vec<ThetaSketch>> {
    let mut pos: usize = 0;
    let num_rows = read_be_u32(data, &mut pos)? as usize;
    if num_rows > MAX_NUMERIC_COLUMN_VALUES {
        return Err(DruidError::Segment(format!(
            "theta column: num_rows {num_rows} exceeds cap {MAX_NUMERIC_COLUMN_VALUES}"
        )));
    }
    // Each row needs at least its 4-byte length prefix — the declared count
    // must physically fit in the remaining bytes before allocation.
    if num_rows
        .checked_mul(4)
        .and_then(|n| pos.checked_add(n))
        .is_none_or(|end| end > data.len())
    {
        return Err(DruidError::Segment(format!(
            "theta column: declared num_rows {num_rows} exceeds remaining data {}",
            data.len().saturating_sub(pos)
        )));
    }
    let mut rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let blob_len = read_be_u32(data, &mut pos)? as usize;
        if blob_len > MAX_THETA_BLOB_BYTES {
            return Err(DruidError::Segment(format!(
                "theta column: row {row_idx} blob length {blob_len} exceeds cap \
                 {MAX_THETA_BLOB_BYTES}"
            )));
        }
        if pos.checked_add(blob_len).is_none_or(|end| end > data.len()) {
            return Err(DruidError::Segment(format!(
                "theta column: row {row_idx} sketch blob truncated"
            )));
        }
        let sketch = ThetaSketch::deserialize(&data[pos..pos + blob_len]).map_err(|e| {
            DruidError::Segment(format!(
                "theta column: row {row_idx} sketch failed to decode: {e}"
            ))
        })?;
        pos += blob_len;
        rows.push(sketch);
    }
    // Exact consumption — trailing bytes are a layout we do not understand.
    if pos != data.len() {
        return Err(DruidError::Segment(format!(
            "theta column: {} trailing bytes after the last row",
            data.len() - pos
        )));
    }
    Ok(rows)
}

/// Build a per-row hyperUnique column binary blob
/// ([`ColumnData::ComplexHyperUnique`] — the FerroDruid-private round-trip
/// form of a decoded Druid `hyperUnique` metric column, W-A v1.5.0).
///
/// Format (mirrors [`encode_theta_column`]):
/// ```text
/// [4 bytes BE: num_rows]
/// for each row:
///   [4 bytes BE: blob_len]
///   [blob_len bytes: DruidHyperUnique::serialize()]
/// ```
///
/// # Errors
///
/// Returns [`DruidError::Segment`] when the row count or a per-row blob
/// length does not fit its `u32` wire field (unreachable for columns
/// produced by the bounded decode paths — the per-row wire image is a
/// fixed 2052 bytes).
pub fn encode_hyper_unique_column(rows: &[DruidHyperUnique]) -> Result<Vec<u8>> {
    let num_rows = u32::try_from(rows.len()).map_err(|_| {
        DruidError::Segment(format!(
            "hyperUnique column: {} rows exceed the u32 row-count field",
            rows.len()
        ))
    })?;
    let mut buf = Vec::new();
    buf.extend_from_slice(&num_rows.to_be_bytes());
    for (row_idx, sketch) in rows.iter().enumerate() {
        let bytes = sketch.serialize();
        let blob_len = u32::try_from(bytes.len()).map_err(|_| {
            DruidError::Segment(format!(
                "hyperUnique column: row {row_idx} sketch blob of {} bytes exceeds \
                 the u32 length field",
                bytes.len()
            ))
        })?;
        buf.extend_from_slice(&blob_len.to_be_bytes());
        buf.extend_from_slice(&bytes);
    }
    Ok(buf)
}

/// Decode a per-row hyperUnique column (see [`encode_hyper_unique_column`]).
///
/// Fully bounded, fail-loud decode (same discipline as
/// [`decode_theta_column`]): the row count is capped and backed by
/// remaining bytes before allocation, every per-row length prefix is
/// capped ([`MAX_HYPER_UNIQUE_BLOB_BYTES`]) and bounds-checked before
/// slicing, each blob must deserialize as a [`DruidHyperUnique`] (which
/// itself enforces the exact wire length), and the column blob must be
/// consumed exactly — trailing bytes are corruption, never skipped.
///
/// # Errors
///
/// Returns [`DruidError::Segment`] on any truncation, cap violation,
/// undecodable sketch blob, or trailing bytes.
pub fn decode_hyper_unique_column(data: &[u8]) -> Result<Vec<DruidHyperUnique>> {
    let mut pos: usize = 0;
    let num_rows = read_be_u32(data, &mut pos)? as usize;
    if num_rows > MAX_NUMERIC_COLUMN_VALUES {
        return Err(DruidError::Segment(format!(
            "hyperUnique column: num_rows {num_rows} exceeds cap {MAX_NUMERIC_COLUMN_VALUES}"
        )));
    }
    // Each row needs at least its 4-byte length prefix — the declared count
    // must physically fit in the remaining bytes before allocation.
    if num_rows
        .checked_mul(4)
        .and_then(|n| pos.checked_add(n))
        .is_none_or(|end| end > data.len())
    {
        return Err(DruidError::Segment(format!(
            "hyperUnique column: declared num_rows {num_rows} exceeds remaining data {}",
            data.len().saturating_sub(pos)
        )));
    }
    let mut rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let blob_len = read_be_u32(data, &mut pos)? as usize;
        if blob_len > MAX_HYPER_UNIQUE_BLOB_BYTES {
            return Err(DruidError::Segment(format!(
                "hyperUnique column: row {row_idx} blob length {blob_len} exceeds cap \
                 {MAX_HYPER_UNIQUE_BLOB_BYTES}"
            )));
        }
        if pos.checked_add(blob_len).is_none_or(|end| end > data.len()) {
            return Err(DruidError::Segment(format!(
                "hyperUnique column: row {row_idx} sketch blob truncated"
            )));
        }
        let sketch = DruidHyperUnique::deserialize(&data[pos..pos + blob_len]).map_err(|e| {
            DruidError::Segment(format!(
                "hyperUnique column: row {row_idx} sketch failed to decode: {e}"
            ))
        })?;
        pos += blob_len;
        rows.push(sketch);
    }
    // Exact consumption — trailing bytes are a layout we do not understand.
    if pos != data.len() {
        return Err(DruidError::Segment(format!(
            "hyperUnique column: {} trailing bytes after the last row",
            data.len() - pos
        )));
    }
    Ok(rows)
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
///
/// The blob must be consumed exactly — trailing bytes are corruption,
/// never skipped (same discipline as every other column decoder).
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
    if data.len() > expected {
        return Err(DruidError::Segment(format!(
            "float column: {} trailing bytes after the declared values",
            data.len() - expected
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
///
/// The blob must be consumed exactly — trailing bytes are corruption,
/// never skipped (same discipline as every other column decoder).
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
    if data.len() > expected {
        return Err(DruidError::Segment(format!(
            "double column: {} trailing bytes after the declared values",
            data.len() - expected
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

/// Build a multi-value string-column binary blob (v1 dictionary coding).
///
/// Format:
/// ```text
/// [4 bytes BE: num_rows]
/// [4 bytes BE × (num_rows + 1): row_offsets]
/// [4 bytes BE × row_offsets[num_rows]: ordinals]
/// [4 bytes BE: dict_len]
/// [dict_len bytes: serialized FrontCodedDictionary]
/// [4 bytes BE: num_bitmaps]
/// for each bitmap:
///   [4 bytes BE: bitmap_len]
///   [bitmap_len bytes: DruidBitmap serialized]
/// ```
///
/// # Errors
///
/// Returns an error when the dictionary or a bitmap fails to serialize,
/// or when the in-memory column violates its own layout invariants
/// (offset table shape) — the writer fails loud rather than persisting a
/// blob the decoder would reject.
pub fn encode_string_multi_column(data: &StringMultiColumnData) -> Result<Vec<u8>> {
    let dict_bytes = data.dictionary.serialize(4)?;
    encode_string_multi_column_with_dict(data, &dict_bytes)
}

/// [`encode_string_multi_column`] with the **front-coded v2** dictionary
/// format (segment FDX).  The outer layout is identical; only the embedded
/// dictionary blob differs, and [`decode_string_multi_column`] auto-detects
/// the dictionary version exactly like the single-value decoder.
///
/// # Errors
///
/// See [`encode_string_multi_column`].
pub fn encode_string_multi_column_v2(
    data: &StringMultiColumnData,
    bucket_size: usize,
) -> Result<Vec<u8>> {
    let dict_bytes = data.dictionary.serialize_v2(bucket_size)?;
    encode_string_multi_column_with_dict(data, &dict_bytes)
}

/// Shared body for the v1/v2 multi-value string-column encoders.
fn encode_string_multi_column_with_dict(
    data: &StringMultiColumnData,
    dict_bytes: &[u8],
) -> Result<Vec<u8>> {
    let num_rows = data.num_rows();
    // Writer-side invariant checks (fail loud, never persist corruption).
    if data.row_offsets.first() != Some(&0)
        || data.row_offsets.last().copied().map(|o| o as usize) != Some(data.ordinals.len())
        || !data.row_offsets.is_sorted()
    {
        return Err(DruidError::Segment(
            "multi-value string column: row_offsets must start at 0, be monotonic \
             non-decreasing, and end at ordinals.len()"
                .to_string(),
        ));
    }

    let mut buf = Vec::new();
    buf.extend_from_slice(&(num_rows as u32).to_be_bytes());
    for &off in &data.row_offsets {
        buf.extend_from_slice(&off.to_be_bytes());
    }
    for &ord in &data.ordinals {
        buf.extend_from_slice(&ord.to_be_bytes());
    }

    buf.extend_from_slice(&(dict_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(dict_bytes);

    buf.extend_from_slice(&(data.bitmap_indexes.len() as u32).to_be_bytes());
    for bm in &data.bitmap_indexes {
        let bm_bytes = bm.serialize_druid()?;
        buf.extend_from_slice(&(bm_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(&bm_bytes);
    }

    Ok(buf)
}

/// Decode a multi-value string column (see [`encode_string_multi_column`]).
///
/// Fully bounded, fail-loud decode (same discipline as
/// [`decode_string_column`]): every length prefix is capped BEFORE
/// allocation, the offset table must be monotonic non-decreasing from 0
/// to `ordinals.len()`, every ordinal must resolve inside the dictionary,
/// bitmaps are row-bounded, the bitmap count must be `dict_card` or
/// `dict_card + 1` (validated BEFORE the bitmap `Vec` is allocated, so an
/// over-declared count cannot reserve memory), and the blob must be
/// consumed exactly.
///
/// # Errors
///
/// Returns [`DruidError::Segment`] on any truncation, cap violation,
/// non-monotonic offsets, out-of-range ordinal/bitmap row, bitmap-count
/// mismatch, or trailing bytes.
pub fn decode_string_multi_column(data: &[u8]) -> Result<StringMultiColumnData> {
    let mut pos: usize = 0;

    let num_rows = read_be_u32(data, &mut pos)? as usize;
    if num_rows > MAX_STRING_COLUMN_ROWS {
        return Err(DruidError::Segment(format!(
            "multi-value string column: num_rows {num_rows} exceeds cap {MAX_STRING_COLUMN_ROWS}"
        )));
    }
    // Offsets: (num_rows + 1) u32s must physically fit before allocation.
    let offsets_len = num_rows + 1;
    if offsets_len
        .checked_mul(4)
        .and_then(|n| pos.checked_add(n))
        .is_none_or(|end| end > data.len())
    {
        return Err(DruidError::Segment(format!(
            "multi-value string column: declared num_rows {num_rows} exceeds remaining data {}",
            data.len().saturating_sub(pos)
        )));
    }
    let mut row_offsets = Vec::with_capacity(offsets_len);
    for _ in 0..offsets_len {
        row_offsets.push(read_be_u32(data, &mut pos)?);
    }
    if row_offsets.first() != Some(&0) {
        return Err(DruidError::Segment(
            "multi-value string column: row_offsets must start at 0".to_string(),
        ));
    }
    if !row_offsets.is_sorted() {
        return Err(DruidError::Segment(
            "multi-value string column: row_offsets must be monotonic non-decreasing".to_string(),
        ));
    }
    // Ordinal count comes from the LAST offset; bound + back it with data.
    let num_ordinals = row_offsets.last().copied().unwrap_or(0) as usize;
    if num_ordinals > MAX_STRING_MULTI_ORDINALS {
        return Err(DruidError::Segment(format!(
            "multi-value string column: ordinal count {num_ordinals} exceeds cap \
             {MAX_STRING_MULTI_ORDINALS}"
        )));
    }
    if num_ordinals
        .checked_mul(4)
        .and_then(|n| pos.checked_add(n))
        .is_none_or(|end| end > data.len())
    {
        return Err(DruidError::Segment(format!(
            "multi-value string column: declared ordinal count {num_ordinals} exceeds \
             remaining data {}",
            data.len().saturating_sub(pos)
        )));
    }
    let mut ordinals = Vec::with_capacity(num_ordinals);
    for _ in 0..num_ordinals {
        ordinals.push(read_be_u32(data, &mut pos)?);
    }

    // Dictionary — bound dict_len.
    let dict_len = read_be_u32(data, &mut pos)? as usize;
    if dict_len > MAX_STRING_DICT_BYTES {
        return Err(DruidError::Segment(format!(
            "multi-value string column: dict_len {dict_len} exceeds cap {MAX_STRING_DICT_BYTES}"
        )));
    }
    if pos + dict_len > data.len() {
        return Err(DruidError::Segment(
            "multi-value string column: dictionary data truncated".to_string(),
        ));
    }
    let dictionary = FrontCodedDictionary::deserialize(&data[pos..pos + dict_len])?;
    pos += dict_len;

    // Every ordinal must resolve inside the dictionary.
    let dict_card = dictionary.len();
    for (idx, &ord) in ordinals.iter().enumerate() {
        if (ord as usize) >= dict_card {
            return Err(DruidError::Segment(format!(
                "multi-value string column: ordinal at element {idx} = {ord} is out of \
                 bounds for dictionary cardinality {dict_card}"
            )));
        }
    }

    // Bitmaps — bound num_bitmaps and per-bitmap length.  NOTE: unlike the
    // single-value decoder, the per-value bitmaps do NOT partition the rows
    // (a row appears in every bitmap of every element it holds), so the
    // aggregate-cardinality bound is `ordinals.len() + num_rows` (each
    // element contributes at most one bitmap membership; the optional
    // trailing null bitmap adds at most one per row).
    let num_bitmaps = read_be_u32(data, &mut pos)? as usize;
    if num_bitmaps > MAX_STRING_BITMAPS {
        return Err(DruidError::Segment(format!(
            "multi-value string column: num_bitmaps {num_bitmaps} exceeds cap {MAX_STRING_BITMAPS}"
        )));
    }
    // compat-11 R2: validate the declared count BEFORE any allocation.  One
    // bitmap per dictionary entry, plus optionally one trailing null/empty-row
    // bitmap — any other count is corruption.  Pre-fix this was only checked
    // AFTER the decode loop, so a tiny blob declaring e.g. 16 M bitmaps
    // reserved hundreds of MiB in `with_capacity` before discovering the
    // mismatch (same species as the `ordinals`/`row_offsets` bounds above).
    if num_bitmaps != dict_card && num_bitmaps != dict_card + 1 {
        return Err(DruidError::Segment(format!(
            "multi-value string column: declared num_bitmaps {num_bitmaps} does not match \
             dictionary cardinality {dict_card} (one bitmap per dictionary entry, plus at \
             most one trailing null-row bitmap, expected)"
        )));
    }
    // Each bitmap needs at least its 4-byte length prefix — the declared
    // count must physically fit in the remaining bytes before allocation.
    if num_bitmaps
        .checked_mul(4)
        .and_then(|n| pos.checked_add(n))
        .is_none_or(|end| end > data.len())
    {
        return Err(DruidError::Segment(format!(
            "multi-value string column: declared num_bitmaps {num_bitmaps} exceeds \
             remaining data {}",
            data.len().saturating_sub(pos)
        )));
    }
    let max_total_bitmap_rows = (num_ordinals as u64).saturating_add(num_rows as u64);
    let mut bitmap_indexes = Vec::with_capacity(num_bitmaps);
    let mut total_bitmap_rows: u64 = 0;
    for _ in 0..num_bitmaps {
        let bm_len = read_be_u32(data, &mut pos)? as usize;
        if bm_len > MAX_BITMAP_BYTES {
            return Err(DruidError::Segment(format!(
                "multi-value string column: bitmap length {bm_len} exceeds cap {MAX_BITMAP_BYTES}"
            )));
        }
        if pos + bm_len > data.len() {
            return Err(DruidError::Segment(
                "multi-value string column: bitmap data truncated".to_string(),
            ));
        }
        let bm = DruidBitmap::deserialize_druid_bounded(&data[pos..pos + bm_len], num_rows as u64)?;
        pos += bm_len;
        let bm_idx = bitmap_indexes.len();
        if let Some(max_row) = bm.as_inner().max()
            && (max_row as usize) >= num_rows
        {
            return Err(DruidError::Segment(format!(
                "multi-value string column: bitmap[{bm_idx}] references row {max_row} \
                 which is out of range for column num_rows {num_rows}"
            )));
        }
        total_bitmap_rows = total_bitmap_rows.saturating_add(bm.len());
        if total_bitmap_rows > max_total_bitmap_rows {
            return Err(DruidError::Segment(format!(
                "multi-value string column: bitmaps claim {total_bitmap_rows} total rows, \
                 exceeding the element+null bound {max_total_bitmap_rows}"
            )));
        }
        bitmap_indexes.push(bm);
    }

    // (The bitmap count itself was validated against the dictionary
    // cardinality BEFORE the loop — see the pre-allocation check above.)

    // Exact consumption — trailing bytes are a layout we do not understand.
    if pos != data.len() {
        return Err(DruidError::Segment(format!(
            "multi-value string column: {} trailing bytes after the bitmaps",
            data.len() - pos
        )));
    }

    Ok(StringMultiColumnData {
        dictionary,
        row_offsets,
        ordinals,
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

    /// Null-free descriptors must serialize WITHOUT the `hasNulls` key so
    /// the descriptor JSON for every pre-existing column shape stays
    /// byte-for-byte identical to the pre-nullable-long writer output; a
    /// nullable-long descriptor carries `"hasNulls":true` and round-trips.
    #[test]
    fn descriptor_has_nulls_skipped_when_false() {
        let desc = ColumnDescriptor {
            value_type: "LONG".to_string(),
            has_multiple_values: false,
            has_bitmap_indexes: false,
            has_spatial_indexes: false,
            has_nulls: false,
            complex_type_name: None,
        };
        let json = serde_json::to_string(&desc).expect("serialize");
        assert!(
            !json.contains("hasNulls"),
            "null-free descriptor must not emit hasNulls: {json}"
        );
        assert!(
            !json.contains("complexTypeName"),
            "non-complex descriptor must not emit complexTypeName: {json}"
        );

        let desc = ColumnDescriptor {
            has_nulls: true,
            ..desc
        };
        let json = serde_json::to_string(&desc).expect("serialize");
        assert!(
            json.contains("\"hasNulls\":true"),
            "nullable descriptor must emit hasNulls: {json}"
        );
        let back = ColumnDescriptor::from_json(json.as_bytes()).expect("parse");
        assert!(back.has_nulls);
    }

    #[test]
    fn long_column_round_trip() {
        let values = vec![100_i64, -200, 0, i64::MAX, i64::MIN];
        let encoded = encode_long_column(&values);
        let decoded = decode_long_column(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    /// Nullable-long round-trip: exact values (including 2^53+1, which a
    /// NaN-Double degrade would silently round, and both i64 extremes) plus
    /// the exact null-row set must survive encode→decode.
    #[test]
    fn long_nullable_column_round_trip() {
        let rows: Vec<Option<i64>> = vec![
            Some(7),
            None,
            Some(9_007_199_254_740_993), // 2^53 + 1: NOT representable in f64
            None,
            Some(i64::MAX),
            Some(i64::MIN),
        ];
        let (values, nulls) = long_nullable_parts(&rows);
        assert_eq!(
            values,
            vec![7, 0, 9_007_199_254_740_993, 0, i64::MAX, i64::MIN]
        );
        assert_eq!(nulls.len(), 2);
        assert!(nulls.contains(1) && nulls.contains(3));

        let blob = encode_long_column_nullable(&values, &nulls).expect("encode");
        let (dec_values, dec_nulls) = decode_long_column_nullable(&blob).expect("decode");
        assert_eq!(dec_values, values, "values must round-trip i64-exactly");
        assert_eq!(dec_nulls, nulls, "null rows must round-trip exactly");
        assert!(is_null_long_row(&dec_nulls, 1) && is_null_long_row(&dec_nulls, 3));
        assert!(!is_null_long_row(&dec_nulls, 2));
    }

    /// The nullable-long decoder rejects a null bitmap referencing a row
    /// past the column's row count, a truncated bitmap section, and
    /// trailing bytes after the bitmap.
    #[test]
    fn long_nullable_column_rejects_corruption() {
        let values = vec![1_i64, 2, 3];
        let mut bad_nulls = DruidBitmap::new();
        bad_nulls.insert(99); // out of range for a 3-row column
        let blob = encode_long_column_nullable(&values, &bad_nulls).expect("encode");
        let err = decode_long_column_nullable(&blob).expect_err("must reject");
        assert!(
            format!("{err:?}").contains("out of range"),
            "expected out-of-range rejection, got: {err:?}"
        );

        let mut nulls = DruidBitmap::new();
        nulls.insert(1);
        let good = encode_long_column_nullable(&values, &nulls).expect("encode");
        // Truncate inside the bitmap section.
        let err = decode_long_column_nullable(&good[..good.len() - 1]).expect_err("truncated");
        assert!(format!("{err:?}").contains("Segment"), "got: {err:?}");
        // Trailing garbage after the bitmap.
        let mut trailing = good.clone();
        trailing.push(0xAB);
        let err = decode_long_column_nullable(&trailing).expect_err("trailing bytes");
        assert!(
            format!("{err:?}").contains("trailing bytes"),
            "expected trailing-bytes rejection, got: {err:?}"
        );
        // The untouched blob still decodes.
        let (v, n) = decode_long_column_nullable(&good).expect("decode good");
        assert_eq!(v, values);
        assert_eq!(n, nulls);
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

    /// Trailing bytes after the declared values are corruption: the plain
    /// numeric decoders must reject them loudly, matching the exact-
    /// consumption discipline of every other decoder in this module
    /// (pre-fix they were silently ignored).
    #[test]
    fn plain_numeric_columns_reject_trailing_bytes() {
        let mut long_blob = encode_long_column(&[1, 2, 3]);
        long_blob.push(0xAB);
        let err = decode_long_column(&long_blob).expect_err("long trailing byte");
        assert!(err.to_string().contains("trailing"), "got: {err}");

        let mut float_blob = encode_float_column(&[1.5, 2.5]);
        float_blob.push(0xAB);
        let err = decode_float_column(&float_blob).expect_err("float trailing byte");
        assert!(err.to_string().contains("trailing"), "got: {err}");

        let mut double_blob = encode_double_column(&[1.5, 2.5]);
        double_blob.push(0xAB);
        let err = decode_double_column(&double_blob).expect_err("double trailing byte");
        assert!(err.to_string().contains("trailing"), "got: {err}");

        // Exact blobs still decode (no behaviour change for real columns).
        assert_eq!(
            decode_long_column(&encode_long_column(&[1, 2, 3])).expect("exact"),
            vec![1, 2, 3]
        );
    }

    /// The silent NULL→0 corruption scenario: a NULLABLE-long blob (values
    /// plus trailing null-bitmap section) read under the PLAIN long decoder
    /// — i.e. a descriptor that lost its `hasNulls` flag (external rewrite,
    /// serde default) — must be a loud error, not a "successful" decode
    /// that serves every SQL NULL row as 0.
    #[test]
    fn nullable_long_blob_under_plain_decoder_is_loud_error() {
        let mut nulls = DruidBitmap::new();
        nulls.insert(1);
        let blob = encode_long_column_nullable(&[7, 0, 9], &nulls).expect("encode");

        // The nullable decoder reads it fine…
        let (values, decoded_nulls) = decode_long_column_nullable(&blob).expect("nullable");
        assert_eq!(values, vec![7, 0, 9]);
        assert!(decoded_nulls.contains(1));

        // …but the plain decoder must refuse the same bytes.
        let err = decode_long_column(&blob)
            .expect_err("plain decoder must not silently drop the null section");
        assert!(err.to_string().contains("trailing"), "got: {err}");
    }

    #[test]
    fn column_data_num_rows() {
        assert_eq!(ColumnData::Long(vec![1, 2, 3]).num_rows(), Some(3));
        assert_eq!(ColumnData::Float(vec![1.0]).num_rows(), Some(1));
        assert_eq!(ColumnData::Double(vec![]).num_rows(), Some(0));
        assert_eq!(ColumnData::Complex(vec![0]).num_rows(), None);
        // Unlike the opaque Complex blob, the decoded theta column IS
        // per-row: its row count is known.
        assert_eq!(
            ColumnData::ComplexTheta(vec![ThetaSketch::empty_druid_origin(); 2]).num_rows(),
            Some(2)
        );
    }

    // -- per-row theta column codec (compat-8 sketch #2) ---------------------

    /// Build a small DataSketches compact image (preLongs 2, exact mode)
    /// so the round-trip covers a genuine Druid-origin sketch.
    fn druid_compact_image(hashes: &[u64]) -> Vec<u8> {
        let mut buf = vec![2u8, 3, 3, 12, 13, 0, 0x1E, 0x93];
        buf.extend_from_slice(&(hashes.len() as i32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        for &h in hashes {
            buf.extend_from_slice(&h.to_le_bytes());
        }
        buf
    }

    #[test]
    fn theta_column_round_trip() {
        // One decoded Druid-origin sketch, one empty Druid-origin sketch,
        // and one native sketch: values, estimates, AND the union-only
        // origin marking must all survive the column codec.
        let druid = ThetaSketch::from_druid_compact(&druid_compact_image(&[10, 20, 30]))
            .expect("decode druid image");
        let mut native = ThetaSketch::new(512);
        native.add(b"a").expect("native add");
        let rows = vec![druid, ThetaSketch::empty_druid_origin(), native];

        let blob = encode_theta_column(&rows).expect("encode theta column");
        let decoded = decode_theta_column(&blob).expect("decode theta column");
        assert_eq!(decoded.len(), 3);
        for (orig, got) in rows.iter().zip(&decoded) {
            assert!((orig.estimate() - got.estimate()).abs() < f64::EPSILON);
            assert_eq!(orig.is_druid_origin(), got.is_druid_origin());
            assert_eq!(orig.retained(), got.retained());
        }
    }

    #[test]
    fn theta_column_rejects_corruption() {
        let rows = vec![ThetaSketch::empty_druid_origin()];
        let blob = encode_theta_column(&rows).expect("encode theta column");

        // Truncated: cut into the row blob.
        assert!(decode_theta_column(&blob[..blob.len() - 2]).is_err());

        // Trailing bytes after the last row.
        let mut trailing = blob.clone();
        trailing.extend_from_slice(&[0xEE; 3]);
        let err = decode_theta_column(&trailing).expect_err("trailing bytes must fail");
        assert!(err.to_string().contains("trailing"), "got: {err}");

        // Hostile row count far beyond the remaining bytes must be
        // rejected BEFORE allocation.
        let mut hostile = u32::MAX.to_be_bytes().to_vec();
        hostile.extend_from_slice(&[0u8; 16]);
        let err = decode_theta_column(&hostile).expect_err("hostile count must fail");
        assert!(err.to_string().contains("exceeds"), "got: {err}");

        // Hostile per-row blob length beyond the cap.
        let mut bad_len = 1u32.to_be_bytes().to_vec();
        bad_len.extend_from_slice(&(u32::MAX).to_be_bytes());
        let err = decode_theta_column(&bad_len).expect_err("hostile blob length must fail");
        assert!(err.to_string().contains("exceeds cap"), "got: {err}");

        // A row blob that is not a valid sketch fails with the row index.
        let mut bad_row = 1u32.to_be_bytes().to_vec();
        bad_row.extend_from_slice(&4u32.to_be_bytes());
        bad_row.extend_from_slice(&[0xFF; 4]);
        let err = decode_theta_column(&bad_row).expect_err("garbage sketch must fail");
        assert!(err.to_string().contains("row 0"), "got: {err}");
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

    // -----------------------------------------------------------------------
    // compat-11: multi-value string columns
    // -----------------------------------------------------------------------

    fn mv_fixture_rows() -> Vec<Vec<String>> {
        vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string()],
            vec![],
            vec!["c".to_string(), "a".to_string()],
        ]
    }

    /// Codec round-trip: `[["a","b"], ["a"], [], ["c","a"]]` through
    /// encode→decode preserves rows, within-row element ORDER (row 3 is
    /// `["c","a"]`, NOT sorted), and the empty (null) row — in both the
    /// v1 (v9) and v2 (FDX) dictionary codings.
    #[test]
    fn string_multi_column_round_trip() {
        let col = StringMultiColumnData::from_rows(&mv_fixture_rows());
        assert_eq!(col.num_rows(), 4);
        assert_eq!(col.dictionary.len(), 3); // a, b, c sorted
        assert_eq!(col.row_values(0), vec!["a", "b"]);
        assert_eq!(col.row_values(1), vec!["a"]);
        assert!(col.row_values(2).is_empty() && col.is_null_row(2));
        assert_eq!(col.row_values(3), vec!["c", "a"], "order preserved");
        // Bitmaps: a={0,1,3}, b={0}, c={3}, null={2} (trailing).
        assert_eq!(col.bitmap_indexes.len(), 4);
        let a_bm = &col.bitmap_indexes[0];
        assert!(a_bm.contains(0) && a_bm.contains(1) && a_bm.contains(3) && !a_bm.contains(2));
        assert!(col.null_rows().expect("null bitmap").contains(2));

        for blob in [
            encode_string_multi_column(&col).expect("encode v1"),
            encode_string_multi_column_v2(&col, 4).expect("encode v2"),
        ] {
            let dec = decode_string_multi_column(&blob).expect("decode");
            assert_eq!(dec.row_offsets, col.row_offsets);
            assert_eq!(dec.ordinals, col.ordinals);
            assert_eq!(dec.dictionary.len(), 3);
            assert_eq!(dec.row_values(0), vec!["a", "b"]);
            assert_eq!(dec.row_values(3), vec!["c", "a"]);
            assert!(dec.is_null_row(2));
            assert_eq!(dec.bitmap_indexes, col.bitmap_indexes);
        }
    }

    /// A column whose every row has >= 1 element carries NO trailing null
    /// bitmap (plain one-bitmap-per-entry layout).
    #[test]
    fn string_multi_column_no_empty_rows_has_no_null_bitmap() {
        let col = StringMultiColumnData::from_rows(&[
            vec!["x".to_string(), "y".to_string()],
            vec!["x".to_string()],
        ]);
        assert_eq!(col.bitmap_indexes.len(), col.dictionary.len());
        assert!(col.null_rows().is_none());
        let blob = encode_string_multi_column(&col).expect("encode");
        let dec = decode_string_multi_column(&blob).expect("decode");
        assert!(dec.null_rows().is_none());
        assert_eq!(dec.row_values(0), vec!["x", "y"]);
    }

    /// The decoder rejects: non-monotonic offsets, an ordinal past the
    /// dictionary, a bad first offset, trailing bytes, and an oversized
    /// num_rows header — all loud `DruidError::Segment`s, never a wrong
    /// decode.
    #[test]
    fn string_multi_column_rejects_corruption() {
        let col = StringMultiColumnData::from_rows(&mv_fixture_rows());
        let good = encode_string_multi_column(&col).expect("encode");

        // Layout: [num_rows][offsets x5][ordinals x5]...  Patch offset[2]
        // (bytes 12..16) from 3 to 4 and offset[3] from 3 to 2 to break
        // monotonicity.
        let mut bad = good.clone();
        bad[12..16].copy_from_slice(&4_u32.to_be_bytes()); // offset[2]: 3 -> 4
        bad[16..20].copy_from_slice(&2_u32.to_be_bytes()); // offset[3]: 3 -> 2
        let err = decode_string_multi_column(&bad).expect_err("must reject");
        assert!(
            format!("{err:?}").contains("monotonic"),
            "expected monotonicity rejection, got: {err:?}"
        );

        // First offset != 0.
        let mut bad = good.clone();
        bad[4..8].copy_from_slice(&1_u32.to_be_bytes());
        let err = decode_string_multi_column(&bad).expect_err("must reject");
        assert!(
            format!("{err:?}").contains("start at 0"),
            "expected first-offset rejection, got: {err:?}"
        );

        // Ordinal past the dictionary: ordinals start at byte 4 + 5*4 = 24.
        let mut bad = good.clone();
        bad[24..28].copy_from_slice(&99_u32.to_be_bytes());
        let err = decode_string_multi_column(&bad).expect_err("must reject");
        assert!(
            format!("{err:?}").contains("out of bounds"),
            "expected ordinal-out-of-bounds rejection, got: {err:?}"
        );

        // Trailing bytes.
        let mut bad = good.clone();
        bad.push(0xAB);
        let err = decode_string_multi_column(&bad).expect_err("must reject");
        assert!(
            format!("{err:?}").contains("trailing bytes"),
            "expected trailing-bytes rejection, got: {err:?}"
        );

        // Oversized num_rows header rejected before allocation.
        let err = decode_string_multi_column(&u32::MAX.to_be_bytes()).expect_err("must reject");
        assert!(
            format!("{err:?}").contains("exceeds cap"),
            "expected num_rows cap rejection, got: {err:?}"
        );

        // The untouched blob still decodes.
        assert!(decode_string_multi_column(&good).is_ok());
    }

    /// compat-11 R2 (HIGH): an over-declared `num_bitmaps` header must be
    /// rejected BEFORE the bitmap `Vec` is allocated.  Pre-fix, a tiny
    /// blob declaring 16,777,215 bitmaps reserved hundreds of MiB in
    /// `with_capacity` and only then failed on truncation ("unexpected
    /// end of data") — the pre-allocation cardinality check now fires
    /// first, with a message naming the mismatch.
    #[test]
    fn string_multi_column_rejects_over_declared_num_bitmaps() {
        let col = StringMultiColumnData::from_rows(&mv_fixture_rows());
        let good = encode_string_multi_column(&col).expect("encode");

        // Layout: [num_rows=4][offsets x5][ordinals x5][dict_len][dict]
        // [num_bitmaps]...  → dict_len is at byte 44, num_bitmaps at
        // 48 + dict_len.
        let dict_len =
            u32::from_be_bytes(good[44..48].try_into().expect("dict_len bytes")) as usize;
        let nb_pos = 48 + dict_len;

        // 16,777,215 is below MAX_STRING_BITMAPS, so only the new
        // cardinality/remaining-bytes pre-checks can reject it.
        let mut bad = good.clone();
        bad[nb_pos..nb_pos + 4].copy_from_slice(&16_777_215_u32.to_be_bytes());
        let err = decode_string_multi_column(&bad).expect_err("must reject");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("num_bitmaps") && msg.contains("does not match dictionary cardinality"),
            "expected the pre-allocation num_bitmaps mismatch rejection, got: {msg}"
        );

        // A small mismatch (dict_card=3 has 4 bitmaps incl. the trailing
        // null bitmap; declare 6) is equally corrupt — same loud error.
        let mut bad = good.clone();
        bad[nb_pos..nb_pos + 4].copy_from_slice(&6_u32.to_be_bytes());
        let err = decode_string_multi_column(&bad).expect_err("must reject");
        assert!(
            format!("{err:?}").contains("does not match dictionary cardinality"),
            "expected mismatch rejection for a small over-declare, got: {err:?}"
        );

        // The untouched blob still decodes.
        assert!(decode_string_multi_column(&good).is_ok());
    }

    /// `ColumnData::StringMulti` reports the row count from the offset
    /// table, not the element count.
    #[test]
    fn string_multi_column_num_rows() {
        let col = StringMultiColumnData::from_rows(&mv_fixture_rows());
        assert_eq!(col.ordinals.len(), 5, "5 total elements");
        assert_eq!(ColumnData::StringMulti(col).num_rows(), Some(4));
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
