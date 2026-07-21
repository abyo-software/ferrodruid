// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Reader for segments written by **upstream Apache Druid** (the on-disk
//! layout emitted by real Druid clusters), as opposed to FerroDruid's own
//! private segment serialization in [`crate::v9`] / [`crate::fdx`].
//!
//! # Provenance (clean room)
//!
//! Everything in this module was reverse-engineered from the **observed
//! bytes** of real segments written by Apache Druid 31.0.2 for the
//! `wikipedia_compat` fixtures (captured 2026-07-12; verbatim samples live in
//! `crates/ferrodruid-segment/testdata/druid31_*`) and by Apache Druid
//! 27.0.0 for the legacy v1 numeric serdes (captured 2026-07-21; verbatim
//! samples in `testdata/druid27_*`, values cross-checked against Druid 27's
//! own `dump-segment` output), cross-checked against the values that were
//! ingested, plus the public segment-format documentation. No upstream
//! source code was referenced. Field names below are descriptive labels
//! chosen by us for what the bytes were observed to contain.
//!
//! # Observed `index.drd` layout (Druid 31.0.2, segment format v9)
//!
//! ```text
//! generic-indexed<string>  all column names (metrics first, then dimensions)
//! generic-indexed<string>  dimension names
//! i64 BE                   interval start (epoch millis)
//! i64 BE                   interval end   (epoch millis)
//! u32 BE + JSON            bitmap codec descriptor, e.g. {"type":"roaring"}
//! generic-indexed          trailer list, one entry per column   (observed all-null)
//! generic-indexed          trailer list, one entry per dimension (observed all-null)
//! ```
//!
//! # Observed "generic indexed" container layout
//!
//! ```text
//! u8         version (0x01)
//! u8         flags   (0x01 observed on sorted lists, 0x00 otherwise)
//! u32 BE     body size in bytes (everything after this field)
//! u32 BE     element count
//! u32 BE ×N  end offset of each element within the value region
//! values     per element: [i32 BE marker: 0 = present, -1 = null][payload]
//! ```
//!
//! # Observed column blob layout
//!
//! Every column file embeds its descriptor: `[u32 BE json_len][descriptor
//! JSON]` followed by the part payload described by `parts[0].type`:
//!
//! * `longV2` / `doubleV2` / `floatV2` (a `[u32 BE part_len]` prefix, then):
//!   `[u8 version=2][u32 BE num_values][u32 BE values_per_block]
//!    [u8 compression][blocks]` where compression `0x01` = LZ4 raw blocks
//!   wrapped in a generic-indexed container, `0xff` = uncompressed blocks in
//!   the same container, and `0xfe` = no container, raw contiguous values.
//!   Values are little-endian within each decompressed block.
//!
//!   **Nullable numeric columns** (SQL-compatible null handling, the
//!   Druid 28+ default): the part serde JSON carries a `bitmapSerdeFactory`
//!   entry (observed `{"type":"roaring"}` on every druid31_* numeric
//!   fixture, null-free ones included), and when any row is NULL the value
//!   part is followed by a trailing null section: `[u32 BE size][portable
//!   Roaring bitmap]` whose set bits are the NULL row indices (the in-block
//!   value at such a row is a placeholder). A column with no NULL rows
//!   appends nothing (byte-verified: all null-free druid31_* fixtures end
//!   exactly at the part boundary). NOTE: the null-section framing itself
//!   (BE length prefix + portable Roaring, nothing after it) is a first
//!   approximation derived from Druid's public docs and this format's own
//!   framing conventions — it is pending byte-verification against a
//!   captured real nullable segment (harness test
//!   `druid_writes_ferrodruid_reads_nullable_numeric`).
//!
//!   **`longEncoding: auto`** (table/delta bit-packed longs): the part
//!   header's compression byte lands in `0x80..=0xFD` (the plain ids
//!   `0x01`/`0xfe`/`0xff` all sit outside that range); adding `0x7e`
//!   (mod 256) recovers the plain block-compression id. Byte-anchored:
//!   a real auto segment (default LZ4 compression) was observed to carry
//!   `0x83`, i.e. LZ4 `0x01` flagged. Two extra header bytes follow the
//!   flagged compression byte — `[u8 encoding format: 0x00 = delta,
//!   0x01 = table][u8 encoding-header version = 0x01]` — then per format:
//!
//!   * delta: `[i64 BE base][u32 BE bit width]`; each decoded value is
//!     `base + offset` (Java-faithful wrapping add).
//!   * table: `[u32 BE table size, 1..=256][i64 BE × size]` lookup values;
//!     the per-row index bit width is the smallest supported width whose
//!     range covers the table size.
//!
//!   The per-value stream is fixed-width bit-packed **MSB-first** (supported
//!   widths 1/2/4/8/12/16/20/24/32/40/48/56/64) inside the SAME block
//!   framing as plain longs: each block holds `ceil(bits×values/8)` packed
//!   bytes plus 4 zero closing bytes; flagged-`none` (`0x80`) stores one
//!   contiguous packed run with no block container. NOTE: beyond the flag
//!   byte, this sub-layout is a first approximation derived from the
//!   public format documentation — it is pending byte-verification against
//!   a captured real auto segment (harness test
//!   `druid_writes_ferrodruid_reads_auto_long`) — BUT the v1-serde captures
//!   below byte-verified the whole auto sub-layout (flagged-LZ4 `0x83`,
//!   TABLE with 4-bit ids, DELTA with base + 12/32-bit offsets, closing
//!   pad), and the supplier payload is byte-identical across serde
//!   generations.
//! * **Legacy v1 numeric serdes** `long` / `double` / `float` (written by
//!   Druid 27 and earlier — the pre-SQL-null era): the SAME supplier
//!   payload as the v2 serdes (`[u8 version=2][u32 BE num_values]
//!   [u32 BE values_per_block][u8 compression][blocks]`) embedded directly
//!   after the descriptor **without** the `[u32 BE part_len]` prefix, and
//!   with no `bitmapSerdeFactory` / trailing null section (v1 columns are
//!   plain non-null; trailing bytes fail loudly). Byte-verified against
//!   real Druid 27.0.0 segments (`testdata/druid27_*`, 2026-07-21): plain
//!   LZ4 longs/doubles/floats decode bit-exactly to what Druid 27's
//!   `dump-segment` reports, as do `longEncoding: auto` TABLE and DELTA
//!   columns. An unobserved supplier version byte (anything but `0x02`)
//!   fails loudly.
//! * `stringDictionary`:
//!   `[u8 version][u32 BE feature flags (version 2 only, observed 0)]`
//!   then a generic-indexed string dictionary (sorted), then the per-row
//!   ordinal section, then a generic-indexed list of value bitmaps
//!   (portable Roaring, no per-bitmap type tag — the codec is named by the
//!   descriptor's `bitmapSerdeFactory`).
//!
//!   The per-row ordinal section comes in two observed forms:
//!   * version `0x02` (compressed): `[u8 bytes_per_value][u32 BE num_rows]
//!     [u32 BE rows_per_block][u8 compression][generic-indexed blocks]`,
//!     values **little-endian** within `bytes_per_value`.
//!   * version `0x00` (uncompressed): `[u8 bytes_per_value][u32 BE
//!     total_bytes][values]`, values **big-endian**, followed by
//!     `4 - bytes_per_value` padding bytes included in `total_bytes`.
//!
//! # Complex (sketch) columns
//!
//! A `complex` part is a generic-indexed container of per-row serialized
//! sketch blobs, written directly after the embedded descriptor with no
//! extra length prefix (the same convention as `stringDictionary`).  Only
//! `typeName: "thetaSketch"` decodes — each per-row blob is an Apache
//! DataSketches compact Theta image, decoded byte-for-byte by
//! [`ferrodruid_sketches::ThetaSketch::from_druid_compact`] into a
//! union-only Druid-origin sketch (compat-8 sketch #2).  Every other
//! complex type (`hyperUnique`, `HLLSketch`, `quantilesDoublesSketch`, …)
//! stays a loud rejection: their serialized forms are not reducible to a
//! `(theta, retained_hashes)` state.
//!
//! # Known-unsupported upstream features (fail loudly, never guess)
//!
//! * Multi-value string dimensions (`hasMultipleValues: true`).
//! * Concise bitmaps, LZF/ZSTD block compression (not observed; rejected).
//! * Complex/sketch columns other than `thetaSketch` (see above).

use ferrodruid_bitmap::DruidBitmap;
use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_compression as compression;
use ferrodruid_dict::FrontCodedDictionary;
use serde::Deserialize;

use crate::column::{ColumnData, StringColumnData};
use crate::segment::Interval;

// ---------------------------------------------------------------------------
// Bounded-reader caps (mirror the Wave 35-37 discipline in v9/FDX/column.rs)
// ---------------------------------------------------------------------------

/// Hard upper bound on the element count of a single generic-indexed
/// container (2^24 = 16,777,216). One bitmap exists per dictionary entry, so
/// this cap mirrors `MAX_STRING_BITMAPS` in [`crate::column`].
///
/// [`parse_generic_indexed`] refuses any container declaring more than this
/// (intersected with the caller's own `max_elements`), so a writer that emits
/// a larger container would produce a segment this reader — and Druid — can
/// never reopen.
///
/// `pub(crate)` so the native writer ([`crate::druid_native_writer`]) can
/// reject an over-cap generic-indexed container (e.g. a string dictionary with
/// more than 2^24 distinct values) against the SAME limit this reader
/// enforces, rather than emitting a container nothing can reopen.
pub(crate) const MAX_GI_ELEMENTS: usize = 16 * 1024 * 1024;

/// A single column can hold at most this many values (2^26 = 67,108,864).
///
/// This module parses FOREIGN on-disk bytes reached via the migration
/// importer, and FerroDruid holds decoded segments fully in-heap
/// (measured ~310 B/row; RSS crosses 4 GB near 13 M rows — W-6 memory
/// evidence), so an implausible declared count must fail loud instead of
/// allocating: pre-fix, the 2^28 cap let a ~32 MiB `bits = 1` TABLE part
/// reserve ~2 GiB of packed ids plus ~2 GiB of looked-up longs. Real
/// Druid targets ~5 M rows per segment, so 2^26 sits comfortably above
/// any legitimate column (13×) while keeping the worst count-sized
/// reservation (8 B/value) at 512 MiB. Enforced in
/// [`read_numeric_part_head`] (longV2 plain + auto, doubleV2, floatV2)
/// and in [`decode_row_ordinals`], BEFORE anything is sized from the
/// count. (Deliberately stricter than `MAX_NUMERIC_COLUMN_VALUES` in
/// [`crate::column`], which reads our own writer's output.)
///
/// `pub(crate)` so the native writer ([`crate::druid_native_writer`]) can
/// reject an over-cap segment against the SAME limit this reader enforces,
/// rather than emitting a segment the reader can never reopen.
pub(crate) const MAX_COLUMN_VALUES: usize = 1 << 26;

/// Hard upper bound on a single decompressed block (64 MiB — real Druid
/// blocks are 64 KiB; the cap leaves 1000× headroom while keeping a crafted
/// `values_per_block` from driving a multi-GiB allocation).
const MAX_BLOCK_BYTES: usize = 64 * 1024 * 1024;

/// Hard upper bound on the embedded column-descriptor JSON length.
const MAX_DESCRIPTOR_JSON: usize = 1024 * 1024;

/// Hard upper bound on a single per-row compact-theta blob (mirrors
/// `MAX_THETA_BLOB_BYTES` in [`crate::column`], the private rewrite
/// decoder's cap).  64 MiB holds ~8.4 M retained hashes — far beyond any
/// real per-row rollup sketch — while an uncapped GenericIndexed row
/// could declare the family-maximum 2^26 hashes (a 512 MiB image) and
/// drive a multi-GiB `BTreeSet` materialization out of a single row.
/// Checked against the ACTUAL blob length BEFORE
/// `ThetaSketch::from_druid_compact` materializes anything (that decoder
/// additionally enforces `curCount * 8 == remaining bytes` exactly, so a
/// blob claiming more hashes than its bytes hold is rejected without a
/// count-sized allocation).
const MAX_THETA_BLOB_BYTES: usize = 64 * 1024 * 1024;

/// Compression strategy ids as observed in real Druid 31.0.2 segments.
///
/// `0x01` verified by decompressing LZ4 blocks back to the ingested values;
/// `0xfe` verified via `metricCompression: "none"` (raw contiguous values,
/// no block container); `0xff` verified via `metricCompression:
/// "uncompressed"` (uncompressed blocks inside the block container).
const COMPRESSION_LZ4: u8 = 0x01;
const COMPRESSION_NONE_RAW: u8 = 0xfe;
const COMPRESSION_UNCOMPRESSED: u8 = 0xff;

// ---------------------------------------------------------------------------
// Bounded byte-cursor helpers
// ---------------------------------------------------------------------------

fn need(data: &[u8], pos: usize, n: usize, what: &str) -> Result<()> {
    if pos.checked_add(n).is_none_or(|end| end > data.len()) {
        return Err(DruidError::Segment(format!(
            "druid-native: truncated reading {what}: need {n} bytes at offset {pos}, have {}",
            data.len()
        )));
    }
    Ok(())
}

fn read_u8(data: &[u8], pos: &mut usize, what: &str) -> Result<u8> {
    need(data, *pos, 1, what)?;
    let v = data[*pos];
    *pos += 1;
    Ok(v)
}

fn read_u32_be(data: &[u8], pos: &mut usize, what: &str) -> Result<u32> {
    need(data, *pos, 4, what)?;
    let v = u32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos += 4;
    Ok(v)
}

fn read_i64_be(data: &[u8], pos: &mut usize, what: &str) -> Result<i64> {
    need(data, *pos, 8, what)?;
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*pos..*pos + 8]);
    *pos += 8;
    Ok(i64::from_be_bytes(buf))
}

// ---------------------------------------------------------------------------
// Generic-indexed container
// ---------------------------------------------------------------------------

/// Parse one generic-indexed container starting at `*pos`.
///
/// Returns the elements (`None` = the observed `-1` null marker) and leaves
/// `*pos` just past the container.
///
/// `max_elements` is the caller's own upper bound on how many elements this
/// particular container may legitimately hold (column caps, expected block
/// counts, dictionary cardinality, …); it is intersected with the module-wide
/// [`MAX_GI_ELEMENTS`] ceiling and enforced against the declared count
/// **before** the offset table or the element vector is allocated, so a
/// hostile count is rejected without materializing anything sized from it.
pub(crate) fn parse_generic_indexed<'a>(
    data: &'a [u8],
    pos: &mut usize,
    max_elements: usize,
    what: &str,
) -> Result<Vec<Option<&'a [u8]>>> {
    let version = read_u8(data, pos, what)?;
    if version != 1 {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: unsupported generic-indexed version {version}"
        )));
    }
    let _flags = read_u8(data, pos, what)?;
    let body_size = read_u32_be(data, pos, what)? as usize;
    need(data, *pos, body_size, what)?;
    let body_end = *pos + body_size;
    // The element count and offset table live INSIDE `body_size`. Bound every
    // following read to the declared body: reading them from the full input
    // would let a lying `body_size` push `*pos` past `body_end` and underflow
    // the value-region length below.
    let body = &data[..body_end];

    let num = read_u32_be(body, pos, what)? as usize;
    let cap = max_elements.min(MAX_GI_ELEMENTS);
    if num > cap {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: element count {num} exceeds cap {cap}"
        )));
    }
    need(body, *pos, num.saturating_mul(4), what)?;
    let mut end_offsets = Vec::with_capacity(num);
    for _ in 0..num {
        end_offsets.push(read_u32_be(body, pos, what)? as usize);
    }

    // All reads above were bounded to `body`, so `*pos <= body_end` holds and
    // this subtraction cannot underflow.
    let values_start = *pos;
    let values_len = body_end - values_start;
    let mut elements = Vec::with_capacity(num);
    let mut prev = 0usize;
    for (i, &end) in end_offsets.iter().enumerate() {
        if end < prev || end > values_len {
            return Err(DruidError::Segment(format!(
                "druid-native: {what}: element {i} has invalid end offset {end} \
                 (prev {prev}, value region {values_len})"
            )));
        }
        let elem = &body[values_start + prev..values_start + end];
        if elem.len() < 4 {
            return Err(DruidError::Segment(format!(
                "druid-native: {what}: element {i} shorter than its 4-byte marker"
            )));
        }
        let marker = i32::from_be_bytes([elem[0], elem[1], elem[2], elem[3]]);
        match marker {
            0 => elements.push(Some(&elem[4..])),
            -1 => {
                if elem.len() != 4 {
                    return Err(DruidError::Segment(format!(
                        "druid-native: {what}: null element {i} carries {} payload bytes",
                        elem.len() - 4
                    )));
                }
                elements.push(None);
            }
            other => {
                return Err(DruidError::Segment(format!(
                    "druid-native: {what}: element {i} has unknown marker {other}"
                )));
            }
        }
        prev = end;
    }
    if prev != values_len {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: {} trailing bytes after last element",
            values_len - prev
        )));
    }
    *pos = body_end;
    Ok(elements)
}

/// Decode a generic-indexed container of UTF-8 strings. Null elements map to
/// `None`.
///
/// `max_elements` bounds the declared element count before anything is
/// materialized (see [`parse_generic_indexed`]).
fn parse_string_list(
    data: &[u8],
    pos: &mut usize,
    max_elements: usize,
    what: &str,
) -> Result<Vec<Option<String>>> {
    let elems = parse_generic_indexed(data, pos, max_elements, what)?;
    let mut out = Vec::with_capacity(elems.len());
    for (i, e) in elems.into_iter().enumerate() {
        match e {
            None => out.push(None),
            Some(bytes) => {
                let s = std::str::from_utf8(bytes).map_err(|e| {
                    DruidError::Segment(format!(
                        "druid-native: {what}: element {i} is not valid UTF-8: {e}"
                    ))
                })?;
                out.push(Some(s.to_string()));
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// index.drd
// ---------------------------------------------------------------------------

/// Parsed upstream-Druid `index.drd`.
pub(crate) struct NativeIndexDrd {
    /// Dimension column names in declaration order.
    pub dimensions: Vec<String>,
    /// Metric column names in declaration order.
    pub metrics: Vec<String>,
    /// Segment time interval.
    pub interval: Interval,
}

/// Whether `index.drd` bytes look like the upstream Apache Druid layout
/// rather than FerroDruid's private layout.
///
/// The private layout starts with a BE i32 format version (9 or 10); the
/// upstream layout starts with a generic-indexed container whose first two
/// bytes are `01 00`/`01 01`, so its leading BE i32 is 0x0100_0000 or
/// 0x0101_0000 — never 9 or 10. Callers peek the version first and only
/// fall through to the native parser when it doesn't match.
pub(crate) fn parse_native_index_drd(
    data: &[u8],
    max_dimensions: usize,
    max_metrics: usize,
) -> Result<NativeIndexDrd> {
    let mut pos = 0usize;

    // Thread the caps INTO the list parses: a real segment has at most
    // `max_dimensions + max_metrics` columns, so the element count of each
    // list is bounded before any element vector or string is materialized.
    // (Pre-fix, a hostile list of ~16M empty names allocated ~700-800 MiB
    // of temporaries from ~128 MiB of input before the late cap fired.)
    let max_columns = max_dimensions.saturating_add(max_metrics);
    let all_columns = parse_string_list(data, &mut pos, max_columns, "index.drd column list")?;
    let dimensions = parse_string_list(data, &mut pos, max_dimensions, "index.drd dimension list")?;
    let start_millis = read_i64_be(data, &mut pos, "index.drd interval start")?;
    let end_millis = read_i64_be(data, &mut pos, "index.drd interval end")?;

    // Trailer: `[u32 BE len][bitmap codec JSON]` then two per-column /
    // per-dimension generic-indexed lists (observed all-null). Only the
    // bitmap codec matters for reading; tolerate anything after it.
    let bitmap_json_len = read_u32_be(data, &mut pos, "index.drd bitmap codec length")? as usize;
    if bitmap_json_len > MAX_DESCRIPTOR_JSON {
        return Err(DruidError::Segment(format!(
            "druid-native: index.drd bitmap codec JSON length {bitmap_json_len} exceeds cap \
             {MAX_DESCRIPTOR_JSON}"
        )));
    }
    need(data, pos, bitmap_json_len, "index.drd bitmap codec JSON")?;
    let bitmap_json = &data[pos..pos + bitmap_json_len];
    let bitmap_json = std::str::from_utf8(bitmap_json).map_err(|e| {
        DruidError::Segment(format!(
            "druid-native: index.drd bitmap codec not UTF-8: {e}"
        ))
    })?;
    if !bitmap_json.contains("roaring") {
        return Err(DruidError::Segment(format!(
            "druid-native: unsupported bitmap codec {bitmap_json} (only roaring is supported)"
        )));
    }

    let mut resolved_dims = Vec::with_capacity(dimensions.len());
    for (i, d) in dimensions.into_iter().enumerate() {
        match d {
            Some(name) => resolved_dims.push(name),
            None => {
                return Err(DruidError::Segment(format!(
                    "druid-native: index.drd dimension {i} is null"
                )));
            }
        }
    }
    // `resolved_dims.len() <= max_dimensions` and `all_columns.len() <=
    // max_columns` are already guaranteed by the caps threaded into
    // `parse_string_list` above, so the O(columns × dimensions) membership
    // work below is bounded too.
    // Metrics = declared columns that are not dimensions, preserving the
    // column-list order (metrics were observed listed first). Membership is
    // an O(1) hash lookup, not a per-column linear scan of `resolved_dims`.
    let dim_set: std::collections::HashSet<&String> = resolved_dims.iter().collect();
    let mut metrics = Vec::new();
    for (i, c) in all_columns.iter().enumerate() {
        match c {
            Some(name) => {
                if !dim_set.contains(name) {
                    metrics.push(name.clone());
                }
            }
            None => {
                return Err(DruidError::Segment(format!(
                    "druid-native: index.drd column {i} is null"
                )));
            }
        }
    }
    if metrics.len() > max_metrics {
        return Err(DruidError::Segment(format!(
            "index.drd: num_metrics {} exceeds cap {max_metrics}",
            metrics.len()
        )));
    }

    Ok(NativeIndexDrd {
        dimensions: resolved_dims,
        metrics,
        interval: Interval {
            start_millis,
            end_millis,
        },
    })
}

// ---------------------------------------------------------------------------
// Embedded column descriptor
// ---------------------------------------------------------------------------

/// The embedded column descriptor found at the head of every upstream-Druid
/// column blob: `[u32 BE json_len][descriptor JSON]`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeColumnDescriptor {
    value_type: String,
    #[serde(default)]
    has_multiple_values: bool,
    #[serde(default)]
    parts: Vec<NativePart>,
}

/// One entry of the descriptor's `parts` list.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativePart {
    #[serde(rename = "type")]
    part_type: String,
    #[serde(default)]
    byte_order: Option<String>,
    /// The complex type of a `complex` part (e.g. `thetaSketch`,
    /// `hyperUnique`).  Only `thetaSketch` decodes; every other name is a
    /// loud rejection (see the module docs).
    #[serde(default)]
    type_name: Option<String>,
    /// The codec of the trailing null-value bitmap on nullable numeric
    /// parts. Observed as `{"type":"roaring"}` on every druid31_* numeric
    /// fixture (null-free ones included), so its presence marks the column
    /// as *nullable*, not as *null-bearing* — nulls exist only when bytes
    /// trail the value part.
    #[serde(default)]
    bitmap_serde_factory: Option<NativeBitmapSerdeFactory>,
}

/// The descriptor's `bitmapSerdeFactory` entry, e.g. `{"type":"roaring"}`.
#[derive(Debug, Deserialize)]
struct NativeBitmapSerdeFactory {
    #[serde(rename = "type", default)]
    factory_type: Option<String>,
}

/// Whether a column blob looks like an upstream-Druid column (embedded
/// length-prefixed JSON descriptor) rather than a FerroDruid private column.
///
/// FerroDruid's own writer always emits a `<name>.column_descriptor.json`
/// sidecar entry, so this heuristic is only consulted for descriptor-less
/// blobs, which the private writer never produces.
pub(crate) fn is_native_column(data: &[u8]) -> bool {
    if data.len() < 6 {
        return false;
    }
    let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    (2..=MAX_DESCRIPTOR_JSON).contains(&len) && 4 + len <= data.len() && data[4] == b'{'
}

/// Decode an upstream-Druid column blob into a [`ColumnData`].
pub(crate) fn decode_native_column(data: &[u8]) -> Result<ColumnData> {
    let mut pos = 0usize;
    let json_len = read_u32_be(data, &mut pos, "column descriptor length")? as usize;
    if json_len > MAX_DESCRIPTOR_JSON {
        return Err(DruidError::Segment(format!(
            "druid-native: descriptor JSON length {json_len} exceeds cap {MAX_DESCRIPTOR_JSON}"
        )));
    }
    need(data, pos, json_len, "column descriptor JSON")?;
    let descriptor: NativeColumnDescriptor = serde_json::from_slice(&data[pos..pos + json_len])
        .map_err(|e| {
            DruidError::Segment(format!("druid-native: bad embedded column descriptor: {e}"))
        })?;
    pos += json_len;

    if descriptor.has_multiple_values {
        return Err(DruidError::Segment(
            "druid-native: multi-value string dimensions are not supported yet".to_string(),
        ));
    }
    let [part] = descriptor.parts.as_slice() else {
        return Err(DruidError::Segment(format!(
            "druid-native: expected exactly 1 descriptor part, got {} (valueType {})",
            descriptor.parts.len(),
            descriptor.value_type
        )));
    };
    if let Some(order) = part.byte_order.as_deref()
        && order != "LITTLE_ENDIAN"
    {
        return Err(DruidError::Segment(format!(
            "druid-native: unsupported byte order {order}"
        )));
    }

    let column = match part.part_type.as_str() {
        "longV2" => ColumnData::Long(read_long_part(data, &mut pos, NumericSerde::V2, "longV2")?),
        "doubleV2" => {
            let raw = read_numeric_part(data, &mut pos, 8, NumericSerde::V2, "doubleV2")?;
            ColumnData::Double(le_f64_values(&raw))
        }
        "floatV2" => {
            let raw = read_numeric_part(data, &mut pos, 4, NumericSerde::V2, "floatV2")?;
            ColumnData::Float(le_f32_values(&raw))
        }
        // Legacy v1 numeric serdes (Druid ≤27): the same compressed-columnar
        // supplier as the v2 serdes, embedded WITHOUT the `[u32 BE part_len]`
        // prefix (see the module docs for the byte evidence).
        "long" => ColumnData::Long(read_long_part(data, &mut pos, NumericSerde::V1, "long")?),
        "double" => {
            let raw = read_numeric_part(data, &mut pos, 8, NumericSerde::V1, "double")?;
            ColumnData::Double(le_f64_values(&raw))
        }
        "float" => {
            let raw = read_numeric_part(data, &mut pos, 4, NumericSerde::V1, "float")?;
            ColumnData::Float(le_f32_values(&raw))
        }
        "stringDictionary" => decode_string_dictionary_part(data, &mut pos)?,
        "complex" => decode_complex_part(data, &mut pos, part)?,
        other => {
            return Err(DruidError::Segment(format!(
                "druid-native: unsupported column part type `{other}` (valueType {})",
                descriptor.value_type
            )));
        }
    };
    // Trailing null-value section (SQL-compatible nulls, Druid 28+ default):
    // a NULLABLE numeric part — one whose serde declares a
    // `bitmapSerdeFactory` — may be followed by a framed portable-Roaring
    // bitmap of the NULL row indices (see the module docs; the framing is
    // pending byte-verification against a captured real nullable segment).
    // Every null-free column observed (all druid31_* fixtures) ends exactly
    // at the part boundary, so the section is decoded only when trailing
    // bytes actually exist.
    let column = if pos < data.len()
        && matches!(part.part_type.as_str(), "longV2" | "doubleV2" | "floatV2")
        && part.bitmap_serde_factory.is_some()
    {
        let nulls = decode_numeric_null_section(data, &mut pos, part, &column)?;
        apply_numeric_nulls(column, &nulls)
    } else {
        column
    };
    // Any OTHER trailing bytes are a layout we do not understand (string
    // parts never carry a null section — their nulls live in the dictionary
    // — and a numeric part without a `bitmapSerdeFactory` has nothing that
    // could name the trailing section's codec). Silently skipping such bytes
    // would silently DROP data (e.g. turning NULL into 0 and making
    // `crate::null_generation` misclassify a genuinely modern column as
    // legacy-consistent). Fail loudly, never guess.
    if pos != data.len() {
        return Err(DruidError::Segment(format!(
            "druid-native: {} trailing bytes after the `{}` part — this column uses a layout \
             extension that is not supported yet (numeric null bitmaps are only read when \
             the part declares a bitmapSerdeFactory)",
            data.len() - pos,
            part.part_type
        )));
    }
    Ok(column)
}

// ---------------------------------------------------------------------------
// Trailing null-value bitmap (nullable numeric columns, SQL-compatible nulls)
// ---------------------------------------------------------------------------

/// Decode the trailing null-value section of a nullable numeric column:
/// `[u32 BE size][portable Roaring bitmap]`, where the set bits are the row
/// indices whose value is SQL NULL. Only called when trailing bytes exist
/// and the part declares a `bitmapSerdeFactory`.
///
/// The section must consume the blob exactly: the null bitmap is the last
/// thing in every layout we understand, so a framed length that leaves
/// bytes behind (or overruns the buffer) is refused rather than guessed at.
fn decode_numeric_null_section(
    data: &[u8],
    pos: &mut usize,
    part: &NativePart,
    column: &ColumnData,
) -> Result<DruidBitmap> {
    let codec = part
        .bitmap_serde_factory
        .as_ref()
        .and_then(|f| f.factory_type.as_deref())
        .unwrap_or("<unspecified>");
    if codec != "roaring" {
        return Err(DruidError::Segment(format!(
            "druid-native: {}: null-value bitmap codec `{codec}` is not supported \
             (only roaring)",
            part.part_type
        )));
    }
    let num_rows = column.num_rows().unwrap_or(0);
    let size = read_u32_be(data, pos, "null-value bitmap length")? as usize;
    // `read_u32_be` bounds `*pos` to the buffer, so this cannot underflow.
    let remaining = data.len() - *pos;
    if size != remaining {
        return Err(DruidError::Segment(format!(
            "druid-native: {}: null-value bitmap section declares {size} bytes but {remaining} \
             bytes remain after its length frame — refusing to guess at the layout",
            part.part_type
        )));
    }
    let bitmap = if size == 0 {
        DruidBitmap::new()
    } else {
        // Bounded against the column's row count BEFORE deserialization
        // (same discipline as the string value bitmaps above).
        DruidBitmap::deserialize_portable(&data[*pos..*pos + size], num_rows as u64).map_err(
            |e| {
                DruidError::Segment(format!(
                    "druid-native: {}: null-value bitmap failed to decode: {e}",
                    part.part_type
                ))
            },
        )?
    };
    *pos += size;
    // A set bit at or beyond the row count would index out of range when the
    // nulls are applied (and can never be legitimate).
    if let Some(max_row) = bitmap.as_inner().max()
        && max_row as usize >= num_rows
    {
        return Err(DruidError::Segment(format!(
            "druid-native: {}: null-value bitmap references row {max_row} but the column \
             has only {num_rows} rows",
            part.part_type
        )));
    }
    Ok(bitmap)
}

/// Overwrite the NULL rows of a decoded numeric column with FerroDruid's
/// in-memory NULL markers (see the `crate::column` null-value convention):
/// NaN for `DOUBLE`/`FLOAT`.  `i64` has no in-band NULL marker, so a
/// null-bearing `LONG` column becomes [`ColumnData::LongNullable`] — the
/// exact decoded `i64` values (normalized to `0` at NULL rows, matching the
/// ingest-side convention) plus the decoded null-row bitmap.  Values beyond
/// ±2^53 are preserved exactly.  A null-free `LONG` column stays `LONG`.
/// (Before 2026-07 the null-bearing LONG case was converted to a NaN-null
/// `DOUBLE`, silently losing precision beyond ±2^53.)
///
/// Every row index in `nulls` was validated against the column's row count
/// by [`decode_numeric_null_section`], so the indexing below cannot go out
/// of bounds.
fn apply_numeric_nulls(column: ColumnData, nulls: &DruidBitmap) -> ColumnData {
    if nulls.is_empty() {
        return column;
    }
    match column {
        ColumnData::Double(mut v) => {
            for row in nulls.iter() {
                v[row as usize] = crate::column::NULL_DOUBLE;
            }
            ColumnData::Double(v)
        }
        ColumnData::Float(mut v) => {
            for row in nulls.iter() {
                v[row as usize] = crate::column::NULL_FLOAT;
            }
            ColumnData::Float(v)
        }
        ColumnData::Long(mut v) => {
            // Normalize NULL rows' in-band values to 0 so the value vector
            // never leaks whatever placeholder the upstream writer stored
            // (usually 0 already) — deterministic and matches the
            // ingest-side `long_nullable_parts` convention.
            for row in nulls.iter() {
                v[row as usize] = 0;
            }
            ColumnData::LongNullable(v, nulls.clone())
        }
        // Unreachable — the caller gates on numeric part types — but stay
        // total rather than panic on a future edit.
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Numeric parts (longV2 / doubleV2 / floatV2, and legacy long / double /
// float)
// ---------------------------------------------------------------------------

/// Which serde generation framed a numeric part.
///
/// Both generations wrap the SAME compressed-columnar supplier payload
/// (`[u8 version=2][u32 BE num_values][u32 BE values_per_block]
/// [u8 compression][blocks]`); they differ only in the outer framing —
/// byte-verified against real Druid 27.0.0 (v1) and 31.0.2 (v2) segments.
#[derive(Clone, Copy, PartialEq, Eq)]
enum NumericSerde {
    /// Legacy `long` / `double` / `float` (Druid 27 and earlier): the
    /// supplier bytes follow the descriptor directly, with NO length
    /// prefix — the part owns the remainder of the column blob.
    V1,
    /// Modern `longV2` / `doubleV2` / `floatV2` (Druid 28+): a
    /// `[u32 BE part_len]` prefix bounds the supplier bytes, and a nullable
    /// column may append a trailing null-bitmap section after the part.
    V2,
}

/// The shared head of every numeric part: the optional v2 `[u32 BE
/// part_len]` prefix, then `[u8 version=2][u32 BE num_values]
/// [u32 BE values_per_block][u8 compression]`.
struct NumericPartHead {
    /// End offset of the declared part within the column blob.
    part_end: usize,
    /// Declared value (row) count, capped by [`MAX_COLUMN_VALUES`].
    num_values: usize,
    /// Declared values per compression block.
    values_per_block: usize,
    /// Raw compression / encoding marker byte (see the module docs).
    compression: u8,
}

/// Read the numeric-part head, leaving `*pos` at the first payload byte.
fn read_numeric_part_head(
    data: &[u8],
    pos: &mut usize,
    serde: NumericSerde,
    what: &str,
) -> Result<NumericPartHead> {
    let part_end = match serde {
        NumericSerde::V2 => {
            // v2 numeric parts carry their own `[u32 BE part_len]` prefix.
            let part_len = read_u32_be(data, pos, what)? as usize;
            need(data, *pos, part_len, what)?;
            *pos + part_len
        }
        // The legacy v1 serdes have no length prefix: the supplier owns
        // everything left in the column blob. Every observed v1 column ends
        // exactly at its last block (they predate SQL-null storage, so no
        // null section can follow), and `expect_part_consumed` still
        // enforces exact consumption — trailing bytes fail loudly.
        NumericSerde::V1 => data.len(),
    };
    // Bound every following read to the declared part: a lying `part_len`
    // must not let the header or value blocks read bytes owned by whatever
    // follows the part, nor push `*pos` past `part_end` (which would
    // underflow the trailing-byte computation in the callers).
    let part = &data[..part_end];

    let version = read_u8(part, pos, what)?;
    if version != 2 {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: unsupported part version {version}"
        )));
    }
    let num_values = read_u32_be(part, pos, what)? as usize;
    if num_values > MAX_COLUMN_VALUES {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: declared {num_values} values exceeds cap {MAX_COLUMN_VALUES}"
        )));
    }
    let values_per_block = read_u32_be(part, pos, what)? as usize;
    let compression = read_u8(part, pos, what)?;
    Ok(NumericPartHead {
        part_end,
        num_values,
        values_per_block,
        compression,
    })
}

/// Fail unless the part was consumed exactly to its declared end.
fn expect_part_consumed(part_end: usize, pos: usize, what: &str) -> Result<()> {
    match part_end.checked_sub(pos) {
        Some(0) => Ok(()),
        Some(trailing) => Err(DruidError::Segment(format!(
            "druid-native: {what}: {trailing} trailing bytes inside part"
        ))),
        // Unreachable while every read is bounded to the part, but fail
        // closed rather than underflow if a future edit widens a read.
        None => Err(DruidError::Segment(format!(
            "druid-native: {what}: part reads overran the declared part end"
        ))),
    }
}

/// Read a plain numeric part (`doubleV2` / `floatV2` and the legacy
/// `double` / `float`; long parts go through [`read_long_part`]) and return
/// the concatenated little-endian value bytes (`num_values * width` bytes).
fn read_numeric_part(
    data: &[u8],
    pos: &mut usize,
    width: usize,
    serde: NumericSerde,
    what: &str,
) -> Result<Vec<u8>> {
    let head = read_numeric_part_head(data, pos, serde, what)?;
    let part = &data[..head.part_end];
    let raw = decode_value_blocks(
        part,
        pos,
        head.part_end,
        head.num_values,
        head.values_per_block,
        width,
        head.compression,
        what,
    )?;
    expect_part_consumed(head.part_end, *pos, what)?;
    Ok(raw)
}

/// Little-endian `f64` values from concatenated raw value bytes (the
/// caller guarantees `raw.len()` is a multiple of 8).
fn le_f64_values(raw: &[u8]) -> Vec<f64> {
    raw.chunks_exact(8)
        .map(|c| {
            let mut b = [0u8; 8];
            b.copy_from_slice(c);
            f64::from_le_bytes(b)
        })
        .collect()
}

/// Little-endian `f32` values from concatenated raw value bytes (the
/// caller guarantees `raw.len()` is a multiple of 4).
fn le_f32_values(raw: &[u8]) -> Vec<f32> {
    raw.chunks_exact(4)
        .map(|c| {
            let mut b = [0u8; 4];
            b.copy_from_slice(c);
            f32::from_le_bytes(b)
        })
        .collect()
}

/// Read a long part (`longV2` or the legacy `long`) into `i64` values,
/// handling both `longEncoding: longs` (plain 8-byte little-endian values,
/// the upstream default) and `longEncoding: auto` (table/delta bit-packed
/// values, marked by an encoding flag on the compression byte — see the
/// module docs; byte-verified for BOTH serde generations).
fn read_long_part(
    data: &[u8],
    pos: &mut usize,
    serde: NumericSerde,
    what: &str,
) -> Result<Vec<i64>> {
    let head = read_numeric_part_head(data, pos, serde, what)?;
    let part = &data[..head.part_end];
    let values = if has_long_encoding_flag(head.compression) {
        decode_auto_long_part(part, pos, &head, what)?
    } else {
        let raw = decode_value_blocks(
            part,
            pos,
            head.part_end,
            head.num_values,
            head.values_per_block,
            8,
            head.compression,
            what,
        )?;
        raw.chunks_exact(8)
            .map(|c| {
                let mut b = [0u8; 8];
                b.copy_from_slice(c);
                i64::from_le_bytes(b)
            })
            .collect()
    };
    expect_part_consumed(head.part_end, *pos, what)?;
    Ok(values)
}

// ---------------------------------------------------------------------------
// `longEncoding: auto` (table/delta bit-packed longs)
// ---------------------------------------------------------------------------
//
// Header/bit layout per the module docs. Everything below the flag byte is
// a first approximation pending byte-verification against a captured real
// auto segment (harness test `druid_writes_ferrodruid_reads_auto_long`).

/// First flagged compression byte of the `longEncoding: auto` marker range.
const LONG_ENCODING_FLAG_MIN: u8 = 0x80;
/// Last flagged compression byte — the PLAIN ids `0xfe` (none) and `0xff`
/// (uncompressed) sit just above this range and must not parse as flagged.
const LONG_ENCODING_FLAG_MAX: u8 = 0xFD;
/// Adding this offset (mod 256) to a flagged byte recovers the plain
/// block-compression id: observed `0x83` (auto + default compression) →
/// `0x01` LZ4; `0x80` → `0xfe` none; `0x81` → `0xff` uncompressed.
const LONG_ENCODING_FLAG_CLEAR_OFFSET: u8 = 0x7E;
/// Encoding-format id following the flagged compression byte: delta.
const LONG_ENCODING_FORMAT_DELTA: u8 = 0x00;
/// Encoding-format id following the flagged compression byte: table.
const LONG_ENCODING_FORMAT_TABLE: u8 = 0x01;
/// Version byte of the per-format encoding header.
const LONG_ENCODING_HEADER_VERSION: u8 = 0x01;
/// Hard cap on the lookup-table size of table-encoded longs.
const MAX_LONG_TABLE_SIZE: usize = 256;
/// Zero closing bytes appended to every packed run (per block, and once for
/// a flagged-`none` contiguous stream).
const PACKED_CLOSING_PAD: usize = 4;
/// The fixed bit widths a packed run may use. Any other width in the header
/// is malformed and fails closed.
const SUPPORTED_PACKED_WIDTHS: [u32; 13] = [1, 2, 4, 8, 12, 16, 20, 24, 32, 40, 48, 56, 64];

/// Whether a numeric-part compression byte carries the `longEncoding: auto`
/// flag (only meaningful on `longV2` parts).
fn has_long_encoding_flag(compression: u8) -> bool {
    (LONG_ENCODING_FLAG_MIN..=LONG_ENCODING_FLAG_MAX).contains(&compression)
}

/// Fail closed on a bit width outside [`SUPPORTED_PACKED_WIDTHS`].
fn validate_packed_width(bits: u32, what: &str) -> Result<()> {
    if SUPPORTED_PACKED_WIDTHS.contains(&bits) {
        Ok(())
    } else {
        Err(DruidError::Segment(format!(
            "druid-native: {what}: packed bit width {bits} is not a supported width \
             (supported: {SUPPORTED_PACKED_WIDTHS:?})"
        )))
    }
}

/// The smallest supported bit width whose value range covers `table_size`
/// distinct ids (`0..table_size`). `table_size` is capped at
/// [`MAX_LONG_TABLE_SIZE`] = 256 by the caller, so this never exceeds 8.
fn bits_for_table_size(table_size: usize) -> u32 {
    for &w in &SUPPORTED_PACKED_WIDTHS {
        if w >= 64 || (1u128 << w) >= table_size as u128 {
            return w;
        }
    }
    64
}

/// Byte length of a packed run of `num_values` values at `bits` per value:
/// `ceil(bits × num_values / 8)` packed bytes + the 4 zero closing bytes.
fn packed_run_len(bits: u32, num_values: usize, what: &str) -> Result<usize> {
    (bits as usize)
        .checked_mul(num_values)
        .map(|total_bits| total_bits.div_ceil(8))
        .and_then(|bytes| bytes.checked_add(PACKED_CLOSING_PAD))
        .ok_or_else(|| {
            DruidError::Segment(format!(
                "druid-native: {what}: packed byte count overflows \
                 ({bits} bits × {num_values} values)"
            ))
        })
}

/// Read the `index`-th `bits`-wide value from a contiguous MSB-first packed
/// stream. The caller guarantees `packed` holds at least
/// `ceil((index + 1) × bits / 8)` bytes, so the indexing below cannot go out
/// of bounds.
fn read_packed_be(packed: &[u8], index: usize, bits: u32) -> u64 {
    let width = bits as usize;
    let start_bit = index * width;
    let end_bit = start_bit + width;
    let first = start_bit / 8;
    let last = (end_bit - 1) / 8;
    // A 64-bit value spans at most 9 bytes (72 bits), so a u128 accumulator
    // always holds the full byte window.
    let mut acc: u128 = 0;
    for &b in &packed[first..=last] {
        acc = (acc << 8) | u128::from(b);
    }
    let trailing = (last + 1) * 8 - end_bit;
    let mask = (1u128 << bits) - 1;
    // Masked to at most 64 bits, so the narrowing cast is exact.
    #[allow(clippy::cast_possible_truncation)]
    let value = ((acc >> trailing) & mask) as u64;
    value
}

/// Append `count` packed values decoded from `packed` onto `out`.
fn push_packed_values(out: &mut Vec<u64>, packed: &[u8], count: usize, bits: u32) {
    for i in 0..count {
        out.push(read_packed_be(packed, i, bits));
    }
}

/// Decode a `longEncoding: auto` `longV2` payload (everything after the
/// flagged compression byte) into plain `i64` values.
fn decode_auto_long_part(
    part: &[u8],
    pos: &mut usize,
    head: &NumericPartHead,
    what: &str,
) -> Result<Vec<i64>> {
    let compression = head
        .compression
        .wrapping_add(LONG_ENCODING_FLAG_CLEAR_OFFSET);
    let format = read_u8(part, pos, "long encoding format")?;
    let header_version = read_u8(part, pos, "long encoding header version")?;
    if header_version != LONG_ENCODING_HEADER_VERSION {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: unsupported long-encoding header version {header_version}"
        )));
    }
    match format {
        LONG_ENCODING_FORMAT_DELTA => {
            let base = read_i64_be(part, pos, "delta encoding base")?;
            let bits = read_u32_be(part, pos, "delta encoding bit width")?;
            validate_packed_width(bits, what)?;
            let offsets = decode_packed_long_stream(part, pos, head, bits, compression, what)?;
            Ok(offsets
                .into_iter()
                .map(|off| {
                    // Java-faithful semantics: the upstream reader adds the
                    // (sign-reinterpreted) offset with Java's wrapping long
                    // addition, so wrap rather than reject here.
                    #[allow(clippy::cast_possible_wrap)]
                    let off = off as i64;
                    base.wrapping_add(off)
                })
                .collect())
        }
        LONG_ENCODING_FORMAT_TABLE => {
            let table_size = read_u32_be(part, pos, "long table size")? as usize;
            if table_size == 0 || table_size > MAX_LONG_TABLE_SIZE {
                return Err(DruidError::Segment(format!(
                    "druid-native: {what}: long table size {table_size} out of range \
                     1..={MAX_LONG_TABLE_SIZE}"
                )));
            }
            let mut table = Vec::with_capacity(table_size);
            for _ in 0..table_size {
                table.push(read_i64_be(part, pos, "long table entry")?);
            }
            let bits = bits_for_table_size(table_size);
            let ids = decode_packed_long_stream(part, pos, head, bits, compression, what)?;
            // Sized from the ids actually decoded (input-validated above),
            // never from the declared count.
            let mut out = Vec::with_capacity(ids.len());
            for (row, id) in ids.into_iter().enumerate() {
                let Some(&value) = usize::try_from(id).ok().and_then(|i| table.get(i)) else {
                    return Err(DruidError::Segment(format!(
                        "druid-native: {what}: row {row} table index {id} out of range \
                         (table has {table_size} entries)"
                    )));
                };
                out.push(value);
            }
            Ok(out)
        }
        other => Err(DruidError::Segment(format!(
            "druid-native: {what}: unknown long encoding format {other:#04x} \
             (supported: 0x00 delta, 0x01 table)"
        ))),
    }
}

/// Decode the packed-value stream of an auto-encoded long part into raw
/// `bits`-wide fields (table indices or delta offsets), reusing the same
/// compressed-block framing as plain longs.
fn decode_packed_long_stream(
    part: &[u8],
    pos: &mut usize,
    head: &NumericPartHead,
    bits: u32,
    compression: u8,
    what: &str,
) -> Result<Vec<u64>> {
    let num_values = head.num_values;

    if compression == COMPRESSION_NONE_RAW {
        // No block container: one contiguous packed run.
        //
        // Input-consistency bound (ORDER IS LOAD-BEARING): the run can
        // physically hold at most `available_bytes × 8 / bits` values, so
        // a stream shorter than `ceil(bits × num_values / 8)` (+ pad) is
        // malformed/truncated and must fail HERE — `num_values` may size
        // the output vector only after both this check and the
        // `MAX_COLUMN_VALUES` head cap have passed.
        let total = packed_run_len(bits, num_values, what)?;
        need(part, *pos, total, what)?;
        let packed = &part[*pos..*pos + total];
        *pos += total;
        let mut out = Vec::with_capacity(num_values);
        push_packed_values(&mut out, packed, num_values, bits);
        return Ok(out);
    }
    if compression != COMPRESSION_LZ4 && compression != COMPRESSION_UNCOMPRESSED {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: unsupported block compression id {compression:#04x} under \
             `longEncoding: auto` (supported: 0x01 lz4, 0xff uncompressed, 0xfe none)"
        )));
    }

    let values_per_block = head.values_per_block;
    if values_per_block == 0 {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: implausible packed block size (0 values per block)"
        )));
    }
    let full_block_len = packed_run_len(bits, values_per_block, what)?;
    if full_block_len > MAX_BLOCK_BYTES {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: implausible packed block size ({values_per_block} values × \
             {bits} bits exceeds {MAX_BLOCK_BYTES} B)"
        )));
    }

    // `values_per_block > 0` was checked above, so this cannot divide by 0.
    // Bound the container parse itself with the exact block count the
    // declared value count implies (same discipline as decode_value_blocks).
    let expected_blocks = num_values.div_ceil(values_per_block);
    let blocks = parse_generic_indexed(part, pos, expected_blocks, what)?;
    if blocks.len() != expected_blocks {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: {} packed blocks cannot cover {num_values} declared values \
             ({values_per_block} per block requires {expected_blocks})",
            blocks.len()
        )));
    }

    // Amplification bound on the PACKED bytes (mirrors decode_value_blocks):
    // the total decompressed packed output must be justified by the block
    // input actually present, at the ratio the codec can actually achieve.
    const MAX_LZ4_RATIO: usize = 256;
    let max_ratio = if compression == COMPRESSION_LZ4 {
        MAX_LZ4_RATIO
    } else {
        1
    };
    let full_blocks = num_values / values_per_block;
    let tail_values = num_values % values_per_block;
    let mut total_packed = full_block_len.saturating_mul(full_blocks);
    if tail_values > 0 {
        total_packed = total_packed.saturating_add(packed_run_len(bits, tail_values, what)?);
    }
    let block_input_bytes: usize = blocks
        .iter()
        .flatten()
        .map(|b| b.len())
        .fold(0usize, |a, n| a.saturating_add(n));
    if total_packed > block_input_bytes.saturating_mul(max_ratio) {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: declared packed output {total_packed} B exceeds {max_ratio}× \
             the {block_input_bytes} B of block input (amplification bound)"
        )));
    }

    // Never pre-size the output from the declared count alone: it grows
    // amortized as each block is proven to decode.
    let mut out = Vec::new();
    let mut remaining = num_values;
    for (i, block) in blocks.iter().enumerate() {
        let Some(block) = block else {
            return Err(DruidError::Segment(format!(
                "druid-native: {what}: packed block {i} is null"
            )));
        };
        if remaining == 0 {
            return Err(DruidError::Segment(format!(
                "druid-native: {what}: more packed blocks than declared values"
            )));
        }
        let block_values = remaining.min(values_per_block);
        let expected = packed_run_len(bits, block_values, what)?;
        match compression {
            COMPRESSION_LZ4 => {
                // Per-block bound before the decode scratch is allocated
                // (same discipline as decode_value_blocks).
                if block.len().saturating_mul(MAX_LZ4_RATIO) < expected {
                    return Err(DruidError::Segment(format!(
                        "druid-native: {what}: packed block {i} has {} bytes and cannot expand \
                         to its declared {expected} B (per-block amplification bound)",
                        block.len()
                    )));
                }
                let decompressed =
                    compression::decompress_raw(compression::Codec::Lz4, block, expected).map_err(
                        |e| {
                            DruidError::Segment(format!(
                                "druid-native: {what}: packed block {i} LZ4 decode failed: {e}"
                            ))
                        },
                    )?;
                push_packed_values(&mut out, &decompressed, block_values, bits);
            }
            _ => {
                // COMPRESSION_UNCOMPRESSED: block bytes are the packed run.
                if block.len() < expected {
                    return Err(DruidError::Segment(format!(
                        "druid-native: {what}: uncompressed packed block {i} has {} bytes, \
                         need {expected}",
                        block.len()
                    )));
                }
                push_packed_values(&mut out, &block[..expected], block_values, bits);
            }
        }
        remaining -= block_values;
    }
    if remaining != 0 {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: packed blocks cover {} of {num_values} declared values",
            num_values - remaining
        )));
    }
    Ok(out)
}

/// Decode the value blocks shared by numeric parts and compressed per-row
/// ordinal sections. Returns exactly `num_values * width` bytes.
#[allow(clippy::too_many_arguments)]
fn decode_value_blocks(
    data: &[u8],
    pos: &mut usize,
    section_end: usize,
    num_values: usize,
    values_per_block: usize,
    width: usize,
    compression: u8,
    what: &str,
) -> Result<Vec<u8>> {
    let total_bytes = num_values.checked_mul(width).ok_or_else(|| {
        DruidError::Segment(format!("druid-native: {what}: value byte count overflows"))
    })?;

    if compression == COMPRESSION_NONE_RAW {
        // No block container: raw contiguous little-endian values.
        need(data, *pos, total_bytes, what)?;
        let raw = data[*pos..*pos + total_bytes].to_vec();
        *pos += total_bytes;
        return Ok(raw);
    }
    if compression != COMPRESSION_LZ4 && compression != COMPRESSION_UNCOMPRESSED {
        // A `longEncoding: auto` flag byte is decoded by read_long_part
        // BEFORE this function is reached, so a flagged byte here sits on a
        // part that can never legitimately carry one (doubleV2 / floatV2 /
        // row ordinals); other values are simply unknown ids. Either way we
        // refuse loudly instead of guessing.
        if has_long_encoding_flag(compression) {
            return Err(DruidError::Segment(format!(
                "druid-native: {what}: header byte {compression:#04x} carries the \
                 `longEncoding: auto` flag, which is only valid on longV2 parts"
            )));
        }
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: unsupported block compression id {compression:#04x} \
             (supported: 0x01 lz4, 0xff uncompressed, 0xfe none)"
        )));
    }

    if values_per_block == 0 || values_per_block.saturating_mul(width) > MAX_BLOCK_BYTES {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: implausible block size ({values_per_block} values × {width} B)"
        )));
    }

    // `values_per_block > 0` was checked above, so this cannot divide by 0.
    // Compute the exact block count the declared value count implies and
    // bound the container parse itself with it, so a hostile block LIST
    // cannot be materialized beyond what the declaration could ever use.
    let expected_blocks = num_values.div_ceil(values_per_block);

    // Parse the (input-bounded) block container BEFORE sizing any buffer:
    // `num_values` is attacker-controlled, so nothing may be allocated from
    // it until the blocks that must back those values are shown to exist.
    let blocks = parse_generic_indexed(data, pos, expected_blocks, what)?;
    if *pos > section_end {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: block container overruns its section"
        )));
    }
    if blocks.len() != expected_blocks {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: {} value blocks cannot cover {num_values} declared values \
             ({values_per_block} per block requires {expected_blocks})",
            blocks.len()
        )));
    }
    // Amplification bound: the total decompressed output must be justified by
    // the compressed INPUT actually present, at the ratio the codec can
    // actually achieve. An uncompressed block IS its own output, so its
    // ratio is exactly 1; only LZ4 may expand, and never beyond ~255× per
    // input byte (LZ4's theoretical maximum), so cap the whole column's
    // output at (sum of block input bytes) × the per-codec ceiling. A
    // legitimately large column (backed by proportionally large input) still
    // reads while a hostile blow-up fails closed. (Pre-fix, the LZ4 ratio
    // was applied to uncompressed sections too, letting 32 short blocks
    // "justify" a 2 GiB reservation from ~8 MiB of input.)
    const MAX_LZ4_RATIO: usize = 256;
    let max_ratio = if compression == COMPRESSION_LZ4 {
        MAX_LZ4_RATIO
    } else {
        1
    };
    let block_input_bytes: usize = blocks
        .iter()
        .flatten()
        .map(|b| b.len())
        .fold(0usize, |a, n| a.saturating_add(n));
    let output_ceiling = block_input_bytes.saturating_mul(max_ratio);
    if total_bytes > output_ceiling {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: declared output {total_bytes} B exceeds {max_ratio}× the \
             {block_input_bytes} B of block input (amplification bound)"
        )));
    }
    // Reserve at most the block input bytes actually present — never
    // pre-size from the declared count alone. Uncompressed output can never
    // exceed that reservation; legitimate LZ4 expansion beyond it grows the
    // Vec amortized via `extend_from_slice` as each block is proven to
    // decode.
    let mut raw = Vec::with_capacity(total_bytes.min(block_input_bytes));
    let mut remaining = num_values;
    for (i, block) in blocks.iter().enumerate() {
        let Some(block) = block else {
            return Err(DruidError::Segment(format!(
                "druid-native: {what}: value block {i} is null"
            )));
        };
        if remaining == 0 {
            return Err(DruidError::Segment(format!(
                "druid-native: {what}: more blocks than declared values"
            )));
        }
        let block_values = remaining.min(values_per_block);
        let expected = block_values * width;
        match compression {
            COMPRESSION_LZ4 => {
                // Per-block bound: THIS block's bytes must be able to
                // justify its share of the output before the decode scratch
                // (up to MAX_BLOCK_BYTES) is allocated for it — the
                // aggregate bound above still admits one long block paired
                // with a few-byte one.
                if block.len().saturating_mul(MAX_LZ4_RATIO) < expected {
                    return Err(DruidError::Segment(format!(
                        "druid-native: {what}: block {i} has {} bytes and cannot expand to \
                         its declared {expected} B (per-block amplification bound)",
                        block.len()
                    )));
                }
                let decompressed =
                    compression::decompress_raw(compression::Codec::Lz4, block, expected).map_err(
                        |e| {
                            DruidError::Segment(format!(
                                "druid-native: {what}: block {i} LZ4 decode failed: {e}"
                            ))
                        },
                    )?;
                raw.extend_from_slice(&decompressed);
            }
            _ => {
                // COMPRESSION_UNCOMPRESSED: block bytes are the values.
                if block.len() < expected {
                    return Err(DruidError::Segment(format!(
                        "druid-native: {what}: uncompressed block {i} has {} bytes, need {expected}",
                        block.len()
                    )));
                }
                raw.extend_from_slice(&block[..expected]);
            }
        }
        remaining -= block_values;
    }
    if remaining != 0 {
        return Err(DruidError::Segment(format!(
            "druid-native: {what}: blocks cover {} of {num_values} declared values",
            num_values - remaining
        )));
    }
    Ok(raw)
}

// ---------------------------------------------------------------------------
// Complex (sketch) part — `thetaSketch` (compat-8 sketch #2) and
// `hyperUnique` (W-A, v1.5.0)
// ---------------------------------------------------------------------------

/// Decode a `complex` part.  Two complex types are decodable:
///
/// * `typeName: "thetaSketch"` — a generic-indexed container of per-row
///   Apache DataSketches compact Theta images, each decoded into a
///   union-only Druid-origin [`ferrodruid_sketches::ThetaSketch`];
/// * `typeName: "hyperUnique"` — a generic-indexed container of per-row
///   Druid HyperLogLog register blobs, each decoded into a merge-only
///   [`ferrodruid_sketches::DruidHyperUnique`] (W-A; the per-row envelope
///   is the same 4-leading-bytes-plus-blob shape observed for theta).
///
/// A null or zero-length row blob decodes as an EMPTY sketch (cardinality
/// 0) in both.  Every other complex type fails loudly — their serialized
/// forms are not decodable, so under the migrate
/// `--allow-unreadable-columns` gate they are dropped with a manifest
/// instead.
fn decode_complex_part(data: &[u8], pos: &mut usize, part: &NativePart) -> Result<ColumnData> {
    let type_name = part.type_name.as_deref().unwrap_or("<unspecified>");
    if type_name == "hyperUnique" {
        return decode_hyper_unique_complex(data, pos);
    }
    if type_name != "thetaSketch" {
        return Err(DruidError::Segment(format!(
            "druid-native: unsupported complex column typeName `{type_name}` (only \
             `thetaSketch` and `hyperUnique` decode; HLLSketch/quantiles sketches \
             remain unreadable)"
        )));
    }
    // One blob per row, so the module-wide row cap bounds the container
    // parse before anything is materialized from the declared count.
    let blobs = parse_generic_indexed(data, pos, MAX_COLUMN_VALUES, "thetaSketch rows")?;
    let mut rows = Vec::with_capacity(blobs.len());
    for (i, blob) in blobs.into_iter().enumerate() {
        let sketch = match blob {
            None | Some([]) => ferrodruid_sketches::ThetaSketch::empty_druid_origin(),
            Some(bytes) => {
                // Bound the blob BEFORE materializing: an in-cap image can
                // still only produce `len / 8` retained hashes (the sketch
                // decoder enforces the exact-length backing), so the byte
                // cap bounds the per-row hash-set allocation too.
                if bytes.len() > MAX_THETA_BLOB_BYTES {
                    return Err(DruidError::Segment(format!(
                        "druid-native: thetaSketch row {i} blob of {} bytes exceeds cap \
                         {MAX_THETA_BLOB_BYTES}",
                        bytes.len()
                    )));
                }
                ferrodruid_sketches::ThetaSketch::from_druid_compact(bytes).map_err(|e| {
                    DruidError::Segment(format!(
                        "druid-native: thetaSketch row {i} failed to decode: {e}"
                    ))
                })?
            }
        };
        rows.push(sketch);
    }
    Ok(ColumnData::ComplexTheta(rows))
}

/// Decode a `hyperUnique` complex part (W-A, v1.5.0): a generic-indexed
/// container of per-row Druid HyperLogLog blobs, each decoded into a
/// merge-only [`ferrodruid_sketches::DruidHyperUnique`].  The blob decoder
/// is strictly fixture-pinned and fails loudly on every unobserved shape
/// (see the `druid_hll` module docs in `ferrodruid-sketches`); no byte cap
/// is needed before decoding because the register state is a FIXED 2048
/// nibbles — the decoder never allocates proportionally to the input.
fn decode_hyper_unique_complex(data: &[u8], pos: &mut usize) -> Result<ColumnData> {
    // One blob per row, so the module-wide row cap bounds the container
    // parse before anything is materialized from the declared count.
    let blobs = parse_generic_indexed(data, pos, MAX_COLUMN_VALUES, "hyperUnique rows")?;
    let mut rows = Vec::with_capacity(blobs.len());
    for (i, blob) in blobs.into_iter().enumerate() {
        let sketch = match blob {
            None | Some([]) => ferrodruid_sketches::DruidHyperUnique::empty(),
            Some(bytes) => {
                ferrodruid_sketches::DruidHyperUnique::from_druid_blob(bytes).map_err(|e| {
                    DruidError::Segment(format!(
                        "druid-native: hyperUnique row {i} failed to decode: {e}"
                    ))
                })?
            }
        };
        rows.push(sketch);
    }
    Ok(ColumnData::ComplexHyperUnique(rows))
}

// ---------------------------------------------------------------------------
// String dictionary part
// ---------------------------------------------------------------------------

/// Decode a `stringDictionary` part into a [`StringColumnData`].
fn decode_string_dictionary_part(data: &[u8], pos: &mut usize) -> Result<ColumnData> {
    let version = read_u8(data, pos, "stringDictionary version")?;
    match version {
        2 => {
            let flags = read_u32_be(data, pos, "stringDictionary flags")?;
            if flags != 0 {
                return Err(DruidError::Segment(format!(
                    "druid-native: stringDictionary feature flags {flags:#x} not supported \
                     (multi-value columns are not readable yet)"
                )));
            }
        }
        0 => {}
        other => {
            return Err(DruidError::Segment(format!(
                "druid-native: unsupported stringDictionary version {other}"
            )));
        }
    }

    // 1. Sorted dictionary. The observed null marker (element `-1`) only
    //    ever appears as the first, null-sorts-first entry. One value bitmap
    //    exists per entry, so the cardinality shares the bitmap-count design
    //    cap (MAX_GI_ELEMENTS mirrors `MAX_STRING_BITMAPS`).
    let dict_entries = parse_string_list(data, pos, MAX_GI_ELEMENTS, "string dictionary")?;
    let mut has_null_entry = false;
    let mut dict_values: Vec<String> = Vec::with_capacity(dict_entries.len());
    for (i, e) in dict_entries.into_iter().enumerate() {
        match e {
            None if i == 0 => {
                // SQL NULL entry: FerroDruid represents NULL rows with an
                // in-range `""` ordinal plus a trailing null-row bitmap
                // (see `StringColumnData`); map the null entry to `""`.
                has_null_entry = true;
                dict_values.push(String::new());
            }
            None => {
                return Err(DruidError::Segment(format!(
                    "druid-native: string dictionary has a null entry at index {i} \
                     (only a leading null entry is supported)"
                )));
            }
            Some(s) => {
                if has_null_entry && i == 1 && s.is_empty() {
                    return Err(DruidError::Segment(
                        "druid-native: string dictionary contains both a null entry and an \
                         empty-string entry; this layout is not supported yet"
                            .to_string(),
                    ));
                }
                dict_values.push(s);
            }
        }
    }
    let dict_len = dict_values.len();

    // Real Druid writes the dictionary as a GenericIndexed sorted in Java
    // UTF-16 code-unit order, and its selectors binary-search that order; so
    // validate THAT order, not Rust codepoint order.  The two coincide for
    // all-BMP text but diverge for supplementary characters, where a Rust-order
    // check would spuriously reject a legitimately UTF-16-sorted Druid segment
    // (and would accept a non-canonical one).  An unsorted dictionary (hostile
    // or corrupt segment) would otherwise silently mis-filter.
    //
    // NOTE (follow-on, out of scope here): the decoded `FrontCodedDictionary`
    // (`ferrodruid-dict`) still binary-searches value→ordinal lookups in Rust
    // codepoint order, so a value-equality FILTER on a supplementary-character
    // dictionary value can mis-resolve at query time.  Positional access
    // (groupBy/scan/ordinal reads) is correct.  All-BMP data (every existing
    // segment/fixture) is unaffected since the two orders coincide there.
    if let Some(w) = dict_values.windows(2).find(|w| {
        crate::druid_native_writer::utf16_cmp(w[0].as_str(), w[1].as_str())
            == core::cmp::Ordering::Greater
    }) {
        return Err(DruidError::Segment(format!(
            "druid-native: string dictionary is not sorted in UTF-16 order ({:?} > {:?})",
            w[0], w[1]
        )));
    }

    // 2. Per-row ordinals.
    let encoded_values = decode_row_ordinals(data, pos)?;
    for (row, &ord) in encoded_values.iter().enumerate() {
        if ord as usize >= dict_len {
            return Err(DruidError::Segment(format!(
                "druid-native: row {row} ordinal {ord} out of dictionary range {dict_len}"
            )));
        }
    }

    // 3. Value bitmaps (portable Roaring, no per-bitmap type tag). Exactly
    //    one bitmap exists per dictionary entry — bound the container parse
    //    with that count so a hostile bitmap LIST cannot be materialized
    //    beyond it either.
    let bitmap_blobs = parse_generic_indexed(data, pos, dict_len, "string value bitmaps")?;
    if bitmap_blobs.len() != dict_len {
        return Err(DruidError::Segment(format!(
            "druid-native: {} value bitmaps for {dict_len} dictionary entries",
            bitmap_blobs.len()
        )));
    }
    let num_rows = encoded_values.len();
    let mut bitmap_indexes = Vec::with_capacity(dict_len + usize::from(has_null_entry));
    let mut total_bitmap_rows: u64 = 0;
    for (i, blob) in bitmap_blobs.into_iter().enumerate() {
        let bitmap = match blob {
            None | Some([]) => DruidBitmap::new(),
            // Bounded against the column's row count BEFORE deserialization:
            // a malformed sub-MiB payload declaring ~65 536 run containers
            // would otherwise expand to ~512 MiB of container stores inside
            // the decoder, long before the row-range check below could run.
            Some(bytes) => {
                DruidBitmap::deserialize_portable(bytes, num_rows as u64).map_err(|e| {
                    DruidError::Segment(format!(
                        "druid-native: value bitmap {i} failed to decode: {e}"
                    ))
                })?
            }
        };
        // A set bit at or beyond the row count would let bitmap-backed
        // filters hand out-of-range row ids to downstream readers.
        if let Some(max_row) = bitmap.as_inner().max()
            && max_row as usize >= num_rows
        {
            return Err(DruidError::Segment(format!(
                "druid-native: value bitmap {i} references row {max_row} but the column \
                 has only {num_rows} rows"
            )));
        }
        // Single-value columns partition their rows across the value
        // bitmaps, so the total cardinality can never exceed the row count.
        // Without this, run-encoded bitmaps that EACH stay within the
        // per-bitmap bounds (~6 B of input per all-rows container) could
        // accumulate dict_len × num_rows/8 bytes of container stores.
        total_bitmap_rows = total_bitmap_rows.saturating_add(bitmap.len());
        if total_bitmap_rows > num_rows as u64 {
            return Err(DruidError::Segment(format!(
                "druid-native: value bitmaps 0..={i} claim {total_bitmap_rows} total rows, \
                 exceeding the column's {num_rows} rows (bitmaps must partition the rows)"
            )));
        }
        bitmap_indexes.push(bitmap);
    }

    if has_null_entry {
        // The upstream bitmap for the null entry is the set of NULL rows.
        // FerroDruid's layout wants: the `""` entry's own bitmap EXCLUDES
        // null rows (empty here — the entry only exists for null rows), and
        // a trailing null-row bitmap.
        let null_rows = std::mem::replace(&mut bitmap_indexes[0], DruidBitmap::new());
        bitmap_indexes.push(null_rows);
    }

    Ok(ColumnData::String(StringColumnData {
        dictionary: FrontCodedDictionary::from_sorted(dict_values),
        encoded_values,
        bitmap_indexes,
    }))
}

/// Decode the per-row ordinal section of a string column.
fn decode_row_ordinals(data: &[u8], pos: &mut usize) -> Result<Vec<u32>> {
    let version = read_u8(data, pos, "row ordinals version")?;
    match version {
        2 => {
            // Compressed: little-endian values inside LZ4 (or uncompressed)
            // blocks.
            let num_bytes = read_u8(data, pos, "row ordinals width")? as usize;
            if !(1..=4).contains(&num_bytes) {
                return Err(DruidError::Segment(format!(
                    "druid-native: row ordinal width {num_bytes} out of range 1..=4"
                )));
            }
            let num_rows = read_u32_be(data, pos, "row count")? as usize;
            if num_rows > MAX_COLUMN_VALUES {
                return Err(DruidError::Segment(format!(
                    "druid-native: row count {num_rows} exceeds cap {MAX_COLUMN_VALUES}"
                )));
            }
            let rows_per_block = read_u32_be(data, pos, "rows per block")? as usize;
            let compression = read_u8(data, pos, "row ordinals compression")?;
            let raw = decode_value_blocks(
                data,
                pos,
                data.len(),
                num_rows,
                rows_per_block,
                num_bytes,
                compression,
                "row ordinals",
            )?;
            let mut values = Vec::with_capacity(num_rows);
            for chunk in raw.chunks_exact(num_bytes) {
                let mut v: u32 = 0;
                // Little-endian within `num_bytes` (verified against a
                // 400-entry dictionary: ids 0,1,2,… appear as 00 00, 01 00,
                // 02 00, … in the decompressed block).
                for (i, &b) in chunk.iter().enumerate() {
                    v |= u32::from(b) << (8 * i);
                }
                values.push(v);
            }
            Ok(values)
        }
        0 => {
            // Uncompressed: big-endian values padded with `4 - num_bytes`
            // trailing bytes (included in the declared byte length).
            let num_bytes = read_u8(data, pos, "row ordinals width")? as usize;
            if !(1..=4).contains(&num_bytes) {
                return Err(DruidError::Segment(format!(
                    "druid-native: row ordinal width {num_bytes} out of range 1..=4"
                )));
            }
            let total_bytes = read_u32_be(data, pos, "row ordinal bytes")? as usize;
            need(data, *pos, total_bytes, "row ordinal values")?;
            let pad = 4 - num_bytes;
            if total_bytes < pad || !(total_bytes - pad).is_multiple_of(num_bytes) {
                return Err(DruidError::Segment(format!(
                    "druid-native: row ordinal byte length {total_bytes} does not fit \
                     width {num_bytes} plus {pad} padding"
                )));
            }
            let num_rows = (total_bytes - pad) / num_bytes;
            if num_rows > MAX_COLUMN_VALUES {
                return Err(DruidError::Segment(format!(
                    "druid-native: row count {num_rows} exceeds cap {MAX_COLUMN_VALUES}"
                )));
            }
            let raw = &data[*pos..*pos + total_bytes - pad];
            *pos += total_bytes;
            let mut values = Vec::with_capacity(num_rows);
            for chunk in raw.chunks_exact(num_bytes) {
                let mut v: u32 = 0;
                for &b in chunk {
                    v = (v << 8) | u32::from(b);
                }
                values.push(v);
            }
            Ok(values)
        }
        other => Err(DruidError::Segment(format!(
            "druid-native: unsupported row ordinal section version {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Tests — every fixture is a verbatim byte capture from a real Druid-31.0.2
// segment (see module docs for provenance).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const INDEX_DRD: &[u8] = include_bytes!("../testdata/druid31_index.drd");
    const TIME_COL: &[u8] = include_bytes!("../testdata/druid31_time.col");
    const ADDED_COL: &[u8] = include_bytes!("../testdata/druid31_added.col");
    const PAGE_COL: &[u8] = include_bytes!("../testdata/druid31_page.col");
    const LANGUAGE_COL: &[u8] = include_bytes!("../testdata/druid31_language.col");
    const ADDED_D_COL: &[u8] = include_bytes!("../testdata/druid31_added_d.col");
    const DELTA_F_COL: &[u8] = include_bytes!("../testdata/druid31_delta_f.col");
    const UNCMP_TIME_COL: &[u8] = include_bytes!("../testdata/druid31_uncmp_time.col");
    const MUNCMP_ADDED_COL: &[u8] = include_bytes!("../testdata/druid31_muncmp_added.col");
    const UNCMP_PAGE_COL: &[u8] = include_bytes!("../testdata/druid31_uncmp_page.col");
    const HICARD_PAGE_COL: &[u8] = include_bytes!("../testdata/druid31_hicard_page.col");
    const HICARD_U_PAGE_COL: &[u8] = include_bytes!("../testdata/druid31_hicard_u_page.col");

    #[test]
    fn parses_real_index_drd() {
        let parsed = parse_native_index_drd(INDEX_DRD, 16_384, 16_384).expect("parse");
        assert_eq!(
            parsed.dimensions,
            vec!["page", "user", "language", "city", "namespace", "channel"]
        );
        assert_eq!(parsed.metrics, vec!["added", "count", "deleted", "delta"]);
        assert_eq!(parsed.interval.start_millis, 1_704_067_200_000);
        assert_eq!(parsed.interval.end_millis, 1_704_153_600_000);
    }

    #[test]
    fn decodes_real_time_column_lz4() {
        assert!(is_native_column(TIME_COL));
        match decode_native_column(TIME_COL).expect("decode __time") {
            ColumnData::Long(v) => assert_eq!(
                v,
                vec![
                    1_704_067_200_000,
                    1_704_070_800_000,
                    1_704_074_400_000,
                    1_704_078_000_000,
                    1_704_110_400_000
                ]
            ),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    #[test]
    fn decodes_real_long_metric_lz4() {
        match decode_native_column(ADDED_COL).expect("decode added") {
            ColumnData::Long(v) => assert_eq!(v, vec![100, 50, 200, 150, 75]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    #[test]
    fn decodes_real_double_and_float_metrics() {
        match decode_native_column(ADDED_D_COL).expect("decode added_d") {
            ColumnData::Double(v) => assert_eq!(v, vec![100.0, 50.0, 200.0, 150.0, 75.0]),
            other => panic!("expected Double, got {other:?}"),
        }
        match decode_native_column(DELTA_F_COL).expect("decode delta_f") {
            ColumnData::Float(v) => assert_eq!(v, vec![90.0, 45.0, 180.0, 120.0, 50.0]),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn decodes_real_string_column_lz4() {
        match decode_native_column(PAGE_COL).expect("decode page") {
            ColumnData::String(s) => {
                assert_eq!(s.dictionary.len(), 4);
                assert_eq!(s.dictionary.get(0), Some("Accueil"));
                assert_eq!(s.dictionary.get(1), Some("Hauptseite"));
                assert_eq!(s.dictionary.get(2), Some("Main_Page"));
                assert_eq!(s.dictionary.get(3), Some("Talk:Main_Page"));
                assert_eq!(s.encoded_values, vec![2, 3, 0, 1, 2]);
                assert_eq!(s.bitmap_indexes.len(), 4);
                // Main_Page appears in rows 0 and 4.
                assert!(s.bitmap_indexes[2].contains(0));
                assert!(s.bitmap_indexes[2].contains(4));
                assert_eq!(s.bitmap_indexes[2].len(), 2);
                assert!(s.null_rows().is_none());
            }
            other => panic!("expected String, got {other:?}"),
        }
        match decode_native_column(LANGUAGE_COL).expect("decode language") {
            ColumnData::String(s) => {
                assert_eq!(s.dictionary.get(0), Some("de"));
                assert_eq!(s.dictionary.get(1), Some("en"));
                assert_eq!(s.dictionary.get(2), Some("fr"));
                assert_eq!(s.encoded_values, vec![1, 1, 2, 0, 1]);
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn decodes_real_uncompressed_block_metric() {
        // metricCompression: "uncompressed" → 0xff blocks (raw bytes inside
        // the block container).
        match decode_native_column(MUNCMP_ADDED_COL).expect("decode uncompressed-block added") {
            ColumnData::Long(v) => assert_eq!(v, vec![100, 50, 200, 150, 75]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    #[test]
    fn decodes_real_uncompressed_variants() {
        // metricCompression: "none" → raw contiguous longs (0xfe).
        match decode_native_column(UNCMP_TIME_COL).expect("decode uncompressed __time") {
            ColumnData::Long(v) => {
                assert_eq!(v.len(), 5);
                assert_eq!(v[0], 1_704_067_200_000);
            }
            other => panic!("expected Long, got {other:?}"),
        }
        // dimensionCompression: "uncompressed" → version-0 ordinal section.
        match decode_native_column(UNCMP_PAGE_COL).expect("decode uncompressed page") {
            ColumnData::String(s) => {
                assert_eq!(s.encoded_values, vec![2, 3, 0, 1, 2]);
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn decodes_two_byte_row_ordinals_in_both_layouts() {
        // 400-entry dictionary → 2-byte ordinals. Compressed layout stores
        // them little-endian…
        match decode_native_column(HICARD_PAGE_COL).expect("decode hicard page") {
            ColumnData::String(s) => {
                assert_eq!(s.dictionary.len(), 400);
                assert_eq!(s.encoded_values.len(), 400);
                assert_eq!(s.encoded_values[0], 0);
                assert_eq!(s.encoded_values[399], 399);
                assert_eq!(s.dictionary.get(399), Some("page_399"));
            }
            other => panic!("expected String, got {other:?}"),
        }
        // …while the uncompressed layout stores them big-endian.
        match decode_native_column(HICARD_U_PAGE_COL).expect("decode hicard_u page") {
            ColumnData::String(s) => {
                assert_eq!(s.encoded_values.len(), 400);
                assert_eq!(s.encoded_values[0], 0);
                assert_eq!(s.encoded_values[399], 399);
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn rejects_encoding_flag_on_non_long_parts() {
        // A doubleV2 part whose compression byte carries the longEncoding
        // flag (0x83): only longV2 parts may be auto-encoded, so this must
        // fail closed rather than guess at a layout.
        let desc = br#"{"valueType":"DOUBLE","parts":[{"type":"doubleV2"}]}"#;
        let mut blob = Vec::new();
        blob.extend_from_slice(&(desc.len() as u32).to_be_bytes());
        blob.extend_from_slice(desc);
        let part = [2u8, 0, 0, 0, 5, 0, 0, 0x20, 0, 0x83];
        blob.extend_from_slice(&(part.len() as u32).to_be_bytes());
        blob.extend_from_slice(&part);
        let err = decode_native_column(&blob).expect_err("flagged doubleV2 must be rejected");
        assert!(
            err.to_string().contains("only valid on longV2"),
            "expected non-long encoding-flag rejection, got: {err}"
        );
    }

    #[test]
    fn oversized_generic_indexed_count_is_rejected() {
        // version 1, flags 0, huge body claiming u32::MAX elements.
        let mut buf = vec![1u8, 0];
        buf.extend_from_slice(&12u32.to_be_bytes());
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        buf.extend_from_slice(&[0u8; 8]);
        let mut pos = 0;
        let err = parse_generic_indexed(&buf, &mut pos, MAX_GI_ELEMENTS, "test")
            .expect_err("element-count cap must trip");
        assert!(err.to_string().contains("exceeds cap"), "got: {err}");
    }

    #[test]
    fn is_native_column_rejects_private_blobs() {
        // A FerroDruid private long column: [u32 count][BE values] — no JSON.
        let private = crate::column::encode_long_column(&[1, 2, 3]);
        assert!(!is_native_column(&private));
        assert!(!is_native_column(b""));
        assert!(is_native_column(TIME_COL));
    }

    // -- hostile-input hardening (crafted malformed segments) ----------------

    /// Build a well-formed generic-indexed container (version 1) from raw
    /// element payloads (`None` = the null marker).
    fn gi(elements: &[Option<&[u8]>]) -> Vec<u8> {
        let mut values = Vec::new();
        let mut offsets = Vec::new();
        for e in elements {
            match e {
                Some(payload) => {
                    values.extend_from_slice(&0i32.to_be_bytes());
                    values.extend_from_slice(payload);
                }
                None => values.extend_from_slice(&(-1i32).to_be_bytes()),
            }
            offsets.push(values.len() as u32);
        }
        let body_size = 4 + 4 * elements.len() + values.len();
        let mut out = vec![1u8, 0];
        out.extend_from_slice(&(body_size as u32).to_be_bytes());
        out.extend_from_slice(&(elements.len() as u32).to_be_bytes());
        for o in &offsets {
            out.extend_from_slice(&o.to_be_bytes());
        }
        out.extend_from_slice(&values);
        out
    }

    /// Prefix a descriptor JSON onto a part payload, forming a column blob.
    fn column_blob(descriptor: &[u8], part: &[u8]) -> Vec<u8> {
        let mut blob = Vec::new();
        blob.extend_from_slice(&(descriptor.len() as u32).to_be_bytes());
        blob.extend_from_slice(descriptor);
        blob.extend_from_slice(part);
        blob
    }

    #[test]
    fn generic_indexed_count_outside_body_fails_closed() {
        // version 1, flags 0, body_size = 0: the 4-byte element count that
        // follows lies OUTSIDE the declared body and must not be read (a
        // lying body_size would otherwise push the cursor past body_end and
        // underflow the value-region length).
        let buf = [1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut pos = 0;
        let err = parse_generic_indexed(&buf, &mut pos, MAX_GI_ELEMENTS, "test")
            .expect_err("count outside the declared body must fail closed");
        assert!(err.to_string().contains("truncated"), "got: {err}");
    }

    #[test]
    fn numeric_part_header_outside_declared_part_fails_closed() {
        let desc = br#"{"valueType":"LONG","parts":[{"type":"longV2"}]}"#;
        // part_len = 0, then trailing bytes that would parse as a plausible
        // header (version 2, zero values, a block size, compression 0xfe) —
        // all of it lies OUTSIDE the declared part and must not be read.
        let mut part = Vec::new();
        part.extend_from_slice(&0u32.to_be_bytes());
        part.extend_from_slice(&[2u8, 0, 0, 0, 0, 0, 0, 0x20, 0, 0xfe]);
        let err = decode_native_column(&column_blob(desc, &part))
            .expect_err("part header outside the declared part must fail closed");
        assert!(err.to_string().contains("truncated"), "got: {err}");
    }

    #[test]
    fn hostile_value_count_rejected_before_allocation() {
        let desc = br#"{"valueType":"LONG","parts":[{"type":"longV2"}]}"#;
        // 8 388 608 declared longs (a 64 MiB value buffer — deliberately
        // UNDER the `MAX_COLUMN_VALUES` head cap, which now rejects larger
        // counts first) backed by an EMPTY block container: the count must
        // be rejected against the blocks that actually exist BEFORE any
        // buffer is sized from it.
        let mut body = vec![2u8];
        body.extend_from_slice(&8_388_608u32.to_be_bytes());
        body.extend_from_slice(&0x2000u32.to_be_bytes());
        body.push(COMPRESSION_LZ4);
        body.extend_from_slice(&gi(&[]));
        let mut part = Vec::new();
        part.extend_from_slice(&(body.len() as u32).to_be_bytes());
        part.extend_from_slice(&body);
        let err = decode_native_column(&column_blob(desc, &part))
            .expect_err("value count with no backing blocks must fail closed");
        assert!(
            err.to_string().contains("cannot cover"),
            "rejection must come from the pre-allocation block-count check, got: {err}"
        );
    }

    #[test]
    fn hostile_index_drd_column_count_rejected_before_materialization() {
        // 100 000 declared column names (all empty) — far beyond the
        // 16 384 + 16 384 column cap but well under the 16M generic-indexed
        // ceiling. The cap must reject the COUNT before the offset/element
        // vectors and strings are materialized: pre-fix, this list was fully
        // decoded (~100k Options + Strings; at the 16M ceiling ~700-800 MiB
        // of temporaries from ~128 MiB of input) and only then rejected by
        // the late column-cap check.
        let hostile: Vec<Option<&[u8]>> = vec![Some(&b""[..]); 100_000];
        let data = gi(&hostile);
        let Err(err) = parse_native_index_drd(&data, 16_384, 16_384) else {
            panic!("hostile column count must fail closed");
        };
        assert!(
            err.to_string().contains("element count 100000"),
            "rejection must come from the early generic-indexed count bound, got: {err}"
        );

        // Same for the dimension list (bounded by max_dimensions alone).
        let mut data = gi(&[Some(&b"col"[..])]);
        data.extend_from_slice(&gi(&hostile));
        let Err(err) = parse_native_index_drd(&data, 16_384, 16_384) else {
            panic!("hostile dimension count must fail closed");
        };
        assert!(
            err.to_string().contains("element count 100000"),
            "rejection must come from the early generic-indexed count bound, got: {err}"
        );
    }

    #[test]
    fn uncompressed_blocks_cannot_claim_lz4_amplification() {
        let desc = br#"{"valueType":"LONG","parts":[{"type":"longV2"}]}"#;
        // 8M declared longs (a 64 MiB output) "backed" by 8 uncompressed
        // blocks of 32 KiB each (256 KiB of block input). 64 MiB is exactly
        // 256 × 256 KiB, so the old shared LZ4 amplification bound accepted
        // the declaration and reserved the full output before discovering
        // every block is too short (scaled up: 256M longs / 32 blocks
        // reserved 2 GiB from ~8 MiB of input). An uncompressed block cannot
        // produce more bytes than it holds: its ratio must be 1.
        let num_values = 8 * 1024 * 1024u32;
        let values_per_block = 1024 * 1024u32;
        let block = vec![0u8; 32 * 1024];
        let blocks: Vec<Option<&[u8]>> = (0..8).map(|_| Some(&block[..])).collect();
        let mut body = vec![2u8];
        body.extend_from_slice(&num_values.to_be_bytes());
        body.extend_from_slice(&values_per_block.to_be_bytes());
        body.push(COMPRESSION_UNCOMPRESSED);
        body.extend_from_slice(&gi(&blocks));
        let mut part = Vec::new();
        part.extend_from_slice(&(body.len() as u32).to_be_bytes());
        part.extend_from_slice(&body);
        let err = decode_native_column(&column_blob(desc, &part))
            .expect_err("uncompressed blocks shorter than the declared output must fail closed");
        assert!(
            err.to_string().contains("exceeds 1×"),
            "rejection must come from the per-codec (ratio 1) amplification bound \
             before any output buffer is reserved, got: {err}"
        );
    }

    #[test]
    fn lz4_block_shorter_than_declared_expansion_rejected_per_block() {
        let desc = br#"{"valueType":"LONG","parts":[{"type":"longV2"}]}"#;
        // Block 0 is REAL LZ4 covering 8 192 longs (64 KiB) of incompressible
        // bytes; block 1 is 4 garbage bytes that also claim to decode to
        // 64 KiB. The aggregate 256× bound passes (block input ≈ 64 KiB), so
        // without a per-block bound the parser would zero-fill the 64 KiB
        // scratch and run the LZ4 decoder on the garbage; per block,
        // 4 B × 256 < 64 KiB is structurally impossible and must be rejected
        // before the scratch buffer for block 1 is allocated.
        let mut noise = Vec::with_capacity(64 * 1024);
        let mut v: u32 = 0x2545_F491;
        for _ in 0..16 * 1024 {
            v = v.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            noise.extend_from_slice(&v.to_le_bytes());
        }
        let compressed =
            compression::compress(compression::Codec::Lz4, &noise).expect("compress noise");
        // Strip the 4-byte lz4_flex size prefix → raw LZ4 block, the framing
        // used inside upstream block containers.
        let block0 = &compressed[4..];
        let garbage = [0xAAu8; 4];
        let mut body = vec![2u8];
        body.extend_from_slice(&16_384u32.to_be_bytes()); // num_values
        body.extend_from_slice(&8_192u32.to_be_bytes()); // values_per_block
        body.push(COMPRESSION_LZ4);
        body.extend_from_slice(&gi(&[Some(block0), Some(&garbage)]));
        let mut part = Vec::new();
        part.extend_from_slice(&(body.len() as u32).to_be_bytes());
        part.extend_from_slice(&body);
        let err = decode_native_column(&column_blob(desc, &part))
            .expect_err("a 4-byte block claiming 64 KiB of output must fail closed");
        assert!(
            err.to_string().contains("cannot expand"),
            "rejection must come from the per-block amplification bound, got: {err}"
        );
    }

    #[test]
    fn hostile_bitmap_container_count_rejected_before_expansion() {
        let desc = br#"{"valueType":"STRING","parts":[{"type":"stringDictionary"}]}"#;
        let mut part = vec![0u8]; // stringDictionary version 0 (no flags)
        part.extend_from_slice(&gi(&[Some(b"only")])); // 1-entry dictionary
        // Uncompressed (version 0) row ordinals: 1 row, 1-byte width,
        // 3 padding bytes.
        part.extend_from_slice(&[0u8, 1]);
        part.extend_from_slice(&4u32.to_be_bytes());
        part.extend_from_slice(&[0u8, 0, 0, 0]);
        // Hostile portable-Roaring header: run-format cookie declaring
        // 65 536 containers (the format maximum). Fully backed, such a
        // payload expands to ~512 MiB of container stores from under 1 MiB
        // of input DURING deserialization — before any row-range check can
        // run. On a 1-row column at most ceil(1/65536) = 1 container can
        // ever be legitimate, so the count must be rejected from the 4-byte
        // header alone, without materializing a single container.
        let cookie: u32 = (0xFFFF << 16) | 12_347;
        part.extend_from_slice(&gi(&[Some(&cookie.to_le_bytes())]));
        let err = decode_native_column(&column_blob(desc, &part))
            .expect_err("hostile container count must fail closed");
        assert!(
            err.to_string().contains("containers"),
            "rejection must come from the pre-deserialization container bound, got: {err}"
        );
    }

    #[test]
    fn bitmap_total_cardinality_beyond_column_rows_is_rejected() {
        // Two dictionary entries whose bitmaps EACH claim every row of a
        // 4-row column. Each bitmap alone passes the per-bitmap bounds
        // (1 container, max row 3 < 4), but a single-value column's bitmaps
        // must partition its rows — accepting overlapping full bitmaps lets
        // dict_len × (num_rows/8) bytes of stores accumulate from ~6 B of
        // run-encoded input per bitmap (~1365× amplification per container).
        let desc = br#"{"valueType":"STRING","parts":[{"type":"stringDictionary"}]}"#;
        let mut part = vec![0u8]; // stringDictionary version 0 (no flags)
        part.extend_from_slice(&gi(&[Some(b"a"), Some(b"b")]));
        // Uncompressed (version 0) row ordinals: 4 rows, 1-byte width,
        // 3 padding bytes; ordinals a, a, b, b.
        part.extend_from_slice(&[0u8, 1]);
        part.extend_from_slice(&7u32.to_be_bytes());
        part.extend_from_slice(&[0u8, 0, 1, 1, 0, 0, 0]);
        let full = DruidBitmap::from_sorted_iter(0..4u32);
        let mut portable = Vec::new();
        full.as_inner()
            .serialize_into(&mut portable)
            .expect("serialize test bitmap");
        part.extend_from_slice(&gi(&[Some(&portable), Some(&portable)]));
        let err = decode_native_column(&column_blob(desc, &part))
            .expect_err("overlapping full bitmaps must fail closed");
        assert!(
            err.to_string().contains("total rows"),
            "rejection must come from the aggregate-cardinality bound, got: {err}"
        );
    }

    /// Undecoded bytes after the declared part must fail loud: upstream
    /// nullable-column layouts can append null metadata past the value
    /// section, and silently ignoring such bytes would silently drop nulls
    /// (and let `crate::null_generation` misclassify a genuinely modern
    /// column as legacy-consistent). Never guess.
    #[test]
    fn trailing_bytes_after_part_fail_loud() {
        // Valid raw-uncompressed (0xfe) longV2 column with values [7, 0].
        let desc = br#"{"valueType":"LONG","parts":[{"type":"longV2"}]}"#;
        let mut body = vec![2u8];
        body.extend_from_slice(&2u32.to_be_bytes()); // num_values
        body.extend_from_slice(&0x2000u32.to_be_bytes()); // values_per_block
        body.push(COMPRESSION_NONE_RAW);
        body.extend_from_slice(&7i64.to_le_bytes());
        body.extend_from_slice(&0i64.to_le_bytes());
        let mut part = Vec::new();
        part.extend_from_slice(&(body.len() as u32).to_be_bytes());
        part.extend_from_slice(&body);
        let blob = column_blob(desc, &part);
        match decode_native_column(&blob).expect("well-formed column decodes") {
            ColumnData::Long(v) => assert_eq!(v, vec![7, 0]),
            other => panic!("expected Long, got {other:?}"),
        }

        let mut with_trailing = blob;
        with_trailing.extend_from_slice(&[0xAB; 8]);
        let err = decode_native_column(&with_trailing)
            .expect_err("trailing bytes after the part must fail loud");
        assert!(
            err.to_string().contains("trailing"),
            "expected trailing-bytes rejection, got: {err}"
        );

        // Same for string columns: bytes past the value bitmaps are refused.
        let sdesc = br#"{"valueType":"STRING","parts":[{"type":"stringDictionary"}]}"#;
        let spart = string_column_part(&[Some(b"a"), Some(b"b")]);
        let mut sblob = column_blob(sdesc, &spart);
        decode_native_column(&sblob).expect("well-formed string column decodes");
        sblob.extend_from_slice(&[0xCD; 4]);
        let err = decode_native_column(&sblob)
            .expect_err("trailing bytes after the string part must fail loud");
        assert!(
            err.to_string().contains("trailing"),
            "expected trailing-bytes rejection, got: {err}"
        );
    }

    // -- nullable numeric columns (trailing null-value bitmap) ---------------
    //
    // First-approximation byte layouts (framing per the module docs); the
    // real-Druid authority is the `druid_writes_ferrodruid_reads_nullable_
    // numeric` harness test in tests/segment-compat.

    /// Descriptor text mirroring the observed druid31_added.col descriptor
    /// byte-for-byte in shape (valueType swapped per test).
    const NULLABLE_LONG_DESC: &[u8] = br#"{"valueType":"LONG","hasMultipleValues":false,"parts":[{"type":"longV2","byteOrder":"LITTLE_ENDIAN","bitmapSerdeFactory":{"type":"roaring"}}]}"#;
    const NULLABLE_DOUBLE_DESC: &[u8] = br#"{"valueType":"DOUBLE","hasMultipleValues":false,"parts":[{"type":"doubleV2","byteOrder":"LITTLE_ENDIAN","bitmapSerdeFactory":{"type":"roaring"}}]}"#;
    const NULLABLE_FLOAT_DESC: &[u8] = br#"{"valueType":"FLOAT","hasMultipleValues":false,"parts":[{"type":"floatV2","byteOrder":"LITTLE_ENDIAN","bitmapSerdeFactory":{"type":"roaring"}}]}"#;

    /// Build a raw-uncompressed (0xfe) numeric part over pre-encoded
    /// little-endian value bytes.
    fn raw_numeric_part(value_bytes: &[u8], num_values: u32) -> Vec<u8> {
        let mut body = vec![2u8];
        body.extend_from_slice(&num_values.to_be_bytes());
        body.extend_from_slice(&0x2000u32.to_be_bytes());
        body.push(COMPRESSION_NONE_RAW);
        body.extend_from_slice(value_bytes);
        let mut part = Vec::new();
        part.extend_from_slice(&(body.len() as u32).to_be_bytes());
        part.extend_from_slice(&body);
        part
    }

    /// Frame a portable-Roaring bitmap of `rows` as the trailing null
    /// section: `[u32 BE size][portable roaring bytes]`.
    fn framed_null_bitmap(rows: &[u32]) -> Vec<u8> {
        let bm = DruidBitmap::from_sorted_iter(rows.iter().copied());
        let mut payload = Vec::new();
        bm.as_inner()
            .serialize_into(&mut payload)
            .expect("serialize null bitmap");
        let mut out = Vec::new();
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Nullable LONG with rows {1, 3} NULL: the decode preserves the exact
    /// `i64` values and marks the NULL rows in an explicit bitmap
    /// (`ColumnData::LongNullable` — the ingestion-side convention, see
    /// `SegmentDataBuilder::add_long_column_nullable`).  Pre-2026-07 this
    /// degraded to a NaN-null DOUBLE, silently losing precision beyond
    /// ±2^53; the 2^53+1 value below pins the exactness.
    #[test]
    fn decodes_nullable_long_null_bitmap() {
        let mut values = Vec::new();
        for v in [10i64, 0, 9_007_199_254_740_993, 0, 50] {
            values.extend_from_slice(&v.to_le_bytes());
        }
        let mut blob = column_blob(NULLABLE_LONG_DESC, &raw_numeric_part(&values, 5));
        blob.extend_from_slice(&framed_null_bitmap(&[1, 3]));
        match decode_native_column(&blob).expect("decode nullable long") {
            ColumnData::LongNullable(v, nulls) => {
                assert_eq!(
                    v,
                    vec![10, 0, 9_007_199_254_740_993, 0, 50],
                    "values must be i64-exact (2^53+1 must NOT round to ...992)"
                );
                assert_eq!(nulls.len(), 2);
                assert!(nulls.contains(1), "row 1 must be NULL");
                assert!(nulls.contains(3), "row 3 must be NULL");
            }
            other => panic!("null-bearing long must decode as LongNullable, got {other:?}"),
        }
    }

    #[test]
    fn decodes_nullable_double_and_float_null_bitmaps() {
        let mut dvals = Vec::new();
        for v in [1.5f64, 0.0, 2.5] {
            dvals.extend_from_slice(&v.to_le_bytes());
        }
        let mut dblob = column_blob(NULLABLE_DOUBLE_DESC, &raw_numeric_part(&dvals, 3));
        dblob.extend_from_slice(&framed_null_bitmap(&[1]));
        match decode_native_column(&dblob).expect("decode nullable double") {
            ColumnData::Double(v) => {
                assert_eq!(v[0], 1.5);
                assert!(v[1].is_nan(), "row 1 must be NULL (NaN)");
                assert_eq!(v[2], 2.5);
            }
            other => panic!("expected Double, got {other:?}"),
        }

        let mut fvals = Vec::new();
        for v in [9.5f32, 0.0, 7.5] {
            fvals.extend_from_slice(&v.to_le_bytes());
        }
        let mut fblob = column_blob(NULLABLE_FLOAT_DESC, &raw_numeric_part(&fvals, 3));
        fblob.extend_from_slice(&framed_null_bitmap(&[1]));
        match decode_native_column(&fblob).expect("decode nullable float") {
            ColumnData::Float(v) => {
                assert_eq!(v[0], 9.5);
                assert!(v[1].is_nan(), "row 1 must be NULL (NaN)");
                assert_eq!(v[2], 7.5);
            }
            other => panic!("expected Float, got {other:?}"),
        }
    }

    /// A nullable part with NO trailing section is null-free and must keep
    /// its native type (this is also what every real druid31_* fixture
    /// exercises — they all carry `bitmapSerdeFactory`).
    #[test]
    fn nullable_long_without_trailing_section_stays_long() {
        let mut values = Vec::new();
        for v in [7i64, 8] {
            values.extend_from_slice(&v.to_le_bytes());
        }
        let blob = column_blob(NULLABLE_LONG_DESC, &raw_numeric_part(&values, 2));
        match decode_native_column(&blob).expect("decode null-free nullable long") {
            ColumnData::Long(v) => assert_eq!(v, vec![7, 8]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// An EMPTY trailing null bitmap means no NULL rows: the long column
    /// must stay LONG (no gratuitous Double conversion).
    #[test]
    fn nullable_long_empty_null_bitmap_stays_long() {
        let mut values = Vec::new();
        for v in [7i64, 8] {
            values.extend_from_slice(&v.to_le_bytes());
        }
        let mut blob = column_blob(NULLABLE_LONG_DESC, &raw_numeric_part(&values, 2));
        blob.extend_from_slice(&framed_null_bitmap(&[]));
        match decode_native_column(&blob).expect("decode empty-null-bitmap long") {
            ColumnData::Long(v) => assert_eq!(v, vec![7, 8]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// The framed length must consume the blob exactly — both a length that
    /// overruns the remaining bytes and one that leaves bytes behind are
    /// refused (never read OOB, never guess at extra sections).
    #[test]
    fn nullable_null_section_bad_framing_fails_closed() {
        let mut values = Vec::new();
        for v in [7i64, 8] {
            values.extend_from_slice(&v.to_le_bytes());
        }
        let blob = column_blob(NULLABLE_LONG_DESC, &raw_numeric_part(&values, 2));

        // Length frame claims 9999 bytes that do not exist.
        let mut overrun = blob.clone();
        overrun.extend_from_slice(&9999u32.to_be_bytes());
        overrun.extend_from_slice(&[0u8; 4]);
        let err = decode_native_column(&overrun)
            .expect_err("overlong null-bitmap frame must fail closed");
        assert!(err.to_string().contains("declares"), "got: {err}");

        // Well-formed frame followed by extra garbage bytes.
        let mut leftover = blob;
        leftover.extend_from_slice(&framed_null_bitmap(&[1]));
        leftover.extend_from_slice(&[0xEE; 3]);
        let err = decode_native_column(&leftover)
            .expect_err("bytes after the null bitmap must fail closed");
        assert!(err.to_string().contains("declares"), "got: {err}");
    }

    /// A null bitmap referencing a row at/beyond the column's row count can
    /// never be legitimate and must be rejected before nulls are applied.
    #[test]
    fn nullable_null_bitmap_row_out_of_range_fails_closed() {
        let mut values = Vec::new();
        for v in [7i64, 8] {
            values.extend_from_slice(&v.to_le_bytes());
        }
        let mut blob = column_blob(NULLABLE_LONG_DESC, &raw_numeric_part(&values, 2));
        blob.extend_from_slice(&framed_null_bitmap(&[5]));
        let err = decode_native_column(&blob)
            .expect_err("null bitmap referencing a nonexistent row must fail closed");
        assert!(err.to_string().contains("references row"), "got: {err}");
    }

    /// A non-roaring null-bitmap codec is refused by name (index.drd already
    /// rejects concise segments, but the per-column factory is validated
    /// independently — fail loud, never guess).
    #[test]
    fn nullable_non_roaring_codec_fails_closed() {
        let desc = br#"{"valueType":"LONG","parts":[{"type":"longV2","bitmapSerdeFactory":{"type":"concise"}}]}"#;
        let mut values = Vec::new();
        for v in [7i64, 8] {
            values.extend_from_slice(&v.to_le_bytes());
        }
        let mut blob = column_blob(desc, &raw_numeric_part(&values, 2));
        blob.extend_from_slice(&framed_null_bitmap(&[1]));
        let err =
            decode_native_column(&blob).expect_err("concise null-bitmap codec must be rejected");
        assert!(err.to_string().contains("codec `concise`"), "got: {err}");
    }

    // -- `longEncoding: auto` (table/delta bit-packed longs) -----------------
    //
    // First-approximation byte layouts (header shape + MSB-first packing per
    // the module docs); the real-Druid authority is the
    // `druid_writes_ferrodruid_reads_auto_long` harness test in
    // tests/segment-compat.

    /// Flagged compression bytes (plain id + 0x82 mod 256, per module docs).
    const AUTO_NONE: u8 = 0x80;
    const AUTO_UNCOMPRESSED: u8 = 0x81;
    const AUTO_LZF: u8 = 0x82;
    const AUTO_LZ4: u8 = 0x83;

    const AUTO_LONG_DESC: &[u8] = br#"{"valueType":"LONG","parts":[{"type":"longV2"}]}"#;

    /// Pack `values` as a contiguous MSB-first `bits`-wide run plus the 4
    /// zero closing bytes (one block / one entire stream).
    fn pack_be(values: &[u64], bits: u32) -> Vec<u8> {
        let width = bits as usize;
        let total_bits = values.len() * width;
        let mut out = vec![0u8; total_bits.div_ceil(8) + PACKED_CLOSING_PAD];
        for (i, &v) in values.iter().enumerate() {
            for b in 0..width {
                if (v >> (width - 1 - b)) & 1 != 0 {
                    let bit = i * width + b;
                    out[bit / 8] |= 1 << (7 - (bit % 8));
                }
            }
        }
        out
    }

    /// Frame an auto-encoded longV2 part: shared numeric head + the given
    /// encoding header (format byte onward) + the packed payload (block
    /// container or entire stream).
    fn auto_long_part(
        num_values: u32,
        values_per_block: u32,
        flagged_compression: u8,
        encoding_header: &[u8],
        payload: &[u8],
    ) -> Vec<u8> {
        let mut body = vec![2u8];
        body.extend_from_slice(&num_values.to_be_bytes());
        body.extend_from_slice(&values_per_block.to_be_bytes());
        body.push(flagged_compression);
        body.extend_from_slice(encoding_header);
        body.extend_from_slice(payload);
        let mut part = Vec::new();
        part.extend_from_slice(&(body.len() as u32).to_be_bytes());
        part.extend_from_slice(&body);
        part
    }

    /// `[format=table][version][u32 BE size][i64 BE × size]`.
    fn table_header(table: &[i64]) -> Vec<u8> {
        let mut h = vec![LONG_ENCODING_FORMAT_TABLE, LONG_ENCODING_HEADER_VERSION];
        h.extend_from_slice(&(table.len() as u32).to_be_bytes());
        for v in table {
            h.extend_from_slice(&v.to_be_bytes());
        }
        h
    }

    /// `[format=delta][version][i64 BE base][u32 BE bits]`.
    fn delta_header(base: i64, bits: u32) -> Vec<u8> {
        let mut h = vec![LONG_ENCODING_FORMAT_DELTA, LONG_ENCODING_HEADER_VERSION];
        h.extend_from_slice(&base.to_be_bytes());
        h.extend_from_slice(&bits.to_be_bytes());
        h
    }

    /// TABLE encoding over uncompressed blocks, exercising the 2-bit index
    /// width (3-entry table), a negative table value, and a partial last
    /// block (5 values at 4 per block).
    #[test]
    fn decodes_auto_table_longs_uncompressed_blocks() {
        let table = [10i64, -20, 30];
        let ids = [0u64, 1, 2, 1, 0];
        // bits_for_table_size(3) == 2.
        let block0 = pack_be(&ids[..4], 2);
        let block1 = pack_be(&ids[4..], 2);
        let payload = gi(&[Some(&block0), Some(&block1)]);
        let part = auto_long_part(5, 4, AUTO_UNCOMPRESSED, &table_header(&table), &payload);
        match decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect("decode auto table long")
        {
            ColumnData::Long(v) => assert_eq!(v, vec![10, -20, 30, -20, 10]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// DELTA encoding over real LZ4 blocks (the default-compression shape a
    /// real auto segment was observed to mark as 0x83), with a negative
    /// base, a 12-bit width, and the offset that saturates the width (4095).
    #[test]
    fn decodes_auto_delta_longs_lz4_blocks() {
        let base = -1_000i64;
        let bits = 12u32;
        let offsets = [0u64, 5, 4095, 700];
        #[allow(clippy::cast_possible_wrap)]
        let expect: Vec<i64> = offsets.iter().map(|&o| base + o as i64).collect();
        let b0 = pack_be(&offsets[..2], bits);
        let b1 = pack_be(&offsets[2..], bits);
        let c0 = compression::compress(compression::Codec::Lz4, &b0).expect("compress b0");
        let c1 = compression::compress(compression::Codec::Lz4, &b1).expect("compress b1");
        // Strip the 4-byte lz4_flex size prefix → raw LZ4 blocks (the
        // framing used inside upstream block containers).
        let payload = gi(&[Some(&c0[4..]), Some(&c1[4..])]);
        let part = auto_long_part(4, 2, AUTO_LZ4, &delta_header(base, bits), &payload);
        match decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect("decode auto delta long")
        {
            ColumnData::Long(v) => assert_eq!(v, expect),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// Flagged-`none` (0x80): one contiguous packed run, no block container.
    #[test]
    fn decodes_auto_delta_entire_layout_none() {
        let offsets = [1u64, 2, 250];
        let packed = pack_be(&offsets, 8);
        let part = auto_long_part(3, 0, AUTO_NONE, &delta_header(100, 8), &packed);
        match decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect("decode auto delta entire layout")
        {
            ColumnData::Long(v) => assert_eq!(v, vec![101, 102, 350]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// Full-width (64-bit) delta offsets round-trip, including the value
    /// whose top bit is set (sign-reinterpreted, Java-wrapping add).
    #[test]
    fn decodes_auto_delta_full_width_64() {
        let offsets = [0u64, 0x8000_0000_0000_0000];
        let packed = pack_be(&offsets, 64);
        let part = auto_long_part(2, 0, AUTO_NONE, &delta_header(0, 64), &packed);
        match decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect("decode 64-bit auto delta")
        {
            ColumnData::Long(v) => assert_eq!(v, vec![0, i64::MIN]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// `base + offset` wraps with Java long semantics (the upstream reader
    /// adds with wrapping arithmetic; documented, not an error).
    #[test]
    fn auto_delta_add_wraps_like_java() {
        let packed = pack_be(&[1u64], 1);
        let part = auto_long_part(1, 0, AUTO_NONE, &delta_header(i64::MAX, 1), &packed);
        match decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect("decode wrapping auto delta")
        {
            ColumnData::Long(v) => assert_eq!(v, vec![i64::MIN]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// An auto-encoded LONG composes with the trailing null section exactly
    /// like a plain LONG: decode the values via the table, THEN mark the
    /// null rows — a null-bearing LONG becomes `ColumnData::LongNullable`
    /// with exact `i64` values (the NULL row's in-band value normalizes
    /// to 0).
    #[test]
    fn decodes_nullable_auto_long_null_bitmap() {
        let table = [7i64, 9];
        let ids = [0u64, 1, 0];
        // bits_for_table_size(2) == 1.
        let packed = pack_be(&ids, 1);
        let part = auto_long_part(3, 0, AUTO_NONE, &table_header(&table), &packed);
        let mut blob = column_blob(NULLABLE_LONG_DESC, &part);
        blob.extend_from_slice(&framed_null_bitmap(&[1]));
        match decode_native_column(&blob).expect("decode nullable auto long") {
            ColumnData::LongNullable(v, nulls) => {
                assert_eq!(v, vec![7, 0, 7], "NULL row value normalizes to 0");
                assert_eq!(nulls.len(), 1);
                assert!(nulls.contains(1), "row 1 must be NULL");
            }
            other => panic!("null-bearing auto long must decode as LongNullable, got {other:?}"),
        }
    }

    /// A null-free auto-encoded LONG under a nullable descriptor stays LONG.
    #[test]
    fn nullable_auto_long_without_nulls_stays_long() {
        let packed = pack_be(&[0u64, 1], 1);
        let part = auto_long_part(2, 0, AUTO_NONE, &table_header(&[5, 6]), &packed);
        let blob = column_blob(NULLABLE_LONG_DESC, &part);
        match decode_native_column(&blob).expect("decode null-free nullable auto long") {
            ColumnData::Long(v) => assert_eq!(v, vec![5, 6]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// A bit width outside the supported set fails closed before any packed
    /// byte is decoded.
    #[test]
    fn auto_delta_bad_bit_width_fails_closed() {
        let part = auto_long_part(1, 0, AUTO_NONE, &delta_header(0, 13), &[0u8; 6]);
        let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect_err("unsupported bit width must fail closed");
        assert!(err.to_string().contains("bit width 13"), "got: {err}");
    }

    /// A packed table index at/beyond the table size fails closed.
    #[test]
    fn auto_table_index_out_of_range_fails_closed() {
        // 3-entry table → 2-bit ids; id 3 is out of range.
        let packed = pack_be(&[3u64], 2);
        let part = auto_long_part(1, 0, AUTO_NONE, &table_header(&[1, 2, 3]), &packed);
        let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect_err("out-of-range table index must fail closed");
        assert!(err.to_string().contains("out of range"), "got: {err}");
    }

    /// Table sizes 0 and >256 are both refused from the header alone.
    #[test]
    fn auto_table_size_out_of_range_fails_closed() {
        for size in [0u32, 257] {
            let mut header = vec![LONG_ENCODING_FORMAT_TABLE, LONG_ENCODING_HEADER_VERSION];
            header.extend_from_slice(&size.to_be_bytes());
            let part = auto_long_part(1, 0, AUTO_NONE, &header, &[0u8; 5]);
            let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
                .expect_err("hostile table size must fail closed");
            assert!(
                err.to_string().contains(&format!("table size {size}")),
                "got: {err}"
            );
        }
    }

    /// A table declaring more entries than the part holds fails closed on
    /// the bounded read (no OOB, no guessing).
    #[test]
    fn auto_table_truncated_fails_closed() {
        let mut header = vec![LONG_ENCODING_FORMAT_TABLE, LONG_ENCODING_HEADER_VERSION];
        header.extend_from_slice(&4u32.to_be_bytes());
        header.extend_from_slice(&1i64.to_be_bytes()); // 1 of 4 declared entries
        let part = auto_long_part(1, 0, AUTO_NONE, &header, &[]);
        let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect_err("truncated table must fail closed");
        assert!(err.to_string().contains("truncated"), "got: {err}");
    }

    /// An unknown encoding-format id fails closed.
    #[test]
    fn auto_unknown_format_fails_closed() {
        let header = [0x02u8, LONG_ENCODING_HEADER_VERSION];
        let part = auto_long_part(1, 0, AUTO_NONE, &header, &[0u8; 5]);
        let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect_err("unknown encoding format must fail closed");
        assert!(err.to_string().contains("encoding format"), "got: {err}");
    }

    /// A flagged compression id we cannot decode (LZF, 0x82) fails closed.
    #[test]
    fn auto_flagged_lzf_compression_fails_closed() {
        let part = auto_long_part(1, 1, AUTO_LZF, &delta_header(0, 8), &[]);
        let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect_err("flagged LZF must fail closed");
        assert!(
            err.to_string().contains("unsupported block compression"),
            "got: {err}"
        );
    }

    /// A truncated auto header (flag byte with nothing after it — the exact
    /// shape the pre-support reader used to reject wholesale) fails closed.
    #[test]
    fn auto_truncated_header_fails_closed() {
        let part = [2u8, 0, 0, 0, 5, 0, 0, 0x20, 0, AUTO_LZ4];
        let mut blob = Vec::new();
        blob.extend_from_slice(&(AUTO_LONG_DESC.len() as u32).to_be_bytes());
        blob.extend_from_slice(AUTO_LONG_DESC);
        blob.extend_from_slice(&(part.len() as u32).to_be_bytes());
        blob.extend_from_slice(&part);
        let err = decode_native_column(&blob).expect_err("truncated auto header must fail closed");
        assert!(err.to_string().contains("truncated"), "got: {err}");
    }

    /// Declared packed output with no backing block input trips the
    /// amplification bound before any buffer is sized from it.
    #[test]
    fn auto_packed_amplification_bound_fails_closed() {
        // 8M declared 64-bit values (64 MiB packed, 8 blocks of 1M values)
        // "backed" by eight 4-byte garbage blocks (32 B of input): 64 MiB
        // > 256 × 32 B, so the aggregate bound must trip before any
        // decompression scratch is allocated.
        let garbage = [0xAAu8; 4];
        let blocks: Vec<Option<&[u8]>> = (0..8).map(|_| Some(&garbage[..])).collect();
        let payload = gi(&blocks);
        let part = auto_long_part(
            8 * 1024 * 1024,
            1024 * 1024,
            AUTO_LZ4,
            &delta_header(0, 64),
            &payload,
        );
        let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect_err("unbacked packed declaration must fail closed");
        assert!(err.to_string().contains("amplification"), "got: {err}");
    }

    // -- declared-count bounds (head cap + packed input consistency) ---------

    /// A hostile declared count far beyond any real segment must be
    /// rejected by the HEAD cap — not merely by downstream truncation,
    /// which an attacker can afford to satisfy: at `bits = 1`, 2^27
    /// declared TABLE ids need only ~16 MiB of packed input yet reserve
    /// 1 GiB of packed ids plus 1 GiB of looked-up longs (under the old
    /// 2^28 cap: ~32 MiB of input reserved ~2 GiB twice).
    #[test]
    fn hostile_num_values_rejected_by_head_cap() {
        let hostile: u32 = 128 * 1024 * 1024; // 2^27, over the 2^26 cap
        let expected = format!("declared {hostile} values exceeds cap {MAX_COLUMN_VALUES}");
        // longV2 `longEncoding: auto` (TABLE): rejected from the shared
        // numeric head, before the encoding header or table is parsed.
        let part = auto_long_part(hostile, 0, AUTO_NONE, &table_header(&[1, 2]), &[0u8; 8]);
        let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect_err("over-cap auto longV2 count must fail closed");
        assert!(
            err.to_string().contains(&expected),
            "rejection must come from the head cap, got: {err}"
        );
        // Plain longV2 (no encoding flag): same shared head, same rejection.
        let part = raw_numeric_part(&[], hostile);
        let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect_err("over-cap plain longV2 count must fail closed");
        assert!(
            err.to_string().contains(&expected),
            "rejection must come from the head cap, got: {err}"
        );
    }

    /// The cap boundary is strict: `cap + 1` is rejected by the head check
    /// alone; `cap` itself clears the head check and fails only because
    /// this crafted part is truncated (nothing is allocated either way).
    #[test]
    fn num_values_cap_boundary_is_exact() {
        let over = u32::try_from(MAX_COLUMN_VALUES + 1).expect("cap + 1 fits in u32");
        for part in [
            auto_long_part(over, 0, AUTO_NONE, &delta_header(0, 1), &[0u8; 8]),
            raw_numeric_part(&[], over),
        ] {
            let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
                .expect_err("count just over the cap must fail closed");
            assert!(err.to_string().contains("exceeds cap"), "got: {err}");
        }
        let at_cap = u32::try_from(MAX_COLUMN_VALUES).expect("cap fits in u32");
        let part = auto_long_part(at_cap, 0, AUTO_NONE, &delta_header(0, 1), &[0u8; 8]);
        let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect_err("at-cap part with 8 payload bytes is truncated");
        let msg = err.to_string();
        assert!(
            msg.contains("truncated") && !msg.contains("exceeds cap"),
            "an at-cap count must clear the head cap and fail only on the \
             missing packed bytes, got: {err}"
        );
    }

    /// A flagged-`none` packed stream shorter than
    /// `ceil(num_values × bits / 8)` is malformed and must fail on the
    /// input-consistency bound BEFORE any vector is reserved from the
    /// declared count; a fully-backed normal count still decodes exactly.
    #[test]
    fn truncated_packed_stream_fails_before_allocation() {
        // 1 000 declared 8-bit offsets need 1 000 + 4 packed bytes; supply 8.
        let part = auto_long_part(1_000, 0, AUTO_NONE, &delta_header(0, 8), &[0u8; 8]);
        let err = decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect_err("truncated packed stream must fail closed");
        assert!(err.to_string().contains("truncated"), "got: {err}");
        // A normal, fully-backed count still decodes exactly.
        let packed = pack_be(&[0u64, 1, 1, 0], 1);
        let part = auto_long_part(4, 0, AUTO_NONE, &table_header(&[40, 50]), &packed);
        match decode_native_column(&column_blob(AUTO_LONG_DESC, &part))
            .expect("normal count decodes")
        {
            ColumnData::Long(v) => assert_eq!(v, vec![40, 50, 50, 40]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    // -- legacy null-generation signature (see crate::null_generation) -------

    /// Build a native string-column part with the given dictionary entries
    /// (`None` = the upstream `-1` null marker) over 4 rows with ordinals
    /// `[0, 1, 1, 0]` and correctly partitioned value bitmaps.
    fn string_column_part(dict: &[Option<&[u8]>]) -> Vec<u8> {
        let mut part = vec![0u8]; // stringDictionary version 0 (no flags)
        part.extend_from_slice(&gi(dict));
        // Uncompressed (version 0) row ordinals: 4 rows, 1-byte width,
        // 3 padding bytes; ordinals 0, 1, 1, 0.
        part.extend_from_slice(&[0u8, 1]);
        part.extend_from_slice(&7u32.to_be_bytes());
        part.extend_from_slice(&[0u8, 1, 1, 0, 0, 0, 0]);
        // Value bitmaps: entry 0 owns rows {0, 3}, entry 1 owns rows {1, 2}.
        let bm0 = DruidBitmap::from_sorted_iter([0u32, 3].into_iter());
        let bm1 = DruidBitmap::from_sorted_iter([1u32, 2].into_iter());
        let mut p0 = Vec::new();
        bm0.as_inner().serialize_into(&mut p0).expect("serialize");
        let mut p1 = Vec::new();
        bm1.as_inner().serialize_into(&mut p1).expect("serialize");
        part.extend_from_slice(&gi(&[Some(&p0), Some(&p1)]));
        part
    }

    /// TG-1 R4 symmetric (reader): a real Druid dictionary is sorted in Java
    /// UTF-16 code-unit order, which for supplementary characters is NOT Rust
    /// codepoint order.  The reader must validate (and accept) UTF-16 order —
    /// a Rust-order check would spuriously reject this legitimate segment
    /// because `U+10000 > U+E000` in Rust but `U+10000 < U+E000` in UTF-16.
    #[test]
    fn string_dictionary_utf16_sorted_supplementary_is_accepted() {
        // UTF-16 order: "A" < U+10000 (0xD800 0xDC00) < U+E000 (0xE000).
        let dict = &[
            Some("A".as_bytes()),
            Some("\u{10000}".as_bytes()),
            Some("\u{E000}".as_bytes()),
        ];
        let mut part = vec![0u8]; // stringDictionary version 0 (no flags)
        part.extend_from_slice(&gi(dict));
        // v0 row ordinals: 4 rows, 1-byte width, 3 pad bytes; ordinals 1,0,2,1.
        part.extend_from_slice(&[0u8, 1]);
        part.extend_from_slice(&7u32.to_be_bytes());
        part.extend_from_slice(&[1u8, 0, 2, 1, 0, 0, 0]);
        // Value bitmaps partition the rows: A->{1}, U+10000->{0,3}, U+E000->{2}.
        let mut bufs: Vec<Vec<u8>> = Vec::new();
        for rows in [vec![1u32], vec![0, 3], vec![2]] {
            let bm = DruidBitmap::from_sorted_iter(rows.into_iter());
            let mut p = Vec::new();
            bm.as_inner().serialize_into(&mut p).expect("serialize");
            bufs.push(p);
        }
        part.extend_from_slice(&gi(&[Some(&bufs[0]), Some(&bufs[1]), Some(&bufs[2])]));

        let desc = br#"{"valueType":"STRING","parts":[{"type":"stringDictionary"}]}"#;
        let col = decode_native_column(&column_blob(desc, &part))
            .expect("a UTF-16-sorted dictionary must decode (not be rejected as unsorted)");
        let ColumnData::String(s) = col else {
            panic!("expected String");
        };
        assert_eq!(s.dictionary.get(0), Some("A"));
        assert_eq!(s.dictionary.get(1), Some("\u{10000}"));
        assert_eq!(s.dictionary.get(2), Some("\u{E000}"));
        assert_eq!(s.encoded_values, vec![1, 0, 2, 1]);
        // Positional per-row reconstruction (groupBy/scan path — correct).
        let vals: Vec<&str> = s
            .encoded_values
            .iter()
            .map(|&o| s.dictionary.get(o as usize).expect("ordinal in range"))
            .collect();
        assert_eq!(vals, vec!["\u{10000}", "A", "\u{E000}", "\u{10000}"]);
    }

    /// A legacy-generation-consistent native column — `""` as a REGULAR
    /// dictionary value, no leading null entry, no null-row bitmap — must
    /// decode with `null_rows() == None` and classify as Unconfirmed WITH
    /// the legacy signature.
    #[test]
    fn native_legacy_empty_string_column_classifies_unconfirmed() {
        let desc = br#"{"valueType":"STRING","parts":[{"type":"stringDictionary"}]}"#;
        let part = string_column_part(&[Some(b""), Some(b"de")]);
        let col = decode_native_column(&column_blob(desc, &part)).expect("decode");
        let ColumnData::String(ref s) = col else {
            panic!("expected String, got {col:?}");
        };
        assert!(s.null_rows().is_none(), "no null-row bitmap in this layout");
        let class = crate::null_generation::classify_column("language", &col);
        assert_eq!(
            class.handling,
            crate::null_generation::NullHandling::Unconfirmed
        );
        assert!(class.legacy_signature);
    }

    /// A modern native column — leading `-1` null dictionary entry — must
    /// decode to the null-row-bitmap layout and classify as ConfirmedModern
    /// (a legacy writer never emits the null entry).
    #[test]
    fn native_modern_null_entry_classifies_confirmed_modern() {
        let desc = br#"{"valueType":"STRING","parts":[{"type":"stringDictionary"}]}"#;
        let part = string_column_part(&[None, Some(b"de")]);
        let col = decode_native_column(&column_blob(desc, &part)).expect("decode");
        let ColumnData::String(ref s) = col else {
            panic!("expected String, got {col:?}");
        };
        let nulls = s.null_rows().expect("null entry maps to a null bitmap");
        assert!(nulls.contains(0) && nulls.contains(3));
        let class = crate::null_generation::classify_column("language", &col);
        assert_eq!(
            class.handling,
            crate::null_generation::NullHandling::ConfirmedModern
        );
        assert!(!class.legacy_signature);
    }

    // -- complex (thetaSketch) columns (compat-8 sketch #2) ------------------

    const THETA_DESC: &[u8] = br#"{"valueType":"COMPLEX","hasMultipleValues":false,"parts":[{"type":"complex","typeName":"thetaSketch"}]}"#;

    /// Build a DataSketches compact Theta image (little-endian): preamble
    /// `[preLongs][serVer=3][family=3][lgNomLongs=12][lgArrLongs][flags]
    /// [seedHash]`, then (preLongs 2) `[count i32][unused]`, then hashes.
    fn compact_theta(pre_longs: u8, flags: u8, hashes: &[u64]) -> Vec<u8> {
        let mut buf = vec![pre_longs, 3, 3, 12, 13, flags, 0x1E, 0x93];
        if pre_longs >= 2 {
            buf.extend_from_slice(&(hashes.len() as i32).to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
        for &h in hashes {
            buf.extend_from_slice(&h.to_le_bytes());
        }
        buf
    }

    /// A real-shaped `thetaSketch` complex column — a generic-indexed
    /// container of per-row compact images directly after the descriptor —
    /// decodes into per-row Druid-origin sketches whose estimates are
    /// exact below capacity, and whose union deduplicates in the shared
    /// hash space.
    #[test]
    fn decodes_theta_sketch_complex_column_per_row() {
        const EMPTY_FLAG: u8 = 1 << 2;
        let row0 = compact_theta(2, 0, &[10, 20]); // 2 users
        let row1 = compact_theta(2, 0, &[20, 30, 40]); // 3 users, 1 shared
        let row2 = compact_theta(1, EMPTY_FLAG, &[]); // empty sketch
        let part = gi(&[Some(&row0), Some(&row1), Some(&row2)]);
        let col = decode_native_column(&column_blob(THETA_DESC, &part))
            .expect("decode thetaSketch column");
        let ColumnData::ComplexTheta(rows) = col else {
            panic!("expected ComplexTheta, got {col:?}");
        };
        assert_eq!(rows.len(), 3);
        assert!((rows[0].estimate() - 2.0).abs() < f64::EPSILON);
        assert!((rows[1].estimate() - 3.0).abs() < f64::EPSILON);
        assert!((rows[2].estimate() - 0.0).abs() < f64::EPSILON);
        assert!(
            rows.iter()
                .all(ferrodruid_sketches::ThetaSketch::is_druid_origin)
        );
        // Union across rows (the query-time merge): {10,20} ∪ {20,30,40}
        // ∪ {} = 4 distinct — exact in the shared Druid hash space.
        let u = rows[0]
            .union(&rows[1])
            .and_then(|u| u.union(&rows[2]))
            .expect("druid-space union");
        assert!((u.estimate() - 4.0).abs() < f64::EPSILON);
    }

    /// A null row marker (and a zero-length payload) decodes as an EMPTY
    /// sketch rather than failing the column.
    #[test]
    fn theta_sketch_null_and_empty_rows_decode_as_empty_sketches() {
        let row0 = compact_theta(2, 0, &[7]);
        let part = gi(&[Some(&row0), None, Some(&[])]);
        let col = decode_native_column(&column_blob(THETA_DESC, &part)).expect("decode");
        let ColumnData::ComplexTheta(rows) = col else {
            panic!("expected ComplexTheta, got {col:?}");
        };
        assert_eq!(rows.len(), 3);
        assert!((rows[0].estimate() - 1.0).abs() < f64::EPSILON);
        assert!((rows[1].estimate() - 0.0).abs() < f64::EPSILON);
        assert!((rows[2].estimate() - 0.0).abs() < f64::EPSILON);
    }

    /// Only `thetaSketch` and `hyperUnique` decode: every other complex
    /// typeName — the DataSketches types whose serialized forms are not
    /// decodable — stays a loud rejection (dropped with a manifest under
    /// the migrate `--allow-unreadable-columns` gate).  `hyperUnique` left
    /// this list in W-A (v1.5.0).
    #[test]
    fn non_theta_complex_types_stay_rejected() {
        for type_name in ["HLLSketch", "quantilesDoublesSketch"] {
            let desc = format!(
                r#"{{"valueType":"COMPLEX","parts":[{{"type":"complex","typeName":"{type_name}"}}]}}"#
            );
            let part = gi(&[Some(&[0u8; 8])]);
            let err = decode_native_column(&column_blob(desc.as_bytes(), &part))
                .expect_err("non-theta complex must fail closed");
            assert!(
                err.to_string().contains(type_name),
                "rejection must name the type, got: {err}"
            );
        }
        // A complex part with NO typeName is equally undecodable.
        let desc = br#"{"valueType":"COMPLEX","parts":[{"type":"complex"}]}"#;
        let part = gi(&[Some(&[0u8; 8])]);
        let err = decode_native_column(&column_blob(desc, &part))
            .expect_err("typeName-less complex must fail closed");
        assert!(
            err.to_string().contains("unsupported complex"),
            "got: {err}"
        );
    }

    // -- complex (hyperUnique) columns (W-A, v1.5.0) -------------------------

    const HU_DESC: &[u8] = br#"{"valueType":"COMPLEX","hasMultipleValues":false,"parts":[{"type":"complex","typeName":"hyperUnique"}]}"#;

    /// Build a sparse hyperUnique blob: the observed 7-byte header plus
    /// `(BE u16 byte position, packed register byte)` pairs.
    fn hu_sparse_blob(non_zero: u16, pairs: &[(u16, u8)]) -> Vec<u8> {
        let mut buf = vec![0x01, 0x00];
        buf.extend_from_slice(&non_zero.to_be_bytes());
        buf.extend_from_slice(&[0x00, 0x00, 0x00]);
        for &(pos, val) in pairs {
            buf.extend_from_slice(&pos.to_be_bytes());
            buf.push(val);
        }
        buf
    }

    /// A real-shaped `hyperUnique` complex column — a generic-indexed
    /// container of per-row register blobs — decodes into per-row
    /// merge-only sketches; null and zero-length rows decode as EMPTY
    /// sketches, and the row-wise fold is the register-wise max.
    #[test]
    fn decodes_hyper_unique_complex_column_per_row() {
        // Row 0: one register set (the observed single-user shape).
        let row0 = hu_sparse_blob(1, &[(0x03E0, 0x20)]);
        // Row 1: two registers set in different page bytes.
        let row1 = hu_sparse_blob(2, &[(0x0023, 0x10), (0x03E0, 0x30)]);
        let part = gi(&[Some(&row0), Some(&row1), None, Some(&[])]);
        let col =
            decode_native_column(&column_blob(HU_DESC, &part)).expect("decode hyperUnique column");
        let ColumnData::ComplexHyperUnique(rows) = col else {
            panic!("expected ComplexHyperUnique, got {col:?}");
        };
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].num_non_zero(), 1);
        assert_eq!(rows[1].num_non_zero(), 2);
        assert!(rows[2].is_empty(), "null row decodes as empty sketch");
        assert!(
            rows[3].is_empty(),
            "zero-length row decodes as empty sketch"
        );
        // Fold: row0's register at byte 0x3E0 (value nibble 2) is absorbed
        // by row1's larger value (3) at the same position → 2 occupied.
        let folded = rows[0].merged(&rows[1]).merged(&rows[2]);
        assert_eq!(folded.num_non_zero(), 2);
        // Linear counting over 2 occupied registers of 2048.
        let want = 2048.0_f64 * (2048.0_f64 / 2046.0).ln();
        assert_eq!(folded.estimate().to_bits(), want.to_bits());
    }

    /// A malformed hyperUnique row blob fails the column loudly.
    #[test]
    fn hyper_unique_malformed_rows_fail_closed() {
        // Unknown version byte.
        let mut bad = hu_sparse_blob(1, &[(1, 0x01)]);
        bad[0] = 0x02;
        let part = gi(&[Some(&bad)]);
        let err = decode_native_column(&column_blob(HU_DESC, &part))
            .expect_err("bad version must fail closed");
        assert!(err.to_string().contains("hyperUnique row 0"), "got: {err}");
        // Non-zero register offset byte (never observed → rejected).
        let mut offset = hu_sparse_blob(1, &[(1, 0x01)]);
        offset[1] = 0x01;
        let part = gi(&[Some(&offset)]);
        let err = decode_native_column(&column_blob(HU_DESC, &part))
            .expect_err("non-zero offset must fail closed");
        assert!(err.to_string().contains("register-offset"), "got: {err}");
    }

    /// A malformed per-row image fails the column loudly (never guesses),
    /// and trailing bytes after the row container are refused.
    #[test]
    fn theta_sketch_malformed_rows_fail_closed() {
        // Truncated image (preamble only claims hashes that are absent).
        let mut bad = compact_theta(2, 0, &[1, 2]);
        bad.truncate(bad.len() - 8);
        let part = gi(&[Some(&bad)]);
        let err = decode_native_column(&column_blob(THETA_DESC, &part))
            .expect_err("truncated image must fail closed");
        assert!(err.to_string().contains("row 0"), "got: {err}");

        // Trailing bytes after the generic-indexed container.
        let good = compact_theta(2, 0, &[1]);
        let mut blob = column_blob(THETA_DESC, &gi(&[Some(&good)]));
        blob.extend_from_slice(&[0xAB; 4]);
        let err = decode_native_column(&blob)
            .expect_err("trailing bytes after the rows must fail closed");
        assert!(err.to_string().contains("trailing"), "got: {err}");
    }

    /// A per-row image declaring the family-maximum retained count (2^26
    /// hashes = 512 MiB of backing) over a tiny body must fail loudly
    /// with no count-sized allocation — the sketch decoder's exact-length
    /// check fires before anything is materialized.
    #[test]
    fn theta_sketch_huge_declared_count_with_tiny_body_is_rejected() {
        let mut img = vec![2u8, 3, 3, 12, 13, 0, 0x1E, 0x93];
        img.extend_from_slice(&(1i32 << 26).to_le_bytes()); // curCount = 2^26
        img.extend_from_slice(&0u32.to_le_bytes()); // no hashes follow
        let part = gi(&[Some(img.as_slice())]);
        let err = decode_native_column(&column_blob(THETA_DESC, &part))
            .expect_err("huge declared count over a tiny body must fail closed");
        assert!(err.to_string().contains("row 0"), "got: {err}");
    }

    /// A per-row blob over the 64 MiB cap is refused on LENGTH alone,
    /// before the sketch decoder ever sees it (an in-cap length is what
    /// bounds the per-row hash materialization).
    #[test]
    fn theta_sketch_blob_over_byte_cap_is_rejected_before_decode() {
        let huge = vec![0u8; MAX_THETA_BLOB_BYTES + 1];
        let part = gi(&[Some(huge.as_slice())]);
        let err = decode_native_column(&column_blob(THETA_DESC, &part))
            .expect_err("over-cap blob must fail closed");
        assert!(err.to_string().contains("exceeds cap"), "got: {err}");
    }

    #[test]
    fn bitmap_row_beyond_column_rows_is_rejected() {
        let desc = br#"{"valueType":"STRING","parts":[{"type":"stringDictionary"}]}"#;
        let mut part = vec![0u8]; // stringDictionary version 0 (no flags)
        part.extend_from_slice(&gi(&[Some(b"only")])); // 1-entry dictionary
        // Uncompressed (version 0) row ordinals: 1 row, 1-byte width,
        // 3 padding bytes.
        part.extend_from_slice(&[0u8, 1]);
        part.extend_from_slice(&4u32.to_be_bytes());
        part.extend_from_slice(&[0u8, 0, 0, 0]);
        // Value bitmap claiming row u32::MAX on this 1-row column.
        let mut hostile = DruidBitmap::new();
        hostile.insert(u32::MAX);
        let mut portable = Vec::new();
        hostile
            .as_inner()
            .serialize_into(&mut portable)
            .expect("serialize test bitmap");
        part.extend_from_slice(&gi(&[Some(&portable)]));
        let err = decode_native_column(&column_blob(desc, &part))
            .expect_err("bitmap referencing a nonexistent row must fail closed");
        assert!(err.to_string().contains("references row"), "got: {err}");
    }

    // -----------------------------------------------------------------------
    // Legacy v1 numeric serdes (`long` / `double` / `float`, Druid ≤27).
    // Every fixture is a verbatim byte capture from a real Apache Druid
    // 27.0.0 segment (2026-07-21); the asserted values are EXACTLY what
    // Druid 27's own `dump-segment` tool reports for the same columns.
    // -----------------------------------------------------------------------

    const D27_TIME_COL: &[u8] = include_bytes!("../testdata/druid27_time.col");
    const D27_VALUE_COL: &[u8] = include_bytes!("../testdata/druid27_value.col");
    const D27_DVAL_COL: &[u8] = include_bytes!("../testdata/druid27_dval_sum.col");
    const D27_FVAL_COL: &[u8] = include_bytes!("../testdata/druid27_fval_sum.col");
    const D27_AUTO_TIME_COL: &[u8] = include_bytes!("../testdata/druid27_auto_time.col");
    const D27_DELTA_LVAL_COL: &[u8] = include_bytes!("../testdata/druid27_delta_lval.col");

    /// `__time` of the `uc1_events` 2024-01-01 segment (10 hourly rows) and
    /// its `value` long metric — plain (`longEncoding: longs`) LZ4 blocks.
    #[test]
    fn decodes_real_druid27_v1_long_columns() {
        assert!(is_native_column(D27_TIME_COL));
        match decode_native_column(D27_TIME_COL).expect("decode Druid 27 __time") {
            ColumnData::Long(v) => assert_eq!(
                v,
                (0..10)
                    .map(|h| 1_704_070_800_000 + h * 3_600_000)
                    .collect::<Vec<i64>>()
            ),
            other => panic!("expected Long, got {other:?}"),
        }
        match decode_native_column(D27_VALUE_COL).expect("decode Druid 27 value") {
            ColumnData::Long(v) => assert_eq!(v, vec![10, 20, 30, 5, 40, 0, 15, 25, 0, 50]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// `double` / `float` v1 metrics of the `v1_dblfloat_probe` segment —
    /// bit-exact against dump-segment (including the non-round values).
    #[test]
    fn decodes_real_druid27_v1_double_and_float_columns() {
        match decode_native_column(D27_DVAL_COL).expect("decode Druid 27 double") {
            ColumnData::Double(v) => {
                // 3.141592653589793 as ingested — bit-identical to f64 π.
                assert_eq!(
                    v,
                    vec![1.5, -2.25, std::f64::consts::PI, 0.0, 12_345_678.906_25]
                );
            }
            other => panic!("expected Double, got {other:?}"),
        }
        match decode_native_column(D27_FVAL_COL).expect("decode Druid 27 float") {
            ColumnData::Float(v) => assert_eq!(v, vec![1.5, -2.25, 3.75, 0.5, -123.625]),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    /// `longEncoding: auto` under the v1 serde, TABLE format: `__time` of
    /// the `v1_autolong_probe` segment (10 distinct hourly timestamps →
    /// 4-bit table indices, flagged-LZ4 `0x83`).
    #[test]
    fn decodes_real_druid27_v1_auto_table_long() {
        match decode_native_column(D27_AUTO_TIME_COL).expect("decode Druid 27 auto __time") {
            ColumnData::Long(v) => assert_eq!(
                v,
                (0..10)
                    .map(|h| 1_709_339_400_000 + h * 3_600_000)
                    .collect::<Vec<i64>>()
            ),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// `longEncoding: auto` under the v1 serde, DELTA format: `lval_sum` of
    /// the `v1_deltalong_probe` segment (300 distinct values 500..=799 →
    /// base 500, 12-bit offsets, flagged-LZ4 `0x83`).
    #[test]
    fn decodes_real_druid27_v1_auto_delta_long() {
        match decode_native_column(D27_DELTA_LVAL_COL).expect("decode Druid 27 delta lval_sum") {
            ColumnData::Long(v) => assert_eq!(v, (500..800).collect::<Vec<i64>>()),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    /// A v1 part owns the remainder of the column blob (no length prefix),
    /// so trailing bytes CANNOT name a null section (the legacy serdes
    /// predate SQL-null storage) and must fail loudly, never be skipped.
    #[test]
    fn rejects_trailing_bytes_after_v1_part() {
        let mut blob = D27_VALUE_COL.to_vec();
        blob.push(0xAB);
        let err = decode_native_column(&blob).expect_err("trailing byte must fail closed");
        assert!(err.to_string().contains("trailing"), "got: {err}");
    }

    /// An unobserved v1 supplier version byte (only `0x02` was ever
    /// captured) fails loudly instead of guessing at an older layout.
    #[test]
    fn rejects_unknown_v1_supplier_version() {
        let mut blob = D27_VALUE_COL.to_vec();
        // The supplier version byte sits right after the 4-byte length
        // prefix + descriptor JSON.
        let json_len = u32::from_be_bytes([blob[0], blob[1], blob[2], blob[3]]) as usize;
        assert_eq!(blob[4 + json_len], 0x02);
        blob[4 + json_len] = 0x01;
        let err = decode_native_column(&blob).expect_err("unknown supplier version must fail");
        assert!(
            err.to_string().contains("unsupported part version 1"),
            "got: {err}"
        );
    }
}
