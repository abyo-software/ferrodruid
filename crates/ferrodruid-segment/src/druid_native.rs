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
//! `crates/ferrodruid-segment/testdata/druid31_*`), cross-checked against the
//! values that were ingested, plus the public segment-format documentation.
//! No upstream source code was referenced. Field names below are descriptive
//! labels chosen by us for what the bytes were observed to contain.
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
//! # Known-unsupported upstream features (fail loudly, never guess)
//!
//! * `longEncoding: auto` (table/delta-encoded longs; compression byte has
//!   the `0x80` bit set) — rejected with a descriptive error.
//! * Multi-value string dimensions (`hasMultipleValues: true`).
//! * Concise bitmaps, LZF/ZSTD block compression (not observed; rejected).
//! * Complex/sketch columns.

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
/// container. One bitmap exists per dictionary entry, so this cap mirrors
/// `MAX_STRING_BITMAPS` in [`crate::column`].
const MAX_GI_ELEMENTS: usize = 16 * 1024 * 1024;

/// Hard upper bound on the row count of a single column (mirrors
/// `MAX_NUMERIC_COLUMN_VALUES` in [`crate::column`]).
const MAX_COLUMN_VALUES: usize = 256 * 1024 * 1024;

/// Hard upper bound on a single decompressed block (64 MiB — real Druid
/// blocks are 64 KiB; the cap leaves 1000× headroom while keeping a crafted
/// `values_per_block` from driving a multi-GiB allocation).
const MAX_BLOCK_BYTES: usize = 64 * 1024 * 1024;

/// Hard upper bound on the embedded column-descriptor JSON length.
const MAX_DESCRIPTOR_JSON: usize = 1024 * 1024;

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
fn parse_generic_indexed<'a>(
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
        "longV2" => {
            let raw = read_numeric_part(data, &mut pos, 8, "longV2")?;
            let values = raw
                .chunks_exact(8)
                .map(|c| {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(c);
                    i64::from_le_bytes(b)
                })
                .collect();
            ColumnData::Long(values)
        }
        "doubleV2" => {
            let raw = read_numeric_part(data, &mut pos, 8, "doubleV2")?;
            let values = raw
                .chunks_exact(8)
                .map(|c| {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(c);
                    f64::from_le_bytes(b)
                })
                .collect();
            ColumnData::Double(values)
        }
        "floatV2" => {
            let raw = read_numeric_part(data, &mut pos, 4, "floatV2")?;
            let values = raw
                .chunks_exact(4)
                .map(|c| {
                    let mut b = [0u8; 4];
                    b.copy_from_slice(c);
                    f32::from_le_bytes(b)
                })
                .collect();
            ColumnData::Float(values)
        }
        "stringDictionary" => decode_string_dictionary_part(data, &mut pos)?,
        other => {
            return Err(DruidError::Segment(format!(
                "druid-native: unsupported column part type `{other}` (valueType {})",
                descriptor.value_type
            )));
        }
    };
    // Every observed column blob ends exactly where its part ends (verified
    // against all druid31_* fixtures). Bytes past that point are a layout we
    // do not understand — per Druid's public docs, SQL-compatible null
    // storage attaches null metadata to numeric columns, and silently
    // skipping such bytes would silently DROP nulls (turning NULL into 0 and
    // making `crate::null_generation` misclassify a genuinely modern column
    // as legacy-consistent). Fail loudly, never guess.
    if pos != data.len() {
        return Err(DruidError::Segment(format!(
            "druid-native: {} trailing bytes after the `{}` part — this column uses a layout \
             extension (e.g. nullable-column metadata) that is not supported yet",
            data.len() - pos,
            part.part_type
        )));
    }
    Ok(column)
}

// ---------------------------------------------------------------------------
// Numeric parts (longV2 / doubleV2 / floatV2)
// ---------------------------------------------------------------------------

/// Read a numeric part and return the concatenated little-endian value bytes
/// (`num_values * width` bytes).
fn read_numeric_part(data: &[u8], pos: &mut usize, width: usize, what: &str) -> Result<Vec<u8>> {
    // Numeric parts carry their own `[u32 BE part_len]` prefix.
    let part_len = read_u32_be(data, pos, what)? as usize;
    need(data, *pos, part_len, what)?;
    let part_end = *pos + part_len;
    // Bound every following read to the declared part: a lying `part_len`
    // must not let the header or value blocks read bytes owned by whatever
    // follows the part, nor push `*pos` past `part_end` (which would
    // underflow the trailing-byte computation below).
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

    let raw = decode_value_blocks(
        part,
        pos,
        part_end,
        num_values,
        values_per_block,
        width,
        compression,
        what,
    )?;
    match part_end.checked_sub(*pos) {
        Some(0) => Ok(raw),
        Some(trailing) => Err(DruidError::Segment(format!(
            "druid-native: {what}: {trailing} trailing bytes inside part"
        ))),
        // Unreachable while every read above is bounded to `part`, but fail
        // closed rather than underflow if a future edit widens a read.
        None => Err(DruidError::Segment(format!(
            "druid-native: {what}: part reads overran the declared part end"
        ))),
    }
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
        // The observed `longEncoding: auto` marker byte (0x83) carries the
        // 0x80 bit; other values are simply unknown ids. Either way we
        // refuse loudly instead of guessing.
        if compression & 0x80 != 0 {
            return Err(DruidError::Segment(format!(
                "druid-native: {what}: encoded values (header byte {compression:#04x}) are not \
                 supported — segments written with `longEncoding: auto` cannot be read yet"
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

    // Query-time value lookups binary-search the dictionary, so an unsorted
    // dictionary (hostile or corrupt segment) would silently mis-filter.
    // Real Druid dictionaries are written sorted; enforce it.
    if let Some(w) = dict_values.windows(2).find(|w| w[0] > w[1]) {
        return Err(DruidError::Segment(format!(
            "druid-native: string dictionary is not sorted ({:?} > {:?})",
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
    fn rejects_longencoding_auto_honestly() {
        // Craft the minimal prefix of a longV2 part whose compression byte
        // carries the observed `longEncoding: auto` flag bit (0x83).
        let desc = br#"{"valueType":"LONG","parts":[{"type":"longV2"}]}"#;
        let mut blob = Vec::new();
        blob.extend_from_slice(&(desc.len() as u32).to_be_bytes());
        blob.extend_from_slice(desc);
        let part = [2u8, 0, 0, 0, 5, 0, 0, 0x20, 0, 0x83];
        blob.extend_from_slice(&(part.len() as u32).to_be_bytes());
        blob.extend_from_slice(&part);
        let err = decode_native_column(&blob).expect_err("auto encoding must be rejected");
        assert!(
            err.to_string().contains("longEncoding"),
            "expected honest auto-encoding rejection, got: {err}"
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
        // 268 435 456 declared longs (a 2 GiB value buffer) backed by an
        // EMPTY block container: the count must be rejected against the
        // blocks that actually exist BEFORE any buffer is sized from it.
        let mut body = vec![2u8];
        body.extend_from_slice(&268_435_456u32.to_be_bytes());
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
}
