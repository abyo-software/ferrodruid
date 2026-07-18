// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Writer for the **upstream Apache Druid** v9 on-disk segment layout —
//! the byte-level INVERSE of [`crate::druid_native`] (TG-1 Part 2,
//! reverse migration: a real Druid cluster reads a FerroDruid-written
//! segment).
//!
//! # Provenance (clean room)
//!
//! Every byte emitted here mirrors what [`crate::druid_native`] was
//! observed to PARSE from real Apache Druid 31.0.2 segments (verbatim
//! captures in `crates/ferrodruid-segment/testdata/druid31_*`, 2026-07-12).
//! No upstream source code was referenced.  The Druid-verified reader is
//! the in-process oracle: every writer component is validated by feeding
//! its output back through the reader and asserting an exact round-trip,
//! and the descriptor JSON / container flag bytes are additionally
//! byte-anchored against the captured fixtures (several writer outputs are
//! asserted byte-identical to real Druid 31.0.2 files).
//!
//! # Emitted layout (milestone 1)
//!
//! * Sidecar files next to `meta.smoosh` (Druid 30+ local deep-storage
//!   layout): `version.bin` = BE i32 `9`, `factory.json` =
//!   `{"type":"mMapSegmentFactory"}`.
//! * `index.drd` (inside the smoosh): generic-indexed column list (metrics
//!   first, then dimensions) + generic-indexed dimension list + BE i64
//!   interval start/end + `[u32 BE len]{"type":"roaring"}` + two all-null
//!   generic-indexed trailers (one entry per column / per dimension,
//!   flags `0x01` as observed).
//! * LONG / DOUBLE / FLOAT columns: `[u32 BE json_len][descriptor JSON]`
//!   then a `longV2`/`doubleV2`/`floatV2` part with **uncompressed blocks**
//!   (compression id `0xff`): `[u32 BE part_len][u8 2][u32 BE num_values]
//!   [u32 BE 8192][u8 0xff][generic-indexed blocks of raw LE values]`.
//!   Byte-anchored: the writer's output for the `wikipedia_compat` `added`
//!   metric is byte-identical to the captured
//!   `metricCompression: "uncompressed"` fixture.
//! * Single-value STRING columns: `[u32 BE json_len][descriptor JSON]`
//!   then a `stringDictionary` part in the **version-0 uncompressed**
//!   form: `[u8 0]` + sorted generic-indexed UTF-8 dictionary + the v0
//!   row-ordinal section (`[u8 0][u8 width][u32 BE total][BE width-byte
//!   ordinals][4-width zero pad]`) + a generic-indexed list of
//!   portable-Roaring value bitmaps (no per-bitmap type tag).
//!
//!   NOTE (byte-anchored deviation from the original plan): real Druid
//!   31.0.2 pairs the part **version byte** with the ordinal encoding —
//!   version `0x00` ⇒ v0 uncompressed ordinals and NO feature-flags word
//!   (`druid31_uncmp_page.col`), version `0x02` + `u32` feature flags ⇒
//!   compressed ordinals (`druid31_page.col`).  Since this writer emits v0
//!   ordinals, it emits part version `0x00`, exactly like the fixture — a
//!   `0x02` header in front of v0 ordinals would be a layout real Druid
//!   never writes.
//!
//! # Scope OUT (milestone 1 — fail loudly, never guess)
//!
//! LZ4 (`0x01`) block byte-fidelity, nullable numeric null sections,
//! `longEncoding: auto`, multi-value strings, null-bearing string columns,
//! complex/sketch columns, `metadata.drd`, front-coded string
//! dictionaries, >2 GB multi-chunk smoosh.

use std::collections::HashSet;
use std::path::Path;

use ferrodruid_common::error::{DruidError, Result};

use crate::column::{ColumnData, StringColumnData};
use crate::druid_native::{MAX_COLUMN_VALUES, MAX_GI_ELEMENTS};
use crate::segment::{Interval, SegmentData};
use crate::smoosh::MAX_SMOOSH_ENTRIES;
use crate::writer::SmooshWriter;

/// Segment control-file / smoosh-entry names that a column may NOT reuse: a
/// column named after any of these would collide with (and shadow or
/// corrupt) the segment's own bookkeeping.  `__time` is a legitimate real
/// Druid column and is deliberately NOT listed here (it is handled
/// separately by the writer).
const RESERVED_SEGMENT_FILENAMES: [&str; 5] = [
    "version.bin",
    "factory.json",
    "meta.smoosh",
    "metadata.drd",
    "index.drd",
];

// ---------------------------------------------------------------------------
// Byte-anchored constants (captured verbatim from Druid 31.0.2 output)
// ---------------------------------------------------------------------------

/// Embedded column-descriptor JSON of a LONG column, byte-identical to the
/// head of `testdata/druid31_added.col` / `druid31_time.col`.
const LONG_DESCRIPTOR_JSON: &str = r#"{"valueType":"LONG","hasMultipleValues":false,"parts":[{"type":"longV2","byteOrder":"LITTLE_ENDIAN","bitmapSerdeFactory":{"type":"roaring"}}]}"#;

/// Embedded column-descriptor JSON of a DOUBLE column, byte-identical to
/// the head of `testdata/druid31_added_d.col`.
const DOUBLE_DESCRIPTOR_JSON: &str = r#"{"valueType":"DOUBLE","hasMultipleValues":false,"parts":[{"type":"doubleV2","byteOrder":"LITTLE_ENDIAN","bitmapSerdeFactory":{"type":"roaring"}}]}"#;

/// Embedded column-descriptor JSON of a FLOAT column, byte-identical to
/// the head of `testdata/druid31_delta_f.col`.
const FLOAT_DESCRIPTOR_JSON: &str = r#"{"valueType":"FLOAT","hasMultipleValues":false,"parts":[{"type":"floatV2","byteOrder":"LITTLE_ENDIAN","bitmapSerdeFactory":{"type":"roaring"}}]}"#;

/// Embedded column-descriptor JSON of a single-value STRING column,
/// byte-identical to the head of `testdata/druid31_page.col` /
/// `druid31_uncmp_page.col` (note: `bitmapSerdeFactory` BEFORE
/// `byteOrder`, the opposite of the numeric descriptors).
const STRING_DESCRIPTOR_JSON: &str = r#"{"valueType":"STRING","hasMultipleValues":false,"parts":[{"type":"stringDictionary","bitmapSerdeFactory":{"type":"roaring"},"byteOrder":"LITTLE_ENDIAN"}]}"#;

/// The bitmap codec descriptor embedded in `index.drd`.
const BITMAP_CODEC_JSON: &str = r#"{"type":"roaring"}"#;

/// Minimal SegmentizerFactory sidecar (`factory.json`).
const FACTORY_JSON: &str = r#"{"type":"mMapSegmentFactory"}"#;

/// Generic-indexed container version byte.
const GI_VERSION: u8 = 0x01;
/// Generic-indexed flags byte of a sorted (reverse-lookup-capable) list.
const GI_FLAG_SORTED: u8 = 0x01;
/// Generic-indexed flags byte of an unsorted list.
const GI_FLAG_UNSORTED: u8 = 0x00;

/// Numeric part version byte (`longV2` / `doubleV2` / `floatV2`).
const NUMERIC_PART_VERSION: u8 = 0x02;
/// Values per compression block — observed `0x2000` on every captured
/// Druid 31.0.2 numeric part regardless of value width.
const VALUES_PER_BLOCK: usize = 8192;
/// Block-compression id: uncompressed blocks inside the block container
/// (`metricCompression: "uncompressed"`; the reader's `0xff` path).
const COMPRESSION_UNCOMPRESSED: u8 = 0xff;

/// `stringDictionary` part version byte of the uncompressed layout: v0
/// ordinals follow and NO feature-flags word is present (byte-anchored to
/// `druid31_uncmp_page.col`; see the module docs).
const STRING_PART_VERSION_UNCOMPRESSED: u8 = 0x00;
/// Row-ordinal section version byte of the v0 uncompressed form.
const ROW_ORDINALS_VERSION_UNCOMPRESSED: u8 = 0x00;
/// The v0 ordinal section right-pads its value stream to `4 - width`
/// bytes, included in the declared byte length.
const ROW_ORDINALS_PAD_TO: usize = 4;

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Encode `n` as a BE u32, failing loudly if it does not fit (a >4 GiB
/// section cannot be represented in this layout).
fn be_len(n: usize, what: &str) -> Result<[u8; 4]> {
    u32::try_from(n).map(u32::to_be_bytes).map_err(|_| {
        DruidError::Segment(format!(
            "druid-native writer: {what} length {n} exceeds the u32 frame limit"
        ))
    })
}

/// Compare two strings in **Java UTF-16 code-unit order** — the ordering
/// `GenericIndexed.STRING_STRATEGY` imposes on string dictionaries, and hence
/// the order real Druid's dictionary binary search assumes.  Lexicographically
/// compares the `u16` code-unit sequences produced by [`str::encode_utf16`].
///
/// For all-BMP text (which includes ASCII) this is identical to Rust's
/// codepoint (`str`) ordering, so ASCII/BMP dictionaries — and every captured
/// fixture — are byte-for-byte unaffected.  The two orders DIVERGE only for
/// supplementary (non-BMP, `U+10000`+) characters, which UTF-16 encodes as a
/// surrogate pair in `0xD800..=0xDFFF`: a surrogate lead unit sorts BEFORE a
/// Private-Use-Area scalar such as `U+E000`, the reverse of codepoint order.
pub(crate) fn utf16_cmp(a: &str, b: &str) -> core::cmp::Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

// ---------------------------------------------------------------------------
// Generic-indexed container writer (inverse of `parse_generic_indexed`)
// ---------------------------------------------------------------------------

/// Reject a generic-indexed container whose element count exceeds the reader's
/// hard ceiling ([`MAX_GI_ELEMENTS`], 2^24). [`crate::druid_native::parse_generic_indexed`]
/// refuses any container declaring more, so a larger one is a container the
/// reader — and Druid — can never reopen. Referencing the SAME const keeps the
/// writer's acceptance set a subset of the reader's; this is the single
/// choke-point that enforces the cap on EVERY generic-indexed container the
/// writer emits (string dictionary, value bitmaps, index.drd column/dimension
/// lists and trailers, numeric blocks).
fn ensure_gi_element_count(count: usize, what: &str) -> Result<()> {
    if count > MAX_GI_ELEMENTS {
        return Err(DruidError::Segment(format!(
            "druid-native writer: {what}: element count {count} exceeds the reader's \
             generic-indexed cap MAX_GI_ELEMENTS ({MAX_GI_ELEMENTS}) — refusing to write a \
             container the reader (and Druid) can never reopen"
        )));
    }
    Ok(())
}

/// Serialize one generic-indexed container:
/// `[u8 version=1][u8 flags][u32 BE body_size][u32 BE count]
/// [u32 BE end-offset ×count][values]`, each value
/// `[i32 BE marker: 0 present / -1 null][payload]`.
fn write_generic_indexed(elements: &[Option<&[u8]>], flags: u8, what: &str) -> Result<Vec<u8>> {
    // Every container the reader parses is bounded by MAX_GI_ELEMENTS; mirror
    // that here so no writer path can emit a container the reader rejects.
    ensure_gi_element_count(elements.len(), what)?;
    let count_be = be_len(elements.len(), what)?;
    let mut offsets: Vec<u8> = Vec::with_capacity(elements.len().saturating_mul(4));
    let mut values: Vec<u8> = Vec::new();
    for element in elements {
        match element {
            Some(payload) => {
                values.extend_from_slice(&0_i32.to_be_bytes());
                values.extend_from_slice(payload);
            }
            None => values.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
        offsets.extend_from_slice(&be_len(values.len(), what)?);
    }
    // `body_size` covers the count field, the offset table, and the value
    // region — exactly what the reader bounds its parse to.
    let body_size = 4_usize
        .checked_add(offsets.len())
        .and_then(|n| n.checked_add(values.len()))
        .ok_or_else(|| {
            DruidError::Segment(format!("druid-native writer: {what} body size overflows"))
        })?;
    let mut out = Vec::with_capacity(6 + body_size);
    out.push(GI_VERSION);
    out.push(flags);
    out.extend_from_slice(&be_len(body_size, what)?);
    out.extend_from_slice(&count_be);
    out.extend_from_slice(&offsets);
    out.extend_from_slice(&values);
    Ok(out)
}

/// The flags byte real Druid writes for a string list: `0x01` iff the list
/// is strictly ascending (sorted + unique ⇒ reverse lookup allowed) in **Java
/// UTF-16 code-unit order** — the order `GenericIndexed`'s reverse lookup
/// binary-searches.  Using Rust codepoint order here would set the
/// reverse-lookup flag on a list Druid considers unsorted (they diverge for
/// supplementary characters), making Druid's binary search invalid; route the
/// decision through [`utf16_cmp`] so the flag is only ever set when Druid's own
/// order agrees.
fn string_list_flags(items: &[&str]) -> u8 {
    if items
        .windows(2)
        .all(|w| utf16_cmp(w[0], w[1]) == core::cmp::Ordering::Less)
    {
        GI_FLAG_SORTED
    } else {
        GI_FLAG_UNSORTED
    }
}

/// Serialize a generic-indexed container of UTF-8 strings, computing the
/// sorted flag the way real Druid does.
fn write_generic_indexed_strings(items: &[&str], what: &str) -> Result<Vec<u8>> {
    let elements: Vec<Option<&[u8]>> = items.iter().map(|s| Some(s.as_bytes())).collect();
    write_generic_indexed(&elements, string_list_flags(items), what)
}

// ---------------------------------------------------------------------------
// index.drd (inverse of `parse_native_index_drd`)
// ---------------------------------------------------------------------------

/// Serialize an upstream-layout `index.drd` (see the module docs for the
/// exact section order).  Byte-anchored: for the `wikipedia_compat`
/// declaration this reproduces `testdata/druid31_index.drd` byte-for-byte.
fn encode_native_index_drd(
    metrics: &[String],
    dimensions: &[String],
    interval: &Interval,
) -> Result<Vec<u8>> {
    // Column list: metrics first, then dimensions (observed order).
    let all_columns: Vec<&str> = metrics
        .iter()
        .chain(dimensions.iter())
        .map(String::as_str)
        .collect();
    let dims: Vec<&str> = dimensions.iter().map(String::as_str).collect();

    let mut out = write_generic_indexed_strings(&all_columns, "index.drd column list")?;
    out.extend_from_slice(&write_generic_indexed_strings(
        &dims,
        "index.drd dimension list",
    )?);
    out.extend_from_slice(&interval.start_millis.to_be_bytes());
    out.extend_from_slice(&interval.end_millis.to_be_bytes());
    out.extend_from_slice(&be_len(BITMAP_CODEC_JSON.len(), "index.drd bitmap codec")?);
    out.extend_from_slice(BITMAP_CODEC_JSON.as_bytes());
    // Two all-null trailers: one entry per column, one per dimension.
    // Observed flags byte on both: 0x01 (druid31_index.drd).
    let column_nulls: Vec<Option<&[u8]>> = vec![None; all_columns.len()];
    out.extend_from_slice(&write_generic_indexed(
        &column_nulls,
        GI_FLAG_SORTED,
        "index.drd column trailer",
    )?);
    let dimension_nulls: Vec<Option<&[u8]>> = vec![None; dims.len()];
    out.extend_from_slice(&write_generic_indexed(
        &dimension_nulls,
        GI_FLAG_SORTED,
        "index.drd dimension trailer",
    )?);
    Ok(out)
}

// ---------------------------------------------------------------------------
// Numeric parts (longV2 / doubleV2 / floatV2, uncompressed blocks)
// ---------------------------------------------------------------------------

/// Serialize a v2 numeric part with uncompressed (`0xff`) blocks from the
/// concatenated little-endian value bytes: `[u32 BE part_len][u8 2]
/// [u32 BE num_values][u32 BE 8192][u8 0xff][generic-indexed blocks]`.
fn write_numeric_v2_part(raw_le: &[u8], width: usize, num_values: usize) -> Result<Vec<u8>> {
    let what = "numeric part";
    // Internal invariant of the callers below, checked rather than assumed.
    if raw_le.len() != num_values.saturating_mul(width) {
        return Err(DruidError::Segment(format!(
            "druid-native writer: {what}: {} raw bytes cannot back {num_values} \
             values of width {width}",
            raw_le.len()
        )));
    }
    let block_bytes = VALUES_PER_BLOCK * width;
    let blocks: Vec<Option<&[u8]>> = raw_le.chunks(block_bytes).map(Some).collect();
    let container = write_generic_indexed(&blocks, GI_FLAG_UNSORTED, what)?;

    let mut part = Vec::with_capacity(10 + container.len());
    part.push(NUMERIC_PART_VERSION);
    part.extend_from_slice(&be_len(num_values, what)?);
    part.extend_from_slice(&be_len(VALUES_PER_BLOCK, what)?);
    part.push(COMPRESSION_UNCOMPRESSED);
    part.extend_from_slice(&container);

    let mut out = Vec::with_capacity(4 + part.len());
    out.extend_from_slice(&be_len(part.len(), what)?);
    out.extend_from_slice(&part);
    Ok(out)
}

/// Frame a column blob: `[u32 BE json_len][descriptor JSON][part bytes]`.
fn assemble_column(descriptor_json: &str, part: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(4 + descriptor_json.len() + part.len());
    out.extend_from_slice(&be_len(descriptor_json.len(), "column descriptor JSON")?);
    out.extend_from_slice(descriptor_json.as_bytes());
    out.extend_from_slice(part);
    Ok(out)
}

/// Serialize a full LONG column blob (descriptor + `longV2` part).
fn write_long_column_native(values: &[i64]) -> Result<Vec<u8>> {
    let mut raw = Vec::with_capacity(values.len().saturating_mul(8));
    for v in values {
        raw.extend_from_slice(&v.to_le_bytes());
    }
    let part = write_numeric_v2_part(&raw, 8, values.len())?;
    assemble_column(LONG_DESCRIPTOR_JSON, &part)
}

/// Serialize a full DOUBLE column blob (descriptor + `doubleV2` part).
///
/// NaN is FerroDruid's in-memory SQL-NULL marker and the nullable numeric
/// null section is a milestone scope-out, so a NaN-bearing column is
/// refused loudly rather than silently turning NULL into a literal NaN on
/// the Druid side.
fn write_double_column_native(name: &str, values: &[f64]) -> Result<Vec<u8>> {
    if let Some(row) = values.iter().position(|v| v.is_nan()) {
        return Err(DruidError::Segment(format!(
            "druid-native writer: column `{name}` row {row} is NaN (FerroDruid's DOUBLE \
             NULL marker) — nullable numeric columns are not writable in the native v9 \
             layout yet (milestone scope-out)"
        )));
    }
    let mut raw = Vec::with_capacity(values.len().saturating_mul(8));
    for v in values {
        raw.extend_from_slice(&v.to_le_bytes());
    }
    let part = write_numeric_v2_part(&raw, 8, values.len())?;
    assemble_column(DOUBLE_DESCRIPTOR_JSON, &part)
}

/// Serialize a full FLOAT column blob (descriptor + `floatV2` part).
/// NaN rejection: same rationale as [`write_double_column_native`].
fn write_float_column_native(name: &str, values: &[f32]) -> Result<Vec<u8>> {
    if let Some(row) = values.iter().position(|v| v.is_nan()) {
        return Err(DruidError::Segment(format!(
            "druid-native writer: column `{name}` row {row} is NaN (FerroDruid's FLOAT \
             NULL marker) — nullable numeric columns are not writable in the native v9 \
             layout yet (milestone scope-out)"
        )));
    }
    let mut raw = Vec::with_capacity(values.len().saturating_mul(4));
    for v in values {
        raw.extend_from_slice(&v.to_le_bytes());
    }
    let part = write_numeric_v2_part(&raw, 4, values.len())?;
    assemble_column(FLOAT_DESCRIPTOR_JSON, &part)
}

// ---------------------------------------------------------------------------
// stringDictionary part (inverse of `decode_string_dictionary_part`)
// ---------------------------------------------------------------------------

/// Serialize the v0 uncompressed row-ordinal section: `[u8 0][u8 width]
/// [u32 BE total_bytes][BE width-byte ordinals][4-width zero pad]`.
fn encode_v0_row_ordinals(ordinals: &[u32], dict_len: usize, name: &str) -> Result<Vec<u8>> {
    let max_ordinal = u32::try_from(dict_len.saturating_sub(1)).map_err(|_| {
        DruidError::Segment(format!(
            "druid-native writer: column `{name}` dictionary cardinality {dict_len} \
             exceeds the u32 ordinal space"
        ))
    })?;
    let width: usize = match max_ordinal {
        0..=0xFF => 1,
        0x100..=0xFFFF => 2,
        0x1_0000..=0xFF_FFFF => 3,
        _ => 4,
    };
    let pad = ROW_ORDINALS_PAD_TO - width;
    let total_bytes = ordinals
        .len()
        .checked_mul(width)
        .and_then(|n| n.checked_add(pad))
        .ok_or_else(|| {
            DruidError::Segment(format!(
                "druid-native writer: column `{name}` ordinal byte count overflows"
            ))
        })?;

    let mut out = Vec::with_capacity(6 + total_bytes);
    out.push(ROW_ORDINALS_VERSION_UNCOMPRESSED);
    out.push(u8::try_from(width).unwrap_or(ROW_ORDINALS_PAD_TO as u8));
    out.extend_from_slice(&be_len(total_bytes, "row ordinal section")?);
    for (row, &ordinal) in ordinals.iter().enumerate() {
        if ordinal as usize >= dict_len {
            return Err(DruidError::Segment(format!(
                "druid-native writer: column `{name}` row {row} ordinal {ordinal} is out \
                 of dictionary range {dict_len} — refusing to write an unreadable column"
            )));
        }
        out.extend_from_slice(&ordinal.to_be_bytes()[ROW_ORDINALS_PAD_TO - width..]);
    }
    out.extend_from_slice(&[0u8; ROW_ORDINALS_PAD_TO][..pad]);
    Ok(out)
}

/// Serialize a full single-value STRING column blob: descriptor + part
/// version `0x00` + sorted generic-indexed dictionary + v0 ordinals + a
/// generic-indexed list of portable-Roaring value bitmaps.
fn write_string_column_native(name: &str, col: &StringColumnData) -> Result<Vec<u8>> {
    if col.null_rows().is_some() {
        return Err(DruidError::Segment(format!(
            "druid-native writer: column `{name}` carries SQL-NULL rows — null-bearing \
             string dimensions are not writable in the native v9 layout yet (milestone \
             scope-out; the upstream layout stores nulls as a leading dictionary entry)"
        )));
    }
    let dict_len = col.dictionary.len();
    // The dictionary is written as a generic-indexed container, and the reader
    // (`decode_string_dictionary_part` → `parse_string_list`) bounds it by
    // MAX_GI_ELEMENTS. A column may legitimately carry up to MAX_COLUMN_VALUES
    // (2^26) distinct values — well past MAX_GI_ELEMENTS (2^24) — so a dictionary
    // over the cap would pass the segment row-count guard yet emit a dictionary
    // the reader can never reopen. Refuse it loudly against the SAME const,
    // naming the column, before any dictionary bytes are built.
    ensure_gi_element_count(dict_len, &format!("column `{name}` string dictionary"))?;

    // FerroDruid's in-memory dictionary is ordered by Rust codepoint order,
    // but a Druid-canonical `stringDictionary` is a SORTED GenericIndexed in
    // Java UTF-16 code-unit order, and real Druid's selectors binary-search
    // that order.  The two orders coincide for all-BMP/ASCII text but DIVERGE
    // for supplementary characters, so re-sort the dictionary by UTF-16 order
    // and remap the per-row ordinals accordingly (the value bitmaps below are
    // DERIVED from the remapped ordinals, so they follow automatically).  For
    // an all-BMP dictionary this is the identity permutation, leaving the ASCII
    // byte-identity fixtures byte-for-byte unchanged.
    let rust_ordered: Vec<&str> = col.dictionary.iter().map(|(_, s)| s).collect();
    let mut order: Vec<usize> = (0..rust_ordered.len()).collect();
    order.sort_by(|&a, &b| utf16_cmp(rust_ordered[a], rust_ordered[b]));
    let entries: Vec<&str> = order.iter().map(|&i| rust_ordered[i]).collect();

    // A Druid-canonical dictionary is strictly ascending (unique) in UTF-16
    // order; a duplicate entry would make both the ordinal remap and Druid's
    // binary search ambiguous, so refuse it loudly.
    if let Some(w) = entries
        .windows(2)
        .find(|w| utf16_cmp(w[0], w[1]) != core::cmp::Ordering::Less)
    {
        return Err(DruidError::Segment(format!(
            "druid-native writer: column `{name}` dictionary has a duplicate entry \
             ({:?}, {:?}) — refusing to write a layout that breaks binary-search lookups",
            w[0], w[1]
        )));
    }

    // Remap old (Rust-order) ordinals to new (UTF-16-order) ordinals:
    // `order[new] == old`, so its inverse maps each stored ordinal forward.
    let mut remap = vec![0u32; order.len()];
    for (new_ord, &old_ord) in order.iter().enumerate() {
        remap[old_ord] = u32::try_from(new_ord).map_err(|_| {
            DruidError::Segment(format!(
                "druid-native writer: column `{name}` dictionary ordinal {new_ord} exceeds \
                 the u32 ordinal space"
            ))
        })?;
    }
    let mut remapped_ordinals: Vec<u32> = Vec::with_capacity(col.encoded_values.len());
    for (row, &ord) in col.encoded_values.iter().enumerate() {
        let new_ord = *remap.get(ord as usize).ok_or_else(|| {
            DruidError::Segment(format!(
                "druid-native writer: column `{name}` row {row} ordinal {ord} is out of \
                 dictionary range {dict_len} — refusing to write an unreadable column"
            ))
        })?;
        remapped_ordinals.push(new_ord);
    }

    let mut part = vec![STRING_PART_VERSION_UNCOMPRESSED];
    part.extend_from_slice(&write_generic_indexed_strings(
        &entries,
        "string dictionary",
    )?);
    part.extend_from_slice(&encode_v0_row_ordinals(&remapped_ordinals, dict_len, name)?);

    // DERIVE the value bitmaps from the REMAPPED (UTF-16-order) ordinals —
    // never trust the column's stored `bitmap_indexes`, which may disagree
    // with the ordinals (wrong rows, or an out-of-range row that makes the
    // segment unreadable) and are in the pre-remap ordinal space anyway.  For
    // dictionary entry `i` the value bitmap is exactly the set of rows whose
    // remapped ordinal equals `i`; correct by construction.  The ordinals were
    // already range-checked by `encode_v0_row_ordinals` above, but the lookup
    // below is guarded so a bad ordinal can never index out of bounds.
    let mut value_bitmaps: Vec<ferrodruid_bitmap::DruidBitmap> =
        vec![ferrodruid_bitmap::DruidBitmap::new(); dict_len];
    for (row, &ordinal) in remapped_ordinals.iter().enumerate() {
        let Some(bitmap) = value_bitmaps.get_mut(ordinal as usize) else {
            return Err(DruidError::Segment(format!(
                "druid-native writer: column `{name}` row {row} ordinal {ordinal} is out \
                 of dictionary range {dict_len} — refusing to write an unreadable column"
            )));
        };
        let row_u32 = u32::try_from(row).map_err(|_| {
            DruidError::Segment(format!(
                "druid-native writer: column `{name}` has more than u32::MAX rows"
            ))
        })?;
        bitmap.insert(row_u32);
    }

    let mut bitmap_blobs: Vec<Vec<u8>> = Vec::with_capacity(dict_len);
    for bitmap in &value_bitmaps {
        bitmap_blobs.push(bitmap.serialize_portable()?);
    }
    let bitmap_elements: Vec<Option<&[u8]>> =
        bitmap_blobs.iter().map(|b| Some(b.as_slice())).collect();
    part.extend_from_slice(&write_generic_indexed(
        &bitmap_elements,
        GI_FLAG_UNSORTED,
        "string value bitmaps",
    )?);

    assemble_column(STRING_DESCRIPTOR_JSON, &part)
}

// ---------------------------------------------------------------------------
// Column dispatch + entry point
// ---------------------------------------------------------------------------

/// Serialize one column into its native blob, failing loudly on every
/// column family outside the milestone scope.
fn write_native_column(name: &str, col: &ColumnData) -> Result<Vec<u8>> {
    match col {
        ColumnData::Long(values) => write_long_column_native(values),
        ColumnData::Double(values) => write_double_column_native(name, values),
        ColumnData::Float(values) => write_float_column_native(name, values),
        ColumnData::String(string_col) => write_string_column_native(name, string_col),
        ColumnData::LongNullable(_, _) => Err(DruidError::Segment(format!(
            "druid-native writer: column `{name}` is a nullable LONG — the nullable \
             numeric null section is not writable in the native v9 layout yet \
             (milestone scope-out)"
        ))),
        ColumnData::StringMulti(_) => Err(DruidError::Segment(format!(
            "druid-native writer: column `{name}` is a multi-value string dimension — \
             not writable in the native v9 layout yet (milestone scope-out)"
        ))),
        ColumnData::Complex(_) | ColumnData::ComplexTheta(_) => Err(DruidError::Segment(format!(
            "druid-native writer: column `{name}` is a complex/sketch column — not \
             writable in the native v9 layout yet (milestone scope-out)"
        ))),
    }
}

/// Validate the declared column names against what the native layout (and
/// its reader) can represent.
fn validate_column_names(segment: &SegmentData) -> Result<()> {
    let mut seen: HashSet<&str> = HashSet::new();
    for name in segment.metrics.iter().chain(segment.dimensions.iter()) {
        if name.is_empty() {
            return Err(DruidError::Segment(
                "druid-native writer: empty column name is not representable".to_string(),
            ));
        }
        if name == "__time" {
            return Err(DruidError::Segment(
                "druid-native writer: `__time` must not be declared as a dimension or \
                 metric (it is implicit in the v9 layout)"
                    .to_string(),
            ));
        }
        // A column whose name equals one of the segment's own control files
        // (or smoosh entries) would collide with that reserved entry,
        // producing a segment nothing can open (or one that shadows its own
        // bookkeeping).  `__time` is intentionally excluded above.
        if RESERVED_SEGMENT_FILENAMES.contains(&name.as_str()) {
            return Err(DruidError::Segment(format!(
                "druid-native writer: column name `{name}` is a reserved segment control \
                 filename — refusing to write a column that collides with the segment's \
                 own bookkeeping"
            )));
        }
        // meta.smoosh is a comma/newline-delimited text index, so these
        // characters cannot appear in an entry name; `/` would nest paths.
        if name.contains(',') || name.contains('\n') || name.contains('\r') || name.contains('/') {
            return Err(DruidError::Segment(format!(
                "druid-native writer: column name {name:?} contains a character that the \
                 smoosh meta index cannot represent"
            )));
        }
        if !seen.insert(name.as_str()) {
            return Err(DruidError::Segment(format!(
                "druid-native writer: duplicate column name `{name}` — the native \
                 index.drd reconstructs metrics as (columns − dimensions), which a \
                 duplicate would corrupt"
            )));
        }
    }
    Ok(())
}

/// Build the smoosh writer holding the full native segment.
fn build_native_smoosh_writer(segment: &SegmentData) -> Result<SmooshWriter> {
    if segment.version != 9 {
        return Err(DruidError::Segment(format!(
            "druid-native writer: segment version {} is not 9 — only the v9 layout is \
             emitted",
            segment.version
        )));
    }
    validate_column_names(segment)?;

    // The reader (`SegmentData::open`) rejects any column — and the segment
    // row count itself — whose value count exceeds `MAX_COLUMN_VALUES`
    // (2^26). Every column carries `num_rows` values, so a segment with more
    // rows than the cap is one this reader (and Druid) can never reopen.
    // Writing it would "succeed" into an unopenable segment, so fail loud
    // naming the SAME cap the reader enforces — huge segments are a milestone
    // scope-out, not silently corruptible output.
    if segment.num_rows > MAX_COLUMN_VALUES {
        return Err(DruidError::Segment(format!(
            "druid-native writer: segment declares {} rows, exceeding the reader's \
             per-column cap MAX_COLUMN_VALUES ({MAX_COLUMN_VALUES}) — segments this large \
             are out of milestone scope; refusing to write a segment nothing can reopen",
            segment.num_rows
        )));
    }

    // Every key in the public `columns` map must be accounted for: `__time`,
    // a declared dimension, or a declared metric.  An undeclared entry would
    // be silently dropped by the (dimensions ∪ metrics) emit loop below,
    // losing data while reporting success.
    let mut declared: HashSet<&str> = HashSet::new();
    declared.insert("__time");
    declared.extend(segment.dimensions.iter().map(String::as_str));
    declared.extend(segment.metrics.iter().map(String::as_str));
    for key in segment.columns.keys() {
        if !declared.contains(key.as_str()) {
            return Err(DruidError::Segment(format!(
                "druid-native writer: column `{key}` is present in the segment's columns \
                 map but is neither `__time` nor a declared dimension nor a declared \
                 metric — refusing to silently drop it"
            )));
        }
    }

    // The reader (`SegmentData::open` → smoosh meta parse) rejects any
    // archive declaring more than `MAX_SMOOSH_ENTRIES` logical files, so a
    // segment that would exceed the cap is unopenable — fail loud (naming the
    // limit) instead of "succeeding" with garbage.  Smoosh entries emitted:
    // `index.drd` + `__time` + one per (dimension ∪ metric).
    let smoosh_entries = 2usize
        .checked_add(segment.dimensions.len())
        .and_then(|n| n.checked_add(segment.metrics.len()))
        .ok_or_else(|| {
            DruidError::Segment(
                "druid-native writer: smoosh entry count overflows usize".to_string(),
            )
        })?;
    if smoosh_entries > MAX_SMOOSH_ENTRIES {
        return Err(DruidError::Segment(format!(
            "druid-native writer: segment would emit {smoosh_entries} smoosh entries, \
             exceeding the reader's limit of {MAX_SMOOSH_ENTRIES} — refusing to write a \
             segment nothing can open"
        )));
    }

    let Some(time_col) = segment.columns.get("__time") else {
        return Err(DruidError::Segment(
            "druid-native writer: required column `__time` is missing".to_string(),
        ));
    };
    let ColumnData::Long(times) = time_col else {
        return Err(DruidError::Segment(
            "druid-native writer: `__time` must be a plain LONG column".to_string(),
        ));
    };
    if times.len() != segment.num_rows {
        return Err(DruidError::Segment(format!(
            "druid-native writer: `__time` has {} rows but the segment declares {}",
            times.len(),
            segment.num_rows
        )));
    }

    let mut writer = SmooshWriter::new();
    writer.add_file(
        "index.drd".to_string(),
        encode_native_index_drd(&segment.metrics, &segment.dimensions, &segment.interval)?,
    );
    writer.add_file("__time".to_string(), write_long_column_native(times)?);

    for name in segment.dimensions.iter().chain(segment.metrics.iter()) {
        let Some(col) = segment.columns.get(name) else {
            return Err(DruidError::Segment(format!(
                "druid-native writer: column `{name}` is declared in index.drd but has \
                 no data — the strict reader would reject the segment"
            )));
        };
        if col.num_rows() != Some(segment.num_rows) {
            return Err(DruidError::Segment(format!(
                "druid-native writer: column `{name}` has {:?} rows but the segment \
                 declares {}",
                col.num_rows(),
                segment.num_rows
            )));
        }
        writer.add_file(name.clone(), write_native_column(name, col)?);
    }

    // Sidecars (NOT smoosh entries): the Druid 30+ local deep-storage
    // layout keeps these as plain siblings of meta.smoosh.
    writer.add_sidecar_file("version.bin".to_string(), 9_i32.to_be_bytes().to_vec());
    writer.add_sidecar_file("factory.json".to_string(), FACTORY_JSON.as_bytes().to_vec());
    Ok(writer)
}

/// Write a [`SegmentData`] to `dir` in the **upstream Apache Druid v9
/// on-disk layout** (TG-1 Part 2 milestone 1: LONG / DOUBLE / FLOAT
/// metrics + single-value STRING dimensions, no nulls), so a real Druid
/// cluster can load the directory as a segment.
///
/// This is separate from [`crate::writer::write_segment_v9`], which keeps
/// FerroDruid's private layout for its own storage.  The emitted directory
/// contains `meta.smoosh` + `00000.smoosh` plus the `version.bin` /
/// `factory.json` sidecar files, written with the same crash-safety
/// discipline as the private writer (staged sibling dir + atomic rename).
///
/// Every emitted structure is validated in-tree by feeding it back through
/// the Druid-verified reader ([`crate::druid_native`]) and asserting an
/// exact round-trip; several outputs are additionally byte-identical to
/// captured Druid 31.0.2 files (see the module tests).
///
/// # Errors
///
/// Fails loudly on anything outside the milestone scope (nullable columns,
/// NaN numeric values, multi-value strings, complex columns), on
/// inconsistent input (row-count/dictionary/bitmap mismatches, missing
/// declared columns), and on any I/O failure.
pub fn write_segment_v9_native(segment: &SegmentData, dir: &Path) -> Result<()> {
    let writer = build_native_smoosh_writer(segment)?;
    writer.write_to_dir(dir)
}

// ---------------------------------------------------------------------------
// Tests — the Druid-verified reader is the oracle; several assertions are
// additionally byte-anchored against verbatim Druid 31.0.2 captures.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnData;
    use crate::druid_native::{
        decode_native_column, is_native_column, parse_generic_indexed, parse_native_index_drd,
    };
    use crate::segment::SegmentDataBuilder;
    use crate::smoosh::SmooshReader;
    use crate::v9::read_segment_v9;
    use ferrodruid_bitmap::DruidBitmap;

    /// Verbatim byte captures from real Druid 31.0.2 segments (same files
    /// the reader's own tests decode).
    const REAL_INDEX_DRD: &[u8] = include_bytes!("../testdata/druid31_index.drd");
    const REAL_MUNCMP_ADDED_COL: &[u8] = include_bytes!("../testdata/druid31_muncmp_added.col");
    const REAL_UNCMP_PAGE_COL: &[u8] = include_bytes!("../testdata/druid31_uncmp_page.col");

    // -- generic-indexed round-trip ----------------------------------------

    #[test]
    fn generic_indexed_writer_round_trips_through_reader() {
        // Unsorted strings, sorted strings, raw bytes, and null markers.
        let cases: Vec<(Vec<Option<&[u8]>>, u8)> = vec![
            (
                vec![Some(b"beta".as_slice()), Some(b"alpha".as_slice())],
                GI_FLAG_UNSORTED,
            ),
            (
                vec![Some(b"alpha".as_slice()), Some(b"beta".as_slice())],
                GI_FLAG_SORTED,
            ),
            (
                vec![Some(b"\x00\xff\x10".as_slice()), Some(b"".as_slice())],
                0,
            ),
            (vec![None, Some(b"x".as_slice()), None], 0),
            (vec![], 0),
        ];
        for (elements, flags) in cases {
            let bytes = write_generic_indexed(&elements, flags, "test").expect("write");
            assert_eq!(bytes[0], GI_VERSION);
            assert_eq!(bytes[1], flags, "flags byte must be emitted verbatim");
            let mut pos = 0usize;
            let parsed = parse_generic_indexed(&bytes, &mut pos, 1024, "test").expect("parse");
            assert_eq!(parsed, elements, "reader must reproduce the element list");
            assert_eq!(pos, bytes.len(), "container must consume itself exactly");
        }
    }

    #[test]
    fn generic_indexed_string_writer_computes_sorted_flag() {
        let sorted = write_generic_indexed_strings(&["a", "b", "c"], "t").expect("write");
        assert_eq!(sorted[1], GI_FLAG_SORTED);
        let unsorted = write_generic_indexed_strings(&["b", "a"], "t").expect("write");
        assert_eq!(unsorted[1], GI_FLAG_UNSORTED);
        // Duplicates break reverse lookup, so they are NOT sorted-unique.
        let dup = write_generic_indexed_strings(&["a", "a"], "t").expect("write");
        assert_eq!(dup[1], GI_FLAG_UNSORTED);
    }

    // -- longV2 ------------------------------------------------------------

    #[test]
    fn long_v2_writer_round_trips_exact_values() {
        let cases: Vec<Vec<i64>> = vec![
            vec![],
            vec![0],
            vec![100, 50, 200, 150, 75],
            vec![i64::MIN, -1, 0, 1, i64::MAX, -9_007_199_254_740_993],
            // Multi-block: 3 full 8192-value blocks minus a bit + tail.
            (0..20_000).map(|i| i * 37 - 500_000).collect(),
        ];
        for values in cases {
            let blob = write_long_column_native(&values).expect("write");
            assert!(is_native_column(&blob), "must look like a native column");
            match decode_native_column(&blob).expect("reader must decode our bytes") {
                ColumnData::Long(decoded) => {
                    assert_eq!(decoded, values, "exact i64 round-trip");
                }
                other => panic!("expected Long, got {other:?}"),
            }
        }
    }

    /// The strongest possible oracle: for the exact values of the captured
    /// `metricCompression: "uncompressed"` fixture, the writer's output is
    /// **byte-identical** to what real Druid 31.0.2 wrote.
    #[test]
    fn long_v2_writer_bytes_identical_to_real_druid31_fixture() {
        let blob = write_long_column_native(&[100, 50, 200, 150, 75]).expect("write");
        assert_eq!(
            blob, REAL_MUNCMP_ADDED_COL,
            "writer output must be byte-identical to the real Druid 31.0.2 column file"
        );
    }

    // -- doubleV2 / floatV2 -------------------------------------------------

    #[test]
    fn double_and_float_writers_round_trip() {
        let doubles = vec![100.0, -0.5, f64::MAX, f64::MIN_POSITIVE, 1e-300];
        let blob = write_double_column_native("d", &doubles).expect("write");
        match decode_native_column(&blob).expect("decode") {
            ColumnData::Double(decoded) => assert_eq!(decoded, doubles),
            other => panic!("expected Double, got {other:?}"),
        }
        let floats = vec![90.0_f32, -45.5, f32::MAX, f32::MIN_POSITIVE];
        let blob = write_float_column_native("f", &floats).expect("write");
        match decode_native_column(&blob).expect("decode") {
            ColumnData::Float(decoded) => assert_eq!(decoded, floats),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn nan_numeric_values_are_rejected_loudly() {
        let err = write_double_column_native("ratio", &[1.0, f64::NAN])
            .expect_err("NaN double must be refused");
        assert!(err.to_string().contains("NaN"), "got: {err}");
        let err = write_float_column_native("ratio_f", &[f32::NAN])
            .expect_err("NaN float must be refused");
        assert!(err.to_string().contains("NaN"), "got: {err}");
    }

    // -- stringDictionary ---------------------------------------------------

    /// Build the `wikipedia_compat` page column exactly as the fixtures
    /// hold it: dictionary [Accueil, Hauptseite, Main_Page,
    /// Talk:Main_Page], rows [2, 3, 0, 1, 2].
    fn page_column() -> StringColumnData {
        let rows = [
            "Main_Page",
            "Talk:Main_Page",
            "Accueil",
            "Hauptseite",
            "Main_Page",
        ];
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![0, 1, 2, 3, 4])
            .add_string_column("page", rows.iter().map(|s| (*s).to_string()).collect())
            .build()
            .expect("build");
        match segment.columns.get("page") {
            Some(ColumnData::String(s)) => s.clone(),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn string_column_writer_round_trips_through_reader() {
        let col = page_column();
        let blob = write_string_column_native("page", &col).expect("write");
        assert!(is_native_column(&blob));
        match decode_native_column(&blob).expect("reader must decode our bytes") {
            ColumnData::String(decoded) => {
                assert_eq!(
                    decoded.dictionary.iter().collect::<Vec<_>>(),
                    col.dictionary.iter().collect::<Vec<_>>(),
                    "dictionary"
                );
                assert_eq!(decoded.encoded_values, col.encoded_values, "ordinals");
                assert_eq!(decoded.bitmap_indexes, col.bitmap_indexes, "value bitmaps");
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    /// 400-entry dictionary forces 2-byte big-endian v0 ordinals (the
    /// same shape as the `druid31_hicard_u_page.col` capture).
    #[test]
    fn string_column_writer_two_byte_ordinals_round_trip() {
        let rows: Vec<String> = (0..400).map(|i| format!("page_{i:03}")).collect();
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column((0..400).collect())
            .add_string_column("page", rows)
            .build()
            .expect("build");
        let Some(ColumnData::String(col)) = segment.columns.get("page") else {
            panic!("expected String");
        };
        let blob = write_string_column_native("page", col).expect("write");
        match decode_native_column(&blob).expect("decode") {
            ColumnData::String(decoded) => {
                assert_eq!(decoded.dictionary.len(), 400);
                assert_eq!(decoded.encoded_values, col.encoded_values);
                assert_eq!(decoded.bitmap_indexes, col.bitmap_indexes);
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    /// Byte-anchor against the real Druid 31.0.2 uncompressed page column:
    /// descriptor + part version + dictionary + v0 ordinals must be
    /// byte-identical.  (The value-bitmap payloads differ legitimately:
    /// Druid's Java Roaring runs run-optimization, the Rust `roaring`
    /// crate serializes array containers — both are spec-valid portable
    /// forms of the SAME sets, asserted semantically below.)
    #[test]
    fn string_column_writer_matches_real_druid31_fixture() {
        let col = page_column();
        let blob = write_string_column_native("page", &col).expect("write");

        // Compute the byte length of everything before the bitmap list.
        let entries: Vec<&str> = col.dictionary.iter().map(|(_, s)| s).collect();
        let dict_gi = write_generic_indexed_strings(&entries, "t").expect("gi");
        let ordinals =
            encode_v0_row_ordinals(&col.encoded_values, col.dictionary.len(), "page").expect("v0");
        let prefix_len = 4 + STRING_DESCRIPTOR_JSON.len() + 1 + dict_gi.len() + ordinals.len();
        assert_eq!(
            &blob[..prefix_len],
            &REAL_UNCMP_PAGE_COL[..prefix_len],
            "descriptor + version + dictionary + ordinals must be byte-identical to the \
             real Druid 31.0.2 column file"
        );

        // Semantic equality of the WHOLE file vs the real capture.
        let ours = decode_native_column(&blob).expect("decode ours");
        let real = decode_native_column(REAL_UNCMP_PAGE_COL).expect("decode real");
        match (ours, real) {
            (ColumnData::String(a), ColumnData::String(b)) => {
                assert_eq!(
                    a.dictionary.iter().collect::<Vec<_>>(),
                    b.dictionary.iter().collect::<Vec<_>>()
                );
                assert_eq!(a.encoded_values, b.encoded_values);
                assert_eq!(a.bitmap_indexes, b.bitmap_indexes);
            }
            other => panic!("expected two String columns, got {other:?}"),
        }
    }

    #[test]
    fn null_bearing_string_column_is_rejected_loudly() {
        let col = StringColumnData::from_nullable_values(&[
            Some("a".to_string()),
            None,
            Some("b".to_string()),
        ]);
        let err = write_string_column_native("tag", &col).expect_err("null rows must be refused");
        assert!(err.to_string().contains("SQL-NULL"), "got: {err}");
    }

    // -- TG-1 R4 (HIGH): UTF-16 dictionary ordering -------------------------

    #[test]
    fn utf16_cmp_orders_supplementary_before_pua() {
        use core::cmp::Ordering;
        // A supplementary char (U+10000, UTF-16 surrogate pair leading with
        // 0xD800) sorts BEFORE the Private-Use-Area scalar U+E000 in Java
        // UTF-16 order — the OPPOSITE of Rust codepoint order.
        assert_eq!(utf16_cmp("\u{10000}", "\u{E000}"), Ordering::Less);
        assert_eq!(utf16_cmp("\u{E000}", "\u{10000}"), Ordering::Greater);
        assert!(
            "\u{E000}" < "\u{10000}",
            "Rust codepoint order is the reverse of UTF-16 order for this pair"
        );
        // For ASCII / all-BMP text, UTF-16 order is identical to Rust order.
        for (a, b) in [
            ("a", "b"),
            ("abc", "abd"),
            ("", "x"),
            ("Main_Page", "Talk:Main_Page"),
            ("de", "en"),
            ("café", "cafz"), // é is BMP (U+00E9) — still equals Rust order
        ] {
            assert_eq!(
                utf16_cmp(a, b),
                a.cmp(b),
                "all-BMP order must match Rust order: {a:?} vs {b:?}"
            );
        }
        assert_eq!(utf16_cmp("同じ", "同じ"), Ordering::Equal);
    }

    /// TG-1 R4 (HIGH): the emitted dictionary must be ordered by Java UTF-16
    /// code units — the order real Druid binary-searches — NOT Rust codepoint
    /// order.  They diverge for supplementary characters: `U+10000` sorts
    /// BEFORE the PUA scalar `U+E000` in UTF-16, but `U+E000 < U+10000` in Rust
    /// `str` order.  The writer must re-sort the dictionary and remap ordinals
    /// so a real-Druid selector for `U+10000` lands on the right ordinal.
    #[test]
    fn native_writer_dictionary_uses_utf16_order() {
        // Rows over a dictionary whose Rust order (A, U+E000, U+10000) differs
        // from its UTF-16 order (A, U+10000, U+E000).
        let rows = ["\u{10000}", "A", "\u{E000}", "\u{10000}"];
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![0, 1, 2, 3])
            .add_string_column("s", rows.iter().map(|s| (*s).to_string()).collect())
            .build()
            .expect("build");
        let Some(ColumnData::String(col)) = segment.columns.get("s") else {
            panic!("expected String column");
        };
        let blob = write_string_column_native("s", col).expect("write");

        match decode_native_column(&blob).expect("reader must decode our bytes") {
            ColumnData::String(decoded) => {
                // The emitted dictionary is in UTF-16 order: U+10000 BEFORE
                // U+E000 (the reverse of the Rust-sorted in-memory dictionary).
                let dict: Vec<&str> = decoded.dictionary.iter().map(|(_, s)| s).collect();
                assert_eq!(
                    dict,
                    vec!["A", "\u{10000}", "\u{E000}"],
                    "dictionary must be in Java UTF-16 order (supplementary before PUA)"
                );

                // Every row's value survives the ordinal remap.
                let reconstructed: Vec<&str> = decoded
                    .encoded_values
                    .iter()
                    .map(|&o| {
                        decoded
                            .dictionary
                            .get(o as usize)
                            .expect("ordinal in range")
                    })
                    .collect();
                assert_eq!(reconstructed, rows, "each row's value must round-trip");

                // A selector/equality on U+10000 resolves to its UTF-16 ordinal
                // (1), and that entry's value bitmap is exactly the rows holding
                // it — proof the ordinals AND bitmaps were remapped together.
                let ord_10000 = dict
                    .iter()
                    .position(|&s| s == "\u{10000}")
                    .expect("U+10000 present in dictionary");
                assert_eq!(ord_10000, 1, "U+10000 is the second UTF-16-ordered entry");
                assert_eq!(
                    decoded.bitmap_indexes[ord_10000]
                        .iter()
                        .collect::<Vec<u32>>(),
                    vec![0, 3],
                    "selector on U+10000 must return exactly rows 0 and 3"
                );
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    /// The reader's `parse_generic_indexed` refuses any container with more
    /// than `MAX_GI_ELEMENTS` (2^24) entries, so the writer's count guard must
    /// too. A string column may legitimately carry up to `MAX_COLUMN_VALUES`
    /// (2^26) distinct dictionary values — past the GI cap — so this is a real,
    /// reachable gap. Tested structurally on the count guard (NOT by building
    /// 16M+ distinct strings), which the writer routes both the dictionary and
    /// every other container through.
    #[test]
    fn generic_indexed_element_count_over_cap_is_rejected() {
        // Exactly at the cap is accepted (the reader uses `num > cap`).
        ensure_gi_element_count(MAX_GI_ELEMENTS, "string dictionary")
            .expect("a container exactly at the cap must be accepted");

        // One element over the cap is refused loudly, naming the const AND the
        // value, exactly as the reader would reject the re-parse.
        let err = ensure_gi_element_count(MAX_GI_ELEMENTS + 1, "column `page` string dictionary")
            .expect_err("an over-cap element count must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("MAX_GI_ELEMENTS"),
            "the error must reference the cap by name, got: {msg}"
        );
        assert!(
            msg.contains(&MAX_GI_ELEMENTS.to_string()),
            "the error must name the cap value, got: {msg}"
        );

        // The cap value must equal the reader's const (same-source invariant):
        // if the reader's ceiling ever moved, this would catch a stale copy.
        assert_eq!(MAX_GI_ELEMENTS, crate::druid_native::MAX_GI_ELEMENTS);
    }

    // -- index.drd ----------------------------------------------------------

    /// Byte-anchor: for the exact `wikipedia_compat` declaration, the
    /// writer reproduces the real Druid 31.0.2 `index.drd` byte-for-byte.
    #[test]
    fn index_drd_writer_bytes_identical_to_real_druid31_fixture() {
        let metrics: Vec<String> = ["added", "count", "deleted", "delta"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let dimensions: Vec<String> = ["page", "user", "language", "city", "namespace", "channel"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let interval = Interval {
            start_millis: 1_704_067_200_000,
            end_millis: 1_704_153_600_000,
        };
        let bytes = encode_native_index_drd(&metrics, &dimensions, &interval).expect("write");
        assert_eq!(
            bytes, REAL_INDEX_DRD,
            "writer output must be byte-identical to the real Druid 31.0.2 index.drd"
        );
    }

    #[test]
    fn index_drd_writer_round_trips_through_reader() {
        let metrics = vec!["added".to_string(), "delta".to_string()];
        let dimensions = vec!["language".to_string(), "page".to_string()];
        let interval = Interval {
            start_millis: 1_000,
            end_millis: 2_000,
        };
        let bytes = encode_native_index_drd(&metrics, &dimensions, &interval).expect("write");
        let parsed = parse_native_index_drd(&bytes, 16_384, 16_384).expect("parse");
        assert_eq!(parsed.dimensions, dimensions);
        assert_eq!(parsed.metrics, metrics);
        assert_eq!(parsed.interval.start_millis, 1_000);
        assert_eq!(parsed.interval.end_millis, 2_000);
    }

    // -- scope-out / consistency rejections ----------------------------------

    #[test]
    fn out_of_scope_columns_are_rejected_loudly() {
        let nullable = ColumnData::LongNullable(vec![1, 0, 3], {
            let mut bm = DruidBitmap::new();
            bm.insert(1);
            bm
        });
        let err = write_native_column("code", &nullable).expect_err("nullable long");
        assert!(err.to_string().contains("nullable LONG"), "got: {err}");

        let complex = ColumnData::Complex(vec![0xDE, 0xAD]);
        let err = write_native_column("sketch", &complex).expect_err("complex");
        assert!(err.to_string().contains("complex/sketch"), "got: {err}");
    }

    #[test]
    fn missing_declared_column_is_rejected() {
        let mut segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2])
            .add_long_column("added", true, vec![10, 20])
            .build()
            .expect("build");
        segment.columns.remove("added");
        let dir = tempfile::tempdir().expect("tempdir");
        let err = write_segment_v9_native(&segment, &dir.path().join("seg"))
            .expect_err("declared-but-missing column must be refused");
        assert!(err.to_string().contains("no data"), "got: {err}");
    }

    #[test]
    fn missing_time_column_is_rejected() {
        let segment = SegmentData {
            version: 9,
            num_rows: 0,
            interval: Interval {
                start_millis: 0,
                end_millis: 0,
            },
            dimensions: vec![],
            metrics: vec![],
            columns: std::collections::HashMap::new(),
            time_sorted: false,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let err = write_segment_v9_native(&segment, &dir.path().join("seg"))
            .expect_err("missing __time must be refused");
        assert!(err.to_string().contains("__time"), "got: {err}");
    }

    /// A column named after any of the segment's own control files would
    /// collide with (and overwrite/shadow) the real entry — every reserved
    /// name must be refused loudly, as metric AND as dimension.
    #[test]
    fn reserved_segment_filenames_are_rejected_as_column_names() {
        for reserved in [
            "version.bin",
            "factory.json",
            "meta.smoosh",
            "metadata.drd",
            "index.drd",
        ] {
            // As a metric (LONG column).
            let segment = SegmentDataBuilder::new()
                .add_timestamp_column(vec![1, 2])
                .add_long_column(reserved, true, vec![10, 20])
                .build()
                .expect("build");
            let dir = tempfile::tempdir().expect("tempdir");
            let err = write_segment_v9_native(&segment, &dir.path().join("seg"))
                .expect_err("reserved filename as metric name must be refused");
            assert!(
                err.to_string().contains("reserved"),
                "metric `{reserved}`: got: {err}"
            );

            // As a dimension (STRING column).
            let segment = SegmentDataBuilder::new()
                .add_timestamp_column(vec![1, 2])
                .add_string_column(reserved, vec!["a".to_string(), "b".to_string()])
                .build()
                .expect("build");
            let dir = tempfile::tempdir().expect("tempdir");
            let err = write_segment_v9_native(&segment, &dir.path().join("seg"))
                .expect_err("reserved filename as dimension name must be refused");
            assert!(
                err.to_string().contains("reserved"),
                "dimension `{reserved}`: got: {err}"
            );
        }
    }

    /// The emitted value bitmaps must be DERIVED from the encoded ordinals,
    /// never trusted from the input column: a `StringColumnData` whose
    /// stored `bitmap_indexes` disagree with its ordinals (wrong rows AND
    /// an out-of-range row) must still produce a segment whose read-back
    /// bitmaps match the ORDINALS exactly.
    #[test]
    fn string_value_bitmaps_are_derived_from_ordinals_not_trusted() {
        // dict [Accueil, Hauptseite, Main_Page, Talk:Main_Page],
        // ordinals [2, 3, 0, 1, 2] over 5 rows.
        let mut segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![0, 1, 2, 3, 4])
            .add_string_column(
                "page",
                [
                    "Main_Page",
                    "Talk:Main_Page",
                    "Accueil",
                    "Hauptseite",
                    "Main_Page",
                ]
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            )
            .build()
            .expect("build");
        let ordinals = {
            let Some(ColumnData::String(col)) = segment.columns.get_mut("page") else {
                panic!("expected String column");
            };
            // Corrupt every stored bitmap: in-range-but-wrong rows plus one
            // out-of-range row (100 in a 5-row column).
            let mut bogus = vec![DruidBitmap::new(); 4];
            bogus[0].insert(0);
            bogus[1].insert(4);
            bogus[2].insert(1);
            bogus[3].insert(3);
            bogus[3].insert(100);
            col.bitmap_indexes = bogus;
            col.encoded_values.clone()
        };

        let workdir = tempfile::tempdir().expect("tempdir");
        let dir = workdir.path().join("index");
        write_segment_v9_native(&segment, &dir).expect("write must succeed (bitmaps derived)");
        let read_back = SegmentData::open(&dir).expect("segment must stay readable");
        let Some(ColumnData::String(decoded)) = read_back.columns.get("page") else {
            panic!("expected String column after round-trip");
        };

        // Every value bitmap == exactly the rows whose ordinal maps to it.
        assert_eq!(decoded.bitmap_indexes.len(), 4);
        for (dict_idx, bitmap) in decoded.bitmap_indexes.iter().enumerate() {
            let expected: Vec<u32> = ordinals
                .iter()
                .enumerate()
                .filter(|&(_, &ordinal)| ordinal as usize == dict_idx)
                .map(|(row, _)| u32::try_from(row).expect("row fits u32"))
                .collect();
            assert_eq!(
                bitmap.iter().collect::<Vec<u32>>(),
                expected,
                "value bitmap {dict_idx} must be rebuilt from the ordinals, \
                 not the bogus input bitmaps"
            );
        }
        // Selector-style read: rows for "Main_Page" (dict index 2).
        assert_eq!(
            decoded.bitmap_indexes[2].iter().collect::<Vec<u32>>(),
            vec![0, 4],
            "selector on `Main_Page` must return exactly rows 0 and 4"
        );
    }

    /// A segment whose entry count would exceed the reader's smoosh cap
    /// must be refused loudly (naming the limit) instead of "succeeding"
    /// with a segment nothing can open: 16 383 metrics + `index.drd` +
    /// `__time` = 16 385 entries > 16 384.
    #[test]
    fn smoosh_entry_limit_is_enforced_before_writing() {
        let mut columns = std::collections::HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![0]));
        let segment = SegmentData {
            version: 9,
            num_rows: 1,
            interval: Interval {
                start_millis: 0,
                end_millis: 1,
            },
            dimensions: vec![],
            metrics: (0..16_383).map(|i| format!("m{i:05}")).collect(),
            columns,
            time_sorted: true,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let err = write_segment_v9_native(&segment, &dir.path().join("seg"))
            .expect_err("an over-limit entry count must be refused");
        assert!(
            err.to_string().contains("16384"),
            "the error must name the reader's entry limit, got: {err}"
        );
    }

    /// A segment whose row count exceeds the reader's per-column cap
    /// (`MAX_COLUMN_VALUES`, 2^26 = 67 108 864) can never be reopened — by
    /// this reader OR by Druid — so the native writer must refuse it loudly
    /// (naming the cap) instead of emitting an unopenable segment. Built
    /// cheaply: the oversized count lives only in `num_rows`, never in a
    /// materialized column, so the test allocates a single-row `__time`.
    #[test]
    fn row_count_over_max_column_values_is_rejected() {
        let cap = crate::druid_native::MAX_COLUMN_VALUES;
        let over = cap.checked_add(1).expect("cap + 1 fits in usize");
        let mut columns = std::collections::HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![0]));
        let segment = SegmentData {
            version: 9,
            num_rows: over,
            interval: Interval {
                start_millis: 0,
                end_millis: 1,
            },
            dimensions: vec![],
            metrics: vec![],
            columns,
            time_sorted: true,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let err = write_segment_v9_native(&segment, &dir.path().join("seg"))
            .expect_err("an over-cap row count must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains(&cap.to_string()),
            "the error must name the cap value, got: {msg}"
        );
        assert!(
            msg.contains("MAX_COLUMN_VALUES"),
            "the error must reference the cap by name, got: {msg}"
        );
    }

    /// A column present in `columns` but declared neither as a dimension
    /// nor a metric (nor `__time`) must be refused loudly — silently
    /// dropping it would lose data while reporting success.
    #[test]
    fn undeclared_column_entry_is_rejected() {
        let mut segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2])
            .add_long_column("added", true, vec![10, 20])
            .build()
            .expect("build");
        segment
            .columns
            .insert("ghost".to_string(), ColumnData::Long(vec![7, 8]));
        let dir = tempfile::tempdir().expect("tempdir");
        let err = write_segment_v9_native(&segment, &dir.path().join("seg"))
            .expect_err("an undeclared column map entry must be refused");
        assert!(err.to_string().contains("ghost"), "got: {err}");
    }

    // -- end-to-end ----------------------------------------------------------

    /// The canonical 10-row `wikipedia_compat` fixture (mirrors the
    /// segment-compat harness's `build_canonical_segment`).
    fn build_canonical_segment() -> SegmentData {
        let base = 1_704_067_200_000_i64;
        let h = 3_600_000_i64;
        let d = 86_400_000_i64;
        let times = vec![
            base,
            base + 2 * h,
            base + 3 * h,
            base + 12 * h,
            base + 8 * h + 2 * d,
            base + d,
            base + 12 * h + d,
            base + 2 * d,
            base + d,
            base + 3 * h,
        ];
        let langs = ["en", "fr", "de", "en", "it", "en", "fr", "en", "en", "en"];
        let pages = [
            "Main_Page",
            "Accueil",
            "Hauptseite",
            "Main_Page",
            "Pagina_principale",
            "Main_Page",
            "Accueil",
            "Main_Page",
            "Portal:Current_events",
            "Talk:Main_Page",
        ];
        let users = [
            "Alice", "Bob", "Diana", "Eve", "Heidi", "Alice", "Claude", "Eve", "Frank", "Grace",
        ];
        let namespaces = [
            "Main", "Main", "Main", "Main", "Main", "Main", "Main", "Main", "Portal", "Talk",
        ];
        let added = vec![100_i64, 200, 150, 75, 110, 120, 180, 90, 300, 50];
        let delta = vec![10_i64, 20, 15, 8, 11, 12, 18, 9, 30, 5];
        SegmentDataBuilder::new()
            .add_timestamp_column(times)
            .add_string_column("language", langs.iter().map(|s| (*s).to_string()).collect())
            .add_string_column("page", pages.iter().map(|s| (*s).to_string()).collect())
            .add_string_column("user", users.iter().map(|s| (*s).to_string()).collect())
            .add_string_column(
                "namespace",
                namespaces.iter().map(|s| (*s).to_string()).collect(),
            )
            .add_long_column("added", true, added)
            .add_long_column("delta", true, delta)
            .build()
            .expect("build canonical segment")
    }

    /// Deep column-for-column equality (the same contract the harness's
    /// per-column SHA table asserts, applied in-process).
    fn assert_segment_deep_equal(read_back: &SegmentData, source: &SegmentData) {
        assert_eq!(read_back.num_rows, source.num_rows, "num_rows");
        assert_eq!(read_back.dimensions, source.dimensions, "dimensions");
        assert_eq!(read_back.metrics, source.metrics, "metrics");
        assert_eq!(
            read_back.interval.start_millis, source.interval.start_millis,
            "interval start"
        );
        assert_eq!(
            read_back.interval.end_millis, source.interval.end_millis,
            "interval end"
        );
        assert_eq!(
            read_back.columns.len(),
            source.columns.len(),
            "column count"
        );
        for (name, source_col) in &source.columns {
            let got = read_back
                .columns
                .get(name)
                .unwrap_or_else(|| panic!("column {name} missing after round-trip"));
            match (got, source_col) {
                (ColumnData::Long(a), ColumnData::Long(b)) => {
                    assert_eq!(a, b, "column {name} values");
                }
                (ColumnData::String(a), ColumnData::String(b)) => {
                    assert_eq!(
                        a.dictionary.iter().collect::<Vec<_>>(),
                        b.dictionary.iter().collect::<Vec<_>>(),
                        "column {name} dictionary"
                    );
                    assert_eq!(a.encoded_values, b.encoded_values, "column {name} ordinals");
                    assert_eq!(
                        a.bitmap_indexes, b.bitmap_indexes,
                        "column {name} value bitmaps"
                    );
                }
                (got, want) => panic!("column {name}: type mismatch {got:?} vs {want:?}"),
            }
        }
    }

    /// THE key GREEN: write the canonical segment in the native layout,
    /// re-open it from disk through the Druid-verified reader stack
    /// (`SmooshReader::open` → `read_segment_v9`, which must auto-detect
    /// the NATIVE layout), and assert every column round-trips exactly.
    #[test]
    fn end_to_end_native_write_read_round_trip() {
        let segment = build_canonical_segment();
        let workdir = tempfile::tempdir().expect("tempdir");
        let dir = workdir.path().join("index");
        write_segment_v9_native(&segment, &dir).expect("write_segment_v9_native");

        // Sidecar contract: version.bin / factory.json are REAL FILES next
        // to meta.smoosh, not smoosh entries (writer.rs:355 regression).
        let version_bytes = std::fs::read(dir.join("version.bin")).expect("version.bin sidecar");
        assert_eq!(version_bytes, 9_i32.to_be_bytes());
        let factory = std::fs::read_to_string(dir.join("factory.json")).expect("factory.json");
        assert_eq!(factory, FACTORY_JSON);
        let meta = std::fs::read_to_string(dir.join("meta.smoosh")).expect("meta.smoosh");
        assert!(
            !meta.contains("version.bin") && !meta.contains("factory.json"),
            "sidecars must NOT be smoosh entries, meta was:\n{meta}"
        );
        assert!(
            !meta.contains("column_descriptor"),
            "the native layout embeds descriptors in the column blobs; sidecar \
             descriptor entries would mark the PRIVATE layout"
        );

        // NATIVE-layout detection: every column blob leads with the
        // embedded length-prefixed JSON descriptor.
        let smoosh = SmooshReader::open(&dir).expect("SmooshReader::open");
        for name in ["__time", "language", "page", "added"] {
            assert!(
                is_native_column(smoosh.read_file(name).expect("column entry")),
                "column {name} must be detected as a NATIVE column"
            );
        }

        // The Druid-verified reader is the oracle: if it reproduces the
        // source exactly, real Druid parses these bytes.
        let read_back = read_segment_v9(&smoosh).expect("read_segment_v9 (native path)");
        assert_segment_deep_equal(&read_back, &segment);

        // The public high-level entry point agrees.
        let opened = SegmentData::open(&dir).expect("SegmentData::open");
        assert_segment_deep_equal(&opened, &segment);
    }

    /// A double/float metric survives the native end-to-end round-trip too
    /// (cheap-add coverage beyond the LONG+STRING milestone core).
    #[test]
    fn end_to_end_with_double_and_float_metrics() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1_000, 2_000, 3_000])
            .add_string_column(
                "city",
                vec![
                    "osaka".to_string(),
                    "tokyo".to_string(),
                    "osaka".to_string(),
                ],
            )
            .add_long_column("hits", true, vec![5, 7, 11])
            .add_double_column("ratio", true, vec![0.5, -2.25, 1e12])
            .build()
            .expect("build");
        let workdir = tempfile::tempdir().expect("tempdir");
        let dir = workdir.path().join("index");
        write_segment_v9_native(&segment, &dir).expect("write");
        let read_back = SegmentData::open(&dir).expect("open");
        assert_eq!(read_back.num_rows, 3);
        match read_back.columns.get("ratio") {
            Some(ColumnData::Double(v)) => assert_eq!(v, &[0.5, -2.25, 1e12]),
            other => panic!("expected Double, got {other:?}"),
        }
        match read_back.columns.get("hits") {
            Some(ColumnData::Long(v)) => assert_eq!(v, &[5, 7, 11]),
            other => panic!("expected Long, got {other:?}"),
        }
    }
}
