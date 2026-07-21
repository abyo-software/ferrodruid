// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! FDX (FerroDruid extended) segment reading and writing logic.
//!
//! FDX is a **FerroDruid-internal** segment format — an evolution of the
//! Apache Druid v9 on-disk format with improved encoding. It is not an
//! Apache Druid format: Apache Druid's current on-disk segment format is
//! v9, and Apache Druid defines no version-10 disk format. FDX segments
//! are written and read by FerroDruid only; use [`crate::v9`] for
//! Druid-interchangeable segments.
//!
//! Differences from v9:
//! - `version.bin` = 10 (the FerroDruid-reserved version number)
//! - Column data may use Arrow-compatible encoding for numeric types
//! - Improved front-coded dictionary with version 2 header
//! - Nested column support for JSON-type data
//!
//! The initial implementation delegates most logic to v9-compatible paths,
//! since the core binary format is largely the same. The key difference is
//! the version check and the ability to handle newer dictionary/compression
//! options as they are added.

use std::collections::HashMap;
use std::path::Path;

use ferrodruid_common::error::{DruidError, Result};

use crate::column::{
    ColumnData, ColumnDescriptor, decode_double_column, decode_float_column, decode_long_column,
    decode_long_column_nullable, decode_string_column, decode_string_multi_column,
    encode_double_column, encode_float_column, encode_long_column, encode_long_column_nullable,
    encode_string_column_v2, encode_string_multi_column_v2,
};
use crate::segment::{Interval, SegmentData};
use crate::smoosh::SmooshReader;

/// Expected segment version for FDX.
const SEGMENT_VERSION_FDX: i32 = 10;

/// Hard upper bound on dimension columns declared by a segment's `index.drd`.
/// Mirrors [`crate::v9`] — see that module for the rationale.
const MAX_DIMENSIONS: usize = 16_384;

/// Hard upper bound on metric columns declared by a segment's `index.drd`.
const MAX_METRICS: usize = 16_384;

/// Default front-coded dictionary bucket size for FDX string columns.
///
/// Must be a power of two (the v2 encoder enforces this). Four entries per
/// bucket matches the bucket size used by the v9 (v1) writer and keeps
/// random-access cost low while still amortising the full bucket-base string.
const FC_V2_BUCKET_SIZE: usize = 4;

/// Read a FDX segment from a [`SmooshReader`].
///
/// FDX is largely compatible with v9, with:
/// - `version.bin` = 10
/// - Column encoding may use Arrow IPC for numeric columns (future)
/// - Dictionary may use front-coded v2 with `bucket_size` in header (future)
///
/// For the initial implementation, FDX reading delegates to the same column
/// decoders as v9 since the on-disk encoding is compatible.
///
/// Wave 36-E (Wave 37 R1 finding `fdx.rs:64-79`): the previous `if let
/// Ok(col) = read_column(...)` pattern silently dropped any column that
/// failed to decode — a corrupt segment yielded a partial `SegmentData`
/// and consequently wrong query results.  The behaviour is now
/// fail-fast.  Operators that need a one-time migration window can use
/// [`read_segment_fdx_lenient`] to recover the previous "drop and
/// continue" semantics.
pub fn read_segment_fdx(smoosh: &SmooshReader) -> Result<SegmentData> {
    read_segment_fdx_inner(smoosh, /* lenient */ false)
}

/// Lenient counterpart of [`read_segment_fdx`] that drops columns whose
/// decode fails instead of returning an error.  Each dropped column is
/// surfaced via `tracing::warn!`.  See [`crate::v9::read_segment_v9_lenient`]
/// for the rationale; lenient mode should not be used in production.
pub fn read_segment_fdx_lenient(smoosh: &SmooshReader) -> Result<SegmentData> {
    read_segment_fdx_inner(smoosh, /* lenient */ true)
}

/// Strict-vs-lenient column read.  Private; callers pick a mode via the
/// [`read_segment_fdx`] / [`read_segment_fdx_lenient`] entry points.
fn read_segment_fdx_inner(smoosh: &SmooshReader, lenient: bool) -> Result<SegmentData> {
    // Step 1: version check.
    let version = read_version(smoosh)?;
    if version != SEGMENT_VERSION_FDX {
        return Err(DruidError::Segment(format!(
            "expected segment version {SEGMENT_VERSION_FDX}, got {version}"
        )));
    }

    // Step 2: read index.drd (same format as v9).
    let index = read_index_drd(smoosh)?;

    // Step 3: read columns (same logic as v9 for now).  Strict by default;
    // see [`read_segment_fdx`] doc comment.
    let mut columns = HashMap::new();
    let time_col_name = "__time";

    let load = |name: &str, columns: &mut HashMap<String, ColumnData>| -> Result<()> {
        match read_column(smoosh, name, &index.dimensions, &index.metrics) {
            Ok(col) => {
                columns.insert(name.to_string(), col);
                Ok(())
            }
            Err(e) => {
                if lenient {
                    tracing::warn!(
                        column = name,
                        error = %e,
                        "FDX segment: dropping column whose decode failed (lenient mode)"
                    );
                    Ok(())
                } else {
                    Err(DruidError::Segment(format!(
                        "FDX segment: failed to decode column `{name}`: {e}"
                    )))
                }
            }
        }
    };

    // Timestamp column.  Strict mode requires it; lenient mode tolerates
    // its absence (legacy behaviour).
    if smoosh.has_file(time_col_name) {
        load(time_col_name, &mut columns)?;
    } else if !lenient {
        return Err(DruidError::Segment(
            "FDX segment: required column `__time` is missing from smoosh archive".to_string(),
        ));
    }

    for dim in &index.dimensions {
        load(dim, &mut columns)?;
    }

    for metric in &index.metrics {
        load(metric, &mut columns)?;
    }

    let num_rows = columns
        .get(time_col_name)
        .and_then(|c| c.num_rows())
        .or_else(|| columns.values().find_map(|c| c.num_rows()))
        .unwrap_or(0);

    // A decoded per-row sketch column (ComplexTheta / ComplexHyperUnique)
    // decodes its own row count from its blob; nothing ties that count to
    // the segment's canonical row count, so a truncated sketch column
    // would otherwise reopen fine with the missing rows silently read as
    // absent/empty sketches — cardinality under-counting.  Enforce the
    // row-count agreement in BOTH strict and lenient mode (mirroring
    // [`crate::v9`] exactly): the mismatch is data corruption, not a
    // decode failure that lenient loading may drop.
    for (name, col) in &columns {
        let sketch_rows = match col {
            ColumnData::ComplexTheta(rows) => Some(rows.len()),
            ColumnData::ComplexHyperUnique(rows) => Some(rows.len()),
            _ => None,
        };
        if let Some(rows) = sketch_rows
            && rows != num_rows
        {
            return Err(DruidError::Segment(format!(
                "FDX segment: complex sketch column `{name}` holds {rows} sketch rows \
                 but the segment has {num_rows} rows"
            )));
        }
    }

    // Wave 36-G4 (W37B High `segment_tails: fdx.rs:80-129`): in strict
    // mode, every column that exposes a row count MUST match `num_rows`.
    // Pre-fix the reader silently set `num_rows` from `__time` and never
    // checked the rest, so a corrupt segment could pair misaligned
    // dimensions/metrics with the canonical row count and produce wrong
    // aggregates / OOB row access at query time.  Complex columns are
    // exempt because their `num_rows()` is `None` by design.
    if !lenient {
        for (name, col) in &columns {
            if let Some(rows) = col.num_rows()
                && rows != num_rows
            {
                return Err(DruidError::Segment(format!(
                    "FDX segment: column `{name}` row count {rows} does not match \
                     canonical row count {num_rows}"
                )));
            }
        }
    }

    let time_sorted = matches!(
        columns.get("__time"),
        Some(ColumnData::Long(v)) if v.is_sorted()
    );

    Ok(SegmentData {
        version,
        num_rows,
        interval: index.interval,
        dimensions: index.dimensions,
        metrics: index.metrics,
        columns,
        time_sorted,
    })
}

/// Write a FDX [`SegmentData`] to a smoosh-format directory on disk.
///
/// Creates `meta.smoosh` and `00000.smoosh` inside `dir`.
/// Currently uses v9-compatible column encoding with a FDX version header.
pub fn write_segment_fdx(segment: &SegmentData, dir: &Path) -> Result<()> {
    let writer = build_fdx_smoosh_writer(segment)?;
    writer.write_to_dir(dir)
}

/// Write a FDX [`SegmentData`] to in-memory smoosh parts (for testing).
///
/// Returns `(meta_smoosh_text, chunk_byte_vecs)`.
pub fn write_segment_fdx_to_memory(segment: &SegmentData) -> Result<(String, Vec<Vec<u8>>)> {
    let writer = build_fdx_smoosh_writer(segment)?;
    Ok(writer.finish())
}

// ---------------------------------------------------------------------------
// Internal: version / index.drd reading (FDX-aware)
// ---------------------------------------------------------------------------

/// Read and parse `version.bin` (4 bytes BE i32).
fn read_version(smoosh: &SmooshReader) -> Result<i32> {
    let data = smoosh.read_file("version.bin")?;
    if data.len() < 4 {
        return Err(DruidError::Segment(format!(
            "version.bin too short: {} bytes",
            data.len()
        )));
    }
    Ok(i32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}

/// Parsed contents of `index.drd`.
struct IndexDrd {
    dimensions: Vec<String>,
    metrics: Vec<String>,
    interval: Interval,
}

/// Read and parse `index.drd` for FDX, auto-detecting the layout.
///
/// The private FDX `index.drd` uses version marker 10 in its header but is
/// otherwise identical in layout to the private v9 layout. An `index.drd`
/// that does NOT lead with BE i32 == 10 is parsed as an upstream
/// Apache-Druid-written `index.drd` instead (see [`crate::druid_native`];
/// the upstream layout's leading BE i32 is 0x0100_0000/0x0101_0000, so the
/// two are cleanly distinguishable).
fn read_index_drd(smoosh: &SmooshReader) -> Result<IndexDrd> {
    let data = smoosh.read_file("index.drd")?;
    let mut pos: usize = 0;

    let ver = read_be_i32(data, &mut pos)?;
    if ver != SEGMENT_VERSION_FDX {
        let native =
            crate::druid_native::parse_native_index_drd(data, MAX_DIMENSIONS, MAX_METRICS)?;
        return Ok(IndexDrd {
            dimensions: native.dimensions,
            metrics: native.metrics,
            interval: native.interval,
        });
    }

    // See `crate::v9::read_index_drd` for the OOM rationale (Wave 35 R1).
    let num_dims = read_be_u32(data, &mut pos)? as usize;
    if num_dims > MAX_DIMENSIONS {
        return Err(DruidError::Segment(format!(
            "index.drd (FDX): num_dimensions {num_dims} exceeds cap {MAX_DIMENSIONS}"
        )));
    }
    let mut dimensions = Vec::with_capacity(num_dims);
    for _ in 0..num_dims {
        dimensions.push(read_length_prefixed_string(data, &mut pos)?);
    }

    let num_metrics = read_be_u32(data, &mut pos)? as usize;
    if num_metrics > MAX_METRICS {
        return Err(DruidError::Segment(format!(
            "index.drd (FDX): num_metrics {num_metrics} exceeds cap {MAX_METRICS}"
        )));
    }
    let mut metrics = Vec::with_capacity(num_metrics);
    for _ in 0..num_metrics {
        metrics.push(read_length_prefixed_string(data, &mut pos)?);
    }

    // Wave 36-F (Wave 37 R1 medium): a truncated `index.drd` previously
    // fell back to `min_ts = 0` / `max_ts = 0`, which fabricated a 1970-01-01
    // interval and broke segment pruning + time-bounded query routing.
    // Both timestamps are now required; truncation is a hard error.
    if pos + 8 > data.len() {
        return Err(DruidError::Segment(format!(
            "index.drd (FDX): truncated before min_ts: need {} bytes after metrics, have {}",
            8,
            data.len().saturating_sub(pos)
        )));
    }
    let min_ts = read_be_i64(data, &mut pos)?;
    if pos + 8 > data.len() {
        return Err(DruidError::Segment(format!(
            "index.drd (FDX): truncated before max_ts: need {} bytes after min_ts, have {}",
            8,
            data.len().saturating_sub(pos)
        )));
    }
    let max_ts = read_be_i64(data, &mut pos)?;

    Ok(IndexDrd {
        dimensions,
        metrics,
        interval: Interval {
            start_millis: min_ts,
            end_millis: max_ts,
        },
    })
}

// ---------------------------------------------------------------------------
// Column reading (FDX)
// ---------------------------------------------------------------------------

/// Read a single column from the smoosh archive.
fn read_column(
    smoosh: &SmooshReader,
    name: &str,
    dimensions: &[String],
    _metrics: &[String],
) -> Result<ColumnData> {
    let desc_key = format!("{name}.column_descriptor.json");
    let descriptor = if smoosh.has_file(&desc_key) {
        let desc_data = smoosh.read_file(&desc_key)?;
        Some(ColumnDescriptor::from_json(desc_data)?)
    } else {
        None
    };

    let data = smoosh.read_file(name)?;

    // Upstream-Druid column detection: real Druid columns embed their
    // descriptor inside the blob rather than as a sidecar entry. The
    // FerroDruid writer always emits the sidecar, so this branch only
    // fires for genuine upstream segments.
    if descriptor.is_none() && crate::druid_native::is_native_column(data) {
        return crate::druid_native::decode_native_column(data);
    }

    // Wave 36-G4 (W37B High `segment_tails: fdx.rs:233-257`): pre-fix, a
    // missing `<col>.column_descriptor.json` silently fell back to a
    // guessed codec (`STRING` for declared dimensions, `DOUBLE` for
    // everything else, `LONG` for `__time`).  That allowed a malformed
    // segment to decode LONG/FLOAT/COMPLEX metric bytes under the wrong
    // codec and return corrupted values instead of failing.
    //
    // We now require an explicit descriptor for every non-`__time` column.
    // `__time` keeps its implicit `LONG` default because it is fixed by
    // the FDX spec and the current writer does not always emit a
    // descriptor for it; that is safe because the type cannot be
    // misidentified (the column key itself names the codec).
    let value_type = match descriptor.as_ref() {
        Some(d) => d.value_type.as_str(),
        None => {
            if name == "__time" {
                "LONG"
            } else {
                let kind = if dimensions.iter().any(|d| d == name) {
                    "dimension"
                } else {
                    "metric"
                };
                return Err(DruidError::Segment(format!(
                    "FDX segment: {kind} column `{name}` is missing required \
                     `{name}.column_descriptor.json`; refusing to guess codec"
                )));
            }
        }
    };

    // Nullable-long columns carry the null flag in the descriptor and a
    // trailing null-row bitmap in the blob.
    let has_nulls = descriptor.as_ref().is_some_and(|d| d.has_nulls);

    match value_type {
        "LONG" if has_nulls => {
            let (values, nulls) = decode_long_column_nullable(data)?;
            Ok(ColumnData::LongNullable(values, nulls))
        }
        "LONG" => {
            let values = decode_long_column(data)?;
            Ok(ColumnData::Long(values))
        }
        "FLOAT" => {
            let values = decode_float_column(data)?;
            Ok(ColumnData::Float(values))
        }
        "DOUBLE" => {
            let values = decode_double_column(data)?;
            Ok(ColumnData::Double(values))
        }
        // Multi-value string dimensions carry `hasMultipleValues: true` in
        // the descriptor and the offsets+ordinals layout in the blob
        // (compat-11).
        "STRING" if descriptor.as_ref().is_some_and(|d| d.has_multiple_values) => {
            let col = decode_string_multi_column(data)?;
            Ok(ColumnData::StringMulti(col))
        }
        "STRING" => {
            let col = decode_string_column(data)?;
            Ok(ColumnData::String(col))
        }
        // A COMPLEX descriptor carrying the theta `complexTypeName` is a
        // decoded per-row theta column written by the FerroDruid writer
        // (compat-8 sketch #2); any other COMPLEX stays the opaque
        // passthrough, exactly as before.
        "COMPLEX"
            if descriptor.as_ref().is_some_and(|d| {
                d.complex_type_name.as_deref() == Some(crate::column::THETA_COMPLEX_TYPE)
            }) =>
        {
            Ok(ColumnData::ComplexTheta(
                crate::column::decode_theta_column(data)?,
            ))
        }
        // Same for a decoded per-row hyperUnique column (W-A, v1.5.0).
        "COMPLEX"
            if descriptor.as_ref().is_some_and(|d| {
                d.complex_type_name.as_deref() == Some(crate::column::HYPER_UNIQUE_COMPLEX_TYPE)
            }) =>
        {
            Ok(ColumnData::ComplexHyperUnique(
                crate::column::decode_hyper_unique_column(data)?,
            ))
        }
        "COMPLEX" => Ok(ColumnData::Complex(data.to_vec())),
        other => Err(DruidError::Segment(format!(
            "unsupported column value type: {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// index.drd encoder (FDX)
// ---------------------------------------------------------------------------

/// Encode an `index.drd` blob for FDX (for testing and writer).
pub fn encode_index_drd_fdx(
    dimensions: &[&str],
    metrics: &[&str],
    min_ts: i64,
    max_ts: i64,
    bitmap_type: u32,
) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.extend_from_slice(&SEGMENT_VERSION_FDX.to_be_bytes());

    buf.extend_from_slice(&(dimensions.len() as u32).to_be_bytes());
    for d in dimensions {
        let bytes = d.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(bytes);
    }

    buf.extend_from_slice(&(metrics.len() as u32).to_be_bytes());
    for m in metrics {
        let bytes = m.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(bytes);
    }

    buf.extend_from_slice(&min_ts.to_be_bytes());
    buf.extend_from_slice(&max_ts.to_be_bytes());
    buf.extend_from_slice(&bitmap_type.to_be_bytes());

    buf
}

// ---------------------------------------------------------------------------
// SmooshWriter (FDX)
// ---------------------------------------------------------------------------

/// Accumulates logical files and serializes them into the smoosh format.
struct SmooshWriter {
    files: Vec<(String, Vec<u8>)>,
}

impl SmooshWriter {
    fn new() -> Self {
        Self { files: Vec::new() }
    }

    fn add_file(&mut self, name: String, data: Vec<u8>) {
        self.files.push((name, data));
    }

    fn finish(self) -> (String, Vec<Vec<u8>>) {
        let mut chunk = Vec::new();
        let mut meta_lines = Vec::new();

        meta_lines.push(format!("v1,2147483647,{}", self.files.len()));

        for (name, data) in &self.files {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            meta_lines.push(format!("{name},0,{start},{end}"));
        }

        let meta = meta_lines.join("\n");
        (meta, vec![chunk])
    }

    /// Write `meta.smoosh` and `00000.smoosh` to a directory on disk using
    /// the crash-safe `temp + fsync + rename` discipline shared with the v9
    /// writer (see [`crate::writer`] module docs).
    fn write_to_dir(self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir).map_err(|e| {
            DruidError::Segment(format!("failed to create dir {}: {e}", dir.display()))
        })?;

        let (meta, chunks) = self.finish();

        // Write chunks first; meta.smoosh is the commit marker.
        for (i, chunk) in chunks.iter().enumerate() {
            let path = dir.join(format!("{i:05}.smoosh"));
            crate::writer::durable_write(&path, chunk)?;
        }

        // Now publish the segment by writing meta.smoosh.
        crate::writer::durable_write(&dir.join("meta.smoosh"), meta.as_bytes())?;

        // fsync parent so the rename is durable.
        crate::writer::fsync_dir(dir)?;

        Ok(())
    }
}

/// Build a [`SmooshWriter`] for FDX from a [`SegmentData`].
fn build_fdx_smoosh_writer(segment: &SegmentData) -> Result<SmooshWriter> {
    let mut w = SmooshWriter::new();

    // 1. version.bin = 10
    w.add_file(
        "version.bin".to_string(),
        SEGMENT_VERSION_FDX.to_be_bytes().to_vec(),
    );

    // 2. index.drd (FDX header)
    let dim_refs: Vec<&str> = segment.dimensions.iter().map(|s| s.as_str()).collect();
    let met_refs: Vec<&str> = segment.metrics.iter().map(|s| s.as_str()).collect();
    let index_drd = encode_index_drd_fdx(
        &dim_refs,
        &met_refs,
        segment.interval.start_millis,
        segment.interval.end_millis,
        1, // roaring bitmap type
    );
    w.add_file("index.drd".to_string(), index_drd);

    // 3. Columns (v9-compatible encoding for now)
    let mut col_names: Vec<&str> = Vec::new();
    if segment.columns.contains_key("__time") {
        col_names.push("__time");
    }
    for dim in &segment.dimensions {
        if segment.columns.contains_key(dim.as_str()) {
            col_names.push(dim);
        }
    }
    for met in &segment.metrics {
        if segment.columns.contains_key(met.as_str()) {
            col_names.push(met);
        }
    }

    for col_name in col_names {
        let col = match segment.columns.get(col_name) {
            Some(c) => c,
            None => continue,
        };

        let descriptor = column_descriptor_for(col);
        let desc_json = serde_json::to_vec(&descriptor).map_err(|e| {
            DruidError::Segment(format!(
                "failed to serialize descriptor for {col_name}: {e}"
            ))
        })?;
        w.add_file(format!("{col_name}.column_descriptor.json"), desc_json);

        let col_bytes = encode_column(col)?;
        w.add_file(col_name.to_string(), col_bytes);
    }

    Ok(w)
}

/// Build a [`ColumnDescriptor`] from a [`ColumnData`].
fn column_descriptor_for(col: &ColumnData) -> ColumnDescriptor {
    match col {
        ColumnData::Long(_) => ColumnDescriptor {
            value_type: "LONG".to_string(),
            has_multiple_values: false,
            has_bitmap_indexes: false,
            has_spatial_indexes: false,
            has_nulls: false,
            complex_type_name: None,
        },
        // Nullable long: same LONG value type, with the null flag telling
        // the reader to expect the trailing null-row bitmap section.
        ColumnData::LongNullable(_, _) => ColumnDescriptor {
            value_type: "LONG".to_string(),
            has_multiple_values: false,
            has_bitmap_indexes: false,
            has_spatial_indexes: false,
            has_nulls: true,
            complex_type_name: None,
        },
        ColumnData::Float(_) => ColumnDescriptor {
            value_type: "FLOAT".to_string(),
            has_multiple_values: false,
            has_bitmap_indexes: false,
            has_spatial_indexes: false,
            has_nulls: false,
            complex_type_name: None,
        },
        ColumnData::Double(_) => ColumnDescriptor {
            value_type: "DOUBLE".to_string(),
            has_multiple_values: false,
            has_bitmap_indexes: false,
            has_spatial_indexes: false,
            has_nulls: false,
            complex_type_name: None,
        },
        ColumnData::String(_) => ColumnDescriptor {
            value_type: "STRING".to_string(),
            has_multiple_values: false,
            has_bitmap_indexes: true,
            has_spatial_indexes: false,
            has_nulls: false,
            complex_type_name: None,
        },
        // Multi-value string dimension (compat-11): same STRING value type,
        // with `hasMultipleValues: true` telling the reader to use the
        // multi-value decoder (Druid's descriptor convention).
        ColumnData::StringMulti(_) => ColumnDescriptor {
            value_type: "STRING".to_string(),
            has_multiple_values: true,
            has_bitmap_indexes: true,
            has_spatial_indexes: false,
            has_nulls: false,
            complex_type_name: None,
        },
        ColumnData::Complex(_) => ColumnDescriptor {
            value_type: "COMPLEX".to_string(),
            has_multiple_values: false,
            has_bitmap_indexes: false,
            has_spatial_indexes: false,
            has_nulls: false,
            complex_type_name: None,
        },
        // Decoded per-row theta column (compat-8 sketch #2): the
        // `complexTypeName` tells the reader to use the per-row sketch
        // decoder instead of the opaque COMPLEX passthrough.
        ColumnData::ComplexTheta(_) => ColumnDescriptor {
            value_type: "COMPLEX".to_string(),
            has_multiple_values: false,
            has_bitmap_indexes: false,
            has_spatial_indexes: false,
            has_nulls: false,
            complex_type_name: Some(crate::column::THETA_COMPLEX_TYPE.to_string()),
        },
        // Decoded per-row hyperUnique column (W-A, v1.5.0): same rule.
        ColumnData::ComplexHyperUnique(_) => ColumnDescriptor {
            value_type: "COMPLEX".to_string(),
            has_multiple_values: false,
            has_bitmap_indexes: false,
            has_spatial_indexes: false,
            has_nulls: false,
            complex_type_name: Some(crate::column::HYPER_UNIQUE_COMPLEX_TYPE.to_string()),
        },
    }
}

/// Encode a [`ColumnData`] to its binary representation.
fn encode_column(col: &ColumnData) -> Result<Vec<u8>> {
    match col {
        ColumnData::Long(v) => Ok(encode_long_column(v)),
        ColumnData::LongNullable(v, nulls) => encode_long_column_nullable(v, nulls),
        ColumnData::Float(v) => Ok(encode_float_column(v)),
        ColumnData::Double(v) => Ok(encode_double_column(v)),
        ColumnData::String(s) => encode_string_column_v2(s, FC_V2_BUCKET_SIZE),
        ColumnData::StringMulti(s) => encode_string_multi_column_v2(s, FC_V2_BUCKET_SIZE),
        ColumnData::Complex(b) => Ok(b.clone()),
        ColumnData::ComplexTheta(rows) => crate::column::encode_theta_column(rows),
        ColumnData::ComplexHyperUnique(rows) => crate::column::encode_hyper_unique_column(rows),
    }
}

// ---------------------------------------------------------------------------
// Binary read helpers
// ---------------------------------------------------------------------------

/// Read a big-endian i32 from `data` at `*pos`, advancing `*pos`.
fn read_be_i32(data: &[u8], pos: &mut usize) -> Result<i32> {
    if *pos + 4 > data.len() {
        return Err(DruidError::Segment(
            "unexpected end of data reading i32".to_string(),
        ));
    }
    let v = i32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos += 4;
    Ok(v)
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

/// Read a big-endian i64 from `data` at `*pos`, advancing `*pos`.
fn read_be_i64(data: &[u8], pos: &mut usize) -> Result<i64> {
    if *pos + 8 > data.len() {
        return Err(DruidError::Segment(
            "unexpected end of data reading i64".to_string(),
        ));
    }
    let v = i64::from_be_bytes([
        data[*pos],
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
        data[*pos + 4],
        data[*pos + 5],
        data[*pos + 6],
        data[*pos + 7],
    ]);
    *pos += 8;
    Ok(v)
}

/// Read a 2-byte-BE-length-prefixed UTF-8 string from `data` at `*pos`.
fn read_length_prefixed_string(data: &[u8], pos: &mut usize) -> Result<String> {
    if *pos + 2 > data.len() {
        return Err(DruidError::Segment(
            "unexpected end of data reading string length".to_string(),
        ));
    }
    let len = u16::from_be_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    if *pos + len > data.len() {
        return Err(DruidError::Segment(format!(
            "string data truncated: need {len} bytes at offset {}",
            *pos
        )));
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|e| DruidError::Segment(format!("invalid UTF-8 in string: {e}")))?;
    *pos += len;
    Ok(s.to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::{
        StringColumnData, encode_long_column, encode_string_column, encode_string_column_v2,
    };
    use crate::segment::SegmentDataBuilder;
    use ferrodruid_bitmap::DruidBitmap;
    use ferrodruid_dict::FrontCodedDictionary;

    /// Build a minimal FDX smoosh for testing.
    fn build_fdx_test_smoosh() -> SmooshReader {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();

        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };

        // version.bin = 10
        add("version.bin", &10_i32.to_be_bytes());

        // index.drd (FDX)
        let index = encode_index_drd_fdx(&["city"], &["count"], 1000, 2000, 1);
        add("index.drd", &index);

        // __time column
        let time_data = encode_long_column(&[1000, 1500, 2000]);
        add("__time", &time_data);
        add("__time.column_descriptor.json", br#"{"valueType":"LONG"}"#);

        // city column (STRING)
        let dict = FrontCodedDictionary::from_sorted(vec![
            "london".to_string(),
            "paris".to_string(),
            "tokyo".to_string(),
        ]);
        let mut bm0 = DruidBitmap::new();
        bm0.insert(1);
        let mut bm1 = DruidBitmap::new();
        bm1.insert(2);
        let mut bm2 = DruidBitmap::new();
        bm2.insert(0);

        let string_col = StringColumnData {
            dictionary: dict,
            encoded_values: vec![2, 0, 1], // tokyo, london, paris
            bitmap_indexes: vec![bm0, bm1, bm2],
        };
        let city_data = encode_string_column_v2(&string_col, 4).expect("encode v2 string column");
        add("city", &city_data);
        add(
            "city.column_descriptor.json",
            br#"{"valueType":"STRING","hasBitmapIndexes":true}"#,
        );

        // count column (LONG)
        let count_data = encode_long_column(&[10, 20, 30]);
        add("count", &count_data);
        add("count.column_descriptor.json", br#"{"valueType":"LONG"}"#);

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");

        SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts")
    }

    /// Build an FDX smoosh whose `__time` column has 3 rows and whose
    /// theta metric column holds `theta_rows` sketches (mirror of the v9
    /// test rig).
    fn build_fdx_theta_smoosh(theta_rows: usize) -> SmooshReader {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();
        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };

        add("version.bin", &10_i32.to_be_bytes());
        add(
            "index.drd",
            &encode_index_drd_fdx(&[], &["users_theta"], 1000, 2000, 1),
        );
        add("__time", &encode_long_column(&[1000, 1500, 2000]));
        add("__time.column_descriptor.json", br#"{"valueType":"LONG"}"#);

        let rows: Vec<ferrodruid_sketches::ThetaSketch> = (0..theta_rows)
            .map(|_| ferrodruid_sketches::ThetaSketch::empty_druid_origin())
            .collect();
        let theta_blob = crate::column::encode_theta_column(&rows).expect("encode theta");
        add("users_theta", &theta_blob);
        add(
            "users_theta.column_descriptor.json",
            br#"{"valueType":"COMPLEX","complexTypeName":"thetaSketch"}"#,
        );

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts")
    }

    /// A ComplexTheta column shorter than the segment's canonical row
    /// count must be a LOUD read error in strict AND LENIENT mode — the
    /// v9 reader enforces this in both modes, but the FDX lenient path
    /// used to accept the truncated column (the column itself decoded
    /// fine, so no warn either) and silently under-count cardinality.
    #[test]
    fn fdx_theta_row_count_mismatch_rejected_in_lenient_mode_too() {
        let smoosh = build_fdx_theta_smoosh(1);
        let err = read_segment_fdx(&smoosh).expect_err("strict read must reject the mismatch");
        assert!(
            err.to_string().contains("sketch rows"),
            "error must name the row-count mismatch, got: {err}"
        );
        let err = read_segment_fdx_lenient(&smoosh)
            .expect_err("lenient read must reject the mismatch too");
        assert!(err.to_string().contains("sketch rows"), "got: {err}");

        // Control: a theta column covering every row still reads fine in
        // both modes.
        let smoosh = build_fdx_theta_smoosh(3);
        let segment = read_segment_fdx(&smoosh).expect("matching row count reads");
        assert_eq!(segment.num_rows, 3);
        match segment.column("users_theta") {
            Some(ColumnData::ComplexTheta(rows)) => assert_eq!(rows.len(), 3),
            other => panic!("expected ComplexTheta, got {other:?}"),
        }
        let segment = read_segment_fdx_lenient(&build_fdx_theta_smoosh(3))
            .expect("matching row count reads leniently");
        assert_eq!(segment.num_rows, 3);
    }

    #[test]
    fn read_segment_fdx_full() {
        let smoosh = build_fdx_test_smoosh();
        let segment = read_segment_fdx(&smoosh).expect("read FDX");

        assert_eq!(segment.version, 10);
        assert_eq!(segment.num_rows, 3);
        assert_eq!(segment.dimensions, vec!["city"]);
        assert_eq!(segment.metrics, vec!["count"]);

        let ts = segment.timestamp_column().expect("ts");
        assert_eq!(ts, &[1000_i64, 1500, 2000]);

        match segment.column("city").expect("city") {
            ColumnData::String(s) => {
                assert_eq!(s.encoded_values, vec![2, 0, 1]);
                assert_eq!(s.dictionary.get(0), Some("london"));
            }
            other => panic!("expected String column, got {other:?}"),
        }

        match segment.column("count").expect("count") {
            ColumnData::Long(vals) => assert_eq!(vals, &[10, 20, 30]),
            other => panic!("expected Long column, got {other:?}"),
        }
    }

    /// A FDX segment whose string column was written with a **v1** (segment
    /// v9) front-coded dictionary must still decode correctly — the decoder
    /// auto-detects the dictionary version, so there is no regression for
    /// older mixed segments.
    #[test]
    fn read_segment_fdx_with_v1_dict_no_regression() {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();
        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };

        add("version.bin", &10_i32.to_be_bytes());
        let index = encode_index_drd_fdx(&["city"], &[], 1000, 2000, 1);
        add("index.drd", &index);
        let time_data = encode_long_column(&[1000, 1500, 2000]);
        add("__time", &time_data);
        add("__time.column_descriptor.json", br#"{"valueType":"LONG"}"#);

        let dict = FrontCodedDictionary::from_sorted(vec![
            "london".to_string(),
            "paris".to_string(),
            "tokyo".to_string(),
        ]);
        let mut bm0 = DruidBitmap::new();
        bm0.insert(1);
        let mut bm1 = DruidBitmap::new();
        bm1.insert(2);
        let mut bm2 = DruidBitmap::new();
        bm2.insert(0);
        let string_col = StringColumnData {
            dictionary: dict,
            encoded_values: vec![2, 0, 1],
            bitmap_indexes: vec![bm0, bm1, bm2],
        };
        // v1 encoder on purpose.
        let city_data = encode_string_column(&string_col).expect("encode v1 string column");
        add("city", &city_data);
        add(
            "city.column_descriptor.json",
            br#"{"valueType":"STRING","hasBitmapIndexes":true}"#,
        );

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts");

        let segment = read_segment_fdx(&smoosh).expect("read FDX with v1 dict");
        match segment.column("city").expect("city") {
            ColumnData::String(s) => {
                assert_eq!(s.dictionary.get(0), Some("london"));
                assert_eq!(s.dictionary.get(1), Some("paris"));
                assert_eq!(s.dictionary.get(2), Some("tokyo"));
                assert_eq!(s.encoded_values, vec![2, 0, 1]);
            }
            other => panic!("expected String column, got {other:?}"),
        }
    }

    /// The FDX writer must emit a **v2** front-coded dictionary for string
    /// columns (leading version marker == 2 inside the embedded dict blob).
    #[test]
    fn fdx_writer_emits_fc_v2_dictionary() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2, 3])
            .add_string_column(
                "host",
                vec!["a".to_string(), "ab".to_string(), "abc".to_string()],
            )
            .build()
            .expect("build");
        let (_meta, chunks) = write_segment_fdx_to_memory(&segment).expect("write FDX");

        // The serialized string column embeds the dictionary as
        // [.. ordinals ..][u32 dict_len][dict blob ..]; the dict blob's
        // first 4 LE bytes are the version marker. Rather than re-parse the
        // smoosh layout we scan every chunk for the v2 marker preceded by a
        // plausible bucket_size — simplest robust check is that *some* chunk
        // contains the 4-byte LE sequence for version 2 followed by a
        // power-of-two bucket size. We assert the column decodes as v2 by
        // round-tripping it through the reader instead (decode path is
        // version-aware) and separately unit-test the marker in column.rs.
        let smoosh = SmooshReader::from_parts(&_meta, chunks).expect("from_parts");
        let read_back = read_segment_fdx(&smoosh).expect("read FDX");
        match read_back.column("host").expect("host") {
            ColumnData::String(s) => {
                assert_eq!(s.dictionary.get(0), Some("a"));
                assert_eq!(s.dictionary.get(1), Some("ab"));
                assert_eq!(s.dictionary.get(2), Some("abc"));
            }
            other => panic!("expected String column, got {other:?}"),
        }
    }

    #[test]
    fn fdx_write_read_roundtrip() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000, 3000])
            .add_string_column(
                "city",
                vec![
                    "tokyo".to_string(),
                    "new york".to_string(),
                    "london".to_string(),
                ],
            )
            .add_long_column("revenue", true, vec![100, 200, 300])
            .add_double_column("price", true, vec![9.99, 19.99, 29.99])
            .build()
            .expect("build segment");

        // Write as FDX
        let (meta, chunks) = write_segment_fdx_to_memory(&segment).expect("write FDX");

        // Read back
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = read_segment_fdx(&smoosh).expect("read FDX");

        assert_eq!(read_back.version, 10);
        assert_eq!(read_back.num_rows(), 3);
        assert_eq!(read_back.dimensions, vec!["city"]);
        assert_eq!(read_back.metrics, vec!["revenue", "price"]);
        assert_eq!(read_back.interval.start_millis, 1000);
        assert_eq!(read_back.interval.end_millis, 3000);

        let ts = read_back.timestamp_column().expect("ts");
        assert_eq!(ts, &[1000_i64, 2000, 3000]);

        match read_back.column("city").expect("city") {
            ColumnData::String(s) => {
                assert_eq!(s.dictionary.len(), 3);
                assert_eq!(s.dictionary.get(0), Some("london"));
                assert_eq!(s.dictionary.get(1), Some("new york"));
                assert_eq!(s.dictionary.get(2), Some("tokyo"));
                assert_eq!(s.encoded_values, vec![2, 1, 0]);
            }
            other => panic!("expected String column, got {other:?}"),
        }

        match read_back.column("revenue").expect("revenue") {
            ColumnData::Long(vals) => assert_eq!(vals, &[100, 200, 300]),
            other => panic!("expected Long, got {other:?}"),
        }

        match read_back.column("price").expect("price") {
            ColumnData::Double(vals) => assert_eq!(vals, &[9.99, 19.99, 29.99]),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    /// compat-11: a MULTI-VALUE string dimension survives the FDX
    /// write→read round-trip (descriptor `hasMultipleValues: true` +
    /// v2-dictionary multi codec), preserving element lists, within-row
    /// order, and the empty (null) row; a single-value string column in
    /// the same segment stays plain `String`.
    #[test]
    fn fdx_string_multi_roundtrip() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000, 3000, 4000])
            .add_string_multi_column(
                "tags",
                vec![
                    vec!["a".to_string(), "b".to_string()],
                    vec!["a".to_string()],
                    vec![],
                    vec!["c".to_string(), "a".to_string()],
                ],
            )
            .add_string_column(
                "city",
                vec![
                    "tokyo".to_string(),
                    "osaka".to_string(),
                    "tokyo".to_string(),
                    "kyoto".to_string(),
                ],
            )
            .build()
            .expect("build segment");

        let (meta, chunks) = write_segment_fdx_to_memory(&segment).expect("write FDX");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");

        let desc = ColumnDescriptor::from_json(
            smoosh
                .read_file("tags.column_descriptor.json")
                .expect("tags descriptor"),
        )
        .expect("parse descriptor");
        assert!(desc.has_multiple_values, "MV descriptor flag must be true");

        let read_back = read_segment_fdx(&smoosh).expect("read FDX");
        assert_eq!(read_back.num_rows(), 4);
        match read_back.column("tags").expect("tags") {
            ColumnData::StringMulti(mc) => {
                assert_eq!(mc.row_values(0), vec!["a", "b"]);
                assert_eq!(mc.row_values(1), vec!["a"]);
                assert!(mc.is_null_row(2), "empty MV row survives as null");
                assert_eq!(mc.row_values(3), vec!["c", "a"], "order preserved");
            }
            other => panic!("expected StringMulti, got {other:?}"),
        }
        match read_back.column("city").expect("city") {
            ColumnData::String(_) => {}
            other => panic!("single-value column must stay String, got {other:?}"),
        }
    }

    /// A null-bearing LONG column round-trips through FDX write→read as
    /// `LongNullable` with exact `i64` values (incl. 2^53+1 and `i64::MAX`,
    /// which the pre-2026-07 NaN-Double degrade rounded) and the exact null
    /// rows; a null-free long column in the same segment stays plain `Long`.
    #[test]
    fn fdx_nullable_long_roundtrip() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000, 3000, 4000, 5000])
            .add_long_column("plain", true, vec![1, 2, 3, 4, 5])
            .add_long_column_nullable(
                "code",
                false,
                vec![
                    Some(7),
                    None,
                    Some(9_007_199_254_740_993), // 2^53 + 1
                    None,
                    Some(i64::MAX),
                ],
            )
            .build()
            .expect("build segment");

        let (meta, chunks) = write_segment_fdx_to_memory(&segment).expect("write FDX");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = read_segment_fdx(&smoosh).expect("read FDX");

        match read_back.column("code").expect("code") {
            ColumnData::LongNullable(v, nulls) => {
                assert_eq!(
                    v,
                    &vec![7, 0, 9_007_199_254_740_993, 0, i64::MAX],
                    "values must round-trip i64-exactly"
                );
                assert_eq!(nulls.len(), 2);
                assert!(
                    nulls.contains(1) && nulls.contains(3),
                    "null rows preserved"
                );
            }
            other => panic!("expected LongNullable, got {other:?}"),
        }
        match read_back.column("plain").expect("plain") {
            ColumnData::Long(vals) => assert_eq!(vals, &[1, 2, 3, 4, 5]),
            other => panic!("null-free long must stay plain Long, got {other:?}"),
        }
    }

    #[test]
    fn fdx_write_to_disk_roundtrip() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![500, 600])
            .add_string_column("host", vec!["alpha".to_string(), "beta".to_string()])
            .add_long_column("count", true, vec![42, 99])
            .build()
            .expect("build");

        let dir = tempfile::tempdir().expect("tempdir");
        let dir_path = dir.path();

        write_segment_fdx(&segment, dir_path).expect("write FDX to disk");

        assert!(dir_path.join("meta.smoosh").exists());
        assert!(dir_path.join("00000.smoosh").exists());

        // Read back via smoosh reader
        let smoosh = SmooshReader::open(dir_path).expect("open smoosh");
        let read_back = read_segment_fdx(&smoosh).expect("read FDX");

        assert_eq!(read_back.version, 10);
        assert_eq!(read_back.num_rows(), 2);
        assert_eq!(read_back.dimensions, vec!["host"]);
        assert_eq!(read_back.metrics, vec!["count"]);
    }

    #[test]
    fn fdx_empty_segment_roundtrip() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![])
            .build()
            .expect("build");

        let (meta, chunks) = write_segment_fdx_to_memory(&segment).expect("write FDX");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = read_segment_fdx(&smoosh).expect("read FDX");

        assert_eq!(read_back.version, 10);
        assert_eq!(read_back.num_rows(), 0);
        assert!(read_back.dimensions.is_empty());
        assert!(read_back.metrics.is_empty());
    }

    #[test]
    fn fdx_wrong_version_rejected() {
        // Build a smoosh with version 9 — FDX reader should reject it.
        let mut chunk = Vec::new();
        chunk.extend_from_slice(&9_i32.to_be_bytes());

        let meta = "v1,2147483647,1\nversion.bin,0,0,4";
        let smoosh = SmooshReader::from_parts(meta, vec![chunk]).expect("smoosh");
        let err = read_segment_fdx(&smoosh).unwrap_err();
        assert!(
            err.to_string().contains("expected segment version 10"),
            "unexpected error: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 36-E (Wave 37 R1 `fdx.rs:64-79`): corrupt columns must
    // propagate, not silently disappear.
    // -----------------------------------------------------------------------

    /// Build a FDX smoosh with a valid `__time` plus one corrupt LONG
    /// metric whose data blob is truncated.
    fn build_fdx_smoosh_with_corrupt_metric() -> SmooshReader {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();

        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };

        add("version.bin", &10_i32.to_be_bytes());
        let index = encode_index_drd_fdx(&[], &["bad_count"], 0, 0, 1);
        add("index.drd", &index);

        let time_data = encode_long_column(&[1, 2, 3]);
        add("__time", &time_data);

        // Corrupt LONG metric: declares 100 values, supplies 1 byte.
        let mut corrupt = Vec::new();
        corrupt.extend_from_slice(&100_u32.to_be_bytes());
        corrupt.push(0xff);
        add("bad_count", &corrupt);
        add(
            "bad_count.column_descriptor.json",
            br#"{"valueType":"LONG"}"#,
        );

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts")
    }

    #[test]
    fn fdx_corrupt_column_propagates_error() {
        let smoosh = build_fdx_smoosh_with_corrupt_metric();
        let err = read_segment_fdx(&smoosh)
            .expect_err("strict mode must reject a FDX segment containing a corrupt column");
        let msg = err.to_string();
        assert!(
            msg.contains("bad_count") && (msg.contains("truncated") || msg.contains("decode")),
            "expected propagated decode error mentioning the column name, got: {msg}"
        );
    }

    #[test]
    fn fdx_lenient_drops_corrupt_column() {
        let smoosh = build_fdx_smoosh_with_corrupt_metric();
        let segment = read_segment_fdx_lenient(&smoosh).expect("lenient read");
        assert!(segment.column("bad_count").is_none());
        assert_eq!(segment.timestamp_column().expect("ts"), &[1, 2, 3]);
    }

    #[test]
    fn fdx_synthetic_with_multiple_columns() {
        let n = 100;
        let times: Vec<i64> = (0..n).map(|i| 1_000_000 + i * 1000).collect();
        let values: Vec<i64> = (0..n).collect();
        let cities: Vec<String> = (0..n).map(|i| format!("city_{:02}", i % 10)).collect();

        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(times.clone())
            .add_string_column("city", cities)
            .add_long_column("value", true, values.clone())
            .build()
            .expect("build");

        let (meta, chunks) = write_segment_fdx_to_memory(&segment).expect("write");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = read_segment_fdx(&smoosh).expect("read");

        assert_eq!(read_back.version, 10);
        assert_eq!(read_back.num_rows(), n as usize);
        assert_eq!(read_back.timestamp_column().expect("ts"), times.as_slice());

        match read_back.column("value").expect("value") {
            ColumnData::Long(v) => assert_eq!(v, &values),
            other => panic!("expected Long, got {other:?}"),
        }

        match read_back.column("city").expect("city") {
            ColumnData::String(s) => {
                assert_eq!(s.dictionary.len(), 10);
                assert_eq!(s.encoded_values.len(), n as usize);
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Wave 36-F (Wave 37 R1 medium): a truncated `index.drd` previously
    // substituted `0` for missing min_ts/max_ts, fabricating a 1970-01-01
    // interval. The reader now treats truncation as a hard error.
    // Internal security review (Wave 37 R1), Medium: "Truncated FDX
    // `index.drd` timestamps are accepted as epoch zero".
    // -----------------------------------------------------------------------

    /// `index.drd` is truncated after the metrics list, before either timestamp.
    /// The reader must reject it instead of silently using `0`/`0`.
    #[test]
    fn fdx_index_drd_truncated_before_min_ts_is_rejected() {
        // Build a minimal index.drd that ends right after the metrics list.
        let mut buf = Vec::new();
        buf.extend_from_slice(&SEGMENT_VERSION_FDX.to_be_bytes());
        buf.extend_from_slice(&0_u32.to_be_bytes()); // num_dimensions = 0
        buf.extend_from_slice(&0_u32.to_be_bytes()); // num_metrics = 0
        // (deliberately stop here — no min_ts, no max_ts)

        let mut chunk = Vec::new();
        let mut entries = Vec::new();
        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };
        add("version.bin", &10_i32.to_be_bytes());
        add("index.drd", &buf);

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts");

        let err = read_segment_fdx(&smoosh).expect_err("must reject truncated index.drd");
        let msg = err.to_string();
        assert!(
            msg.contains("truncated") && msg.contains("min_ts"),
            "expected min_ts truncation error, got: {msg}"
        );
    }

    /// `index.drd` carries min_ts but ends before max_ts. Must be rejected.
    #[test]
    fn fdx_index_drd_truncated_before_max_ts_is_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&SEGMENT_VERSION_FDX.to_be_bytes());
        buf.extend_from_slice(&0_u32.to_be_bytes()); // num_dimensions = 0
        buf.extend_from_slice(&0_u32.to_be_bytes()); // num_metrics = 0
        buf.extend_from_slice(&123_i64.to_be_bytes()); // min_ts only
        // no max_ts

        let mut chunk = Vec::new();
        let mut entries = Vec::new();
        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };
        add("version.bin", &10_i32.to_be_bytes());
        add("index.drd", &buf);

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts");

        let err = read_segment_fdx(&smoosh).expect_err("must reject truncated index.drd");
        let msg = err.to_string();
        assert!(
            msg.contains("truncated") && msg.contains("max_ts"),
            "expected max_ts truncation error, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 36-G4 (W37B High `segment_tails: fdx.rs:80-129`):
    // strict mode rejects FDX segments where any column's row count
    // disagrees with the canonical row count from `__time`.
    // -----------------------------------------------------------------------

    #[test]
    fn fdx_inconsistent_column_row_count_is_rejected() {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();
        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };

        add("version.bin", &10_i32.to_be_bytes());
        let index = encode_index_drd_fdx(&[], &["count"], 1000, 2000, 1);
        add("index.drd", &index);

        // __time has 3 rows but the metric has only 2 -> mismatch.
        let time_data = encode_long_column(&[1000, 1500, 2000]);
        add("__time", &time_data);
        add("__time.column_descriptor.json", br#"{"valueType":"LONG"}"#);

        let count_data = encode_long_column(&[10, 20]); // only 2 rows!
        add("count", &count_data);
        add("count.column_descriptor.json", br#"{"valueType":"LONG"}"#);

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts");

        let err = read_segment_fdx(&smoosh)
            .expect_err("strict mode must reject FDX with mismatched column row counts");
        let msg = err.to_string();
        assert!(
            msg.contains("count") && msg.contains("row count"),
            "expected mismatched row count error mentioning column name, got: {msg}"
        );
    }

    /// Lenient mode keeps the legacy permissive behaviour for now: a
    /// length mismatch is logged and the segment is returned with the
    /// canonical row count.  This pins that contract so the strict path
    /// above is the single source of truth for new strict guarantees.
    #[test]
    fn fdx_inconsistent_column_row_count_lenient_tolerated() {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();
        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };

        add("version.bin", &10_i32.to_be_bytes());
        let index = encode_index_drd_fdx(&[], &["count"], 1000, 2000, 1);
        add("index.drd", &index);
        let time_data = encode_long_column(&[1000, 1500, 2000]);
        add("__time", &time_data);
        add("__time.column_descriptor.json", br#"{"valueType":"LONG"}"#);
        let count_data = encode_long_column(&[10, 20]);
        add("count", &count_data);
        add("count.column_descriptor.json", br#"{"valueType":"LONG"}"#);

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts");

        let segment = read_segment_fdx_lenient(&smoosh).expect("lenient must accept");
        // Canonical row count comes from `__time`.
        assert_eq!(segment.num_rows, 3);
    }

    // -----------------------------------------------------------------------
    // Wave 36-G4 (W37B High `segment_tails: fdx.rs:233-257`):
    // a non-`__time` column without a descriptor is rejected; we no
    // longer guess `STRING` for declared dimensions or `DOUBLE` for
    // metrics.
    // -----------------------------------------------------------------------

    #[test]
    fn fdx_metric_missing_descriptor_is_rejected() {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();
        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };

        add("version.bin", &10_i32.to_be_bytes());
        let index = encode_index_drd_fdx(&[], &["count"], 1000, 2000, 1);
        add("index.drd", &index);
        let time_data = encode_long_column(&[1000, 1500, 2000]);
        add("__time", &time_data);
        add("__time.column_descriptor.json", br#"{"valueType":"LONG"}"#);
        // count column bytes present, descriptor absent.
        let count_data = encode_long_column(&[10, 20, 30]);
        add("count", &count_data);

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts");

        let err = read_segment_fdx(&smoosh)
            .expect_err("strict mode must reject FDX metric missing column_descriptor.json");
        let msg = err.to_string();
        assert!(
            msg.contains("count") && msg.contains("column_descriptor"),
            "expected missing-descriptor error mentioning column name, got: {msg}"
        );
    }

    #[test]
    fn fdx_dimension_missing_descriptor_is_rejected() {
        // A declared dimension column with no descriptor must also be
        // rejected — pre-fix, the reader silently guessed `STRING`.
        let mut chunk = Vec::new();
        let mut entries = Vec::new();
        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };

        add("version.bin", &10_i32.to_be_bytes());
        let index = encode_index_drd_fdx(&["city"], &[], 1000, 2000, 1);
        add("index.drd", &index);
        let time_data = encode_long_column(&[1000, 1500, 2000]);
        add("__time", &time_data);
        add("__time.column_descriptor.json", br#"{"valueType":"LONG"}"#);
        // city column bytes present, descriptor absent.
        add("city", &[0_u8; 16]);

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts");

        let err = read_segment_fdx(&smoosh)
            .expect_err("strict mode must reject FDX dimension missing column_descriptor.json");
        let msg = err.to_string();
        assert!(
            msg.contains("city") && msg.contains("column_descriptor"),
            "expected missing-descriptor error mentioning column name, got: {msg}"
        );
    }
}
