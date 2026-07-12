// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid segment v9 reading logic.
//!
//! This module reads the internal files of a v9 smoosh archive and assembles
//! them into a [`crate::segment::SegmentData`].

use ferrodruid_common::error::{DruidError, Result};

use crate::column::{
    ColumnData, ColumnDescriptor, decode_double_column, decode_float_column, decode_long_column,
    decode_string_column,
};
use crate::segment::{Interval, SegmentData};
use crate::smoosh::SmooshReader;

use std::collections::HashMap;

/// Expected segment version for v9.
const SEGMENT_VERSION_V9: i32 = 9;

/// Hard upper bound on the number of dimension columns declared by a
/// single segment's `index.drd`.
///
/// Real-world Druid segments have on the order of 10s of dimensions; a few
/// hundred is exotic. We pick `16384` as a number that is 100-1000x larger
/// than realistic use yet small enough that allocating that many `String`
/// slots cannot OOM the process. Wave 35 R1 finding (`v9.rs:133`) showed
/// that an attacker-supplied `u32::MAX` value was being fed straight into
/// `Vec::with_capacity`. Tunable via perf testing if we ever observe a
/// legitimate segment that exceeds this cap.
const MAX_DIMENSIONS: usize = 16_384;

/// Hard upper bound on the number of metric columns declared by a single
/// segment's `index.drd`. Same rationale as [`MAX_DIMENSIONS`].
const MAX_METRICS: usize = 16_384;

/// Read a v9 segment from a [`SmooshReader`].
///
/// 1. Verifies `version.bin` contains `9`.
/// 2. Parses `index.drd` for dimensions, metrics, and timestamp info.
/// 3. Reads each column.  In **strict mode** (the default and only mode of
///    this entry point) any column whose decode fails — e.g. truncated
///    bytes, header that trips a Wave 36-D bounded-reader cap, missing
///    file inside the smoosh archive — surfaces as a `DruidError::Segment`
///    rather than being silently dropped.  Operators with on-disk
///    segments that pre-date the strict reader and need a one-time
///    migration window may use [`read_segment_v9_lenient`] instead.
/// 4. Returns the assembled [`SegmentData`].
///
/// Wave 36-E (Wave 37 R1 `v9.rs:46`): the previous `if let Ok(col) = …`
/// pattern silently dropped any column that failed to decode — a corrupt
/// segment yielded a partial `SegmentData` and consequently wrong query
/// results.  The behaviour is now fail-fast.
pub fn read_segment_v9(smoosh: &SmooshReader) -> Result<SegmentData> {
    read_segment_v9_inner(smoosh, /* lenient */ false)
}

/// Lenient counterpart of [`read_segment_v9`] that drops columns whose
/// decode fails instead of returning an error.  Each dropped column is
/// surfaced via `tracing::warn!` so an operator can detect corruption in
/// log inspection.  Should only be used during a migration window when
/// loading legacy segments written before the Wave 36-D bounded reader
/// landed; production code paths should use the strict
/// [`read_segment_v9`] entry point.
pub fn read_segment_v9_lenient(smoosh: &SmooshReader) -> Result<SegmentData> {
    read_segment_v9_inner(smoosh, /* lenient */ true)
}

/// Strict-vs-lenient column read.  Private; callers pick a mode via the
/// [`read_segment_v9`] / [`read_segment_v9_lenient`] entry points.
fn read_segment_v9_inner(smoosh: &SmooshReader, lenient: bool) -> Result<SegmentData> {
    // Step 1: version check.
    let version = read_version(smoosh)?;
    if version != SEGMENT_VERSION_V9 {
        return Err(DruidError::Segment(format!(
            "expected segment version {SEGMENT_VERSION_V9}, got {version}"
        )));
    }

    // Step 2: read index.drd.
    let index = read_index_drd(smoosh)?;

    // Step 3: read columns.  Strict by default — any decode failure
    // propagates.  Wave 36-E (Wave 37 R1 finding `v9.rs:46`).
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
                        "v9 segment: dropping column whose decode failed (lenient mode)"
                    );
                    Ok(())
                } else {
                    Err(DruidError::Segment(format!(
                        "v9 segment: failed to decode column `{name}`: {e}"
                    )))
                }
            }
        }
    };

    // Timestamp column.  Special case: if `__time` is entirely missing
    // from the smoosh archive and we are in strict mode, surface that as
    // an error.  In lenient mode keep the legacy behaviour of treating
    // an absent `__time` as a 0-row segment.
    if smoosh.has_file(time_col_name) {
        load(time_col_name, &mut columns)?;
    } else if !lenient {
        // Strict mode: a v9 segment without a __time column is malformed.
        // Defer the error to query time only when the operator explicitly
        // opted in to lenient loading.
        return Err(DruidError::Segment(
            "v9 segment: required column `__time` is missing from smoosh archive".to_string(),
        ));
    }

    // Dimension columns.
    for dim in &index.dimensions {
        load(dim, &mut columns)?;
    }

    // Metric columns.
    for metric in &index.metrics {
        load(metric, &mut columns)?;
    }

    // Determine num_rows from the timestamp column or any available column.
    let num_rows = columns
        .get(time_col_name)
        .and_then(|c| c.num_rows())
        .or_else(|| columns.values().find_map(|c| c.num_rows()))
        .unwrap_or(0);

    // Cache whether __time is sorted ascending so query-time interval pruning
    // can binary-search without an O(n) check per query. Druid (and FerroDruid
    // ingestion) write time-sorted segments, so this is normally `true`.
    let time_sorted = matches!(
        columns.get(time_col_name),
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

// ---------------------------------------------------------------------------
// version.bin
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

// ---------------------------------------------------------------------------
// index.drd
// ---------------------------------------------------------------------------

/// Parsed contents of `index.drd`.
struct IndexDrd {
    dimensions: Vec<String>,
    metrics: Vec<String>,
    interval: Interval,
}

/// Read and parse `index.drd`, auto-detecting the layout.
///
/// Two layouts are supported:
///
/// 1. **FerroDruid private layout** (what [`encode_index_drd`] and the
///    FerroDruid writer emit). Identified by a leading BE i32 == 9:
/// ```text
/// [4 bytes BE: version (9)]
/// [4 bytes BE: num_dimensions]
/// for each dimension: [2 bytes BE: name_len][name bytes]
/// [4 bytes BE: num_metrics]
/// for each metric: [2 bytes BE: name_len][name bytes]
/// [8 bytes BE: min_timestamp (epoch millis)]
/// [8 bytes BE: max_timestamp (epoch millis)]
/// [4 bytes BE: bitmap_serialization_type]
/// ```
///
/// 2. **Upstream Apache Druid layout** (what real Druid clusters write; see
///    [`crate::druid_native`] for the observed byte layout). Its first two
///    bytes are an indexed-container version/flags pair (`01 00`/`01 01`),
///    so the leading BE i32 is 0x0100_0000 or 0x0101_0000 — never 9 — which
///    makes the two layouts cleanly distinguishable.
fn read_index_drd(smoosh: &SmooshReader) -> Result<IndexDrd> {
    let data = smoosh.read_file("index.drd")?;
    let mut pos: usize = 0;

    // Layout auto-detect: the private layout leads with BE i32 == 9.
    let ver = read_be_i32(data, &mut pos)?;
    if ver != SEGMENT_VERSION_V9 {
        // Not the private layout — parse as a real Apache-Druid-written
        // `index.drd` (TG-1-finding-W2A-1-index-drd).
        let native =
            crate::druid_native::parse_native_index_drd(data, MAX_DIMENSIONS, MAX_METRICS)?;
        return Ok(IndexDrd {
            dimensions: native.dimensions,
            metrics: native.metrics,
            interval: native.interval,
        });
    }

    // Dimensions — bound the count BEFORE allocating to defeat OOM via a
    // crafted `index.drd`. See Wave 35 R1 (`v9.rs:133`): u32::MAX was being
    // passed straight to `Vec::with_capacity`.
    let num_dims = read_be_u32(data, &mut pos)? as usize;
    if num_dims > MAX_DIMENSIONS {
        return Err(DruidError::Segment(format!(
            "index.drd: num_dimensions {num_dims} exceeds cap {MAX_DIMENSIONS}"
        )));
    }
    let mut dimensions = Vec::with_capacity(num_dims);
    for _ in 0..num_dims {
        dimensions.push(read_length_prefixed_string(data, &mut pos)?);
    }

    // Metrics — same bound as dimensions.
    let num_metrics = read_be_u32(data, &mut pos)? as usize;
    if num_metrics > MAX_METRICS {
        return Err(DruidError::Segment(format!(
            "index.drd: num_metrics {num_metrics} exceeds cap {MAX_METRICS}"
        )));
    }
    let mut metrics = Vec::with_capacity(num_metrics);
    for _ in 0..num_metrics {
        metrics.push(read_length_prefixed_string(data, &mut pos)?);
    }

    // Timestamps (min, max)
    let min_ts = if pos + 8 <= data.len() {
        read_be_i64(data, &mut pos)?
    } else {
        0
    };
    let max_ts = if pos + 8 <= data.len() {
        read_be_i64(data, &mut pos)?
    } else {
        0
    };

    // bitmap_serialization_type (skip — we don't need it for the read path)
    // pos += 4 if available

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
// Column reading
// ---------------------------------------------------------------------------

/// Read a single column from the smoosh archive.
///
/// Looks for `<name>.column_descriptor.json` and `<name>` data files.
/// Columns written by upstream Apache Druid embed their descriptor inside
/// the column blob instead of using a sidecar smoosh entry; those are
/// detected and routed to [`crate::druid_native::decode_native_column`].
fn read_column(
    smoosh: &SmooshReader,
    name: &str,
    dimensions: &[String],
    _metrics: &[String],
) -> Result<ColumnData> {
    // Try to read the descriptor first.
    let desc_key = format!("{name}.column_descriptor.json");
    let descriptor = if smoosh.has_file(&desc_key) {
        let desc_data = smoosh.read_file(&desc_key)?;
        Some(ColumnDescriptor::from_json(desc_data)?)
    } else {
        None
    };

    // Read the column data blob.
    let data = smoosh.read_file(name)?;

    // Upstream-Druid column detection: the FerroDruid writer always emits
    // the sidecar descriptor, so a descriptor-less blob that leads with an
    // embedded length-prefixed JSON descriptor is a real Druid column.
    if descriptor.is_none() && crate::druid_native::is_native_column(data) {
        return crate::druid_native::decode_native_column(data);
    }

    // Determine the type from the descriptor or infer from the column name.
    let value_type = descriptor
        .as_ref()
        .map(|d| d.value_type.as_str())
        .unwrap_or_else(|| {
            if name == "__time" {
                "LONG"
            } else if dimensions.iter().any(|d| d == name) {
                "STRING"
            } else {
                "DOUBLE"
            }
        });

    match value_type {
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
        "STRING" => {
            let col = decode_string_column(data)?;
            Ok(ColumnData::String(col))
        }
        "COMPLEX" => Ok(ColumnData::Complex(data.to_vec())),
        other => Err(DruidError::Segment(format!(
            "unsupported column value type: {other}"
        ))),
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
// index.drd builder (for tests)
// ---------------------------------------------------------------------------

/// Encode an `index.drd` blob from components (for testing).
pub fn encode_index_drd(
    dimensions: &[&str],
    metrics: &[&str],
    min_ts: i64,
    max_ts: i64,
    bitmap_type: u32,
) -> Vec<u8> {
    let mut buf = Vec::new();

    // Version
    buf.extend_from_slice(&SEGMENT_VERSION_V9.to_be_bytes());

    // Dimensions
    buf.extend_from_slice(&(dimensions.len() as u32).to_be_bytes());
    for d in dimensions {
        let bytes = d.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(bytes);
    }

    // Metrics
    buf.extend_from_slice(&(metrics.len() as u32).to_be_bytes());
    for m in metrics {
        let bytes = m.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(bytes);
    }

    // Timestamps
    buf.extend_from_slice(&min_ts.to_be_bytes());
    buf.extend_from_slice(&max_ts.to_be_bytes());

    // Bitmap type
    buf.extend_from_slice(&bitmap_type.to_be_bytes());

    buf
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::{StringColumnData, encode_long_column, encode_string_column};
    use ferrodruid_bitmap::DruidBitmap;
    use ferrodruid_dict::FrontCodedDictionary;

    /// Build a minimal synthetic smoosh with version.bin, index.drd,
    /// a __time column, one dimension, and one metric.
    fn build_test_smoosh() -> SmooshReader {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();

        // Helper: append data to chunk and record the entry.
        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };

        // version.bin
        add("version.bin", &9_i32.to_be_bytes());

        // index.drd
        let index = encode_index_drd(&["city"], &["count"], 1000, 2000, 1);
        add("index.drd", &index);

        // __time column (LONG)
        let time_data = encode_long_column(&[1000, 1500, 2000]);
        add("__time", &time_data);

        // __time descriptor
        let time_desc = br#"{"valueType":"LONG"}"#;
        add("__time.column_descriptor.json", time_desc);

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
        let city_data = encode_string_column(&string_col).expect("encode string column");
        add("city", &city_data);

        let city_desc = br#"{"valueType":"STRING","hasBitmapIndexes":true}"#;
        add("city.column_descriptor.json", city_desc);

        // count column (LONG)
        let count_data = encode_long_column(&[10, 20, 30]);
        add("count", &count_data);

        let count_desc = br#"{"valueType":"LONG"}"#;
        add("count.column_descriptor.json", count_desc);

        // Build meta.smoosh
        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");

        SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts")
    }

    #[test]
    fn read_version_ok() {
        let smoosh = build_test_smoosh();
        let version = read_version(&smoosh).unwrap();
        assert_eq!(version, 9);
    }

    #[test]
    fn read_index_drd_parses_dims_and_metrics() {
        let smoosh = build_test_smoosh();
        let index = read_index_drd(&smoosh).unwrap();
        assert_eq!(index.dimensions, vec!["city"]);
        assert_eq!(index.metrics, vec!["count"]);
        assert_eq!(index.interval.start_millis, 1000);
        assert_eq!(index.interval.end_millis, 2000);
    }

    #[test]
    fn read_segment_v9_full() {
        let smoosh = build_test_smoosh();
        let segment = read_segment_v9(&smoosh).unwrap();

        assert_eq!(segment.version, 9);
        assert_eq!(segment.num_rows, 3);
        assert_eq!(segment.dimensions, vec!["city"]);
        assert_eq!(segment.metrics, vec!["count"]);

        // Check timestamp
        let ts = segment.timestamp_column().unwrap();
        assert_eq!(ts, &[1000_i64, 1500, 2000]);

        // Check city column
        match segment.column("city").unwrap() {
            ColumnData::String(s) => {
                assert_eq!(s.encoded_values, vec![2, 0, 1]);
                assert_eq!(s.dictionary.get(0), Some("london"));
                assert_eq!(s.dictionary.get(1), Some("paris"));
                assert_eq!(s.dictionary.get(2), Some("tokyo"));
                assert_eq!(s.bitmap_indexes.len(), 3);
            }
            other => panic!("expected String column, got {other:?}"),
        }

        // Check count column
        match segment.column("count").unwrap() {
            ColumnData::Long(vals) => assert_eq!(vals, &[10, 20, 30]),
            other => panic!("expected Long column, got {other:?}"),
        }
    }

    #[test]
    fn wrong_version_rejected() {
        // Build a smoosh with version 8 instead of 9.
        let mut chunk = Vec::new();
        chunk.extend_from_slice(&8_i32.to_be_bytes());

        let meta = "v1,2147483647,1\nversion.bin,0,0,4";
        let smoosh = SmooshReader::from_parts(meta, vec![chunk]).unwrap();
        assert!(read_segment_v9(&smoosh).is_err());
    }

    #[test]
    fn index_drd_version_mismatch() {
        let mut chunk = Vec::new();
        // version.bin OK
        let ver_start = chunk.len();
        chunk.extend_from_slice(&9_i32.to_be_bytes());
        let ver_end = chunk.len();

        // index.drd with bad internal version
        let idx_start = chunk.len();
        chunk.extend_from_slice(&8_i32.to_be_bytes()); // wrong version
        chunk.extend_from_slice(&0_u32.to_be_bytes()); // 0 dims
        chunk.extend_from_slice(&0_u32.to_be_bytes()); // 0 metrics
        let idx_end = chunk.len();

        let meta = format!(
            "v1,2147483647,2\nversion.bin,0,{ver_start},{ver_end}\nindex.drd,0,{idx_start},{idx_end}"
        );
        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).unwrap();
        assert!(read_segment_v9(&smoosh).is_err());
    }

    #[test]
    fn encode_index_drd_round_trip() {
        let data = encode_index_drd(&["dim1", "dim2"], &["metric1"], 500, 1500, 1);

        // Parse it back via the reader by wrapping in a smoosh.
        let mut chunk = Vec::new();
        // version.bin
        let vstart = chunk.len();
        chunk.extend_from_slice(&9_i32.to_be_bytes());
        let vend = chunk.len();
        // index.drd
        let istart = chunk.len();
        chunk.extend_from_slice(&data);
        let iend = chunk.len();

        let meta =
            format!("v1,2147483647,2\nversion.bin,0,{vstart},{vend}\nindex.drd,0,{istart},{iend}");
        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).unwrap();
        let index = read_index_drd(&smoosh).unwrap();
        assert_eq!(index.dimensions, vec!["dim1", "dim2"]);
        assert_eq!(index.metrics, vec!["metric1"]);
        assert_eq!(index.interval.start_millis, 500);
        assert_eq!(index.interval.end_millis, 1500);
    }

    // -----------------------------------------------------------------------
    // Wave 36-D / R1: bounded `index.drd` reader.
    // Internal security review (Wave 35 R1), High: "index.drd trusts
    // attacker-controlled dimension/metric counts".
    // -----------------------------------------------------------------------

    /// Helper: package a hand-crafted `index.drd` blob into a smoosh that
    /// also has a valid `version.bin`.
    fn smoosh_with_index_drd(index_bytes: &[u8]) -> SmooshReader {
        let mut chunk = Vec::new();
        let vstart = chunk.len();
        chunk.extend_from_slice(&9_i32.to_be_bytes());
        let vend = chunk.len();
        let istart = chunk.len();
        chunk.extend_from_slice(index_bytes);
        let iend = chunk.len();
        let meta =
            format!("v1,2147483647,2\nversion.bin,0,{vstart},{vend}\nindex.drd,0,{istart},{iend}");
        SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts")
    }

    #[test]
    fn oversized_dimension_count_is_rejected() {
        // Craft: version=9, num_dims=u32::MAX, then no dimension bytes.
        // The cap must trip before we attempt the per-dimension loop.
        let mut buf = Vec::new();
        buf.extend_from_slice(&9_i32.to_be_bytes()); // version
        buf.extend_from_slice(&u32::MAX.to_be_bytes()); // num_dims = 4 billion

        let smoosh = smoosh_with_index_drd(&buf);
        let result = read_index_drd(&smoosh);
        let msg = match result {
            Ok(_) => panic!("read_index_drd accepted u32::MAX dimensions"),
            Err(e) => format!("{e}"),
        };
        assert!(
            msg.contains("num_dimensions") && msg.contains("exceeds cap"),
            "expected num_dimensions cap error, got: {msg}"
        );
        // The crafted blob is tiny — no chance the rejection itself
        // allocated multiple MB.
        assert!(buf.len() < 64);
    }

    #[test]
    fn oversized_metric_count_is_rejected() {
        // Craft: version=9, num_dims=0, num_metrics=u32::MAX.
        let mut buf = Vec::new();
        buf.extend_from_slice(&9_i32.to_be_bytes()); // version
        buf.extend_from_slice(&0_u32.to_be_bytes()); // num_dims = 0
        buf.extend_from_slice(&u32::MAX.to_be_bytes()); // num_metrics

        let smoosh = smoosh_with_index_drd(&buf);
        let result = read_index_drd(&smoosh);
        let msg = match result {
            Ok(_) => panic!("read_index_drd accepted u32::MAX metrics"),
            Err(e) => format!("{e}"),
        };
        assert!(
            msg.contains("num_metrics") && msg.contains("exceeds cap"),
            "expected num_metrics cap error, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 36-E (Wave 37 R1 `v9.rs:46`): corrupt columns must propagate, not
    // silently disappear.  Mirrors the FDX fix in `crate::fdx`.
    // -----------------------------------------------------------------------

    /// Build a smoosh archive with a valid `__time` column plus one corrupt
    /// LONG metric whose data blob is truncated (declares 100 values but
    /// supplies a single byte).  `decode_long_column` rejects this with
    /// "long column truncated".
    fn build_v9_smoosh_with_corrupt_metric() -> SmooshReader {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();

        let mut add = |name: &str, data: &[u8]| {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            entries.push(format!("{name},0,{start},{end}"));
        };

        add("version.bin", &9_i32.to_be_bytes());
        let index = encode_index_drd(&[], &["bad_count"], 0, 0, 1);
        add("index.drd", &index);

        // Valid __time
        let time_data = encode_long_column(&[1, 2, 3]);
        add("__time", &time_data);

        // Corrupt LONG metric: header claims 100 values, data has 1 byte.
        let mut corrupt = Vec::new();
        corrupt.extend_from_slice(&100_u32.to_be_bytes());
        corrupt.push(0xff);
        add("bad_count", &corrupt);
        // Descriptor declares LONG so decode_long_column is invoked.
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
    fn v9_corrupt_column_propagates_error() {
        let smoosh = build_v9_smoosh_with_corrupt_metric();
        let err = read_segment_v9(&smoosh)
            .expect_err("strict mode must reject a segment containing a corrupt column");
        let msg = err.to_string();
        assert!(
            msg.contains("bad_count") && (msg.contains("truncated") || msg.contains("decode")),
            "expected propagated decode error mentioning the column name, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Real Apache-Druid-written `index.drd` (TG-1-finding-W2A-1-index-drd).
    //
    // The fixture bytes were captured verbatim from a segment written by
    // Apache Druid 31.0.2 for the `wikipedia_compat` sample dataset
    // (2024-01-01 partition, captured 2026-07-12).  The reader must parse
    // the upstream layout — NOT just FerroDruid's own private layout.
    // -----------------------------------------------------------------------

    /// Verbatim `index.drd` from a Druid-31.0.2-written segment.
    const REAL_DRUID31_INDEX_DRD: &[u8] = include_bytes!("../testdata/druid31_index.drd");

    #[test]
    fn read_index_drd_parses_real_druid31_bytes() {
        let smoosh = smoosh_with_index_drd(REAL_DRUID31_INDEX_DRD);
        let index = read_index_drd(&smoosh).expect("real Druid 31.0.2 index.drd must parse");
        assert_eq!(
            index.dimensions,
            vec!["page", "user", "language", "city", "namespace", "channel"]
        );
        assert_eq!(index.metrics, vec!["added", "count", "deleted", "delta"]);
        // 2024-01-01T00:00:00Z .. 2024-01-02T00:00:00Z
        assert_eq!(index.interval.start_millis, 1_704_067_200_000);
        assert_eq!(index.interval.end_millis, 1_704_153_600_000);
    }

    #[test]
    fn v9_lenient_drops_corrupt_column() {
        // Lenient mode preserves the legacy "drop and continue" behaviour
        // for one-time migration scenarios.  The corrupt column is absent
        // from the loaded segment but the load succeeds.
        let smoosh = build_v9_smoosh_with_corrupt_metric();
        let segment = read_segment_v9_lenient(&smoosh).expect("lenient read");
        assert!(segment.column("bad_count").is_none());
        // __time was valid and is preserved.
        assert_eq!(segment.timestamp_column().expect("ts"), &[1, 2, 3]);
    }
}
