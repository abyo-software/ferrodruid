// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! High-level segment reader API.
//!
//! [`SegmentData`] is the primary entry point for reading a Druid segment from
//! disk or from an in-memory smoosh archive.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use ferrodruid_bitmap::DruidBitmap;
use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_dict::FrontCodedDictionary;

use crate::column::{ColumnData, StringColumnData};
use crate::fdx::read_segment_fdx;
use crate::smoosh::SmooshReader;
use crate::v9::{read_segment_v9, read_segment_v9_lenient_report};

// ---------------------------------------------------------------------------
// Interval
// ---------------------------------------------------------------------------

/// Time interval for a segment (epoch milliseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    /// Inclusive start timestamp in epoch milliseconds.
    pub start_millis: i64,
    /// Inclusive end timestamp in epoch milliseconds.
    pub end_millis: i64,
}

// ---------------------------------------------------------------------------
// SegmentData
// ---------------------------------------------------------------------------

/// A fully-read Druid segment.
///
/// Contains the segment version, row count, time interval, dimension and metric
/// names, and all column data.
///
/// `Clone` is a deep copy of every column. It exists so a caller-supplied,
/// possibly-shared `Arc<SegmentData>` can be corrected copy-on-write at the
/// heap-load boundary (`Arc::make_mut`) without disturbing other holders of the
/// same `Arc`; a well-formed segment never triggers that clone (the corrected
/// flag equals the existing one), so the copy only ever pays for a pathological
/// caller-installed `time_sorted` lie on an already-shared segment.
#[derive(Debug, Clone)]
pub struct SegmentData {
    /// Segment format version (9 for v9).
    pub version: i32,
    /// Number of rows in the segment.
    pub num_rows: usize,
    /// Time interval covered by the segment.
    pub interval: Interval,
    /// Ordered list of dimension column names.
    pub dimensions: Vec<String>,
    /// Ordered list of metric column names.
    pub metrics: Vec<String>,
    /// Column data keyed by column name.
    pub columns: HashMap<String, ColumnData>,
    /// Whether the `__time` column is sorted ascending. Computed once at
    /// build/decode so query-time interval pruning can binary-search the
    /// timestamp range without an O(n) `is_sorted` check per query.
    pub time_sorted: bool,
}

impl SegmentData {
    /// Open and read a segment from a directory on disk.
    ///
    /// The directory must contain `meta.smoosh` and the corresponding chunk
    /// files.
    pub fn open(dir: &Path) -> Result<Self> {
        let smoosh = SmooshReader::open(dir)?;
        read_segment_v9(&smoosh)
    }

    /// LENIENT variant of [`SegmentData::open`]: a column whose decode
    /// fails (e.g. a complex/sketch column the v9 reader cannot read) is
    /// DROPPED instead of failing the whole segment, and the returned
    /// manifest names every dropped column so the caller can surface the
    /// loss loudly.  The dropped names remain listed in the segment's
    /// `dimensions` / `metrics` (the `index.drd` declaration is kept
    /// intact) but have no `columns` entry — a caller that re-persists
    /// the segment must prune those lists against the manifest first.
    /// Opt-in only: production load paths use the strict
    /// [`SegmentData::open`].
    pub fn open_lenient(dir: &Path) -> Result<(Self, Vec<String>)> {
        let smoosh = SmooshReader::open(dir)?;
        read_segment_v9_lenient_report(&smoosh)
    }

    /// Read a segment from a pre-opened [`SmooshReader`].
    ///
    /// Auto-detects the segment version (v9 or FDX) from `version.bin` and
    /// delegates to the appropriate reader.
    pub fn from_smoosh(smoosh: &SmooshReader) -> Result<Self> {
        let version = read_version_from_smoosh(smoosh)?;
        match version {
            9 => read_segment_v9(smoosh),
            10 => read_segment_fdx(smoosh),
            _ => Err(DruidError::Segment(format!(
                "unsupported segment version: {version}"
            ))),
        }
    }

    /// Look up a column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnData> {
        self.columns.get(name)
    }

    /// Number of rows in the segment.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Access the `__time` column as a slice of epoch-millis `i64` values.
    pub fn timestamp_column(&self) -> Result<&[i64]> {
        match self.columns.get("__time") {
            Some(ColumnData::Long(v)) => Ok(v),
            Some(_) => Err(DruidError::Segment(
                "__time column is not of type LONG".to_string(),
            )),
            None => Err(DruidError::Segment(
                "__time column not found in segment".to_string(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// SegmentDataBuilder
// ---------------------------------------------------------------------------

/// Builder for constructing a [`SegmentData`] programmatically.
///
/// String columns are automatically dictionary-encoded with sorted dictionaries
/// and per-value bitmap indexes.
pub struct SegmentDataBuilder {
    dimensions: Vec<String>,
    metrics: Vec<String>,
    columns: HashMap<String, ColumnData>,
    timestamps: Option<Vec<i64>>,
}

impl SegmentDataBuilder {
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self {
            dimensions: Vec::new(),
            metrics: Vec::new(),
            columns: HashMap::new(),
            timestamps: None,
        }
    }

    /// Add the `__time` timestamp column (epoch milliseconds).
    pub fn add_timestamp_column(mut self, times: Vec<i64>) -> Self {
        self.timestamps = Some(times);
        self
    }

    /// Add a `LONG` column. If `is_metric` is true it is registered as a metric,
    /// otherwise as a dimension.
    pub fn add_long_column(mut self, name: &str, is_metric: bool, values: Vec<i64>) -> Self {
        if is_metric {
            self.metrics.push(name.to_string());
        } else {
            self.dimensions.push(name.to_string());
        }
        self.columns
            .insert(name.to_string(), ColumnData::Long(values));
        self
    }

    /// Add a `DOUBLE` column. If `is_metric` is true it is registered as a
    /// metric, otherwise as a dimension.
    pub fn add_double_column(mut self, name: &str, is_metric: bool, values: Vec<f64>) -> Self {
        if is_metric {
            self.metrics.push(name.to_string());
        } else {
            self.dimensions.push(name.to_string());
        }
        self.columns
            .insert(name.to_string(), ColumnData::Double(values));
        self
    }

    /// Add a `STRING` dimension column.
    ///
    /// Automatically builds a sorted dictionary, ordinals, and per-value bitmaps.
    pub fn add_string_column(mut self, name: &str, values: Vec<String>) -> Self {
        self.dimensions.push(name.to_string());

        // Build sorted dictionary of unique values.
        let mut unique: BTreeMap<String, u32> = BTreeMap::new();
        for v in &values {
            let next_id = unique.len() as u32;
            unique.entry(v.clone()).or_insert(next_id);
        }

        // Sorted dictionary entries.
        let sorted_values: Vec<String> = unique.keys().cloned().collect();
        // Map from value to sorted ordinal.
        let ordinal_map: HashMap<&str, u32> = sorted_values
            .iter()
            .enumerate()
            .map(|(i, v)| (v.as_str(), i as u32))
            .collect();

        // Encode per-row ordinals.
        let encoded_values: Vec<u32> = values.iter().map(|v| ordinal_map[v.as_str()]).collect();

        // Build per-dictionary-value bitmaps.
        let num_unique = sorted_values.len();
        let mut bitmaps: Vec<DruidBitmap> = (0..num_unique).map(|_| DruidBitmap::new()).collect();
        for (row_idx, &ord) in encoded_values.iter().enumerate() {
            bitmaps[ord as usize].insert(row_idx as u32);
        }

        let dictionary = FrontCodedDictionary::from_sorted(sorted_values);

        let string_col = StringColumnData {
            dictionary,
            encoded_values,
            bitmap_indexes: bitmaps,
        };

        self.columns
            .insert(name.to_string(), ColumnData::String(string_col));
        self
    }

    /// Add a MULTI-VALUE `STRING` dimension column (compat-11).
    ///
    /// Each row is an ordered element list; an empty list stores the row
    /// as SQL NULL (the Druid `[]`/`null` equivalence).  Element order is
    /// preserved as given.  This always builds a
    /// [`crate::column::StringMultiColumnData`] — use
    /// [`Self::add_string_column`] for single-value dimensions, which keep
    /// their historical layout byte-for-byte.
    pub fn add_string_multi_column(mut self, name: &str, rows: Vec<Vec<String>>) -> Self {
        self.dimensions.push(name.to_string());
        let col = crate::column::StringMultiColumnData::from_rows(&rows);
        self.columns
            .insert(name.to_string(), ColumnData::StringMulti(col));
        self
    }

    /// Add a nullable `DOUBLE` column.  `None` entries are stored as SQL
    /// NULL using the in-band NaN marker
    /// ([`crate::column::NULL_DOUBLE`]).
    pub fn add_double_column_nullable(
        self,
        name: &str,
        is_metric: bool,
        values: Vec<Option<f64>>,
    ) -> Self {
        let vals: Vec<f64> = values
            .into_iter()
            .map(|v| v.unwrap_or(crate::column::NULL_DOUBLE))
            .collect();
        self.add_double_column(name, is_metric, vals)
    }

    /// Add a nullable `LONG` column.
    ///
    /// `i64` has no in-band NULL marker, so a null-bearing long column is
    /// stored as [`ColumnData::LongNullable`]: the exact `i64` values (with
    /// `0` at NULL rows) plus an explicit null-row bitmap — values beyond
    /// ±2^53 are preserved exactly.  A null-free input stays a true `LONG`
    /// column with the historical layout byte-for-byte.  (Before 2026-07
    /// the null-bearing case degraded to a NaN-null `DOUBLE`, silently
    /// losing precision beyond ±2^53.)
    pub fn add_long_column_nullable(
        mut self,
        name: &str,
        is_metric: bool,
        values: Vec<Option<i64>>,
    ) -> Self {
        if values.iter().all(Option::is_some) {
            let vals: Vec<i64> = values.into_iter().flatten().collect();
            self.add_long_column(name, is_metric, vals)
        } else {
            let (vals, nulls) = crate::column::long_nullable_parts(&values);
            if is_metric {
                self.metrics.push(name.to_string());
            } else {
                self.dimensions.push(name.to_string());
            }
            self.columns
                .insert(name.to_string(), ColumnData::LongNullable(vals, nulls));
            self
        }
    }

    /// Add a nullable `STRING` dimension column.  `None` entries are stored
    /// as SQL NULL — distinct from `""` — via the trailing null-row bitmap
    /// (see [`StringColumnData::null_rows`]).
    pub fn add_string_column_nullable(mut self, name: &str, values: Vec<Option<String>>) -> Self {
        self.dimensions.push(name.to_string());
        let string_col = StringColumnData::from_nullable_values(&values);
        self.columns
            .insert(name.to_string(), ColumnData::String(string_col));
        self
    }

    /// Build the [`SegmentData`].
    ///
    /// The interval is derived from the timestamp column (min/max). If no
    /// timestamp column was added, the interval defaults to `0..0`.
    pub fn build(self) -> Result<SegmentData> {
        let mut columns = self.columns;

        let (min_ts, max_ts, num_rows, time_sorted) = if let Some(times) = self.timestamps {
            let min = times.iter().copied().min().unwrap_or(0);
            let max = times.iter().copied().max().unwrap_or(0);
            let n = times.len();
            let sorted = times.is_sorted();
            columns.insert("__time".to_string(), ColumnData::Long(times));
            (min, max, n, sorted)
        } else {
            // Infer num_rows from any column.
            let nr = columns.values().find_map(|c| c.num_rows()).unwrap_or(0);
            (0, 0, nr, false)
        };

        Ok(SegmentData {
            version: 9,
            num_rows,
            interval: Interval {
                start_millis: min_ts,
                end_millis: max_ts,
            },
            dimensions: self.dimensions,
            metrics: self.metrics,
            columns,
            time_sorted,
        })
    }
}

impl Default for SegmentDataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Version auto-detection helper
// ---------------------------------------------------------------------------

/// Read the version from `version.bin` without consuming the smoosh.
fn read_version_from_smoosh(smoosh: &SmooshReader) -> Result<i32> {
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::{ColumnData, StringColumnData, encode_long_column, encode_string_column};
    use crate::fdx::encode_index_drd_fdx;
    use crate::v9::encode_index_drd;
    use ferrodruid_bitmap::DruidBitmap;
    use ferrodruid_dict::FrontCodedDictionary;

    /// Build a minimal smoosh and read it via `SegmentData::from_smoosh`.
    #[test]
    fn segment_data_from_smoosh() {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();

        let add = |name: &str, data: &[u8], c: &mut Vec<u8>, e: &mut Vec<String>| {
            let start = c.len();
            c.extend_from_slice(data);
            let end = c.len();
            e.push(format!("{name},0,{start},{end}"));
        };

        add(
            "version.bin",
            &9_i32.to_be_bytes(),
            &mut chunk,
            &mut entries,
        );

        let index = encode_index_drd(&["region"], &["value"], 100, 200, 1);
        add("index.drd", &index, &mut chunk, &mut entries);

        let time_data = encode_long_column(&[100, 150, 200]);
        add("__time", &time_data, &mut chunk, &mut entries);
        add(
            "__time.column_descriptor.json",
            br#"{"valueType":"LONG"}"#,
            &mut chunk,
            &mut entries,
        );

        // region = STRING
        let dict = FrontCodedDictionary::from_sorted(vec!["eu".to_string(), "us".to_string()]);
        let mut bm0 = DruidBitmap::new();
        bm0.insert(0);
        let mut bm1 = DruidBitmap::new();
        bm1.insert(1);
        bm1.insert(2);
        let sc = StringColumnData {
            dictionary: dict,
            encoded_values: vec![0, 1, 1],
            bitmap_indexes: vec![bm0, bm1],
        };
        let region_bytes = encode_string_column(&sc).expect("encode");
        add("region", &region_bytes, &mut chunk, &mut entries);
        add(
            "region.column_descriptor.json",
            br#"{"valueType":"STRING","hasBitmapIndexes":true}"#,
            &mut chunk,
            &mut entries,
        );

        // value = DOUBLE
        let val_bytes = crate::column::encode_double_column(&[1.1, 2.2, 3.3]);
        add("value", &val_bytes, &mut chunk, &mut entries);
        add(
            "value.column_descriptor.json",
            br#"{"valueType":"DOUBLE"}"#,
            &mut chunk,
            &mut entries,
        );

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");

        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("smoosh");
        let seg = SegmentData::from_smoosh(&smoosh).expect("segment");

        assert_eq!(seg.version, 9);
        assert_eq!(seg.num_rows(), 3);
        assert_eq!(seg.dimensions, vec!["region"]);
        assert_eq!(seg.metrics, vec!["value"]);

        let ts = seg.timestamp_column().unwrap();
        assert_eq!(ts, &[100_i64, 150, 200]);

        assert!(matches!(seg.column("region"), Some(ColumnData::String(_))));
        assert!(matches!(seg.column("value"), Some(ColumnData::Double(_))));
        assert!(seg.column("nonexistent").is_none());
    }

    #[test]
    fn timestamp_column_missing_rejected_strict() {
        // Wave 36-E: a segment with no `__time` column is now rejected at
        // load time in strict mode (the only public mode of `from_smoosh`).
        // Pre-Wave-36-E this was loaded with an empty column map and the
        // failure deferred to `timestamp_column()` query-time.  The new
        // behaviour fails fast with a clear error.
        let mut chunk = Vec::new();
        let mut entries = Vec::new();

        let add = |name: &str, data: &[u8], c: &mut Vec<u8>, e: &mut Vec<String>| {
            let start = c.len();
            c.extend_from_slice(data);
            let end = c.len();
            e.push(format!("{name},0,{start},{end}"));
        };

        add(
            "version.bin",
            &9_i32.to_be_bytes(),
            &mut chunk,
            &mut entries,
        );
        let index = encode_index_drd(&[], &[], 0, 0, 1);
        add("index.drd", &index, &mut chunk, &mut entries);

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");

        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("smoosh");
        let err =
            SegmentData::from_smoosh(&smoosh).expect_err("strict mode rejects missing __time");
        let msg = err.to_string();
        assert!(
            msg.contains("__time") && msg.contains("missing"),
            "expected missing-__time error, got: {msg}"
        );
    }

    /// Auto-detect v9 via `SegmentData::from_smoosh`.
    #[test]
    fn auto_detect_v9() {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();

        let add = |name: &str, data: &[u8], c: &mut Vec<u8>, e: &mut Vec<String>| {
            let start = c.len();
            c.extend_from_slice(data);
            let end = c.len();
            e.push(format!("{name},0,{start},{end}"));
        };

        add(
            "version.bin",
            &9_i32.to_be_bytes(),
            &mut chunk,
            &mut entries,
        );
        let index = encode_index_drd(&[], &[], 0, 0, 1);
        add("index.drd", &index, &mut chunk, &mut entries);
        // Wave 36-E: strict reader requires `__time`.  Add a minimal
        // (empty) timestamp column so version auto-detection still works.
        let time_data = encode_long_column(&[]);
        add("__time", &time_data, &mut chunk, &mut entries);

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");

        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("smoosh");
        let seg = SegmentData::from_smoosh(&smoosh).expect("segment");
        assert_eq!(seg.version, 9);
    }

    /// Auto-detect FDX via `SegmentData::from_smoosh`.
    #[test]
    fn auto_detect_fdx() {
        let mut chunk = Vec::new();
        let mut entries = Vec::new();

        let add = |name: &str, data: &[u8], c: &mut Vec<u8>, e: &mut Vec<String>| {
            let start = c.len();
            c.extend_from_slice(data);
            let end = c.len();
            e.push(format!("{name},0,{start},{end}"));
        };

        add(
            "version.bin",
            &10_i32.to_be_bytes(),
            &mut chunk,
            &mut entries,
        );
        let index = encode_index_drd_fdx(&[], &[], 0, 0, 1);
        add("index.drd", &index, &mut chunk, &mut entries);
        // Wave 36-E: strict reader requires `__time`.
        let time_data = encode_long_column(&[]);
        add("__time", &time_data, &mut chunk, &mut entries);

        let header = format!("v1,2147483647,{}", entries.len());
        let meta = std::iter::once(header.as_str())
            .chain(entries.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
            .join("\n");

        let smoosh = SmooshReader::from_parts(&meta, vec![chunk]).expect("smoosh");
        let seg = SegmentData::from_smoosh(&smoosh).expect("segment");
        assert_eq!(seg.version, 10);
    }

    // -----------------------------------------------------------------------
    // Nullable builder methods (2026-07-11 null-faithful ingestion)
    // -----------------------------------------------------------------------

    /// `add_double_column_nullable` stores `None` as NaN; `add_long_column_nullable`
    /// keeps LONG when null-free and falls back to DOUBLE+NaN when nulls exist;
    /// `add_string_column_nullable` carries the trailing null-row bitmap.
    #[test]
    fn builder_nullable_columns() {
        use crate::column::is_null_double;

        let seg = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000, 3000])
            .add_string_column_nullable(
                "device",
                vec![Some("d1".to_string()), None, Some("d2".to_string())],
            )
            .add_double_column_nullable("value", true, vec![Some(1.5), None, Some(2.5)])
            .add_long_column_nullable("code_dense", true, vec![Some(1), Some(2), Some(3)])
            .add_long_column_nullable("code_sparse", true, vec![Some(1), None, Some(3)])
            .build()
            .expect("build");

        match seg.column("device") {
            Some(ColumnData::String(sc)) => {
                assert!(sc.is_null_row(1));
                assert!(!sc.is_null_row(0));
                assert_eq!(
                    sc.null_rows().map(ferrodruid_bitmap::DruidBitmap::len),
                    Some(1)
                );
            }
            other => panic!("expected STRING, got {other:?}"),
        }
        match seg.column("value") {
            Some(ColumnData::Double(v)) => {
                assert_eq!(v[0], 1.5);
                assert!(is_null_double(v[1]));
                assert_eq!(v[2], 2.5);
            }
            other => panic!("expected DOUBLE, got {other:?}"),
        }
        assert!(matches!(
            seg.column("code_dense"),
            Some(ColumnData::Long(_))
        ));
        match seg.column("code_sparse") {
            Some(ColumnData::LongNullable(v, nulls)) => {
                assert_eq!(v, &[1, 0, 3], "exact i64 values, 0 at the NULL row");
                assert_eq!(nulls.len(), 1);
                assert!(nulls.contains(1), "row 1 must be NULL");
            }
            other => panic!("null-bearing long column must be LongNullable, got {other:?}"),
        }

        // Write→read round-trip (v9) preserves all null information.
        let (meta, chunks) = crate::writer::write_segment_v9_to_memory(&seg).expect("write");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("smoosh");
        let read_back = SegmentData::from_smoosh(&smoosh).expect("read");
        match read_back.column("device") {
            Some(ColumnData::String(sc)) => assert!(sc.is_null_row(1)),
            other => panic!("expected STRING, got {other:?}"),
        }
        match read_back.column("value") {
            Some(ColumnData::Double(v)) => assert!(is_null_double(v[1])),
            other => panic!("expected DOUBLE, got {other:?}"),
        }
        match read_back.column("code_sparse") {
            Some(ColumnData::LongNullable(v, nulls)) => {
                assert_eq!(v, &[1, 0, 3]);
                assert!(nulls.contains(1));
            }
            other => panic!("expected LongNullable after round-trip, got {other:?}"),
        }
    }

    /// On-disk lenient open: a directory whose archive carries one
    /// undecodable column strict-fails via [`SegmentData::open`]
    /// (unchanged) but opens via [`SegmentData::open_lenient`] with the
    /// dropped-column manifest, and the surviving columns intact.
    #[test]
    fn open_lenient_on_disk_reports_dropped_column() {
        // A valid segment (via the real writer) ...
        let seg = SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200, 300])
            .add_string_column("region", vec!["a".into(), "b".into(), "b".into()])
            .add_double_column("value", true, vec![1.0, 2.0, 3.0])
            .build()
            .expect("build");
        let (meta, chunks) = crate::writer::write_segment_v9_to_memory(&seg).expect("write");

        // ... repacked with ONE extra metric whose descriptor declares an
        // unsupported (sketch) value type.  The repack rebuilds the chunk
        // from the parsed entries, replaces index.drd to DECLARE the
        // extra metric, and appends its two files.
        let mut files: Vec<(String, Vec<u8>)> = Vec::new();
        for line in meta.lines().skip(1) {
            let parts: Vec<&str> = line.split(',').collect();
            let (name, start, end) = (
                parts[0],
                parts[2].parse::<usize>().expect("start"),
                parts[3].parse::<usize>().expect("end"),
            );
            let bytes = if name == "index.drd" {
                crate::v9::encode_index_drd(&["region"], &["value", "theta_col"], 100, 300, 1)
            } else {
                chunks[0][start..end].to_vec()
            };
            files.push((name.to_string(), bytes));
        }
        files.push((
            "theta_col.column_descriptor.json".to_string(),
            br#"{"valueType":"thetaSketch"}"#.to_vec(),
        ));
        files.push(("theta_col".to_string(), vec![0xDE, 0xAD]));

        let dir = tempfile::tempdir().expect("tempdir");
        let mut chunk = Vec::new();
        let mut meta_lines = vec![format!("v1,2147483647,{}", files.len())];
        for (name, bytes) in &files {
            let start = chunk.len();
            chunk.extend_from_slice(bytes);
            meta_lines.push(format!("{name},0,{start},{}", chunk.len()));
        }
        std::fs::write(dir.path().join("meta.smoosh"), meta_lines.join("\n")).expect("meta");
        std::fs::write(dir.path().join("00000.smoosh"), &chunk).expect("chunk");

        // Strict on-disk open still fails the whole segment (unchanged).
        let err = SegmentData::open(dir.path()).expect_err("strict open must reject");
        assert!(
            err.to_string().contains("theta_col"),
            "strict reason names the column: {err}"
        );

        // Lenient on-disk open drops it WITH the manifest.
        let (segment, dropped) = SegmentData::open_lenient(dir.path()).expect("lenient open");
        assert_eq!(dropped, vec!["theta_col".to_string()]);
        assert!(segment.column("theta_col").is_none());
        assert_eq!(segment.num_rows(), 3);
        assert_eq!(segment.timestamp_column().expect("ts"), &[100, 200, 300]);
        assert!(matches!(
            segment.column("region"),
            Some(ColumnData::String(_))
        ));
        assert!(matches!(
            segment.column("value"),
            Some(ColumnData::Double(_))
        ));
    }

    /// Unknown version (v11) produces an error.
    #[test]
    fn auto_detect_unknown_version() {
        let mut chunk = Vec::new();
        chunk.extend_from_slice(&11_i32.to_be_bytes());

        let meta = "v1,2147483647,1\nversion.bin,0,0,4";
        let smoosh = SmooshReader::from_parts(meta, vec![chunk]).expect("smoosh");
        let err = SegmentData::from_smoosh(&smoosh).unwrap_err();
        assert!(
            err.to_string().contains("unsupported segment version: 11"),
            "unexpected error: {err}"
        );
    }
}
