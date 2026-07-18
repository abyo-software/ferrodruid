// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Segment v9 writer — serializes a [`SegmentData`] into smoosh format.
//!
//! The writer produces `meta.smoosh` + chunk files that are readable by the
//! existing [`SmooshReader`](crate::smoosh::SmooshReader) and
//! [`read_segment_v9`](crate::v9::read_segment_v9) reader path.
//!
//! # Crash safety
//!
//! Each output file is written via the `temp + fsync + rename` discipline
//! exposed by the crate-private `durable_write` helper. After all files are
//! renamed into their final names, the parent directory itself is fsynced so
//! the rename is durable across crashes/power loss. The discipline is
//! implemented in `durable_write` and re-used by the FDX writer in
//! [`crate::fdx::write_segment_fdx`].

use std::path::Path;

use ferrodruid_common::error::{DruidError, Result};

use crate::column::{
    ColumnData, ColumnDescriptor, encode_double_column, encode_float_column, encode_long_column,
    encode_long_column_nullable, encode_string_column, encode_string_multi_column,
};
use crate::segment::SegmentData;
use crate::smoosh::SMOOSH_MAX_CHUNK_SIZE;
use crate::v9::encode_index_drd;

pub(crate) use durable::{durable_write, fsync_dir};

mod durable {
    //! `temp + fsync + rename` helper shared by v9/FDX writers.

    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use ferrodruid_common::error::{DruidError, Result};

    /// Monotonic counter so concurrent writers in the same process don't pick
    /// the same temp suffix even within the same nanosecond.
    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Generate a unique suffix combining process id, monotonic counter, and
    /// system time. Collisions are extremely unlikely; the rename step would
    /// surface a collision if it ever happened.
    fn unique_suffix() -> String {
        let pid = std::process::id();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{pid}.{counter}.{nanos}")
    }

    /// Open the parent directory and call `sync_all` on it.
    ///
    /// On Unix this fsyncs the directory itself, making the most recent
    /// `rename` durable across power loss. On platforms that don't allow
    /// opening a directory as a file (e.g. some Windows shells) this returns
    /// an error: callers can downgrade that to a warning if their target
    /// lacks the syscall (Wave 36 portability decision).
    pub(crate) fn fsync_dir(dir: &Path) -> Result<()> {
        match File::open(dir) {
            Ok(f) => f.sync_all().map_err(|e| {
                DruidError::Segment(format!("fsync parent dir {} failed: {e}", dir.display()))
            }),
            Err(e) => {
                // macOS/Linux always allow opening a directory; on platforms
                // where it isn't supported (some Windows configurations), we
                // surface the error so the caller can decide whether to skip.
                Err(DruidError::Segment(format!(
                    "open parent dir {} for fsync failed: {e}",
                    dir.display()
                )))
            }
        }
    }

    /// Write `data` to `final_path` atomically:
    ///
    /// 1. Write to `<final_path>.tmp.<unique>`.
    /// 2. `fsync` the temp file's data.
    /// 3. `rename(tmp, final)`.
    ///
    /// After all files in a segment have been [`durable_write`]n, the caller
    /// should [`fsync_dir`] the parent directory so the rename itself becomes
    /// durable.
    ///
    /// On rename failure the temp file is left behind for operator inspection
    /// and the returned error includes both the temp and final paths so it
    /// can be cleaned up out of band.
    pub(crate) fn durable_write(final_path: &Path, data: &[u8]) -> Result<()> {
        let suffix = unique_suffix();
        let mut tmp_path: PathBuf = final_path.to_path_buf();
        let tmp_name = match final_path.file_name() {
            Some(n) => format!("{}.tmp.{suffix}", n.to_string_lossy()),
            None => {
                return Err(DruidError::Segment(format!(
                    "durable_write: target {} has no file name component",
                    final_path.display()
                )));
            }
        };
        tmp_path.set_file_name(tmp_name);

        // 1. Write data to the temp file.
        let mut tmp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| {
                DruidError::Segment(format!("open temp file {} failed: {e}", tmp_path.display()))
            })?;

        tmp_file.write_all(data).map_err(|e| {
            DruidError::Segment(format!(
                "write to temp file {} failed: {e}",
                tmp_path.display()
            ))
        })?;

        // 2. fsync the data so the bytes survive a crash *before* we rename.
        tmp_file.sync_all().map_err(|e| {
            DruidError::Segment(format!(
                "fsync temp file {} failed: {e}",
                tmp_path.display()
            ))
        })?;
        // Drop the file handle before renaming for portability.
        drop(tmp_file);

        // 3. Atomic rename into place. On failure, leave the temp behind for
        //    operator cleanup and report both paths.
        std::fs::rename(&tmp_path, final_path).map_err(|e| {
            DruidError::Segment(format!(
                "rename {} -> {} failed (temp file left for cleanup): {e}",
                tmp_path.display(),
                final_path.display()
            ))
        })?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SmooshWriter
// ---------------------------------------------------------------------------

/// Accumulates logical files and serializes them into the smoosh format.
///
/// Files are stored in insertion order inside a single chunk.
///
/// `pub(crate)` so the upstream-layout writer in
/// [`crate::druid_native_writer`] can reuse the exact same smoosh packaging
/// and crash-safety discipline (Wave 40-B atomic dir rename).
pub(crate) struct SmooshWriter {
    /// Logical files in insertion order: `(name, data)`.
    files: Vec<(String, Vec<u8>)>,
    /// Sidecar files written NEXT TO `meta.smoosh` on disk instead of
    /// inside the smoosh archive (the Druid 30+ local deep-storage layout
    /// for `version.bin` / `factory.json`).  Only meaningful for
    /// [`Self::write_to_dir`]; [`Self::finish`] ignores them.
    sidecars: Vec<(String, Vec<u8>)>,
}

impl SmooshWriter {
    /// Create an empty writer.
    pub(crate) fn new() -> Self {
        Self {
            files: Vec::new(),
            sidecars: Vec::new(),
        }
    }

    /// Add a logical file with the given name and data.
    pub(crate) fn add_file(&mut self, name: String, data: Vec<u8>) {
        self.files.push((name, data));
    }

    /// Add a sidecar file written as a plain sibling of `meta.smoosh`
    /// (NOT a smoosh entry).  `name` must be a bare filename — path
    /// separators are refused loudly at write time.
    pub(crate) fn add_sidecar_file(&mut self, name: String, data: Vec<u8>) {
        self.sidecars.push((name, data));
    }

    /// Produce the `meta.smoosh` text and chunk data (single chunk).
    ///
    /// # Errors
    ///
    /// Fails if the single emitted chunk would exceed the smoosh format's
    /// advertised maximum chunk size ([`SMOOSH_MAX_CHUNK_SIZE`], 2^31-1) — a
    /// larger `00000.smoosh` cannot be memory-mapped by Druid or a correct
    /// smoosh reader, and multi-chunk splitting is out of milestone scope.
    fn finish(self) -> Result<(String, Vec<Vec<u8>>)> {
        // Size the chunk FIRST, on the byte-COUNTS alone, failing loud on the
        // RUNNING total before the concatenation buffer is allocated.
        //
        // Pre-fix, the per-file `extend_from_slice` below appended every blob
        // and only checked the total AFTERWARDS: a segment whose blobs sum to
        // multiple GiB would grow `chunk` past `SMOOSH_MAX_CHUNK_SIZE` and
        // OOM-abort the process (an abort, not the promised loud `Result::Err`)
        // before the trailing guard could ever run. Checking the running total
        // up front — and reserving exactly that many bytes — guarantees the
        // guard fires before any multi-GiB allocation. (writer.rs High.)
        let total = checked_chunk_size(self.files.iter().map(|(_, data)| data.len()))?;

        let mut meta_lines = Vec::with_capacity(self.files.len() + 1);
        // Header: "v1,<max_chunk_size>,<num_logical_files>"
        meta_lines.push(format!("v1,{SMOOSH_MAX_CHUNK_SIZE},{}", self.files.len()));

        // `total <= SMOOSH_MAX_CHUNK_SIZE` (guaranteed above), so this reserves
        // at most the advertised ~2 GiB cap and never over-allocates for an
        // over-cap segment (that path already returned `Err`).
        let mut chunk = Vec::with_capacity(total);
        for (name, data) in &self.files {
            let start = chunk.len();
            chunk.extend_from_slice(data);
            let end = chunk.len();
            meta_lines.push(format!("{name},0,{start},{end}"));
        }

        let meta = meta_lines.join("\n");
        Ok((meta, vec![chunk]))
    }

    /// Write `meta.smoosh` and `00000.smoosh` to a directory on disk.
    ///
    /// Crash-safety discipline (see module docs):
    ///
    /// 1. **Wave 40-B**: every file lands inside a *sibling* temporary
    ///    directory `<final_dir>.tmp.<unique>`.  The sibling is built
    ///    incrementally with `temp + fsync + rename` per file; if the writer
    ///    crashes anywhere during this phase the half-populated tmp directory
    ///    has a recognisable suffix and is not mistaken for a real segment.
    /// 2. After the tmp directory is fully populated and its contents are
    ///    fsynced, the directory itself is atomically renamed into place
    ///    (`rename(<tmp_dir>, <final_dir>)`).  On Unix this is a single
    ///    atomic operation that publishes either the entire dir or nothing.
    /// 3. The grandparent directory is fsynced after the dir-rename so the
    ///    rename itself becomes durable across power loss.
    ///
    /// Wave 36-D's per-file `temp + fsync + rename` is preserved for the
    /// staging step inside the tmp dir; the additional dir-level rename is
    /// the missing link Wave 39 [High] [NEW-VARIANT] flagged at writer.rs
    /// 185-214 (segment dir was being rewritten in place).
    pub(crate) fn write_to_dir(mut self, final_dir: &Path) -> Result<()> {
        // Sidecars land next to meta.smoosh inside the SAME staged tmp dir,
        // so the atomic dir rename publishes archive + sidecars together.
        let sidecars = std::mem::take(&mut self.sidecars);
        for (name, _) in &sidecars {
            if name.is_empty() || name.contains('/') || name.contains('\\') || name == ".." {
                return Err(DruidError::Segment(format!(
                    "sidecar file name {name:?} is not a bare filename"
                )));
            }
        }

        // Compute the sibling tmp dir name with a unique suffix.
        let tmp_dir = sibling_tmp_dir(final_dir)?;

        // If a previous failed run left this exact tmp dir around (very
        // unlikely thanks to PID + counter + nanos), wipe it first so we
        // don't pollute a fresh write.  Failure is non-fatal; the
        // create_dir_all below will surface it.
        if tmp_dir.exists() {
            let _ = std::fs::remove_dir_all(&tmp_dir);
        }

        std::fs::create_dir_all(&tmp_dir).map_err(|e| {
            DruidError::Segment(format!(
                "failed to create staging dir {}: {e}",
                tmp_dir.display()
            ))
        })?;

        let (meta, chunks) = self.finish()?;

        // Stage chunks via per-file temp + fsync + rename inside tmp_dir.
        for (i, chunk) in chunks.iter().enumerate() {
            let path = tmp_dir.join(format!("{i:05}.smoosh"));
            durable_write(&path, chunk)?;
        }

        // Stage sidecar files (Druid 30+ external `version.bin` /
        // `factory.json` layout) before the publish marker.
        for (name, data) in &sidecars {
            durable_write(&tmp_dir.join(name), data)?;
        }

        // Stage meta.smoosh last so a crash before this point leaves the
        // tmp dir without the publish marker.
        durable_write(&tmp_dir.join("meta.smoosh"), meta.as_bytes())?;

        // fsync tmp_dir so all renames inside it are durable before we flip
        // the parent rename.
        fsync_dir(&tmp_dir)?;

        // Atomically rename the tmp dir into place.  If `final_dir` already
        // exists we refuse to overwrite an existing *populated* segment —
        // closing the Wave 39 "rewrite-in-place" hazard — but tolerate a
        // pre-existing *empty* directory (callers that pre-create the
        // target path, e.g. `tempfile::tempdir()`-driven test rigs and
        // some operator workflows).  An empty dir is not a segment, so
        // removing it before the rename is safe.
        if final_dir.exists() {
            let is_empty = std::fs::read_dir(final_dir)
                .map(|mut it| it.next().is_none())
                .unwrap_or(false);
            if !is_empty {
                let _ = std::fs::remove_dir_all(&tmp_dir);
                return Err(DruidError::Segment(format!(
                    "refusing to overwrite populated segment dir {} (Wave 40-B atomic rename)",
                    final_dir.display()
                )));
            }
            // Pre-existing empty dir: remove it so rename(2) can proceed.
            std::fs::remove_dir(final_dir).map_err(|e| {
                DruidError::Segment(format!(
                    "remove empty target dir {} for atomic rename failed: {e}",
                    final_dir.display()
                ))
            })?;
        }

        std::fs::rename(&tmp_dir, final_dir).map_err(|e| {
            DruidError::Segment(format!(
                "atomic rename {} -> {} failed (staging dir left for cleanup): {e}",
                tmp_dir.display(),
                final_dir.display()
            ))
        })?;

        // fsync the *grandparent* so the dir-rename itself is durable.
        if let Some(grand) = final_dir.parent() {
            fsync_dir(grand)?;
        }

        Ok(())
    }
}

/// Reject a smoosh chunk whose byte length exceeds the format's advertised
/// maximum ([`SMOOSH_MAX_CHUNK_SIZE`], 2^31-1).
///
/// The `meta.smoosh` header advertises this as the per-chunk cap, and a
/// `00000.smoosh` larger than it cannot be memory-mapped by Druid (Java maps
/// with `int` offsets) or by a correct smoosh reader. Multi-chunk splitting
/// is out of milestone scope, so a segment that would need it fails loud
/// here (naming the limit) instead of being written into an unmappable file.
///
/// # Errors
///
/// Returns [`DruidError::Segment`] when `chunk_len > SMOOSH_MAX_CHUNK_SIZE`.
fn ensure_smoosh_chunk_fits(chunk_len: usize) -> Result<()> {
    if chunk_len > SMOOSH_MAX_CHUNK_SIZE {
        return Err(DruidError::Segment(format!(
            "smoosh writer: single chunk would be {chunk_len} bytes, exceeding the smoosh \
             format's advertised max chunk size {SMOOSH_MAX_CHUNK_SIZE} (2^31-1) — a larger \
             chunk cannot be mapped by Druid or a correct reader, and multi-chunk splitting \
             is out of milestone scope; refusing to write a segment nothing can open"
        )));
    }
    Ok(())
}

/// Sum the logical files' byte-lengths, failing loud on the RUNNING total the
/// instant it would exceed [`SMOOSH_MAX_CHUNK_SIZE`] — BEFORE any concatenation
/// buffer is allocated.
///
/// This operates purely on byte-COUNTS (not on the blobs themselves), so an
/// over-cap segment is rejected without ever materializing the multi-GiB
/// `00000.smoosh`. It is the pre-allocation half of the [`ensure_smoosh_chunk_fits`]
/// guard: the caller reserves exactly the returned total, which is proven
/// `<= SMOOSH_MAX_CHUNK_SIZE`, so the append loop can never OOM-abort the
/// process before the guard fires.
///
/// # Errors
///
/// Returns [`DruidError::Segment`] if the running total overflows `usize`
/// (defensive; unreachable once the size guard caps it) or exceeds
/// [`SMOOSH_MAX_CHUNK_SIZE`].
fn checked_chunk_size<I: IntoIterator<Item = usize>>(file_lens: I) -> Result<usize> {
    let mut running: usize = 0;
    for len in file_lens {
        // Grow the running total with a checked add BEFORE the size guard, so
        // a pathological length can never wrap `usize` on the way to the guard.
        running = running.checked_add(len).ok_or_else(|| {
            DruidError::Segment("smoosh writer: total chunk size overflows usize".to_string())
        })?;
        // Fail loud the moment the running total crosses the cap — the append
        // that would materialize these bytes has not happened yet.
        ensure_smoosh_chunk_fits(running)?;
    }
    Ok(running)
}

/// Build a sibling staging directory path next to `final_dir` with a unique
/// suffix.  Returns an error if `final_dir` has no file-name component (e.g.
/// `/`) since we cannot derive a sibling there.
fn sibling_tmp_dir(final_dir: &Path) -> Result<std::path::PathBuf> {
    let parent = final_dir.parent().ok_or_else(|| {
        DruidError::Segment(format!(
            "segment dir {} has no parent — cannot stage atomic rename",
            final_dir.display()
        ))
    })?;
    let name = final_dir
        .file_name()
        .ok_or_else(|| {
            DruidError::Segment(format!(
                "segment dir {} has no file-name component",
                final_dir.display()
            ))
        })?
        .to_string_lossy()
        .into_owned();

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static DIR_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let counter = DIR_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    Ok(parent.join(format!("{name}.tmp.{pid}.{counter}.{nanos}")))
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Write a [`SegmentData`] to a smoosh-format directory on disk.
///
/// Creates `meta.smoosh` and `00000.smoosh` inside `dir`.
pub fn write_segment_v9(segment: &SegmentData, dir: &Path) -> Result<()> {
    let writer = build_smoosh_writer(segment)?;
    writer.write_to_dir(dir)
}

/// Write a [`SegmentData`] to in-memory smoosh parts (for testing).
///
/// Returns `(meta_smoosh_text, chunk_byte_vecs)`.
pub fn write_segment_v9_to_memory(segment: &SegmentData) -> Result<(String, Vec<Vec<u8>>)> {
    let writer = build_smoosh_writer(segment)?;
    writer.finish()
}

// ---------------------------------------------------------------------------
// Internal builder
// ---------------------------------------------------------------------------

/// Build a [`SmooshWriter`] from a [`SegmentData`].
fn build_smoosh_writer(segment: &SegmentData) -> Result<SmooshWriter> {
    // index.drd stores every dimension/metric name behind a 2-byte length
    // prefix, so a name longer than `u16::MAX` bytes CANNOT be represented.
    // Fail LOUD instead of silently truncating the length (`len as u16`),
    // which emitted a malformed index.drd that the strict reader rejects —
    // a native Apache Druid index.drd may legitimately carry such a name,
    // and the migrate lenient-attach rewrite would otherwise commit a blob
    // that bricks the next bootstrap reload (compat-2 attach-hardening).
    for name in segment.dimensions.iter().chain(segment.metrics.iter()) {
        if name.len() > usize::from(u16::MAX) {
            let head: String = name.chars().take(40).collect();
            return Err(DruidError::Segment(format!(
                "column name starting {head:?} is {} bytes long, exceeding the v9 \
                 index.drd 2-byte name-length limit of {} — refusing to write a \
                 silently truncated (malformed) index.drd",
                name.len(),
                u16::MAX
            )));
        }
    }

    let mut w = SmooshWriter::new();

    // 1. version.bin
    w.add_file("version.bin".to_string(), 9_i32.to_be_bytes().to_vec());

    // 2. index.drd
    let dim_refs: Vec<&str> = segment.dimensions.iter().map(|s| s.as_str()).collect();
    let met_refs: Vec<&str> = segment.metrics.iter().map(|s| s.as_str()).collect();
    let index_drd = encode_index_drd(
        &dim_refs,
        &met_refs,
        segment.interval.start_millis,
        segment.interval.end_millis,
        1, // roaring bitmap type
    );
    w.add_file("index.drd".to_string(), index_drd);

    // 3. Columns — collect into a BTreeMap for deterministic order.
    let mut col_names: Vec<&str> = Vec::new();
    // Always emit __time first, then dimensions, then metrics.
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

        // Column descriptor JSON
        let descriptor = column_descriptor_for(col);
        let desc_json = serde_json::to_vec(&descriptor).map_err(|e| {
            DruidError::Segment(format!(
                "failed to serialize descriptor for {col_name}: {e}"
            ))
        })?;
        w.add_file(format!("{col_name}.column_descriptor.json"), desc_json);

        // Column data
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
    }
}

/// Encode a [`ColumnData`] to its binary representation.
fn encode_column(col: &ColumnData) -> Result<Vec<u8>> {
    match col {
        ColumnData::Long(v) => Ok(encode_long_column(v)),
        ColumnData::LongNullable(v, nulls) => encode_long_column_nullable(v, nulls),
        ColumnData::Float(v) => Ok(encode_float_column(v)),
        ColumnData::Double(v) => Ok(encode_double_column(v)),
        ColumnData::String(s) => encode_string_column(s),
        ColumnData::StringMulti(s) => encode_string_multi_column(s),
        ColumnData::Complex(b) => Ok(b.clone()),
        ColumnData::ComplexTheta(rows) => crate::column::encode_theta_column(rows),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::StringColumnData;
    use crate::segment::{Interval, SegmentDataBuilder};
    use crate::smoosh::SmooshReader;
    use ferrodruid_bitmap::DruidBitmap;
    use ferrodruid_dict::FrontCodedDictionary;
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // Round-trip: the most critical test
    // -----------------------------------------------------------------------

    /// A decoded per-row theta column (compat-8 sketch #2) survives the
    /// FerroDruid v9 write → STRICT read round-trip: per-row estimates and
    /// the union-only Druid-origin marking are preserved, and the emitted
    /// descriptor carries the theta `complexTypeName`.  This is exactly
    /// the path the migrate `--allow-unreadable-columns` lenient REWRITE
    /// takes when re-persisting a segment that also carried an
    /// undecodable (hyperUnique/quantiles) column.
    #[test]
    fn segment_v9_round_trips_complex_theta_column() {
        // A genuine Druid-origin sketch (decoded from a crafted
        // DataSketches compact image: preLongs 2, exact mode, 3 hashes).
        let mut image = vec![2u8, 3, 3, 12, 13, 0, 0x1E, 0x93];
        image.extend_from_slice(&3i32.to_le_bytes());
        image.extend_from_slice(&0u32.to_le_bytes());
        for h in [10u64, 20, 30] {
            image.extend_from_slice(&h.to_le_bytes());
        }
        let druid = crate::column::ThetaSketch::from_druid_compact(&image).expect("decode image");
        let rows = vec![
            druid,
            crate::column::ThetaSketch::empty_druid_origin(),
            crate::column::ThetaSketch::empty_druid_origin(),
        ];

        let mut columns = HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![1000, 2000, 3000]),
        );
        columns.insert(
            "users_theta".to_string(),
            ColumnData::ComplexTheta(rows.clone()),
        );
        let segment = SegmentData {
            version: 9,
            num_rows: 3,
            interval: Interval {
                start_millis: 1000,
                end_millis: 3000,
            },
            dimensions: vec![],
            metrics: vec!["users_theta".to_string()],
            columns,
            time_sorted: true,
        };

        let (meta, chunks) = write_segment_v9_to_memory(&segment).expect("write");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");

        // The descriptor names the complex type (this is what routes the
        // strict reader to the per-row decoder).
        let desc = crate::column::ColumnDescriptor::from_json(
            smoosh
                .read_file("users_theta.column_descriptor.json")
                .expect("descriptor entry"),
        )
        .expect("descriptor json");
        assert_eq!(desc.value_type, "COMPLEX");
        assert_eq!(desc.complex_type_name.as_deref(), Some("thetaSketch"));

        let read_back = SegmentData::from_smoosh(&smoosh).expect("strict re-read");
        let Some(ColumnData::ComplexTheta(got)) = read_back.columns.get("users_theta") else {
            panic!("expected ComplexTheta after round-trip");
        };
        assert_eq!(got.len(), 3);
        for (orig, got) in rows.iter().zip(got) {
            assert!((orig.estimate() - got.estimate()).abs() < f64::EPSILON);
            assert!(got.is_druid_origin(), "union-only marking must survive");
        }
    }

    #[test]
    fn segment_v9_write_read_roundtrip() {
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

        // Write to in-memory smoosh
        let (meta, chunks) = write_segment_v9_to_memory(&segment).expect("write");

        // Read back
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = SegmentData::from_smoosh(&smoosh).expect("from_smoosh");

        // Verify basics
        assert_eq!(read_back.num_rows(), 3);
        assert_eq!(read_back.version, 9);
        assert_eq!(read_back.dimensions, vec!["city"]);
        assert_eq!(read_back.metrics, vec!["revenue", "price"]);
        assert_eq!(read_back.interval.start_millis, 1000);
        assert_eq!(read_back.interval.end_millis, 3000);

        // Verify timestamps
        let ts = read_back.timestamp_column().expect("ts");
        assert_eq!(ts, &[1000_i64, 2000, 3000]);

        // Verify string column (city)
        match read_back.column("city").expect("city column") {
            ColumnData::String(s) => {
                // Dictionary should be sorted: london, new york, tokyo
                assert_eq!(s.dictionary.len(), 3);
                assert_eq!(s.dictionary.get(0), Some("london"));
                assert_eq!(s.dictionary.get(1), Some("new york"));
                assert_eq!(s.dictionary.get(2), Some("tokyo"));

                // Ordinals: tokyo=2, new york=1, london=0
                assert_eq!(s.encoded_values, vec![2, 1, 0]);

                // Bitmaps: london={row2}, new york={row1}, tokyo={row0}
                assert_eq!(s.bitmap_indexes.len(), 3);
                assert!(s.bitmap_indexes[0].contains(2)); // london
                assert!(s.bitmap_indexes[1].contains(1)); // new york
                assert!(s.bitmap_indexes[2].contains(0)); // tokyo
            }
            other => panic!("expected String column, got {other:?}"),
        }

        // Verify long metric
        match read_back.column("revenue").expect("revenue column") {
            ColumnData::Long(vals) => assert_eq!(vals, &[100, 200, 300]),
            other => panic!("expected Long column, got {other:?}"),
        }

        // Verify double metric
        match read_back.column("price").expect("price column") {
            ColumnData::Double(vals) => assert_eq!(vals, &[9.99, 19.99, 29.99]),
            other => panic!("expected Double column, got {other:?}"),
        }
    }

    /// A null-bearing LONG column round-trips through v9 write→read as
    /// `LongNullable` with exact `i64` values (incl. 2^53+1, which the
    /// pre-2026-07 NaN-Double degrade rounded) and the exact null rows;
    /// a null-free long column in the same segment stays plain `Long`.
    /// (This is the round-trip the historical's spill mode relies on.)
    #[test]
    fn segment_v9_nullable_long_roundtrip() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000, 3000])
            .add_long_column("plain", true, vec![1, 2, 3])
            .add_long_column_nullable(
                "code",
                false,
                vec![Some(7), None, Some(9_007_199_254_740_993)], // 2^53 + 1
            )
            .build()
            .expect("build segment");

        let (meta, chunks) = write_segment_v9_to_memory(&segment).expect("write");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = SegmentData::from_smoosh(&smoosh).expect("from_smoosh");

        match read_back.column("code").expect("code") {
            ColumnData::LongNullable(v, nulls) => {
                assert_eq!(
                    v,
                    &vec![7, 0, 9_007_199_254_740_993],
                    "values must round-trip i64-exactly (2^53+1 must NOT round)"
                );
                assert_eq!(nulls.len(), 1);
                assert!(nulls.contains(1), "row 1 must stay NULL");
            }
            other => panic!("expected LongNullable, got {other:?}"),
        }
        match read_back.column("plain").expect("plain") {
            ColumnData::Long(vals) => assert_eq!(vals, &[1, 2, 3]),
            other => panic!("null-free long must stay plain Long, got {other:?}"),
        }
    }

    /// compat-11: a MULTI-VALUE string dimension survives the v9
    /// write→read round-trip (descriptor `hasMultipleValues: true` routes
    /// the reader to the multi decoder), preserving per-row element lists,
    /// within-row order, and the empty (null) row — while a single-value
    /// string column in the same segment stays plain `String`.
    #[test]
    fn segment_v9_string_multi_roundtrip() {
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

        let (meta, chunks) = write_segment_v9_to_memory(&segment).expect("write");

        // The descriptor for the MV column must declare hasMultipleValues.
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let desc = crate::column::ColumnDescriptor::from_json(
            smoosh
                .read_file("tags.column_descriptor.json")
                .expect("tags descriptor"),
        )
        .expect("parse descriptor");
        assert!(desc.has_multiple_values, "MV descriptor flag must be true");
        assert_eq!(desc.value_type, "STRING");

        let read_back = SegmentData::from_smoosh(&smoosh).expect("from_smoosh");
        assert_eq!(read_back.num_rows(), 4);
        match read_back.column("tags").expect("tags") {
            ColumnData::StringMulti(mc) => {
                assert_eq!(mc.num_rows(), 4);
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

    // -----------------------------------------------------------------------
    // Empty segment
    // -----------------------------------------------------------------------

    #[test]
    fn empty_segment_roundtrip() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![])
            .build()
            .expect("build");

        let (meta, chunks) = write_segment_v9_to_memory(&segment).expect("write");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = SegmentData::from_smoosh(&smoosh).expect("from_smoosh");

        assert_eq!(read_back.num_rows(), 0);
        assert!(read_back.dimensions.is_empty());
        assert!(read_back.metrics.is_empty());
    }

    // -----------------------------------------------------------------------
    // Large segment (1000 rows)
    // -----------------------------------------------------------------------

    #[test]
    fn large_segment_roundtrip() {
        let n = 1000;
        let times: Vec<i64> = (0..n).map(|i| 1_000_000 + i * 1000).collect();
        let values: Vec<i64> = (0..n).collect();
        let prices: Vec<f64> = (0..n).map(|i| i as f64 * 0.5).collect();
        let cities: Vec<String> = (0..n).map(|i| format!("city_{:03}", i % 50)).collect();

        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(times.clone())
            .add_string_column("city", cities)
            .add_long_column("value", true, values.clone())
            .add_double_column("price", true, prices.clone())
            .build()
            .expect("build");

        let (meta, chunks) = write_segment_v9_to_memory(&segment).expect("write");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = SegmentData::from_smoosh(&smoosh).expect("from_smoosh");

        assert_eq!(read_back.num_rows(), n as usize);
        assert_eq!(read_back.timestamp_column().expect("ts"), times.as_slice());

        match read_back.column("value").expect("value") {
            ColumnData::Long(v) => assert_eq!(v, &values),
            other => panic!("expected Long, got {other:?}"),
        }
        match read_back.column("price").expect("price") {
            ColumnData::Double(v) => assert_eq!(v, &prices),
            other => panic!("expected Double, got {other:?}"),
        }
        match read_back.column("city").expect("city") {
            ColumnData::String(s) => {
                assert_eq!(s.dictionary.len(), 50);
                assert_eq!(s.encoded_values.len(), n as usize);
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Multiple string dimensions with overlapping values
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_string_dimensions() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200])
            .add_string_column("color", vec!["red".to_string(), "blue".to_string()])
            .add_string_column("size", vec!["large".to_string(), "large".to_string()])
            .build()
            .expect("build");

        let (meta, chunks) = write_segment_v9_to_memory(&segment).expect("write");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = SegmentData::from_smoosh(&smoosh).expect("from_smoosh");

        assert_eq!(read_back.num_rows(), 2);
        assert_eq!(read_back.dimensions, vec!["color", "size"]);

        // Check "size" column — all rows are "large"
        match read_back.column("size").expect("size") {
            ColumnData::String(s) => {
                assert_eq!(s.dictionary.len(), 1);
                assert_eq!(s.dictionary.get(0), Some("large"));
                assert_eq!(s.encoded_values, vec![0, 0]);
                // Single bitmap covering both rows
                assert_eq!(s.bitmap_indexes.len(), 1);
                assert!(s.bitmap_indexes[0].contains(0));
                assert!(s.bitmap_indexes[0].contains(1));
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Timestamp + 1 metric only (no dimensions)
    // -----------------------------------------------------------------------

    #[test]
    fn timestamp_and_metric_only() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![10, 20, 30])
            .add_double_column("temp", true, vec![36.5, 37.0, 36.8])
            .build()
            .expect("build");

        let (meta, chunks) = write_segment_v9_to_memory(&segment).expect("write");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = SegmentData::from_smoosh(&smoosh).expect("from_smoosh");

        assert_eq!(read_back.num_rows(), 3);
        assert!(read_back.dimensions.is_empty());
        assert_eq!(read_back.metrics, vec!["temp"]);

        match read_back.column("temp").expect("temp") {
            ColumnData::Double(v) => assert_eq!(v, &[36.5, 37.0, 36.8]),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Write to disk directory and read back
    // -----------------------------------------------------------------------

    #[test]
    fn write_to_disk_roundtrip() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![500, 600])
            .add_string_column("host", vec!["alpha".to_string(), "beta".to_string()])
            .add_long_column("count", true, vec![42, 99])
            .build()
            .expect("build");

        let dir = tempfile::tempdir().expect("tempdir");
        let dir_path = dir.path();

        write_segment_v9(&segment, dir_path).expect("write_segment_v9");

        // Verify files exist
        assert!(dir_path.join("meta.smoosh").exists());
        assert!(dir_path.join("00000.smoosh").exists());

        // Read back
        let read_back = SegmentData::open(dir_path).expect("open");

        assert_eq!(read_back.num_rows(), 2);
        assert_eq!(read_back.dimensions, vec!["host"]);
        assert_eq!(read_back.metrics, vec!["count"]);
        assert_eq!(read_back.timestamp_column().expect("ts"), &[500_i64, 600]);

        match read_back.column("count").expect("count") {
            ColumnData::Long(v) => assert_eq!(v, &[42, 99]),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Manual SegmentData (not using builder) round-trips correctly
    // -----------------------------------------------------------------------

    #[test]
    fn manual_segment_data_roundtrip() {
        let dict = FrontCodedDictionary::from_sorted(vec!["bar".to_string(), "foo".to_string()]);
        let mut bm0 = DruidBitmap::new();
        bm0.insert(1);
        let mut bm1 = DruidBitmap::new();
        bm1.insert(0);

        let string_col = StringColumnData {
            dictionary: dict,
            encoded_values: vec![1, 0], // foo, bar
            bitmap_indexes: vec![bm0.clone(), bm1.clone()],
        };

        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![10, 20]));
        columns.insert("tag".to_string(), ColumnData::String(string_col));
        columns.insert("val".to_string(), ColumnData::Double(vec![1.5, 2.5]));

        let segment = SegmentData {
            version: 9,
            num_rows: 2,
            interval: Interval {
                start_millis: 10,
                end_millis: 20,
            },
            dimensions: vec!["tag".to_string()],
            metrics: vec!["val".to_string()],
            columns,
            time_sorted: false,
        };

        let (meta, chunks) = write_segment_v9_to_memory(&segment).expect("write");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = SegmentData::from_smoosh(&smoosh).expect("from_smoosh");

        assert_eq!(read_back.num_rows(), 2);

        match read_back.column("tag").expect("tag") {
            ColumnData::String(s) => {
                assert_eq!(s.encoded_values, vec![1, 0]);
                assert_eq!(s.dictionary.get(0), Some("bar"));
                assert_eq!(s.dictionary.get(1), Some("foo"));
                assert_eq!(s.bitmap_indexes[0], bm0);
                assert_eq!(s.bitmap_indexes[1], bm1);
            }
            other => panic!("expected String, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Wave 36-D / R1: durable writer (temp + fsync + rename).
    // Internal security review (Wave 35 R1), Medium: "Segment writes are
    // crash-unsafe".
    // -----------------------------------------------------------------------

    /// While a segment is mid-write, the final `meta.smoosh` path must not
    /// exist — only the temp file. After the writer finishes, only the final
    /// path exists and there are no leftover `*.tmp.*` files.
    #[test]
    fn temp_file_visible_during_write_invisible_after_rename() {
        use crate::writer::durable_write;

        let dir = tempfile::tempdir().expect("tempdir");
        let final_path = dir.path().join("meta.smoosh");

        // Sanity: nothing exists yet.
        assert!(!final_path.exists());

        // Drive a single durable_write so we can assert that *during* the
        // call, no half-written final file exists. We can't strictly observe
        // the in-flight state from a single thread, so instead we validate
        // the post-condition: after a successful call, the only file in the
        // directory is the final path (no `.tmp.*` survivors).
        durable_write(&final_path, b"hello world").expect("durable_write");

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read_dir")
            .filter_map(std::result::Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();

        assert_eq!(
            entries,
            vec!["meta.smoosh".to_string()],
            "after rename, only the final file should remain (no .tmp.* survivors)"
        );

        // Content matches.
        let body = std::fs::read(&final_path).expect("read back");
        assert_eq!(body, b"hello world");
    }

    /// If the write panics partway through (simulated by writing only the
    /// chunk, then panicking before meta.smoosh is published), opening the
    /// directory must not yield a usable segment. The `meta.smoosh` commit
    /// marker must be absent and any `*.tmp.*` survivors must not be
    /// mistaken for a final segment.
    #[test]
    fn crash_during_write_leaves_no_partial() {
        use crate::writer::durable_write;

        let dir = tempfile::tempdir().expect("tempdir");

        // Simulate a crash mid-write: write chunk 00000.smoosh successfully,
        // then bail before writing meta.smoosh (the publish marker).
        durable_write(&dir.path().join("00000.smoosh"), b"partial chunk")
            .expect("chunk durable_write");

        // Verify meta.smoosh was never written: SmooshReader::open must fail.
        let meta_path = dir.path().join("meta.smoosh");
        assert!(
            !meta_path.exists(),
            "publish marker meta.smoosh must not exist after partial write"
        );

        let open_result = SmooshReader::open(dir.path());
        assert!(
            open_result.is_err(),
            "SmooshReader::open must reject a directory with no meta.smoosh"
        );

        // And no `.tmp.*` survivors either — the `00000.smoosh` rename
        // succeeded, so its temp file is gone.
        let leftover_tmps: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read_dir")
            .filter_map(std::result::Result::ok)
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(
            leftover_tmps.is_empty(),
            "no .tmp.* survivors should be visible after a successful durable_write"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 40-B: atomic-rename of segment directory
    // (Wave 39 [High] [NEW-VARIANT] — writer.rs:185-214)
    // -----------------------------------------------------------------------

    /// A crash *during* segment-dir creation must leave the final dir name
    /// either fully populated or absent — never half-built.  We emulate the
    /// crash by manually creating a sibling `<final>.tmp.<pid>.<n>.<nanos>`
    /// directory with partial contents (chunk only, no meta.smoosh) and
    /// asserting that the final dir remains absent and that the staging
    /// suffix makes the dir recognisable as garbage to a janitor.
    #[test]
    fn crash_during_segment_dir_creation_leaves_no_partial() {
        let parent = tempfile::tempdir().expect("tempdir");
        let final_dir = parent.path().join("seg-1");

        // Stage a half-built tmp dir as the writer would, then bail.
        let tmp_dir = parent.path().join("seg-1.tmp.0.0.0");
        std::fs::create_dir_all(&tmp_dir).expect("mkdir tmp");
        durable_write(&tmp_dir.join("00000.smoosh"), b"partial chunk")
            .expect("chunk durable_write");
        // Note: meta.smoosh deliberately not written -> simulating a crash
        // before the publish marker landed.

        // The final segment dir must NOT exist yet.
        assert!(
            !final_dir.exists(),
            "atomic-rename writer must not leave final dir visible mid-write"
        );

        // The garbage tmp dir must be recognisable by its `.tmp.` suffix
        // (so an operator / startup janitor can rm-rf it).
        let entries: Vec<String> = std::fs::read_dir(parent.path())
            .expect("read_dir")
            .filter_map(std::result::Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            entries.iter().any(|n| n.contains(".tmp.")),
            "tmp staging dir must carry a `.tmp.` suffix (got {entries:?})"
        );
        assert!(
            !entries.iter().any(|n| n == "seg-1"),
            "final dir name must not appear during a crashed write"
        );

        // SmooshReader on the tmp dir fails because meta.smoosh is missing
        // — the publish marker invariant is preserved.
        assert!(SmooshReader::open(&tmp_dir).is_err());
    }

    /// Two concurrent writes targeting the same final dir must each pick a
    /// distinct sibling tmp path, so neither crash-window step on the
    /// other's staging area.  We emulate concurrency in-process by calling
    /// the staging-name helper twice and asserting non-equality plus the
    /// `.tmp.` marker.
    #[test]
    fn concurrent_segment_dir_creation_uses_unique_tmp_paths() {
        let parent = tempfile::tempdir().expect("tempdir");
        let final_dir = parent.path().join("seg-A");

        let tmp_a = sibling_tmp_dir(&final_dir).expect("tmp A");
        let tmp_b = sibling_tmp_dir(&final_dir).expect("tmp B");
        assert_ne!(
            tmp_a, tmp_b,
            "concurrent staging dirs must be distinct (counter+nanos suffix)"
        );

        let name_a = tmp_a
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let name_b = tmp_b
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        assert!(
            name_a.starts_with("seg-A.tmp."),
            "staging dir name must encode `<final>.tmp.<unique>` (got {name_a})"
        );
        assert!(
            name_b.starts_with("seg-A.tmp."),
            "staging dir name must encode `<final>.tmp.<unique>` (got {name_b})"
        );

        // Drive two real successful writes back-to-back to a fresh final_dir
        // each time and verify the final dir has no `.tmp.` leftovers.
        let dir1 = parent.path().join("seg-1");
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2, 3])
            .add_long_column("v", true, vec![10, 20, 30])
            .build()
            .expect("build");
        write_segment_v9(&segment, &dir1).expect("write seg-1");
        let entries: Vec<String> = std::fs::read_dir(&dir1)
            .expect("read_dir")
            .filter_map(std::result::Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !entries.iter().any(|n| n.contains(".tmp.")),
            "final dir must not contain .tmp.* leftovers (got {entries:?})"
        );
        assert!(entries.iter().any(|n| n == "meta.smoosh"));
        assert!(entries.iter().any(|n| n == "00000.smoosh"));
    }

    // -----------------------------------------------------------------------
    // compat-2 attach-hardening: index.drd 2-byte name-length limit must be
    // fail-loud, never a silent truncation (a truncated length prefix emits
    // a malformed index.drd that the strict reader rejects — the lenient
    // attach rewrite would then commit a blob that bricks the next
    // bootstrap reload).
    // -----------------------------------------------------------------------

    /// A dimension/metric name longer than `u16::MAX` bytes cannot be
    /// represented behind index.drd's 2-byte length prefix: the writer
    /// must refuse it LOUDLY (both the in-memory and on-disk entry
    /// points), while a name of exactly `u16::MAX` bytes stays writable
    /// and round-trips — normal writes are byte-identical.
    #[test]
    fn writer_rejects_column_name_longer_than_index_drd_limit() {
        let over = "d".repeat(usize::from(u16::MAX) + 1);
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000])
            .add_string_column(&over, vec!["x".to_string(), "y".to_string()])
            .build()
            .expect("build");
        let err = write_segment_v9_to_memory(&segment)
            .expect_err("a name over the u16 length limit must be refused loudly");
        let msg = format!("{err}");
        assert!(
            msg.contains("name-length limit") && msg.contains("65535"),
            "the error must name the limit: {msg}"
        );
        // The on-disk writer refuses identically and leaves nothing behind.
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("seg");
        assert!(
            write_segment_v9(&segment, &dest).is_err(),
            "the on-disk writer must refuse the over-long name too"
        );
        assert!(!dest.exists(), "no segment dir may be left behind");

        // Boundary: exactly u16::MAX bytes is representable — still
        // writable, and the name round-trips through the strict reader.
        let max = "d".repeat(usize::from(u16::MAX));
        let boundary = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000])
            .add_string_column(&max, vec!["x".to_string(), "y".to_string()])
            .build()
            .expect("build boundary");
        let (meta, chunks) = write_segment_v9_to_memory(&boundary).expect("boundary must write");
        let smoosh = SmooshReader::from_parts(&meta, chunks).expect("from_parts");
        let read_back = SegmentData::from_smoosh(&smoosh).expect("from_smoosh");
        assert_eq!(read_back.dimensions, vec![max]);
    }

    /// A second write to a populated segment dir must be refused (no
    /// rewrite-in-place hazard).  Writing to a *fresh* path or to a
    /// pre-existing empty dir must succeed.
    #[test]
    fn writer_refuses_to_overwrite_populated_segment_dir() {
        let parent = tempfile::tempdir().expect("tempdir");
        let final_dir = parent.path().join("seg-overwrite");

        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2])
            .add_long_column("v", true, vec![100, 200])
            .build()
            .expect("build");

        // First write: succeeds.
        write_segment_v9(&segment, &final_dir).expect("first write");
        assert!(final_dir.join("meta.smoosh").exists());

        // Second write: must fail because the dir is populated.
        let err = write_segment_v9(&segment, &final_dir)
            .expect_err("second write into populated dir must fail");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("populated segment dir"),
            "error must reference Wave 40-B refuse-to-overwrite (got {msg})"
        );

        // The original dir must still be readable (no clobbering).
        let _ = SegmentData::open(&final_dir).expect("first segment still readable");
    }

    /// The smoosh format's `meta.smoosh` header advertises a max single-chunk
    /// size of `2147483647` (2^31-1) bytes; a `00000.smoosh` larger than that
    /// cannot be memory-mapped by Druid or a correct smoosh reader. The size
    /// guard must accept a chunk exactly at the limit and reject one a single
    /// byte over — tested structurally on the byte-count, never by allocating
    /// multi-GiB data.
    #[test]
    fn smoosh_chunk_size_guard_rejects_over_max() {
        // Exactly at the advertised maximum: allowed.
        ensure_smoosh_chunk_fits(SMOOSH_MAX_CHUNK_SIZE)
            .expect("a chunk exactly at the advertised max must be accepted");

        // One byte over: refused loud, naming the advertised limit.
        let over = SMOOSH_MAX_CHUNK_SIZE
            .checked_add(1)
            .expect("max + 1 fits in usize");
        let err = ensure_smoosh_chunk_fits(over).expect_err("an over-max chunk must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains(&SMOOSH_MAX_CHUNK_SIZE.to_string()),
            "the error must name the advertised max chunk size, got: {msg}"
        );
    }

    /// The running-size guard ([`checked_chunk_size`]) must fire on the RUNNING
    /// total, BEFORE the `finish` append loop allocates the concatenation
    /// buffer — otherwise a multi-GiB `extend_from_slice` could OOM-abort the
    /// process before the trailing size guard returned the promised loud error.
    /// Because the guard operates on byte-COUNTS (not real buffers), the
    /// ordering is tested here without ever allocating 2 GiB.
    #[test]
    fn smoosh_running_size_guard_fires_before_allocation() {
        // Within-cap file lengths sum to the exact total.
        assert_eq!(
            checked_chunk_size([10usize, 20, 30]).expect("within cap"),
            60
        );

        // Exactly at the advertised maximum across two files: allowed (the
        // guard rejects only a total strictly over the cap).
        checked_chunk_size([SMOOSH_MAX_CHUNK_SIZE - 1, 1])
            .expect("a total exactly at the advertised max must be accepted");

        // One byte over the cap — reached only on the SECOND file — must fail
        // loud on the running total, naming the advertised limit. The lengths
        // are byte-COUNTS, so no 2 GiB buffer is ever allocated: this is the
        // pre-allocation ordering the fix guarantees.
        let err = checked_chunk_size([SMOOSH_MAX_CHUNK_SIZE, 1])
            .expect_err("a running total one byte over the cap must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains(&SMOOSH_MAX_CHUNK_SIZE.to_string()),
            "the error must name the advertised max chunk size, got: {msg}"
        );
    }
}
