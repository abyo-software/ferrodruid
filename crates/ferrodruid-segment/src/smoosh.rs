// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Smoosh file format reader.
//!
//! Druid concatenates multiple logical files into numbered chunk files
//! (`00000.smoosh`, `00001.smoosh`, …). A `meta.smoosh` text file provides an
//! index mapping logical filenames to (chunk number, start offset, end offset).
//!
//! Wire format of `meta.smoosh`:
//! ```text
//! v1,<max_chunk_size>,<num_files>
//! <filename>,<file_number>,<start_offset>,<end_offset>
//! ...
//! ```

use std::collections::HashMap;
use std::path::Path;

use ferrodruid_common::error::{DruidError, Result};

// ---------------------------------------------------------------------------
// Bounded-reader caps
// ---------------------------------------------------------------------------
//
// The `meta.smoosh` text file is the index for a Druid segment archive on
// disk.  Several integer fields in the file are read into `Vec::with_capacity`
// or used to drive a filesystem walk — feeding an attacker-controlled count
// straight into either is a classic OOM / fanout DoS.  Wave 37 Codex DD R1
// flagged `num_files` and the derived `num_chunks` (`smoosh.rs:63-71,
// 148-156`) as exactly that.  We bound both to numbers that are 100-1000×
// larger than any realistic Druid segment, mirroring the
// `MAX_DIMENSIONS`/`MAX_METRICS` discipline already established in
// [`crate::v9`] and [`crate::fdx`].

/// Hard upper bound on the number of logical files declared by a single
/// `meta.smoosh` header.  Real Druid segments rarely exceed a few hundred
/// files; the cap is set to 16 384 to allow headroom while keeping the
/// `HashMap::with_capacity` allocation bounded.  Wave 37 R1 finding
/// `smoosh.rs:63-71` (resource-exhaustion DoS).
pub(crate) const MAX_SMOOSH_ENTRIES: usize = 16_384;

/// Hard upper bound on the number of numbered chunk files
/// (`00000.smoosh`, `00001.smoosh`, …) referenced from `meta.smoosh`.  Real
/// Druid uses ~64 chunks per archive at most; the cap is set to 65 536.
/// This bounds both the `Vec::with_capacity` allocation and the
/// per-chunk filesystem walk in [`SmooshReader::open`].  Wave 37 R1
/// finding `smoosh.rs:148-156` (resource-exhaustion + fanout DoS).
const MAX_SMOOSH_CHUNKS: usize = 65_536;

/// Hard upper bound, in bytes, on a single *sidecar* file loaded by
/// [`SmooshReader::open`]'s directory sweep.  Legitimate sidecars are
/// segment control files (`version.bin` = 4 bytes, `factory.json` ≈ 30
/// bytes), so 1 MiB is >30 000× headroom.  The sweep reads untrusted
/// operator-supplied directories (compat-2 attach), and pre-cap it
/// `fs::read` every non-chunk regular file with NO size bound — a stray
/// multi-GiB operator log / core dump / leftover tarball next to
/// `meta.smoosh` OOMed the historical at segment-open time.  Oversized
/// files are SKIPPED with a warning (not a hard error): a stray file is
/// not part of the segment, and failing the whole open would brick an
/// otherwise healthy attach.
const MAX_SIDECAR_BYTES: u64 = 1024 * 1024;

/// Hard upper bound on the number of sidecar files the directory sweep
/// will load.  Real layouts carry at most a handful (`version.bin`,
/// `factory.json`); the cap bounds total sidecar memory to
/// `MAX_SIDECAR_FILES × MAX_SIDECAR_BYTES` against a directory stuffed
/// with thousands of stray files.  Excess files are skipped with a
/// warning, same rationale as [`MAX_SIDECAR_BYTES`].
const MAX_SIDECAR_FILES: usize = 256;

/// The smoosh format's advertised maximum size, in bytes, of a single
/// numbered chunk file (`00000.smoosh`, …).  It is written verbatim into
/// every `meta.smoosh` header (`v1,<max_chunk_size>,<num_files>`) and equals
/// `i32::MAX` (2^31-1): a single chunk larger than this cannot be
/// memory-mapped by Druid (Java `MappedByteBuffer` offsets are `int`) nor by
/// a correct smoosh reader, so a writer that would emit one MUST fail loud
/// rather than produce an archive nothing can open.  Referenced by
/// [`crate::writer::SmooshWriter`] both when emitting the header and when
/// enforcing the single-chunk size guard (multi-chunk splitting is out of
/// milestone scope).
pub(crate) const SMOOSH_MAX_CHUNK_SIZE: usize = 2_147_483_647;

// ---------------------------------------------------------------------------
// Entry-name validation (shared writer guard)
// ---------------------------------------------------------------------------

/// Segment control-file / smoosh-entry names that a column may NOT reuse: a
/// column named after any of these would collide with (and shadow or
/// corrupt) the segment's own bookkeeping — [`parse_meta`]'s entry map is
/// last-wins and columns are appended after the control entries, so the
/// column silently overwrites the control entry and the segment cannot be
/// reopened.  `__time` is a legitimate real Druid column and is
/// deliberately NOT listed here (writers reject it separately as a
/// declared dimension/metric name).  Mirrors the native writer's
/// `RESERVED_SEGMENT_FILENAMES` list in [`crate::druid_native_writer`],
/// which keeps its own copy for the upstream-layout path.
pub(crate) const RESERVED_SEGMENT_ENTRY_NAMES: [&str; 5] = [
    "version.bin",
    "factory.json",
    "meta.smoosh",
    "metadata.drd",
    "index.drd",
];

/// Validate that a column name is representable as a smoosh entry name and
/// does not collide with the segment's own control files.
///
/// `meta.smoosh` is a comma/newline-delimited text index, so `,`, `\n`,
/// and `\r` cannot appear in an entry name; `/` would nest paths.  This is
/// the same hazard the native writer's `validate_column_names` guard
/// ([`crate::druid_native_writer`]) closes for the upstream-layout path;
/// the private v9 writer ([`crate::writer`]) calls this shared helper so
/// the production spill path (Kafka consumer / Historical persist /
/// overlord publish) fails LOUD at write time instead of emitting a
/// `meta.smoosh` its own reader cannot parse — a "successful" write whose
/// data loss surfaces only at restart-reload.
///
/// # Errors
///
/// Returns [`DruidError::Segment`] naming the offending column when the
/// name is empty, is a reserved control filename, or carries a character
/// the smoosh meta index cannot represent.
pub(crate) fn validate_smoosh_entry_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(DruidError::Segment(
            "empty column name is not representable as a smoosh entry".to_string(),
        ));
    }
    if RESERVED_SEGMENT_ENTRY_NAMES.contains(&name) {
        return Err(DruidError::Segment(format!(
            "column name `{name}` is a reserved segment control filename — refusing to \
             write a column that collides with the segment's own bookkeeping"
        )));
    }
    if name.contains(',') || name.contains('\n') || name.contains('\r') || name.contains('/') {
        return Err(DruidError::Segment(format!(
            "column name {name:?} contains a character that the smoosh meta index \
             cannot represent"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// SmooshEntry
// ---------------------------------------------------------------------------

/// Location of a single logical file inside a smoosh archive.
#[derive(Debug, Clone)]
struct SmooshEntry {
    /// Which chunk file (0-indexed) contains the data.
    file_number: usize,
    /// Byte offset within the chunk where data starts.
    start_offset: usize,
    /// Byte offset within the chunk where data ends (exclusive).
    end_offset: usize,
}

// ---------------------------------------------------------------------------
// SmooshReader
// ---------------------------------------------------------------------------

/// A reader for Druid's smoosh (concatenated file) format.
///
/// `SmooshReader` parses the `meta.smoosh` index and provides random access to
/// the logical files stored across one or more chunk files.
///
/// **Sidecar files (Druid 30+ local deep-storage layout)**: some Druid
/// releases (verified against 31.0.2 on 2026-07-01 while closing
/// Task #8 / TG-1-finding-W2A-1) store certain segment-level files
/// (`version.bin`, `factory.json`) *outside* the smoosh archive as
/// siblings of `meta.smoosh` on disk, rather than embedded as
/// `meta.smoosh` entries. [`SmooshReader::open`] transparently picks
/// those up: any regular file in the same directory that is NOT the
/// meta index (`meta.smoosh`) or a chunk (`NNNNN.smoosh`) is loaded
/// as a *sidecar*. Sidecars are addressed by the same
/// [`Self::read_file`] / [`Self::has_file`] / [`Self::file_names`]
/// API as embedded entries and are considered after — but do not
/// shadow — real meta entries. This restores the invariant that a
/// segment dir on disk exposes the union of its embedded + sidecar
/// files, whichever layout the upstream engine chose. The FerroDruid
/// segment writer continues to embed everything inside the smoosh
/// (round-tripping to Druid via the harness's
/// `pack_smoosh_dir_to_index_zip` path); this fallback is purely for
/// reading segments that some Druid version wrote with sidecars.
#[derive(Debug)]
pub struct SmooshReader {
    /// Parsed `meta.smoosh` entries keyed by logical filename.
    entries: HashMap<String, SmooshEntry>,
    /// Raw bytes of each numbered chunk file (`00000.smoosh`, …).
    chunks: Vec<Vec<u8>>,
    /// Sidecar files (Druid 30+ external `version.bin` / `factory.json`
    /// pattern) keyed by their filename in the same directory as the
    /// smoosh archive. `read_file` returns these when a name is absent
    /// from the primary [`Self::entries`] index; embedded entries take
    /// precedence so a file present in both places (unusual) preserves
    /// the meta.smoosh contract.
    sidecars: HashMap<String, Vec<u8>>,
}

impl SmooshReader {
    /// Open a smoosh archive from a directory on disk.
    ///
    /// Reads `<dir>/meta.smoosh` and all referenced chunk files.
    pub fn open(dir: &Path) -> Result<Self> {
        let meta_path = dir.join("meta.smoosh");
        let meta = std::fs::read_to_string(&meta_path).map_err(|e| {
            DruidError::Segment(format!("failed to read {}: {e}", meta_path.display()))
        })?;

        let (_, num_chunks, entries) = parse_meta(&meta)?;

        // `parse_meta` already bounds `num_chunks` against
        // `MAX_SMOOSH_CHUNKS`, but defense-in-depth: re-check here so a
        // future caller that hand-rolls a `(num_chunks, entries)` pair
        // can never drive an unbounded filesystem walk.  Wave 37 R1
        // (`smoosh.rs:63-71`).
        if num_chunks > MAX_SMOOSH_CHUNKS {
            return Err(DruidError::Segment(format!(
                "smoosh meta: num_chunks {num_chunks} exceeds cap {MAX_SMOOSH_CHUNKS}"
            )));
        }

        let mut chunks = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let chunk_path = dir.join(format!("{i:05}.smoosh"));
            let data = std::fs::read(&chunk_path).map_err(|e| {
                DruidError::Segment(format!("failed to read {}: {e}", chunk_path.display()))
            })?;
            chunks.push(data);
        }

        // Sidecar sweep: pick up any regular file in `dir` that is NOT
        // the meta index or a numbered chunk. Druid 30+ local
        // deep-storage layout keeps `version.bin` / `factory.json` as
        // siblings of `meta.smoosh` rather than embedding them; the
        // FerroDruid segment writer embeds everything, so this is a
        // read-side compat feature only.
        //
        // The sweep is BOUNDED (attach reads untrusted directories):
        // every candidate's on-disk size is checked BEFORE any read, a
        // file over `MAX_SIDECAR_BYTES` or past the `MAX_SIDECAR_FILES`
        // count is skipped with a warning (legitimate sidecars are tiny
        // control files, and a stray operator log / core dump must not
        // OOM — or brick — the open), and the writer's own `.tmp.`
        // staging leftovers are never surfaced as segment content.
        let mut sidecars = HashMap::new();
        if let Ok(read_dir) = std::fs::read_dir(dir) {
            for entry in read_dir.flatten() {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if name == "meta.smoosh" {
                    continue;
                }
                // Skip numbered chunks `NNNNN.smoosh` (5 digits + `.smoosh`).
                if name.ends_with(".smoosh")
                    && name.len() == "NNNNN.smoosh".len()
                    && name
                        .trim_end_matches(".smoosh")
                        .chars()
                        .all(|c| c.is_ascii_digit())
                {
                    continue;
                }
                // Skip the durable-writer's crash-leftover staging files
                // (`<name>.tmp.<pid>.<counter>.<nanos>`) — recognisable
                // garbage by contract, never segment content.
                if name.contains(".tmp.") {
                    continue;
                }
                if sidecars.len() >= MAX_SIDECAR_FILES {
                    tracing::warn!(
                        dir = %dir.display(),
                        file = name,
                        cap = MAX_SIDECAR_FILES,
                        "smoosh sidecar sweep: skipping file past the sidecar count cap"
                    );
                    continue;
                }
                // Size gate BEFORE the read, so an oversized stray file is
                // never slurped into memory at all.
                let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                if size > MAX_SIDECAR_BYTES {
                    tracing::warn!(
                        dir = %dir.display(),
                        file = name,
                        size,
                        cap = MAX_SIDECAR_BYTES,
                        "smoosh sidecar sweep: skipping oversized file (not segment \
                         control data)"
                    );
                    continue;
                }
                let data = std::fs::read(&path).map_err(|e| {
                    DruidError::Segment(format!("failed to read sidecar {}: {e}", path.display()))
                })?;
                sidecars.insert(name.to_string(), data);
            }
        }

        Ok(Self {
            entries,
            chunks,
            sidecars,
        })
    }

    /// Build a `SmooshReader` from in-memory parts (useful for testing).
    ///
    /// `meta` is the text content of `meta.smoosh`; `chunks` are the raw bytes
    /// of each numbered chunk file in order.
    pub fn from_parts(meta: &str, chunks: Vec<Vec<u8>>) -> Result<Self> {
        let (_, _, entries) = parse_meta(meta)?;
        Ok(Self {
            entries,
            chunks,
            sidecars: HashMap::new(),
        })
    }

    /// List all logical filenames in the archive (embedded meta
    /// entries plus any sidecars loaded from disk siblings of
    /// `meta.smoosh`).
    pub fn file_names(&self) -> Vec<&str> {
        self.entries
            .keys()
            .map(|s| s.as_str())
            .chain(
                self.sidecars
                    .keys()
                    .filter(|k| !self.entries.contains_key(k.as_str()))
                    .map(|s| s.as_str()),
            )
            .collect()
    }

    /// Read the bytes of a logical file by name. Embedded meta.smoosh
    /// entries take precedence; if `name` is absent from the meta index
    /// it is looked up in the sidecar map (Druid 30+ external
    /// `version.bin` / `factory.json` layout).
    pub fn read_file(&self, name: &str) -> Result<&[u8]> {
        let Some(entry) = self.entries.get(name) else {
            if let Some(bytes) = self.sidecars.get(name) {
                return Ok(bytes.as_slice());
            }
            return Err(DruidError::Segment(format!(
                "smoosh: file not found: {name}"
            )));
        };

        let chunk = self.chunks.get(entry.file_number).ok_or_else(|| {
            DruidError::Segment(format!(
                "smoosh: chunk {} out of range (have {})",
                entry.file_number,
                self.chunks.len()
            ))
        })?;

        // Wave 37 R1 (`smoosh.rs:92-115`): a crafted `meta.smoosh` entry
        // with `start_offset > end_offset` would panic on the `&chunk[s..e]`
        // slice with "slice index starts at X but ends at Y".  Reject the
        // reversed range explicitly before slicing so a malformed segment
        // can never DoS the reader.
        if entry.start_offset > entry.end_offset {
            return Err(DruidError::Segment(format!(
                "smoosh: file {name} has reversed offsets in chunk {} (start {} > end {})",
                entry.file_number, entry.start_offset, entry.end_offset
            )));
        }

        if entry.end_offset > chunk.len() {
            return Err(DruidError::Segment(format!(
                "smoosh: file {name} extends past end of chunk {} ({} > {})",
                entry.file_number,
                entry.end_offset,
                chunk.len()
            )));
        }

        Ok(&chunk[entry.start_offset..entry.end_offset])
    }

    /// Check whether a logical file exists in the archive (either as
    /// an embedded meta entry or as a sidecar).
    pub fn has_file(&self, name: &str) -> bool {
        self.entries.contains_key(name) || self.sidecars.contains_key(name)
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Parse `meta.smoosh` text, returning `(max_chunk_size, num_chunk_files, entries)`.
fn parse_meta(meta: &str) -> Result<(usize, usize, HashMap<String, SmooshEntry>)> {
    let mut lines = meta.lines();

    // Header line: "v1,<max_chunk_size>,<num_files>"
    let header = lines
        .next()
        .ok_or_else(|| DruidError::Segment("smoosh meta is empty".to_string()))?;

    let header_parts: Vec<&str> = header.split(',').collect();
    if header_parts.len() < 3 || header_parts[0] != "v1" {
        return Err(DruidError::Segment(format!(
            "smoosh meta: unsupported header: {header}"
        )));
    }

    let max_chunk_size: usize = header_parts[1]
        .parse()
        .map_err(|e| DruidError::Segment(format!("smoosh meta: bad max_chunk_size: {e}")))?;

    let num_files: usize = header_parts[2]
        .parse()
        .map_err(|e| DruidError::Segment(format!("smoosh meta: bad num_files: {e}")))?;

    // Wave 37 R1 (`smoosh.rs:63-71`): cap `num_files` BEFORE the
    // `HashMap::with_capacity` reservation so a crafted header claiming
    // billions of files cannot OOM the reader.  Real segments have <=
    // a few hundred logical files; the cap is set to a number 100-1000×
    // larger than realistic use.
    if num_files > MAX_SMOOSH_ENTRIES {
        return Err(DruidError::Segment(format!(
            "smoosh meta: num_files {num_files} exceeds cap {MAX_SMOOSH_ENTRIES}"
        )));
    }

    // Entry lines: "<filename>,<file_number>,<start_offset>,<end_offset>"
    let mut entries = HashMap::with_capacity(num_files);

    // We need to figure out how many chunk files are referenced.
    let mut max_chunk_index: usize = 0;

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 4 {
            return Err(DruidError::Segment(format!(
                "smoosh meta: malformed entry line: {line}"
            )));
        }

        let filename = parts[0].to_string();
        let file_number: usize = parts[1]
            .parse()
            .map_err(|e| DruidError::Segment(format!("smoosh meta: bad file_number: {e}")))?;

        // Wave 37 R1 (`smoosh.rs:148-156`): cap `file_number` per-entry so
        // the derived `max_chunk_index` (and consequently `num_chunks`,
        // which drives `Vec::with_capacity` and a per-chunk filesystem
        // walk in `SmooshReader::open`) cannot be amplified by a single
        // crafted entry line.
        if file_number >= MAX_SMOOSH_CHUNKS {
            return Err(DruidError::Segment(format!(
                "smoosh meta: file_number {file_number} exceeds cap {MAX_SMOOSH_CHUNKS}"
            )));
        }

        let start_offset: usize = parts[2]
            .parse()
            .map_err(|e| DruidError::Segment(format!("smoosh meta: bad start_offset: {e}")))?;
        let end_offset: usize = parts[3]
            .parse()
            .map_err(|e| DruidError::Segment(format!("smoosh meta: bad end_offset: {e}")))?;

        if file_number >= max_chunk_index {
            max_chunk_index = file_number + 1;
        }

        entries.insert(
            filename,
            SmooshEntry {
                file_number,
                start_offset,
                end_offset,
            },
        );
        // Wave 37 R1 follow-up: the header check above only bounds the
        // DECLARED `num_files`; the entry map itself must be bounded
        // against the ACTUAL lines, or a hostile meta declaring
        // `num_files=1` while carrying millions of entry lines grows the
        // map unboundedly (~10× memory amplification over the file bytes)
        // — bypassing the exact OOM-DoS cap the header check enforces.
        if entries.len() > MAX_SMOOSH_ENTRIES {
            return Err(DruidError::Segment(format!(
                "smoosh meta: entry count exceeds cap {MAX_SMOOSH_ENTRIES} (header declared \
                 {num_files} files)"
            )));
        }
    }

    // The number of chunk files needed is max(file_number) + 1, or at least 1
    // if there are any entries.
    let num_chunks = if entries.is_empty() {
        0
    } else {
        max_chunk_index
    };

    Ok((max_chunk_size, num_chunks, entries))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta() -> String {
        [
            "v1,2147483647,3",
            "version.bin,0,0,4",
            "index.drd,0,4,50",
            "col/__time,0,50,200",
        ]
        .join("\n")
    }

    fn sample_chunk() -> Vec<u8> {
        let mut data = vec![0u8; 200];
        // version.bin at [0..4]: version 9 BE
        data[0..4].copy_from_slice(&9_i32.to_be_bytes());
        // rest is filler
        data
    }

    #[test]
    fn from_parts_lists_files() {
        let meta = sample_meta();
        let reader = SmooshReader::from_parts(&meta, vec![sample_chunk()]).unwrap();
        let mut names: Vec<&str> = reader.file_names();
        names.sort();
        assert_eq!(names, vec!["col/__time", "index.drd", "version.bin"]);
    }

    #[test]
    fn read_file_returns_correct_slice() {
        let meta = sample_meta();
        let reader = SmooshReader::from_parts(&meta, vec![sample_chunk()]).unwrap();
        let version_bytes = reader.read_file("version.bin").unwrap();
        assert_eq!(version_bytes.len(), 4);
        let version = i32::from_be_bytes([
            version_bytes[0],
            version_bytes[1],
            version_bytes[2],
            version_bytes[3],
        ]);
        assert_eq!(version, 9);
    }

    #[test]
    fn has_file_works() {
        let meta = sample_meta();
        let reader = SmooshReader::from_parts(&meta, vec![sample_chunk()]).unwrap();
        assert!(reader.has_file("version.bin"));
        assert!(!reader.has_file("nonexistent"));
    }

    #[test]
    fn missing_file_returns_error() {
        let meta = sample_meta();
        let reader = SmooshReader::from_parts(&meta, vec![sample_chunk()]).unwrap();
        assert!(reader.read_file("nope").is_err());
    }

    #[test]
    fn empty_meta_fails() {
        assert!(SmooshReader::from_parts("", vec![]).is_err());
    }

    #[test]
    fn bad_version_in_meta() {
        assert!(SmooshReader::from_parts("v2,100,0", vec![]).is_err());
    }

    #[test]
    fn multi_chunk_smoosh() {
        let meta = ["v1,100,2", "file_a,0,0,5", "file_b,1,0,3"].join("\n");
        let chunk0 = b"hello".to_vec();
        let chunk1 = b"bye".to_vec();
        let reader = SmooshReader::from_parts(&meta, vec![chunk0, chunk1]).unwrap();
        assert_eq!(reader.read_file("file_a").unwrap(), b"hello");
        assert_eq!(reader.read_file("file_b").unwrap(), b"bye");
    }

    #[test]
    fn chunk_out_of_range() {
        let meta = ["v1,100,1", "file_a,5,0,3"].join("\n");
        let reader = SmooshReader::from_parts(&meta, vec![b"abc".to_vec()]).unwrap();
        assert!(reader.read_file("file_a").is_err());
    }

    #[test]
    fn offset_past_end_of_chunk() {
        let meta = ["v1,100,1", "file_a,0,0,999"].join("\n");
        let reader = SmooshReader::from_parts(&meta, vec![b"short".to_vec()]).unwrap();
        assert!(reader.read_file("file_a").is_err());
    }

    // -----------------------------------------------------------------------
    // Wave 36-E / Wave 37 R1: hostile `meta.smoosh` hardening.
    // Internal security review (Wave 37 R1) findings:
    //   - High: `meta.smoosh` can panic on reversed offsets
    //   - High: `meta.smoosh` header is an attacker-controlled allocation
    //     multiplier
    // -----------------------------------------------------------------------

    #[test]
    fn reversed_offset_returns_corrupt_offset_not_panic() {
        // Craft an entry where start > end.  The previous code path slices
        // `&chunk[100..10]` which panics with "slice index starts at 100
        // but ends at 10".  After the fix this is a clean Err.
        let meta = ["v1,200,1", "evil,0,100,10"].join("\n");
        let chunk = vec![0u8; 200];
        let reader = SmooshReader::from_parts(&meta, vec![chunk]).expect("from_parts");

        let err = reader
            .read_file("evil")
            .expect_err("must reject reversed offsets");
        let msg = err.to_string();
        assert!(
            msg.contains("reversed offsets") && msg.contains("evil"),
            "expected reversed-offset error, got: {msg}"
        );
    }

    #[test]
    fn oversized_num_files_rejected() {
        // Header claims u32::MAX files.  We build the smallest possible
        // crafted meta string so we can also assert the rejection itself
        // didn't allocate megabytes of HashMap capacity before tripping.
        let meta = format!("v1,2147483647,{}", u32::MAX);
        assert!(
            meta.len() < 64,
            "crafted meta should be tiny: {} bytes",
            meta.len()
        );

        let err = SmooshReader::from_parts(&meta, vec![]).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("num_files") && msg.contains("exceeds cap"),
            "expected num_files cap error, got: {msg}"
        );
    }

    #[test]
    fn oversized_num_chunks_rejected() {
        // A single entry whose `file_number` is u32::MAX-1 would otherwise
        // drive `max_chunk_index = u32::MAX` and then a 4-billion-iter
        // filesystem walk + `Vec::with_capacity(u32::MAX)`.  Reject at
        // entry-parse time before any allocation.
        let meta = format!("v1,2147483647,1\nevil,{},0,0", u32::MAX as usize - 1);
        assert!(
            meta.len() < 96,
            "crafted meta should be tiny: {} bytes",
            meta.len()
        );

        let err = SmooshReader::from_parts(&meta, vec![]).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("file_number") && msg.contains("exceeds cap"),
            "expected file_number cap error, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 48 — proptest hardening (smoosh meta + reader)
    //
    // * `prop_smoosh_offsets_never_panic` — for any (start, end, chunk_len)
    //   triple, `read_file` must return Result without panicking on the
    //   slice bounds (defends Wave 36-E reversed-offsets and past-end
    //   guards under randomized inputs).
    // * `prop_smoosh_arbitrary_meta_no_panic` — any random ASCII string
    //   fed as `meta` text must be either parsed Ok or rejected with
    //   `DruidError::Segment`, never panic.
    // -----------------------------------------------------------------------
    mod proptests {
        use super::super::*;
        use proptest::prelude::*;

        proptest! {
            /// Any combination of `start_offset` / `end_offset` /
            /// `chunk_len` (all bounded so proptest stays fast) must
            /// produce a Result, never panic.
            #[test]
            fn prop_smoosh_offsets_never_panic(
                start in 0u32..1024,
                end in 0u32..1024,
                chunk_len in 0u32..1024,
            ) {
                let meta = format!("v1,2147483647,1\nfile_a,0,{start},{end}");
                let chunk = vec![0u8; chunk_len as usize];
                if let Ok(reader) = SmooshReader::from_parts(&meta, vec![chunk]) {
                    let _ = reader.read_file("file_a");
                }
            }

            /// Any arbitrary ASCII string must be either parsed or
            /// rejected — never panic the parser.
            #[test]
            fn prop_smoosh_arbitrary_meta_no_panic(
                meta in r"[A-Za-z0-9,.\n]{0,256}"
            ) {
                let _ = SmooshReader::from_parts(&meta, vec![]);
            }
        }
    }

    /// The header cap alone is bypassable: a hostile meta declaring
    /// `num_files=1` but carrying more than `MAX_SMOOSH_ENTRIES` entry
    /// LINES must be rejected by the loop-side count, or the entry map
    /// grows unboundedly (memory amplification OOM-DoS on the untrusted
    /// attach path).
    #[test]
    fn actual_entry_lines_over_cap_rejected_despite_small_header() {
        let mut meta = String::from("v1,2147483647,1\n");
        for i in 0..=MAX_SMOOSH_ENTRIES {
            meta.push_str(&format!("f{i},0,0,0\n"));
        }
        let err = SmooshReader::from_parts(&meta, vec![]).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("entry count exceeds cap"),
            "expected loop-side entry cap error, got: {msg}"
        );

        // Control: exactly MAX_SMOOSH_ENTRIES actual lines still parse
        // (the cap is a bound, not a behaviour change for real segments).
        let mut ok_meta = String::from("v1,2147483647,1\n");
        for i in 0..MAX_SMOOSH_ENTRIES {
            ok_meta.push_str(&format!("f{i},0,0,0\n"));
        }
        SmooshReader::from_parts(&ok_meta, vec![vec![]]).expect("at-cap meta must parse");
    }

    /// A stray oversized file next to `meta.smoosh` (operator log, core
    /// dump, leftover tarball) must NOT be slurped into memory as a
    /// sidecar — pre-fix `open` `fs::read` every non-chunk regular file
    /// unbounded, so a multi-GiB stray OOMed the historical at
    /// segment-open time.  The open itself must still succeed.
    #[test]
    fn open_skips_oversized_sidecar_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = ["v1,2147483647,1", "file_a,0,0,5"].join("\n");
        std::fs::write(dir.path().join("meta.smoosh"), meta).expect("meta");
        std::fs::write(dir.path().join("00000.smoosh"), b"hello").expect("chunk");
        // A legitimate tiny sidecar.
        std::fs::write(dir.path().join("version.bin"), 9_i32.to_be_bytes()).expect("sidecar");
        // A stray file one byte over the sidecar size cap.
        let big = vec![0u8; usize::try_from(MAX_SIDECAR_BYTES).expect("cap fits usize") + 1];
        std::fs::write(dir.path().join("stray.log"), &big).expect("stray");

        let reader = SmooshReader::open(dir.path()).expect("open must succeed");
        assert_eq!(reader.read_file("file_a").expect("embedded"), b"hello");
        assert!(
            reader.has_file("version.bin"),
            "legitimate small sidecar must still load"
        );
        assert!(
            !reader.has_file("stray.log"),
            "oversized stray file must be skipped, not loaded into memory"
        );
    }

    /// The durable-writer's crash-leftover staging files
    /// (`<name>.tmp.<unique>`) are recognisable garbage and must never be
    /// surfaced as segment sidecars.
    #[test]
    fn open_skips_tmp_staging_leftovers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = ["v1,2147483647,1", "file_a,0,0,5"].join("\n");
        std::fs::write(dir.path().join("meta.smoosh"), meta).expect("meta");
        std::fs::write(dir.path().join("00000.smoosh"), b"hello").expect("chunk");
        std::fs::write(dir.path().join("00001.smoosh.tmp.42.0.7"), b"junk").expect("tmp");

        let reader = SmooshReader::open(dir.path()).expect("open must succeed");
        assert!(
            !reader.has_file("00001.smoosh.tmp.42.0.7"),
            ".tmp. staging leftovers must not become sidecars"
        );
    }

    /// A directory stuffed with stray files must not translate into
    /// unbounded sidecar memory: at most `MAX_SIDECAR_FILES` are loaded,
    /// the rest are skipped, and the open still succeeds.
    #[test]
    fn open_bounds_sidecar_file_count() {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = ["v1,2147483647,1", "file_a,0,0,5"].join("\n");
        std::fs::write(dir.path().join("meta.smoosh"), meta).expect("meta");
        std::fs::write(dir.path().join("00000.smoosh"), b"hello").expect("chunk");
        for i in 0..(MAX_SIDECAR_FILES + 5) {
            std::fs::write(dir.path().join(format!("stray_{i}")), b"x").expect("stray");
        }

        let reader = SmooshReader::open(dir.path()).expect("open must succeed");
        let sidecar_count = reader
            .file_names()
            .iter()
            .filter(|n| n.starts_with("stray_"))
            .count();
        assert!(
            sidecar_count <= MAX_SIDECAR_FILES,
            "sidecar count {sidecar_count} exceeds cap {MAX_SIDECAR_FILES}"
        );
        assert_eq!(reader.read_file("file_a").expect("embedded"), b"hello");
    }

    #[test]
    fn open_rejects_oversized_num_chunks_before_filesystem_walk() {
        // Defense-in-depth: even if some future caller manufactures a
        // (num_chunks, entries) pair that bypasses parse_meta's cap, the
        // `SmooshReader::open` check must trip before walking the
        // filesystem.  We exercise the path indirectly by routing
        // through parse_meta — the cap message is the same.
        let mut meta = String::from("v1,2147483647,2\n");
        meta.push_str("a,0,0,0\n");
        meta.push_str(&format!("b,{},0,0", MAX_SMOOSH_CHUNKS + 1));
        let err = SmooshReader::from_parts(&meta, vec![]).expect_err("must reject");
        assert!(
            err.to_string().contains("exceeds cap"),
            "expected cap error, got: {err}"
        );
    }
}
