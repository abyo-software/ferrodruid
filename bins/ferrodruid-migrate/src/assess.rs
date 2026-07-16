// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `ferrodruid-migrate assess` — dry-run readability assessment of a
//! Druid **local deep-storage** directory.
//!
//! Scans a deep-storage root for segment artifacts and reports, per
//! segment, whether it is **readable by FerroDruid's v9 reader —
//! attachable when the attach feature ships**.  This is a judgment
//! tool only: nothing is migrated, copied, or modified (segments are
//! extracted into throwaway temp dirs for reading and deleted
//! afterwards).
//!
//! ## Recognized on-disk layouts
//!
//! Per the Apache Druid documentation (druid.apache.org: segment
//! identifiers are `dataSource / interval / version / partitionNum`;
//! local deep storage writes segments under
//! `druid.storage.storageDirectory` either as zip files or as plain
//! directories depending on `druid.storage.zip`), plus this repo's own
//! segment-compat evidence against Druid 31.0.2:
//!
//! * `<root>/<dataSource>/<interval>/<version>/<partitionNum>/index.zip`
//!   — zipped smoosh archive (`druid.storage.zip=true` / older
//!   releases), optionally with a sibling `descriptor.json`;
//! * `<root>/.../<partitionNum>/` or `<root>/.../<partitionNum>/index/`
//!   — an already-unzipped directory of smoosh files (`meta.smoosh` +
//!   `NNNNN.smoosh`), the layout Druid 31.0.2's local deep storage
//!   emits.
//!
//! The scan is a tolerant recursive walk: any `index.zip` file and any
//! directory directly containing `meta.smoosh` is treated as a segment
//! artifact, so path-layout drift between Druid releases does not hide
//! segments.  Segment identity (dataSource/interval/version/partition)
//! is taken from `descriptor.json` when present, otherwise inferred
//! from the path structure; the report states which source was used.
//!
//! ## Honest limitations
//!
//! * Local (filesystem) deep storage only.  S3/HDFS/GCS deep storage is
//!   out of scope — pull the segments to a local dir first.
//! * The verdict is exactly the v9 reader's own verdict: unsupported
//!   encodings and corrupt files surface with the reader's fail-loud
//!   reason, verbatim.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use ferrodruid_segment::SegmentData;

/// Maximum directory depth (relative to the deep-storage root) the
/// scanner descends into.  The documented layout is 4 levels deep
/// (`dataSource/interval/version/partitionNum`) plus one optional
/// `index/` level; 16 leaves generous headroom while bounding walks
/// over pathological trees.
const MAX_SCAN_DEPTH: usize = 16;

/// Cap on the total uncompressed bytes extracted from a single
/// `index.zip`.  Real Druid segments are hundreds of MB to a few GB;
/// the cap only exists so a malformed/hostile zip cannot fill the temp
/// filesystem (zip-bomb defence, same spirit as the bounded-reader
/// caps in `ferrodruid-segment`).
const MAX_UNCOMPRESSED_BYTES: u64 = 64 * 1024 * 1024 * 1024; // 64 GiB

/// Cap on the number of entries a single `index.zip` may declare.
/// A real segment archive holds a handful of files (`meta.smoosh` +
/// `NNNNN.smoosh` chunks + sidecars); 65 536 mirrors the reader's
/// `MAX_SMOOSH_CHUNKS` bound and blocks inode-exhaustion archives
/// (Codex R1 finding).
const MAX_ZIP_ENTRIES: usize = 65_536;

/// Cap on the size of a `descriptor.json` the identity probe will
/// read.  Real descriptors are a few KB; the cap keeps a hostile
/// multi-GB "descriptor" from being slurped into memory (Codex R1
/// finding).
const MAX_DESCRIPTOR_BYTES: u64 = 4 * 1024 * 1024; // 4 MiB

/// Cap on the number of scan warnings kept verbatim; further warnings
/// are folded into a final "+N more" entry so a hostile tree cannot
/// bloat the report itself.
const MAX_WARNINGS: usize = 100;

/// Cap on the number of directories a single scan visits.  A real
/// deep-storage tree has `dataSources × intervals × versions ×
/// partitions` directories — even a large cluster stays well under
/// this; a hostile arbitrarily-wide tree aborts the walk loudly
/// instead of consuming unbounded time and stack memory (Codex R2
/// finding).
const MAX_SCAN_DIRS: usize = 1_000_000;

/// Cap on the entries considered within a single directory.  Bounds
/// the per-directory `Vec` (and its sort) against a hostile flat
/// directory with millions of entries (Codex R3 finding); entries past
/// the cap are skipped with a warning and the scan is marked aborted.
const MAX_DIR_ENTRIES: usize = 1_000_000;

/// Cap on the length (chars) of a descriptor-supplied identity field
/// retained in the report (Codex R4 finding — a hostile descriptor
/// could otherwise pin ~4 MiB of strings per segment).
const MAX_IDENTITY_FIELD_CHARS: usize = 1_024;

/// Cap on the path-component depth of a single zip entry.  Real
/// segment archives are flat (smoosh files at the archive root); a
/// hostile entry nested thousands of directories deep would burden
/// `create_dir_all` and the recursive tempdir cleanup (Codex R5
/// finding).
const MAX_ZIP_ENTRY_DEPTH: usize = 16;

// ---------------------------------------------------------------------------
// Report data model
// ---------------------------------------------------------------------------

/// What kind of on-disk artifact a segment was found as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactKind {
    /// A zipped smoosh archive (`index.zip`).
    IndexZip,
    /// An already-unzipped directory of smoosh files.
    SmooshDir,
}

impl ArtifactKind {
    fn as_str(self) -> &'static str {
        match self {
            ArtifactKind::IndexZip => "index.zip",
            ArtifactKind::SmooshDir => "smoosh-dir",
        }
    }
}

/// Where the segment identity (dataSource/interval/version/partition)
/// came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdentitySource {
    /// Parsed from a `descriptor.json` next to the artifact.
    DescriptorJson,
    /// A partial `descriptor.json` supplied some fields; the rest were
    /// filled from the path structure.
    DescriptorJsonPlusPath,
    /// Inferred from the `<dataSource>/<interval>/<version>/<partition>`
    /// path structure.
    Path,
    /// Neither source available — identity unknown, artifact still
    /// assessed.
    Unknown,
}

impl IdentitySource {
    fn as_str(self) -> &'static str {
        match self {
            IdentitySource::DescriptorJson => "descriptor.json",
            IdentitySource::DescriptorJsonPlusPath => "descriptor.json+path",
            IdentitySource::Path => "path",
            IdentitySource::Unknown => "unknown",
        }
    }
}

/// Segment identity as far as it could be established.
#[derive(Debug, Default)]
struct SegmentIdentity {
    data_source: Option<String>,
    interval: Option<String>,
    version: Option<String>,
    partition: Option<String>,
    source: Option<IdentitySource>,
}

impl SegmentIdentity {
    fn source(&self) -> IdentitySource {
        self.source.unwrap_or(IdentitySource::Unknown)
    }
}

/// Outcome of opening one segment with the v9 reader.
#[derive(Debug)]
enum Outcome {
    /// The v9 reader opened the segment.
    Readable {
        /// Row count reported by the reader.
        rows: usize,
        /// Ordered column names (`__time`, dimensions, metrics).
        columns: Vec<String>,
    },
    /// The v9 reader (or the zip extraction feeding it) failed; the
    /// fail-loud reason is preserved verbatim.
    Unreadable {
        /// Reason string, verbatim from the failing layer.
        reason: String,
    },
}

/// One assessed segment artifact.
#[derive(Debug)]
struct Assessment {
    /// Artifact path (the `index.zip` file or the smoosh dir).
    path: PathBuf,
    kind: ArtifactKind,
    identity: SegmentIdentity,
    outcome: Outcome,
}

/// A segment artifact discovered by the scan, before assessment.
#[derive(Debug)]
struct FoundArtifact {
    path: PathBuf,
    kind: ArtifactKind,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run the `assess` subcommand: scan `deep_storage`, assess up to
/// `max_segments` artifacts, print a human table or (with `json`) a
/// machine-readable report to stdout.
///
/// Returns `Err` only for operational failures (missing/unreadable
/// root).  Unreadable segments are report content, not errors — the
/// process exits 0 once the scan itself completed.
pub fn run(
    deep_storage: &Path,
    json: bool,
    max_segments: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !deep_storage.is_dir() {
        return Err(format!(
            "deep-storage path {} is not a directory (local deep storage only; \
             for S3/HDFS/GCS pull the segments to a local dir first)",
            deep_storage.display()
        )
        .into());
    }

    let scan_result = scan(deep_storage, max_segments)?;
    let assessments: Vec<Assessment> = scan_result
        .found
        .into_iter()
        .map(|f| {
            let identity = derive_identity(deep_storage, &f);
            let outcome = assess_artifact(&f);
            Assessment {
                path: f.path,
                kind: f.kind,
                identity,
                outcome,
            }
        })
        .collect();

    let report = ReportInput {
        root: deep_storage,
        assessments: &assessments,
        truncated: scan_result.truncated,
        max_segments,
        warnings: &scan_result.warnings,
        aborted: scan_result.aborted,
    };
    if json {
        print_json(&report);
    } else {
        print_human(&report);
    }
    Ok(())
}

/// Bundle of everything the two printers need.
struct ReportInput<'a> {
    root: &'a Path,
    assessments: &'a [Assessment],
    truncated: bool,
    max_segments: Option<usize>,
    warnings: &'a [String],
    /// The scan hit a hard resource cap and is incomplete.
    aborted: bool,
}

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------

/// Result of walking the deep-storage tree.
struct ScanResult {
    /// Segment artifacts to assess, in deterministic (path) order.
    found: Vec<FoundArtifact>,
    /// The scan stopped at `--max-segments` with at least one further
    /// artifact left unassessed.
    truncated: bool,
    /// Non-fatal scan anomalies (skipped symlinks, unreadable
    /// subdirectories) — the report surfaces these so an incomplete
    /// scan is never silently presented as complete.
    warnings: Vec<String>,
    /// The walk hit a hard resource cap ([`MAX_SCAN_DIRS`] /
    /// [`MAX_DIR_ENTRIES`]) and stopped early: results are incomplete.
    /// Carried as a dedicated flag (not just a warning line) so JSON
    /// consumers cannot miss it even when the warning list overflows
    /// (Codex R3 finding).
    aborted: bool,
}

/// Recursively walk `root` collecting segment artifacts, stopping once
/// `max_segments` artifacts have been collected.
///
/// Hostile-tree hardening (Codex R1/R2 findings):
/// * **Symlinks are never followed** — neither directory nor file
///   symlinks — so the scan cannot escape `root`, cannot loop on
///   symlink cycles, and cannot double-count segments.  Each skip is
///   recorded as a warning.
/// * Only **regular files** are considered artifact candidates: a FIFO
///   or device node named `index.zip` is skipped with a warning
///   instead of being handed to a potentially blocking `open` (R2).
/// * A subdirectory that cannot be read (permissions, races) degrades
///   to a warning instead of aborting the scan; only an unreadable
///   `root` itself is fatal.
/// * At most [`MAX_SCAN_DIRS`] directories are visited; a wider tree
///   aborts the walk with a loud warning instead of consuming
///   unbounded time/memory (R2).
fn scan(
    root: &Path,
    max_segments: Option<usize>,
) -> Result<ScanResult, Box<dyn std::error::Error>> {
    let mut found = Vec::new();
    let mut truncated = false;
    let mut aborted = false;
    let mut warnings = Warnings::new();
    let mut dirs_visited: usize = 0;
    // Depth-first, deterministic (sorted) order.
    let mut stack: Vec<(PathBuf, usize)> = vec![(root.to_path_buf(), 0)];

    'walk: while let Some((dir, depth)) = stack.pop() {
        dirs_visited += 1;
        if dirs_visited > MAX_SCAN_DIRS {
            warnings.push(format!(
                "scan aborted after visiting {MAX_SCAN_DIRS} directories — \
                 the tree is wider than any realistic deep-storage layout; \
                 results below are incomplete"
            ));
            aborted = true;
            break 'walk;
        }
        let read = match std::fs::read_dir(&dir) {
            Ok(read) => read,
            Err(e) => {
                let msg = format!("failed to read directory {}: {e}", dir.display());
                if dir == root {
                    return Err(msg.into());
                }
                warnings.push(msg);
                continue;
            }
        };
        // (path, is_dir) — symlinks and special files filtered out up
        // front so nothing non-regular is ever opened.
        let mut entries: Vec<(PathBuf, bool)> = Vec::new();
        let mut has_meta_smoosh = false;
        // Counts EVERY iterated entry, including skipped symlinks and
        // special files, so a hostile directory of millions of
        // symlinks cannot bypass the cap (Codex R4 finding).
        let mut seen: usize = 0;
        for entry in read {
            seen += 1;
            if seen > MAX_DIR_ENTRIES {
                warnings.push(format!(
                    "directory {} has more than {MAX_DIR_ENTRIES} entries — \
                     the rest were skipped; results are incomplete",
                    dir.display()
                ));
                aborted = true;
                break;
            }
            let Ok(entry) = entry else {
                warnings.push(format!("failed to read an entry of {}", dir.display()));
                continue;
            };
            let Ok(file_type) = entry.file_type() else {
                warnings.push(format!(
                    "failed to stat {} — skipped",
                    entry.path().display()
                ));
                continue;
            };
            if file_type.is_symlink() {
                warnings.push(format!(
                    "skipped symlink {} (symlinks are never followed)",
                    entry.path().display()
                ));
                continue;
            }
            if !file_type.is_dir() && !file_type.is_file() {
                warnings.push(format!(
                    "skipped {} (not a regular file or directory)",
                    entry.path().display()
                ));
                continue;
            }
            if file_type.is_file() && entry.file_name() == "meta.smoosh" {
                has_meta_smoosh = true;
            }
            entries.push((entry.path(), file_type.is_dir()));
        }
        entries.sort();

        // A directory holding `meta.smoosh` IS a segment — but it must
        // not shadow anything else (Codex R3 finding): a colocated
        // `index.zip` and nested directories are still processed below.
        if depth > 0 && has_meta_smoosh {
            if at_capacity(&found, max_segments) {
                truncated = true;
                break 'walk;
            }
            found.push(FoundArtifact {
                path: dir.clone(),
                kind: ArtifactKind::SmooshDir,
            });
        }

        for (path, is_dir) in entries {
            if is_dir {
                if depth >= MAX_SCAN_DEPTH {
                    // A skipped subtree means the report is incomplete
                    // — flag it, don't just warn (Codex R4 finding).
                    warnings.push(format!(
                        "not descending into {} (deeper than {MAX_SCAN_DEPTH} levels); \
                         results are incomplete",
                        path.display()
                    ));
                    aborted = true;
                } else if stack.len() >= MAX_SCAN_DIRS {
                    // Bound the pending-directory queue itself so a
                    // nested-wide hostile tree cannot accumulate
                    // unbounded paths before the visit cap triggers
                    // (Codex R4 finding).
                    warnings.push(format!(
                        "scan queue exceeded {MAX_SCAN_DIRS} pending directories — \
                         {} was not scanned; results are incomplete",
                        path.display()
                    ));
                    aborted = true;
                } else {
                    stack.push((path, depth + 1));
                }
            } else if path.file_name().is_some_and(|n| n == "index.zip") {
                if at_capacity(&found, max_segments) {
                    truncated = true;
                    break 'walk;
                }
                found.push(FoundArtifact {
                    path,
                    kind: ArtifactKind::IndexZip,
                });
            }
        }
    }

    // Deterministic report order regardless of stack traversal order.
    found.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(ScanResult {
        found,
        truncated,
        warnings: warnings.into_vec(),
        aborted,
    })
}

fn at_capacity(found: &[FoundArtifact], max_segments: Option<usize>) -> bool {
    max_segments.is_some_and(|m| found.len() >= m)
}

/// Warning collector bounded at [`MAX_WARNINGS`] verbatim entries;
/// overflow is folded into a single trailing "+N more" entry.
struct Warnings {
    kept: Vec<String>,
    dropped: usize,
}

impl Warnings {
    fn new() -> Self {
        Self {
            kept: Vec::new(),
            dropped: 0,
        }
    }

    fn push(&mut self, warning: String) {
        if self.kept.len() < MAX_WARNINGS {
            self.kept.push(warning);
        } else {
            self.dropped += 1;
        }
    }

    fn into_vec(mut self) -> Vec<String> {
        if self.dropped > 0 {
            self.kept
                .push(format!("... and {} more warnings (elided)", self.dropped));
        }
        self.kept
    }
}

// ---------------------------------------------------------------------------
// Identity derivation
// ---------------------------------------------------------------------------

/// Establish segment identity: prefer a `descriptor.json` sibling,
/// fall back to the documented `<dataSource>/<interval>/<version>/
/// <partitionNum>` path structure, else `unknown`.
fn derive_identity(root: &Path, artifact: &FoundArtifact) -> SegmentIdentity {
    // The directory the segment logically lives in (partition dir for
    // zips; the smoosh dir itself for raw dirs).
    let segment_dir = match artifact.kind {
        ArtifactKind::IndexZip => artifact.path.parent().map(Path::to_path_buf),
        ArtifactKind::SmooshDir => Some(artifact.path.clone()),
    };

    // 1. descriptor.json.  For the `<partition>/index/` raw layout the
    //    authoritative descriptor is the PARTITION-level one (the file
    //    Druid's pusher writes next to the artifact), so it is probed
    //    FIRST; a JSON file planted inside the segment dir must not
    //    override it (Codex R4 finding).
    let mut candidates = Vec::new();
    if let Some(dir) = &segment_dir {
        if artifact.kind == ArtifactKind::SmooshDir
            && dir.file_name().is_some_and(|n| n == "index")
            && let Some(parent) = dir.parent()
        {
            candidates.push(parent.join("descriptor.json"));
        }
        candidates.push(dir.join("descriptor.json"));
    }
    let from_descriptor = candidates
        .iter()
        .find_map(|candidate| identity_from_descriptor(candidate));

    // 2. Path structure.
    let from_path = segment_dir
        .as_deref()
        .and_then(|dir| identity_from_path(root, dir));

    // Merge: descriptor fields win; a partial descriptor is filled
    // from the path and the identity source says so (Codex R1 finding
    // — a descriptor carrying only `dataSource` must not null out an
    // otherwise derivable interval/version/partition).
    match (from_descriptor, from_path) {
        (Some(mut desc), Some(path_id)) => {
            let mut filled = false;
            for (dst, src) in [
                (&mut desc.data_source, path_id.data_source),
                (&mut desc.interval, path_id.interval),
                (&mut desc.version, path_id.version),
                (&mut desc.partition, path_id.partition),
            ] {
                if dst.is_none() && src.is_some() {
                    *dst = src;
                    filled = true;
                }
            }
            if filled {
                desc.source = Some(IdentitySource::DescriptorJsonPlusPath);
            }
            desc
        }
        (Some(desc), None) => desc,
        (None, Some(path_id)) => path_id,
        (None, None) => SegmentIdentity {
            source: Some(IdentitySource::Unknown),
            ..SegmentIdentity::default()
        },
    }
}

/// Parse `descriptor.json` (Druid segment metadata: `dataSource`,
/// `interval`, `version`, `shardSpec.partitionNum`).  Returns `None`
/// when the file is absent, is not a regular file (symlink/FIFO), is
/// larger than [`MAX_DESCRIPTOR_BYTES`], or is not parseable as a JSON
/// object with a `dataSource` field — in every such case the path
/// fallback applies (Codex R1 finding: unbounded read of an untrusted
/// "descriptor").
fn identity_from_descriptor(path: &Path) -> Option<SegmentIdentity> {
    // Static gate: never open symlinks or special files.
    let meta = std::fs::symlink_metadata(path).ok()?;
    if !meta.is_file() || meta.len() > MAX_DESCRIPTOR_BYTES {
        return None;
    }
    // Handle-based gate (Codex R2 finding: the pre-open stat alone is
    // TOCTOU-prone): re-check the type/size on the opened handle and
    // read through a hard `take` cap so a concurrently grown or
    // swapped file can never be slurped unbounded.
    let file = std::fs::File::open(path).ok()?;
    let fmeta = file.metadata().ok()?;
    if !fmeta.is_file() || fmeta.len() > MAX_DESCRIPTOR_BYTES {
        return None;
    }
    let mut bytes = Vec::new();
    file.take(MAX_DESCRIPTOR_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > MAX_DESCRIPTOR_BYTES {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let data_source = bound_field(value.get("dataSource")?.as_str()?);
    let interval = value
        .get("interval")
        .and_then(|v| v.as_str())
        .map(bound_field);
    let version = value
        .get("version")
        .and_then(|v| v.as_str())
        .map(bound_field);
    let partition = value
        .get("shardSpec")
        .and_then(|s| s.get("partitionNum"))
        .and_then(serde_json::Value::as_i64)
        .map(|p| p.to_string());
    Some(SegmentIdentity {
        data_source: Some(data_source),
        interval,
        version,
        partition,
        source: Some(IdentitySource::DescriptorJson),
    })
}

/// Bound a descriptor-supplied identity field to
/// [`MAX_IDENTITY_FIELD_CHARS`] so a hostile descriptor cannot bloat
/// the retained report by megabytes per segment (Codex R4 finding).
fn bound_field(s: &str) -> String {
    if s.chars().count() <= MAX_IDENTITY_FIELD_CHARS {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(MAX_IDENTITY_FIELD_CHARS).collect();
        out.push('…');
        out
    }
}

/// Infer identity from `<root>/<dataSource>/<interval>/<version>/
/// <partitionNum>[/index]`.  A trailing `index` component (Druid 31
/// local layout) is stripped before matching.  Only an exact 4-level
/// match is trusted; anything else returns `None`.
fn identity_from_path(root: &Path, segment_dir: &Path) -> Option<SegmentIdentity> {
    let rel = segment_dir.strip_prefix(root).ok()?;
    let mut parts: Vec<String> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    if parts.last().is_some_and(|p| p == "index") {
        parts.pop();
    }
    if parts.len() != 4 {
        return None;
    }
    let mut it = parts.into_iter();
    Some(SegmentIdentity {
        data_source: it.next(),
        interval: it.next(),
        version: it.next(),
        partition: it.next(),
        source: Some(IdentitySource::Path),
    })
}

// ---------------------------------------------------------------------------
// Per-segment assessment
// ---------------------------------------------------------------------------

/// Assess one artifact: extract if zipped, then ask the real v9 reader
/// to open it.  Never propagates a failure — every failure becomes an
/// `Outcome::Unreadable` with the fail-loud reason preserved.
fn assess_artifact(artifact: &FoundArtifact) -> Outcome {
    // Hold the temp dir so extracted files live until the read is done.
    let _extract_guard: Option<tempfile::TempDir>;
    let smoosh_dir: PathBuf = match artifact.kind {
        ArtifactKind::SmooshDir => {
            // Symlinks must be excluded end-to-end (Codex R2 finding):
            // the walk skips symlinked dirs, but a recognized segment
            // dir could still contain symlinked chunks/sidecars that
            // the reader would follow (e.g. to a FIFO or /dev/zero).
            if let Err(reason) = reject_non_regular_children(&artifact.path) {
                return Outcome::Unreadable { reason };
            }
            _extract_guard = None;
            artifact.path.clone()
        }
        ArtifactKind::IndexZip => match extract_index_zip(&artifact.path) {
            Ok(tmp) => {
                let dir = tmp.path().to_path_buf();
                _extract_guard = Some(tmp);
                dir
            }
            Err(reason) => return Outcome::Unreadable { reason },
        },
    };

    // The reader returns errors rather than panicking; catch_unwind is
    // defence-in-depth so one hostile segment cannot abort the scan.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        SegmentData::open(&smoosh_dir)
    }));
    match result {
        Ok(Ok(segment)) => {
            let mut columns =
                Vec::with_capacity(1 + segment.dimensions.len() + segment.metrics.len());
            if segment.columns.contains_key("__time") {
                columns.push("__time".to_string());
            }
            columns.extend(segment.dimensions.iter().cloned());
            columns.extend(segment.metrics.iter().cloned());
            Outcome::Readable {
                rows: segment.num_rows,
                columns,
            }
        }
        Ok(Err(e)) => Outcome::Unreadable {
            reason: e.to_string(),
        },
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            Outcome::Unreadable {
                reason: format!("reader panicked (defensive catch): {msg}"),
            }
        }
    }
}

/// Refuse to hand a smoosh dir to the reader when any direct child is
/// a symlink or special file — the reader loads every regular-looking
/// file in the dir (chunks + sidecars) and must never be pointed at a
/// FIFO or an unbounded device via symlink (Codex R2 finding).
/// Plain subdirectories are tolerated (the reader ignores them).
fn reject_non_regular_children(dir: &Path) -> Result<(), String> {
    let read = std::fs::read_dir(dir)
        .map_err(|e| format!("failed to re-read segment dir before opening: {e}"))?;
    for (seen, entry) in read.enumerate() {
        // Same per-directory bound as the scan (Codex R4 finding: this
        // re-enumeration must not be a way around the cap).
        if seen >= MAX_DIR_ENTRIES {
            return Err(format!(
                "segment dir has more than {MAX_DIR_ENTRIES} entries — refusing to open"
            ));
        }
        let entry =
            entry.map_err(|e| format!("failed to enumerate segment dir before opening: {e}"))?;
        let file_type = entry
            .file_type()
            .map_err(|e| format!("failed to stat a segment dir entry before opening: {e}"))?;
        if !file_type.is_file() && !file_type.is_dir() {
            let kind = if file_type.is_symlink() {
                "a symlink"
            } else {
                "not a regular file"
            };
            return Err(format!(
                "segment dir entry `{}` is {kind} — refusing to open (symlinks and \
                 special files are never followed)",
                entry.file_name().to_string_lossy()
            ));
        }
    }
    Ok(())
}

/// Extract `index.zip` into a fresh temp dir with zip-slip and
/// zip-bomb defences.  Returns the temp dir (kept alive by the caller
/// while reading) or a human-readable failure reason.
///
/// Failure reasons deliberately do NOT embed the artifact path (the
/// report already prints it per segment): keeping reasons path-free
/// lets the by-reason summary aggregate identical failures across
/// many segments instead of one bucket per path.
fn extract_index_zip(zip_path: &Path) -> Result<tempfile::TempDir, String> {
    let file =
        std::fs::File::open(zip_path).map_err(|e| format!("failed to open index.zip: {e}"))?;
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|e| format!("failed to open index.zip as a zip archive: {e}"))?;
    // Inode-exhaustion defence: a real segment archive holds a handful
    // of entries (Codex R1 finding).
    if archive.len() > MAX_ZIP_ENTRIES {
        return Err(format!(
            "zip archive declares {} entries, exceeding the {MAX_ZIP_ENTRIES}-entry cap",
            archive.len()
        ));
    }
    let tmp =
        tempfile::tempdir().map_err(|e| format!("failed to create temp extraction dir: {e}"))?;

    let mut total: u64 = 0;
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| format!("failed to read zip entry #{i}: {e}"))?;
        // Zip-slip defence: reject entries whose name escapes the
        // extraction dir.
        let Some(rel) = entry.enclosed_name() else {
            return Err(format!(
                "zip entry `{}` has an unsafe path (would escape the extraction dir)",
                entry.name()
            ));
        };
        // Depth bound (Codex R5 finding): real segment archives are
        // flat; refuse pathological nesting instead of extracting it.
        if rel.components().count() > MAX_ZIP_ENTRY_DEPTH {
            return Err(format!(
                "zip entry `{}` is nested more than {MAX_ZIP_ENTRY_DEPTH} directories deep",
                entry.name()
            ));
        }
        let entry_name = rel.display().to_string();
        let out_path = tmp.path().join(&rel);
        if entry.is_dir() {
            std::fs::create_dir_all(&out_path)
                .map_err(|e| format!("failed to create extracted dir `{entry_name}`: {e}"))?;
            continue;
        }
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create extraction dir for `{entry_name}`: {e}"))?;
        }
        let mut out = std::fs::File::create(&out_path)
            .map_err(|e| format!("failed to create extracted file `{entry_name}`: {e}"))?;
        // Zip-bomb defence: bound total uncompressed bytes.
        let budget = MAX_UNCOMPRESSED_BYTES.saturating_sub(total);
        let copied = std::io::copy(&mut (&mut entry).take(budget.saturating_add(1)), &mut out)
            .map_err(|e| format!("failed to extract zip entry `{entry_name}`: {e}"))?;
        total = total.saturating_add(copied);
        if copied > budget {
            return Err(format!(
                "zip archive exceeds the {MAX_UNCOMPRESSED_BYTES}-byte uncompressed cap \
                 (zip-bomb defence)"
            ));
        }
    }
    Ok(tmp)
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

/// Note printed with both output formats — the approved claim wording.
const VERDICT_NOTE: &str = "readable = openable by FerroDruid's v9 reader — attachable when \
     the attach feature ships. Dry-run assessment only: nothing is migrated.";

/// Group unreadable reasons for the summary (`reason -> count`).
fn reason_histogram(assessments: &[Assessment]) -> BTreeMap<&str, usize> {
    let mut map: BTreeMap<&str, usize> = BTreeMap::new();
    for a in assessments {
        if let Outcome::Unreadable { reason } = &a.outcome {
            *map.entry(reason.as_str()).or_insert(0) += 1;
        }
    }
    map
}

/// Short display identity: `ds / interval / version / partition`, or
/// the artifact path when identity is unknown.
fn display_identity(root: &Path, a: &Assessment) -> String {
    let id = &a.identity;
    if id.source() == IdentitySource::Unknown {
        return a
            .path
            .strip_prefix(root)
            .unwrap_or(&a.path)
            .display()
            .to_string();
    }
    let field = |f: &Option<String>| f.clone().unwrap_or_else(|| "?".to_string());
    format!(
        "{} / {} / {} / {}",
        field(&id.data_source),
        field(&id.interval),
        field(&id.version),
        field(&id.partition),
    )
}

/// Replace terminal control characters in untrusted strings (zip entry
/// names, descriptor fields, column names, reader reasons) so hostile
/// input cannot inject ANSI escapes or forge report lines in the
/// human-readable output (Codex R1 finding).  JSON output is already
/// safe via serde escaping and is left verbatim.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { '\u{FFFD}' } else { c })
        .collect()
}

fn print_human(report: &ReportInput<'_>) {
    let assessments = report.assessments;
    let readable = assessments
        .iter()
        .filter(|a| matches!(a.outcome, Outcome::Readable { .. }))
        .count();
    let unreadable = assessments.len() - readable;

    println!("FerroDruid deep-storage assessment (dry run)");
    println!(
        "deep-storage root: {}",
        sanitize(&report.root.display().to_string())
    );
    println!("note: {VERDICT_NOTE}");
    println!();

    if assessments.is_empty() {
        if report.truncated {
            // --max-segments 0: do not claim the store is empty.
            print_truncation_note(report.max_segments);
        } else {
            println!(
                "No segment artifacts found (looked for `index.zip` files and \
                 directories containing `meta.smoosh`)."
            );
        }
        print_abort_note(report.aborted);
        print_human_warnings(report.warnings);
        return;
    }

    println!(
        "{:<11} {:>9} {:>5}  {:<60} IDENTITY",
        "RESULT", "ROWS", "COLS", "SEGMENT (dataSource / interval / version / partition)"
    );
    for a in assessments {
        let identity = sanitize(&display_identity(report.root, a));
        match &a.outcome {
            Outcome::Readable { rows, columns } => {
                println!(
                    "{:<11} {:>9} {:>5}  {:<60} [{}]",
                    "READABLE",
                    rows,
                    columns.len(),
                    identity,
                    a.identity.source().as_str(),
                );
                println!("            columns: {}", sanitize(&columns.join(", ")));
            }
            Outcome::Unreadable { reason } => {
                println!(
                    "{:<11} {:>9} {:>5}  {:<60} [{}]",
                    "UNREADABLE",
                    "-",
                    "-",
                    identity,
                    a.identity.source().as_str(),
                );
                println!(
                    "            artifact: {}",
                    sanitize(&a.path.display().to_string())
                );
                println!("            reason: {}", sanitize(reason));
            }
        }
    }

    println!();
    println!(
        "Summary: {} segment artifact(s) scanned — {} readable by FerroDruid's \
         v9 reader (attachable when the attach feature ships), {} unreadable.",
        assessments.len(),
        readable,
        unreadable,
    );
    let reasons = reason_histogram(assessments);
    if !reasons.is_empty() {
        println!("Unreadable, by reason:");
        for (reason, count) in reasons {
            println!("  {count}x {}", sanitize(reason));
        }
    }
    if report.truncated {
        print_truncation_note(report.max_segments);
    }
    print_abort_note(report.aborted);
    print_human_warnings(report.warnings);
}

fn print_abort_note(aborted: bool) {
    if aborted {
        println!(
            "WARNING: the scan hit a hard resource cap and stopped early — \
             this report is INCOMPLETE (see scan warnings)."
        );
    }
}

fn print_truncation_note(max_segments: Option<usize>) {
    let cap = max_segments.map_or_else(String::new, |m| m.to_string());
    println!(
        "NOTE: scan stopped at --max-segments {cap}; more artifacts exist \
         and were not assessed."
    );
}

fn print_human_warnings(warnings: &[String]) {
    if warnings.is_empty() {
        return;
    }
    println!("Scan warnings ({}):", warnings.len());
    for w in warnings {
        println!("  - {}", sanitize(w));
    }
}

fn print_json(report: &ReportInput<'_>) {
    let assessments = report.assessments;
    let readable = assessments
        .iter()
        .filter(|a| matches!(a.outcome, Outcome::Readable { .. }))
        .count();
    let segments: Vec<serde_json::Value> = assessments
        .iter()
        .map(|a| {
            let (is_readable, rows, columns, error) = match &a.outcome {
                Outcome::Readable { rows, columns } => (
                    true,
                    serde_json::json!(rows),
                    serde_json::json!(columns),
                    serde_json::Value::Null,
                ),
                Outcome::Unreadable { reason } => (
                    false,
                    serde_json::Value::Null,
                    serde_json::Value::Null,
                    serde_json::json!(reason),
                ),
            };
            serde_json::json!({
                "path": a.path.display().to_string(),
                "artifact": a.kind.as_str(),
                "data_source": a.identity.data_source,
                "interval": a.identity.interval,
                "version": a.identity.version,
                "partition": a.identity.partition,
                "identity_source": a.identity.source().as_str(),
                "readable": is_readable,
                "rows": rows,
                "columns": columns,
                "num_columns": match &a.outcome {
                    Outcome::Readable { columns, .. } => serde_json::json!(columns.len()),
                    Outcome::Unreadable { .. } => serde_json::Value::Null,
                },
                "error": error,
            })
        })
        .collect();

    let reasons: BTreeMap<String, usize> = reason_histogram(assessments)
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

    let out = serde_json::json!({
        "deep_storage": report.root.display().to_string(),
        "total": assessments.len(),
        "readable": readable,
        "unreadable": assessments.len() - readable,
        "truncated": report.truncated,
        "scan_aborted": report.aborted,
        "max_segments": report.max_segments,
        "unreadable_reasons": reasons,
        "warnings": report.warnings,
        "segments": segments,
        "note": VERDICT_NOTE,
    });
    println!("{out:#}");
}
