// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `ferrodruid-migrate assess` — dry-run readability assessment of a
//! Druid **local deep-storage** directory.
//!
//! Scans a deep-storage root for segment artifacts and reports, per
//! segment, whether it is **readable by FerroDruid's v9 reader —
//! attachable with `ferrodruid-migrate attach`**.  This is a judgment
//! tool only: nothing is migrated, copied, or modified (segments are
//! extracted into throwaway temp dirs for reading and deleted
//! afterwards).  The scan / identity / extraction building blocks here
//! are shared with the `attach` subcommand (`crate::attach`), so what
//! `assess` judges readable is exactly what `attach` imports.
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
//! * Symlink containment (compat-2 R2 H2 + R4 H2): the scan never
//!   follows symlinks, source opens are `O_NOFOLLOW`, and every opened
//!   source path is re-checked to RESOLVE under the (canonicalized)
//!   deep-storage root, so an ancestor-directory symlink swapped in
//!   after the scan cannot smuggle EXTERNAL bytes in under an in-tree
//!   identity.  Residual, honestly: a WITHIN-TREE symlink swap (one
//!   in-root segment posing as another) and the precise race between
//!   the containment check and the read of the already-open handle are
//!   not closed — that needs `openat2(RESOLVE_NO_SYMLINKS)` / cap-std
//!   (follow-up), out of reach for std file APIs under
//!   `#![forbid(unsafe_code)]`.

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

/// Open a SOURCE file for reading, refusing to follow a final-component
/// symlink (`O_NOFOLLOW` — Codex compat-2 R2 H2).
///
/// The pre-open lstat gates catch a symlink that is PRESENT when the
/// tree is scanned, and the handle-based `is_file()` re-checks catch a
/// swap to a directory/FIFO — but neither catches the TOCTOU where the
/// path is swapped, after the scan, for a symlink to an external
/// REGULAR file: the plain open follows the link and the handle still
/// looks like a perfectly normal file, so external bytes would be
/// adopted as descriptor identity / staged / uploaded.  With
/// `O_NOFOLLOW` such a swap fails the open itself (`ELOOP`), which
/// every caller surfaces through its existing loud-skip error path.
/// Every open of an UNTRUSTED source file (descriptor.json, index.zip,
/// raw smoosh children) goes through this helper.
#[cfg(unix)]
fn open_source_file_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt as _;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

/// Non-Unix fallback: `O_NOFOLLOW` is unavailable, so the pre-open
/// lstat gates and the handle-based `is_file()` re-checks remain the
/// only (weaker, race-prone) defence.  FerroDruid's deploy target is
/// Unix, where the `#[cfg(unix)]` variant above is authoritative.
#[cfg(not(unix))]
fn open_source_file_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new().read(true).open(path)
}

/// Canonical source-containment re-check (Codex compat-2 R4 H2): after
/// an `O_NOFOLLOW` open of a SOURCE path succeeded, verify the path
/// still RESOLVES under the resolved deep-storage source root.
///
/// `O_NOFOLLOW` protects only the FINAL path component: if an ANCESTOR
/// directory (e.g. the partition dir) is swapped for a symlink to an
/// EXTERNAL dir after the scan, the open follows the symlinked ancestor
/// and hands back a perfectly regular external file — which would then
/// be adopted as descriptor identity / staged / hashed / uploaded and
/// COMMITTED under the original in-tree identity.  Canonicalizing both
/// sides catches exactly that escape, and keeps two legitimate layouts
/// working: a symlink-free source (canonical == literal) and a source
/// ROOT that is itself a symlink (a mount — both sides resolve to the
/// same real prefix).  Same pattern as the deep-storage layer's
/// `verify_resolves_under_base`.
///
/// Honest limitation: this closes the EXTERNAL escape (the dangerous
/// case — foreign bytes under a trusted identity), not every race.  A
/// WITHIN-TREE symlink swap (one in-root segment posing as another) and
/// the precise window between this check and the read of the
/// already-open handle are not detectable with std file APIs under
/// `#![forbid(unsafe_code)]`; `openat2(RESOLVE_NO_SYMLINKS)` / cap-std
/// would close them and are follow-up work.
fn verify_source_resolves_under_root(root: &Path, path: &Path) -> Result<(), String> {
    let canonical_root = std::fs::canonicalize(root).map_err(|e| {
        format!("cannot canonicalize the deep-storage source root for the containment check: {e}")
    })?;
    let canonical = std::fs::canonicalize(path)
        .map_err(|e| format!("cannot canonicalize a source path for the containment check: {e}"))?;
    if canonical.starts_with(&canonical_root) {
        Ok(())
    } else {
        Err(format!(
            "source path {} resolves to {} — OUTSIDE the deep-storage root (ancestor \
             symlink escape rejected; symlinked segment paths are never followed)",
            path.display(),
            canonical.display(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Report data model
// ---------------------------------------------------------------------------

/// What kind of on-disk artifact a segment was found as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArtifactKind {
    /// A zipped smoosh archive (`index.zip`).
    IndexZip,
    /// An already-unzipped directory of smoosh files.
    SmooshDir,
}

impl ArtifactKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ArtifactKind::IndexZip => "index.zip",
            ArtifactKind::SmooshDir => "smoosh-dir",
        }
    }
}

/// Where the segment identity (dataSource/interval/version/partition)
/// came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdentitySource {
    /// Parsed from a `descriptor.json` next to the artifact.
    DescriptorJson,
    /// A partial `descriptor.json` supplied some fields; the rest were
    /// filled from the path structure.
    DescriptorJsonPlusPath,
    /// Inferred from the `<dataSource>/<interval>/<version>/<partition>`
    /// path structure.
    Path,
    /// Synthesized from a foreign Druid metadata-DB row's payload
    /// (`ferrodruid-migrate import-druid-metadata`, compat-7).
    MetadataDb,
    /// Neither source available — identity unknown, artifact still
    /// assessed.
    Unknown,
}

impl IdentitySource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            IdentitySource::DescriptorJson => "descriptor.json",
            IdentitySource::DescriptorJsonPlusPath => "descriptor.json+path",
            IdentitySource::Path => "path",
            IdentitySource::MetadataDb => "druid-metadata-db",
            IdentitySource::Unknown => "unknown",
        }
    }
}

/// Segment identity as far as it could be established.
#[derive(Debug, Default)]
pub(crate) struct SegmentIdentity {
    pub(crate) data_source: Option<String>,
    pub(crate) interval: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) partition: Option<String>,
    source: Option<IdentitySource>,
    /// A `descriptor.json` EXISTS next to the artifact but could not be
    /// used (unreadable / oversized / malformed / not a regular file) —
    /// H4: a broken AUTHORITATIVE identity source is loud, never a
    /// silent fallback to the path identity, in both `assess` and
    /// `attach`. When set, no identity fields are populated.
    pub(crate) descriptor_error: Option<String>,
}

impl SegmentIdentity {
    pub(crate) fn source(&self) -> IdentitySource {
        self.source.unwrap_or(IdentitySource::Unknown)
    }

    /// Build a COMPLETE identity from a foreign Druid metadata-DB row's
    /// PAYLOAD (compat-7): `dataSource` / `interval` / `version`
    /// straight from the payload, the partition from
    /// `shardSpec.partitionNum` (`"0"` when the shardSpec is absent).
    ///
    /// The fields are NOT validated here — `attach_one` applies the same
    /// H5 validation (interval parse + ordering, partition
    /// canonicalization, path-component safety) to this identity source
    /// as to every other, so a hostile source row fails loudly there.
    pub(crate) fn from_metadata_db(
        data_source: String,
        interval: String,
        version: String,
        partition: String,
    ) -> Self {
        Self {
            data_source: Some(data_source),
            interval: Some(interval),
            version: Some(version),
            partition: Some(partition),
            source: Some(IdentitySource::MetadataDb),
            descriptor_error: None,
        }
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
pub(crate) struct FoundArtifact {
    pub(crate) path: PathBuf,
    pub(crate) kind: ArtifactKind,
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
            // H4: a PRESENT-but-unusable descriptor is loud — the segment
            // must not be presented as attachable under a silently adopted
            // path identity (`attach` refuses it with the same reason).
            let outcome = if let Some(err) = identity.descriptor_error.as_deref() {
                Outcome::Unreadable {
                    reason: format!(
                        "identity failure — {err}; refusing the silent path-identity \
                         fallback (attach skips this segment for the same reason)"
                    ),
                }
            } else {
                assess_artifact(deep_storage, &f)
            };
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
pub(crate) struct ScanResult {
    /// Segment artifacts to assess, in deterministic (path) order.
    pub(crate) found: Vec<FoundArtifact>,
    /// The scan stopped at `--max-segments` with at least one further
    /// artifact left unassessed.
    pub(crate) truncated: bool,
    /// Non-fatal scan anomalies (skipped symlinks, unreadable
    /// subdirectories) — the report surfaces these so an incomplete
    /// scan is never silently presented as complete.
    pub(crate) warnings: Vec<String>,
    /// The walk hit a hard resource cap ([`MAX_SCAN_DIRS`] /
    /// [`MAX_DIR_ENTRIES`]) and stopped early: results are incomplete.
    /// Carried as a dedicated flag (not just a warning line) so JSON
    /// consumers cannot miss it even when the warning list overflows
    /// (Codex R3 finding).
    pub(crate) aborted: bool,
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
pub(crate) fn scan(
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
pub(crate) fn derive_identity(root: &Path, artifact: &FoundArtifact) -> SegmentIdentity {
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
    // The FIRST candidate that EXISTS decides: a descriptor that is
    // present but unusable is a loud identity failure (H4), never
    // silently skipped in favour of the next candidate or the path.
    let mut from_descriptor: Option<SegmentIdentity> = None;
    for candidate in &candidates {
        match identity_from_descriptor(root, candidate) {
            Ok(Some(id)) => {
                from_descriptor = Some(id);
                break;
            }
            Ok(None) => {}
            Err(reason) => {
                return SegmentIdentity {
                    descriptor_error: Some(reason),
                    source: Some(IdentitySource::Unknown),
                    ..SegmentIdentity::default()
                };
            }
        }
    }

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

/// Loud reason for a descriptor that was OBSERVED to exist at the lstat
/// gate but could not then be opened (Codex R9).
///
/// `NotFound` is deliberately NOT special-cased back to `Ok(None)`: a
/// descriptor present at the stat that vanishes before the open is a
/// concurrent unlink/rename, not the genuinely-absent case, and silently
/// degrading it to the path-identity fallback (H4) would commit the
/// segment under a conflicting path-derived identity.
fn present_descriptor_open_failure(err: &std::io::Error) -> String {
    format!(
        "descriptor.json existed at the stat but could not be opened \
         (concurrent unlink/rename, or an I/O error) — refusing the silent \
         path-identity fallback for a descriptor that was present: {err}"
    )
}

/// Parse `descriptor.json` (Druid segment metadata: `dataSource`,
/// `interval`, `version`, `shardSpec.partitionNum`).
///
/// * `Ok(None)` — the file is ABSENT: the path fallback applies (the
///   normal descriptor-less layout).
/// * `Err(reason)` — the file EXISTS but cannot be used: not a regular
///   file (symlink/FIFO), larger than [`MAX_DESCRIPTOR_BYTES`] (Codex R1
///   finding: unbounded read of an untrusted "descriptor"), unreadable,
///   resolving OUTSIDE the deep-storage root (R4 H2 ancestor-symlink
///   containment), not valid JSON, lacking a string `dataSource`, or
///   carrying a PRESENT-but-malformed identity field (R4 H1: wrong JSON
///   type, or a partitionNum that is not a non-negative i64-range
///   integer).  H4: a broken AUTHORITATIVE identity source must surface
///   loudly — silently falling back to the path identity could attach a
///   segment under a wrong identity.  The absent/invalid line: a key
///   that is genuinely ABSENT falls back to the path (legitimate
///   partial descriptor); a key that is PRESENT but unusable is loud.
fn identity_from_descriptor(root: &Path, path: &Path) -> Result<Option<SegmentIdentity>, String> {
    // Static gate: never open symlinks or special files.
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("descriptor.json exists but cannot be stat'd: {e}")),
    };
    if !meta.is_file() {
        return Err(
            "descriptor.json exists but is not a regular file (symlink/special file) — \
             refusing to read it"
                .to_string(),
        );
    }
    if meta.len() > MAX_DESCRIPTOR_BYTES {
        return Err(format!(
            "descriptor.json exists but is {} bytes, over the {MAX_DESCRIPTOR_BYTES}-byte cap",
            meta.len()
        ));
    }
    // Handle-based gate (Codex R2 finding: the pre-open stat alone is
    // TOCTOU-prone): re-check the type/size on the opened handle and
    // read through a hard `take` cap so a concurrently grown or
    // swapped file can never be slurped unbounded.  The open itself is
    // `O_NOFOLLOW` (compat-2 R2 H2): a swap to a symlink pointing at an
    // external REGULAR file would pass the handle `is_file()` re-check,
    // so it must fail the open instead.
    let file = match open_source_file_nofollow(path) {
        Ok(file) => file,
        // The descriptor was OBSERVED to exist (the lstat gate above
        // passed is_file + size), so ANY failure to open it now — a
        // concurrent unlink/rename between the stat and the open (Codex
        // R9), or any other I/O error — is a LOUD identity failure, never
        // a silent `Ok(None)`.  `Ok(None)` is reserved for the genuinely
        // ABSENT case (the lstat NotFound at the top of this function);
        // degrading a present-but-vanished descriptor to it would let the
        // path-identity fallback attach the segment under a CONFLICTING
        // datasource/interval/version/partition — a wrong metadata row.
        Err(e) => return Err(present_descriptor_open_failure(&e)),
    };
    // Ancestor containment (compat-2 R4 H2): `O_NOFOLLOW` protects only
    // the final component — a partition dir swapped for a symlink would
    // have this open hand back an EXTERNAL descriptor whose identity
    // fields we would then trust.
    verify_source_resolves_under_root(root, path)
        .map_err(|e| format!("descriptor.json failed the source-containment check: {e}"))?;
    let fmeta = file
        .metadata()
        .map_err(|e| format!("descriptor.json exists but cannot be stat'd: {e}"))?;
    if !fmeta.is_file() {
        return Err(
            "descriptor.json exists but is not a regular file (symlink/special file) — \
             refusing to read it"
                .to_string(),
        );
    }
    if fmeta.len() > MAX_DESCRIPTOR_BYTES {
        return Err(format!(
            "descriptor.json exists but is {} bytes, over the {MAX_DESCRIPTOR_BYTES}-byte cap",
            fmeta.len()
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_DESCRIPTOR_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|e| format!("descriptor.json exists but cannot be read: {e}"))?;
    if bytes.len() as u64 > MAX_DESCRIPTOR_BYTES {
        return Err(format!(
            "descriptor.json exists but grew over the {MAX_DESCRIPTOR_BYTES}-byte cap while \
             being read"
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| format!("descriptor.json exists but is not valid JSON: {e}"))?;
    let Some(data_source) = value.get("dataSource").and_then(|v| v.as_str()) else {
        return Err(
            "descriptor.json exists and parses but has no string `dataSource` field".to_string(),
        );
    };
    let data_source = checked_identity_field("dataSource", data_source)?;
    // H1 (Codex compat-2 R4): a field that is PRESENT but malformed is a
    // loud error — `None` is reserved for a genuinely ABSENT key, the
    // only case the identity merge may backfill from the path.  A string
    // `"7"` partitionNum silently becoming path partition `0` would
    // attach the segment under a WRONG identity.
    let interval = descriptor_string_field(&value, "interval")?;
    let version = descriptor_string_field(&value, "version")?;
    let partition = descriptor_partition(&value)?;
    Ok(Some(SegmentIdentity {
        data_source: Some(data_source),
        interval,
        version,
        partition,
        source: Some(IdentitySource::DescriptorJson),
        descriptor_error: None,
    }))
}

/// Extract an OPTIONAL string identity field (`interval` / `version`)
/// from a decoded descriptor, strictly (H1 — Codex compat-2 R4): an
/// ABSENT key yields `Ok(None)` (the identity merge may fill it from
/// the path — the legitimate partial-descriptor case), while a key that
/// is PRESENT with anything but a JSON string (number, null, object,
/// ...) is a loud error.  A present-but-malformed authoritative field
/// must never be silently replaced by the path-derived value.  Only the
/// value's TYPE is echoed, never the value itself (a hostile descriptor
/// could otherwise bloat the retained reason by megabytes — the same
/// concern [`checked_identity_field`] addresses by rejecting over-long
/// identity fields outright).
fn descriptor_string_field(value: &serde_json::Value, key: &str) -> Result<Option<String>, String> {
    match value.get(key) {
        None => Ok(None),
        Some(v) => match v.as_str() {
            Some(s) => Ok(Some(checked_identity_field(key, s)?)),
            None => Err(format!(
                "descriptor.json field `{key}` is present but is {} instead of a string — \
                 refusing the silent path-value fallback for a present-but-malformed field",
                json_type_name(v)
            )),
        },
    }
}

/// Extract the OPTIONAL `shardSpec.partitionNum` from a decoded
/// descriptor, strictly (H1 — Codex compat-2 R4): an absent `shardSpec`
/// key — or an absent `partitionNum` key inside a shardSpec OBJECT —
/// yields `Ok(None)` (path fallback), while anything PRESENT must be
/// well-formed: `shardSpec` a JSON object and `partitionNum` a
/// NON-NEGATIVE integer in the i64 range.  A string (`"7"`), float,
/// negative, over-range, or null partitionNum is a loud error — the
/// merge silently adopting the path partition (e.g. `0`) instead would
/// commit the segment under a WRONG identity.
fn descriptor_partition(value: &serde_json::Value) -> Result<Option<String>, String> {
    let Some(shard_spec) = value.get("shardSpec") else {
        return Ok(None);
    };
    let Some(shard_obj) = shard_spec.as_object() else {
        return Err(format!(
            "descriptor.json field `shardSpec` is present but is {} instead of an object — \
             refusing the silent path-value fallback for a present-but-malformed field",
            json_type_name(shard_spec)
        ));
    };
    let Some(partition) = shard_obj.get("partitionNum") else {
        return Ok(None);
    };
    let malformed = |what: String| {
        format!(
            "descriptor.json field `shardSpec.partitionNum` is present but is {what} — a \
             partition must be a non-negative integer; refusing the silent path-value \
             fallback for a present-but-malformed field"
        )
    };
    let Some(n) = partition.as_i64() else {
        return Err(malformed(format!(
            "{} (not an integer in the i64 range)",
            json_type_name(partition)
        )));
    };
    if n < 0 {
        return Err(malformed("negative".to_string()));
    }
    Ok(Some(n.to_string()))
}

/// Short JSON type name for the loud descriptor-field errors above —
/// the VALUE is deliberately never echoed into a reason (a hostile
/// descriptor holds up to [`MAX_DESCRIPTOR_BYTES`] of it).
fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

/// Reject — never truncate — a descriptor-supplied identity field
/// (`dataSource` / `interval` / `version`) longer than
/// [`MAX_IDENTITY_FIELD_CHARS`].
///
/// These fields flow into the `ferro_id`
/// (`<ds>_<start>_<end>_<version>_<partition>`). Truncating two distinct
/// fields that share a long prefix onto one bounded value would collapse
/// them onto ONE `ferro_id` — a silent collision skip, or a `--force`
/// overwrite of the first segment by the second = data loss (Codex R6,
/// same species as the R5 lossy-decode collapse). A real Druid identity
/// field is never this long, so rejecting is fail-closed AND still bounds
/// the retained report (the loud reason carries only the length, never the
/// multi-megabyte field body — the R4 bloat concern this replaces).
fn checked_identity_field(field: &str, s: &str) -> Result<String, String> {
    let n = s.chars().count();
    if n <= MAX_IDENTITY_FIELD_CHARS {
        Ok(s.to_string())
    } else {
        Err(format!(
            "descriptor `{field}` is {n} chars, over the \
             {MAX_IDENTITY_FIELD_CHARS}-char identity limit — refusing to \
             truncate an identity field onto a colliding ferro_id"
        ))
    }
}

/// Infer identity from `<root>/<dataSource>/<interval>/<version>/
/// <partitionNum>[/index]`.  A trailing `index` component (Druid 31
/// local layout) is stripped before matching.  Only an exact 4-level
/// match is trusted; anything else returns `None`.
fn identity_from_path(root: &Path, segment_dir: &Path) -> Option<SegmentIdentity> {
    let rel = segment_dir.strip_prefix(root).ok()?;
    // Reject a non-UTF-8 path component rather than lossy-decoding it
    // (Codex R5): `to_string_lossy` maps every distinct invalid-byte
    // sequence to the same U+FFFD, which would collapse two source
    // segments whose components differ only in invalid bytes (e.g. version
    // `v\x80` vs `v\x81`) onto one ferro_id — a silent collision skip, or a
    // `--force` overwrite of the first by the second = data loss. No
    // reliable path identity ⇒ `None`, which the caller turns into a loud
    // identity-incomplete skip.
    let mut parts: Vec<String> = Vec::with_capacity(4);
    for c in rel.components() {
        parts.push(c.as_os_str().to_str()?.to_string());
    }
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
        descriptor_error: None,
    })
}

// ---------------------------------------------------------------------------
// Per-segment assessment
// ---------------------------------------------------------------------------

/// An artifact materialized into a directory of smoosh files the v9
/// reader (and the deep-storage uploader) can consume: the segment dir
/// itself for raw layouts, a temp extraction dir for `index.zip`.
///
/// The optional temp-dir guard keeps an extraction alive for as long as
/// the caller holds the value — `attach` holds it across the read gate,
/// the content hash, AND the deep-storage upload, so all three see the
/// exact same file set.
#[derive(Debug)]
pub(crate) struct MaterializedArtifact {
    dir: PathBuf,
    _extract_guard: Option<tempfile::TempDir>,
}

impl MaterializedArtifact {
    /// The directory holding the segment's smoosh file set.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }
}

/// Materialize one artifact for reading/uploading: reject non-regular
/// children for a raw smoosh dir, extract (with the zip-slip/zip-bomb
/// caps) for an `index.zip`.  Every failure is a human-readable reason,
/// never a propagated error.
pub(crate) fn materialize_artifact(
    root: &Path,
    artifact: &FoundArtifact,
) -> Result<MaterializedArtifact, String> {
    match artifact.kind {
        ArtifactKind::SmooshDir => {
            // Ancestor containment (compat-2 R4 H2): the dir the reader
            // is about to load whole must still RESOLVE under the source
            // root — a partition dir swapped for a symlink to an
            // external dir would otherwise hand the reader foreign
            // bytes under the original identity.
            verify_source_resolves_under_root(root, &artifact.path)
                .map_err(|e| format!("segment dir failed the source-containment check: {e}"))?;
            // Symlinks must be excluded end-to-end (Codex R2 finding):
            // the walk skips symlinked dirs, but a recognized segment
            // dir could still contain symlinked chunks/sidecars that
            // the reader would follow (e.g. to a FIFO or /dev/zero).
            reject_non_regular_children(&artifact.path)?;
            Ok(MaterializedArtifact {
                dir: artifact.path.clone(),
                _extract_guard: None,
            })
        }
        ArtifactKind::IndexZip => {
            let tmp = extract_index_zip(root, &artifact.path)?;
            Ok(MaterializedArtifact {
                dir: tmp.path().to_path_buf(),
                _extract_guard: Some(tmp),
            })
        }
    }
}

/// Materialize one artifact for the attach P→M pipeline: like
/// [`materialize_artifact`], but ALWAYS into a private staging tempdir
/// (Codex compat-2 H1).
///
/// The in-place variant hands a raw smoosh dir back as-is, so `attach`'s
/// verify → hash → upload sequence would read the LIVE source directory
/// three times: a mutation between the content hash and the upload makes
/// the committed sha256 diverge from the uploaded bytes, and the next
/// bootstrap reload bricks on the hash check.  Staging pins one immutable
/// byte set for all three steps:
///
/// * `index.zip` — the extraction tempdir already IS a private staging
///   dir (unchanged);
/// * raw smoosh dir — every top-level regular file is COPIED into a
///   fresh tempdir (the exact set `upload_segment` uploads and
///   `blob_content_hash` hashes).  Each file is re-checked as a regular
///   file on its OPEN handle (same TOCTOU discipline as the descriptor
///   probe) so a racing swap to a symlink/FIFO is refused, and the same
///   [`MAX_DIR_ENTRIES`] bound applies as everywhere else.
///
/// Honest cost: a raw smoosh dir is copied once more per attach.  That
/// is the price of the committed-hash == uploaded-bytes guarantee.
pub(crate) fn materialize_artifact_staged(
    root: &Path,
    artifact: &FoundArtifact,
) -> Result<MaterializedArtifact, String> {
    match artifact.kind {
        ArtifactKind::SmooshDir => {
            // Ancestor containment (compat-2 R4 H2): a partition dir
            // swapped for a symlink to an EXTERNAL dir after the scan
            // would have the staging copy pull foreign bytes that are
            // then hashed, uploaded, and COMMITTED under the original
            // in-tree identity — refuse before reading anything.
            verify_source_resolves_under_root(root, &artifact.path)
                .map_err(|e| format!("segment dir failed the source-containment check: {e}"))?;
            reject_non_regular_children(&artifact.path)?;
            let tmp = tempfile::tempdir()
                .map_err(|e| format!("failed to create staging dir for attach: {e}"))?;
            let read = std::fs::read_dir(&artifact.path)
                .map_err(|e| format!("failed to read segment dir for staging: {e}"))?;
            for (seen, entry) in read.enumerate() {
                if seen >= MAX_DIR_ENTRIES {
                    return Err(format!(
                        "segment dir has more than {MAX_DIR_ENTRIES} entries — refusing to stage"
                    ));
                }
                let entry =
                    entry.map_err(|e| format!("failed to enumerate segment dir entry: {e}"))?;
                let file_type = entry
                    .file_type()
                    .map_err(|e| format!("failed to stat a segment dir entry: {e}"))?;
                if !file_type.is_file() {
                    // Plain subdirectories are tolerated (ignored) exactly
                    // like the reader and the uploader ignore them; symlinks
                    // and special files were already rejected above.
                    continue;
                }
                let name = entry.file_name();
                let child_path = entry.path();
                // `O_NOFOLLOW` open (compat-2 R2 H2): a child swapped for
                // a symlink to an external regular file after the
                // `reject_non_regular_children` gate would pass the
                // handle `is_file()` re-check below — the open itself
                // must refuse to follow it.
                let mut src = open_source_file_nofollow(&child_path).map_err(|e| {
                    format!(
                        "failed to open `{}` for staging: {e}",
                        name.to_string_lossy()
                    )
                })?;
                // Per-child ancestor containment (compat-2 R4 H2): the
                // dir-level check above is a point-in-time gate; re-check
                // each opened child so an ancestor swapped mid-staging is
                // still caught before its bytes are staged.
                verify_source_resolves_under_root(root, &child_path).map_err(|e| {
                    format!(
                        "staging `{}` failed the source-containment check: {e}",
                        name.to_string_lossy()
                    )
                })?;
                // Handle-based re-check: the pre-open scan is TOCTOU-prone.
                let fmeta = src.metadata().map_err(|e| {
                    format!(
                        "failed to stat `{}` for staging: {e}",
                        name.to_string_lossy()
                    )
                })?;
                if !fmeta.is_file() {
                    return Err(format!(
                        "segment dir entry `{}` is not a regular file — refusing to stage",
                        name.to_string_lossy()
                    ));
                }
                let mut dst = std::fs::File::create(tmp.path().join(&name)).map_err(|e| {
                    format!("failed to create staged `{}`: {e}", name.to_string_lossy())
                })?;
                std::io::copy(&mut src, &mut dst)
                    .map_err(|e| format!("failed to stage `{}`: {e}", name.to_string_lossy()))?;
            }
            Ok(MaterializedArtifact {
                dir: tmp.path().to_path_buf(),
                _extract_guard: Some(tmp),
            })
        }
        ArtifactKind::IndexZip => materialize_artifact(root, artifact),
    }
}

/// Open a materialized smoosh dir with the real v9 reader.  The reader
/// returns errors rather than panicking; catch_unwind is
/// defence-in-depth so one hostile segment cannot abort a scan or an
/// attach run.  Every failure becomes a reason string, verbatim from
/// the failing layer.
pub(crate) fn open_segment_guarded(dir: &Path) -> Result<SegmentData, String> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| SegmentData::open(dir)));
    match result {
        Ok(Ok(segment)) => Ok(segment),
        Ok(Err(e)) => Err(e.to_string()),
        Err(panic) => Err(panic_reason(panic.as_ref())),
    }
}

/// LENIENT counterpart of [`open_segment_guarded`] — the
/// `--allow-unreadable-columns` read gate.  Opens via
/// [`SegmentData::open_lenient`], so a column whose decode fails is
/// DROPPED and named in the returned manifest instead of failing the
/// whole segment; everything else (panic catch, reason strings) matches
/// the strict opener.
pub(crate) fn open_segment_guarded_lenient(
    dir: &Path,
) -> Result<(SegmentData, Vec<String>), String> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        SegmentData::open_lenient(dir)
    }));
    match result {
        Ok(Ok(pair)) => Ok(pair),
        Ok(Err(e)) => Err(e.to_string()),
        Err(panic) => Err(panic_reason(panic.as_ref())),
    }
}

/// Human-readable reason string for a caught reader panic (shared by the
/// strict and lenient guarded openers).
fn panic_reason(panic: &(dyn std::any::Any + Send)) -> String {
    let msg = panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string());
    format!("reader panicked (defensive catch): {msg}")
}

/// Assess one artifact: extract if zipped, then ask the real v9 reader
/// to open it.  Never propagates a failure — every failure becomes an
/// `Outcome::Unreadable` with the fail-loud reason preserved.
fn assess_artifact(root: &Path, artifact: &FoundArtifact) -> Outcome {
    // Hold the materialization so an extraction lives until the read is
    // done.
    let materialized = match materialize_artifact(root, artifact) {
        Ok(m) => m,
        Err(reason) => return Outcome::Unreadable { reason },
    };
    match open_segment_guarded(materialized.dir()) {
        Ok(segment) => {
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
        Err(reason) => Outcome::Unreadable { reason },
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
fn extract_index_zip(root: &Path, zip_path: &Path) -> Result<tempfile::TempDir, String> {
    // `O_NOFOLLOW` open (compat-2 R2 H2): the scan only RECOGNIZES real
    // files as artifacts (lstat semantics), so an `index.zip` that is a
    // symlink here was swapped in after the scan — refuse to follow it
    // instead of extracting external bytes.
    let file = open_source_file_nofollow(zip_path)
        .map_err(|e| format!("failed to open index.zip: {e}"))?;
    // Ancestor containment (compat-2 R4 H2): a symlinked ANCESTOR
    // (partition dir → external dir) passes the `O_NOFOLLOW` open with a
    // perfectly regular external zip — refuse it before extracting.
    verify_source_resolves_under_root(root, zip_path)
        .map_err(|e| format!("index.zip failed the source-containment check: {e}"))?;
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
const VERDICT_NOTE: &str = "readable = openable by FerroDruid's v9 reader — attachable with \
     `ferrodruid-migrate attach`. Dry-run assessment only: nothing is migrated.";

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
pub(crate) fn sanitize(s: &str) -> String {
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
         v9 reader (attachable with `ferrodruid-migrate attach`), {} unreadable.",
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

// ---------------------------------------------------------------------------
// Tests — Codex R2 (compat-2) H2: source opens must not follow a
// replacement symlink (TOCTOU)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// H2 (mechanism): the shared `O_NOFOLLOW` open helper — used for
    /// every untrusted source open (descriptor.json / index.zip / raw
    /// smoosh staging) — must refuse a final-component symlink even when
    /// its target is a perfectly normal REGULAR file (the case the
    /// handle-based `is_file()` re-checks cannot catch), while a real
    /// file keeps opening.  The staging open (a symlink swapped in
    /// between the child gate and the open) is reachable only through
    /// that race, so the mechanism is pinned here directly.
    #[cfg(unix)]
    #[test]
    fn open_source_file_nofollow_refuses_a_symlink_to_a_regular_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real = dir.path().join("real.bin");
        std::fs::write(&real, b"bytes").expect("write real file");
        let link = dir.path().join("link.bin");
        std::os::unix::fs::symlink(&real, &link).expect("plant symlink");

        assert!(
            open_source_file_nofollow(&real).is_ok(),
            "a real regular file must keep opening"
        );
        assert!(
            open_source_file_nofollow(&link).is_err(),
            "a symlink final component must fail the open (O_NOFOLLOW), \
             even though its target is a regular file"
        );
    }

    /// H2: a scanned `index.zip` swapped for a SYMLINK to an external
    /// zip between the scan's lstat gate and the open must be REFUSED —
    /// the open itself fails (`O_NOFOLLOW`) so external bytes are never
    /// extracted / staged / uploaded. (The scan never RECOGNIZES a
    /// symlink as an artifact, so reaching this open with one is exactly
    /// the TOCTOU replacement case.)
    #[cfg(unix)]
    #[test]
    fn extract_index_zip_refuses_a_symlinked_zip() {
        use std::io::Write as _;

        // A perfectly VALID zip outside the source root.
        let outside = tempfile::tempdir().expect("outside tempdir");
        let real_zip = outside.path().join("external.zip");
        {
            let file = std::fs::File::create(&real_zip).expect("create zip");
            let mut zip = zip::ZipWriter::new(file);
            let options: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default();
            zip.start_file("meta.smoosh", options).expect("start_file");
            zip.write_all(b"v1,2147483647,1").expect("write entry");
            zip.finish().expect("finish zip");
        }

        let root = tempfile::tempdir().expect("root tempdir");
        let link = root.path().join("index.zip");
        std::os::unix::fs::symlink(&real_zip, &link).expect("plant symlink");

        let err = extract_index_zip(root.path(), &link)
            .expect_err("a symlinked index.zip must fail the open, not extract external bytes");
        assert!(
            err.contains("failed to open index.zip"),
            "expected a loud open failure, got: {err}"
        );

        // The same zip via its REAL path stays extractable (no regression
        // for genuine files).
        assert!(
            extract_index_zip(outside.path(), &real_zip).is_ok(),
            "a real (non-symlink) zip must keep extracting"
        );
    }

    /// H2 guard pin: a `descriptor.json` that IS a symlink surfaces as a
    /// loud descriptor error (never a silent follow, never a silent path
    /// fallback) — the static lstat gate catches the planted case and
    /// the `O_NOFOLLOW` open closes the swap-after-stat race.
    #[cfg(unix)]
    #[test]
    fn identity_from_descriptor_refuses_a_symlinked_descriptor() {
        let outside = tempfile::tempdir().expect("outside tempdir");
        let real = outside.path().join("descriptor.json");
        std::fs::write(&real, br#"{"dataSource":"evil"}"#).expect("write real descriptor");

        let dir = tempfile::tempdir().expect("tempdir");
        let link = dir.path().join("descriptor.json");
        std::os::unix::fs::symlink(&real, &link).expect("plant symlink");

        let err = identity_from_descriptor(dir.path(), &link)
            .expect_err("a symlinked descriptor.json must surface a loud error");
        assert!(
            err.contains("not a regular file"),
            "expected the symlink refusal, got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Codex R4 (compat-2) H1: a descriptor FIELD that is PRESENT but
    // malformed is loud — never a silent path-value backfill
    // -----------------------------------------------------------------------

    /// Write `descriptor` into a fresh tempdir and probe it with
    /// [`identity_from_descriptor`] (root = the tempdir).
    fn probe_descriptor(descriptor: &serde_json::Value) -> Result<Option<SegmentIdentity>, String> {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("descriptor.json");
        std::fs::write(&path, descriptor.to_string()).expect("write descriptor");
        identity_from_descriptor(dir.path(), &path)
    }

    /// H1: every authoritative identity field that is PRESENT in the
    /// descriptor but not usable (wrong JSON type / negative / over-range
    /// partition) must surface a loud error naming the field — the merge
    /// must never silently replace it with the path-derived value (a
    /// string `"7"` partitionNum silently becoming path partition `0`
    /// would commit the segment under a WRONG identity).
    #[test]
    fn descriptor_present_but_invalid_fields_are_loud() {
        let cases: Vec<(serde_json::Value, &str)> = vec![
            // partitionNum present but a STRING
            (
                serde_json::json!({"dataSource":"ds","shardSpec":{"partitionNum":"7"}}),
                "partitionNum",
            ),
            // partitionNum present but NEGATIVE
            (
                serde_json::json!({"dataSource":"ds","shardSpec":{"partitionNum":-1}}),
                "partitionNum",
            ),
            // partitionNum present but a FLOAT
            (
                serde_json::json!({"dataSource":"ds","shardSpec":{"partitionNum":7.5}}),
                "partitionNum",
            ),
            // partitionNum present but NULL
            (
                serde_json::json!({"dataSource":"ds","shardSpec":{"partitionNum":null}}),
                "partitionNum",
            ),
            // partitionNum present but OVER the i64 range (u64 territory)
            (
                serde_json::from_str(
                    r#"{"dataSource":"ds","shardSpec":{"partitionNum":9223372036854775808}}"#,
                )
                .expect("parse over-range descriptor"),
                "partitionNum",
            ),
            // shardSpec present but not an OBJECT
            (
                serde_json::json!({"dataSource":"ds","shardSpec":"numbered"}),
                "shardSpec",
            ),
            // interval present but a NUMBER
            (
                serde_json::json!({"dataSource":"ds","interval":123}),
                "interval",
            ),
            // interval present but NULL
            (
                serde_json::json!({"dataSource":"ds","interval":null}),
                "interval",
            ),
            // version present but an OBJECT
            (
                serde_json::json!({"dataSource":"ds","version":{}}),
                "version",
            ),
        ];
        for (descriptor, field) in cases {
            match probe_descriptor(&descriptor) {
                Err(reason) => assert!(
                    reason.contains(field),
                    "the loud reason must name `{field}`, got: {reason}"
                ),
                Ok(None) => panic!("descriptor {descriptor} unexpectedly treated as absent"),
                Ok(Some(id)) => panic!(
                    "descriptor {descriptor} with a present-but-invalid `{field}` must be \
                     a LOUD error, never a usable identity (got {id:?}) — the merge would \
                     silently backfill the path value"
                ),
            }
        }
    }

    /// Codex R6: an identity field (dataSource / interval / version) over
    /// the identity-length limit is a LOUD skip, not a truncation. Two
    /// descriptors whose dataSource shares the first
    /// `MAX_IDENTITY_FIELD_CHARS` chars but differs afterward must BOTH be
    /// rejected — truncating them would collapse them onto one ferro_id
    /// (silent collision skip / --force overwrite = data loss).
    #[test]
    fn descriptor_over_long_identity_field_is_loud_not_truncated() {
        let shared = "d".repeat(MAX_IDENTITY_FIELD_CHARS);
        for (field, descriptor) in [
            (
                "dataSource",
                serde_json::json!({ "dataSource": format!("{shared}A") }),
            ),
            (
                "dataSource",
                serde_json::json!({ "dataSource": format!("{shared}B") }),
            ),
            (
                "version",
                serde_json::json!({ "dataSource": "ds", "version": format!("{shared}A") }),
            ),
            (
                "interval",
                serde_json::json!({ "dataSource": "ds", "interval": format!("{shared}A") }),
            ),
        ] {
            match probe_descriptor(&descriptor) {
                Err(reason) => assert!(
                    reason.contains(field) && reason.contains("identity limit"),
                    "the loud reason must name `{field}` and the identity limit, got: {reason}"
                ),
                other => panic!(
                    "an over-long `{field}` must be a LOUD error, never truncated onto a \
                     colliding identity (got {other:?})"
                ),
            }
        }
        // A field exactly at the limit is still accepted (boundary, not off-by-one).
        assert!(
            probe_descriptor(&serde_json::json!({ "dataSource": shared })).is_ok(),
            "a dataSource exactly at the limit must remain accepted"
        );
    }

    /// H1 regression guard: a field that is genuinely ABSENT (no key at
    /// all — the legitimate partial-descriptor case) still falls back to
    /// the path, exactly as before.
    #[test]
    fn descriptor_absent_fields_still_fall_back_to_path() {
        let id = probe_descriptor(&serde_json::json!({"dataSource":"ds"}))
            .expect("dataSource-only descriptor is usable")
            .expect("descriptor present");
        assert_eq!(id.data_source.as_deref(), Some("ds"));
        assert!(
            id.interval.is_none() && id.version.is_none() && id.partition.is_none(),
            "absent fields stay None so the path merge can fill them"
        );

        // A shardSpec OBJECT without a `partitionNum` key: the partition
        // field is absent, not invalid.
        let id = probe_descriptor(
            &serde_json::json!({"dataSource":"ds","shardSpec":{"type":"numbered"}}),
        )
        .expect("partitionNum-less shardSpec object is usable")
        .expect("descriptor present");
        assert!(
            id.partition.is_none(),
            "an absent partitionNum key falls back to the path"
        );
    }

    /// Codex R9: a descriptor OBSERVED to exist at the lstat gate that
    /// then fails to open — INCLUDING with `NotFound` (a concurrent
    /// unlink/rename between the stat and the open) — is a LOUD identity
    /// failure, never the silent `Ok(None)` path fallback that the
    /// genuinely-absent (lstat-`NotFound`) case uses.  The exact TOCTOU is
    /// not deterministically reproducible from outside (the racing window
    /// also produces the legitimate lstat-miss `Ok(None)`), so the
    /// classification is pinned here at its extracted seam: `NotFound`
    /// must NOT be special-cased back to a silent fallback.
    #[test]
    fn present_descriptor_open_failure_is_loud_even_for_notfound() {
        for kind in [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::Other,
        ] {
            let reason = present_descriptor_open_failure(&std::io::Error::from(kind));
            assert!(
                reason.contains("was present") && reason.contains("refusing"),
                "a present-but-unopenable descriptor ({kind:?}) must be a loud \
                 path-fallback refusal, never a silent absent, got: {reason}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Codex R4 (compat-2) H2: an ANCESTOR directory swapped for a symlink
    // to an EXTERNAL dir after the scan must not smuggle external bytes
    // in under the original identity
    // -----------------------------------------------------------------------

    /// Write a minimal VALID zip (one `meta.smoosh` entry) at `path`.
    #[cfg(unix)]
    fn write_minimal_zip(path: &Path) {
        use std::io::Write as _;
        let file = std::fs::File::create(path).expect("create zip");
        let mut zip = zip::ZipWriter::new(file);
        let options: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default();
        zip.start_file("meta.smoosh", options).expect("start_file");
        zip.write_all(b"v1,2147483647,1").expect("write entry");
        zip.finish().expect("finish zip");
    }

    /// Build `<root>/ds/iv/v1/0` where `0` IS a symlink to `target` —
    /// the post-scan ancestor-swap state (the scan itself never records
    /// symlinks, so reaching an open through one is exactly the TOCTOU
    /// replacement case the final-component `O_NOFOLLOW` cannot catch).
    #[cfg(unix)]
    fn plant_ancestor_symlink(root: &Path, target: &Path) -> PathBuf {
        let version_dir = root.join("ds").join("iv").join("v1");
        std::fs::create_dir_all(&version_dir).expect("mkdir version dir");
        let part_dir = version_dir.join("0");
        std::os::unix::fs::symlink(target, &part_dir).expect("plant ancestor symlink");
        part_dir
    }

    /// H2: an `index.zip` reached through a symlinked ANCESTOR (partition
    /// dir → external dir) must be refused — the final-component
    /// `O_NOFOLLOW` open succeeds on the external REGULAR file, so the
    /// canonical-containment re-check has to catch the escape before any
    /// external bytes are extracted.
    #[cfg(unix)]
    #[test]
    fn extract_index_zip_refuses_an_ancestor_symlink_escape() {
        let outside = tempfile::tempdir().expect("outside tempdir");
        let ext_part = outside.path().join("part");
        std::fs::create_dir_all(&ext_part).expect("mkdir external part");
        write_minimal_zip(&ext_part.join("index.zip"));

        let root = tempfile::tempdir().expect("root tempdir");
        let part_dir = plant_ancestor_symlink(root.path(), &ext_part);

        let err = extract_index_zip(root.path(), &part_dir.join("index.zip")).expect_err(
            "an index.zip reached through a symlinked ancestor must be refused, \
             not extracted from outside the source root",
        );
        assert!(
            err.contains("OUTSIDE") || err.contains("escape"),
            "expected the containment refusal, got: {err}"
        );
    }

    /// H2: a `descriptor.json` reached through a symlinked ANCESTOR must
    /// be refused loudly — external bytes must never be adopted as the
    /// authoritative identity.
    #[cfg(unix)]
    #[test]
    fn identity_from_descriptor_refuses_an_ancestor_symlink_escape() {
        let outside = tempfile::tempdir().expect("outside tempdir");
        let ext_part = outside.path().join("part");
        std::fs::create_dir_all(&ext_part).expect("mkdir external part");
        std::fs::write(
            ext_part.join("descriptor.json"),
            br#"{"dataSource":"evil"}"#,
        )
        .expect("write external descriptor");

        let root = tempfile::tempdir().expect("root tempdir");
        let part_dir = plant_ancestor_symlink(root.path(), &ext_part);

        let err = identity_from_descriptor(root.path(), &part_dir.join("descriptor.json"))
            .expect_err(
                "a descriptor.json reached through a symlinked ancestor must surface \
                 a loud error, never external identity bytes",
            );
        assert!(
            err.contains("OUTSIDE") || err.contains("escape"),
            "expected the containment refusal, got: {err}"
        );
    }

    /// Codex R5: two source segments whose path components differ ONLY in
    /// invalid UTF-8 bytes (version `v\x80` vs `v\x81`) must NOT collapse
    /// onto one path identity. `to_string_lossy` maps both to `v\u{FFFD}`,
    /// which would give them the same `ferro_id` — a silent collision skip
    /// or `--force` overwrite = data loss. The reject-non-UTF-8 rule turns
    /// each into `None` (⇒ a loud identity-incomplete skip upstream), so no
    /// two distinct segments can ever share a lossily-decoded identity.
    #[cfg(unix)]
    #[test]
    fn identity_from_path_rejects_a_non_utf8_component() {
        use std::os::unix::ffi::OsStrExt as _;

        let root = tempfile::tempdir().expect("root tempdir");
        // <root>/wiki/2020-01-01T00:00:00.000Z_.../v<byte>/0
        let interval = "2020-01-01T00:00:00.000Z_2020-01-02T00:00:00.000Z";
        let mk = |byte: u8| {
            let mut version = std::ffi::OsString::from("v");
            version.push(std::ffi::OsStr::from_bytes(&[byte]));
            let dir = root
                .path()
                .join("wiki")
                .join(interval)
                .join(&version)
                .join("0");
            std::fs::create_dir_all(&dir).expect("mkdir segment dir");
            dir
        };
        let dir_80 = mk(0x80);
        let dir_81 = mk(0x81);

        assert!(
            identity_from_path(root.path(), &dir_80).is_none(),
            "a non-UTF-8 version component must not yield a lossy path identity"
        );
        assert!(
            identity_from_path(root.path(), &dir_81).is_none(),
            "a distinct non-UTF-8 version component must also be rejected, \
             so the two cannot collapse onto one ferro_id"
        );
        // A valid-UTF-8 sibling still resolves (the guard is byte-scoped,
        // not a blanket refusal of the path-fallback identity).
        let ok_dir = root.path().join("wiki").join(interval).join("v1").join("0");
        std::fs::create_dir_all(&ok_dir).expect("mkdir valid segment dir");
        assert!(
            identity_from_path(root.path(), &ok_dir).is_some(),
            "a fully valid-UTF-8 4-level layout must still resolve"
        );
    }

    /// H2: a raw smoosh dir whose path traverses a symlinked ANCESTOR
    /// must be refused by BOTH materializations (in-place for `assess`,
    /// staged for `attach`) — otherwise external smoosh bytes are staged,
    /// hashed, uploaded, and committed under the original identity.
    #[cfg(unix)]
    #[test]
    fn materializations_refuse_an_ancestor_symlink_escape() {
        let outside = tempfile::tempdir().expect("outside tempdir");
        let ext_part = outside.path().join("part");
        std::fs::create_dir_all(&ext_part).expect("mkdir external part");
        std::fs::write(ext_part.join("meta.smoosh"), b"v1,2147483647,1").expect("write meta");
        std::fs::write(ext_part.join("00000.smoosh"), b"EXTERNAL-BYTES").expect("write chunk");

        let root = tempfile::tempdir().expect("root tempdir");
        let part_dir = plant_ancestor_symlink(root.path(), &ext_part);
        let artifact = FoundArtifact {
            path: part_dir,
            kind: ArtifactKind::SmooshDir,
        };

        let err = materialize_artifact_staged(root.path(), &artifact).expect_err(
            "staging a smoosh dir through a symlinked ancestor must be refused, \
             not stage external bytes for hash/upload/commit",
        );
        assert!(
            err.contains("OUTSIDE") || err.contains("escape"),
            "expected the containment refusal (staged), got: {err}"
        );

        let err = materialize_artifact(root.path(), &artifact)
            .expect_err("the in-place materialization must refuse a symlinked ancestor too");
        assert!(
            err.contains("OUTSIDE") || err.contains("escape"),
            "expected the containment refusal (in-place), got: {err}"
        );
    }

    /// H2 regression guard: a source ROOT that is itself a symlink (a
    /// legitimate mount layout) keeps working — both sides of the
    /// containment check are canonicalized, so the resolved paths agree.
    #[cfg(unix)]
    #[test]
    fn symlinked_source_root_itself_is_legitimate() {
        let holder = tempfile::tempdir().expect("holder tempdir");
        let real_root = holder.path().join("real");
        let part_dir = real_root.join("ds").join("iv").join("v1").join("0");
        std::fs::create_dir_all(&part_dir).expect("mkdir partition dir");
        write_minimal_zip(&part_dir.join("index.zip"));
        std::fs::write(
            part_dir.join("descriptor.json"),
            br#"{"dataSource":"legit"}"#,
        )
        .expect("write descriptor");

        let link_root = holder.path().join("link");
        std::os::unix::fs::symlink(&real_root, &link_root).expect("symlink root");

        let linked_part = link_root.join("ds").join("iv").join("v1").join("0");
        assert!(
            extract_index_zip(&link_root, &linked_part.join("index.zip")).is_ok(),
            "a legitimately symlinked source ROOT must keep extracting"
        );
        let id = identity_from_descriptor(&link_root, &linked_part.join("descriptor.json"))
            .expect("descriptor under a symlinked root stays readable")
            .expect("descriptor present");
        assert_eq!(id.data_source.as_deref(), Some("legit"));
    }
}
