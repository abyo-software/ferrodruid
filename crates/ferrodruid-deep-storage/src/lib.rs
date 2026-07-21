// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! S3/GCS/Azure/Local deep storage abstraction for FerroDruid.
//!
//! Deep storage is where segments are permanently stored after ingestion.
//! The [`DeepStorage`] trait abstracts over different backends (local
//! filesystem, S3, etc.) and the [`LocalDeepStorage`] provides a
//! filesystem-backed implementation suitable for single-node deployments.
//! [`S3DeepStorage`] provides S3-compatible object storage via the
//! [`object_store`] crate, and [`InMemoryDeepStorage`] wraps an in-memory
//! store for testing.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod segment_artifact;

pub use segment_artifact::{
    ColumnSpec, ColumnType, Segment, SegmentArtifactError, SegmentHeader, SegmentRow,
};

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use ferrodruid_common::config::{DeepStorageConfig, DeepStorageType};
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use thiserror::Error;

/// Errors from deep storage operations.
#[derive(Debug, Error)]
pub enum DeepStorageError {
    /// An I/O error occurred.
    #[error("deep storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The requested segment was not found.
    #[error("segment not found: {data_source}/{segment_id}")]
    NotFound {
        /// Data source name.
        data_source: String,
        /// Segment identifier.
        segment_id: String,
    },

    /// A generic backend error.
    #[error("deep storage error: {0}")]
    Other(String),
}

/// Convenience alias for deep storage results.
pub type Result<T> = std::result::Result<T, DeepStorageError>;

/// Abstraction over deep storage backends.
///
/// Implementations must be safe to share across threads and usable from
/// async contexts.
#[async_trait]
pub trait DeepStorage: Send + Sync {
    /// List segment identifiers stored for a given data source.
    async fn list_segments(&self, data_source: &str) -> Result<Vec<String>>;

    /// Download a segment directory from deep storage to a local path.
    async fn download_segment(
        &self,
        data_source: &str,
        segment_id: &str,
        dest: &Path,
    ) -> Result<()>;

    /// Upload a segment directory from a local path to deep storage.
    async fn upload_segment(&self, data_source: &str, segment_id: &str, src: &Path) -> Result<()>;

    /// Delete a segment from deep storage.
    ///
    /// Idempotent: deleting a segment that does not (or no longer) exist
    /// succeeds with `Ok(())` rather than surfacing a `NotFound` error.
    /// This matches the natural semantics of S3's `DELETE` (which returns
    /// 204 for missing keys) and keeps cleanup workflows safe to retry.
    async fn delete_segment(&self, data_source: &str, segment_id: &str) -> Result<()>;

    /// Check whether a segment exists in deep storage.
    async fn segment_exists(&self, data_source: &str, segment_id: &str) -> Result<bool>;

    /// A short, stable identifier for this backend (`"local"`, `"s3"`, …).
    ///
    /// Stamped into a persisted segment's `loadSpec` descriptor as a
    /// forward-compatible marker of where the segment blob lives. The
    /// bootstrap reload does not parse it yet — it downloads by
    /// `(data_source, segment_id)` directly — so it is informational; the
    /// default is a generic marker for backends that do not override it.
    fn backend_type(&self) -> &'static str {
        "deep-storage"
    }
}

// ---------------------------------------------------------------------------
// LocalDeepStorage
// ---------------------------------------------------------------------------

/// Local filesystem deep storage backend.
///
/// Segments are stored as directories under `<base_dir>/<data_source>/<segment_id>/`.
/// Each segment directory contains a single `segment.bin` marker file
/// (real implementations would store the full segment archive).
pub struct LocalDeepStorage {
    base_dir: PathBuf,
}

impl LocalDeepStorage {
    /// Create a new local deep storage rooted at `base_dir`.
    ///
    /// The directory is created if it does not already exist.
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Compute the on-disk path for a segment, REJECTING a `data_source` or
    /// `segment_id` that is not a single safe path component (H9).
    ///
    /// A hostile value such as `../../outside` would otherwise
    /// [`Path::join`] its way OUT of `base_dir` and let deep storage
    /// create / overwrite / delete files anywhere the process can write. See
    /// [`validate_path_component`].
    fn segment_path(&self, data_source: &str, segment_id: &str) -> Result<PathBuf> {
        validate_path_component("data source", data_source)?;
        validate_path_component("segment id", segment_id)?;
        Ok(self.base_dir.join(data_source).join(segment_id))
    }

    /// [`Self::segment_path`] plus the compat-2 R2 H1 symlink gate: the
    /// datasource dir (`<base>/<ds>`) and the segment dir
    /// (`<base>/<ds>/<id>`) must not themselves be symlinks.
    ///
    /// H9 guarantees both components are single safe path components, but
    /// a component that EXISTS as a symlink still escapes the root when
    /// followed: `create_dir_all` / copy / the REPLACE prune / fsync /
    /// download / `remove_dir_all` would then write — and DELETE —
    /// outside the storage root, and a symlink swap makes committed blobs
    /// vanish (brick). Every LocalDeepStorage operation resolves its
    /// segment dir through this gate. `base_dir` ITSELF may legitimately
    /// be a symlink (a data dir moved to another mount); only the
    /// components UNDER it are constrained — legitimate layouts
    /// (compat-3 persist, live publish tails) only ever create REAL
    /// directories here, so the gate rejects nothing but hostile or
    /// corrupted trees.
    async fn verified_segment_path(&self, data_source: &str, segment_id: &str) -> Result<PathBuf> {
        let seg_dir = self.segment_path(data_source, segment_id)?;
        reject_symlink_component(&self.base_dir.join(data_source)).await?;
        reject_symlink_component(&seg_dir).await?;
        Ok(seg_dir)
    }

    /// Canonical containment check (compat-2 R2 H1 defence-in-depth):
    /// after the lstat gate — and, on the upload path, after the
    /// directory chain exists — verify that `dir` RESOLVES to a real path
    /// under the resolved storage root, so a symlink raced in between the
    /// gate and the directory creation is still caught before any bytes
    /// are copied, pruned, downloaded, or deleted. Both sides are
    /// canonicalized, so a `base_dir` that is itself a symlink (legit)
    /// compares correctly.
    async fn verify_resolves_under_base(&self, dir: &Path) -> Result<()> {
        let canonical_base = tokio::fs::canonicalize(&self.base_dir).await?;
        let canonical_dir = tokio::fs::canonicalize(dir).await?;
        if !canonical_dir.starts_with(&canonical_base) {
            return Err(DeepStorageError::Other(format!(
                "segment dir {} resolves to {} — OUTSIDE the storage root {} \
                 (symlink escape rejected)",
                dir.display(),
                canonical_dir.display(),
                canonical_base.display()
            )));
        }
        Ok(())
    }
}

/// Reject a directory-chain component under the storage root
/// (`<base>/<ds>` or `<base>/<ds>/<id>`) that exists but IS a symlink
/// (Codex compat-2 R2 H1 — see
/// [`LocalDeepStorage::verified_segment_path`]). A missing component is
/// fine (it will be created as a REAL directory); an existing
/// non-symlink is fine; an existing symlink is refused fail-loud —
/// operators must use real directories (or bind mounts) below the
/// storage root.
async fn reject_symlink_component(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(meta) if meta.is_symlink() => Err(DeepStorageError::Other(format!(
            "deep-storage path {} is a symlink — refusing to operate through it \
             (storage-root escape rejected; use a real directory or a bind mount)",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Reject a `data_source` / `segment_id` that is not a single, safe path
/// component before it is joined onto a storage root (H9 path-traversal
/// hardening, applied by both [`LocalDeepStorage`] and — as a defensive
/// second layer over `object_store`'s own normalization —
/// [`S3DeepStorage`]).
///
/// A component is rejected when it is empty, `.` / `..`, or contains a path
/// separator (`/` or `\`) or a NUL. Druid datasource names and segment ids
/// use `[A-Za-z0-9._:+-]` and never contain any of these, so this rejects
/// only genuinely hostile / malformed input. `:` is deliberately allowed
/// (it appears in ISO-8601 segment-id timestamps).
fn validate_path_component(kind: &str, component: &str) -> Result<()> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component.contains('/')
        || component.contains('\\')
        || component.contains('\0')
    {
        return Err(DeepStorageError::Other(format!(
            "unsafe {kind} {component:?}: not a single path component \
             (path traversal rejected)"
        )));
    }
    Ok(())
}

/// Validate an UNTRUSTED raw object key so that the byte-for-byte key
/// names EXACTLY the object addressed — or the operation fails loudly
/// (W-C fail-closed hardening).
///
/// `object_store::path::Path::parse` silently NORMALIZES a leading or
/// trailing `/` away, so the distinct key `/wiki/index.zip` would
/// resolve to `wiki/index.zip` — a DIFFERENT object — and an importer
/// could attach the wrong object's bytes under the requesting row's
/// identity (wrong-object substitution). This check runs BEFORE any
/// parse/normalization and refuses every key that does not round-trip
/// verbatim:
///
/// * an empty key, or a leading / trailing `/`;
/// * empty segments (`a//b`);
/// * `.` / `..` segments;
/// * backslashes and ASCII control characters (including NUL).
///
/// [`S3DeepStorage::fetch_object_to_file`] applies it to every key
/// before the GET; the `ferrodruid-migrate` download planner applies
/// the same per-segment rule ([`validate_object_key_segment`]) before
/// mirroring keys onto the local filesystem, so both paths enforce ONE
/// rule.
///
/// This guard covers CALLER-SUPPLIED raw keys (e.g. an untrusted
/// metadata row's `loadSpec`). LISTING-derived fetches do not rely on
/// it: they carry the exact enumerated path ([`ListedObject`], fetched
/// via [`S3DeepStorage::fetch_listed_to_file`]) so no string
/// round-trip exists for normalization to exploit.
///
/// # Errors
///
/// A message naming the offending key and the violated rule.
pub fn validate_object_key(key: &str) -> std::result::Result<(), String> {
    if key.is_empty() {
        return Err("object key is empty — refusing to fetch the prefix root".to_string());
    }
    if key.starts_with('/') || key.ends_with('/') {
        return Err(format!(
            "object key {key:?} carries a leading/trailing `/` — the store would \
             silently normalize it and fetch a DIFFERENT object; refused (the raw \
             key must name the object byte-for-byte)"
        ));
    }
    for segment in key.split('/') {
        validate_object_key_segment(segment).map_err(|e| format!("object key {key:?}: {e}"))?;
    }
    Ok(())
}

/// Per-segment rule of [`validate_object_key`]: reject an empty,
/// `.`/`..`, backslash-carrying, or control-character-carrying key
/// segment (`/` cannot appear post-split). Exposed so callers that
/// split keys themselves — the `ferrodruid-migrate` download planner,
/// which mirrors each component onto the local filesystem — enforce
/// exactly the same rule as the direct fetch path.
///
/// # Errors
///
/// A message naming the offending segment.
pub fn validate_object_key_segment(segment: &str) -> std::result::Result<(), String> {
    if segment.is_empty()
        || segment == "."
        || segment == ".."
        || segment.contains('\\')
        || segment.chars().any(|c| c.is_ascii_control())
    {
        return Err(format!(
            "key segment {segment:?} is empty, relative (`.`/`..`), or carries a \
             backslash/control character — refused (fail-closed)"
        ));
    }
    Ok(())
}

/// fsync a just-written file so its bytes reach stable storage (H7). Opening
/// the path read-only and `sync_all`-ing flushes both data and metadata.
async fn fsync_file(path: &Path) -> Result<()> {
    let file = tokio::fs::File::open(path).await?;
    file.sync_all().await?;
    Ok(())
}

/// fsync a directory so its newly-created entries survive a crash (H7, C3).
///
/// On Unix the directory is opened read-only and `fsync`'d, and BOTH the open
/// and the sync are AUTHORITATIVE: a failure is PROPAGATED, never swallowed.
/// A directory whose entry list did not reach stable storage means the segment
/// just "uploaded" beneath it is not durable — reporting the upload as
/// successful (letting a metadata row be committed over a blob a power loss
/// could orphan) would violate the "committed ⇒ durable" invariant, so
/// [`LocalDeepStorage::upload_segment`] must instead fail (C3).
#[cfg(unix)]
async fn fsync_dir(path: &Path) -> Result<()> {
    let dir = tokio::fs::File::open(path).await?;
    dir.sync_all().await?;
    Ok(())
}

/// Non-Unix fallback: a directory handle cannot be `fsync`'d, so this is a
/// DOCUMENTED durability degrade — the file bytes themselves were already
/// fsync'd by [`fsync_file`]. FerroDruid's deploy target is Unix, where the
/// `#[cfg(unix)]` [`fsync_dir`] above is authoritative (C3).
#[cfg(not(unix))]
async fn fsync_dir(path: &Path) -> Result<()> {
    tracing::debug!(
        path = %path.display(),
        "directory fsync unavailable on this platform — durability degraded (non-Unix)"
    );
    Ok(())
}

/// The directory chain whose entries must be fsync'd after writing a segment
/// into `dest_dir` (`<root>/<data_source>/<segment_id>`), ordered from the
/// segment dir UP to AND INCLUDING the storage root (H7, C3). Pure so the rule
/// is unit-testable.
///
/// Includes the segment dir (its new file entries), its datasource dir (the
/// new segment-dir entry), AND the storage root — ALWAYS, never gated on
/// whether the datasource dir looked like it "already existed" (Codex R19 H1):
/// [`upload_segment`] is retried ([`UPLOAD_ATTEMPTS`]), and a FIRST attempt can
/// create the datasource directory and then fail before this chain fsyncs the
/// root. A later attempt would observe the datasource dir already present and,
/// under the old `ds_existed` optimization, SKIP the root fsync — leaving the
/// datasource's dentry in the root non-durable even though the retry returned
/// `Ok`, so a power loss after the metadata + Kafka-offset commit could erase
/// the whole datasource dir and strand every "committed" segment beneath it
/// (loss). `upload_segment`'s contract is "on `Ok` the segment is durable",
/// and durability of the segment includes the durability of the dentry chain
/// that names it; the caller cannot reason about a prior failed attempt, so
/// the root fsync must be unconditional. A re-fsync of an already-durable root
/// is cheap at flush cadence and matches the belt-and-braces re-fsync
/// [`durable_create_fsync_chain`] already performs for a pre-existing path
/// (an earlier NON-durable creator may never have made the dentry durable,
/// Codex R8 H3 — the same failure class).
fn fsync_chain(dest_dir: &Path) -> Vec<PathBuf> {
    let mut chain = vec![dest_dir.to_path_buf()];
    if let Some(ds_dir) = dest_dir.parent() {
        chain.push(ds_dir.to_path_buf());
        if let Some(root) = ds_dir.parent() {
            chain.push(root.to_path_buf());
        }
    }
    chain
}

/// The directory chain [`create_dir_all_durable`] must fsync after creating
/// `path`, given which ancestors PRE-EXISTED the creation (Codex R7 H2).
/// Pure so the rule is unit-testable without touching a filesystem.
///
/// * `path` did NOT pre-exist: every newly created directory from `path`
///   upward, PLUS the deepest pre-existing ancestor — the directory whose
///   entry list gained the topmost new dir. Without that last fsync the
///   whole created subtree hangs off a non-durable dentry: a power loss can
///   drop it even though everything *inside* it was fsynced (the exact H2
///   failure — `data_dir/deep-storage` created at startup, segments uploaded
///   and fsynced beneath it, metadata committed, then the `deep-storage`
///   dentry in `data_dir` vanishes and every committed blob is orphaned).
/// * `path` DID pre-exist: belt-and-braces re-fsync of `path`'s ancestor
///   chain — an earlier NON-durable creator (e.g. a pre-H2 build's plain
///   `create_dir_all`) may never have made these dentries durable. For an
///   ABSOLUTE `path` the immediate parent (which holds `path`'s dentry)
///   suffices — deeper ancestors are OS/admin-managed and trusted, the same
///   boundary the create walk stops at. For a RELATIVE `path` the WHOLE
///   subtree down from the CWD could be non-durable, so the walk continues up
///   to and including `.` (R21 H1: re-fsyncing only the immediate parent left
///   a relative data root's own dentry — which lives in the CWD — hanging off
///   a non-durable `.` entry, the pre-existing analogue of the R8 H3 create
///   hole; a power loss could then drop the entire relative data root after
///   its blobs + metadata + Kafka offsets committed).
///
/// A RELATIVE `path` (`--data-dir data`) reaches a topmost component whose
/// `Path::parent` is the EMPTY path — but that dir's dentry lives in the
/// current working directory, which gained an entry all the same. The chain
/// names it as `.` so it is fsynced too (Codex R8 H3: filtering the empty
/// parent out silently dropped the CWD fsync, leaving the entire relative
/// data tree hanging off a non-durable dentry).
fn durable_create_fsync_chain(path: &Path, preexisted: impl Fn(&Path) -> bool) -> Vec<PathBuf> {
    let mut chain = vec![path.to_path_buf()];
    if preexisted(path) {
        // Belt-and-braces re-fsync up `path`'s ancestor chain (R21 H1). For a
        // relative tree the walk reaches `.` (the CWD holds the relative
        // root's dentry); for an absolute path it stops at the immediate
        // parent (deeper ancestors are the trusted OS/admin boundary).
        let mut cur = path.parent();
        while let Some(dir) = cur {
            if dir.as_os_str().is_empty() {
                // Bare relative root reached: its dentry lives in the CWD.
                chain.push(PathBuf::from("."));
                break;
            }
            chain.push(dir.to_path_buf());
            if dir.is_absolute() {
                // The immediate absolute parent holds `path`'s dentry; deeper
                // absolute ancestors are trusted (matches the create walk).
                break;
            }
            cur = dir.parent();
        }
        return chain;
    }
    let mut cur = path.parent();
    while let Some(dir) = cur {
        if dir.as_os_str().is_empty() {
            // The topmost NEW dir is a bare relative component: the
            // directory that gained its dentry is the CWD, which the empty
            // `Path::parent` cannot name — fsync `.` explicitly and stop
            // (the CWD necessarily pre-existed the creation; Codex R8 H3).
            chain.push(PathBuf::from("."));
            break;
        }
        chain.push(dir.to_path_buf());
        if preexisted(dir) {
            break;
        }
        cur = dir.parent();
    }
    chain
}

/// Durable `create_dir_all` (Codex R7 H2): create `path` (and any missing
/// ancestors) and fsync every directory whose entry list this creation
/// changed — each newly created directory AND the deepest pre-existing
/// ancestor, whose dentry list gained the topmost new dir (see
/// [`durable_create_fsync_chain`]).
///
/// A plain `create_dir_all` only fills the page cache with the new dentries:
/// the startup deep-storage root (`data_dir/deep-storage`) created that way
/// hangs off a non-durable `data_dir` entry, so a power loss AFTER segments
/// were persisted + fsynced beneath it and their metadata rows committed
/// could drop the whole `deep-storage/` dentry — every committed blob
/// orphaned, the bootstrap reload fails, the committed⇒durable invariant
/// broken from the OUTSIDE of the upload path's own [`fsync_chain`] (which
/// deliberately stops at the storage root and assumes the root itself is
/// durable).
///
/// Returns the directories that were fsynced, so callers/tests can pin that
/// the parent of a created root became durable. Fsync failures PROPAGATE
/// (same C3 discipline as [`fsync_dir`]): a root whose dentry did not reach
/// stable storage must not be reported as durably created.
///
/// # Errors
///
/// Any `create_dir_all` failure (e.g. a file where a directory is needed),
/// an existence-probe I/O error, or a failed open/fsync of a chain
/// directory.
pub async fn create_dir_all_durable(path: &Path) -> Result<Vec<PathBuf>> {
    // Find the deepest PRE-EXISTING ancestor BEFORE creating (the same
    // observe-then-create order as `upload_segment`'s `ds_existed`). A
    // concurrent creator racing us can only make this OVER-fsync (harmless).
    let mut boundary: Option<PathBuf> = None;
    let mut probe = Some(path);
    while let Some(dir) = probe.filter(|p| !p.as_os_str().is_empty()) {
        if tokio::fs::try_exists(dir).await? {
            boundary = Some(dir.to_path_buf());
            break;
        }
        probe = dir.parent();
    }
    // Everything between the boundary and `path` is missing. A dir
    // pre-existed iff it is the boundary or one of the boundary's ancestors.
    let preexisted = |p: &Path| {
        boundary
            .as_deref()
            .is_some_and(|b| p == b || b.starts_with(p))
    };

    tokio::fs::create_dir_all(path).await?;
    let chain = durable_create_fsync_chain(path, preexisted);
    for dir in &chain {
        fsync_dir(dir).await?;
    }
    Ok(chain)
}

/// Compute a deterministic SHA-256 content hash over every regular file in a
/// segment blob directory `dir`, returned as a lowercase-hex string.
///
/// This is the content-identity witness recorded when a segment blob is
/// committed to deep storage (the publish tails in `ferrodruid-overlord` and
/// `ferrodruid-migrate attach` stamp it into the metadata row's
/// `payload.loadSpec.sha256`) and re-checked at bootstrap reload (H2): a
/// durable blob that is swapped for a *different* valid v9 artifact — or
/// silently corrupted in a way that still decodes — changes this hash and is
/// rejected fail-loud instead of silently serving different data than was
/// committed.
///
/// Determinism (an identical value on both the commit and the reload side):
///   * files are enumerated then **sorted by file name**, so the on-disk
///     directory iteration order does not matter;
///   * each file contributes a length-framed `(name, bytes)` record
///     (`name_len ‖ name ‖ content_len ‖ content`, each length an 8-byte
///     little-endian prefix), so no re-partitioning of the byte stream across
///     a different file set can collide;
///   * only regular files are hashed — exactly the set
///     [`DeepStorage::upload_segment`] copies to (and
///     [`DeepStorage::download_segment`] copies back from) deep storage, so
///     the local staging dir and the re-downloaded dir hash identically.
///
/// File contents are **streamed** through the hasher in fixed-size chunks
/// (H6): a multi-GB smoosh chunk is never materialized in memory in one
/// allocation, and the digest is byte-identical to a whole-file read (the
/// `content_len` frame is taken from the open handle's metadata and verified
/// against the streamed byte count). The blob dir must be quiescent while it
/// is hashed — a file that changes size mid-hash fails loudly instead of
/// framing a length that does not match the hashed bytes.
///
/// # Errors
///
/// Fails if `dir` cannot be read, a contained file's bytes cannot be loaded,
/// or a file changes size while it is being hashed.
pub fn blob_content_hash(dir: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read as _;

    /// Fixed streaming-read chunk size (H6).
    const HASH_BUF_BYTES: usize = 64 * 1024;

    let mut files: Vec<(Vec<u8>, PathBuf)> = Vec::new();
    let read_dir = std::fs::read_dir(dir).map_err(|e| {
        DeepStorageError::Other(format!("hash: read blob dir {}: {e}", dir.display()))
    })?;
    for entry in read_dir {
        let entry = entry.map_err(|e| {
            DeepStorageError::Other(format!("hash: read entry in {}: {e}", dir.display()))
        })?;
        let file_type = entry
            .file_type()
            .map_err(|e| DeepStorageError::Other(format!("hash: file type: {e}")))?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        files.push((name.as_encoded_bytes().to_vec(), entry.path()));
    }
    // Sort by file name so the hash is independent of enumeration order.
    files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; HASH_BUF_BYTES];
    for (name, path) in &files {
        let mut file = std::fs::File::open(path)
            .map_err(|e| DeepStorageError::Other(format!("hash: open {}: {e}", path.display())))?;
        // The length frame must precede the content, so it comes from the
        // OPEN handle's metadata and is verified against the streamed byte
        // count below — for the quiescent dirs this hash is defined over,
        // the two always agree and the digest matches the historical
        // whole-file-read framing exactly.
        let framed_len = file
            .metadata()
            .map_err(|e| DeepStorageError::Other(format!("hash: stat {}: {e}", path.display())))?
            .len();
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name);
        hasher.update(framed_len.to_le_bytes());
        let mut streamed: u64 = 0;
        loop {
            let n = match file.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    return Err(DeepStorageError::Other(format!(
                        "hash: read {}: {e}",
                        path.display()
                    )));
                }
            };
            streamed = streamed.saturating_add(n as u64);
            if streamed > framed_len {
                return Err(DeepStorageError::Other(format!(
                    "hash: {} grew past its framed length ({framed_len} bytes) while being \
                     hashed — the blob dir must be quiescent during hashing",
                    path.display()
                )));
            }
            hasher.update(&buf[..n]);
        }
        if streamed != framed_len {
            return Err(DeepStorageError::Other(format!(
                "hash: {} changed size while being hashed (streamed {streamed} of the framed \
                 {framed_len} bytes) — the blob dir must be quiescent during hashing",
                path.display()
            )));
        }
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Clear a pre-existing DEST entry that a plain `tokio::fs::copy` could
/// either write THROUGH (a symlink — the copy would follow it and
/// overwrite the symlink's target, potentially OUTSIDE the storage root:
/// a write escape, Codex compat-2 H2) or trip over (a subdirectory /
/// special file). A pre-existing REGULAR file is left in place — the
/// copy truncates and rewrites it, which is the normal `--force` /
/// retry overwrite path. Removal targets the entry ITSELF
/// (`symlink_metadata`, `remove_file` on the link), never its target.
async fn clear_non_regular_dest_entry(dest_file: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(dest_file).await {
        Ok(meta) => {
            if meta.is_file() {
                return Ok(());
            }
            if meta.is_dir() {
                tokio::fs::remove_dir_all(dest_file).await?;
            } else {
                tokio::fs::remove_file(dest_file).await?;
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[async_trait]
impl DeepStorage for LocalDeepStorage {
    async fn list_segments(&self, data_source: &str) -> Result<Vec<String>> {
        validate_path_component("data source", data_source)?;
        let ds_dir = self.base_dir.join(data_source);
        // Never enumerate THROUGH a symlinked datasource dir — external
        // directories would be reported as segments (compat-2 R2 H1).
        reject_symlink_component(&ds_dir).await?;
        if !ds_dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries = tokio::fs::read_dir(&ds_dir).await?;
        let mut segments = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                segments.push(name.to_string());
            }
        }

        segments.sort();
        Ok(segments)
    }

    async fn download_segment(
        &self,
        data_source: &str,
        segment_id: &str,
        dest: &Path,
    ) -> Result<()> {
        let src_dir = self.verified_segment_path(data_source, segment_id).await?;
        if !src_dir.exists() {
            return Err(DeepStorageError::NotFound {
                data_source: data_source.to_string(),
                segment_id: segment_id.to_string(),
            });
        }
        // Defence-in-depth (compat-2 R2 H1): the segment dir must RESOLVE
        // under the storage root before any external bytes could be
        // materialized as segment content.
        self.verify_resolves_under_base(&src_dir).await?;

        // Copy the segment directory contents to dest.
        tokio::fs::create_dir_all(dest).await?;
        let mut entries = tokio::fs::read_dir(&src_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let dest_file = dest.join(entry.file_name());
            tokio::fs::copy(entry.path(), dest_file).await?;
        }

        Ok(())
    }

    async fn upload_segment(&self, data_source: &str, segment_id: &str, src: &Path) -> Result<()> {
        // Symlink gate (compat-2 R2 H1): a symlinked `<base>/<ds>` or
        // `<base>/<ds>/<id>` would route the create/copy/prune/fsync
        // below OUTSIDE the storage root.
        let dest_dir = self.verified_segment_path(data_source, segment_id).await?;
        tokio::fs::create_dir_all(&dest_dir).await?;
        // Defence-in-depth: after the chain exists, the segment dir must
        // RESOLVE under the resolved storage root — a symlink raced in
        // between the gate and the mkdir is caught here, BEFORE any bytes
        // are written or pruned (compat-2 R2 H1).
        self.verify_resolves_under_base(&dest_dir).await?;

        // The exact set of file names THIS upload writes — the prune below
        // makes the destination equal to it (REPLACE semantics, R22 H1).
        let mut written: std::collections::HashSet<std::ffi::OsString> =
            std::collections::HashSet::new();

        if src.is_dir() {
            // Copy every regular file from src (segment dirs are flat v9 files;
            // subdirs are skipped, matching `S3DeepStorage::upload_segment`).
            let mut entries = tokio::fs::read_dir(src).await?;
            while let Some(entry) = entries.next_entry().await? {
                if !entry.file_type().await?.is_file() {
                    continue;
                }
                let name = entry.file_name();
                let dest_file = dest_dir.join(&name);
                // Never write THROUGH a pre-existing symlink (or into a
                // same-named subdir) left by a crash orphan or a hostile
                // actor — replace the entry itself (Codex compat-2 H2).
                clear_non_regular_dest_entry(&dest_file).await?;
                tokio::fs::copy(entry.path(), &dest_file).await?;
                // Durability (H7): the "a metadata row is only committed AFTER
                // a durable upload" invariant requires the bytes to actually
                // reach stable storage — a plain copy only fills the page
                // cache, so a power loss could leave a metadata-referenced blob
                // missing or truncated.
                fsync_file(&dest_file).await?;
                written.insert(name);
            }
        } else {
            // Single file — copy it as segment.bin, then fsync (H7).
            let dest_file = dest_dir.join("segment.bin");
            clear_non_regular_dest_entry(&dest_file).await?;
            tokio::fs::copy(src, &dest_file).await?;
            fsync_file(&dest_file).await?;
            written.insert(std::ffi::OsString::from("segment.bin"));
        }

        // REPLACE semantics (Codex R22 H1): prune EVERY entry in the
        // destination that THIS upload did not write — regular files,
        // symlinks, subdirectories, and special files alike (Codex compat-2
        // H2: a surviving symlink/subdir makes the dest differ from the
        // uploaded set, so the staging-computed sha256 no longer matches a
        // re-hash of the dest and the bootstrap reload bricks). A crash
        // after an upload but before the metadata commit leaves an ORPHAN
        // blob that `allocate_segment_id` cannot see (it consults only
        // metadata + historical), so the id can be reused for a SMALLER
        // artifact; with merge semantics the orphan's surplus files survived
        // alongside the new upload, dest != src, and the staging-computed
        // content hash (R17 H2) failed against the reloaded dest → bootstrap
        // abort. The orphan is by definition UNCOMMITTED (a committed id is
        // rejected by the collision check), so deleting its leftovers is
        // safe. Runs after the copies and BEFORE `fsync_chain` so the
        // removed dentries are covered by the directory fsyncs below; a
        // crash mid-prune merely leaves another uncommitted orphan for the
        // next upload to replace. `DirEntry::file_type` has lstat semantics,
        // so a symlink is removed as the LINK entry, never via its target.
        let mut dest_entries = tokio::fs::read_dir(&dest_dir).await?;
        while let Some(entry) = dest_entries.next_entry().await? {
            if written.contains(&entry.file_name()) {
                continue;
            }
            if entry.file_type().await?.is_dir() {
                tokio::fs::remove_dir_all(entry.path()).await?;
            } else {
                tokio::fs::remove_file(entry.path()).await?;
            }
        }

        // fsync every directory whose entries this upload changed, from the
        // segment dir up to AND INCLUDING the storage root (H7, C3, R19 H1).
        // Each `?` PROPAGATES a fsync failure so a non-durable upload is
        // reported as an error rather than committed over — a fsync'd file
        // whose directory entry is not fsync'd can still vanish, and a
        // datasource dir whose entry in the storage root is not fsync'd can
        // take every segment with it. The root fsync is UNCONDITIONAL: a prior
        // failed retry may have created the datasource dir non-durably, so
        // observing it "already present" this attempt is no proof its dentry
        // reached stable storage (R19 H1).
        for dir in fsync_chain(&dest_dir) {
            fsync_dir(&dir).await?;
        }

        Ok(())
    }

    async fn delete_segment(&self, data_source: &str, segment_id: &str) -> Result<()> {
        // Symlink gate (compat-2 R2 H1): `remove_dir_all`'s own symlink
        // defence guards the FINAL component only — a real dir reached
        // through a symlinked ANCESTOR (`<base>/<ds>` → outside) would be
        // recursively deleted OUTSIDE the storage root.
        let seg_dir = self.verified_segment_path(data_source, segment_id).await?;
        if !seg_dir.exists() {
            // Idempotent: nothing to delete is success, not error.
            return Ok(());
        }
        // Defence-in-depth: never delete a dir that does not RESOLVE
        // under the storage root (compat-2 R2 H1).
        self.verify_resolves_under_base(&seg_dir).await?;
        tokio::fs::remove_dir_all(&seg_dir).await?;
        Ok(())
    }

    async fn segment_exists(&self, data_source: &str, segment_id: &str) -> Result<bool> {
        // Symlink gate (compat-2 R2 H1): never report an EXTERNAL dir
        // reached through a symlinked component as an existing segment.
        let seg_dir = self.verified_segment_path(data_source, segment_id).await?;
        Ok(seg_dir.exists())
    }

    fn backend_type(&self) -> &'static str {
        "local"
    }
}

// ---------------------------------------------------------------------------
// S3DeepStorage
// ---------------------------------------------------------------------------

/// One object enumerated by [`S3DeepStorage::list_objects`]: the EXACT
/// [`object_store`] path the listing produced, plus its
/// store-prefix-relative key string for planning/display/local
/// mirroring.
///
/// The path is PRIVATE and only ever produced by the listing, and
/// [`S3DeepStorage::fetch_listed_to_file`] GETs that very path — so
/// between enumeration and fetch there is NO String→parse round-trip in
/// which `object_store`'s silent normalization (its parser strips a
/// leading/trailing `/`) could re-address the request to a DIFFERENT
/// object (W-C wrong-object-substitution hardening — the
/// listing-derived sibling of the direct-GET guard
/// [`validate_object_key`]).
///
/// Honest residual: an S3 object whose RAW key carries a
/// leading/trailing `/` is not addressable through `object_store` at
/// all (the parser strips the slash before any request). Such an object
/// can never be fetched by this pipeline — it is SKIPPED, never
/// mis-fetched: its normalized alias in a listing either collides with
/// a real sibling (loud [`S3DeepStorage::list_objects`] refusal) or
/// fails the fetch loudly. Druid deep-storage keys never carry
/// leading/trailing slashes, so this only surfaces on pathological
/// buckets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    /// The exact enumerated path — fetched verbatim, never re-parsed.
    path: ObjectPath,
    /// The key RELATIVE to the store's configured prefix.
    key: String,
}

impl ListedObject {
    /// The listed key, RELATIVE to the store's configured prefix
    /// (planning/display; the fetch itself uses the exact enumerated
    /// path, not this string).
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Cap on the number of objects one [`S3DeepStorage::list_objects`]
/// call may enumerate (1M — the same resource-bound discipline as
/// `ferrodruid-migrate`'s local scan caps, whose planner shares this
/// very constant).
///
/// The cap is enforced WHILE the paginated listing stream is consumed:
/// the moment entry `cap + 1` arrives, the stream is dropped and the
/// call fails loudly — so a hostile or accidentally-broad prefix over a
/// bucket with tens of millions of objects bounds memory by THIS
/// constant, never by the bucket size (W-C memory-exhaustion
/// hardening; the old shape collected the whole listing first and only
/// then let the planner count it).
pub const MAX_S3_KEYS: usize = 1_000_000;

/// S3-backed deep storage using the [`object_store`] crate.
///
/// Segments are stored as objects under `<prefix><data_source>/<segment_id>/<filename>`.
/// Each file inside a segment's local directory becomes a separate object.
pub struct S3DeepStorage {
    store: Box<dyn ObjectStore>,
    prefix: String,
}

impl S3DeepStorage {
    /// Create from explicit bucket and region configuration.
    ///
    /// AWS credentials are resolved from the standard SDK chain
    /// (environment variables, instance profile, etc.).
    pub fn new(bucket: &str, region: &str, prefix: &str) -> Result<Self> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .build()
            .map_err(|e| DeepStorageError::Other(e.to_string()))?;
        Ok(Self {
            store: Box::new(store),
            prefix: prefix.to_string(),
        })
    }

    /// Create from environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
    /// `AWS_DEFAULT_REGION`, etc.).
    pub fn from_env(bucket: &str, prefix: &str) -> Result<Self> {
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| DeepStorageError::Other(e.to_string()))?;
        Ok(Self {
            store: Box::new(store),
            prefix: prefix.to_string(),
        })
    }

    /// Create with a custom [`ObjectStore`] backend (e.g. [`InMemory`] for testing).
    pub fn with_store(store: Box<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    /// Like [`S3DeepStorage::from_env`], with a caller-bounded retry
    /// policy instead of `object_store`'s default (which retries an
    /// unreachable endpoint for minutes).
    ///
    /// Used by offline tools (`ferrodruid-migrate`) where a wrong
    /// endpoint / bucket must fail in seconds with a loud reason rather
    /// than hang the run. `AWS_ENDPOINT` and `AWS_ALLOW_HTTP` are
    /// honored from the environment (S3-compatible stores like MinIO).
    ///
    /// # Errors
    ///
    /// Fails if the underlying S3 builder rejects the configuration.
    pub fn from_env_with_retry(
        bucket: &str,
        prefix: &str,
        max_retries: usize,
        retry_timeout: std::time::Duration,
    ) -> Result<Self> {
        let retry = object_store::RetryConfig {
            max_retries,
            retry_timeout,
            ..object_store::RetryConfig::default()
        };
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_retry(retry)
            .build()
            .map_err(|e| DeepStorageError::Other(e.to_string()))?;
        Ok(Self {
            store: Box::new(store),
            prefix: prefix.to_string(),
        })
    }

    /// Fetch ONE raw object — `key` resolved under this store's
    /// configured prefix — into the local file `dest`, STREAMING the
    /// body chunk-by-chunk (the whole object is never materialized in
    /// RAM in one allocation). Parent directories of `dest` are
    /// created. Returns the number of bytes written.
    ///
    /// `max_bytes` caps the download: a body that exceeds it aborts the
    /// fetch and removes the partial file (disk-fill guard for keys
    /// that come from untrusted input, e.g. a foreign metadata DB's
    /// `loadSpec`). Any failure after the destination file was created
    /// removes the partial file too — an `Err` never leaves bytes
    /// behind.
    ///
    /// The write is flushed but NOT fsync'd: this is a staging
    /// download, not a durability commit — durable persistence happens
    /// when the artifact is uploaded through
    /// [`DeepStorage::upload_segment`].
    ///
    /// # Errors
    ///
    /// A malformed key (empty, leading/trailing `/`, empty segment,
    /// `.`/`..`, backslash, control characters — see
    /// [`validate_object_key`], enforced on the RAW key BEFORE any
    /// parse/normalization and before the GET, so the byte-for-byte key
    /// names exactly the object fetched or nothing is fetched), a
    /// missing object, an object over `max_bytes`, or any backend/local
    /// I/O failure.
    pub async fn fetch_object_to_file(
        &self,
        key: &str,
        dest: &Path,
        max_bytes: Option<u64>,
    ) -> Result<u64> {
        // W-C H2: `ObjectPath::parse` below silently strips a leading/
        // trailing `/`, which would substitute a DIFFERENT object for
        // the requested key — validate the raw key first, fail-closed.
        validate_object_key(key).map_err(DeepStorageError::Other)?;
        let full = format!("{}{}", self.prefix, key);
        let obj_path = ObjectPath::parse(&full)
            .map_err(|e| DeepStorageError::Other(format!("invalid object key {full:?}: {e}")))?;
        // GET before creating anything locally, so a missing object
        // leaves no file behind.
        let result = self
            .store
            .get(&obj_path)
            .await
            .map_err(|e| DeepStorageError::Other(format!("get {full}: {e}")))?;

        let res = stream_get_result_to_file(result, dest, &full, max_bytes).await;
        if res.is_err() {
            // Never leave a partial download behind on failure.
            let _ = tokio::fs::remove_file(dest).await;
        }
        res
    }

    /// List every object under `key_prefix` — resolved under this
    /// store's configured prefix — as [`ListedObject`]s (the exact
    /// enumerated path + the prefix-relative key), sorted by key.
    /// Prefix matching is per whole path segment (`a` matches `a/x`,
    /// never `ab/x`). The listing streams through the backend's native
    /// pagination (S3 `ListObjectsV2` continuation tokens), so no
    /// manual paging — but NEVER more than `max_keys` objects are
    /// accumulated: the moment entry `max_keys + 1` arrives, the
    /// stream is dropped and the call fails loudly naming the cap, so
    /// memory is bounded by `max_keys`, not by the bucket size
    /// (callers inside FerroDruid pass [`MAX_S3_KEYS`]). An empty
    /// result is `Ok(vec![])`, not an error.
    ///
    /// Fetch listed objects with
    /// [`S3DeepStorage::fetch_listed_to_file`], which GETs the very
    /// path enumerated here (see [`ListedObject`] for the
    /// wrong-object-substitution rationale).
    ///
    /// # Errors
    ///
    /// * a malformed prefix, or a backend listing failure;
    /// * a listing exceeding `max_keys` objects (aborted mid-stream at
    ///   entry `max_keys + 1` — memory-exhaustion guard; narrow the
    ///   prefix);
    /// * a listed location that does not sit under this store's
    ///   configured prefix (inconsistent backend — the old string
    ///   fallback would have let a later fetch re-prefix it into a
    ///   DIFFERENT address, fail-closed instead);
    /// * two listed entries whose paths collide (only possible when
    ///   raw keys differing in a leading/trailing `/` were normalized
    ///   onto ONE address by `object_store`'s parser — fetching that
    ///   address could silently substitute one object for the other,
    ///   refused).
    pub async fn list_objects(
        &self,
        key_prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<ListedObject>> {
        let full = format!("{}{}", self.prefix, key_prefix);
        let parsed;
        let list_prefix: Option<&ObjectPath> = if full.is_empty() {
            None
        } else {
            parsed = ObjectPath::parse(&full).map_err(|e| {
                DeepStorageError::Other(format!("invalid key prefix {full:?}: {e}"))
            })?;
            Some(&parsed)
        };
        // Stream the paginated listing, enforcing `max_keys` PER ENTRY
        // as it arrives — the whole listing is never collected first,
        // so an over-cap bucket costs at most cap+1 entries of memory
        // before the loud abort below (W-C memory-exhaustion guard).
        let mut stream = self.store.list(list_prefix);
        let mut listed: Vec<ListedObject> = Vec::new();
        while let Some(meta) = stream
            .try_next()
            .await
            .map_err(|e| DeepStorageError::Other(format!("list {full}: {e}")))?
        {
            if listed.len() >= max_keys {
                return Err(DeepStorageError::Other(format!(
                    "listing under {full:?} exceeds the {max_keys}-key cap — aborting \
                     the listing mid-stream so memory stays bounded by the cap, not \
                     the bucket size; narrow the prefix"
                )));
            }
            let raw = meta.location.as_ref();
            let key = if self.prefix.is_empty() {
                raw.to_string()
            } else {
                match raw.strip_prefix(self.prefix.as_str()) {
                    Some(rel) => rel.to_string(),
                    None => {
                        return Err(DeepStorageError::Other(format!(
                            "listing returned object {raw:?} OUTSIDE the configured store \
                             prefix {:?} — refusing the inconsistent listing (a re-prefixed \
                             fetch would address a DIFFERENT object)",
                            self.prefix
                        )));
                    }
                }
            };
            listed.push(ListedObject {
                path: meta.location,
                key,
            });
        }
        listed.sort_by(|a, b| a.key.cmp(&b.key));
        // Two raw keys can only collapse onto ONE enumerated path when
        // they differ by a leading/trailing `/` that object_store's
        // parser silently stripped at listing time. Any fetch of that
        // path returns exactly one of the raw objects, so the other
        // entry would silently receive a DIFFERENT object's bytes:
        // fail-closed (H2 sibling, listing-derived).
        for pair in listed.windows(2) {
            if pair[0].path == pair[1].path {
                return Err(DeepStorageError::Other(format!(
                    "listing under {full:?} enumerated the object path {:?} TWICE — two \
                     distinct raw S3 keys (differing by a leading/trailing `/`) were \
                     normalized onto one address by object_store; fetching it could \
                     silently substitute one object for the other, refused (remove or \
                     rename the slash-carrying raw key)",
                    pair[0].path
                )));
            }
        }
        Ok(listed)
    }

    /// Fetch EXACTLY the object `listed` enumerates — the GET uses the
    /// very [`object_store`] path produced by
    /// [`S3DeepStorage::list_objects`], with no String round-trip and
    /// no re-parse in between — into the local file `dest`, streaming
    /// chunk-by-chunk with the same `max_bytes` cap and
    /// partial-file-cleanup contract as
    /// [`S3DeepStorage::fetch_object_to_file`].
    ///
    /// Because enumeration and fetch share one address, the staged
    /// bytes are the enumerated object's or the call fails loudly —
    /// a wrong-object substitution is impossible by construction (see
    /// [`ListedObject`] for the residual: raw keys `object_store`
    /// cannot address are skipped, never mis-fetched).
    ///
    /// # Errors
    ///
    /// A listed path that no longer resolves (including the alias of a
    /// raw leading/trailing-`/` key, which is not addressable via
    /// `object_store` — named in the error), an object over
    /// `max_bytes`, or any backend/local I/O failure. On any failure
    /// the partial file is removed.
    pub async fn fetch_listed_to_file(
        &self,
        listed: &ListedObject,
        dest: &Path,
        max_bytes: Option<u64>,
    ) -> Result<u64> {
        let result = match self.store.get(&listed.path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { path, source }) => {
                return Err(DeepStorageError::Other(format!(
                    "get {path}: the LISTED object does not resolve ({source}) — if the \
                     bucket holds a raw key differing only by a leading/trailing `/`, \
                     that object is not addressable via object_store and is skipped \
                     loudly here, never silently substituted"
                )));
            }
            Err(e) => {
                return Err(DeepStorageError::Other(format!("get {}: {e}", listed.path)));
            }
        };
        let full = listed.path.to_string();
        let res = stream_get_result_to_file(result, dest, &full, max_bytes).await;
        if res.is_err() {
            // Never leave a partial download behind on failure.
            let _ = tokio::fs::remove_file(dest).await;
        }
        res
    }

    /// Build the object path prefix for a specific data source.
    fn ds_prefix(&self, data_source: &str) -> ObjectPath {
        ObjectPath::from(format!("{}{}/", self.prefix, data_source))
    }

    /// Build the object path prefix for a specific segment.
    fn segment_prefix(&self, data_source: &str, segment_id: &str) -> ObjectPath {
        ObjectPath::from(format!("{}{}/{}/", self.prefix, data_source, segment_id))
    }

    /// Build the full object path for a file inside a segment.
    fn file_path(&self, data_source: &str, segment_id: &str, filename: &str) -> ObjectPath {
        ObjectPath::from(format!(
            "{}{}/{}/{}",
            self.prefix, data_source, segment_id, filename
        ))
    }
}

/// Stream a `GET` body into `dest` chunk-by-chunk, enforcing
/// `max_bytes` mid-stream (see [`S3DeepStorage::fetch_object_to_file`]
/// — which also owns the cleanup-on-error of the partial file).
async fn stream_get_result_to_file(
    result: object_store::GetResult,
    dest: &Path,
    full_key: &str,
    max_bytes: Option<u64>,
) -> Result<u64> {
    use tokio::io::AsyncWriteExt as _;

    if let Some(parent) = dest.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::File::create(dest).await?;
    let mut stream = result.into_stream();
    let mut written: u64 = 0;
    loop {
        let chunk = match stream.try_next().await {
            Ok(Some(c)) => c,
            Ok(None) => break,
            Err(e) => {
                return Err(DeepStorageError::Other(format!("read {full_key}: {e}")));
            }
        };
        written = written.saturating_add(chunk.len() as u64);
        if let Some(cap) = max_bytes
            && written > cap
        {
            return Err(DeepStorageError::Other(format!(
                "object {full_key} exceeds the {cap}-byte fetch cap — aborting the \
                 download (disk-fill guard)"
            )));
        }
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(written)
}

#[async_trait]
impl DeepStorage for S3DeepStorage {
    async fn list_segments(&self, data_source: &str) -> Result<Vec<String>> {
        validate_path_component("data source", data_source)?;
        let prefix = self.ds_prefix(data_source);
        let listing = self
            .store
            .list_with_delimiter(Some(&prefix))
            .await
            .map_err(|e| DeepStorageError::Other(e.to_string()))?;

        let mut segments: Vec<String> = listing
            .common_prefixes
            .into_iter()
            .filter_map(|p| {
                let s = p.as_ref();
                // Strip trailing slash and extract last component.
                let trimmed = s.trim_end_matches('/');
                trimmed.rsplit('/').next().map(String::from)
            })
            .collect();

        // Also check objects directly — some stores may not return common_prefixes
        // for flat listings. Extract unique segment IDs from object keys.
        for meta in &listing.objects {
            let key = meta.location.as_ref();
            let suffix = key.strip_prefix(prefix.as_ref()).unwrap_or(key);
            let suffix = suffix.trim_start_matches('/');
            if let Some(seg_id) = suffix.split('/').next()
                && !seg_id.is_empty()
                && !segments.contains(&seg_id.to_string())
            {
                segments.push(seg_id.to_string());
            }
        }

        segments.sort();
        Ok(segments)
    }

    async fn download_segment(
        &self,
        data_source: &str,
        segment_id: &str,
        dest: &Path,
    ) -> Result<()> {
        validate_path_component("data source", data_source)?;
        validate_path_component("segment id", segment_id)?;
        let prefix = self.segment_prefix(data_source, segment_id);
        let objects: Vec<_> = self
            .store
            .list(Some(&prefix))
            .try_collect()
            .await
            .map_err(|e| DeepStorageError::Other(e.to_string()))?;

        if objects.is_empty() {
            return Err(DeepStorageError::NotFound {
                data_source: data_source.to_string(),
                segment_id: segment_id.to_string(),
            });
        }

        tokio::fs::create_dir_all(dest).await?;

        for meta in &objects {
            let key = meta.location.as_ref();
            // Extract filename — everything after the segment prefix.
            let filename = key
                .strip_prefix(prefix.as_ref())
                .unwrap_or(key)
                .trim_start_matches('/');
            if filename.is_empty() {
                continue;
            }

            let data = self
                .store
                .get(&meta.location)
                .await
                .map_err(|e| DeepStorageError::Other(e.to_string()))?
                .bytes()
                .await
                .map_err(|e| DeepStorageError::Other(e.to_string()))?;

            let dest_file = dest.join(filename);
            // Create parent dirs for nested files.
            if let Some(parent) = dest_file.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(&dest_file, &data).await?;
        }

        Ok(())
    }

    async fn upload_segment(&self, data_source: &str, segment_id: &str, src: &Path) -> Result<()> {
        validate_path_component("data source", data_source)?;
        validate_path_component("segment id", segment_id)?;
        // The exact set of object keys THIS upload puts — the prune below
        // makes the segment prefix equal to it (REPLACE semantics, R22 H1).
        let mut written: std::collections::HashSet<ObjectPath> = std::collections::HashSet::new();
        if src.is_dir() {
            let mut entries = tokio::fs::read_dir(src).await?;
            while let Some(entry) = entries.next_entry().await? {
                let ft = entry.file_type().await?;
                if ft.is_file() {
                    let filename = entry
                        .file_name()
                        .to_str()
                        .ok_or_else(|| DeepStorageError::Other("non-UTF-8 filename".to_string()))?
                        .to_string();
                    let data = tokio::fs::read(entry.path()).await?;
                    let obj_path = self.file_path(data_source, segment_id, &filename);
                    self.store
                        .put(&obj_path, PutPayload::from(Bytes::from(data)))
                        .await
                        .map_err(|e| DeepStorageError::Other(e.to_string()))?;
                    written.insert(obj_path);
                }
            }
        } else {
            // Single file — store as segment.bin.
            let data = tokio::fs::read(src).await?;
            let obj_path = self.file_path(data_source, segment_id, "segment.bin");
            self.store
                .put(&obj_path, PutPayload::from(Bytes::from(data)))
                .await
                .map_err(|e| DeepStorageError::Other(e.to_string()))?;
            written.insert(obj_path);
        }

        // REPLACE semantics (Codex R22 H1): delete every object under THIS
        // segment's prefix that this upload did not put. An orphan blob from
        // a crash-after-upload-before-metadata-commit is invisible to
        // `allocate_segment_id`, so its id can be reused for a SMALLER
        // artifact; merge semantics left the orphan's surplus objects in the
        // prefix, dest != src, and the staging-computed content hash (R17 H2)
        // failed at bootstrap reload. The orphan is by definition UNCOMMITTED
        // (committed ids are rejected by the collision check), so deleting
        // its leftovers is safe; a crash mid-prune merely leaves another
        // uncommitted orphan for the next upload to replace. Scoping: the
        // listing is bounded to `<prefix><ds>/<segment_id>/` and
        // `object_store` prefixes match on whole path segments (`…/seg_1`
        // does NOT match `…/seg_10/x`), so no other segment's keys can ever
        // be touched — and only keys absent from `written` are deleted.
        let prefix = self.segment_prefix(data_source, segment_id);
        let existing: Vec<_> = self
            .store
            .list(Some(&prefix))
            .try_collect()
            .await
            .map_err(|e| DeepStorageError::Other(e.to_string()))?;
        for meta in &existing {
            if !written.contains(&meta.location) {
                self.store
                    .delete(&meta.location)
                    .await
                    .map_err(|e| DeepStorageError::Other(e.to_string()))?;
            }
        }

        Ok(())
    }

    async fn delete_segment(&self, data_source: &str, segment_id: &str) -> Result<()> {
        validate_path_component("data source", data_source)?;
        validate_path_component("segment id", segment_id)?;
        let prefix = self.segment_prefix(data_source, segment_id);
        let objects: Vec<_> = self
            .store
            .list(Some(&prefix))
            .try_collect()
            .await
            .map_err(|e| DeepStorageError::Other(e.to_string()))?;

        // Idempotent: empty listing means nothing to delete, succeed silently.
        // Matches S3's native semantics where DELETE of a missing key returns 204.
        for meta in &objects {
            self.store
                .delete(&meta.location)
                .await
                .map_err(|e| DeepStorageError::Other(e.to_string()))?;
        }

        Ok(())
    }

    async fn segment_exists(&self, data_source: &str, segment_id: &str) -> Result<bool> {
        validate_path_component("data source", data_source)?;
        validate_path_component("segment id", segment_id)?;
        let prefix = self.segment_prefix(data_source, segment_id);
        let mut listing = self.store.list(Some(&prefix));
        // If we can get at least one object the segment exists.
        match listing.try_next().await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(DeepStorageError::Other(e.to_string())),
        }
    }

    fn backend_type(&self) -> &'static str {
        "s3"
    }
}

// ---------------------------------------------------------------------------
// InMemoryDeepStorage
// ---------------------------------------------------------------------------

/// In-memory deep storage for testing.
///
/// Backed by [`object_store::memory::InMemory`] under the hood so all
/// operations go through the same code path as [`S3DeepStorage`].
pub struct InMemoryDeepStorage {
    inner: S3DeepStorage,
}

impl InMemoryDeepStorage {
    /// Create a new in-memory deep storage with a default `"segments/"` prefix.
    pub fn new() -> Self {
        let store = InMemory::new();
        Self {
            inner: S3DeepStorage::with_store(Box::new(store), "segments/".to_string()),
        }
    }
}

impl Default for InMemoryDeepStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DeepStorage for InMemoryDeepStorage {
    async fn list_segments(&self, data_source: &str) -> Result<Vec<String>> {
        self.inner.list_segments(data_source).await
    }

    async fn download_segment(
        &self,
        data_source: &str,
        segment_id: &str,
        dest: &Path,
    ) -> Result<()> {
        self.inner
            .download_segment(data_source, segment_id, dest)
            .await
    }

    async fn upload_segment(&self, data_source: &str, segment_id: &str, src: &Path) -> Result<()> {
        self.inner
            .upload_segment(data_source, segment_id, src)
            .await
    }

    async fn delete_segment(&self, data_source: &str, segment_id: &str) -> Result<()> {
        self.inner.delete_segment(data_source, segment_id).await
    }

    async fn segment_exists(&self, data_source: &str, segment_id: &str) -> Result<bool> {
        self.inner.segment_exists(data_source, segment_id).await
    }

    fn backend_type(&self) -> &'static str {
        "memory"
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Create a [`DeepStorage`] implementation from a [`DeepStorageConfig`].
///
/// - `Local` produces a [`LocalDeepStorage`]
/// - `S3` produces an [`S3DeepStorage`] (credentials from environment)
/// - Other variants return an error
pub fn create_deep_storage(config: &DeepStorageConfig) -> Result<Box<dyn DeepStorage>> {
    match config.typ {
        DeepStorageType::Local => Ok(Box::new(LocalDeepStorage::new(PathBuf::from(
            &config.base_path,
        )))),
        DeepStorageType::S3 => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                DeepStorageError::Other("s3_bucket required for S3 storage".into())
            })?;
            let region = config.s3_region.as_deref().unwrap_or("us-east-1");
            let prefix = config.s3_prefix.as_deref().unwrap_or("segments/");
            Ok(Box::new(S3DeepStorage::new(bucket, region, prefix)?))
        }
        other => Err(DeepStorageError::Other(format!(
            "unsupported storage type: {other:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // =======================================================================
    // LocalDeepStorage tests (unchanged)
    // =======================================================================

    #[tokio::test]
    async fn local_list_segments_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        let segments = storage.list_segments("wiki").await.expect("list");
        assert!(segments.is_empty());
    }

    #[tokio::test]
    async fn local_upload_and_list_segments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"hello")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_001", &src)
            .await
            .expect("upload");

        let segments = storage.list_segments("wiki").await.expect("list");
        assert_eq!(segments, vec!["seg_001"]);
    }

    #[tokio::test]
    async fn local_upload_and_download_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"segment-data")
            .await
            .expect("write");

        storage
            .upload_segment("clicks", "seg_002", &src)
            .await
            .expect("upload");

        let dest = dir.path().join("downloaded");
        storage
            .download_segment("clicks", "seg_002", &dest)
            .await
            .expect("download");

        let content = tokio::fs::read(dest.join("data.bin")).await.expect("read");
        assert_eq!(content, b"segment-data");
    }

    #[tokio::test]
    async fn local_segment_exists_check() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        assert!(
            !storage
                .segment_exists("wiki", "seg_x")
                .await
                .expect("exists")
        );

        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"x")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_x", &src)
            .await
            .expect("upload");

        assert!(
            storage
                .segment_exists("wiki", "seg_x")
                .await
                .expect("exists")
        );
    }

    #[tokio::test]
    async fn local_delete_segment_removes_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"x")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_del", &src)
            .await
            .expect("upload");

        assert!(
            storage
                .segment_exists("wiki", "seg_del")
                .await
                .expect("exists")
        );

        storage
            .delete_segment("wiki", "seg_del")
            .await
            .expect("delete");

        assert!(
            !storage
                .segment_exists("wiki", "seg_del")
                .await
                .expect("exists")
        );
    }

    #[tokio::test]
    async fn local_delete_nonexistent_segment_is_idempotent() {
        // Idempotent delete: removing a segment that never existed is Ok(()),
        // mirroring S3's native DELETE-of-missing-key semantics (204 No Content).
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        storage
            .delete_segment("wiki", "no_such")
            .await
            .expect("idempotent delete of nonexistent segment");
    }

    #[tokio::test]
    async fn local_delete_segment_twice_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"x")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_twice", &src)
            .await
            .expect("upload");
        storage
            .delete_segment("wiki", "seg_twice")
            .await
            .expect("first delete");
        storage
            .delete_segment("wiki", "seg_twice")
            .await
            .expect("second delete must also succeed (idempotency)");
    }

    #[tokio::test]
    async fn local_download_nonexistent_segment_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        let dest = dir.path().join("dest");
        let result = storage.download_segment("wiki", "no_such", &dest).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn local_multiple_datasources() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"x")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_1", &src)
            .await
            .expect("upload");
        storage
            .upload_segment("clicks", "seg_2", &src)
            .await
            .expect("upload");
        storage
            .upload_segment("wiki", "seg_3", &src)
            .await
            .expect("upload");

        let wiki_segs = storage.list_segments("wiki").await.expect("list");
        assert_eq!(wiki_segs, vec!["seg_1", "seg_3"]);

        let click_segs = storage.list_segments("clicks").await.expect("list");
        assert_eq!(click_segs, vec!["seg_2"]);
    }

    #[tokio::test]
    async fn local_upload_single_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        let src_file = dir.path().join("single.bin");
        tokio::fs::write(&src_file, b"single-file-data")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_single", &src_file)
            .await
            .expect("upload");

        assert!(
            storage
                .segment_exists("wiki", "seg_single")
                .await
                .expect("exists")
        );

        let stored = dir
            .path()
            .join("wiki")
            .join("seg_single")
            .join("segment.bin");
        let content = tokio::fs::read(stored).await.expect("read");
        assert_eq!(content, b"single-file-data");
    }

    // =======================================================================
    // InMemoryDeepStorage / S3DeepStorage tests
    // =======================================================================

    #[tokio::test]
    async fn s3_upload_and_list_segments() {
        let storage = InMemoryDeepStorage::new();

        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"hello")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_001", &src)
            .await
            .expect("upload");

        let segments = storage.list_segments("wiki").await.expect("list");
        assert_eq!(segments, vec!["seg_001"]);
    }

    #[tokio::test]
    async fn s3_upload_and_download_segment() {
        let storage = InMemoryDeepStorage::new();

        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"segment-data")
            .await
            .expect("write");

        storage
            .upload_segment("clicks", "seg_002", &src)
            .await
            .expect("upload");

        let dest = dir.path().join("downloaded");
        storage
            .download_segment("clicks", "seg_002", &dest)
            .await
            .expect("download");

        let content = tokio::fs::read(dest.join("data.bin")).await.expect("read");
        assert_eq!(content, b"segment-data");
    }

    #[tokio::test]
    async fn s3_delete_and_verify_gone() {
        let storage = InMemoryDeepStorage::new();

        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"x")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_del", &src)
            .await
            .expect("upload");

        assert!(
            storage
                .segment_exists("wiki", "seg_del")
                .await
                .expect("exists")
        );

        storage
            .delete_segment("wiki", "seg_del")
            .await
            .expect("delete");

        assert!(
            !storage
                .segment_exists("wiki", "seg_del")
                .await
                .expect("exists")
        );
    }

    #[tokio::test]
    async fn s3_segment_exists() {
        let storage = InMemoryDeepStorage::new();

        assert!(
            !storage
                .segment_exists("wiki", "nope")
                .await
                .expect("exists")
        );

        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"x")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_x", &src)
            .await
            .expect("upload");

        assert!(
            storage
                .segment_exists("wiki", "seg_x")
                .await
                .expect("exists")
        );
    }

    #[tokio::test]
    async fn s3_multiple_datasources() {
        let storage = InMemoryDeepStorage::new();

        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"x")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_1", &src)
            .await
            .expect("upload");
        storage
            .upload_segment("clicks", "seg_2", &src)
            .await
            .expect("upload");
        storage
            .upload_segment("wiki", "seg_3", &src)
            .await
            .expect("upload");

        let wiki_segs = storage.list_segments("wiki").await.expect("list");
        assert_eq!(wiki_segs, vec!["seg_1", "seg_3"]);

        let click_segs = storage.list_segments("clicks").await.expect("list");
        assert_eq!(click_segs, vec!["seg_2"]);
    }

    #[tokio::test]
    async fn s3_empty_prefix_listing() {
        let storage = InMemoryDeepStorage::new();

        let segments = storage.list_segments("nonexistent").await.expect("list");
        assert!(segments.is_empty());
    }

    #[tokio::test]
    async fn s3_download_nonexistent_errors() {
        let storage = InMemoryDeepStorage::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("dest");

        let result = storage.download_segment("wiki", "no_such", &dest).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn s3_delete_nonexistent_is_idempotent() {
        // Matches S3-native DELETE semantics: missing key is success.
        let storage = InMemoryDeepStorage::new();

        storage
            .delete_segment("wiki", "no_such")
            .await
            .expect("idempotent delete of nonexistent segment");
    }

    #[tokio::test]
    async fn s3_delete_segment_twice_is_idempotent() {
        let storage = InMemoryDeepStorage::new();

        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"x")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_twice", &src)
            .await
            .expect("upload");
        storage
            .delete_segment("wiki", "seg_twice")
            .await
            .expect("first delete");
        storage
            .delete_segment("wiki", "seg_twice")
            .await
            .expect("second delete must also succeed (idempotency)");
    }

    #[tokio::test]
    async fn s3_upload_multiple_files() {
        let storage = InMemoryDeepStorage::new();

        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("meta.json"), b"{\"version\":1}")
            .await
            .expect("write");
        tokio::fs::write(src.join("data.bin"), b"binary-data")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_multi", &src)
            .await
            .expect("upload");

        let dest = dir.path().join("downloaded");
        storage
            .download_segment("wiki", "seg_multi", &dest)
            .await
            .expect("download");

        let meta = tokio::fs::read(dest.join("meta.json"))
            .await
            .expect("read meta");
        assert_eq!(meta, b"{\"version\":1}");

        let data = tokio::fs::read(dest.join("data.bin"))
            .await
            .expect("read data");
        assert_eq!(data, b"binary-data");
    }

    #[tokio::test]
    async fn factory_creates_local() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = DeepStorageConfig {
            typ: DeepStorageType::Local,
            base_path: dir.path().to_str().expect("utf8").to_string(),
            s3_bucket: None,
            s3_region: None,
            s3_prefix: None,
        };
        let storage = create_deep_storage(&config).expect("factory");
        let segments = storage.list_segments("test").await.expect("list");
        assert!(segments.is_empty());
    }

    #[tokio::test]
    async fn factory_rejects_unsupported() {
        let config = DeepStorageConfig {
            typ: DeepStorageType::Gcs,
            base_path: "/tmp".into(),
            s3_bucket: None,
            s3_region: None,
            s3_prefix: None,
        };
        let result = create_deep_storage(&config);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn factory_s3_requires_bucket() {
        let config = DeepStorageConfig {
            typ: DeepStorageType::S3,
            base_path: String::new(),
            s3_bucket: None,
            s3_region: None,
            s3_prefix: None,
        };
        let result = create_deep_storage(&config);
        assert!(result.is_err());
    }

    // ----- H9: path-traversal rejection ----------------------------------

    #[test]
    fn validate_path_component_rejects_traversal_and_separators() {
        for bad in [
            "..",
            ".",
            "",
            "../../outside",
            "a/b",
            "/abs",
            "a\\b",
            "x\0y",
        ] {
            assert!(
                validate_path_component("data source", bad).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
        // Legitimate datasource names / segment ids (`:` from ISO timestamps).
        for ok in [
            "wiki",
            "clicks",
            "seg_001",
            "events_2020-01-01T00:00:00.000Z_v1",
        ] {
            assert!(
                validate_path_component("segment id", ok).is_ok(),
                "expected {ok:?} to be accepted"
            );
        }
    }

    #[tokio::test]
    async fn local_upload_rejects_traversal_datasource_stays_in_root() {
        // A hostile datasource must NOT let the upload escape base_dir.
        let root = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let storage = LocalDeepStorage::new(root.path().to_path_buf());

        let src = root.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"pwn")
            .await
            .expect("write");

        // `..`-laden datasource pointed at a sibling temp dir: must be refused,
        // and nothing must be written outside the storage root.
        let escape = format!(
            "../{}",
            outside.path().file_name().unwrap().to_str().unwrap()
        );
        let err = storage
            .upload_segment(&escape, "seg", &src)
            .await
            .expect_err("traversal datasource must be rejected");
        assert!(matches!(err, DeepStorageError::Other(_)));
        // The outside dir got no `seg` subdir.
        assert!(!outside.path().join("seg").exists());

        // Segment id traversal is rejected too.
        assert!(
            storage
                .upload_segment("wiki", "../evil", &src)
                .await
                .is_err()
        );
        assert!(
            storage
                .download_segment("wiki", "../evil", &src)
                .await
                .is_err()
        );
        assert!(storage.delete_segment("..", "seg").await.is_err());
        assert!(storage.segment_exists("wiki", "../evil").await.is_err());
    }

    // ----- H7: durable (fsync'd) upload ----------------------------------

    #[tokio::test]
    async fn local_upload_is_durable_and_readable() {
        // After upload the blob is fsync'd and fully readable — a
        // structural durability check: multi-file segment round-trips with
        // exact bytes and the fsync path (file + dir) runs without error.
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("v9.bin"), b"segment-bytes")
            .await
            .expect("write");
        tokio::fs::write(src.join("meta.json"), b"{\"n\":3}")
            .await
            .expect("write");

        storage
            .upload_segment("events", "seg_dur", &src)
            .await
            .expect("durable upload");

        let dest = dir.path().join("dl");
        storage
            .download_segment("events", "seg_dur", &dest)
            .await
            .expect("download");
        assert_eq!(
            tokio::fs::read(dest.join("v9.bin")).await.expect("read"),
            b"segment-bytes"
        );
        assert_eq!(
            tokio::fs::read(dest.join("meta.json")).await.expect("read"),
            b"{\"n\":3}"
        );
    }

    // =======================================================================
    // C3: authoritative fsync (error propagation + fsync-to-root)
    // =======================================================================

    /// C3: a directory fsync that cannot even open its target must PROPAGATE
    /// the error, not silently succeed. The pre-fix `fsync_dir` swallowed every
    /// failure (best-effort no-op), so a non-durable upload was reported as
    /// durable; since `upload_segment` threads every `fsync_dir` through `?`,
    /// propagating here makes the upload fail (C3).
    #[cfg(unix)]
    #[tokio::test]
    async fn fsync_dir_propagates_failure_instead_of_swallowing() {
        let missing = Path::new("/ferrodruid-nonexistent-fsync-root/deep/does-not-exist");
        assert!(
            fsync_dir(missing).await.is_err(),
            "fsync_dir must propagate an fsync-step failure (C3), not swallow it"
        );
    }

    /// C3 + R19 H1: the fsync chain ALWAYS reaches the storage ROOT — segment
    /// dir, datasource dir, and root — regardless of whether the datasource dir
    /// looked "already present" this attempt. `upload_segment` is retried, and
    /// a first attempt can create the datasource dir then fail before fsyncing
    /// the root; a later attempt must still fsync the root or the datasource
    /// dentry stays non-durable while the retry reports `Ok` (loss on power
    /// loss after the metadata + Kafka-offset commit). Pre-R19 this stopped at
    /// the datasource dir whenever it observed the dir already existing — the
    /// exact retry hole.
    #[test]
    fn fsync_chain_always_reaches_the_storage_root() {
        let dest = Path::new("/base/ds/seg");
        assert_eq!(
            fsync_chain(dest),
            vec![
                PathBuf::from("/base/ds/seg"),
                PathBuf::from("/base/ds"),
                PathBuf::from("/base"),
            ],
            "every upload must fsync up to the storage root — a prior failed \
             retry may have created the datasource dir non-durably (R19 H1)"
        );
    }

    /// Codex R7 H2 (pure chain): a durable `create_dir_all` must fsync every
    /// NEWLY created directory PLUS the deepest PRE-EXISTING ancestor — the
    /// directory whose entry list gained the topmost new dir. The startup
    /// deep-storage-root creation (`data_dir/deep-storage`) previously
    /// fsynced nothing: the upload path's `fsync_chain` stops AT the storage
    /// root, silently assuming the root's own dentry (in `data_dir`) is
    /// durable — so a power loss after a metadata commit could drop the
    /// whole `deep-storage/` dentry and orphan every committed blob.
    #[test]
    fn durable_create_chain_fsyncs_parent_of_new_root() {
        let existing = |p: &Path| p == Path::new("/data") || p == Path::new("/");
        // Fresh `data_dir/deep-storage` under an existing `/data`: fsync the
        // new dir AND `/data` (its dentry list changed).
        assert_eq!(
            durable_create_fsync_chain(Path::new("/data/deep-storage"), existing),
            vec![PathBuf::from("/data/deep-storage"), PathBuf::from("/data")],
            "the parent that gained the new dentry must be fsynced (H2)"
        );
        // Multi-level creation (`/data` itself new): every created level plus
        // the deepest pre-existing ancestor (`/`).
        let only_root = |p: &Path| p == Path::new("/");
        assert_eq!(
            durable_create_fsync_chain(Path::new("/data/deep-storage"), only_root),
            vec![
                PathBuf::from("/data/deep-storage"),
                PathBuf::from("/data"),
                PathBuf::from("/"),
            ],
            "every newly created ancestor and its pre-existing parent must be fsynced"
        );
        // Already-existing path: belt-and-braces re-fsync of the dir + its
        // parent (an earlier NON-durable creator may never have fsynced the
        // dentry), nothing further up.
        let all = |_: &Path| true;
        assert_eq!(
            durable_create_fsync_chain(Path::new("/data/deep-storage"), all),
            vec![PathBuf::from("/data/deep-storage"), PathBuf::from("/data")],
            "a pre-existing dir re-fsyncs itself + its parent only"
        );
    }

    /// Codex R8 H3: a RELATIVE creation (`--data-dir data`) whose topmost
    /// NEW directory has no nameable parent component must fsync the current
    /// working directory (`.`) — the directory whose dentry list gained the
    /// new tree. R7's chain walk silently dropped the empty `Path::parent`
    /// of a bare relative component, so `data/` (and everything created
    /// beneath it: metadata + deep-storage) hung off a non-durable CWD
    /// dentry — a power loss could drop the ENTIRE data tree even though
    /// every inner directory and blob had been fsynced.
    #[test]
    fn durable_create_chain_fsyncs_cwd_for_relative_paths() {
        // Nothing pre-exists: every created level PLUS the CWD.
        let none = |_: &Path| false;
        assert_eq!(
            durable_create_fsync_chain(Path::new("data/deep-storage"), none),
            vec![
                PathBuf::from("data/deep-storage"),
                PathBuf::from("data"),
                PathBuf::from("."),
            ],
            "the CWD gained the `data` dentry and must be fsynced (H3)"
        );
        // Single bare relative component.
        assert_eq!(
            durable_create_fsync_chain(Path::new("data"), none),
            vec![PathBuf::from("data"), PathBuf::from(".")],
            "a bare relative dir's dentry lives in the CWD (H3)"
        );
        // Pre-existing bare relative dir: the belt-and-braces parent
        // re-fsync also targets the CWD.
        let all = |_: &Path| true;
        assert_eq!(
            durable_create_fsync_chain(Path::new("data"), all),
            vec![PathBuf::from("data"), PathBuf::from(".")],
            "belt-and-braces re-fsync of a relative dir reaches the CWD (H3)"
        );
        // A pre-existing boundary INSIDE the relative path stops the walk
        // before the CWD, exactly like the absolute case.
        let data = |p: &Path| p == Path::new("data");
        assert_eq!(
            durable_create_fsync_chain(Path::new("data/deep-storage"), data),
            vec![PathBuf::from("data/deep-storage"), PathBuf::from("data")],
            "an existing relative parent bounds the chain as before"
        );
        // `./data` (explicit CWD prefix) keeps working via the normal walk.
        let dot = |p: &Path| p == Path::new(".");
        assert_eq!(
            durable_create_fsync_chain(Path::new("./data"), dot),
            vec![PathBuf::from("./data"), PathBuf::from(".")],
        );
    }

    /// Codex R21 H1: a MULTI-level relative tree that ALREADY EXISTS must
    /// still belt-and-braces re-fsync every relative ancestor up to AND
    /// including the CWD. Pre-R21 the pre-existing branch re-fsynced only the
    /// immediate parent (`data`), leaving the relative ROOT's own dentry —
    /// which lives in the CWD — hanging off a non-durable `.` entry: a prior
    /// non-durable creation of `data` could be lost on a power cut after
    /// blobs + metadata + Kafka offsets committed.
    #[test]
    fn durable_create_chain_refsyncs_cwd_for_a_preexisting_relative_tree() {
        let all = |_: &Path| true;
        assert_eq!(
            durable_create_fsync_chain(Path::new("data/deep-storage"), all),
            vec![
                PathBuf::from("data/deep-storage"),
                PathBuf::from("data"),
                PathBuf::from("."),
            ],
            "a pre-existing relative tree must re-fsync up to the CWD (R21 H1)"
        );
        // An ABSOLUTE pre-existing path stops at the immediate parent — deeper
        // ancestors are the trusted OS/admin boundary (no walk to `/`).
        assert_eq!(
            durable_create_fsync_chain(Path::new("/var/lib/ferro/deep-storage"), all),
            vec![
                PathBuf::from("/var/lib/ferro/deep-storage"),
                PathBuf::from("/var/lib/ferro"),
            ],
            "an absolute pre-existing path re-fsyncs only its immediate parent"
        );
    }

    /// Codex R7 H2 (functional): `create_dir_all_durable` creates the whole
    /// missing chain, RETURNS the directories it fsynced (so callers/tests
    /// can pin that the parent of the created root is durable), and is
    /// idempotent.
    #[tokio::test]
    async fn create_dir_all_durable_fsyncs_parent_of_new_root() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data_dir = tmp.path().join("data");
        let root = data_dir.join("deep-storage");

        let synced = create_dir_all_durable(&root).await.expect("durable create");
        assert!(root.is_dir(), "the root must exist");
        assert_eq!(
            synced,
            vec![root.clone(), data_dir.clone(), tmp.path().to_path_buf()],
            "must fsync the new root, the new data_dir, and the pre-existing \
             tempdir that gained the data_dir dentry (H2)"
        );

        // Idempotent second call: the dir + its parent are re-fsynced
        // (belt-and-braces), nothing fails.
        let synced_again = create_dir_all_durable(&root).await.expect("idempotent");
        assert_eq!(synced_again, vec![root.clone(), data_dir.clone()]);

        // A path component that exists as a FILE must fail loudly, not
        // silently succeed.
        let file_path = tmp.path().join("blocker");
        tokio::fs::write(&file_path, b"x")
            .await
            .expect("write file");
        assert!(
            create_dir_all_durable(&file_path.join("sub"))
                .await
                .is_err(),
            "a file in the way must surface as an error"
        );
    }

    /// C3: uploading into a FRESH storage root (new datasource dir) succeeds —
    /// the added root fsync runs without error — and the blob is durable and
    /// readable. Exercises the `ds_existed == false` branch end-to-end.
    #[tokio::test]
    async fn local_upload_new_datasource_fsyncs_root_and_succeeds() {
        let root = tempfile::tempdir().expect("tempdir");
        // A storage root with NO datasource dir yet.
        let storage = LocalDeepStorage::new(root.path().join("store"));
        tokio::fs::create_dir_all(root.path().join("store"))
            .await
            .expect("mk store root");

        let src = root.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("v9.bin"), b"fresh-ds")
            .await
            .expect("write");

        storage
            .upload_segment("brand_new_ds", "seg_root", &src)
            .await
            .expect("upload into a fresh datasource dir fsyncs the root and succeeds");

        assert!(
            storage
                .segment_exists("brand_new_ds", "seg_root")
                .await
                .expect("exists")
        );
    }

    // =======================================================================
    // Codex R22 H1: re-upload REPLACE semantics (stale prune)
    // =======================================================================
    //
    // A crash after `upload_segment` but before the metadata commit leaves an
    // ORPHAN blob in deep storage that `allocate_segment_id` cannot see (it
    // consults only metadata + historical). The reused id can then receive a
    // SMALLER artifact; with the old merge semantics the orphan's surplus
    // files survived alongside the new upload, so `dest != src` — the
    // staging-computed content hash (R17 H2) no longer matched the reloaded
    // dest and bootstrap failed loudly. `upload_segment` must therefore be
    // REPLACE: after an `Ok` the destination holds EXACTLY the source files.

    /// List the file names currently stored for a local segment, sorted.
    async fn local_segment_file_names(root: &Path, ds: &str, seg: &str) -> Vec<String> {
        let seg_dir = root.join(ds).join(seg);
        let mut names = Vec::new();
        let mut entries = tokio::fs::read_dir(&seg_dir).await.expect("read seg dir");
        while let Some(entry) = entries.next_entry().await.expect("entry") {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        names
    }

    /// List the file names a download of the segment yields, sorted. Goes
    /// through the SAME listing the bootstrap reload uses, so a stale
    /// (unpruned) object would show up here exactly as it would at reload.
    async fn downloaded_file_names(
        storage: &dyn DeepStorage,
        ds: &str,
        seg: &str,
        dest: &Path,
    ) -> Vec<String> {
        storage
            .download_segment(ds, seg, dest)
            .await
            .expect("download");
        let mut names = Vec::new();
        let mut entries = tokio::fs::read_dir(dest).await.expect("read dl dir");
        while let Some(entry) = entries.next_entry().await.expect("entry") {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        names
    }

    #[tokio::test]
    async fn local_reupload_smaller_artifact_prunes_stale_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        // First upload: the LARGER orphan artifact {a,b,c}.
        let big = dir.path().join("big");
        tokio::fs::create_dir_all(&big).await.expect("mkdir");
        for name in ["a.bin", "b.bin", "c.bin"] {
            tokio::fs::write(big.join(name), b"old-bytes")
                .await
                .expect("write");
        }
        storage
            .upload_segment("wiki", "seg_reuse", &big)
            .await
            .expect("upload big");

        // Reused id gets a SMALLER artifact {a} — dest must become EXACTLY it.
        let small = dir.path().join("small");
        tokio::fs::create_dir_all(&small).await.expect("mkdir");
        tokio::fs::write(small.join("a.bin"), b"new-bytes")
            .await
            .expect("write");
        storage
            .upload_segment("wiki", "seg_reuse", &small)
            .await
            .expect("upload small");

        assert_eq!(
            local_segment_file_names(dir.path(), "wiki", "seg_reuse").await,
            vec!["a.bin"],
            "stale b.bin/c.bin from the orphan must be pruned (R22 H1)"
        );
        let dl = dir.path().join("dl");
        assert_eq!(
            downloaded_file_names(&storage, "wiki", "seg_reuse", &dl).await,
            vec!["a.bin"]
        );
        assert_eq!(
            tokio::fs::read(dl.join("a.bin")).await.expect("read"),
            b"new-bytes",
            "the kept file must carry the NEW upload's bytes"
        );
    }

    #[tokio::test]
    async fn local_reupload_single_file_over_directory_prunes_stale_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        // Orphan: a multi-file DIRECTORY artifact.
        let big = dir.path().join("big");
        tokio::fs::create_dir_all(&big).await.expect("mkdir");
        tokio::fs::write(big.join("a.bin"), b"old")
            .await
            .expect("w");
        tokio::fs::write(big.join("b.bin"), b"old")
            .await
            .expect("w");
        storage
            .upload_segment("wiki", "seg_shrink", &big)
            .await
            .expect("upload dir");

        // Reused id gets a SINGLE-FILE artifact → only segment.bin survives.
        let single = dir.path().join("single.bin");
        tokio::fs::write(&single, b"new-single").await.expect("w");
        storage
            .upload_segment("wiki", "seg_shrink", &single)
            .await
            .expect("upload single");

        assert_eq!(
            local_segment_file_names(dir.path(), "wiki", "seg_shrink").await,
            vec!["segment.bin"],
            "single-file re-upload must prune every non-segment.bin file (R22 H1)"
        );
        let dl = dir.path().join("dl");
        assert_eq!(
            downloaded_file_names(&storage, "wiki", "seg_shrink", &dl).await,
            vec!["segment.bin"]
        );
        assert_eq!(
            tokio::fs::read(dl.join("segment.bin")).await.expect("read"),
            b"new-single"
        );
    }

    #[tokio::test]
    async fn s3_reupload_smaller_artifact_prunes_stale_objects() {
        let storage = InMemoryDeepStorage::new();
        let dir = tempfile::tempdir().expect("tempdir");

        let big = dir.path().join("big");
        tokio::fs::create_dir_all(&big).await.expect("mkdir");
        for name in ["a.bin", "b.bin", "c.bin"] {
            tokio::fs::write(big.join(name), b"old-bytes")
                .await
                .expect("write");
        }
        storage
            .upload_segment("wiki", "seg_reuse", &big)
            .await
            .expect("upload big");

        let small = dir.path().join("small");
        tokio::fs::create_dir_all(&small).await.expect("mkdir");
        tokio::fs::write(small.join("a.bin"), b"new-bytes")
            .await
            .expect("write");
        storage
            .upload_segment("wiki", "seg_reuse", &small)
            .await
            .expect("upload small");

        // The download path lists the segment prefix — a stale unpruned
        // object would be materialized here just as at bootstrap reload.
        let dl = dir.path().join("dl");
        assert_eq!(
            downloaded_file_names(&storage, "wiki", "seg_reuse", &dl).await,
            vec!["a.bin"],
            "stale b.bin/c.bin objects must be pruned from the prefix (R22 H1)"
        );
        assert_eq!(
            tokio::fs::read(dl.join("a.bin")).await.expect("read"),
            b"new-bytes"
        );
    }

    #[tokio::test]
    async fn s3_reupload_single_file_over_directory_prunes_stale_objects() {
        let storage = InMemoryDeepStorage::new();
        let dir = tempfile::tempdir().expect("tempdir");

        let big = dir.path().join("big");
        tokio::fs::create_dir_all(&big).await.expect("mkdir");
        tokio::fs::write(big.join("a.bin"), b"old")
            .await
            .expect("w");
        tokio::fs::write(big.join("b.bin"), b"old")
            .await
            .expect("w");
        storage
            .upload_segment("wiki", "seg_shrink", &big)
            .await
            .expect("upload dir");

        let single = dir.path().join("single.bin");
        tokio::fs::write(&single, b"new-single").await.expect("w");
        storage
            .upload_segment("wiki", "seg_shrink", &single)
            .await
            .expect("upload single");

        let dl = dir.path().join("dl");
        assert_eq!(
            downloaded_file_names(&storage, "wiki", "seg_shrink", &dl).await,
            vec!["segment.bin"],
            "single-file re-upload must prune every non-segment.bin object (R22 H1)"
        );
        assert_eq!(
            tokio::fs::read(dl.join("segment.bin")).await.expect("read"),
            b"new-single"
        );
    }

    // =======================================================================
    // Codex R1 (compat-2) H2: dest symlink follow + non-regular prune
    // =======================================================================

    /// H2 (a): a pre-existing DEST entry that is a symlink must never be
    /// written THROUGH — `tokio::fs::copy` would follow it and overwrite
    /// the symlink's target OUTSIDE the storage root (write escape). The
    /// upload must replace the symlink entry itself with a regular file.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_upload_never_writes_through_a_dest_symlink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        // A crash-orphan dest dir whose `data.bin` is a SYMLINK to a file
        // OUTSIDE the storage root.
        let target = outside.path().join("target.bin");
        tokio::fs::write(&target, b"SECRET-UNTOUCHED")
            .await
            .expect("write outside target");
        let dest = dir.path().join("wiki").join("seg_sym");
        tokio::fs::create_dir_all(&dest).await.expect("mkdir dest");
        std::os::unix::fs::symlink(&target, dest.join("data.bin")).expect("plant symlink");

        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir src");
        tokio::fs::write(src.join("data.bin"), b"new-bytes")
            .await
            .expect("write src");

        storage
            .upload_segment("wiki", "seg_sym", &src)
            .await
            .expect("upload over a symlink-laden orphan dest");

        assert_eq!(
            tokio::fs::read(&target).await.expect("read target"),
            b"SECRET-UNTOUCHED",
            "the symlink's outside target must NOT be written through (H2)"
        );
        let meta = tokio::fs::symlink_metadata(dest.join("data.bin"))
            .await
            .expect("lstat dest entry");
        assert!(
            meta.is_file() && !meta.is_symlink(),
            "the dest entry must now be a REGULAR file, not the old symlink"
        );
        assert_eq!(
            tokio::fs::read(dest.join("data.bin")).await.expect("read"),
            b"new-bytes"
        );
    }

    /// H2 (b): the REPLACE prune must remove EVERY leftover entry the
    /// upload did not write — stale symlinks and subdirectories included,
    /// not just regular files. A surviving symlink/subdir makes the dest
    /// differ from the uploaded set, so the staging-computed sha256 no
    /// longer matches a re-hash of the dest and the bootstrap reload
    /// bricks.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_upload_prunes_stale_symlinks_and_subdirs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let storage = LocalDeepStorage::new(dir.path().to_path_buf());

        let target = outside.path().join("target.bin");
        tokio::fs::write(&target, b"KEEP")
            .await
            .expect("write target");

        // Orphan dest with a stale regular file, a stale subdir, and a
        // stale symlink.
        let dest = dir.path().join("wiki").join("seg_stale");
        tokio::fs::create_dir_all(dest.join("stale_dir"))
            .await
            .expect("mkdir stale_dir");
        tokio::fs::write(dest.join("stale_dir").join("inner.bin"), b"old")
            .await
            .expect("write inner");
        tokio::fs::write(dest.join("stale.bin"), b"old")
            .await
            .expect("write stale");
        std::os::unix::fs::symlink(&target, dest.join("stale_link")).expect("plant symlink");

        let src = dir.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir src");
        tokio::fs::write(src.join("a.bin"), b"fresh")
            .await
            .expect("write src");

        storage
            .upload_segment("wiki", "seg_stale", &src)
            .await
            .expect("upload over a stale orphan dest");

        assert_eq!(
            local_segment_file_names(dir.path(), "wiki", "seg_stale").await,
            vec!["a.bin"],
            "EVERY stale entry (regular, symlink, subdir) must be pruned (H2)"
        );
        assert_eq!(
            blob_content_hash(&dest).expect("re-hash dest"),
            blob_content_hash(&src).expect("hash src"),
            "after upload the dest must re-hash to exactly the source set — \
             the check the bootstrap reload enforces"
        );
        assert_eq!(
            tokio::fs::read(&target).await.expect("read target"),
            b"KEEP",
            "pruning removes the symlink ENTRY, never its target"
        );
    }

    // =======================================================================
    // Codex R1 (compat-2) H6: streaming content hash
    // =======================================================================

    /// H6: the streaming implementation must produce a digest
    /// BYTE-IDENTICAL to the original whole-file-read length-framed
    /// reference (`name_len ‖ name ‖ content_len ‖ content`, 8-byte LE
    /// lengths, name-sorted, regular files only) — including files larger
    /// than the streaming buffer, empty files, and ignored subdirs.
    #[test]
    fn blob_content_hash_matches_the_length_framed_full_read_reference() {
        use sha2::{Digest, Sha256};

        let dir = tempfile::tempdir().expect("tempdir");
        let big: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(dir.path().join("big.bin"), &big).expect("write big");
        std::fs::write(dir.path().join("empty.bin"), b"").expect("write empty");
        std::fs::write(dir.path().join("small.bin"), b"abc").expect("write small");
        std::fs::create_dir(dir.path().join("subdir")).expect("mkdir");
        std::fs::write(dir.path().join("subdir").join("x.bin"), b"ignored").expect("write sub");

        let got = blob_content_hash(dir.path()).expect("streaming hash");

        // Inline full-read reference — the EXACT pre-streaming framing.
        let mut files: Vec<(Vec<u8>, PathBuf)> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .map(|e| e.expect("entry"))
            .filter(|e| e.file_type().expect("file type").is_file())
            .map(|e| (e.file_name().as_encoded_bytes().to_vec(), e.path()))
            .collect();
        files.sort_by(|a, b| a.0.cmp(&b.0));
        let mut hasher = Sha256::new();
        for (name, path) in &files {
            let bytes = std::fs::read(path).expect("full read");
            hasher.update((name.len() as u64).to_le_bytes());
            hasher.update(name);
            hasher.update((bytes.len() as u64).to_le_bytes());
            hasher.update(&bytes);
        }
        assert_eq!(
            got,
            hex::encode(hasher.finalize()),
            "streaming hash must be byte-identical to the full-read framing \
             (compat-3 stored sha256 values must keep verifying)"
        );
    }

    /// The prune is scoped to THIS segment's prefix: re-uploading `seg_1`
    /// must never delete objects of `seg_10` (a segment id for which
    /// `seg_1` is a STRING prefix — the adversarial scoping case) or of any
    /// other segment/datasource.
    #[tokio::test]
    async fn s3_reupload_prune_is_scoped_to_its_own_segment_prefix() {
        let storage = InMemoryDeepStorage::new();
        let dir = tempfile::tempdir().expect("tempdir");

        let full = dir.path().join("full");
        tokio::fs::create_dir_all(&full).await.expect("mkdir");
        tokio::fs::write(full.join("a.bin"), b"keep")
            .await
            .expect("w");
        tokio::fs::write(full.join("b.bin"), b"keep")
            .await
            .expect("w");
        storage
            .upload_segment("wiki", "seg_1", &full)
            .await
            .expect("upload seg_1");
        storage
            .upload_segment("wiki", "seg_10", &full)
            .await
            .expect("upload seg_10");
        storage
            .upload_segment("clicks", "seg_1", &full)
            .await
            .expect("upload clicks/seg_1");

        // Shrink wiki/seg_1 to {a.bin} only.
        let small = dir.path().join("small");
        tokio::fs::create_dir_all(&small).await.expect("mkdir");
        tokio::fs::write(small.join("a.bin"), b"new")
            .await
            .expect("w");
        storage
            .upload_segment("wiki", "seg_1", &small)
            .await
            .expect("re-upload seg_1");

        assert_eq!(
            downloaded_file_names(&storage, "wiki", "seg_1", &dir.path().join("dl1")).await,
            vec!["a.bin"]
        );
        // Neighbours are untouched: full {a,b} still present.
        assert_eq!(
            downloaded_file_names(&storage, "wiki", "seg_10", &dir.path().join("dl2")).await,
            vec!["a.bin", "b.bin"],
            "prune must NOT leak into seg_10 (string-prefix neighbour)"
        );
        assert_eq!(
            downloaded_file_names(&storage, "clicks", "seg_1", &dir.path().join("dl3")).await,
            vec!["a.bin", "b.bin"],
            "prune must NOT leak into another datasource"
        );
    }

    // =======================================================================
    // Codex R2 (compat-2) H1: ancestor / final-dir symlink escape
    // =======================================================================
    //
    // R1 hardened the CHILD entries of a segment dir, but the directory
    // COMPONENTS themselves (`<base>/<ds>` and `<base>/<ds>/<id>`) were
    // still followed: planted as symlinks to a directory OUTSIDE the
    // storage root, create_dir_all / copy / prune / fsync / download /
    // remove_dir_all all operate through them — writes and DELETES escape
    // the root, and a symlink swap makes committed blobs vanish (brick).
    // Every LocalDeepStorage operation must refuse a symlinked datasource
    // or segment dir, fail-loud. `base_dir` ITSELF may legitimately be a
    // symlink (data dir on another mount); only the components UNDER it
    // are constrained.

    /// H1 (a): `<base>/<ds>` planted as a symlink to an outside dir must
    /// be REFUSED by upload — nothing may be created or written outside
    /// the storage root.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_upload_refuses_symlinked_datasource_dir() {
        let root = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let storage = LocalDeepStorage::new(root.path().to_path_buf());

        std::os::unix::fs::symlink(outside.path(), root.path().join("wiki"))
            .expect("plant datasource-dir symlink");

        let src = root.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"escape")
            .await
            .expect("write");

        let err = storage
            .upload_segment("wiki", "seg_esc", &src)
            .await
            .expect_err("upload through a symlinked datasource dir must be refused (H1)");
        assert!(matches!(err, DeepStorageError::Other(_)));
        assert!(
            !outside.path().join("seg_esc").exists(),
            "nothing may be created outside the storage root (H1)"
        );
    }

    /// H1 (b): `<base>/<ds>/<id>` planted as a symlink to an outside dir
    /// must be REFUSED by upload — neither the copy (write-through) nor
    /// the REPLACE prune (delete-through!) may touch the outside dir.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_upload_refuses_symlinked_segment_dir() {
        let root = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let storage = LocalDeepStorage::new(root.path().to_path_buf());

        // A pre-existing EXTERNAL file: the prune would delete it, the
        // copy would write next to it.
        tokio::fs::write(outside.path().join("stale.bin"), b"EXTERNAL")
            .await
            .expect("write outside file");
        tokio::fs::create_dir_all(root.path().join("wiki"))
            .await
            .expect("mkdir ds");
        std::os::unix::fs::symlink(outside.path(), root.path().join("wiki").join("seg_esc"))
            .expect("plant segment-dir symlink");

        let src = root.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"escape")
            .await
            .expect("write");

        let err = storage
            .upload_segment("wiki", "seg_esc", &src)
            .await
            .expect_err("upload into a symlinked segment dir must be refused (H1)");
        assert!(matches!(err, DeepStorageError::Other(_)));
        assert!(
            !outside.path().join("data.bin").exists(),
            "the copy must not write through the segment-dir symlink (H1)"
        );
        assert_eq!(
            tokio::fs::read(outside.path().join("stale.bin"))
                .await
                .expect("read outside file"),
            b"EXTERNAL",
            "the REPLACE prune must never delete files outside the root (H1)"
        );
    }

    /// H1 (c): download through a symlinked segment dir must be REFUSED —
    /// otherwise external bytes are materialized as segment content.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_download_refuses_symlinked_segment_dir() {
        let root = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let storage = LocalDeepStorage::new(root.path().to_path_buf());

        tokio::fs::write(outside.path().join("leak.bin"), b"OUTSIDE-BYTES")
            .await
            .expect("write outside file");
        tokio::fs::create_dir_all(root.path().join("wiki"))
            .await
            .expect("mkdir ds");
        std::os::unix::fs::symlink(outside.path(), root.path().join("wiki").join("seg_leak"))
            .expect("plant segment-dir symlink");

        let dest = root.path().join("dl");
        let err = storage
            .download_segment("wiki", "seg_leak", &dest)
            .await
            .expect_err("download through a symlinked segment dir must be refused (H1)");
        assert!(matches!(err, DeepStorageError::Other(_)));
        assert!(
            !dest.join("leak.bin").exists(),
            "external bytes must never be materialized as segment content (H1)"
        );
    }

    /// H1 (d): delete through a symlinked DATASOURCE dir must be REFUSED —
    /// `remove_dir_all`'s own symlink defence guards the final component
    /// only, so a real dir reached through a symlinked ANCESTOR would be
    /// deleted OUTSIDE the root. `segment_exists` must fail-loud too
    /// instead of reporting external dirs as segments.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_delete_refuses_symlinked_datasource_dir() {
        let root = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let storage = LocalDeepStorage::new(root.path().to_path_buf());

        // A REAL directory outside the root, reached via the ds symlink.
        tokio::fs::create_dir_all(outside.path().join("seg_del"))
            .await
            .expect("mkdir outside seg");
        tokio::fs::write(outside.path().join("seg_del").join("blob.bin"), b"KEEP")
            .await
            .expect("write outside blob");
        std::os::unix::fs::symlink(outside.path(), root.path().join("wiki"))
            .expect("plant datasource-dir symlink");

        assert!(
            storage.segment_exists("wiki", "seg_del").await.is_err(),
            "segment_exists through a symlinked ds dir must fail-loud (H1)"
        );
        let err = storage
            .delete_segment("wiki", "seg_del")
            .await
            .expect_err("delete through a symlinked datasource dir must be refused (H1)");
        assert!(matches!(err, DeepStorageError::Other(_)));
        assert_eq!(
            tokio::fs::read(outside.path().join("seg_del").join("blob.bin"))
                .await
                .expect("read outside blob"),
            b"KEEP",
            "nothing outside the storage root may be deleted (H1)"
        );
    }

    /// H1 (e): `list_segments` through a symlinked datasource dir must
    /// fail-loud instead of listing external directories as segments.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_list_refuses_symlinked_datasource_dir() {
        let root = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let storage = LocalDeepStorage::new(root.path().to_path_buf());

        tokio::fs::create_dir_all(outside.path().join("not_a_segment"))
            .await
            .expect("mkdir outside");
        std::os::unix::fs::symlink(outside.path(), root.path().join("wiki"))
            .expect("plant datasource-dir symlink");

        assert!(
            storage.list_segments("wiki").await.is_err(),
            "list through a symlinked ds dir must fail-loud (H1)"
        );
    }

    /// H1 invariance pin: the storage ROOT itself being a symlink (data
    /// dir moved to another mount — a legitimate operational layout) keeps
    /// working end-to-end; only the components UNDER the root are
    /// constrained.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_symlinked_base_dir_itself_keeps_working() {
        let real = tempfile::tempdir().expect("tempdir");
        let holder = tempfile::tempdir().expect("holder tempdir");
        let base_link = holder.path().join("store");
        std::os::unix::fs::symlink(real.path(), &base_link).expect("symlink base");
        let storage = LocalDeepStorage::new(base_link.clone());

        let src = holder.path().join("src_seg");
        tokio::fs::create_dir_all(&src).await.expect("mkdir");
        tokio::fs::write(src.join("data.bin"), b"via-base-link")
            .await
            .expect("write");

        storage
            .upload_segment("wiki", "seg_base", &src)
            .await
            .expect("upload through a symlinked BASE dir stays supported");
        let dest = holder.path().join("dl");
        storage
            .download_segment("wiki", "seg_base", &dest)
            .await
            .expect("download");
        assert_eq!(
            tokio::fs::read(dest.join("data.bin")).await.expect("read"),
            b"via-base-link"
        );
        storage
            .delete_segment("wiki", "seg_base")
            .await
            .expect("delete");
    }

    // =======================================================================
    // W-C: raw-object fetch + key listing (s3:// migrate-attach source)
    // =======================================================================
    //
    // These exercise the exact `object_store` code path `S3DeepStorage`
    // uses against real S3/MinIO, via the same in-memory store that
    // backs `InMemoryDeepStorage`.

    /// A store with NO configured prefix — the shape `ferrodruid-migrate`
    /// uses for raw Druid deep-storage keys.
    fn raw_store() -> S3DeepStorage {
        S3DeepStorage::with_store(Box::new(InMemory::new()), String::new())
    }

    async fn put_raw(storage: &S3DeepStorage, key: &str, bytes: &[u8]) {
        storage
            .store
            .put(
                &ObjectPath::parse(key).expect("test key parses"),
                PutPayload::from(Bytes::from(bytes.to_vec())),
            )
            .await
            .expect("raw put");
    }

    #[tokio::test]
    async fn s3_fetch_object_to_file_round_trips_bytes() {
        let storage = raw_store();
        let key =
            "druid/segments/wiki/2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z/v1/0/index.zip";
        put_raw(&storage, key, b"ZIP-BYTES").await;

        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("nested").join("index.zip");
        let written = storage
            .fetch_object_to_file(key, &dest, None)
            .await
            .expect("fetch");
        assert_eq!(written, 9, "returns the byte count written");
        assert_eq!(
            tokio::fs::read(&dest).await.expect("read dest"),
            b"ZIP-BYTES",
            "fetched file must be byte-identical to the object"
        );
    }

    #[tokio::test]
    async fn s3_fetch_object_to_file_missing_key_is_loud_and_leaves_no_file() {
        let storage = raw_store();
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("index.zip");
        let err = storage
            .fetch_object_to_file("no/such/key", &dest, None)
            .await
            .expect_err("missing object must be a loud error");
        assert!(matches!(err, DeepStorageError::Other(_)));
        assert!(
            !dest.exists(),
            "no destination file may be left behind for a missing object"
        );
    }

    #[tokio::test]
    async fn s3_fetch_object_to_file_enforces_byte_cap_and_removes_partial() {
        let storage = raw_store();
        put_raw(&storage, "big/index.zip", &vec![0xAB; 4096]).await;

        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("index.zip");
        let err = storage
            .fetch_object_to_file("big/index.zip", &dest, Some(100))
            .await
            .expect_err("an object over the byte cap must be refused");
        let msg = err.to_string();
        assert!(msg.contains("100"), "reason names the cap: {msg}");
        assert!(
            !dest.exists(),
            "the partial download must be removed on a cap breach"
        );

        // At exactly the cap the fetch succeeds.
        storage
            .fetch_object_to_file("big/index.zip", &dest, Some(4096))
            .await
            .expect("an object exactly at the cap fetches");
        assert_eq!(
            tokio::fs::read(&dest).await.expect("read").len(),
            4096,
            "the full object arrived"
        );
    }

    #[tokio::test]
    async fn s3_fetch_object_to_file_rejects_malformed_keys() {
        let storage = raw_store();
        let dir = tempfile::tempdir().expect("tempdir");
        for bad in ["", "a//b", "a/../b", "a/./b"] {
            assert!(
                storage
                    .fetch_object_to_file(bad, &dir.path().join("never-written.bin"), None)
                    .await
                    .is_err(),
                "malformed key {bad:?} must be refused"
            );
        }
    }

    #[tokio::test]
    async fn s3_fetch_object_to_file_rejects_normalizing_keys_before_any_get() {
        let storage = raw_store();
        // The object a silently-NORMALIZED key would resolve to EXISTS:
        // fetching the raw malformed key must still be refused, or the
        // importer would attach a DIFFERENT object under the requesting
        // row's identity (wrong-object substitution, H2 — object_store's
        // `Path::parse` strips a leading/trailing `/`).
        put_raw(&storage, "wiki/index.zip", b"REAL-OBJECT").await;

        let dir = tempfile::tempdir().expect("tempdir");
        for bad in [
            "/wiki/index.zip",     // leading `/` — Path::parse would strip it
            "wiki/index.zip/",     // trailing `/` — ditto
            "wiki//index.zip",     // empty segment
            "wiki/../evil",        // traversal token
            "wiki/./index.zip",    // relative token
            "wiki\\evil",          // backslash
            "wiki/in\u{7}dex.zip", // control character
        ] {
            let dest = dir.path().join("never-written.bin");
            let err = storage
                .fetch_object_to_file(bad, &dest, None)
                .await
                .expect_err("a key that does not name the object byte-for-byte must be refused");
            let msg = err.to_string();
            assert!(
                msg.contains("key"),
                "refusal names the key rule for {bad:?}: {msg}"
            );
            assert!(
                !msg.contains("get wiki") && !msg.contains("get /wiki"),
                "{bad:?} must be refused BEFORE any GET reaches the store: {msg}"
            );
            assert!(!dest.exists(), "no file may be written for {bad:?}");
        }

        // The clean byte-for-byte key still fetches the exact object.
        let dest = dir.path().join("index.zip");
        storage
            .fetch_object_to_file("wiki/index.zip", &dest, None)
            .await
            .expect("the canonical key fetches");
        assert_eq!(tokio::fs::read(&dest).await.expect("read"), b"REAL-OBJECT");
    }

    #[tokio::test]
    async fn s3_fetch_object_to_file_resolves_under_store_prefix() {
        let storage = S3DeepStorage::with_store(Box::new(InMemory::new()), "base/".to_string());
        put_raw(&storage, "base/k/blob.bin", b"under-prefix").await;

        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("blob.bin");
        storage
            .fetch_object_to_file("k/blob.bin", &dest, None)
            .await
            .expect("key resolves under the store's configured prefix");
        assert_eq!(tokio::fs::read(&dest).await.expect("read"), b"under-prefix");
    }

    /// The listed keys as plain strings (assertion convenience).
    fn listed_keys(listed: &[ListedObject]) -> Vec<String> {
        listed.iter().map(|o| o.key().to_string()).collect()
    }

    #[tokio::test]
    async fn s3_list_objects_returns_all_keys_sorted_across_many_objects() {
        let storage = raw_store();
        // Enough keys that a real S3 backend would need >1 ListObjectsV2
        // page (the stream API follows continuation tokens transparently;
        // the MinIO leg of the compat suite proves the paginated path on
        // a real server).
        const N: usize = 1200;
        for i in 0..N {
            put_raw(
                &storage,
                &format!("druid/segments/wiki/iv/v1/{i}/index.zip"),
                b"z",
            )
            .await;
        }
        put_raw(&storage, "other/tree/file.bin", b"x").await;

        let listed = storage
            .list_objects("druid/segments", MAX_S3_KEYS)
            .await
            .expect("list objects");
        let keys = listed_keys(&listed);
        assert_eq!(keys.len(), N, "every key under the prefix is returned");
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "keys come back sorted");
        assert!(
            keys.iter().all(|k| k.starts_with("druid/segments/")),
            "keys outside the prefix must not leak in"
        );

        // A prefix with no objects is an empty listing, not an error.
        let empty = storage
            .list_objects("druid/absent", MAX_S3_KEYS)
            .await
            .expect("list");
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn s3_list_objects_matches_whole_path_segments_only() {
        let storage = raw_store();
        put_raw(&storage, "a/b.bin", b"1").await;
        put_raw(&storage, "ab/c.bin", b"2").await;

        let listed = storage.list_objects("a", MAX_S3_KEYS).await.expect("list");
        assert_eq!(
            listed_keys(&listed),
            vec!["a/b.bin".to_string()],
            "prefix `a` must not match the sibling `ab/` tree"
        );
    }

    #[tokio::test]
    async fn s3_list_objects_composes_with_store_prefix() {
        let storage = S3DeepStorage::with_store(Box::new(InMemory::new()), "segments/".to_string());
        put_raw(&storage, "segments/x/1.bin", b"1").await;
        put_raw(&storage, "segments/x/2.bin", b"2").await;
        put_raw(&storage, "elsewhere/x/3.bin", b"3").await;

        let listed = storage.list_objects("x", MAX_S3_KEYS).await.expect("list");
        assert_eq!(
            listed_keys(&listed),
            vec!["x/1.bin".to_string(), "x/2.bin".to_string()],
            "keys are returned RELATIVE to the store prefix (so a re-fetch \
             through the same store resolves them)"
        );
    }

    // =======================================================================
    // W-C listing→fetch same-object guarantee (wrong-object substitution)
    // =======================================================================
    //
    // `object_store`'s S3 listing converts every RAW key with
    // `Path::parse`, which SILENTLY strips a leading/trailing `/`
    // (verified against object_store 0.12.5: `client/s3.rs` does
    // `location: Path::parse(value.key)?`). [`RawKeyS3Sim`] simulates a
    // real S3 bucket at the RAW-key level so tests can plant objects
    // whose raw keys `object_store` cannot address — the exact scenario
    // the InMemory store cannot express (its `put` takes an
    // already-parsed `Path`).

    /// Simulated S3 backend keyed by RAW S3 keys (which, unlike
    /// [`ObjectPath`], may carry a leading/trailing `/`):
    ///
    /// * `list` mimics object_store's own S3 conversion — every raw key
    ///   goes through `ObjectPath::parse` (silent lead/trail-`/`
    ///   normalization included);
    /// * `get` mimics real S3 GET semantics — EXACT raw-key match only,
    ///   so a raw key carrying a leading/trailing `/` is not
    ///   addressable by any [`ObjectPath`];
    /// * every GET's path is recorded so tests can assert the fetch
    ///   addressed EXACTLY the enumerated path.
    ///
    /// Bytes live in an inner [`InMemory`] under a hex-encoded
    /// (slash-free) alias of the raw key, so `ObjectMeta`/`GetResult`
    /// values come from a real store implementation.
    #[derive(Debug)]
    struct RawKeyS3Sim {
        inner: std::sync::Arc<InMemory>,
        raw_keys: std::sync::Mutex<std::collections::BTreeSet<String>>,
        gets: std::sync::Arc<std::sync::Mutex<Vec<ObjectPath>>>,
        /// Simulate an INCONSISTENT backend whose listing returns
        /// locations outside the requested prefix.
        ignore_list_prefix: bool,
    }

    impl RawKeyS3Sim {
        fn new() -> Self {
            Self {
                inner: std::sync::Arc::new(InMemory::new()),
                raw_keys: std::sync::Mutex::new(std::collections::BTreeSet::new()),
                gets: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
                ignore_list_prefix: false,
            }
        }

        /// A handle onto the GET-path recorder that stays usable after
        /// the sim is boxed into an [`S3DeepStorage`].
        fn gets_handle(&self) -> std::sync::Arc<std::sync::Mutex<Vec<ObjectPath>>> {
            std::sync::Arc::clone(&self.gets)
        }

        /// The slash-free inner alias a raw key's bytes live under.
        fn alias(raw_key: &str) -> ObjectPath {
            ObjectPath::parse(hex::encode(raw_key)).expect("hex alias parses")
        }

        /// Plant an object under its RAW S3 key (leading/trailing `/`
        /// allowed — exactly what a real bucket can hold).
        async fn plant(&self, raw_key: &str, bytes: &[u8]) {
            self.raw_keys
                .lock()
                .expect("raw_keys lock")
                .insert(raw_key.to_string());
            self.inner
                .put(
                    &Self::alias(raw_key),
                    PutPayload::from(Bytes::from(bytes.to_vec())),
                )
                .await
                .expect("plant raw object");
        }
    }

    impl std::fmt::Display for RawKeyS3Sim {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RawKeyS3Sim")
        }
    }

    #[async_trait]
    impl ObjectStore for RawKeyS3Sim {
        async fn put_opts(
            &self,
            _location: &ObjectPath,
            _payload: PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn put_multipart_opts(
            &self,
            _location: &ObjectPath,
            _opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            Err(object_store::Error::NotImplemented)
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.gets.lock().expect("gets lock").push(location.clone());
            // Real S3 GET semantics: the request names a raw key
            // byte-for-byte. A raw key that differs (e.g. by a trailing
            // `/`) is a DIFFERENT object and is NOT returned.
            let known = self
                .raw_keys
                .lock()
                .expect("raw_keys lock")
                .contains(location.as_ref());
            if !known {
                return Err(object_store::Error::NotFound {
                    path: location.to_string(),
                    source: "no raw S3 key with exactly this name".into(),
                });
            }
            let mut result = self
                .inner
                .get_opts(&Self::alias(location.as_ref()), options)
                .await?;
            result.meta.location = location.clone();
            Ok(result)
        }

        async fn delete(&self, _location: &ObjectPath) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            use futures::StreamExt as _;
            // Real S3 listing: the prefix is matched against RAW keys
            // as a string prefix (object_store sends `prefix=<path>/`).
            let want = match prefix {
                Some(p) if !self.ignore_list_prefix => format!("{}/", p.as_ref()),
                _ => String::new(),
            };
            let raws: Vec<String> = self
                .raw_keys
                .lock()
                .expect("raw_keys lock")
                .iter()
                .filter(|k| want.is_empty() || k.starts_with(&want))
                .cloned()
                .collect();
            let inner = std::sync::Arc::clone(&self.inner);
            futures::stream::iter(raws)
                .then(move |raw| {
                    let inner = std::sync::Arc::clone(&inner);
                    async move {
                        // EXACTLY what object_store's S3 client does with
                        // every listed raw key (client/s3.rs):
                        // `location: Path::parse(value.key)?` — the parse
                        // silently strips a leading/trailing `/`.
                        let location = ObjectPath::parse(&raw)?;
                        let meta_template = inner.head(&Self::alias(&raw)).await?;
                        Ok(object_store::ObjectMeta {
                            location,
                            ..meta_template
                        })
                    }
                })
                .boxed()
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn copy(&self, _from: &ObjectPath, _to: &ObjectPath) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        async fn copy_if_not_exists(
            &self,
            _from: &ObjectPath,
            _to: &ObjectPath,
        ) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }
    }

    /// H2 sibling (listing-derived keys): a raw trailing-slash object
    /// (`…/index.zip/`) is listed by object_store under its NORMALIZED
    /// alias (`…/index.zip`) — the very key of a DISTINCT sibling
    /// object. The listing→fetch pipeline must never silently stage the
    /// sibling's bytes for the enumerated entry: the collision must be
    /// refused loudly.
    ///
    /// RED (pre-fix) witness, 2026-07-20: `list_keys` returned the
    /// alias silently and `fetch_object_to_file` staged the DISTINCT
    /// sibling's bytes (`DECOY-SIBLING`) with no error.
    #[tokio::test]
    async fn s3_listing_normalization_collision_is_never_a_silent_substitution() {
        let sim = RawKeyS3Sim::new();
        // The DISTINCT sibling the normalized alias points at…
        sim.plant("p/wiki/iv/v1/0/index.zip", b"DECOY-SIBLING")
            .await;
        // …and the real object whose raw key object_store cannot
        // address (trailing slash).
        sim.plant("p/wiki/iv/v1/0/index.zip/", b"ENUMERATED-REAL")
            .await;
        let storage = S3DeepStorage::with_store(Box::new(sim), String::new());

        // The listing enumerates the SAME normalized path twice — two
        // distinct raw objects collapsed onto one address. Fetching
        // that address can only ever return ONE of them, so one entry
        // is guaranteed to get the OTHER object's bytes: this listing
        // must be refused loudly, never handed to the fetch loop.
        let err = storage
            .list_objects("p", MAX_S3_KEYS)
            .await
            .expect_err("a normalization collision must refuse the listing");
        let msg = err.to_string();
        assert!(
            msg.contains("p/wiki/iv/v1/0/index.zip"),
            "the refusal names the colliding path: {msg}"
        );
        assert!(
            msg.contains("leading/trailing"),
            "the refusal explains the normalization collision: {msg}"
        );
    }

    /// The same class through the store-prefix round-trip: a listed
    /// location OUTSIDE the configured store prefix must be a loud
    /// listing error — the old string fallback returned the full raw
    /// key, which a later fetch re-prefixed into a DIFFERENT address
    /// (wrong-object substitution against an inconsistent backend).
    ///
    /// RED (pre-fix) witness, 2026-07-20: `list_keys` fell back to the
    /// full raw key and `fetch_object_to_file` re-prefixed it, staging
    /// `DECOY-REPREFIXED` — a different object than enumerated — with
    /// no error.
    #[tokio::test]
    async fn s3_listing_location_outside_store_prefix_is_loud_never_refetched_elsewhere() {
        let mut sim = RawKeyS3Sim::new();
        sim.ignore_list_prefix = true; // inconsistent backend
        // The out-of-prefix location the backend leaks into the listing…
        sim.plant("elsewhere/secret.bin", b"ENUMERATED-REAL").await;
        // …and the object sitting where the string round-trip
        // (strip fails → full key → fetch re-prefixes) would point.
        sim.plant("base/elsewhere/secret.bin", b"DECOY-REPREFIXED")
            .await;
        let storage = S3DeepStorage::with_store(Box::new(sim), "base/".to_string());

        let err = storage
            .list_objects("", MAX_S3_KEYS)
            .await
            .expect_err("an out-of-prefix listed location must refuse the listing");
        let msg = err.to_string();
        assert!(
            msg.contains("prefix") && msg.contains("elsewhere/secret.bin"),
            "the refusal names the prefix inconsistency and the location: {msg}"
        );
    }

    /// The listing→fetch pipeline addresses ONE object: the path GETted
    /// is the very path enumerated (asserted at the store boundary via
    /// the mock's GET recorder), and the staged bytes are that exact
    /// object's.
    #[tokio::test]
    async fn s3_fetch_listed_to_file_addresses_exactly_the_enumerated_path() {
        let sim = RawKeyS3Sim::new();
        sim.plant("p/wiki/iv/v1/0/index.zip", b"REAL-SEGMENT").await;
        sim.plant("p/wiki/iv/v1/0/descriptor.json", b"{}").await;
        let gets = sim.gets_handle();
        let storage = S3DeepStorage::with_store(Box::new(sim), String::new());

        let listed = storage.list_objects("p", MAX_S3_KEYS).await.expect("list");
        assert_eq!(
            listed_keys(&listed),
            vec![
                "p/wiki/iv/v1/0/descriptor.json".to_string(),
                "p/wiki/iv/v1/0/index.zip".to_string(),
            ]
        );

        let dir = tempfile::tempdir().expect("tempdir");
        let mut fetched_under: Vec<ObjectPath> = Vec::new();
        for (i, obj) in listed.iter().enumerate() {
            let dest = dir.path().join(format!("obj-{i}"));
            storage
                .fetch_listed_to_file(obj, &dest, None)
                .await
                .expect("listed object fetches");
            fetched_under.push(obj.path.clone());
        }
        assert_eq!(
            tokio::fs::read(dir.path().join("obj-1"))
                .await
                .expect("read"),
            b"REAL-SEGMENT",
            "the staged bytes are the enumerated object's"
        );

        // The store received GETs for EXACTLY the enumerated paths —
        // no re-parsed / re-prefixed address ever reaches the backend.
        assert_eq!(
            gets.lock().expect("gets lock").clone(),
            fetched_under,
            "every GET addressed the very path the listing enumerated"
        );
    }

    /// Alias-only case: when ONLY the raw trailing-slash object exists,
    /// its normalized alias is enumerated (object_store's conversion —
    /// nothing this crate can see through) but the fetch must fail
    /// LOUDLY, naming the addressability residual — never resolve to
    /// some other object, and never leave a file behind.
    #[tokio::test]
    async fn s3_fetch_listed_alias_of_unaddressable_object_fails_loud_leaves_no_file() {
        let sim = RawKeyS3Sim::new();
        sim.plant("p/wiki/iv/v1/0/index.zip/", b"UNADDRESSABLE")
            .await;
        let storage = S3DeepStorage::with_store(Box::new(sim), String::new());

        let listed = storage.list_objects("p", MAX_S3_KEYS).await.expect("list");
        assert_eq!(
            listed_keys(&listed),
            vec!["p/wiki/iv/v1/0/index.zip".to_string()],
            "the alias is enumerated (normalization happens inside object_store)"
        );

        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("index.zip");
        let err = storage
            .fetch_listed_to_file(&listed[0], &dest, None)
            .await
            .expect_err("the alias of an unaddressable raw key must fail loudly");
        let msg = err.to_string();
        assert!(
            msg.contains("leading/trailing"),
            "the error names the addressability residual: {msg}"
        );
        assert!(!dest.exists(), "no file may be left behind");
    }

    // =======================================================================
    // W-C listing memory bound (streaming key cap)
    // =======================================================================

    /// A backend whose listing WOULD yield `total` synthetic objects,
    /// counting every entry the consumer actually pulls off the stream.
    /// The stream is lazy (entries are fabricated per poll), so the
    /// recorded count proves whether [`S3DeepStorage::list_objects`]
    /// stopped consuming at the cap or drained the whole "bucket" into
    /// memory first.
    #[derive(Debug)]
    struct HugeListSim {
        /// A real [`ObjectMeta`] template (from an [`InMemory`] `head`)
        /// so fabricated entries carry authentic metadata fields.
        meta_template: object_store::ObjectMeta,
        /// How many objects the simulated bucket holds.
        total: usize,
        /// Entries the consumer has actually pulled off the stream.
        yielded: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl HugeListSim {
        async fn new(total: usize) -> Self {
            let inner = InMemory::new();
            let path = ObjectPath::parse("template").expect("template key parses");
            inner
                .put(&path, PutPayload::from(Bytes::from_static(b"x")))
                .await
                .expect("plant template object");
            let meta_template = inner.head(&path).await.expect("head template");
            Self {
                meta_template,
                total,
                yielded: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        /// A handle onto the pull counter that stays usable after the
        /// sim is boxed into an [`S3DeepStorage`].
        fn yielded_handle(&self) -> std::sync::Arc<std::sync::atomic::AtomicUsize> {
            std::sync::Arc::clone(&self.yielded)
        }
    }

    impl std::fmt::Display for HugeListSim {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "HugeListSim")
        }
    }

    #[async_trait]
    impl ObjectStore for HugeListSim {
        async fn put_opts(
            &self,
            _location: &ObjectPath,
            _payload: PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn put_multipart_opts(
            &self,
            _location: &ObjectPath,
            _opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            Err(object_store::Error::NotImplemented)
        }

        async fn get_opts(
            &self,
            _location: &ObjectPath,
            _options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn delete(&self, _location: &ObjectPath) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        fn list(
            &self,
            _prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            use futures::StreamExt as _;
            let template = self.meta_template.clone();
            let yielded = std::sync::Arc::clone(&self.yielded);
            futures::stream::iter(0..self.total)
                .map(move |i| {
                    yielded.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(object_store::ObjectMeta {
                        location: ObjectPath::from(format!("p/obj-{i:08}")),
                        ..template.clone()
                    })
                })
                .boxed()
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn copy(&self, _from: &ObjectPath, _to: &ObjectPath) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        async fn copy_if_not_exists(
            &self,
            _from: &ObjectPath,
            _to: &ObjectPath,
        ) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }
    }

    /// H (memory exhaustion): a prefix with more objects than the cap
    /// must abort the listing AT the cap — pulling exactly cap+1
    /// entries off the stream, never draining the bucket into RAM —
    /// with a loud error naming the cap.
    ///
    /// RED (pre-fix) witness, 2026-07-21: `list_objects` had no bound
    /// at all — it `try_collect`ed the ENTIRE listing (all 200_000
    /// simulated entries pulled) and returned `Ok`, so a hostile/broad
    /// prefix exhausted memory before the planner's after-the-fact cap
    /// check could fire.
    #[tokio::test]
    async fn s3_list_objects_over_cap_aborts_stream_at_cap_plus_one() {
        const CAP: usize = 100;
        // The simulated bucket holds VASTLY more objects than the cap;
        // the stream is lazy, so only what the consumer pulls exists.
        let sim = HugeListSim::new(200_000).await;
        let yielded = sim.yielded_handle();
        let storage = S3DeepStorage::with_store(Box::new(sim), String::new());

        let err = storage
            .list_objects("p", CAP)
            .await
            .expect_err("an over-cap listing must be refused, not collected");
        let msg = err.to_string();
        assert!(
            msg.contains(&CAP.to_string()) && msg.contains("cap"),
            "the refusal names the key cap: {msg}"
        );
        assert_eq!(
            yielded.load(std::sync::atomic::Ordering::SeqCst),
            CAP + 1,
            "the consumer must stop at entry cap+1 — NOT drain the whole \
             listing into memory before erroring"
        );
    }

    /// Boundary: a listing with EXACTLY `max_keys` objects succeeds
    /// unchanged (the cap refuses cap+1, not cap).
    #[tokio::test]
    async fn s3_list_objects_at_cap_boundary_succeeds() {
        let sim = HugeListSim::new(100).await;
        let storage = S3DeepStorage::with_store(Box::new(sim), String::new());

        let listed = storage
            .list_objects("p", 100)
            .await
            .expect("an exactly-at-cap listing is fine");
        assert_eq!(listed.len(), 100);
    }
}
