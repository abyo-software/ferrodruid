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

#[async_trait]
impl DeepStorage for LocalDeepStorage {
    async fn list_segments(&self, data_source: &str) -> Result<Vec<String>> {
        validate_path_component("data source", data_source)?;
        let ds_dir = self.base_dir.join(data_source);
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
        let src_dir = self.segment_path(data_source, segment_id)?;
        if !src_dir.exists() {
            return Err(DeepStorageError::NotFound {
                data_source: data_source.to_string(),
                segment_id: segment_id.to_string(),
            });
        }

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
        let dest_dir = self.segment_path(data_source, segment_id)?;
        tokio::fs::create_dir_all(&dest_dir).await?;

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
            tokio::fs::copy(src, &dest_file).await?;
            fsync_file(&dest_file).await?;
            written.insert(std::ffi::OsString::from("segment.bin"));
        }

        // REPLACE semantics (Codex R22 H1): prune every regular file in the
        // destination that THIS upload did not write. A crash after an upload
        // but before the metadata commit leaves an ORPHAN blob that
        // `allocate_segment_id` cannot see (it consults only metadata +
        // historical), so the id can be reused for a SMALLER artifact; with
        // merge semantics the orphan's surplus files survived alongside the
        // new upload, dest != src, and the staging-computed content hash
        // (R17 H2) failed against the reloaded dest → bootstrap abort. The
        // orphan is by definition UNCOMMITTED (a committed id is rejected by
        // the collision check), so deleting its leftovers is safe. Runs after
        // the copies and BEFORE `fsync_chain` so the removed dentries are
        // covered by the directory fsyncs below; a crash mid-prune merely
        // leaves another uncommitted orphan for the next upload to replace.
        let mut dest_entries = tokio::fs::read_dir(&dest_dir).await?;
        while let Some(entry) = dest_entries.next_entry().await? {
            if entry.file_type().await?.is_file() && !written.contains(&entry.file_name()) {
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
        let seg_dir = self.segment_path(data_source, segment_id)?;
        if !seg_dir.exists() {
            // Idempotent: nothing to delete is success, not error.
            return Ok(());
        }
        tokio::fs::remove_dir_all(&seg_dir).await?;
        Ok(())
    }

    async fn segment_exists(&self, data_source: &str, segment_id: &str) -> Result<bool> {
        let seg_dir = self.segment_path(data_source, segment_id)?;
        Ok(seg_dir.exists())
    }

    fn backend_type(&self) -> &'static str {
        "local"
    }
}

// ---------------------------------------------------------------------------
// S3DeepStorage
// ---------------------------------------------------------------------------

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
}
