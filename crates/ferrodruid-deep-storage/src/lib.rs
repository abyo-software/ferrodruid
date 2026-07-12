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

    /// Compute the on-disk path for a segment.
    fn segment_path(&self, data_source: &str, segment_id: &str) -> PathBuf {
        self.base_dir.join(data_source).join(segment_id)
    }
}

#[async_trait]
impl DeepStorage for LocalDeepStorage {
    async fn list_segments(&self, data_source: &str) -> Result<Vec<String>> {
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
        let src_dir = self.segment_path(data_source, segment_id);
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
        let dest_dir = self.segment_path(data_source, segment_id);
        tokio::fs::create_dir_all(&dest_dir).await?;

        if src.is_dir() {
            // Copy all files from src directory.
            let mut entries = tokio::fs::read_dir(src).await?;
            while let Some(entry) = entries.next_entry().await? {
                let dest_file = dest_dir.join(entry.file_name());
                tokio::fs::copy(entry.path(), dest_file).await?;
            }
        } else {
            // Single file — copy it as segment.bin.
            let dest_file = dest_dir.join("segment.bin");
            tokio::fs::copy(src, dest_file).await?;
        }

        Ok(())
    }

    async fn delete_segment(&self, data_source: &str, segment_id: &str) -> Result<()> {
        let seg_dir = self.segment_path(data_source, segment_id);
        if !seg_dir.exists() {
            // Idempotent: nothing to delete is success, not error.
            return Ok(());
        }
        tokio::fs::remove_dir_all(&seg_dir).await?;
        Ok(())
    }

    async fn segment_exists(&self, data_source: &str, segment_id: &str) -> Result<bool> {
        let seg_dir = self.segment_path(data_source, segment_id);
        Ok(seg_dir.exists())
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
        }

        Ok(())
    }

    async fn delete_segment(&self, data_source: &str, segment_id: &str) -> Result<()> {
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
        let prefix = self.segment_prefix(data_source, segment_id);
        let mut listing = self.store.list(Some(&prefix));
        // If we can get at least one object the segment exists.
        match listing.try_next().await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(DeepStorageError::Other(e.to_string())),
        }
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
}
