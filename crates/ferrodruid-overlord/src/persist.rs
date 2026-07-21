// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Deep-storage persistence seam shared by both publish tails (compat-3
//! stage 1).
//!
//! Before a segment's metadata row is committed, [`persist_segment`] writes
//! the [`SegmentData`] to a v9 smoosh directory in a private staging tempdir
//! and uploads it to the configured [`DeepStorage`] backend under
//! `<base>/<data_source>/<segment_id>/`. The publish sequence is therefore
//! ordered **P (persist) → M (metadata) → swap**: the crash-consistency
//! invariant is that a segment's metadata row is only ever committed AFTER a
//! successful upload, so a durable metadata row always has a durable blob to
//! reload from on the next startup ([`crate::Overlord::bootstrap_reload_segments`]).
//!
//! The returned [`LoadSpec`] is a forward-compatible marker stamped into the
//! metadata row's `payload.loadSpec`. The bootstrap reload does not parse it
//! yet — it downloads by `(data_source, segment_id)` directly — so it is
//! informational provenance for now.
//!
//! `#![forbid(unsafe_code)]` is inherited from the crate root; the writer
//! uses buffered `write_segment_v9` (no `memmap`), matching the in-heap
//! segment residency of the rest of the product.

use std::path::Path;

use ferrodruid_common::{DruidError, Result};
use ferrodruid_deep_storage::DeepStorage;
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::writer::write_segment_v9;
use serde::{Deserialize, Serialize};

/// Forward-compatible descriptor of where a persisted segment's blob lives,
/// stamped into the metadata row's `payload.loadSpec`.
///
/// Deliberately minimal: `{ "type": <backend>, "dataSource": <ds>,
/// "segmentId": <id> }`. The bootstrap reload keys on `(dataSource,
/// segmentId)` directly, so this is a marker a future loadSpec-driven loader
/// can grow into without changing the publish path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadSpec {
    /// Deep-storage backend identifier ([`DeepStorage::backend_type`]).
    #[serde(rename = "type")]
    pub(crate) backend: String,
    /// Data source the segment belongs to.
    #[serde(rename = "dataSource")]
    pub(crate) data_source: String,
    /// Segment identifier (unique within the data source).
    #[serde(rename = "segmentId")]
    pub(crate) segment_id: String,
    /// Lowercase-hex SHA-256 content hash of the persisted v9 blob
    /// ([`blob_content_hash`]), stamped so the bootstrap reload can verify
    /// the downloaded blob's content identity and fail loud on a swapped or
    /// silently-corrupted durable segment (H2).
    #[serde(rename = "sha256")]
    pub(crate) sha256: String,
}

impl LoadSpec {
    /// Render as the `payload.loadSpec` JSON value.
    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": self.backend,
            "dataSource": self.data_source,
            "segmentId": self.segment_id,
            "sha256": self.sha256,
        })
    }
}

/// Compute a deterministic SHA-256 content hash over every regular file in a
/// v9 blob directory `dir`, returned as a lowercase-hex string.
///
/// Thin adapter over the single shared implementation,
/// [`ferrodruid_deep_storage::blob_content_hash`] (it lives beside the
/// upload/download code whose file-set semantics it mirrors, and is also
/// used by `ferrodruid-migrate attach`): this wrapper only maps the error
/// into [`DruidError::Segment`] for the publish/bootstrap call sites. See
/// the shared function for the determinism + integrity (H2) contract.
///
/// # Errors
///
/// Fails if `dir` cannot be read or a contained file's bytes cannot be loaded.
pub(crate) fn blob_content_hash(dir: &Path) -> Result<String> {
    ferrodruid_deep_storage::blob_content_hash(dir).map_err(|e| DruidError::Segment(e.to_string()))
}

/// Bounded upload attempts for transient backend errors (S3 5xx / throttling).
///
/// Correctness does not depend on this — a total upload failure fails the
/// whole publish BEFORE any metadata row is committed (the P→M→swap
/// invariant), so a caller simply retries the task. The bounded retry only
/// smooths over transient object-store hiccups.
const UPLOAD_ATTEMPTS: u32 = 3;

/// Persist `segment_data` to deep storage under `(data_source, segment_id)`
/// and return its [`LoadSpec`] descriptor.
///
/// Steps:
///   1. stage the segment as a v9 smoosh directory (`meta.smoosh` + chunk
///      files) in a private, auto-cleaned tempdir (never a predictable raw
///      `/tmp` path);
///   2. upload the staging directory to the backend (which copies it to
///      `<base>/<data_source>/<segment_id>/`), retrying transient failures a
///      bounded number of times;
///   3. return the `{ type, dataSource, segmentId }` marker.
///
/// # Errors
///
/// Fails if the segment cannot be written locally or if the upload does not
/// succeed within [`UPLOAD_ATTEMPTS`]. On ANY error nothing durable was
/// committed downstream: the caller must NOT commit the metadata row — this
/// is what upholds the crash-consistency invariant (a metadata row only ever
/// follows a successful upload).
pub(crate) async fn persist_segment(
    deep_storage: &dyn DeepStorage,
    data_source: &str,
    segment_id: &str,
    segment_data: &SegmentData,
) -> Result<LoadSpec> {
    // 1. Stage into a private tempdir. `tempfile::tempdir()` creates a
    //    unique, 0700, auto-cleaned directory — not a hand-rolled,
    //    guessable `/tmp/<id>` path. The segment is written to a SUBDIR so
    //    `write_segment_v9`'s sibling staging + atomic rename stays inside
    //    the tempdir (and is cleaned up with it).
    let staging = tempfile::tempdir()
        .map_err(|e| DruidError::Segment(format!("persist: create staging tempdir: {e}")))?;
    let seg_dir = staging.path().join("v9");
    write_segment_v9(segment_data, &seg_dir).map_err(|e| {
        DruidError::Segment(format!(
            "persist: write segment '{segment_id}' (data source '{data_source}') to staging: {e}"
        ))
    })?;

    // Content-identity hash of the exact bytes about to be uploaded (H2).
    // Computed from the staged dir so it covers precisely the file set the
    // backend stores — the bootstrap reload recomputes it over the
    // re-downloaded dir and refuses to serve a blob whose hash has changed.
    let sha256 = blob_content_hash(&seg_dir).map_err(|e| {
        DruidError::Segment(format!(
            "persist: hash segment '{segment_id}' (data source '{data_source}'): {e}"
        ))
    })?;

    // 2. Upload to deep storage with bounded retries for transient errors.
    let mut last_err: Option<String> = None;
    for attempt in 1..=UPLOAD_ATTEMPTS {
        match deep_storage
            .upload_segment(data_source, segment_id, &seg_dir)
            .await
        {
            Ok(()) => {
                return Ok(LoadSpec {
                    backend: deep_storage.backend_type().to_string(),
                    data_source: data_source.to_string(),
                    segment_id: segment_id.to_string(),
                    sha256: sha256.clone(),
                });
            }
            Err(e) => {
                if attempt < UPLOAD_ATTEMPTS {
                    tracing::warn!(
                        data_source,
                        segment_id,
                        attempt,
                        error = %e,
                        "persist: deep-storage upload failed; retrying (nothing committed yet)",
                    );
                }
                last_err = Some(e.to_string());
            }
        }
    }
    Err(DruidError::Ingestion(format!(
        "persist: could not upload segment '{segment_id}' (data source '{data_source}') to deep \
         storage after {UPLOAD_ATTEMPTS} attempts (no metadata committed): {}",
        last_err.unwrap_or_else(|| "unknown error".to_string()),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use ferrodruid_deep_storage::LocalDeepStorage;
    use ferrodruid_ingest_batch::BatchIngester;
    use serde_json::json;

    fn sample_segment() -> SegmentData {
        let ingester = BatchIngester::new(
            "persist_ds".to_string(),
            "__time".to_string(),
            vec!["page".to_string()],
            vec![],
        );
        const BASE_MS: i64 = 1_700_000_000_000;
        ingester
            .ingest(vec![
                json!({ "__time": BASE_MS + 1, "page": "a" }),
                json!({ "__time": BASE_MS + 2, "page": "b" }),
                json!({ "__time": BASE_MS + 3, "page": "a" }),
            ])
            .expect("ingest sample rows")
            .segment_data
    }

    /// Round-trip: persist a segment, then re-download it and re-open it —
    /// the reloaded [`SegmentData`] must match the original (columns, rows).
    #[tokio::test]
    async fn persist_then_download_round_trips() {
        let base = tempfile::tempdir().expect("base dir");
        let storage = LocalDeepStorage::new(base.path().to_path_buf());
        let original = sample_segment();

        let spec = persist_segment(&storage, "persist_ds", "seg_rt", &original)
            .await
            .expect("persist");
        assert_eq!(spec.backend, "local");
        assert_eq!(spec.data_source, "persist_ds");
        assert_eq!(spec.segment_id, "seg_rt");
        assert_eq!(
            spec.sha256.len(),
            64,
            "sha256 is recorded as 64 hex chars, got {:?}",
            spec.sha256
        );

        assert!(
            storage
                .segment_exists("persist_ds", "seg_rt")
                .await
                .expect("exists")
        );

        let dl = tempfile::tempdir().expect("dl dir");
        let dest = dl.path().join("seg");
        Arc::new(storage)
            .download_segment("persist_ds", "seg_rt", &dest)
            .await
            .expect("download");

        // H2: the content hash recomputed over the RE-DOWNLOADED blob must
        // equal the hash stamped at persist time — the round-trip is stable,
        // so a legitimate reload is never mistaken for a swap.
        let redownloaded_hash = blob_content_hash(&dest).expect("re-hash downloaded blob");
        assert_eq!(
            redownloaded_hash, spec.sha256,
            "re-downloaded blob hash must match the stamped hash"
        );

        let reloaded = SegmentData::open(&dest).expect("reopen persisted segment");

        assert_eq!(reloaded.num_rows, original.num_rows);
        assert_eq!(reloaded.dimensions, original.dimensions);
        assert_eq!(reloaded.metrics, original.metrics);
        assert_eq!(
            reloaded.timestamp_column().expect("ts").len(),
            original.timestamp_column().expect("ts").len()
        );
    }

    /// A failed upload surfaces as `Err` (so the caller never commits a
    /// metadata row) and leaves no segment behind. Modeled by uploading into
    /// a read-only base directory.
    #[tokio::test]
    #[cfg(unix)]
    async fn persist_upload_failure_is_error() {
        use std::os::unix::fs::PermissionsExt;

        let base = tempfile::tempdir().expect("base dir");
        // Make the base read-only so `create_dir_all(<base>/<ds>/<id>)`
        // fails inside `upload_segment`.
        let mut perms = std::fs::metadata(base.path()).expect("meta").permissions();
        perms.set_mode(0o500);
        std::fs::set_permissions(base.path(), perms).expect("chmod ro");

        let storage = LocalDeepStorage::new(base.path().join("nested").join("ro"));
        let result = persist_segment(&storage, "persist_ds", "seg_fail", &sample_segment()).await;
        assert!(result.is_err(), "upload into a read-only base must fail");

        // Restore perms so the tempdir can be cleaned up.
        let mut perms = std::fs::metadata(base.path()).expect("meta").permissions();
        perms.set_mode(0o700);
        std::fs::set_permissions(base.path(), perms).expect("chmod rw");
    }

    /// [`blob_content_hash`] is a deterministic 64-hex-char digest that is
    /// independent of the file-write order but SENSITIVE to any content or
    /// file-set change — the two properties the H2 integrity check relies on.
    #[test]
    fn blob_content_hash_is_deterministic_and_content_sensitive() {
        // Two dirs with the SAME files written in the OPPOSITE order must hash
        // identically (order-independence).
        let a = tempfile::tempdir().expect("dir a");
        std::fs::write(a.path().join("00000.smoosh"), b"chunk-bytes").expect("w");
        std::fs::write(a.path().join("meta.smoosh"), b"meta-bytes").expect("w");

        let b = tempfile::tempdir().expect("dir b");
        std::fs::write(b.path().join("meta.smoosh"), b"meta-bytes").expect("w");
        std::fs::write(b.path().join("00000.smoosh"), b"chunk-bytes").expect("w");

        let ha = blob_content_hash(a.path()).expect("hash a");
        let hb = blob_content_hash(b.path()).expect("hash b");
        assert_eq!(ha.len(), 64, "sha256 hex length");
        assert_eq!(ha, hb, "identical content hashes regardless of write order");

        // A one-byte change to a file flips the hash (silent corruption).
        let c = tempfile::tempdir().expect("dir c");
        std::fs::write(c.path().join("00000.smoosh"), b"chunk-bytez").expect("w");
        std::fs::write(c.path().join("meta.smoosh"), b"meta-bytes").expect("w");
        assert_ne!(blob_content_hash(c.path()).expect("hash c"), ha);

        // Length-framing: moving a byte across the file boundary (so the naive
        // concatenation is unchanged) still flips the hash.
        let d = tempfile::tempdir().expect("dir d");
        std::fs::write(d.path().join("00000.smoosh"), b"chunk-byte").expect("w");
        std::fs::write(d.path().join("meta.smoosh"), b"smeta-bytes").expect("w");
        assert_ne!(
            blob_content_hash(d.path()).expect("hash d"),
            ha,
            "length-framed hash is not fooled by re-partitioning the byte stream"
        );
    }
}
