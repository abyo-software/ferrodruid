// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Real MinIO ⇄ FerroDruid `S3DeepStorage` end-to-end integration tests.
//!
//! Marked `#[ignore]` so they are excluded from default `cargo test`.
//! Run via the docker-compose-backed driver:
//!
//! ```sh
//! tests/deep-storage-compat/run_minio_e2e.sh
//! ```
//!
//! Each test:
//!
//! * Builds an `S3DeepStorage` against `FERRODRUID_MINIO_ENDPOINT`.
//! * Uses a unique per-test prefix so tests run independently against a
//!   shared `ferrodruid-compat` bucket.
//! * Tears down its own prefix at the end so the bucket stays clean.
//!
//! The four required CL-2 operations (read / write / list / delete) are
//! each exercised, plus list pagination across >1000 segments and
//! delete idempotency.

#![cfg(test)]
#![forbid(unsafe_code)]

mod common;

use ferrodruid_deep_storage::DeepStorage;

const DS: &str = "wiki";

/// Skip the body of an `#[ignore]`d test (so it still runs *something*
/// when invoked without `--ignored`, and prints a clear "skipped" line
/// rather than silently doing nothing).
macro_rules! require_minio {
    ($prefix:expr) => {
        match common::minio_storage($prefix) {
            Some(s) => s,
            None => {
                eprintln!(
                    "[skip] FERRODRUID_MINIO_ENDPOINT not set; run via tests/deep-storage-compat/run_minio_e2e.sh"
                );
                return;
            }
        }
    };
}

#[tokio::test]
#[ignore = "requires running MinIO (use run_minio_e2e.sh)"]
async fn minio_round_trip_single_segment() {
    let prefix = common::unique_prefix("round-trip");
    let storage = require_minio!(&prefix);
    common::purge_prefix(&storage, DS).await;

    // --- write ---
    let dir = tempfile::tempdir().expect("tempdir");
    let src = common::make_dir(dir.path(), "src").await;
    common::write_file(&src, "data.bin", b"hello-minio").await;
    common::write_file(&src, "meta.json", br#"{"v":1}"#).await;

    storage
        .upload_segment(DS, "seg_round", &src)
        .await
        .expect("upload");

    // --- list ---
    let segments = storage.list_segments(DS).await.expect("list");
    assert!(
        segments.contains(&"seg_round".to_string()),
        "uploaded segment must appear in listing; got {segments:?}",
    );

    // --- segment_exists ---
    assert!(
        storage
            .segment_exists(DS, "seg_round")
            .await
            .expect("exists"),
        "segment_exists must return true after upload",
    );

    // --- read (download) ---
    let dest = common::make_dir(dir.path(), "dest").await;
    storage
        .download_segment(DS, "seg_round", &dest)
        .await
        .expect("download");
    let data = tokio::fs::read(dest.join("data.bin"))
        .await
        .expect("read data.bin");
    let meta = tokio::fs::read(dest.join("meta.json"))
        .await
        .expect("read meta.json");
    assert_eq!(&data, b"hello-minio");
    assert_eq!(&meta, br#"{"v":1}"#);

    // --- delete ---
    storage
        .delete_segment(DS, "seg_round")
        .await
        .expect("delete");
    assert!(
        !storage
            .segment_exists(DS, "seg_round")
            .await
            .expect("exists after delete"),
        "segment must be gone after delete",
    );

    common::purge_prefix(&storage, DS).await;
}

#[tokio::test]
#[ignore = "requires running MinIO (use run_minio_e2e.sh)"]
async fn minio_delete_is_idempotent() {
    let prefix = common::unique_prefix("idempotent");
    let storage = require_minio!(&prefix);
    common::purge_prefix(&storage, DS).await;

    // First case: delete a never-existed segment.
    storage
        .delete_segment(DS, "never_existed")
        .await
        .expect("delete-of-missing must succeed (idempotent)");

    // Second case: upload then delete twice.
    let dir = tempfile::tempdir().expect("tempdir");
    let src = common::make_dir(dir.path(), "src").await;
    common::write_file(&src, "data.bin", b"x").await;

    storage
        .upload_segment(DS, "seg_idem", &src)
        .await
        .expect("upload");
    storage
        .delete_segment(DS, "seg_idem")
        .await
        .expect("first delete");
    storage
        .delete_segment(DS, "seg_idem")
        .await
        .expect("second delete must succeed (idempotent)");

    common::purge_prefix(&storage, DS).await;
}

#[tokio::test]
#[ignore = "requires running MinIO (use run_minio_e2e.sh)"]
async fn minio_list_pagination_above_1000_segments() {
    // S3 ListObjectsV2 caps each response page at 1000 keys; this test
    // proves `list_segments` correctly follows continuation tokens by
    // creating more segments than fit in one page.
    let prefix = common::unique_prefix("pagination");
    let storage = require_minio!(&prefix);
    common::purge_prefix(&storage, DS).await;

    const N: usize = 1100;

    // Stage source dir once; reuse for all uploads to avoid filesystem churn.
    let dir = tempfile::tempdir().expect("tempdir");
    let src = common::make_dir(dir.path(), "src").await;
    common::write_file(&src, "data.bin", b"x").await;

    // Upload in modest concurrency batches to keep the run under ~30s on
    // localhost MinIO without overloading the single-node server.
    use futures::stream::{self, StreamExt};
    let upload_results: Vec<_> = stream::iter(0..N)
        .map(|i| {
            let storage = &storage;
            let src = src.clone();
            async move {
                let seg_id = format!("seg_{i:06}");
                storage.upload_segment(DS, &seg_id, &src).await
            }
        })
        .buffer_unordered(16)
        .collect()
        .await;
    for r in upload_results {
        r.expect("upload");
    }

    let listed = storage.list_segments(DS).await.expect("list");
    assert_eq!(
        listed.len(),
        N,
        "list_segments must return all {N} segments across paginated responses",
    );

    // Spot-check a known segment id appears.
    assert!(
        listed.contains(&"seg_001050".to_string()),
        "expected seg_001050 in listing",
    );

    common::purge_prefix(&storage, DS).await;
}

#[tokio::test]
#[ignore = "requires running MinIO (use run_minio_e2e.sh)"]
async fn minio_multi_file_segment_round_trip() {
    let prefix = common::unique_prefix("multi-file");
    let storage = require_minio!(&prefix);
    common::purge_prefix(&storage, DS).await;

    let dir = tempfile::tempdir().expect("tempdir");
    let src = common::make_dir(dir.path(), "src").await;

    // Multiple files of varying sizes to make sure download preserves each.
    common::write_file(&src, "data.bin", &vec![0xAB; 4096]).await;
    common::write_file(&src, "index.bin", &vec![0xCD; 1024]).await;
    common::write_file(&src, "meta.json", br#"{"version":2}"#).await;

    storage
        .upload_segment(DS, "seg_multi", &src)
        .await
        .expect("upload");

    let dest = common::make_dir(dir.path(), "dest").await;
    storage
        .download_segment(DS, "seg_multi", &dest)
        .await
        .expect("download");

    let data = tokio::fs::read(dest.join("data.bin")).await.expect("read");
    let index = tokio::fs::read(dest.join("index.bin")).await.expect("read");
    let meta = tokio::fs::read(dest.join("meta.json")).await.expect("read");
    assert_eq!(data, vec![0xAB; 4096]);
    assert_eq!(index, vec![0xCD; 1024]);
    assert_eq!(meta, br#"{"version":2}"#);

    storage
        .delete_segment(DS, "seg_multi")
        .await
        .expect("delete");

    common::purge_prefix(&storage, DS).await;
}

#[tokio::test]
#[ignore = "requires running MinIO (use run_minio_e2e.sh)"]
async fn minio_download_nonexistent_surfaces_error() {
    let prefix = common::unique_prefix("nonexistent");
    let storage = require_minio!(&prefix);
    common::purge_prefix(&storage, DS).await;

    let dir = tempfile::tempdir().expect("tempdir");
    let dest = common::make_dir(dir.path(), "dest").await;
    let result = storage
        .download_segment(DS, "definitely_not_there", &dest)
        .await;
    assert!(
        result.is_err(),
        "downloading a missing segment must surface an error, got {result:?}",
    );

    common::purge_prefix(&storage, DS).await;
}
