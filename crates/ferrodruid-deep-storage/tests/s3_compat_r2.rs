// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Cloudflare R2 ⇄ `S3DeepStorage` end-to-end tests.
//!
//! **Gated**. These tests are `#[ignore]`d AND short-circuit unless
//! `FERRODRUID_R2=1` is set, plus an R2 bucket + API token are pre-
//! provisioned out-of-band (R2 is billed per request + per GB stored).
//!
//! Required environment for activation:
//!
//! | env var                       | purpose                                |
//! |-------------------------------|----------------------------------------|
//! | `FERRODRUID_R2=1`             | activation gate                        |
//! | `FERRODRUID_R2_BUCKET`        | bucket name (must already exist)       |
//! | `FERRODRUID_R2_ACCOUNT_ID`    | Cloudflare account id (for endpoint)   |
//! | `FERRODRUID_R2_ACCESS_KEY`    | R2 API access key                      |
//! | `FERRODRUID_R2_SECRET_KEY`    | R2 API secret key                      |
//!
//! Orchestrator pre-flight (one-time, requires explicit approval per
//! CLAUDE.md AWS/cloud discipline):
//!
//! ```sh
//! # Via Cloudflare dashboard or wrangler:
//! wrangler r2 bucket create ferrodruid-cl2-compat-$(date +%Y%m%d)
//! # Generate an R2 API token scoped to that bucket with Object Read+Write
//! ```
//!
//! Run:
//!
//! ```sh
//! FERRODRUID_R2=1 \
//! FERRODRUID_R2_BUCKET=ferrodruid-cl2-compat-YYYYMMDD \
//! FERRODRUID_R2_ACCOUNT_ID=... \
//! FERRODRUID_R2_ACCESS_KEY=... \
//! FERRODRUID_R2_SECRET_KEY=... \
//! cargo test -p ferrodruid-deep-storage --test s3_compat_r2 -- --ignored --nocapture
//! ```

#![cfg(test)]
#![forbid(unsafe_code)]

mod common;

use ferrodruid_deep_storage::DeepStorage;

const DS: &str = "compat";

macro_rules! require_r2 {
    ($prefix:expr) => {
        match common::r2_storage($prefix) {
            Some(s) => s,
            None => {
                eprintln!(
                    "[skip] FERRODRUID_R2 not set or R2 credentials missing; \
                     gated Cloudflare R2 test skipped"
                );
                return;
            }
        }
    };
}

#[tokio::test]
#[ignore = "requires real Cloudflare R2 (gated by FERRODRUID_R2=1)"]
async fn r2_round_trip_single_segment() {
    let prefix = common::unique_prefix("round-trip");
    let storage = require_r2!(&prefix);
    common::purge_prefix(&storage, DS).await;

    let dir = tempfile::tempdir().expect("tempdir");
    let src = common::make_dir(dir.path(), "src").await;
    common::write_file(&src, "data.bin", b"hello-r2").await;

    storage
        .upload_segment(DS, "seg_round", &src)
        .await
        .expect("upload");

    assert!(
        storage
            .segment_exists(DS, "seg_round")
            .await
            .expect("exists"),
    );

    let dest = common::make_dir(dir.path(), "dest").await;
    storage
        .download_segment(DS, "seg_round", &dest)
        .await
        .expect("download");
    let data = tokio::fs::read(dest.join("data.bin")).await.expect("read");
    assert_eq!(&data, b"hello-r2");

    storage
        .delete_segment(DS, "seg_round")
        .await
        .expect("delete");

    common::purge_prefix(&storage, DS).await;
}

#[tokio::test]
#[ignore = "requires real Cloudflare R2 (gated by FERRODRUID_R2=1)"]
async fn r2_delete_is_idempotent() {
    let prefix = common::unique_prefix("idempotent");
    let storage = require_r2!(&prefix);
    common::purge_prefix(&storage, DS).await;

    storage
        .delete_segment(DS, "never_existed")
        .await
        .expect("delete-of-missing on R2 must succeed (idempotent)");

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
        .expect("delete 1");
    storage
        .delete_segment(DS, "seg_idem")
        .await
        .expect("delete 2 must succeed on R2 (idempotent)");

    common::purge_prefix(&storage, DS).await;
}

#[tokio::test]
#[ignore = "requires real Cloudflare R2 (gated, slow ~30s)"]
async fn r2_list_pagination_above_1000_segments() {
    let prefix = common::unique_prefix("pagination");
    let storage = require_r2!(&prefix);
    common::purge_prefix(&storage, DS).await;

    const N: usize = 1100;

    let dir = tempfile::tempdir().expect("tempdir");
    let src = common::make_dir(dir.path(), "src").await;
    common::write_file(&src, "data.bin", b"x").await;

    use futures::stream::{self, StreamExt};
    let results: Vec<_> = stream::iter(0..N)
        .map(|i| {
            let storage = &storage;
            let src = src.clone();
            async move {
                let seg_id = format!("seg_{i:06}");
                storage.upload_segment(DS, &seg_id, &src).await
            }
        })
        .buffer_unordered(32)
        .collect()
        .await;
    for r in results {
        r.expect("upload");
    }

    let listed = storage.list_segments(DS).await.expect("list");
    assert_eq!(listed.len(), N);

    common::purge_prefix(&storage, DS).await;
}
