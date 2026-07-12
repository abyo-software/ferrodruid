// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Real AWS S3 ⇄ `S3DeepStorage` end-to-end tests.
//!
//! **Gated**. These tests are `#[ignore]`d AND short-circuit unless
//! `FERRODRUID_S3_REAL=1` is set in the environment, because they create
//! real (chargeable) AWS objects under the orchestrator-approved bucket.
//!
//! Required environment for activation:
//!
//! | env var                       | purpose                                  |
//! |-------------------------------|------------------------------------------|
//! | `FERRODRUID_S3_REAL=1`        | activation gate                          |
//! | `FERRODRUID_S3_REAL_BUCKET`   | bucket name (must already exist)         |
//! | `FERRODRUID_S3_REAL_REGION`   | AWS region (default `us-east-1`)         |
//! | `AWS_PROFILE=as`              | marketplace seller profile (CLAUDE.md)   |
//!
//! Orchestrator must pre-create the bucket with:
//!
//! ```sh
//! aws --profile as s3api create-bucket \
//!     --bucket ferrodruid-cl2-compat-$(date +%Y%m%d) \
//!     --region us-east-1
//! ```
//!
//! Run:
//!
//! ```sh
//! FERRODRUID_S3_REAL=1 \
//! FERRODRUID_S3_REAL_BUCKET=ferrodruid-cl2-compat-YYYYMMDD \
//! FERRODRUID_S3_REAL_REGION=us-east-1 \
//! AWS_PROFILE=as \
//! cargo test -p ferrodruid-deep-storage --test s3_compat_aws -- --ignored --nocapture
//! ```
//!
//! Each test uses a unique prefix and tears down after itself; the
//! bucket itself is left in place for the orchestrator to delete.

#![cfg(test)]
#![forbid(unsafe_code)]

mod common;

use ferrodruid_deep_storage::DeepStorage;

const DS: &str = "compat";

macro_rules! require_aws {
    ($prefix:expr) => {
        match common::aws_s3_storage($prefix) {
            Some(s) => s,
            None => {
                eprintln!(
                    "[skip] FERRODRUID_S3_REAL not set or FERRODRUID_S3_REAL_BUCKET missing; \
                     gated AWS S3 test skipped"
                );
                return;
            }
        }
    };
}

#[tokio::test]
#[ignore = "requires real AWS S3 (gated by FERRODRUID_S3_REAL=1)"]
async fn aws_round_trip_single_segment() {
    let prefix = common::unique_prefix("round-trip");
    let storage = require_aws!(&prefix);
    common::purge_prefix(&storage, DS).await;

    let dir = tempfile::tempdir().expect("tempdir");
    let src = common::make_dir(dir.path(), "src").await;
    common::write_file(&src, "data.bin", b"hello-aws-s3").await;

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
    assert_eq!(&data, b"hello-aws-s3");

    storage
        .delete_segment(DS, "seg_round")
        .await
        .expect("delete");

    common::purge_prefix(&storage, DS).await;
}

#[tokio::test]
#[ignore = "requires real AWS S3 (gated by FERRODRUID_S3_REAL=1)"]
async fn aws_delete_is_idempotent() {
    let prefix = common::unique_prefix("idempotent");
    let storage = require_aws!(&prefix);
    common::purge_prefix(&storage, DS).await;

    storage
        .delete_segment(DS, "never_existed")
        .await
        .expect("delete-of-missing on AWS S3 must succeed (idempotent)");

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
        .expect("delete 2 must succeed on AWS S3 (idempotent)");

    common::purge_prefix(&storage, DS).await;
}

#[tokio::test]
#[ignore = "requires real AWS S3 (gated, slow ~30s)"]
async fn aws_list_pagination_above_1000_segments() {
    let prefix = common::unique_prefix("pagination");
    let storage = require_aws!(&prefix);
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
