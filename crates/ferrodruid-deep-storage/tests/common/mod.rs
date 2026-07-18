// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Shared helpers for the deep-storage real-infra compat integration tests.
//!
//! These helpers build [`object_store::aws::AmazonS3Builder`] instances
//! pointed at MinIO / AWS S3 / Cloudflare R2 endpoints and wrap them in
//! [`ferrodruid_deep_storage::S3DeepStorage`] for use by the per-backend
//! integration tests under `crates/ferrodruid-deep-storage/tests/`.

#![allow(dead_code)]

use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ferrodruid_deep_storage::{DeepStorage, S3DeepStorage};
use object_store::aws::AmazonS3Builder;
use object_store::{BackoffConfig, ObjectStore, RetryConfig};

/// Tight retry config used by integration tests so failures surface in
/// seconds, not minutes. `max_retries = 3` means each request is attempted
/// up to 4 times in total (1 original + 3 retries).
pub fn fast_retry() -> RetryConfig {
    RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(80),
            base: 2.0,
        },
        max_retries: 3,
        retry_timeout: Duration::from_secs(10),
    }
}

/// Build an `S3DeepStorage` targeting MinIO. Credentials and endpoint are
/// read from environment variables populated by `run_minio_e2e.sh`.
///
/// Returns `None` if `FERRODRUID_MINIO_ENDPOINT` is not set, which lets the
/// callers skip cleanly when MinIO isn't available.
pub fn minio_storage(prefix: &str) -> Option<S3DeepStorage> {
    let endpoint = env::var("FERRODRUID_MINIO_ENDPOINT").ok()?;
    let bucket =
        env::var("FERRODRUID_MINIO_BUCKET").unwrap_or_else(|_| "ferrodruid-compat".to_string());
    let access_key =
        env::var("FERRODRUID_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key =
        env::var("FERRODRUID_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());

    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        // MinIO uses path-style addressing; virtual-hosted style requires
        // DNS wildcards which our localhost setup does not provide.
        .with_virtual_hosted_style_request(false)
        .with_retry(fast_retry())
        .build()
        .expect("MinIO S3 builder must succeed with fixed test config");

    Some(S3DeepStorage::with_store(
        Box::new(store) as Box<dyn ObjectStore>,
        prefix.to_string(),
    ))
}

/// Build an `S3DeepStorage` targeting a real AWS S3 bucket. Reads the
/// bucket name from `FERRODRUID_S3_REAL_BUCKET` and the region from
/// `FERRODRUID_S3_REAL_REGION`. AWS credentials are resolved from the
/// standard SDK chain (so the orchestrator can `export AWS_PROFILE=as`
/// before running).
pub fn aws_s3_storage(prefix: &str) -> Option<S3DeepStorage> {
    if env::var("FERRODRUID_S3_REAL").ok().as_deref() != Some("1") {
        return None;
    }
    let bucket = env::var("FERRODRUID_S3_REAL_BUCKET").ok()?;
    let region = env::var("FERRODRUID_S3_REAL_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_retry(fast_retry())
        .build()
        .expect("AWS S3 builder must succeed with env credentials");

    Some(S3DeepStorage::with_store(
        Box::new(store) as Box<dyn ObjectStore>,
        prefix.to_string(),
    ))
}

/// Build an `S3DeepStorage` targeting a Cloudflare R2 bucket. Reads
/// `FERRODRUID_R2_BUCKET`, `FERRODRUID_R2_ACCOUNT_ID`,
/// `FERRODRUID_R2_ACCESS_KEY` and `FERRODRUID_R2_SECRET_KEY`. Endpoint
/// is derived as `https://<account>.r2.cloudflarestorage.com`. R2 advertises
/// region `auto`.
pub fn r2_storage(prefix: &str) -> Option<S3DeepStorage> {
    if env::var("FERRODRUID_R2").ok().as_deref() != Some("1") {
        return None;
    }
    let bucket = env::var("FERRODRUID_R2_BUCKET").ok()?;
    let account_id = env::var("FERRODRUID_R2_ACCOUNT_ID").ok()?;
    let access_key = env::var("FERRODRUID_R2_ACCESS_KEY").ok()?;
    let secret_key = env::var("FERRODRUID_R2_SECRET_KEY").ok()?;

    let endpoint = format!("https://{account_id}.r2.cloudflarestorage.com");

    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        // R2 ignores region but the SDK still requires one; "auto" is the
        // documented Cloudflare value.
        .with_region("auto")
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_retry(fast_retry())
        .build()
        .expect("R2 S3 builder must succeed with explicit credentials");

    Some(S3DeepStorage::with_store(
        Box::new(store) as Box<dyn ObjectStore>,
        prefix.to_string(),
    ))
}

/// A scoped working directory: creates `<base>/<name>/` and returns the path.
pub async fn make_dir(base: &Path, name: &str) -> PathBuf {
    let p = base.join(name);
    tokio::fs::create_dir_all(&p).await.expect("mkdir");
    p
}

/// Write a small marker file under `dir` with the given filename and bytes.
pub async fn write_file(dir: &Path, name: &str, bytes: &[u8]) {
    tokio::fs::write(dir.join(name), bytes)
        .await
        .expect("write");
}

/// Generate a unique per-test prefix so concurrent test runs cannot
/// collide on shared buckets (especially relevant for AWS / R2 legs).
pub fn unique_prefix(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("ferrodruid-compat/{tag}-{nanos}/")
}

/// Aggressively drop everything under `prefix` from the underlying store.
/// Used by per-test teardown so shared buckets stay clean across runs.
pub async fn purge_prefix(storage: &S3DeepStorage, data_source: &str) {
    // Listing then deleting each segment exercises the same code path that
    // production cleanup would use. Errors are intentionally swallowed so
    // teardown never masks the actual test assertion failure.
    if let Ok(segments) = storage.list_segments(data_source).await {
        for seg in segments {
            let _ = storage.delete_segment(data_source, &seg).await;
        }
    }
}
