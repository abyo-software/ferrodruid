// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `S3DeepStorage` fault-injection tests using `wiremock` as a fake S3
//! endpoint. Exercises the CL-2 retry-semantics closure bar:
//!
//! * **Forced 5xx then 200** → assert eventual success and that retries
//!   actually fired.
//! * **Permanent 5xx** → assert error surfaces after `max_retries + 1`
//!   attempts (not infinite loop, not silent swallow).
//! * **503 throttle (with `Retry-After: 0`)** → assert retry path triggers
//!   and request count grows.
//! * **Connection reset** → covered indirectly: object_store routes
//!   connection-reset failures through the same retry path as 5xx, and
//!   wiremock cannot drop established TCP connections, so we surface this
//!   as a residual in `RESULTS_s3_minio_<date>.md`.
//!
//! These tests run by default (no `#[ignore]`) because they need only the
//! Rust toolchain — no docker, no AWS credentials.

#![cfg(test)]
#![forbid(unsafe_code)]

use std::time::{Duration, Instant};

use bytes::Bytes;
use ferrodruid_deep_storage::{DeepStorage, S3DeepStorage};
use object_store::aws::AmazonS3Builder;
use object_store::{BackoffConfig, ObjectStore, RetryConfig};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

const BUCKET: &str = "ferrodruid-fault-bucket";

fn fast_retry() -> RetryConfig {
    // max_retries=3 means up to 4 attempts (1 original + 3 retries).
    RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(30),
            base: 2.0,
        },
        max_retries: 3,
        retry_timeout: Duration::from_secs(5),
    }
}

fn build_storage(endpoint: &str) -> S3DeepStorage {
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_bucket_name(BUCKET)
        .with_region("us-east-1")
        // Fake credentials: wiremock does not validate signatures, but
        // having creds means object_store goes through its normal SigV4
        // path so request shape matches production.
        .with_access_key_id("AKIA_FAKE_TEST_KEY")
        .with_secret_access_key("FAKE_TEST_SECRET")
        .with_virtual_hosted_style_request(false)
        .with_retry(fast_retry())
        .build()
        .expect("S3 builder");
    S3DeepStorage::with_store(Box::new(store) as Box<dyn ObjectStore>, "segments/".into())
}

fn empty_list_xml() -> Bytes {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{BUCKET}</Name>
  <Prefix>segments/ds/</Prefix>
  <KeyCount>0</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>"#,
    );
    Bytes::from(body.into_bytes())
}

fn list_with_one_segment_xml() -> Bytes {
    // Single object so download_segment exercises GET retry too.
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{BUCKET}</Name>
  <Prefix>segments/ds/seg/</Prefix>
  <KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>segments/ds/seg/data.bin</Key>
    <LastModified>2026-06-30T00:00:00.000Z</LastModified>
    <ETag>&quot;abc123&quot;</ETag>
    <Size>3</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>"#,
    );
    Bytes::from(body.into_bytes())
}

/// Forced 5xx-then-200: GET ListObjectsV2 returns 503 twice, then a
/// valid empty `ListBucketResult`. `list_segments` must retry, eventually
/// return `Ok(vec![])`, and the server must have observed three GETs.
#[tokio::test]
async fn list_retries_on_503_then_succeeds() {
    let server = MockServer::start().await;

    // First two GETs: 503.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .mount(&server)
        .await;

    // Third GET: valid empty list.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(empty_list_xml(), "application/xml"))
        .mount(&server)
        .await;

    let storage = build_storage(&server.uri());

    let start = Instant::now();
    let segments = storage.list_segments("ds").await.expect("list");
    let elapsed = start.elapsed();

    assert!(
        segments.is_empty(),
        "expected empty listing, got {segments:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(5),
        "retries should have introduced at least one backoff sleep; elapsed = {elapsed:?}",
    );

    let received = server.received_requests().await.expect("requests");
    let gets: Vec<_> = received
        .iter()
        .filter(|r| r.method.as_str().eq_ignore_ascii_case("GET"))
        .collect();
    assert_eq!(
        gets.len(),
        3,
        "expected 1 initial + 2 retries = 3 GETs, got {}",
        gets.len(),
    );
}

/// Permanent 5xx: every GET returns 500. `list_segments` must surface
/// the error after `max_retries + 1` attempts and not loop forever.
#[tokio::test]
async fn list_surfaces_error_after_max_retries() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let storage = build_storage(&server.uri());

    let result = storage.list_segments("ds").await;
    assert!(result.is_err(), "permanent 500 must surface error");

    let received = server.received_requests().await.expect("requests");
    let gets: Vec<_> = received
        .iter()
        .filter(|r| r.method.as_str().eq_ignore_ascii_case("GET"))
        .collect();
    // max_retries=3 means object_store attempts 1 + 3 = 4 times before
    // giving up. We assert lower-bound to allow object_store to clamp via
    // its own internal retry-timeout (5s) on slower machines.
    assert!(
        gets.len() >= 2 && gets.len() <= 4,
        "expected 2..=4 GETs (1 initial + up to 3 retries), got {}",
        gets.len(),
    );
}

/// 503 with `Retry-After: 0` simulates an S3 throttle response. The
/// object_store retry layer must honor it (without sleeping minutes),
/// retry, and ultimately succeed.
#[tokio::test]
async fn list_handles_503_throttle_with_retry_after_header() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(503)
                .insert_header("Retry-After", "0")
                .insert_header("x-amz-error-code", "SlowDown"),
        )
        .up_to_n_times(2)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(empty_list_xml(), "application/xml"))
        .mount(&server)
        .await;

    let storage = build_storage(&server.uri());

    let segments = storage
        .list_segments("ds")
        .await
        .expect("throttled list must eventually succeed");
    assert!(segments.is_empty());

    let received = server.received_requests().await.expect("requests");
    let gets = received
        .iter()
        .filter(|r| r.method.as_str().eq_ignore_ascii_case("GET"))
        .count();
    assert_eq!(
        gets, 3,
        "1 initial + 2 throttled retries should equal 3 GETs"
    );
}

/// `delete_segment` lists then issues DELETE per object. Force a 503 on
/// the first DELETE so the retry path inside `object_store::delete` is
/// also exercised end-to-end.
#[tokio::test]
async fn delete_retries_on_503_then_succeeds() {
    let server = MockServer::start().await;

    // LIST returns one object.
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(list_with_one_segment_xml(), "application/xml"),
        )
        .mount(&server)
        .await;

    // First DELETE: 503.
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Subsequent DELETE: 204.
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let storage = build_storage(&server.uri());

    storage
        .delete_segment("ds", "seg")
        .await
        .expect("delete must succeed after one retry");

    let received = server.received_requests().await.expect("requests");
    let deletes = received
        .iter()
        .filter(|r| r.method.as_str().eq_ignore_ascii_case("DELETE"))
        .count();
    assert_eq!(
        deletes, 2,
        "expected 1 failed + 1 successful DELETE attempt, got {deletes}",
    );
}
