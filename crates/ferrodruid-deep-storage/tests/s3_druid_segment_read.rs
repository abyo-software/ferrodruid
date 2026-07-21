// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)
//
//! Gated real-AWS-S3 proof for **P0-A**: FerroDruid stores a REAL Apache Druid
//! v9 on-disk segment in **real** S3 deep storage, downloads it via the S3
//! client, and reads it back with the native Druid-segment reader — the row
//! count and column set must survive the round-trip identically.
//!
//! This is the "verified against real S3 deep storage" leg of the segment-read
//! claim (the byte-level local deep-match vs Druid 31/35 is proven separately in
//! the segment crate). Gated + `#[ignore]`d: it creates chargeable AWS objects.
//!
//! ```sh
//! FERRODRUID_S3_REAL=1 \
//! FERRODRUID_S3_REAL_BUCKET=<bucket> \
//! FERRODRUID_S3_REAL_REGION=us-east-1 \
//! FERRODRUID_DRUID_SEG_DIR=/path/to/a/real/druid/segment/dir \
//! AWS_PROFILE=as \
//! cargo test -p ferrodruid-deep-storage --test s3_druid_segment_read -- --ignored --nocapture
//! ```
#![cfg(test)]
#![forbid(unsafe_code)]

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;

use ferrodruid_deep_storage::DeepStorage;
use ferrodruid_segment::SegmentData;

#[tokio::test]
#[ignore = "requires real AWS S3 (FERRODRUID_S3_REAL=1) + a real Druid segment (FERRODRUID_DRUID_SEG_DIR)"]
async fn reads_a_real_druid_segment_from_real_s3() {
    let prefix = common::unique_prefix("druid-read");
    let storage = match common::aws_s3_storage(&prefix) {
        Some(s) => s,
        None => {
            eprintln!("[skip] FERRODRUID_S3_REAL not set / bucket missing");
            return;
        }
    };
    let seg_dir = match std::env::var("FERRODRUID_DRUID_SEG_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => {
            eprintln!("[skip] FERRODRUID_DRUID_SEG_DIR (a real Druid segment dir) not set");
            return;
        }
    };

    let data_source = "druid_compat";
    let seg_id = "druid_s3_read_probe";

    // Baseline: the local real Druid segment reads with the native reader.
    let local = SegmentData::open(&seg_dir).expect("open the local Druid segment");
    assert!(
        local.columns.contains_key("__time"),
        "a real Druid segment always has a __time column"
    );
    let local_cols: BTreeSet<String> = local.columns.keys().cloned().collect();

    // Upload the REAL Druid segment to REAL S3, confirm it exists, download it.
    storage
        .upload_segment(data_source, seg_id, &seg_dir)
        .await
        .expect("upload the Druid segment to real S3");
    assert!(
        storage
            .segment_exists(data_source, seg_id)
            .await
            .expect("segment_exists"),
        "the Druid segment must be present in S3 after upload"
    );
    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("from_s3");
    tokio::fs::create_dir_all(&dest).await.expect("mkdir dest");
    storage
        .download_segment(data_source, seg_id, &dest)
        .await
        .expect("download the Druid segment from real S3");

    // Read the S3-fetched copy with the native reader; it must match the local.
    let from_s3 = SegmentData::open(&dest).expect("open the Druid segment fetched from S3");
    let s3_cols: BTreeSet<String> = from_s3.columns.keys().cloned().collect();
    assert_eq!(
        from_s3.num_rows(),
        local.num_rows(),
        "row count must be identical after the S3 round-trip"
    );
    assert_eq!(
        s3_cols, local_cols,
        "column set must be identical after the S3 round-trip"
    );
    assert!(from_s3.columns.contains_key("__time"));

    eprintln!(
        "P0-A S3 OK — read a real Apache Druid v9 segment from real S3: {} rows, {} columns {:?}",
        from_s3.num_rows(),
        s3_cols.len(),
        s3_cols
    );

    // Clean up the chargeable objects (bucket left for the operator to delete).
    storage
        .delete_segment(data_source, seg_id)
        .await
        .expect("cleanup: delete the probe segment from S3");
}
