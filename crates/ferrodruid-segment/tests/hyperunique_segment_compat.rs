// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-A (v1.5.0) segment-layer tests: REAL Apache-Druid-written
//! `hyperUnique` columns decode strictly from the captured Druid 31.0.2
//! fixture segments, and the decoded column survives the FerroDruid
//! v9/FDX write → strict-reopen round-trip.
//!
//! The register-count assertions are pinned to the byte observations in
//! `tests/segment-compat/fixtures/hyperunique_druid31/byte_observations/`
//! (dense JP header declares 0x024d = 589 non-zero registers, dense US
//! 0x0256 = 598 with a max-overflow pair `0x10 @ 1453`).

use std::path::{Path, PathBuf};

use ferrodruid_segment::column::ColumnData;
use ferrodruid_segment::{SegmentData, write_segment_fdx, write_segment_v9};

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/segment-compat/fixtures/hyperunique_druid31/segments")
}

fn segment_dirs(ds: &str) -> Vec<PathBuf> {
    let dir = fixture_root().join(ds);
    let mut seg_dirs: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("fixture dir {}: {e}", dir.display()))
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("segment_"))
        })
        .collect();
    seg_dirs.sort();
    seg_dirs
}

fn hyper_unique_rows(segment: &SegmentData) -> &[ferrodruid_segment::column::DruidHyperUnique] {
    match segment.column("uu") {
        Some(ColumnData::ComplexHyperUnique(rows)) => rows,
        other => panic!("expected ComplexHyperUnique `uu`, got {other:?}"),
    }
}

/// Every captured fixture segment (all 4 datasources, 11 segments) opens
/// STRICT and decodes its `uu` column into per-row sketches whose count
/// matches the segment row count.
#[test]
fn all_fixture_segments_open_strict_with_decoded_uu() {
    let mut opened = 0usize;
    for ds in [
        "hu_rollup_false",
        "hu_rollup_day",
        "hu_multiseg",
        "hu_dense",
    ] {
        for dir in segment_dirs(ds) {
            let segment = SegmentData::open(&dir)
                .unwrap_or_else(|e| panic!("strict open {}: {e}", dir.display()));
            let rows = hyper_unique_rows(&segment);
            assert_eq!(
                rows.len(),
                segment.num_rows(),
                "{}: per-row sketch count must match the segment row count",
                dir.display()
            );
            assert!(
                rows.iter().all(|r| !r.is_empty()),
                "{}: every fixture row carries a non-empty blob",
                dir.display()
            );
            opened += 1;
        }
    }
    assert_eq!(opened, 11, "oracle captured 2+2+6+1 segments");
}

/// The dense fixture's declared non-zero-register counts (JP 589, US 598
/// with the max-overflow pair) survive decode — pinned to the captured
/// byte observations.
#[test]
fn dense_fixture_register_counts_match_byte_observations() {
    let dirs = segment_dirs("hu_dense");
    assert_eq!(dirs.len(), 1);
    let segment = SegmentData::open(&dirs[0]).expect("open dense");
    let rows = hyper_unique_rows(&segment);
    assert_eq!(rows.len(), 2, "one folded row per country");
    assert_eq!(rows[0].num_non_zero(), 589, "JP blob: header 0x024d");
    assert_eq!(rows[1].num_non_zero(), 598, "US blob: header 0x0256");
}

/// The lenient open path no longer drops a hyperUnique column (it used to
/// be undecodable and dropped under `--allow-unreadable-columns`).
#[test]
fn lenient_open_no_longer_drops_hyperunique() {
    let dirs = segment_dirs("hu_multiseg");
    let (segment, dropped) = SegmentData::open_lenient(&dirs[0]).expect("lenient open");
    assert!(
        dropped.is_empty(),
        "no column may be dropped any more, got {dropped:?}"
    );
    assert!(!hyper_unique_rows(&segment).is_empty());
}

/// A REAL-fixture decoded column survives the FerroDruid v9 AND FDX
/// write → strict-reopen round-trips with identical register state
/// (estimates bit-identical per row).
#[test]
fn real_fixture_column_round_trips_v9_and_fdx() {
    let dirs = segment_dirs("hu_dense");
    let original = SegmentData::open(&dirs[0]).expect("open dense");
    let original_rows = hyper_unique_rows(&original).to_vec();

    type WriteFn = fn(&SegmentData, &Path) -> ferrodruid_common::error::Result<()>;
    let writers: [(&str, WriteFn); 2] = [("v9", write_segment_v9), ("FDX", write_segment_fdx)];
    for (what, write) in writers {
        let tmp = tempfile::tempdir().expect("tempdir");
        write(&original, tmp.path()).unwrap_or_else(|e| panic!("{what} write: {e}"));
        // `from_smoosh` auto-detects v9 vs FDX from `version.bin`.
        let smoosh =
            ferrodruid_segment::SmooshReader::open(tmp.path()).expect("open smoosh archive");
        let reread = SegmentData::from_smoosh(&smoosh)
            .unwrap_or_else(|e| panic!("{what} strict reopen: {e}"));
        let reread_rows = hyper_unique_rows(&reread);
        assert_eq!(reread_rows.len(), original_rows.len(), "{what} row count");
        for (i, (a, b)) in original_rows.iter().zip(reread_rows.iter()).enumerate() {
            assert_eq!(a, b, "{what} row {i}: register state must round-trip");
            assert_eq!(
                a.estimate().to_bits(),
                b.estimate().to_bits(),
                "{what} row {i}: estimate must be bit-identical"
            );
        }
    }
}

/// A truncated per-row hyperUnique column blob fails the reopen loudly
/// (mirrors the theta truncation guard).
#[test]
fn truncated_hyperunique_column_fails_reopen() {
    let dirs = segment_dirs("hu_rollup_day");
    let original = SegmentData::open(&dirs[0]).expect("open");
    let tmp = tempfile::tempdir().expect("tempdir");
    write_segment_v9(&original, tmp.path()).expect("write");
    // Truncate the tail of the single smoosh chunk (the column bytes live
    // near the end of the archive).
    let chunk = tmp.path().join("00000.smoosh");
    let bytes = std::fs::read(&chunk).expect("read chunk");
    std::fs::write(&chunk, &bytes[..bytes.len() - 8]).expect("truncate");
    assert!(
        SegmentData::open(tmp.path()).is_err(),
        "a truncated archive must fail strict reopen loudly"
    );
}
