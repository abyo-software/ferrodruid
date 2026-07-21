// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Oracle regression tests for the Druid `hyperUnique` SPARSE blob body
//! (v1.5.0 W-A follow-up), pinned against targeted fixtures captured from
//! real Druid 31.0.2 under
//! `tests/segment-compat/fixtures/hyperunique_sparse_druid31/`
//! (`capture_hyperunique_sparse_probe.sh` — black-box, no Druid source).
//!
//! The fixtures settle two format questions the original W-A capture
//! could not disambiguate (every observed position fell in the 7..1023
//! overlap of both hypotheses and no page byte held two occupied
//! nibbles):
//!
//! 1. **Position base** — a 3000-single-user census: positions span
//!    EXACTLY 7..=1030 (none in 0..=6, twenty-two past 1023), and the
//!    900-user dense page nibble-covers every constituent user's sparse
//!    value at `page[pos - 7]` (900/900) but not at `page[pos]`
//!    (202/900, chance level).  Sparse positions are therefore
//!    BUFFER-relative (7-byte header included), NOT page-relative.
//! 2. **Body sizing** — folding two users whose registers share one page
//!    byte produced `01 00 0002 00 0000 | 00 08 11 | 00 00 00`: ONE real
//!    entry carrying BOTH nibbles (`0x11`), a declared count of 2
//!    (registers, not entries), and a ZERO-PADDED trailing entry — the
//!    body is sized `declared_count * 3` with one entry per non-zero
//!    page byte.
//!
//! The behavioral pin is Druid's own sparse⊕dense fold: day1 = 900 users
//! (dense blob), day2 = 6 targeted users (sparse blob, includes position
//! 1030 AND the both-nibbles byte), and Druid's granularity-all estimate
//! over both days must be reproduced bit-exactly by decode + merge +
//! estimate.  A page-relative decode shifts every sparse register by 14
//! and cannot reproduce it.

use std::path::{Path, PathBuf};

use ferrodruid_sketches::DruidHyperUnique;

// ---------------------------------------------------------------------------
// Oracle constants (captured 2026-07-20 from real Druid 31.0.2 — see
// fixtures/hyperunique_sparse_druid31/oracle/*/native_timeseries_*.json)
// ---------------------------------------------------------------------------

/// `hu_sparse_pair` fold of the two same-byte users (2 distinct).
const ORACLE_PAIR: f64 = 2.000_977_198_748_901;
/// `hu_sparse_mix` day-1 (900 distinct, dense blob).
const ORACLE_MIX_DAY1: f64 = 884.090_398_604_490_8;
/// `hu_sparse_mix` day-2 (6 distinct, sparse blob).
const ORACLE_MIX_DAY2: f64 = 6.008_806_266_444_944;
/// `hu_sparse_mix` granularity-all fold: Druid's own sparse⊕dense merge.
const ORACLE_MIX_MERGED: f64 = 890.259_077_967_033_6;

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/segment-compat/fixtures/hyperunique_sparse_druid31")
}

fn hex_bytes(s: &str) -> Vec<u8> {
    let s = s.trim();
    assert!(s.len().is_multiple_of(2), "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn blob_fixture(name: &str) -> Vec<u8> {
    let path = fixture_root().join("blobs_hex").join(name);
    hex_bytes(
        &std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("fixture {}: {e}", path.display())),
    )
}

/// `(user, declared position, value byte, full blob)` per probe row.
fn probe_map() -> Vec<(String, u16, u8, Vec<u8>)> {
    let path = fixture_root().join("probe_map.tsv");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("fixture {}: {e}", path.display()));
    let mut out = Vec::new();
    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut f = line.split('\t');
        let user = f.next().expect("user").to_owned();
        let pos: u16 = f.next().expect("pos").parse().expect("pos u16");
        let val = hex_bytes(f.next().expect("value"))[0];
        let blob = hex_bytes(f.next().expect("blob"));
        out.push((user, pos, val, blob));
    }
    out
}

/// The 3000-user census: positions span exactly 7..=1030 (BUFFER-relative
/// — the page-relative-only zone 0..=6 is EMPTY, the buffer-relative-only
/// zone 1024..=1030 is populated), and every single-user blob decodes to
/// exactly one occupied register with the 1-distinct linear-counting
/// estimate.  On the pre-fix decoder the 22 tail-position blobs were
/// REJECTED as "past the register page".
#[test]
fn probe_census_positions_are_buffer_relative() {
    let rows = probe_map();
    assert_eq!(rows.len(), 3000, "probe census size");

    let positions: Vec<u16> = rows.iter().map(|&(_, p, _, _)| p).collect();
    let min = positions.iter().copied().min().expect("min");
    let max = positions.iter().copied().max().expect("max");
    let below_header = positions.iter().filter(|&&p| p < 7).count();
    let past_page = positions.iter().filter(|&&p| p > 1023).count();
    assert_eq!(min, 7, "smallest observed sparse position");
    assert_eq!(max, 1030, "largest observed sparse position");
    assert_eq!(
        below_header, 0,
        "page-relative-only zone 0..=6 must be empty"
    );
    assert_eq!(past_page, 22, "buffer-relative-only zone 1024..=1030");

    let single_user_estimate = 2048.0_f64 * (2048.0_f64 / 2047.0).ln();
    for (user, pos, _, blob) in &rows {
        let s = DruidHyperUnique::from_druid_blob(blob)
            .unwrap_or_else(|e| panic!("{user} (position {pos}): {e}"));
        assert_eq!(s.num_non_zero(), 1, "{user}: one occupied register");
        assert_eq!(
            s.estimate().to_bits(),
            single_user_estimate.to_bits(),
            "{user}: 1-distinct linear-counting estimate"
        );
    }
}

/// The both-nibbles fold: Druid's blob for exactly two users sharing one
/// page byte carries ONE real entry (`00 08 11` — both nibbles occupied),
/// a declared register count of 2, and a zero-padded trailing entry.  The
/// decode must accept the padding, credit BOTH registers, and reproduce
/// Druid's own estimate bit-exactly.  The pre-fix decoder rejected the
/// zero-padded entry ("zero value byte").
#[test]
fn pair_fold_zero_padded_both_nibbles_blob() {
    let blob = blob_fixture("hu_sparse_pair_row0.hex");
    // Pin the raw shape this test is about: declared count 2, 6-byte body
    // = ONE real entry + one zero-padded entry.
    assert_eq!(blob.len(), 13, "7-byte header + 2*3-byte body");
    assert_eq!(u16::from_be_bytes([blob[2], blob[3]]), 2, "declared count");
    assert_eq!(&blob[7..10], &[0x00, 0x08, 0x11], "both-nibbles entry");
    assert_eq!(&blob[10..13], &[0x00, 0x00, 0x00], "zero-padded tail entry");

    let s = DruidHyperUnique::from_druid_blob(&blob).expect("decode pair blob");
    assert_eq!(s.num_non_zero(), 2, "both nibbles credited as registers");
    assert_eq!(
        s.estimate().to_bits(),
        ORACLE_PAIR.to_bits(),
        "oracle: Druid answered {ORACLE_PAIR}"
    );
}

/// The decisive behavioral pin: decode Druid's dense day-1 blob (900
/// users) and sparse day-2 blob (6 users — including position 1030, past
/// the pre-fix limit, and the both-nibbles byte), merge them, and
/// reproduce Druid's own granularity-all fold bit-exactly, in both merge
/// directions.  A page-relative decode misaligns every sparse register by
/// 14 register slots and cannot match.
#[test]
fn sparse_dense_merge_matches_druid_fold() {
    let dense_blob = blob_fixture("hu_sparse_mix_day1_dense.hex");
    let sparse_blob = blob_fixture("hu_sparse_mix_day2_sparse.hex");
    assert_eq!(dense_blob.len(), 1031, "dense = 7-byte header + 1024 page");
    // Pin the sparse shape: 6 declared registers, 5 real entries (one
    // both-nibbles byte), 1 zero-padded entry, positions up to 1030.
    assert_eq!(sparse_blob.len(), 25, "7 + 6*3");
    assert_eq!(
        u16::from_be_bytes([sparse_blob[2], sparse_blob[3]]),
        6,
        "declared count"
    );
    assert_eq!(
        &sparse_blob[19..22],
        &[0x04, 0x06, 0x01],
        "entry at position 1030 (page byte 1023 — rejected pre-fix)"
    );
    assert_eq!(
        &sparse_blob[22..25],
        &[0x00, 0x00, 0x00],
        "zero-padded tail"
    );

    let dense = DruidHyperUnique::from_druid_blob(&dense_blob).expect("decode dense");
    let sparse = DruidHyperUnique::from_druid_blob(&sparse_blob).expect("decode sparse");
    assert_eq!(
        dense.estimate().to_bits(),
        ORACLE_MIX_DAY1.to_bits(),
        "day-1 dense estimate"
    );
    assert_eq!(sparse.num_non_zero(), 6, "day-2 registers");
    assert_eq!(
        sparse.estimate().to_bits(),
        ORACLE_MIX_DAY2.to_bits(),
        "day-2 sparse estimate"
    );

    let merged = dense.merged(&sparse);
    assert_eq!(
        merged.estimate().to_bits(),
        ORACLE_MIX_MERGED.to_bits(),
        "sparse⊕dense fold must match Druid's own granularity-all answer"
    );
    assert_eq!(
        sparse.merged(&dense).estimate().to_bits(),
        ORACLE_MIX_MERGED.to_bits(),
        "merge is direction-independent"
    );
}
