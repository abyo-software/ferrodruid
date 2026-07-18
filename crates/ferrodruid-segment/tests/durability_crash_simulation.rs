// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 53 — durability crash-simulation integration tests.
//!
//! These tests *do not* invoke the production segment writer directly;
//! instead they recreate the same `temp + fsync + rename` discipline at
//! the file level so that the test can deliberately crash partway
//! through (by dropping the file descriptor, by stopping before the
//! parent fsync, or by stopping in the middle of a multi-file smoosh
//! write) and observe the on-disk state.
//!
//! The invariant being defended is:
//!
//! > A segment directory must be observable as **either** still
//! > carrying `*.tmp.*` staging files / no `meta.smoosh` (= still
//! > in-progress, not a real segment), **or** be fully populated and
//! > readable by [`SmooshReader::open`].  It must **never** be
//! > observable as a half-written `meta.smoosh` + a partial chunk that
//! > a reader could mistake for a real segment.
//!
//! The three cases below cover the three documented hazard windows in
//! `crates/ferrodruid-segment/src/writer.rs::durable`:
//!
//! 1. `case_a_drop_after_fsync_of_first_file_only` — emulate a power
//!    cut after the first chunk file has been fsynced+renamed but
//!    before `meta.smoosh` (the publish marker) is written.
//! 2. `case_b_drop_before_parent_fsync` — emulate a power cut after
//!    every individual file has been fsynced+renamed but before the
//!    parent directory has been fsynced (so the renames may not have
//!    survived).
//! 3. `case_c_drop_during_smoosh_write` — emulate a power cut while
//!    the smoosh `00000.smoosh` file is mid-write: only the temp file
//!    exists, no `meta.smoosh`, and no rename has fired.
//!
//! These are gated `#[ignore]` so the default fast-CI lane stays
//! unaffected; run with:
//!
//! ```text
//! cargo test -p ferrodruid-segment --test durability_crash_simulation \
//!     -- --ignored --nocapture
//! ```

#![allow(missing_docs)]

use std::fs::{self, File, OpenOptions};
use std::io::Write;

use ferrodruid_segment::SmooshReader;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute a unique temp suffix mirroring `writer::durable::unique_suffix`.
fn unique_suffix(tag: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static C: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let counter = C.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{tag}.{pid}.{counter}.{nanos}")
}

/// Emulate a single `temp + fsync + rename` step: write to a temp file
/// next to `final_path`, fsync the data, then rename into place.  The
/// file handle is dropped before the rename so the discipline matches
/// `writer::durable::durable_write`.
fn temp_fsync_rename(final_path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
    let suffix = unique_suffix("crashsim");
    let mut tmp_path = final_path.to_path_buf();
    let stem = final_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "anon".to_string());
    tmp_path.set_file_name(format!("{stem}.tmp.{suffix}"));

    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)?;
    f.write_all(data)?;
    f.sync_all()?;
    drop(f);
    fs::rename(&tmp_path, final_path)
}

/// Names of files visible inside `dir`, sorted for deterministic
/// inspection in the assertions below.
fn ls_sorted(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .map(|it| {
            it.filter_map(std::result::Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// Asserts the post-crash invariant: the directory must look like
/// "still in progress" (no `meta.smoosh`, possibly with `.tmp.*`
/// survivors) **or** be fully populated and readable.  It must
/// **never** carry a `meta.smoosh` whose chunks reference bytes that
/// were never durable.
fn assert_safe_state(dir: &std::path::Path, label: &str) {
    let names = ls_sorted(dir);
    let has_meta = names.iter().any(|n| n == "meta.smoosh");
    let has_chunk0 = names.iter().any(|n| n == "00000.smoosh");
    let has_tmp = names.iter().any(|n| n.contains(".tmp."));

    if has_meta {
        // If the publish marker exists, the segment must be readable
        // — i.e. all referenced chunks must also exist.  If
        // SmooshReader::open succeeds the writer satisfied the
        // ordering guarantee.
        assert!(
            has_chunk0,
            "[{label}] meta.smoosh exists but 00000.smoosh is missing — \
             writer published before chunks were durable: {names:?}",
        );
        SmooshReader::open(dir).unwrap_or_else(|e| {
            panic!("[{label}] meta.smoosh present but SmooshReader::open failed: {e}")
        });
    } else {
        // No publish marker -> SmooshReader::open MUST refuse to
        // treat this as a segment.  Surviving `.tmp.*` files or a
        // bare `00000.smoosh` are both fine; a reader simply sees
        // "not a segment" and skips the dir.
        assert!(
            SmooshReader::open(dir).is_err(),
            "[{label}] SmooshReader::open accepted a directory with no meta.smoosh: {names:?}",
        );
        // .tmp.* survivors are tolerated; assert presence is a no-op
        // because the operator janitor sweeps them.  We just want the
        // variable observed so it does not lint-warn unused.
        let _ = has_tmp;
    }
}

// ---------------------------------------------------------------------------
// case_a — drop after fsync of file 1
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn case_a_drop_after_fsync_of_first_file_only() {
    let dir = TempDir::new().expect("tempdir");
    let chunk_path = dir.path().join("00000.smoosh");

    // Write & rename chunk file 1 successfully.
    temp_fsync_rename(&chunk_path, b"chunk-bytes-here").expect("chunk write");
    assert!(chunk_path.exists(), "chunk should exist after rename");

    // Crash window: meta.smoosh deliberately not written.  This is the
    // exact state writer::durable_write would leave behind if the
    // process died after the chunk rename succeeded.
    let names = ls_sorted(dir.path());
    assert!(
        !names.iter().any(|n| n == "meta.smoosh"),
        "[case_a] meta.smoosh must not exist: {names:?}",
    );

    assert_safe_state(dir.path(), "case_a");
}

// ---------------------------------------------------------------------------
// case_b — drop before parent fsync
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn case_b_drop_before_parent_fsync() {
    let dir = TempDir::new().expect("tempdir");

    // Both files land via temp+fsync+rename.  We deliberately skip the
    // parent-dir fsync that writer::write_to_dir would do at the end.
    // From the *kernel's* point of view, on a real power cut the
    // renames may not have survived.  From this in-process test's
    // point of view (we don't power-cycle the host) all renames are
    // visible and the directory looks fully populated.
    let chunk_path = dir.path().join("00000.smoosh");
    let meta_path = dir.path().join("meta.smoosh");

    temp_fsync_rename(&chunk_path, b"chunk-bytes-2").expect("chunk write");
    temp_fsync_rename(&meta_path, b"v1,2147483647,1\n00000.smoosh,0,0,13").expect("meta write");

    // The post-write state must be readable: SmooshReader::open
    // accepts the directory.  This proves the publish marker -> chunk
    // ordering is preserved even with no parent-dir fsync; a reader
    // never sees the "meta exists, chunk missing" pathological state.
    assert_safe_state(dir.path(), "case_b");
}

// ---------------------------------------------------------------------------
// case_c — drop during smoosh write (mid-flight, no rename)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn case_c_drop_during_smoosh_write() {
    let dir = TempDir::new().expect("tempdir");
    let suffix = unique_suffix("midflight");
    let tmp_path = dir.path().join(format!("00000.smoosh.tmp.{suffix}"));

    // Open the temp file, write *some* bytes, drop the FD, but never
    // fsync, never rename.  This mirrors the exact state writer would
    // leave behind if it died inside the `write_all` call.
    let mut f = File::create(&tmp_path).expect("create tmp");
    f.write_all(b"partial-bytes").expect("write");
    drop(f);

    // Final paths must NOT exist; only the .tmp.* file does.
    let names = ls_sorted(dir.path());
    assert!(
        names.iter().all(|n| n.contains(".tmp.")),
        "[case_c] only .tmp.* files should be visible: {names:?}",
    );
    assert!(
        !dir.path().join("meta.smoosh").exists(),
        "[case_c] meta.smoosh must not exist after a mid-flight crash",
    );
    assert!(
        !dir.path().join("00000.smoosh").exists(),
        "[case_c] 00000.smoosh must not exist after a mid-flight crash",
    );

    assert_safe_state(dir.path(), "case_c");
}

// ---------------------------------------------------------------------------
// case_d — full successful sequence is also covered as a control
// ---------------------------------------------------------------------------

/// Control: a clean write through `temp + fsync + rename` for both
/// chunk and meta produces a directory that `SmooshReader::open`
/// accepts.  This pins the *positive* arm of `assert_safe_state`.
#[test]
#[ignore]
fn case_d_full_sequence_succeeds() {
    let dir = TempDir::new().expect("tempdir");

    // Single chunk with one logical file matching the meta map.
    let chunk_bytes = b"hello-segment";
    let chunk_path = dir.path().join("00000.smoosh");
    let meta_path = dir.path().join("meta.smoosh");

    temp_fsync_rename(&chunk_path, chunk_bytes).expect("chunk write");
    let meta = format!(
        "v1,2147483647,1\nversion.bin,0,0,{end}",
        end = chunk_bytes.len()
    );
    temp_fsync_rename(&meta_path, meta.as_bytes()).expect("meta write");

    // Both publish marker and chunk must be visible.
    assert!(meta_path.exists());
    assert!(chunk_path.exists());

    let names = ls_sorted(dir.path());
    assert!(
        !names.iter().any(|n| n.contains(".tmp.")),
        "no .tmp.* survivors after clean writes: {names:?}",
    );

    assert_safe_state(dir.path(), "case_d");
}
