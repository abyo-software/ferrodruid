// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Integration tests for `ferrodruid-migrate assess` — the dry-run
//! deep-storage readability assessment.
//!
//! Fixtures are generated with FerroDruid's own v9 writer
//! ([`ferrodruid_segment::write_segment_v9`]) and, where a zip artifact
//! is needed, packed into a Druid-shaped `index.zip` exactly like the
//! `tests/segment-compat` harness does.  Every test drives the real
//! binary via `CARGO_BIN_EXE_ferrodruid-migrate` so the CLI surface
//! (flags, exit codes, stdout wording) is what is asserted.

use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Output};

use ferrodruid_segment::{SegmentData, SegmentDataBuilder, write_segment_fdx, write_segment_v9};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn migrate_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ferrodruid-migrate")
}

fn run_assess(args: &[&str]) -> Output {
    Command::new(migrate_bin())
        .arg("assess")
        .args(args)
        .output()
        .expect("spawn ferrodruid-migrate assess")
}

/// Canonical 3-row fixture: `__time` + STRING dim `region` + DOUBLE
/// metric `value`.
fn fixture_segment() -> SegmentData {
    SegmentDataBuilder::new()
        .add_timestamp_column(vec![
            1_442_016_000_000,
            1_442_019_600_000,
            1_442_023_200_000,
        ])
        .add_string_column(
            "region",
            vec!["eu".to_string(), "us".to_string(), "us".to_string()],
        )
        .add_double_column("value", true, vec![1.5, 2.5, 3.5])
        .build()
        .expect("build fixture segment")
}

/// Write the fixture as a raw smoosh dir (`meta.smoosh` + chunks).
fn write_raw_segment(dir: &Path) {
    std::fs::create_dir_all(dir).expect("mkdir segment dir");
    write_segment_v9(&fixture_segment(), dir).expect("write_segment_v9");
}

/// Pack every regular file in `smoosh_dir` into a Druid-shaped
/// `index.zip` (same shape as `pack_smoosh_dir_to_index_zip` in the
/// segment-compat harness).
fn pack_index_zip(smoosh_dir: &Path, out_zip: &Path) {
    let file = std::fs::File::create(out_zip).expect("create index.zip");
    let mut zip = zip::ZipWriter::new(file);
    let options: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    let mut names: Vec<_> = std::fs::read_dir(smoosh_dir)
        .expect("read smoosh dir")
        .map(|e| e.expect("dir entry"))
        .filter(|e| e.path().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    for name in names {
        if name == "index.zip" {
            continue;
        }
        let bytes = std::fs::read(smoosh_dir.join(&name)).expect("read smoosh file");
        zip.start_file(&name, options).expect("zip start_file");
        zip.write_all(&bytes).expect("zip write");
    }
    zip.finish().expect("zip finish");
}

/// Create `<root>/<ds>/<interval>/<version>/<partition>/index.zip`
/// containing the canonical readable fixture.
fn make_zip_layout(root: &Path, ds: &str, interval: &str, version: &str, partition: &str) {
    let part_dir = root.join(ds).join(interval).join(version).join(partition);
    std::fs::create_dir_all(&part_dir).expect("mkdir partition dir");
    let staging = tempfile::tempdir().expect("staging tempdir");
    write_raw_segment(staging.path());
    pack_index_zip(staging.path(), &part_dir.join("index.zip"));
}

fn parse_json(out: &Output) -> serde_json::Value {
    assert!(
        out.status.success(),
        "assess exited non-zero: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "assess --json stdout is not valid JSON ({e}): {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

// ---------------------------------------------------------------------------
// Readable artifacts
// ---------------------------------------------------------------------------

/// A canonical `index.zip` under the documented
/// `<dataSource>/<interval>/<version>/<partitionNum>/` layout is found,
/// extracted, opened with the v9 reader, and reported readable with
/// identity derived from the path.
#[test]
fn readable_index_zip_via_json() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(
        root.path(),
        "wiki",
        "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z",
        "2026-01-01T00:00:00.000Z",
        "0",
    );

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);

    assert_eq!(v["total"], 1, "json: {v}");
    assert_eq!(v["readable"], 1, "json: {v}");
    assert_eq!(v["unreadable"], 0, "json: {v}");
    assert_eq!(v["truncated"], false, "json: {v}");

    let seg = &v["segments"][0];
    assert_eq!(seg["artifact"], "index.zip");
    assert_eq!(seg["readable"], true);
    assert_eq!(seg["data_source"], "wiki");
    assert_eq!(
        seg["interval"],
        "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z"
    );
    assert_eq!(seg["version"], "2026-01-01T00:00:00.000Z");
    assert_eq!(seg["partition"], "0");
    assert_eq!(seg["identity_source"], "path");
    assert_eq!(seg["rows"], 3);
    let cols: Vec<String> = seg["columns"]
        .as_array()
        .expect("columns array")
        .iter()
        .map(|c| c.as_str().expect("column name").to_string())
        .collect();
    assert_eq!(cols, vec!["__time", "region", "value"]);
    assert_eq!(seg["error"], serde_json::Value::Null);
}

/// Raw (already-unzipped) smoosh dirs are also assessed: both the
/// smoosh-files-directly-in-`<partition>/` shape and the Druid 31
/// `<partition>/index/` shape.  The trailing `index` path component
/// must not be mistaken for the partition number.
#[test]
fn readable_raw_smoosh_dirs() {
    let root = tempfile::tempdir().expect("tempdir");
    // Shape A: smoosh files directly in the partition dir.
    let part_a = root
        .path()
        .join("ds_a")
        .join("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z")
        .join("v1")
        .join("0");
    write_raw_segment(&part_a);
    // Shape B: Druid 31 local layout — `<partition>/index/` subdir.
    let part_b = root
        .path()
        .join("ds_b")
        .join("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z")
        .join("v1")
        .join("7")
        .join("index");
    write_raw_segment(&part_b);

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);

    assert_eq!(v["total"], 2, "json: {v}");
    assert_eq!(v["readable"], 2, "json: {v}");
    let segs = v["segments"].as_array().expect("segments array");
    let seg_a = segs
        .iter()
        .find(|s| s["data_source"] == "ds_a")
        .expect("ds_a assessed");
    let seg_b = segs
        .iter()
        .find(|s| s["data_source"] == "ds_b")
        .expect("ds_b assessed");
    assert_eq!(seg_a["artifact"], "smoosh-dir");
    assert_eq!(seg_a["partition"], "0");
    assert_eq!(seg_a["identity_source"], "path");
    assert_eq!(seg_b["artifact"], "smoosh-dir");
    assert_eq!(
        seg_b["partition"], "7",
        "trailing `index` dir must be stripped"
    );
    assert_eq!(seg_b["rows"], 3);
}

/// A smoosh dir must not shadow other artifacts: an `index.zip`
/// colocated in the same directory and a nested segment in a child
/// directory are both still found and assessed.  Codex R3 finding
/// (smoosh-dir match stopped the descent and silently hid artifacts).
#[test]
fn smoosh_dir_does_not_shadow_colocated_or_nested_artifacts() {
    let root = tempfile::tempdir().expect("tempdir");
    // Raw smoosh segment at the partition dir...
    let part = root.path().join("ds").join("iv").join("v1").join("0");
    write_raw_segment(&part);
    // ...with a colocated index.zip sibling...
    let staging = tempfile::tempdir().expect("staging");
    write_raw_segment(staging.path());
    pack_index_zip(staging.path(), &part.join("index.zip"));
    // ...and a nested raw segment in a child dir.
    write_raw_segment(&part.join("nested"));

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    assert_eq!(
        v["total"], 3,
        "smoosh dir + colocated zip + nested segment must all be assessed: {v}"
    );
    assert_eq!(v["readable"], 3, "json: {v}");
    // Schema lock: an unaborted scan reports scan_aborted = false.
    assert_eq!(v["scan_aborted"], false, "json: {v}");
}

/// For the `<partition>/index/` raw layout, the authoritative
/// descriptor is the partition-level `descriptor.json` (the one Druid
/// writes next to the artifact) — a JSON file planted *inside* the
/// segment dir must not override it.  Codex R4 finding.
#[test]
fn partition_descriptor_beats_planted_inner_descriptor() {
    let root = tempfile::tempdir().expect("tempdir");
    let part = root.path().join("ds").join("iv").join("v1").join("0");
    let index = part.join("index");
    write_raw_segment(&index);
    std::fs::write(
        part.join("descriptor.json"),
        serde_json::json!({
            "dataSource": "authoritative",
            "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
            "version": "v_real",
            "shardSpec": {"partitionNum": 0},
        })
        .to_string(),
    )
    .expect("write partition descriptor");
    std::fs::write(
        index.join("descriptor.json"),
        serde_json::json!({"dataSource": "planted"}).to_string(),
    )
    .expect("write planted descriptor");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(
        seg["data_source"], "authoritative",
        "partition-level descriptor must win: {v}"
    );
}

/// An over-long identity field parsed from a descriptor is REJECTED
/// (loud skip), never truncated.  Truncation would bound the retained
/// report (the original R4 concern) but at the cost of collapsing two
/// distinct identities sharing a long prefix onto one `ferro_id` — a
/// silent collision skip or `--force` overwrite = data loss (Codex R6).
/// Rejecting is fail-closed and still bounds the report: no usable
/// identity, a loud reason, and the segment is not attachable.
#[test]
fn descriptor_over_long_identity_field_is_loud_not_truncated() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "wiki", "iv", "v1", "0");
    let part_dir = root.path().join("wiki").join("iv").join("v1").join("0");
    std::fs::write(
        part_dir.join("descriptor.json"),
        serde_json::json!({"dataSource": "d".repeat(100_000)}).to_string(),
    )
    .expect("write bloated descriptor");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(
        seg["readable"], false,
        "an over-long identity field must not be reported attachable: {v}"
    );
    assert!(
        seg["error"]
            .as_str()
            .is_some_and(|e| e.contains("identity limit")),
        "the loud reason must name the identity limit: {v}"
    );
    assert!(
        seg["data_source"].is_null(),
        "no truncated identity, no silent path fallback: {v}"
    );
}

/// When a `descriptor.json` sits next to the artifact, identity comes
/// from it (and the output says so) instead of the path guess.
#[test]
fn descriptor_json_supplies_identity() {
    let root = tempfile::tempdir().expect("tempdir");
    // Deliberately non-standard path so path-derivation alone would fail.
    let part_dir = root.path().join("mystery").join("nested");
    std::fs::create_dir_all(&part_dir).expect("mkdir");
    let staging = tempfile::tempdir().expect("staging");
    write_raw_segment(staging.path());
    pack_index_zip(staging.path(), &part_dir.join("index.zip"));
    std::fs::write(
        part_dir.join("descriptor.json"),
        serde_json::json!({
            "dataSource": "wikipedia",
            "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
            "version": "2026-02-02T00:00:00.000Z",
            "shardSpec": {"type": "numbered", "partitionNum": 3},
        })
        .to_string(),
    )
    .expect("write descriptor.json");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(seg["identity_source"], "descriptor.json", "json: {v}");
    assert_eq!(seg["data_source"], "wikipedia");
    assert_eq!(
        seg["interval"],
        "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z"
    );
    assert_eq!(seg["version"], "2026-02-02T00:00:00.000Z");
    assert_eq!(seg["partition"], "3");
    assert_eq!(seg["readable"], true);
}

// ---------------------------------------------------------------------------
// Unreadable artifacts — must be reported, never fatal
// ---------------------------------------------------------------------------

/// A corrupt `index.zip` is reported unreadable with a reason; the scan
/// itself still succeeds (exit 0) and other segments are unaffected.
#[test]
fn broken_zip_is_unreadable_not_fatal() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "good", "iv", "v1", "0");
    let bad_dir = root.path().join("bad").join("iv").join("v1").join("0");
    std::fs::create_dir_all(&bad_dir).expect("mkdir");
    std::fs::write(bad_dir.join("index.zip"), b"this is not a zip archive").expect("write");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);

    assert_eq!(v["total"], 2, "json: {v}");
    assert_eq!(v["readable"], 1, "json: {v}");
    assert_eq!(v["unreadable"], 1, "json: {v}");
    let bad = v["segments"]
        .as_array()
        .expect("segments")
        .iter()
        .find(|s| s["data_source"] == "bad")
        .expect("bad segment assessed");
    assert_eq!(bad["readable"], false);
    let reason = bad["error"].as_str().expect("error reason string");
    assert!(!reason.is_empty(), "reason must be non-empty");
    // The reason must be aggregated in the by-reason summary.
    let reasons = v["unreadable_reasons"].as_object().expect("reasons map");
    assert_eq!(reasons.len(), 1, "json: {v}");
    assert_eq!(reasons.values().next().expect("one reason"), 1);
}

/// A segment written in FerroDruid's FDX (version 10) format is NOT
/// readable by the v9 reader; the reader's own fail-loud reason must be
/// passed through verbatim.
#[test]
fn fdx_segment_unreadable_with_failloud_reason() {
    let root = tempfile::tempdir().expect("tempdir");
    let part_dir = root.path().join("fdx_ds").join("iv").join("v1").join("0");
    std::fs::create_dir_all(&part_dir).expect("mkdir");
    write_segment_fdx(&fixture_segment(), &part_dir).expect("write_segment_fdx");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(seg["readable"], false, "json: {v}");
    let reason = seg["error"].as_str().expect("error reason");
    assert!(
        reason.contains("expected segment version 9"),
        "fail-loud reader reason must be passed through, got: {reason}"
    );
}

/// A zip entry that tries to escape the extraction dir (`../evil.bin`)
/// is rejected: the segment is unreadable and nothing is written
/// outside the temp extraction dir.
#[test]
fn zip_slip_entry_rejected() {
    let root = tempfile::tempdir().expect("tempdir");
    let part_dir = root.path().join("slip").join("iv").join("v1").join("0");
    std::fs::create_dir_all(&part_dir).expect("mkdir");
    let file = std::fs::File::create(part_dir.join("index.zip")).expect("create zip");
    let mut zip = zip::ZipWriter::new(file);
    let options: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    zip.start_file("../evil.bin", options).expect("start_file");
    zip.write_all(b"escape attempt").expect("write");
    zip.finish().expect("finish");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(seg["readable"], false, "json: {v}");
    let reason = seg["error"].as_str().expect("error reason");
    assert!(
        reason.contains("unsafe path") || reason.contains("../"),
        "zip-slip rejection reason expected, got: {reason}"
    );
}

// ---------------------------------------------------------------------------
// Scan behaviour
// ---------------------------------------------------------------------------

/// An empty deep-storage dir yields a successful, zero-segment report.
#[test]
fn empty_deep_storage_dir() {
    let root = tempfile::tempdir().expect("tempdir");
    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    assert_eq!(v["total"], 0, "json: {v}");
    assert_eq!(v["readable"], 0);
    assert_eq!(v["unreadable"], 0);
    assert_eq!(v["segments"].as_array().expect("segments").len(), 0);
}

/// A missing deep-storage dir is an operational error: exit 1.
#[test]
fn missing_deep_storage_dir_is_an_error() {
    let out = run_assess(&[
        "--deep-storage",
        "/nonexistent/ferrodruid-assess-test",
        "--json",
    ]);
    assert!(!out.status.success(), "must exit non-zero for missing dir");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("Error"), "stderr: {stderr}");
}

/// `--max-segments N` stops the scan after N artifacts and flags the
/// truncation; with exactly N artifacts nothing was skipped, so the
/// report is not flagged truncated.
#[test]
fn max_segments_truncates() {
    let root = tempfile::tempdir().expect("tempdir");
    for i in 0..3 {
        make_zip_layout(root.path(), &format!("ds{i}"), "iv", "v1", "0");
    }

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
        "--max-segments",
        "2",
    ]);
    let v = parse_json(&out);
    assert_eq!(v["total"], 2, "json: {v}");
    assert_eq!(v["truncated"], true, "json: {v}");

    // Exactly-N case: no truncation.
    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
        "--max-segments",
        "3",
    ]);
    let v = parse_json(&out);
    assert_eq!(v["total"], 3, "json: {v}");
    assert_eq!(v["truncated"], false, "json: {v}");
}

// ---------------------------------------------------------------------------
// Wording discipline
// ---------------------------------------------------------------------------

/// The human-readable output must use the approved claim wording:
/// "readable by FerroDruid's v9 reader — attachable with
/// `ferrodruid-migrate attach`" (the attach subcommand ships, so
/// pointing at it is the honest claim), while still not promising an
/// automatic attach.
#[test]
fn human_output_wording() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "wiki", "iv", "v1", "0");

    let out = run_assess(&["--deep-storage", root.path().to_str().expect("utf8")]);
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("readable by FerroDruid's v9 reader"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("attachable with `ferrodruid-migrate attach`"),
        "stdout: {stdout}"
    );
    assert!(
        !stdout.contains("will attach") && !stdout.contains("can be attached now"),
        "banned attach-implying wording found: {stdout}"
    );
    // Table + summary basics.
    assert!(stdout.contains("READABLE"), "stdout: {stdout}");
    assert!(stdout.contains("wiki"), "stdout: {stdout}");
}

/// `--help` for the subcommand keeps the same wording discipline.
#[test]
fn assess_help_wording() {
    let out = Command::new(migrate_bin())
        .args(["assess", "--help"])
        .output()
        .expect("spawn help");
    assert!(out.status.success(), "assess --help must exist and exit 0");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("attachable with `ferrodruid-migrate attach`"),
        "help: {stdout}"
    );
    assert!(
        !stdout.contains("will attach") && !stdout.contains("can be attached now"),
        "banned wording in help: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// Hostile-input hardening (Codex review round 1 findings)
// ---------------------------------------------------------------------------

/// Symlinks in the deep-storage tree are never followed (no escape
/// outside the root, no symlink cycles); each skip is surfaced as a
/// scan warning.  Codex R1 finding (scan symlink escape + cycle
/// blowup).
#[test]
fn symlinks_are_not_followed() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "real", "iv", "v1", "0");

    // Cycle: root/loop -> root.
    std::os::unix::fs::symlink(root.path(), root.path().join("loop")).expect("symlink cycle");
    // Escape: root/outside -> a segment OUTSIDE the root.
    let outside = tempfile::tempdir().expect("outside tempdir");
    make_zip_layout(outside.path(), "external", "iv", "v1", "0");
    std::os::unix::fs::symlink(outside.path(), root.path().join("outside"))
        .expect("symlink escape");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    assert_eq!(v["total"], 1, "only the real segment, no dup/escape: {v}");
    assert_eq!(v["segments"][0]["data_source"], "real");
    let warnings = v["warnings"].as_array().expect("warnings array");
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().is_some_and(|s| s.contains("symlink"))),
        "expected symlink-skip warnings, got: {v}"
    );
}

/// An unreadable subdirectory does not abort the scan: readable
/// segments are still assessed and the failure surfaces as a warning.
/// Codex R1 finding (scan abort via `?` on descendant dirs).
#[test]
fn unreadable_subdir_is_warning_not_fatal() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "good", "iv", "v1", "0");
    let locked = root.path().join("locked");
    std::fs::create_dir(&locked).expect("mkdir locked");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod 000");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    // Restore perms so the tempdir can be cleaned up.
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("chmod back");

    let v = parse_json(&out);
    assert_eq!(v["total"], 1, "good segment still assessed: {v}");
    assert_eq!(v["readable"], 1, "json: {v}");
    let warnings = v["warnings"].as_array().expect("warnings array");
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().is_some_and(|s| s.contains("locked"))),
        "expected a warning naming the unreadable dir, got: {v}"
    );
}

/// An oversized `descriptor.json` is never slurped into memory (Codex
/// R1 finding) AND — H4 — a descriptor that EXISTS but cannot be used
/// is a LOUD per-segment failure, not a silent path-identity fallback:
/// the report must not present the segment as attachable under the path
/// identity while `attach` would refuse it.
#[test]
fn oversized_descriptor_json_is_loud_not_silent_path_fallback() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "wiki", "iv", "v1", "0");
    let part_dir = root.path().join("wiki").join("iv").join("v1").join("0");
    // > 4 MiB of valid JSON.
    let mut blob = String::with_capacity(5 * 1024 * 1024 + 64);
    blob.push_str("{\"dataSource\":\"evil\",\"pad\":\"");
    blob.push_str(&"x".repeat(5 * 1024 * 1024));
    blob.push_str("\"}");
    std::fs::write(part_dir.join("descriptor.json"), blob).expect("write big descriptor");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(
        seg["readable"], false,
        "an unusable descriptor must not be reported attachable: {v}"
    );
    assert!(
        seg["error"]
            .as_str()
            .is_some_and(|e| e.contains("descriptor")),
        "the loud reason must name the descriptor: {v}"
    );
    assert!(
        seg["data_source"].is_null(),
        "no silent path-identity fallback (H4): {v}"
    );
}

/// H4: a malformed (unparseable) `descriptor.json` next to an otherwise
/// valid path layout is a LOUD failure in the report — never a silent
/// fallback to the path identity.
#[test]
fn broken_descriptor_json_is_loud_not_silent_path_fallback() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "wiki", "iv", "v1", "0");
    let part_dir = root.path().join("wiki").join("iv").join("v1").join("0");
    std::fs::write(part_dir.join("descriptor.json"), "{ this is not json")
        .expect("write broken descriptor");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(
        seg["readable"], false,
        "a broken descriptor must not be reported attachable: {v}"
    );
    assert!(
        seg["error"]
            .as_str()
            .is_some_and(|e| e.contains("descriptor")),
        "the loud reason must name the descriptor: {v}"
    );
    assert!(
        seg["data_source"].is_null(),
        "no silent path-identity fallback (H4): {v}"
    );
}

/// Codex R4 H1 (assess side): a descriptor FIELD that is PRESENT but
/// malformed (string `"7"` partitionNum) must be a loud identity
/// failure in the report — never silently replaced by the path value
/// (which would present the segment as attachable under path partition
/// 0 while its descriptor says partition 7).
#[test]
fn present_but_malformed_descriptor_field_is_loud_in_assess() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "wiki", "iv", "v1", "0");
    let part_dir = root.path().join("wiki").join("iv").join("v1").join("0");
    std::fs::write(
        part_dir.join("descriptor.json"),
        serde_json::json!({
            "dataSource": "wiki",
            "shardSpec": {"type": "numbered", "partitionNum": "7"},
        })
        .to_string(),
    )
    .expect("write string-partitionNum descriptor");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(
        seg["readable"], false,
        "a malformed-field descriptor must not be reported attachable: {v}"
    );
    assert!(
        seg["error"]
            .as_str()
            .is_some_and(|e| e.contains("partitionNum")),
        "the loud reason must name the malformed field: {v}"
    );
    assert!(
        seg["partition"].is_null(),
        "the path partition must NOT be silently adopted: {v}"
    );
    assert!(
        seg["data_source"].is_null(),
        "identity failure yields no identity fields (H4 pattern): {v}"
    );
}

/// A descriptor.json that only carries `dataSource` is still used, but
/// the missing fields are filled from the path and the identity source
/// says so.  Codex R1 finding (partial descriptor nulls out fields).
#[test]
fn partial_descriptor_filled_from_path() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "pathds", "iv", "v1", "5");
    let part_dir = root.path().join("pathds").join("iv").join("v1").join("5");
    std::fs::write(
        part_dir.join("descriptor.json"),
        serde_json::json!({"dataSource": "descds"}).to_string(),
    )
    .expect("write partial descriptor");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(seg["data_source"], "descds", "descriptor wins: {v}");
    assert_eq!(seg["interval"], "iv", "path fills the gap: {v}");
    assert_eq!(seg["version"], "v1");
    assert_eq!(seg["partition"], "5");
    assert_eq!(seg["identity_source"], "descriptor.json+path", "json: {v}");
}

/// A zip declaring an absurd number of entries is rejected before
/// extraction (inode-exhaustion defence).  Codex R1 finding (no entry
/// count bound).
#[test]
fn zip_entry_count_cap() {
    let root = tempfile::tempdir().expect("tempdir");
    let part_dir = root.path().join("many").join("iv").join("v1").join("0");
    std::fs::create_dir_all(&part_dir).expect("mkdir");
    let file = std::fs::File::create(part_dir.join("index.zip")).expect("create zip");
    let mut zip = zip::ZipWriter::new(file);
    let options: zip::write::SimpleFileOptions =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for i in 0..65_537 {
        zip.start_file(format!("e{i}"), options).expect("start");
    }
    zip.finish().expect("finish");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(seg["readable"], false, "json: {v}");
    let reason = seg["error"].as_str().expect("reason");
    assert!(
        reason.contains("entries") && reason.contains("cap"),
        "entry-cap rejection expected, got: {reason}"
    );
}

/// `--max-segments 0` must not claim the store is empty; the
/// truncation notice must still appear.  Codex R1 finding.
#[test]
fn max_segments_zero_shows_truncation() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "wiki", "iv", "v1", "0");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--max-segments",
        "0",
    ]);
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("--max-segments"),
        "truncation notice expected: {stdout}"
    );
    assert!(
        !stdout.contains("No segment artifacts found"),
        "a truncated scan must not claim the store is empty: {stdout}"
    );
}

/// A FIFO named `index.zip` must not hang the scan: it is skipped
/// with a warning instead of being opened for reading.  Codex R2
/// finding (blocking open on special files).
#[test]
fn fifo_index_zip_skipped_not_opened() {
    let root = tempfile::tempdir().expect("tempdir");
    make_zip_layout(root.path(), "good", "iv", "v1", "0");
    let trap_dir = root.path().join("trap").join("iv").join("v1").join("0");
    std::fs::create_dir_all(&trap_dir).expect("mkdir");
    let fifo = trap_dir.join("index.zip");
    let status = Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("spawn mkfifo");
    assert!(status.success(), "mkfifo failed");

    // If the implementation opens the FIFO this blocks forever; the
    // outer `timeout` in the test-runner invocation converts that into
    // a loud failure.
    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    assert_eq!(v["total"], 1, "only the real segment: {v}");
    assert_eq!(v["segments"][0]["data_source"], "good");
    let warnings = v["warnings"].as_array().expect("warnings array");
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().is_some_and(|s| s.contains("trap"))),
        "expected a warning naming the skipped special file, got: {v}"
    );
}

/// A smoosh dir whose contents include a symlink (e.g. a chunk or
/// sidecar pointing outside the tree) is rejected before the reader
/// touches it — symlinks are excluded end-to-end, not just during the
/// walk.  Codex R2 finding.
#[test]
fn smoosh_dir_with_symlink_child_rejected() {
    let root = tempfile::tempdir().expect("tempdir");
    let part = root.path().join("ds").join("iv").join("v1").join("0");
    write_raw_segment(&part);
    // Plant a symlink sidecar pointing outside the root.
    let outside = tempfile::tempdir().expect("outside");
    std::fs::write(outside.path().join("payload.bin"), b"outside data").expect("write");
    std::os::unix::fs::symlink(
        outside.path().join("payload.bin"),
        part.join("factory.json"),
    )
    .expect("symlink sidecar");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(seg["readable"], false, "json: {v}");
    let reason = seg["error"].as_str().expect("reason");
    assert!(
        reason.contains("symlink") || reason.contains("not a regular file"),
        "symlink-child rejection expected, got: {reason}"
    );
}

/// A zip entry nested absurdly deep is rejected before extraction
/// (deep `create_dir_all` trees would burden tempdir cleanup).  Codex
/// R5 finding.
#[test]
fn zip_deeply_nested_entry_rejected() {
    let root = tempfile::tempdir().expect("tempdir");
    let part_dir = root.path().join("deep").join("iv").join("v1").join("0");
    std::fs::create_dir_all(&part_dir).expect("mkdir");
    let file = std::fs::File::create(part_dir.join("index.zip")).expect("create zip");
    let mut zip = zip::ZipWriter::new(file);
    let options: zip::write::SimpleFileOptions =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let deep_name = format!("{}leaf.bin", "d/".repeat(64));
    zip.start_file(deep_name, options).expect("start");
    zip.write_all(b"x").expect("write");
    zip.finish().expect("finish");

    let out = run_assess(&[
        "--deep-storage",
        root.path().to_str().expect("utf8"),
        "--json",
    ]);
    let v = parse_json(&out);
    let seg = &v["segments"][0];
    assert_eq!(seg["readable"], false, "json: {v}");
    let reason = seg["error"].as_str().expect("reason");
    assert!(
        reason.contains("deep") || reason.contains("depth"),
        "depth rejection expected, got: {reason}"
    );
}

/// Untrusted strings (zip entry names, reasons) are sanitized in the
/// human-readable output — no raw terminal control sequences.  Codex
/// R1 finding (terminal escape injection).
#[test]
fn control_chars_sanitized_in_human_output() {
    let root = tempfile::tempdir().expect("tempdir");
    let part_dir = root.path().join("esc").join("iv").join("v1").join("0");
    std::fs::create_dir_all(&part_dir).expect("mkdir");
    let file = std::fs::File::create(part_dir.join("index.zip")).expect("create zip");
    let mut zip = zip::ZipWriter::new(file);
    let options: zip::write::SimpleFileOptions =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    // Entry name carries both a path escape and an ANSI clear-screen.
    zip.start_file("../\u{1b}[2Jevil", options).expect("start");
    zip.write_all(b"x").expect("write");
    zip.finish().expect("finish");

    let out = run_assess(&["--deep-storage", root.path().to_str().expect("utf8")]);
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains('\u{1b}'),
        "raw ESC byte leaked into human output: {stdout:?}"
    );
    assert!(stdout.contains("UNREADABLE"), "stdout: {stdout}");
}
