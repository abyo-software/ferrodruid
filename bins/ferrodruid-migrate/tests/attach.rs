// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Integration tests for `ferrodruid-migrate attach` — the offline
//! Druid-v9-segment importer (compat-2).
//!
//! Fixtures are generated with FerroDruid's own v9 writer and laid out
//! exactly like a Druid local deep-storage tree (both the raw
//! `<partition>/index/` smoosh layout with a partition-level
//! `descriptor.json` and the zipped `index.zip` layout).  Every test
//! drives the real binary via `CARGO_BIN_EXE_ferrodruid-migrate`, and
//! the crown E2E test then runs the REAL post-attach path — the same
//! `Overlord::bootstrap_reload_segments` sweep the next `ferrodruid
//! serve` startup runs — and asserts the attached segments are
//! query-visible with the expected aggregates.

use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use ferrodruid_deep_storage::{DeepStorage, LocalDeepStorage, blob_content_hash};
use ferrodruid_metadata::MetadataStore;
use ferrodruid_segment::{SegmentData, SegmentDataBuilder, write_segment_fdx, write_segment_v9};
use serde_json::json;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The wiki fixture's ferro segment id: Druid id synthesis
/// `<ds>_<start>_<end>_<version>_<partition>` over the descriptor
/// identity.
const WIKI_ID: &str =
    "wiki_2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z_2026-01-01T00:00:00.000Z_0";

/// The clicks fixture's ferro segment id, synthesized from the PATH
/// identity (underscore-form interval directory).
const CLICKS_ID: &str = "clicks_2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z_v1_0";

fn migrate_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ferrodruid-migrate")
}

fn run_attach(args: &[&str]) -> Output {
    Command::new(migrate_bin())
        .arg("attach")
        .args(args)
        .output()
        .expect("spawn ferrodruid-migrate attach")
}

fn stdout_of(out: &Output) -> String {
    assert!(
        out.status.success(),
        "attach exited non-zero: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Canonical 3-row fixture: `__time` + STRING dim `region` + DOUBLE
/// metric `value` (sum = 7.5), timestamps inside 2015-09-12.
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
/// `index.zip`.
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
/// containing the canonical readable fixture (path-derived identity).
fn make_zip_layout(root: &Path, ds: &str, interval: &str, version: &str, partition: &str) {
    let part_dir = root.join(ds).join(interval).join(version).join(partition);
    std::fs::create_dir_all(&part_dir).expect("mkdir partition dir");
    let staging = tempfile::tempdir().expect("staging tempdir");
    write_raw_segment(staging.path());
    pack_index_zip(staging.path(), &part_dir.join("index.zip"));
}

/// Create the Druid-31-style raw layout
/// `<root>/wiki/<interval>/<version>/0/index/` (smoosh files) with the
/// authoritative partition-level `descriptor.json` (slash-form
/// interval) next to the artifact — descriptor-derived identity.
fn make_wiki_raw_layout(root: &Path) {
    let part_dir = root
        .join("wiki")
        .join("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z")
        .join("2026-01-01T00:00:00.000Z")
        .join("0");
    write_raw_segment(&part_dir.join("index"));
    std::fs::write(
        part_dir.join("descriptor.json"),
        json!({
            "dataSource": "wiki",
            "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
            "version": "2026-01-01T00:00:00.000Z",
            "shardSpec": {"type": "numbered", "partitionNum": 0},
        })
        .to_string(),
    )
    .expect("write descriptor.json");
}

/// Create the zipped clicks layout (path identity, underscore interval).
fn make_clicks_zip_layout(root: &Path) {
    make_zip_layout(
        root,
        "clicks",
        "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z",
        "v1",
        "0",
    );
}

async fn open_store(db_path: &Path) -> MetadataStore {
    let store = MetadataStore::new_sqlite(db_path.to_str().expect("utf8 db path"))
        .await
        .expect("open sqlite store");
    store.initialize().await.expect("init store");
    store
}

/// Timeseries `count` + `doubleSum(value)` over the datasource — the
/// same query shape the product serves, so "query-visible" is asserted
/// through the real query path, not via internal maps.
fn queried_count_and_sum(historical: &ferrodruid_historical::Historical, ds: &str) -> (i64, f64) {
    let query: ferrodruid_query::DruidQuery = serde_json::from_value(json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": ds},
        "intervals": ["2000-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z"],
        "granularity": "all",
        "aggregations": [
            {"type": "count", "name": "cnt"},
            {"type": "doubleSum", "name": "s", "fieldName": "value"},
        ]
    }))
    .expect("build query");
    let results = historical.execute_query(&query).expect("execute query");
    let mut cnt = 0i64;
    let mut sum = 0f64;
    for r in &results {
        if let ferrodruid_query::QueryResult::Timeseries(ts) = r {
            for row in ts {
                cnt += row
                    .result
                    .get("cnt")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                sum += row
                    .result
                    .get("s")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
            }
        }
    }
    (cnt, sum)
}

// ---------------------------------------------------------------------------
// --allow-unreadable-columns fixtures: a v9 archive with one column the
// reader cannot decode (a `thetaSketch`-typed complex column), built by
// repacking the writer's own output with the extra column declared in
// index.drd.
// ---------------------------------------------------------------------------

/// Split a smoosh archive (meta text + single chunk) back into its
/// ordered `(name, bytes)` entries.
fn smoosh_files_of(meta: &str, chunk: &[u8]) -> Vec<(String, Vec<u8>)> {
    meta.lines()
        .skip(1) // header
        .map(|line| {
            let parts: Vec<&str> = line.split(',').collect();
            let start: usize = parts[2].parse().expect("entry start");
            let end: usize = parts[3].parse().expect("entry end");
            (parts[0].to_string(), chunk[start..end].to_vec())
        })
        .collect()
}

/// Write `(name, bytes)` entries as an on-disk smoosh dir
/// (`meta.smoosh` + `00000.smoosh`).
fn write_smoosh_files(dir: &Path, files: &[(String, Vec<u8>)]) {
    std::fs::create_dir_all(dir).expect("mkdir smoosh dir");
    let mut chunk = Vec::new();
    let mut meta_lines = vec![format!("v1,2147483647,{}", files.len())];
    for (name, bytes) in files {
        let start = chunk.len();
        chunk.extend_from_slice(bytes);
        meta_lines.push(format!("{name},0,{start},{}", chunk.len()));
    }
    std::fs::write(dir.join("meta.smoosh"), meta_lines.join("\n")).expect("write meta.smoosh");
    std::fs::write(dir.join("00000.smoosh"), &chunk).expect("write chunk");
}

/// The canonical fixture repacked with ONE extra metric `sketchcol`
/// declared in `index.drd`, whose sidecar descriptor names a value type
/// the v9 reader cannot decode (`thetaSketch` — the Apache DataSketches
/// complex-column case) over opaque bytes.  Optionally corrupts other
/// columns' data blobs (`corrupt`: file name → replacement bytes).
fn write_raw_segment_with_sketch(dir: &Path, corrupt: &[(&str, Vec<u8>)]) {
    let (meta, chunks) =
        ferrodruid_segment::write_segment_v9_to_memory(&fixture_segment()).expect("write fixture");
    let mut files = smoosh_files_of(&meta, &chunks[0]);
    for entry in &mut files {
        if entry.0 == "index.drd" {
            // Re-declare the metrics to include the sketch column.
            entry.1 = ferrodruid_segment::v9::encode_index_drd(
                &["region"],
                &["value", "sketchcol"],
                1_442_016_000_000,
                1_442_023_200_000,
                1,
            );
        }
        if let Some((_, bytes)) = corrupt.iter().find(|(name, _)| *name == entry.0) {
            entry.1 = bytes.clone();
        }
    }
    files.push((
        "sketchcol.column_descriptor.json".to_string(),
        br#"{"valueType":"thetaSketch"}"#.to_vec(),
    ));
    files.push(("sketchcol".to_string(), vec![0xDE, 0xAD, 0xBE, 0xEF]));
    write_smoosh_files(dir, &files);
}

/// Lay the sketch fixture out Druid-31-style
/// (`<root>/<ds>/<interval>/v1/0/index/` + descriptor.json).
fn make_sketch_raw_layout(root: &Path, ds: &str, corrupt: &[(&str, Vec<u8>)]) {
    let part_dir = root
        .join(ds)
        .join("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z")
        .join("v1")
        .join("0");
    write_raw_segment_with_sketch(&part_dir.join("index"), corrupt);
    std::fs::write(
        part_dir.join("descriptor.json"),
        json!({
            "dataSource": ds,
            "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
            "version": "v1",
            "shardSpec": {"type": "numbered", "partitionNum": 0},
        })
        .to_string(),
    )
    .expect("write descriptor.json");
}

/// A truncated column blob: declares 100 values, supplies one byte —
/// every typed decoder rejects it.
fn truncated_column_bytes() -> Vec<u8> {
    let mut b = 100_u32.to_be_bytes().to_vec();
    b.push(0xFF);
    b
}

// ---------------------------------------------------------------------------
// The crown E2E: attach → bootstrap reload → query-visible
// ---------------------------------------------------------------------------

/// Attach a raw-layout segment (descriptor identity) and a zipped
/// segment (path identity), then run the REAL next-startup path —
/// `Overlord::bootstrap_reload_segments` over the store + deep storage
/// the CLI wrote — and assert both segments are query-visible with the
/// expected row count and aggregate.  Also pins the P→M artifacts: the
/// blob lives in the FerroDruid layout and the row's
/// `loadSpec.sha256` matches a re-hash of the uploaded blob (the exact
/// check the bootstrap reload enforces).
#[tokio::test]
async fn attach_then_bootstrap_reload_makes_segments_queryable() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    make_wiki_raw_layout(druid_root.path());
    make_clicks_zip_layout(druid_root.path());

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(stdout.contains("ATTACHED"), "stdout: {stdout}");
    assert!(stdout.contains(WIKI_ID), "stdout: {stdout}");
    assert!(stdout.contains(CLICKS_ID), "stdout: {stdout}");
    assert!(
        stdout.contains("2 attached"),
        "summary must count 2 attached: {stdout}"
    );

    // ---- M: the metadata rows the CLI committed --------------------------
    let store = open_store(&db_path).await;
    let wiki = store
        .get_segment(WIKI_ID)
        .await
        .expect("get wiki row")
        .expect("wiki row exists");
    assert!(wiki.used, "attached row must be used");
    assert_eq!(wiki.data_source, "wiki");
    assert_eq!(
        wiki.start, "2015-09-12T00:00:00.000Z",
        "descriptor interval start"
    );
    assert_eq!(
        wiki.end, "2015-09-13T00:00:00.000Z",
        "descriptor interval end"
    );
    assert_eq!(
        wiki.version, "2026-01-01T00:00:00.000Z",
        "Druid version kept"
    );
    let load_spec = wiki
        .payload
        .get("loadSpec")
        .expect("attached row carries a loadSpec");
    assert_eq!(load_spec["type"], "local");
    assert_eq!(load_spec["dataSource"], "wiki");
    assert_eq!(load_spec["segmentId"], WIKI_ID);

    // ---- P: the blob in the FerroDruid deep-storage layout ---------------
    let wiki_blob = ferro_base.path().join("wiki").join(WIKI_ID);
    assert!(
        wiki_blob.join("meta.smoosh").is_file(),
        "blob must be a v9 smoosh dir at <base>/<ds>/<segmentId>/"
    );
    let recomputed = blob_content_hash(&wiki_blob).expect("re-hash uploaded blob");
    assert_eq!(
        load_spec["sha256"].as_str().expect("sha256 recorded"),
        recomputed,
        "recorded sha256 must match a re-hash of the uploaded blob — this is \
         the exact integrity check the bootstrap reload enforces"
    );

    let clicks = store
        .get_segment(CLICKS_ID)
        .await
        .expect("get clicks row")
        .expect("clicks row exists");
    assert_eq!(
        clicks.start, "2015-09-12T00:00:00.000Z",
        "path interval start"
    );
    assert_eq!(clicks.end, "2015-09-13T00:00:00.000Z", "path interval end");
    assert_eq!(clicks.version, "v1");

    // ---- The next-startup path: bootstrap reload → query ------------------
    let metadata = Arc::new(store);
    let cache = tempfile::tempdir().expect("historical cache");
    let historical = Arc::new(ferrodruid_historical::Historical::new(
        cache.path().to_path_buf(),
        10_000_000,
    ));
    let deep_storage: Arc<dyn DeepStorage> =
        Arc::new(LocalDeepStorage::new(ferro_base.path().to_path_buf()));
    let overlord = ferrodruid_overlord::Overlord::with_executor(
        Arc::clone(&metadata),
        Arc::clone(&historical),
    )
    .with_deep_storage(deep_storage);

    let reloaded = overlord
        .bootstrap_reload_segments()
        .await
        .expect("bootstrap reload succeeds over attached segments");
    assert_eq!(reloaded, 2, "both attached segments reload");
    assert!(historical.is_initial_load_complete());

    let (wiki_cnt, wiki_sum) = queried_count_and_sum(&historical, "wiki");
    assert_eq!(wiki_cnt, 3, "attached wiki segment is query-visible");
    assert!(
        (wiki_sum - 7.5).abs() < 1e-9,
        "attached wiki data round-trips byte-faithfully (sum {wiki_sum})"
    );
    let (clicks_cnt, _) = queried_count_and_sum(&historical, "clicks");
    assert_eq!(clicks_cnt, 3, "attached clicks segment is query-visible");
}

// ---------------------------------------------------------------------------
// --dry-run writes NOTHING
// ---------------------------------------------------------------------------

/// A dry run reports what would be attached but creates neither the
/// SQLite file, nor the deep-storage base dir, nor any blob.
#[test]
fn dry_run_writes_nothing() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = holder.path().join("deep-storage");
    let db_path = holder.path().join("ferrodruid.db");
    make_wiki_raw_layout(druid_root.path());

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
        "--dry-run",
    ]);
    let stdout = stdout_of(&out);
    assert!(stdout.contains("DRY RUN"), "stdout: {stdout}");
    assert!(stdout.contains("WOULD-ATTACH"), "stdout: {stdout}");
    assert!(stdout.contains(WIKI_ID), "stdout: {stdout}");

    assert!(
        !db_path.exists(),
        "--dry-run must not create the metadata store file"
    );
    assert!(
        !ferro_base.exists(),
        "--dry-run must not create the deep-storage base dir"
    );
}

// ---------------------------------------------------------------------------
// id collision: skip by default, replace with --force
// ---------------------------------------------------------------------------

/// Re-attaching the same segment id is refused by default (the existing
/// row is left byte-identical) and replaced with `--force`.
#[tokio::test]
async fn second_attach_skips_existing_id_and_force_replaces() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    make_wiki_raw_layout(druid_root.path());

    let base_args = [
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ];

    let stdout = stdout_of(&run_attach(&base_args));
    assert!(stdout.contains("ATTACHED"), "first run attaches: {stdout}");
    let store = open_store(&db_path).await;
    let first = store
        .get_segment(WIKI_ID)
        .await
        .expect("get")
        .expect("row exists");
    drop(store);

    // Second run: skipped, row untouched.
    let stdout = stdout_of(&run_attach(&base_args));
    assert!(
        stdout.contains("SKIP-EXISTS"),
        "second run must skip the existing id: {stdout}"
    );
    assert!(
        stdout.contains("--force"),
        "skip hints at --force: {stdout}"
    );
    assert!(
        stdout.contains("1 skipped"),
        "summary counts the skip: {stdout}"
    );
    let store = open_store(&db_path).await;
    let second = store
        .get_segment(WIKI_ID)
        .await
        .expect("get")
        .expect("row still exists");
    assert_eq!(
        first.created_date, second.created_date,
        "a skipped row must be left untouched"
    );
    drop(store);

    // --force: replaced (still exactly one used row, still reloadable).
    let mut force_args = base_args.to_vec();
    force_args.push("--force");
    let stdout = stdout_of(&run_attach(&force_args));
    assert!(
        stdout.contains("ATTACHED") && stdout.contains("replaced"),
        "--force must replace: {stdout}"
    );
    let store = open_store(&db_path).await;
    let used = store
        .get_used_segments("wiki")
        .await
        .expect("used segments");
    assert_eq!(used.len(), 1, "replace must not duplicate the row");
}

// ---------------------------------------------------------------------------
// Loud skips: unreadable / unsupported / identity-incomplete
// ---------------------------------------------------------------------------

/// An artifact the v9 reader cannot open (here: FerroDruid's own FDX
/// format) is skipped LOUDLY — reader reason in the report, no metadata
/// row, no blob — while readable neighbours still attach.
#[tokio::test]
async fn unreadable_segment_is_loud_skip_and_writes_nothing_for_it() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    make_clicks_zip_layout(druid_root.path());
    // An FDX segment in a Druid-shaped path: findable, not v9-readable.
    let fdx_dir = druid_root
        .path()
        .join("fdx_ds")
        .join("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z")
        .join("v1")
        .join("0");
    std::fs::create_dir_all(&fdx_dir).expect("mkdir");
    write_segment_fdx(&fixture_segment(), &fdx_dir).expect("write_segment_fdx");

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(stdout.contains("FAILED"), "loud failure line: {stdout}");
    assert!(
        stdout.contains("expected segment version 9"),
        "the v9 reader's fail-loud reason must be surfaced verbatim: {stdout}"
    );
    assert!(
        stdout.contains("1 attached") && stdout.contains("1 failed"),
        "summary must count both outcomes: {stdout}"
    );

    let store = open_store(&db_path).await;
    assert!(
        store
            .get_used_segments("fdx_ds")
            .await
            .expect("used")
            .is_empty(),
        "no metadata row may be committed for an unreadable segment"
    );
    assert!(
        !ferro_base.path().join("fdx_ds").exists(),
        "no blob may be uploaded for an unreadable segment"
    );
    assert!(
        store.get_segment(CLICKS_ID).await.expect("get").is_some(),
        "the readable neighbour still attaches"
    );
}

/// An artifact whose identity cannot be established (no descriptor, no
/// 4-level path) is skipped loudly — attach never guesses an id.
#[tokio::test]
async fn identity_incomplete_is_loud_skip() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    // Non-standard nesting, no descriptor.json.
    let odd = druid_root.path().join("mystery").join("nested");
    std::fs::create_dir_all(&odd).expect("mkdir");
    let staging = tempfile::tempdir().expect("staging");
    write_raw_segment(staging.path());
    pack_index_zip(staging.path(), &odd.join("index.zip"));

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("FAILED") && stdout.contains("identity"),
        "identity-incomplete must be a loud failure: {stdout}"
    );
    let store = open_store(&db_path).await;
    assert!(
        store
            .get_used_segments_all()
            .await
            .expect("all used")
            .is_empty(),
        "nothing may be attached without an established identity"
    );
}

// ---------------------------------------------------------------------------
// --datasource filter
// ---------------------------------------------------------------------------

/// `--datasource` limits the import to one Druid dataSource; everything
/// else is counted as filtered, not failed.
#[tokio::test]
async fn datasource_filter_limits_attach() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    make_wiki_raw_layout(druid_root.path());
    make_clicks_zip_layout(druid_root.path());

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
        "--datasource",
        "wiki",
    ]);
    let stdout = stdout_of(&out);
    assert!(stdout.contains("1 attached"), "stdout: {stdout}");
    assert!(stdout.contains("filtered"), "stdout: {stdout}");

    let store = open_store(&db_path).await;
    assert!(store.get_segment(WIKI_ID).await.expect("get").is_some());
    assert!(
        store.get_segment(CLICKS_ID).await.expect("get").is_none(),
        "the filtered-out datasource must not be attached"
    );
    assert!(
        !ferro_base.path().join("clicks").exists(),
        "no blob for the filtered-out datasource"
    );
}

// ---------------------------------------------------------------------------
// P→M order under failure: a failed upload commits NO row
// ---------------------------------------------------------------------------

/// When the blob upload fails (read-only deep-storage base), NO
/// metadata row is committed — the P→M order means a failure can only
/// ever leave a harmless blob-less state, never a loadSpec row whose
/// blob is missing (which would abort the next startup, H4).
#[cfg(unix)]
#[tokio::test]
async fn upload_failure_commits_no_metadata_row() {
    use std::os::unix::fs::PermissionsExt;

    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    make_wiki_raw_layout(druid_root.path());

    // Read-only base: `upload_segment` cannot create `<base>/<ds>/`.
    let mut ro = std::fs::metadata(ferro_base.path())
        .expect("meta")
        .permissions();
    ro.set_mode(0o555);
    std::fs::set_permissions(ferro_base.path(), ro).expect("chmod ro");

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ]);

    // Restore perms so the tempdir can be cleaned up.
    let mut rw = std::fs::metadata(ferro_base.path())
        .expect("meta")
        .permissions();
    rw.set_mode(0o755);
    std::fs::set_permissions(ferro_base.path(), rw).expect("chmod rw");

    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("FAILED") && stdout.contains("upload"),
        "upload failure must be a loud per-segment failure: {stdout}"
    );
    let store = open_store(&db_path).await;
    assert!(
        store
            .get_used_segments_all()
            .await
            .expect("all used")
            .is_empty(),
        "P→M: a failed upload must commit no metadata row (a loadSpec row \
         without a blob would abort the next serve startup)"
    );
}

// ---------------------------------------------------------------------------
// H4: a PRESENT-but-broken descriptor.json is loud, never a silent
// path-identity fallback
// ---------------------------------------------------------------------------

/// A `descriptor.json` that EXISTS next to the artifact but cannot be
/// decoded must fail the segment loudly — the authoritative identity
/// source being broken must never silently degrade to the path
/// fallback (a hostile/corrupt descriptor could otherwise attach under
/// a wrong identity).
#[tokio::test]
async fn broken_descriptor_is_loud_skip_not_silent_path_fallback() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    // Fully valid PATH identity...
    make_zip_layout(
        druid_root.path(),
        "brokendesc",
        "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z",
        "v1",
        "0",
    );
    // ...but the partition dir carries a malformed descriptor.json.
    let part_dir = druid_root
        .path()
        .join("brokendesc")
        .join("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z")
        .join("v1")
        .join("0");
    std::fs::write(part_dir.join("descriptor.json"), "{ this is not json")
        .expect("write broken descriptor");

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("FAILED") && stdout.contains("descriptor"),
        "a present-but-broken descriptor.json must be a LOUD failure \
         naming the descriptor: {stdout}"
    );
    assert!(
        stdout.contains("0 attached"),
        "nothing may be attached under the silent path fallback: {stdout}"
    );
    let store = open_store(&db_path).await;
    assert!(
        store
            .get_used_segments_all()
            .await
            .expect("all used")
            .is_empty(),
        "no metadata row may be committed for a broken-descriptor segment"
    );
    assert!(
        !ferro_base.path().join("brokendesc").exists(),
        "no blob may be uploaded for a broken-descriptor segment"
    );
}

// ---------------------------------------------------------------------------
// R7: the ferro_id is non-injective; a true collision between two DISTINCT
// identities must be loud, never a silent skip / --force overwrite
// ---------------------------------------------------------------------------

/// Lay a readable zip segment at `part_dir` whose identity is supplied by
/// an authoritative `descriptor.json` (so the id is decoupled from the
/// path — the way a real Druid descriptor drives it).
fn make_descriptor_zip(part_dir: &Path, ds: &str, interval: &str, version: &str, partition: i64) {
    std::fs::create_dir_all(part_dir).expect("mkdir part dir");
    let staging = tempfile::tempdir().expect("staging tempdir");
    write_raw_segment(staging.path());
    pack_index_zip(staging.path(), &part_dir.join("index.zip"));
    std::fs::write(
        part_dir.join("descriptor.json"),
        json!({
            "dataSource": ds,
            "interval": interval,
            "version": version,
            "shardSpec": {"type": "numbered", "partitionNum": partition},
        })
        .to_string(),
    )
    .expect("write descriptor.json");
}

/// The `ferro_id` is `<ds>_<start>_<end>_<version>_<partition>`; because
/// `_` is the separator yet a `dataSource`/`version` may contain `_`, two
/// DISTINCT identities can collapse onto ONE id:
///   A: ds=`x`,            version=`2022-…Z_v`
///   B: ds=`x_2020-…Z`,    version=`v`
/// both → `x_2020-…Z_2021-…Z_2022-…Z_v_0`.  Attaching both must attach
/// exactly ONE and refuse the other LOUDLY — never silently skip it (old
/// HashSet<id> behaviour) or `--force`-overwrite one distinct segment with
/// the other (data loss).
#[tokio::test]
async fn colliding_ferro_id_across_distinct_identities_is_loud_not_data_loss() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");

    make_descriptor_zip(
        &druid_root.path().join("a").join("0"),
        "x",
        "2020-01-01T00:00:00.000Z/2021-01-01T00:00:00.000Z",
        "2022-01-01T00:00:00.000Z_v",
        0,
    );
    make_descriptor_zip(
        &druid_root.path().join("b").join("0"),
        "x_2020-01-01T00:00:00.000Z",
        "2021-01-01T00:00:00.000Z/2022-01-01T00:00:00.000Z",
        "v",
        0,
    );

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("1 attached"),
        "exactly one of the two colliding identities must attach: {stdout}"
    );
    assert!(
        stdout.contains("FAILED") && stdout.contains("DIFFERENT segment identity"),
        "the second, DISTINCT identity that collides onto the same ferro_id must be a \
         LOUD collision refusal, never a silent skip or overwrite: {stdout}"
    );
    let store = open_store(&db_path).await;
    assert_eq!(
        store.get_used_segments_all().await.expect("all used").len(),
        1,
        "a true ferro_id collision must leave exactly one committed segment — never \
         merge two DISTINCT segments into one row (data loss)"
    );
}

// ---------------------------------------------------------------------------
// H5: interval / partition identity fields are VALIDATED, not trusted
// ---------------------------------------------------------------------------

/// A descriptor interval that is not a pair of ISO-8601 instants
/// (`"z/a"`) and a negative partition (`-1`) are loud skips — never
/// attached under a garbage id.
#[tokio::test]
async fn invalid_interval_or_partition_is_loud_skip() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");

    // (a) garbage interval "z/a" from an otherwise valid descriptor.
    make_zip_layout(
        druid_root.path(),
        "badiv",
        "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z",
        "v1",
        "0",
    );
    std::fs::write(
        druid_root
            .path()
            .join("badiv")
            .join("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z")
            .join("v1")
            .join("0")
            .join("descriptor.json"),
        json!({
            "dataSource": "badiv",
            "interval": "z/a",
            "version": "v1",
            "shardSpec": {"type": "numbered", "partitionNum": 0},
        })
        .to_string(),
    )
    .expect("write bad-interval descriptor");

    // (b) negative partitionNum from an otherwise valid descriptor.
    make_zip_layout(
        druid_root.path(),
        "badpart",
        "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z",
        "v1",
        "0",
    );
    std::fs::write(
        druid_root
            .path()
            .join("badpart")
            .join("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z")
            .join("v1")
            .join("0")
            .join("descriptor.json"),
        json!({
            "dataSource": "badpart",
            "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
            "version": "v1",
            "shardSpec": {"type": "numbered", "partitionNum": -1},
        })
        .to_string(),
    )
    .expect("write bad-partition descriptor");

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("0 attached") && stdout.contains("2 failed"),
        "both invalid identities must fail loudly: {stdout}"
    );
    assert!(
        stdout.contains("interval"),
        "the bad-interval reason must name the interval: {stdout}"
    );
    assert!(
        stdout.contains("partition"),
        "the bad-partition reason must name the partition: {stdout}"
    );
    let store = open_store(&db_path).await;
    assert!(
        store
            .get_used_segments_all()
            .await
            .expect("all used")
            .is_empty(),
        "nothing may be attached with an invalid interval/partition"
    );
}

/// Codex R4 H1: a descriptor FIELD that is PRESENT but malformed (a
/// STRING `"7"` partitionNum; a NUMBER interval) next to a perfectly
/// valid partition-`0` path layout must FAIL the segment loudly — the
/// identity merge must never silently backfill the broken field from
/// the path (the string-`"7"` segment would otherwise be COMMITTED
/// under path partition 0, a wrong identity).
#[tokio::test]
async fn present_but_malformed_descriptor_field_is_loud_skip_not_path_fallback() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    let interval = "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z";

    // (a) shardSpec.partitionNum is a STRING — the path says partition 0.
    make_zip_layout(druid_root.path(), "strpart", interval, "v1", "0");
    std::fs::write(
        druid_root
            .path()
            .join("strpart")
            .join(interval)
            .join("v1")
            .join("0")
            .join("descriptor.json"),
        json!({
            "dataSource": "strpart",
            "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
            "version": "v1",
            "shardSpec": {"type": "numbered", "partitionNum": "7"},
        })
        .to_string(),
    )
    .expect("write string-partitionNum descriptor");

    // (b) interval is a NUMBER — the path carries a valid interval.
    make_zip_layout(druid_root.path(), "numiv", interval, "v1", "0");
    std::fs::write(
        druid_root
            .path()
            .join("numiv")
            .join(interval)
            .join("v1")
            .join("0")
            .join("descriptor.json"),
        json!({
            "dataSource": "numiv",
            "interval": 123,
            "version": "v1",
            "shardSpec": {"type": "numbered", "partitionNum": 0},
        })
        .to_string(),
    )
    .expect("write numeric-interval descriptor");

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("0 attached") && stdout.contains("2 failed"),
        "both malformed-field descriptors must fail loudly, nothing may attach \
         under the silently backfilled path identity: {stdout}"
    );
    assert!(
        stdout.contains("partitionNum"),
        "the string-partitionNum reason must name the field: {stdout}"
    );
    assert!(
        stdout.contains("interval"),
        "the numeric-interval reason must name the field: {stdout}"
    );

    let store = open_store(&db_path).await;
    assert!(
        store
            .get_used_segments_all()
            .await
            .expect("all used")
            .is_empty(),
        "no metadata row may be committed for a malformed-field descriptor \
         (the path-fallback identity would be WRONG)"
    );
    assert!(
        !ferro_base.path().join("strpart").exists() && !ferro_base.path().join("numiv").exists(),
        "no blob may be uploaded for a malformed-field descriptor"
    );
}

/// A non-canonical partition directory (`00`) normalizes to the same
/// partition as `0`: the two artifacts synthesize the SAME segment id,
/// so the second is a skip, not a duplicate attach.
#[tokio::test]
async fn noncanonical_partition_deduplicates_against_canonical() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    let interval = "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z";
    make_zip_layout(druid_root.path(), "dup", interval, "v1", "0");
    make_zip_layout(druid_root.path(), "dup", interval, "v1", "00");

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("1 attached") && stdout.contains("1 skipped"),
        "\"00\" and \"0\" are the SAME partition — one attach, one skip: {stdout}"
    );

    let store = open_store(&db_path).await;
    let used = store.get_used_segments("dup").await.expect("used");
    assert_eq!(used.len(), 1, "exactly one row for the one partition");
    assert!(
        used[0].id.ends_with("_v1_0"),
        "the id must carry the CANONICAL partition: {}",
        used[0].id
    );
}

// ---------------------------------------------------------------------------
// H3: --force upload failure leaves the existing row untouched, and the
// failure names the real crash window
// ---------------------------------------------------------------------------

/// When a `--force` re-upload fails midway, the existing metadata row
/// must be left byte-identical (M is only written after a COMPLETE P),
/// and the loud failure must state the honest consequence: the old blob
/// may already be partially overwritten, so the next startup hash check
/// fails loud until `attach --force` is re-run.
#[cfg(unix)]
#[tokio::test]
async fn force_replace_upload_failure_leaves_the_existing_row_untouched() {
    use std::os::unix::fs::PermissionsExt;

    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    make_wiki_raw_layout(druid_root.path());

    let base_args = [
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ];
    let stdout = stdout_of(&run_attach(&base_args));
    assert!(stdout.contains("ATTACHED"), "first attach: {stdout}");
    let store = open_store(&db_path).await;
    let before = store
        .get_segment(WIKI_ID)
        .await
        .expect("get")
        .expect("row exists");
    drop(store);

    // Inject a mid-upload failure: one existing dest blob file is made
    // read-only, so the copy loop fails while REPLACING the blob.
    let blob_file = ferro_base
        .path()
        .join("wiki")
        .join(WIKI_ID)
        .join("meta.smoosh");
    let mut ro = std::fs::metadata(&blob_file).expect("meta").permissions();
    ro.set_mode(0o444);
    std::fs::set_permissions(&blob_file, ro).expect("chmod ro");

    let mut force_args = base_args.to_vec();
    force_args.push("--force");
    let out = run_attach(&force_args);

    let mut rw = std::fs::metadata(&blob_file).expect("meta").permissions();
    rw.set_mode(0o644);
    std::fs::set_permissions(&blob_file, rw).expect("chmod rw");

    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("FAILED") && !stdout.contains("ATTACHED"),
        "the failed --force replace must be loud and not claim success: {stdout}"
    );
    assert!(
        stdout.contains("partially overwritten") && stdout.contains("attach --force"),
        "the failure must state the honest --force crash window (old blob \
         may be partially overwritten; re-run attach --force): {stdout}"
    );

    let store = open_store(&db_path).await;
    let after = store
        .get_segment(WIKI_ID)
        .await
        .expect("get")
        .expect("row still exists");
    assert_eq!(
        before.created_date, after.created_date,
        "a failed --force upload must NOT touch the metadata row"
    );
    assert_eq!(
        before.payload["loadSpec"]["sha256"], after.payload["loadSpec"]["sha256"],
        "the row must keep referencing the OLD committed sha256"
    );
}

// ---------------------------------------------------------------------------
// --allow-unreadable-columns: opt-in lenient attach of segments carrying
// an undecodable (sketch/complex) column
// ---------------------------------------------------------------------------

/// The sketch fixture's ferro segment id.
const SKETCH_ID: &str = "sketchds_2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z_v1_0";

/// DEFAULT (no flag) is byte-identical strict behavior: a segment with
/// one undecodable `thetaSketch` column fails WHOLE-segment, loudly,
/// with the reader's reason — nothing committed.
#[tokio::test]
async fn sketch_column_fails_whole_segment_strict_by_default() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    make_sketch_raw_layout(druid_root.path(), "sketchds", &[]);

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("FAILED") && stdout.contains("0 attached"),
        "strict default must fail the whole segment: {stdout}"
    );
    assert!(
        stdout.contains("sketchcol") && stdout.contains("unsupported column value type"),
        "the reader's reason must name the sketch column: {stdout}"
    );
    let store = open_store(&db_path).await;
    assert!(
        store
            .get_used_segments_all()
            .await
            .expect("all used")
            .is_empty(),
        "no metadata row may be committed under the strict default"
    );
    assert!(
        !ferro_base.path().join("sketchds").exists(),
        "no blob may be uploaded under the strict default"
    );
}

/// The crown lenient E2E: `--allow-unreadable-columns` drops JUST the
/// sketch column with a LOUD manifest (report line + metadata payload),
/// re-writes the blob WITHOUT it (strict-openable, names absent from
/// the raw archive), and the remaining columns are query-visible after
/// the real bootstrap reload — while a query naming the dropped column
/// behaves EXACTLY like one naming a column that never existed.
#[tokio::test]
async fn allow_unreadable_columns_drops_sketch_and_attaches_rest() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    make_sketch_raw_layout(druid_root.path(), "sketchds", &[]);
    // A fully-readable neighbour: the flag must not change its handling.
    make_clicks_zip_layout(druid_root.path());

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
        "--allow-unreadable-columns",
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("2 attached") && stdout.contains(SKETCH_ID),
        "both segments attach under the flag: {stdout}"
    );
    assert!(
        stdout.contains("dropped 1 unreadable column(s): [sketchcol]"),
        "the report must carry the LOUD dropped-column manifest: {stdout}"
    );
    assert_eq!(
        stdout.matches("dropped 1 unreadable column").count(),
        1,
        "the fully-readable neighbour must NOT get a manifest line: {stdout}"
    );

    // ---- M: the row records the manifest durably ------------------------
    let store = open_store(&db_path).await;
    let row = store
        .get_segment(SKETCH_ID)
        .await
        .expect("get sketch row")
        .expect("sketch row exists");
    assert_eq!(
        row.payload["droppedUnreadableColumns"],
        json!(["sketchcol"]),
        "payload must carry the dropped-column manifest"
    );
    assert_eq!(row.payload["numRows"], json!(3));

    // ---- P: the blob GENUINELY lacks the dropped column ------------------
    let blob = ferro_base.path().join("sketchds").join(SKETCH_ID);
    let smoosh = ferrodruid_segment::SmooshReader::open(&blob).expect("open blob archive");
    assert!(
        smoosh.file_names().iter().all(|n| !n.contains("sketchcol")),
        "no sketchcol entry (data or descriptor) may exist in the uploaded archive"
    );
    let reread = SegmentData::open(&blob)
        .expect("the pruned blob must open with the STRICT reader (the bootstrap reload uses it)");
    assert!(reread.column("sketchcol").is_none());
    assert!(
        !reread.metrics.iter().any(|m| m == "sketchcol")
            && !reread.dimensions.iter().any(|d| d == "sketchcol"),
        "the dropped column may not be declared in the pruned index.drd"
    );
    assert_eq!(reread.num_rows(), 3);
    // The committed sha256 covers the REWRITTEN bytes.
    assert_eq!(
        row.payload["loadSpec"]["sha256"].as_str().expect("sha256"),
        blob_content_hash(&blob).expect("re-hash blob"),
        "the recorded sha256 must match the pruned blob (the reload's integrity check)"
    );

    // ---- The next-startup path: bootstrap reload → query ------------------
    let metadata = Arc::new(store);
    let cache = tempfile::tempdir().expect("historical cache");
    let historical = Arc::new(ferrodruid_historical::Historical::new(
        cache.path().to_path_buf(),
        10_000_000,
    ));
    let deep_storage: Arc<dyn DeepStorage> =
        Arc::new(LocalDeepStorage::new(ferro_base.path().to_path_buf()));
    let overlord = ferrodruid_overlord::Overlord::with_executor(
        Arc::clone(&metadata),
        Arc::clone(&historical),
    )
    .with_deep_storage(deep_storage);
    let reloaded = overlord
        .bootstrap_reload_segments()
        .await
        .expect("bootstrap reload succeeds over the pruned blob");
    assert_eq!(reloaded, 2, "both attached segments reload");

    let (cnt, sum) = queried_count_and_sum(&historical, "sketchds");
    assert_eq!(cnt, 3, "the surviving columns are query-visible");
    assert!(
        (sum - 7.5).abs() < 1e-9,
        "the surviving metric round-trips faithfully (sum {sum})"
    );

    // A query naming the DROPPED column behaves exactly like one naming
    // a column that NEVER existed (Druid null semantics for an absent
    // column) — never a value fabricated from the undecodable bytes.
    let query: ferrodruid_query::DruidQuery = serde_json::from_value(json!({
        "queryType": "timeseries",
        "dataSource": {"type": "table", "name": "sketchds"},
        "intervals": ["2000-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z"],
        "granularity": "all",
        "aggregations": [
            {"type": "doubleSum", "name": "s_dropped", "fieldName": "sketchcol"},
            {"type": "doubleSum", "name": "s_never", "fieldName": "never_existed"},
        ]
    }))
    .expect("build query");
    let results = historical.execute_query(&query).expect("execute query");
    let mut saw_row = false;
    for r in &results {
        if let ferrodruid_query::QueryResult::Timeseries(ts) = r {
            for row in ts {
                saw_row = true;
                assert!(
                    row.result
                        .get("s_dropped")
                        .is_some_and(serde_json::Value::is_null),
                    "the dropped column must aggregate to SQL null, got {:?}",
                    row.result.get("s_dropped")
                );
                assert_eq!(
                    row.result.get("s_dropped"),
                    row.result.get("s_never"),
                    "dropped column must behave IDENTICALLY to a never-existed column"
                );
            }
        }
    }
    assert!(saw_row, "the timeseries must emit a row");
}

/// Even WITH the flag, a segment left without queryable data is a loud
/// per-segment failure: an unreadable `__time`, or EVERY dimension
/// unreadable, refuses the partial import — nothing committed.
#[tokio::test]
async fn allow_unreadable_columns_still_fails_timeless_or_dimensionless() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    // (a) __time undecodable (truncated LONG blob).
    make_sketch_raw_layout(
        druid_root.path(),
        "badtime",
        &[("__time", truncated_column_bytes())],
    );
    // (b) the ONLY dimension undecodable (truncated STRING blob).
    make_sketch_raw_layout(
        druid_root.path(),
        "baddims",
        &[("region", truncated_column_bytes())],
    );

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
        "--allow-unreadable-columns",
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("0 attached") && stdout.contains("2 failed"),
        "both no-queryable-data segments must fail loudly even with the flag: {stdout}"
    );
    assert!(
        stdout.contains("__time") && stdout.contains("no queryable data"),
        "the timeless failure names __time: {stdout}"
    );
    assert!(
        stdout.contains("EVERY dimension"),
        "the all-dimensions failure names the rule: {stdout}"
    );

    let store = open_store(&db_path).await;
    assert!(
        store
            .get_used_segments_all()
            .await
            .expect("all used")
            .is_empty(),
        "nothing may be committed for a no-queryable-data segment"
    );
    assert!(
        !ferro_base.path().join("badtime").exists() && !ferro_base.path().join("baddims").exists(),
        "no blob may be uploaded for a no-queryable-data segment"
    );
}

// ---------------------------------------------------------------------------
// --allow-unreadable-columns: a pruned REWRITE the STRICT reader cannot
// re-open must be a CLEAN per-segment failure, never a committed blob
// ---------------------------------------------------------------------------

/// Encode a Druid-native generic-indexed container of UTF-8 strings
/// (the layout `parse_generic_indexed` reads): `[version=1][flags=0]
/// [u32 body_size]` then `[u32 num][u32 end_offset × num]` and elements
/// of `[i32 marker=0][utf8 bytes]`.
fn native_generic_indexed_strings(items: &[&str]) -> Vec<u8> {
    let mut elems = Vec::new();
    let mut end_offsets = Vec::new();
    for s in items {
        elems.extend_from_slice(&0_i32.to_be_bytes()); // non-null marker
        elems.extend_from_slice(s.as_bytes());
        end_offsets.push(u32::try_from(elems.len()).expect("element region fits u32"));
    }
    let mut body = Vec::new();
    body.extend_from_slice(
        &u32::try_from(items.len())
            .expect("count fits u32")
            .to_be_bytes(),
    );
    for end in &end_offsets {
        body.extend_from_slice(&end.to_be_bytes());
    }
    body.extend_from_slice(&elems);
    let mut out = vec![1u8, 0u8];
    out.extend_from_slice(
        &u32::try_from(body.len())
            .expect("body fits u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(&body);
    out
}

/// Encode an UPSTREAM-Apache-Druid-layout `index.drd`: the all-columns
/// list, the dimensions list, the interval bounds, and the roaring
/// bitmap-codec trailer (metrics = columns minus dimensions).
fn native_index_drd(all_columns: &[&str], dims: &[&str], start_ms: i64, end_ms: i64) -> Vec<u8> {
    let mut out = native_generic_indexed_strings(all_columns);
    out.extend_from_slice(&native_generic_indexed_strings(dims));
    out.extend_from_slice(&start_ms.to_be_bytes());
    out.extend_from_slice(&end_ms.to_be_bytes());
    let codec = br#"{"type":"roaring"}"#;
    out.extend_from_slice(
        &u32::try_from(codec.len())
            .expect("codec fits u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(codec);
    out
}

/// Raw Druid-31-style layout whose `index.drd` is the NATIVE layout,
/// declaring a dimension name LONGER than the 65,535-byte u16 length
/// prefix of FerroDruid's own index.drd encoding (only the native
/// layout can carry such a name) plus the droppable `sketchcol` — so a
/// lenient attach must take the prune-and-REWRITE path, and the rewrite
/// cannot faithfully represent the giant name.
fn make_giant_name_raw_layout(root: &Path, ds: &str, giant_dim: &str) {
    let part_dir = root
        .join(ds)
        .join("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z")
        .join("v1")
        .join("0");
    let (meta, chunks) =
        ferrodruid_segment::write_segment_v9_to_memory(&fixture_segment()).expect("write fixture");
    let mut files = smoosh_files_of(&meta, &chunks[0]);
    for entry in &mut files {
        if entry.0 == "index.drd" {
            entry.1 = native_index_drd(
                &["value", "sketchcol", giant_dim],
                &[giant_dim],
                1_442_016_000_000,
                1_442_023_200_000,
            );
        } else if entry.0 == "region" {
            entry.0 = giant_dim.to_string();
        } else if entry.0 == "region.column_descriptor.json" {
            entry.0 = format!("{giant_dim}.column_descriptor.json");
        }
    }
    files.push((
        "sketchcol.column_descriptor.json".to_string(),
        br#"{"valueType":"thetaSketch"}"#.to_vec(),
    ));
    files.push(("sketchcol".to_string(), vec![0xDE, 0xAD, 0xBE, 0xEF]));
    write_smoosh_files(&part_dir.join("index"), &files);
    std::fs::write(
        part_dir.join("descriptor.json"),
        json!({
            "dataSource": ds,
            "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
            "version": "v1",
            "shardSpec": {"type": "numbered", "partitionNum": 0},
        })
        .to_string(),
    )
    .expect("write descriptor.json");
}

/// The HIGH finding: a lenient attach whose pruned rewrite would be
/// STRICT-unreadable must be a CLEAN per-segment failure — nothing
/// hashed, uploaded, or committed — never a committed malformed blob
/// that bricks the next `serve` bootstrap reload (which re-opens every
/// committed blob with the STRICT reader).  Concrete species: a native
/// Druid `index.drd` carries a column name longer than 65,535 bytes;
/// the lenient decode accepts it, but the FerroDruid index.drd encoding
/// stores name lengths behind a u16 prefix — pre-fix the length was
/// silently truncated, the rewrite "succeeded", and the corrupt blob
/// was committed.
#[tokio::test]
async fn lenient_rewrite_that_would_brick_reload_fails_clean_nothing_committed() {
    let druid_root = tempfile::tempdir().expect("druid root");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let db_path = meta_dir.path().join("ferrodruid.db");
    let giant = "d".repeat(usize::from(u16::MAX) + 2);
    make_giant_name_raw_layout(druid_root.path(), "giantds", &giant);

    let out = run_attach(&[
        "--deep-storage",
        druid_root.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db_path.to_str().expect("utf8"),
        "--allow-unreadable-columns",
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("0 attached") && stdout.contains("1 failed"),
        "the un-rewritable segment must be a clean per-segment FAILURE, \
         not a committed malformed blob: {stdout}"
    );
    assert!(
        stdout.contains("re-writ"),
        "the reason must point at the failed/unverifiable rewrite: {stdout}"
    );

    // NOTHING partial reached the store or deep storage.
    let store = open_store(&db_path).await;
    assert!(
        store
            .get_used_segments_all()
            .await
            .expect("all used")
            .is_empty(),
        "no metadata row may be committed for the failed rewrite"
    );
    assert!(
        !ferro_base.path().join("giantds").exists(),
        "no deep-storage blob may be uploaded for the failed rewrite"
    );
}

// ---------------------------------------------------------------------------
// Help wording — honest scope
// ---------------------------------------------------------------------------

/// `attach --help` states the honest scope: offline import for the
/// single-binary deployment, visible after the next startup's bootstrap
/// reload.
#[test]
fn attach_help_states_scope() {
    let out = Command::new(migrate_bin())
        .args(["attach", "--help"])
        .output()
        .expect("spawn help");
    assert!(out.status.success(), "attach --help must exist and exit 0");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("single-binary"),
        "help must state the single-binary scope: {stdout}"
    );
    assert!(
        stdout.contains("restart") || stdout.contains("next start"),
        "help must state segments become visible on the next startup: {stdout}"
    );
}
