// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Integration tests for `ferrodruid-migrate import-druid-metadata` —
//! the compat-7 Druid metadata-DB segment importer.
//!
//! The always-on source fixture is a SQLite `druid_segments` table laid
//! out Druid-style (quoted `"start"`/`"end"`, INTEGER `used`, TEXT
//! `payload`) whose payload `loadSpec.local.path` values point at
//! FerroDruid-written v9 blobs packed as Druid `index.zip` artifacts.
//! Every test drives the real binary via
//! `CARGO_BIN_EXE_ferrodruid-migrate`, and the crown test then runs the
//! REAL post-import path — the same `Overlord::bootstrap_reload_segments`
//! sweep the next `ferrodruid serve` startup runs — and asserts the
//! imported segment is query-visible with the expected aggregates
//! (mirroring compat-2's `attach_then_bootstrap_reload_makes_segments_queryable`).

use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use ferrodruid_deep_storage::{DeepStorage, LocalDeepStorage, blob_content_hash};
use ferrodruid_metadata::MetadataStore;
use ferrodruid_segment::{SegmentData, SegmentDataBuilder, write_segment_v9};
use serde_json::json;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The wiki fixture's ferro segment id, synthesized from the PAYLOAD
/// identity (`<ds>_<start>_<end>_<version>_<partition>`).
const WIKI_ID: &str =
    "wiki_2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z_2026-01-01T00:00:00.000Z_0";

const WIKI_INTERVAL: &str = "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z";
const WIKI_VERSION: &str = "2026-01-01T00:00:00.000Z";

fn migrate_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ferrodruid-migrate")
}

fn run_import(args: &[&str]) -> Output {
    Command::new(migrate_bin())
        .arg("import-druid-metadata")
        .args(args)
        .output()
        .expect("spawn ferrodruid-migrate import-druid-metadata")
}

fn stdout_of(out: &Output) -> String {
    assert!(
        out.status.success(),
        "import exited non-zero: stdout={} stderr={}",
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

/// Write the canonical fixture as a Druid-style
/// `<root>/<ds>/<interval-dir>/<version>/<partition>/index.zip` and
/// return the zip's absolute path.
fn make_druid_blob(root: &Path, ds: &str, version: &str, partition: &str) -> std::path::PathBuf {
    let interval_dir = "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z";
    let part_dir = root
        .join(ds)
        .join(interval_dir)
        .join(version)
        .join(partition);
    std::fs::create_dir_all(&part_dir).expect("mkdir partition dir");
    let staging = tempfile::tempdir().expect("staging tempdir");
    write_segment_v9(&fixture_segment(), staging.path()).expect("write_segment_v9");
    let zip_path = part_dir.join("index.zip");
    pack_index_zip(staging.path(), &zip_path);
    zip_path
}

/// A canonical Druid segment payload JSON with a `local` loadSpec.
fn local_payload(ds: &str, version: &str, partition: i64, path: &str) -> String {
    json!({
        "dataSource": ds,
        "interval": WIKI_INTERVAL,
        "version": version,
        "loadSpec": {"type": "local", "path": path},
        "dimensions": "region",
        "metrics": "value",
        "shardSpec": {"type": "numbered", "partitionNum": partition, "partitions": 1},
        "binaryVersion": 9,
        "size": 1024,
        "identifier": format!("{ds}_{WIKI_INTERVAL}_{version}_{partition}"),
    })
    .to_string()
}

/// Create the SQLite source fixture DB with a Druid-shaped
/// `druid_segments` table and the given `(id, dataSource, used,
/// payload)` rows.  Returns the DB file path.
///
/// The row's authoritative `start`/`end`/`version` COLUMNS are derived
/// from each payload's `interval`/`version` so the fixture is
/// internally CONSISTENT (Druid keeps them byte-equal) — the compat-7
/// importer now refuses a row whose payload interval/version disagree
/// with its columns, so an inconsistent fixture would be (correctly)
/// skipped.
async fn make_source_db(dir: &Path, rows: &[(&str, &str, i64, String)]) -> std::path::PathBuf {
    let db_path = dir.join("druid-source.db");
    let opts = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true);
    let pool = sqlx::SqlitePool::connect_with(opts)
        .await
        .expect("open source fixture db");
    sqlx::query(
        "CREATE TABLE druid_segments (\
         id VARCHAR(255) NOT NULL PRIMARY KEY, \
         dataSource VARCHAR(255) NOT NULL, \
         created_date VARCHAR(255) NOT NULL, \
         \"start\" VARCHAR(255) NOT NULL, \
         \"end\" VARCHAR(255) NOT NULL, \
         partitioned BOOLEAN NOT NULL, \
         version VARCHAR(255) NOT NULL, \
         used BOOLEAN NOT NULL, \
         payload TEXT NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("create druid_segments");
    for (id, ds, used, payload) in rows {
        let v: serde_json::Value =
            serde_json::from_str(payload).expect("fixture payload is valid JSON");
        let interval = v["interval"].as_str().expect("payload carries an interval");
        let (start, end) = interval.split_once('/').expect("interval is start/end");
        let version = v["version"].as_str().expect("payload carries a version");
        sqlx::query(
            "INSERT INTO druid_segments \
             (id, dataSource, created_date, \"start\", \"end\", partitioned, version, used, payload) \
             VALUES (?, ?, '2026-01-01T00:00:00.000Z', ?, ?, 1, ?, ?, ?)",
        )
        .bind(id)
        .bind(ds)
        .bind(start)
        .bind(end)
        .bind(version)
        .bind(used)
        .bind(payload)
        .execute(&pool)
        .await
        .expect("insert source row");
    }
    pool.close().await;
    db_path
}

async fn open_store(db_path: &Path) -> MetadataStore {
    let store = MetadataStore::new_sqlite(db_path.to_str().expect("utf8 db path"))
        .await
        .expect("open sqlite store");
    store.initialize().await.expect("init store");
    store
}

/// Timeseries `count` + `doubleSum(value)` over the datasource — the
/// same query shape the product serves.
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
// The crown E2E: source DB → import → bootstrap reload → query-visible
// ---------------------------------------------------------------------------

/// Import from a SQLite Druid metadata source whose used row points at
/// a real v9 `index.zip`, then run the REAL next-startup path —
/// `Overlord::bootstrap_reload_segments` over the store + deep storage
/// the CLI wrote — and assert the imported segment is query-visible
/// with the expected row count and aggregate.  Also pins:
/// * `used = false` rows are NOT imported (the semantic difference from
///   `attach`);
/// * the P→M artifacts (blob in the FerroDruid layout; the row's
///   `loadSpec.sha256` matches a re-hash of the uploaded blob);
/// * rules are COUNTED, not imported;
/// * the source DB file is byte-identical after the run (READ-ONLY).
#[tokio::test]
async fn import_then_bootstrap_reload_makes_segment_queryable() {
    let blobs = tempfile::tempdir().expect("druid blobs");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let target_db = meta_dir.path().join("ferrodruid.db");

    let zip = make_druid_blob(blobs.path(), "wiki", WIKI_VERSION, "0");
    let zip_str = zip.to_str().expect("utf8").to_string();
    let src_db = make_source_db(
        holder.path(),
        &[
            (
                "wiki_used",
                "wiki",
                1,
                local_payload("wiki", WIKI_VERSION, 0, &zip_str),
            ),
            // A coordinator-dropped segment: valid local blob, used = 0.
            (
                "wiki_dropped",
                "wiki",
                0,
                local_payload("wiki", "2026-02-02T00:00:00.000Z", 0, &zip_str),
            ),
        ],
    )
    .await;
    // Rules exist in the source: they must be counted, never imported.
    {
        let pool = sqlx::SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::new().filename(&src_db),
        )
        .await
        .expect("reopen source");
        sqlx::query("CREATE TABLE druid_rules (id VARCHAR(255) PRIMARY KEY, payload TEXT)")
            .execute(&pool)
            .await
            .expect("create druid_rules");
        sqlx::query("INSERT INTO druid_rules (id, payload) VALUES ('r1', '[]'), ('r2', '[]')")
            .execute(&pool)
            .await
            .expect("insert rules");
        pool.close().await;
    }
    let source_bytes_before = std::fs::read(&src_db).expect("read source db");

    let out = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        blobs.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(stdout.contains("IMPORTED"), "stdout: {stdout}");
    assert!(stdout.contains(WIKI_ID), "stdout: {stdout}");
    assert!(
        stdout.contains("1 imported"),
        "exactly the used row imports: {stdout}"
    );
    assert!(
        !stdout.contains("2026-02-02T00:00:00.000Z"),
        "the used = false row must not be processed at all: {stdout}"
    );
    assert!(
        stdout.contains("used = true"),
        "the report states the used-only semantic: {stdout}"
    );
    assert!(
        stdout.contains("2 rule row(s)") && stdout.contains("NOT imported"),
        "rules are counted, not imported: {stdout}"
    );

    // READ-ONLY source: byte-identical after the run.
    let source_bytes_after = std::fs::read(&src_db).expect("re-read source db");
    assert_eq!(
        source_bytes_before, source_bytes_after,
        "the source Druid metadata DB must not be modified"
    );

    // ---- M: the metadata row the CLI committed ---------------------------
    let store = open_store(&target_db).await;
    let wiki = store
        .get_segment(WIKI_ID)
        .await
        .expect("get wiki row")
        .expect("wiki row exists");
    assert!(wiki.used, "imported row must be used");
    assert_eq!(wiki.data_source, "wiki");
    assert_eq!(
        wiki.start, "2015-09-12T00:00:00.000Z",
        "payload interval start"
    );
    assert_eq!(wiki.end, "2015-09-13T00:00:00.000Z", "payload interval end");
    assert_eq!(wiki.version, WIKI_VERSION, "payload version kept");
    let load_spec = wiki
        .payload
        .get("loadSpec")
        .expect("imported row carries a loadSpec");
    assert_eq!(load_spec["type"], "local");
    assert_eq!(load_spec["segmentId"], WIKI_ID);
    assert_eq!(
        store.get_used_segments_all().await.expect("all used").len(),
        1,
        "the used = false source row must not produce a target row"
    );

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
        "recorded sha256 must match a re-hash of the uploaded blob"
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
        .expect("bootstrap reload succeeds over the imported segment");
    assert_eq!(reloaded, 1, "the imported segment reloads");
    assert!(historical.is_initial_load_complete());

    let (cnt, sum) = queried_count_and_sum(&historical, "wiki");
    assert_eq!(cnt, 3, "imported segment is query-visible");
    assert!(
        (sum - 7.5).abs() < 1e-9,
        "imported data round-trips byte-faithfully (sum {sum})"
    );
}

// ---------------------------------------------------------------------------
// R5: payload identity must agree with the authoritative row columns
// ---------------------------------------------------------------------------

/// A source row whose PAYLOAD interval/version disagree with its
/// authoritative `start`/`end`/`version` COLUMNS must be a per-row
/// FAILED skip — never imported under the payload's fabricated
/// identity.  The consistent neighbour imports and is query-visible
/// under its TRUE (column) identity.
#[tokio::test]
async fn import_refuses_payload_identity_mismatch_with_columns() {
    let blobs = tempfile::tempdir().expect("druid blobs");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let target_db = meta_dir.path().join("ferrodruid.db");

    let zip = make_druid_blob(blobs.path(), "wiki", WIKI_VERSION, "0");
    let zip_str = zip.to_str().expect("utf8").to_string();

    // A CONSISTENT row (columns derived from the payload) that imports.
    let src_db = make_source_db(
        holder.path(),
        &[(
            "wiki_ok",
            "wiki",
            1,
            local_payload("wiki", WIKI_VERSION, 0, &zip_str),
        )],
    )
    .await;

    // Two MISMATCHED rows whose columns DISAGREE with their payload —
    // inserted raw (bypassing the column-deriving fixture helper) with a
    // valid local artifact so the ONLY reason to fail is the identity
    // gate.
    let mismatch_payload = |version: &str, interval: &str| {
        json!({
            "dataSource": "wiki",
            "interval": interval,
            "version": version,
            "loadSpec": {"type": "local", "path": zip_str.clone()},
            "dimensions": "region",
            "metrics": "value",
            "shardSpec": {"type": "numbered", "partitionNum": 0, "partitions": 1},
            "binaryVersion": 9,
            "size": 1024,
            "identifier": "wiki_mismatch",
        })
        .to_string()
    };
    {
        let pool = sqlx::SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::new().filename(&src_db),
        )
        .await
        .expect("reopen source");
        let raw_insert = "INSERT INTO druid_segments \
             (id, dataSource, created_date, \"start\", \"end\", partitioned, version, used, payload) \
             VALUES (?, 'wiki', 'c', ?, ?, 1, ?, 1, ?)";
        // Version disagreement: columns v1 vs payload v2.
        sqlx::query(raw_insert)
            .bind("wiki_badver")
            .bind("2024-01-01T00:00:00.000Z")
            .bind("2024-01-02T00:00:00.000Z")
            .bind("v1")
            .bind(mismatch_payload(
                "v2",
                "2030-01-01T00:00:00.000Z/2030-01-02T00:00:00.000Z",
            ))
            .execute(&pool)
            .await
            .expect("insert version-mismatch row");
        // Interval disagreement: version matches, columns 2024 vs payload 2030.
        sqlx::query(raw_insert)
            .bind("wiki_badint")
            .bind("2024-01-01T00:00:00.000Z")
            .bind("2024-01-02T00:00:00.000Z")
            .bind("samever")
            .bind(mismatch_payload(
                "samever",
                "2030-01-01T00:00:00.000Z/2030-01-02T00:00:00.000Z",
            ))
            .execute(&pool)
            .await
            .expect("insert interval-mismatch row");
        pool.close().await;
    }

    let out = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        blobs.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    // The consistent row imports under its TRUE (column) identity.
    assert!(
        stdout.contains("IMPORTED") && stdout.contains(WIKI_ID),
        "the consistent row imports under the column identity: {stdout}"
    );
    assert!(
        stdout.contains("1 imported") && stdout.contains("2 failed"),
        "the two mismatched rows are per-row failures: {stdout}"
    );
    assert!(
        stdout.contains("version") && stdout.contains("disagrees"),
        "the version-mismatch reason is reported: {stdout}"
    );
    assert!(
        stdout.contains("interval") && stdout.contains("columns"),
        "the interval-mismatch reason is reported: {stdout}"
    );

    // The store holds ONLY the true-identity row — no fabricated 2030 id.
    let store = open_store(&target_db).await;
    assert!(
        store.get_segment(WIKI_ID).await.expect("get").is_some(),
        "the true (column) identity is committed"
    );
    assert_eq!(
        store.get_used_segments_all().await.expect("all used").len(),
        1,
        "no row imports under a fabricated payload identity"
    );

    // Query-visible under the TRUE identity via the real next-startup path.
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
        .expect("bootstrap reload over the consistent segment");
    assert_eq!(reloaded, 1, "only the true-identity segment reloads");
    let (cnt, sum) = queried_count_and_sum(&historical, "wiki");
    assert_eq!(cnt, 3, "the consistent segment is query-visible");
    assert!(
        (sum - 7.5).abs() < 1e-9,
        "data round-trips under the true identity (sum {sum})"
    );
}

// ---------------------------------------------------------------------------
// --dry-run writes NOTHING
// ---------------------------------------------------------------------------

/// A dry run reports what would be imported but creates neither the
/// target SQLite file, nor the deep-storage base dir, nor any blob —
/// and leaves the source byte-identical.
#[tokio::test]
async fn import_dry_run_writes_nothing() {
    let blobs = tempfile::tempdir().expect("druid blobs");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = holder.path().join("deep-storage");
    let target_db = holder.path().join("ferrodruid.db");

    let zip = make_druid_blob(blobs.path(), "wiki", WIKI_VERSION, "0");
    let src_db = make_source_db(
        holder.path(),
        &[(
            "wiki_used",
            "wiki",
            1,
            local_payload("wiki", WIKI_VERSION, 0, zip.to_str().expect("utf8")),
        )],
    )
    .await;
    let source_bytes_before = std::fs::read(&src_db).expect("read source db");

    let out = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        blobs.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
        "--dry-run",
    ]);
    let stdout = stdout_of(&out);
    assert!(stdout.contains("DRY RUN"), "stdout: {stdout}");
    assert!(stdout.contains("WOULD-IMPORT"), "stdout: {stdout}");
    assert!(stdout.contains(WIKI_ID), "stdout: {stdout}");

    assert!(
        !target_db.exists(),
        "--dry-run must not create the target metadata store file"
    );
    assert!(
        !ferro_base.exists(),
        "--dry-run must not create the deep-storage base dir"
    );
    assert_eq!(
        source_bytes_before,
        std::fs::read(&src_db).expect("re-read source db"),
        "--dry-run must not modify the source DB"
    );
}

// ---------------------------------------------------------------------------
// R6: --dry-run writes NOTHING to the target (no create, no grow, no DDL)
// ---------------------------------------------------------------------------

/// `--dry-run` pointed at an EXISTING but empty (0-byte) SQLite target
/// must leave it 0 bytes with NO `druid_segments` table — it must never
/// run `initialize()` DDL that would grow the file — while still
/// reporting the source rows as WouldImport and exiting Ok.
#[tokio::test]
async fn dry_run_does_not_create_or_grow_a_fresh_sqlite_target() {
    let blobs = tempfile::tempdir().expect("druid blobs");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = holder.path().join("deep-storage");
    let target_db = holder.path().join("empty-target.db");
    // An EXISTING, empty (0-byte) target file.
    std::fs::File::create(&target_db).expect("create empty target file");
    assert_eq!(
        std::fs::metadata(&target_db).expect("stat").len(),
        0,
        "the target starts empty"
    );

    let zip = make_druid_blob(blobs.path(), "wiki", WIKI_VERSION, "0");
    let src_db = make_source_db(
        holder.path(),
        &[(
            "wiki_used",
            "wiki",
            1,
            local_payload("wiki", WIKI_VERSION, 0, zip.to_str().expect("utf8")),
        )],
    )
    .await;

    let out = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        blobs.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
        "--dry-run",
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("WOULD-IMPORT") && stdout.contains(WIKI_ID),
        "the dry run still reports the source row: {stdout}"
    );

    // The target file is STILL 0 bytes — no `initialize()` DDL ran.
    assert_eq!(
        std::fs::metadata(&target_db).expect("stat").len(),
        0,
        "--dry-run must not grow the target from 0 bytes (no DDL)"
    );
    // And it has NO druid_segments table (read-only probe via the new API).
    let store = MetadataStore::open_sqlite_read_only(target_db.to_str().expect("utf8"))
        .await
        .expect("open the empty target read-only");
    assert!(
        !store.is_initialized().await.expect("probe target schema"),
        "--dry-run must not create the target schema"
    );
}

/// A `--dry-run` against an ALREADY-initialized, populated target still
/// reports existing-segment skips accurately (the read-only probe opens
/// the store; `get_segment` is consulted) and writes nothing.
#[tokio::test]
async fn dry_run_against_initialized_store_reports_existing_skips() {
    let blobs = tempfile::tempdir().expect("druid blobs");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let target_db = holder.path().join("ferrodruid.db");

    let zip = make_druid_blob(blobs.path(), "wiki", WIKI_VERSION, "0");
    let src_db = make_source_db(
        holder.path(),
        &[(
            "wiki_used",
            "wiki",
            1,
            local_payload("wiki", WIKI_VERSION, 0, zip.to_str().expect("utf8")),
        )],
    )
    .await;

    // A REAL import first, to populate the target.
    let real = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        blobs.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
    ]);
    assert!(
        stdout_of(&real).contains("1 imported"),
        "the real import populates the target"
    );
    let bytes_after_real = std::fs::read(&target_db).expect("read populated target");

    // Now a dry run: the segment already exists → SKIP-EXISTS, no write.
    let dry = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        blobs.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
        "--dry-run",
    ]);
    let stdout = stdout_of(&dry);
    assert!(
        stdout.contains("SKIP-EXISTS"),
        "the already-present segment is reported as a skip: {stdout}"
    );
    assert!(
        !stdout.contains("WOULD-IMPORT"),
        "an existing segment is not a would-import: {stdout}"
    );
    assert_eq!(
        bytes_after_real,
        std::fs::read(&target_db).expect("re-read target"),
        "--dry-run must not modify an already-initialized target"
    );
}

// ---------------------------------------------------------------------------
// --datasource filter
// ---------------------------------------------------------------------------

/// `--datasource` limits the import to one Druid dataSource at the
/// source SELECT; other datasources are not even processed.
#[tokio::test]
async fn import_datasource_filter_limits_import() {
    let blobs = tempfile::tempdir().expect("druid blobs");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let target_db = holder.path().join("ferrodruid.db");

    let wiki_zip = make_druid_blob(blobs.path(), "wiki", WIKI_VERSION, "0");
    let clicks_zip = make_druid_blob(blobs.path(), "clicks", "v1", "0");
    let src_db = make_source_db(
        holder.path(),
        &[
            (
                "wiki_row",
                "wiki",
                1,
                local_payload("wiki", WIKI_VERSION, 0, wiki_zip.to_str().expect("utf8")),
            ),
            (
                "clicks_row",
                "clicks",
                1,
                local_payload("clicks", "v1", 0, clicks_zip.to_str().expect("utf8")),
            ),
        ],
    )
    .await;

    let out = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        blobs.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
        "--datasource",
        "wiki",
    ]);
    let stdout = stdout_of(&out);
    assert!(stdout.contains("1 imported"), "stdout: {stdout}");
    assert!(
        stdout.contains("1 source row(s) processed"),
        "the filtered-out datasource is not even read: {stdout}"
    );

    let store = open_store(&target_db).await;
    assert!(store.get_segment(WIKI_ID).await.expect("get").is_some());
    assert!(
        store
            .get_used_segments("clicks")
            .await
            .expect("used clicks")
            .is_empty(),
        "the filtered-out datasource must not be imported"
    );
    assert!(
        !ferro_base.path().join("clicks").exists(),
        "no blob for the filtered-out datasource"
    );
}

// ---------------------------------------------------------------------------
// Non-local loadSpec: loud skip, run continues
// ---------------------------------------------------------------------------

/// An s3_zip row is a LOUD per-segment skip (with the remediation hint)
/// while the local neighbour still imports — the run never aborts.
#[tokio::test]
async fn non_local_loadspec_is_loud_skip_while_local_imports() {
    let blobs = tempfile::tempdir().expect("druid blobs");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let target_db = holder.path().join("ferrodruid.db");

    let zip = make_druid_blob(blobs.path(), "wiki", WIKI_VERSION, "0");
    let s3_payload = json!({
        "dataSource": "clicks",
        "interval": WIKI_INTERVAL,
        "version": "v1",
        "loadSpec": {"type": "s3_zip", "bucket": "druid-deep", "key": "clicks/0/index.zip"},
        "dimensions": "region",
        "metrics": "value",
        "shardSpec": {"type": "numbered", "partitionNum": 0, "partitions": 1},
        "binaryVersion": 9,
        "size": 1024,
        "identifier": "clicks_s3",
    })
    .to_string();
    let src_db = make_source_db(
        holder.path(),
        &[
            (
                "wiki_local",
                "wiki",
                1,
                local_payload("wiki", WIKI_VERSION, 0, zip.to_str().expect("utf8")),
            ),
            ("clicks_s3", "clicks", 1, s3_payload),
        ],
    )
    .await;

    let out = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        blobs.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("SKIP-LOADSPEC") && stdout.contains("s3_zip"),
        "the s3_zip row must be a loud skip naming the type: {stdout}"
    );
    assert!(
        stdout.contains("--deep-storage"),
        "the skip must carry the pull-locally remediation hint: {stdout}"
    );
    assert!(
        stdout.contains("1 imported") && stdout.contains("1 skipped (unsupported loadSpec)"),
        "summary counts both outcomes: {stdout}"
    );

    let store = open_store(&target_db).await;
    assert!(store.get_segment(WIKI_ID).await.expect("get").is_some());
    assert!(
        store
            .get_used_segments("clicks")
            .await
            .expect("used clicks")
            .is_empty(),
        "no metadata row may be committed for a skipped loadSpec"
    );
}

// ---------------------------------------------------------------------------
// --deep-storage remap: foreign absolute paths resolve by suffix
// ---------------------------------------------------------------------------

/// The source records the Druid HOST's absolute deep-storage path; the
/// operator rsynced that tree under a local dir and passes
/// `--deep-storage`.  The importer resolves the path by longest suffix
/// under the base and the segment imports end-to-end.
#[tokio::test]
async fn import_remaps_foreign_absolute_loadspec_path() {
    let base = tempfile::tempdir().expect("copied deep-storage");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let target_db = holder.path().join("ferrodruid.db");

    // Blob at <base>/wiki/<interval>/<version>/0/index.zip; the DB
    // records /opt/druid/var/druid/segments/wiki/…/index.zip.
    let local_zip = make_druid_blob(base.path(), "wiki", WIKI_VERSION, "0");
    let foreign = format!(
        "/opt/druid/var/druid/segments/{}",
        local_zip
            .strip_prefix(base.path())
            .expect("under base")
            .display()
    );
    let src_db = make_source_db(
        holder.path(),
        &[(
            "wiki_foreign",
            "wiki",
            1,
            local_payload("wiki", WIKI_VERSION, 0, &foreign),
        )],
    )
    .await;

    let out = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        base.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("IMPORTED") && stdout.contains(WIKI_ID),
        "the foreign absolute path must resolve under --deep-storage: {stdout}"
    );

    let store = open_store(&target_db).await;
    assert!(
        store.get_segment(WIKI_ID).await.expect("get").is_some(),
        "remapped segment must be committed"
    );
}

// ---------------------------------------------------------------------------
// --deep-storage is a REAL containment ceiling (H1 regression)
// ---------------------------------------------------------------------------

/// H1 regression (compat-7 security review): an absolute loadSpec path
/// naming a real, readable file OUTSIDE `--deep-storage` must be a
/// per-row FAILED — the external file is never read (the /etc/shadow
/// class).  And without `--deep-storage` there is no containment
/// ceiling at all, so a local loadSpec cannot resolve either.
#[tokio::test]
async fn import_refuses_loadspec_path_outside_deep_storage() {
    let blobs = tempfile::tempdir().expect("external druid blobs");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let target_db = holder.path().join("ferrodruid.db");
    // The ceiling: an existing dir that does NOT contain the blob.
    let ceiling = tempfile::tempdir().expect("containment ceiling");

    let zip = make_druid_blob(blobs.path(), "wiki", WIKI_VERSION, "0");
    let src_db = make_source_db(
        holder.path(),
        &[(
            "wiki_outside",
            "wiki",
            1,
            local_payload("wiki", WIKI_VERSION, 0, zip.to_str().expect("utf8")),
        )],
    )
    .await;

    // With --deep-storage: the existing external file must NOT import.
    let out = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        ceiling.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("FAILED"),
        "an absolute path outside --deep-storage must be a per-row FAILED: {stdout}"
    );
    assert!(
        stdout.contains("0 imported"),
        "nothing may import from outside the ceiling: {stdout}"
    );
    assert!(
        !ferro_base.path().join("wiki").exists(),
        "no blob may be staged/uploaded from outside the ceiling"
    );

    // Without --deep-storage: no ceiling exists, so the row must FAIL
    // with the remediation hint (never read the absolute path as-is).
    let out = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
    ]);
    let stdout = stdout_of(&out);
    assert!(
        stdout.contains("FAILED") && stdout.contains("0 imported"),
        "an absolute path without --deep-storage must not import: {stdout}"
    );
    assert!(
        stdout.contains("--deep-storage"),
        "the failure must hint at --deep-storage: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// source == target refusal
// ---------------------------------------------------------------------------

/// Passing the SAME database as both source and target is refused
/// loudly BEFORE anything runs (swapped flags would otherwise
/// bootstrap/write the "read-only" source).
#[tokio::test]
async fn import_source_equals_target_is_refused() {
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let db = holder.path().join("one-and-the-same.db");
    // A pre-existing FerroDruid store (Druid-compatible schema) — the
    // realistic swapped-flags shape.
    drop(open_store(&db).await);

    let out = run_import(&[
        "--source-uri",
        &format!("sqlite://{}", db.display()),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        db.to_str().expect("utf8"),
    ]);
    assert!(
        !out.status.success(),
        "source == target must exit non-zero: stdout={}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("SAME database"),
        "the refusal must name the problem: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Help wording — honest scope
// ---------------------------------------------------------------------------

/// `import-druid-metadata --help` states the honest scope: used=true
/// only, local loadSpec only, read-only source, Derby unsupported,
/// restart to become visible.
#[test]
fn import_help_states_scope() {
    let out = Command::new(migrate_bin())
        .args(["import-druid-metadata", "--help"])
        .output()
        .expect("spawn help");
    assert!(
        out.status.success(),
        "import-druid-metadata --help must exist and exit 0"
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    for needle in [
        "used = true",
        "local",
        "READ-ONLY",
        "Derby",
        "restart",
        "single-binary",
    ] {
        assert!(
            stdout.contains(needle),
            "help must mention {needle:?}: {stdout}"
        );
    }
}

// ---------------------------------------------------------------------------
// R7: operational-error stderr is sanitized against escape injection
// ---------------------------------------------------------------------------

/// An operational error carrying untrusted data (here a `--source-uri`
/// bearing a raw ESC clear-screen sequence) must not reach stderr with
/// the terminal-escape bytes intact — the single `main()` error boundary
/// sanitizes every subcommand's `Err` exactly like the report path.
#[test]
fn operational_error_stderr_is_sanitized_against_escape_injection() {
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let target_db = holder.path().join("ferrodruid.db");
    // A NON-EXISTENT source SQLite path carrying a raw ESC (`\x1b[2J`,
    // clear-screen) byte; the read-only open fails and the path lands in
    // the operational error.
    let evil_source = "/tmp/ferrodruid-missing-\x1b[2J-source.db";
    assert!(
        evil_source.contains('\u{1b}'),
        "the fixture must carry a raw ESC byte"
    );

    let out = run_import(&[
        "--source-uri",
        evil_source,
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
    ]);
    assert!(
        !out.status.success(),
        "a missing source DB must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains('\u{1b}'),
        "operational-error stderr must not carry raw terminal escapes: {stderr:?}"
    );
    assert!(
        stderr.contains("Error:"),
        "a human-readable error is still printed: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// --allow-unreadable-columns through the shared attach tail
// ---------------------------------------------------------------------------

/// The sketch fixture's ferro segment id (interval as WIKI_INTERVAL).
const SKETCH_IMPORT_ID: &str = "sketchds_2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z_v1_0";

/// The canonical fixture repacked with ONE extra metric `sketchcol`
/// declared in `index.drd` whose sidecar descriptor names a value type
/// the v9 reader cannot decode (`thetaSketch`), written as smoosh files
/// into `dir` (mirrors the attach test's fixture builder).
fn write_raw_segment_with_sketch(dir: &Path) {
    let (meta, chunks) =
        ferrodruid_segment::write_segment_v9_to_memory(&fixture_segment()).expect("write fixture");
    let mut files: Vec<(String, Vec<u8>)> = meta
        .lines()
        .skip(1)
        .map(|line| {
            let parts: Vec<&str> = line.split(',').collect();
            let start: usize = parts[2].parse().expect("entry start");
            let end: usize = parts[3].parse().expect("entry end");
            (parts[0].to_string(), chunks[0][start..end].to_vec())
        })
        .collect();
    for entry in &mut files {
        if entry.0 == "index.drd" {
            entry.1 = ferrodruid_segment::v9::encode_index_drd(
                &["region"],
                &["value", "sketchcol"],
                1_442_016_000_000,
                1_442_023_200_000,
                1,
            );
        }
    }
    files.push((
        "sketchcol.column_descriptor.json".to_string(),
        br#"{"valueType":"thetaSketch"}"#.to_vec(),
    ));
    files.push(("sketchcol".to_string(), vec![0xDE, 0xAD, 0xBE, 0xEF]));

    std::fs::create_dir_all(dir).expect("mkdir smoosh dir");
    let mut chunk = Vec::new();
    let mut meta_lines = vec![format!("v1,2147483647,{}", files.len())];
    for (name, bytes) in &files {
        let start = chunk.len();
        chunk.extend_from_slice(bytes);
        meta_lines.push(format!("{name},0,{start},{}", chunk.len()));
    }
    std::fs::write(dir.join("meta.smoosh"), meta_lines.join("\n")).expect("write meta.smoosh");
    std::fs::write(dir.join("00000.smoosh"), &chunk).expect("write chunk");
}

/// `import-druid-metadata --allow-unreadable-columns` flows through the
/// SAME lenient attach tail: without the flag the sketch-bearing row is
/// a loud per-row FAILURE (strict default, unchanged); with the flag it
/// imports minus the sketch column, with the loud manifest in the
/// report AND the row payload, and a strict-openable pruned blob.
#[tokio::test]
async fn allow_unreadable_columns_imports_sketch_row_minus_sketch_column() {
    let blobs = tempfile::tempdir().expect("druid blobs");
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let meta_dir = tempfile::tempdir().expect("metadata dir");
    let target_db = meta_dir.path().join("ferrodruid.db");

    // The sketch blob, zipped Druid-style under the blobs root.
    let part_dir = blobs
        .path()
        .join("sketchds")
        .join("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z")
        .join("v1")
        .join("0");
    std::fs::create_dir_all(&part_dir).expect("mkdir partition dir");
    let staging = tempfile::tempdir().expect("staging tempdir");
    write_raw_segment_with_sketch(staging.path());
    let zip_path = part_dir.join("index.zip");
    pack_index_zip(staging.path(), &zip_path);
    let zip_str = zip_path.to_str().expect("utf8").to_string();

    let src_db = make_source_db(
        holder.path(),
        &[(
            "sketch_row",
            "sketchds",
            1,
            local_payload("sketchds", "v1", 0, &zip_str),
        )],
    )
    .await;

    let base_args = [
        "--source-uri",
        &format!("sqlite://{}", src_db.display()),
        "--deep-storage",
        blobs.path().to_str().expect("utf8"),
        "--ferro-deep-storage",
        ferro_base.path().to_str().expect("utf8"),
        "--metadata-uri",
        target_db.to_str().expect("utf8"),
    ];

    // Strict default: the whole row fails loudly, nothing committed.
    let stdout = stdout_of(&run_import(&base_args));
    assert!(
        stdout.contains("FAILED") && stdout.contains("0 imported"),
        "strict default must fail the sketch-bearing row: {stdout}"
    );
    assert!(
        stdout.contains("sketchcol") && stdout.contains("unsupported column value type"),
        "the reader's reason must name the sketch column: {stdout}"
    );
    assert!(
        !ferro_base.path().join("sketchds").exists(),
        "no blob may be uploaded under the strict default"
    );

    // Opt-in lenient: imports minus the sketch column, loudly.
    let mut lenient_args = base_args.to_vec();
    lenient_args.push("--allow-unreadable-columns");
    let stdout = stdout_of(&run_import(&lenient_args));
    assert!(
        stdout.contains("1 imported") && stdout.contains(SKETCH_IMPORT_ID),
        "the row must import under the flag: {stdout}"
    );
    assert!(
        stdout.contains("dropped 1 unreadable column(s): [sketchcol]"),
        "the import report must carry the LOUD dropped-column manifest: {stdout}"
    );

    // Row manifest + genuinely pruned, strict-openable blob.
    let store = open_store(&target_db).await;
    let row = store
        .get_segment(SKETCH_IMPORT_ID)
        .await
        .expect("get row")
        .expect("row exists");
    assert_eq!(
        row.payload["droppedUnreadableColumns"],
        json!(["sketchcol"])
    );
    let blob = ferro_base.path().join("sketchds").join(SKETCH_IMPORT_ID);
    let reread = SegmentData::open(&blob).expect("pruned blob opens with the STRICT reader");
    assert!(reread.column("sketchcol").is_none());
    assert!(!reread.metrics.iter().any(|m| m == "sketchcol"));
    assert_eq!(reread.num_rows(), 3);
    assert_eq!(
        row.payload["loadSpec"]["sha256"].as_str().expect("sha256"),
        blob_content_hash(&blob).expect("re-hash blob"),
        "the recorded sha256 must match the pruned blob"
    );
}
