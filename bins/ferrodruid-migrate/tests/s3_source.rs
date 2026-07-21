// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Integration tests for the W-C `s3://` source support in
//! `ferrodruid-migrate`:
//!
//! * `import-druid-metadata` FETCHES an `s3_zip` loadSpec row's
//!   `index.zip` directly from S3 (bucket + key from the row, client
//!   from the `AWS_*` environment) instead of skipping it;
//! * `attach --deep-storage s3://bucket/prefix` lists + stages the
//!   whole prefix into a local tempdir and runs the existing
//!   scan/identity/attach tail over it;
//! * `assess --deep-storage s3://bucket/prefix` gets the same staging.
//!
//! The always-on tests need NO network: they pin the loud failure modes
//! (unreachable endpoint → per-row FAILED / operational error, hostile
//! bucket names refused) and, critically, that an `s3_zip` row is no
//! longer a `SKIP-LOADSPEC`.
//!
//! The `#[ignore]`d MinIO tests are the real E2E (the compat-5 "free
//! docker, no AWS billing" precedent): they put a real v9 `index.zip`
//! into MinIO under the Druid S3 key layout, run BOTH subcommands
//! against it, then run the REAL next-startup path
//! (`Overlord::bootstrap_reload_segments`) and query the attached
//! segment. Run them with the existing MinIO stack:
//!
//! ```sh
//! docker compose -f tests/deep-storage-compat/docker-compose.yml up -d
//! FERRODRUID_MINIO_ENDPOINT=http://localhost:9100 \
//!   cargo test -p ferrodruid-migrate --test s3_source -- --ignored
//! docker compose -f tests/deep-storage-compat/docker-compose.yml down -v
//! ```

use std::io::Write as _;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use ferrodruid_deep_storage::{DeepStorage, LocalDeepStorage};
use ferrodruid_metadata::MetadataStore;
use ferrodruid_segment::{SegmentData, SegmentDataBuilder, write_segment_v9};
use serde_json::json;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const WIKI_INTERVAL: &str = "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z";
const WIKI_INTERVAL_DIR: &str = "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z";

/// The fixture's ferro segment id (path/payload identity, version v1,
/// partition 0).
const WIKI_ID: &str = "wiki_2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z_v1_0";

fn migrate_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ferrodruid-migrate")
}

/// Run a `ferrodruid-migrate` subcommand with extra environment
/// variables (the `AWS_*` client config) injected per-process — no
/// global env mutation.
fn run_migrate(subcommand: &str, args: &[&str], envs: &[(&str, String)]) -> Output {
    let mut cmd = Command::new(migrate_bin());
    cmd.arg(subcommand).args(args);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.output().expect("spawn ferrodruid-migrate")
}

/// An endpoint on localhost that is GUARANTEED closed: bind an
/// ephemeral port, remember it, drop the listener.
fn closed_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    format!("http://127.0.0.1:{port}")
}

/// `AWS_*` env pointing the S3 client at a CLOSED local endpoint —
/// every fetch/list fails fast with a connection error, proving the
/// code path genuinely attempts S3 I/O.
fn closed_port_env() -> Vec<(&'static str, String)> {
    vec![
        ("AWS_ENDPOINT", closed_endpoint()),
        ("AWS_ALLOW_HTTP", "true".to_string()),
        ("AWS_ACCESS_KEY_ID", "test".to_string()),
        ("AWS_SECRET_ACCESS_KEY", "test".to_string()),
        ("AWS_REGION", "us-east-1".to_string()),
    ]
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
/// `index.zip` and return the zip bytes.
fn index_zip_bytes(smoosh_dir: &Path) -> Vec<u8> {
    let mut buf = std::io::Cursor::new(Vec::new());
    {
        let mut zip = zip::ZipWriter::new(&mut buf);
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
            let bytes = std::fs::read(smoosh_dir.join(&name)).expect("read smoosh file");
            zip.start_file(&name, options).expect("zip start_file");
            zip.write_all(&bytes).expect("zip write");
        }
        zip.finish().expect("zip finish");
    }
    buf.into_inner()
}

/// The canonical fixture packed as `index.zip` bytes.
fn fixture_zip_bytes() -> Vec<u8> {
    let staging = tempfile::tempdir().expect("staging tempdir");
    write_segment_v9(&fixture_segment(), staging.path()).expect("write_segment_v9");
    index_zip_bytes(staging.path())
}

/// A canonical Druid segment payload JSON with an `s3_zip` loadSpec.
fn s3_payload(ds: &str, version: &str, partition: i64, bucket: &str, key: &str) -> String {
    json!({
        "dataSource": ds,
        "interval": WIKI_INTERVAL,
        "version": version,
        "loadSpec": {"type": "s3_zip", "bucket": bucket, "key": key},
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
/// `druid_segments` table (same shape as the compat-7 fixtures).
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

// ---------------------------------------------------------------------------
// Always-on: import-druid-metadata fetches s3_zip rows (no more skip)
// ---------------------------------------------------------------------------

/// An `s3_zip` row must be FETCHED from S3, not skipped: pointed at a
/// closed endpoint the row becomes a loud per-row FAILURE naming the
/// fetch, never a `SKIP-LOADSPEC`, and the run still exits 0 (per-row
/// fail-safe).
#[tokio::test]
async fn import_s3_zip_row_is_fetched_not_skipped() {
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let target_db = holder.path().join("ferrodruid.db");

    let payload = s3_payload(
        "wiki",
        "v1",
        0,
        "druid-deep",
        "druid/segments/wiki/iv/v1/0/index.zip",
    );
    let src_db = make_source_db(holder.path(), &[("wiki_s3", "wiki", 1, payload)]).await;

    let out = run_migrate(
        "import-druid-metadata",
        &[
            "--source-uri",
            &format!("sqlite://{}", src_db.display()),
            "--ferro-deep-storage",
            ferro_base.path().to_str().expect("utf8"),
            "--metadata-uri",
            target_db.to_str().expect("utf8"),
        ],
        &closed_port_env(),
    );
    assert!(
        out.status.success(),
        "a per-row s3 fetch failure must not abort the run: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        !stdout.contains("SKIP-LOADSPEC"),
        "an s3_zip row must no longer be a loadSpec skip: {stdout}"
    );
    assert!(
        stdout.contains("FAILED"),
        "the unreachable-endpoint fetch must be a loud per-row failure: {stdout}"
    );
    assert!(
        stdout.contains("s3://druid-deep"),
        "the failure reason must name the s3 source: {stdout}"
    );
    assert!(stdout.contains("0 imported"), "nothing imported: {stdout}");

    // Nothing may be committed for the failed row.
    let store = MetadataStore::new_sqlite(target_db.to_str().expect("utf8"))
        .await
        .expect("open target");
    store.initialize().await.expect("init");
    assert!(
        store
            .get_used_segments("wiki")
            .await
            .expect("used wiki")
            .is_empty(),
        "no metadata row may be committed for a failed s3 fetch"
    );
}

/// A hostile bucket name from the untrusted source row is refused by
/// VALIDATION — a loud per-row failure with no network I/O at all (no
/// AWS_* env is set, so any contact attempt would also fail, but the
/// reason must name the bucket validation, not a connection error).
#[tokio::test]
async fn import_s3_zip_hostile_bucket_is_loud_validation_failure() {
    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let target_db = holder.path().join("ferrodruid.db");

    let payload = s3_payload("wiki", "v1", 0, "../evil", "k/index.zip");
    let src_db = make_source_db(holder.path(), &[("wiki_bad", "wiki", 1, payload)]).await;

    let out = run_migrate(
        "import-druid-metadata",
        &[
            "--source-uri",
            &format!("sqlite://{}", src_db.display()),
            "--ferro-deep-storage",
            ferro_base.path().to_str().expect("utf8"),
            "--metadata-uri",
            target_db.to_str().expect("utf8"),
        ],
        &[],
    );
    assert!(out.status.success(), "per-row failure, run continues");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        !stdout.contains("SKIP-LOADSPEC"),
        "not a loadSpec-type skip: {stdout}"
    );
    assert!(
        stdout.contains("FAILED") && stdout.contains("bucket"),
        "the reason must name the refused bucket: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// Always-on: attach / assess accept s3:// URLs
// ---------------------------------------------------------------------------

/// `attach --deep-storage s3://…` must be treated as an S3 source
/// (list and stage), NOT as a local path: pointed at a closed endpoint
/// it fails loudly naming the S3 listing, never with the local "is not
/// a directory" complaint.
#[test]
fn attach_s3_url_is_staged_not_treated_as_local_dir() {
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let holder = tempfile::tempdir().expect("holder");
    let target_db = holder.path().join("ferrodruid.db");

    let out = run_migrate(
        "attach",
        &[
            "--deep-storage",
            "s3://ferrodruid-test/druid/segments",
            "--ferro-deep-storage",
            ferro_base.path().to_str().expect("utf8"),
            "--metadata-uri",
            target_db.to_str().expect("utf8"),
        ],
        &closed_port_env(),
    );
    assert!(
        !out.status.success(),
        "an unreachable S3 source is an operational failure"
    );
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        !stderr.contains("is not a directory"),
        "an s3:// URL must not be probed as a local path: {stderr}"
    );
    assert!(
        stderr.contains("s3://ferrodruid-test"),
        "the error must name the s3 source: {stderr}"
    );
}

/// A malformed `s3://` URL (empty bucket) is refused loudly with the
/// expected form named, before any network contact.
#[test]
fn attach_malformed_s3_url_is_refused() {
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let holder = tempfile::tempdir().expect("holder");
    let target_db = holder.path().join("ferrodruid.db");

    let out = run_migrate(
        "attach",
        &[
            "--deep-storage",
            "s3://",
            "--ferro-deep-storage",
            ferro_base.path().to_str().expect("utf8"),
            "--metadata-uri",
            target_db.to_str().expect("utf8"),
        ],
        &[],
    );
    assert!(!out.status.success(), "malformed s3 URL must be refused");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        stderr.contains("s3://") && stderr.contains("bucket"),
        "the error names the expected s3://bucket[/prefix] form: {stderr}"
    );
    assert!(
        !stderr.contains("is not a directory"),
        "must not fall through to the local-path probe: {stderr}"
    );
}

/// `assess --deep-storage s3://…` gets the same staging (symmetry): a
/// closed endpoint is a loud S3 error, not a local-path complaint.
#[test]
fn assess_s3_url_is_staged_not_treated_as_local_dir() {
    let out = run_migrate(
        "assess",
        &["--deep-storage", "s3://ferrodruid-test/druid/segments"],
        &closed_port_env(),
    );
    assert!(!out.status.success(), "unreachable S3 source fails loudly");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        !stderr.contains("is not a directory"),
        "an s3:// URL must not be probed as a local path: {stderr}"
    );
    assert!(
        stderr.contains("s3://ferrodruid-test"),
        "the error must name the s3 source: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// MinIO E2E (free docker; gated like the deep-storage compat suite)
// ---------------------------------------------------------------------------

/// MinIO client config for one E2E run.
struct MinioCfg {
    /// `AWS_*` env injected into the spawned binary.
    envs: Vec<(&'static str, String)>,
    endpoint: String,
    bucket: String,
}

/// MinIO client config from the environment, or `None` to skip.
fn minio_env() -> Option<MinioCfg> {
    let endpoint = std::env::var("FERRODRUID_MINIO_ENDPOINT").ok()?;
    let bucket = std::env::var("FERRODRUID_MINIO_BUCKET")
        .unwrap_or_else(|_| "ferrodruid-compat".to_string());
    let access_key =
        std::env::var("FERRODRUID_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key =
        std::env::var("FERRODRUID_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let envs = vec![
        ("AWS_ENDPOINT", endpoint.clone()),
        ("AWS_ALLOW_HTTP", "true".to_string()),
        ("AWS_ACCESS_KEY_ID", access_key),
        ("AWS_SECRET_ACCESS_KEY", secret_key),
        ("AWS_REGION", "us-east-1".to_string()),
    ];
    Some(MinioCfg {
        envs,
        endpoint,
        bucket,
    })
}

/// Build a raw `object_store` client against MinIO for test-side puts.
fn minio_store(
    envs: &[(&'static str, String)],
    endpoint: &str,
    bucket: &str,
) -> object_store::aws::AmazonS3 {
    let find = |k: &str| {
        envs.iter()
            .find(|(key, _)| *key == k)
            .map(|(_, v)| v.clone())
            .expect("env entry")
    };
    object_store::aws::AmazonS3Builder::new()
        .with_endpoint(endpoint.to_string())
        .with_allow_http(true)
        .with_bucket_name(bucket.to_string())
        .with_region("us-east-1")
        .with_access_key_id(find("AWS_ACCESS_KEY_ID"))
        .with_secret_access_key(find("AWS_SECRET_ACCESS_KEY"))
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("MinIO builder")
}

async fn put_object(store: &object_store::aws::AmazonS3, key: &str, bytes: Vec<u8>) {
    use object_store::ObjectStore as _;
    store
        .put(
            &object_store::path::Path::parse(key).expect("key parses"),
            object_store::PutPayload::from(bytes),
        )
        .await
        .expect("put object");
}

/// Unique per-run key prefix so concurrent/repeated runs never collide.
fn unique_prefix(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("wc-e2e/{tag}-{nanos}")
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

/// Run the REAL next-startup path (`Overlord::bootstrap_reload_segments`)
/// over the store + deep storage the CLI wrote and assert the wiki
/// fixture is query-visible (count 3, sum 7.5).
async fn assert_bootstrap_reload_queryable(
    db_path: &Path,
    ferro_base: &Path,
    expect_reloaded: usize,
) {
    let store = open_store(db_path).await;
    let metadata = Arc::new(store);
    let cache = tempfile::tempdir().expect("historical cache");
    let historical = Arc::new(ferrodruid_historical::Historical::new(
        cache.path().to_path_buf(),
        10_000_000,
    ));
    let deep_storage: Arc<dyn DeepStorage> =
        Arc::new(LocalDeepStorage::new(ferro_base.to_path_buf()));
    let overlord = ferrodruid_overlord::Overlord::with_executor(
        Arc::clone(&metadata),
        Arc::clone(&historical),
    )
    .with_deep_storage(deep_storage);

    let reloaded = overlord
        .bootstrap_reload_segments()
        .await
        .expect("bootstrap reload succeeds over s3-sourced segments");
    assert_eq!(reloaded, expect_reloaded, "attached segments reload");
    let (cnt, sum) = queried_count_and_sum(&historical, "wiki");
    assert_eq!(cnt, 3, "s3-sourced wiki segment is query-visible");
    assert!(
        (sum - 7.5).abs() < 1e-9,
        "s3-sourced wiki data round-trips byte-faithfully (sum {sum})"
    );
}

/// Crown E2E (attach): a real Druid-layout S3 tree
/// (`<prefix>/wiki/<interval>/<version>/<partition>/index.zip`) in
/// MinIO → `attach --deep-storage s3://…` (composed with
/// `--allow-unreadable-columns` and `--datasource`) → bootstrap reload
/// → query PASS.
#[tokio::test]
#[ignore = "requires running MinIO (tests/deep-storage-compat/docker-compose.yml)"]
async fn minio_attach_s3_tree_end_to_end() {
    let Some(cfg) = minio_env() else {
        eprintln!("[skip] FERRODRUID_MINIO_ENDPOINT not set");
        return;
    };
    let (envs, bucket) = (&cfg.envs, &cfg.bucket);
    let store = minio_store(envs, &cfg.endpoint, bucket);
    let run = unique_prefix("attach");
    let prefix = format!("{run}/druid/segments");

    // The wiki fixture under the Druid S3 key layout, plus a second
    // datasource that must be filtered out at LISTING time.
    let zip = fixture_zip_bytes();
    put_object(
        &store,
        &format!("{prefix}/wiki/{WIKI_INTERVAL_DIR}/v1/0/index.zip"),
        zip.clone(),
    )
    .await;
    put_object(
        &store,
        &format!("{prefix}/clicks/{WIKI_INTERVAL_DIR}/v1/0/index.zip"),
        zip.clone(),
    )
    .await;

    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let holder = tempfile::tempdir().expect("holder");
    let db_path = holder.path().join("ferrodruid.db");

    let out = run_migrate(
        "attach",
        &[
            "--deep-storage",
            &format!("s3://{bucket}/{prefix}"),
            "--ferro-deep-storage",
            ferro_base.path().to_str().expect("utf8"),
            "--metadata-uri",
            db_path.to_str().expect("utf8"),
            "--datasource",
            "wiki",
            "--allow-unreadable-columns",
        ],
        envs,
    );
    assert!(
        out.status.success(),
        "attach from MinIO failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(stdout.contains("ATTACHED"), "stdout: {stdout}");
    assert!(stdout.contains(WIKI_ID), "stdout: {stdout}");
    assert!(stdout.contains("1 attached"), "stdout: {stdout}");
    assert!(stdout.contains("0 failed"), "stdout: {stdout}");

    // clicks was filtered at listing time — never attached.
    let store_sql = open_store(&db_path).await;
    assert!(
        store_sql
            .get_used_segments("clicks")
            .await
            .expect("used clicks")
            .is_empty(),
        "--datasource must filter the other datasource"
    );
    drop(store_sql);

    assert_bootstrap_reload_queryable(&db_path, ferro_base.path(), 1).await;
}

/// Crown E2E (import): a `druid_segments` row with an `s3_zip` loadSpec
/// naming the MinIO bucket/key → `import-druid-metadata` → bootstrap
/// reload → query PASS.
#[tokio::test]
#[ignore = "requires running MinIO (tests/deep-storage-compat/docker-compose.yml)"]
async fn minio_import_s3_zip_row_end_to_end() {
    let Some(cfg) = minio_env() else {
        eprintln!("[skip] FERRODRUID_MINIO_ENDPOINT not set");
        return;
    };
    let (envs, bucket) = (&cfg.envs, &cfg.bucket);
    let store = minio_store(envs, &cfg.endpoint, bucket);
    let run = unique_prefix("import");
    let key = format!("{run}/druid/segments/wiki/{WIKI_INTERVAL_DIR}/v1/0/index.zip");
    put_object(&store, &key, fixture_zip_bytes()).await;

    let holder = tempfile::tempdir().expect("holder");
    let ferro_base = tempfile::tempdir().expect("ferro deep-storage");
    let db_path = holder.path().join("ferrodruid.db");

    let payload = s3_payload("wiki", "v1", 0, bucket, &key);
    let src_db = make_source_db(holder.path(), &[("wiki_s3", "wiki", 1, payload)]).await;

    let out = run_migrate(
        "import-druid-metadata",
        &[
            "--source-uri",
            &format!("sqlite://{}", src_db.display()),
            "--ferro-deep-storage",
            ferro_base.path().to_str().expect("utf8"),
            "--metadata-uri",
            db_path.to_str().expect("utf8"),
        ],
        envs,
    );
    assert!(
        out.status.success(),
        "import from MinIO failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(stdout.contains("IMPORTED"), "stdout: {stdout}");
    assert!(stdout.contains(WIKI_ID), "stdout: {stdout}");
    assert!(stdout.contains("1 imported"), "stdout: {stdout}");
    assert!(!stdout.contains("SKIP-LOADSPEC"), "stdout: {stdout}");

    assert_bootstrap_reload_queryable(&db_path, ferro_base.path(), 1).await;
}
