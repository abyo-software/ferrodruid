// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use ferrodruid_metadata::MetadataStore;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod assess;
mod attach;
mod gen_certs;
mod import_druid;
mod s3_source;

/// FerroDruid migration tool — assess/attach Druid v9 segments,
/// import/export metadata + W1-I cross-role cert bootstrap.
#[derive(Parser)]
#[command(name = "ferrodruid-migrate", version, about)]
struct Cli {
    /// Subcommand to execute.
    #[command(subcommand)]
    command: Command,
}

/// Available migration subcommands.
#[derive(Subcommand)]
enum Command {
    /// Dry-run assessment of a Druid deep-storage tree: scan for
    /// segment artifacts and report, per segment, whether it is
    /// readable by FerroDruid's v9 reader — attachable with
    /// `ferrodruid-migrate attach`. Judgment tool only: nothing is
    /// migrated or modified (an s3:// source is downloaded into a
    /// throwaway local staging dir first). Local directories and
    /// s3://bucket/prefix sources are supported; for HDFS/GCS/Azure
    /// pull the segments to a local dir first.
    Assess {
        /// Druid deep-storage root: a local directory
        /// (`druid.storage.storageDirectory`) or `s3://bucket/prefix`
        /// (`druid.storage.bucket`/`baseKey`; AWS_* env configures the
        /// client, AWS_ENDPOINT + AWS_ALLOW_HTTP=true for
        /// S3-compatible stores). Both `index.zip` artifacts and
        /// already-unzipped smoosh directories are recognized.
        #[arg(long)]
        deep_storage: PathBuf,
        /// Emit a machine-readable JSON report to stdout instead of
        /// the human-readable table.
        #[arg(long)]
        json: bool,
        /// Assess at most N segment artifacts (the scan stops once N
        /// have been collected; the report notes the truncation).
        #[arg(long, value_name = "N")]
        max_segments: Option<usize>,
    },
    /// Attach existing Apache Druid v9 segments to a FerroDruid
    /// single-binary deployment: import each readable segment's blob
    /// into FerroDruid's deep-storage layout and commit its metadata
    /// row (blob first, row second — per-segment crash-safe). Nothing
    /// is loaded eagerly: the segments become query-visible after the
    /// next `ferrodruid serve` startup (restart), whose bootstrap
    /// reload downloads, hash-verifies, and loads every attached
    /// segment. Scope: single-binary deployments; the SOURCE may be a
    /// local directory or s3://bucket/prefix (staged to a local
    /// tempdir first — a dry run still downloads), while the TARGET
    /// FerroDruid deep storage stays local (the distributed role
    /// binaries do not serve attached v9 blobs); segments the v9
    /// reader cannot open (e.g. unsupported encodings) are skipped
    /// loudly — run `assess` first for a readability map. Run offline:
    /// stop the instance (or attach before first start); concurrent
    /// writers to the metadata store are unsupported.
    Attach {
        /// Druid deep-storage root to import from: a local directory
        /// (`druid.storage.storageDirectory`) or `s3://bucket/prefix`
        /// (`druid.storage.bucket`/`baseKey`; AWS_* env configures the
        /// client — AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_REGION,
        /// plus AWS_ENDPOINT + AWS_ALLOW_HTTP=true for S3-compatible
        /// stores like MinIO). Both `index.zip` artifacts and
        /// already-unzipped smoosh directories are recognized; an s3
        /// source is listed and staged locally, then attached through
        /// the identical pipeline. For HDFS/GCS/Azure pull the
        /// segments to a local dir first.
        #[arg(long)]
        deep_storage: PathBuf,
        /// FerroDruid deep-storage base directory to import into: the
        /// `<data_dir>/deep-storage` of the target `ferrodruid serve`
        /// instance.
        #[arg(long)]
        ferro_deep_storage: PathBuf,
        /// Metadata store path or URI of the target instance:
        /// `<data_dir>/metadata/ferrodruid.db` (SQLite, the default
        /// deployment), `sqlite://<path>`, `postgres://…`, or
        /// `mysql://…`.
        #[arg(long)]
        metadata_uri: String,
        /// Only attach segments of this Druid dataSource; everything
        /// else is reported as filtered out.
        #[arg(long)]
        datasource: Option<String>,
        /// Report what would be attached without writing anything (no
        /// blobs, no metadata rows, no files created).
        #[arg(long)]
        dry_run: bool,
        /// Replace a segment that is already present under the same
        /// segment id (default: skip it with a warning). The existing
        /// blob is replaced IN PLACE: if the upload fails or the tool
        /// dies any time between the start of the blob replacement and
        /// the row update, the row keeps referencing the old sha256
        /// while the blob bytes may already be partially replaced, so
        /// the next startup fails loud on the content-hash check —
        /// re-run `attach --force` to converge.
        #[arg(long)]
        force: bool,
        /// Attach at most N segment artifacts (the scan stops once N
        /// have been collected; the report notes the truncation).
        #[arg(long, value_name = "N")]
        max_segments: Option<usize>,
        /// OPT-IN lossy mode: when a segment contains a column the v9
        /// reader cannot decode (e.g. an HLLSketch or
        /// quantilesDoublesSketch complex column; thetaSketch and
        /// hyperUnique DO decode), drop JUST that column and attach the
        /// rest instead of failing the whole segment. Every dropped
        /// column is listed loudly in the report and recorded in the
        /// metadata row's
        /// payload.droppedUnreadableColumns, and the segment is
        /// re-written WITHOUT it — a query naming a dropped column
        /// behaves exactly as for a column that never existed (never a
        /// silent null). A segment whose __time column — or EVERY
        /// dimension — is unreadable still fails loudly. Default off:
        /// any undecodable column fails its segment (strict).
        #[arg(long)]
        allow_unreadable_columns: bool,
    },
    /// Import segments recorded in an EXISTING Apache Druid metadata
    /// database (its druid_segments table on PostgreSQL, MySQL, or
    /// SQLite) into a FerroDruid single-binary deployment. The source
    /// DB is READ-ONLY input (SELECTs only; a source equal to the
    /// target --metadata-uri is refused). Only rows with used = true
    /// are imported; `local` and `s3_zip` loadSpecs are in scope: each
    /// row's payload identity (dataSource/interval/version/
    /// shardSpec.partitionNum) is synthesized, its local.path resolved
    /// (optionally remapped under --deep-storage) or its s3 index.zip
    /// fetched directly (bucket+key from the row; AWS_* env configures
    /// the client, AWS_ENDPOINT + AWS_ALLOW_HTTP=true for
    /// S3-compatible stores; a dry run still downloads), and the
    /// segment then runs the same per-segment crash-safe blob-first
    /// import as `attach` (blob upload, then metadata row).
    /// hdfs/google/azure loadSpecs are loudly skipped — pull those
    /// blobs to a local dir and re-run with --deep-storage. Derby
    /// sources are unsupported (no Rust driver): externalize the Druid
    /// metadata to PostgreSQL/MySQL first, or use `attach` on the
    /// deep-storage directory. Rules/supervisors are counted in the
    /// report but NOT imported. Nothing is loaded eagerly: segments
    /// become query-visible after the next `ferrodruid serve` startup
    /// (restart), whose bootstrap reload downloads, hash-verifies, and
    /// loads them. Run offline: stop the target instance first.
    ImportDruidMetadata {
        /// SOURCE Druid metadata DB URI: `postgres://…`,
        /// `postgresql://…`, `mysql://…`, `sqlite://<path>`, or a bare
        /// SQLite file path. Read-only: only SELECTs are issued.
        #[arg(long)]
        source_uri: String,
        /// Optional local base directory the source loadSpec
        /// `local.path` values live under: a loadSpec path that does
        /// not exist as-is is probed by longest suffix under this base
        /// (the "Druid deep-storage dir copied/rsynced/mounted here"
        /// case). Relative loadSpec paths require it.
        #[arg(long)]
        deep_storage: Option<PathBuf>,
        /// FerroDruid deep-storage base directory to import into: the
        /// `<data_dir>/deep-storage` of the target `ferrodruid serve`
        /// instance.
        #[arg(long)]
        ferro_deep_storage: PathBuf,
        /// Metadata store path or URI of the TARGET instance:
        /// `<data_dir>/metadata/ferrodruid.db` (SQLite, the default
        /// deployment), `sqlite://<path>`, `postgres://…`, or
        /// `mysql://…`.
        #[arg(long)]
        metadata_uri: String,
        /// Only import segments of this Druid dataSource (filtered in
        /// the source SELECT).
        #[arg(long)]
        datasource: Option<String>,
        /// Report what would be imported without writing anything (no
        /// blobs, no metadata rows, no files created).
        #[arg(long)]
        dry_run: bool,
        /// Replace a segment that is already present under the same
        /// segment id (default: skip it with a warning). Same crash
        /// window as `attach --force`: the existing blob is replaced in
        /// place, and a mid-replace failure leaves the old row
        /// referencing partially replaced bytes until a re-run
        /// converges.
        #[arg(long)]
        force: bool,
        /// Read at most N used source rows (the report notes when more
        /// exist).
        #[arg(long, value_name = "N")]
        max_segments: Option<usize>,
        /// OPT-IN lossy mode: drop columns the v9 reader cannot decode
        /// (e.g. HLLSketch/quantilesDoublesSketch complex columns;
        /// thetaSketch and hyperUnique DO decode) instead of failing the
        /// whole segment; dropped names are reported loudly and recorded
        /// in payload.droppedUnreadableColumns, and the
        /// segment is re-written WITHOUT them. A segment whose __time
        /// or EVERY dimension is unreadable still fails loudly.
        /// Default off (strict). Same semantics as `attach
        /// --allow-unreadable-columns`.
        #[arg(long)]
        allow_unreadable_columns: bool,
    },
    /// Export metadata from FerroDruid to a JSON file.
    Export {
        /// Metadata store path or URI (bare SQLite path,
        /// `sqlite://<path>`, `postgres://…`, or `mysql://…`).
        #[arg(long)]
        metadata_uri: String,
        /// Output JSON file path.
        #[arg(long)]
        output: String,
    },
    /// Import metadata from a JSON file into FerroDruid.
    Import {
        /// Metadata store path or URI (bare SQLite path,
        /// `sqlite://<path>`, `postgres://…`, or `mysql://…`).
        #[arg(long)]
        metadata_uri: String,
        /// Input JSON file path.
        #[arg(long)]
        input: String,
    },
    /// W1-I (CL-J1): generate a self-signed CA + per-role leaf certs
    /// for cross-role HTTP wire mTLS.
    ///
    /// Writes one shared `ca.pem` / `ca.key` plus `<role>/leaf.pem` +
    /// `<role>/leaf.key` for each of the six v1.0 roles (broker,
    /// historical, coordinator, router, overlord, middlemanager).
    /// Each role binary expects `<data_dir>/cross-role/` to contain
    /// `ca.pem`, `leaf.pem`, `leaf.key` (mode `0600`), so an operator
    /// rolls out the certs by copying `--out-dir/<role>/leaf.{pem,key}`
    /// + `ca.pem` into each role's `<data_dir>/cross-role/`.
    ///
    /// Suitable for dev / staging. Production deployments should use
    /// an operator-managed CA (e.g. cert-manager, HashiCorp Vault) and
    /// skip this helper.
    GenCrossRoleCerts {
        /// Output directory the helper writes the cert bundle into.
        /// Will be created if it does not exist.
        #[arg(long)]
        out_dir: PathBuf,
        /// Comma-separated list of role names to issue leaf certs for.
        /// Defaults to the canonical six-role topology.
        #[arg(
            long,
            default_value = "broker,historical,coordinator,router,overlord,middlemanager"
        )]
        roles: String,
        /// Comma-separated list of SANs every leaf cert carries in
        /// addition to its role name. Defaults to `localhost,127.0.0.1`
        /// so loopback dev deployments work without further config.
        #[arg(long, default_value = "localhost,127.0.0.1")]
        extra_sans: String,
        /// CA common-name written to the self-signed CA cert.
        #[arg(long, default_value = "FerroDruid cross-role dev CA")]
        ca_cn: String,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    if let Err(e) = run(cli.command).await {
        // Sanitize before printing: operational errors can carry
        // untrusted data (a source `loadSpec.path`, a bad `--source-uri`,
        // a DB-derived string) with raw terminal escapes intact — this
        // single boundary is where every subcommand's `Err` surfaces, so
        // it must strip escape sequences exactly like the report path.
        eprintln!("Error: {}", assess::sanitize(&e.to_string()));
        std::process::exit(1);
    }
}

async fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Assess {
            deep_storage,
            json,
            max_segments,
        } => {
            // W-C: an s3://bucket/prefix source is staged into a
            // throwaway local dir first; the assessment then runs over
            // it unchanged (same staging as `attach`).
            if let Some(parsed) = s3_source::s3_url_of_path(&deep_storage) {
                let url = match parsed {
                    Ok(u) => u,
                    Err(e) => return Err(e.into()),
                };
                let staged = s3_source::stage_s3_tree(&url, None, max_segments).await?;
                if !json {
                    println!(
                        "staged {} object(s) ({} bytes) from {} into a local staging dir",
                        staged.objects_downloaded,
                        staged.bytes_downloaded,
                        url.display()
                    );
                }
                assess::run(&staged.root, json, max_segments)?;
            } else {
                assess::run(&deep_storage, json, max_segments)?;
            }
        }
        Command::Attach {
            deep_storage,
            ferro_deep_storage,
            metadata_uri,
            datasource,
            dry_run,
            force,
            max_segments,
            allow_unreadable_columns,
        } => {
            attach::run(attach::AttachParams {
                deep_storage,
                ferro_deep_storage,
                metadata_uri,
                datasource,
                dry_run,
                force,
                max_segments,
                allow_unreadable_columns,
            })
            .await?;
        }
        Command::ImportDruidMetadata {
            source_uri,
            deep_storage,
            ferro_deep_storage,
            metadata_uri,
            datasource,
            dry_run,
            force,
            max_segments,
            allow_unreadable_columns,
        } => {
            import_druid::run(import_druid::ImportParams {
                source_uri,
                deep_storage,
                ferro_deep_storage,
                metadata_uri,
                datasource,
                dry_run,
                force,
                max_segments,
                allow_unreadable_columns,
            })
            .await?;
        }
        Command::Export {
            metadata_uri,
            output,
        } => {
            let store = MetadataStore::connect(&metadata_uri).await?;
            store.initialize().await?;
            let exporter = ferrodruid_import_export::MetadataExporter::new(Arc::new(store));
            let data = exporter.export_all().await?;
            let json = serde_json::to_string_pretty(&data)?;
            fs::write(&output, json)?;
            println!("Exported metadata to {output}");
        }
        Command::Import {
            metadata_uri,
            input,
        } => {
            let contents = fs::read_to_string(&input)?;
            let data: serde_json::Value = serde_json::from_str(&contents)?;
            let store = MetadataStore::connect(&metadata_uri).await?;
            store.initialize().await?;
            let importer = ferrodruid_import_export::MetadataImporter::new(Arc::new(store));
            let summary = importer.import_all(&data).await?;
            println!(
                "Imported: {} segments, {} rules, {} supervisors, {} config entries",
                summary.segments_imported,
                summary.rules_imported,
                summary.supervisors_imported,
                summary.config_imported,
            );
        }
        Command::GenCrossRoleCerts {
            out_dir,
            roles,
            extra_sans,
            ca_cn,
        } => {
            let roles: Vec<String> = roles
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if roles.is_empty() {
                return Err("--roles must list at least one role".into());
            }
            let extra_sans: Vec<String> = extra_sans
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let written = gen_certs::generate_bundle(&out_dir, &roles, &extra_sans, &ca_cn)?;
            println!("Wrote cross-role cert bundle to {}:", out_dir.display(),);
            println!("  CA:   {}", written.ca_pem.display());
            println!("        {} (mode 0600)", written.ca_key.display());
            for (role, leaf) in &written.leafs {
                println!(
                    "  {role:<14} {}",
                    leaf.leaf_pem
                        .strip_prefix(Path::new(&out_dir))
                        .unwrap_or(leaf.leaf_pem.as_path())
                        .display(),
                );
                println!(
                    "                 {} (mode 0600)",
                    leaf.leaf_key
                        .strip_prefix(Path::new(&out_dir))
                        .unwrap_or(leaf.leaf_key.as_path())
                        .display(),
                );
            }
            println!(
                "Next step: copy `ca.pem`, `<role>/leaf.pem`, and `<role>/leaf.key` into \
                 each role's `<data_dir>/cross-role/` directory (rename the leaf files to \
                 `leaf.pem`/`leaf.key`), then start the binaries with the new default \
                 `--cross-role-mtls=required` posture."
            );
        }
    }
    Ok(())
}
