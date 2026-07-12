// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use ferrodruid_metadata::MetadataStore;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod gen_certs;

/// FerroDruid migration tool — import/export metadata + W1-I
/// cross-role cert bootstrap.
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
    /// Export metadata from FerroDruid to a JSON file.
    Export {
        /// SQLite metadata store path.
        #[arg(long)]
        metadata_uri: String,
        /// Output JSON file path.
        #[arg(long)]
        output: String,
    },
    /// Import metadata from a JSON file into FerroDruid.
    Import {
        /// SQLite metadata store path.
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
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Export {
            metadata_uri,
            output,
        } => {
            let store = MetadataStore::new_sqlite(&metadata_uri).await?;
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
            let store = MetadataStore::new_sqlite(&metadata_uri).await?;
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
