// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `ferrodruid-overlord` — Wave 39.HH role-split launcher with real
//! HTTP wire.
//!
//! This binary is the v1.0 entry point for the **overlord** role: it
//! is the indexing service master, accepting ingestion task specs
//! from clients and dispatching them to a middleManager. As of Wave
//! 39.HH it accepts `POST /druid/indexer/v1/task` from clients,
//! picks a middleManager from the configured `--middlemanager-url`
//! list (round-robin), and forwards via
//! [`ferrodruid_rpc::HttpMiddleManagerClient`]. Status polling is
//! relayed via `GET /druid/indexer/v1/task/{id}/status`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod overlord_app;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use ferrodruid_role::{Role, RoleConfig};
use ferrodruid_rpc::{CrossRoleMtlsMode, CrossRoleStartup, serve_cross_role};

use crate::overlord_app::{OverlordState, build_router};

/// CLI surface for the overlord role.
#[derive(Parser, Debug)]
#[command(name = "ferrodruid-overlord", version, about)]
struct Cli {
    /// Address to bind to. Defaults to loopback.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,

    /// Port to listen on. Default mirrors Apache Druid's overlord
    /// port (8090).
    #[arg(long, default_value_t = 8090)]
    port: u16,

    /// Data directory for indexing task metadata.
    #[arg(long, default_value = "./data/overlord")]
    data_dir: PathBuf,

    /// Comma-separated list of middleManager base URLs to dispatch
    /// to (e.g. `https://127.0.0.1:9091,https://mm-2:9091`). Required.
    #[arg(long, env = "FERRODRUID_OVERLORD_MIDDLEMANAGERS")]
    middlemanager_url: String,

    /// W-B legacy null mode: run Apache Druid's LEGACY null semantics
    /// (`useDefaultValueForNull=true`, the default on Druid <= 27) for
    /// everything this process serves. Latched process-globally at
    /// startup (before serving); cannot change without a restart. Must
    /// match the value every other role in the cluster was started
    /// with. Default off (modern SQL-compatible ANSI nulls).
    #[arg(
        long,
        env = "FERRODRUID_USE_DEFAULT_VALUE_FOR_NULL",
        default_value_t = false
    )]
    use_default_value_for_null: bool,

    /// W1-I (CL-J1): cross-role HTTP wire mTLS mode
    /// (`required` / `permissive` / `disabled`). See `docs/SECURITY.md`.
    #[arg(long, env = "FERRODRUID_CROSS_ROLE_MTLS", default_value = "required")]
    cross_role_mtls: String,

    /// Optional TLS bind port (default = `port + 1000`).
    #[arg(long)]
    tls_port: Option<u16>,

    /// Explicit PEM leaf cert chain.
    #[arg(long)]
    cross_role_tls_cert: Option<PathBuf>,

    /// Explicit PEM private key.
    #[arg(long)]
    cross_role_tls_key: Option<PathBuf>,

    /// Explicit PEM CA bundle.
    #[arg(long)]
    cross_role_tls_ca: Option<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    // W-B legacy null mode (H4): latch the process-global flag BEFORE
    // anything can read it, so the role-split path serves the same null
    // semantics as the single binary (immutable from first observation).
    if let Err(e) =
        ferrodruid_common::null_mode::init_legacy_null_mode_serve(cli.use_default_value_for_null)
    {
        eprintln!("FATAL: {e}");
        return ExitCode::from(2);
    }

    let cfg = match RoleConfig::try_new(Role::Overlord, &cli.bind, cli.port, cli.data_dir.clone()) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("FATAL: {e}");
            return ExitCode::from(2);
        }
    };

    let mm_urls: Vec<String> = cli
        .middlemanager_url
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if mm_urls.is_empty() {
        eprintln!("FATAL: --middlemanager-url must list at least one middleManager");
        return ExitCode::from(2);
    }

    println!("{}", cfg.banner());
    tracing::info!(
        role = %cfg.role,
        bind = %cfg.bind,
        port = cfg.port,
        middlemanagers = ?mm_urls,
        "overlord role starting",
    );

    let mode: CrossRoleMtlsMode = match cli.cross_role_mtls.parse() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("FATAL: invalid --cross-role-mtls: {e}");
            return ExitCode::from(2);
        }
    };
    let legacy_bind: SocketAddr = SocketAddr::new(cfg.bind, cfg.port);
    let tls_bind = cli.tls_port.map(|p| SocketAddr::new(cfg.bind, p));
    let cert_dir = cli.data_dir.join("cross-role");
    let startup = match CrossRoleStartup::resolve(
        mode,
        legacy_bind,
        tls_bind,
        cli.cross_role_tls_cert,
        cli.cross_role_tls_key,
        cli.cross_role_tls_ca,
        Some(cert_dir),
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("FATAL: cross-role mTLS setup: {e}");
            return ExitCode::from(2);
        }
    };
    let outbound = match startup.build_outbound_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("FATAL: cannot build outbound HTTP client: {e}");
            return ExitCode::from(2);
        }
    };

    let state = OverlordState::from_mm_urls_with_client(&mm_urls, outbound);

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("FATAL: failed to start tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };

    runtime.block_on(async move {
        let app = build_router(state);
        let (mode, plain, tls) = match startup.into_listeners() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("FATAL: cross-role listener build: {e}");
                return ExitCode::from(2);
            }
        };
        eprintln!(
            "ferrodruid-overlord (W1-I): mTLS={mode} | dispatches to {} middleManager(s)",
            mm_urls.len(),
        );
        if let Err(e) = serve_cross_role(app, mode, plain, tls).await {
            eprintln!("FATAL: serve_cross_role: {e}");
            return ExitCode::from(1);
        }
        ExitCode::SUCCESS
    })
}
