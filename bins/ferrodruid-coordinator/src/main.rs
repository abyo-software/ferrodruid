// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `ferrodruid-coordinator` — Wave 40.LL role-split launcher with
//! real HTTP wire to historicals.
//!
//! This binary is the v1.0 entry point for the **coordinator** role:
//! it owns cluster metadata, segment assignment, and balancing. As of
//! Wave 40.LL it accepts a comma-separated `--historical-url` list
//! and exposes:
//!
//! - `POST /druid/coordinator/v1/loadqueue/{historical}` — load a
//!   segment on the chosen historical (by index, e.g. `0` or
//!   `historical-0`).
//! - `POST /druid/coordinator/v1/dropqueue/{historical}` — drop a
//!   segment.
//! - `GET /druid/coordinator/v1/loadstatus` — aggregated load status.
//! - `GET /druid/coordinator/v1/health` — readiness probe.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod coordinator_app;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use ferrodruid_role::{Role, RoleConfig};
use ferrodruid_rpc::{CrossRoleMtlsMode, CrossRoleStartup, serve_cross_role};

use crate::coordinator_app::{CoordinatorState, build_router};

/// CLI surface for the coordinator role.
#[derive(Parser, Debug)]
#[command(name = "ferrodruid-coordinator", version, about)]
struct Cli {
    /// Address to bind to. Defaults to loopback.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,

    /// Port to listen on. Default mirrors Apache Druid's coordinator
    /// port.
    #[arg(long, default_value_t = 8081)]
    port: u16,

    /// Data directory for cluster metadata.
    #[arg(long, default_value = "./data/coordinator")]
    data_dir: PathBuf,

    /// Comma-separated list of historical base URLs the coordinator
    /// will issue load / drop / status commands against. Optional —
    /// when empty the coordinator boots and answers `/health` but
    /// every load / drop endpoint returns 503.
    #[arg(long, env = "FERRODRUID_COORDINATOR_HISTORICALS", default_value = "")]
    historical_url: String,

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

    let cfg =
        match RoleConfig::try_new(Role::Coordinator, &cli.bind, cli.port, cli.data_dir.clone()) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("FATAL: {e}");
                return ExitCode::from(2);
            }
        };

    println!("{}", cfg.banner());
    tracing::info!(role = %cfg.role, bind = %cfg.bind, port = cfg.port, "coordinator role starting");

    let historical_urls: Vec<String> = cli
        .historical_url
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    tracing::info!(historicals = ?historical_urls, "coordinator load targets");

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

    let state = CoordinatorState::from_historical_urls_with_client(&historical_urls, outbound);

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
            "ferrodruid-coordinator (W1-I): mTLS={mode} | dispatches to {} historical(s)",
            historical_urls.len(),
        );
        if let Err(e) = serve_cross_role(app, mode, plain, tls).await {
            eprintln!("FATAL: serve_cross_role: {e}");
            return ExitCode::from(1);
        }
        ExitCode::SUCCESS
    })
}
