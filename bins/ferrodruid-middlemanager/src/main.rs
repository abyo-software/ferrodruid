// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `ferrodruid-middlemanager` — Wave 39.HH role-split launcher with
//! real HTTP wire.
//!
//! This binary is the v1.0 entry point for the **middleManager** role.
//! As of Wave 39.HH it boots an axum HTTP server on `--bind:--port`
//! (default `127.0.0.1:8091`, mirroring Apache Druid's middleManager
//! port) that exposes the cross-role contracts defined in
//! `ferrodruid-rpc`:
//!
//! - `POST /druid/v1/middlemanager/task` — accept a task assignment
//!   from the overlord, register it as `Pending`, then run a
//!   simulated executor that flips it through `Running` →
//!   `Success`. Real ingestion execution is W4+ scope.
//! - `GET /druid/v1/middlemanager/task/{id}/status` — return the
//!   current lifecycle state.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ferrodruid_role::{Role, RoleConfig};
use ferrodruid_rpc::mm_server::{self, MiddleManagerServerState};
use ferrodruid_rpc::{CrossRoleMtlsMode, CrossRoleStartup, serve_cross_role};

/// CLI surface for the middleManager role.
#[derive(Parser, Debug)]
#[command(name = "ferrodruid-middlemanager", version, about)]
struct Cli {
    /// Address to bind to. Defaults to loopback.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,

    /// Port to listen on. Default mirrors Apache Druid's
    /// middleManager port (8091).
    #[arg(long, default_value_t = 8091)]
    port: u16,

    /// Data directory for peon working state and task scratch space.
    #[arg(long, default_value = "./data/middlemanager")]
    data_dir: PathBuf,

    /// Simulated `Pending → Running` delay in milliseconds.
    #[arg(long, default_value_t = 50)]
    pending_to_running_ms: u64,

    /// Simulated `Running → Success` delay in milliseconds.
    #[arg(long, default_value_t = 50)]
    running_to_success_ms: u64,

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

    /// Explicit PEM CA bundle used to verify peer client certs.
    #[arg(long)]
    cross_role_tls_ca: Option<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let cfg = match RoleConfig::try_new(
        Role::MiddleManager,
        &cli.bind,
        cli.port,
        cli.data_dir.clone(),
    ) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("FATAL: {e}");
            return ExitCode::from(2);
        }
    };

    println!("{}", cfg.banner());
    tracing::info!(
        role = %cfg.role,
        bind = %cfg.bind,
        port = cfg.port,
        "middlemanager role starting",
    );

    let state = Arc::new(MiddleManagerServerState::with_timings(
        Duration::from_millis(cli.pending_to_running_ms),
        Duration::from_millis(cli.running_to_success_ms),
    ));

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

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("FATAL: failed to start tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };

    runtime.block_on(async move {
        let app = mm_server::router(state);
        let (mode, plain, tls) = match startup.into_listeners() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("FATAL: cross-role listener build: {e}");
                return ExitCode::from(2);
            }
        };
        eprintln!(
            "ferrodruid-middlemanager (W1-I): mTLS={mode} | endpoints: \
             POST /druid/v1/middlemanager/task, GET /druid/v1/middlemanager/task/{{id}}/status",
        );
        if let Err(e) = serve_cross_role(app, mode, plain, tls).await {
            eprintln!("FATAL: serve_cross_role: {e}");
            return ExitCode::from(1);
        }
        ExitCode::SUCCESS
    })
}
