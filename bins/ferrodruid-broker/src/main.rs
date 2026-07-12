// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `ferrodruid-broker` — Wave 40.LL role-split launcher; W1-I (CL-J1)
//! mTLS-default cross-role HTTP wire.
//!
//! This binary is the v1.0 entry point for the **broker** role. It
//! boots an axum HTTP server on `--bind:--port` (default
//! `127.0.0.1:8082`, mirroring Apache Druid's broker port) and exposes
//! the cross-role contracts defined in `ferrodruid-rpc`:
//!
//! - `POST /druid/v2/sql` — SQL query forwarded by a router, with the
//!   real SQL → native bridge.
//! - `GET /druid/v2/info` — broker introspection.
//! - `POST /druid/v2/sql/scatter` — per-segment scatter.
//!
//! W1-I (CL-J1) wires mTLS-default cross-role HTTP: by default the
//! HTTP listener requires a client cert signed by the configured CA
//! bundle, and the outbound `HttpHistoricalClient` presents the broker
//! leaf cert when dialling historicals. See `docs/SECURITY.md` for the
//! migration runbook (`--cross-role-mtls=permissive` / `disabled`).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod broker_app;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use ferrodruid_role::{Role, RoleConfig};
use ferrodruid_rpc::broker_server::BrokerServerState;
use ferrodruid_rpc::{CrossRoleMtlsMode, CrossRoleStartup, serve_cross_role};

use crate::broker_app::{BrokerScatterState, build_router};

/// CLI surface for the broker role.
#[derive(Parser, Debug)]
#[command(name = "ferrodruid-broker", version, about)]
struct Cli {
    /// Address to bind to. Defaults to loopback so a fresh install
    /// never exposes the role to the public network.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,

    /// Port to listen on. Default mirrors Apache Druid's broker port.
    #[arg(long, default_value_t = 8082)]
    port: u16,

    /// Data directory for any role-local state.
    #[arg(long, default_value = "./data/broker")]
    data_dir: PathBuf,

    /// Optional broker identity override; auto-generated when absent.
    #[arg(long)]
    broker_id: Option<String>,

    /// Tier label this broker reports via `/druid/v2/info`.
    #[arg(long, default_value = "default")]
    tier: String,

    /// Comma-separated list of historical base URLs to scatter to
    /// (e.g. `https://127.0.0.1:9083,https://hist-2:9083`). Optional —
    /// when empty the broker keeps the W3 echo behaviour and the
    /// `/druid/v2/sql/scatter` route returns 503.
    #[arg(long, env = "FERRODRUID_BROKER_HISTORICALS", default_value = "")]
    historical_url: String,

    /// W1-I (CL-J1): cross-role HTTP wire mTLS mode
    /// (`required` / `permissive` / `disabled`).
    ///
    /// `required` (default) is the v1.0 secure posture; `permissive`
    /// binds BOTH the plain HTTP port AND a TLS port for migration
    /// rollouts; `disabled` keeps the v0.2.0 plain HTTP behaviour.
    /// See `docs/SECURITY.md` cross-role section.
    #[arg(long, env = "FERRODRUID_CROSS_ROLE_MTLS", default_value = "required")]
    cross_role_mtls: String,

    /// Optional TLS bind port (used in `required` / `permissive`).
    /// Defaults to `port + 1000` (Druid `tlsPort` convention).
    #[arg(long)]
    tls_port: Option<u16>,

    /// Explicit path to the PEM-encoded leaf cert chain this role
    /// presents. Must be set together with `--cross-role-tls-key` and
    /// `--cross-role-tls-ca`; if all three are omitted the role falls
    /// back to `<data_dir>/cross-role/{leaf.pem,leaf.key,ca.pem}`.
    #[arg(long)]
    cross_role_tls_cert: Option<PathBuf>,

    /// Explicit path to the PEM-encoded private key.
    #[arg(long)]
    cross_role_tls_key: Option<PathBuf>,

    /// Explicit path to the PEM-encoded CA bundle used to verify peer
    /// certs.
    #[arg(long)]
    cross_role_tls_ca: Option<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let cfg = match RoleConfig::try_new(Role::Broker, &cli.bind, cli.port, cli.data_dir.clone()) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("FATAL: {e}");
            return ExitCode::from(2);
        }
    };

    println!("{}", cfg.banner());
    tracing::info!(role = %cfg.role, bind = %cfg.bind, port = cfg.port, "broker role starting");

    let server_state = BrokerServerState {
        broker_id: cli
            .broker_id
            .clone()
            .unwrap_or_else(|| BrokerServerState::default().broker_id),
        tier: cli.tier.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };

    let historical_urls: Vec<String> = cli
        .historical_url
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    tracing::info!(
        historicals = ?historical_urls,
        "broker scatter targets",
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

    let scatter_state = BrokerScatterState::from_historical_urls_with_client(
        server_state,
        &historical_urls,
        outbound,
    );

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("FATAL: failed to start tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };

    runtime.block_on(async move {
        let app = build_router(scatter_state);
        let (mode, plain, tls) = match startup.into_listeners() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("FATAL: cross-role listener build: {e}");
                return ExitCode::from(2);
            }
        };
        eprintln!(
            "ferrodruid-broker (W1-I): mTLS={mode} | endpoints: POST /druid/v2/sql, POST \
             /druid/v2/sql/scatter, POST /druid/v2/native, GET /druid/v2/info ({} \
             historicals configured)",
            historical_urls.len(),
        );
        if let Err(e) = serve_cross_role(app, mode, plain, tls).await {
            eprintln!("FATAL: serve_cross_role: {e}");
            return ExitCode::from(1);
        }
        ExitCode::SUCCESS
    })
}
