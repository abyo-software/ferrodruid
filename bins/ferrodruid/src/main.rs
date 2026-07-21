// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use ferrodruid_auth::AuthStore;
use ferrodruid_authz::Authorizer;
use ferrodruid_cluster::auth::derive_psk;
use ferrodruid_cluster::replication::{ReplicationConfig, ReplicationEngine};
use ferrodruid_cluster::transport::{TcpTransport, TcpTransportConfig};
use ferrodruid_cluster::{ClusterManager, NodeInfo, NodeRole, ServiceEntry};
use ferrodruid_common::config::bind_addr_is_loopback;
use ferrodruid_coordinator::Coordinator;
use ferrodruid_deep_storage::{DeepStorage, LocalDeepStorage};
use ferrodruid_discovery::ServiceDiscovery;
use ferrodruid_historical::Historical;
use ferrodruid_metadata::MetadataStore;
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_rest::{AppState, create_router};
use ferrodruid_role::{
    DispatchOutcome, Role as DispatchRole, RoleConfig, dispatch as role_dispatch,
};

/// FerroDruid — Druid-compatible OLAP engine in Rust.
#[derive(Parser)]
#[command(name = "ferrodruid", version, about)]
struct Cli {
    /// Log output format.
    #[arg(long, default_value = "text", value_parser = ["text", "json"])]
    log_format: String,

    #[command(subcommand)]
    command: Command,
}

/// Top-level commands.
#[derive(Subcommand)]
enum Command {
    /// Start the FerroDruid server.
    Serve {
        /// Deployment mode.
        #[arg(long, default_value = "single-binary")]
        mode: DeployMode,

        /// Address to bind to.  Defaults to `127.0.0.1` (loopback) so a
        /// fresh install never accidentally exposes the admin API to the
        /// internet.  Pass `0.0.0.0` (or a routable address) only when
        /// auth is enabled.
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        /// Port to listen on.
        #[arg(long, default_value = "8888")]
        port: u16,

        /// Data directory for segments, metadata, and deep storage.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Metadata store URI: `postgres://…`/`postgresql://…`,
        /// `mysql://…`, `sqlite://<path>`, a bare SQLite file path, or
        /// `:memory:`. Defaults to `<data_dir>/metadata/ferrodruid.db`
        /// (SQLite) when omitted, so existing deployments are
        /// unchanged. The env var carries credentials for the network
        /// backends without exposing them in the process list.
        #[arg(long, env = "FERRODRUID_METADATA_URI")]
        metadata_uri: Option<String>,

        /// Reject loaded segments whose modern null-generation cannot be confirmed.
        #[arg(
            long,
            env = "FERRODRUID_STRICT_NULL_GENERATION",
            default_value_t = false
        )]
        strict_null_generation: bool,

        /// W-B legacy null mode: run Apache Druid's LEGACY null semantics
        /// (`useDefaultValueForNull=true`, the default on Druid <= 27):
        /// null/missing strings are identical to `""` and null/missing
        /// numerics to `0`, at ingest (coerced, no null markers written)
        /// and at query time.  Latched process-globally at startup; cannot
        /// change without a restart.  Default off (modern SQL-compatible
        /// ANSI nulls).
        #[arg(
            long,
            env = "FERRODRUID_USE_DEFAULT_VALUE_FOR_NULL",
            default_value_t = false
        )]
        use_default_value_for_null: bool,

        /// FG-7: spill loaded segments to `<data_dir>/segments/spill/` and
        /// decode them on demand under a memory-budgeted LRU, instead of
        /// holding every segment resident on the heap. Trades query latency
        /// for a flat, low memory ceiling. Default off.
        #[arg(long, env = "FERRODRUID_SEGMENT_SPILL", default_value_t = false)]
        segment_spill: bool,

        /// Segment cache byte budget. Heap mode: total admitted segment
        /// payload (fail-closed admission). Spill mode
        /// (`--segment-spill`): resident decoded bytes (LRU-evicted).
        /// Default 1 GiB (1073741824). Must be > 0 — a zero budget makes
        /// spill mode treat every segment as oversized (re-decoded on every
        /// query) and heap mode reject all admission, so it is rejected at
        /// parse time (Codex R11 HIGH). The `1..` range guard covers both
        /// this flag and the `FERRODRUID_SEGMENT_CACHE_BYTES` env source.
        #[arg(
            long,
            env = "FERRODRUID_SEGMENT_CACHE_BYTES",
            default_value_t = 1_073_741_824,
            value_parser = clap::value_parser!(u64).range(1..)
        )]
        segment_cache_bytes: u64,

        /// Peer addresses for simplified mode (comma-separated host:port).
        #[arg(long)]
        peers: Option<String>,

        /// Classic mode role.
        #[arg(long)]
        role: Option<ClassicRole>,

        /// Disable authentication.  Refused unless `--bind` is loopback or
        /// `--allow-insecure-public-bind` is also passed.  Default `false`.
        #[arg(long, default_value_t = false)]
        no_auth: bool,

        /// Operator opt-in to bind a non-loopback address with `--no-auth`.
        /// Intended for sealed-network test rigs only.
        #[arg(long, default_value_t = false)]
        allow_insecure_public_bind: bool,

        /// Stable identifier for this node when running in multi-node
        /// cluster mode. Required when `--cluster-peers` is set.
        #[arg(long)]
        node_id: Option<String>,

        /// Listen address for cluster replication traffic (separate from
        /// the HTTP bind/port). Defaults to `127.0.0.1:<port+10000>`.
        #[arg(long)]
        cluster_bind: Option<String>,

        /// Comma-separated peers in the form `nodeid@host:port`. Spawning
        /// with this flag enables multi-node cluster mode using the
        /// production `TcpTransport`.
        #[arg(long)]
        cluster_peers: Option<String>,

        /// Election timeout in milliseconds (cluster mode). Followers wait
        /// at least this long after the last heartbeat before challenging
        /// the leader; an additional uniform jitter of up to 150 ms is
        /// added to avoid split votes. Default `1500`.
        #[arg(long, default_value_t = 1500)]
        election_timeout_ms: u64,

        /// Heartbeat broadcast interval in milliseconds (cluster mode).
        /// Leaders send empty AppendEntries this often. Should be roughly
        /// `election_timeout_ms / 6` so a single dropped heartbeat does
        /// not trigger a re-election. Default `250`.
        #[arg(long, default_value_t = 250)]
        heartbeat_interval_ms: u64,

        /// Wave 40-A: shared cluster pre-shared key. Either a 64-hex-char
        /// string (parsed as 32 raw bytes) or any other string (SHA-256
        /// hashed to 32 bytes). Required for cluster mode unless
        /// `--cluster-psk-not-required` is set. Falls back to the
        /// `FERRODRUID_CLUSTER_PSK` env var when the flag is omitted.
        #[arg(long)]
        cluster_psk: Option<String>,

        /// Wave 40-A: development-only flag that allows cluster mode to
        /// start WITHOUT a shared PSK. Refused in any production
        /// deployment — every cluster TCP frame is then unauthenticated
        /// and a network adversary can forge ACKs / steal votes.
        #[arg(long, default_value_t = false)]
        cluster_psk_not_required: bool,

        /// Phase 2.4: cluster transport security posture. Defaults to
        /// `mtls` — the node will not start in cluster mode unless
        /// `--cluster-tls-cert`, `--cluster-tls-key` and
        /// `--cluster-tls-ca` are all provided (fail-loud, no silent
        /// downgrade). Pass `--cluster-security psk` to explicitly opt
        /// into the weaker PSK-over-cleartext fallback (auth + integrity
        /// but no confidentiality), e.g. for sealed-network test rigs or
        /// backward compatibility. Falls back to the
        /// `FERRODRUID_CLUSTER_SECURITY` env var when the flag is omitted.
        #[arg(long, value_enum)]
        cluster_security: Option<ClusterSecurityArg>,

        /// Wave 44 (`cluster-tls` feature only): PEM-encoded leaf
        /// certificate (with optional intermediate chain) this node
        /// presents on the cluster wire. Must be paired with
        /// `--cluster-tls-key` and `--cluster-tls-ca`. Adds TLS
        /// confidentiality + forward secrecy on top of the PSK auth
        /// layer; ignored when the binary was built without
        /// `--features cluster-tls`.
        #[arg(long)]
        cluster_tls_cert: Option<String>,

        /// Wave 44: PEM-encoded private key matching `--cluster-tls-cert`.
        /// PKCS#8, SEC1 and PKCS#1 formats are auto-detected.
        #[arg(long)]
        cluster_tls_key: Option<String>,

        /// Wave 44: PEM-encoded CA bundle used to verify peer certs in
        /// both directions (mTLS). Connections without a peer cert
        /// signed by this CA are refused at the TLS handshake.
        #[arg(long)]
        cluster_tls_ca: Option<String>,
    },
}

/// Deployment mode for the FerroDruid process.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum DeployMode {
    /// All services in one process.
    SingleBinary,
    /// Simplified — broker + historical + coordinator in one process.
    Simplified,
    /// Classic — run a single Druid service type.
    Classic,
}

/// Classic mode role selection.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum ClassicRole {
    /// Coordinator service — segment-side master tier (cluster
    /// metadata + segment assignment + balancing).
    Coordinator,
    /// Overlord service — indexing-side master tier (assigns
    /// ingestion tasks to middleManagers).
    Overlord,
    /// Broker service — query routing tier.
    Broker,
    /// Historical service — segment-serving tier.
    Historical,
    /// Router service — HTTP front-end / load-balancer tier in
    /// front of brokers.
    Router,
    /// MiddleManager service — indexing-worker tier (hosts peons
    /// that execute batch + streaming ingestion tasks).
    Middlemanager,
}

/// Phase 2.4: operator selection of the cluster transport security
/// posture. Maps onto [`ferrodruid_cluster::transport::ClusterSecurityMode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ClusterSecurityArg {
    /// Mutual TLS (default posture). Requires `--cluster-tls-{cert,key,ca}`.
    Mtls,
    /// Explicit opt-in: PSK over cleartext TCP (auth + integrity, no
    /// confidentiality). For sealed networks / test rigs / backward
    /// compatibility only.
    Psk,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    if cli.log_format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }

    match cli.command {
        Command::Serve {
            mode,
            bind,
            port,
            data_dir,
            metadata_uri,
            peers,
            role,
            no_auth,
            allow_insecure_public_bind,
            node_id,
            cluster_bind,
            cluster_peers,
            election_timeout_ms,
            heartbeat_interval_ms,
            cluster_psk,
            cluster_psk_not_required,
            cluster_security,
            cluster_tls_cert,
            cluster_tls_key,
            cluster_tls_ca,
            strict_null_generation,
            use_default_value_for_null,
            segment_spill,
            segment_cache_bytes,
        } => match mode {
            DeployMode::SingleBinary => {
                let auth_enabled = !no_auth;
                if !auth_enabled && !allow_insecure_public_bind && !bind_addr_is_loopback(&bind) {
                    eprintln!(
                        "FATAL: refusing to bind non-loopback address `{bind}` with \
                         --no-auth.  Either drop --no-auth (recommended) or pass \
                         --allow-insecure-public-bind to override."
                    );
                    std::process::exit(1);
                }
                // Wave 40-A: env-var fallback when --cluster-psk is omitted.
                let psk_from_env = std::env::var("FERRODRUID_CLUSTER_PSK").ok();
                let resolved_psk = cluster_psk.as_deref().or(psk_from_env.as_deref());
                // Phase 2.4: resolve the security posture. Explicit flag
                // wins; otherwise the FERRODRUID_CLUSTER_SECURITY env var;
                // otherwise the default (mTLS) is applied inside
                // build_cluster_options.
                let resolved_security = resolve_cluster_security(cluster_security)?;
                let cluster_opts = build_cluster_options(
                    node_id.as_deref(),
                    cluster_bind.as_deref(),
                    cluster_peers.as_deref(),
                    &bind,
                    port,
                    election_timeout_ms,
                    heartbeat_interval_ms,
                    resolved_psk,
                    cluster_psk_not_required,
                    resolved_security,
                    cluster_tls_cert.as_deref(),
                    cluster_tls_key.as_deref(),
                    cluster_tls_ca.as_deref(),
                )?;
                run_single_binary(
                    &bind,
                    port,
                    &data_dir,
                    metadata_uri.as_deref(),
                    auth_enabled,
                    cluster_opts,
                    strict_null_generation,
                    use_default_value_for_null,
                    segment_spill,
                    segment_cache_bytes,
                )
                .await?;
            }
            DeployMode::Simplified => {
                let peers_str = peers.as_deref().unwrap_or("");
                tracing::info!(
                    peers = peers_str,
                    "Simplified mode is not yet implemented. Use --mode single-binary."
                );
                eprintln!(
                    "FerroDruid v{} — simplified mode not yet implemented. \
                     Peers: {peers_str}. Use --mode single-binary.",
                    env!("CARGO_PKG_VERSION")
                );
            }
            DeployMode::Classic => {
                // Wave 34.T → 38.FF: route through the role-split scaffold so
                // operators get a single source of truth for role
                // dispatch (also covered by the per-role binaries
                // `ferrodruid-broker`, `ferrodruid-historical`,
                // `ferrodruid-coordinator`, `ferrodruid-router`,
                // `ferrodruid-overlord`, `ferrodruid-middlemanager`).
                // The cross-role wire is still v1.0 work — see
                // docs/v1.0-roadmap.md (W3) — so the dispatcher logs
                // and exits success for now.
                run_classic_role(role, &bind, port, &data_dir)?;
            }
        },
    }

    Ok(())
}

/// Resolved multi-node cluster options derived from CLI flags.
#[derive(Debug)]
struct ClusterOptions {
    node_id: String,
    bind_addr: SocketAddr,
    peers: Vec<(String, SocketAddr)>,
    /// Election timeout in milliseconds (operator-tunable, default 1500).
    election_timeout_ms: u64,
    /// Heartbeat broadcast interval in milliseconds (operator-tunable,
    /// default 250).
    heartbeat_interval_ms: u64,
    /// Wave 40-A: derived 32-byte cluster PSK. Wrapped in `Arc` so the
    /// transport can clone it cheaply into per-task contexts.
    psk: Arc<ferrodruid_cluster::auth::ClusterPsk>,
    /// Phase 2.4: resolved transport security posture. Defaults to mTLS
    /// (built from `--cluster-tls-{cert,key,ca}`); the explicit
    /// `--cluster-security psk` opt-in selects the PSK-cleartext fallback.
    security: ferrodruid_cluster::transport::ClusterSecurityMode,
}

/// Phase 2.4: resolve the requested cluster security posture from the CLI
/// flag, falling back to the `FERRODRUID_CLUSTER_SECURITY` env var, and
/// finally to the default (mTLS).
///
/// Returns `None` to mean "no explicit selection — apply the default
/// (mTLS)"; `Some(arg)` carries an explicit operator choice.
fn resolve_cluster_security(
    flag: Option<ClusterSecurityArg>,
) -> anyhow::Result<Option<ClusterSecurityArg>> {
    if let Some(arg) = flag {
        return Ok(Some(arg));
    }
    match std::env::var("FERRODRUID_CLUSTER_SECURITY") {
        Ok(v) => {
            let parsed = match v.trim().to_ascii_lowercase().as_str() {
                "mtls" | "mutual-tls" | "tls" => ClusterSecurityArg::Mtls,
                "psk" | "psk-cleartext" | "cleartext" => ClusterSecurityArg::Psk,
                other => {
                    return Err(anyhow::anyhow!(
                        "invalid FERRODRUID_CLUSTER_SECURITY={other:?}; \
                         expected `mtls` or `psk`",
                    ));
                }
            };
            Ok(Some(parsed))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(anyhow::anyhow!(
            "could not read FERRODRUID_CLUSTER_SECURITY: {e}",
        )),
    }
}

/// Parse `--node-id` / `--cluster-bind` / `--cluster-peers` into a
/// [`ClusterOptions`] if multi-node mode was requested. Returns `Ok(None)`
/// when the operator has not opted into clustering.
///
/// Wave 40-A: also resolves and validates the cluster pre-shared key.
/// When `cluster_peers` is set:
/// - `cluster_psk` is `Some(...)` → derive the 32-byte key.
/// - `cluster_psk` is `None` and `cluster_psk_not_required = false`
///   (the default) → fatal error: production deployments must always
///   pass `--cluster-psk` (or set the `FERRODRUID_CLUSTER_PSK` env
///   var, which `clap` already maps to the same flag).
/// - `cluster_psk` is `None` and `cluster_psk_not_required = true` →
///   the binary refuses to start without a PSK regardless. Wave 40-A
///   does not ship an unauthenticated wire path; this flag is reserved
///   for a future toggle and currently behaves identically to omitting
///   it.
///
/// Phase 2.4: also resolves the transport **security posture**. The
/// **default** posture is mTLS — when `security` is `None` (no explicit
/// selection) or `Some(Mtls)` the function requires all three of
/// `--cluster-tls-{cert,key,ca}` and returns a configuration error if any
/// are missing (fail-loud; NO silent downgrade to PSK/cleartext). The
/// weaker PSK-over-cleartext fallback is selected ONLY by an explicit
/// `Some(Psk)`.
#[allow(clippy::too_many_arguments)]
fn build_cluster_options(
    node_id: Option<&str>,
    cluster_bind: Option<&str>,
    cluster_peers: Option<&str>,
    http_bind: &str,
    http_port: u16,
    election_timeout_ms: u64,
    heartbeat_interval_ms: u64,
    cluster_psk: Option<&str>,
    cluster_psk_not_required: bool,
    cluster_security: Option<ClusterSecurityArg>,
    cluster_tls_cert: Option<&str>,
    cluster_tls_key: Option<&str>,
    cluster_tls_ca: Option<&str>,
) -> anyhow::Result<Option<ClusterOptions>> {
    let Some(peers_str) = cluster_peers else {
        return Ok(None);
    };
    let trimmed = peers_str.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let id = node_id
        .ok_or_else(|| anyhow::anyhow!("--cluster-peers requires --node-id"))?
        .to_string();

    let bind_addr: SocketAddr = match cluster_bind {
        Some(s) => s
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid --cluster-bind {s}: {e}"))?,
        None => {
            let computed = format!("{http_bind}:{}", http_port.saturating_add(10_000));
            computed
                .parse()
                .map_err(|e| anyhow::anyhow!("could not derive cluster bind {computed}: {e}"))?
        }
    };

    let mut peers = Vec::new();
    for raw in trimmed.split(',') {
        let entry = raw.trim();
        if entry.is_empty() {
            continue;
        }
        let (peer_id, addr_str) = entry
            .split_once('@')
            .ok_or_else(|| anyhow::anyhow!("expected `nodeid@host:port` peer, got {entry}"))?;
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid peer address {addr_str}: {e}"))?;
        peers.push((peer_id.to_string(), addr));
    }

    // Wave 40-A: resolve PSK.
    let psk_str = match cluster_psk {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            let _ = cluster_psk_not_required; // reserved future toggle
            return Err(anyhow::anyhow!(
                "cluster mode requires --cluster-psk (or FERRODRUID_CLUSTER_PSK).\n\
                 Generate one with: head -c 32 /dev/urandom | xxd -p -c 64\n\
                 Wave 40-A does not ship an unauthenticated cluster wire path; \
                 PSK protects every frame against forged ACKs / vote theft.",
            ));
        }
    };
    let psk =
        Arc::new(derive_psk(&psk_str).map_err(|e| anyhow::anyhow!("invalid cluster PSK: {e}"))?);

    // Phase 2.4: resolve the transport security posture. The default
    // (no explicit `--cluster-security`) is mTLS; PSK-cleartext is only
    // selected by an explicit `--cluster-security psk`.
    //
    // All three of `--cluster-tls-{cert,key,ca}` must be set together;
    // partial configuration is rejected to avoid ambiguity.
    let tls_flags_set = [cluster_tls_cert, cluster_tls_key, cluster_tls_ca]
        .iter()
        .filter(|f| f.map(|s| !s.is_empty()).unwrap_or(false))
        .count();
    if tls_flags_set != 0 && tls_flags_set != 3 {
        return Err(anyhow::anyhow!(
            "--cluster-tls-cert, --cluster-tls-key and --cluster-tls-ca must \
             all be provided together (got {tls_flags_set} of 3)",
        ));
    }

    // `None` => default (mTLS); `Some(Mtls)` => explicit mTLS; `Some(Psk)`
    // => explicit cleartext fallback.
    let want_mtls = !matches!(cluster_security, Some(ClusterSecurityArg::Psk));

    let security = if want_mtls {
        // Default / explicit mTLS posture. Require certs; fail loudly if
        // they are missing — NEVER silently fall back to PSK/cleartext.
        if tls_flags_set != 3 {
            return Err(anyhow::anyhow!(
                "cluster mode defaults to mTLS but no TLS certificates were \
                 configured.\nProvide --cluster-tls-cert, --cluster-tls-key and \
                 --cluster-tls-ca (PEM files), or explicitly opt into the weaker \
                 PSK-over-cleartext fallback with --cluster-security psk \
                 (or FERRODRUID_CLUSTER_SECURITY=psk).\nRefusing to start: a \
                 silent downgrade to cleartext would drop wire confidentiality.",
            ));
        }
        #[cfg(feature = "cluster-tls")]
        {
            // `tls_flags_set == 3` was checked above, so all three are
            // `Some(non-empty)`; surface a config error rather than
            // unwrap/expect if that invariant is ever violated.
            let missing = || anyhow::anyhow!("internal: TLS flag missing after count check");
            let cert_path = std::path::PathBuf::from(cluster_tls_cert.ok_or_else(missing)?);
            let key_path = std::path::PathBuf::from(cluster_tls_key.ok_or_else(missing)?);
            let ca_path = std::path::PathBuf::from(cluster_tls_ca.ok_or_else(missing)?);
            let tls = ferrodruid_cluster::tls::TlsConfig::new(cert_path, key_path, ca_path);
            ferrodruid_cluster::transport::ClusterSecurityMode::MutualTls(tls)
        }
        #[cfg(not(feature = "cluster-tls"))]
        {
            return Err(anyhow::anyhow!(
                "cluster mode defaults to mTLS, but this binary was built with \
                 `--no-default-features` (cluster-tls disabled) so it cannot run an \
                 encrypted wire.\nRebuild with the default features, or explicitly \
                 opt into the PSK-over-cleartext fallback with --cluster-security psk \
                 (or FERRODRUID_CLUSTER_SECURITY=psk).\nRefusing to start rather than \
                 silently downgrade to cleartext.",
            ));
        }
    } else {
        // Explicit PSK-over-cleartext opt-in. If the operator also passed
        // TLS certs, that is a contradictory request — reject it loudly.
        if tls_flags_set == 3 {
            return Err(anyhow::anyhow!(
                "--cluster-security psk requested but --cluster-tls-* certs were \
                 also provided; these are contradictory. Drop the TLS flags to run \
                 PSK-cleartext, or drop --cluster-security psk to run the default mTLS.",
            ));
        }
        ferrodruid_cluster::transport::ClusterSecurityMode::PskCleartext
    };

    Ok(Some(ClusterOptions {
        node_id: id,
        bind_addr,
        peers,
        election_timeout_ms,
        heartbeat_interval_ms,
        psk,
        security,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn run_single_binary(
    bind: &str,
    port: u16,
    data_dir: &std::path::Path,
    metadata_uri: Option<&str>,
    auth_enabled: bool,
    cluster_opts: Option<ClusterOptions>,
    strict_null_generation: bool,
    use_default_value_for_null: bool,
    segment_spill: bool,
    segment_cache_bytes: u64,
) -> anyhow::Result<()> {
    println!(
        "FerroDruid v{} — single-binary mode",
        env!("CARGO_PKG_VERSION")
    );
    // W-B legacy null mode: latch the process-global flag ONCE, before any
    // ingest/query path can read it.  The shared serve-init helper fails
    // loudly on a conflicting earlier observation and mirrors Druid's own
    // startup WARN when the legacy property is enabled.
    ferrodruid_common::null_mode::init_legacy_null_mode_serve(use_default_value_for_null)
        .map_err(|e| anyhow::anyhow!(e))?;
    tracing::info!(
        bind = bind,
        port = port,
        data_dir = %data_dir.display(),
        auth_enabled = auth_enabled,
        "starting FerroDruid in single-binary mode"
    );

    // Paid AWS Marketplace **Container** product gate. Only compiled
    // when the binary is built with `--features marketplace-metering`
    // (the paid container image). With the feature off — the default
    // single-binary / OSS build — this block is absent and behaviour is
    // byte-identical. We run it here: config is fully resolved and the
    // HTTP listener has not yet been bound, so a not-entitled or
    // unverifiable deployment exits non-zero *before* serving any
    // traffic (fail closed). The self-host opt-out
    // (`FERRODRUID_MARKETPLACE_DISABLE=1`) is handled loudly inside
    // `verify_startup_entitlement`.
    #[cfg(feature = "marketplace-metering")]
    {
        let mkt_cfg = match ferrodruid_marketplace::MarketplaceConfig::from_env() {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::error!(error = %e, "AWS Marketplace entitlement configuration error");
                eprintln!("FATAL: {e}");
                std::process::exit(3);
            }
        };
        if let Err(e) = ferrodruid_marketplace::verify_startup_entitlement(&mkt_cfg).await {
            tracing::error!(
                error = %e,
                "AWS Marketplace entitlement check failed — refusing to start FerroDruid"
            );
            eprintln!("FATAL: {e}");
            std::process::exit(3);
        }
    }

    // Ensure data directories exist — DURABLY (Codex R7 H2).
    let DataDirs {
        segments_dir,
        deep_storage_dir,
        metadata_dir,
    } = prepare_data_dirs(data_dir).await?;

    // 1. Metadata store. Default (no --metadata-uri / env) is the
    //    historical SQLite file under the data dir — byte-for-byte the
    //    previous behavior; a URI selects PostgreSQL/MySQL/SQLite via
    //    scheme dispatch.
    let default_db_path = metadata_dir.join("ferrodruid.db");
    let default_uri = default_db_path.to_string_lossy();
    let resolved_uri = metadata_uri.unwrap_or(&default_uri);
    // Log only the backend kind — the URI may embed credentials.
    let metadata_backend = match ferrodruid_metadata::parse_metadata_uri(resolved_uri)? {
        ferrodruid_metadata::MetadataUri::Postgres => "postgres",
        ferrodruid_metadata::MetadataUri::MySql => "mysql",
        ferrodruid_metadata::MetadataUri::SqliteMemory => "sqlite (in-memory)",
        ferrodruid_metadata::MetadataUri::SqlitePath(_) => "sqlite",
    };
    let metadata = MetadataStore::connect(resolved_uri).await?;
    metadata.initialize().await?;
    let metadata = Arc::new(metadata);
    tracing::info!(backend = metadata_backend, "metadata store initialized");

    // 2. Deep storage (local filesystem). Durable source for the bootstrap
    //    segment reload (compat-3 stage 1): every published segment is
    //    persisted here before its metadata row is committed, and reloaded
    //    from here on startup.
    let deep_storage: Arc<dyn DeepStorage> = Arc::new(LocalDeepStorage::new(deep_storage_dir));
    tracing::info!("deep storage initialized (local filesystem)");

    // 3. Cluster manager (single-node).
    let node = NodeInfo {
        id: "ferrodruid-single".to_string(),
        host: bind.to_string(),
        port,
        role: NodeRole::AllInOne,
    };
    let cluster = Arc::new(ClusterManager::new_single_node(node));

    // Register all services on this node.
    for svc in &["broker", "historical", "coordinator", "overlord"] {
        cluster.register_service(ServiceEntry {
            service_type: (*svc).to_string(),
            host: bind.to_string(),
            port,
            node_id: "ferrodruid-single".to_string(),
        });
    }

    // 4. Service discovery.
    let _discovery = Arc::new(ServiceDiscovery::new(Arc::clone(&cluster)));
    tracing::info!("cluster state and service discovery initialized");

    // 4b. Multi-node replication (Wave 38-C): if `--cluster-peers` was set,
    // spawn the production TcpTransport so this binary participates in
    // cluster-mode replication. The transport drives a 50 ms tick loop on
    // top of `ReplicationEngine::tick` which performs heartbeat-driven
    // failover (followers run an election timer, leaders broadcast
    // periodic heartbeats). Single-binary mode without cluster-peers is a
    // self-elected single-node Raft.
    let mut _replication_engine: Option<Arc<ReplicationEngine>> = None;
    let mut _replication_transport: Option<Arc<TcpTransport>> = None;
    if let Some(opts) = cluster_opts {
        // CL-A1-R-mTLS-source-fix (W2-E RCA): thread the resolved
        // cluster-wire security posture as a hint to the replication
        // engine so it can widen the election-timeout floor + jitter
        // under mTLS (5 s floor + ~±25 % jitter). Under PSK-cleartext
        // the historical 1.5 s / 150 ms values stay intact — the fix
        // is targeted at the mTLS-only election-storm liveness
        // regression documented in
        // `tests/jepsen-rs/RESULTS_w2e_mtls-rca_2026-06-30.md`.
        let hint = if opts.security.is_mutual_tls() {
            ferrodruid_cluster::replication::ClusterSecurityHint::Mtls
        } else {
            ferrodruid_cluster::replication::ClusterSecurityHint::Psk
        };
        let rcfg = ReplicationConfig {
            node_id: opts.node_id.clone(),
            listen_addr: opts.bind_addr.to_string(),
            peers: opts.peers.iter().map(|(id, _)| id.clone()).collect(),
            heartbeat_interval_ms: opts.heartbeat_interval_ms,
            election_timeout_ms: opts.election_timeout_ms,
            cluster_security_hint: hint,
        };
        let engine = Arc::new(ReplicationEngine::new(rcfg, Arc::clone(&cluster)));
        let tcfg = TcpTransportConfig {
            bind_addr: opts.bind_addr,
            peers: opts.peers,
            connect_timeout: Duration::from_secs(2),
            heartbeat_period: Duration::from_millis(opts.heartbeat_interval_ms),
            psk: Arc::clone(&opts.psk),
            local_node_id: opts.node_id.clone(),
            security: opts.security.clone(),
        };
        tracing::info!(
            mtls = opts.security.is_mutual_tls(),
            "cluster transport security posture resolved",
        );
        let transport = TcpTransport::bind(tcfg, Arc::clone(&engine)).await?;
        // Wave 38-C: timer-driven leader emergence. There is no longer a
        // bootstrap shortcut keyed off `node_id.ends_with('1')` — each node
        // simply runs the same election timer and the first one whose
        // randomly-jittered deadline fires becomes a candidate. The 50 ms
        // cadence is much faster than the 1.5 s default election timeout
        // so a missed-heartbeat deadline is detected promptly.
        transport
            .spawn_tick_loop(Arc::clone(&engine), Duration::from_millis(50))
            .await;
        tracing::info!(
            node_id = %opts.node_id,
            election_timeout_ms = opts.election_timeout_ms,
            heartbeat_interval_ms = opts.heartbeat_interval_ms,
            "cluster tick loop started",
        );
        _replication_engine = Some(engine);
        _replication_transport = Some(transport);
    }

    // 5. Core services.
    let coordinator = Arc::new(Coordinator::new(Arc::clone(&metadata)));
    if segment_spill {
        tracing::info!(
            segment_cache_bytes,
            "FG-7 spill mode ENABLED: segments are spilled to <data_dir>/segments/spill/ and \
             decoded on demand under a memory-budgeted LRU (bounded by segment_cache_bytes)"
        );
    }
    let historical = Arc::new(Historical::with_options(
        segments_dir,
        segment_cache_bytes,
        strict_null_generation,
        segment_spill,
    ));
    let overlord = Arc::new(
        Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical))
            .with_deep_storage(Arc::clone(&deep_storage)),
    );
    let broker = Arc::new(ferrodruid_broker::Broker::new());
    tracing::info!("coordinator, overlord, broker, and historical initialized");

    // Bootstrap reload (compat-3 stage 1): re-download every used segment
    // from deep storage into the (empty) Historical BEFORE resuming Kafka
    // supervisors, so restarted datasources are query-visible again without
    // relying solely on a stream replay. Runs in every build (not kafka-io
    // gated). Held BEFORE `resume_kafka_supervisors` deliberately — see the
    // stage-1 double-count limitation in `bootstrap_reload_segments`.
    match overlord.bootstrap_reload_segments().await {
        Ok(0) => {}
        Ok(n) => tracing::info!(
            reloaded = n,
            "bootstrap-reloaded persisted segments from deep storage after restart"
        ),
        // A durable (loadSpec-bearing) segment that cannot be restored from
        // deep storage is a genuine durability violation (H4): a batch segment
        // cannot be rebuilt by Kafka replay, so serving would silently omit its
        // rows. `bootstrap_reload_segments` only errs on that case (legacy rows
        // with no blob are skipped internally), so refuse to come up rather than
        // serve incomplete data — restart after the operator restores the blob.
        Err(e) => {
            return Err(anyhow::Error::new(e).context(
                "bootstrap segment reload failed: a durable segment could not be \
                 restored from deep storage; refusing to serve incomplete data",
            ));
        }
    }

    // Resume persisted, non-suspended Kafka supervisors (kafka-io builds).
    // Segments are memory-resident only, so this re-consumes from Kafka to
    // rebuild in-memory segments after a restart. Takes an Arc clone so a
    // candidate whose startup lifecycle op fails transiently (unreachable
    // broker) is retried by a background task instead of being starved
    // until the next restart (Codex R6 H3).
    #[cfg(feature = "kafka-io")]
    match Arc::clone(&overlord).resume_kafka_supervisors().await {
        Ok(0) => {}
        Ok(n) => tracing::info!(
            resumed = n,
            "resumed persisted Kafka supervisors after restart"
        ),
        Err(e) => tracing::warn!(error = %e, "failed to resume persisted Kafka supervisors"),
    }

    // Resume persisted, non-suspended Kinesis supervisors (kinesis-io
    // builds, compat-5). Runs AFTER the bootstrap reload above so each
    // consumer's durable resume frontier sees the reloaded segments and
    // resumes past them (zero loss, no double-count); one consumer per
    // (datasource, stream) pair. Transient startup failures are retried
    // by an at-most-one background task.
    #[cfg(feature = "kinesis-io")]
    match Arc::clone(&overlord).resume_kinesis_supervisors().await {
        Ok(0) => {}
        Ok(n) => tracing::info!(
            resumed = n,
            "resumed persisted Kinesis supervisors after restart"
        ),
        Err(e) => tracing::warn!(error = %e, "failed to resume persisted Kinesis supervisors"),
    }

    // 6. Auth.  If enabled, bootstrap a single `admin` account on first
    //    launch (marker file `<data_dir>/.bootstrap_done`) and print the
    //    generated password to stderr.  Subsequent launches reuse the
    //    persisted store (currently in-memory; persistence tracked in
    //    Wave 36+).
    let mut auth_store_inner = AuthStore::new();
    if auth_enabled {
        bootstrap_admin_if_needed(&mut auth_store_inner, data_dir)?;
    }
    // Shared, runtime-mutable store: the change-credential endpoint takes a
    // write lock to rotate a password while the auth middleware verifies
    // under a read lock.
    let auth_store = Arc::new(parking_lot::RwLock::new(auth_store_inner));
    let authorizer = Arc::new(Authorizer::new().with_admin_role());
    tracing::info!(auth_enabled = auth_enabled, "auth and authz initialized");

    // 7. App state and router.
    let lookup_manager = Arc::new(ferrodruid_lookup::LookupManager::new());
    let metrics = Arc::new(ferrodruid_telemetry::Metrics::new());
    tracing::info!("lookup manager and metrics initialized");

    let msq_manager = Arc::new(MsqManager::new());

    let state = Arc::new(AppState {
        coordinator,
        overlord,
        metadata,
        auth_store,
        // Persist credential rotations (the change-credential endpoint) back
        // to `<data_dir>/auth/admin.json` so a changed admin password — and
        // the cleared force-change flag — survive a restart.
        auth_cred_dir: Some(data_dir.join("auth")),
        authorizer,
        auth_enabled,
        broker,
        historicals: vec![historical],
        start_time: chrono::Utc::now(),
        lookup_manager,
        metrics,
        msq_manager,
        // Wave 36-B: 100 in-flight cap per `RateLimitConfig::default`.
        rate_limit_max_concurrent: 100,
    });

    let app = create_router(state);

    // 8. Bind and serve.
    let addr: SocketAddr = format!("{bind}:{port}")
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid bind address: {e}"))?;
    tracing::info!(addr = %addr, "listening");
    println!("Listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(
        "FerroDruid v{} listening on {}",
        env!("CARGO_PKG_VERSION"),
        addr
    );

    // Graceful shutdown on SIGINT (Ctrl-C) or SIGTERM.
    let shutdown = async {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut sig) => {
                    sig.recv().await;
                }
                Err(e) => {
                    // Do not panic a running server because a second signal
                    // source could not be installed; fall back to Ctrl-C only.
                    tracing::warn!(
                        error = %e,
                        "could not install SIGTERM handler; relying on Ctrl-C (SIGINT) for shutdown"
                    );
                    std::future::pending::<()>().await;
                }
            }
        };
        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => tracing::info!("received SIGINT, shutting down gracefully..."),
            _ = terminate => tracing::info!("received SIGTERM, shutting down gracefully..."),
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;

    tracing::info!("FerroDruid stopped.");
    Ok(())
}

/// Ensure an `admin` user exists, persisting its credential so the deployment
/// survives a restart.
///
/// On first launch (no persisted credential) a random 32-char admin password is
/// generated, added to the store, and its **Argon2id hash** (never the
/// plaintext) plus roles are written to `<data_dir>/auth/admin.json` (mode
/// `0600`). The password is printed to stderr exactly once (not to the
/// structured tracing log, to discourage shipping it to an aggregator).
///
/// On every subsequent launch the persisted credential is reloaded into the
/// in-memory store, so an auth-enabled deployment is **not** locked out after a
/// reboot / task / pod restart. (The previous marker-only approach skipped
/// admin generation on restart but left the in-memory store empty, locking the
/// operator out — fixed here.)
fn bootstrap_admin_if_needed(store: &mut AuthStore, data_dir: &Path) -> anyhow::Result<()> {
    let cred_path = data_dir.join("auth").join("admin.json");

    // Returning deployment: reload the persisted admin credential so login keeps
    // working across restarts.
    if cred_path.exists() {
        let bytes = std::fs::read(&cred_path)
            .map_err(|e| anyhow::anyhow!("read admin credential {}: {e}", cred_path.display()))?;
        let record: ferrodruid_auth::UserRecord = serde_json::from_slice(&bytes)
            .map_err(|e| anyhow::anyhow!("parse admin credential {}: {e}", cred_path.display()))?;
        // Honour the persisted force-change flag: a never-rotated admin stays
        // gated across restarts; a rotated admin (flag cleared) does not.
        store.add_user_with_hash(
            &record.username,
            &record.password_hash,
            record.roles,
            record.must_change_password,
        );
        tracing::info!(
            path = %cred_path.display(),
            must_change_password = record.must_change_password,
            "loaded persisted admin credential (restart-safe)"
        );
        return Ok(());
    }

    // First launch: generate, add, persist the hash, print the password once.
    // The admin is created with `must_change_password = true` so the
    // auto-generated initial password can only reach the change-credential
    // endpoint until the operator rotates it (AWS Marketplace requirement).
    let password = generate_random_password(32);
    store
        .add_user_must_change("admin", &password, vec!["admin".to_string()], true)
        .map_err(|e| anyhow::anyhow!("bootstrap admin user: {e}"))?;

    let record = store
        .user_record("admin")
        .ok_or_else(|| anyhow::anyhow!("admin user missing immediately after creation"))?;
    let json = serde_json::to_vec_pretty(record)
        .map_err(|e| anyhow::anyhow!("serialize admin credential: {e}"))?;

    let auth_dir = cred_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("admin credential path has no parent"))?;
    std::fs::create_dir_all(auth_dir)
        .map_err(|e| anyhow::anyhow!("create auth dir {}: {e}", auth_dir.display()))?;
    restrict_dir_perms(auth_dir);
    write_private_file(&cred_path, &json)?;

    eprintln!(
        "============================================================\n\
         FerroDruid FIRST-LAUNCH ADMIN CREDENTIALS\n\
         ------------------------------------------------------------\n\
         username = admin\n\
         password = {password}\n\
         ------------------------------------------------------------\n\
         PASSWORD CHANGE REQUIRED ON FIRST LOGIN.  Until you rotate it,\n\
         this password can ONLY be used to set a new one:\n\
           curl -u admin:<above> -X POST \\\n\
             http://<host>:<port>/druid-ext/basic-security/authentication/\\\n\
         db/basic/users/admin/credential \\\n\
             -H 'Content-Type: application/json' \\\n\
             -d '{{\"password\":\"<new-strong-password>\"}}'\n\
         This password is printed exactly once.  The Argon2id hash is\n\
         persisted to `{}` (mode 0600) so login survives a restart; the\n\
         plaintext password is NOT stored.\n\
         ============================================================",
        cred_path.display()
    );
    Ok(())
}

/// The three per-`--data-dir` directories the single-binary server needs,
/// created DURABLY at startup by [`prepare_data_dirs`].
struct DataDirs {
    /// Historical segment cache (`<data_dir>/segments`).
    segments_dir: PathBuf,
    /// Deep-storage root (`<data_dir>/deep-storage`) — the durable blob
    /// store the committed⇒durable invariant depends on.
    deep_storage_dir: PathBuf,
    /// Metadata store dir (`<data_dir>/metadata`, holds `ferrodruid.db`).
    metadata_dir: PathBuf,
}

/// Create the data directories DURABLY (Codex R7 H2): each is created via
/// [`ferrodruid_deep_storage::create_dir_all_durable`], which fsyncs every
/// newly created directory AND the pre-existing parent whose entry list
/// changed (`data_dir` itself — and, on a first launch with a fresh
/// `data_dir`, its parent too).
///
/// The plain `create_dir_all` this replaces left the deep-storage ROOT's
/// dentry (inside `data_dir`) in the page cache only: the segment upload
/// path fsyncs everything from the blob file UP TO the storage root
/// ([`fsync_chain`](ferrodruid_deep_storage) stops there, assuming the root
/// is durable), so after P (persist) and M (metadata commit) a power loss
/// could still drop the `deep-storage/` dentry itself — every committed
/// blob orphaned, the bootstrap reload finds phantom rows (loss). The
/// metadata dir gets the same treatment: SQLite fsyncs its files, not the
/// directory entry `ferrodruid.db` lives in.
async fn prepare_data_dirs(data_dir: &Path) -> anyhow::Result<DataDirs> {
    let dirs = DataDirs {
        segments_dir: data_dir.join("segments"),
        deep_storage_dir: data_dir.join("deep-storage"),
        metadata_dir: data_dir.join("metadata"),
    };
    for dir in [
        &dirs.segments_dir,
        &dirs.deep_storage_dir,
        &dirs.metadata_dir,
    ] {
        let synced = ferrodruid_deep_storage::create_dir_all_durable(dir)
            .await
            .map_err(|e| anyhow::anyhow!("create data dir {}: {e}", dir.display()))?;
        tracing::debug!(
            dir = %dir.display(),
            fsynced = ?synced,
            "data directory durably created (dir + parent dentries fsynced)"
        );
    }
    Ok(dirs)
}

/// Write `bytes` to `path`, creating/truncating it with owner-only permissions
/// (`0600`) on Unix so the persisted credential hash is not world-readable.
#[cfg(unix)]
fn write_private_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| anyhow::anyhow!("open {} for write: {e}", path.display()))?;
    f.write_all(bytes)
        .map_err(|e| anyhow::anyhow!("write {}: {e}", path.display()))?;
    Ok(())
}

/// Non-Unix fallback: best-effort write without Unix permission bits.
#[cfg(not(unix))]
fn write_private_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    std::fs::write(path, bytes).map_err(|e| anyhow::anyhow!("write {}: {e}", path.display()))?;
    Ok(())
}

/// Best-effort tighten of a directory to owner-only (`0700`) on Unix. The
/// credential file itself is already `0600`; this is defense in depth, so a
/// failure here is logged, not fatal.
fn restrict_dir_perms(dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)) {
            tracing::warn!(dir = %dir.display(), error = %e, "could not tighten auth dir permissions");
        }
    }
    #[cfg(not(unix))]
    let _ = dir;
}

/// Wave 34.T → 38.FF: route classic mode through the role-split
/// scaffold.
///
/// Maps the existing [`ClassicRole`] CLI enum onto the
/// [`ferrodruid_role::Role`] enum, builds a [`RoleConfig`], and then
/// asks the dispatcher what to do. In v0.1.x every dedicated role
/// (broker/historical/coordinator/router/overlord/middleManager)
/// prints a banner and exits success — the cross-role wire is v1.0
/// work, tracked in `docs/v1.0-roadmap.md` (W3). Wave 38.FF expands
/// this from the original 3-role scaffold to the complete Druid
/// 6-role topology.
fn run_classic_role(
    role: Option<ClassicRole>,
    bind: &str,
    port: u16,
    data_dir: &std::path::Path,
) -> anyhow::Result<()> {
    let Some(role) = role else {
        eprintln!(
            "FerroDruid v{} — `--mode classic` requires `--role \
             <broker|historical|coordinator|router|overlord|middlemanager>`. \
             Use `--mode single-binary` for an all-in-one node, or see docs/v1.0-roadmap.md.",
            env!("CARGO_PKG_VERSION")
        );
        std::process::exit(2);
    };
    let dispatch_role = match role {
        ClassicRole::Broker => DispatchRole::Broker,
        ClassicRole::Historical => DispatchRole::Historical,
        ClassicRole::Coordinator => DispatchRole::Coordinator,
        ClassicRole::Overlord => DispatchRole::Overlord,
        ClassicRole::Router => DispatchRole::Router,
        ClassicRole::Middlemanager => DispatchRole::MiddleManager,
    };
    let cfg = RoleConfig::try_new(dispatch_role, bind, port, data_dir.to_path_buf())
        .map_err(|e| anyhow::anyhow!("invalid role config: {e}"))?;
    println!("{}", cfg.banner());
    tracing::info!(
        role = %cfg.role,
        bind = %cfg.bind,
        port = cfg.port,
        "classic mode role dispatch (Wave 38.FF 6-role scaffold)",
    );
    match role_dispatch(cfg.role) {
        DispatchOutcome::LaunchSingleBinary => {
            // The dispatcher only emits this for `Standalone`, which
            // we never construct here. Kept exhaustive for safety.
            anyhow::bail!("dispatcher unexpectedly resolved to single-binary for classic role");
        }
        DispatchOutcome::LogAndExitOk => {
            eprintln!(
                "FerroDruid v{} (Wave 38.FF 6-role scaffold): role `{}` boots and reports \
                 its identity but the cross-role wire is not yet implemented. Use \
                 `--mode single-binary` for a working node, or follow docs/v1.0-roadmap.md \
                 for the v1.0 multi-process plan.",
                env!("CARGO_PKG_VERSION"),
                cfg.role,
            );
            Ok(())
        }
    }
}

/// Generate a random alphanumeric password of `len` characters using the
/// OS RNG.
fn generate_random_password(len: usize) -> String {
    use rand::{Rng, distributions::Alphanumeric};
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[cfg(test)]
mod segment_cache_bytes_tests {
    use super::*;
    use clap::Parser;

    /// FG-7 (Codex R11 HIGH): `--segment-cache-bytes 0` is rejected at CLI
    /// parse time. The same `value_parser` range (`1..`) also guards the
    /// `FERRODRUID_SEGMENT_CACHE_BYTES` env source (clap applies the value
    /// parser to env-sourced values), so both binary entry paths are covered
    /// by this one choke point. A zero budget would make spill mode treat
    /// every segment as oversized (re-decoded per query) and heap mode reject
    /// all admission.
    #[test]
    fn zero_segment_cache_bytes_is_rejected() {
        // `Cli` does not derive `Debug`, so match rather than `expect_err`.
        match Cli::try_parse_from(["ferrodruid", "serve", "--segment-cache-bytes", "0"]) {
            Ok(_) => panic!("zero segment-cache-bytes budget must be rejected at parse time"),
            Err(err) => assert_eq!(
                err.kind(),
                clap::error::ErrorKind::ValueValidation,
                "a zero budget must be a value-range validation error: {err}"
            ),
        }
    }

    /// Regression: a normal (nonzero) budget parses through and is preserved.
    #[test]
    fn nonzero_segment_cache_bytes_parses() {
        let cli =
            Cli::try_parse_from(["ferrodruid", "serve", "--segment-cache-bytes", "1073741824"])
                .expect("nonzero budget must parse");
        let Command::Serve {
            segment_cache_bytes,
            ..
        } = cli.command;
        assert_eq!(segment_cache_bytes, 1_073_741_824);
    }
}

#[cfg(test)]
mod cluster_security_tests {
    use super::*;

    /// Thin wrapper around [`build_cluster_options`] for the common test
    /// case: a single peer, a valid PSK, and configurable security +
    /// TLS-flag inputs.
    fn build(
        security: Option<ClusterSecurityArg>,
        cert: Option<&str>,
        key: Option<&str>,
        ca: Option<&str>,
    ) -> anyhow::Result<Option<ClusterOptions>> {
        build_cluster_options(
            Some("node-1"),
            Some("127.0.0.1:9000"),
            Some("node-2@127.0.0.1:9001"),
            "127.0.0.1",
            8888,
            1500,
            250,
            Some("test-psk-phase-2-4"),
            false,
            security,
            cert,
            key,
            ca,
        )
    }

    /// DEFAULT (no `--cluster-security`, no certs) → mTLS is required and
    /// the absence of certs is a FATAL config error. NO silent downgrade.
    #[test]
    fn default_without_certs_fails_loudly() {
        let err = build(None, None, None, None).expect_err("default mTLS without certs must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("mTLS") && msg.contains("Refusing to start"),
            "error must explain the fail-loud mTLS default, got: {msg}",
        );
    }

    /// DEFAULT with all three certs → mTLS posture is selected.
    #[test]
    fn default_with_certs_selects_mtls() {
        let opts = build(None, Some("c.pem"), Some("k.pem"), Some("ca.pem"))
            .expect("build")
            .expect("cluster opts present");
        assert!(
            opts.security.is_mutual_tls(),
            "default posture with certs must be mTLS",
        );
    }

    /// Explicit `--cluster-security mtls` with no certs also fails loudly.
    #[test]
    fn explicit_mtls_without_certs_fails_loudly() {
        let err = build(Some(ClusterSecurityArg::Mtls), None, None, None)
            .expect_err("explicit mTLS without certs must fail");
        assert!(err.to_string().contains("Refusing to start"));
    }

    /// Explicit `--cluster-security psk` selects the PSK-cleartext opt-in.
    #[test]
    fn explicit_psk_opt_in_selects_psk_cleartext() {
        let opts = build(Some(ClusterSecurityArg::Psk), None, None, None)
            .expect("build")
            .expect("cluster opts present");
        assert!(
            !opts.security.is_mutual_tls(),
            "explicit PSK opt-in must select PSK-cleartext, not mTLS",
        );
    }

    /// `--cluster-security psk` WITH TLS certs is contradictory → rejected.
    #[test]
    fn explicit_psk_with_certs_is_contradictory() {
        let err = build(
            Some(ClusterSecurityArg::Psk),
            Some("c.pem"),
            Some("k.pem"),
            Some("ca.pem"),
        )
        .expect_err("psk + certs must be rejected");
        assert!(err.to_string().contains("contradictory"));
    }

    /// Partial TLS config (only 2 of 3 flags) is rejected before posture
    /// selection.
    #[test]
    fn partial_tls_flags_rejected() {
        let err = build(None, Some("c.pem"), Some("k.pem"), None)
            .expect_err("partial TLS flags must be rejected");
        assert!(err.to_string().contains("all be provided together"));
    }

    /// No `--cluster-peers` → no cluster options, regardless of security.
    #[test]
    fn no_peers_returns_none() {
        let out = build_cluster_options(
            Some("node-1"),
            None,
            None,
            "127.0.0.1",
            8888,
            1500,
            250,
            Some("psk"),
            false,
            None,
            None,
            None,
            None,
        )
        .expect("build");
        assert!(out.is_none(), "no peers => no cluster options");
    }

    /// `resolve_cluster_security`: an explicit flag wins over any env var
    /// (deterministic regardless of the ambient environment).
    #[test]
    fn resolve_security_explicit_flag_wins() {
        assert_eq!(
            resolve_cluster_security(Some(ClusterSecurityArg::Mtls)).expect("resolve"),
            Some(ClusterSecurityArg::Mtls),
        );
        assert_eq!(
            resolve_cluster_security(Some(ClusterSecurityArg::Psk)).expect("resolve"),
            Some(ClusterSecurityArg::Psk),
        );
    }

    /// Codex R7 H2: the startup data-dir preparation must create the three
    /// data directories DURABLY — via `create_dir_all_durable`, whose fsync
    /// chain reaches the parent that gained each new dentry (`data_dir`) —
    /// not via a plain page-cache-only `create_dir_all`. The chain rule
    /// itself is pinned in the deep-storage crate; this pins that the
    /// startup path uses it for all three dirs and survives a re-run.
    #[tokio::test]
    async fn prepare_data_dirs_creates_all_three_durably() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data_dir = tmp.path().join("data");
        let dirs = prepare_data_dirs(&data_dir).await.expect("prepare");
        assert!(dirs.segments_dir.is_dir());
        assert!(dirs.deep_storage_dir.is_dir());
        assert!(dirs.metadata_dir.is_dir());
        assert_eq!(dirs.segments_dir, data_dir.join("segments"));
        assert_eq!(dirs.deep_storage_dir, data_dir.join("deep-storage"));
        assert_eq!(dirs.metadata_dir, data_dir.join("metadata"));
        // Idempotent restart with the same --data-dir.
        prepare_data_dirs(&data_dir).await.expect("re-prepare");
        // A file blocking a data dir must fail loudly, not half-start.
        let blocked = tmp.path().join("blocked");
        tokio::fs::create_dir_all(&blocked).await.expect("mk");
        tokio::fs::write(blocked.join("segments"), b"x")
            .await
            .expect("write blocker");
        assert!(
            prepare_data_dirs(&blocked).await.is_err(),
            "a file where a data dir belongs must surface as an error"
        );
    }
}
