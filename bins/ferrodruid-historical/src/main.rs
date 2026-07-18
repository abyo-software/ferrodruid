// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `ferrodruid-historical` — Wave 41.OO + 42.RR role-split launcher
//! with real HTTP wire, real segment loading + query execution, and
//! pluggable deep-storage backends (local FS or S3-compatible).
//!
//! This binary is the v1.0 entry point for the **historical** role:
//! it loads immutable segments from deep storage and answers
//! per-segment query fragments forwarded by brokers. The HTTP server
//! binds on `--bind:--port` (default `127.0.0.1:8083`, mirroring
//! Apache Druid's historical port) and exposes the cross-role
//! contracts defined in `ferrodruid-rpc`:
//!
//! - `POST /druid/v2/native` — accept a per-segment query (either the
//!   W4 `SegmentQuery` echo shape or the Wave 41.OO real
//!   `NativeQuery` shape: `timeseries` / `scan`). When the body
//!   carries a `queryType` field the historical executes the query
//!   against the loaded segment artifact and returns real rows.
//! - `POST /druid/v1/historical/load` — accept a segment-load
//!   command from the coordinator. With `--real-loader` (Wave 41.OO),
//!   the historical reads the JSON-Lines segment artifact from
//!   `<deep-storage-root>/<dataSource>/<segmentId>/segment.jsonl`,
//!   parses it into the in-memory segment store, and flips state to
//!   `Loaded` on success / `Failed` on missing-or-malformed artifact.
//!   Without `--real-loader` the W4 stub-loader semantics apply
//!   (state always flips to `Loaded`).
//! - `POST /druid/v1/historical/drop` — accept a segment-drop
//!   command, mark the segment `Dropped`, and evict it from the
//!   in-memory store.
//! - `GET /druid/v1/historical/loadstatus` — return the current
//!   `segment_id -> SegmentLoadState` table.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ferrodruid_deep_storage::{DeepStorage, S3DeepStorage};
use ferrodruid_role::{Role, RoleConfig};
use ferrodruid_rpc::historical_server::{self, HistoricalServerState};
use ferrodruid_rpc::{CrossRoleMtlsMode, CrossRoleStartup, serve_cross_role};

/// CLI surface for the historical role.
#[derive(Parser, Debug)]
#[command(name = "ferrodruid-historical", version, about)]
struct Cli {
    /// Address to bind to. Defaults to loopback.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,

    /// Port to listen on. Default mirrors Apache Druid's historical
    /// port.
    #[arg(long, default_value_t = 8083)]
    port: u16,

    /// Data directory for the segment cache.
    #[arg(long, default_value = "./data/historical")]
    data_dir: PathBuf,

    /// Optional historical identity override; auto-generated when
    /// absent.
    #[arg(long)]
    historical_id: Option<String>,

    /// Tier label this historical reports.
    #[arg(long, default_value = "default")]
    tier: String,

    /// Simulated `Loading → Loaded` delay in milliseconds. With
    /// `--real-loader` the actual artifact I/O happens during this
    /// window; without it the loader simply waits then reports
    /// `Loaded` (Wave 40.LL stub-loader semantics).
    #[arg(long, default_value_t = 50)]
    loading_to_loaded_ms: u64,

    /// Wave 41.OO: opt into real segment loading + query execution.
    /// When set, segment-load commands actually read the JSON-Lines
    /// artifact at
    /// `<deep-storage-root>/<dataSource>/<segmentId>/segment.jsonl`
    /// and the historical answers `/druid/v2/native` queries against
    /// the parsed segment. Defaults off so the W4 wire tests keep
    /// passing unchanged.
    #[arg(long, default_value_t = false)]
    real_loader: bool,

    /// Filesystem root or S3 URI the real loader reads segment
    /// artifacts from. Only consulted when `--real-loader` is set.
    ///
    /// Accepted shapes (Wave 42.RR):
    /// - bare path or `file://<path>` — local FS layout
    ///   `<root>/<dataSource>/<segmentId>/segment.jsonl` (Wave 41.OO).
    /// - `s3://<bucket>/<prefix>` — segments live as objects under
    ///   `<prefix>/<dataSource>/<segmentId>/segment.jsonl`. Credentials
    ///   come from the standard AWS SDK chain. LocalStack-friendly via
    ///   the `AWS_ENDPOINT_URL` env var.
    ///
    /// Defaults to `/tmp/ferrodruid-deep` (matching the v1.0 docs).
    #[arg(long, default_value = "/tmp/ferrodruid-deep")]
    deep_storage_root: String,

    /// Wave 42.RR: local cache directory the historical materialises
    /// remote-backed segment artifacts into before parsing them.
    /// Ignored when `--deep-storage-root` is a local path. Defaults to
    /// `<TMPDIR>/ferrodruid-historical-cache`.
    #[arg(long)]
    cache_dir: Option<PathBuf>,

    /// W1-I (CL-J1): cross-role HTTP wire mTLS mode
    /// (`required` / `permissive` / `disabled`). See `docs/SECURITY.md`.
    #[arg(long, env = "FERRODRUID_CROSS_ROLE_MTLS", default_value = "required")]
    cross_role_mtls: String,

    /// Optional TLS bind port (default = `port + 1000`).
    #[arg(long)]
    tls_port: Option<u16>,

    /// Explicit PEM leaf cert chain (set with `--cross-role-tls-key`
    /// and `--cross-role-tls-ca`; otherwise falls back to
    /// `<data_dir>/cross-role/`).
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

    let cfg = match RoleConfig::try_new(Role::Historical, &cli.bind, cli.port, cli.data_dir.clone())
    {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("FATAL: {e}");
            return ExitCode::from(2);
        }
    };

    println!("{}", cfg.banner());
    tracing::info!(role = %cfg.role, bind = %cfg.bind, port = cfg.port, "historical role starting");

    let historical_id = cli
        .historical_id
        .clone()
        .unwrap_or_else(|| HistoricalServerState::default().historical_id);
    let state = if cli.real_loader {
        match build_real_loader_state(&cli, historical_id.clone()) {
            Ok(s) => Arc::new(s),
            Err(e) => {
                eprintln!("FATAL: failed to build deep-storage backend: {e}");
                return ExitCode::from(2);
            }
        }
    } else {
        Arc::new(HistoricalServerState::with_config(
            historical_id,
            cli.tier.clone(),
            Duration::from_millis(cli.loading_to_loaded_ms),
        ))
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("FATAL: failed to start tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };

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
        cli.cross_role_tls_cert.clone(),
        cli.cross_role_tls_key.clone(),
        cli.cross_role_tls_ca.clone(),
        Some(cert_dir),
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("FATAL: cross-role mTLS setup: {e}");
            return ExitCode::from(2);
        }
    };

    runtime.block_on(async move {
        let app = historical_server::router(state);
        let (mode, plain, tls) = match startup.into_listeners() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("FATAL: cross-role listener build: {e}");
                return ExitCode::from(2);
            }
        };
        eprintln!(
            "ferrodruid-historical (W1-I): mTLS={mode} | endpoints: POST /druid/v2/native, \
             POST /druid/v1/historical/load, POST /druid/v1/historical/drop, \
             GET /druid/v1/historical/loadstatus | real_loader={} deep_root={}",
            cli.real_loader, cli.deep_storage_root,
        );
        if let Err(e) = serve_cross_role(app, mode, plain, tls).await {
            eprintln!("FATAL: serve_cross_role: {e}");
            return ExitCode::from(1);
        }
        ExitCode::SUCCESS
    })
}

/// Parsed shape of the `--deep-storage-root` argument.
#[derive(Debug, PartialEq, Eq)]
enum DeepStorageRoot {
    /// Local filesystem rooted at the supplied path.
    Local(PathBuf),
    /// S3-compatible object store. Credentials come from the standard
    /// AWS SDK chain. The optional `prefix` is appended to every
    /// object key, and always ends in `/` if non-empty.
    S3 {
        /// Bucket name (the host component of the URI).
        bucket: String,
        /// Object-key prefix (the path component, normalised to end
        /// with `/`).
        prefix: String,
    },
}

/// Parse a `--deep-storage-root` value into a [`DeepStorageRoot`].
///
/// Accepts:
/// - bare path (`/var/lib/ferro`) → `Local`.
/// - `file://<path>` → `Local`.
/// - `s3://bucket[/prefix]` → `S3` with normalised prefix.
fn parse_deep_storage_root(raw: &str) -> Result<DeepStorageRoot, String> {
    if let Some(rest) = raw.strip_prefix("s3://") {
        let mut parts = rest.splitn(2, '/');
        let bucket = parts.next().unwrap_or("");
        if bucket.is_empty() {
            return Err(format!("s3 URI missing bucket: {raw}"));
        }
        let raw_prefix = parts.next().unwrap_or("");
        let prefix = if raw_prefix.is_empty() {
            String::new()
        } else if raw_prefix.ends_with('/') {
            raw_prefix.to_string()
        } else {
            format!("{raw_prefix}/")
        };
        Ok(DeepStorageRoot::S3 {
            bucket: bucket.to_string(),
            prefix,
        })
    } else if let Some(rest) = raw.strip_prefix("file://") {
        Ok(DeepStorageRoot::Local(PathBuf::from(rest)))
    } else {
        Ok(DeepStorageRoot::Local(PathBuf::from(raw)))
    }
}

fn build_real_loader_state(
    cli: &Cli,
    historical_id: String,
) -> Result<HistoricalServerState, String> {
    let parsed = parse_deep_storage_root(&cli.deep_storage_root)?;
    match parsed {
        DeepStorageRoot::Local(path) => Ok(HistoricalServerState::with_root(
            historical_id,
            cli.tier.clone(),
            Duration::from_millis(cli.loading_to_loaded_ms),
            path,
        )),
        DeepStorageRoot::S3 { bucket, prefix } => {
            let cache = cli
                .cache_dir
                .clone()
                .unwrap_or_else(|| std::env::temp_dir().join("ferrodruid-historical-cache"));
            // `from_env` consumes AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
            // AWS_DEFAULT_REGION / AWS_ENDPOINT_URL — the latter is the
            // LocalStack hook the integration tests exercise.
            let s3 = S3DeepStorage::from_env(&bucket, &prefix)
                .map_err(|e| format!("build S3 deep storage: {e}"))?;
            let remote: Arc<dyn DeepStorage> = Arc::new(s3);
            Ok(HistoricalServerState::with_remote(
                historical_id,
                cli.tier.clone(),
                Duration::from_millis(cli.loading_to_loaded_ms),
                cache,
                remote,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_local_path_without_scheme() {
        let r = parse_deep_storage_root("/tmp/x").expect("parse");
        assert_eq!(r, DeepStorageRoot::Local(PathBuf::from("/tmp/x")));
    }

    #[test]
    fn parse_file_scheme_strips_prefix() {
        let r = parse_deep_storage_root("file:///var/data").expect("parse");
        assert_eq!(r, DeepStorageRoot::Local(PathBuf::from("/var/data")));
    }

    #[test]
    fn parse_s3_uri_with_prefix_normalises_trailing_slash() {
        let r = parse_deep_storage_root("s3://my-bucket/segments").expect("parse");
        assert_eq!(
            r,
            DeepStorageRoot::S3 {
                bucket: "my-bucket".into(),
                prefix: "segments/".into(),
            }
        );
    }

    #[test]
    fn parse_s3_uri_with_explicit_trailing_slash_preserved() {
        let r = parse_deep_storage_root("s3://b/p/").expect("parse");
        assert_eq!(
            r,
            DeepStorageRoot::S3 {
                bucket: "b".into(),
                prefix: "p/".into(),
            }
        );
    }

    #[test]
    fn parse_s3_uri_bucket_only_yields_empty_prefix() {
        let r = parse_deep_storage_root("s3://only-bucket").expect("parse");
        assert_eq!(
            r,
            DeepStorageRoot::S3 {
                bucket: "only-bucket".into(),
                prefix: String::new(),
            }
        );
    }

    #[test]
    fn parse_s3_uri_missing_bucket_errors() {
        let r = parse_deep_storage_root("s3://");
        assert!(r.is_err(), "{r:?}");
    }
}
