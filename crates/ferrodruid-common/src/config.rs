// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Configuration types for FerroDruid.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{DruidError, Result};

/// Top-level configuration for a FerroDruid node.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DruidConfig {
    /// Deployment mode.
    pub mode: DeployMode,
    /// Address to bind to.
    pub bind_addr: String,
    /// Port to bind to.
    pub bind_port: u16,
    /// Metadata storage configuration.
    pub metadata_storage: MetadataStorageConfig,
    /// Deep storage configuration.
    pub deep_storage: DeepStorageConfig,
    /// Authentication / authorization configuration.
    #[serde(default)]
    pub auth: AuthConfig,
    /// Rate-limiter configuration (max concurrent in-flight requests).
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    /// Segment reader: if `true`, columns whose decode fails are silently
    /// dropped from the loaded segment instead of failing the read.
    ///
    /// **Defaults to `false` (strict mode)**.  Wave 36-E (Wave 37 R1
    /// `fdx.rs:64-79`) made strict the default: a corrupt column now
    /// surfaces as a hard error rather than silently producing wrong query
    /// results.  Operators with on-disk segments that pre-date the strict
    /// reader and need a one-time migration window may flip this knob to
    /// `true` to recover the old "drop and continue" behaviour.  Not
    /// recommended for production.
    #[serde(default)]
    pub segment_lenient_mode: bool,

    /// Segment reader: when `true`, a segment that carries the
    /// legacy-null-CONSISTENT signature — columns containing the legacy
    /// coercion defaults (`""` strings / `0` numerics) with **no** null
    /// markers (no null dictionary entry, no null-row bitmap, no null
    /// encoding) — is **refused with a hard error** instead of being loaded
    /// with a once-per-datasource warning.
    ///
    /// **Defaults to `false` (warn only).**  The detection is a heuristic:
    /// data written by a legacy Druid (`useDefaultValueForNull=true`, the
    /// default on <= 27) is byte-identical to modern data that genuinely
    /// contains empty strings / zeros and no NULLs, so strict mode WILL
    /// reject such genuine data too (`0` is common in real metrics).  Turn
    /// this on only to gate ingestion/serving of segments whose null
    /// generation must be positively confirmed before FerroDruid answers
    /// with modern SQL-null semantics.  See
    /// `ferrodruid_segment::null_generation` for the full honesty notes.
    #[serde(default)]
    pub strict_null_generation: bool,

    /// Per-query resource limits for `topN` / `groupBy`.
    ///
    /// Wave 36-G1 (Wave 37B query Top-1 finding): an attacker-crafted
    /// query that groups by a high-cardinality column (e.g. UUID, request
    /// id) would otherwise drive the historical's
    /// `HashMap<String, Vec<Box<dyn Aggregator>>>` to RAM exhaustion well
    /// before any threshold / limit was applied.  These caps reject the
    /// query with `DruidError::ResourceLimit` (REST: `429 Too Many Keys`)
    /// once the in-flight per-bucket bucket count exceeds the threshold.
    #[serde(default)]
    pub query_limits: QueryLimitsConfig,

    /// Wave 38-DE: deadline (in milliseconds) used by
    /// `ReplicationEngine::submit_with_majority_ack` when waiting for a
    /// majority of follower acks before returning success.  Defaults to
    /// `5000` (5 s).  Set to `0` to fall back to the legacy
    /// "best-effort" submit semantics (entry is appended + applied
    /// locally, no consensus wait).  Surfaced as `--cluster-submit-timeout-ms`
    /// on the binary CLI.
    #[serde(default = "default_cluster_submit_timeout_ms")]
    pub cluster_submit_timeout_ms: u64,

    /// Wave 40-A: shared cluster pre-shared key used to authenticate
    /// every cluster TCP frame (HMAC-SHA256). Either a 64-hex-char
    /// string (parsed as 32 raw bytes) or any other string (SHA-256
    /// hashed to 32 bytes). Generate with
    /// `head -c 32 /dev/urandom | xxd -p -c 64`. When `None` and
    /// [`Self::cluster_psk_required`] is `true` the binary refuses to
    /// start cluster mode.
    #[serde(default)]
    pub cluster_psk: Option<String>,

    /// Wave 40-A: when `true` (the default), the binary refuses to
    /// enter multi-node cluster mode without a [`Self::cluster_psk`]
    /// set. Operators can flip this to `false` only for development /
    /// sealed-network test rigs — production deployments must keep the
    /// default.
    #[serde(default = "default_cluster_psk_required")]
    pub cluster_psk_required: bool,
}

/// Wave 38-DE default for `DruidConfig::cluster_submit_timeout_ms`.
fn default_cluster_submit_timeout_ms() -> u64 {
    5_000
}

/// Wave 40-A default for `DruidConfig::cluster_psk_required`: PSK is
/// required for cluster mode unless the operator explicitly opts out.
fn default_cluster_psk_required() -> bool {
    true
}

/// Per-query resource limits applied during native query execution.
///
/// All limits count *intermediate* (pre-truncation) keys — the cap fires
/// while the query is still building its per-bucket map, before any
/// `threshold` / `limitSpec` truncation.  This is exactly when a
/// high-cardinality DoS becomes detectable.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryLimitsConfig {
    /// Maximum number of distinct group keys retained in-flight by a
    /// `groupBy` query, summed across all time buckets.  Defaults to
    /// `1_000_000`.  Set to `0` to disable.
    #[serde(default = "default_groupby_max_keys")]
    pub groupby_max_keys: usize,
    /// Maximum number of distinct dimension values retained in-flight by
    /// a `topN` query, summed across all time buckets, before truncation
    /// to `threshold`.  Defaults to `100_000`.  Set to `0` to disable.
    #[serde(default = "default_topn_max_inflight_threshold")]
    pub topn_max_inflight_threshold: usize,
}

fn default_groupby_max_keys() -> usize {
    1_000_000
}

fn default_topn_max_inflight_threshold() -> usize {
    100_000
}

impl Default for QueryLimitsConfig {
    fn default() -> Self {
        Self {
            groupby_max_keys: default_groupby_max_keys(),
            topn_max_inflight_threshold: default_topn_max_inflight_threshold(),
        }
    }
}

/// Concurrency-cap rate limiter configuration.
///
/// Wired into the FerroDruid REST router by `ferrodruid_rest::create_router`
/// (Wave 36-B).  When the number of in-flight requests on the gated routes
/// reaches `max_concurrent`, further requests receive `429 Too Many
/// Requests` until a slot frees.  Set `max_concurrent` to `0` to disable
/// the limiter entirely (not recommended in production).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitConfig {
    /// Maximum concurrent in-flight requests across the rate-limited
    /// routes (`/druid/v2/sql`, `/druid/v2`, `/druid/indexer/v1/task`,
    /// task submission).  Defaults to `100`.
    pub max_concurrent: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 100,
        }
    }
}

/// Authentication and authorization configuration.
///
/// Auth is **enabled by default**.  The server will refuse to bind to a
/// non-loopback address with `enabled = false` (see `validate`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthConfig {
    /// Whether authentication is enforced on every non-public route.
    ///
    /// Defaults to `true`.  Setting this to `false` while binding to a
    /// non-loopback address is rejected at startup.
    pub enabled: bool,
    /// Explicit operator opt-in to bind a non-loopback address with
    /// `enabled = false`.  Intended for sealed-network test rigs.
    /// Defaults to `false`.
    #[serde(default)]
    pub allow_insecure_public_bind: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_insecure_public_bind: false,
        }
    }
}

/// Returns `true` if `addr` resolves to a loopback / link-local-only address.
///
/// `0.0.0.0` (any IPv4) and `::` (any IPv6) are explicitly **not** loopback —
/// binding those exposes the listener to every reachable interface.
///
/// Accepts the bare host (`127.0.0.1`, `::1`, `localhost`) and the
/// `host:port` / `[ipv6]:port` socket forms operators commonly write.
#[must_use]
pub fn bind_addr_is_loopback(addr: &str) -> bool {
    // 1. Strip a bracketed `[ipv6]:port` form first.
    let host = if let Some(rest) = addr.strip_prefix('[') {
        if let Some((ip, _port)) = rest.split_once("]:") {
            ip
        } else {
            // `[::1]` with no port.
            rest.trim_end_matches(']')
        }
    } else if let Ok(ip) = addr.parse::<std::net::IpAddr>() {
        // 2. Bare IP (covers `::1` whose colons would confuse the
        //    `host:port` heuristic below).
        return ip.is_loopback();
    } else if let Some((host, _port)) = addr.rsplit_once(':') {
        // 3. Treat the suffix after the last `:` as a port for IPv4 / hostname.
        host
    } else {
        addr
    };

    if host.is_empty() {
        return false;
    }
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return ip.is_loopback();
    }
    matches!(host, "localhost")
}

impl DruidConfig {
    /// Validate the configuration for correctness.
    ///
    /// Returns an error if any required field is missing or invalid.
    pub fn validate(&self) -> Result<()> {
        // Port range
        if self.bind_port == 0 {
            return Err(DruidError::Config("bind_port must be > 0".into()));
        }

        // Metadata storage
        match self.metadata_storage.driver {
            MetadataDriver::Sqlite => {
                if self.metadata_storage.uri.is_empty() {
                    return Err(DruidError::Config("metadata URI required".into()));
                }
            }
            MetadataDriver::Postgres => {
                if !self.metadata_storage.uri.starts_with("postgres") {
                    return Err(DruidError::Config(
                        "invalid metadata URI scheme for Postgres".into(),
                    ));
                }
            }
            MetadataDriver::Mysql => {
                if !self.metadata_storage.uri.starts_with("mysql") {
                    return Err(DruidError::Config(
                        "invalid metadata URI scheme for MySQL".into(),
                    ));
                }
            }
        }

        // Deep storage
        if self.deep_storage.typ == DeepStorageType::S3 && self.deep_storage.s3_bucket.is_none() {
            return Err(DruidError::Config(
                "s3_bucket required for S3 deep storage".into(),
            ));
        }

        // Refuse to bind a public address with auth disabled unless the
        // operator has explicitly opted in.  This is the compat-blocker fix from
        // Wave 35 Codex DD: shipping `0.0.0.0:8888` + `auth.enabled = false`
        // is a remotely reachable open admin API.
        if !self.auth.enabled
            && !self.auth.allow_insecure_public_bind
            && !bind_addr_is_loopback(&self.bind_addr)
        {
            return Err(DruidError::Config(format!(
                "refusing to bind non-loopback address `{}` with auth disabled; \
                 enable `auth.enabled` (recommended) or set \
                 `auth.allowInsecurePublicBind = true` to override",
                self.bind_addr
            )));
        }

        Ok(())
    }

    /// Load configuration from a TOML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| DruidError::Config(format!("failed to read config file: {e}")))?;
        Self::from_toml_str(&contents)
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml_str(s: &str) -> Result<Self> {
        toml::from_str(s)
            .map_err(|e| DruidError::Config(format!("failed to parse TOML config: {e}")))
    }

    /// Load configuration from environment variables with `FERRODRUID_` prefix.
    ///
    /// Supported variables:
    /// - `FERRODRUID_BIND_ADDR` — address to bind to (default `"0.0.0.0"`)
    /// - `FERRODRUID_BIND_PORT` — port to bind to (default `8888`)
    /// - `FERRODRUID_METADATA_URI` — metadata store URI
    /// - `FERRODRUID_METADATA_DRIVER` — metadata driver (`sqlite`, `postgres`, `mysql`)
    /// - `FERRODRUID_DEEP_STORAGE_TYPE` — deep storage type (`local`, `s3`, `gcs`, `azure`)
    /// - `FERRODRUID_DEEP_STORAGE_BASE_PATH` — base path for deep storage
    /// - `FERRODRUID_S3_BUCKET` — S3 bucket name
    pub fn from_env() -> Result<Self> {
        let bind_addr =
            std::env::var("FERRODRUID_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0".to_string());
        let bind_port: u16 = std::env::var("FERRODRUID_BIND_PORT")
            .unwrap_or_else(|_| "8888".to_string())
            .parse()
            .map_err(|e| DruidError::Config(format!("invalid FERRODRUID_BIND_PORT: {e}")))?;
        let metadata_uri = std::env::var("FERRODRUID_METADATA_URI")
            .unwrap_or_else(|_| "sqlite:///var/lib/ferrodruid/metadata/ferrodruid.db".to_string());
        let metadata_driver = match std::env::var("FERRODRUID_METADATA_DRIVER")
            .unwrap_or_else(|_| "sqlite".to_string())
            .as_str()
        {
            "postgres" => MetadataDriver::Postgres,
            "mysql" => MetadataDriver::Mysql,
            _ => MetadataDriver::Sqlite,
        };
        let deep_storage_type = match std::env::var("FERRODRUID_DEEP_STORAGE_TYPE")
            .unwrap_or_else(|_| "local".to_string())
            .as_str()
        {
            "s3" => DeepStorageType::S3,
            "gcs" => DeepStorageType::Gcs,
            "azure" => DeepStorageType::Azure,
            _ => DeepStorageType::Local,
        };
        let deep_storage_base_path = std::env::var("FERRODRUID_DEEP_STORAGE_BASE_PATH")
            .unwrap_or_else(|_| "/var/lib/ferrodruid/deep-storage".to_string());
        let s3_bucket = std::env::var("FERRODRUID_S3_BUCKET").ok();

        Ok(DruidConfig {
            mode: DeployMode::SingleBinary,
            bind_addr,
            bind_port,
            metadata_storage: MetadataStorageConfig {
                uri: metadata_uri,
                driver: metadata_driver,
            },
            deep_storage: DeepStorageConfig {
                typ: deep_storage_type,
                base_path: deep_storage_base_path,
                s3_bucket,
                s3_region: None,
                s3_prefix: None,
            },
            auth: AuthConfig::default(),
            rate_limit: RateLimitConfig::default(),
            segment_lenient_mode: false,
            strict_null_generation: false,
            query_limits: QueryLimitsConfig::default(),
            cluster_submit_timeout_ms: default_cluster_submit_timeout_ms(),
            cluster_psk: std::env::var("FERRODRUID_CLUSTER_PSK").ok(),
            cluster_psk_required: default_cluster_psk_required(),
        })
    }

    /// Returns a default configuration suitable for single-binary deployment.
    ///
    /// **Default bind address is `127.0.0.1`** (loopback), and authentication
    /// is enabled.  An operator must explicitly opt-in to public binding by
    /// setting `bind_addr` and either enabling auth or setting
    /// `auth.allow_insecure_public_bind = true`.
    pub fn default_single_binary() -> Self {
        DruidConfig {
            mode: DeployMode::SingleBinary,
            bind_addr: "127.0.0.1".into(),
            bind_port: 8888,
            metadata_storage: MetadataStorageConfig {
                uri: "sqlite:///var/lib/ferrodruid/metadata/ferrodruid.db".into(),
                driver: MetadataDriver::Sqlite,
            },
            deep_storage: DeepStorageConfig {
                typ: DeepStorageType::Local,
                base_path: "/var/lib/ferrodruid/deep-storage".into(),
                s3_bucket: None,
                s3_region: None,
                s3_prefix: None,
            },
            auth: AuthConfig::default(),
            rate_limit: RateLimitConfig::default(),
            segment_lenient_mode: false,
            strict_null_generation: false,
            query_limits: QueryLimitsConfig::default(),
            cluster_submit_timeout_ms: default_cluster_submit_timeout_ms(),
            cluster_psk: None,
            cluster_psk_required: default_cluster_psk_required(),
        }
    }
}

/// Deployment mode for a FerroDruid cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeployMode {
    /// All services in a single process.
    SingleBinary,
    /// Simplified two-server deployment (data + query).
    Simplified,
    /// Classic multi-process Druid deployment.
    ClassicDruid,
}

/// Configuration for the metadata store backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataStorageConfig {
    /// JDBC-style connection URI.
    pub uri: String,
    /// Database driver.
    pub driver: MetadataDriver,
}

/// Supported metadata store drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetadataDriver {
    /// PostgreSQL.
    Postgres,
    /// MySQL / MariaDB.
    Mysql,
    /// SQLite (single-node / testing).
    Sqlite,
}

/// Configuration for deep (segment) storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepStorageConfig {
    /// Storage backend type.
    #[serde(rename = "type")]
    pub typ: DeepStorageType,
    /// Base path or prefix for segment files.
    pub base_path: String,
    /// S3 bucket name (required when `typ` is `S3`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_bucket: Option<String>,
    /// S3 region (defaults to `"us-east-1"` when absent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_region: Option<String>,
    /// S3 key prefix for segment objects (defaults to `"segments/"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_prefix: Option<String>,
}

/// Supported deep storage backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeepStorageType {
    /// Local filesystem.
    Local,
    /// Amazon S3 (or S3-compatible).
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    Azure,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_config() {
        let cfg = DruidConfig {
            mode: DeployMode::SingleBinary,
            bind_addr: "0.0.0.0".into(),
            bind_port: 8888,
            metadata_storage: MetadataStorageConfig {
                uri: "sqlite:///tmp/ferrodruid.db".into(),
                driver: MetadataDriver::Sqlite,
            },
            deep_storage: DeepStorageConfig {
                typ: DeepStorageType::Local,
                base_path: "/tmp/segments".into(),
                s3_bucket: None,
                s3_region: None,
                s3_prefix: None,
            },
            auth: AuthConfig::default(),
            rate_limit: RateLimitConfig::default(),
            segment_lenient_mode: false,
            strict_null_generation: false,
            query_limits: QueryLimitsConfig::default(),
            cluster_submit_timeout_ms: default_cluster_submit_timeout_ms(),
            cluster_psk: None,
            cluster_psk_required: default_cluster_psk_required(),
        };
        let json = serde_json::to_string(&cfg).expect("serialize");
        let back: DruidConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.mode, DeployMode::SingleBinary);
        assert_eq!(back.bind_port, 8888);
        assert!(back.auth.enabled, "auth must default to enabled");
    }

    #[test]
    fn refuses_public_bind_without_auth() {
        let mut cfg = DruidConfig::default_single_binary();
        cfg.bind_addr = "0.0.0.0".into();
        cfg.auth.enabled = false;
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("refusing to bind") && msg.contains("0.0.0.0"),
            "expected refusal message, got: {msg}"
        );
    }

    #[test]
    fn allows_loopback_bind_without_auth() {
        let mut cfg = DruidConfig::default_single_binary();
        cfg.bind_addr = "127.0.0.1".into();
        cfg.auth.enabled = false;
        cfg.validate().expect("loopback + auth-off is allowed");
    }

    #[test]
    fn allows_public_bind_with_explicit_override() {
        let mut cfg = DruidConfig::default_single_binary();
        cfg.bind_addr = "0.0.0.0".into();
        cfg.auth.enabled = false;
        cfg.auth.allow_insecure_public_bind = true;
        cfg.validate().expect("explicit override is honoured");
    }

    #[test]
    fn loopback_detection_handles_common_forms() {
        assert!(bind_addr_is_loopback("127.0.0.1"));
        assert!(bind_addr_is_loopback("127.0.0.1:8080"));
        assert!(bind_addr_is_loopback("localhost"));
        assert!(bind_addr_is_loopback("::1"));
        assert!(bind_addr_is_loopback("[::1]:8080"));
        assert!(!bind_addr_is_loopback("0.0.0.0"));
        assert!(!bind_addr_is_loopback("0.0.0.0:8888"));
        assert!(!bind_addr_is_loopback("10.0.0.5"));
        assert!(!bind_addr_is_loopback("::"));
    }

    #[test]
    fn validate_valid_config() {
        let cfg = DruidConfig::default_single_binary();
        cfg.validate().expect("valid config");
    }

    #[test]
    fn validate_invalid_port_zero() {
        let mut cfg = DruidConfig::default_single_binary();
        cfg.bind_port = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("bind_port"));
    }

    #[test]
    fn validate_missing_sqlite_uri() {
        let mut cfg = DruidConfig::default_single_binary();
        cfg.metadata_storage.uri = String::new();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("metadata URI required"));
    }

    #[test]
    fn validate_invalid_postgres_uri() {
        let mut cfg = DruidConfig::default_single_binary();
        cfg.metadata_storage.driver = MetadataDriver::Postgres;
        cfg.metadata_storage.uri = "sqlite:///tmp/bad".into();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("invalid metadata URI scheme"));
    }

    #[test]
    fn validate_missing_s3_bucket() {
        let mut cfg = DruidConfig::default_single_binary();
        cfg.deep_storage.typ = DeepStorageType::S3;
        cfg.deep_storage.s3_bucket = None;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("s3_bucket required"));
    }

    #[test]
    fn validate_s3_with_bucket_ok() {
        let mut cfg = DruidConfig::default_single_binary();
        cfg.deep_storage.typ = DeepStorageType::S3;
        cfg.deep_storage.s3_bucket = Some("my-bucket".into());
        cfg.validate().expect("s3 with bucket is valid");
    }

    #[test]
    fn from_toml_str_valid() {
        let toml = r#"
            mode = "singleBinary"
            bindAddr = "0.0.0.0"
            bindPort = 9999

            [metadataStorage]
            uri = "sqlite:///tmp/test.db"
            driver = "sqlite"

            [deepStorage]
            type = "local"
            basePath = "/tmp/deep"
        "#;
        let cfg = DruidConfig::from_toml_str(toml).expect("parse toml");
        assert_eq!(cfg.bind_port, 9999);
        assert_eq!(cfg.deep_storage.typ, DeepStorageType::Local);
    }

    #[test]
    fn strict_null_generation_defaults_off_and_parses() {
        // Default off: the legacy-null-generation detection warns instead of
        // failing unless the operator explicitly opts in.
        assert!(!DruidConfig::default_single_binary().strict_null_generation);

        let toml = r#"
            mode = "singleBinary"
            bindAddr = "127.0.0.1"
            bindPort = 8888
            strictNullGeneration = true

            [metadataStorage]
            uri = "sqlite:///tmp/test.db"
            driver = "sqlite"

            [deepStorage]
            type = "local"
            basePath = "/tmp/deep"
        "#;
        let cfg = DruidConfig::from_toml_str(toml).expect("parse toml");
        assert!(cfg.strict_null_generation, "TOML opt-in must be honoured");

        // Absent from TOML -> serde default (off).
        let toml_absent = toml.replace("strictNullGeneration = true\n", "");
        let cfg = DruidConfig::from_toml_str(&toml_absent).expect("parse toml");
        assert!(!cfg.strict_null_generation);
    }

    #[test]
    fn from_toml_str_invalid() {
        let toml = "this is not valid toml [[[";
        let err = DruidConfig::from_toml_str(toml).unwrap_err();
        assert!(err.to_string().contains("TOML"));
    }

    #[test]
    fn default_single_binary_is_valid() {
        let cfg = DruidConfig::default_single_binary();
        cfg.validate().expect("default config is valid");
        assert_eq!(cfg.mode, DeployMode::SingleBinary);
        assert_eq!(cfg.bind_port, 8888);
    }

    // -----------------------------------------------------------------------
    // Wave 48 — proptest hardening (config TOML round-trip)
    //
    // * `prop_config_toml_roundtrip` — for any DruidConfig built from
    //   bounded random fields, serialize to TOML and parse back must
    //   yield an equal-shaped config.
    // -----------------------------------------------------------------------
    mod proptests {
        use super::super::*;
        use proptest::prelude::*;

        proptest! {
            /// Property: a config built from bounded random fields must
            /// survive a `toml::to_string` -> `from_toml_str` round-trip
            /// with all key fields preserved.
            #[test]
            fn prop_config_toml_roundtrip(
                bind_port in 1u16..=u16::MAX,
                base_path in r"/[a-z0-9/_-]{1,32}",
                max_concurrent in 0usize..1024,
                groupby_max_keys in 0usize..1_000_000,
                topn_threshold in 0usize..1_000_000,
                cluster_timeout in 0u64..600_000,
            ) {
                let cfg = DruidConfig {
                    mode: DeployMode::SingleBinary,
                    bind_addr: "127.0.0.1".into(),
                    bind_port,
                    metadata_storage: MetadataStorageConfig {
                        uri: "sqlite:///tmp/x.db".into(),
                        driver: MetadataDriver::Sqlite,
                    },
                    deep_storage: DeepStorageConfig {
                        typ: DeepStorageType::Local,
                        base_path: base_path.clone(),
                        s3_bucket: None,
                        s3_region: None,
                        s3_prefix: None,
                    },
                    auth: AuthConfig::default(),
                    rate_limit: RateLimitConfig { max_concurrent },
                    segment_lenient_mode: false,
            strict_null_generation: false,
                    query_limits: QueryLimitsConfig {
                        groupby_max_keys,
                        topn_max_inflight_threshold: topn_threshold,
                    },
                    cluster_submit_timeout_ms: cluster_timeout,
                    cluster_psk: None,
                    cluster_psk_required: true,
                };
                let serialized = toml::to_string(&cfg).expect("serialize TOML");
                let back = DruidConfig::from_toml_str(&serialized).expect("parse TOML");
                prop_assert_eq!(back.bind_port, bind_port);
                prop_assert_eq!(back.deep_storage.base_path, base_path);
                prop_assert_eq!(back.rate_limit.max_concurrent, max_concurrent);
                prop_assert_eq!(back.query_limits.groupby_max_keys, groupby_max_keys);
                prop_assert_eq!(back.query_limits.topn_max_inflight_threshold, topn_threshold);
                prop_assert_eq!(back.cluster_submit_timeout_ms, cluster_timeout);
            }
        }
    }
}
