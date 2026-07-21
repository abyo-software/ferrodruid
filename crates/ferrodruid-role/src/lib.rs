// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Role-split scaffold for FerroDruid v1.0.
//!
//! Wave 34.T introduces a thin role-dispatch layer in preparation for the
//! v1.0 multi-process topology. Today (v0.1.x) every FerroDruid deployment
//! runs as a single binary that hosts the broker, historical, and
//! coordinator services in one process. The roadmap (see
//! `docs/v1.0-roadmap.md`) splits these responsibilities into three
//! separate processes (and eventually six, mirroring the Apache Druid
//! topology of broker/historical/coordinator/router/overlord/middleManager).
//!
//! This crate is intentionally minimal: it exposes a [`Role`] enum, a
//! [`RoleConfig`] struct describing the bind/data/peer surface every role
//! shares, and a stable parser/formatter so that the existing
//! `ferrodruid serve --mode classic --role <r>` flag and the new
//! per-role binaries (`ferrodruid-broker`, `ferrodruid-historical`,
//! `ferrodruid-coordinator`, `ferrodruid-router`, `ferrodruid-overlord`,
//! `ferrodruid-middlemanager`) agree on a single source of truth.
//!
//! Wave 38.FF (this commit) adds the remaining three Apache Druid roles
//! (router / overlord / middleManager) so the [`Role`] enum now covers
//! the complete 6-role topology. The cross-role wire (router→broker
//! selection, overlord→middleManager dispatch) is still a stub — Wave
//! 39+ will land the real HTTP/gRPC contracts. See
//! `docs/v1.0-roadmap.md` W3.
//!
//! What is NOT here yet (tracked as Wave 39+ work):
//! - Inter-role wire protocol negotiation (broker→historical scatter,
//!   coordinator→historical load/drop, router→broker selection,
//!   overlord→middleManager task assignment) — today the single-binary
//!   path uses in-process `Arc` handles; the role-split path will need
//!   an HTTP/gRPC contract.
//! - Per-role state machine subsets — every role still pulls in the
//!   full crate graph until a future wave feature-gates it.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::fmt;
use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Logical role this FerroDruid process will host.
///
/// In v0.1.x, only [`Role::Standalone`] runs end-to-end; the three
/// dedicated roles boot, log their identity, and report `not yet
/// implemented` for any cross-role traffic that has not yet been
/// implemented in the role-split path. The single-binary path
/// (everything in one process) remains the supported production
/// posture until v1.0 lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Role {
    /// Query routing tier: receives client SQL/native queries and
    /// dispatches them to historical (and, in future waves, realtime)
    /// nodes.
    Broker,
    /// Segment serving tier: loads immutable segments from deep
    /// storage and answers per-segment query fragments forwarded by
    /// brokers.
    Historical,
    /// Cluster metadata + segment assignment + balancing for query
    /// state. In Druid terms, this is the segment-side "Master" tier;
    /// the indexing-side master is [`Role::Overlord`].
    Coordinator,
    /// HTTP front-end / load-balancer tier: rewrites incoming client
    /// requests and selects an appropriate broker by tier. Sits in
    /// front of brokers in large clusters; small deployments skip it.
    /// Wave 38.FF scaffold — cross-role wire is W3 work.
    Router,
    /// Indexing service master: receives ingestion task specs from
    /// clients and assigns them to middleManagers. Plays the same
    /// "master" role for the indexing side that [`Role::Coordinator`]
    /// plays for the query side.
    Overlord,
    /// Indexing service worker: hosts peon processes that execute
    /// batch + streaming ingestion tasks assigned by the overlord.
    MiddleManager,
    /// Single-process all-in-one mode (today's default — every role
    /// in one binary, in-process Arc wiring).
    Standalone,
}

impl Role {
    /// Returns the canonical kebab-case label used on the CLI and in
    /// log lines (e.g. `--role broker`, `role=historical`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Broker => "broker",
            Self::Historical => "historical",
            Self::Coordinator => "coordinator",
            Self::Router => "router",
            Self::Overlord => "overlord",
            Self::MiddleManager => "middlemanager",
            Self::Standalone => "standalone",
        }
    }

    /// Whether this role currently runs end-to-end. Only
    /// [`Role::Standalone`] returns `true` in v0.1.x; the dedicated
    /// roles boot and log but do not yet participate in inter-role
    /// traffic.
    #[must_use]
    pub const fn is_implemented_end_to_end(self) -> bool {
        matches!(self, Self::Standalone)
    }

    /// Apache Druid default port for this role. Mirrors the upstream
    /// Druid documentation so an operator migrating from Druid sees
    /// familiar numbers. Returns `None` for [`Role::Standalone`]
    /// because the all-in-one binary uses its own `--port` flag and
    /// has no canonical Druid analogue.
    ///
    /// | role           | port |
    /// |----------------|------|
    /// | coordinator    | 8081 |
    /// | broker         | 8082 |
    /// | historical     | 8083 |
    /// | overlord       | 8090 |
    /// | middlemanager  | 8091 |
    /// | router         | 8888 |
    #[must_use]
    pub const fn druid_default_port(self) -> Option<u16> {
        match self {
            Self::Coordinator => Some(8081),
            Self::Broker => Some(8082),
            Self::Historical => Some(8083),
            Self::Overlord => Some(8090),
            Self::MiddleManager => Some(8091),
            Self::Router => Some(8888),
            Self::Standalone => None,
        }
    }

    /// Whether this role belongs to the Druid "data" tier (services
    /// that hold or ingest segment data on local storage).
    #[must_use]
    pub const fn is_data_tier(self) -> bool {
        matches!(self, Self::Historical | Self::MiddleManager)
    }

    /// Whether this role belongs to the Druid "query" tier (services
    /// that route or answer client queries).
    #[must_use]
    pub const fn is_query_tier(self) -> bool {
        matches!(self, Self::Broker | Self::Router)
    }

    /// Whether this role belongs to the Druid "master" tier (services
    /// that own cluster metadata and assignment decisions).
    #[must_use]
    pub const fn is_master_tier(self) -> bool {
        matches!(self, Self::Coordinator | Self::Overlord)
    }

    /// All variants in declaration order. Useful for tests that want
    /// to round-trip every role through the parser.
    #[must_use]
    pub const fn all() -> [Self; 7] {
        [
            Self::Broker,
            Self::Historical,
            Self::Coordinator,
            Self::Router,
            Self::Overlord,
            Self::MiddleManager,
            Self::Standalone,
        ]
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Role {
    type Err = RoleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "broker" => Ok(Self::Broker),
            "historical" => Ok(Self::Historical),
            "coordinator" => Ok(Self::Coordinator),
            "router" => Ok(Self::Router),
            "overlord" => Ok(Self::Overlord),
            // Accept both the kebab-case canonical form and the
            // legacy single-token Druid spelling so operators
            // migrating from Druid configs do not have to touch
            // every flag.
            "middlemanager" | "middle-manager" => Ok(Self::MiddleManager),
            "standalone" | "single-binary" | "all-in-one" => Ok(Self::Standalone),
            other => Err(RoleError::Unknown(other.to_string())),
        }
    }
}

/// Errors raised by the role-dispatch scaffold.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RoleError {
    /// The string passed to [`Role::from_str`] did not match any known
    /// kebab-case role label.
    #[error(
        "unknown role `{0}` (expected: broker, historical, coordinator, router, overlord, \
         middlemanager, standalone)"
    )]
    Unknown(String),
    /// The bind address could not be parsed as an `IpAddr`. Surfaced
    /// up so the binary entry point can refuse to start instead of
    /// silently defaulting.
    #[error("invalid bind address `{0}`: {1}")]
    InvalidBind(String, String),
}

/// Shared launch parameters every role consumes. Today this is a
/// minimal surface — the per-role binaries simply log it and return.
/// As Wave 35+ wires real inter-role traffic, additional fields
/// (peer URIs, deep-storage backends, auth handles) will land here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleConfig {
    /// Logical role this process will host.
    pub role: Role,
    /// Address the role's HTTP service binds to (e.g. `127.0.0.1`).
    pub bind: IpAddr,
    /// TCP port the role's HTTP service binds to.
    pub port: u16,
    /// Filesystem location for any role-local state (segments cache,
    /// metadata SQLite, deep-storage root). Each role binary creates
    /// it lazily; a non-existent path is not an error here.
    pub data_dir: PathBuf,
}

impl RoleConfig {
    /// Construct a [`RoleConfig`], validating the bind address.
    ///
    /// # Errors
    ///
    /// Returns [`RoleError::InvalidBind`] if `bind` does not parse as
    /// an [`IpAddr`].
    pub fn try_new(
        role: Role,
        bind: &str,
        port: u16,
        data_dir: PathBuf,
    ) -> Result<Self, RoleError> {
        let bind_addr: IpAddr = bind.parse().map_err(|e: std::net::AddrParseError| {
            RoleError::InvalidBind(bind.to_string(), e.to_string())
        })?;
        Ok(Self {
            role,
            bind: bind_addr,
            port,
            data_dir,
        })
    }

    /// Render a one-line banner the binary emits at start-up. Kept in
    /// the library so unit tests can assert on the exact wording.
    #[must_use]
    pub fn banner(&self) -> String {
        format!(
            "FerroDruid v0.1.x role-split scaffold | role={} bind={}:{} data_dir={} \
             implemented_end_to_end={}",
            self.role,
            self.bind,
            self.port,
            self.data_dir.display(),
            self.role.is_implemented_end_to_end(),
        )
    }
}

/// Outcome of a role-dispatch dry-run. The Wave 34.T scaffold uses
/// this to keep the per-role binary entry points pure functions that
/// are unit-testable: the real `tokio::main` wrappers consume a
/// [`DispatchOutcome`] and only spawn long-lived tasks if it asks
/// them to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOutcome {
    /// Continue into the existing single-binary boot sequence.
    /// Emitted only for [`Role::Standalone`].
    LaunchSingleBinary,
    /// Print the per-role boot banner and exit success. Emitted for
    /// the three dedicated roles in v0.1.x — the cross-role wire is
    /// not yet implemented.
    LogAndExitOk,
}

/// Decide what the binary should do given the role the operator
/// requested. Pure function — no I/O — so unit tests can exercise
/// every variant without spinning up a runtime.
#[must_use]
pub fn dispatch(role: Role) -> DispatchOutcome {
    match role {
        Role::Standalone => DispatchOutcome::LaunchSingleBinary,
        Role::Broker
        | Role::Historical
        | Role::Coordinator
        | Role::Router
        | Role::Overlord
        | Role::MiddleManager => DispatchOutcome::LogAndExitOk,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_round_trips_through_string() {
        for role in Role::all() {
            let s = role.as_str().to_string();
            let parsed: Role = s.parse().expect("known role round-trips");
            assert_eq!(parsed, role);
        }
    }

    #[test]
    fn role_accepts_legacy_aliases() {
        assert_eq!("single-binary".parse::<Role>().unwrap(), Role::Standalone);
        assert_eq!("all-in-one".parse::<Role>().unwrap(), Role::Standalone);
        // Druid's documentation uses both spellings interchangeably,
        // so the parser accepts both.
        assert_eq!(
            "middle-manager".parse::<Role>().unwrap(),
            Role::MiddleManager
        );
        assert_eq!(
            "middlemanager".parse::<Role>().unwrap(),
            Role::MiddleManager
        );
    }

    #[test]
    fn role_rejects_unknown_string() {
        // After Wave 38.FF, "router" / "overlord" / "middlemanager"
        // are valid; pick a string that is genuinely unknown so the
        // error path stays covered.
        let err = "peon".parse::<Role>().unwrap_err();
        assert!(matches!(err, RoleError::Unknown(s) if s == "peon"));
    }

    #[test]
    fn six_role_topology_strings_round_trip() {
        // Every Druid role parses back to its enum variant and
        // formats back to the same string. This is the contract the
        // per-role binaries depend on.
        let pairs = [
            ("broker", Role::Broker),
            ("historical", Role::Historical),
            ("coordinator", Role::Coordinator),
            ("router", Role::Router),
            ("overlord", Role::Overlord),
            ("middlemanager", Role::MiddleManager),
        ];
        for (s, expected) in pairs {
            let parsed: Role = s.parse().expect("known role parses");
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_str(), s);
        }
    }

    #[test]
    fn druid_default_ports_match_documented_table() {
        assert_eq!(Role::Coordinator.druid_default_port(), Some(8081));
        assert_eq!(Role::Broker.druid_default_port(), Some(8082));
        assert_eq!(Role::Historical.druid_default_port(), Some(8083));
        assert_eq!(Role::Overlord.druid_default_port(), Some(8090));
        assert_eq!(Role::MiddleManager.druid_default_port(), Some(8091));
        assert_eq!(Role::Router.druid_default_port(), Some(8888));
        assert_eq!(Role::Standalone.druid_default_port(), None);
    }

    #[test]
    fn tier_predicates_partition_dedicated_roles_correctly() {
        // Each dedicated role lives in exactly one tier; standalone
        // lives in none (it hosts every tier in-process).
        let cases = [
            (Role::Broker, "query"),
            (Role::Router, "query"),
            (Role::Historical, "data"),
            (Role::MiddleManager, "data"),
            (Role::Coordinator, "master"),
            (Role::Overlord, "master"),
        ];
        for (role, expected_tier) in cases {
            assert_eq!(role.is_query_tier(), expected_tier == "query", "{role}");
            assert_eq!(role.is_data_tier(), expected_tier == "data", "{role}");
            assert_eq!(role.is_master_tier(), expected_tier == "master", "{role}");
        }
        assert!(!Role::Standalone.is_query_tier());
        assert!(!Role::Standalone.is_data_tier());
        assert!(!Role::Standalone.is_master_tier());
    }

    #[test]
    fn role_display_matches_as_str() {
        for role in Role::all() {
            assert_eq!(format!("{role}"), role.as_str());
        }
    }

    #[test]
    fn standalone_is_only_role_implemented_end_to_end() {
        assert!(Role::Standalone.is_implemented_end_to_end());
        for role in [
            Role::Broker,
            Role::Historical,
            Role::Coordinator,
            Role::Router,
            Role::Overlord,
            Role::MiddleManager,
        ] {
            assert!(!role.is_implemented_end_to_end(), "{role}");
        }
    }

    #[test]
    fn dispatch_routes_standalone_to_full_boot() {
        assert_eq!(
            dispatch(Role::Standalone),
            DispatchOutcome::LaunchSingleBinary
        );
    }

    #[test]
    fn dispatch_routes_dedicated_roles_to_log_and_exit() {
        for role in [
            Role::Broker,
            Role::Historical,
            Role::Coordinator,
            Role::Router,
            Role::Overlord,
            Role::MiddleManager,
        ] {
            assert_eq!(dispatch(role), DispatchOutcome::LogAndExitOk, "{role}");
        }
    }

    #[test]
    fn role_config_validates_bind_address() {
        let cfg = RoleConfig::try_new(
            Role::Broker,
            "127.0.0.1",
            8082,
            PathBuf::from("/tmp/ferrodruid-broker"),
        )
        .expect("loopback should parse");
        assert_eq!(cfg.role, Role::Broker);
        assert_eq!(cfg.port, 8082);
    }

    #[test]
    fn role_config_rejects_garbage_bind_address() {
        let err = RoleConfig::try_new(
            Role::Broker,
            "not-an-ip",
            8082,
            PathBuf::from("/tmp/ferrodruid-broker"),
        )
        .expect_err("garbage bind should fail");
        match err {
            RoleError::InvalidBind(s, _) => assert_eq!(s, "not-an-ip"),
            other => panic!("expected InvalidBind, got {other:?}"),
        }
    }

    #[test]
    fn role_config_banner_mentions_role_and_implementation_status() {
        let cfg = RoleConfig::try_new(
            Role::Historical,
            "127.0.0.1",
            8083,
            PathBuf::from("/var/lib/ferrodruid"),
        )
        .unwrap();
        let banner = cfg.banner();
        assert!(banner.contains("role=historical"), "{banner}");
        assert!(banner.contains("127.0.0.1:8083"), "{banner}");
        assert!(banner.contains("implemented_end_to_end=false"), "{banner}",);
    }

    #[test]
    fn role_config_banner_for_standalone_reports_implemented() {
        let cfg = RoleConfig::try_new(Role::Standalone, "127.0.0.1", 8888, PathBuf::from("./data"))
            .unwrap();
        assert!(
            cfg.banner().contains("implemented_end_to_end=true"),
            "{}",
            cfg.banner(),
        );
    }

    #[test]
    fn role_all_lists_every_variant_exactly_once() {
        let all = Role::all();
        // 7 distinct variants — the complete Druid 6-role topology
        // plus the Standalone all-in-one mode.
        let mut sorted: Vec<&'static str> = all.iter().map(|r| r.as_str()).collect();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            vec![
                "broker",
                "coordinator",
                "historical",
                "middlemanager",
                "overlord",
                "router",
                "standalone",
            ],
        );
    }
}
