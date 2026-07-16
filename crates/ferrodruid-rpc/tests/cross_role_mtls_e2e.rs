// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W1-I (CL-J1) — end-to-end mTLS integration tests for the four
//! cross-role HTTP wires.
//!
//! Strategy: spin up the per-role axum routers in-process on loopback
//! ports using `serve_cross_role` in `Required` mode, build a real
//! TLS-aware `reqwest::Client` for the outbound peer, and drive every
//! wire through real TLS handshakes:
//!
//! 1. **router → broker** via `HttpBrokerClient`.
//! 2. **broker → historical** via `HttpHistoricalClient` (used both by
//!    the broker for scatter AND by the coordinator for load/drop).
//! 3. **overlord → middlemanager** via `HttpMiddleManagerClient`.
//!
//! The tests also exercise the **fail-closed posture**: a client that
//! is missing a client cert (plain `reqwest::Client`) or that does not
//! trust the server CA cannot connect.
//!
//! Why in-process and not via spawned binaries: this gives a faster
//! and more deterministic test (no port races, no binary lookup) while
//! exercising the *exact same* TLS code paths the binaries take
//! (`build_cross_role_server_acceptor` → `axum_server::bind_rustls`,
//! `build_cross_role_client` → `reqwest::Client`).
//!
//! The multi-process `cargo run` validation lives in
//! `tests/integration/RESULTS_cross_role_mtls_<date>.md` evidence file
//! and is documented in `docs/SECURITY.md`.

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use ferrodruid_rpc::broker_server::{self, BrokerServerState};
use ferrodruid_rpc::historical_server::{self, HistoricalServerState};
use ferrodruid_rpc::mm_server::{self, MiddleManagerServerState};
use ferrodruid_rpc::{
    BrokerClient, CrossRoleMtlsMode, CrossRoleStartup, CrossRoleTlsConfig, HistoricalClient,
    HttpBrokerClient, HttpHistoricalClient, HttpMiddleManagerClient, MiddleManagerClient,
    SegmentLoadCommand, SegmentLoadState, SegmentQuery, SqlQuery, TaskAssignment, TaskKind,
    build_cross_role_client, serve_cross_role,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Test cert factory — one CA shared across every role.
// ---------------------------------------------------------------------------

/// A test cert bundle: shared CA + one leaf per role name. The
/// [`TempDir`] field keeps the on-disk PEM files alive for the
/// duration of the test.
struct TestCertBundle {
    _dir: TempDir,
    ca_cert: rcgen::Certificate,
    ca_key: rcgen::KeyPair,
    /// Per-role leaf bundle (`leaf.pem`, `leaf.key`, `ca.pem`) the
    /// real binary would load via `load_from_dir`.
    leaves: std::collections::HashMap<&'static str, CrossRoleTlsConfig>,
}

fn issue_test_bundle(roles: &[&'static str]) -> TestCertBundle {
    let dir = tempfile::tempdir().expect("tempdir");
    let ca_key = rcgen::KeyPair::generate().expect("ca keypair");
    let mut ca_params = rcgen::CertificateParams::new(vec!["ferrodruid-cross-role-e2e-ca".into()])
        .expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca self-sign");
    let ca_pem = ca_cert.pem();

    let mut leaves = std::collections::HashMap::new();
    for role in roles {
        let role_dir = dir.path().join(role);
        fs::create_dir_all(&role_dir).expect("mkdir");
        let leaf_key = rcgen::KeyPair::generate().expect("leaf keypair");
        let leaf_params = rcgen::CertificateParams::new(vec![
            (*role).to_string(),
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .expect("leaf params");
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &ca_cert, &ca_key)
            .expect("leaf sign");

        let ca_path = role_dir.join("ca.pem");
        let cert_path = role_dir.join("leaf.pem");
        let key_path = role_dir.join("leaf.key");
        fs::write(&ca_path, &ca_pem).expect("write ca");
        fs::write(&cert_path, leaf_cert.pem()).expect("write leaf");
        fs::write(&key_path, leaf_key.serialize_pem()).expect("write key");
        leaves.insert(*role, CrossRoleTlsConfig::new(cert_path, key_path, ca_path));
    }

    TestCertBundle {
        _dir: dir,
        ca_cert,
        ca_key,
        leaves,
    }
}

impl TestCertBundle {
    fn leaf(&self, role: &'static str) -> &CrossRoleTlsConfig {
        self.leaves.get(role).expect("role must be in bundle")
    }

    /// Issue a one-off leaf cert that is NOT signed by this bundle's
    /// CA. Used by the "untrusted CA" negative test.
    fn untrusted_leaf(&self) -> (TempDir, CrossRoleTlsConfig) {
        let dir = tempfile::tempdir().expect("tempdir");
        let rogue_ca_key = rcgen::KeyPair::generate().expect("rogue ca keypair");
        let mut rogue_ca_params =
            rcgen::CertificateParams::new(vec!["rogue-ca".into()]).expect("rogue ca params");
        rogue_ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let rogue_ca_cert = rogue_ca_params
            .self_signed(&rogue_ca_key)
            .expect("rogue ca self-sign");

        let leaf_key = rcgen::KeyPair::generate().expect("leaf keypair");
        let leaf_params = rcgen::CertificateParams::new(vec![
            "rogue-leaf".to_string(),
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .expect("rogue leaf params");
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &rogue_ca_cert, &rogue_ca_key)
            .expect("rogue leaf sign");

        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("leaf.pem");
        let key_path = dir.path().join("leaf.key");
        // Point the rogue client at the LEGIT bundle's CA so it
        // accepts the server, but it presents the ROGUE leaf so the
        // server rejects the client cert.
        fs::write(&ca_path, self.ca_cert.pem()).expect("write ca");
        fs::write(&cert_path, leaf_cert.pem()).expect("write leaf");
        fs::write(&key_path, leaf_key.serialize_pem()).expect("write key");
        // Silence "unused" on the unused-but-needed fields.
        let _ = (&self.ca_key, &self.ca_cert);
        (dir, CrossRoleTlsConfig::new(cert_path, key_path, ca_path))
    }
}

// ---------------------------------------------------------------------------
// Listener bring-up helpers — wrap `serve_cross_role` in a tokio::spawn
// so the test can drive the running server.
// ---------------------------------------------------------------------------

async fn spawn_role_tls<F>(cfg: &CrossRoleTlsConfig, build_app: F) -> SocketAddr
where
    F: FnOnce() -> axum::Router,
{
    // Pick an ephemeral port for the TLS listener.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe bind");
    let addr = probe.local_addr().expect("addr");
    drop(probe);

    let app = build_app();
    // Reuse the same path the per-role binary takes: build a
    // CrossRoleStartup, then into_listeners().
    let startup = CrossRoleStartup::resolve(
        CrossRoleMtlsMode::Required,
        SocketAddr::from(([127, 0, 0, 1], 0)), // legacy bind unused in Required
        Some(addr),
        Some(cfg.cert_path.clone()),
        Some(cfg.key_path.clone()),
        Some(cfg.ca_path.clone()),
        None,
    )
    .expect("startup resolve");
    let (mode, plain, tls) = startup.into_listeners().expect("listeners");
    tokio::spawn(async move {
        let _ = serve_cross_role(app, mode, plain, tls).await;
    });
    // Give the listener a moment to bind.
    wait_for_tls_listener(addr).await;
    addr
}

async fn wait_for_tls_listener(addr: SocketAddr) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            // Connection succeeded — the listener is bound. The TLS
            // handshake itself will happen on the first real client
            // dial; we don't care to drive it here.
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("listener at {addr} never bound");
}

// ---------------------------------------------------------------------------
// Tests — happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn router_to_broker_over_mtls_round_trips_info() {
    let bundle = issue_test_bundle(&["broker", "router"]);
    let broker_addr = spawn_role_tls(bundle.leaf("broker"), || {
        broker_server::router(BrokerServerState {
            broker_id: "mtls-broker".into(),
            tier: "default".into(),
            version: "test".into(),
        })
    })
    .await;

    let router_outbound = build_cross_role_client(bundle.leaf("router")).expect("router client");
    let client = HttpBrokerClient::with_client(format!("https://{broker_addr}"), router_outbound);

    let info = client.info().await.expect("info round-trip");
    assert_eq!(info.role, "broker");
    assert_eq!(info.broker_id, "mtls-broker");
}

#[tokio::test]
async fn broker_to_historical_over_mtls_round_trips_scatter() {
    let bundle = issue_test_bundle(&["broker", "historical"]);
    let hist_addr = spawn_role_tls(bundle.leaf("historical"), || {
        historical_server::router(Arc::new(HistoricalServerState::with_config(
            "mtls-hist".to_string(),
            "default".to_string(),
            Duration::from_millis(5),
        )))
    })
    .await;

    let broker_outbound = build_cross_role_client(bundle.leaf("broker")).expect("broker client");
    let client = HttpHistoricalClient::with_client(format!("https://{hist_addr}"), broker_outbound);

    let resp = client
        .scatter_query(SegmentQuery::new("SELECT 'mtls'", "seg-mtls"))
        .await
        .expect("scatter over mTLS");
    assert_eq!(resp.segment_id, "seg-mtls");
}

#[tokio::test]
async fn coordinator_to_historical_over_mtls_round_trips_load() {
    let bundle = issue_test_bundle(&["coordinator", "historical"]);
    let hist_addr = spawn_role_tls(bundle.leaf("historical"), || {
        historical_server::router(Arc::new(HistoricalServerState::with_config(
            "mtls-hist".to_string(),
            "default".to_string(),
            Duration::from_millis(5),
        )))
    })
    .await;

    let coord_outbound =
        build_cross_role_client(bundle.leaf("coordinator")).expect("coordinator client");
    let client = HttpHistoricalClient::with_client(format!("https://{hist_addr}"), coord_outbound);

    let report = client
        .load_segment(SegmentLoadCommand::new(
            "seg-load-mtls",
            "ds-mtls",
            "deepstore://mtls/seg",
        ))
        .await
        .expect("load over mTLS");
    assert_eq!(report.state, SegmentLoadState::Loading);
}

#[tokio::test]
async fn overlord_to_middlemanager_over_mtls_round_trips_assign() {
    let bundle = issue_test_bundle(&["overlord", "middlemanager"]);
    let mm_addr = spawn_role_tls(bundle.leaf("middlemanager"), || {
        mm_server::router(Arc::new(MiddleManagerServerState::with_timings(
            Duration::from_millis(5),
            Duration::from_millis(5),
        )))
    })
    .await;

    let overlord_outbound =
        build_cross_role_client(bundle.leaf("overlord")).expect("overlord client");
    let client =
        HttpMiddleManagerClient::with_client(format!("https://{mm_addr}"), overlord_outbound);

    let task = TaskAssignment::new(TaskKind::Index, "ds-mtls");
    let status = client.assign_task(task).await.expect("assign over mTLS");
    assert!(matches!(
        status.state,
        ferrodruid_rpc::TaskState::Pending | ferrodruid_rpc::TaskState::Running
    ));
}

// ---------------------------------------------------------------------------
// Tests — fail-closed posture
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plain_client_cannot_connect_to_required_tls_listener() {
    let bundle = issue_test_bundle(&["broker"]);
    let broker_addr = spawn_role_tls(bundle.leaf("broker"), || {
        broker_server::router(BrokerServerState::default())
    })
    .await;

    // A plain (no client cert, no CA trust override) reqwest::Client
    // cannot connect — it does not present a client cert AND it does
    // not trust the bundle's self-signed CA.
    let plain = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("plain client");
    let client = HttpBrokerClient::with_client(format!("https://{broker_addr}"), plain);
    let err = client
        .info()
        .await
        .expect_err("plain client must be rejected");
    // It does not matter precisely which error variant we get — the
    // important assertion is that a request without client cert + CA
    // trust does NOT succeed against a Required-mode listener.
    eprintln!("plain client rejected with: {err:?}");
}

#[tokio::test]
async fn client_with_untrusted_leaf_is_rejected_at_handshake() {
    let bundle = issue_test_bundle(&["broker"]);
    let broker_addr = spawn_role_tls(bundle.leaf("broker"), || {
        broker_server::router(BrokerServerState::default())
    })
    .await;

    // Build a client whose leaf cert chains to a *different* CA than
    // the server trusts. The client still trusts the legit CA (so it
    // accepts the server cert), but the server rejects the client cert
    // because it does not chain to the bundle CA.
    let (_rogue_dir, rogue_cfg) = bundle.untrusted_leaf();
    let rogue_client =
        build_cross_role_client(&rogue_cfg).expect("client builds despite untrusted leaf");
    let client = HttpBrokerClient::with_client(format!("https://{broker_addr}"), rogue_client);
    let err = client
        .info()
        .await
        .expect_err("untrusted leaf must be rejected");
    eprintln!("untrusted leaf rejected with: {err:?}");
}

// ---------------------------------------------------------------------------
// Tests — Disabled mode + Permissive mode sanity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn disabled_mode_serves_plain_http_unchanged() {
    // Disabled mode binds plain HTTP only. We use serve_cross_role
    // directly to exercise the plain-HTTP serve path.
    let app = broker_server::router(BrokerServerState::default());
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe bind");
    let addr = probe.local_addr().expect("addr");
    drop(probe);

    let startup = CrossRoleStartup::resolve(
        CrossRoleMtlsMode::Disabled,
        addr,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("resolve");
    let (mode, plain, tls) = startup.into_listeners().expect("listeners");
    tokio::spawn(async move {
        let _ = serve_cross_role(app, mode, plain, tls).await;
    });
    wait_for_tls_listener(addr).await;

    // A plain reqwest client succeeds.
    let plain = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");
    let client = HttpBrokerClient::with_client(format!("http://{addr}"), plain);
    let info = client.info().await.expect("disabled-mode plain GET");
    assert_eq!(info.role, "broker");
}

#[tokio::test]
async fn permissive_mode_accepts_plain_and_tls() {
    // Permissive: bind BOTH a plain HTTP listener AND a TLS listener
    // on different ephemeral ports. Plain clients connect over the
    // plain port; mTLS-aware clients connect over the TLS port.
    let bundle = issue_test_bundle(&["broker"]);
    let plain_probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("plain probe");
    let plain_addr = plain_probe.local_addr().expect("addr");
    drop(plain_probe);
    let tls_probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("tls probe");
    let tls_addr = tls_probe.local_addr().expect("addr");
    drop(tls_probe);

    let app = broker_server::router(BrokerServerState::default());
    let cfg = bundle.leaf("broker");
    let startup = CrossRoleStartup::resolve(
        CrossRoleMtlsMode::Permissive,
        plain_addr,
        Some(tls_addr),
        Some(cfg.cert_path.clone()),
        Some(cfg.key_path.clone()),
        Some(cfg.ca_path.clone()),
        None,
    )
    .expect("permissive resolve");
    let (mode, plain, tls) = startup.into_listeners().expect("listeners");
    tokio::spawn(async move {
        let _ = serve_cross_role(app, mode, plain, tls).await;
    });
    wait_for_tls_listener(plain_addr).await;
    wait_for_tls_listener(tls_addr).await;

    // Plain client → plain port.
    let plain_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");
    let plain_broker = HttpBrokerClient::with_client(format!("http://{plain_addr}"), plain_client);
    let info_plain = plain_broker.info().await.expect("plain port reachable");
    assert_eq!(info_plain.role, "broker");

    // TLS client → TLS port.
    let tls_outbound = build_cross_role_client(cfg).expect("tls client");
    let tls_broker = HttpBrokerClient::with_client(format!("https://{tls_addr}"), tls_outbound);
    let info_tls = tls_broker.info().await.expect("tls port reachable");
    assert_eq!(info_tls.role, "broker");
}

// ---------------------------------------------------------------------------
// Smoke test — exercise the full SqlQuery type just so the test file
// pulls every public type the binaries import.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_query_smoke_for_unused_import_warning() {
    let _ = SqlQuery::new("SELECT 1");
}
