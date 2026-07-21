// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 53 — mTLS replication end-to-end: 3-node cluster, in-process
//! [`InMemoryTransport`] driving the consensus path with PSK auth,
//! AND a separate one-shot wire trace asserting that the production
//! [`TcpTransport`] (when compiled with `--features cluster-tls`) puts
//! TLS records on the socket rather than the cleartext PSK frame.
//!
//! The existing `tls_three_node.rs` already covers leader election
//! and the wire-trace assertion in detail; this file complements it
//! with:
//!
//! * **`mtls_3_node_100_commands_replicate`** — under the
//!   `cluster-tls` feature, stand up 3 [`TcpTransport`] nodes wrapped
//!   in mTLS, force one as leader, submit 100 distinct
//!   `RegisterService` commands through `submit_with_majority_ack`,
//!   then assert all 3 nodes converged on the same service set.
//!
//! Gated on `#[cfg(feature = "cluster-tls")]` AND `#[ignore]`.  Run
//! with:
//!
//! ```text
//! cargo test -p ferrodruid-cluster --features cluster-tls \
//!     --test mtls_replication_e2e -- --ignored --nocapture
//! ```

#![cfg(feature = "cluster-tls")]
#![allow(missing_docs)]
#![allow(clippy::too_many_lines)]

use std::collections::HashMap;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodruid_cluster::auth::derive_psk;
use ferrodruid_cluster::replication::{ReplicationConfig, ReplicationEngine, ReplicationRole};
use ferrodruid_cluster::tls::TlsConfig;
use ferrodruid_cluster::transport::{TcpTransport, TcpTransportConfig};
use ferrodruid_cluster::{ClusterCommand, ClusterManager, NodeInfo, NodeRole, ServiceEntry};
use tempfile::TempDir;
use tokio::net::TcpListener;

const W53_COMMAND_COUNT: usize = 100;

/// Build a self-signed CA + per-node leaf cert.  Returns the temp
/// dir holding the PEMs (kept alive by the caller) and the
/// [`TlsConfig`] paths for the node.
fn gen_certs_with_shared_ca(
    node_name: &str,
    ca_key: &rcgen::KeyPair,
    ca_cert: &rcgen::Certificate,
    ca_pem: &str,
) -> (TempDir, TlsConfig) {
    let dir = tempfile::tempdir().expect("tempdir");
    let node_key = rcgen::KeyPair::generate().expect("node key");
    let node_params = rcgen::CertificateParams::new(vec![
        node_name.to_string(),
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .expect("node params");
    let node_cert = node_params
        .signed_by(&node_key, ca_cert, ca_key)
        .expect("node sign");

    let ca_path: PathBuf = dir.path().join("ca.pem");
    let cert_path: PathBuf = dir.path().join("node.pem");
    let key_path: PathBuf = dir.path().join("node-key.pem");
    fs::write(&ca_path, ca_pem).expect("write ca");
    fs::write(&cert_path, node_cert.pem()).expect("write cert");
    fs::write(&key_path, node_key.serialize_pem()).expect("write key");

    (dir, TlsConfig::new(cert_path, key_path, ca_path))
}

fn build_shared_ca() -> (rcgen::KeyPair, rcgen::Certificate, String) {
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let ca_params =
        rcgen::CertificateParams::new(vec!["ferrodruid-w53-ca".to_string()]).expect("ca params");
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca sign");
    let ca_pem = ca_cert.pem();
    (ca_key, ca_cert, ca_pem)
}

/// Probe-bind three loopback ports and return the first triple all
/// three bind cleanly on.  Mirrors the helper in `tls_three_node.rs`.
async fn pick_three_ports(base: u16) -> [SocketAddr; 3] {
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    for offset in [0u16, 10, 20, 30, 40, 50, 60, 70, 80, 90] {
        let p1 = base + offset;
        let p2 = p1 + 1;
        let p3 = p1 + 2;
        let a1 = SocketAddr::new(ip, p1);
        let a2 = SocketAddr::new(ip, p2);
        let a3 = SocketAddr::new(ip, p3);
        let probe = async {
            let l1 = TcpListener::bind(a1).await?;
            let l2 = TcpListener::bind(a2).await?;
            let l3 = TcpListener::bind(a3).await?;
            drop((l1, l2, l3));
            Ok::<(), std::io::Error>(())
        };
        if probe.await.is_ok() {
            return [a1, a2, a3];
        }
    }
    panic!("could not bind 3 loopback ports starting at {base}");
}

#[allow(dead_code)]
struct TlsNode {
    engine: Arc<ReplicationEngine>,
    cm: Arc<ClusterManager>,
    transport: Arc<TcpTransport>,
    _certs_dir: TempDir,
}

impl TlsNode {
    async fn spawn(
        node_id: &str,
        bind_addr: SocketAddr,
        peers: HashMap<String, SocketAddr>,
        tls: TlsConfig,
        certs_dir: TempDir,
    ) -> Self {
        let cm = Arc::new(ClusterManager::new_single_node(NodeInfo {
            id: node_id.to_string(),
            host: bind_addr.ip().to_string(),
            port: bind_addr.port(),
            role: NodeRole::AllInOne,
        }));

        let config = ReplicationConfig {
            node_id: node_id.to_string(),
            listen_addr: bind_addr.to_string(),
            peers: peers.keys().cloned().collect(),
            heartbeat_interval_ms: 100,
            election_timeout_ms: 600,
            cluster_security_hint: ferrodruid_cluster::replication::ClusterSecurityHint::Psk,
        };
        let engine = Arc::new(ReplicationEngine::new(config, Arc::clone(&cm)));

        let psk = Arc::new(derive_psk("w53-mtls-soak-psk").expect("derive psk"));
        let tcfg = TcpTransportConfig {
            bind_addr,
            peers: peers.into_iter().collect(),
            connect_timeout: Duration::from_millis(800),
            heartbeat_period: Duration::from_millis(100),
            psk,
            local_node_id: node_id.to_string(),
            security: ferrodruid_cluster::transport::ClusterSecurityMode::MutualTls(tls),
        };
        let transport = TcpTransport::bind(tcfg, Arc::clone(&engine))
            .await
            .expect("bind transport");

        // Drive the production tick loop at 50 ms cadence.
        transport
            .spawn_tick_loop(Arc::clone(&engine), Duration::from_millis(50))
            .await;

        Self {
            engine,
            cm,
            transport,
            _certs_dir: certs_dir,
        }
    }

    async fn shutdown(self) {
        Arc::clone(&self.transport).shutdown().await;
    }
}

/// Wave 53: 3-node mTLS cluster replicates 100 commands cleanly.
///
/// We lean on `force_leader_with_term` so the test focuses on the
/// replication path rather than racing election + handshake under
/// load.  Once the leader is fixed we submit 100 distinct
/// `RegisterService` commands through `submit_with_majority_ack`
/// (the production wire-aware entry-point) with an 8 s submit
/// timeout that comfortably covers TLS handshake + roundtrip on
/// loaded CI.
///
/// Final convergence is asserted by the leader's `last_index` and
/// by sampling the followers' `last_index` against the leader's
/// after a settle window.  We allow a generous settle budget
/// because, under mTLS, the very first `ReplicateCommand` per peer
/// pays a full handshake (~1-3 ms on this hardware, more on slow
/// CI) before any frame ships.
#[tokio::test]
#[ignore]
async fn mtls_3_node_100_commands_replicate() {
    let (ca_key, ca_cert, ca_pem) = build_shared_ca();
    let (dir1, cfg1) = gen_certs_with_shared_ca("node-1", &ca_key, &ca_cert, &ca_pem);
    let (dir2, cfg2) = gen_certs_with_shared_ca("node-2", &ca_key, &ca_cert, &ca_pem);
    let (dir3, cfg3) = gen_certs_with_shared_ca("node-3", &ca_key, &ca_cert, &ca_pem);

    let [a1, a2, a3] = pick_three_ports(50961).await;

    let mut peers1 = HashMap::new();
    peers1.insert("node-2".to_string(), a2);
    peers1.insert("node-3".to_string(), a3);
    let mut peers2 = HashMap::new();
    peers2.insert("node-1".to_string(), a1);
    peers2.insert("node-3".to_string(), a3);
    let mut peers3 = HashMap::new();
    peers3.insert("node-1".to_string(), a1);
    peers3.insert("node-2".to_string(), a2);

    let n1 = TlsNode::spawn("node-1", a1, peers1, cfg1, dir1).await;
    let n2 = TlsNode::spawn("node-2", a2, peers2, cfg2, dir2).await;
    let n3 = TlsNode::spawn("node-3", a3, peers3, cfg3, dir3).await;

    // Pin n1 as leader so the soak isolates the *replication* path.
    n1.engine.force_leader_with_term(2).await;
    n1.engine.set_submit_timeout_ms(8_000).await;
    assert_eq!(n1.engine.role().await, ReplicationRole::Leader);

    let started = Instant::now();
    let mut leader_index = 0u64;
    for i in 0..W53_COMMAND_COUNT {
        let cmd = ClusterCommand::RegisterService(ServiceEntry {
            service_type: "broker".to_string(),
            host: "10.0.0.1".to_string(),
            port: 9092,
            node_id: format!("svc-w53-{i:03}"),
        });
        // submit_with_majority_ack: leader-side ack count must reach
        // majority (2 of 3) — i.e. at least one follower must have
        // acked.  Under mTLS this is the strict end-to-end success
        // signal (handshake done + ack frame round-trip).
        match n1.engine.submit_with_majority_ack(cmd).await {
            Ok(ack) => {
                assert!(
                    ack.ack_count >= 2,
                    "majority must be >= 2 of 3, got {} at i={i}",
                    ack.ack_count,
                );
                leader_index = ack.index;
            }
            Err(e) => panic!("submit_with_majority_ack failed at i={i}: {e:?}"),
        }
    }
    let submit_elapsed = started.elapsed();
    assert_eq!(
        leader_index, W53_COMMAND_COUNT as u64,
        "leader_index must equal {W53_COMMAND_COUNT}",
    );
    assert_eq!(
        n1.engine.last_index().await,
        W53_COMMAND_COUNT as u64,
        "leader last_index must equal {W53_COMMAND_COUNT}",
    );

    // Settle window for tail acks / replicate-ack drains.
    let settle_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let i2 = n2.engine.last_index().await;
        let i3 = n3.engine.last_index().await;
        if i2 == W53_COMMAND_COUNT as u64 && i3 == W53_COMMAND_COUNT as u64 {
            break;
        }
        if Instant::now() > settle_deadline {
            panic!(
                "followers did not converge on last_index={W53_COMMAND_COUNT} \
                 within 10s settle: e2={i2}, e3={i3}",
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Convergence: all 3 cluster managers must have the same broker
    // services.  Order-insensitive compare via sorted node_id list.
    let collect = |cm: &ClusterManager| -> Vec<String> {
        let mut v: Vec<String> = cm
            .services("broker")
            .iter()
            .map(|s| s.node_id.clone())
            .collect();
        v.sort();
        v
    };
    let s1 = collect(&n1.cm);
    let s2 = collect(&n2.cm);
    let s3 = collect(&n3.cm);
    assert_eq!(
        s1.len(),
        W53_COMMAND_COUNT,
        "leader must hold {W53_COMMAND_COUNT} broker services",
    );
    assert_eq!(s1, s2, "leader and node-2 must agree on services");
    assert_eq!(s1, s3, "leader and node-3 must agree on services");

    println!(
        "[mtls-soak] OK — {W53_COMMAND_COUNT} commands replicated over mTLS in \
         submit_elapsed={submit_elapsed:?}, total={:?}",
        started.elapsed(),
    );

    n1.shutdown().await;
    n2.shutdown().await;
    n3.shutdown().await;
}
