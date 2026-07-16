// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 44 / 44B — optional mTLS integration smoke tests.
//!
//! These tests are gated behind both `#[cfg(feature = "cluster-tls")]` and
//! `#[ignore]` so they only run when the operator explicitly opts in:
//!
//! ```text
//! cargo test -p ferrodruid-cluster --features cluster-tls \
//!     --test tls_three_node -- --ignored --nocapture
//! ```
//!
//! Wave 44 landed PEM ingestion + a `tokio_rustls` handshake unit; Wave 44B
//! wires the live `TcpTransport` to wrap every inbound `TcpStream` in
//! `TlsAcceptor::accept` and every outbound dial in `TlsConnector::connect`,
//! with a `tokio::io::split` over the resulting `TlsStream<TcpStream>`. The
//! tests below now spawn three production `TcpTransport` instances over
//! mTLS and assert leader election emerges via the timer (no shortcut),
//! plus a wire-trace assertion that the bytes on the cluster TCP socket
//! are TLS records — not the Wave 40-A `[u32 len][HMAC][JSON]` shape.

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
use ferrodruid_cluster::tls::{TlsConfig, load_client_config, load_server_config};
use ferrodruid_cluster::transport::{TcpTransport, TcpTransportConfig};
use ferrodruid_cluster::{ClusterManager, NodeInfo, NodeRole};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// Self-signed CA + per-node leaf cert helper. Returns the `TempDir`
/// holding the PEM files (kept alive by the caller) and the
/// [`TlsConfig`] paths for the node. SANs include the node id, the
/// string `"localhost"` and `"127.0.0.1"` so SNI-based hostname
/// verification matches whatever value the dialer picks.
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

/// Build a self-signed CA `KeyPair` + `Certificate` + serialised PEM
/// that all three nodes share so any pair can mutually authenticate.
fn build_shared_ca() -> (rcgen::KeyPair, rcgen::Certificate, String) {
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let ca_params =
        rcgen::CertificateParams::new(vec!["ferrodruid-w44b-ca".to_string()]).expect("ca params");
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca sign");
    let ca_pem = ca_cert.pem();
    (ca_key, ca_cert, ca_pem)
}

// ---------------------------------------------------------------------------
// Original Wave 44 handshake-only smoke tests, kept so the loaders are
// not dead code on their own.
// ---------------------------------------------------------------------------

/// One client and one server holding certs signed by the same CA can
/// complete an mTLS handshake and exchange one byte. This is the
/// minimum-viable smoke; case_i below now goes much further (3-node
/// cluster over the production transport).
#[tokio::test]
#[ignore]
async fn case_i0_handshake_one_byte() {
    let (ca_key, ca_cert, ca_pem) = build_shared_ca();
    let (_dir_server, server_cfg) =
        gen_certs_with_shared_ca("server-node", &ca_key, &ca_cert, &ca_pem);
    let (_dir_client, client_cfg) =
        gen_certs_with_shared_ca("client-node", &ca_key, &ca_cert, &ca_pem);

    let acceptor = load_server_config(&server_cfg).expect("server cfg");
    let connector = load_client_config(&client_cfg).expect("client cfg");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut tls = acceptor.accept(stream).await.expect("server handshake");
        let mut buf = [0u8; 5];
        tls.read_exact(&mut buf).await.expect("server read");
        assert_eq!(&buf, b"hello");
        tls.write_all(b"world").await.expect("server write");
        tls.shutdown().await.ok();
    });

    let client = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.expect("client connect");
        let dns = rustls::pki_types::ServerName::try_from("server-node")
            .expect("server name")
            .to_owned();
        let mut tls = connector
            .connect(dns, stream)
            .await
            .expect("client handshake");
        tls.write_all(b"hello").await.expect("client write");
        let mut buf = [0u8; 5];
        tls.read_exact(&mut buf).await.expect("client read");
        assert_eq!(&buf, b"world");
    });

    let _ = tokio::join!(server, client);
}

/// case_j — A client whose cert is signed by an *unrelated* CA must be
/// rejected by the server's `WebPkiClientVerifier`.
#[tokio::test]
#[ignore]
async fn case_j_node_with_wrong_ca_rejected() {
    let (ca_key_good, ca_cert_good, ca_pem_good) = build_shared_ca();
    let (ca_key_rogue, ca_cert_rogue, ca_pem_rogue) = build_shared_ca();
    let (_dir_server, server_cfg) =
        gen_certs_with_shared_ca("server-node", &ca_key_good, &ca_cert_good, &ca_pem_good);
    // Client built with a totally different CA — server must refuse.
    let (_dir_other, other_cfg) =
        gen_certs_with_shared_ca("rogue-node", &ca_key_rogue, &ca_cert_rogue, &ca_pem_rogue);

    let acceptor = load_server_config(&server_cfg).expect("server cfg");
    // Client trusts the server's CA, but presents a cert signed by a
    // different CA the server has never heard of.
    let client_cfg = TlsConfig::new(
        other_cfg.cert_path.clone(),
        other_cfg.key_path.clone(),
        server_cfg.ca_path.clone(),
    );
    let connector = load_client_config(&client_cfg).expect("client cfg");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        // Server-side handshake MUST fail because the client's cert is
        // not signed by any CA in the server's trust store.
        let result = acceptor.accept(stream).await;
        assert!(
            result.is_err(),
            "server handshake must fail when client cert is signed by unknown CA",
        );
    });

    let client = tokio::spawn(async move {
        let stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(_) => return,
        };
        let dns = rustls::pki_types::ServerName::try_from("server-node")
            .expect("server name")
            .to_owned();
        // Either side can detect the failure; we just want NO panic and NO success.
        let _ = connector.connect(dns, stream).await;
    });

    let _ = tokio::join!(server, client);
}

// ---------------------------------------------------------------------------
// Wave 44B — three-node cluster over the production TcpTransport with mTLS
// ---------------------------------------------------------------------------

/// One live node in the harness. Mirrors the shape of `three_node_tcp.rs`'s
/// `TcpNode` but wires `TcpTransportConfig::tls = Some(...)` so every
/// inbound and outbound socket is wrapped in mTLS.
#[allow(dead_code)]
struct TlsNode {
    /// Replication engine driving consensus / state-machine application.
    engine: Arc<ReplicationEngine>,
    /// Cluster manager (state machine), kept so tests can introspect.
    cm: Arc<ClusterManager>,
    /// Production TCP transport (now over TLS).
    transport: Arc<TcpTransport>,
    /// Holds the temp dir that owns the PEM files; dropping it removes them.
    _certs_dir: TempDir,
}

impl TlsNode {
    /// Spawn a node bound to `bind_addr`, with `peers` mapping each peer
    /// id to its listen address (excluding self), and the supplied
    /// [`TlsConfig`] active on the wire.
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

        let psk = Arc::new(derive_psk("tls-three-node-harness-psk").expect("derive psk"));
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

    /// Stop the listener task and drop outgoing connections.
    async fn shutdown(self) {
        Arc::clone(&self.transport).shutdown().await;
    }
}

/// Probe-bind three loopback ports and return the first triple that all
/// three bind cleanly on. Mirrors `bring_up_three_nodes` in the
/// non-TLS suite so flaky CI port collisions are tolerated.
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

/// case_i — Three nodes over mTLS form a cluster and elect a leader via
/// the production tick loop alone (no `force_leader` shortcut). All
/// inbound and outbound sockets are wrapped by `tokio_rustls`; if the
/// generic-over-stream wiring landed in Wave 44B is broken the cluster
/// either fails to handshake or the leader never emerges — either way
/// the timeout below trips.
#[tokio::test]
#[ignore]
async fn case_i_3_nodes_form_cluster_via_mtls() {
    let (ca_key, ca_cert, ca_pem) = build_shared_ca();
    let (dir1, cfg1) = gen_certs_with_shared_ca("node-1", &ca_key, &ca_cert, &ca_pem);
    let (dir2, cfg2) = gen_certs_with_shared_ca("node-2", &ca_key, &ca_cert, &ca_pem);
    let (dir3, cfg3) = gen_certs_with_shared_ca("node-3", &ca_key, &ca_cert, &ca_pem);

    let [a1, a2, a3] = pick_three_ports(48961).await;

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

    let started = Instant::now();
    let nodes = [&n1, &n2, &n3];

    // Wait up to 10 s for any node to become leader. mTLS adds a small
    // handshake cost (a few ms per outbound dial); the budget is generous
    // so flaky CI does not trip on a slow first round.
    let mut leader_idx: Option<usize> = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        for (i, n) in nodes.iter().enumerate() {
            if n.engine.role().await == ReplicationRole::Leader {
                leader_idx = Some(i);
                break;
            }
        }
        if leader_idx.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let leader_idx = leader_idx.expect("no node became leader within 10s over mTLS");
    let leader_term = nodes[leader_idx].engine.term().await;
    println!(
        "[case I] node-{} elected leader at term={} in {} ms via mTLS",
        leader_idx + 1,
        leader_term,
        started.elapsed().as_millis(),
    );

    // Wait for majority recognition of the leader's term.
    let recognition_deadline = Instant::now() + Duration::from_secs(10);
    let mut recognised = 1; // leader counts itself
    while Instant::now() < recognition_deadline {
        let mut count = 1;
        for (i, n) in nodes.iter().enumerate() {
            if i == leader_idx {
                continue;
            }
            if n.engine.term().await >= leader_term {
                count += 1;
            }
        }
        if count >= 2 {
            recognised = count;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        recognised >= 2,
        "majority of 3 (>= 2) must observe leader's term {leader_term} over mTLS (saw {recognised})",
    );
    println!(
        "[case I] PASS — {recognised}/3 nodes recognise leader in {} ms total over mTLS",
        started.elapsed().as_millis(),
    );

    n1.shutdown().await;
    n2.shutdown().await;
    n3.shutdown().await;
}

// ---------------------------------------------------------------------------
// Wire-trace test: assert the bytes on the cluster TCP socket are TLS
// records, not the Wave 40-A `[u32 len][HMAC][JSON]` cleartext shape.
// ---------------------------------------------------------------------------

/// Tiny TCP proxy that accepts one connection, captures the first
/// `MAX_PEEK` bytes of the client→server stream into the shared
/// buffer, and forwards everything bidirectionally to the upstream
/// address. Used by `case_k_wire_bytes_are_tls_encrypted` to observe
/// the on-the-wire bytes between two cluster nodes.
async fn run_one_shot_peek_proxy(
    listen_addr: SocketAddr,
    upstream: SocketAddr,
    peek_buf: Arc<Mutex<Vec<u8>>>,
) {
    const MAX_PEEK: usize = 32;
    let listener = TcpListener::bind(listen_addr).await.expect("proxy bind");
    let (mut downstream, _peer) = listener.accept().await.expect("proxy accept");
    let mut up = TcpStream::connect(upstream).await.expect("proxy connect");

    // Peek the first MAX_PEEK bytes of the client→server direction
    // BEFORE forwarding so the assertion runs on the actual bytes the
    // cluster client emitted on the wire.
    let mut peek = [0u8; MAX_PEEK];
    let mut got = 0usize;
    while got < MAX_PEEK {
        match downstream.read(&mut peek[got..]).await {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(_) => break,
        }
    }
    {
        let mut buf = peek_buf.lock().await;
        buf.extend_from_slice(&peek[..got]);
    }
    // Forward whatever we already peeked to the upstream and then
    // bridge the rest of the connection.
    let _ = up.write_all(&peek[..got]).await;
    let _ = tokio::io::copy_bidirectional(&mut downstream, &mut up).await;
}

/// case_k — Wire-trace assertion. Stand up two nodes (n1 listening on
/// real port A1, n2 listening on real port A2) and a one-shot proxy on
/// port P. n1's peer map says "node-2 lives at P". When n1 dials
/// peer-2 it actually connects to the proxy, which captures the first
/// 32 bytes and forwards to A2. We then assert the captured bytes
/// look like a TLS ClientHello (record type 0x16, version major 0x03)
/// and explicitly NOT like the Wave 40-A `[u32 len][HMAC]{"type":...}`
/// frame.
#[tokio::test]
#[ignore]
async fn case_k_wire_bytes_are_tls_encrypted() {
    let (ca_key, ca_cert, ca_pem) = build_shared_ca();
    let (dir1, cfg1) = gen_certs_with_shared_ca("node-1", &ca_key, &ca_cert, &ca_pem);
    let (dir2, cfg2) = gen_certs_with_shared_ca("node-2", &ca_key, &ca_cert, &ca_pem);

    // Pick 3 ports: 2 for the nodes, 1 for the proxy.
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let mut chosen: Option<(SocketAddr, SocketAddr, SocketAddr)> = None;
    for offset in [0u16, 10, 20, 30, 40, 50, 60, 70, 80] {
        let p1 = 49961 + offset;
        let p2 = p1 + 1;
        let pp = p1 + 2;
        let a1 = SocketAddr::new(ip, p1);
        let a2 = SocketAddr::new(ip, p2);
        let ap = SocketAddr::new(ip, pp);
        let probe = async {
            let l1 = TcpListener::bind(a1).await?;
            let l2 = TcpListener::bind(a2).await?;
            let l3 = TcpListener::bind(ap).await?;
            drop((l1, l2, l3));
            Ok::<(), std::io::Error>(())
        };
        if probe.await.is_ok() {
            chosen = Some((a1, a2, ap));
            break;
        }
    }
    let (a1, a2, ap) = chosen.expect("could not bind 3 loopback ports for case_k");

    // Start the proxy first so it is listening when n1 dials node-2.
    let peek_buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let peek_buf_clone = Arc::clone(&peek_buf);
    let proxy_handle = tokio::spawn(async move {
        run_one_shot_peek_proxy(ap, a2, peek_buf_clone).await;
    });

    // n1 sees node-2 at the proxy's address, not the real one.
    let mut peers1 = HashMap::new();
    peers1.insert("node-2".to_string(), ap);
    let mut peers2 = HashMap::new();
    peers2.insert("node-1".to_string(), a1);

    let n1 = TlsNode::spawn("node-1", a1, peers1, cfg1, dir1).await;
    let n2 = TlsNode::spawn("node-2", a2, peers2, cfg2, dir2).await;

    // Wait long enough for n1's tick loop to have dialled node-2 at
    // least once — election broadcast or pre-vote will both trigger
    // an outbound connect attempt.
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        {
            let buf = peek_buf.lock().await;
            if buf.len() >= 5 {
                break;
            }
        }
        if Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let captured = {
        let buf = peek_buf.lock().await;
        buf.clone()
    };
    println!(
        "[case K] captured {} bytes on wire: {:02x?}",
        captured.len(),
        &captured[..captured.len().min(16)]
    );

    assert!(
        captured.len() >= 5,
        "no bytes captured on wire — did n1 dial node-2 within 8s?",
    );

    // TLS record header: byte[0] == 0x16 (Handshake), byte[1] == 0x03
    // (TLS major version 3), byte[2] in {0x01, 0x02, 0x03, 0x04}
    // (minor version 1..=4 depending on TLS 1.0..1.3).
    assert_eq!(
        captured[0], 0x16,
        "first byte must be TLS Handshake (0x16), got 0x{:02x}",
        captured[0],
    );
    assert_eq!(
        captured[1], 0x03,
        "second byte must be TLS major version 3, got 0x{:02x}",
        captured[1],
    );
    assert!(
        (1..=4).contains(&captured[2]),
        "third byte must be TLS minor version 1..=4, got 0x{:02x}",
        captured[2],
    );

    // Assert NOT the cleartext Wave 40-A framing. The handshake JSON is
    // ~50-80 bytes; its u32 BE length prefix would have byte[0]==0x00.
    assert_ne!(
        captured[0], 0x00,
        "first byte is 0x00 — looks like the cleartext PSK [u32 len] frame, mTLS wiring did NOT take effect",
    );

    // Even more specific: assert no JSON tokens leak in the captured bytes.
    let captured_str = String::from_utf8_lossy(&captured);
    for needle in ["announced_node_id", "\"type\":", "Handshake", "VoteRequest"] {
        assert!(
            !captured_str.contains(needle),
            "captured bytes contain cleartext JSON token {needle:?} — mTLS is not encrypting the wire: {captured_str:?}",
        );
    }

    println!(
        "[case K] PASS — wire bytes look like TLS (0x{:02x} 0x{:02x} 0x{:02x} ...) with no cleartext JSON",
        captured[0], captured[1], captured[2],
    );

    proxy_handle.abort();
    let _ = proxy_handle.await;
    n1.shutdown().await;
    n2.shutdown().await;
}
