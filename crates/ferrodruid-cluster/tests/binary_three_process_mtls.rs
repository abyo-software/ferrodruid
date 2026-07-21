// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W1-C — production-binary 3-process cluster smoke under the **default
//! mTLS** posture.
//!
//! Companion to `tests/binary_three_process.rs` which exercises the
//! explicit PSK-cleartext opt-in. This test generates a self-signed
//! CA + per-node leaf certs at runtime, spawns three `ferrodruid serve`
//! processes WITHOUT `--cluster-security psk` (so the binary picks
//! up its default mTLS posture, see `bins/ferrodruid/src/main.rs`
//! commit 47b002f) and asserts that:
//!
//!   * each process logs `cluster tick loop started` within 20 s
//!   * at least one process logs a leader-promotion marker within
//!     another 8 s
//!
//! The leader-side wire is therefore validated end-to-end on the
//! mTLS path — handshake + cert validation + chunked frame I/O —
//! through the production binary rather than the crate-internal
//! `TcpTransport` helpers (which the crate-level `tls_three_node`
//! and `mtls_replication_e2e` already cover).
//!
//! Marked `#[ignore]` because it requires a release build of the
//! `ferrodruid` binary. Invoke via:
//!
//! ```sh
//! cargo test -p ferrodruid-cluster --features cluster-tls \
//!     --test binary_three_process_mtls -- --ignored --nocapture
//! ```

#![cfg(feature = "cluster-tls")]
#![allow(missing_docs)]
#![allow(clippy::too_many_lines)]

use std::fs;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn ensure_release_binary() -> PathBuf {
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
            let mut p = PathBuf::from(manifest);
            p.pop(); // crates/ferrodruid-cluster -> crates
            p.pop(); // crates -> repo
            p.push("target");
            p
        });
    let candidate = target_dir.join("release").join("ferrodruid");
    if candidate.exists() {
        return candidate;
    }
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("cluster crate must be under <workspace>/crates")
        .to_path_buf();
    let status = Command::new(env!("CARGO"))
        .current_dir(workspace_root)
        .args([
            "build",
            "--release",
            "-p",
            "ferrodruid",
            "--bin",
            "ferrodruid",
        ])
        .status()
        .expect("invoke cargo");
    assert!(status.success(), "cargo build --release failed");
    assert!(candidate.exists(), "binary not found at {candidate:?}");
    candidate
}

fn port_is_free(port: u16) -> bool {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    TcpListener::bind(addr).is_ok()
}

fn pick_port_block(base: u16) -> Option<(u16, u16)> {
    for offset in (0u16..200).step_by(10) {
        let http = base + offset;
        let cluster = http + 10_000;
        let all_free = (0..3).all(|i| port_is_free(http + i) && port_is_free(cluster + i));
        if all_free {
            return Some((http, cluster));
        }
    }
    None
}

/// One self-signed CA + three node certs all SANed for the
/// loopback test names + 127.0.0.1.
struct TestCerts {
    _dir: tempfile::TempDir,
    ca_pem: PathBuf,
    node_certs: [(PathBuf, PathBuf); 3],
}

fn build_test_certs() -> TestCerts {
    let dir = tempfile::tempdir().expect("tempdir");

    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let ca_params = rcgen::CertificateParams::new(vec!["ferrodruid-binary-mtls-ca".to_string()])
        .expect("ca params");
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca self-sign");
    let ca_pem_path = dir.path().join("ca.pem");
    fs::write(&ca_pem_path, ca_cert.pem()).expect("write ca");

    let mut node_certs: [Option<(PathBuf, PathBuf)>; 3] = [None, None, None];
    for (i, slot) in node_certs.iter_mut().enumerate() {
        let node_name = format!("node-{}", i + 1);
        let node_key = rcgen::KeyPair::generate().expect("node key");
        let node_params = rcgen::CertificateParams::new(vec![
            node_name.clone(),
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .expect("node params");
        let node_cert = node_params
            .signed_by(&node_key, &ca_cert, &ca_key)
            .expect("node sign");
        let cert_path = dir.path().join(format!("{node_name}.pem"));
        let key_path = dir.path().join(format!("{node_name}-key.pem"));
        fs::write(&cert_path, node_cert.pem()).expect("write node cert");
        fs::write(&key_path, node_key.serialize_pem()).expect("write node key");
        *slot = Some((cert_path, key_path));
    }
    TestCerts {
        _dir: dir,
        ca_pem: ca_pem_path,
        node_certs: node_certs.map(Option::unwrap),
    }
}

struct Node {
    name: String,
    child: Child,
    log_lines: Arc<Mutex<Vec<String>>>,
    data_dir: tempfile::TempDir,
}

impl Node {
    #[allow(clippy::too_many_arguments)]
    fn spawn(
        binary: &PathBuf,
        node_id: &str,
        http_port: u16,
        cluster_port: u16,
        peers: &str,
        cert_path: &PathBuf,
        key_path: &PathBuf,
        ca_path: &PathBuf,
    ) -> Self {
        let data_dir = tempfile::tempdir().expect("tempdir");
        let cluster_bind = format!("127.0.0.1:{cluster_port}");
        let mut cmd = Command::new(binary);
        cmd.arg("serve")
            .arg("--mode")
            .arg("single-binary")
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(http_port.to_string())
            .arg("--data-dir")
            .arg(data_dir.path())
            .arg("--no-auth")
            .arg("--node-id")
            .arg(node_id)
            .arg("--cluster-bind")
            .arg(&cluster_bind)
            .arg("--cluster-peers")
            .arg(peers)
            // W1-C: NO `--cluster-security psk` here — the binary's
            // default mTLS posture is exercised via the cert flags.
            .arg("--cluster-psk")
            .arg("binary-three-process-mtls-harness-psk")
            .arg("--cluster-tls-cert")
            .arg(cert_path)
            .arg("--cluster-tls-key")
            .arg(key_path)
            .arg("--cluster-tls-ca")
            .arg(ca_path)
            .env("RUST_LOG", "ferrodruid_cluster=info,ferrodruid=info,info")
            .env("NO_COLOR", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn ferrodruid");

        let log_lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        if let Some(stdout) = child.stdout.take() {
            let lines = Arc::clone(&log_lines);
            let name = node_id.to_string();
            thread::spawn(move || {
                let reader = BufReader::new(stdout);
                for line in reader.lines().map_while(Result::ok) {
                    eprintln!("[{name} stdout] {line}");
                    if let Ok(mut g) = lines.lock() {
                        g.push(line);
                    }
                }
            });
        }
        if let Some(stderr) = child.stderr.take() {
            let lines = Arc::clone(&log_lines);
            let name = node_id.to_string();
            thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines().map_while(Result::ok) {
                    eprintln!("[{name} stderr] {line}");
                    if let Ok(mut g) = lines.lock() {
                        g.push(line);
                    }
                }
            });
        }
        Self {
            name: node_id.to_string(),
            child,
            log_lines,
            data_dir,
        }
    }

    fn saw_log(&self, needle: &str) -> bool {
        match self.log_lines.lock() {
            Ok(g) => g.iter().any(|l| l.contains(needle)),
            Err(_) => false,
        }
    }

    fn shutdown(mut self) {
        #[cfg(unix)]
        {
            let pid = self.child.id();
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .status();
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if let Ok(Some(_)) = self.child.try_wait() {
                    let _ = self.data_dir;
                    return;
                }
                thread::sleep(Duration::from_millis(50));
            }
            let _ = self.child.kill();
            let _ = self.child.wait();
            eprintln!("[{}] forced kill after grace window", self.name);
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = self.data_dir;
    }
}

#[tokio::test]
#[ignore]
async fn binary_three_process_mtls_default_forms_cluster() {
    let binary = ensure_release_binary();
    let certs = build_test_certs();

    let (http_base, cluster_base) =
        pick_port_block(38_961).expect("could not find 3 free contiguous ports");
    eprintln!(
        "mTLS binary smoke: http {http_base}-{} / cluster {cluster_base}-{}",
        http_base + 2,
        cluster_base + 2,
    );

    let cluster_peers = |idx: usize| -> String {
        let mut parts = Vec::new();
        for j in 0..3 {
            if j == idx {
                continue;
            }
            parts.push(format!(
                "node-{}@127.0.0.1:{}",
                j + 1,
                cluster_base + j as u16,
            ));
        }
        parts.join(",")
    };

    let n1 = Node::spawn(
        &binary,
        "node-1",
        http_base,
        cluster_base,
        &cluster_peers(0),
        &certs.node_certs[0].0,
        &certs.node_certs[0].1,
        &certs.ca_pem,
    );
    let n2 = Node::spawn(
        &binary,
        "node-2",
        http_base + 1,
        cluster_base + 1,
        &cluster_peers(1),
        &certs.node_certs[1].0,
        &certs.node_certs[1].1,
        &certs.ca_pem,
    );
    let n3 = Node::spawn(
        &binary,
        "node-3",
        http_base + 2,
        cluster_base + 2,
        &cluster_peers(2),
        &certs.node_certs[2].0,
        &certs.node_certs[2].1,
        &certs.ca_pem,
    );

    // Wait for cluster tick loop bring-up (binary marker).
    let started = Instant::now();
    let mut tick_loop_started_count = 0usize;
    while started.elapsed() < Duration::from_secs(20) {
        tick_loop_started_count = [&n1, &n2, &n3]
            .iter()
            .filter(|n| n.saw_log("cluster tick loop started"))
            .count();
        if tick_loop_started_count >= 3 {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }

    // Wait for at least one leader promotion.
    // Under mTLS the engine arms MTLS_STARTUP_GRACE_MAX_MS (15 s) before the
    // first election and floors the election timeout at 5 s (W2-E RCA). An
    // 8 s deadline races the grace window and flakes on KVM; allow grace +
    // one election cycle + margin.
    let leader_deadline = Instant::now() + Duration::from_secs(45);
    let mut leader_count = 0usize;
    while Instant::now() < leader_deadline {
        leader_count = [&n1, &n2, &n3]
            .iter()
            .filter(|n| {
                n.saw_log("promoted to leader on vote response")
                    || n.saw_log("cluster leader elected")
            })
            .count();
        if leader_count >= 1 {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }

    let listening_count = [&n1, &n2, &n3]
        .iter()
        .filter(|n| n.saw_log("listening"))
        .count();

    n1.shutdown();
    n2.shutdown();
    n3.shutdown();

    assert!(
        listening_count >= 3,
        "all 3 mTLS-binary processes must reach HTTP listening (got {listening_count})",
    );
    assert!(
        tick_loop_started_count >= 3,
        "all 3 mTLS-binary processes must log `cluster tick loop started` within 20s (got {tick_loop_started_count})",
    );
    assert!(
        leader_count >= 1,
        "expected at least one mTLS-binary process to win the election (got {leader_count})",
    );
}
