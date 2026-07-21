// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Production-binary 3-process cluster smoke test.
//!
//! Spawns three independent `ferrodruid serve` processes on
//! `127.0.0.1:38911 / 38912 / 38913` (HTTP) with replication on
//! `127.0.0.1:48911 / 48912 / 48913` (cluster TCP) and asserts that the
//! cluster forms purely over the wire — no in-process shortcut. Wave 38-B's
//! production [`ferrodruid_cluster::transport::TcpTransport`] is the only
//! transport on the path.
//!
//! Marked `#[ignore]` because it requires a release build of the
//! `ferrodruid` binary; invoke via:
//!
//! ```sh
//! cargo test -p ferrodruid-cluster --test binary_three_process \
//!     -- --ignored --nocapture
//! ```

#![allow(clippy::too_many_lines)]

use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Locate the `ferrodruid` binary, building it if necessary.
fn ensure_release_binary() -> PathBuf {
    // Prefer the path Cargo gives us when running tests under cargo;
    // CARGO_TARGET_DIR or the implicit `target/` next to the workspace root.
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            // tests run with CWD = crate root; workspace target/ is two
            // levels up at `<repo>/target/`.
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

    // Build it. This is slow on first run; the test is `#[ignore]` so we
    // accept the latency.
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

/// Probe whether `port` is currently free on loopback.
fn port_is_free(port: u16) -> bool {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    TcpListener::bind(addr).is_ok()
}

/// Pick a contiguous block of 3 HTTP + 3 cluster ports starting near `base`.
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

/// One spawned child process.
struct Node {
    name: String,
    child: Child,
    log_lines: Arc<Mutex<Vec<String>>>,
    data_dir: tempfile::TempDir,
}

impl Node {
    fn spawn(
        binary: &PathBuf,
        node_id: &str,
        http_port: u16,
        cluster_port: u16,
        peers: &str,
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
            // W1-C: post-mTLS-default fix (binary commit 47b002f, 2026-06-14).
            // The single-binary node now defaults its cluster transport
            // to mTLS and refuses to start without --cluster-tls-{cert,key,ca}.
            // This harness is the PSK-only smoke; opt back into the
            // PSK-over-cleartext fallback explicitly so the harness still
            // exercises the same wire it always did. The default mTLS
            // path is covered by tests/mtls_replication_e2e.rs +
            // tests/tls_three_node.rs.
            .arg("--cluster-security")
            .arg("psk")
            // Wave 40-A: every spawned binary uses the same harness PSK so
            // the authenticated wire path matches the production posture.
            .arg("--cluster-psk")
            .arg("binary-three-process-harness-psk")
            .env("RUST_LOG", "ferrodruid_cluster=info,ferrodruid=info,info")
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
        // Try graceful (SIGTERM) first; fall back to SIGKILL.
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            // SAFETY: libc::kill is intrinsically unsafe; this test crate
            // does not have forbid(unsafe_code), and process control is
            // intrinsic to multi-process testing.
            let pid = self.child.id() as i32;
            // Send SIGTERM via the Tokio-free path: std::process has no
            // SIGTERM helper, so we shell out via `kill`.
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .status();
            // Wait briefly.
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                match self.child.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) => thread::sleep(Duration::from_millis(50)),
                    Err(_) => break,
                }
            }
            let _ = self.child.kill();
            if let Ok(status) = self.child.wait() {
                eprintln!(
                    "[{}] exit signal={:?} code={:?}",
                    self.name,
                    status.signal(),
                    status.code()
                );
            }
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
async fn binary_3_process_forms_cluster_via_tcp_only() {
    let binary = ensure_release_binary();

    let (http_base, cluster_base) =
        pick_port_block(38_911).expect("could not find 3 free contiguous ports");
    eprintln!(
        "binary smoke: http {http_base}-{} / cluster {cluster_base}-{}",
        http_base + 2,
        cluster_base + 2,
    );

    // Build the cluster-peers strings. Each node lists the OTHER two.
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
    );
    let n2 = Node::spawn(
        &binary,
        "node-2",
        http_base + 1,
        cluster_base + 1,
        &cluster_peers(1),
    );
    let n3 = Node::spawn(
        &binary,
        "node-3",
        http_base + 2,
        cluster_base + 2,
        &cluster_peers(2),
    );

    // Give the nodes time to start up.  Wave 38-DE: the binary now logs
    // `"cluster tick loop started"` when the production tick loop is armed
    // (see `bins/ferrodruid/src/main.rs:388`); leader emergence happens
    // implicitly via `ReplicationEngine::tick` and is not log-pinned at the
    // binary level (the engine logs `"promoted to leader on vote response"`
    // at info!).  We treat tick-loop-started as the bring-up gate and
    // accept either of the two known leader-promotion log markers so the
    // test stays robust against future log polish.
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

    // Allow a further window for one of the nodes to win the election
    // (election_timeout_ms default = 1500, so 5 s is generous).
    let leader_deadline = Instant::now() + Duration::from_secs(8);
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

    // SIGTERM all 3 first so the assertion failure messages show after
    // teardown (so the test runner does not leave zombies on assert!()).
    n1.shutdown();
    n2.shutdown();
    n3.shutdown();

    assert!(
        listening_count >= 3,
        "all 3 processes must reach the HTTP listening state (got {listening_count})",
    );
    assert!(
        tick_loop_started_count >= 3,
        "all 3 processes must log `cluster tick loop started` within 20s (got {tick_loop_started_count})",
    );
    assert!(
        leader_count >= 1,
        "expected at least one process to win the election within the deadline (got {leader_count})",
    );
}
