// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Real-TCP 3-node cluster replication E2E test.
//!
//! Wave 38-B refactor: this file is now an orchestration harness on top of
//! the production [`ferrodruid_cluster::transport::TcpTransport`]. The
//! transport itself (listener task, connection pool, length-prefixed JSON
//! framing, VoteResponse-back-over-the-wire) lives in
//! `crates/ferrodruid-cluster/src/transport.rs` so the binary can use it
//! directly.
//!
//! Marked `#[ignore]` so normal `cargo test` does not run it; invoke via:
//!
//! ```sh
//! cargo test -p ferrodruid-cluster --test three_node_tcp -- --ignored --nocapture
//! ```

#![allow(clippy::too_many_lines)]

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodruid_cluster::auth::{ClusterPsk, derive_psk};
use ferrodruid_cluster::replication::{
    ReplicationConfig, ReplicationEngine, ReplicationMessage, ReplicationRole,
};
use ferrodruid_cluster::transport::{TcpTransport, TcpTransportConfig};
use ferrodruid_cluster::{
    ClusterCommand, ClusterManager, NodeInfo, NodeRole, SegmentAnnouncement, ServiceEntry,
};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// Wave 40-A: every harness-spawned node uses the same shared PSK so the
/// authenticated wire path matches what `bins/ferrodruid` would use in
/// production with operators sharing a single secret across the cluster.
fn harness_psk() -> Arc<ClusterPsk> {
    Arc::new(derive_psk("three-node-tcp-harness-psk").expect("derive harness psk"))
}

// ---------------------------------------------------------------------------
// Test harness: TcpNode wraps an engine + a production TcpTransport
// ---------------------------------------------------------------------------

/// One live node in the harness.
struct TcpNode {
    /// Replication engine driving consensus / state-machine application.
    engine: Arc<ReplicationEngine>,
    /// Cluster manager (state machine), kept so tests can introspect.
    cm: Arc<ClusterManager>,
    /// Production TCP transport.
    transport: Arc<TcpTransport>,
    /// Set of peer ids that this node refuses to talk to (one-way partition).
    blocked_peers: Arc<Mutex<Vec<String>>>,
}

impl TcpNode {
    /// Spawn a node bound to `bind_addr`. `peers` maps each peer id to its
    /// listen address (excluding self).
    async fn spawn(
        node_id: &str,
        bind_addr: SocketAddr,
        peers: HashMap<String, SocketAddr>,
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
            election_timeout_ms: 1000,
            cluster_security_hint: ferrodruid_cluster::replication::ClusterSecurityHint::Psk,
        };
        let engine = Arc::new(ReplicationEngine::new(config, Arc::clone(&cm)));

        let tcfg = TcpTransportConfig {
            bind_addr,
            peers: peers.into_iter().collect(),
            connect_timeout: Duration::from_millis(500),
            heartbeat_period: Duration::from_millis(100),
            psk: harness_psk(),
            local_node_id: node_id.to_string(),
            security: ferrodruid_cluster::transport::ClusterSecurityMode::PskCleartext,
        };
        let transport = TcpTransport::bind(tcfg, Arc::clone(&engine))
            .await
            .expect("bind transport");

        Self {
            engine,
            cm,
            transport,
            blocked_peers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Send a single message to a peer via the production transport. Honors
    /// the test-only `blocked_peers` set so partition cases can refuse a
    /// send without touching the transport.
    async fn send_to(&self, peer_id: &str, msg: &ReplicationMessage) -> std::io::Result<()> {
        {
            let blocked = self.blocked_peers.lock().await;
            if blocked.iter().any(|p| p == peer_id) {
                return Err(std::io::Error::other("peer blocked (partition)"));
            }
        }
        self.transport
            .send(peer_id, msg)
            .await
            .map_err(|e| std::io::Error::other(format!("transport: {e}")))
    }

    /// Block all outgoing traffic to `peer_id` (simulates one-way partition).
    async fn partition_from(&self, peer_id: &str) {
        let mut blocked = self.blocked_peers.lock().await;
        if !blocked.iter().any(|p| p == peer_id) {
            blocked.push(peer_id.to_string());
        }
    }

    /// Stop the listener task and drop outgoing connections.
    async fn shutdown(self) {
        Arc::clone(&self.transport).shutdown().await;
    }

    /// Wave 38-C: spawn a node and wire up the production tick loop so it
    /// participates in heartbeat-driven failover with no `force_leader`
    /// shortcut. `election_timeout_ms` and `heartbeat_interval_ms` are the
    /// engine-level knobs; the tick cadence is hard-wired at 50 ms.
    async fn spawn_with_scheduler(
        node_id: &str,
        bind_addr: SocketAddr,
        peers: HashMap<String, SocketAddr>,
        election_timeout_ms: u64,
        heartbeat_interval_ms: u64,
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
            heartbeat_interval_ms,
            election_timeout_ms,
            cluster_security_hint: ferrodruid_cluster::replication::ClusterSecurityHint::Psk,
        };
        let engine = Arc::new(ReplicationEngine::new(config, Arc::clone(&cm)));

        let tcfg = TcpTransportConfig {
            bind_addr,
            peers: peers.into_iter().collect(),
            connect_timeout: Duration::from_millis(500),
            heartbeat_period: Duration::from_millis(heartbeat_interval_ms),
            psk: harness_psk(),
            local_node_id: node_id.to_string(),
            security: ferrodruid_cluster::transport::ClusterSecurityMode::PskCleartext,
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
            blocked_peers: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

// ---------------------------------------------------------------------------
// Cluster bootstrap helpers
// ---------------------------------------------------------------------------

/// Bring up three nodes on `base..base+3`. Retries a few alternative bases
/// if the requested ports are already taken.
async fn bring_up_three_nodes(base: u16) -> (Vec<TcpNode>, u16) {
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    for offset in [0u16, 10, 20, 30, 40] {
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
        if probe.await.is_err() {
            continue;
        }

        let mut peers1 = HashMap::new();
        peers1.insert("node-2".to_string(), a2);
        peers1.insert("node-3".to_string(), a3);
        let mut peers2 = HashMap::new();
        peers2.insert("node-1".to_string(), a1);
        peers2.insert("node-3".to_string(), a3);
        let mut peers3 = HashMap::new();
        peers3.insert("node-1".to_string(), a1);
        peers3.insert("node-2".to_string(), a2);

        let n1 = TcpNode::spawn("node-1", a1, peers1).await;
        let n2 = TcpNode::spawn("node-2", a2, peers2).await;
        let n3 = TcpNode::spawn("node-3", a3, peers3).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        return (vec![n1, n2, n3], p1);
    }
    panic!("could not bind any 3 contiguous ports starting at {base}");
}

/// Drive an election where `candidate_idx` becomes leader. Uses real TCP for
/// `VoteRequest` — the production `TcpTransport` writes the `VoteResponse`
/// back over the same connection, then promotes the candidate locally once
/// the votes arrive (Wave 38-B). The harness still calls
/// `force_leader_with_term` as a belt-and-braces guarantee since the
/// election scheduler loop is Wave 38-C scope.
async fn elect_leader(nodes: &[TcpNode], candidate_idx: usize, target_term: u64) {
    let candidate = &nodes[candidate_idx];
    let candidate_id = candidate.engine.node_id().to_string();

    // Send VoteRequest over TCP. The production transport will respond with
    // a VoteResponse back over the same socket, which the candidate's own
    // inbound listener task will consume and feed into `record_vote`.
    let req = ReplicationMessage::VoteRequest {
        candidate_id: candidate_id.clone(),
        term: target_term,
    };
    for (i, n) in nodes.iter().enumerate() {
        if i == candidate_idx {
            continue;
        }
        let _ = candidate.send_to(n.engine.node_id(), &req).await;
    }

    // Allow inbound pipeline to deliver responses both directions.
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Promote: this is the test-harness equivalent of the election
    // scheduler. Wave 38-B feeds VoteResponses through the wire but does
    // not yet drive automatic role transitions from the heartbeat loop —
    // that ships in Wave 38-C.
    candidate.engine.force_leader_with_term(target_term).await;

    // Broadcast a heartbeat so followers update their term.
    let hb = ReplicationMessage::Heartbeat {
        leader_id: candidate_id.clone(),
        term: target_term,
    };
    for (i, n) in nodes.iter().enumerate() {
        if i == candidate_idx {
            continue;
        }
        let _ = candidate.send_to(n.engine.node_id(), &hb).await;
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
}

/// Submit `cmd` on the leader, then push the resulting `ReplicateCommand` to
/// each follower over TCP.
async fn replicate(
    leader: &TcpNode,
    followers: &[&TcpNode],
    cmd: ClusterCommand,
    expected_index: u64,
    term: u64,
) {
    let _ = leader.engine.submit(cmd.clone()).await.expect("submit");
    let msg = ReplicationMessage::ReplicateCommand {
        term,
        index: expected_index,
        command: cmd,
    };
    for f in followers {
        let _ = leader.send_to(f.engine.node_id(), &msg).await;
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
}

/// Wait until `predicate` returns true, polling every 50 ms, up to `timeout`.
async fn wait_until<F>(timeout: Duration, mut predicate: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    predicate()
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn case_a_election_and_replicate_5_commands() {
    let (nodes, base) = bring_up_three_nodes(38901).await;
    println!("[case A] bound on 127.0.0.1:{base}-{}", base + 2);
    let started = Instant::now();

    elect_leader(&nodes, 0, 1).await;
    assert_eq!(nodes[0].engine.role().await, ReplicationRole::Leader);
    assert_eq!(nodes[0].engine.term().await, 1);
    assert_eq!(nodes[1].engine.term().await, 1);
    assert_eq!(nodes[2].engine.term().await, 1);
    println!(
        "[case A] node-1 elected leader at term=1 ({} ms)",
        started.elapsed().as_millis()
    );

    let leader = &nodes[0];
    let followers = [&nodes[1], &nodes[2]];

    for i in 0..5 {
        let cmd = ClusterCommand::RegisterService(ServiceEntry {
            service_type: "broker".to_string(),
            host: "10.0.0.1".to_string(),
            port: 9092 + i,
            node_id: format!("svc-{i}"),
        });
        replicate(leader, &followers, cmd, (i + 1) as u64, 1).await;
    }

    let ok = wait_until(Duration::from_secs(3), || {
        nodes[0].cm.services("broker").len() == 5
            && nodes[1].cm.services("broker").len() == 5
            && nodes[2].cm.services("broker").len() == 5
    })
    .await;
    assert!(ok, "all 3 nodes should converge on 5 broker services");

    assert_eq!(nodes[0].engine.last_index().await, 5);
    println!(
        "[case A] PASS — replicated 5 commands in {} ms total",
        started.elapsed().as_millis()
    );

    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test]
#[ignore]
async fn case_b_leader_kill_and_failover() {
    let (mut nodes, base) = bring_up_three_nodes(38911).await;
    println!("[case B] bound on 127.0.0.1:{base}-{}", base + 2);
    let started = Instant::now();

    elect_leader(&nodes, 0, 1).await;
    let leader = &nodes[0];
    replicate(
        leader,
        &[&nodes[1], &nodes[2]],
        ClusterCommand::RegisterService(ServiceEntry {
            service_type: "broker".to_string(),
            host: "10.0.0.1".to_string(),
            port: 9092,
            node_id: "svc-pre".to_string(),
        }),
        1,
        1,
    )
    .await;

    // "Kill" node-1: shut it down.
    let killed = nodes.remove(0);
    let kill_t = Instant::now();
    killed.shutdown().await;
    println!(
        "[case B] killed node-1 in {} ms",
        kill_t.elapsed().as_millis()
    );

    // Surviving nodes are now nodes[0]=node-2 and nodes[1]=node-3.
    elect_leader(&nodes, 0, 2).await;
    assert_eq!(nodes[0].engine.role().await, ReplicationRole::Leader);
    assert_eq!(nodes[0].engine.term().await, 2);
    assert_eq!(nodes[1].engine.term().await, 2);
    println!(
        "[case B] node-2 elected at term=2 in {} ms",
        started.elapsed().as_millis()
    );

    let new_leader = &nodes[0];
    replicate(
        new_leader,
        &[&nodes[1]],
        ClusterCommand::AnnounceSegment(SegmentAnnouncement {
            segment_id: "seg-post-failover".to_string(),
            server_name: "hist-1".to_string(),
            data_source: "wiki".to_string(),
            tier: "_default_tier".to_string(),
        }),
        2,
        2,
    )
    .await;

    let ok = wait_until(Duration::from_secs(3), || {
        nodes[0].cm.segments("wiki").len() == 1 && nodes[1].cm.segments("wiki").len() == 1
    })
    .await;
    assert!(ok, "wiki segment should be replicated to surviving 2 nodes");
    println!(
        "[case B] PASS — failover + new write replicated in {} ms total",
        started.elapsed().as_millis()
    );

    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test]
#[ignore]
async fn case_c_restart_old_leader_catches_up() {
    let (mut nodes, base) = bring_up_three_nodes(38921).await;
    println!("[case C] bound on 127.0.0.1:{base}-{}", base + 2);
    let started = Instant::now();
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let a1 = SocketAddr::new(ip, base);
    let a2 = SocketAddr::new(ip, base + 1);
    let a3 = SocketAddr::new(ip, base + 2);

    elect_leader(&nodes, 0, 1).await;
    replicate(
        &nodes[0],
        &[&nodes[1], &nodes[2]],
        ClusterCommand::RegisterService(ServiceEntry {
            service_type: "broker".to_string(),
            host: "10.0.0.1".to_string(),
            port: 9092,
            node_id: "svc-pre".to_string(),
        }),
        1,
        1,
    )
    .await;

    let killed = nodes.remove(0);
    killed.shutdown().await;
    elect_leader(&nodes, 0, 2).await;
    let new_leader_idx = 0; // node-2
    let other_idx = 1; // node-3
    for i in 0..2 {
        replicate(
            &nodes[new_leader_idx],
            &[&nodes[other_idx]],
            ClusterCommand::AnnounceSegment(SegmentAnnouncement {
                segment_id: format!("seg-{i}"),
                server_name: "hist-1".to_string(),
                data_source: "wiki".to_string(),
                tier: "_default_tier".to_string(),
            }),
            (i + 2) as u64,
            2,
        )
        .await;
    }
    assert_eq!(nodes[other_idx].cm.segments("wiki").len(), 2);

    let mut peers1 = HashMap::new();
    peers1.insert("node-2".to_string(), a2);
    peers1.insert("node-3".to_string(), a3);
    let restart_t = Instant::now();
    let n1_new = TcpNode::spawn("node-1", a1, peers1).await;
    println!(
        "[case C] restarted node-1 in {} ms",
        restart_t.elapsed().as_millis()
    );

    let (snap, last_idx, term) = nodes[new_leader_idx].engine.get_snapshot().await;
    assert_eq!(last_idx, 3);
    assert_eq!(term, 2);
    n1_new
        .engine
        .restore_snapshot(snap, last_idx, term)
        .await
        .expect("restore");

    assert_eq!(n1_new.cm.segments("wiki").len(), 2);
    assert_eq!(n1_new.cm.services("broker").len(), 1);
    assert_eq!(n1_new.engine.last_index().await, 3);
    assert_eq!(n1_new.engine.term().await, 2);
    println!(
        "[case C] PASS — node-1 caught up in {} ms total",
        started.elapsed().as_millis()
    );

    n1_new.shutdown().await;
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test]
#[ignore]
async fn case_d_partition_minority_stuck_majority_progresses() {
    let (nodes, base) = bring_up_three_nodes(38931).await;
    println!("[case D] bound on 127.0.0.1:{base}-{}", base + 2);
    let started = Instant::now();

    elect_leader(&nodes, 0, 1).await;
    let leader = &nodes[0]; // node-1
    let majority_peer = &nodes[1]; // node-2
    let minority_peer = &nodes[2]; // node-3

    leader.partition_from(minority_peer.engine.node_id()).await;
    minority_peer.partition_from(leader.engine.node_id()).await;

    for i in 0..3 {
        let cmd = ClusterCommand::RegisterService(ServiceEntry {
            service_type: "broker".to_string(),
            host: "10.0.0.1".to_string(),
            port: 9092 + i,
            node_id: format!("svc-{i}"),
        });
        let _ = leader.engine.submit(cmd.clone()).await.expect("submit");
        let msg = ReplicationMessage::ReplicateCommand {
            term: 1,
            index: (i + 1) as u64,
            command: cmd,
        };
        let _ = leader.send_to(majority_peer.engine.node_id(), &msg).await;
        let send_minor = leader.send_to(minority_peer.engine.node_id(), &msg).await;
        assert!(
            send_minor.is_err(),
            "send to partitioned minority must fail (got Ok)"
        );
    }

    let progressed = wait_until(Duration::from_secs(3), || {
        leader.cm.services("broker").len() == 3 && majority_peer.cm.services("broker").len() == 3
    })
    .await;
    assert!(progressed, "majority side must converge on 3 services");

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        minority_peer.cm.services("broker").len(),
        0,
        "minority must not have applied any service"
    );
    println!(
        "[case D] PASS — majority @3 / minority @0 in {} ms total",
        started.elapsed().as_millis()
    );

    for n in nodes {
        n.shutdown().await;
    }
}

/// Wave 38-A regression for DD R1 vote-dedup High.
#[tokio::test]
#[ignore]
async fn case_e_byzantine_duplicate_votes() {
    use ferrodruid_cluster::replication::InMemoryTransport;

    let (nodes, base) = bring_up_three_nodes(38941).await;
    println!("[case E] bound on 127.0.0.1:{base}-{}", base + 2);
    let started = Instant::now();

    let transport = Arc::new(InMemoryTransport::new());
    let mut _rxs = Vec::new();
    for i in 1..=5 {
        _rxs.push(transport.register(&format!("node-{i}")).await);
    }

    let cm = Arc::new(ClusterManager::new_single_node(NodeInfo {
        id: "node-1".to_string(),
        host: "127.0.0.1".to_string(),
        port: base,
        role: NodeRole::AllInOne,
    }));
    let cfg = ReplicationConfig {
        node_id: "node-1".to_string(),
        listen_addr: format!("127.0.0.1:{base}"),
        peers: vec![
            "node-2".to_string(),
            "node-3".to_string(),
            "node-4".to_string(),
            "node-5".to_string(),
        ],
        heartbeat_interval_ms: 100,
        election_timeout_ms: 1000,
        cluster_security_hint: ferrodruid_cluster::replication::ClusterSecurityHint::Psk,
    };
    let engine = ReplicationEngine::with_transport(cfg, cm, Arc::clone(&transport));

    for _ in 0..5 {
        transport
            .send(
                "node-1",
                ReplicationMessage::VoteResponse {
                    voter_id: "node-2".to_string(),
                    term: 1,
                    granted: true,
                },
            )
            .await
            .expect("send");
    }

    let won = engine.start_election().await.expect("election");
    assert!(
        !won,
        "5-node cluster majority is 3; only 1 distinct peer voted, replayed 5x — must NOT elect leader",
    );
    assert_eq!(
        engine.role().await,
        ReplicationRole::Follower,
        "candidate must revert to follower on insufficient distinct votes",
    );
    assert_eq!(
        engine.votes_count(1).await,
        2,
        "self + 1 distinct peer (5 replays collapsed)",
    );
    println!(
        "[case E] PASS — 5 byzantine replays collapsed to 1 distinct vote in {} ms",
        started.elapsed().as_millis(),
    );

    for n in nodes {
        n.shutdown().await;
    }
}

/// Wave 38-C: heartbeat-driven failover end-to-end. Three TcpTransport-backed
/// engines start with NO `force_leader_with_term` cheat; the only way one of
/// them becomes leader is for its election timer to fire on its own and for
/// the other two to grant their votes back over the wire. Bound the wait at
/// 5 s of wall-clock — the default 1.5 s election timeout + jitter means
/// the first candidate normally fires within ~1.6 s, and a single retry on
/// split vote still completes well under 5 s.
#[tokio::test]
#[ignore]
async fn case_g_3_nodes_emerge_leader_via_timer_only() {
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

    // Bring up 3 nodes with the production tick loop. We aim for a
    // sub-second election so the case_g wall clock budget is small: a
    // 600 ms base timeout + up to 150 ms jitter.
    let mut chosen_base: Option<u16> = None;
    let mut nodes_opt: Option<Vec<TcpNode>> = None;
    for offset in [0u16, 10, 20, 30, 40, 50, 60] {
        let p1 = 38961 + offset;
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
        if probe.await.is_err() {
            continue;
        }

        let mut peers1 = HashMap::new();
        peers1.insert("node-2".to_string(), a2);
        peers1.insert("node-3".to_string(), a3);
        let mut peers2 = HashMap::new();
        peers2.insert("node-1".to_string(), a1);
        peers2.insert("node-3".to_string(), a3);
        let mut peers3 = HashMap::new();
        peers3.insert("node-1".to_string(), a1);
        peers3.insert("node-2".to_string(), a2);

        let n1 = TcpNode::spawn_with_scheduler("node-1", a1, peers1, 600, 100).await;
        let n2 = TcpNode::spawn_with_scheduler("node-2", a2, peers2, 600, 100).await;
        let n3 = TcpNode::spawn_with_scheduler("node-3", a3, peers3, 600, 100).await;
        chosen_base = Some(p1);
        nodes_opt = Some(vec![n1, n2, n3]);
        break;
    }
    let nodes = nodes_opt.expect("could not bind 3 ports for case_g");
    let base = chosen_base.expect("base port chosen");
    println!("[case G] bound on 127.0.0.1:{base}-{}", base + 2);
    let started = Instant::now();

    // Wait up to 5 s for any node to become leader and for the other two
    // to recognise the same leader's term.
    let mut leader_idx: Option<usize> = None;
    let deadline = Instant::now() + Duration::from_secs(5);
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
    let leader_idx = leader_idx.expect("no node became leader within 5s");
    let leader_term = nodes[leader_idx].engine.term().await;
    println!(
        "[case G] node-{} elected leader at term={} in {} ms",
        leader_idx + 1,
        leader_term,
        started.elapsed().as_millis(),
    );

    // Now properly check majority recognition asynchronously.
    let mut recognised = 1; // leader counts itself
    let recognition_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < recognition_deadline {
        let mut count = 1; // leader
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
        "majority of 3 (>= 2) must observe leader's term {leader_term} (saw {recognised})",
    );
    println!(
        "[case G] PASS — {recognised}/3 nodes recognise leader in {} ms total",
        started.elapsed().as_millis(),
    );

    for n in nodes {
        n.shutdown().await;
    }
}

/// Wave 40-A regression for the W39 NEW Critical + High findings.
///
/// Brings up the same 3-node cluster as `case_g_3_nodes_emerge_leader_via_timer_only`
/// but assert that the cluster forms with the production PSK-authenticated
/// wire path active. All three nodes share one PSK (the harness one); a
/// network adversary using a different PSK would be unable to inject any
/// frame that the listener tasks accepted (covered separately by the
/// transport unit tests `transport_rejects_message_with_invalid_hmac` /
/// `transport_rejects_handshake_with_mismatched_hmac`).
#[tokio::test]
#[ignore]
async fn case_h_3_nodes_form_cluster_with_psk_auth() {
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

    let mut chosen_base: Option<u16> = None;
    let mut nodes_opt: Option<Vec<TcpNode>> = None;
    for offset in [0u16, 10, 20, 30, 40, 50, 60] {
        let p1 = 38981 + offset;
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
        if probe.await.is_err() {
            continue;
        }

        let mut peers1 = HashMap::new();
        peers1.insert("node-2".to_string(), a2);
        peers1.insert("node-3".to_string(), a3);
        let mut peers2 = HashMap::new();
        peers2.insert("node-1".to_string(), a1);
        peers2.insert("node-3".to_string(), a3);
        let mut peers3 = HashMap::new();
        peers3.insert("node-1".to_string(), a1);
        peers3.insert("node-2".to_string(), a2);

        let n1 = TcpNode::spawn_with_scheduler("node-1", a1, peers1, 600, 100).await;
        let n2 = TcpNode::spawn_with_scheduler("node-2", a2, peers2, 600, 100).await;
        let n3 = TcpNode::spawn_with_scheduler("node-3", a3, peers3, 600, 100).await;
        chosen_base = Some(p1);
        nodes_opt = Some(vec![n1, n2, n3]);
        break;
    }
    let nodes = nodes_opt.expect("could not bind 3 ports for case_h");
    let base = chosen_base.expect("base port chosen");
    println!(
        "[case H / Wave 40-A] PSK-authenticated cluster on 127.0.0.1:{base}-{}",
        base + 2,
    );
    let started = Instant::now();

    let mut leader_idx: Option<usize> = None;
    let deadline = Instant::now() + Duration::from_secs(5);
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
    let leader_idx = leader_idx.expect("no node became leader within 5s under PSK auth");
    let leader_term = nodes[leader_idx].engine.term().await;
    let mut count = 1; // leader
    let recognition_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < recognition_deadline {
        let mut c = 1;
        for (i, n) in nodes.iter().enumerate() {
            if i == leader_idx {
                continue;
            }
            if n.engine.term().await >= leader_term {
                c += 1;
            }
        }
        if c >= 2 {
            count = c;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        count >= 2,
        "majority of 3 must observe leader's term {leader_term} under PSK auth (saw {count})",
    );
    println!(
        "[case H] PASS — {count}/3 nodes recognise leader in {} ms (PSK active)",
        started.elapsed().as_millis(),
    );

    for n in nodes {
        n.shutdown().await;
    }
}

/// Wave 38-A regression for DD R1 log-monotonicity High.
#[tokio::test]
#[ignore]
async fn case_f_reordered_append_entries_does_not_corrupt_log() {
    let (nodes, base) = bring_up_three_nodes(38951).await;
    println!("[case F] bound on 127.0.0.1:{base}-{}", base + 2);
    let started = Instant::now();

    elect_leader(&nodes, 0, 1).await;
    let leader = &nodes[0];
    let followers = [&nodes[1], &nodes[2]];

    let cmd1 = ClusterCommand::RegisterService(ServiceEntry {
        service_type: "broker".to_string(),
        host: "10.0.0.1".to_string(),
        port: 9092,
        node_id: "svc-1".to_string(),
    });
    replicate(leader, &followers, cmd1.clone(), 1, 1).await;
    for f in &followers {
        assert_eq!(f.engine.last_index().await, 1);
        assert_eq!(f.cm.services("broker").len(), 1);
    }

    let cmd_gap = ClusterCommand::RegisterService(ServiceEntry {
        service_type: "broker".to_string(),
        host: "10.0.0.99".to_string(),
        port: 9099,
        node_id: "svc-gap".to_string(),
    });
    let gap_msg = ReplicationMessage::ReplicateCommand {
        term: 1,
        index: 5,
        command: cmd_gap.clone(),
    };
    for f in &followers {
        let _ = leader.send_to(f.engine.node_id(), &gap_msg).await;
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    for f in &followers {
        assert_eq!(
            f.engine.last_index().await,
            1,
            "follower last_index must NOT jump on gap append",
        );
        assert_eq!(
            f.cm.services("broker").len(),
            1,
            "follower must NOT apply gap-append command",
        );
    }

    let replay_msg = ReplicationMessage::ReplicateCommand {
        term: 1,
        index: 1,
        command: cmd1.clone(),
    };
    for _ in 0..3 {
        for f in &followers {
            let _ = leader.send_to(f.engine.node_id(), &replay_msg).await;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    for f in &followers {
        assert_eq!(
            f.cm.services("broker").len(),
            1,
            "replay of already-applied index must not re-apply",
        );
        assert_eq!(f.engine.last_index().await, 1);
    }

    println!(
        "[case F] PASS — gap rejected + replay idempotent in {} ms",
        started.elapsed().as_millis(),
    );

    for n in nodes {
        n.shutdown().await;
    }
}

/// Wave 47-A regression: leader-side tick-driven replay scan.
///
/// Closes the W38-DE honest gap "replay loop is engine-side only" — the
/// transport tick loop now calls [`ReplicationEngine::build_replay_actions`]
/// every Nth tick (~500 ms cadence) and back-fills any lagging follower
/// without manual operator intervention.
///
/// Scenario:
/// 1. Bring up a 3-node cluster with the production tick scheduler.
/// 2. Force node-1 leader at term=1 and replicate 4 entries to all
///    followers (manual hand-replication so we know the steady state).
/// 3. Truncate node-2's log back to `last_index = 1` to simulate a
///    follower that came back from a crash with a stale tail.
/// 4. Send a `ReplicateAck { success=false, hint=1 }` from node-2 so the
///    leader's `next_index[node-2]` walks back to 2.
/// 5. Wait up to 5 s for the leader's tick-driven replay scan to push
///    indices 2..=4 into node-2 — node-2 must observe `last_index = 4`
///    again WITHOUT any further manual hand-off from the test harness.
#[tokio::test]
#[ignore]
async fn case_l_leader_auto_replays_to_lagging_follower() {
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

    // Bind 3 ports for the scheduled cluster — same dance as case_g.
    let mut chosen_base: Option<u16> = None;
    let mut nodes_opt: Option<Vec<TcpNode>> = None;
    for offset in [0u16, 10, 20, 30, 40, 50, 60] {
        let p1 = 39001 + offset;
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
        if probe.await.is_err() {
            continue;
        }

        let mut peers1 = HashMap::new();
        peers1.insert("node-2".to_string(), a2);
        peers1.insert("node-3".to_string(), a3);
        let mut peers2 = HashMap::new();
        peers2.insert("node-1".to_string(), a1);
        peers2.insert("node-3".to_string(), a3);
        let mut peers3 = HashMap::new();
        peers3.insert("node-1".to_string(), a1);
        peers3.insert("node-2".to_string(), a2);

        // Use a long election timeout so the manually-installed leader
        // is not pre-empted by an over-eager pre-vote round during the
        // replay window.  Tick cadence is 50 ms (hard-wired in
        // spawn_with_scheduler), so REPLAY_TICK_INTERVAL=10 means the
        // replay scan fires every ~500 ms.
        let n1 = TcpNode::spawn_with_scheduler("node-1", a1, peers1, 5_000, 100).await;
        let n2 = TcpNode::spawn_with_scheduler("node-2", a2, peers2, 5_000, 100).await;
        let n3 = TcpNode::spawn_with_scheduler("node-3", a3, peers3, 5_000, 100).await;
        chosen_base = Some(p1);
        nodes_opt = Some(vec![n1, n2, n3]);
        break;
    }
    let nodes = nodes_opt.expect("could not bind 3 ports for case_l");
    let base = chosen_base.expect("base port chosen");
    println!("[case L] bound on 127.0.0.1:{base}-{}", base + 2);
    let started = Instant::now();

    // Force node-1 leader at term=1 and have it broadcast a heartbeat so
    // followers learn the term.
    nodes[0].engine.force_leader_with_term(1).await;
    let hb = ReplicationMessage::Heartbeat {
        leader_id: "node-1".to_string(),
        term: 1,
    };
    for i in 1..=2 {
        let _ = nodes[0].send_to(nodes[i].engine.node_id(), &hb).await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Replicate 4 entries to all followers via the harness `replicate`
    // helper so the steady-state has every node at last_index = 4.
    let leader = &nodes[0];
    let followers = [&nodes[1], &nodes[2]];
    for i in 0..4u32 {
        let cmd = ClusterCommand::AnnounceSegment(SegmentAnnouncement {
            segment_id: format!("seg-{i}"),
            server_name: "hist-1".to_string(),
            data_source: "wiki".to_string(),
            tier: "_default_tier".to_string(),
        });
        replicate(leader, &followers, cmd, (i + 1) as u64, 1).await;
    }
    let converged = wait_until(Duration::from_secs(3), || {
        // Synchronous closure — best-effort: we accept either the cm
        // count having converged or rely on the explicit last_index
        // check below.  Here we just wait long enough for the
        // hand-replicated frames to settle.
        true
    })
    .await;
    let _ = converged;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(nodes[0].engine.last_index().await, 4);
    let f1_last = nodes[1].engine.last_index().await;
    let f2_last = nodes[2].engine.last_index().await;
    assert_eq!(
        f1_last, 4,
        "node-2 must converge to last_index=4 in steady state"
    );
    assert_eq!(
        f2_last, 4,
        "node-3 must converge to last_index=4 in steady state"
    );
    println!(
        "[case L] steady state reached in {} ms",
        started.elapsed().as_millis(),
    );

    // Simulate node-2 coming back from a crash with a truncated log:
    // drop everything from index 2 onwards on the engine.
    nodes[1].engine.truncate_log_for_test(2).await;
    assert_eq!(
        nodes[1].engine.last_index().await,
        1,
        "node-2 must be at last_index=1 after truncation",
    );

    // Send a fake `ReplicateAck { success=false, hint=1 }` from node-2
    // so the leader's `next_index[node-2]` walks back to 2 — this is
    // what would happen organically when node-2 rejects the next
    // AppendEntries it sees because of the gap (case_f exercises that
    // path).  Here we shortcut it so the leader's tick-driven replay
    // is what gets isolated as the back-fill mechanism.
    let reject = ReplicationMessage::ReplicateAck {
        follower_id: "node-2".to_string(),
        index: 4,
        success: false,
        last_log_index_hint: Some(1),
    };
    let _ = nodes[1].send_to(nodes[0].engine.node_id(), &reject).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let next = leader.engine.next_index_for("node-2").await;
    println!("[case L] after rejection, leader next_index[node-2] = {next:?}");

    // Now wait for the tick-driven replay scan (every 10 ticks at 50 ms
    // = ~500 ms cadence) to back-fill node-2.  Allow up to 5 s for
    // generous CI timing — production cadence guarantees a single scan
    // completes within ~600 ms.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_seen: u64 = 0;
    while Instant::now() < deadline {
        last_seen = nodes[1].engine.last_index().await;
        if last_seen >= 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        last_seen >= 4,
        "leader's tick-driven replay must back-fill node-2 to last_index>=4 (saw {last_seen})",
    );
    println!(
        "[case L] PASS — node-2 back-filled to last_index={last_seen} in {} ms total",
        started.elapsed().as_millis(),
    );

    for n in nodes {
        n.shutdown().await;
    }
}
