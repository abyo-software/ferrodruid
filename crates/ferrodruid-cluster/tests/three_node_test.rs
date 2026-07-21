// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Integration tests for 3-node cluster replication.

use std::sync::Arc;

use ferrodruid_cluster::replication::{
    InMemoryTransport, ReplicationConfig, ReplicationEngine, ReplicationMessage, ReplicationRole,
};
use ferrodruid_cluster::{
    ClusterCommand, ClusterManager, CommandResult, NodeInfo, NodeRole, SegmentAnnouncement,
    ServiceEntry,
};

fn make_node(id: &str) -> NodeInfo {
    NodeInfo {
        id: id.to_string(),
        host: "127.0.0.1".to_string(),
        port: 8888,
        role: NodeRole::AllInOne,
    }
}

fn make_cm(id: &str) -> Arc<ClusterManager> {
    Arc::new(ClusterManager::new_single_node(make_node(id)))
}

fn make_config(node_id: &str, peers: &[&str]) -> ReplicationConfig {
    ReplicationConfig {
        node_id: node_id.to_string(),
        listen_addr: "127.0.0.1:0".to_string(),
        peers: peers.iter().map(|s| s.to_string()).collect(),
        heartbeat_interval_ms: 100,
        election_timeout_ms: 300,
        cluster_security_hint: ferrodruid_cluster::replication::ClusterSecurityHint::Psk,
    }
}

/// Helper: set up a 3-node cluster with in-memory transport. Returns
/// `(engines, cluster_managers, transport, receivers)`.
/// The receivers must be kept alive to prevent channel closure.
async fn setup_three_nodes() -> (
    [ReplicationEngine; 3],
    [Arc<ClusterManager>; 3],
    Arc<InMemoryTransport>,
    [tokio::sync::mpsc::UnboundedReceiver<ReplicationMessage>; 3],
) {
    let transport = Arc::new(InMemoryTransport::new());
    let rx1 = transport.register("node-1").await;
    let rx2 = transport.register("node-2").await;
    let rx3 = transport.register("node-3").await;

    let cm1 = make_cm("node-1");
    let cm2 = make_cm("node-2");
    let cm3 = make_cm("node-3");

    let e1 = ReplicationEngine::with_transport(
        make_config("node-1", &["node-2", "node-3"]),
        Arc::clone(&cm1),
        Arc::clone(&transport),
    );
    let e2 = ReplicationEngine::with_transport(
        make_config("node-2", &["node-1", "node-3"]),
        Arc::clone(&cm2),
        Arc::clone(&transport),
    );
    let e3 = ReplicationEngine::with_transport(
        make_config("node-3", &["node-1", "node-2"]),
        Arc::clone(&cm3),
        Arc::clone(&transport),
    );

    ([e1, e2, e3], [cm1, cm2, cm3], transport, [rx1, rx2, rx3])
}

#[tokio::test]
async fn three_node_leader_election() {
    let (engines, _cms, transport, _rxs) = setup_three_nodes().await;
    let [ref e1, ref e2, ref e3] = engines;

    // Node-2 and node-3 grant their votes to node-1 for term 1.
    let resp2 = e2.receive_vote_request("node-1", 1).await;
    let resp3 = e3.receive_vote_request("node-1", 1).await;
    transport.send("node-1", resp2).await.expect("send");
    transport.send("node-1", resp3).await.expect("send");

    let won = e1.start_election().await.expect("election");
    assert!(won, "node-1 should win election with 3/3 votes");
    assert_eq!(e1.role().await, ReplicationRole::Leader);
    assert_eq!(e1.term().await, 1);
}

#[tokio::test]
async fn three_node_command_replication() {
    let (engines, cms, _transport, _rxs) = setup_three_nodes().await;
    let [ref e1, ref e2, ref e3] = engines;
    let [ref cm1, ref cm2, ref cm3] = cms;

    // Make node-1 leader at term 1.
    e1.force_leader_with_term(1).await;

    // Submit a command on the leader.
    let cmd = ClusterCommand::RegisterService(ServiceEntry {
        service_type: "broker".to_string(),
        host: "10.0.0.1".to_string(),
        port: 9092,
        node_id: "svc-1".to_string(),
    });
    let result = e1.submit(cmd.clone()).await.expect("submit");
    assert_eq!(result, CommandResult::Ok);

    // Simulate followers receiving the replicated command.
    e2.receive_command(1, 1, cmd.clone())
        .await
        .expect("e2 apply");
    e3.receive_command(1, 1, cmd).await.expect("e3 apply");

    // All 3 nodes should have the service.
    assert_eq!(cm1.services("broker").len(), 1);
    assert_eq!(cm2.services("broker").len(), 1);
    assert_eq!(cm3.services("broker").len(), 1);
}

#[tokio::test]
async fn three_node_leader_failover() {
    let (engines, _cms, transport, _rxs) = setup_three_nodes().await;
    let [ref e1, ref e2, ref e3] = engines;

    // Node-1 is leader at term 1. Followers also know about term 1.
    e1.force_leader_with_term(1).await;
    e2.receive_heartbeat("node-1", 1).await.expect("hb");
    e3.receive_heartbeat("node-1", 1).await.expect("hb");

    // Simulate node-1 "crashing" (stop sending heartbeats).
    // Node-2 decides to start an election. Its term goes from 1 -> 2.

    // Node-3 grants vote to node-2 for term 2.
    let resp3 = e3.receive_vote_request("node-2", 2).await;
    transport.send("node-2", resp3).await.expect("send");

    // Node-1 (crashed) doesn't respond, but node-2 has majority (2/3).
    let won = e2.start_election().await.expect("election");
    assert!(won, "node-2 should win with 2/3 votes (self + node-3)");
    assert_eq!(e2.role().await, ReplicationRole::Leader);
    assert_eq!(e2.term().await, 2);

    // When node-1 "recovers" and gets a heartbeat, it steps down.
    e1.receive_heartbeat("node-2", 2).await.expect("hb");
    assert_eq!(e1.role().await, ReplicationRole::Follower);
    assert_eq!(e1.term().await, 2);
}

#[tokio::test]
async fn three_node_snapshot_join() {
    let (engines, cms, _transport, _rxs) = setup_three_nodes().await;
    let [ref e1, _ref_e2, ref e3] = engines;
    let [ref _cm1, _ref_cm2, ref cm3] = cms;

    // Node-1 is leader, has some state.
    e1.force_leader_with_term(1).await;
    e1.submit(ClusterCommand::RegisterService(ServiceEntry {
        service_type: "broker".to_string(),
        host: "10.0.0.1".to_string(),
        port: 9092,
        node_id: "svc-1".to_string(),
    }))
    .await
    .expect("submit");

    e1.submit(ClusterCommand::AnnounceSegment(SegmentAnnouncement {
        segment_id: "seg-001".to_string(),
        server_name: "hist-1".to_string(),
        data_source: "wiki".to_string(),
        tier: "_default_tier".to_string(),
    }))
    .await
    .expect("submit");

    // Node-3 joins late and requests a snapshot.
    let (snapshot, last_idx, term) = e1.get_snapshot().await;
    assert_eq!(last_idx, 2);
    assert_eq!(term, 1);

    // Node-3 restores the snapshot.
    e3.restore_snapshot(snapshot, last_idx, term)
        .await
        .expect("restore");

    // Verify node-3 caught up.
    assert_eq!(cm3.services("broker").len(), 1);
    assert_eq!(cm3.segments("wiki").len(), 1);
    assert_eq!(e3.last_index().await, 2);
    assert_eq!(e3.term().await, 1);
}
