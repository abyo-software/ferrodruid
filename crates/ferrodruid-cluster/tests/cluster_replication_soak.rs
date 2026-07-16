// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 53 — 3-node replication soak over [`InMemoryTransport`].
//!
//! Submits 1_000 distinct [`ClusterCommand::RegisterService`] entries
//! through the leader, drains follower inboxes after each batch to
//! drive [`ReplicationEngine::receive_command`], and asserts that:
//!
//! * the leader's `last_index` advances to 1_000;
//! * every follower's `last_index` reaches 1_000;
//! * every node's [`ClusterManager::snapshot`] is structurally
//!   equivalent (services map keyed by service-id) to the leader's;
//! * the entire cycle completes within a 30-second wall-clock budget.
//!
//! The soak is `#[ignore]`d so the default fast-CI lane stays under a
//! few seconds.  Run with:
//!
//! ```text
//! cargo test -p ferrodruid-cluster --test cluster_replication_soak \
//!     -- --ignored --nocapture
//! ```

#![allow(missing_docs)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodruid_cluster::replication::{
    InMemoryTransport, ReplicationConfig, ReplicationEngine, ReplicationMessage,
};
use ferrodruid_cluster::{ClusterCommand, ClusterManager, NodeInfo, NodeRole, ServiceEntry};

const SOAK_COMMANDS: usize = 1_000;
const SOAK_BUDGET_SECS: u64 = 30;

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

/// Drain follower inbox via the transport and dispatch every
/// `ReplicateCommand` frame into [`ReplicationEngine::receive_command`].
/// Returns the number of frames applied (used for assertions only).
async fn pump_follower(
    follower: &ReplicationEngine,
    follower_id: &str,
    transport: &InMemoryTransport,
) -> usize {
    let mut applied = 0usize;
    if let Some(msgs) = transport.try_receive(follower_id).await {
        for msg in msgs {
            if let ReplicationMessage::ReplicateCommand {
                term,
                index,
                command,
            } = msg
            {
                // Idempotent on already-applied indices; surfaces stale
                // term as Err which the soak tolerates and counts.
                if follower.receive_command(term, index, command).await.is_ok() {
                    applied += 1;
                }
            }
        }
    }
    applied
}

#[tokio::test]
#[ignore]
async fn replication_soak_1000_commands_3_nodes() {
    let started = Instant::now();
    let transport = Arc::new(InMemoryTransport::new());
    let _rx1 = transport.register("node-1").await;
    let _rx2 = transport.register("node-2").await;
    let _rx3 = transport.register("node-3").await;

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

    // Make node-1 leader at term 1.
    e1.force_leader_with_term(1).await;

    // Submit 1_000 commands.  After each submit, drain the leader
    // inbox to feed it any pre-staged ReplicateAck frames (none in
    // this harness — followers don't auto-ack), then drain the
    // follower inboxes to drive their state machine forward.
    //
    // We use `submit` (the legacy 0-ms-timeout helper) so the leader
    // never blocks waiting for follower acks the harness doesn't
    // send.  The leader's CommandLog still grows by 1 per call.
    let mut leader_ok = 0usize;
    let mut follower_e2_applied = 0usize;
    let mut follower_e3_applied = 0usize;
    for i in 0..SOAK_COMMANDS {
        // Catch the budget early: if we're already over 30 s we want
        // a clear error rather than a vague timeout under heavy CI.
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(SOAK_BUDGET_SECS),
            "soak exceeded {SOAK_BUDGET_SECS}s budget at iter {i} ({elapsed:?})",
        );

        let cmd = ClusterCommand::RegisterService(ServiceEntry {
            service_type: "broker".to_string(),
            host: "10.0.0.1".to_string(),
            port: 9092,
            // Distinct node_id per command so the snapshot grows
            // monotonically instead of dedup-collapsing.
            node_id: format!("svc-{i:04}"),
        });

        // Best-effort submit.  We tolerate Err (e.g. the legacy
        // submit() returns NotLeader briefly during state transitions
        // — never expected in this harness because we forced the
        // leader, but defensive).
        if e1.submit(cmd).await.is_ok() {
            leader_ok += 1;
        }

        // Drive followers.
        follower_e2_applied += pump_follower(&e2, "node-2", &transport).await;
        follower_e3_applied += pump_follower(&e3, "node-3", &transport).await;
    }

    // Final budget assertion before we start checking equivalence.
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(SOAK_BUDGET_SECS),
        "soak completed but exceeded {SOAK_BUDGET_SECS}s budget: {elapsed:?}",
    );

    // Leader must have committed all SOAK_COMMANDS into its log.
    assert_eq!(
        leader_ok, SOAK_COMMANDS,
        "leader.submit() must succeed on every iteration",
    );
    assert_eq!(
        e1.last_index().await,
        SOAK_COMMANDS as u64,
        "leader last_index must advance to {SOAK_COMMANDS}",
    );

    // Followers must have applied every replicate frame the leader
    // emitted (= SOAK_COMMANDS).
    assert_eq!(
        follower_e2_applied, SOAK_COMMANDS,
        "node-2 must have received and applied all {SOAK_COMMANDS} replicate frames",
    );
    assert_eq!(
        follower_e3_applied, SOAK_COMMANDS,
        "node-3 must have received and applied all {SOAK_COMMANDS} replicate frames",
    );
    assert_eq!(
        e2.last_index().await,
        SOAK_COMMANDS as u64,
        "follower e2 last_index must reach {SOAK_COMMANDS}",
    );
    assert_eq!(
        e3.last_index().await,
        SOAK_COMMANDS as u64,
        "follower e3 last_index must reach {SOAK_COMMANDS}",
    );

    // Cluster manager state machines must agree.  We compare via the
    // services list because that is what `RegisterService` mutates;
    // the snapshot's `leader` field intentionally differs per node
    // (each `ClusterManager::new_single_node` self-elects), so we
    // compare just the broker service set.
    let s1: Vec<String> = cm1
        .services("broker")
        .iter()
        .map(|s| s.node_id.clone())
        .collect();
    let s2: Vec<String> = cm2
        .services("broker")
        .iter()
        .map(|s| s.node_id.clone())
        .collect();
    let s3: Vec<String> = cm3
        .services("broker")
        .iter()
        .map(|s| s.node_id.clone())
        .collect();
    assert_eq!(s1.len(), SOAK_COMMANDS, "leader services map size");
    assert_eq!(s2.len(), SOAK_COMMANDS, "follower e2 services map size");
    assert_eq!(s3.len(), SOAK_COMMANDS, "follower e3 services map size");

    // Sort to make the equality check order-insensitive in case
    // future ClusterManager internals re-order on insert.
    let mut s1_sorted = s1.clone();
    s1_sorted.sort();
    let mut s2_sorted = s2.clone();
    s2_sorted.sort();
    let mut s3_sorted = s3.clone();
    s3_sorted.sort();
    assert_eq!(s1_sorted, s2_sorted, "leader and e2 must agree on services");
    assert_eq!(s1_sorted, s3_sorted, "leader and e3 must agree on services");

    println!(
        "[soak] OK — {SOAK_COMMANDS} commands replicated to 3 nodes in {elapsed:?} \
         (leader_ok={leader_ok}, e2_applied={follower_e2_applied}, \
         e3_applied={follower_e3_applied})",
    );
}
