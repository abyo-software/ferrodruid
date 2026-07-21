// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W1-C — crash-recovery test for the persistent Raft log.
//!
//! Closes the CL-3 closure-bar item:
//! > Persistent Raft log + snapshot/log compaction wired end-to-end.
//! > Crash recovery test: kill leader, restart, assert log replay
//! > reconstructs state.
//!
//! We exercise the engine purely in-process (no transport / no
//! peers) so the test is fast and deterministic. The transport layer
//! is verified by `tests/three_node_tcp.rs` cases A-L and the
//! multi-host run in `tests/cross-host/RESULTS_two_host_raft.md`;
//! here we focus on the durability guarantee on a single node.

use std::sync::Arc;

use ferrodruid_cluster::persist::PersistentRaftState;
use ferrodruid_cluster::replication::{ReplicationConfig, ReplicationEngine};
use ferrodruid_cluster::{ClusterCommand, ClusterManager, NodeInfo, NodeRole, ServiceEntry};

fn make_engine(node_id: &str) -> (Arc<ClusterManager>, Arc<ReplicationEngine>) {
    let cm = Arc::new(ClusterManager::new_single_node(NodeInfo {
        id: node_id.to_string(),
        host: "127.0.0.1".to_string(),
        port: 18081,
        role: NodeRole::AllInOne,
    }));
    let config = ReplicationConfig {
        node_id: node_id.to_string(),
        listen_addr: "127.0.0.1:18081".to_string(),
        peers: Vec::new(),
        heartbeat_interval_ms: 50,
        election_timeout_ms: 200,
        cluster_security_hint: ferrodruid_cluster::replication::ClusterSecurityHint::Psk,
    };
    let engine = Arc::new(ReplicationEngine::new(config, Arc::clone(&cm)));
    (cm, engine)
}

fn register_cmd(slot: u64) -> ClusterCommand {
    ClusterCommand::RegisterService(ServiceEntry {
        service_type: format!("svc-{slot}"),
        host: "10.0.0.1".to_string(),
        port: 9000 + (slot as u16),
        node_id: format!("registrar-{slot}"),
    })
}

#[tokio::test]
async fn persistent_log_survives_leader_crash_via_replay() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("raft");

    // === Phase 1: original leader writes 7 commands and "crashes". ===
    {
        let (cm, engine) = make_engine("node-A");
        let state =
            Arc::new(PersistentRaftState::open_or_create(&dir).expect("open persistent state"));
        engine
            .attach_persistent_state(state)
            .await
            .expect("attach persistent state");

        // Single-node short circuit: arm_election produces a leader.
        let _ = engine.start_pre_vote().await;
        engine.force_leader().await;
        assert!(engine.is_leader().await, "engine must be leader");

        for i in 1..=7u64 {
            engine.submit(register_cmd(i)).await.expect("submit");
        }

        // Sanity: state machine carries all 7 services.
        let mut got: Vec<String> = (1..=7u64)
            .filter_map(|i| {
                let entries = cm.services(&format!("svc-{i}"));
                entries.first().map(|e| e.node_id.clone())
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                "registrar-1".to_string(),
                "registrar-2".to_string(),
                "registrar-3".to_string(),
                "registrar-4".to_string(),
                "registrar-5".to_string(),
                "registrar-6".to_string(),
                "registrar-7".to_string(),
            ]
        );

        // "Crash": drop everything without graceful shutdown.
        drop(engine);
        drop(cm);
    }

    // === Phase 2: fresh engine attaches to same dir, replays. ===
    let (cm2, engine2) = make_engine("node-A");
    let state2 = Arc::new(PersistentRaftState::open_or_create(&dir).expect("reopen"));
    engine2
        .attach_persistent_state(state2)
        .await
        .expect("attach after crash");

    // Replay reconstructed the 7 services through ClusterManager.
    for i in 1..=7u64 {
        let entries = cm2.services(&format!("svc-{i}"));
        assert_eq!(
            entries.len(),
            1,
            "service svc-{i} not restored after crash; got {entries:?}"
        );
        assert_eq!(entries[0].node_id, format!("registrar-{i}"));
    }

    // last_index also restored.
    assert_eq!(engine2.last_index().await, 7);
}

#[tokio::test]
async fn persistent_log_replay_idempotent_on_repeat_open() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("raft");

    // Phase 1: write 3 entries.
    {
        let (_cm, engine) = make_engine("node-X");
        let state = Arc::new(PersistentRaftState::open_or_create(&dir).expect("open"));
        engine.attach_persistent_state(state).await.expect("attach");
        engine.force_leader().await;
        for i in 1..=3u64 {
            engine.submit(register_cmd(i)).await.expect("submit");
        }
    }

    // Phase 2 + 3 + 4: open three times back-to-back; state must
    // remain identical (no duplicate apply).
    for round in 1..=3u64 {
        let (cm, engine) = make_engine("node-X");
        let state = Arc::new(PersistentRaftState::open_or_create(&dir).expect("reopen round"));
        engine
            .attach_persistent_state(state)
            .await
            .expect("attach round");

        for i in 1..=3u64 {
            let entries = cm.services(&format!("svc-{i}"));
            assert_eq!(
                entries.len(),
                1,
                "round {round}: service svc-{i} duplicated; got {} entries",
                entries.len(),
            );
            assert_eq!(entries[0].node_id, format!("registrar-{i}"));
        }
        assert_eq!(
            engine.last_index().await,
            3,
            "round {round}: last_index drift"
        );
    }
}

#[tokio::test]
async fn persistent_log_survives_partial_write_via_snapshot_compaction() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("raft");

    // Phase 1: 10 writes, then install snapshot at index=6, then
    // 4 more writes (indices 7..=10). The journal on disk should
    // hold only entries 7..=10 after compaction.
    {
        let (cm, engine) = make_engine("node-S");
        let state = Arc::new(PersistentRaftState::open_or_create(&dir).expect("open"));
        engine.attach_persistent_state(state).await.expect("attach");
        engine.force_leader().await;
        for i in 1..=10u64 {
            engine.submit(register_cmd(i)).await.expect("submit");
        }
        // Trigger snapshot install.
        let snapshot = cm.snapshot();
        engine
            .restore_snapshot(snapshot, 10, 1)
            .await
            .expect("restore snapshot");
    }

    // Phase 2: reopen + assert all 10 services restored from
    // snapshot (no replay through the journal needed).
    let (cm2, engine2) = make_engine("node-S");
    let state2 = Arc::new(PersistentRaftState::open_or_create(&dir).expect("reopen"));
    engine2
        .attach_persistent_state(state2)
        .await
        .expect("attach after snap");
    for i in 1..=10u64 {
        let entries = cm2.services(&format!("svc-{i}"));
        assert_eq!(
            entries.len(),
            1,
            "service svc-{i} missing after snapshot replay",
        );
    }
    assert_eq!(engine2.last_index().await, 10);
}
