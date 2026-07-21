// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W1-C / CL-A4 — chunked + resumable snapshot transfer test.
//!
//! Closes the CL-3 closure-bar item:
//! > Chunked / resumable snapshot transfer (CL-A4 closure).
//! > Regression test: kill snapshot transfer mid-stream, verify
//! > resume completes successfully.
//!
//! This test exercises the engine-level chunking + reassembly path
//! directly. The transport layer just forwards SnapshotChunk /
//! SnapshotChunkAck frames; the resume semantics live entirely in
//! `ReplicationEngine` (per-follower cursor on the leader side,
//! per-transfer reassembly buffer on the follower side, both keyed
//! on `transfer_id` so a fresh transfer evicts a stale partial
//! buffer).

use std::sync::Arc;

use ferrodruid_cluster::replication::{
    DEFAULT_SNAPSHOT_CHUNK_SIZE, ReplicationConfig, ReplicationEngine, ReplicationMessage,
};
use ferrodruid_cluster::{ClusterCommand, ClusterManager, NodeInfo, NodeRole, ServiceEntry};

fn make_engine(node_id: &str, peers: Vec<&str>) -> Arc<ReplicationEngine> {
    let cm = Arc::new(ClusterManager::new_single_node(NodeInfo {
        id: node_id.to_string(),
        host: "127.0.0.1".to_string(),
        port: 0,
        role: NodeRole::AllInOne,
    }));
    let config = ReplicationConfig {
        node_id: node_id.to_string(),
        listen_addr: "127.0.0.1:0".to_string(),
        peers: peers.into_iter().map(String::from).collect(),
        heartbeat_interval_ms: 100,
        election_timeout_ms: 1000,
        cluster_security_hint: ferrodruid_cluster::replication::ClusterSecurityHint::Psk,
    };
    Arc::new(ReplicationEngine::new(config, cm))
}

/// Pump enough RegisterService writes through the leader so the
/// snapshot JSON exceeds `target_bytes` and therefore needs >1
/// chunk at the default 256 KiB chunk size.
async fn populate_leader_until(leader: &Arc<ReplicationEngine>, target_bytes: usize) {
    let mut i = 0u64;
    loop {
        leader
            .submit(ClusterCommand::RegisterService(ServiceEntry {
                service_type: format!("svc-{}", i % 16),
                host: format!("10.0.0.{}", i % 250),
                port: 9000 + (i as u16 % 1000),
                node_id: format!("registrar-{i:06}"),
            }))
            .await
            .expect("submit");
        i += 1;
        if i.is_multiple_of(100) {
            let (snap, _, _) = leader.get_snapshot().await;
            let serialised = serde_json::to_vec(&snap).expect("encode");
            if serialised.len() >= target_bytes {
                return;
            }
        }
        if i > 200_000 {
            panic!(
                "populate_leader_until: snapshot never grew large enough (capped at 200k entries)"
            );
        }
    }
}

#[tokio::test]
async fn chunked_snapshot_transfer_round_trip() {
    let leader = make_engine("leader", vec!["follower"]);
    leader.force_leader().await;
    // Populate so the snapshot needs multiple chunks at the default
    // 256 KiB chunk size.
    populate_leader_until(&leader, 600 * 1024).await;

    let follower = make_engine("follower", vec!["leader"]);

    let transfer_id = leader.next_snapshot_transfer_id().await;
    let chunks = leader
        .build_snapshot_chunks(transfer_id, 0, DEFAULT_SNAPSHOT_CHUNK_SIZE)
        .await
        .expect("build chunks");
    assert!(
        chunks.len() >= 3,
        "expected multi-chunk transfer; got {} chunks",
        chunks.len(),
    );

    let total = chunks.len();
    let mut applied = false;
    for (i, msg) in chunks.into_iter().enumerate() {
        let ReplicationMessage::SnapshotChunk {
            transfer_id: tid,
            chunk_index,
            total_chunks,
            is_final,
            last_index,
            term,
            total_bytes,
            payload,
        } = msg
        else {
            panic!("expected SnapshotChunk");
        };
        let ack = follower
            .handle_snapshot_chunk(
                tid,
                chunk_index,
                total_chunks,
                is_final,
                last_index,
                term,
                total_bytes,
                payload,
            )
            .await
            .expect("apply chunk");
        let ReplicationMessage::SnapshotChunkAck { applied: app, .. } = ack else {
            panic!("expected ChunkAck back");
        };
        if i + 1 == total {
            assert!(app, "final chunk should yield applied=true");
            applied = true;
        }
    }
    assert!(applied);

    // Follower's last_index now matches the leader's snapshot
    // last_index.
    assert_eq!(follower.last_index().await, leader.last_index().await);
}

#[tokio::test]
async fn chunked_snapshot_resumes_after_mid_stream_drop() {
    let leader = make_engine("leader", vec!["follower"]);
    leader.force_leader().await;
    populate_leader_until(&leader, 600 * 1024).await;
    let follower = make_engine("follower", vec!["leader"]);

    let transfer_id = leader.next_snapshot_transfer_id().await;
    let chunks = leader
        .build_snapshot_chunks(transfer_id, 0, DEFAULT_SNAPSHOT_CHUNK_SIZE)
        .await
        .expect("build chunks");
    assert!(chunks.len() >= 3, "need multi-chunk transfer");

    // === Send only the first 2 chunks. ===
    let mut iter = chunks.into_iter();
    for _ in 0..2 {
        let msg = iter.next().unwrap();
        let ReplicationMessage::SnapshotChunk {
            transfer_id: tid,
            chunk_index,
            total_chunks,
            is_final,
            last_index,
            term,
            total_bytes,
            payload,
        } = msg
        else {
            panic!()
        };
        let ack = follower
            .handle_snapshot_chunk(
                tid,
                chunk_index,
                total_chunks,
                is_final,
                last_index,
                term,
                total_bytes,
                payload,
            )
            .await
            .expect("apply chunk");
        let ReplicationMessage::SnapshotChunkAck {
            follower_id,
            transfer_id: tid_ack,
            last_received_chunk,
            applied,
        } = ack
        else {
            panic!()
        };
        assert!(!applied, "not yet final");
        // Feed the ack back into the leader's cursor.
        leader
            .receive_snapshot_chunk_ack(&follower_id, tid_ack, last_received_chunk, applied)
            .await;
    }

    // At this point chunks 0 + 1 have been acked; cursor says
    // resume from chunk 2.
    let resume = leader.resume_chunk_idx("follower", transfer_id).await;
    assert_eq!(resume, 2, "expected resume cursor at chunk 2, got {resume}");

    // === Simulate a transport drop: build a fresh chunks list
    // starting from `resume`. The follower's partial buffer should
    // still be intact, so the new chunks complete the transfer.
    let resume_chunks = leader
        .build_snapshot_chunks(transfer_id, resume, DEFAULT_SNAPSHOT_CHUNK_SIZE)
        .await
        .expect("resume chunks");
    let mut applied = false;
    let resume_total = resume_chunks.len();
    for (i, msg) in resume_chunks.into_iter().enumerate() {
        let ReplicationMessage::SnapshotChunk {
            transfer_id: tid,
            chunk_index,
            total_chunks,
            is_final,
            last_index,
            term,
            total_bytes,
            payload,
        } = msg
        else {
            panic!()
        };
        let ack = follower
            .handle_snapshot_chunk(
                tid,
                chunk_index,
                total_chunks,
                is_final,
                last_index,
                term,
                total_bytes,
                payload,
            )
            .await
            .expect("apply resume chunk");
        let ReplicationMessage::SnapshotChunkAck { applied: app, .. } = ack else {
            panic!()
        };
        if i + 1 == resume_total {
            applied = app;
        }
    }
    assert!(applied, "transfer should complete on resume");

    // Final state: follower's last_index matches leader's.
    assert_eq!(follower.last_index().await, leader.last_index().await);
}

#[tokio::test]
async fn chunked_snapshot_evicts_stale_buffer_on_new_transfer_id() {
    let leader = make_engine("leader", vec!["follower"]);
    leader.force_leader().await;
    populate_leader_until(&leader, 600 * 1024).await;

    let follower = make_engine("follower", vec!["leader"]);

    // Start transfer_id=1, send chunk 0 + 1 (incomplete).
    let t1 = leader.next_snapshot_transfer_id().await;
    let chunks1 = leader
        .build_snapshot_chunks(t1, 0, DEFAULT_SNAPSHOT_CHUNK_SIZE)
        .await
        .expect("build1");
    for msg in chunks1.into_iter().take(2) {
        let ReplicationMessage::SnapshotChunk {
            transfer_id,
            chunk_index,
            total_chunks,
            is_final,
            last_index,
            term,
            total_bytes,
            payload,
        } = msg
        else {
            panic!()
        };
        let _ = follower
            .handle_snapshot_chunk(
                transfer_id,
                chunk_index,
                total_chunks,
                is_final,
                last_index,
                term,
                total_bytes,
                payload,
            )
            .await
            .expect("apply");
    }
    let progress = follower.snapshot_buffer_progress(t1).await;
    assert_eq!(progress, Some((2, progress.unwrap().1)));

    // Start a FRESH transfer with t2 > t1 — should evict t1.
    let t2 = leader.next_snapshot_transfer_id().await;
    assert!(t2 > t1);
    let chunks2 = leader
        .build_snapshot_chunks(t2, 0, DEFAULT_SNAPSHOT_CHUNK_SIZE)
        .await
        .expect("build2");
    // Feed all of t2 to drive completion.
    let total2 = chunks2.len();
    let mut applied = false;
    for (i, msg) in chunks2.into_iter().enumerate() {
        let ReplicationMessage::SnapshotChunk {
            transfer_id,
            chunk_index,
            total_chunks,
            is_final,
            last_index,
            term,
            total_bytes,
            payload,
        } = msg
        else {
            panic!()
        };
        let ack = follower
            .handle_snapshot_chunk(
                transfer_id,
                chunk_index,
                total_chunks,
                is_final,
                last_index,
                term,
                total_bytes,
                payload,
            )
            .await
            .expect("apply");
        if i + 1 == total2 {
            let ReplicationMessage::SnapshotChunkAck { applied: app, .. } = ack else {
                panic!()
            };
            applied = app;
        }
    }
    assert!(applied);
    // t1 buffer must be gone.
    assert!(follower.snapshot_buffer_progress(t1).await.is_none());
}

#[tokio::test]
async fn chunked_snapshot_rejects_oversized_total_bytes() {
    let follower = make_engine("follower", vec!["leader"]);
    let err = follower
        .handle_snapshot_chunk(
            1,
            0,
            1,
            true,
            1,
            1,
            10 * 1024 * 1024 * 1024, // 10 GiB > 128 MiB cap
            vec![0u8; 4],
        )
        .await
        .expect_err("must reject oversized total_bytes");
    let msg = format!("{err}");
    assert!(msg.contains("total_bytes"), "unexpected error: {msg}",);
}
