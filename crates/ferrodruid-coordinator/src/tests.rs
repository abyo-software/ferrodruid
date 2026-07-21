// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Integration-level tests for the [`Coordinator`].

use super::*;
use ferrodruid_metadata::SegmentMetadataRow;
use serde_json::json;

async fn setup() -> (Arc<MetadataStore>, Coordinator) {
    let store = MetadataStore::new_in_memory().await.expect("create store");
    store.initialize().await.expect("init schema");
    let store = Arc::new(store);
    let coord = Coordinator::new(Arc::clone(&store));
    (store, coord)
}

fn make_segment(id: &str, ds: &str) -> SegmentMetadataRow {
    make_segment_sized(id, ds, 100)
}

fn make_segment_sized(id: &str, ds: &str, size: u64) -> SegmentMetadataRow {
    SegmentMetadataRow {
        id: id.to_string(),
        data_source: ds.to_string(),
        created_date: "2024-01-01T00:00:00Z".to_string(),
        start: "2024-01-01T00:00:00+00:00".to_string(),
        end: "2024-02-01T00:00:00+00:00".to_string(),
        version: "2024-01-01T00:00:00.000Z".to_string(),
        used: true,
        payload: json!({"dataSource": ds, "size": size}),
    }
}

fn make_segment_interval(
    id: &str,
    ds: &str,
    start: &str,
    end: &str,
    size: u64,
) -> SegmentMetadataRow {
    SegmentMetadataRow {
        id: id.to_string(),
        data_source: ds.to_string(),
        created_date: "2024-01-01T00:00:00Z".to_string(),
        start: start.to_string(),
        end: end.to_string(),
        version: "2024-01-01T00:00:00.000Z".to_string(),
        used: true,
        payload: json!({"dataSource": ds, "size": size}),
    }
}

fn make_server(name: &str, max_size: u64, current_size: u64) -> ServerInfo {
    make_server_tier(name, "_default_tier", max_size, current_size)
}

fn make_server_tier(name: &str, tier: &str, max_size: u64, current_size: u64) -> ServerInfo {
    ServerInfo {
        name: name.to_string(),
        host: "127.0.0.1".to_string(),
        port: 8083,
        tier: tier.to_string(),
        max_size,
        current_size,
    }
}

/// Default rules: load forever on `_default_tier` with 1 replica.
async fn set_load_forever(store: &MetadataStore, ds: &str, tier: &str, replicas: usize) {
    let rules = vec![json!({
        "type": "loadForever",
        "tier_replicants": { tier: replicas }
    })];
    store.set_rules(ds, &rules).await.expect("set rules");
}

// ---------------------------------------------------------------------------
// Basic placement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn balance_places_one_replica_per_segment() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    for i in 0..4 {
        store
            .insert_segment(&make_segment(&format!("seg_{i}"), "wiki"))
            .await
            .expect("insert");
    }
    let servers = vec![
        make_server("hist-1", 100_000, 0),
        make_server("hist-2", 100_000, 0),
    ];
    let actions = coord.run_balance(&servers).await.expect("balance");

    let loads: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, SegmentLoadAction::Load { .. }))
        .collect();
    assert_eq!(loads.len(), 4, "one replica per segment");
}

#[tokio::test]
async fn balance_empty_cluster() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    let actions = coord.run_balance(&[]).await.expect("balance");
    assert!(actions.is_empty());
}

#[tokio::test]
async fn balance_idempotent() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    let servers = vec![make_server("hist-1", 100_000, 0)];

    let first = coord.run_balance(&servers).await.expect("first");
    assert_eq!(first.len(), 1);
    let second = coord.run_balance(&servers).await.expect("second");
    assert!(second.is_empty(), "no new actions on second run");
}

// ---------------------------------------------------------------------------
// Real size capacity gating
// ---------------------------------------------------------------------------

#[tokio::test]
async fn capacity_gating_uses_real_sizes() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    // Two 600-byte segments, one server with room for only one.
    store
        .insert_segment(&make_segment_sized("big_1", "wiki", 600))
        .await
        .expect("insert");
    store
        .insert_segment(&make_segment_sized("big_2", "wiki", 600))
        .await
        .expect("insert");
    let servers = vec![make_server("hist-1", 1000, 0)];
    let actions = coord.run_balance(&servers).await.expect("balance");
    let loads = actions
        .iter()
        .filter(|a| matches!(a, SegmentLoadAction::Load { .. }))
        .count();
    assert_eq!(loads, 1, "only one 600-byte segment fits in 1000 bytes");

    let srv = coord.get_servers().await;
    assert_eq!(srv[0].current_size, 600);
}

#[tokio::test]
async fn full_server_receives_nothing() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    store
        .insert_segment(&make_segment_sized("seg_0", "wiki", 100))
        .await
        .expect("insert");
    let servers = vec![make_server("hist-1", 100, 100)];
    let actions = coord.run_balance(&servers).await.expect("balance");
    assert!(actions.is_empty());
}

// ---------------------------------------------------------------------------
// Tier filtering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn segment_only_assigned_to_target_tier() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "hot", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");

    let servers = vec![
        make_server_tier("cold-1", "cold", 100_000, 0),
        make_server_tier("hot-1", "hot", 100_000, 0),
    ];
    let actions = coord.run_balance(&servers).await.expect("balance");
    let loaded_on: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            SegmentLoadAction::Load { server, .. } => Some(server.clone()),
            SegmentLoadAction::Drop { .. } => None,
        })
        .collect();
    assert_eq!(loaded_on, vec!["hot-1"], "must land on hot tier only");
}

#[tokio::test]
async fn no_tier_server_means_no_load() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "hot", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    // Only a cold server available.
    let servers = vec![make_server_tier("cold-1", "cold", 100_000, 0)];
    let actions = coord.run_balance(&servers).await.expect("balance");
    assert!(
        actions.is_empty(),
        "no server in the target tier -> no load"
    );
}

#[tokio::test]
async fn tier_migration_keeps_off_tier_copy_when_target_tier_unavailable() {
    // DD R23: a segment whose only replica is off-tier (cold-1) and whose rule
    // now targets a tier with no available server must NOT be dropped — pruning
    // the off-tier copy before a target-tier replacement exists would make the
    // segment unavailable. The off-tier copy is retained and no Drop is emitted.
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "cold", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    // Cycle 1: place the sole replica on the cold tier.
    let c1 = coord
        .run_balance(&[make_server_tier("cold-1", "cold", 100_000, 0)])
        .await
        .expect("balance 1");
    assert!(
        c1.iter()
            .any(|a| matches!(a, SegmentLoadAction::Load { server, .. } if server == "cold-1")),
        "cycle 1 must load seg_0 on cold-1: {c1:?}"
    );

    // Rule now targets the hot tier, but no hot server is available.
    set_load_forever(&store, "wiki", "hot", 1).await;
    let c2 = coord.run_balance_registered().await.expect("balance 2");

    assert!(
        !c2.iter()
            .any(|a| matches!(a, SegmentLoadAction::Drop { server, .. } if server == "cold-1")),
        "must not drop the only (off-tier) copy when the target tier is unavailable: {c2:?}"
    );
}

#[tokio::test]
async fn tier_migration_loads_target_then_drops_off_tier() {
    // DD R23: when the target tier CAN hold the replica, migration loads the
    // target-tier copy first, then prunes the off-tier copy — and the Load is
    // emitted before the Drop (Load-before-Drop invariant).
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "cold", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    coord
        .run_balance(&[make_server_tier("cold-1", "cold", 100_000, 0)])
        .await
        .expect("balance 1");

    set_load_forever(&store, "wiki", "hot", 1).await;
    let c2 = coord
        .run_balance(&[make_server_tier("hot-1", "hot", 100_000, 0)])
        .await
        .expect("balance 2");

    let load_pos = c2
        .iter()
        .position(|a| matches!(a, SegmentLoadAction::Load { server, .. } if server == "hot-1"));
    let drop_pos = c2
        .iter()
        .position(|a| matches!(a, SegmentLoadAction::Drop { server, .. } if server == "cold-1"));
    let load_pos = load_pos.expect("must load on hot-1");
    let drop_pos = drop_pos.expect("must drop the off-tier cold-1 copy once hot is satisfied");
    assert!(
        load_pos < drop_pos,
        "Load on the target tier must precede the off-tier Drop: {c2:?}"
    );
}

/// Set a multi-tier `loadForever` rule (tier -> replica count).
async fn set_load_forever_multi(store: &MetadataStore, ds: &str, tiers: &[(&str, usize)]) {
    let map: serde_json::Map<String, serde_json::Value> = tiers
        .iter()
        .map(|(t, r)| ((*t).to_string(), json!(r)))
        .collect();
    let rules = vec![json!({ "type": "loadForever", "tier_replicants": map })];
    store.set_rules(ds, &rules).await.expect("set rules");
}

#[tokio::test]
async fn multi_tier_zero_replica_entry_does_not_drop_last_copy() {
    // DD R24: a rule like {"cold":0,"hot":1} must NOT drop the segment's only
    // copy. Previously the alphabetically-first tier (cold, 0 replicas) was
    // chosen and the on-tier==0>=0 prune dropped the hot copy.
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "hot", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    // Cycle 1: place the copy on hot-1.
    coord
        .run_balance(&[make_server_tier("hot-1", "hot", 100_000, 0)])
        .await
        .expect("balance 1");

    // Rule now lists cold with 0 replicas plus the existing hot:1.
    set_load_forever_multi(&store, "wiki", &[("cold", 0), ("hot", 1)]).await;
    let c2 = coord.run_balance_registered().await.expect("balance 2");

    assert!(
        !c2.iter()
            .any(|a| matches!(a, SegmentLoadAction::Drop { server, .. } if server == "hot-1")),
        "a 0-replica tier entry must not drop the only (hot) copy: {c2:?}"
    );
}

#[tokio::test]
async fn multi_tier_loads_every_positive_tier() {
    // DD R24: {"cold":1,"hot":1} must place a replica on BOTH tiers, not just
    // one. Collapsing to a single tier silently under-replicated the segment.
    let (store, coord) = setup().await;
    set_load_forever_multi(&store, "wiki", &[("cold", 1), ("hot", 1)]).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    let actions = coord
        .run_balance(&[
            make_server_tier("cold-1", "cold", 100_000, 0),
            make_server_tier("hot-1", "hot", 100_000, 0),
        ])
        .await
        .expect("balance");

    let loaded_on: std::collections::HashSet<&str> = actions
        .iter()
        .filter_map(|a| match a {
            SegmentLoadAction::Load { server, .. } => Some(server.as_str()),
            SegmentLoadAction::Drop { .. } => None,
        })
        .collect();
    assert!(
        loaded_on.contains("cold-1") && loaded_on.contains("hot-1"),
        "multi-tier rule must load both tiers, got: {loaded_on:?}"
    );
}

#[tokio::test]
async fn multi_tier_overfull_tier_not_trimmed_until_all_satisfied() {
    // DD R25: a segment with 3 hot replicas under {"hot":3} whose rule becomes
    // {"cold":1,"hot":1} must NOT shed its surplus hot copies while the requested
    // cold replica is unsatisfied (no cold server) — that would dip total
    // availability below the desired count mid-migration.
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "hot", 3).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    coord
        .run_balance(&[
            make_server_tier("hot-1", "hot", 100_000, 0),
            make_server_tier("hot-2", "hot", 100_000, 0),
            make_server_tier("hot-3", "hot", 100_000, 0),
        ])
        .await
        .expect("balance 1");

    set_load_forever_multi(&store, "wiki", &[("cold", 1), ("hot", 1)]).await;
    let c2 = coord.run_balance_registered().await.expect("balance 2");

    assert!(
        !c2.iter().any(|a| matches!(
            a,
            SegmentLoadAction::Drop { server, .. } if server.starts_with("hot-")
        )),
        "overfull hot tier must not be trimmed while cold is unsatisfied: {c2:?}"
    );
}

#[tokio::test]
async fn multi_tier_trims_surplus_once_all_tiers_satisfied() {
    // DD R25: once every requested tier is satisfied, the surplus IS trimmed.
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "hot", 3).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    coord
        .run_balance(&[
            make_server_tier("hot-1", "hot", 100_000, 0),
            make_server_tier("hot-2", "hot", 100_000, 0),
            make_server_tier("hot-3", "hot", 100_000, 0),
        ])
        .await
        .expect("balance 1");

    set_load_forever_multi(&store, "wiki", &[("cold", 1), ("hot", 1)]).await;
    // Now a cold server is available: cold gets loaded and hot trims 3 -> 1.
    let c2 = coord
        .run_balance(&[make_server_tier("cold-1", "cold", 100_000, 0)])
        .await
        .expect("balance 2");

    assert!(
        c2.iter()
            .any(|a| matches!(a, SegmentLoadAction::Load { server, .. } if server == "cold-1")),
        "cold-1 must be loaded: {c2:?}"
    );
    let hot_drops = c2
        .iter()
        .filter(
            |a| matches!(a, SegmentLoadAction::Drop { server, .. } if server.starts_with("hot-")),
        )
        .count();
    assert_eq!(
        hot_drops, 2,
        "hot must trim from 3 to 1 once all tiers are satisfied: {c2:?}"
    );
}

// ---------------------------------------------------------------------------
// Replica counting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn loads_requested_replica_count() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "hot", 2).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    let servers = vec![
        make_server_tier("hot-1", "hot", 100_000, 0),
        make_server_tier("hot-2", "hot", 100_000, 0),
    ];
    let actions = coord.run_balance(&servers).await.expect("balance");
    let loads = actions
        .iter()
        .filter(|a| matches!(a, SegmentLoadAction::Load { .. }))
        .count();
    assert_eq!(loads, 2, "two replicas requested -> two loads");

    // Each replica on a distinct server.
    let s1 = coord.get_segments_for_server("hot-1").await;
    let s2 = coord.get_segments_for_server("hot-2").await;
    assert_eq!(s1.len(), 1);
    assert_eq!(s2.len(), 1);
}

#[tokio::test]
async fn replicas_capped_by_available_servers() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "hot", 3).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    // Only two hot servers for a 3-replica request.
    let servers = vec![
        make_server_tier("hot-1", "hot", 100_000, 0),
        make_server_tier("hot-2", "hot", 100_000, 0),
    ];
    let actions = coord.run_balance(&servers).await.expect("balance");
    let loads = actions
        .iter()
        .filter(|a| matches!(a, SegmentLoadAction::Load { .. }))
        .count();
    assert_eq!(loads, 2, "cannot exceed server count");
}

#[tokio::test]
async fn over_replication_drops_surplus() {
    let (store, coord) = setup().await;
    // First load 2 replicas.
    set_load_forever(&store, "wiki", "hot", 2).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    let servers = vec![
        make_server_tier("hot-1", "hot", 100_000, 0),
        make_server_tier("hot-2", "hot", 100_000, 0),
    ];
    coord.run_balance(&servers).await.expect("balance 2x");

    // Now reduce to 1 replica.
    set_load_forever(&store, "wiki", "hot", 1).await;
    let actions = coord
        .run_balance_registered()
        .await
        .expect("rebalance to 1");
    let drops = actions
        .iter()
        .filter(|a| matches!(a, SegmentLoadAction::Drop { .. }))
        .count();
    assert_eq!(drops, 1, "one surplus replica dropped");

    let total: usize = (coord.get_segments_for_server("hot-1").await.len())
        + (coord.get_segments_for_server("hot-2").await.len());
    assert_eq!(total, 1, "exactly one replica remains");
}

// ---------------------------------------------------------------------------
// Drop of unused / rule-dropped segments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drop_rule_unloads_segment() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    let servers = vec![make_server("hist-1", 100_000, 0)];
    coord.run_balance(&servers).await.expect("load");
    assert_eq!(coord.get_segments_for_server("hist-1").await.len(), 1);

    // Switch rules to dropForever.
    let drop_rules = vec![json!({"type": "dropForever"})];
    store
        .set_rules("wiki", &drop_rules)
        .await
        .expect("set drop");
    let actions = coord.run_balance_registered().await.expect("drop");
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, SegmentLoadAction::Drop { .. })),
        "drop action emitted"
    );
    assert!(coord.get_segments_for_server("hist-1").await.is_empty());
}

#[tokio::test]
async fn unused_segment_is_dropped() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    let servers = vec![make_server("hist-1", 100_000, 0)];
    coord.run_balance(&servers).await.expect("load");

    // Mark unused, then rebalance.
    coord.disable_segment("seg_0").await.expect("disable");
    let actions = coord.run_balance_registered().await.expect("rebalance");
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, SegmentLoadAction::Drop { .. })),
        "unused segment dropped"
    );
    assert!(coord.get_segments_for_server("hist-1").await.is_empty());
    // Server size released.
    assert_eq!(coord.get_servers().await[0].current_size, 0);
}

// ---------------------------------------------------------------------------
// Cost-based placement ordering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cost_spreads_temporally_adjacent_segments() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    // Three temporally adjacent segments; two empty servers. Cost balancing
    // should not pile all of them on one server.
    store
        .insert_segment(&make_segment_interval(
            "jan",
            "wiki",
            "2024-01-01T00:00:00+00:00",
            "2024-02-01T00:00:00+00:00",
            100,
        ))
        .await
        .expect("insert");
    store
        .insert_segment(&make_segment_interval(
            "feb",
            "wiki",
            "2024-02-01T00:00:00+00:00",
            "2024-03-01T00:00:00+00:00",
            100,
        ))
        .await
        .expect("insert");
    store
        .insert_segment(&make_segment_interval(
            "mar",
            "wiki",
            "2024-03-01T00:00:00+00:00",
            "2024-04-01T00:00:00+00:00",
            100,
        ))
        .await
        .expect("insert");

    let servers = vec![
        make_server("hist-1", 100_000, 0),
        make_server("hist-2", 100_000, 0),
    ];
    coord.run_balance(&servers).await.expect("balance");

    let c1 = coord.get_segments_for_server("hist-1").await.len();
    let c2 = coord.get_segments_for_server("hist-2").await.len();
    assert_eq!(c1 + c2, 3, "all three placed");
    assert!(c1 >= 1 && c2 >= 1, "spread across both servers: {c1},{c2}");
}

// ---------------------------------------------------------------------------
// Rebalance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rebalance_moves_off_overloaded_server() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    // Place several segments while only one server is available, then add a
    // second empty server and rebalance.
    for i in 0..4 {
        store
            .insert_segment(&make_segment_interval(
                &format!("seg_{i}"),
                "wiki",
                &format!("2024-0{}-01T00:00:00+00:00", i + 1),
                &format!("2024-0{}-01T00:00:00+00:00", i + 2),
                100,
            ))
            .await
            .expect("insert");
    }
    let one = vec![make_server("hist-1", 100_000, 0)];
    coord.run_balance(&one).await.expect("balance one");
    assert_eq!(coord.get_segments_for_server("hist-1").await.len(), 4);

    // Add a second server.
    coord
        .register_server(make_server("hist-2", 100_000, 0))
        .await;
    let actions = coord.rebalance().await.expect("rebalance");
    let moves = actions
        .iter()
        .filter(|a| matches!(a, SegmentLoadAction::Load { .. }))
        .count();
    assert!(moves >= 1, "at least one segment moved");
    // Matched drop+load pairs.
    let drops = actions
        .iter()
        .filter(|a| matches!(a, SegmentLoadAction::Drop { .. }))
        .count();
    assert_eq!(moves, drops, "each move is a matched drop+load");

    assert!(
        !coord.get_segments_for_server("hist-2").await.is_empty(),
        "second server received segments"
    );
}

#[tokio::test]
async fn rebalance_emits_load_before_drop_per_move() {
    // DD R10 #5: a move of a 1-replica segment must emit the destination
    // `Load` BEFORE the source `Drop`, so the segment is never left
    // unavailable if the load fails/delays. Assert that for every moved
    // segment, its Load index precedes its Drop index in the action vector.
    let (store, _coord) = setup().await;
    let coord = Coordinator::with_config(
        Arc::clone(&store),
        BalancerConfig {
            max_segments_to_move: 1,
        },
    );
    // Single replica per segment.
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    for i in 0..4 {
        store
            .insert_segment(&make_segment_interval(
                &format!("seg_{i}"),
                "wiki",
                &format!("2024-0{}-01T00:00:00+00:00", i + 1),
                &format!("2024-0{}-01T00:00:00+00:00", i + 2),
                100,
            ))
            .await
            .expect("insert");
    }
    let one = vec![make_server("hist-1", 100_000, 0)];
    coord.run_balance(&one).await.expect("balance one");
    coord
        .register_server(make_server("hist-2", 100_000, 0))
        .await;

    let actions = coord.rebalance().await.expect("rebalance");

    // Collect (kind, segment_id) in order.
    let mut moved_at_least_one = false;
    // For each segment that has both a Load and a Drop, Load must come first.
    use std::collections::HashMap;
    let mut load_idx: HashMap<&str, usize> = HashMap::new();
    let mut drop_idx: HashMap<&str, usize> = HashMap::new();
    for (i, act) in actions.iter().enumerate() {
        match act {
            SegmentLoadAction::Load { segment_id, .. } => {
                load_idx.entry(segment_id.as_str()).or_insert(i);
            }
            SegmentLoadAction::Drop { segment_id, .. } => {
                drop_idx.entry(segment_id.as_str()).or_insert(i);
            }
        }
    }
    for (seg, &li) in &load_idx {
        if let Some(&di) = drop_idx.get(seg) {
            moved_at_least_one = true;
            assert!(
                li < di,
                "segment {seg}: Load (idx {li}) must precede Drop (idx {di})"
            );
        }
    }
    assert!(
        moved_at_least_one,
        "expected at least one moved segment with a matched Load+Drop pair"
    );
}

#[tokio::test]
async fn rebalance_respects_max_moves() {
    let (store, _coord) = setup().await;
    let coord = Coordinator::with_config(
        Arc::clone(&store),
        BalancerConfig {
            max_segments_to_move: 1,
        },
    );
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    for i in 0..6 {
        store
            .insert_segment(&make_segment_interval(
                &format!("seg_{i}"),
                "wiki",
                &format!("2024-{:02}-01T00:00:00+00:00", i + 1),
                &format!("2024-{:02}-01T00:00:00+00:00", i + 2),
                100,
            ))
            .await
            .expect("insert");
    }
    let one = vec![make_server("hist-1", 1_000_000, 0)];
    coord.run_balance(&one).await.expect("balance one");
    coord
        .register_server(make_server("hist-2", 1_000_000, 0))
        .await;

    let actions = coord.rebalance().await.expect("rebalance");
    let moves = actions
        .iter()
        .filter(|a| matches!(a, SegmentLoadAction::Load { .. }))
        .count();
    assert!(moves <= 1, "bounded by max_segments_to_move=1, got {moves}");
}

#[tokio::test]
async fn rebalance_noop_when_balanced() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    let servers = vec![
        make_server("hist-1", 100_000, 0),
        make_server("hist-2", 100_000, 0),
    ];
    coord.run_balance(&servers).await.expect("balance");
    let actions = coord.rebalance().await.expect("rebalance");
    assert!(actions.is_empty(), "already balanced -> no moves");
}

// ---------------------------------------------------------------------------
// Server registry CRUD via the Coordinator
// ---------------------------------------------------------------------------

#[tokio::test]
async fn server_registry_crud() {
    let (_store, coord) = setup().await;
    assert!(coord.get_servers().await.is_empty());

    coord.register_server(make_server("h1", 1000, 0)).await;
    coord.register_server(make_server("h2", 2000, 0)).await;
    let servers = coord.get_servers().await;
    assert_eq!(servers.len(), 2);
    assert_eq!(servers[0].name, "h1"); // sorted

    // Update.
    assert!(
        coord
            .update_server("h1", Some("hot".into()), Some(5000))
            .await
    );
    let h1 = coord
        .get_servers()
        .await
        .into_iter()
        .find(|s| s.name == "h1")
        .expect("h1");
    assert_eq!(h1.tier, "hot");
    assert_eq!(h1.max_size, 5000);

    // Update of missing server.
    assert!(!coord.update_server("ghost", None, None).await);

    // Remove.
    assert!(coord.remove_server("h2").await.is_some());
    assert_eq!(coord.get_servers().await.len(), 1);
    assert!(coord.remove_server("h2").await.is_none());
}

#[tokio::test]
async fn remove_server_forgets_assignments() {
    let (store, coord) = setup().await;
    set_load_forever(&store, "wiki", "_default_tier", 1).await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    let servers = vec![make_server("hist-1", 100_000, 0)];
    coord.run_balance(&servers).await.expect("balance");
    assert_eq!(coord.get_segments_for_server("hist-1").await.len(), 1);

    coord.remove_server("hist-1").await;
    assert!(coord.get_segments_for_server("hist-1").await.is_empty());
    assert!(coord.get_servers().await.is_empty());
}

// ---------------------------------------------------------------------------
// Preserved lifecycle API
// ---------------------------------------------------------------------------

#[tokio::test]
async fn segment_counts() {
    let (store, coord) = setup().await;
    store
        .insert_segment(&make_segment("wiki_1", "wiki"))
        .await
        .expect("insert");
    store
        .insert_segment(&make_segment("wiki_2", "wiki"))
        .await
        .expect("insert");
    store
        .insert_segment(&make_segment("clicks_1", "clicks"))
        .await
        .expect("insert");
    let counts = coord.get_segment_counts().await.expect("counts");
    assert_eq!(*counts.get("wiki").unwrap_or(&0), 2);
    assert_eq!(*counts.get("clicks").unwrap_or(&0), 1);
}

#[tokio::test]
async fn disable_and_enable_datasource() {
    let (store, coord) = setup().await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    coord.disable_datasource("wiki").await.expect("disable");
    assert!(
        store
            .get_used_segments("wiki")
            .await
            .expect("get")
            .is_empty()
    );

    coord.enable_datasource("wiki").await.expect("enable");
    assert_eq!(store.get_used_segments("wiki").await.expect("get").len(), 1);
}

/// Codex 2026-07-12 round-2 HIGH #2/#3: `disable_segment` must be mutually
/// exclusive with a segment publication for the same datasource. The test
/// holds the datasource's publish lock (exactly what the Overlord holds
/// for its whole plan → metadata-txn → swap → rollback critical section)
/// and asserts the disable BLOCKS until it is released — pre-fix the
/// disable mutated the used flag immediately, mid-publish.
#[tokio::test]
async fn disable_segment_blocks_while_publish_lock_held() {
    let (store, coord) = setup().await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");

    let lock = store.datasource_publish_lock("wiki").await;
    let guard = lock.lock().await;

    let coord = Arc::new(coord);
    let disabling = {
        let coord = Arc::clone(&coord);
        tokio::spawn(async move { coord.disable_segment("seg_0").await })
    };

    // While the publish lock is held the disable must NOT complete (a held
    // tokio::Mutex can never be acquired, so this is deterministic).
    let mut disabling = disabling;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut disabling)
            .await
            .is_err(),
        "disable_segment must block while a publish holds the datasource lock"
    );
    assert_eq!(
        store.get_used_segments("wiki").await.expect("get").len(),
        1,
        "the segment must still be used while the disable is blocked"
    );

    // Release the "publish": the disable proceeds and lands afterwards.
    drop(guard);
    disabling
        .await
        .expect("join disable task")
        .expect("disable succeeds after the lock is released");
    assert!(
        store
            .get_used_segments("wiki")
            .await
            .expect("get")
            .is_empty(),
        "the disable applies once the publish critical section is over"
    );
}

/// Same mutual exclusion for the datasource-wide disable path.
#[tokio::test]
async fn disable_datasource_blocks_while_publish_lock_held() {
    let (store, coord) = setup().await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");

    let lock = store.datasource_publish_lock("wiki").await;
    let guard = lock.lock().await;

    let coord = Arc::new(coord);
    let mut disabling = {
        let coord = Arc::clone(&coord);
        tokio::spawn(async move { coord.disable_datasource("wiki").await })
    };
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut disabling)
            .await
            .is_err(),
        "disable_datasource must block while a publish holds the datasource lock"
    );
    assert_eq!(store.get_used_segments("wiki").await.expect("get").len(), 1);

    drop(guard);
    disabling
        .await
        .expect("join disable task")
        .expect("disable succeeds after the lock is released");
    assert!(
        store
            .get_used_segments("wiki")
            .await
            .expect("get")
            .is_empty()
    );
}

/// `disable_segment` on an id with no metadata row stays a no-op (and must
/// not deadlock on any lock lookup path).
#[tokio::test]
async fn disable_segment_missing_id_is_noop() {
    let (store, coord) = setup().await;
    coord
        .disable_segment("no_such_segment")
        .await
        .expect("missing id is a no-op");
    assert!(store.get_all_segments().await.expect("all").is_empty());
}

#[tokio::test]
async fn invalid_rule_chain_surfaces_error() {
    let (store, coord) = setup().await;
    store
        .insert_segment(&make_segment("seg_0", "wiki"))
        .await
        .expect("insert");
    let bad = vec![json!({"type": "totallyBogusRule"})];
    store.set_rules("wiki", &bad).await.expect("set");
    let servers = vec![make_server("hist-1", 100_000, 0)];
    let err = coord.run_balance(&servers).await.expect_err("should error");
    assert!(err.to_string().contains("invalid load rule"));
}
