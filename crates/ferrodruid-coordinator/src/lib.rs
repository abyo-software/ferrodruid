// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Segment balancing and load rules for FerroDruid.
//!
//! The [`Coordinator`] manages which segments are loaded on which Historical
//! nodes. Each balancing cycle:
//!
//! 1. reads the used segments and each data source's load-rule chain from the
//!    [`MetadataStore`];
//! 2. derives, per segment, the desired number of replicas in which tier (or a
//!    drop) via [`rules`];
//! 3. places under-replicated segments on the tier server that minimizes the
//!    cost-based co-location score (see [`balancer`]), subject to real
//!    byte-size capacity gating;
//! 4. emits [`SegmentLoadAction::Drop`] for over-replicated, dropped, or unused
//!    segments.
//!
//! A separate [`Coordinator::rebalance`] pass moves segments off the most
//! heavily filled servers toward cost-optimal placement, bounded per cycle.
//!
//! The Coordinator owns an in-memory [`ServerRegistry`]; servers are registered
//! via [`Coordinator::register_server`] and the registry's tracked
//! `current_size` is the authoritative input to capacity gating.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod balancer;
mod rules;
mod server;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use ferrodruid_common::{Result, types::Interval};
use ferrodruid_metadata::{MetadataStore, SegmentMetadataRow};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub use balancer::{HALF_LIFE_MILLIS, interval_cost, placement_cost};
pub use rules::DesiredPlacement;
pub use server::{DEFAULT_SEGMENT_SIZE_BYTES, ServerInfo, ServerRegistry, segment_size_bytes};

use balancer::FILL_PENALTY_WEIGHT;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A segment assignment to a particular server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentAssignment {
    /// Segment identifier.
    pub segment_id: String,
    /// Data source name.
    pub data_source: String,
    /// Time interval covered by the segment.
    pub interval: Interval,
    /// Tier this assignment belongs to.
    pub tier: String,
    /// Byte size of the segment (from its metadata payload).
    pub size_bytes: u64,
}

/// An action the Coordinator wants a Historical node to perform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentLoadAction {
    /// Load a segment onto a server.
    Load {
        /// Target server name.
        server: String,
        /// Segment to load.
        segment_id: String,
    },
    /// Drop a segment from a server.
    Drop {
        /// Target server name.
        server: String,
        /// Segment to drop.
        segment_id: String,
    },
}

/// Tunable knobs for a balancing / rebalancing cycle.
#[derive(Debug, Clone)]
pub struct BalancerConfig {
    /// Maximum number of segment moves emitted by a single
    /// [`Coordinator::rebalance`] call.
    pub max_segments_to_move: usize,
}

impl Default for BalancerConfig {
    fn default() -> Self {
        Self {
            max_segments_to_move: 5,
        }
    }
}

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------

/// The Coordinator manages segment-to-server assignments.
///
/// It queries the [`MetadataStore`] for used segments and their rule chains and
/// distributes replicas across the registered Historical nodes, respecting tier
/// placement, replica counts, and real byte-size capacity constraints.
pub struct Coordinator {
    metadata: Arc<MetadataStore>,
    /// Server name -> assignments currently placed on it.
    segment_assignments: RwLock<HashMap<String, Vec<SegmentAssignment>>>,
    /// Authoritative registry of known servers (owns `current_size`).
    registry: RwLock<ServerRegistry>,
    config: BalancerConfig,
}

impl Coordinator {
    /// Create a new Coordinator backed by the given metadata store and default
    /// [`BalancerConfig`].
    #[must_use]
    pub fn new(metadata: Arc<MetadataStore>) -> Self {
        Self::with_config(metadata, BalancerConfig::default())
    }

    /// Create a new Coordinator with an explicit [`BalancerConfig`].
    #[must_use]
    pub fn with_config(metadata: Arc<MetadataStore>, config: BalancerConfig) -> Self {
        Self {
            metadata,
            segment_assignments: RwLock::new(HashMap::new()),
            registry: RwLock::new(ServerRegistry::new()),
            config,
        }
    }

    // ----- Server registry -------------------------------------------------

    /// Register (or replace) a Historical server.
    pub async fn register_server(&self, server: ServerInfo) {
        let mut reg = self.registry.write().await;
        reg.register(server);
    }

    /// Update an existing server's tier and/or `max_size`, preserving the
    /// tracked `current_size`. Returns `true` if the server existed.
    pub async fn update_server(
        &self,
        name: &str,
        tier: Option<String>,
        max_size: Option<u64>,
    ) -> bool {
        let mut reg = self.registry.write().await;
        reg.update(name, tier, max_size)
    }

    /// Remove a server from the registry and forget its assignments.
    ///
    /// Returns the removed [`ServerInfo`] if it was registered. The segments it
    /// held become unassigned and will be re-placed on the next balance cycle.
    pub async fn remove_server(&self, name: &str) -> Option<ServerInfo> {
        let removed = {
            let mut reg = self.registry.write().await;
            reg.remove(name)
        };
        if removed.is_some() {
            let mut assignments = self.segment_assignments.write().await;
            assignments.remove(name);
        }
        removed
    }

    /// Return all registered servers (sorted by name), with their tracked
    /// `current_size` reflecting outstanding assignments.
    pub async fn get_servers(&self) -> Vec<ServerInfo> {
        let reg = self.registry.read().await;
        reg.list()
    }

    // ----- Balancing -------------------------------------------------------

    /// Run a balancing cycle: assign under-replicated segments to servers and
    /// drop over-replicated / unused / rule-dropped segments.
    ///
    /// `available_servers` augments the in-memory registry: any server passed
    /// here that is not yet registered is registered with `current_size = 0`
    /// before balancing, so callers can drive the cycle by simply passing the
    /// live cluster snapshot (preserving the prior round-robin API shape).
    /// Capacity gating uses the registry's tracked sizes and real segment byte
    /// sizes — not a placeholder.
    ///
    /// Returns the list of [`SegmentLoadAction`]s to dispatch to Historicals.
    pub async fn run_balance(
        &self,
        available_servers: &[ServerInfo],
    ) -> Result<Vec<SegmentLoadAction>> {
        // Fold any newly-seen servers into the registry. Existing servers keep
        // their tracked current_size (do not clobber it from the snapshot).
        {
            let mut reg = self.registry.write().await;
            for s in available_servers {
                if reg.get(&s.name).is_none() {
                    reg.register(s.clone());
                }
            }
        }
        self.run_balance_registered().await
    }

    /// Run a balancing cycle over the registered servers only.
    pub async fn run_balance_registered(&self) -> Result<Vec<SegmentLoadAction>> {
        let now = Utc::now();
        let servers = {
            let reg = self.registry.read().await;
            reg.list()
        };
        if servers.is_empty() {
            return Ok(Vec::new());
        }

        let segments = self.load_used_segments().await?;
        let rules_by_ds = self.load_rules_for(&segments).await?;

        let mut actions = Vec::new();
        let mut assignments = self.segment_assignments.write().await;
        let mut reg = self.registry.write().await;

        // Set of segment ids that are still used (for cleanup of stale drops).
        let used_ids: HashSet<&str> = segments.iter().map(|s| s.id.as_str()).collect();

        // 1. Drop any assignment whose segment is no longer used.
        for srv_name in reg.list().into_iter().map(|s| s.name) {
            if let Some(list) = assignments.get_mut(&srv_name) {
                let mut kept = Vec::with_capacity(list.len());
                for a in list.drain(..) {
                    if used_ids.contains(a.segment_id.as_str()) {
                        kept.push(a);
                    } else {
                        reg.sub_size(&srv_name, a.size_bytes);
                        actions.push(SegmentLoadAction::Drop {
                            server: srv_name.clone(),
                            segment_id: a.segment_id,
                        });
                    }
                }
                *list = kept;
            }
        }

        // 2. Per-segment desired placement.
        for seg in &segments {
            let interval = segment_interval(seg);
            let size = segment_size_bytes(seg);
            let rules = rules_by_ds
                .get(&seg.data_source)
                .map_or(&[][..], |v| &v[..]);
            let placement = rules::desired_placement(rules, &interval, &now)?;

            match placement {
                DesiredPlacement::Drop => {
                    Self::drop_segment_everywhere(
                        &mut assignments,
                        &mut reg,
                        &seg.id,
                        &mut actions,
                    );
                }
                DesiredPlacement::Broadcast => {
                    // Broadcast: ensure a copy on every server (any tier).
                    let all: Vec<ServerInfo> = reg.list();
                    self.ensure_replicas(
                        seg,
                        &interval,
                        size,
                        &all,
                        all.len(),
                        true,
                        &mut assignments,
                        &mut reg,
                        &mut actions,
                    );
                }
                DesiredPlacement::Load { tier_replicants } => {
                    // DD R24: a rule may request replicas across MULTIPLE tiers
                    // (e.g. `{"cold":1,"hot":1}`). Enforce every tier with a
                    // positive count independently, each with the DD R23
                    // Load-before-Drop discipline, then prune holders on tiers
                    // that are NOT requested — but only once every requested tier
                    // holds its desired count, so a full/absent target tier never
                    // costs the segment its only copy. A request with no positive
                    // tier (all zeros) means "load nowhere" -> drop everywhere.
                    let desired: Vec<(String, usize)> = tier_replicants
                        .iter()
                        .filter(|(_, r)| **r > 0)
                        .map(|(t, r)| (t.clone(), *r))
                        .collect();

                    if desired.is_empty() {
                        Self::drop_segment_everywhere(
                            &mut assignments,
                            &mut reg,
                            &seg.id,
                            &mut actions,
                        );
                    } else {
                        // Phase 1 (DD R25): LOAD-ONLY for every requested tier —
                        // add under-replicated copies but suppress surplus drops,
                        // so a full/absent tier cannot make another (overfull)
                        // tier shed live copies before the request is satisfied.
                        for (tier, replicas) in &desired {
                            let tier_servers: Vec<ServerInfo> =
                                reg.list().into_iter().filter(|s| &s.tier == tier).collect();
                            self.ensure_replicas(
                                seg,
                                &interval,
                                size,
                                &tier_servers,
                                *replicas,
                                false,
                                &mut assignments,
                                &mut reg,
                                &mut actions,
                            );
                        }
                        // Phase 2: only once EVERY requested tier holds its
                        // desired count, trim surplus within requested tiers and
                        // prune replicas on non-requested tiers.
                        let all_satisfied = desired.iter().all(|(tier, replicas)| {
                            Self::count_tier_replicas(&assignments, &reg, &seg.id, tier)
                                >= *replicas
                        });
                        if all_satisfied {
                            for (tier, replicas) in &desired {
                                let tier_servers: Vec<ServerInfo> =
                                    reg.list().into_iter().filter(|s| &s.tier == tier).collect();
                                self.ensure_replicas(
                                    seg,
                                    &interval,
                                    size,
                                    &tier_servers,
                                    *replicas,
                                    true,
                                    &mut assignments,
                                    &mut reg,
                                    &mut actions,
                                );
                            }
                            let wanted: HashSet<&str> =
                                desired.iter().map(|(t, _)| t.as_str()).collect();
                            Self::prune_other_tiers(
                                &mut assignments,
                                &mut reg,
                                &seg.id,
                                &wanted,
                                &mut actions,
                            );
                        }
                    }
                }
            }
        }

        Ok(actions)
    }

    /// Ensure the segment has exactly `target` replicas across `candidates`.
    ///
    /// Adds the cost-optimal new placements until `target` is met or capacity
    /// is exhausted; drops surplus replicas (over-replication) starting from the
    /// fullest server.
    #[allow(clippy::too_many_arguments)]
    fn ensure_replicas(
        &self,
        seg: &SegmentMetadataRow,
        interval: &Interval,
        size: u64,
        candidates: &[ServerInfo],
        target: usize,
        allow_surplus_drop: bool,
        assignments: &mut HashMap<String, Vec<SegmentAssignment>>,
        reg: &mut ServerRegistry,
        actions: &mut Vec<SegmentLoadAction>,
    ) {
        // Servers that currently hold this segment.
        let holders: Vec<String> = candidates
            .iter()
            .filter(|s| {
                assignments
                    .get(&s.name)
                    .is_some_and(|v| v.iter().any(|a| a.segment_id == seg.id))
            })
            .map(|s| s.name.clone())
            .collect();

        let mut current = holders.len();

        // Over-replicated: drop surplus, fullest server first. DD R25: when the
        // caller is mid-migration across multiple tiers, dropping surplus here
        // would shed live copies before the other requested tiers are satisfied,
        // dipping total availability below the desired count; the multi-tier path
        // suppresses this until every requested tier is satisfied.
        if current > target {
            if !allow_surplus_drop {
                return;
            }
            let mut sorted = holders.clone();
            sorted.sort_by(|a, b| {
                let fa = reg.get(a).map_or(0, |s| s.current_size);
                let fb = reg.get(b).map_or(0, |s| s.current_size);
                fb.cmp(&fa)
            });
            for name in sorted.into_iter().take(current - target) {
                Self::remove_assignment(assignments, reg, &name, &seg.id, actions);
            }
            return;
        }

        // Under-replicated: add cost-optimal placements not already holding it.
        while current < target {
            let holder_set: HashSet<String> = candidates
                .iter()
                .filter(|s| {
                    assignments
                        .get(&s.name)
                        .is_some_and(|v| v.iter().any(|a| a.segment_id == seg.id))
                })
                .map(|s| s.name.clone())
                .collect();

            let best =
                Self::pick_best_server(interval, size, candidates, &holder_set, reg, assignments);
            match best {
                Some(name) => {
                    let tier = reg
                        .get(&name)
                        .map_or_else(|| "_default_tier".to_string(), |s| s.tier.clone());
                    assignments
                        .entry(name.clone())
                        .or_default()
                        .push(SegmentAssignment {
                            segment_id: seg.id.clone(),
                            data_source: seg.data_source.clone(),
                            interval: interval.clone(),
                            tier,
                            size_bytes: size,
                        });
                    reg.add_size(&name, size);
                    actions.push(SegmentLoadAction::Load {
                        server: name,
                        segment_id: seg.id.clone(),
                    });
                    current += 1;
                }
                None => break, // no capacity / no remaining candidate
            }
        }
    }

    /// Pick the capacity-fitting candidate (excluding current holders) that
    /// minimizes the cost-based placement score, breaking ties by server name.
    fn pick_best_server(
        interval: &Interval,
        size: u64,
        candidates: &[ServerInfo],
        exclude: &HashSet<String>,
        reg: &ServerRegistry,
        assignments: &HashMap<String, Vec<SegmentAssignment>>,
    ) -> Option<String> {
        let mut best: Option<(String, f64)> = None;
        for cand in candidates {
            if exclude.contains(&cand.name) {
                continue;
            }
            let live = match reg.get(&cand.name) {
                Some(s) if s.can_hold(size) => s,
                _ => continue,
            };
            let cost = placement_cost(
                interval,
                &intervals_on(assignments, &cand.name),
                live.fill_ratio(),
            );
            let take = match &best {
                None => true,
                Some((bn, bc)) => cost < *bc || (cost == *bc && cand.name < *bn),
            };
            if take {
                best = Some((cand.name.clone(), cost));
            }
        }
        best.map(|(n, _)| n)
    }

    /// Drop a segment from every server that currently holds it.
    fn drop_segment_everywhere(
        assignments: &mut HashMap<String, Vec<SegmentAssignment>>,
        reg: &mut ServerRegistry,
        segment_id: &str,
        actions: &mut Vec<SegmentLoadAction>,
    ) {
        let holders: Vec<String> = assignments
            .iter()
            .filter(|(_, v)| v.iter().any(|a| a.segment_id == segment_id))
            .map(|(k, _)| k.clone())
            .collect();
        for name in holders {
            Self::remove_assignment(assignments, reg, &name, segment_id, actions);
        }
    }

    /// Drop replicas of `segment_id` that sit on a server outside `tier`.
    /// Count the replicas of `segment_id` held by servers in `tier`.
    fn count_tier_replicas(
        assignments: &HashMap<String, Vec<SegmentAssignment>>,
        reg: &ServerRegistry,
        segment_id: &str,
        tier: &str,
    ) -> usize {
        assignments
            .iter()
            .filter(|(name, v)| {
                v.iter().any(|a| a.segment_id == segment_id)
                    && reg.get(name).map(|s| s.tier.as_str()) == Some(tier)
            })
            .count()
    }

    /// Drop replicas of `segment_id` held by servers whose tier is NOT in
    /// `wanted`. DD R24: callers gate this on every wanted tier being satisfied
    /// so an unsatisfied target tier never costs the segment its last copy.
    fn prune_other_tiers(
        assignments: &mut HashMap<String, Vec<SegmentAssignment>>,
        reg: &mut ServerRegistry,
        segment_id: &str,
        wanted: &HashSet<&str>,
        actions: &mut Vec<SegmentLoadAction>,
    ) {
        let off_tier: Vec<String> = assignments
            .iter()
            .filter(|(name, v)| {
                v.iter().any(|a| a.segment_id == segment_id)
                    && !reg
                        .get(name)
                        .is_some_and(|s| wanted.contains(s.tier.as_str()))
            })
            .map(|(k, _)| k.clone())
            .collect();
        for name in off_tier {
            Self::remove_assignment(assignments, reg, &name, segment_id, actions);
        }
    }

    /// Remove a single assignment of `segment_id` from `server`, decrement the
    /// server's tracked size, and emit a Drop action.
    fn remove_assignment(
        assignments: &mut HashMap<String, Vec<SegmentAssignment>>,
        reg: &mut ServerRegistry,
        server: &str,
        segment_id: &str,
        actions: &mut Vec<SegmentLoadAction>,
    ) {
        if let Some(list) = assignments.get_mut(server)
            && let Some(pos) = list.iter().position(|a| a.segment_id == segment_id)
        {
            let a = list.remove(pos);
            reg.sub_size(server, a.size_bytes);
            actions.push(SegmentLoadAction::Drop {
                server: server.to_string(),
                segment_id: segment_id.to_string(),
            });
        }
    }

    /// Rebalance: move segments off the most heavily filled servers toward
    /// cost-optimal placement on the same tier, bounded by
    /// `max_segments_to_move`.
    ///
    /// Emits matched [`SegmentLoadAction::Drop`] + [`SegmentLoadAction::Load`]
    /// pairs and updates internal assignment + size tracking. A move is only
    /// performed when a strictly better-fitting server exists with capacity.
    pub async fn rebalance(&self) -> Result<Vec<SegmentLoadAction>> {
        let mut actions = Vec::new();
        let mut assignments = self.segment_assignments.write().await;
        let mut reg = self.registry.write().await;

        let mut moves_left = self.config.max_segments_to_move;
        if moves_left == 0 {
            return Ok(actions);
        }

        // Consider servers from fullest to emptiest.
        let mut servers = reg.list();
        servers.sort_by(|a, b| {
            b.fill_ratio()
                .partial_cmp(&a.fill_ratio())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        for src in &servers {
            if moves_left == 0 {
                break;
            }
            // Snapshot of segments currently on this source server.
            let segs: Vec<SegmentAssignment> =
                assignments.get(&src.name).cloned().unwrap_or_default();

            for a in segs {
                if moves_left == 0 {
                    break;
                }
                // Candidate destinations: same tier, not the source, with room.
                let dest_candidates: Vec<ServerInfo> = reg
                    .list()
                    .into_iter()
                    .filter(|s| s.tier == a.tier && s.name != src.name)
                    .collect();

                let src_cost = {
                    // Cost of the segment on its current server, *excluding
                    // itself* from both the co-location set and the fill ratio
                    // (so the comparison against a destination is symmetric: we
                    // weigh "segment here vs. there", not which server happens
                    // to currently hold it).
                    let others: Vec<Interval> =
                        intervals_on_excluding(&assignments, &src.name, &a.segment_id);
                    let fill = reg
                        .get(&src.name)
                        .map_or(1.0, |s| fill_ratio_without(s, a.size_bytes));
                    placement_cost(&a.interval, &others, fill)
                };

                // Find the best destination strictly cheaper than staying. The
                // destination cost includes the segment's own size in the fill
                // ratio so a pure swap between equal servers shows no gain.
                let mut best: Option<(String, f64)> = None;
                for cand in &dest_candidates {
                    let already = assignments
                        .get(&cand.name)
                        .is_some_and(|v| v.iter().any(|x| x.segment_id == a.segment_id));
                    if already {
                        continue;
                    }
                    let live = match reg.get(&cand.name) {
                        Some(s) if s.can_hold(a.size_bytes) => s,
                        _ => continue,
                    };
                    let cost = placement_cost(
                        &a.interval,
                        &intervals_on(&assignments, &cand.name),
                        fill_ratio_with(live, a.size_bytes),
                    );
                    let take = match &best {
                        None => true,
                        Some((bn, bc)) => cost < *bc || (cost == *bc && cand.name < *bn),
                    };
                    if take {
                        best = Some((cand.name.clone(), cost));
                    }
                }

                if let Some((dest, dest_cost)) = best
                    && dest_cost + COST_IMPROVEMENT_EPSILON < src_cost
                {
                    // Perform the move. CRITICAL ORDERING (DD R10 #5): emit the
                    // destination `Load` BEFORE the source `Drop`. A move of a
                    // 1-replica segment must never make it unavailable if the
                    // load fails or is delayed. Dispatchers MUST NOT execute the
                    // `Drop` until the corresponding `Load` has been
                    // acknowledged on `dest`; the action vector reflects that
                    // safe order (Load first, then Drop).
                    let tier = reg
                        .get(&dest)
                        .map_or_else(|| a.tier.clone(), |s| s.tier.clone());
                    actions.push(SegmentLoadAction::Load {
                        server: dest.clone(),
                        segment_id: a.segment_id.clone(),
                    });
                    assignments
                        .entry(dest.clone())
                        .or_default()
                        .push(SegmentAssignment { tier, ..a.clone() });
                    reg.add_size(&dest, a.size_bytes);
                    // Now retire the source replica (pushes the `Drop` and
                    // updates assignment/size bookkeeping).
                    Self::remove_assignment(
                        &mut assignments,
                        &mut reg,
                        &src.name,
                        &a.segment_id,
                        &mut actions,
                    );
                    moves_left -= 1;
                }
            }
        }

        Ok(actions)
    }

    // ----- Metadata helpers ------------------------------------------------

    /// Load every used segment across all data sources.
    async fn load_used_segments(&self) -> Result<Vec<SegmentMetadataRow>> {
        let data_sources = self.metadata.get_all_data_sources().await?;
        let mut all = Vec::new();
        for ds in &data_sources {
            all.extend(self.metadata.get_used_segments(ds).await?);
        }
        Ok(all)
    }

    /// Load + parse the rule chain for each distinct data source in `segments`.
    async fn load_rules_for(
        &self,
        segments: &[SegmentMetadataRow],
    ) -> Result<HashMap<String, Vec<ferrodruid_loadrules::LoadRule>>> {
        let mut out = HashMap::new();
        let distinct: HashSet<&str> = segments.iter().map(|s| s.data_source.as_str()).collect();
        for ds in distinct {
            let raw = self.metadata.get_rules(ds).await?;
            let parsed = rules::parse_rules(&raw)?;
            out.insert(ds.to_string(), parsed);
        }
        Ok(out)
    }

    // ----- Read accessors / lifecycle (preserved API) ----------------------

    /// Get all segments currently assigned to a server.
    pub async fn get_segments_for_server(&self, server: &str) -> Vec<SegmentAssignment> {
        let assignments = self.segment_assignments.read().await;
        assignments.get(server).cloned().unwrap_or_default()
    }

    /// Return all distinct data source names from the metadata store.
    pub async fn get_data_sources(&self) -> Result<Vec<String>> {
        self.metadata.get_all_data_sources().await
    }

    /// Disable a datasource by marking all its segments as unused.
    ///
    /// Serialized against segment publication (Codex 2026-07-12 round-2
    /// HIGH #3): the whole read-used-set → flip loop runs under the
    /// datasource's [`MetadataStore::datasource_publish_lock`], the same
    /// mutex the Overlord holds for its plan → metadata-transaction →
    /// segment-swap → rollback critical section. Without it, a disable
    /// could land between a publish's metadata insert and its Historical
    /// swap (making a just-disabled segment query-visible), or between a
    /// failing publish's victim flip and its rollback (which would then
    /// resurrect the admin's disable).
    pub async fn disable_datasource(&self, data_source: &str) -> Result<()> {
        let lock = self.metadata.datasource_publish_lock(data_source).await;
        let _guard = lock.lock().await;
        // ONE atomic UPDATE (Codex round-3) — a per-segment loop of
        // autocommits could leave a durable partially-disabled state on
        // cancellation / mid-loop failure.
        self.metadata.set_datasource_used(data_source, false).await
    }

    /// Enable a datasource by re-marking all its segments as used.
    ///
    /// Serialized against segment publication via the datasource's
    /// [`MetadataStore::datasource_publish_lock`] — see
    /// [`disable_datasource`] for the rationale (an enable interleaving
    /// with a replace could resurrect a row the replace just retired).
    ///
    /// [`disable_datasource`]: Coordinator::disable_datasource
    pub async fn enable_datasource(&self, data_source: &str) -> Result<()> {
        let lock = self.metadata.datasource_publish_lock(data_source).await;
        let _guard = lock.lock().await;
        // ONE atomic UPDATE (Codex round-3) — see `disable_datasource`.
        self.metadata.set_datasource_used(data_source, true).await
    }

    /// Disable a specific segment by marking it as unused.
    ///
    /// Serialized against segment publication via the owning datasource's
    /// [`MetadataStore::datasource_publish_lock`] — see
    /// [`disable_datasource`] for the rationale. A segment id that has no
    /// metadata row keeps the pre-existing no-op semantics (the underlying
    /// `UPDATE` matched zero rows before this change).
    ///
    /// [`disable_datasource`]: Coordinator::disable_datasource
    pub async fn disable_segment(&self, segment_id: &str) -> Result<()> {
        // A segment's id -> datasource mapping is immutable, so looking it
        // up before taking the lock is race-free.
        let Some(row) = self.metadata.get_segment(segment_id).await? else {
            return Ok(());
        };
        let lock = self
            .metadata
            .datasource_publish_lock(&row.data_source)
            .await;
        let _guard = lock.lock().await;
        self.metadata.mark_segment_unused(segment_id).await
    }

    /// Return per-data-source used-segment counts.
    pub async fn get_segment_counts(&self) -> Result<HashMap<String, usize>> {
        let data_sources = self.metadata.get_all_data_sources().await?;
        let mut counts = HashMap::with_capacity(data_sources.len());
        for ds in &data_sources {
            let segs = self.metadata.get_used_segments(ds).await?;
            counts.insert(ds.clone(), segs.len());
        }
        Ok(counts)
    }
}

/// Minimum cost improvement required to justify a rebalance move; avoids
/// thrashing between near-equal-cost placements.
const COST_IMPROVEMENT_EPSILON: f64 = FILL_PENALTY_WEIGHT * 1e-6;

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Parse a segment's `[start, end)` interval, defaulting to a zero-width
/// interval at the Unix epoch if either bound fails to parse (degenerate
/// intervals contribute zero cost rather than panicking).
fn segment_interval(seg: &SegmentMetadataRow) -> Interval {
    let epoch = DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_default();
    let start = seg.start.parse().unwrap_or(epoch);
    let end = seg.end.parse().unwrap_or(start);
    Interval { start, end }
}

/// Fill ratio of `server` as if `size` bytes were removed from it.
fn fill_ratio_without(server: &ServerInfo, size: u64) -> f64 {
    if server.max_size == 0 {
        return 1.0;
    }
    let current = server.current_size.saturating_sub(size) as f64;
    (current / server.max_size as f64).clamp(0.0, 1.0)
}

/// Fill ratio of `server` as if `size` bytes were added to it.
fn fill_ratio_with(server: &ServerInfo, size: u64) -> f64 {
    if server.max_size == 0 {
        return 1.0;
    }
    let current = server.current_size.saturating_add(size) as f64;
    (current / server.max_size as f64).clamp(0.0, 1.0)
}

/// Intervals of all segments currently placed on `server`.
fn intervals_on(
    assignments: &HashMap<String, Vec<SegmentAssignment>>,
    server: &str,
) -> Vec<Interval> {
    assignments
        .get(server)
        .map(|v| v.iter().map(|a| a.interval.clone()).collect())
        .unwrap_or_default()
}

/// Intervals of segments on `server`, excluding the assignment for
/// `exclude_id` (used to compute a segment's own cost without self-counting).
fn intervals_on_excluding(
    assignments: &HashMap<String, Vec<SegmentAssignment>>,
    server: &str,
    exclude_id: &str,
) -> Vec<Interval> {
    assignments
        .get(server)
        .map(|v| {
            v.iter()
                .filter(|a| a.segment_id != exclude_id)
                .map(|a| a.interval.clone())
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
