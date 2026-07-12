// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Cluster state management for FerroDruid.
//!
//! Provides a Raft-ready abstraction layer that can be backed by either
//! in-memory (single-node) or openraft (future multi-node consensus).
//! [`ClusterManager`] is the primary entry point for all cluster operations.
//! The legacy [`ClusterState`] is preserved for backward compatibility.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod auth;
pub mod persist;
pub mod replication;
#[cfg(feature = "cluster-tls")]
pub mod tls;
pub mod transport;

/// The role a node plays in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Master / coordinator leader.
    Master,
    /// Query-serving node (broker).
    Query,
    /// Data-serving node (historical).
    Data,
    /// All roles in a single process (single-binary mode).
    AllInOne,
}

/// Information about a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique node identifier.
    pub id: String,
    /// Hostname or IP address.
    pub host: String,
    /// Port for the HTTP API.
    pub port: u16,
    /// Role of this node.
    pub role: NodeRole,
}

/// A registered service instance in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEntry {
    /// The type of service (e.g. `"broker"`, `"historical"`, `"coordinator"`, `"overlord"`).
    pub service_type: String,
    /// Hostname or IP address.
    pub host: String,
    /// Port number.
    pub port: u16,
    /// ID of the node hosting this service.
    pub node_id: String,
}

/// An announcement that a particular server is serving a segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentAnnouncement {
    /// Segment identifier.
    pub segment_id: String,
    /// Name of the server serving this segment.
    pub server_name: String,
    /// Data source the segment belongs to.
    pub data_source: String,
    /// Storage tier (e.g. `"_default_tier"`, `"hot"`, `"cold"`).
    pub tier: String,
}

// ---------------------------------------------------------------------------
// ClusterState (legacy, preserved for backward compatibility)
// ---------------------------------------------------------------------------

/// Cluster state manager.
///
/// Phase 1: single-node in-memory state. All operations are synchronous
/// behind `RwLock` guards.  The state machine interface is designed to be
/// compatible with a future openraft-backed implementation.
pub struct ClusterState {
    leader: RwLock<Option<NodeInfo>>,
    services: RwLock<HashMap<String, Vec<ServiceEntry>>>,
    segment_announcements: RwLock<HashMap<String, Vec<SegmentAnnouncement>>>,
}

impl ClusterState {
    /// Create a cluster state for a single-node deployment.
    ///
    /// The provided node is immediately registered as the leader.
    pub fn new_single_node(node: NodeInfo) -> Self {
        Self {
            leader: RwLock::new(Some(node)),
            services: RwLock::new(HashMap::new()),
            segment_announcements: RwLock::new(HashMap::new()),
        }
    }

    /// Get the current cluster leader, if any.
    pub fn get_leader(&self) -> Option<NodeInfo> {
        self.leader.read().ok().and_then(|g| g.clone())
    }

    /// Register a service entry.
    ///
    /// If a service with the same `(node_id, service_type)` already exists,
    /// the old entry is replaced.
    ///
    /// **Lock poisoning is silently ignored** for backward compatibility
    /// with pre-Wave-45-C callers (e.g. `ferrodruid-discovery`).  Code
    /// that needs to surface poisoning to callers should use
    /// [`Self::register_service_checked`] (Wave 45-C closure of
    /// Wave 37B `cluster_lib` Medium #1).
    pub fn register_service(&self, entry: ServiceEntry) {
        let _ = self.register_service_checked(entry);
    }

    /// Register a service entry, returning `Err(ClusterError::LockPoisoned)`
    /// when the internal service map lock has been poisoned.
    ///
    /// Wave 45-C closure of Wave 37B `cluster_lib` Medium #1: pre-fix
    /// the legacy [`Self::register_service`] swallowed `RwLock` poison
    /// silently, so [`ClusterManager::apply`] for
    /// [`ClusterCommand::RegisterService`] returned `CommandResult::Ok`
    /// even when no state change had occurred.  Callers that go
    /// through `apply()` now propagate the failure so the discrepancy
    /// is observable in CLI / REST surfaces and audit logs.
    pub fn register_service_checked(&self, entry: ServiceEntry) -> Result<(), ClusterError> {
        let mut services = self
            .services
            .write()
            .map_err(|_| ClusterError::LockPoisoned)?;
        let entries = services.entry(entry.service_type.clone()).or_default();
        entries.retain(|e| e.node_id != entry.node_id);
        entries.push(entry);
        Ok(())
    }

    /// Deregister a service entry by node ID and service type.
    ///
    /// Lock-poison handling matches [`Self::register_service`]; for a
    /// `Result`-returning variant see [`Self::deregister_service_checked`].
    pub fn deregister_service(&self, node_id: &str, service_type: &str) {
        let _ = self.deregister_service_checked(node_id, service_type);
    }

    /// Deregister a service entry, returning `Err(ClusterError::LockPoisoned)`
    /// when the internal service map lock has been poisoned.
    ///
    /// Wave 45-C closure of Wave 37B `cluster_lib` Medium #1.
    pub fn deregister_service_checked(
        &self,
        node_id: &str,
        service_type: &str,
    ) -> Result<(), ClusterError> {
        let mut services = self
            .services
            .write()
            .map_err(|_| ClusterError::LockPoisoned)?;
        if let Some(entries) = services.get_mut(service_type) {
            entries.retain(|e| e.node_id != node_id);
        }
        Ok(())
    }

    /// Get all service entries for a given service type.
    pub fn get_services(&self, service_type: &str) -> Vec<ServiceEntry> {
        match self.services.read() {
            Ok(services) => services.get(service_type).cloned().unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    }

    /// Announce that a server is serving a segment.
    ///
    /// Lock-poison handling matches [`Self::register_service`]; for a
    /// `Result`-returning variant see [`Self::announce_segment_checked`].
    pub fn announce_segment(&self, announcement: SegmentAnnouncement) {
        let _ = self.announce_segment_checked(announcement);
    }

    /// Announce a segment, returning `Err(ClusterError::LockPoisoned)`
    /// when the internal announcement map lock has been poisoned.
    ///
    /// Wave 45-C closure of Wave 37B `cluster_lib` Medium #1.
    pub fn announce_segment_checked(
        &self,
        announcement: SegmentAnnouncement,
    ) -> Result<(), ClusterError> {
        let mut announcements = self
            .segment_announcements
            .write()
            .map_err(|_| ClusterError::LockPoisoned)?;
        let entries = announcements
            .entry(announcement.data_source.clone())
            .or_default();
        // Avoid duplicate announcements for the same (segment_id, server_name).
        entries.retain(|a| {
            a.segment_id != announcement.segment_id || a.server_name != announcement.server_name
        });
        entries.push(announcement);
        Ok(())
    }

    /// Remove a segment announcement for a specific server.
    ///
    /// Lock-poison handling matches [`Self::register_service`]; for a
    /// `Result`-returning variant see [`Self::remove_segment_announcement_checked`].
    pub fn remove_segment_announcement(&self, segment_id: &str, server_name: &str) {
        let _ = self.remove_segment_announcement_checked(segment_id, server_name);
    }

    /// Remove a segment announcement, returning `Err(ClusterError::LockPoisoned)`
    /// when the internal announcement map lock has been poisoned.
    ///
    /// Wave 45-C closure of Wave 37B `cluster_lib` Medium #1.
    pub fn remove_segment_announcement_checked(
        &self,
        segment_id: &str,
        server_name: &str,
    ) -> Result<(), ClusterError> {
        let mut announcements = self
            .segment_announcements
            .write()
            .map_err(|_| ClusterError::LockPoisoned)?;
        for entries in announcements.values_mut() {
            entries.retain(|a| a.segment_id != segment_id || a.server_name != server_name);
        }
        Ok(())
    }

    /// Get all segment announcements for a data source.
    pub fn get_segment_announcements(&self, data_source: &str) -> Vec<SegmentAnnouncement> {
        match self.segment_announcements.read() {
            Ok(announcements) => announcements.get(data_source).cloned().unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Raft-ready command/result types
// ---------------------------------------------------------------------------

/// A state machine command that can be replicated via Raft (or applied locally in single-node).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterCommand {
    /// Leader election.
    ElectLeader {
        /// Role to elect leader for (e.g. `"coordinator"`).
        role: String,
        /// Node ID of the candidate.
        node_id: String,
    },
    /// Service registration.
    RegisterService(ServiceEntry),
    /// Service deregistration.
    DeregisterService {
        /// Node ID to deregister.
        node_id: String,
        /// Service type to deregister.
        service_type: String,
    },
    /// Segment announcement.
    AnnounceSegment(SegmentAnnouncement),
    /// Segment removal.
    RemoveSegment {
        /// Segment ID to remove.
        segment_id: String,
        /// Server name to remove from.
        server_name: String,
    },
    /// Segment load/drop queue.
    EnqueueSegmentAction {
        /// Server name for the action.
        server_name: String,
        /// Segment ID for the action.
        segment_id: String,
        /// Action to enqueue.
        action: SegmentAction,
    },
    /// Task lock acquisition.
    AcquireTaskLock {
        /// Task identifier.
        task_id: String,
        /// Data source the task operates on.
        data_source: String,
        /// Start of the interval.
        interval_start: String,
        /// End of the interval.
        interval_end: String,
    },
    /// Task lock release.
    ReleaseTaskLock {
        /// Task identifier to release.
        task_id: String,
    },
}

/// Action to perform on a segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SegmentAction {
    /// Load the segment into memory.
    Load,
    /// Drop the segment from memory.
    Drop,
}

/// Result of applying a command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommandResult {
    /// Command succeeded.
    Ok,
    /// Leader was elected.
    LeaderElected {
        /// Node ID of the elected leader.
        node_id: String,
    },
    /// Lock was acquired.
    LockAcquired {
        /// Unique lock identifier.
        lock_id: String,
    },
    /// Lock was denied.
    LockDenied {
        /// Reason the lock was denied.
        reason: String,
    },
    /// Requested resource was not found.
    NotFound,
}

/// A task lock held by the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLock {
    /// Task identifier.
    pub task_id: String,
    /// Data source the task operates on.
    pub data_source: String,
    /// Start of the interval.
    pub interval_start: String,
    /// End of the interval.
    pub interval_end: String,
    /// When the lock was acquired.
    pub acquired_at: chrono::DateTime<Utc>,
    /// Unique lock identifier.
    pub lock_id: String,
}

/// A queued segment action for a server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedSegmentAction {
    /// Segment identifier.
    pub segment_id: String,
    /// Action to perform.
    pub action: SegmentAction,
    /// When the action was queued.
    pub queued_at: chrono::DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Cluster mode
// ---------------------------------------------------------------------------

/// Operating mode of the cluster manager.
#[derive(Debug, Clone)]
pub enum ClusterMode {
    /// Single-node mode: all commands applied immediately in-process.
    SingleNode,
    /// Multi-node mode: commands would be replicated via Raft consensus.
    /// In this phase, commands are still applied locally but the mode is
    /// tracked for future openraft integration.
    MultiNode {
        /// Peer addresses for Raft communication.
        peers: Vec<String>,
    },
}

// ---------------------------------------------------------------------------
// ClusterHealth
// ---------------------------------------------------------------------------

/// Health status of the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealth {
    /// Whether the cluster is healthy.
    pub is_healthy: bool,
    /// Current leader node ID, if any.
    pub leader: Option<String>,
    /// Number of nodes in the cluster.
    pub node_count: usize,
    /// Operating mode.
    pub mode: String,
}

// ---------------------------------------------------------------------------
// ClusterSnapshot
// ---------------------------------------------------------------------------

/// A snapshot of the full cluster state, suitable for Raft snapshot transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    /// All registered services, keyed by service type.
    pub services: HashMap<String, Vec<ServiceEntry>>,
    /// All segment announcements, keyed by data source.
    pub segments: HashMap<String, Vec<SegmentAnnouncement>>,
    /// All active task locks, keyed by task ID.
    pub task_locks: HashMap<String, TaskLock>,
    /// Current leader, if any.
    pub leader: Option<NodeInfo>,
    /// Segment action queue, keyed by server name.
    pub segment_queue: HashMap<String, Vec<QueuedSegmentAction>>,
}

// ---------------------------------------------------------------------------
// ClusterManager
// ---------------------------------------------------------------------------

/// Raft-ready cluster manager supporting single-node and multi-node modes.
///
/// In single-node mode, all commands are applied immediately to the in-memory
/// state. In multi-node mode, the same command interface is preserved but
/// commands would be replicated via Raft consensus (future openraft integration).
pub struct ClusterManager {
    mode: ClusterMode,
    state: ClusterState,
    node_info: NodeInfo,
    peers: RwLock<Vec<NodeInfo>>,
    task_locks: RwLock<HashMap<String, TaskLock>>,
    segment_queue: RwLock<HashMap<String, Vec<QueuedSegmentAction>>>,
}

/// Errors that can occur in cluster operations.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// Internal lock poisoned.
    #[error("internal lock poisoned")]
    LockPoisoned,
    /// Command failed.
    #[error("command failed: {0}")]
    CommandFailed(String),
    /// Per-server or global segment-action queue cap exceeded.
    ///
    /// Wave 45-D closure of W37B `cluster_lib` Medium #3 (DoS): the
    /// per-server segment-action queue previously grew without bound and
    /// repeated `EnqueueSegmentAction` traffic could exhaust historical
    /// memory before any consumer dequeued.  `apply()` now rejects new
    /// enqueues with `QueueFull` once either the per-server or global
    /// cap is reached.
    #[error("segment action queue full: {scope} (cap={cap}, current={current})")]
    QueueFull {
        /// `"per-server <name>"` or `"global"`.
        scope: String,
        /// The cap that was reached.
        cap: usize,
        /// The current size at the time of refusal (`>= cap`).
        current: usize,
    },
}

/// Maximum number of pending segment actions per server (Wave 45-D).
///
/// Picked to comfortably accommodate the largest legitimate burst we
/// observe in practice (Raft replay at `MAX_REPLAY_BATCH = 1000`
/// distinct segments) plus an order of magnitude headroom, while still
/// being small enough that 256-byte action records cap RAM at
/// ~16 MiB / server.
pub const MAX_SEGMENT_QUEUE_PER_SERVER: usize = 65_536;

/// Maximum number of pending segment actions across all servers (Wave 45-D).
///
/// Globally caps coordinator memory at ~256 MiB worst-case
/// (1_048_576 entries × ~256 B per `QueuedSegmentAction`).
pub const MAX_SEGMENT_QUEUE_TOTAL: usize = 1_048_576;

impl ClusterManager {
    /// Create a single-node cluster manager.
    ///
    /// The provided node is immediately registered as the leader.
    pub fn new_single_node(node: NodeInfo) -> Self {
        let state = ClusterState::new_single_node(node.clone());
        Self {
            mode: ClusterMode::SingleNode,
            state,
            node_info: node,
            peers: RwLock::new(Vec::new()),
            task_locks: RwLock::new(HashMap::new()),
            segment_queue: RwLock::new(HashMap::new()),
        }
    }

    /// Create a multi-node cluster manager.
    ///
    /// Peer discovery and Raft networking are deferred to future openraft
    /// integration. In this phase, commands are still applied locally.
    pub fn new_multi_node(node: NodeInfo, peers: Vec<String>) -> Self {
        let state = ClusterState::new_single_node(node.clone());
        let peer_infos: Vec<NodeInfo> = peers
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let (host, port) = parse_host_port(addr);
                NodeInfo {
                    id: format!("peer-{i}"),
                    host,
                    port,
                    role: NodeRole::AllInOne,
                }
            })
            .collect();
        Self {
            mode: ClusterMode::MultiNode { peers },
            state,
            node_info: node,
            peers: RwLock::new(peer_infos),
            task_locks: RwLock::new(HashMap::new()),
            segment_queue: RwLock::new(HashMap::new()),
        }
    }

    /// Apply a command to the cluster state machine.
    ///
    /// In single-node mode, the command is applied immediately. In multi-node
    /// mode (future), the command would first be replicated via Raft consensus.
    pub fn apply(&self, cmd: ClusterCommand) -> Result<CommandResult, ClusterError> {
        match cmd {
            ClusterCommand::ElectLeader { role: _, node_id } => {
                // W37B cluster_lib Medium #2 closure: previously this
                // branch always copied the local node's host/port/role
                // even when electing a remote peer, so clients sent to
                // a leader endpoint that did not exist. Resolve from
                // the configured peer set first, then fall back to
                // local node info only when the elected id matches
                // self (or no peer matches; we still publish *some*
                // leader to preserve liveness, but log a warning so
                // operators notice misconfiguration).
                let node = if node_id == self.node_info.id {
                    NodeInfo {
                        id: node_id.clone(),
                        host: self.node_info.host.clone(),
                        port: self.node_info.port,
                        role: self.node_info.role,
                    }
                } else {
                    let peers = self.peers.read().map_err(|_| ClusterError::LockPoisoned)?;
                    if let Some(peer) = peers.iter().find(|p| p.id == node_id) {
                        peer.clone()
                    } else {
                        tracing::warn!(
                            elected = %node_id,
                            "ElectLeader for unknown peer id; falling back to id-only \
                             leader record (host/port left as local)",
                        );
                        NodeInfo {
                            id: node_id.clone(),
                            host: self.node_info.host.clone(),
                            port: self.node_info.port,
                            role: self.node_info.role,
                        }
                    }
                };
                let mut leader = self
                    .state
                    .leader
                    .write()
                    .map_err(|_| ClusterError::LockPoisoned)?;
                *leader = Some(node);
                Ok(CommandResult::LeaderElected { node_id })
            }
            ClusterCommand::RegisterService(entry) => {
                // W37B cluster_lib Medium #1 closure (Wave 45-C): pre-fix
                // a poisoned services lock would have been swallowed and
                // `apply()` would still return `Ok` even though no state
                // change happened.  We now propagate via `_checked`.
                self.state.register_service_checked(entry)?;
                Ok(CommandResult::Ok)
            }
            ClusterCommand::DeregisterService {
                node_id,
                service_type,
            } => {
                self.state
                    .deregister_service_checked(&node_id, &service_type)?;
                Ok(CommandResult::Ok)
            }
            ClusterCommand::AnnounceSegment(announcement) => {
                self.state.announce_segment_checked(announcement)?;
                Ok(CommandResult::Ok)
            }
            ClusterCommand::RemoveSegment {
                segment_id,
                server_name,
            } => {
                self.state
                    .remove_segment_announcement_checked(&segment_id, &server_name)?;
                Ok(CommandResult::Ok)
            }
            ClusterCommand::EnqueueSegmentAction {
                server_name,
                segment_id,
                action,
            } => {
                // W37B cluster_lib Medium #3 closure (Wave 45-D): the
                // pre-fix arm appended unconditionally with no per-server
                // or global cap and no coalescing, so a malicious or
                // misconfigured producer could grow `segment_queue`
                // without bound.  We now (a) coalesce by `segment_id`
                // with last-write-wins on the action (a Drop after a
                // pending Load supersedes the Load and vice versa,
                // matching coordinator intent), and (b) enforce caps.
                let mut queue = self
                    .segment_queue
                    .write()
                    .map_err(|_| ClusterError::LockPoisoned)?;

                let actions = queue.entry(server_name.clone()).or_default();

                // Coalesce: last-write-wins on (segment_id) for this
                // server.  We sweep the existing list once, drop any
                // entry that targets the same segment_id, and append
                // the fresh action at the tail to preserve FIFO for
                // distinct segments.
                let pre_len = actions.len();
                actions.retain(|a| a.segment_id != segment_id);
                let coalesced = actions.len() < pre_len;

                // Cap check — counts the about-to-be-pushed entry.
                // We compute the post-push per-server size and the
                // post-push global size, then refuse if either exceeds
                // the cap.  Note: coalescing reduces both counts
                // before this check, so a steady stream of
                // duplicate-`segment_id` enqueues never trips the cap.
                let new_per_server = actions.len() + 1;
                if new_per_server > MAX_SEGMENT_QUEUE_PER_SERVER {
                    return Err(ClusterError::QueueFull {
                        scope: format!("per-server {server_name}"),
                        cap: MAX_SEGMENT_QUEUE_PER_SERVER,
                        current: new_per_server,
                    });
                }
                let new_global: usize =
                    queue.values().map(Vec::len).sum::<usize>() + (if coalesced { 0 } else { 1 });
                if new_global > MAX_SEGMENT_QUEUE_TOTAL {
                    return Err(ClusterError::QueueFull {
                        scope: "global".to_string(),
                        cap: MAX_SEGMENT_QUEUE_TOTAL,
                        current: new_global,
                    });
                }

                let actions = queue.entry(server_name).or_default();
                actions.push(QueuedSegmentAction {
                    segment_id,
                    action,
                    queued_at: Utc::now(),
                });
                Ok(CommandResult::Ok)
            }
            ClusterCommand::AcquireTaskLock {
                task_id,
                data_source,
                interval_start,
                interval_end,
            } => self.apply_acquire_lock(task_id, data_source, interval_start, interval_end),
            ClusterCommand::ReleaseTaskLock { task_id } => {
                let mut locks = self
                    .task_locks
                    .write()
                    .map_err(|_| ClusterError::LockPoisoned)?;
                if locks.remove(&task_id).is_some() {
                    Ok(CommandResult::Ok)
                } else {
                    Ok(CommandResult::NotFound)
                }
            }
        }
    }

    /// Apply a task lock acquisition, checking for conflicts.
    fn apply_acquire_lock(
        &self,
        task_id: String,
        data_source: String,
        interval_start: String,
        interval_end: String,
    ) -> Result<CommandResult, ClusterError> {
        let mut locks = self
            .task_locks
            .write()
            .map_err(|_| ClusterError::LockPoisoned)?;

        // Check for overlapping locks on the same data source.
        for existing in locks.values() {
            if existing.data_source == data_source
                && intervals_overlap(
                    &existing.interval_start,
                    &existing.interval_end,
                    &interval_start,
                    &interval_end,
                )
            {
                return Ok(CommandResult::LockDenied {
                    reason: format!(
                        "overlapping lock held by task '{}' on data_source '{}'",
                        existing.task_id, data_source
                    ),
                });
            }
        }

        let lock_id = uuid::Uuid::new_v4().to_string();
        let task_lock = TaskLock {
            task_id: task_id.clone(),
            data_source,
            interval_start,
            interval_end,
            acquired_at: Utc::now(),
            lock_id: lock_id.clone(),
        };
        locks.insert(task_id, task_lock);
        Ok(CommandResult::LockAcquired { lock_id })
    }

    /// Get the current cluster leader, if any.
    pub fn leader(&self) -> Option<NodeInfo> {
        self.state.get_leader()
    }

    /// Check whether this node is the current leader.
    pub fn is_leader(&self) -> bool {
        self.state
            .get_leader()
            .map(|l| l.id == self.node_info.id)
            .unwrap_or(false)
    }

    /// Get all registered services of a given type.
    pub fn services(&self, service_type: &str) -> Vec<ServiceEntry> {
        self.state.get_services(service_type)
    }

    /// Register a service entry (convenience wrapper around [`apply`]).
    pub fn register_service(&self, entry: ServiceEntry) {
        self.state.register_service(entry);
    }

    /// Deregister a service entry (convenience wrapper).
    pub fn deregister_service(&self, node_id: &str, service_type: &str) {
        self.state.deregister_service(node_id, service_type);
    }

    /// Get all segment announcements for a data source.
    pub fn segments(&self, data_source: &str) -> Vec<SegmentAnnouncement> {
        self.state.get_segment_announcements(data_source)
    }

    /// Get pending segment actions for a server.
    pub fn pending_actions(&self, server_name: &str) -> Vec<QueuedSegmentAction> {
        match self.segment_queue.read() {
            Ok(queue) => queue.get(server_name).cloned().unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    }

    /// Drain pending segment actions for a server (Wave 45-D).
    ///
    /// Removes the per-server queue and returns its contents in FIFO
    /// order.  This is the dequeue/ack half of the queue-cap
    /// (W37B `cluster_lib` Medium #3) closure: consumers that previously
    /// could only `pending_actions()` (a clone) and then *separately*
    /// observe drain via some other channel can now atomically take
    /// ownership.  Returns an empty `Vec` if the server has no queue
    /// or if the lock is poisoned.
    pub fn dequeue_actions(&self, server_name: &str) -> Vec<QueuedSegmentAction> {
        match self.segment_queue.write() {
            Ok(mut queue) => queue.remove(server_name).unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    }

    /// Total number of queued segment actions across all servers
    /// (Wave 45-D, observability for `MAX_SEGMENT_QUEUE_TOTAL`).
    pub fn segment_queue_total(&self) -> usize {
        match self.segment_queue.read() {
            Ok(queue) => queue.values().map(Vec::len).sum(),
            Err(_) => 0,
        }
    }

    /// Try to acquire a task lock.
    pub fn try_lock(
        &self,
        task_id: &str,
        data_source: &str,
        interval_start: &str,
        interval_end: &str,
    ) -> Result<CommandResult, ClusterError> {
        self.apply(ClusterCommand::AcquireTaskLock {
            task_id: task_id.to_string(),
            data_source: data_source.to_string(),
            interval_start: interval_start.to_string(),
            interval_end: interval_end.to_string(),
        })
    }

    /// Release a task lock.
    pub fn unlock(&self, task_id: &str) -> Result<(), ClusterError> {
        let result = self.apply(ClusterCommand::ReleaseTaskLock {
            task_id: task_id.to_string(),
        })?;
        match result {
            CommandResult::Ok | CommandResult::NotFound => Ok(()),
            _ => Err(ClusterError::CommandFailed(
                "unexpected result from unlock".to_string(),
            )),
        }
    }

    /// Get all active task locks.
    pub fn active_locks(&self) -> Vec<TaskLock> {
        match self.task_locks.read() {
            Ok(locks) => locks.values().cloned().collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Check cluster health.
    pub fn health(&self) -> ClusterHealth {
        let leader = self.state.get_leader().map(|l| l.id);
        let node_count = self.node_count();
        let mode_str = match &self.mode {
            ClusterMode::SingleNode => "single-node".to_string(),
            ClusterMode::MultiNode { peers } => format!("multi-node ({} peers)", peers.len()),
        };
        ClusterHealth {
            is_healthy: leader.is_some(),
            leader,
            node_count,
            mode: mode_str,
        }
    }

    /// Get the node count (including self).
    pub fn node_count(&self) -> usize {
        match self.peers.read() {
            Ok(peers) => 1 + peers.len(),
            Err(_) => 1,
        }
    }

    /// Snapshot the full state (for Raft snapshot transfer).
    pub fn snapshot(&self) -> ClusterSnapshot {
        let services = self
            .state
            .services
            .read()
            .map(|s| s.clone())
            .unwrap_or_default();
        let segments = self
            .state
            .segment_announcements
            .read()
            .map(|s| s.clone())
            .unwrap_or_default();
        let task_locks = self
            .task_locks
            .read()
            .map(|l| l.clone())
            .unwrap_or_default();
        let leader = self.state.get_leader();
        let segment_queue = self
            .segment_queue
            .read()
            .map(|q| q.clone())
            .unwrap_or_default();

        ClusterSnapshot {
            services,
            segments,
            task_locks,
            leader,
            segment_queue,
        }
    }

    /// Restore state from a snapshot.
    pub fn restore(&self, snapshot: ClusterSnapshot) -> Result<(), ClusterError> {
        {
            let mut leader = self
                .state
                .leader
                .write()
                .map_err(|_| ClusterError::LockPoisoned)?;
            *leader = snapshot.leader;
        }
        {
            let mut services = self
                .state
                .services
                .write()
                .map_err(|_| ClusterError::LockPoisoned)?;
            *services = snapshot.services;
        }
        {
            let mut segments = self
                .state
                .segment_announcements
                .write()
                .map_err(|_| ClusterError::LockPoisoned)?;
            *segments = snapshot.segments;
        }
        {
            let mut locks = self
                .task_locks
                .write()
                .map_err(|_| ClusterError::LockPoisoned)?;
            *locks = snapshot.task_locks;
        }
        {
            let mut queue = self
                .segment_queue
                .write()
                .map_err(|_| ClusterError::LockPoisoned)?;
            *queue = snapshot.segment_queue;
        }
        Ok(())
    }

    /// Get information about this node.
    pub fn node_info(&self) -> &NodeInfo {
        &self.node_info
    }

    /// Get the cluster operating mode.
    pub fn mode(&self) -> &ClusterMode {
        &self.mode
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Wave 36-G4 (Wave 37B High): parse a single interval bound into a UTC instant.
///
/// Accepts:
/// - RFC3339 / ISO 8601 with timezone (e.g. `2026-04-28T00:00:00+09:00`,
///   `2026-04-27T15:00:00Z`).
/// - Date-only `YYYY-MM-DD` (interpreted as midnight UTC, preserves the
///   pre-Wave-36-G4 behaviour for callers that pass plain dates).
///
/// Returns `None` if neither form parses; callers MUST treat that as a
/// malformed lock request and refuse to grant the lock.
fn parse_interval_bound(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // Date-only fallback: `YYYY-MM-DD` -> midnight UTC.
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        && let Some(dt) = d.and_hms_opt(0, 0, 0)
    {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }
    None
}

/// Check whether two intervals overlap, comparing chronologically.
///
/// Wave 36-G4 (W37B High `cluster_lib:451-495,651-655`): the previous
/// implementation lex-compared raw `&str`, so equivalent instants like
/// `2026-04-28T00:00:00+09:00` and `2026-04-27T15:00:00Z` could be ordered
/// incorrectly and conflicting locks could both be granted.  We now parse
/// each bound to a UTC `DateTime` and compare chronologically.  Malformed
/// or zero-/negative-length intervals (start >= end) are reported as
/// "overlapping" so the caller refuses the lock — that is strictly safer
/// than silently granting on un-checkable input.
fn intervals_overlap(start_a: &str, end_a: &str, start_b: &str, end_b: &str) -> bool {
    let (a_start, a_end, b_start, b_end) = match (
        parse_interval_bound(start_a),
        parse_interval_bound(end_a),
        parse_interval_bound(start_b),
        parse_interval_bound(end_b),
    ) {
        (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
        // Any unparseable bound -> conservatively report as overlapping
        // so the caller denies the lock instead of granting it on
        // malformed input.
        _ => return true,
    };

    // Reject zero- or negative-length intervals: they cannot legitimately
    // hold a lock.  Treating them as "overlapping" makes the caller emit
    // `LockDenied` rather than silently inserting a degenerate lock.
    if a_start >= a_end || b_start >= b_end {
        return true;
    }

    // Two intervals [a_start, a_end) and [b_start, b_end) overlap when
    // a_start < b_end AND b_start < a_end.
    a_start < b_end && b_start < a_end
}

/// Parse a `host:port` string, defaulting to port 8888 if not specified.
///
/// Supports three forms (W37B cluster_lib Medium #4 closure):
///   * IPv4 / hostname: `10.0.0.1:9999` / `myhost:1234` / `myhost`
///   * Bracketed IPv6:  `[2001:db8::1]:9999` / `[2001:db8::1]`
///   * Bare IPv6:       `2001:db8::1` (port defaults to 8888; the
///     trailing `:1` hextet is **not** misparsed as a port number)
///
/// Bare IPv6 forms with an embedded port (`2001:db8::1:9999`) are
/// ambiguous and rejected — operators must use the bracketed form.
fn parse_host_port(addr: &str) -> (String, u16) {
    let trimmed = addr.trim();
    // Bracketed IPv6: `[addr]` or `[addr]:port`.
    if let Some(rest) = trimmed.strip_prefix('[')
        && let Some(end) = rest.find(']')
    {
        let host = rest[..end].to_string();
        let after = &rest[end + 1..];
        let port = if let Some(port_str) = after.strip_prefix(':') {
            port_str.parse::<u16>().unwrap_or(8888)
        } else {
            8888
        };
        return (host, port);
    }
    // Bare IPv6 literal — any address containing two or more `:` is
    // assumed to be IPv6 with no embedded port; accept verbatim and
    // default the port. This avoids the historical `rfind(':')` bug
    // that turned `2001:db8::1` into host `2001:db8:` / port `1`.
    if trimmed.matches(':').count() >= 2 {
        return (trimmed.to_string(), 8888);
    }
    if let Some(colon_idx) = trimmed.rfind(':') {
        let host = trimmed[..colon_idx].to_string();
        let port = trimmed[colon_idx + 1..].parse::<u16>().unwrap_or(8888);
        (host, port)
    } else {
        (trimmed.to_string(), 8888)
    }
}

// ---------------------------------------------------------------------------
// Tests — legacy ClusterState
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str, role: NodeRole) -> NodeInfo {
        NodeInfo {
            id: id.to_string(),
            host: "127.0.0.1".to_string(),
            port: 8888,
            role,
        }
    }

    // ---- Legacy ClusterState tests (preserved) ----

    #[test]
    fn single_node_leader() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let state = ClusterState::new_single_node(node.clone());

        let leader = state.get_leader();
        assert!(leader.is_some());
        let leader = leader.expect("leader");
        assert_eq!(leader.id, "node-1");
        assert_eq!(leader.role, NodeRole::AllInOne);
    }

    #[test]
    fn register_and_get_services() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let state = ClusterState::new_single_node(node);

        state.register_service(ServiceEntry {
            service_type: "broker".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8082,
            node_id: "node-1".to_string(),
        });
        state.register_service(ServiceEntry {
            service_type: "historical".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8083,
            node_id: "node-1".to_string(),
        });

        let brokers = state.get_services("broker");
        assert_eq!(brokers.len(), 1);
        assert_eq!(brokers[0].port, 8082);

        let historicals = state.get_services("historical");
        assert_eq!(historicals.len(), 1);
        assert_eq!(historicals[0].port, 8083);

        let coordinators = state.get_services("coordinator");
        assert!(coordinators.is_empty());
    }

    #[test]
    fn deregister_service() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let state = ClusterState::new_single_node(node);

        state.register_service(ServiceEntry {
            service_type: "broker".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8082,
            node_id: "node-1".to_string(),
        });

        assert_eq!(state.get_services("broker").len(), 1);

        state.deregister_service("node-1", "broker");
        assert!(state.get_services("broker").is_empty());
    }

    #[test]
    fn register_service_replaces_same_node() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let state = ClusterState::new_single_node(node);

        state.register_service(ServiceEntry {
            service_type: "broker".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8082,
            node_id: "node-1".to_string(),
        });
        // Re-register with different port.
        state.register_service(ServiceEntry {
            service_type: "broker".to_string(),
            host: "127.0.0.1".to_string(),
            port: 9999,
            node_id: "node-1".to_string(),
        });

        let brokers = state.get_services("broker");
        assert_eq!(brokers.len(), 1);
        assert_eq!(brokers[0].port, 9999);
    }

    #[test]
    fn multiple_nodes_same_service() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let state = ClusterState::new_single_node(node);

        state.register_service(ServiceEntry {
            service_type: "historical".to_string(),
            host: "10.0.0.1".to_string(),
            port: 8083,
            node_id: "hist-1".to_string(),
        });
        state.register_service(ServiceEntry {
            service_type: "historical".to_string(),
            host: "10.0.0.2".to_string(),
            port: 8083,
            node_id: "hist-2".to_string(),
        });

        let historicals = state.get_services("historical");
        assert_eq!(historicals.len(), 2);
    }

    #[test]
    fn announce_and_get_segments() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let state = ClusterState::new_single_node(node);

        state.announce_segment(SegmentAnnouncement {
            segment_id: "seg_001".to_string(),
            server_name: "hist-1".to_string(),
            data_source: "wiki".to_string(),
            tier: "_default_tier".to_string(),
        });
        state.announce_segment(SegmentAnnouncement {
            segment_id: "seg_002".to_string(),
            server_name: "hist-1".to_string(),
            data_source: "wiki".to_string(),
            tier: "_default_tier".to_string(),
        });

        let announcements = state.get_segment_announcements("wiki");
        assert_eq!(announcements.len(), 2);

        let clicks = state.get_segment_announcements("clicks");
        assert!(clicks.is_empty());
    }

    #[test]
    fn remove_segment_announcement() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let state = ClusterState::new_single_node(node);

        state.announce_segment(SegmentAnnouncement {
            segment_id: "seg_001".to_string(),
            server_name: "hist-1".to_string(),
            data_source: "wiki".to_string(),
            tier: "_default_tier".to_string(),
        });
        state.announce_segment(SegmentAnnouncement {
            segment_id: "seg_001".to_string(),
            server_name: "hist-2".to_string(),
            data_source: "wiki".to_string(),
            tier: "_default_tier".to_string(),
        });

        assert_eq!(state.get_segment_announcements("wiki").len(), 2);

        state.remove_segment_announcement("seg_001", "hist-1");

        let remaining = state.get_segment_announcements("wiki");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].server_name, "hist-2");
    }

    #[test]
    fn duplicate_announcement_is_deduplicated() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let state = ClusterState::new_single_node(node);

        let ann = SegmentAnnouncement {
            segment_id: "seg_001".to_string(),
            server_name: "hist-1".to_string(),
            data_source: "wiki".to_string(),
            tier: "_default_tier".to_string(),
        };

        state.announce_segment(ann.clone());
        state.announce_segment(ann);

        assert_eq!(state.get_segment_announcements("wiki").len(), 1);
    }

    // ---- ClusterManager tests ----

    #[test]
    fn manager_single_node_leader() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        assert!(mgr.is_leader());
        let leader = mgr.leader();
        assert!(leader.is_some());
        assert_eq!(leader.expect("leader").id, "node-1");
    }

    #[test]
    fn manager_apply_elect_leader() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        let result = mgr
            .apply(ClusterCommand::ElectLeader {
                role: "coordinator".to_string(),
                node_id: "node-2".to_string(),
            })
            .expect("apply");
        assert_eq!(
            result,
            CommandResult::LeaderElected {
                node_id: "node-2".to_string()
            }
        );

        // Leader changed, so this node is no longer leader.
        assert!(!mgr.is_leader());
        assert_eq!(mgr.leader().expect("leader").id, "node-2");
    }

    #[test]
    fn manager_apply_register_service() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        let result = mgr
            .apply(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "127.0.0.1".to_string(),
                port: 8082,
                node_id: "node-1".to_string(),
            }))
            .expect("apply");
        assert_eq!(result, CommandResult::Ok);

        let brokers = mgr.services("broker");
        assert_eq!(brokers.len(), 1);
        assert_eq!(brokers[0].port, 8082);
    }

    #[test]
    fn manager_apply_deregister_service() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        mgr.register_service(ServiceEntry {
            service_type: "broker".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8082,
            node_id: "node-1".to_string(),
        });
        assert_eq!(mgr.services("broker").len(), 1);

        let result = mgr
            .apply(ClusterCommand::DeregisterService {
                node_id: "node-1".to_string(),
                service_type: "broker".to_string(),
            })
            .expect("apply");
        assert_eq!(result, CommandResult::Ok);
        assert!(mgr.services("broker").is_empty());
    }

    #[test]
    fn manager_apply_announce_segment() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        let result = mgr
            .apply(ClusterCommand::AnnounceSegment(SegmentAnnouncement {
                segment_id: "seg_001".to_string(),
                server_name: "hist-1".to_string(),
                data_source: "wiki".to_string(),
                tier: "_default_tier".to_string(),
            }))
            .expect("apply");
        assert_eq!(result, CommandResult::Ok);

        let segs = mgr.segments("wiki");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].segment_id, "seg_001");
    }

    #[test]
    fn manager_apply_remove_segment() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        mgr.apply(ClusterCommand::AnnounceSegment(SegmentAnnouncement {
            segment_id: "seg_001".to_string(),
            server_name: "hist-1".to_string(),
            data_source: "wiki".to_string(),
            tier: "_default_tier".to_string(),
        }))
        .expect("apply");

        let result = mgr
            .apply(ClusterCommand::RemoveSegment {
                segment_id: "seg_001".to_string(),
                server_name: "hist-1".to_string(),
            })
            .expect("apply");
        assert_eq!(result, CommandResult::Ok);
        assert!(mgr.segments("wiki").is_empty());
    }

    #[test]
    fn manager_task_lock_acquire_success() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        let result = mgr
            .try_lock("task-1", "wiki", "2026-01-01", "2026-01-02")
            .expect("try_lock");
        match result {
            CommandResult::LockAcquired { lock_id } => {
                assert!(!lock_id.is_empty());
            }
            other => panic!("expected LockAcquired, got {other:?}"),
        }

        assert_eq!(mgr.active_locks().len(), 1);
    }

    #[test]
    fn manager_task_lock_conflict() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        mgr.try_lock("task-1", "wiki", "2026-01-01", "2026-01-10")
            .expect("try_lock");

        // Overlapping interval on same data source.
        let result = mgr
            .try_lock("task-2", "wiki", "2026-01-05", "2026-01-15")
            .expect("try_lock");
        match result {
            CommandResult::LockDenied { reason } => {
                assert!(reason.contains("task-1"), "reason should mention holder");
                assert!(reason.contains("wiki"), "reason should mention data source");
            }
            other => panic!("expected LockDenied, got {other:?}"),
        }
    }

    #[test]
    fn manager_task_lock_non_overlapping_allowed() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        mgr.try_lock("task-1", "wiki", "2026-01-01", "2026-01-10")
            .expect("try_lock");

        // Non-overlapping: starts after first ends.
        let result = mgr
            .try_lock("task-2", "wiki", "2026-01-10", "2026-01-20")
            .expect("try_lock");
        assert!(matches!(result, CommandResult::LockAcquired { .. }));
        assert_eq!(mgr.active_locks().len(), 2);
    }

    #[test]
    fn manager_task_lock_different_datasource_allowed() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        mgr.try_lock("task-1", "wiki", "2026-01-01", "2026-01-10")
            .expect("try_lock");

        // Same interval but different data source: allowed.
        let result = mgr
            .try_lock("task-2", "clicks", "2026-01-01", "2026-01-10")
            .expect("try_lock");
        assert!(matches!(result, CommandResult::LockAcquired { .. }));
    }

    #[test]
    fn manager_task_lock_release() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        mgr.try_lock("task-1", "wiki", "2026-01-01", "2026-01-10")
            .expect("try_lock");
        assert_eq!(mgr.active_locks().len(), 1);

        mgr.unlock("task-1").expect("unlock");
        assert!(mgr.active_locks().is_empty());
    }

    #[test]
    fn manager_task_lock_release_not_found() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        // Releasing a non-existent lock should not error.
        mgr.unlock("nonexistent").expect("unlock");
    }

    #[test]
    fn manager_segment_queue() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        mgr.apply(ClusterCommand::EnqueueSegmentAction {
            server_name: "hist-1".to_string(),
            segment_id: "seg_001".to_string(),
            action: SegmentAction::Load,
        })
        .expect("apply");

        mgr.apply(ClusterCommand::EnqueueSegmentAction {
            server_name: "hist-1".to_string(),
            segment_id: "seg_002".to_string(),
            action: SegmentAction::Drop,
        })
        .expect("apply");

        let actions = mgr.pending_actions("hist-1");
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].segment_id, "seg_001");
        assert_eq!(actions[0].action, SegmentAction::Load);
        assert_eq!(actions[1].segment_id, "seg_002");
        assert_eq!(actions[1].action, SegmentAction::Drop);

        // Different server has no pending actions.
        assert!(mgr.pending_actions("hist-2").is_empty());
    }

    #[test]
    fn manager_snapshot_restore() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node.clone());

        // Populate state.
        mgr.register_service(ServiceEntry {
            service_type: "broker".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8082,
            node_id: "node-1".to_string(),
        });
        mgr.apply(ClusterCommand::AnnounceSegment(SegmentAnnouncement {
            segment_id: "seg_001".to_string(),
            server_name: "hist-1".to_string(),
            data_source: "wiki".to_string(),
            tier: "_default_tier".to_string(),
        }))
        .expect("apply");
        mgr.try_lock("task-1", "wiki", "2026-01-01", "2026-01-10")
            .expect("try_lock");
        mgr.apply(ClusterCommand::EnqueueSegmentAction {
            server_name: "hist-1".to_string(),
            segment_id: "seg_002".to_string(),
            action: SegmentAction::Load,
        })
        .expect("apply");

        let snapshot = mgr.snapshot();

        // Create a fresh manager and restore.
        let mgr2 = ClusterManager::new_single_node(make_node("node-2", NodeRole::AllInOne));
        mgr2.restore(snapshot).expect("restore");

        assert_eq!(mgr2.services("broker").len(), 1);
        assert_eq!(mgr2.segments("wiki").len(), 1);
        assert_eq!(mgr2.active_locks().len(), 1);
        assert_eq!(mgr2.pending_actions("hist-1").len(), 1);

        // Leader was restored from snapshot (node-1), not the new node.
        assert_eq!(mgr2.leader().expect("leader").id, "node-1");
    }

    #[test]
    fn manager_multi_node_creation() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_multi_node(
            node,
            vec!["10.0.0.2:8888".to_string(), "10.0.0.3:8888".to_string()],
        );

        // Node count includes self + peers.
        assert_eq!(mgr.node_count(), 3);

        // Still functions as leader in this phase (single-node fallback).
        assert!(mgr.is_leader());

        // Mode is multi-node.
        match mgr.mode() {
            ClusterMode::MultiNode { peers } => assert_eq!(peers.len(), 2),
            ClusterMode::SingleNode => panic!("expected multi-node"),
        }
    }

    #[test]
    fn manager_health_single_node() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        let health = mgr.health();
        assert!(health.is_healthy);
        assert_eq!(health.leader.as_deref(), Some("node-1"));
        assert_eq!(health.node_count, 1);
        assert_eq!(health.mode, "single-node");
    }

    #[test]
    fn manager_health_multi_node() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_multi_node(
            node,
            vec!["10.0.0.2:8888".to_string(), "10.0.0.3:9999".to_string()],
        );

        let health = mgr.health();
        assert!(health.is_healthy);
        assert_eq!(health.node_count, 3);
        assert!(health.mode.starts_with("multi-node"));
    }

    #[test]
    fn manager_apply_all_command_types() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        // ElectLeader
        assert!(matches!(
            mgr.apply(ClusterCommand::ElectLeader {
                role: "coordinator".to_string(),
                node_id: "node-1".to_string(),
            })
            .expect("apply"),
            CommandResult::LeaderElected { .. }
        ));

        // RegisterService
        assert_eq!(
            mgr.apply(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "127.0.0.1".to_string(),
                port: 8082,
                node_id: "node-1".to_string(),
            }))
            .expect("apply"),
            CommandResult::Ok
        );

        // DeregisterService
        assert_eq!(
            mgr.apply(ClusterCommand::DeregisterService {
                node_id: "node-1".to_string(),
                service_type: "broker".to_string(),
            })
            .expect("apply"),
            CommandResult::Ok
        );

        // AnnounceSegment
        assert_eq!(
            mgr.apply(ClusterCommand::AnnounceSegment(SegmentAnnouncement {
                segment_id: "seg_001".to_string(),
                server_name: "hist-1".to_string(),
                data_source: "wiki".to_string(),
                tier: "_default_tier".to_string(),
            }))
            .expect("apply"),
            CommandResult::Ok
        );

        // RemoveSegment
        assert_eq!(
            mgr.apply(ClusterCommand::RemoveSegment {
                segment_id: "seg_001".to_string(),
                server_name: "hist-1".to_string(),
            })
            .expect("apply"),
            CommandResult::Ok
        );

        // EnqueueSegmentAction
        assert_eq!(
            mgr.apply(ClusterCommand::EnqueueSegmentAction {
                server_name: "hist-1".to_string(),
                segment_id: "seg_002".to_string(),
                action: SegmentAction::Load,
            })
            .expect("apply"),
            CommandResult::Ok
        );

        // AcquireTaskLock
        assert!(matches!(
            mgr.apply(ClusterCommand::AcquireTaskLock {
                task_id: "task-1".to_string(),
                data_source: "wiki".to_string(),
                interval_start: "2026-01-01".to_string(),
                interval_end: "2026-01-10".to_string(),
            })
            .expect("apply"),
            CommandResult::LockAcquired { .. }
        ));

        // ReleaseTaskLock
        assert_eq!(
            mgr.apply(ClusterCommand::ReleaseTaskLock {
                task_id: "task-1".to_string(),
            })
            .expect("apply"),
            CommandResult::Ok
        );
    }

    #[test]
    fn manager_release_nonexistent_lock_returns_not_found() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        let result = mgr
            .apply(ClusterCommand::ReleaseTaskLock {
                task_id: "nonexistent".to_string(),
            })
            .expect("apply");
        assert_eq!(result, CommandResult::NotFound);
    }

    #[test]
    fn manager_snapshot_empty_state() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        let snapshot = mgr.snapshot();
        assert!(snapshot.services.is_empty());
        assert!(snapshot.segments.is_empty());
        assert!(snapshot.task_locks.is_empty());
        assert!(snapshot.leader.is_some()); // leader is set even in empty state
        assert!(snapshot.segment_queue.is_empty());
    }

    #[test]
    fn manager_snapshot_serialization_roundtrip() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node.clone());

        mgr.register_service(ServiceEntry {
            service_type: "broker".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8082,
            node_id: "node-1".to_string(),
        });
        mgr.try_lock("task-1", "wiki", "2026-01-01", "2026-01-10")
            .expect("try_lock");

        let snapshot = mgr.snapshot();
        let json = serde_json::to_string(&snapshot).expect("serialize");
        let restored: ClusterSnapshot = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(restored.services.len(), snapshot.services.len());
        assert_eq!(restored.task_locks.len(), snapshot.task_locks.len());
        assert_eq!(
            restored.leader.as_ref().map(|l| &l.id),
            snapshot.leader.as_ref().map(|l| &l.id)
        );
    }

    #[test]
    fn intervals_overlap_cases() {
        // Overlapping.
        assert!(intervals_overlap(
            "2026-01-01",
            "2026-01-10",
            "2026-01-05",
            "2026-01-15"
        ));
        // Contained.
        assert!(intervals_overlap(
            "2026-01-01",
            "2026-01-20",
            "2026-01-05",
            "2026-01-10"
        ));
        // Exact same.
        assert!(intervals_overlap(
            "2026-01-01",
            "2026-01-10",
            "2026-01-01",
            "2026-01-10"
        ));
        // Adjacent (not overlapping: [01, 10) and [10, 20)).
        assert!(!intervals_overlap(
            "2026-01-01",
            "2026-01-10",
            "2026-01-10",
            "2026-01-20"
        ));
        // Disjoint.
        assert!(!intervals_overlap(
            "2026-01-01",
            "2026-01-05",
            "2026-01-10",
            "2026-01-20"
        ));
    }

    /// Wave 36-G4 regression for W37B High `cluster_lib:451-495`:
    /// equivalent instants in different timezones MUST be detected as
    /// overlapping.  Pre-fix, lex-compare reported `2026-04-28T00:00:00+09:00`
    /// (instant = 2026-04-27T15:00:00Z) > any earlier UTC string and granted
    /// conflicting locks.
    #[test]
    fn intervals_overlap_timezone_normalised() {
        // Instant equality across timezones: [2026-04-27T15Z .. 2026-04-27T16Z)
        // overlaps [2026-04-28T00:00+09:00 .. 2026-04-28T01:00+09:00).
        assert!(intervals_overlap(
            "2026-04-27T15:00:00Z",
            "2026-04-27T16:00:00Z",
            "2026-04-28T00:00:00+09:00",
            "2026-04-28T01:00:00+09:00",
        ));
        // Same intervals expressed entirely in +09:00 -> still overlap.
        assert!(intervals_overlap(
            "2026-04-28T00:00:00+09:00",
            "2026-04-28T01:00:00+09:00",
            "2026-04-28T00:30:00+09:00",
            "2026-04-28T02:00:00+09:00",
        ));
        // Disjoint when chronologically ordered (UTC vs JST that lands later).
        assert!(!intervals_overlap(
            "2026-04-27T10:00:00Z",
            "2026-04-27T11:00:00Z",
            "2026-04-28T00:00:00+09:00",
            "2026-04-28T01:00:00+09:00",
        ));
    }

    /// Wave 36-G4: malformed bounds are rejected as "overlapping" so the
    /// caller denies the lock instead of granting on un-checkable input.
    #[test]
    fn intervals_overlap_malformed_is_denied() {
        assert!(intervals_overlap(
            "not-a-timestamp",
            "2026-01-10",
            "2026-01-05",
            "2026-01-15"
        ));
        // Zero-length: start == end -> deny.
        assert!(intervals_overlap(
            "2026-01-01",
            "2026-01-01",
            "2026-01-05",
            "2026-01-15"
        ));
        // Negative-length: start > end -> deny.
        assert!(intervals_overlap(
            "2026-01-10",
            "2026-01-01",
            "2026-01-05",
            "2026-01-15"
        ));
    }

    #[test]
    fn parse_host_port_cases() {
        assert_eq!(
            parse_host_port("10.0.0.1:9999"),
            ("10.0.0.1".to_string(), 9999)
        );
        assert_eq!(parse_host_port("10.0.0.1"), ("10.0.0.1".to_string(), 8888));
        assert_eq!(parse_host_port("myhost:1234"), ("myhost".to_string(), 1234));
    }

    /// W37B cluster_lib Medium #4: bare IPv6 literal must not be
    /// misparsed by `rfind(':')`. Bracketed form with explicit port
    /// must still be honored.
    #[test]
    fn parse_host_port_ipv6_cases() {
        // Bare IPv6 — port defaults to 8888, full address preserved.
        assert_eq!(
            parse_host_port("2001:db8::1"),
            ("2001:db8::1".to_string(), 8888)
        );
        // Bracketed IPv6 + port.
        assert_eq!(
            parse_host_port("[2001:db8::1]:9999"),
            ("2001:db8::1".to_string(), 9999)
        );
        // Bracketed IPv6 without explicit port.
        assert_eq!(parse_host_port("[::1]"), ("::1".to_string(), 8888));
        // Pre-fix bug: `2001:db8::1` would have produced
        // `("2001:db8:", 1)`. Assert the post-fix host is intact.
        let (host, port) = parse_host_port("2001:db8::1");
        assert!(host.contains("::1"));
        assert_eq!(port, 8888);
    }

    #[test]
    fn manager_node_info() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);
        assert_eq!(mgr.node_info().id, "node-1");
    }

    /// W37B cluster_lib Medium #2 regression: electing a remote peer
    /// must preserve that peer's host/port (not silently rewrite to
    /// the local node's host/port). Pre-fix the code returned a
    /// leader pointing at the local node's address.
    #[test]
    fn elect_leader_for_remote_peer_preserves_peer_address() {
        let local = NodeInfo {
            id: "node-1".to_string(),
            host: "10.0.0.1".to_string(),
            port: 8081,
            role: NodeRole::AllInOne,
        };
        let mgr = ClusterManager::new_multi_node(local, vec!["10.0.0.2:8082".to_string()]);

        // Elect peer-0 (the only configured peer).
        mgr.apply(ClusterCommand::ElectLeader {
            role: "coordinator".to_string(),
            node_id: "peer-0".to_string(),
        })
        .expect("apply ElectLeader");

        let leader = mgr.leader().expect("leader present after election");
        assert_eq!(leader.id, "peer-0");
        // Pre-fix this would have been "10.0.0.1" / 8081 (local).
        assert_eq!(leader.host, "10.0.0.2");
        assert_eq!(leader.port, 8082);
    }

    // -----------------------------------------------------------------
    // Wave 45-C — Wave 37B `cluster_lib` Medium #1 closure
    // (apply() must propagate RwLock poison instead of silently
    // returning Ok when no state change occurred).
    // -----------------------------------------------------------------

    /// Helper: poison the given `RwLock` by panicking inside a write
    /// guard on a scoped thread, *catching* the panic via `join()` so
    /// the test harness sees only the post-poison state and not the
    /// intentional thread panic.
    ///
    /// `std::thread::scope` re-panics the parent if any spawned thread
    /// panics — exactly the wrong behaviour for an intentional-panic
    /// poison helper.  We use the `Builder::spawn_scoped` +
    /// explicit `join()` form so the panic is contained.
    ///
    /// Used by the Wave 45-C `apply_*` regression tests below.
    fn poison_inplace<T: Send + Sync>(lock: &std::sync::RwLock<T>) {
        std::thread::scope(|s| {
            let handle = std::thread::Builder::new()
                .name("poison-helper".into())
                .spawn_scoped(s, || {
                    let _g = lock.write().expect("acquire write to poison");
                    panic!("intentional poison for Wave 45-C cluster_lib test");
                })
                .expect("spawn poison-helper");
            // Joining a panicked thread returns Err; we drop it so the
            // panic is contained inside this helper.  scope() then
            // exits cleanly because the panic was already consumed.
            let _ = handle.join();
        });
        assert!(lock.is_poisoned(), "test setup: lock must be poisoned");
    }

    /// W37B cluster_lib Medium #1 regression: when the services lock
    /// is poisoned, `apply(RegisterService)` must return
    /// `Err(ClusterError::LockPoisoned)` rather than silently
    /// returning `Ok(CommandResult::Ok)` while leaving the state
    /// unchanged.
    #[test]
    fn apply_register_service_propagates_lock_poison() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        // The services lock is a private field on `ClusterState`; we
        // can reach it from inside this module's tests submodule and
        // poison it via a scoped thread.
        poison_inplace(&mgr.state.services);

        let entry = ServiceEntry {
            service_type: "broker".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8082,
            node_id: "node-1".to_string(),
        };
        let result = mgr.apply(ClusterCommand::RegisterService(entry));
        // Pre-fix: this would have been `Ok(CommandResult::Ok)` with
        // the actual state-change silently swallowed.
        assert!(
            matches!(result, Err(ClusterError::LockPoisoned)),
            "apply(RegisterService) on poisoned lock must surface \
             ClusterError::LockPoisoned; got {result:?}",
        );
    }

    /// Companion regression: the `_checked` variants on
    /// [`ClusterState`] must themselves return `Err(LockPoisoned)`,
    /// while the legacy `pub fn` no-Result variants stay silently
    /// no-op for backward compatibility with `ferrodruid-discovery`
    /// and the existing legacy callers documented in the doc-comment.
    /// This pins both halves of the Wave 45-C contract simultaneously.
    #[test]
    fn cluster_state_checked_variants_distinguish_lock_poison() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let state = ClusterState::new_single_node(node);

        // Healthy: both variants succeed.
        let entry = ServiceEntry {
            service_type: "broker".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8082,
            node_id: "node-1".to_string(),
        };
        state
            .register_service_checked(entry.clone())
            .expect("checked register on healthy state");
        assert_eq!(state.get_services("broker").len(), 1);

        // Poison the services lock.
        poison_inplace(&state.services);

        // The legacy variant silently no-ops (back-compat contract).
        // We can't observe success/failure directly, but it must not
        // panic.
        state.register_service(ServiceEntry {
            service_type: "historical".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8083,
            node_id: "node-1".to_string(),
        });

        // The `_checked` variants must surface `LockPoisoned`.
        let r1 = state.register_service_checked(entry.clone());
        assert!(matches!(r1, Err(ClusterError::LockPoisoned)));

        let r2 = state.deregister_service_checked("node-1", "broker");
        assert!(matches!(r2, Err(ClusterError::LockPoisoned)));

        // The segment-announcements lock is independent; poison it
        // separately so its own checked variants are exercised under
        // the same contract.
        poison_inplace(&state.segment_announcements);

        let ann = SegmentAnnouncement {
            segment_id: "seg-1".to_string(),
            server_name: "hist-1".to_string(),
            data_source: "ds".to_string(),
            tier: "_default_tier".to_string(),
        };
        let r3 = state.announce_segment_checked(ann);
        assert!(matches!(r3, Err(ClusterError::LockPoisoned)));

        let r4 = state.remove_segment_announcement_checked("seg-1", "hist-1");
        assert!(matches!(r4, Err(ClusterError::LockPoisoned)));
    }

    // ------------------------------------------------------------------
    // Wave 45-D regression — W37B `cluster_lib` Medium #3
    //
    // Pre-fix: `EnqueueSegmentAction` appended unconditionally with no
    // per-server cap, no global cap, and no coalescing, so a malicious
    // or misconfigured producer could grow `segment_queue` without
    // bound.  Post-fix: `apply()` (a) coalesces by `segment_id` with
    // last-write-wins on the action, and (b) refuses with
    // `ClusterError::QueueFull` once either the per-server or global
    // cap is reached.  These tests pin both halves of the contract.
    // ------------------------------------------------------------------

    #[test]
    fn enqueue_segment_action_coalesces_by_segment_id_last_write_wins() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        // Three enqueues to the same (server, segment_id) — should
        // coalesce to a single entry whose action reflects the last
        // enqueue.  Before Wave 45-D this would have left 3 entries.
        for action in [
            SegmentAction::Load,
            SegmentAction::Drop,
            SegmentAction::Load,
        ] {
            mgr.apply(ClusterCommand::EnqueueSegmentAction {
                server_name: "hist-1".to_string(),
                segment_id: "seg_001".to_string(),
                action,
            })
            .expect("apply");
        }

        let actions = mgr.pending_actions("hist-1");
        assert_eq!(
            actions.len(),
            1,
            "duplicate-segment_id enqueues must coalesce to 1 entry, got {actions:?}"
        );
        assert_eq!(
            actions[0].action,
            SegmentAction::Load,
            "last-write-wins: final action should be Load"
        );
        assert_eq!(actions[0].segment_id, "seg_001");

        // Distinct segment_ids must NOT coalesce — FIFO preserved.
        mgr.apply(ClusterCommand::EnqueueSegmentAction {
            server_name: "hist-1".to_string(),
            segment_id: "seg_002".to_string(),
            action: SegmentAction::Drop,
        })
        .expect("apply");
        let actions = mgr.pending_actions("hist-1");
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].segment_id, "seg_001");
        assert_eq!(actions[1].segment_id, "seg_002");
    }

    #[test]
    fn enqueue_segment_action_refuses_when_per_server_cap_exceeded() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        // Push the per-server cap to its limit with distinct segment
        // ids so coalescing does not absorb them.  We use a small
        // *test-local* helper cap by submitting exactly
        // `MAX_SEGMENT_QUEUE_PER_SERVER` distinct ids; the
        // `+ 1`-th push must be rejected.
        //
        // To keep the test fast we use a small slice of the cap: we
        // build a fresh state and a *manually capped* assertion by
        // reaching into the queue directly.  We populate
        // `MAX_SEGMENT_QUEUE_PER_SERVER` entries via the RwLock to
        // avoid the O(N) cost of N real `apply()` calls, then assert
        // that the next `apply()` is refused with `QueueFull`.
        {
            let mut q = mgr.segment_queue.write().expect("queue lock");
            let entries = q.entry("hist-1".to_string()).or_default();
            for i in 0..MAX_SEGMENT_QUEUE_PER_SERVER {
                entries.push(QueuedSegmentAction {
                    segment_id: format!("preload-{i}"),
                    action: SegmentAction::Load,
                    queued_at: Utc::now(),
                });
            }
        }
        assert_eq!(
            mgr.pending_actions("hist-1").len(),
            MAX_SEGMENT_QUEUE_PER_SERVER
        );

        let res = mgr.apply(ClusterCommand::EnqueueSegmentAction {
            server_name: "hist-1".to_string(),
            segment_id: "would-overflow".to_string(),
            action: SegmentAction::Load,
        });
        match res {
            Err(ClusterError::QueueFull {
                scope,
                cap,
                current,
            }) => {
                assert!(
                    scope.starts_with("per-server"),
                    "expected per-server scope, got: {scope}"
                );
                assert_eq!(cap, MAX_SEGMENT_QUEUE_PER_SERVER);
                assert!(current > cap);
            }
            other => panic!("expected QueueFull(per-server), got: {other:?}"),
        }

        // Coalescing path must still succeed — re-enqueueing a
        // segment_id that is already pending does not grow the queue.
        mgr.apply(ClusterCommand::EnqueueSegmentAction {
            server_name: "hist-1".to_string(),
            segment_id: "preload-0".to_string(),
            action: SegmentAction::Drop,
        })
        .expect("re-enqueue of pending segment_id must succeed (coalesced)");
        // Total still capped — coalescing absorbed the new entry.
        assert_eq!(
            mgr.pending_actions("hist-1").len(),
            MAX_SEGMENT_QUEUE_PER_SERVER
        );
    }

    #[test]
    fn dequeue_segment_actions_drains_and_relieves_cap_pressure() {
        let node = make_node("node-1", NodeRole::AllInOne);
        let mgr = ClusterManager::new_single_node(node);

        for i in 0..5 {
            mgr.apply(ClusterCommand::EnqueueSegmentAction {
                server_name: "hist-1".to_string(),
                segment_id: format!("seg_{i:03}"),
                action: SegmentAction::Load,
            })
            .expect("apply");
        }
        assert_eq!(mgr.segment_queue_total(), 5);
        assert_eq!(mgr.pending_actions("hist-1").len(), 5);

        // Drain hist-1; total drops to 0 and pending_actions echoes
        // an empty list afterward.  This is the "ack/dequeue" half
        // referenced in the W37B finding.
        let drained = mgr.dequeue_actions("hist-1");
        assert_eq!(drained.len(), 5);
        assert_eq!(drained[0].segment_id, "seg_000");
        assert_eq!(drained[4].segment_id, "seg_004");
        assert_eq!(mgr.segment_queue_total(), 0);
        assert!(mgr.pending_actions("hist-1").is_empty());

        // Idempotent: draining an absent server returns empty.
        assert!(mgr.dequeue_actions("never-existed").is_empty());

        // After drain, new enqueues are accepted as if fresh.
        mgr.apply(ClusterCommand::EnqueueSegmentAction {
            server_name: "hist-1".to_string(),
            segment_id: "seg_post_drain".to_string(),
            action: SegmentAction::Drop,
        })
        .expect("apply");
        assert_eq!(mgr.pending_actions("hist-1").len(), 1);
    }
}
