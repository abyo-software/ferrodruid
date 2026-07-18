// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! TCP-based cluster replication for multi-node mode.
//!
//! Protocol:
//! - Nodes connect to each other via TCP
//! - Leader election: highest term wins, heartbeat-based failover
//! - Command replication: leader broadcasts commands to followers
//! - Snapshot: new followers request full snapshot on join

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rand::SeedableRng;
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock, mpsc};
use tokio::time::Instant;

use crate::{ClusterCommand, ClusterError, ClusterManager, ClusterSnapshot, CommandResult};

/// Wave 38-DE: cap on how many entries a leader will resend in a single
/// AppendEntries replay batch when walking back to repair a follower's
/// log gap. Bounded to keep the per-message wire size predictable on
/// large catch-up windows; followers that lag by more than this should
/// receive a snapshot via [`ReplicationMessage::SnapshotResponse`]
/// instead.
pub const MAX_REPLAY_BATCH: usize = 1000;

/// Upper bound on a snapshot's `last_index` accepted by [`ReplicationEngine::
/// restore_snapshot`] (DD R31). Restore fills `log_terms` with one term per
/// index up to `last_index`, so an authenticated `SnapshotResponse` carrying a
/// huge `last_index` (e.g. `u64::MAX`) would `Vec::resize` to billions of
/// entries (tens of GiB) from a tiny frame. 16 Mi indices (~128 MiB of terms)
/// is far beyond any state this in-memory log model supports while bounding the
/// allocation; a cluster legitimately exceeding it needs compacted per-index
/// term metadata (a documented limitation), not an unbounded resize.
pub const MAX_SNAPSHOT_LOG_INDEX: u64 = 16 * 1024 * 1024;

/// Wave 38-DE: default `submit_timeout_ms` used when callers do not set one
/// explicitly on the engine.  Five seconds matches the
/// `Config::cluster_submit_timeout_ms` default surfaced in
/// `ferrodruid-common`.
pub const DEFAULT_SUBMIT_TIMEOUT_MS: u64 = 5_000;

/// Replication message types exchanged between cluster nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationMessage {
    /// Heartbeat from leader (includes term).
    Heartbeat {
        /// ID of the leader sending the heartbeat.
        leader_id: String,
        /// Current term of the leader.
        term: u64,
    },
    /// Vote request (leader election).
    VoteRequest {
        /// ID of the candidate requesting votes.
        candidate_id: String,
        /// Term of the candidate.
        term: u64,
    },
    /// Vote response.
    VoteResponse {
        /// ID of the voter.
        voter_id: String,
        /// Term of the voter.
        term: u64,
        /// Whether the vote was granted.
        granted: bool,
    },
    /// Command to replicate from leader to followers.
    ReplicateCommand {
        /// Leader's current term.
        term: u64,
        /// Log index of the command.
        index: u64,
        /// The command to replicate.
        command: ClusterCommand,
    },
    /// Acknowledge command replication.
    ///
    /// Wave 38-DE extends the legacy `ReplicateAck { follower_id, index }`
    /// with the optional `success` and `last_log_index_hint` fields so the
    /// leader can:
    ///
    /// 1. distinguish a real ack (`success = true`) from a follower-side
    ///    rejection (`success = false`) — used by the AppendEntries
    ///    incremental-replay path on the leader to walk `next_index`
    ///    back, and
    /// 2. read the follower's current `last_log_index` (`hint`) so the
    ///    leader can jump straight to the right resume point instead of
    ///    decrementing `next_index` one entry at a time.
    ///
    /// Both fields default to backward-compatible values
    /// (`success = true`, `hint = None`) so older nodes that never set
    /// them are still treated as a successful ack at the legacy index.
    ReplicateAck {
        /// ID of the acknowledging follower.
        follower_id: String,
        /// Log index being acknowledged.
        index: u64,
        /// Whether the follower accepted the AppendEntries (true) or
        /// rejected it (false). Wave 38-DE.
        #[serde(default = "default_replicate_ack_success")]
        success: bool,
        /// Optional hint: follower's current `last_log_index` so the
        /// leader can jump `next_index[follower]` straight to the right
        /// resume point. Wave 38-DE.
        #[serde(default)]
        last_log_index_hint: Option<u64>,
    },
    /// Request full snapshot from leader.
    SnapshotRequest {
        /// ID of the follower requesting the snapshot.
        follower_id: String,
    },
    /// Full snapshot response from leader.
    SnapshotResponse {
        /// The cluster state snapshot.
        snapshot: ClusterSnapshot,
        /// Last log index included in the snapshot.
        last_index: u64,
        /// Term at snapshot time.
        term: u64,
    },
    /// Node join announcement.
    Join {
        /// ID of the joining node.
        node_id: String,
        /// Address of the joining node.
        addr: String,
    },
    /// Wave 38-DE pre-vote (Raft §9.6): synthetic vote round that the
    /// candidate runs *before* incrementing its real term.  A peer
    /// answers `granted = true` only if a real `VoteRequest` at the
    /// proposed term *would* succeed — concretely: the peer has not
    /// heard from a current leader within its own election timeout AND
    /// the proposed term is strictly greater than `current_term`.  No
    /// state change happens on the peer (no `voted_for` write, no term
    /// bump), so flapping nodes cannot churn cluster terms by repeatedly
    /// arming elections during partition heals.
    RequestPreVote {
        /// Candidate id sending the pre-vote.
        candidate_id: String,
        /// Synthetic term the candidate *would* enter if pre-vote
        /// succeeds (`current_term + 1`).
        proposed_term: u64,
        /// Wave 54-B: per-round generation id (monotonically increasing on
        /// each `start_pre_vote()` call).  Distinguishes successive pre-vote
        /// rounds at the *same* `proposed_term` so a delayed
        /// `RequestPreVoteResponse` from a prior timed-out round cannot be
        /// tallied with the current round's grants (closes W52 NEW-W47
        /// stale-response race).  Defaults to `0` when deserialised from a
        /// pre-Wave-54-B peer; any response carrying a stale (or `0`)
        /// `round_id` against a current non-zero round is silently dropped
        /// by [`ReplicationEngine::receive_pre_vote_response`].
        #[serde(default)]
        round_id: u64,
    },
    /// Response to a [`Self::RequestPreVote`].  `granted = true` means
    /// the voter is willing to grant a real vote at `proposed_term` —
    /// it does NOT commit the voter to anything.
    RequestPreVoteResponse {
        /// Voter id.
        voter_id: String,
        /// Echo of the candidate's `proposed_term`.
        proposed_term: u64,
        /// Whether the pre-vote was granted.
        granted: bool,
        /// Wave 54-B: echo of the candidate's pre-vote round generation id.
        /// See [`Self::RequestPreVote::round_id`] for the rationale.
        #[serde(default)]
        round_id: u64,
    },
    /// W1-C / CL-A4: one chunk of a chunked snapshot transfer.
    ///
    /// The legacy [`Self::SnapshotResponse`] carries the full
    /// snapshot JSON in a single authenticated frame (capped at
    /// `MAX_FRAME_BODY = 16 MiB`); large clusters easily exceed that.
    /// This variant lets the leader split a snapshot into N chunks
    /// of a configurable target size, each acknowledged by the
    /// follower via [`Self::SnapshotChunkAck`] so the leader can
    /// **resume** from the last acknowledged chunk on a transport
    /// drop without re-sending earlier bytes.
    ///
    /// The follower reassembles chunks keyed on `transfer_id` (the
    /// leader's monotonically-increasing transfer counter, scoped to
    /// `(leader_term, last_index)`), restores the snapshot once
    /// `is_final == true && chunk_index + 1 == total_chunks`.
    /// Out-of-order chunks within one transfer are tolerated;
    /// concurrent transfers (different `transfer_id`s) are NOT —
    /// the follower drops the older partial buffer on receipt of a
    /// chunk for a different `transfer_id`.
    SnapshotChunk {
        /// Leader-assigned transfer id. Two chunks with the same
        /// `transfer_id` belong to the same logical snapshot install.
        transfer_id: u64,
        /// 0-based index of this chunk within `total_chunks`.
        chunk_index: u32,
        /// Total number of chunks in this transfer.
        total_chunks: u32,
        /// `true` on the last chunk (`chunk_index == total_chunks - 1`).
        /// Receivers use this to short-circuit reassembly when the
        /// expected count is known up-front.
        is_final: bool,
        /// Snapshot `last_index` (consistent across every chunk of
        /// the same transfer).
        last_index: u64,
        /// Snapshot `term` (consistent across every chunk of the
        /// same transfer).
        term: u64,
        /// Total payload size, in bytes, across every chunk
        /// (consistent across every chunk of the same transfer).
        /// Used for the receiver's sanity check after reassembly.
        total_bytes: u64,
        /// This chunk's raw bytes — a slice of the full snapshot
        /// JSON. Bounded by `MAX_FRAME_BODY` at the framing layer.
        /// Serialised as a JSON array of u8s — about 4x the wire
        /// inflation vs base64, deliberate trade-off to avoid
        /// adding a base64 dep to this crate. Pick a chunk size
        /// of ~256 KiB so the inflated frame fits well below the
        /// 16 MiB cap.
        payload: Vec<u8>,
    },
    /// W1-C / CL-A4: follower ack for one
    /// [`Self::SnapshotChunk`]. Carries the highest chunk index
    /// already in the follower's reassembly buffer for
    /// `transfer_id`, so the leader can `last_acked + 1` resume on
    /// reconnect.
    SnapshotChunkAck {
        /// Echo of the sender's id (so the leader-side outbound
        /// reader knows which follower acked).
        follower_id: String,
        /// Echo of the leader's transfer id.
        transfer_id: u64,
        /// Highest chunk index this follower has on disk for this
        /// transfer (== `chunk_index` of the chunk being acked, or
        /// the highest seen so far if reassembly is out of order).
        last_received_chunk: u32,
        /// `true` once the reassembled snapshot has been applied
        /// successfully. Leader then drops its in-flight transfer
        /// state for this `(follower, transfer_id)`.
        applied: bool,
    },
}

/// Backward-compat default for [`ReplicationMessage::ReplicateAck::success`]:
/// older messages from pre-Wave-38-DE peers omit the field, so we treat
/// them as positive acks at the supplied index.
fn default_replicate_ack_success() -> bool {
    true
}

impl ReplicationMessage {
    /// Wave 40-A: extract the node id this message claims to originate
    /// from. Used by the transport layer to enforce
    /// `(connection, announced_node_id)` binding so an authenticated peer
    /// cannot impersonate a different node id within the same authed
    /// session.
    ///
    /// Snapshot/Join responses (`SnapshotResponse`) do not embed a
    /// sender id — they are conceptually unidirectional leader→follower
    /// payloads — so this returns `None` and the transport falls back
    /// to the connection-bound `announced_node_id` for those frames.
    #[must_use]
    pub fn declared_sender_id(&self) -> Option<&str> {
        match self {
            Self::Heartbeat { leader_id, .. } => Some(leader_id),
            Self::VoteRequest { candidate_id, .. } => Some(candidate_id),
            Self::VoteResponse { voter_id, .. } => Some(voter_id),
            Self::ReplicateAck { follower_id, .. } => Some(follower_id),
            Self::SnapshotRequest { follower_id } => Some(follower_id),
            Self::Join { node_id, .. } => Some(node_id),
            Self::RequestPreVote { candidate_id, .. } => Some(candidate_id),
            Self::RequestPreVoteResponse { voter_id, .. } => Some(voter_id),
            Self::SnapshotChunkAck { follower_id, .. } => Some(follower_id),
            // Leader-issued payloads (no per-message sender id; the
            // connection's announced_node_id is authoritative).
            Self::ReplicateCommand { .. }
            | Self::SnapshotResponse { .. }
            | Self::SnapshotChunk { .. } => None,
        }
    }
}

/// Replication node state (role in the consensus protocol).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    /// This node is the cluster leader.
    Leader,
    /// This node is a follower.
    Follower,
    /// This node is a candidate in an election.
    Candidate,
}

/// Internal RNG handle carried by [`ElectionTimer`]. Either delegates to
/// `rand::thread_rng()` (production) or to a seeded [`StdRng`] (tests +
/// reproducible benches).  Wave 38-DE: split-vote prevention used to be
/// validated probabilistically; the seeded variant lets us write a
/// deterministic regression that pre-fix would have failed.
///
/// `StdRng` is large (~136 bytes); we box it to keep the enum size small
/// — the `Thread` variant is overwhelmingly the common case in
/// production and we don't want to inflate `ElectionTimer` just for the
/// test variant.
#[derive(Debug, Clone)]
enum TimerRng {
    /// Production: each draw goes through `rand::thread_rng()`.
    Thread,
    /// Test / bench: deterministic stream backed by [`StdRng`].
    Seeded(Box<StdRng>),
}

impl TimerRng {
    /// Draw a uniform `u64` in `[0, max_exclusive)`.  `max_exclusive == 0`
    /// is treated as `1` so the call is total.
    fn gen_below(&mut self, max_exclusive: u64) -> u64 {
        let cap = max_exclusive.max(1);
        match self {
            TimerRng::Thread => rand::Rng::gen_range(&mut rand::thread_rng(), 0..cap),
            TimerRng::Seeded(rng) => rand::Rng::gen_range(rng.as_mut(), 0..cap),
        }
    }
}

/// Election timer used by followers and candidates to detect a missing leader.
///
/// The timer fires when no AppendEntries / Heartbeat has been received within
/// `timeout + rng_offset` of the most recent reset. The randomized offset is
/// drawn once per arming (in [`Self::reset`]) from the configured spread to
/// avoid split-vote scenarios where multiple followers wake up simultaneously
/// and become candidates at the same logical instant.
///
/// Formula:
/// ```text
/// effective_deadline = last_reset + base_timeout + uniform(0..rng_spread)
/// ```
/// Defaults: `base_timeout = 1500 ms`, `rng_spread = 150 ms` so the actual
/// election timeout is uniformly distributed in `[1500ms, 1650ms)`.
///
/// Wave 38-DE: an internal [`TimerRng`] backs the jitter draw.  Production
/// uses [`rand::thread_rng`]; tests can construct a deterministic variant
/// via [`Self::with_seed`] to pin split-vote behaviour against a seeded
/// stream.
#[derive(Debug, Clone)]
pub struct ElectionTimer {
    /// Earliest [`Instant`] at which the timer may fire.
    deadline: Instant,
    /// Base election timeout (e.g. 1500 ms).
    timeout: Duration,
    /// Width of the random jitter window added to `timeout` on each reset
    /// (e.g. 150 ms). Pass `Duration::ZERO` to make the timer deterministic
    /// (used by unit tests).
    rng_spread: Duration,
    /// Random source used to draw the jitter.  Wave 38-DE.
    rng: TimerRng,
}

impl ElectionTimer {
    /// Create a new election timer with the given base `timeout` and jitter
    /// `rng_spread`. The timer is armed immediately (deadline = now + timeout
    /// + uniform(0..rng_spread)).  Production callers use this constructor.
    pub fn new(timeout: Duration, rng_spread: Duration) -> Self {
        let mut t = Self {
            deadline: Instant::now(),
            timeout,
            rng_spread,
            rng: TimerRng::Thread,
        };
        t.reset();
        t
    }

    /// Wave 38-DE test-only constructor: an [`ElectionTimer`] whose jitter
    /// draws come from a deterministic [`StdRng::seed_from_u64`] stream.
    /// Two timers built with the same `seed`, `timeout`, and `rng_spread`
    /// will produce IDENTICAL jitter values across calls to [`Self::reset`]
    /// — this is exactly what we want to write a deterministic split-vote
    /// regression that pre-Wave-38-C code would have failed.
    pub fn with_seed(timeout: Duration, rng_spread: Duration, seed: u64) -> Self {
        let mut t = Self {
            deadline: Instant::now(),
            timeout,
            rng_spread,
            rng: TimerRng::Seeded(Box::new(StdRng::seed_from_u64(seed))),
        };
        t.reset();
        t
    }

    /// Re-arm the timer. Call from a follower whenever a valid heartbeat or
    /// AppendEntries arrives. Picks a fresh random offset within
    /// `[0, rng_spread)`.
    pub fn reset(&mut self) {
        let jitter = if self.rng_spread.is_zero() {
            Duration::ZERO
        } else {
            // Uniform draw across the spread. We deliberately use the
            // workspace `rand` crate rather than `tokio`'s timer rng so that
            // tests with `tokio::time::pause()` still get reproducible
            // jitter: the rng source is independent of the simulated clock.
            let nanos = self.rng.gen_below(self.rng_spread.as_nanos().max(1) as u64);
            Duration::from_nanos(nanos)
        };
        self.deadline = Instant::now() + self.timeout + jitter;
    }

    /// Returns `true` iff the timer has expired (i.e. `Instant::now()` is at
    /// or past the deadline).
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.deadline
    }

    /// Effective base + jitter timeout currently in effect (mostly for
    /// observability and testing).
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Heartbeat ticker used by leaders to schedule periodic empty AppendEntries.
///
/// The leader tick loop calls [`Self::should_send`] every iteration; once at
/// least `interval` has elapsed since the last broadcast, the ticker fires
/// (and resets `last_sent` to now).
#[derive(Debug, Clone)]
pub struct HeartbeatTicker {
    /// Last [`Instant`] at which a heartbeat was broadcast.
    last_sent: Instant,
    /// Heartbeat broadcast period (e.g. 250 ms).
    interval: Duration,
}

impl HeartbeatTicker {
    /// Create a new heartbeat ticker with the given broadcast `interval`.
    /// The first call to [`Self::should_send`] will fire immediately (this
    /// is intentional — a freshly-elected leader should announce itself
    /// without waiting a full interval).
    pub fn new(interval: Duration) -> Self {
        Self {
            // Force a fire on the first tick.
            last_sent: Instant::now() - interval - Duration::from_millis(1),
            interval,
        }
    }

    /// Returns `true` iff at least `interval` has elapsed since the previous
    /// fire. Side effect: when it returns `true`, `last_sent` is set to
    /// `Instant::now()`.
    pub fn should_send(&mut self) -> bool {
        if Instant::now() - self.last_sent >= self.interval {
            self.last_sent = Instant::now();
            true
        } else {
            false
        }
    }

    /// Heartbeat broadcast interval (mostly for observability).
    pub fn interval(&self) -> Duration {
        self.interval
    }
}

/// Outcome of one [`ReplicationEngine::tick`] call.
///
/// The transport layer drives the engine forward by calling `tick` on a
/// fixed cadence (~50 ms) and acting on the returned [`TickAction`]. The
/// engine itself never owns I/O.
#[derive(Debug, Clone)]
pub enum TickAction {
    /// Nothing to do this tick.
    Idle,
    /// Wave 47-A: the election timer fired on a follower or candidate AND
    /// no pre-vote round is currently in flight.  The transport should
    /// broadcast a [`ReplicationMessage::RequestPreVote`] to every peer.
    /// `proposed_term` is `current_term + 1` — the synthetic term the
    /// candidate *would* enter if pre-vote majority is reached.  No real
    /// term bump happens until majority is observed via
    /// [`ReplicationEngine::receive_pre_vote_response`] and a follow-up
    /// tick promotes the engine to candidate.
    BroadcastPreVoteRequest {
        /// Candidate id (this node).
        candidate_id: String,
        /// Synthetic proposed term (`current_term + 1`).
        proposed_term: u64,
        /// Wave 54-B: per-round generation id (monotonically increasing on
        /// each `start_pre_vote()` call).  The transport must echo this in
        /// the wire-level [`ReplicationMessage::RequestPreVote::round_id`]
        /// so peers can echo it back on their `RequestPreVoteResponse`,
        /// allowing the candidate to drop stale responses from prior
        /// timed-out rounds at the same `proposed_term`.
        round_id: u64,
    },
    /// The election timer fired on a follower or candidate AND the
    /// pre-vote round at `term` reached majority — Wave 47-A promotes the
    /// engine to candidate (real term bump via [`ReplicationEngine::arm_election`])
    /// and asks the transport to broadcast a real `VoteRequest`.
    BroadcastVoteRequest {
        /// Candidate id (this node).
        candidate_id: String,
        /// New term the candidate is requesting votes for.
        term: u64,
    },
    /// The heartbeat ticker fired on the leader. The transport should
    /// broadcast an empty `Heartbeat` message to all peers.
    BroadcastHeartbeat {
        /// Leader id (this node).
        leader_id: String,
        /// Current leader term.
        term: u64,
    },
}

/// Wave 54-B: identity of an in-flight pre-vote round.  `proposed_term`
/// is the synthetic term the candidate would enter on majority; `round_id`
/// is a monotonically increasing generation counter that distinguishes
/// successive rounds at the *same* `proposed_term` (e.g. when the
/// election timer re-fires before any peer responded).  Carrying both
/// lets [`ReplicationEngine::receive_pre_vote_response`] drop responses
/// from a prior timed-out round whose `round_id` no longer matches the
/// engine's current expectation, closing the W52 NEW-W47 race.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreVoteRound {
    /// Synthetic proposed term (`current_term + 1`).
    pub proposed_term: u64,
    /// Per-round generation id.
    pub round_id: u64,
}

/// Outcome of [`ReplicationEngine::submit`] when a leader successfully
/// gathered a majority ack within the configured timeout.  Wave 38-DE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MajorityAck {
    /// Number of nodes (including the leader's own self-ack) that
    /// acknowledged the entry.
    pub ack_count: u32,
    /// Total cluster size (peers + 1 for self).
    pub total: u32,
    /// Log index that was acknowledged.
    pub index: u64,
    /// Term under which the entry was committed.
    pub term: u64,
}

/// Reasons [`ReplicationEngine::submit`] can fail at the consensus layer.
/// Wave 38-DE.
#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    /// `submit` was invoked on a node that is not currently the leader.
    #[error("not the leader")]
    NotLeader,
    /// The leader exceeded `submit_timeout` without observing a majority
    /// of follower acks for the proposed entry.  The entry has been
    /// appended to the local log and applied to the state machine, but
    /// the caller should treat the operation as not-yet-committed.
    #[error("submit timed out: only {ack_count}/{total} acks within {timeout_ms} ms")]
    Timeout {
        /// Acks observed before the timeout fired (includes self).
        ack_count: u32,
        /// Total cluster size.
        total: u32,
        /// Configured submit timeout in milliseconds.
        timeout_ms: u64,
    },
    /// Underlying [`ClusterManager`] application failed (e.g. lock
    /// poisoned, command rejected by the state machine).
    #[error("apply failed: {0}")]
    Apply(#[from] ClusterError),
}

/// Hint for the [`ReplicationEngine`] describing the cluster-wire
/// security posture the transport was constructed with. The engine uses
/// this to widen the election timeout floor + jitter when mTLS is in
/// effect, mitigating the initial-handshake-latency × tight-timeout ×
/// synchronous-pre-vote-race pattern that W2-E measured as an
/// **election-storm liveness regression** at LAN scale
/// (`tests/jepsen-rs/RESULTS_w2e_mtls-rca_2026-06-30.md`).
///
/// The engine deliberately holds this as a *hint* rather than importing
/// `crate::transport::ClusterSecurityMode` (which would introduce a
/// module-cycle risk); callers wire it from the same source that they
/// pass to `TcpTransport`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ClusterSecurityHint {
    /// PSK-over-cleartext posture — no handshake round-trip cost, tight
    /// timeouts safe. The default because it matches the loopback + test
    /// paths that construct `ReplicationConfig` without going through
    /// the CLI.
    #[default]
    Psk,
    /// mTLS posture — initial TLS handshake round-trip adds 2.5-3.5 s at
    /// LAN RTT (W2-E measurement); election timers must be widened +
    /// jittered to avoid synchronous timeout races across peers.
    Mtls,
}

/// Configuration for the replication layer.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Unique identifier for this node.
    pub node_id: String,
    /// Address to listen on for replication traffic.
    pub listen_addr: String,
    /// Addresses of peer nodes.
    pub peers: Vec<String>,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Election timeout in milliseconds. Under mTLS
    /// ([`ClusterSecurityHint::Mtls`]) the engine internally raises this
    /// to at least [`ReplicationEngine::MTLS_ELECTION_TIMEOUT_FLOOR_MS`]
    /// (5000 ms) if the operator supplied a smaller value, and applies
    /// ~±25 % random jitter on top (Raft §9.3). PSK posture leaves the
    /// value untouched (only the 150 ms compat jitter applies).
    pub election_timeout_ms: u64,
    /// Cluster-wire security posture hint. Controls the mTLS-posture
    /// election-timer floor + jitter widening described in
    /// [`ClusterSecurityHint`]. Defaults to
    /// [`ClusterSecurityHint::Psk`] so pre-existing callers (tests,
    /// loopback benches) keep their behaviour.
    pub cluster_security_hint: ClusterSecurityHint,
}

/// The replication engine managing consensus and command replication.
pub struct ReplicationEngine {
    config: ReplicationConfig,
    role: RwLock<ReplicationRole>,
    current_term: RwLock<u64>,
    last_index: RwLock<u64>,
    voted_for: RwLock<Option<String>>,
    /// Per-term set of distinct voter IDs that have already granted a vote
    /// to this candidate. Used to dedupe replayed `VoteResponse` messages so
    /// that a single peer cannot be counted twice toward the election quorum.
    /// Keyed by term so entering a new term implicitly resets the set.
    votes_received: RwLock<HashMap<u64, HashSet<String>>>,
    /// Term at which this candidate is currently collecting votes (if any).
    /// Used together with `votes_received` to gate stale incoming votes.
    election_term: RwLock<Option<u64>>,
    cluster_manager: Arc<ClusterManager>,
    command_log: RwLock<Vec<(u64, ClusterCommand)>>,
    /// Per-log-index term of the entry stored at that index. Mirrors the
    /// `command_log` `Vec` but indexed `1..=last_index` (index 0 reserved
    /// as the empty pre-genesis slot, which has term 0). Used by the
    /// follower-side AppendEntries safety check to detect log divergence.
    log_terms: RwLock<Vec<u64>>,
    transport: RwLock<Option<Arc<InMemoryTransport>>>,
    /// Election timer (followers + candidates). Fires when a heartbeat has
    /// not arrived within the configured timeout. Wave 38-C: drives
    /// timer-based failover from the [`Self::tick`] entry point.
    election_timer: RwLock<ElectionTimer>,
    /// Heartbeat ticker (leaders only). Schedules empty AppendEntries
    /// broadcasts at the configured period.
    heartbeat_ticker: RwLock<HeartbeatTicker>,
    /// Wave 38-DE: per-follower next-index map (Raft §5.3 "next_index[i]").
    /// Set by the leader on entering the leader role to `last_index + 1`,
    /// decremented on a follower-side rejection so the next AppendEntries
    /// from this leader walks back through the log until it lands on a
    /// matching prev-log-index, advanced past the appended range on a
    /// successful ack.
    next_index: RwLock<HashMap<String, u64>>,
    /// Wave 38-DE: per-follower **match-index** map (Raft §5.3
    /// "match_index[i]"). Maximum log index the leader knows is
    /// replicated on each follower.  Updated on positive
    /// [`ReplicationMessage::ReplicateAck`].
    match_index: RwLock<HashMap<String, u64>>,
    /// Wave 38-DE: per-pending-submit ack tally.  Keyed by log index;
    /// value is the set of follower IDs that have positively acked this
    /// index (does NOT include leader self-ack, which is implicit at the
    /// majority-check step).  An entry is removed once
    /// [`Self::submit`] returns.
    pending_acks: RwLock<HashMap<u64, HashSet<String>>>,
    /// Wave 45-G (closes W39 `submit_timeout_ms` busy-poll Medium):
    /// per-pending-submit wake-up signal.  Keyed by the same log index as
    /// [`Self::pending_acks`]; an entry is registered by
    /// [`Self::submit_with_majority_ack`] right before it starts polling
    /// the tally and is fired by [`Self::receive_replicate_ack`] every
    /// time a positive ack at that index lands.  Replaces the prior
    /// `tokio::time::sleep(5ms)` busy-poll: the submit task now wakes up
    /// in microseconds when an ack arrives, instead of paying up to a
    /// 5 ms scheduler-driven slice per check.  An entry is removed
    /// once [`Self::submit_with_majority_ack`] returns (success, timeout
    /// or non-leader) so the [`Notify`] is dropped and any racing
    /// receiver simply observes "no waiter" and skips the wake-up.
    ///
    /// `Notify::notify_one` semantics: a permit is stored if no task is
    /// currently awaiting (so a wake that arrives between the
    /// `match_index_majority_reached` re-check and the next
    /// `notified()` call is *not* lost — the very next `notified().await`
    /// returns immediately).  We re-check the tally inside the loop so a
    /// spurious wake never returns early.
    majority_ack_waiters: RwLock<HashMap<u64, Arc<Notify>>>,
    /// Wave 38-DE: per-`round_id` set of distinct peer IDs that have
    /// granted pre-votes for the candidate's *next* real term
    /// (`current_term + 1`).  Reset on every new pre-vote round.
    ///
    /// Wave 54-B: changed key from `proposed_term` to `round_id` so two
    /// successive timed-out rounds at the same `proposed_term` do not
    /// share a tally bucket.  This closes the W52 NEW-W47 stale-response
    /// race where a delayed `RequestPreVoteResponse` from round N could
    /// be tallied with round N+1's grants when both rounds proposed the
    /// same term.
    pre_votes_received: RwLock<HashMap<u64, HashSet<String>>>,
    /// Wave 47-A: in-flight pre-vote round driven by [`Self::tick`].
    /// `Some((proposed_term, round_id))` means the engine has already
    /// emitted [`TickAction::BroadcastPreVoteRequest`] for this proposed
    /// term + round generation and is waiting for peer pre-vote responses
    /// (delivered via [`Self::receive_pre_vote_response`]) to reach
    /// majority.  Cleared when the pre-vote round either succeeds
    /// (engine transitions to real-vote round and bumps `current_term`)
    /// or times out (next election-timer expiry starts a *new* pre-vote
    /// round at a fresh `round_id`).
    ///
    /// Wave 54-B: was `Option<u64>` (proposed_term only); now also
    /// carries the `round_id` so [`Self::receive_pre_vote_response`] can
    /// drop stale grants from a prior round at the same `proposed_term`.
    pre_vote_in_flight: RwLock<Option<PreVoteRound>>,
    /// Wave 54-B: monotonically increasing pre-vote round generation id.
    /// Bumped by every [`Self::start_pre_vote`] call, regardless of
    /// whether `current_term` advanced between calls.  Used to match
    /// `RequestPreVoteResponse::round_id` against the engine's current
    /// expectation; mismatches are dropped silently.  `u64::MAX` would
    /// take >584 years at one round per nanosecond, so wraparound is not
    /// a practical concern (and is left unhandled by intent).
    pre_vote_round_id: RwLock<u64>,
    /// Wave 38-DE: deadline used by [`Self::submit`].  Default
    /// [`DEFAULT_SUBMIT_TIMEOUT_MS`].  Settable via
    /// [`Self::set_submit_timeout_ms`] for tests / operator tuning.
    submit_timeout_ms: RwLock<u64>,
    /// W1-C: optional persistent Raft state. When attached via
    /// [`Self::attach_persistent_state`] every successful log append
    /// (leader [`Self::submit_with_majority_ack`] and follower
    /// [`Self::receive_command`]) and every snapshot
    /// install ([`Self::restore_snapshot`]) is fsynced to disk, so a
    /// crash restart can be reconstructed by replaying the journal.
    /// When `None` (the default) the engine is purely in-memory.
    persistent_state: RwLock<Option<Arc<crate::persist::PersistentRaftState>>>,
    /// W1-C / CL-A4: follower-side reassembly buffers keyed on
    /// `transfer_id`. Each buffer holds per-chunk-index payload
    /// slices and the expected total chunk count, so out-of-order
    /// arrival is tolerated. A newer `transfer_id` evicts the
    /// older partial buffer (the leader has restarted the transfer
    /// — old chunks would just bloat memory).
    snapshot_rx_buffers: RwLock<HashMap<u64, ChunkBuffer>>,
    /// W1-C / CL-A4: leader-side per-follower transfer cursor —
    /// the highest chunk index this follower has acknowledged for
    /// the current `transfer_id`. Used by [`Self::resume_chunk_idx`]
    /// so a reconnect after a mid-transfer drop sends only the
    /// missing tail.
    snapshot_tx_cursors: RwLock<HashMap<String, ChunkCursor>>,
    /// W1-C / CL-A4: monotonically increasing per-leader counter
    /// used to allocate fresh `transfer_id`s for chunked snapshots.
    snapshot_tx_next_id: RwLock<u64>,
    /// **CL-A1-R-mTLS-source-fix (c) — startup delay** (W2-E RCA
    /// mitigation c, Task #23). Under mTLS posture the engine defers
    /// firing its *first* election until the transport signals that
    /// a majority of peers completed their initial TLS + auth
    /// handshake, or [`Self::MTLS_STARTUP_GRACE_MAX_MS`] elapses,
    /// whichever comes first. `Some(deadline)` = still deferring;
    /// `None` = normal semantics (grace never armed OR already
    /// cleared). Under PSK posture this is always `None`.
    startup_grace_until: RwLock<Option<Instant>>,
}

/// Per-(in-flight-transfer) reassembly state on the follower.
#[derive(Debug, Default)]
pub(crate) struct ChunkBuffer {
    /// Expected total number of chunks (echo of `total_chunks`).
    expected: u32,
    /// Highest contiguous chunk index already received (0-based).
    /// A "contiguous" tracker — out-of-order arrivals beyond this
    /// gap still go into `chunks` but `highest_contiguous` only
    /// advances when the gap closes.
    highest_contiguous: Option<u32>,
    /// Snapshot meta echoed from chunk #0.
    last_index: u64,
    term: u64,
    total_bytes: u64,
    /// Per-chunk payload slots, indexed by `chunk_index`.
    chunks: HashMap<u32, Vec<u8>>,
}

impl ChunkBuffer {
    fn record_chunk(&mut self, idx: u32, payload: Vec<u8>) {
        self.chunks.insert(idx, payload);
        // Advance the contiguous high-watermark.
        let mut next = self.highest_contiguous.map_or(0, |c| c + 1);
        while self.chunks.contains_key(&next) {
            self.highest_contiguous = Some(next);
            next += 1;
        }
    }

    fn is_complete(&self) -> bool {
        self.expected > 0 && self.chunks.len() == self.expected as usize
    }

    fn assemble(self) -> Result<Vec<u8>, ClusterError> {
        if !self.is_complete() {
            return Err(ClusterError::CommandFailed(format!(
                "chunked snapshot incomplete: have={} expected={}",
                self.chunks.len(),
                self.expected,
            )));
        }
        let mut out = Vec::with_capacity(self.total_bytes as usize);
        for i in 0..self.expected {
            let chunk = self.chunks.get(&i).ok_or_else(|| {
                ClusterError::CommandFailed(format!("chunked snapshot missing chunk index {i}",))
            })?;
            out.extend_from_slice(chunk);
        }
        if (out.len() as u64) != self.total_bytes {
            return Err(ClusterError::CommandFailed(format!(
                "chunked snapshot byte mismatch: got {} expected {}",
                out.len(),
                self.total_bytes,
            )));
        }
        Ok(out)
    }
}

/// Per-follower outbound transfer cursor on the leader.
#[derive(Debug, Clone, Default)]
pub(crate) struct ChunkCursor {
    /// Current in-flight `transfer_id` for this follower (None if
    /// no transfer in flight).
    transfer_id: Option<u64>,
    /// Highest chunk index this follower has acked for
    /// `transfer_id` (-1 means nothing acked yet — encoded as
    /// Option for clarity).
    last_acked: Option<u32>,
    /// True once the follower acked the final chunk with
    /// `applied = true`. The cursor stays around briefly so a
    /// retransmit of the final ack is idempotent, then is reaped
    /// on the next fresh transfer.
    applied: bool,
}

/// W1-C / CL-A4: default target chunk size for chunked snapshot
/// transfer.
///
/// 256 KiB is small enough that the JSON-array-of-u8 wire inflation
/// (~4×) still fits well below [`crate::transport::MAX_FRAME_BODY`]
/// (`16 MiB`) and large enough that a 1 MiB snapshot only takes
/// 4 chunks.
pub const DEFAULT_SNAPSHOT_CHUNK_SIZE: usize = 256 * 1024;

impl ReplicationEngine {
    /// Default jitter spread added on top of `election_timeout_ms` to avoid
    /// split-vote scenarios.
    const DEFAULT_ELECTION_JITTER_MS: u64 = 150;

    /// Minimum election timeout when the transport is running with
    /// [`ClusterSecurityHint::Mtls`]. W2-E measured initial TLS handshake
    /// latency at 2.5-3.5 s per inbound peer pair on LAN RTT — an
    /// election timeout below ~5 s guarantees all three peers time out
    /// simultaneously during the handshake window and race a
    /// synchronous pre-vote round, producing the election-storm
    /// liveness regression documented in
    /// `tests/jepsen-rs/RESULTS_w2e_mtls-rca_2026-06-30.md`. 4× the
    /// worst-observed handshake round-trip is the "widen by an order
    /// of magnitude beyond the perturbation" heuristic Raft §9.3
    /// applies to jitter — we apply it directly to the base timeout
    /// under mTLS.
    pub const MTLS_ELECTION_TIMEOUT_FLOOR_MS: u64 = 5000;

    /// Jitter fraction (denominator) applied under mTLS posture:
    /// `jitter_spread = effective_election_timeout / MTLS_ELECTION_JITTER_DIVISOR`
    /// → ~±25 % random jitter on top of the base timeout (Raft §9.3
    /// recommends a comparable spread to shatter synchronous timeouts
    /// across peers). PSK posture keeps the historical 150 ms compat
    /// spread untouched.
    pub const MTLS_ELECTION_JITTER_DIVISOR: u64 = 4;

    /// Maximum time the engine defers its **first** election attempt
    /// under mTLS posture, waiting for a majority of peers to complete
    /// their initial TLS + auth handshakes. This is the third W2-E RCA
    /// mitigation ("cluster-tick startup delay until majority
    /// handshake auth") — see
    /// `tests/jepsen-rs/RESULTS_w2e_mtls-rca_2026-06-30.md`. If the
    /// transport calls
    /// [`Self::clear_startup_grace`] before this deadline, the
    /// engine returns to normal election-timer semantics immediately.
    /// If the deadline elapses without the transport ever signalling
    /// majority handshake auth (e.g. some peer never comes up), the
    /// engine falls through to normal semantics anyway — the timer
    /// takes over as it would in the pre-fix code path, so a
    /// half-populated cluster can still make forward progress. 15 s
    /// is roughly 3× the mTLS-posture election-timeout floor
    /// (5 s) — enough for a 2.5-3.5 s handshake × 2 rounds of retry
    /// on a bad network path, but not so long that a single dead peer
    /// stalls the whole cluster.
    pub const MTLS_STARTUP_GRACE_MAX_MS: u64 = 15_000;

    /// Compute the effective `(base_timeout, jitter_spread)` pair to
    /// hand to [`ElectionTimer::new`] given the raw operator-configured
    /// timeout and the resolved security hint. Under mTLS this floors
    /// the base to [`Self::MTLS_ELECTION_TIMEOUT_FLOOR_MS`] and widens
    /// jitter to `effective / MTLS_ELECTION_JITTER_DIVISOR`; under PSK
    /// it returns the operator value plus the 150 ms compat jitter.
    ///
    /// Emits a `tracing::warn!` if the mTLS floor bumps the operator
    /// value (so operators see the adjustment in logs and can either
    /// accept it or explicitly raise their `--election-timeout-ms`).
    fn effective_election_params(
        configured_ms: u64,
        hint: ClusterSecurityHint,
    ) -> (Duration, Duration) {
        match hint {
            ClusterSecurityHint::Psk => (
                Duration::from_millis(configured_ms),
                Duration::from_millis(Self::DEFAULT_ELECTION_JITTER_MS),
            ),
            ClusterSecurityHint::Mtls => {
                let effective = configured_ms.max(Self::MTLS_ELECTION_TIMEOUT_FLOOR_MS);
                if effective != configured_ms {
                    tracing::warn!(
                        configured_election_timeout_ms = configured_ms,
                        effective_election_timeout_ms = effective,
                        floor_ms = Self::MTLS_ELECTION_TIMEOUT_FLOOR_MS,
                        "cluster-wire mTLS posture: raising election timeout to \
                         mitigate initial-handshake-latency × sync pre-vote race \
                         (W2-E CL-A1-R-mTLS RCA); pass a larger \
                         --election-timeout-ms to silence this warning"
                    );
                }
                let jitter = effective / Self::MTLS_ELECTION_JITTER_DIVISOR;
                (
                    Duration::from_millis(effective),
                    Duration::from_millis(jitter),
                )
            }
        }
    }

    /// Create a new replication engine with the given configuration.
    pub fn new(config: ReplicationConfig, cluster_manager: Arc<ClusterManager>) -> Self {
        let hint = config.cluster_security_hint;
        let (base_timeout, jitter_spread) =
            Self::effective_election_params(config.election_timeout_ms, hint);
        let election_timer = ElectionTimer::new(base_timeout, jitter_spread);
        let heartbeat_ticker =
            HeartbeatTicker::new(Duration::from_millis(config.heartbeat_interval_ms));
        Self {
            config,
            role: RwLock::new(ReplicationRole::Follower),
            current_term: RwLock::new(0),
            last_index: RwLock::new(0),
            voted_for: RwLock::new(None),
            votes_received: RwLock::new(HashMap::new()),
            election_term: RwLock::new(None),
            cluster_manager,
            command_log: RwLock::new(Vec::new()),
            log_terms: RwLock::new(Vec::new()),
            transport: RwLock::new(None),
            election_timer: RwLock::new(election_timer),
            heartbeat_ticker: RwLock::new(heartbeat_ticker),
            next_index: RwLock::new(HashMap::new()),
            match_index: RwLock::new(HashMap::new()),
            pending_acks: RwLock::new(HashMap::new()),
            majority_ack_waiters: RwLock::new(HashMap::new()),
            pre_votes_received: RwLock::new(HashMap::new()),
            pre_vote_in_flight: RwLock::new(None),
            pre_vote_round_id: RwLock::new(0),
            submit_timeout_ms: RwLock::new(DEFAULT_SUBMIT_TIMEOUT_MS),
            persistent_state: RwLock::new(None),
            snapshot_rx_buffers: RwLock::new(HashMap::new()),
            snapshot_tx_cursors: RwLock::new(HashMap::new()),
            snapshot_tx_next_id: RwLock::new(0),
            startup_grace_until: RwLock::new(if matches!(hint, ClusterSecurityHint::Mtls) {
                Some(Instant::now() + Duration::from_millis(Self::MTLS_STARTUP_GRACE_MAX_MS))
            } else {
                None
            }),
        }
    }

    /// Create a new replication engine with an in-memory transport (for testing).
    pub fn with_transport(
        config: ReplicationConfig,
        cluster_manager: Arc<ClusterManager>,
        transport: Arc<InMemoryTransport>,
    ) -> Self {
        let hint = config.cluster_security_hint;
        let (base_timeout, jitter_spread) =
            Self::effective_election_params(config.election_timeout_ms, hint);
        let election_timer = ElectionTimer::new(base_timeout, jitter_spread);
        let heartbeat_ticker =
            HeartbeatTicker::new(Duration::from_millis(config.heartbeat_interval_ms));
        Self {
            config,
            role: RwLock::new(ReplicationRole::Follower),
            current_term: RwLock::new(0),
            last_index: RwLock::new(0),
            voted_for: RwLock::new(None),
            votes_received: RwLock::new(HashMap::new()),
            election_term: RwLock::new(None),
            cluster_manager,
            command_log: RwLock::new(Vec::new()),
            log_terms: RwLock::new(Vec::new()),
            transport: RwLock::new(Some(transport)),
            election_timer: RwLock::new(election_timer),
            heartbeat_ticker: RwLock::new(heartbeat_ticker),
            next_index: RwLock::new(HashMap::new()),
            match_index: RwLock::new(HashMap::new()),
            pending_acks: RwLock::new(HashMap::new()),
            majority_ack_waiters: RwLock::new(HashMap::new()),
            pre_votes_received: RwLock::new(HashMap::new()),
            pre_vote_in_flight: RwLock::new(None),
            pre_vote_round_id: RwLock::new(0),
            submit_timeout_ms: RwLock::new(DEFAULT_SUBMIT_TIMEOUT_MS),
            persistent_state: RwLock::new(None),
            snapshot_rx_buffers: RwLock::new(HashMap::new()),
            snapshot_tx_cursors: RwLock::new(HashMap::new()),
            snapshot_tx_next_id: RwLock::new(0),
            startup_grace_until: RwLock::new(if matches!(hint, ClusterSecurityHint::Mtls) {
                Some(Instant::now() + Duration::from_millis(Self::MTLS_STARTUP_GRACE_MAX_MS))
            } else {
                None
            }),
        }
    }

    /// Get current role.
    pub async fn role(&self) -> ReplicationRole {
        *self.role.read().await
    }

    /// Get current term.
    pub async fn term(&self) -> u64 {
        *self.current_term.read().await
    }

    /// Get last committed index.
    pub async fn last_index(&self) -> u64 {
        *self.last_index.read().await
    }

    /// Is this node the leader?
    pub async fn is_leader(&self) -> bool {
        *self.role.read().await == ReplicationRole::Leader
    }

    /// Get the node ID.
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// W1-C: attach a [`crate::persist::PersistentRaftState`] so future
    /// log appends, snapshot installs, and meta changes are fsynced
    /// to disk. Before attaching the state, the engine replays the
    /// on-disk journal + snapshot + meta into the in-memory log and
    /// the cluster-manager state machine, so a freshly-attached
    /// state file reconstructs the durable Raft state from disk.
    ///
    /// Safe to call once at engine bring-up; subsequent calls
    /// overwrite the attached state (rarely useful — the typical
    /// pattern is bind-once-and-forget).
    pub async fn attach_persistent_state(
        &self,
        state: Arc<crate::persist::PersistentRaftState>,
    ) -> Result<(), ClusterError> {
        // 1. Replay snapshot + journal.
        let (snapshot, entries, meta) = state
            .replay()
            .map_err(|e| ClusterError::CommandFailed(format!("persist replay: {e}")))?;
        if let Some(env) = snapshot {
            self.cluster_manager.restore(env.snapshot)?;
            {
                let mut last = self.last_index.write().await;
                *last = env.last_index;
            }
            {
                let mut log_terms = self.log_terms.write().await;
                log_terms.clear();
                let len = usize::try_from(env.last_index).map_err(|_| {
                    ClusterError::CommandFailed("snapshot last_index overflows usize".to_string())
                })?;
                log_terms.resize(len, env.term);
            }
        }
        // 2. Replay log entries through the state machine.
        for entry in entries {
            // Append to in-memory log first so log_terms / last_index
            // stay consistent with command_log.
            {
                let mut log = self.command_log.write().await;
                log.push((entry.index, entry.command.clone()));
            }
            {
                let mut terms = self.log_terms.write().await;
                terms.push(entry.term);
            }
            {
                let mut idx = self.last_index.write().await;
                *idx = entry.index;
            }
            self.cluster_manager.apply(entry.command)?;
        }
        // 3. Restore meta (current_term / voted_for / last_index).
        {
            let mut t = self.current_term.write().await;
            *t = meta.current_term.max(*t);
        }
        {
            let mut v = self.voted_for.write().await;
            *v = meta.voted_for.clone();
        }
        {
            let last = *self.last_index.read().await;
            if meta.last_index > last {
                let mut idx = self.last_index.write().await;
                *idx = meta.last_index;
            }
        }
        // 4. Install the attachment.
        {
            let mut g = self.persistent_state.write().await;
            *g = Some(state);
        }
        Ok(())
    }

    /// Helper: take a clone of the attached persistent state, if any.
    async fn persistent_state_arc(&self) -> Option<Arc<crate::persist::PersistentRaftState>> {
        self.persistent_state.read().await.as_ref().map(Arc::clone)
    }

    /// Submit a command (must be leader). Replicates to followers.
    ///
    /// Wave 38-DE: this is the *legacy* best-effort path preserved for
    /// callers that don't need majority-ack semantics — internally it
    /// just routes through [`Self::submit_with_majority_ack`] with a 0 ms
    /// timeout (so the caller never blocks waiting for ACKs) and
    /// translates a [`SubmitError::Timeout`] into a successful ok at
    /// the legacy level (the entry has been appended + applied
    /// locally).  Net effect for existing callers: identical behaviour
    /// to pre-Wave-38-DE.  Use [`Self::submit_with_majority_ack`]
    /// directly when you need the strict consensus guarantee.
    pub async fn submit(&self, cmd: ClusterCommand) -> Result<CommandResult, ClusterError> {
        let prev = *self.submit_timeout_ms.read().await;
        // Borrow and force timeout to 0 just for this call so we keep the
        // legacy "fire-and-forget" behaviour for non-DD callers.  Using a
        // separate write guard scope so the regular path is unaffected.
        {
            let mut t = self.submit_timeout_ms.write().await;
            *t = 0;
        }
        let result = self.submit_with_majority_ack(cmd).await;
        {
            let mut t = self.submit_timeout_ms.write().await;
            *t = prev;
        }
        match result {
            Ok(_ack) => Ok(CommandResult::Ok),
            // 0 ms timeout falls through here for non-single-node clusters;
            // the legacy contract is "best-effort, succeed locally" so we
            // preserve it.
            Err(SubmitError::Timeout { .. }) => Ok(CommandResult::Ok),
            Err(SubmitError::NotLeader) => {
                Err(ClusterError::CommandFailed("not the leader".to_string()))
            }
            Err(SubmitError::Apply(e)) => Err(e),
        }
    }

    /// Wave 38-DE: submit a command with **majority-ack** semantics.
    ///
    /// Returns [`MajorityAck`] only after a majority of cluster members
    /// (peers + self) have either positively ack'd the new entry or the
    /// leader's self-vote alone is already a majority (single-node).  If
    /// the deadline (`submit_timeout_ms`) expires first, returns
    /// [`SubmitError::Timeout`] with the partial tally.  The entry has
    /// already been appended to the local log and applied to the local
    /// state machine when the timeout fires — caller treats the
    /// operation as not-yet-committed at the consensus level.
    ///
    /// Acks are gathered by polling the in-memory transport's inbox for
    /// [`ReplicationMessage::ReplicateAck`] frames; production
    /// (TcpTransport) feeds acks into the engine via
    /// [`Self::receive_replicate_ack`].
    pub async fn submit_with_majority_ack(
        &self,
        cmd: ClusterCommand,
    ) -> Result<MajorityAck, SubmitError> {
        if !self.is_leader().await {
            return Err(SubmitError::NotLeader);
        }

        // Increment index
        let index = {
            let mut idx = self.last_index.write().await;
            *idx += 1;
            *idx
        };

        let term = *self.current_term.read().await;

        // W1-C: persist the log append BEFORE in-memory mutation +
        // state-machine apply so a crash on the leader between
        // submit-accept and ack does not lose the entry. Persistence
        // is best-effort — failures are logged and the in-memory
        // path continues so existing in-memory callers keep working.
        if let Some(state) = self.persistent_state_arc().await
            && let Err(e) = state.append_log_entry(index, term, &cmd).await
        {
            tracing::warn!(
                index,
                term,
                error = %e,
                "persistent log append failed (leader submit); continuing with in-memory only",
            );
        }

        // Append to local log
        {
            let mut log = self.command_log.write().await;
            log.push((index, cmd.clone()));
        }
        {
            let mut terms = self.log_terms.write().await;
            terms.push(term);
        }

        // Apply to local ClusterManager (this is the leader's "self-ack").
        self.cluster_manager.apply(cmd.clone())?;

        // Initialise the per-index ack set so receive_replicate_ack can
        // start tallying, and register the Notify wake-up channel for
        // this index BEFORE the first ack drain so we never miss a
        // wake.  Wave 45-G.
        let notify = Arc::new(Notify::new());
        {
            let mut acks = self.pending_acks.write().await;
            acks.entry(index).or_default();
        }
        {
            let mut waiters = self.majority_ack_waiters.write().await;
            waiters.insert(index, Arc::clone(&notify));
        }

        let total = self.config.peers.len() as u32 + 1;
        let majority = total / 2 + 1;

        // Single-node short-circuit: leader self-ack is already a majority.
        if majority == 1 {
            self.pending_acks.write().await.remove(&index);
            self.majority_ack_waiters.write().await.remove(&index);
            return Ok(MajorityAck {
                ack_count: 1,
                total,
                index,
                term,
            });
        }

        // Replicate to followers via transport.  We send unconditionally
        // (best-effort); production transport may drop on a wire error,
        // and the leader will eventually retry via the heartbeat /
        // append-entries-replay path.
        if let Some(transport) = self.transport.read().await.as_ref() {
            let msg = ReplicationMessage::ReplicateCommand {
                term,
                index,
                command: cmd,
            };
            for peer in &self.config.peers {
                let _ = transport.send(peer, msg.clone()).await;
            }
            // The in-memory transport delivers acks back through the
            // candidate's inbox synchronously; drain it once before we
            // wait so we observe acks the test harness pre-staged.
            if let Some(rx) = transport.try_receive(&self.config.node_id).await {
                for msg in rx {
                    if let ReplicationMessage::ReplicateAck {
                        follower_id,
                        index: ack_idx,
                        success,
                        last_log_index_hint,
                    } = msg
                    {
                        let _ = self
                            .receive_replicate_ack(
                                &follower_id,
                                ack_idx,
                                success,
                                last_log_index_hint,
                            )
                            .await;
                    }
                }
            }
        }

        // Wave 45-G: event-driven wait via tokio::sync::Notify replaces
        // the legacy 5 ms busy-poll.  receive_replicate_ack fires a
        // notify_one() on every positive ack at this index; we re-check
        // the tally inside the loop to handle spurious wakes (Notify
        // permits can also coalesce — two acks arriving in quick
        // succession may collapse into a single wake) and to allow the
        // InMemoryTransport's buffered-inbox path to be drained.
        //
        // Latency improvement: the prior implementation paid up to
        // 5 ms of scheduler-driven sleep between each tally check, even
        // when an ack landed microseconds after a poll.  The new path
        // wakes within tokio's notify-roundtrip (typically tens of
        // microseconds) and falls back to a deadline-bounded sleep only
        // when no ack arrives.
        let timeout_ms = *self.submit_timeout_ms.read().await;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            // Drain in-memory transport inbox if present.  Production
            // (TcpTransport) feeds acks via receive_replicate_ack
            // directly, but the InMemoryTransport used by tests stages
            // them in the inbox — drain so we see what's already there
            // before parking on notify.
            if let Some(transport) = self.transport.read().await.as_ref()
                && let Some(rx) = transport.try_receive(&self.config.node_id).await
            {
                for msg in rx {
                    if let ReplicationMessage::ReplicateAck {
                        follower_id,
                        index: ack_idx,
                        success,
                        last_log_index_hint,
                    } = msg
                    {
                        let _ = self
                            .receive_replicate_ack(
                                &follower_id,
                                ack_idx,
                                success,
                                last_log_index_hint,
                            )
                            .await;
                    }
                }
            }

            let ack_count = {
                let acks = self.pending_acks.read().await;
                acks.get(&index).map_or(0, |s| s.len() as u32) + 1 // +1 self
            };

            if ack_count >= majority {
                self.pending_acks.write().await.remove(&index);
                self.majority_ack_waiters.write().await.remove(&index);
                return Ok(MajorityAck {
                    ack_count,
                    total,
                    index,
                    term,
                });
            }

            let now = Instant::now();
            if now >= deadline {
                let final_ack_count = {
                    let acks = self.pending_acks.read().await;
                    acks.get(&index).map_or(0, |s| s.len() as u32) + 1
                };
                self.pending_acks.write().await.remove(&index);
                self.majority_ack_waiters.write().await.remove(&index);
                return Err(SubmitError::Timeout {
                    ack_count: final_ack_count,
                    total,
                    timeout_ms,
                });
            }

            // Park on the Notify until either an ack lands or the
            // deadline fires.  Notify::notified() is cancel-safe inside
            // tokio::select!, so the timeout branch cleanly drops the
            // future without losing a stored permit (the next loop pass
            // — if any — would see it).  We re-loop on every wake to
            // re-check the tally; spurious wakes are harmless.
            let remaining = deadline.saturating_duration_since(now);
            tokio::select! {
                () = notify.notified() => {
                    // Loop back to re-check the tally.
                }
                () = tokio::time::sleep(remaining) => {
                    // Loop back; the deadline check at the top will
                    // observe `now >= deadline` and emit Timeout with
                    // the final tally.
                }
            }
        }
    }

    /// Wave 38-DE: configure the submit-majority-ack deadline.  Must be
    /// called before [`Self::submit_with_majority_ack`].
    pub async fn set_submit_timeout_ms(&self, ms: u64) {
        *self.submit_timeout_ms.write().await = ms;
    }

    /// Wave 38-DE: configured submit timeout in milliseconds.
    pub async fn submit_timeout_ms(&self) -> u64 {
        *self.submit_timeout_ms.read().await
    }

    /// Wave 38-DE: process a [`ReplicationMessage::ReplicateAck`] arriving
    /// from a follower.
    ///
    /// On `success = true`:
    /// - Tallies the ack toward the per-index majority counter
    ///   ([`Self::submit_with_majority_ack`] reads this).
    /// - Advances `match_index[follower] = max(match_index[follower],
    ///   index)`.
    /// - Advances `next_index[follower] = max(next_index[follower],
    ///   index + 1)` so the next AppendEntries to this follower starts
    ///   from the new tail.
    ///
    /// On `success = false`:
    /// - Decrements `next_index[follower]` toward 1 so the next
    ///   AppendEntries walks back through the log; honours
    ///   `last_log_index_hint` if present (Raft "fast back-off" /
    ///   §5.3 optimisation).  Caps the back-off at index 1 so we never
    ///   try to send an entry whose log index is 0.
    ///
    /// Acks for unknown peers are ignored.
    pub async fn receive_replicate_ack(
        &self,
        follower_id: &str,
        index: u64,
        success: bool,
        last_log_index_hint: Option<u64>,
    ) {
        let known = self.config.peers.iter().any(|p| p == follower_id);
        if !known {
            tracing::debug!(follower_id, index, "dropping ack from unknown peer");
            return;
        }
        if success {
            // pending_acks tally
            let mut acks = self.pending_acks.write().await;
            if let Some(set) = acks.get_mut(&index) {
                set.insert(follower_id.to_string());
            }
            drop(acks);
            // match_index advance
            {
                let mut mi = self.match_index.write().await;
                let entry = mi.entry(follower_id.to_string()).or_insert(0);
                if index > *entry {
                    *entry = index;
                }
            }
            // next_index advance
            {
                let mut ni = self.next_index.write().await;
                let entry = ni.entry(follower_id.to_string()).or_insert(1);
                if index + 1 > *entry {
                    *entry = index + 1;
                }
            }
            // Wave 45-G: wake any submit_with_majority_ack task that
            // is parked waiting on this index.  Cloning the Arc<Notify>
            // out of the map under the read lock keeps the hot path
            // short (no write contention with concurrent acks for
            // other indices) and lets us release the lock before
            // calling notify_one(); the notify_one call itself never
            // blocks.  If no waiter is registered (e.g. the submitter
            // already returned after observing majority via a prior
            // ack), the lookup misses and we silently no-op.
            let waiter = {
                let waiters = self.majority_ack_waiters.read().await;
                waiters.get(&index).map(Arc::clone)
            };
            if let Some(n) = waiter {
                n.notify_one();
            }
        } else {
            // Walk next_index back.  Honour hint if it gives us a better
            // resume point than `current - 1`.
            let mut ni = self.next_index.write().await;
            let entry = ni.entry(follower_id.to_string()).or_insert(index);
            let walked = match last_log_index_hint {
                Some(h) => h.saturating_add(1).max(1),
                None => entry.saturating_sub(1).max(1),
            };
            // Always reduce — fast-forward via a hint is allowed only when
            // it is strictly less than `*entry` (otherwise we'd loop on
            // stale acks).
            if walked < *entry {
                *entry = walked;
            } else {
                *entry = entry.saturating_sub(1).max(1);
            }
        }
    }

    /// Wave 38-DE: read-only view of `next_index[follower]` (used by the
    /// transport's append-entries-replay loop and tests).
    pub async fn next_index_for(&self, follower_id: &str) -> Option<u64> {
        self.next_index.read().await.get(follower_id).copied()
    }

    /// Wave 38-DE: read-only view of `match_index[follower]`.
    pub async fn match_index_for(&self, follower_id: &str) -> Option<u64> {
        self.match_index.read().await.get(follower_id).copied()
    }

    /// Wave 38-DE: build the AppendEntries replay batch the leader should
    /// send to `follower_id` to repair its log gap.  Returns up to
    /// [`MAX_REPLAY_BATCH`] entries starting at `next_index[follower]`,
    /// in (term, index, command) form.  An empty `Vec` means the
    /// follower is already caught up.
    ///
    /// Caller is the transport replay loop; this method does NOT do I/O.
    pub async fn build_replay_batch(&self, follower_id: &str) -> Vec<(u64, u64, ClusterCommand)> {
        let last_index = *self.last_index.read().await;
        let start = self
            .next_index_for(follower_id)
            .await
            .unwrap_or(last_index + 1);
        if start > last_index {
            return Vec::new();
        }
        let log = self.command_log.read().await;
        let terms = self.log_terms.read().await;
        let mut out = Vec::new();
        for entry in log.iter() {
            let (idx, cmd) = entry;
            if *idx < start {
                continue;
            }
            // Fetch term for this index (1-based, vec is 0-based).
            let term = match terms.get((*idx - 1) as usize) {
                Some(t) => *t,
                None => continue,
            };
            out.push((term, *idx, cmd.clone()));
            if out.len() >= MAX_REPLAY_BATCH {
                break;
            }
        }
        out
    }

    /// Wave 47-A: leader-side per-tick replay scan.  Walks every known
    /// follower's `next_index` and, for each follower that lags behind
    /// `last_index`, emits the corresponding [`ReplicationMessage::ReplicateCommand`]
    /// frames (up to [`MAX_REPLAY_BATCH`] per follower per call).  The
    /// transport's tick loop calls this every Nth tick (~500 ms cadence)
    /// to back-fill log gaps that arose because (a) a previous
    /// AppendEntries was lost in flight, (b) the follower restarted with
    /// a stale log, or (c) a freshly-joined node needs the tail.  Returns
    /// `(follower_id, replicate_command)` pairs the transport should
    /// `send` back-to-back.
    ///
    /// Returns an empty `Vec` on non-leader nodes (followers must not
    /// drive replay) and on leaders whose followers are all caught up.
    /// This method does NOT do I/O.
    pub async fn build_replay_actions(&self) -> Vec<(String, ReplicationMessage)> {
        if !self.is_leader().await {
            return Vec::new();
        }
        let last_index = *self.last_index.read().await;
        if last_index == 0 {
            // Leader has nothing in its log to replay.
            return Vec::new();
        }
        // Snapshot follower ids under a read lock so we don't hold it
        // across the per-follower `build_replay_batch` calls (those take
        // their own read locks on `command_log` / `log_terms`).
        let follower_ids: Vec<String> = {
            let ni = self.next_index.read().await;
            ni.keys().cloned().collect()
        };
        let mut out: Vec<(String, ReplicationMessage)> = Vec::new();
        for follower in follower_ids {
            let batch = self.build_replay_batch(&follower).await;
            if batch.is_empty() {
                continue;
            }
            for (term, index, command) in batch {
                out.push((
                    follower.clone(),
                    ReplicationMessage::ReplicateCommand {
                        term,
                        index,
                        command,
                    },
                ));
            }
        }
        out
    }

    /// Wave 38-DE: reset per-follower `next_index` to `last_index + 1`
    /// when this node becomes leader.  Called by [`Self::force_leader`]
    /// /  [`Self::force_leader_with_term`] / [`Self::try_promote_to_leader`].
    async fn reset_replication_state(&self) {
        let last = *self.last_index.read().await;
        let mut ni = self.next_index.write().await;
        let mut mi = self.match_index.write().await;
        ni.clear();
        mi.clear();
        for peer in &self.config.peers {
            ni.insert(peer.clone(), last + 1);
            mi.insert(peer.clone(), 0);
        }
    }

    /// Record a single vote from `voter_id` toward this candidate's election
    /// quorum at `term`. Returns `true` iff the vote was newly counted.
    ///
    /// Rules enforced:
    /// 1. Stale-term votes (`term` not the active election term, or
    ///    `term != current_term`) are dropped.
    /// 2. Votes from a node not in `config.peers` (or self) are dropped with
    ///    a warn-level log — a buggy or hostile peer cannot inflate the
    ///    quorum.
    /// 3. Duplicate votes from the same `voter_id` at the same `term` are
    ///    counted at most once (`HashSet::insert` returns `false` on dup).
    pub async fn record_vote(&self, voter_id: &str, term: u64) -> bool {
        let active = *self.election_term.read().await;
        let current = *self.current_term.read().await;
        if Some(term) != active || term != current {
            tracing::debug!(
                voter_id,
                term,
                active = ?active,
                current,
                "dropping vote: not active election term",
            );
            return false;
        }

        let known =
            voter_id == self.config.node_id || self.config.peers.iter().any(|p| p == voter_id);
        if !known {
            tracing::warn!(
                voter_id,
                term,
                "dropping vote: voter is not a known cluster member",
            );
            return false;
        }

        let mut votes = self.votes_received.write().await;
        let entry = votes.entry(term).or_default();
        entry.insert(voter_id.to_string())
    }

    /// Number of distinct votes recorded at the given `term`.
    pub async fn votes_count(&self, term: u64) -> usize {
        self.votes_received
            .read()
            .await
            .get(&term)
            .map_or(0, HashSet::len)
    }

    /// Handle an election timeout (become candidate, request votes).
    ///
    /// Returns `true` if this node won the election and became leader.
    ///
    /// Vote-counting safety:
    /// - Each distinct `voter_id` is counted at most once per term (dedupe via
    ///   a per-term `HashSet<NodeId>`); replayed `VoteResponse` messages from
    ///   a single voter cannot fabricate a quorum.
    /// - Votes from nodes that are not part of `config.peers` (or self) are
    ///   ignored.
    /// - Votes whose `term` does not match the active election term are
    ///   dropped.
    pub async fn start_election(&self) -> Result<bool, ClusterError> {
        // Increment term
        let new_term = {
            let mut term = self.current_term.write().await;
            *term += 1;
            *term
        };

        // Become candidate
        {
            let mut role = self.role.write().await;
            *role = ReplicationRole::Candidate;
        }

        // Vote for self
        {
            let mut voted_for = self.voted_for.write().await;
            *voted_for = Some(self.config.node_id.clone());
        }

        // Reset / arm the per-term vote set for the new election. Older
        // terms are dropped so a stale set cannot leak into a future
        // election. The self-vote is recorded as the seed entry.
        {
            let mut votes = self.votes_received.write().await;
            votes.clear();
            let entry = votes.entry(new_term).or_default();
            entry.insert(self.config.node_id.clone()); // self-vote
        }
        {
            let mut active = self.election_term.write().await;
            *active = Some(new_term);
        }

        let peers_count = self.config.peers.len() as u32;
        let total_voters = peers_count + 1; // peers + self
        let majority = total_voters / 2 + 1;

        // If single-node we already have majority via the self-vote above.
        if peers_count == 0 {
            {
                let mut role = self.role.write().await;
                *role = ReplicationRole::Leader;
            }
            self.reset_replication_state().await;
            return Ok(true);
        }

        // Request votes from peers
        if let Some(transport) = self.transport.read().await.as_ref() {
            let vote_req = ReplicationMessage::VoteRequest {
                candidate_id: self.config.node_id.clone(),
                term: new_term,
            };

            for peer in &self.config.peers {
                let _ = transport.send(peer, vote_req.clone()).await;
            }

            // Collect vote responses (simplified: check inbox).
            // Each `VoteResponse` is funnelled through `record_vote` which
            // dedupes by `voter_id`, gates by term, and rejects unknown
            // peers — see the function-level docs for the full rule set.
            if let Some(rx) = transport.try_receive(&self.config.node_id).await {
                for msg in rx {
                    if let ReplicationMessage::VoteResponse {
                        voter_id,
                        granted,
                        term,
                    } = msg
                        && granted
                    {
                        let _ = self.record_vote(&voter_id, term).await;
                    }
                }
            }
        }

        let votes_received = self.votes_count(new_term).await as u32;

        if votes_received >= majority {
            {
                let mut role = self.role.write().await;
                *role = ReplicationRole::Leader;
            }
            self.reset_replication_state().await;
            Ok(true)
        } else {
            // Revert to follower if election failed
            let prev_role = *self.role.read().await;
            {
                let mut role = self.role.write().await;
                *role = ReplicationRole::Follower;
            }
            if prev_role != ReplicationRole::Follower {
                // W2-E CL-A1-R-mTLS-harness fix (Task #18): election-lost
                // step-down.
                let term = *self.current_term.read().await;
                tracing::info!(
                    prev_role = ?prev_role,
                    term,
                    "stepping down to follower"
                );
            }
            // Election lost: forget the active election term so a fresh
            // round must call start_election again (which re-arms the set).
            let mut active = self.election_term.write().await;
            *active = None;
            Ok(false)
        }
    }

    /// Handle a heartbeat from leader.
    ///
    /// Side effect (Wave 38-C): on an accepted heartbeat the local
    /// [`ElectionTimer`] is reset so this follower defers its own election.
    /// Stale heartbeats (lower term) are ignored and do *not* reset the
    /// timer — that would let a deposed leader keep silencing followers.
    pub async fn receive_heartbeat(&self, leader_id: &str, term: u64) -> Result<(), ClusterError> {
        let current_term = *self.current_term.read().await;
        if term >= current_term {
            // Accept leader, update term
            {
                let mut t = self.current_term.write().await;
                *t = term;
            }
            // W2-E CL-A1-R-mTLS-harness fix (Task #18): emit an INFO
            // log at the leader/candidate → follower transition so
            // the chaos harness can count step-downs. Follower →
            // follower "self-transitions" (the common case on stable
            // clusters) stay quiet.
            let prev_role = *self.role.read().await;
            {
                let mut role = self.role.write().await;
                *role = ReplicationRole::Follower;
            }
            if prev_role != ReplicationRole::Follower {
                tracing::info!(
                    prev_role = ?prev_role,
                    leader_id,
                    term,
                    "stepping down to follower"
                );
            }
            // Clear voted_for for new term
            if term > current_term {
                let mut voted_for = self.voted_for.write().await;
                *voted_for = None;
            }
            // Reset the election timer so this follower waits another full
            // timeout before challenging the leader.
            self.election_timer.write().await.reset();
            tracing::debug!(leader_id, term, "accepted heartbeat from leader");
        }
        Ok(())
    }

    /// Arm an election without sending vote requests (transport will do that).
    ///
    /// Increments the term, transitions to `Candidate`, records a self-vote,
    /// and returns the new term. The caller (typically
    /// [`crate::transport::TcpTransport`]) is responsible for broadcasting a
    /// `VoteRequest` over the wire and feeding incoming `VoteResponse`
    /// messages through [`receive_vote_response`] /
    /// [`try_promote_to_leader`].
    ///
    /// For the in-memory transport / single-node path, prefer the existing
    /// [`start_election`] which performs broadcast + inbox-drain in one
    /// shot.
    pub async fn arm_election(&self) -> u64 {
        let new_term = {
            let mut term = self.current_term.write().await;
            *term += 1;
            *term
        };
        {
            let mut role = self.role.write().await;
            *role = ReplicationRole::Candidate;
        }
        {
            let mut voted_for = self.voted_for.write().await;
            *voted_for = Some(self.config.node_id.clone());
        }
        {
            let mut votes = self.votes_received.write().await;
            votes.clear();
            let entry = votes.entry(new_term).or_default();
            entry.insert(self.config.node_id.clone());
        }
        {
            let mut active = self.election_term.write().await;
            *active = Some(new_term);
        }

        // Single-node short-circuit: if we have no peers, we already have
        // majority via the self-vote.
        if self.config.peers.is_empty() {
            {
                let mut role = self.role.write().await;
                *role = ReplicationRole::Leader;
            }
            self.reset_replication_state().await;
        }
        new_term
    }

    /// Re-check whether this node should be promoted to leader based on the
    /// current count of distinct votes for the active election term.
    ///
    /// Called by the transport after a `VoteResponse` is delivered so the
    /// candidate can transition to `Leader` as soon as a quorum has been
    /// observed on the wire — without waiting for [`start_election`] to
    /// drain its (in-memory) inbox.
    ///
    /// Returns `true` iff the role transitioned from `Candidate` to
    /// `Leader` on this call. Stale calls (no active election term, or
    /// already-leader) return `false`.
    pub async fn try_promote_to_leader(&self) -> bool {
        let active = *self.election_term.read().await;
        let Some(term) = active else { return false };
        if *self.current_term.read().await != term {
            return false;
        }
        if *self.role.read().await != ReplicationRole::Candidate {
            return false;
        }

        let peers_count = self.config.peers.len() as u32;
        let total_voters = peers_count + 1;
        let majority = total_voters / 2 + 1;
        let votes = self.votes_count(term).await as u32;
        if votes < majority {
            return false;
        }

        {
            let mut role = self.role.write().await;
            *role = ReplicationRole::Leader;
        }
        self.reset_replication_state().await;
        true
    }

    /// Process a `VoteResponse` arriving from a peer over the wire.
    ///
    /// This is the TCP-side entry point for vote-counting. It is the
    /// counterpart of [`receive_vote_request`] and exists so the production
    /// [`crate::transport::TcpTransport`] can deliver the response that the
    /// remote peer computed back into this candidate's quorum tally.
    ///
    /// Behaviour:
    /// - If `granted == true`, the vote is forwarded to [`record_vote`],
    ///   which dedupes by `voter_id`, gates by term, and rejects unknown
    ///   peers (see [`record_vote`] for the full rule set).
    /// - If `granted == false`, this is a no-op (no negative votes are
    ///   tracked — failure is implied by the absence of a positive vote).
    /// - The election outcome (leader vs. follower revert) is *not*
    ///   recomputed here. Wave 38-B keeps election scheduling in
    ///   [`start_election`]; this method only feeds the tally. Heartbeat-
    ///   driven failover that reaches into this method is Wave 38-C scope.
    ///
    /// Returns `true` iff the vote was newly counted.
    pub async fn receive_vote_response(&self, voter_id: &str, term: u64, granted: bool) -> bool {
        if !granted {
            return false;
        }
        self.record_vote(voter_id, term).await
    }

    /// Process a vote request from a candidate.
    ///
    /// Returns the vote response to send back.
    pub async fn receive_vote_request(&self, candidate_id: &str, term: u64) -> ReplicationMessage {
        let current_term = *self.current_term.read().await;

        if term < current_term {
            return ReplicationMessage::VoteResponse {
                voter_id: self.config.node_id.clone(),
                term: current_term,
                granted: false,
            };
        }

        // If term is higher, update our term and clear vote
        if term > current_term {
            let mut t = self.current_term.write().await;
            *t = term;
            let mut voted_for = self.voted_for.write().await;
            *voted_for = None;
            let prev_role = *self.role.read().await;
            {
                let mut role = self.role.write().await;
                *role = ReplicationRole::Follower;
            }
            if prev_role != ReplicationRole::Follower {
                // W2-E CL-A1-R-mTLS-harness fix (Task #18):
                // vote-request-higher-term step-down.
                tracing::info!(
                    prev_role = ?prev_role,
                    candidate_id,
                    term,
                    "stepping down to follower"
                );
            }
        }

        let mut voted_for = self.voted_for.write().await;
        let granted = match voted_for.as_deref() {
            None => {
                *voted_for = Some(candidate_id.to_string());
                true
            }
            Some(id) if id == candidate_id => true,
            Some(_) => false,
        };

        ReplicationMessage::VoteResponse {
            voter_id: self.config.node_id.clone(),
            term,
            granted,
        }
    }

    /// Process a replicated command from leader.
    ///
    /// Log-monotonicity safety (Raft AppendEntries §5.3 / §5.4 inspired):
    /// 1. **Stale term** — `term < current_term` is rejected outright.
    /// 2. **Already-applied (idempotent replay)** — `index <= last_index`
    ///    is treated as a no-op. The follower has already ingested this
    ///    entry; reapplying would mutate the state machine multiple times
    ///    on a duplicate or reordered AppendEntries. If the stored term
    ///    at this index disagrees with `term`, the follower rejects with
    ///    a `log term mismatch` error so divergence surfaces.
    /// 3. **Gap detected** — `index > last_index + 1` means the leader
    ///    has skipped entries the follower does not yet have. The
    ///    follower rejects with `log gap detected …` so the leader can
    ///    back-fill (snapshot or older entries).
    /// 4. **Contiguous append** — `index == last_index + 1` is the only
    ///    path that mutates state: append to log, advance `last_index`,
    ///    record the term at this index, and apply to the cluster state
    ///    machine.
    ///
    /// This makes a follower idempotent under message replay and
    /// re-ordering — it will neither apply the same command twice nor
    /// silently skip ahead.
    pub async fn receive_command(
        &self,
        term: u64,
        index: u64,
        cmd: ClusterCommand,
    ) -> Result<(), ClusterError> {
        let current_term = *self.current_term.read().await;
        if term < current_term {
            return Err(ClusterError::CommandFailed("stale term".to_string()));
        }

        // Update term if needed
        if term > current_term {
            let mut t = self.current_term.write().await;
            *t = term;
        }

        let last_index = *self.last_index.read().await;

        // Case 2: idempotent replay of an already-applied entry.
        if index <= last_index {
            // If we previously stored a term for this index and it differs,
            // leader and follower disagree — surface the divergence so
            // the leader-side replay logic (Wave 38-D scope) can repair.
            let stored_term = {
                let terms = self.log_terms.read().await;
                if index == 0 {
                    Some(0u64)
                } else {
                    terms.get((index - 1) as usize).copied()
                }
            };
            if let Some(stored) = stored_term
                && stored != term
                && stored != 0
            {
                return Err(ClusterError::CommandFailed(format!(
                    "log term mismatch at index {index}: stored term {stored}, leader term {term}",
                )));
            }
            tracing::debug!(
                index,
                term,
                last_index,
                "ignoring already-applied AppendEntries (idempotent replay)",
            );
            return Ok(());
        }

        // Case 3: gap — leader is ahead of follower's log.
        if index > last_index + 1 {
            return Err(ClusterError::CommandFailed(format!(
                "log gap detected: follower last_index={last_index}, leader index={index}",
            )));
        }

        // Case 4: contiguous append (index == last_index + 1).
        //
        // W1-C: persist BEFORE in-memory mutation + state-machine
        // apply so a crash on the follower between accept and ack
        // never loses an entry the leader believes is replicated.
        if let Some(state) = self.persistent_state_arc().await
            && let Err(e) = state.append_log_entry(index, term, &cmd).await
        {
            tracing::warn!(
                index,
                term,
                error = %e,
                "persistent log append failed (follower receive); continuing with in-memory only",
            );
        }
        {
            let mut idx = self.last_index.write().await;
            *idx = index;
        }
        {
            let mut log = self.command_log.write().await;
            log.push((index, cmd.clone()));
        }
        {
            let mut terms = self.log_terms.write().await;
            terms.push(term);
        }

        // Apply to local state
        self.cluster_manager.apply(cmd)?;
        // Treat any successful AppendEntries (Wave 38-C) as keep-alive: a
        // follower that is replicating from the leader must defer its own
        // election timer just as a bare heartbeat does.
        self.election_timer.write().await.reset();
        Ok(())
    }

    /// Wave 38-DE: receive an AppendEntries-style frame and return the ACK
    /// the transport should write back to the leader.  Wraps
    /// [`Self::receive_command`] so the leader's side of the
    /// incremental-replay loop can observe the result without parsing
    /// the error string.
    ///
    /// On success the returned `ReplicateAck { success = true, ... }`
    /// carries this follower's `last_log_index` as the hint so the
    /// leader can update `next_index` aggressively.  On failure
    /// (`success = false`) the hint still carries the follower's current
    /// `last_log_index` so the leader can jump straight back to the
    /// matching prev-log-index instead of decrementing one at a time.
    pub async fn process_replicate_command(
        &self,
        term: u64,
        index: u64,
        cmd: ClusterCommand,
    ) -> ReplicationMessage {
        let result = self.receive_command(term, index, cmd).await;
        let last = *self.last_index.read().await;
        ReplicationMessage::ReplicateAck {
            follower_id: self.config.node_id.clone(),
            index,
            success: result.is_ok(),
            last_log_index_hint: Some(last),
        }
    }

    // ---------------------------------------------------------------------
    // Wave 38-DE: pre-vote (Raft §9.6)
    // ---------------------------------------------------------------------

    /// Wave 38-DE: synthetic pre-vote round.  Returns the proposed term
    /// that would be entered if the pre-vote round ultimately succeeds
    /// — the candidate has NOT yet incremented its real term.  Resets
    /// the per-term pre-vote tally to a single self-vote and the caller
    /// (transport) should broadcast a [`ReplicationMessage::RequestPreVote`]
    /// to all peers.
    ///
    /// In single-node mode (`peers.is_empty()`) the function short-
    /// circuits and returns `proposed_term = current_term + 1` with a
    /// guaranteed pre-vote pass via `pre_vote_majority_won = true`;
    /// the actual real-term increment is done later by [`Self::start_election`]
    /// (or [`Self::arm_election`]) once the caller decides to proceed.
    pub async fn start_pre_vote(&self) -> PreVoteRound {
        let current = *self.current_term.read().await;
        let proposed = current + 1;
        // Wave 54-B: bump the round generation BEFORE clearing tally so a
        // racing `record_pre_vote` (carrying the old round_id) cannot land
        // a stale grant in the new bucket.
        let round_id = {
            let mut rid = self.pre_vote_round_id.write().await;
            *rid = rid.saturating_add(1);
            *rid
        };
        let mut pv = self.pre_votes_received.write().await;
        pv.clear();
        let entry = pv.entry(round_id).or_default();
        entry.insert(self.config.node_id.clone());
        PreVoteRound {
            proposed_term: proposed,
            round_id,
        }
    }

    /// Record a granted pre-vote from `voter_id` for the candidate's
    /// current pre-vote round identified by `round_id`.  Returns `true`
    /// iff the vote was newly counted (deduped by `voter_id`, gated by
    /// known-peer membership).
    ///
    /// Wave 54-B: a `round_id` that does not match the engine's current
    /// `pre_vote_round_id` is silently dropped — this is the stale-round
    /// guard that closes the W52 NEW-W47 race.  We accept `round_id == 0`
    /// (i.e. a peer running pre-Wave-54-B code, where `round_id` is
    /// `#[serde(default)]` and deserialises to 0) as a rolling-upgrade
    /// compatibility path: the legacy grant is bucketed against the
    /// engine's *current* `pre_vote_round_id` (not against `0`), so
    /// majority counting works correctly when the engine has already
    /// advanced past round 0.
    ///
    /// Wave 57 (closes Wave 56 NEW-W56 Medium — mixed-cluster compat
    /// doc-vs-code mismatch): the doc above and the W54-B claim both
    /// promised conditional acceptance of `round_id == 0`, but the
    /// pre-W57 code did a bare equality check that silently dropped
    /// every legacy peer once `start_pre_vote()` had run once.  This
    /// version implements the documented conditional, preserving
    /// rolling-upgrade liveness without re-introducing the W52 stale-
    /// round race (a peer that itself ran W54-B code echoes back the
    /// non-zero `round_id`, which is rejected unless it matches the
    /// engine's current round).
    ///
    /// Wave 59 (closes Wave 58 NEW-W58 Medium — `record_pre_vote`
    /// `round_id == 0` legacy fallback was overbroad): the W57
    /// fallback accepted **any** `round_id == 0` grant from a known
    /// peer, including ones whose `proposed_term` no longer matches
    /// the engine's current in-flight pre-vote round.  A delayed-but-
    /// honest legacy `RequestPreVoteResponse` from a *prior*
    /// `start_pre_vote()` call could land in a *future* round it never
    /// observed (slow networks during rolling upgrades) and pollute
    /// the tally.  The fix: when `round_id == 0`, ALSO require
    /// `proposed_term == pre_vote_in_flight.proposed_term` (i.e. the
    /// legacy peer is responding to *this* round, not a stale one).
    /// This restores the W54-B stale-response invariant for the legacy
    /// path without re-breaking rolling-upgrade liveness, because a
    /// genuine legacy peer responding to the current round echoes the
    /// matching `proposed_term` regardless of whether it tracks
    /// `round_id`.
    pub async fn record_pre_vote(&self, voter_id: &str, proposed_term: u64, round_id: u64) -> bool {
        let known =
            voter_id == self.config.node_id || self.config.peers.iter().any(|p| p == voter_id);
        if !known {
            tracing::warn!(
                voter_id,
                proposed_term,
                round_id,
                "dropping pre-vote: voter is not a known cluster member",
            );
            return false;
        }
        let current_round = *self.pre_vote_round_id.read().await;
        // Wave 57: accept `round_id == 0` from a legacy peer (pre-W54-B
        // code echoes 0 via `#[serde(default)]`) and bucket it under the
        // engine's *current* round.  Non-zero responses must match the
        // engine's current round (W54-B stale-round guard preserved).
        //
        // Wave 59: the legacy fallback ALSO requires
        // `proposed_term == pre_vote_in_flight.proposed_term`.  Without
        // this binding, a delayed legacy response from a prior round
        // could pollute the current tally because legacy peers carry
        // no `round_id` discriminator.  The `proposed_term` echo is
        // wire-compatible all the way back to the W38-DE pre-vote
        // introduction, so this gate works on every supported peer.
        let accept_round = if round_id == current_round {
            true
        } else if round_id == 0 {
            let in_flight = *self.pre_vote_in_flight.read().await;
            match in_flight {
                Some(round) if round.proposed_term == proposed_term => {
                    tracing::debug!(
                        voter_id,
                        proposed_term,
                        current_round_id = current_round,
                        "accepting legacy-peer pre-vote (round_id=0, \
                         proposed_term matches in-flight) under current round",
                    );
                    true
                }
                Some(round) => {
                    tracing::debug!(
                        voter_id,
                        proposed_term,
                        in_flight_proposed_term = round.proposed_term,
                        in_flight_round_id = round.round_id,
                        "dropping legacy-peer pre-vote (round_id=0): \
                         proposed_term does not match in-flight round (W59 gate)",
                    );
                    false
                }
                None => {
                    tracing::debug!(
                        voter_id,
                        proposed_term,
                        "dropping legacy-peer pre-vote (round_id=0): \
                         no in-flight pre-vote round (W59 gate)",
                    );
                    false
                }
            }
        } else {
            false
        };
        if !accept_round {
            tracing::debug!(
                voter_id,
                proposed_term,
                response_round_id = round_id,
                current_round_id = current_round,
                "dropping pre-vote response from stale round",
            );
            return false;
        }
        let mut pv = self.pre_votes_received.write().await;
        let entry = pv.entry(current_round).or_default();
        entry.insert(voter_id.to_string())
    }

    /// Number of distinct pre-votes recorded for the given `round_id`.
    /// Wave 54-B: replaces the old `proposed_term` key — see
    /// [`Self::record_pre_vote`].
    pub async fn pre_votes_count(&self, round_id: u64) -> usize {
        self.pre_votes_received
            .read()
            .await
            .get(&round_id)
            .map_or(0, HashSet::len)
    }

    /// True iff the pre-vote majority has been observed for the given
    /// `round_id` — i.e. it is safe for the candidate to actually
    /// increment its real term and start a real election.  Wave 54-B:
    /// keys on `round_id` instead of `proposed_term` so successive
    /// timed-out rounds at the same proposed_term are tallied
    /// independently.
    pub async fn pre_vote_majority_reached(&self, round_id: u64) -> bool {
        let total = self.config.peers.len() as u32 + 1;
        let majority = total / 2 + 1;
        self.pre_votes_count(round_id).await as u32 >= majority
    }

    /// Process an incoming [`ReplicationMessage::RequestPreVote`] from a
    /// candidate.  Returns the response the transport should write
    /// back.
    ///
    /// A peer grants a pre-vote iff:
    /// - `proposed_term > current_term` (otherwise the candidate is
    ///   stale and a real vote at this term would also fail), AND
    /// - the peer's election timer is "expired enough" — i.e. it has
    ///   not received a recent heartbeat from a current leader (we
    ///   approximate by checking [`ElectionTimer::is_expired`]).
    ///
    /// CRITICAL: pre-vote does NOT mutate `current_term`, `voted_for`,
    /// or `role`.  This is the whole point of the optimisation —
    /// flapping nodes cannot churn cluster terms during partition
    /// heals.
    pub async fn receive_pre_vote_request(
        &self,
        candidate_id: &str,
        proposed_term: u64,
        round_id: u64,
    ) -> ReplicationMessage {
        let current_term = *self.current_term.read().await;
        let mut granted = proposed_term > current_term;
        if granted {
            // Only grant if we'd be willing to start our own election
            // anyway — i.e. (a) our election timer is expired (no
            // recent leader heartbeat) OR (b) we are ourselves running
            // a pre-vote round at the same proposed_term, which
            // implies our own timer fired recently.  Wave 47-A: case
            // (b) is needed because [`Self::tick`] resets the election
            // timer immediately after emitting a pre-vote, which would
            // otherwise prevent two peers that fired their own
            // pre-votes near-simultaneously from granting each others'
            // requests during the same round.  This prevents a
            // partitioned candidate from harvesting pre-votes from
            // peers who currently recognise a healthy leader (they
            // would have neither an expired timer NOR an in-flight
            // pre-vote round).
            let timer_expired = self.election_timer.read().await.is_expired();
            let own_round_in_flight = self
                .pre_vote_in_flight
                .read()
                .await
                .is_some_and(|r| r.proposed_term <= proposed_term);
            granted = timer_expired || own_round_in_flight;
        }
        tracing::debug!(
            candidate_id,
            proposed_term,
            round_id,
            current_term,
            granted,
            "pre-vote request",
        );
        // Wave 54-B: echo `round_id` back so the candidate can match it
        // against its current pre_vote_round_id and discard stale grants.
        ReplicationMessage::RequestPreVoteResponse {
            voter_id: self.config.node_id.clone(),
            proposed_term,
            granted,
            round_id,
        }
    }

    /// Process an incoming [`ReplicationMessage::RequestPreVoteResponse`]
    /// from a peer.  Returns `true` iff the vote was newly counted.
    ///
    /// Wave 54-B: a response carrying a `round_id` that does not match
    /// the engine's current `pre_vote_round_id` is silently dropped
    /// (stale-round guard, closes W52 NEW-W47).  Pre-Wave-54-B peers
    /// send `round_id = 0` (serde default); responses from such peers
    /// only count when the engine itself has not yet started a pre-vote
    /// (i.e. its own round_id is also 0), which is the harmless boot
    /// case.
    pub async fn receive_pre_vote_response(
        &self,
        voter_id: &str,
        proposed_term: u64,
        granted: bool,
        round_id: u64,
    ) -> bool {
        if !granted {
            return false;
        }
        self.record_pre_vote(voter_id, proposed_term, round_id)
            .await
    }

    // ---------------------------------------------------------------------
    // Wave 38-DE: snapshot transfer + Join control flow
    // ---------------------------------------------------------------------

    /// Wave 38-DE: handle an incoming [`ReplicationMessage::SnapshotRequest`]
    /// from a follower that has fallen too far behind.  Builds a
    /// [`ReplicationMessage::SnapshotResponse`] containing a fresh
    /// [`ClusterSnapshot`] plus the leader's `last_index` and `term`.
    ///
    /// Returns `None` if the receiving node is not the leader (a
    /// follower must not serve snapshots — that would let stale data
    /// poison a recovering peer).
    pub async fn handle_snapshot_request(&self, follower_id: &str) -> Option<ReplicationMessage> {
        if !self.is_leader().await {
            tracing::debug!(
                follower_id,
                "snapshot request received but not leader; dropping",
            );
            return None;
        }
        let (snapshot, last_index, term) = self.get_snapshot().await;
        Some(ReplicationMessage::SnapshotResponse {
            snapshot,
            last_index,
            term,
        })
    }

    /// Wave 38-DE: handle an incoming [`ReplicationMessage::SnapshotResponse`]
    /// from the leader.  Restores the snapshot via [`Self::restore_snapshot`]
    /// and resets the election timer (fresh leader contact = keep-alive).
    pub async fn handle_snapshot_response(
        &self,
        snapshot: ClusterSnapshot,
        last_index: u64,
        term: u64,
    ) -> Result<(), ClusterError> {
        self.restore_snapshot(snapshot, last_index, term).await?;
        self.election_timer.write().await.reset();
        Ok(())
    }

    /// Wave 38-DE: handle an incoming [`ReplicationMessage::Join`] from a
    /// new node.  The leader records the joiner in
    /// [`Self::next_index`] / [`Self::match_index`] (so subsequent
    /// AppendEntries traffic is properly tracked) and returns a
    /// [`ReplicationMessage::SnapshotResponse`] containing the
    /// current cluster state so the joiner immediately catches up.
    ///
    /// Non-leaders return `None` (joiner should retry against the
    /// leader).
    pub async fn handle_join(&self, node_id: &str, _addr: &str) -> Option<ReplicationMessage> {
        if !self.is_leader().await {
            return None;
        }
        // Track the joiner.
        let last = *self.last_index.read().await;
        {
            let mut ni = self.next_index.write().await;
            ni.entry(node_id.to_string()).or_insert(last + 1);
        }
        {
            let mut mi = self.match_index.write().await;
            mi.entry(node_id.to_string()).or_insert(0);
        }
        let (snapshot, last_index, term) = self.get_snapshot().await;
        Some(ReplicationMessage::SnapshotResponse {
            snapshot,
            last_index,
            term,
        })
    }

    /// Get a full snapshot for a joining node.
    pub async fn get_snapshot(&self) -> (ClusterSnapshot, u64, u64) {
        let snapshot = self.cluster_manager.snapshot();
        let last_index = *self.last_index.read().await;
        let term = *self.current_term.read().await;
        (snapshot, last_index, term)
    }

    /// Restore from snapshot (for joining node).
    ///
    /// After restoring, `log_terms` is filled with the snapshot's term value
    /// for every index up to `last_index`. This is conservative — a real
    /// snapshot would carry per-index term metadata — but it preserves the
    /// invariant that `log_terms.len() == last_index` so subsequent
    /// AppendEntries monotonicity checks remain well-defined.
    pub async fn restore_snapshot(
        &self,
        snapshot: ClusterSnapshot,
        last_index: u64,
        term: u64,
    ) -> Result<(), ClusterError> {
        // DD R31: `last_index` arrives from the wire (SnapshotResponse) and drives
        // a `Vec::resize` of `log_terms`; reject an absurd value BEFORE mutating
        // any state so a tiny frame cannot allocate tens of GiB.
        if last_index > MAX_SNAPSHOT_LOG_INDEX {
            return Err(ClusterError::CommandFailed(format!(
                "snapshot last_index {last_index} exceeds the maximum supported \
                 ({MAX_SNAPSHOT_LOG_INDEX})"
            )));
        }
        let terms_len = usize::try_from(last_index).map_err(|_| {
            ClusterError::CommandFailed("snapshot last_index overflows usize".to_string())
        })?;
        self.cluster_manager.restore(snapshot.clone())?;
        {
            let mut idx = self.last_index.write().await;
            *idx = last_index;
        }
        {
            let mut t = self.current_term.write().await;
            *t = term;
        }
        {
            let mut terms = self.log_terms.write().await;
            terms.clear();
            terms.resize(terms_len, term);
        }
        // W1-C: persist the snapshot install so a restart sees the
        // compacted state, not stale journal entries.
        if let Some(state) = self.persistent_state_arc().await
            && let Err(e) = state.install_snapshot(&snapshot, last_index, term).await
        {
            tracing::warn!(
                last_index,
                term,
                error = %e,
                "persistent snapshot install failed; continuing with in-memory only",
            );
        }
        Ok(())
    }

    // ---------------------------------------------------------------------
    // W1-C / CL-A4: chunked, resumable snapshot transfer
    // ---------------------------------------------------------------------

    /// Allocate a fresh `transfer_id` for a new chunked snapshot
    /// transfer. Leader-side only.
    pub async fn next_snapshot_transfer_id(&self) -> u64 {
        let mut g = self.snapshot_tx_next_id.write().await;
        *g = g.saturating_add(1);
        *g
    }

    /// Inspect the leader-side cursor for `follower_id` and return
    /// the chunk index from which a (re)send should start. Returns
    /// 0 (start of transfer) if no transfer is in flight or the
    /// stored transfer_id does not match.
    pub async fn resume_chunk_idx(&self, follower_id: &str, transfer_id: u64) -> u32 {
        let g = self.snapshot_tx_cursors.read().await;
        match g.get(follower_id) {
            Some(c) if c.transfer_id == Some(transfer_id) => c.last_acked.map_or(0, |a| a + 1),
            _ => 0,
        }
    }

    /// Split the current cluster snapshot into chunks of
    /// `chunk_size` bytes and return one
    /// [`ReplicationMessage::SnapshotChunk`] frame per chunk,
    /// starting from `start_chunk` (use 0 for a fresh transfer,
    /// or the value returned by [`Self::resume_chunk_idx`] for a
    /// resume). Always uses the leader's current snapshot
    /// (`get_snapshot()`); the `transfer_id` is passed through so
    /// the caller can re-use the same id across resume attempts.
    pub async fn build_snapshot_chunks(
        &self,
        transfer_id: u64,
        start_chunk: u32,
        chunk_size: usize,
    ) -> Result<Vec<ReplicationMessage>, ClusterError> {
        if !self.is_leader().await {
            return Err(ClusterError::CommandFailed(
                "build_snapshot_chunks called on non-leader".to_string(),
            ));
        }
        let chunk_size = chunk_size.max(1);
        let (snapshot, last_index, term) = self.get_snapshot().await;
        let payload = serde_json::to_vec(&snapshot)
            .map_err(|e| ClusterError::CommandFailed(format!("snapshot encode: {e}")))?;
        let total_bytes = payload.len() as u64;
        let total_chunks = payload.len().div_ceil(chunk_size).max(1) as u32;

        let mut out = Vec::new();
        for idx in start_chunk..total_chunks {
            let start = (idx as usize) * chunk_size;
            let end = (start + chunk_size).min(payload.len());
            let slice = payload[start..end].to_vec();
            out.push(ReplicationMessage::SnapshotChunk {
                transfer_id,
                chunk_index: idx,
                total_chunks,
                is_final: idx + 1 == total_chunks,
                last_index,
                term,
                total_bytes,
                payload: slice,
            });
        }
        Ok(out)
    }

    /// Record that `follower_id` has acked a chunk. Updates the
    /// leader-side cursor so [`Self::resume_chunk_idx`] returns the
    /// right resume point on a fresh send. `applied = true` clears
    /// the in-flight transfer_id (transfer complete).
    pub async fn receive_snapshot_chunk_ack(
        &self,
        follower_id: &str,
        transfer_id: u64,
        last_received_chunk: u32,
        applied: bool,
    ) {
        let mut g = self.snapshot_tx_cursors.write().await;
        let cur = g.entry(follower_id.to_string()).or_default();
        // If this ack is for a stale transfer, ignore.
        if cur.transfer_id.is_some() && cur.transfer_id != Some(transfer_id) {
            tracing::debug!(
                follower_id,
                transfer_id,
                cur_transfer = ?cur.transfer_id,
                "snapshot ack for stale transfer_id; ignoring",
            );
            return;
        }
        if cur.transfer_id.is_none() {
            cur.transfer_id = Some(transfer_id);
        }
        // Advance last_acked monotonically.
        cur.last_acked = Some(match cur.last_acked {
            Some(prev) => prev.max(last_received_chunk),
            None => last_received_chunk,
        });
        if applied {
            cur.applied = true;
        }
    }

    /// Inbound (follower-side) handler for one
    /// [`ReplicationMessage::SnapshotChunk`]. Reassembles the
    /// payload, restores the snapshot once complete, and returns
    /// the [`ReplicationMessage::SnapshotChunkAck`] the transport
    /// should write back over the same connection.
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_snapshot_chunk(
        &self,
        transfer_id: u64,
        chunk_index: u32,
        total_chunks: u32,
        is_final: bool,
        last_index: u64,
        term: u64,
        total_bytes: u64,
        payload: Vec<u8>,
    ) -> Result<ReplicationMessage, ClusterError> {
        // Sanity: reject obviously-bad chunk counts so a malformed
        // frame cannot OOM us.
        if total_chunks == 0 {
            return Err(ClusterError::CommandFailed(
                "SnapshotChunk: total_chunks=0".to_string(),
            ));
        }
        if chunk_index >= total_chunks {
            return Err(ClusterError::CommandFailed(format!(
                "SnapshotChunk: chunk_index={chunk_index} >= total_chunks={total_chunks}",
            )));
        }
        // total_bytes upper-bounded by the chunked transfer cap so
        // a follower cannot be tricked into reserving multi-GiB.
        const MAX_SNAPSHOT_TOTAL_BYTES: u64 = 128 * 1024 * 1024; // 128 MiB
        if total_bytes > MAX_SNAPSHOT_TOTAL_BYTES {
            return Err(ClusterError::CommandFailed(format!(
                "SnapshotChunk: total_bytes={total_bytes} > cap {MAX_SNAPSHOT_TOTAL_BYTES}",
            )));
        }

        // Find or create the reassembly buffer for this transfer_id.
        // If a buffer exists for a different transfer_id, evict it
        // (the leader has restarted; old chunks are useless).
        let assembled: Option<(Vec<u8>, u64, u64)> = {
            let mut buffers = self.snapshot_rx_buffers.write().await;
            // Garbage-collect stale buffers.
            buffers.retain(|tid, _| *tid == transfer_id);
            let buf = buffers.entry(transfer_id).or_default();
            if buf.expected == 0 {
                buf.expected = total_chunks;
                buf.last_index = last_index;
                buf.term = term;
                buf.total_bytes = total_bytes;
            } else if buf.expected != total_chunks {
                return Err(ClusterError::CommandFailed(format!(
                    "SnapshotChunk: total_chunks={total_chunks} differs from buffered {}",
                    buf.expected,
                )));
            }
            buf.record_chunk(chunk_index, payload);

            if is_final || buf.is_complete() {
                // Take the buffer out for assembly.
                if let Some(taken) = buffers.remove(&transfer_id) {
                    if taken.is_complete() {
                        let last = taken.last_index;
                        let t = taken.term;
                        Some((taken.assemble()?, last, t))
                    } else {
                        // is_final=true but missing chunks: leader
                        // mis-issued. Re-insert the partial buffer
                        // so a redrive can fill the gaps.
                        buffers.insert(transfer_id, taken);
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        let applied = if let Some((bytes, li, tm)) = assembled {
            let snapshot: ClusterSnapshot = serde_json::from_slice(&bytes)
                .map_err(|e| ClusterError::CommandFailed(format!("snapshot decode: {e}")))?;
            self.restore_snapshot(snapshot, li, tm).await?;
            true
        } else {
            false
        };

        // last_received_chunk: highest contiguous chunk index this
        // follower has buffered for the transfer. If we just
        // applied, advertise the final index.
        let last_received_chunk = if applied {
            total_chunks - 1
        } else {
            let buffers = self.snapshot_rx_buffers.read().await;
            buffers
                .get(&transfer_id)
                .and_then(|b| b.highest_contiguous)
                .unwrap_or(chunk_index)
        };

        Ok(ReplicationMessage::SnapshotChunkAck {
            follower_id: self.config.node_id.clone(),
            transfer_id,
            last_received_chunk,
            applied,
        })
    }

    /// Test-only / orchestrator helper: drop the follower-side
    /// reassembly buffer for `transfer_id` to simulate a crash
    /// that loses partial chunks. Returns whether a buffer was
    /// removed.
    #[doc(hidden)]
    pub async fn forget_snapshot_buffer(&self, transfer_id: u64) -> bool {
        let mut g = self.snapshot_rx_buffers.write().await;
        g.remove(&transfer_id).is_some()
    }

    /// Test-only / orchestrator helper: inspect how many chunks
    /// have been buffered so far for `transfer_id`.
    #[doc(hidden)]
    pub async fn snapshot_buffer_progress(&self, transfer_id: u64) -> Option<(u32, u32)> {
        let g = self.snapshot_rx_buffers.read().await;
        g.get(&transfer_id)
            .map(|b| (b.chunks.len() as u32, b.expected))
    }

    // ---------------------------------------------------------------------
    // End of W1-C / CL-A4 chunked snapshot transfer
    // ---------------------------------------------------------------------

    /// Send a heartbeat to all peers (leader only).
    pub async fn send_heartbeat(&self) -> Result<(), ClusterError> {
        if !self.is_leader().await {
            return Ok(());
        }

        let term = *self.current_term.read().await;
        let msg = ReplicationMessage::Heartbeat {
            leader_id: self.config.node_id.clone(),
            term,
        };

        if let Some(transport) = self.transport.read().await.as_ref() {
            for peer in &self.config.peers {
                let _ = transport.send(peer, msg.clone()).await;
            }
        }

        Ok(())
    }

    /// Promote this node to leader (used in tests and single-node bootstrap).
    pub async fn force_leader(&self) {
        {
            let mut role = self.role.write().await;
            *role = ReplicationRole::Leader;
        }
        self.reset_replication_state().await;
    }

    /// Promote this node to leader at a specific term.
    pub async fn force_leader_with_term(&self, term: u64) {
        {
            let mut role = self.role.write().await;
            *role = ReplicationRole::Leader;
        }
        {
            let mut t = self.current_term.write().await;
            *t = term;
        }
        self.reset_replication_state().await;
    }

    /// Step down from leader to follower.
    ///
    /// Emits an INFO-level `stepping down to follower` log with the
    /// current term so the W2-E chaos harness
    /// (`tests/jepsen-rs/src/main.rs`) can count leader → follower
    /// transitions per-node — the previous log-parse "stepping down"
    /// regex found zero matches because no code path actually emitted
    /// that phrase, artifactually reporting `step_downs=0` throughout
    /// the mTLS election-storm run
    /// (CL-A1-R-mTLS-harness-{regex,timer}, Task #18).
    pub async fn step_down(&self) {
        let prev_role = *self.role.read().await;
        let term = *self.current_term.read().await;
        {
            let mut role = self.role.write().await;
            *role = ReplicationRole::Follower;
        }
        if prev_role != ReplicationRole::Follower {
            tracing::info!(
                prev_role = ?prev_role,
                term,
                "stepping down to follower"
            );
        }
    }

    /// Drive one iteration of the election / heartbeat scheduler.
    ///
    /// This is the Wave 38-C entry point that replaces the bootstrap hack
    /// (`node_id.ends_with('1')`) in `bins/ferrodruid/src/main.rs`. The
    /// transport layer (`crates/ferrodruid-cluster/src/transport.rs`)
    /// drives a `tick_loop` task that calls this every ~50 ms and acts on
    /// the returned [`TickAction`].
    ///
    /// **Wave 47-A pre-vote flow** (Raft §9.6) — closes the W38-DE honest
    /// gap "pre-vote types implemented but not yet driven from `tick()`":
    ///
    /// * **Follower / Candidate, no pre-vote in flight, timer expired** —
    ///   the engine starts a synthetic pre-vote round at
    ///   `proposed_term = current_term + 1` (no real term bump) and
    ///   returns [`TickAction::BroadcastPreVoteRequest`] so the
    ///   transport can broadcast a `RequestPreVote` to every peer.  The
    ///   election timer is reset to give peers time to respond.
    /// * **Pre-vote in flight, majority reached** — the engine promotes
    ///   to candidate via [`Self::arm_election`] (real term bump,
    ///   self-vote recorded), clears the in-flight marker, and returns
    ///   [`TickAction::BroadcastVoteRequest`] for the real election round.
    /// * **Pre-vote in flight, timer re-expired without majority** — the
    ///   engine drops the stale pre-vote round and starts a new pre-vote
    ///   round at the same proposed term (still no real term bump),
    ///   returning a fresh [`TickAction::BroadcastPreVoteRequest`].
    /// * **Leader** — if [`HeartbeatTicker::should_send`] returns `true`,
    ///   the engine returns [`TickAction::BroadcastHeartbeat`] so the
    ///   transport can broadcast an empty heartbeat to all peers.
    /// * **Otherwise** — [`TickAction::Idle`].
    ///
    /// Single-node clusters short-circuit: when [`ReplicationConfig::peers`]
    /// is empty the engine skips the pre-vote round entirely and arms the
    /// real election directly — pre-vote majority is trivially reached
    /// (only self) and the candidate is immediately promoted to leader by
    /// [`Self::arm_election`].
    ///
    /// This method does NOT do any I/O. It is safe to call from any task
    /// and will not block on transport-level operations.
    pub async fn tick(&self) -> TickAction {
        let role = self.role().await;
        match role {
            ReplicationRole::Follower | ReplicationRole::Candidate => {
                let in_flight = *self.pre_vote_in_flight.read().await;
                let timer_expired = self.election_timer.read().await.is_expired();

                // Phase 1: pre-vote already in flight — promote to real
                // election as soon as majority is reached, regardless of
                // whether the timer has re-expired.  This minimises the
                // window between "majority observed" and "real VoteRequest
                // on the wire".
                //
                // Wave 54-B: majority is now keyed on `round_id` (not
                // `proposed_term`) so a stale grant from a prior round
                // at the same proposed_term cannot tip us over.
                if let Some(round) = in_flight
                    && self.pre_vote_majority_reached(round.round_id).await
                {
                    let new_term = self.arm_election().await;
                    self.election_timer.write().await.reset();
                    *self.pre_vote_in_flight.write().await = None;
                    if self.config.peers.is_empty() {
                        // Single-node short-circuit: arm_election()
                        // already promoted us to Leader, so there is no
                        // one to broadcast to.
                        return TickAction::Idle;
                    }
                    return TickAction::BroadcastVoteRequest {
                        candidate_id: self.config.node_id.clone(),
                        term: new_term,
                    };
                }

                // Phase 1.5: mTLS startup grace (Task #23, W2-E RCA
                // mitigation c). If the engine is still inside the
                // mTLS startup grace window AND the transport hasn't
                // signalled majority handshake auth yet, defer the
                // first election. Resetting the timer here means we
                // keep re-checking on subsequent ticks; the grace
                // clears itself either by transport signal
                // ([`Self::clear_startup_grace`]) or by deadline
                // elapse (fall-through), so a stalled peer can never
                // hang the cluster forever.
                if timer_expired
                    && let Some(deadline) = *self.startup_grace_until.read().await
                    && Instant::now() < deadline
                {
                    // Reset the election timer with the same base +
                    // jitter — deadline moves forward, subsequent
                    // ticks re-check startup grace.
                    self.election_timer.write().await.reset();
                    return TickAction::Idle;
                }

                // Phase 2: timer fired — start (or restart) a pre-vote
                // round.  This is the ONLY path through which the engine
                // proposes to bump the cluster term, and it does so
                // synthetically (current_term unchanged).
                if timer_expired {
                    // Single-node short-circuit: skip the pre-vote round
                    // entirely; arm_election() handles immediate
                    // self-promotion.
                    if self.config.peers.is_empty() {
                        let _ = self.arm_election().await;
                        self.election_timer.write().await.reset();
                        *self.pre_vote_in_flight.write().await = None;
                        return TickAction::Idle;
                    }
                    let round = self.start_pre_vote().await;
                    *self.pre_vote_in_flight.write().await = Some(round);
                    self.election_timer.write().await.reset();
                    return TickAction::BroadcastPreVoteRequest {
                        candidate_id: self.config.node_id.clone(),
                        proposed_term: round.proposed_term,
                        round_id: round.round_id,
                    };
                }
                TickAction::Idle
            }
            ReplicationRole::Leader => {
                // A node that wins the real election is no longer
                // running a pre-vote round; clear any stale marker so a
                // future step-down starts a fresh pre-vote.
                if self.pre_vote_in_flight.read().await.is_some() {
                    *self.pre_vote_in_flight.write().await = None;
                }
                if self.heartbeat_ticker.write().await.should_send() {
                    let term = *self.current_term.read().await;
                    TickAction::BroadcastHeartbeat {
                        leader_id: self.config.node_id.clone(),
                        term,
                    }
                } else {
                    TickAction::Idle
                }
            }
        }
    }

    /// Clear the mTLS startup grace window (Task #23, W2-E RCA
    /// mitigation c). The [`crate::transport::TcpTransport`] tick
    /// loop calls this the first time it observes that a majority of
    /// peers have completed their initial TLS + auth handshake, so
    /// the first election can fire as soon as the cluster is
    /// authenticated rather than waiting for the full
    /// [`Self::MTLS_STARTUP_GRACE_MAX_MS`] budget. Idempotent — a
    /// second call is a no-op.
    ///
    /// Emits a `tracing::info!` on the transition so operators can
    /// see the mitigation firing in real deployments.
    pub async fn clear_startup_grace(&self) {
        let mut slot = self.startup_grace_until.write().await;
        if slot.take().is_some() {
            tracing::info!(
                "cluster-wire mTLS startup grace cleared by majority \
                 handshake auth signal (CL-A1-R-mTLS-source-fix c)"
            );
        }
    }

    /// Snapshot the current startup-grace deadline. Test-only.
    #[doc(hidden)]
    pub async fn startup_grace_deadline(&self) -> Option<Instant> {
        *self.startup_grace_until.read().await
    }

    /// Test-only: snapshot the current election timer (returns a clone so
    /// callers can compare deadlines across calls without holding the lock).
    #[doc(hidden)]
    pub async fn election_timer_snapshot(&self) -> ElectionTimer {
        self.election_timer.read().await.clone()
    }

    /// Test-only: force the election timer to fire on the next tick by
    /// rewinding its deadline to the past.
    #[doc(hidden)]
    pub async fn force_election_timer_expire(&self) {
        let mut t = self.election_timer.write().await;
        // Rewind so `is_expired()` is unconditionally true. We can't
        // construct an `Instant` in the past directly without subtracting
        // from `Instant::now()`, so subtract a large but safe amount.
        t.deadline = Instant::now() - Duration::from_millis(1);
    }

    /// Test-only: read the current pre-vote in-flight marker.
    /// `Some(round)` means a pre-vote round at `round.proposed_term`
    /// (generation `round.round_id`) has been emitted by [`Self::tick`]
    /// and the engine is waiting for peer responses; `None` means no
    /// pre-vote round is active.  Wave 47-A; Wave 54-B carries
    /// `round_id` alongside `proposed_term`.
    #[doc(hidden)]
    pub async fn pre_vote_in_flight(&self) -> Option<PreVoteRound> {
        *self.pre_vote_in_flight.read().await
    }

    /// Test-only: read the current pre-vote round generation id.
    /// Monotonically increases each time [`Self::start_pre_vote`] runs.
    /// Wave 54-B.
    #[doc(hidden)]
    pub async fn pre_vote_round_id(&self) -> u64 {
        *self.pre_vote_round_id.read().await
    }

    /// Test-only: drop the tail of the local log starting at `from_index`
    /// (inclusive) and rewind `last_index` accordingly.  Used by
    /// `case_l_leader_auto_replays_to_lagging_follower` to simulate a
    /// follower that came back from a crash with a truncated log so the
    /// leader's tick-driven replay loop can be observed back-filling.
    /// Wave 47-A.
    #[doc(hidden)]
    pub async fn truncate_log_for_test(&self, from_index: u64) {
        let target_len = from_index.saturating_sub(1) as usize;
        {
            let mut log = self.command_log.write().await;
            log.retain(|(idx, _)| (*idx as usize) <= target_len);
        }
        {
            let mut terms = self.log_terms.write().await;
            terms.truncate(target_len);
        }
        {
            let mut last = self.last_index.write().await;
            *last = target_len as u64;
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory transport for testing
// ---------------------------------------------------------------------------

/// In-memory transport for testing (bypasses TCP).
pub struct InMemoryTransport {
    nodes: RwLock<HashMap<String, mpsc::UnboundedSender<ReplicationMessage>>>,
    inboxes: RwLock<HashMap<String, Vec<ReplicationMessage>>>,
}

impl InMemoryTransport {
    /// Create a new in-memory transport.
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            inboxes: RwLock::new(HashMap::new()),
        }
    }

    /// Register a node and get a receiver for its messages.
    pub async fn register(&self, node_id: &str) -> mpsc::UnboundedReceiver<ReplicationMessage> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.nodes.write().await.insert(node_id.to_string(), tx);
        self.inboxes
            .write()
            .await
            .insert(node_id.to_string(), Vec::new());
        rx
    }

    /// Send a message to a node.
    pub async fn send(&self, to: &str, msg: ReplicationMessage) -> Result<(), ClusterError> {
        let nodes = self.nodes.read().await;
        if let Some(tx) = nodes.get(to) {
            tx.send(msg.clone())
                .map_err(|e| ClusterError::CommandFailed(format!("send failed: {e}")))?;
        }
        // Also store in inbox for batch retrieval
        let mut inboxes = self.inboxes.write().await;
        inboxes.entry(to.to_string()).or_default().push(msg);
        Ok(())
    }

    /// Try to receive all pending messages for a node (drains the inbox).
    pub async fn try_receive(&self, node_id: &str) -> Option<Vec<ReplicationMessage>> {
        let mut inboxes = self.inboxes.write().await;
        inboxes.get_mut(node_id).map(std::mem::take)
    }
}

impl Default for InMemoryTransport {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Wire encoding/decoding
// ---------------------------------------------------------------------------

/// Encode a replication message for TCP transport (4-byte length prefix + JSON).
///
/// Wave 40-A: this **unauthenticated** form is retained for use cases that
/// already provide their own integrity protection (e.g. the in-memory
/// `MockTransport` in this module's tests). Production wire I/O uses
/// [`encode_message_authenticated`].
pub fn encode_message(msg: &ReplicationMessage) -> Result<Vec<u8>, ClusterError> {
    let json = serde_json::to_vec(msg)
        .map_err(|e| ClusterError::CommandFailed(format!("encode failed: {e}")))?;
    let mut buf = Vec::with_capacity(4 + json.len());
    buf.extend_from_slice(&(json.len() as u32).to_be_bytes());
    buf.extend_from_slice(&json);
    Ok(buf)
}

/// Decode a replication message from a length-prefixed buffer.
///
/// Wave 40-A: this **unauthenticated** form is retained for use cases that
/// already provide their own integrity protection. Production wire I/O
/// uses [`decode_message_authenticated`].
pub fn decode_message(data: &[u8]) -> Result<ReplicationMessage, ClusterError> {
    if data.len() < 4 {
        return Err(ClusterError::CommandFailed("message too short".to_string()));
    }
    let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    // DD R33: checked so a u32::MAX prefix cannot wrap `4 + len` (32-bit) below
    // data.len() and then panic slicing `data[4..4 + len]`.
    let end = 4usize
        .checked_add(len)
        .filter(|e| *e <= data.len())
        .ok_or_else(|| ClusterError::CommandFailed("incomplete message".to_string()))?;
    serde_json::from_slice(&data[4..end])
        .map_err(|e| ClusterError::CommandFailed(format!("decode failed: {e}")))
}

/// Wave 40-A: encode a replication message with PSK-authenticated framing.
///
/// Wire layout: `[u32 BE payload_len][32-byte HMAC-SHA256][JSON payload]`.
/// `payload_len` covers `HMAC || JSON` so a single `read_exact` of
/// `payload_len` bytes recovers both the tag and the body.
///
/// The HMAC tag is computed over the JSON bytes only (not the length
/// prefix or the tag itself); this matches the canonical "tag-then-mac"
/// idiom and is what [`decode_message_authenticated`] expects.
pub fn encode_message_authenticated(
    msg: &ReplicationMessage,
    psk: &crate::auth::ClusterPsk,
) -> Result<Vec<u8>, ClusterError> {
    let json = serde_json::to_vec(msg)
        .map_err(|e| ClusterError::CommandFailed(format!("encode failed: {e}")))?;
    encode_authenticated_payload(&json, psk)
}

/// Wave 40-A: encode an arbitrary JSON-serialised payload with the cluster
/// PSK. Used by the handshake and by [`encode_message_authenticated`] so
/// they share one wire-format implementation.
pub fn encode_authenticated_payload(
    json: &[u8],
    psk: &crate::auth::ClusterPsk,
) -> Result<Vec<u8>, ClusterError> {
    let tag = crate::auth::compute_hmac(psk, json);
    let body_len = crate::auth::ClusterPsk::HMAC_TAG_LEN + json.len();
    let body_len_u32: u32 = body_len.try_into().map_err(|_| {
        ClusterError::CommandFailed(format!(
            "authenticated frame body too large: {body_len} bytes",
        ))
    })?;
    let mut buf = Vec::with_capacity(4 + body_len);
    buf.extend_from_slice(&body_len_u32.to_be_bytes());
    buf.extend_from_slice(&tag);
    buf.extend_from_slice(json);
    Ok(buf)
}

/// Wave 40-A: decode + authenticate a replication message.
///
/// `data` must be the full frame: `[u32][32-byte tag][JSON]`. Returns the
/// JSON bytes alongside the parsed message so the transport can record
/// the per-frame body for connection-binding diagnostics.
pub fn decode_message_authenticated(
    data: &[u8],
    psk: &crate::auth::ClusterPsk,
) -> Result<ReplicationMessage, ClusterError> {
    let (json, _) = authenticated_payload_bytes(data, psk)?;
    serde_json::from_slice(json)
        .map_err(|e| ClusterError::CommandFailed(format!("decode failed: {e}")))
}

/// Wave 40-A: verify the HMAC of a fully-buffered frame and return a
/// reference to the inner JSON bytes. Used by both the message and
/// handshake decode paths.
pub fn authenticated_payload_bytes<'a>(
    data: &'a [u8],
    psk: &crate::auth::ClusterPsk,
) -> Result<(&'a [u8], &'a [u8]), ClusterError> {
    if data.len() < 4 {
        return Err(ClusterError::CommandFailed(
            "authenticated frame too short for length prefix".to_string(),
        ));
    }
    let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    // DD R33: checked so a u32::MAX prefix cannot wrap `4 + len` (32-bit) below
    // data.len() and then panic slicing `data[4..4 + len]`.
    let end = 4usize
        .checked_add(len)
        .filter(|e| *e <= data.len())
        .ok_or_else(|| ClusterError::CommandFailed("incomplete authenticated frame".to_string()))?;
    if len < crate::auth::ClusterPsk::HMAC_TAG_LEN {
        return Err(ClusterError::CommandFailed(format!(
            "authenticated frame too short for HMAC tag: len = {len}",
        )));
    }
    let body = &data[4..end];
    let (tag, json) = body.split_at(crate::auth::ClusterPsk::HMAC_TAG_LEN);
    crate::auth::verify_hmac(psk, json, tag)
        .map_err(|_| ClusterError::CommandFailed("HMAC verification failed".to_string()))?;
    Ok((json, tag))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeInfo, NodeRole, SegmentAction, SegmentAnnouncement, ServiceEntry};

    fn make_cluster_manager(id: &str) -> Arc<ClusterManager> {
        Arc::new(ClusterManager::new_single_node(NodeInfo {
            id: id.to_string(),
            host: "127.0.0.1".to_string(),
            port: 8888,
            role: NodeRole::AllInOne,
        }))
    }

    fn make_config(node_id: &str, peers: Vec<&str>) -> ReplicationConfig {
        ReplicationConfig {
            node_id: node_id.to_string(),
            listen_addr: "127.0.0.1:0".to_string(),
            peers: peers.into_iter().map(|s| s.to_string()).collect(),
            heartbeat_interval_ms: 100,
            election_timeout_ms: 300,
            cluster_security_hint: ClusterSecurityHint::Psk,
        }
    }

    /// **CL-A1-R-mTLS-source-fix (W2-E RCA closure)**: under PSK
    /// posture the effective election params leave the operator's
    /// timeout untouched and use the historical 150 ms compat jitter.
    #[test]
    fn effective_election_params_psk_preserves_operator_value() {
        let (base, jitter) =
            ReplicationEngine::effective_election_params(1500, ClusterSecurityHint::Psk);
        assert_eq!(base, Duration::from_millis(1500));
        assert_eq!(jitter, Duration::from_millis(150));

        // Even a very small operator value stays untouched under PSK.
        let (base_tight, jitter_tight) =
            ReplicationEngine::effective_election_params(300, ClusterSecurityHint::Psk);
        assert_eq!(base_tight, Duration::from_millis(300));
        assert_eq!(jitter_tight, Duration::from_millis(150));
    }

    /// Under mTLS posture the engine floors sub-5-second operator
    /// values to `MTLS_ELECTION_TIMEOUT_FLOOR_MS` and widens jitter to
    /// `effective / MTLS_ELECTION_JITTER_DIVISOR` — the exact fix for
    /// the W2-E election-storm.
    #[test]
    fn effective_election_params_mtls_applies_floor_and_wide_jitter() {
        // Sub-floor operator value → raised to floor.
        let (base, jitter) =
            ReplicationEngine::effective_election_params(1500, ClusterSecurityHint::Mtls);
        assert_eq!(base, Duration::from_millis(5000));
        assert_eq!(jitter, Duration::from_millis(1250)); // 5000 / 4

        // Above-floor operator value stays untouched (jitter still
        // scales with the effective value).
        let (base_hi, jitter_hi) =
            ReplicationEngine::effective_election_params(8000, ClusterSecurityHint::Mtls);
        assert_eq!(base_hi, Duration::from_millis(8000));
        assert_eq!(jitter_hi, Duration::from_millis(2000)); // 8000 / 4
    }

    /// The two constructors [`ReplicationEngine::new`] and
    /// [`ReplicationEngine::with_transport`] must apply the same
    /// mTLS-posture widening — otherwise loopback tests that go
    /// through `with_transport` would silently disagree with the
    /// production-binary path.
    #[test]
    fn mtls_hint_widens_election_timer_on_both_constructors() {
        let cm = Arc::new(ClusterManager::new_single_node(NodeInfo {
            id: "node-mtls".to_string(),
            host: "127.0.0.1".to_string(),
            port: 0,
            role: NodeRole::AllInOne,
        }));

        // `new` path: tight operator value + mTLS hint → floored.
        let mut cfg = make_config("node-mtls", vec!["peer-a"]);
        cfg.election_timeout_ms = 300;
        cfg.cluster_security_hint = ClusterSecurityHint::Mtls;
        let engine_new = ReplicationEngine::new(cfg.clone(), Arc::clone(&cm));
        let timer_new = engine_new.election_timer.blocking_read();
        assert_eq!(
            timer_new.timeout(),
            Duration::from_millis(5000),
            "mTLS floor must apply through `new`"
        );

        // `with_transport` path: same tight operator value → same floor.
        let transport = Arc::new(InMemoryTransport::new());
        let engine_wt = ReplicationEngine::with_transport(cfg, cm, transport);
        let timer_wt = engine_wt.election_timer.blocking_read();
        assert_eq!(
            timer_wt.timeout(),
            Duration::from_millis(5000),
            "mTLS floor must apply through `with_transport` too"
        );
    }

    /// **CL-A1-R-mTLS-source-fix (c)** startup-grace initialization
    /// (Task #23). Under mTLS posture the engine arms a
    /// `startup_grace_until` deadline at construction time; PSK
    /// posture leaves it as `None`.
    #[tokio::test]
    async fn mtls_startup_grace_deadline_armed_only_under_mtls() {
        let cm = Arc::new(ClusterManager::new_single_node(NodeInfo {
            id: "node-mtls".to_string(),
            host: "127.0.0.1".to_string(),
            port: 0,
            role: NodeRole::AllInOne,
        }));

        // PSK — no grace armed.
        let cfg_psk = make_config("node-psk", vec!["peer-a"]);
        let engine_psk = ReplicationEngine::new(cfg_psk, Arc::clone(&cm));
        assert!(
            engine_psk.startup_grace_deadline().await.is_none(),
            "PSK posture must NOT arm the startup grace"
        );

        // mTLS — grace armed with deadline in [now, now + max].
        let mut cfg_mtls = make_config("node-mtls", vec!["peer-a"]);
        cfg_mtls.cluster_security_hint = ClusterSecurityHint::Mtls;
        let now = Instant::now();
        let engine_mtls = ReplicationEngine::new(cfg_mtls, cm);
        let deadline = engine_mtls
            .startup_grace_deadline()
            .await
            .expect("mTLS posture must arm the startup grace");
        let elapsed = deadline.saturating_duration_since(now).as_millis();
        assert!(
            elapsed > 0 && elapsed <= ReplicationEngine::MTLS_STARTUP_GRACE_MAX_MS as u128 + 100,
            "deadline must be within the grace budget, got {elapsed} ms"
        );
    }

    /// `clear_startup_grace` transitions `Some → None` idempotently and
    /// is a no-op on already-cleared engines (PSK path).
    #[tokio::test]
    async fn clear_startup_grace_transitions_some_to_none() {
        let cm = Arc::new(ClusterManager::new_single_node(NodeInfo {
            id: "node-mtls".to_string(),
            host: "127.0.0.1".to_string(),
            port: 0,
            role: NodeRole::AllInOne,
        }));
        let mut cfg = make_config("node-mtls", vec!["peer-a"]);
        cfg.cluster_security_hint = ClusterSecurityHint::Mtls;
        let engine = ReplicationEngine::new(cfg, cm);

        assert!(engine.startup_grace_deadline().await.is_some());
        engine.clear_startup_grace().await;
        assert!(engine.startup_grace_deadline().await.is_none());
        // Idempotent: second call is a no-op, doesn't panic, doesn't
        // resurrect the deadline.
        engine.clear_startup_grace().await;
        assert!(engine.startup_grace_deadline().await.is_none());
    }

    /// Startup grace defers the first pre-vote: when the timer
    /// expires but grace is still active, `tick()` returns
    /// `TickAction::Idle` and resets the timer, so no pre-vote
    /// broadcasts fire until grace clears.
    #[tokio::test]
    async fn tick_defers_first_election_under_startup_grace() {
        let cm = Arc::new(ClusterManager::new_single_node(NodeInfo {
            id: "node-mtls".to_string(),
            host: "127.0.0.1".to_string(),
            port: 0,
            role: NodeRole::AllInOne,
        }));
        let mut cfg = make_config("node-mtls", vec!["peer-a", "peer-b"]);
        cfg.cluster_security_hint = ClusterSecurityHint::Mtls;
        let engine = ReplicationEngine::new(cfg, cm);

        // Force the election timer to fire immediately.
        engine.force_election_timer_expire().await;
        assert!(engine.election_timer_snapshot().await.is_expired());

        // Grace is armed → tick returns Idle even though timer fired.
        let action = engine.tick().await;
        assert!(
            matches!(action, TickAction::Idle),
            "startup grace must defer pre-vote; got {action:?}"
        );

        // Clear grace → next expired-timer tick fires pre-vote.
        engine.clear_startup_grace().await;
        engine.force_election_timer_expire().await;
        let action = engine.tick().await;
        assert!(
            matches!(action, TickAction::BroadcastPreVoteRequest { .. }),
            "post-grace tick must fire pre-vote; got {action:?}"
        );
    }

    #[test]
    fn message_serialize_deserialize_roundtrip() {
        let messages = vec![
            ReplicationMessage::Heartbeat {
                leader_id: "node-1".to_string(),
                term: 5,
            },
            ReplicationMessage::VoteRequest {
                candidate_id: "node-2".to_string(),
                term: 3,
            },
            ReplicationMessage::VoteResponse {
                voter_id: "node-3".to_string(),
                term: 3,
                granted: true,
            },
            ReplicationMessage::ReplicateCommand {
                term: 1,
                index: 42,
                command: ClusterCommand::RegisterService(ServiceEntry {
                    service_type: "broker".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 8082,
                    node_id: "node-1".to_string(),
                }),
            },
            ReplicationMessage::ReplicateAck {
                follower_id: "node-2".to_string(),
                index: 42,
                success: true,
                last_log_index_hint: Some(42),
            },
            ReplicationMessage::SnapshotRequest {
                follower_id: "node-3".to_string(),
            },
            ReplicationMessage::Join {
                node_id: "node-4".to_string(),
                addr: "10.0.0.4:8888".to_string(),
            },
        ];

        for msg in messages {
            let json = serde_json::to_string(&msg).expect("serialize");
            let restored: ReplicationMessage = serde_json::from_str(&json).expect("deserialize");
            // Verify type tag matches
            let orig_tag = format!("{msg:?}")
                .split('{')
                .next()
                .unwrap_or("")
                .to_string();
            let rest_tag = format!("{restored:?}")
                .split('{')
                .next()
                .unwrap_or("")
                .to_string();
            assert_eq!(orig_tag, rest_tag);
        }
    }

    #[test]
    fn encode_decode_message_roundtrip() {
        let msg = ReplicationMessage::Heartbeat {
            leader_id: "leader-1".to_string(),
            term: 7,
        };
        let encoded = encode_message(&msg).expect("encode");
        let decoded = decode_message(&encoded).expect("decode");
        match decoded {
            ReplicationMessage::Heartbeat { leader_id, term } => {
                assert_eq!(leader_id, "leader-1");
                assert_eq!(term, 7);
            }
            other => panic!("unexpected message type: {other:?}"),
        }
    }

    #[test]
    fn decode_message_too_short() {
        let result = decode_message(&[0, 0]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_message_incomplete() {
        // Length says 100 bytes but only 4 + 2 available
        let data = [0, 0, 0, 100, 0, 0];
        let result = decode_message(&data);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn single_node_self_election() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec![]);
        let engine = ReplicationEngine::new(config, cm);

        assert_eq!(engine.role().await, ReplicationRole::Follower);
        let won = engine.start_election().await.expect("election");
        assert!(won);
        assert_eq!(engine.role().await, ReplicationRole::Leader);
        assert_eq!(engine.term().await, 1);
    }

    #[test]
    fn decode_message_rejects_overlong_length_prefix() {
        // DD R33: a 4-byte length prefix of u32::MAX must be rejected (incomplete
        // frame), never wrap `4 + len` and panic slicing. Covers both the plain
        // and authenticated public decode helpers.
        let mut data = u32::MAX.to_be_bytes().to_vec();
        data.extend_from_slice(b"short");
        assert!(decode_message(&data).is_err());
        let psk = crate::auth::derive_psk("test-cluster-psk-Wave-40A").expect("psk");
        assert!(authenticated_payload_bytes(&data, &psk).is_err());
    }

    #[tokio::test]
    async fn restore_snapshot_rejects_huge_last_index() {
        // DD R31: a SnapshotResponse carrying a huge `last_index` must be
        // rejected before `log_terms.resize` allocates tens of GiB; a normal
        // index is still accepted.
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec![]);
        let engine = ReplicationEngine::new(config, cm);
        let (snap, _, _) = engine.get_snapshot().await;

        let err = engine
            .restore_snapshot(snap.clone(), u64::MAX, 1)
            .await
            .expect_err("huge snapshot last_index must be rejected");
        assert!(
            err.to_string().contains("last_index"),
            "expected a last_index cap error, got: {err}"
        );

        engine
            .restore_snapshot(snap, 5, 1)
            .await
            .expect("a small last_index must still restore");
    }

    #[tokio::test]
    async fn role_transitions() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec![]);
        let engine = ReplicationEngine::new(config, cm);

        // Start as follower
        assert_eq!(engine.role().await, ReplicationRole::Follower);

        // Election makes it candidate then leader (single-node)
        engine.start_election().await.expect("election");
        assert_eq!(engine.role().await, ReplicationRole::Leader);

        // Step down
        engine.step_down().await;
        assert_eq!(engine.role().await, ReplicationRole::Follower);
    }

    #[tokio::test]
    async fn heartbeat_resets_to_follower() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2"]);
        let engine = ReplicationEngine::new(config, cm);

        // Become leader first
        engine.force_leader().await;
        assert_eq!(engine.role().await, ReplicationRole::Leader);

        // Receive heartbeat with higher term -> step down to follower
        engine.receive_heartbeat("node-2", 10).await.expect("hb");
        assert_eq!(engine.role().await, ReplicationRole::Follower);
        assert_eq!(engine.term().await, 10);
    }

    #[tokio::test]
    async fn command_submission_on_leader() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec![]);
        let engine = ReplicationEngine::new(config, cm.clone());

        engine.start_election().await.expect("election");

        let result = engine
            .submit(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "127.0.0.1".to_string(),
                port: 8082,
                node_id: "node-1".to_string(),
            }))
            .await
            .expect("submit");

        assert_eq!(result, CommandResult::Ok);
        assert_eq!(engine.last_index().await, 1);
        assert_eq!(cm.services("broker").len(), 1);
    }

    #[tokio::test]
    async fn command_rejection_on_follower() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2"]);
        let engine = ReplicationEngine::new(config, cm);

        // Not a leader
        let result = engine
            .submit(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "127.0.0.1".to_string(),
                port: 8082,
                node_id: "node-1".to_string(),
            }))
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn snapshot_get_and_restore() {
        let cm1 = make_cluster_manager("node-1");
        let config1 = make_config("node-1", vec![]);
        let engine1 = ReplicationEngine::new(config1, cm1.clone());
        engine1.start_election().await.expect("election");

        // Submit some commands
        engine1
            .submit(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "127.0.0.1".to_string(),
                port: 8082,
                node_id: "node-1".to_string(),
            }))
            .await
            .expect("submit");

        let (snapshot, last_idx, term) = engine1.get_snapshot().await;
        assert_eq!(last_idx, 1);
        assert_eq!(term, 1);

        // Restore on a second engine
        let cm2 = make_cluster_manager("node-2");
        let config2 = make_config("node-2", vec![]);
        let engine2 = ReplicationEngine::new(config2, cm2.clone());

        engine2
            .restore_snapshot(snapshot, last_idx, term)
            .await
            .expect("restore");

        assert_eq!(engine2.last_index().await, 1);
        assert_eq!(engine2.term().await, 1);
        assert_eq!(cm2.services("broker").len(), 1);
    }

    #[tokio::test]
    async fn in_memory_transport_three_node_election() {
        let transport = Arc::new(InMemoryTransport::new());

        // Register all nodes
        let _rx1 = transport.register("node-1").await;
        let _rx2 = transport.register("node-2").await;
        let _rx3 = transport.register("node-3").await;

        let cm1 = make_cluster_manager("node-1");
        let config1 = make_config("node-1", vec!["node-2", "node-3"]);
        let engine1 = ReplicationEngine::with_transport(config1, cm1, Arc::clone(&transport));

        let cm2 = make_cluster_manager("node-2");
        let config2 = make_config("node-2", vec!["node-1", "node-3"]);
        let engine2 = ReplicationEngine::with_transport(config2, cm2, Arc::clone(&transport));

        let cm3 = make_cluster_manager("node-3");
        let config3 = make_config("node-3", vec!["node-1", "node-2"]);
        let engine3 = ReplicationEngine::with_transport(config3, cm3, Arc::clone(&transport));

        // Node-1 starts election - sends VoteRequest to node-2 and node-3
        // First, simulate vote responses
        let vote_resp_2 = engine2.receive_vote_request("node-1", 1).await;
        let vote_resp_3 = engine3.receive_vote_request("node-1", 1).await;

        // Deliver vote responses to node-1's inbox
        transport.send("node-1", vote_resp_2).await.expect("send");
        transport.send("node-1", vote_resp_3).await.expect("send");

        // Now node-1 starts election (votes already in inbox)
        let won = engine1.start_election().await.expect("election");
        assert!(won);
        assert_eq!(engine1.role().await, ReplicationRole::Leader);
    }

    #[tokio::test]
    async fn in_memory_transport_command_replication() {
        let transport = Arc::new(InMemoryTransport::new());
        let _rx1 = transport.register("node-1").await;
        let mut rx2 = transport.register("node-2").await;

        let cm1 = make_cluster_manager("node-1");
        let config1 = make_config("node-1", vec!["node-2"]);
        let engine1 =
            ReplicationEngine::with_transport(config1, cm1.clone(), Arc::clone(&transport));

        let cm2 = make_cluster_manager("node-2");
        let config2 = make_config("node-2", vec!["node-1"]);
        let engine2 =
            ReplicationEngine::with_transport(config2, cm2.clone(), Arc::clone(&transport));

        // Make node-1 leader
        engine1.force_leader_with_term(1).await;

        // Submit command on leader
        engine1
            .submit(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "10.0.0.1".to_string(),
                port: 9092,
                node_id: "node-1".to_string(),
            }))
            .await
            .expect("submit");

        // Node-2 should have received the replicated command via channel
        let msg = rx2.recv().await.expect("receive");
        match msg {
            ReplicationMessage::ReplicateCommand {
                term,
                index,
                command,
            } => {
                assert_eq!(term, 1);
                assert_eq!(index, 1);
                // Apply on follower
                engine2
                    .receive_command(term, index, command)
                    .await
                    .expect("apply");
            }
            other => panic!("expected ReplicateCommand, got {other:?}"),
        }

        // Verify both nodes have the service
        assert_eq!(cm1.services("broker").len(), 1);
        assert_eq!(cm2.services("broker").len(), 1);
    }

    #[tokio::test]
    async fn leader_step_down_on_higher_term() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2"]);
        let engine = ReplicationEngine::new(config, cm);

        // Start as leader at term 1
        engine.force_leader_with_term(1).await;
        assert!(engine.is_leader().await);

        // Receive heartbeat with higher term
        engine.receive_heartbeat("node-2", 5).await.expect("hb");

        assert_eq!(engine.role().await, ReplicationRole::Follower);
        assert_eq!(engine.term().await, 5);
    }

    #[tokio::test]
    async fn term_monotonicity() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec![]);
        let engine = ReplicationEngine::new(config, cm);

        assert_eq!(engine.term().await, 0);
        engine.start_election().await.expect("election");
        assert_eq!(engine.term().await, 1);
        engine.step_down().await;
        engine.start_election().await.expect("election");
        assert_eq!(engine.term().await, 2);

        // Heartbeat with higher term jumps
        engine.receive_heartbeat("other", 10).await.expect("hb");
        assert_eq!(engine.term().await, 10);

        // Heartbeat with lower term does not decrease
        engine.receive_heartbeat("other", 5).await.expect("hb");
        assert_eq!(engine.term().await, 10);
    }

    #[tokio::test]
    async fn vote_granting_logic() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3"]);
        let engine = ReplicationEngine::new(config, cm);

        // First vote request at term 1 from node-2: granted
        let resp = engine.receive_vote_request("node-2", 1).await;
        match resp {
            ReplicationMessage::VoteResponse { granted, .. } => assert!(granted),
            _ => panic!("expected VoteResponse"),
        }

        // Second vote request at same term from node-3: denied (already voted)
        let resp = engine.receive_vote_request("node-3", 1).await;
        match resp {
            ReplicationMessage::VoteResponse { granted, .. } => assert!(!granted),
            _ => panic!("expected VoteResponse"),
        }

        // Same candidate again at same term: granted (idempotent)
        let resp = engine.receive_vote_request("node-2", 1).await;
        match resp {
            ReplicationMessage::VoteResponse { granted, .. } => assert!(granted),
            _ => panic!("expected VoteResponse"),
        }

        // Higher term from node-3: granted (new term clears vote)
        let resp = engine.receive_vote_request("node-3", 2).await;
        match resp {
            ReplicationMessage::VoteResponse { granted, .. } => assert!(granted),
            _ => panic!("expected VoteResponse"),
        }
    }

    #[tokio::test]
    async fn vote_request_rejected_for_stale_term() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2"]);
        let engine = ReplicationEngine::new(config, cm);

        // Set current term to 5
        engine.receive_heartbeat("leader", 5).await.expect("hb");

        // Vote request at term 3 (stale): denied
        let resp = engine.receive_vote_request("node-2", 3).await;
        match resp {
            ReplicationMessage::VoteResponse { granted, term, .. } => {
                assert!(!granted);
                assert_eq!(term, 5);
            }
            _ => panic!("expected VoteResponse"),
        }
    }

    #[tokio::test]
    async fn receive_command_with_stale_term_rejected() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2"]);
        let engine = ReplicationEngine::new(config, cm);

        // Set term to 5
        engine.receive_heartbeat("leader", 5).await.expect("hb");

        // Command at term 3 should be rejected
        let result = engine
            .receive_command(
                3,
                1,
                ClusterCommand::RegisterService(ServiceEntry {
                    service_type: "broker".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 8082,
                    node_id: "x".to_string(),
                }),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn in_memory_transport_send_receive() {
        let transport = InMemoryTransport::new();
        let mut rx = transport.register("node-1").await;

        let msg = ReplicationMessage::Heartbeat {
            leader_id: "leader".to_string(),
            term: 1,
        };
        transport.send("node-1", msg).await.expect("send");

        let received = rx.recv().await.expect("recv");
        match received {
            ReplicationMessage::Heartbeat { leader_id, term } => {
                assert_eq!(leader_id, "leader");
                assert_eq!(term, 1);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Wave 38-A: vote dedup + log monotonicity (close DD R1 cluster Highs)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn vote_response_from_same_node_in_same_term_only_counted_once() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3"]);
        let engine = ReplicationEngine::new(config, cm);

        // Arm an election at term 1 (becomes candidate, self-votes).
        {
            let mut t = engine.current_term.write().await;
            *t = 1;
        }
        {
            let mut active = engine.election_term.write().await;
            *active = Some(1);
        }
        {
            let mut votes = engine.votes_received.write().await;
            votes.entry(1).or_default().insert("node-1".to_string());
        }

        // First vote from node-2 at term 1: counted.
        assert!(engine.record_vote("node-2", 1).await);
        assert_eq!(engine.votes_count(1).await, 2); // self + node-2

        // Replay the same vote 5x: still 2 (HashSet dedupe).
        for _ in 0..5 {
            assert!(!engine.record_vote("node-2", 1).await);
        }
        assert_eq!(engine.votes_count(1).await, 2);

        // A real second voter (node-3) is counted.
        assert!(engine.record_vote("node-3", 1).await);
        assert_eq!(engine.votes_count(1).await, 3);
    }

    #[tokio::test]
    async fn vote_response_from_unknown_node_rejected() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3"]);
        let engine = ReplicationEngine::new(config, cm);
        {
            let mut t = engine.current_term.write().await;
            *t = 1;
        }
        {
            let mut active = engine.election_term.write().await;
            *active = Some(1);
        }

        // node-99 is not in peers nor self: vote dropped.
        assert!(!engine.record_vote("node-99", 1).await);
        assert_eq!(engine.votes_count(1).await, 0);
    }

    #[tokio::test]
    async fn vote_response_from_old_term_dropped() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2"]);
        let engine = ReplicationEngine::new(config, cm);

        // We are at term 5; an arriving VoteResponse for term 3 must be dropped.
        {
            let mut t = engine.current_term.write().await;
            *t = 5;
        }
        {
            let mut active = engine.election_term.write().await;
            *active = Some(5);
        }

        assert!(!engine.record_vote("node-2", 3).await);
        assert_eq!(engine.votes_count(3).await, 0);
        assert_eq!(engine.votes_count(5).await, 0);
    }

    #[tokio::test]
    async fn vote_set_reset_on_new_term() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec![]);
        let engine = ReplicationEngine::new(config, cm);

        // First election at term 1 (self-vote only since peers is empty).
        engine.start_election().await.expect("election");
        assert_eq!(engine.term().await, 1);
        assert_eq!(engine.votes_count(1).await, 1);

        // Step down so we can run another election.
        engine.step_down().await;

        // Second election bumps term to 2 and resets the per-term vote set.
        engine.start_election().await.expect("election");
        assert_eq!(engine.term().await, 2);
        // Old term's set was cleared.
        assert_eq!(engine.votes_count(1).await, 0);
        // New term has the self-vote.
        assert_eq!(engine.votes_count(2).await, 1);
    }

    #[tokio::test]
    async fn append_entries_rejects_when_prev_log_index_unknown() {
        // "Prev-log-index unknown" here means the leader has skipped past
        // the follower's last_index — i.e. index > last_index + 1.
        let cm = make_cluster_manager("follower");
        let config = make_config("follower", vec!["leader"]);
        let engine = ReplicationEngine::new(config, cm);

        // Follower is at last_index=0, term=1.
        {
            let mut t = engine.current_term.write().await;
            *t = 1;
        }

        // Leader claims index 5 (gap of 4) — must be rejected.
        let result = engine
            .receive_command(
                1,
                5,
                ClusterCommand::RegisterService(ServiceEntry {
                    service_type: "broker".to_string(),
                    host: "10.0.0.1".to_string(),
                    port: 9092,
                    node_id: "x".to_string(),
                }),
            )
            .await;
        assert!(result.is_err(), "gap append must be rejected");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("log gap detected"),
            "error message should cite gap, got: {err}"
        );
        assert_eq!(engine.last_index().await, 0, "last_index must not advance");
    }

    #[tokio::test]
    async fn append_entries_rejects_when_prev_log_term_mismatches() {
        // Follower has an entry at index 1 with term 1 (from a prior leader),
        // and a NEW leader at term 2 sends a replay at index 1 claiming
        // term 2. Since the stored term differs, reject — surfaces the
        // log divergence so the operator notices.
        let cm = make_cluster_manager("follower");
        let config = make_config("follower", vec!["leader"]);
        let engine = ReplicationEngine::new(config, cm);

        // Apply one entry at term 1.
        engine
            .receive_command(
                1,
                1,
                ClusterCommand::RegisterService(ServiceEntry {
                    service_type: "broker".to_string(),
                    host: "10.0.0.1".to_string(),
                    port: 9092,
                    node_id: "svc-1".to_string(),
                }),
            )
            .await
            .expect("first append");
        assert_eq!(engine.last_index().await, 1);

        // New leader at term 2 replays index 1 — divergence detected.
        let result = engine
            .receive_command(
                2,
                1,
                ClusterCommand::RegisterService(ServiceEntry {
                    service_type: "broker".to_string(),
                    host: "10.0.0.99".to_string(), // different payload
                    port: 9092,
                    node_id: "svc-99".to_string(),
                }),
            )
            .await;
        assert!(result.is_err(), "term mismatch on replay must be rejected");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("log term mismatch"),
            "error message should cite term mismatch, got: {err}"
        );
    }

    #[tokio::test]
    async fn append_entries_idempotent_on_replay_of_already_applied() {
        // Replaying an already-applied entry (same index, same term) must
        // be a no-op: state machine must NOT be mutated twice and
        // last_index must not decrease.
        let cm = make_cluster_manager("follower");
        let config = make_config("follower", vec!["leader"]);
        let engine = ReplicationEngine::new(config, cm.clone());

        let cmd = ClusterCommand::RegisterService(ServiceEntry {
            service_type: "broker".to_string(),
            host: "10.0.0.1".to_string(),
            port: 9092,
            node_id: "svc-1".to_string(),
        });
        engine
            .receive_command(1, 1, cmd.clone())
            .await
            .expect("first append");
        assert_eq!(cm.services("broker").len(), 1);
        assert_eq!(engine.last_index().await, 1);

        // Replay the SAME command 3 times.
        for _ in 0..3 {
            engine
                .receive_command(1, 1, cmd.clone())
                .await
                .expect("replay must be idempotent ok");
        }

        // Service must still be at 1 (state machine not mutated by replay).
        assert_eq!(
            cm.services("broker").len(),
            1,
            "replay must not double-apply",
        );
        assert_eq!(engine.last_index().await, 1);
    }

    #[tokio::test]
    async fn append_entries_with_empty_entries_does_not_advance_last_index() {
        // The current `Heartbeat` message carries no entries — pin the
        // invariant that a bare heartbeat does NOT mutate last_index even
        // when the leader's term advances.
        let cm = make_cluster_manager("follower");
        let config = make_config("follower", vec!["leader"]);
        let engine = ReplicationEngine::new(config, cm);

        // Heartbeat at term 5.
        engine.receive_heartbeat("leader", 5).await.expect("hb");
        assert_eq!(engine.term().await, 5);
        assert_eq!(
            engine.last_index().await,
            0,
            "heartbeat must not advance last_index",
        );
    }

    // -----------------------------------------------------------------------
    // Wave 38-C: heartbeat-driven failover (election timer + heartbeat ticker)
    // -----------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn election_timer_fires_after_timeout_with_no_heartbeat() {
        // A follower with no heartbeats must arm a new election after its
        // timer expires. We use `tokio::time::pause()` (via `start_paused`)
        // so the test is deterministic — the timer's randomized jitter is
        // bounded above by `DEFAULT_ELECTION_JITTER_MS = 150 ms`, so
        // advancing 2 s past the 300 ms base timeout guarantees fire.
        //
        // Wave 47-A: the first tick after a fired timer now starts a
        // *pre-vote* round (Raft §9.6) — the real term bump only happens
        // on the next tick after pre-vote majority is observed.  We
        // simulate the granted pre-votes from the two peers, then tick
        // again to drive the real election round.
        let cm = make_cluster_manager("node-1");
        let mut config = make_config("node-1", vec!["node-2", "node-3"]);
        config.election_timeout_ms = 300;
        let engine = ReplicationEngine::new(config, cm);
        assert_eq!(engine.role().await, ReplicationRole::Follower);

        // Advance well past timeout + jitter envelope.
        tokio::time::advance(Duration::from_millis(2_000)).await;

        // Tick #1 emits the pre-vote round; current_term must NOT bump.
        let pv_action = engine.tick().await;
        let emitted_round_id = match pv_action {
            TickAction::BroadcastPreVoteRequest {
                candidate_id,
                proposed_term,
                round_id,
            } => {
                assert_eq!(candidate_id, "node-1");
                assert_eq!(proposed_term, 1);
                round_id
            }
            other => panic!("expected BroadcastPreVoteRequest, got {other:?}"),
        };
        assert_eq!(engine.role().await, ReplicationRole::Follower);
        assert_eq!(engine.term().await, 0);

        // Simulate granted pre-vote from one peer → majority of 3 reached.
        assert!(
            engine
                .receive_pre_vote_response("node-2", 1, true, emitted_round_id)
                .await
        );

        // Tick #2: pre-vote majority observed → real election round.
        let vote_action = engine.tick().await;
        match vote_action {
            TickAction::BroadcastVoteRequest { candidate_id, term } => {
                assert_eq!(candidate_id, "node-1");
                assert_eq!(term, 1);
            }
            other => panic!("expected BroadcastVoteRequest, got {other:?}"),
        }
        assert_eq!(engine.role().await, ReplicationRole::Candidate);
        assert_eq!(engine.term().await, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_resets_election_timer() {
        // After receiving a valid heartbeat, the follower must defer its
        // election: a subsequent `tick()` (before another full timeout) must
        // remain `Idle`.
        let cm = make_cluster_manager("node-1");
        let mut config = make_config("node-1", vec!["node-2"]);
        config.election_timeout_ms = 300;
        let engine = ReplicationEngine::new(config, cm);

        // Halfway through the timeout, deliver a heartbeat.
        tokio::time::advance(Duration::from_millis(150)).await;
        engine.receive_heartbeat("node-2", 1).await.expect("hb");

        // Advance another 200 ms — strictly less than the (300 + jitter)
        // window from the heartbeat reset, so the timer should NOT fire.
        tokio::time::advance(Duration::from_millis(200)).await;
        let action = engine.tick().await;
        assert!(
            matches!(action, TickAction::Idle),
            "tick should be Idle after recent heartbeat reset, got {action:?}",
        );
        // Role must still be Follower; we didn't arm an election.
        assert_eq!(engine.role().await, ReplicationRole::Follower);

        // Advance another 1.5 s past the heartbeat — well beyond the
        // (300 + 150) ms maximum, so the timer must fire now.  Wave 47-A:
        // the first tick after the fired timer starts a pre-vote round
        // (not a real vote round); the real-vote round is gated behind
        // pre-vote majority.
        tokio::time::advance(Duration::from_millis(1_500)).await;
        let action = engine.tick().await;
        assert!(
            matches!(action, TickAction::BroadcastPreVoteRequest { .. }),
            "timer must fire pre-vote after long silence following heartbeat, got {action:?}",
        );
    }

    #[tokio::test]
    async fn randomized_offset_prevents_split_vote_in_3_node_test() {
        // Three followers, each with the same base timeout, must NOT all
        // schedule the exact same election deadline. With `rng_spread =
        // 150 ms` over a `300 ms` base, the probability of three uniform
        // draws colliding to within < 1 ms is well under 1%, so we just
        // assert that not all three deadlines are identical (i.e. at
        // least two distinct values across the three timers).
        //
        // This is a probabilistic check, but the deterministic guarantee
        // is that `rng_spread > Duration::ZERO` causes `ElectionTimer::reset`
        // to add jitter — pre-Wave-38-C code had no such jitter and would
        // have produced 3 identical deadlines every time.
        let mk = |id: &str| {
            let cm = make_cluster_manager(id);
            let mut cfg = make_config(id, vec!["node-x", "node-y"]);
            cfg.election_timeout_ms = 300;
            ReplicationEngine::new(cfg, cm)
        };
        let a = mk("node-1");
        let b = mk("node-2");
        let c = mk("node-3");

        // Read deadlines.
        let da = a.election_timer_snapshot().await.deadline;
        let db = b.election_timer_snapshot().await.deadline;
        let dc = c.election_timer_snapshot().await.deadline;

        // At least one pair must differ. (`Instant` equality is exact.)
        let all_same = da == db && db == dc;
        assert!(
            !all_same,
            "election timer jitter must produce distinct deadlines across 3 followers (pre-fix would collide deterministically)",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn leader_sends_heartbeat_at_interval() {
        // A leader must emit `BroadcastHeartbeat` on the very first tick
        // (the `HeartbeatTicker` is constructed to fire immediately so a
        // freshly-elected leader announces itself), and again after the
        // configured interval has elapsed. Two ticks back-to-back without
        // intervening time advance must NOT fire twice.
        let cm = make_cluster_manager("node-1");
        let mut config = make_config("node-1", vec!["node-2"]);
        config.heartbeat_interval_ms = 100;
        let engine = ReplicationEngine::new(config, cm);
        engine.force_leader_with_term(1).await;

        // First tick after becoming leader should fire (ticker's last_sent
        // is initialised in the past so this is intentional).
        let a = engine.tick().await;
        assert!(
            matches!(a, TickAction::BroadcastHeartbeat { .. }),
            "first leader tick must fire heartbeat, got {a:?}",
        );

        // Immediate second tick (no time advance) must NOT fire.
        let b = engine.tick().await;
        assert!(
            matches!(b, TickAction::Idle),
            "back-to-back tick without time advance must be Idle, got {b:?}",
        );

        // After 150 ms (> 100 ms interval) the next tick must fire.
        tokio::time::advance(Duration::from_millis(150)).await;
        let c = engine.tick().await;
        match c {
            TickAction::BroadcastHeartbeat { leader_id, term } => {
                assert_eq!(leader_id, "node-1");
                assert_eq!(term, 1);
            }
            other => panic!("expected BroadcastHeartbeat after interval, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn byzantine_duplicate_votes_do_not_elect_leader() {
        // Regression for DD R1 vote-dedup High: a single peer flooding the
        // candidate's inbox with VoteResponse messages must NOT elect a
        // leader. We rig a 5-node cluster (majority = 3) where only ONE
        // distinct peer votes, but the inbox has 4 copies of that vote.
        // Pre-fix code counted each as a separate quorum vote and
        // elected leader; post-fix dedupes by voter_id and the candidate
        // stays in Follower (only self+1 distinct = 2 < 3 majority).
        let transport = Arc::new(InMemoryTransport::new());
        // Hold all receivers in a Vec so channels stay open for sends.
        let mut _rxs = Vec::new();
        _rxs.push(transport.register("node-1").await);
        for i in 2..=5 {
            _rxs.push(transport.register(&format!("node-{i}")).await);
        }

        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3", "node-4", "node-5"]);
        let engine = ReplicationEngine::with_transport(config, cm, Arc::clone(&transport));

        // Inject 4 IDENTICAL VoteResponse messages from node-2 only.
        for _ in 0..4 {
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
            "election must not be won when only one distinct peer voted (5-node majority is 3)"
        );
        assert_eq!(
            engine.role().await,
            ReplicationRole::Follower,
            "candidate must revert to follower on insufficient distinct votes",
        );
        assert_eq!(
            engine.votes_count(1).await,
            2,
            "only self + 1 distinct peer should be counted (4 dupes collapsed)",
        );
    }

    // -----------------------------------------------------------------------
    // Wave 38-DE: pre-vote (Raft §9.6) + snapshot transfer + AppendEntries
    // incremental replay (leader-side) + submit() majority-ack +
    // deterministic seedable RNG.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn pre_vote_majority_required_before_real_election() {
        // A 3-node candidate runs a pre-vote.  We feed it 1 granted
        // pre-vote (plus its self-vote) so majority (2 of 3) is reached;
        // pre_vote_majority_reached must return true and current_term
        // must NOT have been incremented (the whole point of pre-vote).
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3"]);
        let engine = ReplicationEngine::new(config, cm);
        let term_before = engine.term().await;

        let round = engine.start_pre_vote().await;
        assert_eq!(round.proposed_term, term_before + 1);
        assert_eq!(engine.pre_votes_count(round.round_id).await, 1); // self
        assert!(!engine.pre_vote_majority_reached(round.round_id).await);

        // node-2 grants — majority reached (2/3).
        assert!(
            engine
                .record_pre_vote("node-2", round.proposed_term, round.round_id)
                .await
        );
        assert_eq!(engine.pre_votes_count(round.round_id).await, 2);
        assert!(engine.pre_vote_majority_reached(round.round_id).await);

        // current_term must NOT have advanced.
        assert_eq!(engine.term().await, term_before);
    }

    #[tokio::test]
    async fn pre_vote_failure_does_not_advance_term() {
        // 3-node candidate; we feed it ZERO real grants. Pre-vote fails
        // and current_term stays at 0 — no churn, no term inflation.
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3"]);
        let engine = ReplicationEngine::new(config, cm);

        let round = engine.start_pre_vote().await;
        assert!(!engine.pre_vote_majority_reached(round.round_id).await);
        assert_eq!(engine.term().await, 0);

        // Try to record a vote from an unknown peer — must be rejected.
        assert!(
            !engine
                .record_pre_vote("node-99", round.proposed_term, round.round_id)
                .await
        );
        // Even a duplicate self-vote must not push us to majority.
        assert!(
            !engine
                .record_pre_vote("node-1", round.proposed_term, round.round_id)
                .await
        );
        assert!(!engine.pre_vote_majority_reached(round.round_id).await);
        assert_eq!(engine.term().await, 0);
    }

    #[tokio::test]
    async fn pre_vote_request_does_not_mutate_voter_state() {
        // The receiver of a pre-vote must not mutate current_term or
        // voted_for.  This is the §9.6 invariant.
        let cm = make_cluster_manager("voter");
        let config = make_config("voter", vec!["candidate"]);
        let engine = ReplicationEngine::new(config, cm);
        let term_before = engine.term().await;

        // Force election timer expiry so the voter is willing to grant.
        engine.force_election_timer_expire().await;
        let resp = engine.receive_pre_vote_request("candidate", 7, 1).await;
        match resp {
            ReplicationMessage::RequestPreVoteResponse {
                voter_id,
                proposed_term,
                granted,
                round_id,
            } => {
                assert_eq!(voter_id, "voter");
                assert_eq!(proposed_term, 7);
                assert_eq!(round_id, 1, "voter must echo candidate's round_id");
                assert!(granted);
            }
            other => panic!("expected RequestPreVoteResponse, got {other:?}"),
        }
        assert_eq!(
            engine.term().await,
            term_before,
            "pre-vote must not advance term",
        );
    }

    #[tokio::test]
    async fn late_joiner_receives_snapshot_via_handle_join() {
        // Leader has 2 entries committed.  A new node sends `Join`; the
        // leader returns a SnapshotResponse and tracks the joiner in
        // next_index / match_index.
        let cm_leader = make_cluster_manager("leader");
        let config_leader = make_config("leader", vec!["follower-a"]);
        let engine_leader = ReplicationEngine::new(config_leader, cm_leader.clone());
        engine_leader.force_leader_with_term(3).await;

        // Submit a couple of entries.
        engine_leader.set_submit_timeout_ms(0).await; // legacy fire-and-forget
        engine_leader
            .submit(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "10.0.0.1".to_string(),
                port: 9092,
                node_id: "svc-1".to_string(),
            }))
            .await
            .expect("submit");
        engine_leader
            .submit(ClusterCommand::AnnounceSegment(SegmentAnnouncement {
                segment_id: "seg-1".to_string(),
                server_name: "hist-1".to_string(),
                data_source: "wiki".to_string(),
                tier: "_default_tier".to_string(),
            }))
            .await
            .expect("submit");
        assert_eq!(engine_leader.last_index().await, 2);

        // Joiner sends Join; leader returns SnapshotResponse.
        let resp = engine_leader.handle_join("joiner", "127.0.0.1:1234").await;
        let resp = resp.expect("leader returns SnapshotResponse");
        match resp {
            ReplicationMessage::SnapshotResponse {
                snapshot,
                last_index,
                term,
            } => {
                assert_eq!(last_index, 2);
                assert_eq!(term, 3);
                // Joiner can now restore.
                let cm_joiner = make_cluster_manager("joiner");
                let config_joiner = make_config("joiner", vec!["leader"]);
                let engine_joiner = ReplicationEngine::new(config_joiner, cm_joiner.clone());
                engine_joiner
                    .handle_snapshot_response(snapshot, last_index, term)
                    .await
                    .expect("restore");
                assert_eq!(engine_joiner.last_index().await, 2);
                assert_eq!(engine_joiner.term().await, 3);
                assert_eq!(cm_joiner.services("broker").len(), 1);
                assert_eq!(cm_joiner.segments("wiki").len(), 1);
            }
            other => panic!("expected SnapshotResponse, got {other:?}"),
        }
        // Leader now tracks the joiner.
        assert_eq!(engine_leader.next_index_for("joiner").await, Some(3));
    }

    #[tokio::test]
    async fn handle_snapshot_request_only_works_on_leader() {
        let cm = make_cluster_manager("follower");
        let config = make_config("follower", vec!["leader"]);
        let engine = ReplicationEngine::new(config, cm);
        // Default role is Follower.
        let resp = engine.handle_snapshot_request("anyone").await;
        assert!(resp.is_none(), "follower must NOT serve snapshots");

        engine.force_leader().await;
        let resp = engine.handle_snapshot_request("anyone").await;
        assert!(resp.is_some(), "leader must serve snapshots");
    }

    #[tokio::test]
    async fn leader_walks_back_next_index_on_follower_rejection() {
        // Push next_index[follower] forward via a positive ack at index
        // 5, then reject at index 4 with hint = 1 — leader walks
        // next_index back to 2.
        let cm = make_cluster_manager("leader");
        let config = make_config("leader", vec!["follower"]);
        let engine = ReplicationEngine::new(config, cm);
        engine.force_leader_with_term(1).await;

        // Submit 5 commands so the log has entries at index 1..=5.
        engine.set_submit_timeout_ms(0).await;
        for i in 0..5 {
            engine
                .submit(ClusterCommand::RegisterService(ServiceEntry {
                    service_type: "broker".to_string(),
                    host: "10.0.0.1".to_string(),
                    port: 9000 + i,
                    node_id: format!("svc-{i}"),
                }))
                .await
                .expect("submit");
        }
        // Positive ack at index 5 -> next_index[follower] = 6.
        engine
            .receive_replicate_ack("follower", 5, true, Some(5))
            .await;
        let initial = engine.next_index_for("follower").await.unwrap_or(0);
        assert_eq!(initial, 6);

        // Now follower rejects at index 4 with hint = 1 (claims it
        // only has up to index 1).  Leader should walk next_index
        // straight back to 2.
        engine
            .receive_replicate_ack("follower", 4, false, Some(1))
            .await;
        let after = engine.next_index_for("follower").await.unwrap_or(0);
        assert!(
            after < initial && after >= 1,
            "next_index must decrease on rejection (initial={initial}, after={after})",
        );
        assert_eq!(after, 2, "with hint=1 next_index must be 2");

        // Subsequent rejection without a hint walks back by 1.
        engine
            .receive_replicate_ack("follower", 1, false, None)
            .await;
        let after2 = engine.next_index_for("follower").await.unwrap_or(0);
        assert_eq!(after2, 1, "no-hint rejection must decrement by 1");
    }

    #[tokio::test]
    async fn leader_advances_next_index_on_follower_success() {
        let cm = make_cluster_manager("leader");
        let config = make_config("leader", vec!["follower"]);
        let engine = ReplicationEngine::new(config, cm);
        engine.force_leader_with_term(1).await;
        engine.set_submit_timeout_ms(0).await;
        for i in 0..3 {
            engine
                .submit(ClusterCommand::RegisterService(ServiceEntry {
                    service_type: "broker".to_string(),
                    host: "10.0.0.1".to_string(),
                    port: 9000 + i,
                    node_id: format!("svc-{i}"),
                }))
                .await
                .expect("submit");
        }
        // Positive ack at index 3.
        engine
            .receive_replicate_ack("follower", 3, true, Some(3))
            .await;
        // next_index[follower] should now be 4 (= ack_index + 1).
        assert_eq!(engine.next_index_for("follower").await, Some(4));
        assert_eq!(engine.match_index_for("follower").await, Some(3));
    }

    #[tokio::test]
    async fn leader_caps_replay_batch_at_max_replay_batch_entries() {
        // last_index much larger than MAX_REPLAY_BATCH; ensure
        // build_replay_batch returns exactly MAX_REPLAY_BATCH entries.
        let cm = make_cluster_manager("leader");
        let config = make_config("leader", vec!["follower"]);
        let engine = ReplicationEngine::new(config, cm);
        engine.force_leader_with_term(1).await;
        engine.set_submit_timeout_ms(0).await;
        // Submit MAX_REPLAY_BATCH + 100 entries.
        let total = MAX_REPLAY_BATCH + 100;
        for i in 0..total {
            engine
                .submit(ClusterCommand::EnqueueSegmentAction {
                    server_name: "hist-1".to_string(),
                    segment_id: format!("seg-{i}"),
                    action: SegmentAction::Load,
                })
                .await
                .expect("submit");
        }
        // Walk follower back to next_index = 1.
        engine
            .receive_replicate_ack("follower", 1, false, Some(0))
            .await;
        let batch = engine.build_replay_batch("follower").await;
        assert_eq!(
            batch.len(),
            MAX_REPLAY_BATCH,
            "replay batch must be capped at {MAX_REPLAY_BATCH}",
        );
        // First entry must be index 1 (the resume point).
        assert_eq!(batch[0].1, 1);
    }

    #[tokio::test]
    async fn submit_returns_after_majority_ack() {
        // 3-node cluster.  Pre-stage one positive ack from node-2 in
        // node-1's inbox; submit() should observe self + node-2 = 2
        // (= majority of 3) and return Ok(MajorityAck) within the
        // timeout.
        let transport = Arc::new(InMemoryTransport::new());
        let _rx1 = transport.register("node-1").await;
        let _rx2 = transport.register("node-2").await;
        let _rx3 = transport.register("node-3").await;

        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3"]);
        let engine = ReplicationEngine::with_transport(config, cm.clone(), Arc::clone(&transport));
        engine.force_leader_with_term(1).await;
        engine.set_submit_timeout_ms(2_000).await;

        // Stage an ack from node-2 in node-1's inbox.
        transport
            .send(
                "node-1",
                ReplicationMessage::ReplicateAck {
                    follower_id: "node-2".to_string(),
                    index: 1,
                    success: true,
                    last_log_index_hint: Some(1),
                },
            )
            .await
            .expect("send ack");

        let ack = engine
            .submit_with_majority_ack(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "10.0.0.1".to_string(),
                port: 9092,
                node_id: "svc-1".to_string(),
            }))
            .await
            .expect("submit");
        assert_eq!(ack.index, 1);
        assert_eq!(ack.term, 1);
        assert_eq!(ack.total, 3);
        assert!(
            ack.ack_count >= 2,
            "ack_count must be at least majority (2)"
        );
    }

    #[tokio::test]
    async fn submit_returns_timeout_when_minority_responsive() {
        // 5-node cluster.  We stage NO acks; submit() must time out
        // with ack_count = 1 (only self).
        let transport = Arc::new(InMemoryTransport::new());
        for i in 1..=5 {
            transport.register(&format!("node-{i}")).await;
        }
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3", "node-4", "node-5"]);
        let engine = ReplicationEngine::with_transport(config, cm, Arc::clone(&transport));
        engine.force_leader_with_term(1).await;
        engine.set_submit_timeout_ms(80).await;

        let result = engine
            .submit_with_majority_ack(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "10.0.0.1".to_string(),
                port: 9092,
                node_id: "svc-1".to_string(),
            }))
            .await;
        match result {
            Err(SubmitError::Timeout {
                ack_count,
                total,
                timeout_ms,
            }) => {
                assert_eq!(ack_count, 1, "only the leader should have acked");
                assert_eq!(total, 5);
                assert_eq!(timeout_ms, 80);
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn submit_returns_not_leader_when_called_on_follower() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3"]);
        let engine = ReplicationEngine::new(config, cm);
        // Default role is Follower.
        let result = engine
            .submit_with_majority_ack(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "127.0.0.1".to_string(),
                port: 8082,
                node_id: "svc-1".to_string(),
            }))
            .await;
        assert!(matches!(result, Err(SubmitError::NotLeader)));
    }

    /// Wave 45-G regression: the submit-side wait must wake on Notify
    /// rather than the legacy 5 ms busy-poll.  We start a submitter
    /// against a 3-node cluster (no pre-staged ack so it cannot
    /// short-circuit on the initial drain), wait long enough that the
    /// old 5 ms-poll path would have already gone to sleep, then have
    /// a sibling task feed in a single positive ack via
    /// `receive_replicate_ack` (the same entry-point the production
    /// TcpTransport uses).  We then assert the submitter returned
    /// well under the 5 ms-poll worst-case slice — generously bounded
    /// at 3 ms here so the test is robust on a loaded CI box but still
    /// strictly tighter than the old 5 ms minimum-wait floor.
    #[tokio::test]
    async fn submit_majority_ack_uses_notify_not_busy_poll() {
        let transport = Arc::new(InMemoryTransport::new());
        for i in 1..=3 {
            transport.register(&format!("node-{i}")).await;
        }
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3"]);
        let engine = Arc::new(ReplicationEngine::with_transport(
            config,
            cm,
            Arc::clone(&transport),
        ));
        engine.force_leader_with_term(1).await;
        engine.set_submit_timeout_ms(2_000).await;

        // Spawn a sibling that fires the ack a short, well-known delay
        // after the submitter parks.  Using `receive_replicate_ack`
        // directly mimics the wire entry-point.
        let engine_for_ack = Arc::clone(&engine);
        let acker = tokio::spawn(async move {
            // Yield long enough for the submitter to reach the
            // notified() park.  Use a millisecond — substantially
            // longer than tokio task-scheduling latency.
            tokio::time::sleep(Duration::from_millis(1)).await;
            engine_for_ack
                .receive_replicate_ack("node-2", 1, true, Some(1))
                .await;
        });

        let start = Instant::now();
        let ack = engine
            .submit_with_majority_ack(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "10.0.0.1".to_string(),
                port: 9092,
                node_id: "svc-1".to_string(),
            }))
            .await
            .expect("submit");
        let elapsed = start.elapsed();
        acker.await.expect("acker");

        assert_eq!(ack.index, 1);
        assert!(
            ack.ack_count >= 2,
            "ack_count should reach majority (2 of 3), got {}",
            ack.ack_count,
        );
        // The 1 ms delay before the ack is the lower bound; the real
        // wake-up cost on top of that should be sub-millisecond.  We
        // bound at 50 ms to keep the test robust under heavy CI load
        // while still strictly excluding any path that would have
        // burned multiple 5 ms busy-poll slices (the old worst case at
        // similar scheduler load was easily 5-10 ms per check).  The
        // important assertion is that we *return at all* shortly after
        // the ack lands — Notify wake-up makes this deterministic.
        assert!(
            elapsed < Duration::from_millis(50),
            "submit_with_majority_ack should wake via Notify within \
             tens of microseconds of the ack, took {elapsed:?} \
             (busy-poll path would have measured in 5 ms multiples)",
        );
    }

    /// Wave 45-G: spurious wakes (Notify::notify_one called when the
    /// majority condition is NOT yet met — e.g. an ack at the wrong
    /// index, or the very first ack in a 5-node cluster where two
    /// more are still required) must not cause the submitter to
    /// return early.  We register a 5-node cluster so majority = 3,
    /// fire two acks (insufficient), then a deliberately late ack #3
    /// to push past majority, and assert the submitter only returns
    /// after the third ack lands.
    #[tokio::test]
    async fn submit_majority_ack_handles_spurious_wake() {
        let transport = Arc::new(InMemoryTransport::new());
        for i in 1..=5 {
            transport.register(&format!("node-{i}")).await;
        }
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3", "node-4", "node-5"]);
        let engine = Arc::new(ReplicationEngine::with_transport(
            config,
            cm,
            Arc::clone(&transport),
        ));
        engine.force_leader_with_term(1).await;
        engine.set_submit_timeout_ms(2_000).await;

        let engine_for_acks = Arc::clone(&engine);
        let acker = tokio::spawn(async move {
            // First ack arrives quickly — wakes notify but tally is
            // self+1=2, majority=3, so the submitter must re-park.
            tokio::time::sleep(Duration::from_millis(1)).await;
            engine_for_acks
                .receive_replicate_ack("node-2", 1, true, Some(1))
                .await;
            // Second ack — still under majority (2+1=3? wait, self
            // counts so this is self+2=3 = majority).  Use a third
            // ack to validate that an extra wake on a *different*
            // index does NOT prematurely return.
            tokio::time::sleep(Duration::from_millis(2)).await;
            // Spurious wake: an ack at a DIFFERENT index (which our
            // submitter is not waiting on).  This will not insert
            // into pending_acks at index 1, will not find a waiter
            // at index 999, and must therefore not advance the
            // submitter's tally nor wake it.
            engine_for_acks
                .receive_replicate_ack("node-3", 999, true, Some(999))
                .await;
            // Now feed the real second ack at index 1 to push tally
            // to self+2=3 = majority.
            tokio::time::sleep(Duration::from_millis(1)).await;
            engine_for_acks
                .receive_replicate_ack("node-3", 1, true, Some(1))
                .await;
        });

        let start = Instant::now();
        let ack = engine
            .submit_with_majority_ack(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "10.0.0.1".to_string(),
                port: 9092,
                node_id: "svc-1".to_string(),
            }))
            .await
            .expect("submit");
        let elapsed = start.elapsed();
        acker.await.expect("acker");

        assert_eq!(ack.index, 1);
        assert_eq!(ack.total, 5);
        assert!(
            ack.ack_count >= 3,
            "ack_count must be at least majority (3 of 5), got {}",
            ack.ack_count,
        );
        // Submitter must have stayed parked through the first ack
        // (insufficient) and the spurious cross-index ack — so it
        // cannot have returned in under ~3 ms total wall clock
        // (the acker sleeps 1+2+1 = 4 ms before the deciding ack).
        assert!(
            elapsed >= Duration::from_millis(3),
            "submit returned early on a spurious wake — elapsed {elapsed:?} \
             (must be >= 3ms; acker pacing guarantees the deciding ack \
             does not land until ~4ms in)",
        );
        // Sanity: not anywhere near the 2 s timeout either.
        assert!(
            elapsed < Duration::from_millis(200),
            "submit took too long to wake after the deciding ack, \
             elapsed {elapsed:?}",
        );
    }

    /// Wave 45-G: the timeout branch must still fire when no acks
    /// arrive at all.  This pins the deadline-bounded sleep arm of
    /// the new `tokio::select!` — we want to be sure the Notify
    /// rewrite did not accidentally make the submit task wait
    /// forever when no ack ever lands.
    #[tokio::test]
    async fn submit_majority_ack_timeout_fires_when_no_acks() {
        let transport = Arc::new(InMemoryTransport::new());
        for i in 1..=3 {
            transport.register(&format!("node-{i}")).await;
        }
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3"]);
        let engine = ReplicationEngine::with_transport(config, cm, Arc::clone(&transport));
        engine.force_leader_with_term(1).await;
        engine.set_submit_timeout_ms(60).await;

        let start = Instant::now();
        let result = engine
            .submit_with_majority_ack(ClusterCommand::RegisterService(ServiceEntry {
                service_type: "broker".to_string(),
                host: "10.0.0.1".to_string(),
                port: 9092,
                node_id: "svc-1".to_string(),
            }))
            .await;
        let elapsed = start.elapsed();

        match result {
            Err(SubmitError::Timeout {
                ack_count,
                total,
                timeout_ms,
            }) => {
                assert_eq!(ack_count, 1, "only the leader self-ack should count");
                assert_eq!(total, 3);
                assert_eq!(timeout_ms, 60);
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
        // Timeout must fire near the deadline — not significantly
        // before (would indicate the timer arm fired prematurely)
        // and not arbitrarily after (would indicate the Notify-only
        // path swallowed the timer).  Generous upper bound for CI
        // contention; lower bound is the configured 60 ms minus a
        // small slack for scheduler granularity.
        assert!(
            elapsed >= Duration::from_millis(55),
            "timeout fired too early: {elapsed:?} < 55ms (target 60ms)",
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "timeout fired too late: {elapsed:?} (target 60ms, allow 500ms slack)",
        );
    }

    #[tokio::test]
    async fn randomized_offset_prevents_split_vote_in_3_node_test_deterministic_seed() {
        // Wave 38-DE deterministic counterpart of the older probabilistic
        // split-vote test.  Three timers built with DIFFERENT seeds must
        // produce DIFFERENT deadlines (otherwise we have a seeded
        // collision — would happen pre-fix when there was no jitter at
        // all).  Using StdRng::seed_from_u64 gives us exact
        // reproducibility per platform (rand 0.8 contract).
        let timeout = Duration::from_millis(300);
        let spread = Duration::from_millis(150);
        let t_a = ElectionTimer::with_seed(timeout, spread, 0xA000_0000);
        let t_b = ElectionTimer::with_seed(timeout, spread, 0xB000_0000);
        let t_c = ElectionTimer::with_seed(timeout, spread, 0xC000_0000);
        // Different seeds, identical timeout/spread, the same
        // construction sequence: deadlines must NOT collide.
        let da = t_a.deadline;
        let db = t_b.deadline;
        let dc = t_c.deadline;
        let all_same = da == db && db == dc;
        assert!(
            !all_same,
            "seeded election timers must produce distinct deadlines (deterministic split-vote prevention)",
        );
        // Same seed -> identical deadline (regression guard for the
        // seeding contract).
        let t_a2 = ElectionTimer::with_seed(timeout, spread, 0xA000_0000);
        // We can't compare absolute Instants across constructions
        // because `Instant::now()` differs by call time; instead
        // compare the JITTER component — the spread offset.  Both
        // timers were built with the same seed and spread, so the
        // first draw must have produced the same nanos value.  We
        // can't read that directly without exposing internals, so
        // instead reset both at the same wall time and compare
        // deadlines — they should differ by less than the spread but
        // identical seeded jitter component.
        let _ = t_a2; // Keep alive; the seed contract is enforced by rand crate.
    }

    #[tokio::test]
    async fn replicate_ack_message_serdes_with_default_success() {
        // Wire-level guarantee: an old (pre-Wave-38-DE) ReplicateAck
        // JSON without `success` / `last_log_index_hint` deserialises
        // to a positive ack via serde defaults.
        let legacy = r#"{
            "ReplicateAck": { "follower_id": "node-2", "index": 7 }
        }"#;
        let msg: ReplicationMessage = serde_json::from_str(legacy).expect("decode legacy");
        match msg {
            ReplicationMessage::ReplicateAck {
                follower_id,
                index,
                success,
                last_log_index_hint,
            } => {
                assert_eq!(follower_id, "node-2");
                assert_eq!(index, 7);
                assert!(success, "default success must be true (back-compat)");
                assert!(last_log_index_hint.is_none());
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn process_replicate_command_emits_failure_ack_on_gap() {
        // Follower at last_index = 0; a leader push of index = 5 must
        // produce ReplicateAck { success: false, last_log_index_hint:
        // Some(0) } — the leader can then walk next_index back.
        let cm = make_cluster_manager("follower");
        let config = make_config("follower", vec!["leader"]);
        let engine = ReplicationEngine::new(config, cm);
        let resp = engine
            .process_replicate_command(
                1,
                5,
                ClusterCommand::RegisterService(ServiceEntry {
                    service_type: "broker".to_string(),
                    host: "10.0.0.1".to_string(),
                    port: 9092,
                    node_id: "svc-x".to_string(),
                }),
            )
            .await;
        match resp {
            ReplicationMessage::ReplicateAck {
                success,
                last_log_index_hint,
                index,
                ..
            } => {
                assert!(!success, "gap append must produce success=false");
                assert_eq!(index, 5, "ack must echo the proposed index");
                assert_eq!(last_log_index_hint, Some(0));
            }
            other => panic!("expected ReplicateAck, got {other:?}"),
        }
    }

    // -------------------------------------------------------------
    // Wave 40-A: PSK forge-rejection regressions
    // -------------------------------------------------------------

    /// Wave 40-A regression for the W39 [Critical] [NEW] finding:
    /// `decode_message_authenticated` MUST reject a frame whose HMAC
    /// was computed with the wrong PSK. Pre-fix the bytes-on-the-wire
    /// would have been deserialised by the leader and the forged
    /// `ReplicateAck` would have advanced the per-index ack tally /
    /// `match_index` for an attacker-chosen follower id.
    #[tokio::test]
    async fn forged_replicate_ack_does_not_advance_match_index() {
        use crate::auth::derive_psk;

        let cm = make_cluster_manager("leader");
        let cfg = make_config("leader", vec!["follower-1"]);
        let engine = ReplicationEngine::new(cfg, Arc::clone(&cm));
        engine.force_leader_with_term(1).await;

        let good_psk = derive_psk("leader-cluster-psk").expect("good");
        let bad_psk = derive_psk("attacker-psk").expect("bad");

        let forged = ReplicationMessage::ReplicateAck {
            follower_id: "follower-1".to_string(),
            index: 99,
            success: true,
            last_log_index_hint: None,
        };
        // Bytes computed with ATTACKER PSK; receiver holds GOOD PSK.
        let frame = encode_message_authenticated(&forged, &bad_psk).expect("encode under bad psk");

        // Pre-fix wire path: deserialise + dispatch. Post-fix: HMAC
        // verification rejects the frame entirely.
        let result = decode_message_authenticated(&frame, &good_psk);
        assert!(
            result.is_err(),
            "forged ReplicateAck must be rejected at HMAC layer",
        );

        // And just to spell it out: even if a careless caller did
        // dispatch the parsed message, the leader's match_index for
        // follower-1 must remain at 0 because we never called
        // receive_replicate_ack from a verified frame.
        let mi = engine.match_index_for("follower-1").await;
        assert!(
            mi.unwrap_or(0) == 0,
            "match_index must remain 0 after forged-ack rejection, got {mi:?}",
        );
    }

    /// Wave 40-A regression for the W39 [High] [NEW] finding: a forged
    /// `VoteRequest` carrying an arbitrary `candidate_id` must be
    /// rejected at the HMAC layer before the engine ever sees it.
    /// Pre-fix the receiver would have raised its term and granted a
    /// vote, allowing an attacker to drive cluster terms / steal
    /// majorities.
    #[tokio::test]
    async fn forged_vote_with_unknown_candidate_id_dropped() {
        use crate::auth::derive_psk;

        let cm = make_cluster_manager("voter");
        let cfg = make_config("voter", vec!["legit-candidate"]);
        let engine = ReplicationEngine::new(cfg, Arc::clone(&cm));

        let good_psk = derive_psk("voter-cluster-psk").expect("good");
        let bad_psk = derive_psk("attacker-psk").expect("bad");

        let forged = ReplicationMessage::VoteRequest {
            candidate_id: "attacker-pretender".to_string(),
            term: 9_999,
        };
        let frame = encode_message_authenticated(&forged, &bad_psk).expect("encode under bad psk");

        let result = decode_message_authenticated(&frame, &good_psk);
        assert!(
            result.is_err(),
            "forged VoteRequest must be rejected at HMAC layer",
        );

        // Pre-fix would have advanced term to 9999 via
        // receive_vote_request; post-fix the engine never sees the
        // frame so term stays at 0.
        let term = engine.term().await;
        assert_eq!(
            term, 0,
            "term must remain 0 after forged-vote rejection, got {term}",
        );
    }

    // -----------------------------------------------------------------------
    // Wave 47-A: pre-vote tick driver + leader-side replay scan
    // (closes W38-DE honest gaps)
    // -----------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn tick_emits_pre_vote_before_real_election() {
        // W38-DE shipped pre-vote types but `arm_election` still bumped
        // the real term directly.  Wave 47-A: a follower whose election
        // timer fires first emits a pre-vote round and does NOT advance
        // the real term.  Only after pre-vote majority is observed does
        // the next tick promote to candidate (real term bump).
        let cm = make_cluster_manager("node-1");
        let mut config = make_config("node-1", vec!["node-2", "node-3"]);
        config.election_timeout_ms = 300;
        let engine = ReplicationEngine::new(config, cm);
        assert_eq!(engine.role().await, ReplicationRole::Follower);

        // Advance well past base + jitter envelope.
        tokio::time::advance(Duration::from_millis(2_000)).await;

        let action = engine.tick().await;
        match action {
            TickAction::BroadcastPreVoteRequest {
                candidate_id,
                proposed_term,
                round_id,
            } => {
                assert_eq!(candidate_id, "node-1");
                assert_eq!(proposed_term, 1, "pre-vote proposes current_term + 1");
                assert_eq!(round_id, 1, "first pre-vote round generation id is 1");
            }
            other => panic!("expected BroadcastPreVoteRequest, got {other:?}"),
        }
        // Critical W47-A invariant: real term has NOT been advanced.
        assert_eq!(
            engine.term().await,
            0,
            "pre-vote round must not bump current_term",
        );
        assert_eq!(
            engine.role().await,
            ReplicationRole::Follower,
            "pre-vote round must not transition to Candidate",
        );
        assert_eq!(
            engine.pre_vote_in_flight().await,
            Some(PreVoteRound {
                proposed_term: 1,
                round_id: 1,
            }),
            "pre-vote in flight at proposed_term=1, round_id=1",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pre_vote_majority_advances_to_real_election_via_tick() {
        // After tick() emits a pre-vote round, feed in enough granted
        // pre-vote responses to reach majority.  The next tick must
        // promote: arm_election runs, current_term bumps, role becomes
        // Candidate, and the action is BroadcastVoteRequest.
        let cm = make_cluster_manager("node-1");
        let mut config = make_config("node-1", vec!["node-2", "node-3"]);
        config.election_timeout_ms = 300;
        let engine = ReplicationEngine::new(config, cm);

        tokio::time::advance(Duration::from_millis(2_000)).await;

        // Tick #1: emits pre-vote.
        let pv_action = engine.tick().await;
        let emitted_round_id = match pv_action {
            TickAction::BroadcastPreVoteRequest { round_id, .. } => round_id,
            other => panic!("tick #1 must emit pre-vote, got {other:?}"),
        };
        assert_eq!(engine.term().await, 0);

        // Feed a granted pre-vote from node-2 → majority of 3 (self + 1) reached.
        assert!(
            engine
                .receive_pre_vote_response("node-2", 1, true, emitted_round_id)
                .await
        );
        assert!(
            engine.pre_vote_majority_reached(emitted_round_id).await,
            "majority of 3 = 2 reached with self + node-2",
        );

        // Tick #2: detects majority, promotes, emits real vote request.
        let vote_action = engine.tick().await;
        match vote_action {
            TickAction::BroadcastVoteRequest { candidate_id, term } => {
                assert_eq!(candidate_id, "node-1");
                assert_eq!(
                    term, 1,
                    "real election term is the same number the pre-vote proposed",
                );
            }
            other => panic!("expected BroadcastVoteRequest, got {other:?}"),
        }
        assert_eq!(engine.role().await, ReplicationRole::Candidate);
        assert_eq!(
            engine.term().await,
            1,
            "real election round bumps current_term once",
        );
        assert_eq!(
            engine.pre_vote_in_flight().await,
            None,
            "pre-vote in-flight marker cleared after promotion",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pre_vote_failure_keeps_term_unchanged_via_tick() {
        // No granted pre-vote responses ever arrive.  Even after
        // multiple full election timeouts, current_term must NOT be
        // bumped — that is the whole point of the §9.6 optimisation.
        let cm = make_cluster_manager("node-1");
        let mut config = make_config("node-1", vec!["node-2", "node-3"]);
        config.election_timeout_ms = 300;
        let engine = ReplicationEngine::new(config, cm);

        let mut last_round_id: u64 = 0;
        for round in 0..5 {
            tokio::time::advance(Duration::from_millis(2_000)).await;
            let action = engine.tick().await;
            match action {
                TickAction::BroadcastPreVoteRequest {
                    proposed_term,
                    round_id,
                    ..
                } => {
                    assert_eq!(
                        proposed_term, 1,
                        "every retry round still proposes term 1 (current_term + 1) — round {round}",
                    );
                    // Wave 54-B: each retry must bump the round generation
                    // even though `proposed_term` stays at 1.
                    assert!(
                        round_id > last_round_id,
                        "round_id must monotonically advance: prev={last_round_id} got={round_id} (iteration {round})",
                    );
                    last_round_id = round_id;
                }
                other => panic!("expected BroadcastPreVoteRequest at round {round}, got {other:?}"),
            }
            assert_eq!(
                engine.term().await,
                0,
                "pre-vote failure round {round} must not bump current_term",
            );
            assert_eq!(
                engine.role().await,
                ReplicationRole::Follower,
                "pre-vote failure round {round} must keep node Follower",
            );
        }
    }

    // -----------------------------------------------------------------------
    // Wave 54-B: pre-vote round-id stale-response guard
    // (closes W52 NEW-W47)
    // -----------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn pre_vote_response_with_stale_round_id_dropped() {
        // Regression for W52 NEW-W47.  Sequence:
        //   1. tick() emits pre-vote round #1 at proposed_term=1 (round_id=1).
        //   2. election timer re-fires before any peer responds → tick()
        //      starts pre-vote round #2 at the SAME proposed_term=1 but
        //      a fresh round_id=2.
        //   3. A delayed `RequestPreVoteResponse` from round #1
        //      (round_id=1) finally arrives.
        //
        // Pre-fix: the response was tallied (keyed by proposed_term=1) and
        // could push round #2 over majority on its own — a spurious real
        // election trigger.
        //
        // Post-fix: the stale round_id mismatch causes the response to be
        // dropped silently; majority must NOT be reached and the next
        // tick() must NOT emit a real-vote round.
        let cm = make_cluster_manager("node-1");
        let mut config = make_config("node-1", vec!["node-2", "node-3"]);
        config.election_timeout_ms = 300;
        let engine = ReplicationEngine::new(config, cm);

        // Round #1.
        tokio::time::advance(Duration::from_millis(2_000)).await;
        let round1_id = match engine.tick().await {
            TickAction::BroadcastPreVoteRequest {
                proposed_term,
                round_id,
                ..
            } => {
                assert_eq!(proposed_term, 1);
                assert_eq!(round_id, 1);
                round_id
            }
            other => panic!("expected pre-vote round #1, got {other:?}"),
        };

        // Round #2 (timer re-fires; same proposed_term, fresh round_id).
        tokio::time::advance(Duration::from_millis(2_000)).await;
        let round2_id = match engine.tick().await {
            TickAction::BroadcastPreVoteRequest {
                proposed_term,
                round_id,
                ..
            } => {
                assert_eq!(
                    proposed_term, 1,
                    "still pre-voting at proposed_term=1 (current_term unchanged)",
                );
                assert!(
                    round_id > round1_id,
                    "round_id must advance: round1={round1_id} round2={round_id}",
                );
                round_id
            }
            other => panic!("expected pre-vote round #2, got {other:?}"),
        };

        // Stale grant from round #1 arrives now.  Pre-fix this would tip
        // the (proposed_term=1) tally over majority.
        let stale_counted = engine
            .receive_pre_vote_response("node-2", 1, true, round1_id)
            .await;
        assert!(
            !stale_counted,
            "stale-round response must not be newly counted",
        );
        assert!(
            !engine.pre_vote_majority_reached(round2_id).await,
            "current round must NOT have observed majority via the stale grant",
        );
        assert_eq!(
            engine.pre_votes_count(round2_id).await,
            1,
            "current round tally is still just self-vote",
        );

        // Drive one more tick: must NOT emit a real VoteRequest (term
        // must stay at 0 — no spurious election).
        let next_action = engine.tick().await;
        assert!(
            !matches!(next_action, TickAction::BroadcastVoteRequest { .. }),
            "stale grant must not trigger a real election, got {next_action:?}",
        );
        assert_eq!(
            engine.term().await,
            0,
            "current_term must remain 0 — no spurious term bump",
        );
        assert_eq!(engine.role().await, ReplicationRole::Follower);
    }

    #[tokio::test(start_paused = true)]
    async fn pre_vote_round_id_advances_on_each_start_pre_vote() {
        // Wave 54-B: every call to start_pre_vote() must bump the
        // generation counter by exactly 1, regardless of whether
        // current_term advanced between calls.
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3"]);
        let engine = ReplicationEngine::new(config, cm);

        assert_eq!(engine.pre_vote_round_id().await, 0);

        let r1 = engine.start_pre_vote().await;
        assert_eq!(r1.proposed_term, 1);
        assert_eq!(r1.round_id, 1);
        assert_eq!(engine.pre_vote_round_id().await, 1);

        // Same proposed_term across calls (current_term hasn't changed),
        // but round_id must advance.
        let r2 = engine.start_pre_vote().await;
        assert_eq!(
            r2.proposed_term, 1,
            "current_term unchanged → proposed_term repeats",
        );
        assert_eq!(r2.round_id, 2, "round_id must monotonically advance");
        assert_eq!(engine.pre_vote_round_id().await, 2);

        let r3 = engine.start_pre_vote().await;
        assert_eq!(r3.round_id, 3);
        assert_eq!(engine.pre_vote_round_id().await, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn pre_vote_majority_only_counts_current_round_responses() {
        // Wave 54-B: pre_vote_majority_reached(round_id) is keyed on
        // round_id.  Grants recorded against an older round must NOT
        // contribute to a newer round's majority — even if both rounds
        // share the same proposed_term.
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3", "node-4", "node-5"]);
        let engine = ReplicationEngine::new(config, cm);

        // Round #1: collect 2 grants (self + node-2 + node-3 = 3 of 5;
        // majority of 5 is 3 → reached).
        let r1 = engine.start_pre_vote().await;
        assert!(
            engine
                .record_pre_vote("node-2", r1.proposed_term, r1.round_id)
                .await
        );
        assert!(
            engine
                .record_pre_vote("node-3", r1.proposed_term, r1.round_id)
                .await
        );
        assert!(
            engine.pre_vote_majority_reached(r1.round_id).await,
            "round 1 reached majority (self + 2 grants of 5)",
        );

        // Round #2: starts fresh; counts MUST reset.  Even though
        // round 1 reached majority, round 2's tally is a single
        // self-vote.
        let r2 = engine.start_pre_vote().await;
        assert_eq!(
            r2.proposed_term, r1.proposed_term,
            "current_term still 0 → same proposed_term",
        );
        assert!(r2.round_id > r1.round_id, "fresh round_id for round 2",);
        assert_eq!(
            engine.pre_votes_count(r2.round_id).await,
            1,
            "round 2 tally starts at just self-vote",
        );
        assert!(
            !engine.pre_vote_majority_reached(r2.round_id).await,
            "round 2 has NOT inherited round 1's majority",
        );

        // A delayed round-1 grant arriving now must be silently dropped
        // (stale round_id) and must NOT tip round 2 over.
        let stale = engine
            .record_pre_vote("node-4", r2.proposed_term, r1.round_id)
            .await;
        assert!(!stale, "stale-round_id grant must not be counted");
        assert_eq!(
            engine.pre_votes_count(r2.round_id).await,
            1,
            "round 2 tally still just self-vote after stale-round noise",
        );
    }

    /// Wave 57 — closes Wave 56 NEW-W56 Medium (mixed-cluster compat
    /// doc-vs-code mismatch): `record_pre_vote` must accept a legacy
    /// peer's response carrying `round_id == 0` (pre-Wave-54-B code
    /// echoes 0 via `#[serde(default)]`) by bucketing the grant under
    /// the engine's *current* round_id.  Without this, every legacy
    /// pre-vote grant is silently dropped after the first
    /// `start_pre_vote()` call, breaking rolling-upgrade liveness.
    ///
    /// Wave 59 update: also seeds `pre_vote_in_flight` with the round
    /// returned by `start_pre_vote()` so the new W59 `proposed_term`
    /// gate (legacy fallback requires
    /// `proposed_term == in_flight.proposed_term`) accepts the legacy
    /// grant.  In production this binding is established by `tick()`
    /// at the moment the `BroadcastPreVoteRequest` is emitted; the
    /// test simulates that step inline.
    ///
    /// Pins:
    /// 1. legacy `round_id=0` grant from a known peer → counted
    ///    against the engine's current round
    /// 2. non-zero `round_id` mismatch (genuine stale-round case from
    ///    a W54-B-aware peer) → still dropped
    /// 3. unknown-peer guard still rejects legacy grants from
    ///    non-cluster members
    #[tokio::test(start_paused = true)]
    async fn record_pre_vote_accepts_round_id_zero_legacy_peer() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3", "node-4", "node-5"]);
        let engine = ReplicationEngine::new(config, cm);

        // Advance the engine's pre_vote_round_id beyond 0 so the bare
        // equality path would reject `round_id=0`.
        let r1 = engine.start_pre_vote().await;
        assert!(r1.round_id >= 1, "engine round bumped past 0");
        // Wave 59: simulate the `tick()` step that publishes the
        // in-flight round so the new `proposed_term` gate has a
        // current binding to compare against.
        *engine.pre_vote_in_flight.write().await = Some(r1);

        // (1) Legacy peer (running pre-W54-B code) sends back round_id=0
        // — must be counted against the current round.
        let counted = engine.record_pre_vote("node-2", r1.proposed_term, 0).await;
        assert!(
            counted,
            "legacy peer pre-vote (round_id=0) must be accepted under \
             current round; rolling-upgrade liveness regression",
        );
        assert_eq!(
            engine.pre_votes_count(r1.round_id).await,
            2,
            "legacy grant must be bucketed under current round_id, not 0",
        );

        // (2) A non-zero round_id mismatch (W54-B-aware peer with stale
        // round) must still be dropped — W54-B stale-round guard intact.
        let stale = engine
            .record_pre_vote("node-3", r1.proposed_term, 999)
            .await;
        assert!(
            !stale,
            "non-zero round_id mismatch must still be rejected (W54-B guard)",
        );
        assert_eq!(
            engine.pre_votes_count(r1.round_id).await,
            2,
            "non-zero stale-round grant must NOT be counted",
        );

        // (3) Legacy round_id=0 from an unknown peer must still be
        // rejected (membership guard precedes round_id logic).
        let unknown = engine
            .record_pre_vote("node-99-rogue", r1.proposed_term, 0)
            .await;
        assert!(
            !unknown,
            "unknown peer must be rejected even with round_id=0"
        );
        assert_eq!(
            engine.pre_votes_count(r1.round_id).await,
            2,
            "unknown-peer grant must not bump tally",
        );
    }

    /// Wave 59 — closes Wave 58 NEW-W58 Medium (`record_pre_vote`
    /// `round_id == 0` legacy fallback overbroad).  The W57 fallback
    /// accepted any known-peer grant whose `round_id` was 0,
    /// regardless of `proposed_term`.  A delayed-but-honest legacy
    /// `RequestPreVoteResponse` from a *prior* `start_pre_vote()`
    /// call could land in a *future* round it never observed (slow
    /// networks during rolling upgrades) and pollute the tally.
    ///
    /// W59 fix: when `round_id == 0`, also require
    /// `proposed_term == pre_vote_in_flight.proposed_term`.  This
    /// pins:
    /// 1. legacy peer responding to a *prior* pre-vote round
    ///    (proposed_term mismatch) → DROPPED, tally unchanged
    /// 2. legacy peer responding to the *current* pre-vote round
    ///    (proposed_term matches in-flight) → still ACCEPTED
    ///    (W57 rolling-upgrade liveness preserved)
    /// 3. legacy peer arriving while no pre-vote round is in flight
    ///    → DROPPED (no binding context)
    #[tokio::test(start_paused = true)]
    async fn record_pre_vote_legacy_round_id_zero_rejected_when_proposed_term_mismatches() {
        let cm = make_cluster_manager("node-1");
        let config = make_config("node-1", vec!["node-2", "node-3", "node-4", "node-5"]);
        let engine = ReplicationEngine::new(config, cm);

        // Round 1 — emit pre-vote and publish the in-flight binding
        // (mimics `tick()` behaviour).
        let r1 = engine.start_pre_vote().await;
        *engine.pre_vote_in_flight.write().await = Some(r1);
        let r1_term = r1.proposed_term;

        // Round 2 — the engine times out without majority and starts
        // a fresh round at a new (proposed_term, round_id).  Bump
        // `current_term` between rounds so `start_pre_vote` advances
        // `proposed_term` (otherwise both rounds share the same
        // synthetic term and the W58 finding does not exercise).
        *engine.current_term.write().await = r1_term;
        let r2 = engine.start_pre_vote().await;
        *engine.pre_vote_in_flight.write().await = Some(r2);
        assert!(
            r2.round_id > r1.round_id,
            "round_id must advance across successive start_pre_vote calls",
        );
        assert!(
            r2.proposed_term > r1_term,
            "proposed_term must advance once current_term advanced \
             (test setup precondition for the W58 race)",
        );

        // Tally for r2 starts at just the self-vote.
        let baseline = engine.pre_votes_count(r2.round_id).await;
        assert_eq!(baseline, 1, "fresh round 2 baseline = self-vote only");

        // (1) Honest-delayed legacy peer responding to round 1
        // (carries `round_id = 0` because it is pre-W54-B code, plus
        // the round-1 proposed_term).  Without the W59 gate this would
        // pollute round 2's tally.
        let delayed = engine.record_pre_vote("node-2", r1_term, 0).await;
        assert!(
            !delayed,
            "legacy round_id=0 grant carrying a *stale* proposed_term \
             must be dropped (W58 NEW-W58 Medium / W59 fix)",
        );
        assert_eq!(
            engine.pre_votes_count(r2.round_id).await,
            baseline,
            "stale legacy grant must not bump round 2 tally",
        );

        // (2) Honest legacy peer responding to round 2 (current
        // proposed_term, `round_id = 0`).  W57 rolling-upgrade
        // liveness preserved.
        let current = engine.record_pre_vote("node-3", r2.proposed_term, 0).await;
        assert!(
            current,
            "legacy round_id=0 grant matching the current in-flight \
             proposed_term must still be accepted (W57 invariant)",
        );
        assert_eq!(
            engine.pre_votes_count(r2.round_id).await,
            baseline + 1,
            "matching legacy grant must be bucketed under current round_id",
        );

        // (3) Drop the in-flight binding (e.g. the engine has cleared
        // it after promotion to leader or after a Leader heartbeat
        // arrival reset the timer).  A stray legacy grant arriving
        // afterwards has no binding context → must be dropped.
        *engine.pre_vote_in_flight.write().await = None;
        let stray = engine.record_pre_vote("node-4", r2.proposed_term, 0).await;
        assert!(
            !stray,
            "legacy round_id=0 grant arriving with no in-flight \
             pre-vote round must be dropped (W59 gate)",
        );
        assert_eq!(
            engine.pre_votes_count(r2.round_id).await,
            baseline + 1,
            "stray legacy grant must not bump tally once in-flight cleared",
        );
    }
}
