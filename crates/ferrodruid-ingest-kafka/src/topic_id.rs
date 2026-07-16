// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! KIP-516 **topic id** (topic UUID) resolution over the Kafka wire protocol
//! (compat-3 durability, Codex R7 H1).
//!
//! ## Why this exists
//!
//! A topic deleted and recreated under the SAME name on the SAME cluster
//! REUSES the offset space: offset 500 of the new generation names a
//! different record than offset 500 of the old one. A durable resume
//! frontier derived from prior-generation segments would then seek the new
//! topic past records that were never consumed — permanent loss — and when
//! the new generation has already been produced past the frontier the R5-H1
//! watermark clamp cannot catch it (the stale target is IN-range). The only
//! broker-side identity that survives this is the KIP-516 topic id, minted
//! fresh on every topic creation.
//!
//! ## Why a wire probe (and not librdkafka)
//!
//! librdkafka 2.12.1 exposes topic ids ONLY through the Admin
//! `DescribeTopics` API (`rd_kafka_TopicDescription_topic_id`), which the
//! safe `rdkafka` Rust binding (0.37 through 0.39, the latest) does not
//! wrap; reaching it needs raw FFI, unavailable under this workspace's
//! `#![forbid(unsafe_code)]`. The consumer metadata rdkafka does expose
//! (`rd_kafka_metadata`) predates KIP-516 and carries no id (the R31/R36
//! limitation notes). The Kafka protocol itself, however, returns the topic
//! id in every **Metadata v10+** response (Kafka >= 2.8), so this module
//! speaks that one request pair — `ApiVersions v0` to learn the broker's
//! Metadata version ceiling, then `Metadata v10..=v12` for the single
//! subscribed topic — over a plain TCP connection in safe Rust.
//!
//! ## Fail-safe posture
//!
//! Resolution is BEST-EFFORT and every failure degrades to
//! [`TopicIdProbe::Unresolved`] (identity UNRESOLVED), never to a wrong id:
//! unreachable broker, a non-PLAINTEXT listener (TLS/SASL handshakes are not
//! spoken here — see [`plaintext_probe_supported`]), a pre-2.8 broker
//! (Metadata < v10), a zero UUID, a malformed/oversized response, or a
//! timeout. An unresolved identity leaves the resume frontier gated by the
//! CLUSTER identity alone (Codex R8 H2): recreation detection is unavailable
//! for the session, but cluster-confirmed durable spans still advance the
//! resume — the topic id only ever tightens the frontier on a positively
//! detected recreation.
//!
//! ## Multi-broker agreement (Codex R28 H1)
//!
//! A topic delete+recreate propagates its new KIP-516 id broker-by-broker:
//! in the propagation window a LAGGING broker still serves the OLD
//! generation's id. Trusting the FIRST responder can then resolve the OLD
//! id, which MATCHES the durable rows' stamped `topicId` — recreation
//! detection never fires, the resume frontier advances, and with the new
//! log regrown past the stale frontier the consumer seeks FORWARD past
//! new-generation records (permanent loss). The probe therefore queries
//! EVERY bootstrap broker (within the one overall deadline) and returns a
//! tri-state [`TopicIdProbe`]: all responsive brokers agreeing on one
//! non-zero id → [`Agreed`](TopicIdProbe::Agreed); CONFLICTING non-zero ids
//! → [`Disagreed`](TopicIdProbe::Disagreed) (metadata in flux — the caller
//! must treat the topic as RECREATION-SUSPECTED and floor its resume, never
//! skip); no usable answer → [`Unresolved`](TopicIdProbe::Unresolved).
//!
//! Honest residual: agreement is only measurable among the brokers that
//! RESPONDED. A single-broker bootstrap has no agreement concept, and if
//! the ONLY responder is a lagging broker (the fresh ones down/unreachable)
//! — or if EVERY broker still serves the stale id early in the window —
//! the old id is unanimous and the R28 exposure remains. Multi-broker
//! agreement NARROWS the propagation-window residual; it cannot eliminate
//! a metadata-based detection's inherent limits.
//!
//! All I/O **BLOCKS the calling thread**, hard-bounded by the caller's
//! timeout: DNS resolution runs on a helper thread the caller abandons at
//! the deadline, connect uses the remaining time, and every socket
//! read/write re-checks the overall deadline between calls so even a peer
//! trickling one byte per timeout window cannot stretch the probe (Codex R8
//! H4). Run [`fetch_topic_id`] on a blocking-capable thread
//! (`tokio::task::spawn_blocking`), like the crate's other identity fetches.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs as _};
use std::time::{Duration, Instant};

/// Kafka api key of the Metadata request.
const API_KEY_METADATA: i16 = 3;
/// Kafka api key of the ApiVersions request.
const API_KEY_API_VERSIONS: i16 = 18;
/// First Metadata version that carries topic ids (KIP-516, Kafka 2.8).
const METADATA_MIN_TOPIC_ID_VERSION: i16 = 10;
/// Highest Metadata version this probe speaks. v11 dropped
/// `include_cluster_authorized_operations`; v12 made the response topic name
/// nullable — both handled; nothing newer is needed for the id.
const METADATA_MAX_SUPPORTED_VERSION: i16 = 12;
/// Cap on an accepted response frame. A single-topic Metadata response is
/// KBs (brokers list + one topic's partitions); the cap only guards against
/// a hostile/broken peer declaring a huge frame before we allocate.
const MAX_RESPONSE_FRAME: usize = 4 * 1024 * 1024;
/// Cap on parsed array element counts (brokers / api keys), same purpose.
const MAX_ARRAY_ELEMENTS: u32 = 100_000;
/// Client id stamped on the probe's requests (visible in broker logs).
const PROBE_CLIENT_ID: &str = "ferrodruid-topic-id-probe";
/// Correlation ids for the two requests (arbitrary, just echoed back).
const CORR_API_VERSIONS: i32 = 0x7001;
const CORR_METADATA: i32 = 0x7002;

/// Whether the topic-id probe can run under the given
/// `security.protocol` consumer property: only a PLAINTEXT listener (the
/// librdkafka default when the property is unset) speaks unwrapped Kafka
/// protocol on the socket. TLS/SASL listeners expect a handshake this probe
/// does not implement — the caller must skip the probe (identity
/// UNRESOLVED: recreation detection unavailable, cluster gating governs —
/// Codex R8 H2) instead of sending them garbage.
#[must_use]
pub fn plaintext_probe_supported(security_protocol: Option<&str>) -> bool {
    match security_protocol {
        None => true,
        Some(p) => p.trim().eq_ignore_ascii_case("plaintext") || p.trim().is_empty(),
    }
}

/// Outcome of the multi-broker KIP-516 topic-id probe (Codex R28 H1) — the
/// tri-state the resume derivation keys on. See the module doc's
/// "Multi-broker agreement" section for why a single first-responder answer
/// is not trustworthy across a topic delete+recreate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicIdProbe {
    /// Every RESPONSIVE bootstrap broker returned the SAME non-zero topic
    /// id (formatted as a hyphenated UUID string — the value stamped into
    /// durable segment provenance as `payload.topicId` and compared on
    /// resume, Codex R7 H1). The normal steady-state outcome; also the
    /// outcome of a single-broker bootstrap or a partially-reachable list
    /// (agreement is only measurable among responders — the documented
    /// residual).
    Agreed(String),
    /// Responsive brokers returned CONFLICTING non-zero topic ids: the
    /// cluster's metadata is IN FLUX — exactly the propagation window of a
    /// topic delete+recreate (Codex R28 H1). No definite current id exists,
    /// and unlike [`Unresolved`](Self::Unresolved) this is POSITIVE evidence
    /// that a recreation may have just happened: the caller must treat the
    /// topic as RECREATION-SUSPECTED and floor its resume at the retained
    /// log's `low` watermark (bounded at-least-once re-consumption), never
    /// advance/skip on any stamped id.
    Disagreed,
    /// No broker yielded a usable id (unreachable, non-PLAINTEXT, pre-2.8,
    /// zero UUID, malformed response, or timeout): identity UNRESOLVED —
    /// the fail-safe posture (Codex R8 H2); the resume is gated by the
    /// cluster identity alone and recreation detection is unavailable.
    Unresolved,
}

impl TopicIdProbe {
    /// The agreed-on id, when every responsive broker returned the same one.
    #[must_use]
    pub fn agreed_id(&self) -> Option<&str> {
        match self {
            Self::Agreed(id) => Some(id),
            Self::Disagreed | Self::Unresolved => None,
        }
    }

    /// Consume the probe into the agreed-on id — the value the overlord
    /// stamps into published segment provenance ([`Disagreed`](Self::Disagreed)
    /// and [`Unresolved`](Self::Unresolved) stamp nothing: no definite
    /// current id exists).
    #[must_use]
    pub fn into_agreed(self) -> Option<String> {
        match self {
            Self::Agreed(id) => Some(id),
            Self::Disagreed | Self::Unresolved => None,
        }
    }

    /// Whether the responsive brokers DISAGREED on the topic id (Codex R28
    /// H1) — the recreation-SUSPECTED signal the resume derivation floors on.
    #[must_use]
    pub fn is_disagreed(&self) -> bool {
        matches!(self, Self::Disagreed)
    }
}

/// Incremental agreement fold over the per-broker probe answers (Codex R28
/// H1). Pure — the aggregation rule (`first id sticks; any DIFFERENT id
/// flips to disagreement, permanently`) is unit-tested without sockets.
#[derive(Default)]
struct ProbeAgreement {
    /// The first non-zero id observed (agreement candidate).
    first: Option<[u8; 16]>,
    /// A conflicting non-zero id was observed — sticky: nothing can
    /// un-disagree once two brokers conflicted.
    disagreed: bool,
}

impl ProbeAgreement {
    /// Fold one broker's NON-ZERO topic id. Returns `true` when this
    /// observation just created a DISAGREEMENT — the caller may stop
    /// probing: further answers can never restore agreement.
    fn observe(&mut self, id: [u8; 16]) -> bool {
        match self.first {
            None => {
                self.first = Some(id);
                false
            }
            Some(first) if first == id => false,
            Some(_) => {
                self.disagreed = true;
                true
            }
        }
    }

    /// Resolve the fold into the tri-state verdict.
    fn finish(self) -> TopicIdProbe {
        if self.disagreed {
            TopicIdProbe::Disagreed
        } else if let Some(id) = self.first {
            TopicIdProbe::Agreed(format_topic_uuid(&id))
        } else {
            TopicIdProbe::Unresolved
        }
    }
}

/// Resolve the KIP-516 **topic id** of `topic` from EVERY responsive broker
/// in `brokers` (a librdkafka-style comma-separated `bootstrap.servers`
/// list), blocking up to `timeout` overall, and return the tri-state
/// agreement verdict (Codex R28 H1):
///
/// * [`TopicIdProbe::Agreed`] — every responsive broker returned the same
///   non-zero id, formatted as a hyphenated UUID string (the value the
///   overlord stamps into durable segment provenance, `payload.topicId`,
///   and compares on resume to detect a topic recreation, Codex R7 H1);
/// * [`TopicIdProbe::Disagreed`] — brokers returned CONFLICTING non-zero
///   ids (a recreation's metadata propagation window): no id is definite
///   and the caller must treat the topic as recreation-SUSPECTED;
/// * [`TopicIdProbe::Unresolved`] — no usable answer (see the module doc's
///   fail-safe posture); a zero UUID (a broker that has no id for the
///   topic) is likewise no answer.
///
/// **BLOCKS the calling thread**, hard-bounded by `timeout` end to end:
/// DNS, connect, and every socket read/write of every broker probe all
/// enforce the ONE overall deadline (Codex R8 H4 — probing all brokers
/// instead of the first responder does not widen the bound; brokers not
/// reached before the deadline are simply not heard, and agreement is
/// measured among the responders). Run it on a blocking-capable thread.
#[must_use]
pub fn fetch_topic_id(brokers: &str, topic: &str, timeout: Duration) -> TopicIdProbe {
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return TopicIdProbe::Unresolved;
    };
    let mut agreement = ProbeAgreement::default();
    for entry in brokers.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        if Instant::now() >= deadline {
            break;
        }
        match probe_host(entry, topic, deadline) {
            Some(id) if id != [0u8; 16] => {
                if agreement.observe(id) {
                    // Sticky: nothing can un-disagree — stop probing.
                    tracing::warn!(
                        broker = entry,
                        topic,
                        conflicting_id = format_topic_uuid(&id),
                        "topic-id probe: bootstrap brokers DISAGREE on the \
                         topic's KIP-516 id — the cluster's metadata is in \
                         flux (a topic delete+recreate propagation window, \
                         Codex R28 H1). No definite current id exists; the \
                         resume must treat the topic as RECREATION-SUSPECTED \
                         (floor, never skip)",
                    );
                    break;
                }
            }
            Some(_) => {
                tracing::debug!(
                    broker = entry,
                    topic,
                    "topic-id probe: broker returned the ZERO topic uuid \
                     (no id assigned) — treating the identity as unresolved",
                );
            }
            None => {}
        }
    }
    agreement.finish()
}

/// Probe ONE bootstrap entry. `None` on any failure (fail-safe).
fn probe_host(entry: &str, topic: &str, deadline: Instant) -> Option<[u8; 16]> {
    // librdkafka accepts `PROTOCOL://host:port` entries; anything but a
    // PLAINTEXT prefix names a listener this probe cannot speak to.
    let hostport = match entry.split_once("://") {
        Some((proto, rest)) => {
            if !proto.eq_ignore_ascii_case("plaintext") {
                tracing::debug!(
                    broker = entry,
                    "topic-id probe: skipping non-PLAINTEXT bootstrap entry",
                );
                return None;
            }
            rest
        }
        None => entry,
    };
    let mut stream = connect(hostport, deadline)?;

    // 1. ApiVersions v0 — every broker answers it — to learn the broker's
    //    Metadata version ceiling.
    send_frame(
        &mut stream,
        &encode_api_versions_request(CORR_API_VERSIONS, PROBE_CLIENT_ID),
        deadline,
    )?;
    let resp = recv_frame(&mut stream, deadline)?;
    let max_metadata =
        parse_api_versions_max(&resp, CORR_API_VERSIONS, API_KEY_METADATA).or_else(|| {
            tracing::debug!(
                broker = entry,
                "topic-id probe: ApiVersions response unusable (error / no \
                 Metadata range / malformed)",
            );
            None
        })?;
    if max_metadata < METADATA_MIN_TOPIC_ID_VERSION {
        tracing::debug!(
            broker = entry,
            max_metadata,
            "topic-id probe: broker predates KIP-516 topic ids in Metadata \
             (needs v10+, Kafka >= 2.8) — identity unresolved",
        );
        return None;
    }
    let version = max_metadata.min(METADATA_MAX_SUPPORTED_VERSION);

    // 2. Metadata v10..=v12 for the ONE topic; the response carries its id.
    send_frame(
        &mut stream,
        &encode_metadata_request(version, CORR_METADATA, PROBE_CLIENT_ID, topic),
        deadline,
    )?;
    let resp = recv_frame(&mut stream, deadline)?;
    parse_metadata_topic_id(&resp, CORR_METADATA, topic, version)
}

/// Connect to `host[:port]` within the deadline (defaulting the port to
/// librdkafka's 9092 when a plain hostname is given). `None` on
/// resolve/connect failure or an already-expired deadline.
fn connect(hostport: &str, deadline: Instant) -> Option<TcpStream> {
    deadline.checked_duration_since(Instant::now())?;
    // A bootstrap entry without a port gets librdkafka's default. (An
    // un-bracketed IPv6 literal contains ':' and simply fails resolution —
    // fail-safe None, exactly like librdkafka requires brackets there.)
    let with_port = if hostport.contains(':') {
        hostport.to_string()
    } else {
        format!("{hostport}:9092")
    };
    let addr = resolve_first_addr(with_port, deadline)?;
    let remaining = deadline.checked_duration_since(Instant::now())?;
    let stream = TcpStream::connect_timeout(&addr, remaining.max(Duration::from_millis(1))).ok()?;
    let _ = stream.set_nodelay(true);
    Some(stream)
}

/// Resolve `host:port` to its first address, bounded by the deadline
/// (Codex R8 H4): `to_socket_addrs` drives the OS resolver, which honors no
/// caller timeout, so it runs on a short-lived helper thread and the caller
/// waits at most the REMAINING deadline for the answer. On timeout the
/// probe aborts (`None`) while the helper thread finishes in the background
/// (its send to the dropped channel is ignored) — a slow resolver can no
/// longer stall supervisor startup past the probe's overall budget.
fn resolve_first_addr(with_port: String, deadline: Instant) -> Option<SocketAddr> {
    let remaining = deadline.checked_duration_since(Instant::now())?;
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("fd-topic-id-dns".to_string())
        .spawn(move || {
            let resolved = with_port
                .to_socket_addrs()
                .ok()
                .and_then(|mut addrs| addrs.next());
            let _ = tx.send(resolved);
        })
        .ok()?;
    rx.recv_timeout(remaining).ok().flatten()
}

/// Arm the socket timeouts to the remaining deadline. `None` when expired.
fn arm_timeouts(stream: &TcpStream, deadline: Instant) -> Option<()> {
    let remaining = deadline
        .checked_duration_since(Instant::now())?
        .max(Duration::from_millis(1));
    stream.set_read_timeout(Some(remaining)).ok()?;
    stream.set_write_timeout(Some(remaining)).ok()?;
    Some(())
}

/// Write one length-prefixed request frame, enforcing the OVERALL deadline
/// across the whole frame (Codex R8 H4): the deadline is re-checked and the
/// socket timeout re-armed to the REMAINING time before every write, so a
/// peer draining the frame one byte per timeout window cannot stretch the
/// probe unboundedly.
fn send_frame(stream: &mut TcpStream, payload: &[u8], deadline: Instant) -> Option<()> {
    let len = i32::try_from(payload.len()).ok()?;
    let mut frame = Vec::with_capacity(payload.len() + 4);
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(payload);
    let mut written = 0usize;
    while written < frame.len() {
        arm_timeouts(stream, deadline)?; // None once the deadline passed
        match stream.write(&frame[written..]) {
            Ok(0) => return None, // peer closed its receive side
            Ok(n) => written += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return None, // incl. the armed timeout firing
        }
    }
    Some(())
}

/// Read one length-prefixed response frame (bounded by
/// [`MAX_RESPONSE_FRAME`]), enforcing the OVERALL deadline across the whole
/// frame (Codex R8 H4). Pre-R8 this armed one socket timeout per frame and
/// `read_exact` then accepted unlimited per-byte waits below it — a hostile
/// broker trickling each byte just inside the window could stretch the
/// probe to declared-body-length × per-byte delay, hanging supervisor
/// startup far past the probe's budget.
fn recv_frame(stream: &mut TcpStream, deadline: Instant) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    read_full(stream, &mut len_buf, deadline)?;
    let len = usize::try_from(i32::from_be_bytes(len_buf)).ok()?;
    if len == 0 || len > MAX_RESPONSE_FRAME {
        return None;
    }
    let mut body = vec![0u8; len];
    read_full(stream, &mut body, deadline)?;
    Some(body)
}

/// `read_exact` with the overall deadline enforced BETWEEN reads (Codex R8
/// H4): before every read the deadline is re-checked (`None` once passed)
/// and the socket timeout re-armed to the REMAINING time, so the total wall
/// time is bounded by the deadline no matter how the peer paces its bytes.
fn read_full(stream: &mut TcpStream, buf: &mut [u8], deadline: Instant) -> Option<()> {
    let mut filled = 0usize;
    while filled < buf.len() {
        arm_timeouts(stream, deadline)?;
        match stream.read(&mut buf[filled..]) {
            Ok(0) => return None, // EOF before the frame completed
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return None, // incl. the armed timeout firing
        }
    }
    Some(())
}

// ---------------------------------------------------------------------------
// Wire codec (pure; unit-tested without a socket)
// ---------------------------------------------------------------------------

/// Bounds-checked big-endian reader over a response body.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }

    fn read_i16(&mut self) -> Option<i16> {
        let b = self.take(2)?;
        Some(i16::from_be_bytes([b[0], b[1]]))
    }

    fn read_i32(&mut self) -> Option<i32> {
        let b = self.take(4)?;
        Some(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Kafka UNSIGNED_VARINT (protobuf-style, at most 5 bytes for u32).
    fn read_uvarint(&mut self) -> Option<u32> {
        let mut value: u32 = 0;
        for shift in 0..5u32 {
            let b = *self.take(1)?.first()?;
            value |= u32::from(b & 0x7f) << (shift * 7);
            if b & 0x80 == 0 {
                return Some(value);
            }
        }
        None // over-long varint
    }

    /// COMPACT_STRING / COMPACT_NULLABLE_STRING: uvarint(len+1) + bytes;
    /// 0 encodes null. Returns the bytes (`None` inner = null).
    fn read_compact_string(&mut self) -> Option<Option<&'a [u8]>> {
        let n = self.read_uvarint()?;
        if n == 0 {
            return Some(None);
        }
        let len = usize::try_from(n - 1).ok()?;
        Some(Some(self.take(len)?))
    }

    /// Skip a flexible-version TAG_BUFFER (uvarint count, then per tag:
    /// uvarint tag, uvarint size, size bytes).
    fn skip_tag_buffer(&mut self) -> Option<()> {
        let count = self.read_uvarint()?;
        if count > MAX_ARRAY_ELEMENTS {
            return None;
        }
        for _ in 0..count {
            let _tag = self.read_uvarint()?;
            let size = usize::try_from(self.read_uvarint()?).ok()?;
            self.take(size)?;
        }
        Some(())
    }

    /// COMPACT_ARRAY length: uvarint(N+1) decoded to the element count `N`,
    /// bounds-checked. A `0` encoding (a NULL compact array) has no valid
    /// count here and yields `None` — every array this probe walks
    /// (brokers / topics / partitions / replica lists) is non-nullable, so a
    /// null is a malformed frame and must fail safe.
    fn read_compact_len(&mut self) -> Option<u32> {
        let n = self.read_uvarint()?.checked_sub(1)?;
        if n > MAX_ARRAY_ELEMENTS {
            return None;
        }
        Some(n)
    }

    /// Skip a COMPACT_ARRAY of INT32 (a partition's replica / isr / offline
    /// node lists), bounds-checked against the buffer.
    fn skip_compact_i32_array(&mut self) -> Option<()> {
        let n = self.read_compact_len()?;
        let bytes = usize::try_from(n).ok()?.checked_mul(4)?;
        self.take(bytes)?;
        Some(())
    }

    /// Whether the cursor has consumed the buffer EXACTLY (no trailing
    /// bytes) — the trailing-garbage guard for a fully parsed response.
    fn at_end(&self) -> bool {
        self.pos == self.buf.len()
    }
}

/// Append a Kafka UNSIGNED_VARINT.
fn put_uvarint(buf: &mut Vec<u8>, mut v: u32) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        buf.push(b);
        if v == 0 {
            return;
        }
    }
}

/// Append a legacy NULLABLE_STRING (i16 length + bytes) — the encoding the
/// REQUEST HEADER's `client_id` keeps even in flexible versions (KIP-482).
fn put_legacy_string(buf: &mut Vec<u8>, s: &str) {
    let len = i16::try_from(s.len()).unwrap_or(0);
    if len as usize != s.len() {
        // Absurdly long client id: send null rather than a corrupt frame.
        buf.extend_from_slice(&(-1i16).to_be_bytes());
        return;
    }
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&s.as_bytes()[..len as usize]);
}

/// Append a COMPACT_STRING (uvarint(len+1) + bytes).
fn put_compact_string(buf: &mut Vec<u8>, s: &str) {
    let n = u32::try_from(s.len()).unwrap_or(0).saturating_add(1);
    put_uvarint(buf, n);
    buf.extend_from_slice(s.as_bytes());
}

/// Encode an `ApiVersions v0` request (header v1 + empty body), sans the
/// frame length prefix.
fn encode_api_versions_request(correlation_id: i32, client_id: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + client_id.len());
    buf.extend_from_slice(&API_KEY_API_VERSIONS.to_be_bytes());
    buf.extend_from_slice(&0i16.to_be_bytes()); // version 0
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    put_legacy_string(&mut buf, client_id);
    buf
}

/// Parse an `ApiVersions v0` response and return the broker's MAX version
/// for `api_key`. `None` on a correlation mismatch, a non-zero error code,
/// an absent key, or a malformed body.
fn parse_api_versions_max(body: &[u8], correlation_id: i32, api_key: i16) -> Option<i16> {
    let mut c = Cursor::new(body);
    if c.read_i32()? != correlation_id {
        return None;
    }
    if c.read_i16()? != 0 {
        return None; // error_code
    }
    let count = u32::try_from(c.read_i32()?).ok()?;
    if count > MAX_ARRAY_ELEMENTS {
        return None;
    }
    let mut found = None;
    for _ in 0..count {
        let key = c.read_i16()?;
        let _min = c.read_i16()?;
        let max = c.read_i16()?;
        if key == api_key {
            found = Some(max);
        }
    }
    found
}

/// Encode a `Metadata` request (v10..=v12, flexible; header v2) for exactly
/// one topic, sans the frame length prefix.
fn encode_metadata_request(
    version: i16,
    correlation_id: i32,
    client_id: &str,
    topic: &str,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(48 + client_id.len() + topic.len());
    buf.extend_from_slice(&API_KEY_METADATA.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    put_legacy_string(&mut buf, client_id);
    put_uvarint(&mut buf, 0); // header TAG_BUFFER
    // topics: COMPACT_ARRAY, one entry.
    put_uvarint(&mut buf, 2); // N + 1
    buf.extend_from_slice(&[0u8; 16]); // topic_id: zero uuid (lookup by name)
    put_compact_string(&mut buf, topic); // name
    put_uvarint(&mut buf, 0); // topic TAG_BUFFER
    buf.push(0); // allow_auto_topic_creation = false
    if version == 10 {
        buf.push(0); // include_cluster_authorized_operations (v8-10 only)
    }
    buf.push(0); // include_topic_authorized_operations = false
    put_uvarint(&mut buf, 0); // body TAG_BUFFER
    buf
}

/// Parse a `Metadata` v10..=v12 response (flexible; header v1) IN FULL,
/// returning the single requested topic's 16-byte topic id only when the
/// entire response is authoritative and well-formed; any anomaly yields
/// `None` (identity UNRESOLVED → cluster-fallback, Codex R8 H2), never a
/// definite-but-wrong id (Codex R10 H1).
///
/// A 16-byte id read out of a garbage/truncated frame is NOT proof of
/// identity: a hostile or broken PLAINTEXT proxy can echo the requested name
/// plus a bogus id and then trail off, and a bogus id compares unequal to the
/// stamped one on resume — a false recreation that re-consumes and re-publishes
/// the whole retained log on every restart. So this validates, in order:
/// the correlation id; **exactly one** topic entry (the one we asked for) with
/// a zero `error_code` and a matching NAME; and then the ENTIRE remaining
/// structure — `is_internal`, the full partitions array (each partition's
/// header, replica/isr/offline node lists, and tag buffer), the topic's
/// `topic_authorized_operations` and tag buffer, v10's trailing
/// `cluster_authorized_operations`, and the response-body tag buffer — with the
/// frame required to END EXACTLY there (no truncation, no trailing garbage;
/// consistent with the [`MAX_RESPONSE_FRAME`] guard). `version` selects the
/// v10-only `cluster_authorized_operations` trailer.
fn parse_metadata_topic_id(
    body: &[u8],
    correlation_id: i32,
    topic: &str,
    version: i16,
) -> Option<[u8; 16]> {
    let mut c = Cursor::new(body);
    if c.read_i32()? != correlation_id {
        return None;
    }
    c.skip_tag_buffer()?; // response header v1
    let _throttle_ms = c.read_i32()?;
    // brokers: COMPACT_ARRAY of { node_id, host, port, rack, tags }.
    let brokers = c.read_compact_len()?;
    for _ in 0..brokers {
        let _node_id = c.read_i32()?;
        let _host = c.read_compact_string()?;
        let _port = c.read_i32()?;
        let _rack = c.read_compact_string()?;
        c.skip_tag_buffer()?;
    }
    let _cluster_id = c.read_compact_string()?;
    let _controller_id = c.read_i32()?;
    // topics: COMPACT_ARRAY — we asked for EXACTLY one; anything else means
    // the response is not the authoritative answer to our request.
    if c.read_compact_len()? != 1 {
        return None;
    }
    let error_code = c.read_i16()?;
    let name = c.read_compact_string()??; // v12 name is nullable; null mismatches below
    let id: [u8; 16] = c.take(16)?.try_into().ok()?;
    if error_code != 0 || name != topic.as_bytes() {
        return None;
    }
    // The id is only trustworthy if the REST of the frame parses cleanly.
    let _is_internal = c.take(1)?; // BOOLEAN
    // partitions: COMPACT_ARRAY, each fully walked.
    let partitions = c.read_compact_len()?;
    for _ in 0..partitions {
        let _p_error_code = c.read_i16()?;
        let _partition_index = c.read_i32()?;
        let _leader_id = c.read_i32()?;
        let _leader_epoch = c.read_i32()?; // v7+ (always present at v10..=v12)
        c.skip_compact_i32_array()?; // replica_nodes
        c.skip_compact_i32_array()?; // isr_nodes
        c.skip_compact_i32_array()?; // offline_replicas (v5+)
        c.skip_tag_buffer()?;
    }
    let _topic_authorized_operations = c.read_i32()?; // v8+
    c.skip_tag_buffer()?; // topic TAG_BUFFER
    if version <= 10 {
        let _cluster_authorized_operations = c.read_i32()?; // v8-10 only
    }
    c.skip_tag_buffer()?; // response-body TAG_BUFFER
    // Trailing-garbage guard: a well-formed frame ends EXACTLY here.
    if !c.at_end() {
        return None;
    }
    Some(id)
}

/// Format a 16-byte topic id as the canonical hyphenated UUID string
/// (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, lowercase hex) — the value
/// stamped into durable provenance and compared on resume.
fn format_topic_uuid(id: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(36);
    for (i, b) in id.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        // Infallible for String, but never panic in non-test code.
        let _ = write!(out, "{b:02x}");
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uvarint_roundtrip() {
        for v in [0u32, 1, 127, 128, 300, 16_383, 16_384, u32::MAX] {
            let mut buf = Vec::new();
            put_uvarint(&mut buf, v);
            let mut c = Cursor::new(&buf);
            assert_eq!(c.read_uvarint(), Some(v), "roundtrip {v}");
            assert_eq!(c.pos, buf.len(), "consumed exactly, {v}");
        }
        // Over-long varint (6 continuation bytes) must be rejected, not spin.
        let mut c = Cursor::new(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_eq!(c.read_uvarint(), None);
    }

    #[test]
    fn api_versions_request_encoding_is_stable() {
        let req = encode_api_versions_request(0x7001, "cid");
        // api_key=18, version=0, correlation=0x7001, client_id "cid".
        assert_eq!(
            req,
            vec![0, 18, 0, 0, 0, 0, 0x70, 0x01, 0, 3, b'c', b'i', b'd'],
        );
    }

    /// Synthetic `ApiVersions v0` response carrying Metadata (key 3) with
    /// max version 12 among other keys.
    fn api_versions_response(corr: i32, error: i16, entries: &[(i16, i16, i16)]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&corr.to_be_bytes());
        b.extend_from_slice(&error.to_be_bytes());
        b.extend_from_slice(&i32::try_from(entries.len()).expect("len").to_be_bytes());
        for (k, lo, hi) in entries {
            b.extend_from_slice(&k.to_be_bytes());
            b.extend_from_slice(&lo.to_be_bytes());
            b.extend_from_slice(&hi.to_be_bytes());
        }
        b
    }

    #[test]
    fn api_versions_parse_finds_metadata_ceiling() {
        let resp = api_versions_response(0x7001, 0, &[(18, 0, 3), (3, 0, 12), (1, 0, 15)]);
        assert_eq!(parse_api_versions_max(&resp, 0x7001, 3), Some(12));
        // Correlation mismatch / broker error / absent key / truncation → None.
        assert_eq!(parse_api_versions_max(&resp, 0x7002, 3), None);
        let err = api_versions_response(0x7001, 35, &[(3, 0, 12)]);
        assert_eq!(parse_api_versions_max(&err, 0x7001, 3), None);
        let none = api_versions_response(0x7001, 0, &[(18, 0, 3)]);
        assert_eq!(parse_api_versions_max(&none, 0x7001, 3), None);
        assert_eq!(parse_api_versions_max(&resp[..6], 0x7001, 3), None);
    }

    #[test]
    fn metadata_request_encodes_one_named_topic() {
        let req = encode_metadata_request(12, 0x7002, "cid", "orders");
        // Header: key 3, version 12, correlation, "cid", empty tag buffer.
        assert_eq!(&req[..2], &3i16.to_be_bytes());
        assert_eq!(&req[2..4], &12i16.to_be_bytes());
        assert_eq!(&req[4..8], &0x7002i32.to_be_bytes());
        assert_eq!(&req[8..10], &3i16.to_be_bytes());
        assert_eq!(&req[10..13], b"cid");
        assert_eq!(req[13], 0); // header tags
        assert_eq!(req[14], 2); // topics compact len = 1 entry
        assert_eq!(&req[15..31], &[0u8; 16]); // zero topic_id
        assert_eq!(req[31], 7); // compact len of "orders" + 1
        assert_eq!(&req[32..38], b"orders");
        // topic tags, allow_auto_create, include_topic_authorized_ops, body tags
        assert_eq!(&req[38..], &[0, 0, 0, 0]);
        // v10 additionally carries include_cluster_authorized_operations.
        let v10 = encode_metadata_request(10, 0x7002, "cid", "orders");
        assert_eq!(v10.len(), req.len() + 1);
        // v11 matches v12's shape.
        assert_eq!(
            encode_metadata_request(11, 0x7002, "cid", "orders").len(),
            req.len()
        );
    }

    /// Synthetic single-topic `Metadata` response prefix THROUGH the first
    /// topic's id only (no `is_internal` / partitions / tag buffers) — the
    /// truncated/garbage tail a hostile peer can echo; the fixed parser must
    /// reject it (Codex R10 H1).
    fn metadata_response(corr: i32, topic: &str, error: i16, id: [u8; 16]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&corr.to_be_bytes());
        b.push(0); // header tag buffer
        b.extend_from_slice(&0i32.to_be_bytes()); // throttle
        // one broker: node 1, host "kafka", port 9092, null rack, no tags.
        put_uvarint(&mut b, 2);
        b.extend_from_slice(&1i32.to_be_bytes());
        put_compact_string(&mut b, "kafka");
        b.extend_from_slice(&9092i32.to_be_bytes());
        put_uvarint(&mut b, 0); // null rack
        b.push(0); // broker tags
        put_compact_string(&mut b, "cluster-A"); // cluster_id
        b.extend_from_slice(&1i32.to_be_bytes()); // controller
        put_uvarint(&mut b, 2); // topics: one entry
        b.extend_from_slice(&error.to_be_bytes());
        put_compact_string(&mut b, topic);
        b.extend_from_slice(&id);
        // (partitions etc. deliberately absent — a truncated tail)
        b
    }

    /// Synthetic COMPLETE single-topic `Metadata` response for `version`
    /// (v10..=v12), from the correlation id through the response-body tag
    /// buffer, with `partitions` fully-formed partitions on the topic. The
    /// fixed parser must consume it EXACTLY and resolve the id.
    fn metadata_response_full(
        corr: i32,
        topic: &str,
        error: i16,
        id: [u8; 16],
        partitions: usize,
        version: i16,
    ) -> Vec<u8> {
        let mut b = metadata_response(corr, topic, error, id); // through the id
        b.push(0); // is_internal = false
        // partitions: COMPACT_ARRAY.
        put_uvarint(&mut b, u32::try_from(partitions + 1).expect("partitions+1"));
        for i in 0..partitions {
            b.extend_from_slice(&0i16.to_be_bytes()); // partition error_code
            b.extend_from_slice(&i32::try_from(i).expect("index").to_be_bytes());
            b.extend_from_slice(&1i32.to_be_bytes()); // leader_id
            b.extend_from_slice(&5i32.to_be_bytes()); // leader_epoch
            put_uvarint(&mut b, 2); // replica_nodes: [1]
            b.extend_from_slice(&1i32.to_be_bytes());
            put_uvarint(&mut b, 2); // isr_nodes: [1]
            b.extend_from_slice(&1i32.to_be_bytes());
            put_uvarint(&mut b, 1); // offline_replicas: []
            b.push(0); // partition tag buffer
        }
        b.extend_from_slice(&i32::MIN.to_be_bytes()); // topic_authorized_operations
        b.push(0); // topic tag buffer
        if version <= 10 {
            b.extend_from_slice(&i32::MIN.to_be_bytes()); // cluster_authorized_operations
        }
        b.push(0); // response-body tag buffer
        b
    }

    #[test]
    fn metadata_parse_extracts_topic_id() {
        let id = *b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10";
        let resp = metadata_response_full(0x7002, "orders", 0, id, 1, 12);
        assert_eq!(
            parse_metadata_topic_id(&resp, 0x7002, "orders", 12),
            Some(id)
        );
        // A topic with ZERO partitions is still a complete, valid response.
        let no_parts = metadata_response_full(0x7002, "orders", 0, id, 0, 12);
        assert_eq!(
            parse_metadata_topic_id(&no_parts, 0x7002, "orders", 12),
            Some(id)
        );
        // A v10 response carries the trailing cluster_authorized_operations.
        let v10 = metadata_response_full(0x7002, "orders", 0, id, 2, 10);
        assert_eq!(
            parse_metadata_topic_id(&v10, 0x7002, "orders", 10),
            Some(id)
        );
        // Correlation mismatch, topic error, name mismatch, truncation → None.
        assert_eq!(parse_metadata_topic_id(&resp, 0x7001, "orders", 12), None);
        let err = metadata_response_full(0x7002, "orders", 3, id, 1, 12);
        assert_eq!(parse_metadata_topic_id(&err, 0x7002, "orders", 12), None);
        assert_eq!(parse_metadata_topic_id(&resp, 0x7002, "other", 12), None);
        assert_eq!(
            parse_metadata_topic_id(&resp[..resp.len() - 8], 0x7002, "orders", 12),
            None,
            "a truncated frame must never panic or mis-parse"
        );
        // Broker with a TAGGED FIELD in the header must still parse.
        let mut tagged = resp.clone();
        // header tag buffer at offset 4: replace 0-count with one 3-byte tag.
        tagged.splice(4..5, [1u8, 0, 3, 0xaa, 0xbb, 0xcc]);
        assert_eq!(
            parse_metadata_topic_id(&tagged, 0x7002, "orders", 12),
            Some(id)
        );
    }

    #[test]
    fn metadata_parse_rejects_garbage_after_id() {
        // Codex R10 H1: a hostile/broken PLAINTEXT peer that echoes the
        // requested name + a NON-ZERO garbage id must NOT yield a definite id
        // unless the WHOLE frame is authoritative and well-formed. A bogus id
        // accepted here reads as a topic recreation on resume and re-consumes
        // the entire retained log on every restart.
        let garbage = *b"\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef";

        // (a) truncated right after the 16-byte id (no is_internal/partitions).
        let truncated = metadata_response(0x7002, "orders", 0, garbage);
        assert_eq!(
            parse_metadata_topic_id(&truncated, 0x7002, "orders", 12),
            None,
            "a response truncated right after the id must be unresolved"
        );

        // (b) well-formed frame + TRAILING garbage (declared length exceeds
        //     the real structure) → None.
        let mut trailing = metadata_response_full(0x7002, "orders", 0, garbage, 1, 12);
        trailing.extend_from_slice(&[0xff, 0xff, 0xff]);
        assert_eq!(
            parse_metadata_topic_id(&trailing, 0x7002, "orders", 12),
            None,
            "trailing garbage past the parsed structure must be unresolved"
        );

        // (c) partitions array declares a count the body does not carry
        //     (interior truncation) → None, never a mis-parse/panic.
        let mut short_parts = metadata_response(0x7002, "orders", 0, garbage);
        short_parts.push(0); // is_internal
        put_uvarint(&mut short_parts, 4); // claims 3 partitions, supplies none
        assert_eq!(
            parse_metadata_topic_id(&short_parts, 0x7002, "orders", 12),
            None,
            "an over-declared partitions array must be unresolved"
        );

        // (d) error_code != 0 (topic metadata not authoritative) → None.
        let err = metadata_response_full(0x7002, "orders", 3, garbage, 1, 12);
        assert_eq!(parse_metadata_topic_id(&err, 0x7002, "orders", 12), None);

        // (e) name mismatch → None.
        let other = metadata_response_full(0x7002, "elsewhere", 0, garbage, 1, 12);
        assert_eq!(parse_metadata_topic_id(&other, 0x7002, "orders", 12), None);

        // (f) using the SAME non-zero id inside a COMPLETE frame resolves —
        //     the fix rejects garbage tails, not non-zero ids (regression).
        let clean = metadata_response_full(0x7002, "orders", 0, garbage, 1, 12);
        assert_eq!(
            parse_metadata_topic_id(&clean, 0x7002, "orders", 12),
            Some(garbage)
        );
    }

    #[test]
    fn metadata_parse_requires_exactly_one_topic() {
        let id = *b"\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00";
        let full = metadata_response_full(0x7002, "orders", 0, id, 1, 12);
        // The topics COMPACT_ARRAY count (encoded N+1) sits right after the
        // fixed-size header/brokers/cluster/controller prefix. Bump it from
        // `2` (one topic) to `3` (two topics) while the body still carries one
        // topic: a response claiming a different topic count is not the
        // authoritative single-topic answer to our request → None. The
        // drift-guard assert fails loudly if the prefix layout ever changes.
        const TOPICS_COUNT_OFFSET: usize = 4  // correlation id
            + 1  // header tag buffer
            + 4  // throttle_ms
            + 1 + 4 + 6 + 4 + 1 + 1  // one broker: count, node, "kafka", port, rack, tags
            + 10 // cluster_id "cluster-A" (compact string)
            + 4; // controller_id
        let mut two = full;
        assert_eq!(
            two[TOPICS_COUNT_OFFSET], 2,
            "topics compact-array count byte"
        );
        two[TOPICS_COUNT_OFFSET] = 3; // claim TWO topics
        assert_eq!(
            parse_metadata_topic_id(&two, 0x7002, "orders", 12),
            None,
            "a multi-topic response is not the authoritative single-topic answer"
        );
    }

    #[test]
    fn zero_uuid_and_formatting() {
        assert_eq!(
            format_topic_uuid(b"\x12\x34\x56\x78\x9a\xbc\xde\xf0\x01\x23\x45\x67\x89\xab\xcd\xef"),
            "12345678-9abc-def0-0123-456789abcdef"
        );
        // fetch_topic_id maps the zero uuid to None — pinned via the pure
        // pieces: a zero id in a COMPLETE frame parses fine but must be
        // discarded by the caller.
        let resp = metadata_response_full(0x7002, "t", 0, [0u8; 16], 1, 12);
        assert_eq!(
            parse_metadata_topic_id(&resp, 0x7002, "t", 12),
            Some([0u8; 16])
        );
    }

    #[test]
    fn probe_security_matrix() {
        assert!(plaintext_probe_supported(None));
        assert!(plaintext_probe_supported(Some("plaintext")));
        assert!(plaintext_probe_supported(Some("PLAINTEXT")));
        assert!(plaintext_probe_supported(Some(" plaintext ")));
        assert!(plaintext_probe_supported(Some("")));
        assert!(!plaintext_probe_supported(Some("ssl")));
        assert!(!plaintext_probe_supported(Some("SASL_SSL")));
        assert!(!plaintext_probe_supported(Some("sasl_plaintext")));
    }

    /// Real-broker witness (needs a PLAINTEXT Kafka >= 2.8 at
    /// `KAFKA_BOOTSTRAP`, default `localhost:9092`; run with `--ignored`):
    /// the probe resolves a real topic id, and a DELETE + RECREATE of the
    /// same topic name yields a DIFFERENT id — the exact recreation signal
    /// the resume frontier keys on (Codex R7 H1).
    #[cfg(feature = "kafka-io")]
    #[test]
    #[ignore = "needs a real PLAINTEXT Kafka broker (KAFKA_BOOTSTRAP)"]
    fn topic_id_probe_real_broker_detects_recreation() {
        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        use rdkafka::config::ClientConfig;

        let bootstrap =
            std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string());
        let topic = format!("fd-topicid-probe-{}", std::process::id());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        let admin: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .create()
            .expect("admin client");
        let opts = AdminOptions::new();
        let new_topic = || NewTopic::new(&topic, 1, TopicReplication::Fixed(1));

        let fetch_with_retry = || {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if let TopicIdProbe::Agreed(id) =
                    fetch_topic_id(&bootstrap, &topic, Duration::from_secs(3))
                {
                    return id;
                }
                assert!(
                    Instant::now() < deadline,
                    "topic id not resolvable within 20s"
                );
                std::thread::sleep(Duration::from_millis(500));
            }
        };

        rt.block_on(admin.create_topics([&new_topic()], &opts))
            .expect("create topic");
        let id1 = fetch_with_retry();

        rt.block_on(admin.delete_topics(&[&topic], &opts))
            .expect("delete topic");
        std::thread::sleep(Duration::from_secs(2));
        // Recreate may race the async deletion — retry briefly.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let res = rt
                .block_on(admin.create_topics([&new_topic()], &opts))
                .expect("issue recreate");
            if matches!(res.first(), Some(Ok(_))) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "recreate did not land within 20s"
            );
            std::thread::sleep(Duration::from_millis(500));
        }
        let id2 = fetch_with_retry();

        // Cleanup (best effort).
        let _ = rt.block_on(admin.delete_topics(&[&topic], &opts));

        assert_eq!(id1.len(), 36, "hyphenated uuid, got {id1}");
        assert_ne!(
            id1, id2,
            "a recreated topic must carry a DIFFERENT KIP-516 topic id"
        );
    }

    /// Codex R8 H4: a hostile/broken broker that TRICKLES one byte per read
    /// — each arriving just inside the armed socket timeout — must not
    /// stretch the probe past its OVERALL deadline. Pre-R8, `recv_frame`
    /// armed the read timeout ONCE per frame and `read_exact` then accepted
    /// unlimited per-byte waits below it, so the declared body length ×
    /// per-byte delay bounded the hang (megabytes × seconds ≫ any startup
    /// budget). The deadline must be re-enforced between reads.
    #[test]
    fn probe_trickled_response_aborts_at_overall_deadline() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let server = std::thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                // Declare a large ApiVersions response body, then trickle
                // single bytes with pauses that stay UNDER the ~600ms
                // per-frame timeout the client arms — 30 × 150ms ≈ 4.5s,
                // an order of magnitude past the overall deadline.
                let _ = sock.write_all(&1000i32.to_be_bytes());
                for _ in 0..30 {
                    if sock.write_all(&[0u8]).is_err() {
                        break; // client hung up (the expected abort)
                    }
                    let _ = sock.flush();
                    std::thread::sleep(Duration::from_millis(150));
                }
            }
        });

        let started = Instant::now();
        let got = fetch_topic_id(&addr.to_string(), "t", Duration::from_millis(600));
        let elapsed = started.elapsed();
        assert_eq!(
            got,
            TopicIdProbe::Unresolved,
            "a trickled garbage frame must resolve nothing"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "the probe must abort at its overall deadline (~600ms), not ride \
             per-byte reads past it — took {elapsed:?} (Codex R8 H4)"
        );
        server.join().expect("server thread");
    }

    /// Minimal single-connection fake Kafka broker on loopback: answers the
    /// probe's exact two request frames — `ApiVersions v0` (advertising a
    /// Metadata ceiling of v12) and `Metadata v12` (one complete topic entry
    /// carrying `id`) — then exits. Pure test scaffolding for multi-broker
    /// agreement scenarios without a real cluster.
    fn fake_broker_with_topic_id(
        topic: &'static str,
        id: [u8; 16],
    ) -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let handle = std::thread::spawn(move || {
            let Ok((mut sock, _)) = listener.accept() else {
                return;
            };
            for _ in 0..2 {
                // Read one length-prefixed request frame.
                let mut len_buf = [0u8; 4];
                if sock.read_exact(&mut len_buf).is_err() {
                    return; // client abandoned the probe
                }
                let Ok(len) = usize::try_from(i32::from_be_bytes(len_buf)) else {
                    return;
                };
                let mut body = vec![0u8; len];
                if sock.read_exact(&mut body).is_err() || body.len() < 8 {
                    return;
                }
                let api_key = i16::from_be_bytes([body[0], body[1]]);
                let corr = i32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                let resp = if api_key == API_KEY_API_VERSIONS {
                    api_versions_response(corr, 0, &[(API_KEY_METADATA, 0, 12)])
                } else {
                    metadata_response_full(corr, topic, 0, id, 1, 12)
                };
                let frame_len = i32::try_from(resp.len()).expect("frame len");
                if sock.write_all(&frame_len.to_be_bytes()).is_err()
                    || sock.write_all(&resp).is_err()
                {
                    return;
                }
            }
        });
        (addr, handle)
    }

    /// Codex R28 H1 (RED→GREEN): after a topic delete+recreate, metadata
    /// propagates broker-by-broker — a LAGGING broker still serves the OLD
    /// generation's topic id while a fresh one serves the NEW id. Pre-R28
    /// the probe trusted the FIRST responder and resolved the OLD id;
    /// downstream that id MATCHES the durable rows' stamped `topicId`, so
    /// `detect_topic_recreation` never fires, the resume frontier advances,
    /// and — with the new log regrown past the stale frontier — the consumer
    /// seeks FORWARD past new-generation records that were never consumed
    /// (permanent loss). Conflicting non-zero ids across brokers must
    /// resolve to [`TopicIdProbe::Disagreed`], never a definite id.
    #[test]
    fn fetch_topic_id_broker_disagreement_is_never_a_definite_id() {
        let old_id = *b"\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01";
        let new_id = *b"\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02\x02";
        let (lagging, h1) = fake_broker_with_topic_id("t", old_id);
        let (fresh, h2) = fake_broker_with_topic_id("t", new_id);
        let got = fetch_topic_id(&format!("{lagging},{fresh}"), "t", Duration::from_secs(5));
        assert_eq!(
            got,
            TopicIdProbe::Disagreed,
            "brokers DISAGREE on the topic id (a recreation propagation \
             window): the first (lagging) broker's OLD id must not be \
             trusted — it matches the durable rows' stamp, recreation \
             detection never fires, and the resume seeks past new-generation \
             records (permanent loss, Codex R28 H1)"
        );
        h1.join().expect("lagging broker thread");
        h2.join().expect("fresh broker thread");
    }

    /// R28 regressions: the multi-broker sweep must NOT change the normal
    /// outcomes — unanimous brokers still resolve the id, a single broker
    /// still resolves alone (no agreement concept), and a partially
    /// reachable list still trusts its only responder.
    #[test]
    fn fetch_topic_id_broker_agreement_resolves_the_id() {
        let id = *b"\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19";

        // Two brokers, SAME id → Agreed.
        let (a, h1) = fake_broker_with_topic_id("t", id);
        let (b, h2) = fake_broker_with_topic_id("t", id);
        let got = fetch_topic_id(&format!("{a},{b}"), "t", Duration::from_secs(5));
        assert_eq!(
            got,
            TopicIdProbe::Agreed(format_topic_uuid(&id)),
            "unanimous brokers must resolve the id (no spurious \
             disagreement — the normal path is unchanged)"
        );
        h1.join().expect("broker a");
        h2.join().expect("broker b");

        // Single broker → Agreed (a single-broker bootstrap has no
        // agreement concept; refusing to resolve would regress every
        // single-broker deployment to permanent Unresolved).
        let (solo, h3) = fake_broker_with_topic_id("t", id);
        let got = fetch_topic_id(&solo.to_string(), "t", Duration::from_secs(5));
        assert_eq!(got, TopicIdProbe::Agreed(format_topic_uuid(&id)));
        h3.join().expect("solo broker");

        // Unreachable first entry + one responsive broker → the responder
        // is trusted (agreement measured among RESPONDERS — the documented
        // residual; anything stricter would downgrade every partial outage
        // to Unresolved).
        let (only, h4) = fake_broker_with_topic_id("t", id);
        let got = fetch_topic_id(&format!("127.0.0.1:1,{only}"), "t", Duration::from_secs(5));
        assert_eq!(got, TopicIdProbe::Agreed(format_topic_uuid(&id)));
        h4.join().expect("only responder");
    }

    /// The pure aggregation fold (Codex R28 H1), socket-free: first id
    /// sticks, an equal id keeps agreement, ANY different id flips to a
    /// sticky disagreement.
    #[test]
    fn probe_agreement_fold_is_sticky_on_conflict() {
        let id1 = [1u8; 16];
        let id2 = [2u8; 16];

        // No answers → Unresolved.
        assert_eq!(ProbeAgreement::default().finish(), TopicIdProbe::Unresolved);

        // One answer → Agreed.
        let mut one = ProbeAgreement::default();
        assert!(!one.observe(id1));
        assert_eq!(one.finish(), TopicIdProbe::Agreed(format_topic_uuid(&id1)));

        // Same answer twice → still Agreed.
        let mut same = ProbeAgreement::default();
        assert!(!same.observe(id1));
        assert!(!same.observe(id1));
        assert_eq!(same.finish(), TopicIdProbe::Agreed(format_topic_uuid(&id1)));

        // Conflict → observe returns true (probe may stop) and the fold is
        // permanently Disagreed, even if the first id repeats afterwards.
        let mut conflict = ProbeAgreement::default();
        assert!(!conflict.observe(id1));
        assert!(
            conflict.observe(id2),
            "a conflicting id must signal the caller"
        );
        assert!(!conflict.observe(id1), "already disagreed: no new signal");
        assert_eq!(conflict.finish(), TopicIdProbe::Disagreed);
    }

    #[test]
    fn fetch_topic_id_unreachable_broker_is_unresolved() {
        // Port 1 refuses immediately; the probe must degrade fast.
        assert_eq!(
            fetch_topic_id("127.0.0.1:1", "t", Duration::from_millis(300)),
            TopicIdProbe::Unresolved
        );
        // Non-PLAINTEXT prefixed entries are skipped outright.
        assert_eq!(
            fetch_topic_id("SSL://127.0.0.1:1", "t", Duration::from_millis(100)),
            TopicIdProbe::Unresolved
        );
        // Empty broker list.
        assert_eq!(
            fetch_topic_id("", "t", Duration::from_millis(100)),
            TopicIdProbe::Unresolved
        );
    }
}
