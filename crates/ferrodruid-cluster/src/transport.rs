// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Production TCP transport for cluster replication.
//!
//! Promoted from `tests/three_node_tcp.rs` (Wave 32) so the binary can spawn a
//! real multi-node cluster using only library code.
//!
//! Transport security posture (Phase 2.4)
//! --------------------------------------
//!
//! The **default** posture is mTLS ([`ClusterSecurityMode::MutualTls`]):
//! every TCP socket is wrapped in a rustls session that demands
//! peer-certificate validation against the configured CA. PSK frame
//! authentication still runs inside the TLS tunnel — the two layers are
//! additive. PSK-over-cleartext ([`ClusterSecurityMode::PskCleartext`]) is
//! retained as an explicit operator opt-in fallback and is never selected
//! implicitly; a misconfiguration fails loudly rather than silently
//! dropping confidentiality.
//!
//! Wire format (Wave 40-A — PSK-authenticated)
//! -------------------------------------------
//!
//! Every frame is `[u32 BE payload_len][32-byte HMAC-SHA256][JSON]` where the
//! HMAC is computed over the JSON bytes using the shared cluster PSK
//! ([`crate::auth::ClusterPsk`]). Receivers verify the HMAC in constant time
//! before any `serde_json` work touches the bytes.
//!
//! The first authenticated frame on every newly-opened TCP connection is a
//! [`crate::auth::HandshakeFrame`] that announces the sender's `node_id`.
//! The receiver records `connection -> announced_node_id` and rejects any
//! subsequent frame whose embedded `sender_id` does not match. This binds
//! the PSK-authenticated session to a single node id and prevents an
//! authenticated peer from forging a `ReplicateAck` claiming
//! `follower_id = some-other-node` (the Wave 39 NEW Critical finding).
//!
//! Inbound path: a single `tokio::net::TcpListener` accepts connections and
//! spawns one reader task per peer. Each authenticated [`ReplicationMessage`]
//! is dispatched to the local [`ReplicationEngine`]. For
//! [`ReplicationMessage::VoteRequest`] the transport computes the response
//! via [`ReplicationEngine::receive_vote_request`] and writes it back over
//! the same TCP connection.
//!
//! Outbound path: one connection pool entry per peer. Sends are best-effort
//! and reconnect with exponential back-off (cap 5 s) on failure. On every
//! reconnect the transport replays the handshake before any business
//! traffic. Connections are closed gracefully on [`TcpTransport::shutdown`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
#[cfg(feature = "cluster-tls")]
use tokio_rustls::TlsConnector;

use crate::auth::{ClusterPsk, HandshakeFrame};
use crate::replication::{
    ReplicationEngine, ReplicationMessage, TickAction, authenticated_payload_bytes,
    encode_authenticated_payload, encode_message_authenticated,
};
#[cfg(feature = "cluster-tls")]
use crate::tls::{TlsConfig, load_client_config, load_server_config};

/// Wave 44B: type-erased outbound write handle. Holds either the
/// `tokio::io::WriteHalf<TcpStream>` from the cleartext path or the
/// `tokio::io::WriteHalf<TlsStream<TcpStream>>` from the mTLS path.
/// Pinned + boxed so the same `out_conns` map can serve both.
type DynWriteHalf = Pin<Box<dyn AsyncWrite + Send + Unpin>>;

/// Identifier for a cluster node (matches `ReplicationConfig::node_id`).
pub type NodeId = String;

/// Maximum size, in bytes, of a single authenticated cluster frame body
/// (HMAC tag + JSON payload). Larger frames are rejected as garbage.
const MAX_FRAME_BODY: usize = 16 * 1024 * 1024;

/// Wave 47-A: how often the tick loop runs the leader-side replay scan,
/// expressed in tick iterations.  At the default 50 ms cadence this is
/// `10 ticks * 50 ms = 500 ms` between scans — fast enough to back-fill
/// a lagging follower within a heartbeat budget, slow enough to keep the
/// per-leader CPU cost under 1 % at idle (the scan is O(peers) when no
/// follower lags, since [`ReplicationEngine::build_replay_actions`]
/// returns an empty `Vec` for every caught-up follower).
const REPLAY_TICK_INTERVAL: u64 = 10;

/// Errors produced by the TCP transport.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Failed to bind the listener socket.
    #[error("bind failed on {addr}: {source}")]
    Bind {
        /// The address that the bind was attempted on.
        addr: SocketAddr,
        /// Underlying I/O error from the OS.
        #[source]
        source: std::io::Error,
    },
    /// Failed to encode an outgoing message.
    #[error("encode failed: {0}")]
    Encode(String),
    /// Send to a specific peer failed (after all retries within the call).
    #[error("send to {peer} failed: {source}")]
    Send {
        /// ID of the peer the send was directed at.
        peer: NodeId,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Attempted to send to a peer that the transport does not know.
    #[error("unknown peer: {0}")]
    UnknownPeer(NodeId),
    /// Wave 44B: failed to build the rustls acceptor / connector from
    /// the configured cert / key / CA paths.
    #[cfg(feature = "cluster-tls")]
    #[error("TLS configuration failed: {0}")]
    Tls(String),
}

/// Configuration for [`TcpTransport`].
#[derive(Clone)]
pub struct TcpTransportConfig {
    /// Address to bind the listener socket on.
    pub bind_addr: SocketAddr,
    /// Map from peer node id to its listening address. Must NOT include self.
    pub peers: Vec<(NodeId, SocketAddr)>,
    /// Per-connection dial timeout used by the outbound path.
    pub connect_timeout: Duration,
    /// Heartbeat broadcast period (leader sends heartbeats this often). The
    /// transport itself does not drive heartbeats; the binary's election
    /// loop is expected to call [`ReplicationEngine::send_heartbeat`] at
    /// this cadence. Surfaced here so the config is the single source of
    /// truth.
    pub heartbeat_period: Duration,
    /// Wave 40-A: shared cluster pre-shared key. Required for production
    /// deployments. The transport refuses to construct without one — see
    /// [`TcpTransportConfig::with_psk`] for the canonical builder. Wrapped
    /// in `Arc` so cheap cloning to per-task contexts does not duplicate
    /// the secret.
    pub psk: Arc<ClusterPsk>,
    /// Wave 40-A: this node's id, written into the [`HandshakeFrame`] sent
    /// on every outbound connection so receivers can bind the
    /// PSK-authenticated session to a single announced sender id.
    pub local_node_id: NodeId,
    /// Phase 2.4: transport security posture. This selects mTLS vs the
    /// explicit-opt-in PSK-over-cleartext fallback.
    ///
    /// The **default** posture is [`ClusterSecurityMode::MutualTls`]:
    /// every inbound and outbound TCP socket is wrapped in a rustls
    /// session that demands peer-certificate validation against the
    /// configured CA (confidentiality + forward secrecy + peer identity).
    ///
    /// PSK-over-cleartext ([`ClusterSecurityMode::PskCleartext`]) is an
    /// **explicit** operator opt-in — auth + integrity but no
    /// confidentiality, the Wave 40-A posture. Construct it deliberately;
    /// it is never selected implicitly.
    ///
    /// When the `cluster-tls` Cargo feature is disabled the mTLS variant
    /// cannot be built, so only [`ClusterSecurityMode::PskCleartext`]
    /// exists; that build can therefore never run an encrypted wire and
    /// the bins fail loudly at startup rather than silently downgrade.
    pub security: ClusterSecurityMode,
}

/// Phase 2.4: cluster transport security posture.
///
/// The production default is [`ClusterSecurityMode::MutualTls`]. PSK over
/// cleartext TCP is retained as an explicit, deliberately-constructed
/// fallback for backward compatibility, test rigs, and sealed-network
/// deployments — it is **never** selected implicitly, so a
/// misconfiguration fails loudly instead of silently dropping
/// confidentiality.
#[derive(Clone)]
pub enum ClusterSecurityMode {
    /// **Default production posture.** Wrap every cluster TCP socket in
    /// mTLS: both endpoints present a CA-signed X.509 cert and reject any
    /// peer whose cert does not validate (untrusted / expired / wrong-CA
    /// peers are refused at the rustls handshake, before any cluster wire
    /// byte is read). PSK frame authentication still runs *inside* the
    /// TLS tunnel — mTLS is additive, not a replacement.
    ///
    /// Only available when the `cluster-tls` Cargo feature is enabled.
    #[cfg(feature = "cluster-tls")]
    MutualTls(TlsConfig),
    /// **Explicit opt-in fallback.** PSK-authenticated frames over
    /// cleartext TCP: auth + integrity but no confidentiality. Selecting
    /// this is an explicit operator decision (config field / env), never
    /// an implicit default.
    PskCleartext,
}

impl std::fmt::Debug for ClusterSecurityMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "cluster-tls")]
            Self::MutualTls(cfg) => f.debug_tuple("MutualTls").field(cfg).finish(),
            Self::PskCleartext => f.write_str("PskCleartext"),
        }
    }
}

impl ClusterSecurityMode {
    /// Whether this mode wraps the wire in mTLS.
    #[must_use]
    pub fn is_mutual_tls(&self) -> bool {
        match self {
            #[cfg(feature = "cluster-tls")]
            Self::MutualTls(_) => true,
            Self::PskCleartext => false,
        }
    }
}

impl std::fmt::Debug for TcpTransportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("TcpTransportConfig");
        d.field("bind_addr", &self.bind_addr)
            .field("peers", &self.peers)
            .field("connect_timeout", &self.connect_timeout)
            .field("heartbeat_period", &self.heartbeat_period)
            .field("psk", &self.psk)
            .field("local_node_id", &self.local_node_id)
            .field("security", &self.security);
        d.finish()
    }
}

impl TcpTransportConfig {
    /// Explicit-opt-in builder for the **PSK-over-cleartext** posture.
    ///
    /// This selects [`ClusterSecurityMode::PskCleartext`]: PSK-authenticated
    /// frames with no confidentiality. It is the deliberate, named fallback
    /// for backward compatibility / test rigs / sealed networks — callers
    /// must reach for it on purpose. The production default is mTLS; see
    /// [`TcpTransportConfig::with_mutual_tls`].
    #[must_use]
    pub fn with_psk(
        bind_addr: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
        local_node_id: NodeId,
        psk: Arc<ClusterPsk>,
    ) -> Self {
        Self {
            bind_addr,
            peers,
            connect_timeout: Duration::from_secs(2),
            heartbeat_period: Duration::from_millis(500),
            psk,
            local_node_id,
            security: ClusterSecurityMode::PskCleartext,
        }
    }

    /// Default-posture builder for **mTLS** over the cluster wire.
    ///
    /// This selects [`ClusterSecurityMode::MutualTls`]: every socket is
    /// wrapped in rustls with peer-certificate verification against the
    /// CA in `tls`, with PSK frame authentication running inside the
    /// tunnel. This is the production posture — operators that want the
    /// weaker PSK-cleartext mode must opt in explicitly via
    /// [`TcpTransportConfig::with_psk`].
    #[cfg(feature = "cluster-tls")]
    #[must_use]
    pub fn with_mutual_tls(
        bind_addr: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
        local_node_id: NodeId,
        psk: Arc<ClusterPsk>,
        tls: TlsConfig,
    ) -> Self {
        Self {
            bind_addr,
            peers,
            connect_timeout: Duration::from_secs(2),
            heartbeat_period: Duration::from_millis(500),
            psk,
            local_node_id,
            security: ClusterSecurityMode::MutualTls(tls),
        }
    }
}

/// Production TCP transport for cluster replication.
///
/// Use [`TcpTransport::bind`] to build one and [`TcpTransport::shutdown`] to
/// drain it.
pub struct TcpTransport {
    config: TcpTransportConfig,
    /// `node_id -> SocketAddr` for fast peer lookup on the send path.
    peer_map: HashMap<NodeId, SocketAddr>,
    /// Cached outgoing TCP write halves to peers (one per peer, lazily
    /// opened and reopened on failure). Wave 38-C: each established
    /// outbound connection also spawns a reader task on the corresponding
    /// read half so peer-initiated reply traffic (`VoteResponse`,
    /// `Heartbeat` from a re-elected peer, etc.) is consumed and routed
    /// back into the engine.
    out_conns: Mutex<HashMap<NodeId, Option<DynWriteHalf>>>,
    /// Per-outbound-peer reader task handles (Wave 38-C). Aborted when the
    /// associated write-half is dropped on reconnect or on `shutdown`.
    out_readers: Mutex<HashMap<NodeId, JoinHandle<()>>>,
    /// Engine reference held so reader tasks spawned on outbound connections
    /// can dispatch messages without an out-of-band channel.
    engine: Arc<ReplicationEngine>,
    /// Listener task handle (aborted on shutdown).
    listener_task: Mutex<Option<JoinHandle<()>>>,
    /// Election + heartbeat scheduler task handle (Wave 38-C). Aborted on
    /// shutdown. `None` if `bind_with_scheduler` was not used.
    tick_task: Mutex<Option<JoinHandle<()>>>,
    /// Wave 44B: cached client-side TLS connector, built once at
    /// [`TcpTransport::bind`]. Each outbound dial wraps the freshly
    /// connected `TcpStream` with this connector when `Some(...)`. The
    /// matching server-side acceptor is moved into the listener task
    /// and not stored here (it is only needed on accept).
    #[cfg(feature = "cluster-tls")]
    tls_connector: Option<TlsConnector>,
}

impl TcpTransport {
    /// Bind a TCP listener and return a transport ready to receive
    /// inbound messages and send outbound ones.
    ///
    /// `engine` is the local replication engine that incoming messages are
    /// dispatched to. The listener task holds an `Arc<TcpTransport>` so the
    /// transport can write `VoteResponse` messages back to the candidate
    /// over the same TCP connection.
    ///
    /// Returns an `Arc<TcpTransport>` so the caller can clone-share it
    /// between the binary's main loop and any background task.
    pub async fn bind(
        config: TcpTransportConfig,
        engine: Arc<ReplicationEngine>,
    ) -> Result<Arc<Self>, TransportError> {
        let listener = TcpListener::bind(config.bind_addr)
            .await
            .map_err(|source| TransportError::Bind {
                addr: config.bind_addr,
                source,
            })?;

        let peer_map: HashMap<NodeId, SocketAddr> = config.peers.iter().cloned().collect();

        // Phase 2.4: build the rustls acceptor/connector once here when the
        // selected posture is mTLS (the default), so every accept / dial
        // reuses the same parsed cert chain instead of re-reading PEM files
        // on each connection. The `PskCleartext` opt-in path builds neither
        // and runs frames over raw TCP.
        #[cfg(feature = "cluster-tls")]
        let (tls_acceptor, tls_connector) = match &config.security {
            ClusterSecurityMode::MutualTls(tls_cfg) => {
                let acceptor =
                    load_server_config(tls_cfg).map_err(|e| TransportError::Tls(e.to_string()))?;
                let connector =
                    load_client_config(tls_cfg).map_err(|e| TransportError::Tls(e.to_string()))?;
                (Some(acceptor), Some(connector))
            }
            ClusterSecurityMode::PskCleartext => (None, None),
        };

        let transport = Arc::new(Self {
            config: config.clone(),
            peer_map,
            out_conns: Mutex::new(HashMap::new()),
            out_readers: Mutex::new(HashMap::new()),
            engine: Arc::clone(&engine),
            listener_task: Mutex::new(None),
            tick_task: Mutex::new(None),
            #[cfg(feature = "cluster-tls")]
            tls_connector,
        });

        let transport_for_listener = Arc::clone(&transport);
        let engine_for_listener = Arc::clone(&engine);
        let psk_for_listener = Arc::clone(&config.psk);
        #[cfg(feature = "cluster-tls")]
        let acceptor_for_listener = tls_acceptor;
        let task = tokio::spawn(async move {
            loop {
                let (stream, _peer_addr) = match listener.accept().await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::debug!(error = %e, "listener accept failed; exiting accept loop");
                        break;
                    }
                };
                let transport = Arc::clone(&transport_for_listener);
                let engine = Arc::clone(&engine_for_listener);
                let psk = Arc::clone(&psk_for_listener);

                // Wave 44B: branch on TLS. When `tls_acceptor` is `Some(...)`
                // every inbound `TcpStream` is wrapped by the rustls server
                // handshake before any cluster wire byte is read; clients
                // without a CA-signed cert are refused at the TLS layer and
                // never reach `handle_inbound`. When `None` the raw
                // `TcpStream` is passed straight through (Wave 40-A
                // PSK-over-cleartext posture).
                #[cfg(feature = "cluster-tls")]
                {
                    if let Some(acc) = acceptor_for_listener.clone() {
                        tokio::spawn(async move {
                            match acc.accept(stream).await {
                                Ok(tls_stream) => {
                                    // DD R21: capture the peer certificate's
                                    // identities so the handshake-announced node
                                    // id can be bound to the presented cert. An
                                    // absent client cert yields an empty list,
                                    // which authorizes no node id (fail-closed).
                                    let peer_ids = {
                                        let (_io, conn) = tls_stream.get_ref();
                                        conn.peer_certificates()
                                            .and_then(<[_]>::first)
                                            .map(|leaf| crate::tls::peer_identities(leaf.as_ref()))
                                            .unwrap_or_default()
                                    };
                                    handle_inbound(
                                        tls_stream,
                                        engine,
                                        transport,
                                        psk,
                                        Some(peer_ids),
                                    )
                                    .await;
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        "inbound TLS handshake failed; closing connection",
                                    );
                                }
                            }
                        });
                    } else {
                        tokio::spawn(handle_inbound(stream, engine, transport, psk, None));
                    }
                }
                #[cfg(not(feature = "cluster-tls"))]
                {
                    tokio::spawn(handle_inbound(stream, engine, transport, psk, None));
                }
            }
        });
        *transport.listener_task.lock().await = Some(task);

        Ok(transport)
    }

    /// Wave 44B: open one outbound connection to `peer_id @ addr` and
    /// return type-erased read/write halves.
    ///
    /// In the cleartext path (`tls_connector` is `None`, or the
    /// `cluster-tls` Cargo feature is disabled) this is a plain
    /// `TcpStream` opened with `connect_timeout`, then split via
    /// `tokio::io::split`. In the mTLS path the freshly connected
    /// `TcpStream` is wrapped by the rustls client handshake first and
    /// the resulting `TlsStream<TcpStream>` is split — the cluster
    /// frame layer never sees the raw TCP socket.
    async fn open_outbound(
        &self,
        #[cfg_attr(not(feature = "cluster-tls"), allow(unused_variables))] peer_id: &str,
        addr: SocketAddr,
    ) -> std::io::Result<(
        Pin<Box<dyn AsyncRead + Send + Unpin>>,
        Pin<Box<dyn AsyncWrite + Send + Unpin>>,
    )> {
        let connect =
            tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(addr)).await;
        let stream = match connect {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "connect timeout",
                ));
            }
        };

        #[cfg(feature = "cluster-tls")]
        {
            if let Some(connector) = self.tls_connector.as_ref().cloned() {
                // Wave 44B: SNI value is the peer node id. The peer's
                // server cert must list this name in its SANs (or be
                // localhost / 127.0.0.1 — the test certs cover both).
                // If the operator runs a name that is not in the cert,
                // the handshake fails fast with a clear error here.
                let server_name = match rustls::pki_types::ServerName::try_from(peer_id.to_string())
                {
                    Ok(n) => n,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("invalid TLS server name {peer_id}: {e}"),
                        ));
                    }
                };
                let tls_stream = connector
                    .connect(server_name, stream)
                    .await
                    .map_err(|e| std::io::Error::other(format!("TLS connect failed: {e}")))?;

                // DD R22 (High): rustls verified the server cert chains to the CA
                // and matches the SNI, but SNI matching accepts wildcards — a
                // server holding a valid `*.cluster.local` cert would satisfy a
                // dial to `node-b.cluster.local` and could then masquerade as it.
                // Require the server leaf cert to assert the EXACT peer id (no
                // wildcard), symmetric to the inbound binding, so a dialer only
                // ever talks to the node whose certificate names that exact id.
                let server_ids = {
                    let (_io, conn) = tls_stream.get_ref();
                    conn.peer_certificates()
                        .and_then(<[_]>::first)
                        .map(|leaf| crate::tls::peer_identities(leaf.as_ref()))
                        .unwrap_or_default()
                };
                if !cert_authorizes_node(Some(&server_ids), peer_id) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "TLS peer {peer_id} server certificate does not assert that exact \
                             node id (wildcard/SNI match is insufficient for cluster identity)"
                        ),
                    ));
                }

                let (r, w) = tokio::io::split(tls_stream);
                return Ok((Box::pin(r), Box::pin(w)));
            }
        }

        let (r, w) = tokio::io::split(stream);
        Ok((Box::pin(r), Box::pin(w)))
    }

    /// Send a single [`ReplicationMessage`] to `peer_id` over TCP.
    ///
    /// Best-effort: the transport opens (or reuses) a connection, then writes
    /// the encoded bytes. On any I/O error the cached connection is dropped
    /// and a fresh one is opened on the next call (with exponential back-off
    /// up to 5 s between retries inside the same call, capped at 3 attempts
    /// per call to avoid blocking).
    pub async fn send(
        &self,
        peer_id: &str,
        msg: &ReplicationMessage,
    ) -> Result<(), TransportError> {
        let addr = self
            .peer_map
            .get(peer_id)
            .copied()
            .ok_or_else(|| TransportError::UnknownPeer(peer_id.to_string()))?;

        let bytes = encode_message_authenticated(msg, &self.config.psk)
            .map_err(|e| TransportError::Encode(e.to_string()))?;

        let mut backoff = Duration::from_millis(50);
        let max_backoff = Duration::from_secs(5);
        let mut last_err: Option<std::io::Error> = None;

        for attempt in 0..3 {
            let mut conns = self.out_conns.lock().await;
            let entry = conns.entry(peer_id.to_string()).or_insert(None);
            if entry.is_none() {
                // Wave 44B: open a fresh connection. The helper handles
                // both the cleartext path (raw TcpStream) and the mTLS
                // path (rustls handshake on top of TcpStream); it returns
                // type-erased read/write halves so this loop does not
                // need to know which it got.
                let connect_result = self.open_outbound(peer_id, addr).await;
                match connect_result {
                    Ok((r, mut w)) => {
                        // Wave 40-A: send the authenticated handshake
                        // BEFORE any business message. If the handshake
                        // write fails treat it like any other connect
                        // failure — drop the half and retry.
                        let handshake = HandshakeFrame {
                            announced_node_id: self.config.local_node_id.clone(),
                        };
                        let handshake_bytes = match serde_json::to_vec(&handshake) {
                            Ok(b) => b,
                            Err(e) => {
                                last_err = Some(std::io::Error::other(format!(
                                    "encode handshake failed: {e}",
                                )));
                                drop(conns);
                                if attempt < 2 {
                                    tokio::time::sleep(backoff).await;
                                    backoff = (backoff * 2).min(max_backoff);
                                }
                                continue;
                            }
                        };
                        let framed_handshake = match encode_authenticated_payload(
                            &handshake_bytes,
                            &self.config.psk,
                        ) {
                            Ok(b) => b,
                            Err(e) => {
                                last_err = Some(std::io::Error::other(format!(
                                    "frame handshake failed: {e}",
                                )));
                                drop(conns);
                                if attempt < 2 {
                                    tokio::time::sleep(backoff).await;
                                    backoff = (backoff * 2).min(max_backoff);
                                }
                                continue;
                            }
                        };
                        if let Err(e) = w.write_all(&framed_handshake).await {
                            last_err = Some(e);
                            drop(conns);
                            if attempt < 2 {
                                tokio::time::sleep(backoff).await;
                                backoff = (backoff * 2).min(max_backoff);
                            }
                            continue;
                        }
                        if let Err(e) = w.flush().await {
                            last_err = Some(e);
                            drop(conns);
                            if attempt < 2 {
                                tokio::time::sleep(backoff).await;
                                backoff = (backoff * 2).min(max_backoff);
                            }
                            continue;
                        }

                        *entry = Some(w);
                        let engine_for_reader = Arc::clone(&self.engine);
                        let psk_for_reader = Arc::clone(&self.config.psk);
                        let peer_id_for_reader = peer_id.to_string();
                        let reader = tokio::spawn(handle_outbound_reader(
                            r,
                            engine_for_reader,
                            psk_for_reader,
                            peer_id_for_reader,
                        ));
                        // Replace any stale reader handle. If a prior task
                        // existed it is aborted to avoid leaking when the
                        // connection was reopened after error.
                        let mut readers = self.out_readers.lock().await;
                        if let Some(prev) = readers.insert(peer_id.to_string(), reader) {
                            prev.abort();
                        }
                    }
                    Err(e) => {
                        last_err = Some(e);
                        drop(conns);
                        if attempt < 2 {
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(max_backoff);
                        }
                        continue;
                    }
                }
            }
            let stream = match entry.as_mut() {
                Some(s) => s,
                None => continue,
            };
            if let Err(e) = stream.write_all(&bytes).await {
                *entry = None;
                last_err = Some(e);
                drop(conns);
                if attempt < 2 {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                }
                continue;
            }
            if let Err(e) = stream.flush().await {
                *entry = None;
                last_err = Some(e);
                drop(conns);
                if attempt < 2 {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                }
                continue;
            }
            return Ok(());
        }

        Err(TransportError::Send {
            peer: peer_id.to_string(),
            source: last_err
                .unwrap_or_else(|| std::io::Error::other("send failed without recorded io error")),
        })
    }

    /// Broadcast a message to every configured peer. Errors per peer are
    /// logged but do not abort the broadcast.
    pub async fn broadcast(&self, msg: &ReplicationMessage) {
        for (peer_id, _) in &self.config.peers {
            if let Err(e) = self.send(peer_id, msg).await {
                tracing::debug!(peer = %peer_id, error = %e, "broadcast send failed");
            }
        }
    }

    /// Spawn a background task that drives [`ReplicationEngine::tick`] every
    /// `cadence` and translates the returned [`TickAction`] into wire I/O.
    ///
    /// Wave 38-C: this is the production-side replacement for the old
    /// `node_id.ends_with('1')` bootstrap hack in `bins/ferrodruid/src/main.rs`.
    /// On expiry of the engine's election timer, this task broadcasts a
    /// pre-vote (Wave 47-A) or real `VoteRequest` to every peer; on a
    /// leader's heartbeat ticker fire it broadcasts an empty `Heartbeat`.
    ///
    /// **Wave 47-A replay scan** — every [`REPLAY_TICK_INTERVAL`] ticks
    /// (~500 ms at the default 50 ms cadence) the leader walks
    /// `next_index` via [`ReplicationEngine::build_replay_actions`] and
    /// sends per-follower `ReplicateCommand` frames to back-fill any log
    /// gap.  This closes the W38-DE honest gap "replay loop is engine-
    /// side only" — the leader's tick loop now automatically repairs a
    /// follower that came back from a crash with a truncated log.
    /// Non-leader nodes return an empty `Vec` from `build_replay_actions`,
    /// so the scan is cheap.
    ///
    /// Safe to call exactly once per transport. A second call replaces the
    /// previous task handle and aborts the old one.
    /// Count of peers with a live outbound connection (i.e. the TLS +
    /// auth handshake completed and the write-half is cached). Used by
    /// [`Self::spawn_tick_loop`] to signal
    /// [`ReplicationEngine::clear_startup_grace`] as soon as a
    /// majority of peers are reachable, closing the W2-E RCA mitigation
    /// (c) source-side. Excludes self.
    pub async fn authenticated_peer_count(&self) -> usize {
        self.out_conns
            .lock()
            .await
            .values()
            .filter(|slot| slot.is_some())
            .count()
    }

    /// Spawn the transport's tick loop task. Called by
    /// `bins/ferrodruid/src/main.rs` after `bind` completes; drives
    /// [`ReplicationEngine::tick`] every `cadence` and dispatches each
    /// returned [`TickAction`] over the wire. Also checks the mTLS
    /// startup grace window on every iteration (Task #23) and calls
    /// [`ReplicationEngine::clear_startup_grace`] as soon as a
    /// majority of peers are authenticated. Idempotent per-call: a
    /// second `spawn_tick_loop` aborts the previous task and replaces
    /// it.
    pub async fn spawn_tick_loop(
        self: &Arc<Self>,
        engine: Arc<ReplicationEngine>,
        cadence: Duration,
    ) {
        let transport = Arc::clone(self);
        let task = tokio::spawn(async move {
            let mut tick_count: u64 = 0;
            loop {
                // CL-A1-R-mTLS-source-fix (c) startup-delay (Task
                // #23, W2-E RCA mitigation c): if the engine is
                // still inside the mTLS startup grace window and a
                // majority of peers have completed their handshakes,
                // signal the engine to allow elections immediately.
                // Compares authenticated peers + 1 (self) against
                // majority (`floor((peers+1)/2) + 1`) — for a 3-node
                // cluster (2 peers), majority = 2 = self + 1
                // authenticated peer.
                if engine.startup_grace_deadline().await.is_some() {
                    let peer_total = transport.peer_map.len();
                    let cluster_size = peer_total + 1;
                    let majority = cluster_size / 2 + 1;
                    let authenticated = transport.authenticated_peer_count().await + 1;
                    if authenticated >= majority {
                        engine.clear_startup_grace().await;
                    }
                }

                let action = engine.tick().await;
                match action {
                    TickAction::Idle => {}
                    TickAction::BroadcastPreVoteRequest {
                        candidate_id,
                        proposed_term,
                        round_id,
                    } => {
                        let msg = ReplicationMessage::RequestPreVote {
                            candidate_id,
                            proposed_term,
                            round_id,
                        };
                        transport.broadcast(&msg).await;
                    }
                    TickAction::BroadcastVoteRequest { candidate_id, term } => {
                        let msg = ReplicationMessage::VoteRequest { candidate_id, term };
                        transport.broadcast(&msg).await;
                    }
                    TickAction::BroadcastHeartbeat { leader_id, term } => {
                        let msg = ReplicationMessage::Heartbeat { leader_id, term };
                        transport.broadcast(&msg).await;
                    }
                }

                // Wave 47-A: leader-side periodic replay scan.  No-op on
                // followers (engine returns an empty Vec).
                tick_count = tick_count.wrapping_add(1);
                if tick_count.is_multiple_of(REPLAY_TICK_INTERVAL) {
                    let replay = engine.build_replay_actions().await;
                    for (follower_id, msg) in replay {
                        if let Err(e) = transport.send(&follower_id, &msg).await {
                            tracing::debug!(
                                peer = %follower_id,
                                error = %e,
                                "tick-loop replay send failed",
                            );
                        }
                    }
                }

                tokio::time::sleep(cadence).await;
            }
        });
        let prev = {
            let mut slot = self.tick_task.lock().await;
            slot.replace(task)
        };
        if let Some(old) = prev {
            old.abort();
            let _ = old.await;
        }
    }

    /// Shut down the listener and drop all cached outbound connections.
    ///
    /// Safe to call multiple times; subsequent calls are no-ops.
    pub async fn shutdown(self: Arc<Self>) {
        let task = self.listener_task.lock().await.take();
        if let Some(t) = task {
            t.abort();
            let _ = t.await;
        }
        let tick = self.tick_task.lock().await.take();
        if let Some(t) = tick {
            t.abort();
            let _ = t.await;
        }
        // Wave 38-C: abort outbound reader tasks before dropping the write
        // halves so they don't observe a half-closed socket and log noise.
        {
            let mut readers = self.out_readers.lock().await;
            for (_id, h) in readers.drain() {
                h.abort();
                let _ = h.await;
            }
        }
        let mut conns = self.out_conns.lock().await;
        for slot in conns.values_mut() {
            if let Some(stream) = slot.take() {
                // best-effort graceful close
                drop(stream);
            }
        }
        conns.clear();
    }

    /// Number of peers configured (used by tests).
    #[doc(hidden)]
    pub fn peer_count(&self) -> usize {
        self.config.peers.len()
    }
}

/// Wave 40-A helper: read one length-prefixed authenticated frame from a
/// half-stream into a returned `Vec<u8>` containing the full frame
/// (length prefix + tag + payload). Returns `Ok(None)` on a clean EOF.
async fn read_authenticated_frame<R>(reader: &mut R) -> std::io::Result<Option<Vec<u8>>>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    if let Err(e) = reader.read_exact(&mut len_buf).await {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(e);
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BODY {
        return Err(std::io::Error::other(format!(
            "oversized frame body: {len} bytes",
        )));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    let mut full = Vec::with_capacity(4 + len);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&payload);
    Ok(Some(full))
}

/// Wave 38-C / Wave 40-A: read length-prefixed authenticated messages
/// from the read half of an outbound connection. The first authenticated
/// frame is expected to be a [`HandshakeFrame`] (the peer's handshake
/// reply, if any) and is otherwise ignored — the binding for an outbound
/// connection is established by the local node's own handshake on the
/// write half. Frames whose HMAC fails to verify or whose declared
/// `sender_id` does not match the peer the connection was opened to are
/// dropped with a warn.
async fn handle_outbound_reader<R>(
    mut reader: R,
    engine: Arc<ReplicationEngine>,
    psk: Arc<ClusterPsk>,
    expected_peer: NodeId,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    loop {
        let frame = match read_authenticated_frame(&mut reader).await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(error = %e, peer = %expected_peer, "outbound reader I/O error; closing");
                break;
            }
        };
        let (json, _tag) = match authenticated_payload_bytes(&frame, &psk) {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!(
                    peer = %expected_peer,
                    error = %e,
                    "outbound reader: HMAC verification failed; closing connection",
                );
                break;
            }
        };
        // Try parse as a handshake (the peer can optionally send one).
        // A handshake on the read side is informational; the binding is
        // already enforced by the connection-targeted dial — we know
        // which peer we connected to.
        if serde_json::from_slice::<HandshakeFrame>(json).is_ok() {
            continue;
        }
        let msg: ReplicationMessage = match serde_json::from_slice(json) {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!(error = %e, peer = %expected_peer, "outbound reader: payload decode failed; skipping frame");
                continue;
            }
        };
        // Wave 40-A: enforce that the sender_id in the JSON matches the
        // peer this socket is connected to. Anything else is a forged
        // claim from an authenticated peer trying to impersonate a
        // different node id over its own session.
        if let Some(declared) = msg.declared_sender_id()
            && declared != expected_peer
        {
            tracing::warn!(
                declared,
                expected = %expected_peer,
                "outbound reader: declared sender_id does not match peer; dropping",
            );
            continue;
        }

        match msg {
            ReplicationMessage::VoteResponse {
                voter_id,
                term,
                granted,
            } => {
                if engine.receive_vote_response(&voter_id, term, granted).await
                    && engine.try_promote_to_leader().await
                {
                    tracing::info!(term, "promoted to leader on vote response (outbound)");
                }
            }
            ReplicationMessage::Heartbeat { leader_id, term } => {
                let _ = engine.receive_heartbeat(&leader_id, term).await;
            }
            ReplicationMessage::ReplicateCommand {
                term,
                index,
                command,
            } => {
                let _ = engine.receive_command(term, index, command).await;
            }
            ReplicationMessage::VoteRequest { candidate_id, term } => {
                tracing::debug!(
                    candidate_id,
                    term,
                    "VoteRequest seen on outbound reader; ignoring (peer should use inbound path)",
                );
            }
            ReplicationMessage::ReplicateAck {
                follower_id,
                index,
                success,
                last_log_index_hint,
            } => {
                engine
                    .receive_replicate_ack(&follower_id, index, success, last_log_index_hint)
                    .await;
            }
            ReplicationMessage::SnapshotResponse {
                snapshot,
                last_index,
                term,
            } => {
                if let Err(e) = engine
                    .handle_snapshot_response(snapshot, last_index, term)
                    .await
                {
                    tracing::debug!(error = %e, "snapshot apply failed");
                }
            }
            ReplicationMessage::SnapshotRequest { .. } | ReplicationMessage::Join { .. } => {
                tracing::debug!("outbound reader: control message ignored (server-side only)");
            }
            ReplicationMessage::RequestPreVoteResponse {
                voter_id,
                proposed_term,
                granted,
                round_id,
            } => {
                let _ = engine
                    .receive_pre_vote_response(&voter_id, proposed_term, granted, round_id)
                    .await;
            }
            ReplicationMessage::RequestPreVote { .. } => {
                tracing::debug!(
                    "RequestPreVote seen on outbound reader; ignoring (peer should use inbound)",
                );
            }
            // W1-C: chunked snapshot transfer. Outbound reader sees
            // these on the follower side (leader pushes
            // SnapshotChunk; follower replies on the same
            // connection with SnapshotChunkAck — that ack comes back
            // through the inbound reader, not here).
            ReplicationMessage::SnapshotChunk {
                transfer_id,
                chunk_index,
                total_chunks,
                is_final,
                last_index,
                term,
                total_bytes,
                payload,
            } => {
                if let Err(e) = engine
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
                {
                    tracing::debug!(
                        transfer_id,
                        chunk_index,
                        error = %e,
                        "snapshot chunk apply failed (outbound reader path)",
                    );
                }
            }
            ReplicationMessage::SnapshotChunkAck {
                follower_id,
                transfer_id,
                last_received_chunk,
                applied,
            } => {
                engine
                    .receive_snapshot_chunk_ack(
                        &follower_id,
                        transfer_id,
                        last_received_chunk,
                        applied,
                    )
                    .await;
            }
        }
    }
}

/// Wave 40-A: read the first authenticated frame on `stream` and parse it
/// as a [`HandshakeFrame`]. Returns the announced node id on success;
/// returns `None` on any error (frame too short, HMAC mismatch, payload
/// not a handshake) so the listener task can close the connection.
async fn read_handshake<S>(stream: &mut S, psk: &ClusterPsk) -> Option<String>
where
    S: AsyncRead + Unpin,
{
    let frame = match read_authenticated_frame(stream).await {
        Ok(Some(f)) => f,
        Ok(None) => {
            tracing::debug!("inbound: connection closed before handshake");
            return None;
        }
        Err(e) => {
            tracing::warn!(error = %e, "inbound: handshake read I/O error");
            return None;
        }
    };
    let (json, _tag) = match authenticated_payload_bytes(&frame, psk) {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "inbound: handshake HMAC verification failed; closing connection",
            );
            return None;
        }
    };
    let handshake: HandshakeFrame = match serde_json::from_slice(json) {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "inbound: first frame was not a HandshakeFrame; closing connection",
            );
            return None;
        }
    };
    if handshake.announced_node_id.is_empty() {
        tracing::warn!("inbound: handshake announced empty node id; closing connection");
        return None;
    }
    Some(handshake.announced_node_id)
}

/// Read length-prefixed authenticated messages from `stream` and dispatch
/// them into the engine. For `VoteRequest` the response is written back
/// over the same connection so the candidate observes votes without
/// out-of-band plumbing.
///
/// Wave 40-A: rejects any frame whose HMAC does not validate against the
/// shared cluster PSK, closes the connection if the handshake is missing
/// or malformed, and drops any subsequent message whose declared
/// `sender_id` does not match the handshake's `announced_node_id`.
/// Whether a TLS peer whose certificate asserts `peer_identities` (its SAN/CN
/// values) may announce the cluster node id `announced`.
///
/// In the explicit PSK-cleartext fallback there is no peer certificate
/// (`None`), so the shared PSK + per-connection sender binding remain the only
/// guard. In mTLS mode (`Some(..)`) the announced id MUST be one of the
/// certificate's identities, so one credential holder cannot impersonate another
/// node id (DD R21). An empty identity list therefore authorizes nothing —
/// fail-closed.
fn cert_authorizes_node(peer_identities: Option<&[String]>, announced: &str) -> bool {
    match peer_identities {
        None => true,
        Some(ids) => ids.iter().any(|id| id == announced),
    }
}

async fn handle_inbound<S>(
    mut stream: S,
    engine: Arc<ReplicationEngine>,
    _transport: Arc<TcpTransport>,
    psk: Arc<ClusterPsk>,
    peer_identities: Option<Vec<String>>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let announced_node_id = match read_handshake(&mut stream, &psk).await {
        Some(id) => id,
        None => return,
    };

    // DD R21 (High): in mTLS mode bind the self-declared handshake node id to the
    // TLS peer certificate identity. `peer_identities` is `Some(..)` only on the
    // mTLS path; the announced id must be one of the certificate's SAN/CN values,
    // otherwise a holder of one valid node cert + the shared PSK could announce a
    // DIFFERENT node id and forge quorum acks/votes as that node. `None` is the
    // explicit PSK-cleartext fallback where there is no certificate to bind to.
    if !cert_authorizes_node(peer_identities.as_deref(), &announced_node_id) {
        tracing::warn!(
            announced = %announced_node_id,
            "inbound: TLS peer certificate does not assert the announced node id; \
             closing connection",
        );
        return;
    }
    tracing::debug!(peer = %announced_node_id, "inbound: handshake authenticated");

    loop {
        let frame = match read_authenticated_frame(&mut stream).await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(
                    peer = %announced_node_id,
                    error = %e,
                    "inbound reader I/O error; closing",
                );
                break;
            }
        };
        let (json, _tag) = match authenticated_payload_bytes(&frame, &psk) {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!(
                    peer = %announced_node_id,
                    error = %e,
                    "inbound: HMAC verification failed; closing connection",
                );
                break;
            }
        };
        let msg: ReplicationMessage = match serde_json::from_slice(json) {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!(
                    peer = %announced_node_id,
                    error = %e,
                    "inbound: payload decode failed; skipping frame",
                );
                continue;
            }
        };

        // Wave 40-A connection binding: every message that carries a
        // sender id must match the handshake. This is the line that
        // closes the W39 NEW Critical (forged ReplicateAck) and High
        // (forged heartbeat / VoteRequest) findings.
        if let Some(declared) = msg.declared_sender_id()
            && declared != announced_node_id
        {
            tracing::warn!(
                declared,
                announced = %announced_node_id,
                "inbound: sender_id does not match handshake-announced node; dropping",
            );
            continue;
        }

        match msg {
            ReplicationMessage::Heartbeat { leader_id, term } => {
                let _ = engine.receive_heartbeat(&leader_id, term).await;
            }
            ReplicationMessage::ReplicateCommand {
                term,
                index,
                command,
            } => {
                // Wave 38-DE: process_replicate_command returns the
                // ReplicateAck the leader expects.  We write it back over
                // the same connection so the leader's outbound reader
                // task feeds it into `receive_replicate_ack`.
                let resp = engine.process_replicate_command(term, index, command).await;
                let bytes = match encode_message_authenticated(&resp, &psk) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::debug!(error = %e, "encode ReplicateAck failed");
                        continue;
                    }
                };
                if stream.write_all(&bytes).await.is_err() {
                    break;
                }
                if stream.flush().await.is_err() {
                    break;
                }
            }
            ReplicationMessage::VoteRequest { candidate_id, term } => {
                let resp = engine.receive_vote_request(&candidate_id, term).await;
                // Write the response back on the same connection.
                let bytes = match encode_message_authenticated(&resp, &psk) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::debug!(error = %e, "encode VoteResponse failed");
                        continue;
                    }
                };
                if stream.write_all(&bytes).await.is_err() {
                    break;
                }
                if stream.flush().await.is_err() {
                    break;
                }
            }
            ReplicationMessage::VoteResponse {
                voter_id,
                term,
                granted,
            } => {
                if engine.receive_vote_response(&voter_id, term, granted).await {
                    // A new vote was counted; check if it carries us over
                    // the majority threshold.
                    if engine.try_promote_to_leader().await {
                        tracing::info!(term, "promoted to leader on vote response");
                    }
                }
            }
            ReplicationMessage::ReplicateAck {
                follower_id,
                index,
                success,
                last_log_index_hint,
            } => {
                engine
                    .receive_replicate_ack(&follower_id, index, success, last_log_index_hint)
                    .await;
            }
            ReplicationMessage::SnapshotRequest { follower_id } => {
                if let Some(resp) = engine.handle_snapshot_request(&follower_id).await {
                    let bytes = match encode_message_authenticated(&resp, &psk) {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::debug!(error = %e, "encode SnapshotResponse failed");
                            continue;
                        }
                    };
                    if stream.write_all(&bytes).await.is_err() {
                        break;
                    }
                    if stream.flush().await.is_err() {
                        break;
                    }
                }
            }
            ReplicationMessage::SnapshotResponse {
                snapshot,
                last_index,
                term,
            } => {
                if let Err(e) = engine
                    .handle_snapshot_response(snapshot, last_index, term)
                    .await
                {
                    tracing::debug!(error = %e, "snapshot apply failed");
                }
            }
            ReplicationMessage::Join { node_id, addr } => {
                if let Some(resp) = engine.handle_join(&node_id, &addr).await {
                    let bytes = match encode_message_authenticated(&resp, &psk) {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::debug!(error = %e, "encode Join response failed");
                            continue;
                        }
                    };
                    if stream.write_all(&bytes).await.is_err() {
                        break;
                    }
                    if stream.flush().await.is_err() {
                        break;
                    }
                }
            }
            ReplicationMessage::RequestPreVote {
                candidate_id,
                proposed_term,
                round_id,
            } => {
                let resp = engine
                    .receive_pre_vote_request(&candidate_id, proposed_term, round_id)
                    .await;
                let bytes = match encode_message_authenticated(&resp, &psk) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::debug!(error = %e, "encode RequestPreVoteResponse failed");
                        continue;
                    }
                };
                if stream.write_all(&bytes).await.is_err() {
                    break;
                }
                if stream.flush().await.is_err() {
                    break;
                }
            }
            ReplicationMessage::RequestPreVoteResponse {
                voter_id,
                proposed_term,
                granted,
                round_id,
            } => {
                let _ = engine
                    .receive_pre_vote_response(&voter_id, proposed_term, granted, round_id)
                    .await;
            }
            // W1-C: chunked snapshot transfer on the inbound (server)
            // path. The follower's listener writes a
            // SnapshotChunkAck back over the SAME inbound connection
            // so the leader's outbound reader can update its
            // resume cursor.
            ReplicationMessage::SnapshotChunk {
                transfer_id,
                chunk_index,
                total_chunks,
                is_final,
                last_index,
                term,
                total_bytes,
                payload,
            } => {
                let ack = match engine
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
                {
                    Ok(ack) => ack,
                    Err(e) => {
                        tracing::debug!(
                            transfer_id,
                            chunk_index,
                            error = %e,
                            "snapshot chunk apply failed (inbound path); skipping ack",
                        );
                        continue;
                    }
                };
                let bytes = match encode_message_authenticated(&ack, &psk) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::debug!(error = %e, "encode SnapshotChunkAck failed");
                        continue;
                    }
                };
                if stream.write_all(&bytes).await.is_err() {
                    break;
                }
                if stream.flush().await.is_err() {
                    break;
                }
            }
            ReplicationMessage::SnapshotChunkAck {
                follower_id,
                transfer_id,
                last_received_chunk,
                applied,
            } => {
                engine
                    .receive_snapshot_chunk_ack(
                        &follower_id,
                        transfer_id,
                        last_received_chunk,
                        applied,
                    )
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;
    use crate::auth::derive_psk;
    use crate::replication::{ClusterSecurityHint, ReplicationConfig, ReplicationEngine};
    use crate::{ClusterManager, NodeInfo, NodeRole};

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
            cluster_security_hint: ClusterSecurityHint::Psk,
        };
        Arc::new(ReplicationEngine::new(config, cm))
    }

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn test_psk() -> Arc<ClusterPsk> {
        Arc::new(derive_psk("test-cluster-psk-Wave-40A").expect("derive"))
    }

    #[test]
    fn cert_authorizes_node_binds_announced_id_to_cert() {
        // PSK-cleartext fallback: no peer cert -> authorized (PSK + sender
        // binding remain the guard).
        assert!(cert_authorizes_node(None, "node-1"));
        // mTLS: the announced id must be one of the cert identities.
        let ids = vec!["node-1".to_string(), "localhost".to_string()];
        assert!(cert_authorizes_node(Some(&ids), "node-1"));
        assert!(cert_authorizes_node(Some(&ids), "localhost"));
        assert!(
            !cert_authorizes_node(Some(&ids), "node-2"),
            "an id absent from the cert must be rejected",
        );
        // An empty identity list authorizes nothing (fail-closed).
        assert!(!cert_authorizes_node(Some(&[]), "node-1"));
    }

    fn cfg(
        bind: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
        local_node_id: &str,
        psk: Arc<ClusterPsk>,
    ) -> TcpTransportConfig {
        TcpTransportConfig {
            bind_addr: bind,
            peers,
            connect_timeout: Duration::from_millis(200),
            heartbeat_period: Duration::from_millis(100),
            psk,
            local_node_id: local_node_id.to_string(),
            security: ClusterSecurityMode::PskCleartext,
        }
    }

    #[tokio::test]
    async fn transport_bind_succeeds_on_loopback() {
        let engine = make_engine("node-1", vec![]);
        let listener = TcpListener::bind(loopback(0)).await.expect("scratch bind");
        let bound = listener.local_addr().expect("local_addr");
        drop(listener);

        let psk = test_psk();
        let conf = cfg(bound, Vec::new(), "node-1", psk);
        let transport = TcpTransport::bind(conf, engine).await.expect("bind");
        assert_eq!(transport.peer_count(), 0);
        Arc::clone(&transport).shutdown().await;
    }

    #[tokio::test]
    async fn transport_send_to_unreachable_peer_returns_error_not_panic() {
        let engine = make_engine("node-1", vec!["node-2"]);
        // Pick a port that is almost certainly unbound.
        let dead_peer = loopback(1);
        let psk = test_psk();
        let mut conf = cfg(
            loopback(0),
            vec![("node-2".to_string(), dead_peer)],
            "node-1",
            psk,
        );
        conf.connect_timeout = Duration::from_millis(80);
        let transport = TcpTransport::bind(conf, engine).await.expect("bind");

        let msg = ReplicationMessage::Heartbeat {
            leader_id: "node-1".to_string(),
            term: 1,
        };
        let result = transport.send("node-2", &msg).await;
        assert!(result.is_err(), "send to unreachable peer must return Err");
        match result {
            Err(TransportError::Send { peer, .. }) => assert_eq!(peer, "node-2"),
            other => panic!("expected Send error, got {other:?}"),
        }

        // unknown peer returns a different variant
        let unknown = transport.send("ghost", &msg).await;
        assert!(matches!(unknown, Err(TransportError::UnknownPeer(_))));

        Arc::clone(&transport).shutdown().await;
    }

    #[tokio::test]
    async fn transport_shutdown_drops_all_connections() {
        let engine_a = make_engine("node-a", vec!["node-b"]);
        let engine_b = make_engine("node-b", vec!["node-a"]);

        let l_a = TcpListener::bind(loopback(0)).await.expect("scratch a");
        let addr_a = l_a.local_addr().expect("addr_a");
        drop(l_a);
        let l_b = TcpListener::bind(loopback(0)).await.expect("scratch b");
        let addr_b = l_b.local_addr().expect("addr_b");
        drop(l_b);

        let psk = test_psk();
        let cfg_a = cfg(
            addr_a,
            vec![("node-b".to_string(), addr_b)],
            "node-a",
            Arc::clone(&psk),
        );
        let cfg_b = cfg(
            addr_b,
            vec![("node-a".to_string(), addr_a)],
            "node-b",
            Arc::clone(&psk),
        );

        let t_a = TcpTransport::bind(cfg_a, engine_a).await.expect("bind a");
        let t_b = TcpTransport::bind(cfg_b, engine_b).await.expect("bind b");

        // Force-open a connection from a -> b.
        let msg = ReplicationMessage::Heartbeat {
            leader_id: "node-a".to_string(),
            term: 1,
        };
        t_a.send("node-b", &msg).await.expect("send a->b");
        // wait for inbound to be processed
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Shut down a; cached connection must be dropped.
        Arc::clone(&t_a).shutdown().await;
        let conns_after = t_a.out_conns.lock().await;
        assert!(
            conns_after.values().all(|s| s.is_none()),
            "no cached connections must remain open after shutdown",
        );
        drop(conns_after);
        Arc::clone(&t_b).shutdown().await;
    }

    #[tokio::test]
    async fn transport_handles_concurrent_sends() {
        let engine_a = make_engine("node-a", vec!["node-b"]);
        let engine_b = make_engine("node-b", vec!["node-a"]);

        let l_a = TcpListener::bind(loopback(0)).await.expect("scratch a");
        let addr_a = l_a.local_addr().expect("addr_a");
        drop(l_a);
        let l_b = TcpListener::bind(loopback(0)).await.expect("scratch b");
        let addr_b = l_b.local_addr().expect("addr_b");
        drop(l_b);

        let psk = test_psk();
        let mut cfg_a = cfg(
            addr_a,
            vec![("node-b".to_string(), addr_b)],
            "node-a",
            Arc::clone(&psk),
        );
        cfg_a.connect_timeout = Duration::from_millis(500);
        let mut cfg_b = cfg(
            addr_b,
            vec![("node-a".to_string(), addr_a)],
            "node-b",
            Arc::clone(&psk),
        );
        cfg_b.connect_timeout = Duration::from_millis(500);

        let t_a = TcpTransport::bind(cfg_a, engine_a).await.expect("bind a");
        let t_b = TcpTransport::bind(cfg_b, Arc::clone(&engine_b))
            .await
            .expect("bind b");

        // Fire 32 concurrent heartbeats from a -> b. None should panic and
        // engine_b must observe term advance.
        let mut handles = Vec::new();
        for i in 0..32u64 {
            let t_a = Arc::clone(&t_a);
            handles.push(tokio::spawn(async move {
                let msg = ReplicationMessage::Heartbeat {
                    leader_id: "node-a".to_string(),
                    term: i + 1,
                };
                let _ = t_a.send("node-b", &msg).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }

        // Allow inbound dispatch to drain.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let term_b = engine_b.term().await;
        assert!(
            term_b > 0,
            "engine_b should have observed at least one heartbeat term advance, got {term_b}",
        );

        Arc::clone(&t_a).shutdown().await;
        Arc::clone(&t_b).shutdown().await;
    }

    // -----------------------------------------------------------------
    // Wave 40-A authentication tests
    // -----------------------------------------------------------------

    /// Helper that opens a raw TCP connection to `addr`, lets the caller
    /// write arbitrary bytes (typically a forged frame), and returns
    /// after a short delay so the listener task has a chance to read.
    async fn raw_send(addr: SocketAddr, bytes: &[u8]) -> std::io::Result<()> {
        let mut s = TcpStream::connect(addr).await?;
        s.write_all(bytes).await?;
        s.flush().await?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        Ok(())
    }

    #[tokio::test]
    async fn transport_rejects_message_with_invalid_hmac() {
        // A node bound with PSK "good" must drop a frame that was HMAC-ed
        // with PSK "bad" (the body is a well-formed Heartbeat).
        let engine = make_engine("node-rcv", vec![]);

        let l = TcpListener::bind(loopback(0)).await.expect("scratch");
        let addr = l.local_addr().expect("addr");
        drop(l);

        let good_psk = Arc::new(derive_psk("good-cluster-psk").expect("derive good"));
        let bad_psk = derive_psk("attacker-psk").expect("derive bad");

        let conf = cfg(addr, Vec::new(), "node-rcv", Arc::clone(&good_psk));
        let transport = TcpTransport::bind(conf, Arc::clone(&engine))
            .await
            .expect("bind");

        // Forge: hand-build a frame using bad PSK for the handshake +
        // heartbeat. Receiver must close the connection at the
        // handshake step; engine term must remain 0.
        let handshake_bytes = serde_json::to_vec(&HandshakeFrame {
            announced_node_id: "node-attacker".to_string(),
        })
        .expect("encode handshake");
        let bad_handshake = encode_authenticated_payload(&handshake_bytes, &bad_psk)
            .expect("frame handshake (bad psk)");

        let _ = raw_send(addr, &bad_handshake).await;
        // Engine must still be at term 0 — no heartbeat applied.
        let t = engine.term().await;
        assert_eq!(
            t, 0,
            "engine term must NOT advance on HMAC-mismatch frame, got {t}",
        );

        Arc::clone(&transport).shutdown().await;
    }

    #[tokio::test]
    async fn transport_rejects_handshake_with_mismatched_hmac() {
        // Same as above but uses the correct HMAC on the heartbeat and
        // a bad HMAC on the handshake. Verifies that the handshake
        // gate fails closed.
        let engine = make_engine("node-rcv", vec![]);

        let l = TcpListener::bind(loopback(0)).await.expect("scratch");
        let addr = l.local_addr().expect("addr");
        drop(l);

        let good_psk = Arc::new(derive_psk("good-cluster-psk").expect("derive"));
        let bad_psk = derive_psk("attacker-psk").expect("derive bad");
        let conf = cfg(addr, Vec::new(), "node-rcv", Arc::clone(&good_psk));
        let transport = TcpTransport::bind(conf, Arc::clone(&engine))
            .await
            .expect("bind");

        // Bad-HMAC handshake.
        let handshake_bytes = serde_json::to_vec(&HandshakeFrame {
            announced_node_id: "node-leader".to_string(),
        })
        .expect("encode handshake");
        let bad_handshake = encode_authenticated_payload(&handshake_bytes, &bad_psk)
            .expect("frame handshake (bad psk)");

        // Good-HMAC heartbeat (would otherwise advance term to 7).
        let hb = ReplicationMessage::Heartbeat {
            leader_id: "node-leader".to_string(),
            term: 7,
        };
        let good_hb =
            encode_message_authenticated(&hb, &good_psk).expect("encode hb under good psk");

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&bad_handshake);
        bytes.extend_from_slice(&good_hb);
        let _ = raw_send(addr, &bytes).await;

        let t = engine.term().await;
        assert_eq!(
            t, 0,
            "term must NOT advance when handshake HMAC fails, got {t}",
        );

        Arc::clone(&transport).shutdown().await;
    }

    #[tokio::test]
    async fn transport_rejects_subsequent_message_with_different_sender_id() {
        // Wave 40-A connection-binding test: an authenticated peer that
        // announces "node-leader" in the handshake and then sends a
        // ReplicateAck claiming follower_id = "node-other" must have
        // the second frame dropped.
        let engine = make_engine("node-rcv", vec!["node-leader", "node-other"]);

        let l = TcpListener::bind(loopback(0)).await.expect("scratch");
        let addr = l.local_addr().expect("addr");
        drop(l);

        let psk = Arc::new(derive_psk("good-cluster-psk").expect("derive"));
        let conf = cfg(addr, Vec::new(), "node-rcv", Arc::clone(&psk));
        let transport = TcpTransport::bind(conf, Arc::clone(&engine))
            .await
            .expect("bind");

        // Good handshake announcing node-leader.
        let handshake_bytes = serde_json::to_vec(&HandshakeFrame {
            announced_node_id: "node-leader".to_string(),
        })
        .expect("encode handshake");
        let handshake_frame =
            encode_authenticated_payload(&handshake_bytes, &psk).expect("frame handshake");

        // Heartbeat from "node-leader" (matches handshake) at term 5 —
        // this MUST be applied.
        let hb = ReplicationMessage::Heartbeat {
            leader_id: "node-leader".to_string(),
            term: 5,
        };
        let hb_frame = encode_message_authenticated(&hb, &psk).expect("encode hb");

        // Forged ReplicateAck claiming follower_id = "node-other"
        // (mismatch with handshake) — this MUST be dropped, so
        // match_index_for("node-other") stays None.
        let forged_ack = ReplicationMessage::ReplicateAck {
            follower_id: "node-other".to_string(),
            index: 99,
            success: true,
            last_log_index_hint: None,
        };
        let forged_frame = encode_message_authenticated(&forged_ack, &psk).expect("encode ack");

        // Become leader at the same term so receive_replicate_ack would
        // otherwise mutate match_index.
        engine.force_leader_with_term(5).await;

        let mut buf = Vec::new();
        buf.extend_from_slice(&handshake_frame);
        buf.extend_from_slice(&hb_frame);
        buf.extend_from_slice(&forged_frame);
        let _ = raw_send(addr, &buf).await;
        tokio::time::sleep(Duration::from_millis(60)).await;

        // The good heartbeat from "node-leader" was applied — receive_heartbeat
        // sets term and demotes us. Verify forged ack did NOT advance
        // match_index for "node-other".
        let mi = engine.match_index_for("node-other").await;
        assert!(
            mi.unwrap_or(0) == 0,
            "forged ReplicateAck must not advance match_index, got {mi:?}",
        );

        Arc::clone(&transport).shutdown().await;
    }

    #[tokio::test]
    async fn transport_accepts_well_formed_authenticated_message() {
        // Sanity: a fully well-formed handshake + heartbeat sent over
        // raw TCP (i.e. without going through TcpTransport::send) must
        // be accepted — proves the wire encoding is symmetric.
        let engine = make_engine("node-rcv", vec![]);

        let l = TcpListener::bind(loopback(0)).await.expect("scratch");
        let addr = l.local_addr().expect("addr");
        drop(l);

        let psk = Arc::new(derive_psk("good-cluster-psk").expect("derive"));
        let conf = cfg(addr, Vec::new(), "node-rcv", Arc::clone(&psk));
        let transport = TcpTransport::bind(conf, Arc::clone(&engine))
            .await
            .expect("bind");

        let handshake_bytes = serde_json::to_vec(&HandshakeFrame {
            announced_node_id: "node-leader".to_string(),
        })
        .expect("encode handshake");
        let handshake_frame =
            encode_authenticated_payload(&handshake_bytes, &psk).expect("frame handshake");

        let hb = ReplicationMessage::Heartbeat {
            leader_id: "node-leader".to_string(),
            term: 11,
        };
        let hb_frame = encode_message_authenticated(&hb, &psk).expect("encode hb");

        let mut buf = Vec::new();
        buf.extend_from_slice(&handshake_frame);
        buf.extend_from_slice(&hb_frame);
        let _ = raw_send(addr, &buf).await;
        tokio::time::sleep(Duration::from_millis(80)).await;

        let t = engine.term().await;
        assert_eq!(
            t, 11,
            "well-formed authenticated heartbeat must advance term to 11, got {t}",
        );

        Arc::clone(&transport).shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Phase 2.4: transport security posture tests.
//
// These cover the security-default change: the default posture is mTLS,
// PSK-cleartext is an explicit opt-in, mTLS rejects untrusted peers, and
// an mTLS round-trip handshake replicates cleanly. The `ClusterSecurityMode`
// + builder tests run unconditionally; the TLS handshake tests are gated on
// the (default-on) `cluster-tls` feature because they need rustls.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod security_mode_tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;
    use crate::auth::derive_psk;
    use crate::replication::{ClusterSecurityHint, ReplicationConfig, ReplicationEngine};
    use crate::{ClusterManager, NodeInfo, NodeRole};

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn test_psk() -> Arc<ClusterPsk> {
        Arc::new(derive_psk("phase-2-4-security-default").expect("derive"))
    }

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
            cluster_security_hint: ClusterSecurityHint::Psk,
        };
        Arc::new(ReplicationEngine::new(config, cm))
    }

    /// The explicit PSK-cleartext opt-in is NOT mTLS.
    #[test]
    fn psk_cleartext_mode_is_not_mutual_tls() {
        assert!(!ClusterSecurityMode::PskCleartext.is_mutual_tls());
    }

    /// `with_psk` selects the explicit PSK-cleartext opt-in posture.
    #[test]
    fn with_psk_builder_selects_psk_cleartext() {
        let cfg =
            TcpTransportConfig::with_psk(loopback(0), Vec::new(), "node-1".to_string(), test_psk());
        assert!(
            !cfg.security.is_mutual_tls(),
            "with_psk must select the PSK-cleartext opt-in, not mTLS",
        );
        assert!(matches!(cfg.security, ClusterSecurityMode::PskCleartext));
    }

    /// Explicit PSK-cleartext opt-in still binds and replicates a
    /// heartbeat end-to-end (the Wave 40-A path must keep working under
    /// the new explicit-opt-in posture).
    #[tokio::test]
    async fn explicit_psk_opt_in_replicates_heartbeat() {
        let engine_a = make_engine("node-a", vec!["node-b"]);
        let engine_b = make_engine("node-b", vec!["node-a"]);

        let l_a = TcpListener::bind(loopback(0)).await.expect("scratch a");
        let addr_a = l_a.local_addr().expect("addr_a");
        drop(l_a);
        let l_b = TcpListener::bind(loopback(0)).await.expect("scratch b");
        let addr_b = l_b.local_addr().expect("addr_b");
        drop(l_b);

        let psk = test_psk();
        let cfg_a = TcpTransportConfig::with_psk(
            addr_a,
            vec![("node-b".to_string(), addr_b)],
            "node-a".to_string(),
            Arc::clone(&psk),
        );
        let cfg_b = TcpTransportConfig::with_psk(
            addr_b,
            vec![("node-a".to_string(), addr_a)],
            "node-b".to_string(),
            Arc::clone(&psk),
        );
        assert!(!cfg_a.security.is_mutual_tls());

        let t_a = TcpTransport::bind(cfg_a, engine_a).await.expect("bind a");
        let t_b = TcpTransport::bind(cfg_b, Arc::clone(&engine_b))
            .await
            .expect("bind b");

        let msg = ReplicationMessage::Heartbeat {
            leader_id: "node-a".to_string(),
            term: 4,
        };
        t_a.send("node-b", &msg).await.expect("psk send a->b");
        tokio::time::sleep(Duration::from_millis(120)).await;

        assert_eq!(
            engine_b.term().await,
            4,
            "explicit PSK-cleartext path must deliver the heartbeat",
        );

        Arc::clone(&t_a).shutdown().await;
        Arc::clone(&t_b).shutdown().await;
    }

    // ---- mTLS-gated tests (cluster-tls is a default feature) ----

    #[cfg(feature = "cluster-tls")]
    mod tls {
        use std::fs;

        use super::*;
        use crate::tls::TlsConfig;
        use tempfile::TempDir;

        /// Build a self-signed CA returning its `KeyPair` + `Certificate`
        /// + serialised PEM so multiple node leaf certs can share it.
        fn build_ca() -> (rcgen::KeyPair, rcgen::Certificate, String) {
            let ca_key = rcgen::KeyPair::generate().expect("ca key");
            let ca_params = rcgen::CertificateParams::new(vec!["ferrodruid-p24-ca".to_string()])
                .expect("ca params");
            let ca_cert = ca_params.self_signed(&ca_key).expect("ca sign");
            let ca_pem = ca_cert.pem();
            (ca_key, ca_cert, ca_pem)
        }

        /// Generate a node leaf cert signed by `ca`, with SANs covering
        /// the node id + localhost + 127.0.0.1 so SNI verification matches
        /// whatever the dialer supplies. `ca_pem` is the bundle written as
        /// the verification root.
        fn gen_node_certs(
            node_name: &str,
            ca_key: &rcgen::KeyPair,
            ca_cert: &rcgen::Certificate,
            ca_pem: &str,
        ) -> (TempDir, TlsConfig) {
            let dir = tempfile::tempdir().expect("tempdir");
            let node_key = rcgen::KeyPair::generate().expect("node key");
            let node_params = rcgen::CertificateParams::new(vec![
                node_name.to_string(),
                "localhost".to_string(),
                "127.0.0.1".to_string(),
            ])
            .expect("node params");
            let node_cert = node_params
                .signed_by(&node_key, ca_cert, ca_key)
                .expect("node sign");

            let ca_path = dir.path().join("ca.pem");
            let cert_path = dir.path().join("node.pem");
            let key_path = dir.path().join("node-key.pem");
            fs::write(&ca_path, ca_pem).expect("write ca");
            fs::write(&cert_path, node_cert.pem()).expect("write cert");
            fs::write(&key_path, node_key.serialize_pem()).expect("write key");
            (dir, TlsConfig::new(cert_path, key_path, ca_path))
        }

        /// `with_mutual_tls` selects the (default) mTLS posture.
        #[test]
        fn with_mutual_tls_builder_selects_mtls() {
            let (ca_key, ca_cert, ca_pem) = build_ca();
            let (_dir, tls) = gen_node_certs("node-1", &ca_key, &ca_cert, &ca_pem);
            let cfg = TcpTransportConfig::with_mutual_tls(
                loopback(0),
                Vec::new(),
                "node-1".to_string(),
                test_psk(),
                tls,
            );
            assert!(
                cfg.security.is_mutual_tls(),
                "with_mutual_tls must select the mTLS posture",
            );
            assert!(matches!(cfg.security, ClusterSecurityMode::MutualTls(_)));
        }

        /// mTLS round trip: two nodes whose leaf certs are signed by the
        /// same CA complete the handshake and a heartbeat replicates.
        #[tokio::test]
        async fn mtls_round_trip_replicates_heartbeat() {
            let (ca_key, ca_cert, ca_pem) = build_ca();
            // DD R21: each node's leaf cert must assert its OWN announced node
            // id in its SAN (the server binds the handshake-announced id to the
            // peer cert). gen_node_certs also adds localhost/127.0.0.1 so the
            // peer dialed under SNI "localhost" still verifies.
            let (_dir_a, tls_a) = gen_node_certs("node-a", &ca_key, &ca_cert, &ca_pem);
            let (_dir_b, tls_b) = gen_node_certs("node-b", &ca_key, &ca_cert, &ca_pem);

            let engine_a = make_engine("node-a", vec!["localhost"]);
            let engine_b = make_engine("node-b", vec!["localhost"]);

            let l_a = TcpListener::bind(loopback(0)).await.expect("scratch a");
            let addr_a = l_a.local_addr().expect("addr_a");
            drop(l_a);
            let l_b = TcpListener::bind(loopback(0)).await.expect("scratch b");
            let addr_b = l_b.local_addr().expect("addr_b");
            drop(l_b);

            let psk = test_psk();
            // a dials b under SNI "localhost"; b's cert lists localhost.
            let mut cfg_a = TcpTransportConfig::with_mutual_tls(
                addr_a,
                vec![("localhost".to_string(), addr_b)],
                "node-a".to_string(),
                Arc::clone(&psk),
                tls_a,
            );
            cfg_a.connect_timeout = Duration::from_millis(800);
            let cfg_b = TcpTransportConfig::with_mutual_tls(
                addr_b,
                Vec::new(),
                "node-b".to_string(),
                Arc::clone(&psk),
                tls_b,
            );

            let t_a = TcpTransport::bind(cfg_a, engine_a).await.expect("bind a");
            let t_b = TcpTransport::bind(cfg_b, Arc::clone(&engine_b))
                .await
                .expect("bind b");

            let msg = ReplicationMessage::Heartbeat {
                leader_id: "node-a".to_string(),
                term: 6,
            };
            // The first send establishes the TLS connection + handshake.
            let _ = t_a.send("localhost", &msg).await;
            tokio::time::sleep(Duration::from_millis(250)).await;

            assert_eq!(
                engine_b.term().await,
                6,
                "mTLS round trip must deliver the heartbeat through the tunnel",
            );

            Arc::clone(&t_a).shutdown().await;
            Arc::clone(&t_b).shutdown().await;
        }

        /// DD R21 (High): a node holding a VALID CA-signed cert + the shared PSK
        /// must not be able to announce a DIFFERENT node id than its certificate
        /// asserts. The server binds the handshake-announced id to the peer cert
        /// SAN/CN and drops the connection on mismatch, so the impersonated
        /// node's frames never reach the engine.
        #[tokio::test]
        async fn mtls_rejects_node_id_impersonation() {
            let (ca_key, ca_cert, ca_pem) = build_ca();
            // Server cert covers localhost (it is dialed under SNI "localhost").
            let (_dir_srv, srv_tls) = gen_node_certs("node-srv", &ca_key, &ca_cert, &ca_pem);
            // Attacker has a fully valid cert from the SAME CA, but its SAN
            // asserts "node-attacker" — NOT the "node-victim" id it will announce.
            let (_dir_att, att_tls) = gen_node_certs("node-attacker", &ca_key, &ca_cert, &ca_pem);

            let engine_srv = make_engine("node-srv", vec![]);
            let engine_att = make_engine("node-attacker", vec!["localhost"]);

            let l = TcpListener::bind(loopback(0)).await.expect("scratch srv");
            let addr_srv = l.local_addr().expect("addr_srv");
            drop(l);
            let l2 = TcpListener::bind(loopback(0)).await.expect("scratch att");
            let addr_att = l2.local_addr().expect("addr_att");
            drop(l2);

            let psk = test_psk();
            // The attacker's announced (local) node id is the impersonated
            // "node-victim", absent from its cert SAN ["node-attacker",localhost,..].
            let mut cfg_att = TcpTransportConfig::with_mutual_tls(
                addr_att,
                vec![("localhost".to_string(), addr_srv)],
                "node-victim".to_string(),
                Arc::clone(&psk),
                att_tls,
            );
            cfg_att.connect_timeout = Duration::from_millis(800);
            let cfg_srv = TcpTransportConfig::with_mutual_tls(
                addr_srv,
                Vec::new(),
                "node-srv".to_string(),
                Arc::clone(&psk),
                srv_tls,
            );

            let t_att = TcpTransport::bind(cfg_att, engine_att)
                .await
                .expect("bind att");
            let t_srv = TcpTransport::bind(cfg_srv, Arc::clone(&engine_srv))
                .await
                .expect("bind srv");

            // A heartbeat announcing the impersonated id. Without the R21 binding
            // this would advance the server's term to 9; with it the handshake is
            // dropped and the term stays 0.
            let msg = ReplicationMessage::Heartbeat {
                leader_id: "node-victim".to_string(),
                term: 9,
            };
            let _ = t_att.send("localhost", &msg).await;
            tokio::time::sleep(Duration::from_millis(250)).await;

            assert_eq!(
                engine_srv.term().await,
                0,
                "server must reject a handshake announcing an id not in the peer cert",
            );

            Arc::clone(&t_att).shutdown().await;
            Arc::clone(&t_srv).shutdown().await;
        }

        /// mTLS rejects an untrusted peer: a client whose leaf is signed by
        /// a DIFFERENT CA cannot complete the handshake, so no business
        /// frame ever reaches the server engine.
        #[tokio::test]
        async fn mtls_rejects_untrusted_peer() {
            // Server trusts CA #1; client presents a cert from CA #2.
            let (ca1_key, ca1_cert, ca1_pem) = build_ca();
            let (ca2_key, ca2_cert, ca2_pem) = build_ca();

            let (_dir_srv, srv_tls) = gen_node_certs("localhost", &ca1_key, &ca1_cert, &ca1_pem);
            // Client leaf signed by CA #2, but configured to trust CA #1
            // as its server root (so the failure is the SERVER rejecting
            // the client cert at WebPkiClientVerifier, i.e. untrusted
            // peer — exactly the property under test).
            let (_dir_cli, mut cli_tls) =
                gen_node_certs("localhost", &ca2_key, &ca2_cert, &ca2_pem);
            // Point the client's verification root at CA #1 so its own
            // outbound validation of the server succeeds; only the server
            // side will reject the CA#2-signed client cert.
            cli_tls.ca_path = srv_tls.ca_path.clone();

            let engine_srv = make_engine("node-srv", vec![]);
            let engine_cli = make_engine("node-cli", vec!["localhost"]);

            let l = TcpListener::bind(loopback(0)).await.expect("scratch");
            let addr_srv = l.local_addr().expect("addr_srv");
            drop(l);
            let l2 = TcpListener::bind(loopback(0)).await.expect("scratch2");
            let addr_cli = l2.local_addr().expect("addr_cli");
            drop(l2);

            let psk = test_psk();
            let cfg_srv = TcpTransportConfig::with_mutual_tls(
                addr_srv,
                Vec::new(),
                "node-srv".to_string(),
                Arc::clone(&psk),
                srv_tls,
            );
            let mut cfg_cli = TcpTransportConfig::with_mutual_tls(
                addr_cli,
                vec![("localhost".to_string(), addr_srv)],
                "node-cli".to_string(),
                Arc::clone(&psk),
                cli_tls,
            );
            cfg_cli.connect_timeout = Duration::from_millis(500);

            let t_srv = TcpTransport::bind(cfg_srv, Arc::clone(&engine_srv))
                .await
                .expect("bind srv");
            let t_cli = TcpTransport::bind(cfg_cli, engine_cli)
                .await
                .expect("bind cli");

            let msg = ReplicationMessage::Heartbeat {
                leader_id: "node-cli".to_string(),
                term: 9,
            };
            // The send will fail because the server rejects the client
            // cert at the TLS handshake (untrusted CA). Either way the
            // engine must NOT observe the heartbeat.
            let _ = t_cli.send("localhost", &msg).await;
            tokio::time::sleep(Duration::from_millis(250)).await;

            assert_eq!(
                engine_srv.term().await,
                0,
                "untrusted peer must be rejected at the mTLS handshake; \
                 no heartbeat may reach the server engine",
            );

            Arc::clone(&t_srv).shutdown().await;
            Arc::clone(&t_cli).shutdown().await;
        }
    }
}
