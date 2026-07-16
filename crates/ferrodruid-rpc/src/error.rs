// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Error type shared by every cross-role RPC client implementation.

use thiserror::Error;

/// Failure surface returned by [`crate::BrokerClient`] and
/// [`crate::MiddleManagerClient`] implementations.
///
/// Every variant maps to a clear operator-visible failure mode. The
/// real HTTP impls (`HttpBrokerClient` / `HttpMiddleManagerClient`)
/// translate `reqwest::Error` into `Transport` and non-2xx HTTP
/// statuses into `Http`. The mock impls only ever return `NotFound`
/// or `Custom` (the latter for negative-path tests).
#[derive(Debug, Error)]
pub enum RpcError {
    /// Network-layer failure (connection refused, DNS, TLS handshake,
    /// timeout, body read interrupted). Surfaces the underlying
    /// `reqwest` error message verbatim so an operator running
    /// `RUST_LOG=debug` can see the exact failure.
    #[error("transport error: {0}")]
    Transport(String),
    /// The remote role responded with a non-2xx HTTP status. Carries
    /// the status code and the response body (best-effort; truncated
    /// to 4 KiB so a misbehaving peer cannot OOM the caller).
    #[error("HTTP {status}: {body}")]
    Http {
        /// HTTP status code returned by the peer (e.g. 503).
        status: u16,
        /// Response body, truncated to 4 KiB.
        body: String,
    },
    /// Failed to (de)serialize the wire payload. Should be rare in
    /// production — appears mostly when caller and callee disagree
    /// on the type contract during a rolling upgrade.
    #[error("serde error: {0}")]
    Serde(String),
    /// The peer accepted the request but reports the resource is
    /// missing. Distinct from a 404 [`RpcError::Http`] because the
    /// mock impls return this without an HTTP layer in the loop.
    #[error("not found: {0}")]
    NotFound(String),
    /// Test-only escape hatch used by mock impls to inject arbitrary
    /// failures. Production code never produces this variant.
    #[error("rpc error: {0}")]
    Custom(String),
}

impl From<reqwest::Error> for RpcError {
    fn from(value: reqwest::Error) -> Self {
        RpcError::Transport(value.to_string())
    }
}

impl From<serde_json::Error> for RpcError {
    fn from(value: serde_json::Error) -> Self {
        RpcError::Serde(value.to_string())
    }
}
