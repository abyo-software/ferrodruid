// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wire protocol for MSQ distributed execution (CL-5).
//!
//! A minimal length-prefixed JSON framing layer used between an
//! [`MsqCoordinator`](crate::coordinator::MsqCoordinator) and one or more
//! [`MsqWorker`](crate::worker::MsqWorker) processes.  The framing shape
//! mirrors the cluster-transport pattern in `ferrodruid-cluster::transport`
//! (length prefix + JSON payload) but intentionally skips HMAC / mTLS at
//! this layer; transport-level authentication is layered above for a
//! multi-host deployment, which is gated on AWS approval per the
//! W1-E closure scope (loopback first).
//!
//! Frame: `[u32 BE payload_len][payload_bytes]` with `payload_bytes` a
//! `serde_json` encoding of a [`WireMessage`].  Payloads above
//! [`MAX_FRAME_BODY`] are rejected on receive.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use ferrodruid_common::{DruidError, Result};

use crate::engine::{AggFn, Row, RowSignature, ShuffleSpec, WorkerCounters};

/// Maximum payload size accepted on a single MSQ wire frame (256 MiB).
///
/// Generous to allow large row batches between workers; raise only with
/// a matching memory-budget review.
pub const MAX_FRAME_BODY: usize = 256 * 1024 * 1024;

/// One-shot RPC message between the coordinator and a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WireMessage {
    /// Coordinator → worker: execute one stage slice on this worker.
    ///
    /// The worker takes `input_rows`, runs the requested processor over them,
    /// hash-partitions the output per `output_shuffle`, and returns
    /// [`WireMessage::StageOutput`].
    ExecuteSlice(ExecuteSlice),
    /// Worker → coordinator: stage slice produced these partitioned rows.
    StageOutput(StageOutput),
    /// Coordinator → worker: graceful shutdown.  The worker drains the
    /// current request (if any) and closes its listener.
    Shutdown,
    /// Worker → coordinator: shutdown acknowledged.
    ShutdownAck,
    /// Wire-level error envelope.
    Error {
        /// Human-readable error message.
        message: String,
    },
}

/// Payload of [`WireMessage::ExecuteSlice`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteSlice {
    /// MSQ task identifier this slice belongs to.
    pub task_id: String,
    /// Stage number being executed.
    pub stage_no: usize,
    /// Worker index within the stage (0..total_workers).
    pub worker_id: usize,
    /// Total number of workers cooperating on this stage.
    pub total_workers: usize,
    /// Idempotency token; an identical slice with the same token is a
    /// no-op replay.  See [`crate::coordinator::idempotency_token`].
    pub idempotency_token: String,
    /// Processor kind to apply to `input_rows`.
    pub processor: WireProcessor,
    /// Signature of `input_rows`.
    pub input_signature: RowSignature,
    /// Input rows fed to the processor.
    pub input_rows: Vec<Row>,
    /// How to partition the output rows (`None` ⇒ all rows go to partition 0).
    pub output_shuffle: ShuffleSpec,
}

/// Wire representation of a stage processor.
///
/// Subset of [`crate::engine::Processor`] carried on the wire so the
/// worker can re-build a [`StageDefinition`](crate::engine::StageDefinition)
/// locally without the rest of the [`QueryDefinition`](crate::engine::QueryDefinition).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WireProcessor {
    /// Pass-through projection (Scan / Shuffle equivalent).
    Passthrough,
    /// Aggregate by `group_by` with `aggs`.
    Aggregate {
        /// Group-by column names.
        group_by: Vec<String>,
        /// Aggregation functions.
        aggs: Vec<AggFn>,
        /// Whether this is a partial (true) or final-merge (false) aggregation.
        partial: bool,
    },
}

/// One output partition produced by an [`ExecuteSlice`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionEntry {
    /// Output partition index.
    pub partition: usize,
    /// Rows in this partition (in producer-side order).
    pub rows: Vec<Row>,
}

/// Payload of [`WireMessage::StageOutput`].
///
/// Partitions are carried as a `Vec` of `(partition_index, rows)` rather
/// than a `HashMap<usize, _>` because `serde_json` only allows string
/// map keys.  The vec is sorted by `partition` on the sender side so
/// the wire shape is deterministic.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StageOutput {
    /// MSQ task id (echoed from the request).
    pub task_id: String,
    /// Stage number (echoed from the request).
    pub stage_no: usize,
    /// Worker id that produced this output.
    pub worker_id: usize,
    /// Output partitions (deterministic order).
    pub partitions: Vec<PartitionEntry>,
    /// Output signature.
    pub signature: RowSignature,
    /// Per-worker execution counters.
    pub counters: WorkerCounters,
}

impl StageOutput {
    /// Look up rows by partition index (linear scan; partition lists
    /// are small relative to row volume).
    #[must_use]
    pub fn partition(&self, idx: usize) -> Option<&Vec<Row>> {
        self.partitions
            .iter()
            .find(|p| p.partition == idx)
            .map(|p| &p.rows)
    }
}

/// Read one [`WireMessage`] from `reader`.
///
/// # Errors
///
/// Returns [`DruidError::Internal`] on socket read failure, oversize
/// frame (> [`MAX_FRAME_BODY`]), or JSON decode failure.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<WireMessage> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| DruidError::Internal(format!("MSQ wire read length: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BODY {
        return Err(DruidError::Internal(format!(
            "MSQ wire frame body {len} exceeds MAX_FRAME_BODY {MAX_FRAME_BODY}"
        )));
    }
    let mut body = vec![0u8; len];
    reader
        .read_exact(&mut body)
        .await
        .map_err(|e| DruidError::Internal(format!("MSQ wire read body: {e}")))?;
    serde_json::from_slice(&body)
        .map_err(|e| DruidError::Internal(format!("MSQ wire JSON decode: {e}")))
}

/// Write one [`WireMessage`] to `writer`.
///
/// # Errors
///
/// Returns [`DruidError::Internal`] on serialisation failure, oversize
/// payload (> [`MAX_FRAME_BODY`]), or socket write failure.
pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, msg: &WireMessage) -> Result<()> {
    let body = serde_json::to_vec(msg)
        .map_err(|e| DruidError::Internal(format!("MSQ wire JSON encode: {e}")))?;
    if body.len() > MAX_FRAME_BODY {
        return Err(DruidError::Internal(format!(
            "MSQ wire frame body {} exceeds MAX_FRAME_BODY {MAX_FRAME_BODY}",
            body.len()
        )));
    }
    let len_u32: u32 = body.len().try_into().map_err(|_| {
        DruidError::Internal(format!("MSQ wire frame body {} too large", body.len()))
    })?;
    writer
        .write_all(&len_u32.to_be_bytes())
        .await
        .map_err(|e| DruidError::Internal(format!("MSQ wire write length: {e}")))?;
    writer
        .write_all(&body)
        .await
        .map_err(|e| DruidError::Internal(format!("MSQ wire write body: {e}")))?;
    writer
        .flush()
        .await
        .map_err(|e| DruidError::Internal(format!("MSQ wire flush: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Value;
    use tokio::io::duplex;

    #[tokio::test]
    async fn frame_round_trip() {
        let (mut a, mut b) = duplex(64 * 1024);
        let msg = WireMessage::Shutdown;
        let writer = tokio::spawn(async move {
            write_frame(&mut a, &msg).await.expect("write");
        });
        let got = read_frame(&mut b).await.expect("read");
        writer.await.expect("join");
        assert!(matches!(got, WireMessage::Shutdown));
    }

    #[tokio::test]
    async fn frame_round_trip_payload() {
        let (mut a, mut b) = duplex(64 * 1024);
        let parts = vec![PartitionEntry {
            partition: 0,
            rows: vec![vec![Value::Long(1)], vec![Value::Long(2)]],
        }];
        let msg = WireMessage::StageOutput(StageOutput {
            task_id: "t".into(),
            stage_no: 0,
            worker_id: 0,
            partitions: parts,
            signature: RowSignature::new(&[("v", "BIGINT")]),
            counters: WorkerCounters {
                worker: 0,
                rows_in: 2,
                rows_out: 2,
                bytes_spilled: 0,
            },
        });
        let msg2 = msg.clone();
        let writer = tokio::spawn(async move {
            write_frame(&mut a, &msg2).await.expect("write");
        });
        let got = read_frame(&mut b).await.expect("read");
        writer.await.expect("join");
        match got {
            WireMessage::StageOutput(out) => {
                assert_eq!(out.partition(0).map(Vec::len), Some(2));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn oversize_frame_rejected_on_send() {
        // Build a giant body via direct serialization first to confirm the
        // sender enforces the cap.
        let huge = vec![0u8; MAX_FRAME_BODY + 1];
        let msg = WireMessage::Error {
            message: String::from_utf8(huge).unwrap_or_else(|_| "x".repeat(MAX_FRAME_BODY + 1)),
        };
        let (mut a, _b) = duplex(64);
        let result = write_frame(&mut a, &msg).await;
        assert!(result.is_err(), "oversize frame must be rejected");
    }
}
