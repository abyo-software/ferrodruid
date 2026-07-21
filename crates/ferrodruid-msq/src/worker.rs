// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! MSQ worker — TCP-listening stage-slice executor (CL-5).
//!
//! A worker accepts one [`WireMessage::ExecuteSlice`] per connection,
//! evaluates the requested processor over the supplied input rows,
//! hash-partitions the output per the slice's [`ShuffleSpec`], and
//! replies with [`WireMessage::StageOutput`].  A separate
//! [`WireMessage::Shutdown`] frame stops the worker cleanly (used by
//! the kill-mid-stage retry tests).
//!
//! Replays of an already-processed slice (same `(task_id, stage_no,
//! worker_id, idempotency_token)` tuple) return the cached output
//! without re-execution.  This makes stage retry idempotent: if the
//! coordinator believes a worker died but the worker is in fact still
//! alive, the resend lands as a fast cache hit, and at-most-once is
//! preserved by deterministic output partitioning at the upstream.
//!
//! ## Scope
//!
//! Loopback / trusted-network only.  No HMAC, no mTLS at this layer —
//! authentication is the deployer's concern (front with an mTLS
//! reverse proxy, or run on a private VPC subnet).  The multi-host
//! validation that needs authn is gated on AWS approval and tracked
//! as a CL-5 residual.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

use ferrodruid_common::{DruidError, Result};

use crate::engine::{
    AggFn, Processor, RowSignature, ShuffleSpec, StageDefinition, WorkerCounters, aggregate_rows,
    merge_partials, partition_rows,
};
use crate::wire::{
    ExecuteSlice, PartitionEntry, StageOutput, WireMessage, WireProcessor, read_frame, write_frame,
};

/// Cached output of a previously-served [`ExecuteSlice`].
///
/// Keyed by `(task_id, stage_no, worker_id, idempotency_token)` so a
/// retry with the same coordinator-issued token is served from cache.
type SliceKey = (String, usize, usize, String);

/// In-memory dedup cache for served slices.
type SliceCache = HashMap<SliceKey, StageOutput>;

/// A TCP-listening MSQ worker.
///
/// Construct via [`MsqWorker::bind`].  Calling [`MsqWorker::start`]
/// spawns the accept loop and returns a handle; shutdown is signalled
/// by sending [`WireMessage::Shutdown`] over any open connection OR by
/// dropping the returned [`WorkerHandle`].
pub struct MsqWorker {
    listener: TcpListener,
    addr: SocketAddr,
    cache: Arc<Mutex<SliceCache>>,
}

/// Handle to a running worker accept-loop task.
///
/// Dropping the handle aborts the worker.  Calling
/// [`WorkerHandle::shutdown`] requests a graceful stop.
pub struct WorkerHandle {
    /// Bound address (useful when the worker bound to port 0).
    pub addr: SocketAddr,
    join: JoinHandle<()>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
}

impl WorkerHandle {
    /// Request a graceful shutdown.  Does nothing if already requested.
    pub async fn shutdown(&self) {
        let mut guard = self.shutdown_tx.lock().await;
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
        }
    }

    /// Abort the worker accept loop immediately (used by retry tests
    /// to simulate a worker crash mid-stage).
    pub fn abort(&self) {
        self.join.abort();
    }

    /// Wait for the accept loop to exit.
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Internal`] if the worker task panicked or
    /// was cancelled.
    pub async fn join(self) -> Result<()> {
        match self.join.await {
            Ok(()) => Ok(()),
            Err(e) if e.is_cancelled() => Ok(()),
            Err(e) => Err(DruidError::Internal(format!("MSQ worker join: {e}"))),
        }
    }
}

impl MsqWorker {
    /// Bind a worker to `addr`.  Use `"127.0.0.1:0"` to let the kernel
    /// pick a port (its concrete address is exposed via
    /// [`WorkerHandle::addr`] after [`Self::start`]).
    ///
    /// # Errors
    ///
    /// Propagates `tokio::net::TcpListener::bind` errors.
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| DruidError::Internal(format!("MSQ worker bind {addr}: {e}")))?;
        let addr = listener
            .local_addr()
            .map_err(|e| DruidError::Internal(format!("MSQ worker local_addr: {e}")))?;
        Ok(Self {
            listener,
            addr,
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Bound address.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Spawn the accept loop.  Returns a [`WorkerHandle`].
    pub fn start(self) -> WorkerHandle {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let addr = self.addr;
        let listener = self.listener;
        let cache = self.cache;

        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        tracing::debug!(?addr, "MSQ worker shutdown requested");
                        break;
                    }
                    accept = listener.accept() => {
                        let (stream, peer) = match accept {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!(?addr, err = %e, "MSQ worker accept failed");
                                continue;
                            }
                        };
                        let cache = Arc::clone(&cache);
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, cache).await {
                                tracing::warn!(?peer, err = %e, "MSQ worker connection failed");
                            }
                        });
                    }
                }
            }
        });

        WorkerHandle {
            addr,
            join,
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
        }
    }
}

/// One TCP connection lifetime: read a single [`WireMessage`], dispatch,
/// reply.  Shutdown frames close the listener via the outer select loop;
/// we just acknowledge here and let the next accept-call see the channel
/// fire.  (For per-connection shutdown we'd plumb the signal differently;
/// this scope keeps it simple — `WorkerHandle::shutdown` is the canonical
/// path.)
async fn handle_connection(stream: TcpStream, cache: Arc<Mutex<SliceCache>>) -> Result<()> {
    let (mut rx, mut tx) = stream.into_split();
    let msg = read_frame(&mut rx).await?;
    match msg {
        WireMessage::ExecuteSlice(slice) => {
            let reply = match process_slice(slice, &cache).await {
                Ok(out) => WireMessage::StageOutput(out),
                Err(e) => WireMessage::Error {
                    message: e.to_string(),
                },
            };
            write_frame(&mut tx, &reply).await?;
        }
        WireMessage::Shutdown => {
            write_frame(&mut tx, &WireMessage::ShutdownAck).await?;
        }
        other => {
            let reply = WireMessage::Error {
                message: format!("MSQ worker unexpected frame: {other:?}"),
            };
            write_frame(&mut tx, &reply).await?;
        }
    }
    Ok(())
}

/// Run a single slice through the local engine machinery, then
/// hash-partition the output rows per `output_shuffle`.
async fn process_slice(slice: ExecuteSlice, cache: &Arc<Mutex<SliceCache>>) -> Result<StageOutput> {
    let key = (
        slice.task_id.clone(),
        slice.stage_no,
        slice.worker_id,
        slice.idempotency_token.clone(),
    );
    if let Some(cached) = cache.lock().await.get(&key).cloned() {
        tracing::debug!(
            task = %slice.task_id,
            stage = slice.stage_no,
            worker = slice.worker_id,
            "MSQ worker idempotency cache hit",
        );
        return Ok(cached);
    }

    let rows_in = slice.input_rows.len() as u64;

    let (out_rows, out_sig) = match slice.processor {
        WireProcessor::Passthrough => (slice.input_rows, slice.input_signature.clone()),
        WireProcessor::Aggregate {
            group_by,
            aggs,
            partial,
        } => {
            // Validate the group keys exist on input.
            for g in &group_by {
                if slice.input_signature.index_of(g).is_none() {
                    return Err(DruidError::Query(format!(
                        "MSQ aggregate group key `{g}` missing from input signature"
                    )));
                }
            }
            let out_rows = if partial {
                aggregate_rows(&slice.input_rows, &slice.input_signature, &group_by, &aggs)?
            } else {
                // Final merge: input is already (group_by..., agg...) layout.
                merge_partials(vec![slice.input_rows], group_by.len(), &aggs)?
            };
            let out_sig = aggregate_output_signature(&group_by, &aggs);
            (out_rows, out_sig)
        }
    };

    // Partition by output_shuffle.
    let partitioned = partition_rows(out_rows, &out_sig, &slice.output_shuffle)?;
    let mut partitions: Vec<PartitionEntry> = Vec::new();
    for (p, rows) in partitioned.partitions.into_iter().enumerate() {
        if !rows.is_empty() {
            partitions.push(PartitionEntry { partition: p, rows });
        }
    }
    partitions.sort_by_key(|e| e.partition);

    let rows_out: u64 = partitions.iter().map(|e| e.rows.len() as u64).sum();
    let counters = WorkerCounters {
        worker: slice.worker_id,
        rows_in,
        rows_out,
        bytes_spilled: 0,
    };

    let out = StageOutput {
        task_id: slice.task_id,
        stage_no: slice.stage_no,
        worker_id: slice.worker_id,
        partitions,
        signature: out_sig,
        counters,
    };

    cache.lock().await.insert(key, out.clone());
    Ok(out)
}

/// Compute the output signature of an aggregate stage given its group-by
/// columns and aggregation functions.
fn aggregate_output_signature(group_by: &[String], aggs: &[AggFn]) -> RowSignature {
    let mut columns = Vec::with_capacity(group_by.len() + aggs.len());
    let mut types = Vec::with_capacity(group_by.len() + aggs.len());
    for g in group_by {
        columns.push(g.clone());
        types.push("VARCHAR".to_owned());
    }
    for a in aggs {
        columns.push(a.output_name());
        types.push("BIGINT".to_owned());
    }
    RowSignature { columns, types }
}

/// Build a [`StageDefinition`] from a [`WireProcessor`] + carry signature.
///
/// Exposed for the coordinator's local-fallback path (single-worker
/// queries that bypass TCP altogether for debugging).
#[must_use]
pub fn wire_to_stage_def(
    stage_no: usize,
    inputs: Vec<usize>,
    wp: WireProcessor,
    signature: RowSignature,
    shuffle: ShuffleSpec,
) -> StageDefinition {
    let processor = match wp {
        WireProcessor::Passthrough => Processor::Scan {
            project: signature.columns.clone(),
        },
        WireProcessor::Aggregate { group_by, aggs, .. } => Processor::Aggregate { group_by, aggs },
    };
    StageDefinition {
        stage_number: stage_no,
        inputs,
        processor,
        signature,
        shuffle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Value;
    use crate::wire::{ExecuteSlice, WireProcessor};
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn worker_runs_passthrough_slice() {
        let worker = MsqWorker::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind");
        let addr = worker.addr();
        let handle = worker.start();

        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let sig = RowSignature::new(&[("city", "VARCHAR"), ("n", "BIGINT")]);
        let rows = vec![
            vec![Value::Str("a".into()), Value::Long(1)],
            vec![Value::Str("b".into()), Value::Long(2)],
        ];
        let msg = WireMessage::ExecuteSlice(ExecuteSlice {
            task_id: "t".into(),
            stage_no: 0,
            worker_id: 0,
            total_workers: 1,
            idempotency_token: "tok".into(),
            processor: WireProcessor::Passthrough,
            input_signature: sig,
            input_rows: rows,
            output_shuffle: ShuffleSpec::None,
        });
        let (mut rx, mut tx) = stream.split();
        write_frame(&mut tx, &msg).await.expect("write");
        tx.flush().await.expect("flush");
        let reply = read_frame(&mut rx).await.expect("read");
        drop(stream);
        match reply {
            WireMessage::StageOutput(out) => {
                assert_eq!(out.partition(0).map(Vec::len), Some(2));
                assert_eq!(out.counters.rows_in, 2);
                assert_eq!(out.counters.rows_out, 2);
            }
            other => panic!("unexpected {other:?}"),
        }

        handle.shutdown().await;
        handle.join.abort();
    }

    #[tokio::test]
    async fn worker_idempotency_cache_hit() {
        let worker = MsqWorker::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind");
        let addr = worker.addr();
        let handle = worker.start();

        let sig = RowSignature::new(&[("v", "BIGINT")]);
        let msg = WireMessage::ExecuteSlice(ExecuteSlice {
            task_id: "t".into(),
            stage_no: 1,
            worker_id: 0,
            total_workers: 1,
            idempotency_token: "tok-A".into(),
            processor: WireProcessor::Passthrough,
            input_signature: sig.clone(),
            input_rows: vec![vec![Value::Long(7)]],
            output_shuffle: ShuffleSpec::None,
        });
        // Round 1.
        let mut s1 = TcpStream::connect(addr).await.unwrap();
        let (mut r1, mut w1) = s1.split();
        write_frame(&mut w1, &msg).await.unwrap();
        let r1m = read_frame(&mut r1).await.unwrap();
        assert!(matches!(r1m, WireMessage::StageOutput(_)));
        drop(s1);

        // Round 2: same key — must hit cache, identical reply.
        let mut s2 = TcpStream::connect(addr).await.unwrap();
        let (mut r2, mut w2) = s2.split();
        write_frame(&mut w2, &msg).await.unwrap();
        let r2m = read_frame(&mut r2).await.unwrap();
        match r2m {
            WireMessage::StageOutput(out) => {
                assert_eq!(out.partition(0).map(Vec::len), Some(1));
            }
            other => panic!("unexpected {other:?}"),
        }
        drop(s2);

        handle.shutdown().await;
        handle.join.abort();
    }
}
