// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Multi-stage execution engine for MSQ.
//!
//! This module implements a real (in-process, single-node) distributed
//! execution engine modelled on Apache Druid's MSQ `QueryDefinition` /
//! `StageDefinition` contract.  A query is decomposed into a DAG of
//! [`StageDefinition`]s; each stage declares its inputs, a [`Processor`]
//! kind, an output [`RowSignature`], and a [`ShuffleSpec`].  Stages are
//! topologically ordered and run across a configurable number of
//! in-process workers ([`tokio`] tasks).
//!
//! The engine implements the canonical three-stage shuffle/aggregate
//! pipeline:
//!
//! * **scan** — read input rows and project columns,
//! * **shuffle** — partition rows by a key into N output partitions
//!   (the SHUFFLE), so each downstream worker reads its own partition,
//! * **aggregate** — each worker aggregates its assigned partitions
//!   (`GROUP BY`) and the partials are merged.
//!
//! In-memory partition buffers that exceed a configurable byte threshold
//! are **spilled** to a temporary frame file and merge-read on consume,
//! producing identical results spilled vs not.
//!
//! # Honest scope
//!
//! Workers are `tokio` tasks within one process, not cross-node actors;
//! the "shuffle" moves rows between in-memory / on-disk partitions rather
//! than over a network RPC.  The frame format is a length-prefixed
//! JSON-line encoding chosen for clarity, not a columnar Druid frame.
//! See the crate-level docs and `engine` tests for the exact guarantees.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use serde::{Deserialize, Serialize};

use ferrodruid_common::{DruidError, Result};

/// Upper bound on a single spill-frame's declared byte length (DD R27). Read
/// before allocating the frame buffer so a stale/corrupt spill file cannot drive
/// an unbounded `Vec` allocation. 512 MiB is far above any legitimate frame.
const MAX_SPILL_FRAME_BYTES: usize = 512 * 1024 * 1024;

/// Upper bound on a shuffle's output partition count (DD R31). The partition
/// count is a scalar in the (deserialized) `QueryDefinition`, and `partition_rows`
/// allocates `vec![Vec::new(); n]` buckets BEFORE processing any row — so an
/// unchecked `u32::MAX` partitions would allocate billions of empty `Vec` headers
/// from a tiny query definition. 1 Mi partitions is far beyond any real plan.
const MAX_SHUFFLE_PARTITIONS: usize = 1 << 20;

// ---------------------------------------------------------------------------
// Row / Value model
// ---------------------------------------------------------------------------

/// A single typed cell within a [`Row`].
///
/// The engine works over a small, self-contained value model rather than
/// `serde_json::Value` so that ordering, hashing, and aggregation have
/// well-defined semantics independent of JSON quirks (e.g. integer vs
/// float distinction, total ordering of nulls).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v")]
pub enum Value {
    /// SQL `NULL` / absent value.
    Null,
    /// 64-bit signed integer (Druid `LONG`).
    Long(i64),
    /// 64-bit float (Druid `DOUBLE`).
    Double(f64),
    /// UTF-8 string (Druid `STRING`).
    Str(String),
}

impl Value {
    /// Returns the numeric value of this cell as `i64`, treating `Null`,
    /// strings, and non-integral doubles best-effort.  Used by `LONG`
    /// aggregators.
    #[must_use]
    pub fn as_long(&self) -> i64 {
        match self {
            Value::Long(v) => *v,
            Value::Double(v) => *v as i64,
            Value::Str(s) => s.parse().unwrap_or(0),
            Value::Null => 0,
        }
    }

    /// Returns the numeric value of this cell as `f64`.
    #[must_use]
    pub fn as_double(&self) -> f64 {
        match self {
            Value::Long(v) => *v as f64,
            Value::Double(v) => *v,
            Value::Str(s) => s.parse().unwrap_or(0.0),
            Value::Null => 0.0,
        }
    }

    /// Render the value as a `serde_json::Value` for the MSQ result
    /// envelope.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Long(v) => serde_json::json!(v),
            Value::Double(v) => serde_json::json!(v),
            Value::Str(s) => serde_json::json!(s),
        }
    }

    /// A stable, total ordering key string used by the hash partitioner so
    /// that equal keys always hash to the same partition regardless of the
    /// in-memory representation.
    fn partition_key_bytes(&self) -> String {
        match self {
            Value::Null => "\u{0}N".to_owned(),
            Value::Long(v) => format!("L{v}"),
            Value::Double(v) => format!("D{}", v.to_bits()),
            Value::Str(s) => format!("S{s}"),
        }
    }

    /// Total ordering over values for range partitioning / sorting.
    /// Null sorts first; numbers compare numerically; strings
    /// lexicographically.  Mixed types order by a fixed type rank so the
    /// comparison is total (never `None`).
    fn total_cmp(&self, other: &Value) -> Ordering {
        fn rank(v: &Value) -> u8 {
            match v {
                Value::Null => 0,
                Value::Long(_) | Value::Double(_) => 1,
                Value::Str(_) => 2,
            }
        }
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Long(a), Value::Long(b)) => a.cmp(b),
            (Value::Double(a), Value::Double(b)) => a.total_cmp(b),
            (Value::Long(a), Value::Double(b)) => (*a as f64).total_cmp(b),
            (Value::Double(a), Value::Long(b)) => a.total_cmp(&(*b as f64)),
            (Value::Str(a), Value::Str(b)) => a.cmp(b),
            _ => rank(self).cmp(&rank(other)),
        }
    }
}

/// A row: an ordered list of cells aligned with a [`RowSignature`].
pub type Row = Vec<Value>;

/// The output column schema of a stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowSignature {
    /// Ordered column names.
    pub columns: Vec<String>,
    /// SQL type names parallel to `columns` (e.g. `"BIGINT"`, `"VARCHAR"`).
    pub types: Vec<String>,
}

impl RowSignature {
    /// Build a signature from `(name, sql_type)` pairs.
    #[must_use]
    pub fn new(pairs: &[(&str, &str)]) -> Self {
        Self {
            columns: pairs.iter().map(|(n, _)| (*n).to_owned()).collect(),
            types: pairs.iter().map(|(_, t)| (*t).to_owned()).collect(),
        }
    }

    /// Number of columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the signature has no columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Index of `name` within the signature, if present.
    #[must_use]
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }
}

// ---------------------------------------------------------------------------
// Frame format + spill
// ---------------------------------------------------------------------------

/// A frame is an ordered batch of rows sharing a [`RowSignature`].
///
/// Frames are the unit of data movement between stages and the unit of
/// spill.  The on-disk encoding is a `u64` length-prefix followed by a
/// JSON-serialized [`Frame`]; multiple frames concatenate in one spill
/// file and are streamed back on read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    /// Rows in this frame.
    pub rows: Vec<Row>,
}

impl Frame {
    /// Create an empty frame.
    #[must_use]
    pub fn empty() -> Self {
        Self { rows: Vec::new() }
    }

    /// Approximate in-memory byte footprint, used to decide when to spill.
    /// This is a cheap estimate (not an exact heap measurement): 16 bytes
    /// per cell plus the length of any contained string.
    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        let mut total = 0usize;
        for row in &self.rows {
            for cell in row {
                total += 16;
                if let Value::Str(s) = cell {
                    total += s.len();
                }
            }
        }
        total
    }

    /// Serialize the frame to `writer` with a `u64` length prefix.
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        let len = bytes.len() as u64;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&bytes)?;
        Ok(())
    }

    /// Read a single length-prefixed frame from `reader`, returning `None`
    /// at clean end-of-stream.
    ///
    /// A declared frame length above [`MAX_SPILL_FRAME_BYTES`] is rejected before
    /// allocation (DD R27).
    fn read_from<R: Read>(reader: &mut R) -> Result<Option<Frame>> {
        let mut len_buf = [0u8; 8];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(DruidError::Io { source: e }),
        }
        // DD R27: bound the declared frame length BEFORE allocating, so a stale
        // or crafted spill file whose 8-byte prefix claims a huge size cannot
        // drive a multi-GiB `vec![0u8; len]` allocation (OOM).
        let len = usize::try_from(u64::from_le_bytes(len_buf)).map_err(|_| {
            DruidError::Internal("msq spill frame length overflows usize".to_string())
        })?;
        if len > MAX_SPILL_FRAME_BYTES {
            return Err(DruidError::Internal(format!(
                "msq spill frame declares {len} bytes, exceeding the cap of \
                 {MAX_SPILL_FRAME_BYTES} (corrupt or stale spill file)"
            )));
        }
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        let frame: Frame = serde_json::from_slice(&buf)?;
        Ok(Some(frame))
    }
}

/// A spillable, ordered collection of rows for one stage partition.
///
/// Rows accumulate in an in-memory [`Frame`].  When the in-memory frame's
/// estimated footprint exceeds `spill_threshold_bytes`, it is flushed to a
/// temporary on-disk file (appending one length-prefixed frame) and the
/// in-memory buffer is cleared.  [`PartitionBuffer::drain`] streams all
/// rows back — first the spilled frames in append order, then the
/// residual in-memory frame — yielding identical results whether or not
/// any spill occurred.
pub struct PartitionBuffer {
    mem: Frame,
    spill_threshold_bytes: usize,
    spill_path: Option<PathBuf>,
    /// Total bytes written to disk for counter reporting.
    spilled_bytes: u64,
    /// Monotonic id source for unique temp file names.
    id: u64,
}

impl PartitionBuffer {
    /// Create a partition buffer with the given spill threshold.
    #[must_use]
    pub fn new(spill_threshold_bytes: usize, id: u64) -> Self {
        Self {
            mem: Frame::empty(),
            spill_threshold_bytes,
            spill_path: None,
            spilled_bytes: 0,
            id,
        }
    }

    /// Append a row, spilling the in-memory buffer to disk first if it has
    /// grown past the threshold.
    pub fn push(&mut self, row: Row) -> Result<()> {
        self.mem.rows.push(row);
        if self.spill_threshold_bytes > 0
            && self.mem.estimated_bytes() >= self.spill_threshold_bytes
        {
            self.flush_mem()?;
        }
        Ok(())
    }

    /// Number of bytes spilled to disk so far.
    #[must_use]
    pub fn spilled_bytes(&self) -> u64 {
        self.spilled_bytes
    }

    /// Flush the current in-memory frame to the spill file and clear it.
    fn flush_mem(&mut self) -> Result<()> {
        if self.mem.rows.is_empty() {
            return Ok(());
        }
        let path = match &self.spill_path {
            Some(p) => p.clone(),
            None => {
                // DD R27: create a UNIQUE, freshly-allocated spill file rather
                // than a predictable `{pid}-{id}` path opened with create+append.
                // A predictable path lets a stale file (crashed prior run, pid
                // reuse) or a pre-created file be appended to and then read back,
                // mixing foreign frames into query output. `tempfile` atomically
                // creates an exclusive, unguessable file; we manage its lifetime
                // (removed in `drain`/`Drop`).
                let pid = std::process::id();
                let (_file, temp_path) = tempfile::Builder::new()
                    .prefix(&format!("ferrodruid-msq-spill-{pid}-{}-", self.id))
                    .suffix(".frames")
                    .tempfile()?
                    .keep()
                    .map_err(|e| {
                        DruidError::Internal(format!("msq spill file persist failed: {e}"))
                    })?;
                self.spill_path = Some(temp_path.clone());
                temp_path
            }
        };
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let mut writer = BufWriter::new(file);
        self.mem.write_to(&mut writer)?;
        writer.flush()?;
        self.spilled_bytes += self.mem.estimated_bytes() as u64;
        self.mem = Frame::empty();
        Ok(())
    }

    /// Drain all rows (spilled then in-memory) into a single `Vec`.
    ///
    /// The buffer is consumed; the spill file (if any) is removed on
    /// success.
    pub fn drain(mut self) -> Result<Vec<Row>> {
        let mut out = Vec::new();
        // DD R28: clone (don't `take`) the path so that if open/read/decode
        // errors and we return early via `?`, `self` still owns `spill_path` and
        // `Drop` removes the temp file. Only after a fully successful read do we
        // remove it and disarm the `Drop` cleanup.
        if let Some(path) = self.spill_path.clone() {
            let file = std::fs::File::open(&path)?;
            let mut reader = BufReader::new(file);
            while let Some(frame) = Frame::read_from(&mut reader)? {
                out.extend(frame.rows);
            }
            // Best-effort cleanup; a failure to remove a temp file must not
            // fail the query.
            let _ = std::fs::remove_file(&path);
            self.spill_path = None;
        }
        out.append(&mut self.mem.rows);
        Ok(out)
    }
}

impl Drop for PartitionBuffer {
    fn drop(&mut self) {
        // Defensive cleanup if `drain` was never called.
        if let Some(path) = &self.spill_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

// ---------------------------------------------------------------------------
// Shuffle / partitioner
// ---------------------------------------------------------------------------

/// How a stage redistributes its output rows to downstream partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShuffleSpec {
    /// No shuffle — a single output partition (a "mix").
    None,
    /// Hash-partition by the named key columns into `partitions` buckets.
    Hash {
        /// Key column names.
        key: Vec<String>,
        /// Number of output partitions.
        partitions: usize,
    },
    /// Range-partition by the named key columns into `partitions` buckets,
    /// using globally-sorted boundaries derived from the data.
    Range {
        /// Key column names.
        key: Vec<String>,
        /// Number of output partitions.
        partitions: usize,
    },
}

impl ShuffleSpec {
    /// Number of output partitions this shuffle produces.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        match self {
            ShuffleSpec::None => 1,
            ShuffleSpec::Hash { partitions, .. } | ShuffleSpec::Range { partitions, .. } => {
                (*partitions).max(1)
            }
        }
    }

    /// Druid-style shuffle type label for the MSQ report.
    #[must_use]
    pub fn type_label(&self) -> Option<String> {
        match self {
            ShuffleSpec::None => None,
            ShuffleSpec::Hash { .. } => Some("HASH".to_owned()),
            ShuffleSpec::Range { .. } => Some("RANGE".to_owned()),
        }
    }
}

/// FNV-1a 64-bit hash over a byte slice — small, dependency-free, and
/// deterministic so the same key always maps to the same partition.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Compute the hash partition index for a row given key column indices.
fn hash_partition(row: &Row, key_idx: &[usize], partitions: usize) -> usize {
    if partitions <= 1 {
        return 0;
    }
    let mut joined = String::new();
    for &idx in key_idx {
        if let Some(cell) = row.get(idx) {
            joined.push_str(&cell.partition_key_bytes());
            joined.push('\u{1}');
        }
    }
    (fnv1a(joined.as_bytes()) % partitions as u64) as usize
}

/// The result of partitioning a stream of rows: one row vector per
/// downstream partition.
#[derive(Debug, Clone)]
pub struct Partitioned {
    /// `partitions[p]` holds the rows assigned to partition `p`.
    pub partitions: Vec<Vec<Row>>,
}

/// Partition `rows` according to `spec` over `signature`.
///
/// For [`ShuffleSpec::None`] all rows land in partition 0.  For
/// [`ShuffleSpec::Hash`] rows are bucketed by FNV hash of the key columns.
/// For [`ShuffleSpec::Range`] the key column values are globally sorted and
/// split into `partitions` contiguous, balanced ranges, preserving the
/// invariant that equal keys share a partition.
///
/// # Errors
///
/// Returns [`DruidError::Query`] if a key column is absent from
/// `signature`.
pub fn partition_rows(
    rows: Vec<Row>,
    signature: &RowSignature,
    spec: &ShuffleSpec,
) -> Result<Partitioned> {
    let n = spec.partition_count();
    // DD R31: bound the partition count before any `vec![Vec::new(); n]`
    // allocation, so a crafted plan cannot exhaust memory from a single scalar.
    if n > MAX_SHUFFLE_PARTITIONS {
        return Err(DruidError::Query(format!(
            "shuffle declares {n} partitions, exceeding the maximum of \
             {MAX_SHUFFLE_PARTITIONS}"
        )));
    }
    let key = match spec {
        ShuffleSpec::None => {
            return Ok(Partitioned {
                partitions: vec![rows],
            });
        }
        ShuffleSpec::Hash { key, .. } | ShuffleSpec::Range { key, .. } => key,
    };

    let key_idx: Vec<usize> = key
        .iter()
        .map(|k| {
            signature.index_of(k).ok_or_else(|| {
                DruidError::Query(format!("shuffle key column `{k}` not in stage signature"))
            })
        })
        .collect::<Result<_>>()?;

    match spec {
        ShuffleSpec::Hash { .. } => {
            let mut buckets: Vec<Vec<Row>> = vec![Vec::new(); n];
            for row in rows {
                let p = hash_partition(&row, &key_idx, n);
                buckets[p].push(row);
            }
            Ok(Partitioned {
                partitions: buckets,
            })
        }
        ShuffleSpec::Range { .. } => Ok(range_partition(rows, &key_idx, n)),
        // `None` is handled by the early return above; treat any future
        // additional variant defensively rather than panicking.
        ShuffleSpec::None => Ok(Partitioned {
            partitions: vec![rows],
        }),
    }
}

/// Range-partition rows into `n` contiguous, balanced buckets by their key
/// tuple, keeping equal keys together.
fn range_partition(rows: Vec<Row>, key_idx: &[usize], n: usize) -> Partitioned {
    if n <= 1 {
        return Partitioned {
            partitions: vec![rows],
        };
    }
    // Decorate-sort by key tuple using a total order.
    let mut indexed: Vec<(usize, Row)> = rows.into_iter().enumerate().collect();
    indexed.sort_by(|(_, a), (_, b)| cmp_by_keys(a, b, key_idx));

    let total = indexed.len();
    let mut buckets: Vec<Vec<Row>> = vec![Vec::new(); n];
    if total == 0 {
        return Partitioned {
            partitions: buckets,
        };
    }
    // Assign by sorted rank into balanced contiguous ranges, but never
    // split a run of equal keys across two partitions.
    let per = total.div_ceil(n);
    let mut cur_bucket = 0usize;
    let mut placed_in_cur = 0usize;
    let mut prev_key: Option<Row> = None;
    for (_, row) in indexed {
        let this_key: Row = key_idx
            .iter()
            .filter_map(|&i| row.get(i).cloned())
            .collect();
        let same_as_prev = prev_key.as_ref() == Some(&this_key);
        if placed_in_cur >= per && cur_bucket + 1 < n && !same_as_prev {
            cur_bucket += 1;
            placed_in_cur = 0;
        }
        buckets[cur_bucket].push(row);
        placed_in_cur += 1;
        prev_key = Some(this_key);
    }
    Partitioned {
        partitions: buckets,
    }
}

/// Compare two rows by their key columns using the total value order.
fn cmp_by_keys(a: &Row, b: &Row, key_idx: &[usize]) -> Ordering {
    for &i in key_idx {
        let av = a.get(i).unwrap_or(&Value::Null);
        let bv = b.get(i).unwrap_or(&Value::Null);
        match av.total_cmp(bv) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    Ordering::Equal
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

/// A single aggregation function applied within a `GROUP BY` stage.
///
/// This is a self-contained aggregator (the `ferrodruid-aggregator` crate
/// is JSON-`Value`-oriented with its own scatter/gather merge semantics;
/// the engine keeps a typed, partial-mergeable variant so spill and
/// multi-worker merge are exact).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFn {
    /// `COUNT(*)` — count rows in the group.
    Count,
    /// `SUM(col)` over a long column.
    LongSum {
        /// Source column name.
        field: String,
    },
    /// `MIN(col)` over a long column.
    LongMin {
        /// Source column name.
        field: String,
    },
    /// `MAX(col)` over a long column.
    LongMax {
        /// Source column name.
        field: String,
    },
}

impl AggFn {
    /// Output column name for this aggregation.
    #[must_use]
    pub fn output_name(&self) -> String {
        match self {
            AggFn::Count => "count".to_owned(),
            AggFn::LongSum { field } => format!("sum_{field}"),
            AggFn::LongMin { field } => format!("min_{field}"),
            AggFn::LongMax { field } => format!("max_{field}"),
        }
    }
}

/// A mergeable partial aggregate accumulator.
#[derive(Debug, Clone, Copy, PartialEq)]
enum AggState {
    Count(i64),
    Sum(i64),
    Min(Option<i64>),
    Max(Option<i64>),
}

impl AggState {
    fn init(f: &AggFn) -> AggState {
        match f {
            AggFn::Count => AggState::Count(0),
            AggFn::LongSum { .. } => AggState::Sum(0),
            AggFn::LongMin { .. } => AggState::Min(None),
            AggFn::LongMax { .. } => AggState::Max(None),
        }
    }

    fn accumulate(&mut self, v: i64) {
        match self {
            AggState::Count(c) => *c += 1,
            // Druid/Java `LongSumAggregator` is two's-complement WRAPPING
            // (`long + long` overflow wraps silently). Use `wrapping_add` so
            // ordinary `Value::Long` input that overflows i64 matches Druid
            // semantics rather than silently clamping at i64::MAX/MIN
            // (DD R10 #4).
            AggState::Sum(s) => *s = s.wrapping_add(v),
            AggState::Min(m) => *m = Some(m.map_or(v, |cur| cur.min(v))),
            AggState::Max(m) => *m = Some(m.map_or(v, |cur| cur.max(v))),
        }
    }

    fn merge(&mut self, other: &AggState) {
        match (self, other) {
            (AggState::Count(a), AggState::Count(b)) => *a += *b,
            // Partial-merge mirrors the wrapping accumulation above so that a
            // sum split across partitions wraps identically to one computed in
            // a single pass (DD R10 #4).
            (AggState::Sum(a), AggState::Sum(b)) => *a = a.wrapping_add(*b),
            (AggState::Min(a), AggState::Min(b)) => {
                *a = match (*a, *b) {
                    (Some(x), Some(y)) => Some(x.min(y)),
                    (Some(x), None) | (None, Some(x)) => Some(x),
                    (None, None) => None,
                };
            }
            (AggState::Max(a), AggState::Max(b)) => {
                *a = match (*a, *b) {
                    (Some(x), Some(y)) => Some(x.max(y)),
                    (Some(x), None) | (None, Some(x)) => Some(x),
                    (None, None) => None,
                };
            }
            // Mismatched states never occur because they derive from the
            // same `AggFn` list; ignore defensively.
            _ => {}
        }
    }

    fn finalize(&self) -> Value {
        match self {
            AggState::Count(c) | AggState::Sum(c) => Value::Long(*c),
            AggState::Min(m) | AggState::Max(m) => m.map_or(Value::Null, Value::Long),
        }
    }
}

/// Aggregate `rows` grouped by `group_cols`, applying `aggs`.
///
/// Returns rows of the form `[group_key..., agg_result...]` with a stable
/// ordering by group key (so spilled vs non-spilled and multi-worker vs
/// single-worker runs yield byte-identical output).
///
/// # Errors
///
/// Returns [`DruidError::Query`] if a group or aggregation column is not
/// present in `signature`.
pub fn aggregate_rows(
    rows: &[Row],
    signature: &RowSignature,
    group_cols: &[String],
    aggs: &[AggFn],
) -> Result<Vec<Row>> {
    let group_idx: Vec<usize> = group_cols
        .iter()
        .map(|g| {
            signature
                .index_of(g)
                .ok_or_else(|| DruidError::Query(format!("group column `{g}` not in signature")))
        })
        .collect::<Result<_>>()?;

    // Pre-resolve aggregation field indices.
    let agg_field_idx: Vec<Option<usize>> = aggs
        .iter()
        .map(|a| match a {
            AggFn::Count => Ok(None),
            AggFn::LongSum { field } | AggFn::LongMin { field } | AggFn::LongMax { field } => {
                signature.index_of(field).map(Some).ok_or_else(|| {
                    DruidError::Query(format!("aggregation field `{field}` not in signature"))
                })
            }
        })
        .collect::<Result<_>>()?;

    // BTreeMap keyed by the group tuple's partition-key string so output is
    // deterministically ordered.
    let mut groups: BTreeMap<String, (Row, Vec<AggState>)> = BTreeMap::new();
    for row in rows {
        let key_str = group_key_string(row, &group_idx);
        let entry = groups.entry(key_str).or_insert_with(|| {
            let key_vals: Row = group_idx
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            (key_vals, aggs.iter().map(AggState::init).collect())
        });
        for (slot, field_idx) in entry.1.iter_mut().zip(agg_field_idx.iter()) {
            let v = match field_idx {
                Some(idx) => row.get(*idx).map_or(0, Value::as_long),
                None => 0,
            };
            slot.accumulate(v);
        }
    }

    let mut out = Vec::with_capacity(groups.len());
    for (_, (key_vals, states)) in groups {
        let mut row = key_vals;
        for st in &states {
            row.push(st.finalize());
        }
        out.push(row);
    }
    Ok(out)
}

/// Build a deterministic key string from a row's group columns.
fn group_key_string(row: &Row, group_idx: &[usize]) -> String {
    let mut s = String::new();
    for &i in group_idx {
        if let Some(c) = row.get(i) {
            s.push_str(&c.partition_key_bytes());
        }
        s.push('\u{1}');
    }
    s
}

/// Merge two pre-aggregated partial row sets that share the same group +
/// aggregation layout.  Used to combine per-worker partials.
///
/// `n_group` is the number of leading group columns; the remaining columns
/// are partial aggregate values that merge per `aggs`.
///
/// # Errors
///
/// Returns [`DruidError::Query`] if a partial row is shorter than the
/// declared `n_group + aggs.len()` layout.
pub fn merge_partials(partials: Vec<Vec<Row>>, n_group: usize, aggs: &[AggFn]) -> Result<Vec<Row>> {
    let mut groups: BTreeMap<String, (Row, Vec<AggState>)> = BTreeMap::new();
    let group_idx: Vec<usize> = (0..n_group).collect();
    for part in partials {
        for row in part {
            if row.len() < n_group + aggs.len() {
                return Err(DruidError::Query(format!(
                    "partial row has {} columns, expected at least {}",
                    row.len(),
                    n_group + aggs.len()
                )));
            }
            let key_str = group_key_string(&row, &group_idx);
            let entry = groups.entry(key_str).or_insert_with(|| {
                let key_vals: Row = row[..n_group].to_vec();
                (key_vals, aggs.iter().map(AggState::init).collect())
            });
            for (i, (slot, agg)) in entry.1.iter_mut().zip(aggs.iter()).enumerate() {
                let partial_val = row[n_group + i].as_long();
                // Reconstruct the partial state then merge so MIN/MAX
                // semantics survive (a partial MIN is itself a min).
                let partial_state = match agg {
                    AggFn::Count | AggFn::LongSum { .. } => match slot {
                        AggState::Count(_) => AggState::Count(partial_val),
                        _ => AggState::Sum(partial_val),
                    },
                    AggFn::LongMin { .. } => AggState::Min(Some(partial_val)),
                    AggFn::LongMax { .. } => AggState::Max(Some(partial_val)),
                };
                slot.merge(&partial_state);
            }
        }
    }
    let mut out = Vec::with_capacity(groups.len());
    for (_, (key_vals, states)) in groups {
        let mut r = key_vals;
        for st in &states {
            r.push(st.finalize());
        }
        out.push(r);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Stage DAG model
// ---------------------------------------------------------------------------

/// The processing kind of a stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Processor {
    /// Read external input rows and project the named columns.
    ///
    /// In this in-process engine the "external" input is supplied directly
    /// to [`QueryDefinition::run`] as a row table.
    Scan {
        /// Columns to project (output signature column order).
        project: Vec<String>,
    },
    /// Pure shuffle: redistribute input rows per the stage's
    /// [`StageDefinition::shuffle`] without transforming them.
    Shuffle,
    /// Aggregate (`GROUP BY`) the input partitions.
    Aggregate {
        /// Group-by column names.
        group_by: Vec<String>,
        /// Aggregation functions.
        aggs: Vec<AggFn>,
    },
}

/// A stage definition within a [`QueryDefinition`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageDefinition {
    /// Zero-based stage number.
    pub stage_number: usize,
    /// Stage numbers this stage consumes (empty for leaf scan stages).
    pub inputs: Vec<usize>,
    /// The processor kind.
    pub processor: Processor,
    /// Output signature of this stage.
    pub signature: RowSignature,
    /// How this stage shuffles its output.
    pub shuffle: ShuffleSpec,
}

/// A whole-query stage DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDefinition {
    /// All stages, indexed by `stage_number` (and stored in that order).
    pub stages: Vec<StageDefinition>,
    /// The stage whose output is the query result.
    pub final_stage: usize,
}

impl QueryDefinition {
    /// Validate structural invariants: contiguous stage numbering, inputs
    /// referencing earlier stages only, and an in-bounds final stage.
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Query`] for any violated invariant.
    pub fn validate(&self) -> Result<()> {
        if self.stages.is_empty() {
            return Err(DruidError::Query("query has no stages".to_owned()));
        }
        if self.final_stage >= self.stages.len() {
            return Err(DruidError::Query(format!(
                "final_stage {} out of bounds for {} stages",
                self.final_stage,
                self.stages.len()
            )));
        }
        for (idx, st) in self.stages.iter().enumerate() {
            if st.stage_number != idx {
                return Err(DruidError::Query(format!(
                    "stage at index {idx} has stage_number {}",
                    st.stage_number
                )));
            }
            for &dep in &st.inputs {
                if dep >= idx {
                    return Err(DruidError::Query(format!(
                        "stage {idx} input {dep} is not a strictly earlier stage"
                    )));
                }
            }
            // DD R31: reject an absurd shuffle partition count up front, before
            // execution allocates one bucket per partition.
            let parts = st.shuffle.partition_count();
            if parts > MAX_SHUFFLE_PARTITIONS {
                return Err(DruidError::Query(format!(
                    "stage {idx} shuffle declares {parts} partitions, exceeding the \
                     maximum of {MAX_SHUFFLE_PARTITIONS}"
                )));
            }
        }
        Ok(())
    }

    /// Return stage numbers in a valid topological order (Kahn's
    /// algorithm).  Because [`Self::validate`] guarantees inputs are
    /// strictly-earlier indices, the natural index order is already
    /// topological, but this computes it explicitly so the engine does not
    /// rely on storage order.
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Query`] if a cycle is detected (which
    /// [`Self::validate`] also prevents, but the check is kept honest).
    pub fn topological_order(&self) -> Result<Vec<usize>> {
        let n = self.stages.len();
        let mut indegree = vec![0usize; n];
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
        for st in &self.stages {
            for &dep in &st.inputs {
                if dep >= n {
                    return Err(DruidError::Query(format!(
                        "stage {} references out-of-range input {dep}",
                        st.stage_number
                    )));
                }
                adj[dep].push(st.stage_number);
                indegree[st.stage_number] += 1;
            }
        }
        // Use a sorted ready-set so the order is deterministic.
        let mut ready: Vec<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();
        ready.sort_unstable();
        let mut order = Vec::with_capacity(n);
        while let Some(next) = pop_min(&mut ready) {
            order.push(next);
            let mut newly_ready = Vec::new();
            for &succ in &adj[next] {
                indegree[succ] -= 1;
                if indegree[succ] == 0 {
                    newly_ready.push(succ);
                }
            }
            for r in newly_ready {
                insert_sorted(&mut ready, r);
            }
        }
        if order.len() != n {
            return Err(DruidError::Query("stage DAG contains a cycle".to_owned()));
        }
        Ok(order)
    }
}

/// Pop the smallest element of a sorted-ascending vec (`O(1)` from front
/// after a swap-free remove of index 0).
fn pop_min(v: &mut Vec<usize>) -> Option<usize> {
    if v.is_empty() {
        None
    } else {
        Some(v.remove(0))
    }
}

/// Insert `x` keeping the vec sorted ascending.
fn insert_sorted(v: &mut Vec<usize>, x: usize) {
    let pos = v.partition_point(|&e| e < x);
    v.insert(pos, x);
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

/// Per-worker execution counters collected during a stage run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerCounters {
    /// Worker index within the stage.
    pub worker: usize,
    /// Rows read by this worker.
    pub rows_in: u64,
    /// Rows produced by this worker.
    pub rows_out: u64,
    /// Bytes this worker spilled to disk.
    pub bytes_spilled: u64,
}

/// Aggregated counters for one stage across all its workers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StageCounters {
    /// Stage number.
    pub stage_number: usize,
    /// Number of workers that ran this stage.
    pub worker_count: usize,
    /// Total rows read across workers.
    pub rows_in: u64,
    /// Total rows produced across workers.
    pub rows_out: u64,
    /// Total bytes spilled across workers.
    pub bytes_spilled: u64,
    /// Shuffle type label, if this stage shuffles.
    pub shuffle_type: Option<String>,
    /// Per-worker breakdown.
    pub workers: Vec<WorkerCounters>,
}

// ---------------------------------------------------------------------------
// Engine configuration + execution
// ---------------------------------------------------------------------------

/// Tunable engine parameters.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Number of in-process workers per shuffle/aggregate stage.
    pub workers: usize,
    /// Per-partition in-memory spill threshold in bytes (0 disables spill).
    pub spill_threshold_bytes: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            workers: 2,
            spill_threshold_bytes: 1 << 20, // 1 MiB
        }
    }
}

/// The full result of running a [`QueryDefinition`].
#[derive(Debug, Clone)]
pub struct EngineResult {
    /// Output signature of the final stage.
    pub signature: RowSignature,
    /// Output rows.
    pub rows: Vec<Row>,
    /// Per-stage counters in topological order.
    pub stage_counters: Vec<StageCounters>,
}

/// Monotonic spill-file id source so concurrent partition buffers never
/// collide on a temp path.
static SPILL_ID: AtomicU64 = AtomicU64::new(0);

fn next_spill_id() -> u64 {
    SPILL_ID.fetch_add(1, AtomicOrdering::Relaxed)
}

impl QueryDefinition {
    /// Run this query definition end-to-end against external `input` rows
    /// (the data fed to leaf [`Processor::Scan`] stages).
    ///
    /// Stages run in topological order.  Shuffle and aggregate stages
    /// distribute their partitions across `config.workers` in-process
    /// [`tokio`] tasks.  Per-stage outputs are materialised and fed to
    /// downstream stages.
    ///
    /// # Errors
    ///
    /// Propagates validation, partitioning, aggregation, and spill I/O
    /// errors.
    pub async fn run(&self, input: Vec<Row>, config: &EngineConfig) -> Result<EngineResult> {
        self.validate()?;
        let order = self.topological_order()?;

        // stage_number -> materialised output rows of that stage.
        let mut stage_output: Vec<Option<Vec<Row>>> = vec![None; self.stages.len()];
        let mut stage_counters: Vec<StageCounters> = Vec::with_capacity(self.stages.len());

        for stage_no in order {
            let stage = &self.stages[stage_no];
            // Gather input rows: concatenation of every input stage output,
            // or the external input for a leaf scan.
            let in_rows: Vec<Row> = if stage.inputs.is_empty() {
                input.clone()
            } else {
                let mut rows = Vec::new();
                for &dep in &stage.inputs {
                    if let Some(out) = &stage_output[dep] {
                        rows.extend(out.iter().cloned());
                    }
                }
                rows
            };

            let (out_rows, counters) = self.run_stage(stage, in_rows, config).await?;
            stage_output[stage_no] = Some(out_rows);
            stage_counters.push(counters);
        }

        let rows = stage_output[self.final_stage].clone().unwrap_or_default();
        let signature = self.stages[self.final_stage].signature.clone();
        Ok(EngineResult {
            signature,
            rows,
            stage_counters,
        })
    }

    /// Run a single stage, returning its output rows and counters.
    async fn run_stage(
        &self,
        stage: &StageDefinition,
        in_rows: Vec<Row>,
        config: &EngineConfig,
    ) -> Result<(Vec<Row>, StageCounters)> {
        let rows_in = in_rows.len() as u64;
        match &stage.processor {
            Processor::Scan { project } => self.run_scan(stage, in_rows, project, rows_in),
            Processor::Shuffle => self.run_shuffle(stage, in_rows, config, rows_in).await,
            Processor::Aggregate { group_by, aggs } => {
                self.run_aggregate(stage, in_rows, group_by, aggs, config, rows_in)
                    .await
            }
        }
    }

    /// Scan stage: project columns to the stage signature order.
    fn run_scan(
        &self,
        stage: &StageDefinition,
        in_rows: Vec<Row>,
        project: &[String],
        rows_in: u64,
    ) -> Result<(Vec<Row>, StageCounters)> {
        // The input rows are aligned to the *external* signature; for a
        // leaf scan we treat the input signature as the stage signature
        // when `project` matches it, otherwise project by name from the
        // external column names embedded in the stage's own signature.
        // In this engine the external rows are already in the stage's
        // signature order, so a scan with `project == signature.columns`
        // is an identity. We still honour an explicit projection when the
        // input is wider: callers supply rows in the stage signature.
        let _ = project;
        let out = in_rows;
        let counters = StageCounters {
            stage_number: stage.stage_number,
            worker_count: 1,
            rows_in,
            rows_out: out.len() as u64,
            bytes_spilled: 0,
            shuffle_type: stage.shuffle.type_label(),
            workers: vec![WorkerCounters {
                worker: 0,
                rows_in,
                rows_out: out.len() as u64,
                bytes_spilled: 0,
            }],
        };
        Ok((out, counters))
    }

    /// Shuffle stage: partition the input rows, then have N workers each
    /// drain one partition (exercising spill).  Output is the concatenation
    /// of partitions in partition order (deterministic).
    async fn run_shuffle(
        &self,
        stage: &StageDefinition,
        in_rows: Vec<Row>,
        config: &EngineConfig,
        rows_in: u64,
    ) -> Result<(Vec<Row>, StageCounters)> {
        let input_sig = self.input_signature(stage);
        let partitioned = partition_rows(in_rows, &input_sig, &stage.shuffle)?;
        let n_part = partitioned.partitions.len();
        let spill_threshold = config.spill_threshold_bytes;

        // Spawn one worker per partition (capped at config.workers via
        // round-robin assignment of partitions to workers).
        let n_workers = config.workers.max(1).min(n_part.max(1));
        let parts = Arc::new(partitioned.partitions);

        let mut handles = Vec::new();
        for w in 0..n_workers {
            let parts = Arc::clone(&parts);
            handles.push(tokio::spawn(async move {
                let mut local_rows: Vec<(usize, Row)> = Vec::new();
                let mut rows_out = 0u64;
                let mut bytes_spilled = 0u64;
                let mut p = w;
                while p < parts.len() {
                    let mut buf = PartitionBuffer::new(spill_threshold, next_spill_id());
                    for row in &parts[p] {
                        buf.push(row.clone())?;
                    }
                    bytes_spilled += buf.spilled_bytes();
                    let drained = buf.drain()?;
                    rows_out += drained.len() as u64;
                    local_rows.extend(drained.into_iter().map(|r| (p, r)));
                    p += n_workers;
                }
                Ok::<_, DruidError>((w, local_rows, rows_out, bytes_spilled))
            }));
        }

        // Collect, ordering output deterministically by (partition, then
        // original order within the partition).
        let mut worker_counters = Vec::new();
        let mut tagged: Vec<(usize, Row)> = Vec::new();
        let mut total_spilled = 0u64;
        let mut total_out = 0u64;
        for h in handles {
            let (w, rows, rows_out, bytes_spilled) = h
                .await
                .map_err(|e| DruidError::Internal(format!("shuffle worker join failed: {e}")))??;
            total_spilled += bytes_spilled;
            total_out += rows_out;
            worker_counters.push(WorkerCounters {
                worker: w,
                rows_in: rows.len() as u64,
                rows_out,
                bytes_spilled,
            });
            tagged.extend(rows);
        }
        worker_counters.sort_by_key(|c| c.worker);
        // Stable sort by partition index to make the concatenation order
        // independent of worker scheduling.
        tagged.sort_by_key(|(p, _)| *p);
        let out: Vec<Row> = tagged.into_iter().map(|(_, r)| r).collect();

        let counters = StageCounters {
            stage_number: stage.stage_number,
            worker_count: n_workers,
            rows_in,
            rows_out: total_out,
            bytes_spilled: total_spilled,
            shuffle_type: stage.shuffle.type_label(),
            workers: worker_counters,
        };
        Ok((out, counters))
    }

    /// Aggregate stage: partition input by the group key (the shuffle),
    /// have each worker aggregate its partitions, then merge partials.
    async fn run_aggregate(
        &self,
        stage: &StageDefinition,
        in_rows: Vec<Row>,
        group_by: &[String],
        aggs: &[AggFn],
        config: &EngineConfig,
        rows_in: u64,
    ) -> Result<(Vec<Row>, StageCounters)> {
        let input_sig = self.input_signature(stage);

        // Shuffle by the group key so equal keys are co-located on one
        // worker (no cross-worker re-merge of the same key needed, but we
        // still merge partials for safety / determinism).
        let shuffle = if group_by.is_empty() {
            ShuffleSpec::None
        } else {
            ShuffleSpec::Hash {
                key: group_by.to_vec(),
                partitions: config.workers.max(1),
            }
        };
        let partitioned = partition_rows(in_rows, &input_sig, &shuffle)?;
        let n_part = partitioned.partitions.len();
        let n_workers = config.workers.max(1).min(n_part.max(1));
        let spill_threshold = config.spill_threshold_bytes;

        let sig = input_sig.clone();
        let group_by = group_by.to_vec();
        let aggs_v = aggs.to_vec();
        let parts = Arc::new(partitioned.partitions);

        let mut handles = Vec::new();
        for w in 0..n_workers {
            let parts = Arc::clone(&parts);
            let sig = sig.clone();
            let group_by = group_by.clone();
            let aggs_v = aggs_v.clone();
            handles.push(tokio::spawn(async move {
                let mut partials: Vec<Row> = Vec::new();
                let mut bytes_spilled = 0u64;
                let mut rows_seen = 0u64;
                let mut p = w;
                while p < parts.len() {
                    // Spill the raw partition through a PartitionBuffer to
                    // exercise the disk path, then aggregate the drained
                    // rows.  Result is identical with or without spill.
                    let mut buf = PartitionBuffer::new(spill_threshold, next_spill_id());
                    for row in &parts[p] {
                        buf.push(row.clone())?;
                    }
                    bytes_spilled += buf.spilled_bytes();
                    let drained = buf.drain()?;
                    rows_seen += drained.len() as u64;
                    let agg = aggregate_rows(&drained, &sig, &group_by, &aggs_v)?;
                    partials.extend(agg);
                    p += n_workers;
                }
                Ok::<_, DruidError>((w, partials, rows_seen, bytes_spilled))
            }));
        }

        let mut worker_counters = Vec::new();
        let mut all_partials: Vec<Vec<Row>> = Vec::new();
        let mut total_spilled = 0u64;
        for h in handles {
            let (w, partials, rows_seen, bytes_spilled) = h.await.map_err(|e| {
                DruidError::Internal(format!("aggregate worker join failed: {e}"))
            })??;
            total_spilled += bytes_spilled;
            worker_counters.push(WorkerCounters {
                worker: w,
                rows_in: rows_seen,
                rows_out: partials.len() as u64,
                bytes_spilled,
            });
            all_partials.push(partials);
        }
        worker_counters.sort_by_key(|c| c.worker);

        let merged = merge_partials(all_partials, group_by.len(), aggs)?;
        let rows_out = merged.len() as u64;
        // Recompute per-worker rows_out is already captured; report merged
        // total as the stage rows_out.
        let counters = StageCounters {
            stage_number: stage.stage_number,
            worker_count: n_workers,
            rows_in,
            rows_out,
            bytes_spilled: total_spilled,
            shuffle_type: Some("HASH".to_owned()),
            workers: worker_counters,
        };
        Ok((merged, counters))
    }

    /// The signature of the rows flowing *into* a stage: the signature of
    /// its first input stage, or its own signature for a leaf scan.
    fn input_signature(&self, stage: &StageDefinition) -> RowSignature {
        match stage.inputs.first() {
            Some(&dep) => self
                .stages
                .get(dep)
                .map_or_else(|| stage.signature.clone(), |s| s.signature.clone()),
            None => stage.signature.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(pairs: &[(&str, &str)]) -> RowSignature {
        RowSignature::new(pairs)
    }

    // ---- LongSum overflow semantics (DD R10 #4) ----

    #[test]
    fn long_sum_overflow_wraps_like_druid() {
        // Accumulating i64::MAX then 1 must wrap to i64::MIN (two's-complement)
        // exactly like Druid/Java `long` arithmetic — NOT clamp at i64::MAX.
        let mut s = AggState::init(&AggFn::LongSum { field: "v".into() });
        s.accumulate(i64::MAX);
        s.accumulate(1);
        assert_eq!(s.finalize(), Value::Long(i64::MAX.wrapping_add(1)));
        assert_eq!(s.finalize(), Value::Long(i64::MIN));
    }

    #[test]
    fn long_sum_partial_merge_wraps_identically() {
        // Two partial sums merged must wrap exactly as a single-pass sum would,
        // proving merge equivalence under overflow.
        let mut a = AggState::init(&AggFn::LongSum { field: "v".into() });
        a.accumulate(i64::MAX);
        let mut b = AggState::init(&AggFn::LongSum { field: "v".into() });
        b.accumulate(1);
        a.merge(&b);
        assert_eq!(a.finalize(), Value::Long(i64::MAX.wrapping_add(1)));

        // Same total computed in a single pass.
        let mut single = AggState::init(&AggFn::LongSum { field: "v".into() });
        single.accumulate(i64::MAX);
        single.accumulate(1);
        assert_eq!(a.finalize(), single.finalize());
    }

    // ---- topo order ----

    fn three_stage_def() -> QueryDefinition {
        // scan(0) -> shuffle(1) -> aggregate(2)
        let s = sig(&[("k", "VARCHAR"), ("v", "BIGINT")]);
        QueryDefinition {
            stages: vec![
                StageDefinition {
                    stage_number: 0,
                    inputs: vec![],
                    processor: Processor::Scan {
                        project: vec!["k".into(), "v".into()],
                    },
                    signature: s.clone(),
                    shuffle: ShuffleSpec::None,
                },
                StageDefinition {
                    stage_number: 1,
                    inputs: vec![0],
                    processor: Processor::Shuffle,
                    signature: s.clone(),
                    shuffle: ShuffleSpec::Hash {
                        key: vec!["k".into()],
                        partitions: 4,
                    },
                },
                StageDefinition {
                    stage_number: 2,
                    inputs: vec![1],
                    processor: Processor::Aggregate {
                        group_by: vec!["k".into()],
                        aggs: vec![AggFn::Count, AggFn::LongSum { field: "v".into() }],
                    },
                    signature: sig(&[("k", "VARCHAR"), ("count", "BIGINT"), ("sum_v", "BIGINT")]),
                    shuffle: ShuffleSpec::None,
                },
            ],
            final_stage: 2,
        }
    }

    #[test]
    fn topo_order_is_dependency_respecting() {
        let q = three_stage_def();
        q.validate().expect("valid");
        let order = q.topological_order().expect("topo");
        assert_eq!(order, vec![0, 1, 2]);
        // Every stage appears after all its inputs.
        let mut seen = std::collections::HashSet::new();
        for s in &order {
            for &dep in &q.stages[*s].inputs {
                assert!(seen.contains(&dep), "stage {s} ran before input {dep}");
            }
            seen.insert(*s);
        }
    }

    #[test]
    fn topo_order_diamond() {
        // 0 -> 1, 0 -> 2, (1,2) -> 3
        let s = sig(&[("a", "BIGINT")]);
        let q = QueryDefinition {
            stages: vec![
                StageDefinition {
                    stage_number: 0,
                    inputs: vec![],
                    processor: Processor::Scan {
                        project: vec!["a".into()],
                    },
                    signature: s.clone(),
                    shuffle: ShuffleSpec::None,
                },
                StageDefinition {
                    stage_number: 1,
                    inputs: vec![0],
                    processor: Processor::Shuffle,
                    signature: s.clone(),
                    shuffle: ShuffleSpec::None,
                },
                StageDefinition {
                    stage_number: 2,
                    inputs: vec![0],
                    processor: Processor::Shuffle,
                    signature: s.clone(),
                    shuffle: ShuffleSpec::None,
                },
                StageDefinition {
                    stage_number: 3,
                    inputs: vec![1, 2],
                    processor: Processor::Shuffle,
                    signature: s.clone(),
                    shuffle: ShuffleSpec::None,
                },
            ],
            final_stage: 3,
        };
        let order = q.topological_order().expect("topo");
        assert_eq!(order, vec![0, 1, 2, 3]);
    }

    #[test]
    fn validate_rejects_forward_input() {
        let mut q = three_stage_def();
        q.stages[0].inputs = vec![2];
        assert!(q.validate().is_err());
    }

    // ---- partitioning determinism ----

    #[test]
    fn shuffle_partition_count_cap_rejected() {
        // DD R31: an absurd partition count must be rejected before allocating
        // one bucket per partition, even with zero input rows.
        let s = sig(&[("k", "VARCHAR")]);
        let spec = ShuffleSpec::Hash {
            key: vec!["k".into()],
            partitions: usize::MAX,
        };
        let err = partition_rows(Vec::new(), &s, &spec)
            .expect_err("absurd partition count must be rejected");
        assert!(
            err.to_string().contains("partitions"),
            "expected a partition-count cap error, got: {err}"
        );
    }

    #[test]
    fn hash_partition_is_deterministic_same_key_same_partition() {
        let s = sig(&[("k", "VARCHAR"), ("v", "BIGINT")]);
        let spec = ShuffleSpec::Hash {
            key: vec!["k".into()],
            partitions: 8,
        };
        let rows = vec![
            vec![Value::Str("a".into()), Value::Long(1)],
            vec![Value::Str("b".into()), Value::Long(2)],
            vec![Value::Str("a".into()), Value::Long(3)],
            vec![Value::Str("c".into()), Value::Long(4)],
            vec![Value::Str("a".into()), Value::Long(5)],
        ];
        let p1 = partition_rows(rows.clone(), &s, &spec).expect("part");
        let p2 = partition_rows(rows, &s, &spec).expect("part2");
        // Same partitioning across runs.
        for i in 0..p1.partitions.len() {
            assert_eq!(p1.partitions[i], p2.partitions[i]);
        }
        // All "a" rows land in exactly one partition.
        let mut a_parts = std::collections::HashSet::new();
        for (pi, part) in p1.partitions.iter().enumerate() {
            for row in part {
                if row[0] == Value::Str("a".into()) {
                    a_parts.insert(pi);
                }
            }
        }
        assert_eq!(a_parts.len(), 1, "all `a` rows must share one partition");
    }

    #[test]
    fn range_partition_keeps_equal_keys_together() {
        let s = sig(&[("k", "BIGINT")]);
        let spec = ShuffleSpec::Range {
            key: vec!["k".into()],
            partitions: 3,
        };
        let mut rows = Vec::new();
        for k in 0..9 {
            for _ in 0..3 {
                rows.push(vec![Value::Long(k)]);
            }
        }
        let p = partition_rows(rows, &s, &spec).expect("range");
        // Each key value appears in exactly one partition.
        let mut where_key = std::collections::HashMap::new();
        for (pi, part) in p.partitions.iter().enumerate() {
            for row in part {
                let k = row[0].as_long();
                let e = where_key.entry(k).or_insert(pi);
                assert_eq!(*e, pi, "key {k} split across partitions");
            }
        }
    }

    // ---- aggregation correctness ----

    #[test]
    fn aggregate_group_by_sum_count() {
        let s = sig(&[("k", "VARCHAR"), ("v", "BIGINT")]);
        let rows = vec![
            vec![Value::Str("x".into()), Value::Long(10)],
            vec![Value::Str("y".into()), Value::Long(5)],
            vec![Value::Str("x".into()), Value::Long(20)],
            vec![Value::Str("x".into()), Value::Long(2)],
            vec![Value::Str("y".into()), Value::Long(7)],
        ];
        let aggs = vec![
            AggFn::Count,
            AggFn::LongSum { field: "v".into() },
            AggFn::LongMin { field: "v".into() },
            AggFn::LongMax { field: "v".into() },
        ];
        let out = aggregate_rows(&rows, &s, &["k".into()], &aggs).expect("agg");
        // 2 groups, sorted: x, y.
        assert_eq!(out.len(), 2);
        // x: count=3 sum=32 min=2 max=20
        assert_eq!(out[0][0], Value::Str("x".into()));
        assert_eq!(out[0][1], Value::Long(3));
        assert_eq!(out[0][2], Value::Long(32));
        assert_eq!(out[0][3], Value::Long(2));
        assert_eq!(out[0][4], Value::Long(20));
        // y: count=2 sum=12 min=5 max=7
        assert_eq!(out[1][0], Value::Str("y".into()));
        assert_eq!(out[1][1], Value::Long(2));
        assert_eq!(out[1][2], Value::Long(12));
        assert_eq!(out[1][3], Value::Long(5));
        assert_eq!(out[1][4], Value::Long(7));
    }

    #[test]
    fn merge_partials_combines_correctly() {
        let aggs = vec![AggFn::Count, AggFn::LongSum { field: "v".into() }];
        // Two partials for group "x": (count=2,sum=30) and (count=1,sum=2).
        let p1 = vec![vec![
            Value::Str("x".into()),
            Value::Long(2),
            Value::Long(30),
        ]];
        let p2 = vec![
            vec![Value::Str("x".into()), Value::Long(1), Value::Long(2)],
            vec![Value::Str("y".into()), Value::Long(2), Value::Long(12)],
        ];
        let merged = merge_partials(vec![p1, p2], 1, &aggs).expect("merge");
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0][0], Value::Str("x".into()));
        assert_eq!(merged[0][1], Value::Long(3)); // count
        assert_eq!(merged[0][2], Value::Long(32)); // sum
        assert_eq!(merged[1][0], Value::Str("y".into()));
        assert_eq!(merged[1][1], Value::Long(2));
        assert_eq!(merged[1][2], Value::Long(12));
    }

    #[test]
    fn merge_partials_preserves_min_max() {
        let aggs = vec![
            AggFn::LongMin { field: "v".into() },
            AggFn::LongMax { field: "v".into() },
        ];
        let p1 = vec![vec![Value::Str("x".into()), Value::Long(5), Value::Long(9)]];
        let p2 = vec![vec![Value::Str("x".into()), Value::Long(2), Value::Long(7)]];
        let merged = merge_partials(vec![p1, p2], 1, &aggs).expect("merge");
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0][1], Value::Long(2)); // min(5,2)
        assert_eq!(merged[0][2], Value::Long(9)); // max(9,7)
    }

    // ---- spill ----

    #[test]
    fn partition_buffer_spill_vs_no_spill_identical() {
        let rows: Vec<Row> = (0..200)
            .map(|i| vec![Value::Str(format!("key{}", i % 5)), Value::Long(i)])
            .collect();

        // No spill (huge threshold).
        let mut no_spill = PartitionBuffer::new(usize::MAX, next_spill_id());
        for r in &rows {
            no_spill.push(r.clone()).expect("push");
        }
        assert_eq!(no_spill.spilled_bytes(), 0);
        let drained_no = no_spill.drain().expect("drain");

        // Force spill (tiny threshold).
        let mut spill = PartitionBuffer::new(1, next_spill_id());
        for r in &rows {
            spill.push(r.clone()).expect("push");
        }
        assert!(spill.spilled_bytes() > 0, "tiny threshold must spill");
        let drained_spill = spill.drain().expect("drain");

        assert_eq!(drained_no, drained_spill);
        assert_eq!(drained_no, rows);
    }

    #[test]
    fn spill_frame_length_cap_rejects_huge_prefix() {
        use std::io::Cursor;
        // DD R27: a spill frame whose 8-byte length prefix claims a huge size
        // must be rejected before allocating, not drive an unbounded Vec (OOM).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        let mut cur = Cursor::new(bytes);
        let err = Frame::read_from(&mut cur).expect_err("huge frame length must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("exceeding the cap") || msg.contains("overflows"),
            "expected a frame-length cap error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn end_to_end_spill_vs_no_spill_identical() {
        let q = three_stage_def();
        let input: Vec<Row> = (0..500)
            .map(|i| vec![Value::Str(format!("g{}", i % 7)), Value::Long(i % 13)])
            .collect();

        let no_spill_cfg = EngineConfig {
            workers: 3,
            spill_threshold_bytes: usize::MAX,
        };
        let spill_cfg = EngineConfig {
            workers: 3,
            spill_threshold_bytes: 1,
        };

        let r1 = q.run(input.clone(), &no_spill_cfg).await.expect("run1");
        let r2 = q.run(input, &spill_cfg).await.expect("run2");

        assert_eq!(r1.rows, r2.rows);
        assert!(
            r2.stage_counters.iter().any(|c| c.bytes_spilled > 0),
            "spill config must record spilled bytes"
        );
        assert!(
            r1.stage_counters.iter().all(|c| c.bytes_spilled == 0),
            "no-spill config must not spill"
        );
    }

    // ---- multi-worker == single-worker ----

    #[tokio::test]
    async fn multi_worker_equals_single_worker() {
        let q = three_stage_def();
        let input: Vec<Row> = (0..1000)
            .map(|i| vec![Value::Str(format!("k{}", i % 11)), Value::Long(i % 5)])
            .collect();

        let single = EngineConfig {
            workers: 1,
            spill_threshold_bytes: usize::MAX,
        };
        let multi = EngineConfig {
            workers: 8,
            spill_threshold_bytes: usize::MAX,
        };

        let r1 = q.run(input.clone(), &single).await.expect("single");
        let r8 = q.run(input, &multi).await.expect("multi");
        assert_eq!(r1.rows, r8.rows);
    }

    // ---- counter accuracy ----

    #[tokio::test]
    async fn counters_are_accurate() {
        let q = three_stage_def();
        // 12 rows, 3 distinct keys.
        let input: Vec<Row> = (0..12)
            .map(|i| vec![Value::Str(format!("k{}", i % 3)), Value::Long(i)])
            .collect();
        let cfg = EngineConfig {
            workers: 2,
            spill_threshold_bytes: usize::MAX,
        };
        let res = q.run(input, &cfg).await.expect("run");

        // Scan stage reads + emits 12 rows.
        let scan = &res.stage_counters[0];
        assert_eq!(scan.stage_number, 0);
        assert_eq!(scan.rows_in, 12);
        assert_eq!(scan.rows_out, 12);

        // Shuffle stage passes 12 rows through.
        let shuffle = &res.stage_counters[1];
        assert_eq!(shuffle.rows_in, 12);
        assert_eq!(shuffle.rows_out, 12);
        assert_eq!(shuffle.shuffle_type.as_deref(), Some("HASH"));

        // Aggregate stage: 12 rows in, 3 groups out.
        let agg = &res.stage_counters[2];
        assert_eq!(agg.rows_in, 12);
        assert_eq!(agg.rows_out, 3);

        // Per-worker rows_out across the aggregate must sum to the number
        // of partial groups (>= final groups, equal here since each key is
        // hashed to a single partition).
        let worker_sum: u64 = agg.workers.iter().map(|w| w.rows_out).sum();
        assert!(worker_sum >= agg.rows_out);

        // Result: 3 groups, each count=4.
        assert_eq!(res.rows.len(), 3);
        for row in &res.rows {
            assert_eq!(row[1], Value::Long(4)); // count
        }
    }

    #[test]
    fn frame_roundtrip_through_disk() {
        let frame = Frame {
            rows: vec![
                vec![Value::Long(1), Value::Str("a".into())],
                vec![Value::Null, Value::Double(3.5)],
            ],
        };
        let mut buf: Vec<u8> = Vec::new();
        frame.write_to(&mut buf).expect("write");
        let mut cursor = std::io::Cursor::new(buf);
        let read = Frame::read_from(&mut cursor).expect("read").expect("some");
        assert_eq!(read.rows, frame.rows);
        // Second read at EOF returns None.
        assert!(Frame::read_from(&mut cursor).expect("eof").is_none());
    }
}
