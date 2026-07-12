// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Window query type — wraps a [`ScanQuery`] and applies one or more SQL
//! window functions (`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`,
//! `LEAD()`, `FIRST_VALUE(col)`, `LAST_VALUE(col)`, `SUM(col)`, `AVG(col)`,
//! `MIN(col)`, `MAX(col)`, `COUNT(*) | COUNT(col)`) over the scanned rows,
//! then optionally re-orders the result by the outer SQL `ORDER BY`.
//!
//! Wave 47-D §1: this is the native execution surface for SQL `OVER (...)`
//! clauses; previously the planner silently dropped the window column and
//! degraded the query to a bare scan.
//!
//! Wave 10 (Wave 47-D §1 extension): aggregator window functions now honour
//! an explicit `ROWS BETWEEN ... AND ...` frame on top of the partition's
//! ORDER BY, enabling running / sliding aggregates.  For ROW_NUMBER /
//! RANK / DENSE_RANK / LAG / LEAD the frame is ignored (Druid does the
//! same — these functions are defined per-row regardless of the frame).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use ferrodruid_common::error::Result;
use ferrodruid_segment::SegmentData;

use crate::context::QueryContext;
use crate::scan::{ScanQuery, ScanResult};

// ---------------------------------------------------------------------------
// Window function spec
// ---------------------------------------------------------------------------

/// Sort direction for an `ORDER BY` column inside a window spec or the
/// outer post-window sort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SortDirection {
    /// Ascending order.
    Ascending,
    /// Descending order.
    Descending,
}

/// One ordering key — a column name plus a direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowOrderBy {
    /// The column to order by.
    pub column: String,
    /// Sort direction.
    pub direction: SortDirection,
}

/// The kind of window function being applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum WindowFunctionKind {
    /// `ROW_NUMBER()` — 1-indexed sequence within the partition.
    RowNumber,
    /// `RANK()` — gap-ranking by `ORDER BY` keys.
    Rank,
    /// `DENSE_RANK()` — gap-free ranking by `ORDER BY` keys.
    DenseRank,
    /// `LAG(column, offset)` — value `offset` rows earlier in the partition.
    Lag {
        /// The column to look back into.
        column: String,
        /// Number of rows to look back (default 1).
        offset: usize,
    },
    /// `LEAD(column, offset)` — value `offset` rows later in the partition.
    Lead {
        /// The column to look forward into.
        column: String,
        /// Number of rows to look forward (default 1).
        offset: usize,
    },
    /// `SUM(column)` — sum of `column` over the frame (default = entire
    /// partition).
    Sum {
        /// The column to sum.
        column: String,
    },
    /// `AVG(column)` — arithmetic mean of `column` over the frame.
    Avg {
        /// The column to average.
        column: String,
    },
    /// `MIN(column)` — minimum of `column` over the frame.
    Min {
        /// The column to take the minimum of.
        column: String,
    },
    /// `MAX(column)` — maximum of `column` over the frame.
    Max {
        /// The column to take the maximum of.
        column: String,
    },
    /// `COUNT(*) | COUNT(column)` — row count over the frame.  For
    /// `COUNT(*)` `column` is `None` and every framed row counts.  For
    /// `COUNT(column)` only frame rows where `column` is non-null count.
    Count {
        /// The column being counted, or `None` for `COUNT(*)`.
        column: Option<String>,
    },
    /// `FIRST_VALUE(column)` — value of `column` at the first row of the
    /// frame (per the partition's `ORDER BY`).
    FirstValue {
        /// The column whose first-frame value is returned.
        column: String,
    },
    /// `LAST_VALUE(column)` — value of `column` at the last row of the
    /// frame.  With Druid's default frame (`UNBOUNDED PRECEDING AND
    /// CURRENT ROW`) this returns the current row's value; with
    /// `UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` it returns the
    /// partition's last row.
    LastValue {
        /// The column whose last-frame value is returned.
        column: String,
    },
    /// `NTH_VALUE(column, n)` — value of `column` at the `n`-th (1-based)
    /// row of the frame.  When the frame has fewer than `n` rows the
    /// result is NULL.  CL-4 / W1-D addition.
    NthValue {
        /// The column whose n-th-frame value is returned.
        column: String,
        /// 1-based position within the frame.
        n: usize,
    },
    /// `NTILE(tiles)` — bucket each row in the partition into one of
    /// `tiles` equally-sized tiles (1..=tiles).  Extra rows from a non-
    /// even partition are distributed to the lower-numbered tiles
    /// (Druid / PostgreSQL convention).  CL-4 / W1-D addition.
    Ntile {
        /// Number of tiles to split the partition into.
        tiles: usize,
    },
    /// `CUME_DIST()` — cumulative distribution: number of rows ≤ the
    /// current row by `ORDER BY` key divided by the partition row count.
    /// CL-4 / W1-D addition.
    CumeDist,
    /// `PERCENT_RANK()` — `(rank - 1) / (partition_rows - 1)`.  Returns
    /// `0.0` for a single-row partition.  CL-4 / W1-D addition.
    PercentRank,
}

/// Window frame mode.  Mirrors `crate::sql` parser-side enum but lives
/// here so the executor crate has no dependency on the SQL crate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WindowFrameMode {
    /// `ROWS` — physical row-based frame (the only mode the executor
    /// supports today).
    Rows,
    /// `RANGE` — logical value-based frame.  Only `RANGE BETWEEN
    /// UNBOUNDED PRECEDING AND ...` patterns reduce to a row frame in
    /// the executor; arbitrary value-range bounds are out of scope.
    Range,
}

/// One bound of a [`WindowFrame`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum WindowFrameBound {
    /// `UNBOUNDED PRECEDING`.
    UnboundedPreceding,
    /// `N PRECEDING` (N rows before the current row).
    Preceding {
        /// The number of rows.
        n: usize,
    },
    /// `CURRENT ROW`.
    CurrentRow,
    /// `N FOLLOWING` (N rows after the current row).
    Following {
        /// The number of rows.
        n: usize,
    },
    /// `UNBOUNDED FOLLOWING`.
    UnboundedFollowing,
}

/// Window frame specification: `<mode> BETWEEN <start> AND <end>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowFrame {
    /// `ROWS` or `RANGE`.
    pub mode: WindowFrameMode,
    /// Start bound.
    pub start: WindowFrameBound,
    /// End bound.
    pub end: WindowFrameBound,
}

/// One window function output column: the result of applying `function`
/// over rows that share `partition_by` keys, ordered by `order_by`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowSpec {
    /// Output column name (`AS rn`, `AS rk`, etc).
    pub output_name: String,
    /// The function to apply.
    pub function: WindowFunctionKind,
    /// `PARTITION BY` columns (empty = single global partition).
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// `ORDER BY` columns within the partition.
    #[serde(default)]
    pub order_by: Vec<WindowOrderBy>,
    /// Optional explicit `ROWS BETWEEN ... AND ...` frame.  When `None`,
    /// the function uses its natural default (entire partition for
    /// aggregators without ORDER BY, or the SQL-standard default per
    /// function semantics).
    #[serde(default)]
    pub frame: Option<WindowFrame>,
}

// ---------------------------------------------------------------------------
// Query spec
// ---------------------------------------------------------------------------

/// A Druid Window query — apply window functions on top of a scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowQuery {
    /// The inner scan that produces the input rows.
    pub inner: ScanQuery,
    /// Window functions to evaluate.  Each adds one output column.
    pub windows: Vec<WindowSpec>,
    /// Outer SQL `ORDER BY` clause applied *after* window evaluation,
    /// so that `RANK()` / `LAG()` are computed against the window's own
    /// `ORDER BY` and only the visible output is re-sorted for the user.
    #[serde(default)]
    pub post_order_by: Vec<WindowOrderBy>,
    /// Outer SQL `LIMIT`.
    #[serde(default)]
    pub post_limit: Option<usize>,
    /// Optional query context.
    #[serde(default)]
    pub context: Option<QueryContext>,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

impl WindowQuery {
    /// Execute this Window query against a segment.
    pub fn execute(&self, segment: &SegmentData) -> Result<ScanResult> {
        // 1. Run the inner scan to get the base rows.
        let base = self.inner.execute(segment)?;
        let mut rows = base.events;

        // 2. Compute and append every window column.
        for spec in &self.windows {
            apply_window(&mut rows, spec);
        }

        // 3. Apply outer SQL ORDER BY (post-window).
        if !self.post_order_by.is_empty() {
            rows.sort_by(|a, b| compare_rows(a, b, &self.post_order_by));
        }

        // 4. Apply outer LIMIT.
        if let Some(limit) = self.post_limit {
            rows.truncate(limit);
        }

        // 5. Build output column list = inner scan columns + window output
        //    names, deduplicated and order-preserving.
        let mut columns: Vec<String> = Vec::with_capacity(base.columns.len() + self.windows.len());
        for c in &base.columns {
            if !columns.iter().any(|x| x == c) {
                columns.push(c.clone());
            }
        }
        for spec in &self.windows {
            if !columns.iter().any(|x| x == &spec.output_name) {
                columns.push(spec.output_name.clone());
            }
        }

        Ok(ScanResult {
            segment_id: base.segment_id,
            columns,
            events: rows,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute one window function's output and write it into every row.
fn apply_window(rows: &mut [HashMap<String, serde_json::Value>], spec: &WindowSpec) {
    // Bucket row indices by partition key (preserving the per-partition
    // input order — important for ORDER BY ties so that the secondary
    // sort lands by encounter order, matching Druid).
    let mut partitions: HashMap<Vec<String>, Vec<usize>> = HashMap::new();
    let mut partition_order: Vec<Vec<String>> = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        let key: Vec<String> = spec
            .partition_by
            .iter()
            .map(|c| stringify(row.get(c)))
            .collect();
        if !partitions.contains_key(&key) {
            partition_order.push(key.clone());
        }
        partitions.entry(key).or_default().push(idx);
    }

    // Per-partition: sort by the window's ORDER BY, then compute and
    // assign the window value for every row in the sorted order.
    for key in &partition_order {
        let indices = match partitions.get_mut(key) {
            Some(v) => v,
            None => continue,
        };
        if !spec.order_by.is_empty() {
            // Sort indices by comparing the rows they refer to.
            let order_by = spec.order_by.clone();
            indices.sort_by(|a, b| compare_rows(&rows[*a], &rows[*b], &order_by));
        }

        compute_partition(rows, indices, spec);
    }
}

/// Compute window values for one already-sorted partition.
fn compute_partition(
    rows: &mut [HashMap<String, serde_json::Value>],
    indices: &[usize],
    spec: &WindowSpec,
) {
    match &spec.function {
        WindowFunctionKind::RowNumber => {
            for (i, &row_idx) in indices.iter().enumerate() {
                rows[row_idx].insert(spec.output_name.clone(), json_int((i + 1) as i64));
            }
        }
        WindowFunctionKind::Rank => {
            // Rank with gaps: each tie group keeps the first index.
            let mut last_rank: i64 = 0;
            let mut last_key: Option<Vec<String>> = None;
            for (i, &row_idx) in indices.iter().enumerate() {
                let key: Vec<String> = spec
                    .order_by
                    .iter()
                    .map(|o| stringify(rows[row_idx].get(&o.column)))
                    .collect();
                let rank = if last_key.as_ref() == Some(&key) {
                    last_rank
                } else {
                    last_key = Some(key);
                    last_rank = (i + 1) as i64;
                    last_rank
                };
                rows[row_idx].insert(spec.output_name.clone(), json_int(rank));
            }
        }
        WindowFunctionKind::DenseRank => {
            let mut current: i64 = 0;
            let mut last_key: Option<Vec<String>> = None;
            for &row_idx in indices {
                let key: Vec<String> = spec
                    .order_by
                    .iter()
                    .map(|o| stringify(rows[row_idx].get(&o.column)))
                    .collect();
                if last_key.as_ref() != Some(&key) {
                    current += 1;
                    last_key = Some(key);
                }
                rows[row_idx].insert(spec.output_name.clone(), json_int(current));
            }
        }
        WindowFunctionKind::Lag { column, offset } => {
            for (i, &row_idx) in indices.iter().enumerate() {
                let value = if i >= *offset {
                    let src_idx = indices[i - offset];
                    rows[src_idx]
                        .get(column)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                };
                rows[row_idx].insert(spec.output_name.clone(), value);
            }
        }
        WindowFunctionKind::Lead { column, offset } => {
            for (i, &row_idx) in indices.iter().enumerate() {
                let target = i + *offset;
                let value = if target < indices.len() {
                    let src_idx = indices[target];
                    rows[src_idx]
                        .get(column)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                };
                rows[row_idx].insert(spec.output_name.clone(), value);
            }
        }
        WindowFunctionKind::Sum { column } => {
            apply_aggregate_frame(rows, indices, spec, AggregateOp::Sum { column });
        }
        WindowFunctionKind::Avg { column } => {
            apply_aggregate_frame(rows, indices, spec, AggregateOp::Avg { column });
        }
        WindowFunctionKind::Min { column } => {
            apply_aggregate_frame(rows, indices, spec, AggregateOp::Min { column });
        }
        WindowFunctionKind::Max { column } => {
            apply_aggregate_frame(rows, indices, spec, AggregateOp::Max { column });
        }
        WindowFunctionKind::Count { column } => {
            apply_aggregate_frame(
                rows,
                indices,
                spec,
                AggregateOp::Count {
                    column: column.as_deref(),
                },
            );
        }
        WindowFunctionKind::FirstValue { column } => {
            for (i, &row_idx) in indices.iter().enumerate() {
                let (start, _end) = frame_bounds(spec, i, indices.len());
                let value = if start < indices.len() {
                    let src_idx = indices[start];
                    rows[src_idx]
                        .get(column)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                };
                rows[row_idx].insert(spec.output_name.clone(), value);
            }
        }
        WindowFunctionKind::NthValue { column, n } => {
            // CL-4 / W1-D: position within the current frame, 1-based.
            // When the frame has fewer than `n` rows the result is NULL,
            // matching Druid / SQL:2003 semantics.
            for (i, &row_idx) in indices.iter().enumerate() {
                let (start, end) = frame_bounds(spec, i, indices.len());
                let target = start.saturating_add(n - 1);
                let value = if target < end && target < indices.len() {
                    let src_idx = indices[target];
                    rows[src_idx]
                        .get(column)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                };
                rows[row_idx].insert(spec.output_name.clone(), value);
            }
        }
        WindowFunctionKind::Ntile { tiles } => {
            // CL-4 / W1-D: PostgreSQL / Druid convention — when the
            // partition does not divide evenly, the first
            // `total % tiles` tiles get one extra row.
            let total = indices.len();
            let tiles = (*tiles).max(1);
            let base = total / tiles;
            let extra = total % tiles;
            let mut row_cursor = 0usize;
            for tile_idx in 0..tiles {
                let this_tile = base + if tile_idx < extra { 1 } else { 0 };
                for _ in 0..this_tile {
                    if row_cursor >= total {
                        break;
                    }
                    let row_idx = indices[row_cursor];
                    rows[row_idx].insert(spec.output_name.clone(), json_int((tile_idx + 1) as i64));
                    row_cursor += 1;
                }
            }
        }
        WindowFunctionKind::CumeDist => {
            // CL-4 / W1-D: number of rows ≤ current by ORDER BY key,
            // divided by partition row count. Ties get the same value.
            let total = indices.len() as f64;
            if total == 0.0 {
                return;
            }
            // Pre-compute the rank-by-tie-group of each row in the
            // sorted partition.  For CUME_DIST the "rank" is the index of
            // the LAST row in the tie group (1-based).
            let mut last_key: Option<Vec<String>> = None;
            let mut group_start: usize = 0;
            let mut group_end: usize;
            let mut i = 0;
            while i < indices.len() {
                let key: Vec<String> = spec
                    .order_by
                    .iter()
                    .map(|o| stringify(rows[indices[i]].get(&o.column)))
                    .collect();
                if last_key.as_ref() != Some(&key) {
                    group_start = i;
                    last_key = Some(key.clone());
                }
                // find end of this group
                group_end = i;
                let mut j = i + 1;
                while j < indices.len() {
                    let next: Vec<String> = spec
                        .order_by
                        .iter()
                        .map(|o| stringify(rows[indices[j]].get(&o.column)))
                        .collect();
                    if next != key {
                        break;
                    }
                    group_end = j;
                    j += 1;
                }
                let dist = ((group_end + 1) as f64) / total;
                for &row_idx in &indices[group_start..=group_end] {
                    rows[row_idx].insert(spec.output_name.clone(), json_float(dist));
                }
                i = group_end + 1;
            }
        }
        WindowFunctionKind::PercentRank => {
            // CL-4 / W1-D: (rank - 1) / (partition_rows - 1) where rank
            // is the gap-style rank (the first ordinal of each tie group).
            // A single-row partition yields 0.0.
            let total = indices.len();
            if total == 0 {
                return;
            }
            let denom = if total > 1 { (total - 1) as f64 } else { 1.0 };
            let mut last_key: Option<Vec<String>> = None;
            let mut current_rank: i64 = 0;
            for (i, &row_idx) in indices.iter().enumerate() {
                let key: Vec<String> = spec
                    .order_by
                    .iter()
                    .map(|o| stringify(rows[row_idx].get(&o.column)))
                    .collect();
                if last_key.as_ref() != Some(&key) {
                    last_key = Some(key);
                    current_rank = (i + 1) as i64;
                }
                let pr = if total == 1 {
                    0.0
                } else {
                    ((current_rank - 1) as f64) / denom
                };
                rows[row_idx].insert(spec.output_name.clone(), json_float(pr));
            }
        }
        WindowFunctionKind::LastValue { column } => {
            for (i, &row_idx) in indices.iter().enumerate() {
                let (_start, end) = frame_bounds(spec, i, indices.len());
                let value = if end > 0 && end <= indices.len() {
                    let src_idx = indices[end - 1];
                    rows[src_idx]
                        .get(column)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                };
                rows[row_idx].insert(spec.output_name.clone(), value);
            }
        }
    }
}

/// What the framed-aggregate driver should compute on each frame.
enum AggregateOp<'a> {
    Sum { column: &'a str },
    Avg { column: &'a str },
    Min { column: &'a str },
    Max { column: &'a str },
    Count { column: Option<&'a str> },
}

/// Per-row driver for SUM/AVG/MIN/MAX/COUNT: compute the frame `[start,
/// end)` for each row of the partition and reduce the column values
/// over that slice.  When the spec has no explicit frame, the natural
/// default is "entire partition" — matching Druid's behaviour for an
/// aggregate without ORDER BY — so the value is identical for every
/// row of the partition.  When the spec carries a frame, the value is
/// recomputed per row.
fn apply_aggregate_frame(
    rows: &mut [HashMap<String, serde_json::Value>],
    indices: &[usize],
    spec: &WindowSpec,
    op: AggregateOp<'_>,
) {
    if spec.frame.is_none() {
        // Whole partition — compute once, broadcast.
        let value = reduce(rows, indices, &op);
        for &row_idx in indices {
            rows[row_idx].insert(spec.output_name.clone(), value.clone());
        }
        return;
    }
    for (i, &row_idx) in indices.iter().enumerate() {
        let (start, end) = frame_bounds(spec, i, indices.len());
        let frame_indices = if start < end {
            &indices[start..end]
        } else {
            &[][..]
        };
        let value = reduce(rows, frame_indices, &op);
        rows[row_idx].insert(spec.output_name.clone(), value);
    }
}

fn reduce(
    rows: &[HashMap<String, serde_json::Value>],
    frame: &[usize],
    op: &AggregateOp<'_>,
) -> serde_json::Value {
    match op {
        AggregateOp::Sum { column } => {
            let mut total: f64 = 0.0;
            let mut all_int = true;
            let mut saw_any = false;
            for &row_idx in frame {
                if let Some(v) = rows[row_idx].get(*column) {
                    if v.is_null() {
                        continue;
                    }
                    if let Some(n) = v.as_f64() {
                        total += n;
                        saw_any = true;
                    }
                    if v.as_i64().is_none() {
                        all_int = false;
                    }
                }
            }
            if !saw_any {
                serde_json::Value::Null
            } else if all_int && total.fract() == 0.0 && total.is_finite() {
                json_int(total as i64)
            } else {
                json_float(total)
            }
        }
        AggregateOp::Avg { column } => {
            let mut total: f64 = 0.0;
            let mut count: usize = 0;
            for &row_idx in frame {
                if let Some(v) = rows[row_idx].get(*column).and_then(|x| x.as_f64()) {
                    total += v;
                    count += 1;
                }
            }
            if count == 0 {
                serde_json::Value::Null
            } else {
                json_float(total / count as f64)
            }
        }
        AggregateOp::Min { column } => {
            let mut best: Option<f64> = None;
            let mut all_int = true;
            for &row_idx in frame {
                if let Some(v) = rows[row_idx].get(*column) {
                    if v.is_null() {
                        continue;
                    }
                    if let Some(n) = v.as_f64() {
                        best = Some(match best {
                            Some(b) if b <= n => b,
                            _ => n,
                        });
                    }
                    if v.as_i64().is_none() {
                        all_int = false;
                    }
                }
            }
            match best {
                None => serde_json::Value::Null,
                Some(n) if all_int && n.fract() == 0.0 && n.is_finite() => json_int(n as i64),
                Some(n) => json_float(n),
            }
        }
        AggregateOp::Max { column } => {
            let mut best: Option<f64> = None;
            let mut all_int = true;
            for &row_idx in frame {
                if let Some(v) = rows[row_idx].get(*column) {
                    if v.is_null() {
                        continue;
                    }
                    if let Some(n) = v.as_f64() {
                        best = Some(match best {
                            Some(b) if b >= n => b,
                            _ => n,
                        });
                    }
                    if v.as_i64().is_none() {
                        all_int = false;
                    }
                }
            }
            match best {
                None => serde_json::Value::Null,
                Some(n) if all_int && n.fract() == 0.0 && n.is_finite() => json_int(n as i64),
                Some(n) => json_float(n),
            }
        }
        AggregateOp::Count { column } => {
            let mut count: i64 = 0;
            for &row_idx in frame {
                match column {
                    None => count += 1,
                    Some(c) => {
                        if let Some(v) = rows[row_idx].get(*c)
                            && !v.is_null()
                        {
                            count += 1;
                        }
                    }
                }
            }
            json_int(count)
        }
    }
}

/// Resolve the row-index half-open window `[start, end)` for the row at
/// position `i` of an `n`-row partition, given the spec's frame.
///
/// When the spec has no explicit frame the result is `(0, n)` — matching
/// Druid's default for an aggregate without ORDER BY (entire partition).
/// For ROWS frames the bounds are clamped into `[0, n]`.  RANGE frames
/// are treated as ROWS frames here — sufficient for the
/// `UNBOUNDED PRECEDING AND CURRENT ROW` and full-partition defaults the
/// SQL planner emits today.
fn frame_bounds(spec: &WindowSpec, i: usize, n: usize) -> (usize, usize) {
    let Some(frame) = &spec.frame else {
        return (0, n);
    };
    let start = match &frame.start {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::Preceding { n: k } => i.saturating_sub(*k),
        WindowFrameBound::CurrentRow => i,
        WindowFrameBound::Following { n: k } => (i.saturating_add(*k)).min(n),
        WindowFrameBound::UnboundedFollowing => n,
    };
    let end_exclusive = match &frame.end {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::Preceding { n: k } => i.saturating_sub(*k).saturating_add(1).min(n),
        WindowFrameBound::CurrentRow => (i + 1).min(n),
        WindowFrameBound::Following { n: k } => (i.saturating_add(*k).saturating_add(1)).min(n),
        WindowFrameBound::UnboundedFollowing => n,
    };
    (start, end_exclusive)
}

fn json_int(n: i64) -> serde_json::Value {
    serde_json::Value::Number(serde_json::Number::from(n))
}

fn json_float(n: f64) -> serde_json::Value {
    serde_json::Number::from_f64(n)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

/// Stringify a JSON value for a partition or rank-tie key.  Numbers use
/// their JSON canonical form so `1` and `1.0` collide (matching Druid's
/// loose partition key semantics for the cases the harness exercises).
fn stringify(v: Option<&serde_json::Value>) -> String {
    match v {
        None | Some(serde_json::Value::Null) => "\u{0}null".to_string(),
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                format!("i:{i}")
            } else if let Some(f) = n.as_f64() {
                format!("f:{f}")
            } else {
                format!("n:{n}")
            }
        }
        Some(serde_json::Value::String(s)) => format!("s:{s}"),
        Some(serde_json::Value::Bool(b)) => format!("b:{b}"),
        Some(other) => other.to_string(),
    }
}

/// Compare two row maps by the given `order_by` keys.
pub(crate) fn compare_rows(
    a: &HashMap<String, serde_json::Value>,
    b: &HashMap<String, serde_json::Value>,
    order_by: &[WindowOrderBy],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for ob in order_by {
        let av = a.get(&ob.column);
        let bv = b.get(&ob.column);
        let mut ord = compare_json(av, bv);
        if ob.direction == SortDirection::Descending {
            ord = ord.reverse();
        }
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn compare_json(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    use serde_json::Value;
    use std::cmp::Ordering;
    // NULLs sort last (Druid default).
    match (a, b) {
        (None | Some(Value::Null), None | Some(Value::Null)) => Ordering::Equal,
        (None | Some(Value::Null), _) => Ordering::Greater,
        (_, None | Some(Value::Null)) => Ordering::Less,
        (Some(Value::Number(an)), Some(Value::Number(bn))) => {
            let af = an.as_f64().unwrap_or(0.0);
            let bf = bn.as_f64().unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap_or(Ordering::Equal)
        }
        (Some(Value::String(a)), Some(Value::String(b))) => a.cmp(b),
        (Some(Value::Bool(a)), Some(Value::Bool(b))) => a.cmp(b),
        (Some(a), Some(b)) => a.to_string().cmp(&b.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_bitmap::DruidBitmap;
    use ferrodruid_common::types::DataSource;
    use ferrodruid_dict::FrontCodedDictionary;
    use ferrodruid_segment::Interval;
    use ferrodruid_segment::column::{ColumnData, StringColumnData};

    /// 10-row wikipedia_compat-style segment matching the Wave 47-D harness:
    /// language ∈ {de, en, fr, it}, page strings, added integers.
    fn build_wiki_segment() -> SegmentData {
        // Rows in segment-insertion order
        // (language, page, added)
        let raw: Vec<(&str, &str, i64)> = vec![
            ("en", "Main_Page", 100),
            ("en", "Talk:Main_Page", 50),
            ("fr", "Accueil", 200),
            ("de", "Hauptseite", 150),
            ("en", "Main_Page", 75),
            ("en", "Main_Page", 120),
            ("en", "Portal:Current_events", 300),
            ("fr", "Accueil", 180),
            ("en", "Main_Page", 90),
            ("it", "Pagina_principale", 110),
        ];
        let timestamps: Vec<i64> = (0..raw.len() as i64)
            .map(|i| 1_700_000_000_000 + i)
            .collect();
        let added: Vec<i64> = raw.iter().map(|(_, _, a)| *a).collect();

        // Build a string dict for language.
        let mut langs: Vec<&str> = raw.iter().map(|(l, _, _)| *l).collect();
        langs.sort();
        langs.dedup();
        let lang_dict =
            FrontCodedDictionary::from_sorted(langs.iter().map(|s| s.to_string()).collect());
        let lang_encoded: Vec<u32> = raw
            .iter()
            .map(|(l, _, _)| langs.iter().position(|x| x == l).unwrap() as u32)
            .collect();
        let mut lang_bitmaps: Vec<DruidBitmap> =
            (0..langs.len()).map(|_| DruidBitmap::new()).collect();
        for (row_idx, ord) in lang_encoded.iter().enumerate() {
            lang_bitmaps[*ord as usize].insert(row_idx as u32);
        }
        let language = ColumnData::String(StringColumnData {
            dictionary: lang_dict,
            encoded_values: lang_encoded,
            bitmap_indexes: lang_bitmaps,
        });

        // Same for page.
        let mut pages: Vec<&str> = raw.iter().map(|(_, p, _)| *p).collect();
        pages.sort();
        pages.dedup();
        let page_dict =
            FrontCodedDictionary::from_sorted(pages.iter().map(|s| s.to_string()).collect());
        let page_encoded: Vec<u32> = raw
            .iter()
            .map(|(_, p, _)| pages.iter().position(|x| x == p).unwrap() as u32)
            .collect();
        let mut page_bitmaps: Vec<DruidBitmap> =
            (0..pages.len()).map(|_| DruidBitmap::new()).collect();
        for (row_idx, ord) in page_encoded.iter().enumerate() {
            page_bitmaps[*ord as usize].insert(row_idx as u32);
        }
        let page = ColumnData::String(StringColumnData {
            dictionary: page_dict,
            encoded_values: page_encoded,
            bitmap_indexes: page_bitmaps,
        });

        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(timestamps));
        columns.insert("language".to_string(), language);
        columns.insert("page".to_string(), page);
        columns.insert("added".to_string(), ColumnData::Long(added));

        SegmentData {
            version: 9,
            num_rows: raw.len(),
            interval: Interval {
                start_millis: 0,
                end_millis: 1_700_000_000_999,
            },
            dimensions: vec!["language".into(), "page".into()],
            metrics: vec!["added".into()],
            columns,
            time_sorted: false,
        }
    }

    fn base_scan() -> ScanQuery {
        ScanQuery {
            data_source: DataSource::Table {
                name: "wikipedia_compat".into(),
            },
            intervals: vec![],
            filter: None,
            virtual_columns: None,
            columns: Some(vec!["language".into(), "page".into(), "added".into()]),
            limit: None,
            offset: None,
            order: Some("none".into()),
            result_format: None,
            context: None,
        }
    }

    #[test]
    fn row_number_global() {
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: ScanQuery {
                columns: Some(vec!["page".into(), "added".into()]),
                ..base_scan()
            },
            windows: vec![WindowSpec {
                output_name: "rn".into(),
                function: WindowFunctionKind::RowNumber,
                partition_by: vec![],
                order_by: vec![
                    WindowOrderBy {
                        column: "added".into(),
                        direction: SortDirection::Descending,
                    },
                    WindowOrderBy {
                        column: "page".into(),
                        direction: SortDirection::Ascending,
                    },
                ],
                frame: None,
            }],
            post_order_by: vec![WindowOrderBy {
                column: "rn".into(),
                direction: SortDirection::Ascending,
            }],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        let rns: Vec<i64> = r
            .events
            .iter()
            .map(|e| e.get("rn").and_then(|v| v.as_i64()).unwrap_or(-1))
            .collect();
        assert_eq!(rns, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        // Top row should be added=300, page=Portal:Current_events.
        assert_eq!(r.events[0].get("added").and_then(|v| v.as_i64()), Some(300));
    }

    #[test]
    fn row_number_partition() {
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![WindowSpec {
                output_name: "rn".into(),
                function: WindowFunctionKind::RowNumber,
                partition_by: vec!["language".into()],
                order_by: vec![
                    WindowOrderBy {
                        column: "added".into(),
                        direction: SortDirection::Descending,
                    },
                    WindowOrderBy {
                        column: "page".into(),
                        direction: SortDirection::Ascending,
                    },
                ],
                frame: None,
            }],
            post_order_by: vec![
                WindowOrderBy {
                    column: "language".into(),
                    direction: SortDirection::Ascending,
                },
                WindowOrderBy {
                    column: "rn".into(),
                    direction: SortDirection::Ascending,
                },
            ],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        // First row: de/Hauptseite/rn=1
        assert_eq!(
            r.events[0].get("language").and_then(|v| v.as_str()),
            Some("de")
        );
        assert_eq!(r.events[0].get("rn").and_then(|v| v.as_i64()), Some(1));
        // Second row: en, Portal:Current_events, rn=1
        assert_eq!(
            r.events[1].get("language").and_then(|v| v.as_str()),
            Some("en")
        );
        assert_eq!(r.events[1].get("rn").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(
            r.events[1].get("page").and_then(|v| v.as_str()),
            Some("Portal:Current_events")
        );
        // Last row: it/Pagina_principale/rn=1
        assert_eq!(
            r.events
                .last()
                .unwrap()
                .get("language")
                .and_then(|v| v.as_str()),
            Some("it")
        );
    }

    #[test]
    fn rank_with_ties_skips() {
        // Build a small segment with a tie in the en partition.
        let timestamps = vec![1_i64, 2, 3, 4];
        let added = vec![100_i64, 100, 50, 75];
        let lang_dict = FrontCodedDictionary::from_sorted(vec!["en".to_string()]);
        let lang_encoded: Vec<u32> = vec![0, 0, 0, 0];
        let mut bm = DruidBitmap::new();
        for i in 0..4u32 {
            bm.insert(i);
        }
        let language = ColumnData::String(StringColumnData {
            dictionary: lang_dict,
            encoded_values: lang_encoded,
            bitmap_indexes: vec![bm],
        });
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(timestamps));
        columns.insert("language".to_string(), language);
        columns.insert("added".to_string(), ColumnData::Long(added));
        let segment = SegmentData {
            version: 9,
            num_rows: 4,
            interval: Interval {
                start_millis: 0,
                end_millis: 100,
            },
            dimensions: vec!["language".into()],
            metrics: vec!["added".into()],
            columns,
            time_sorted: false,
        };

        let q = WindowQuery {
            inner: ScanQuery {
                columns: Some(vec!["language".into(), "added".into()]),
                ..base_scan()
            },
            windows: vec![WindowSpec {
                output_name: "rk".into(),
                function: WindowFunctionKind::Rank,
                partition_by: vec!["language".into()],
                order_by: vec![WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Descending,
                }],
                frame: None,
            }],
            post_order_by: vec![WindowOrderBy {
                column: "rk".into(),
                direction: SortDirection::Ascending,
            }],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        let rks: Vec<i64> = r
            .events
            .iter()
            .map(|e| e.get("rk").and_then(|v| v.as_i64()).unwrap_or(-1))
            .collect();
        // 100, 100, 75, 50 -> ranks 1, 1, 3, 4
        assert_eq!(rks, vec![1, 1, 3, 4]);
    }

    #[test]
    fn dense_rank_no_skip() {
        let timestamps = vec![1_i64, 2, 3, 4];
        let added = vec![100_i64, 100, 50, 75];
        let lang_dict = FrontCodedDictionary::from_sorted(vec!["en".to_string()]);
        let lang_encoded: Vec<u32> = vec![0, 0, 0, 0];
        let mut bm = DruidBitmap::new();
        for i in 0..4u32 {
            bm.insert(i);
        }
        let language = ColumnData::String(StringColumnData {
            dictionary: lang_dict,
            encoded_values: lang_encoded,
            bitmap_indexes: vec![bm],
        });
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(timestamps));
        columns.insert("language".to_string(), language);
        columns.insert("added".to_string(), ColumnData::Long(added));
        let segment = SegmentData {
            version: 9,
            num_rows: 4,
            interval: Interval {
                start_millis: 0,
                end_millis: 100,
            },
            dimensions: vec!["language".into()],
            metrics: vec!["added".into()],
            columns,
            time_sorted: false,
        };

        let q = WindowQuery {
            inner: ScanQuery {
                columns: Some(vec!["language".into(), "added".into()]),
                ..base_scan()
            },
            windows: vec![WindowSpec {
                output_name: "dr".into(),
                function: WindowFunctionKind::DenseRank,
                partition_by: vec!["language".into()],
                order_by: vec![WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Descending,
                }],
                frame: None,
            }],
            post_order_by: vec![WindowOrderBy {
                column: "dr".into(),
                direction: SortDirection::Ascending,
            }],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        let drs: Vec<i64> = r
            .events
            .iter()
            .map(|e| e.get("dr").and_then(|v| v.as_i64()).unwrap_or(-1))
            .collect();
        // 100, 100, 75, 50 -> 1, 1, 2, 3
        assert_eq!(drs, vec![1, 1, 2, 3]);
    }

    #[test]
    fn lag_returns_null_at_partition_start() {
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![WindowSpec {
                output_name: "prev_added".into(),
                function: WindowFunctionKind::Lag {
                    column: "added".into(),
                    offset: 1,
                },
                partition_by: vec!["language".into()],
                order_by: vec![
                    WindowOrderBy {
                        column: "added".into(),
                        direction: SortDirection::Descending,
                    },
                    WindowOrderBy {
                        column: "page".into(),
                        direction: SortDirection::Ascending,
                    },
                ],
                frame: None,
            }],
            post_order_by: vec![
                WindowOrderBy {
                    column: "language".into(),
                    direction: SortDirection::Ascending,
                },
                WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Descending,
                },
                WindowOrderBy {
                    column: "page".into(),
                    direction: SortDirection::Ascending,
                },
            ],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        // First row in each partition (de, en, fr, it) has prev_added = NULL.
        assert!(
            r.events[0]
                .get("prev_added")
                .map(|v| v.is_null())
                .unwrap_or(false)
        );
        // en partition: 300, 120, 100, 90, 75, 50 → prev_added: null, 300, 120, 100, 90, 75
        let en_rows: Vec<&HashMap<String, serde_json::Value>> = r
            .events
            .iter()
            .filter(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
            .collect();
        assert_eq!(en_rows.len(), 6);
        assert!(
            en_rows[0]
                .get("prev_added")
                .map(|v| v.is_null())
                .unwrap_or(false)
        );
        assert_eq!(
            en_rows[1].get("prev_added").and_then(|v| v.as_i64()),
            Some(300)
        );
    }

    #[test]
    fn lead_returns_null_at_partition_end() {
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![WindowSpec {
                output_name: "next_added".into(),
                function: WindowFunctionKind::Lead {
                    column: "added".into(),
                    offset: 1,
                },
                partition_by: vec!["language".into()],
                order_by: vec![
                    WindowOrderBy {
                        column: "added".into(),
                        direction: SortDirection::Descending,
                    },
                    WindowOrderBy {
                        column: "page".into(),
                        direction: SortDirection::Ascending,
                    },
                ],
                frame: None,
            }],
            post_order_by: vec![
                WindowOrderBy {
                    column: "language".into(),
                    direction: SortDirection::Ascending,
                },
                WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Descending,
                },
                WindowOrderBy {
                    column: "page".into(),
                    direction: SortDirection::Ascending,
                },
            ],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        // de/it have only 1 row each → next_added should be null.
        let de_row = r
            .events
            .iter()
            .find(|e| e.get("language").and_then(|v| v.as_str()) == Some("de"))
            .expect("de row");
        assert!(
            de_row
                .get("next_added")
                .map(|v| v.is_null())
                .unwrap_or(false)
        );
        // en partition: descending added 300, 120, 100, 90, 75, 50.
        // next_added: 120, 100, 90, 75, 50, null.
        let en_rows: Vec<&HashMap<String, serde_json::Value>> = r
            .events
            .iter()
            .filter(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
            .collect();
        assert_eq!(
            en_rows[0].get("next_added").and_then(|v| v.as_i64()),
            Some(120)
        );
        assert!(
            en_rows[5]
                .get("next_added")
                .map(|v| v.is_null())
                .unwrap_or(false)
        );
    }

    #[test]
    fn sum_over_partition_constant() {
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![WindowSpec {
                output_name: "lang_total".into(),
                function: WindowFunctionKind::Sum {
                    column: "added".into(),
                },
                partition_by: vec!["language".into()],
                order_by: vec![],
                frame: None,
            }],
            post_order_by: vec![
                WindowOrderBy {
                    column: "language".into(),
                    direction: SortDirection::Ascending,
                },
                WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Descending,
                },
                WindowOrderBy {
                    column: "page".into(),
                    direction: SortDirection::Ascending,
                },
            ],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        let de_total = r
            .events
            .iter()
            .find(|e| e.get("language").and_then(|v| v.as_str()) == Some("de"))
            .and_then(|e| e.get("lang_total"))
            .and_then(|v| v.as_i64());
        assert_eq!(de_total, Some(150));
        let fr_total = r
            .events
            .iter()
            .find(|e| e.get("language").and_then(|v| v.as_str()) == Some("fr"))
            .and_then(|e| e.get("lang_total"))
            .and_then(|v| v.as_i64());
        assert_eq!(fr_total, Some(380));
        let en_total = r
            .events
            .iter()
            .find(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
            .and_then(|e| e.get("lang_total"))
            .and_then(|v| v.as_i64());
        assert_eq!(en_total, Some(735));
    }

    #[test]
    fn avg_over_partition_constant() {
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![WindowSpec {
                output_name: "lang_avg".into(),
                function: WindowFunctionKind::Avg {
                    column: "added".into(),
                },
                partition_by: vec!["language".into()],
                order_by: vec![],
                frame: None,
            }],
            post_order_by: vec![],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        let de_avg = r
            .events
            .iter()
            .find(|e| e.get("language").and_then(|v| v.as_str()) == Some("de"))
            .and_then(|e| e.get("lang_avg"))
            .and_then(|v| v.as_f64());
        assert!((de_avg.unwrap() - 150.0).abs() < 1e-9);
        let fr_avg = r
            .events
            .iter()
            .find(|e| e.get("language").and_then(|v| v.as_str()) == Some("fr"))
            .and_then(|e| e.get("lang_avg"))
            .and_then(|v| v.as_f64());
        assert!((fr_avg.unwrap() - 190.0).abs() < 1e-9);
        let en_avg = r
            .events
            .iter()
            .find(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
            .and_then(|e| e.get("lang_avg"))
            .and_then(|v| v.as_f64());
        assert!((en_avg.unwrap() - 122.5).abs() < 1e-9);
    }

    // -----------------------------------------------------------------
    // Wave 10 — additional aggregators + frame variations
    // -----------------------------------------------------------------

    #[test]
    fn min_max_count_over_partition() {
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![
                WindowSpec {
                    output_name: "lang_min".into(),
                    function: WindowFunctionKind::Min {
                        column: "added".into(),
                    },
                    partition_by: vec!["language".into()],
                    order_by: vec![],
                    frame: None,
                },
                WindowSpec {
                    output_name: "lang_max".into(),
                    function: WindowFunctionKind::Max {
                        column: "added".into(),
                    },
                    partition_by: vec!["language".into()],
                    order_by: vec![],
                    frame: None,
                },
                WindowSpec {
                    output_name: "lang_cnt".into(),
                    function: WindowFunctionKind::Count { column: None },
                    partition_by: vec!["language".into()],
                    order_by: vec![],
                    frame: None,
                },
            ],
            post_order_by: vec![],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");

        // en: added = {100, 50, 75, 120, 300, 90} → min=50, max=300, cnt=6
        let en = r
            .events
            .iter()
            .find(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
            .expect("en row");
        assert_eq!(en.get("lang_min").and_then(|v| v.as_i64()), Some(50));
        assert_eq!(en.get("lang_max").and_then(|v| v.as_i64()), Some(300));
        assert_eq!(en.get("lang_cnt").and_then(|v| v.as_i64()), Some(6));

        // fr: added = {200, 180} → min=180, max=200, cnt=2
        let fr = r
            .events
            .iter()
            .find(|e| e.get("language").and_then(|v| v.as_str()) == Some("fr"))
            .expect("fr row");
        assert_eq!(fr.get("lang_min").and_then(|v| v.as_i64()), Some(180));
        assert_eq!(fr.get("lang_max").and_then(|v| v.as_i64()), Some(200));
        assert_eq!(fr.get("lang_cnt").and_then(|v| v.as_i64()), Some(2));
    }

    #[test]
    fn first_value_and_last_value_over_partition() {
        // Default frame for FIRST_VALUE / LAST_VALUE in our executor is
        // "entire partition" when no explicit frame is provided.
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![
                WindowSpec {
                    output_name: "first_added".into(),
                    function: WindowFunctionKind::FirstValue {
                        column: "added".into(),
                    },
                    partition_by: vec!["language".into()],
                    order_by: vec![WindowOrderBy {
                        column: "added".into(),
                        direction: SortDirection::Descending,
                    }],
                    frame: None,
                },
                WindowSpec {
                    output_name: "last_added".into(),
                    function: WindowFunctionKind::LastValue {
                        column: "added".into(),
                    },
                    partition_by: vec!["language".into()],
                    order_by: vec![WindowOrderBy {
                        column: "added".into(),
                        direction: SortDirection::Descending,
                    }],
                    frame: None,
                },
            ],
            post_order_by: vec![],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        // en sorted desc: 300, 120, 100, 90, 75, 50 → first=300, last=50
        let en = r
            .events
            .iter()
            .find(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
            .expect("en");
        assert_eq!(en.get("first_added").and_then(|v| v.as_i64()), Some(300));
        assert_eq!(en.get("last_added").and_then(|v| v.as_i64()), Some(50));
    }

    #[test]
    fn sum_running_frame_unbounded_preceding_to_current_row() {
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![WindowSpec {
                output_name: "running_total".into(),
                function: WindowFunctionKind::Sum {
                    column: "added".into(),
                },
                partition_by: vec!["language".into()],
                order_by: vec![WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Ascending,
                }],
                frame: Some(WindowFrame {
                    mode: WindowFrameMode::Rows,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: WindowFrameBound::CurrentRow,
                }),
            }],
            post_order_by: vec![
                WindowOrderBy {
                    column: "language".into(),
                    direction: SortDirection::Ascending,
                },
                WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Ascending,
                },
            ],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");

        // en sorted asc: 50, 75, 90, 100, 120, 300
        // running_total: 50, 125, 215, 315, 435, 735
        let en_running: Vec<i64> = r
            .events
            .iter()
            .filter(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
            .map(|e| {
                e.get("running_total")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1)
            })
            .collect();
        assert_eq!(en_running, vec![50, 125, 215, 315, 435, 735]);
    }

    #[test]
    fn sum_sliding_frame_two_preceding_to_current_row() {
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![WindowSpec {
                output_name: "sliding_total".into(),
                function: WindowFunctionKind::Sum {
                    column: "added".into(),
                },
                partition_by: vec!["language".into()],
                order_by: vec![WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Ascending,
                }],
                frame: Some(WindowFrame {
                    mode: WindowFrameMode::Rows,
                    start: WindowFrameBound::Preceding { n: 2 },
                    end: WindowFrameBound::CurrentRow,
                }),
            }],
            post_order_by: vec![
                WindowOrderBy {
                    column: "language".into(),
                    direction: SortDirection::Ascending,
                },
                WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Ascending,
                },
            ],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");

        // en sorted asc: 50, 75, 90, 100, 120, 300
        // sliding 3-row sum (current + 2 prev):
        //   row0: 50
        //   row1: 50+75 = 125
        //   row2: 50+75+90 = 215
        //   row3: 75+90+100 = 265
        //   row4: 90+100+120 = 310
        //   row5: 100+120+300 = 520
        let en_sliding: Vec<i64> = r
            .events
            .iter()
            .filter(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
            .map(|e| {
                e.get("sliding_total")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1)
            })
            .collect();
        assert_eq!(en_sliding, vec![50, 125, 215, 265, 310, 520]);
    }

    #[test]
    fn last_value_full_partition_frame() {
        // With explicit `UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`,
        // LAST_VALUE returns the partition's truly-last row's value
        // (per the ORDER BY) — distinct from the SQL default frame
        // `UNBOUNDED PRECEDING AND CURRENT ROW`, which would return
        // the *current* row's value.
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![WindowSpec {
                output_name: "max_added".into(),
                function: WindowFunctionKind::LastValue {
                    column: "added".into(),
                },
                partition_by: vec!["language".into()],
                order_by: vec![WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Ascending,
                }],
                frame: Some(WindowFrame {
                    mode: WindowFrameMode::Rows,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: WindowFrameBound::UnboundedFollowing,
                }),
            }],
            post_order_by: vec![],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        // en sorted asc: 50, 75, 90, 100, 120, 300 → last = 300 for every row
        for row in r
            .events
            .iter()
            .filter(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
        {
            assert_eq!(row.get("max_added").and_then(|v| v.as_i64()), Some(300));
        }
    }

    #[test]
    fn count_col_skips_nulls_in_running_frame() {
        // Build a 4-row segment with one explicit Null `added` value
        // (encoded by simply not inserting a value for that row in the
        // long column won't work since long columns have no null mask;
        // instead we exercise COUNT(*) which always counts each frame
        // row, plus COUNT(col) which only counts non-nulls — both reduce
        // to the row count for this segment, so the assertion focuses on
        // the running-frame growth pattern rather than null handling).
        let timestamps = vec![1_i64, 2, 3, 4];
        let added = vec![10_i64, 20, 30, 40];
        let lang_dict = FrontCodedDictionary::from_sorted(vec!["en".to_string()]);
        let lang_encoded: Vec<u32> = vec![0, 0, 0, 0];
        let mut bm = DruidBitmap::new();
        for i in 0..4u32 {
            bm.insert(i);
        }
        let language = ColumnData::String(StringColumnData {
            dictionary: lang_dict,
            encoded_values: lang_encoded,
            bitmap_indexes: vec![bm],
        });
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(timestamps));
        columns.insert("language".to_string(), language);
        columns.insert("added".to_string(), ColumnData::Long(added));
        let segment = SegmentData {
            version: 9,
            num_rows: 4,
            interval: Interval {
                start_millis: 0,
                end_millis: 100,
            },
            dimensions: vec!["language".into()],
            metrics: vec!["added".into()],
            columns,
            time_sorted: false,
        };

        let q = WindowQuery {
            inner: ScanQuery {
                columns: Some(vec!["language".into(), "added".into()]),
                ..base_scan()
            },
            windows: vec![
                WindowSpec {
                    output_name: "rc".into(),
                    function: WindowFunctionKind::Count { column: None },
                    partition_by: vec!["language".into()],
                    order_by: vec![WindowOrderBy {
                        column: "added".into(),
                        direction: SortDirection::Ascending,
                    }],
                    frame: Some(WindowFrame {
                        mode: WindowFrameMode::Rows,
                        start: WindowFrameBound::UnboundedPreceding,
                        end: WindowFrameBound::CurrentRow,
                    }),
                },
                WindowSpec {
                    output_name: "cc".into(),
                    function: WindowFunctionKind::Count {
                        column: Some("added".into()),
                    },
                    partition_by: vec!["language".into()],
                    order_by: vec![WindowOrderBy {
                        column: "added".into(),
                        direction: SortDirection::Ascending,
                    }],
                    frame: Some(WindowFrame {
                        mode: WindowFrameMode::Rows,
                        start: WindowFrameBound::UnboundedPreceding,
                        end: WindowFrameBound::CurrentRow,
                    }),
                },
            ],
            post_order_by: vec![WindowOrderBy {
                column: "added".into(),
                direction: SortDirection::Ascending,
            }],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        let rc: Vec<i64> = r
            .events
            .iter()
            .map(|e| e.get("rc").and_then(|v| v.as_i64()).unwrap_or(-1))
            .collect();
        let cc: Vec<i64> = r
            .events
            .iter()
            .map(|e| e.get("cc").and_then(|v| v.as_i64()).unwrap_or(-1))
            .collect();
        assert_eq!(rc, vec![1, 2, 3, 4]);
        assert_eq!(cc, vec![1, 2, 3, 4]);
    }

    #[test]
    fn min_max_running_frame() {
        let segment = build_wiki_segment();
        let q = WindowQuery {
            inner: base_scan(),
            windows: vec![
                WindowSpec {
                    output_name: "running_min".into(),
                    function: WindowFunctionKind::Min {
                        column: "added".into(),
                    },
                    partition_by: vec!["language".into()],
                    order_by: vec![WindowOrderBy {
                        column: "added".into(),
                        direction: SortDirection::Ascending,
                    }],
                    frame: Some(WindowFrame {
                        mode: WindowFrameMode::Rows,
                        start: WindowFrameBound::UnboundedPreceding,
                        end: WindowFrameBound::CurrentRow,
                    }),
                },
                WindowSpec {
                    output_name: "running_max".into(),
                    function: WindowFunctionKind::Max {
                        column: "added".into(),
                    },
                    partition_by: vec!["language".into()],
                    order_by: vec![WindowOrderBy {
                        column: "added".into(),
                        direction: SortDirection::Ascending,
                    }],
                    frame: Some(WindowFrame {
                        mode: WindowFrameMode::Rows,
                        start: WindowFrameBound::UnboundedPreceding,
                        end: WindowFrameBound::CurrentRow,
                    }),
                },
            ],
            post_order_by: vec![
                WindowOrderBy {
                    column: "language".into(),
                    direction: SortDirection::Ascending,
                },
                WindowOrderBy {
                    column: "added".into(),
                    direction: SortDirection::Ascending,
                },
            ],
            post_limit: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        // en asc: 50, 75, 90, 100, 120, 300
        // running min: 50, 50, 50, 50, 50, 50
        // running max: 50, 75, 90, 100, 120, 300
        let en_min: Vec<i64> = r
            .events
            .iter()
            .filter(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
            .map(|e| e.get("running_min").and_then(|v| v.as_i64()).unwrap_or(-1))
            .collect();
        let en_max: Vec<i64> = r
            .events
            .iter()
            .filter(|e| e.get("language").and_then(|v| v.as_str()) == Some("en"))
            .map(|e| e.get("running_max").and_then(|v| v.as_i64()).unwrap_or(-1))
            .collect();
        assert_eq!(en_min, vec![50, 50, 50, 50, 50, 50]);
        assert_eq!(en_max, vec![50, 75, 90, 100, 120, 300]);
    }
}
