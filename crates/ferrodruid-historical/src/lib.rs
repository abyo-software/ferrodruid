// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Segment serving and query execution for FerroDruid.
//!
//! The [`Historical`] node loads segments, caches them locally, and executes
//! queries against them. It is the primary query-serving component.
//!
//! # Segment residency (FG-7)
//!
//! A Historical serves segments in one of two residency modes, chosen once at
//! construction and fixed for the node's lifetime:
//!
//! * **Heap mode** (the default) keeps every loaded segment's decoded
//!   [`SegmentData`] resident on the heap. `max_cache_bytes` bounds the total
//!   admitted payload weight and admission is **fail-closed**: a load that
//!   would exceed the limit is rejected. This is the long-standing behaviour
//!   and is byte-for-byte unchanged.
//! * **Spill mode** (opt-in, `FERRODRUID_SEGMENT_SPILL`) writes each loaded
//!   segment's bytes to this instance's private `spill/<pid>-<nonce>/` directory
//!   (via [`ferrodruid_segment::write_segment_v9`]) and keeps only a
//!   decode-on-demand handle in memory. A query re-decodes
//!   ([`SegmentData::open`]) a segment the first time it is touched and pins it
//!   in a **memory-budgeted LRU** of decoded segments; `max_cache_bytes` bounds
//!   the *LRU-pinned resident decoded* bytes and over-budget pressure is
//!   absorbed by **LRU eviction** (not admission rejection). The one exception:
//!   a single segment whose decoded weight exceeds the entire budget is **never
//!   pinned** — it is supplied query-local (re-decoded on every use, counted
//!   against nothing) so the resident ceiling holds exactly rather than being
//!   permanently blown by one oversized segment. This trades query latency for a
//!   flat, low memory ceiling independent of the number of loaded segments. No
//!   `mmap` is used — reload is an ordinary buffered read.
//!
//! Each Historical owns a **private** spill root under `cache_dir/spill/`,
//! pinned for the process's lifetime by an exclusive **advisory file lock** on a
//! `.lock` sentinel inside it. Two Historicals pointed at the same `cache_dir`
//! therefore never read or delete each other's spilled bytes. The startup
//! janitor reaps a peer's root only when it can itself take that root's
//! exclusive lock — proof that no live holder exists **anywhere**, including in
//! another PID namespace or under `hidepid` where `/proc` liveness is blind
//! (findings 3+4). It never parses or trusts process ids, and errs toward
//! keeping: any root whose lock it cannot take (or that has no sentinel) is left
//! untouched.
//!
//! The root is claimed by **staging-then-atomic-rename**: a transient
//! `spill/.staging-<nonce>/` directory is created, its `.lock` sentinel is taken
//! EXCLUSIVELY, and only then is the directory renamed to its final reapable
//! `spill/<pid>-<nonce>/` name. Because the exclusive `flock` is bound to the
//! open file description (not the path), it survives the rename, so the final
//! root is *born locked*. A peer janitor can therefore never observe a
//! final-named root that is not yet lock-pinned — the construction/sweep TOCTOU
//! (R4/R5 HIGH) is closed **structurally**, with no arbitration lock. The
//! dot-prefixed staging name is not of the final `<pid>-<nonce>` form, so it can
//! never collide with a published root; a crash mid-claim can leave one behind,
//! and the same janitor reaps it (its `.lock` is then free) like any other dead
//! root, so it is never an unbounded leak.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use fs4::FileExt;

use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_common::types::{ColumnType, DataSource};
use ferrodruid_query::scan::ScanResult;
use ferrodruid_query::{DruidQuery, QueryResult, execute_query};
use ferrodruid_segment::{Interval, SegmentData, check_null_generation, write_segment_v9};

// ---------------------------------------------------------------------------
// HistoricalInfo
// ---------------------------------------------------------------------------

/// Diagnostic information about a Historical node.
#[derive(Debug, Clone)]
pub struct HistoricalInfo {
    /// Number of loaded segments.
    pub segment_count: usize,
    /// Bytes currently counted against the cache limit.
    ///
    /// **Heap mode:** the exact, incrementally-maintained sum of each loaded
    /// segment's estimated heap plus its id/datasource string bytes — the
    /// quantity the cache limit bounds. It deliberately does NOT include the
    /// Historical's own index overhead (the two routing `HashMap` bucket
    /// tables), which is a small O(loaded segments) term of pointers/control
    /// bytes, negligible next to segment payload for realistic segment sizes.
    /// It is therefore a payload-weight bound, not a hard resident-memory
    /// total for the process.
    ///
    /// **Spill mode:** the currently-resident decoded byte weight (the sum of
    /// the LRU-pinned decoded segments' estimated heap), which the cache limit
    /// bounds by eviction rather than admission. Spilled-but-not-resident
    /// segments contribute nothing here.
    pub cache_bytes_used: u64,
    /// Maximum admitted segment-payload weight in bytes (heap mode) / maximum
    /// resident decoded bytes (spill mode) — the cache limit.
    pub cache_bytes_max: u64,
}

// ---------------------------------------------------------------------------
// SegmentSwapEntry
// ---------------------------------------------------------------------------

/// One segment in a [`Historical::replace_segments`] swap: its id, its
/// columnar data, and its datasource mapping for query routing.
///
/// The same shape is used for segments being added and for the removed
/// segments returned by the swap, so a caller can feed the returned
/// entries straight back into another [`Historical::replace_segments`]
/// call to roll a swap back verbatim.
#[derive(Clone)]
pub struct SegmentSwapEntry {
    /// Segment identifier.
    pub id: String,
    /// Shared columnar segment data.
    pub data: Arc<SegmentData>,
    /// Datasource mapping used for query routing. `None` means the segment
    /// is (or was) loaded without a mapping; such segments are excluded
    /// from table-datasource queries by the default-deny isolation rule.
    pub datasource: Option<String>,
}

// ---------------------------------------------------------------------------
// Historical
// ---------------------------------------------------------------------------

/// A Historical node that serves segments and executes queries.
///
/// Segments are loaded into memory and indexed by segment ID. The Historical
/// executes queries by iterating over loaded segments that match the query's
/// data source and intervals, delegating to `ferrodruid_query::execute_query`
/// for each matching segment, and collecting results.
pub struct Historical {
    /// Loaded segments indexed by segment ID.
    segments: Arc<RwLock<HashMap<String, SegmentEntry>>>,
    /// Local cache directory for segments.
    cache_dir: PathBuf,
    /// Maximum bytes to cache (heap payload weight, or resident decoded bytes
    /// in spill mode).
    max_cache_bytes: u64,
    /// Current cache size in bytes (heap mode ledger; stays `0` in spill mode,
    /// where residency is tracked by `resident_bytes`).
    current_cache_bytes: Arc<AtomicU64>,
    /// Reject segments whose null-generation cannot be confirmed modern.
    strict_null_generation: bool,
    /// Map of segment ID to data source name for efficient routing.
    segment_datasources: Arc<RwLock<HashMap<String, String>>>,
    /// Whether the initial-load sweep has completed and the node is ready
    /// to serve queries.  Used by `/status/health` readiness checks
    /// (Wave 36-B).  Newly constructed Historicals are marked ready
    /// immediately because there are no segments to load on startup; the
    /// flag exists so a future bootstrap that pre-loads from deep storage
    /// can flip it to `false` until the sweep completes.
    initial_load_complete: Arc<AtomicBool>,
    /// FG-7: when `true`, loaded segments are spilled to
    /// `cache_dir/spill/<pid>-<nonce>/` and decoded on demand under an LRU
    /// budget. When `false` (default) every segment stays heap-resident (the
    /// long-standing behaviour).
    spill_mode: bool,
    /// This instance's private spill root `cache_dir/spill/<pid>-<nonce>/`
    /// (finding 2). Every segment this Historical spills lives under here, so
    /// two Historicals that share a `cache_dir` occupy disjoint spill
    /// namespaces and can never open (or wipe) each other's bytes. Claimed with
    /// `create_dir` at construction; only used in spill mode.
    spill_instance_dir: PathBuf,
    /// Exclusive advisory lock on `spill_instance_dir/.lock`, held for this
    /// Historical's whole lifetime (findings 3+4). While this `File` is open the
    /// flock is held, so a peer's janitor — even across a PID namespace — sees
    /// this instance as live and never reaps its root; dropping the `Historical`
    /// closes the fd and releases the lock. `None` outside spill mode, or if no
    /// locked directory could be established at construction (best-effort
    /// degraded path); with no locked sentinel present a peer janitor keeps the
    /// root anyway. Never read directly; held purely for its lock/RAII effect.
    #[allow(dead_code)]
    spill_lock: Option<File>,
    /// Monotonic counter guaranteeing a unique spill directory per write,
    /// within this instance's private spill root.
    spill_counter: Arc<AtomicU64>,
    /// Memory-budgeted LRU of decoded segments (spill mode only).
    resident_lru: Arc<Mutex<ResidentLru>>,
    /// Sum of the LRU-resident decoded segments' estimated bytes (spill mode
    /// only). Read locklessly by [`Historical::info`]; mutated only under the
    /// `resident_lru` lock.
    resident_bytes: Arc<AtomicU64>,
    /// Count of real segment decodes ([`SegmentData::open`]) performed by the
    /// spill reload path. Used by tests to assert single-flight and
    /// interval-prune behaviour; a cheap observability counter otherwise.
    decode_count: Arc<AtomicU64>,
}

/// Where a loaded segment's decoded [`SegmentData`] currently lives.
enum Residency {
    /// Heap mode: the decoded segment is held resident for the segment's
    /// whole lifetime.
    Resident(Arc<SegmentData>),
    /// Spill mode: the segment's bytes live on disk under `dir`; `decoded`
    /// caches a [`Weak`] to the last decode so a still-resident (LRU-pinned)
    /// decode is reused instead of re-read. A dangling weak means the segment
    /// must be re-decoded from `dir`.
    Spilled {
        /// Directory holding the spilled `v9` segment bytes.
        dir: PathBuf,
        /// Weak handle to the most recent decode (single-flight guard).
        decoded: Mutex<Weak<SegmentData>>,
    },
}

/// One loaded segment: its residency plus the routing/accounting metadata that
/// stays in memory even when the decoded bytes are spilled.
struct SegmentEntry {
    /// Where the decoded data currently lives.
    residency: Residency,
    /// Estimated decoded heap size, computed once at load. Feeds both the heap
    /// cache ledger and the spill LRU budget.
    estimated_bytes: u64,
    /// Row count — kept resident (spill or heap) for routing / metadata so it
    /// is available without decoding a spilled segment.
    num_rows: usize,
    /// Interval used to skip this segment during query-time interval pruning,
    /// derived from the ACTUAL `__time` column min/max at load — NOT from the
    /// segment's (unverified) declared header interval (finding 1). `None`
    /// means the segment carries no `__time` values (0 rows / absent column):
    /// such a segment is excluded from pruning and always executes, so a lying
    /// or missing header can never cause a matching row to be dropped.
    prune_interval: Option<Interval>,
    /// The segment's column schema, captured once at load (while the decoded
    /// [`SegmentData`] is in hand). Schema discovery (INFORMATION_SCHEMA /
    /// `SELECT *`) unions these across a datasource's segments WITHOUT decoding
    /// any of them, so a spilled — or later externally-corrupted — segment can
    /// neither force a disk decode on the schema path nor break schema discovery.
    schema: CachedSchema,
}

impl SegmentEntry {
    /// Build a heap-resident entry, deriving the accounting metadata (and the
    /// cached column schema) from the decoded segment.
    ///
    /// Before the segment becomes query-visible on the heap its
    /// [`SegmentData::time_sorted`] flag is reconciled against the segment's
    /// ACTUAL `__time` values ([`derived_time_sorted`]). This is the ONLY
    /// path that installs a caller-supplied [`SegmentData`] into a query-visible
    /// heap [`Arc`] (spill mode never makes the caller's copy resident — it
    /// re-reads from disk with `time_sorted` re-derived), so reconciling here is
    /// what keeps a heap answer identical to the same segment's spilled answer
    /// (FG-7 R19/R21; see [`derived_time_sorted`]).
    ///
    /// `data` is NOT assumed uniquely owned. The direct-load paths hand in a
    /// freshly created `Arc::new(..)` (unique), but the replace path takes a
    /// swap-add entry's `Arc<SegmentData>` whose public field the caller may
    /// still hold a clone of — and a concurrent query, or a re-fed `removed`
    /// entry, is also a live second strong reference. `Arc::get_mut` returns
    /// `None` on any such shared `Arc`, so R19's in-place reconcile SILENTLY
    /// SKIPPED a shared segment and let a lying `time_sorted = true` reach the
    /// query executor (FG-7 R21 HIGH: heap binary-range-prunes a non-ascending
    /// segment and drops rows). We instead compute the correct flag first and,
    /// ONLY when it differs from what the caller supplied, write it through
    /// [`Arc::make_mut`] — which reconciles in place on a unique `Arc` and
    /// copy-on-writes a fresh private [`SegmentData`] on a shared one, leaving
    /// every other holder's copy untouched. A well-formed segment (the common
    /// case) already carries the correct flag, so the guarded write is skipped
    /// entirely: no clone even when the `Arc` is shared, and the heap segment
    /// stays byte-for-byte identical.
    fn resident(mut data: Arc<SegmentData>) -> Self {
        let correct = derived_time_sorted(&data);
        if data.time_sorted != correct {
            // Unique Arc → in place; shared Arc → copy-on-write a private copy
            // (only ever reached by a caller-installed lie, never a well-formed
            // segment, so the honest common path never clones).
            Arc::make_mut(&mut data).time_sorted = correct;
        }
        Self {
            estimated_bytes: estimate_segment_bytes(&data),
            num_rows: data.num_rows,
            prune_interval: derive_prune_interval(&data),
            schema: CachedSchema::from_segment(&data),
            residency: Residency::Resident(data),
        }
    }
}

/// The column schema of one loaded segment, captured at load time so schema
/// discovery needs no decode.
///
/// This is the exact information the REST schema paths derive per segment: the
/// ordered dimension and metric columns, each with the SQL/native [`ColumnType`]
/// resolved from the actual decoded [`ColumnData`](ferrodruid_segment::column::ColumnData)
/// variant (defaulting to `String` for a dimension — or `Double` for a metric —
/// absent from `columns`, matching the pre-cache decode-time schema build), plus
/// whether the segment carries a `__time` column. Caching it at load means
/// INFORMATION_SCHEMA / `SELECT *` can union a datasource's columns across all
/// its segments without materializing (and, in spill mode, without a disk
/// decode) any of them.
#[derive(Clone)]
struct CachedSchema {
    /// Whether the segment carries a `__time` column (surfaced as `TIMESTAMP`).
    has_time: bool,
    /// Ordered dimension columns with their resolved column type.
    dimensions: Vec<(String, ColumnType)>,
    /// Ordered metric columns with their resolved column type.
    metrics: Vec<(String, ColumnType)>,
}

impl CachedSchema {
    /// Capture the schema from a decoded segment at load time.
    ///
    /// `__time` is deliberately excluded from BOTH the dimension and metric
    /// vectors: it is the segment's time column, surfaced to consumers via the
    /// [`has_time`](Self::has_time) flag (and re-emitted exactly once, first, as
    /// a `TIMESTAMP`/`__time` column by every schema consumer). A well-formed
    /// segment never lists `__time` among its `dimensions`/`metrics`, but the
    /// public [`SegmentData`] fields are caller-mutable, so a defensively- (or
    /// adversarially-) constructed heap segment could carry `dimensions =
    /// ["__time"]` with a `columns["__time"]` entry. Without this filter that
    /// `__time` would flow into the dimension list AND be re-emitted via
    /// `has_time`, duplicating the `__time` column in `SELECT *` /
    /// INFORMATION_SCHEMA. Filtering it out here — the single source every
    /// consumer reads from — keeps `__time` a lone, once-only time column
    /// (the cross-role `seen` seeds in `union_cached_schemas` / the REST
    /// consumers are the belt-and-suspenders backstop).
    fn from_segment(data: &SegmentData) -> Self {
        let dimensions = data
            .dimensions
            .iter()
            .filter(|dim| dim.as_str() != "__time")
            .map(|dim| {
                let ct = data
                    .columns
                    .get(dim)
                    .map_or(ColumnType::String, column_type_of);
                (dim.clone(), ct)
            })
            .collect();
        let metrics = data
            .metrics
            .iter()
            .filter(|met| met.as_str() != "__time")
            .map(|met| {
                let ct = data
                    .columns
                    .get(met)
                    .map_or(ColumnType::Double, column_type_of);
                (met.clone(), ct)
            })
            .collect();
        Self {
            has_time: data.columns.contains_key("__time"),
            dimensions,
            metrics,
        }
    }

    /// Heap bytes this cached schema owns beyond the inline `size_of` folded into
    /// [`segment_entry_bytes`] (the two typed-column vectors' backing allocations,
    /// each owned column name's `String` capacity, and any `Complex` type name's
    /// `String` capacity). Charged so a segment with a wide schema cannot slip its
    /// real footprint past the cache quota; mirrors the `estimate_segment_bytes`
    /// pattern so the incremental ledger and the exactness oracle fold the same
    /// helper and stay symmetric by construction.
    fn schema_heap_bytes(&self) -> u64 {
        fn typed_vec_bytes(cols: &[(String, ColumnType)], capacity: usize) -> u64 {
            cols.iter().fold(
                allocation_bytes::<(String, ColumnType)>(capacity),
                |total, (name, ct)| {
                    total
                        .saturating_add(u64::try_from(name.capacity()).unwrap_or(u64::MAX))
                        .saturating_add(column_type_heap_bytes(ct))
                },
            )
        }
        typed_vec_bytes(&self.dimensions, self.dimensions.capacity())
            .saturating_add(typed_vec_bytes(&self.metrics, self.metrics.capacity()))
    }
}

/// The unioned column schema of a datasource across all its loaded segments,
/// produced WITHOUT decoding any segment — columns come from each segment's
/// load-time [`CachedSchema`].
///
/// This is the schema-path replacement for handing a datasource's raw
/// [`SegmentData`] back to the caller: because a datasource's columns are the
/// **union** of every one of its segments' columns (schema evolution — segment
/// `s1` may carry column `a` and `s2` column `b`), returning one representative
/// segment's columns would drop the others (a HashMap-order-dependent regression).
///
/// Column names are unique ACROSS the two vectors: a name appears in EXACTLY ONE
/// of [`Self::dimensions`] / [`Self::metrics`], never both. A name seen as a
/// dimension in one segment and a metric in another is resolved to a single role
/// (segment-id-order / dimension-before-metric first-wins) by
/// `union_cached_schemas`, so a consumer can treat the concatenation as a
/// `name → (role, type)` map with no duplicate keys.
pub struct DatasourceColumns {
    /// Whether ANY segment of the datasource carries a `__time` column.
    pub has_time: bool,
    /// Union of dimension columns across the datasource's segments, in a stable
    /// (segment-id-sorted, first-seen-wins) order that does not depend on
    /// HashMap iteration order. The first segment to introduce a name fixes its
    /// resolved type.
    pub dimensions: Vec<(String, ColumnType)>,
    /// Union of metric columns across the datasource's segments (same stable
    /// order / first-seen-type semantics as [`Self::dimensions`]).
    pub metrics: Vec<(String, ColumnType)>,
}

/// The role a column plays in a datasource's canonical schema, as reported by
/// the single [`DatasourceColumns::ordered_columns`] emit helper. Every schema
/// consumer (SQL `SELECT *` planning and `INFORMATION_SCHEMA` enumeration)
/// derives the SAME ordered column list from the SAME helper, so `__time`'s
/// presence and position cannot diverge between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnRole {
    /// The datasource's `__time` column (rendered as SQL `TIMESTAMP`). Emitted at
    /// most once and always FIRST, iff [`DatasourceColumns::has_time`] is true.
    Time,
    /// A dimension column.
    Dimension,
    /// A metric column.
    Metric,
}

impl DatasourceColumns {
    /// Merge several historicals' per-datasource column views for the SAME
    /// datasource into ONE aggregated view: [`Self::has_time`] is the OR of every
    /// part's flag, and the dimension/metric columns are unioned under a SINGLE
    /// cross-role dedup — the first part (in iterator order) to introduce a name,
    /// in EITHER role, fixes that column's role and type; every later same-name
    /// sighting (in either role) is dropped. `__time` is pre-seeded into the
    /// dedup as a backstop so a part that defensively carried `__time` among its
    /// dimensions/metrics never duplicates the time column. Parts are consumed in
    /// iterator order (the caller's deterministic `historicals` Vec order), so the
    /// merge is order-stable.
    ///
    /// This is the OUTER cross-historical seam shared by BOTH schema consumers
    /// (`build_schema_for` / `enumerate_datasources` in the REST crate), mirroring
    /// the WITHIN-historical [`union_cached_schemas`] rule. Feeding the result to
    /// [`Self::ordered_columns`] is what makes the two consumers agree on `__time`.
    #[must_use]
    pub fn merged<'a, I>(parts: I) -> Self
    where
        I: IntoIterator<Item = &'a DatasourceColumns>,
    {
        let mut has_time = false;
        let mut dimensions: Vec<(String, ColumnType)> = Vec::new();
        let mut metrics: Vec<(String, ColumnType)> = Vec::new();
        // ONE seen-set spanning BOTH roles (see `union_cached_schemas`), pre-
        // seeded with `__time` so it is never re-emitted as a dimension/metric.
        let mut seen: HashSet<String> = HashSet::new();
        seen.insert("__time".to_string());
        for part in parts {
            has_time |= part.has_time;
            for (name, ct) in &part.dimensions {
                if seen.insert(name.clone()) {
                    dimensions.push((name.clone(), ct.clone()));
                }
            }
            for (name, ct) in &part.metrics {
                if seen.insert(name.clone()) {
                    metrics.push((name.clone(), ct.clone()));
                }
            }
        }
        Self {
            has_time,
            dimensions,
            metrics,
        }
    }

    /// The datasource's canonical, deterministic ordered column list — the SINGLE
    /// source every schema consumer emits from: `__time` FIRST and exactly once
    /// iff [`Self::has_time`], then the deduped dimensions, then the deduped
    /// metrics, each as `(name, type, role)`. `__time` is a `LONG` /
    /// [`ColumnRole::Time`] (consumers render it as SQL `TIMESTAMP`).
    ///
    /// Routing BOTH the `SELECT *` schema (`build_schema_for`) and
    /// `INFORMATION_SCHEMA` enumeration (`enumerate_datasources`) through this one
    /// helper makes `__time`'s presence AND position structurally identical
    /// between them — they can no longer diverge: a time-less-only datasource
    /// (`has_time == false`) omits `__time` in BOTH, and a datasource with ANY
    /// timed segment leads with `__time` in BOTH regardless of segment/historical
    /// processing order.
    #[must_use]
    pub fn ordered_columns(&self) -> Vec<(String, ColumnType, ColumnRole)> {
        let mut out = Vec::with_capacity(
            usize::from(self.has_time) + self.dimensions.len() + self.metrics.len(),
        );
        if self.has_time {
            out.push(("__time".to_string(), ColumnType::Long, ColumnRole::Time));
        }
        for (name, ct) in &self.dimensions {
            out.push((name.clone(), ct.clone(), ColumnRole::Dimension));
        }
        for (name, ct) in &self.metrics {
            out.push((name.clone(), ct.clone(), ColumnRole::Metric));
        }
        out
    }
}

/// Map a decoded segment column to its SQL/native [`ColumnType`] for schema
/// caching (the historical-crate mirror of the REST `column_to_type`, kept here
/// so schema capture stays inside the historical without a REST dependency).
fn column_type_of(col: &ferrodruid_segment::column::ColumnData) -> ColumnType {
    use ferrodruid_segment::column::ColumnData;
    match col {
        // Nullable longs are LONG-typed to SQL/schema consumers (the null
        // bitmap is a storage detail, not a type change).
        ColumnData::Long(_) | ColumnData::LongNullable(_, _) => ColumnType::Long,
        ColumnData::Float(_) => ColumnType::Float,
        ColumnData::Double(_) => ColumnType::Double,
        // Multi-value string dimensions are STRING-typed to SQL/schema
        // consumers (the per-row list shape is a storage/query detail
        // surfaced via segmentMetadata `hasMultipleValues`).
        ColumnData::String(_) | ColumnData::StringMulti(_) => ColumnType::String,
        ColumnData::Complex(_) => ColumnType::Complex("opaque".to_string()),
        // Decoded per-row theta column (compat-8 sketch #2): schema
        // consumers see the concrete complex type name.
        ColumnData::ComplexTheta(_) => ColumnType::Complex("thetaSketch".to_string()),
    }
}

/// Heap owned by a [`ColumnType`] beyond its own inline `size_of`. Only the
/// `Complex` variant owns a heap `String` (its type name).
fn column_type_heap_bytes(ct: &ColumnType) -> u64 {
    match ct {
        ColumnType::Complex(name) => u64::try_from(name.capacity()).unwrap_or(u64::MAX),
        ColumnType::Long | ColumnType::Float | ColumnType::Double | ColumnType::String => 0,
    }
}

/// Union the cached schemas of a datasource's segments into a single
/// [`DatasourceColumns`], deterministically (segment-id order → HashMap-order
/// independent).
///
/// A column name is unique within a datasource, so the union yields EXACTLY ONE
/// entry per name — a name is a dimension XOR a metric, never both. The first
/// segment (in id order) to introduce a name, and within a segment the
/// dimension role ahead of the metric role, fixes both that column's role
/// (dimension vs. metric) and its resolved type; every later sighting of the
/// same name — in EITHER role — is dropped. This is why a single cross-role
/// `seen` set is used rather than separate dimension/metric sets: a name that is
/// a dimension in one segment and a metric in another would otherwise surface in
/// BOTH buckets, duplicating the column across INFORMATION_SCHEMA and the
/// `SELECT *` schema.
fn union_cached_schemas(mut members: Vec<(&str, &CachedSchema)>) -> DatasourceColumns {
    members.sort_by(|a, b| a.0.cmp(b.0));
    let mut has_time = false;
    let mut dimensions: Vec<(String, ColumnType)> = Vec::new();
    let mut metrics: Vec<(String, ColumnType)> = Vec::new();
    // ONE seen-set spanning BOTH roles: a column name occupies a single entry
    // (dimension XOR metric), so a name already claimed as a dimension cannot
    // reappear as a metric (or vice versa) elsewhere in the union.
    //
    // Pre-seed with `__time`: it is the time column (reported separately via
    // `has_time`), so should a member's `CachedSchema` ever carry `__time` in
    // its dimension/metric vectors (defensively-constructed segment — the
    // `CachedSchema::from_segment` filter is the primary guard, this is the
    // backstop), it is dropped here rather than duplicating the `__time` column
    // in the union a consumer emits alongside the `has_time` time column.
    let mut seen: HashSet<String> = HashSet::new();
    seen.insert("__time".to_string());
    for (_id, schema) in members {
        has_time |= schema.has_time;
        for (name, ct) in &schema.dimensions {
            if seen.insert(name.clone()) {
                dimensions.push((name.clone(), ct.clone()));
            }
        }
        for (name, ct) in &schema.metrics {
            if seen.insert(name.clone()) {
                metrics.push((name.clone(), ct.clone()));
            }
        }
    }
    DatasourceColumns {
        has_time,
        dimensions,
        metrics,
    }
}

/// Memory-budgeted LRU of decoded segments, used only in spill mode.
///
/// Keys are segment ids; the ordered `order` index (recency tick → id) lets
/// eviction pop the least-recently-used resident in `O(log n)`. All mutation
/// happens under the enclosing [`Mutex`], so `resident_bytes` (an external
/// atomic read locklessly by `info`) is only ever changed here.
struct ResidentLru {
    /// id → resident node.
    nodes: HashMap<String, LruNode>,
    /// recency tick → id, ascending (front = least recently used).
    order: BTreeMap<u64, String>,
    /// Monotonic recency counter.
    next_tick: u64,
}

/// One LRU-pinned decoded segment.
struct LruNode {
    /// Strong reference that keeps the decode resident. Never read directly —
    /// it exists purely so the [`Weak`] cached in the segment's
    /// [`Residency::Spilled`] handle keeps upgrading until this node is
    /// evicted. Dropping it (on eviction) frees the decode.
    #[allow(dead_code)]
    data: Arc<SegmentData>,
    /// Recency tick (its key in [`ResidentLru::order`]).
    tick: u64,
    /// Resident byte weight (the segment's `estimated_bytes`).
    bytes: u64,
}

impl ResidentLru {
    fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            order: BTreeMap::new(),
            next_tick: 0,
        }
    }

    /// Total resident byte weight (the sum of node weights). Test-only oracle
    /// for the `resident_bytes == Σ node.bytes` LRU-consistency invariant.
    #[cfg(test)]
    fn total_bytes(&self) -> u64 {
        self.nodes
            .values()
            .fold(0_u64, |total, node| total.saturating_add(node.bytes))
    }

    /// Whether `id` is currently LRU-resident. Test-only.
    #[cfg(test)]
    fn contains(&self, id: &str) -> bool {
        self.nodes.contains_key(id)
    }

    /// Move `id` to most-recently-used, if resident.
    fn touch(&mut self, id: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            self.order.remove(&node.tick);
            let tick = self.next_tick;
            self.next_tick = self.next_tick.wrapping_add(1);
            node.tick = tick;
            self.order.insert(tick, id.to_owned());
        }
    }

    /// Admit `data` (weight `bytes`) for `id`, first evicting the
    /// least-recently-used residents until the budget would hold.
    ///
    /// A segment whose own weight exceeds the whole budget (`bytes > budget`) is
    /// **never admitted** (finding 3): pinning it would hold `resident_bytes`
    /// permanently above the budget for the segment's whole loaded lifetime (the
    /// LRU's strong `Arc` keeps it alive), breaking the invariant
    /// "LRU-pinned `resident_bytes` ≤ budget". Such an oversized segment is
    /// instead supplied query-local by [`Historical::materialize`] — decoded on
    /// each use, counted against nothing, pinned by nothing — so the query still
    /// runs but the memory ceiling holds.
    fn admit(
        &mut self,
        id: String,
        data: Arc<SegmentData>,
        bytes: u64,
        resident_bytes: &AtomicU64,
        budget: u64,
    ) {
        if self.nodes.contains_key(&id) {
            // Defensive: a concurrent path already admitted it — just refresh.
            self.touch(&id);
            return;
        }
        if bytes > budget {
            // Oversized: do not pin (would exceed budget indefinitely). The
            // caller's returned `Arc` still serves this query (query-local).
            return;
        }
        while resident_bytes.load(Ordering::Relaxed).saturating_add(bytes) > budget {
            let Some((&victim_tick, _)) = self.order.iter().next() else {
                break;
            };
            let Some(victim_id) = self.order.remove(&victim_tick) else {
                break;
            };
            if let Some(victim) = self.nodes.remove(&victim_id) {
                resident_bytes.fetch_sub(victim.bytes, Ordering::Relaxed);
                // `victim.data` drops here; the heap is freed once any
                // in-flight query holding a clone finishes.
            }
        }
        let tick = self.next_tick;
        self.next_tick = self.next_tick.wrapping_add(1);
        self.order.insert(tick, id.clone());
        self.nodes.insert(id, LruNode { data, tick, bytes });
        resident_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Evict `id` if resident (drop/replace victim path).
    fn remove(&mut self, id: &str, resident_bytes: &AtomicU64) {
        if let Some(node) = self.nodes.remove(id) {
            self.order.remove(&node.tick);
            resident_bytes.fetch_sub(node.bytes, Ordering::Relaxed);
        }
    }
}

struct PreparedSwapEntry {
    id: String,
    data: Arc<SegmentData>,
    datasource: Option<(String, String)>,
    estimated_bytes: u64,
}

/// Per-loaded-segment heap charge: the estimated decoded payload, the segment
/// id's owned string bytes, and the fixed control overhead of one map entry.
///
/// The control overhead is charged as the REAL `size_of::<SegmentEntry>()`
/// (finding 5). The retired `2 * size_of::<usize>()` (16 B) constant modelled
/// the long-gone `LoadedSegment` shape; the current `SegmentEntry` also carries
/// the spill residency handle, row count, and a derived prune interval, so the
/// old constant undercounted every entry — enough that many tiny/empty heap
/// segments could slip real footprint past the cache quota. Both the
/// incremental ledger and the exactness oracle fold this one helper, so the
/// charge stays self-consistent by construction.
///
/// **Charging the real `size_of::<SegmentEntry>()` is intentional and correct
/// (FG-7, R7 finding 1 adjudicated won't-fix).** FG-7 grew `SegmentEntry` (it
/// now carries residency, `num_rows`, and a `prune_interval`), so its real
/// per-entry footprint is larger than the pre-FG-7 16 B constant. Reverting to
/// 16 B would re-introduce the undercount/OOM risk this is exactly the fix for.
/// The only observable effect is that heap admission reaches `max_cache_bytes` a
/// few dozen bytes-per-entry sooner — a faithful reflection of real memory, not
/// a behaviour change: the "heap byte-for-byte" invariant is about query
/// *answers*, which this does not touch. The threshold micro-shift matters only
/// under a pathological config (a tiny `max_cache_bytes` with a huge count of
/// near-empty segments); see `honest-limitations.md` FG-7.
///
/// `schema_bytes` is the entry's cached-schema heap
/// ([`CachedSchema::schema_heap_bytes`]) — the column-name / type vectors held
/// resident for decode-free schema discovery. It is charged here (not folded
/// into `estimated_bytes`, which drives the spill LRU budget) so the same
/// helper counts it on both the incremental-ledger and exactness-oracle sides.
fn segment_entry_bytes(id: &String, estimated_bytes: u64, schema_bytes: u64) -> u64 {
    u64::try_from(id.capacity())
        .unwrap_or(u64::MAX)
        .saturating_add(estimated_bytes)
        .saturating_add(schema_bytes)
        .saturating_add(u64::try_from(std::mem::size_of::<SegmentEntry>()).unwrap_or(u64::MAX))
}

fn datasource_entry_bytes(id: &String, datasource: &String) -> u64 {
    u64::try_from(id.capacity())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(datasource.capacity()).unwrap_or(u64::MAX))
}

fn cache_bytes_after_delta(current: u64, removed_entries: u64, added_entries: u64) -> u64 {
    current
        .saturating_sub(removed_entries)
        .saturating_add(added_entries)
}

impl Historical {
    /// Create a new Historical node with the given cache directory and size limit.
    ///
    /// The node starts in the `initial_load_complete = true` state because
    /// there are no segments to pre-load.  Callers that drive a deep-storage
    /// bootstrap should call [`Self::set_initial_load_complete`] with `false`
    /// before kicking it off and `true` after it finishes so that
    /// `/status/health` correctly reports `historical: false` while the
    /// pre-load is in flight.
    pub fn new(cache_dir: PathBuf, max_cache_bytes: u64) -> Self {
        Self::with_strict_null_generation(cache_dir, max_cache_bytes, false)
    }

    /// Create a Historical node with explicit null-generation enforcement.
    ///
    /// Residency defaults to **heap mode** (`spill_mode = false`) for backward
    /// compatibility; use [`Self::with_options`] to opt into spill mode.
    pub fn with_strict_null_generation(
        cache_dir: PathBuf,
        max_cache_bytes: u64,
        strict_null_generation: bool,
    ) -> Self {
        Self::with_options(cache_dir, max_cache_bytes, strict_null_generation, false)
    }

    /// Create a Historical node with explicit null-generation enforcement and
    /// residency mode (FG-7).
    ///
    /// When `spill_mode` is `true`, `max_cache_bytes` bounds the resident
    /// *decoded* bytes (LRU-evicted, not admission-rejected) and each loaded
    /// segment is written under this instance's private spill root
    /// `cache_dir/spill/<pid>-<nonce>/` and decoded on demand. The root is
    /// claimed with `create_dir` and pinned by an exclusive advisory lock held
    /// for the process's lifetime; a startup janitor reaps a peer root only when
    /// it can itself take that root's lock — proof no live holder exists
    /// anywhere, including across a PID namespace (findings 3+4). The root is
    /// published by renaming an already-`.lock`-held staging directory into
    /// place, so the janitor never observes — nor reaps — a final-named root that
    /// is not yet lock-pinned (R4/R5 HIGH). It never touches a live instance's
    /// directory,
    /// so two Historicals sharing a `cache_dir` do not clobber each other.
    /// Segments are not durable across a restart: the node is
    /// memory-resident-only until segments are re-loaded.
    #[must_use]
    pub fn with_options(
        cache_dir: PathBuf,
        max_cache_bytes: u64,
        strict_null_generation: bool,
        spill_mode: bool,
    ) -> Self {
        // Compute this instance's private spill root unconditionally so the
        // field is always meaningful; the filesystem is only touched in spill
        // mode.
        let spill_root = cache_dir.join("spill");
        let (spill_instance_dir, spill_lock) = if spill_mode {
            // Create the SHARED spill root (parent of every instance's root).
            if let Err(e) = std::fs::create_dir_all(&spill_root) {
                tracing::warn!(
                    spill_root = %spill_root.display(),
                    error = %e,
                    "spill janitor: failed to (re)create spill root (loads will retry)"
                );
            }
            // Claim THIS instance's private root with `create_dir` (exclusive
            // creation) and take its lifetime advisory lock (findings 3+4).
            let (dir, lock) = claim_instance_dir(&spill_root);
            // Reap orphaned instance roots whose advisory lock is FREE (their
            // owner is provably dead). A live peer's root (lock held, even in
            // another PID namespace) blocks the try-lock and is kept, as is this
            // instance's own root — the janitor errs toward keeping so it can
            // never wipe a peer's live bytes (findings 3+4).
            reap_dead_instance_dirs(&spill_root, &dir);
            (dir, lock)
        } else {
            // Not spill mode: a nominal path, never created or locked.
            (spill_root.join(new_spill_instance_name()), None)
        };
        Self {
            segments: Arc::new(RwLock::new(HashMap::new())),
            cache_dir,
            max_cache_bytes,
            current_cache_bytes: Arc::new(AtomicU64::new(0)),
            strict_null_generation,
            segment_datasources: Arc::new(RwLock::new(HashMap::new())),
            initial_load_complete: Arc::new(AtomicBool::new(true)),
            spill_mode,
            spill_instance_dir,
            spill_lock,
            spill_counter: Arc::new(AtomicU64::new(0)),
            resident_lru: Arc::new(Mutex::new(ResidentLru::new())),
            resident_bytes: Arc::new(AtomicU64::new(0)),
            decode_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns `true` once the initial-load sweep has completed.
    ///
    /// Used by readiness probes (`/status/health`) to refuse traffic while
    /// segments are still being warmed from deep storage on startup.
    #[must_use]
    pub fn is_initial_load_complete(&self) -> bool {
        self.initial_load_complete.load(Ordering::Acquire)
    }

    /// Mark the initial-load sweep as complete (`true`) or in-progress
    /// (`false`).  This is what flips the `historical` bit in the
    /// `/status/health` JSON envelope (Wave 36-B).
    pub fn set_initial_load_complete(&self, value: bool) {
        self.initial_load_complete.store(value, Ordering::Release);
    }

    /// Load a segment, associating it with the given segment ID.
    ///
    /// The segment is stored in memory and becomes available for query
    /// execution.
    ///
    /// **Fail-closed on id collision (Codex 2026-07-12 round-2 HIGH #4):**
    /// if a segment with the same ID is already loaded this returns an
    /// error instead of silently replacing it. Segment ids embed a
    /// millisecond-resolution version, so two tasks publishing over the
    /// same interval in the same millisecond used to collide here and one
    /// task's rows were silently discarded. Callers that genuinely intend
    /// an in-place refresh must say so explicitly by listing the id in
    /// [`replace_segments`]' `drop_ids`.
    ///
    /// [`replace_segments`]: Historical::replace_segments
    pub fn load_segment(&self, segment_id: &str, segment: SegmentData) -> Result<()> {
        if self.strict_null_generation {
            check_null_generation(segment_id, &segment, true)?;
        }
        if self.spill_mode {
            return self.load_spilled(segment_id, None, segment);
        }
        let id = segment_id.to_owned();
        let entry = SegmentEntry::resident(Arc::new(segment));
        let added_entries =
            segment_entry_bytes(&id, entry.estimated_bytes, entry.schema.schema_heap_bytes());

        let mut segments = self.segments.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment write lock: {e}"))
        })?;
        let _ds_map = self.segment_datasources.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire datasource write lock: {e}"))
        })?;

        if segments.contains_key(segment_id) {
            return Err(DruidError::Segment(format!(
                "segment id collision: '{segment_id}' is already loaded; refusing to \
                 silently overwrite it (use replace_segments with the id in drop_ids \
                 for an explicit in-place refresh)"
            )));
        }

        let current = self.current_cache_bytes.load(Ordering::Relaxed);
        let next = cache_bytes_after_delta(current, 0, added_entries);
        ensure_cache_limit(next, self.max_cache_bytes)?;

        let replaced = segments.insert(id, entry);
        debug_assert!(replaced.is_none());
        self.current_cache_bytes.store(next, Ordering::Relaxed);

        // Record data source association (we don't have it from SegmentData directly,
        // but the caller can set it via set_segment_datasource).
        Ok(())
    }

    /// Atomically load a segment and publish its datasource mapping.
    ///
    /// Null-generation and cache-limit checks complete before either map is
    /// mutated, so a failure cannot expose an unmapped or partially loaded
    /// segment.
    pub fn load_segment_with_datasource(
        &self,
        segment_id: &str,
        datasource: &str,
        segment: SegmentData,
    ) -> Result<()> {
        check_null_generation(datasource, &segment, self.strict_null_generation)?;
        if self.spill_mode {
            return self.load_spilled(segment_id, Some(datasource), segment);
        }
        let segment_key = segment_id.to_owned();
        let datasource_key = segment_id.to_owned();
        let datasource_value = datasource.to_owned();
        let entry = SegmentEntry::resident(Arc::new(segment));
        let added_entries = segment_entry_bytes(
            &segment_key,
            entry.estimated_bytes,
            entry.schema.schema_heap_bytes(),
        )
        .saturating_add(datasource_entry_bytes(&datasource_key, &datasource_value));
        let mut segments = self.segments.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment write lock: {e}"))
        })?;
        let mut ds_map = self.segment_datasources.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire datasource write lock: {e}"))
        })?;
        if segments.contains_key(segment_id) {
            return Err(DruidError::Segment(format!(
                "segment id collision: '{segment_id}' is already loaded"
            )));
        }
        let current = self.current_cache_bytes.load(Ordering::Relaxed);
        let next = cache_bytes_after_delta(current, 0, added_entries);
        ensure_cache_limit(next, self.max_cache_bytes)?;

        let replaced_segment = segments.insert(segment_key, entry);
        let replaced_datasource = ds_map.insert(datasource_key, datasource_value);
        debug_assert!(replaced_segment.is_none());
        debug_assert!(replaced_datasource.is_none());
        self.current_cache_bytes.store(next, Ordering::Relaxed);
        Ok(())
    }

    /// Associate a segment ID with a data source name for query routing.
    ///
    /// This should be called after `load_segment` to enable data-source-aware
    /// query routing.
    pub fn set_segment_datasource(&self, segment_id: &str, datasource: &str) -> Result<()> {
        if self.spill_mode {
            return self.set_segment_datasource_spilled(segment_id, datasource);
        }
        let segments = self.segments.read().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment read lock: {e}"))
        })?;
        let loaded = segments.get(segment_id).ok_or_else(|| {
            DruidError::Segment(format!(
                "cannot map datasource for unloaded segment: {segment_id}"
            ))
        })?;
        let Residency::Resident(loaded_data) = &loaded.residency else {
            return Err(DruidError::Internal(
                "heap-mode segment is unexpectedly spilled".to_owned(),
            ));
        };
        check_null_generation(
            datasource,
            loaded_data.as_ref(),
            self.strict_null_generation,
        )?;
        let mut ds_map = self.segment_datasources.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire datasource write lock: {e}"))
        })?;
        let new_key = segment_id.to_owned();
        let new_value = datasource.to_owned();
        let (removed_entries, added_entries) =
            if let Some((old_key, old_value)) = ds_map.get_key_value(segment_id) {
                (
                    datasource_entry_bytes(old_key, old_value),
                    datasource_entry_bytes(old_key, &new_value),
                )
            } else {
                (0, datasource_entry_bytes(&new_key, &new_value))
            };
        let current = self.current_cache_bytes.load(Ordering::Relaxed);
        let next = cache_bytes_after_delta(current, removed_entries, added_entries);
        ensure_cache_limit(next, self.max_cache_bytes)?;

        ds_map.insert(new_key, new_value);
        self.current_cache_bytes.store(next, Ordering::Relaxed);
        Ok(())
    }

    /// Drop a loaded segment by ID.
    ///
    /// Returns an error if the segment is not currently loaded.
    pub fn drop_segment(&self, segment_id: &str) -> Result<()> {
        if self.spill_mode {
            return self.drop_spilled(segment_id);
        }
        let mut segments = self.segments.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment write lock: {e}"))
        })?;
        let mut ds_map = self.segment_datasources.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire datasource write lock: {e}"))
        })?;
        let (stored_segment_id, loaded) = segments
            .get_key_value(segment_id)
            .ok_or_else(|| DruidError::Query(format!("segment not loaded: {segment_id}")))?;
        let mut removed_entries = segment_entry_bytes(
            stored_segment_id,
            loaded.estimated_bytes,
            loaded.schema.schema_heap_bytes(),
        );
        if let Some((stored_datasource_id, datasource)) = ds_map.get_key_value(segment_id) {
            removed_entries = removed_entries
                .saturating_add(datasource_entry_bytes(stored_datasource_id, datasource));
        }

        let current = self.current_cache_bytes.load(Ordering::Relaxed);
        let next = cache_bytes_after_delta(current, removed_entries, 0);
        ensure_cache_limit(next, self.max_cache_bytes)?;

        let removed_segment = segments.remove(segment_id);
        let _removed_datasource = ds_map.remove(segment_id);
        debug_assert!(removed_segment.is_some());
        if segments.is_empty() {
            segments.shrink_to_fit();
        }
        if ds_map.is_empty() {
            ds_map.shrink_to_fit();
        }
        self.current_cache_bytes.store(next, Ordering::Relaxed);

        Ok(())
    }

    /// Atomically swap loaded segments: remove every id in `drop_ids` and
    /// insert every entry in `add`, all under a **single** acquisition of
    /// the segment-map and datasource-map write locks.
    ///
    /// [`execute_query`] holds the segment-map read lock for its entire
    /// run, so a concurrent query observes either the complete pre-swap
    /// segment set or the complete post-swap set — never a partial mix.
    /// This is what makes a multi-victim batch replace atomic to queries
    /// (Codex 2026-07-12 HIGH finding #3): dropping victims and loading
    /// the replacement through separate [`drop_segment`] /
    /// [`load_segment`] calls opens a window in which only some of the
    /// victims are gone.
    ///
    /// Ids in `drop_ids` that are not currently loaded are skipped rather
    /// than treated as an error (the replace path tolerates a metadata row
    /// whose segment is not resident, e.g. after a restart).
    ///
    /// **Returned removed-entries contract (scope, finding 2).** The removed
    /// entries exist to roll a **swap** back: when `add` is non-empty, every
    /// removed victim is returned with its real `data` and datasource mapping in
    /// BOTH residency modes, so a caller can feed them straight into a
    /// compensating `replace_segments` to restore the pre-swap state verbatim.
    /// The one deliberate divergence is a **pure drop** (`add` empty):
    /// * **Heap mode** returns each dropped victim data-bearing (its
    ///   already-resident `Arc<SegmentData>` is free to hand back).
    /// * **Spill mode** returns an **empty** vector. A `SegmentSwapEntry`
    ///   requires a full `Arc<SegmentData>`, which for a spilled victim is only
    ///   recoverable by a *fallible* disk decode; the drop-only-decrease
    ///   invariant (R30) forbids failing a drop on a corrupt/unreadable victim,
    ///   so the pure-drop path decodes nothing and returns nothing. This is safe
    ///   because a pure drop is only ever a rollback-free cleanup (streaming
    ///   respawn / unused-row prune) whose sole caller discards the return value
    ///   — nothing is rolled back, so no removed data is needed. A pure drop
    ///   that DOES need its victims returned must run in heap mode.
    ///
    /// **Fail-closed on id collision (Codex 2026-07-12 round-2 HIGH #4):**
    /// an `add` entry whose id is already loaded but NOT listed in
    /// `drop_ids` — or that duplicates another `add` entry — is a
    /// segment-id collision (e.g. two same-millisecond publications over
    /// the same interval): the swap fails without mutating anything
    /// instead of silently discarding the previously loaded rows. Listing
    /// the id in `drop_ids` remains the explicit way to refresh a segment
    /// in place.
    ///
    /// Cache accounting applies the swap's net byte delta atomically and
    /// rejects a swap that would exceed the configured cache limit.
    ///
    /// # Errors
    ///
    /// Fails on an undeclared id collision (above) or when a lock is
    /// poisoned; all validation happens after both locks are acquired and
    /// before anything is mutated, so a failure leaves the maps untouched.
    ///
    /// [`execute_query`]: Historical::execute_query
    /// [`drop_segment`]: Historical::drop_segment
    /// [`load_segment`]: Historical::load_segment
    pub fn replace_segments(
        &self,
        drop_ids: &[String],
        add: Vec<SegmentSwapEntry>,
    ) -> Result<Vec<SegmentSwapEntry>> {
        if self.spill_mode {
            return self.replace_spilled(drop_ids, add);
        }
        for entry in &add {
            if let Some(datasource) = entry.datasource.as_deref() {
                check_null_generation(
                    datasource,
                    entry.data.as_ref(),
                    self.strict_null_generation,
                )?;
            } else if self.strict_null_generation {
                check_null_generation(&entry.id, entry.data.as_ref(), true)?;
            }
        }
        let add: Vec<PreparedSwapEntry> = add
            .into_iter()
            .map(|entry| PreparedSwapEntry {
                estimated_bytes: estimate_segment_bytes(&entry.data),
                datasource: entry
                    .datasource
                    .map(|datasource| (entry.id.clone(), datasource)),
                id: entry.id,
                data: entry.data,
            })
            .collect();
        // Acquire BOTH locks before mutating anything (same order as
        // `execute_query`: segments, then datasources) so the swap is
        // all-or-nothing even if the second acquisition fails.
        let mut segments = self.segments.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment write lock: {e}"))
        })?;
        let mut ds_map = self.segment_datasources.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire datasource write lock: {e}"))
        })?;

        // Validate BEFORE mutating: every added id must be fresh, or its
        // overwrite must be explicitly declared via drop_ids.
        let mut drop_set = std::collections::HashSet::with_capacity(drop_ids.len());
        for id in drop_ids {
            drop_set.insert(id.as_str());
        }
        {
            let mut seen_add_ids = std::collections::HashSet::with_capacity(add.len());
            for entry in &add {
                if !seen_add_ids.insert(entry.id.as_str()) {
                    return Err(DruidError::Segment(format!(
                        "segment id collision: '{}' appears more than once in one \
                         replace_segments call; refusing the ambiguous swap",
                        entry.id
                    )));
                }
                if segments.contains_key(&entry.id) && !drop_set.contains(entry.id.as_str()) {
                    return Err(DruidError::Segment(format!(
                        "segment id collision: '{}' is already loaded and not listed for \
                         removal; refusing to silently overwrite it (two publications in \
                         the same millisecond over the same interval collide here — the \
                         previously loaded rows would be discarded)",
                        entry.id
                    )));
                }
            }
        }

        let mut removed_entries = 0_u64;
        for id in &drop_set {
            if let Some((stored_id, loaded)) = segments.get_key_value(*id) {
                removed_entries = removed_entries.saturating_add(segment_entry_bytes(
                    stored_id,
                    loaded.estimated_bytes,
                    loaded.schema.schema_heap_bytes(),
                ));
                if let Some((stored_ds_id, datasource)) = ds_map.get_key_value(*id) {
                    removed_entries = removed_entries
                        .saturating_add(datasource_entry_bytes(stored_ds_id, datasource));
                }
            }
        }

        let mut added_entries = 0_u64;
        for entry in &add {
            // The cached schema stored for this add is a deterministic function
            // of `entry.data` (`SegmentEntry::resident` recomputes the identical
            // vectors from the same segment below), so charging its heap here
            // stays exactly symmetric with the exactness oracle.
            added_entries = added_entries.saturating_add(segment_entry_bytes(
                &entry.id,
                entry.estimated_bytes,
                CachedSchema::from_segment(&entry.data).schema_heap_bytes(),
            ));

            let segment_is_removed =
                drop_set.contains(entry.id.as_str()) && segments.contains_key(&entry.id);
            let existing_datasource = if segment_is_removed {
                None
            } else {
                ds_map.get_key_value(&entry.id)
            };
            match (&entry.datasource, existing_datasource) {
                (Some((_new_key, new_value)), Some((old_key, old_value))) => {
                    removed_entries =
                        removed_entries.saturating_add(datasource_entry_bytes(old_key, old_value));
                    added_entries =
                        added_entries.saturating_add(datasource_entry_bytes(old_key, new_value));
                }
                (Some((new_key, new_value)), None) => {
                    added_entries =
                        added_entries.saturating_add(datasource_entry_bytes(new_key, new_value));
                }
                (None, Some((old_key, old_value))) => {
                    removed_entries =
                        removed_entries.saturating_add(datasource_entry_bytes(old_key, old_value));
                }
                (None, None) => {}
            }
        }

        let current = self.current_cache_bytes.load(Ordering::Relaxed);
        let next = cache_bytes_after_delta(current, removed_entries, added_entries);
        ensure_cache_limit(next, self.max_cache_bytes)?;

        let mut removed = Vec::with_capacity(drop_ids.len());
        for id in drop_ids {
            if let Some(loaded) = segments.remove(id) {
                let Residency::Resident(data) = loaded.residency else {
                    return Err(DruidError::Internal(
                        "heap-mode segment is unexpectedly spilled".to_owned(),
                    ));
                };
                removed.push(SegmentSwapEntry {
                    id: id.clone(),
                    data,
                    datasource: ds_map.remove(id),
                });
            }
        }
        for entry in add {
            match entry.datasource {
                Some((datasource_key, datasource)) => {
                    ds_map.insert(datasource_key, datasource);
                }
                None => {
                    ds_map.remove(&entry.id);
                }
            }
            segments.insert(entry.id, SegmentEntry::resident(entry.data));
        }
        if segments.is_empty() {
            segments.shrink_to_fit();
        }
        if ds_map.is_empty() {
            ds_map.shrink_to_fit();
        }
        self.current_cache_bytes.store(next, Ordering::Relaxed);
        Ok(removed)
    }

    /// Execute a query against all matching loaded segments.
    ///
    /// Segments are matched by data source name (if the query specifies a table
    /// data source). All matching segments are queried, and results are
    /// collected into a `Vec<QueryResult>`.
    ///
    /// **Cross-datasource isolation (Wave 36-D / R1, `lib.rs:152`):** when a
    /// query targets a specific table datasource, only segments with an
    /// explicit `set_segment_datasource` mapping that matches the target are
    /// queried. Unmapped segments are *excluded* (default-deny) — the previous
    /// "conservative include" behavior allowed a segment loaded for DS-A to
    /// silently leak rows into a query for DS-B if the caller forgot to
    /// register the mapping or hit the publish-time race window described in
    /// Wave 35 R1 finding `historical/src/lib.rs:152`.
    pub fn execute_query(&self, query: &DruidQuery) -> Result<Vec<QueryResult>> {
        let segments = self.segments.read().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment read lock: {e}"))
        })?;
        let ds_map = self.segment_datasources.read().map_err(|e| {
            DruidError::Internal(format!("failed to acquire datasource read lock: {e}"))
        })?;
        self.execute_query_on_snapshot(query, &segments, &ds_map)
    }

    fn execute_query_on_snapshot(
        &self,
        query: &DruidQuery,
        segments: &HashMap<String, SegmentEntry>,
        ds_map: &HashMap<String, String>,
    ) -> Result<Vec<QueryResult>> {
        if let DruidQuery::UnionAll(queries) = query {
            let mut results = Vec::new();
            for branch in queries {
                let DruidQuery::Scan(scan) = branch else {
                    return Err(DruidError::Query(
                        "UNION ALL is only supported over direct scan branches".to_owned(),
                    ));
                };
                // Druid treats an omitted scan order as `none`, so the
                // planner's default (`order: None`) and an explicit
                // `"none"` are equivalent here.
                if scan.order.as_deref().unwrap_or("none") != "none"
                    || scan.limit.is_some()
                    || scan.offset.unwrap_or(0) != 0
                {
                    return Err(DruidError::Query(
                        "UNION ALL currently requires unordered, unbounded scan branches \
                         (order must be `none`; limit and non-zero offset are unsupported)"
                            .to_owned(),
                    ));
                }
                // Each branch prunes/materializes independently through the
                // same read-lock snapshot.
                let branch_results = self.execute_query_on_snapshot(branch, segments, ds_map)?;
                results.push(merge_scan_branch(branch, branch_results)?);
            }
            return Ok(results);
        }

        let target_ds = query_datasources(query)?;

        // Interval-based segment pruning (Druid timeline pruning, applied at
        // the Historical): a segment whose time interval intersects NONE of the
        // query's intervals can contribute no rows, so we skip it BEFORE
        // materializing to avoid an on-demand spill decode.
        //
        // Pruning is **gated to spill mode** (finding 2). In heap mode every
        // routed segment is already resident, so skipping a disjoint one saves
        // no decode — but it DOES reduce the per-segment partial fan-out (a
        // disjoint segment otherwise still contributes its empty/zero-fill
        // partial). Heap results are a byte-for-byte long-standing contract, so
        // heap keeps `prune = None` and executes every routed candidate exactly
        // as it did before FG-7. `None` also disables pruning for absent /
        // unparseable intervals, unmodelled query types, and — crucially —
        // Scan/Search, whose result shape is segment-derived (R10 HIGH; see
        // `query_prune_intervals`). Pruning is therefore restricted to the
        // aggregating types (timeseries/topN/groupBy) where a skipped segment
        // could only ever have produced an empty, answer-neutral partial.
        let prune = if self.spill_mode {
            query_prune_intervals(query)
        } else {
            None
        };

        let mut results = Vec::new();
        // Interval pruning skips a segment only when it can contribute no rows.
        // But a per-segment execution does more than filter rows: for e.g. a
        // timeseries with `skipEmptyBuckets:false` the executor SYNTHESIZES the
        // query interval's (zero-count) buckets, and it runs query-level
        // validation (bad filter / granularity / post-agg). If pruning skips
        // EVERY routed candidate the fan-out returns `[]` and those effects are
        // lost — the answer silently changes and validation errors vanish. So
        // we remember one routed candidate (heap-resident preferred to avoid a
        // spill decode) and, if pruning skipped them all, force exactly one to
        // execute. A disjoint segment yields only empty/zero buckets over the
        // query interval, so forcing one is answer-equivalent to the pre-prune
        // fan-out that executed every candidate, and any query-level validation
        // error fires identically on whichever candidate runs (finding 1).
        let mut first_candidate: Option<(&String, &SegmentEntry)> = None;
        let mut resident_candidate: Option<(&String, &SegmentEntry)> = None;
        let mut executed_candidate = false;

        for (seg_id, entry) in segments.iter() {
            // If the query targets a specific table datasource, default-deny
            // any segment that lacks an explicit mapping or whose mapping
            // disagrees. This is the cross-DS isolation guarantee.
            if let Some(ref ds_names) = target_ds {
                match ds_map.get(seg_id) {
                    None => {
                        // Unmapped segment — exclude. Logged so an operator
                        // can detect mis-publishes.
                        tracing::debug!(
                            segment_id = %seg_id,
                            target_datasources = ?ds_names,
                            "skipping unmapped segment (default-deny cross-DS isolation)"
                        );
                        continue;
                    }
                    Some(seg_ds) if !ds_names.contains(seg_ds) => continue,
                    Some(_) => {}
                }
            }

            // This segment is a routed candidate for the query — record it as a
            // forced-execution fallback (see the block comment above).
            if first_candidate.is_none() {
                first_candidate = Some((seg_id, entry));
            }
            if resident_candidate.is_none() && matches!(entry.residency, Residency::Resident(_)) {
                resident_candidate = Some((seg_id, entry));
            }

            // Interval prune BEFORE materialize (see `prune` above). Prune only
            // against the `__time`-derived interval; a segment with no derived
            // interval (`None` — 0 rows / no `__time`) is never pruned and
            // always executes, so a lying/absent header cannot drop its rows
            // (finding 1).
            if let Some(ranges) = &prune
                && let Some(seg_interval) = &entry.prune_interval
                && !interval_intersects(seg_interval, ranges)
            {
                continue;
            }

            // Materialize the decoded segment (heap: shared Arc; spill:
            // decode-or-reuse under the LRU budget). A decode failure fails the
            // whole query (fail-loud) — never a silent per-segment skip.
            executed_candidate = true;
            let data = self.materialize(entry, seg_id)?;
            match execute_query(query, &data) {
                Ok(result) => results.push(result),
                Err(e) => {
                    tracing::error!(segment_id = %seg_id, error = %e, "query failed on segment");
                    return Err(e);
                }
            }
        }

        // Semantics guard (finding 1): a query that routed to ≥1 candidate must
        // execute at least one, even if interval pruning skipped them all.
        if !executed_candidate && let Some((seg_id, entry)) = resident_candidate.or(first_candidate)
        {
            let data = self.materialize(entry, seg_id)?;
            match execute_query(query, &data) {
                Ok(result) => results.push(result),
                Err(e) => {
                    tracing::error!(segment_id = %seg_id, error = %e, "query failed on forced segment");
                    return Err(e);
                }
            }
        }

        Ok(results)
    }

    /// Get the list of loaded segment IDs.
    pub fn loaded_segments(&self) -> Vec<String> {
        let segments = match self.segments.read() {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        segments.keys().cloned().collect()
    }

    /// Get the number of loaded segments.
    pub fn segment_count(&self) -> usize {
        match self.segments.read() {
            Ok(s) => s.len(),
            Err(_) => 0,
        }
    }

    /// Check if a segment is loaded.
    pub fn has_segment(&self, segment_id: &str) -> bool {
        match self.segments.read() {
            Ok(s) => s.contains_key(segment_id),
            Err(_) => false,
        }
    }

    /// Get a clone of the loaded segment by ID.
    ///
    /// Returns the shared `Arc<SegmentData>` so callers can inspect
    /// dimensions/metrics/columns without acquiring a lock.
    ///
    /// The three-valued result distinguishes true absence from a read failure
    /// (finding 2): `Ok(None)` means the segment is not loaded, while `Err`
    /// means it IS loaded but its spilled bytes could not be decoded. Callers
    /// MUST propagate the `Err` (fail-loud) rather than treat it as absence —
    /// swallowing it silently drops the datasource from metadata / schema
    /// discovery and returns wrong-but-successful results.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment read lock is poisoned or, in spill mode,
    /// if a loaded segment's on-disk bytes cannot be decoded.
    pub fn get_segment(&self, segment_id: &str) -> Result<Option<Arc<SegmentData>>> {
        let segments = self.segments.read().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment read lock: {e}"))
        })?;
        let Some(entry) = segments.get(segment_id) else {
            return Ok(None);
        };
        self.materialize(entry, segment_id).map(Some)
    }

    /// Atomically compute the UNION column schema of `datasource` across every
    /// one of its loaded segments, WITHOUT decoding any of them.
    ///
    /// This is the building block for datasource-scoped schema discovery (the
    /// REST `build_schema_for`). A datasource's columns are the **union** of its
    /// segments' columns (schema evolution: segment `s1` may carry column `a`,
    /// `s2` column `b`), so a single representative segment's columns would drop
    /// the others — a HashMap-order-dependent regression (R14 HIGH). Each
    /// segment's schema is served from its load-time [`CachedSchema`], so this
    /// path never materializes (nor, in spill mode, disk-decodes) a segment: a
    /// spilled — or later externally-corrupted — segment can neither force a
    /// decode here nor break schema discovery, because its schema was captured
    /// (and spill-round-trip-verified) at load.
    ///
    /// **Cross-map atomicity (schema-discovery TOCTOU, R13 sibling).** BOTH read
    /// locks are held across the whole scan, in the single global order
    /// `segments` **then** `segment_datasources` (load / drop / swap /
    /// `execute_query_on_snapshot` take the same two locks in this order, so
    /// acquiring both here cannot deadlock). Resolving membership and reading the
    /// cached schema under one consistent view means a concurrent datasource
    /// remap (`swap A→B`) can no longer let an "id ∈ `datasource`" membership
    /// judgment pair with a differently-mapped segment's columns.
    ///
    /// **Default-deny.** Only a segment explicitly mapped to `datasource`
    /// contributes; unmapped and differently-mapped segments are excluded
    /// (mirrors native routing's default-deny). An absent datasource yields an
    /// empty [`DatasourceColumns`].
    ///
    /// # Errors
    ///
    /// Returns an error only if a read lock is poisoned. Unlike the retired
    /// data-returning accessor, no decode is attempted, so a mapped-but-
    /// undecodable spill segment can no longer surface a decode error here — its
    /// columns come from the cache.
    pub fn schema_for_datasource(&self, datasource: &str) -> Result<DatasourceColumns> {
        let segments = self.segments.read().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment read lock: {e}"))
        })?;
        let ds_map = self.segment_datasources.read().map_err(|e| {
            DruidError::Internal(format!("failed to acquire datasource read lock: {e}"))
        })?;
        // Default-deny: collect the cached schema of every segment mapped to
        // THIS datasource, under both held locks (consistent view), then union.
        let mut members: Vec<(&str, &CachedSchema)> = Vec::new();
        for (seg_id, entry) in segments.iter() {
            if ds_map.get(seg_id).is_some_and(|ds| ds == datasource) {
                members.push((seg_id.as_str(), &entry.schema));
            }
        }
        Ok(union_cached_schemas(members))
    }

    /// Atomically compute the UNION column schema of EVERY mapped datasource,
    /// each unioned across all of its loaded segments, WITHOUT decoding any of
    /// them.
    ///
    /// This is the building block for all-datasources schema enumeration (the
    /// REST `INFORMATION_SCHEMA` `enumerate_datasources`). Like
    /// [`Historical::schema_for_datasource`] it unions each datasource's columns
    /// across its segments (schema evolution), serving every column from its
    /// load-time [`CachedSchema`] — no materialize, no spill decode.
    ///
    /// **Cross-map atomicity (schema-discovery TOCTOU, R13 sibling).** BOTH read
    /// locks are held across the whole scan (order `segments` **then**
    /// `segment_datasources`, the single global order), so every datasource
    /// attribution and the schema unioned for it observe ONE consistent view.
    ///
    /// An unmapped segment carries no datasource to attribute its columns to and
    /// is skipped (mirrors the pre-atomic `enumerate_datasources`
    /// `segment_datasource == None` skip). The result is sorted by datasource
    /// name for a stable, HashMap-order-independent enumeration.
    ///
    /// # Errors
    ///
    /// Returns an error only if a read lock is poisoned (no decode is attempted).
    pub fn datasource_schemas(&self) -> Result<Vec<(String, DatasourceColumns)>> {
        let segments = self.segments.read().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment read lock: {e}"))
        })?;
        let ds_map = self.segment_datasources.read().map_err(|e| {
            DruidError::Internal(format!("failed to acquire datasource read lock: {e}"))
        })?;
        // Group each mapped segment's cached schema under its datasource, then
        // union per datasource. An unmapped segment is skipped (no datasource).
        let mut groups: HashMap<&str, Vec<(&str, &CachedSchema)>> = HashMap::new();
        for (seg_id, entry) in segments.iter() {
            if let Some(ds) = ds_map.get(seg_id) {
                groups
                    .entry(ds.as_str())
                    .or_default()
                    .push((seg_id.as_str(), &entry.schema));
            }
        }
        let mut out: Vec<(String, DatasourceColumns)> = groups
            .into_iter()
            .map(|(ds, members)| (ds.to_owned(), union_cached_schemas(members)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Get the data source name associated with a loaded segment, if
    /// any was registered via [`set_segment_datasource`].
    ///
    /// [`set_segment_datasource`]: Historical::set_segment_datasource
    pub fn segment_datasource(&self, segment_id: &str) -> Option<String> {
        let ds_map = self.segment_datasources.read().ok()?;
        ds_map.get(segment_id).cloned()
    }

    /// Row count of a loaded segment, if present — served from resident
    /// metadata, so a spilled segment answers without being decoded.
    pub fn segment_num_rows(&self, segment_id: &str) -> Option<usize> {
        let segments = self.segments.read().ok()?;
        segments.get(segment_id).map(|entry| entry.num_rows)
    }

    /// Get diagnostic information about this Historical node.
    pub fn info(&self) -> HistoricalInfo {
        let cache_bytes_used = if self.spill_mode {
            // Spill mode bounds the resident decoded bytes, not the total
            // admitted payload weight.
            self.resident_bytes.load(Ordering::Relaxed)
        } else {
            self.current_cache_bytes.load(Ordering::Relaxed)
        };
        HistoricalInfo {
            segment_count: self.segment_count(),
            cache_bytes_used,
            cache_bytes_max: self.max_cache_bytes,
        }
    }

    /// Get the local cache directory path.
    pub fn cache_dir(&self) -> &PathBuf {
        &self.cache_dir
    }

    // -----------------------------------------------------------------------
    // Spill-mode residency (FG-7)
    // -----------------------------------------------------------------------

    /// Write a segment's `v9` bytes to a fresh, uniquely-named spill directory
    /// under this instance's private root `cache_dir/spill/<pid>-<nonce>/`. The
    /// unique counter suffix means concurrent writers never clash on disk, so
    /// the (slow) write runs OUTSIDE any lock; the per-instance root means two
    /// Historicals sharing a `cache_dir` never collide on an `<id>-<counter>`
    /// path (finding 2).
    ///
    /// The returned path is the per-write **enclosing** directory
    /// `spill/<pid>-<nonce>/<id>-<counter>/`; the segment's own `v9` bytes live
    /// in the [`SPILL_SEGMENT_SUBDIR`] child. Because [`write_segment_v9`]
    /// stages through a sibling `*.tmp.*` directory *next to* its target,
    /// placing the target one level deep means BOTH that staging sibling AND the
    /// final segment directory are created inside the enclosing directory. Any
    /// write error (disk-full / quota / fsync / rename) is therefore fully
    /// cleaned up by removing the single enclosing directory — no orphaned
    /// staging or half-published segment survives a failed admission (finding
    /// 4).
    fn spill_write(&self, segment_id: &str, segment: &SegmentData) -> Result<PathBuf> {
        // Idempotent — the constructor created it, but a subsequent external
        // `rm -rf` should not wedge loads.
        std::fs::create_dir_all(&self.spill_instance_dir).map_err(|e| {
            DruidError::Segment(format!(
                "failed to create spill instance dir {}: {e}",
                self.spill_instance_dir.display()
            ))
        })?;
        let counter = self.spill_counter.fetch_add(1, Ordering::Relaxed);
        let dir = self
            .spill_instance_dir
            .join(format!("{}-{counter}", sanitize_segment_id(segment_id)));
        let seg_dir = dir.join(SPILL_SEGMENT_SUBDIR);
        if let Err(e) = write_segment_v9(segment, &seg_dir) {
            // Best-effort clean the whole enclosing dir so no staging/final
            // orphan accumulates across repeated failed admissions.
            remove_spill_dir(&dir);
            return Err(e);
        }
        // Self-verify the spill round-trip (R8): re-read what we just wrote with
        // the same strict decoder a later query uses and confirm it restores the
        // original faithfully. A silent writer-fidelity gap (a `columns` entry
        // dropped because it is neither a dimension nor a metric, a smoosh-meta
        // delimiter in a column name, a future gap) otherwise surfaces only as a
        // permanent wrong-or-never materialize at query time. Reject fail-loud,
        // cleaning the enclosing dir so no orphan survives (finding 4).
        if let Err(e) = verify_spill_roundtrip(segment_id, segment, &seg_dir) {
            remove_spill_dir(&dir);
            return Err(e);
        }
        Ok(dir)
    }

    /// Spill-mode load: write the bytes to disk (lock-free), then insert a
    /// decode-on-demand handle under the write locks. On any rejection the
    /// freshly-written directory is deleted so no orphan is left behind.
    fn load_spilled(
        &self,
        segment_id: &str,
        datasource: Option<&str>,
        segment: SegmentData,
    ) -> Result<()> {
        // Refuse a segment that cannot survive the spill round-trip BEFORE
        // touching the disk, so a no-`__time` segment fails at admission rather
        // than permanently at every later query (finding 3).
        ensure_spillable(segment_id, &segment)?;
        let estimated_bytes = estimate_segment_bytes(&segment);
        let num_rows = segment.num_rows;
        let prune_interval = derive_prune_interval(&segment);
        // Capture the column schema BEFORE the heap copy is freed, so schema
        // discovery is decode-free even for a spilled segment.
        let schema = CachedSchema::from_segment(&segment);
        // No `time_sorted` reconciliation is needed on the spill path (FG-7
        // R19): the caller's copy never becomes query-visible (it is dropped
        // below once the bytes are on disk), and every later read comes from
        // `SegmentData::open`, which RE-DERIVES `time_sorted` from the persisted
        // `__time` (`is_sorted`). The reloaded flag is therefore authoritative
        // and always truthful — which is precisely why the spilled residency was
        // the CORRECT side of the R19 divergence; only the heap residency
        // (`SegmentEntry::resident`) had to reconcile the caller-supplied flag.
        let dir = self.spill_write(segment_id, &segment)?;
        drop(segment); // the bytes are on disk now; free the heap copy.

        let mut segments = match self.segments.write() {
            Ok(guard) => guard,
            Err(e) => {
                remove_spill_dir(&dir);
                return Err(DruidError::Internal(format!(
                    "failed to acquire segment write lock: {e}"
                )));
            }
        };
        let mut ds_map = match self.segment_datasources.write() {
            Ok(guard) => guard,
            Err(e) => {
                remove_spill_dir(&dir);
                return Err(DruidError::Internal(format!(
                    "failed to acquire datasource write lock: {e}"
                )));
            }
        };
        if segments.contains_key(segment_id) {
            remove_spill_dir(&dir);
            return Err(DruidError::Segment(format!(
                "segment id collision: '{segment_id}' is already loaded"
            )));
        }
        segments.insert(
            segment_id.to_owned(),
            SegmentEntry {
                residency: Residency::Spilled {
                    dir,
                    decoded: Mutex::new(Weak::new()),
                },
                estimated_bytes,
                num_rows,
                prune_interval,
                schema,
            },
        );
        if let Some(datasource) = datasource {
            ds_map.insert(segment_id.to_owned(), datasource.to_owned());
        }
        Ok(())
    }

    /// Spill-mode datasource mapping: validate against the segment's real
    /// (decoded, possibly re-read) data exactly as heap mode does.
    fn set_segment_datasource_spilled(&self, segment_id: &str, datasource: &str) -> Result<()> {
        let segments = self.segments.read().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment read lock: {e}"))
        })?;
        let entry = segments.get(segment_id).ok_or_else(|| {
            DruidError::Segment(format!(
                "cannot map datasource for unloaded segment: {segment_id}"
            ))
        })?;
        let data = self.materialize(entry, segment_id)?;
        check_null_generation(datasource, data.as_ref(), self.strict_null_generation)?;
        let mut ds_map = self.segment_datasources.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire datasource write lock: {e}"))
        })?;
        ds_map.insert(segment_id.to_owned(), datasource.to_owned());
        Ok(())
    }

    /// Spill-mode drop: remove the entry (and its LRU pin), then delete the
    /// spill directory outside the lock. Drop-only never fails on admission
    /// (there is none in spill mode); its sole failure mode is a poisoned lock
    /// or a not-loaded id — matching the heap contract.
    fn drop_spilled(&self, segment_id: &str) -> Result<()> {
        let removed = {
            let mut segments = self.segments.write().map_err(|e| {
                DruidError::Internal(format!("failed to acquire segment write lock: {e}"))
            })?;
            let mut ds_map = self.segment_datasources.write().map_err(|e| {
                DruidError::Internal(format!("failed to acquire datasource write lock: {e}"))
            })?;
            let Some(entry) = segments.remove(segment_id) else {
                return Err(DruidError::Query(format!(
                    "segment not loaded: {segment_id}"
                )));
            };
            ds_map.remove(segment_id);
            self.lru_remove(segment_id);
            if segments.is_empty() {
                segments.shrink_to_fit();
            }
            if ds_map.is_empty() {
                ds_map.shrink_to_fit();
            }
            entry
        };
        if let Residency::Spilled { dir, .. } = &removed.residency {
            remove_spill_dir(dir);
        }
        Ok(())
    }

    /// Spill-mode atomic swap (mirror of the heap `replace_segments`).
    ///
    /// The swap itself runs under a single write-lock acquisition (atomic to
    /// queries); spilling the adds to disk is kept OUTSIDE the lock.
    ///
    /// **Returned removed-entries contract (spill divergence, finding 3):**
    /// * A **real swap** (`add` non-empty) decodes every loaded victim under
    ///   the lock BEFORE mutating and returns each with its real data, exactly
    ///   like heap mode — feedable verbatim into a rollback swap. If any victim
    ///   cannot be decoded the swap is rejected fail-closed with NO mutation
    ///   (rather than silently omitting the victim from the return, which would
    ///   let the caller mistake a loaded-but-corrupt victim for a never-loaded
    ///   one). The cost is that a real swap holds the write lock across any cold
    ///   victim's decode.
    /// * A **pure drop** (`add` empty) decodes NOTHING and returns an EMPTY
    ///   vector. Heap mode returns dropped victims with their (already-resident)
    ///   data, but spill mode cannot without a fallible disk decode on the drop
    ///   path, which the drop-only-decrease invariant (R30) forbids. No caller
    ///   consumes a pure drop's returned data, so this divergence is benign.
    fn replace_spilled(
        &self,
        drop_ids: &[String],
        add: Vec<SegmentSwapEntry>,
    ) -> Result<Vec<SegmentSwapEntry>> {
        // 1. Validate every add BEFORE spilling anything: reject a no-`__time`
        //    segment (finding 3 — it cannot survive the spill round-trip) and
        //    run the null-generation gate. Both checks run on the in-memory data
        //    before any byte hits the disk, so an invalid swap spills nothing and
        //    leaves no orphan.
        for entry in &add {
            ensure_spillable(&entry.id, entry.data.as_ref())?;
            if let Some(datasource) = entry.datasource.as_deref() {
                check_null_generation(
                    datasource,
                    entry.data.as_ref(),
                    self.strict_null_generation,
                )?;
            } else if self.strict_null_generation {
                check_null_generation(&entry.id, entry.data.as_ref(), true)?;
            }
        }
        // 2. Spill each add to a unique directory OUTSIDE the lock. Track the
        //    written directories so an invalid swap deletes them all (no
        //    orphan) — the spill happens only after the null-gen gate above.
        let mut prepared: Vec<PreparedSpill> = Vec::with_capacity(add.len());
        for entry in add {
            let estimated_bytes = estimate_segment_bytes(&entry.data);
            let num_rows = entry.data.num_rows;
            let prune_interval = derive_prune_interval(&entry.data);
            let schema = CachedSchema::from_segment(&entry.data);
            // Like `load_spilled`, no `time_sorted` reconciliation is needed
            // here (FG-7 R19): the add's decoded copy is never made query-visible
            // resident — it is spilled and dropped, and every later read is a
            // `SegmentData::open` that re-derives `time_sorted` from the
            // persisted `__time`. The spilled copy is therefore authoritative;
            // only the heap path (`SegmentEntry::resident`) reconciles.
            let dir = match self.spill_write(&entry.id, &entry.data) {
                Ok(dir) => dir,
                Err(e) => {
                    for prep in &prepared {
                        remove_spill_dir(&prep.dir);
                    }
                    return Err(e);
                }
            };
            prepared.push(PreparedSpill {
                id: entry.id,
                dir,
                datasource: entry.datasource,
                estimated_bytes,
                num_rows,
                prune_interval,
                schema,
            });
        }

        // A real swap (non-empty add) honors the removed-entries data contract;
        // a pure drop (empty add) does not decode victims at all.
        let is_real_swap = !prepared.is_empty();

        // 3. Phase A — the atomic swap, under a single write-lock acquisition.
        //    For a REAL swap every loaded victim is decoded for the returned-
        //    entries contract BEFORE any mutation, so a decode failure rejects
        //    the whole swap fail-closed with NO mutation — never a silent
        //    omission that would let the caller mistake a loaded-but-corrupt
        //    victim for a never-loaded one (finding 3). A PURE drop decodes
        //    NOTHING: the drop path must succeed even on a corrupt victim
        //    (R30 drop-only-decrease), so it returns no removed-entry data.
        //    Victim spill directories are collected and deleted in phase B
        //    (outside the lock).
        let (removed, victim_dirs) = {
            let mut segments = match self.segments.write() {
                Ok(guard) => guard,
                Err(e) => {
                    for prep in &prepared {
                        remove_spill_dir(&prep.dir);
                    }
                    return Err(DruidError::Internal(format!(
                        "failed to acquire segment write lock: {e}"
                    )));
                }
            };
            let mut ds_map = match self.segment_datasources.write() {
                Ok(guard) => guard,
                Err(e) => {
                    for prep in &prepared {
                        remove_spill_dir(&prep.dir);
                    }
                    return Err(DruidError::Internal(format!(
                        "failed to acquire datasource write lock: {e}"
                    )));
                }
            };

            // Validate BEFORE mutating — same rules as the heap path.
            let mut drop_set = std::collections::HashSet::with_capacity(drop_ids.len());
            for id in drop_ids {
                drop_set.insert(id.as_str());
            }
            let mut seen_add_ids = std::collections::HashSet::with_capacity(prepared.len());
            for prep in &prepared {
                let ambiguous = !seen_add_ids.insert(prep.id.as_str());
                let undeclared =
                    segments.contains_key(&prep.id) && !drop_set.contains(prep.id.as_str());
                if ambiguous || undeclared {
                    // Reject WITHOUT mutating; delete every pre-written dir.
                    drop(segments);
                    drop(ds_map);
                    for prep in &prepared {
                        remove_spill_dir(&prep.dir);
                    }
                    return Err(DruidError::Segment(format!(
                        "segment id collision: '{}' {}; refusing the ambiguous swap",
                        prep.id,
                        if ambiguous {
                            "appears more than once in one replace_segments call"
                        } else {
                            "is already loaded and not listed for removal"
                        }
                    )));
                }
            }

            // Real swap: materialize every loaded victim's data BEFORE mutating,
            // failing the whole swap (no mutation, no dir deletion) if any
            // cannot be decoded. This keeps `Vec<SegmentSwapEntry>` (feedable
            // verbatim into a rollback swap) with real data for every returned
            // victim, and closes the silent-omit hole (finding 3).
            let mut decoded: HashMap<String, Arc<SegmentData>> = HashMap::new();
            if is_real_swap {
                for id in drop_ids {
                    if let Some(entry) = segments.get(id) {
                        match self.take_resident_or_decode(entry, id) {
                            Ok(data) => {
                                decoded.insert(id.clone(), data);
                            }
                            Err(e) => {
                                drop(segments);
                                drop(ds_map);
                                for prep in &prepared {
                                    remove_spill_dir(&prep.dir);
                                }
                                return Err(DruidError::Segment(format!(
                                    "spill replace: refusing to swap — removed victim '{id}' \
                                     could not be decoded for the returned-entries contract \
                                     (fail-closed, nothing mutated): {e}"
                                )));
                            }
                        }
                    }
                }
            }

            let mut removed = Vec::with_capacity(drop_ids.len());
            let mut victim_dirs = Vec::with_capacity(drop_ids.len());
            for id in drop_ids {
                if let Some(entry) = segments.remove(id) {
                    let datasource = ds_map.remove(id);
                    self.lru_remove(id);
                    if let Residency::Spilled { dir, .. } = &entry.residency {
                        victim_dirs.push(dir.clone());
                    }
                    // Real swap: the victim's data was decoded fail-closed above,
                    // so it is present for every loaded victim. Pure drop: none.
                    if let Some(data) = decoded.remove(id) {
                        removed.push(SegmentSwapEntry {
                            id: id.clone(),
                            data,
                            datasource,
                        });
                    }
                }
            }
            for prep in prepared {
                match prep.datasource {
                    Some(datasource) => {
                        ds_map.insert(prep.id.clone(), datasource);
                    }
                    None => {
                        ds_map.remove(&prep.id);
                    }
                }
                segments.insert(
                    prep.id,
                    SegmentEntry {
                        residency: Residency::Spilled {
                            dir: prep.dir,
                            decoded: Mutex::new(Weak::new()),
                        },
                        estimated_bytes: prep.estimated_bytes,
                        num_rows: prep.num_rows,
                        prune_interval: prep.prune_interval,
                        schema: prep.schema,
                    },
                );
            }
            if segments.is_empty() {
                segments.shrink_to_fit();
            }
            if ds_map.is_empty() {
                ds_map.shrink_to_fit();
            }
            (removed, victim_dirs)
        };

        // 4. Phase B (no lock): free the victims' spill directories. The removed
        //    entries were already fully built under the lock (real swap) or are
        //    intentionally empty (pure drop) — no decode happens here, so the
        //    drop path can never fail on a corrupt victim.
        for dir in &victim_dirs {
            remove_spill_dir(dir);
        }
        Ok(removed)
    }

    /// Materialize an entry's decoded data. Heap: clone the shared `Arc`.
    /// Spill: reuse the LRU-pinned decode if still resident (single-flight via
    /// the per-entry `decoded` weak), otherwise re-read from disk once and
    /// admit it to the memory-budgeted LRU (evicting the least-recently-used
    /// residents to stay within budget).
    fn materialize(&self, entry: &SegmentEntry, seg_id: &str) -> Result<Arc<SegmentData>> {
        match &entry.residency {
            Residency::Resident(data) => Ok(Arc::clone(data)),
            Residency::Spilled { dir, decoded } => {
                let mut weak = decoded.lock().map_err(|e| {
                    DruidError::Internal(format!(
                        "spill decode lock poisoned for segment {seg_id}: {e}"
                    ))
                })?;
                if let Some(data) = weak.upgrade() {
                    // The decode is still alive (reused via the weak, no
                    // re-read). Admit-or-touch it: if it is still LRU-resident
                    // just refresh recency, but if it was evicted while an
                    // external `Arc` kept it alive, re-register it so a later
                    // query reuses this decode instead of re-reading from disk
                    // (finding 5). `admit` counts bytes only when it is not
                    // already resident, so this can never double-count.
                    let mut lru = self.resident_lru.lock().map_err(|e| {
                        DruidError::Internal(format!("resident LRU lock poisoned: {e}"))
                    })?;
                    lru.admit(
                        seg_id.to_owned(),
                        Arc::clone(&data),
                        entry.estimated_bytes,
                        &self.resident_bytes,
                        self.max_cache_bytes,
                    );
                    return Ok(data);
                }
                // The one decode, under the per-entry lock (single-flight).
                let data = Arc::new(SegmentData::open(&dir.join(SPILL_SEGMENT_SUBDIR)).map_err(
                    |e| {
                        DruidError::Segment(format!(
                            "failed to re-read spilled segment {seg_id} from {}: {e}",
                            dir.display()
                        ))
                    },
                )?);
                self.decode_count.fetch_add(1, Ordering::Relaxed);
                *weak = Arc::downgrade(&data);
                let mut lru = self.resident_lru.lock().map_err(|e| {
                    DruidError::Internal(format!("resident LRU lock poisoned: {e}"))
                })?;
                lru.admit(
                    seg_id.to_owned(),
                    Arc::clone(&data),
                    entry.estimated_bytes,
                    &self.resident_bytes,
                    self.max_cache_bytes,
                );
                Ok(data)
            }
        }
    }

    /// Get a removed victim's data for the `replace_segments` return: reuse the
    /// resident decode if one is still alive, otherwise re-read from disk once.
    /// Never touches the LRU (the entry is being removed).
    fn take_resident_or_decode(
        &self,
        entry: &SegmentEntry,
        seg_id: &str,
    ) -> Result<Arc<SegmentData>> {
        match &entry.residency {
            Residency::Resident(data) => Ok(Arc::clone(data)),
            Residency::Spilled { dir, decoded } => {
                if let Ok(weak) = decoded.lock()
                    && let Some(data) = weak.upgrade()
                {
                    return Ok(data);
                }
                let data = Arc::new(SegmentData::open(&dir.join(SPILL_SEGMENT_SUBDIR)).map_err(
                    |e| {
                        DruidError::Segment(format!(
                            "failed to re-read spilled segment {seg_id} from {}: {e}",
                            dir.display()
                        ))
                    },
                )?);
                self.decode_count.fetch_add(1, Ordering::Relaxed);
                Ok(data)
            }
        }
    }

    /// Evict `seg_id` from the resident LRU (spill mode). A poisoned LRU lock
    /// leaves the byte counter approximate but never blocks the drop.
    fn lru_remove(&self, seg_id: &str) {
        if let Ok(mut lru) = self.resident_lru.lock() {
            lru.remove(seg_id, &self.resident_bytes);
        }
    }
}

/// One add entry of a spill-mode swap after its bytes have been written to a
/// unique spill directory (but before the atomic map mutation).
struct PreparedSpill {
    id: String,
    dir: PathBuf,
    datasource: Option<String>,
    estimated_bytes: u64,
    num_rows: usize,
    prune_interval: Option<Interval>,
    /// Column schema captured from the add's decoded data BEFORE it is spilled
    /// and its heap copy dropped, so the resulting spilled entry serves schema
    /// discovery decode-free.
    schema: CachedSchema,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract routable table datasource names, rejecting unsupported sources.
fn query_datasources(query: &DruidQuery) -> Result<Option<std::collections::HashSet<String>>> {
    let ds = match query {
        DruidQuery::Timeseries(q) => &q.data_source,
        DruidQuery::TopN(q) => &q.data_source,
        DruidQuery::GroupBy(q) => &q.data_source,
        DruidQuery::Scan(q) => &q.data_source,
        DruidQuery::Search(q) => &q.data_source,
        DruidQuery::SegmentMetadata(q) => &q.data_source,
        DruidQuery::DataSourceMetadata(q) => &q.data_source,
        DruidQuery::TimeBoundary(q) => &q.data_source,
        DruidQuery::UnionAll(queries) => {
            let mut names = std::collections::HashSet::new();
            for query in queries {
                if let Some(branch_names) = query_datasources(query)? {
                    names.extend(branch_names);
                }
            }
            return Ok(Some(names));
        }
        DruidQuery::Window(q) => &q.inner.data_source,
    };
    match ds {
        DataSource::Table { name } => Ok(Some(std::iter::once(name.clone()).collect())),
        DataSource::Union { data_sources } => Ok(Some(data_sources.iter().cloned().collect())),
        DataSource::Query { .. } => Err(DruidError::Query(
            "query datasources are not supported by Historical routing".to_owned(),
        )),
        DataSource::Lookup { .. } => Err(DruidError::Query(
            "lookup datasources are not supported by Historical routing".to_owned(),
        )),
        DataSource::Inline { .. } => Err(DruidError::Query(
            "inline datasources are not supported by Historical routing".to_owned(),
        )),
    }
}

fn merge_scan_branch(_query: &DruidQuery, partials: Vec<QueryResult>) -> Result<QueryResult> {
    let mut merged = ScanResult {
        segment_id: None,
        columns: Vec::new(),
        events: Vec::new(),
    };
    for partial in partials {
        let QueryResult::Scan(scan) = partial else {
            return Err(DruidError::Query(
                "UNION ALL is only supported over scan-shaped branches; \
                 aggregate/groupBy/topN/window branches are not supported"
                    .to_owned(),
            ));
        };
        for column in scan.columns {
            if !merged.columns.contains(&column) {
                merged.columns.push(column);
            }
        }
        merged.events.extend(scan.events);
    }
    Ok(QueryResult::Scan(merged))
}

fn allocation_bytes<T>(capacity: usize) -> u64 {
    u64::try_from(capacity)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX))
}

/// Conservatively estimate the raw allocation of a `HashMap<_, Elem>` that
/// currently reports `capacity` usable slots.
///
/// `HashMap::capacity()` reports usable slots (≈ `buckets * 7/8`), not the
/// raw table size. hashbrown rounds the bucket count up to a power of two
/// and stores one control byte per bucket PLUS a trailing `Group::WIDTH`
/// control group (8/16/32/64 bytes by target) for probe wraparound, so a
/// capacity-only charge under-counts the real allocation near a growth
/// boundary. This helper charges the next power-of-two bucket count above
/// the load-factor-adjusted capacity, one control byte per bucket, plus a
/// fixed group/alignment allowance.
///
/// This remains part of the per-segment estimate for the segment's columns
/// map. The Historical's outer segment and datasource maps are deliberately
/// excluded from cache accounting so admission never has to predict a future
/// hash-table capacity.
fn hash_table_bytes<Elem>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    // Generous fixed allowance for the trailing control group (`Group::WIDTH`
    // is 8/16/32/64 by target) plus table alignment padding.
    const GROUP_AND_ALIGN_ALLOWANCE: u64 = 128;
    // Smallest bucket count whose 7/8 load factor still holds `capacity`.
    let min_buckets = capacity.saturating_mul(8) / 7 + 1;
    let buckets = min_buckets.max(4).next_power_of_two();
    // Element slot + one control byte per bucket.
    let per_bucket = std::mem::size_of::<Elem>().saturating_add(1);
    u64::try_from(buckets)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(per_bucket).unwrap_or(u64::MAX))
        .saturating_add(GROUP_AND_ALIGN_ALLOWANCE)
}

#[cfg(test)]
std::thread_local! {
    static CACHE_STATE_FULL_FOLDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn estimate_cache_state(
    segments: &HashMap<String, SegmentEntry>,
    datasources: &HashMap<String, String>,
) -> u64 {
    CACHE_STATE_FULL_FOLDS.with(|folds| folds.set(folds.get().saturating_add(1)));
    let bytes = segments.iter().fold(0_u64, |total, (id, loaded)| {
        total.saturating_add(segment_entry_bytes(
            id,
            loaded.estimated_bytes,
            loaded.schema.schema_heap_bytes(),
        ))
    });
    datasources.iter().fold(bytes, |total, (id, datasource)| {
        total.saturating_add(datasource_entry_bytes(id, datasource))
    })
}

fn ensure_cache_limit(observed: u64, maximum: u64) -> Result<()> {
    if observed > maximum {
        return Err(DruidError::ResourceLimit {
            kind: "historical.cacheBytes",
            limit: usize::try_from(maximum).unwrap_or(usize::MAX),
            observed: usize::try_from(observed).unwrap_or(usize::MAX),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Spill-mode free helpers (FG-7)
// ---------------------------------------------------------------------------

/// Child directory of a per-write spill enclosure that actually holds the
/// segment's `v9` bytes. The enclosure (`spill/<id>-<counter>/`) exists so a
/// failed [`write_segment_v9`] — whose atomic-rename staging sibling would
/// otherwise land *next to* the target — can be cleaned up in full by removing
/// just the enclosure (finding 4).
const SPILL_SEGMENT_SUBDIR: &str = "v9";

/// Name of the advisory-lock sentinel created directly inside each instance
/// spill root (findings 3+4). An instance takes an exclusive flock on this file
/// for its whole lifetime; the startup janitor reaps a peer root only when it
/// can itself take that root's lock (proof the owner is dead). It is filtered
/// out of any per-write enclosure count.
const SPILL_LOCK_FILE: &str = ".lock";

/// Filename prefix of a **staging** instance directory: a transient
/// `spill/.staging-<nonce>/` created and `.lock`-pinned *before* it is
/// atomically renamed to its final reapable `spill/<pid>-<nonce>/` name
/// ([`claim_instance_dir`]). The leading `.` and the `staging-` prefix keep it
/// clear of the final `<pid>-<nonce>` form, so a rename can never collide with a
/// peer's published root, and it reads as obviously transient in logs. A crash
/// mid-claim can leave one behind; the startup janitor reaps it exactly like any
/// other instance root (its `.lock` is then free), so it is never an unbounded
/// leak.
const SPILL_STAGING_PREFIX: &str = ".staging-";

/// Turn a segment id into a filesystem-safe directory component. Non
/// `[A-Za-z0-9._-]` characters become `_` and the result is length-capped;
/// uniqueness across ids is guaranteed by the caller's monotonic counter
/// suffix, so a lossy sanitization can never cause a spill-directory clash.
fn sanitize_segment_id(id: &str) -> String {
    let mut sanitized: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .take(180)
        .collect();
    if sanitized.is_empty() {
        sanitized.push_str("seg");
    }
    sanitized
}

/// A monotonic, per-process-start-distinct nonce used to name spill directories
/// (both the transient `.staging-<nonce>` and the final `<pid>-<nonce>`). It is
/// a process-local sequence mixed with a wall-clock timestamp: successive calls
/// always differ, so a retry after a collision makes progress. Uniqueness is NOT
/// relied upon for correctness — `create_dir` and `rename` are the exclusion
/// primitives — only for readable, progress-making names.
fn spill_nonce() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{nanos}_{seq}")
}

/// Build a human-informative, per-process-start-distinct **final** instance
/// spill-root name `<pid>-<nonce>`.
///
/// Cross-instance UNIQUENESS is no longer the name's job (findings 3+4): the
/// staging-then-rename claim in [`claim_instance_dir`] is the exclusion
/// primitive, so a colliding name simply forces a retry with a fresh nonce. The
/// `<pid>` prefix and monotonic `<nonce>` remain only to keep names readable and
/// to make successive claims differ so a retry makes progress. The janitor no
/// longer parses this name for a pid — it tests liveness via the advisory lock.
fn new_spill_instance_name() -> String {
    format!("{}-{}", std::process::id(), spill_nonce())
}

/// Build a transient **staging** directory name `.staging-<nonce>`. It is
/// dot-prefixed and never of the final `<pid>-<nonce>` form, so renaming it onto
/// a peer's published root is impossible and the janitor's reap treats it
/// uniformly with any other instance root.
fn new_spill_staging_name() -> String {
    format!("{SPILL_STAGING_PREFIX}{}", spill_nonce())
}

/// Claim a private instance spill root under `spill_root` and take its lifetime
/// exclusive advisory lock (findings 3+4; R4/R5 HIGH).
///
/// The claim is **staging-then-atomic-rename** so a reapable final-named root is
/// never observable without its `.lock` already exclusively held:
///
/// 1. A transient `spill/.staging-<nonce>/` directory is created and its `.lock`
///    sentinel taken EXCLUSIVELY ([`create_locked_staging_dir`]).
/// 2. The staging directory is published to its final `spill/<pid>-<nonce>/`
///    name with a single [`std::fs::rename`] (atomic). Because the exclusive
///    `flock` is bound to the open file description — not the path — it survives
///    the rename, so the final root is *born locked*. A peer janitor can
///    therefore never observe a final-named root that is not yet lock-pinned,
///    which closes the construction/sweep TOCTOU **structurally** — no
///    arbitration lock needed (R4/R5 HIGH).
///
/// The returned `File` MUST be kept alive by the caller (dropping it releases
/// the lock, marking the root reapable).
///
/// Construction stays **infallible**:
/// * A `NotFound` rename means the staging dir vanished (a peer janitor reaped
///   it in the brief pre-lock window); we drop the now-dangling lock and retry a
///   fresh staging dir.
/// * Any other rename error (an astronomically rare final-name collision, or a
///   transient FS fault) leaves the staging dir intact and locked, so we adopt
///   IT as the instance root — its `.lock` is already held, so there is still no
///   reapable window.
/// * Only if no locked directory can be established at all do we fall back to an
///   unlocked nominal name (best-effort). In that degraded case no locked
///   sentinel is left behind, so a peer janitor — which requires a lockable
///   sentinel to reap — errs toward KEEPING the root.
fn claim_instance_dir(spill_root: &Path) -> (PathBuf, Option<File>) {
    for _ in 0..64 {
        // Create a fresh staging dir and take its `.lock` EXCLUSIVELY *before*
        // the directory is ever exposed under a reapable final name.
        let (staging, lock) = match create_locked_staging_dir(spill_root) {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(
                    spill_root = %spill_root.display(),
                    error = %e,
                    "spill: failed to create/lock a staging instance dir (retrying)"
                );
                continue;
            }
        };
        // Publish staging → final with an atomic rename. The lock is already
        // held, so the final name can never be observed unlocked.
        let final_dir = spill_root.join(new_spill_instance_name());
        match std::fs::rename(&staging, &final_dir) {
            Ok(()) => return (final_dir, Some(lock)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // The staging dir vanished (reaped in the brief pre-lock window):
                // drop the dangling lock and retry with a fresh staging dir.
                drop(lock);
                continue;
            }
            Err(e) => {
                // Collision or transient fault: the staging dir survives and we
                // still hold its lock, so adopt IT as the instance root — its
                // `.lock` is held, so there is no reapable window.
                tracing::warn!(
                    staging = %staging.display(),
                    final_dir = %final_dir.display(),
                    error = %e,
                    "spill: could not publish staging root to its final name; \
                     adopting the (locked) staging dir as the instance root"
                );
                return (staging, Some(lock));
            }
        }
    }
    // Pathological: no locked directory could be established. Degrade to an
    // unlocked nominal name (best-effort; construction stays infallible). No
    // locked sentinel is left behind, so a peer janitor keeps the root.
    tracing::warn!(
        spill_root = %spill_root.display(),
        "spill: exhausted staging/rename retries; proceeding without a liveness lock"
    );
    (spill_root.join(new_spill_instance_name()), None)
}

/// Create ONE fresh `spill/.staging-<nonce>/` directory and take its `.lock`
/// sentinel's **exclusive** advisory lock, returning the held `File` (which the
/// caller keeps for the process's lifetime).
///
/// `create_dir` (NOT `create_dir_all`) makes the directory creation itself an
/// exclusion primitive: a name collision fails `AlreadyExists` and forces a
/// fresh nonce (a couple of iterations resolve the pathological case). The lock
/// is taken HERE — before [`claim_instance_dir`] renames the staging dir to its
/// final reapable name — so the final root is born locked. On any error the
/// half-built staging dir is rolled back and the error is returned.
fn create_locked_staging_dir(spill_root: &Path) -> std::io::Result<(PathBuf, File)> {
    for _ in 0..64 {
        let staging = spill_root.join(new_spill_staging_name());
        match std::fs::create_dir(&staging) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
        let lock_path = staging.join(SPILL_LOCK_FILE);
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(file) => file,
            Err(e) => {
                // Freshly created and solely ours; roll the whole dir back.
                remove_spill_dir(&staging);
                return Err(e);
            }
        };
        return match FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok((staging, file)),
            Err(e) => {
                // A brand-new, solely-owned sentinel that cannot be locked is
                // pathological (not mere contention); roll back and report.
                drop(file);
                remove_spill_dir(&staging);
                Err(e)
            }
        };
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "spill: exhausted staging-name candidates",
    ))
}

/// Startup janitor (findings 3+4): reap orphaned instance spill roots directly
/// under `spill_root`, deployment-independently and WITHOUT trusting process
/// ids. For each peer root (not `self_root`) it opens the `.lock` sentinel and
/// attempts the exclusive advisory lock:
///
/// * lock acquired → NO live holder exists anywhere (even in another PID
///   namespace or under `hidepid`, where `/proc` liveness is blind) → the root
///   is safe to `remove_dir_all`; the lock is released immediately after.
/// * lock refused (`WouldBlock`), sentinel absent, or any open/lock error →
///   treated as "maybe live" and KEPT.
///
/// The janitor thus errs toward keeping and can never wipe a live peer's bytes.
///
/// It needs no arbitration lock: [`claim_instance_dir`] publishes a new instance
/// root by renaming an already-`.lock`-held staging directory into place, so a
/// reapable final-named root is *never* observable without its lock already held
/// — the construction/sweep TOCTOU is closed structurally (R4/R5 HIGH). This
/// same pass also collects crash-residue `spill/.staging-<nonce>/` directories:
/// their `.lock` is free once the mid-claim owner is gone, so they reap like any
/// other dead root; a still-live claim holds its staging `.lock` (or has not yet
/// created it), so it is kept.
fn reap_dead_instance_dirs(spill_root: &Path, self_root: &Path) {
    let entries = match std::fs::read_dir(spill_root) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path == self_root {
            continue;
        }
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        // Try to take the peer's exclusive advisory lock. A missing sentinel or
        // any open error leaves the root untouched (err toward keeping).
        let lock_path = path.join(SPILL_LOCK_FILE);
        let Ok(file) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
        else {
            continue;
        };
        // ONLY an exclusive lock we can actually acquire proves no live holder.
        // Every error (WouldBlock or otherwise) keeps the root.
        if FileExt::try_lock_exclusive(&file).is_ok() {
            remove_spill_dir(&path);
            let _ = FileExt::unlock(&file);
        }
    }
}

/// Best-effort recursive removal of a spill directory. A failure (other than
/// "already gone") is logged and swallowed: an orphaned directory is cleaned by
/// the next startup janitor and never blocks a drop/replace.
fn remove_spill_dir(dir: &Path) {
    if let Err(e) = std::fs::remove_dir_all(dir)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            dir = %dir.display(),
            error = %e,
            "failed to remove spill directory (leaving it for the next janitor)"
        );
    }
}

/// Reject a segment that cannot survive a spill round-trip (finding 3; R8/H3).
///
/// Spill mode persists a segment with [`write_segment_v9`] and re-reads it with
/// [`SegmentData::open`] (strict `read_segment_v9`). Two `__time` shapes are
/// permanently unrecoverable through that round-trip and are refused here
/// **fail-loud, BEFORE writing any bytes** — turning a latent, permanent
/// query-time failure into a clear load-time error with no orphaned spill
/// directory:
///
/// * **No `__time` column at all.** The writer emits the `__time` file ONLY when
///   the segment carries a `__time` column, and the strict reader rejects any v9
///   archive with no `__time` file (`"required column `__time` is missing"`). A
///   metric-only segment therefore round-trips to a permanently-undecodable
///   spill directory.
/// * **A non-`LONG` `__time` column (R8/H3).** The reader restores `__time` with
///   its declared column type, so a `STRING`/`DOUBLE`/… `__time` reloads as a
///   non-`LONG` column and every later interval/timestamp query on the spilled
///   copy fails forever (`timestamp_column()` rejects a non-`LONG` `__time`).
///   This is caught early with a clear reason; the generic round-trip verify in
///   [`verify_spill_roundtrip`] would not (a non-`LONG` `__time` round-trips to
///   the *same* non-`LONG` type, so the write is faithful — it is the segment
///   itself that is unqueryable).
///
/// Heap mode is unaffected: it never round-trips through disk, so it keeps
/// accepting both shapes byte-for-byte as before. This mirrors
/// [`derive_prune_interval`], which already returns `None` (no prune) for a
/// segment with no usable `__time` values.
fn ensure_spillable(segment_id: &str, segment: &SegmentData) -> Result<()> {
    match segment.columns.get("__time") {
        Some(ferrodruid_segment::column::ColumnData::Long(_)) => Ok(()),
        Some(other) => Err(DruidError::Segment(format!(
            "spill mode: refusing to admit segment '{segment_id}' — its `__time` column is \
             {kind}, not LONG; a v9 spill round-trip reloads `__time` with its declared type, \
             and a non-LONG `__time` makes every later interval/timestamp query on the spilled \
             copy fail permanently, so rejecting fail-loud at admission instead",
            kind = column_shape(other).0,
        ))),
        None => Err(DruidError::Segment(format!(
            "spill mode: refusing to admit segment '{segment_id}' — it has no `__time` column, \
             which a v9 spill round-trip requires (the writer omits `__time` and the strict \
             reload rejects the archive), so a spilled copy could never be re-decoded; rejecting \
             fail-loud at admission instead of failing every later query on it"
        ))),
    }
}

/// The `(type tag, length)` shape of a decoded column, used for order-independent
/// spill round-trip comparison ([`verify_spill_roundtrip`]) and for naming a
/// column's type in a rejection message ([`ensure_spillable`]).
fn column_shape(col: &ferrodruid_segment::column::ColumnData) -> (&'static str, usize) {
    use ferrodruid_segment::column::ColumnData;
    match col {
        ColumnData::Long(v) => ("LONG", v.len()),
        // Deliberately DISTINCT from the plain-LONG tag: a spill round-trip
        // that silently dropped the null bitmap (LongNullable → Long) must
        // trip the shape comparison, not pass as an equal "LONG".
        ColumnData::LongNullable(v, _) => ("LONG (nullable)", v.len()),
        ColumnData::Float(v) => ("FLOAT", v.len()),
        ColumnData::Double(v) => ("DOUBLE", v.len()),
        ColumnData::String(s) => ("STRING", s.encoded_values.len()),
        // Deliberately DISTINCT from the single-value STRING tag: a spill
        // round-trip that silently degraded a multi-value column to a
        // single-value one (or vice versa) must trip the shape comparison.
        ColumnData::StringMulti(s) => ("STRING (multi-value)", s.num_rows()),
        ColumnData::Complex(b) => ("COMPLEX", b.len()),
        // Deliberately DISTINCT from the opaque COMPLEX tag: a spill
        // round-trip that silently degraded a decoded theta column to an
        // opaque blob (or vice versa) must trip the shape comparison.
        ColumnData::ComplexTheta(v) => ("COMPLEX<thetaSketch>", v.len()),
    }
}

/// Verify that a segment just written to `seg_dir` survives the spill round-trip
/// (R8): re-read it with the SAME strict decoder a later query uses
/// ([`SegmentData::open`]) and confirm the decode restores `original` faithfully.
///
/// [`write_segment_v9`] is **not** a total function of [`SegmentData`]: it emits
/// only `__time` plus the declared `dimensions`/`metrics` columns (a `columns`
/// entry that is in neither list is silently dropped — R8/H1), and it writes
/// column names verbatim into the comma-delimited `meta.smoosh` index (a name
/// containing a `,` corrupts the index so the strict reader can never parse the
/// archive back — R8/H2). Each such gap is invisible at write time but makes a
/// spilled segment materialize wrong-or-never at query time, permanently.
///
/// Rather than enumerate every writer gap one-by-one (the next latent one would
/// slip through), spill admission structurally guarantees the invariant
/// "a spilled segment re-reads to its original": it writes, immediately re-reads,
/// and rejects the admission **fail-loud** if the round-trip is not faithful.
/// This closes the whole class — present and future writer fidelity gaps — as a
/// loud load-time reject instead of silent query-time data loss. Heap mode never
/// round-trips through disk and is entirely unaffected.
///
/// The `time_sorted` flag is deliberately NOT compared: the reader **re-derives**
/// it from the actual `__time` values, so a caller that under- or over-claims it
/// (see the lying-`time_sorted` tests) would be falsely rejected, and the spilled
/// copy's re-derived flag is authoritative anyway. Everything compared here
/// round-trips exactly for a faithful write.
fn verify_spill_roundtrip(segment_id: &str, original: &SegmentData, seg_dir: &Path) -> Result<()> {
    use ferrodruid_segment::column::ColumnData;

    // A decode failure (e.g. the H2 comma-corrupted `meta.smoosh`, or any other
    // corruption) is an immediate reject: a later query would hit the same
    // failure forever.
    let read_back = SegmentData::open(seg_dir).map_err(|e| {
        DruidError::Segment(format!(
            "spill round-trip verify: segment '{segment_id}' could not be re-read after being \
             written (a later query would fail identically); rejecting at admission fail-loud: {e}"
        ))
    })?;

    let mismatch = |what: &str| -> Result<()> {
        Err(DruidError::Segment(format!(
            "spill round-trip verify: segment '{segment_id}' is not faithfully restored by a v9 \
             write→read round-trip ({what}); a spilled query would diverge from the original, so \
             rejecting the admission fail-loud"
        )))
    };

    if read_back.num_rows != original.num_rows {
        return mismatch(&format!(
            "row count {} → {}",
            original.num_rows, read_back.num_rows
        ));
    }
    if read_back.interval != original.interval {
        return mismatch("time interval changed");
    }
    if read_back.dimensions != original.dimensions {
        return mismatch("dimension list changed");
    }
    if read_back.metrics != original.metrics {
        return mismatch("metric list changed");
    }

    // Column NAME set, order-independent — catches a column silently dropped by
    // the writer because it is in `columns` but neither a dimension nor a metric
    // (R8/H1).
    let orig_names: std::collections::BTreeSet<&str> =
        original.columns.keys().map(String::as_str).collect();
    let read_names: std::collections::BTreeSet<&str> =
        read_back.columns.keys().map(String::as_str).collect();
    if orig_names != read_names {
        return mismatch(&format!(
            "column set changed (wrote {orig_names:?}, read back {read_names:?})"
        ));
    }

    // Per-column type + length. The name sets are equal, so every original
    // column is present in the read-back.
    for (name, orig_col) in &original.columns {
        let Some(read_col) = read_back.columns.get(name) else {
            return mismatch(&format!("column `{name}` missing after round-trip"));
        };
        if column_shape(orig_col) != column_shape(read_col) {
            return mismatch(&format!(
                "column `{name}` type/length {:?} → {:?}",
                column_shape(orig_col),
                column_shape(read_col)
            ));
        }
    }

    // The query-critical `__time` values must round-trip exactly (ensured LONG by
    // `ensure_spillable`); this also makes the reader's re-derived `time_sorted`
    // authoritative for the spilled copy.
    if let (Some(ColumnData::Long(orig_ts)), Some(ColumnData::Long(read_ts))) = (
        original.columns.get("__time"),
        read_back.columns.get("__time"),
    ) && orig_ts != read_ts
    {
        return mismatch("`__time` values changed after round-trip");
    }

    Ok(())
}

/// Derive the interval used for query-time interval pruning from the segment's
/// ACTUAL `__time` column — never from its declared (unverified) header
/// interval (finding 1).
///
/// The v9 read path copies `index.drd`'s declared `[min_ts][max_ts]` into
/// [`SegmentData::interval`] verbatim, without checking it against the real
/// `__time` values. Trusting that header for pruning lets a segment whose
/// header lies (e.g. declares 2030 over rows that are really 2024) be pruned
/// out of a query its real rows match, silently dropping those rows. Deriving
/// the prune bound from the real `__time` min/max makes such a mis-prune
/// structurally impossible.
///
/// Returns `None` when the segment carries no usable `__time` values (absent /
/// non-`LONG` column, or zero rows): the entry then has no prune interval and
/// always executes, so a missing time column can never trigger a false prune.
///
/// The min/max is ALWAYS computed by a full O(n) scan of the real `__time`
/// values — the segment's `time_sorted` flag is deliberately NOT trusted
/// (finding 1). `time_sorted` is a public, caller-mutable field: a caller that
/// sets it `true` without actually sorting `__time` (e.g. rows `[2030, 2024,
/// 2030]`) would, under a first/last shortcut, derive a `2030`-only interval
/// and prune a matching `2024` query — silently dropping the middle row (and
/// the all-pruned force-one fallback does NOT save it when an honest peer
/// segment already executed). A single unconditional scan makes that
/// structurally impossible; it is paid once at load, never on the query hot
/// path.
fn derive_prune_interval(segment: &SegmentData) -> Option<Interval> {
    let ferrodruid_segment::column::ColumnData::Long(times) = segment.columns.get("__time")? else {
        return None;
    };
    let mut iter = times.iter().copied();
    let first = iter.next()?;
    let (start_millis, end_millis) =
        iter.fold((first, first), |(lo, hi), t| (lo.min(t), hi.max(t)));
    Some(Interval {
        start_millis,
        end_millis,
    })
}

/// Compute the correct [`SegmentData::time_sorted`] from the segment's ACTUAL
/// `__time` column, ignoring whatever the caller supplied. Consulted at the heap
/// load boundary ([`SegmentEntry::resident`]) so a heap-resident segment and the
/// same segment spilled-and-reloaded report the identical sortedness.
///
/// This is a PURE query (it does not mutate), so `resident` can compare the
/// caller's flag against the truth and write it through [`Arc::make_mut`] only
/// when they differ — reconciling a shared `Arc` copy-on-write while leaving an
/// already-correct (well-formed) segment untouched, clone-free, even when its
/// `Arc` is shared (FG-7 R21).
///
/// `time_sorted` is a public, caller-mutable field. The query executor trusts
/// it to binary-range-prune (`pruned_row_range`) a single-interval query to a
/// `[lo, hi)` sub-range via `partition_point` instead of scanning every row.
/// `partition_point` is only correct on ASCENDING data: a segment whose flag
/// claims `true` over rows that are not actually ascending (e.g. the timestamps
/// `[2030, 2024, 2030]`) makes that binary search select a wrong row range, so
/// a heap query drops or mis-counts rows. The SAME segment spilled to disk
/// reloads with the flag re-derived from its persisted `__time` (the v9/FDX
/// reader computes `is_sorted`), so the spilled copy full-scans and answers
/// correctly. Reconciling the heap flag from this value removes that
/// residency-dependent answer (FG-7 R19/R21: heap wrong, spill right).
///
/// The derivation is IDENTICAL to the v9/FDX reader's
/// (`matches!(.., Some(Long(v)) if v.is_sorted())`), so a heap segment
/// reconciled from this and its spilled reload always agree. A well-formed
/// segment (built by [`SegmentDataBuilder`](ferrodruid_segment::SegmentDataBuilder)
/// or decoded by a v9/FDX read) already carries this exact flag, so the
/// reconciliation is a no-op and leaves the segment byte-for-byte unchanged; it
/// only corrects a caller-installed lie. A segment with no `__time`, or a
/// non-`LONG` `__time`, is `false` (there is no timestamp range to binary
/// search anyway), matching the builder's no-timestamp default and the reader.
fn derived_time_sorted(segment: &SegmentData) -> bool {
    matches!(
        segment.columns.get("__time"),
        Some(ferrodruid_segment::column::ColumnData::Long(times)) if times.is_sorted()
    )
}

/// Does the segment's inclusive `[start, end]` interval intersect any of the
/// query's half-open `[start, end)` ranges? The segment bound is treated as
/// inclusive on both ends (a conservative over-estimate for on-disk segments
/// whose declared end is exclusive), so this can only ever KEEP a segment that
/// might match — never prune one that could.
fn interval_intersects(seg: &Interval, ranges: &[(i64, i64)]) -> bool {
    ranges
        .iter()
        .any(|&(start, end)| seg.start_millis < end && start <= seg.end_millis)
}

/// Extract the parsed `[start, end)` ranges a query filters rows by, for
/// interval-based segment pruning — but only for the **aggregating** query
/// types whose result columns are defined by the query itself (timeseries /
/// topN / groupBy). For those, a routed segment that matches zero rows
/// contributes a true no-op empty partial, so pruning it can never change the
/// answer (and the all-pruned force-one fallback still synthesizes zero-fill
/// buckets / runs query-level validation; see `execute_query_on_snapshot`).
///
/// **Scan and Search are deliberately EXCLUDED (R10 HIGH).** Their result shape
/// is *segment-derived*: every routed segment contributes its column schema to
/// the result even when it matches zero rows (a scan partial carries its
/// declared `columns` with empty events; a search reports its searched
/// dimensions per bucket). Interval pruning is gated to spill mode, so pruning a
/// disjoint scan/search segment would make the answer depend on residency — a
/// heterogeneous-schema datasource would drop a disjoint segment's schema-unique
/// column in spill mode but keep it in heap mode. Returning `None` for them
/// keeps spill scan/search executing every routed segment exactly like heap
/// (correctness over the prune's decode saving).
///
/// Returns `None` (meaning "do not prune, materialize every candidate") when
/// the query carries no intervals (== all time), when any interval string does
/// not parse (the executor re-parses and surfaces the error), or for a query
/// type (including scan/search) whose interval semantics are not prune-eligible
/// here. Because a `None` or a non-intersection is the only signal that can skip
/// a segment, pruning is strictly conservative: it can never change an answer,
/// only avoid work.
fn query_prune_intervals(query: &DruidQuery) -> Option<Vec<(i64, i64)>> {
    let raw: &[String] = match query {
        DruidQuery::Timeseries(q) => &q.intervals,
        DruidQuery::TopN(q) => &q.intervals,
        DruidQuery::GroupBy(q) => &q.intervals,
        // Scan/Search return a segment-derived result shape — never prune them
        // (R10 HIGH); see the doc comment above. Handled explicitly (not by the
        // catch-all) so a future query type is a compile-visible decision.
        DruidQuery::Scan(_) | DruidQuery::Search(_) => return None,
        _ => return None,
    };
    if raw.is_empty() {
        return None;
    }
    let mut ranges = Vec::with_capacity(raw.len());
    for interval in raw {
        let (start, end) = interval.split_once('/')?;
        ranges.push((
            ferrodruid_query::parse_iso_millis(start)?,
            ferrodruid_query::parse_iso_millis(end)?,
        ));
    }
    Some(ranges)
}

/// Estimate the in-memory size of a segment in bytes.
///
/// Includes capacity-aware heap allocations behind columns, string
/// dictionaries, bitmap indexes, column names, dimensions, and metrics.
fn estimate_segment_bytes(segment: &SegmentData) -> u64 {
    fn string_vec_bytes(values: &[String], capacity: usize) -> u64 {
        values
            .iter()
            .fold(allocation_bytes::<String>(capacity), |total, value| {
                total.saturating_add(u64::try_from(value.capacity()).unwrap_or(u64::MAX))
            })
    }

    let mut bytes = u64::try_from(std::mem::size_of::<SegmentData>()).unwrap_or(u64::MAX);
    // Charge the segment's OWN internal columns map raw bucket table (part of
    // the segment's heap footprint), so a segment with many tiny columns
    // cannot slip its bucket allocation under the cache limit. (This is the
    // only remaining `hash_table_bytes` use — the Historical's outer routing
    // maps are no longer charged; see `HistoricalInfo::cache_bytes_used`.)
    bytes = bytes.saturating_add(hash_table_bytes::<(
        String,
        ferrodruid_segment::column::ColumnData,
    )>(segment.columns.capacity()));
    bytes = bytes.saturating_add(string_vec_bytes(
        &segment.dimensions,
        segment.dimensions.capacity(),
    ));
    bytes = bytes.saturating_add(string_vec_bytes(
        &segment.metrics,
        segment.metrics.capacity(),
    ));
    for (name, col) in &segment.columns {
        bytes = bytes.saturating_add(u64::try_from(name.capacity()).unwrap_or(u64::MAX));
        let column_bytes = match col {
            ferrodruid_segment::column::ColumnData::Long(v) => {
                allocation_bytes::<i64>(v.capacity())
            }
            ferrodruid_segment::column::ColumnData::LongNullable(v, nulls) => {
                allocation_bytes::<i64>(v.capacity()).saturating_add(nulls.estimated_heap_bytes())
            }
            ferrodruid_segment::column::ColumnData::Float(v) => {
                allocation_bytes::<f32>(v.capacity())
            }
            ferrodruid_segment::column::ColumnData::Double(v) => {
                allocation_bytes::<f64>(v.capacity())
            }
            ferrodruid_segment::column::ColumnData::String(sc) => {
                let mut string_bytes = allocation_bytes::<u32>(sc.encoded_values.capacity())
                    .saturating_add(allocation_bytes::<ferrodruid_bitmap::DruidBitmap>(
                        sc.bitmap_indexes.capacity(),
                    ))
                    .saturating_add(sc.dictionary.estimated_heap_bytes());
                for bitmap in &sc.bitmap_indexes {
                    string_bytes = string_bytes.saturating_add(bitmap.estimated_heap_bytes());
                }
                string_bytes
            }
            ferrodruid_segment::column::ColumnData::StringMulti(mc) => {
                let mut string_bytes = allocation_bytes::<u32>(mc.ordinals.capacity())
                    .saturating_add(allocation_bytes::<u32>(mc.row_offsets.capacity()))
                    .saturating_add(allocation_bytes::<ferrodruid_bitmap::DruidBitmap>(
                        mc.bitmap_indexes.capacity(),
                    ))
                    .saturating_add(mc.dictionary.estimated_heap_bytes());
                for bitmap in &mc.bitmap_indexes {
                    string_bytes = string_bytes.saturating_add(bitmap.estimated_heap_bytes());
                }
                string_bytes
            }
            ferrodruid_segment::column::ColumnData::Complex(v) => {
                allocation_bytes::<u8>(v.capacity())
            }
            // Decoded per-row theta column: the Vec of sketch structs plus
            // each sketch's retained-hash set.  A `BTreeSet<u64>` node
            // costs well over the 8 value bytes, so charge 2 words per
            // retained hash (conservative, same spirit as the bitmap
            // estimates above).
            ferrodruid_segment::column::ColumnData::ComplexTheta(v) => {
                let mut theta_bytes =
                    allocation_bytes::<ferrodruid_segment::column::ThetaSketch>(v.capacity());
                for sketch in v {
                    theta_bytes = theta_bytes.saturating_add(
                        u64::try_from(sketch.retained())
                            .unwrap_or(u64::MAX)
                            .saturating_mul(16),
                    );
                }
                theta_bytes
            }
        };
        bytes = bytes.saturating_add(column_bytes);
    }
    bytes
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use ferrodruid_bitmap::DruidBitmap;
    use ferrodruid_dict::FrontCodedDictionary;
    use ferrodruid_segment::Interval;
    use ferrodruid_segment::column::{ColumnData, StringColumnData};

    /// Build a synthetic segment for a given data source name.
    ///
    /// 6 rows:
    ///   __time:  day1, day1, day1, day2, day2, day2
    ///   region:  us,   us,   eu,   eu,   jp,   us
    ///   value:   10.0, 20.0, 30.0, 40.0, 50.0, 60.0
    fn build_test_segment() -> SegmentData {
        let day1 = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("parse")
            .timestamp_millis();
        let day2 = chrono::DateTime::parse_from_rfc3339("2024-01-02T00:00:00Z")
            .expect("parse")
            .timestamp_millis();

        let timestamps = vec![day1, day1, day1, day2, day2, day2];

        let dict = FrontCodedDictionary::from_sorted(vec![
            "eu".to_string(),
            "jp".to_string(),
            "us".to_string(),
        ]);
        let encoded_values = vec![2, 2, 0, 0, 1, 2];
        let mut bm_eu = DruidBitmap::new();
        bm_eu.insert(2);
        bm_eu.insert(3);
        let mut bm_jp = DruidBitmap::new();
        bm_jp.insert(4);
        let mut bm_us = DruidBitmap::new();
        bm_us.insert(0);
        bm_us.insert(1);
        bm_us.insert(5);
        let region_col = ColumnData::String(StringColumnData {
            dictionary: dict,
            encoded_values,
            bitmap_indexes: vec![bm_eu, bm_jp, bm_us],
        });

        let value_col = ColumnData::Double(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);

        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(timestamps));
        columns.insert("region".to_string(), region_col);
        columns.insert("value".to_string(), value_col);

        let start = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("parse")
            .timestamp_millis();
        let end = chrono::DateTime::parse_from_rfc3339("2024-01-03T00:00:00Z")
            .expect("parse")
            .timestamp_millis();

        SegmentData {
            version: 9,
            num_rows: 6,
            interval: Interval {
                start_millis: start,
                end_millis: end,
            },
            dimensions: vec!["region".to_string()],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: false,
        }
    }

    #[test]
    fn load_and_list_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);

        let segment = build_test_segment();
        hist.load_segment("seg_0", segment).expect("load");

        assert!(hist.has_segment("seg_0"));
        assert!(!hist.has_segment("seg_1"));
        assert_eq!(hist.segment_count(), 1);

        let ids = hist.loaded_segments();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&"seg_0".to_string()));
    }

    fn dim_names(c: &DatasourceColumns) -> Vec<&str> {
        c.dimensions.iter().map(|(n, _)| n.as_str()).collect()
    }

    fn metric_names(c: &DatasourceColumns) -> Vec<&str> {
        c.metrics.iter().map(|(n, _)| n.as_str()).collect()
    }

    /// Names of the shared emit helper's ordered column list, in order.
    fn ordered_names(c: &DatasourceColumns) -> Vec<String> {
        c.ordered_columns()
            .into_iter()
            .map(|(name, _, _)| name)
            .collect()
    }

    /// The single emit helper leads with `__time` exactly once when the
    /// datasource has ANY timed segment, then the deduped dimensions, then the
    /// deduped metrics — the canonical order every schema consumer emits.
    #[test]
    fn ordered_columns_puts_time_first_when_has_time() {
        let cols = DatasourceColumns {
            has_time: true,
            dimensions: vec![("region".to_string(), ColumnType::String)],
            metrics: vec![("value".to_string(), ColumnType::Double)],
        };
        let ordered = cols.ordered_columns();
        assert_eq!(
            ordered_names(&cols),
            vec!["__time", "region", "value"],
            "__time must be first exactly once"
        );
        assert_eq!(ordered[0].1, ColumnType::Long, "__time is a LONG");
        assert_eq!(ordered[0].2, ColumnRole::Time);
        assert_eq!(ordered[1].2, ColumnRole::Dimension);
        assert_eq!(ordered[2].2, ColumnRole::Metric);
    }

    /// The single emit helper OMITS `__time` entirely for a time-less-only
    /// datasource (`has_time == false`) — the SAME decision `build_schema_for`
    /// and `enumerate_datasources` now inherit, so they cannot diverge.
    #[test]
    fn ordered_columns_omits_time_when_timeless() {
        let cols = DatasourceColumns {
            has_time: false,
            dimensions: vec![("a".to_string(), ColumnType::Long)],
            metrics: vec![],
        };
        assert_eq!(ordered_names(&cols), vec!["a"]);
        assert!(
            !cols.ordered_columns().iter().any(|(n, _, _)| n == "__time"),
            "a time-less-only datasource must not surface __time"
        );
    }

    /// FG-7 R17 (b) at the helper level: merging a time-less historical
    /// (dimension `a`, FIRST) with a timed one (metric `m`) ORs `has_time` true
    /// and, crucially, the shared emit helper still leads with `__time` — the
    /// merge order can no longer bury it as `[a, __time]`.
    #[test]
    fn merged_ors_has_time_and_emits_time_first_regardless_of_order() {
        let timeless_first = DatasourceColumns {
            has_time: false,
            dimensions: vec![("a".to_string(), ColumnType::Long)],
            metrics: vec![],
        };
        let timed_second = DatasourceColumns {
            has_time: true,
            dimensions: vec![],
            metrics: vec![("m".to_string(), ColumnType::Double)],
        };
        let merged = DatasourceColumns::merged([&timeless_first, &timed_second]);
        assert!(merged.has_time, "has_time must OR to true");
        assert_eq!(
            ordered_names(&merged),
            vec!["__time", "a", "m"],
            "__time must lead even when the time-less part is merged first"
        );
    }

    /// The cross-historical merge unions columns under a SINGLE cross-role dedup:
    /// a name that is a dimension in one part and a metric in another is kept
    /// EXACTLY once (dimension, first-wins), while non-colliding columns survive.
    #[test]
    fn merged_single_cross_role_dedup() {
        let h1 = DatasourceColumns {
            has_time: true,
            dimensions: vec![("a".to_string(), ColumnType::Long)],
            metrics: vec![("x".to_string(), ColumnType::Double)],
        };
        let h2 = DatasourceColumns {
            has_time: true,
            // `a` collides (metric here) and must NOT duplicate; `y` is new.
            dimensions: vec![],
            metrics: vec![
                ("a".to_string(), ColumnType::Double),
                ("y".to_string(), ColumnType::Double),
            ],
        };
        let merged = DatasourceColumns::merged([&h1, &h2]);
        assert_eq!(dim_names(&merged), vec!["a"], "a resolves to a dimension");
        assert_eq!(
            metric_names(&merged),
            vec!["x", "y"],
            "a must not re-appear as a metric; x and y both survive"
        );
    }

    /// Schema-discovery TOCTOU fix (R13 sibling): `schema_for_datasource`
    /// resolves datasource MEMBERSHIP and the (cached) schema under ONE
    /// consistent lock view, default-denying non-members. The membership
    /// judgment and the columns it yields move TOGETHER across a remap — the
    /// property the old two-call lookup (`segment_datasource` THEN `get_segment`)
    /// could not provide atomically.
    #[test]
    fn schema_for_datasource_atomic_membership_heap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);
        hist.load_segment("seg", build_test_segment())
            .expect("load");
        hist.set_segment_datasource("seg", "A").expect("map A");

        // Mapped to A: A sees region/value, B is default-denied (empty).
        let a = hist.schema_for_datasource("A").expect("A schema");
        assert!(a.has_time, "A must surface __time");
        assert_eq!(dim_names(&a), vec!["region"]);
        assert_eq!(metric_names(&a), vec!["value"]);
        let b = hist.schema_for_datasource("B").expect("B schema");
        assert!(
            !b.has_time && b.dimensions.is_empty() && b.metrics.is_empty(),
            "segment mapped to A must be invisible for datasource B"
        );

        // Remap A→B: membership judgment and columns move together.
        hist.set_segment_datasource("seg", "B").expect("remap B");
        let a2 = hist.schema_for_datasource("A").expect("A schema 2");
        assert!(
            a2.dimensions.is_empty() && a2.metrics.is_empty(),
            "after remap to B, datasource A must see nothing"
        );
        let b2 = hist.schema_for_datasource("B").expect("B schema 2");
        assert_eq!(dim_names(&b2), vec!["region"]);
        assert_eq!(metric_names(&b2), vec!["value"]);

        // A never-loaded datasource is empty.
        let ghost = hist.schema_for_datasource("ghost").expect("ghost schema");
        assert!(ghost.dimensions.is_empty() && ghost.metrics.is_empty());
    }

    /// UNION (R14 HIGH, the fix): a datasource whose columns are spread across
    /// MULTIPLE segments must surface the UNION of every segment's columns, not
    /// one representative's. Here `d` has `s1` (metric `a`) and `s2` (metric
    /// `b`); `schema_for_datasource("d")` must report BOTH — and deterministically
    /// (segment-id-sorted, HashMap-order independent). The pre-fix one-per-ds
    /// dedup dropped whichever segment lost the HashMap-order race.
    #[test]
    fn schema_for_datasource_unions_columns_across_segments_heap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 50_000_000);
        hist.load_segment("s1", build_named_metric_segment(0, "a", 3))
            .expect("load s1");
        hist.set_segment_datasource("s1", "d").expect("map s1");
        hist.load_segment("s2", build_named_metric_segment(0, "b", 2))
            .expect("load s2");
        hist.set_segment_datasource("s2", "d").expect("map s2");

        let d = hist.schema_for_datasource("d").expect("d schema");
        assert!(d.has_time);
        // Deterministic union, both segments' metrics present.
        assert_eq!(
            metric_names(&d),
            vec!["a", "b"],
            "union must include BOTH segments' columns, in stable seg-id order"
        );
    }

    /// UNION in spill mode: the same schema-evolution union holds when the
    /// segments are spilled to disk — the union is served from the load-time
    /// cache, so no disk decode is needed.
    #[test]
    fn schema_for_datasource_unions_columns_across_segments_spill() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::with_options(dir.path().to_path_buf(), 50_000_000, false, true);
        hist.load_segment_with_datasource("s1", "d", build_named_metric_segment(0, "a", 3))
            .expect("load+map s1");
        hist.load_segment_with_datasource("s2", "d", build_named_metric_segment(0, "b", 2))
            .expect("load+map s2");

        let d = hist.schema_for_datasource("d").expect("d schema");
        assert_eq!(metric_names(&d), vec!["a", "b"]);
    }

    /// Same-name dimension/metric collision (R15 HIGH): a column name is unique
    /// within a datasource, so the union must yield EXACTLY ONE entry for it —
    /// never once as a dimension AND once as a metric. Here `d` has `s1` (column
    /// `a` as a STRING **dimension**) and `s2` (column `a` as a DOUBLE
    /// **metric**). The separate dim/metric dedup sets leaked `a` into BOTH the
    /// dimensions and metrics buckets (RED — INFORMATION_SCHEMA reported `a`
    /// twice with conflicting definitions). The single cross-role seen-set fixes
    /// it: `s1 < s2` in id order and the dimension role is visited first, so `a`
    /// resolves to a lone STRING **dimension** and never reappears as a metric.
    #[test]
    fn schema_for_datasource_same_name_dim_metric_deduped_to_one_heap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 50_000_000);
        hist.load_segment("s1", build_named_dim_segment(0, "a", "x", 3))
            .expect("load s1 (dim a)");
        hist.set_segment_datasource("s1", "d").expect("map s1");
        hist.load_segment("s2", build_named_metric_segment(0, "a", 2))
            .expect("load s2 (metric a)");
        hist.set_segment_datasource("s2", "d").expect("map s2");

        let d = hist.schema_for_datasource("d").expect("d schema");
        // First-wins by segment-id order (s1), dimension role visited first:
        // `a` is a lone STRING dimension, absent from metrics — one entry total.
        assert_eq!(dim_names(&d), vec!["a"]);
        assert!(
            !metric_names(&d).contains(&"a"),
            "a name claimed as a dimension must not ALSO surface as a metric: {:?}",
            metric_names(&d)
        );
        assert_eq!(d.dimensions.len(), 1);
        assert!(d.metrics.is_empty(), "no duplicate metric entry for `a`");
        assert_eq!(
            d.dimensions[0].1,
            ColumnType::String,
            "the winning (s1 dimension) role fixes the type"
        );
    }

    /// Same-name dim/metric collision, spill mode: the single-entry-per-name
    /// invariant holds when both colliding segments are spilled (the union is
    /// served from the load-time cache, no decode).
    #[test]
    fn schema_for_datasource_same_name_dim_metric_deduped_to_one_spill() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::with_options(dir.path().to_path_buf(), 50_000_000, false, true);
        hist.load_segment_with_datasource("s1", "d", build_named_dim_segment(0, "a", "x", 3))
            .expect("load+map s1 (dim a)");
        hist.load_segment_with_datasource("s2", "d", build_named_metric_segment(0, "a", 2))
            .expect("load+map s2 (metric a)");

        let d = hist.schema_for_datasource("d").expect("d schema");
        assert_eq!(dim_names(&d), vec!["a"]);
        assert!(d.metrics.is_empty(), "no duplicate metric entry for `a`");
    }

    /// Non-colliding union regression: distinct dimension and metric names still
    /// BOTH survive (the collision fix must not collapse legitimately different
    /// columns). `s1` carries dimension `dim_a`, `s2` metric `met_b`.
    #[test]
    fn schema_for_datasource_distinct_dim_and_metric_both_kept_heap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 50_000_000);
        hist.load_segment("s1", build_named_dim_segment(0, "dim_a", "x", 3))
            .expect("load s1");
        hist.set_segment_datasource("s1", "d").expect("map s1");
        hist.load_segment("s2", build_named_metric_segment(0, "met_b", 2))
            .expect("load s2");
        hist.set_segment_datasource("s2", "d").expect("map s2");

        let d = hist.schema_for_datasource("d").expect("d schema");
        assert_eq!(dim_names(&d), vec!["dim_a"]);
        assert_eq!(metric_names(&d), vec!["met_b"]);
    }

    /// FG-7 R16 HIGH: `__time` is the time column (surfaced via `has_time`) and
    /// must NEVER ALSO appear among a datasource's dimensions/metrics, or a
    /// consumer that emits the `has_time` time column plus the dimension list
    /// reports `__time` TWICE. A defensively-constructed segment lists `__time`
    /// as a dimension; `schema_for_datasource` must still surface it ONLY as the
    /// time column (RED before the `CachedSchema::from_segment` `__time` filter:
    /// `dim_names` contained `__time`). The genuine `region`/`value` columns are
    /// unaffected.
    #[test]
    fn schema_for_datasource_excludes_time_from_dimensions_heap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 50_000_000);
        hist.load_segment("s", build_time_in_dims_segment())
            .expect("load s");
        hist.set_segment_datasource("s", "d").expect("map s");

        let d = hist.schema_for_datasource("d").expect("d schema");
        assert!(d.has_time, "the segment carries __time as its time column");
        assert!(
            !dim_names(&d).contains(&"__time"),
            "__time is the time column, never a dimension: {:?}",
            dim_names(&d)
        );
        assert!(
            !metric_names(&d).contains(&"__time"),
            "__time is the time column, never a metric: {:?}",
            metric_names(&d)
        );
        assert_eq!(dim_names(&d), vec!["region"], "genuine dimension survives");
        assert_eq!(metric_names(&d), vec!["value"], "genuine metric survives");
    }

    /// Spill-mode counterpart: the load-time `CachedSchema` (captured from the
    /// in-memory segment BEFORE the disk write) applies the same `__time`
    /// exclusion, so a spilled segment whose dimensions list `__time` still
    /// surfaces it only as the time column.
    #[test]
    fn schema_for_datasource_excludes_time_from_dimensions_spill() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::with_options(dir.path().to_path_buf(), 50_000_000, false, true);
        hist.load_segment_with_datasource("s", "d", build_time_in_dims_segment())
            .expect("load+map s");

        let d = hist.schema_for_datasource("d").expect("d schema");
        assert!(d.has_time);
        assert!(
            !dim_names(&d).contains(&"__time"),
            "__time must not be a dimension (spill): {:?}",
            dim_names(&d)
        );
        assert_eq!(dim_names(&d), vec!["region"]);
        assert_eq!(metric_names(&d), vec!["value"]);
    }

    /// `datasource_schemas` (the all-datasources enumeration building block)
    /// applies the same `__time` exclusion — it shares `union_cached_schemas`
    /// with `schema_for_datasource`.
    #[test]
    fn datasource_schemas_excludes_time_from_dimensions_heap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 50_000_000);
        hist.load_segment("s", build_time_in_dims_segment())
            .expect("load s");
        hist.set_segment_datasource("s", "d").expect("map s");

        let all = hist.datasource_schemas().expect("schemas");
        let (_, d) = all.iter().find(|(name, _)| name == "d").expect("d present");
        assert!(d.has_time);
        assert!(
            !dim_names(d).contains(&"__time"),
            "__time must not be a dimension: {:?}",
            dim_names(d)
        );
        assert_eq!(dim_names(d), vec!["region"]);
        assert_eq!(metric_names(d), vec!["value"]);
    }

    /// A regular segment (whose `__time` is the time column ONLY, never in its
    /// dimensions) keeps surfacing `__time` exactly once as the time column —
    /// the `__time` filter must not disturb the normal path.
    #[test]
    fn schema_for_datasource_regular_segment_time_unaffected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 50_000_000);
        hist.load_segment("s", build_test_segment())
            .expect("load s");
        hist.set_segment_datasource("s", "d").expect("map s");

        let d = hist.schema_for_datasource("d").expect("d schema");
        assert!(d.has_time);
        assert_eq!(dim_names(&d), vec!["region"]);
        assert_eq!(metric_names(&d), vec!["value"]);
        assert!(!dim_names(&d).contains(&"__time"));
    }

    /// Improvement over the retired data-returning accessor: because the schema
    /// is captured (and spill-round-trip-verified) at LOAD, a spilled segment
    /// whose on-disk bytes are later corrupted still serves its columns from the
    /// cache — schema discovery never decodes, so it neither errors nor 500s.
    #[test]
    fn schema_for_datasource_served_from_cache_despite_corrupt_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::with_options(dir.path().to_path_buf(), 10_000_000, false, true);
        hist.load_segment_with_datasource("s", "target", build_named_metric_segment(0, "m", 3))
            .expect("load+map");
        // Corrupt the spilled bytes so ANY decode attempt would error.
        std::fs::remove_dir_all(dir.path().join("spill")).expect("corrupt spill");

        // Schema is decode-free: the mapped datasource still reports its column,
        // and an unrelated datasource is cleanly empty (default-deny).
        let target = hist
            .schema_for_datasource("target")
            .expect("cached schema needs no decode");
        assert_eq!(metric_names(&target), vec!["m"]);
        let other = hist.schema_for_datasource("other").expect("other schema");
        assert!(other.metrics.is_empty(), "default-deny on unrelated ds");
    }

    /// All-datasources UNION + attribution (R14 HIGH): `datasource_schemas`
    /// attributes each datasource ONLY its own columns AND unions across a
    /// datasource's segments. Two `A` segments (both `a_metric`) plus one `B`
    /// segment (`b_metric`) yield exactly A→{a_metric}, B→{b_metric}.
    #[test]
    fn datasource_schemas_attributes_columns_to_correct_datasource_heap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 50_000_000);
        hist.load_segment("a1", build_named_metric_segment(0, "a_metric", 3))
            .expect("load a1");
        hist.set_segment_datasource("a1", "A").expect("map a1");
        hist.load_segment("a2", build_named_metric_segment(0, "a_metric", 3))
            .expect("load a2");
        hist.set_segment_datasource("a2", "A").expect("map a2");
        hist.load_segment("b1", build_named_metric_segment(0, "b_metric", 3))
            .expect("load b1");
        hist.set_segment_datasource("b1", "B").expect("map b1");

        let schemas = hist.datasource_schemas().expect("schemas");
        assert_eq!(schemas.len(), 2, "one row per datasource");
        // Sorted by datasource name → A first, B second.
        assert_eq!(schemas[0].0, "A");
        assert_eq!(schemas[1].0, "B");
        assert_eq!(metric_names(&schemas[0].1), vec!["a_metric"]);
        assert!(
            !metric_names(&schemas[0].1).contains(&"b_metric"),
            "A must not carry B's column"
        );
        assert_eq!(metric_names(&schemas[1].1), vec!["b_metric"]);
        assert!(
            !metric_names(&schemas[1].1).contains(&"a_metric"),
            "B must not carry A's column"
        );
    }

    /// All-datasources UNION (the fix), heap: a single datasource `d` whose
    /// columns are spread across `s1` (metric `a`) and `s2` (metric `b`) is
    /// enumerated with the UNION of both.
    #[test]
    fn datasource_schemas_unions_columns_across_segments_heap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 50_000_000);
        hist.load_segment("s1", build_named_metric_segment(0, "a", 3))
            .expect("load s1");
        hist.set_segment_datasource("s1", "d").expect("map s1");
        hist.load_segment("s2", build_named_metric_segment(0, "b", 2))
            .expect("load s2");
        hist.set_segment_datasource("s2", "d").expect("map s2");

        let schemas = hist.datasource_schemas().expect("schemas");
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].0, "d");
        assert_eq!(
            metric_names(&schemas[0].1),
            vec!["a", "b"],
            "enumeration must union both segments' columns"
        );
    }

    /// All-datasources UNION (the fix), spill mode.
    #[test]
    fn datasource_schemas_unions_columns_across_segments_spill() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::with_options(dir.path().to_path_buf(), 50_000_000, false, true);
        hist.load_segment_with_datasource("s1", "d", build_named_metric_segment(0, "a", 3))
            .expect("load+map s1");
        hist.load_segment_with_datasource("s2", "d", build_named_metric_segment(0, "b", 2))
            .expect("load+map s2");

        let schemas = hist.datasource_schemas().expect("schemas");
        assert_eq!(schemas.len(), 1);
        assert_eq!(metric_names(&schemas[0].1), vec!["a", "b"]);
    }

    /// All-datasources enumeration also enforces the one-entry-per-name invariant
    /// (R15 HIGH): with `s1` carrying `a` as a dimension and `s2` carrying `a` as
    /// a metric under the same datasource `d`, `datasource_schemas` reports `a`
    /// exactly once (dimension, first-wins by seg-id order) — INFORMATION_SCHEMA
    /// gets no duplicate row.
    #[test]
    fn datasource_schemas_same_name_dim_metric_deduped_to_one_heap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 50_000_000);
        hist.load_segment("s1", build_named_dim_segment(0, "a", "x", 3))
            .expect("load s1 (dim a)");
        hist.set_segment_datasource("s1", "d").expect("map s1");
        hist.load_segment("s2", build_named_metric_segment(0, "a", 2))
            .expect("load s2 (metric a)");
        hist.set_segment_datasource("s2", "d").expect("map s2");

        let schemas = hist.datasource_schemas().expect("schemas");
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].0, "d");
        assert_eq!(dim_names(&schemas[0].1), vec!["a"]);
        assert!(
            !metric_names(&schemas[0].1).contains(&"a"),
            "`a` must not be enumerated as both a dimension and a metric: {:?}",
            metric_names(&schemas[0].1)
        );
        assert!(schemas[0].1.metrics.is_empty());
    }

    /// A datasource remap moves the segment's ATTRIBUTION and its columns
    /// together: the enumeration before the remap attributes the segment to A,
    /// after the remap to B, never reporting the stale datasource.
    #[test]
    fn datasource_schemas_attribution_moves_with_remap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 50_000_000);
        hist.load_segment("seg", build_named_metric_segment(0, "a_metric", 3))
            .expect("load");
        hist.set_segment_datasource("seg", "A").expect("map A");

        let before = hist.datasource_schemas().expect("schemas A");
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].0, "A");
        assert_eq!(metric_names(&before[0].1), vec!["a_metric"]);

        hist.set_segment_datasource("seg", "B").expect("remap B");
        let after = hist.datasource_schemas().expect("schemas B");
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].0, "B", "after remap, the columns belong to B only");
        assert!(
            !after.iter().any(|(ds, _)| ds == "A"),
            "the stale datasource A must never be attributed the remapped segment"
        );
        assert_eq!(metric_names(&after[0].1), vec!["a_metric"]);
    }

    /// Improvement: a MAPPED-but-later-corrupted spill segment no longer breaks
    /// enumeration — its columns are served from the load-time cache, so
    /// `datasource_schemas` returns cleanly (no decode, no error), where the
    /// retired data-returning snapshot would have propagated a decode error.
    #[test]
    fn datasource_schemas_served_from_cache_despite_corrupt_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::with_options(dir.path().to_path_buf(), 10_000_000, false, true);
        hist.load_segment_with_datasource(
            "mapped",
            "A",
            build_named_metric_segment(0, "a_metric", 3),
        )
        .expect("load+map");
        std::fs::remove_dir_all(dir.path().join("spill")).expect("corrupt spill");

        let schemas = hist
            .datasource_schemas()
            .expect("cached schema needs no decode");
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].0, "A");
        assert_eq!(metric_names(&schemas[0].1), vec!["a_metric"]);
    }

    /// An UNMAPPED segment has no datasource to attribute its columns to, so it
    /// is skipped — the enumeration returns cleanly empty (mirrors the pre-atomic
    /// `segment_datasource == None` skip).
    #[test]
    fn datasource_schemas_skips_unmapped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);
        hist.load_segment("orphan", build_test_segment())
            .expect("load orphan"); // deliberately UNMAPPED

        let schemas = hist.datasource_schemas().expect("schemas");
        assert!(
            schemas.is_empty(),
            "an unmapped segment has no datasource and is skipped"
        );
    }

    /// Accounting symmetry (A1 oracle): the cached-schema heap added to
    /// `SegmentEntry` is charged through `segment_entry_bytes`, so the
    /// incremental ledger stays byte-exact against the full-map oracle across
    /// load / map / drop of a segment carrying a real (dim + metric) schema.
    #[test]
    fn schema_cache_heap_is_charged_symmetrically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), u64::MAX);

        reset_cache_state_full_folds();
        hist.load_segment("seg", build_test_segment())
            .expect("load");
        assert_no_cache_state_full_folds();
        assert_exact_cache_invariant(&hist);

        hist.set_segment_datasource("seg", "d").expect("map d");
        assert_no_cache_state_full_folds();
        assert_exact_cache_invariant(&hist);

        hist.drop_segment("seg").expect("drop");
        assert_no_cache_state_full_folds();
        assert_exact_cache_invariant(&hist);
    }

    #[test]
    fn drop_segment_removes_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);

        hist.load_segment("seg_0", build_test_segment())
            .expect("load");
        assert_eq!(hist.segment_count(), 1);

        hist.drop_segment("seg_0").expect("drop");
        assert_eq!(hist.segment_count(), 0);
        assert!(!hist.has_segment("seg_0"));
    }

    #[test]
    fn drop_nonexistent_segment_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);

        let result = hist.drop_segment("no_such_segment");
        assert!(result.is_err());
    }

    #[test]
    fn execute_timeseries_against_loaded_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);

        hist.load_segment("seg_0", build_test_segment())
            .expect("load");
        hist.set_segment_datasource("seg_0", "wiki")
            .expect("set ds");

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"count","name":"cnt"},
                    {"type":"doubleSum","name":"total","fieldName":"value"}
                ]
            }"#,
        )
        .expect("parse query");

        let results = hist.execute_query(&query).expect("execute");
        assert_eq!(results.len(), 1);

        match &results[0] {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts.len(), 1);
                assert_eq!(ts[0].result.get("cnt"), Some(&serde_json::json!(6)));
                assert_eq!(ts[0].result.get("total"), Some(&serde_json::json!(210.0)));
            }
            _ => panic!("expected timeseries result"),
        }
    }

    #[test]
    fn query_with_no_matching_segments_returns_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);

        // Load a segment for "wiki".
        hist.load_segment("seg_0", build_test_segment())
            .expect("load");
        hist.set_segment_datasource("seg_0", "wiki")
            .expect("set ds");

        // Query for a different data source.
        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"clicks"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        )
        .expect("parse query");

        let results = hist.execute_query(&query).expect("execute");
        assert!(results.is_empty());
    }

    #[test]
    fn multiple_datasources_query_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);

        // Load two segments, one for "wiki" and one for "clicks".
        hist.load_segment("wiki_seg", build_test_segment())
            .expect("load wiki");
        hist.set_segment_datasource("wiki_seg", "wiki")
            .expect("set ds");

        hist.load_segment("clicks_seg", build_test_segment())
            .expect("load clicks");
        hist.set_segment_datasource("clicks_seg", "clicks")
            .expect("set ds");

        assert_eq!(hist.segment_count(), 2);

        // Query only wiki.
        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        )
        .expect("parse query");

        let results = hist.execute_query(&query).expect("execute");
        // Only one segment matches.
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn historical_info() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 5_000_000);

        hist.load_segment("seg_0", build_test_segment())
            .expect("load");

        let info = hist.info();
        assert_eq!(info.segment_count, 1);
        assert!(info.cache_bytes_used > 0);
        assert_eq!(info.cache_bytes_max, 5_000_000);
    }

    /// Wave 36-D / R1: cross-datasource isolation.
    ///
    /// Loading "seg_a" into datasource "DS-A" must not cause it to surface
    /// in a query for datasource "DS-B" — even though both segments are
    /// resident in the same `Historical` process. Internal security review
    /// (Wave 35 R1), High: "Historical serves unmapped segments to any
    /// datasource query".
    #[test]
    fn segment_in_ds_a_not_visible_in_ds_b() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);

        // Load a segment into DS-A.
        hist.load_segment("seg_a", build_test_segment())
            .expect("load seg_a");
        hist.set_segment_datasource("seg_a", "DS-A")
            .expect("set ds for seg_a");

        // Query DS-B (a completely different datasource that has no segments).
        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"DS-B"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        )
        .expect("parse query");

        let results = hist.execute_query(&query).expect("execute");
        assert!(
            results.is_empty(),
            "DS-B query must not see DS-A's segment, got {} result(s)",
            results.len()
        );

        // Repeat the experiment with an UNMAPPED segment to also lock in the
        // default-deny behavior (the previous code included unmapped segments
        // for *any* query — that is the cross-tenant leak).
        hist.load_segment("seg_unmapped", build_test_segment())
            .expect("load seg_unmapped");
        // Deliberately do NOT call set_segment_datasource for seg_unmapped.

        let results_after = hist.execute_query(&query).expect("execute again");
        assert!(
            results_after.is_empty(),
            "unmapped segment must NOT bleed into a DS-B query (default-deny), got {} result(s)",
            results_after.len()
        );

        // Sanity: the segment IS visible to its proper DS.
        let query_a: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"DS-A"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        )
        .expect("parse query a");
        let results_a = hist.execute_query(&query_a).expect("execute DS-A");
        assert_eq!(
            results_a.len(),
            1,
            "DS-A query must still see its own seg_a"
        );
    }

    /// Minimal `n`-row segment (only `__time` + one metric) for row-count
    /// oriented swap tests.
    fn build_rowcount_segment(n: usize) -> SegmentData {
        let day1 = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("parse")
            .timestamp_millis();
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![day1; n]));
        columns.insert(
            "value".to_string(),
            ColumnData::Double((0..n).map(|i| i as f64).collect()),
        );
        SegmentData {
            version: 9,
            num_rows: n,
            interval: Interval {
                start_millis: day1,
                end_millis: day1 + 1,
            },
            dimensions: vec![],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: true,
        }
    }

    /// Like [`build_rowcount_segment`] but with all rows/interval anchored at
    /// `day_start_millis` (for building segments whose time interval is
    /// disjoint from a query interval — interval-prune tests).
    fn build_rowcount_segment_at(n: usize, day_start_millis: i64) -> SegmentData {
        let mut columns = HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![day_start_millis; n]),
        );
        columns.insert(
            "value".to_string(),
            ColumnData::Double((0..n).map(|i| i as f64).collect()),
        );
        SegmentData {
            version: 9,
            num_rows: n,
            interval: Interval {
                start_millis: day_start_millis,
                end_millis: day_start_millis + 1,
            },
            dimensions: vec![],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: true,
        }
    }

    /// Epoch-millis for an RFC3339 instant (test convenience).
    fn millis_at(rfc3339: &str) -> i64 {
        chrono::DateTime::parse_from_rfc3339(rfc3339)
            .expect("parse")
            .timestamp_millis()
    }

    /// Minimal `n`-row segment anchored at `day_start_millis` whose only
    /// non-`__time` column is a single **metric** named `metric`. Two
    /// disjoint-interval segments built with DIFFERENT metric names give a
    /// datasource a heterogeneous schema, so a scan's segment-derived result
    /// shape (each segment contributes its columns even with zero matching
    /// rows — R10 HIGH) is observable.
    fn build_named_metric_segment(day_start_millis: i64, metric: &str, n: usize) -> SegmentData {
        let mut columns = HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![day_start_millis; n]),
        );
        columns.insert(
            metric.to_string(),
            ColumnData::Double((0..n).map(|i| i as f64).collect()),
        );
        SegmentData {
            version: 9,
            num_rows: n,
            interval: Interval {
                start_millis: day_start_millis,
                end_millis: day_start_millis + 1,
            },
            dimensions: vec![],
            metrics: vec![metric.to_string()],
            columns,
            time_sorted: true,
        }
    }

    /// Minimal `n`-row segment anchored at `day_start_millis` whose only
    /// non-`__time` column is a single string **dimension** named `dim`, every
    /// row holding `value`. Companion to [`build_named_metric_segment`] for the
    /// search-parity R10 HIGH test (search operates over string dimensions).
    fn build_named_dim_segment(
        day_start_millis: i64,
        dim: &str,
        value: &str,
        n: usize,
    ) -> SegmentData {
        let dictionary = FrontCodedDictionary::from_sorted(vec![value.to_string()]);
        let encoded_values = vec![0u32; n];
        let mut bitmap = DruidBitmap::new();
        for row in 0..n {
            bitmap.insert(u32::try_from(row).expect("row index fits u32"));
        }
        let dim_col = ColumnData::String(StringColumnData {
            dictionary,
            encoded_values,
            bitmap_indexes: vec![bitmap],
        });
        let mut columns = HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![day_start_millis; n]),
        );
        columns.insert(dim.to_string(), dim_col);
        SegmentData {
            version: 9,
            num_rows: n,
            interval: Interval {
                start_millis: day_start_millis,
                end_millis: day_start_millis + 1,
            },
            dimensions: vec![dim.to_string()],
            metrics: vec![],
            columns,
            time_sorted: true,
        }
    }

    /// A DEFENSIVELY-constructed segment whose `dimensions` list ILLEGALLY names
    /// `__time` (ahead of a genuine `region` dimension) with a matching
    /// `columns["__time"]` LONG. A well-formed segment never lists `__time`
    /// among its `dimensions`/`metrics` — it is the time column — but the public
    /// `SegmentData` fields are caller-mutable, so this shape is constructible.
    /// It reproduces the FG-7 R16 HIGH: without the `CachedSchema::from_segment`
    /// `__time` filter, `__time` is captured BOTH as a dimension AND (via
    /// `has_time`) as the time column, so schema discovery emits it twice.
    fn build_time_in_dims_segment() -> SegmentData {
        let dictionary = FrontCodedDictionary::from_sorted(vec!["us".to_string()]);
        let mut bitmap = DruidBitmap::new();
        for row in 0..3u32 {
            bitmap.insert(row);
        }
        let region_col = ColumnData::String(StringColumnData {
            dictionary,
            encoded_values: vec![0u32; 3],
            bitmap_indexes: vec![bitmap],
        });
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![0_i64; 3]));
        columns.insert("region".to_string(), region_col);
        columns.insert("value".to_string(), ColumnData::Double(vec![1.0, 2.0, 3.0]));
        SegmentData {
            version: 9,
            num_rows: 3,
            interval: Interval {
                start_millis: 0,
                end_millis: 1,
            },
            // ILLEGAL shape: `__time` named as a dimension; `region` is genuine.
            dimensions: vec!["__time".to_string(), "region".to_string()],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: true,
        }
    }

    fn single_entry_cache_bytes(id: &str, datasource: Option<&str>, data: &SegmentData) -> u64 {
        let mut segments = HashMap::new();
        segments.insert(
            id.to_string(),
            SegmentEntry {
                residency: Residency::Resident(Arc::new(build_rowcount_segment(0))),
                estimated_bytes: estimate_segment_bytes(data),
                num_rows: 0,
                prune_interval: None,
                schema: CachedSchema::from_segment(data),
            },
        );
        let mut datasources = HashMap::new();
        if let Some(datasource) = datasource {
            datasources.insert(id.to_string(), datasource.to_string());
        }
        estimate_cache_state(&segments, &datasources)
    }

    fn reset_cache_state_full_folds() {
        CACHE_STATE_FULL_FOLDS.with(|folds| folds.set(0));
    }

    fn assert_no_cache_state_full_folds() {
        CACHE_STATE_FULL_FOLDS.with(|folds| {
            assert_eq!(
                folds.get(),
                0,
                "a cache mutation folded the full committed maps"
            );
        });
    }

    fn assert_exact_cache_invariant(hist: &Historical) {
        let segments = hist.segments.read().expect("segment read lock");
        let datasources = hist
            .segment_datasources
            .read()
            .expect("datasource read lock");
        reset_cache_state_full_folds();
        let expected = estimate_cache_state(&segments, &datasources);
        assert_eq!(
            hist.current_cache_bytes.load(Ordering::Relaxed),
            expected,
            "incremental cache bytes must equal the full-map oracle"
        );
        reset_cache_state_full_folds();
    }

    #[test]
    fn cache_mutations_are_incremental_and_remain_exact() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), u64::MAX);

        reset_cache_state_full_folds();
        hist.load_segment("a", build_rowcount_segment(1))
            .expect("load a");
        assert_no_cache_state_full_folds();
        assert_exact_cache_invariant(&hist);

        hist.set_segment_datasource("a", "wiki").expect("map a");
        assert_no_cache_state_full_folds();
        assert_exact_cache_invariant(&hist);

        hist.set_segment_datasource("a", "datasource-with-more-capacity")
            .expect("replace mapping for a");
        assert_no_cache_state_full_folds();
        assert_exact_cache_invariant(&hist);

        hist.load_segment_with_datasource("b", "wiki", build_rowcount_segment(2))
            .expect("load b");
        assert_no_cache_state_full_folds();
        assert_exact_cache_invariant(&hist);

        hist.replace_segments(
            &["a".to_string()],
            vec![SegmentSwapEntry {
                id: "c".to_string(),
                data: Arc::new(build_rowcount_segment(3)),
                datasource: Some("wiki".to_string()),
            }],
        )
        .expect("replace a with c");
        assert_no_cache_state_full_folds();
        assert_exact_cache_invariant(&hist);

        hist.drop_segment("b").expect("drop b");
        assert_no_cache_state_full_folds();
        assert_exact_cache_invariant(&hist);
    }

    #[test]
    fn audit_randomized_ops_preserve_exactness_and_never_over_admit() {
        // Independent auditor hammer (distinct from the implementer's tests):
        // a deterministic pseudo-random sequence of every mutation kind over a
        // LARGE id space (so the maps repeatedly cross capacity boundaries)
        // under both a generous and a tight limit. After EVERY operation
        // assert (a) the incremental ledger equals a full re-estimate
        // (exactness), (b) the committed total never exceeds the limit
        // (fail-closed admission control), and (c) no mutation folded the maps.
        fn run(limit: u64, ids: u64, iters: usize, seed: u64) {
            let dir = tempfile::tempdir().expect("tempdir");
            let hist = Historical::new(dir.path().to_path_buf(), limit);
            reset_cache_state_full_folds();
            let mut state = seed | 1;
            let mut next = || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            };
            for _ in 0..iters {
                let r = next();
                let id = format!("seg{}", (r >> 3) % ids);
                let id2 = format!("seg{}", (r >> 19) % ids);
                let ds = if (r >> 5) & 1 == 0 { "ds_a" } else { "ds_b" };
                let rows = ((r >> 8) % 3) as usize + 1;
                match r % 6 {
                    0 => {
                        let _ = hist.load_segment(&id, build_rowcount_segment(rows));
                    }
                    1 => {
                        let _ = hist.load_segment_with_datasource(
                            &id,
                            ds,
                            build_rowcount_segment(rows),
                        );
                    }
                    2 => {
                        let _ = hist.set_segment_datasource(&id, ds);
                    }
                    3 => {
                        let _ = hist.drop_segment(&id);
                    }
                    4 => {
                        let add = vec![SegmentSwapEntry {
                            id: id.clone(),
                            data: Arc::new(build_rowcount_segment(rows)),
                            datasource: if r % 4 == 0 {
                                None
                            } else {
                                Some(ds.to_string())
                            },
                        }];
                        let _ = hist.replace_segments(&[id.clone(), id2.clone()], add);
                    }
                    _ => {
                        let add = vec![SegmentSwapEntry {
                            id: id.clone(),
                            data: Arc::new(build_rowcount_segment(rows)),
                            datasource: Some(ds.to_string()),
                        }];
                        let _ = hist.replace_segments(&[], add);
                    }
                }
                assert_no_cache_state_full_folds();
                assert!(
                    hist.current_cache_bytes.load(Ordering::Relaxed) <= hist.max_cache_bytes,
                    "committed total exceeded the cache limit after a random op"
                );
                assert_exact_cache_invariant(&hist);
            }
        }
        // Generous limit (exactness + growth/shrink churn across many tiers).
        run(1 << 40, 80, 6000, 0x9e37_79b9_7f4a_7c15);
        // Tight limits near boundary tiers (fail-closed admission under churn).
        run(40_000, 80, 6000, 0x1234_5678_9abc_def0);
        run(12_000, 40, 6000, 0xdead_beef_cafe_f00d);
    }

    #[test]
    fn cache_admission_never_commits_over_the_limit_across_capacity_growth() {
        for full in [7_usize, 14, 28, 56, 112] {
            for attempt in 0..400 {
                let probe_dir = tempfile::tempdir().expect("probe tempdir");
                let probe = Historical::new(probe_dir.path().to_path_buf(), u64::MAX);
                for resident in 0..full {
                    probe
                        .load_segment(&format!("resident-{resident}"), build_rowcount_segment(1))
                        .expect("unlimited probe load");
                }
                let limit = probe.info().cache_bytes_used;

                let dir = tempfile::tempdir().expect("tempdir");
                let hist = Historical::new(dir.path().to_path_buf(), limit);
                for resident in 0..full {
                    if hist
                        .load_segment(&format!("resident-{resident}"), build_rowcount_segment(1))
                        .is_ok()
                    {
                        assert!(hist.info().cache_bytes_used <= limit);
                        assert_exact_cache_invariant(&hist);
                    }
                }

                if hist.has_segment("resident-0") {
                    hist.drop_segment("resident-0").expect("drop resident-0");
                    assert!(hist.info().cache_bytes_used <= limit);
                    assert_exact_cache_invariant(&hist);
                }
                if hist
                    .load_segment(&format!("fresh-{attempt}"), build_rowcount_segment(1))
                    .is_ok()
                {
                    assert!(hist.info().cache_bytes_used <= limit);
                    assert_exact_cache_invariant(&hist);
                }
            }
        }
    }

    /// Total `count` over a datasource through the real query path,
    /// summed across matching segments.
    fn total_count(hist: &Historical, ds: &str) -> i64 {
        let query: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": ds},
            "intervals": ["2000-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z"],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("build count query");
        hist.execute_query(&query)
            .expect("execute count query")
            .iter()
            .map(|r| match r {
                QueryResult::Timeseries(ts) => ts
                    .iter()
                    .map(|row| {
                        row.result
                            .get("cnt")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0)
                    })
                    .sum::<i64>(),
                _ => 0,
            })
            .sum()
    }

    /// `replace_segments` semantics: swaps in one call, returns the removed
    /// entries (with data + datasource) for rollback, and tolerates
    /// drop-ids that are not loaded.
    #[test]
    fn replace_segments_swaps_and_returns_removed_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);

        hist.load_segment("seg_a", build_rowcount_segment(6))
            .expect("load a");
        hist.set_segment_datasource("seg_a", "wiki").expect("ds a");
        hist.load_segment("seg_b", build_rowcount_segment(6))
            .expect("load b");
        hist.set_segment_datasource("seg_b", "wiki").expect("ds b");
        assert_eq!(total_count(&hist, "wiki"), 12);

        // Swap A + B (and a never-loaded id, tolerated) out for C.
        let removed = hist
            .replace_segments(
                &[
                    "seg_a".to_string(),
                    "seg_b".to_string(),
                    "not_loaded".to_string(),
                ],
                vec![SegmentSwapEntry {
                    id: "seg_c".to_string(),
                    data: Arc::new(build_rowcount_segment(5)),
                    datasource: Some("wiki".to_string()),
                }],
            )
            .expect("swap");
        assert_eq!(removed.len(), 2, "only loaded victims are returned");
        assert_eq!(total_count(&hist, "wiki"), 5);
        assert_eq!(hist.segment_count(), 1);
        assert!(!hist.has_segment("seg_a"));
        assert_eq!(hist.segment_datasource("seg_a"), None);

        // Roll the swap back by feeding the removed entries straight in.
        let undone = hist
            .replace_segments(&["seg_c".to_string()], removed)
            .expect("rollback swap");
        assert_eq!(undone.len(), 1);
        assert_eq!(undone[0].id, "seg_c");
        assert_eq!(total_count(&hist, "wiki"), 12);
        assert_eq!(
            hist.segment_datasource("seg_a").as_deref(),
            Some("wiki"),
            "datasource mapping must be restored with the segment"
        );
    }

    /// Codex 2026-07-12 HIGH #3: the swap must be atomic to concurrent
    /// queries. Two 6-row segments are repeatedly swapped for one 5-row
    /// segment and back while a reader hammers `count`; every observation
    /// must be exactly 12 (full old set) or 5 (full new set). Any partial
    /// mix (0, 6, 11, 17, ...) is a violation.
    #[test]
    fn replace_segments_atomic_under_concurrent_queries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Arc::new(Historical::new(dir.path().to_path_buf(), 10_000_000));

        hist.load_segment("seg_a", build_rowcount_segment(6))
            .expect("load a");
        hist.set_segment_datasource("seg_a", "wiki").expect("ds a");
        hist.load_segment("seg_b", build_rowcount_segment(6))
            .expect("load b");
        hist.set_segment_datasource("seg_b", "wiki").expect("ds b");

        let stop = Arc::new(AtomicBool::new(false));
        let violations = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
        let reader = {
            let hist = Arc::clone(&hist);
            let stop = Arc::clone(&stop);
            let violations = Arc::clone(&violations);
            std::thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let n = total_count(&hist, "wiki");
                    if n != 12 && n != 5 {
                        violations.lock().expect("violations lock").push(n);
                    }
                }
            })
        };

        let seg_c = Arc::new(build_rowcount_segment(5));
        for _ in 0..300 {
            let removed = hist
                .replace_segments(
                    &["seg_a".to_string(), "seg_b".to_string()],
                    vec![SegmentSwapEntry {
                        id: "seg_c".to_string(),
                        data: Arc::clone(&seg_c),
                        datasource: Some("wiki".to_string()),
                    }],
                )
                .expect("swap in");
            assert_eq!(removed.len(), 2);
            hist.replace_segments(&["seg_c".to_string()], removed)
                .expect("swap back");
        }
        stop.store(true, Ordering::SeqCst);
        reader.join().expect("reader thread");
        let seen = violations.lock().expect("violations lock").clone();
        assert!(
            seen.is_empty(),
            "concurrent queries observed partial swap states: {seen:?}"
        );
        assert_eq!(total_count(&hist, "wiki"), 12);
    }

    /// Codex 2026-07-12 round-2 HIGH #4: loading a second segment under an
    /// already-loaded id must FAIL CLOSED, not silently discard the first
    /// segment's rows (pre-fix, `load_segment` replaced it in place).
    #[test]
    fn load_segment_same_id_fails_closed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);

        hist.load_segment("seg_x", build_rowcount_segment(6))
            .expect("first load");
        hist.set_segment_datasource("seg_x", "wiki").expect("ds");
        assert_eq!(total_count(&hist, "wiki"), 6);

        let err = hist
            .load_segment("seg_x", build_rowcount_segment(3))
            .expect_err("same-id load must fail closed");
        assert!(
            format!("{err}").contains("seg_x"),
            "error names the colliding id: {err}"
        );

        // The original rows are untouched — nothing was overwritten.
        assert_eq!(total_count(&hist, "wiki"), 6);
        assert_eq!(hist.segment_count(), 1);
    }

    /// Codex 2026-07-12 round-2 HIGH #4: `replace_segments` fails closed on
    /// an undeclared id collision (add id already loaded, not in drop_ids)
    /// and on duplicate add ids, mutating NOTHING; an id listed in both
    /// drop_ids and add remains an explicit, legal in-place refresh.
    #[test]
    fn replace_segments_id_collision_fails_closed_without_mutation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);

        hist.load_segment("seg_a", build_rowcount_segment(6))
            .expect("load a");
        hist.set_segment_datasource("seg_a", "wiki").expect("ds a");
        hist.load_segment("seg_b", build_rowcount_segment(6))
            .expect("load b");
        hist.set_segment_datasource("seg_b", "wiki").expect("ds b");

        // Undeclared collision: adding seg_a while only dropping seg_b.
        let err = match hist.replace_segments(
            &["seg_b".to_string()],
            vec![SegmentSwapEntry {
                id: "seg_a".to_string(),
                data: Arc::new(build_rowcount_segment(1)),
                datasource: Some("wiki".to_string()),
            }],
        ) {
            Err(e) => e,
            Ok(_) => panic!("undeclared same-id add must fail closed"),
        };
        assert!(format!("{err}").contains("seg_a"));

        // NOTHING was mutated: seg_b was not dropped, seg_a not replaced.
        assert_eq!(total_count(&hist, "wiki"), 12);
        assert_eq!(hist.segment_count(), 2);

        // Duplicate ids within one add batch are rejected too.
        let dup = |n: usize| SegmentSwapEntry {
            id: "seg_new".to_string(),
            data: Arc::new(build_rowcount_segment(n)),
            datasource: Some("wiki".to_string()),
        };
        assert!(
            hist.replace_segments(&[], vec![dup(1), dup(2)]).is_err(),
            "duplicate add ids must be rejected"
        );
        assert_eq!(total_count(&hist, "wiki"), 12);

        // Explicit refresh (id in BOTH drop_ids and add) stays legal.
        let removed = hist
            .replace_segments(
                &["seg_a".to_string()],
                vec![SegmentSwapEntry {
                    id: "seg_a".to_string(),
                    data: Arc::new(build_rowcount_segment(2)),
                    datasource: Some("wiki".to_string()),
                }],
            )
            .expect("declared in-place refresh must succeed");
        assert_eq!(removed.len(), 1);
        assert_eq!(total_count(&hist, "wiki"), 8, "6 (b) + 2 (refreshed a)");
    }

    #[test]
    fn hash_table_bytes_covers_raw_bucket_table() {
        // hashbrown rounds the bucket count up to a power of two: a map that
        // reports capacity 3 allocates 4 raw buckets. The charge must cover
        // at least that raw table (4 × element), not just 3 × element, so a
        // small map cannot slip its bucket allocation under the cache limit.
        // Cover the full raw table: 4 buckets × (element + 1 control byte)
        // PLUS the trailing control-group / alignment allowance, so the
        // charge is not below the real allocation (which also has a
        // Group::WIDTH trailing control group). Use the production element
        // type: `hash_table_bytes` is only applied to a segment's internal
        // columns map, `(String, ColumnData)`.
        type ColumnEntry = (String, ferrodruid_segment::column::ColumnData);
        let per_bucket = std::mem::size_of::<ColumnEntry>() as u64 + 1;
        assert!(
            hash_table_bytes::<ColumnEntry>(3) >= 4 * per_bucket + 16,
            "capacity-3 map must charge its 4-bucket table plus a control-group allowance"
        );
        // Load-factor rounding: capacity 4 needs 8 buckets (4 > 4×7/8=3.5).
        assert!(
            hash_table_bytes::<ColumnEntry>(4) >= 8 * per_bucket,
            "capacity-4 map must charge its next-power-of-two (8) bucket table"
        );
        assert_eq!(hash_table_bytes::<ColumnEntry>(0), 0);
    }

    #[test]
    fn cache_limit_and_drop_accounting_are_exact() {
        let dir = tempfile::tempdir().expect("tempdir");
        let two_rows = build_rowcount_segment(2);
        let two_row_bytes = single_entry_cache_bytes("seg_a", None, &two_rows);
        let one_row_bytes = single_entry_cache_bytes("seg_b", None, &build_rowcount_segment(1));
        let hist = Historical::new(dir.path().to_path_buf(), two_row_bytes);

        hist.load_segment("seg_a", two_rows)
            .expect("segment fits exactly");
        assert_eq!(hist.info().cache_bytes_used, two_row_bytes);

        let err = hist
            .load_segment("seg_b", build_rowcount_segment(1))
            .expect_err("load beyond cache limit must fail");
        assert!(matches!(err, DruidError::ResourceLimit { .. }));
        assert!(!hist.has_segment("seg_b"));
        assert_eq!(hist.info().cache_bytes_used, two_row_bytes);

        hist.drop_segment("seg_a").expect("drop");
        assert_eq!(hist.info().cache_bytes_used, 0);
        hist.load_segment("seg_b", build_rowcount_segment(1))
            .expect("freed bytes are reusable");
        assert_eq!(hist.info().cache_bytes_used, one_row_bytes);
    }

    #[test]
    fn concurrent_loads_enforce_one_atomic_cache_limit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let segment_bytes = single_entry_cache_bytes("seg_a", None, &build_rowcount_segment(2));
        let hist = Arc::new(Historical::new(dir.path().to_path_buf(), segment_bytes));
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut workers = Vec::new();
        for id in ["seg_a", "seg_b"] {
            let hist = Arc::clone(&hist);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                hist.load_segment(id, build_rowcount_segment(2))
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker"))
            .collect();
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Err(DruidError::ResourceLimit { .. })))
                .count(),
            1
        );
        assert_eq!(hist.segment_count(), 1);
        assert_eq!(hist.info().cache_bytes_used, segment_bytes);
    }

    #[test]
    fn replace_cache_limit_uses_net_delta_and_is_atomic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let old_segment = build_rowcount_segment(6);
        let old_bytes = single_entry_cache_bytes("old", Some("wiki"), &old_segment);
        let smaller_bytes =
            single_entry_cache_bytes("smaller", Some("wiki"), &build_rowcount_segment(5));
        let hist = Historical::new(dir.path().to_path_buf(), old_bytes);
        hist.load_segment_with_datasource("old", "wiki", old_segment)
            .expect("initial");

        hist.replace_segments(
            &["old".to_string()],
            vec![SegmentSwapEntry {
                id: "smaller".to_string(),
                data: Arc::new(build_rowcount_segment(5)),
                datasource: Some("wiki".to_string()),
            }],
        )
        .expect("net-smaller replace");
        assert_eq!(hist.info().cache_bytes_used, smaller_bytes);

        let err = match hist.replace_segments(
            &[],
            vec![SegmentSwapEntry {
                id: "too_much".to_string(),
                data: Arc::new(build_rowcount_segment(2)),
                datasource: Some("wiki".to_string()),
            }],
        ) {
            Err(err) => err,
            Ok(_) => panic!("over-limit replace must fail"),
        };
        assert!(matches!(err, DruidError::ResourceLimit { .. }));
        assert!(hist.has_segment("smaller"));
        assert!(!hist.has_segment("too_much"));
        assert_eq!(hist.info().cache_bytes_used, smaller_bytes);
    }

    #[test]
    fn cache_estimate_charges_string_dictionary_capacity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let day = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("parse")
            .timestamp_millis();
        let mut capacity_heavy = String::with_capacity(64 * 1024);
        capacity_heavy.push('x');
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![day]));
        columns.insert(
            "payload".to_string(),
            ColumnData::String(StringColumnData {
                dictionary: FrontCodedDictionary::from_sorted(vec![capacity_heavy]),
                encoded_values: vec![0],
                bitmap_indexes: vec![DruidBitmap::new()],
            }),
        );
        let segment = SegmentData {
            version: 9,
            num_rows: 1,
            interval: Interval {
                start_millis: day,
                end_millis: day + 1,
            },
            dimensions: vec!["payload".to_string()],
            metrics: Vec::new(),
            columns,
            time_sorted: true,
        };
        assert!(
            estimate_segment_bytes(&segment) > 64 * 1024,
            "dictionary allocation capacity must be included in cache accounting"
        );

        let hist = Historical::new(dir.path().to_path_buf(), 1024);
        let err = hist
            .load_segment("string_heavy", segment)
            .expect_err("capacity-heavy dictionary must exceed the cache limit");
        assert!(matches!(err, DruidError::ResourceLimit { .. }));
        assert_eq!(hist.segment_count(), 0);
        assert_eq!(hist.info().cache_bytes_used, 0);
    }

    #[test]
    fn cache_estimate_charges_sparse_bitmap_containers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let day = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("parse")
            .timestamp_millis();
        let mut sparse = DruidBitmap::new();
        for container in 0..1024_u32 {
            sparse.insert(container << 16);
        }
        assert!(
            sparse.estimated_heap_bytes() >= 1024 * 256,
            "each sparse roaring container and growth slack must be charged"
        );
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![day]));
        columns.insert(
            "payload".to_string(),
            ColumnData::String(StringColumnData {
                dictionary: FrontCodedDictionary::from_sorted(vec!["x".to_string()]),
                encoded_values: vec![0],
                bitmap_indexes: vec![sparse],
            }),
        );
        let segment = SegmentData {
            version: 9,
            num_rows: 1,
            interval: Interval {
                start_millis: day,
                end_millis: day + 1,
            },
            dimensions: vec!["payload".to_string()],
            metrics: Vec::new(),
            columns,
            time_sorted: true,
        };
        let hist = Historical::new(dir.path().to_path_buf(), 64 * 1024);
        let err = hist
            .load_segment("bitmap_heavy", segment)
            .expect_err("sparse bitmap containers must exceed the cache limit");
        assert!(matches!(err, DruidError::ResourceLimit { .. }));
        assert_eq!(hist.info().cache_bytes_used, 0);
    }

    #[test]
    fn cache_limit_charges_segment_id_and_datasource_allocations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let segment = build_rowcount_segment(1);
        let short_limit = single_entry_cache_bytes("short", None, &segment);
        let hist = Historical::new(dir.path().to_path_buf(), short_limit);
        let huge_id = "i".repeat(64 * 1024);
        let err = hist
            .load_segment(&huge_id, segment)
            .expect_err("large segment id must be charged");
        assert!(matches!(err, DruidError::ResourceLimit { .. }));
        assert_eq!(hist.segment_count(), 0);
        assert_eq!(hist.info().cache_bytes_used, 0);

        let segment = build_rowcount_segment(1);
        let unmapped_limit = single_entry_cache_bytes("seg", None, &segment);
        let hist = Historical::new(dir.path().to_path_buf(), unmapped_limit);
        hist.load_segment("seg", segment)
            .expect("unmapped load fits");
        let before = hist.info().cache_bytes_used;
        let huge_datasource = "d".repeat(64 * 1024);
        let err = hist
            .set_segment_datasource("seg", &huge_datasource)
            .expect_err("large datasource mapping must be charged");
        assert!(matches!(err, DruidError::ResourceLimit { .. }));
        assert_eq!(hist.segment_datasource("seg"), None);
        assert_eq!(hist.info().cache_bytes_used, before);
    }

    #[test]
    fn strict_null_generation_rejects_before_publication() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist =
            Historical::with_strict_null_generation(dir.path().to_path_buf(), 1_000_000, true);

        let err = hist
            .load_segment("legacy_unmapped", build_rowcount_segment(2))
            .expect_err("legacy load API must not bypass strict mode");
        assert!(format!("{err}").contains("strict_null_generation"));
        assert!(!hist.has_segment("legacy_unmapped"));

        let err = hist
            .load_segment_with_datasource("legacyish", "wiki", build_rowcount_segment(2))
            .expect_err("zero-valued unconfirmed column must fail strict mode");
        assert!(format!("{err}").contains("strict_null_generation"));
        assert!(!hist.has_segment("legacyish"));
        assert_eq!(hist.segment_datasource("legacyish"), None);
        assert_eq!(hist.info().cache_bytes_used, 0);

        assert!(
            hist.set_segment_datasource("missing", "wiki").is_err(),
            "mapping an unloaded segment must fail"
        );

        let err = match hist.replace_segments(
            &[],
            vec![SegmentSwapEntry {
                id: "legacy_replace".to_string(),
                data: Arc::new(build_rowcount_segment(2)),
                datasource: None,
            }],
        ) {
            Err(err) => err,
            Ok(_) => panic!("datasource-less replace must not bypass strict mode"),
        };
        assert!(format!("{err}").contains("strict_null_generation"));
        assert!(!hist.has_segment("legacy_replace"));
        assert_eq!(hist.info().cache_bytes_used, 0);
    }

    #[test]
    fn drop_is_atomic_when_datasource_lock_is_poisoned() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);
        hist.load_segment_with_datasource("seg", "wiki", build_rowcount_segment(2))
            .expect("load");
        let bytes_before = hist.info().cache_bytes_used;

        let ds_map = Arc::clone(&hist.segment_datasources);
        let poisoner = std::thread::spawn(move || {
            let _guard = ds_map.write().expect("datasource lock");
            panic!("poison datasource lock");
        });
        assert!(poisoner.join().is_err());

        assert!(
            hist.drop_segment("seg").is_err(),
            "poisoned second lock must fail before mutation"
        );
        assert!(hist.has_segment("seg"));
        assert_eq!(hist.info().cache_bytes_used, bytes_before);
    }

    #[test]
    fn union_datasource_routes_only_named_members() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);
        for (id, ds) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
            hist.load_segment_with_datasource(id, ds, build_rowcount_segment(1))
                .expect("load");
        }
        let query: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "union", "dataSources": ["alpha", "beta"]},
            "intervals": ["2000-01-01/2100-01-01"],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("union query");
        let results = hist.execute_query(&query).expect("execute");
        assert_eq!(results.len(), 2, "gamma must not be queried");
    }

    #[test]
    fn union_all_routes_each_branch_independently() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);
        hist.load_segment_with_datasource("a", "alpha", build_rowcount_segment(2))
            .expect("load alpha");
        hist.load_segment_with_datasource("a2", "alpha", build_rowcount_segment(4))
            .expect("load second alpha");
        hist.load_segment_with_datasource("b", "beta", build_rowcount_segment(3))
            .expect("load beta");

        let scan = |datasource: &str| {
            serde_json::from_value::<DruidQuery>(serde_json::json!({
                "queryType": "scan",
                "dataSource": {"type": "table", "name": datasource},
                "intervals": ["2000-01-01/2100-01-01"],
                "columns": ["value"],
                "order": "none"
            }))
            .expect("scan query")
        };
        let query = DruidQuery::UnionAll(vec![scan("alpha"), scan("beta")]);
        let results = hist.execute_query(&query).expect("execute union all");
        let rows: usize = results
            .iter()
            .map(|result| match result {
                QueryResult::Scan(scan) => scan.events.len(),
                other => panic!("unexpected result: {other:?}"),
            })
            .sum();
        assert_eq!(rows, 9, "each branch must see only its datasource");
    }

    #[test]
    fn union_all_holds_one_snapshot_across_all_branches() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Arc::new(Historical::new(dir.path().to_path_buf(), 1_000_000));
        hist.load_segment_with_datasource("old_a", "alpha", build_rowcount_segment(2))
            .expect("load old alpha");
        hist.load_segment_with_datasource("old_b", "beta", build_rowcount_segment(2))
            .expect("load old beta");

        let scan = |datasource: &str| {
            serde_json::from_value::<DruidQuery>(serde_json::json!({
                "queryType": "scan",
                "dataSource": {"type": "table", "name": datasource},
                "intervals": ["2000-01-01/2100-01-01"],
                "columns": ["value"],
                "order": "none"
            }))
            .expect("scan query")
        };
        let query = Arc::new(DruidQuery::UnionAll(vec![scan("alpha"), scan("beta")]));
        let stop = Arc::new(AtomicBool::new(false));
        let violations = Arc::new(std::sync::Mutex::new(Vec::<usize>::new()));
        let reader = {
            let hist = Arc::clone(&hist);
            let query = Arc::clone(&query);
            let stop = Arc::clone(&stop);
            let violations = Arc::clone(&violations);
            std::thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let rows = hist
                        .execute_query(&query)
                        .expect("union query")
                        .iter()
                        .map(|result| match result {
                            QueryResult::Scan(scan) => scan.events.len(),
                            _ => 0,
                        })
                        .sum();
                    if rows != 4 && rows != 6 {
                        violations.lock().expect("violations").push(rows);
                    }
                }
            })
        };

        for _ in 0..300 {
            let old = hist
                .replace_segments(
                    &["old_a".to_string(), "old_b".to_string()],
                    vec![
                        SegmentSwapEntry {
                            id: "new_a".to_string(),
                            data: Arc::new(build_rowcount_segment(3)),
                            datasource: Some("alpha".to_string()),
                        },
                        SegmentSwapEntry {
                            id: "new_b".to_string(),
                            data: Arc::new(build_rowcount_segment(3)),
                            datasource: Some("beta".to_string()),
                        },
                    ],
                )
                .expect("swap new generation");
            hist.replace_segments(&["new_a".to_string(), "new_b".to_string()], old)
                .expect("restore old generation");
        }
        stop.store(true, Ordering::SeqCst);
        reader.join().expect("reader");
        let seen = violations.lock().expect("violations");
        assert!(
            seen.is_empty(),
            "UNION ALL observed mixed segment generations: {seen:?}"
        );
    }

    #[test]
    fn unsupported_datasource_fails_before_segment_scan() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);
        let query: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "scan",
            "dataSource": {
                "type": "inline",
                "columnNames": ["x"],
                "rows": [[1]]
            },
            "intervals": ["2000-01-01/2100-01-01"],
            "columns": ["x"]
        }))
        .expect("inline query");
        let err = hist
            .execute_query(&query)
            .expect_err("inline datasource must fail closed");
        assert!(format!("{err}").contains("inline datasources"));
    }

    // -----------------------------------------------------------------------
    // FG-7 — spill-to-disk residency + on-demand reload + memory-budgeted LRU
    // -----------------------------------------------------------------------

    fn spill_historical(dir: &std::path::Path, budget: u64) -> Historical {
        Historical::with_options(dir.to_path_buf(), budget, false, true)
    }

    fn count_query(ds: &str) -> DruidQuery {
        serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": ds},
            "intervals": ["2000-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z"],
            "granularity": "all",
            "aggregations": [
                {"type": "count", "name": "cnt"},
                {"type": "doubleSum", "name": "total", "fieldName": "value"}
            ]
        }))
        .expect("build count query")
    }

    /// Count the per-write spill enclosures in an instance root, EXCLUDING the
    /// `.lock` liveness sentinel (always present in a live spill root since
    /// findings 3+4). Every `count_..._enclosures(&hist.spill_instance_dir)`
    /// assertion is about spilled SEGMENTS, not the sentinel.
    fn count_spill_enclosures(path: &std::path::Path) -> usize {
        std::fs::read_dir(path)
            .map(|rd| {
                rd.flatten()
                    .filter(|e| e.file_name() != std::ffi::OsStr::new(SPILL_LOCK_FILE))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Order-independent, structural snapshot of a per-segment result vector.
    fn sorted_debug(results: &[QueryResult]) -> Vec<String> {
        let mut serialized: Vec<String> = results.iter().map(|r| format!("{r:?}")).collect();
        serialized.sort();
        serialized
    }

    /// Spill-mode query results are byte-identical to heap mode, and stay
    /// identical when segments are evicted and re-read on a later query.
    #[test]
    fn spill_query_result_matches_heap_and_survives_reload() {
        let heap_dir = tempfile::tempdir().expect("tempdir");
        let heap = Historical::new(heap_dir.path().to_path_buf(), 10_000_000);
        heap.load_segment_with_datasource("s1", "wiki", build_test_segment())
            .expect("heap load s1");
        heap.load_segment_with_datasource("s2", "wiki", build_rowcount_segment(4))
            .expect("heap load s2");

        let spill_dir = tempfile::tempdir().expect("tempdir");
        // Budget 1: at most one segment stays resident, forcing a re-decode of
        // the other on every full fan-out.
        let spill = spill_historical(spill_dir.path(), 1);
        spill
            .load_segment_with_datasource("s1", "wiki", build_test_segment())
            .expect("spill load s1");
        spill
            .load_segment_with_datasource("s2", "wiki", build_rowcount_segment(4))
            .expect("spill load s2");

        let query = count_query("wiki");
        let heap_res = heap.execute_query(&query).expect("heap query");
        let spill_res = spill.execute_query(&query).expect("spill query");
        assert_eq!(
            sorted_debug(&heap_res),
            sorted_debug(&spill_res),
            "spill-mode results must be byte-identical to heap mode"
        );

        let decodes_after_first = spill.decode_count.load(Ordering::Relaxed);
        let spill_res2 = spill.execute_query(&query).expect("spill query 2");
        assert_eq!(
            sorted_debug(&spill_res),
            sorted_debug(&spill_res2),
            "re-read (evict → reload) results must be identical"
        );
        assert!(
            spill.decode_count.load(Ordering::Relaxed) > decodes_after_first,
            "the tiny budget must have forced at least one segment re-decode"
        );
    }

    /// Randomized load/query/drop hammer: resident bytes never exceed the
    /// budget and the external ledger always equals the LRU's own sum; the LRU
    /// never pins a dropped segment.
    #[test]
    fn spill_lru_budget_invariant_under_random_ops() {
        let dir = tempfile::tempdir().expect("tempdir");
        let one = estimate_segment_bytes(&build_rowcount_segment(1));
        // Holds ~3 residents; every segment is 1-row (< budget), so the
        // single-oversized-segment exception can never fire.
        let budget = one.saturating_mul(3);
        let hist = spill_historical(dir.path(), budget);

        let mut state = 0xC0FF_EE00_1234_5678_u64 | 1;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..800 {
            let r = next();
            let id = format!("s{}", (r >> 3) % 10);
            match r % 4 {
                0 => {
                    let _ =
                        hist.load_segment_with_datasource(&id, "wiki", build_rowcount_segment(1));
                }
                1 => {
                    let _ = hist.get_segment(&id);
                }
                2 => {
                    let _ = hist.execute_query(&count_query("wiki"));
                }
                _ => {
                    let _ = hist.drop_segment(&id);
                }
            }

            let resident = hist.resident_bytes.load(Ordering::Relaxed);
            assert!(
                resident <= budget,
                "resident_bytes {resident} exceeded budget {budget}"
            );
            let lru_ids: Vec<String> = {
                let lru = hist.resident_lru.lock().expect("lru lock");
                assert_eq!(
                    resident,
                    lru.total_bytes(),
                    "resident_bytes ledger diverged from the LRU's own sum"
                );
                lru.nodes.keys().cloned().collect()
            };
            for lru_id in &lru_ids {
                assert!(
                    hist.has_segment(lru_id),
                    "LRU pinned a segment ({lru_id}) that is no longer loaded"
                );
            }
        }
    }

    /// Eviction follows least-recently-used order; `touch` refreshes recency.
    #[test]
    fn spill_lru_evicts_least_recently_used() {
        let dir = tempfile::tempdir().expect("tempdir");
        let one = estimate_segment_bytes(&build_rowcount_segment(1));
        // Holds exactly two 1-row residents (2·one ≤ 2.5·one < 3·one).
        let budget = one.saturating_mul(2).saturating_add(one / 2);
        let hist = spill_historical(dir.path(), budget);
        for id in ["a", "b", "c"] {
            hist.load_segment(id, build_rowcount_segment(1))
                .expect("load");
        }

        let lru_contains = |id: &str| hist.resident_lru.lock().expect("lru").contains(id);

        // Materialize a then b → resident {a, b}.
        hist.get_segment("a")
            .expect("get a")
            .expect("materialize a");
        hist.get_segment("b")
            .expect("get b")
            .expect("materialize b");
        assert!(lru_contains("a") && lru_contains("b"));

        // Materialize c → evict LRU (a) → {b, c}.
        hist.get_segment("c")
            .expect("get c")
            .expect("materialize c");
        assert!(!lru_contains("a"), "a was least-recently-used");
        assert!(lru_contains("b") && lru_contains("c"));

        // Touch b (now MRU), then materialize a → evict LRU (c) → {a, b}.
        hist.get_segment("b").expect("get b").expect("touch b");
        hist.get_segment("a")
            .expect("get a")
            .expect("re-materialize a");
        assert!(
            !lru_contains("c"),
            "c became least-recently-used after b touch"
        );
        assert!(lru_contains("a") && lru_contains("b"));
        assert!(hist.resident_bytes.load(Ordering::Relaxed) <= budget);
    }

    /// Two threads racing to materialize the SAME cold spilled segment trigger
    /// exactly ONE decode (single-flight) and observe the SAME `Arc`.
    #[test]
    fn spill_single_flight_decodes_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Arc::new(spill_historical(dir.path(), 10_000_000));
        hist.load_segment("s", build_rowcount_segment(5))
            .expect("load");
        assert_eq!(hist.decode_count.load(Ordering::Relaxed), 0);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let hist = Arc::clone(&hist);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                hist.get_segment("s").expect("get s").expect("materialize")
            }));
        }
        let arcs: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("join"))
            .collect();
        assert_eq!(
            hist.decode_count.load(Ordering::Relaxed),
            1,
            "single-flight: exactly one SegmentData::open across both threads"
        );
        assert!(
            Arc::ptr_eq(&arcs[0], &arcs[1]),
            "both threads must observe the same decode"
        );
    }

    /// Dropping a spilled segment deletes its spill directory and its LRU pin.
    #[test]
    fn spill_drop_deletes_dir_and_lru_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        hist.load_segment("s", build_rowcount_segment(3))
            .expect("load");
        hist.get_segment("s").expect("get s").expect("materialize");
        assert!(hist.resident_bytes.load(Ordering::Relaxed) > 0);
        assert_eq!(count_spill_enclosures(&hist.spill_instance_dir), 1);

        hist.drop_segment("s").expect("drop");
        assert_eq!(hist.segment_count(), 0);
        assert_eq!(
            hist.resident_bytes.load(Ordering::Relaxed),
            0,
            "the LRU pin must be released on drop"
        );
        assert_eq!(
            count_spill_enclosures(&hist.spill_instance_dir),
            0,
            "the spill directory must be deleted on drop"
        );
        assert!(!hist.resident_lru.lock().expect("lru").contains("s"));
    }

    /// A spill-mode swap deletes the victim's spill directory, evicts its LRU
    /// pin, inserts the replacement, and returns the removed victim's data.
    #[test]
    fn spill_replace_swaps_deletes_victim_dir_and_returns_removed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        hist.load_segment_with_datasource("old", "wiki", build_rowcount_segment(6))
            .expect("load old");
        hist.get_segment("old")
            .expect("get old")
            .expect("materialize old");
        assert_eq!(count_spill_enclosures(&hist.spill_instance_dir), 1);

        let removed = hist
            .replace_segments(
                &["old".to_string()],
                vec![SegmentSwapEntry {
                    id: "new".to_string(),
                    data: Arc::new(build_rowcount_segment(5)),
                    datasource: Some("wiki".to_string()),
                }],
            )
            .expect("swap");
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].id, "old");
        assert_eq!(
            removed[0].data.num_rows, 6,
            "the removed victim's real data must be returned"
        );
        assert!(!hist.has_segment("old") && hist.has_segment("new"));
        assert_eq!(total_count(&hist, "wiki"), 5);
        assert_eq!(
            count_spill_enclosures(&hist.spill_instance_dir),
            1,
            "the old spill dir is deleted and the new one written (net one)"
        );
        assert!(!hist.resident_lru.lock().expect("lru").contains("old"));
    }

    /// Findings 3+4: the startup janitor reaps an orphaned instance root whose
    /// advisory lock is FREE (a dead holder leaves its `.lock` sentinel behind
    /// but no live flock on it), and leaves the SHARED spill root in place. It
    /// must NOT wipe the whole spill area (that would destroy a live peer's
    /// bytes). Pre-fix the pid-parsing janitor left this non-numeric-named root
    /// untouched (`"dead-peer"` does not parse as `<pid>-…`), so the reap never
    /// happened — this asserts the lock-based reap does.
    #[test]
    fn spill_janitor_reaps_unlocked_orphan_instance_roots() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill_root = dir.path().join("spill");
        // A dead-holder orphan: stale segment bytes plus an UNLOCKED sentinel.
        let orphan = spill_root.join("dead-peer");
        std::fs::create_dir_all(orphan.join("orphan-seg-0")).expect("mk orphan");
        std::fs::write(orphan.join("orphan-seg-0").join("stale.bin"), b"stale").expect("write");
        std::fs::write(orphan.join(SPILL_LOCK_FILE), b"").expect("stale sentinel");
        assert!(orphan.exists());

        let _hist = spill_historical(dir.path(), 10_000_000);
        assert!(
            !orphan.exists(),
            "the janitor must reap an unlocked (dead-holder) orphan instance root"
        );
        assert!(spill_root.exists(), "the shared spill root must persist");
    }

    /// Findings 3+4: the janitor must NOT reap a root whose advisory lock is
    /// still HELD (a live holder, even in another PID namespace). It stages a
    /// peer root, takes and holds its exclusive lock, then constructs a
    /// Historical (running the janitor) and asserts the root survives.
    #[test]
    fn spill_janitor_keeps_lock_held_instance_roots() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill_root = dir.path().join("spill");
        let live = spill_root.join("live-peer");
        std::fs::create_dir_all(live.join("s-0").join(SPILL_SEGMENT_SUBDIR)).expect("mk live");
        // Simulate a live holder: create and EXCLUSIVELY lock the sentinel, and
        // hold the lock across the janitor run.
        let held = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(live.join(SPILL_LOCK_FILE))
            .expect("open sentinel");
        FileExt::lock_exclusive(&held).expect("hold exclusive lock");

        let _hist = spill_historical(dir.path(), 10_000_000);
        assert!(
            live.exists(),
            "a lock-held (live) instance root must be kept by the janitor"
        );
        let _ = FileExt::unlock(&held);
        drop(held);
    }

    /// Findings 3+4: an orphan root with NO `.lock` sentinel is KEPT (the
    /// janitor requires a lockable sentinel to prove death; absence == keep, a
    /// bounded disk leak that never risks a live peer's bytes).
    #[test]
    fn spill_janitor_keeps_sentinel_less_roots() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill_root = dir.path().join("spill");
        let no_sentinel = spill_root.join("no-sentinel-peer");
        std::fs::create_dir_all(no_sentinel.join("seg-0")).expect("mk root");

        let _hist = spill_historical(dir.path(), 10_000_000);
        assert!(
            no_sentinel.exists(),
            "a root without a lock sentinel must be kept (err toward keeping)"
        );
    }

    /// R4/R5 HIGH (construction/sweep TOCTOU): the staging-then-rename claim
    /// leaves NO window in which a reapable final-named root is observable
    /// without its `.lock` already held. Model a constructor mid-claim — a
    /// `.staging-<nonce>` dir whose `.lock` it holds EXCLUSIVELY — then run a
    /// peer's janitor twice: once before the rename (it must keep the locked
    /// staging dir) and once after (the final root is born locked, so it is
    /// kept too).
    ///
    /// R5 regression: the R4 `.dirlock` arbitration made the shared-lock take
    /// best-effort, so a failed take (EINTR / open failure) let a final-named
    /// root appear unlocked and be reaped. The staging-then-rename claim removes
    /// that window structurally — there is no arbitration lock to fail.
    #[test]
    fn spill_staging_rename_leaves_no_unlocked_reapable_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill_root = dir.path().join("spill");
        std::fs::create_dir_all(&spill_root).expect("mk spill root");

        // Constructor A, mid-claim: a locked staging dir, not yet renamed.
        let (staging, held) = create_locked_staging_dir(&spill_root).expect("A stages + locks");
        assert!(
            staging
                .file_name()
                .expect("staging name")
                .to_string_lossy()
                .starts_with(SPILL_STAGING_PREFIX),
            "the staging dir must use the dot-prefixed staging name"
        );

        // A peer's janitor runs while A holds the staging lock. It must keep the
        // staging dir (A's lock is held).
        let peer_self = spill_root.join("B-self");
        reap_dead_instance_dirs(&spill_root, &peer_self);
        assert!(
            staging.exists(),
            "a lock-held staging dir must survive a peer janitor"
        );

        // A publishes staging → final. The lock survives the rename (it is bound
        // to the open file description), so the final root is born locked; a
        // second janitor pass keeps it.
        let final_dir = spill_root.join(new_spill_instance_name());
        std::fs::rename(&staging, &final_dir).expect("publish staging → final");
        reap_dead_instance_dirs(&spill_root, &peer_self);
        assert!(
            final_dir.exists(),
            "the final root is born locked (flock survives rename), so the janitor keeps it"
        );

        let _ = FileExt::unlock(&held);
        drop(held);
    }

    /// The exclusive advisory lock is bound to the open file description, so it
    /// survives renaming its directory: after locking `staging/.lock` and
    /// renaming the staging dir to its final name, a FRESH handle to
    /// `final/.lock` still sees the lock HELD (`WouldBlock`). This is the premise
    /// the staging-then-rename claim relies on to make the final root "born
    /// locked".
    #[test]
    fn instance_lock_survives_staging_rename() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill_root = dir.path().join("spill");
        std::fs::create_dir_all(&spill_root).expect("mk spill root");

        let (staging, held) = create_locked_staging_dir(&spill_root).expect("stage + lock");
        let final_dir = spill_root.join(new_spill_instance_name());
        std::fs::rename(&staging, &final_dir).expect("rename staging → final");

        // A separate handle to the FINAL sentinel must observe the lock held.
        let probe = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(final_dir.join(SPILL_LOCK_FILE))
            .expect("open final sentinel");
        assert_eq!(
            FileExt::try_lock_exclusive(&probe)
                .expect_err("the held lock must survive the rename")
                .kind(),
            std::io::ErrorKind::WouldBlock,
            "flock is bound to the open file description and survives rename"
        );
        let _ = FileExt::unlock(&held);
        drop(held);
    }

    /// `create_locked_staging_dir` creates a dot-prefixed `.staging-<nonce>/`
    /// dir, creates+exclusively-locks its `.lock` sentinel, and returns a
    /// DISTINCT name on each call (so a retry always makes progress).
    #[test]
    fn create_locked_staging_dir_locks_and_is_distinct() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill_root = dir.path().join("spill");
        std::fs::create_dir_all(&spill_root).expect("mk root");

        let (s1, l1) = create_locked_staging_dir(&spill_root).expect("stage 1");
        assert!(
            s1.join(SPILL_LOCK_FILE).exists(),
            "the sentinel must be created inside the staging dir"
        );
        assert!(
            s1.file_name()
                .expect("name")
                .to_string_lossy()
                .starts_with(SPILL_STAGING_PREFIX),
            "the staging dir must use the dot-prefixed staging name"
        );
        // Its lock is HELD: a fresh handle cannot take it.
        let probe = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(s1.join(SPILL_LOCK_FILE))
            .expect("open s1 sentinel");
        assert_eq!(
            FileExt::try_lock_exclusive(&probe)
                .expect_err("s1's sentinel must be locked")
                .kind(),
            std::io::ErrorKind::WouldBlock,
        );

        // A second staging claim yields a distinct directory.
        let (s2, l2) = create_locked_staging_dir(&spill_root).expect("stage 2");
        assert_ne!(s1, s2, "each staging claim must use a fresh name");
        drop(l1);
        drop(l2);
    }

    /// `claim_instance_dir` returns a locked FINAL-named root with no unlocked
    /// reapable window: a peer janitor (a different `self_root`) running
    /// immediately after the claim must NOT reap it, because its `.lock` is held.
    #[test]
    fn claim_instance_dir_has_no_unlocked_reapable_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill_root = dir.path().join("spill");
        std::fs::create_dir_all(&spill_root).expect("mk root");

        let (claimed, lock) = claim_instance_dir(&spill_root);
        assert!(lock.is_some(), "the claim must hold the instance lock");
        assert!(claimed.exists(), "the claimed root must exist");
        assert!(
            claimed.join(SPILL_LOCK_FILE).exists(),
            "the claimed root must carry its `.lock` sentinel"
        );

        // A peer's janitor runs. The claimed root's lock is HELD, so it survives.
        let peer_self = spill_root.join("peer-self");
        reap_dead_instance_dirs(&spill_root, &peer_self);
        assert!(
            claimed.exists(),
            "a peer janitor must never reap a lock-held freshly-claimed root"
        );
        drop(lock);
    }

    /// A failed on-demand decode FAILS the query — it is never a silent
    /// per-segment skip (which would return a wrong answer).
    #[test]
    fn spill_decode_failure_fails_query_not_skip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        hist.load_segment_with_datasource("s", "wiki", build_rowcount_segment(6))
            .expect("load");
        // Wipe the spill area so the cold segment cannot be re-read.
        std::fs::remove_dir_all(hist.cache_dir().join("spill")).expect("rm spill");

        let query: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": ["2000-01-01/2100-01-01"],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("query");
        let err = hist
            .execute_query(&query)
            .expect_err("a decode failure must fail the query, not skip the segment");
        let msg = format!("{err}");
        assert!(
            msg.contains("re-read") && msg.contains('s'),
            "the error must surface the spill-reload failure: {msg}"
        );
    }

    /// Interval pruning skips a disjoint-interval segment BEFORE decoding it
    /// (pure gain) as long as ANOTHER candidate still executes — so the query's
    /// executed set is never empty (finding 1). A covering-only query decodes
    /// just the covering segment; the disjoint one stays unread.
    #[test]
    fn spill_interval_prune_avoids_decode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        // "cover" covers 2024-01-01; "far" covers 2030-01-01 (disjoint).
        hist.load_segment_with_datasource("cover", "wiki", build_rowcount_segment(6))
            .expect("load cover");
        hist.load_segment_with_datasource(
            "far",
            "wiki",
            build_rowcount_segment_at(6, millis_at("2030-01-01T00:00:00Z")),
        )
        .expect("load far");
        assert_eq!(hist.decode_count.load(Ordering::Relaxed), 0);

        // Query covers "cover" only; "far" is interval-pruned. "cover" executes,
        // so the executed set is non-empty and "far" is never decoded.
        let covering: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": ["2024-01-01/2024-01-02"],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("query");
        let res = hist.execute_query(&covering).expect("covering query");
        assert_eq!(res.len(), 1, "only the covering segment executes");
        assert_eq!(
            hist.decode_count.load(Ordering::Relaxed),
            1,
            "the pruned (disjoint) segment must not be decoded"
        );
    }

    /// Finding 1: when interval pruning would skip EVERY routed candidate, the
    /// fan-out must still force exactly one to execute so the executor
    /// synthesizes the query interval's zero-fill bucket (a fully-pruned
    /// fan-out silently returned `[]` instead — a changed answer). Verified in
    /// BOTH residency modes; the forced spill execution decodes once.
    #[test]
    fn all_pruned_candidates_force_one_zero_fill() {
        for spill in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            let hist = if spill {
                spill_historical(dir.path(), 10_000_000)
            } else {
                Historical::new(dir.path().to_path_buf(), 10_000_000)
            };
            // Only segment covers 2024-01-01; query a DISJOINT day in 2030.
            hist.load_segment_with_datasource("s", "wiki", build_rowcount_segment(6))
                .expect("load");

            let q: DruidQuery = serde_json::from_value(serde_json::json!({
                "queryType": "timeseries",
                "dataSource": {"type": "table", "name": "wiki"},
                "intervals": ["2030-01-01T00:00:00.000Z/2030-01-02T00:00:00.000Z"],
                "granularity": "day",
                "aggregations": [{"type": "count", "name": "cnt"}]
            }))
            .expect("query");

            let res = hist.execute_query(&q).expect("query");
            // Reference = executing that one segment directly (pre-prune fan-out).
            let direct = ferrodruid_query::execute_query(&q, &build_rowcount_segment(6))
                .expect("direct exec");
            assert!(
                !res.is_empty(),
                "all-pruned fan-out must still execute one candidate (spill={spill})"
            );
            assert_eq!(res.len(), 1);
            assert_eq!(
                format!("{:?}", res[0]),
                format!("{direct:?}"),
                "forced execution must equal the pre-prune fan-out (spill={spill})"
            );
            match &res[0] {
                QueryResult::Timeseries(ts) => {
                    assert_eq!(ts.len(), 1, "one synthetic zero bucket");
                    assert_eq!(ts[0].result.get("cnt"), Some(&serde_json::json!(0)));
                }
                other => panic!("expected timeseries, got {other:?}"),
            }
        }
    }

    /// Finding 1 (spill): the forced-execution guard is SCOPED to the all-pruned
    /// case — when a real (intersecting) segment already executes, a disjoint
    /// segment stays pruned and adds nothing (no spurious forced execution).
    /// SPILL mode, because pruning is now gated to spill only (finding 2): in
    /// heap mode every routed segment executes and the disjoint one contributes
    /// its own zero-fill partial (see
    /// `heap_executes_every_routed_segment_no_prune`).
    #[test]
    fn prune_with_real_segment_adds_no_disjoint_result() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        hist.load_segment_with_datasource("cover", "wiki", build_rowcount_segment(6))
            .expect("load cover");
        hist.load_segment_with_datasource(
            "far",
            "wiki",
            build_rowcount_segment_at(3, millis_at("2030-01-01T00:00:00Z")),
        )
        .expect("load far");

        let q: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"],
            "granularity": "day",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("query");

        let res = hist.execute_query(&q).expect("query");
        assert_eq!(
            res.len(),
            1,
            "only the covering segment executes; no forcing"
        );
        let direct =
            ferrodruid_query::execute_query(&q, &build_rowcount_segment(6)).expect("direct exec");
        assert_eq!(format!("{:?}", res[0]), format!("{direct:?}"));
    }

    /// Finding 2: heap-mode results are a byte-for-byte long-standing contract,
    /// so interval pruning is gated to spill mode. A heap query routed to an
    /// intersecting segment AND a disjoint one must return a partial PER routed
    /// segment (the FG-7-before fan-out), not the single partial FG-7's leaked
    /// heap pruning produced. Pre-fix (prune active in heap) `far` is pruned and
    /// only ONE partial comes back — this asserts TWO.
    #[test]
    fn heap_executes_every_routed_segment_no_prune() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);
        hist.load_segment_with_datasource("cover", "wiki", build_rowcount_segment(6))
            .expect("load cover");
        hist.load_segment_with_datasource(
            "far",
            "wiki",
            build_rowcount_segment_at(3, millis_at("2030-01-01T00:00:00Z")),
        )
        .expect("load far");

        let q: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"],
            "granularity": "day",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("query");

        let res = hist.execute_query(&q).expect("query");
        assert_eq!(
            res.len(),
            2,
            "heap must return one partial per routed segment (no heap pruning) \
             — FG-7-before behaviour, not the pruned single partial (finding 2)"
        );
        // The disjoint `far` contributes an empty/zero-fill partial; the total
        // count is still only `cover`'s six rows (answer is unchanged).
        assert_eq!(sum_cnt(&res), 6, "the disjoint partial adds only zero-fill");
    }

    /// R10 HIGH: a **Scan** result's columns are *segment-derived* — every
    /// routed segment contributes its declared column schema even when it
    /// matches zero rows (an empty scan partial still carries its `columns`).
    /// Interval pruning is spill-only, so pruning a disjoint scan segment would
    /// make the scan answer depend on residency: a heterogeneous-schema
    /// datasource whose disjoint segment carries a schema-unique column would
    /// drop that column in spill mode but keep it in heap mode. This asserts the
    /// per-segment scan partials — count AND union of columns — are IDENTICAL in
    /// heap and spill mode (pre-fix, spill prunes `far` and loses `b` → RED).
    #[test]
    fn spill_scan_over_heterogeneous_schema_matches_heap() {
        let cover_day = millis_at("2024-01-01T00:00:00Z");
        let far_day = millis_at("2030-01-01T00:00:00Z");

        // Scan over 2024: intersects `cover` (metric `a`), disjoint from `far`
        // (metric `b`). `SELECT *` (no explicit `columns`) so each segment's own
        // schema drives the result columns.
        let scan: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "scan",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"]
        }))
        .expect("query");

        // Returns (partial count, union of all partials' column names).
        fn scan_shape(
            hist: &Historical,
            scan: &DruidQuery,
        ) -> (usize, std::collections::BTreeSet<String>) {
            let res = hist.execute_query(scan).expect("scan query");
            let mut cols = std::collections::BTreeSet::new();
            for r in &res {
                match r {
                    QueryResult::Scan(s) => cols.extend(s.columns.iter().cloned()),
                    other => panic!("expected scan result, got {other:?}"),
                }
            }
            (res.len(), cols)
        }

        // Heap reference (never prunes).
        let heap_dir = tempfile::tempdir().expect("tempdir");
        let heap = Historical::new(heap_dir.path().to_path_buf(), 10_000_000);
        heap.load_segment_with_datasource(
            "cover",
            "wiki",
            build_named_metric_segment(cover_day, "a", 3),
        )
        .expect("load cover");
        heap.load_segment_with_datasource(
            "far",
            "wiki",
            build_named_metric_segment(far_day, "b", 2),
        )
        .expect("load far");
        let (heap_count, heap_cols) = scan_shape(&heap, &scan);

        // Spill under test (must NOT interval-prune scan segments).
        let spill_dir = tempfile::tempdir().expect("tempdir");
        let spill = spill_historical(spill_dir.path(), 10_000_000);
        spill
            .load_segment_with_datasource(
                "cover",
                "wiki",
                build_named_metric_segment(cover_day, "a", 3),
            )
            .expect("load cover");
        spill
            .load_segment_with_datasource(
                "far",
                "wiki",
                build_named_metric_segment(far_day, "b", 2),
            )
            .expect("load far");
        let (spill_count, spill_cols) = scan_shape(&spill, &scan);

        let expected: std::collections::BTreeSet<String> = ["__time", "a", "b"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        assert_eq!(
            heap_cols, expected,
            "heap scan must surface every routed segment's schema (both `a` and `b`)"
        );
        assert_eq!(
            heap_count, 2,
            "heap executes both routed segments (one partial each)"
        );
        assert_eq!(
            spill_cols, heap_cols,
            "spill scan must surface the SAME schema as heap — the disjoint `far` \
             segment's `b` column must not be interval-pruned away (R10 HIGH)"
        );
        assert_eq!(
            spill_count, heap_count,
            "spill scan must return one partial per routed segment, like heap \
             (no residency-dependent pruning of scan segments)"
        );
    }

    /// R10 HIGH (search companion): like scan, a **Search** reports per-segment
    /// results and its partial fan-out is segment-derived, so it must not be
    /// interval-pruned either. Assert the spill search partial fan-out matches
    /// heap's (pre-fix spill prunes the disjoint segment → one fewer partial).
    #[test]
    fn spill_search_partials_match_heap() {
        let cover_day = millis_at("2024-01-01T00:00:00Z");
        let far_day = millis_at("2030-01-01T00:00:00Z");

        // Search over 2024 for the substring "x" (both `us`/`de` contain it via
        // the wildcard "" query below). Use an all-match query so the searched
        // dimension shape is what varies, not the value filter.
        let search: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "search",
            "dataSource": {"type": "table", "name": "wiki"},
            "granularity": "all",
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"],
            "query": {"type": "insensitive_contains", "value": ""}
        }))
        .expect("query");

        fn search_partial_count(hist: &Historical, search: &DruidQuery) -> usize {
            let res = hist.execute_query(search).expect("search query");
            for r in &res {
                assert!(
                    matches!(r, QueryResult::Search(_)),
                    "expected search result, got {r:?}"
                );
            }
            res.len()
        }

        let heap_dir = tempfile::tempdir().expect("tempdir");
        let heap = Historical::new(heap_dir.path().to_path_buf(), 10_000_000);
        heap.load_segment_with_datasource(
            "cover",
            "wiki",
            build_named_dim_segment(cover_day, "region", "us", 3),
        )
        .expect("load cover");
        heap.load_segment_with_datasource(
            "far",
            "wiki",
            build_named_dim_segment(far_day, "country", "de", 2),
        )
        .expect("load far");
        let heap_count = search_partial_count(&heap, &search);

        let spill_dir = tempfile::tempdir().expect("tempdir");
        let spill = spill_historical(spill_dir.path(), 10_000_000);
        spill
            .load_segment_with_datasource(
                "cover",
                "wiki",
                build_named_dim_segment(cover_day, "region", "us", 3),
            )
            .expect("load cover");
        spill
            .load_segment_with_datasource(
                "far",
                "wiki",
                build_named_dim_segment(far_day, "country", "de", 2),
            )
            .expect("load far");
        let spill_count = search_partial_count(&spill, &search);

        assert_eq!(heap_count, 2, "heap runs search on both routed segments");
        assert_eq!(
            spill_count, heap_count,
            "spill search must not interval-prune routed segments (R10 HIGH)"
        );
    }

    /// Regression: the R10 HIGH fix scopes the Scan/Search prune EXEMPTION
    /// narrowly — aggregating query types (timeseries/topN/groupBy) still enjoy
    /// spill interval pruning, since their result columns are query-defined and
    /// an empty partial is a true no-op. A disjoint aggregating segment must NOT
    /// be decoded in spill mode, while a disjoint SCAN segment over the same
    /// data IS decoded (so its schema survives). The decode counter proves the
    /// prune benefit is retained for aggregates and dropped only for scan.
    #[test]
    fn spill_prune_retained_for_aggregates_not_scan() {
        let cover_day = millis_at("2024-01-01T00:00:00Z");
        let far_day = millis_at("2030-01-01T00:00:00Z");

        // --- Aggregate (timeseries): disjoint `far` stays pruned (not decoded).
        let ts_dir = tempfile::tempdir().expect("tempdir");
        let ts_hist = spill_historical(ts_dir.path(), 10_000_000);
        ts_hist
            .load_segment_with_datasource(
                "cover",
                "wiki",
                build_named_metric_segment(cover_day, "a", 3),
            )
            .expect("load cover");
        ts_hist
            .load_segment_with_datasource(
                "far",
                "wiki",
                build_named_metric_segment(far_day, "b", 2),
            )
            .expect("load far");
        assert_eq!(ts_hist.decode_count.load(Ordering::Relaxed), 0);

        let ts: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("query");
        let ts_res = ts_hist.execute_query(&ts).expect("timeseries query");
        assert_eq!(ts_res.len(), 1, "aggregate prunes the disjoint segment");
        assert_eq!(
            ts_hist.decode_count.load(Ordering::Relaxed),
            1,
            "aggregate keeps the spill prune benefit — `far` is never decoded"
        );

        // --- Scan: disjoint `far` is NOT pruned (decoded so its schema is kept).
        let scan_dir = tempfile::tempdir().expect("tempdir");
        let scan_hist = spill_historical(scan_dir.path(), 10_000_000);
        scan_hist
            .load_segment_with_datasource(
                "cover",
                "wiki",
                build_named_metric_segment(cover_day, "a", 3),
            )
            .expect("load cover");
        scan_hist
            .load_segment_with_datasource(
                "far",
                "wiki",
                build_named_metric_segment(far_day, "b", 2),
            )
            .expect("load far");
        assert_eq!(scan_hist.decode_count.load(Ordering::Relaxed), 0);

        let scan: DruidQuery = serde_json::from_value(serde_json::json!({
            "queryType": "scan",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": ["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"]
        }))
        .expect("query");
        let scan_res = scan_hist.execute_query(&scan).expect("scan query");
        assert_eq!(scan_res.len(), 2, "scan executes both routed segments");
        assert_eq!(
            scan_hist.decode_count.load(Ordering::Relaxed),
            2,
            "scan must decode the disjoint segment too (no prune) so its schema survives"
        );
    }

    /// Finding 1: a query-level validation error must fire even when interval
    /// pruning would skip every candidate. Pre-fix the fully-pruned fan-out
    /// returned `Ok([])` (success-empty), swallowing the error. Verified in
    /// both residency modes.
    #[test]
    fn all_pruned_validation_error_still_surfaces() {
        for spill in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            let hist = if spill {
                spill_historical(dir.path(), 10_000_000)
            } else {
                Historical::new(dir.path().to_path_buf(), 10_000_000)
            };
            hist.load_segment_with_datasource("s", "wiki", build_rowcount_segment(6))
                .expect("load");

            // Disjoint interval (all candidates pruned) + an unsupported
            // post-aggregator that the executor rejects up front.
            let q: DruidQuery = serde_json::from_value(serde_json::json!({
                "queryType": "timeseries",
                "dataSource": {"type": "table", "name": "wiki"},
                "intervals": ["2030-01-01T00:00:00.000Z/2030-01-02T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [{"type": "count", "name": "cnt"}],
                "postAggregations": [
                    {"type": "expression", "name": "bad", "expression": "concat(cnt, 1)"}
                ]
            }))
            .expect("query");

            let err = hist
                .execute_query(&q)
                .expect_err("a fully-pruned query must still run query-level validation");
            assert!(
                format!("{err}").contains("expression"),
                "must surface the validation error (spill={spill}): {err}"
            );
        }
    }

    /// A same-id load in spill mode fails closed AND deletes the rejected
    /// load's freshly-written spill directory (no orphan).
    #[test]
    fn spill_load_collision_fails_closed_without_orphan() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        hist.load_segment("s", build_rowcount_segment(6))
            .expect("first load");
        assert_eq!(count_spill_enclosures(&hist.spill_instance_dir), 1);

        let err = hist
            .load_segment("s", build_rowcount_segment(3))
            .expect_err("same-id load must fail closed");
        assert!(format!("{err}").contains('s'));
        assert_eq!(
            count_spill_enclosures(&hist.spill_instance_dir),
            1,
            "the rejected load's spill dir must be cleaned up (no orphan)"
        );
        assert_eq!(hist.segment_count(), 1);
        // The original data is untouched.
        hist.set_segment_datasource("s", "wiki").expect("map");
        assert_eq!(total_count(&hist, "wiki"), 6);
    }

    /// Strict null-generation rejection happens BEFORE any spill write, so a
    /// refused segment never leaves an orphan on disk.
    #[test]
    fn spill_strict_null_generation_rejects_without_spilling() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::with_options(dir.path().to_path_buf(), 1_000_000, true, true);
        let err = hist
            .load_segment("legacy", build_rowcount_segment(2))
            .expect_err("strict mode must reject");
        assert!(format!("{err}").contains("strict_null_generation"));
        assert!(!hist.has_segment("legacy"));
        assert_eq!(
            count_spill_enclosures(&hist.spill_instance_dir),
            0,
            "a strict-rejected segment must never be spilled"
        );
    }

    /// Finding 3: a REAL swap whose removed victim cannot be decoded for the
    /// returned-entries contract must fail CLOSED — no mutation — rather than
    /// silently dropping the victim from the returned set (which would make the
    /// caller mistake a loaded-but-corrupt victim for a never-loaded one).
    #[test]
    fn spill_replace_real_swap_fails_closed_on_corrupt_victim() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        hist.load_segment_with_datasource("old", "wiki", build_rowcount_segment(6))
            .expect("load old");
        // Corrupt the cold victim: wipe the spill area so "old" cannot decode.
        // The add's own spill_write recreates the instance dir, so the add
        // still spills.
        std::fs::remove_dir_all(hist.cache_dir().join("spill")).expect("wipe spill");

        let err = match hist.replace_segments(
            &["old".to_string()],
            vec![SegmentSwapEntry {
                id: "new".to_string(),
                data: Arc::new(build_rowcount_segment(5)),
                datasource: Some("wiki".to_string()),
            }],
        ) {
            Ok(_) => panic!("a corrupt removed victim must fail the real swap closed"),
            Err(e) => e,
        };
        assert!(
            format!("{err}").contains("old"),
            "error must name the undecodable victim: {err}"
        );
        // Fail-closed: nothing mutated — "old" still loaded, "new" absent.
        assert!(hist.has_segment("old"), "victim must remain loaded");
        assert!(!hist.has_segment("new"), "the add must not have landed");
        // No orphan: the add's pre-written spill dir was cleaned on reject.
        assert_eq!(
            count_spill_enclosures(&hist.spill_instance_dir),
            0,
            "the rejected swap must leave no spill orphan"
        );
    }

    /// Finding 3 / R30: a PURE drop (empty add) must SUCCEED even when the
    /// victim's spilled bytes are unreadable — the drop-only-decrease invariant
    /// forbids failing a drop on a corrupt victim. It decodes nothing and
    /// returns no removed-entry data.
    #[test]
    fn spill_replace_pure_drop_succeeds_on_corrupt_victim() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        hist.load_segment_with_datasource("old", "wiki", build_rowcount_segment(6))
            .expect("load old");
        // Corrupt the cold victim.
        std::fs::remove_dir_all(hist.cache_dir().join("spill")).expect("wipe spill");

        let removed = hist
            .replace_segments(&["old".to_string()], vec![])
            .expect("a pure drop must succeed even on a corrupt victim (R30)");
        assert!(
            removed.is_empty(),
            "spill-mode pure drop returns no removed-entry data"
        );
        assert!(!hist.has_segment("old"), "the victim must be dropped");
        assert_eq!(hist.segment_count(), 0);
    }

    /// Build a metric-only segment with NO `__time` column (via the public
    /// [`SegmentDataBuilder`] with no timestamp column). Heap mode holds it fine;
    /// a spill round-trip cannot re-decode it (finding 3).
    fn build_no_time_segment() -> SegmentData {
        let seg = ferrodruid_segment::SegmentDataBuilder::new()
            .add_double_column("value", true, vec![1.0, 2.0, 3.0])
            .build()
            .expect("build metric-only segment");
        assert!(
            !seg.columns.contains_key("__time"),
            "precondition: a builder with no timestamp column yields no `__time`"
        );
        seg
    }

    /// Finding 3 (R7 HIGH, real bug): spill mode must REJECT a segment with no
    /// `__time` column **at admission, fail-loud**, because the spill round-trip
    /// (`write_segment_v9` → strict `SegmentData::open`) can never re-decode it —
    /// accepting it would make every later `materialize` on that segment fail
    /// forever. Heap mode is unchanged (no disk round-trip): it still accepts the
    /// same segment. A spill-only reject.
    #[test]
    fn spill_rejects_load_of_segment_without_time_column() {
        // Spill mode: rejected fail-loud, nothing written to disk.
        let spill_dir = tempfile::tempdir().expect("tempdir");
        let spill = spill_historical(spill_dir.path(), 10_000_000);
        let err = spill
            .load_segment("metric_only", build_no_time_segment())
            .expect_err("spill must reject a no-__time segment at admission");
        assert!(
            format!("{err}").contains("__time"),
            "error must name the missing __time requirement: {err}"
        );
        assert!(
            !spill.has_segment("metric_only"),
            "a rejected segment must not be loaded"
        );
        assert_eq!(
            count_spill_enclosures(&spill.spill_instance_dir),
            0,
            "a rejected no-__time segment must never be spilled (no orphan)"
        );

        // Heap mode: byte-for-byte unchanged — the same segment still loads.
        let heap_dir = tempfile::tempdir().expect("tempdir");
        let heap = Historical::new(heap_dir.path().to_path_buf(), 10_000_000);
        heap.load_segment("metric_only", build_no_time_segment())
            .expect("heap mode still accepts a no-__time segment (unchanged)");
        assert!(heap.has_segment("metric_only"));
    }

    /// Finding 3 (R7 HIGH): the spill reject also guards the `replace_segments`
    /// ADD path — a no-`__time` add is refused before anything is spilled or
    /// mutated (no orphan, no partial swap).
    #[test]
    fn spill_replace_rejects_add_without_time_column() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        hist.load_segment_with_datasource("keep", "wiki", build_rowcount_segment(3))
            .expect("load keep");

        let err = match hist.replace_segments(
            &[],
            vec![SegmentSwapEntry {
                id: "bad".to_string(),
                data: Arc::new(build_no_time_segment()),
                datasource: Some("wiki".to_string()),
            }],
        ) {
            Ok(_) => panic!("a no-__time add must be rejected in spill mode"),
            Err(e) => e,
        };
        assert!(
            format!("{err}").contains("__time"),
            "error must name the missing __time requirement: {err}"
        );
        // Fail-closed: nothing mutated, the pre-existing segment is intact, the
        // bad add never landed, and no spill orphan was left behind.
        assert!(hist.has_segment("keep"), "the existing segment must remain");
        assert!(!hist.has_segment("bad"), "the bad add must not land");
        assert_eq!(
            count_spill_enclosures(&hist.spill_instance_dir),
            1,
            "only the pre-loaded 'keep' segment is spilled; the rejected add left no orphan"
        );
    }

    // -----------------------------------------------------------------------
    // FG-7 R8 (spill round-trip fidelity): `write_segment_v9` + `SegmentData::open`
    // is not a faithful identity for every `SegmentData`. Three same-class writer
    // gaps let admission pass but make a spilled segment materialize
    // wrong-or-never at query time, permanently. Spill admission now writes then
    // re-reads and rejects any segment that does not round-trip. Heap mode never
    // round-trips through disk and stays byte-for-byte unchanged.
    // -----------------------------------------------------------------------

    /// R8/H1 segment: a `columns` entry (`extra`) that is listed in NEITHER
    /// `dimensions` NOR `metrics`. `write_segment_v9` only emits `__time` +
    /// declared dims/metrics, so `extra` is silently dropped on the way to disk;
    /// heap mode keeps it, spill mode must reject it (it would vanish from a
    /// spilled scan).
    fn build_column_outside_dims_and_metrics_segment() -> SegmentData {
        let day1 = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("parse")
            .timestamp_millis();
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![day1, day1]));
        // Present in `columns` but declared as neither a dimension nor a metric.
        columns.insert("extra".to_string(), ColumnData::Long(vec![10, 20]));
        SegmentData {
            version: 9,
            num_rows: 2,
            interval: Interval {
                start_millis: day1,
                end_millis: day1,
            },
            dimensions: vec![],
            metrics: vec![],
            columns,
            time_sorted: true,
        }
    }

    /// R8/H2 segment: a dimension whose NAME contains a smoosh meta delimiter
    /// (`,`). The writer emits column names verbatim into the comma-delimited
    /// `meta.smoosh` index, corrupting it so the strict reader can never parse
    /// the archive back.
    fn build_comma_column_name_segment() -> SegmentData {
        let day1 = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("parse")
            .timestamp_millis();
        ferrodruid_segment::SegmentDataBuilder::new()
            .add_timestamp_column(vec![day1, day1])
            .add_string_column("a,b", vec!["x".to_string(), "y".to_string()])
            .build()
            .expect("build comma-named-column segment")
    }

    /// R8/H3 segment: a `__time` column that is NOT `LONG` (here `STRING`). A v9
    /// spill round-trip reloads `__time` with its declared type, and a non-LONG
    /// `__time` makes every later interval/timestamp query on the spilled copy
    /// fail permanently.
    fn build_non_long_time_segment() -> SegmentData {
        let mut bm_a = DruidBitmap::new();
        bm_a.insert(0);
        let mut bm_b = DruidBitmap::new();
        bm_b.insert(1);
        let mut columns = HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::String(StringColumnData {
                dictionary: FrontCodedDictionary::from_sorted(vec![
                    "a".to_string(),
                    "b".to_string(),
                ]),
                encoded_values: vec![0, 1],
                bitmap_indexes: vec![bm_a, bm_b],
            }),
        );
        columns.insert("value".to_string(), ColumnData::Double(vec![1.0, 2.0]));
        SegmentData {
            version: 9,
            num_rows: 2,
            interval: Interval {
                start_millis: 0,
                end_millis: 1,
            },
            dimensions: vec![],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: false,
        }
    }

    /// R8/H1: spill admission must reject a segment whose `columns` carries an
    /// entry that is neither a dimension nor a metric — the writer would drop it
    /// silently, so a spilled scan would be missing it. Rejected fail-loud with
    /// no orphan; heap mode still accepts it byte-for-byte.
    #[test]
    fn spill_rejects_column_outside_dims_and_metrics() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill = spill_historical(dir.path(), 10_000_000);
        let err = spill
            .load_segment("h1", build_column_outside_dims_and_metrics_segment())
            .expect_err("spill must reject a segment that does not round-trip (dropped column)");
        assert!(
            format!("{err}").contains("round-trip") || format!("{err}").contains("column set"),
            "error must explain the round-trip fidelity failure: {err}"
        );
        assert!(!spill.has_segment("h1"), "a rejected segment must not load");
        assert_eq!(
            count_spill_enclosures(&spill.spill_instance_dir),
            0,
            "a rejected segment must leave no spill orphan"
        );

        // Heap mode is unchanged: the same segment still loads.
        let heap_dir = tempfile::tempdir().expect("tempdir");
        let heap = Historical::new(heap_dir.path().to_path_buf(), 10_000_000);
        heap.load_segment("h1", build_column_outside_dims_and_metrics_segment())
            .expect("heap mode accepts the same segment (no disk round-trip)");
        assert!(heap.has_segment("h1"));
    }

    /// R8/H2: spill admission must reject a segment with a comma in a column
    /// name — the resulting `meta.smoosh` cannot be re-read, so a spilled query
    /// would fail forever. Rejected fail-loud with no orphan; heap mode accepts.
    #[test]
    fn spill_rejects_comma_in_column_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill = spill_historical(dir.path(), 10_000_000);
        let err = spill
            .load_segment("h2", build_comma_column_name_segment())
            .expect_err("spill must reject a segment that cannot be re-read (corrupt meta)");
        assert!(
            format!("{err}").contains("round-trip"),
            "error must explain the round-trip fidelity failure: {err}"
        );
        assert!(!spill.has_segment("h2"), "a rejected segment must not load");
        assert_eq!(
            count_spill_enclosures(&spill.spill_instance_dir),
            0,
            "a rejected segment must leave no spill orphan"
        );

        // Heap mode is unchanged: the same segment still loads.
        let heap_dir = tempfile::tempdir().expect("tempdir");
        let heap = Historical::new(heap_dir.path().to_path_buf(), 10_000_000);
        heap.load_segment("h2", build_comma_column_name_segment())
            .expect("heap mode accepts the same segment (no disk round-trip)");
        assert!(heap.has_segment("h2"));
    }

    /// R8/H3: spill admission must reject a segment whose `__time` is not LONG —
    /// the spilled copy would reload a non-LONG `__time` and every interval query
    /// on it would fail forever. Rejected fail-loud (clear reason, before any
    /// bytes are written, so no orphan); heap mode accepts.
    #[test]
    fn spill_rejects_non_long_time_column() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill = spill_historical(dir.path(), 10_000_000);
        let err = spill
            .load_segment("h3", build_non_long_time_segment())
            .expect_err("spill must reject a non-LONG __time segment at admission");
        let msg = format!("{err}");
        assert!(
            msg.contains("__time") && msg.contains("LONG"),
            "error must name the non-LONG __time requirement: {err}"
        );
        assert!(!spill.has_segment("h3"), "a rejected segment must not load");
        assert_eq!(
            count_spill_enclosures(&spill.spill_instance_dir),
            0,
            "a non-LONG __time reject happens before any spill write (no orphan)"
        );

        // Heap mode is unchanged: the same segment still loads.
        let heap_dir = tempfile::tempdir().expect("tempdir");
        let heap = Historical::new(heap_dir.path().to_path_buf(), 10_000_000);
        heap.load_segment("h3", build_non_long_time_segment())
            .expect("heap mode accepts the same segment (no disk round-trip)");
        assert!(heap.has_segment("h3"));
    }

    /// R8 regression: a normal, faithful segment passes the round-trip verify,
    /// spills, and queries correctly (the verify must not reject healthy data).
    #[test]
    fn spill_roundtrip_verify_accepts_faithful_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spill = spill_historical(dir.path(), 10_000_000);
        spill
            .load_segment_with_datasource("ok", "wiki", build_test_segment())
            .expect("a faithful segment must pass the round-trip verify and spill");
        assert!(spill.has_segment("ok"));
        assert_eq!(
            count_spill_enclosures(&spill.spill_instance_dir),
            1,
            "the faithful segment is spilled exactly once"
        );
        // And it still answers queries (materialize re-reads it cleanly).
        assert_eq!(total_count(&spill, "wiki"), 6);
    }

    /// Finding 2 (R7 HIGH, scope): the returned removed-entries contract for a
    /// **pure drop** (empty `add`) deliberately differs by residency — heap
    /// returns each dropped victim data-bearing, spill returns an EMPTY vector
    /// (R30 forbids the fallible decode a `SegmentSwapEntry` would need). Both
    /// modes still perform the drop (drop-only-decrease). This pins the
    /// documented contract for a HEALTHY victim (the corrupt-victim case is
    /// covered separately).
    #[test]
    fn pure_drop_removed_contract_differs_by_residency() {
        // Heap: pure drop returns the victim with its real data.
        let heap_dir = tempfile::tempdir().expect("tempdir");
        let heap = Historical::new(heap_dir.path().to_path_buf(), 10_000_000);
        heap.load_segment_with_datasource("v", "wiki", build_rowcount_segment(6))
            .expect("heap load v");
        let heap_removed = heap
            .replace_segments(&["v".to_string()], vec![])
            .expect("heap pure drop");
        assert_eq!(heap_removed.len(), 1, "heap pure drop returns the victim");
        assert_eq!(heap_removed[0].id, "v");
        assert_eq!(
            heap_removed[0].data.num_rows, 6,
            "heap pure drop returns the victim's real data (rollback-feedable)"
        );
        assert!(!heap.has_segment("v"), "R30: the victim is dropped");
        assert_eq!(heap.segment_count(), 0);

        // Spill: pure drop returns an empty vector, yet still drops the victim.
        let spill_dir = tempfile::tempdir().expect("tempdir");
        let spill = spill_historical(spill_dir.path(), 10_000_000);
        spill
            .load_segment_with_datasource("v", "wiki", build_rowcount_segment(6))
            .expect("spill load v");
        let spill_removed = spill
            .replace_segments(&["v".to_string()], vec![])
            .expect("spill pure drop");
        assert!(
            spill_removed.is_empty(),
            "spill pure drop returns an empty vec (R30 — no fallible victim decode)"
        );
        assert!(!spill.has_segment("v"), "R30: the victim is still dropped");
        assert_eq!(spill.segment_count(), 0);
    }

    /// Finding 4: a failed segment write must leave NO orphaned staging/final
    /// directory behind. Each spill write lands inside a unique enclosing
    /// directory, so any `write_segment_v9` error cleans up in full. We force a
    /// write failure by pre-populating the first write's target dir (the same
    /// deterministic injection the segment writer's own refuse-to-overwrite
    /// test uses) and assert the spill root is left empty.
    #[test]
    fn spill_write_failure_leaves_no_orphan_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        assert_eq!(count_spill_enclosures(&hist.spill_instance_dir), 0);

        // The first spill write targets `<instance>/s-0/v9` (counter starts at
        // 0). Pre-populate that exact target so write_segment_v9 refuses to
        // overwrite it and returns an error mid-admission.
        let target = hist
            .spill_instance_dir
            .join("s-0")
            .join(SPILL_SEGMENT_SUBDIR);
        std::fs::create_dir_all(&target).expect("mk target");
        std::fs::write(target.join("marker"), b"populated").expect("write marker");

        let err = hist
            .load_segment("s", build_rowcount_segment(6))
            .expect_err("a failed write must surface as an error");
        assert!(
            format!("{err}").contains("populated") || format!("{err}").contains("overwrite"),
            "error must reflect the write failure: {err}"
        );
        assert!(!hist.has_segment("s"), "a failed admission loads nothing");
        assert_eq!(
            count_spill_enclosures(&hist.spill_instance_dir),
            0,
            "a failed write cleans its whole enclosing dir — no staging/final \
             orphan (nor the collision dir) survives in the instance root"
        );
    }

    /// Finding 2: `get_segment` distinguishes true absence (`Ok(None)`) from a
    /// spill decode failure (`Err`). A loaded-but-unreadable segment must NOT
    /// masquerade as absent, which would silently drop it from
    /// INFORMATION_SCHEMA / schema discovery and return wrong-but-successful
    /// metadata.
    #[test]
    fn get_segment_surfaces_decode_failure_as_err() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        hist.load_segment_with_datasource("s", "wiki", build_rowcount_segment(6))
            .expect("load");

        // A never-loaded id is true absence.
        assert!(
            hist.get_segment("missing").expect("no error").is_none(),
            "an unloaded id must be Ok(None)"
        );

        // Corrupt the cold segment's spilled bytes so it cannot be decoded.
        std::fs::remove_dir_all(hist.cache_dir().join("spill")).expect("wipe spill");
        match hist.get_segment("s") {
            Err(e) => assert!(
                format!("{e}").contains("re-read"),
                "must surface the decode failure: {e}"
            ),
            Ok(_) => panic!("a loaded-but-undecodable segment must surface as Err, not absence"),
        }
    }

    // -----------------------------------------------------------------------
    // FG-7 R2 findings 1/2/3/5
    // -----------------------------------------------------------------------

    /// Sum the `count` aggregation across a timeseries fan-out result.
    fn sum_cnt(results: &[QueryResult]) -> i64 {
        results
            .iter()
            .map(|r| match r {
                QueryResult::Timeseries(ts) => ts
                    .iter()
                    .map(|row| {
                        row.result
                            .get("cnt")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0)
                    })
                    .sum::<i64>(),
                _ => 0,
            })
            .sum()
    }

    /// A timeseries `count` query over an explicit `[start, end)` day.
    fn count_query_over(interval: &str) -> DruidQuery {
        serde_json::from_value(serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": [interval],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("build count query")
    }

    /// A segment whose real `__time` is 2024-01-01 but whose DECLARED header
    /// interval lies (2030) — the exact shape that made the v9 read path's
    /// header-trusting prune drop matching rows (finding 1).
    fn lying_header_segment(rows: usize) -> SegmentData {
        let mut seg = build_rowcount_segment(rows); // real __time 2024-01-01
        seg.interval = Interval {
            start_millis: millis_at("2030-01-01T00:00:00Z"),
            end_millis: millis_at("2030-01-02T00:00:00Z"),
        };
        seg
    }

    /// Finding 1: interval pruning must key on the segment's REAL `__time`
    /// min/max, not its declared header. An honest peer executes, so no
    /// forced-execution fallback fires; a lying-header segment whose real rows
    /// match the query would then be pruned and its rows silently lost. Pre-fix
    /// the query returns only the honest rows; post-fix it returns all rows.
    /// Verified in both residency modes.
    #[test]
    fn prune_keys_on_real_time_not_lying_header() {
        for spill in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            let hist = if spill {
                spill_historical(dir.path(), 10_000_000)
            } else {
                Historical::new(dir.path().to_path_buf(), 10_000_000)
            };
            hist.load_segment_with_datasource("honest", "wiki", build_rowcount_segment(6))
                .expect("load honest");
            hist.load_segment_with_datasource("liar", "wiki", lying_header_segment(4))
                .expect("load liar");

            // Query the REAL day; both segments' rows fall inside it.
            let res = hist
                .execute_query(&count_query_over(
                    "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z",
                ))
                .expect("execute");
            assert_eq!(
                sum_cnt(&res),
                10,
                "lying header must not drop the liar's matching rows (spill={spill})"
            );
        }
    }

    /// Finding 1: when EVERY routed segment has a lying header disjoint from the
    /// query, the fallback forces just ONE to execute — so the others' matching
    /// rows are lost. Deriving the prune interval from real `__time` makes all
    /// of them intersect and execute. Verified in both residency modes.
    #[test]
    fn all_lying_headers_return_every_matching_row() {
        for spill in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            let hist = if spill {
                spill_historical(dir.path(), 10_000_000)
            } else {
                Historical::new(dir.path().to_path_buf(), 10_000_000)
            };
            // Distinct row counts so a single forced execution is detectable
            // regardless of which segment the fallback happens to pick.
            hist.load_segment_with_datasource("liar_a", "wiki", lying_header_segment(6))
                .expect("load liar_a");
            hist.load_segment_with_datasource("liar_b", "wiki", lying_header_segment(4))
                .expect("load liar_b");

            let res = hist
                .execute_query(&count_query_over(
                    "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z",
                ))
                .expect("execute");
            assert_eq!(
                sum_cnt(&res),
                10,
                "all-lying-header fan-out must still return every matching row (spill={spill})"
            );
        }
    }

    /// A segment whose `__time` is deliberately NOT ascending yet whose
    /// public `time_sorted` flag is set `true` (a caller lie). Rows
    /// `[2030, 2024, 2030]`: a first/last shortcut would derive a `2030`-only
    /// prune interval and drop the middle `2024` row from a `2024` query.
    fn out_of_order_time_sorted_segment() -> SegmentData {
        let t2024 = millis_at("2024-01-01T00:00:00Z");
        let t2030 = millis_at("2030-01-01T00:00:00Z");
        let mut columns = HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![t2030, t2024, t2030]),
        );
        columns.insert("value".to_string(), ColumnData::Double(vec![1.0, 2.0, 3.0]));
        SegmentData {
            version: 9,
            num_rows: 3,
            interval: Interval {
                start_millis: t2024,
                end_millis: t2030 + 1,
            },
            dimensions: vec![],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: true, // LIE: not actually ascending.
        }
    }

    /// Finding 1 (CRITICAL, unit): `derive_prune_interval` must ALWAYS scan the
    /// real `__time` values and never trust the public, caller-mutable
    /// `time_sorted` flag. For unsorted rows `[2030, 2024, 2030]` flagged
    /// `time_sorted = true`, a first/last shortcut derives a `2030`-only
    /// interval; the full scan derives the true `[2024, 2030]`. This is the
    /// deterministic RED anchor: pre-fix `start_millis == 2030`, post-fix
    /// `start_millis == 2024`.
    #[test]
    fn derive_prune_interval_ignores_lying_time_sorted_flag() {
        let seg = out_of_order_time_sorted_segment();
        let t2024 = millis_at("2024-01-01T00:00:00Z");
        let t2030 = millis_at("2030-01-01T00:00:00Z");
        let interval = derive_prune_interval(&seg).expect("segment has a __time column");
        assert_eq!(
            interval.start_millis, t2024,
            "min must be the scanned 2024, not the first-element 2030 shortcut"
        );
        assert_eq!(interval.end_millis, t2030, "max must be the scanned 2030");
    }

    /// Finding 1 (CRITICAL, end-to-end): a liar segment claiming `time_sorted =
    /// true` over unsorted rows `[2030, 2024, 2030]` must NOT be interval-pruned
    /// from a 2024 query — its real `__time` intersects 2024. An HONEST segment
    /// legitimately covers 2024 and executes, so this is the "not all-pruned"
    /// case where the force-one fallback does NOT rescue the liar; a mis-prune
    /// would silently drop the liar's middle 2024 row. Spill mode (pruning is
    /// spill-gated by finding 2): pre-fix the flag is trusted, the liar's prune
    /// interval is a `2030`-only shortcut, it is pruned, ONLY the honest segment
    /// decodes+executes (`res.len() == 1`, `sum_cnt == 6`); post-fix the scanned
    /// `[2024, 2030]` intersects, so BOTH decode+execute (`res.len() == 2`) and
    /// the liar's 2024 row is counted (`sum_cnt == 7`). The reloaded liar's
    /// `time_sorted` is re-derived `false` on read, so the executor counts it
    /// correctly.
    #[test]
    fn lying_time_sorted_liar_is_not_pruned_in_spill() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = spill_historical(dir.path(), 10_000_000);
        hist.load_segment_with_datasource("honest", "wiki", build_rowcount_segment(6))
            .expect("load honest");
        hist.load_segment_with_datasource("liar", "wiki", out_of_order_time_sorted_segment())
            .expect("load liar");
        let decodes_before = hist.decode_count.load(Ordering::Relaxed);

        let res = hist
            .execute_query(&count_query_over(
                "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z",
            ))
            .expect("execute");
        assert_eq!(
            res.len(),
            2,
            "the liar must not be pruned — its real __time intersects 2024"
        );
        assert!(
            hist.decode_count.load(Ordering::Relaxed) >= decodes_before + 2,
            "both routed segments must decode (neither pruned)"
        );
        assert_eq!(
            sum_cnt(&res),
            7,
            "honest 6 + liar's middle 2024 row 1 — no row dropped by a false prune"
        );
    }

    /// FG-7 R19 (HIGH): a segment whose public `time_sorted` flag LIES (`true`
    /// over the unsorted rows `[2030, 2024, 2030]`) must answer a time-interval
    /// query IDENTICALLY whether it is heap-resident or spilled. On the heap the
    /// executor trusts the flag and binary-range-prunes (`pruned_row_range`)
    /// with `partition_point`, which is only correct on ASCENDING data — so a
    /// lying flag scans a wrong row range and drops/mis-counts the liar's real
    /// 2024 row. Spilled, the SAME segment reloads with `time_sorted` re-derived
    /// `false` from its persisted `__time`, so it full-scans and counts the 2024
    /// row. That is the residency-dependent answer this fix removes: pre-fix
    /// heap != spill; post-fix reconciling the heap flag from the real `__time`
    /// at load makes BOTH full-scan and count the row (`sum_cnt == 7`: honest 6
    /// + the liar's one real 2024 row).
    #[test]
    fn lying_time_sorted_answer_is_residency_independent() {
        let query = count_query_over("2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z");

        let heap_dir = tempfile::tempdir().expect("tempdir");
        let heap = Historical::new(heap_dir.path().to_path_buf(), 10_000_000);
        heap.load_segment_with_datasource("honest", "wiki", build_rowcount_segment(6))
            .expect("load honest (heap)");
        heap.load_segment_with_datasource("liar", "wiki", out_of_order_time_sorted_segment())
            .expect("load liar (heap)");
        let heap_res = heap.execute_query(&query).expect("execute (heap)");

        let spill_dir = tempfile::tempdir().expect("tempdir");
        let spill = spill_historical(spill_dir.path(), 10_000_000);
        spill
            .load_segment_with_datasource("honest", "wiki", build_rowcount_segment(6))
            .expect("load honest (spill)");
        spill
            .load_segment_with_datasource("liar", "wiki", out_of_order_time_sorted_segment())
            .expect("load liar (spill)");
        let spill_res = spill.execute_query(&query).expect("execute (spill)");

        assert_eq!(
            sum_cnt(&heap_res),
            sum_cnt(&spill_res),
            "heap and spill must return the same count for a lying-time_sorted segment \
             (residency-independent answer)"
        );
        assert_eq!(
            sum_cnt(&heap_res),
            7,
            "honest 6 + liar's one real 2024 row — no row dropped by a lying binary prune"
        );
    }

    /// FG-7 R19: the heap load boundary re-derives `time_sorted` from the real
    /// `__time`, so a LYING `true` flag over unsorted rows is corrected to
    /// `false` on the resident entry (the executor then full-scans), while an
    /// HONEST flag over ascending rows is left `true` unchanged (binary-range
    /// pruning stays engaged — the reconciliation is a no-op for a well-formed
    /// segment, leaving the heap segment byte-for-byte identical).
    #[test]
    fn heap_load_reconciles_time_sorted_from_real_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);
        hist.load_segment_with_datasource("liar", "wiki", out_of_order_time_sorted_segment())
            .expect("load liar");
        hist.load_segment_with_datasource("honest", "wiki", build_rowcount_segment(3))
            .expect("load honest");

        let flag_of = |id: &str| -> bool {
            let segments = hist.segments.read().expect("segment read lock");
            let entry = segments.get(id).expect("segment loaded");
            match &entry.residency {
                Residency::Resident(data) => data.time_sorted,
                Residency::Spilled { .. } => panic!("heap mode must be resident"),
            }
        };

        assert!(
            !flag_of("liar"),
            "a lying time_sorted=true over unsorted [2030, 2024, 2030] must be re-derived false"
        );
        assert!(
            flag_of("honest"),
            "an honest ascending (constant) __time keeps time_sorted=true (no-op reconcile)"
        );
    }

    /// FG-7 R21 (HIGH): the heap reconcile must fire even when the caller's
    /// `Arc<SegmentData>` is SHARED. R19 used `Arc::get_mut`, which returns
    /// `None` on any shared `Arc` (a caller-retained `SegmentSwapEntry.data`
    /// clone, a concurrent query, a re-fed `removed` entry), silently SKIPPING
    /// the reconcile and letting a lying `time_sorted = true` reach the query
    /// executor — heap binary-range-prunes a non-ascending segment and drops
    /// its real 2024 row. Here the caller holds a live clone of the liar's
    /// `Arc` across the `replace_segments` add, so `get_mut` would refuse:
    /// pre-fix the resident copy stays `true` and the heap under-counts;
    /// post-fix `Arc::make_mut` copy-on-writes a private, reconciled `false`
    /// copy, so heap == spill (`sum_cnt == 7`).
    #[test]
    fn replace_segments_shared_arc_lying_time_sorted_is_residency_independent() {
        let query = count_query_over("2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z");

        // HEAP via replace_segments with SHARED Arcs (the caller keeps live
        // clones), the exact edge `Arc::get_mut` refused to reconcile.
        let heap_dir = tempfile::tempdir().expect("tempdir");
        let heap = Historical::new(heap_dir.path().to_path_buf(), 10_000_000);
        let honest_arc = Arc::new(build_rowcount_segment(6));
        let liar_arc = Arc::new(out_of_order_time_sorted_segment());
        let _honest_held = Arc::clone(&honest_arc);
        let liar_held = Arc::clone(&liar_arc);
        heap.replace_segments(
            &[],
            vec![
                SegmentSwapEntry {
                    id: "honest".to_string(),
                    data: Arc::clone(&honest_arc),
                    datasource: Some("wiki".to_string()),
                },
                SegmentSwapEntry {
                    id: "liar".to_string(),
                    data: Arc::clone(&liar_arc),
                    datasource: Some("wiki".to_string()),
                },
            ],
        )
        .expect("heap replace add");
        let heap_res = heap.execute_query(&query).expect("execute (heap)");

        // The resident (query-visible) liar copy must be reconciled to false
        // despite the shared Arc — the deterministic RED anchor.
        {
            let segments = heap.segments.read().expect("segment read lock");
            let entry = segments.get("liar").expect("liar loaded");
            let resident = match &entry.residency {
                Residency::Resident(data) => data,
                Residency::Spilled { .. } => panic!("heap mode must be resident"),
            };
            assert!(
                !resident.time_sorted,
                "shared-Arc lying time_sorted=true over unsorted [2030, 2024, 2030] must be \
                 reconciled to false (R21); Arc::get_mut silently skipped it"
            );
            // make_mut installed a PRIVATE copy-on-write for the corrected lie,
            // so the resident Arc is a different allocation from the caller's.
            assert!(
                !Arc::ptr_eq(&liar_held, resident),
                "a corrected shared Arc must be a private copy-on-write allocation"
            );
        }
        assert!(
            liar_held.time_sorted,
            "make_mut must not mutate the caller's shared copy of the segment"
        );

        // SPILL reference (reloads with the flag re-derived from disk).
        let spill_dir = tempfile::tempdir().expect("tempdir");
        let spill = spill_historical(spill_dir.path(), 10_000_000);
        spill
            .load_segment_with_datasource("honest", "wiki", build_rowcount_segment(6))
            .expect("load honest (spill)");
        spill
            .load_segment_with_datasource("liar", "wiki", out_of_order_time_sorted_segment())
            .expect("load liar (spill)");
        let spill_res = spill.execute_query(&query).expect("execute (spill)");

        assert_eq!(
            sum_cnt(&heap_res),
            sum_cnt(&spill_res),
            "heap (shared-Arc replace) and spill must agree for a lying-time_sorted segment"
        );
        assert_eq!(
            sum_cnt(&heap_res),
            7,
            "honest 6 + liar's one real 2024 row — no row dropped by a shared-Arc-skipped prune"
        );
    }

    /// FG-7 R21: reconciling a SHARED but HONEST (well-formed) segment is a
    /// clone-free no-op. The corrected flag equals the caller's, so
    /// `Arc::make_mut` is never invoked — the resident `Arc` is the SAME
    /// allocation the caller handed in (asserted via `Arc::ptr_eq`), leaving the
    /// heap segment byte-for-byte identical even though the `Arc` is shared. The
    /// copy-on-write cost is paid only by a pathological caller-installed lie.
    #[test]
    fn replace_segments_shared_arc_honest_time_sorted_is_clone_free_no_op() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);

        let honest_arc = Arc::new(build_rowcount_segment(6));
        let held = Arc::clone(&honest_arc); // external strong ref → shared Arc

        hist.replace_segments(
            &[],
            vec![SegmentSwapEntry {
                id: "honest".to_string(),
                data: Arc::clone(&honest_arc),
                datasource: Some("wiki".to_string()),
            }],
        )
        .expect("replace add");

        let segments = hist.segments.read().expect("segment read lock");
        let entry = segments.get("honest").expect("segment loaded");
        let resident = match &entry.residency {
            Residency::Resident(data) => data,
            Residency::Spilled { .. } => panic!("heap mode must be resident"),
        };
        assert!(
            resident.time_sorted,
            "an honest ascending (constant) __time keeps time_sorted=true"
        );
        assert!(
            Arc::ptr_eq(&held, resident),
            "a well-formed shared segment must NOT be copy-on-write cloned (no-op reconcile): \
             the resident Arc must be the caller's exact allocation"
        );
    }

    /// Finding 5: the per-loaded-segment heap charge must use the REAL
    /// `size_of::<SegmentEntry>()`, not the retired 16 B (`2 * usize`) constant
    /// that modelled the long-gone `LoadedSegment`. The current entry also
    /// carries a residency handle, row count, and prune interval, so the old
    /// constant undercounted every entry — enough that many tiny/empty heap
    /// segments could slip real footprint past the cache quota. This asserts
    /// `segment_entry_bytes` charges the full entry footprint.
    #[test]
    fn segment_entry_bytes_charges_full_entry_footprint() {
        // The regression is only observable if `SegmentEntry` outgrew the 16 B
        // constant — which it has (residency + row count + prune interval).
        assert!(
            std::mem::size_of::<SegmentEntry>() > 2 * std::mem::size_of::<usize>(),
            "SegmentEntry ({}B) must exceed the retired 16B (2*usize) charge",
            std::mem::size_of::<SegmentEntry>()
        );

        let id = "a-representative-segment-id".to_string();
        let estimated = 4096_u64;
        let schema_bytes = 512_u64;
        let charged = segment_entry_bytes(&id, estimated, schema_bytes);
        let expected = u64::try_from(id.capacity()).expect("cap")
            + estimated
            + schema_bytes
            + u64::try_from(std::mem::size_of::<SegmentEntry>()).expect("size");
        assert_eq!(
            charged, expected,
            "the per-entry heap charge must fold size_of::<SegmentEntry>() and schema heap"
        );

        // End-to-end: loading many empty heap segments reflects the full
        // per-entry overhead in `cache_bytes_used`, and the incremental ledger
        // stays exactly equal to the full-map oracle (which folds the same
        // helper, so the new size is validated on both sides).
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), u64::MAX);
        for i in 0..64_u32 {
            hist.load_segment(&format!("seg-{i:04}"), build_rowcount_segment(0))
                .expect("load");
        }
        let used = hist.info().cache_bytes_used;
        let entry_overhead = 64_u64
            .saturating_mul(u64::try_from(std::mem::size_of::<SegmentEntry>()).expect("size"));
        assert!(
            used >= entry_overhead,
            "cache_bytes_used {used} must include ≥ {entry_overhead} of per-entry \
             overhead (64 × size_of::<SegmentEntry>()), not the legacy 16B charge"
        );
        assert_exact_cache_invariant(&hist);
    }

    /// Finding 3: a single segment whose decoded weight exceeds the whole
    /// budget must be served query-local, NOT pinned in the LRU over budget.
    /// Pre-fix `admit` inserted it anyway, driving `resident_bytes` permanently
    /// above the limit.
    #[test]
    fn spill_oversized_segment_not_pinned_over_budget() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A 1-byte budget is smaller than any real segment's estimate.
        let hist = spill_historical(dir.path(), 1);
        hist.load_segment_with_datasource("big", "wiki", build_rowcount_segment(6))
            .expect("load");

        // The query is still answered correctly (query-local decode)...
        let data = hist.get_segment("big").expect("get").expect("resident");
        assert_eq!(data.num_rows, 6);
        assert_eq!(
            total_count(&hist, "wiki"),
            6,
            "oversized segment stays queryable"
        );

        // ...but nothing is pinned over the budget.
        assert!(
            hist.resident_bytes.load(Ordering::Relaxed) <= 1,
            "oversized segment must not be pinned over budget (finding 3)"
        );
        assert!(
            !hist.resident_lru.lock().expect("lru").contains("big"),
            "oversized segment must never enter the LRU"
        );
    }

    /// Finding 3 regression: a within-budget segment is still pinned normally.
    #[test]
    fn spill_within_budget_segment_is_pinned() {
        let dir = tempfile::tempdir().expect("tempdir");
        let one = estimate_segment_bytes(&build_rowcount_segment(1));
        let hist = spill_historical(dir.path(), one.saturating_mul(4));
        hist.load_segment("s", build_rowcount_segment(1))
            .expect("load");
        hist.get_segment("s").expect("get").expect("materialize");
        assert!(
            hist.resident_lru.lock().expect("lru").contains("s"),
            "a within-budget segment must be pinned"
        );
        let resident = hist.resident_bytes.load(Ordering::Relaxed);
        assert!(resident > 0 && resident <= one.saturating_mul(4));
    }

    /// Finding 5: a segment evicted from the LRU while an external `Arc` keeps
    /// its decode alive must be RE-ADMITTED on reuse (not left orphaned outside
    /// the LRU). Pre-fix the upgrade path only `touch`ed — a no-op for an
    /// already-evicted node — so the live decode stayed outside the LRU and the
    /// next query re-read it needlessly.
    #[test]
    fn evicted_but_live_segment_is_readmitted_on_reuse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let one = estimate_segment_bytes(&build_rowcount_segment(1));
        // Budget holds exactly ONE 1-row resident (one <= budget < 2*one).
        let budget = one.saturating_add(one / 2);
        let hist = spill_historical(dir.path(), budget);
        hist.load_segment("a", build_rowcount_segment(1))
            .expect("load a");
        hist.load_segment("b", build_rowcount_segment(1))
            .expect("load b");

        // Materialize `a` and HOLD its Arc so the decode survives eviction.
        let a_arc = hist
            .get_segment("a")
            .expect("get a")
            .expect("materialize a");
        assert!(hist.resident_lru.lock().expect("lru").contains("a"));

        // Materialize `b` — evicts `a` (budget holds one), but `a`'s decode is
        // still alive via `a_arc`, so its weak still upgrades.
        hist.get_segment("b")
            .expect("get b")
            .expect("materialize b");
        assert!(
            !hist.resident_lru.lock().expect("lru").contains("a"),
            "a must be evicted by b under a one-slot budget"
        );

        // Re-access `a`: its weak upgrades (no re-decode), and it must be
        // re-admitted to the LRU (evicting b to fit).
        let decodes_before = hist.decode_count.load(Ordering::Relaxed);
        let reused = hist.get_segment("a").expect("get a2").expect("reuse a");
        assert!(
            Arc::ptr_eq(&reused, &a_arc),
            "reuse must return the same live decode"
        );
        assert_eq!(
            hist.decode_count.load(Ordering::Relaxed),
            decodes_before,
            "reuse must upgrade the live weak, not re-decode"
        );
        assert!(
            hist.resident_lru.lock().expect("lru").contains("a"),
            "an evicted-but-live segment must be re-admitted on reuse (finding 5)"
        );
        // `a` is counted exactly once (no double accounting).
        let resident = hist.resident_bytes.load(Ordering::Relaxed);
        assert!(resident <= budget, "resident bytes must stay within budget");
        assert_eq!(
            resident,
            hist.resident_lru.lock().expect("lru").total_bytes(),
            "resident ledger must equal the LRU's own sum (a counted once)"
        );
        drop(a_arc);
    }

    /// Finding 2: two Historicals sharing one `cache_dir` occupy disjoint spill
    /// roots, so constructing the second must NOT wipe the first's live spilled
    /// bytes. Pre-fix the second's constructor `remove_dir_all(cache_dir/spill)`
    /// destroyed H1's on-disk segment.
    #[test]
    fn two_historicals_sharing_cache_dir_do_not_wipe_each_other() {
        let dir = tempfile::tempdir().expect("tempdir");
        let h1 = spill_historical(dir.path(), 10_000_000);
        h1.load_segment_with_datasource("s", "wiki", build_rowcount_segment(6))
            .expect("h1 load");

        // Construct H2 over the SAME cache_dir. Its janitor must leave H1's
        // live instance root alone.
        let _h2 = spill_historical(dir.path(), 10_000_000);

        match h1.get_segment("s") {
            Ok(Some(data)) => assert_eq!(data.num_rows, 6),
            other => {
                panic!("H2's construction wiped H1's live spilled bytes: {other:?}")
            }
        }
    }

    /// Findings 3+4: the same logical `<id>-<counter>` in two instances resolves
    /// to distinct physical paths (disjoint instance roots), so neither can open
    /// the other's bytes. Both remain independently decodable with their own
    /// rows.
    #[test]
    fn spill_instance_roots_give_disjoint_physical_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let h1 = spill_historical(dir.path(), 10_000_000);
        let h2 = spill_historical(dir.path(), 10_000_000);
        assert_ne!(
            h1.spill_instance_dir, h2.spill_instance_dir,
            "two instances must not share a spill root"
        );

        // Identical logical id + counter (both start at 0), different bytes.
        h1.load_segment("x", build_rowcount_segment(1))
            .expect("h1 load");
        h2.load_segment("x", build_rowcount_segment(2))
            .expect("h2 load");
        assert_eq!(
            h1.get_segment("x")
                .expect("h1 get")
                .expect("resident")
                .num_rows,
            1
        );
        assert_eq!(
            h2.get_segment("x")
                .expect("h2 get")
                .expect("resident")
                .num_rows,
            2
        );
    }
}
