// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Segment serving, mmap, and query execution for FerroDruid.
//!
//! The [`Historical`] node loads segments from deep storage, caches them locally,
//! and executes queries against them. It is the primary query-serving component.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_common::types::DataSource;
use ferrodruid_query::scan::ScanResult;
use ferrodruid_query::{DruidQuery, QueryResult, execute_query};
use ferrodruid_segment::{SegmentData, check_null_generation};

// ---------------------------------------------------------------------------
// HistoricalInfo
// ---------------------------------------------------------------------------

/// Diagnostic information about a Historical node.
#[derive(Debug, Clone)]
pub struct HistoricalInfo {
    /// Number of loaded segments.
    pub segment_count: usize,
    /// Loaded-segment payload weight currently admitted, in bytes.
    ///
    /// This is the exact, incrementally-maintained sum of each loaded
    /// segment's estimated heap plus its id/datasource string bytes — the
    /// quantity the cache limit bounds. It deliberately does NOT include the
    /// Historical's own index overhead (the two routing `HashMap` bucket
    /// tables), which is a small O(loaded segments) term of pointers/control
    /// bytes, negligible next to segment payload for realistic segment sizes.
    /// It is therefore a payload-weight bound, not a hard resident-memory
    /// total for the process.
    pub cache_bytes_used: u64,
    /// Maximum admitted segment-payload weight in bytes (the cache limit).
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
    segments: Arc<RwLock<HashMap<String, LoadedSegment>>>,
    /// Local cache directory for segments.
    cache_dir: PathBuf,
    /// Maximum bytes to cache.
    max_cache_bytes: u64,
    /// Current cache size in bytes.
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
}

#[derive(Clone)]
struct LoadedSegment {
    data: Arc<SegmentData>,
    estimated_bytes: u64,
}

struct PreparedSwapEntry {
    id: String,
    data: Arc<SegmentData>,
    datasource: Option<(String, String)>,
    estimated_bytes: u64,
}

fn segment_entry_bytes(id: &String, loaded: &LoadedSegment) -> u64 {
    u64::try_from(id.capacity())
        .unwrap_or(u64::MAX)
        .saturating_add(loaded.estimated_bytes)
        .saturating_add(u64::try_from(2 * std::mem::size_of::<usize>()).unwrap_or(u64::MAX))
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
    pub fn with_strict_null_generation(
        cache_dir: PathBuf,
        max_cache_bytes: u64,
        strict_null_generation: bool,
    ) -> Self {
        Self {
            segments: Arc::new(RwLock::new(HashMap::new())),
            cache_dir,
            max_cache_bytes,
            current_cache_bytes: Arc::new(AtomicU64::new(0)),
            strict_null_generation,
            segment_datasources: Arc::new(RwLock::new(HashMap::new())),
            initial_load_complete: Arc::new(AtomicBool::new(true)),
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
        let id = segment_id.to_owned();
        let loaded = LoadedSegment {
            estimated_bytes: estimate_segment_bytes(&segment),
            data: Arc::new(segment),
        };

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

        let added_entries = segment_entry_bytes(&id, &loaded);
        let current = self.current_cache_bytes.load(Ordering::Relaxed);
        let next = cache_bytes_after_delta(current, 0, added_entries);
        ensure_cache_limit(next, self.max_cache_bytes)?;

        let replaced = segments.insert(id, loaded);
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
        let segment_key = segment_id.to_owned();
        let datasource_key = segment_id.to_owned();
        let datasource_value = datasource.to_owned();
        let loaded = LoadedSegment {
            estimated_bytes: estimate_segment_bytes(&segment),
            data: Arc::new(segment),
        };
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
        let added_entries = segment_entry_bytes(&segment_key, &loaded)
            .saturating_add(datasource_entry_bytes(&datasource_key, &datasource_value));
        let current = self.current_cache_bytes.load(Ordering::Relaxed);
        let next = cache_bytes_after_delta(current, 0, added_entries);
        ensure_cache_limit(next, self.max_cache_bytes)?;

        let replaced_segment = segments.insert(segment_key, loaded);
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
        let segments = self.segments.read().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment read lock: {e}"))
        })?;
        let loaded = segments.get(segment_id).ok_or_else(|| {
            DruidError::Segment(format!(
                "cannot map datasource for unloaded segment: {segment_id}"
            ))
        })?;
        check_null_generation(
            datasource,
            loaded.data.as_ref(),
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
        let mut segments = self.segments.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire segment write lock: {e}"))
        })?;
        let mut ds_map = self.segment_datasources.write().map_err(|e| {
            DruidError::Internal(format!("failed to acquire datasource write lock: {e}"))
        })?;
        let (stored_segment_id, loaded) = segments
            .get_key_value(segment_id)
            .ok_or_else(|| DruidError::Query(format!("segment not loaded: {segment_id}")))?;
        let mut removed_entries = segment_entry_bytes(stored_segment_id, loaded);
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
    /// whose segment is not resident, e.g. after a restart). The entries
    /// actually removed are returned — including their data and datasource
    /// mapping — so a caller can restore them verbatim to roll back.
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
                removed_entries =
                    removed_entries.saturating_add(segment_entry_bytes(stored_id, loaded));
                if let Some((stored_ds_id, datasource)) = ds_map.get_key_value(*id) {
                    removed_entries = removed_entries
                        .saturating_add(datasource_entry_bytes(stored_ds_id, datasource));
                }
            }
        }

        let mut added_entries = 0_u64;
        for entry in &add {
            let loaded = LoadedSegment {
                data: Arc::clone(&entry.data),
                estimated_bytes: entry.estimated_bytes,
            };
            added_entries = added_entries.saturating_add(segment_entry_bytes(&entry.id, &loaded));

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
                removed.push(SegmentSwapEntry {
                    id: id.clone(),
                    data: loaded.data,
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
            segments.insert(
                entry.id,
                LoadedSegment {
                    data: entry.data,
                    estimated_bytes: entry.estimated_bytes,
                },
            );
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
        Self::execute_query_on_snapshot(query, &segments, &ds_map)
    }

    fn execute_query_on_snapshot(
        query: &DruidQuery,
        segments: &HashMap<String, LoadedSegment>,
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
                let branch_results = Self::execute_query_on_snapshot(branch, segments, ds_map)?;
                results.push(merge_scan_branch(branch, branch_results)?);
            }
            return Ok(results);
        }

        let target_ds = query_datasources(query)?;

        let mut results = Vec::new();

        for (seg_id, segment) in segments.iter() {
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

            match execute_query(query, &segment.data) {
                Ok(result) => results.push(result),
                Err(e) => {
                    tracing::error!(segment_id = %seg_id, error = %e, "query failed on segment");
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

    /// Get a clone of the loaded segment by ID, if present.
    ///
    /// Returns the shared `Arc<SegmentData>` so callers can inspect
    /// dimensions/metrics/columns without acquiring a lock.
    pub fn get_segment(&self, segment_id: &str) -> Option<Arc<SegmentData>> {
        let segments = self.segments.read().ok()?;
        segments
            .get(segment_id)
            .map(|loaded| Arc::clone(&loaded.data))
    }

    /// Get the data source name associated with a loaded segment, if
    /// any was registered via [`set_segment_datasource`].
    ///
    /// [`set_segment_datasource`]: Historical::set_segment_datasource
    pub fn segment_datasource(&self, segment_id: &str) -> Option<String> {
        let ds_map = self.segment_datasources.read().ok()?;
        ds_map.get(segment_id).cloned()
    }

    /// Get diagnostic information about this Historical node.
    pub fn info(&self) -> HistoricalInfo {
        HistoricalInfo {
            segment_count: self.segment_count(),
            cache_bytes_used: self.current_cache_bytes.load(Ordering::Relaxed),
            cache_bytes_max: self.max_cache_bytes,
        }
    }

    /// Get the local cache directory path.
    pub fn cache_dir(&self) -> &PathBuf {
        &self.cache_dir
    }
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
    segments: &HashMap<String, LoadedSegment>,
    datasources: &HashMap<String, String>,
) -> u64 {
    CACHE_STATE_FULL_FOLDS.with(|folds| folds.set(folds.get().saturating_add(1)));
    let bytes = segments.iter().fold(0_u64, |total, (id, loaded)| {
        total.saturating_add(segment_entry_bytes(id, loaded))
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
            ferrodruid_segment::column::ColumnData::Complex(v) => {
                allocation_bytes::<u8>(v.capacity())
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

    fn single_entry_cache_bytes(id: &str, datasource: Option<&str>, data: &SegmentData) -> u64 {
        let mut segments = HashMap::new();
        segments.insert(
            id.to_string(),
            LoadedSegment {
                data: Arc::new(build_rowcount_segment(0)),
                estimated_bytes: estimate_segment_bytes(data),
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
}
