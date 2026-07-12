// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Callee-side axum router for the historical role.
//!
//! ## Wave 40.LL → 41.OO upgrade path
//!
//! Wave 40.LL shipped *stub* handlers so the cross-process wire was
//! observable end to end. Wave 41.OO replaces those stubs with a real
//! implementation:
//!
//! - `POST /druid/v2/native` — accepts both the W4 [`SegmentQuery`]
//!   shape (a free-form `query` string the historical echoes back) and
//!   the new [`crate::native_query::NativeQuery`] shape (real
//!   timeseries / scan execution against the loaded segment artifact).
//!   The handler dispatches on the request body: an object carrying a
//!   `queryType` field is decoded as a [`NativeQuery`] and executed
//!   against the segment store; everything else falls back to the W4
//!   echo path so the existing wire tests keep passing.
//! - `POST /druid/v1/historical/load` — accepts a
//!   [`SegmentLoadCommand`], registers it as `Loading`, then *really*
//!   reads the JSON-Lines segment artifact at
//!   `<deep_storage_root>/<data_source>/<segment_id>/segment.jsonl`
//!   into the in-memory segment store. The simulated tokio-timer state
//!   machine still flips state to `Loaded` on success (and to `Failed`
//!   when the artifact cannot be read).
//! - `POST /druid/v1/historical/drop` — accepts a
//!   [`SegmentDropCommand`], removes the segment from both the load
//!   table and the segment store, returns `Dropped`.
//! - `GET /druid/v1/historical/loadstatus` — returns the full
//!   `segment_id -> SegmentLoadState` table.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use ferrodruid_deep_storage::{DeepStorage, Segment};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::native_query::{NativeQuery, NativeQueryResult};
use crate::types::{
    LoadStatusReport, SegmentDropCommand, SegmentLoadCommand, SegmentLoadState, SegmentQuery,
    SegmentQueryResponse,
};

/// In-memory segment store used by the historical role. Keys are
/// segment ids; values are the parsed [`Segment`] artifact loaded from
/// deep storage.
pub type SegmentStore = HashMap<String, Arc<Segment>>;

/// Default cap on the number of segments the historical HTTP server
/// retains in its load-state table.
///
/// DD R35: the load handler tracks per-segment load state in an
/// in-memory map. Without a cap a caller can submit unbounded unique
/// segment ids and grow retained state forever (terminal Failed/Dropped
/// states are never evicted) and duplicate load commands spawn duplicate
/// loaders. New loads preferentially evict a terminal (Failed/Dropped)
/// status entry — never a `Loaded` segment that still backs a live
/// artifact in `store`; when every retained segment is still active the
/// server returns 503.
const DEFAULT_MAX_SEGMENTS: usize = 8192;

/// Load-state table guarded by a single mutex.
///
/// DD R36: alongside each segment's [`SegmentLoadState`] it tracks a monotonic
/// *generation* — bumped on every new load AND on drop — so a slow loader that
/// completes after its segment was dropped or superseded can detect it no longer
/// owns the entry and discard its result instead of resurrecting a dropped
/// segment. The state and generation maps are behind one mutex so the
/// idempotency check, capacity check, generation stamp, and commit are atomic.
#[derive(Debug, Default)]
struct SegmentTable {
    states: HashMap<String, SegmentLoadState>,
    gens: HashMap<String, u64>,
    next_gen: u64,
}

impl SegmentTable {
    fn len(&self) -> usize {
        self.states.len()
    }

    fn get(&self, id: &str) -> Option<SegmentLoadState> {
        self.states.get(id).copied()
    }

    fn current_gen(&self, id: &str) -> Option<u64> {
        self.gens.get(id).copied()
    }

    /// Stamp a fresh generation and set `state` for `id`, returning the new
    /// generation. Used for `Loading` (begin a load) and terminal transitions.
    fn stamp(&mut self, id: &str, state: SegmentLoadState) -> u64 {
        let g = self.next_gen;
        self.next_gen = self.next_gen.wrapping_add(1);
        self.states.insert(id.to_string(), state);
        self.gens.insert(id.to_string(), g);
        g
    }

    fn remove(&mut self, id: &str) {
        self.states.remove(id);
        self.gens.remove(id);
    }

    /// Find an evictable terminal entry (Failed/Dropped/Unknown) — never a
    /// `Loaded` segment (it still backs a live artifact in `store`) nor a
    /// `Loading` segment (a loader is in flight).
    fn find_terminal(&self) -> Option<String> {
        self.states
            .iter()
            .find(|(_, s)| {
                matches!(
                    s,
                    SegmentLoadState::Failed
                        | SegmentLoadState::Dropped
                        | SegmentLoadState::Unknown
                )
            })
            .map(|(k, _)| k.clone())
    }
}

/// Per-server state for the historical axum router.
///
/// Tracks every segment the historical has been told to load / drop
/// (the `segments` field), plus the parsed segment artifacts the
/// historical can answer queries against (the `store` field).
pub struct HistoricalServerState {
    /// Cluster-unique historical identifier.
    pub historical_id: String,
    /// Tier label this historical serves.
    pub tier: String,
    /// Per-segment load state + generation for every segment the
    /// coordinator has told this historical about.
    segments: Mutex<SegmentTable>,
    /// Maximum number of entries retained in `segments` at once
    /// (DD R35 admission bound).
    max_segments: usize,
    /// `segment_id -> Segment` for every segment currently `Loaded`.
    /// Populated by [`HistoricalServerState::load_segment_artifact`]
    /// and consumed by the query handler.
    store: Mutex<SegmentStore>,
    /// Filesystem root the simulated loader reads segment artifacts
    /// from. Defaults to `/tmp/ferrodruid-deep`. Tests override this
    /// to point at a `tempfile::tempdir()`.
    pub deep_storage_root: PathBuf,
    /// When `true` (Wave 41.OO opt-in), the load handler reads the
    /// JSON-Lines segment artifact from the deep-storage root and
    /// flips state to `Loaded` on success / `Failed` on missing or
    /// malformed artifact. When `false` (Wave 40.LL default for
    /// backwards-compat), the load handler skips artifact I/O and
    /// flips state to `Loaded` unconditionally — the W4 stub
    /// behaviour. Defaults to `false` so the wire-only integration
    /// tests stay green; the historical binary turns this on via
    /// `--real-loader`.
    pub real_loader: bool,
    /// Time the simulated loader stays in `Loading` before flipping
    /// to `Loaded`. Defaults to 50 ms. Real artifact I/O happens
    /// during this window.
    pub loading_to_loaded: Duration,
    /// Wave 42.RR: optional remote deep-storage backend (S3 / GCS /
    /// Azure / in-memory). When set, the loader downloads
    /// `<data_source>/<segment_id>/segment.jsonl` from the backend
    /// into a local cache directory under `deep_storage_root` and then
    /// parses it. When `None` the loader reads the JSON-Lines artifact
    /// directly from `deep_storage_root` (Wave 41.OO local-FS path).
    remote: Option<Arc<dyn DeepStorage>>,
}

impl std::fmt::Debug for HistoricalServerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoricalServerState")
            .field("historical_id", &self.historical_id)
            .field("tier", &self.tier)
            .field("segments", &self.segments)
            .field("max_segments", &self.max_segments)
            .field("deep_storage_root", &self.deep_storage_root)
            .field("real_loader", &self.real_loader)
            .field("loading_to_loaded", &self.loading_to_loaded)
            .field("remote_set", &self.remote.is_some())
            .finish()
    }
}

impl Default for HistoricalServerState {
    fn default() -> Self {
        Self {
            historical_id: format!("historical-{}", Uuid::new_v4()),
            tier: "default".into(),
            segments: Mutex::new(SegmentTable::default()),
            max_segments: DEFAULT_MAX_SEGMENTS,
            store: Mutex::new(HashMap::new()),
            deep_storage_root: PathBuf::from("/tmp/ferrodruid-deep"),
            real_loader: false,
            loading_to_loaded: Duration::from_millis(50),
            remote: None,
        }
    }
}

impl HistoricalServerState {
    /// Construct fresh server state with the supplied identity and
    /// loader timing. `loading_to_loaded` may be zero for tests that
    /// want immediate `Loaded` state. Deep-storage root defaults to
    /// `/tmp/ferrodruid-deep`.
    #[must_use]
    pub fn with_config(
        historical_id: impl Into<String>,
        tier: impl Into<String>,
        loading_to_loaded: Duration,
    ) -> Self {
        Self {
            historical_id: historical_id.into(),
            tier: tier.into(),
            segments: Mutex::new(SegmentTable::default()),
            max_segments: DEFAULT_MAX_SEGMENTS,
            store: Mutex::new(HashMap::new()),
            deep_storage_root: PathBuf::from("/tmp/ferrodruid-deep"),
            real_loader: false,
            loading_to_loaded,
            remote: None,
        }
    }

    /// Construct fresh server state with a custom deep-storage root
    /// AND with real-loader semantics enabled (Wave 41.OO opt-in).
    /// Used by tests to point the loader at a `tempfile::tempdir()`
    /// and exercise real artifact I/O.
    #[must_use]
    pub fn with_root(
        historical_id: impl Into<String>,
        tier: impl Into<String>,
        loading_to_loaded: Duration,
        deep_storage_root: PathBuf,
    ) -> Self {
        Self {
            historical_id: historical_id.into(),
            tier: tier.into(),
            segments: Mutex::new(SegmentTable::default()),
            max_segments: DEFAULT_MAX_SEGMENTS,
            store: Mutex::new(HashMap::new()),
            deep_storage_root,
            real_loader: true,
            loading_to_loaded,
            remote: None,
        }
    }

    /// Wave 42.RR: construct fresh server state with a remote
    /// deep-storage backend (`S3DeepStorage`, `InMemoryDeepStorage`,
    /// etc.) plus a local cache directory used to materialise
    /// downloaded segment artifacts. The cache directory follows the
    /// same `<root>/<data_source>/<segment_id>/segment.jsonl` layout
    /// as the local-FS path, so the parser can be reused unchanged.
    #[must_use]
    pub fn with_remote(
        historical_id: impl Into<String>,
        tier: impl Into<String>,
        loading_to_loaded: Duration,
        cache_root: PathBuf,
        remote: Arc<dyn DeepStorage>,
    ) -> Self {
        Self {
            historical_id: historical_id.into(),
            tier: tier.into(),
            segments: Mutex::new(SegmentTable::default()),
            max_segments: DEFAULT_MAX_SEGMENTS,
            store: Mutex::new(HashMap::new()),
            deep_storage_root: cache_root,
            real_loader: true,
            loading_to_loaded,
            remote: Some(remote),
        }
    }

    /// Override the retained-segment cap (DD R35). Used by tests to
    /// exercise the admission bound cheaply.
    #[must_use]
    pub fn with_max_segments(mut self, max_segments: usize) -> Self {
        self.max_segments = max_segments.max(1);
        self
    }

    /// Snapshot the tracked segment table.
    pub async fn snapshot(&self) -> HashMap<String, SegmentLoadState> {
        self.segments.lock().await.states.clone()
    }

    /// Snapshot the segment store keys (test introspection).
    pub async fn loaded_segment_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.store.lock().await.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Resolve the on-disk path the loader reads a segment artifact
    /// from.
    #[must_use]
    pub fn artifact_path(&self, data_source: &str, segment_id: &str) -> PathBuf {
        self.deep_storage_root
            .join(data_source)
            .join(segment_id)
            .join("segment.jsonl")
    }

    /// Read the segment artifact for `(data_source, segment_id)` from
    /// the deep-storage root and insert it into the segment store.
    /// Used by the load handler; exposed for tests so they can pre-seed
    /// the store without going through the HTTP wire.
    ///
    /// # Errors
    ///
    /// Returns the underlying `SegmentArtifactError` when the artifact
    /// is missing or malformed.
    pub async fn load_segment_artifact(
        &self,
        data_source: &str,
        segment_id: &str,
    ) -> Result<(), ferrodruid_deep_storage::SegmentArtifactError> {
        let segment = self.build_segment_artifact(data_source, segment_id).await?;
        let mut store = self.store.lock().await;
        store.insert(segment_id.to_string(), Arc::new(segment));
        Ok(())
    }

    /// Read + parse a segment artifact WITHOUT inserting it into the store.
    ///
    /// DD R36: the load handler uses this so the store insert can be performed
    /// under the segment-state lock and gated on the load generation — a loader
    /// that completes after its segment was dropped/superseded must not insert a
    /// stale artifact (which would resurrect a dropped segment).
    async fn build_segment_artifact(
        &self,
        data_source: &str,
        segment_id: &str,
    ) -> Result<Segment, ferrodruid_deep_storage::SegmentArtifactError> {
        // Wave 42.RR: when a remote backend is configured, download
        // the segment directory into the local cache root first; then
        // fall through to the existing parse-from-disk path so all
        // backends share the same JSON-Lines parser.
        if let Some(remote) = self.remote.as_ref() {
            let cache_dir = self.deep_storage_root.join(data_source).join(segment_id);
            tokio::fs::create_dir_all(&cache_dir).await?;
            remote
                .download_segment(data_source, segment_id, &cache_dir)
                .await
                .map_err(|e| {
                    ferrodruid_deep_storage::SegmentArtifactError::Io(std::io::Error::other(
                        e.to_string(),
                    ))
                })?;
        }
        let path = self.artifact_path(data_source, segment_id);
        Segment::read_jsonl_async(&path).await
    }

    /// Direct insert of a segment into the store. Test helper: skips
    /// the deep-storage round trip.
    pub async fn insert_segment(&self, segment_id: &str, segment: Segment) {
        let mut store = self.store.lock().await;
        store.insert(segment_id.to_string(), Arc::new(segment));
        let mut segs = self.segments.lock().await;
        segs.stamp(segment_id, SegmentLoadState::Loaded);
    }

    /// Look up a segment in the store. Returns `None` when the segment
    /// has not been loaded.
    pub async fn get_segment(&self, segment_id: &str) -> Option<Arc<Segment>> {
        self.store.lock().await.get(segment_id).cloned()
    }
}

/// Build the axum [`Router`] the historical binary mounts on its
/// HTTP server.
pub fn router(state: Arc<HistoricalServerState>) -> Router {
    Router::new()
        .route("/druid/v2/native", post(handle_query))
        .route("/druid/v1/historical/load", post(handle_load))
        .route("/druid/v1/historical/drop", post(handle_drop))
        .route("/druid/v1/historical/loadstatus", get(handle_loadstatus))
        .with_state(state)
}

async fn handle_query(
    State(state): State<Arc<HistoricalServerState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<SegmentQueryResponse> {
    let started = std::time::Instant::now();

    // Wave 41.OO: dispatch on body shape.
    //
    // - When the body has a `queryType` field (i.e. it's a real
    //   `NativeQuery`), decode it and execute against the segment
    //   store, returning real rows.
    // - Otherwise fall back to the W4 stub behaviour: decode as
    //   `SegmentQuery` and echo the `query` string back as a
    //   single-row response.
    if body.get("queryType").is_some() {
        match serde_json::from_value::<NativeQuery>(body.clone()) {
            Ok(native) => {
                let segment_id = body
                    .get("segmentId")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let segment = state.get_segment(&segment_id).await;
                let (segment_id_out, rows) = match segment {
                    Some(seg) => {
                        let result = native.execute(&seg);
                        (segment_id.clone(), encode_native_result(result))
                    }
                    None => {
                        tracing::debug!(
                            segment_id = %segment_id,
                            "historical received native query for un-loaded segment",
                        );
                        (segment_id.clone(), Vec::new())
                    }
                };
                let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                return Json(SegmentQueryResponse {
                    segment_id: segment_id_out,
                    rows,
                    elapsed_ms,
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to decode NativeQuery body, falling back to echo");
            }
        }
    }

    // W4 echo fallback.
    let query: SegmentQuery = match serde_json::from_value(body) {
        Ok(q) => q,
        Err(_) => {
            return Json(SegmentQueryResponse {
                segment_id: String::new(),
                rows: vec![vec![serde_json::Value::Null]],
                elapsed_ms: 0,
            });
        }
    };
    tracing::debug!(
        segment_id = %query.segment_id,
        query = %query.query,
        "historical received scatter query (echo path)",
    );
    let echo = serde_json::Value::String(query.query.clone());
    Json(SegmentQueryResponse {
        segment_id: query.segment_id.clone(),
        rows: vec![vec![echo]],
        elapsed_ms: 0,
    })
}

/// Encode a [`NativeQueryResult`] into the row-vector shape the W4
/// [`SegmentQueryResponse`] carries.
///
/// - Timeseries buckets are flattened to one row each, with
///   `[timestamp_ms, result_object]` columns.
/// - Scan rows are passed through as one-element rows wrapping the
///   row's JSON object.
/// - GroupBy / TopN rows are passed through as one-element rows
///   wrapping the result-row JSON object (same wire shape as scan,
///   distinguished by the broker's per-query merge dispatch).
fn encode_native_result(result: NativeQueryResult) -> Vec<Vec<serde_json::Value>> {
    match result {
        NativeQueryResult::Timeseries(buckets) => buckets
            .into_iter()
            .map(|b| {
                vec![
                    serde_json::Value::Number(b.timestamp_ms.into()),
                    serde_json::Value::Object(b.result),
                ]
            })
            .collect(),
        NativeQueryResult::Scan(rows)
        | NativeQueryResult::GroupBy(rows)
        | NativeQueryResult::TopN(rows) => rows
            .into_iter()
            .map(|r| vec![serde_json::Value::Object(r)])
            .collect(),
    }
}

async fn handle_load(
    State(state): State<Arc<HistoricalServerState>>,
    Json(cmd): Json<SegmentLoadCommand>,
) -> Result<Json<LoadStatusReport>, SegmentCapacityExceeded> {
    let segment_id = cmd.segment_id.clone();
    let initial = LoadStatusReport {
        segment_id: segment_id.clone(),
        state: SegmentLoadState::Loading,
        message: String::new(),
    };
    // DD R35/R36: atomic admission — idempotency, capacity, and the generation
    // stamp all under one lock. `gen` is the load generation this loader owns; a
    // drop or a newer load bumps the segment's generation so this loader can
    // detect it was superseded and discard its result (instead of resurrecting
    // a dropped segment).
    let load_gen = {
        let mut t = state.segments.lock().await;
        match t.get(&segment_id) {
            // Idempotent re-load — already Loading/Loaded returns current state
            // without spawning a second loader.
            Some(existing @ (SegmentLoadState::Loading | SegmentLoadState::Loaded)) => {
                return Ok(Json(LoadStatusReport {
                    segment_id,
                    state: existing,
                    message: String::new(),
                }));
            }
            // Failed/Dropped/Unknown: in-place retry, no growth, no eviction.
            Some(_) => {}
            // New id: bound retained state, evicting one terminal entry at cap.
            None => {
                if t.len() >= state.max_segments {
                    match t.find_terminal() {
                        Some(done) => t.remove(&done),
                        None => return Err(SegmentCapacityExceeded(state.max_segments)),
                    }
                }
            }
        }
        t.stamp(&segment_id, SegmentLoadState::Loading)
    };
    tracing::info!(
        segment_id = %segment_id,
        data_source = %cmd.data_source,
        deep_store = %cmd.deep_storage_uri,
        "historical accepted load command",
    );

    let loading_delay = state.loading_to_loaded;
    let driver = Arc::clone(&state);
    let data_source = cmd.data_source.clone();
    tokio::spawn(async move {
        if !loading_delay.is_zero() {
            tokio::time::sleep(loading_delay).await;
        }
        if !driver.real_loader {
            // Wave 40.LL stub-loader fallback: flip to `Loaded` — but only if
            // this load still owns the entry (DD R36: a drop/supersede in the
            // delay window must not be overwritten).
            let mut t = driver.segments.lock().await;
            if t.current_gen(&segment_id) == Some(load_gen) {
                t.stamp(&segment_id, SegmentLoadState::Loaded);
            }
            return;
        }

        // Wave 41.OO real loader: build the JSON-Lines artifact, then commit it
        // into the store AND flip the state to `Loaded` together — taking the
        // store lock before the segments lock (the same order as
        // `insert_segment`, so no lock-order inversion) and ONLY if this load
        // still owns the generation. DD R36: this prevents a loader that
        // finishes after a drop/supersede from resurrecting the segment.
        let outcome = driver
            .build_segment_artifact(&data_source, &segment_id)
            .await;
        match outcome {
            Ok(segment) => {
                let mut store = driver.store.lock().await;
                let mut t = driver.segments.lock().await;
                if t.current_gen(&segment_id) == Some(load_gen) {
                    store.insert(segment_id.clone(), Arc::new(segment));
                    t.stamp(&segment_id, SegmentLoadState::Loaded);
                    tracing::info!(segment_id = %segment_id, "historical loaded segment artifact");
                } else {
                    tracing::info!(
                        segment_id = %segment_id,
                        "historical discarding stale load (segment dropped or superseded)",
                    );
                }
            }
            Err(e) => {
                let mut t = driver.segments.lock().await;
                if t.current_gen(&segment_id) == Some(load_gen) {
                    tracing::warn!(
                        segment_id = %segment_id,
                        error = %e,
                        "historical failed to load segment artifact",
                    );
                    t.stamp(&segment_id, SegmentLoadState::Failed);
                }
            }
        }
    });

    Ok(Json(initial))
}

/// 503 response when the historical has no capacity for a new segment
/// and no terminal (Failed/Dropped/Unknown) status entry can be evicted
/// (DD R35 admission backpressure).
struct SegmentCapacityExceeded(usize);

impl IntoResponse for SegmentCapacityExceeded {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "historical at capacity ({} active segments); retry later",
                self.0
            ),
        )
            .into_response()
    }
}

// DD R35: `drop` records the segment as the terminal `Dropped` state
// rather than removing the status entry outright. This keeps the
// `loadstatus` poll observable (the coordinator can confirm the drop
// applied) while still bounding retained state: `Dropped` is an
// evictable terminal status, so [`handle_load`] reclaims it under
// admission pressure. The live artifact in `store` is removed
// immediately below, so a `Dropped` status entry never backs memory
// beyond the small enum value itself.
async fn handle_drop(
    State(state): State<Arc<HistoricalServerState>>,
    Json(cmd): Json<SegmentDropCommand>,
) -> Json<LoadStatusReport> {
    {
        // DD R36: `stamp` bumps the segment's generation, so a loader still
        // in flight for this segment will see a generation mismatch at commit
        // and discard its (now stale) result instead of resurrecting it.
        let mut g = state.segments.lock().await;
        g.stamp(&cmd.segment_id, SegmentLoadState::Dropped);
    }
    {
        let mut store = state.store.lock().await;
        store.remove(&cmd.segment_id);
    }
    Json(LoadStatusReport {
        segment_id: cmd.segment_id,
        state: SegmentLoadState::Dropped,
        message: "drop applied (Wave 41.OO)".into(),
    })
}

async fn handle_loadstatus(
    State(state): State<Arc<HistoricalServerState>>,
) -> Json<HashMap<String, SegmentLoadState>> {
    Json(state.segments.lock().await.states.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::historical_client::{HistoricalClient, HttpHistoricalClient};
    use crate::native_query::{Aggregation, NativeQuery, ScanSpec, TimeseriesSpec};

    async fn spawn(state: Arc<HistoricalServerState>) -> String {
        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn scatter_query_endpoint_echoes_query_and_segment_id() {
        let state = Arc::new(HistoricalServerState::default());
        let url = spawn(Arc::clone(&state)).await;
        let client = HttpHistoricalClient::try_new(&url).expect("client");
        let resp = client
            .scatter_query(SegmentQuery::new("SELECT count(*)", "seg-xyz"))
            .await
            .expect("scatter");
        assert_eq!(resp.segment_id, "seg-xyz");
        assert_eq!(resp.rows.len(), 1);
        assert_eq!(
            resp.rows[0][0],
            serde_json::Value::String("SELECT count(*)".into())
        );
    }

    #[tokio::test]
    async fn load_rejects_at_capacity_and_dedups() {
        // DD R35: with a tiny cap and a long loading delay (segments
        // stay in `Loading`), a new distinct segment beyond capacity is
        // rejected (503), re-loading an existing id is idempotent (no
        // second loader), and a terminal (Dropped) entry is evicted to
        // admit a fresh segment. State never exceeds the cap.
        let state = Arc::new(
            HistoricalServerState::with_config(
                "hist-cap",
                "default",
                Duration::from_secs(60), // segments stay Loading
            )
            .with_max_segments(1),
        );
        let url = spawn(Arc::clone(&state)).await;
        let client = HttpHistoricalClient::try_new(&url).expect("client");

        // First segment: accepted, stays Loading.
        let r1 = client
            .load_segment(SegmentLoadCommand::new("seg-1", "ds", "deepstore://1"))
            .await
            .expect("first load ok");
        assert_eq!(r1.state, SegmentLoadState::Loading);
        assert_eq!(state.snapshot().await.len(), 1);

        // A second DISTINCT segment cannot be admitted (cap=1, seg-1 active).
        let rejected = client
            .load_segment(SegmentLoadCommand::new("seg-2", "ds", "deepstore://2"))
            .await;
        assert!(
            rejected.is_err(),
            "a new segment beyond capacity must be rejected (503)"
        );
        assert_eq!(state.snapshot().await.len(), 1);

        // Re-loading the SAME id is idempotent and returns Loading.
        let r1_again = client
            .load_segment(SegmentLoadCommand::new("seg-1", "ds", "deepstore://1"))
            .await
            .expect("idempotent re-load of an existing segment id must succeed");
        assert_eq!(r1_again.state, SegmentLoadState::Loading);
        assert_eq!(state.snapshot().await.len(), 1);

        // Drop seg-1 → terminal (Dropped) status; a fresh segment now
        // evicts it to make room.
        client
            .drop_segment(SegmentDropCommand::new("seg-1"))
            .await
            .expect("drop ok");
        let r3 = client
            .load_segment(SegmentLoadCommand::new("seg-3", "ds", "deepstore://3"))
            .await
            .expect("load after terminal eviction must succeed");
        assert_eq!(r3.state, SegmentLoadState::Loading);

        let table = state.snapshot().await;
        assert_eq!(table.len(), 1, "state never exceeds the cap");
        assert!(table.contains_key("seg-3"));
        assert!(!table.contains_key("seg-1"));
    }

    #[tokio::test]
    async fn load_then_status_observes_loaded_after_real_artifact_load() {
        // Build a deep-storage root with a real segment artifact.
        let dir = tempfile::tempdir().expect("tempdir");
        let seg_dir = dir.path().join("ds-test").join("seg-A");
        tokio::fs::create_dir_all(&seg_dir).await.expect("mkdir");
        let artifact = r#"{"segmentId":"seg-A","dataSource":"ds-test","columns":[{"name":"__time","type":"long"},{"name":"x","type":"long"}]}
{"__time":1,"x":1}
"#;
        tokio::fs::write(seg_dir.join("segment.jsonl"), artifact)
            .await
            .expect("write");

        let state = Arc::new(HistoricalServerState::with_root(
            "hist-test",
            "hot",
            Duration::from_millis(0),
            dir.path().to_path_buf(),
        ));
        let url = spawn(Arc::clone(&state)).await;
        let client = HttpHistoricalClient::try_new(&url).expect("client");

        let cmd = SegmentLoadCommand::new("seg-A", "ds-test", "deepstore://A/seg-A");
        let initial = client.load_segment(cmd).await.expect("load");
        assert_eq!(initial.state, SegmentLoadState::Loading);

        let mut last = SegmentLoadState::Loading;
        for _ in 0..50 {
            let table = client.load_status().await.expect("poll");
            if let Some(&s) = table.get("seg-A") {
                last = s;
                if last == SegmentLoadState::Loaded {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(last, SegmentLoadState::Loaded);
        assert_eq!(state.loaded_segment_ids().await, vec!["seg-A".to_string()]);
    }

    #[tokio::test]
    async fn drop_during_load_does_not_resurrect_segment() {
        // DD R36: a drop that arrives while a loader is in flight must win — the
        // late loader must NOT resurrect the dropped segment into Loaded / the
        // store. The generation stamped by `drop` makes the loader's commit
        // detect it was superseded and discard its result.
        let dir = tempfile::tempdir().expect("tempdir");
        let seg_dir = dir.path().join("ds-test").join("seg-A");
        tokio::fs::create_dir_all(&seg_dir).await.expect("mkdir");
        let artifact = r#"{"segmentId":"seg-A","dataSource":"ds-test","columns":[{"name":"__time","type":"long"}]}
{"__time":1}
"#;
        tokio::fs::write(seg_dir.join("segment.jsonl"), artifact)
            .await
            .expect("write");

        // Long loading delay so the loader is still in flight when we drop.
        let state = Arc::new(HistoricalServerState::with_root(
            "hist-test",
            "hot",
            Duration::from_millis(300),
            dir.path().to_path_buf(),
        ));
        let url = spawn(Arc::clone(&state)).await;
        let client = HttpHistoricalClient::try_new(&url).expect("client");

        let cmd = SegmentLoadCommand::new("seg-A", "ds-test", "deepstore://A/seg-A");
        assert_eq!(
            client.load_segment(cmd).await.expect("load").state,
            SegmentLoadState::Loading
        );

        // Drop while the loader is still sleeping (300 ms delay).
        tokio::time::sleep(Duration::from_millis(50)).await;
        let dropped = client
            .drop_segment(SegmentDropCommand::new("seg-A"))
            .await
            .expect("drop");
        assert_eq!(dropped.state, SegmentLoadState::Dropped);

        // Wait well past the loader's delay so the late loader has completed.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let table = client.load_status().await.expect("status");
        assert_eq!(
            table.get("seg-A").copied(),
            Some(SegmentLoadState::Dropped),
            "a dropped segment must not be resurrected to Loaded by a late loader",
        );
        assert!(
            state.loaded_segment_ids().await.is_empty(),
            "the store must not contain a resurrected dropped segment",
        );
    }

    #[tokio::test]
    async fn load_failure_reports_failed_state() {
        // No artifact on disk → loader should record `Failed`.
        let dir = tempfile::tempdir().expect("tempdir");
        let state = Arc::new(HistoricalServerState::with_root(
            "hist-test",
            "default",
            Duration::from_millis(0),
            dir.path().to_path_buf(),
        ));
        let url = spawn(Arc::clone(&state)).await;
        let client = HttpHistoricalClient::try_new(&url).expect("client");

        client
            .load_segment(SegmentLoadCommand::new(
                "seg-missing",
                "ds-x",
                "deepstore://x",
            ))
            .await
            .expect("load");

        let mut last = SegmentLoadState::Loading;
        for _ in 0..50 {
            let table = client.load_status().await.expect("poll");
            if let Some(&s) = table.get("seg-missing") {
                last = s;
                if last == SegmentLoadState::Failed {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(last, SegmentLoadState::Failed);
        assert!(state.loaded_segment_ids().await.is_empty());
    }

    #[tokio::test]
    async fn drop_segment_marks_dropped_and_clears_store() {
        // Pre-seed a segment.
        let state = Arc::new(HistoricalServerState::default());
        let seg = Segment::parse_jsonl(
            "{\"segmentId\":\"seg-D\",\"dataSource\":\"ds\",\"columns\":[{\"name\":\"__time\",\"type\":\"long\"}]}\n{\"__time\":1}\n",
        )
        .expect("parse");
        state.insert_segment("seg-D", seg).await;

        let url = spawn(Arc::clone(&state)).await;
        let client = HttpHistoricalClient::try_new(&url).expect("client");

        let drop_report = client
            .drop_segment(SegmentDropCommand::new("seg-D"))
            .await
            .expect("drop");
        assert_eq!(drop_report.state, SegmentLoadState::Dropped);

        let table = client.load_status().await.expect("status");
        assert_eq!(table.get("seg-D"), Some(&SegmentLoadState::Dropped));
        assert!(state.loaded_segment_ids().await.is_empty());
    }

    #[tokio::test]
    async fn native_timeseries_query_executes_against_loaded_segment() {
        let state = Arc::new(HistoricalServerState::default());
        let seg = Segment::parse_jsonl(r#"{"segmentId":"seg-Q","dataSource":"wiki","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"},{"name":"count","type":"long"}]}
{"__time":1714694400000,"page":"home","count":3}
{"__time":1714694460000,"page":"home","count":2}
{"__time":1714694520000,"page":"about","count":1}
"#)
        .expect("parse");
        state.insert_segment("seg-Q", seg).await;

        let url = spawn(Arc::clone(&state)).await;
        // The reqwest call below builds its own client; the trait
        // helper isn't used in this test (we hit the raw axum route
        // via a hand-built body so we can inject `segmentId`).
        let _client = HttpHistoricalClient::try_new(&url).expect("client");

        let query = NativeQuery::Timeseries(TimeseriesSpec {
            data_source: "wiki".into(),
            granularity_ms: 0,
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            filter: None,
        });
        // Compose body manually so the wire body carries `queryType`
        // (= the dispatch trigger) plus a top-level `segmentId`.
        let mut body = serde_json::to_value(&query).expect("ser");
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "segmentId".into(),
                serde_json::Value::String("seg-Q".into()),
            );
        }

        let http = reqwest::Client::new();
        let resp = http
            .post(format!("{url}/druid/v2/native"))
            .json(&body)
            .send()
            .await
            .expect("send")
            .json::<SegmentQueryResponse>()
            .await
            .expect("decode");
        assert_eq!(resp.segment_id, "seg-Q");
        assert_eq!(resp.rows.len(), 1, "single bucket for granularity=0");
        // Row shape: [timestamp_ms, result_object].
        let bucket = &resp.rows[0];
        assert_eq!(bucket.len(), 2);
        let result_obj = bucket[1].as_object().expect("obj");
        assert_eq!(result_obj.get("total"), Some(&serde_json::json!(6)));
    }

    #[tokio::test]
    async fn native_scan_query_returns_filtered_rows() {
        let state = Arc::new(HistoricalServerState::default());
        let seg = Segment::parse_jsonl(r#"{"segmentId":"seg-S","dataSource":"wiki","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"}]}
{"__time":1,"page":"home"}
{"__time":2,"page":"about"}
{"__time":3,"page":"home"}
"#)
        .expect("parse");
        state.insert_segment("seg-S", seg).await;

        let url = spawn(Arc::clone(&state)).await;
        let query = NativeQuery::Scan(ScanSpec {
            data_source: "wiki".into(),
            columns: Some(vec!["page".into()]),
            limit: None,
            filter: Some(crate::native_query::EqualsFilter {
                dimension: "page".into(),
                value: "home".into(),
            }),
        });
        let mut body = serde_json::to_value(&query).expect("ser");
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "segmentId".into(),
                serde_json::Value::String("seg-S".into()),
            );
        }
        let http = reqwest::Client::new();
        let resp: SegmentQueryResponse = http
            .post(format!("{url}/druid/v2/native"))
            .json(&body)
            .send()
            .await
            .expect("send")
            .json()
            .await
            .expect("decode");
        assert_eq!(resp.rows.len(), 2);
        for row in &resp.rows {
            let obj = row[0].as_object().expect("obj");
            assert_eq!(obj.get("page"), Some(&serde_json::json!("home")));
        }
    }

    #[tokio::test]
    async fn native_query_against_unloaded_segment_returns_empty_rows() {
        let state = Arc::new(HistoricalServerState::default());
        let url = spawn(Arc::clone(&state)).await;
        let query = NativeQuery::Scan(ScanSpec {
            data_source: "wiki".into(),
            columns: None,
            limit: None,
            filter: None,
        });
        let mut body = serde_json::to_value(&query).expect("ser");
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "segmentId".into(),
                serde_json::Value::String("seg-not-here".into()),
            );
        }
        let http = reqwest::Client::new();
        let resp: SegmentQueryResponse = http
            .post(format!("{url}/druid/v2/native"))
            .json(&body)
            .send()
            .await
            .expect("send")
            .json()
            .await
            .expect("decode");
        assert_eq!(resp.segment_id, "seg-not-here");
        assert!(resp.rows.is_empty());
    }

    // ===================================================================
    // Wave 42.RR — remote deep-storage loader (S3-compatible / InMemory)
    // ===================================================================

    #[tokio::test]
    async fn remote_deep_storage_loader_downloads_then_parses_artifact() {
        use ferrodruid_deep_storage::InMemoryDeepStorage;

        // Stage a segment.jsonl artifact in the InMemory backend.
        let remote = Arc::new(InMemoryDeepStorage::new());
        let staging = tempfile::tempdir().expect("staging");
        let stage_dir = staging.path().join("seg-stage");
        tokio::fs::create_dir_all(&stage_dir).await.expect("mkdir");
        let artifact = r#"{"segmentId":"seg-R","dataSource":"wikipedia","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"}]}
{"__time":1,"page":"home"}
{"__time":2,"page":"about"}
"#;
        tokio::fs::write(stage_dir.join("segment.jsonl"), artifact)
            .await
            .expect("write");
        DeepStorage::upload_segment(remote.as_ref(), "wikipedia", "seg-R", &stage_dir)
            .await
            .expect("upload");

        // Stand up a historical with the remote backend + a fresh
        // local cache root.
        let cache = tempfile::tempdir().expect("cache");
        let state = Arc::new(HistoricalServerState::with_remote(
            "hist-remote",
            "default",
            Duration::from_millis(0),
            cache.path().to_path_buf(),
            remote as Arc<dyn DeepStorage>,
        ));
        let url = spawn(Arc::clone(&state)).await;
        let client = HttpHistoricalClient::try_new(&url).expect("client");

        // Drive through the load handler.
        let cmd = SegmentLoadCommand::new("seg-R", "wikipedia", "s3://demo/wikipedia/seg-R");
        client.load_segment(cmd).await.expect("load");
        let mut last = SegmentLoadState::Loading;
        for _ in 0..100 {
            let table = client.load_status().await.expect("poll");
            if let Some(&s) = table.get("seg-R") {
                last = s;
                if last == SegmentLoadState::Loaded {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            last,
            SegmentLoadState::Loaded,
            "remote loader should reach Loaded state",
        );
        assert_eq!(state.loaded_segment_ids().await, vec!["seg-R".to_string()]);

        // The cache directory now holds the materialised artifact.
        let cached_path = cache
            .path()
            .join("wikipedia")
            .join("seg-R")
            .join("segment.jsonl");
        assert!(
            cached_path.exists(),
            "cache materialised at {cached_path:?}"
        );
    }

    #[tokio::test]
    async fn remote_deep_storage_loader_failure_on_missing_object_reports_failed() {
        use ferrodruid_deep_storage::InMemoryDeepStorage;

        let remote = Arc::new(InMemoryDeepStorage::new());
        let cache = tempfile::tempdir().expect("cache");
        let state = Arc::new(HistoricalServerState::with_remote(
            "hist-remote",
            "default",
            Duration::from_millis(0),
            cache.path().to_path_buf(),
            remote as Arc<dyn DeepStorage>,
        ));
        let url = spawn(Arc::clone(&state)).await;
        let client = HttpHistoricalClient::try_new(&url).expect("client");

        client
            .load_segment(SegmentLoadCommand::new("nope", "wikipedia", "s3://demo/x"))
            .await
            .expect("load");

        let mut last = SegmentLoadState::Loading;
        for _ in 0..50 {
            let table = client.load_status().await.expect("poll");
            if let Some(&s) = table.get("nope") {
                last = s;
                if last == SegmentLoadState::Failed {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(last, SegmentLoadState::Failed);
        assert!(state.loaded_segment_ids().await.is_empty());
    }

    // ===================================================================
    // Wave 42.RR — groupBy + topN end-to-end through the historical
    // ===================================================================

    #[tokio::test]
    async fn native_group_by_query_executes_against_loaded_segment() {
        use crate::native_query::{Aggregation, GroupBySpec, NativeQuery, SortDirection, SortSpec};

        let state = Arc::new(HistoricalServerState::default());
        let seg = Segment::parse_jsonl(r#"{"segmentId":"seg-G","dataSource":"wiki","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"},{"name":"count","type":"long"}]}
{"__time":1,"page":"home","count":3}
{"__time":2,"page":"home","count":2}
{"__time":3,"page":"about","count":1}
"#)
        .expect("parse");
        state.insert_segment("seg-G", seg).await;

        let url = spawn(Arc::clone(&state)).await;

        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wiki".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            filter: None,
            having: None,
            sort: Some(vec![SortSpec {
                dimension: "total".into(),
                direction: SortDirection::Descending,
            }]),
            limit: None,
        });
        let mut body = serde_json::to_value(&q).expect("ser");
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "segmentId".into(),
                serde_json::Value::String("seg-G".into()),
            );
        }
        let http = reqwest::Client::new();
        let resp: SegmentQueryResponse = http
            .post(format!("{url}/druid/v2/native"))
            .json(&body)
            .send()
            .await
            .expect("send")
            .json()
            .await
            .expect("decode");
        assert_eq!(resp.rows.len(), 2);
        // Each row is `[Object{page,total}]` — descending by total.
        let first = resp.rows[0][0].as_object().expect("first obj");
        assert_eq!(first.get("page"), Some(&serde_json::json!("home")));
        assert_eq!(first.get("total"), Some(&serde_json::json!(5)));
        let second = resp.rows[1][0].as_object().expect("second obj");
        assert_eq!(second.get("page"), Some(&serde_json::json!("about")));
        assert_eq!(second.get("total"), Some(&serde_json::json!(1)));
    }

    #[tokio::test]
    async fn native_top_n_query_executes_against_loaded_segment() {
        use crate::native_query::{Aggregation, NativeQuery, TopNSpec};

        let state = Arc::new(HistoricalServerState::default());
        let seg = Segment::parse_jsonl(r#"{"segmentId":"seg-T","dataSource":"wiki","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"},{"name":"count","type":"long"}]}
{"__time":1,"page":"home","count":3}
{"__time":2,"page":"home","count":2}
{"__time":3,"page":"about","count":1}
{"__time":4,"page":"kb","count":7}
"#)
        .expect("parse");
        state.insert_segment("seg-T", seg).await;

        let url = spawn(Arc::clone(&state)).await;
        let q = NativeQuery::TopN(TopNSpec {
            data_source: "wiki".into(),
            dimension: "page".into(),
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            metric: "total".into(),
            threshold: 2,
            filter: None,
        });
        let mut body = serde_json::to_value(&q).expect("ser");
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "segmentId".into(),
                serde_json::Value::String("seg-T".into()),
            );
        }
        let http = reqwest::Client::new();
        let resp: SegmentQueryResponse = http
            .post(format!("{url}/druid/v2/native"))
            .json(&body)
            .send()
            .await
            .expect("send")
            .json()
            .await
            .expect("decode");
        assert_eq!(resp.rows.len(), 2, "threshold=2 caps the wire");
        let first = resp.rows[0][0].as_object().expect("first obj");
        assert_eq!(first.get("page"), Some(&serde_json::json!("kb")));
        assert_eq!(first.get("total"), Some(&serde_json::json!(7)));
        let second = resp.rows[1][0].as_object().expect("second obj");
        assert_eq!(second.get("page"), Some(&serde_json::json!("home")));
        assert_eq!(second.get("total"), Some(&serde_json::json!(5)));
    }
}
