// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Query routing, scatter/gather merge for FerroDruid.
//!
//! The [`Broker`] receives queries from clients, fans them out to Historical
//! nodes, and merges the partial results into a single unified response.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use ferrodruid_aggregator::{AggregatorSpec, PostAggregatorSpec, merge_json_by_spec};
use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_common::types::DimensionSpec;
use ferrodruid_historical::Historical;
use ferrodruid_query::{
    DruidQuery, GroupByResult, QueryResult, ScanResult, SearchHit, SearchResult, TimeseriesResult,
    TopNMetricSpec, TopNQuery, TopNResult, numeric_agg_cell,
};

// ---------------------------------------------------------------------------
// HistoricalEndpoint
// ---------------------------------------------------------------------------

/// Network endpoint for a Historical node known to the Broker.
#[derive(Debug, Clone)]
pub struct HistoricalEndpoint {
    /// Unique name for this Historical.
    pub name: String,
    /// Hostname or IP address.
    pub host: String,
    /// Plain-text port.
    pub port: u16,
    /// Optional TLS port.
    pub tls_port: Option<u16>,
}

// ---------------------------------------------------------------------------
// BrokerQueryResult
// ---------------------------------------------------------------------------

/// The merged result of a Broker query.
#[derive(Debug, Clone)]
pub struct BrokerQueryResult {
    /// Unique query identifier.
    pub query_id: String,
    /// The merged query result.
    pub result: QueryResult,
}

// ---------------------------------------------------------------------------
// Broker
// ---------------------------------------------------------------------------

/// A Broker that routes queries to Historical nodes and merges results.
///
/// In distributed mode the Broker would send queries over the network.
/// The `execute_local` method supports single-binary mode where Historical
/// instances live in-process.
pub struct Broker {
    /// Known Historical endpoints.
    historicals: Arc<RwLock<Vec<HistoricalEndpoint>>>,
    /// Segment-to-Historical mapping (segment_id -> historical_name).
    segment_map: Arc<RwLock<HashMap<String, String>>>,
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

impl Broker {
    /// Create a new Broker with no registered Historicals.
    pub fn new() -> Self {
        Self {
            historicals: Arc::new(RwLock::new(Vec::new())),
            segment_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a Historical endpoint.
    pub fn register_historical(&self, endpoint: HistoricalEndpoint) {
        if let Ok(mut hist) = self.historicals.write() {
            // Remove any existing entry with the same name.
            hist.retain(|h| h.name != endpoint.name);
            hist.push(endpoint);
        }
    }

    /// Remove a Historical endpoint by name.
    pub fn deregister_historical(&self, name: &str) {
        if let Ok(mut hist) = self.historicals.write() {
            hist.retain(|h| h.name != name);
        }
    }

    /// Update the segment-to-Historical mapping (typically provided by the Coordinator).
    pub fn update_segment_map(&self, map: HashMap<String, String>) {
        if let Ok(mut sm) = self.segment_map.write() {
            *sm = map;
        }
    }

    /// Execute a query locally in single-binary mode.
    ///
    /// Each Historical executes the query against its loaded segments, and the
    /// Broker merges all partial results into a single `BrokerQueryResult`.
    pub fn execute_local(
        &self,
        query: &DruidQuery,
        historicals: &[&Historical],
    ) -> Result<BrokerQueryResult> {
        if let DruidQuery::UnionAll(branches) = query {
            for branch in branches {
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
            }

            let mut branch_partials: Vec<Vec<QueryResult>> =
                (0..branches.len()).map(|_| Vec::new()).collect();
            for hist in historicals {
                let results = hist.execute_query(query)?;
                if results.len() != branches.len() {
                    return Err(DruidError::Internal(format!(
                        "Historical returned {} UNION ALL branch result(s), expected {}",
                        results.len(),
                        branches.len()
                    )));
                }
                for (partials, result) in branch_partials.iter_mut().zip(results) {
                    partials.push(result);
                }
            }

            let mut branch_results = Vec::with_capacity(branches.len());
            for (branch, partials) in branches.iter().zip(branch_partials) {
                let result = Self::merge_results(branch, partials)?;
                if !matches!(result, QueryResult::Scan(_)) {
                    return Err(DruidError::Query(
                        "UNION ALL branch did not produce a scan result".to_owned(),
                    ));
                }
                branch_results.push(result);
            }
            // Druid names the union output from the FIRST branch and maps every
            // later branch's columns into it by position. Align each branch's
            // scan columns to the first branch's target columns before the
            // final concatenation, so differently-named branches are combined
            // positionally rather than dropped.
            //
            // The target is the first branch's PROJECTED scan columns (from the
            // query), which is robust to a first branch whose datasource has no
            // loaded segments and therefore produced an empty result with no
            // column metadata. Fall back to the first non-empty branch result
            // (e.g. `SELECT *` branches carry no explicit column list).
            let target_columns: Vec<String> = branches
                .first()
                .and_then(|b| match b {
                    DruidQuery::Scan(scan) => scan.columns.clone(),
                    _ => None,
                })
                .filter(|cols| !cols.is_empty())
                .or_else(|| {
                    branch_results.iter().find_map(|r| match r {
                        QueryResult::Scan(scan) if !scan.columns.is_empty() => {
                            Some(scan.columns.clone())
                        }
                        _ => None,
                    })
                })
                .unwrap_or_default();
            for result in &mut branch_results {
                if let QueryResult::Scan(scan) = result {
                    // An empty branch (datasource with no loaded segments)
                    // carries no rows and no column metadata; it contributes
                    // nothing to the union, so skip alignment rather than trip
                    // the arity check on its empty column list.
                    if scan.events.is_empty() {
                        continue;
                    }
                    ferrodruid_query::align_union_branch(&target_columns, scan)?;
                }
            }
            let merged = Self::merge_results(query, branch_results)?;
            return Ok(BrokerQueryResult {
                query_id: uuid::Uuid::new_v4().to_string(),
                result: merged,
            });
        }

        // W-C S3: rewrite the per-segment fan-out query where per-segment
        // execution semantics would corrupt the broker-side re-ordering
        // (see `fan_out_query`); the ORIGINAL query still drives the
        // merge so the client-visible limit/projection are honored.
        let (fan_out, strip_time) = fan_out_query(query);

        let mut partials = Vec::new();

        for hist in historicals {
            let results = hist.execute_query(fan_out.as_ref())?;
            partials.extend(results);
        }

        let mut merged = Self::merge_results(query, partials)?;

        // Strip the broker-injected `__time` sort column back out of the
        // merged result (the client did not project it).
        if strip_time && let QueryResult::Scan(scan) = &mut merged {
            scan.columns.retain(|c| c != "__time");
            for event in &mut scan.events {
                event.remove("__time");
            }
        }

        Ok(BrokerQueryResult {
            query_id: uuid::Uuid::new_v4().to_string(),
            result: merged,
        })
    }

    /// Merge partial `QueryResult`s from multiple shards into a single result.
    ///
    /// The merge strategy depends on the query type:
    /// - **Timeseries**: merge by timestamp bucket, re-aggregate values.
    /// - **TopN**: merge by dimension value per time bucket, re-sort, take top-N.
    /// - **GroupBy**: merge by group key, re-aggregate.
    /// - **Scan**: concatenate rows (respecting any limit).
    /// - **Search**: merge, deduplicate, re-sort.
    /// - **Metadata**: concatenate.
    pub fn merge_results(query: &DruidQuery, partials: Vec<QueryResult>) -> Result<QueryResult> {
        if partials.is_empty() {
            return empty_result(query);
        }

        let multi_partials = partials.len() > 1;
        let merged = if partials.len() == 1 {
            partials.into_iter().next().expect("checked len == 1")
        } else {
            match query {
                DruidQuery::Timeseries(q) => merge_timeseries(partials, &q.aggregations),
                DruidQuery::TopN(q) => merge_topn(partials, q),
                DruidQuery::GroupBy(q) => merge_groupby(partials, &q.dimensions, &q.aggregations),
                DruidQuery::Scan(q) => merge_scan(partials, q.limit, q.order.as_deref()),
                DruidQuery::Search(_) => merge_search(partials),
                DruidQuery::SegmentMetadata(_) => merge_segment_metadata(partials),
                DruidQuery::DataSourceMetadata(_) => merge_datasource_metadata(partials),
                DruidQuery::TimeBoundary(_) => merge_time_boundary(partials),
                DruidQuery::UnionAll(_) => merge_scan(partials, None, None),
                DruidQuery::Window(_) => merge_scan(partials, None, None),
            }?
        };

        // Fail-closed exact-cardinality program (2026-07-11): reject any
        // merged exact-cardinality output whose value degraded to an
        // inexact upper bound, and collapse broker-internal
        // `CardinalityState` envelopes to bare counts so they never leak
        // onto the client wire.  When partials were actually merged (or a
        // cardinality envelope collapsed), post-aggregations are also
        // recomputed from the merged aggregator values (2026-07-12).
        let merged = finalize_merged_outputs(query, merged, multi_partials)?;

        // W-C S3: re-apply the groupBy query's ordering + limit AFTER
        // finalization (so ordering on a collapsed exact-cardinality
        // count or a recomputed post-aggregation ranks on the FINAL
        // value).  `merge_groupby` merges group VALUES exactly but emits
        // groups in shard-arrival order, so `ORDER BY … LIMIT k`
        // selected the wrong k groups pre-fix.  The sequence mirrors
        // per-segment execution exactly (timestamp sort, then the SAME
        // shared limitSpec comparator, then truncate), which makes this
        // pass idempotent for single-partial results that already
        // applied it — and it is the ONLY place the limit is applied for
        // queries fanned out by `execute_local`, which strips the
        // per-segment limit push-down (see `fan_out_query`).
        Ok(apply_groupby_ordering(query, merged))
    }

    /// Get the list of known Historical endpoint names.
    pub fn known_historicals(&self) -> Vec<String> {
        match self.historicals.read() {
            Ok(hist) => hist.iter().map(|h| h.name.clone()).collect(),
            Err(_) => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Merge implementations
// ---------------------------------------------------------------------------

/// W-C S3: build the per-segment fan-out form of a query for
/// `execute_local`, returning `(fan-out query, strip __time after merge)`.
///
/// Two rewrites, both required for the broker's post-merge re-ordering
/// to be CORRECT (not just deterministic):
///
/// * **Time-ordered scan without a `__time` projection**: the broker
///   re-sorts merged scan events by their `__time` cell, so when the
///   client's explicit column list excludes `__time` it is appended for
///   the fan-out and stripped from the merged result (flag = `true`).
///   Druid's broker likewise merges per-segment streams on the row
///   timestamp regardless of projection.
/// * **GroupBy limitSpec limit is NOT pushed down**: per-segment
///   truncation keeps each segment's LOCAL top-k, but a group can be
///   below top-k on every segment and still be the GLOBAL top-k once
///   its partial aggregates merge (and a boundary group's merged value
///   would silently miss the truncated segments' contribution).  Druid
///   applies the limitSpec at the broker (per-segment push-down only
///   under the opt-in `forceLimitPushDown`), so the fan-out strips the
///   `limit` — the ordering columns stay, they are semantics-free for
///   correctness — and `merge_results` applies the full limitSpec after
///   the merge.  Per-partial size stays bounded by the executor's
///   group-cardinality cap exactly as in Druid's default mode.
///
/// Everything else fans out unchanged (`Cow::Borrowed`).
fn fan_out_query(query: &DruidQuery) -> (std::borrow::Cow<'_, DruidQuery>, bool) {
    match query {
        DruidQuery::Scan(q)
            if matches!(q.order.as_deref(), Some("ascending" | "descending"))
                && q.columns.as_ref().is_some_and(|cols| {
                    !cols.is_empty() && !cols.iter().any(|c| c == "__time")
                }) =>
        {
            let mut rewritten = q.clone();
            if let Some(cols) = &mut rewritten.columns {
                cols.push("__time".to_string());
            }
            (std::borrow::Cow::Owned(DruidQuery::Scan(rewritten)), true)
        }
        DruidQuery::GroupBy(q) if q.limit_spec.as_ref().is_some_and(|ls| ls.limit.is_some()) => {
            let mut rewritten = q.clone();
            if let Some(ls) = &mut rewritten.limit_spec {
                ls.limit = None;
            }
            (
                std::borrow::Cow::Owned(DruidQuery::GroupBy(rewritten)),
                false,
            )
        }
        _ => (std::borrow::Cow::Borrowed(query), false),
    }
}

/// Produce an empty result matching the expected query type.
fn empty_result(query: &DruidQuery) -> Result<QueryResult> {
    Ok(match query {
        DruidQuery::Timeseries(_) => QueryResult::Timeseries(Vec::new()),
        DruidQuery::TopN(_) => QueryResult::TopN(Vec::new()),
        DruidQuery::GroupBy(_) => QueryResult::GroupBy(Vec::new()),
        DruidQuery::Scan(_) => QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: Vec::new(),
            events: Vec::new(),
        }),
        DruidQuery::Search(_) => QueryResult::Search(Vec::new()),
        DruidQuery::SegmentMetadata(_) => QueryResult::SegmentMetadata(Vec::new()),
        DruidQuery::DataSourceMetadata(_) => QueryResult::DataSourceMetadata(Vec::new()),
        DruidQuery::TimeBoundary(_) => QueryResult::TimeBoundary(Vec::new()),
        DruidQuery::UnionAll(_) => QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: Vec::new(),
            events: Vec::new(),
        }),
        DruidQuery::Window(_) => QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: Vec::new(),
            events: Vec::new(),
        }),
    })
}

/// Merge timeseries results by timestamp bucket, dispatching by aggregator
/// kind so that min/max/first/last/cardinality results are not corrupted by
/// the previous "sum every numeric field" behavior (Wave 37B High
/// `broker/lib.rs:462-483`).
fn merge_timeseries(
    partials: Vec<QueryResult>,
    aggregations: &[AggregatorSpec],
) -> Result<QueryResult> {
    let mut bucket_map: HashMap<String, serde_json::Map<String, serde_json::Value>> =
        HashMap::new();
    let mut ts_order: Vec<String> = Vec::new();

    for partial in partials {
        if let QueryResult::Timeseries(entries) = partial {
            for entry in entries {
                let ts = entry.timestamp.clone();
                let existing = bucket_map.entry(ts.clone()).or_insert_with(|| {
                    ts_order.push(ts);
                    serde_json::Map::new()
                });
                merge_agg_maps(existing, &entry.result, aggregations);
            }
        }
    }

    let results: Vec<TimeseriesResult> = ts_order
        .into_iter()
        .map(|ts| {
            let result = bucket_map.remove(&ts).unwrap_or_default();
            TimeseriesResult {
                timestamp: ts,
                result,
            }
        })
        .collect();

    Ok(QueryResult::Timeseries(results))
}

/// Merge TopN results: combine entries per time bucket, re-sort, take top-N.
///
/// Wave 36-F (Wave 37 R1 medium `lib.rs:230-291`): the previous heuristic
/// merged on "first string field" and ranked by "first numeric field",
/// which silently diverged from the requested dimension and metric. The
/// implementation now uses [`TopNQuery::dimension`] for grouping and
/// [`TopNQuery::metric`] for sorting, matching what the per-shard Historical
/// already does in `TopNQuery::execute`.
fn merge_topn(partials: Vec<QueryResult>, query: &TopNQuery) -> Result<QueryResult> {
    merge_topn_capped(partials, query, BROKER_TOPN_MERGE_MAX_INFLIGHT)
}

/// Broker-wide cap on the DISTINCT `(time bucket, dimension value)`
/// keys [`merge_topn`] may hold in flight — the topN twin of
/// [`BROKER_GROUPBY_MERGE_MAX_KEYS`]: per-segment topN execution caps
/// its in-flight keys at
/// [`ferrodruid_query::topn::DEFAULT_TOPN_MAX_INFLIGHT`] (the
/// `topN.maxIntermediateRows` guard), but the broker merge unions the
/// per-segment survivors across shards, so a many-segment
/// high-cardinality topN could grow the merge maps to
/// `shards × 100K` keys.  Shares the per-segment constant; fail-loud,
/// never silent truncation.
const BROKER_TOPN_MERGE_MAX_INFLIGHT: usize = ferrodruid_query::topn::DEFAULT_TOPN_MAX_INFLIGHT;

/// [`merge_topn`] with an explicit in-flight distinct-key cap (`0`
/// disables), split out so tests can inject a small cap.  The counter
/// spans ALL time buckets (mirroring the per-segment `inflight`
/// counter in `ferrodruid_query::topn`) and the guard fires DURING
/// merge-map accumulation with `observed = cap + 1`, exactly like the
/// per-segment `topN.maxIntermediateRows` guard.
fn merge_topn_capped(
    partials: Vec<QueryResult>,
    query: &TopNQuery,
    max_inflight: usize,
) -> Result<QueryResult> {
    // Collect all TopN results grouped by timestamp.
    let mut bucket_map: HashMap<String, Vec<serde_json::Map<String, serde_json::Value>>> =
        HashMap::new();
    let mut ts_order: Vec<String> = Vec::new();

    for partial in partials {
        if let QueryResult::TopN(entries) = partial {
            for entry in entries {
                let ts = entry.timestamp.clone();
                if !bucket_map.contains_key(&ts) {
                    ts_order.push(ts.clone());
                }
                let bucket = bucket_map.entry(ts).or_default();
                bucket.extend(entry.result);
            }
        }
    }

    let dim_out = dimension_output_name(&query.dimension);
    let threshold = query.threshold;

    // Fail-closed (2026-07-11): exact-cardinality outputs must be
    // finalized BEFORE ranking. A merged value that degraded to a
    // (non-numeric) saturated `CardinalityState` envelope sorts as 0.0
    // and could be silently truncated below the threshold, evading the
    // post-merge finalization pass in `merge_results`; a non-saturated
    // union envelope must likewise be collapsed to its exact count so
    // the metric ranks on the real number.
    let cardinality_outputs: Vec<&str> = query
        .aggregations
        .iter()
        .filter(|spec| spec_is_exact_cardinality(spec))
        .map(ferrodruid_aggregator::AggregatorSpec::name)
        .collect();

    // For each bucket, deduplicate by dimension key and take top-N.
    //
    // Broker-wide merge cardinality guard (W-C S3 follow-up High, see
    // [`BROKER_TOPN_MERGE_MAX_INFLIGHT`]): `inflight` counts DISTINCT
    // `(bucket, dim)` keys across ALL buckets, mirroring the
    // per-segment counter, and the guard fires on insertion so the
    // merge maps never grow past the cap.
    let mut inflight: usize = 0;
    let mut results: Vec<TopNResult> = Vec::with_capacity(ts_order.len());
    for ts in ts_order {
        let entries = bucket_map.remove(&ts).unwrap_or_default();

        // Merge entries with the same dimension key (declared output name).
        // Falls back to the previous "first string field" heuristic only
        // when the declared output column is absent or non-string, to
        // preserve compatibility with shards that emit unusual schemas.
        let mut merged: HashMap<String, serde_json::Map<String, serde_json::Value>> =
            HashMap::new();
        let mut key_order: Vec<String> = Vec::new();
        for entry in entries {
            let dim_key = entry
                .get(dim_out)
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    entry
                        .iter()
                        .find(|(_, v)| v.is_string())
                        .map(|(_, v)| v.as_str().unwrap_or("").to_string())
                        .unwrap_or_default()
                });

            if !merged.contains_key(&dim_key) {
                if max_inflight > 0 && inflight >= max_inflight {
                    return Err(DruidError::ResourceLimit {
                        kind: "topN.maxMergedIntermediateRows",
                        limit: max_inflight,
                        observed: inflight + 1,
                    });
                }
                inflight += 1;
                key_order.push(dim_key.clone());
            }
            let existing = merged.entry(dim_key).or_default();
            merge_agg_maps(existing, &entry, &query.aggregations);
        }

        let mut entries: Vec<serde_json::Map<String, serde_json::Value>> = key_order
            .into_iter()
            .filter_map(|k| merged.remove(&k))
            .collect();

        // Fail closed / collapse exact-cardinality envelopes before the
        // sort-and-truncate (see the comment above the loop), then
        // recompute post-aggregations from the merged aggregator values —
        // the metric may BE a post-aggregation (over an exact-cardinality
        // output, an HLL sketch, a merged sum, ...), and it must rank on
        // the recomputed value, not the first shard's stale per-segment
        // one.  `merge_topn` only runs with two or more partials, so the
        // recompute is never a spurious behavior change for single-segment
        // results (and is idempotent for buckets seen by one shard).
        for entry in &mut entries {
            finalize_cardinality_map(entry, &cardinality_outputs)?;
            reapply_post_aggs(query.post_aggregations.as_ref(), entry);
        }

        // Sort by the query-declared metric, falling back to the dimension
        // output name as a deterministic tiebreaker (mirrors per-shard
        // execution in `TopNQuery::execute`).
        sort_topn_merged(&mut entries, dim_out, &query.metric);
        entries.truncate(threshold);

        results.push(TopNResult {
            timestamp: ts,
            result: entries,
        });
    }

    Ok(QueryResult::TopN(results))
}

/// Recursive sort matching `TopNMetricSpec` semantics for merge.
fn sort_topn_merged(
    entries: &mut [serde_json::Map<String, serde_json::Value>],
    dim_out: &str,
    metric: &TopNMetricSpec,
) {
    match metric {
        TopNMetricSpec::Numeric { metric: name } => {
            entries.sort_by(|a, b| {
                // Resolve each merged metric cell through the ONE shared
                // numeric resolver (`ferrodruid_query::numeric_agg_cell`):
                // a sketch aggregation (`thetaSketch` / `hyperUnique`)
                // is carried across the broker fold as its partial-state
                // ENVELOPE `{"@sketch": …, "estimate": N, …}` so the
                // shards' sketches union exactly, and its ranking value
                // is the envelope's `estimate` — exactly how the
                // per-shard topN already ranks it.  Pre-fix, `as_f64()`
                // ranked every envelope as 0.0 and the dimension
                // tiebreaker silently returned the wrong groups on the
                // multi-segment path (the v1.1.1 bug class) even though
                // single-segment topN was correct.
                let va = a.get(name).and_then(numeric_agg_cell).unwrap_or(0.0);
                let vb = b.get(name).and_then(numeric_agg_cell).unwrap_or(0.0);
                let primary = vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal);
                primary.then_with(|| {
                    let da = a.get(dim_out).and_then(|v| v.as_str()).unwrap_or("");
                    let db = b.get(dim_out).and_then(|v| v.as_str()).unwrap_or("");
                    da.cmp(db)
                })
            });
        }
        TopNMetricSpec::Dimension { ordering, .. } => {
            let numeric = ordering.as_deref() == Some("numeric");
            entries.sort_by(|a, b| {
                let da = a.get(dim_out).and_then(|v| v.as_str()).unwrap_or("");
                let db = b.get(dim_out).and_then(|v| v.as_str()).unwrap_or("");
                if numeric {
                    let va: f64 = da.parse().unwrap_or(0.0);
                    let vb: f64 = db.parse().unwrap_or(0.0);
                    va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    da.cmp(db)
                }
            });
        }
        TopNMetricSpec::Inverted { metric: inner } => {
            sort_topn_merged(entries, dim_out, inner);
            entries.reverse();
        }
    }
}

/// Extract the output column name from a `DimensionSpec` (matches
/// `ferrodruid_query::topn::dimension_spec_output_name` but lives here so
/// the broker doesn't need to depend on a private helper).
fn dimension_output_name(spec: &DimensionSpec) -> &str {
    match spec {
        DimensionSpec::Default { output_name, .. } => output_name,
        DimensionSpec::Extraction { output_name, .. } => output_name,
        DimensionSpec::ListFiltered { delegate, .. } => dimension_output_name(delegate),
        DimensionSpec::RegexFiltered { delegate, .. } => dimension_output_name(delegate),
        DimensionSpec::PrefixFiltered { delegate, .. } => dimension_output_name(delegate),
    }
}

/// Broker-wide cap on the number of DISTINCT merged groups
/// [`merge_groupby`] may accumulate.
///
/// W-C S3 follow-up (High): stripping the groupBy limit push-down (see
/// [`fan_out_query`]) is required for LIMIT correctness, but it means
/// the broker merge accumulates the UNION of every segment's groups.
/// Each per-segment partial is individually capped at
/// [`ferrodruid_query::groupby::DEFAULT_GROUPBY_MAX_KEYS`], so without
/// a broker-side guard a many-segment high-cardinality `GROUP BY …
/// LIMIT k` could grow the merge map to `segments × 1M` keys and
/// exhaust broker memory.  The broker enforces the SAME shared
/// constant on the merged distinct-group count and fails loud
/// ([`DruidError::ResourceLimit`], REST: Druid's
/// `ResourceLimitExceededException`) — never a silent truncation,
/// which would drop groups and could return wrong LIMIT results.
const BROKER_GROUPBY_MERGE_MAX_KEYS: usize = ferrodruid_query::groupby::DEFAULT_GROUPBY_MAX_KEYS;

/// Merge GroupBy results by composite group key, dispatching aggregator
/// merges by kind. Wave 36-G2 (Wave 37B Highs `broker/lib.rs:486-503` +
/// `462-483`): the group key now uses the query-declared dimension list
/// with canonical typed encoding, and per-aggregator merge replaces the
/// blanket numeric sum.
///
/// Merged distinct-group cardinality is capped at
/// [`BROKER_GROUPBY_MERGE_MAX_KEYS`] (fail-loud; see the constant).
fn merge_groupby(
    partials: Vec<QueryResult>,
    dimensions: &[DimensionSpec],
    aggregations: &[AggregatorSpec],
) -> Result<QueryResult> {
    merge_groupby_capped(
        partials,
        dimensions,
        aggregations,
        BROKER_GROUPBY_MERGE_MAX_KEYS,
    )
}

/// [`merge_groupby`] with an explicit merged-group cardinality cap
/// (`0` disables), split out so tests can inject a small cap without
/// materializing `BROKER_GROUPBY_MERGE_MAX_KEYS` groups.
///
/// The guard fires DURING accumulation — as soon as the merged
/// DISTINCT-group count would exceed `max_keys` — so broker memory
/// stays bounded by the cap rather than by the full union; `observed`
/// therefore reports `max_keys + 1`, mirroring the per-segment
/// `groupBy.maxResults` guard in
/// `ferrodruid_query::groupby::GroupByQuery::execute_with_limit`.
fn merge_groupby_capped(
    partials: Vec<QueryResult>,
    dimensions: &[DimensionSpec],
    aggregations: &[AggregatorSpec],
    max_keys: usize,
) -> Result<QueryResult> {
    let mut group_map: HashMap<String, GroupByResult> = HashMap::new();
    let mut key_order: Vec<String> = Vec::new();

    for partial in partials {
        if let QueryResult::GroupBy(entries) = partial {
            for entry in entries {
                let key = group_key(&entry, dimensions);

                if let Some(existing) = group_map.get_mut(&key) {
                    merge_agg_maps(&mut existing.event, &entry.event, aggregations);
                } else {
                    if max_keys > 0 && group_map.len() >= max_keys {
                        return Err(DruidError::ResourceLimit {
                            kind: "groupBy.maxMergedResults",
                            limit: max_keys,
                            observed: group_map.len() + 1,
                        });
                    }
                    key_order.push(key.clone());
                    group_map.insert(key, entry);
                }
            }
        }
    }

    let results: Vec<GroupByResult> = key_order
        .into_iter()
        .filter_map(|k| group_map.remove(&k))
        .collect();

    Ok(QueryResult::GroupBy(results))
}

/// W-C S3: re-apply a groupBy query's result ordering (and limitSpec
/// limit) to a merged result, mirroring the per-segment execution
/// sequence exactly: timestamp sort first, then the shared limitSpec
/// column comparator ([`ferrodruid_query::sort_by_order_columns`] —
/// sketch-envelope- and null-placement-aware), then the limit
/// truncation.  Every sort is stable, so ties keep merge arrival order
/// (deterministic segment-id order per shard since W-C S3).
///
/// Applying the same sequence the executor applies makes this
/// idempotent on a single-partial result: same comparators, same
/// truncation.  Non-groupBy results pass through untouched.
fn apply_groupby_ordering(query: &DruidQuery, mut result: QueryResult) -> QueryResult {
    if let DruidQuery::GroupBy(q) = query
        && let QueryResult::GroupBy(entries) = &mut result
    {
        entries.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        if let Some(spec) = &q.limit_spec {
            if let Some(cols) = &spec.columns {
                ferrodruid_query::sort_by_order_columns(entries, cols);
            }
            if let Some(limit) = spec.limit {
                entries.truncate(limit);
            }
        }
    }
    result
}

/// Merge Scan results by concatenating events, re-applying the scan
/// query's time ordering (W-C S3), and respecting an optional limit.
fn merge_scan(
    partials: Vec<QueryResult>,
    limit: Option<usize>,
    order: Option<&str>,
) -> Result<QueryResult> {
    // Wave 45-A (Wave 37B broker_tail Medium #1 + Medium #3):
    //
    //   1. **Schema is unioned across partials, not picked from the
    //      first shard.**  Pre-W45A `all_columns` was assigned once from
    //      the first non-empty partial; if later shards reported
    //      additional columns they appeared in `events` but were
    //      missing from the advertised `columns` list.  The merge now
    //      walks every partial and appends previously-unseen columns in
    //      shard-arrival order, preserving deterministic ordering for
    //      stable output.
    //   2. **Early-terminate `limit`.**  Pre-W45A every shard's events
    //      were `extend`-ed into `all_events`, then truncated; a small
    //      client `limit` still allowed an O(total_rows) allocation.
    //      The merge now stops appending once `limit` rows have been
    //      collected.  When `limit` is `None` we keep the legacy
    //      behaviour (no cap).
    //
    // W-C S3 (proving-run High, 2026-07-21): a TIME-ORDERED scan
    // (`order` = "ascending"/"descending") must NOT early-terminate on
    // shard-arrival order — the per-shard Historical iterates segments
    // in an order unrelated to time, so concatenate-then-truncate
    // returned the WRONG `limit` rows (Druid oracle: `ORDER BY __time
    // ASC LIMIT k` returns the k earliest rows).  The merge now
    // collects every partial's events (each already per-segment
    // truncated to `limit`, so the buffer is bounded by
    // `shards × limit` when a limit is present), re-sorts them by the
    // `__time` cell in the query direction, THEN truncates.  The
    // early-terminate fast path is kept ONLY for unordered scans,
    // where concatenation order is the (Druid-unordered) contract.
    let time_ordered = matches!(order, Some("ascending" | "descending"));

    let mut all_columns: Vec<String> = Vec::new();
    let mut seen_columns: HashSet<String> = HashSet::new();
    let mut all_events: Vec<HashMap<String, serde_json::Value>> = Vec::new();

    'outer: for partial in partials {
        if let QueryResult::Scan(scan) = partial {
            // Union the schema in arrival order.
            for col in scan.columns {
                if seen_columns.insert(col.clone()) {
                    all_columns.push(col);
                }
            }
            // Append events under the global cap (unordered scans only —
            // a time-ordered merge must see every candidate row before
            // it can know which ones survive the limit).
            if let (Some(lim), false) = (limit, time_ordered) {
                for ev in scan.events {
                    if all_events.len() >= lim {
                        break 'outer;
                    }
                    all_events.push(ev);
                }
            } else {
                all_events.extend(scan.events);
            }
        }
    }

    if time_ordered {
        // Stable sort: rows with equal `__time` keep shard-arrival
        // order (deterministic segment-id order since W-C S3 — see
        // `Historical::execute_query_on_snapshot`), then per-segment
        // row order — matching Druid's n-way merge of per-segment
        // time-sorted streams (timeline order, then natural row order).
        // A missing / non-integer `__time` cell sorts as NULL: FIRST in
        // ASC, LAST in DESC (W1-J NULLS FIRST convention; Druid rows
        // always carry `__time`, so this arm is defensive only —
        // `Option<i64>` ordering has `None < Some(_)`).
        let descending = order == Some("descending");
        all_events.sort_by(|a, b| {
            let ka = a.get("__time").and_then(serde_json::Value::as_i64);
            let kb = b.get("__time").and_then(serde_json::Value::as_i64);
            if descending { kb.cmp(&ka) } else { ka.cmp(&kb) }
        });
    }

    if let Some(lim) = limit {
        all_events.truncate(lim);
    }

    Ok(QueryResult::Scan(ScanResult {
        segment_id: None,
        columns: all_columns,
        events: all_events,
    }))
}

/// Merge Search results: deduplicate hits, re-sort.
///
/// Wave 36-F (Wave 37 R1 medium `lib.rs:348-386`): the previous
/// implementation kept only the first hit on a duplicate `(dimension, value)`
/// pair across shards, which underreported the total `count`. Counts are
/// now summed (saturating on `u64` overflow).
///
/// Wave 45-A (Wave 37B broker_tail Medium #2): the `Vec<SearchHit>`-based
/// dedup walked the full bucket on every incoming hit, making merge time
/// `O(n²)` per timestamp bucket — a CPU-amplification vector when a
/// shard returns a large `Search` result.  The dedup map is now keyed by
/// `(dimension, value)` so duplicate detection is `O(1)` per hit; the
/// per-bucket arrival-order vector is preserved for the final
/// stable-by-`value` sort.
fn merge_search(partials: Vec<QueryResult>) -> Result<QueryResult> {
    // Per-timestamp dedup index: (dimension, value) -> position in `hits`.
    type DedupIdx = HashMap<(String, String), usize>;
    let mut bucket_hits: HashMap<String, Vec<SearchHit>> = HashMap::new();
    let mut bucket_dedup: HashMap<String, DedupIdx> = HashMap::new();
    let mut ts_order: Vec<String> = Vec::new();

    for partial in partials {
        if let QueryResult::Search(entries) = partial {
            for entry in entries {
                let ts = entry.timestamp.clone();
                if !bucket_hits.contains_key(&ts) {
                    ts_order.push(ts.clone());
                }
                let hits = bucket_hits.entry(ts.clone()).or_default();
                let dedup = bucket_dedup.entry(ts).or_default();
                for hit in entry.result {
                    let key = (hit.dimension.clone(), hit.value.clone());
                    if let Some(&idx) = dedup.get(&key) {
                        // Existing entry: sum counts (saturating).
                        if let Some(existing) = hits.get_mut(idx) {
                            existing.count = existing.count.saturating_add(hit.count);
                        }
                    } else {
                        dedup.insert(key, hits.len());
                        hits.push(hit);
                    }
                }
            }
        }
    }

    let results: Vec<SearchResult> = ts_order
        .into_iter()
        .map(|ts| {
            let mut hits = bucket_hits.remove(&ts).unwrap_or_default();
            hits.sort_by(|a, b| a.value.cmp(&b.value));
            SearchResult {
                timestamp: ts,
                result: hits,
            }
        })
        .collect();

    Ok(QueryResult::Search(results))
}

/// Merge segmentMetadata results by concatenation.
fn merge_segment_metadata(partials: Vec<QueryResult>) -> Result<QueryResult> {
    let mut all = Vec::new();
    for partial in partials {
        if let QueryResult::SegmentMetadata(entries) = partial {
            all.extend(entries);
        }
    }
    Ok(QueryResult::SegmentMetadata(all))
}

/// Merge dataSourceMetadata results: take the latest timestamp.
fn merge_datasource_metadata(partials: Vec<QueryResult>) -> Result<QueryResult> {
    let mut all = Vec::new();
    for partial in partials {
        if let QueryResult::DataSourceMetadata(entries) = partial {
            all.extend(entries);
        }
    }
    // Keep the entry with the latest timestamp.
    all.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    all.truncate(1);
    Ok(QueryResult::DataSourceMetadata(all))
}

/// Merge timeBoundary results: take the global min/max.
fn merge_time_boundary(partials: Vec<QueryResult>) -> Result<QueryResult> {
    let mut min_time: Option<String> = None;
    let mut max_time: Option<String> = None;

    for partial in partials {
        if let QueryResult::TimeBoundary(entries) = partial {
            for entry in entries {
                if let Some(mt) = entry.result.get("minTime").and_then(|v| v.as_str())
                    && min_time.as_deref().is_none_or(|cur| mt < cur)
                {
                    min_time = Some(mt.to_string());
                }
                if let Some(mt) = entry.result.get("maxTime").and_then(|v| v.as_str())
                    && max_time.as_deref().is_none_or(|cur| mt > cur)
                {
                    max_time = Some(mt.to_string());
                }
            }
        }
    }

    let mut result_map = serde_json::Map::new();
    if let Some(mt) = &min_time {
        result_map.insert("minTime".to_string(), serde_json::Value::String(mt.clone()));
    }
    if let Some(mt) = &max_time {
        result_map.insert("maxTime".to_string(), serde_json::Value::String(mt.clone()));
    }

    let timestamp = min_time.unwrap_or_default();

    Ok(QueryResult::TimeBoundary(vec![
        ferrodruid_query::TimeBoundaryResult {
            timestamp,
            result: result_map,
        },
    ]))
}

// ---------------------------------------------------------------------------
// Merge helpers
// ---------------------------------------------------------------------------

/// Post-merge finalization for exact-cardinality outputs and merged
/// post-aggregations (fail-closed, 2026-07-11; exact multi-shard union +
/// post-aggregation recompute, 2026-07-12).
///
/// Walks the merged Timeseries / TopN / GroupBy result maps for every
/// output declared by an exact `cardinality` aggregator (including
/// `filtered`-wrapped ones, the E16 exact COUNT(DISTINCT) shape) and:
///
/// * **collapses** a non-saturated
///   [`ferrodruid_aggregator::CardinalityState`] envelope to its exact
///   bare count — the envelope is broker-internal wire plumbing
///   (Wave 40-B; since 2026-07-12 the executors emit it as the standard
///   partial form, so this pass also finalizes the single-partial
///   pass-through) and must never leak to clients;
/// * **fails closed** with [`DruidError::ResourceLimit`] when the envelope
///   is saturated: either the exact cross-segment union exceeded the
///   exact-set cap ([`ferrodruid_aggregator::exact_cardinality_set_cap`]),
///   or a partial shipped a bare count with no per-key information, so the
///   merged number is only an over-counting saturating-add upper bound.
///   Druid never silently returns a wrong exact distinct count, so
///   neither does FerroDruid;
/// * **re-applies the query's post-aggregations** (2026-07-12) on any map
///   where an envelope was collapsed OR when partials were actually merged
///   (`multi_partials`), so a post-aggregation is computed from the merged
///   aggregator values rather than left at the first shard's per-segment
///   value (`merge_agg_maps` keeps non-aggregator fields dst-wins, which
///   goes stale the moment two shards contribute to the same bucket —
///   e.g. the SQL `APPROX_COUNT_DISTINCT` `HLLSketchEstimate` output
///   under-counted on multi-segment datasources even though the hidden
///   sketch itself merged correctly).  For buckets/groups produced by a
///   single segment the recompute is idempotent: same inputs, same
///   emission rule as the executors.
///
/// Bare numeric counts (e.g. `count` fields, or the zero-side identity
/// merge) pass through unchanged — they are exact.
fn finalize_merged_outputs(
    query: &DruidQuery,
    mut result: QueryResult,
    multi_partials: bool,
) -> Result<QueryResult> {
    let (aggregations, post_aggs): (&[AggregatorSpec], Option<&Vec<PostAggregatorSpec>>) =
        match query {
            DruidQuery::Timeseries(q) => (&q.aggregations, q.post_aggregations.as_ref()),
            DruidQuery::TopN(q) => (&q.aggregations, q.post_aggregations.as_ref()),
            DruidQuery::GroupBy(q) => (&q.aggregations, q.post_aggregations.as_ref()),
            // Other query types carry no aggregator result maps.
            _ => return Ok(result),
        };
    let cardinality_outputs: Vec<&str> = aggregations
        .iter()
        .filter(|spec| spec_is_exact_cardinality(spec))
        .map(ferrodruid_aggregator::AggregatorSpec::name)
        .collect();
    let has_post_aggs = post_aggs.is_some_and(|pa| !pa.is_empty());
    if cardinality_outputs.is_empty() && !(multi_partials && has_post_aggs) {
        return Ok(result);
    }
    match &mut result {
        QueryResult::Timeseries(entries) => {
            for entry in entries {
                if finalize_cardinality_map(&mut entry.result, &cardinality_outputs)?
                    || multi_partials
                {
                    reapply_post_aggs(post_aggs, &mut entry.result);
                }
            }
        }
        QueryResult::TopN(entries) => {
            for entry in entries {
                for row in &mut entry.result {
                    if finalize_cardinality_map(row, &cardinality_outputs)? || multi_partials {
                        reapply_post_aggs(post_aggs, row);
                    }
                }
            }
        }
        QueryResult::GroupBy(entries) => {
            for entry in entries {
                if finalize_cardinality_map(&mut entry.event, &cardinality_outputs)?
                    || multi_partials
                {
                    reapply_post_aggs(post_aggs, &mut entry.event);
                }
            }
        }
        _ => {}
    }
    Ok(result)
}

/// `true` when `spec` is an exact `cardinality` aggregation, unwrapping
/// `filtered` wrappers (the E16 exact COUNT(DISTINCT) lowering).
fn spec_is_exact_cardinality(spec: &AggregatorSpec) -> bool {
    match spec {
        AggregatorSpec::Cardinality { .. } => true,
        AggregatorSpec::Filtered { aggregator, .. } => spec_is_exact_cardinality(aggregator),
        _ => false,
    }
}

/// Apply the [`finalize_merged_outputs`] cardinality rule to one result map.
///
/// Returns `Ok(true)` when at least one envelope was collapsed to its
/// exact count (the caller then re-applies post-aggregations on the map).
/// Uses the clone-free [`ferrodruid_aggregator::CardinalityState::peek_json`]
/// probe — the envelope can carry up to the 1,000,000-key exact-set cap,
/// so a full `from_json` deserialization here would clone the whole set
/// just to read two fields.
///
/// Untrusted-peer hardening (2026-07-12, Codex HIGH findings 1+4): this
/// is the last gate before a count reaches the client, and partials are
/// hostile input, so `peek_json` VALIDATES the envelope invariants (a
/// non-saturated envelope's `count` must equal its actual distinct value
/// set — a forged `values=["x"], count=1000000, saturated=false` used to
/// finalize as an exact 1,000,000 here).  Anything tagged-but-malformed,
/// and any non-envelope value that is not a bare u64 count, fails the
/// query closed.
fn finalize_cardinality_map(
    map: &mut serde_json::Map<String, serde_json::Value>,
    cardinality_outputs: &[&str],
) -> Result<bool> {
    let mut collapsed_any = false;
    for name in cardinality_outputs {
        let Some(value) = map.get_mut(*name) else {
            continue;
        };
        let peeked = match ferrodruid_aggregator::CardinalityState::peek_json(value) {
            Ok(peeked) => peeked,
            Err(_) => {
                return Err(DruidError::ResourceLimit {
                    kind: ferrodruid_aggregator::CARDINALITY_MALFORMED_STATE_KIND,
                    limit: ferrodruid_aggregator::exact_cardinality_set_cap(),
                    observed: 0,
                });
            }
        };
        let Some((saturated, count)) = peeked else {
            // Not an envelope: only a bare numeric count is a legitimate
            // exact wire shape — anything else fails closed rather than
            // leaking peer-controlled JSON under an exact-count output.
            if value.as_u64().is_none() {
                return Err(DruidError::ResourceLimit {
                    kind: ferrodruid_aggregator::CARDINALITY_MALFORMED_STATE_KIND,
                    limit: ferrodruid_aggregator::exact_cardinality_set_cap(),
                    observed: 0,
                });
            }
            continue;
        };
        if saturated {
            return Err(DruidError::ResourceLimit {
                kind: ferrodruid_aggregator::CARDINALITY_CROSS_SHARD_MERGE_LIMIT_KIND,
                limit: ferrodruid_aggregator::exact_cardinality_set_cap(),
                observed: usize::try_from(count).unwrap_or(usize::MAX),
            });
        }
        *value = serde_json::Value::Number(serde_json::Number::from(count));
        collapsed_any = true;
    }
    Ok(collapsed_any)
}

/// Re-evaluate the query's post-aggregations on a merged result map after
/// exact-cardinality envelopes were collapsed to their union counts
/// (multi-shard exact union, 2026-07-12).
///
/// The per-shard partials computed their post-aggregation outputs from the
/// per-segment counts; after a cross-segment union those values are stale
/// (the map keeps the first shard's number under `merge_agg_maps`'s
/// dimension-passthrough rule).  Recomputing here from the collapsed exact
/// counts is idempotent for buckets/groups produced by a single segment
/// (same inputs, same emission rule as the executors: `Number::from_f64`,
/// explicit JSON null when evaluation yields none).  The input snapshot is
/// taken before the loop, matching the executors — post-aggregations never
/// see each other's outputs.
fn reapply_post_aggs(
    post_aggs: Option<&Vec<PostAggregatorSpec>>,
    map: &mut serde_json::Map<String, serde_json::Value>,
) {
    let Some(post_aggs) = post_aggs else {
        return;
    };
    if post_aggs.is_empty() {
        return;
    }
    let agg_results: HashMap<String, serde_json::Value> =
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    for pa in post_aggs {
        let json_val = pa
            .evaluate(&agg_results)
            .and_then(serde_json::Number::from_f64)
            .map_or(serde_json::Value::Null, serde_json::Value::Number);
        map.insert(pa.name().to_string(), json_val);
    }
}

/// Merge aggregation values from `src` into `dst`, dispatching by
/// aggregator kind from the query spec.
///
/// Wave 36-G2 (Wave 37B High `broker/lib.rs:462-483`): the previous
/// implementation summed every numeric field across shards regardless of
/// aggregator type, corrupting Min/Max/First/Last/Cardinality results. The
/// new behavior consults the query's [`AggregatorSpec`] list and applies
/// the matching merge rule (sum / min / max / first-wins / last-wins /
/// cardinality-additive). Fields not declared as aggregations (i.e.
/// dimension passthrough) are left at the destination value if already
/// present, otherwise copied from source.
fn merge_agg_maps(
    dst: &mut serde_json::Map<String, serde_json::Value>,
    src: &serde_json::Map<String, serde_json::Value>,
    aggregations: &[AggregatorSpec],
) {
    // Index aggregations by output name for O(1) dispatch.
    let mut spec_by_name: HashMap<&str, &AggregatorSpec> =
        HashMap::with_capacity(aggregations.len());
    for spec in aggregations {
        spec_by_name.insert(spec.name(), spec);
    }

    for (key, src_val) in src {
        if let Some(dst_val) = dst.get_mut(key) {
            if let Some(&spec) = spec_by_name.get(key.as_str()) {
                // Aggregator field: dispatch by spec.
                *dst_val = merge_json_by_spec(spec, dst_val, src_val);
            }
            // Non-aggregator field (dimension passthrough): keep dst value.
        } else {
            dst.insert(key.clone(), src_val.clone());
        }
    }
}

/// Build a composite group key from a GroupBy result for deduplication.
///
/// Wave 36-G2 (Wave 37B High `broker/lib.rs:486-503`): the previous
/// implementation only included `event` fields where `v.is_string()`, so
/// numeric/boolean/null grouping dimensions were silently dropped from the
/// dedup key, collapsing distinct groups into one. We now build the key
/// from the **query-declared dimension list** with canonical typed
/// encoding, so that JSON `1` (number) and `"1"` (string) under the same
/// dimension name produce different keys.
///
/// Collision hardening (2026-07-21 High): the previous encoding joined
/// `<output_name>=<enc>` parts with an unescaped `|` separator, so any
/// value containing the separator or the `<name>=<type>:` framing bytes
/// could forge an element boundary — with dimensions `d1,d2`, the
/// DISTINCT tuples `("x|d2=s:y", "z")` and `("x", "y|d2=s:z")` produced
/// the SAME key, silently merging two real groups (cardinality
/// undercount, dropped group, wrong `ORDER BY … LIMIT` retention, and a
/// downward-skewed [`BROKER_GROUPBY_MERGE_MAX_KEYS`] count). The key is
/// now a **length-prefixed frame sequence**: every element is appended
/// via [`push_key_frame`] as `<decimal-byte-len>:<bytes>`, which decodes
/// deterministically left-to-right, so two keys are byte-equal IFF their
/// element sequences are equal element-wise — no value content can forge
/// a boundary. Frames align positionally (frame 0 = bucket timestamp,
/// frame i = the canonical encoding of the i-th query-declared
/// dimension) and the dimension list is fixed for the whole merge, so
/// identical tuples still map to identical keys across shards.
fn group_key(result: &GroupByResult, dimensions: &[DimensionSpec]) -> String {
    let mut key = String::with_capacity(32 + 16 * dimensions.len());
    push_key_frame(&mut key, &result.timestamp);
    for dim in dimensions {
        let out_name = dimension_output_name(dim);
        let value = result.event.get(out_name);
        push_key_frame(&mut key, &canonical_dim_encoding(value));
    }
    key
}

/// Append one length-prefixed frame (`<decimal-byte-len>:<bytes>`) of a
/// [`group_key`]. A decoder reads digits up to the first `:` (a length
/// is always pure decimal digits, so the terminator is unambiguous),
/// then exactly that many bytes — frame boundaries therefore cannot be
/// forged by any byte inside a value, unlike an unescaped separator.
fn push_key_frame(key: &mut String, part: &str) {
    key.push_str(&part.len().to_string());
    key.push(':');
    key.push_str(part);
}

/// Encode a JSON dimension value canonically with a type tag so that
/// distinct types (e.g. `1` vs `"1"` vs `true`) never collide in
/// `group_key`.
fn canonical_dim_encoding(value: Option<&serde_json::Value>) -> String {
    match value {
        None | Some(serde_json::Value::Null) => "null:".to_string(),
        Some(serde_json::Value::String(s)) => format!("s:{s}"),
        Some(serde_json::Value::Number(n)) => format!("n:{n}"),
        Some(serde_json::Value::Bool(b)) => format!("b:{b}"),
        Some(other) => format!("j:{other}"),
    }
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
    use ferrodruid_query::{
        GroupByResult, ScanResult, SearchHit, SearchResult, TimeseriesResult, TopNResult,
    };
    use ferrodruid_segment::Interval;
    use ferrodruid_segment::SegmentData;
    use ferrodruid_segment::column::{ColumnData, StringColumnData};

    /// Build a synthetic segment (same as in ferrodruid-query tests).
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

    // -----------------------------------------------------------------------
    // Merge tests
    // -----------------------------------------------------------------------

    #[test]
    fn merge_timeseries_same_buckets() {
        let ts1 = QueryResult::Timeseries(vec![TimeseriesResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: {
                let mut m = serde_json::Map::new();
                m.insert("cnt".to_string(), serde_json::json!(3));
                m.insert("total".to_string(), serde_json::json!(60.0));
                m
            },
        }]);
        let ts2 = QueryResult::Timeseries(vec![TimeseriesResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: {
                let mut m = serde_json::Map::new();
                m.insert("cnt".to_string(), serde_json::json!(3));
                m.insert("total".to_string(), serde_json::json!(150.0));
                m
            },
        }]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "day",
                "aggregations": [
                    {"type":"count","name":"cnt"},
                    {"type":"doubleSum","name":"total","fieldName":"value"}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![ts1, ts2]).expect("merge");
        match merged {
            QueryResult::Timeseries(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].result.get("cnt"), Some(&serde_json::json!(6)));
                assert_eq!(
                    results[0].result.get("total"),
                    Some(&serde_json::json!(210.0))
                );
            }
            _ => panic!("expected timeseries"),
        }
    }

    #[test]
    fn merge_topn_combined() {
        let t1 = QueryResult::TopN(vec![TopNResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: vec![
                {
                    let mut m = serde_json::Map::new();
                    m.insert("region".to_string(), serde_json::json!("us"));
                    m.insert("cnt".to_string(), serde_json::json!(5));
                    m
                },
                {
                    let mut m = serde_json::Map::new();
                    m.insert("region".to_string(), serde_json::json!("eu"));
                    m.insert("cnt".to_string(), serde_json::json!(3));
                    m
                },
            ],
        }]);
        let t2 = QueryResult::TopN(vec![TopNResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: vec![
                {
                    let mut m = serde_json::Map::new();
                    m.insert("region".to_string(), serde_json::json!("us"));
                    m.insert("cnt".to_string(), serde_json::json!(2));
                    m
                },
                {
                    let mut m = serde_json::Map::new();
                    m.insert("region".to_string(), serde_json::json!("jp"));
                    m.insert("cnt".to_string(), serde_json::json!(4));
                    m
                },
            ],
        }]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "topN",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "dimension": {"type":"default","dimension":"region","output_name":"region","output_type":"STRING"},
                "threshold": 2,
                "metric": {"type":"numeric","metric":"cnt"},
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![t1, t2]).expect("merge");
        match merged {
            QueryResult::TopN(results) => {
                assert_eq!(results.len(), 1);
                // us=5+2=7, jp=4, eu=3 -> top 2 = us(7), jp(4)
                assert!(results[0].result.len() <= 2);
                let first = &results[0].result[0];
                assert_eq!(first.get("region"), Some(&serde_json::json!("us")));
                assert_eq!(first.get("cnt"), Some(&serde_json::json!(7)));
            }
            _ => panic!("expected topN"),
        }
    }

    #[test]
    fn merge_scan_with_limit() {
        let s1 = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["a".to_string()],
            events: vec![
                HashMap::from([("a".to_string(), serde_json::json!(1))]),
                HashMap::from([("a".to_string(), serde_json::json!(2))]),
                HashMap::from([("a".to_string(), serde_json::json!(3))]),
            ],
        });
        let s2 = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["a".to_string()],
            events: vec![
                HashMap::from([("a".to_string(), serde_json::json!(4))]),
                HashMap::from([("a".to_string(), serde_json::json!(5))]),
            ],
        });

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "scan",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "limit": 4
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![s1, s2]).expect("merge");
        match merged {
            QueryResult::Scan(scan) => {
                assert_eq!(scan.events.len(), 4);
            }
            _ => panic!("expected scan"),
        }
    }

    #[test]
    fn merge_groupby_results() {
        let g1 = QueryResult::GroupBy(vec![GroupByResult {
            version: "v1".to_string(),
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            event: {
                let mut m = serde_json::Map::new();
                m.insert("region".to_string(), serde_json::json!("us"));
                m.insert("cnt".to_string(), serde_json::json!(3));
                m
            },
        }]);
        let g2 = QueryResult::GroupBy(vec![GroupByResult {
            version: "v1".to_string(),
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            event: {
                let mut m = serde_json::Map::new();
                m.insert("region".to_string(), serde_json::json!("us"));
                m.insert("cnt".to_string(), serde_json::json!(2));
                m
            },
        }]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "groupBy",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": [{"type":"default","dimension":"region","output_name":"region","output_type":"STRING"}],
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![g1, g2]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].event.get("cnt"), Some(&serde_json::json!(5)));
                assert_eq!(
                    results[0].event.get("region"),
                    Some(&serde_json::json!("us"))
                );
            }
            _ => panic!("expected groupBy"),
        }
    }

    // -----------------------------------------------------------------------
    // Local execution test
    // -----------------------------------------------------------------------

    #[test]
    fn execute_local_two_historicals() {
        let dir1 = tempfile::tempdir().expect("tempdir");
        let dir2 = tempfile::tempdir().expect("tempdir");

        let hist1 = Historical::new(dir1.path().to_path_buf(), 1_000_000);
        hist1
            .load_segment("seg_a", build_test_segment())
            .expect("load");
        hist1.set_segment_datasource("seg_a", "wiki").expect("ds");

        let hist2 = Historical::new(dir2.path().to_path_buf(), 1_000_000);
        hist2
            .load_segment("seg_b", build_test_segment())
            .expect("load");
        hist2.set_segment_datasource("seg_b", "wiki").expect("ds");

        let broker = Broker::new();
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
        .expect("parse");

        let result = broker
            .execute_local(&query, &[&hist1, &hist2])
            .expect("execute_local");

        match &result.result {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts.len(), 1);
                // 6 rows per segment * 2 segments = 12 total.
                assert_eq!(ts[0].result.get("cnt"), Some(&serde_json::json!(12)));
                // 210.0 per segment * 2 = 420.0.
                assert_eq!(ts[0].result.get("total"), Some(&serde_json::json!(420.0)));
            }
            _ => panic!("expected timeseries"),
        }

        assert!(!result.query_id.is_empty());
    }

    #[test]
    fn execute_local_union_all_rejects_ordered_or_bounded_branches() {
        let dir1 = tempfile::tempdir().expect("tempdir");
        let dir2 = tempfile::tempdir().expect("tempdir");
        let hist1 = Historical::new(dir1.path().to_path_buf(), 1_000_000);
        hist1
            .load_segment_with_datasource("seg_a", "wiki", build_test_segment())
            .expect("load a");
        let hist2 = Historical::new(dir2.path().to_path_buf(), 1_000_000);
        hist2
            .load_segment_with_datasource("seg_b", "wiki", build_test_segment())
            .expect("load b");

        let scan = |limit: usize| {
            serde_json::from_value::<DruidQuery>(serde_json::json!({
                "queryType": "scan",
                "dataSource": {"type": "table", "name": "wiki"},
                "intervals": ["2024-01-01/2024-01-03"],
                "columns": ["value"],
                "limit": limit
            }))
            .expect("scan")
        };
        let query = DruidQuery::UnionAll(vec![scan(1), scan(2)]);
        let err = Broker::new()
            .execute_local(&query, &[&hist1, &hist2])
            .expect_err("bounded UNION ALL branches must fail closed");
        assert!(
            format!("{err}").contains("unbounded"),
            "error must explain the supported subset: {err}"
        );
    }

    #[test]
    fn execute_local_union_all_accepts_default_unordered_branches() {
        // The SQL planner emits `order: None` for a scan branch with no
        // ORDER BY. Druid treats an omitted scan order as `none`, so an
        // ordinary `SELECT ... UNION ALL SELECT ...` (no ORDER BY, no LIMIT)
        // must be accepted, not rejected by the UNION guard.
        let dir1 = tempfile::tempdir().expect("tempdir");
        let dir2 = tempfile::tempdir().expect("tempdir");
        let hist1 = Historical::new(dir1.path().to_path_buf(), 1_000_000);
        hist1
            .load_segment_with_datasource("seg_a", "wiki", build_test_segment())
            .expect("load a");
        let hist2 = Historical::new(dir2.path().to_path_buf(), 1_000_000);
        hist2
            .load_segment_with_datasource("seg_b", "wiki", build_test_segment())
            .expect("load b");

        // No `order` key => ScanQuery.order == None (planner default).
        let scan = || {
            serde_json::from_value::<DruidQuery>(serde_json::json!({
                "queryType": "scan",
                "dataSource": {"type": "table", "name": "wiki"},
                "intervals": ["2024-01-01/2024-01-03"],
                "columns": ["value"]
            }))
            .expect("scan")
        };
        let query = DruidQuery::UnionAll(vec![scan(), scan()]);
        let result = Broker::new()
            .execute_local(&query, &[&hist1, &hist2])
            .expect("default-unordered UNION ALL must be accepted");
        assert!(!result.query_id.is_empty());
    }

    #[test]
    fn execute_local_union_all_maps_differently_named_branches_positionally() {
        // `SELECT region ... UNION ALL SELECT value ...`: Druid names the
        // output from the first branch (`region`) and maps branch 2's `value`
        // into it by position. The merged result must carry ONE column
        // `region` holding BOTH branch 1's region strings and branch 2's
        // numeric values — branch 2 must NOT be dropped, and no stray
        // `value` column must appear.
        let dir1 = tempfile::tempdir().expect("tempdir");
        let dir2 = tempfile::tempdir().expect("tempdir");
        let hist1 = Historical::new(dir1.path().to_path_buf(), 1_000_000);
        hist1
            .load_segment_with_datasource("seg_a", "wiki", build_test_segment())
            .expect("load a");
        let hist2 = Historical::new(dir2.path().to_path_buf(), 1_000_000);
        hist2
            .load_segment_with_datasource("seg_b", "wiki", build_test_segment())
            .expect("load b");

        let scan = |column: &str| {
            serde_json::from_value::<DruidQuery>(serde_json::json!({
                "queryType": "scan",
                "dataSource": {"type": "table", "name": "wiki"},
                "intervals": ["2024-01-01/2024-01-03"],
                "columns": [column]
            }))
            .expect("scan")
        };
        let query = DruidQuery::UnionAll(vec![scan("region"), scan("value")]);
        let result = Broker::new()
            .execute_local(&query, &[&hist1, &hist2])
            .expect("differently-named UNION ALL must be accepted");

        let QueryResult::Scan(scan_result) = result.result else {
            panic!("UNION ALL must produce a scan result");
        };
        assert_eq!(
            scan_result.columns,
            vec!["region".to_string()],
            "output must be named from the first branch"
        );
        // Every row is keyed only under `region`; none leaks a `value` key.
        assert!(
            scan_result.events.iter().all(|e| !e.contains_key("value")),
            "branch 2's native `value` key must be remapped to `region`"
        );
        // Branch 1 contributes region strings; branch 2 contributes numbers,
        // now under `region`.
        let has_string_region = scan_result
            .events
            .iter()
            .any(|e| e.get("region").and_then(|v| v.as_str()).is_some());
        let has_numeric_region = scan_result.events.iter().any(|e| {
            e.get("region")
                .and_then(serde_json::Value::as_f64)
                .is_some()
        });
        assert!(
            has_string_region && has_numeric_region,
            "the `region` column must hold BOTH branches' values (branch 2 not dropped)"
        );
    }

    #[test]
    fn execute_local_union_all_tolerates_a_branch_with_no_loaded_segments() {
        // A UNION ALL where one branch's datasource has no loaded segments must
        // still return the populated branch's rows, not fail with an arity
        // error from the empty branch's absent column metadata.
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 1_000_000);
        hist.load_segment_with_datasource("seg_a", "wiki", build_test_segment())
            .expect("load wiki");

        let scan = |datasource: &str, column: &str| {
            serde_json::from_value::<DruidQuery>(serde_json::json!({
                "queryType": "scan",
                "dataSource": {"type": "table", "name": datasource},
                "intervals": ["2024-01-01/2024-01-03"],
                "columns": [column]
            }))
            .expect("scan")
        };

        // Populated first branch, empty second branch.
        let query = DruidQuery::UnionAll(vec![scan("wiki", "region"), scan("empty_ds", "value")]);
        let result = Broker::new()
            .execute_local(&query, &[&hist])
            .expect("UNION ALL with an empty branch must not fail");
        let QueryResult::Scan(scan_result) = result.result else {
            panic!("expected scan result");
        };
        assert_eq!(scan_result.columns, vec!["region".to_string()]);
        assert!(
            !scan_result.events.is_empty(),
            "the populated branch's rows must survive"
        );

        // Empty FIRST branch, populated second branch: output is still named
        // from the first branch's declared column (`value`).
        let query = DruidQuery::UnionAll(vec![scan("empty_ds", "value"), scan("wiki", "region")]);
        let result = Broker::new()
            .execute_local(&query, &[&hist])
            .expect("UNION ALL with an empty first branch must not fail");
        let QueryResult::Scan(scan_result) = result.result else {
            panic!("expected scan result");
        };
        assert_eq!(scan_result.columns, vec!["value".to_string()]);
        assert!(
            !scan_result.events.is_empty(),
            "the populated later branch's rows must survive, remapped to `value`"
        );
    }

    // -----------------------------------------------------------------------
    // Register / deregister tests
    // -----------------------------------------------------------------------

    #[test]
    fn register_and_deregister_historical() {
        let broker = Broker::new();

        broker.register_historical(HistoricalEndpoint {
            name: "hist-1".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8083,
            tls_port: None,
        });
        broker.register_historical(HistoricalEndpoint {
            name: "hist-2".to_string(),
            host: "127.0.0.2".to_string(),
            port: 8083,
            tls_port: Some(8283),
        });

        let known = broker.known_historicals();
        assert_eq!(known.len(), 2);
        assert!(known.contains(&"hist-1".to_string()));
        assert!(known.contains(&"hist-2".to_string()));

        broker.deregister_historical("hist-1");
        let known = broker.known_historicals();
        assert_eq!(known.len(), 1);
        assert!(!known.contains(&"hist-1".to_string()));
    }

    #[test]
    fn merge_empty_partials() {
        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => assert!(ts.is_empty()),
            _ => panic!("expected timeseries"),
        }
    }

    #[test]
    fn merge_search_deduplicates() {
        let s1 = QueryResult::Search(vec![SearchResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: vec![
                SearchHit {
                    dimension: "region".to_string(),
                    value: "us".to_string(),
                    count: 1,
                },
                SearchHit {
                    dimension: "region".to_string(),
                    value: "eu".to_string(),
                    count: 1,
                },
            ],
        }]);
        let s2 = QueryResult::Search(vec![SearchResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: vec![SearchHit {
                dimension: "region".to_string(),
                value: "us".to_string(),
                count: 1,
            }],
        }]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "search",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "query": {"type":"contains","value":"u"},
                "searchDimensions": ["region"]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![s1, s2]).expect("merge");
        match merged {
            QueryResult::Search(results) => {
                assert_eq!(results.len(), 1);
                // "us" should appear only once (deduplicated).
                assert_eq!(results[0].result.len(), 2);
                let values: Vec<&str> =
                    results[0].result.iter().map(|h| h.value.as_str()).collect();
                assert!(values.contains(&"us"));
                assert!(values.contains(&"eu"));
            }
            _ => panic!("expected search"),
        }
    }

    // -------------------------------------------------------------------
    // Wave 36-F: Wave 37 R1 medium "Broker search merge drops hit counts".
    // -------------------------------------------------------------------

    /// `merge_search` must combine duplicate `(dimension, value)` hits across
    /// shards by summing their counts, not by keeping only the first one.
    #[test]
    fn merge_search_sums_duplicate_counts() {
        let s1 = QueryResult::Search(vec![SearchResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: vec![
                SearchHit {
                    dimension: "region".to_string(),
                    value: "us".to_string(),
                    count: 7,
                },
                SearchHit {
                    dimension: "region".to_string(),
                    value: "eu".to_string(),
                    count: 3,
                },
            ],
        }]);
        let s2 = QueryResult::Search(vec![SearchResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: vec![SearchHit {
                dimension: "region".to_string(),
                value: "us".to_string(),
                count: 5,
            }],
        }]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "search",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "query": {"type":"contains","value":"u"},
                "searchDimensions": ["region"]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![s1, s2]).expect("merge");
        match merged {
            QueryResult::Search(results) => {
                assert_eq!(results.len(), 1);
                let us = results[0]
                    .result
                    .iter()
                    .find(|h| h.value == "us")
                    .expect("us hit");
                assert_eq!(us.count, 12, "duplicate us counts must sum (7 + 5)");
                let eu = results[0]
                    .result
                    .iter()
                    .find(|h| h.value == "eu")
                    .expect("eu hit");
                assert_eq!(eu.count, 3);
            }
            _ => panic!("expected search"),
        }
    }

    // -------------------------------------------------------------------
    // Wave 36-F: Wave 37 R1 medium "Broker TopN merge ignores the query's
    // declared ranking metric". The merge path must rank by `query.metric`,
    // not by "first numeric field".
    // -------------------------------------------------------------------

    /// Construct a TopN result with two string fields and two numeric fields
    /// where the "first numeric" heuristic and the declared metric
    /// disagree.  The merger must rank by the declared metric.
    #[test]
    fn merge_topn_uses_query_metric_not_first_numeric() {
        // Each entry has: region (the declared dimension), tag (also string),
        // throughput (the field that would win the "first numeric" heuristic
        // if iteration order put it first), and revenue (the declared metric).
        let mk = |region: &str, throughput: i64, revenue: i64| {
            let mut m = serde_json::Map::new();
            m.insert("region".to_string(), serde_json::json!(region));
            m.insert("tag".to_string(), serde_json::json!("misc"));
            m.insert("throughput".to_string(), serde_json::json!(throughput));
            m.insert("revenue".to_string(), serde_json::json!(revenue));
            m
        };

        let t1 = QueryResult::TopN(vec![TopNResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: vec![mk("us", 1000, 10), mk("eu", 1, 100)],
        }]);
        let t2 = QueryResult::TopN(vec![TopNResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: vec![mk("us", 500, 5), mk("eu", 0, 50)],
        }]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "topN",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "dimension": {"type":"default","dimension":"region","output_name":"region","output_type":"STRING"},
                "threshold": 2,
                "metric": {"type":"numeric","metric":"revenue"},
                "aggregations": [
                    {"type":"longSum","name":"throughput","fieldName":"throughput"},
                    {"type":"longSum","name":"revenue","fieldName":"revenue"}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![t1, t2]).expect("merge");
        match merged {
            QueryResult::TopN(results) => {
                assert_eq!(results.len(), 1);
                // Ranked by revenue: eu = 150, us = 15. So eu must come first.
                let first = &results[0].result[0];
                assert_eq!(
                    first.get("region"),
                    Some(&serde_json::json!("eu")),
                    "merge must rank by declared metric `revenue`, not `throughput`"
                );
                assert_eq!(first.get("revenue"), Some(&serde_json::json!(150)));
                let second = &results[0].result[1];
                assert_eq!(second.get("region"), Some(&serde_json::json!("us")));
                assert_eq!(second.get("revenue"), Some(&serde_json::json!(15)));
            }
            _ => panic!("expected topN"),
        }
    }

    // -------------------------------------------------------------------
    // Wave 36-G2: Wave 37B High `broker/lib.rs:462-483`. `merge_agg_maps`
    // must dispatch by aggregator kind instead of summing every numeric
    // field. Tests cover Min, Max, First, Last, Cardinality, and a mixed
    // dispatch case.
    // -------------------------------------------------------------------

    fn ts_partial(values: &[(&str, serde_json::Value)]) -> QueryResult {
        let mut m = serde_json::Map::new();
        for (k, v) in values {
            m.insert((*k).to_string(), v.clone());
        }
        QueryResult::Timeseries(vec![TimeseriesResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: m,
        }])
    }

    #[test]
    fn merge_agg_maps_min_takes_min_not_sum() {
        let p1 = ts_partial(&[("min_v", serde_json::json!(10))]);
        let p2 = ts_partial(&[("min_v", serde_json::json!(3))]);
        let p3 = ts_partial(&[("min_v", serde_json::json!(7))]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"longMin","name":"min_v","fieldName":"value"}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![p1, p2, p3]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts.len(), 1);
                assert_eq!(
                    ts[0].result.get("min_v"),
                    Some(&serde_json::json!(3)),
                    "min must take the minimum across shards (was 20 under buggy SUM-everything)"
                );
            }
            _ => panic!("expected timeseries"),
        }
    }

    #[test]
    fn merge_agg_maps_max_takes_max_not_sum() {
        let p1 = ts_partial(&[("max_v", serde_json::json!(10.5))]);
        let p2 = ts_partial(&[("max_v", serde_json::json!(99.0))]);
        let p3 = ts_partial(&[("max_v", serde_json::json!(42.0))]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"doubleMax","name":"max_v","fieldName":"value"}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![p1, p2, p3]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts.len(), 1);
                assert_eq!(
                    ts[0].result.get("max_v"),
                    Some(&serde_json::json!(99.0)),
                    "max must take the maximum, not 151.5 (the buggy SUM)"
                );
            }
            _ => panic!("expected timeseries"),
        }
    }

    #[test]
    fn merge_agg_maps_first_keeps_existing_value() {
        // First/Last in the broker JSON layer cannot consult per-row
        // timestamps (the published shape is bare value). Honest semantics:
        // first-wins keeps dst (idempotent), last-wins takes src.
        let p1 = ts_partial(&[("first_v", serde_json::json!(100))]);
        let p2 = ts_partial(&[("first_v", serde_json::json!(200))]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"longFirst","name":"first_v","fieldName":"value"}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![p1, p2]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts.len(), 1);
                assert_eq!(
                    ts[0].result.get("first_v"),
                    Some(&serde_json::json!(100)),
                    "first must keep the dst value, NOT 300 (the buggy SUM)"
                );
            }
            _ => panic!("expected timeseries"),
        }
    }

    #[test]
    fn merge_agg_maps_last_takes_new_value() {
        let p1 = ts_partial(&[("last_v", serde_json::json!(100))]);
        let p2 = ts_partial(&[("last_v", serde_json::json!(200))]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"longLast","name":"last_v","fieldName":"value"}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![p1, p2]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts.len(), 1);
                assert_eq!(
                    ts[0].result.get("last_v"),
                    Some(&serde_json::json!(200)),
                    "last must take the src value, NOT 300 (the buggy SUM)"
                );
            }
            _ => panic!("expected timeseries"),
        }
    }

    #[test]
    fn merge_agg_maps_cardinality_bare_count_merge_fails_closed() {
        // Fail-closed (2026-07-11): in the broker JSON layer two bare
        // counts carry no per-key information, so their sum is only an
        // over-counting upper bound (7 + 11 = 18 even when the underlying
        // sets overlap). The pre-fix behavior returned the silent 18;
        // finalization must now FAIL the query instead.
        let p1 = ts_partial(&[("uniq", serde_json::json!(7))]);
        let p2 = ts_partial(&[("uniq", serde_json::json!(11))]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"cardinality","name":"uniq","fields":["region"]}
                ]
            }"#,
        )
        .expect("parse");

        let err = Broker::merge_results(&query, vec![p1, p2])
            .expect_err("cross-partial bare-count cardinality add must fail closed");
        match err {
            ferrodruid_common::error::DruidError::ResourceLimit { kind, .. } => {
                assert!(
                    kind.contains("cardinality.crossShardExactMerge"),
                    "kind must name the cross-shard merge limit, got: {kind}"
                );
            }
            other => panic!("expected ResourceLimit, got {other:?}"),
        }
    }

    /// A zero count on one side is exact (union with the empty set), so
    /// the merge must NOT fail closed — the other side's exact count
    /// stands. This is the E16 shape where one shard's not-null filtered
    /// cardinality matched no rows.
    #[test]
    fn merge_agg_maps_cardinality_zero_side_stays_exact() {
        let p1 = ts_partial(&[("uniq", serde_json::json!(0))]);
        let p2 = ts_partial(&[("uniq", serde_json::json!(11))]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"cardinality","name":"uniq","fields":["region"]}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![p1, p2]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts.len(), 1);
                assert_eq!(
                    ts[0].result.get("uniq"),
                    Some(&serde_json::json!(11)),
                    "zero-side merge is exact and must pass through"
                );
            }
            _ => panic!("expected timeseries"),
        }
    }

    fn topn_partial(rows: &[(&str, serde_json::Value)]) -> QueryResult {
        let result = rows
            .iter()
            .map(|(dim, uniq)| {
                let mut m = serde_json::Map::new();
                m.insert("region".to_string(), serde_json::json!(dim));
                m.insert("uniq".to_string(), uniq.clone());
                m
            })
            .collect();
        QueryResult::TopN(vec![TopNResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result,
        }])
    }

    fn topn_cardinality_query() -> DruidQuery {
        serde_json::from_str(
            r#"{
                "queryType": "topN",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "dimension": "region",
                "metric": "uniq",
                "threshold": 1,
                "aggregations": [
                    {"type":"cardinality","name":"uniq","fields":["user"]}
                ]
            }"#,
        )
        .expect("parse")
    }

    /// Fail-closed (2026-07-11) — truncation must not evade the check: a
    /// merged dim value whose cardinality degraded to a saturated
    /// envelope does NOT sort as a number, so pre-fix it silently fell
    /// below `threshold` and was truncated away, letting the query
    /// succeed with a wrong top-N. The merge must fail closed even when
    /// the offending entry would not survive the cut.
    #[test]
    fn merge_topn_saturated_cardinality_fails_closed_even_when_truncated() {
        // "b" merges 4 + 5 across partials (degrades, saturated
        // envelope); "a" appears once with 100 (bare, exact) and wins
        // the threshold=1 cut.
        let p1 = topn_partial(&[("a", serde_json::json!(100)), ("b", serde_json::json!(4))]);
        let p2 = topn_partial(&[("b", serde_json::json!(5))]);

        let err = Broker::merge_results(&topn_cardinality_query(), vec![p1, p2])
            .expect_err("saturated cardinality entry must fail closed, not be truncated away");
        match err {
            ferrodruid_common::error::DruidError::ResourceLimit { kind, .. } => {
                assert!(
                    kind.contains("cardinality.crossShardExactMerge"),
                    "got: {kind}"
                );
            }
            other => panic!("expected ResourceLimit, got {other:?}"),
        }
    }

    /// Non-saturated typed-state unions must collapse to their exact
    /// counts BEFORE ranking so the metric sorts on real numbers.
    #[test]
    fn merge_topn_state_union_ranks_on_collapsed_exact_count() {
        use ferrodruid_aggregator::{Aggregator, CardinalityAggregator};

        let mut x1 = CardinalityAggregator::new(false);
        let mut x2 = CardinalityAggregator::new(false);
        for v in 0..10u64 {
            x1.aggregate(Some(&serde_json::json!(v)));
        }
        for v in 5..15u64 {
            x2.aggregate(Some(&serde_json::json!(v)));
        }

        // "x" unions to 15 exact; "y" is a bare 3 seen on one shard only.
        let p1 = topn_partial(&[
            ("x", x1.into_state().to_json()),
            ("y", serde_json::json!(3)),
        ]);
        let p2 = topn_partial(&[("x", x2.into_state().to_json())]);

        let merged = Broker::merge_results(&topn_cardinality_query(), vec![p1, p2]).expect("merge");
        match merged {
            QueryResult::TopN(ts) => {
                assert_eq!(ts.len(), 1);
                assert_eq!(ts[0].result.len(), 1, "threshold=1");
                let top = &ts[0].result[0];
                assert_eq!(top.get("region"), Some(&serde_json::json!("x")));
                assert_eq!(
                    top.get("uniq"),
                    Some(&serde_json::json!(15)),
                    "union envelope must collapse to the exact count and win the ranking"
                );
            }
            _ => panic!("expected topN"),
        }
    }

    /// A single partial never merges, so its exact per-shard count must
    /// pass through unchanged (regression guard for the fail-closed
    /// program: single-segment exact COUNT(DISTINCT) keeps working).
    #[test]
    fn merge_results_single_partial_cardinality_passes_through() {
        let p1 = ts_partial(&[("uniq", serde_json::json!(42))]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"cardinality","name":"uniq","fields":["region"]}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![p1]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts[0].result.get("uniq"), Some(&serde_json::json!(42)));
            }
            _ => panic!("expected timeseries"),
        }
    }

    // -------------------------------------------------------------------
    // Codex HIGH findings (2026-07-12): CardinalityState envelopes are
    // UNTRUSTED peer input — forged or malformed envelopes must fail
    // closed, never finalize as a wrong exact count or drop a shard.
    // -------------------------------------------------------------------

    /// Finding 4: a forged `values=["s:x"], count=1_000_000,
    /// saturated=false` envelope must NOT finalize as an exact 1,000,000 —
    /// the peer-supplied count is validated against the actual value set
    /// and the query fails closed on mismatch.  Exercises the
    /// single-partial pass-through, the path that reads the envelope's
    /// count directly.
    #[test]
    fn merge_results_rejects_forged_unsaturated_envelope_count() {
        let p = ts_partial(&[(
            "uniq",
            serde_json::json!({
                "@type": "cardinality_state",
                "values": ["s:x"],
                "saturated": false,
                "count": 1_000_000u64
            }),
        )]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"cardinality","name":"uniq","fields":["region"]}
                ]
            }"#,
        )
        .expect("parse");

        let err = Broker::merge_results(&query, vec![p])
            .expect_err("forged envelope count must fail closed, not finalize as 1,000,000");
        match err {
            ferrodruid_common::error::DruidError::ResourceLimit { kind, .. } => {
                assert!(
                    kind.contains("cardinality"),
                    "kind must name the cardinality guard, got: {kind}"
                );
            }
            other => panic!("expected ResourceLimit, got {other:?}"),
        }
    }

    /// Finding 1: a partial TAGGED as a `CardinalityState` envelope that is
    /// malformed must fail the merge closed — the pre-fix code treated it
    /// as a bare 0 and silently dropped the shard, returning the other
    /// shard's count as if it were the complete exact answer.
    #[test]
    fn merge_results_tagged_malformed_envelope_fails_closed_not_dropped() {
        use ferrodruid_aggregator::{Aggregator, CardinalityAggregator};

        let mut a = CardinalityAggregator::new(false);
        for v in 0..4u64 {
            a.aggregate(Some(&serde_json::json!(v)));
        }
        let p1 = ts_partial(&[("uniq", a.into_state().to_json())]);
        // Second shard's envelope is tagged but malformed (`values` is not
        // an array) — e.g. a corrupt or hostile peer.
        let p2 = ts_partial(&[(
            "uniq",
            serde_json::json!({
                "@type": "cardinality_state",
                "values": 17,
                "saturated": false,
                "count": 6
            }),
        )]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"cardinality","name":"uniq","fields":["region"]}
                ]
            }"#,
        )
        .expect("parse");

        let err = Broker::merge_results(&query, vec![p1, p2]).expect_err(
            "a tagged-but-malformed peer state must fail the query closed, \
             not be dropped as an empty shard",
        );
        match err {
            ferrodruid_common::error::DruidError::ResourceLimit { kind, .. } => {
                assert!(
                    kind.contains("cardinality"),
                    "kind must name the cardinality guard, got: {kind}"
                );
            }
            other => panic!("expected ResourceLimit, got {other:?}"),
        }
    }

    // -------------------------------------------------------------------
    // Wave 40-B (Wave 39 [High] [NEW-VARIANT] aggregator/lib.rs:88-129):
    // when a per-shard partial ships a typed `CardinalityState` envelope,
    // the broker must run a true HashSet union rather than saturating-add.
    // -------------------------------------------------------------------

    /// Two shards each report 10 keys with 5 overlapping. The pre-W40-B
    /// merge produced 20 (saturating-add). The W40-B typed-state path
    /// must produce 15 (true union).
    #[test]
    fn broker_cardinality_merges_via_typed_state_when_below_cap() {
        use ferrodruid_aggregator::{Aggregator, CardinalityAggregator};

        let mut a = CardinalityAggregator::new(false);
        for v in 0..10u64 {
            a.aggregate(Some(&serde_json::json!(v)));
        }
        let mut b = CardinalityAggregator::new(false);
        for v in 5..15u64 {
            b.aggregate(Some(&serde_json::json!(v)));
        }

        let p1 = ts_partial(&[("uniq", a.into_state().to_json())]);
        let p2 = ts_partial(&[("uniq", b.into_state().to_json())]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"cardinality","name":"uniq","fields":["region"]}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![p1, p2]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts.len(), 1);
                let v = ts[0].result.get("uniq").expect("uniq");
                // Fail-closed program (2026-07-11): the broker-internal
                // `CardinalityState` envelope must never leak to clients —
                // a non-saturated union collapses to its exact bare count
                // at finalization.
                assert_eq!(
                    v,
                    &serde_json::json!(15),
                    "typed state union must collapse to the exact bare count \
                     15 (10 ∪ 10 with 5 overlap), not 20 and not an envelope"
                );
            }
            _ => panic!("expected timeseries"),
        }
    }

    /// When either side's aggregator saturated (its exact set refused an
    /// insert at the per-aggregator cap, so the envelope ships no keys),
    /// the union degrades to a saturating-add — an over-counting upper
    /// bound — and finalization must FAIL CLOSED instead of returning the
    /// inexact count (fail-closed program, 2026-07-11; this is the
    /// cross-shard saturation unit test requested by the program spec).
    /// The saturated side uses a per-instance test cap so the process-wide
    /// override is not touched (it would race parallel tests).
    #[test]
    fn broker_cardinality_saturated_state_merge_fails_closed() {
        use ferrodruid_aggregator::{Aggregator, CardinalityAggregator};

        // Side A is exact with 5 keys.
        let mut a = CardinalityAggregator::new(false);
        for v in 0..5u64 {
            a.aggregate(Some(&serde_json::json!(format!("a{v}"))));
        }
        // Side B saturates its (test-lowered, per-instance) exact-set cap
        // of 8 by seeing 10 distinct keys -> saturated envelope, no keys.
        let mut b = CardinalityAggregator::with_cap_for_tests(false, Vec::new(), 8);
        for v in 0..10u64 {
            b.aggregate(Some(&serde_json::json!(format!("b{v}"))));
        }
        let s_b = b.into_state();
        assert!(s_b.saturated, "test setup: B must saturate");
        assert_eq!(s_b.count, 8, "test setup: B's count is its capped set size");

        let p1 = ts_partial(&[("uniq", a.into_state().to_json())]);
        let p2 = ts_partial(&[("uniq", s_b.to_json())]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"cardinality","name":"uniq","fields":["region"]}
                ]
            }"#,
        )
        .expect("parse");

        let err = Broker::merge_results(&query, vec![p1, p2])
            .expect_err("saturated cross-shard cardinality union must fail closed");
        match err {
            ferrodruid_common::error::DruidError::ResourceLimit {
                kind,
                limit,
                observed,
            } => {
                assert!(
                    kind.contains("cardinality.crossShardExactMerge"),
                    "kind must name the cross-shard merge limit, got: {kind}"
                );
                assert_eq!(
                    limit,
                    ferrodruid_aggregator::exact_cardinality_set_cap(),
                    "limit must be the exact-set cap"
                );
                // Observed carries the (inexact) saturating-add upper bound.
                assert_eq!(observed, 5 + 8);
            }
            other => panic!("expected ResourceLimit, got {other:?}"),
        }
    }

    /// Multi-shard exact union (2026-07-12): a single partial that ships an
    /// envelope (the executors' standard partial form now) must finalize to
    /// its exact bare count through the single-partial pass-through — a
    /// single segment with more distinct values than the former 1,000-key
    /// wire cap must NOT fail closed.
    #[test]
    fn merge_results_single_partial_envelope_collapses_to_exact_count() {
        use ferrodruid_aggregator::{Aggregator, CardinalityAggregator};

        let mut a = CardinalityAggregator::new(false);
        for v in 0..5_000u64 {
            a.aggregate(Some(&serde_json::json!(v)));
        }
        let p1 = ts_partial(&[("uniq", a.into_state().to_json())]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"cardinality","name":"uniq","fields":["region"]}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![p1]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => {
                assert_eq!(
                    ts[0].result.get("uniq"),
                    Some(&serde_json::json!(5_000)),
                    "lone envelope must collapse to its exact count (5,000 > \
                     the former 1,000-key wire cap; no fail-closed, no leak)"
                );
            }
            _ => panic!("expected timeseries"),
        }
    }

    /// Multi-shard exact union (2026-07-12): a post-aggregation referencing
    /// an exact-cardinality output must be recomputed from the exact union
    /// count after envelope collapse — not left at the first shard's stale
    /// per-segment value.
    #[test]
    fn merged_post_agg_over_cardinality_recomputed_from_union_count() {
        use ferrodruid_aggregator::{Aggregator, CardinalityAggregator};

        let mut a = CardinalityAggregator::new(false);
        for v in 0..10u64 {
            a.aggregate(Some(&serde_json::json!(v)));
        }
        let mut b = CardinalityAggregator::new(false);
        for v in 5..15u64 {
            b.aggregate(Some(&serde_json::json!(v)));
        }

        // Each shard computed uniq_plus_one from ITS per-segment count
        // (10 + 1 = 11) — stale once the union count (15) is known.
        let p1 = ts_partial(&[
            ("uniq", a.into_state().to_json()),
            ("uniq_plus_one", serde_json::json!(11.0)),
        ]);
        let p2 = ts_partial(&[
            ("uniq", b.into_state().to_json()),
            ("uniq_plus_one", serde_json::json!(11.0)),
        ]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"cardinality","name":"uniq","fields":["region"]}
                ],
                "postAggregations": [
                    {"type":"arithmetic","name":"uniq_plus_one","fn":"+","fields":[
                        {"type":"fieldAccess","name":"u","fieldName":"uniq"},
                        {"type":"constant","name":"one","value":1}
                    ]}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![p1, p2]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts[0].result.get("uniq"), Some(&serde_json::json!(15)));
                assert_eq!(
                    ts[0].result.get("uniq_plus_one"),
                    Some(&serde_json::json!(16.0)),
                    "post-agg must be recomputed from the union count 15, \
                     not left at the per-shard 11"
                );
            }
            _ => panic!("expected timeseries"),
        }
    }

    #[test]
    fn merge_agg_maps_mixed_aggs_dispatch_correctly() {
        // Critical regression: a single response must dispatch each field
        // independently by its declared aggregator kind.
        let mut m1 = serde_json::Map::new();
        m1.insert("cnt".to_string(), serde_json::json!(5));
        m1.insert("max_p".to_string(), serde_json::json!(99));
        m1.insert("min_p".to_string(), serde_json::json!(10));
        m1.insert("total".to_string(), serde_json::json!(123));

        let mut m2 = serde_json::Map::new();
        m2.insert("cnt".to_string(), serde_json::json!(3));
        m2.insert("max_p".to_string(), serde_json::json!(50));
        m2.insert("min_p".to_string(), serde_json::json!(2));
        m2.insert("total".to_string(), serde_json::json!(77));

        let p1 = QueryResult::Timeseries(vec![TimeseriesResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: m1,
        }]);
        let p2 = QueryResult::Timeseries(vec![TimeseriesResult {
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            result: m2,
        }]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "timeseries",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "aggregations": [
                    {"type":"count","name":"cnt"},
                    {"type":"longMax","name":"max_p","fieldName":"price"},
                    {"type":"longMin","name":"min_p","fieldName":"price"},
                    {"type":"longSum","name":"total","fieldName":"value"}
                ]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![p1, p2]).expect("merge");
        match merged {
            QueryResult::Timeseries(ts) => {
                assert_eq!(ts.len(), 1);
                let r = &ts[0].result;
                assert_eq!(r.get("cnt"), Some(&serde_json::json!(8)), "count sums");
                assert_eq!(
                    r.get("max_p"),
                    Some(&serde_json::json!(99)),
                    "max picks max"
                );
                assert_eq!(r.get("min_p"), Some(&serde_json::json!(2)), "min picks min");
                assert_eq!(r.get("total"), Some(&serde_json::json!(200)), "sum sums");
            }
            _ => panic!("expected timeseries"),
        }
    }

    // -------------------------------------------------------------------
    // Wave 36-G2: Wave 37B High `broker/lib.rs:486-503`. `group_key` must
    // include non-string grouping dimensions (LONG/DOUBLE/BOOL) and treat
    // typed values distinctly.
    // -------------------------------------------------------------------

    #[test]
    fn merge_groupby_key_includes_long_dimensions() {
        // Two shards each emit a row with a LONG grouping dimension
        // (`shard_id`). Under the buggy v.is_string()-only filter, both
        // rows collapsed under the same key (no string dim), and counts
        // merged together. The fix builds the key from the declared
        // dimension list with typed encoding, so two distinct shard_id
        // values produce two separate output rows.
        let mk = |shard_id: i64, cnt: i64| GroupByResult {
            version: "v1".to_string(),
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            event: {
                let mut m = serde_json::Map::new();
                m.insert("shard_id".to_string(), serde_json::json!(shard_id));
                m.insert("cnt".to_string(), serde_json::json!(cnt));
                m
            },
        };

        let g1 = QueryResult::GroupBy(vec![mk(1, 5), mk(2, 7)]);
        let g2 = QueryResult::GroupBy(vec![mk(1, 3), mk(2, 11)]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "groupBy",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": [{"type":"default","dimension":"shard_id","output_name":"shard_id","output_type":"LONG"}],
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![g1, g2]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                assert_eq!(
                    results.len(),
                    2,
                    "two distinct LONG group keys must produce two output rows (was 1 under buggy is_string-only filter)"
                );
                let mut by_id: HashMap<i64, i64> = HashMap::new();
                for r in &results {
                    let id = r
                        .event
                        .get("shard_id")
                        .and_then(|v| v.as_i64())
                        .expect("shard_id present");
                    let cnt = r
                        .event
                        .get("cnt")
                        .and_then(|v| v.as_i64())
                        .expect("cnt present");
                    by_id.insert(id, cnt);
                }
                assert_eq!(by_id.get(&1).copied(), Some(8), "shard_id=1 -> 5+3 = 8");
                assert_eq!(by_id.get(&2).copied(), Some(18), "shard_id=2 -> 7+11 = 18");
            }
            _ => panic!("expected groupBy"),
        }
    }

    #[test]
    fn merge_groupby_key_distinguishes_typed_dimension_values() {
        // The string "1" and the number 1 under the same dimension name
        // must NOT collide.
        let s_one = GroupByResult {
            version: "v1".to_string(),
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            event: {
                let mut m = serde_json::Map::new();
                m.insert("dim".to_string(), serde_json::json!("1"));
                m.insert("cnt".to_string(), serde_json::json!(10));
                m
            },
        };
        let n_one = GroupByResult {
            version: "v1".to_string(),
            timestamp: "2024-01-01T00:00:00.000Z".to_string(),
            event: {
                let mut m = serde_json::Map::new();
                m.insert("dim".to_string(), serde_json::json!(1));
                m.insert("cnt".to_string(), serde_json::json!(20));
                m
            },
        };

        let g1 = QueryResult::GroupBy(vec![s_one]);
        let g2 = QueryResult::GroupBy(vec![n_one]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "groupBy",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": [{"type":"default","dimension":"dim","output_name":"dim","output_type":"STRING"}],
                "aggregations": [{"type":"count","name":"cnt"}]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![g1, g2]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                assert_eq!(
                    results.len(),
                    2,
                    "string '1' and number 1 must produce distinct group keys"
                );
            }
            _ => panic!("expected groupBy"),
        }
    }

    // -------------------------------------------------------------------
    // Group-key collision hardening (2026-07-21 High): the pre-fix
    // `group_key` joined `<name>=<enc>` parts with an unescaped `|`
    // separator, so dimension values containing the separator or the
    // `<name>=<type>:` framing bytes forged element boundaries and
    // DISTINCT value tuples merged into one group (cardinality
    // undercount, dropped groups, wrong ORDER BY … LIMIT retention,
    // skewed broker-wide cap). The key is now a length-prefixed frame
    // sequence: byte-equal IFF element-wise equal.
    // -------------------------------------------------------------------

    fn multi_dim_row(dims: &[(&str, &str)], cnt: i64) -> GroupByResult {
        GroupByResult {
            version: "v1".to_string(),
            timestamp: "1970-01-01T00:00:00.000Z".to_string(),
            event: {
                let mut m = serde_json::Map::new();
                for (name, value) in dims {
                    m.insert((*name).to_string(), serde_json::json!(value));
                }
                m.insert("cnt".to_string(), serde_json::json!(cnt));
                m
            },
        }
    }

    fn multi_dim_query(dim_names: &[&str], limit_spec: Option<&str>) -> DruidQuery {
        let dims = dim_names
            .iter()
            .map(|d| {
                format!(
                    r#"{{"type":"default","dimension":"{d}","output_name":"{d}","output_type":"STRING"}}"#
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let limit = limit_spec.map_or_else(String::new, |s| format!(r#","limitSpec": {s}"#));
        serde_json::from_str(&format!(
            r#"{{
                "queryType": "groupBy",
                "dataSource": {{"type":"table","name":"wiki"}},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-05T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": [{dims}],
                "aggregations": [{{"type":"count","name":"cnt"}}]{limit}
            }}"#
        ))
        .expect("parse")
    }

    fn dims_and_counts(results: &[GroupByResult], dim_names: &[&str]) -> Vec<(Vec<String>, i64)> {
        results
            .iter()
            .map(|r| {
                let dims = dim_names
                    .iter()
                    .map(|d| {
                        r.event
                            .get(*d)
                            .and_then(|v| v.as_str())
                            .expect("string dim present")
                            .to_string()
                    })
                    .collect();
                let cnt = r
                    .event
                    .get("cnt")
                    .and_then(serde_json::Value::as_i64)
                    .expect("cnt present");
                (dims, cnt)
            })
            .collect()
    }

    /// RED→GREEN core: with dimensions `d1,d2`, the tuples
    /// `("x|d2=s:y", "z")` and `("x", "y|d2=s:z")` produced the SAME
    /// pre-fix concatenated key (`ts=…|d1=s:x|d2=s:y|d2=s:z`), so two
    /// DISTINCT groups silently merged into one (count 8) and the
    /// cardinality undercounted. They must stay separate, each keeping
    /// its own count.
    #[test]
    fn merge_groupby_forged_separator_tuples_stay_distinct() {
        let g1 = QueryResult::GroupBy(vec![multi_dim_row(&[("d1", "x|d2=s:y"), ("d2", "z")], 3)]);
        let g2 = QueryResult::GroupBy(vec![multi_dim_row(&[("d1", "x"), ("d2", "y|d2=s:z")], 5)]);

        let query = multi_dim_query(&["d1", "d2"], None);
        let merged = Broker::merge_results(&query, vec![g1, g2]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                assert_eq!(
                    dims_and_counts(&results, &["d1", "d2"]),
                    vec![
                        (vec!["x|d2=s:y".to_string(), "z".to_string()], 3),
                        (vec!["x".to_string(), "y|d2=s:z".to_string()], 5),
                    ],
                    "distinct tuples whose values contain the separator/framing \
                     bytes must stay two groups with their own counts (pre-fix: \
                     one merged group with cnt 8)"
                );
            }
            _ => panic!("expected groupBy"),
        }
    }

    /// Element-boundary ambiguity in the MIDDLE of a 3-dim tuple: which
    /// dimension owns `"z"`? `("x", "y|d3=s:z", "w")` and
    /// `("x", "y", "z|d3=s:w")` concatenated to the same pre-fix key.
    /// Length-prefixed frames make the boundary unforgeable.
    #[test]
    fn merge_groupby_ambiguous_element_boundaries_stay_distinct() {
        let g1 = QueryResult::GroupBy(vec![multi_dim_row(
            &[("d1", "x"), ("d2", "y|d3=s:z"), ("d3", "w")],
            2,
        )]);
        let g2 = QueryResult::GroupBy(vec![multi_dim_row(
            &[("d1", "x"), ("d2", "y"), ("d3", "z|d3=s:w")],
            9,
        )]);

        let query = multi_dim_query(&["d1", "d2", "d3"], None);
        let merged = Broker::merge_results(&query, vec![g1, g2]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                assert_eq!(
                    dims_and_counts(&results, &["d1", "d2", "d3"]),
                    vec![
                        (
                            vec!["x".to_string(), "y|d3=s:z".to_string(), "w".to_string()],
                            2
                        ),
                        (
                            vec!["x".to_string(), "y".to_string(), "z|d3=s:w".to_string()],
                            9
                        ),
                    ],
                    "shifting a value across the ambiguous element boundary must \
                     not produce the same group key"
                );
            }
            _ => panic!("expected groupBy"),
        }
    }

    /// ORDER BY … LIMIT must rank the TRUE top group: pre-fix, the two
    /// colliding tuples (cnt 3 each) merged into a phantom cnt-6 group
    /// that outranked the genuine cnt-5 top group, so `LIMIT 1` returned
    /// a group that does not exist and dropped the real winner.
    #[test]
    fn merge_groupby_forged_separator_keeps_order_by_limit_ranking() {
        let g1 = QueryResult::GroupBy(vec![
            multi_dim_row(&[("d1", "x|d2=s:y"), ("d2", "z")], 3),
            multi_dim_row(&[("d1", "t"), ("d2", "t")], 5),
        ]);
        let g2 = QueryResult::GroupBy(vec![multi_dim_row(&[("d1", "x"), ("d2", "y|d2=s:z")], 3)]);

        let query = multi_dim_query(
            &["d1", "d2"],
            Some(
                r#"{"type":"default","limit":1,
                    "columns":[{"dimension":"cnt","direction":"descending","dimensionOrder":"numeric"}]}"#,
            ),
        );
        let merged = Broker::merge_results(&query, vec![g1, g2]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                assert_eq!(
                    dims_and_counts(&results, &["d1", "d2"]),
                    vec![(vec!["t".to_string(), "t".to_string()], 5)],
                    "the genuine cnt-5 group must win LIMIT 1 (pre-fix: a \
                     phantom merged cnt-6 group outranked it)"
                );
            }
            _ => panic!("expected groupBy"),
        }
    }

    /// The broker-wide cap counts DISTINCT groups — two tuples that
    /// collided pre-fix are two distinct groups and must trip a cap of
    /// 1 loudly (pre-fix they silently counted as ONE key, skewing the
    /// cap accounting downward).
    #[test]
    fn merge_groupby_cap_counts_forged_separator_groups_as_distinct() {
        let g1 = QueryResult::GroupBy(vec![multi_dim_row(&[("d1", "x|d2=s:y"), ("d2", "z")], 3)]);
        let g2 = QueryResult::GroupBy(vec![multi_dim_row(&[("d1", "x"), ("d2", "y|d2=s:z")], 5)]);

        let query = multi_dim_query(&["d1", "d2"], None);
        let q = parsed_groupby(&query);
        let err = merge_groupby_capped(vec![g1, g2], &q.dimensions, &q.aggregations, 1)
            .expect_err("two distinct (pre-fix colliding) groups must trip cap 1");
        match err {
            DruidError::ResourceLimit {
                kind,
                limit,
                observed,
            } => {
                assert_eq!(kind, "groupBy.maxMergedResults");
                assert_eq!(limit, 1);
                assert_eq!(observed, 2, "cap must see 2 distinct groups, not 1");
            }
            other => panic!("expected ResourceLimit, got {other:?}"),
        }
    }

    /// Determinism guard for the new encoding: IDENTICAL tuples whose
    /// values contain a literal `|` and length-prefix-looking framing
    /// bytes (`3:s:c`) must still produce identical keys across shards
    /// and merge into one group with summed counts.
    #[test]
    fn merge_groupby_identical_tuples_with_framing_bytes_still_merge() {
        let g1 = QueryResult::GroupBy(vec![multi_dim_row(&[("d1", "a|b"), ("d2", "3:s:c")], 3)]);
        let g2 = QueryResult::GroupBy(vec![multi_dim_row(&[("d1", "a|b"), ("d2", "3:s:c")], 4)]);

        let query = multi_dim_query(&["d1", "d2"], None);
        let merged = Broker::merge_results(&query, vec![g1, g2]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                assert_eq!(
                    dims_and_counts(&results, &["d1", "d2"]),
                    vec![(vec!["a|b".to_string(), "3:s:c".to_string()], 7)],
                    "genuinely equal tuples must keep merging (3 + 4 = 7)"
                );
            }
            _ => panic!("expected groupBy"),
        }
    }

    // -------------------------------------------------------------------
    // Wave 45-A: Wave 37B broker_tail Mediums #1 + #3 — `merge_scan`
    // -------------------------------------------------------------------

    /// Wave 45-A regression for Wave 37B broker_tail Medium #3:
    /// `merge_scan` must union the schema across all partials, not pick
    /// it from the first shard.  Pre-W45A, columns reported only by
    /// later shards were dropped from `ScanResult.columns` even though
    /// their values still appeared in `events`.
    #[test]
    fn merge_scan_unions_schema_across_partials() {
        let s1 = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["a".to_string(), "b".to_string()],
            events: vec![HashMap::from([
                ("a".to_string(), serde_json::json!(1)),
                ("b".to_string(), serde_json::json!(2)),
            ])],
        });
        let s2 = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["a".to_string(), "c".to_string()],
            events: vec![HashMap::from([
                ("a".to_string(), serde_json::json!(3)),
                ("c".to_string(), serde_json::json!(4)),
            ])],
        });

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "scan",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"]
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![s1, s2]).expect("merge");
        match merged {
            QueryResult::Scan(scan) => {
                // Pre-W45A: `columns == ["a","b"]` (first shard only),
                // so `c` was unrepresented in metadata even though `c=4`
                // appeared in events.
                assert!(
                    scan.columns.contains(&"a".to_string()),
                    "schema must contain 'a'"
                );
                assert!(
                    scan.columns.contains(&"b".to_string()),
                    "schema must contain 'b'"
                );
                assert!(
                    scan.columns.contains(&"c".to_string()),
                    "schema must union 'c' from later shard (Wave 45-A)"
                );
                assert_eq!(scan.columns.len(), 3, "no duplicate columns");
            }
            _ => panic!("expected scan"),
        }
    }

    /// Wave 45-A regression for Wave 37B broker_tail Medium #1:
    /// `merge_scan` must stop appending events once `limit` is reached
    /// rather than collecting every shard's full event list before
    /// truncating.
    ///
    /// We cannot directly observe the early break, but we can prove
    /// that the truncation result is correct *and* the columns metadata
    /// from a never-visited later shard is still preserved by the
    /// schema-union path.  (This is the worst-case semantic surface
    /// pre-W45A would have hidden.)
    #[test]
    fn merge_scan_early_terminates_under_limit() {
        let s1 = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["a".to_string()],
            events: (0..1000_i64)
                .map(|i| HashMap::from([("a".to_string(), serde_json::json!(i))]))
                .collect(),
        });
        let s2 = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["a".to_string()],
            events: (1000..2000_i64)
                .map(|i| HashMap::from([("a".to_string(), serde_json::json!(i))]))
                .collect(),
        });

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "scan",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "limit": 5
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![s1, s2]).expect("merge");
        match merged {
            QueryResult::Scan(scan) => {
                assert_eq!(scan.events.len(), 5, "limit=5 must truncate (Wave 45-A)");
                let codes: Vec<i64> = scan
                    .events
                    .iter()
                    .filter_map(|m| m.get("a").and_then(|v| v.as_i64()))
                    .collect();
                assert_eq!(codes, vec![0_i64, 1_i64, 2_i64, 3_i64, 4_i64]);
            }
            _ => panic!("expected scan"),
        }
    }

    // -------------------------------------------------------------------
    // Wave 45-A: Wave 37B broker_tail Medium #2 — `merge_search` dedup
    // -------------------------------------------------------------------

    /// Wave 45-A regression for Wave 37B broker_tail Medium #2: the
    /// dedup map is now `O(1)` per hit so a CPU-amplification vector
    /// against the broker is closed.  Behaviour must be identical to
    /// the prior `O(n²)` implementation: across-shard dups sum,
    /// ordering remains stable per `value`.
    #[test]
    fn merge_search_dedup_uses_hash_map() {
        // 4 shards, each emits the same 3 (dim, value) hits with count 1.
        // Pre-W45A the linear scan would still produce the correct sum
        // (4) but in `O(shards * hits_per_shard²)` time; this test
        // sanity-checks the new HashMap-based path returns the same sums
        // as before.
        let mk = |c: u64| {
            QueryResult::Search(vec![SearchResult {
                timestamp: "2024-01-01T00:00:00.000Z".to_string(),
                result: vec![
                    SearchHit {
                        dimension: "region".to_string(),
                        value: "us".to_string(),
                        count: c,
                    },
                    SearchHit {
                        dimension: "region".to_string(),
                        value: "eu".to_string(),
                        count: c,
                    },
                    SearchHit {
                        dimension: "region".to_string(),
                        value: "jp".to_string(),
                        count: c,
                    },
                ],
            }])
        };

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "search",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "query": {"type":"contains","value":""},
                "searchDimensions": ["region"]
            }"#,
        )
        .expect("parse");

        let merged =
            Broker::merge_results(&query, vec![mk(1), mk(1), mk(1), mk(1)]).expect("merge");
        match merged {
            QueryResult::Search(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].result.len(), 3, "3 distinct values, deduped");
                for h in &results[0].result {
                    assert_eq!(
                        h.count, 4,
                        "each (region, {}) must sum across 4 shards (Wave 45-A)",
                        h.value
                    );
                }
                // Sort stability: alphabetically eu, jp, us.
                let order: Vec<&str> = results[0].result.iter().map(|h| h.value.as_str()).collect();
                assert_eq!(order, vec!["eu", "jp", "us"]);
            }
            _ => panic!("expected search"),
        }
    }

    // -------------------------------------------------------------------
    // W-C S3: broker merge must re-apply the query ordering BEFORE limit
    // (multi-segment scan `ORDER BY __time` + groupBy limitSpec ordering)
    // -------------------------------------------------------------------

    fn iso_millis(s: &str) -> i64 {
        chrono::DateTime::parse_from_rfc3339(s)
            .expect("parse")
            .timestamp_millis()
    }

    fn scan_event(time_millis: i64, a: i64) -> HashMap<String, serde_json::Value> {
        HashMap::from([
            ("__time".to_string(), serde_json::json!(time_millis)),
            ("a".to_string(), serde_json::json!(a)),
        ])
    }

    fn time_ordered_scan_query(order: &str, limit: Option<usize>) -> DruidQuery {
        let limit_json = limit.map_or("null".to_string(), |l| l.to_string());
        serde_json::from_str(&format!(
            r#"{{
                "queryType": "scan",
                "dataSource": {{"type":"table","name":"wiki"}},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-05T00:00:00.000Z"],
                "order": "{order}",
                "limit": {limit_json}
            }}"#
        ))
        .expect("parse")
    }

    /// W-C S3 (proving-run High): `SELECT … ORDER BY __time ASC LIMIT k`
    /// on a multi-segment datasource must return the k EARLIEST rows in
    /// `__time` order regardless of shard-arrival order.  Pre-fix,
    /// `merge_scan` concatenated shards in arrival order and truncated,
    /// so an early-arriving late-time shard returned the WRONG rows
    /// (Druid oracle: the 01-01 pair; FerroDruid returned the 01-02 pair).
    #[test]
    fn merge_scan_ascending_resorts_across_shards_before_limit() {
        let day1 = iso_millis("2024-01-01T00:00:00Z");
        let day2 = iso_millis("2024-01-02T00:00:00Z");

        // Deliberately WRONG arrival order: the later-time shard first.
        let s_late = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["__time".to_string(), "a".to_string()],
            events: vec![scan_event(day2, 3), scan_event(day2 + 1, 4)],
        });
        let s_early = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["__time".to_string(), "a".to_string()],
            events: vec![scan_event(day1, 1), scan_event(day1 + 1, 2)],
        });

        let query = time_ordered_scan_query("ascending", Some(2));
        let merged = Broker::merge_results(&query, vec![s_late, s_early]).expect("merge");
        match merged {
            QueryResult::Scan(scan) => {
                let a: Vec<i64> = scan
                    .events
                    .iter()
                    .filter_map(|m| m.get("a").and_then(serde_json::Value::as_i64))
                    .collect();
                assert_eq!(
                    a,
                    vec![1, 2],
                    "ORDER BY __time ASC LIMIT 2 must return the 2 earliest rows in time order"
                );
            }
            _ => panic!("expected scan"),
        }
    }

    /// W-C S3: the DESC counterpart — the k LATEST rows, latest first.
    #[test]
    fn merge_scan_descending_resorts_across_shards_before_limit() {
        let day1 = iso_millis("2024-01-01T00:00:00Z");
        let day2 = iso_millis("2024-01-02T00:00:00Z");

        // WRONG arrival order for DESC: the earlier-time shard first
        // (each shard is internally DESC-sorted, as the executor emits).
        let s_early = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["__time".to_string(), "a".to_string()],
            events: vec![scan_event(day1 + 1, 2), scan_event(day1, 1)],
        });
        let s_late = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["__time".to_string(), "a".to_string()],
            events: vec![scan_event(day2 + 1, 4), scan_event(day2, 3)],
        });

        let query = time_ordered_scan_query("descending", Some(2));
        let merged = Broker::merge_results(&query, vec![s_early, s_late]).expect("merge");
        match merged {
            QueryResult::Scan(scan) => {
                let a: Vec<i64> = scan
                    .events
                    .iter()
                    .filter_map(|m| m.get("a").and_then(serde_json::Value::as_i64))
                    .collect();
                assert_eq!(
                    a,
                    vec![4, 3],
                    "ORDER BY __time DESC LIMIT 2 must return the 2 latest rows, latest first"
                );
            }
            _ => panic!("expected scan"),
        }
    }

    /// W-C S3: without a LIMIT the row SET was already right but the
    /// ORDER was wrong — the merge must emit global `__time` order.
    #[test]
    fn merge_scan_ascending_without_limit_returns_global_time_order() {
        let day1 = iso_millis("2024-01-01T00:00:00Z");
        let day2 = iso_millis("2024-01-02T00:00:00Z");

        let s_late = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["__time".to_string(), "a".to_string()],
            events: vec![scan_event(day2, 3), scan_event(day2 + 1, 4)],
        });
        let s_early = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["__time".to_string(), "a".to_string()],
            events: vec![scan_event(day1, 1), scan_event(day1 + 1, 2)],
        });

        let query = time_ordered_scan_query("ascending", None);
        let merged = Broker::merge_results(&query, vec![s_late, s_early]).expect("merge");
        match merged {
            QueryResult::Scan(scan) => {
                let a: Vec<i64> = scan
                    .events
                    .iter()
                    .filter_map(|m| m.get("a").and_then(serde_json::Value::as_i64))
                    .collect();
                assert_eq!(a, vec![1, 2, 3, 4], "global ascending __time order");
            }
            _ => panic!("expected scan"),
        }
    }

    /// W-C S3 guard: a scan WITHOUT a time order (`"none"`) keeps the
    /// existing concatenation + early-terminate behavior (Druid scan
    /// without order is unordered), so the fast path is preserved
    /// exactly where it is still correct.
    #[test]
    fn merge_scan_unordered_keeps_arrival_concatenation() {
        let day1 = iso_millis("2024-01-01T00:00:00Z");
        let day2 = iso_millis("2024-01-02T00:00:00Z");

        let s_late = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["__time".to_string(), "a".to_string()],
            events: vec![scan_event(day2, 3), scan_event(day2 + 1, 4)],
        });
        let s_early = QueryResult::Scan(ScanResult {
            segment_id: None,
            columns: vec!["__time".to_string(), "a".to_string()],
            events: vec![scan_event(day1, 1), scan_event(day1 + 1, 2)],
        });

        let query = time_ordered_scan_query("none", Some(3));
        let merged = Broker::merge_results(&query, vec![s_late, s_early]).expect("merge");
        match merged {
            QueryResult::Scan(scan) => {
                let a: Vec<i64> = scan
                    .events
                    .iter()
                    .filter_map(|m| m.get("a").and_then(serde_json::Value::as_i64))
                    .collect();
                assert_eq!(a, vec![3, 4, 1], "unordered scan keeps arrival order");
            }
            _ => panic!("expected scan"),
        }
    }

    fn groupby_row(region: serde_json::Value, cnt: i64) -> GroupByResult {
        GroupByResult {
            version: "v1".to_string(),
            timestamp: "1970-01-01T00:00:00.000Z".to_string(),
            event: {
                let mut m = serde_json::Map::new();
                m.insert("region".to_string(), region);
                m.insert("cnt".to_string(), serde_json::json!(cnt));
                m
            },
        }
    }

    fn groupby_limit_query(limit_spec: &str) -> DruidQuery {
        serde_json::from_str(&format!(
            r#"{{
                "queryType": "groupBy",
                "dataSource": {{"type":"table","name":"wiki"}},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-05T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": [{{"type":"default","dimension":"region","output_name":"region","output_type":"STRING"}}],
                "aggregations": [{{"type":"count","name":"cnt"}}],
                "limitSpec": {limit_spec}
            }}"#
        ))
        .expect("parse")
    }

    /// W-C S3 (groupBy side): the broker merged group VALUES correctly
    /// but returned groups in shard-arrival order and never applied the
    /// limitSpec — so `ORDER BY <metric> DESC LIMIT k` could return the
    /// wrong k groups.  The merge must re-sort by the limitSpec ordering
    /// and truncate to the limit, exactly like the per-segment path.
    #[test]
    fn merge_groupby_resorts_by_limit_spec_before_limit() {
        // Arrival order puts the LOSING group first: us = 3 + 2 = 5,
        // eu = 10.  ORDER BY cnt DESC LIMIT 1 must return eu(10).
        let g1 = QueryResult::GroupBy(vec![groupby_row(serde_json::json!("us"), 3)]);
        let g2 = QueryResult::GroupBy(vec![
            groupby_row(serde_json::json!("eu"), 10),
            groupby_row(serde_json::json!("us"), 2),
        ]);

        let query = groupby_limit_query(
            r#"{"type":"default","limit":1,
                "columns":[{"dimension":"cnt","direction":"descending","dimensionOrder":"numeric"}]}"#,
        );
        let merged = Broker::merge_results(&query, vec![g1, g2]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                assert_eq!(
                    results.len(),
                    1,
                    "limitSpec limit=1 must truncate the merge"
                );
                assert_eq!(
                    results[0].event.get("region"),
                    Some(&serde_json::json!("eu")),
                    "the globally-highest group must survive the LIMIT"
                );
                assert_eq!(results[0].event.get("cnt"), Some(&serde_json::json!(10)));
            }
            _ => panic!("expected groupBy"),
        }
    }

    /// W-C S3 + W-A regression: the merged-groups re-sort must rank a
    /// SKETCH aggregation cell by its partial-state envelope's
    /// `estimate` (the shared `numeric_agg_cell` resolution), exactly
    /// like the per-segment limitSpec path and the topN merge.
    #[test]
    fn merge_groupby_limit_spec_ranks_sketch_envelope_by_estimate() {
        let sketch_row = |region: &str, estimate: f64| GroupByResult {
            version: "v1".to_string(),
            timestamp: "1970-01-01T00:00:00.000Z".to_string(),
            event: {
                let mut m = serde_json::Map::new();
                m.insert("region".to_string(), serde_json::json!(region));
                m.insert(
                    "uniq".to_string(),
                    serde_json::json!({
                        "@sketch": {"type": "theta", "keys": []},
                        "estimate": estimate
                    }),
                );
                m
            },
        };
        // Arrival order puts the low-estimate group first.
        let g1 = QueryResult::GroupBy(vec![sketch_row("low", 3.0)]);
        let g2 = QueryResult::GroupBy(vec![sketch_row("high", 10.0)]);

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "groupBy",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-05T00:00:00.000Z"],
                "granularity": "all",
                "dimensions": [{"type":"default","dimension":"region","output_name":"region","output_type":"STRING"}],
                "aggregations": [{"type":"thetaSketch","name":"uniq","fieldName":"user"}],
                "limitSpec": {"type":"default","limit":1,
                    "columns":[{"dimension":"uniq","direction":"descending"}]}
            }"#,
        )
        .expect("parse");

        let merged = Broker::merge_results(&query, vec![g1, g2]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(
                    results[0].event.get("region"),
                    Some(&serde_json::json!("high")),
                    "sketch envelope must rank by its estimate (W-A), not as 0.0"
                );
            }
            _ => panic!("expected groupBy"),
        }
    }

    /// W-C S3 + W-B / W1-J regression: the merged-groups re-sort must
    /// keep Druid's NULLS FIRST (ASC) / NULLS LAST (DESC) placement for
    /// a canonical-null dimension group (the W-B legacy read renders
    /// `''` as canonical JSON null, so this is exactly the legacy-null
    /// ordering surface).
    #[test]
    fn merge_groupby_limit_spec_orders_nulls_first_in_asc() {
        // Arrival order puts the non-null group first.
        let g1 = QueryResult::GroupBy(vec![groupby_row(serde_json::json!("a"), 1)]);
        let g2 = QueryResult::GroupBy(vec![groupby_row(serde_json::Value::Null, 2)]);

        let asc = groupby_limit_query(
            r#"{"type":"default","columns":[{"dimension":"region","direction":"ascending"}]}"#,
        );
        let merged = Broker::merge_results(&asc, vec![g1.clone(), g2.clone()]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                let regions: Vec<Option<&str>> = results
                    .iter()
                    .map(|r| r.event.get("region").and_then(|v| v.as_str()))
                    .collect();
                assert_eq!(
                    regions,
                    vec![None, Some("a")],
                    "ASC must put the null group FIRST (Druid / Calcite default)"
                );
            }
            _ => panic!("expected groupBy"),
        }

        let desc = groupby_limit_query(
            r#"{"type":"default","columns":[{"dimension":"region","direction":"descending"}]}"#,
        );
        let merged = Broker::merge_results(&desc, vec![g2, g1]).expect("merge");
        match merged {
            QueryResult::GroupBy(results) => {
                let regions: Vec<Option<&str>> = results
                    .iter()
                    .map(|r| r.event.get("region").and_then(|v| v.as_str()))
                    .collect();
                assert_eq!(
                    regions,
                    vec![Some("a"), None],
                    "DESC must put the null group LAST"
                );
            }
            _ => panic!("expected groupBy"),
        }
    }

    // -------------------------------------------------------------------
    // W-C S3 end-to-end: multi-segment through Historical + execute_local
    // -------------------------------------------------------------------

    /// Build a single-day segment: every row at `day_iso` + `row_idx`
    /// millis, one `region` string dimension, one `value` double metric.
    fn build_day_segment(day_iso: &str, regions: &[&str]) -> SegmentData {
        let base = iso_millis(day_iso);
        let timestamps: Vec<i64> = (0..regions.len() as i64).map(|i| base + i).collect();

        let mut sorted: Vec<String> = regions.iter().map(|s| (*s).to_string()).collect();
        sorted.sort();
        sorted.dedup();
        let dict = FrontCodedDictionary::from_sorted(sorted.clone());
        let encoded_values: Vec<u32> = regions
            .iter()
            .map(|r| {
                u32::try_from(sorted.iter().position(|s| s == r).expect("in dict"))
                    .expect("dict idx")
            })
            .collect();
        let bitmap_indexes: Vec<DruidBitmap> = (0..sorted.len())
            .map(|dict_idx| {
                let mut bm = DruidBitmap::new();
                for (row, &enc) in encoded_values.iter().enumerate() {
                    if enc as usize == dict_idx {
                        bm.insert(u32::try_from(row).expect("row idx"));
                    }
                }
                bm
            })
            .collect();
        let region_col = ColumnData::String(StringColumnData {
            dictionary: dict,
            encoded_values,
            bitmap_indexes,
        });

        let value_col = ColumnData::Double((0..regions.len()).map(|i| i as f64).collect());

        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(timestamps.clone()));
        columns.insert("region".to_string(), region_col);
        columns.insert("value".to_string(), value_col);

        SegmentData {
            version: 9,
            num_rows: regions.len(),
            interval: Interval {
                start_millis: base,
                end_millis: base + regions.len() as i64,
            },
            dimensions: vec!["region".to_string()],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: false,
        }
    }

    /// W-C S3 RED→GREEN core: a multi-segment `ORDER BY __time ASC
    /// LIMIT 1` must return THE earliest row deterministically across
    /// runs.  Pre-fix the per-shard HashMap iteration order decided
    /// which segment's rows survived the LIMIT (restart variance
    /// [03,01,02] / [01,02,03] / … in the proving run); each loop
    /// iteration below uses a fresh Historical (fresh HashMap seed) to
    /// witness exactly that nondeterminism.
    #[test]
    fn execute_local_multi_segment_time_order_limit_is_deterministic() {
        let asc: DruidQuery = time_ordered_scan_query("ascending", Some(1));
        let desc: DruidQuery = time_ordered_scan_query("descending", Some(1));

        for _ in 0..10 {
            let dir = tempfile::tempdir().expect("tempdir");
            let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);
            // Load in an order unrelated to time order.
            hist.load_segment("seg_c", build_day_segment("2024-01-03T00:00:00Z", &["d3"]))
                .expect("load");
            hist.load_segment("seg_a", build_day_segment("2024-01-01T00:00:00Z", &["d1"]))
                .expect("load");
            hist.load_segment("seg_b", build_day_segment("2024-01-02T00:00:00Z", &["d2"]))
                .expect("load");
            for seg in ["seg_a", "seg_b", "seg_c"] {
                hist.set_segment_datasource(seg, "wiki").expect("ds");
            }

            let broker = Broker::new();
            let result = broker.execute_local(&asc, &[&hist]).expect("query");
            match result.result {
                QueryResult::Scan(scan) => {
                    assert_eq!(scan.events.len(), 1);
                    assert_eq!(
                        scan.events[0].get("region"),
                        Some(&serde_json::json!("d1")),
                        "ORDER BY __time ASC LIMIT 1 must always return the earliest row"
                    );
                }
                _ => panic!("expected scan"),
            }

            let result = broker.execute_local(&desc, &[&hist]).expect("query");
            match result.result {
                QueryResult::Scan(scan) => {
                    assert_eq!(scan.events.len(), 1);
                    assert_eq!(
                        scan.events[0].get("region"),
                        Some(&serde_json::json!("d3")),
                        "ORDER BY __time DESC LIMIT 1 must always return the latest row"
                    );
                }
                _ => panic!("expected scan"),
            }
        }
    }

    /// W-C S3: a time-ordered scan whose projection EXCLUDES `__time`
    /// (`SELECT region … ORDER BY __time LIMIT 2`) must still return
    /// the right rows — the broker threads `__time` through the fan-out
    /// for its re-sort and strips it from the merged result.
    #[test]
    fn execute_local_time_ordered_scan_without_time_projection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);
        hist.load_segment(
            "seg_b",
            build_day_segment("2024-01-02T00:00:00Z", &["d2a", "d2b"]),
        )
        .expect("load");
        hist.load_segment(
            "seg_a",
            build_day_segment("2024-01-01T00:00:00Z", &["d1a", "d1b"]),
        )
        .expect("load");
        hist.set_segment_datasource("seg_a", "wiki").expect("ds");
        hist.set_segment_datasource("seg_b", "wiki").expect("ds");

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "scan",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-05T00:00:00.000Z"],
                "columns": ["region"],
                "order": "ascending",
                "limit": 2
            }"#,
        )
        .expect("parse");

        let broker = Broker::new();
        let result = broker.execute_local(&query, &[&hist]).expect("query");
        match result.result {
            QueryResult::Scan(scan) => {
                let regions: Vec<Option<&str>> = scan
                    .events
                    .iter()
                    .map(|m| m.get("region").and_then(|v| v.as_str()))
                    .collect();
                assert_eq!(
                    regions,
                    vec![Some("d1a"), Some("d1b")],
                    "the 2 earliest rows, in time order, without __time projected"
                );
                assert!(
                    scan.events.iter().all(|m| !m.contains_key("__time")),
                    "injected __time must be stripped from events"
                );
                assert!(
                    !scan.columns.iter().any(|c| c == "__time"),
                    "injected __time must be stripped from columns"
                );
            }
            _ => panic!("expected scan"),
        }
    }

    /// Multi-segment ordering High (2026-07-21): a scan whose
    /// `virtualColumns` defines a column named `__time` must be REJECTED
    /// (Druid reserves `__time` for the row timestamp).  Pre-fix the
    /// query was accepted and the virtual value OVERWROTE the emitted
    /// `__time` cell after the executor's physical-time sort, so the
    /// broker's merge (which sorts on the projected `__time` cell) saw
    /// a constant — every cross-segment row tied and shard-arrival
    /// order decided which rows survived the LIMIT (late rows could win
    /// an ascending scan).
    #[test]
    fn execute_local_rejects_virtual_column_named_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);
        hist.load_segment(
            "seg_b",
            build_day_segment("2024-01-02T00:00:00Z", &["d2a", "d2b"]),
        )
        .expect("load");
        hist.load_segment(
            "seg_a",
            build_day_segment("2024-01-01T00:00:00Z", &["d1a", "d1b"]),
        )
        .expect("load");
        hist.set_segment_datasource("seg_a", "wiki").expect("ds");
        hist.set_segment_datasource("seg_b", "wiki").expect("ds");

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "scan",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-05T00:00:00.000Z"],
                "virtualColumns": [
                    {"type": "expression", "name": "__time", "expression": "0"}
                ],
                "order": "ascending",
                "limit": 2
            }"#,
        )
        .expect("parse");

        let broker = Broker::new();
        let err = broker
            .execute_local(&query, &[&hist])
            .expect_err("a virtual column named __time must fail loud, not shadow the sort key");
        let msg = err.to_string();
        assert!(msg.contains("__time"), "{msg}");
        assert!(msg.contains("reserved"), "{msg}");
    }

    /// Guard for the fix above: a NON-`__time` virtual column on a
    /// time-ordered multi-segment scan must still work — the merge sorts
    /// on the PHYSICAL `__time` and the virtual column's value is
    /// emitted alongside it.
    #[test]
    fn execute_local_time_ordered_scan_with_virtual_column_sorts_by_physical_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);
        // Load the LATER segment first (wrong arrival order for ASC).
        hist.load_segment(
            "seg_b",
            build_day_segment("2024-01-02T00:00:00Z", &["d2a", "d2b"]),
        )
        .expect("load");
        hist.load_segment(
            "seg_a",
            build_day_segment("2024-01-01T00:00:00Z", &["d1a", "d1b"]),
        )
        .expect("load");
        hist.set_segment_datasource("seg_a", "wiki").expect("ds");
        hist.set_segment_datasource("seg_b", "wiki").expect("ds");

        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "scan",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-05T00:00:00.000Z"],
                "virtualColumns": [
                    {"type": "expression", "name": "bonus", "expression": "value + 100.0"}
                ],
                "order": "ascending",
                "limit": 2
            }"#,
        )
        .expect("parse");

        let broker = Broker::new();
        let result = broker.execute_local(&query, &[&hist]).expect("query");
        match result.result {
            QueryResult::Scan(scan) => {
                let regions: Vec<Option<&str>> = scan
                    .events
                    .iter()
                    .map(|m| m.get("region").and_then(|v| v.as_str()))
                    .collect();
                assert_eq!(
                    regions,
                    vec![Some("d1a"), Some("d1b")],
                    "ORDER BY __time ASC LIMIT 2 must return the 2 physically-earliest rows"
                );
                let day1 = iso_millis("2024-01-01T00:00:00Z");
                let times: Vec<Option<i64>> = scan
                    .events
                    .iter()
                    .map(|m| m.get("__time").and_then(serde_json::Value::as_i64))
                    .collect();
                assert_eq!(
                    times,
                    vec![Some(day1), Some(day1 + 1)],
                    "the emitted __time must be the physical timestamp"
                );
                let bonuses: Vec<Option<f64>> = scan
                    .events
                    .iter()
                    .map(|m| m.get("bonus").and_then(serde_json::Value::as_f64))
                    .collect();
                assert_eq!(
                    bonuses,
                    vec![Some(100.0), Some(101.0)],
                    "the virtual column must be evaluated and emitted"
                );
            }
            _ => panic!("expected scan"),
        }
    }

    /// W-C S3 (groupBy limit push-down): Druid does NOT push a groupBy
    /// limitSpec limit down to per-segment execution — a group that is
    /// below the per-segment top-k on EVERY segment can still be the
    /// global top-1 once its partial counts merge.  seg1 = a×5, b×4;
    /// seg2 = c×5, b×4 → global b=8 must win `ORDER BY cnt DESC LIMIT 1`.
    #[test]
    fn execute_local_groupby_limit_not_pushed_down_to_segments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Historical::new(dir.path().to_path_buf(), 10_000_000);
        hist.load_segment(
            "seg_1",
            build_day_segment(
                "2024-01-01T00:00:00Z",
                &["a", "a", "a", "a", "a", "b", "b", "b", "b"],
            ),
        )
        .expect("load");
        hist.load_segment(
            "seg_2",
            build_day_segment(
                "2024-01-02T00:00:00Z",
                &["c", "c", "c", "c", "c", "b", "b", "b", "b"],
            ),
        )
        .expect("load");
        hist.set_segment_datasource("seg_1", "wiki").expect("ds");
        hist.set_segment_datasource("seg_2", "wiki").expect("ds");

        let query = groupby_limit_query(
            r#"{"type":"default","limit":1,
                "columns":[{"dimension":"cnt","direction":"descending","dimensionOrder":"numeric"}]}"#,
        );
        let broker = Broker::new();
        let result = broker.execute_local(&query, &[&hist]).expect("query");
        match result.result {
            QueryResult::GroupBy(results) => {
                assert_eq!(results.len(), 1, "limit 1 must hold");
                assert_eq!(
                    results[0].event.get("region"),
                    Some(&serde_json::json!("b")),
                    "the globally-highest group (b=8) must win even though it is \
                     second on every individual segment"
                );
                assert_eq!(results[0].event.get("cnt"), Some(&serde_json::json!(8)));
            }
            _ => panic!("expected groupBy"),
        }
    }

    // -------------------------------------------------------------------
    // Broker-wide merge cardinality caps (W-C S3 follow-up High): the
    // limit-push-down strip made `merge_groupby` accumulate the UNION of
    // every segment's groups (each partial individually capped at 1M),
    // so a many-segment high-cardinality `GROUP BY … LIMIT 1` could
    // allocate segments × 1M keys at the broker.  The merge must fail
    // loud (`ResourceLimit`, like the per-segment guard) DURING
    // accumulation — never silently truncate (that would drop groups
    // and return wrong LIMIT results), never build the whole union
    // first.  Same species guard for the topN merge union.
    // -------------------------------------------------------------------

    fn parsed_groupby(query: &DruidQuery) -> &ferrodruid_query::GroupByQuery {
        match query {
            DruidQuery::GroupBy(q) => q,
            _ => panic!("expected groupBy query"),
        }
    }

    /// RED→GREEN core: a merge whose DISTINCT-group union crosses the
    /// broker cap must fail with the loud `ResourceLimit` error — and
    /// `observed == cap + 1` proves the guard fired at the moment the
    /// (cap+1)-th distinct group was inserted (a post-union check would
    /// observe the full distinct count, 10 here).
    #[test]
    fn merge_groupby_over_cap_fails_loud_during_accumulation() {
        let p1 = QueryResult::GroupBy(
            (0..5)
                .map(|i| groupby_row(serde_json::json!(format!("g{i}")), 1))
                .collect(),
        );
        let p2 = QueryResult::GroupBy(
            (5..10)
                .map(|i| groupby_row(serde_json::json!(format!("g{i}")), 1))
                .collect(),
        );

        let query = groupby_limit_query(
            r#"{"type":"default","limit":1,
                "columns":[{"dimension":"cnt","direction":"descending","dimensionOrder":"numeric"}]}"#,
        );
        let q = parsed_groupby(&query);
        let err = merge_groupby_capped(vec![p1, p2], &q.dimensions, &q.aggregations, 3)
            .expect_err("over-cap merged group cardinality must fail loud, not exhaust memory");
        match err {
            DruidError::ResourceLimit {
                kind,
                limit,
                observed,
            } => {
                assert_eq!(kind, "groupBy.maxMergedResults");
                assert_eq!(limit, 3);
                assert_eq!(
                    observed, 4,
                    "guard must fire DURING accumulation (at distinct group cap+1), \
                     not after building the whole 10-group union"
                );
            }
            other => panic!("expected ResourceLimit, got {other:?}"),
        }
    }

    /// The cap bounds DISTINCT merged groups, not raw entries: the same
    /// 4 groups arriving from 2 shards (8 entries) must merge fine at
    /// cap 4, with exact per-group merged values — a legitimate
    /// many-segment query whose union stays under the cap is untouched.
    #[test]
    fn merge_groupby_cap_counts_distinct_merged_groups_not_entries() {
        let partial = || {
            QueryResult::GroupBy(
                (0..4)
                    .map(|i| groupby_row(serde_json::json!(format!("g{i}")), i + 1))
                    .collect(),
            )
        };

        let query = groupby_limit_query(
            r#"{"type":"default","limit":4,
                "columns":[{"dimension":"cnt","direction":"descending","dimensionOrder":"numeric"}]}"#,
        );
        let q = parsed_groupby(&query);
        let merged = merge_groupby_capped(
            vec![partial(), partial()],
            &q.dimensions,
            &q.aggregations,
            4,
        )
        .expect("4 distinct groups at cap 4 must not trip the guard");
        match merged {
            QueryResult::GroupBy(results) => {
                assert_eq!(results.len(), 4, "all 4 distinct groups survive");
                for (i, row) in results.iter().enumerate() {
                    assert_eq!(
                        row.event.get("cnt"),
                        Some(&serde_json::json!(2 * (i as i64 + 1))),
                        "per-group values must merge exactly (2 shards × cnt)"
                    );
                }
            }
            _ => panic!("expected groupBy"),
        }
    }

    /// The default-cap wrapper must not spuriously fire and the W-C S3
    /// LIMIT correctness must be preserved: an under-cap multi-segment
    /// merge through the REAL `merge_results` path still returns the
    /// correct globally-ordered top-k.
    #[test]
    fn merge_results_groupby_under_default_cap_keeps_global_top_k() {
        let p1 = QueryResult::GroupBy(
            (0..10)
                .map(|i| groupby_row(serde_json::json!(format!("x{i}")), i + 1))
                .collect(),
        );
        // x0 merges 1 + 100 = 101 and must jump to global rank 1.
        let p2 = QueryResult::GroupBy(vec![groupby_row(serde_json::json!("x0"), 100)]);

        let query = groupby_limit_query(
            r#"{"type":"default","limit":2,
                "columns":[{"dimension":"cnt","direction":"descending","dimensionOrder":"numeric"}]}"#,
        );
        let merged = Broker::merge_results(&query, vec![p1, p2]).expect("under-cap merge");
        match merged {
            QueryResult::GroupBy(results) => {
                let top: Vec<(Option<&str>, Option<i64>)> = results
                    .iter()
                    .map(|r| {
                        (
                            r.event.get("region").and_then(|v| v.as_str()),
                            r.event.get("cnt").and_then(serde_json::Value::as_i64),
                        )
                    })
                    .collect();
                assert_eq!(
                    top,
                    vec![(Some("x0"), Some(101)), (Some("x9"), Some(10))],
                    "global top-2 after cross-shard merge (ordering fix preserved)"
                );
            }
            _ => panic!("expected groupBy"),
        }
    }

    fn topn_longsum_query() -> TopNQuery {
        let query: DruidQuery = serde_json::from_str(
            r#"{
                "queryType": "topN",
                "dataSource": {"type":"table","name":"wiki"},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-03T00:00:00.000Z"],
                "granularity": "all",
                "dimension": "region",
                "metric": "uniq",
                "threshold": 10,
                "aggregations": [
                    {"type":"longSum","name":"uniq","fieldName":"x"}
                ]
            }"#,
        )
        .expect("parse");
        match query {
            DruidQuery::TopN(q) => q,
            _ => panic!("expected topN query"),
        }
    }

    fn topn_partial_at(ts: &str, rows: &[(&str, i64)]) -> QueryResult {
        QueryResult::TopN(vec![TopNResult {
            timestamp: ts.to_string(),
            result: rows
                .iter()
                .map(|(dim, v)| {
                    let mut m = serde_json::Map::new();
                    m.insert("region".to_string(), serde_json::json!(dim));
                    m.insert("uniq".to_string(), serde_json::json!(v));
                    m
                })
                .collect(),
        }])
    }

    /// TopN twin of the groupBy merge cap: the in-flight counter spans
    /// ALL time buckets (mirroring the per-segment
    /// `topN.maxIntermediateRows` guard), so 2 distinct dims in each of
    /// 2 buckets cross a cap of 3 — and `observed == cap + 1` proves
    /// the accumulation-time bail.
    #[test]
    fn merge_topn_over_cap_fails_loud_during_accumulation() {
        let p1 = topn_partial_at("2024-01-01T00:00:00.000Z", &[("a", 1), ("b", 2)]);
        let p2 = topn_partial_at("2024-01-02T00:00:00.000Z", &[("c", 3), ("d", 4)]);

        let query = topn_longsum_query();
        let err = merge_topn_capped(vec![p1, p2], &query, 3)
            .expect_err("over-cap merged topN cardinality must fail loud, not exhaust memory");
        match err {
            DruidError::ResourceLimit {
                kind,
                limit,
                observed,
            } => {
                assert_eq!(kind, "topN.maxMergedIntermediateRows");
                assert_eq!(limit, 3);
                assert_eq!(
                    observed, 4,
                    "guard must fire DURING accumulation across buckets"
                );
            }
            other => panic!("expected ResourceLimit, got {other:?}"),
        }
    }

    /// The topN cap bounds DISTINCT `(bucket, dim)` keys, not entries:
    /// the same 2 dims from 2 shards (4 entries) merge fine at cap 2
    /// with exact summed values.
    #[test]
    fn merge_topn_cap_counts_distinct_dims_not_entries() {
        let p1 = topn_partial_at("2024-01-01T00:00:00.000Z", &[("a", 1), ("b", 2)]);
        let p2 = topn_partial_at("2024-01-01T00:00:00.000Z", &[("a", 10), ("b", 20)]);

        let query = topn_longsum_query();
        let merged = merge_topn_capped(vec![p1, p2], &query, 2)
            .expect("2 distinct dims at cap 2 must not trip the guard");
        match merged {
            QueryResult::TopN(results) => {
                assert_eq!(results.len(), 1, "one time bucket");
                let by_dim: HashMap<&str, i64> = results[0]
                    .result
                    .iter()
                    .filter_map(|m| {
                        Some((
                            m.get("region").and_then(|v| v.as_str())?,
                            m.get("uniq").and_then(serde_json::Value::as_i64)?,
                        ))
                    })
                    .collect();
                assert_eq!(by_dim.get("a"), Some(&11));
                assert_eq!(by_dim.get("b"), Some(&22));
            }
            _ => panic!("expected topN"),
        }
    }
}
