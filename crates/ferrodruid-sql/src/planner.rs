// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! SQL query planner — converts a parsed [`DruidSqlStatement`] into a Druid
//! [`DruidQuery`] (native query) suitable for execution.
//!
//! Planning rules:
//! 1. `SELECT count(*) FROM ds` -> Timeseries with count aggregation
//! 2. `SELECT city, count(*) FROM ds GROUP BY city` -> GroupBy
//! 3. `SELECT city, count(*) FROM ds GROUP BY city ORDER BY count(*) DESC LIMIT 10` -> TopN
//! 4. `SELECT * FROM ds WHERE ...` (no aggregation) -> Scan
//! 5. `TIME_FLOOR(__time, period)` in GROUP BY -> Timeseries with granularity
//! 6. `EXPLAIN PLAN FOR ...` -> plan but return the native query JSON

use serde::{Deserialize, Serialize};

use ferrodruid_aggregator::{AggregatorSpec, PostAggregatorSpec};
use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_common::types::{ColumnType, DataSource, DimensionSpec, Granularity};
use ferrodruid_query::DruidQuery;
use ferrodruid_query::GranularitySpec;
use ferrodruid_query::filter::FilterSpec;
use ferrodruid_query::groupby::{GroupByQuery, LimitSpec, OrderByColumnSpec};
use ferrodruid_query::scan::ScanQuery;
use ferrodruid_query::timeseries::TimeseriesQuery;
use ferrodruid_query::topn::{TopNMetricSpec, TopNQuery};
use ferrodruid_query::window::{
    SortDirection, WindowFrame as ExecWindowFrame, WindowFrameBound as ExecFrameBound,
    WindowFrameMode as ExecFrameMode, WindowFunctionKind, WindowOrderBy, WindowQuery, WindowSpec,
};

use crate::functions::{
    DruidFunction, FrameBound as SqlFrameBound, FrameMode as SqlFrameMode,
    WindowFrame as SqlWindowFrame, WindowFunctionType, period_to_fixed_millis,
    period_to_granularity,
};
use crate::parser::{
    BinaryOperator, DruidSqlStatement, JoinClause, JoinRightSide, OrderByExpr, Projection,
    SelectQuery, SqlExpr, SqlJoinType, SqlLiteral, TableReference, time_literal_to_millis,
};
use crate::types::{OutputColumn, SqlType};

// ---------------------------------------------------------------------------
// PlannedQuery — the output of the planner
// ---------------------------------------------------------------------------

/// The result of planning a SQL statement: a native query plus output column metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedQuery {
    /// The Druid native query to execute.
    pub native_query: DruidQuery,
    /// Column metadata for the output result set.
    pub output_columns: Vec<OutputColumn>,
    /// Join lowering, when the SQL FROM clause contained one or more joins.
    ///
    /// `None` for a plain (join-free) query — existing callers are unaffected.
    /// When `Some`, the executor materialises the left side (a scan over the
    /// base data source named by [`DruidQuery`]'s data source), applies the
    /// joins in order (each producing the left side for the next), and runs the
    /// outer [`PlannedQuery::native_query`] over the joined rows. The native
    /// query's own data source identifies the *left base table*; the join
    /// enrichment is described here so the shared
    /// [`ferrodruid_common::types::DataSource`] enum need not gain a `join`
    /// variant (which would break exhaustive matches across the workspace).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub joins: Vec<PlannedJoin>,
    /// The output column that carries the `TIME_FLOOR(...)` bucket key, BY
    /// PLANNED ROLE (codex-review HIGH finding C on P1-#2).
    ///
    /// `Some(name)` only when the SELECT list contains the time-grain key
    /// (the planner's `SelectOutputItem::TimeKey`): the granular Timeseries
    /// path and the granular GroupBy path, where the bucket value lives in
    /// the native result ENVELOPE (`timestamp`), never in the result map.
    /// The REST wire layer surfaces the envelope timestamp under this
    /// column.
    ///
    /// This replaces the REST layer's former name-based inference (any
    /// TIMESTAMP-typed output column not named like an aggregation), which
    /// collided with HIDDEN `$`-prefixed helper aggregators: a bucket
    /// legitimately aliased `"$avg_sum_0"` was dropped from the bucket role
    /// and the hidden AVG sum was emitted as an ISO timestamp. Role marking
    /// cannot collide — hidden helpers never participate, and a
    /// TIMESTAMP-typed aggregate (`MAX(__time)`, P1-#2) is never marked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_bucket_column: Option<String>,
}

// ---------------------------------------------------------------------------
// PlannedJoin — a single lowered join
// ---------------------------------------------------------------------------

/// One lowered join: the executor-facing [`ferrodruid_query::JoinDataSource`]
/// shape, plus — for a base-table or sub-query right side — the native query
/// whose results materialise the right relation before the join runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedJoin {
    /// The join data source (right relation, prefix, condition, type).
    ///
    /// For an inline `(VALUES ...)` or `LOOKUP(...)` right side this is fully
    /// self-contained. For a base-table / sub-query right side the
    /// [`ferrodruid_query::JoinRight::Rows`] is left empty and
    /// [`PlannedJoin::right_native_query`] carries the native query to run to
    /// fill it.
    pub join: ferrodruid_query::JoinDataSource,
    /// When the right side is a base table or sub-query, the native query whose
    /// scan materialises the right rows. `None` for inline / lookup rights.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_native_query: Option<Box<DruidQuery>>,
}

// ---------------------------------------------------------------------------
// DataSourceSchema — metadata needed for planning
// ---------------------------------------------------------------------------

/// Schema information about a data source, used during query planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceSchema {
    /// The data source name.
    pub name: String,
    /// Dimension columns.
    pub dimensions: Vec<ColumnSchema>,
    /// Metric columns.
    pub metrics: Vec<ColumnSchema>,
    /// The name of the time column (defaults to `__time`).
    pub time_column: String,
    /// Schemas for base tables that may appear on the *right* side of a JOIN,
    /// keyed by data-source name. When a `JOIN <table>` references a base table,
    /// the planner consults this map to materialise the right relation's full
    /// projected column set (so every right column, not just the join key, is
    /// available downstream). Defaults to empty; callers that join base tables
    /// should populate it with each joined table's schema.
    #[serde(default)]
    pub join_schemas: Vec<DataSourceSchema>,
}

impl DataSourceSchema {
    /// The list of output column names for this data source: the time column,
    /// every dimension, then every metric. This mirrors what a `SELECT * FROM
    /// <table>` scan exposes and is used to populate a base-table join right's
    /// `column_names`.
    fn projected_column_names(&self) -> Vec<String> {
        let mut names = Vec::with_capacity(1 + self.dimensions.len() + self.metrics.len());
        // A time-less datasource carries an empty `time_column` (no segment has
        // `__time`); it exposes no time column, so it is not projected.
        if !self.time_column.is_empty() {
            names.push(self.time_column.clone());
        }
        names.extend(self.dimensions.iter().map(|c| c.name.clone()));
        names.extend(self.metrics.iter().map(|c| c.name.clone()));
        names
    }
}

/// Schema for a single column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// Column name.
    pub name: String,
    /// Column type.
    pub column_type: ColumnType,
}

impl DataSourceSchema {
    /// Look up a column by name, returning its type if found.
    pub fn column_type(&self, name: &str) -> Option<&ColumnType> {
        if name == self.time_column {
            return Some(&ColumnType::Long);
        }
        self.dimensions
            .iter()
            .chain(self.metrics.iter())
            .find(|c| c.name == name)
            .map(|c| &c.column_type)
    }
}

// ---------------------------------------------------------------------------
// Default interval (covers "all time" when none specified)
// ---------------------------------------------------------------------------

const DEFAULT_INTERVAL: &str = "1970-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z";

// ---------------------------------------------------------------------------
// Planner options (SQL query-context knobs that change the lowering)
// ---------------------------------------------------------------------------

/// Planner knobs derived from the SQL query context (the `context` object of
/// a `POST /druid/v2/sql` request body).
///
/// Druid's `useApproximateCountDistinct` context key (default `true`)
/// selects how `COUNT(DISTINCT col)` lowers:
///
/// * `true` (the default) — a hidden `HLLSketchBuild` finalised by an
///   `HllSketchEstimate { round: true }` post-aggregation (approximate;
///   deep-matches live Druid 30-36 defaults — harness Section 7
///   `null_count_distinct_device`).
/// * `false` — a VISIBLE not-null-filtered exact `cardinality` aggregation
///   (`AggregatorSpec::Cardinality { fields: [col], by_row: false }` wrapped
///   in the same not-null `Filtered` the `COUNT(col)` lowering uses, because
///   SQL `COUNT(DISTINCT)` ignores NULLs while the bare cardinality
///   aggregator would count a `__null__` key). Output stays BIGINT.
///
/// `APPROX_COUNT_DISTINCT(col)` is an explicit approximate request and stays
/// on the HLL path in BOTH modes, matching Druid.
///
/// Exactness bound of the exact path (FAIL-CLOSED as of 2026-07-11,
/// exact multi-shard union as of 2026-07-12, enforced in
/// `ferrodruid_aggregator` / `ferrodruid-query` / `ferrodruid-broker` —
/// the cap stays as DoS protection, but hitting it errors instead of
/// returning a silently-wrong count):
///
/// * the exact distinct set caps at `MAX_CARDINALITY_SET_SIZE`
///   (1,000,000 keys) — a single unified bound covering both the
///   per-aggregator set and the broker's cross-segment set union (the
///   executors ship the full exact set per shard as a
///   `CardinalityState` envelope; the former 1,000-key wire cap is
///   gone). A saturated set or an over-cap union FAILS the query with
///   `DruidError::ResourceLimit` (REST: HTTP 400,
///   `io.druid.query.ResourceLimitExceededException`) instead of
///   silently under-/over-counting.
///
/// Within that bound exact mode returns the exact count on single- AND
/// multi-segment datasources (overlapping per-segment sets union without
/// over-counting); beyond it the query errors with a message pointing at
/// `APPROX_COUNT_DISTINCT`.
#[derive(Debug, Clone, Copy)]
pub struct PlannerOptions {
    /// Druid's `useApproximateCountDistinct` SQL context key. `true` (the
    /// Druid default) lowers `COUNT(DISTINCT col)` to the approximate HLL
    /// sketch path; `false` lowers it to the exact `cardinality`
    /// aggregation.
    pub use_approximate_count_distinct: bool,
}

impl Default for PlannerOptions {
    fn default() -> Self {
        Self {
            use_approximate_count_distinct: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Plan a parsed SQL statement into a native query with default
/// [`PlannerOptions`] (approximate `COUNT(DISTINCT)`, Druid's default).
pub fn plan_sql(stmt: &DruidSqlStatement, schema: &DataSourceSchema) -> Result<PlannedQuery> {
    plan_sql_with_options(stmt, schema, PlannerOptions::default())
}

/// Plan a parsed SQL statement into a native query with explicit
/// [`PlannerOptions`] (e.g. `useApproximateCountDistinct: false` parsed from
/// the SQL query context by the REST layer).
pub fn plan_sql_with_options(
    stmt: &DruidSqlStatement,
    schema: &DataSourceSchema,
    options: PlannerOptions,
) -> Result<PlannedQuery> {
    match stmt {
        DruidSqlStatement::Select(select) => plan_select(select, schema, options),
        // A constant SELECT (`SELECT 1`) has no data source to scan; it is
        // materialised directly at the query layer, so there is no native query
        // to plan. It never reaches here on the execute path (the SQL handler
        // intercepts it); this arm only guards EXPLAIN of a constant SELECT.
        DruidSqlStatement::ConstantSelect(_) => Err(DruidError::Query(
            "constant SELECT (no FROM) is materialised directly, not planned as a native query"
                .to_string(),
        )),
        DruidSqlStatement::ExplainPlan(inner) => plan_sql_with_options(inner, schema, options),
        DruidSqlStatement::UnionAll(parts) => {
            // Plan each sub-query independently. The first determines output columns.
            if parts.is_empty() {
                return Err(DruidError::Query(
                    "UNION ALL requires at least one sub-query".to_string(),
                ));
            }
            // The native key an output column reads from the result row:
            // an aliased scan projection (`SELECT a AS x`) keeps the raw
            // column key `a` in the native row (`source`), while aggregates
            // carry their alias as the native `outputName` (`source` is
            // None, so the key is `name`).
            fn native_keys(planned: &PlannedQuery) -> Vec<&str> {
                planned
                    .output_columns
                    .iter()
                    .map(|c| c.source.as_deref().unwrap_or(c.name.as_str()))
                    .collect()
            }

            // Canonical repetition pattern of a branch's native keys: each
            // position maps to the FIRST position that reads the same native
            // column. Positional cross-branch alignment collapses repeated
            // source columns to one deduplicated native column, so it is only
            // sound when every branch shares the SAME repetition pattern (e.g.
            // `a AS x, a AS y, b AS z` and `c AS p, c AS q, d AS r` both have
            // pattern [0,0,2] and align safely; `a AS x, a AS y` vs
            // `c AS p, d AS q` — patterns [0,0] vs [0,1] — would silently
            // mis-map and are rejected). Distinct-column branches trivially
            // share pattern [0,1,2,…].
            fn repetition_pattern(keys: &[&str]) -> Vec<usize> {
                let mut first_seen: std::collections::HashMap<&str, usize> =
                    std::collections::HashMap::new();
                keys.iter()
                    .enumerate()
                    .map(|(i, k)| *first_seen.entry(*k).or_insert(i))
                    .collect()
            }

            let first = plan_sql_with_options(&parts[0], schema, options)?;
            let first_native = native_keys(&first);
            let first_keys: Vec<String> = first_native.iter().map(|k| (*k).to_owned()).collect();
            let first_pattern = repetition_pattern(&first_native);
            let mut sub_queries = vec![first.native_query];
            for (branch_idx, part) in parts[1..].iter().enumerate() {
                let planned = plan_sql_with_options(part, schema, options)?;
                // Druid names the UNION ALL output from the first branch and
                // maps every later branch's columns into it by POSITION. The
                // executor/broker align each branch's native column keys to
                // the first branch's at merge time (see
                // `ferrodruid_query::align_union_branch`), so differently-named
                // branches are supported. A differing ARITY (column count) or
                // a differing repeated-source PATTERN is a genuine error —
                // reject it fail-closed rather than emit mis-mapped rows.
                let branch_keys = native_keys(&planned);
                if branch_keys.len() != first_keys.len() {
                    return Err(DruidError::Query(format!(
                        "UNION ALL branch {} projects {} column(s) {:?}, but the first branch \
                         projects {} column(s) {:?}; every branch must project the same \
                         number of columns",
                        branch_idx + 2,
                        branch_keys.len(),
                        branch_keys,
                        first_keys.len(),
                        first_keys,
                    )));
                }
                if repetition_pattern(&branch_keys) != first_pattern {
                    return Err(DruidError::Query(format!(
                        "UNION ALL branch {} repeats source columns in a different pattern than \
                         the first branch (native columns {:?} vs {:?}); positional remapping \
                         requires every branch to repeat source columns in the same positions",
                        branch_idx + 2,
                        branch_keys,
                        first_keys,
                    )));
                }
                sub_queries.push(planned.native_query);
            }
            // Wrap in a UnionAll query — each sub-query is executed independently
            // and results are concatenated. We represent this using the first
            // sub-query plus storing additional queries for the executor.
            Ok(PlannedQuery {
                native_query: DruidQuery::UnionAll(sub_queries),
                output_columns: first.output_columns,
                joins: Vec::new(),
                // Output columns follow the first sub-query; so does the
                // time-bucket role marking.
                time_bucket_column: first.time_bucket_column,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Select planner
// ---------------------------------------------------------------------------

fn plan_select(
    select: &SelectQuery,
    schema: &DataSourceSchema,
    options: PlannerOptions,
) -> Result<PlannedQuery> {
    // A FROM clause carrying one or more joins is lowered separately: the outer
    // SELECT (projection / WHERE / GROUP BY) runs over the *joined* rows, so we
    // plan a join-free copy of this query (with column qualifiers normalised)
    // and attach the lowered joins to the resulting plan.
    if !select.from.joins.is_empty() {
        return plan_select_with_joins(select, schema, options);
    }

    // Resolve the data source. When the FROM clause is an inlined common
    // table expression (a `WITH` CTE that the parser substituted in), lower
    // it to a Druid `query` data source whose body is the planned native
    // query of the CTE. Otherwise it is the base table named by the schema.
    let ds = match &select.from.subquery {
        Some(inner) => {
            let inner_planned = plan_sql_with_options(inner, schema, options)?;
            let query_json = serde_json::to_value(&inner_planned.native_query).map_err(|e| {
                DruidError::Query(format!("failed to serialise CTE sub-query: {e}"))
            })?;
            DataSource::Query {
                query: Box::new(query_json),
            }
        }
        None => DataSource::Table {
            name: schema.name.clone(),
        },
    };
    let intervals = vec![DEFAULT_INTERVAL.to_string()];

    // Wave 47-D §1: any projection containing a window function call
    // (`func(...) OVER (...)`) lowers to a native Window query — a Scan
    // wrapped in one or more window operators. Without this dispatch the
    // window column was silently dropped.
    if select.projections.iter().any(|p| {
        matches!(
            p,
            Projection::Expr {
                expr: SqlExpr::Window(_),
                ..
            }
        )
    }) {
        return plan_window(select, ds, intervals, schema);
    }

    // Extract aggregations and non-aggregation projections from the SELECT
    // clause. `select_items` records every output slot in SELECT-projection
    // order (codex QA r5) so the planners can emit `output_columns` exactly
    // matching the SELECT list.
    let mut aggs = AggProjections {
        exact_count_distinct: !options.use_approximate_count_distinct,
        ..AggProjections::default()
    };
    let mut dimension_exprs: Vec<(SqlExpr, Option<String>)> = Vec::new();
    let mut select_items: Vec<SelectOutputItem> = Vec::new();
    let mut is_wildcard = false;
    let mut time_grain: Option<TimeGrain> = None;
    let mut time_floor_alias: Option<String> = None;

    for proj in &select.projections {
        match proj {
            Projection::Wildcard => {
                is_wildcard = true;
            }
            Projection::Expr { expr, alias } => {
                if let Some(spec) = try_as_aggregation(
                    expr,
                    alias,
                    aggs.specs.len(),
                    Some(schema),
                    aggs.exact_count_distinct,
                )? {
                    let agg_name = spec.name().to_string();
                    // P1-#2: `MIN(__time)` / `MAX(__time)` (lowered to
                    // longMin/longMax over the time column just above) are
                    // TIMESTAMP-typed outputs — the SQL wire renders them as
                    // Druid's ISO-8601 millis string, not a bare integer.
                    let is_time_min_max = matches!(
                        &spec,
                        AggregatorSpec::LongMin { field_name, .. }
                        | AggregatorSpec::LongMax { field_name, .. }
                            if is_time_column(field_name, Some(schema))
                    );
                    aggs.specs.push(spec);
                    let out_name = alias.clone().unwrap_or(agg_name);
                    if is_time_min_max {
                        aggs.post_agg_types
                            .push((out_name.clone(), SqlType::Timestamp));
                    }
                    aggs.aliases.push(out_name.clone());
                    aggs.output_names.push(out_name.clone());
                    select_items.push(SelectOutputItem::Aggregate(out_name));
                } else if let SqlExpr::Function(DruidFunction::TimeFloor { period, .. }) = expr {
                    time_grain = Some(TimeGrain::Period(period.clone()));
                    time_floor_alias = alias.clone();
                    dimension_exprs.push((expr.clone(), alias.clone()));
                    select_items.push(SelectOutputItem::TimeKey);
                } else if let Some(gran) = shifted_week_granularity(expr) {
                    // Superset's week_starting_sunday grain: the nested
                    // TIME_SHIFT/TIME_FLOOR expression is itself the time
                    // key, pre-lowered to a Sunday-anchored week granularity.
                    time_grain = Some(TimeGrain::Native(gran));
                    time_floor_alias = alias.clone();
                    dimension_exprs.push((expr.clone(), alias.clone()));
                    select_items.push(SelectOutputItem::TimeKey);
                } else if let Some((post, sql_type)) =
                    try_as_post_aggregation(expr, alias, &mut aggs, Some(schema))?
                {
                    // W2-C: AVG(x), arithmetic over aggregates, count-distinct
                    // sketches, and ROUND/ABS/FLOOR/CEIL over aggregates lower
                    // to a post-aggregation over hidden helper aggregations.
                    // The post-agg's name is the projection's output column;
                    // its SQL type drives the wire normalisation (BIGINT
                    // count-distinct collapses to an integer, DOUBLE keeps
                    // the trailing `.0`).
                    let name = post.name().to_string();
                    aggs.post_agg_types.push((name.clone(), sql_type));
                    aggs.output_names.push(name.clone());
                    aggs.post_aggs.push(post);
                    select_items.push(SelectOutputItem::Aggregate(name));
                } else {
                    dimension_exprs.push((expr.clone(), alias.clone()));
                    // A bare column reads itself; a non-column expression has
                    // no native column — record its alias (the only name a
                    // GROUP BY could bind it by; unaliased forms are rejected
                    // by `ensure_projections_grouped` before any output is
                    // planned, so the `$`-sentinel never surfaces).
                    let (column, output) = match expr {
                        SqlExpr::Column(name) => {
                            (name.clone(), alias.clone().unwrap_or_else(|| name.clone()))
                        }
                        _ => {
                            let name = alias
                                .clone()
                                .unwrap_or_else(|| "$unsupported_projection".to_string());
                            (name.clone(), name)
                        }
                    };
                    select_items.push(SelectOutputItem::Dimension { column, output });
                }
            }
        }
    }

    // Determine GROUP BY dimension names (skip positional refs to TIME_FLOOR).
    let group_by_dims: Vec<String> = extract_group_by_dims(select)?;

    // Determine the native filter.
    let filter = select.filter.as_ref().map(convert_filter).transpose()?;

    // Decision: what native query type to use?
    let has_aggregation = aggs.has_aggregation();
    let has_group_by = !select.group_by.is_empty();

    if !has_aggregation && !has_group_by {
        // Scan query
        return plan_scan(select, ds, intervals, filter, schema, is_wildcard);
    }

    // W2-C (fail closed): in an aggregating / grouping plan, every remaining
    // non-aggregate projection must be a grouping key (or the TIME_FLOOR time
    // key) — anything else has no output slot and previously vanished from
    // the results silently (HTTP 200 with the column missing).
    ensure_projections_grouped(&dimension_exprs, &group_by_dims)?;

    if has_aggregation && !has_group_by && time_grain.is_none() {
        // Simple aggregation with no group-by -> Timeseries with granularity=all
        return plan_timeseries_all(ds, intervals, filter, &aggs);
    }

    if has_aggregation
        && group_by_dims.is_empty()
        && let Some(tg) = &time_grain
    {
        // Aggregation grouped by the time grain only -> Timeseries with
        // specific granularity.
        let gran = tg.resolve()?;
        return plan_timeseries_granular(
            ds,
            intervals,
            filter,
            &aggs,
            gran,
            time_floor_alias,
            &select_items,
        );
    }

    // Check for TopN pattern: single non-time dimension + ORDER BY + LIMIT.
    // A multi-dimensional grouping (GROUPING SETS / CUBE / ROLLUP) cannot be
    // expressed as a TopN, so it always falls through to the GroupBy path
    // where the `subtotalsSpec` is carried.
    if group_by_dims.len() == 1
        && select.grouping_sets.is_none()
        && select.limit.is_some()
        && !select.order_by.is_empty()
        && time_grain.is_none()
    {
        let dim_name = &group_by_dims[0];
        if let Some(planned) = try_plan_topn(
            select,
            &ds,
            &intervals,
            filter.as_ref(),
            &aggs,
            dim_name,
            schema,
            &select_items,
        ) {
            return Ok(planned);
        }
    }

    // SELECT-list aliases of the non-aggregate projections, so ORDER BY can
    // reference an output column by its SELECT alias (standard SQL; Superset
    // emits `ORDER BY <alias>`). Each alias maps to the underlying output
    // column when the projection is a bare column, or to `None` when the
    // aliased expression has no planned output column (so the resolver can
    // report the real limitation instead of "unknown key").
    let select_aliases: Vec<(String, Option<String>)> = dimension_exprs
        .iter()
        .filter_map(|(expr, alias)| {
            let alias = alias.clone()?;
            let target = match expr {
                SqlExpr::Column(name) => Some(name.clone()),
                _ => None,
            };
            Some((alias, target))
        })
        .collect();

    // GroupBy query (general case)
    plan_groupby(
        select,
        ds,
        intervals,
        filter,
        &aggs,
        &group_by_dims,
        &select_aliases,
        schema,
        time_grain.as_ref(),
        time_floor_alias.as_deref(),
        &select_items,
    )
}

// ---------------------------------------------------------------------------
// Aggregate projections (visible aggregations + lowered post-aggregations)
// ---------------------------------------------------------------------------

/// Aggregate-derived SELECT outputs accumulated while scanning the projection
/// list: plain aggregations, plus post-aggregations lowered from `AVG(x)` and
/// binary arithmetic over aggregates (W2-C), together with the hidden helper
/// aggregations that feed those post-aggregations.
#[derive(Debug, Default)]
struct AggProjections {
    /// Visible aggregations, parallel to [`AggProjections::aliases`].
    specs: Vec<AggregatorSpec>,
    /// Output aliases, parallel to [`AggProjections::specs`].
    aliases: Vec<String>,
    /// Hidden helper aggregations that feed post-aggregations. Their names
    /// are `$`-prefixed and they never appear in the output columns.
    hidden_specs: Vec<AggregatorSpec>,
    /// Lowered post-aggregations; each spec's `name()` is a SELECT output
    /// alias.
    post_aggs: Vec<PostAggregatorSpec>,
    /// Aggregate-derived output names in SELECT projection order (plain
    /// aggregation aliases and post-aggregation aliases interleaved as
    /// written).
    output_names: Vec<String>,
    /// Per-output SQL type overrides, keyed by output name (any output not
    /// listed defaults to `BIGINT`). AVG / arithmetic /
    /// function-over-aggregate post-aggregation outputs are `DOUBLE`;
    /// count-distinct (HLL estimate) outputs are `BIGINT`; `MIN(__time)` /
    /// `MAX(__time)` are `TIMESTAMP` (P1-#2) — the REST layer keys its
    /// integer-collapse and ISO-8601 wire normalisation off this type.
    post_agg_types: Vec<(String, SqlType)>,
    /// Top-level `AVG(col)` lowerings as `(source column, output alias)`
    /// pairs, used to resolve `ORDER BY AVG(col)` to the post-aggregation
    /// output.
    avg_sources: Vec<(String, String)>,
    /// Monotonic counter for unique hidden aggregator / operand names.
    hidden_seq: usize,
    /// E16: `true` when the SQL context set `useApproximateCountDistinct:
    /// false` — `COUNT(DISTINCT col)` then lowers to the exact not-null-
    /// filtered `cardinality` aggregation instead of the HLL sketch (see
    /// [`PlannerOptions`]). Default `false` = approximate (Druid default).
    exact_count_distinct: bool,
}

/// One SELECT-list output slot, recorded in projection order (codex QA r5).
///
/// The GroupBy / Timeseries / TopN planners assemble
/// [`PlannedQuery::output_columns`] by walking this list, so the SQL wire
/// columns mirror the SELECT list exactly — aliased names, in SELECT order —
/// matching Druid's SQL layer. (Previously the GroupBy path emitted the
/// TIME_FLOOR alias, then every grouping dimension under its RAW column name,
/// then every aggregate, so positional clients like pydruid / Superset saw
/// swapped columns and dimension aliases vanished from the wire.)
#[derive(Debug)]
enum SelectOutputItem {
    /// The `TIME_FLOOR(__time, …)` time-bucket key.
    TimeKey,
    /// A non-aggregate projection: the native column it reads and the output
    /// name it surfaces under (the SELECT alias when present, otherwise the
    /// column name itself).
    Dimension { column: String, output: String },
    /// An aggregate-derived output (plain aggregation or lowered
    /// post-aggregation), by output name.
    Aggregate(String),
}

/// Resolve a GROUP BY key name — a raw column, or a SELECT alias when the
/// GROUP BY used the alias / ordinal form — to its `(native column, output
/// name)` pair using the SELECT projection items. A grouped-but-unselected
/// key groups under its raw name (it stays a native dimension but never
/// surfaces as a wire column).
fn resolve_dim_binding(name: &str, select_items: &[SelectOutputItem]) -> (String, String) {
    select_items
        .iter()
        .find_map(|item| match item {
            SelectOutputItem::Dimension { column, output } if column == name || output == name => {
                Some((column.clone(), output.clone()))
            }
            _ => None,
        })
        .unwrap_or_else(|| (name.to_string(), name.to_string()))
}

/// Sentinel occupying the SELECT position of an UNALIASED `TIME_FLOOR`
/// projection in the ORDER-BY resolution list, so positional ordinals stay
/// aligned with the SELECT list. `$`-prefixed names can never be typed as
/// SQL identifiers, so the sentinel is unreachable by name; an ordinal that
/// lands on it fails closed (there is no output column to sort by).
const UNALIASED_TIME_KEY: &str = "$unaliased_time_floor";

/// The native not-null selector filter used for null-aware counting:
/// `{"type":"not","field":{"type":"selector","dimension":C,"value":null}}`
/// counts exactly the non-null rows of `C` when wrapped around a `count`
/// aggregator via `AggregatorSpec::Filtered`.
fn not_null_selector_filter(column: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "not",
        "field": {"type": "selector", "dimension": column, "value": null}
    })
}

/// E16: the EXACT `COUNT(DISTINCT col)` lowering used when the SQL context
/// sets `useApproximateCountDistinct: false` — a visible single-field
/// `cardinality` aggregation (`byRow: false`, an exact `HashSet` distinct
/// count with a broker set-union merge) wrapped in the same not-null
/// `Filtered` the `COUNT(col)` lowering uses.
///
/// The not-null wrapper is REQUIRED for Druid parity: SQL `COUNT(DISTINCT)`
/// ignores NULLs (Druid returns 3 for the Section-7 `nulltest` device_id
/// column), while the bare cardinality aggregator counts a `__null__`
/// sentinel key as a distinct value (it would return 4).
///
/// Exactness bound (FAIL-CLOSED as of 2026-07-11, exact multi-shard
/// union as of 2026-07-12 — see [`PlannerOptions`] for the full
/// contract): the exact distinct set caps at
/// `ferrodruid_aggregator::MAX_CARDINALITY_SET_SIZE` (1,000,000), a
/// single unified bound covering the per-aggregator set and the broker's
/// cross-segment set union (per-shard partials carry the full exact set).
/// Hitting the bound FAILS the query with `DruidError::ResourceLimit`
/// (REST: HTTP 400, `io.druid.query.ResourceLimitExceededException`)
/// instead of silently under-/over-counting; the error message points at
/// `APPROX_COUNT_DISTINCT` as the unbounded-cardinality alternative.
fn exact_count_distinct_aggregator(name: String, column: String) -> AggregatorSpec {
    AggregatorSpec::Filtered {
        filter: not_null_selector_filter(&column),
        aggregator: Box::new(AggregatorSpec::Cardinality {
            name,
            fields: vec![column],
            by_row: Some(false),
        }),
    }
}

impl AggProjections {
    /// `true` when the SELECT list contains any aggregate-derived output.
    fn has_aggregation(&self) -> bool {
        !self.specs.is_empty() || !self.post_aggs.is_empty()
    }

    /// The full aggregation list for the native query: visible aggregations
    /// first (matching [`AggProjections::aliases`] order), hidden helper
    /// aggregations after.
    fn native_aggregations(&self) -> Vec<AggregatorSpec> {
        let mut all = self.specs.clone();
        all.extend(self.hidden_specs.iter().cloned());
        all
    }

    /// The post-aggregation list for the native query (`None` when empty).
    fn native_post_aggregations(&self) -> Option<Vec<PostAggregatorSpec>> {
        if self.post_aggs.is_empty() {
            None
        } else {
            Some(self.post_aggs.clone())
        }
    }

    /// Append this block's output columns (in SELECT projection order) to
    /// `out`. Hidden helper aggregations are internal and never emitted.
    /// Outputs with a recorded per-output SQL type carry it (`DOUBLE` for
    /// AVG / arithmetic / function-over-aggregate, `BIGINT` for
    /// count-distinct estimates, `TIMESTAMP` for `MIN`/`MAX` over the time
    /// column); everything else stays `BIGINT`.
    fn push_output_columns(&self, out: &mut Vec<OutputColumn>) {
        for name in &self.output_names {
            out.push(self.output_column_for(name));
        }
    }

    /// The [`OutputColumn`] for a single aggregate-derived output name:
    /// outputs with a recorded SQL type override (post-aggregations,
    /// `MIN`/`MAX` over the time column) carry it, everything else is
    /// `BIGINT`.
    fn output_column_for(&self, name: &str) -> OutputColumn {
        let sql_type = self
            .post_agg_types
            .iter()
            .find(|(n, _)| n == name)
            .map_or(SqlType::Bigint, |(_, t)| t.clone());
        OutputColumn {
            name: name.to_string(),
            sql_type,
            source: None,
        }
    }

    /// A fresh unique *internal* name (`$`-prefixed) with the given tag.
    fn fresh_hidden_name(&mut self, tag: &str) -> String {
        let n = self.hidden_seq;
        self.hidden_seq += 1;
        format!("${tag}_{n}")
    }

    /// A fresh default *output* name for an unaliased lowered projection.
    fn fresh_output_name(&mut self, tag: &str) -> String {
        let n = self.hidden_seq;
        self.hidden_seq += 1;
        format!("{tag}_{n}")
    }

    /// Push the hidden helper aggregations for `AVG(field)` and return the
    /// `(sum_name, count_name)` pair: a hidden `doubleSum` over `field` plus
    /// a hidden **not-null-filtered** `count` — SQL-standard AVG ignores
    /// NULLs, so the denominator counts only the non-null rows of `field`
    /// (measured against Druid 35/36, 2026-07-11).
    fn push_avg_helpers(&mut self, field: String) -> (String, String) {
        let n = self.hidden_seq;
        self.hidden_seq += 1;
        let sum_name = format!("$avg_sum_{n}");
        let count_name = format!("$avg_count_{n}");
        self.hidden_specs.push(AggregatorSpec::DoubleSum {
            name: sum_name.clone(),
            field_name: field.clone(),
        });
        self.hidden_specs.push(AggregatorSpec::Filtered {
            filter: not_null_selector_filter(&field),
            aggregator: Box::new(AggregatorSpec::Count {
                name: count_name.clone(),
            }),
        });
        (sum_name, count_name)
    }

    /// Lower `AVG(field)` into hidden `doubleSum` + not-null-filtered `count`
    /// helpers finalised by an **`expression` post-aggregation**
    /// `"$avg_sum_N" / "$avg_count_N"` named `output_name`.
    ///
    /// The expression post-aggregator's `0/0 -> None -> SQL null` matches
    /// Druid, which returns null for `AVG` over an all-null group. (The
    /// `arithmetic` post-aggregator's documented divide-by-zero -> 0 would
    /// return 0 there; plain SQL `/` arithmetic keeps that behaviour — only
    /// AVG uses the expression form.)
    ///
    /// **W-B legacy null mode (H2 scoping)**: under the latch, legacy Druid
    /// answers 0 for AVG over an EMPTY set (oracle `empty_avg_y.json`;
    /// ANSI: null), so the latched lowering emits an **arithmetic `/`
    /// post-aggregation** over the SAME hidden helpers instead — the
    /// divide-by-zero → 0 rule then belongs to AVG's own post-agg
    /// (Druid arithmetic post-agg semantics, pre-existing and
    /// mode-independent in the evaluator), never to the generic
    /// expression evaluator, whose division stays ordinary in every mode
    /// (a global 0/0 → 0 exception there corrupted every legacy
    /// expression containing 0/0, e.g. `ROUND(SUM(x)/COUNT(*), 2)` over
    /// an empty match).  The ANSI plan JSON stays byte-for-byte
    /// unchanged.
    fn lower_avg(&mut self, field: String, output_name: String) -> PostAggregatorSpec {
        let (sum_name, count_name) = self.push_avg_helpers(field);
        if ferrodruid_common::legacy_null_mode() {
            return PostAggregatorSpec::Arithmetic {
                name: output_name,
                fn_name: "/".to_string(),
                fields: vec![
                    PostAggregatorSpec::FieldAccess {
                        name: sum_name.clone(),
                        field_name: sum_name,
                    },
                    PostAggregatorSpec::FieldAccess {
                        name: count_name.clone(),
                        field_name: count_name,
                    },
                ],
            };
        }
        PostAggregatorSpec::Expression {
            name: output_name,
            expression: format!("(\"{sum_name}\" / \"{count_name}\")"),
        }
    }

    /// Lower `COUNT(DISTINCT field)` / `APPROX_COUNT_DISTINCT(field)` into a
    /// hidden `HLLSketchBuild` finalised by an `HLLSketchEstimate` with
    /// `round: true` (BIGINT result). This matches Druid's DEFAULT
    /// (`useApproximateCountDistinct=true` → approximate); E16: with the
    /// context flag OFF, `COUNT(DISTINCT col)` instead lowers to the exact
    /// `cardinality` aggregation (see [`exact_count_distinct_aggregator`])
    /// and never reaches this helper, while `APPROX_COUNT_DISTINCT` keeps
    /// using it in both modes. The sketch build skips nulls, matching Druid
    /// (nulls are not counted).
    fn lower_count_distinct(&mut self, field: String, output_name: String) -> PostAggregatorSpec {
        let n = self.hidden_seq;
        self.hidden_seq += 1;
        let sketch_name = format!("$hll_{n}");
        self.hidden_specs.push(AggregatorSpec::HllSketchBuild {
            name: sketch_name.clone(),
            field_name: field,
            lg_k: None,
            // SQL finalizes via the explicit HLLSketchEstimate post-agg; the
            // per-aggregator native flag stays at its default.
            should_finalize: None,
        });
        PostAggregatorSpec::HllSketchEstimate {
            name: output_name,
            field: Box::new(PostAggregatorSpec::FieldAccess {
                name: sketch_name.clone(),
                field_name: sketch_name,
            }),
            round: Some(true),
        }
    }
}

/// The Druid post-aggregation `fn` string for an arithmetic SQL operator,
/// or `None` for comparison operators.
fn arithmetic_fn_name(op: &BinaryOperator) -> Option<&'static str> {
    match op {
        BinaryOperator::Plus => Some("+"),
        BinaryOperator::Minus => Some("-"),
        BinaryOperator::Multiply => Some("*"),
        BinaryOperator::Divide => Some("/"),
        _ => None,
    }
}

/// `true` when an expression tree contains an aggregate call, so the
/// projection is aggregate-derived and must lower to a post-aggregation
/// (or fail closed) rather than be treated as a grouping dimension.
/// Descends binary arithmetic and the ROUND/ABS/FLOOR/CEIL function
/// wrappers (T4: function-over-aggregate projections).
fn arithmetic_contains_aggregate(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Aggregate { .. } => true,
        SqlExpr::Function(f) => match f {
            DruidFunction::Earliest(_)
            | DruidFunction::Latest(_)
            | DruidFunction::AnyValue(_)
            | DruidFunction::EarliestBy { .. }
            | DruidFunction::LatestBy { .. }
            | DruidFunction::ApproxCountDistinct { .. }
            | DruidFunction::ApproxQuantileDs { .. } => true,
            DruidFunction::Round { expr: inner, .. }
            | DruidFunction::Abs(inner)
            | DruidFunction::Floor(inner)
            | DruidFunction::Ceil(inner) => arithmetic_contains_aggregate(inner),
            _ => false,
        },
        SqlExpr::BinaryOp { left, right, .. } => {
            arithmetic_contains_aggregate(left) || arithmetic_contains_aggregate(right)
        }
        _ => false,
    }
}

/// W2-C + null-semantics T3/T4: try to lower a SELECT projection into a
/// post-aggregation over hidden helper aggregations. On success also returns
/// the output's SQL type (`DOUBLE` for AVG / arithmetic / function-over-
/// aggregate, `BIGINT` for count-distinct estimates).
///
/// Shapes that lower:
/// - `AVG(col)` — hidden `doubleSum` + not-null-filtered `count` finalised
///   by an `expression` post-aggregation (see [`AggProjections::lower_avg`]).
/// - `COUNT(DISTINCT col)` / `APPROX_COUNT_DISTINCT(col)` — hidden
///   `HLLSketchBuild` + `HLLSketchEstimate { round: true }` (see
///   [`AggProjections::lower_count_distinct`]). Matches Druid's default
///   approximate mode. E16: in exact mode
///   (`useApproximateCountDistinct=false`) a top-level `COUNT(DISTINCT
///   col)` is intercepted by `try_as_aggregation` (visible `cardinality`
///   aggregation) and never reaches this function; the COUNT-DISTINCT arm
///   below therefore only fires in approximate mode, while
///   `APPROX_COUNT_DISTINCT` uses it in both modes.
/// - binary arithmetic (`+ - * /`) whose tree contains at least one
///   aggregate, e.g. `SUM(a) / COUNT(*)`, `SUM(a) * 100`,
///   `(SUM(a) + SUM(b)) / COUNT(*)` — each aggregate operand becomes a
///   hidden aggregation accessed via `fieldAccess`, numeric literals become
///   `constant` operands, and nested arithmetic recurses.
/// - `ROUND(expr[, n])` / `ABS(expr)` / `FLOOR(expr)` / `CEIL(expr)` over an
///   aggregate-bearing expression — rendered into an `expression`
///   post-aggregation string (`round("$avg_sum_0" / "$avg_count_0", 1)`).
///
/// Returns `Ok(None)` when `expr` is not aggregate-derived (it may then be a
/// grouping dimension). An aggregate-derived expression that cannot be
/// lowered returns an error — fail closed, never silently dropped.
fn try_as_post_aggregation(
    expr: &SqlExpr,
    alias: &Option<String>,
    aggs: &mut AggProjections,
    schema: Option<&DataSourceSchema>,
) -> Result<Option<(PostAggregatorSpec, SqlType)>> {
    match expr {
        SqlExpr::Aggregate {
            func,
            arg,
            distinct,
        } if func == "AVG" => {
            if *distinct {
                return Err(DruidError::Query(
                    "AVG(DISTINCT ...) is not supported (distinct aggregation is not implemented)"
                        .to_owned(),
                ));
            }
            let Some(field) = arg_to_field_name(arg) else {
                return Err(DruidError::Query(
                    "AVG(...) argument must be a bare column reference".to_owned(),
                ));
            };
            let name = alias.clone().unwrap_or_else(|| format!("avg_{field}"));
            let post = aggs.lower_avg(field.clone(), name.clone());
            aggs.avg_sources.push((field, name));
            Ok(Some((post, SqlType::Double)))
        }
        // T3: COUNT(DISTINCT col) — `try_as_aggregation` deliberately
        // returned None for this shape so it lands here.
        SqlExpr::Aggregate {
            func,
            arg,
            distinct: true,
        } if func == "COUNT" => {
            let Some(field) = arg_to_field_name(arg) else {
                return Err(DruidError::Query(
                    "COUNT(DISTINCT ...) argument must be a bare column reference".to_owned(),
                ));
            };
            let name = alias
                .clone()
                .unwrap_or_else(|| format!("count_distinct_{field}"));
            let post = aggs.lower_count_distinct(field, name);
            Ok(Some((post, SqlType::Bigint)))
        }
        // T3: APPROX_COUNT_DISTINCT(col) — same lowering as COUNT(DISTINCT).
        SqlExpr::Function(DruidFunction::ApproxCountDistinct { expr: inner }) => {
            let SqlExpr::Column(field) = inner.as_ref() else {
                return Err(DruidError::Query(
                    "APPROX_COUNT_DISTINCT(...) argument must be a bare column reference"
                        .to_owned(),
                ));
            };
            let name = alias
                .clone()
                .unwrap_or_else(|| format!("approx_count_distinct_{field}"));
            let post = aggs.lower_count_distinct(field.clone(), name);
            Ok(Some((post, SqlType::Bigint)))
        }
        SqlExpr::BinaryOp { left, op, right } if arithmetic_contains_aggregate(expr) => {
            let Some(fn_name) = arithmetic_fn_name(op) else {
                // A comparison over aggregates is not a projection the planner
                // supports; report it rather than silently dropping it.
                return Err(DruidError::Query(format!(
                    "unsupported operator {op:?} over aggregate expressions in the SELECT \
                     list; only + - * / arithmetic is supported"
                )));
            };
            let lhs = lower_post_agg_operand(left, aggs, schema)?;
            let rhs = lower_post_agg_operand(right, aggs, schema)?;
            let name = alias
                .clone()
                .unwrap_or_else(|| aggs.fresh_output_name("expr"));
            Ok(Some((
                PostAggregatorSpec::Arithmetic {
                    name,
                    fn_name: fn_name.to_owned(),
                    fields: vec![lhs, rhs],
                },
                SqlType::Double,
            )))
        }
        // T4: ROUND / ABS / FLOOR / CEIL over an aggregate-bearing expression
        // renders to an `expression` post-aggregation string (the executor
        // grammar supports exactly these functions; null propagates).
        SqlExpr::Function(
            DruidFunction::Round { .. }
            | DruidFunction::Abs(_)
            | DruidFunction::Floor(_)
            | DruidFunction::Ceil(_),
        ) if arithmetic_contains_aggregate(expr) => {
            let expression = lower_aggregate_expr_string(expr, aggs, schema)?;
            let name = alias
                .clone()
                .unwrap_or_else(|| aggs.fresh_output_name("expr"));
            Ok(Some((
                PostAggregatorSpec::Expression { name, expression },
                SqlType::Double,
            )))
        }
        _ => Ok(None),
    }
}

/// T4: render an aggregate-bearing expression into the executor's
/// `expression` post-aggregation grammar (quoted field refs over hidden
/// helper aggregations, numeric literals, `+ - * /`, and
/// `round/abs/floor/ceil`), pushing the hidden aggregations it needs.
/// Anything outside the grammar fails closed.
fn lower_aggregate_expr_string(
    expr: &SqlExpr,
    aggs: &mut AggProjections,
    schema: Option<&DataSourceSchema>,
) -> Result<String> {
    match expr {
        SqlExpr::Literal(SqlLiteral::Integer(v)) => Ok(v.to_string()),
        SqlExpr::Literal(SqlLiteral::Float(v)) => {
            if v.is_finite() {
                Ok(format!("{v}"))
            } else {
                Err(DruidError::Query(format!(
                    "constant {v} is not a finite number usable in an aggregate expression"
                )))
            }
        }
        // AVG(col) renders as its sum/count division fragment.
        SqlExpr::Aggregate {
            func,
            arg,
            distinct: false,
        } if func == "AVG" => {
            let Some(field) = arg_to_field_name(arg) else {
                return Err(DruidError::Query(
                    "AVG(...) argument must be a bare column reference".to_owned(),
                ));
            };
            let (sum_name, count_name) = aggs.push_avg_helpers(field);
            Ok(format!("(\"{sum_name}\" / \"{count_name}\")"))
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let Some(fn_name) = arithmetic_fn_name(op) else {
                return Err(DruidError::Query(format!(
                    "unsupported operator {op:?} inside a function-over-aggregate \
                     expression; only + - * / are supported"
                )));
            };
            let lhs = lower_aggregate_expr_string(left, aggs, schema)?;
            let rhs = lower_aggregate_expr_string(right, aggs, schema)?;
            Ok(format!("({lhs} {fn_name} {rhs})"))
        }
        SqlExpr::Function(DruidFunction::Round {
            expr: inner,
            digits,
        }) => {
            let inner = lower_aggregate_expr_string(inner, aggs, schema)?;
            Ok(format!("round({inner}, {digits})"))
        }
        SqlExpr::Function(DruidFunction::Abs(inner)) => {
            let inner = lower_aggregate_expr_string(inner, aggs, schema)?;
            Ok(format!("abs({inner})"))
        }
        SqlExpr::Function(DruidFunction::Floor(inner)) => {
            let inner = lower_aggregate_expr_string(inner, aggs, schema)?;
            Ok(format!("floor({inner})"))
        }
        SqlExpr::Function(DruidFunction::Ceil(inner)) => {
            let inner = lower_aggregate_expr_string(inner, aggs, schema)?;
            Ok(format!("ceil({inner})"))
        }
        // Any other aggregate the planner can lower becomes a hidden
        // aggregation referenced by name (COUNT(*), SUM/MIN/MAX, COUNT(col)
        // via the filtered count, the EARLIEST/LATEST family, ...).
        SqlExpr::Aggregate { .. } | SqlExpr::Function(_) => {
            let hidden_name = aggs.fresh_hidden_name("agg");
            match try_as_aggregation(
                expr,
                &Some(hidden_name.clone()),
                0,
                schema,
                aggs.exact_count_distinct,
            )? {
                Some(spec) => {
                    aggs.hidden_specs.push(spec);
                    Ok(format!("\"{hidden_name}\""))
                }
                None => Err(DruidError::Query(format!(
                    "unsupported operand inside a function-over-aggregate expression: \
                     {expr:?}; operands must be supported aggregates, numeric constants, \
                     + - * / arithmetic, or round/abs/floor/ceil over them"
                ))),
            }
        }
        other => Err(DruidError::Query(format!(
            "unsupported operand inside a function-over-aggregate expression: {other:?}; \
             operands must be supported aggregates, numeric constants, + - * / \
             arithmetic, or round/abs/floor/ceil over them"
        ))),
    }
}

/// Lower a single operand of an arithmetic-over-aggregates expression into a
/// post-aggregation node, pushing any hidden helper aggregation it needs.
fn lower_post_agg_operand(
    expr: &SqlExpr,
    aggs: &mut AggProjections,
    schema: Option<&DataSourceSchema>,
) -> Result<PostAggregatorSpec> {
    match expr {
        SqlExpr::Literal(SqlLiteral::Integer(v)) => Ok(PostAggregatorSpec::Constant {
            name: aggs.fresh_hidden_name("const"),
            value: serde_json::Number::from(*v),
        }),
        SqlExpr::Literal(SqlLiteral::Float(v)) => {
            let value = serde_json::Number::from_f64(*v).ok_or_else(|| {
                DruidError::Query(format!(
                    "constant {v} is not a finite number usable in an aggregate arithmetic \
                     expression"
                ))
            })?;
            Ok(PostAggregatorSpec::Constant {
                name: aggs.fresh_hidden_name("const"),
                value,
            })
        }
        SqlExpr::Aggregate {
            func,
            arg,
            distinct,
        } if func == "AVG" => {
            if *distinct {
                return Err(DruidError::Query(
                    "AVG(DISTINCT ...) is not supported (distinct aggregation is not implemented)"
                        .to_owned(),
                ));
            }
            let Some(field) = arg_to_field_name(arg) else {
                return Err(DruidError::Query(
                    "AVG(...) argument must be a bare column reference".to_owned(),
                ));
            };
            // A nested AVG operand is internal — its expression node name is
            // `$`-prefixed and it is NOT recorded in `avg_sources` (only
            // top-level AVG projections are ORDER BY-addressable).
            let name = aggs.fresh_hidden_name("avg");
            Ok(aggs.lower_avg(field, name))
        }
        // T3: COUNT(DISTINCT col) / APPROX_COUNT_DISTINCT(col) as an
        // arithmetic operand — the HLL estimate node nests directly. E16:
        // in exact mode (`useApproximateCountDistinct=false`) COUNT
        // (DISTINCT col) becomes a hidden not-null-filtered `cardinality`
        // aggregation referenced through a `fieldAccess` node instead;
        // APPROX_COUNT_DISTINCT stays on the HLL path in both modes.
        SqlExpr::Aggregate {
            func,
            arg,
            distinct: true,
        } if func == "COUNT" => {
            let Some(field) = arg_to_field_name(arg) else {
                return Err(DruidError::Query(
                    "COUNT(DISTINCT ...) argument must be a bare column reference".to_owned(),
                ));
            };
            if aggs.exact_count_distinct {
                let hidden_name = aggs.fresh_hidden_name("card");
                aggs.hidden_specs
                    .push(exact_count_distinct_aggregator(hidden_name.clone(), field));
                return Ok(PostAggregatorSpec::FieldAccess {
                    name: hidden_name.clone(),
                    field_name: hidden_name,
                });
            }
            let name = aggs.fresh_hidden_name("cd");
            Ok(aggs.lower_count_distinct(field, name))
        }
        SqlExpr::Function(DruidFunction::ApproxCountDistinct { expr: inner }) => {
            let SqlExpr::Column(field) = inner.as_ref() else {
                return Err(DruidError::Query(
                    "APPROX_COUNT_DISTINCT(...) argument must be a bare column reference"
                        .to_owned(),
                ));
            };
            let name = aggs.fresh_hidden_name("cd");
            Ok(aggs.lower_count_distinct(field.clone(), name))
        }
        // T4: a ROUND/ABS/FLOOR/CEIL wrapper as an arithmetic operand lowers
        // to a nested `expression` post-aggregation node.
        SqlExpr::Function(
            DruidFunction::Round { .. }
            | DruidFunction::Abs(_)
            | DruidFunction::Floor(_)
            | DruidFunction::Ceil(_),
        ) if arithmetic_contains_aggregate(expr) => {
            let expression = lower_aggregate_expr_string(expr, aggs, schema)?;
            Ok(PostAggregatorSpec::Expression {
                name: aggs.fresh_hidden_name("expr"),
                expression,
            })
        }
        SqlExpr::Aggregate { .. } | SqlExpr::Function(_) => {
            let hidden_name = aggs.fresh_hidden_name("agg");
            match try_as_aggregation(
                expr,
                &Some(hidden_name.clone()),
                0,
                schema,
                aggs.exact_count_distinct,
            )? {
                Some(spec) => {
                    aggs.hidden_specs.push(spec);
                    Ok(PostAggregatorSpec::FieldAccess {
                        name: hidden_name.clone(),
                        field_name: hidden_name,
                    })
                }
                None => Err(DruidError::Query(format!(
                    "unsupported operand in an arithmetic expression over aggregates: \
                     {expr:?}; operands must be supported aggregates, numeric constants, \
                     or + - * / arithmetic over them"
                ))),
            }
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let Some(fn_name) = arithmetic_fn_name(op) else {
                return Err(DruidError::Query(format!(
                    "unsupported operator {op:?} inside an arithmetic expression over \
                     aggregates; only + - * / are supported"
                )));
            };
            let lhs = lower_post_agg_operand(left, aggs, schema)?;
            let rhs = lower_post_agg_operand(right, aggs, schema)?;
            Ok(PostAggregatorSpec::Arithmetic {
                name: aggs.fresh_hidden_name("expr"),
                fn_name: fn_name.to_owned(),
                fields: vec![lhs, rhs],
            })
        }
        other => Err(DruidError::Query(format!(
            "unsupported operand in an arithmetic expression over aggregates: {other:?}; \
             operands must be supported aggregates, numeric constants, or + - * / \
             arithmetic over them"
        ))),
    }
}

/// W2-C (fail closed): refuse to plan a grouped / aggregated query any of
/// whose SELECT projections has no output slot — i.e. it is neither an
/// aggregate-derived output nor one of the GROUP BY keys (nor the TIME_FLOOR
/// time key). Previously such a projection was pushed into the dimension
/// list and then silently vanished from the results (HTTP 200 with the
/// column missing and no error).
fn ensure_projections_grouped(
    dimension_exprs: &[(SqlExpr, Option<String>)],
    group_by_dims: &[String],
) -> Result<()> {
    for (expr, alias) in dimension_exprs {
        // The TIME_FLOOR (or shifted-week) projection is the granular time key.
        if is_time_grain_expr(expr) {
            continue;
        }
        let is_grouping_key = match expr {
            SqlExpr::Column(name) => {
                group_by_dims.iter().any(|d| d == name)
                    || alias
                        .as_ref()
                        .is_some_and(|a| group_by_dims.iter().any(|d| d == a))
            }
            // A non-column projection can only be a grouping key through its
            // alias (a positional `GROUP BY <n>` resolves to the alias).
            _ => alias
                .as_ref()
                .is_some_and(|a| group_by_dims.iter().any(|d| d == a)),
        };
        if !is_grouping_key {
            let label = alias.clone().unwrap_or_else(|| match expr {
                SqlExpr::Column(name) => name.clone(),
                other => format!("{other:?}"),
            });
            return Err(DruidError::Query(format!(
                "SELECT projection `{label}` is neither an aggregate expression the planner \
                 supports nor a GROUP BY key; refusing to plan a query that would silently \
                 drop it from the results. Supported aggregate projections: COUNT(*), \
                 COUNT(col), COUNT(DISTINCT col), APPROX_COUNT_DISTINCT(col), \
                 SUM/MIN/MAX/AVG(col), the EARLIEST/LATEST/ARRAY_AGG families, TIME_FLOOR, \
                 + - * / arithmetic over aggregates and numeric constants, and \
                 ROUND/ABS/FLOOR/CEIL over those"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Join lowering
// ---------------------------------------------------------------------------

/// Plan a SELECT whose FROM clause carries one or more joins.
///
/// The outer SELECT runs over the joined rows. We plan a *join-free* copy of
/// the query — its FROM is the left base table, and every column reference has
/// the left base-table qualifier stripped (so `a.cc` becomes `cc`, matching the
/// unprefixed left columns the join executor emits) while right-side qualifiers
/// are preserved (so `j.v` stays `j.v`, matching the executor's right prefix
/// `j.`). The lowered joins are then attached to the resulting plan.
///
/// `a JOIN b JOIN c` lowers to two [`PlannedJoin`]s applied in order: the first
/// joins the left base to `b`, the second joins that result to `c` (nested join
/// data sources).
fn plan_select_with_joins(
    select: &SelectQuery,
    schema: &DataSourceSchema,
    options: PlannerOptions,
) -> Result<PlannedQuery> {
    // The set of qualifiers that name the *left* (base) side. The base table's
    // alias (or name) plus the schema name all denote the left columns.
    let left_qualifier = select
        .from
        .alias
        .clone()
        .unwrap_or_else(|| select.from.name.clone());
    let left_qualifiers = vec![left_qualifier, schema.name.clone()];

    // Build the join-free copy with qualifiers normalised.
    let mut inner = select.clone();
    inner.from = TableReference {
        name: select.from.name.clone(),
        alias: select.from.alias.clone(),
        subquery: select.from.subquery.clone(),
        joins: Vec::new(),
    };
    normalize_select_columns(&mut inner, &left_qualifiers);

    let mut planned = plan_select(&inner, schema, options)?;

    // Lower each join clause in order.
    let mut planned_joins = Vec::with_capacity(select.from.joins.len());
    for clause in &select.from.joins {
        planned_joins.push(lower_join(clause, schema)?);
    }
    planned.joins = planned_joins;
    Ok(planned)
}

/// Strip the left-side qualifier from every column reference in a SELECT so the
/// join-free inner plan references bare left columns.
fn normalize_select_columns(select: &mut SelectQuery, left_qualifiers: &[String]) {
    for proj in &mut select.projections {
        if let Projection::Expr { expr, .. } = proj {
            normalize_expr_columns(expr, left_qualifiers);
        }
    }
    if let Some(filter) = &mut select.filter {
        normalize_expr_columns(filter, left_qualifiers);
    }
    for g in &mut select.group_by {
        normalize_expr_columns(g, left_qualifiers);
    }
    // GROUPING SETS / CUBE / ROLLUP columns must be normalised the SAME way
    // as `group_by`, otherwise a qualified left key (e.g. `sales.city`) is
    // left intact in `grouping_sets` while `group_by` is stripped to `city`,
    // and `plan_groupby`'s subtotals validation then rejects the set as not
    // in the GROUP BY dimensions (DD R11 #3). These entries are bare column
    // names (the parser already expanded the clause), so apply the same
    // left-qualifier stripping while preserving right-side alias prefixes.
    if let Some(sets) = &mut select.grouping_sets {
        for set in sets {
            for col in set {
                normalize_grouping_set_column(col, left_qualifiers);
            }
        }
    }
    if let Some(having) = &mut select.having {
        normalize_expr_columns(having, left_qualifiers);
    }
    for ob in &mut select.order_by {
        normalize_expr_columns(&mut ob.expr, left_qualifiers);
    }
}

/// Strip a left-side `qualifier.` prefix from a single GROUPING SETS / CUBE /
/// ROLLUP column name. Mirrors the [`SqlExpr::Column`] arm of
/// [`normalize_expr_columns`]: if the column carries one of the left
/// qualifiers, reduce it to the bare column; right-side alias prefixes are
/// left untouched so they keep matching the join executor's right prefix.
fn normalize_grouping_set_column(col: &mut String, left_qualifiers: &[String]) {
    if let Some((qual, bare)) = col.split_once('.')
        && left_qualifiers.iter().any(|q| q == qual)
    {
        *col = bare.to_string();
    }
}

/// Strip a left-side `qualifier.` prefix from a single column reference,
/// recursing into compound expressions. Right-side qualifiers are preserved.
fn normalize_expr_columns(expr: &mut SqlExpr, left_qualifiers: &[String]) {
    match expr {
        SqlExpr::Column(name) => {
            if let Some((qual, col)) = name.split_once('.')
                && left_qualifiers.iter().any(|q| q == qual)
            {
                *name = col.to_string();
            }
        }
        SqlExpr::Aggregate { arg: Some(a), .. } => {
            normalize_expr_columns(a, left_qualifiers);
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            normalize_expr_columns(left, left_qualifiers);
            normalize_expr_columns(right, left_qualifiers);
        }
        SqlExpr::And(a, b) | SqlExpr::Or(a, b) => {
            normalize_expr_columns(a, left_qualifiers);
            normalize_expr_columns(b, left_qualifiers);
        }
        SqlExpr::Not(a) | SqlExpr::IsNull(a) | SqlExpr::IsNotNull(a) => {
            normalize_expr_columns(a, left_qualifiers);
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            normalize_expr_columns(expr, left_qualifiers);
            normalize_expr_columns(low, left_qualifiers);
            normalize_expr_columns(high, left_qualifiers);
        }
        SqlExpr::InList { expr, list, .. } => {
            normalize_expr_columns(expr, left_qualifiers);
            for e in list {
                normalize_expr_columns(e, left_qualifiers);
            }
        }
        SqlExpr::Like { expr, pattern, .. } => {
            normalize_expr_columns(expr, left_qualifiers);
            normalize_expr_columns(pattern, left_qualifiers);
        }
        SqlExpr::Cast { expr, .. } => normalize_expr_columns(expr, left_qualifiers),
        _ => {}
    }
}

/// Lower one [`JoinClause`] into a [`PlannedJoin`].
fn lower_join(clause: &JoinClause, schema: &DataSourceSchema) -> Result<PlannedJoin> {
    let join_type = match clause.join_type {
        SqlJoinType::Inner => ferrodruid_query::JoinType::Inner,
        SqlJoinType::Left => ferrodruid_query::JoinType::Left,
    };

    let right_alias = join_right_alias_for_plan(&clause.right);
    let right_prefix = format!("{right_alias}.");

    let (right, right_native_query) = match &clause.right {
        JoinRightSide::Lookup { lookup, .. } => (
            ferrodruid_query::JoinRight::Lookup {
                lookup: lookup.clone(),
                key_column: "k".to_string(),
                value_column: "v".to_string(),
            },
            None,
        ),
        JoinRightSide::Values {
            column_names, rows, ..
        } => {
            let json_rows: Vec<Vec<serde_json::Value>> = rows
                .iter()
                .map(|row| row.iter().map(literal_to_json).collect())
                .collect();
            (
                ferrodruid_query::JoinRight::Inline {
                    column_names: column_names.clone(),
                    rows: json_rows,
                },
                None,
            )
        }
        JoinRightSide::Subquery { query, .. } => {
            let planned = plan_sql(query, schema)?;
            (
                ferrodruid_query::JoinRight::Rows {
                    column_names: planned
                        .output_columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                    rows: Vec::new(),
                },
                Some(Box::new(planned.native_query)),
            )
        }
        JoinRightSide::Table { name, .. } => {
            // A base-table right side: scan it to materialise the right rows.
            // The executor runs `right_native_query` against the named data
            // source and feeds the resulting rows into the join.
            //
            // `column_names` must list the right relation's *full* projected
            // column set (not just the join key) — the executor builds prefixed
            // columns, null-fill and output columns strictly from it. We resolve
            // the column set from the joined table's schema (supplied via
            // `schema.join_schemas`) exactly as the Subquery arm resolves it from
            // a planned sub-query's `output_columns`. When the joined table's
            // schema is not statically known we fall back to the join key alone
            // (the historical behaviour), which is honest about the missing
            // catalog rather than inventing columns.
            let column_names = schema
                .join_schemas
                .iter()
                .find(|s| s.name == *name)
                .map(DataSourceSchema::projected_column_names)
                .filter(|cols| !cols.is_empty())
                .unwrap_or_else(|| vec![clause.right_key.clone()]);
            let scan = DruidQuery::Scan(ScanQuery {
                data_source: DataSource::Table { name: name.clone() },
                intervals: vec![DEFAULT_INTERVAL.to_string()],
                filter: None,
                virtual_columns: None,
                columns: None,
                limit: None,
                offset: None,
                order: Some("none".to_string()),
                result_format: None,
                context: None,
            });
            (
                ferrodruid_query::JoinRight::Rows {
                    column_names,
                    rows: Vec::new(),
                },
                Some(Box::new(scan)),
            )
        }
    };

    let join = ferrodruid_query::JoinDataSource {
        right,
        right_prefix,
        condition: ferrodruid_query::JoinCondition {
            left_key: clause.left_key.clone(),
            right_key: clause.right_key.clone(),
        },
        join_type,
    };

    Ok(PlannedJoin {
        join,
        right_native_query,
    })
}

/// The right-side alias used as the column prefix for a lowered join.
fn join_right_alias_for_plan(right: &JoinRightSide) -> String {
    match right {
        JoinRightSide::Table { alias, name } => alias.clone().unwrap_or_else(|| name.clone()),
        JoinRightSide::Lookup { alias, lookup } => alias.clone().unwrap_or_else(|| lookup.clone()),
        JoinRightSide::Subquery { alias, .. } | JoinRightSide::Values { alias, .. } => {
            alias.clone().unwrap_or_else(|| "j".to_string())
        }
    }
}

/// Convert a parsed [`SqlLiteral`] into a JSON value for an inline join right.
fn literal_to_json(lit: &SqlLiteral) -> serde_json::Value {
    match lit {
        SqlLiteral::Integer(i) => serde_json::Value::from(*i),
        SqlLiteral::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        SqlLiteral::String(s) => serde_json::Value::String(s.clone()),
        SqlLiteral::Boolean(b) => serde_json::Value::Bool(*b),
        SqlLiteral::Null => serde_json::Value::Null,
        SqlLiteral::Timestamp(ms) => serde_json::Value::from(*ms),
    }
}

// ---------------------------------------------------------------------------
// Window query (Wave 47-D §1)
// ---------------------------------------------------------------------------

/// Lower a SELECT containing one or more `func(...) OVER (...)` calls into
/// a native [`WindowQuery`] wrapping a [`ScanQuery`].
///
/// Each window expression in the projection list becomes one [`WindowSpec`]
/// added to the wrapper.  Non-window projections must be column references
/// (no per-row computed expressions today); the executor reads those
/// straight from the inner scan.
fn plan_window(
    select: &SelectQuery,
    ds: DataSource,
    intervals: Vec<String>,
    schema: &DataSourceSchema,
) -> Result<PlannedQuery> {
    let filter = select.filter.as_ref().map(convert_filter).transpose()?;

    // Collect output columns (in projection order) and window specs.
    // Inner-scan columns are the union of (a) base column projections,
    // (b) all columns referenced by window functions (partition_by /
    // order_by / lag/lead column / sum/avg column).
    let mut output_columns: Vec<OutputColumn> = Vec::new();
    let mut scan_columns: Vec<String> = Vec::new();
    let mut windows: Vec<WindowSpec> = Vec::new();

    let push_scan_col = |cols: &mut Vec<String>, name: &str| {
        if !cols.iter().any(|c| c == name) {
            cols.push(name.to_string());
        }
    };

    for proj in &select.projections {
        match proj {
            Projection::Wildcard => {
                return Err(DruidError::Query(
                    "SELECT * with window functions is not supported; \
                     name the columns explicitly"
                        .to_string(),
                ));
            }
            Projection::Expr { expr, alias } => match expr {
                SqlExpr::Column(name) => {
                    push_scan_col(&mut scan_columns, name);
                    let sql_type = schema
                        .column_type(name)
                        .map(SqlType::from_druid)
                        .unwrap_or(SqlType::Varchar);
                    output_columns.push(OutputColumn {
                        name: alias.clone().unwrap_or_else(|| name.clone()),
                        sql_type,
                        source: None,
                    });
                }
                SqlExpr::Window(wf) => {
                    let output_name = alias.clone().ok_or_else(|| {
                        DruidError::Query(
                            "Window function projections must be aliased \
                                 (e.g. `... OVER (...) AS rn`)"
                                .to_string(),
                        )
                    })?;
                    let (kind, sql_type) = lower_window_function(&wf.function)?;
                    // Add referenced columns to inner scan.
                    for col in &wf.partition_by {
                        push_scan_col(&mut scan_columns, col);
                    }
                    for ob in &wf.order_by {
                        if let SqlExpr::Column(c) = &ob.expr {
                            push_scan_col(&mut scan_columns, c);
                        }
                    }
                    if let Some(col) = window_function_column(&wf.function) {
                        push_scan_col(&mut scan_columns, &col);
                    }

                    let order_by = wf
                        .order_by
                        .iter()
                        .map(|ob| match &ob.expr {
                            SqlExpr::Column(c) => Ok(WindowOrderBy {
                                column: c.clone(),
                                direction: if ob.asc {
                                    SortDirection::Ascending
                                } else {
                                    SortDirection::Descending
                                },
                            }),
                            other => Err(DruidError::Query(format!(
                                "Window ORDER BY only supports column references, got: {other:?}"
                            ))),
                        })
                        .collect::<Result<Vec<_>>>()?;

                    windows.push(WindowSpec {
                        output_name: output_name.clone(),
                        function: kind,
                        partition_by: wf.partition_by.clone(),
                        order_by,
                        frame: wf.frame.as_ref().map(lower_window_frame).transpose()?,
                    });
                    output_columns.push(OutputColumn {
                        name: output_name,
                        sql_type,
                        source: None,
                    });
                }
                other => {
                    return Err(DruidError::Query(format!(
                        "Window-function SELECT only supports column and window \
                         projections, got: {other:?}"
                    )));
                }
            },
        }
    }

    // The outer SQL ORDER BY / LIMIT applies *after* window evaluation so
    // that LAG/LEAD/RANK still see the per-window ORDER BY ordering.
    let mut post_order_by: Vec<WindowOrderBy> = Vec::new();
    for ob in &select.order_by {
        let column = match &ob.expr {
            SqlExpr::Column(name) => name.clone(),
            SqlExpr::Literal(SqlLiteral::Integer(pos)) => {
                let idx = (*pos as usize).saturating_sub(1);
                output_columns
                    .get(idx)
                    .map(|oc| oc.name.clone())
                    .ok_or_else(|| {
                        DruidError::Query(format!(
                            "ORDER BY positional reference {pos} is out of range"
                        ))
                    })?
            }
            other => {
                return Err(DruidError::Query(format!(
                    "Window-query ORDER BY only supports column / positional \
                     references, got: {other:?}"
                )));
            }
        };
        // Make sure post-sort columns that come from the inner scan are
        // included in the scan column projection.
        if scan_columns.iter().any(|c| c == &column)
            || windows.iter().any(|w| w.output_name == column)
        {
            // already projected
        } else if schema.column_type(&column).is_some() {
            scan_columns.push(column.clone());
        }
        post_order_by.push(WindowOrderBy {
            column,
            direction: if ob.asc {
                SortDirection::Ascending
            } else {
                SortDirection::Descending
            },
        });
    }

    let inner = ScanQuery {
        data_source: ds,
        intervals,
        filter,
        virtual_columns: None,
        columns: if scan_columns.is_empty() {
            None
        } else {
            Some(scan_columns)
        },
        limit: None,
        offset: None,
        order: Some("none".to_string()),
        result_format: None,
        context: None,
    };

    let query = DruidQuery::Window(WindowQuery {
        inner,
        windows,
        post_order_by,
        post_limit: select.limit,
        context: None,
    });

    Ok(PlannedQuery {
        native_query: query,
        output_columns,
        joins: Vec::new(),
        time_bucket_column: None,
    })
}

/// Map a parsed [`WindowFunctionType`] to the executor's
/// [`WindowFunctionKind`] plus the SQL output type for the column.
fn lower_window_function(t: &WindowFunctionType) -> Result<(WindowFunctionKind, SqlType)> {
    match t {
        WindowFunctionType::RowNumber => Ok((WindowFunctionKind::RowNumber, SqlType::Bigint)),
        WindowFunctionType::Rank => Ok((WindowFunctionKind::Rank, SqlType::Bigint)),
        WindowFunctionType::DenseRank => Ok((WindowFunctionKind::DenseRank, SqlType::Bigint)),
        WindowFunctionType::Lag {
            column,
            offset,
            default,
        } => {
            // DD R43 (Finding 4): the third `default` argument of
            // `LAG(col, offset, default)` was parsed then silently dropped
            // at lowering (`default: _`), and the executor returns NULL for
            // rows with no row at the offset. Supplying an explicit default
            // therefore produced a silently-wrong NULL instead of the
            // requested value. Carrying the default through the window
            // executor is not implemented, so fail closed rather than ignore
            // it. The two-argument form (no default → NULL outside the
            // partition, matching Druid) is unaffected.
            reject_window_default("LAG", default.as_ref())?;
            Ok((
                WindowFunctionKind::Lag {
                    column: column.clone(),
                    offset: *offset,
                },
                SqlType::Bigint,
            ))
        }
        WindowFunctionType::Lead {
            column,
            offset,
            default,
        } => {
            // DD R43 (Finding 4): see `LAG` above — an explicit `default`
            // argument was silently dropped; fail closed.
            reject_window_default("LEAD", default.as_ref())?;
            Ok((
                WindowFunctionKind::Lead {
                    column: column.clone(),
                    offset: *offset,
                },
                SqlType::Bigint,
            ))
        }
        WindowFunctionType::Sum { column } => Ok((
            WindowFunctionKind::Sum {
                column: column.clone(),
            },
            SqlType::Bigint,
        )),
        WindowFunctionType::Avg { column } => Ok((
            WindowFunctionKind::Avg {
                column: column.clone(),
            },
            SqlType::Double,
        )),
        WindowFunctionType::Min { column } => Ok((
            WindowFunctionKind::Min {
                column: column.clone(),
            },
            SqlType::Bigint,
        )),
        WindowFunctionType::Max { column } => Ok((
            WindowFunctionKind::Max {
                column: column.clone(),
            },
            SqlType::Bigint,
        )),
        WindowFunctionType::Count { column, distinct } => {
            if *distinct {
                return Err(DruidError::Query(
                    "COUNT(DISTINCT col) OVER (...) is not yet supported".to_string(),
                ));
            }
            Ok((
                WindowFunctionKind::Count {
                    column: column.clone(),
                },
                SqlType::Bigint,
            ))
        }
        WindowFunctionType::FirstValue { column } => Ok((
            WindowFunctionKind::FirstValue {
                column: column.clone(),
            },
            SqlType::Bigint,
        )),
        WindowFunctionType::LastValue { column } => Ok((
            WindowFunctionKind::LastValue {
                column: column.clone(),
            },
            SqlType::Bigint,
        )),
        // ----- CL-4 / W1-D window function additions -----
        WindowFunctionType::NthValue { column, n } => Ok((
            WindowFunctionKind::NthValue {
                column: column.clone(),
                n: *n,
            },
            // Output column type follows the underlying column; we report
            // Bigint to match the existing FIRST_VALUE / LAST_VALUE
            // convention so SQL-side projection metadata is consistent.
            SqlType::Bigint,
        )),
        WindowFunctionType::Ntile { tiles } => {
            Ok((WindowFunctionKind::Ntile { tiles: *tiles }, SqlType::Bigint))
        }
        WindowFunctionType::CumeDist => Ok((WindowFunctionKind::CumeDist, SqlType::Double)),
        WindowFunctionType::PercentRank => Ok((WindowFunctionKind::PercentRank, SqlType::Double)),
    }
}

/// Map a parsed [`SqlWindowFrame`] to the executor's [`ExecWindowFrame`].
/// DD R43 (Finding 4): reject an explicit, non-default `default` argument on
/// `LAG`/`LEAD`. The window executor cannot carry the default value, so an
/// explicit one would be silently ignored (returning NULL outside the
/// partition). A bare `NULL` default is equivalent to the implicit behaviour
/// and is therefore allowed through.
fn reject_window_default(func: &str, default: Option<&serde_json::Value>) -> Result<()> {
    match default {
        None | Some(serde_json::Value::Null) => Ok(()),
        Some(_) => Err(DruidError::Query(format!(
            "{func}(col, offset, default) with an explicit default value is not supported \
             (the default would be silently ignored); omit the third argument to get NULL \
             outside the partition"
        ))),
    }
}

fn lower_window_frame(frame: &SqlWindowFrame) -> Result<ExecWindowFrame> {
    Ok(ExecWindowFrame {
        mode: match frame.mode {
            SqlFrameMode::Rows => ExecFrameMode::Rows,
            SqlFrameMode::Range => ExecFrameMode::Range,
        },
        start: lower_frame_bound(&frame.start),
        end: lower_frame_bound(&frame.end),
    })
}

fn lower_frame_bound(bound: &SqlFrameBound) -> ExecFrameBound {
    match bound {
        SqlFrameBound::UnboundedPreceding => ExecFrameBound::UnboundedPreceding,
        SqlFrameBound::Preceding(n) => ExecFrameBound::Preceding { n: *n },
        SqlFrameBound::CurrentRow => ExecFrameBound::CurrentRow,
        SqlFrameBound::Following(n) => ExecFrameBound::Following { n: *n },
        SqlFrameBound::UnboundedFollowing => ExecFrameBound::UnboundedFollowing,
    }
}

/// Return the input column name a window function reads from, if any.
fn window_function_column(t: &WindowFunctionType) -> Option<String> {
    match t {
        WindowFunctionType::Lag { column, .. }
        | WindowFunctionType::Lead { column, .. }
        | WindowFunctionType::Sum { column }
        | WindowFunctionType::Avg { column }
        | WindowFunctionType::Min { column }
        | WindowFunctionType::Max { column }
        | WindowFunctionType::FirstValue { column }
        | WindowFunctionType::LastValue { column } => Some(column.clone()),
        WindowFunctionType::Count { column, .. } => column.clone(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Scan query
// ---------------------------------------------------------------------------

fn plan_scan(
    select: &SelectQuery,
    ds: DataSource,
    intervals: Vec<String>,
    filter: Option<FilterSpec>,
    schema: &DataSourceSchema,
    is_wildcard: bool,
) -> Result<PlannedQuery> {
    let columns = if is_wildcard {
        None
    } else {
        // DD R43 (Finding 1): the projection list was previously built with a
        // `filter_map` that kept only bare `SqlExpr::Column` references and
        // silently discarded every other projection. When *all* projections
        // were non-column expressions the resulting `cols` was empty and
        // collapsed to `columns: None` (= return all physical columns), so
        // `SELECT UPPER(city) FROM sales` silently returned every column of
        // `sales` instead of the computed expression. Expression projections
        // in a bare scan (no GROUP BY) are not supported, so reject them
        // rather than return wrong data.
        // (raw column, output name) per projection — codex QA r12: scan
        // aliases must surface on the wire. Native scan rows key by the RAW
        // column (scan has no outputName concept), so an aliased projection
        // records the raw key in `OutputColumn::source` and the wire
        // projection re-keys it. Duplicate aliases of one column each get
        // their own output entry reading the same source.
        let mut cols: Vec<(String, String)> = Vec::with_capacity(select.projections.len());
        for p in &select.projections {
            match p {
                Projection::Expr {
                    expr: SqlExpr::Column(name),
                    alias,
                } => cols.push((name.clone(), alias.clone().unwrap_or_else(|| name.clone()))),
                Projection::Wildcard => {} // handled by `is_wildcard` above
                Projection::Expr { .. } => {
                    return Err(DruidError::Query(
                        "expression projections in a bare scan (SELECT of a non-column \
                         expression without GROUP BY) are not supported; select bare columns, \
                         or add a GROUP BY to aggregate"
                            .to_owned(),
                    ));
                }
            }
        }
        if cols.is_empty() { None } else { Some(cols) }
    };

    let output_columns: Vec<OutputColumn> = if let Some(ref cols) = columns {
        cols.iter()
            .map(|(c, out)| OutputColumn {
                name: out.clone(),
                // T7: the time column is a SQL TIMESTAMP (rendered as an
                // ISO-8601 string on the SQL wire), matching the wildcard
                // arm below and Druid's scan output.
                sql_type: if *c == schema.time_column {
                    SqlType::Timestamp
                } else {
                    schema
                        .column_type(c)
                        .map(SqlType::from_druid)
                        .unwrap_or(SqlType::Varchar)
                },
                source: (c != out).then(|| c.clone()),
            })
            .collect()
    } else {
        // All columns. A time-less datasource carries an empty `time_column`
        // (no segment has `__time`), so `SELECT *` exposes no time column —
        // matching what `INFORMATION_SCHEMA` reports for the same datasource.
        let mut out = Vec::new();
        if !schema.time_column.is_empty() {
            out.push(OutputColumn {
                name: schema.time_column.clone(),
                sql_type: SqlType::Timestamp,
                source: None,
            });
        }
        for d in &schema.dimensions {
            out.push(OutputColumn {
                name: d.name.clone(),
                sql_type: SqlType::from_druid(&d.column_type),
                source: None,
            });
        }
        for m in &schema.metrics {
            out.push(OutputColumn {
                name: m.name.clone(),
                sql_type: SqlType::from_druid(&m.column_type),
                source: None,
            });
        }
        out
    };

    // T5 (fail closed): a scan of a base TABLE can only be ordered by the
    // time column. Druid rejects `ORDER BY <non-time column>` on a table
    // scan with HTTP 400 ("SQL query requires ordering a table by non-time
    // column [[col]], which is not supported"); FerroDruid previously took
    // only the DIRECTION of the first key and silently returned
    // time-ordered rows. Every key must resolve to the time column — bare,
    // via a SELECT alias, or via a 1-based ordinal. A sub-query / CTE FROM
    // is exempt: Druid CAN order a scan over a sub-query relation by any
    // column (and that path stays fail-closed downstream until CL-4-R8
    // wires the nested executor).
    let order = if select.order_by.is_empty() {
        None
    } else if select.from.subquery.is_some() {
        let first = &select.order_by[0];
        if first.asc {
            Some("ascending".to_string())
        } else {
            Some("descending".to_string())
        }
    } else {
        // SELECT aliases of bare-column projections (`__time AS t`).
        let aliases: Vec<(&String, &String)> = select
            .projections
            .iter()
            .filter_map(|p| match p {
                Projection::Expr {
                    expr: SqlExpr::Column(name),
                    alias: Some(a),
                } => Some((a, name)),
                _ => None,
            })
            .collect();
        for ob in &select.order_by {
            let resolved: String = match &ob.expr {
                SqlExpr::Column(name) => aliases
                    .iter()
                    .find(|(a, _)| *a == name)
                    .map_or_else(|| name.clone(), |(_, col)| (*col).clone()),
                SqlExpr::Positional(pos) => scan_order_ordinal(*pos, &output_columns)?,
                SqlExpr::Literal(SqlLiteral::Integer(pos)) => {
                    let ordinal = usize::try_from(*pos).map_err(|_| {
                        DruidError::Query(format!("ORDER BY position {pos} is not a valid ordinal"))
                    })?;
                    scan_order_ordinal(ordinal, &output_columns)?
                }
                other => format!("{other:?}"),
            };
            if resolved != schema.time_column {
                return Err(DruidError::Query(format!(
                    "SQL query requires ordering a table by non-time column [[{resolved}]], \
                     which is not supported"
                )));
            }
        }
        let first = &select.order_by[0];
        if first.asc {
            Some("ascending".to_string())
        } else {
            Some("descending".to_string())
        }
    };

    // The native scan reads RAW columns (deduplicated — duplicate aliases
    // of one column read it once; the wire projection fans it out).
    let native_columns: Option<Vec<String>> = columns.as_ref().map(|cols| {
        let mut raw: Vec<String> = Vec::with_capacity(cols.len());
        for (c, _) in cols {
            if !raw.contains(c) {
                raw.push(c.clone());
            }
        }
        raw
    });

    let query = DruidQuery::Scan(ScanQuery {
        data_source: ds,
        intervals,
        filter,
        virtual_columns: None,
        columns: native_columns,
        limit: select.limit,
        offset: select.offset,
        order,
        result_format: None,
        context: None,
    });

    Ok(PlannedQuery {
        native_query: query,
        output_columns,
        joins: Vec::new(),
        time_bucket_column: None,
    })
}

/// Resolve a 1-based scan ORDER BY ordinal to the output column it names.
fn scan_order_ordinal(pos: usize, output_columns: &[OutputColumn]) -> Result<String> {
    if pos == 0 {
        return Err(DruidError::Query(
            "ORDER BY position 0 is invalid (positions are 1-based)".to_owned(),
        ));
    }
    output_columns
        .get(pos - 1)
        .map(|c| c.name.clone())
        .ok_or_else(|| {
            DruidError::Query(format!(
                "ORDER BY position {pos} is out of range for the {} output columns",
                output_columns.len()
            ))
        })
}

// ---------------------------------------------------------------------------
// Timeseries (granularity = all)
// ---------------------------------------------------------------------------

fn plan_timeseries_all(
    ds: DataSource,
    intervals: Vec<String>,
    filter: Option<FilterSpec>,
    aggs: &AggProjections,
) -> Result<PlannedQuery> {
    let mut output_columns = Vec::new();
    aggs.push_output_columns(&mut output_columns);

    let query = DruidQuery::Timeseries(TimeseriesQuery {
        data_source: ds,
        intervals,
        granularity: GranularitySpec::Simple("all".to_string()),
        filter,
        virtual_columns: None,
        aggregations: aggs.native_aggregations(),
        post_aggregations: aggs.native_post_aggregations(),
        descending: None,
        context: None,
    });

    Ok(PlannedQuery {
        native_query: query,
        output_columns,
        joins: Vec::new(),
        time_bucket_column: None,
    })
}

// ---------------------------------------------------------------------------
// Timeseries (with granularity from TIME_FLOOR)
// ---------------------------------------------------------------------------

fn plan_timeseries_granular(
    ds: DataSource,
    intervals: Vec<String>,
    filter: Option<FilterSpec>,
    aggs: &AggProjections,
    granularity: GranularitySpec,
    time_floor_alias: Option<String>,
    select_items: &[SelectOutputItem],
) -> Result<PlannedQuery> {
    // codex QA r5: output columns follow the SELECT list — the time bucket
    // keeps its SELECT position instead of being hardcoded first (the REST
    // layer surfaces the bucket under the Timestamp-typed column wherever it
    // sits). Grouping dimensions cannot appear on this path (it requires
    // `group_by_dims.is_empty()` and non-grouped projections were rejected
    // by `ensure_projections_grouped`).
    let mut output_columns = Vec::with_capacity(select_items.len());
    // The bucket column's ROLE is recorded on the plan (finding C): the
    // REST layer surfaces the envelope timestamp under this name without
    // inferring it from (collision-prone) aggregation names.
    let mut time_bucket_column: Option<String> = None;
    for item in select_items {
        match item {
            SelectOutputItem::TimeKey => {
                let name = time_floor_alias
                    .clone()
                    .unwrap_or_else(|| "timestamp".to_string());
                time_bucket_column = Some(name.clone());
                output_columns.push(OutputColumn {
                    name,
                    sql_type: SqlType::Timestamp,
                    source: None,
                });
            }
            SelectOutputItem::Aggregate(name) => {
                output_columns.push(aggs.output_column_for(name));
            }
            SelectOutputItem::Dimension { .. } => {}
        }
    }

    let query = DruidQuery::Timeseries(TimeseriesQuery {
        data_source: ds,
        intervals,
        granularity,
        filter,
        virtual_columns: None,
        aggregations: aggs.native_aggregations(),
        post_aggregations: aggs.native_post_aggregations(),
        descending: None,
        // SQL `GROUP BY TIME_FLOOR(...)` has GROUP BY semantics: only buckets
        // with matching rows appear. A plain Druid timeseries fills empty
        // buckets, so set skipEmptyBuckets to match SQL (and Druid's SQL layer).
        context: Some(ferrodruid_query::QueryContext {
            skip_empty_buckets: Some(true),
            ..Default::default()
        }),
    });

    Ok(PlannedQuery {
        native_query: query,
        output_columns,
        joins: Vec::new(),
        time_bucket_column,
    })
}

// ---------------------------------------------------------------------------
// TopN
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn try_plan_topn(
    select: &SelectQuery,
    ds: &DataSource,
    intervals: &[String],
    filter: Option<&FilterSpec>,
    aggs: &AggProjections,
    dim_name: &str,
    schema: &DataSourceSchema,
    select_items: &[SelectOutputItem],
) -> Option<PlannedQuery> {
    let threshold = select.limit?;
    let first_order = select.order_by.first()?;

    // Determine which metric to sort by. A post-aggregation output (e.g. an
    // AVG alias) is a valid TopN metric: the executor evaluates
    // post-aggregations before ranking (`TopNQuery::validate_metric` accepts
    // post-agg names).
    let metric_name = order_expr_to_name(first_order, aggs)?;

    let metric = if first_order.asc {
        TopNMetricSpec::Inverted {
            metric: Box::new(TopNMetricSpec::Numeric {
                metric: metric_name.clone(),
            }),
        }
    } else {
        TopNMetricSpec::Numeric {
            metric: metric_name.clone(),
        }
    };

    // codex QA r5: `dim_name` may be the raw column or its SELECT alias
    // (GROUP BY alias / ordinal form). The native dimension reads the RAW
    // column and outputs the SELECT alias (the executor keys result rows by
    // `outputName`, so the REST name-based projection surfaces the alias).
    let (dim_column, dim_output) = resolve_dim_binding(dim_name, select_items);

    // codex QA r10: the dimension projected under SEVERAL aliases needs one
    // emitted key per alias — a native TopN has exactly one dimension
    // output, so fall back to the GroupBy path (which emits one
    // DimensionSpec per distinct alias).
    let distinct_dim_outputs = select_items
        .iter()
        .filter_map(|item| match item {
            SelectOutputItem::Dimension { column, output } if *column == dim_column => {
                Some(output.as_str())
            }
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    if distinct_dim_outputs > 1 {
        return None;
    }

    let dim_type = schema
        .column_type(&dim_column)
        .cloned()
        .unwrap_or(ColumnType::String);

    let dimension = DimensionSpec::Default {
        dimension: dim_column,
        output_name: dim_output.clone(),
        output_type: dim_type.clone(),
    };

    // Output columns follow the SELECT list (the single grouping dimension
    // plus aggregate-derived outputs, in projection order).
    let mut output_columns = Vec::with_capacity(select_items.len());
    for item in select_items {
        match item {
            SelectOutputItem::Dimension { .. } => output_columns.push(OutputColumn {
                name: dim_output.clone(),
                sql_type: SqlType::from_druid(&dim_type),
                source: None,
            }),
            SelectOutputItem::Aggregate(name) => {
                output_columns.push(aggs.output_column_for(name));
            }
            // Unreachable: a TIME_FLOOR projection disqualifies the TopN
            // path (`time_grain.is_none()` guard at the call site).
            SelectOutputItem::TimeKey => {}
        }
    }

    let query = DruidQuery::TopN(TopNQuery {
        data_source: ds.clone(),
        intervals: intervals.to_vec(),
        granularity: GranularitySpec::Simple("all".to_string()),
        dimension,
        threshold,
        metric,
        filter: filter.cloned(),
        virtual_columns: None,
        aggregations: aggs.native_aggregations(),
        post_aggregations: aggs.native_post_aggregations(),
        context: None,
    });

    Some(PlannedQuery {
        native_query: query,
        output_columns,
        joins: Vec::new(),
        time_bucket_column: None,
    })
}

fn order_expr_to_name(order: &OrderByExpr, aggs: &AggProjections) -> Option<String> {
    match &order.expr {
        SqlExpr::Column(name) => {
            if aggs.output_names.contains(name) {
                Some(name.clone())
            } else {
                None
            }
        }
        SqlExpr::Aggregate {
            func,
            arg,
            distinct,
        } => {
            // W2-C: `ORDER BY AVG(col)` resolves to the post-aggregation
            // output of the matching top-level AVG projection.
            if func == "AVG"
                && !*distinct
                && let Some(SqlExpr::Column(col)) = arg.as_deref()
                && let Some((_, out)) = aggs.avg_sources.iter().find(|(c, _)| c == col)
            {
                return Some(out.clone());
            }
            // DISTINCT aggregates (`COUNT(DISTINCT col)`) and
            // `APPROX_COUNT_DISTINCT` do NOT reconstruct to a plain
            // `count_col`/`sum_col` output name — matching them as a TopN
            // metric here would silently order by the WRONG aggregate. Return
            // None so `try_plan_topn` declines and the query lowers to a
            // GroupBy, whose `resolve_group_order_key` matches the exact
            // distinct aggregation by structure (or fails closed).
            if *distinct || func == "APPROX_COUNT_DISTINCT" || func == "COUNT_DISTINCT" {
                return None;
            }
            // Match the aggregate by STRUCTURE against a planned aggregation
            // and return THAT aggregation's actual output alias. Matching by a
            // reconstructed default name would silently pick an unrelated
            // aggregate a user aliased to that name (e.g. `SUM(x) AS cnt` for
            // `ORDER BY COUNT(*)`, or `SUM(x) AS min_c` for `ORDER BY
            // MIN(c)`), so it is NOT used. NEVER fall back to the first
            // aggregate. No structural match → None → lower to GroupBy (which
            // resolves the order key, or fails closed for a case neither path
            // can type without a schema — never a silently wrong metric).
            let _ = (func, arg);
            match try_as_aggregation(&order.expr, &None, 0, None, aggs.exact_count_distinct) {
                Ok(Some(candidate)) => aggs
                    .specs
                    .iter()
                    .position(|planned| agg_spec_matches_ignoring_name(&candidate, planned))
                    .and_then(|i| aggs.aliases.get(i).cloned()),
                _ => None,
            }
        }
        // Any other ORDER BY expression is not a recognizable TopN metric;
        // decline (lower to GroupBy) rather than default to the first agg.
        _ => None,
    }
}

/// DD R43 (Finding 3): resolve a GroupBy ORDER BY key to one of the query's
/// output column names. Accepts a bare column / aggregate alias, a SELECT
/// output alias of a grouping dimension, an aggregate expression that matches
/// a planned aggregation, or a 1-based positional ordinal. Returns
/// [`DruidError::Query`] when the key cannot be resolved to an output column
/// (rather than silently dropping it).
fn resolve_group_order_key(
    ob: &OrderByExpr,
    output_names: &[String],
    aggs: &AggProjections,
    select_aliases: &[(String, Option<String>)],
    dim_bindings: &[(String, String)],
) -> Result<String> {
    match &ob.expr {
        // A bare name: a grouping dimension (by its OUTPUT name — the SELECT
        // alias when aliased), the TIME_FLOOR alias, an aggregate referenced
        // by its output alias (e.g. `ORDER BY cnt`), or the RAW column name
        // of a grouping dimension (standard SQL also resolves ORDER BY
        // against the underlying grouped columns, e.g. `SELECT city AS c ...
        // ORDER BY city`, and a grouped-but-unselected key). The returned
        // name is always the executor's emitted key (the `outputName`).
        SqlExpr::Column(name) => {
            if output_names.contains(name) {
                Ok(name.clone())
            } else if let Some((_, output)) = dim_bindings.iter().find(|(column, _)| column == name)
            {
                Ok(output.clone())
            } else if let Some((_, target)) = select_aliases.iter().find(|(alias, _)| alias == name)
            {
                match target {
                    Some(col) => dim_bindings
                        .iter()
                        .find(|(column, _)| column == col)
                        .map(|(_, output)| output.clone())
                        .ok_or_else(|| {
                            DruidError::Query(format!(
                                "ORDER BY key `{name}` is a SELECT alias for a column that is \
                                 not a grouped output column of this query"
                            ))
                        }),
                    None => Err(DruidError::Query(format!(
                        "ORDER BY key `{name}` is a SELECT alias for an expression that is \
                         not a grouped output column of this query"
                    ))),
                }
            } else {
                Err(DruidError::Query(format!(
                    "ORDER BY key `{name}` does not reference a SELECT output column"
                )))
            }
        }
        // A positional ordinal: `ORDER BY 2`.
        SqlExpr::Positional(pos) => resolve_order_ordinal(*pos, output_names),
        SqlExpr::Literal(SqlLiteral::Integer(pos)) => {
            let ordinal = usize::try_from(*pos).map_err(|_| {
                DruidError::Query(format!("ORDER BY position {pos} is not a valid ordinal"))
            })?;
            resolve_order_ordinal(ordinal, output_names)
        }
        // An aggregate expression: `ORDER BY COUNT(*)` / `ORDER BY SUM(x)`.
        // Reconstruct the aggregation and match it (ignoring its synthetic
        // output name) against a planned aggregation, then return that
        // aggregation's alias.
        SqlExpr::Aggregate { .. } | SqlExpr::Function(_) => {
            // W2-C: `ORDER BY AVG(col)` — AVG lowers to a post-aggregation,
            // so match it against the recorded top-level AVG lowerings rather
            // than the plain aggregations.
            if let SqlExpr::Aggregate {
                func,
                arg,
                distinct: false,
            } = &ob.expr
                && func == "AVG"
                && let Some(SqlExpr::Column(col)) = arg.as_deref()
                && let Some((_, out)) = aggs.avg_sources.iter().find(|(c, _)| c == col)
            {
                return Ok(out.clone());
            }
            match try_as_aggregation(&ob.expr, &None, 0, None, aggs.exact_count_distinct)? {
                Some(candidate) => aggs
                    .specs
                    .iter()
                    .position(|planned| agg_spec_matches_ignoring_name(&candidate, planned))
                    .and_then(|i| aggs.aliases.get(i).cloned())
                    .ok_or_else(|| {
                        DruidError::Query(
                            "ORDER BY aggregate does not match any SELECT aggregate".to_owned(),
                        )
                    }),
                None => Err(DruidError::Query(
                    "unsupported ORDER BY expression in a grouped query".to_owned(),
                )),
            }
        }
        _ => Err(DruidError::Query(
            "unsupported ORDER BY expression in a grouped query".to_owned(),
        )),
    }
}

/// Resolve a 1-based ORDER BY ordinal against the output column list.
fn resolve_order_ordinal(pos: usize, output_names: &[String]) -> Result<String> {
    // DD R44: SQL ordinals are 1-based; `ORDER BY 0` must fail closed rather than
    // `saturating_sub(1)` folding it to the first column.
    if pos == 0 {
        return Err(DruidError::Query(
            "ORDER BY position 0 is invalid (positions are 1-based)".to_owned(),
        ));
    }
    output_names.get(pos - 1).cloned().ok_or_else(|| {
        DruidError::Query(format!(
            "ORDER BY position {pos} is out of range for the {} output columns",
            output_names.len()
        ))
    })
}

/// Compare two aggregator specs for equality ignoring their output `name`, so
/// an ORDER BY aggregate expression can be matched to the planned aggregation
/// regardless of the alias under which the SELECT clause named it. `name`
/// keys are stripped recursively because a wrapped spec (e.g. the
/// `Filtered{not-null} -> Count` that `COUNT(col)` lowers to) nests its
/// output name inside the inner aggregator object.
fn agg_spec_matches_ignoring_name(a: &AggregatorSpec, b: &AggregatorSpec) -> bool {
    fn strip_names(v: &mut serde_json::Value) {
        match v {
            serde_json::Value::Object(obj) => {
                obj.remove("name");
                for child in obj.values_mut() {
                    strip_names(child);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    strip_names(item);
                }
            }
            _ => {}
        }
    }
    let (Ok(mut ja), Ok(mut jb)) = (serde_json::to_value(a), serde_json::to_value(b)) else {
        return false;
    };
    strip_names(&mut ja);
    strip_names(&mut jb);
    ja == jb
}

// ---------------------------------------------------------------------------
// GroupBy
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn plan_groupby(
    select: &SelectQuery,
    ds: DataSource,
    intervals: Vec<String>,
    filter: Option<FilterSpec>,
    aggs: &AggProjections,
    group_by_dims: &[String],
    select_aliases: &[(String, Option<String>)],
    schema: &DataSourceSchema,
    time_grain: Option<&TimeGrain>,
    time_floor_alias: Option<&str>,
    select_items: &[SelectOutputItem],
) -> Result<PlannedQuery> {
    // codex QA r5: resolve every GROUP BY key (raw column, or SELECT alias
    // via the alias / ordinal forms) to its `(native column, output name)`
    // pair. The native dimension reads the RAW column; the executor keys
    // group rows by `outputName`, so the SELECT alias is what surfaces on
    // the wire and what ORDER BY / subtotals must reference.
    let dim_bindings: Vec<(String, String)> = group_by_dims
        .iter()
        .map(|gd| resolve_dim_binding(gd, select_items))
        .collect();

    // The native aggregation list: visible aggregations plus the hidden
    // helpers feeding post-aggregations (W2-C).
    let mut agg_specs = aggs.native_aggregations();
    // CL-4 / W1-H R7: now that the GROUP BY dim list is finalised,
    // patch every `GROUPING(...)` aggregator spec's `group_by_dims`
    // field so the executor's per-row bitmask computation references
    // the correct dim ordering (by OUTPUT name — the executor compares
    // against the active dimensions' `outputName`s).
    finalize_grouping_specs(&mut agg_specs, &dim_bindings);
    let granularity = if let Some(tg) = time_grain {
        tg.resolve()?
    } else {
        GranularitySpec::Simple("all".to_string())
    };

    // codex QA r10: a dimension projected under SEVERAL aliases
    // (`SELECT city AS a, city AS b … GROUP BY city`) emits one native
    // DimensionSpec per DISTINCT output name — grouping is unaffected (a
    // duplicated key adds nothing to the group identity) and the executor
    // emits every alias key, so no alias null-fills on the wire.
    let mut dimensions: Vec<DimensionSpec> = Vec::with_capacity(dim_bindings.len());
    {
        let mut seen_outputs: Vec<&str> = Vec::new();
        for (column, output) in &dim_bindings {
            let col_type = schema
                .column_type(column)
                .cloned()
                .unwrap_or(ColumnType::String);
            // The binding's own output first, then any FURTHER aliases of
            // the same raw column from the SELECT list.
            let more = select_items.iter().filter_map(|item| match item {
                SelectOutputItem::Dimension {
                    column: c,
                    output: o,
                } if c == column && o != output => Some(o.as_str()),
                _ => None,
            });
            for out in std::iter::once(output.as_str()).chain(more) {
                if seen_outputs.contains(&out) {
                    continue;
                }
                seen_outputs.push(out);
                dimensions.push(DimensionSpec::Default {
                    dimension: column.clone(),
                    output_name: out.to_string(),
                    output_type: col_type.clone(),
                });
            }
        }
    }
    let dimensions = dimensions;

    // Convert HAVING clause.
    let having = select.having.as_ref().map(convert_having).transpose()?;

    // Convert ORDER BY + LIMIT into a LimitSpec.
    //
    // DD R43 (Finding 3): the previous `filter_map` kept only ORDER BY *column*
    // references and silently dropped aggregate / aliased / positional order
    // keys, while LIMIT still applied — so `... ORDER BY COUNT(*) DESC LIMIT 1`
    // returned an arbitrary group. Each order key is now resolved to an output
    // column name (a grouping dimension, the TIME_FLOOR alias, an aggregate by
    // its alias or by the aggregate expression, or a positional ordinal); a key
    // that cannot be resolved is rejected rather than dropped.
    let limit_spec = if !select.order_by.is_empty() || select.limit.is_some() {
        // Build the SELECT-ordered output names (codex QA r5) so positional
        // `ORDER BY <n>` counts SELECT positions. An unaliased TIME_FLOOR
        // key occupies its slot with a hidden sentinel to keep ordinals
        // aligned; resolving to it fails closed below.
        let mut output_names: Vec<String> = Vec::with_capacity(select_items.len());
        for item in select_items {
            match item {
                SelectOutputItem::TimeKey => output_names.push(
                    time_floor_alias.map_or_else(|| UNALIASED_TIME_KEY.to_string(), str::to_string),
                ),
                SelectOutputItem::Dimension { output, .. } => output_names.push(output.clone()),
                SelectOutputItem::Aggregate(name) => output_names.push(name.clone()),
            }
        }

        let mut columns: Vec<OrderByColumnSpec> = Vec::with_capacity(select.order_by.len());
        for ob in &select.order_by {
            let name =
                resolve_group_order_key(ob, &output_names, aggs, select_aliases, &dim_bindings)?;
            if name == UNALIASED_TIME_KEY {
                return Err(DruidError::Query(
                    "ORDER BY references the unaliased TIME_FLOOR projection; alias the \
                     TIME_FLOOR to order by the time bucket"
                        .to_owned(),
                ));
            }
            // An aggregate output is numeric; a grouping dimension uses its
            // declared column type (numeric columns sort numerically, others
            // lexicographically) so a string dimension is not coerced through
            // the numeric path (which would tie every value at 0). The
            // resolved name is the dimension's OUTPUT name; schema lookups go
            // through the binding's raw column.
            let is_aggregate = aggs.output_names.contains(&name);
            let dimension_order = if is_aggregate || time_floor_alias == Some(name.as_str()) {
                "numeric"
            } else {
                let raw_column = dim_bindings
                    .iter()
                    .find(|(_, output)| *output == name)
                    .map_or(name.as_str(), |(column, _)| column.as_str());
                match schema.column_type(raw_column) {
                    Some(ColumnType::Long | ColumnType::Float | ColumnType::Double) => "numeric",
                    _ => "lexicographic",
                }
            };
            columns.push(OrderByColumnSpec {
                dimension: name,
                direction: Some(if ob.asc {
                    "ascending".to_string()
                } else {
                    "descending".to_string()
                }),
                dimension_order: Some(dimension_order.to_string()),
            });
        }
        Some(LimitSpec {
            typ: "default".to_string(),
            limit: select.limit,
            columns: if columns.is_empty() {
                None
            } else {
                Some(columns)
            },
        })
    } else {
        None
    };

    // Lower GROUPING SETS / CUBE / ROLLUP to a native `subtotalsSpec`. The
    // parser has already expanded the clause into an explicit list of sets,
    // each named by its dimension columns. Each set must be a subset of the
    // query's grouping dimensions; the native spec carries each dimension's
    // OUTPUT name (the executor resolves subtotal subsets against the
    // declared dimensions' `outputName`s).
    let subtotals_spec = match &select.grouping_sets {
        Some(sets) => {
            let mut native_sets: Vec<Vec<String>> = Vec::with_capacity(sets.len());
            for set in sets {
                let mut native_set: Vec<String> = Vec::with_capacity(set.len());
                for dim in set {
                    let Some((_, output)) = dim_bindings
                        .iter()
                        .find(|(column, output)| column == dim || output == dim)
                    else {
                        return Err(DruidError::Query(format!(
                            "GROUPING SETS / CUBE / ROLLUP references dimension `{dim}` \
                             that is not in the GROUP BY"
                        )));
                    };
                    native_set.push(output.clone());
                }
                native_sets.push(native_set);
            }
            Some(native_sets)
        }
        None => None,
    };

    // codex QA r5: output columns follow the SELECT list — aliased names, in
    // SELECT-projection order. A grouping key that is not in the SELECT list
    // stays a native dimension but surfaces no wire column (Druid emits
    // exactly the SELECT list); an unaliased TIME_FLOOR key likewise has no
    // SQL output name on this path (pre-existing behaviour).
    let mut output_columns: Vec<OutputColumn> = Vec::with_capacity(select_items.len());
    // Role marking (finding C): the aliased TIME_FLOOR key is THE bucket
    // column; its value lives in the native envelope's `timestamp` on this
    // granular path (`time_grain` is always `Some` when a TimeKey exists,
    // so granularity is never `all` here).
    let mut time_bucket_column: Option<String> = None;
    for item in select_items {
        match item {
            SelectOutputItem::TimeKey => {
                if let Some(alias) = time_floor_alias {
                    time_bucket_column = Some(alias.to_string());
                    output_columns.push(OutputColumn {
                        name: alias.to_string(),
                        sql_type: SqlType::Timestamp,
                        source: None,
                    });
                }
            }
            SelectOutputItem::Dimension { column, output } => {
                let col_type = schema
                    .column_type(column)
                    .cloned()
                    .unwrap_or(ColumnType::String);
                output_columns.push(OutputColumn {
                    name: output.clone(),
                    sql_type: SqlType::from_druid(&col_type),
                    source: None,
                });
            }
            SelectOutputItem::Aggregate(name) => {
                output_columns.push(aggs.output_column_for(name));
            }
        }
    }

    let query = DruidQuery::GroupBy(GroupByQuery {
        data_source: ds,
        intervals,
        granularity,
        dimensions,
        filter,
        virtual_columns: None,
        aggregations: agg_specs,
        post_aggregations: aggs.native_post_aggregations(),
        subtotals_spec,
        having,
        limit_spec,
        context: None,
    });

    Ok(PlannedQuery {
        native_query: query,
        output_columns,
        joins: Vec::new(),
        time_bucket_column,
    })
}

// ---------------------------------------------------------------------------
// Helpers: extract aggregation spec from SQL expression
// ---------------------------------------------------------------------------

/// Pick the typed First aggregator (Long / Float / Double / String)
/// that matches the underlying column type for `EARLIEST(expr)` /
/// `EARLIEST(expr, timeCol)` / `EARLIEST(expr, timeCol, maxBytesPerString)`.
///
/// W1-J finding-B follow-up: the previous code unconditionally
/// lowered to `AggregatorSpec::DoubleFirst`, which for a STRING
/// column made the executor read `0.0` per row and report `null` at
/// finalize.  Now we honour the column's declared type, falling back
/// to `DoubleFirst` only when the schema is absent (ORDER BY
/// recursion) or the column is not in the schema.
fn typed_first_aggregator(
    name: String,
    field_name: String,
    time_column: Option<String>,
    schema: Option<&DataSourceSchema>,
) -> AggregatorSpec {
    match schema.and_then(|s| s.column_type(&field_name)) {
        Some(ColumnType::String) => AggregatorSpec::StringFirst {
            name,
            field_name,
            max_string_bytes: None,
            time_column,
        },
        Some(ColumnType::Long) => AggregatorSpec::LongFirst {
            name,
            field_name,
            time_column,
        },
        Some(ColumnType::Float) => AggregatorSpec::FloatFirst {
            name,
            field_name,
            time_column,
        },
        Some(ColumnType::Double) | Some(ColumnType::Complex(_)) | None => {
            AggregatorSpec::DoubleFirst {
                name,
                field_name,
                time_column,
            }
        }
    }
}

/// Mirror of [`typed_first_aggregator`] for the LATEST family.
fn typed_last_aggregator(
    name: String,
    field_name: String,
    time_column: Option<String>,
    schema: Option<&DataSourceSchema>,
) -> AggregatorSpec {
    match schema.and_then(|s| s.column_type(&field_name)) {
        Some(ColumnType::String) => AggregatorSpec::StringLast {
            name,
            field_name,
            max_string_bytes: None,
            time_column,
        },
        Some(ColumnType::Long) => AggregatorSpec::LongLast {
            name,
            field_name,
            time_column,
        },
        Some(ColumnType::Float) => AggregatorSpec::FloatLast {
            name,
            field_name,
            time_column,
        },
        Some(ColumnType::Double) | Some(ColumnType::Complex(_)) | None => {
            AggregatorSpec::DoubleLast {
                name,
                field_name,
                time_column,
            }
        }
    }
}

/// `true` when `field` is the datasource's time column. Without a schema
/// (the `ORDER BY <aggregate>` reconstruction path passes `None`), the
/// reserved Druid name `__time` is used — Druid reserves that name, so the
/// two views can never disagree and an `ORDER BY MIN(__time)` reconstruction
/// matches the SELECT-planned `longMin` aggregation.
fn is_time_column(field: &str, schema: Option<&DataSourceSchema>) -> bool {
    schema.map_or(field == "__time", |s| field == s.time_column)
}

/// Whether `field` is a LONG-typed column in the schema (dimension or
/// metric).  Used ONLY by the W-B legacy-null MIN/MAX lowering below —
/// consulted exclusively under the legacy latch so the ANSI plan JSON
/// stays byte-for-byte unchanged.
fn is_long_column(field: &str, schema: Option<&DataSourceSchema>) -> bool {
    schema.is_some_and(|s| {
        s.dimensions
            .iter()
            .chain(s.metrics.iter())
            .any(|c| c.name == field && matches!(c.column_type, ColumnType::Long))
    })
}

fn try_as_aggregation(
    expr: &SqlExpr,
    alias: &Option<String>,
    index: usize,
    schema: Option<&DataSourceSchema>,
    exact_count_distinct: bool,
) -> Result<Option<AggregatorSpec>> {
    match expr {
        SqlExpr::Aggregate {
            func,
            arg,
            distinct,
        } => {
            // DD R41 (updated by null-semantics T3, then E16): in the
            // DEFAULT mode (`useApproximateCountDistinct=true`),
            // COUNT(DISTINCT <col>) lowers to an HLL sketch
            // post-aggregation — return `None` here so the projection loop
            // falls through to `try_as_post_aggregation`. E16: with the
            // context flag OFF (`exact_count_distinct`), it lowers to a
            // VISIBLE exact `cardinality` aggregation named by the alias
            // (see `exact_count_distinct_aggregator`). Every other DISTINCT
            // aggregate (SUM/AVG/MIN/MAX DISTINCT, and COUNT DISTINCT over
            // a non-column expression) stays fail-closed in BOTH modes.
            if *distinct {
                if func == "COUNT"
                    && let Some(SqlExpr::Column(col)) = arg.as_deref()
                {
                    if exact_count_distinct {
                        let name = alias
                            .clone()
                            .unwrap_or_else(|| format!("count_distinct_{col}"));
                        return Ok(Some(exact_count_distinct_aggregator(name, col.clone())));
                    }
                    return Ok(None);
                }
                return Err(DruidError::Query(format!(
                    "{func}(DISTINCT ...) is not supported (distinct aggregation is not implemented)"
                )));
            }
            let default_name = || alias.clone().unwrap_or_else(|| format!("agg_{index}"));
            match func.as_str() {
                // Null-semantics T2: `COUNT(<col>)` lowers to a not-null-
                // filtered count — SQL COUNT(col) counts only non-null rows
                // (measured against Druid 35/36, 2026-07-11). The
                // argument-less forms (`COUNT(*)`, which the parser lowers
                // to `arg: None`, and constant forms such as `COUNT(1)`)
                // stay plain row counts. A non-column argument fails closed
                // (DD R42's silent-wrong row count must never come back).
                "COUNT" => match arg.as_deref() {
                    None | Some(SqlExpr::Literal(SqlLiteral::Integer(_))) => {
                        Ok(Some(AggregatorSpec::Count {
                            name: default_name(),
                        }))
                    }
                    Some(SqlExpr::Column(col)) => Ok(Some(AggregatorSpec::Filtered {
                        filter: not_null_selector_filter(col),
                        aggregator: Box::new(AggregatorSpec::Count {
                            name: default_name(),
                        }),
                    })),
                    Some(_) => Err(DruidError::Query(
                        "COUNT(<expr>) is only supported for a bare column reference \
                         (null-aware count) or the row-count forms COUNT(*) / COUNT(1)"
                            .to_owned(),
                    )),
                },
                "SUM" => {
                    let Some(field) = arg_to_field_name(arg) else {
                        return Ok(None);
                    };
                    Ok(Some(AggregatorSpec::DoubleSum {
                        name: default_name(),
                        field_name: field,
                    }))
                }
                // P1-#2: MIN/MAX over the time column lower to longMin /
                // longMax (Druid's native lowering for `MIN(__time)`), so
                // the result stays an integral epoch-millis long that the
                // SQL wire can render as Druid's ISO-8601 TIMESTAMP string.
                // Every other column keeps the doubleMin/doubleMax
                // lowering.
                "MIN" => {
                    let Some(field) = arg_to_field_name(arg) else {
                        return Ok(None);
                    };
                    // W-B legacy null mode: MIN over a LONG column lowers
                    // to longMin so an EMPTY set answers the i64::MAX
                    // sentinel exactly like legacy Druid (oracle
                    // empty_min_y.json = 9223372036854775807; doubleMin
                    // would answer the "Infinity" double sentinel).  Gated
                    // on the latch so the ANSI plan stays byte-identical.
                    if is_time_column(&field, schema)
                        || (ferrodruid_common::legacy_null_mode() && is_long_column(&field, schema))
                    {
                        return Ok(Some(AggregatorSpec::LongMin {
                            name: default_name(),
                            field_name: field,
                        }));
                    }
                    Ok(Some(AggregatorSpec::DoubleMin {
                        name: default_name(),
                        field_name: field,
                    }))
                }
                "MAX" => {
                    let Some(field) = arg_to_field_name(arg) else {
                        return Ok(None);
                    };
                    // W-B legacy null mode: see the MIN arm (oracle
                    // empty_max_y.json = the i64::MIN sentinel).
                    if is_time_column(&field, schema)
                        || (ferrodruid_common::legacy_null_mode() && is_long_column(&field, schema))
                    {
                        return Ok(Some(AggregatorSpec::LongMax {
                            name: default_name(),
                            field_name: field,
                        }));
                    }
                    Ok(Some(AggregatorSpec::DoubleMax {
                        name: default_name(),
                        field_name: field,
                    }))
                }
                // W2-C: AVG(x) is not a plain aggregation — it lowers to a
                // hidden doubleSum + count pair finalised by a `/`
                // post-aggregation (see `try_as_post_aggregation`).
                // Returning `None` lets the projection loop fall through to
                // that lowering; an AVG shape the lowering cannot handle
                // fails closed there.
                "AVG" => Ok(None),
                _ => Ok(None),
            }
        }
        // Null-semantics T3: APPROX_COUNT_DISTINCT(col) lowers to an HLL
        // sketch post-aggregation — return `None` so the projection loop
        // falls through to `try_as_post_aggregation` (which fails closed on
        // a non-column argument).
        SqlExpr::Function(DruidFunction::ApproxCountDistinct { .. }) => Ok(None),
        // DD R41: this was lowered to a DoubleSum placeholder that returned
        // a wrong result (a sum) under the function's output name. Real
        // quantile-sketch finalization is not wired through the broker, so
        // fail closed instead of returning silently-incorrect data.
        SqlExpr::Function(DruidFunction::ApproxQuantileDs { .. }) => Err(DruidError::Query(
            "APPROX_QUANTILE_DS is not supported (the placeholder returned a sum)".to_owned(),
        )),
        SqlExpr::Function(DruidFunction::Earliest(inner)) => {
            let SqlExpr::Column(field) = inner.as_ref() else {
                return Ok(None);
            };
            let name = alias.clone().unwrap_or_else(|| format!("earliest_{field}"));
            Ok(Some(typed_first_aggregator(
                name,
                field.clone(),
                None,
                schema,
            )))
        }
        SqlExpr::Function(DruidFunction::Latest(inner)) => {
            let SqlExpr::Column(field) = inner.as_ref() else {
                return Ok(None);
            };
            let name = alias.clone().unwrap_or_else(|| format!("latest_{field}"));
            Ok(Some(typed_last_aggregator(
                name,
                field.clone(),
                None,
                schema,
            )))
        }
        SqlExpr::Function(DruidFunction::AnyValue(inner)) => {
            let SqlExpr::Column(field) = inner.as_ref() else {
                return Ok(None);
            };
            let name = alias
                .clone()
                .unwrap_or_else(|| format!("any_value_{field}"));
            Ok(Some(AggregatorSpec::DoubleFirst {
                name,
                field_name: field.clone(),
                time_column: None,
            }))
        }

        // ----- CL-4 / W1-D + W1-H — aggregate-family additions -----
        // R6 (W1-H): `EARLIEST(expr, timeCol)` / `LATEST(expr, timeCol)`
        // lower to the existing First/Last family with `time_column =
        // Some(_)`.  The query layer reads the per-row timestamp from
        // `timeCol` (rather than synthetic insertion order) and
        // dispatches through `Aggregator::aggregate_with_time`.
        SqlExpr::Function(DruidFunction::EarliestBy { expr, time_col }) => {
            let SqlExpr::Column(field) = expr.as_ref() else {
                return Err(DruidError::Query(
                    "EARLIEST(expr, timeCol): first argument must be a bare column reference"
                        .to_owned(),
                ));
            };
            let name = alias
                .clone()
                .unwrap_or_else(|| format!("earliest_{field}_by_{time_col}"));
            Ok(Some(typed_first_aggregator(
                name,
                field.clone(),
                Some(time_col.clone()),
                schema,
            )))
        }
        SqlExpr::Function(DruidFunction::LatestBy { expr, time_col }) => {
            let SqlExpr::Column(field) = expr.as_ref() else {
                return Err(DruidError::Query(
                    "LATEST(expr, timeCol): first argument must be a bare column reference"
                        .to_owned(),
                ));
            };
            let name = alias
                .clone()
                .unwrap_or_else(|| format!("latest_{field}_by_{time_col}"));
            Ok(Some(typed_last_aggregator(
                name,
                field.clone(),
                Some(time_col.clone()),
                schema,
            )))
        }
        // R1 (W1-H): ARRAY_AGG([DISTINCT] expr [, size_limit]).
        SqlExpr::Function(DruidFunction::ArrayAgg {
            expr,
            distinct,
            size_limit,
        }) => {
            let SqlExpr::Column(field) = expr.as_ref() else {
                return Err(DruidError::Query(
                    "ARRAY_AGG(expr): expression must be a bare column reference".to_owned(),
                ));
            };
            let name = alias
                .clone()
                .unwrap_or_else(|| format!("array_agg_{field}"));
            Ok(Some(AggregatorSpec::ArrayAgg {
                name,
                field_name: field.clone(),
                distinct: *distinct,
                size_limit: *size_limit,
            }))
        }
        // R2 (W1-H): LISTAGG(expr [, separator [, size_limit]]).
        SqlExpr::Function(DruidFunction::Listagg {
            expr,
            separator,
            size_limit,
        }) => {
            let SqlExpr::Column(field) = expr.as_ref() else {
                return Err(DruidError::Query(
                    "LISTAGG(expr, ...): expression must be a bare column reference".to_owned(),
                ));
            };
            let name = alias.clone().unwrap_or_else(|| format!("listagg_{field}"));
            Ok(Some(AggregatorSpec::StringAgg {
                name,
                field_name: field.clone(),
                separator: separator.clone(),
                size_limit: *size_limit,
            }))
        }
        // R3 (W1-H): STRING_AGG(expr, separator [, size_limit]).
        SqlExpr::Function(DruidFunction::StringAgg {
            expr,
            separator,
            size_limit,
        }) => {
            let SqlExpr::Column(field) = expr.as_ref() else {
                return Err(DruidError::Query(
                    "STRING_AGG(expr, ...): expression must be a bare column reference".to_owned(),
                ));
            };
            let name = alias
                .clone()
                .unwrap_or_else(|| format!("string_agg_{field}"));
            Ok(Some(AggregatorSpec::StringAgg {
                name,
                field_name: field.clone(),
                separator: separator.clone(),
                size_limit: *size_limit,
            }))
        }
        // R4 (W1-H): BLOOM_FILTER(expr, num_entries).
        SqlExpr::Function(DruidFunction::BloomFilter { expr, num_entries }) => {
            let SqlExpr::Column(field) = expr.as_ref() else {
                return Err(DruidError::Query(
                    "BLOOM_FILTER(expr, n): expression must be a bare column reference".to_owned(),
                ));
            };
            let n = u64::try_from(*num_entries).map_err(|_| {
                DruidError::Query(format!(
                    "BLOOM_FILTER(expr, {num_entries}): numEntries must be a positive integer"
                ))
            })?;
            let name = alias
                .clone()
                .unwrap_or_else(|| format!("bloom_filter_{field}"));
            Ok(Some(AggregatorSpec::BloomFilter {
                name,
                field_name: field.clone(),
                num_entries: n,
            }))
        }
        // R7 (W1-H): GROUPING(c1, c2, ...) lowers to the indicator
        // pseudo-aggregator.  The bitmask is finalised by the GroupBy
        // executor against the active subtotals subset; this lowering
        // captures only the referenced columns — the enclosing query's
        // full GROUP BY dim list is patched into the spec by
        // `finalize_grouping_specs` once `plan_groupby` has resolved it.
        SqlExpr::Function(DruidFunction::Grouping(args)) => {
            let mut fields = Vec::with_capacity(args.len());
            for a in args {
                match a {
                    SqlExpr::Column(c) => fields.push(c.clone()),
                    other => {
                        return Err(DruidError::Query(format!(
                            "GROUPING(...) arguments must be bare column references, got: {other:?}"
                        )));
                    }
                }
            }
            let name = alias.clone().unwrap_or_else(|| "grouping".to_owned());
            Ok(Some(AggregatorSpec::Grouping {
                name,
                fields: fields.clone(),
                group_by_dims: fields,
            }))
        }

        _ => Ok(None),
    }
}

fn arg_to_field_name(arg: &Option<Box<SqlExpr>>) -> Option<String> {
    match arg.as_deref() {
        Some(SqlExpr::Column(name)) => Some(name.clone()),
        _ => None,
    }
}

/// Rewrite every `AggregatorSpec::Grouping` in `agg_specs` so its
/// `group_by_dims` field carries the enclosing query's full GROUP BY
/// dim list (CL-4 / W1-H R7).  The lowering in `try_as_aggregation`
/// only knows the per-call argument list and uses it as a placeholder
/// `group_by_dims`; this pass patches in the authoritative ordering so
/// the executor's bitmask computation is correct under subtotals.
///
/// codex QA r5: both `group_by_dims` and the `GROUPING(...)` argument
/// `fields` are rewritten to the dimensions' OUTPUT names — the executor
/// compares them against the active dimensions' `outputName`s, which carry
/// the SELECT alias when the dimension is aliased. A field that names no
/// grouping dimension is left as-is (it stays "always aggregated",
/// preserving the defensive bit-set behaviour).
fn finalize_grouping_specs(agg_specs: &mut [AggregatorSpec], dim_bindings: &[(String, String)]) {
    let output_dims: Vec<String> = dim_bindings
        .iter()
        .map(|(_, output)| output.clone())
        .collect();
    for spec in agg_specs.iter_mut() {
        if let AggregatorSpec::Grouping {
            fields,
            group_by_dims: spec_dims,
            ..
        } = spec
        {
            *spec_dims = output_dims.clone();
            for field in fields.iter_mut() {
                if let Some((_, output)) = dim_bindings
                    .iter()
                    .find(|(column, output)| column == field || output == field)
                {
                    field.clone_from(output);
                }
            }
        }
    }
}

/// Resolve a `TIME_FLOOR` ISO period to a native granularity, failing closed
/// on a period with no granularity mapping. Previously an unknown period
/// silently fell back to `all`, collapsing the time series into one giant
/// bucket (silent-wrong for e.g. Superset's PT5S / PT30S grains).
///
/// Named periods lower to their simple granularity name; any other
/// single-field fixed time period (`PTnS` / `PTnM` / `PTnH` — Superset's
/// PT5S / PT30S grains, or user-emitted multiples like PT2H) lowers to the
/// native fixed-period `duration` granularity anchored at the epoch, which
/// is Druid's TIME_FLOOR semantics for such periods in UTC
/// (`epoch + floor((t - epoch) / period_ms) * period_ms`).
fn resolve_time_floor_granularity(period: &str) -> Result<GranularitySpec> {
    if let Some(name) = period_to_granularity(period) {
        return Ok(GranularitySpec::Simple(name.to_owned()));
    }
    if let Some(period_ms) = period_to_fixed_millis(period) {
        return Ok(GranularitySpec::Full(Granularity::Duration {
            period_ms,
            origin: chrono::DateTime::UNIX_EPOCH,
        }));
    }
    Err(DruidError::Query(format!(
        "TIME_FLOOR period {period} has no supported granularity; supported \
         periods: PT1S PT1M PT5M PT10M PT15M PT30M PT1H PT6H P1D P1W P1M P3M P1Y, \
         or any single-field fixed time period (PTnS / PTnM / PTnH)"
    )))
}

/// The granular time key of a GROUP BY, found in the SELECT projections:
/// either a `TIME_FLOOR(t, period)` whose period resolves lazily at the
/// timeseries/groupBy branch (so non-grouping paths keep their previous
/// behaviour for an unresolvable period), or an already-lowered native
/// granularity (the shifted-week Superset grain, whose recognition IS its
/// lowering).
enum TimeGrain {
    /// A `TIME_FLOOR` ISO period string.
    Period(String),
    /// A pre-lowered native granularity.
    Native(GranularitySpec),
}

impl TimeGrain {
    fn resolve(&self) -> Result<GranularitySpec> {
        match self {
            TimeGrain::Period(p) => resolve_time_floor_granularity(p),
            TimeGrain::Native(g) => Ok(g.clone()),
        }
    }
}

/// `true` when the projection expression is a time-grain key handled through
/// the query granularity (a `TIME_FLOOR`, or the shifted-week form recognised
/// by [`shifted_week_granularity`]) rather than a grouping dimension.
fn is_time_grain_expr(expr: &SqlExpr) -> bool {
    matches!(expr, SqlExpr::Function(DruidFunction::TimeFloor { .. }))
        || shifted_week_granularity(expr).is_some()
}

/// Recognise Superset's `week_starting_sunday` grain —
/// `TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(t, 'P1D', 1), 'P1W'), 'P1D', -1)` —
/// and lower it to its equivalent native granularity.
///
/// Per row, the nested expression floors `t + 1d` to the ISO Monday and
/// shifts back one day: `t` lands in the bucket `[Sunday, Sunday + 7d)`
/// labeled by that Sunday. A bucket labeled by its own start is exactly a
/// fixed 7-day `duration` granularity anchored on any Sunday — the one
/// before the epoch, 1969-12-28T00:00:00Z. Like the plain `TIME_FLOOR`
/// detection, the innermost timestamp operand is not inspected (the time
/// key is taken to be over the time column, CAST-wrapped or bare).
///
/// Superset's `week_ending_saturday` (outer step `5`) has NO such lowering:
/// its label (the Saturday ENDING the bucket) lies 6 days after the bucket
/// start, and a floor-to-origin granularity can only label a bucket by its
/// start — that grain stays fail-closed rather than silently mis-labeled.
fn shifted_week_granularity(expr: &SqlExpr) -> Option<GranularitySpec> {
    // Outer: TIME_SHIFT(inner, 'P1D', -1).
    let SqlExpr::Function(DruidFunction::TimeShift {
        expr: floored,
        period: outer_period,
        step: -1,
    }) = expr
    else {
        return None;
    };
    if outer_period != "P1D" {
        return None;
    }
    // Middle: TIME_FLOOR(shifted, 'P1W') — UTC only (an explicit timezone
    // moves the bucket boundaries; keep failing closed on that form).
    let SqlExpr::Function(DruidFunction::TimeFloor {
        expr: shifted,
        period: floor_period,
        timezone: None,
    }) = floored.as_ref()
    else {
        return None;
    };
    if floor_period != "P1W" {
        return None;
    }
    // Inner: TIME_SHIFT(t, 'P1D', 1).
    let SqlExpr::Function(DruidFunction::TimeShift {
        expr: _,
        period: inner_period,
        step: 1,
    }) = shifted.as_ref()
    else {
        return None;
    };
    if inner_period != "P1D" {
        return None;
    }
    /// 1969-12-28T00:00:00Z — the Sunday before the (Thursday) epoch.
    const SUNDAY_ORIGIN_MS: i64 = -345_600_000;
    /// One week in milliseconds (weeks are fixed-length in UTC).
    const WEEK_MS: u64 = 604_800_000;
    let origin = chrono::DateTime::from_timestamp_millis(SUNDAY_ORIGIN_MS)?;
    Some(GranularitySpec::Full(Granularity::Duration {
        period_ms: WEEK_MS,
        origin,
    }))
}

// ---------------------------------------------------------------------------
// Helpers: GROUP BY dimension extraction
// ---------------------------------------------------------------------------

fn extract_group_by_dims(select: &SelectQuery) -> Result<Vec<String>> {
    let mut dims = Vec::new();
    for gb in &select.group_by {
        match gb {
            SqlExpr::Column(name) => {
                // Only include actual dimension columns, not time columns.
                dims.push(name.clone());
            }
            SqlExpr::Positional(pos) => {
                resolve_group_by_position(*pos, &select.projections, &mut dims)?
            }
            SqlExpr::Literal(SqlLiteral::Integer(pos)) => {
                resolve_group_by_position(
                    usize::try_from(*pos).map_err(|_| {
                        DruidError::Query(format!("GROUP BY position {pos} is not a valid ordinal"))
                    })?,
                    &select.projections,
                    &mut dims,
                )?;
            }
            // `GROUP BY TIME_FLOOR(__time, period)` — or the shifted-week
            // grain — is handled by the projection scan (it sets the query
            // granularity), so it is not a grouping dimension here.
            expr if is_time_grain_expr(expr) => {}
            // DD R43 (Finding 2): any other GROUP BY expression (e.g.
            // `GROUP BY UPPER(city)`) was silently dropped, collapsing the
            // query to a global aggregation that returns a single wrong row.
            // Expression grouping keys are not supported, so fail closed.
            _ => {
                return Err(DruidError::Query(
                    "unsupported GROUP BY expression; group by a bare column, a positional \
                     ordinal, or TIME_FLOOR(__time, period)"
                        .to_owned(),
                ));
            }
        }
    }
    Ok(dims)
}

/// Resolve a positional `GROUP BY <n>` ordinal (1-based) against the FULL
/// SELECT projection list. DD R43 (Finding 2): an out-of-range ordinal, or
/// one that points at an unsupported (non-column) projection, is rejected
/// rather than silently dropped.
///
/// codex QA r5: ordinals previously indexed only the NON-AGGREGATE
/// projections, so `SELECT COUNT(*) AS c, site_id AS s … GROUP BY 2` counted
/// past the end (or, with interleaved aggregates, silently bound the wrong
/// column). SQL ordinals count SELECT positions, so the full projection list
/// is the correct index space; an ordinal that lands on an aggregate
/// projection fails closed (grouping by an aggregate is invalid SQL).
fn resolve_group_by_position(
    pos: usize,
    projections: &[Projection],
    dims: &mut Vec<String>,
) -> Result<()> {
    // DD R44: ordinals are 1-based; reject `GROUP BY 0` rather than folding it to
    // the first projection via `saturating_sub`.
    if pos == 0 {
        return Err(DruidError::Query(
            "GROUP BY position 0 is invalid (positions are 1-based)".to_owned(),
        ));
    }
    let idx = pos - 1;
    let Some(proj) = projections.get(idx) else {
        return Err(DruidError::Query(format!(
            "GROUP BY position {pos} is out of range for the SELECT list"
        )));
    };
    let Projection::Expr { expr, alias } = proj else {
        return Err(DruidError::Query(format!(
            "GROUP BY position {pos} references `*`; group by a bare column, \
             or TIME_FLOOR(__time, period) for a time grain"
        )));
    };
    // Skip TIME_FLOOR (and shifted-week) projections — those set the query
    // granularity instead.
    if is_time_grain_expr(expr) {
        return Ok(());
    }
    // Only bare-column projections are grouping keys. An aliased non-column
    // projection (e.g. `TIME_SHIFT(...) AS t ... GROUP BY 1`, Superset's
    // week-variant grains) previously pushed the ALIAS as a native
    // dimension, grouping by a column that does not exist — one silently
    // wrong all-null group. Fail closed instead. (An aggregate projection
    // also lands here and is rejected.)
    if let SqlExpr::Column(name) = expr {
        if let Some(a) = alias {
            dims.push(a.clone());
        } else {
            dims.push(name.clone());
        }
        Ok(())
    } else {
        Err(DruidError::Query(format!(
            "GROUP BY position {pos} references an unsupported projection expression; \
             group by a bare column, or TIME_FLOOR(__time, period) for a time grain"
        )))
    }
}

// ---------------------------------------------------------------------------
// Helpers: filter conversion
// ---------------------------------------------------------------------------

/// Convert a SQL `WHERE`-clause expression into a native Druid [`FilterSpec`].
///
/// Exposed (DD R43, Finding 5) so the MSQ executor can lower a SQL `WHERE`
/// into its scan-stage filter using the same conversion the broker uses,
/// rather than re-implementing filter parsing.
///
/// # Errors
///
/// Returns [`DruidError::Query`] for a `WHERE` form that has no native filter
/// representation.
pub fn convert_filter(expr: &SqlExpr) -> Result<FilterSpec> {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => {
                let dim = expr_to_column_name(left)?;
                let val = expr_to_json_value(right)?;
                Ok(FilterSpec::Selector {
                    dimension: dim,
                    value: Some(val),
                })
            }
            BinaryOperator::NotEq => {
                let dim = expr_to_column_name(left)?;
                let val = expr_to_json_value(right)?;
                Ok(FilterSpec::Not {
                    field: Box::new(FilterSpec::Selector {
                        dimension: dim,
                        value: Some(val),
                    }),
                })
            }
            BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                let dim = expr_to_column_name(left)?;
                let val = expr_to_json_value(right)?;
                let val_str = json_to_string(&val);
                let (lower, upper, lower_strict, upper_strict) = match op {
                    BinaryOperator::Gt => (Some(val_str), None, Some(true), None),
                    BinaryOperator::GtEq => (Some(val_str), None, Some(false), None),
                    BinaryOperator::Lt => (None, Some(val_str), None, Some(true)),
                    BinaryOperator::LtEq => (None, Some(val_str), None, Some(false)),
                    _ => unreachable!(),
                };
                Ok(FilterSpec::Bound {
                    dimension: dim,
                    lower,
                    upper,
                    lower_strict,
                    upper_strict,
                    ordering: Some("numeric".to_string()),
                })
            }
            _ => Err(DruidError::Query(format!(
                "Unsupported filter operator: {op:?}"
            ))),
        },
        SqlExpr::And(left, right) => {
            let l = convert_filter(left)?;
            let r = convert_filter(right)?;
            Ok(FilterSpec::And { fields: vec![l, r] })
        }
        SqlExpr::Or(left, right) => {
            let l = convert_filter(left)?;
            let r = convert_filter(right)?;
            Ok(FilterSpec::Or { fields: vec![l, r] })
        }
        SqlExpr::Not(inner) => {
            let f = convert_filter(inner)?;
            Ok(FilterSpec::Not { field: Box::new(f) })
        }
        SqlExpr::IsNull(inner) => {
            let dim = expr_to_column_name(inner)?;
            Ok(FilterSpec::Null { column: dim })
        }
        SqlExpr::IsNotNull(inner) => {
            let dim = expr_to_column_name(inner)?;
            Ok(FilterSpec::Not {
                field: Box::new(FilterSpec::Null { column: dim }),
            })
        }
        SqlExpr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let dim = expr_to_column_name(inner)?;
            let values: Vec<serde_json::Value> = list
                .iter()
                .map(expr_to_json_value)
                .collect::<Result<Vec<_>>>()?;
            let filter = FilterSpec::In {
                dimension: dim,
                values,
            };
            if *negated {
                Ok(FilterSpec::Not {
                    field: Box::new(filter),
                })
            } else {
                Ok(filter)
            }
        }
        SqlExpr::Like {
            expr: inner,
            pattern,
            negated,
        } => {
            let dim = expr_to_column_name(inner)?;
            let pat = match pattern.as_ref() {
                SqlExpr::Literal(SqlLiteral::String(s)) => s.clone(),
                _ => {
                    return Err(DruidError::Query(
                        "LIKE pattern must be a string literal".to_string(),
                    ));
                }
            };
            let filter = FilterSpec::Like {
                dimension: dim,
                pattern: pat,
                escape: None,
            };
            if *negated {
                Ok(FilterSpec::Not {
                    field: Box::new(filter),
                })
            } else {
                Ok(filter)
            }
        }
        SqlExpr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => {
            let dim = expr_to_column_name(inner)?;
            let lo = json_to_string(&expr_to_json_value(low)?);
            let hi = json_to_string(&expr_to_json_value(high)?);
            let filter = FilterSpec::Bound {
                dimension: dim,
                lower: Some(lo),
                upper: Some(hi),
                lower_strict: Some(false),
                upper_strict: Some(false),
                ordering: Some("numeric".to_string()),
            };
            if *negated {
                Ok(FilterSpec::Not {
                    field: Box::new(filter),
                })
            } else {
                Ok(filter)
            }
        }
        // ----- CL-4 / W1-D + W1-H — Druid-specific filter forms in WHERE -----
        // R4 (W1-H): BLOOM_FILTER_TEST(col, base64) lowers to the
        // native bloomFilter probe.  The aggregator wire format and
        // filter expect identical envelope shapes.
        SqlExpr::Function(DruidFunction::BloomFilterTest {
            expr,
            encoded_filter,
        }) => {
            let dim = expr_to_column_name(expr)?;
            Ok(FilterSpec::BloomFilter {
                dimension: dim,
                base64_filter: encoded_filter.clone(),
            })
        }
        // R5 (W1-H): MV_FILTER_ONLY(col, ARRAY[v1, ...]).
        SqlExpr::Function(DruidFunction::MvFilterOnly { column, values }) => {
            let dim = expr_to_column_name(column)?;
            let vals: Vec<serde_json::Value> = values
                .iter()
                .map(expr_to_json_value)
                .collect::<Result<Vec<_>>>()?;
            Ok(FilterSpec::MvFilterOnly {
                dimension: dim,
                values: vals,
            })
        }
        // R5 (W1-H): MV_FILTER_NONE(col, ARRAY[v1, ...]).
        SqlExpr::Function(DruidFunction::MvFilterNone { column, values }) => {
            let dim = expr_to_column_name(column)?;
            let vals: Vec<serde_json::Value> = values
                .iter()
                .map(expr_to_json_value)
                .collect::<Result<Vec<_>>>()?;
            Ok(FilterSpec::MvFilterNone {
                dimension: dim,
                values: vals,
            })
        }
        _ => Err(DruidError::Query(format!(
            "Unsupported filter expression: {expr:?}"
        ))),
    }
}

fn expr_to_column_name(expr: &SqlExpr) -> Result<String> {
    match expr {
        SqlExpr::Column(name) => Ok(name.clone()),
        _ => Err(DruidError::Query(format!(
            "Expected column name, got: {expr:?}"
        ))),
    }
}

fn expr_to_json_value(expr: &SqlExpr) -> Result<serde_json::Value> {
    match expr {
        SqlExpr::Literal(lit) => match lit {
            SqlLiteral::Integer(i) => Ok(serde_json::Value::from(*i)),
            SqlLiteral::Float(f) => Ok(serde_json::json!(*f)),
            SqlLiteral::String(s) => Ok(serde_json::Value::String(s.clone())),
            SqlLiteral::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
            SqlLiteral::Null => Ok(serde_json::Value::Null),
            // P1-#2: `TIMESTAMP '...'` / `DATE '...'` typed literals were
            // folded to epoch millis at parse time; surfacing them as a
            // JSON number makes `__time >= TIMESTAMP '...'` lower to the
            // same numeric bound as the `TIME_PARSE(...)` path below
            // (previously the literal fell through as a plain string,
            // whose f64 parse failed the numeric bound for every row —
            // Superset time-range filters matched ZERO rows).
            SqlLiteral::Timestamp(ms) => Ok(serde_json::Value::from(*ms)),
        },
        // T9 (Superset temporal filters): `TIME_PARSE('<literal>')` — the
        // shape Superset emits in WHERE clauses — evaluates at plan time to
        // epoch millis so `__time >= TIME_PARSE(...)` becomes a numeric
        // bound. Only the format-less form over a string literal is
        // constant-foldable; anything else keeps failing closed below.
        SqlExpr::Function(DruidFunction::TimeParse {
            expr: inner,
            format: None,
        }) => {
            let SqlExpr::Literal(SqlLiteral::String(s)) = inner.as_ref() else {
                return Err(DruidError::Query(format!(
                    "TIME_PARSE in a filter must take a string literal, got: {inner:?}"
                )));
            };
            let millis = time_literal_to_millis(s).ok_or_else(|| {
                DruidError::Query(format!(
                    "TIME_PARSE('{s}') is not an ISO-8601 timestamp the planner can \
                     evaluate (supported: YYYY-MM-DD[THH:MM:SS[.fff]][Z])"
                ))
            })?;
            Ok(serde_json::Value::from(millis))
        }
        // `CAST(<time expr> AS DATE)` floors the evaluated timestamp to the
        // day; `CAST(<time expr> AS TIMESTAMP)` is a no-op passthrough. A
        // string operand (`CAST('2024-01-01' AS TIMESTAMP)`, which Druid
        // accepts) folds to epoch millis first (P1-#2).
        SqlExpr::Cast {
            expr: inner,
            data_type,
        } => {
            let mut val = expr_to_json_value(inner)?;
            let dt = data_type.to_ascii_uppercase();
            if (dt.contains("DATE") || dt.contains("TIMESTAMP"))
                && let serde_json::Value::String(s) = &val
            {
                let millis = time_literal_to_millis(s).ok_or_else(|| {
                    DruidError::Query(format!(
                        "CAST('{s}' AS {dt}) in a filter is not a timestamp literal the \
                         planner can evaluate (supported: \
                         YYYY-MM-DD[{{T| }}HH:MM:SS[.fff]][Z])"
                    ))
                })?;
                val = serde_json::Value::from(millis);
            }
            if dt.contains("DATE") && !dt.contains("TIMESTAMP") {
                let millis = val.as_i64().ok_or_else(|| {
                    DruidError::Query(format!(
                        "CAST(... AS DATE) in a filter requires a timestamp value, got: {val}"
                    ))
                })?;
                return Ok(serde_json::Value::from(
                    millis - millis.rem_euclid(86_400_000),
                ));
            }
            Ok(val)
        }
        _ => Err(DruidError::Query(format!(
            "Expected a literal value, got: {expr:?}"
        ))),
    }
}

fn json_to_string(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Helpers: HAVING conversion
// ---------------------------------------------------------------------------

fn convert_having(expr: &SqlExpr) -> Result<ferrodruid_query::groupby::HavingSpec> {
    use ferrodruid_query::groupby::HavingSpec;
    match expr {
        SqlExpr::BinaryOp { left, op, right } => {
            let agg_name = having_agg_name(left)?;
            let threshold = having_threshold(right)?;
            match op {
                BinaryOperator::Gt => Ok(HavingSpec::GreaterThan {
                    aggregation: agg_name,
                    value: threshold,
                }),
                BinaryOperator::GtEq => {
                    // >= X is implemented as NOT (< X) which is NOT lessThan(X)
                    // For simplicity, use greaterThan(X - epsilon)
                    Ok(HavingSpec::GreaterThan {
                        aggregation: agg_name,
                        value: threshold - f64::EPSILON,
                    })
                }
                BinaryOperator::Lt => Ok(HavingSpec::LessThan {
                    aggregation: agg_name,
                    value: threshold,
                }),
                BinaryOperator::LtEq => Ok(HavingSpec::LessThan {
                    aggregation: agg_name,
                    value: threshold + f64::EPSILON,
                }),
                BinaryOperator::Eq => Ok(HavingSpec::EqualTo {
                    aggregation: agg_name,
                    value: threshold,
                }),
                _ => Err(DruidError::Query(format!(
                    "Unsupported HAVING operator: {op:?}"
                ))),
            }
        }
        SqlExpr::And(l, r) => {
            let lh = convert_having(l)?;
            let rh = convert_having(r)?;
            Ok(HavingSpec::And {
                having_specs: vec![lh, rh],
            })
        }
        SqlExpr::Or(l, r) => {
            let lh = convert_having(l)?;
            let rh = convert_having(r)?;
            Ok(HavingSpec::Or {
                having_specs: vec![lh, rh],
            })
        }
        _ => Err(DruidError::Query(format!(
            "Unsupported HAVING expression: {expr:?}"
        ))),
    }
}

fn having_agg_name(expr: &SqlExpr) -> Result<String> {
    match expr {
        SqlExpr::Column(name) => Ok(name.clone()),
        SqlExpr::Aggregate { func, arg, .. } => match (func.as_str(), arg) {
            ("COUNT", None) => Ok("cnt".to_string()),
            (f, Some(inner)) => {
                if let SqlExpr::Column(col) = inner.as_ref() {
                    Ok(format!("{}_{}", f.to_lowercase(), col))
                } else {
                    Err(DruidError::Query(
                        "HAVING aggregate arg must be a column".to_string(),
                    ))
                }
            }
            _ => Err(DruidError::Query(
                "Unsupported HAVING aggregate".to_string(),
            )),
        },
        _ => Err(DruidError::Query(format!(
            "Expected aggregate or column in HAVING, got: {expr:?}"
        ))),
    }
}

fn having_threshold(expr: &SqlExpr) -> Result<f64> {
    match expr {
        SqlExpr::Literal(SqlLiteral::Integer(i)) => Ok(*i as f64),
        SqlExpr::Literal(SqlLiteral::Float(f)) => Ok(*f),
        _ => Err(DruidError::Query(format!(
            "HAVING threshold must be a number, got: {expr:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_druid_sql;

    fn test_schema() -> DataSourceSchema {
        DataSourceSchema {
            name: "sales".to_string(),
            dimensions: vec![
                ColumnSchema {
                    name: "city".to_string(),
                    column_type: ColumnType::String,
                },
                ColumnSchema {
                    name: "country".to_string(),
                    column_type: ColumnType::String,
                },
            ],
            metrics: vec![
                ColumnSchema {
                    name: "revenue".to_string(),
                    column_type: ColumnType::Double,
                },
                ColumnSchema {
                    name: "quantity".to_string(),
                    column_type: ColumnType::Long,
                },
            ],
            time_column: "__time".to_string(),
            join_schemas: Vec::new(),
        }
    }

    #[test]
    fn plan_count_star_to_timeseries() {
        let stmt = parse_druid_sql("SELECT COUNT(*) AS cnt FROM sales").expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert!(matches!(planned.native_query, DruidQuery::Timeseries(_)));
        assert_eq!(planned.output_columns.len(), 1);
        assert_eq!(planned.output_columns[0].name, "cnt");
    }

    #[test]
    fn plan_group_by_dimension_to_groupby() {
        let sql = "SELECT city, COUNT(*) AS cnt FROM sales GROUP BY city";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert!(matches!(planned.native_query, DruidQuery::GroupBy(_)));
        assert_eq!(planned.output_columns.len(), 2);
    }

    #[test]
    fn zero_ordinal_in_order_by_and_group_by_fails_closed() {
        // DD R44: SQL ordinals are 1-based; ORDER BY 0 / GROUP BY 0 must reject,
        // not fold to the first column via saturating_sub.
        let schema = test_schema();
        let stmt = parse_druid_sql(
            "SELECT city, country, COUNT(*) AS cnt FROM sales GROUP BY city, country ORDER BY 0 DESC",
        )
        .expect("parse");
        assert!(
            plan_sql(&stmt, &schema).is_err(),
            "ORDER BY 0 must fail closed"
        );

        let stmt = parse_druid_sql("SELECT city, COUNT(*) FROM sales GROUP BY 0").expect("parse");
        assert!(
            plan_sql(&stmt, &schema).is_err(),
            "GROUP BY 0 must fail closed"
        );

        // A valid 1-based ordinal still works.
        let stmt =
            parse_druid_sql("SELECT city, COUNT(*) AS cnt FROM sales GROUP BY 1").expect("parse");
        assert!(
            plan_sql(&stmt, &schema).is_ok(),
            "GROUP BY 1 must still plan"
        );
    }

    #[test]
    fn rejects_unsupported_aggregates_fail_closed() {
        // DD R41 (updated for the null-semantics program): COUNT(DISTINCT col)
        // and APPROX_COUNT_DISTINCT(col) now lower to an HLL sketch (see
        // `count_distinct_lowers_to_hll_estimate`); APPROX_QUANTILE_DS and the
        // non-COUNT DISTINCT forms stay fail-closed.
        let schema = test_schema();
        for sql in [
            "SELECT APPROX_QUANTILE_DS(revenue, 0.5) FROM sales",
            "SELECT SUM(DISTINCT revenue) FROM sales",
            "SELECT AVG(DISTINCT revenue) FROM sales",
            // Non-column DISTINCT args stay fail-closed too.
            "SELECT COUNT(DISTINCT UPPER(city)) FROM sales",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            assert!(
                plan_sql(&stmt, &schema).is_err(),
                "unsupported aggregate must fail closed: {sql}"
            );
        }
        // Supported aggregates still plan.
        let stmt = parse_druid_sql("SELECT city, SUM(revenue) AS s FROM sales GROUP BY city")
            .expect("parse");
        assert!(plan_sql(&stmt, &schema).is_ok(), "SUM must still plan");
    }

    /// The not-null selector filter JSON the planner emits for null-aware
    /// counting (`COUNT(col)` and the hidden AVG denominator).
    fn not_null_filter_json_for(column: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "not",
            "field": {"type": "selector", "dimension": column, "value": null}
        })
    }

    #[test]
    fn count_with_column_arg_lowers_to_filtered_not_null_count() {
        // Null-semantics T2: `COUNT(<col>)` lowers to a visible
        // `Filtered{not-null selector} -> Count` named by the alias (BIGINT),
        // matching Druid's non-null count. (Previously fail-closed, DD R42.)
        let schema = test_schema();
        let stmt = parse_druid_sql("SELECT COUNT(city) AS c FROM sales").expect("parse");
        let planned = plan_sql(&stmt, &schema).expect("COUNT(col) must plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries");
        };
        let AggregatorSpec::Filtered { filter, aggregator } = &ts.aggregations[0] else {
            panic!(
                "COUNT(col) must lower to a filtered aggregator, got {:?}",
                ts.aggregations[0]
            );
        };
        assert_eq!(*filter, not_null_filter_json_for("city"));
        assert!(
            matches!(aggregator.as_ref(), AggregatorSpec::Count { name } if name == "c"),
            "inner aggregator must be Count named by the alias, got {aggregator:?}"
        );
        assert_eq!(planned.output_columns.len(), 1);
        assert_eq!(planned.output_columns[0].name, "c");
        assert_eq!(planned.output_columns[0].sql_type, crate::SqlType::Bigint);

        // Grouped form plans too.
        let stmt = parse_druid_sql("SELECT city, COUNT(revenue) AS c FROM sales GROUP BY city")
            .expect("parse");
        assert!(
            plan_sql(&stmt, &schema).is_ok(),
            "grouped COUNT(col) must plan"
        );

        // SUM(x)/COUNT(x) arithmetic now plans (hidden filtered count operand).
        let stmt = parse_druid_sql(
            "SELECT city, SUM(revenue) / COUNT(revenue) AS x FROM sales GROUP BY city",
        )
        .expect("parse");
        assert!(
            plan_sql(&stmt, &schema).is_ok(),
            "SUM(x)/COUNT(x) must plan with a hidden filtered count operand"
        );

        // A non-column COUNT argument stays fail-closed.
        let stmt = parse_druid_sql("SELECT COUNT(UPPER(city)) FROM sales").expect("parse");
        assert!(
            plan_sql(&stmt, &schema).is_err(),
            "COUNT(<non-column expr>) must fail closed"
        );

        // The row-count forms `COUNT(*)` and `COUNT(1)` still plan as plain counts.
        for sql in [
            "SELECT COUNT(*) AS c FROM sales",
            "SELECT COUNT(1) AS c FROM sales",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let planned = plan_sql(&stmt, &schema).expect("row-count COUNT must plan");
            let DruidQuery::Timeseries(ref ts) = planned.native_query else {
                panic!("expected Timeseries for {sql}");
            };
            assert!(
                matches!(&ts.aggregations[0], AggregatorSpec::Count { .. }),
                "row-count COUNT must stay a plain count: {sql}"
            );
        }
    }

    #[test]
    fn count_distinct_lowers_to_hll_estimate() {
        // Null-semantics T3: COUNT(DISTINCT col) and APPROX_COUNT_DISTINCT(col)
        // both lower to a hidden HLLSketchBuild + HLLSketchEstimate{round:true}
        // post-agg named by the alias, typed BIGINT. This matches Druid's
        // DEFAULT (useApproximateCountDistinct=true); the exact mode (the
        // flag off) is covered by `count_distinct_exact_mode_*` below.
        let schema = test_schema();
        for sql in [
            "SELECT COUNT(DISTINCT city) AS dc FROM sales",
            "SELECT APPROX_COUNT_DISTINCT(city) AS dc FROM sales",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let planned = plan_sql(&stmt, &schema).expect("count-distinct must plan");
            let DruidQuery::Timeseries(ref ts) = planned.native_query else {
                panic!("expected Timeseries for {sql}");
            };
            let sketch_name = ts
                .aggregations
                .iter()
                .find_map(|a| match a {
                    AggregatorSpec::HllSketchBuild {
                        name, field_name, ..
                    } if field_name == "city" && name.starts_with('$') => Some(name.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("hidden HLLSketchBuild missing for {sql}"));
            let post_aggs = ts.post_aggregations.as_ref().expect("post aggs");
            assert_eq!(post_aggs.len(), 1, "sql = {sql}");
            let PostAggregatorSpec::HllSketchEstimate { name, field, round } = &post_aggs[0] else {
                panic!("expected HllSketchEstimate, got {:?}", post_aggs[0]);
            };
            assert_eq!(name, "dc");
            assert_eq!(*round, Some(true), "estimate must round (BIGINT result)");
            assert!(
                matches!(field.as_ref(), PostAggregatorSpec::FieldAccess { field_name, .. } if *field_name == sketch_name),
                "estimate must access the hidden sketch, got {field:?}"
            );
            // Wire typing: the count-distinct output is BIGINT (integer on the
            // wire, not 3.0).
            assert_eq!(planned.output_columns.len(), 1, "sql = {sql}");
            assert_eq!(planned.output_columns[0].name, "dc");
            assert_eq!(
                planned.output_columns[0].sql_type,
                crate::SqlType::Bigint,
                "count-distinct output must be BIGINT: {sql}"
            );
        }

        // Grouped + AVG mix: AVG output stays DOUBLE while count-distinct is
        // BIGINT (per-output post-agg types).
        let stmt = parse_druid_sql(
            "SELECT city, AVG(revenue) AS avg_r, COUNT(DISTINCT country) AS dc \
             FROM sales GROUP BY city",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("mixed plan");
        let types: Vec<(&str, &crate::SqlType)> = planned
            .output_columns
            .iter()
            .map(|c| (c.name.as_str(), &c.sql_type))
            .collect();
        assert_eq!(
            types,
            vec![
                ("city", &crate::SqlType::Varchar),
                ("avg_r", &crate::SqlType::Double),
                ("dc", &crate::SqlType::Bigint),
            ]
        );
    }

    // ---- E16: exact COUNT(DISTINCT) (useApproximateCountDistinct=false) ----

    /// The exact-mode planner options (`useApproximateCountDistinct: false`).
    fn exact_options() -> PlannerOptions {
        PlannerOptions {
            use_approximate_count_distinct: false,
        }
    }

    /// `true` when `spec` is (or wraps, via `Filtered`) an exact
    /// `cardinality` aggregation.
    fn is_cardinality(spec: &AggregatorSpec) -> bool {
        match spec {
            AggregatorSpec::Cardinality { .. } => true,
            AggregatorSpec::Filtered { aggregator, .. } => is_cardinality(aggregator),
            _ => false,
        }
    }

    /// Assert `spec` is the E16 exact lowering: a not-null `Filtered`
    /// wrapper around a visible single-field `cardinality` aggregation
    /// named `expected_name` over `expected_field` with `byRow: false`.
    fn assert_exact_cardinality(spec: &AggregatorSpec, expected_name: &str, expected_field: &str) {
        let AggregatorSpec::Filtered { filter, aggregator } = spec else {
            panic!("expected not-null Filtered wrapper, got {spec:?}");
        };
        assert_eq!(
            *filter,
            not_null_filter_json_for(expected_field),
            "exact COUNT(DISTINCT) must skip NULLs (SQL semantics; the bare \
             cardinality aggregator would count a __null__ key)"
        );
        let AggregatorSpec::Cardinality {
            name,
            fields,
            by_row,
        } = aggregator.as_ref()
        else {
            panic!("expected exact cardinality aggregation, got {aggregator:?}");
        };
        assert_eq!(
            name, expected_name,
            "cardinality must be named by the alias"
        );
        assert_eq!(fields, &vec![expected_field.to_string()]);
        assert_eq!(
            *by_row,
            Some(false),
            "single-column distinct is byRow:false"
        );
    }

    #[test]
    fn count_distinct_exact_mode_lowers_to_cardinality() {
        // E16 (failing-first): with `useApproximateCountDistinct=false`,
        // COUNT(DISTINCT col) lowers to a VISIBLE exact `cardinality`
        // aggregation named by the alias — no HLL sketch, no
        // post-aggregation. Output stays BIGINT.
        let stmt = parse_druid_sql("SELECT COUNT(DISTINCT city) AS dc FROM sales").expect("parse");
        let planned = plan_sql_with_options(&stmt, &test_schema(), exact_options())
            .expect("exact count-distinct must plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries, got {:?}", planned.native_query);
        };
        assert_eq!(ts.aggregations.len(), 1, "one visible aggregation");
        assert_exact_cardinality(&ts.aggregations[0], "dc", "city");
        assert!(
            !ts.aggregations
                .iter()
                .any(|a| matches!(a, AggregatorSpec::HllSketchBuild { .. })),
            "exact mode must not build an HLL sketch"
        );
        assert!(
            ts.post_aggregations.is_none(),
            "exact mode needs no post-aggregation, got {:?}",
            ts.post_aggregations
        );
        assert_eq!(planned.output_columns.len(), 1);
        assert_eq!(planned.output_columns[0].name, "dc");
        assert_eq!(
            planned.output_columns[0].sql_type,
            crate::SqlType::Bigint,
            "exact count-distinct output must stay BIGINT"
        );
    }

    #[test]
    fn count_distinct_exact_mode_group_by_lowers_to_cardinality() {
        // E16: the grouped form carries the visible cardinality aggregation
        // into the GroupBy lowering (dimension column + BIGINT count).
        let stmt =
            parse_druid_sql("SELECT city, COUNT(DISTINCT country) AS dc FROM sales GROUP BY city")
                .expect("parse");
        let planned = plan_sql_with_options(&stmt, &test_schema(), exact_options()).expect("plan");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy, got {:?}", planned.native_query);
        };
        let spec = gb
            .aggregations
            .iter()
            .find(|a| is_cardinality(a))
            .expect("visible exact cardinality aggregation in GroupBy");
        assert_exact_cardinality(spec, "dc", "country");
        assert!(
            !gb.aggregations
                .iter()
                .any(|a| matches!(a, AggregatorSpec::HllSketchBuild { .. })),
            "exact mode must not build an HLL sketch"
        );
        let types: Vec<(&str, &crate::SqlType)> = planned
            .output_columns
            .iter()
            .map(|c| (c.name.as_str(), &c.sql_type))
            .collect();
        assert_eq!(
            types,
            vec![
                ("city", &crate::SqlType::Varchar),
                ("dc", &crate::SqlType::Bigint),
            ]
        );
    }

    #[test]
    fn count_distinct_default_options_stay_hll() {
        // E16 regression guard: the DEFAULT (`plan_sql` and explicit
        // `PlannerOptions::default()`) keeps the deep-match-verified
        // approximate HLL lowering — no cardinality aggregation anywhere.
        let stmt = parse_druid_sql("SELECT COUNT(DISTINCT city) AS dc FROM sales").expect("parse");
        for planned in [
            plan_sql(&stmt, &test_schema()).expect("default plan"),
            plan_sql_with_options(&stmt, &test_schema(), PlannerOptions::default())
                .expect("default-options plan"),
        ] {
            let DruidQuery::Timeseries(ref ts) = planned.native_query else {
                panic!("expected Timeseries");
            };
            assert!(
                ts.aggregations
                    .iter()
                    .any(|a| matches!(a, AggregatorSpec::HllSketchBuild { .. })),
                "default mode must keep the hidden HLL sketch"
            );
            assert!(
                !ts.aggregations.iter().any(is_cardinality),
                "default mode must not plan a cardinality aggregation"
            );
        }
    }

    #[test]
    fn approx_count_distinct_stays_hll_in_exact_mode() {
        // E16: APPROX_COUNT_DISTINCT is an explicit approximate request —
        // Druid keeps it on the HLL path even with
        // `useApproximateCountDistinct=false`.
        let stmt =
            parse_druid_sql("SELECT APPROX_COUNT_DISTINCT(city) AS adc FROM sales").expect("parse");
        let planned = plan_sql_with_options(&stmt, &test_schema(), exact_options()).expect("plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries");
        };
        assert!(
            ts.aggregations
                .iter()
                .any(|a| matches!(a, AggregatorSpec::HllSketchBuild { .. })),
            "APPROX_COUNT_DISTINCT must keep the HLL sketch in exact mode"
        );
        assert!(
            !ts.aggregations.iter().any(is_cardinality),
            "APPROX_COUNT_DISTINCT must not become a cardinality aggregation"
        );
        let post_aggs = ts.post_aggregations.as_ref().expect("post aggs");
        assert!(
            matches!(
                &post_aggs[0],
                PostAggregatorSpec::HllSketchEstimate { name, .. } if name == "adc"
            ),
            "estimate post-agg named by the alias, got {:?}",
            post_aggs[0]
        );
    }

    #[test]
    fn count_distinct_exact_mode_non_column_fails_closed() {
        // E16: non-column DISTINCT args and non-COUNT DISTINCT aggregates
        // stay fail-closed in exact mode, exactly as in approximate mode.
        for sql in [
            "SELECT COUNT(DISTINCT UPPER(city)) FROM sales",
            "SELECT SUM(DISTINCT revenue) FROM sales",
            "SELECT AVG(DISTINCT revenue) FROM sales",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            assert!(
                plan_sql_with_options(&stmt, &test_schema(), exact_options()).is_err(),
                "must stay fail-closed in exact mode: {sql}"
            );
        }
    }

    #[test]
    fn count_distinct_exact_mode_arithmetic_operand_uses_hidden_cardinality() {
        // E16: COUNT(DISTINCT col) as an arithmetic operand lowers to a
        // HIDDEN not-null-filtered cardinality aggregation referenced via
        // fieldAccess (the HLL estimate node is not used in exact mode).
        let stmt =
            parse_druid_sql("SELECT COUNT(DISTINCT city) * 2 AS x FROM sales").expect("parse");
        let planned = plan_sql_with_options(&stmt, &test_schema(), exact_options()).expect("plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries");
        };
        let hidden = ts
            .aggregations
            .iter()
            .find_map(|a| match a {
                AggregatorSpec::Filtered { aggregator, .. } => match aggregator.as_ref() {
                    AggregatorSpec::Cardinality { name, .. } if name.starts_with('$') => {
                        Some(name.clone())
                    }
                    _ => None,
                },
                _ => None,
            })
            .expect("hidden exact cardinality aggregation");
        assert!(
            !ts.aggregations
                .iter()
                .any(|a| matches!(a, AggregatorSpec::HllSketchBuild { .. })),
            "exact mode must not build an HLL sketch"
        );
        let post_aggs = ts.post_aggregations.as_ref().expect("post aggs");
        let PostAggregatorSpec::Arithmetic { fields, .. } = &post_aggs[0] else {
            panic!("expected arithmetic post-agg, got {:?}", post_aggs[0]);
        };
        assert!(
            matches!(
                &fields[0],
                PostAggregatorSpec::FieldAccess { field_name, .. } if *field_name == hidden
            ),
            "arithmetic operand must fieldAccess the hidden cardinality, got {:?}",
            fields[0]
        );
    }

    // ---- W2-C: AVG lowering + arithmetic post-aggregations ----

    use ferrodruid_aggregator::PostAggregatorSpec;

    /// Find the hidden (`$`-prefixed) doubleSum-over-`field` and the hidden
    /// not-null-filtered count aggregator names in an aggregation list.
    ///
    /// Null-semantics T1: the AVG denominator is a `Filtered{not-null
    /// selector} -> Count` (SQL AVG ignores NULLs), not a plain row count.
    fn hidden_avg_parts(aggs: &[AggregatorSpec], field: &str) -> (String, String) {
        let sum = aggs
            .iter()
            .find_map(|a| match a {
                AggregatorSpec::DoubleSum { name, field_name }
                    if field_name == field && name.starts_with('$') =>
                {
                    Some(name.clone())
                }
                _ => None,
            })
            .expect("hidden doubleSum aggregator");
        let count = aggs
            .iter()
            .find_map(|a| match a {
                AggregatorSpec::Filtered { filter, aggregator } => match aggregator.as_ref() {
                    AggregatorSpec::Count { name } if name.starts_with('$') => {
                        assert_eq!(
                            *filter,
                            not_null_filter_json_for(field),
                            "AVG denominator filter must be the not-null selector on {field}"
                        );
                        Some(name.clone())
                    }
                    _ => None,
                },
                _ => None,
            })
            .expect("hidden not-null-filtered count aggregator");
        (sum, count)
    }

    #[test]
    fn avg_lowers_to_expression_post_agg_in_timeseries() {
        // Null-semantics T1: AVG finalizes via an `expression` post-agg
        // (`"$avg_sum_N" / "$avg_count_N"`) whose 0/0 -> None -> SQL null
        // matches Druid's all-null-group AVG. (Arithmetic `/` would give 0.)
        let stmt = parse_druid_sql("SELECT AVG(revenue) AS avg_r FROM sales").expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("AVG must plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries");
        };
        let (sum, count) = hidden_avg_parts(&ts.aggregations, "revenue");
        let post_aggs = ts.post_aggregations.as_ref().expect("post aggregations");
        assert_eq!(post_aggs.len(), 1);
        let PostAggregatorSpec::Expression { name, expression } = &post_aggs[0] else {
            panic!("expected Expression post-agg, got {:?}", post_aggs[0]);
        };
        assert_eq!(name, "avg_r");
        assert_eq!(*expression, format!("(\"{sum}\" / \"{count}\")"));
        // Only the AVG alias is an output column — hidden aggs are internal.
        assert_eq!(planned.output_columns.len(), 1);
        assert_eq!(planned.output_columns[0].name, "avg_r");
        assert_eq!(planned.output_columns[0].sql_type, crate::SqlType::Double);
    }

    #[test]
    fn avg_lowers_in_groupby_with_positional_output() {
        let stmt = parse_druid_sql(
            "SELECT city, AVG(revenue) AS avg_r, COUNT(*) AS cnt FROM sales GROUP BY city",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("AVG must plan in GROUP BY");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy");
        };
        let (_sum, _count) = hidden_avg_parts(&gb.aggregations, "revenue");
        let post_aggs = gb.post_aggregations.as_ref().expect("post aggregations");
        assert_eq!(post_aggs.len(), 1);
        assert_eq!(post_aggs[0].name(), "avg_r");
        // Output columns: city, avg_r, cnt — in projection order, no hidden names.
        let names: Vec<&str> = planned
            .output_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["city", "avg_r", "cnt"]);
    }

    #[test]
    fn avg_order_by_alias_resolves_in_groupby() {
        // Two grouping dims => GroupBy (not TopN); ORDER BY the AVG alias must
        // resolve to the post-aggregation output.
        let stmt = parse_druid_sql(
            "SELECT city, country, AVG(revenue) AS avg_r FROM sales \
             GROUP BY city, country ORDER BY avg_r DESC LIMIT 5",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("ORDER BY avg alias must plan");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy");
        };
        let limit_spec = gb.limit_spec.as_ref().expect("limit spec");
        let cols = limit_spec.columns.as_ref().expect("order columns");
        assert_eq!(cols[0].dimension, "avg_r");
        assert_eq!(cols[0].dimension_order.as_deref(), Some("numeric"));
    }

    #[test]
    fn avg_order_by_alias_plans_topn() {
        let stmt = parse_druid_sql(
            "SELECT city, AVG(revenue) AS avg_r FROM sales \
             GROUP BY city ORDER BY avg_r DESC LIMIT 5",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("AVG must plan in TopN path");
        let DruidQuery::TopN(ref tq) = planned.native_query else {
            panic!("expected TopN, got {:?}", planned.native_query);
        };
        let (_sum, _count) = hidden_avg_parts(&tq.aggregations, "revenue");
        assert!(tq.post_aggregations.is_some(), "post aggs must be threaded");
        assert!(
            matches!(&tq.metric, ferrodruid_query::topn::TopNMetricSpec::Numeric { metric } if metric == "avg_r"),
            "metric must be the AVG output, got {:?}",
            tq.metric
        );
        let names: Vec<&str> = planned
            .output_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["city", "avg_r"]);
    }

    #[test]
    fn sum_div_count_lowers_to_arithmetic_post_agg() {
        // The exact silent-drop repro: SUM(x)/COUNT(*) must become an
        // arithmetic post-agg, not vanish from the output.
        let stmt = parse_druid_sql(
            "SELECT city, SUM(revenue) / COUNT(*) AS avg_v FROM sales GROUP BY city",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("SUM/COUNT must plan");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy");
        };
        let post_aggs = gb.post_aggregations.as_ref().expect("post aggregations");
        let PostAggregatorSpec::Arithmetic { name, fn_name, .. } = &post_aggs[0] else {
            panic!("expected Arithmetic post-agg");
        };
        assert_eq!(name, "avg_v");
        assert_eq!(fn_name, "/");
        let names: Vec<&str> = planned
            .output_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["city", "avg_v"]);
    }

    #[test]
    fn sum_times_constant_lowers_constant_operand() {
        let stmt =
            parse_druid_sql("SELECT city, SUM(revenue) * 100 AS pct FROM sales GROUP BY city")
                .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("SUM*const must plan");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy");
        };
        let post_aggs = gb.post_aggregations.as_ref().expect("post aggregations");
        let PostAggregatorSpec::Arithmetic {
            fn_name, fields, ..
        } = &post_aggs[0]
        else {
            panic!("expected Arithmetic post-agg");
        };
        assert_eq!(fn_name, "*");
        assert!(
            matches!(&fields[1], PostAggregatorSpec::Constant { value, .. } if value.as_i64() == Some(100)),
            "second operand must be the constant 100, got {:?}",
            fields[1]
        );
    }

    #[test]
    fn nested_arithmetic_over_aggregates_lowers() {
        let stmt = parse_druid_sql(
            "SELECT city, (SUM(revenue) + SUM(quantity)) / COUNT(*) AS blended \
             FROM sales GROUP BY city",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("nested arithmetic must plan");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy");
        };
        let post_aggs = gb.post_aggregations.as_ref().expect("post aggregations");
        let PostAggregatorSpec::Arithmetic {
            name,
            fn_name,
            fields,
        } = &post_aggs[0]
        else {
            panic!("expected Arithmetic post-agg");
        };
        assert_eq!(name, "blended");
        assert_eq!(fn_name, "/");
        assert!(
            matches!(&fields[0], PostAggregatorSpec::Arithmetic { fn_name, .. } if fn_name == "+"),
            "numerator must be a nested + arithmetic, got {:?}",
            fields[0]
        );
    }

    #[test]
    fn avg_in_timeseries_granular_path() {
        let stmt = parse_druid_sql(
            "SELECT TIME_FLOOR(__time, 'PT1H') AS t, AVG(revenue) AS avg_r \
             FROM sales GROUP BY 1",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("granular AVG must plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries");
        };
        assert!(ts.post_aggregations.is_some());
        let names: Vec<&str> = planned
            .output_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["t", "avg_r"]);
    }

    #[test]
    fn non_grouped_projection_fails_closed_not_silent() {
        // Part 3 — a projection that is neither a supported aggregate nor a
        // grouping key must ERROR, never silently vanish from the output.
        // (ROUND(AVG(x), 1) and SUM(x)/COUNT(x), formerly in this list, now
        // lower — see `round_over_avg_lowers_to_expression` and
        // `count_with_column_arg_lowers_to_filtered_not_null_count`.)
        let schema = test_schema();
        for sql in [
            // A bare column that is not in the GROUP BY.
            "SELECT country, COUNT(*) AS c FROM sales GROUP BY city",
            // Ungrouped bare column with no GROUP BY at all.
            "SELECT city, COUNT(*) AS c FROM sales",
            // ROUND over a non-aggregate expression in a grouped query still
            // has no output slot — fail closed, not silent drop.
            "SELECT city, ROUND(revenue, 1) AS r, COUNT(*) AS c FROM sales GROUP BY city",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            assert!(
                plan_sql(&stmt, &schema).is_err(),
                "must fail closed, not silently drop the projection: {sql}"
            );
        }
    }

    // ---- Null-semantics T4: function-over-aggregate projections ----

    #[test]
    fn round_over_avg_lowers_to_expression() {
        // Ground truth #4: ROUND(AVG("value"), 1) plans and finalizes via an
        // `expression` post-agg (round is decimal half-up; null propagates).
        let stmt = parse_druid_sql(
            "SELECT city, ROUND(AVG(revenue), 1) AS avg_rev, COUNT(*) AS readings \
             FROM sales GROUP BY city ORDER BY avg_rev DESC",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("ROUND(AVG) must plan");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy");
        };
        let (sum, count) = hidden_avg_parts(&gb.aggregations, "revenue");
        let post_aggs = gb.post_aggregations.as_ref().expect("post aggs");
        let PostAggregatorSpec::Expression { name, expression } = &post_aggs[0] else {
            panic!("expected Expression post-agg, got {:?}", post_aggs[0]);
        };
        assert_eq!(name, "avg_rev");
        assert_eq!(*expression, format!("round((\"{sum}\" / \"{count}\"), 1)"));
        // ORDER BY the alias resolves to the post-agg output.
        let cols = gb
            .limit_spec
            .as_ref()
            .and_then(|l| l.columns.as_ref())
            .expect("order columns");
        assert_eq!(cols[0].dimension, "avg_rev");
        assert_eq!(cols[0].dimension_order.as_deref(), Some("numeric"));
        // Output order + types: city VARCHAR, avg_rev DOUBLE, readings BIGINT.
        let names: Vec<&str> = planned
            .output_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["city", "avg_rev", "readings"]);
        assert_eq!(planned.output_columns[1].sql_type, crate::SqlType::Double);
    }

    #[test]
    fn abs_floor_ceil_and_nested_round_over_aggregates_lower() {
        let schema = test_schema();

        // ABS over a plain aggregate.
        let stmt = parse_druid_sql("SELECT ABS(SUM(revenue)) AS a FROM sales").expect("parse");
        let planned = plan_sql(&stmt, &schema).expect("ABS(SUM) must plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries");
        };
        let hidden = ts
            .aggregations
            .iter()
            .find_map(|s| match s {
                AggregatorSpec::DoubleSum { name, field_name }
                    if field_name == "revenue" && name.starts_with('$') =>
                {
                    Some(name.clone())
                }
                _ => None,
            })
            .expect("hidden sum");
        let post_aggs = ts.post_aggregations.as_ref().expect("post aggs");
        let PostAggregatorSpec::Expression { name, expression } = &post_aggs[0] else {
            panic!("expected Expression post-agg");
        };
        assert_eq!(name, "a");
        assert_eq!(*expression, format!("abs(\"{hidden}\")"));

        // FLOOR / CEIL over aggregates plan.
        for sql in [
            "SELECT FLOOR(AVG(revenue)) AS f FROM sales",
            "SELECT CEIL(SUM(revenue)) AS c FROM sales",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            assert!(plan_sql(&stmt, &schema).is_ok(), "must plan: {sql}");
        }

        // ROUND over arithmetic-over-aggregates composes recursively.
        let stmt = parse_druid_sql(
            "SELECT city, ROUND(SUM(revenue) / COUNT(*), 2) AS r FROM sales GROUP BY city",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &schema).expect("ROUND(SUM/COUNT) must plan");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy");
        };
        let post_aggs = gb.post_aggregations.as_ref().expect("post aggs");
        let PostAggregatorSpec::Expression { expression, .. } = &post_aggs[0] else {
            panic!("expected Expression post-agg");
        };
        assert!(
            expression.starts_with("round((\"$") && expression.ends_with(", 2)"),
            "nested arithmetic must render inside round(..., 2): {expression}"
        );
    }

    #[test]
    fn order_by_avg_expression_resolves() {
        // ORDER BY the AVG *expression* (not the alias) must resolve to the
        // post-aggregation output in both the GroupBy and TopN paths.
        let stmt = parse_druid_sql(
            "SELECT city, country, AVG(revenue) AS avg_r FROM sales \
             GROUP BY city, country ORDER BY AVG(revenue) DESC LIMIT 5",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("ORDER BY AVG(expr) must plan");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy");
        };
        let cols = gb
            .limit_spec
            .as_ref()
            .and_then(|l| l.columns.as_ref())
            .expect("order columns");
        assert_eq!(cols[0].dimension, "avg_r");

        // Unaliased AVG in the TopN path: default output name `avg_revenue`.
        let stmt = parse_druid_sql(
            "SELECT city, AVG(revenue) FROM sales \
             GROUP BY city ORDER BY AVG(revenue) DESC LIMIT 3",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("unaliased AVG must plan");
        let DruidQuery::TopN(ref tq) = planned.native_query else {
            panic!("expected TopN, got {:?}", planned.native_query);
        };
        assert!(
            matches!(&tq.metric, ferrodruid_query::topn::TopNMetricSpec::Numeric { metric } if metric == "avg_revenue"),
            "metric must be the default AVG output name, got {:?}",
            tq.metric
        );
    }

    #[test]
    fn avg_distinct_still_rejected() {
        let stmt = parse_druid_sql("SELECT AVG(DISTINCT revenue) FROM sales").expect("parse");
        assert!(
            plan_sql(&stmt, &test_schema()).is_err(),
            "AVG(DISTINCT ...) must fail closed"
        );
    }

    #[test]
    fn plan_group_by_order_limit_to_topn() {
        let sql =
            "SELECT city, COUNT(*) AS cnt FROM sales GROUP BY city ORDER BY cnt DESC LIMIT 10";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert!(
            matches!(planned.native_query, DruidQuery::TopN(_)),
            "Expected TopN, got: {:?}",
            std::mem::discriminant(&planned.native_query)
        );
        if let DruidQuery::TopN(ref q) = planned.native_query {
            assert_eq!(q.threshold, 10);
        }
    }

    #[test]
    fn plan_no_aggregation_to_scan() {
        let sql = "SELECT * FROM sales WHERE city = 'tokyo'";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert!(matches!(planned.native_query, DruidQuery::Scan(_)));
    }

    #[test]
    fn plan_time_floor_to_timeseries() {
        let sql = "SELECT TIME_FLOOR(__time, 'PT1H') AS t, COUNT(*) AS cnt FROM sales GROUP BY 1";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert!(matches!(planned.native_query, DruidQuery::Timeseries(_)));
        if let DruidQuery::Timeseries(ref q) = planned.native_query {
            match &q.granularity {
                GranularitySpec::Simple(s) => assert_eq!(s, "hour"),
                _ => panic!("expected simple granularity"),
            }
        }
    }

    /// Codex-review HIGH finding C: the TIME_FLOOR bucket column is marked
    /// on the plan BY ROLE — even when its alias collides with a hidden
    /// `$`-helper aggregation name — while TIMESTAMP-typed aggregates
    /// (`MAX(__time)`, P1-#2) and non-granular plans are never marked.
    #[test]
    fn time_bucket_column_marked_by_role() {
        // Granular timeseries: marked with the SELECT alias.
        let stmt = parse_druid_sql(
            "SELECT TIME_FLOOR(__time, 'PT1H') AS t, COUNT(*) AS cnt FROM sales GROUP BY 1",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(planned.time_bucket_column.as_deref(), Some("t"));

        // The alias colliding with AVG's hidden `$avg_sum_0` helper stays
        // marked (the collision used to drop the bucket role at the REST
        // layer's name-based inference).
        let stmt = parse_druid_sql(
            "SELECT TIME_FLOOR(__time, 'PT1H') AS \"$avg_sum_0\", AVG(revenue) AS a \
             FROM sales GROUP BY 1",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(planned.time_bucket_column.as_deref(), Some("$avg_sum_0"));

        // Granular GroupBy (bucket + another dimension): marked.
        let stmt = parse_druid_sql(
            "SELECT TIME_FLOOR(__time, 'PT1H') AS t, city, COUNT(*) AS cnt \
             FROM sales GROUP BY 1, city",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert!(matches!(planned.native_query, DruidQuery::GroupBy(_)));
        assert_eq!(planned.time_bucket_column.as_deref(), Some("t"));

        // A TIMESTAMP-typed aggregate output is NOT the bucket.
        let stmt = parse_druid_sql("SELECT MAX(__time) AS mx FROM sales").expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(planned.time_bucket_column, None);

        // Plain grouped / scan plans carry no bucket either.
        let stmt = parse_druid_sql("SELECT city, COUNT(*) AS cnt FROM sales GROUP BY city")
            .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(planned.time_bucket_column, None);
    }

    #[test]
    fn plan_multiple_aggregations() {
        let sql = "SELECT city, SUM(revenue) AS rev, COUNT(*) AS cnt FROM sales GROUP BY city";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert!(matches!(planned.native_query, DruidQuery::GroupBy(_)));
        if let DruidQuery::GroupBy(ref q) = planned.native_query {
            assert_eq!(q.aggregations.len(), 2);
        }
    }

    #[test]
    fn plan_where_clause_filter() {
        let sql = "SELECT COUNT(*) AS cnt FROM sales WHERE city = 'tokyo'";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        if let DruidQuery::Timeseries(ref q) = planned.native_query {
            assert!(q.filter.is_some());
        } else {
            panic!("expected timeseries");
        }
    }

    #[test]
    fn plan_having_clause() {
        let sql = "SELECT city, COUNT(*) AS cnt FROM sales GROUP BY city HAVING cnt > 10";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        if let DruidQuery::GroupBy(ref q) = planned.native_query {
            assert!(q.having.is_some());
        } else {
            panic!("expected groupby");
        }
    }

    #[test]
    fn plan_explain_returns_same_native_query() {
        let sql = "EXPLAIN SELECT COUNT(*) AS cnt FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        // EXPLAIN still produces the same native query; the REST layer checks the statement type.
        assert!(matches!(planned.native_query, DruidQuery::Timeseries(_)));
    }

    #[test]
    fn plan_scan_with_limit_offset() {
        let sql = "SELECT city, revenue FROM sales LIMIT 10 OFFSET 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        if let DruidQuery::Scan(ref q) = planned.native_query {
            assert_eq!(q.limit, Some(10));
            assert_eq!(q.offset, Some(5));
        } else {
            panic!("expected scan");
        }
    }

    /// Null-semantics T5: a scan `ORDER BY <non-time column>` fails closed
    /// with Druid's error shape instead of silently returning time-ordered
    /// rows (the pre-fix behaviour took only the DIRECTION of the first key).
    #[test]
    fn plan_scan_order_by_non_time_fails_closed() {
        let schema = test_schema();
        for (sql, col) in [
            ("SELECT city FROM sales ORDER BY city ASC", "city"),
            ("SELECT * FROM sales ORDER BY revenue DESC", "revenue"),
            // Multi-key: __time first does not legitimise a non-time key.
            (
                "SELECT __time, city FROM sales ORDER BY __time, city",
                "city",
            ),
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let err = plan_sql(&stmt, &schema).expect_err("must fail closed");
            let msg = err.to_string();
            assert!(
                msg.contains(&format!("non-time column [[{col}]]"))
                    && msg.contains("not supported"),
                "expected Druid-shaped ordering error naming [[{col}]] for [{sql}], got: {msg}"
            );
        }
    }

    /// Null-semantics T5: ORDER BY the time column (bare, aliased, or by
    /// ordinal on a wildcard scan) keeps working exactly as before.
    #[test]
    fn plan_scan_order_by_time_still_plans() {
        let schema = test_schema();
        for (sql, dir) in [
            (
                "SELECT __time, city FROM sales ORDER BY __time ASC",
                "ascending",
            ),
            ("SELECT * FROM sales ORDER BY __time DESC", "descending"),
            (
                "SELECT __time AS t, city FROM sales ORDER BY t DESC",
                "descending",
            ),
            ("SELECT * FROM sales ORDER BY 1 ASC", "ascending"),
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let planned = plan_sql(&stmt, &schema)
                .unwrap_or_else(|e| panic!("time ordering must plan for [{sql}]: {e}"));
            let DruidQuery::Scan(ref q) = planned.native_query else {
                panic!("expected scan for {sql}");
            };
            assert_eq!(q.order.as_deref(), Some(dir), "sql = {sql}");
        }
    }

    /// Null-semantics T7 (planner side): an explicit `__time` scan projection
    /// is typed TIMESTAMP so the REST layer renders it as an ISO-8601 string
    /// (Druid SQL wire shape), matching the wildcard scan's typing.
    #[test]
    fn plan_scan_explicit_time_column_typed_timestamp() {
        let stmt = parse_druid_sql("SELECT __time, city FROM sales").expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(planned.output_columns[0].name, "__time");
        assert_eq!(
            planned.output_columns[0].sql_type,
            crate::SqlType::Timestamp
        );
        assert_eq!(planned.output_columns[1].sql_type, crate::SqlType::Varchar);
    }

    // ---- T9: Superset time-grain surface ----

    /// `TIME_FLOOR(CAST(__time AS TIMESTAMP), p)` — the exact shape Superset
    /// emits for every time grain — plans like the bare-column form (the CAST
    /// is a no-op on the time column).
    #[test]
    fn time_floor_over_cast_timestamp_plans() {
        let stmt = parse_druid_sql(
            "SELECT TIME_FLOOR(CAST(__time AS TIMESTAMP), 'P1D') AS __timestamp, \
             COUNT(*) AS c FROM sales GROUP BY 1",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries, got {:?}", planned.native_query);
        };
        match &ts.granularity {
            GranularitySpec::Simple(s) => assert_eq!(s, "day"),
            other => panic!("expected simple granularity, got {other:?}"),
        }
        assert_eq!(planned.output_columns[0].name, "__timestamp");
    }

    /// A TIME_FLOOR period that is neither a named grain nor a single-field
    /// fixed time period (compound `PT1H30M`, calendar multiples like `P2M`)
    /// fails closed instead of silently collapsing to `all` (which returned
    /// one giant bucket).
    #[test]
    fn time_floor_unknown_period_fails_closed() {
        for period in ["PT1H30M", "P2M", "P2W", "PT5X", "garbage"] {
            let stmt = parse_druid_sql(&format!(
                "SELECT TIME_FLOOR(__time, '{period}') AS t, COUNT(*) AS c FROM sales GROUP BY 1",
            ))
            .expect("parse");
            let err = plan_sql(&stmt, &test_schema()).expect_err("unknown period must fail closed");
            assert!(
                err.to_string().contains(period),
                "error must name the unsupported period {period}: {err}"
            );
        }
    }

    /// Superset's sub-minute grains (`PT5S` / `PT30S`) — and any single-field
    /// fixed multiple `PTnS`/`PTnM`/`PTnH` — lower to a native fixed-period
    /// `duration` granularity anchored at the epoch, on the Timeseries path
    /// (TIME_FLOOR-only GROUP BY). Previously these failed closed with
    /// "no supported granularity".
    #[test]
    fn time_floor_fixed_period_lowers_to_duration_timeseries() {
        use ferrodruid_common::types::Granularity;
        for (period, expect_ms) in [
            ("PT5S", 5_000_u64),
            ("PT10S", 10_000),
            ("PT15S", 15_000),
            ("PT30S", 30_000),
            ("PT2H", 7_200_000),
        ] {
            let stmt = parse_druid_sql(&format!(
                "SELECT TIME_FLOOR(CAST(__time AS TIMESTAMP), '{period}') AS __timestamp, \
                 COUNT(*) AS c FROM sales GROUP BY 1",
            ))
            .expect("parse");
            let planned = plan_sql(&stmt, &test_schema()).expect("plan");
            let DruidQuery::Timeseries(ref ts) = planned.native_query else {
                panic!(
                    "expected Timeseries for {period}, got {:?}",
                    planned.native_query
                );
            };
            match &ts.granularity {
                GranularitySpec::Full(Granularity::Duration { period_ms, origin }) => {
                    assert_eq!(*period_ms, expect_ms, "period_ms for {period}");
                    assert_eq!(
                        origin.timestamp_millis(),
                        0,
                        "duration granularity must be epoch-anchored for {period}"
                    );
                }
                other => panic!("expected duration granularity for {period}, got {other:?}"),
            }
            assert_eq!(planned.output_columns[0].name, "__timestamp");
        }
    }

    /// Superset's `week_starting_sunday` grain — the nested
    /// `TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(t,'P1D',1),'P1W'),'P1D',-1)` —
    /// lowers to a fixed 7-day duration granularity anchored on a Sunday
    /// (1969-12-28T00:00:00Z, the Sunday before the epoch): buckets are
    /// `[Sunday, Sunday+7d)` labeled by their Sunday start, exactly what the
    /// nested expression computes per row.
    #[test]
    fn week_starting_sunday_lowers_to_sunday_anchored_duration() {
        use ferrodruid_common::types::Granularity;
        let stmt = parse_druid_sql(
            "SELECT TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(CAST(__time AS TIMESTAMP), 'P1D', 1), \
             'P1W'), 'P1D', -1) AS __timestamp, COUNT(*) AS \"count\" FROM sales GROUP BY 1",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries, got {:?}", planned.native_query);
        };
        match &ts.granularity {
            GranularitySpec::Full(Granularity::Duration { period_ms, origin }) => {
                assert_eq!(*period_ms, 604_800_000, "one fixed week");
                assert_eq!(
                    origin.timestamp_millis(),
                    -345_600_000,
                    "anchored on the Sunday before the epoch (1969-12-28)"
                );
            }
            other => panic!("expected duration granularity, got {other:?}"),
        }
        assert_eq!(planned.output_columns[0].name, "__timestamp");
    }

    /// The same shifted-week grain with an additional grouping dimension
    /// takes the GroupBy path with the same lowered granularity.
    #[test]
    fn week_starting_sunday_lowers_on_groupby_path() {
        use ferrodruid_common::types::Granularity;
        let stmt = parse_druid_sql(
            "SELECT city, TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(CAST(__time AS TIMESTAMP), 'P1D', \
             1), 'P1W'), 'P1D', -1) AS __timestamp, COUNT(*) AS c FROM sales GROUP BY city, 2",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy, got {:?}", planned.native_query);
        };
        match &gb.granularity {
            GranularitySpec::Full(Granularity::Duration { period_ms, origin }) => {
                assert_eq!(*period_ms, 604_800_000);
                assert_eq!(origin.timestamp_millis(), -345_600_000);
            }
            other => panic!("expected duration granularity, got {other:?}"),
        }
    }

    /// Superset's `week_ending_saturday` grain (outer shift `5`) labels each
    /// `[Sunday, Sunday+7d)` bucket by the Saturday ENDING it — 6 days after
    /// the bucket start. A floor-to-origin granularity can only label a
    /// bucket by its start, so this grain has no native lowering and must
    /// keep failing closed (not silently mis-label).
    #[test]
    fn week_ending_saturday_stays_fail_closed() {
        let stmt = parse_druid_sql(
            "SELECT TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(CAST(__time AS TIMESTAMP), 'P1D', 1), \
             'P1W'), 'P1D', 5) AS __timestamp, COUNT(*) AS \"count\" FROM sales GROUP BY 1",
        )
        .expect("parse");
        plan_sql(&stmt, &test_schema())
            .expect_err("week_ending_saturday has no native lowering; must fail closed");
    }

    /// The same fixed-period lowering on the GroupBy path (TIME_FLOOR plus a
    /// regular grouping dimension).
    #[test]
    fn time_floor_fixed_period_lowers_to_duration_groupby() {
        use ferrodruid_common::types::Granularity;
        let stmt = parse_druid_sql(
            "SELECT city, TIME_FLOOR(CAST(__time AS TIMESTAMP), 'PT30S') AS __timestamp, \
             COUNT(*) AS c FROM sales GROUP BY city, 2",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy, got {:?}", planned.native_query);
        };
        match &gb.granularity {
            GranularitySpec::Full(Granularity::Duration { period_ms, origin }) => {
                assert_eq!(*period_ms, 30_000);
                assert_eq!(origin.timestamp_millis(), 0);
            }
            other => panic!("expected duration granularity, got {other:?}"),
        }
    }

    /// `WHERE __time >= TIME_PARSE('...')` (Superset temporal filter) lowers
    /// the TIME_PARSE literal to epoch millis in a numeric bound filter;
    /// `CAST(TIME_PARSE('...') AS DATE)` floors to the day.
    #[test]
    fn where_time_parse_literal_lowers_to_numeric_bound() {
        // 2024-01-02T00:00:00Z == 1704153600000 ms.
        for sql in [
            "SELECT COUNT(*) AS c FROM sales WHERE __time >= TIME_PARSE('2024-01-02T00:00:00')",
            "SELECT COUNT(*) AS c FROM sales WHERE __time >= CAST(TIME_PARSE('2024-01-02') AS DATE)",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let planned = plan_sql(&stmt, &test_schema())
                .unwrap_or_else(|e| panic!("TIME_PARSE filter must plan for [{sql}]: {e}"));
            let DruidQuery::Timeseries(ref ts) = planned.native_query else {
                panic!("expected Timeseries for {sql}");
            };
            let Some(FilterSpec::Bound {
                dimension,
                lower,
                ordering,
                ..
            }) = &ts.filter
            else {
                panic!("expected bound filter for {sql}, got {:?}", ts.filter);
            };
            assert_eq!(dimension, "__time");
            assert_eq!(lower.as_deref(), Some("1704153600000"), "sql = {sql}");
            assert_eq!(ordering.as_deref(), Some("numeric"));
        }
    }

    /// P1-#2: `WHERE __time >= TIMESTAMP '...'` (the Calcite/Druid typed
    /// literal Superset's time-range filter emits) must lower to the same
    /// numeric epoch-millis bound as the `TIME_PARSE('...')` path. The
    /// space-separated form, the fractional-seconds form (Superset emits
    /// microseconds), the ISO `T` form, and `DATE '...'` all fold.
    #[test]
    fn where_timestamp_literal_lowers_to_numeric_bound() {
        // 2024-01-02T00:00:00Z == 1704153600000 ms.
        for sql in [
            "SELECT COUNT(*) AS c FROM sales WHERE __time >= TIMESTAMP '2024-01-02 00:00:00'",
            "SELECT COUNT(*) AS c FROM sales \
             WHERE __time >= TIMESTAMP '2024-01-02 00:00:00.000000'",
            "SELECT COUNT(*) AS c FROM sales WHERE __time >= TIMESTAMP '2024-01-02T00:00:00'",
            "SELECT COUNT(*) AS c FROM sales WHERE __time >= DATE '2024-01-02'",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let planned = plan_sql(&stmt, &test_schema())
                .unwrap_or_else(|e| panic!("TIMESTAMP-literal filter must plan for [{sql}]: {e}"));
            let DruidQuery::Timeseries(ref ts) = planned.native_query else {
                panic!("expected Timeseries for {sql}");
            };
            let Some(FilterSpec::Bound {
                dimension,
                lower,
                ordering,
                ..
            }) = &ts.filter
            else {
                panic!("expected bound filter for {sql}, got {:?}", ts.filter);
            };
            assert_eq!(dimension, "__time");
            assert_eq!(lower.as_deref(), Some("1704153600000"), "sql = {sql}");
            assert_eq!(ordering.as_deref(), Some("numeric"), "sql = {sql}");
        }
    }

    /// P1-#2: the Superset time-range shape — a half-open interval of two
    /// TIMESTAMP literals ANDed together — lowers to two numeric bounds.
    #[test]
    fn where_timestamp_literal_range_lowers_to_bounds() {
        let sql = "SELECT COUNT(*) AS c FROM sales \
                   WHERE __time >= TIMESTAMP '2024-01-01 00:00:00' \
                   AND __time < TIMESTAMP '2024-01-02 00:00:00'";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("range must plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries");
        };
        let Some(FilterSpec::And { fields }) = &ts.filter else {
            panic!("expected AND filter, got {:?}", ts.filter);
        };
        let FilterSpec::Bound { lower, .. } = &fields[0] else {
            panic!("expected lower bound, got {:?}", fields[0]);
        };
        // 2024-01-01T00:00:00Z == 1704067200000 ms.
        assert_eq!(lower.as_deref(), Some("1704067200000"));
        let FilterSpec::Bound {
            upper,
            upper_strict,
            ..
        } = &fields[1]
        else {
            panic!("expected upper bound, got {:?}", fields[1]);
        };
        assert_eq!(upper.as_deref(), Some("1704153600000"));
        assert_eq!(*upper_strict, Some(true));
    }

    /// P1-#2: `MIN(__time)` / `MAX(__time)` lower to `longMin` / `longMax`
    /// over the time column (Druid's native lowering) with a TIMESTAMP
    /// output type, so the SQL wire emits the ISO-8601 string, not epoch
    /// millis. Non-time MIN/MAX keeps the doubleMin/BIGINT behaviour.
    #[test]
    fn min_max_time_lowers_to_long_with_timestamp_type() {
        let stmt = parse_druid_sql("SELECT MIN(__time) AS mn, MAX(__time) AS mx FROM sales")
            .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("MIN/MAX(__time) must plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries");
        };
        assert!(
            matches!(
                &ts.aggregations[0],
                AggregatorSpec::LongMin { name, field_name }
                    if name == "mn" && field_name == "__time"
            ),
            "MIN(__time) must lower to longMin, got {:?}",
            ts.aggregations[0]
        );
        assert!(
            matches!(
                &ts.aggregations[1],
                AggregatorSpec::LongMax { name, field_name }
                    if name == "mx" && field_name == "__time"
            ),
            "MAX(__time) must lower to longMax, got {:?}",
            ts.aggregations[1]
        );
        assert_eq!(
            planned.output_columns[0].sql_type,
            crate::SqlType::Timestamp
        );
        assert_eq!(
            planned.output_columns[1].sql_type,
            crate::SqlType::Timestamp
        );

        // Guard: MIN over a non-time metric keeps the existing lowering.
        let stmt = parse_druid_sql("SELECT MIN(revenue) AS mn FROM sales").expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("MIN(revenue) must plan");
        let DruidQuery::Timeseries(ref ts) = planned.native_query else {
            panic!("expected Timeseries");
        };
        assert!(
            matches!(&ts.aggregations[0], AggregatorSpec::DoubleMin { .. }),
            "MIN(non-time) must stay doubleMin, got {:?}",
            ts.aggregations[0]
        );
        assert_eq!(planned.output_columns[0].sql_type, crate::SqlType::Bigint);
    }

    #[test]
    fn plan_in_filter() {
        let sql = "SELECT * FROM sales WHERE city IN ('tokyo', 'london')";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        if let DruidQuery::Scan(ref q) = planned.native_query {
            assert!(matches!(q.filter, Some(FilterSpec::In { .. })));
        } else {
            panic!("expected scan");
        }
    }

    #[test]
    fn plan_like_filter() {
        let sql = "SELECT * FROM sales WHERE city LIKE 'tok%'";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        if let DruidQuery::Scan(ref q) = planned.native_query {
            assert!(matches!(q.filter, Some(FilterSpec::Like { .. })));
        } else {
            panic!("expected scan");
        }
    }

    #[test]
    fn plan_between_filter() {
        let sql = "SELECT * FROM sales WHERE revenue BETWEEN 100 AND 500";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        if let DruidQuery::Scan(ref q) = planned.native_query {
            assert!(matches!(q.filter, Some(FilterSpec::Bound { .. })));
        } else {
            panic!("expected scan");
        }
    }

    #[test]
    fn plan_and_or_filter() {
        let sql = "SELECT * FROM sales WHERE city = 'tokyo' AND revenue > 100";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        if let DruidQuery::Scan(ref q) = planned.native_query {
            assert!(matches!(q.filter, Some(FilterSpec::And { .. })));
        } else {
            panic!("expected scan");
        }
    }

    #[test]
    fn plan_native_query_json_round_trip() {
        let sql = "SELECT city, COUNT(*) AS cnt FROM sales GROUP BY city ORDER BY cnt DESC LIMIT 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        // Verify it serializes to valid JSON.
        let json = serde_json::to_string(&planned.native_query).expect("serialize");
        let _: DruidQuery = serde_json::from_str(&json).expect("deserialize");
    }

    #[test]
    fn plan_union_all() {
        let sql = "SELECT city, revenue FROM sales UNION ALL SELECT city, revenue FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        match planned.native_query {
            DruidQuery::UnionAll(queries) => {
                assert_eq!(queries.len(), 2);
                assert!(matches!(queries[0], DruidQuery::Scan(_)));
                assert!(matches!(queries[1], DruidQuery::Scan(_)));
            }
            _ => panic!("expected UnionAll"),
        }
    }

    #[test]
    fn plan_union_all_concat_output_columns() {
        let sql = "SELECT COUNT(*) AS cnt FROM sales UNION ALL SELECT COUNT(*) AS cnt FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        // Output columns should come from the first sub-query.
        assert_eq!(planned.output_columns.len(), 1);
        assert_eq!(planned.output_columns[0].name, "cnt");
    }

    #[test]
    fn plan_union_all_maps_differently_named_branches_positionally() {
        // Druid names the UNION ALL output from the first branch and maps
        // later branches into it by position, so differently-named branches
        // (same arity) now PLAN — they are aligned positionally at merge
        // time. Output is named from the first branch (`city`).
        let sql = "SELECT city FROM sales UNION ALL SELECT revenue FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema())
            .expect("differently-named same-arity UNION ALL branches must plan");
        assert!(matches!(planned.native_query, DruidQuery::UnionAll(_)));
        assert_eq!(planned.output_columns.len(), 1);
        assert_eq!(planned.output_columns[0].name, "city");
    }

    #[test]
    fn plan_union_all_rejects_repeated_source_column_across_differing_branches() {
        // A branch that projects the SAME source column twice
        // (`city AS x, city AS y`) deduplicates to one native column, which
        // positional cross-branch alignment cannot reconstruct — reject when
        // the branches are not identical (silent value mis-mapping otherwise).
        let sql = "SELECT city AS x, city AS y, country AS z FROM sales \
                   UNION ALL \
                   SELECT revenue AS p, quantity AS q, quantity AS r FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        let err = plan_sql(&stmt, &test_schema())
            .expect_err("repeated-source differing-branch UNION ALL must be rejected");
        assert!(
            format!("{err}").contains("UNION ALL"),
            "error must reference UNION ALL: {err}"
        );
    }

    #[test]
    fn plan_union_all_allows_repeated_source_column_with_matching_pattern() {
        // Both branches repeat their FIRST source column in the same positions
        // (pattern [0,0,2]), so positional alignment is sound even though the
        // branches read different native columns. Must plan (output named
        // from the first branch).
        let sql = "SELECT city AS x, city AS y, country AS z FROM sales \
                   UNION ALL \
                   SELECT revenue AS p, revenue AS q, quantity AS r FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned =
            plan_sql(&stmt, &test_schema()).expect("matching repeated-source pattern must plan");
        assert!(matches!(planned.native_query, DruidQuery::UnionAll(_)));
        assert_eq!(planned.output_columns.len(), 3);
        assert_eq!(planned.output_columns[0].name, "x");
    }

    #[test]
    fn plan_union_all_allows_repeated_source_column_when_branches_identical() {
        // Identical branches (even with a repeated source column) align to a
        // no-op, so they remain supported.
        let sql = "SELECT city AS x, city AS y FROM sales \
                   UNION ALL \
                   SELECT city AS x, city AS y FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema())
            .expect("identical repeated-source UNION ALL branches must plan");
        assert!(matches!(planned.native_query, DruidQuery::UnionAll(_)));
    }

    #[test]
    fn plan_union_all_rejects_mismatched_branch_arity() {
        let sql = "SELECT city FROM sales UNION ALL SELECT city, revenue FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        let err = plan_sql(&stmt, &test_schema())
            .expect_err("different-arity UNION ALL branches must be rejected");
        assert!(
            format!("{err}").contains("UNION ALL"),
            "error must reference UNION ALL"
        );
    }

    #[test]
    fn plan_union_all_maps_same_output_name_different_native_source_positionally() {
        // Both branches alias to the SAME output name `x` but read DIFFERENT
        // native columns (city vs revenue). Positional alignment at merge
        // time maps branch 2's `revenue` into branch 1's native key, so this
        // now PLANS (output named `x` from the first branch).
        let sql = "SELECT city AS x FROM sales UNION ALL SELECT revenue AS x FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema())
            .expect("same-output-name different-native-source branches must plan");
        assert!(matches!(planned.native_query, DruidQuery::UnionAll(_)));
        assert_eq!(planned.output_columns.len(), 1);
        assert_eq!(planned.output_columns[0].name, "x");
    }

    #[test]
    fn plan_union_all_accepts_same_native_source_different_output_alias() {
        // Both branches read the SAME native column `city` but alias it to
        // different OUTPUT names. SQL names the union output from the first
        // branch (`a`); concatenation by native key `city` aligns, so this
        // is valid and must plan.
        let sql = "SELECT city AS a FROM sales UNION ALL SELECT city AS b FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema())
            .expect("same-native-source UNION ALL must plan regardless of output alias");
        assert!(matches!(planned.native_query, DruidQuery::UnionAll(_)));
        assert_eq!(planned.output_columns.len(), 1);
        assert_eq!(planned.output_columns[0].name, "a");
    }

    // ----- Wave 47-D §1: SQL window functions -----

    fn window_schema() -> DataSourceSchema {
        DataSourceSchema {
            name: "wikipedia_compat".to_string(),
            dimensions: vec![
                ColumnSchema {
                    name: "language".to_string(),
                    column_type: ColumnType::String,
                },
                ColumnSchema {
                    name: "page".to_string(),
                    column_type: ColumnType::String,
                },
            ],
            metrics: vec![ColumnSchema {
                name: "added".to_string(),
                column_type: ColumnType::Long,
            }],
            time_column: "__time".to_string(),
            join_schemas: Vec::new(),
        }
    }

    #[test]
    fn plan_row_number_global_to_window_query() {
        let sql = "SELECT page, added, ROW_NUMBER() OVER (ORDER BY added DESC, page ASC) AS rn \
                   FROM wikipedia_compat ORDER BY rn";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &window_schema()).expect("plan");
        let DruidQuery::Window(q) = planned.native_query else {
            panic!("expected Window query");
        };
        assert_eq!(q.windows.len(), 1);
        assert_eq!(q.windows[0].output_name, "rn");
        assert!(matches!(
            q.windows[0].function,
            WindowFunctionKind::RowNumber
        ));
        assert!(q.windows[0].partition_by.is_empty());
        assert_eq!(q.windows[0].order_by.len(), 2);
        // Output projection includes rn so post_order_by(rn) is preserved.
        assert!(q.post_order_by.iter().any(|o| o.column == "rn"));
        // Output columns: page, added, rn
        let names: Vec<&str> = planned
            .output_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["page", "added", "rn"]);
    }

    #[test]
    fn plan_rank_partitioned() {
        let sql = "SELECT language, page, added, RANK() OVER (PARTITION BY language ORDER BY added DESC) AS rk \
                   FROM wikipedia_compat ORDER BY language, rk, page";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &window_schema()).expect("plan");
        let DruidQuery::Window(q) = planned.native_query else {
            panic!("expected Window query");
        };
        assert!(matches!(q.windows[0].function, WindowFunctionKind::Rank));
        assert_eq!(q.windows[0].partition_by, vec!["language".to_string()]);
        assert_eq!(q.post_order_by.len(), 3);
    }

    #[test]
    fn plan_dense_rank_partitioned() {
        let sql = "SELECT language, page, DENSE_RANK() OVER (PARTITION BY language ORDER BY added DESC) AS dr \
                   FROM wikipedia_compat";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &window_schema()).expect("plan");
        let DruidQuery::Window(q) = planned.native_query else {
            panic!("expected Window query");
        };
        assert!(matches!(
            q.windows[0].function,
            WindowFunctionKind::DenseRank
        ));
    }

    #[test]
    fn plan_lag_function() {
        let sql = "SELECT language, page, added, LAG(added, 1) OVER (PARTITION BY language ORDER BY added DESC, page ASC) AS prev_added \
                   FROM wikipedia_compat ORDER BY language, added DESC, page";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &window_schema()).expect("plan");
        let DruidQuery::Window(q) = planned.native_query else {
            panic!("expected Window query");
        };
        match &q.windows[0].function {
            WindowFunctionKind::Lag { column, offset } => {
                assert_eq!(column, "added");
                assert_eq!(*offset, 1);
            }
            other => panic!("expected Lag, got {other:?}"),
        }
    }

    #[test]
    fn plan_lead_function() {
        let sql = "SELECT language, page, added, LEAD(added, 1) OVER (PARTITION BY language ORDER BY added DESC, page ASC) AS next_added \
                   FROM wikipedia_compat";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &window_schema()).expect("plan");
        let DruidQuery::Window(q) = planned.native_query else {
            panic!("expected Window query");
        };
        assert!(matches!(
            q.windows[0].function,
            WindowFunctionKind::Lead { .. }
        ));
    }

    #[test]
    fn plan_sum_over_partition() {
        let sql = "SELECT language, page, added, SUM(added) OVER (PARTITION BY language) AS lang_total \
                   FROM wikipedia_compat";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &window_schema()).expect("plan");
        let DruidQuery::Window(q) = planned.native_query else {
            panic!("expected Window query");
        };
        match &q.windows[0].function {
            WindowFunctionKind::Sum { column } => assert_eq!(column, "added"),
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    #[test]
    fn plan_avg_over_partition() {
        let sql = "SELECT language, page, added, AVG(added) OVER (PARTITION BY language) AS lang_avg \
                   FROM wikipedia_compat";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &window_schema()).expect("plan");
        let DruidQuery::Window(q) = planned.native_query else {
            panic!("expected Window query");
        };
        match &q.windows[0].function {
            WindowFunctionKind::Avg { column } => assert_eq!(column, "added"),
            other => panic!("expected Avg, got {other:?}"),
        }
    }

    #[test]
    fn plan_window_requires_alias() {
        let sql = "SELECT page, ROW_NUMBER() OVER (ORDER BY added DESC) FROM wikipedia_compat";
        let stmt = parse_druid_sql(sql).expect("parse");
        let err = plan_sql(&stmt, &window_schema()).expect_err("must reject unaliased");
        assert!(err.to_string().contains("must be aliased"));
    }

    // -----------------------------------------------------------------
    // CTE planning: FROM <cte> lowers to a `query` data source.
    // -----------------------------------------------------------------

    #[test]
    fn plan_single_cte_to_query_datasource() {
        let sql = "WITH c AS (SELECT city, COUNT(*) AS cnt FROM sales GROUP BY city) \
                   SELECT city, cnt FROM c";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan CTE");
        // The outer query's data source must be the inlined sub-query.
        let ds = native_data_source(&planned.native_query);
        assert!(
            matches!(ds, DataSource::Query { .. }),
            "CTE outer query must use a `query` data source, got: {ds:?}"
        );
    }

    #[test]
    fn plan_chained_cte_nests_query_datasources() {
        let sql = "WITH a AS (SELECT city FROM sales), \
                        b AS (SELECT city FROM a) \
                   SELECT city FROM b";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan chained CTE");
        let ds = native_data_source(&planned.native_query);
        // Outer is a query datasource; its embedded JSON also carries a
        // nested query datasource for the inner `a`.
        let DataSource::Query { query } = ds else {
            panic!("expected query data source, got {ds:?}");
        };
        let nested_type = query
            .get("dataSource")
            .and_then(|d| d.get("type"))
            .and_then(|t| t.as_str());
        assert_eq!(
            nested_type,
            Some("query"),
            "chained CTE must nest a `query` data source: {query:?}"
        );
    }

    /// Extract the data source of a planned native query (for the query
    /// types the SQL planner can emit).
    fn native_data_source(q: &DruidQuery) -> DataSource {
        match q {
            DruidQuery::Scan(s) => s.data_source.clone(),
            DruidQuery::Timeseries(t) => t.data_source.clone(),
            DruidQuery::TopN(t) => t.data_source.clone(),
            DruidQuery::GroupBy(g) => g.data_source.clone(),
            other => panic!("unexpected query type: {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // GROUPING SETS / CUBE / ROLLUP -> subtotalsSpec on the native groupBy.
    // -----------------------------------------------------------------

    fn groupby_subtotals(sql: &str) -> Vec<Vec<String>> {
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let DruidQuery::GroupBy(q) = planned.native_query else {
            panic!("expected GroupBy query");
        };
        q.subtotals_spec.expect("subtotals_spec present")
    }

    #[test]
    fn plan_explicit_grouping_sets_subtotals() {
        let sql = "SELECT city, country, COUNT(*) AS cnt FROM sales \
                   GROUP BY GROUPING SETS ((city, country), (city), ())";
        let subtotals = groupby_subtotals(sql);
        assert_eq!(
            subtotals,
            vec![
                vec!["city".to_string(), "country".to_string()],
                vec!["city".to_string()],
                vec![],
            ]
        );
    }

    #[test]
    fn plan_cube_subtotals() {
        let sql = "SELECT city, country, COUNT(*) AS cnt FROM sales GROUP BY CUBE(city, country)";
        let subtotals = groupby_subtotals(sql);
        assert_eq!(subtotals.len(), 4, "CUBE(a,b) -> 4 sets: {subtotals:?}");
        assert!(subtotals.contains(&vec![]));
        assert!(subtotals.contains(&vec!["city".to_string()]));
        assert!(subtotals.contains(&vec!["country".to_string()]));
        assert!(subtotals.contains(&vec!["city".to_string(), "country".to_string()]));
    }

    #[test]
    fn plan_rollup_subtotals() {
        let sql = "SELECT city, country, COUNT(*) AS cnt FROM sales GROUP BY ROLLUP(city, country)";
        let subtotals = groupby_subtotals(sql);
        assert_eq!(
            subtotals,
            vec![
                vec!["city".to_string(), "country".to_string()],
                vec!["city".to_string()],
                vec![],
            ]
        );
    }

    #[test]
    fn plan_join_with_qualified_cube_normalizes_left_keys() {
        // DD R11 #3: join planning normalised left-table qualifiers in
        // `group_by` but NOT in `grouping_sets`, so a qualified left key in a
        // CUBE/ROLLUP/GROUPING SETS clause (e.g. `sales.city`) was left intact
        // while `group_by` was stripped to `city`, and `plan_groupby` then
        // rejected the subtotals set as not in the GROUP BY dims.
        let mut schema = test_schema();
        schema.join_schemas = vec![DataSourceSchema {
            name: "countries".to_string(),
            dimensions: vec![
                ColumnSchema {
                    name: "code".to_string(),
                    column_type: ColumnType::String,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    column_type: ColumnType::String,
                },
            ],
            metrics: vec![],
            time_column: "__time".to_string(),
            join_schemas: Vec::new(),
        }];

        let sql = "SELECT sales.city, COUNT(*) AS cnt \
                   FROM sales JOIN countries c ON sales.country = c.code \
                   GROUP BY CUBE(sales.city)";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &schema).expect("plan must succeed");

        let DruidQuery::GroupBy(q) = planned.native_query else {
            panic!("expected GroupBy query");
        };
        let subtotals = q.subtotals_spec.expect("subtotals_spec present");
        // CUBE(city) -> [[city], []], referencing the NORMALIZED bare `city`
        // (matching the stripped group_by_dims), not `sales.city`.
        assert!(
            subtotals.contains(&vec!["city".to_string()]),
            "subtotals must reference normalized `city`, got {subtotals:?}"
        );
        assert!(
            subtotals.contains(&Vec::<String>::new()),
            "CUBE includes the empty grand-total set, got {subtotals:?}"
        );
        assert!(
            !subtotals
                .iter()
                .any(|set| set.iter().any(|c| c.contains('.'))),
            "no grouping-set column may retain a left qualifier, got {subtotals:?}"
        );
    }

    #[test]
    fn plan_grouping_sets_forces_groupby_not_topn() {
        // Single dim + ORDER BY + LIMIT would normally be a TopN; with a
        // GROUPING SETS clause it must stay a GroupBy so the subtotalsSpec
        // is carried.
        let sql = "SELECT city, COUNT(*) AS cnt FROM sales \
                   GROUP BY GROUPING SETS ((city), ()) ORDER BY cnt DESC LIMIT 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert!(
            matches!(planned.native_query, DruidQuery::GroupBy(_)),
            "GROUPING SETS must route to GroupBy, not TopN"
        );
    }

    #[test]
    fn plan_plain_group_by_has_no_subtotals() {
        let sql = "SELECT city, COUNT(*) AS cnt FROM sales GROUP BY city";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let DruidQuery::GroupBy(q) = planned.native_query else {
            panic!("expected GroupBy query");
        };
        assert!(q.subtotals_spec.is_none());
    }

    // -----------------------------------------------------------------------
    // JOIN planning
    // -----------------------------------------------------------------------

    #[test]
    fn plan_inner_join_inline_values() {
        let sql = "SELECT city, c.name FROM sales \
                   INNER JOIN (VALUES ('tokyo', 'Tokyo')) AS c(code, name) \
                   ON sales.city = c.code";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        // The base query is a scan over sales; one join is attached.
        assert!(matches!(planned.native_query, DruidQuery::Scan(_)));
        assert_eq!(planned.joins.len(), 1);
        let pj = &planned.joins[0];
        assert_eq!(pj.join.join_type, ferrodruid_query::JoinType::Inner);
        assert_eq!(pj.join.right_prefix, "c.");
        assert_eq!(pj.join.condition.left_key, "city");
        assert_eq!(pj.join.condition.right_key, "code");
        match &pj.join.right {
            ferrodruid_query::JoinRight::Inline { column_names, rows } => {
                assert_eq!(column_names, &vec!["code".to_string(), "name".to_string()]);
                assert_eq!(rows.len(), 1);
            }
            other => panic!("expected inline right, got {other:?}"),
        }
    }

    #[test]
    fn plan_left_join_keeps_left_type() {
        let sql = "SELECT city, c.name FROM sales \
                   LEFT JOIN (VALUES ('tokyo', 'Tokyo')) AS c(code, name) \
                   ON sales.city = c.code";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(planned.joins.len(), 1);
        assert_eq!(
            planned.joins[0].join.join_type,
            ferrodruid_query::JoinType::Left
        );
    }

    #[test]
    fn plan_join_against_lookup() {
        let sql = "SELECT city, l.v FROM sales \
                   JOIN LOOKUP('city_name') AS l ON sales.city = l.k";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(planned.joins.len(), 1);
        let pj = &planned.joins[0];
        assert_eq!(pj.join.right_prefix, "l.");
        assert_eq!(pj.join.condition.left_key, "city");
        assert_eq!(pj.join.condition.right_key, "k");
        match &pj.join.right {
            ferrodruid_query::JoinRight::Lookup { lookup, .. } => {
                assert_eq!(lookup, "city_name");
            }
            other => panic!("expected lookup right, got {other:?}"),
        }
    }

    #[test]
    fn plan_two_way_join_nests() {
        let sql = "SELECT city FROM sales \
                   JOIN (VALUES ('tokyo','T')) AS a(code, n) ON sales.city = a.code \
                   JOIN LOOKUP('cc') AS b ON sales.country = b.k";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        // Two joins applied in order (nested join data sources).
        assert_eq!(planned.joins.len(), 2);
        assert_eq!(planned.joins[0].join.right_prefix, "a.");
        assert_eq!(planned.joins[1].join.right_prefix, "b.");
        assert_eq!(planned.joins[1].join.condition.left_key, "country");
    }

    #[test]
    fn plan_join_with_where_and_group_by() {
        let sql = "SELECT c.name, COUNT(*) AS cnt FROM sales \
                   JOIN (VALUES ('tokyo','Tokyo')) AS c(code, name) ON sales.city = c.code \
                   WHERE sales.revenue > 100 \
                   GROUP BY c.name";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        // The outer aggregation runs over the join result -> GroupBy.
        assert!(matches!(planned.native_query, DruidQuery::GroupBy(_)));
        assert_eq!(planned.joins.len(), 1);
        // The WHERE referenced the left table (sales.revenue) -> qualifier
        // stripped to a bare `revenue` bound filter on the inner query.
        if let DruidQuery::GroupBy(ref q) = planned.native_query {
            assert!(q.filter.is_some());
            // GROUP BY c.name keeps the right prefix `c.name`.
            assert!(q.dimensions.iter().any(|d| {
                serde_json::to_string(d)
                    .map(|s| s.contains("c.name"))
                    .unwrap_or(false)
            }));
        }
    }

    #[test]
    fn plan_join_subquery_right_carries_native_query() {
        let sql = "SELECT city FROM sales \
                   JOIN (SELECT city AS code, COUNT(*) AS n FROM sales GROUP BY city) AS s \
                   ON sales.city = s.code";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(planned.joins.len(), 1);
        let pj = &planned.joins[0];
        // Sub-query right side carries a native query to materialise its rows.
        assert!(pj.right_native_query.is_some());
        assert!(matches!(
            pj.join.right,
            ferrodruid_query::JoinRight::Rows { .. }
        ));
    }

    #[test]
    fn plan_join_rejects_non_equi_condition() {
        let sql = "SELECT city FROM sales \
                   JOIN (VALUES ('tokyo','T')) AS c(code, n) ON sales.revenue > c.code";
        // Non-equi join conditions are rejected at parse time.
        let err = parse_druid_sql(sql);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().to_lowercase().contains("equ"));
    }

    #[test]
    fn plan_join_rejects_right_outer() {
        let sql = "SELECT city FROM sales \
                   RIGHT JOIN LOOKUP('cc') AS b ON sales.city = b.k";
        let err = parse_druid_sql(sql);
        assert!(err.is_err());
    }

    // -----------------------------------------------------------------------
    // DD R43 regression tests
    // -----------------------------------------------------------------------

    fn limit_spec_of(planned: &PlannedQuery) -> LimitSpec {
        match &planned.native_query {
            DruidQuery::GroupBy(q) => q.limit_spec.clone().expect("limit spec"),
            other => panic!("expected GroupBy, got {:?}", std::mem::discriminant(other)),
        }
    }

    /// DD R43 (Finding 3): a GroupBy `ORDER BY COUNT(*)` must sort by the count,
    /// not be silently dropped while LIMIT still applies. Two grouping
    /// dimensions keep this on the GroupBy (not TopN) path.
    #[test]
    fn group_by_order_by_aggregate_expression_is_resolved() {
        let sql = "SELECT city, country, COUNT(*) AS cnt FROM sales \
                   GROUP BY city, country ORDER BY COUNT(*) DESC LIMIT 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let spec = limit_spec_of(&planned);
        let cols = spec.columns.expect("ordering columns must not be dropped");
        assert_eq!(cols.len(), 1);
        // Before the fix `columns` was `None` (COUNT(*) dropped) yet limit=5.
        assert_eq!(cols[0].dimension, "cnt");
        assert_eq!(cols[0].direction.as_deref(), Some("descending"));
        assert_eq!(cols[0].dimension_order.as_deref(), Some("numeric"));
        assert_eq!(spec.limit, Some(5));
    }

    /// DD R43 (Finding 3): ordering by the aggregate's output *alias* and by a
    /// *positional ordinal* both resolve to the aggregate output column.
    #[test]
    fn group_by_order_by_alias_and_ordinal_resolve() {
        let schema = test_schema();
        for sql in [
            "SELECT city, country, COUNT(*) AS cnt FROM sales \
             GROUP BY city, country ORDER BY cnt DESC LIMIT 5",
            "SELECT city, country, COUNT(*) AS cnt FROM sales \
             GROUP BY city, country ORDER BY 3 DESC LIMIT 5",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let planned = plan_sql(&stmt, &schema).expect("plan");
            let cols = limit_spec_of(&planned).columns.expect("columns");
            assert_eq!(cols[0].dimension, "cnt", "sql = {sql}");
        }
    }

    /// DD R43 (Finding 3): ordering by a grouping dimension uses lexicographic
    /// order for a string dimension (not the numeric coercion that tied every
    /// value at 0).
    #[test]
    fn group_by_order_by_string_dimension_is_lexicographic() {
        let sql = "SELECT city, country, COUNT(*) AS cnt FROM sales \
                   GROUP BY city, country ORDER BY city ASC LIMIT 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let cols = limit_spec_of(&planned).columns.expect("columns");
        assert_eq!(cols[0].dimension, "city");
        assert_eq!(cols[0].dimension_order.as_deref(), Some("lexicographic"));
    }

    /// DD R43 (Finding 3): an ORDER BY key that resolves to no output column
    /// is rejected rather than silently dropped.
    #[test]
    fn group_by_order_by_unknown_key_fails_closed() {
        let sql = "SELECT city, country, COUNT(*) AS cnt FROM sales \
                   GROUP BY city, country ORDER BY revenue DESC LIMIT 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        assert!(
            plan_sql(&stmt, &test_schema()).is_err(),
            "ORDER BY a non-output column must fail closed"
        );
    }

    /// DD R43 (Finding 1): a bare scan projecting a non-column expression must
    /// fail closed instead of returning all physical columns.
    #[test]
    fn scan_expression_projection_fails_closed() {
        let stmt = parse_druid_sql("SELECT UPPER(city) FROM sales").expect("parse");
        let err = plan_sql(&stmt, &test_schema()).expect_err("must reject");
        assert!(
            err.to_string()
                .contains("expression projections in a bare scan"),
            "msg = {err}"
        );
        // A bare-column scan still plans.
        let ok = parse_druid_sql("SELECT city FROM sales").expect("parse");
        assert!(plan_sql(&ok, &test_schema()).is_ok());
    }

    /// DD R43 (Finding 2): a GROUP BY on a non-column expression must fail
    /// closed instead of collapsing to a global aggregation.
    #[test]
    fn group_by_expression_key_fails_closed() {
        let stmt =
            parse_druid_sql("SELECT COUNT(*) AS c FROM sales GROUP BY UPPER(city)").expect("parse");
        let err = plan_sql(&stmt, &test_schema()).expect_err("must reject");
        assert!(
            err.to_string().contains("unsupported GROUP BY expression"),
            "msg = {err}"
        );
        // A bare-column GROUP BY still plans.
        let ok =
            parse_druid_sql("SELECT city, COUNT(*) AS c FROM sales GROUP BY city").expect("parse");
        assert!(plan_sql(&ok, &test_schema()).is_ok());
    }

    /// Superset compat: `ORDER BY <select-alias>` where the alias names a
    /// grouping-dimension projection (`SELECT city AS c ... ORDER BY c`) must
    /// resolve to the dimension's output column instead of being rejected
    /// with "does not reference a SELECT output column".
    ///
    /// codex QA r5 update: the aliased dimension now plans with
    /// `outputName = "c"` (the executor keys and sorts group rows by
    /// `outputName`), so the sort spec references `c`, not the raw `city` —
    /// referencing `city` would sort on a key absent from the result rows.
    #[test]
    fn group_by_order_by_select_alias_of_dimension_resolves() {
        let sql = "SELECT city AS c, country, COUNT(*) AS cnt FROM sales \
                   GROUP BY city, country ORDER BY c ASC LIMIT 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let cols = limit_spec_of(&planned).columns.expect("columns");
        assert_eq!(cols.len(), 1);
        // The sort spec references the emitted output column (the alias),
        // with the underlying dimension's (string) ordering.
        assert_eq!(cols[0].dimension, "c");
        assert_eq!(cols[0].direction.as_deref(), Some("ascending"));
        assert_eq!(cols[0].dimension_order.as_deref(), Some("lexicographic"));
    }

    /// Superset compat: a multi-key ORDER BY mixing an aggregate alias and a
    /// dimension alias resolves both keys to their output columns.
    ///
    /// codex QA r5 update: the dimension alias key now resolves to the
    /// dimension's `outputName` (`c`), the key the executor emits and sorts
    /// by, rather than the raw column name.
    #[test]
    fn group_by_order_by_mixed_aliases_resolve() {
        let sql = "SELECT city AS c, country, SUM(revenue) AS total FROM sales \
                   GROUP BY city, country ORDER BY total DESC, c ASC";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let cols = limit_spec_of(&planned).columns.expect("columns");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].dimension, "total");
        assert_eq!(cols[0].direction.as_deref(), Some("descending"));
        assert_eq!(cols[0].dimension_order.as_deref(), Some("numeric"));
        assert_eq!(cols[1].dimension, "c");
        assert_eq!(cols[1].direction.as_deref(), Some("ascending"));
    }

    /// Null-semantics T2 follow-through: `ORDER BY COUNT(col)` (the aggregate
    /// *expression*, not the alias) matches the planned filtered count.
    #[test]
    fn group_by_order_by_count_col_expression_resolves() {
        let sql = "SELECT city, country, COUNT(revenue) AS c FROM sales \
                   GROUP BY city, country ORDER BY COUNT(revenue) DESC LIMIT 3";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let cols = limit_spec_of(&planned).columns.expect("columns");
        assert_eq!(cols[0].dimension, "c");
        assert_eq!(cols[0].direction.as_deref(), Some("descending"));
    }

    // -----------------------------------------------------------------------
    // codex QA r5 — `output_columns` must mirror the SELECT list: aliased
    // names, SELECT-projection order, across GroupBy / Timeseries / TopN.
    // -----------------------------------------------------------------------

    fn output_names_of(planned: &PlannedQuery) -> Vec<&str> {
        planned
            .output_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect()
    }

    /// The r5 trigger query: an aggregate projected BEFORE an aliased
    /// dimension must surface as `["c", "s"]` — the alias applied, SELECT
    /// order preserved. Previously the wire columns were `{city, c}`: the
    /// alias vanished and positional clients (pydruid / Superset) saw the
    /// columns swapped.
    #[test]
    fn groupby_output_columns_follow_select_order_and_alias() {
        let sql = "SELECT COUNT(*) AS c, city AS s FROM sales GROUP BY city";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(output_names_of(&planned), vec!["c", "s"]);
        assert_eq!(planned.output_columns[0].sql_type, crate::SqlType::Bigint);
        // `s` is typed from the underlying `city` column schema.
        assert_eq!(planned.output_columns[1].sql_type, crate::SqlType::Varchar);
        // The native dimension reads the RAW column but is keyed by the alias
        // (the executor emits group rows under `outputName`).
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy");
        };
        assert!(
            matches!(
                &gb.dimensions[0],
                DimensionSpec::Default { dimension, output_name, .. }
                    if dimension == "city" && output_name == "s"
            ),
            "dimension must read `city` and output `s`, got {:?}",
            gb.dimensions[0]
        );
    }

    /// codex QA r5: `GROUP BY <alias>` and `GROUP BY <ordinal>` must resolve
    /// to the raw column even when the aliased dimension FOLLOWS the
    /// aggregate in the SELECT list (positional GROUP BY previously indexed
    /// only the non-aggregate projections, so `GROUP BY 2` here failed).
    #[test]
    fn groupby_alias_and_ordinal_group_keys_resolve_with_agg_first() {
        for sql in [
            "SELECT COUNT(*) AS c, city AS s FROM sales GROUP BY s",
            "SELECT COUNT(*) AS c, city AS s FROM sales GROUP BY 2",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let planned =
                plan_sql(&stmt, &test_schema()).unwrap_or_else(|e| panic!("must plan: {sql}: {e}"));
            assert_eq!(output_names_of(&planned), vec!["c", "s"], "sql = {sql}");
            let DruidQuery::GroupBy(ref gb) = planned.native_query else {
                panic!("expected GroupBy for {sql}");
            };
            assert!(
                matches!(
                    &gb.dimensions[0],
                    DimensionSpec::Default { dimension, output_name, .. }
                        if dimension == "city" && output_name == "s"
                ),
                "sql = {sql}, got {:?}",
                gb.dimensions[0]
            );
        }
    }

    /// codex QA r5 (regression guard): a dimension projected BEFORE the
    /// aggregate keeps working, with the alias surfaced.
    #[test]
    fn groupby_dim_before_agg_keeps_select_order() {
        let sql = "SELECT city AS s, COUNT(*) AS c FROM sales GROUP BY city";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(output_names_of(&planned), vec!["s", "c"]);
    }

    /// codex QA r5: a grouping key that is NOT in the SELECT list stays a
    /// native dimension but no longer surfaces as a wire column (Druid emits
    /// exactly the SELECT list).
    #[test]
    fn groupby_unselected_group_key_not_in_output_columns() {
        let sql = "SELECT COUNT(*) AS c FROM sales GROUP BY city";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        assert_eq!(output_names_of(&planned), vec!["c"]);
        let DruidQuery::GroupBy(ref gb) = planned.native_query else {
            panic!("expected GroupBy");
        };
        assert!(
            matches!(
                &gb.dimensions[0],
                DimensionSpec::Default { dimension, output_name, .. }
                    if dimension == "city" && output_name == "city"
            ),
            "unselected key groups under its raw name, got {:?}",
            gb.dimensions[0]
        );
    }

    /// codex QA r5: the Timeseries (TIME_FLOOR) path must keep SELECT order
    /// when the time bucket is not the first SELECT item (previously the
    /// planner hardcoded time-column-first).
    #[test]
    fn timeseries_time_floor_not_first_keeps_select_order() {
        for sql in [
            "SELECT COUNT(*) AS c, TIME_FLOOR(__time, 'PT1H') AS t FROM sales GROUP BY 2",
            "SELECT COUNT(*) AS c, TIME_FLOOR(__time, 'PT1H') AS t FROM sales \
             GROUP BY TIME_FLOOR(__time, 'PT1H')",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let planned =
                plan_sql(&stmt, &test_schema()).unwrap_or_else(|e| panic!("must plan: {sql}: {e}"));
            assert!(
                matches!(planned.native_query, DruidQuery::Timeseries(_)),
                "expected Timeseries for {sql}"
            );
            assert_eq!(output_names_of(&planned), vec!["c", "t"], "sql = {sql}");
            assert_eq!(
                planned.output_columns[1].sql_type,
                crate::SqlType::Timestamp
            );
        }
    }

    /// codex QA r5: the TopN path must keep SELECT order and surface the
    /// dimension alias (native dim reads the raw column, outputs the alias).
    #[test]
    fn topn_output_columns_follow_select_order_and_alias() {
        let sql = "SELECT COUNT(*) AS c, city AS s FROM sales \
                   GROUP BY city ORDER BY c DESC LIMIT 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let DruidQuery::TopN(ref tq) = planned.native_query else {
            panic!("expected TopN, got {:?}", planned.native_query);
        };
        assert!(
            matches!(
                &tq.dimension,
                DimensionSpec::Default { dimension, output_name, .. }
                    if dimension == "city" && output_name == "s"
            ),
            "TopN dimension must read `city` and output `s`, got {:?}",
            tq.dimension
        );
        assert_eq!(output_names_of(&planned), vec!["c", "s"]);
    }

    /// codex QA r5: `ORDER BY <ordinal>` counts SELECT positions — with the
    /// aggregate first, ordinal 2 is the aliased dimension (previously the
    /// ordinal indexed the dims-first output list and picked `country`).
    #[test]
    fn group_by_order_by_ordinal_uses_select_positions() {
        let sql = "SELECT COUNT(*) AS c, city AS s, country FROM sales \
                   GROUP BY city, country ORDER BY 2 ASC LIMIT 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let cols = limit_spec_of(&planned).columns.expect("columns");
        assert_eq!(cols[0].dimension, "s");
        assert_eq!(cols[0].dimension_order.as_deref(), Some("lexicographic"));
    }

    /// codex QA r5: `ORDER BY <raw column>` of an aliased dimension resolves
    /// to the emitted (alias) key — the executor sorts group rows keyed by
    /// the dimension's `outputName` — and keeps the column-schema ordering.
    #[test]
    fn group_by_order_by_raw_name_of_aliased_dim_resolves_to_output_name() {
        let sql = "SELECT city AS c2, country, COUNT(*) AS cnt FROM sales \
                   GROUP BY city, country ORDER BY city ASC LIMIT 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let cols = limit_spec_of(&planned).columns.expect("columns");
        assert_eq!(cols[0].dimension, "c2");
        assert_eq!(cols[0].dimension_order.as_deref(), Some("lexicographic"));
    }

    /// DD R43 (Finding 4): an explicit LAG/LEAD default argument must fail
    /// closed (it would otherwise be silently ignored). The two-argument form
    /// still plans.
    #[test]
    fn window_lag_lead_explicit_default_fails_closed() {
        let schema = window_schema();
        for sql in [
            "SELECT language, page, LAG(added, 1, 0) OVER (PARTITION BY language ORDER BY added) AS p \
             FROM wikipedia_compat",
            "SELECT language, page, LEAD(added, 1, 99) OVER (PARTITION BY language ORDER BY added) AS n \
             FROM wikipedia_compat",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            assert!(
                plan_sql(&stmt, &schema).is_err(),
                "explicit LAG/LEAD default must fail closed: {sql}"
            );
        }
        // The two-argument form (no default) still plans.
        let stmt = parse_druid_sql(
            "SELECT language, page, LAG(added, 1) OVER (PARTITION BY language ORDER BY added) AS p \
             FROM wikipedia_compat",
        )
        .expect("parse");
        assert!(
            plan_sql(&stmt, &schema).is_ok(),
            "two-argument LAG must still plan"
        );
    }

    // -----------------------------------------------------------------------
    // CL-4 / W1-D — planner coverage for the new Calcite + Druid-specific
    // SQL surface.  Each unsupported aggregate fails closed with a precise
    // message; the four new window functions plan end-to-end.
    // -----------------------------------------------------------------------

    #[allow(dead_code)]
    fn assert_plan_err_contains(sql: &str, needle: &str) {
        let stmt = parse_druid_sql(sql).expect("parse");
        let err = plan_sql(&stmt, &test_schema()).expect_err("must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains(needle),
            "expected error containing `{needle}` for [{sql}], got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // CL-4 / W1-H — R1-R7 close the W1-D fail-closed contract:  each
    // family now lowers to a real native primitive.  The unit tests
    // below verify the lowering shape (aggregator / filter / indicator
    // spec); end-to-end execution is exercised by the integration tests
    // in `crates/ferrodruid-sql/tests/cl4_calcite.rs`.
    // -----------------------------------------------------------------------

    fn lower_aggregator_of(sql: &str) -> AggregatorSpec {
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        match &planned.native_query {
            DruidQuery::Timeseries(q) => q.aggregations[0].clone(),
            DruidQuery::GroupBy(q) => {
                // Return the first non-COUNT aggregator (since tests build
                // queries like `SELECT city, ARRAY_AGG(country) ... GROUP BY city`).
                q.aggregations
                    .iter()
                    .find(|a| !matches!(a, AggregatorSpec::Count { .. }))
                    .cloned()
                    .unwrap_or_else(|| q.aggregations[0].clone())
            }
            other => panic!("expected Timeseries/GroupBy, got {other:?}"),
        }
    }

    fn lower_filter_of(sql: &str) -> FilterSpec {
        let stmt = parse_druid_sql(sql).expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        match &planned.native_query {
            DruidQuery::Timeseries(q) => q.filter.clone().expect("filter"),
            DruidQuery::GroupBy(q) => q.filter.clone().expect("filter"),
            DruidQuery::Scan(q) => q.filter.clone().expect("filter"),
            other => panic!("expected query with filter, got {other:?}"),
        }
    }

    fn find_nested_filter(
        f: &FilterSpec,
        predicate: impl Fn(&FilterSpec) -> bool + Copy,
    ) -> Option<&FilterSpec> {
        if predicate(f) {
            return Some(f);
        }
        match f {
            FilterSpec::And { fields } | FilterSpec::Or { fields } => {
                for child in fields {
                    if let Some(hit) = find_nested_filter(child, predicate) {
                        return Some(hit);
                    }
                }
                None
            }
            FilterSpec::Not { field } => find_nested_filter(field, predicate),
            _ => None,
        }
    }

    // ----- R1: ARRAY_AGG -----

    #[test]
    fn cl4_plan_array_agg_lowers_to_native() {
        let spec = lower_aggregator_of("SELECT ARRAY_AGG(city) FROM sales");
        assert!(matches!(
            spec,
            AggregatorSpec::ArrayAgg { ref field_name, distinct: false, size_limit: None, .. }
                if field_name == "city"
        ));
        let distinct = lower_aggregator_of("SELECT ARRAY_AGG(DISTINCT city, 500) FROM sales");
        assert!(matches!(
            distinct,
            AggregatorSpec::ArrayAgg {
                distinct: true,
                size_limit: Some(500),
                ..
            }
        ));
        let grouped =
            lower_aggregator_of("SELECT city, ARRAY_AGG(country) AS gs FROM sales GROUP BY city");
        assert!(matches!(
            grouped,
            AggregatorSpec::ArrayAgg { ref name, ref field_name, .. }
                if name == "gs" && field_name == "country"
        ));
    }

    // ----- R2: LISTAGG -----

    #[test]
    fn cl4_plan_listagg_lowers_to_native() {
        let default_sep = lower_aggregator_of("SELECT LISTAGG(city) FROM sales");
        assert!(matches!(
            default_sep,
            AggregatorSpec::StringAgg { ref separator, .. } if separator == ","
        ));
        let pipe = lower_aggregator_of("SELECT LISTAGG(city, '|') FROM sales");
        assert!(matches!(
            pipe,
            AggregatorSpec::StringAgg { ref separator, .. } if separator == "|"
        ));
        let with_cap = lower_aggregator_of(
            "SELECT city, LISTAGG(country, ',', 4096) AS list FROM sales GROUP BY city",
        );
        assert!(matches!(
            with_cap,
            AggregatorSpec::StringAgg { size_limit: Some(4096), ref name, .. }
                if name == "list"
        ));
    }

    // ----- R3: STRING_AGG -----

    #[test]
    fn cl4_plan_string_agg_lowers_to_native() {
        let basic = lower_aggregator_of("SELECT STRING_AGG(city, ',') FROM sales");
        assert!(
            matches!(basic, AggregatorSpec::StringAgg { ref separator, .. } if separator == ",")
        );
        let cap = lower_aggregator_of("SELECT STRING_AGG(city, '|', 1024) FROM sales");
        assert!(matches!(
            cap,
            AggregatorSpec::StringAgg { size_limit: Some(1024), ref separator, .. }
                if separator == "|"
        ));
        let grouped = lower_aggregator_of(
            "SELECT city, STRING_AGG(country, ',') AS cs FROM sales GROUP BY city",
        );
        assert!(matches!(
            grouped,
            AggregatorSpec::StringAgg { ref name, .. } if name == "cs"
        ));
    }

    // ----- R4: BLOOM_FILTER aggregate + BLOOM_FILTER_TEST filter -----

    #[test]
    fn cl4_plan_bloom_filter_aggregate_lowers_to_native() {
        let agg = lower_aggregator_of("SELECT BLOOM_FILTER(city, 1000) FROM sales");
        assert!(matches!(
            agg,
            AggregatorSpec::BloomFilter { ref field_name, num_entries: 1000, .. }
                if field_name == "city"
        ));
        let grouped = lower_aggregator_of(
            "SELECT city, BLOOM_FILTER(country, 50000) AS bf FROM sales GROUP BY city",
        );
        assert!(matches!(
            grouped,
            AggregatorSpec::BloomFilter { ref name, num_entries: 50000, .. } if name == "bf"
        ));
    }

    #[test]
    fn cl4_plan_bloom_filter_test_in_where_lowers_to_native() {
        // The filter literal must be a real base64-encoded FerroDruid bloom
        // envelope so the validate() pass that runs at execute time would
        // accept it; for unit-shape coverage we just check the lowering arm
        // produces a `BloomFilter` filter spec with the right base64 string.
        let f = lower_filter_of("SELECT COUNT(*) FROM sales WHERE BLOOM_FILTER_TEST(city, 'AAA=')");
        assert!(matches!(
            f,
            FilterSpec::BloomFilter { ref dimension, ref base64_filter }
                if dimension == "city" && base64_filter == "AAA="
        ));
        // Nested under AND
        let f2 = lower_filter_of(
            "SELECT city FROM sales \
             WHERE city = 'tokyo' AND BLOOM_FILTER_TEST(city, 'XYZ=')",
        );
        let hit = find_nested_filter(&f2, |x| matches!(x, FilterSpec::BloomFilter { .. }));
        assert!(hit.is_some(), "nested bloomFilter must lower: {f2:?}");
    }

    // ----- R5: MV_FILTER_ONLY / MV_FILTER_NONE -----

    #[test]
    fn cl4_plan_mv_filter_only_in_where_lowers_to_native() {
        let f = lower_filter_of(
            "SELECT COUNT(*) FROM sales WHERE MV_FILTER_ONLY(city, ARRAY['tokyo'])",
        );
        assert!(matches!(
            f,
            FilterSpec::MvFilterOnly { ref dimension, ref values }
                if dimension == "city" && values.len() == 1
        ));
        let f2 = lower_filter_of(
            "SELECT COUNT(*) FROM sales WHERE MV_FILTER_ONLY(country, ARRAY['JP','US'])",
        );
        assert!(matches!(
            f2,
            FilterSpec::MvFilterOnly { ref values, .. } if values.len() == 2
        ));
        let f3 = lower_filter_of(
            "SELECT COUNT(*) FROM sales \
             WHERE MV_FILTER_ONLY(city, ARRAY['x']) AND revenue > 0",
        );
        let hit = find_nested_filter(&f3, |x| matches!(x, FilterSpec::MvFilterOnly { .. }));
        assert!(hit.is_some(), "nested MvFilterOnly must lower: {f3:?}");
    }

    #[test]
    fn cl4_plan_mv_filter_none_in_where_lowers_to_native() {
        let f =
            lower_filter_of("SELECT COUNT(*) FROM sales WHERE MV_FILTER_NONE(city, ARRAY['spam'])");
        assert!(matches!(
            f,
            FilterSpec::MvFilterNone { ref dimension, ref values }
                if dimension == "city" && values.len() == 1
        ));
        let f2 = lower_filter_of(
            "SELECT COUNT(*) FROM sales \
             WHERE revenue > 0 AND MV_FILTER_NONE(city, ARRAY['x'])",
        );
        let hit = find_nested_filter(&f2, |x| matches!(x, FilterSpec::MvFilterNone { .. }));
        assert!(hit.is_some(), "nested MvFilterNone must lower: {f2:?}");
    }

    // ----- R6: EARLIEST / LATEST 2-arg form (by non-`__time` column) -----

    #[test]
    fn cl4_plan_earliest_by_non_time_column_lowers_to_native() {
        let agg = lower_aggregator_of("SELECT EARLIEST(revenue, quantity) FROM sales");
        assert!(matches!(
            agg,
            AggregatorSpec::DoubleFirst { ref field_name, time_column: Some(ref tc), .. }
                if field_name == "revenue" && tc == "quantity"
        ));
        let grouped = lower_aggregator_of(
            "SELECT city, EARLIEST(revenue, quantity) AS first_r FROM sales GROUP BY city",
        );
        assert!(matches!(
            grouped,
            AggregatorSpec::DoubleFirst { ref name, time_column: Some(ref tc), .. }
                if name == "first_r" && tc == "quantity"
        ));
    }

    #[test]
    fn cl4_plan_latest_by_non_time_column_lowers_to_native() {
        let agg = lower_aggregator_of("SELECT LATEST(revenue, quantity) FROM sales");
        assert!(matches!(
            agg,
            AggregatorSpec::DoubleLast { ref field_name, time_column: Some(ref tc), .. }
                if field_name == "revenue" && tc == "quantity"
        ));
        let grouped = lower_aggregator_of(
            "SELECT city, LATEST(revenue, quantity) AS last_r FROM sales GROUP BY city",
        );
        assert!(matches!(
            grouped,
            AggregatorSpec::DoubleLast { ref name, time_column: Some(ref tc), .. }
                if name == "last_r" && tc == "quantity"
        ));
    }

    #[test]
    fn cl4_plan_earliest_latest_single_arg_still_works() {
        // Legacy single-arg EARLIEST / LATEST must continue to plan with
        // `time_column = None` (no regression from the new 2-arg arm).
        let schema = test_schema();
        for sql in [
            "SELECT EARLIEST(revenue) AS first_r FROM sales",
            "SELECT LATEST(revenue) AS last_r FROM sales",
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let planned = plan_sql(&stmt, &schema).expect("plan");
            let DruidQuery::Timeseries(q) = &planned.native_query else {
                panic!("expected Timeseries, got {:?}", planned.native_query);
            };
            match &q.aggregations[0] {
                AggregatorSpec::DoubleFirst { time_column, .. }
                | AggregatorSpec::DoubleLast { time_column, .. } => {
                    assert!(time_column.is_none(), "1-arg form must not set time_column");
                }
                other => panic!("unexpected spec: {other:?}"),
            }
        }
    }

    // ----- R7: GROUPING() indicator -----

    #[test]
    fn cl4_plan_grouping_indicator_lowers_to_native() {
        let agg = lower_aggregator_of("SELECT city, GROUPING(city) FROM sales GROUP BY city");
        assert!(matches!(
            agg,
            AggregatorSpec::Grouping { ref fields, ref group_by_dims, .. }
                if fields == &vec!["city".to_string()]
                    && group_by_dims == &vec!["city".to_string()]
        ));
        // Multi-arg GROUPING under GROUPING SETS — `finalize_grouping_specs`
        // must thread the full GROUP BY dim list into `group_by_dims`.
        let stmt = parse_druid_sql(
            "SELECT city, country, GROUPING(city, country) AS g FROM sales \
             GROUP BY GROUPING SETS ((city, country), (city), ())",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &test_schema()).expect("plan");
        let DruidQuery::GroupBy(q) = &planned.native_query else {
            panic!("expected GroupBy");
        };
        let g_spec = q
            .aggregations
            .iter()
            .find(|a| matches!(a, AggregatorSpec::Grouping { .. }))
            .expect("Grouping spec");
        let AggregatorSpec::Grouping {
            fields,
            group_by_dims,
            name,
            ..
        } = g_spec
        else {
            unreachable!()
        };
        assert_eq!(name, "g");
        assert_eq!(fields, &vec!["city".to_string(), "country".to_string()]);
        // `finalize_grouping_specs` patches in the enclosing GROUP BY dims.
        assert_eq!(
            group_by_dims,
            &vec!["city".to_string(), "country".to_string()]
        );
    }

    // ----- Window functions: NTH_VALUE / NTILE / CUME_DIST / PERCENT_RANK ---

    #[test]
    fn cl4_plan_nth_value_window() {
        let schema = window_schema();
        let stmt = parse_druid_sql(
            "SELECT page, NTH_VALUE(added, 2) OVER (PARTITION BY language ORDER BY added) AS v2 \
             FROM wikipedia_compat",
        )
        .expect("parse");
        let planned = plan_sql(&stmt, &schema).expect("plan");
        let DruidQuery::Window(q) = planned.native_query else {
            panic!("expected Window query");
        };
        assert!(matches!(
            q.windows[0].function,
            WindowFunctionKind::NthValue { n: 2, .. }
        ));
    }

    #[test]
    fn cl4_plan_ntile_window() {
        let schema = window_schema();
        let stmt =
            parse_druid_sql("SELECT NTILE(4) OVER (ORDER BY added) AS q FROM wikipedia_compat")
                .expect("parse");
        let planned = plan_sql(&stmt, &schema).expect("plan");
        let DruidQuery::Window(q) = planned.native_query else {
            panic!("expected Window query");
        };
        assert!(matches!(
            q.windows[0].function,
            WindowFunctionKind::Ntile { tiles: 4 }
        ));
    }

    #[test]
    fn cl4_plan_cume_dist_and_percent_rank_window() {
        let schema = window_schema();
        for (sql, want_kind) in [
            (
                "SELECT CUME_DIST() OVER (ORDER BY added) AS cd FROM wikipedia_compat",
                "CumeDist",
            ),
            (
                "SELECT PERCENT_RANK() OVER (ORDER BY added) AS pr FROM wikipedia_compat",
                "PercentRank",
            ),
        ] {
            let stmt = parse_druid_sql(sql).expect("parse");
            let planned = plan_sql(&stmt, &schema).expect("plan");
            let DruidQuery::Window(q) = planned.native_query else {
                panic!("expected Window query for {sql}");
            };
            let serialized = format!("{:?}", q.windows[0].function);
            assert!(
                serialized.contains(want_kind),
                "expected {want_kind} variant, got: {serialized}"
            );
        }
    }
}
