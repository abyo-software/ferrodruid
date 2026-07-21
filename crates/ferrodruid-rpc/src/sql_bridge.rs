// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 43.TT — Druid SQL → wire-side native query bridge.
//!
//! This module is the glue between [`ferrodruid_sql`] (parser + planner
//! producing the rich [`ferrodruid_query::DruidQuery`] AST) and the
//! Wave 41.OO + 42.RR wire subset shipped by
//! [`crate::native_query::NativeQuery`].
//!
//! The flow:
//!
//! ```text
//! SQL string
//!   ↓ ferrodruid_sql::parse_druid_sql
//! DruidSqlStatement
//!   ↓ ferrodruid_sql::plan_sql
//! DruidQuery (rich, single-binary AST)
//!   ↓ translate (this module)
//! NativeQuery (wire subset; the broker can scatter this)
//! ```
//!
//! Four SQL patterns map cleanly onto the four wire query types:
//!
//! | SQL pattern                                              | wire query |
//! |----------------------------------------------------------|------------|
//! | `SELECT * FROM ds [WHERE …] [LIMIT N]`                   | `Scan`     |
//! | `SELECT TIME_FLOOR(__time, 'PT1H'), SUM(m) FROM ds GROUP BY 1` | `Timeseries` |
//! | `SELECT dim, …, COUNT(*) … FROM ds GROUP BY … HAVING … ORDER BY … LIMIT N` | `GroupBy` |
//! | `SELECT dim, COUNT(*) FROM ds GROUP BY dim ORDER BY <metric> DESC LIMIT N` | `TopN` |
//!
//! ## Honest scope (Wave 43.TT)
//!
//! The bridge is intentionally narrow. It does **not** translate:
//!
//! - `UNION ALL` (planner emits this; wire surface lacks it).
//! - `search` / `segmentMetadata` / `dataSourceMetadata` / `timeBoundary`
//!   native query types (planner cannot emit these from SQL today
//!   anyway, but the translator returns a typed error).
//! - Filter shapes other than scalar string/number/bool equality
//!   (`Selector { value: Some(<scalar>) }`) — `IN`, `BETWEEN`, `LIKE`,
//!   `BOUND`, range, AND/OR combinators, and NULL-valued selectors all
//!   surface as `TranslateError::UnsupportedFilter` (DD R42: a
//!   null/non-scalar selector is rejected, never dropped to "no filter").
//! - Aggregator shapes other than `count` / `longSum` / `doubleSum`
//!   (DD R42: min/max/first/last are rejected, never folded to
//!   `doubleSum`, which would have turned `MIN`/`MAX` into a SUM).
//! - Composite `HAVING` (`AND` / `OR` / `NOT`) — the wire `HavingClause`
//!   carries a single numeric comparison, so DD R42 rejects boolean
//!   trees rather than keeping only the first child.
//! - Window functions, joins, subqueries, CTEs — these never reach the
//!   bridge because the planner rejects them upstream.
//!
//! Anything not covered above returns a [`TranslateError`] the caller
//! can surface as a 4xx so the client can either rewrite the query or
//! fall back to the single-binary `ferrodruid` path.

use ferrodruid_aggregator::AggregatorSpec;
use ferrodruid_common::types::DataSource;
use ferrodruid_query::DruidQuery;
use ferrodruid_query::GranularitySpec;
use ferrodruid_query::filter::FilterSpec;
use ferrodruid_query::topn::TopNMetricSpec;
use ferrodruid_sql::planner::{DataSourceSchema, plan_sql};
pub use ferrodruid_sql::types::{OutputColumn, SqlType};
use ferrodruid_sql::{DruidSqlStatement, SelectQuery, parse_druid_sql};

use crate::native_query::{
    Aggregation, DimensionOutputType, DimensionRef, EqualsFilter, GroupBySpec, HavingClause,
    NativeQuery, ScanSpec, SortDirection, SortSpec, TimeseriesSpec, TopNSpec,
};

/// Errors produced while folding a [`DruidQuery`] down to a wire
/// [`NativeQuery`].
///
/// Distinct from `RpcError` because the bridge errors map to client
/// 4xx, not transport / 5xx.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranslateError {
    /// Top-level query type the wire surface does not support.
    UnsupportedQueryType(&'static str),
    /// Filter shape the wire surface does not support.
    UnsupportedFilter(String),
    /// Aggregation shape the wire surface does not support.
    UnsupportedAggregation(String),
    /// Datasource shape the wire surface does not support.
    UnsupportedDataSource(String),
    /// `topN` metric variants other than `Numeric` / `Inverted(Numeric)`.
    UnsupportedTopNMetric(String),
    /// SQL parse / plan failure surfaced from `ferrodruid_sql`.
    ParseOrPlan(String),
}

impl std::fmt::Display for TranslateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedQueryType(s) => {
                write!(f, "unsupported native query type for SQL bridge: {s}")
            }
            Self::UnsupportedFilter(s) => write!(f, "unsupported filter for SQL bridge: {s}"),
            Self::UnsupportedAggregation(s) => {
                write!(f, "unsupported aggregation for SQL bridge: {s}")
            }
            Self::UnsupportedDataSource(s) => {
                write!(f, "unsupported datasource for SQL bridge: {s}")
            }
            Self::UnsupportedTopNMetric(s) => {
                write!(f, "unsupported topN metric for SQL bridge: {s}")
            }
            Self::ParseOrPlan(s) => write!(f, "parse/plan error: {s}"),
        }
    }
}

impl std::error::Error for TranslateError {}

/// Result alias.
pub type TranslateResult<T> = std::result::Result<T, TranslateError>;

/// Build a permissive default schema (empty dimension / metric lists,
/// `__time` as the time column) for the SQL's FROM table when the
/// caller has no explicit catalog. The resulting schema lets the
/// planner emit a `Scan` / `Timeseries` / `GroupBy` / `TopN` query
/// against the table — column-type metadata defaults to `String` for
/// dimensions and `Bigint` for aggregations, which matches Druid's
/// "lenient SQL with no metadata" behaviour.
///
/// Returns `None` when the SQL does not contain a recognisable
/// single-table FROM clause (e.g. `UNION ALL` of two SELECTs).
#[must_use]
pub fn default_schema_for_sql(sql: &str) -> Option<DataSourceSchema> {
    let stmt = parse_druid_sql(sql).ok()?;
    let table = first_select_table(&stmt)?;
    Some(DataSourceSchema {
        name: table,
        dimensions: Vec::new(),
        metrics: Vec::new(),
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    })
}

fn first_select_table(stmt: &DruidSqlStatement) -> Option<String> {
    match stmt {
        DruidSqlStatement::Select(select) => Some(select_table(select)),
        DruidSqlStatement::ExplainPlan(inner) => first_select_table(inner),
        DruidSqlStatement::UnionAll(parts) => parts.first().and_then(first_select_table),
        // A constant SELECT (`SELECT 1`) references no table.
        DruidSqlStatement::ConstantSelect(_) => None,
    }
}

fn select_table(select: &SelectQuery) -> String {
    select.from.name.clone()
}

/// The output of the SQL bridge: a wire-side native query plus the
/// output column metadata the broker uses to format the
/// Druid-aligned response.
#[derive(Debug, Clone)]
pub struct BridgedQuery {
    /// The wire-side native query the broker can scatter to historicals.
    pub native_query: NativeQuery,
    /// Output column descriptors, in projection order.
    pub output_columns: Vec<OutputColumn>,
    /// The original Druid AST query type label (`"timeseries"`,
    /// `"scan"`, …) — used for diagnostic logging on the broker.
    pub query_type: &'static str,
}

/// Parse + plan + translate a SQL string into a wire-side native query.
///
/// # Errors
///
/// Returns [`TranslateError::ParseOrPlan`] when parsing or planning
/// fails, and one of the `Unsupported*` variants when the planned
/// query lies outside the wire's supported subset.
pub fn translate_sql(sql: &str, schema: &DataSourceSchema) -> TranslateResult<BridgedQuery> {
    let stmt = parse_druid_sql(sql).map_err(|e| TranslateError::ParseOrPlan(e.to_string()))?;
    let planned =
        plan_sql(&stmt, schema).map_err(|e| TranslateError::ParseOrPlan(e.to_string()))?;
    let native_query = translate_query(&planned.native_query)?;
    let query_type = match &planned.native_query {
        DruidQuery::Timeseries(_) => "timeseries",
        DruidQuery::Scan(_) => "scan",
        DruidQuery::GroupBy(_) => "groupBy",
        DruidQuery::TopN(_) => "topN",
        DruidQuery::Search(_) => "search",
        DruidQuery::SegmentMetadata(_) => "segmentMetadata",
        DruidQuery::DataSourceMetadata(_) => "dataSourceMetadata",
        DruidQuery::TimeBoundary(_) => "timeBoundary",
        DruidQuery::UnionAll(_) => "unionAll",
        DruidQuery::Window(_) => "window",
    };
    Ok(BridgedQuery {
        native_query,
        output_columns: planned.output_columns,
        query_type,
    })
}

/// Fold a planner-produced [`DruidQuery`] into the wire
/// [`NativeQuery`] subset.
///
/// # Errors
///
/// Returns [`TranslateError`] when the query carries a shape the wire
/// surface cannot represent.
pub fn translate_query(query: &DruidQuery) -> TranslateResult<NativeQuery> {
    match query {
        DruidQuery::Timeseries(q) => translate_timeseries(q),
        DruidQuery::Scan(q) => translate_scan(q),
        DruidQuery::GroupBy(q) => translate_groupby(q),
        DruidQuery::TopN(q) => translate_topn(q),
        DruidQuery::Search(_) => Err(TranslateError::UnsupportedQueryType("search")),
        DruidQuery::SegmentMetadata(_) => {
            Err(TranslateError::UnsupportedQueryType("segmentMetadata"))
        }
        DruidQuery::DataSourceMetadata(_) => {
            Err(TranslateError::UnsupportedQueryType("dataSourceMetadata"))
        }
        DruidQuery::TimeBoundary(_) => Err(TranslateError::UnsupportedQueryType("timeBoundary")),
        DruidQuery::UnionAll(_) => Err(TranslateError::UnsupportedQueryType("unionAll")),
        DruidQuery::Window(_) => Err(TranslateError::UnsupportedQueryType("window")),
    }
}

/// Render a merged [`NativeQueryResult`] into Druid-aligned
/// `(columns, rows)` matching the `BridgedQuery` output schema.
///
/// This is the role-split broker's SQL response formatter (moved here
/// from the broker binary so the W-B legacy rendering rules live next
/// to the bridge and stay testable).
///
/// # Legacy null mode (W-B H1)
///
/// Every value cell passes through the SAME
/// [`ferrodruid_sql::legacy_string_cell`] canonicalization the
/// single-binary SQL formatter applies: under the legacy latch a NULL in
/// a `VARCHAR` output column renders as `""` (the merged ''/null value's
/// SQL-wire face, Druid 27 oracle `group_strcol.json` /
/// `select_all_rows.json`), so single-binary and role-split SQL answers
/// are cell-identical.  ANSI mode passes every cell through unchanged
/// (the helper is a no-op with the latch off).
#[must_use]
pub fn sql_response_rows(
    bridged: &BridgedQuery,
    merged: &crate::native_query::NativeQueryResult,
) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    use crate::native_query::NativeQueryResult;
    use ferrodruid_sql::legacy_string_cell;

    let columns: Vec<String> = bridged
        .output_columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    // For timeseries projections that include a TIME_FLOOR / time
    // column, the planner emits a `SqlType::Timestamp` as the first
    // OutputColumn. We use that to decide whether the per-row first
    // cell is the bucket timestamp or a regular aggregation value.
    let timeseries_first_col_is_timestamp = bridged
        .output_columns
        .first()
        .map(|c| matches!(c.sql_type, SqlType::Timestamp))
        .unwrap_or(false);
    let rows = match merged {
        NativeQueryResult::Timeseries(buckets) => buckets
            .iter()
            .map(|b| {
                let mut row: Vec<serde_json::Value> = Vec::with_capacity(columns.len());
                for (i, col) in bridged.output_columns.iter().enumerate() {
                    if i == 0 && timeseries_first_col_is_timestamp {
                        row.push(serde_json::Value::from(b.timestamp_ms));
                    } else {
                        let v = b
                            .result
                            .get(col.name.as_str())
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        row.push(legacy_string_cell(col, v));
                    }
                }
                row
            })
            .collect(),
        NativeQueryResult::Scan(rows)
        | NativeQueryResult::GroupBy(rows)
        | NativeQueryResult::TopN(rows) => rows
            .iter()
            .map(|m| {
                if bridged.output_columns.is_empty() {
                    // No output schema metadata — fall back to a single
                    // raw-object cell so the caller still sees the
                    // payload.
                    vec![serde_json::Value::Object(m.clone())]
                } else {
                    bridged
                        .output_columns
                        .iter()
                        .map(|col| {
                            let v = m
                                .get(col.name.as_str())
                                .cloned()
                                .unwrap_or(serde_json::Value::Null);
                            legacy_string_cell(col, v)
                        })
                        .collect()
                }
            })
            .collect(),
    };
    (columns, rows)
}

// ---------------------------------------------------------------------------
// Per-query translators
// ---------------------------------------------------------------------------

fn translate_timeseries(
    q: &ferrodruid_query::timeseries::TimeseriesQuery,
) -> TranslateResult<NativeQuery> {
    let data_source = data_source_name(&q.data_source)?;
    let granularity_ms = granularity_to_ms(&q.granularity);
    let aggregations = q
        .aggregations
        .iter()
        .map(translate_aggregation)
        .collect::<TranslateResult<Vec<_>>>()?;
    let filter = match q.filter.as_ref() {
        Some(f) => Some(translate_filter(f)?),
        None => None,
    };
    Ok(NativeQuery::Timeseries(TimeseriesSpec {
        data_source,
        granularity_ms,
        aggregations,
        filter,
        // W-B H2: under the LEGACY latch, thread the planner's query
        // intervals onto the wire so the broker can anchor the legacy
        // empty-result bucket to the interval start after the scatter
        // merge (the single-binary empty-bucket fill anchor); the SQL
        // planner always plans at least the default eternity interval.
        // ANSI must stay byte-identical to the pre-W-B wire (no
        // `intervals` key — the empty list is skipped by serialization),
        // and nothing reads the field with the latch off (the broker
        // fill is a no-op, the per-segment executor never row-filters
        // on it).
        intervals: if ferrodruid_common::legacy_null_mode() {
            q.intervals.clone()
        } else {
            Vec::new()
        },
    }))
}

fn translate_scan(q: &ferrodruid_query::scan::ScanQuery) -> TranslateResult<NativeQuery> {
    let data_source = data_source_name(&q.data_source)?;
    let columns = q.columns.clone();
    let limit = q.limit;
    let filter = match q.filter.as_ref() {
        Some(f) => Some(translate_filter(f)?),
        None => None,
    };
    Ok(NativeQuery::Scan(ScanSpec {
        data_source,
        columns,
        limit,
        filter,
    }))
}

fn translate_groupby(q: &ferrodruid_query::groupby::GroupByQuery) -> TranslateResult<NativeQuery> {
    let data_source = data_source_name(&q.data_source)?;
    let dimensions: Vec<DimensionRef> = q
        .dimensions
        .iter()
        .map(dimension_ref)
        .collect::<TranslateResult<Vec<_>>>()?;
    let aggregations = q
        .aggregations
        .iter()
        .map(translate_aggregation)
        .collect::<TranslateResult<Vec<_>>>()?;
    let filter = match q.filter.as_ref() {
        Some(f) => Some(translate_filter(f)?),
        None => None,
    };
    let having = q.having.as_ref().map(translate_having).transpose()?;
    let (sort, limit) = match &q.limit_spec {
        Some(spec) => {
            let sorts = spec.columns.as_ref().map(|cols| {
                cols.iter()
                    .map(|c| SortSpec {
                        dimension: c.dimension.clone(),
                        direction: match c.direction.as_deref() {
                            Some("descending") => SortDirection::Descending,
                            _ => SortDirection::Ascending,
                        },
                    })
                    .collect::<Vec<_>>()
            });
            (sorts, spec.limit)
        }
        None => (None, None),
    };
    Ok(NativeQuery::GroupBy(GroupBySpec {
        data_source,
        dimensions,
        aggregations,
        filter,
        having,
        sort,
        limit,
    }))
}

fn translate_topn(q: &ferrodruid_query::topn::TopNQuery) -> TranslateResult<NativeQuery> {
    let data_source = data_source_name(&q.data_source)?;
    let dimension = dimension_ref(&q.dimension)?;
    let aggregations = q
        .aggregations
        .iter()
        .map(translate_aggregation)
        .collect::<TranslateResult<Vec<_>>>()?;
    let metric = topn_metric_name(&q.metric)?;
    let filter = match q.filter.as_ref() {
        Some(f) => Some(translate_filter(f)?),
        None => None,
    };
    Ok(NativeQuery::TopN(TopNSpec {
        data_source,
        dimension,
        aggregations,
        metric,
        threshold: q.threshold,
        filter,
    }))
}

// ---------------------------------------------------------------------------
// Component translators
// ---------------------------------------------------------------------------

fn data_source_name(ds: &DataSource) -> TranslateResult<String> {
    match ds {
        DataSource::Table { name } => Ok(name.clone()),
        other => Err(TranslateError::UnsupportedDataSource(format!("{other:?}"))),
    }
}

/// Fold a planner `DimensionSpec` onto the wire [`DimensionRef`].
///
/// **ANSI (legacy latch off)** — byte-identical to the pre-W-B mapping:
/// the ONE wire name is the SELECTED name (the output alias when one
/// exists, else the physical dimension), serialized as the historical
/// bare string with no `outputName`/`outputType` metadata, and the
/// executor reads segment rows by that name exactly as before.  W-B's
/// absolute invariant is that flag-off behavior (wire bytes AND
/// results) never changes, so the richer mapping below must not leak
/// here — it changed both (numeric dimensions grew an object spelling;
/// aliased queries switched the executor's read column).
///
/// **Legacy latch on** — the R9 rich mapping: the INPUT (physical)
/// column, the output alias when one exists, and the CONSUMING
/// `outputType` (role-split legacy-null divergence fix + role-split
/// aliasing fix): reducing the dimension to a bare name lost the
/// declared kind, so the role-split historical legacy-defaulted a
/// header-absent LONG grouping column to the String null instead of the
/// single-binary `0`; and collapsing the alias onto the ONE wire name
/// made the executor READ the alias (a nonexistent column) instead of
/// the physical column, silently merging every real group into the
/// missing-column default group under legacy.
///
/// STRING is the planner/serde default when the query never spelled an
/// `outputType` (the two are indistinguishable — same documented
/// limitation as the single-binary coercion), and COMPLEX is
/// pass-through; both map to the name-only wire spelling, keeping the
/// wire bytes AND the header-kind/String fallback identical to the
/// pre-fix behavior.
fn dimension_ref(d: &ferrodruid_common::types::DimensionSpec) -> TranslateResult<DimensionRef> {
    use ferrodruid_common::types::{ColumnType, DimensionSpec as D};
    match d {
        D::Default {
            output_name,
            dimension,
            output_type,
        } => {
            if !ferrodruid_common::legacy_null_mode() {
                // ANSI: the pre-W-B name-only mapping — prefer the
                // output name (alias) but fall back to the physical
                // dimension name when the alias is empty.
                let name = if output_name.is_empty() {
                    dimension.clone()
                } else {
                    output_name.clone()
                };
                return Ok(DimensionRef::from(name));
            }
            // READ side: the PHYSICAL dimension — what the executor
            // pulls from each segment row.  Fall back to the alias only
            // when the planner left the physical name empty (defensive;
            // the planner always populates `dimension`).
            let name = if dimension.is_empty() {
                output_name.clone()
            } else {
                dimension.clone()
            };
            // EMIT side: carry the alias ONLY when it actually differs
            // from the physical name, so the no-alias wire bytes stay
            // byte-identical to the historical spelling (mixed-version
            // safety).  Collapsing the alias ONTO `name` here (the
            // pre-fix shape) made the executor read a nonexistent
            // column and merge every group into one under legacy.
            let output_name = if output_name.is_empty() || *output_name == name {
                None
            } else {
                Some(output_name.clone())
            };
            let output_type = match output_type {
                ColumnType::Long => Some(DimensionOutputType::Long),
                ColumnType::Double => Some(DimensionOutputType::Double),
                ColumnType::Float => Some(DimensionOutputType::Float),
                ColumnType::String | ColumnType::Complex(_) => None,
            };
            Ok(DimensionRef {
                name,
                output_name,
                output_type,
            })
        }
        other => Err(TranslateError::UnsupportedDataSource(format!(
            "non-default DimensionSpec: {other:?}"
        ))),
    }
}

/// Map a planner [`AggregatorSpec`] to the wire [`Aggregation`] subset.
///
/// The wire surface only ships three shapes: `count`, `longSum`,
/// `doubleSum`, and its per-segment executor only knows how to
/// *sum-fold* a column. Anything outside that list cannot be carried
/// faithfully, so it is rejected.
///
/// DD R42: this previously folded the min/max/first/last family
/// (`LongMin`, `DoubleMax`, `DoubleFirst`, …) into `doubleSum` over the
/// same field name. Because the wire executor only sums, `MIN(added)`
/// then *summed* `added` and returned it under the `MIN` output name —
/// a silently-wrong result. The wire `Aggregation` enum has no
/// min/max/first/last variant (and no fold logic to compute one), so we
/// now fail closed for every aggregator that is not genuinely a count
/// or a sum, rather than translate it into a sum.
fn translate_aggregation(spec: &AggregatorSpec) -> TranslateResult<Aggregation> {
    use AggregatorSpec as A;
    let result = match spec {
        A::Count { name } => Aggregation::Count { name: name.clone() },
        A::LongSum { name, field_name } => Aggregation::LongSum {
            name: name.clone(),
            field_name: field_name.clone(),
        },
        A::DoubleSum { name, field_name } | A::FloatSum { name, field_name } => {
            Aggregation::DoubleSum {
                name: name.clone(),
                field_name: field_name.clone(),
            }
        }
        // DD R42: min/max/first/last (and any other non-sum aggregator)
        // cannot be represented by the count/longSum/doubleSum wire
        // surface; folding them to `doubleSum` made `MIN`/`MAX`/`EARLIEST`
        // /`LATEST`/`ANY_VALUE` silently return a SUM. Fail closed.
        other => {
            return Err(TranslateError::UnsupportedAggregation(format!("{other:?}")));
        }
    };
    Ok(result)
}

/// Map a planner [`FilterSpec`] to the wire [`EqualsFilter`].
///
/// Only ever called for a *present* filter (the per-query translators
/// short-circuit an absent filter before reaching here), so it always
/// yields a concrete `EqualsFilter` or an `Err`.
///
/// DD R42: this previously returned `Ok(None)` (i.e. NO filter) for any
/// selector whose value was not a string/number/bool — including a
/// NULL-valued selector (`WHERE x = NULL`) and array/object values. A
/// present-but-untranslatable constraint folded to "no filter" matches
/// *all* rows (fail-open). The wire `EqualsFilter` carries only a string
/// literal and cannot express null-matching or non-scalar values, so a
/// constrained filter it cannot represent is now rejected rather than
/// dropped.
///
/// # Legacy null mode (W-B H3)
///
/// Under [`ferrodruid_common::legacy_null_mode`] a null-matching filter
/// (`WHERE x IS NULL` → [`FilterSpec::Null`]; `WHERE x = NULL` → a
/// NULL-valued selector) IS the `''`-equality selector — `''` and null
/// are ONE merged value — so both translate to the wire
/// `EqualsFilter { value: "" }` instead of failing.  The executor's
/// header-kind canonicalization
/// (`crate::native_query::EqualsFilter::matches_with_kind`) then makes
/// the answer schema-correct per segment: a STRING column matches its
/// null/`''` rows, while a NUMERIC column matches NO rows (numeric
/// cells are never null under the latch — they read as the coerced
/// `0`/`0.0` defaults, which the `""` literal never equals) — exactly
/// the single-binary `FilterSpec::Null` / null-selector behavior.  ANSI
/// translation is unchanged (fail closed).
fn translate_filter(filter: &FilterSpec) -> TranslateResult<EqualsFilter> {
    match filter {
        FilterSpec::Selector { dimension, value } => match value {
            Some(serde_json::Value::String(s)) => Ok(EqualsFilter {
                dimension: dimension.clone(),
                value: s.clone(),
            }),
            Some(serde_json::Value::Number(n)) => Ok(EqualsFilter {
                dimension: dimension.clone(),
                value: n.to_string(),
            }),
            Some(serde_json::Value::Bool(b)) => Ok(EqualsFilter {
                dimension: dimension.clone(),
                value: b.to_string(),
            }),
            // DD R42: NULL-valued / array / object selectors cannot be
            // expressed by the string-equality wire filter; reject them
            // instead of silently matching every row.
            Some(other) => Err(TranslateError::UnsupportedFilter(format!(
                "selector with non-scalar value for `{dimension}`: {other:?}"
            ))),
            None if ferrodruid_common::legacy_null_mode() => Ok(EqualsFilter {
                dimension: dimension.clone(),
                value: String::new(),
            }),
            None => Err(TranslateError::UnsupportedFilter(format!(
                "null-matching selector for `{dimension}` is not supported by the wire surface"
            ))),
        },
        FilterSpec::Null { column } if ferrodruid_common::legacy_null_mode() => Ok(EqualsFilter {
            dimension: column.clone(),
            value: String::new(),
        }),
        other => Err(TranslateError::UnsupportedFilter(format!("{other:?}"))),
    }
}

fn translate_having(h: &ferrodruid_query::groupby::HavingSpec) -> TranslateResult<HavingClause> {
    use ferrodruid_query::groupby::HavingSpec as H;
    let clause = match h {
        H::EqualTo { aggregation, value } => HavingClause::Equal {
            aggregation: aggregation.clone(),
            value: *value,
        },
        H::GreaterThan { aggregation, value } => HavingClause::GreaterThan {
            aggregation: aggregation.clone(),
            value: *value,
        },
        H::LessThan { aggregation, value } => HavingClause::LessThan {
            aggregation: aggregation.clone(),
            value: *value,
        },
        // DD R42: a composite HAVING was translated using only the FIRST
        // child, so `HAVING cnt > 10 AND s < 5` silently dropped `s < 5`.
        // The wire `HavingClause` has no AND/OR/NOT variants and so cannot
        // represent a boolean tree — fail closed instead of keeping one
        // child.
        H::And { .. } | H::Or { .. } | H::Not { .. } => {
            return Err(TranslateError::UnsupportedFilter(
                "composite HAVING (AND/OR/NOT) is not supported by the wire surface".to_string(),
            ));
        }
    };
    Ok(clause)
}

fn topn_metric_name(metric: &TopNMetricSpec) -> TranslateResult<String> {
    match metric {
        TopNMetricSpec::Numeric { metric } => Ok(metric.clone()),
        TopNMetricSpec::Inverted { metric } => topn_metric_name(metric),
        TopNMetricSpec::Dimension { .. } => Err(TranslateError::UnsupportedTopNMetric(
            "dimension".to_string(),
        )),
    }
}

/// Map a [`GranularitySpec`] to the wire's `granularity_ms`. Unknown
/// or `all` granularities fold to `0` (single-bucket).
fn granularity_to_ms(g: &GranularitySpec) -> i64 {
    use ferrodruid_common::types::Granularity;
    let s = match g {
        GranularitySpec::Simple(s) => s.to_lowercase(),
        GranularitySpec::Full(Granularity::None) => return 1,
        GranularitySpec::Full(Granularity::Second) => return 1_000,
        GranularitySpec::Full(Granularity::Minute) => return 60_000,
        GranularitySpec::Full(Granularity::FiveMinute) => return 300_000,
        GranularitySpec::Full(Granularity::TenMinute) => return 600_000,
        GranularitySpec::Full(Granularity::FifteenMinute) => return 900_000,
        GranularitySpec::Full(Granularity::ThirtyMinute) => return 1_800_000,
        GranularitySpec::Full(Granularity::Hour) => return 3_600_000,
        GranularitySpec::Full(Granularity::SixHour) => return 21_600_000,
        GranularitySpec::Full(Granularity::Day) => return 86_400_000,
        GranularitySpec::Full(Granularity::Week) => return 604_800_000,
        GranularitySpec::Full(Granularity::Month) => return 86_400_000 * 30,
        GranularitySpec::Full(Granularity::Quarter) => return 86_400_000 * 90,
        GranularitySpec::Full(Granularity::Year) => return 86_400_000 * 365,
        GranularitySpec::Full(Granularity::Duration { period_ms, .. }) => {
            return *period_ms as i64;
        }
    };
    match s.as_str() {
        "all" | "none" => 0,
        "second" => 1_000,
        "minute" => 60_000,
        "five_minute" => 300_000,
        "ten_minute" => 600_000,
        "fifteen_minute" => 900_000,
        "thirty_minute" => 1_800_000,
        "hour" => 3_600_000,
        "six_hour" => 21_600_000,
        "day" => 86_400_000,
        "week" => 604_800_000,
        "month" => 86_400_000 * 30,
        "quarter" => 86_400_000 * 90,
        "year" => 86_400_000 * 365,
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Tests — translator-only (parse+plan+translate is exercised by the
// integration tests in `tests/sql_bridge.rs`).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_common::types::ColumnType;
    use ferrodruid_sql::planner::{ColumnSchema, DataSourceSchema};

    fn schema() -> DataSourceSchema {
        DataSourceSchema {
            name: "wikipedia".to_string(),
            dimensions: vec![
                ColumnSchema {
                    name: "page".to_string(),
                    column_type: ColumnType::String,
                },
                ColumnSchema {
                    name: "country".to_string(),
                    column_type: ColumnType::String,
                },
            ],
            metrics: vec![
                ColumnSchema {
                    name: "count".to_string(),
                    column_type: ColumnType::Long,
                },
                ColumnSchema {
                    name: "added".to_string(),
                    column_type: ColumnType::Double,
                },
            ],
            time_column: "__time".to_string(),
            join_schemas: Vec::new(),
        }
    }

    #[test]
    fn translate_count_star_to_wire_timeseries_all() {
        let bridged =
            translate_sql("SELECT COUNT(*) AS cnt FROM wikipedia", &schema()).expect("translate");
        assert_eq!(bridged.query_type, "timeseries");
        match bridged.native_query {
            NativeQuery::Timeseries(spec) => {
                assert_eq!(spec.data_source, "wikipedia");
                assert_eq!(spec.granularity_ms, 0);
                assert_eq!(spec.aggregations.len(), 1);
                assert_eq!(spec.aggregations[0].name(), "cnt");
                assert!(spec.filter.is_none());
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }

    #[test]
    fn translate_time_floor_to_wire_timeseries_hour() {
        let bridged = translate_sql(
            "SELECT TIME_FLOOR(__time, 'PT1H') AS t, COUNT(*) AS cnt \
             FROM wikipedia GROUP BY 1",
            &schema(),
        )
        .expect("translate");
        match bridged.native_query {
            NativeQuery::Timeseries(spec) => {
                assert_eq!(spec.granularity_ms, 3_600_000);
                assert_eq!(spec.aggregations.len(), 1);
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }

    #[test]
    fn translate_select_star_to_wire_scan() {
        let bridged = translate_sql("SELECT * FROM wikipedia", &schema()).expect("translate");
        assert_eq!(bridged.query_type, "scan");
        match bridged.native_query {
            NativeQuery::Scan(spec) => {
                assert_eq!(spec.data_source, "wikipedia");
                assert!(spec.columns.is_none());
                assert!(spec.limit.is_none());
                assert!(spec.filter.is_none());
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn translate_scan_with_limit_and_columns() {
        let bridged = translate_sql("SELECT page, country FROM wikipedia LIMIT 50", &schema())
            .expect("translate");
        match bridged.native_query {
            NativeQuery::Scan(spec) => {
                assert_eq!(spec.limit, Some(50));
                assert_eq!(
                    spec.columns,
                    Some(vec!["page".to_string(), "country".to_string()])
                );
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn translate_scan_with_equality_filter() {
        let bridged = translate_sql("SELECT * FROM wikipedia WHERE page = 'Home'", &schema())
            .expect("translate");
        match bridged.native_query {
            NativeQuery::Scan(spec) => {
                let f = spec.filter.expect("filter present");
                assert_eq!(f.dimension, "page");
                assert_eq!(f.value, "Home");
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn translate_groupby_dim_sum_count() {
        let bridged = translate_sql(
            "SELECT page, COUNT(*) AS cnt, SUM(added) AS sum_added \
             FROM wikipedia GROUP BY page",
            &schema(),
        )
        .expect("translate");
        assert_eq!(bridged.query_type, "groupBy");
        match bridged.native_query {
            NativeQuery::GroupBy(spec) => {
                assert_eq!(spec.dimensions, vec![DimensionRef::from("page")]);
                assert_eq!(spec.aggregations.len(), 2);
                assert_eq!(spec.aggregations[0].name(), "cnt");
                assert_eq!(spec.aggregations[1].name(), "sum_added");
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn translate_groupby_two_dims() {
        let bridged = translate_sql(
            "SELECT page, country, COUNT(*) AS cnt \
             FROM wikipedia GROUP BY page, country",
            &schema(),
        )
        .expect("translate");
        match bridged.native_query {
            NativeQuery::GroupBy(spec) => {
                assert_eq!(
                    spec.dimensions,
                    vec![DimensionRef::from("page"), DimensionRef::from("country")]
                );
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn translate_topn_pattern() {
        let bridged = translate_sql(
            "SELECT page, COUNT(*) AS cnt \
             FROM wikipedia GROUP BY page ORDER BY cnt DESC LIMIT 10",
            &schema(),
        )
        .expect("translate");
        assert_eq!(bridged.query_type, "topN");
        match bridged.native_query {
            NativeQuery::TopN(spec) => {
                assert_eq!(spec.dimension, DimensionRef::from("page"));
                assert_eq!(spec.threshold, 10);
                assert_eq!(spec.metric, "cnt");
            }
            other => panic!("expected topN, got {other:?}"),
        }
    }

    /// W-B ANSI byte-identity (H1): with the legacy latch OFF (this
    /// unit-test binary never latches, so it is ANSI) the bridge must
    /// keep the PRE-W-B name-only dimension mapping — the SELECTED name
    /// (the alias when one exists) as a bare wire string, with no
    /// `outputName`/`outputType` metadata — for groupBy AND topN.  The
    /// rich input/output/type split is a LEGACY-latch carrier and is
    /// pinned under the latch in `tests/legacy_null_parity.rs`.
    #[test]
    fn ansi_aliased_dimension_keeps_pre_wb_name_only_mapping() {
        assert!(
            !ferrodruid_common::legacy_null_mode(),
            "the unit-test binary must stay in ANSI mode"
        );
        let bridged = translate_sql(
            "SELECT page AS p, COUNT(*) AS cnt FROM wikipedia GROUP BY page",
            &schema(),
        )
        .expect("translate");
        match bridged.native_query {
            NativeQuery::GroupBy(spec) => {
                assert_eq!(spec.dimensions, vec![DimensionRef::from("p")]);
            }
            other => panic!("expected groupBy, got {other:?}"),
        }

        // Numeric dimension: the declared kind must NOT leak into the
        // ANSI wire either — pre-W-B mapped it to the bare alias name.
        let bridged = translate_sql(
            "SELECT added AS a, COUNT(*) AS cnt FROM wikipedia GROUP BY added",
            &schema(),
        )
        .expect("translate");
        match bridged.native_query {
            NativeQuery::GroupBy(spec) => {
                assert_eq!(spec.dimensions, vec![DimensionRef::from("a")]);
            }
            other => panic!("expected groupBy, got {other:?}"),
        }

        let bridged = translate_sql(
            "SELECT page AS p, COUNT(*) AS cnt \
             FROM wikipedia GROUP BY page ORDER BY cnt DESC LIMIT 10",
            &schema(),
        )
        .expect("translate");
        match bridged.native_query {
            NativeQuery::TopN(spec) => {
                assert_eq!(spec.dimension, DimensionRef::from("p"));
            }
            other => panic!("expected topN, got {other:?}"),
        }
    }

    /// W-B ANSI byte-identity (H1): EVERY ANSI dimension — string or
    /// numeric, aliased or not — serializes as the historical bare
    /// string, byte-identical to the pre-W-B wire.  The object spelling
    /// (`{"dimension": …, "outputName": …, "outputType": …}`) is a
    /// legacy-latch carrier only.
    #[test]
    fn ansi_dimension_wire_bytes_are_pre_wb_bare_strings() {
        for (sql, want) in [
            (
                "SELECT page, COUNT(*) AS cnt FROM wikipedia GROUP BY page",
                r#""dimensions":["page"]"#,
            ),
            (
                "SELECT added, COUNT(*) AS cnt FROM wikipedia GROUP BY added",
                r#""dimensions":["added"]"#,
            ),
            (
                "SELECT added AS a, COUNT(*) AS cnt FROM wikipedia GROUP BY added",
                r#""dimensions":["a"]"#,
            ),
        ] {
            let bridged = translate_sql(sql, &schema()).expect("translate");
            let wire = serde_json::to_string(&bridged.native_query).expect("ser");
            assert!(
                wire.contains(want),
                "ANSI wire must keep the pre-W-B bare-string dimension bytes for {sql}: {wire}"
            );
            assert!(
                !wire.contains("outputName") && !wire.contains("outputType"),
                "no dimension metadata may leak onto the ANSI wire for {sql}: {wire}"
            );
        }
    }

    /// W-B ANSI byte-identity (H1): the ANSI aliased-groupBy RESULT is
    /// byte-restored to pre-W-B — the wire carries only the SELECTED
    /// (alias) name, so the executor reads `p` (a nonexistent column)
    /// and every row collapses into ONE group with no dimension cell.
    /// (The aliased physical-read fix is a legacy-latch behavior;
    /// changing the ANSI answer belongs to a separate, reviewed ANSI
    /// fix, not to W-B.)
    #[test]
    fn ansi_aliased_groupby_result_matches_pre_wb() {
        use crate::native_query::{NativeQueryResult, merge_group_by};

        let bridged = translate_sql(
            "SELECT page AS p, COUNT(*) AS cnt FROM wikipedia GROUP BY page",
            &schema(),
        )
        .expect("translate");
        let NativeQuery::GroupBy(spec) = bridged.native_query.clone() else {
            panic!("expected groupBy, got {:?}", bridged.native_query);
        };
        let text = r#"{"segmentId":"wiki_v0_0","dataSource":"wikipedia","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"},{"name":"count","type":"long"}]}
{"__time":1714694400000,"page":"home","count":3}
{"__time":1714694460000,"page":"about","count":1}
{"__time":1714694520000,"page":"home","count":2}
{"__time":1714694580000,"page":"home","count":5}
"#;
        let segment = ferrodruid_deep_storage::Segment::parse_jsonl(text).expect("segment parses");
        let NativeQueryResult::GroupBy(part) = NativeQuery::GroupBy(spec.clone()).execute(&segment)
        else {
            panic!("groupBy result expected");
        };
        let merged = merge_group_by(vec![part], &spec);
        assert_eq!(
            merged.len(),
            1,
            "pre-W-B ANSI read the alias name (a nonexistent column), collapsing \
             every row into one group: {merged:?}"
        );
        assert_eq!(merged[0].get("cnt"), Some(&serde_json::json!(4)));
        assert!(
            merged[0].get("p").is_none(),
            "pre-W-B rows carry no dimension cell for the missing alias column: {merged:?}"
        );
    }

    /// W-B ANSI byte-identity (latch-leak audit): the wire timeseries
    /// must NOT grow an `intervals` field under ANSI — the field is a
    /// legacy-only carrier for the broker's post-merge empty-bucket
    /// anchor, and pre-W-B wire bytes had no such key.
    #[test]
    fn ansi_timeseries_wire_omits_intervals() {
        let bridged =
            translate_sql("SELECT COUNT(*) AS cnt FROM wikipedia", &schema()).expect("translate");
        assert_eq!(bridged.query_type, "timeseries");
        let wire = serde_json::to_string(&bridged.native_query).expect("ser");
        assert!(
            !wire.contains("intervals"),
            "ANSI timeseries wire bytes must stay pre-W-B (no intervals field): {wire}"
        );
    }

    #[test]
    fn translate_groupby_with_having() {
        let bridged = translate_sql(
            "SELECT page, COUNT(*) AS cnt FROM wikipedia GROUP BY page HAVING cnt > 5",
            &schema(),
        )
        .expect("translate");
        match bridged.native_query {
            NativeQuery::GroupBy(spec) => match spec.having {
                Some(HavingClause::GreaterThan { aggregation, value }) => {
                    assert_eq!(aggregation, "cnt");
                    assert!((value - 5.0).abs() < f64::EPSILON);
                }
                other => panic!("expected GreaterThan having, got {other:?}"),
            },
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn translate_unsupported_query_type_returns_err() {
        let res = translate_sql(
            "SELECT * FROM wikipedia UNION ALL SELECT * FROM wikipedia",
            &schema(),
        );
        assert!(matches!(
            res,
            Err(TranslateError::UnsupportedQueryType("unionAll"))
        ));
    }

    #[test]
    fn translate_unsupported_filter_returns_err() {
        let res = translate_sql("SELECT * FROM wikipedia WHERE page LIKE 'A%'", &schema());
        assert!(matches!(res, Err(TranslateError::UnsupportedFilter(_))));
    }

    #[test]
    fn translate_min_max_fails_closed_not_summed() {
        // DD R42: MIN/MAX were folded into `doubleSum`, so the wire executor
        // SUMMED the column and returned it under the MIN/MAX name. They must
        // now be rejected, never silently translated into a sum.
        for sql in [
            "SELECT MIN(added) AS m FROM wikipedia",
            "SELECT MAX(added) AS m FROM wikipedia",
            "SELECT page, MIN(added) AS m FROM wikipedia GROUP BY page",
        ] {
            let res = translate_sql(sql, &schema());
            assert!(
                matches!(res, Err(TranslateError::UnsupportedAggregation(_))),
                "MIN/MAX must fail closed (never fold to sum): {sql} -> {res:?}"
            );
        }
    }

    #[test]
    fn translate_earliest_latest_any_value_fails_closed() {
        // DD R42: EARLIEST/LATEST/ANY_VALUE lowered to first/last aggregators
        // that the wire folded into `doubleSum`. Reject instead of summing.
        for sql in [
            "SELECT EARLIEST(page) AS e FROM wikipedia",
            "SELECT LATEST(page) AS l FROM wikipedia",
            "SELECT ANY_VALUE(page) AS a FROM wikipedia",
        ] {
            let res = translate_sql(sql, &schema());
            assert!(
                matches!(res, Err(TranslateError::UnsupportedAggregation(_))),
                "first/last family must fail closed: {sql} -> {res:?}"
            );
        }
    }

    #[test]
    fn translate_composite_having_fails_closed_not_first_child() {
        // DD R42: a composite HAVING was translated using only the FIRST
        // child, dropping the rest of the predicate. It must now be rejected.
        let res = translate_sql(
            "SELECT page, COUNT(*) AS cnt, SUM(added) AS s FROM wikipedia \
             GROUP BY page HAVING cnt > 10 AND s < 5",
            &schema(),
        );
        assert!(
            matches!(res, Err(TranslateError::UnsupportedFilter(_))),
            "composite HAVING must fail closed: {res:?}"
        );
    }

    #[test]
    fn translate_null_selector_fails_closed_not_fail_open() {
        // DD R42: `WHERE x = NULL` lowered to a null-valued selector that
        // translated to `Ok(None)` — i.e. NO filter — so all rows matched
        // (fail-open). A present-but-untranslatable filter must error.
        let res = translate_sql("SELECT * FROM wikipedia WHERE page = NULL", &schema());
        assert!(
            matches!(res, Err(TranslateError::UnsupportedFilter(_))),
            "null-valued selector must fail closed: {res:?}"
        );
    }

    #[test]
    fn translate_groupby_with_order_by_emits_sort() {
        let bridged = translate_sql(
            "SELECT page, country, COUNT(*) AS cnt \
             FROM wikipedia GROUP BY page, country ORDER BY cnt DESC",
            &schema(),
        )
        .expect("translate");
        match bridged.native_query {
            NativeQuery::GroupBy(spec) => {
                let sort = spec.sort.expect("sort present");
                assert_eq!(sort.len(), 1);
                assert_eq!(sort[0].dimension, "cnt");
                assert_eq!(sort[0].direction, SortDirection::Descending);
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn translate_groupby_with_limit_only() {
        let bridged = translate_sql(
            "SELECT page, country, COUNT(*) AS cnt \
             FROM wikipedia GROUP BY page, country LIMIT 7",
            &schema(),
        )
        .expect("translate");
        match bridged.native_query {
            NativeQuery::GroupBy(spec) => {
                assert_eq!(spec.limit, Some(7));
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn granularity_simple_strings_map_to_known_ms_values() {
        assert_eq!(
            granularity_to_ms(&GranularitySpec::Simple("hour".into())),
            3_600_000
        );
        assert_eq!(
            granularity_to_ms(&GranularitySpec::Simple("day".into())),
            86_400_000
        );
        assert_eq!(granularity_to_ms(&GranularitySpec::Simple("all".into())), 0);
        assert_eq!(
            granularity_to_ms(&GranularitySpec::Simple("minute".into())),
            60_000
        );
        // Unknown string folds to 0 (single bucket).
        assert_eq!(granularity_to_ms(&GranularitySpec::Simple("xyz".into())), 0);
    }

    #[test]
    fn translate_int_equality_filter() {
        let bridged = translate_sql("SELECT * FROM wikipedia WHERE count = 42", &schema())
            .expect("translate");
        match bridged.native_query {
            NativeQuery::Scan(spec) => {
                let f = spec.filter.expect("filter present");
                assert_eq!(f.dimension, "count");
                assert_eq!(f.value, "42");
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn translate_malformed_sql_returns_parse_or_plan_err() {
        let res = translate_sql("THIS IS NOT SQL", &schema());
        assert!(matches!(res, Err(TranslateError::ParseOrPlan(_))));
    }

    #[test]
    fn default_schema_extracts_table_name_from_select() {
        let s = default_schema_for_sql("SELECT * FROM wikipedia").expect("schema");
        assert_eq!(s.name, "wikipedia");
        assert_eq!(s.time_column, "__time");
        assert!(s.dimensions.is_empty());
        assert!(s.metrics.is_empty());
    }

    #[test]
    fn default_schema_handles_explain() {
        let s = default_schema_for_sql("EXPLAIN SELECT * FROM ds_a").expect("schema");
        assert_eq!(s.name, "ds_a");
    }

    #[test]
    fn default_schema_returns_none_on_unparseable_sql() {
        assert!(default_schema_for_sql("THIS IS NOT SQL").is_none());
    }

    #[test]
    fn translate_sql_round_trip_via_default_schema() {
        let s = default_schema_for_sql("SELECT * FROM wiki_default LIMIT 3").expect("schema");
        let bridged = translate_sql("SELECT * FROM wiki_default LIMIT 3", &s).expect("translate");
        match bridged.native_query {
            NativeQuery::Scan(spec) => {
                assert_eq!(spec.data_source, "wiki_default");
                assert_eq!(spec.limit, Some(3));
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn translate_sum_metric_to_long_sum() {
        let bridged = translate_sql("SELECT SUM(count) AS total FROM wikipedia", &schema())
            .expect("translate");
        match bridged.native_query {
            NativeQuery::Timeseries(spec) => {
                assert_eq!(spec.aggregations.len(), 1);
                // The SQL planner currently emits DoubleSum for every
                // SQL `SUM`, regardless of the underlying column type;
                // the wire fold-down keeps `doubleSum` for `f64`.
                match &spec.aggregations[0] {
                    Aggregation::DoubleSum { name, field_name }
                    | Aggregation::LongSum { name, field_name } => {
                        assert_eq!(name, "total");
                        assert_eq!(field_name, "count");
                    }
                    other => panic!("expected long/double sum, got {other:?}"),
                }
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }
}
