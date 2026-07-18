// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Virtual `INFORMATION_SCHEMA` tables for SQL metadata introspection.
//!
//! BI tools reach Druid's schema through the JDBC/SQL metadata tables:
//! Apache Superset's pydruid SQLAlchemy dialect issues
//! `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA` (`get_schema_names`),
//! `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES` (`get_table_names`), and
//! `SELECT COLUMN_NAME, JDBC_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM
//! INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '…'` (`get_columns`) to
//! populate dataset pickers and sync columns.
//!
//! FerroDruid does not persist these as real datasources, so this module
//! materialises them on demand: it enumerates the live datasources + their
//! column signatures from the loaded historicals and builds a throwaway
//! [`SegmentData`] the normal planner + executor can run any SELECT against
//! (projection / WHERE / even `COUNT(*)`), so no bespoke query interpreter is
//! needed.

use ferrodruid_common::types::ColumnType;
use ferrodruid_historical::{ColumnRole, DatasourceColumns};
use ferrodruid_segment::{SegmentData, SegmentDataBuilder};
use ferrodruid_sql::parser::{BinaryOperator, DruidSqlStatement, Projection, SqlExpr, SqlLiteral};
use ferrodruid_sql::{ColumnSchema, DataSourceSchema, plan_sql};

use crate::AppState;

// ---------------------------------------------------------------------------
// Post-execution ORDER BY (null-semantics T6)
//
// Druid SORTS INFORMATION_SCHEMA results (verified live against Druid
// 35/36, 2026-07-11: `... ORDER BY COLUMN_NAME` returns sorted rows),
// but these virtual tables execute as a Scan — and a scan can only be
// ordered by the time column (T5 fails closed otherwise). So the ORDER
// BY (plus LIMIT/OFFSET, which must apply AFTER the sort) is stripped
// from the statement before planning and applied here to the produced
// SQL rows.
// ---------------------------------------------------------------------------

/// ORDER BY / LIMIT / OFFSET extracted from an `INFORMATION_SCHEMA` SELECT,
/// to apply after execution.
pub struct PostSort {
    /// Sort keys in priority order: `(output column name, ascending)`.
    keys: Vec<(String, bool)>,
    /// Row limit, applied after sorting (and after the offset).
    limit: Option<usize>,
    /// Row offset, applied after sorting.
    offset: Option<usize>,
}

/// Split an `INFORMATION_SCHEMA` SELECT into an ORDER-BY-free statement to
/// plan plus the [`PostSort`] to apply to its rows. Returns the original
/// statement and `None` when there is no ORDER BY, or when a sort key is
/// not resolvable to an output column (the planner then reports the
/// unsupported shape instead of rows being silently mis-sorted).
#[must_use]
pub fn extract_post_sort(stmt: &DruidSqlStatement) -> (DruidSqlStatement, Option<PostSort>) {
    let DruidSqlStatement::Select(sel) = stmt else {
        return (stmt.clone(), None);
    };
    if sel.order_by.is_empty() {
        return (stmt.clone(), None);
    }

    // The produced rows are keyed by each projection's OUTPUT name (the
    // SELECT alias when present, else the bare column) — `result_to_sql_rows`
    // projects onto `output_columns`. Resolve every ORDER BY key into that
    // namespace. codex QA r15 (Medium): keys used to resolve to the RAW
    // column instead, so `SELECT COLUMN_NAME AS c … ORDER BY c` compared a
    // key the projected rows don't carry and the stable sort silently left
    // ingestion order.
    let mut keys: Vec<(String, bool)> = Vec::with_capacity(sel.order_by.len());
    for ob in &sel.order_by {
        let key = match &ob.expr {
            SqlExpr::Column(name) => resolve_output_name(sel, name),
            SqlExpr::Positional(pos) => match resolve_projection_ordinal(sel, *pos) {
                Some(col) => col,
                None => return (stmt.clone(), None),
            },
            SqlExpr::Literal(SqlLiteral::Integer(pos)) => {
                match usize::try_from(*pos)
                    .ok()
                    .and_then(|p| resolve_projection_ordinal(sel, p))
                {
                    Some(col) => col,
                    None => return (stmt.clone(), None),
                }
            }
            // A non-column key falls through to the planner unstripped —
            // it fails closed there rather than being silently dropped.
            _ => return (stmt.clone(), None),
        };
        keys.push((key, ob.asc));
    }

    let mut stripped = (**sel).clone();
    stripped.order_by = Vec::new();
    let post = PostSort {
        keys,
        limit: stripped.limit.take(),
        offset: stripped.offset.take(),
    };
    (DruidSqlStatement::Select(Box::new(stripped)), Some(post))
}

/// Resolve an ORDER BY column reference into the OUTPUT (alias) namespace
/// the projected rows carry: a key that IS a projection alias is used
/// as-is; a key naming the underlying column of an aliased projection maps
/// to that alias (`SELECT COLUMN_NAME AS c … ORDER BY COLUMN_NAME` sorts by
/// `c`, as Calcite allows); anything else (a bare projected column, or any
/// column under a wildcard projection) passes through unchanged.
fn resolve_output_name(sel: &ferrodruid_sql::SelectQuery, name: &str) -> String {
    let is_alias = sel
        .projections
        .iter()
        .any(|p| matches!(p, Projection::Expr { alias: Some(a), .. } if a == name));
    if is_alias {
        return name.to_string();
    }
    sel.projections
        .iter()
        .find_map(|p| match p {
            Projection::Expr {
                expr: SqlExpr::Column(col),
                alias: Some(a),
            } if col == name => Some(a.clone()),
            _ => None,
        })
        .unwrap_or_else(|| name.to_string())
}

/// Resolve a 1-based ORDER BY ordinal against the projection list to the
/// projection's OUTPUT name — the SELECT alias when present, else the bare
/// column (bare columns only; `None` for wildcard / expression projections).
fn resolve_projection_ordinal(sel: &ferrodruid_sql::SelectQuery, pos: usize) -> Option<String> {
    if pos == 0 {
        return None;
    }
    match sel.projections.get(pos - 1)? {
        Projection::Expr {
            expr: SqlExpr::Column(col),
            alias,
        } => Some(alias.clone().unwrap_or_else(|| col.clone())),
        _ => None,
    }
}

/// Sort (and offset/limit) the SQL wire rows produced for an
/// `INFORMATION_SCHEMA` query. String columns compare as strings and the
/// JDBC-int columns numerically; nulls sort first ascending (Calcite
/// default). The sort is stable, so equal keys keep their catalog order.
pub fn apply_post_sort(rows: &mut serde_json::Value, sort: &PostSort) {
    let serde_json::Value::Array(items) = rows else {
        return;
    };
    items.sort_by(|a, b| {
        for (key, asc) in &sort.keys {
            let ord = compare_json_values(a.get(key), b.get(key));
            let ord = if *asc { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    if let Some(offset) = sort.offset {
        let n = offset.min(items.len());
        items.drain(..n);
    }
    if let Some(limit) = sort.limit {
        items.truncate(limit);
    }
}

/// Total order over the JSON values these virtual tables emit:
/// null/missing < number < string < everything else (by serialization).
fn compare_json_values(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    fn rank(v: Option<&serde_json::Value>) -> u8 {
        match v {
            None | Some(serde_json::Value::Null) => 0,
            Some(serde_json::Value::Number(_)) => 1,
            Some(serde_json::Value::String(_)) => 2,
            Some(_) => 3,
        }
    }
    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Some(serde_json::Value::Number(na)), Some(serde_json::Value::Number(nb))) => na
            .as_f64()
            .partial_cmp(&nb.as_f64())
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(serde_json::Value::String(sa)), Some(serde_json::Value::String(sb))) => sa.cmp(sb),
        (Some(va), Some(vb)) => va.to_string().cmp(&vb.to_string()),
        _ => std::cmp::Ordering::Equal,
    }
}

/// The virtual metadata tables this module serves (matched case-insensitively).
const SCHEMATA: &str = "INFORMATION_SCHEMA.SCHEMATA";
const TABLES: &str = "INFORMATION_SCHEMA.TABLES";
const COLUMNS: &str = "INFORMATION_SCHEMA.COLUMNS";

/// Returns `true` if `name` is a virtual metadata table this module can serve.
#[must_use]
pub fn is_virtual_table(name: &str) -> bool {
    let up = name.to_ascii_uppercase();
    up == SCHEMATA || up == TABLES || up == COLUMNS
}

/// Map a Druid column type to its `(DATA_TYPE, JDBC_TYPE)` pair, where
/// `JDBC_TYPE` is the `java.sql.Types` integer code pydruid's `_map_jdbc_type`
/// expects.
fn sql_and_jdbc_type(ct: &ColumnType) -> (&'static str, i64) {
    match ct {
        ColumnType::Long => ("BIGINT", -5),
        ColumnType::Float => ("FLOAT", 6),
        ColumnType::Double => ("DOUBLE", 8),
        ColumnType::String => ("VARCHAR", 12),
        ColumnType::Complex(_) => ("OTHER", 1111),
    }
}

/// One datasource and its ordered `(column, type, is_time)` signature.
struct DsColumns {
    name: String,
    columns: Vec<(String, ColumnType, bool)>,
}

/// Enumerate every loaded datasource and its column signature across all
/// historicals, with `__time` surfaced as a `TIMESTAMP` column like Druid does.
///
/// **Single shared emit helper (FG-7 R17 HIGH — `__time` parity with
/// [`build_schema_for`]).** Each datasource's per-historical column views (the
/// WITHIN-historical UNION computed by [`Historical::datasource_schemas`]) are
/// grouped by datasource, merged across historicals by
/// [`DatasourceColumns::merged`] (OR of every part's `has_time` + a SINGLE
/// cross-role dedup so a name that is a dimension in one historical and a metric
/// in another is reported EXACTLY ONCE, first-wins role/type, in the
/// deterministic `historicals` Vec order), then rendered by the ONE shared
/// [`DatasourceColumns::ordered_columns`] helper — the SAME helper
/// [`build_schema_for`] uses. `__time` is therefore emitted FIRST and exactly
/// once iff the merged datasource has ANY timed segment, and OMITTED entirely
/// for a time-less-only datasource, IDENTICALLY to the `SELECT *` schema: the
/// two consumers can no longer disagree on `__time`'s presence or position
/// (the former hand-rolled per-historical `__time`-first insertion emitted
/// `__time` late — `[a, __time]` — when a time-less historical was processed
/// before a timed one, and disagreed with `build_schema_for` on a time-less-only
/// datasource).
///
/// **Cross-map atomicity + decode-free (schema-discovery TOCTOU, R13 sibling).**
/// [`Historical::datasource_schemas`] resolves every segment's datasource
/// ATTRIBUTION and reads its load-time cached schema under ONE consistent
/// two-lock view — closing the remap window the old per-segment form
/// (`segment_datasource` THEN `get_segment`) left open — and never decodes a
/// segment. A mapped-but-later-corrupted spill segment therefore no longer
/// breaks enumeration: its columns are served from cache.
///
/// # Errors
///
/// Propagates a [`Historical::datasource_schemas`] read-lock poison. No segment
/// is decoded on this path, so a loaded-but-unreadable segment can no longer
/// surface a decode error here.
///
/// [`Historical::datasource_schemas`]: ferrodruid_historical::Historical::datasource_schemas
/// [`build_schema_for`]: crate::query_routes
fn enumerate_datasources(state: &AppState) -> ferrodruid_common::error::Result<Vec<DsColumns>> {
    // Group every historical's per-datasource column view by datasource, in
    // first-seen order. Determinism: historicals in their `historicals` Vec
    // order, columns in each historical's stable `datasource_schemas` order.
    let mut grouped: Vec<(String, Vec<DatasourceColumns>)> = Vec::new();
    for hist in &state.historicals {
        for (ds, cols) in hist.datasource_schemas()? {
            match grouped.iter_mut().find(|(name, _)| *name == ds) {
                Some((_, views)) => views.push(cols),
                None => grouped.push((ds, vec![cols])),
            }
        }
    }
    // Merge each datasource's views across historicals (OR has_time + cross-role
    // union), then render through the SINGLE shared emit helper so `__time`
    // presence/position matches `build_schema_for` exactly.
    let mut out: Vec<DsColumns> = grouped
        .into_iter()
        .map(|(name, views)| {
            let columns = DatasourceColumns::merged(views.iter())
                .ordered_columns()
                .into_iter()
                .map(|(cname, ct, role)| (cname, ct, matches!(role, ColumnRole::Time)))
                .collect();
            DsColumns { name, columns }
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// A materialised virtual table: its rows as parallel typed columns plus the
/// schema the planner needs. All columns are `String` or `Long`; every row
/// carries `__time = 0` so no interval filter excludes it.
struct VirtualTable {
    name: String,
    str_cols: Vec<(String, Vec<String>)>,
    long_cols: Vec<(String, Vec<i64>)>,
}

impl VirtualTable {
    fn into_segment_and_schema(self) -> Option<(SegmentData, DataSourceSchema)> {
        let n = self
            .str_cols
            .first()
            .map(|(_, v)| v.len())
            .or_else(|| self.long_cols.first().map(|(_, v)| v.len()))
            .unwrap_or(0);
        let mut b = SegmentDataBuilder::new().add_timestamp_column(vec![0i64; n]);
        let mut dimensions = Vec::new();
        for (name, values) in &self.str_cols {
            b = b.add_string_column(name, values.clone());
            dimensions.push(ColumnSchema {
                name: name.clone(),
                column_type: ColumnType::String,
            });
        }
        for (name, values) in &self.long_cols {
            b = b.add_long_column(name, false, values.clone());
            dimensions.push(ColumnSchema {
                name: name.clone(),
                column_type: ColumnType::Long,
            });
        }
        let segment = b.build().ok()?;
        let schema = DataSourceSchema {
            name: self.name,
            dimensions,
            metrics: Vec::new(),
            time_column: "__time".to_string(),
            join_schemas: Vec::new(),
        };
        Some((segment, schema))
    }
}

/// Evaluate an aggregate-comparison existence check of the form
/// `SELECT <agg> <cmp> <numeric literal> [AS alias] FROM <infoschema table>
/// [WHERE …]` against the synthetic `segment`, returning the single boolean row
/// as SQL wire JSON — or `None` if `stmt` is not that shape (caller falls back
/// to the normal plan+execute path).
///
/// This is exactly Apache Superset's `has_table` probe (pydruid emits
/// `SELECT COUNT(*) > 0 AS exists_ FROM INFORMATION_SCHEMA.TABLES WHERE
/// TABLE_NAME = '…'`) run when creating a dataset. FerroDruid's planner does not
/// support an aggregate wrapped in a comparison as a bare projection, so we
/// strip the comparison, run the inner aggregate through the normal
/// planner+executor, then apply the comparison to its scalar result.
#[must_use]
pub fn try_existence_check(
    stmt: &DruidSqlStatement,
    segment: &SegmentData,
    schema: &DataSourceSchema,
) -> Option<serde_json::Value> {
    let DruidSqlStatement::Select(sel) = stmt else {
        return None;
    };
    // Exactly one projection: <aggregate> <comparison> <numeric literal>.
    let [Projection::Expr { expr, alias }] = sel.projections.as_slice() else {
        return None;
    };
    let SqlExpr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    if !matches!(**left, SqlExpr::Aggregate { .. }) {
        return None;
    }
    let SqlExpr::Literal(SqlLiteral::Integer(rhs)) = **right else {
        return None;
    };
    let out_name = alias.clone().unwrap_or_else(|| "EXPR$0".to_string());

    // Rewrite the projection to the bare inner aggregate and run it.
    let mut inner = (**sel).clone();
    inner.projections = vec![Projection::Expr {
        expr: (**left).clone(),
        alias: Some(out_name.clone()),
    }];
    let inner_stmt = DruidSqlStatement::Select(Box::new(inner));
    let planned = plan_sql(&inner_stmt, schema).ok()?;
    let result = ferrodruid_query::execute_query(&planned.native_query, segment).ok()?;

    // The aggregate is a granularity=all Timeseries → one row with the value.
    let ferrodruid_query::QueryResult::Timeseries(entries) = &result else {
        return None;
    };
    // Read the scalar as f64 so a non-integer aggregate (e.g. `AVG(...)`, or a
    // sum that lands on a double) is compared on its real value rather than
    // silently coerced to 0. An absent bucket (no matching rows) reads as 0,
    // which is the correct base for `COUNT(*) > 0`.
    let agg_val = entries
        .first()
        .and_then(|e| e.result.get(&out_name))
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0);
    let rhs = rhs as f64;
    let passed = match op {
        BinaryOperator::Gt => agg_val > rhs,
        BinaryOperator::GtEq => agg_val >= rhs,
        BinaryOperator::Lt => agg_val < rhs,
        BinaryOperator::LtEq => agg_val <= rhs,
        BinaryOperator::Eq => (agg_val - rhs).abs() < f64::EPSILON,
        BinaryOperator::NotEq => (agg_val - rhs).abs() >= f64::EPSILON,
        _ => return None,
    };
    let mut row = serde_json::Map::new();
    row.insert(out_name, serde_json::Value::Bool(passed));
    Some(serde_json::Value::Array(vec![serde_json::Value::Object(
        row,
    )]))
}

/// Build the synthetic segment + schema for a virtual metadata table, or
/// `Ok(None)` if `table` is not one we serve.
///
/// # Errors
///
/// Propagates a read-lock poison from [`enumerate_datasources`]. Schema
/// discovery is decode-free (columns come from each segment's load-time cache),
/// so a loaded-but-unreadable segment no longer errors this path.
pub fn build(
    state: &AppState,
    table: &str,
) -> ferrodruid_common::error::Result<Option<(SegmentData, DataSourceSchema)>> {
    let up = table.to_ascii_uppercase();
    let datasources = enumerate_datasources(state)?;

    let vt = if up == SCHEMATA {
        // Fixed schema list, matching Druid's catalog.
        let names = ["druid", "INFORMATION_SCHEMA", "sys"];
        VirtualTable {
            name: SCHEMATA.to_string(),
            str_cols: vec![
                (
                    "CATALOG_NAME".to_string(),
                    vec!["druid".to_string(); names.len()],
                ),
                (
                    "SCHEMA_NAME".to_string(),
                    names.iter().map(|s| (*s).to_string()).collect(),
                ),
                ("SCHEMA_OWNER".to_string(), vec![String::new(); names.len()]),
            ],
            long_cols: Vec::new(),
        }
    } else if up == TABLES {
        let mut catalog = Vec::new();
        let mut schema = Vec::new();
        let mut name = Vec::new();
        let mut ttype = Vec::new();
        for ds in &datasources {
            catalog.push("druid".to_string());
            schema.push("druid".to_string());
            name.push(ds.name.clone());
            ttype.push("TABLE".to_string());
        }
        VirtualTable {
            name: TABLES.to_string(),
            str_cols: vec![
                ("TABLE_CATALOG".to_string(), catalog),
                ("TABLE_SCHEMA".to_string(), schema),
                ("TABLE_NAME".to_string(), name),
                ("TABLE_TYPE".to_string(), ttype),
            ],
            long_cols: Vec::new(),
        }
    } else if up == COLUMNS {
        let mut catalog = Vec::new();
        let mut schema = Vec::new();
        let mut table_name = Vec::new();
        let mut col_name = Vec::new();
        let mut is_nullable = Vec::new();
        let mut col_default: Vec<String> = Vec::new();
        let mut data_type = Vec::new();
        let mut ordinal = Vec::new();
        let mut jdbc = Vec::new();
        for ds in &datasources {
            for (pos, (cname, ct, is_time)) in ds.columns.iter().enumerate() {
                let (dt, jt) = if *is_time {
                    ("TIMESTAMP", 93)
                } else {
                    sql_and_jdbc_type(ct)
                };
                catalog.push("druid".to_string());
                schema.push("druid".to_string());
                table_name.push(ds.name.clone());
                col_name.push(cname.clone());
                // __time is never null; all others are nullable in Druid's model.
                is_nullable.push(if *is_time { "NO" } else { "YES" }.to_string());
                col_default.push(String::new());
                data_type.push(dt.to_string());
                ordinal.push(i64::try_from(pos + 1).unwrap_or(0));
                jdbc.push(jt);
            }
        }
        VirtualTable {
            name: COLUMNS.to_string(),
            str_cols: vec![
                ("TABLE_CATALOG".to_string(), catalog),
                ("TABLE_SCHEMA".to_string(), schema),
                ("TABLE_NAME".to_string(), table_name),
                ("COLUMN_NAME".to_string(), col_name),
                ("IS_NULLABLE".to_string(), is_nullable),
                ("COLUMN_DEFAULT".to_string(), col_default),
                ("DATA_TYPE".to_string(), data_type),
            ],
            long_cols: vec![
                ("ORDINAL_POSITION".to_string(), ordinal),
                ("JDBC_TYPE".to_string(), jdbc),
            ],
        }
    } else {
        return Ok(None);
    };

    Ok(vt.into_segment_and_schema())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Arc;

    use ferrodruid_historical::Historical;
    use ferrodruid_segment::column::ColumnData;
    use ferrodruid_segment::{Interval, SegmentData};

    /// Minimal 3-row segment: `__time` LONG + one named DOUBLE **metric**, so
    /// each datasource carries a distinguishable column signature.
    fn metric_segment(metric: &str) -> SegmentData {
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![0_i64; 3]));
        columns.insert(metric.to_string(), ColumnData::Double(vec![1.0, 2.0, 3.0]));
        SegmentData {
            version: 9,
            num_rows: 3,
            interval: Interval {
                start_millis: 0,
                end_millis: 1,
            },
            dimensions: vec![],
            metrics: vec![metric.to_string()],
            columns,
            time_sorted: true,
        }
    }

    /// Minimal 3-row segment: `__time` LONG + one named LONG **dimension**, so a
    /// column can collide by NAME with a same-named metric in another segment
    /// (R15 same-name dim/metric single-row test).
    fn dim_segment(dim: &str) -> SegmentData {
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![0_i64; 3]));
        columns.insert(dim.to_string(), ColumnData::Long(vec![1, 2, 3]));
        SegmentData {
            version: 9,
            num_rows: 3,
            interval: Interval {
                start_millis: 0,
                end_millis: 1,
            },
            dimensions: vec![dim.to_string()],
            metrics: vec![],
            columns,
            time_sorted: true,
        }
    }

    /// A DEFENSIVELY-constructed 3-row segment whose `dimensions` list ILLEGALLY
    /// names `__time` (ahead of a genuine `city` dimension) with a matching
    /// `columns["__time"]`. A well-formed segment never lists `__time` among its
    /// dimensions — it is the time column — but `SegmentData`'s fields are
    /// public/caller-mutable, so this shape is constructible. It reproduces the
    /// FG-7 R16 HIGH: INFORMATION_SCHEMA would report `__time` TWICE (once as the
    /// `has_time` time column, once as a dimension).
    fn time_in_dims_segment() -> SegmentData {
        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(vec![0_i64; 3]));
        columns.insert("city".to_string(), ColumnData::Long(vec![1, 2, 3]));
        columns.insert("value".to_string(), ColumnData::Double(vec![1.0, 2.0, 3.0]));
        SegmentData {
            version: 9,
            num_rows: 3,
            interval: Interval {
                start_millis: 0,
                end_millis: 1,
            },
            // ILLEGAL shape: `__time` named as a dimension; `city` is genuine.
            dimensions: vec!["__time".to_string(), "city".to_string()],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: true,
        }
    }

    /// A TIME-LESS 3-row segment: one named LONG dimension and NO `__time`
    /// column at all (`has_time == false`). A well-formed Druid segment always
    /// carries `__time`, but `SegmentData`'s fields are public, so a time-less
    /// segment is constructible — and heap mode admits it (only spill mode
    /// requires a LONG `__time`). Used to reproduce the FG-7 R17 HIGH where the
    /// two schema consumers disagreed on `__time` for a time-less datasource.
    fn timeless_dim_segment(dim: &str) -> SegmentData {
        let mut columns = HashMap::new();
        columns.insert(dim.to_string(), ColumnData::Long(vec![1, 2, 3]));
        SegmentData {
            version: 9,
            num_rows: 3,
            interval: Interval {
                start_millis: 0,
                end_millis: 1,
            },
            dimensions: vec![dim.to_string()],
            metrics: vec![],
            columns,
            time_sorted: false,
        }
    }

    /// Build an `AppState` whose only meaningful field is `historicals`
    /// (mirrors the `query_routes` schema-discovery test rig).
    async fn state_with(historicals: Vec<Arc<Historical>>) -> AppState {
        let metadata = ferrodruid_metadata::MetadataStore::new_in_memory()
            .await
            .expect("create metadata store");
        metadata.initialize().await.expect("init schema");
        let metadata = Arc::new(metadata);
        AppState {
            coordinator: Arc::new(ferrodruid_coordinator::Coordinator::new(Arc::clone(
                &metadata,
            ))),
            overlord: Arc::new(ferrodruid_overlord::Overlord::new(Arc::clone(&metadata))),
            metadata,
            auth_store: Arc::new(parking_lot::RwLock::new(ferrodruid_auth::AuthStore::new())),
            auth_cred_dir: None,
            authorizer: Arc::new(ferrodruid_authz::Authorizer::new().with_admin_role()),
            auth_enabled: false,
            broker: Arc::new(ferrodruid_broker::Broker::new()),
            historicals,
            start_time: chrono::Utc::now(),
            lookup_manager: Arc::new(ferrodruid_lookup::LookupManager::new()),
            metrics: Arc::new(ferrodruid_telemetry::Metrics::new()),
            msq_manager: Arc::new(ferrodruid_msq::MsqManager::new()),
            rate_limit_max_concurrent: 0,
        }
    }

    fn col_names(ds: &DsColumns) -> Vec<&str> {
        ds.columns.iter().map(|(n, _, _)| n.as_str()).collect()
    }

    /// Cross-map atomicity (sibling of R13): each datasource's columns come from
    /// its OWN segments via the atomic per-historical schema union, so two
    /// datasources with distinct signatures never bleed columns into each other.
    /// `enumerate_datasources` consumes `Historical::datasource_schemas`, which
    /// resolves attribution + cached schema under one lock view (the old
    /// `segment_datasource` THEN `get_segment` form could mis-attribute a
    /// remapped segment's columns).
    #[tokio::test]
    async fn enumerate_datasources_attributes_columns_without_cross_mixing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Arc::new(Historical::new(dir.path().to_path_buf(), 50_000_000));
        hist.load_segment("a1", metric_segment("a_metric"))
            .expect("load a1");
        hist.set_segment_datasource("a1", "A").expect("map a1");
        hist.load_segment("b1", metric_segment("b_metric"))
            .expect("load b1");
        hist.set_segment_datasource("b1", "B").expect("map b1");

        let state = state_with(vec![hist]).await;
        let dss = enumerate_datasources(&state).expect("enumerate");

        assert_eq!(dss.len(), 2, "one row per datasource");
        let a = dss.iter().find(|d| d.name == "A").expect("A present");
        let b = dss.iter().find(|d| d.name == "B").expect("B present");
        // Each datasource surfaces __time + ONLY its own metric column.
        assert_eq!(col_names(a), vec!["__time", "a_metric"]);
        assert_eq!(col_names(b), vec!["__time", "b_metric"]);
        assert!(
            !col_names(a).contains(&"b_metric"),
            "A must not carry B's column"
        );
        assert!(
            !col_names(b).contains(&"a_metric"),
            "B must not carry A's column"
        );
    }

    /// FG-7 R16 HIGH: `__time` is the datasource's time column (emitted first via
    /// `has_time`) and must NEVER ALSO be enumerated as a dimension/metric, or
    /// INFORMATION_SCHEMA reports `__time` twice. A defensively-constructed
    /// segment lists `__time` as a dimension; `enumerate_datasources` must
    /// surface `__time` EXACTLY ONCE (RED before the `CachedSchema::from_segment`
    /// `__time` filter + the `enumerate_datasources` `__time` skip). The genuine
    /// `city`/`value` columns survive.
    #[tokio::test]
    async fn enumerate_datasources_excludes_time_from_dimensions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Arc::new(Historical::new(dir.path().to_path_buf(), 50_000_000));
        hist.load_segment("s", time_in_dims_segment())
            .expect("load s");
        hist.set_segment_datasource("s", "d").expect("map s");

        let state = state_with(vec![hist]).await;
        let dss = enumerate_datasources(&state).expect("enumerate");
        let d = dss.iter().find(|x| x.name == "d").expect("d present");

        let names = col_names(d);
        assert_eq!(
            names.iter().filter(|n| **n == "__time").count(),
            1,
            "__time must be enumerated EXACTLY once (the time column): {names:?}"
        );
        // __time first (time column), then the genuine dimension + metric.
        assert_eq!(names, vec!["__time", "city", "value"]);
        // __time is flagged as the time column, city/value are not.
        let time_flag = d
            .columns
            .iter()
            .find(|(n, _, _)| n == "__time")
            .map(|(_, _, is_time)| *is_time);
        assert_eq!(
            time_flag,
            Some(true),
            "__time must be flagged as time column"
        );
    }

    /// Schema evolution UNION (HIGH regression fix): a datasource whose columns
    /// live across MULTIPLE segments must surface the UNION of every segment's
    /// columns, not just one representative segment's. Here datasource `d` has
    /// segment `s1` (column `a`) and `s2` (column `b`); INFORMATION_SCHEMA must
    /// report BOTH `a` and `b`. The pre-fix dedup (one representative segment per
    /// datasource) dropped whichever segment lost the HashMap-order race — the
    /// regression the `datasource_segments_snapshot` sibling introduced.
    #[tokio::test]
    async fn enumerate_datasources_unions_columns_across_segments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Arc::new(Historical::new(dir.path().to_path_buf(), 50_000_000));
        hist.load_segment("s1", metric_segment("a"))
            .expect("load s1");
        hist.set_segment_datasource("s1", "d").expect("map s1");
        hist.load_segment("s2", metric_segment("b"))
            .expect("load s2");
        hist.set_segment_datasource("s2", "d").expect("map s2");

        let state = state_with(vec![hist]).await;
        let dss = enumerate_datasources(&state).expect("enumerate");
        assert_eq!(dss.len(), 1, "one row per datasource");
        let d = dss.iter().find(|d| d.name == "d").expect("d present");
        let names = col_names(d);
        assert!(names.contains(&"__time"), "must include __time: {names:?}");
        assert!(
            names.contains(&"a"),
            "union must include s1's column a: {names:?}"
        );
        assert!(
            names.contains(&"b"),
            "union must include s2's column b: {names:?}"
        );
    }

    /// Same-name dimension/metric collision (R15 HIGH): a column name is unique
    /// within a datasource, so INFORMATION_SCHEMA must report it EXACTLY ONCE.
    /// `d` has `s1` (column `a` as a LONG **dimension**) and `s2` (column `a` as
    /// a DOUBLE **metric**); the pre-fix separate dim/metric dedup enumerated `a`
    /// as BOTH a dimension row and a metric row (RED — a duplicate column in
    /// INFORMATION_SCHEMA with conflicting types). Post-fix the historical union
    /// resolves `a` to a single role, so it is enumerated once.
    #[tokio::test]
    async fn enumerate_datasources_same_name_dim_metric_is_single_column() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Arc::new(Historical::new(dir.path().to_path_buf(), 50_000_000));
        hist.load_segment("s1", dim_segment("a"))
            .expect("load s1 (dim a)");
        hist.set_segment_datasource("s1", "d").expect("map s1");
        hist.load_segment("s2", metric_segment("a"))
            .expect("load s2 (metric a)");
        hist.set_segment_datasource("s2", "d").expect("map s2");

        let state = state_with(vec![hist]).await;
        let dss = enumerate_datasources(&state).expect("enumerate");
        assert_eq!(dss.len(), 1, "one row per datasource");
        let d = dss.iter().find(|d| d.name == "d").expect("d present");
        let a_count = col_names(d).iter().filter(|n| **n == "a").count();
        assert_eq!(
            a_count,
            1,
            "INFORMATION_SCHEMA must report `a` exactly once (no duplicate row): {:?}",
            col_names(d)
        );
    }

    /// A datasource whose columns live in one historical and another whose
    /// columns live in a SECOND historical are each enumerated from their own
    /// atomic snapshot — no cross-historical bleed.
    #[tokio::test]
    async fn enumerate_datasources_across_historicals_stay_isolated() {
        let dir_a = tempfile::tempdir().expect("tempdir a");
        let hist_a = Arc::new(Historical::new(dir_a.path().to_path_buf(), 50_000_000));
        hist_a
            .load_segment("a1", metric_segment("a_metric"))
            .expect("load a1");
        hist_a.set_segment_datasource("a1", "A").expect("map a1");

        let dir_b = tempfile::tempdir().expect("tempdir b");
        let hist_b = Arc::new(Historical::new(dir_b.path().to_path_buf(), 50_000_000));
        hist_b
            .load_segment("b1", metric_segment("b_metric"))
            .expect("load b1");
        hist_b.set_segment_datasource("b1", "B").expect("map b1");

        let state = state_with(vec![hist_a, hist_b]).await;
        let dss = enumerate_datasources(&state).expect("enumerate");
        assert_eq!(dss.len(), 2);
        assert_eq!(
            col_names(dss.iter().find(|d| d.name == "A").expect("A")),
            vec!["__time", "a_metric"]
        );
        assert_eq!(
            col_names(dss.iter().find(|d| d.name == "B").expect("B")),
            vec!["__time", "b_metric"]
        );
    }

    /// Improvement over the retired data-returning snapshot: schema discovery is
    /// decode-free (columns come from each segment's load-time cache), so a
    /// MAPPED spill segment whose on-disk bytes are later corrupted still
    /// surfaces its columns in INFORMATION_SCHEMA — it neither vanishes nor 500s.
    #[tokio::test]
    async fn enumerate_datasources_serves_mapped_columns_from_cache_despite_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Arc::new(Historical::with_options(
            dir.path().to_path_buf(),
            10_000_000,
            false,
            true,
        ));
        hist.load_segment_with_datasource("mapped", "A", metric_segment("a_metric"))
            .expect("load+map");
        // Corrupt the spilled bytes so ANY decode attempt would error.
        std::fs::remove_dir_all(dir.path().join("spill")).expect("corrupt spill");

        let state = state_with(vec![hist]).await;
        let dss = enumerate_datasources(&state).expect("cached schema needs no decode, cannot 500");
        let a = dss.iter().find(|d| d.name == "A").expect("A present");
        assert_eq!(col_names(a), vec!["__time", "a_metric"]);
    }

    /// A lone UNMAPPED corrupt segment is skipped before any decode (it has no
    /// datasource to attribute), so INFORMATION_SCHEMA enumeration returns
    /// cleanly empty rather than 500-ing — mirroring the pre-atomic
    /// `segment_datasource == None` skip.
    #[tokio::test]
    async fn enumerate_datasources_skips_lone_unmapped_corrupt() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Arc::new(Historical::with_options(
            dir.path().to_path_buf(),
            10_000_000,
            false,
            true,
        ));
        hist.load_segment("orphan", metric_segment("a_metric"))
            .expect("load orphan"); // deliberately UNMAPPED
        std::fs::remove_dir_all(dir.path().join("spill")).expect("corrupt orphan");

        let state = state_with(vec![hist]).await;
        let dss = enumerate_datasources(&state)
            .expect("an unmapped corrupt segment must not 500 enumeration");
        assert!(dss.is_empty());
    }

    /// Cross-historical same-name dimension/metric collision (FG-7 R15 sibling,
    /// OUTER seam): the SAME datasource split across TWO historicals with a
    /// column in CONFLICTING roles must be enumerated with that column EXACTLY
    /// ONCE, while the cross-historical UNION still keeps each historical's
    /// non-colliding columns. `d` lives in `h1` (column `a` as a LONG
    /// **dimension** + metric `x`) and `h2` (column `a` as a DOUBLE **metric** +
    /// metric `y`). The pre-fix cross-historical merge (first-historical-wins
    /// per WHOLE datasource) DROPPED h2's `y` entirely (RED: cross-historical
    /// union broken), while a separate dim/metric seen would instead duplicate
    /// `a`. Post-fix a single cross-role seen unions across historicals: `a`
    /// appears once (dimension, first-wins) and BOTH `x` and `y` are present.
    /// Deterministic (order-stable across calls).
    #[tokio::test]
    async fn enumerate_datasources_cross_historical_same_name_dim_metric_is_single_column() {
        let dir1 = tempfile::tempdir().expect("tempdir h1");
        let h1 = Arc::new(Historical::new(dir1.path().to_path_buf(), 50_000_000));
        h1.load_segment("h1_a", dim_segment("a"))
            .expect("load h1 dim a");
        h1.set_segment_datasource("h1_a", "d").expect("map h1_a");
        h1.load_segment("h1_x", metric_segment("x"))
            .expect("load h1 metric x");
        h1.set_segment_datasource("h1_x", "d").expect("map h1_x");

        let dir2 = tempfile::tempdir().expect("tempdir h2");
        let h2 = Arc::new(Historical::new(dir2.path().to_path_buf(), 50_000_000));
        h2.load_segment("h2_a", metric_segment("a"))
            .expect("load h2 metric a");
        h2.set_segment_datasource("h2_a", "d").expect("map h2_a");
        h2.load_segment("h2_y", metric_segment("y"))
            .expect("load h2 metric y");
        h2.set_segment_datasource("h2_y", "d").expect("map h2_y");

        let state = state_with(vec![h1, h2]).await;
        let dss = enumerate_datasources(&state).expect("enumerate");
        assert_eq!(dss.len(), 1, "one row per datasource");
        let d = dss.iter().find(|d| d.name == "d").expect("d present");
        let names = col_names(d);

        // `a` reported EXACTLY once across historicals (no duplicate column).
        let a_count = names.iter().filter(|n| **n == "a").count();
        assert_eq!(
            a_count, 1,
            "INFORMATION_SCHEMA must report `a` exactly once across historicals: {names:?}"
        );
        // Cross-historical UNION regression: both non-colliding columns survive.
        assert!(
            names.contains(&"x") && names.contains(&"y"),
            "cross-historical union must keep h1.x and h2.y: {names:?}"
        );
        assert!(names.contains(&"__time"), "must include __time: {names:?}");

        // Deterministic: repeated evaluations yield identical column order.
        let first: Vec<String> = names.iter().map(|s| (*s).to_string()).collect();
        for _ in 0..8 {
            let again = enumerate_datasources(&state).expect("enumerate again");
            let d2 = again.iter().find(|d| d.name == "d").expect("d present");
            let names2: Vec<String> = col_names(d2).iter().map(|s| (*s).to_string()).collect();
            assert_eq!(
                names2, first,
                "cross-historical enumerate must be order-stable"
            );
        }
    }

    /// FG-7 R17 HIGH (a) — consistency: a TIME-LESS-ONLY datasource (every
    /// segment lacks `__time`, so `has_time == false`) must OMIT `__time` in
    /// BOTH schema consumers. Before the single-emit-helper consolidation
    /// `enumerate_datasources` omitted `__time` (gated on `has_time`) while
    /// `build_schema_for` set `time_column = "__time"` unconditionally, so
    /// `SELECT *` surfaced `__time` and `INFORMATION_SCHEMA` did not — the two
    /// disagreed on the SAME datasource (RED). Post-fix both route through
    /// `DatasourceColumns::ordered_columns`, so neither surfaces `__time` and
    /// they expose the SAME column set.
    #[tokio::test]
    async fn timeless_only_datasource_omits_time_in_both_consumers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hist = Arc::new(Historical::new(dir.path().to_path_buf(), 50_000_000));
        hist.load_segment("s", timeless_dim_segment("a"))
            .expect("load s");
        hist.set_segment_datasource("s", "d").expect("map s");
        let state = state_with(vec![hist]).await;

        // INFORMATION_SCHEMA: no __time.
        let dss = enumerate_datasources(&state).expect("enumerate");
        let d = dss.iter().find(|x| x.name == "d").expect("d present");
        let enum_cols = col_names(d);
        assert!(
            !enum_cols.contains(&"__time"),
            "enumerate must omit __time for a time-less-only datasource: {enum_cols:?}"
        );
        assert_eq!(enum_cols, vec!["a"]);

        // SELECT * schema: no __time (empty time_column, absent from dims/metrics).
        let schema = crate::query_routes::build_schema_for(&state, "d").expect("schema");
        assert_ne!(
            schema.time_column, "__time",
            "build must not surface __time for a time-less-only datasource"
        );
        assert!(
            schema.time_column.is_empty(),
            "time_column must be empty when the datasource has no timed segment"
        );
        let build_cols: Vec<&str> = schema
            .dimensions
            .iter()
            .chain(schema.metrics.iter())
            .map(|c| c.name.as_str())
            .collect();
        assert!(
            !build_cols.contains(&"__time"),
            "build must not carry __time as a dim/metric: {build_cols:?}"
        );
        // The two consumers expose the SAME column set (structural parity).
        assert_eq!(
            build_cols, enum_cols,
            "SELECT* schema and INFORMATION_SCHEMA must expose identical columns"
        );
    }

    /// FG-7 R17 HIGH (b) — ordering: a datasource split across a TIME-LESS
    /// historical (dimension `a`, processed FIRST) and a TIMED historical
    /// (metric `m`) must lead with `__time` in BOTH consumers. Before the fix
    /// `enumerate_datasources` inserted `__time` at the point the first TIMED
    /// historical was seen, so a time-less-first order produced `[a, __time]`
    /// (RED — `__time` not first, order processing-dependent). Post-fix the
    /// cross-historical `merged` ORs `has_time` and the shared emit helper always
    /// leads with `__time`.
    #[tokio::test]
    async fn cross_historical_timeless_first_then_timed_leads_with_time_in_both() {
        // h1: time-less segment (dimension `a`, no __time), processed FIRST.
        let dir1 = tempfile::tempdir().expect("tempdir h1");
        let h1 = Arc::new(Historical::new(dir1.path().to_path_buf(), 50_000_000));
        h1.load_segment("h1_a", timeless_dim_segment("a"))
            .expect("load h1 a");
        h1.set_segment_datasource("h1_a", "d").expect("map h1_a");

        // h2: timed segment (metric `m`, carries __time).
        let dir2 = tempfile::tempdir().expect("tempdir h2");
        let h2 = Arc::new(Historical::new(dir2.path().to_path_buf(), 50_000_000));
        h2.load_segment("h2_m", metric_segment("m"))
            .expect("load h2 m");
        h2.set_segment_datasource("h2_m", "d").expect("map h2_m");

        let state = state_with(vec![h1, h2]).await;

        // INFORMATION_SCHEMA: __time FIRST, then the unioned columns.
        let dss = enumerate_datasources(&state).expect("enumerate");
        let d = dss.iter().find(|x| x.name == "d").expect("d present");
        let enum_cols = col_names(d);
        assert_eq!(
            enum_cols.first(),
            Some(&"__time"),
            "__time must be FIRST regardless of historical order: {enum_cols:?}"
        );
        assert_eq!(enum_cols, vec!["__time", "a", "m"]);
        // __time flagged as the time column exactly once.
        assert_eq!(
            d.columns
                .iter()
                .filter(|(n, _, is_time)| n == "__time" && *is_time)
                .count(),
            1,
            "__time must be the lone time column"
        );

        // SELECT * schema: __time is the time column; a & m both present.
        let schema = crate::query_routes::build_schema_for(&state, "d").expect("schema");
        assert_eq!(
            schema.time_column, "__time",
            "any timed segment makes __time the time column"
        );
        let dim_names: Vec<&str> = schema.dimensions.iter().map(|c| c.name.as_str()).collect();
        let metric_names: Vec<&str> = schema.metrics.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(dim_names, vec!["a"], "time-less historical's dim survives");
        assert_eq!(
            metric_names,
            vec!["m"],
            "timed historical's metric survives"
        );
        assert!(
            !dim_names.contains(&"__time") && !metric_names.contains(&"__time"),
            "__time is the time column, never a dim/metric"
        );
    }
}
