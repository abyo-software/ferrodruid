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
use ferrodruid_segment::{SegmentData, SegmentDataBuilder};
use ferrodruid_sql::parser::{BinaryOperator, DruidSqlStatement, Projection, SqlExpr, SqlLiteral};
use ferrodruid_sql::{ColumnSchema, DataSourceSchema, plan_sql};

use crate::AppState;
use crate::query_routes::column_to_type;

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
/// historicals. Columns are de-duplicated by name (first segment wins), with
/// `__time` surfaced as a `TIMESTAMP` column like Druid does.
fn enumerate_datasources(state: &AppState) -> Vec<DsColumns> {
    let mut out: Vec<DsColumns> = Vec::new();
    for hist in &state.historicals {
        for seg_id in hist.loaded_segments() {
            let Some(ds) = hist.segment_datasource(&seg_id) else {
                continue;
            };
            if out.iter().any(|d| d.name == ds) {
                continue; // one segment's signature per datasource is enough
            }
            let Some(seg) = hist.get_segment(&seg_id) else {
                continue;
            };
            let mut columns: Vec<(String, ColumnType, bool)> = Vec::new();
            // __time first, as Druid presents it.
            if seg.columns.contains_key("__time") {
                columns.push(("__time".to_string(), ColumnType::Long, true));
            }
            for dim in &seg.dimensions {
                let ct = seg
                    .columns
                    .get(dim)
                    .map_or(ColumnType::String, column_to_type);
                columns.push((dim.clone(), ct, false));
            }
            for met in &seg.metrics {
                let ct = seg
                    .columns
                    .get(met)
                    .map_or(ColumnType::Double, column_to_type);
                columns.push((met.clone(), ct, false));
            }
            out.push(DsColumns { name: ds, columns });
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
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

/// Build the synthetic segment + schema for a virtual metadata table, or `None`
/// if `table` is not one we serve.
#[must_use]
pub fn build(state: &AppState, table: &str) -> Option<(SegmentData, DataSourceSchema)> {
    let up = table.to_ascii_uppercase();
    let datasources = enumerate_datasources(state);

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
        return None;
    };

    vt.into_segment_and_schema()
}
