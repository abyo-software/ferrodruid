// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Static classification of one query through FerroDruid's *existing*
//! parse + plan path — nothing is executed and no data is needed.
//!
//! * SQL: [`ferrodruid_sql::parse_druid_sql`] then
//!   [`ferrodruid_sql::plan_sql`] against a synthetic schema (the planner
//!   treats unknown columns as `VARCHAR`, exactly like the REST layer's
//!   schema-from-segments fallback, so planning needs no segments).
//! * Native: the same serde deserializers the `/druid/v2` wire endpoint
//!   uses ([`ferrodruid_query::DruidQuery`]).
//!
//! Buckets:
//!
//! * [`Bucket::Supported`] — parses and plans (deserializes, for native).
//!   Plan-through counts as supported for Phase 1; no replay/diff is done.
//! * [`Bucket::FailClosed`] — recognized but deliberately rejected by
//!   FerroDruid (e.g. `FULL OUTER JOIN`, `WITH RECURSIVE`, JavaScript
//!   aggregators), with the rejection reason.
//! * [`Bucket::Unsupported`] — parse/plan error that is not a recognized
//!   deliberate rejection, with the error text.

use serde_json::Value;

use ferrodruid_query::DruidQuery;
use ferrodruid_sql::parser::{
    BinaryOperator, DruidSqlStatement, JoinRightSide, Projection, SqlExpr, SqlLiteral,
};
use ferrodruid_sql::planner::DataSourceSchema;
use ferrodruid_sql::{parse_druid_sql, plan_sql};

/// Compatibility bucket for one query shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Bucket {
    /// Parses and plans through FerroDruid's existing query path.
    Supported,
    /// Recognized construct that FerroDruid deliberately rejects.
    FailClosed,
    /// Parse or plan error (not a recognized deliberate rejection).
    Unsupported,
}

/// The result of classifying one query.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Classification {
    /// Compatibility bucket.
    pub bucket: Bucket,
    /// Why the query landed outside `supported` (parse/plan/deserialize
    /// error text, or the deliberate-rejection reason). `None` for
    /// supported queries.
    pub reason: Option<String>,
}

impl Classification {
    fn supported() -> Self {
        Self {
            bucket: Bucket::Supported,
            reason: None,
        }
    }

    fn fail_closed(reason: impl Into<String>) -> Self {
        Self {
            bucket: Bucket::FailClosed,
            reason: Some(reason.into()),
        }
    }

    fn unsupported(reason: impl Into<String>) -> Self {
        Self {
            bucket: Bucket::Unsupported,
            reason: Some(reason.into()),
        }
    }

    /// Split an error message into fail-closed vs unsupported: FerroDruid's
    /// deliberate rejections say so ("… is not supported", "… not
    /// implemented", "fail closed").
    fn from_error(msg: String) -> Self {
        let lower = msg.to_ascii_lowercase();
        let deliberate = lower.contains("not supported")
            || lower.contains("unsupported")
            || lower.contains("not implemented")
            || lower.contains("fail closed")
            || lower.contains("fail-closed");
        if deliberate {
            Self::fail_closed(msg)
        } else {
            Self::unsupported(msg)
        }
    }
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

/// The INFORMATION_SCHEMA virtual tables served by FerroDruid's broker
/// metadata path (mirrors `ferrodruid-rest`'s `infoschema::is_virtual_table`).
const VIRTUAL_TABLES: &[&str] = &[
    "INFORMATION_SCHEMA.SCHEMATA",
    "INFORMATION_SCHEMA.TABLES",
    "INFORMATION_SCHEMA.COLUMNS",
];

/// Classify a SQL query through `parse_druid_sql` + `plan_sql` (never
/// executed).
pub fn classify_sql(sql: &str) -> Classification {
    let stmt = match parse_druid_sql(sql) {
        Ok(stmt) => stmt,
        Err(e) => return Classification::from_error(format!("SQL parse: {e}")),
    };

    // Constant SELECT (`SELECT 1`) is materialised directly by the SQL
    // handler (Superset's `do_ping` path) — supported without planning.
    if matches!(stmt, DruidSqlStatement::ConstantSelect(_)) {
        return Classification::supported();
    }

    let ds_name = datasource_name(&stmt).unwrap_or_else(|| "unknown".to_string());
    let upper = ds_name.to_ascii_uppercase();

    // Druid system tables (`sys.segments`, `sys.servers`, …) have no
    // FerroDruid equivalent. Checked FIRST, over EVERY UNION ALL branch, so
    // no later early return (INFORMATION_SCHEMA, etc.) can let a `sys.*`
    // branch slip through as supported.
    if let Some(sys_ds) = all_datasource_names(&stmt)
        .into_iter()
        .find(|d| d.to_ascii_uppercase().starts_with("SYS."))
    {
        return Classification::fail_closed(format!(
            "Druid system tables (sys.*) are not implemented (query targets `{sys_ds}`)"
        ));
    }

    // INFORMATION_SCHEMA introspection: served from a virtual table built
    // out of live segment metadata by the broker (Superset dataset sync).
    // The broker still PLANS these like any other SELECT, so recognized
    // virtual tables are classified by planning through the same path the
    // broker serves them with (a query the planner rejects — e.g.
    // `AVG(DISTINCT …)` — must not be reported supported just because of
    // the table it targets).
    if VIRTUAL_TABLES.contains(&upper.as_str()) {
        return classify_infoschema(&stmt, ds_name);
    }
    if upper.starts_with("INFORMATION_SCHEMA.") {
        return Classification::fail_closed(format!(
            "INFORMATION_SCHEMA virtual table `{ds_name}` is not implemented \
             (only SCHEMATA, TABLES and COLUMNS are served)"
        ));
    }
    match plan_sql(&stmt, &synthetic_schema(ds_name)) {
        Ok(_) => Classification::supported(),
        Err(e) => Classification::from_error(format!("SQL plan: {e}")),
    }
}

/// A synthetic schema for `name`: the planner resolves unknown columns as
/// VARCHAR (the same fallback the REST layer relies on before segments
/// arrive), so an empty column list is sufficient for static plan
/// classification — no data is needed.
fn synthetic_schema(name: String) -> DataSourceSchema {
    DataSourceSchema {
        name,
        dimensions: Vec::new(),
        metrics: Vec::new(),
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

/// Classify a SELECT against a recognized `INFORMATION_SCHEMA` virtual
/// table by statically mirroring the broker's actual serving path
/// (`ferrodruid-rest`'s `infoschema` handling in `query_routes`):
///
/// 1. the aggregate-comparison existence check (Superset's `has_table`
///    probe, `SELECT COUNT(*) > 0 AS exists_ FROM …`) is served by
///    planning the bare inner aggregate and applying the comparison to its
///    scalar result — supported iff the inner aggregate plans;
/// 2. otherwise ORDER BY (+ LIMIT/OFFSET) is stripped when its keys are
///    resolvable (the broker sorts the produced rows itself, because the
///    virtual table executes as a Scan which is only time-orderable) and
///    the stripped statement is planned like any SELECT.
///
/// Still fully static: nothing is executed, and the schema is synthetic.
fn classify_infoschema(stmt: &DruidSqlStatement, ds_name: String) -> Classification {
    let schema = synthetic_schema(ds_name);
    if let Some(inner) = existence_check_inner(stmt)
        && plan_sql(&inner, &schema).is_ok()
    {
        return Classification::supported();
    }
    let stripped = strip_resolvable_post_sort(stmt);
    match plan_sql(&stripped, &schema) {
        Ok(_) => Classification::supported(),
        Err(e) => Classification::from_error(format!("SQL plan: {e}")),
    }
}

/// If `stmt` has the existence-check shape the broker special-cases —
/// exactly one projection of `<aggregate> <comparison> <integer literal>`
/// — return the statement rewritten to project the bare inner aggregate
/// (what the broker actually plans and executes). `None` otherwise.
fn existence_check_inner(stmt: &DruidSqlStatement) -> Option<DruidSqlStatement> {
    let DruidSqlStatement::Select(sel) = stmt else {
        return None;
    };
    let [Projection::Expr { expr, alias }] = sel.projections.as_slice() else {
        return None;
    };
    let SqlExpr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    if !matches!(**left, SqlExpr::Aggregate { .. }) {
        return None;
    }
    if !matches!(**right, SqlExpr::Literal(SqlLiteral::Integer(_))) {
        return None;
    }
    if !matches!(
        op,
        BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
    ) {
        return None;
    }
    let mut inner = (**sel).clone();
    inner.projections = vec![Projection::Expr {
        expr: (**left).clone(),
        alias: Some(alias.clone().unwrap_or_else(|| "EXPR$0".to_string())),
    }];
    Some(DruidSqlStatement::Select(Box::new(inner)))
}

/// Mirror of the broker's ORDER BY stripping for virtual-table SELECTs:
/// when every ORDER BY key is resolvable (a column reference, or an
/// in-range projection ordinal naming a bare column), the broker strips
/// ORDER BY / LIMIT / OFFSET before planning and applies them to the
/// produced rows. An unresolvable key is left in place so the planner
/// reports the unsupported shape (fail closed), exactly like the broker.
fn strip_resolvable_post_sort(stmt: &DruidSqlStatement) -> DruidSqlStatement {
    let DruidSqlStatement::Select(sel) = stmt else {
        return stmt.clone();
    };
    if sel.order_by.is_empty() {
        return stmt.clone();
    }
    let ordinal_resolvable = |pos: usize| {
        pos >= 1
            && matches!(
                sel.projections.get(pos - 1),
                Some(Projection::Expr {
                    expr: SqlExpr::Column(_),
                    ..
                })
            )
    };
    for ob in &sel.order_by {
        let resolvable = match &ob.expr {
            SqlExpr::Column(_) => true,
            SqlExpr::Positional(pos) => ordinal_resolvable(*pos),
            SqlExpr::Literal(SqlLiteral::Integer(pos)) => {
                usize::try_from(*pos).is_ok_and(ordinal_resolvable)
            }
            _ => false,
        };
        if !resolvable {
            return stmt.clone();
        }
    }
    let mut stripped = (**sel).clone();
    stripped.order_by = Vec::new();
    stripped.limit = None;
    stripped.offset = None;
    DruidSqlStatement::Select(Box::new(stripped))
}

/// The base datasource a parsed statement targets (mirrors the REST
/// layer's `extract_datasource_name`).
fn datasource_name(stmt: &DruidSqlStatement) -> Option<String> {
    match stmt {
        DruidSqlStatement::Select(sel) => Some(sel.from.name.clone()),
        DruidSqlStatement::ExplainPlan(inner) => datasource_name(inner),
        DruidSqlStatement::UnionAll(parts) => parts.first().and_then(datasource_name),
        DruidSqlStatement::ConstantSelect(_) => None,
    }
}

/// Every datasource referenced by a statement, recursing into ALL `UNION ALL`
/// branches — so a `SELECT … FROM wiki UNION ALL SELECT … FROM sys.segments`
/// cannot slip an unimplemented table past a first-branch-only check.
fn all_datasource_names(stmt: &DruidSqlStatement) -> Vec<String> {
    match stmt {
        DruidSqlStatement::Select(sel) => {
            // When `from` is a sub-query (CTE), `from.name` is only the CTE
            // ALIAS, not a data source — recurse the sub-query instead of
            // treating the alias as a table (an alias like "sys.x" must not
            // falsely trip the sys.* check).
            let mut out = Vec::new();
            if let Some(sub) = &sel.from.subquery {
                out.extend(all_datasource_names(sub));
            } else {
                out.push(sel.from.name.clone());
            }
            for j in &sel.from.joins {
                match &j.right {
                    JoinRightSide::Table { name, .. } => out.push(name.clone()),
                    JoinRightSide::Subquery { query, .. } => {
                        out.extend(all_datasource_names(query));
                    }
                    // LOOKUP(...) / inline VALUES reference no data source.
                    JoinRightSide::Lookup { .. } | JoinRightSide::Values { .. } => {}
                }
            }
            out
        }
        DruidSqlStatement::ExplainPlan(inner) => all_datasource_names(inner),
        DruidSqlStatement::UnionAll(parts) => parts.iter().flat_map(all_datasource_names).collect(),
        DruidSqlStatement::ConstantSelect(_) => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Native
// ---------------------------------------------------------------------------

/// Native query types that exist in Apache Druid but are deliberately
/// absent from FerroDruid, with the reason reported for each.
const KNOWN_ABSENT_QUERY_TYPES: &[(&str, &str)] = &[
    (
        "select",
        "the legacy 'select' native query type is not supported (it was removed \
         from modern Apache Druid as well; use a 'scan' query)",
    ),
    (
        "windowOperator",
        "the 'windowOperator' native query (Druid's internal window-function \
         lowering) is not accepted on the native wire; window functions are \
         supported through Druid SQL instead",
    ),
    (
        "movingAverage",
        "the 'movingAverage' contrib-extension query type is not supported",
    ),
];

/// Classify a native query by running it through the same serde
/// deserializers the `/druid/v2` endpoint uses (never executed).
pub fn classify_native(query: &Value) -> Classification {
    // JavaScript-based constructs (aggregators, filters, extraction
    // functions) are a deliberate, permanent exclusion — detect them
    // up front for a precise reason instead of a generic serde error.
    if contains_javascript_construct(query) {
        return Classification::fail_closed(
            "JavaScript-based constructs (aggregators, filters, extraction \
             functions) are deliberately not supported (FerroDruid embeds no \
             JavaScript engine)",
        );
    }

    match serde_json::from_value::<DruidQuery>(query.clone()) {
        Ok(_) => Classification::supported(),
        Err(e) => {
            let query_type = query
                .get("queryType")
                .and_then(Value::as_str)
                .unwrap_or_default();
            for (absent, reason) in KNOWN_ABSENT_QUERY_TYPES {
                if query_type == *absent {
                    return Classification::fail_closed(*reason);
                }
            }
            Classification::unsupported(format!("native deserialize: {e}"))
        }
    }
}

/// `true` when any object in the JSON tree is a real Druid JavaScript
/// construct — `{"type":"javascript", …}` carrying a function body. Requiring
/// a `function`/`fnAggregate` sibling avoids a false positive on an arbitrary
/// data value that merely happens to be `{"type":"javascript"}` (a JS
/// aggregator/filter/extractionFn always ships its function source).
fn contains_javascript_construct(v: &Value) -> bool {
    match v {
        Value::Object(map) => {
            if map.get("type").and_then(Value::as_str) == Some("javascript")
                && (map.contains_key("function") || map.contains_key("fnAggregate"))
            {
                return true;
            }
            map.values().any(contains_javascript_construct)
        }
        Value::Array(items) => items.iter().any(contains_javascript_construct),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn plain_aggregate_sql_is_supported() {
        let c = classify_sql("SELECT COUNT(*) AS cnt FROM wikipedia_compat");
        assert_eq!(c.bucket, Bucket::Supported, "{:?}", c.reason);
    }

    #[test]
    fn superset_chart_sql_is_supported() {
        let c = classify_sql(
            "SELECT DATE_TRUNC('day', CAST(__time AS TIMESTAMP)) AS __timestamp, \
             \"language\" AS \"language\", COUNT(*) AS \"count\" \
             FROM \"druid\".\"wikipedia_compat\" \
             WHERE __time >= TIMESTAMP '2024-01-01 00:00:00' \
             GROUP BY 1, 2 ORDER BY \"count\" DESC LIMIT 1000",
        );
        assert_eq!(c.bucket, Bucket::Supported, "{:?}", c.reason);
    }

    #[test]
    fn constant_select_and_information_schema_supported() {
        assert_eq!(classify_sql("SELECT 1").bucket, Bucket::Supported);
        assert_eq!(
            classify_sql(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid'"
            )
            .bucket,
            Bucket::Supported
        );
    }

    #[test]
    fn information_schema_is_planned_not_blanket_supported() {
        // A query the broker's planner rejects must not be reported
        // supported just because it targets an INFORMATION_SCHEMA virtual
        // table — the real broker path plans these like any SELECT.
        let c = classify_sql("SELECT AVG(DISTINCT TABLE_NAME) FROM INFORMATION_SCHEMA.TABLES");
        assert_ne!(c.bucket, Bucket::Supported, "{:?}", c.reason);
        // Plain introspection still plans through.
        let ok = classify_sql(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid'",
        );
        assert_eq!(ok.bucket, Bucket::Supported, "{:?}", ok.reason);
        // Superset's `has_table` probe is served by the broker's
        // aggregate-comparison existence check (verified E2E in the real
        // UI) — the classifier must mirror that path, not the raw planner
        // (which cannot project an aggregate wrapped in a comparison).
        let has_table = classify_sql(
            "SELECT COUNT(*) > 0 AS exists_ FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_NAME = 'wikipedia_compat'",
        );
        assert_eq!(
            has_table.bucket,
            Bucket::Supported,
            "{:?}",
            has_table.reason
        );
        // ...but the same shape against a REAL datasource is NOT served
        // specially and must keep failing closed.
        let on_real_table =
            classify_sql("SELECT COUNT(*) > 0 AS exists_ FROM wiki WHERE page = 'x'");
        assert_ne!(
            on_real_table.bucket,
            Bucket::Supported,
            "{:?}",
            on_real_table.reason
        );
        // ORDER BY on a virtual table is stripped and applied post-
        // execution by the broker; the classifier mirrors that too.
        let ordered = classify_sql(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME LIMIT 10",
        );
        assert_eq!(ordered.bucket, Bucket::Supported, "{:?}", ordered.reason);
    }

    #[test]
    fn sys_tables_fail_closed() {
        let c = classify_sql("SELECT segment_id FROM sys.segments");
        assert_eq!(c.bucket, Bucket::FailClosed);
        let reason = c.reason.unwrap_or_default();
        assert!(reason.contains("sys.*"), "{reason}");
    }

    // -- the three work-order "intentionally incompatible" probes ---------

    #[test]
    fn full_outer_join_fail_closed_with_reason() {
        let c = classify_sql(
            "SELECT a.\"page\", b.\"language\" FROM wiki a \
             FULL OUTER JOIN wiki b ON a.\"page\" = b.\"page\" LIMIT 10",
        );
        assert_eq!(c.bucket, Bucket::FailClosed, "{:?}", c.reason);
        let reason = c.reason.unwrap_or_default();
        assert!(
            reason.contains("FULL OUTER JOIN is not supported"),
            "{reason}"
        );
    }

    #[test]
    fn recursive_cte_fail_closed_with_reason() {
        let c = classify_sql("WITH RECURSIVE r AS (SELECT 1 AS n) SELECT * FROM r");
        assert_eq!(c.bucket, Bucket::FailClosed, "{:?}", c.reason);
        let reason = c.reason.unwrap_or_default();
        assert!(reason.contains("Recursive CTEs"), "{reason}");
    }

    #[test]
    fn javascript_aggregator_fail_closed_with_reason() {
        let c = classify_native(&json!({
            "queryType": "timeseries",
            "dataSource": "wiki",
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-04"],
            "aggregations": [{
                "type": "javascript",
                "name": "js_sum",
                "fieldNames": ["added"],
                "fnAggregate": "function(c,a){return c+a;}",
                "fnCombine": "function(a,b){return a+b;}",
                "fnReset": "function(){return 0;}"
            }]
        }));
        assert_eq!(c.bucket, Bucket::FailClosed);
        let reason = c.reason.unwrap_or_default();
        assert!(reason.contains("JavaScript"), "{reason}");
    }

    // -- non-rejected forms of the same features stay supported -----------

    #[test]
    fn plain_inner_join_and_plain_cte_are_supported() {
        // FerroDruid executes INNER/LEFT equi-joins and inlines
        // non-recursive CTEs, so these must NOT be flagged.
        let join = classify_sql(
            "SELECT a.\"page\" FROM wiki a INNER JOIN wiki b ON a.\"page\" = b.\"page\" LIMIT 10",
        );
        assert_eq!(join.bucket, Bucket::Supported, "{:?}", join.reason);
        let cte = classify_sql(
            "WITH top_pages AS (SELECT \"page\", COUNT(*) AS c FROM wiki GROUP BY \"page\") \
             SELECT * FROM top_pages ORDER BY c DESC LIMIT 5",
        );
        assert_eq!(cte.bucket, Bucket::Supported, "{:?}", cte.reason);
    }

    #[test]
    fn native_timeseries_and_topn_supported() {
        let ts = classify_native(&json!({
            "queryType": "timeseries", "dataSource": "wiki", "granularity": "day",
            "intervals": ["2024-01-01/2024-01-04"],
            "aggregations": [{"type": "count", "name": "rows"}]
        }));
        assert_eq!(ts.bucket, Bucket::Supported, "{:?}", ts.reason);
        let topn = classify_native(&json!({
            "queryType": "topN", "dataSource": "wiki", "dimension": "page",
            "metric": "rows", "threshold": 5, "granularity": "all",
            "intervals": ["2024-01-01/2024-01-04"],
            "aggregations": [{"type": "count", "name": "rows"}]
        }));
        assert_eq!(topn.bucket, Bucket::Supported, "{:?}", topn.reason);
    }

    #[test]
    fn legacy_select_native_fail_closed_unknown_type_unsupported() {
        let legacy = classify_native(&json!({
            "queryType": "select", "dataSource": "wiki",
            "intervals": ["2024-01-01/2024-01-04"],
            "dimensions": [], "metrics": [],
            "pagingSpec": {"pagingIdentifiers": {}, "threshold": 5}
        }));
        assert_eq!(legacy.bucket, Bucket::FailClosed);
        assert!(
            legacy
                .reason
                .unwrap_or_default()
                .contains("legacy 'select'")
        );

        let unknown = classify_native(&json!({
            "queryType": "someBrandNewType", "dataSource": "wiki"
        }));
        assert_eq!(unknown.bucket, Bucket::Unsupported);
    }
}
