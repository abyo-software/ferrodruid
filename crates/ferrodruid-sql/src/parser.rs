// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! SQL parser for Druid SQL dialect.
//!
//! Uses [`sqlparser`] to parse standard SQL and then converts the AST into
//! Druid-specific intermediate representation that the planner can consume.

use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    self, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, SelectItem, SetExpr, Statement,
    TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use ferrodruid_common::error::{DruidError, Result};

use crate::functions::{
    DruidFunction, FrameBound, FrameMode, TimeUnit, WindowFrame, WindowFunction, WindowFunctionType,
};

// ---------------------------------------------------------------------------
// DruidSqlStatement — top-level parsed result
// ---------------------------------------------------------------------------

/// A parsed Druid SQL statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DruidSqlStatement {
    /// A SELECT query.
    Select(Box<SelectQuery>),
    /// `EXPLAIN PLAN FOR <query>` — return the native query JSON.
    ExplainPlan(Box<DruidSqlStatement>),
    /// A `UNION ALL` of two or more SELECT queries.
    UnionAll(Vec<DruidSqlStatement>),
    /// A constant `SELECT` with no `FROM` clause whose projections are all
    /// literals (e.g. `SELECT 1`, `SELECT 1 AS x`). BI tools — notably Apache
    /// Superset's Druid engine `do_ping()` — issue `SELECT 1` as a connection
    /// health check, which Calcite/Druid answers with a single synthetic row.
    /// This carries the output columns so the executor can materialise that row
    /// without touching a data source.
    ConstantSelect(Vec<ConstantColumn>),
}

/// One output column of a [`DruidSqlStatement::ConstantSelect`]: a name and its
/// literal value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstantColumn {
    /// Output column name (the alias, or Calcite-style `EXPR$<n>` when unnamed).
    pub name: String,
    /// The literal value of the column.
    pub value: SqlLiteral,
}

// ---------------------------------------------------------------------------
// SelectQuery — the core SELECT representation
// ---------------------------------------------------------------------------

/// A parsed Druid SQL SELECT query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectQuery {
    /// The projection list.
    pub projections: Vec<Projection>,
    /// The table (data source) being queried.
    pub from: TableReference,
    /// Optional WHERE clause.
    pub filter: Option<SqlExpr>,
    /// GROUP BY expressions.
    pub group_by: Vec<SqlExpr>,
    /// Optional multi-dimensional grouping spec for `GROUP BY GROUPING SETS`,
    /// `CUBE` or `ROLLUP`.
    ///
    /// When present, this is the fully-expanded list of grouping sets, each
    /// expressed as the dimension names of one set. `CUBE(a, b)` expands to
    /// all four subsets, `ROLLUP(a, b)` to `[(a, b), (a), ()]`, and explicit
    /// `GROUPING SETS (...)` is preserved as written. An empty inner vector
    /// denotes the grand-total (empty) grouping set. `None` means an ordinary
    /// (single) GROUP BY.
    pub grouping_sets: Option<Vec<Vec<String>>>,
    /// Optional HAVING clause.
    pub having: Option<SqlExpr>,
    /// ORDER BY expressions.
    pub order_by: Vec<OrderByExpr>,
    /// Optional LIMIT.
    pub limit: Option<usize>,
    /// Optional OFFSET.
    pub offset: Option<usize>,
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

/// A single projection item in a SELECT clause.
// The `Expr` variant is inherently larger than `Wildcard` (it carries a full
// `SqlExpr` AST). Enabling `serde_json`'s `preserve_order` for BI-tool column
// ordering swaps its `Map` backing to the larger `IndexMap`, which grew the
// embedded `serde_json::Value` (window `default` values) enough to trip this
// lint. Boxing every projection expression would add a heap allocation to the
// hot parse path across ~50 sites for a size-variance lint on a parser AST —
// disproportionate — so we allow it here (cf. the `too_many_arguments` allows
// in `planner.rs`).
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Projection {
    /// A wildcard (`*`).
    Wildcard,
    /// A named expression, optionally aliased.
    Expr {
        /// The expression.
        expr: SqlExpr,
        /// Optional alias name.
        alias: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// TableReference
// ---------------------------------------------------------------------------

/// A reference to a table (data source) in the FROM clause.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableReference {
    /// The data source name.
    ///
    /// For a plain table reference this is the table (data source) name. When
    /// [`TableReference::subquery`] is set (an inlined common table
    /// expression), this carries the CTE alias for diagnostics; the planner
    /// uses the subquery rather than this name as the data source.
    pub name: String,
    /// Optional alias.
    pub alias: Option<String>,
    /// An inlined sub-query data source (a resolved common table expression).
    ///
    /// When present, the FROM clause is not a base table but the body of a
    /// `WITH` CTE that was substituted in for its name. The planner lowers
    /// this to a Druid `query` data source (Druid's contract for CTEs /
    /// sub-queries).
    #[serde(default)]
    pub subquery: Option<Box<DruidSqlStatement>>,
    /// Zero or more joins applied to this base relation, in left-to-right
    /// order. `a JOIN b JOIN c` produces `[b, c]`; the planner lowers them to
    /// nested `join` data sources (the left side of `c` is the `a JOIN b`
    /// result). Empty for a plain `FROM a`.
    #[serde(default)]
    pub joins: Vec<JoinClause>,
}

// ---------------------------------------------------------------------------
// JoinClause — one `JOIN <right> ON <cond>` in a FROM clause
// ---------------------------------------------------------------------------

/// The join type recognised by the SQL front end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlJoinType {
    /// `[INNER] JOIN`.
    Inner,
    /// `LEFT [OUTER] JOIN`.
    Left,
}

/// The right-hand side of a single SQL join.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinRightSide {
    /// A base table / data source referenced by name, with an optional alias.
    Table {
        /// Data source name.
        name: String,
        /// Optional alias used to prefix the right columns.
        alias: Option<String>,
    },
    /// A `LOOKUP(<name>)` table-function reference, with an optional alias.
    Lookup {
        /// Registered lookup name.
        lookup: String,
        /// Optional alias used to prefix the right columns.
        alias: Option<String>,
    },
    /// A sub-query (`(SELECT ...)`), captured as a nested SELECT statement.
    Subquery {
        /// The parsed right sub-query.
        query: Box<DruidSqlStatement>,
        /// Optional alias used to prefix the right columns.
        alias: Option<String>,
    },
    /// An inline `(VALUES (...), ...) AS t(c1, c2)` relation.
    Values {
        /// Column names taken from the alias column list.
        column_names: Vec<String>,
        /// Literal row values (each inner vec aligns with `column_names`).
        rows: Vec<Vec<SqlLiteral>>,
        /// Optional alias used to prefix the right columns.
        alias: Option<String>,
    },
}

/// One join in a FROM clause: a right side, a type, and an equi-join condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinClause {
    /// INNER or LEFT.
    pub join_type: SqlJoinType,
    /// The right-hand relation.
    pub right: JoinRightSide,
    /// The left-side column of the equi-join condition (`left.k`).
    pub left_key: String,
    /// The right-side column of the equi-join condition (`right.k`), stripped
    /// of any table-qualifier prefix.
    pub right_key: String,
}

// ---------------------------------------------------------------------------
// SqlExpr — Druid SQL expression tree
// ---------------------------------------------------------------------------

/// An expression node in the Druid SQL AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlExpr {
    /// A column reference.
    Column(String),
    /// A literal value.
    Literal(SqlLiteral),
    /// A function call.
    Function(DruidFunction),
    /// An aggregate function call: `COUNT(*)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)`.
    Aggregate {
        /// The aggregate function name (uppercase).
        func: String,
        /// The argument expression (None for `COUNT(*)`).
        arg: Option<Box<SqlExpr>>,
        /// Whether DISTINCT was specified.
        distinct: bool,
    },
    /// A binary comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`.
    BinaryOp {
        /// Left operand.
        left: Box<SqlExpr>,
        /// Operator.
        op: BinaryOperator,
        /// Right operand.
        right: Box<SqlExpr>,
    },
    /// Logical AND.
    And(Box<SqlExpr>, Box<SqlExpr>),
    /// Logical OR.
    Or(Box<SqlExpr>, Box<SqlExpr>),
    /// Logical NOT.
    Not(Box<SqlExpr>),
    /// `expr IS NULL`.
    IsNull(Box<SqlExpr>),
    /// `expr IS NOT NULL`.
    IsNotNull(Box<SqlExpr>),
    /// `expr BETWEEN low AND high`.
    Between {
        /// The expression to test.
        expr: Box<SqlExpr>,
        /// The low bound.
        low: Box<SqlExpr>,
        /// The high bound.
        high: Box<SqlExpr>,
        /// Whether this is `NOT BETWEEN`.
        negated: bool,
    },
    /// `expr IN (values...)`.
    InList {
        /// The expression.
        expr: Box<SqlExpr>,
        /// The list of values.
        list: Vec<SqlExpr>,
        /// Whether this is `NOT IN`.
        negated: bool,
    },
    /// `expr LIKE pattern`.
    Like {
        /// The expression.
        expr: Box<SqlExpr>,
        /// The pattern.
        pattern: Box<SqlExpr>,
        /// Whether this is `NOT LIKE`.
        negated: bool,
    },
    /// A positional reference to a GROUP BY column (e.g. `GROUP BY 1`).
    Positional(usize),
    /// A CAST expression.
    Cast {
        /// The expression to cast.
        expr: Box<SqlExpr>,
        /// The target type name.
        data_type: String,
    },
    /// A star expression for `COUNT(*)`.
    Star,
    /// A window function expression (`func(...) OVER (...)`).
    Window(WindowFunction),
    /// An `ARRAY[v1, v2, ...]` literal.  Currently only used by the
    /// `MV_FILTER_ONLY` / `MV_FILTER_NONE` second-argument shapes (CL-4 /
    /// W1-D); a general-purpose ARRAY column type is out of scope until
    /// FG-3 (nested columns).
    Array(Vec<SqlExpr>),
}

/// A SQL literal value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlLiteral {
    /// A numeric integer.
    Integer(i64),
    /// A numeric floating-point.
    Float(f64),
    /// A string.
    String(String),
    /// A boolean.
    Boolean(bool),
    /// NULL.
    Null,
    /// A `TIMESTAMP '...'` / `DATE '...'` typed literal, folded at parse
    /// time to epoch milliseconds UTC (Calcite lowers these literals to a
    /// timestamp constant; Superset's time-range filter emits exactly
    /// `WHERE __time >= TIMESTAMP 'YYYY-MM-DD HH:MM:SS.ffffff'`). Keeping
    /// the millis in a dedicated variant (rather than a plain string) lets
    /// filter lowering build the same numeric `__time` bound the
    /// `TIME_PARSE('...')` path produces.
    Timestamp(i64),
}

/// A SQL binary comparison operator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOperator {
    /// `=`
    Eq,
    /// `!=` or `<>`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `+`
    Plus,
    /// `-`
    Minus,
    /// `*`
    Multiply,
    /// `/`
    Divide,
}

/// An ORDER BY expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderByExpr {
    /// The expression to order by.
    pub expr: SqlExpr,
    /// `true` for ascending (the default), `false` for descending.
    pub asc: bool,
}

// ---------------------------------------------------------------------------
// Public parse entry point
// ---------------------------------------------------------------------------

/// Maximum nesting depth of `(...)` allowed in a SQL string before we refuse
/// to call into the underlying recursive-descent parser.
///
/// `sqlparser` 0.53 uses unbounded recursion when expanding parenthesized
/// expressions / statements; a pathological input like
/// `(((UPDATE(((UPDATE...` (the 108 B reproducer cited below has paren
/// depth 41 and repeated `UPDATE` keywords) overflows the call stack in
/// debug builds and runs past the libfuzzer 10 s wall-clock timeout in
/// release builds. The 24 h fuzz runner classified this as a `timeout-…`
/// artifact, but a stripped-down `cargo test` reproduces the crash as a
/// hard stack overflow in <1 ms. Empirically, `sqlparser` blows the
/// 8 MiB default stack somewhere between 30 and 50 paren levels of this
/// shape; we cap conservatively at 24, which is still well beyond any
/// realistic hand-written Druid SQL query (sample queries from the
/// integration test suite top out around 6 levels).
///
/// Reproducer:
/// `fuzz/artifacts/fuzz_sql_parse/timeout-663b5bf798ff54c08971f570bb5277dcbd1dc449`
/// (108 B, 24 h fuzz finding 2026-05-02).
///
/// 2026-05-08 24 h fuzz wave produced two more artifacts that slipped past
/// the previous `32` cap by hitting depth 31 / 32 exactly. Standalone they
/// run in 226 ms / 544 ms, but under sancov + ASan the fuzz runner saw them
/// exceed the 10 s wall-clock cap. The cap is dropped to `24` (still ~4x
/// the real-world max) so depths 25-32 are rejected pre-parse instead of
/// driving sqlparser into super-linear backtracking.
///
/// The guard is applied at the string level here because we delegate parsing
/// to a third-party crate whose internals we cannot annotate with depth
/// tracking.
const MAX_SQL_PAREN_DEPTH: u32 = 24;

/// Walk `sql` byte-by-byte and reject inputs whose `(` or `[` nesting exceeds
/// [`MAX_SQL_PAREN_DEPTH`]. We treat string and quoted-identifier literals
/// as opaque so user-supplied data inside them cannot inflate the count.
///
/// Why `[` is counted alongside `(`: a 2026-05-04 fuzz finding (1501 B input
/// `CALL"..."(...,[BASE]+ARRAY[...+ARRAY[...+ARRAY[...]]])`) reproduces the
/// same super-linear sqlparser backtracking via square-bracket nesting in
/// ARRAY/JSON-access syntax. The `(` cap alone (commit 864f3ce) caught the
/// `(((UPDATE...` shape but missed the bracket variant.
fn check_paren_depth(sql: &str) -> Result<()> {
    let bytes = sql.as_bytes();
    let mut depth: u32 = 0;
    let mut max_depth: u32 = 0;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            // Single-quoted string literal — skip until matching `'`,
            // honoring SQL's `''` escape.
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2; // escaped quote inside string
                            continue;
                        }
                        i += 1; // closing quote
                        break;
                    }
                    i += 1;
                }
            }
            // Double-quoted identifier — same skip semantics with `""`.
            b'"' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            // Line comment `--` … to EOL.
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            // Block comment `/* … */` — non-nesting (matches sqlparser).
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < bytes.len() {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'(' | b'[' => {
                depth = depth.saturating_add(1);
                if depth > max_depth {
                    max_depth = depth;
                    if max_depth > MAX_SQL_PAREN_DEPTH {
                        return Err(DruidError::Query(format!(
                            "SQL parse error: parenthesis nesting exceeds maximum depth {MAX_SQL_PAREN_DEPTH}"
                        )));
                    }
                }
                i += 1;
            }
            b')' | b']' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            _ => i += 1,
        }
    }
    Ok(())
}

/// Maximum total count of SQL comparison operator bytes (`<`, `>`, `=`)
/// in an attacker-supplied query, outside string and quoted-identifier
/// literals.
///
/// 2026-05-15 fuzz wave found a 3675-byte input
/// (`CALL"B'"((n(S(...%a<DFP#<a6666=6<6%a<...`) carrying 428 `<` and 98
/// `=` (526 comparison ops total, 14% of the input). `parse_druid_sql`
/// spent 98 s of wall-clock inside sqlparser's recursive-descent
/// expression parser exploring binary-op precedence permutations across
/// the dense `<a<=a<=...` token stream. The existing
/// `check_paren_depth` cap did not fire (`(`/`[` depth stays under 10
/// in this input) because the blowup is at the token level, not the
/// paren level.
///
/// Realistic Druid SQL queries (the conformance suite, the Calcite
/// fixtures, the upstream Druid integration tests) carry at most 10-15
/// comparison ops even in complex WHERE chains and CASE expressions.
/// Cap = 32 gives generous headroom while rejecting all observed
/// adversarial inputs by 1-2 orders of magnitude.
///
/// The cap is enforced via `>=` (boundary-inclusive reject) so a fuzz farm
/// cannot learn the boundary and pack 31 ops in a just-below pattern — the
/// same boundary-learning failure mode a sibling regex parser hit earlier.
const MAX_SQL_COMPARISON_OPS: usize = 32;

/// Maximum consecutive run of comparison operator bytes (`<`, `>`, `=`)
/// outside string and quoted-identifier literals. ASCII whitespace
/// inside the run is preserved.
///
/// Realistic SQL never chains more than 2 consecutive comparison ops
/// (`<=`, `>=`, `<>`, `==`); 4 covers every legitimate combination
/// (`<==>` is not legal but is below the cap by inspection) and
/// rejects adversarial sequences like `<<<<<<` and `<=<=<=`. This is
/// the consecutive-run companion to `MAX_SQL_COMPARISON_OPS` and
/// closes the same boundary-learning escape that the regexp
/// consecutive-quantifier cap closes — even if a future fuzz pattern
/// drops total comparison ops to 31, a 5+ consecutive `<` run is
/// catastrophic on its own (sqlparser explores the same precedence
/// permutations within a tight token cluster).
const MAX_SQL_COMPARISON_RUN: usize = 4;

/// Maximum total count of `+` arithmetic operator bytes in an
/// attacker-supplied query, outside string and quoted-identifier
/// literals.
///
/// 2026-05-16 fuzz wave (evo-x2) found two artifacts
/// (`fuzz/artifacts/fuzz_sql_parse/timeout-cd937ce1...`, 1889 B,
/// 420 `+`; `timeout-d2db056a...`, 1482 B, 246 `+`) that burn 11.9 s
/// and 8.7 s respectively inside `parse_druid_sql`. Each input is
/// dominated by long runs of `+` (`++++++` bursts up to 420 chars)
/// interleaved with bare identifiers and unmatched `ARRAY [` opens.
/// sqlparser's recursive-descent expression engine explores binary-op
/// precedence permutations across the dense `a++++b` token stream
/// just like it did for the comparison-operator artifact closed in
/// `fc0c9b6`; the existing `MAX_SQL_COMPARISON_OPS` cap does not
/// fire because `+` is a different operator family.
///
/// Realistic Druid SQL queries carry at most ~10 `+` ops even in
/// complex column-arithmetic projections (`SELECT a + b + c + ...`).
/// Cap = 32 gives generous headroom while rejecting all observed
/// adversarial inputs by an order of magnitude.
///
/// The cap is enforced via `>=` (boundary-inclusive reject) so a fuzz
/// farm cannot learn the boundary and pack 31 ops in a just-below
/// pattern — same boundary-learning discipline as
/// `MAX_SQL_COMPARISON_OPS`.
const MAX_SQL_ARITH_PLUS_OPS: usize = 32;

/// Maximum consecutive run of `+` arithmetic operator bytes outside
/// string and quoted-identifier literals. ASCII whitespace inside
/// the run is preserved.
///
/// Realistic SQL never chains more than 1 consecutive `+` between
/// operands; 4 covers exotic but valid forms like `a + +b` (unary
/// plus) and `a + + + b` while rejecting adversarial bursts like
/// `++++++++++` and the 420-consecutive-`+` run observed in
/// timeout-cd937ce1. Consecutive-run companion to
/// `MAX_SQL_ARITH_PLUS_OPS`: even if a future fuzz pattern drops
/// total `+` to 31, a 5+ consecutive `+` burst is catastrophic on
/// its own (sqlparser explores the same precedence permutations
/// within a tight token cluster).
const MAX_SQL_ARITH_PLUS_RUN: usize = 4;

/// Maximum consecutive run of ASCII whitespace bytes (` `/`\t`/`\n`/`\r`)
/// outside string and quoted-identifier literals.
///
/// 2026-05-16 fuzz wave (evo-x2) found a 1072 B artifact
/// (`timeout-505a90ab...`) that burns 30 s+ inside `parse_druid_sql`.
/// The input is dominated by 31 `[` ARRAY-opens interleaved with
/// runs of up to 27 consecutive spaces (only 22 `+`, just below
/// `MAX_SQL_ARITH_PLUS_OPS`; only 5-8 max `[` nesting, well under
/// `MAX_SQL_PAREN_DEPTH=24`; only 8 `]` closes so most opens stay
/// pending in sqlparser's expression-state). The existing density
/// caps do not fire because each individual family stays just below
/// its own boundary. The blowup is sqlparser exploring permutations
/// across the wide whitespace-separated token stream.
///
/// BI tools (Apache Superset via the pydruid SQLAlchemy dialect) generate
/// multi-line SQL whose continuation lines carry the Python source's leading
/// indentation — pydruid 0.6.9's `get_columns` introspection query alone has a
/// **20-space** run, and nested Superset queries indent deeper. The original
/// cap of 16 rejected these legitimately-formatted queries (`SQL parse error:
/// input has a run of 20 consecutive whitespace bytes`), breaking Superset's
/// dataset column-sync. Raised to 64 = 8 indent levels × 8 spaces, a defensible
/// upper bound for real formatted SQL.
///
/// This does NOT re-open the 2026-05-16 artifact: that input's blow-up is
/// driven by its 31 `[` ARRAY-opens, which `MAX_SQL_BRACKET_OPENS_TOTAL = 12`
/// (added/tightened afterwards, 2026-06-21) now rejects on its own —
/// `reject_dense_whitespace_run_2026_05_16_artifact_shape` verifies the
/// combined shape still rejects. This cap remains as defence-in-depth against
/// pure-whitespace padding (a 1000-space run is still rejected). A fuzz re-run
/// over the SQL parser is recommended in the release QA gate after this change.
///
/// Boundary-inclusive `>` reject — keep symmetric with
/// `MAX_SQL_COMPARISON_RUN`/`MAX_SQL_ARITH_PLUS_RUN`.
const MAX_SQL_TOKEN_WHITESPACE_RUN: usize = 64;

/// Maximum total count of `[` ARRAY/subscript-open bytes in an
/// attacker-supplied query. Brackets outside string literals
/// (`'...'`) are always counted; brackets inside `"..."` quoted-
/// identifier regions are also counted (see 2026-06-21 note below).
///
/// Companion to `MAX_SQL_TOKEN_WHITESPACE_RUN` from the same
/// 2026-05-16 evo-x2 artifact (`timeout-505a90ab...`). The input
/// carries 31 `[` opens versus only 8 `]` closes (= 23 "pending"
/// ARRAY opens at end-of-input). `check_paren_depth` does not
/// trigger because between adjacent `[BASE      ]` pairs the depth
/// dips back, never accumulating beyond ~6-8. But the total `[`
/// open count is what drives sqlparser's per-`[` ARRAY-state push;
/// 31 pushes on a wide whitespace-padded stream is enough to
/// time-out by itself even if the depth cap stays satisfied.
///
/// Cap was originally 16 (~2× headroom over typical ~5-10 brackets).
///
/// 2026-06-21 fuzz farm found three new cap-hug bypasses (all at
/// bracket_open ≤ 15 outside quoted regions, staying under the old
/// cap of 16):
///   • `timeout-0d0f8883` (372 B): hid brackets inside `#"(…ARRAY[…)"`
///     double-quoted identifier regions — the counter skipped them,
///     leaving only 15 counted outside.  Fix: brackets inside `"..."`
///     are now counted too (see `b'"'` arm in `check_sql_token_density`).
///   • `timeout-810d3ddb` (246 B): `ARRAY[+NOT+++` interleaving with
///     VT/FF whitespace bytes; 15 `[` outside quoted regions.
///   • `timeout-fdf030a5` (384 B): ARRAY × NOT × plus density blowup;
///     exactly 12 `[` brackets (at the new boundary).
///
/// Cap reduced from 16 to 12 (fuzz farm 2026-06-21, 3 cap-hug bypasses):
/// realistic Druid SQL has at most ~10 `[` even in complex ARRAY
/// projections; 12 still gives ~20 % headroom while rejecting all
/// three bypass inputs.  Counting brackets inside `"..."` is the
/// second layer of defence against the quoting-hide mechanism.
///
/// Boundary-inclusive `>=` reject — same discipline as
/// `MAX_SQL_COMPARISON_OPS`/`MAX_SQL_ARITH_PLUS_OPS`.
const MAX_SQL_BRACKET_OPENS_TOTAL: usize = 12;

/// Maximum number of unclosed `(` opens at end of input (i.e. running
/// depth `(`-count − `)`-count after the scan completes), outside string
/// and quoted-identifier literals.
///
/// 2026-05-19 fuzz wave (evo-x2) found `timeout-bbec5dd4...` (198 B)
/// burning 11 s of wall-clock inside `parse_druid_sql`. The shape:
/// `CaLL"B'"(PPP*+\x0bNOT(((UPDATE(\x0c\x0c... PP+\x0bNOT\x0c+NOT\x0c+NOT...`
/// — 15 `(` opens with zero `)` closes, 17 `NOT` unary prefix keywords,
/// 19 `+` and 1 `*` operator bytes, and `\x0b`/`\x0c` (VT/FF) used as
/// inter-token whitespace.
///
/// Every existing cap stays just below boundary: max paren *depth* is
/// 15 < `MAX_SQL_PAREN_DEPTH=24`; `+` total 19 < `MAX_SQL_ARITH_PLUS_OPS=32`;
/// `[` total 0; ASCII whitespace runs 0 (VT/FF were not counted by the
/// pre-`MAX_SQL_TOKEN_WHITESPACE_EXTENDED` whitespace tracker). The
/// blowup mechanism is sqlparser's recursive-descent expression engine
/// exploring permutations of how to close 15 pending `(` opens through
/// chained unary-prefix `NOT` and binary `+` operator clusters — a
/// distinct adversarial axis from peak nesting depth.
///
/// Realistic Druid SQL queries are always balanced (`(`-count = `)`-count;
/// final unclosed = 0). A handful of unclosed `(` could legitimately appear
/// in a partial / half-typed query passed to dry-run validation; cap at
/// 4 leaves headroom for that case while rejecting the 15-unclosed
/// adversarial signature pre-parse.
///
/// Boundary-inclusive `>` reject — consistent with run-count caps.
const MAX_SQL_UNCLOSED_PAREN_OPENS: u32 = 4;

/// Maximum total count of `NOT` keywords (case-insensitive, leading- and
/// trailing-word-boundary aware) in an attacker-supplied query, outside
/// string and quoted-identifier literals.
///
/// 2026-06-21 fuzz farm found that `NOT` keyword density amplifies
/// sqlparser's recursive-descent blowup when combined with `ARRAY[` and
/// `+` operator clusters.  Artifacts `timeout-810d3ddb` (246 B) and
/// `timeout-fdf030a5` (384 B) both interleave `+NOT+++` / `NOT.++.NOT`
/// patterns that drive super-linear parse time.  While the bracket cap
/// at 12 catches these specific inputs, a future bypass may use fewer
/// brackets with denser `NOT`/`+` interleaving.
///
/// Realistic Druid SQL with complex WHERE clauses may carry up to ~15
/// `NOT` conditions (`WHERE NOT a AND NOT b AND … NOT o`); cap = 24
/// gives a margin of ~9 while rejecting adversarial NOT-dense patterns
/// at roughly half the cap.
///
/// Boundary-inclusive `>=` reject.
const MAX_SQL_NOT_KEYWORDS: usize = 24;

/// Maximum value of the product `bracket_open_total × not_kw_total` in an
/// attacker-supplied query, outside string and quoted-identifier literals.
///
/// 2026-06-22 fuzz farm found two new cap-hug bypasses that evade every
/// individual cap introduced in commit 386bfc8 (12 brackets, 24 NOTs,
/// 32 arith-plus, 4 run caps):
///
///   • `timeout-4b462550` (193 B): `CALL"B'"(b +NOTNOT+\x0bARRAY \x0b[…`
///     — 9 `[` brackets (< 12) and 10 `NOT` keywords (< 24), both under
///     cap, but their combination drives sqlparser recursive-descent into
///     a multi-second blowup. Wall time: >10 s (fuzz timeout). The
///     `+NOTNOT+` fused form also suppressed NOT counting: the leading-
///     boundary `T` left `prev_was_ident=true`, making the second `N`
///     silently fail the leading check (see NOTNOT fix in `_ =>` arm).
///
///   • `oom-6c9e5a7b` (344 B): case-mixed `ArRAY`/`ARRaY` + dense `NOT`
///     interleaving — 9 brackets × 8 NOTs = 72, measured 4.29 s /
///     522 MB RSS.
///
/// The bracket × NOT product captures the combined adversarial pressure
/// more accurately than either metric alone: real Druid SQL with complex
/// WHERE clauses uses at most ~5 NOTs and ~7 brackets (product ≤ 35);
/// adversarial inputs push the product to 72–132.
///
/// Cap = 60 gives ~70 % headroom above the realistic maximum (~35) while
/// catching both new artifacts (products 90 and 72 respectively).
///
/// Boundary-inclusive `>=` reject.
const MAX_SQL_BRACKET_NOT_JOINT: usize = 60;

/// Maximum value of the product `plus_total × not_kw_total` in an
/// attacker-supplied query, outside string and quoted-identifier literals.
///
/// 2026-06-29 fuzz farm (evo-x2) found `slow-unit-64c5e97e` (187 B): a
/// `SELECT P++NOT+T.+ARRAY[  ARRAY [ArR+NOT.++.NOT.+T_a1.+NOT+aT...` shape
/// that slips through every previously-introduced cap. Counts:
///   • brackets = 6 (< 12, bracket cap)
///   • NOTs = 9 (< 24, NOT cap)
///   • plus = 25 (< 32, plus cap)
///   • bracket × NOT = 54 (< 60, joint cap)
///   • paren × NOT = 27 (< 60, joint cap)
///
/// But `plus × NOT = 225`, and the `+NOT+ident.+ARRAY[...+NOT+...` braid
/// is exactly the precedence-explosion shape that sqlparser blows out on:
/// every unary `NOT` prefix multiplies the binary `+`-precedence search
/// space by the number of `+` operators still pending in the same
/// expression. 187 B, parsed in 91 ms locally; **5098 ms under libFuzzer's
/// sancov instrumentation on evo-x2**, well past the slow-unit threshold.
///
/// Realistic Druid SQL has at most ~10 `+` ops in column arithmetic and
/// ~5 `NOT` conditions in a complex WHERE chain, product ≤ 50.
/// Cap = 150 leaves ~3× headroom while rejecting the 225-product input by
/// 1.5×.
///
/// Boundary-inclusive `>=` reject — same discipline as
/// `MAX_SQL_BRACKET_NOT_JOINT` / `MAX_SQL_PAREN_NOT_JOINT`.
const MAX_SQL_PLUS_NOT_JOINT: usize = 150;

/// Maximum value of the product `paren_open_total × not_kw_total` in an
/// attacker-supplied query, outside string and quoted-identifier literals.
///
/// 2026-06-22 fuzz farm found two additional artifacts that evade all
/// individual caps and also evade the `MAX_SQL_BRACKET_NOT_JOINT` check
/// (few brackets, but many `NOT` keywords combined with many `(` opens):
///
///   • `oom-93980b7e` (200 B): `CaLL"Bo"(a&aa+\x0bNOT\x0c…` — 6 `(`
///     opens × 11 `NOT` keywords = 66. Measured 140 ms / 77 MB RSS.
///
///   • `oom-d36b06bf` (232 B): `SET\nNU=EC,C,…NOT(+NOT(…` — 12 `(`
///     opens × 10 `NOT` keywords = 120. Caused a **stack overflow** in
///     sqlparser's recursive-descent engine when our check was absent.
///
/// The `(` × NOT product captures the per-frame recursion pressure: each
/// open paren adds a stack frame that the unary NOT prefix then multiplies
/// into an exponential branching factor for the expression-backtracking
/// engine. Real Druid SQL rarely exceeds 3 nested calls × 3 NOT conditions
/// = 9; even complex analytics queries stay ≤ 40.
///
/// Cap = 60 gives ~50 % headroom above the realistic maximum (~40) while
/// catching both new artifacts (products 66 and 120 respectively).
///
/// Boundary-inclusive `>=` reject.
const MAX_SQL_PAREN_NOT_JOINT: usize = 60;

/// Walk `sql` byte-by-byte and reject inputs whose total or consecutive
/// operator counts exceed the caps above. Tracks comparison operators
/// (`<`/`>`/`=`) and arithmetic `+` separately, each with its own
/// total and consecutive-run cap; also tracks `[` ARRAY-open totals,
/// whitespace runs (including VT/FF for sqlparser parity), and the
/// running `(` − `)` depth so we can reject inputs that leave many
/// `(` opens pending at end of input (2026-05-19 evo-x2 fuzz finding,
/// see `MAX_SQL_UNCLOSED_PAREN_OPENS`). String literals, quoted
/// identifiers, line comments, and block comments are skipped,
/// mirroring `check_paren_depth`.
fn check_sql_token_density(sql: &str) -> Result<()> {
    let bytes = sql.as_bytes();
    let mut comp_total: usize = 0;
    let mut comp_run: usize = 0;
    let mut comp_max_run: usize = 0;
    let mut plus_total: usize = 0;
    let mut plus_run: usize = 0;
    let mut plus_max_run: usize = 0;
    let mut ws_run: usize = 0;
    let mut ws_max_run: usize = 0;
    let mut bracket_open_total: usize = 0;
    // NOT keyword count: tracks `NOT` (case-insensitive) outside string and
    // quoted-identifier literals. Used for the `MAX_SQL_NOT_KEYWORDS` cap
    // (fuzz farm 2026-06-21, 3 cap-hug bypasses: timeout-810d3ddb +
    // timeout-fdf030a5 drive blowup via ARRAY × NOT × plus density).
    let mut not_kw_total: usize = 0;
    // Tracks whether the byte immediately preceding the current position
    // (outside skip regions) was an ASCII identifier char (alphanumeric or
    // `_`). Used as the leading word-boundary guard for NOT detection: if the
    // preceding byte was an identifier char, the `N` is mid-word (`NOTABLE`,
    // `ISNOT`, etc.) and is not counted.
    let mut prev_was_ident: bool = false;
    // Running `(` − `)` depth used for the end-of-input unclosed-opens
    // check. We deliberately reuse the same `[`/`(` skip rules as
    // `check_paren_depth` so the two counters cannot diverge on
    // string/comment skips.
    let mut paren_depth: i64 = 0;
    // Total `(` opens (not net depth) used for the joint paren × NOT
    // density check (`MAX_SQL_PAREN_NOT_JOINT`). Unlike `paren_depth`,
    // this never decrements — each `(` byte outside skip regions counts
    // once regardless of whether a matching `)` follows.
    let mut paren_open_total: usize = 0;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' => {
                comp_run = 0;
                plus_run = 0;
                ws_run = 0;
                prev_was_ident = false;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                comp_run = 0;
                plus_run = 0;
                ws_run = 0;
                prev_was_ident = false;
                i += 1;
                while i < bytes.len() {
                    // Count `[` inside double-quoted identifier regions toward
                    // the bracket cap. This prevents the "hide brackets in a
                    // `#"..."` quoted identifier" bypass found by the 2026-06-21
                    // fuzz farm (`timeout-0d0f8883`, 372 B): the old scanner
                    // skipped the entire `"..."` region, so 15+ hidden `[` bytes
                    // were invisible to `bracket_open_total`.
                    if bytes[i] == b'[' {
                        bracket_open_total += 1;
                    }
                    if bytes[i] == b'"' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                comp_run = 0;
                plus_run = 0;
                ws_run = 0;
                prev_was_ident = false;
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                comp_run = 0;
                plus_run = 0;
                ws_run = 0;
                prev_was_ident = false;
                i += 2;
                while i + 1 < bytes.len() {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'<' | b'>' | b'=' => {
                comp_total += 1;
                comp_run += 1;
                if comp_run > comp_max_run {
                    comp_max_run = comp_run;
                }
                plus_run = 0;
                ws_run = 0;
                prev_was_ident = false;
                i += 1;
            }
            b'+' => {
                plus_total += 1;
                plus_run += 1;
                if plus_run > plus_max_run {
                    plus_max_run = plus_run;
                }
                comp_run = 0;
                ws_run = 0;
                prev_was_ident = false;
                i += 1;
            }
            b'[' => {
                bracket_open_total += 1;
                comp_run = 0;
                plus_run = 0;
                ws_run = 0;
                prev_was_ident = false;
                i += 1;
            }
            b'(' => {
                paren_depth = paren_depth.saturating_add(1);
                paren_open_total += 1;
                comp_run = 0;
                plus_run = 0;
                ws_run = 0;
                prev_was_ident = false;
                i += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                comp_run = 0;
                plus_run = 0;
                ws_run = 0;
                prev_was_ident = false;
                i += 1;
            }
            // VT (`\x0b`) and FF (`\x0c`) are recognized as whitespace
            // by sqlparser's lexer (and by the SQL standard), so the
            // run-cap must count them too — otherwise a fuzz input can
            // hide a wide whitespace-padded token stream by substituting
            // VT/FF for spaces. See 2026-05-19 `timeout-bbec5dd4...` for
            // the artifact that exploits this gap with `\x0c` runs.
            b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c => {
                ws_run += 1;
                if ws_run > ws_max_run {
                    ws_max_run = ws_run;
                }
                prev_was_ident = false;
                i += 1;
            }
            _ => {
                // NOT keyword detection (case-insensitive, word-boundary aware).
                // `prev_was_ident` guards the leading boundary: if the preceding
                // byte was an identifier char, this `N`/`n` is mid-word
                // (`NOTABLE`, `ISNOT`, etc.) and must not be counted as NOT.
                if !prev_was_ident
                    && (b == b'N' || b == b'n')
                    && i + 2 < bytes.len()
                    && (bytes[i + 1] == b'O' || bytes[i + 1] == b'o')
                    && (bytes[i + 2] == b'T' || bytes[i + 2] == b't')
                {
                    // Trailing boundary: the byte immediately after `T`/`t`
                    // must be a non-identifier char (whitespace, `(`, `[`, end
                    // of input, etc.) — prevents matching inside `BETWEEN`,
                    // `DISTINCT`, `NOT NULL` not followed by a space, etc.
                    let trail_ok = i + 3 >= bytes.len()
                        || !(bytes[i + 3].is_ascii_alphanumeric() || bytes[i + 3] == b'_');
                    if trail_ok {
                        not_kw_total += 1;
                    }
                    // Consume all 3 bytes of the NOT token and reset boundary
                    // state regardless of whether trail_ok fired. Without this
                    // advance, `+NOTNOT+` counts 0 NOTs: the `T` at byte 2 sets
                    // prev_was_ident=true so the second `N` at byte 3 silently
                    // fails the leading check. Advancing 3 bytes ensures that
                    // the second NOT in any fused `NOTNOT` sequence begins with
                    // prev_was_ident=false and is independently evaluated.
                    prev_was_ident = false;
                    comp_run = 0;
                    plus_run = 0;
                    ws_run = 0;
                    i += 3;
                    continue;
                }
                prev_was_ident = b.is_ascii_alphanumeric() || b == b'_';
                comp_run = 0;
                plus_run = 0;
                ws_run = 0;
                i += 1;
            }
        }
    }
    if comp_total >= MAX_SQL_COMPARISON_OPS {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has {comp_total} comparison-operator bytes \
             (`<`/`>`/`=`); maximum {} (exclusive)",
            MAX_SQL_COMPARISON_OPS
        )));
    }
    if comp_max_run > MAX_SQL_COMPARISON_RUN {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has a run of {comp_max_run} consecutive \
             comparison-operator bytes (`<`/`>`/`=`); maximum {MAX_SQL_COMPARISON_RUN}"
        )));
    }
    if plus_total >= MAX_SQL_ARITH_PLUS_OPS {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has {plus_total} arithmetic-`+` bytes; \
             maximum {} (exclusive)",
            MAX_SQL_ARITH_PLUS_OPS
        )));
    }
    if plus_max_run > MAX_SQL_ARITH_PLUS_RUN {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has a run of {plus_max_run} consecutive \
             arithmetic-`+` bytes; maximum {MAX_SQL_ARITH_PLUS_RUN}"
        )));
    }
    if ws_max_run > MAX_SQL_TOKEN_WHITESPACE_RUN {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has a run of {ws_max_run} consecutive \
             whitespace bytes; maximum {MAX_SQL_TOKEN_WHITESPACE_RUN}"
        )));
    }
    if bracket_open_total >= MAX_SQL_BRACKET_OPENS_TOTAL {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has {bracket_open_total} `[` ARRAY/subscript-open \
             bytes; maximum {} (exclusive)",
            MAX_SQL_BRACKET_OPENS_TOTAL
        )));
    }
    if not_kw_total >= MAX_SQL_NOT_KEYWORDS {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has {not_kw_total} `NOT` keywords; \
             maximum {} (exclusive)",
            MAX_SQL_NOT_KEYWORDS
        )));
    }
    // Joint bracket × NOT density cap. Neither bracket count nor NOT count
    // alone fires on the 2026-06-22 bypasses (9 brackets + 10-11 NOTs each),
    // but their product — which tracks the combined recursive-descent surface
    // exposed by ARRAY-open contexts and unary-NOT prefix chains — is a
    // reliable signal. See `MAX_SQL_BRACKET_NOT_JOINT` for the derivation.
    let bracket_not_joint = bracket_open_total.saturating_mul(not_kw_total);
    if bracket_not_joint >= MAX_SQL_BRACKET_NOT_JOINT {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has {bracket_open_total} `[` opens × \
             {not_kw_total} `NOT` keywords = {bracket_not_joint} (joint \
             bracket-NOT density); maximum {} (exclusive)",
            MAX_SQL_BRACKET_NOT_JOINT
        )));
    }
    // Joint paren × NOT density cap. Some adversarial inputs hide their
    // blowup behind parentheses instead of square brackets: each `(` adds a
    // parse frame that unary NOT multiplies into exponential backtracking.
    // Artifacts `oom-93980b7e` (6 opens × 11 NOTs = 66) and `oom-d36b06bf`
    // (12 opens × 10 NOTs = 120) both caused sqlparser stack overflows
    // when this check was absent. See `MAX_SQL_PAREN_NOT_JOINT`.
    let paren_not_joint = paren_open_total.saturating_mul(not_kw_total);
    if paren_not_joint >= MAX_SQL_PAREN_NOT_JOINT {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has {paren_open_total} `(` opens × \
             {not_kw_total} `NOT` keywords = {paren_not_joint} (joint \
             paren-NOT density); maximum {} (exclusive)",
            MAX_SQL_PAREN_NOT_JOINT
        )));
    }
    // End-of-input unclosed `(` count. `paren_depth` can be negative if
    // the input has more `)` than `(` (malformed but cheap for sqlparser
    // to reject); only the positive direction is the DoS vector.
    if paren_depth > i64::from(MAX_SQL_UNCLOSED_PAREN_OPENS) {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has {paren_depth} unclosed `(` opens at \
             end of input; maximum {MAX_SQL_UNCLOSED_PAREN_OPENS}"
        )));
    }
    // Joint plus × NOT density cap (added 2026-06-29). Ordered last so
    // historical artifacts continue to trip the cap they were authored to
    // exercise: every previous bypass with high `+`/`NOT` density also has
    // high bracket-NOT, paren-NOT, or unclosed-paren counts that fire
    // first. The motivating artifact (`slow-unit-64c5e97e`) is the first
    // input observed to keep all of those under boundary while still
    // driving sqlparser to a multi-second parse via 25 `+` × 9 `NOT`. See
    // `MAX_SQL_PLUS_NOT_JOINT`.
    let plus_not_joint = plus_total.saturating_mul(not_kw_total);
    if plus_not_joint >= MAX_SQL_PLUS_NOT_JOINT {
        return Err(DruidError::Query(format!(
            "SQL parse error: input has {plus_total} arithmetic-`+` × \
             {not_kw_total} `NOT` keywords = {plus_not_joint} (joint \
             plus-NOT density); maximum {} (exclusive)",
            MAX_SQL_PLUS_NOT_JOINT
        )));
    }
    Ok(())
}

/// Split off the first whitespace-delimited word, returning `(word, rest)`.
fn next_word(s: &str) -> Option<(&str, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }
    let end = s.find(char::is_whitespace).unwrap_or(s.len());
    Some((&s[..end], &s[end..]))
}

/// Rewrite a leading `EXPLAIN PLAN FOR <query>` (Druid / Calcite syntax) into
/// the standard `EXPLAIN <query>` that sqlparser understands. Returns `None`
/// when the input is not an `EXPLAIN PLAN FOR` statement (case-insensitive).
fn rewrite_explain_plan_for(sql: &str) -> Option<String> {
    let (w0, r0) = next_word(sql)?;
    let (w1, r1) = next_word(r0)?;
    let (w2, r2) = next_word(r1)?;
    if w0.eq_ignore_ascii_case("EXPLAIN")
        && w1.eq_ignore_ascii_case("PLAN")
        && w2.eq_ignore_ascii_case("FOR")
    {
        Some(format!("EXPLAIN {}", r2.trim_start()))
    } else {
        None
    }
}

/// Parse a Druid SQL query string into a [`DruidSqlStatement`].
pub fn parse_druid_sql(sql: &str) -> Result<DruidSqlStatement> {
    // Cheap guard before we hand the string to sqlparser's recursive-descent
    // engine: a deeply nested `(((((...` blows the stack and/or times out.
    check_paren_depth(sql)?;
    // Companion guard for token-level DoS: dense comparison-operator
    // runs drive sqlparser's expression-precedence search to seconds-
    // long wall-clock burns even when paren depth stays low. See
    // MAX_SQL_COMPARISON_OPS / MAX_SQL_COMPARISON_RUN for the
    // 2026-05-15 fuzz finding that motivates this cap.
    check_sql_token_density(sql)?;

    // Druid / Superset SQL Lab issue `EXPLAIN PLAN FOR <query>`; the underlying
    // sqlparser only understands the standard `EXPLAIN <query>`. Rewrite the
    // prefix so plan-validation queries parse (the ExplainPlan statement arm
    // handles the rest).
    let rewritten = rewrite_explain_plan_for(sql);
    let sql = rewritten.as_deref().unwrap_or(sql);

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| DruidError::Query(format!("SQL parse error: {e}")))?;

    if statements.is_empty() {
        return Err(DruidError::Query("Empty SQL statement".to_string()));
    }

    if statements.len() > 1 {
        return Err(DruidError::Query(
            "Only single SQL statements are supported".to_string(),
        ));
    }

    convert_statement(&statements[0])
}

// ---------------------------------------------------------------------------
// Internal conversion from sqlparser AST
// ---------------------------------------------------------------------------

fn convert_statement(stmt: &Statement) -> Result<DruidSqlStatement> {
    match stmt {
        Statement::Query(query) => convert_query(query),
        Statement::ExplainTable { .. } | Statement::Explain { .. } => {
            // sqlparser represents EXPLAIN PLAN FOR as Explain { statement, .. }
            if let Statement::Explain { statement, .. } = stmt {
                let inner = convert_statement(statement)?;
                Ok(DruidSqlStatement::ExplainPlan(Box::new(inner)))
            } else {
                Err(DruidError::Query("Unsupported EXPLAIN variant".to_string()))
            }
        }
        _ => Err(DruidError::Query(format!(
            "Unsupported SQL statement type: {stmt}"
        ))),
    }
}

fn convert_query(query: &ast::Query) -> Result<DruidSqlStatement> {
    // Resolve any `WITH` (common table expressions) by inlining each CTE
    // reference in a FROM clause with the CTE body. Druid does not support
    // recursive CTEs, so we reject `WITH RECURSIVE`. After resolution the
    // query carries no `WITH` clause and is processed by the normal path.
    if let Some(with) = query.with.as_ref() {
        if with.recursive {
            return Err(DruidError::Query(
                "Recursive CTEs (WITH RECURSIVE) are not supported".to_string(),
            ));
        }
        let resolved = resolve_ctes(query, with)?;
        return convert_query(&resolved);
    }

    // Handle UNION ALL
    if let SetExpr::SetOperation {
        op: ast::SetOperator::Union,
        set_quantifier,
        left,
        right,
    } = query.body.as_ref()
    {
        if *set_quantifier != ast::SetQuantifier::All {
            return Err(DruidError::Query(
                "Only UNION ALL is supported (not UNION DISTINCT)".to_string(),
            ));
        }
        // Wave 36-F (Wave 37 R1 medium `parser.rs:273-290, 359-391`): the
        // previous code silently dropped query-level ORDER BY / LIMIT /
        // OFFSET when the body was a UNION ALL. The planner therefore
        // received a different query than the user wrote. We now reject
        // those forms instead of silently rewriting them. Branch-local
        // clauses are still rejected by `collect_union_all`.
        if query.order_by.is_some() {
            return Err(DruidError::Query(
                "ORDER BY on top of UNION ALL is not supported".to_string(),
            ));
        }
        if query.limit.is_some() {
            return Err(DruidError::Query(
                "LIMIT on top of UNION ALL is not supported".to_string(),
            ));
        }
        if query.offset.is_some() {
            return Err(DruidError::Query(
                "OFFSET on top of UNION ALL is not supported".to_string(),
            ));
        }
        let mut parts = Vec::new();
        collect_union_all(left, &mut parts)?;
        collect_union_all(right, &mut parts)?;
        return Ok(DruidSqlStatement::UnionAll(parts));
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(sel) => sel,
        _ => {
            return Err(DruidError::Query(
                "Only simple SELECT queries are supported".to_string(),
            ));
        }
    };

    // FROM clause. A FROM-less SELECT is accepted only when every projection is
    // a literal — a constant SELECT (e.g. Superset's `SELECT 1` do_ping) that
    // yields one synthetic row. Any FROM-less column reference is still an error
    // (it has no source to resolve against).
    let from = if select.from.is_empty() {
        return parse_constant_select(select);
    } else {
        convert_table_ref(&select.from[0])?
    };

    // Projections
    let projections = select
        .projection
        .iter()
        .map(convert_select_item)
        .collect::<Result<Vec<_>>>()?;

    // WHERE clause
    let filter = select.selection.as_ref().map(convert_expr).transpose()?;

    // GROUP BY (including GROUPING SETS / CUBE / ROLLUP)
    let (group_by, grouping_sets) = convert_group_by(&select.group_by)?;

    // HAVING
    let having = select.having.as_ref().map(convert_expr).transpose()?;

    // ORDER BY
    let order_by = if let Some(ref ob) = query.order_by {
        convert_order_by(ob)?
    } else {
        Vec::new()
    };

    // LIMIT — Wave 36-F (Wave 37 R1 medium): non-positive-literal expressions
    // (e.g. `LIMIT -1`, `LIMIT 1+1`, `LIMIT some_col`) used to be silently
    // treated as "no limit". We now reject them explicitly so the planner
    // can never receive a different query than the user wrote.
    let limit: Option<usize> = match query.limit.as_ref() {
        None => None,
        Some(e) => expr_to_usize_clause(e, "LIMIT")?,
    };

    // OFFSET — same hardening as LIMIT.
    let offset: Option<usize> = match query.offset.as_ref() {
        None => None,
        Some(o) => expr_to_usize_clause(&o.value, "OFFSET")?,
    };

    let select_query = SelectQuery {
        projections,
        from,
        filter,
        group_by,
        grouping_sets,
        having,
        order_by,
        limit,
        offset,
    };

    Ok(DruidSqlStatement::Select(Box::new(select_query)))
}

fn collect_union_all(set_expr: &SetExpr, parts: &mut Vec<DruidSqlStatement>) -> Result<()> {
    match set_expr {
        SetExpr::Select(sel) => {
            let from = if sel.from.is_empty() {
                return Err(DruidError::Query(
                    "SELECT in UNION ALL requires a FROM clause".to_string(),
                ));
            } else {
                convert_table_ref(&sel.from[0])?
            };
            let projections = sel
                .projection
                .iter()
                .map(convert_select_item)
                .collect::<Result<Vec<_>>>()?;
            let filter = sel.selection.as_ref().map(convert_expr).transpose()?;
            let (group_by, grouping_sets) = convert_group_by(&sel.group_by)?;
            let having = sel.having.as_ref().map(convert_expr).transpose()?;
            parts.push(DruidSqlStatement::Select(Box::new(SelectQuery {
                projections,
                from,
                filter,
                group_by,
                grouping_sets,
                having,
                order_by: Vec::new(),
                limit: None,
                offset: None,
            })));
            Ok(())
        }
        SetExpr::SetOperation {
            op: ast::SetOperator::Union,
            set_quantifier,
            left,
            right,
        } if *set_quantifier == ast::SetQuantifier::All => {
            collect_union_all(left, parts)?;
            collect_union_all(right, parts)?;
            Ok(())
        }
        _ => Err(DruidError::Query(
            "Unsupported set operation in UNION ALL".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// GROUP BY conversion (plain + GROUPING SETS / CUBE / ROLLUP)
// ---------------------------------------------------------------------------

/// The result of lowering a `GROUP BY` clause: the flat list of grouping
/// expressions and, for multi-dimensional groupings, the expanded grouping
/// sets (each named by its dimension columns).
type GroupByLowering = (Vec<SqlExpr>, Option<Vec<Vec<String>>>);

/// Convert a sqlparser `GROUP BY` clause into our representation.
///
/// Returns `(group_by, grouping_sets)`:
/// - `group_by` is the flat list of grouping expressions (the union of all
///   dimensions referenced anywhere in the clause). The planner uses this to
///   build the `dimensions` array of the native groupBy.
/// - `grouping_sets` is `Some(expanded_sets)` when the clause uses
///   `GROUPING SETS`, `CUBE` or `ROLLUP`, each set listed by dimension name;
///   otherwise `None`.
fn convert_group_by(group_by: &GroupByExpr) -> Result<GroupByLowering> {
    let exprs = match group_by {
        GroupByExpr::All(_) => return Ok((Vec::new(), None)),
        GroupByExpr::Expressions(exprs, _modifiers) => exprs,
    };

    // Detect any GROUPING SETS / CUBE / ROLLUP element.
    let has_multidim = exprs
        .iter()
        .any(|e| matches!(e, Expr::GroupingSets(_) | Expr::Cube(_) | Expr::Rollup(_)));

    if !has_multidim {
        let converted = exprs.iter().map(convert_expr).collect::<Result<Vec<_>>>()?;
        return Ok((converted, None));
    }

    // Multi-dimensional grouping. Each top-level element contributes a list of
    // "atoms" (each atom is itself a tuple of dimension names). The overall
    // grouping sets are the cross product of the per-element atom lists, with
    // the atoms within each chosen combination concatenated.
    //
    // Examples:
    //   GROUPING SETS ((a,b),(a),())  -> the listed sets verbatim
    //   CUBE(a,b)                     -> [[],[a],[b],[a,b]] (all subsets)
    //   ROLLUP(a,b)                   -> [[a,b],[a],[]] (prefixes)
    //   a, CUBE(b)                    -> a combined with each cube subset
    let mut combinations: Vec<Vec<String>> = vec![Vec::new()];
    let mut flat_dims: Vec<String> = Vec::new();
    let mut seen_flat: std::collections::HashSet<String> = std::collections::HashSet::new();

    let record_flat =
        |name: &str, flat: &mut Vec<String>, seen: &mut std::collections::HashSet<String>| {
            if seen.insert(name.to_string()) {
                flat.push(name.to_string());
            }
        };

    for elem in exprs {
        // Atoms produced by this element.
        let atoms: Vec<Vec<String>> = match elem {
            Expr::GroupingSets(sets) => {
                let mut out = Vec::with_capacity(sets.len());
                for set in sets {
                    let names = exprs_to_dim_names(set)?;
                    for n in &names {
                        record_flat(n, &mut flat_dims, &mut seen_flat);
                    }
                    out.push(names);
                }
                out
            }
            Expr::Cube(sets) => {
                let groups = grouping_groups(sets)?;
                for grp in &groups {
                    for n in grp {
                        record_flat(n, &mut flat_dims, &mut seen_flat);
                    }
                }
                cube_subsets(&groups)?
            }
            Expr::Rollup(sets) => {
                let groups = grouping_groups(sets)?;
                for grp in &groups {
                    for n in grp {
                        record_flat(n, &mut flat_dims, &mut seen_flat);
                    }
                }
                rollup_prefixes(&groups)
            }
            other => {
                // A plain grouping expression sitting alongside a multi-dim
                // element (e.g. `GROUP BY a, CUBE(b)`). It is present in every
                // resulting set.
                let name = expr_to_group_dim_name(other)?;
                record_flat(&name, &mut flat_dims, &mut seen_flat);
                vec![vec![name]]
            }
        };

        // Cross product: each existing combination is extended by each atom.
        // DD R30: bound the product BEFORE reserving — several multi-dimensional
        // elements multiply, so a small query can otherwise reserve a colossal
        // (or overflowing) capacity and OOM.
        let product = combinations
            .len()
            .checked_mul(atoms.len())
            .filter(|p| *p <= MAX_GROUPING_SETS)
            .ok_or_else(|| {
                DruidError::Query(format!(
                    "GROUP BY expands to more than {MAX_GROUPING_SETS} grouping sets; \
                     reduce the number/arity of CUBE/ROLLUP/GROUPING SETS elements"
                ))
            })?;
        let mut next: Vec<Vec<String>> = Vec::with_capacity(product);
        for base in &combinations {
            for atom in &atoms {
                let mut merged = base.clone();
                for n in atom {
                    if !merged.contains(n) {
                        merged.push(n.clone());
                    }
                }
                next.push(merged);
            }
        }
        combinations = next;
    }

    // Deduplicate identical grouping sets while preserving order.
    //
    // DD R10 A#6 (deliberate deviation): standard SQL / Druid keep duplicate
    // grouping sets — e.g. `GROUPING SETS ((a), (a))` produces two output rows
    // per group. We collapse duplicates to a single set instead. This is an
    // accepted simplification (duplicate-only-differing result rows are rarely
    // relied upon and complicate the subtotalsSpec lowering); revisit if exact
    // duplicate-row parity becomes required.
    let mut seen_sets: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
    let mut grouping_sets: Vec<Vec<String>> = Vec::new();
    for c in combinations {
        if seen_sets.insert(c.clone()) {
            grouping_sets.push(c);
        }
    }

    // The flat `group_by` carries each referenced dimension once as a column
    // expression, in first-seen order, so the planner builds the full
    // `dimensions` array.
    let flat_group_by: Vec<SqlExpr> = flat_dims.into_iter().map(SqlExpr::Column).collect();

    Ok((flat_group_by, Some(grouping_sets)))
}

/// Resolve the `Vec<Vec<Expr>>` argument lists of `CUBE`/`ROLLUP` into a list
/// of grouping *groups*, preserving composite parenthesised groups as atomic
/// units. `CUBE(a, b)` parses as `[[a], [b]]` -> groups `[[a], [b]]`;
/// `CUBE((a, b), c)` parses as `[[a, b], [c]]` -> groups `[[a, b], [c]]`.
///
/// Druid (and standard SQL) treats each parenthesised group as a single
/// composite dimension for subset/prefix purposes: it is included or excluded
/// as a whole, never split. We therefore keep the group structure rather than
/// flattening to individual names (the latter produced spurious sets such as
/// `ROLLUP((a,b),c)` -> `{a}`).
fn grouping_groups(sets: &[Vec<Expr>]) -> Result<Vec<Vec<String>>> {
    let mut out = Vec::with_capacity(sets.len());
    for grp in sets {
        let names: Vec<String> = grp
            .iter()
            .map(expr_to_group_dim_name)
            .collect::<Result<_>>()?;
        out.push(names);
    }
    Ok(out)
}

/// Convert one explicit grouping-set tuple into dimension names.
fn exprs_to_dim_names(set: &[Expr]) -> Result<Vec<String>> {
    set.iter().map(expr_to_group_dim_name).collect()
}

/// Maximum CUBE arity. `CUBE(d_1, ..., d_n)` expands to `2^n` grouping sets,
/// so the count grows exponentially. We refuse beyond this arity to avoid
/// pathological expansion (and a shift overflow); the cap is far past any
/// realistic analytic query.
const MAX_CUBE_DIMS: usize = 20;

/// Maximum total number of grouping sets a single multi-dimensional `GROUP BY`
/// may expand to (DD R30). [`MAX_CUBE_DIMS`] bounds ONE `CUBE`/`ROLLUP`, but
/// several multi-dimensional elements cross-multiply — e.g. two `CUBE(20)`s
/// would reserve `2^20 * 2^20` slots and OOM (or overflow) from a tiny valid
/// query. The cross-product is bounded against this cap before each allocation.
const MAX_GROUPING_SETS: usize = 1 << 20;

/// Generate all `2^n` subsets of the `n` grouping *groups* for `CUBE`, ordered
/// from the empty set up to the full set. Each composite group is atomic: it is
/// included or excluded as a whole, and its member names are concatenated (in
/// order) into the resulting subset. `CUBE((a,b),c)` over groups `[[a,b],[c]]`
/// yields `{}, {a,b}, {c}, {a,b,c}`.
///
/// Returns an error if the group count exceeds [`MAX_CUBE_DIMS`].
fn cube_subsets(groups: &[Vec<String>]) -> Result<Vec<Vec<String>>> {
    let n = groups.len();
    if n == 0 {
        return Ok(vec![Vec::new()]);
    }
    if n > MAX_CUBE_DIMS {
        return Err(DruidError::Query(format!(
            "CUBE arity {n} exceeds the maximum supported ({MAX_CUBE_DIMS})"
        )));
    }
    let mut subsets: Vec<Vec<String>> = Vec::with_capacity(1usize << n);
    for mask in 0u32..(1u32 << n) {
        let mut subset = Vec::new();
        for (i, grp) in groups.iter().enumerate() {
            if mask & (1u32 << i) != 0 {
                subset.extend(grp.iter().cloned());
            }
        }
        subsets.push(subset);
    }
    Ok(subsets)
}

/// Generate the prefix grouping sets for `ROLLUP` over `n` grouping *groups*.
/// Each composite group is atomic and contributes all its member names when its
/// prefix length includes it. `ROLLUP((a,b),c)` over groups `[[a,b],[c]]`
/// yields `[(a,b,c), (a,b), ()]`.
fn rollup_prefixes(groups: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut out = Vec::with_capacity(groups.len() + 1);
    for len in (0..=groups.len()).rev() {
        let mut prefix = Vec::new();
        for grp in &groups[..len] {
            prefix.extend(grp.iter().cloned());
        }
        out.push(prefix);
    }
    out
}

/// Extract a dimension name from a GROUP BY grouping-set element expression.
/// Only plain column references (and compound identifiers) are supported
/// inside GROUPING SETS / CUBE / ROLLUP.
fn expr_to_group_dim_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => Ok(parts
            .iter()
            .map(|p| p.value.as_str())
            .collect::<Vec<_>>()
            .join(".")),
        Expr::Nested(inner) => expr_to_group_dim_name(inner),
        other => Err(DruidError::Query(format!(
            "GROUPING SETS / CUBE / ROLLUP only supports column references, got: {other}"
        ))),
    }
}

/// Render an [`ast::ObjectName`] (one or more dotted identifier parts) as
/// a Druid table-name string using each part's *raw* value, stripping the
/// surrounding double quotes that sqlparser preserves for quoted
/// identifiers. Without this, a query like `FROM "wikipedia_compat"` would
/// carry the literal string `"wikipedia_compat"` (quotes included) into
/// catalog lookup and silently return zero rows because no segment is
/// registered under that name — see TG-4-finding-003 (W2-D Superset
/// chart-render). Druid SQL's quoted-identifier semantics are
/// case-preserving for the *value*, not the surrounding quotes.
fn object_name_to_string(name: &ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|p| p.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

fn convert_table_ref(from: &ast::TableWithJoins) -> Result<TableReference> {
    let mut table_ref = match &from.relation {
        TableFactor::Table { name, alias, .. } => {
            let raw = object_name_to_string(name);
            // Druid's default datasource schema is `druid`, so a schema-
            // qualified `druid.telemetry` (as the pydruid dialect / Superset
            // emit: `FROM "druid"."telemetry"`) is the same datasource as
            // `telemetry`. Strip that one prefix. `INFORMATION_SCHEMA.*` and
            // `sys.*` keep their prefix (resolved as virtual schemas elsewhere).
            let table_name = match raw.split_once('.') {
                Some((schema, rest)) if schema.eq_ignore_ascii_case("druid") => rest.to_string(),
                _ => raw,
            };
            let alias_name = alias.as_ref().map(|a| a.name.value.clone());
            TableReference {
                name: table_name,
                alias: alias_name,
                subquery: None,
                joins: Vec::new(),
            }
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            // An inlined CTE body (or an explicit FROM-subquery). We convert
            // the sub-query into its own statement; the planner lowers it to a
            // Druid `query` data source.
            //
            // A column-alias list `(SELECT ...) AS t(x, y)` renames the
            // subquery's output columns. Apply it to the subquery body (or
            // reject when it cannot be honoured) rather than silently dropping
            // it, which would leave the renamed columns unresolvable.
            let alias_name = alias.as_ref().map(|a| a.name.value.clone());
            let inner = match alias.as_ref().filter(|a| !a.columns.is_empty()) {
                Some(a) => {
                    let renames: Vec<String> =
                        a.columns.iter().map(|c| c.name.value.clone()).collect();
                    let mut renamed = (**subquery).clone();
                    let cte_label = alias_name.as_deref().unwrap_or("<subquery>");
                    apply_cte_column_aliases(&mut renamed, &renames, cte_label)?;
                    convert_query(&renamed)?
                }
                None => convert_query(subquery)?,
            };
            TableReference {
                name: alias_name
                    .clone()
                    .unwrap_or_else(|| "<subquery>".to_string()),
                alias: alias_name,
                subquery: Some(Box::new(inner)),
                joins: Vec::new(),
            }
        }
        _ => {
            return Err(DruidError::Query(
                "Only simple table references are supported in FROM".to_string(),
            ));
        }
    };

    // Parse any joins applied to this base relation. The alias of the left
    // base relation (or its table name) anchors the equi-condition's
    // left/right resolution; each subsequent join uses the previous right
    // alias as a candidate left qualifier too.
    let left_anchor = table_ref
        .alias
        .clone()
        .unwrap_or_else(|| table_ref.name.clone());
    let mut left_qualifiers: Vec<String> = vec![left_anchor];
    // Aliases of *prior* joins' right sides. The executor emits a previous
    // join's right columns WITH their prefix (e.g. `b.m`), so when a later
    // join's ON condition keys off such a column the lowered `left_key` must
    // carry that prefix to match. Base-relation columns stay unprefixed.
    let mut prior_right_aliases: Vec<String> = Vec::new();

    for join in &from.joins {
        let clause = convert_join(join, &left_qualifiers, &prior_right_aliases)?;
        // The right alias becomes a valid left qualifier for any later join,
        // and a prefixed-column source for any later ON condition.
        if let Some(right_alias) = join_right_alias(&clause.right) {
            left_qualifiers.push(right_alias.clone());
            prior_right_aliases.push(right_alias);
        }
        table_ref.joins.push(clause);
    }

    Ok(table_ref)
}

/// The alias (or, failing that, the relation name) of a join's right side,
/// used as the right-column prefix and to disambiguate the equi-condition.
fn join_right_alias(right: &JoinRightSide) -> Option<String> {
    match right {
        JoinRightSide::Table { alias, name } => Some(alias.clone().unwrap_or_else(|| name.clone())),
        JoinRightSide::Lookup { alias, lookup } => {
            Some(alias.clone().unwrap_or_else(|| lookup.clone()))
        }
        JoinRightSide::Subquery { alias, .. } => alias.clone(),
        JoinRightSide::Values { alias, .. } => alias.clone(),
    }
}

/// Convert one sqlparser [`ast::Join`] into a [`JoinClause`].
fn convert_join(
    join: &ast::Join,
    left_qualifiers: &[String],
    prior_right_aliases: &[String],
) -> Result<JoinClause> {
    let (join_type, constraint) = match &join.join_operator {
        ast::JoinOperator::Inner(c) => (SqlJoinType::Inner, c),
        ast::JoinOperator::LeftOuter(c) => (SqlJoinType::Left, c),
        ast::JoinOperator::RightOuter(_) => {
            return Err(DruidError::Query(
                "RIGHT OUTER JOIN is not supported (only INNER and LEFT)".to_string(),
            ));
        }
        ast::JoinOperator::FullOuter(_) => {
            return Err(DruidError::Query(
                "FULL OUTER JOIN is not supported (only INNER and LEFT)".to_string(),
            ));
        }
        ast::JoinOperator::CrossJoin => {
            return Err(DruidError::Query(
                "CROSS JOIN is not supported (a join requires an ON equi-condition)".to_string(),
            ));
        }
        _ => {
            return Err(DruidError::Query(
                "Unsupported join type (only INNER and LEFT equi-joins are supported)".to_string(),
            ));
        }
    };

    let right = convert_join_right(&join.relation)?;
    let right_qualifier = join_right_alias(&right);

    // Only an `ON <a> = <b>` equi-condition is supported.
    let on_expr = match constraint {
        ast::JoinConstraint::On(expr) => expr,
        ast::JoinConstraint::Using(_) => {
            return Err(DruidError::Query(
                "JOIN ... USING is not supported; use ON a.k = b.k".to_string(),
            ));
        }
        ast::JoinConstraint::Natural | ast::JoinConstraint::None => {
            return Err(DruidError::Query(
                "JOIN requires an ON equi-condition (NATURAL / CROSS joins are not supported)"
                    .to_string(),
            ));
        }
    };

    let (left_key, right_key) = resolve_equi_condition(
        on_expr,
        left_qualifiers,
        prior_right_aliases,
        right_qualifier.as_deref(),
    )?;

    Ok(JoinClause {
        join_type,
        right,
        left_key,
        right_key,
    })
}

/// Convert a join's right [`TableFactor`] into a [`JoinRightSide`].
fn convert_join_right(factor: &TableFactor) -> Result<JoinRightSide> {
    match factor {
        // `LOOKUP(name)` parses as a table-valued function: a `Table` with
        // `args = Some(...)` whose name is `LOOKUP`.
        TableFactor::Table {
            name,
            alias,
            args: Some(args),
            ..
        } if object_name_to_string(name).eq_ignore_ascii_case("lookup") => {
            let lookup = lookup_arg_name(&args.args)?;
            Ok(JoinRightSide::Lookup {
                lookup,
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            })
        }
        TableFactor::Table {
            name, alias, args, ..
        } => {
            if args.is_some() {
                return Err(DruidError::Query(format!(
                    "Unsupported table function in JOIN right side: {name}"
                )));
            }
            // Strip sqlparser's preserved quote_style — see
            // `object_name_to_string` for the TG-4-finding-003 rationale.
            // Without this, a `JOIN "wikipedia_compat" ON ...` propagated
            // the literal `"wikipedia_compat"` (quotes included) into
            // join-RHS catalog lookup and silently joined against an
            // empty datasource.
            Ok(JoinRightSide::Table {
                name: object_name_to_string(name),
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            })
        }
        // `(VALUES ...)` and explicit sub-queries both arrive as `Derived`.
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let alias_name = alias.as_ref().map(|a| a.name.value.clone());
            // A `(VALUES ...)` body is captured as an inline relation; its
            // column names come from the `AS t(c1, c2)` alias column list.
            if let SetExpr::Values(values) = subquery.body.as_ref() {
                let column_names: Vec<String> = alias
                    .as_ref()
                    .map(|a| a.columns.iter().map(|c| c.name.value.clone()).collect())
                    .unwrap_or_default();
                if column_names.is_empty() {
                    return Err(DruidError::Query(
                        "VALUES join right side requires column names: \
                         `(VALUES (...)) AS t(c1, c2)`"
                            .to_string(),
                    ));
                }
                let mut rows = Vec::with_capacity(values.rows.len());
                for row in &values.rows {
                    if row.len() != column_names.len() {
                        return Err(DruidError::Query(format!(
                            "VALUES row has {} values but {} column names",
                            row.len(),
                            column_names.len()
                        )));
                    }
                    let lits = row
                        .iter()
                        .map(value_expr_to_literal)
                        .collect::<Result<Vec<_>>>()?;
                    rows.push(lits);
                }
                return Ok(JoinRightSide::Values {
                    column_names,
                    rows,
                    alias: alias_name,
                });
            }
            let inner = convert_query(subquery)?;
            Ok(JoinRightSide::Subquery {
                query: Box::new(inner),
                alias: alias_name,
            })
        }
        _ => Err(DruidError::Query(
            "Unsupported JOIN right side (expected a table, LOOKUP(...), (VALUES ...), or subquery)"
                .to_string(),
        )),
    }
}

/// Extract the single string argument of a `LOOKUP('name')` table function.
fn lookup_arg_name(args: &[FunctionArg]) -> Result<String> {
    if args.len() != 1 {
        return Err(DruidError::Query(
            "LOOKUP(...) takes exactly one argument: the lookup name".to_string(),
        ));
    }
    let expr = match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        _ => {
            return Err(DruidError::Query(
                "LOOKUP(...) argument must be a lookup name".to_string(),
            ));
        }
    };
    match expr {
        Expr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) => Ok(s.clone()),
        Expr::Identifier(id) => Ok(id.value.clone()),
        other => Err(DruidError::Query(format!(
            "LOOKUP(...) argument must be a string lookup name, got: {other}"
        ))),
    }
}

/// Resolve an `ON a.k = b.k` equi-condition into `(left_key, right_key)`.
///
/// The side whose qualifier matches the join's right alias is the right key;
/// the other side is the left key. Unqualified columns are accepted when only
/// one side is qualified (the qualified side disambiguates). A non-equality
/// operator, or a condition that references neither/both sides ambiguously, is
/// rejected with a clear error (non-equi joins are out of scope).
fn resolve_equi_condition(
    expr: &Expr,
    left_qualifiers: &[String],
    prior_right_aliases: &[String],
    right_qualifier: Option<&str>,
) -> Result<(String, String)> {
    let (left, op, right) = match expr {
        Expr::BinaryOp { left, op, right } => (left, op, right),
        _ => {
            return Err(DruidError::Query(
                "JOIN ON must be a single equi-condition `a.k = b.k` \
                 (non-equi join conditions are not supported)"
                    .to_string(),
            ));
        }
    };
    if !matches!(op, ast::BinaryOperator::Eq) {
        return Err(DruidError::Query(
            "JOIN ON only supports equality (`=`); non-equi joins are not supported".to_string(),
        ));
    }

    let lhs = qualified_column(left)?;
    let rhs = qualified_column(right)?;

    // Decide which operand is the right side based on the right qualifier.
    let lhs_is_right = matches!((&lhs.0, right_qualifier), (Some(q), Some(rq)) if q == rq);
    let rhs_is_right = matches!((&rhs.0, right_qualifier), (Some(q), Some(rq)) if q == rq);

    let (left_part, right_part) = if rhs_is_right && !lhs_is_right {
        (lhs, rhs)
    } else if lhs_is_right && !rhs_is_right {
        (rhs, lhs)
    } else if right_qualifier.is_none() {
        // No alias to disambiguate (e.g. a subquery with no alias). Fall back
        // to "left operand is the left key" ordering.
        (lhs, rhs)
    } else {
        return Err(DruidError::Query(
            "Could not resolve JOIN ON condition sides; qualify columns as \
             `left.k = right.k` so the right alias is unambiguous"
                .to_string(),
        ));
    };

    // When the left side references a *prior* join's right alias, the executor
    // has already emitted that column under its prefix (e.g. `b.m`). Emit the
    // prefixed left key so `left.get("b.m")` matches. Columns sourced from the
    // base relation (or carrying no recognised prior-right qualifier) stay
    // unprefixed.
    let _ = left_qualifiers;
    let left_key = match &left_part.0 {
        Some(qual) if prior_right_aliases.iter().any(|a| a == qual) => {
            format!("{qual}.{}", left_part.1)
        }
        _ => left_part.1,
    };
    Ok((left_key, right_part.1))
}

/// Split a column expression into `(optional qualifier, column name)`.
fn qualified_column(expr: &Expr) -> Result<(Option<String>, String)> {
    match expr {
        Expr::Identifier(id) => Ok((None, id.value.clone())),
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
            let col = parts[parts.len() - 1].value.clone();
            let qual = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].value.clone())
            } else {
                None
            };
            Ok((qual, col))
        }
        other => Err(DruidError::Query(format!(
            "JOIN ON operands must be column references, got: {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// CTE (WITH) resolution by inlining
// ---------------------------------------------------------------------------

/// Maximum number of CTEs in a single `WITH` clause. A generous cap that
/// rejects pathological inputs while permitting realistic queries.
const MAX_CTES: usize = 64;

/// Maximum number of relations (table factors / inlined sub-queries) an
/// expanded CTE body — or the final inlined outer query — may contain.
///
/// DD R27 (High): CTEs are resolved by *cloning* the referenced body at each
/// reference site (`inline_table_factor`). A chain where each CTE references the
/// previous one twice (`cN AS (SELECT … FROM c{N-1} a JOIN c{N-1} b …)`) doubles
/// the body size per level, so a ~30-CTE query expands to ~2^30 nodes and OOMs
/// the planner from a tiny input. `MAX_CTES` bounds only the *count* of
/// definitions, not the expanded size. Checking the relation count after each
/// inlining step caps the doubling chain at the first body to exceed this bound
/// (which is at most ~2× this many relations, safe to have materialized).
const MAX_CTE_EXPANDED_RELATIONS: usize = 50_000;

/// Count the relations (table factors, recursing into derived sub-queries and
/// nested joins) reachable in a query body — used to bound CTE expansion.
fn count_query_relations(query: &ast::Query) -> usize {
    count_set_expr_relations(query.body.as_ref())
}

fn count_set_expr_relations(set_expr: &SetExpr) -> usize {
    match set_expr {
        SetExpr::Select(select) => {
            let mut n = 0;
            for twj in &select.from {
                n += count_table_factor_relations(&twj.relation);
                for join in &twj.joins {
                    n += count_table_factor_relations(&join.relation);
                }
            }
            n
        }
        SetExpr::SetOperation { left, right, .. } => {
            count_set_expr_relations(left).saturating_add(count_set_expr_relations(right))
        }
        SetExpr::Query(q) => count_query_relations(q),
        _ => 0,
    }
}

fn count_table_factor_relations(factor: &TableFactor) -> usize {
    match factor {
        TableFactor::Derived { subquery, .. } => {
            1usize.saturating_add(count_query_relations(subquery))
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            let mut n = count_table_factor_relations(&table_with_joins.relation);
            for join in &table_with_joins.joins {
                n = n.saturating_add(count_table_factor_relations(&join.relation));
            }
            n
        }
        _ => 1,
    }
}

/// Resolve a query's `WITH` clause by inlining each CTE reference in a FROM
/// clause with the CTE body, producing an equivalent [`ast::Query`] that no
/// longer carries a `WITH` clause.
///
/// CTEs are resolved in declaration order: each CTE may reference any CTE
/// declared *before* it (Druid / Calcite semantics — non-recursive, no
/// forward references). The resolved body of an earlier CTE is substituted in
/// for its name when a later CTE (or the outer query) references it. A CTE
/// referenced multiple times is inlined at each reference site.
fn resolve_ctes(query: &ast::Query, with: &ast::With) -> Result<ast::Query> {
    if with.cte_tables.len() > MAX_CTES {
        return Err(DruidError::Query(format!(
            "WITH clause has too many CTEs (max {MAX_CTES})"
        )));
    }

    // Map of CTE name -> (resolved body, body relation count). The Vec preserves
    // declaration order so a later CTE sees only earlier ones; the pre-counted
    // size lets the inliner decrement an expansion budget BEFORE each clone.
    let mut resolved: Vec<(String, ast::Query, usize)> = Vec::with_capacity(with.cte_tables.len());

    for cte in &with.cte_tables {
        let name = cte.alias.name.value.clone();
        if resolved.iter().any(|(n, _, _)| n == &name) {
            return Err(DruidError::Query(format!(
                "Duplicate CTE name in WITH clause: {name}"
            )));
        }
        // Inline references to earlier CTEs within this CTE's body.
        let mut body = (*cte.query).clone();
        // A CTE body may itself carry a nested WITH; resolve that first.
        if let Some(inner_with) = body.with.take() {
            if inner_with.recursive {
                return Err(DruidError::Query(
                    "Recursive CTEs (WITH RECURSIVE) are not supported".to_string(),
                ));
            }
            body = resolve_ctes(&body, &inner_with)?;
        }
        // DD R27/R28: bound the expansion DURING inlining. A fresh budget per
        // body is decremented by each referenced CTE's size before it is cloned,
        // so even a body with many references to a large CTE is rejected before
        // the clones are materialized (not merely after the fact).
        let mut budget = MAX_CTE_EXPANDED_RELATIONS;
        inline_query(&mut body, &resolved, &mut budget)?;

        // A column-alias list `WITH a(x, y) AS (...)` renames the CTE body's
        // output columns. Apply the rename to the body's top-level projection
        // so downstream references to `x`/`y` resolve; reject when the arity
        // cannot be matched rather than silently dropping the list.
        if !cte.alias.columns.is_empty() {
            let renames: Vec<String> = cte
                .alias
                .columns
                .iter()
                .map(|c| c.name.value.clone())
                .collect();
            apply_cte_column_aliases(&mut body, &renames, &name)?;
        }
        let size = count_query_relations(&body);
        resolved.push((name, body, size));
    }

    // Inline references in the outer query body.
    let mut outer = query.clone();
    outer.with = None;
    let mut budget = MAX_CTE_EXPANDED_RELATIONS;
    inline_query(&mut outer, &resolved, &mut budget)?;
    Ok(outer)
}

/// Apply a CTE column-alias list (`WITH a(x, y) AS (...)`) to the body's
/// top-level projection, renaming each output column to the provided name.
///
/// The rename is applied by wrapping each projection in an explicit alias.
/// A `SELECT *` (or `t.*`) projection has statically-unknown arity, and a
/// rename list whose length does not match the projection arity cannot be
/// honoured — both are rejected with a clear error rather than silently
/// dropping the rename (which would leave the renamed columns unresolvable).
fn apply_cte_column_aliases(
    body: &mut ast::Query,
    renames: &[String],
    cte_name: &str,
) -> Result<()> {
    let select = match body.body.as_mut() {
        SetExpr::Select(select) => select,
        _ => {
            return Err(DruidError::Query(format!(
                "column alias list on CTE `{cte_name}` is only supported for a \
                 simple SELECT body (set operations / VALUES are not supported)"
            )));
        }
    };

    if select.projection.iter().any(|p| {
        matches!(
            p,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    }) {
        return Err(DruidError::Query(format!(
            "column alias list on CTE `{cte_name}` requires an explicit \
             projection (cannot rename the columns of a `SELECT *`)"
        )));
    }

    if select.projection.len() != renames.len() {
        return Err(DruidError::Query(format!(
            "column alias list on CTE `{cte_name}` has {} names but the body \
             projects {} columns",
            renames.len(),
            select.projection.len()
        )));
    }

    for (item, new_name) in select.projection.iter_mut().zip(renames.iter()) {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e.clone(),
            SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
            // Wildcards were already rejected above.
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => unreachable!(),
        };
        *item = SelectItem::ExprWithAlias {
            expr,
            alias: ast::Ident::new(new_name.clone()),
        };
    }

    Ok(())
}

/// Reject when the CTE expansion budget would be exceeded.
fn cte_budget_err() -> DruidError {
    DruidError::Query(format!(
        "WITH clause expands too large (CTE inlining exceeded \
         {MAX_CTE_EXPANDED_RELATIONS} relations); reduce repeated CTE references"
    ))
}

/// Recursively inline CTE references within a query's set-expression body,
/// decrementing `remaining` by each referenced body's size BEFORE cloning it so
/// expansion is bounded during (not after) inlining (DD R28).
fn inline_query(
    query: &mut ast::Query,
    ctes: &[(String, ast::Query, usize)],
    remaining: &mut usize,
) -> Result<()> {
    inline_set_expr(query.body.as_mut(), ctes, remaining)
}

fn inline_set_expr(
    set_expr: &mut SetExpr,
    ctes: &[(String, ast::Query, usize)],
    remaining: &mut usize,
) -> Result<()> {
    match set_expr {
        SetExpr::Select(select) => {
            for twj in &mut select.from {
                inline_table_factor(&mut twj.relation, ctes, remaining)?;
                for join in &mut twj.joins {
                    inline_table_factor(&mut join.relation, ctes, remaining)?;
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            inline_set_expr(left.as_mut(), ctes, remaining)?;
            inline_set_expr(right.as_mut(), ctes, remaining)?;
        }
        SetExpr::Query(q) => inline_query(q.as_mut(), ctes, remaining)?,
        _ => {}
    }
    Ok(())
}

fn inline_table_factor(
    factor: &mut TableFactor,
    ctes: &[(String, ast::Query, usize)],
    remaining: &mut usize,
) -> Result<()> {
    match factor {
        // A single-part table name matching a CTE alias is a CTE reference.
        TableFactor::Table { name, alias, .. } if name.0.len() == 1 => {
            let ref_name = &name.0[0].value;
            if let Some((_, body, size)) = ctes.iter().find(|(n, _, _)| n == ref_name) {
                // DD R28: charge the clone against the budget BEFORE materializing
                // it, so a body referencing a large CTE many times is rejected
                // before the clones exhaust memory.
                if *size > *remaining {
                    return Err(cte_budget_err());
                }
                *remaining -= *size;
                // Preserve any alias the reference site used; otherwise
                // default the derived-table alias to the CTE name so the
                // FROM remains well-formed.
                let derived_alias = alias.clone().or_else(|| {
                    Some(ast::TableAlias {
                        name: ast::Ident::new(ref_name.clone()),
                        columns: Vec::new(),
                    })
                });
                *factor = TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(body.clone()),
                    alias: derived_alias,
                };
            }
        }
        TableFactor::Derived { subquery, .. } => {
            // Nested subquery: inline within it too (CTEs are visible inside
            // FROM-subqueries of the same scope).
            inline_query(subquery.as_mut(), ctes, remaining)?;
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            inline_table_factor(&mut table_with_joins.relation, ctes, remaining)?;
            for join in &mut table_with_joins.joins {
                inline_table_factor(&mut join.relation, ctes, remaining)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Build a [`DruidSqlStatement::ConstantSelect`] from a FROM-less `SELECT`.
///
/// Succeeds only when the statement is a bare list of literal projections
/// (no WHERE / HAVING, which would imply row semantics needing a source);
/// otherwise returns the standard "requires a FROM clause" error so a
/// FROM-less column reference still fails cleanly.
fn parse_constant_select(select: &ast::Select) -> Result<DruidSqlStatement> {
    let from_err = || DruidError::Query("SELECT requires a FROM clause".to_string());
    if select.selection.is_some() || select.having.is_some() || select.projection.is_empty() {
        return Err(from_err());
    }
    let mut columns = Vec::with_capacity(select.projection.len());
    for (idx, item) in select.projection.iter().enumerate() {
        let Projection::Expr {
            expr: SqlExpr::Literal(value),
            alias,
        } = convert_select_item(item)?
        else {
            return Err(from_err());
        };
        let name = alias.unwrap_or_else(|| format!("EXPR${idx}"));
        columns.push(ConstantColumn { name, value });
    }
    Ok(DruidSqlStatement::ConstantSelect(columns))
}

fn convert_select_item(item: &SelectItem) -> Result<Projection> {
    match item {
        SelectItem::Wildcard(_) => Ok(Projection::Wildcard),
        SelectItem::UnnamedExpr(expr) => {
            let sql_expr = convert_expr(expr)?;
            Ok(Projection::Expr {
                expr: sql_expr,
                alias: None,
            })
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            let sql_expr = convert_expr(expr)?;
            Ok(Projection::Expr {
                expr: sql_expr,
                alias: Some(alias.value.clone()),
            })
        }
        _ => Err(DruidError::Query(format!(
            "Unsupported select item: {item}"
        ))),
    }
}

fn convert_expr(expr: &Expr) -> Result<SqlExpr> {
    match expr {
        Expr::Identifier(ident) => Ok(SqlExpr::Column(ident.value.clone())),

        Expr::CompoundIdentifier(parts) => {
            let name = parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            Ok(SqlExpr::Column(name))
        }

        Expr::Value(val) => convert_value(val),

        Expr::BinaryOp { left, op, right } => {
            let l = convert_expr(left)?;
            let r = convert_expr(right)?;
            match op {
                ast::BinaryOperator::And => Ok(SqlExpr::And(Box::new(l), Box::new(r))),
                ast::BinaryOperator::Or => Ok(SqlExpr::Or(Box::new(l), Box::new(r))),
                _ => {
                    let bin_op = convert_binop(op)?;
                    Ok(SqlExpr::BinaryOp {
                        left: Box::new(l),
                        op: bin_op,
                        right: Box::new(r),
                    })
                }
            }
        }

        Expr::UnaryOp {
            op: ast::UnaryOperator::Not,
            expr: inner,
        } => {
            let e = convert_expr(inner)?;
            Ok(SqlExpr::Not(Box::new(e)))
        }

        // Unary minus folds into a NUMERIC LITERAL (e.g. the `-1` step of
        // Superset's week_starting_sunday `TIME_SHIFT(..., 'P1D', -1)`).
        // Negating any other expression stays unsupported (fail-closed via
        // the catch-all below), so nothing that previously parsed changes
        // meaning.
        Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => match convert_expr(inner)? {
            SqlExpr::Literal(SqlLiteral::Integer(i)) => {
                let neg = i.checked_neg().ok_or_else(|| {
                    DruidError::Query(format!("integer literal -{i} is out of range"))
                })?;
                Ok(SqlExpr::Literal(SqlLiteral::Integer(neg)))
            }
            SqlExpr::Literal(SqlLiteral::Float(f)) => Ok(SqlExpr::Literal(SqlLiteral::Float(-f))),
            _ => Err(DruidError::Query(format!(
                "Unsupported SQL expression: {expr}"
            ))),
        },

        Expr::IsNull(inner) => {
            let e = convert_expr(inner)?;
            Ok(SqlExpr::IsNull(Box::new(e)))
        }

        Expr::IsNotNull(inner) => {
            let e = convert_expr(inner)?;
            Ok(SqlExpr::IsNotNull(Box::new(e)))
        }

        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let e = convert_expr(inner)?;
            let lo = convert_expr(low)?;
            let hi = convert_expr(high)?;
            Ok(SqlExpr::Between {
                expr: Box::new(e),
                low: Box::new(lo),
                high: Box::new(hi),
                negated: *negated,
            })
        }

        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let e = convert_expr(inner)?;
            let items = list.iter().map(convert_expr).collect::<Result<Vec<_>>>()?;
            Ok(SqlExpr::InList {
                expr: Box::new(e),
                list: items,
                negated: *negated,
            })
        }

        Expr::Like {
            expr: inner,
            pattern,
            negated,
            ..
        } => {
            let e = convert_expr(inner)?;
            let p = convert_expr(pattern)?;
            Ok(SqlExpr::Like {
                expr: Box::new(e),
                pattern: Box::new(p),
                negated: *negated,
            })
        }

        Expr::Function(func) => convert_function(func),

        Expr::Nested(inner) => convert_expr(inner),

        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let e = convert_expr(inner)?;
            Ok(SqlExpr::Cast {
                expr: Box::new(e),
                data_type: format!("{data_type}"),
            })
        }

        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let op = operand.as_ref().map(|e| convert_expr(e)).transpose()?;
            let when_clauses: Vec<(SqlExpr, SqlExpr)> = conditions
                .iter()
                .zip(results.iter())
                .map(|(c, r)| Ok((convert_expr(c)?, convert_expr(r)?)))
                .collect::<Result<Vec<_>>>()?;
            let else_r = else_result.as_ref().map(|e| convert_expr(e)).transpose()?;
            Ok(SqlExpr::Function(DruidFunction::Case {
                operand: op.map(Box::new),
                when_clauses,
                else_result: else_r.map(Box::new),
            }))
        }

        Expr::Position { expr, r#in } => {
            let substr = convert_expr(expr)?;
            let string = convert_expr(r#in)?;
            Ok(SqlExpr::Function(DruidFunction::Position {
                substr: Box::new(substr),
                string: Box::new(string),
            }))
        }

        // CEIL(expr) / FLOOR(expr) — sqlparser represents these as special Expr variants.
        Expr::Ceil { expr: inner, .. } => {
            let e = convert_expr(inner)?;
            Ok(SqlExpr::Function(DruidFunction::Ceil(Box::new(e))))
        }
        Expr::Floor { expr: inner, .. } => {
            let e = convert_expr(inner)?;
            Ok(SqlExpr::Function(DruidFunction::Floor(Box::new(e))))
        }

        // SUBSTRING(expr FROM start [FOR length])
        //
        // Wave 36-F (Wave 37 R1 low `parser.rs:601-629`): the previous code
        // silently rewrote a non-integer / out-of-range `FROM` to `start = 1`,
        // and silently dropped a non-integer `FOR`. That meant
        // syntactically valid but semantically invalid bounds were accepted
        // with rewritten semantics. We now reject them explicitly.
        Expr::Substring {
            expr: inner,
            substring_from,
            substring_for,
            ..
        } => {
            let e = convert_expr(inner)?;
            let start = match substring_from.as_ref() {
                None => 1,
                Some(sf) => match convert_expr(sf)? {
                    SqlExpr::Literal(SqlLiteral::Integer(i)) => {
                        usize::try_from(i).map_err(|_| {
                            DruidError::Query(format!(
                                "SUBSTRING start position must be a non-negative integer, got: {i}"
                            ))
                        })?
                    }
                    other => {
                        return Err(DruidError::Query(format!(
                            "SUBSTRING start position must be an integer literal, got: {other:?}"
                        )));
                    }
                },
            };
            let length = match substring_for.as_ref() {
                None => None,
                Some(sf) => match convert_expr(sf)? {
                    SqlExpr::Literal(SqlLiteral::Integer(i)) => {
                        Some(usize::try_from(i).map_err(|_| {
                            DruidError::Query(format!(
                                "SUBSTRING length must be a non-negative integer, got: {i}"
                            ))
                        })?)
                    }
                    other => {
                        return Err(DruidError::Query(format!(
                            "SUBSTRING length must be an integer literal, got: {other:?}"
                        )));
                    }
                },
            };
            Ok(SqlExpr::Function(DruidFunction::Substring {
                expr: Box::new(e),
                start,
                length,
            }))
        }

        // TRIM(expr) — sqlparser special form
        Expr::Trim { expr: inner, .. } => {
            let e = convert_expr(inner)?;
            Ok(SqlExpr::Function(DruidFunction::Trim(Box::new(e))))
        }

        // TIMESTAMP '2024-01-01 00:00:00' / DATE '2024-01-01' typed
        // literals (the shape Calcite emits and Superset's time-range
        // filter generates) fold to epoch millis UTC at parse time, so a
        // `__time >= TIMESTAMP '...'` comparison lowers to the same
        // numeric bound as the `TIME_PARSE('...')` path. An unparseable
        // time literal fails closed (Calcite rejects it too). Other typed
        // string literals stay plain strings.
        //
        // Codex-review HIGH finding A: a SQL `DATE` literal must be
        // DATE-ONLY (`YYYY-MM-DD`, folded to midnight UTC). A time
        // component (`DATE '2024-01-01 12:00:00'`) is invalid SQL —
        // Calcite rejects it — and previously slipped through the
        // timestamp parser silently; it now fails closed.
        Expr::TypedString { data_type, value } => match data_type {
            ast::DataType::Date => {
                let millis = date_literal_to_millis(value).ok_or_else(|| {
                    DruidError::Query(format!(
                        "Cannot parse DATE literal '{value}' (a DATE literal must be \
                         date-only: YYYY-MM-DD; for a date-time use TIMESTAMP '...')"
                    ))
                })?;
                Ok(SqlExpr::Literal(SqlLiteral::Timestamp(millis)))
            }
            ast::DataType::Timestamp(..) | ast::DataType::Datetime(..) => {
                let millis = time_literal_to_millis(value).ok_or_else(|| {
                    DruidError::Query(format!(
                        "Cannot parse {data_type} literal '{value}' (supported: \
                         YYYY-MM-DD, YYYY-MM-DD HH:MM:SS[.fff], \
                         YYYY-MM-DDTHH:MM:SS[.fff][Z|offset])"
                    ))
                })?;
                Ok(SqlExpr::Literal(SqlLiteral::Timestamp(millis)))
            }
            _ => Ok(SqlExpr::Literal(SqlLiteral::String(value.clone()))),
        },

        // ARRAY[v1, v2, ...] literal — CL-4 / W1-D, primarily for
        // `MV_FILTER_ONLY` / `MV_FILTER_NONE` second arguments.  We accept
        // both `ARRAY[..]` (named) and `[..]` (unnamed) since Druid SQL
        // tolerates either form in this slot.
        Expr::Array(arr) => {
            let elems = arr
                .elem
                .iter()
                .map(convert_expr)
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlExpr::Array(elems))
        }

        _ => Err(DruidError::Query(format!(
            "Unsupported SQL expression: {expr}"
        ))),
    }
}

fn convert_value(val: &Value) -> Result<SqlExpr> {
    match val {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(SqlExpr::Literal(SqlLiteral::Integer(i)))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(SqlExpr::Literal(SqlLiteral::Float(f)))
            } else {
                Err(DruidError::Query(format!("Cannot parse number: {n}")))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(SqlExpr::Literal(SqlLiteral::String(s.clone())))
        }
        Value::Boolean(b) => Ok(SqlExpr::Literal(SqlLiteral::Boolean(*b))),
        Value::Null => Ok(SqlExpr::Literal(SqlLiteral::Null)),
        _ => Err(DruidError::Query(format!("Unsupported SQL value: {val}"))),
    }
}

/// Parse an ISO-8601-ish timestamp literal to epoch milliseconds UTC:
/// `YYYY-MM-DD`, `YYYY-MM-DD[T| ]HH:MM:SS[.fff…]`, and the `Z` /
/// offset-suffixed RFC 3339 form. Covers both the shapes Superset emits
/// into `TIME_PARSE(...)` (ISO `T`) and the Calcite `TIMESTAMP '...'` /
/// `DATE '...'` typed-literal shapes (space separator, optional
/// microsecond fraction). Returns `None` when the literal matches none of
/// the shapes; callers fail closed.
pub(crate) fn time_literal_to_millis(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_millis());
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(naive.and_utc().timestamp_millis());
        }
    }
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|naive| naive.and_utc().timestamp_millis())
}

/// Parse a SQL `DATE` literal to epoch milliseconds UTC (midnight).
///
/// STRICTLY date-only (`YYYY-MM-DD`): `chrono::NaiveDate::parse_from_str`
/// rejects any trailing content, so a time component
/// (`'2024-01-01 12:00:00'`, `'2024-01-01T00:00:00Z'`) returns `None` and
/// the caller fails closed — matching Calcite, which rejects a `DATE`
/// literal carrying a time part (codex-review HIGH finding A).
pub(crate) fn date_literal_to_millis(s: &str) -> Option<i64> {
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|naive| naive.and_utc().timestamp_millis())
}

/// Convert a single `VALUES` cell expression into a [`SqlLiteral`].
///
/// Only literal values (and a unary-minus on a numeric literal) are accepted;
/// a `VALUES` join right side must be a constant inline relation.
fn value_expr_to_literal(expr: &Expr) -> Result<SqlLiteral> {
    match expr {
        Expr::Value(val) => match convert_value(val)? {
            SqlExpr::Literal(lit) => Ok(lit),
            _ => Err(DruidError::Query(
                "VALUES cell must be a literal".to_string(),
            )),
        },
        Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => match value_expr_to_literal(inner)? {
            SqlLiteral::Integer(i) => Ok(SqlLiteral::Integer(-i)),
            SqlLiteral::Float(f) => Ok(SqlLiteral::Float(-f)),
            other => Err(DruidError::Query(format!(
                "Cannot negate non-numeric VALUES literal: {other:?}"
            ))),
        },
        other => Err(DruidError::Query(format!(
            "VALUES cell must be a literal, got: {other}"
        ))),
    }
}

fn convert_binop(op: &ast::BinaryOperator) -> Result<BinaryOperator> {
    match op {
        ast::BinaryOperator::Eq => Ok(BinaryOperator::Eq),
        ast::BinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
        ast::BinaryOperator::Lt => Ok(BinaryOperator::Lt),
        ast::BinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
        ast::BinaryOperator::Gt => Ok(BinaryOperator::Gt),
        ast::BinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
        ast::BinaryOperator::Plus => Ok(BinaryOperator::Plus),
        ast::BinaryOperator::Minus => Ok(BinaryOperator::Minus),
        ast::BinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
        ast::BinaryOperator::Divide => Ok(BinaryOperator::Divide),
        _ => Err(DruidError::Query(format!("Unsupported operator: {op}"))),
    }
}

/// Expected argument count for a SQL function.
///
/// Wave 36-H (Wave 37B Medium follow-up): the per-function dispatch in
/// `convert_function` only enforced *minimum* argument counts via
/// `args.len() < N` checks, so calls like `LENGTH(a, b, c)` silently
/// reduced to `LENGTH(a)` and any extra positional args were dropped.
/// `FunctionArity` is consulted by [`validate_function_arity`] before
/// dispatch and rejects any call whose arity does not match the declared
/// shape (with explicit min/max bounds).
#[derive(Debug, Clone, Copy)]
enum FunctionArity {
    /// Exactly `n` arguments.
    Fixed(usize),
    /// Inclusive range `[min, max]` arguments (e.g. `SUBSTRING` is 2 or 3).
    Range(usize, usize),
    /// Variadic with a minimum count (e.g. `CONCAT(...)` is `>=0`,
    /// `GREATEST(...)` is `>=2`). No upper bound.
    Variadic(usize),
}

impl FunctionArity {
    /// Validate that `got` is acceptable for this arity, returning a
    /// per-function error message on mismatch.
    fn check(self, name: &str, got: usize) -> Result<()> {
        let ok = match self {
            FunctionArity::Fixed(n) => got == n,
            FunctionArity::Range(min, max) => got >= min && got <= max,
            FunctionArity::Variadic(min) => got >= min,
        };
        if ok {
            return Ok(());
        }
        let expected = match self {
            FunctionArity::Fixed(n) => format!("exactly {n} arguments"),
            FunctionArity::Range(min, max) => format!("between {min} and {max} arguments"),
            FunctionArity::Variadic(min) => format!("at least {min} arguments"),
        };
        Err(DruidError::Query(format!(
            "function {name} expects {expected}, got {got}"
        )))
    }
}

/// Look up the declared arity for a SQL function name (uppercased).
///
/// Returns `None` for names whose arity is validated entirely by their own
/// dispatch arm (none today after Wave 40-C; aggregate window functions
/// are still checked by [`convert_window_function`] which runs *before*
/// the table consult).
fn function_arity(name: &str) -> Option<FunctionArity> {
    use FunctionArity::{Fixed, Range, Variadic};
    let arity = match name {
        // ----- Time functions -----
        // `TIME_FLOOR(expr, period [, origin, timezone])`.
        "TIME_FLOOR" | "TIME_CEIL" => Range(2, 4),
        // `DATE_TRUNC('unit', expr)` — Superset time grains; lowered to
        // TIME_FLOOR.
        "DATE_TRUNC" => Fixed(2),
        "TIME_SHIFT" => Fixed(3),
        "TIME_FORMAT" => Range(2, 3),
        "TIME_PARSE" => Range(1, 2),
        "TIME_EXTRACT" => Fixed(2),
        "TIMESTAMPDIFF" => Fixed(3),
        "CURRENT_TIMESTAMP" => Fixed(0),

        // ----- Standard SQL aggregates (Wave 40-C) -----
        // Closes the W37B-PB-H1 STILL-OPEN finding from the Wave 39 DD
        // audit: Wave 36-H added the table but explicitly excluded the
        // five standard aggregates because of their bespoke `*` /
        // DISTINCT handling.  After Wave 40-C extra positional args
        // (`SUM(a, b)`, `AVG(a, b)`) are rejected with the same
        // explicit `function <name> expects ...` error as every other
        // entry.  `COUNT(*)` is parsed as a single argument
        // (`SqlExpr::Star`) and `COUNT(DISTINCT col)` is one argument
        // with the distinct flag, so all three legal `COUNT` forms have
        // arity 1.
        "COUNT" | "SUM" | "MIN" | "MAX" | "AVG" => Fixed(1),

        // ----- Aggregate-like Druid functions -----
        "APPROX_COUNT_DISTINCT" => Fixed(1),
        "APPROX_QUANTILE_DS" => Range(2, 3),
        // ANY_VALUE stays 1-arg. EARLIEST/LATEST accept either the legacy
        // 1-arg form (uses `__time` implicitly) OR the Druid 2-arg form
        // `EARLIEST(expr, timeCol)` introduced for non-`__time` ordering
        // (CL-4 / W1-D), OR the Druid VARCHAR / COMPLEX 3-arg wire form
        // `EARLIEST(expr, timeCol, maxBytesPerString)` (CL-4 / W1-J
        // finding-B).  Druid requires `maxBytesPerString` as an integer
        // literal so it can size the off-heap accumulator buffer; the
        // FerroDruid heap-string executor ignores it but accepts the
        // argument so wire-equivalent SQL parses unchanged.
        "ANY_VALUE" => Fixed(1),
        "EARLIEST" | "LATEST" => Range(1, 3),

        // ----- CL-4 / W1-D additions -----
        // `ARRAY_AGG(col [, size_limit])` — second arg is the optional
        // accumulator-size cap.  Druid also accepts a `MAX_SIZE_BYTES`
        // keyword argument but the FerroDruid parser only recognises the
        // positional cap form, matching the conformance harness queries.
        "ARRAY_AGG" => Range(1, 2),
        // `LISTAGG(col [, separator [, size_limit]])` — Druid 33+ alias of
        // `STRING_AGG` with an optional separator (default `","`).
        "LISTAGG" => Range(1, 3),
        // `STRING_AGG(col, separator [, size_limit])` — separator is
        // mandatory per Druid SQL semantics.
        "STRING_AGG" => Range(2, 3),
        // `BLOOM_FILTER(col, num_entries)` — second arg sizes the filter.
        "BLOOM_FILTER" => Fixed(2),
        // `BLOOM_FILTER_TEST(col, base64_filter)` — SQL filter form.
        "BLOOM_FILTER_TEST" => Fixed(2),
        // `MV_FILTER_ONLY(col, ARRAY[v, ...])` /
        // `MV_FILTER_NONE(col, ARRAY[v, ...])`.
        "MV_FILTER_ONLY" | "MV_FILTER_NONE" => Fixed(2),
        // `GROUPING(col, ...)` — variadic, must reference at least one
        // dimension from the surrounding GROUP BY.
        "GROUPING" => Variadic(1),

        // ----- Numeric functions -----
        "ABS" | "CEIL" | "FLOOR" | "SQRT" | "LN" | "LOG10" => Fixed(1),
        "ROUND" | "TRUNCATE" => Range(1, 2),
        "POWER" | "MOD" => Fixed(2),
        // `GREATEST` / `LEAST` are documented as `>=2` arguments; preserve
        // the existing minimum and let the variadic upper-bound stay open.
        "GREATEST" | "LEAST" => Variadic(2),

        // ----- String functions -----
        "CONCAT" => Variadic(0),
        "LENGTH" | "CHAR_LENGTH" => Fixed(1),
        "LOWER" | "UPPER" | "TRIM" | "LTRIM" | "RTRIM" | "REVERSE" => Fixed(1),
        "SUBSTRING" | "SUBSTR" => Range(2, 3),
        "REPLACE" => Fixed(3),
        "REGEXP_EXTRACT" => Range(2, 3),
        "LOOKUP" => Fixed(2),
        "LPAD" | "RPAD" => Fixed(3),
        "REPEAT" => Fixed(2),
        "POSITION" => Fixed(2),

        // ----- Conditional functions -----
        "COALESCE" => Variadic(1),
        "NULLIF" | "NVL" => Fixed(2),

        _ => return None,
    };
    Some(arity)
}

/// Validate the declared arity of a function call at the dispatch site.
///
/// Wave 36-H (Wave 37B Medium follow-up): closes the silent-extra-args
/// gap where e.g. `LENGTH(a, b, c)` reduced to `LENGTH(a)` and extra
/// positional arguments were dropped.
///
/// Wave 40-C extends the table to include the standard aggregates
/// (`COUNT`/`SUM`/`MIN`/`MAX`/`AVG`); `COUNT(*)` is one argument
/// (`SqlExpr::Star`), `COUNT(DISTINCT col)` is one argument with the
/// distinct flag, and `SUM/MIN/MAX/AVG` are exact-arity 1.  Bare
/// `COUNT()` (zero args) is now an explicit error rather than silently
/// rewritten.
fn validate_function_arity(name: &str, got: usize) -> Result<()> {
    if let Some(arity) = function_arity(name) {
        arity.check(name, got)?;
    }
    Ok(())
}

/// Map an ISO-8601 period for a SQL `DATE_TRUNC` time unit (case-insensitive),
/// matching Druid's `DATE_TRUNC` → `TIME_FLOOR` lowering.
fn date_trunc_unit_to_period(unit: &str) -> Option<&'static str> {
    Some(match unit.to_ascii_lowercase().as_str() {
        "second" => "PT1S",
        "minute" => "PT1M",
        "hour" => "PT1H",
        "day" => "P1D",
        "week" => "P1W",
        "month" => "P1M",
        "quarter" => "P3M",
        "year" => "P1Y",
        _ => return None,
    })
}

fn convert_function(func: &ast::Function) -> Result<SqlExpr> {
    let name = func.name.to_string().to_uppercase();
    let args = extract_function_args(&func.args)?;

    // Check for OVER clause (window function)
    if let Some(ref window_type) = func.over {
        return convert_window_function(&name, &args, window_type, func_is_distinct(func));
    }

    // Wave 36-H: enforce declared arity for fixed/range/variadic
    // functions before per-function dispatch.  Wave 40-C extended the
    // table to include the standard aggregates (`COUNT`/`SUM`/`MIN`/
    // `MAX`/`AVG`) — see `function_arity` — closing the W37B-PB-H1
    // STILL-OPEN finding from the Wave 39 DD audit.
    validate_function_arity(&name, args.len())?;

    // Standard aggregates
    match name.as_str() {
        "COUNT" => {
            // Wave 40-C: arity-1 already enforced above.  Three legal
            // forms reach this arm: `COUNT(*)` (arg = `Star`),
            // `COUNT(col)` (arg = column), `COUNT(DISTINCT col)` (arg
            // = column + `func_is_distinct`).
            if matches!(args.first(), Some(SqlExpr::Star)) {
                return Ok(SqlExpr::Aggregate {
                    func: "COUNT".to_string(),
                    arg: None,
                    distinct: false,
                });
            }
            return Ok(SqlExpr::Aggregate {
                func: "COUNT".to_string(),
                arg: Some(Box::new(args.into_iter().next().unwrap_or(SqlExpr::Star))),
                distinct: func_is_distinct(func),
            });
        }
        "SUM" | "MIN" | "MAX" | "AVG" => {
            let arg = args
                .into_iter()
                .next()
                .ok_or_else(|| DruidError::Query(format!("{name} requires an argument")))?;
            return Ok(SqlExpr::Aggregate {
                func: name.clone(),
                arg: Some(Box::new(arg)),
                distinct: func_is_distinct(func),
            });
        }
        _ => {}
    }

    // Druid-specific functions
    match name.as_str() {
        // ----- Time functions -----
        "DATE_TRUNC" => {
            // `DATE_TRUNC('unit', expr)` (Superset time grains) lowers to
            // `TIME_FLOOR(expr, <ISO period>)` — the same native granularity
            // path. Note the argument order is reversed vs TIME_FLOOR.
            let unit = expr_to_string(&args[0])?;
            let period = date_trunc_unit_to_period(&unit).ok_or_else(|| {
                DruidError::Query(format!("DATE_TRUNC: unsupported time unit '{unit}'"))
            })?;
            Ok(SqlExpr::Function(DruidFunction::TimeFloor {
                expr: Box::new(args[1].clone()),
                period: period.to_string(),
                timezone: None,
            }))
        }
        "TIME_FLOOR" => {
            if args.len() < 2 {
                return Err(DruidError::Query(
                    "TIME_FLOOR requires at least 2 arguments".to_string(),
                ));
            }
            let period = expr_to_string(&args[1])?;
            // args[2] is origin (skip null), args[3] is timezone
            let timezone = if args.len() >= 4 {
                expr_to_string_opt(&args[3])
            } else if args.len() == 3 {
                expr_to_string_opt(&args[2])
            } else {
                None
            };
            Ok(SqlExpr::Function(DruidFunction::TimeFloor {
                expr: Box::new(args[0].clone()),
                period,
                timezone,
            }))
        }
        "TIME_CEIL" => {
            if args.len() < 2 {
                return Err(DruidError::Query(
                    "TIME_CEIL requires at least 2 arguments".to_string(),
                ));
            }
            let period = expr_to_string(&args[1])?;
            let timezone = if args.len() >= 4 {
                expr_to_string_opt(&args[3])
            } else if args.len() == 3 {
                expr_to_string_opt(&args[2])
            } else {
                None
            };
            Ok(SqlExpr::Function(DruidFunction::TimeCeil {
                expr: Box::new(args[0].clone()),
                period,
                timezone,
            }))
        }
        "TIME_SHIFT" => {
            if args.len() < 3 {
                return Err(DruidError::Query(
                    "TIME_SHIFT requires 3 arguments".to_string(),
                ));
            }
            let period = expr_to_string(&args[1])?;
            let step = expr_to_i64(&args[2])?;
            Ok(SqlExpr::Function(DruidFunction::TimeShift {
                expr: Box::new(args[0].clone()),
                period,
                step,
            }))
        }
        "TIME_FORMAT" => {
            if args.len() < 2 {
                return Err(DruidError::Query(
                    "TIME_FORMAT requires 2 arguments".to_string(),
                ));
            }
            let format = expr_to_string(&args[1])?;
            Ok(SqlExpr::Function(DruidFunction::TimeFormat {
                expr: Box::new(args[0].clone()),
                format,
            }))
        }
        "TIME_PARSE" => {
            if args.is_empty() {
                return Err(DruidError::Query(
                    "TIME_PARSE requires at least 1 argument".to_string(),
                ));
            }
            let format = args.get(1).and_then(|e| expr_to_string(e).ok());
            Ok(SqlExpr::Function(DruidFunction::TimeParse {
                expr: Box::new(args[0].clone()),
                format,
            }))
        }
        "TIME_EXTRACT" => {
            if args.len() < 2 {
                return Err(DruidError::Query(
                    "TIME_EXTRACT requires 2 arguments".to_string(),
                ));
            }
            let unit_str = expr_to_string(&args[1])?;
            let unit = TimeUnit::parse(&unit_str)
                .ok_or_else(|| DruidError::Query(format!("Unknown time unit: {unit_str}")))?;
            Ok(SqlExpr::Function(DruidFunction::TimeExtract {
                expr: Box::new(args[0].clone()),
                unit,
            }))
        }
        "TIMESTAMPDIFF" => {
            if args.len() < 3 {
                return Err(DruidError::Query(
                    "TIMESTAMPDIFF requires 3 arguments".to_string(),
                ));
            }
            let unit_str = expr_to_string(&args[0])?;
            let unit = TimeUnit::parse(&unit_str)
                .ok_or_else(|| DruidError::Query(format!("Unknown time unit: {unit_str}")))?;
            Ok(SqlExpr::Function(DruidFunction::TimestampDiff {
                unit,
                start: Box::new(args[1].clone()),
                end: Box::new(args[2].clone()),
            }))
        }
        "CURRENT_TIMESTAMP" => Ok(SqlExpr::Function(DruidFunction::CurrentTimestamp)),

        // ----- Aggregate-like Druid functions -----
        "APPROX_COUNT_DISTINCT" => {
            let arg = args.into_iter().next().ok_or_else(|| {
                DruidError::Query("APPROX_COUNT_DISTINCT requires 1 argument".to_string())
            })?;
            Ok(SqlExpr::Function(DruidFunction::ApproxCountDistinct {
                expr: Box::new(arg),
            }))
        }
        "APPROX_QUANTILE_DS" => {
            if args.len() < 2 {
                return Err(DruidError::Query(
                    "APPROX_QUANTILE_DS requires 2 arguments".to_string(),
                ));
            }
            let probability = expr_to_f64(&args[1])?;
            Ok(SqlExpr::Function(DruidFunction::ApproxQuantileDs {
                expr: Box::new(args[0].clone()),
                probability,
            }))
        }
        "EARLIEST" => {
            // arity validated upstream as Range(1, 3).
            //
            // CL-4 / W1-J finding-B: Druid 35/36 require the VARCHAR / COMPLEX
            // form `EARLIEST(expr, timeCol, maxBytesPerString)` where
            // `maxBytesPerString` is an integer literal that sizes the
            // off-heap accumulator.  FerroDruid's heap-string executor
            // ignores it, but accepts the same wire shape so harness
            // queries parse-identically against both backends.  The
            // verified-against-Druid-35.0.1 ordering puts the literal
            // last (probed `EARLIEST("page","added",1024)` → 200 OK,
            // vs the literal-middle form which Druid rejects).
            match args.len() {
                3 => {
                    let time_col = arg_to_column_str(args.get(1), "EARLIEST timestamp column")?;
                    let _max_bytes = expr_to_usize_val(&args[2]).map_err(|_| {
                        DruidError::Query(
                            "EARLIEST third argument must be a positive integer literal \
                             (maxBytesPerString); use EARLIEST(expr, timeCol) for the 2-arg \
                             non-VARCHAR form"
                                .to_string(),
                        )
                    })?;
                    Ok(SqlExpr::Function(DruidFunction::EarliestBy {
                        expr: Box::new(args[0].clone()),
                        time_col,
                    }))
                }
                2 => {
                    let time_col = arg_to_column_str(args.get(1), "EARLIEST timestamp column")?;
                    Ok(SqlExpr::Function(DruidFunction::EarliestBy {
                        expr: Box::new(args[0].clone()),
                        time_col,
                    }))
                }
                _ => {
                    let arg = single_arg(args, "EARLIEST")?;
                    Ok(SqlExpr::Function(DruidFunction::Earliest(Box::new(arg))))
                }
            }
        }
        "LATEST" => {
            // arity validated upstream as Range(1, 3) — see EARLIEST above
            // for the W1-J finding-B rationale (literal LAST, not middle).
            match args.len() {
                3 => {
                    let time_col = arg_to_column_str(args.get(1), "LATEST timestamp column")?;
                    let _max_bytes = expr_to_usize_val(&args[2]).map_err(|_| {
                        DruidError::Query(
                            "LATEST third argument must be a positive integer literal \
                             (maxBytesPerString); use LATEST(expr, timeCol) for the 2-arg \
                             non-VARCHAR form"
                                .to_string(),
                        )
                    })?;
                    Ok(SqlExpr::Function(DruidFunction::LatestBy {
                        expr: Box::new(args[0].clone()),
                        time_col,
                    }))
                }
                2 => {
                    let time_col = arg_to_column_str(args.get(1), "LATEST timestamp column")?;
                    Ok(SqlExpr::Function(DruidFunction::LatestBy {
                        expr: Box::new(args[0].clone()),
                        time_col,
                    }))
                }
                _ => {
                    let arg = single_arg(args, "LATEST")?;
                    Ok(SqlExpr::Function(DruidFunction::Latest(Box::new(arg))))
                }
            }
        }
        "ANY_VALUE" => {
            let arg = single_arg(args, "ANY_VALUE")?;
            Ok(SqlExpr::Function(DruidFunction::AnyValue(Box::new(arg))))
        }

        // ----- Aggregate additions (CL-4 / W1-D) -----
        "ARRAY_AGG" => {
            // arity validated as Range(1, 2).
            let size_limit = if args.len() == 2 {
                Some(expr_to_usize_val(&args[1])?)
            } else {
                None
            };
            Ok(SqlExpr::Function(DruidFunction::ArrayAgg {
                expr: Box::new(args[0].clone()),
                distinct: func_is_distinct(func),
                size_limit,
            }))
        }
        "LISTAGG" => {
            // arity validated as Range(1, 3).
            let separator = if args.len() >= 2 {
                expr_to_string_literal(&args[1], "LISTAGG separator")?
            } else {
                ",".to_string()
            };
            let size_limit = if args.len() == 3 {
                Some(expr_to_usize_val(&args[2])?)
            } else {
                None
            };
            Ok(SqlExpr::Function(DruidFunction::Listagg {
                expr: Box::new(args[0].clone()),
                separator,
                size_limit,
            }))
        }
        "STRING_AGG" => {
            // arity validated as Range(2, 3).
            let separator = expr_to_string_literal(&args[1], "STRING_AGG separator")?;
            let size_limit = if args.len() == 3 {
                Some(expr_to_usize_val(&args[2])?)
            } else {
                None
            };
            Ok(SqlExpr::Function(DruidFunction::StringAgg {
                expr: Box::new(args[0].clone()),
                separator,
                size_limit,
            }))
        }
        "BLOOM_FILTER" => {
            // arity validated as Fixed(2).
            let num_entries = expr_to_i64(&args[1])?;
            if num_entries <= 0 {
                return Err(DruidError::Query(format!(
                    "BLOOM_FILTER num_entries must be a positive integer, got: {num_entries}"
                )));
            }
            Ok(SqlExpr::Function(DruidFunction::BloomFilter {
                expr: Box::new(args[0].clone()),
                num_entries,
            }))
        }
        "BLOOM_FILTER_TEST" => {
            // arity validated as Fixed(2).
            let encoded_filter =
                expr_to_string_literal(&args[1], "BLOOM_FILTER_TEST encoded filter")?;
            Ok(SqlExpr::Function(DruidFunction::BloomFilterTest {
                expr: Box::new(args[0].clone()),
                encoded_filter,
            }))
        }
        "MV_FILTER_ONLY" | "MV_FILTER_NONE" => {
            // arity validated as Fixed(2).  Second arg must be an ARRAY[..]
            // literal.
            let values = match &args[1] {
                SqlExpr::Array(items) => items.clone(),
                other => {
                    return Err(DruidError::Query(format!(
                        "{name} second argument must be an ARRAY[..] literal, got: {other:?}"
                    )));
                }
            };
            if values.is_empty() {
                return Err(DruidError::Query(format!(
                    "{name} requires a non-empty ARRAY[..] value list"
                )));
            }
            let column = Box::new(args[0].clone());
            if name == "MV_FILTER_ONLY" {
                Ok(SqlExpr::Function(DruidFunction::MvFilterOnly {
                    column,
                    values,
                }))
            } else {
                Ok(SqlExpr::Function(DruidFunction::MvFilterNone {
                    column,
                    values,
                }))
            }
        }
        "GROUPING" => {
            // arity validated as Variadic(1).  All args must be column refs
            // referring to dimensions in the surrounding GROUP BY.
            for (i, a) in args.iter().enumerate() {
                if !matches!(a, SqlExpr::Column(_)) {
                    return Err(DruidError::Query(format!(
                        "GROUPING() argument {pos} must be a column reference, got: {a:?}",
                        pos = i + 1
                    )));
                }
            }
            Ok(SqlExpr::Function(DruidFunction::Grouping(args)))
        }

        // ----- Numeric functions -----
        "ABS" => {
            let arg = single_arg(args, "ABS")?;
            Ok(SqlExpr::Function(DruidFunction::Abs(Box::new(arg))))
        }
        "CEIL" => {
            let arg = single_arg(args, "CEIL")?;
            Ok(SqlExpr::Function(DruidFunction::Ceil(Box::new(arg))))
        }
        "FLOOR" => {
            let arg = single_arg(args, "FLOOR")?;
            Ok(SqlExpr::Function(DruidFunction::Floor(Box::new(arg))))
        }
        "ROUND" => {
            if args.is_empty() {
                return Err(DruidError::Query(
                    "ROUND requires at least 1 argument".to_string(),
                ));
            }
            let digits = if args.len() >= 2 {
                expr_to_i32(&args[1])?
            } else {
                0
            };
            Ok(SqlExpr::Function(DruidFunction::Round {
                expr: Box::new(args[0].clone()),
                digits,
            }))
        }
        "POWER" => {
            if args.len() < 2 {
                return Err(DruidError::Query("POWER requires 2 arguments".to_string()));
            }
            Ok(SqlExpr::Function(DruidFunction::Power {
                base: Box::new(args[0].clone()),
                exponent: Box::new(args[1].clone()),
            }))
        }
        "SQRT" => {
            let arg = single_arg(args, "SQRT")?;
            Ok(SqlExpr::Function(DruidFunction::Sqrt(Box::new(arg))))
        }
        "LOG10" => {
            let arg = single_arg(args, "LOG10")?;
            Ok(SqlExpr::Function(DruidFunction::Log10(Box::new(arg))))
        }
        "LN" => {
            let arg = single_arg(args, "LN")?;
            Ok(SqlExpr::Function(DruidFunction::Ln(Box::new(arg))))
        }
        "MOD" => {
            if args.len() < 2 {
                return Err(DruidError::Query("MOD requires 2 arguments".to_string()));
            }
            Ok(SqlExpr::Function(DruidFunction::Mod {
                a: Box::new(args[0].clone()),
                b: Box::new(args[1].clone()),
            }))
        }
        "TRUNCATE" => {
            if args.is_empty() {
                return Err(DruidError::Query(
                    "TRUNCATE requires at least 1 argument".to_string(),
                ));
            }
            let digits = if args.len() >= 2 {
                expr_to_i32(&args[1])?
            } else {
                0
            };
            Ok(SqlExpr::Function(DruidFunction::Truncate {
                expr: Box::new(args[0].clone()),
                digits,
            }))
        }
        "GREATEST" => {
            if args.len() < 2 {
                return Err(DruidError::Query(
                    "GREATEST requires at least 2 arguments".to_string(),
                ));
            }
            Ok(SqlExpr::Function(DruidFunction::Greatest(args)))
        }
        "LEAST" => {
            if args.len() < 2 {
                return Err(DruidError::Query(
                    "LEAST requires at least 2 arguments".to_string(),
                ));
            }
            Ok(SqlExpr::Function(DruidFunction::Least(args)))
        }

        // ----- String functions -----
        "CONCAT" => Ok(SqlExpr::Function(DruidFunction::Concat(args))),
        "LENGTH" | "CHAR_LENGTH" => {
            let arg = single_arg(args, &name)?;
            Ok(SqlExpr::Function(DruidFunction::Length(Box::new(arg))))
        }
        "LOWER" => {
            let arg = single_arg(args, "LOWER")?;
            Ok(SqlExpr::Function(DruidFunction::Lower(Box::new(arg))))
        }
        "UPPER" => {
            let arg = single_arg(args, "UPPER")?;
            Ok(SqlExpr::Function(DruidFunction::Upper(Box::new(arg))))
        }
        "TRIM" => {
            let arg = single_arg(args, "TRIM")?;
            Ok(SqlExpr::Function(DruidFunction::Trim(Box::new(arg))))
        }
        "LTRIM" => {
            let arg = single_arg(args, "LTRIM")?;
            Ok(SqlExpr::Function(DruidFunction::Ltrim(Box::new(arg))))
        }
        "RTRIM" => {
            let arg = single_arg(args, "RTRIM")?;
            Ok(SqlExpr::Function(DruidFunction::Rtrim(Box::new(arg))))
        }
        "SUBSTRING" | "SUBSTR" => {
            if args.len() < 2 {
                return Err(DruidError::Query(
                    "SUBSTRING requires at least 2 arguments".to_string(),
                ));
            }
            let start = expr_to_usize_val(&args[1])?;
            let length = args.get(2).map(expr_to_usize_val).transpose()?;
            Ok(SqlExpr::Function(DruidFunction::Substring {
                expr: Box::new(args[0].clone()),
                start,
                length,
            }))
        }
        "REPLACE" => {
            if args.len() < 3 {
                return Err(DruidError::Query(
                    "REPLACE requires 3 arguments".to_string(),
                ));
            }
            let pattern = expr_to_string_literal(&args[1], "REPLACE pattern")?;
            let replacement = expr_to_string_literal(&args[2], "REPLACE replacement")?;
            Ok(SqlExpr::Function(DruidFunction::Replace {
                expr: Box::new(args[0].clone()),
                pattern,
                replacement,
            }))
        }
        "REGEXP_EXTRACT" => {
            if args.len() < 2 {
                return Err(DruidError::Query(
                    "REGEXP_EXTRACT requires at least 2 arguments".to_string(),
                ));
            }
            let pattern = expr_to_string_literal(&args[1], "REGEXP_EXTRACT pattern")?;
            let index = if args.len() >= 3 {
                expr_to_usize_val(&args[2])?
            } else {
                0
            };
            Ok(SqlExpr::Function(DruidFunction::RegexpExtract {
                expr: Box::new(args[0].clone()),
                pattern,
                index,
            }))
        }
        "LOOKUP" => {
            if args.len() < 2 {
                return Err(DruidError::Query("LOOKUP requires 2 arguments".to_string()));
            }
            let lookup_name = expr_to_string_literal(&args[1], "LOOKUP name")?;
            Ok(SqlExpr::Function(DruidFunction::Lookup {
                expr: Box::new(args[0].clone()),
                lookup_name,
            }))
        }
        "LPAD" => {
            if args.len() < 3 {
                return Err(DruidError::Query("LPAD requires 3 arguments".to_string()));
            }
            let length = expr_to_usize_val(&args[1])?;
            let pad = expr_to_string_literal(&args[2], "LPAD pad")?;
            Ok(SqlExpr::Function(DruidFunction::Lpad {
                expr: Box::new(args[0].clone()),
                length,
                pad,
            }))
        }
        "RPAD" => {
            if args.len() < 3 {
                return Err(DruidError::Query("RPAD requires 3 arguments".to_string()));
            }
            let length = expr_to_usize_val(&args[1])?;
            let pad = expr_to_string_literal(&args[2], "RPAD pad")?;
            Ok(SqlExpr::Function(DruidFunction::Rpad {
                expr: Box::new(args[0].clone()),
                length,
                pad,
            }))
        }
        "REVERSE" => {
            let arg = single_arg(args, "REVERSE")?;
            Ok(SqlExpr::Function(DruidFunction::Reverse(Box::new(arg))))
        }
        "REPEAT" => {
            if args.len() < 2 {
                return Err(DruidError::Query("REPEAT requires 2 arguments".to_string()));
            }
            let count = expr_to_usize_val(&args[1])?;
            Ok(SqlExpr::Function(DruidFunction::Repeat {
                expr: Box::new(args[0].clone()),
                count,
            }))
        }
        "POSITION" => {
            // POSITION as function call syntax: POSITION(substr, str)
            if args.len() < 2 {
                return Err(DruidError::Query(
                    "POSITION requires 2 arguments".to_string(),
                ));
            }
            Ok(SqlExpr::Function(DruidFunction::Position {
                substr: Box::new(args[0].clone()),
                string: Box::new(args[1].clone()),
            }))
        }

        // ----- Conditional functions -----
        "COALESCE" => {
            if args.is_empty() {
                return Err(DruidError::Query(
                    "COALESCE requires at least 1 argument".to_string(),
                ));
            }
            Ok(SqlExpr::Function(DruidFunction::Coalesce(args)))
        }
        "NULLIF" => {
            if args.len() < 2 {
                return Err(DruidError::Query("NULLIF requires 2 arguments".to_string()));
            }
            Ok(SqlExpr::Function(DruidFunction::Nullif {
                a: Box::new(args[0].clone()),
                b: Box::new(args[1].clone()),
            }))
        }
        "NVL" => {
            if args.len() < 2 {
                return Err(DruidError::Query("NVL requires 2 arguments".to_string()));
            }
            Ok(SqlExpr::Function(DruidFunction::Nvl {
                a: Box::new(args[0].clone()),
                b: Box::new(args[1].clone()),
            }))
        }

        _ => Err(DruidError::Query(format!("Unknown function: {name}"))),
    }
}

fn extract_function_args(args: &ast::FunctionArguments) -> Result<Vec<SqlExpr>> {
    match args {
        ast::FunctionArguments::None => Ok(Vec::new()),
        ast::FunctionArguments::Subquery(_) => Err(DruidError::Query(
            "Subquery function arguments are not supported".to_string(),
        )),
        ast::FunctionArguments::List(arg_list) => {
            let mut result = Vec::new();
            for arg in &arg_list.args {
                match arg {
                    FunctionArg::Unnamed(fae) => match fae {
                        FunctionArgExpr::Expr(e) => result.push(convert_expr(e)?),
                        FunctionArgExpr::Wildcard => result.push(SqlExpr::Star),
                        FunctionArgExpr::QualifiedWildcard(_) => result.push(SqlExpr::Star),
                    },
                    FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                        match arg {
                            FunctionArgExpr::Expr(e) => result.push(convert_expr(e)?),
                            _ => result.push(SqlExpr::Star),
                        }
                    }
                }
            }
            Ok(result)
        }
    }
}

fn func_is_distinct(func: &ast::Function) -> bool {
    if let ast::FunctionArguments::List(arg_list) = &func.args {
        arg_list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct)
    } else {
        false
    }
}

fn single_arg(mut args: Vec<SqlExpr>, name: &str) -> Result<SqlExpr> {
    if args.len() != 1 {
        return Err(DruidError::Query(format!(
            "{name} requires exactly 1 argument"
        )));
    }
    Ok(args.remove(0))
}

fn expr_to_string(expr: &SqlExpr) -> Result<String> {
    match expr {
        SqlExpr::Literal(SqlLiteral::String(s)) => Ok(s.clone()),
        SqlExpr::Column(c) => Ok(c.clone()),
        _ => Err(DruidError::Query(format!(
            "Expected a string literal, got: {expr:?}"
        ))),
    }
}

/// Strict variant of [`expr_to_string`] that rejects column references.
///
/// Wave 36-G3 (Wave 37B High `parser.rs:1172-1179`): the lenient
/// [`expr_to_string`] silently coerces `SqlExpr::Column` into a string
/// literal containing the column name, so `REPLACE(x, old_col, new_col)`
/// was rewritten to `REPLACE(x, "old_col", "new_col")` — the column
/// values were never read. Slots that exist solely for SQL string literals
/// (regex patterns, lookup names, padding strings) MUST use this helper.
fn expr_to_string_literal(expr: &SqlExpr, ctx: &str) -> Result<String> {
    match expr {
        SqlExpr::Literal(SqlLiteral::String(s)) => Ok(s.clone()),
        _ => Err(DruidError::Query(format!(
            "{ctx} requires a string literal, got: {expr:?}"
        ))),
    }
}

fn expr_to_string_opt(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Literal(SqlLiteral::String(s)) => Some(s.clone()),
        SqlExpr::Column(c) => Some(c.clone()),
        SqlExpr::Literal(SqlLiteral::Null) => None,
        _ => None,
    }
}

fn expr_to_i64(expr: &SqlExpr) -> Result<i64> {
    match expr {
        SqlExpr::Literal(SqlLiteral::Integer(i)) => Ok(*i),
        _ => Err(DruidError::Query(format!(
            "Expected an integer literal, got: {expr:?}"
        ))),
    }
}

fn expr_to_i32(expr: &SqlExpr) -> Result<i32> {
    match expr {
        SqlExpr::Literal(SqlLiteral::Integer(i)) => i32::try_from(*i)
            .map_err(|_| DruidError::Query(format!("Integer out of i32 range: {i}"))),
        _ => Err(DruidError::Query(format!(
            "Expected an integer literal, got: {expr:?}"
        ))),
    }
}

fn expr_to_f64(expr: &SqlExpr) -> Result<f64> {
    match expr {
        SqlExpr::Literal(SqlLiteral::Float(f)) => Ok(*f),
        SqlExpr::Literal(SqlLiteral::Integer(i)) => Ok(*i as f64),
        _ => Err(DruidError::Query(format!(
            "Expected a numeric literal, got: {expr:?}"
        ))),
    }
}

fn expr_to_usize(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse::<usize>().ok(),
        _ => None,
    }
}

/// Strict variant of [`expr_to_usize`] used by `LIMIT` / `OFFSET`.
///
/// Wave 36-F (Wave 37 R1 medium `parser.rs:339-343, 1220-1224`): the previous
/// code silently dropped any unsupported `LIMIT` / `OFFSET` expression to
/// `None`, so `LIMIT -1`, `LIMIT 1+1`, or `OFFSET some_col` all silently
/// disabled pagination. This helper rejects them with a precise error
/// instead, returning `None` only for the explicit `LIMIT NULL` form.
fn expr_to_usize_clause(expr: &Expr, clause: &'static str) -> Result<Option<usize>> {
    match expr {
        // `LIMIT NULL` / `OFFSET NULL` — Druid treats these as "no limit".
        Expr::Value(Value::Null) => Ok(None),
        Expr::Value(Value::Number(n, _)) => match n.parse::<usize>() {
            Ok(v) => Ok(Some(v)),
            Err(_) => Err(DruidError::Query(format!(
                "{clause} requires a non-negative integer literal, got: {n}"
            ))),
        },
        other => Err(DruidError::Query(format!(
            "{clause} requires a non-negative integer literal, got: {other}"
        ))),
    }
}

fn expr_to_usize_val(expr: &SqlExpr) -> Result<usize> {
    match expr {
        SqlExpr::Literal(SqlLiteral::Integer(i)) => {
            usize::try_from(*i).map_err(|_| DruidError::Query(format!("Invalid position: {i}")))
        }
        _ => Err(DruidError::Query(format!(
            "Expected an integer literal, got: {expr:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Window function conversion
// ---------------------------------------------------------------------------

fn convert_window_function(
    name: &str,
    args: &[SqlExpr],
    window_type: &ast::WindowType,
    is_distinct: bool,
) -> Result<SqlExpr> {
    let window_spec = match window_type {
        ast::WindowType::WindowSpec(spec) => spec,
        ast::WindowType::NamedWindow(_) => {
            return Err(DruidError::Query(
                "Named windows are not supported".to_string(),
            ));
        }
    };

    // Parse the function type
    let function = match name {
        "ROW_NUMBER" => WindowFunctionType::RowNumber,
        "RANK" => WindowFunctionType::Rank,
        "DENSE_RANK" => WindowFunctionType::DenseRank,
        "LAG" => {
            let col = arg_to_column_str(args.first(), "LAG")?;
            let offset = args.get(1).map(expr_to_usize_val).transpose()?.unwrap_or(1);
            let default = args.get(2).map(sql_expr_to_json).transpose()?;
            WindowFunctionType::Lag {
                column: col,
                offset,
                default,
            }
        }
        "LEAD" => {
            let col = arg_to_column_str(args.first(), "LEAD")?;
            let offset = args.get(1).map(expr_to_usize_val).transpose()?.unwrap_or(1);
            let default = args.get(2).map(sql_expr_to_json).transpose()?;
            WindowFunctionType::Lead {
                column: col,
                offset,
                default,
            }
        }
        "FIRST_VALUE" => {
            let col = arg_to_column_str(args.first(), "FIRST_VALUE")?;
            WindowFunctionType::FirstValue { column: col }
        }
        "LAST_VALUE" => {
            let col = arg_to_column_str(args.first(), "LAST_VALUE")?;
            WindowFunctionType::LastValue { column: col }
        }
        "SUM" => {
            let col = arg_to_column_str(args.first(), "SUM OVER")?;
            WindowFunctionType::Sum { column: col }
        }
        "AVG" => {
            let col = arg_to_column_str(args.first(), "AVG OVER")?;
            WindowFunctionType::Avg { column: col }
        }
        "MIN" => {
            let col = arg_to_column_str(args.first(), "MIN OVER")?;
            WindowFunctionType::Min { column: col }
        }
        "MAX" => {
            let col = arg_to_column_str(args.first(), "MAX OVER")?;
            WindowFunctionType::Max { column: col }
        }
        // ----- CL-4 / W1-D window function additions -----
        "NTH_VALUE" => {
            // NTH_VALUE(col, n) — 2 positional args.
            if args.len() != 2 {
                return Err(DruidError::Query(
                    "NTH_VALUE OVER requires exactly 2 arguments (column, n)".to_string(),
                ));
            }
            let col = arg_to_column_str(args.first(), "NTH_VALUE")?;
            let n = expr_to_usize_val(&args[1])?;
            if n == 0 {
                return Err(DruidError::Query(
                    "NTH_VALUE position must be >= 1".to_string(),
                ));
            }
            WindowFunctionType::NthValue { column: col, n }
        }
        "NTILE" => {
            // NTILE(n) — single integer literal.
            if args.len() != 1 {
                return Err(DruidError::Query(
                    "NTILE OVER requires exactly 1 argument (tile count)".to_string(),
                ));
            }
            let tiles = expr_to_usize_val(&args[0])?;
            if tiles == 0 {
                return Err(DruidError::Query(
                    "NTILE tile count must be >= 1".to_string(),
                ));
            }
            WindowFunctionType::Ntile { tiles }
        }
        "CUME_DIST" => {
            if !args.is_empty() {
                return Err(DruidError::Query(
                    "CUME_DIST OVER takes no arguments".to_string(),
                ));
            }
            WindowFunctionType::CumeDist
        }
        "PERCENT_RANK" => {
            if !args.is_empty() {
                return Err(DruidError::Query(
                    "PERCENT_RANK OVER takes no arguments".to_string(),
                ));
            }
            WindowFunctionType::PercentRank
        }
        "COUNT" => {
            // Wave 36-G3 (Wave 37B High `parser.rs:1256-1305`): preserve
            // whether this was `COUNT(*)`, `COUNT(col)`, or
            // `COUNT(DISTINCT col)` instead of collapsing all forms into
            // a bare row count. `COUNT(*)` => column = None. Any other
            // form requires a single column reference.
            let column = match args.first() {
                None | Some(SqlExpr::Star) => None,
                Some(SqlExpr::Column(c)) => Some(c.clone()),
                Some(other) => {
                    return Err(DruidError::Query(format!(
                        "COUNT OVER requires `*` or a column reference, got: {other:?}"
                    )));
                }
            };
            if args.len() > 1 {
                return Err(DruidError::Query(
                    "COUNT OVER takes at most 1 argument".to_string(),
                ));
            }
            WindowFunctionType::Count {
                column,
                distinct: is_distinct,
            }
        }
        _ => {
            return Err(DruidError::Query(format!(
                "Unsupported window function: {name}"
            )));
        }
    };

    // Parse PARTITION BY
    //
    // Wave 36-G3 (Wave 37B High `parser.rs:1313-1324`): the previous code
    // silently dropped any partition expression that was not a bare column
    // reference, so e.g. `PARTITION BY foo + 1` parsed successfully but
    // partitioned the entire input as a single group. Reject any
    // unsupported partition expression with an explicit error instead.
    let partition_by: Vec<String> = window_spec
        .partition_by
        .iter()
        .map(|e| match convert_expr(e)? {
            SqlExpr::Column(c) => Ok(c),
            other => Err(DruidError::Query(format!(
                "PARTITION BY only supports bare column references, got: {other:?}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;

    // Parse ORDER BY
    let order_by = if window_spec.order_by.is_empty() {
        Vec::new()
    } else {
        // Convert Vec<ast::OrderByExpr> into our OrderByExpr format.
        window_spec
            .order_by
            .iter()
            .map(|ob| {
                let expr = convert_expr(&ob.expr)?;
                let asc = ob.asc.unwrap_or(true);
                Ok(OrderByExpr { expr, asc })
            })
            .collect::<Result<Vec<_>>>()?
    };

    // Parse window frame
    let frame = window_spec
        .window_frame
        .as_ref()
        .map(convert_window_frame)
        .transpose()?;

    Ok(SqlExpr::Window(WindowFunction {
        function,
        partition_by,
        order_by,
        frame,
    }))
}

fn arg_to_column_str(arg: Option<&SqlExpr>, func_name: &str) -> Result<String> {
    match arg {
        Some(SqlExpr::Column(c)) => Ok(c.clone()),
        Some(SqlExpr::Star) => Ok("*".to_string()),
        _ => Err(DruidError::Query(format!(
            "{func_name} requires a column argument"
        ))),
    }
}

fn sql_expr_to_json(expr: &SqlExpr) -> Result<serde_json::Value> {
    match expr {
        SqlExpr::Literal(SqlLiteral::Integer(i)) => Ok(serde_json::Value::from(*i)),
        SqlExpr::Literal(SqlLiteral::Float(f)) => Ok(serde_json::json!(*f)),
        SqlExpr::Literal(SqlLiteral::String(s)) => Ok(serde_json::Value::String(s.clone())),
        SqlExpr::Literal(SqlLiteral::Boolean(b)) => Ok(serde_json::Value::Bool(*b)),
        SqlExpr::Literal(SqlLiteral::Null) => Ok(serde_json::Value::Null),
        SqlExpr::Literal(SqlLiteral::Timestamp(ms)) => Ok(serde_json::Value::from(*ms)),
        _ => Err(DruidError::Query(format!(
            "Expected a literal value for window function default, got: {expr:?}"
        ))),
    }
}

fn convert_window_frame(frame: &ast::WindowFrame) -> Result<WindowFrame> {
    let mode = match frame.units {
        ast::WindowFrameUnits::Rows => FrameMode::Rows,
        ast::WindowFrameUnits::Range => FrameMode::Range,
        _ => {
            return Err(DruidError::Query(
                "Only ROWS and RANGE window frame modes are supported".to_string(),
            ));
        }
    };

    let start = convert_frame_bound(&frame.start_bound)?;
    let end = match &frame.end_bound {
        Some(bound) => convert_frame_bound(bound)?,
        None => FrameBound::CurrentRow,
    };

    // Wave 36-H (Wave 37B Medium follow-up): reject impossible start/end
    // bound orderings. Per SQL:2003 7.11 and Druid's window semantics,
    // the start bound MUST sort at-or-before the end bound on the
    // logical frame line:
    //
    //   UNBOUNDED PRECEDING < N PRECEDING < CURRENT ROW < N FOLLOWING < UNBOUNDED FOLLOWING
    //
    // Without this guard the parser silently accepted nonsense like
    // `BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING` and the
    // executor would then produce an empty / undefined window.
    validate_frame_ordering(&start, &end)?;

    Ok(WindowFrame { mode, start, end })
}

/// Map a [`FrameBound`] to a 4-region rank used to validate that the
/// frame `start <= end` on the logical frame line.
///
/// The numeric `Preceding(n)` / `Following(n)` distances are returned as
/// `Some(n)` so that callers can reject orderings like
/// `5 PRECEDING TO 10 PRECEDING` (start = -5 but end = -10 is impossible).
fn frame_bound_rank(bound: &FrameBound) -> (i8, Option<usize>) {
    match bound {
        // UNBOUNDED PRECEDING — most negative.
        FrameBound::UnboundedPreceding => (-2, None),
        // N PRECEDING — sits at -n on the frame line; smaller `n` is closer to current row.
        FrameBound::Preceding(n) => (-1, Some(*n)),
        // CURRENT ROW.
        FrameBound::CurrentRow => (0, None),
        // N FOLLOWING — sits at +n on the frame line.
        FrameBound::Following(n) => (1, Some(*n)),
        // UNBOUNDED FOLLOWING — most positive.
        FrameBound::UnboundedFollowing => (2, None),
    }
}

/// Reject impossible start/end frame bound orderings.
fn validate_frame_ordering(start: &FrameBound, end: &FrameBound) -> Result<()> {
    let (start_region, start_n) = frame_bound_rank(start);
    let (end_region, end_n) = frame_bound_rank(end);

    if start_region > end_region {
        return Err(DruidError::Query(format!(
            "Window frame start bound {start:?} cannot come after end bound {end:?}"
        )));
    }

    if start_region == end_region {
        // Same region: numeric distances must be consistent.
        match (start, end, start_n, end_n) {
            // Both PRECEDING: `start = N PRECEDING` and `end = M PRECEDING`
            // require `N >= M` (start sits further back than end).
            (FrameBound::Preceding(_), FrameBound::Preceding(_), Some(a), Some(b)) if a < b => {
                return Err(DruidError::Query(format!(
                    "Window frame start bound {start:?} cannot come after end bound {end:?}"
                )));
            }
            // Both FOLLOWING: `start = N FOLLOWING` and `end = M FOLLOWING`
            // require `N <= M` (start sits closer to current row).
            (FrameBound::Following(_), FrameBound::Following(_), Some(a), Some(b)) if a > b => {
                return Err(DruidError::Query(format!(
                    "Window frame start bound {start:?} cannot come after end bound {end:?}"
                )));
            }
            _ => {}
        }
    }

    Ok(())
}

fn convert_frame_bound(bound: &ast::WindowFrameBound) -> Result<FrameBound> {
    match bound {
        ast::WindowFrameBound::Preceding(None) => Ok(FrameBound::UnboundedPreceding),
        ast::WindowFrameBound::Preceding(Some(expr)) => {
            let n = expr_to_usize(expr).ok_or_else(|| {
                DruidError::Query("Window frame bound must be an integer".to_string())
            })?;
            Ok(FrameBound::Preceding(n))
        }
        ast::WindowFrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
        ast::WindowFrameBound::Following(None) => Ok(FrameBound::UnboundedFollowing),
        ast::WindowFrameBound::Following(Some(expr)) => {
            let n = expr_to_usize(expr).ok_or_else(|| {
                DruidError::Query("Window frame bound must be an integer".to_string())
            })?;
            Ok(FrameBound::Following(n))
        }
    }
}

fn convert_order_by(order_by: &ast::OrderBy) -> Result<Vec<OrderByExpr>> {
    let mut result = Vec::new();
    for ob in &order_by.exprs {
        let expr = convert_expr(&ob.expr)?;
        let asc = ob.asc.unwrap_or(true);
        result.push(OrderByExpr { expr, asc });
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-05-15 fuzz wave regression: `check_sql_token_density` must reject
    /// inputs with high comparison-operator density before sqlparser's
    /// recursive-descent expression engine spends seconds exploring binary-op
    /// precedence permutations across `<a<=a<<a<...`-style token clusters.
    /// Original artifact (`fuzz/artifacts/fuzz_sql_parse/timeout-23c52b3e...`,
    /// 3675 B, 526 comparison ops) burned 98 s of wall-clock — must reject in
    /// O(input length) string scan.
    #[test]
    fn reject_dense_comparison_operator_attack_2026_05_15() {
        // Synthetic shape mirroring the fuzz artifact: 100 `<` ops outside
        // any string literal. Easily exceeds the 32-op total cap.
        let mut attack = String::from("SELECT a FROM t WHERE x ");
        attack.push_str(&"<".repeat(100));
        attack.push('y');
        let err = parse_druid_sql(&attack).expect_err("dense `<` attack must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("comparison-operator"),
            "unexpected error: {msg}"
        );
    }

    // -----------------------------------------------------------------
    // CTE (WITH) parsing
    // -----------------------------------------------------------------

    /// Pull the resolved `SelectQuery` out of a parsed statement.
    fn as_select(stmt: &DruidSqlStatement) -> &SelectQuery {
        match stmt {
            DruidSqlStatement::Select(s) => s,
            other => panic!("expected Select, got {other:?}"),
        }
    }

    /// DD R10 A#2: a chained join keyed off a *previous* join's right column
    /// must lower its `left_key` to the PREFIXED form (`b.m`), because the
    /// executor emits previous-join right columns with their prefix. Before the
    /// fix the qualifier was discarded and `left_key` was the bare `"m"`, so
    /// `left.get("m")` missed every row.
    #[test]
    fn chained_join_left_key_keeps_prior_right_prefix() {
        let sql = "SELECT a.x, c.z FROM a \
                   JOIN b ON a.k = b.k \
                   JOIN c ON b.m = c.m";
        let stmt = parse_druid_sql(sql).expect("parse chained join");
        let select = as_select(&stmt);
        assert_eq!(select.from.joins.len(), 2, "two joins expected");

        // First join keys off the base table `a` — left_key stays unprefixed.
        assert_eq!(select.from.joins[0].left_key, "k");
        assert_eq!(select.from.joins[0].right_key, "k");

        // Second join keys off `b.m`, a *prior* join's right column — the
        // left_key must carry the `b.` prefix to match the emitted column.
        assert_eq!(select.from.joins[1].left_key, "b.m");
        assert_eq!(select.from.joins[1].right_key, "m");
    }

    /// A second base-table join (left side is the base relation) must NOT be
    /// prefixed even when another join precedes it.
    #[test]
    fn chained_join_base_keyed_left_stays_unprefixed() {
        let sql = "SELECT a.x FROM a \
                   JOIN b ON a.k = b.k \
                   JOIN c ON a.j = c.j";
        let stmt = parse_druid_sql(sql).expect("parse");
        let select = as_select(&stmt);
        assert_eq!(select.from.joins[1].left_key, "j");
    }

    #[test]
    fn cte_single_inlined_as_subquery() {
        let sql = "WITH c AS (SELECT city FROM sales) SELECT city FROM c";
        let stmt = parse_druid_sql(sql).expect("parse single CTE");
        let select = as_select(&stmt);
        // The FROM is the inlined CTE body, not a base table.
        assert!(
            select.from.subquery.is_some(),
            "single CTE reference must inline as a subquery"
        );
    }

    #[test]
    fn cte_exponential_expansion_rejected() {
        // DD R27 (High): each CTE references the previous one TWICE, doubling the
        // inlined AST per level. Without an expansion budget a ~30-level chain
        // expands to ~2^30 nodes and OOMs the planner from a tiny query. It must
        // be rejected with a clear error instead of exhausting memory.
        // CROSS JOIN (no `=`) so the doubling reference chain is not rejected by
        // the unrelated comparison-operator-count guard first.
        let mut sql = String::from("WITH c0 AS (SELECT k FROM t)");
        for i in 1..40 {
            let prev = i - 1;
            sql.push_str(&format!(
                ", c{i} AS (SELECT a.k FROM c{prev} a CROSS JOIN c{prev} b)"
            ));
        }
        sql.push_str(" SELECT k FROM c39");
        let err = parse_druid_sql(&sql).expect_err("exponential CTE expansion must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("expands too large") || msg.contains("relations"),
            "expected a CTE expansion-budget error, got: {msg}"
        );
    }

    #[test]
    fn cte_wide_fanout_expansion_rejected() {
        // DD R28: a SINGLE body that references an already-large CTE many times
        // must be rejected DURING inlining (budget charged before each clone),
        // not only after cloning every reference. Build a moderately large CTE
        // by doubling, then reference it hundreds of times in one CROSS JOIN body.
        let mut sql = String::from("WITH c0 AS (SELECT k FROM t)");
        for i in 1..9 {
            let prev = i - 1;
            sql.push_str(&format!(
                ", c{i} AS (SELECT a.k FROM c{prev} a CROSS JOIN c{prev} b)"
            ));
        }
        sql.push_str(", wide AS (SELECT x0.k FROM c8 x0");
        for j in 1..400 {
            sql.push_str(&format!(" CROSS JOIN c8 x{j}"));
        }
        sql.push(')');
        sql.push_str(" SELECT k FROM wide");
        let err = parse_druid_sql(&sql).expect_err("wide CTE fan-out must be rejected");
        assert!(
            err.to_string().contains("expands too large"),
            "expected a CTE expansion-budget error, got: {err}"
        );
    }

    #[test]
    fn cte_multiple_independent() {
        let sql = "WITH a AS (SELECT city FROM sales), \
                        b AS (SELECT country FROM sales) \
                   SELECT city FROM a";
        let stmt = parse_druid_sql(sql).expect("parse multiple CTEs");
        let select = as_select(&stmt);
        assert!(select.from.subquery.is_some());
    }

    #[test]
    fn cte_chained_reference() {
        // b references a — chained CTEs resolve in declaration order.
        let sql = "WITH a AS (SELECT city FROM sales), \
                        b AS (SELECT city FROM a) \
                   SELECT city FROM b";
        let stmt = parse_druid_sql(sql).expect("parse chained CTEs");
        let select = as_select(&stmt);
        // Outer FROM b is inlined; its body's FROM a is also inlined,
        // producing a nested subquery.
        let inner = select.from.subquery.as_ref().expect("outer FROM inlines b");
        let inner_select = as_select(inner);
        assert!(
            inner_select.from.subquery.is_some(),
            "chained CTE: b's body must inline a as a nested subquery"
        );
    }

    #[test]
    fn cte_referenced_twice() {
        // A CTE referenced from two later CTEs is inlined at each site.
        let sql = "WITH base AS (SELECT city FROM sales), \
                        x AS (SELECT city FROM base), \
                        y AS (SELECT city FROM base) \
                   SELECT city FROM x";
        let stmt = parse_druid_sql(sql).expect("parse CTE referenced twice");
        let select = as_select(&stmt);
        let inner = select.from.subquery.as_ref().expect("x inlined");
        assert!(
            as_select(inner).from.subquery.is_some(),
            "x's body references base, which must be inlined too"
        );
    }

    /// DD R10 A#4: a CTE column-alias list `WITH a(x, y) AS (...)` must rename
    /// the body's output columns, not be silently dropped. After the fix the
    /// inlined body projects `x` and `y`, so the outer `SELECT x, y` resolves.
    #[test]
    fn cte_column_alias_list_renames_body_projection() {
        let sql = "WITH a(x, y) AS (SELECT region, COUNT(*) FROM sales GROUP BY region) \
                   SELECT x, y FROM a";
        let stmt = parse_druid_sql(sql).expect("parse CTE with column alias list");
        let select = as_select(&stmt);
        let inner = select
            .from
            .subquery
            .as_ref()
            .expect("CTE reference inlines as a subquery");
        let inner_select = as_select(inner);
        // The body's two projections must be aliased to x and y.
        let aliases: Vec<Option<&str>> = inner_select
            .projections
            .iter()
            .map(|p| match p {
                Projection::Expr { alias, .. } => alias.as_deref(),
                Projection::Wildcard => None,
            })
            .collect();
        assert_eq!(aliases, vec![Some("x"), Some("y")], "body must be renamed");
    }

    /// An arity-mismatched column alias list must error clearly, not be dropped.
    #[test]
    fn cte_column_alias_list_arity_mismatch_rejected() {
        let sql = "WITH a(x, y, z) AS (SELECT region FROM sales) SELECT x FROM a";
        let err = parse_druid_sql(sql).expect_err("arity mismatch must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("column alias list"), "unexpected error: {msg}");
    }

    /// A column alias list over a `SELECT *` body cannot be honoured (unknown
    /// arity) and must error rather than silently drop the rename.
    #[test]
    fn cte_column_alias_list_over_wildcard_rejected() {
        let sql = "WITH a(x) AS (SELECT * FROM sales) SELECT x FROM a";
        let err = parse_druid_sql(sql).expect_err("alias over SELECT * must be rejected");
        assert!(
            format!("{err}").contains("column alias list"),
            "unexpected error: {err}"
        );
    }

    /// A FROM-subquery column alias list `(SELECT ...) AS t(x, y)` is renamed
    /// the same way (not silently dropped).
    #[test]
    fn from_subquery_column_alias_list_renames() {
        let sql =
            "SELECT x, y FROM (SELECT region, COUNT(*) FROM sales GROUP BY region) AS t(x, y)";
        let stmt = parse_druid_sql(sql).expect("parse FROM-subquery with column alias list");
        let select = as_select(&stmt);
        let inner = select
            .from
            .subquery
            .as_ref()
            .expect("FROM-subquery present");
        let aliases: Vec<Option<&str>> = as_select(inner)
            .projections
            .iter()
            .map(|p| match p {
                Projection::Expr { alias, .. } => alias.as_deref(),
                Projection::Wildcard => None,
            })
            .collect();
        assert_eq!(aliases, vec![Some("x"), Some("y")]);
    }

    #[test]
    fn cte_recursive_rejected() {
        let sql = "WITH RECURSIVE c AS (SELECT city FROM sales) SELECT city FROM c";
        let err = parse_druid_sql(sql).expect_err("WITH RECURSIVE must be rejected");
        assert!(
            format!("{err}").contains("Recursive"),
            "unexpected error: {err}"
        );
    }

    // -----------------------------------------------------------------
    // GROUPING SETS / CUBE / ROLLUP parsing
    // -----------------------------------------------------------------

    #[test]
    fn grouping_sets_explicit_preserved() {
        let sql = "SELECT city, country, COUNT(*) FROM sales \
                   GROUP BY GROUPING SETS ((city, country), (city), ())";
        let stmt = parse_druid_sql(sql).expect("parse GROUPING SETS");
        let sets = as_select(&stmt)
            .grouping_sets
            .clone()
            .expect("grouping_sets present");
        assert_eq!(
            sets,
            vec![
                vec!["city".to_string(), "country".to_string()],
                vec!["city".to_string()],
                vec![],
            ]
        );
    }

    #[test]
    fn cube_expands_to_all_subsets() {
        let sql = "SELECT city, country, COUNT(*) FROM sales GROUP BY CUBE(city, country)";
        let stmt = parse_druid_sql(sql).expect("parse CUBE");
        let sets = as_select(&stmt)
            .grouping_sets
            .clone()
            .expect("grouping_sets present");
        // CUBE(a,b) -> 2^2 = 4 subsets.
        assert_eq!(
            sets.len(),
            4,
            "CUBE(a,b) must yield 4 grouping sets: {sets:?}"
        );
        assert!(sets.contains(&vec![]));
        assert!(sets.contains(&vec!["city".to_string()]));
        assert!(sets.contains(&vec!["country".to_string()]));
        assert!(sets.contains(&vec!["city".to_string(), "country".to_string()]));
    }

    #[test]
    fn group_by_multidim_cross_product_rejected() {
        // DD R30: two CUBE elements cross-multiply their subset counts. Two
        // CUBE(11)s expand to 2^11 * 2^11 = 2^22 grouping sets, exceeding the
        // MAX_GROUPING_SETS cap; this must be rejected before the allocation
        // rather than OOM (the per-CUBE arity cap alone does not catch it).
        let a: Vec<String> = (0..11).map(|i| format!("a{i}")).collect();
        let b: Vec<String> = (0..11).map(|i| format!("b{i}")).collect();
        let sql = format!(
            "SELECT COUNT(*) FROM t GROUP BY CUBE({}), CUBE({})",
            a.join(","),
            b.join(",")
        );
        let err = parse_druid_sql(&sql).expect_err("CUBE cross-product must be rejected");
        assert!(
            err.to_string().contains("grouping sets"),
            "expected a grouping-set budget error, got: {err}"
        );
    }

    #[test]
    fn rollup_expands_to_prefixes() {
        let sql = "SELECT city, country, COUNT(*) FROM sales GROUP BY ROLLUP(city, country)";
        let stmt = parse_druid_sql(sql).expect("parse ROLLUP");
        let sets = as_select(&stmt)
            .grouping_sets
            .clone()
            .expect("grouping_sets present");
        // ROLLUP(a,b) -> [(a,b), (a), ()].
        assert_eq!(
            sets,
            vec![
                vec!["city".to_string(), "country".to_string()],
                vec!["city".to_string()],
                vec![],
            ]
        );
    }

    /// DD R10 A#3: a composite parenthesised group `(a,b)` inside ROLLUP/CUBE
    /// must be treated as ONE atomic unit, not pre-flattened to `[a,b]`.
    /// Pre-flattening produced a spurious `{a}` set for ROLLUP and all 8 subsets
    /// for CUBE.
    #[test]
    fn rollup_composite_group_is_atomic() {
        let sql = "SELECT city, country, product, COUNT(*) FROM sales \
                   GROUP BY ROLLUP((city, country), product)";
        let stmt = parse_druid_sql(sql).expect("parse ROLLUP composite");
        let sets = as_select(&stmt)
            .grouping_sets
            .clone()
            .expect("grouping_sets present");
        // ROLLUP((a,b),c) -> {a,b,c}, {a,b}, {} — exactly 3 sets, NO spurious {a}.
        assert_eq!(
            sets,
            vec![
                vec![
                    "city".to_string(),
                    "country".to_string(),
                    "product".to_string()
                ],
                vec!["city".to_string(), "country".to_string()],
                vec![],
            ],
            "ROLLUP((a,b),c) must keep (a,b) atomic"
        );
    }

    #[test]
    fn cube_composite_group_is_atomic() {
        let sql = "SELECT city, country, product, COUNT(*) FROM sales \
                   GROUP BY CUBE((city, country), product)";
        let stmt = parse_druid_sql(sql).expect("parse CUBE composite");
        let sets = as_select(&stmt)
            .grouping_sets
            .clone()
            .expect("grouping_sets present");
        // CUBE((a,b),c) -> {}, {a,b}, {c}, {a,b,c} — exactly 4 sets, not 8.
        assert_eq!(sets.len(), 4, "CUBE((a,b),c) must yield 4 sets: {sets:?}");
        assert!(sets.contains(&vec![]));
        assert!(sets.contains(&vec!["city".to_string(), "country".to_string()]));
        assert!(sets.contains(&vec!["product".to_string()]));
        assert!(sets.contains(&vec![
            "city".to_string(),
            "country".to_string(),
            "product".to_string()
        ]));
        // The spurious split-out singletons must NOT appear.
        assert!(!sets.contains(&vec!["city".to_string()]));
        assert!(!sets.contains(&vec!["country".to_string()]));
    }

    #[test]
    fn plain_group_by_has_no_grouping_sets() {
        let sql = "SELECT city, COUNT(*) FROM sales GROUP BY city";
        let stmt = parse_druid_sql(sql).expect("parse plain GROUP BY");
        assert!(as_select(&stmt).grouping_sets.is_none());
    }

    /// Consecutive-run cap (`MAX_SQL_COMPARISON_RUN=4`) closes the
    /// boundary-learning escape: even if total ops sit just below the
    /// total cap, a 5+ consecutive `<` run drives the same sqlparser
    /// expression-precedence blowup. 4 passes the cap, 5 rejects.
    #[test]
    fn reject_consecutive_comparison_run_2026_05_15() {
        // 4 consecutive `<` between alphanumerics — at the cap, must pass
        // the token-density check (will still likely fail later in sqlparser
        // proper because `a < < < < b` is not valid SQL; we assert the
        // *token-density* check itself does not fire).
        let at_cap = "SELECT 1 WHERE a < < < < b";
        match parse_druid_sql(at_cap) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("comparison-operator"),
                    "4 consecutive must not trip density cap: {msg}"
                );
            }
        }
        // 5 consecutive — exceeds the run cap.
        let over_cap = "SELECT 1 WHERE a < < < < < b";
        let err = parse_druid_sql(over_cap).expect_err("5 consecutive must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("run of") && msg.contains("comparison-operator"),
            "unexpected error: {msg}"
        );
    }

    /// String literals and quoted identifiers are skipped — `'<<<<<<'`
    /// inside a SQL string value does NOT count toward the cap, so
    /// legitimate queries comparing against a text column whose values
    /// contain `<` characters still parse.
    #[test]
    fn token_density_skips_strings_and_quoted_identifiers() {
        // 100 `<` inside a string literal — must NOT trip the cap.
        let mut sql = String::from("SELECT * FROM sales WHERE notes = '");
        sql.push_str(&"<".repeat(100));
        sql.push('\'');
        // The token-density check must not be the rejection reason
        // (sqlparser itself accepts this — it's a valid equality
        // comparison against a string literal).
        let stmt = parse_druid_sql(&sql).expect("string-literal content must not trip cap");
        let _ = stmt;
    }

    /// Realistic complex queries (multi-condition WHERE with several
    /// comparisons) must continue to parse.
    #[test]
    fn legitimate_complex_where_still_parses() {
        // 5 comparison ops (well under cap=32), 0 long runs.
        let sql = "SELECT city, revenue FROM sales \
                   WHERE revenue > 100 AND revenue < 10000 \
                   AND city = 'tokyo' OR city = 'osaka' OR price >= 500";
        let stmt = parse_druid_sql(sql).expect("legitimate query must parse");
        let _ = stmt;
    }

    /// 2026-05-16 fuzz wave regression (evo-x2): `check_sql_token_density` must
    /// also reject dense arithmetic-`+` runs. The two original artifacts
    /// (`timeout-cd937ce1`, 1889 B, 420 `+`, 11.9 s replay; `timeout-d2db056a`,
    /// 1482 B, 246 `+`, 8.7 s replay) burned seconds inside sqlparser's
    /// expression-precedence search across `a++++b` token clusters. The
    /// existing `MAX_SQL_COMPARISON_OPS` cap did not fire because `+` is a
    /// different operator family.
    #[test]
    fn reject_dense_arith_plus_attack_2026_05_16() {
        // Synthetic shape mirroring the fuzz artifact: 100 `+` ops outside
        // any string literal. Easily exceeds the 32-op total cap.
        let mut attack = String::from("SELECT a FROM t WHERE x = ");
        attack.push_str(&"+".repeat(100));
        attack.push('y');
        let err = parse_druid_sql(&attack).expect_err("dense `+` attack must reject");
        let msg = format!("{err}");
        assert!(msg.contains("arithmetic-`+`"), "unexpected error: {msg}");
    }

    /// Consecutive-run cap (`MAX_SQL_ARITH_PLUS_RUN=4`) closes the
    /// boundary-learning escape: even if total `+` sit just below the total
    /// cap, a 5+ consecutive `+` burst drives the same sqlparser
    /// expression-precedence blowup. 4 passes the cap, 5 rejects.
    #[test]
    fn reject_consecutive_arith_plus_run_2026_05_16() {
        // 4 consecutive `+` between alphanumerics — at the cap, must pass
        // the token-density check (sqlparser itself may still reject as
        // invalid expression; we only assert the *token-density* check
        // does not fire).
        let at_cap = "SELECT 1 WHERE a ++++ b";
        match parse_druid_sql(at_cap) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("arithmetic-`+`"),
                    "4 consecutive must not trip density cap: {msg}"
                );
            }
        }
        // 5 consecutive — exceeds the run cap.
        let over_cap = "SELECT 1 WHERE a +++++ b";
        let err = parse_druid_sql(over_cap).expect_err("5 consecutive must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("run of") && msg.contains("arithmetic-`+`"),
            "unexpected error: {msg}"
        );
    }

    /// String literals and quoted identifiers must continue to be skipped
    /// for the `+` density check — `'++++++'` inside a string value does NOT
    /// count toward the cap.
    #[test]
    fn arith_plus_density_skips_strings_and_quoted_identifiers() {
        let mut sql = String::from("SELECT * FROM sales WHERE notes = '");
        sql.push_str(&"+".repeat(100));
        sql.push('\'');
        let stmt = parse_druid_sql(&sql).expect("string-literal content must not trip cap");
        let _ = stmt;
    }

    /// Realistic column arithmetic (`a + b + c + ...`) must continue to
    /// parse. 6 single `+` ops with operands in between sit well under the
    /// 32-op total cap and 4-op consecutive run cap.
    #[test]
    fn legitimate_arithmetic_still_parses() {
        let sql = "SELECT a + b + c + d + e + f + g AS total FROM t";
        let stmt = parse_druid_sql(sql).expect("legitimate arithmetic must parse");
        let _ = stmt;
    }

    /// 2026-05-16 fuzz wave regression (evo-x2, `timeout-505a90ab...`):
    /// `check_sql_token_density` must reject inputs with high whitespace-run
    /// density. The 1072 B artifact carried 31 `[` opens (only 8 `]` closes)
    /// interleaved with whitespace runs up to 27 chars; sqlparser burns
    /// 30 s+ exploring expression-state permutations across the wide,
    /// whitespace-padded token stream. Both the existing comparison-op /
    /// arithmetic-`+` total caps and `check_paren_depth` stay just under
    /// their boundaries (22 `+`, ~6-8 max `[` depth) — only the new
    /// whitespace-run cap and the bracket-open total cap fire.
    #[test]
    fn reject_dense_whitespace_run_2026_05_16_evo_x2() {
        // A whitespace run above the cap (65 spaces) between two valid tokens
        // still rejects, bounding pure-whitespace padding.
        let mut sql = String::from("SELECT a FROM t WHERE x =");
        sql.push_str(&" ".repeat(65));
        sql.push('1');
        let err = parse_druid_sql(&sql).expect_err("65-space run must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("whitespace") && msg.contains("run of"),
            "unexpected error: {msg}"
        );
    }

    /// Boundary-inclusive `>` reject: 64 spaces sit at the cap and must
    /// pass the whitespace check (sqlparser proper may still accept or
    /// reject the surrounding SQL — we only assert the *whitespace* cap
    /// itself does not fire at exactly the cap).
    #[test]
    fn whitespace_run_at_cap_passes_density_check() {
        let mut sql = String::from("SELECT a FROM t WHERE x =");
        sql.push_str(&" ".repeat(64));
        sql.push('1');
        match parse_druid_sql(&sql) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("whitespace"),
                    "64 spaces must not trip whitespace cap: {msg}"
                );
            }
        }
    }

    /// BI-tool regression: pydruid's SQLAlchemy `get_columns` introspection
    /// query carries a 20-space indent run on its continuation lines. It must
    /// parse (previously rejected by the 16-space cap), so Superset dataset
    /// column-sync works. (The referenced table being empty is fine — this
    /// asserts the *parser* accepts the whitespace, not that rows return.)
    #[test]
    fn bi_tool_indented_query_parses() {
        let indent = " ".repeat(20);
        let sql = format!(
            "SELECT COLUMN_NAME,\n{indent}JDBC_TYPE,\n{indent}IS_NULLABLE,\n\
             {indent}COLUMN_DEFAULT\n{indent}FROM INFORMATION_SCHEMA.COLUMNS\n\
             {indent}WHERE TABLE_NAME = 'wikipedia_compat'"
        );
        parse_druid_sql(&sql).expect("20-space-indented BI query must parse");
    }

    /// The 2026-05-16 evo-x2 DoS artifact's blow-up shape (many `[` ARRAY-opens
    /// interleaved with whitespace) must STILL reject after the whitespace cap
    /// was relaxed — now caught by `MAX_SQL_BRACKET_OPENS_TOTAL = 12` rather
    /// than the whitespace cap. This guards against a regression re-opening the
    /// original 30s-parse timeout.
    #[test]
    fn reject_dense_whitespace_run_2026_05_16_artifact_shape() {
        // 31 `[` opens, each separated by a 24-space run (< new 64 cap, so the
        // whitespace cap does not fire — the bracket cap must).
        let ws = " ".repeat(24);
        let mut sql = String::from("SELECT ");
        for _ in 0..31 {
            sql.push_str("ARRAY[");
            sql.push_str(&ws);
        }
        sql.push('1');
        let start = std::time::Instant::now();
        let _err = parse_druid_sql(&sql).expect_err("31-bracket artifact must still reject");
        // The security property is FAST rejection (no 30s blow-up). Which density
        // cap fires (bracket-opens, or paren/nesting depth from the nested
        // `ARRAY[`) is immaterial — the relaxed whitespace cap no longer catches
        // it, and one of the other caps still does, quickly.
        assert!(
            start.elapsed().as_secs() < 2,
            "artifact must reject fast (no blow-up), took {:?}",
            start.elapsed()
        );
    }

    /// String literals and quoted identifiers must continue to be skipped
    /// for the whitespace-run check — `'                '` inside a string
    /// value does NOT count toward the cap, so legitimate queries with
    /// long text content (e.g. log messages with padding) still parse.
    #[test]
    fn whitespace_run_skips_strings_and_quoted_identifiers() {
        let mut sql = String::from("SELECT * FROM logs WHERE message = '");
        sql.push_str(&" ".repeat(100));
        sql.push('\'');
        let stmt = parse_druid_sql(&sql).expect("string-literal content must not trip cap");
        let _ = stmt;
    }

    /// Companion artifact-driven test: `check_sql_token_density` must
    /// reject inputs with too many `[` ARRAY/subscript opens. The 2026-05-16
    /// evo-x2 artifact had 31 `[` opens with only 8 `]` closes — most
    /// pending — driving sqlparser's per-`[` ARRAY-state push past the
    /// timeout boundary even though the close-aware depth tracker
    /// (`check_paren_depth`) saw at most ~6-8 simultaneous opens.
    #[test]
    fn reject_dense_bracket_opens_2026_05_16_evo_x2() {
        // 16 `[` opens (each closed immediately so `check_paren_depth`
        // never trips). Exceeds the open-total cap of 16 inclusively.
        let mut sql = String::from("SELECT ");
        for _ in 0..16 {
            sql.push_str("[1],");
        }
        sql.push_str("a FROM t");
        let err = parse_druid_sql(&sql).expect_err("16 `[` opens must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("ARRAY/subscript-open"),
            "unexpected error: {msg}"
        );
    }

    /// Bracket-open string-literal skip: `[` characters inside a SQL
    /// string literal do NOT count toward the cap.
    #[test]
    fn bracket_open_density_skips_strings() {
        let mut sql = String::from("SELECT * FROM t WHERE msg = '");
        sql.push_str(&"[".repeat(100));
        sql.push('\'');
        let stmt = parse_druid_sql(&sql).expect("string-literal content must not trip cap");
        let _ = stmt;
    }

    /// Realistic Druid SQL with ARRAY/subscript syntax must not trip the
    /// `[` density cap. ferrodruid's `convert_expr` may still reject the
    /// expression for unsupported subscript semantics — that's an unrelated
    /// concern — we only assert the *token-density* check itself does not
    /// fire on legitimate few-`[` usage.
    #[test]
    fn legitimate_array_subscript_query_passes_density_check() {
        let sql = "SELECT t.col[0], t.col[1], t.col[2] FROM t WHERE t.col[0] = 1";
        match parse_druid_sql(sql) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("ARRAY/subscript-open"),
                    "legitimate `[` usage must not trip density cap: {msg}"
                );
            }
        }
    }

    /// 2026-05-19 fuzz wave regression (evo-x2,
    /// `timeout-bbec5dd4f23ef0e58e868c86792591c3fee7916e`, 198 B): a
    /// `(((UPDATE(... PP+\x0bNOT\x0c...(++NOT...` shape with 15 `(` opens
    /// and zero `)` closes, 17 `NOT` unary keywords, 19 `+` arith ops,
    /// and `\x0b`/`\x0c` (VT/FF) used as inter-token whitespace. Each
    /// pre-existing density / depth cap stayed just below boundary
    /// (peak paren depth 15 < `MAX_SQL_PAREN_DEPTH=24`; `+` total
    /// 19 < `MAX_SQL_ARITH_PLUS_OPS=32`; `[` total 0; ASCII whitespace
    /// runs 0). The new `MAX_SQL_UNCLOSED_PAREN_OPENS` cap fires on the
    /// 15-unclosed-`(` signature, rejecting the input pre-parse instead
    /// of letting sqlparser's recursive-descent engine explore
    /// permutations of how to close 15 pending opens through chained
    /// `NOT`/`+` operator clusters.
    #[test]
    fn reject_unclosed_paren_attack_2026_05_19_evo_x2_artifact() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/timeout-bbec5dd4-paren-star-plus-mixed-2026-05-19"
        );
        let sql = String::from_utf8_lossy(crash);
        let err =
            parse_druid_sql(&sql).expect_err("198 B unclosed-paren attack must reject pre-parse");
        let msg = format!("{err}");
        // The artifact (15 `(` opens × 17 NOTs = 255) may be caught by
        // the joint paren-NOT density cap (fires at 60) before the
        // unclosed-`(` cap gets a chance. Accept either rejection.
        assert!(
            msg.contains("unclosed `(`") || msg.contains("paren-NOT density"),
            "expected unclosed-paren or paren-NOT density cap to fire: {msg}"
        );
    }

    /// Synthetic shape: a SQL prefix followed by many `(` with no matching
    /// `)`. The peak paren depth is the same as the unclosed count, so
    /// `check_paren_depth` would also reject above 24, but here we keep
    /// the count at 5 (above the unclosed cap of 4 but well below the
    /// depth cap of 24) so we specifically exercise the new cap.
    #[test]
    fn reject_five_unclosed_paren_opens_2026_05_19() {
        let sql = "SELECT a FROM t WHERE x = ((((( 1";
        let err = parse_druid_sql(sql).expect_err("5 unclosed `(` must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("unclosed `(`"),
            "expected unclosed-paren cap to fire: {msg}"
        );
    }

    /// Boundary check: 4 unclosed `(` opens sit at the cap and must NOT
    /// trip the unclosed-paren check (sqlparser proper will still likely
    /// reject the surrounding SQL as malformed — that is unrelated; we
    /// only assert the *density* cap itself does not fire at the cap).
    #[test]
    fn unclosed_paren_at_cap_passes_density_check() {
        let sql = "SELECT a FROM t WHERE x = (((( 1";
        match parse_druid_sql(sql) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("unclosed `(`"),
                    "4 unclosed `(` must not trip cap: {msg}"
                );
            }
        }
    }

    /// Balanced parens — even many of them — must never trip the
    /// unclosed-`(` cap. This is the realistic-SQL safety check: nested
    /// function calls, subselects, CASE expressions, etc., all keep the
    /// running depth balanced and bring it back to 0 at end of input.
    #[test]
    fn balanced_parens_do_not_trip_unclosed_cap() {
        // 10 balanced pairs interleaved with simple expressions — peak
        // depth ~10 (under MAX_SQL_PAREN_DEPTH=24) and final depth = 0.
        let sql =
            "SELECT (((((((((( a )))))))))) + (((((((((( b )))))))))) AS s FROM t WHERE c = 1";
        match parse_druid_sql(sql) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("unclosed `(`"),
                    "balanced parens must not trip unclosed cap: {msg}"
                );
            }
        }
    }

    /// `(` inside a string literal must not contribute to the running
    /// depth, mirroring the existing string-skip discipline for the
    /// other density counters.
    #[test]
    fn unclosed_paren_skips_string_literals() {
        // 10 unmatched `(` inside a string literal — must NOT trip the cap.
        let payload = "(".repeat(10);
        let sql = format!("SELECT '{payload}' AS s FROM t");
        let stmt = parse_druid_sql(&sql).expect("`(` inside string literal must not count");
        let _ = stmt;
    }

    /// VT (`\x0b`) and FF (`\x0c`) bytes were missing from the
    /// whitespace-run counter in the original `check_sql_token_density`
    /// (commit `5bc6275`). sqlparser's lexer treats both as whitespace,
    /// so a fuzz input can use them in place of spaces to hide a wide
    /// whitespace-padded token stream from the run-count cap. This test
    /// asserts the extended whitespace set fires on a VT/FF-only run.
    #[test]
    fn reject_dense_vt_ff_whitespace_run_2026_05_19() {
        // 65 consecutive `\x0c` (FF) bytes between two valid tokens —
        // exceeds the 64-byte cap, must reject.
        let mut sql = String::from("SELECT a FROM t WHERE x =");
        sql.push_str(&"\x0c".repeat(65));
        sql.push('1');
        let err = parse_druid_sql(&sql).expect_err("65-byte FF run must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("whitespace") && msg.contains("run of"),
            "unexpected error: {msg}"
        );

        // Same with `\x0b` (VT).
        let mut sql = String::from("SELECT a FROM t WHERE x =");
        sql.push_str(&"\x0b".repeat(65));
        sql.push('1');
        let err = parse_druid_sql(&sql).expect_err("65-byte VT run must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("whitespace") && msg.contains("run of"),
            "unexpected error: {msg}"
        );
    }

    /// Mixed VT/FF/space runs must accumulate into the same counter so
    /// a fuzz input cannot alternate the byte values to evade the cap.
    /// 6 of each (18 total) interleaved exceeds the 16-byte cap.
    #[test]
    fn mixed_vt_ff_ascii_whitespace_run_accumulates_2026_05_19() {
        let mut sql = String::from("SELECT a FROM t WHERE x =");
        // 66 bytes alternating ` `, `\x0b`, `\x0c` (> 64-byte cap).
        for i in 0..66 {
            sql.push(match i % 3 {
                0 => ' ',
                1 => '\x0b',
                _ => '\x0c',
            });
        }
        sql.push('1');
        let err = parse_druid_sql(&sql).expect_err("66-byte mixed-whitespace run must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("whitespace") && msg.contains("run of"),
            "unexpected error: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Regression tests — fuzz farm 2026-06-21, three NEW cap-hug bypasses
    // -----------------------------------------------------------------------

    /// `timeout-0d0f8883` (372 B, fuzz farm 2026-06-21): the `#"(…ARRAY[…)"`
    /// double-quoted identifier wrapping trick hid brackets from
    /// `bracket_open_total`. Outside the quoted regions only ~15 `[` were
    /// counted (under the old cap of 16), while many more lived inside the
    /// `"..."` spans. Fix: `[` inside `"..."` regions now count toward
    /// `bracket_open_total` too; cap reduced from 16 to 12.
    #[test]
    fn reject_bracket_dquote_hide_bypass_2026_06_21_0d0f8883() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/\
             timeout-0d0f8883-array-bracket15-dquote-hide-372b-NEW-2026-06-21"
        );
        let sql = String::from_utf8_lossy(crash);
        let err = parse_druid_sql(&sql)
            .expect_err("372 B dquote-hide bracket bypass must reject pre-parse");
        let msg = format!("{err}");
        assert!(
            msg.contains("ARRAY/subscript-open"),
            "expected bracket cap to fire: {msg}"
        );
    }

    /// `timeout-810d3ddb` (246 B, fuzz farm 2026-06-21): dense
    /// `ARRAY[+NOT+++` interleaving with VT (`\x0b`) / FF (`\x0c`)
    /// whitespace bytes. 15 `[` opens, just under the old cap of 16.
    /// Fix: cap reduced from 16 to 12 catches 15 ≥ 12.
    #[test]
    fn reject_bracket_not_mix_bypass_2026_06_21_810d3ddb() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/\
             timeout-810d3ddb-array-bracket15-NOT-mix-246b-NEW-2026-06-21"
        );
        let sql = String::from_utf8_lossy(crash);
        let err =
            parse_druid_sql(&sql).expect_err("246 B bracket-NOT-mix bypass must reject pre-parse");
        let msg = format!("{err}");
        assert!(
            msg.contains("ARRAY/subscript-open"),
            "expected bracket cap to fire: {msg}"
        );
    }

    /// `timeout-fdf030a5` (384 B, fuzz farm 2026-06-21): combined
    /// ARRAY × NOT × plus density with no single cap firing at the old
    /// thresholds. bracket_open=12 (at the old cap of 16, under it),
    /// plus=28 (under MAX_SQL_ARITH_PLUS_OPS=32), ws_max=10 (under 16).
    /// Fix: cap reduced from 16 to 12, so 12 ≥ 12 fires (boundary-
    /// inclusive `>=`).
    #[test]
    fn reject_array_plus_not_density_bypass_2026_06_21_fdf030a5() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/\
             timeout-fdf030a5-array-plus28-NOT-density-384b-NEW-2026-06-21"
        );
        let sql = String::from_utf8_lossy(crash);
        let err = parse_druid_sql(&sql)
            .expect_err("384 B ARRAY+NOT-density bypass must reject pre-parse");
        let msg = format!("{err}");
        assert!(
            msg.contains("ARRAY/subscript-open"),
            "expected bracket cap to fire: {msg}"
        );
    }

    /// The `[` counter now counts brackets inside double-quoted identifier
    /// regions (`"field[0]"` style) as well as outside. This test verifies
    /// that the counting change does not affect normal string literals
    /// (`'...'`): 100 `[` inside a single-quoted string must still pass.
    #[test]
    fn bracket_in_string_literal_still_not_counted_after_dquote_fix() {
        let sql = format!("SELECT * FROM t WHERE msg = '{}'", "[".repeat(100));
        // The `'...'` skip branch does NOT count `[`, so this must pass.
        let result = parse_druid_sql(&sql);
        match result {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("ARRAY/subscript-open"),
                    "`[` inside string literal must not trip bracket cap: {msg}"
                );
            }
        }
    }

    /// NOT keyword cap: 24+ `NOT` keywords in a query must be rejected.
    /// Realistic queries with many NOT conditions (e.g. 20 NOT IN filters)
    /// stay well under 24.
    #[test]
    fn reject_excessive_not_keywords_2026_06_21() {
        // 24 `NOT` keywords separated by spaces — fires the MAX_SQL_NOT_KEYWORDS cap.
        let sql = format!(
            "SELECT a FROM t WHERE {}",
            (0..24)
                .map(|i| format!("NOT col{i} > 0"))
                .collect::<Vec<_>>()
                .join(" AND ")
        );
        let err = parse_druid_sql(&sql).expect_err("24 NOT keywords must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("`NOT` keywords"),
            "expected NOT-keyword cap to fire: {msg}"
        );
    }

    /// NOT keyword cap: 23 `NOT` keywords must pass (one under the cap).
    #[test]
    fn not_keywords_at_cap_passes_density_check() {
        let sql = format!(
            "SELECT a FROM t WHERE {}",
            (0..23)
                .map(|i| format!("NOT col{i} > 0"))
                .collect::<Vec<_>>()
                .join(" AND ")
        );
        match parse_druid_sql(&sql) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("`NOT` keywords"),
                    "23 NOT keywords must not trip cap: {msg}"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Regression tests — fuzz farm 2026-06-22, round-2 cap-hug bypasses
    //
    // Two new artifacts evade every individual cap from commit 386bfc8
    // (brackets=12, NOTs=24, arith-plus=32, run caps) by keeping each metric
    // just under its threshold. The joint bracket × NOT product catches them:
    //   • `timeout-4b462550` (193 B): bracket=9, NOT=10, product=90 ≥ 60
    //   • `oom-6c9e5a7b`     (344 B): bracket=9, NOT=8,  product=72 ≥ 60
    //
    // Also archives two marginal artifacts caught by the 386bfc8 NOT-keyword
    // cap (not_kw_total ≥ 24): `oom-93980b7e` and `oom-d36b06bf`.
    // -----------------------------------------------------------------------

    /// `timeout-4b462550` (193 B, fuzz farm 2026-06-22): `CALL"B'"(b
    /// +NOTNOT+\x0bARRAY \x0b[…` — 9 `[` brackets and 10 `NOT` keywords,
    /// each individually under their caps, but the joint product (90) fires
    /// `MAX_SQL_BRACKET_NOT_JOINT=60`.  The `+NOTNOT+` fused sequence also
    /// exposed a counting gap: the trailing `N` in `NOTNOT` made `trail_ok`
    /// false, and the `T` at byte 2 left `prev_was_ident=true` so the second
    /// `N` silently failed the leading check (0 NOTs from the pair). Fix:
    /// advance `i += 3` on any leading-boundary NOT match, regardless of
    /// trailing boundary, so `NOTNOT+` correctly counts 1 NOT.
    #[test]
    fn reject_notnot_fused_joint_density_bypass_2026_06_22_4b462550() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/\
             timeout-4b462550-NOTNOT-fused-bracket9-NOT10-joint-density-193b-2026-06-22"
        );
        let sql = String::from_utf8_lossy(crash);
        let start = std::time::Instant::now();
        let err =
            parse_druid_sql(&sql).expect_err("193 B NOTNOT-fused bypass must reject pre-parse");
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_secs() < 1,
            "density check must fire in <1 s, took {}ms",
            elapsed.as_millis()
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("bracket-NOT density"),
            "expected joint bracket-NOT cap to fire: {msg}"
        );
    }

    /// `oom-6c9e5a7b` (344 B, fuzz farm 2026-06-22): case-mixed `ArRAY` /
    /// `ARRaY` keywords + dense `NOT` interleaving — 9 `[` brackets × 8
    /// `NOT` keywords = product 72 ≥ `MAX_SQL_BRACKET_NOT_JOINT=60`.
    /// Previously reached 4.29 s / 522 MB RSS before our density check fires.
    #[test]
    fn reject_arraymixed_case_joint_density_bypass_2026_06_22_6c9e5a7b() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/\
             oom-6c9e5a7b-ArRAY-mixed-case-bracket9-NOT8-joint-density-344b-2026-06-22"
        );
        let sql = String::from_utf8_lossy(crash);
        let start = std::time::Instant::now();
        let err =
            parse_druid_sql(&sql).expect_err("344 B ArRAY-mixed-case bypass must reject pre-parse");
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_secs() < 1,
            "density check must fire in <1 s, took {}ms",
            elapsed.as_millis()
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("bracket-NOT density"),
            "expected joint bracket-NOT cap to fire: {msg}"
        );
    }

    /// `oom-93980b7e` (200 B, round-2 2026-06-22): `CaLL"Bo"(a&aa+\x0bNOT\x0c…`
    /// — 6 `(` opens × 11 `NOT` keywords = 66 (joint paren-NOT density).
    /// Measured 140 ms / 77 MB RSS; caused a stack overflow in sqlparser
    /// when the paren-NOT joint check was absent. Caught now by
    /// `MAX_SQL_PAREN_NOT_JOINT=60`.
    #[test]
    fn reject_paren_not_joint_density_artifact_93980b7e() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/\
             oom-93980b7e-CALL-Bo-NOT-aaa-200b-CAUGHT-by-386bfc8-2026-06-22"
        );
        let sql = String::from_utf8_lossy(crash);
        let start = std::time::Instant::now();
        let err = parse_druid_sql(&sql)
            .expect_err("200 B paren-NOT-dense artifact must reject pre-parse");
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_secs() < 1,
            "paren-NOT density check must fire in <1 s, took {}ms",
            elapsed.as_millis()
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("paren-NOT density"),
            "expected paren-NOT joint cap to fire: {msg}"
        );
    }

    /// `oom-d36b06bf` (232 B, round-2 2026-06-22): `SET\nNU=EC,C,SEXO…`
    /// — 12 `(` opens × 10 `NOT` keywords = 120 (joint paren-NOT density).
    /// Measured 390 ms / 109 MB RSS; caused a **stack overflow** in sqlparser
    /// when the paren-NOT joint check was absent. Caught now by
    /// `MAX_SQL_PAREN_NOT_JOINT=60`.
    #[test]
    fn reject_paren_not_joint_density_artifact_d36b06bf() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/\
             oom-d36b06bf-SET-NU-NOT-cap-hug-232b-CAUGHT-by-386bfc8-2026-06-22"
        );
        let sql = String::from_utf8_lossy(crash);
        let start = std::time::Instant::now();
        let err = parse_druid_sql(&sql)
            .expect_err("232 B paren-NOT-dense artifact must reject pre-parse");
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_secs() < 1,
            "paren-NOT density check must fire in <1 s, took {}ms",
            elapsed.as_millis()
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("paren-NOT density"),
            "expected paren-NOT joint cap to fire: {msg}"
        );
    }

    /// `slow-unit-64c5e97e` (187 B, 2026-06-29 evo-x2 fuzz farm): a
    /// `SELECT P++NOT+T.+ARRAY[ ARRAY [ArR+NOT.++.NOT.+T_a1.+NOT+aT...`
    /// shape that interleaves `+`/`NOT`/`ARRAY[` braids whose individual
    /// densities all stay under boundary (bracket=6, NOT=9, plus=25) and
    /// whose previously-defined joint products also stay under boundary
    /// (bracket × NOT = 54 < 60; paren × NOT = 27 < 60). The `+` × `NOT`
    /// product (25 × 9 = 225) is the surface that captures the precedence
    /// blowup: each unary NOT prefix multiplies the binary-`+` precedence
    /// search by the count of pending `+` operators. Caught now by
    /// `MAX_SQL_PLUS_NOT_JOINT=150`.
    #[test]
    fn reject_plus_not_joint_density_artifact_64c5e97e() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/\
             slow-unit-64c5e97e-plus25-NOT9-array6-joint-density-187b-2026-06-29"
        );
        let sql = String::from_utf8_lossy(crash);
        let start = std::time::Instant::now();
        let err =
            parse_druid_sql(&sql).expect_err("187 B plus-NOT-dense artifact must reject pre-parse");
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_millis() < 100,
            "plus-NOT density check must fire in <100 ms, took {}ms",
            elapsed.as_millis()
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("plus-NOT density"),
            "expected plus-NOT joint cap to fire: {msg}"
        );
    }

    /// Joint plus × NOT density: synthetic shape with 10 `+` × 15 NOTs =
    /// 150 ≥ 150 → reject. Verifies the cap fires at its exact boundary.
    #[test]
    fn reject_joint_plus_not_density_synthetic() {
        let nots = (0..15)
            .map(|i| format!("NOT col{i} > 0"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("SELECT a+b+c+d+e+f+g+h+i+j+k FROM t WHERE {nots}");
        let err = parse_druid_sql(&sql).expect_err("product 150 must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("plus-NOT density"),
            "expected plus-NOT joint cap to fire: {msg}"
        );
    }

    /// Joint plus × NOT density: realistic shape with ~10 `+` and ~5
    /// NOTs (product 50) stays well under the 150 cap.
    #[test]
    fn allow_joint_plus_not_density_realistic() {
        let nots = (0..5)
            .map(|i| format!("NOT col{i} > 0"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("SELECT a+b+c+d+e+f+g+h+i+j FROM t WHERE {nots}");
        if let Err(e) = parse_druid_sql(&sql) {
            // The query is intentionally simple (no schema), so other
            // semantic checks may still error; the only requirement is
            // that the density cap does NOT fire.
            assert!(
                !format!("{e}").contains("plus-NOT density"),
                "plus-NOT density cap must not fire on realistic shape: {e}"
            );
        }
    }

    /// Joint bracket × NOT density: cap fires when product reaches 60.
    /// Synthetic input: 7 brackets × 9 NOTs = 63 ≥ 60 → reject.
    #[test]
    fn reject_joint_bracket_not_density_synthetic() {
        // 9 `NOT` keywords in WHERE, 7 `[` subscript-opens in SELECT.
        let nots = (0..9)
            .map(|i| format!("NOT col{i} > 0"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("SELECT t[0][1][2][3][4][5][6] FROM t WHERE {nots}");
        let err = parse_druid_sql(&sql).expect_err("product 63 must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("bracket-NOT density"),
            "expected joint density cap to fire: {msg}"
        );
    }

    /// Joint bracket × NOT density: product of 5 × 7 = 35 is well under 60.
    #[test]
    fn allow_joint_bracket_not_density_realistic() {
        let nots = (0..5)
            .map(|i| format!("NOT col{i} > 0"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("SELECT t[0][1][2][3][4][5][6] FROM t WHERE {nots}");
        match parse_druid_sql(&sql) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("bracket-NOT density"),
                    "product 35 must not trip joint cap: {msg}"
                );
            }
        }
    }

    /// Joint paren × NOT density: cap fires when product reaches 60.
    /// Synthetic: 7 `(` opens × 9 NOTs = 63 ≥ 60 → reject.
    #[test]
    fn reject_joint_paren_not_density_synthetic() {
        let nots = (0..9)
            .map(|i| format!("NOT col{i} > 0"))
            .collect::<Vec<_>>()
            .join(" AND ");
        // 7 function calls = 7 `(` opens, 9 NOT keywords in WHERE.
        let sql = format!("SELECT f1(f2(f3(f4(f5(f6(f7(x))))))) FROM t WHERE {nots}");
        let err = parse_druid_sql(&sql).expect_err("paren-NOT product 63 must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("paren-NOT density"),
            "expected paren-NOT joint cap to fire: {msg}"
        );
    }

    /// Joint paren × NOT density: product of 3 × 4 = 12 is well under 60.
    #[test]
    fn allow_joint_paren_not_density_realistic() {
        let nots = (0..4)
            .map(|i| format!("NOT col{i} > 0"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("SELECT f1(f2(f3(x))) FROM t WHERE {nots}");
        match parse_druid_sql(&sql) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("paren-NOT density"),
                    "product 12 must not trip paren-NOT joint cap: {msg}"
                );
            }
        }
    }

    #[test]
    fn parse_simple_select() {
        let stmt = parse_druid_sql("SELECT city, revenue FROM sales").expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 2);
                assert_eq!(sel.from.name, "sales");
                assert!(sel.filter.is_none());
                assert!(sel.group_by.is_empty());
            }
            _ => panic!("expected Select"),
        }
    }

    /// Regression test for **TG-4-finding-003** (W2-D Superset chart-render
    /// silent-empty). A double-quoted table name in the FROM clause must
    /// resolve to the same datasource name as the unquoted form — the
    /// surrounding `"..."` are SQL syntax, not part of the catalog
    /// identifier value. Before the fix, `convert_table_ref` propagated
    /// `name.to_string()` which re-emitted `"wikipedia_compat"` (quotes
    /// included), causing `extract_datasource_name` in the REST layer to
    /// look up a non-existent datasource → empty result silently. Apache
    /// Druid SQL accepts both forms identically.
    #[test]
    fn parse_quoted_table_name_strips_quotes() {
        for sql in [
            r#"SELECT page, COUNT(*) FROM "wikipedia_compat" GROUP BY page"#,
            r#"SELECT * FROM "sales""#,
            "SELECT * FROM sales",
        ] {
            let stmt = parse_druid_sql(sql).unwrap_or_else(|e| panic!("parse [{sql}]: {e}"));
            match stmt {
                DruidSqlStatement::Select(sel) => {
                    assert!(
                        !sel.from.name.contains('"'),
                        "table name must not carry quotes: got [{}] from [{sql}]",
                        sel.from.name
                    );
                }
                _ => panic!("expected Select for [{sql}]"),
            }
        }
    }

    /// Sweep regression for TG-4-finding-003 on the JOIN right-hand
    /// side: pre-fix `convert_join_right` propagated `name.to_string()`
    /// the same way `convert_table_ref` did, so
    /// `JOIN "wikipedia_compat" b ON a.k = b.k` carried the quoted
    /// literal into the join executor and silently joined against an
    /// empty datasource.
    #[test]
    fn parse_quoted_join_right_strips_quotes() {
        let sql = r#"
            SELECT a.page, b.cnt
            FROM "wikipedia_compat" a
            JOIN "wikipedia_compat" b ON a.page = b.page
        "#;
        let stmt = parse_druid_sql(sql).unwrap_or_else(|e| panic!("parse [{sql}]: {e}"));
        let DruidSqlStatement::Select(sel) = stmt else {
            panic!("expected Select for [{sql}]");
        };
        assert!(
            !sel.from.name.contains('"'),
            "left table name must not carry quotes: got [{}]",
            sel.from.name
        );
        assert_eq!(
            sel.from.joins.len(),
            1,
            "expected one JOIN, got {} for [{sql}]",
            sel.from.joins.len()
        );
        let join = &sel.from.joins[0];
        match &join.right {
            crate::parser::JoinRightSide::Table { name, .. } => {
                assert!(
                    !name.contains('"'),
                    "JOIN right table name must not carry quotes: got [{name}] for [{sql}]"
                );
                assert_eq!(name, "wikipedia_compat");
            }
            other => panic!("expected JoinRightSide::Table, got {other:?} for [{sql}]"),
        }
    }

    #[test]
    fn parse_select_star() {
        let stmt = parse_druid_sql("SELECT * FROM sales").expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 1);
                assert!(matches!(sel.projections[0], Projection::Wildcard));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_with_where() {
        let stmt = parse_druid_sql("SELECT city FROM sales WHERE city = 'tokyo'").expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert!(sel.filter.is_some());
                match sel.filter.as_ref() {
                    Some(SqlExpr::BinaryOp { op, .. }) => {
                        assert_eq!(*op, BinaryOperator::Eq);
                    }
                    _ => panic!("expected binary op filter"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_with_group_by_having() {
        let sql = "SELECT city, COUNT(*) AS cnt FROM sales GROUP BY city HAVING COUNT(*) > 10";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.group_by.len(), 1);
                assert!(sel.having.is_some());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_with_order_by_limit() {
        let sql =
            "SELECT city, COUNT(*) AS cnt FROM sales GROUP BY city ORDER BY cnt DESC LIMIT 10";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 1);
                assert!(!sel.order_by[0].asc);
                assert_eq!(sel.limit, Some(10));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_time_floor() {
        let sql = "SELECT TIME_FLOOR(__time, 'PT1H') AS t, COUNT(*) FROM sales GROUP BY 1";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 2);
                match &sel.projections[0] {
                    Projection::Expr {
                        expr: SqlExpr::Function(DruidFunction::TimeFloor { period, .. }),
                        alias,
                    } => {
                        assert_eq!(period, "PT1H");
                        assert_eq!(alias.as_deref(), Some("t"));
                    }
                    _ => panic!("expected TIME_FLOOR function"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_approx_count_distinct() {
        let sql = "SELECT APPROX_COUNT_DISTINCT(user_id) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 1);
                match &sel.projections[0] {
                    Projection::Expr {
                        expr: SqlExpr::Function(DruidFunction::ApproxCountDistinct { .. }),
                        ..
                    } => {}
                    _ => panic!("expected APPROX_COUNT_DISTINCT"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_explain_plan_for_druid_syntax() {
        // Druid / Superset SQL Lab syntax with the `PLAN FOR` keywords.
        let stmt = parse_druid_sql("EXPLAIN PLAN FOR SELECT COUNT(*) FROM sales").expect("parse");
        assert!(matches!(stmt, DruidSqlStatement::ExplainPlan(_)));
        // Case-insensitive.
        let stmt = parse_druid_sql("explain plan for SELECT city FROM sales").expect("parse");
        assert!(matches!(stmt, DruidSqlStatement::ExplainPlan(_)));
    }

    #[test]
    fn parse_date_trunc_lowers_to_time_floor() {
        // DATE_TRUNC('hour', __time) -> TIME_FLOOR(__time, 'PT1H').
        let stmt = parse_druid_sql(
            "SELECT DATE_TRUNC('hour', __time) AS t, COUNT(*) FROM sales GROUP BY 1",
        )
        .expect("parse");
        let DruidSqlStatement::Select(sel) = stmt else {
            panic!("expected Select");
        };
        let has_time_floor_pt1h = sel.projections.iter().any(|p| {
            matches!(
                p,
                Projection::Expr {
                    expr: SqlExpr::Function(DruidFunction::TimeFloor { period, .. }),
                    ..
                } if period == "PT1H"
            )
        });
        assert!(
            has_time_floor_pt1h,
            "DATE_TRUNC('hour',..) must lower to TIME_FLOOR PT1H"
        );
        // Unsupported unit rejects.
        assert!(parse_druid_sql("SELECT DATE_TRUNC('fortnight', __time) FROM sales").is_err());
    }

    /// Codex-review HIGH finding A: a `DATE` literal is DATE-ONLY. A time
    /// component previously slipped through the timestamp parser; it now
    /// fails closed, while `DATE 'YYYY-MM-DD'` keeps folding to midnight
    /// UTC and `TIMESTAMP '...'` keeps accepting date-time forms.
    #[test]
    fn date_literal_is_date_only() {
        // Valid: date-only folds to midnight UTC epoch millis.
        let stmt = parse_druid_sql("SELECT * FROM sales WHERE __time >= DATE '2024-01-01'")
            .expect("parse");
        let DruidSqlStatement::Select(sel) = stmt else {
            panic!("expected Select");
        };
        let filter = sel.filter.expect("filter");
        let found = matches!(
            &filter,
            SqlExpr::BinaryOp { right, .. }
                if matches!(**right, SqlExpr::Literal(SqlLiteral::Timestamp(1_704_067_200_000)))
        );
        assert!(
            found,
            "DATE '2024-01-01' must fold to midnight UTC: {filter:?}"
        );

        // Invalid: any time component fails closed (Calcite rejects these).
        for bad in [
            "DATE '2024-01-01 12:00:00'",
            "DATE '2024-01-01T12:00:00'",
            "DATE '2024-01-01 00:00:00.000'",
            "DATE '2024-01-01T00:00:00Z'",
            "DATE 'not-a-date'",
        ] {
            let sql = format!("SELECT * FROM sales WHERE __time >= {bad}");
            let err = parse_druid_sql(&sql).expect_err(bad);
            assert!(
                err.to_string().contains("DATE"),
                "error for {bad} must name the DATE literal: {err}"
            );
        }

        // TIMESTAMP keeps accepting the date-time forms.
        parse_druid_sql("SELECT * FROM sales WHERE __time >= TIMESTAMP '2024-01-01 12:00:00'")
            .expect("TIMESTAMP with time component stays valid");
    }

    #[test]
    fn parse_explain_plan() {
        let sql = "EXPLAIN SELECT COUNT(*) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        assert!(matches!(stmt, DruidSqlStatement::ExplainPlan(_)));
        match stmt {
            DruidSqlStatement::ExplainPlan(inner) => {
                assert!(matches!(*inner, DruidSqlStatement::Select(_)));
            }
            _ => panic!("expected ExplainPlan"),
        }
    }

    #[test]
    fn parse_select_with_offset() {
        let sql = "SELECT * FROM sales LIMIT 10 OFFSET 5";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.limit, Some(10));
                assert_eq!(sel.offset, Some(5));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_between() {
        let sql = "SELECT * FROM sales WHERE revenue BETWEEN 100 AND 500";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert!(matches!(sel.filter, Some(SqlExpr::Between { .. })));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_in_list() {
        let sql = "SELECT * FROM sales WHERE city IN ('tokyo', 'london', 'paris')";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert!(matches!(sel.filter, Some(SqlExpr::InList { .. })));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_like() {
        let sql = "SELECT * FROM sales WHERE city LIKE 'tok%'";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert!(matches!(sel.filter, Some(SqlExpr::Like { .. })));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_is_null() {
        let sql = "SELECT * FROM sales WHERE city IS NULL";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert!(matches!(sel.filter, Some(SqlExpr::IsNull(_))));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_multiple_aggregations() {
        let sql = "SELECT city, SUM(revenue), MIN(price), MAX(price) FROM sales GROUP BY city";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                // city + 3 aggregations
                assert_eq!(sel.projections.len(), 4);
                assert_eq!(sel.group_by.len(), 1);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_count_star() {
        let sql = "SELECT COUNT(*) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 1);
                match &sel.projections[0] {
                    Projection::Expr {
                        expr: SqlExpr::Aggregate { func, arg, .. },
                        ..
                    } => {
                        assert_eq!(func, "COUNT");
                        assert!(arg.is_none());
                    }
                    _ => panic!("expected COUNT(*)"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_time_extract() {
        let sql = "SELECT TIME_EXTRACT(__time, 'HOUR') FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Function(DruidFunction::TimeExtract { unit, .. }),
                    ..
                } => {
                    assert_eq!(*unit, TimeUnit::Hour);
                }
                _ => panic!("expected TIME_EXTRACT"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_nested_and_or() {
        let sql = "SELECT * FROM sales WHERE (city = 'tokyo' OR city = 'london') AND revenue > 100";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert!(matches!(sel.filter, Some(SqlExpr::And(_, _))));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_error_empty() {
        let result = parse_druid_sql("");
        assert!(result.is_err());
    }

    #[test]
    fn parse_constant_select_no_from() {
        // A FROM-less constant SELECT (Superset `do_ping`) is now accepted and
        // yields a ConstantSelect with one synthetic row.
        let stmt = parse_druid_sql("SELECT 1").expect("SELECT 1 should parse");
        match stmt {
            DruidSqlStatement::ConstantSelect(cols) => {
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].name, "EXPR$0");
                assert!(matches!(cols[0].value, SqlLiteral::Integer(1)));
            }
            other => panic!("expected ConstantSelect, got {other:?}"),
        }

        // Aliased literals keep their alias.
        let stmt = parse_druid_sql("SELECT 1 AS ping, 'ok' AS status").expect("parse");
        let DruidSqlStatement::ConstantSelect(cols) = stmt else {
            panic!("expected ConstantSelect");
        };
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "ping");
        assert_eq!(cols[1].name, "status");
        assert!(matches!(&cols[1].value, SqlLiteral::String(s) if s == "ok"));
    }

    #[test]
    fn parse_error_no_from_column_ref() {
        // A FROM-less *column* reference has no source to resolve and still errors.
        assert!(parse_druid_sql("SELECT foo").is_err());
        assert!(parse_druid_sql("SELECT 1 WHERE x > 0").is_err());
    }

    #[test]
    fn parse_alias() {
        let sql = "SELECT city AS c, COUNT(*) AS cnt FROM sales GROUP BY city";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                match &sel.projections[0] {
                    Projection::Expr { alias, .. } => {
                        assert_eq!(alias.as_deref(), Some("c"));
                    }
                    _ => panic!("expected named expr"),
                }
                match &sel.projections[1] {
                    Projection::Expr { alias, .. } => {
                        assert_eq!(alias.as_deref(), Some("cnt"));
                    }
                    _ => panic!("expected named expr"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    // --- Window function tests ---

    #[test]
    fn parse_row_number_over_order_by() {
        let sql = "SELECT city, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 2);
                match &sel.projections[1] {
                    Projection::Expr {
                        expr: SqlExpr::Window(wf),
                        alias,
                    } => {
                        assert!(matches!(wf.function, WindowFunctionType::RowNumber));
                        assert!(wf.partition_by.is_empty());
                        assert_eq!(wf.order_by.len(), 1);
                        assert!(!wf.order_by[0].asc);
                        assert_eq!(alias.as_deref(), Some("rn"));
                    }
                    _ => panic!("expected Window function"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_rank_with_partition_by() {
        let sql =
            "SELECT city, RANK() OVER (PARTITION BY country ORDER BY revenue DESC) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[1] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => {
                    assert!(matches!(wf.function, WindowFunctionType::Rank));
                    assert_eq!(wf.partition_by, vec!["country"]);
                    assert_eq!(wf.order_by.len(), 1);
                }
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_dense_rank() {
        let sql = "SELECT DENSE_RANK() OVER (ORDER BY revenue DESC) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => {
                    assert!(matches!(wf.function, WindowFunctionType::DenseRank));
                }
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_lag_with_offset_and_default() {
        let sql = "SELECT LAG(revenue, 2, 0) OVER (ORDER BY __time) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => match &wf.function {
                    WindowFunctionType::Lag {
                        column,
                        offset,
                        default,
                    } => {
                        assert_eq!(column, "revenue");
                        assert_eq!(*offset, 2);
                        assert_eq!(*default, Some(serde_json::json!(0)));
                    }
                    _ => panic!("expected LAG"),
                },
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_lead_with_offset() {
        let sql = "SELECT LEAD(revenue, 1) OVER (ORDER BY __time) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => match &wf.function {
                    WindowFunctionType::Lead {
                        column,
                        offset,
                        default,
                    } => {
                        assert_eq!(column, "revenue");
                        assert_eq!(*offset, 1);
                        assert!(default.is_none());
                    }
                    _ => panic!("expected LEAD"),
                },
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_first_value() {
        let sql = "SELECT FIRST_VALUE(revenue) OVER (PARTITION BY city ORDER BY __time) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => {
                    assert!(matches!(wf.function, WindowFunctionType::FirstValue { .. }));
                    assert_eq!(wf.partition_by, vec!["city"]);
                    assert_eq!(wf.order_by.len(), 1);
                }
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_last_value() {
        let sql = "SELECT LAST_VALUE(revenue) OVER (PARTITION BY city ORDER BY __time) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => {
                    assert!(matches!(wf.function, WindowFunctionType::LastValue { .. }));
                }
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_sum_over_with_frame() {
        let sql = "SELECT SUM(revenue) OVER (PARTITION BY city ORDER BY __time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => {
                    assert!(matches!(wf.function, WindowFunctionType::Sum { .. }));
                    assert_eq!(wf.partition_by, vec!["city"]);
                    let frame = wf.frame.as_ref().expect("frame");
                    assert_eq!(frame.mode, FrameMode::Rows);
                    assert_eq!(frame.start, FrameBound::UnboundedPreceding);
                    assert_eq!(frame.end, FrameBound::CurrentRow);
                }
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_count_over_partition() {
        let sql = "SELECT COUNT(*) OVER (PARTITION BY city) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => {
                    // COUNT(*) OVER => column = None, distinct = false
                    assert!(matches!(
                        &wf.function,
                        WindowFunctionType::Count {
                            column: None,
                            distinct: false,
                        }
                    ));
                    assert_eq!(wf.partition_by, vec!["city"]);
                    assert!(wf.order_by.is_empty());
                }
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_multiple_window_functions() {
        let sql = "SELECT city, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn, SUM(revenue) OVER (PARTITION BY city ORDER BY __time) AS running_total FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 3);
                assert!(matches!(
                    &sel.projections[1],
                    Projection::Expr {
                        expr: SqlExpr::Window(_),
                        ..
                    }
                ));
                assert!(matches!(
                    &sel.projections[2],
                    Projection::Expr {
                        expr: SqlExpr::Window(_),
                        ..
                    }
                ));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_window_function_with_group_by() {
        let sql = "SELECT city, COUNT(*) AS cnt, ROW_NUMBER() OVER (ORDER BY city) AS rn FROM sales GROUP BY city";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert_eq!(sel.projections.len(), 3);
                assert_eq!(sel.group_by.len(), 1);
                assert!(matches!(
                    &sel.projections[2],
                    Projection::Expr {
                        expr: SqlExpr::Window(_),
                        ..
                    }
                ));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_lag_default_offset() {
        let sql = "SELECT LAG(city) OVER (ORDER BY __time) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => match &wf.function {
                    WindowFunctionType::Lag { offset, .. } => {
                        assert_eq!(*offset, 1);
                    }
                    _ => panic!("expected LAG"),
                },
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_avg_over() {
        let sql = "SELECT AVG(revenue) OVER (PARTITION BY city) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => {
                    assert!(matches!(wf.function, WindowFunctionType::Avg { .. }));
                }
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_min_max_over() {
        let sql = "SELECT MIN(revenue) OVER (ORDER BY __time) AS mn, MAX(revenue) OVER (ORDER BY __time) AS mx FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => {
                assert!(matches!(
                    &sel.projections[0],
                    Projection::Expr {
                        expr: SqlExpr::Window(wf),
                        ..
                    } if matches!(wf.function, WindowFunctionType::Min { .. })
                ));
                assert!(matches!(
                    &sel.projections[1],
                    Projection::Expr {
                        expr: SqlExpr::Window(wf),
                        ..
                    } if matches!(wf.function, WindowFunctionType::Max { .. })
                ));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_range_frame() {
        let sql = "SELECT SUM(revenue) OVER (ORDER BY __time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::Select(sel) => match &sel.projections[0] {
                Projection::Expr {
                    expr: SqlExpr::Window(wf),
                    ..
                } => {
                    let frame = wf.frame.as_ref().expect("frame");
                    assert_eq!(frame.mode, FrameMode::Range);
                }
                _ => panic!("expected Window"),
            },
            _ => panic!("expected Select"),
        }
    }

    // --- UNION ALL tests ---

    #[test]
    fn parse_union_all_two_selects() {
        let sql = "SELECT city, revenue FROM sales UNION ALL SELECT city, revenue FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::UnionAll(parts) => {
                assert_eq!(parts.len(), 2);
                assert!(matches!(parts[0], DruidSqlStatement::Select(_)));
                assert!(matches!(parts[1], DruidSqlStatement::Select(_)));
            }
            _ => panic!("expected UnionAll, got: {stmt:?}"),
        }
    }

    #[test]
    fn parse_union_all_three_selects() {
        let sql = "SELECT city FROM sales UNION ALL SELECT city FROM sales UNION ALL SELECT city FROM sales";
        let stmt = parse_druid_sql(sql).expect("parse");
        match stmt {
            DruidSqlStatement::UnionAll(parts) => {
                assert_eq!(parts.len(), 3);
            }
            _ => panic!("expected UnionAll"),
        }
    }

    #[test]
    fn parse_union_distinct_is_rejected() {
        let sql = "SELECT city FROM sales UNION SELECT city FROM sales";
        let result = parse_druid_sql(sql);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Wave 36-F (Wave 37 R1 mediums + low): parser hardening tests.
    // -----------------------------------------------------------------------

    /// `LIMIT -1` previously became a silent "no limit"; now it must surface
    /// a query error.
    #[test]
    fn parse_limit_negative_is_rejected() {
        let err = parse_druid_sql("SELECT * FROM t LIMIT -1").expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("LIMIT") && msg.contains("non-negative integer"),
            "expected LIMIT rejection, got: {msg}"
        );
    }

    /// `LIMIT 1+1` previously became a silent "no limit"; now it must error.
    #[test]
    fn parse_limit_expression_is_rejected() {
        let err = parse_druid_sql("SELECT * FROM t LIMIT 1+1").expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("LIMIT") && msg.contains("integer literal"),
            "expected LIMIT rejection, got: {msg}"
        );
    }

    /// `OFFSET 1+1 ROWS` previously became a silent no-offset; now it errors.
    #[test]
    fn parse_offset_expression_is_rejected() {
        let err = parse_druid_sql("SELECT * FROM t OFFSET 1+1 ROWS").expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("OFFSET") && msg.contains("integer literal"),
            "expected OFFSET rejection, got: {msg}"
        );
    }

    /// UNION ALL with outer ORDER BY must be rejected (the previous parser
    /// silently dropped it, sending the planner a different query than the
    /// user wrote).
    #[test]
    fn parse_union_all_with_outer_order_by_is_rejected() {
        let sql = "SELECT city FROM sales UNION ALL SELECT city FROM sales ORDER BY 1";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("ORDER BY") && msg.contains("UNION ALL"),
            "expected UNION ALL ORDER BY rejection, got: {msg}"
        );
    }

    /// UNION ALL with outer LIMIT must be rejected.
    #[test]
    fn parse_union_all_with_outer_limit_is_rejected() {
        let sql = "SELECT city FROM sales UNION ALL SELECT city FROM sales LIMIT 10";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("LIMIT") && msg.contains("UNION ALL"),
            "expected UNION ALL LIMIT rejection, got: {msg}"
        );
    }

    /// `SUBSTRING(col FROM bad_expr FOR ...)` must not silently rewrite the
    /// start to `1` — it must reject the non-integer FROM.
    #[test]
    fn parse_substring_non_integer_from_is_rejected() {
        let sql = "SELECT SUBSTRING(name FROM other_col FOR 3) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("SUBSTRING") && msg.contains("start position"),
            "expected SUBSTRING start rejection, got: {msg}"
        );
    }

    /// `SUBSTRING(col FROM 1 FOR bad_expr)` must reject the non-integer FOR
    /// instead of silently dropping it.
    #[test]
    fn parse_substring_non_integer_for_is_rejected() {
        let sql = "SELECT SUBSTRING(name FROM 1 FOR other_col) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("SUBSTRING") && msg.contains("length"),
            "expected SUBSTRING length rejection, got: {msg}"
        );
    }

    /// Wave 36-G3 regression for Wave 37B High `parser.rs:1172-1179`.
    ///
    /// `expr_to_string` previously accepted `SqlExpr::Column` and returned
    /// the column *name* as a string literal. So `REPLACE(s, old_col,
    /// new_col)` was silently rewritten to `REPLACE(s, "old_col",
    /// "new_col")` — the column values were never actually read. The
    /// strict literal-only slots now reject column references with a
    /// precise error.
    #[test]
    fn parse_replace_rejects_column_reference_in_pattern_slot() {
        let sql = "SELECT REPLACE(name, old_col, 'new') FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("REPLACE pattern") && msg.contains("string literal"),
            "expected REPLACE pattern rejection, got: {msg}"
        );

        let sql = "SELECT REPLACE(name, 'old', new_col) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("REPLACE replacement") && msg.contains("string literal"),
            "expected REPLACE replacement rejection, got: {msg}"
        );

        let sql = "SELECT REGEXP_EXTRACT(name, pat_col) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("REGEXP_EXTRACT pattern") && msg.contains("string literal"),
            "expected REGEXP_EXTRACT pattern rejection, got: {msg}"
        );

        let sql = "SELECT LOOKUP(name, lookup_col) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("LOOKUP name") && msg.contains("string literal"),
            "expected LOOKUP name rejection, got: {msg}"
        );

        let sql = "SELECT LPAD(name, 10, pad_col) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("LPAD pad") && msg.contains("string literal"),
            "expected LPAD pad rejection, got: {msg}"
        );

        let sql = "SELECT RPAD(name, 10, pad_col) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("RPAD pad") && msg.contains("string literal"),
            "expected RPAD pad rejection, got: {msg}"
        );

        // Sanity: literal-string forms still parse successfully.
        parse_druid_sql("SELECT REPLACE(name, 'old', 'new') FROM t")
            .expect("literal REPLACE must still parse");
        parse_druid_sql("SELECT REGEXP_EXTRACT(name, '(\\w+)', 1) FROM t")
            .expect("literal REGEXP_EXTRACT must still parse");
        parse_druid_sql("SELECT LPAD(name, 10, ' ') FROM t")
            .expect("literal LPAD must still parse");
    }

    /// Wave 36-G3 regression for Wave 37B High `parser.rs:1256-1305`.
    ///
    /// `WindowFunctionType::Count` was a unit variant, so the parser
    /// could not distinguish `COUNT(*) OVER`, `COUNT(col) OVER`, or
    /// `COUNT(DISTINCT col) OVER`. The variant now carries an optional
    /// column and a `distinct` flag, and non-trivial argument shapes are
    /// rejected outright.
    #[test]
    fn parse_count_over_preserves_argument_and_distinct_flag() {
        // COUNT(*) OVER — column = None, distinct = false.
        let sql = "SELECT COUNT(*) OVER (PARTITION BY city) FROM t";
        let stmt = parse_druid_sql(sql).expect("parse");
        let DruidSqlStatement::Select(sel) = stmt else {
            panic!("expected Select")
        };
        let Projection::Expr {
            expr: SqlExpr::Window(wf),
            ..
        } = &sel.projections[0]
        else {
            panic!("expected Window")
        };
        assert!(matches!(
            &wf.function,
            WindowFunctionType::Count {
                column: None,
                distinct: false,
            }
        ));

        // COUNT(col) OVER — column preserved, distinct = false.
        let sql = "SELECT COUNT(name) OVER (PARTITION BY city) FROM t";
        let stmt = parse_druid_sql(sql).expect("parse");
        let DruidSqlStatement::Select(sel) = stmt else {
            panic!("expected Select")
        };
        let Projection::Expr {
            expr: SqlExpr::Window(wf),
            ..
        } = &sel.projections[0]
        else {
            panic!("expected Window")
        };
        match &wf.function {
            WindowFunctionType::Count { column, distinct } => {
                assert_eq!(column.as_deref(), Some("name"));
                assert!(!distinct);
            }
            other => panic!("expected Count {{ column: Some(name) }}, got: {other:?}"),
        }

        // COUNT(DISTINCT col) OVER — distinct flag preserved.
        let sql = "SELECT COUNT(DISTINCT name) OVER (PARTITION BY city) FROM t";
        let stmt = parse_druid_sql(sql).expect("parse");
        let DruidSqlStatement::Select(sel) = stmt else {
            panic!("expected Select")
        };
        let Projection::Expr {
            expr: SqlExpr::Window(wf),
            ..
        } = &sel.projections[0]
        else {
            panic!("expected Window")
        };
        match &wf.function {
            WindowFunctionType::Count { column, distinct } => {
                assert_eq!(column.as_deref(), Some("name"));
                assert!(*distinct, "DISTINCT flag must be preserved");
            }
            other => panic!("expected Count {{ distinct: true }}, got: {other:?}"),
        }
    }

    /// Wave 36-G3 regression for Wave 37B High `parser.rs:1313-1324`.
    ///
    /// `PARTITION BY` previously used `filter_map` to keep only bare
    /// column references and silently dropped any other expression
    /// (including valid SQL forms like `PARTITION BY foo + 1`). The
    /// parser now rejects unsupported partition expressions with an
    /// explicit error rather than silently changing partitioning
    /// semantics.
    #[test]
    fn parse_partition_by_rejects_non_column_expression() {
        // Arithmetic expression — must be rejected, not silently dropped.
        let sql = "SELECT ROW_NUMBER() OVER (PARTITION BY foo + 1 ORDER BY ts) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("PARTITION BY") && msg.contains("column references"),
            "expected PARTITION BY rejection, got: {msg}"
        );

        // String literal — must be rejected.
        let sql = "SELECT ROW_NUMBER() OVER (PARTITION BY 'literal' ORDER BY ts) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("PARTITION BY") && msg.contains("column references"),
            "expected PARTITION BY rejection on literal, got: {msg}"
        );

        // Sanity: bare column form still parses and is preserved.
        let sql = "SELECT ROW_NUMBER() OVER (PARTITION BY city ORDER BY ts) FROM t";
        let stmt = parse_druid_sql(sql).expect("parse");
        let DruidSqlStatement::Select(sel) = stmt else {
            panic!("expected Select")
        };
        let Projection::Expr {
            expr: SqlExpr::Window(wf),
            ..
        } = &sel.projections[0]
        else {
            panic!("expected Window")
        };
        assert_eq!(wf.partition_by, vec!["city".to_string()]);
    }

    // -----------------------------------------------------------------
    // Wave 36-H — fixed-arity argument validation
    //
    // Wave 37B Codex DD flagged that per-function dispatch only enforced
    // *minimum* argument counts via `args.len() < N`, so calls like
    // `LENGTH(a, b, c)` silently dropped the trailing arguments. The
    // arity table in `function_arity` now rejects arity mismatches with
    // an explicit `function <name> expects ...` error. The tests below
    // pin the rejection path for representative `Fixed` / `Range` /
    // `Variadic` shapes.
    // -----------------------------------------------------------------

    #[test]
    fn parse_length_with_extra_args_is_rejected() {
        // LENGTH is fixed-arity 1; extra positional args must error.
        let err = parse_druid_sql("SELECT LENGTH(name, 'noise', 3) FROM t")
            .expect_err("LENGTH(a, b, c) must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("LENGTH") && msg.contains("exactly 1") && msg.contains("got 3"),
            "expected explicit LENGTH arity error, got: {msg}"
        );

        // Sanity: legitimate single-arg call still parses.
        parse_druid_sql("SELECT LENGTH(name) FROM t").expect("LENGTH(a) must still parse");

        // CHAR_LENGTH alias is also rejected on extras.
        let err = parse_druid_sql("SELECT CHAR_LENGTH(a, b) FROM t")
            .expect_err("CHAR_LENGTH(a, b) must reject");
        assert!(
            err.to_string().contains("CHAR_LENGTH"),
            "expected CHAR_LENGTH arity error"
        );
    }

    #[test]
    fn parse_replace_with_too_few_args_is_rejected() {
        // REPLACE is fixed-arity 3; too-few must error with explicit count.
        let err = parse_druid_sql("SELECT REPLACE(s, 'old') FROM t")
            .expect_err("REPLACE with 2 args must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("REPLACE") && msg.contains("exactly 3") && msg.contains("got 2"),
            "expected explicit REPLACE arity error, got: {msg}"
        );

        // Too many also rejected.
        let err = parse_druid_sql("SELECT REPLACE(s, 'a', 'b', 'c') FROM t")
            .expect_err("REPLACE with 4 args must reject");
        assert!(
            err.to_string().contains("REPLACE"),
            "expected REPLACE arity error on extras"
        );

        // Sanity: 3-arg form still parses.
        parse_druid_sql("SELECT REPLACE(s, 'a', 'b') FROM t").expect("REPLACE(s, a, b) must parse");
    }

    #[test]
    fn parse_substring_2_or_3_args_accepted() {
        // SUBSTRING accepts 2 or 3 args via the function-call syntax that
        // routes through `convert_function`. Both forms must parse.
        // (Note: `SUBSTRING(s)` in sqlparser routes through the special
        // `Expr::Substring(... FROM ...)` form rather than the positional
        // function dispatch, so the 1-arg reject path is exercised by the
        // dedicated SUBSTR-alias test below.)
        parse_druid_sql("SELECT SUBSTRING(s, 1) FROM t").expect("SUBSTRING(s, 1) must parse");
        parse_druid_sql("SELECT SUBSTRING(s, 1, 5) FROM t").expect("SUBSTRING(s, 1, 5) must parse");

        // 4 args is above maximum on the positional function path.
        let err = parse_druid_sql("SELECT SUBSTRING(s, 1, 5, 9) FROM t")
            .expect_err("SUBSTRING with 4 args must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("SUBSTRING") && msg.contains("between 2 and 3") && msg.contains("got 4"),
            "expected explicit range error, got: {msg}"
        );

        // SUBSTR alias is purely positional; 1-arg form must reject through
        // the function-arity table.
        let err = parse_druid_sql("SELECT SUBSTR(s) FROM t").expect_err("SUBSTR(s) must reject");
        assert!(
            err.to_string().contains("SUBSTR"),
            "expected SUBSTR arity error: {err}"
        );
        // Same for SUBSTR alias.
        parse_druid_sql("SELECT SUBSTR(s, 1, 5) FROM t").expect("SUBSTR alias must parse");
    }

    #[test]
    fn parse_concat_variadic_accepted_with_n_args() {
        // CONCAT is variadic with no upper bound; 0/1/2/many all accepted.
        parse_druid_sql("SELECT CONCAT() FROM t").expect("CONCAT() must parse");
        parse_druid_sql("SELECT CONCAT('a') FROM t").expect("CONCAT(a) must parse");
        parse_druid_sql("SELECT CONCAT('a', 'b', 'c', 'd', 'e') FROM t")
            .expect("CONCAT with 5 args must parse");

        // COALESCE is variadic with min=1.
        let err = parse_druid_sql("SELECT COALESCE() FROM t").expect_err("COALESCE() must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("COALESCE") && msg.contains("at least 1") && msg.contains("got 0"),
            "expected COALESCE variadic-min error, got: {msg}"
        );
        parse_druid_sql("SELECT COALESCE(a, b, c) FROM t").expect("COALESCE(a, b, c) must parse");

        // GREATEST/LEAST are variadic with min=2.
        let err =
            parse_druid_sql("SELECT GREATEST(a) FROM t").expect_err("GREATEST(a) must reject");
        assert!(
            err.to_string().contains("GREATEST"),
            "expected GREATEST arity error"
        );
    }

    // -----------------------------------------------------------------
    // Wave 40-C — standard-aggregate arity validation
    //
    // Closes the W37B-PB-H1 STILL-OPEN finding from the Wave 39 DD audit:
    // Wave 36-H added the arity table but explicitly excluded
    // `COUNT/SUM/MIN/MAX/AVG`, so e.g. `SUM(a, b)` and `AVG(a, b)` were
    // silently rewritten by the per-function dispatch arms.  After Wave
    // 40-C all five aggregates are `Fixed(1)` in `function_arity` and
    // surface explicit arity errors.  `COUNT(*)` is parsed as a single
    // `SqlExpr::Star` argument and `COUNT(DISTINCT col)` is one argument
    // with the distinct flag, so all three legal `COUNT` forms have
    // arity 1.
    // -----------------------------------------------------------------

    #[test]
    fn parse_count_with_extra_args_is_rejected() {
        let err =
            parse_druid_sql("SELECT COUNT(a, b) FROM t").expect_err("COUNT(a, b) must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("COUNT") && msg.contains("exactly 1") && msg.contains("got 2"),
            "expected explicit COUNT arity error, got: {msg}"
        );

        // Three-arg form also rejected.
        let err = parse_druid_sql("SELECT COUNT(a, b, c) FROM t")
            .expect_err("COUNT(a, b, c) must reject");
        assert!(
            err.to_string().contains("COUNT"),
            "expected COUNT arity error on extras"
        );
    }

    #[test]
    fn parse_sum_with_zero_args_is_rejected() {
        let err = parse_druid_sql("SELECT SUM() FROM t").expect_err("SUM() must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("SUM") && msg.contains("exactly 1") && msg.contains("got 0"),
            "expected explicit SUM arity error, got: {msg}"
        );
    }

    #[test]
    fn parse_avg_with_two_args_is_rejected() {
        let err = parse_druid_sql("SELECT AVG(a, b) FROM t").expect_err("AVG(a, b) must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("AVG") && msg.contains("exactly 1") && msg.contains("got 2"),
            "expected explicit AVG arity error, got: {msg}"
        );

        // MIN / MAX likewise reject extras.
        let err = parse_druid_sql("SELECT MIN(a, b) FROM t").expect_err("MIN(a, b) must reject");
        assert!(
            err.to_string().contains("MIN"),
            "expected MIN arity error on extras"
        );
        let err = parse_druid_sql("SELECT MAX(a, b) FROM t").expect_err("MAX(a, b) must reject");
        assert!(
            err.to_string().contains("MAX"),
            "expected MAX arity error on extras"
        );
    }

    #[test]
    fn parse_count_star_accepted() {
        // Sanity: COUNT(*) is one argument from the parser's perspective
        // (`SqlExpr::Star`) and must continue to parse cleanly under the
        // new Fixed(1) arity rule.
        parse_druid_sql("SELECT COUNT(*) FROM t").expect("COUNT(*) must parse");
        parse_druid_sql("SELECT COUNT(*) FROM t WHERE x > 0").expect("COUNT(*) WHERE must parse");
    }

    #[test]
    fn parse_count_distinct_col_accepted() {
        // Sanity: COUNT(DISTINCT col) is also one argument with the
        // distinct flag set; must continue to parse cleanly.
        parse_druid_sql("SELECT COUNT(DISTINCT user_id) FROM t")
            .expect("COUNT(DISTINCT col) must parse");
        // SUM(col), MIN(col), MAX(col), AVG(col) sanity-check too.
        parse_druid_sql("SELECT SUM(x) FROM t").expect("SUM(x) must parse");
        parse_druid_sql("SELECT MIN(x) FROM t").expect("MIN(x) must parse");
        parse_druid_sql("SELECT MAX(x) FROM t").expect("MAX(x) must parse");
        parse_druid_sql("SELECT AVG(x) FROM t").expect("AVG(x) must parse");
    }

    // -----------------------------------------------------------------
    // Wave 36-H — window-frame impossible bound ordering
    //
    // Wave 37B Codex DD flagged that frame validation checked each bound
    // independently but did not reject impossible start/end orderings.
    // `validate_frame_ordering` now enforces that the start bound sits
    // at-or-before the end bound on the logical frame line:
    //
    //   UNBOUNDED PRECEDING < N PRECEDING < CURRENT ROW < N FOLLOWING < UNBOUNDED FOLLOWING
    // -----------------------------------------------------------------

    #[test]
    fn parse_frame_unbounded_following_to_unbounded_preceding_is_rejected() {
        let sql = "SELECT SUM(x) OVER (ORDER BY ts \
                   ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject impossible UF..UP");
        let msg = err.to_string();
        assert!(
            msg.contains("Window frame")
                && msg.contains("start bound")
                && msg.contains("UnboundedFollowing")
                && msg.contains("UnboundedPreceding"),
            "expected impossible-frame rejection, got: {msg}"
        );

        // Sanity: legitimate UNBOUNDED PRECEDING ... UNBOUNDED FOLLOWING parses.
        let sql = "SELECT SUM(x) OVER (ORDER BY ts \
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t";
        parse_druid_sql(sql).expect("UP..UF must parse");
    }

    #[test]
    fn parse_frame_current_row_to_preceding_is_rejected() {
        // CURRENT ROW ... 5 PRECEDING — start > end on the frame line.
        let sql = "SELECT SUM(x) OVER (ORDER BY ts \
                   ROWS BETWEEN CURRENT ROW AND 5 PRECEDING) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject CURRENT ROW..N PRECEDING");
        let msg = err.to_string();
        assert!(
            msg.contains("Window frame") && msg.contains("CurrentRow") && msg.contains("Preceding"),
            "expected CURRENT ROW..PRECEDING rejection, got: {msg}"
        );

        // CURRENT ROW ... UNBOUNDED PRECEDING is also impossible.
        let sql = "SELECT SUM(x) OVER (ORDER BY ts \
                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject CURRENT ROW..UNBOUNDED PRECEDING");
        assert!(
            err.to_string().contains("Window frame"),
            "expected CR..UP rejection: {err}"
        );

        // Sanity: CURRENT ROW ... CURRENT ROW (degenerate but legal) parses.
        let sql = "SELECT SUM(x) OVER (ORDER BY ts \
                   ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t";
        parse_druid_sql(sql).expect("CR..CR must parse");
    }

    #[test]
    fn parse_frame_5_preceding_to_10_preceding_is_rejected() {
        // 5 PRECEDING ... 10 PRECEDING — start sits at -5, end at -10:
        // start > end on the frame line, so the window is empty/impossible.
        let sql = "SELECT SUM(x) OVER (ORDER BY ts \
                   ROWS BETWEEN 5 PRECEDING AND 10 PRECEDING) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject 5 PRECEDING..10 PRECEDING");
        let msg = err.to_string();
        assert!(
            msg.contains("Window frame") && msg.contains("Preceding"),
            "expected impossible-PRECEDING rejection, got: {msg}"
        );

        // Numeric FOLLOWING in the wrong order: 10 FOLLOWING..5 FOLLOWING is also impossible.
        let sql = "SELECT SUM(x) OVER (ORDER BY ts \
                   ROWS BETWEEN 10 FOLLOWING AND 5 FOLLOWING) FROM t";
        let err = parse_druid_sql(sql).expect_err("must reject 10 FOLLOWING..5 FOLLOWING");
        assert!(
            err.to_string().contains("Window frame"),
            "expected impossible-FOLLOWING rejection: {err}"
        );

        // Sanity: 10 PRECEDING ... 5 PRECEDING is correctly ordered.
        let sql = "SELECT SUM(x) OVER (ORDER BY ts \
                   ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) FROM t";
        parse_druid_sql(sql).expect("10 PRECEDING..5 PRECEDING must parse");

        // Sanity: 5 PRECEDING ... 5 FOLLOWING (different regions) parses.
        let sql = "SELECT SUM(x) OVER (ORDER BY ts \
                   ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) FROM t";
        parse_druid_sql(sql).expect("5 PRECEDING..5 FOLLOWING must parse");
    }

    // -----------------------------------------------------------------
    // Wave 36-H — window-COUNT executor follow-up
    //
    // Wave 36-G3 promoted `WindowFunctionType::Count` to a struct
    // variant carrying `column: Option<String>` and `distinct: bool`.
    // The Wave 36-G3 RESULTS noted that the executor still treated
    // every Count as a row count because it never branched on the new
    // shape. The query/planner crates do **not** currently consume
    // `SqlExpr::Window` — there is no window executor wiring yet — so
    // the executor follow-up here is the AST routing required for any
    // future executor to discriminate `COUNT(*)` from `COUNT(col)` /
    // `COUNT(DISTINCT col)`. The asserts below pin that routing so a
    // future executor that switches on the variant cannot regress
    // silently to row-count semantics. Honest gap: end-to-end execution
    // is not exercised because no executor consumes window functions
    // yet (deferred to Wave 36-I or the broader query-window sweep).
    // -----------------------------------------------------------------

    /// `COUNT(*) OVER (...)` must route to the row-count branch
    /// (`column = None`) so the future executor counts every row in the
    /// window frame regardless of column nulls.
    #[test]
    fn count_star_over_window_counts_all_rows() {
        let sql = "SELECT COUNT(*) OVER (PARTITION BY city ORDER BY ts) FROM t";
        let stmt = parse_druid_sql(sql).expect("parse");
        let DruidSqlStatement::Select(sel) = stmt else {
            panic!("expected Select")
        };
        let Projection::Expr {
            expr: SqlExpr::Window(wf),
            ..
        } = &sel.projections[0]
        else {
            panic!("expected Window projection")
        };
        match &wf.function {
            WindowFunctionType::Count { column, distinct } => {
                assert!(
                    column.is_none(),
                    "COUNT(*) must route as Count {{ column: None }}, got {column:?}"
                );
                assert!(!distinct, "COUNT(*) must not set distinct flag");
            }
            other => panic!("expected Count variant, got: {other:?}"),
        }
    }

    /// `COUNT(col) OVER (...)` must route to the null-skipping branch
    /// (`column = Some(c)`, `distinct = false`) so the future executor
    /// can count only non-null occurrences of `col` in the frame.
    #[test]
    fn count_col_over_window_skips_nulls() {
        let sql = "SELECT COUNT(name) OVER (PARTITION BY city ORDER BY ts) FROM t";
        let stmt = parse_druid_sql(sql).expect("parse");
        let DruidSqlStatement::Select(sel) = stmt else {
            panic!("expected Select")
        };
        let Projection::Expr {
            expr: SqlExpr::Window(wf),
            ..
        } = &sel.projections[0]
        else {
            panic!("expected Window projection")
        };
        match &wf.function {
            WindowFunctionType::Count { column, distinct } => {
                assert_eq!(
                    column.as_deref(),
                    Some("name"),
                    "COUNT(col) must route as Count {{ column: Some(col) }}"
                );
                assert!(
                    !distinct,
                    "COUNT(col) without DISTINCT must not set distinct flag"
                );
            }
            other => panic!("expected Count variant, got: {other:?}"),
        }
    }

    /// `COUNT(DISTINCT col) OVER (...)` must route to the deduping
    /// branch (`column = Some(c)`, `distinct = true`) so the future
    /// executor can use a HashSet to count distinct non-null values.
    #[test]
    fn count_distinct_col_over_window_dedups() {
        let sql = "SELECT COUNT(DISTINCT name) OVER (PARTITION BY city ORDER BY ts) FROM t";
        let stmt = parse_druid_sql(sql).expect("parse");
        let DruidSqlStatement::Select(sel) = stmt else {
            panic!("expected Select")
        };
        let Projection::Expr {
            expr: SqlExpr::Window(wf),
            ..
        } = &sel.projections[0]
        else {
            panic!("expected Window projection")
        };
        match &wf.function {
            WindowFunctionType::Count { column, distinct } => {
                assert_eq!(
                    column.as_deref(),
                    Some("name"),
                    "COUNT(DISTINCT col) must preserve column"
                );
                assert!(
                    *distinct,
                    "COUNT(DISTINCT col) must set distinct flag for HashSet dedupe"
                );
            }
            other => panic!("expected Count variant, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // 24 h fuzz regression (2026-05-03): deep paren nesting DoS
    // -----------------------------------------------------------------------

    /// Regression for fuzz finding
    /// `fuzz/artifacts/fuzz_sql_parse/timeout-663b5bf798ff54c08971f570bb5277dcbd1dc449`
    /// (108 B `(((UPDATE(((UPDATE…`).
    ///
    /// Previously this input caused `sqlparser`'s recursive-descent engine
    /// to either overflow the stack or run past the libfuzzer 10 s timeout
    /// (the artifact filename indicates the latter on the 24 h runner).
    /// The fix pre-validates `(`/`)` nesting at the byte level and rejects
    /// any input that exceeds [`MAX_SQL_PAREN_DEPTH`] before invoking
    /// `Parser::parse_sql`.
    #[test]
    fn parser_rejects_deeply_nested_paren_timeout_artifact() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/timeout-663b5bf798ff54c08971f570bb5277dcbd1dc449"
        );
        let sql = std::str::from_utf8(crash).expect("artifact is ASCII");
        let result = parse_druid_sql(sql);
        assert!(result.is_err(), "expected error, got {result:?}");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("parenthesis nesting") && msg.contains("24"),
            "error message should mention the cap: {msg}"
        );
    }

    /// 40-deep `(` nesting is rejected (above the 24 cap).
    #[test]
    fn parser_rejects_paren_nesting_above_cap() {
        let sql = format!("SELECT {}1{} FROM t", "(".repeat(40), ")".repeat(40));
        let result = parse_druid_sql(&sql);
        assert!(result.is_err(), "expected error, got {result:?}");
    }

    /// 8-deep `(` is well below the cap and must still parse normally.
    #[test]
    fn parser_modest_paren_nesting_is_ok() {
        let sql = format!("SELECT {}1{} FROM t", "(".repeat(8), ")".repeat(8));
        let result = parse_druid_sql(&sql);
        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    /// 2026-05-04 24 h fuzz finding: bracket-only nesting reproduces the
    /// same sqlparser super-linear backtracking that paren nesting did
    /// (1501 B input `CALL"..."(...,[BASE]+ARRAY[...+ARRAY[...+ARRAY[...]]])`,
    /// 2.4 s in standalone test). The `(` cap (commit 864f3ce) failed to
    /// catch this; the fix counts `[` toward the same depth.
    #[test]
    fn parser_rejects_bracket_array_nesting_timeout_artifact() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/timeout-35d049ed10230a74d16491a7ccf0c08e83cf3cf5"
        );
        let sql = String::from_utf8_lossy(crash);
        let result = parse_druid_sql(&sql);
        assert!(result.is_err(), "expected error, got {result:?}");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("parenthesis nesting") && msg.contains("24"),
            "error message should mention the cap: {msg}"
        );
    }

    /// 2026-05-08 24 h fuzz wave: 89 B / 101 B inputs hitting depth
    /// 31 / 32 exactly slipped past the previous `32` cap. Standalone
    /// they ran in 226 ms / 544 ms, but under sancov + ASan the fuzz
    /// runner saw them exceed 10 s wall-clock. Cap dropped to `24`.
    #[test]
    fn parser_rejects_paren_depth_31_artifact_2026_05_08() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/regression-paren-depth-31-2026-05-08"
        );
        let sql = String::from_utf8_lossy(crash);
        let result = parse_druid_sql(&sql);
        assert!(result.is_err(), "expected error, got {result:?}");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("parenthesis nesting") && msg.contains("24"),
            "error message should mention the cap: {msg}"
        );
    }

    #[test]
    fn parser_rejects_paren_depth_32_artifact_2026_05_08() {
        let crash = include_bytes!(
            "../../../fuzz/known-crash/fuzz_sql_parse/regression-paren-depth-32-2026-05-08"
        );
        let sql = String::from_utf8_lossy(crash);
        let result = parse_druid_sql(&sql);
        assert!(result.is_err(), "expected error, got {result:?}");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("parenthesis nesting") && msg.contains("24"),
            "error message should mention the cap: {msg}"
        );
    }

    /// 40-deep `[` nesting is rejected at the same cap as `(`.
    #[test]
    fn parser_rejects_bracket_nesting_above_cap() {
        let sql = format!("SELECT a{}{} FROM t", "[".repeat(40), "]".repeat(40));
        let result = parse_druid_sql(&sql);
        assert!(result.is_err(), "expected error, got {result:?}");
    }

    /// `[` inside a string literal must NOT count, mirroring the `(` rule.
    #[test]
    fn parser_bracket_inside_string_literal_does_not_count() {
        let payload = "[".repeat(200);
        let sql = format!("SELECT '{payload}' FROM t");
        let result = parse_druid_sql(&sql);
        assert!(
            result.is_ok(),
            "brackets inside string literals should not count: {result:?}"
        );
    }

    /// `(` inside a string literal must NOT count toward nesting depth —
    /// otherwise a long literal of `(((((…` would be falsely rejected.
    #[test]
    fn parser_paren_inside_string_literal_does_not_count() {
        let payload = "(".repeat(200);
        let sql = format!("SELECT '{payload}' FROM t");
        let result = parse_druid_sql(&sql);
        assert!(
            result.is_ok(),
            "parens inside string literals should not count: {result:?}"
        );
    }

    /// `(` inside a `--` line comment also must not count.
    #[test]
    fn parser_paren_inside_line_comment_does_not_count() {
        let payload = "(".repeat(200);
        let sql = format!("SELECT 1 FROM t -- {payload}\n");
        let result = parse_druid_sql(&sql);
        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    /// `(` inside a block comment must not count either.
    #[test]
    fn parser_paren_inside_block_comment_does_not_count() {
        let payload = "(".repeat(200);
        let sql = format!("SELECT 1 FROM t /* {payload} */");
        let result = parse_druid_sql(&sql);
        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // -----------------------------------------------------------------------
    // CL-4 / W1-D — parser-level coverage for the new Calcite + Druid-
    // specific SQL surface.  Each function / clause has ≥3 dedicated
    // parser-level tests below; planner-level tests live in
    // `planner.rs::tests` and integration tests in
    // `tests/cl4_calcite.rs`.
    // -----------------------------------------------------------------------

    /// Pull the single SELECT body out of a parsed statement.
    fn select_of(stmt: &DruidSqlStatement) -> &SelectQuery {
        match stmt {
            DruidSqlStatement::Select(s) => s,
            other => panic!("expected SELECT, got: {other:?}"),
        }
    }

    /// Extract the single un-aliased projection expression for tests that
    /// only need to inspect the first SELECT column.
    fn first_projection_expr(stmt: &DruidSqlStatement) -> &SqlExpr {
        let sel = select_of(stmt);
        match sel.projections.first().expect("at least one projection") {
            Projection::Expr { expr, .. } => expr,
            other => panic!("expected expression projection, got: {other:?}"),
        }
    }

    // ----- ARRAY_AGG -----

    #[test]
    fn cl4_parse_array_agg_basic() {
        let stmt = parse_druid_sql("SELECT ARRAY_AGG(city) AS xs FROM t").expect("parse");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::ArrayAgg {
                distinct,
                size_limit,
                ..
            }) => {
                assert!(!*distinct);
                assert_eq!(*size_limit, None);
            }
            other => panic!("expected ArrayAgg, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_array_agg_distinct_with_size_limit() {
        let stmt =
            parse_druid_sql("SELECT ARRAY_AGG(DISTINCT city, 1000) AS xs FROM t").expect("parse");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::ArrayAgg {
                distinct,
                size_limit,
                ..
            }) => {
                assert!(*distinct);
                assert_eq!(*size_limit, Some(1000));
            }
            other => panic!("expected ArrayAgg, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_array_agg_arity_zero_rejected() {
        let err = parse_druid_sql("SELECT ARRAY_AGG() FROM t").expect_err("must reject 0-arg");
        let msg = format!("{err}");
        assert!(msg.contains("ARRAY_AGG"), "wrong error: {msg}");
    }

    // ----- LISTAGG -----

    #[test]
    fn cl4_parse_listagg_default_separator() {
        let stmt = parse_druid_sql("SELECT LISTAGG(city) FROM t").expect("parse");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::Listagg {
                separator,
                size_limit,
                ..
            }) => {
                assert_eq!(separator, ",");
                assert_eq!(*size_limit, None);
            }
            other => panic!("expected Listagg, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_listagg_explicit_separator() {
        let stmt = parse_druid_sql("SELECT LISTAGG(city, '|') FROM t").expect("parse");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::Listagg { separator, .. }) => {
                assert_eq!(separator, "|");
            }
            other => panic!("expected Listagg, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_listagg_size_limit() {
        let stmt = parse_druid_sql("SELECT LISTAGG(city, '|', 4096) FROM t").expect("parse");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::Listagg {
                size_limit,
                separator,
                ..
            }) => {
                assert_eq!(separator, "|");
                assert_eq!(*size_limit, Some(4096));
            }
            other => panic!("expected Listagg, got: {other:?}"),
        }
    }

    // ----- STRING_AGG -----

    #[test]
    fn cl4_parse_string_agg_basic() {
        let stmt = parse_druid_sql("SELECT STRING_AGG(city, ',') FROM t").expect("parse");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::StringAgg {
                separator,
                size_limit,
                ..
            }) => {
                assert_eq!(separator, ",");
                assert_eq!(*size_limit, None);
            }
            other => panic!("expected StringAgg, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_string_agg_size_limit() {
        let stmt = parse_druid_sql("SELECT STRING_AGG(city, '|', 2048) FROM t").expect("parse");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::StringAgg { size_limit, .. }) => {
                assert_eq!(*size_limit, Some(2048));
            }
            other => panic!("expected StringAgg, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_string_agg_separator_required() {
        let err = parse_druid_sql("SELECT STRING_AGG(city) FROM t")
            .expect_err("STRING_AGG without separator must reject");
        assert!(
            format!("{err}").contains("STRING_AGG"),
            "wrong error: {err}"
        );
    }

    // ----- BLOOM_FILTER -----

    #[test]
    fn cl4_parse_bloom_filter_basic() {
        let stmt =
            parse_druid_sql("SELECT BLOOM_FILTER(user_id, 10000) FROM t").expect("parse bloom");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::BloomFilter { num_entries, .. }) => {
                assert_eq!(*num_entries, 10000);
            }
            other => panic!("expected BloomFilter, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_bloom_filter_zero_entries_rejected() {
        let err = parse_druid_sql("SELECT BLOOM_FILTER(user_id, 0) FROM t")
            .expect_err("0 entries must reject");
        assert!(
            format!("{err}").contains("BLOOM_FILTER"),
            "wrong error: {err}"
        );
    }

    #[test]
    fn cl4_parse_bloom_filter_arity_rejected() {
        let err = parse_druid_sql("SELECT BLOOM_FILTER(user_id) FROM t")
            .expect_err("1-arg form must reject");
        assert!(
            format!("{err}").contains("BLOOM_FILTER"),
            "wrong error: {err}"
        );
    }

    // ----- BLOOM_FILTER_TEST -----

    #[test]
    fn cl4_parse_bloom_filter_test_in_where() {
        let stmt =
            parse_druid_sql("SELECT user_id FROM t WHERE BLOOM_FILTER_TEST(user_id, 'AAA=')")
                .expect("parse BLOOM_FILTER_TEST");
        let sel = select_of(&stmt);
        match sel.filter.as_ref().expect("filter") {
            SqlExpr::Function(DruidFunction::BloomFilterTest { encoded_filter, .. }) => {
                assert_eq!(encoded_filter, "AAA=");
            }
            other => panic!("expected BloomFilterTest, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_bloom_filter_test_requires_string_literal() {
        let err =
            parse_druid_sql("SELECT user_id FROM t WHERE BLOOM_FILTER_TEST(user_id, user_id)")
                .expect_err("column second-arg must reject");
        assert!(
            format!("{err}").contains("BLOOM_FILTER_TEST"),
            "wrong error: {err}"
        );
    }

    #[test]
    fn cl4_parse_bloom_filter_test_arity_rejected() {
        let err = parse_druid_sql("SELECT user_id FROM t WHERE BLOOM_FILTER_TEST(user_id)")
            .expect_err("1-arg form must reject");
        assert!(
            format!("{err}").contains("BLOOM_FILTER_TEST"),
            "wrong error: {err}"
        );
    }

    // ----- MV_FILTER_ONLY / MV_FILTER_NONE -----

    #[test]
    fn cl4_parse_mv_filter_only_with_array_literal() {
        let stmt = parse_druid_sql("SELECT MV_FILTER_ONLY(tags, ARRAY['a','b']) AS kept FROM t")
            .expect("parse MV_FILTER_ONLY");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::MvFilterOnly { values, .. }) => {
                assert_eq!(values.len(), 2, "expected 2 values, got {values:?}");
            }
            other => panic!("expected MvFilterOnly, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_mv_filter_none_with_array_literal() {
        let stmt = parse_druid_sql("SELECT MV_FILTER_NONE(tags, ARRAY['spam']) AS clean FROM t")
            .expect("parse MV_FILTER_NONE");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::MvFilterNone { values, .. }) => {
                assert_eq!(values.len(), 1, "expected 1 value, got {values:?}");
            }
            other => panic!("expected MvFilterNone, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_mv_filter_only_requires_array_literal() {
        let err = parse_druid_sql("SELECT MV_FILTER_ONLY(tags, 'a') FROM t")
            .expect_err("scalar second-arg must reject");
        assert!(
            format!("{err}").contains("MV_FILTER_ONLY"),
            "wrong error: {err}"
        );
    }

    #[test]
    fn cl4_parse_mv_filter_only_empty_array_rejected() {
        let err = parse_druid_sql("SELECT MV_FILTER_ONLY(tags, ARRAY[]) FROM t")
            .expect_err("empty ARRAY must reject");
        assert!(
            format!("{err}").contains("MV_FILTER_ONLY"),
            "wrong error: {err}"
        );
    }

    // ----- EARLIEST / LATEST (2-arg form) -----

    #[test]
    fn cl4_parse_earliest_by_two_arg() {
        let stmt = parse_druid_sql("SELECT EARLIEST(price, evt_time) FROM t").expect("parse");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::EarliestBy { time_col, .. }) => {
                assert_eq!(time_col, "evt_time");
            }
            other => panic!("expected EarliestBy, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_latest_by_two_arg() {
        let stmt = parse_druid_sql("SELECT LATEST(price, evt_time) FROM t").expect("parse");
        match first_projection_expr(&stmt) {
            SqlExpr::Function(DruidFunction::LatestBy { time_col, .. }) => {
                assert_eq!(time_col, "evt_time");
            }
            other => panic!("expected LatestBy, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_earliest_single_arg_still_works() {
        // Legacy 1-arg form must continue to parse as the implicit-__time
        // EARLIEST variant (no regression from the new 2-arg dispatch).
        let stmt = parse_druid_sql("SELECT EARLIEST(price) FROM t").expect("parse");
        assert!(
            matches!(
                first_projection_expr(&stmt),
                SqlExpr::Function(DruidFunction::Earliest(_))
            ),
            "expected single-arg Earliest"
        );
    }

    /// 3-arg `EARLIEST(expr, timeCol, x)` with a non-literal `x`
    /// must still be rejected — the third argument MUST be the
    /// `maxBytesPerString` int literal Druid 35/36 require (W1-J
    /// finding-B).  The 4-arg form is also rejected by the
    /// `Range(1, 3)` arity gate upstream.
    #[test]
    fn cl4_parse_earliest_arity_3_non_literal_rejected() {
        let err = parse_druid_sql("SELECT EARLIEST(price, evt_time, x) FROM t")
            .expect_err("3-arg form with non-literal third arg must reject");
        assert!(format!("{err}").contains("EARLIEST"), "wrong error: {err}");
    }

    /// 3-arg `EARLIEST(expr, timeCol, maxBytesPerString)` with the
    /// `maxBytesPerString` as a positive int literal must parse and
    /// lower to the same `EarliestBy` AST node as the 2-arg form (the
    /// literal is honoured at parse-time but discarded — FerroDruid's
    /// heap-string executor doesn't need an off-heap buffer hint).
    /// W1-J finding-B regression test.
    #[test]
    fn cl4_parse_earliest_arity_3_with_literal_accepted() {
        let stmt = parse_druid_sql("SELECT EARLIEST(price, evt_time, 1024) FROM t")
            .expect("3-arg literal form must parse");
        let proj = &select_of(&stmt).projections[0];
        let Projection::Expr { expr, .. } = proj else {
            panic!("expected an expression projection, got {proj:?}");
        };
        match expr {
            SqlExpr::Function(DruidFunction::EarliestBy { time_col, .. }) => {
                assert_eq!(time_col, "evt_time");
            }
            other => panic!("expected EarliestBy, got {other:?}"),
        }
    }

    /// Same for LATEST.
    #[test]
    fn cl4_parse_latest_arity_3_with_literal_accepted() {
        let stmt = parse_druid_sql("SELECT LATEST(price, evt_time, 2048) FROM t")
            .expect("3-arg literal form must parse");
        let proj = &select_of(&stmt).projections[0];
        let Projection::Expr { expr, .. } = proj else {
            panic!("expected an expression projection, got {proj:?}");
        };
        match expr {
            SqlExpr::Function(DruidFunction::LatestBy { time_col, .. }) => {
                assert_eq!(time_col, "evt_time");
            }
            other => panic!("expected LatestBy, got {other:?}"),
        }
    }

    // ----- GROUPING() -----

    #[test]
    fn cl4_parse_grouping_single_col() {
        let stmt =
            parse_druid_sql("SELECT city, GROUPING(city) FROM t GROUP BY city").expect("parse");
        let exprs: Vec<&SqlExpr> = select_of(&stmt)
            .projections
            .iter()
            .filter_map(|p| match p {
                Projection::Expr { expr, .. } => Some(expr),
                _ => None,
            })
            .collect();
        assert!(
            exprs
                .iter()
                .any(|e| matches!(e, SqlExpr::Function(DruidFunction::Grouping(_)))),
            "expected a Grouping projection in: {exprs:?}"
        );
    }

    #[test]
    fn cl4_parse_grouping_multi_col() {
        let stmt = parse_druid_sql(
            "SELECT city, country, GROUPING(city, country) FROM t GROUP BY city, country",
        )
        .expect("parse");
        let exprs: Vec<&SqlExpr> = select_of(&stmt)
            .projections
            .iter()
            .filter_map(|p| match p {
                Projection::Expr { expr, .. } => Some(expr),
                _ => None,
            })
            .collect();
        let g = exprs
            .iter()
            .find_map(|e| match e {
                SqlExpr::Function(DruidFunction::Grouping(cols)) => Some(cols),
                _ => None,
            })
            .expect("Grouping projection");
        assert_eq!(g.len(), 2, "expected 2 args, got: {g:?}");
    }

    #[test]
    fn cl4_parse_grouping_non_column_arg_rejected() {
        let err =
            parse_druid_sql("SELECT GROUPING(1 + 1) FROM t").expect_err("non-column must reject");
        assert!(format!("{err}").contains("GROUPING"), "wrong error: {err}");
    }

    // ----- Window functions: NTH_VALUE / NTILE / CUME_DIST / PERCENT_RANK ---

    #[test]
    fn cl4_parse_nth_value_window() {
        let stmt = parse_druid_sql(
            "SELECT NTH_VALUE(price, 2) OVER (PARTITION BY city ORDER BY ts) AS p2 FROM t",
        )
        .expect("parse NTH_VALUE");
        match first_projection_expr(&stmt) {
            SqlExpr::Window(w) => match &w.function {
                WindowFunctionType::NthValue { column, n } => {
                    assert_eq!(column, "price");
                    assert_eq!(*n, 2);
                }
                other => panic!("expected NthValue, got: {other:?}"),
            },
            other => panic!("expected Window expr, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_ntile_window() {
        let stmt = parse_druid_sql("SELECT NTILE(4) OVER (ORDER BY price) FROM t").expect("parse");
        match first_projection_expr(&stmt) {
            SqlExpr::Window(w) => match &w.function {
                WindowFunctionType::Ntile { tiles } => assert_eq!(*tiles, 4),
                other => panic!("expected Ntile, got: {other:?}"),
            },
            other => panic!("expected Window expr, got: {other:?}"),
        }
    }

    #[test]
    fn cl4_parse_cume_dist_and_percent_rank_window() {
        let s1 =
            parse_druid_sql("SELECT CUME_DIST() OVER (ORDER BY price) FROM t").expect("CUME_DIST");
        assert!(matches!(
            first_projection_expr(&s1),
            SqlExpr::Window(WindowFunction {
                function: WindowFunctionType::CumeDist,
                ..
            })
        ));
        let s2 = parse_druid_sql("SELECT PERCENT_RANK() OVER (ORDER BY price) FROM t")
            .expect("PERCENT_RANK");
        assert!(matches!(
            first_projection_expr(&s2),
            SqlExpr::Window(WindowFunction {
                function: WindowFunctionType::PercentRank,
                ..
            })
        ));
    }

    #[test]
    fn cl4_parse_nth_value_position_zero_rejected() {
        let err = parse_druid_sql("SELECT NTH_VALUE(price, 0) OVER () FROM t")
            .expect_err("position 0 must reject");
        assert!(format!("{err}").contains("NTH_VALUE"), "wrong error: {err}");
    }

    #[test]
    fn cl4_parse_ntile_zero_tiles_rejected() {
        let err = parse_druid_sql("SELECT NTILE(0) OVER () FROM t").expect_err("0 tiles");
        assert!(format!("{err}").contains("NTILE"), "wrong error: {err}");
    }

    // -----------------------------------------------------------------------
    // Wave 48 — proptest hardening (SQL parser robustness)
    // Wave 54-C — close W52 NEW-W48 [High] test bug:
    //   the previous `prop_parser_handles_random_select_idents` used
    //   `prop_assume!(parsed.is_ok())` which silently DISCARDED any
    //   parser-rejected case, making the property vacuous on the exact
    //   inputs it was supposed to catch.  The replacement property is
    //   reframed as the panic-safety invariant: `parse_druid_sql` MUST
    //   NOT panic on arbitrary `SELECT … FROM …` input; it MAY return
    //   `Err`.  Both `Ok(_)` and `Err(_)` are acceptable; only an
    //   unwind escaping `parse_druid_sql` is treated as failure.
    //
    // * `prop_parser_never_panics_on_arbitrary_string` — feeding any
    //   random byte-soup or printable-ASCII string into `parse_druid_sql`
    //   must produce `Result<_, DruidError>` — never panic.  Uses
    //   `catch_unwind` so a panic produces an explicit `prop_assert!`
    //   failure (with the offending input) rather than relying on the
    //   harness's implicit panic capture.
    // * `prop_parser_never_panics_on_arbitrary_select_input` — same
    //   panic-safety invariant on well-formed `SELECT a, b, c FROM t`
    //   shaped strings with random identifier names; both `Ok` and
    //   `Err` are acceptable, only panic is not.
    //
    // Note on `catch_unwind`: `parse_druid_sql` takes `&str` and
    // returns `Result<DruidSqlStatement, DruidError>`; both are
    // `RefUnwindSafe`/`UnwindSafe`, so `catch_unwind` requires no
    // `AssertUnwindSafe` shim.
    // -----------------------------------------------------------------------
    mod proptests {
        use super::super::*;
        use proptest::prelude::*;
        use std::panic;

        proptest! {
            /// Any printable-ASCII string fed to the parser must
            /// surface as either Ok or Err — never panic.  This guards
            /// `parse_druid_sql` against pathological tokenizer paths
            /// in upstream `sqlparser` even on invalid SQL.
            #[test]
            fn prop_parser_never_panics_on_arbitrary_string(
                s in r"[ -~\t\n]{0,256}"
            ) {
                let result = panic::catch_unwind(|| parse_druid_sql(&s));
                prop_assert!(
                    result.is_ok(),
                    "parse_druid_sql PANICKED on input {:?}",
                    s
                );
            }

            /// `SELECT <idents> FROM <table>` shaped strings (with
            /// random but well-formed identifiers) must never cause
            /// `parse_druid_sql` to panic.  Both `Ok(_)` and `Err(_)`
            /// are acceptable outcomes — only a panic escaping the
            /// parser counts as a failure.  Wave 54-C: replaces the
            /// earlier `prop_parser_handles_random_select_idents`
            /// which used `prop_assume!(parsed.is_ok())` and was
            /// therefore vacuous on parser-rejected inputs.
            #[test]
            fn prop_parser_never_panics_on_arbitrary_select_input(
                idents in prop::collection::vec(r"[a-z][a-z0-9_]{0,15}", 1..6),
                table in r"[a-z][a-z0-9_]{0,15}",
            ) {
                let cols = idents.join(", ");
                let sql = format!("SELECT {cols} FROM {table}");
                let result = panic::catch_unwind(|| parse_druid_sql(&sql));
                prop_assert!(
                    result.is_ok(),
                    "parse_druid_sql PANICKED on input {:?}",
                    sql
                );
            }
        }
    }
}
