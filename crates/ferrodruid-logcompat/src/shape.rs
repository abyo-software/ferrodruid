// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Shape normalization — strip literal values to a canonical *query shape*.
//!
//! Two queries that differ only in constants (filter values, interval
//! bounds, LIMIT/threshold numbers) have the same shape; the report groups
//! and counts by shape. Because every literal is replaced by `?`, the
//! default (redacted) report contains **no literal values** from the log:
//! only structural text — keywords, table names, column names, function
//! names — survives.
//!
//! * SQL: a literal-masking scanner replaces `'…'` string literals and
//!   numeric literals with `?`, strips SQL comments (`--` to end of line
//!   and `/* … */` blocks — comment text is customer text), collapses
//!   whitespace, and collapses `?, ?, ?` runs (so `IN` lists of different
//!   lengths unify). String literals are scanned under **both** quote
//!   conventions in parallel — SQL's `''` doubling and the
//!   backslash-escape convention of Druid/Calcite expression strings — and
//!   a character is masked if *either* reading considers it string
//!   content, so a mis-guessed convention can only over-mask, never leak.
//! * Native JSON: a recursive walker keeps a string value **only** when
//!   its exact path from the query root is a known structural slot
//!   (`queryType` at the root, an aggregator's `type`/`name`, a filter's
//!   `dimension`, …). Key names alone never whitelist a value: an
//!   unanticipated key puts the whole subtree into an *unknown* context
//!   where every string and number is masked (default-deny by path). The
//!   operational `context` object is dropped, and rendering sorts keys so
//!   serialization order never splits a shape.

use serde_json::Value;

// ---------------------------------------------------------------------------
// SQL shapes
// ---------------------------------------------------------------------------

/// Canonical shape of a SQL query: comments stripped, literals masked to
/// `?`, whitespace collapsed, `?`-list runs collapsed.
pub fn sql_shape(sql: &str) -> String {
    let no_strings = mask_strings_and_comments(sql);
    let masked = mask_numbers_and_whitespace(&no_strings);
    collapse_placeholder_lists(&masked)
}

/// Character classification produced by [`Lexer`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Region {
    /// Plain SQL text (keywords, identifiers, operators).
    Plain,
    /// Inside a `'…'` string literal (delimiters included).
    Str,
    /// Inside a `"…"` quoted identifier (schema text, kept verbatim).
    Ident,
    /// Inside a `--` line comment or `/* … */` block comment.
    Comment,
}

/// Lexer mode between characters.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Plain SQL text.
    Plain,
    /// Inside a string literal.
    Str,
    /// Inside a quoted identifier.
    Ident,
    /// Inside a `--` line comment.
    LineComment,
    /// Inside a block comment, with nesting depth (nesting is honored:
    /// over-extending a comment can only over-mask, never leak).
    Block(u32),
}

/// A tiny streaming SQL lexer that classifies every character as string /
/// identifier / comment / plain under one string-escape convention.
struct Lexer {
    mode: Mode,
    /// Number of upcoming characters already claimed by a two-character
    /// token (`/*`, `*/`, `''`, `""`) or an escape; they are classified as
    /// `pending_region` without running mode logic.
    pending: u8,
    pending_region: Region,
    /// When `true`, a backslash inside a string literal escapes the next
    /// character (Druid/Calcite expression convention).
    backslash: bool,
}

impl Lexer {
    fn new(backslash: bool) -> Self {
        Self {
            mode: Mode::Plain,
            pending: 0,
            pending_region: Region::Plain,
            backslash,
        }
    }

    /// Classify `c` (with one character of lookahead) and advance.
    fn step(&mut self, c: char, next: Option<char>) -> Region {
        if self.pending > 0 {
            self.pending -= 1;
            return self.pending_region;
        }
        match self.mode {
            Mode::Plain => match c {
                '\'' => {
                    self.mode = Mode::Str;
                    Region::Str
                }
                '"' => {
                    self.mode = Mode::Ident;
                    Region::Ident
                }
                '-' if next == Some('-') => {
                    self.mode = Mode::LineComment;
                    Region::Comment
                }
                '/' if next == Some('*') => {
                    self.mode = Mode::Block(1);
                    self.pending = 1;
                    self.pending_region = Region::Comment;
                    Region::Comment
                }
                _ => Region::Plain,
            },
            Mode::Str => match c {
                '\'' if next == Some('\'') => {
                    // Doubled quote: escaped content, stay in the string.
                    self.pending = 1;
                    self.pending_region = Region::Str;
                    Region::Str
                }
                '\'' => {
                    self.mode = Mode::Plain;
                    Region::Str
                }
                '\\' if self.backslash => {
                    // Backslash escape: the next character is content.
                    self.pending = 1;
                    self.pending_region = Region::Str;
                    Region::Str
                }
                _ => Region::Str,
            },
            Mode::Ident => match c {
                '"' if next == Some('"') => {
                    self.pending = 1;
                    self.pending_region = Region::Ident;
                    Region::Ident
                }
                '"' => {
                    self.mode = Mode::Plain;
                    Region::Ident
                }
                _ => Region::Ident,
            },
            Mode::LineComment => {
                if c == '\n' {
                    self.mode = Mode::Plain;
                }
                Region::Comment
            }
            Mode::Block(depth) => match c {
                '*' if next == Some('/') => {
                    self.pending = 1;
                    self.pending_region = Region::Comment;
                    self.mode = if depth <= 1 {
                        Mode::Plain
                    } else {
                        Mode::Block(depth - 1)
                    };
                    Region::Comment
                }
                '/' if next == Some('*') => {
                    self.mode = Mode::Block(depth.saturating_add(1));
                    self.pending = 1;
                    self.pending_region = Region::Comment;
                    Region::Comment
                }
                _ => Region::Comment,
            },
        }
    }
}

/// Replace string literals with `?` and drop comments, scanning under both
/// string-escape conventions in parallel.
///
/// A character is **dropped** if either convention classifies it as
/// comment text, and **masked** if either classifies it as string content;
/// only characters that both conventions agree are plain SQL text (or
/// quoted-identifier text — schema references, not data) survive. Whatever
/// convention the producer actually used, its string/comment regions are a
/// subset of the union, so a literal can never survive; disagreement only
/// ever over-masks.
fn mask_strings_and_comments(sql: &str) -> String {
    let mut standard = Lexer::new(false); // SQL: only '' doubling
    let mut backslashed = Lexer::new(true); // Druid/Calcite expressions: \'
    let mut out = String::with_capacity(sql.len());
    let mut in_masked_run = false;
    let mut iter = sql.chars().peekable();
    while let Some(c) = iter.next() {
        let next = iter.peek().copied();
        let a = standard.step(c, next);
        let b = backslashed.step(c, next);
        if a == Region::Comment || b == Region::Comment {
            // Comment text is dropped; leave a separator so adjacent
            // tokens don't fuse (whitespace collapse cleans this up).
            out.push(' ');
            in_masked_run = false;
            continue;
        }
        if a == Region::Str || b == Region::Str {
            if !in_masked_run {
                out.push('?');
            }
            in_masked_run = true;
            continue;
        }
        out.push(c);
        in_masked_run = false;
    }
    out
}

/// Replace numeric literals with `?` and collapse whitespace runs to
/// single spaces. Double-quoted identifiers are copied verbatim (schema
/// references, not data). Runs after [`mask_strings_and_comments`], so no
/// string-literal or comment text remains in the input.
fn mask_numbers_and_whitespace(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    // The previous character emitted, used to decide whether a digit
    // starts a numeric literal or continues an identifier.
    let mut prev: Option<char> = None;
    let mut iter = s.chars().peekable();
    while let Some(c) = iter.next() {
        if c == '"' {
            // Quoted identifier — copy verbatim.
            out.push(c);
            for ci in iter.by_ref() {
                out.push(ci);
                if ci == '"' {
                    break;
                }
            }
            prev = Some('"');
            continue;
        }
        if c.is_ascii_digit() && !prev.is_some_and(is_identifier_char) {
            // Numeric literal (integer / decimal / exponent).
            let mut last = c;
            while let Some(&n) = iter.peek() {
                let continues = n.is_ascii_digit()
                    || n == '.'
                    || n == 'e'
                    || n == 'E'
                    || ((n == '+' || n == '-') && matches!(last, 'e' | 'E'));
                if !continues {
                    break;
                }
                last = n;
                iter.next();
            }
            out.push('?');
            prev = Some('?');
            continue;
        }
        if c.is_whitespace() {
            if prev != Some(' ') {
                out.push(' ');
                prev = Some(' ');
            }
            continue;
        }
        out.push(c);
        prev = Some(c);
    }
    out.trim().to_string()
}

/// `true` for characters that can continue a SQL identifier.
fn is_identifier_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '$'
}

/// Collapse `?, ?, ?` runs to a single `?` so `IN` lists (and `VALUES`
/// rows) of different lengths share a shape.
fn collapse_placeholder_lists(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut iter = s.chars().peekable();
    while let Some(c) = iter.next() {
        out.push(c);
        if c != '?' {
            continue;
        }
        // Swallow any following `, ?` (with optional spaces) runs. On a
        // partial match the buffered separators are re-emitted; the
        // buffer never exceeds a few characters because whitespace was
        // already collapsed.
        loop {
            let mut lookahead = String::new();
            while iter.peek() == Some(&' ') {
                lookahead.push(' ');
                iter.next();
            }
            if iter.peek() == Some(&',') {
                lookahead.push(',');
                iter.next();
                while iter.peek() == Some(&' ') {
                    lookahead.push(' ');
                    iter.next();
                }
                if iter.peek() == Some(&'?') {
                    iter.next();
                    continue;
                }
            }
            out.push_str(&lookahead);
            break;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Native shapes
// ---------------------------------------------------------------------------

/// The grammatical context a JSON subtree sits in, tracked along the path
/// from the query root. A string value is kept **only** when the
/// `(context, key)` pair it sits at is an explicitly enumerated structural
/// slot; contexts are only ever entered through explicitly enumerated
/// `(context, key)` transitions. Any key not enumerated leads to
/// [`Ctx::Unknown`], which has **no** structural slots and from which every
/// transition leads back to [`Ctx::Unknown`] — so nothing under an
/// unanticipated key can ever surface a string (default-deny by path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ctx {
    /// A native query object (the root, or a nested `query` datasource).
    Query,
    /// A datasource spec (table / union / join / query / lookup / inline).
    DataSource,
    /// A filter spec (selector / bound / in / equals / and-or-not / …).
    Filter,
    /// A search-query spec (`contains` / `insensitive_contains` / …).
    SearchQuery,
    /// An aggregator spec.
    Aggregator,
    /// A post-aggregator spec.
    PostAggregator,
    /// A dimension spec (default / extraction / lookup / listFiltered).
    DimensionSpec,
    /// A topN metric spec (numeric / dimension / inverted).
    TopNMetric,
    /// A groupBy `limitSpec`.
    LimitSpec,
    /// One orderBy column inside a `limitSpec`.
    OrderByColumn,
    /// A groupBy `having` spec.
    Having,
    /// A virtual column spec.
    VirtualColumn,
    /// A search `sort` spec.
    Sort,
    /// A segmentMetadata `toInclude` spec.
    ToInclude,
    /// A granularity spec object (`period` / `duration` / simple).
    Granularity,
    /// An extraction function spec.
    ExtractionFn,
    /// A lookup spec (`{"type":"map","map":{…}}` / registered lookup).
    Lookup,
    /// A legacy select `pagingSpec`.
    PagingSpec,
    /// A map whose keys AND values are data (lookup maps, paging
    /// identifiers): collapsed to `{"?":"?"}`.
    MaskedMap,
    /// Anything reached through a non-enumerated key: no structural
    /// slots, all strings and numbers masked.
    Unknown,
}

/// The context of the subtree under `key` within an object in `ctx`.
/// Default-deny: every `(context, key)` pair not enumerated here lands in
/// [`Ctx::Unknown`].
fn child_ctx(ctx: Ctx, key: &str) -> Ctx {
    use Ctx::{
        Aggregator, DataSource, DimensionSpec, ExtractionFn, Filter, Granularity, Having,
        LimitSpec, Lookup, MaskedMap, OrderByColumn, PagingSpec, PostAggregator, Query,
        SearchQuery, Sort, ToInclude, TopNMetric, Unknown, VirtualColumn,
    };
    match (ctx, key) {
        (Query, "dataSource") => DataSource,
        (Query, "filter") => Filter,
        (Query, "having") => Having,
        (Query, "aggregations") => Aggregator,
        (Query, "postAggregations") => PostAggregator,
        (Query, "virtualColumns") => VirtualColumn,
        (Query, "limitSpec") => LimitSpec,
        (Query, "dimension" | "dimensions" | "searchDimensions") => DimensionSpec,
        (Query, "metric") => TopNMetric,
        (Query, "granularity") | (ExtractionFn, "granularity") => Granularity,
        (Query, "sort") => Sort,
        // A search query's match spec (`"query": {"type": "contains", …}`).
        (Query | Filter, "query") => SearchQuery,
        (Query, "toInclude") => ToInclude,
        (Query, "pagingSpec") => PagingSpec,
        (DataSource, "base" | "left" | "right" | "dataSources") => DataSource,
        // A nested query datasource re-enters the full query grammar.
        (DataSource, "query") => Query,
        (Filter, "field" | "fields" | "filter") => Filter,
        (Filter | DimensionSpec, "extractionFn") => ExtractionFn,
        (Having, "havingSpec" | "havingSpecs") => Having,
        (Having | Aggregator, "filter") => Filter,
        (Aggregator, "aggregator") => Aggregator,
        // Cardinality/HLL aggregators take dimension specs in `fields`.
        (Aggregator, "fields") => DimensionSpec,
        (PostAggregator, "field" | "fields") => PostAggregator,
        (DimensionSpec, "delegate") => DimensionSpec,
        (DimensionSpec | ExtractionFn, "lookup") => Lookup,
        (ExtractionFn, "extractionFns") => ExtractionFn,
        (TopNMetric, "metric") => TopNMetric,
        (LimitSpec, "columns") => OrderByColumn,
        // Data-keyed maps: both keys and values are customer data.
        (Lookup, "map") | (PagingSpec, "pagingIdentifiers") => MaskedMap,
        _ => Unknown,
    }
}

/// Whether a **string** at `(ctx, key)` is a structural slot (an operator
/// name, or a schema reference: table / column / alias name) kept verbatim
/// in the shape. Everything not enumerated here is masked.
fn structural_string(ctx: Ctx, key: &str) -> bool {
    use Ctx::{
        Aggregator, DataSource, DimensionSpec, ExtractionFn, Filter, Granularity, Having,
        LimitSpec, Lookup, MaskedMap, OrderByColumn, PagingSpec, PostAggregator, Query,
        SearchQuery, Sort, ToInclude, TopNMetric, Unknown, VirtualColumn,
    };
    match (ctx, key) {
        // No structural slots outside enumerated contexts, ever.
        (Unknown | MaskedMap | PagingSpec, _) => false,
        // `type` names the operator in every spec-object context (but not
        // at the query root, where the slot is `queryType`).
        (
            DataSource | Filter | SearchQuery | Aggregator | PostAggregator | DimensionSpec
            | TopNMetric | LimitSpec | Having | VirtualColumn | Sort | ToInclude | Granularity
            | ExtractionFn | Lookup,
            "type",
        ) => true,
        (
            Query,
            "queryType" | "dataSource" | "granularity" | "dimension" | "dimensions" | "metric"
            | "metrics" | "columns" | "searchDimensions" | "bound" | "resultFormat" | "order"
            | "analysisTypes" | "subtotalsSpec",
        ) => true,
        (
            DataSource,
            "name" | "lookup" | "rightPrefix" | "joinType" | "leftKey" | "rightKey" | "dataSources"
            | "columnNames",
        ) => true,
        (Filter, "dimension" | "dimensions" | "column" | "matchValueType" | "ordering") => true,
        (Aggregator, "name" | "fieldName" | "fieldNames") => true,
        (PostAggregator, "name" | "fieldName" | "fieldNames" | "fn" | "ordering") => true,
        (DimensionSpec, "dimension" | "outputName" | "outputType" | "lookup") => true,
        (TopNMetric, "metric" | "ordering") => true,
        (LimitSpec, "columns") => true, // plain-string orderBy column
        (OrderByColumn, "dimension" | "direction" | "dimensionOrder") => true,
        (Having, "aggregation" | "dimension") => true,
        (VirtualColumn, "name" | "outputName" | "outputType" | "columnName") => true,
        (Granularity, "period" | "timeZone") => true,
        (ExtractionFn, "format" | "timeZone" | "locale") => true,
        (ToInclude, "columns") => true,
        _ => false,
    }
}

/// Whether a **string** at `(ctx, key)` is a Druid expression: kept, but
/// with SQL-style literal masking applied (expressions embed constants,
/// e.g. `added > 5`, and string literals under either quote convention).
fn expression_string(ctx: Ctx, key: &str) -> bool {
    matches!(
        (ctx, key),
        (
            Ctx::Aggregator,
            "expression"
                | "fold"
                | "combine"
                | "compare"
                | "finalize"
                | "initialValue"
                | "initialCombineValue",
        ) | (
            Ctx::PostAggregator | Ctx::VirtualColumn | Ctx::Filter,
            "expression"
        ) | (Ctx::DataSource, "condition")
    )
}

/// Keys dropped from the shape entirely: operational tuning that varies
/// per-request (query IDs, timeouts) and would otherwise split identical
/// workload shapes.
const DROPPED_KEYS: &[&str] = &["context"];

/// Canonical shape of a native query: literals masked (default-deny by
/// path), `context` dropped, keys sorted.
pub fn native_shape(v: &Value) -> String {
    let masked = mask_native(v, Ctx::Query, None);
    canonical_string(&masked)
}

/// Recursive masking walker. `ctx` is the context of the *enclosing*
/// object (`Ctx::Query` at the root) and `key` is the object key under
/// which `v` sits (`None` at the root). Arrays are transparent: items keep
/// the `(ctx, key)` of the array itself.
fn mask_native(v: &Value, ctx: Ctx, key: Option<&str>) -> Value {
    match v {
        Value::Object(map) => {
            // The context this object lives in, derived from its position.
            let obj_ctx = match key {
                None => ctx,
                Some(k) => child_ctx(ctx, k),
            };
            if obj_ctx == Ctx::MaskedMap {
                // Data-keyed map: keys and values are both customer data.
                let mut masked = serde_json::Map::new();
                masked.insert("?".to_string(), Value::String("?".into()));
                return Value::Object(masked);
            }
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                if DROPPED_KEYS.contains(&k.as_str()) {
                    continue;
                }
                out.insert(k.clone(), mask_native(val, obj_ctx, Some(k.as_str())));
            }
            Value::Object(out)
        }
        Value::Array(items) => {
            let masked: Vec<Value> = items
                .iter()
                .map(|item| mask_native(item, ctx, key))
                .collect();
            // An all-`"?"` array (a masked IN-list / interval list)
            // collapses to `["?"]` so lists of different lengths unify.
            if !masked.is_empty() && masked.iter().all(|m| m == &Value::String("?".into())) {
                Value::Array(vec![Value::String("?".into())])
            } else {
                Value::Array(masked)
            }
        }
        Value::String(s) => match key {
            Some(k) if expression_string(ctx, k) => Value::String(sql_shape(s)),
            Some(k) if structural_string(ctx, k) => Value::String(s.clone()),
            _ => Value::String("?".into()),
        },
        // Numbers are literals wherever they appear (thresholds, limits,
        // filter bounds); booleans and nulls are structural flags.
        Value::Number(_) => Value::String("?".into()),
        Value::Bool(_) | Value::Null => v.clone(),
    }
}

/// Render a JSON value with object keys sorted, so serialization order
/// never splits shapes.
fn canonical_string(v: &Value) -> String {
    let mut out = String::new();
    write_canonical(v, &mut out);
    out
}

fn write_canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                // Object keys serialize infallibly.
                out.push_str(&Value::String((*k).clone()).to_string());
                out.push(':');
                if let Some(val) = map.get(*k) {
                    write_canonical(val, out);
                }
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sql_literals_masked_and_deduped() {
        let a = sql_shape("SELECT COUNT(*) FROM wiki WHERE lang = 'en' AND added > 100 LIMIT 5");
        let b = sql_shape("SELECT COUNT(*) FROM wiki WHERE lang = 'ja' AND added > 999 LIMIT 50");
        assert_eq!(a, b);
        assert!(!a.contains("en"), "literal leaked: {a}");
        assert!(!a.contains("100"), "literal leaked: {a}");
        assert_eq!(
            a,
            "SELECT COUNT(*) FROM wiki WHERE lang = ? AND added > ? LIMIT ?"
        );
    }

    #[test]
    fn sql_in_lists_collapse() {
        let a = sql_shape("SELECT * FROM t WHERE x IN ('a', 'b', 'c')");
        let b = sql_shape("SELECT * FROM t WHERE x IN ('a', 'b', 'c', 'd', 'e')");
        assert_eq!(a, b);
        assert_eq!(a, "SELECT * FROM t WHERE x IN (?)");
    }

    #[test]
    fn sql_escaped_quotes_and_identifiers() {
        let s = sql_shape("SELECT \"page\" FROM t WHERE note = 'it''s'");
        assert_eq!(s, "SELECT \"page\" FROM t WHERE note = ?");
        // identifiers with digits survive
        let s2 = sql_shape("SELECT col1, __time FROM t2");
        assert_eq!(s2, "SELECT col1, __time FROM t2");
    }

    #[test]
    fn sql_timestamp_literals_masked() {
        let s = sql_shape("SELECT 1 FROM t WHERE __time >= TIMESTAMP '2024-01-01 00:00:00'");
        assert_eq!(s, "SELECT ? FROM t WHERE __time >= TIMESTAMP ?");
    }

    #[test]
    fn sql_comments_do_not_leak() {
        // Line comment: everything to end of line is customer text.
        let s = sql_shape(
            "SELECT COUNT(*) FROM wiki WHERE x = 'ok' -- customer-secret-SSN 078-05-1120",
        );
        assert!(
            !s.contains("customer-secret-SSN"),
            "line comment leaked: {s}"
        );
        assert!(!s.contains("078"), "line comment leaked: {s}");
        // Block comment, including a nested block comment.
        let b = sql_shape(
            "SELECT /* customer-secret-note */ COUNT(*) FROM wiki /* a /* nested */ secret2 */",
        );
        assert!(
            !b.contains("customer-secret-note"),
            "block comment leaked: {b}"
        );
        assert!(!b.contains("secret2"), "nested block comment leaked: {b}");
        // An unterminated block comment swallows to end of input.
        let c = sql_shape("SELECT 1 /* trailing-customer-secret");
        assert!(!c.contains("trailing-customer-secret"), "{c}");
        // A `--` inside a string literal is not a comment; the query after
        // it must survive as structure.
        let d = sql_shape("SELECT COUNT(*) FROM wiki WHERE x = 'a--b' AND y = 3");
        assert!(
            d.contains("AND y = ?"),
            "string containing -- broke masking: {d}"
        );
    }

    #[test]
    fn sql_backslash_escaped_quotes_do_not_leak() {
        // Druid/Calcite expression convention: a backslash escapes the
        // quote, the literal does NOT end at \' — the secret is string
        // content and must be masked.
        let s = sql_shape(r"SELECT concat(x,'abc\'customer-secret') FROM t");
        assert!(
            !s.contains("customer-secret"),
            "escaped-quote literal leaked: {s}"
        );
        // Standard-SQL convention (backslash not special): masking must
        // not leak under that reading either.
        let t = sql_shape(r"SELECT 1 FROM t WHERE a = 'C:\' AND b = 'windows-path-secret'");
        assert!(!t.contains("windows-path-secret"), "literal leaked: {t}");
    }

    #[test]
    fn native_masks_values_keeps_structure() {
        let q = json!({
            "queryType": "topN",
            "dataSource": "wiki",
            "dimension": "page",
            "metric": "rows",
            "threshold": 5,
            "granularity": "all",
            "intervals": ["2024-01-01/2024-01-04"],
            "filter": {"type": "selector", "dimension": "language", "value": "en"},
            "aggregations": [{"type": "count", "name": "rows"}],
            "context": {"queryId": "abc-123"}
        });
        let shape = native_shape(&q);
        assert!(!shape.contains("en\""), "filter literal leaked: {shape}");
        assert!(!shape.contains('5'), "threshold leaked: {shape}");
        assert!(!shape.contains("2024"), "interval leaked: {shape}");
        assert!(!shape.contains("abc-123"), "context leaked: {shape}");
        assert!(shape.contains("\"queryType\":\"topN\""));
        assert!(shape.contains("\"dimension\":\"page\""));
    }

    #[test]
    fn native_shapes_dedupe_across_literals_and_key_order() {
        let a = json!({
            "queryType": "timeseries", "dataSource": "wiki", "granularity": "day",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [{"type": "count", "name": "rows"}],
            "filter": {"type": "selector", "dimension": "language", "value": "en"}
        });
        let b = json!({
            "intervals": ["2025-06-01/2025-06-30", "2025-07-01/2025-07-31"],
            "granularity": "day", "dataSource": "wiki", "queryType": "timeseries",
            "aggregations": [{"name": "rows", "type": "count"}],
            "filter": {"value": "ja", "dimension": "language", "type": "selector"}
        });
        assert_eq!(native_shape(&a), native_shape(&b));
    }

    #[test]
    fn native_unknown_string_keys_are_masked_default_deny() {
        let q = json!({
            "queryType": "scan",
            "dataSource": "wiki",
            "intervals": ["2024-01-01/2024-01-02"],
            "someFutureKey": "potentially sensitive value"
        });
        let shape = native_shape(&q);
        assert!(!shape.contains("sensitive"), "unknown key leaked: {shape}");
        assert!(shape.contains("someFutureKey"));
    }

    #[test]
    fn native_string_kept_only_at_structural_paths() {
        // An unanticipated key whose value object reuses structurally
        // whitelisted key names ("type", "name") — key-name whitelisting
        // alone would leak the values; path validation must mask them.
        let q = json!({
            "queryType": "scan",
            "dataSource": "wiki",
            "intervals": ["2024-01-01/2024-01-02"],
            "resultFormat": "compactedList",
            "future": {"type": "customer-secret-token", "name": "customer-secret-name"}
        });
        let shape = native_shape(&q);
        assert!(
            !shape.contains("customer-secret-token"),
            "unanticipated `type` slot leaked: {shape}"
        );
        assert!(
            !shape.contains("customer-secret-name"),
            "unanticipated `name` slot leaked: {shape}"
        );
        // Structure survives: the unknown key itself and the real slots.
        assert!(shape.contains("\"future\""));
        assert!(shape.contains("\"queryType\":\"scan\""));
        assert!(shape.contains("\"resultFormat\":\"compactedList\""));
    }

    #[test]
    fn native_structural_keys_masked_outside_their_slot() {
        // Whitelisted key names buried under an unknown parent are not
        // structural slots; nothing under them may survive.
        let q = json!({
            "queryType": "timeseries", "dataSource": "wiki", "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "aggregations": [{"type": "count", "name": "rows"}],
            "vendorExtension": {
                "dimension": "secret-dimension-value",
                "columns": ["secret-column-value"],
                "queryType": "secret-qt"
            }
        });
        let shape = native_shape(&q);
        for leaked in ["secret-dimension-value", "secret-column-value", "secret-qt"] {
            assert!(!shape.contains(leaked), "{leaked} leaked: {shape}");
        }
        // ... while the real structural slots still survive.
        assert!(shape.contains("\"type\":\"count\""));
        assert!(shape.contains("\"name\":\"rows\""));
    }

    #[test]
    fn native_lookup_map_keys_and_values_masked() {
        // A map lookup carries data in both keys and values.
        let q = json!({
            "queryType": "topN", "dataSource": "wiki", "metric": "rows",
            "threshold": 5, "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "dimension": {"type": "lookup", "dimension": "d", "outputName": "o",
                          "lookup": {"type": "map",
                                     "map": {"secret-lookup-key": "secret-lookup-val"}}},
            "aggregations": [{"type": "count", "name": "rows"}]
        });
        let shape = native_shape(&q);
        assert!(
            !shape.contains("secret-lookup-key"),
            "lookup map key leaked: {shape}"
        );
        assert!(
            !shape.contains("secret-lookup-val"),
            "lookup map value leaked: {shape}"
        );
    }

    #[test]
    fn native_expression_backslash_escape_does_not_leak() {
        let q = json!({
            "queryType": "timeseries", "dataSource": "wiki", "granularity": "all",
            "intervals": ["2024-01-01/2024-01-02"],
            "virtualColumns": [{"type": "expression", "name": "v0",
                                "expression": "concat(\"page\", 'a\\'expr-secret')",
                                "outputType": "STRING"}],
            "aggregations": [{"type": "count", "name": "rows"}]
        });
        let shape = native_shape(&q);
        assert!(
            !shape.contains("expr-secret"),
            "escaped-quote expression literal leaked: {shape}"
        );
    }

    #[test]
    fn native_expression_strings_get_literal_masking() {
        let q = json!({
            "queryType": "timeseries", "dataSource": "wiki",
            "intervals": ["2024-01-01/2024-01-02"], "granularity": "all",
            "virtualColumns": [{"type": "expression", "name": "v0",
                                "expression": "concat(\"page\", 'secret')",
                                "outputType": "STRING"}],
            "aggregations": [{"type": "count", "name": "rows"}]
        });
        let shape = native_shape(&q);
        assert!(
            !shape.contains("secret"),
            "expression literal leaked: {shape}"
        );
        assert!(shape.contains("concat"));
    }
}
