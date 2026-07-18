// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! SQL dialect rendering for the multi-backend metadata store.
//!
//! Every query in the store is written ONCE as a backend-neutral template
//! and rendered to concrete backend SQL here.  Template tokens:
//!
//! - `{1}`..`{N}` — bind placeholders, rendered `?N` (SQLite), `$N`
//!   (PostgreSQL) or `?` (MySQL, positional).  Because MySQL placeholders
//!   are positional, [`Rendered::bind_order`] records the argument index
//!   for each placeholder in TEXTUAL order and the execution layer binds
//!   in that order — a template whose tokens are not textually ascending
//!   (e.g. `SET x = {2} WHERE id = {1}`) still binds correctly.
//! - `{start}` / `{end}` — the two reserved column names of
//!   `druid_segments`, rendered bare on SQLite (byte-identical to the
//!   shipped SQL), `"start"`/`"end"` on PostgreSQL (`END` is a reserved
//!   word there) and `` `start` ``/`` `end` `` on MySQL.
//! - `{gen}` — the monotonic generation-ordering column: SQLite's
//!   implicit `rowid`, or the explicit `seq` column (`BIGSERIAL` /
//!   `BIGINT AUTO_INCREMENT`) on PostgreSQL/MySQL.  The Codex R16/R17
//!   "newest generation wins" invariant for rules and supervisors rides
//!   on this column, NOT on wall-clock text (see the store docs).
//!
//! The renderer is intentionally NOT a SQL parser: it is a token
//! substituter over `const` templates, validated by unit tests that
//! snapshot the rendered SQLite SQL against the exact pre-refactor
//! literals (byte-identity for the shipped path).

/// Which database backend a [`Dialect`] renders for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackendKind {
    /// SQLite (the shipped, default backend).
    Sqlite,
    /// PostgreSQL.
    Postgres,
    /// MySQL.
    MySql,
}

/// A rendered SQL statement plus the argument bind order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rendered {
    /// Backend-concrete SQL text.
    pub(crate) sql: String,
    /// 0-based argument indices in the order they must be bound.
    ///
    /// Identity (`0..n`) for SQLite/PostgreSQL (numbered placeholders
    /// select their argument by number); the textual placeholder order
    /// for MySQL (purely positional `?`).
    pub(crate) bind_order: Vec<usize>,
}

/// Renders backend-neutral SQL templates into concrete backend SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Dialect {
    /// Target backend.
    pub(crate) kind: BackendKind,
}

impl Dialect {
    /// The monotonic generation-ordering column (`{gen}` token).
    pub(crate) fn generation_col(self) -> &'static str {
        match self.kind {
            BackendKind::Sqlite => "rowid",
            BackendKind::Postgres | BackendKind::MySql => "seq",
        }
    }

    /// Render `template` for this backend, validating it against the
    /// number of arguments the caller intends to bind.
    ///
    /// Fails loudly (never silently mis-binds) when the template
    /// references an argument outside `1..=arg_count`, when an argument
    /// is never referenced, or when a token is unknown/unterminated.
    pub(crate) fn render(self, template: &str, arg_count: usize) -> Result<Rendered, String> {
        let mut sql = String::with_capacity(template.len() + 8);
        let mut textual: Vec<usize> = Vec::new();
        let mut seen = vec![false; arg_count];

        let mut rest = template;
        while let Some(pos) = rest.find('{') {
            sql.push_str(&rest[..pos]);
            let after = &rest[pos + 1..];
            let Some(close) = after.find('}') else {
                return Err(format!("unterminated '{{' in SQL template: {template}"));
            };
            let token = &after[..close];
            if !token.is_empty() && token.bytes().all(|b| b.is_ascii_digit()) {
                let n: usize = token
                    .parse()
                    .map_err(|e| format!("bad placeholder token '{{{token}}}': {e}"))?;
                if n == 0 || n > arg_count {
                    return Err(format!(
                        "SQL template references arg {{{n}}} but {arg_count} argument(s) \
                         were bound: {template}"
                    ));
                }
                if let Some(s) = seen.get_mut(n - 1) {
                    *s = true;
                }
                textual.push(n - 1);
                match self.kind {
                    BackendKind::Sqlite => {
                        sql.push('?');
                        sql.push_str(token);
                    }
                    BackendKind::Postgres => {
                        sql.push('$');
                        sql.push_str(token);
                    }
                    BackendKind::MySql => sql.push('?'),
                }
            } else {
                match token {
                    "start" | "end" => match self.kind {
                        // Bare on SQLite: byte-identical to the shipped SQL.
                        BackendKind::Sqlite => sql.push_str(token),
                        BackendKind::Postgres => {
                            sql.push('"');
                            sql.push_str(token);
                            sql.push('"');
                        }
                        BackendKind::MySql => {
                            sql.push('`');
                            sql.push_str(token);
                            sql.push('`');
                        }
                    },
                    "gen" => sql.push_str(self.generation_col()),
                    other => {
                        return Err(format!("unknown SQL template token '{{{other}}}'"));
                    }
                }
            }
            rest = &after[close + 1..];
        }
        sql.push_str(rest);

        if let Some(missing) = seen.iter().position(|s| !s) {
            return Err(format!(
                "SQL template never references arg {{{}}} ({arg_count} argument(s) bound): \
                 {template}",
                missing + 1
            ));
        }

        let bind_order = match self.kind {
            BackendKind::MySql => textual,
            BackendKind::Sqlite | BackendKind::Postgres => (0..arg_count).collect(),
        };
        Ok(Rendered { sql, bind_order })
    }

    /// Build an **overwrite-in-place** upsert template for `table`.
    ///
    /// `cols` may contain `{start}`/`{end}` identifier tokens; `pk` is the
    /// conflict target column.  Semantics on every backend: the incoming
    /// row REPLACES the existing row's non-key columns wholesale —
    /// `INSERT OR REPLACE` (SQLite), `INSERT .. ON CONFLICT (pk) DO
    /// UPDATE SET c = EXCLUDED.c` (PostgreSQL), `INSERT .. ON DUPLICATE
    /// KEY UPDATE c = VALUES(c)` (MySQL; `VALUES()` is deprecated-but-
    /// universal — the 8.0.19+ row-alias form is not MariaDB-portable).
    ///
    /// `reset` lists `(column, default-literal)` pairs for columns that
    /// exist in the table's DDL with a DEFAULT but are NOT in `cols`
    /// (compat-6 Codex H3: `druid_segments.partitioned`).  SQLite's
    /// `INSERT OR REPLACE` replaces the WHOLE row, so an omitted column
    /// falls back to its DDL default on every re-upsert — but a
    /// `DO UPDATE` that only touches `cols` would keep the prior row's
    /// STALE value, a silent backend divergence.  The PostgreSQL/MySQL
    /// update clauses therefore also set each `reset` column to its
    /// default literal; the SQLite arm ignores `reset` entirely, keeping
    /// the shipped SQL byte-identical.
    ///
    /// Use ONLY where overwrite-in-place is the intended semantics
    /// (segments, task rows, config).  `set_rules` must NOT use this: it
    /// needs fresh-generation semantics (a new `{gen}` position), which
    /// `DO UPDATE` would silently break — see `MetadataStore::set_rules`.
    pub(crate) fn upsert(
        self,
        table: &str,
        cols: &[&str],
        pk: &str,
        reset: &[(&str, &str)],
    ) -> String {
        let col_list = cols.join(", ");
        let values = (1..=cols.len())
            .map(|i| format!("{{{i}}}"))
            .collect::<Vec<_>>()
            .join(", ");
        match self.kind {
            BackendKind::Sqlite => {
                format!("INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({values})")
            }
            BackendKind::Postgres => {
                let sets = cols
                    .iter()
                    .filter(|c| **c != pk)
                    .map(|c| format!("{c} = EXCLUDED.{c}"))
                    .chain(reset.iter().map(|(c, dflt)| format!("{c} = {dflt}")))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "INSERT INTO {table} ({col_list}) VALUES ({values}) \
                     ON CONFLICT ({pk}) DO UPDATE SET {sets}"
                )
            }
            BackendKind::MySql => {
                let sets = cols
                    .iter()
                    .filter(|c| **c != pk)
                    .map(|c| format!("{c} = VALUES({c})"))
                    .chain(reset.iter().map(|(c, dflt)| format!("{c} = {dflt}")))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "INSERT INTO {table} ({col_list}) VALUES ({values}) \
                     ON DUPLICATE KEY UPDATE {sets}"
                )
            }
        }
    }
}
