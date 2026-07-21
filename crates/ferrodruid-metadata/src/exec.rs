// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Execution layer for the multi-backend metadata store.
//!
//! [`Backend`] owns one concrete `sqlx` pool per supported database and
//! exposes a tiny template-driven API (`execute` / `fetch_all` /
//! `fetch_optional` / `begin`; id-returning inserts live on the
//! transaction handle, [`StoreTxn::execute_returning_id`]).  The 3-arm
//! backend `match` lives HERE, once per helper — not in the ~40 store
//! methods.  Row decoding is normalized through [`MetaRow`], so
//! backend-specific decode quirks (MySQL `TINYINT(1)` booleans,
//! PostgreSQL lower-case identifier folding) are handled in exactly one
//! place.

use crate::dialect::{BackendKind, Dialect};
use sqlx::mysql::MySqlRow;
use sqlx::postgres::PgRow;
use sqlx::query::Query;
use sqlx::sqlite::SqliteRow;
use sqlx::{MySql, MySqlPool, PgPool, Postgres, Row, Sqlite, SqlitePool};

// ---------------------------------------------------------------------------
// Arguments
// ---------------------------------------------------------------------------

/// A bind argument.  The store only ever binds these four shapes.
#[derive(Debug, Clone)]
pub(crate) enum Arg {
    /// A non-null string.
    Str(String),
    /// A nullable string.
    OptStr(Option<String>),
    /// A boolean (`BOOLEAN` / `BOOL` / `TINYINT(1)`).
    Bool(bool),
    /// A 64-bit signed integer.
    I64(i64),
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// An execution-layer error.  Store methods wrap this with call-site
/// context (`"insert segment: {e}"` …), exactly as they wrapped the raw
/// `sqlx::Error` before the multi-backend refactor — the `Db` variant
/// displays the underlying `sqlx` error verbatim so error text on the
/// shipped SQLite path is unchanged.
#[derive(Debug)]
pub(crate) enum ExecError {
    /// A template failed to render (programming error, fails loudly).
    Template(String),
    /// The database rejected the statement.
    Db(sqlx::Error),
    /// Anything else (e.g. an id that overflows `i64`).
    Other(String),
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::Template(m) | ExecError::Other(m) => f.write_str(m),
            ExecError::Db(e) => write!(f, "{e}"),
        }
    }
}

impl From<sqlx::Error> for ExecError {
    fn from(e: sqlx::Error) -> Self {
        ExecError::Db(e)
    }
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// An owned result row, decoupled from the backend that produced it.
///
/// The typed getters are the ONLY decode path in the store; each arm
/// normalizes its backend's quirks:
///
/// - **MySQL**: `BOOLEAN` columns are `TINYINT(1)` on disk — `sqlx`
///   decodes them to `bool` here, so `used`/`revoked`/`partitioned`
///   round-trip as real booleans (the silent-safety-bug class the
///   survey flagged).
/// - **PostgreSQL**: unquoted identifiers fold to lower-case, so a
///   `SELECT dataSource ..` result column is named `datasource`; the
///   getters fold the requested name for PostgreSQL so store code can
///   keep using the Druid-canonical `dataSource` spelling everywhere.
pub(crate) enum MetaRow {
    /// A SQLite row.
    Sqlite(SqliteRow),
    /// A PostgreSQL row.
    Postgres(PgRow),
    /// A MySQL row.
    MySql(MySqlRow),
}

impl MetaRow {
    /// PostgreSQL folds unquoted identifiers to lower-case.
    fn pg_name(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    /// Decode a non-null string column.
    pub(crate) fn str(&self, name: &str) -> Result<String, sqlx::Error> {
        match self {
            MetaRow::Sqlite(r) => r.try_get(name),
            MetaRow::Postgres(r) => r.try_get(Self::pg_name(name).as_str()),
            MetaRow::MySql(r) => r.try_get(name),
        }
    }

    /// Decode a nullable string column.
    pub(crate) fn opt_str(&self, name: &str) -> Result<Option<String>, sqlx::Error> {
        match self {
            MetaRow::Sqlite(r) => r.try_get(name),
            MetaRow::Postgres(r) => r.try_get(Self::pg_name(name).as_str()),
            MetaRow::MySql(r) => r.try_get(name),
        }
    }

    /// Decode a boolean column (MySQL `TINYINT(1)` normalized to `bool`).
    pub(crate) fn bool(&self, name: &str) -> Result<bool, sqlx::Error> {
        match self {
            MetaRow::Sqlite(r) => r.try_get(name),
            MetaRow::Postgres(r) => r.try_get(Self::pg_name(name).as_str()),
            MetaRow::MySql(r) => r.try_get(name),
        }
    }

    /// Decode a 64-bit integer column.
    pub(crate) fn i64(&self, name: &str) -> Result<i64, sqlx::Error> {
        match self {
            MetaRow::Sqlite(r) => r.try_get(name),
            MetaRow::Postgres(r) => r.try_get(Self::pg_name(name).as_str()),
            MetaRow::MySql(r) => r.try_get(name),
        }
    }
}

// ---------------------------------------------------------------------------
// Query building
// ---------------------------------------------------------------------------

fn build_sqlite<'q>(
    sql: &'q str,
    args: &'q [Arg],
) -> Query<'q, Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    let mut q = sqlx::query(sql);
    for a in args {
        q = match a {
            Arg::Str(s) => q.bind(s.as_str()),
            Arg::OptStr(s) => q.bind(s.as_deref()),
            Arg::Bool(b) => q.bind(*b),
            Arg::I64(i) => q.bind(*i),
        };
    }
    q
}

fn build_pg<'q>(sql: &'q str, args: &'q [Arg]) -> Query<'q, Postgres, sqlx::postgres::PgArguments> {
    let mut q = sqlx::query(sql);
    for a in args {
        q = match a {
            Arg::Str(s) => q.bind(s.as_str()),
            Arg::OptStr(s) => q.bind(s.as_deref()),
            Arg::Bool(b) => q.bind(*b),
            Arg::I64(i) => q.bind(*i),
        };
    }
    q
}

fn build_mysql<'q>(sql: &'q str, args: &'q [Arg]) -> Query<'q, MySql, sqlx::mysql::MySqlArguments> {
    let mut q = sqlx::query(sql);
    for a in args {
        q = match a {
            Arg::Str(s) => q.bind(s.as_str()),
            Arg::OptStr(s) => q.bind(s.as_deref()),
            Arg::Bool(b) => q.bind(*b),
            Arg::I64(i) => q.bind(*i),
        };
    }
    q
}

/// Render `template` and clone the arguments into final bind order.
///
/// The returned argument vector is already permuted per
/// [`crate::dialect::Rendered::bind_order`], so every backend binds it
/// sequentially (this is what makes MySQL's positional `?` safe for
/// templates whose numbered tokens are not textually ascending).
fn prepare(
    dialect: Dialect,
    template: &str,
    args: &[Arg],
) -> Result<(String, Vec<Arg>), ExecError> {
    let rendered = dialect
        .render(template, args.len())
        .map_err(ExecError::Template)?;
    let mut ordered = Vec::with_capacity(rendered.bind_order.len());
    for &i in &rendered.bind_order {
        let a = args.get(i).ok_or_else(|| {
            ExecError::Template(format!("bind index {i} out of range: {template}"))
        })?;
        ordered.push(a.clone());
    }
    Ok((rendered.sql, ordered))
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

/// The concrete database backend of a `MetadataStore`.
pub(crate) enum Backend {
    /// SQLite (file or in-memory).
    Sqlite(SqlitePool),
    /// PostgreSQL.
    Postgres(PgPool),
    /// MySQL.
    MySql(MySqlPool),
}

impl Backend {
    /// The dialect kind of this backend.
    pub(crate) fn kind(&self) -> BackendKind {
        match self {
            Backend::Sqlite(_) => BackendKind::Sqlite,
            Backend::Postgres(_) => BackendKind::Postgres,
            Backend::MySql(_) => BackendKind::MySql,
        }
    }

    /// The dialect used to render templates for this backend.
    pub(crate) fn dialect(&self) -> Dialect {
        Dialect { kind: self.kind() }
    }

    /// Execute a raw, backend-final SQL statement (DDL only — templates
    /// go through [`Backend::execute`]).
    pub(crate) async fn execute_raw(&self, sql: &str) -> Result<(), ExecError> {
        match self {
            Backend::Sqlite(pool) => {
                sqlx::query(sql).execute(pool).await?;
            }
            Backend::Postgres(pool) => {
                sqlx::query(sql).execute(pool).await?;
            }
            Backend::MySql(pool) => {
                sqlx::query(sql).execute(pool).await?;
            }
        }
        Ok(())
    }

    /// Render + execute a template statement.
    pub(crate) async fn execute(&self, template: &str, args: &[Arg]) -> Result<(), ExecError> {
        let (sql, ordered) = prepare(self.dialect(), template, args)?;
        match self {
            Backend::Sqlite(pool) => {
                build_sqlite(&sql, &ordered).execute(pool).await?;
            }
            Backend::Postgres(pool) => {
                build_pg(&sql, &ordered).execute(pool).await?;
            }
            Backend::MySql(pool) => {
                build_mysql(&sql, &ordered).execute(pool).await?;
            }
        }
        Ok(())
    }

    /// Render + run a template query, returning every row.
    pub(crate) async fn fetch_all(
        &self,
        template: &str,
        args: &[Arg],
    ) -> Result<Vec<MetaRow>, ExecError> {
        let (sql, ordered) = prepare(self.dialect(), template, args)?;
        Ok(match self {
            Backend::Sqlite(pool) => build_sqlite(&sql, &ordered)
                .fetch_all(pool)
                .await?
                .into_iter()
                .map(MetaRow::Sqlite)
                .collect(),
            Backend::Postgres(pool) => build_pg(&sql, &ordered)
                .fetch_all(pool)
                .await?
                .into_iter()
                .map(MetaRow::Postgres)
                .collect(),
            Backend::MySql(pool) => build_mysql(&sql, &ordered)
                .fetch_all(pool)
                .await?
                .into_iter()
                .map(MetaRow::MySql)
                .collect(),
        })
    }

    /// Render + run a template query, returning at most one row.
    pub(crate) async fn fetch_optional(
        &self,
        template: &str,
        args: &[Arg],
    ) -> Result<Option<MetaRow>, ExecError> {
        let (sql, ordered) = prepare(self.dialect(), template, args)?;
        Ok(match self {
            Backend::Sqlite(pool) => build_sqlite(&sql, &ordered)
                .fetch_optional(pool)
                .await?
                .map(MetaRow::Sqlite),
            Backend::Postgres(pool) => build_pg(&sql, &ordered)
                .fetch_optional(pool)
                .await?
                .map(MetaRow::Postgres),
            Backend::MySql(pool) => build_mysql(&sql, &ordered)
                .fetch_optional(pool)
                .await?
                .map(MetaRow::MySql),
        })
    }

    /// Begin an explicit transaction.
    pub(crate) async fn begin(&self) -> Result<StoreTxn, ExecError> {
        let dialect = self.dialect();
        let inner = match self {
            Backend::Sqlite(pool) => TxnInner::Sqlite(pool.begin().await?),
            Backend::Postgres(pool) => TxnInner::Postgres(pool.begin().await?),
            Backend::MySql(pool) => TxnInner::MySql(pool.begin().await?),
        };
        Ok(StoreTxn { dialect, inner })
    }
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

enum TxnInner {
    Sqlite(sqlx::Transaction<'static, Sqlite>),
    Postgres(sqlx::Transaction<'static, Postgres>),
    MySql(sqlx::Transaction<'static, MySql>),
}

/// An explicit database transaction over the store's backend.
///
/// Dropped without [`StoreTxn::commit`] → rolled back (sqlx semantics on
/// every backend).
pub(crate) struct StoreTxn {
    dialect: Dialect,
    inner: TxnInner,
}

impl StoreTxn {
    /// Render + execute a template statement inside the transaction.
    pub(crate) async fn execute(&mut self, template: &str, args: &[Arg]) -> Result<(), ExecError> {
        let (sql, ordered) = prepare(self.dialect, template, args)?;
        match &mut self.inner {
            TxnInner::Sqlite(t) => {
                build_sqlite(&sql, &ordered).execute(&mut **t).await?;
            }
            TxnInner::Postgres(t) => {
                build_pg(&sql, &ordered).execute(&mut **t).await?;
            }
            TxnInner::MySql(t) => {
                build_mysql(&sql, &ordered).execute(&mut **t).await?;
            }
        }
        Ok(())
    }

    /// Render + execute an `INSERT` template inside the transaction and
    /// return the database-generated integer id of the inserted row.
    ///
    /// Per backend: SQLite `last_insert_rowid()`; PostgreSQL appends
    /// ` RETURNING id` and fetches it; MySQL `LAST_INSERT_ID()` — each
    /// scoped to THIS transaction's connection.
    pub(crate) async fn execute_returning_id(
        &mut self,
        template: &str,
        args: &[Arg],
    ) -> Result<i64, ExecError> {
        let (sql, ordered) = prepare(self.dialect, template, args)?;
        match &mut self.inner {
            TxnInner::Sqlite(t) => Ok(build_sqlite(&sql, &ordered)
                .execute(&mut **t)
                .await?
                .last_insert_rowid()),
            TxnInner::Postgres(t) => {
                let sql = format!("{sql} RETURNING id");
                let row = build_pg(&sql, &ordered).fetch_one(&mut **t).await?;
                row.try_get::<i64, _>("id").map_err(ExecError::Db)
            }
            TxnInner::MySql(t) => {
                let id = build_mysql(&sql, &ordered)
                    .execute(&mut **t)
                    .await?
                    .last_insert_id();
                i64::try_from(id)
                    .map_err(|_| ExecError::Other(format!("last_insert_id {id} overflows i64")))
            }
        }
    }

    /// Commit the transaction.
    pub(crate) async fn commit(self) -> Result<(), ExecError> {
        match self.inner {
            TxnInner::Sqlite(t) => t.commit().await?,
            TxnInner::Postgres(t) => t.commit().await?,
            TxnInner::MySql(t) => t.commit().await?,
        }
        Ok(())
    }
}
