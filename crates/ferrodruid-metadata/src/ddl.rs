// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Per-backend schema DDL.
//!
//! Three explicit, greppable statement lists (NOT a rendered template):
//! DDL divergence between the backends is structural (auto-increment
//! spelling, key-length limits, generation columns), so per-backend
//! constants are the honest representation.
//!
//! Cross-backend invariants:
//!
//! - Every statement is `CREATE TABLE IF NOT EXISTS` / additive, so
//!   re-running [`schema_statements`] against an existing database is a
//!   no-op and never drops data (there is NO migration machinery).
//! - Payloads and timestamps stay TEXT everywhere (RFC-3339 text
//!   ordering is already correct; native JSON types would re-order keys
//!   and break the `serde_json::from_str` decode path).
//! - **SQLite keeps the exact shipped schema** — no `seq` column is
//!   added (existing on-disk databases would not get it from
//!   `CREATE TABLE IF NOT EXISTS` anyway); generation ordering uses the
//!   implicit `rowid`.
//! - PostgreSQL/MySQL add an explicit monotonic `seq` column
//!   (`BIGSERIAL` / `BIGINT AUTO_INCREMENT`) to `druid_rules` and
//!   `druid_supervisors` — those backends have no rowid, and the Codex
//!   R16/R17 "newest generation wins" ordering must NOT fall back to
//!   wall-clock text (clock steps would resurrect stopped supervisors /
//!   serve stale rules).
//! - MySQL: indexed TEXT is not allowed, so key columns become
//!   `VARCHAR` (primary keys `VARCHAR(512)` = 2048 utf8mb4 bytes, under
//!   the 3072-byte InnoDB key cap, deliberate headroom over Druid's own
//!   255); payload columns are `LONGTEXT`; secondary indexes are inline
//!   `KEY` clauses because MySQL has no `CREATE INDEX IF NOT EXISTS`.
//! - MySQL identity columns pin `CHARACTER SET utf8mb4 COLLATE
//!   utf8mb4_0900_as_cs` (compat-6 Codex H1, R3): stock MySQL 8.0
//!   defaults to the case- and accent-INSENSITIVE `utf8mb4_0900_ai_ci`,
//!   under which case-distinct config names, segment/task/supervisor ids
//!   and datasource lookups collide onto one row — silent metadata
//!   corruption — while SQLite and PostgreSQL compare bytes.
//!   `utf8mb4_0900_as_cs` is case- AND accent-SENSITIVE, so it
//!   distinguishes `Foo`/`foo` and every real Druid id exactly as
//!   SQLite/PostgreSQL do.
//!
//!   Why NOT `utf8mb4_bin` (the byte-exact collation): a binary
//!   collation makes MySQL report the column over the wire as
//!   **VARBINARY**, so `sqlx`'s `String` (`VARCHAR`) decode of every
//!   identity column fails at runtime (`Rust type String … is not
//!   compatible with SQL type VARBINARY`) — it broke the whole MySQL
//!   suite (R2). `as_cs` stays a TEXT collation, so the columns keep
//!   decoding as `VARCHAR`/`String`.
//!
//!   Accepted residual: `as_cs` follows the UCA, so two ids that differ
//!   only in a UCA-*ignorable* codepoint (e.g. a zero-width U+200B)
//!   would still compare equal. This is unreachable for real Druid
//!   identities — datasource names are ASCII, versions/ids are ISO-8601
//!   timestamps and partition suffixes, none of which carry zero-width
//!   or combining marks — so it is documented and accepted rather than
//!   forcing `_bin` (which would reintroduce the VARBINARY decode
//!   break). `utf8mb4_0900_*` is MySQL-8-only (not MariaDB-portable),
//!   the same portability line as `VALUES()` in the upsert builder.
//!
//!   Applied to every textual PK and every column the store compares
//!   identities against (`dataSource`/`datasource`, `spec_id`,
//!   `task_id`, `audit_key`); pure payload/timestamp columns and the
//!   `status` enum (only ever compared against fixed SCREAMING_SNAKE
//!   literals) keep the server default.

use crate::dialect::BackendKind;

/// The SQLite schema — byte-for-byte the shipped Phase-1 statements
/// (split one statement per string so every backend initializes
/// statement-by-statement).
const SQLITE_SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS druid_segments (
    id TEXT PRIMARY KEY,
    dataSource TEXT NOT NULL,
    created_date TEXT NOT NULL,
    start TEXT NOT NULL,
    end TEXT NOT NULL,
    partitioned BOOLEAN NOT NULL DEFAULT FALSE,
    version TEXT NOT NULL,
    used BOOLEAN NOT NULL DEFAULT TRUE,
    payload TEXT NOT NULL,
    used_status_last_updated TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_rules (
    id TEXT PRIMARY KEY,
    dataSource TEXT NOT NULL,
    version TEXT NOT NULL,
    payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_supervisors (
    id TEXT PRIMARY KEY,
    spec_id TEXT NOT NULL,
    created_date TEXT NOT NULL,
    payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_config (
    name TEXT PRIMARY KEY,
    payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    audit_key TEXT NOT NULL,
    type TEXT NOT NULL,
    author TEXT,
    comment TEXT,
    created_date TEXT NOT NULL,
    payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_tasklogs (
    id TEXT PRIMARY KEY,
    created_date TEXT NOT NULL,
    datasource TEXT,
    payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_tasklocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    lock_payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_task_status (
        id TEXT PRIMARY KEY,
        task_type TEXT NOT NULL,
        datasource TEXT NOT NULL,
        status TEXT NOT NULL,
        created_date TEXT NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 0,
        worker TEXT,
        payload TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS druid_task_lock_detail (
        id INTEGER PRIMARY KEY,
        datasource TEXT NOT NULL,
        interval_start TEXT NOT NULL,
        interval_end TEXT NOT NULL,
        lock_type TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        revoked BOOLEAN NOT NULL DEFAULT FALSE
    )",
    "CREATE INDEX IF NOT EXISTS idx_task_status_active
        ON druid_task_status (status)",
    "CREATE INDEX IF NOT EXISTS idx_task_lock_ds
        ON druid_task_lock_detail (datasource, revoked)",
];

/// The PostgreSQL schema.  Integer columns are BIGINT/BIGSERIAL across
/// the board (the store decodes `i64`; PostgreSQL will not widen INT4).
/// `"start"`/`"end"` are quoted (`END` is reserved); every other
/// identifier is left unquoted and folds to lower-case — the row decode
/// layer folds lookups to match.
const PG_SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS druid_segments (
    id TEXT PRIMARY KEY,
    dataSource TEXT NOT NULL,
    created_date TEXT NOT NULL,
    \"start\" TEXT NOT NULL,
    \"end\" TEXT NOT NULL,
    partitioned BOOLEAN NOT NULL DEFAULT FALSE,
    version TEXT NOT NULL,
    used BOOLEAN NOT NULL DEFAULT TRUE,
    payload TEXT NOT NULL,
    used_status_last_updated TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_rules (
    id TEXT PRIMARY KEY,
    dataSource TEXT NOT NULL,
    version TEXT NOT NULL,
    payload TEXT NOT NULL,
    seq BIGSERIAL
)",
    "CREATE INDEX IF NOT EXISTS idx_rules_ds_seq ON druid_rules (dataSource, seq)",
    "CREATE TABLE IF NOT EXISTS druid_supervisors (
    id TEXT PRIMARY KEY,
    spec_id TEXT NOT NULL,
    created_date TEXT NOT NULL,
    payload TEXT NOT NULL,
    seq BIGSERIAL
)",
    "CREATE INDEX IF NOT EXISTS idx_supervisors_spec_seq ON druid_supervisors (spec_id, seq)",
    "CREATE TABLE IF NOT EXISTS druid_config (
    name TEXT PRIMARY KEY,
    payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_audit (
    id BIGSERIAL PRIMARY KEY,
    audit_key TEXT NOT NULL,
    type TEXT NOT NULL,
    author TEXT,
    comment TEXT,
    created_date TEXT NOT NULL,
    payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_tasklogs (
    id TEXT PRIMARY KEY,
    created_date TEXT NOT NULL,
    datasource TEXT,
    payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_tasklocks (
    id BIGSERIAL PRIMARY KEY,
    task_id TEXT NOT NULL,
    lock_payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_task_status (
    id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    datasource TEXT NOT NULL,
    status TEXT NOT NULL,
    created_date TEXT NOT NULL,
    attempt BIGINT NOT NULL DEFAULT 0,
    worker TEXT,
    payload TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_task_lock_detail (
    id BIGINT PRIMARY KEY,
    datasource TEXT NOT NULL,
    interval_start TEXT NOT NULL,
    interval_end TEXT NOT NULL,
    lock_type TEXT NOT NULL,
    priority BIGINT NOT NULL DEFAULT 0,
    revoked BOOLEAN NOT NULL DEFAULT FALSE
)",
    "CREATE INDEX IF NOT EXISTS idx_task_status_active ON druid_task_status (status)",
    "CREATE INDEX IF NOT EXISTS idx_task_lock_ds ON druid_task_lock_detail (datasource, revoked)",
];

/// The MySQL schema.  `BOOLEAN` is the `TINYINT(1)` alias (normalized
/// back to `bool` in the row decode layer); the `seq` AUTO_INCREMENT
/// columns carry their own `KEY` (MySQL requires an AUTO_INCREMENT
/// column to head an index, and it need not be the primary key).
const MYSQL_SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS druid_segments (
    id VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs PRIMARY KEY,
    dataSource VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
    created_date TEXT NOT NULL,
    `start` TEXT NOT NULL,
    `end` TEXT NOT NULL,
    partitioned BOOLEAN NOT NULL DEFAULT FALSE,
    version TEXT NOT NULL,
    used BOOLEAN NOT NULL DEFAULT TRUE,
    payload LONGTEXT NOT NULL,
    used_status_last_updated TEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_rules (
    id VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs PRIMARY KEY,
    dataSource VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
    version TEXT NOT NULL,
    payload LONGTEXT NOT NULL,
    seq BIGINT NOT NULL AUTO_INCREMENT,
    KEY idx_rules_seq (seq),
    KEY idx_rules_ds_seq (dataSource, seq)
)",
    "CREATE TABLE IF NOT EXISTS druid_supervisors (
    id VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs PRIMARY KEY,
    spec_id VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
    created_date TEXT NOT NULL,
    payload LONGTEXT NOT NULL,
    seq BIGINT NOT NULL AUTO_INCREMENT,
    KEY idx_supervisors_seq (seq),
    KEY idx_supervisors_spec_seq (spec_id, seq)
)",
    "CREATE TABLE IF NOT EXISTS druid_config (
    name VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs PRIMARY KEY,
    payload LONGTEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_audit (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    audit_key TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
    type TEXT NOT NULL,
    author TEXT,
    comment TEXT,
    created_date TEXT NOT NULL,
    payload LONGTEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_tasklogs (
    id VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs PRIMARY KEY,
    created_date TEXT NOT NULL,
    datasource TEXT,
    payload LONGTEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_tasklocks (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
    lock_payload LONGTEXT NOT NULL
)",
    "CREATE TABLE IF NOT EXISTS druid_task_status (
    id VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs PRIMARY KEY,
    task_type TEXT NOT NULL,
    datasource TEXT NOT NULL,
    status VARCHAR(64) NOT NULL,
    created_date TEXT NOT NULL,
    attempt BIGINT NOT NULL DEFAULT 0,
    worker TEXT,
    payload LONGTEXT NOT NULL,
    KEY idx_task_status_active (status)
)",
    "CREATE TABLE IF NOT EXISTS druid_task_lock_detail (
    id BIGINT PRIMARY KEY,
    datasource VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
    interval_start TEXT NOT NULL,
    interval_end TEXT NOT NULL,
    lock_type TEXT NOT NULL,
    priority BIGINT NOT NULL DEFAULT 0,
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    KEY idx_task_lock_ds (datasource, revoked)
)",
];

/// The `CREATE TABLE IF NOT EXISTS` statement list for a backend, to be
/// executed one statement at a time (MySQL rejects multi-statement
/// prepared queries; per-statement execution is uniform and costs
/// nothing on the others).
pub(crate) fn schema_statements(kind: BackendKind) -> &'static [&'static str] {
    match kind {
        BackendKind::Sqlite => SQLITE_SCHEMA,
        BackendKind::Postgres => PG_SCHEMA,
        BackendKind::MySql => MYSQL_SCHEMA,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// compat-6 Codex H1 guard: stock `mysql:8.0` databases default to
    /// the case- and accent-INSENSITIVE `utf8mb4_0900_ai_ci` collation,
    /// under which `set_config("Foo")` / `set_config("foo")` — and any
    /// case-distinct segment/task/lock/supervisor ids — silently collide
    /// onto ONE row, while SQLite and PostgreSQL compare bytes.  Every
    /// textual identity column in the MySQL DDL must therefore pin the
    /// case- and accent-SENSITIVE `utf8mb4_0900_as_cs` collation (NOT
    /// the byte-exact `_bin`, which the wire protocol reports as
    /// VARBINARY and breaks the `String` decode — compat-6 R2/R3). The
    /// behavioral halves run against a real MySQL server in the
    /// env-gated `mysql_store_suite`.
    #[test]
    fn mysql_identity_columns_pin_case_sensitive_collation() {
        const CS: &str = "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs";
        // Every textual PRIMARY KEY plus every textual column the store
        // compares identities against (`WHERE`/`DISTINCT`/join keys).
        let identity_cols: &[(&str, &[&str])] = &[
            ("druid_segments", &["id", "dataSource"]),
            ("druid_rules", &["id", "dataSource"]),
            ("druid_supervisors", &["id", "spec_id"]),
            ("druid_config", &["name"]),
            ("druid_audit", &["audit_key"]),
            ("druid_tasklogs", &["id"]),
            ("druid_tasklocks", &["task_id"]),
            ("druid_task_status", &["id"]),
            ("druid_task_lock_detail", &["datasource"]),
        ];
        for (table, cols) in identity_cols {
            let stmt = MYSQL_SCHEMA
                .iter()
                .find(|s| s.contains(&format!("EXISTS {table} (")))
                .unwrap_or_else(|| panic!("MySQL DDL for table {table} not found"));
            for col in *cols {
                let line = stmt
                    .lines()
                    .map(str::trim_start)
                    .find(|l| l.starts_with(&format!("{col} ")))
                    .unwrap_or_else(|| panic!("column {col} not found in MySQL {table} DDL"));
                assert!(
                    line.contains(CS),
                    "MySQL identity column {table}.{col} must pin '{CS}' \
                     (stock utf8mb4_0900_ai_ci collides case-distinct ids); got: {line}"
                );
            }
        }
        // Guard against a `_bin` relapse: a binary collation is reported
        // as VARBINARY on the wire and breaks the `String` decode.
        for stmt in MYSQL_SCHEMA.iter() {
            assert!(
                !stmt.contains("utf8mb4_bin"),
                "MySQL identity columns must NOT use utf8mb4_bin \
                 (VARBINARY decode break); use utf8mb4_0900_as_cs: {stmt}"
            );
        }
        // SQLite and PostgreSQL already compare bytes — their DDL must
        // NOT grow collation clauses (SQLite stays the exact shipped
        // schema; see the module invariants).
        for stmt in SQLITE_SCHEMA.iter().chain(PG_SCHEMA.iter()) {
            assert!(
                !stmt.contains("COLLATE"),
                "unexpected COLLATE outside the MySQL DDL: {stmt}"
            );
        }
    }
}
