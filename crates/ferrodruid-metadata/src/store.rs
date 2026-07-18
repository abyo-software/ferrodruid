// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Core metadata store implementation (SQLite default, PostgreSQL and
//! MySQL backends via `MetadataStore::connect`).

use crate::ddl;
use crate::dialect::{BackendKind, Dialect};
use crate::exec::{Arg, Backend, MetaRow};
use crate::uri::{MetadataUri, parse_metadata_uri};
use ferrodruid_common::{DruidError, Result};
use serde::{Deserialize, Serialize};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

/// A row in the `druid_segments` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadataRow {
    /// Segment identifier (data-source + interval + version + partition).
    pub id: String,
    /// Data source name.
    pub data_source: String,
    /// ISO-8601 creation timestamp.
    pub created_date: String,
    /// ISO-8601 interval start.
    pub start: String,
    /// ISO-8601 interval end.
    pub end: String,
    /// Version string (typically ISO-8601).
    pub version: String,
    /// Whether this segment is currently in use.
    pub used: bool,
    /// Full JSON payload with segment details.
    pub payload: serde_json::Value,
}

/// A row in the `druid_supervisors` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorRow {
    /// Auto-generated row identifier.
    pub id: String,
    /// Supervisor spec identifier.
    pub spec_id: String,
    /// ISO-8601 creation timestamp.
    pub created_date: String,
    /// Full JSON supervisor spec.
    pub payload: serde_json::Value,
}

/// A persisted ingestion-task row, stored in `druid_tasklogs`.
///
/// This mirrors the lifecycle state the Overlord tracks for a task; the
/// `payload` column carries the full JSON status object while the scalar
/// columns make active/terminal queries cheap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRow {
    /// Unique task identifier.
    pub id: String,
    /// Task type (e.g. `"index_parallel"`, `"index_kafka"`).
    pub task_type: String,
    /// Target data source name.
    pub data_source: String,
    /// Current task status, as a `SCREAMING_SNAKE_CASE` Druid `TaskState`
    /// string (`WAITING`, `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`).
    pub status: String,
    /// ISO-8601 creation timestamp.
    pub created_date: String,
    /// Number of execution attempts made so far.
    pub attempt: i64,
    /// Worker that the task is currently assigned to, if any.
    pub worker: Option<String>,
    /// Full JSON payload with task status detail.
    pub payload: serde_json::Value,
}

/// A persisted task lock row, stored in `druid_tasklocks`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLockRow {
    /// Auto-generated row identifier (string form of the rowid).
    pub id: String,
    /// Task that owns the lock.
    pub task_id: String,
    /// Data source the lock applies to.
    pub data_source: String,
    /// ISO-8601 interval start (inclusive).
    pub interval_start: String,
    /// ISO-8601 interval end (exclusive).
    pub interval_end: String,
    /// Lock type, `"SHARED"` or `"EXCLUSIVE"`.
    pub lock_type: String,
    /// Lock priority; higher wins on contention.
    pub priority: i64,
    /// Whether the lock has been revoked (preempted).
    pub revoked: bool,
    /// Full JSON lock payload.
    pub payload: serde_json::Value,
}

// ---------------------------------------------------------------------------
// SQL templates (see `crate::dialect` for the token language)
// ---------------------------------------------------------------------------

/// Columns of a full `druid_segments` row write, in bind order.  Shared
/// by the overwrite upsert (insert_segment / rollback restore) and the
/// fail-closed plain insert (replace txn).
const SEGMENT_COLS: &[&str] = &[
    "id",
    "dataSource",
    "created_date",
    "{start}",
    "{end}",
    "version",
    "used",
    "payload",
    "used_status_last_updated",
];

/// `druid_segments` columns OMITTED from [`SEGMENT_COLS`] that carry a
/// literal DDL DEFAULT, as `(column, default)` reset pairs for
/// [`Dialect::upsert`] (compat-6 Codex H3).  SQLite's full-row
/// `INSERT OR REPLACE` implicitly resets an omitted column to its DDL
/// default on every re-upsert; without an explicit reset, the
/// PostgreSQL/MySQL `DO UPDATE` clauses would keep the prior row's
/// STALE value — a silent backend divergence.  `partitioned` is a
/// vestigial Druid-shape column FerroDruid itself never reads or
/// writes (grep-verified: DDL-only).
const SEGMENT_RESET_COLS: &[(&str, &str)] = &[("partitioned", "FALSE")];

const T_GET_SEGMENT: &str = "SELECT id, dataSource, created_date, {start}, {end}, version, \
     used, payload FROM druid_segments WHERE id = {1}";
const T_SEGMENT_EXISTS: &str = "SELECT 1 FROM druid_segments WHERE id = {1} LIMIT 1";
const T_MARK_SEGMENT_UNUSED: &str =
    "UPDATE druid_segments SET used = FALSE, used_status_last_updated = {1} WHERE id = {2}";
/// The publication-critical-section insert. Deliberately a PLAIN INSERT:
/// a pre-existing row with the same id must ABORT the transaction
/// (fail closed) instead of being overwritten (Codex 2026-07-12 round-2
/// HIGH #4). **Do NOT add `ON CONFLICT` (PostgreSQL) or `ON DUPLICATE
/// KEY UPDATE` (MySQL) here — on every backend the primary-key
/// violation IS the safety mechanism.**
const T_INSERT_SEGMENT_PLAIN: &str = "INSERT INTO druid_segments \
     (id, dataSource, created_date, {start}, {end}, version, used, payload, used_status_last_updated) \
     VALUES ({1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})";
const T_DELETE_SEGMENT: &str = "DELETE FROM druid_segments WHERE id = {1}";
const T_GET_USED_SEGMENTS: &str = "SELECT id, dataSource, created_date, {start}, {end}, version, \
     used, payload FROM druid_segments WHERE dataSource = {1} AND used = TRUE";
const T_GET_USED_SEGMENTS_ALL: &str = "SELECT id, dataSource, created_date, {start}, {end}, \
     version, used, payload FROM druid_segments WHERE used = TRUE ORDER BY dataSource, {start}";
const T_SET_DATASOURCE_USED: &str = "UPDATE druid_segments SET used = {1}, \
     used_status_last_updated = {2} WHERE dataSource = {3}";
const T_GET_ALL_DATA_SOURCES: &str =
    "SELECT DISTINCT dataSource FROM druid_segments ORDER BY dataSource";
const T_GET_ALL_SEGMENTS: &str = "SELECT id, dataSource, created_date, {start}, {end}, version, \
     used, payload FROM druid_segments ORDER BY dataSource, {start}";

const T_GET_RULES: &str =
    "SELECT payload FROM druid_rules WHERE dataSource = {1} ORDER BY {gen} DESC LIMIT 1";
/// PostgreSQL/MySQL ONLY (`set_rules`): clears the exact colliding
/// primary key — deliberately `id`, never the datasource, so prior
/// rule generations are NOT wiped — before the fresh-generation
/// re-insert. SQLite does not execute this: it keeps the shipped
/// single-statement [`T_UPSERT_RULES_ROW_SQLITE`].
const T_DELETE_RULES_ROW: &str = "DELETE FROM druid_rules WHERE id = {1}";
const T_INSERT_RULES_ROW: &str =
    "INSERT INTO druid_rules (id, dataSource, version, payload) VALUES ({1}, {2}, {3}, {4})";
/// SQLite ONLY (`set_rules`): the shipped single-statement rules write,
/// byte-identical to the pre-multi-backend SQL. `INSERT OR REPLACE`
/// implicitly deletes-then-inserts on a PK collision, so the row takes
/// a fresh `rowid` (`druid_rules` is not `WITHOUT ROWID`) and the
/// newest-generation-wins ordering of `get_rules` holds. Never executed
/// on PostgreSQL/MySQL — they have no `OR REPLACE`; see
/// [`MetadataStore::set_rules`].
const T_UPSERT_RULES_ROW_SQLITE: &str = "INSERT OR REPLACE INTO druid_rules (id, dataSource, version, payload) VALUES ({1}, {2}, {3}, {4})";
const T_GET_RULE_DATA_SOURCES: &str =
    "SELECT DISTINCT dataSource FROM druid_rules ORDER BY dataSource";

const T_INSERT_SUPERVISOR: &str = "INSERT INTO druid_supervisors (id, spec_id, created_date, payload) VALUES ({1}, {2}, {3}, {4})";
const T_GET_SUPERVISOR: &str = "SELECT payload FROM druid_supervisors \
     WHERE spec_id = {1} ORDER BY {gen} DESC LIMIT 1";
const T_GET_ALL_SUPERVISORS: &str = "SELECT id, spec_id, created_date, payload \
     FROM druid_supervisors ORDER BY {gen} ASC";

const TASKLOG_COLS: &[&str] = &["id", "created_date", "datasource", "payload"];
const TASK_STATUS_COLS: &[&str] = &[
    "id",
    "task_type",
    "datasource",
    "status",
    "created_date",
    "attempt",
    "worker",
    "payload",
];
const T_UPDATE_TASK_STATUS: &str = "UPDATE druid_task_status \
     SET status = {2}, attempt = {3}, worker = {4}, payload = {5} WHERE id = {1}";
const T_UPDATE_TASKLOG_PAYLOAD: &str = "UPDATE druid_tasklogs SET payload = {2} WHERE id = {1}";
const T_GET_TASK: &str = "SELECT id, task_type, datasource, status, created_date, attempt, \
     worker, payload FROM druid_task_status WHERE id = {1}";
const T_GET_ACTIVE_TASKS: &str = "SELECT id, task_type, datasource, status, created_date, \
     attempt, worker, payload FROM druid_task_status \
     WHERE status IN ('WAITING', 'PENDING', 'RUNNING') ORDER BY created_date";
const T_GET_ALL_TASKS: &str = "SELECT id, task_type, datasource, status, created_date, attempt, \
     worker, payload FROM druid_task_status ORDER BY created_date";

const T_INSERT_TASKLOCK: &str =
    "INSERT INTO druid_tasklocks (task_id, lock_payload) VALUES ({1}, {2})";
const T_INSERT_LOCK_DETAIL: &str = "INSERT INTO druid_task_lock_detail \
     (id, datasource, interval_start, interval_end, lock_type, priority, revoked) \
     VALUES ({1}, {2}, {3}, {4}, {5}, {6}, {7})";
const T_GET_LOCKS_DS_ALL: &str = "SELECT l.id, l.task_id, l.lock_payload, d.datasource, \
     d.interval_start, d.interval_end, d.lock_type, d.priority, d.revoked \
     FROM druid_tasklocks l \
     JOIN druid_task_lock_detail d ON d.id = l.id \
     WHERE d.datasource = {1} ORDER BY d.priority DESC, l.id";
const T_GET_LOCKS_DS_ACTIVE: &str = "SELECT l.id, l.task_id, l.lock_payload, d.datasource, \
     d.interval_start, d.interval_end, d.lock_type, d.priority, d.revoked \
     FROM druid_tasklocks l \
     JOIN druid_task_lock_detail d ON d.id = l.id \
     WHERE d.datasource = {1} AND d.revoked = FALSE ORDER BY d.priority DESC, l.id";
const T_GET_LOCKS_FOR_TASK: &str = "SELECT l.id, l.task_id, l.lock_payload, d.datasource, \
     d.interval_start, d.interval_end, d.lock_type, d.priority, d.revoked \
     FROM druid_tasklocks l \
     JOIN druid_task_lock_detail d ON d.id = l.id \
     WHERE l.task_id = {1} ORDER BY l.id";
const T_REVOKE_LOCK: &str = "UPDATE druid_task_lock_detail SET revoked = TRUE WHERE id = {1}";
const T_DELETE_LOCK_DETAIL: &str = "DELETE FROM druid_task_lock_detail WHERE id = {1}";
const T_DELETE_LOCK: &str = "DELETE FROM druid_tasklocks WHERE id = {1}";

const T_GET_CONFIG: &str = "SELECT payload FROM druid_config WHERE name = {1}";
const T_GET_ALL_CONFIG: &str = "SELECT name, payload FROM druid_config ORDER BY name";
const CONFIG_COLS: &[&str] = &["name", "payload"];

// ---------------------------------------------------------------------------
// MetadataStore
// ---------------------------------------------------------------------------

/// Relational metadata store for FerroDruid.
///
/// Backends: SQLite (default, file or in-memory), PostgreSQL and MySQL —
/// select via [`MetadataStore::connect`].  Manages the Druid-compatible
/// schema covering segments, rules, supervisors, config, audit entries,
/// task logs, and task locks; behavior is identical across backends
/// (the parametrized test suite runs the same assertions on all three).
pub struct MetadataStore {
    backend: Backend,
    /// Per-datasource publish mutexes (see [`datasource_publish_lock`]).
    ///
    /// [`datasource_publish_lock`]: MetadataStore::datasource_publish_lock
    publish_locks: tokio::sync::Mutex<std::collections::HashMap<String, PublishLock>>,
}

/// Shared handle to a per-datasource publish mutex.
///
/// See [`MetadataStore::datasource_publish_lock`] for the protocol.
pub type PublishLock = std::sync::Arc<tokio::sync::Mutex<()>>;

/// Build the 9 bind arguments of a full `druid_segments` row write
/// ([`SEGMENT_COLS`] order).
fn segment_args(seg: &SegmentMetadataRow, payload_str: &str) -> [Arg; 9] {
    [
        Arg::Str(seg.id.clone()),
        Arg::Str(seg.data_source.clone()),
        Arg::Str(seg.created_date.clone()),
        Arg::Str(seg.start.clone()),
        Arg::Str(seg.end.clone()),
        Arg::Str(seg.version.clone()),
        Arg::Bool(seg.used),
        Arg::Str(payload_str.to_string()),
        Arg::Str(seg.created_date.clone()),
    ]
}

impl MetadataStore {
    fn from_backend(backend: Backend) -> Self {
        Self {
            backend,
            publish_locks: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Connect to the metadata store selected by `uri`.
    ///
    /// Dispatches on the URI scheme (see [`MetadataUri`]):
    /// `postgres://…`/`postgresql://…` → PostgreSQL, `mysql://…` →
    /// MySQL, `sqlite://<path>` / a bare path → SQLite (created if
    /// missing), `:memory:` → in-memory SQLite.  Unknown schemes fail
    /// loudly instead of being treated as SQLite filenames.
    pub async fn connect(uri: &str) -> Result<Self> {
        match parse_metadata_uri(uri)? {
            MetadataUri::Postgres => Self::new_postgres(uri).await,
            MetadataUri::MySql => Self::new_mysql(uri).await,
            MetadataUri::SqliteMemory => Self::new_in_memory().await,
            MetadataUri::SqlitePath(path) => Self::new_sqlite(&path).await,
        }
    }

    /// Create a new [`MetadataStore`] backed by SQLite at the given path.
    ///
    /// Use `":memory:"` for an ephemeral in-memory database (useful for tests).
    pub async fn new_sqlite(path: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .map_err(|e| DruidError::Metadata(format!("sqlite connect: {e}")))?;

        Ok(Self::from_backend(Backend::Sqlite(pool)))
    }

    /// Open an EXISTING SQLite database file READ-ONLY.
    ///
    /// Uses `read_only(true)` + `create_if_missing(false)` (mirroring the
    /// compat-7 source reader), so the file is never created, never
    /// grown, and no DDL/journal is written — the store can only be
    /// queried.  Intended for read-only probes such as
    /// `import-druid-metadata --dry-run`, which must write NOTHING to the
    /// target.  Fails if the file does not exist.
    pub async fn open_sqlite_read_only(path: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .read_only(true)
            .create_if_missing(false);

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .map_err(|e| DruidError::Metadata(format!("sqlite read-only connect: {e}")))?;

        Ok(Self::from_backend(Backend::Sqlite(pool)))
    }

    /// Create a new [`MetadataStore`] using an in-memory SQLite database.
    ///
    /// Primarily intended for testing.
    pub async fn new_in_memory() -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .filename(":memory:")
            .create_if_missing(true);

        // For in-memory DBs we need exactly 1 connection so the data persists
        // for the lifetime of the pool.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .map_err(|e| DruidError::Metadata(format!("sqlite in-memory connect: {e}")))?;

        Ok(Self::from_backend(Backend::Sqlite(pool)))
    }

    /// Create a new [`MetadataStore`] backed by PostgreSQL.
    ///
    /// `uri` is passed to the driver unmodified, so standard connection
    /// parameters (`?sslmode=`, …) apply.  The database itself must
    /// already exist; the schema is created by
    /// [`initialize`](Self::initialize).
    pub async fn new_postgres(uri: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(uri)
            .await
            .map_err(|e| DruidError::Metadata(format!("postgres connect: {e}")))?;
        Ok(Self::from_backend(Backend::Postgres(pool)))
    }

    /// Create a new [`MetadataStore`] backed by MySQL.
    ///
    /// `uri` is passed to the driver unmodified.  The database itself
    /// must already exist; the schema is created by
    /// [`initialize`](Self::initialize).
    pub async fn new_mysql(uri: &str) -> Result<Self> {
        let pool = MySqlPoolOptions::new()
            .max_connections(4)
            .connect(uri)
            .await
            .map_err(|e| DruidError::Metadata(format!("mysql connect: {e}")))?;
        Ok(Self::from_backend(Backend::MySql(pool)))
    }

    /// The rendering dialect of this store's backend.
    fn dialect(&self) -> Dialect {
        self.backend.dialect()
    }

    /// Get (or create) the per-datasource **publish lock** for `data_source`.
    ///
    /// This is the process-wide serialization point for every mutation of
    /// segment `used` flags and for segment publication (Codex 2026-07-12
    /// round-2 HIGH #2/#3). All components that share this
    /// `Arc<MetadataStore>` — the Overlord's replace/append publication
    /// critical section and the Coordinator's `disable_segment` /
    /// `disable_datasource` / `enable_datasource` — must hold this lock
    /// while reading-then-writing used flags, so an admin disable can never
    /// interleave with a publish's plan → metadata-transaction → segment
    /// swap → rollback sequence (which would let a disable be silently
    /// overwritten by a rollback, or a just-disabled segment become
    /// query-visible mid-publish).
    ///
    /// **Scope (documented residual):** the lock lives on this
    /// `MetadataStore` *instance*, so it only serializes callers inside one
    /// process — sufficient for the supported single-binary deployment
    /// (`bins/ferrodruid`), which constructs exactly one store shared by
    /// its one Overlord and one Coordinator. Separate processes pointed at
    /// the same database (the standalone role binaries, the
    /// `ferrodruid-migrate` import tool, or any out-of-band SQL writer)
    /// are NOT serialized by it; concurrent used-flag mutation from a
    /// second process during a publish is unsupported.  **This holds for
    /// the PostgreSQL/MySQL backends too**: a shared network database
    /// does NOT make multi-process publishing safe — the supported
    /// topology remains a single writer process.
    pub async fn datasource_publish_lock(&self, data_source: &str) -> PublishLock {
        let mut map = self.publish_locks.lock().await;
        std::sync::Arc::clone(
            map.entry(data_source.to_string())
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Run `CREATE TABLE IF NOT EXISTS` for every Druid metadata table,
    /// one statement at a time (idempotent on every backend).
    pub async fn initialize(&self) -> Result<()> {
        for stmt in ddl::schema_statements(self.backend.kind()) {
            self.backend
                .execute_raw(stmt)
                .await
                .map_err(|e| DruidError::Metadata(format!("initialize schema: {e}")))?;
        }
        Ok(())
    }

    /// Whether the schema is already present — a READ-ONLY probe for the
    /// core `druid_segments` table (never CREATEs or writes anything).
    ///
    /// Used by `import-druid-metadata --dry-run`, which must touch the
    /// target only through read-only queries.  Each backend has a
    /// structurally different catalog query (these are NOT one dialect
    /// template): SQLite reads `sqlite_master`, PostgreSQL and MySQL read
    /// `information_schema.tables` scoped to the current schema/database.
    /// Each returns at most one row, present exactly when the table
    /// exists.
    pub async fn is_initialized(&self) -> Result<bool> {
        let sql = match self.backend.kind() {
            BackendKind::Sqlite => {
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'druid_segments'"
            }
            BackendKind::Postgres => {
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_name = 'druid_segments' AND table_schema = current_schema()"
            }
            BackendKind::MySql => {
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = DATABASE() AND table_name = 'druid_segments'"
            }
        };
        // No `{...}` template tokens and no bind args — the exec layer's
        // renderer passes the SQL through verbatim.
        let row = self
            .backend
            .fetch_optional(sql, &[])
            .await
            .map_err(|e| DruidError::Metadata(format!("is_initialized probe: {e}")))?;
        Ok(row.is_some())
    }

    // ----- Segments --------------------------------------------------------

    /// Insert (or replace) a segment metadata row.
    pub async fn insert_segment(&self, segment: &SegmentMetadataRow) -> Result<()> {
        let payload_str = serde_json::to_string(&segment.payload)
            .map_err(|e| DruidError::Metadata(format!("serialize payload: {e}")))?;

        let tpl = self
            .dialect()
            .upsert("druid_segments", SEGMENT_COLS, "id", SEGMENT_RESET_COLS);
        self.backend
            .execute(&tpl, &segment_args(segment, &payload_str))
            .await
            .map_err(|e| DruidError::Metadata(format!("insert segment: {e}")))?;

        Ok(())
    }

    /// Fetch a single segment metadata row by id (used or unused).
    pub async fn get_segment(&self, id: &str) -> Result<Option<SegmentMetadataRow>> {
        let row = self
            .backend
            .fetch_optional(T_GET_SEGMENT, &[Arg::Str(id.to_string())])
            .await
            .map_err(|e| DruidError::Metadata(format!("get segment: {e}")))?;

        match row {
            Some(r) => Ok(Some(row_to_segment(&r)?)),
            None => Ok(None),
        }
    }

    /// Whether ANY segment row (used or unused) exists with this id.
    ///
    /// Segment-id allocation must consult this rather than the used set:
    /// an unused row with the same id would still be clobbered by an
    /// overwrite upsert, destroying its audit history.
    pub async fn segment_exists(&self, id: &str) -> Result<bool> {
        let row = self
            .backend
            .fetch_optional(T_SEGMENT_EXISTS, &[Arg::Str(id.to_string())])
            .await
            .map_err(|e| DruidError::Metadata(format!("segment exists: {e}")))?;
        Ok(row.is_some())
    }

    /// Atomically publish a replace: mark every id in `unused_ids` unused
    /// AND insert `new_row`, as ONE database transaction (Codex 2026-07-12
    /// round-2 HIGH #1).
    ///
    /// Pre-fix, the publication critical section issued these as separate
    /// autocommit statements; a crash, panic, or cancellation between them
    /// left partial durable state (some victims unused without the new
    /// row, or vice versa) that no in-memory rollback can undo after a
    /// restart. Here either every write commits or none does — the
    /// backend's journal (SQLite) / WAL (PostgreSQL, InnoDB) guarantees
    /// the same atomicity across a process crash mid-transaction.
    ///
    /// The new row is inserted with a plain `INSERT` (NOT an upsert): a
    /// pre-existing row with the same id — a segment-id collision that
    /// would otherwise silently overwrite another task's publication
    /// (round-2 HIGH #4) — aborts and rolls back the whole transaction,
    /// victims included, on every backend. Callers must hold the
    /// datasource's [`datasource_publish_lock`] so the plan this write
    /// applies cannot go stale.
    ///
    /// [`datasource_publish_lock`]: MetadataStore::datasource_publish_lock
    pub async fn replace_segments_txn(
        &self,
        unused_ids: &[String],
        new_row: &SegmentMetadataRow,
    ) -> Result<()> {
        let payload_str = serde_json::to_string(&new_row.payload)
            .map_err(|e| DruidError::Metadata(format!("serialize payload: {e}")))?;
        let now = chrono::Utc::now().to_rfc3339();

        let mut txn = self
            .backend
            .begin()
            .await
            .map_err(|e| DruidError::Metadata(format!("begin replace txn: {e}")))?;

        for id in unused_ids {
            txn.execute(
                T_MARK_SEGMENT_UNUSED,
                &[Arg::Str(now.clone()), Arg::Str(id.clone())],
            )
            .await
            .map_err(|e| {
                DruidError::Metadata(format!("replace txn: mark segment '{id}' unused: {e}"))
            })?;
        }

        // Fail-closed by design: T_INSERT_SEGMENT_PLAIN is a plain INSERT
        // on ALL backends (no ON CONFLICT / ON DUPLICATE KEY — see the
        // template's doc comment).
        txn.execute(T_INSERT_SEGMENT_PLAIN, &segment_args(new_row, &payload_str))
            .await
            .map_err(|e| {
                DruidError::Metadata(format!(
                    "replace txn: insert new segment row '{}' failed (a pre-existing row \
                     with the same id fails closed instead of being overwritten): {e}",
                    new_row.id
                ))
            })?;

        txn.commit()
            .await
            .map_err(|e| DruidError::Metadata(format!("commit replace txn: {e}")))?;
        Ok(())
    }

    /// Atomically undo a committed [`replace_segments_txn`] after a
    /// downstream failure (e.g. the Historical swap could not be applied):
    /// DELETE the just-inserted `new_segment_id` row and restore each
    /// victim snapshot in `restore` verbatim, as ONE transaction.
    ///
    /// Deleting (rather than flipping to unused) restores exactly the
    /// pre-publish metadata state and frees the id for a retry. Callers
    /// must hold the datasource's [`datasource_publish_lock`] for the
    /// entire publish + rollback so no other used-flag mutation (e.g. an
    /// admin disable) can land between the two transactions and be
    /// overwritten by the restore (Codex 2026-07-12 round-2 HIGH #2).
    ///
    /// [`replace_segments_txn`]: MetadataStore::replace_segments_txn
    /// [`datasource_publish_lock`]: MetadataStore::datasource_publish_lock
    pub async fn rollback_replace_txn(
        &self,
        new_segment_id: &str,
        restore: &[SegmentMetadataRow],
    ) -> Result<()> {
        let mut txn = self
            .backend
            .begin()
            .await
            .map_err(|e| DruidError::Metadata(format!("begin rollback txn: {e}")))?;

        txn.execute(T_DELETE_SEGMENT, &[Arg::Str(new_segment_id.to_string())])
            .await
            .map_err(|e| {
                DruidError::Metadata(format!(
                    "rollback txn: delete new segment row '{new_segment_id}': {e}"
                ))
            })?;

        let restore_tpl =
            self.dialect()
                .upsert("druid_segments", SEGMENT_COLS, "id", SEGMENT_RESET_COLS);
        for seg in restore {
            let payload_str = serde_json::to_string(&seg.payload)
                .map_err(|e| DruidError::Metadata(format!("serialize payload: {e}")))?;
            txn.execute(&restore_tpl, &segment_args(seg, &payload_str))
                .await
                .map_err(|e| {
                    DruidError::Metadata(format!("rollback txn: restore segment '{}': {e}", seg.id))
                })?;
        }

        txn.commit()
            .await
            .map_err(|e| DruidError::Metadata(format!("commit rollback txn: {e}")))?;
        Ok(())
    }

    /// Delete a set of segment rows in ONE transaction (all-or-nothing).
    ///
    /// Used by the Kafka streaming respawn cleanup: the victim rows must
    /// vanish atomically, or a mid-batch failure leaves a half-deleted
    /// history that a retry then re-drops only partially — duplicating the
    /// surviving rows after the replay (Codex R19).
    pub async fn delete_segments(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut txn = self
            .backend
            .begin()
            .await
            .map_err(|e| DruidError::Metadata(format!("begin delete-segments txn: {e}")))?;
        for id in ids {
            txn.execute(T_DELETE_SEGMENT, &[Arg::Str(id.clone())])
                .await
                .map_err(|e| {
                    DruidError::Metadata(format!("delete-segments txn: row '{id}': {e}"))
                })?;
        }
        txn.commit()
            .await
            .map_err(|e| DruidError::Metadata(format!("commit delete-segments txn: {e}")))
    }

    /// Return all segments for `data_source` where `used = TRUE`.
    pub async fn get_used_segments(&self, data_source: &str) -> Result<Vec<SegmentMetadataRow>> {
        let rows = self
            .backend
            .fetch_all(T_GET_USED_SEGMENTS, &[Arg::Str(data_source.to_string())])
            .await
            .map_err(|e| DruidError::Metadata(format!("get used segments: {e}")))?;

        rows.iter().map(row_to_segment).collect()
    }

    /// Return every segment where `used = TRUE`, across ALL data sources.
    ///
    /// Used by the Overlord's startup bootstrap reload to re-download every
    /// currently-used segment from deep storage after a restart — the
    /// query-visible set is exactly the `used = TRUE` rows.
    pub async fn get_used_segments_all(&self) -> Result<Vec<SegmentMetadataRow>> {
        let rows = self
            .backend
            .fetch_all(T_GET_USED_SEGMENTS_ALL, &[])
            .await
            .map_err(|e| DruidError::Metadata(format!("get all used segments: {e}")))?;

        rows.iter().map(row_to_segment).collect()
    }

    /// Mark a segment as unused.
    pub async fn mark_segment_unused(&self, id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.backend
            .execute(
                T_MARK_SEGMENT_UNUSED,
                &[Arg::Str(now), Arg::Str(id.to_string())],
            )
            .await
            .map_err(|e| DruidError::Metadata(format!("mark segment unused: {e}")))?;
        Ok(())
    }

    /// Set the `used` flag for EVERY segment of a data source ATOMICALLY, in a
    /// single `UPDATE` statement (Codex 2026-07-12). The datasource-wide
    /// disable/enable must not be a loop of per-segment autocommits — a
    /// cancellation or a mid-loop failure would leave only a subset of the
    /// data source disabled/enabled, a durable partial administrative state.
    /// One statement is inherently atomic on every backend (all rows or
    /// none). Callers hold the datasource's
    /// [`MetadataStore::datasource_publish_lock`] so this cannot interleave
    /// with a publish.
    pub async fn set_datasource_used(&self, data_source: &str, used: bool) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.backend
            .execute(
                T_SET_DATASOURCE_USED,
                &[
                    Arg::Bool(used),
                    Arg::Str(now),
                    Arg::Str(data_source.to_string()),
                ],
            )
            .await
            .map_err(|e| DruidError::Metadata(format!("set datasource used={used}: {e}")))?;
        Ok(())
    }

    /// Return the distinct data source names across all segments (used or not).
    pub async fn get_all_data_sources(&self) -> Result<Vec<String>> {
        let rows = self
            .backend
            .fetch_all(T_GET_ALL_DATA_SOURCES, &[])
            .await
            .map_err(|e| DruidError::Metadata(format!("get all data sources: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let ds = r
                .str("dataSource")
                .map_err(|e| DruidError::Metadata(format!("decode dataSource: {e}")))?;
            out.push(ds);
        }
        Ok(out)
    }

    /// Return all segments across all data sources.
    pub async fn get_all_segments(&self) -> Result<Vec<SegmentMetadataRow>> {
        let rows = self
            .backend
            .fetch_all(T_GET_ALL_SEGMENTS, &[])
            .await
            .map_err(|e| DruidError::Metadata(format!("get all segments: {e}")))?;

        rows.iter().map(row_to_segment).collect()
    }

    // ----- Rules -----------------------------------------------------------

    /// Return the rule list (JSON array) for a data source.
    ///
    /// Ordered by the monotonic generation column — SQLite's insertion
    /// `rowid`, or the explicit `seq` sequence column on PostgreSQL/MySQL
    /// — NOT the wall-clock `version` text:
    /// [`set_rules`](Self::set_rules) stamps `version` from the system
    /// clock, so if the clock steps BACKWARD between two rule updates
    /// (NTP step, VM snapshot restore), a `version DESC` order would keep
    /// serving the OLDER generation forever — a newer ACKed update would
    /// never be returned again. Every `set_rules` write takes a fresh
    /// generation position (SQLite: `INSERT OR REPLACE`'s implicit
    /// delete + re-insert yields a fresh `rowid`; PostgreSQL/MySQL: an
    /// explicit delete + insert transaction yields a fresh `seq`), so
    /// the highest generation value is the newest ACKed update — same
    /// discipline as the Codex R16
    /// [`get_supervisor`](Self::get_supervisor) fix.
    pub async fn get_rules(&self, data_source: &str) -> Result<Vec<serde_json::Value>> {
        let row = self
            .backend
            .fetch_optional(T_GET_RULES, &[Arg::Str(data_source.to_string())])
            .await
            .map_err(|e| DruidError::Metadata(format!("get rules: {e}")))?;

        match row {
            Some(r) => {
                let payload_str = r
                    .str("payload")
                    .map_err(|e| DruidError::Metadata(format!("decode rules payload: {e}")))?;
                let rules: Vec<serde_json::Value> = serde_json::from_str(&payload_str)?;
                Ok(rules)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Set (replace) the rules for a data source.
    ///
    /// Backend-specific ON PURPOSE — in-place upserts are wrong here, and
    /// so is porting either backend's statement to the other:
    ///
    /// - **SQLite** executes the single shipped `INSERT OR REPLACE`
    ///   statement, byte-identical to the pre-multi-backend store (the
    ///   Codex-hardened path regresses zero — statement text AND
    ///   statement sequence; a distinct `DELETE` is observably different,
    ///   e.g. to `DELETE` triggers). On a primary-key collision (same
    ///   datasource + same clock reading) `OR REPLACE` implicitly
    ///   deletes-then-re-inserts, so the row takes a FRESH `rowid` and
    ///   [`get_rules`](Self::get_rules) — which serves the highest
    ///   `rowid`/`seq` — still returns the newest ACKed update.
    /// - **PostgreSQL/MySQL** have no `OR REPLACE`; an `ON CONFLICT ..
    ///   DO UPDATE` / `ON DUPLICATE KEY UPDATE` port would keep the OLD
    ///   generation position and serve stale rules forever after a
    ///   clock-collision edge — a real correctness bug, not a style
    ///   choice (Codex R16/R17 species). They reproduce the SQLite
    ///   semantics explicitly: one transaction that `DELETE`s the exact
    ///   colliding primary key — only ever `id`, never the datasource,
    ///   so prior generations survive — then `INSERT`s, taking a fresh
    ///   `seq`.
    pub async fn set_rules(&self, data_source: &str, rules: &[serde_json::Value]) -> Result<()> {
        let payload_str = serde_json::to_string(rules)?;
        let version = chrono::Utc::now().to_rfc3339();
        let id = format!("{data_source}_{version}");
        let insert_args = [
            Arg::Str(id.clone()),
            Arg::Str(data_source.to_string()),
            Arg::Str(version),
            Arg::Str(payload_str),
        ];

        match self.backend.kind() {
            BackendKind::Sqlite => {
                // The shipped path, unchanged (byte-identical SQL).
                self.backend
                    .execute(T_UPSERT_RULES_ROW_SQLITE, &insert_args)
                    .await
                    .map_err(|e| DruidError::Metadata(format!("set rules: {e}")))?;
            }
            BackendKind::Postgres | BackendKind::MySql => {
                let mut txn = self
                    .backend
                    .begin()
                    .await
                    .map_err(|e| DruidError::Metadata(format!("begin set-rules txn: {e}")))?;
                txn.execute(T_DELETE_RULES_ROW, &[Arg::Str(id)])
                    .await
                    .map_err(|e| {
                        DruidError::Metadata(format!("set rules (clear colliding id): {e}"))
                    })?;
                txn.execute(T_INSERT_RULES_ROW, &insert_args)
                    .await
                    .map_err(|e| DruidError::Metadata(format!("set rules: {e}")))?;
                txn.commit()
                    .await
                    .map_err(|e| DruidError::Metadata(format!("commit set-rules txn: {e}")))?;
            }
        }

        Ok(())
    }

    /// Return the distinct data source names that have rules persisted.
    ///
    /// Enumerates `druid_rules` itself — NOT `druid_segments` (see
    /// [`get_all_data_sources`](Self::get_all_data_sources)) — because
    /// retention/load rules are routinely set BEFORE any segment exists for
    /// the datasource. A segment-based enumeration would silently omit those
    /// rules from a backup/export.
    pub async fn get_rule_data_sources(&self) -> Result<Vec<String>> {
        let rows = self
            .backend
            .fetch_all(T_GET_RULE_DATA_SOURCES, &[])
            .await
            .map_err(|e| DruidError::Metadata(format!("get rule data sources: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let ds = r
                .str("dataSource")
                .map_err(|e| DruidError::Metadata(format!("decode dataSource: {e}")))?;
            out.push(ds);
        }
        Ok(out)
    }

    // ----- Supervisors -----------------------------------------------------

    /// Insert a supervisor spec.
    pub async fn insert_supervisor(&self, spec_id: &str, spec: &serde_json::Value) -> Result<()> {
        let payload_str = serde_json::to_string(spec)?;
        let now = chrono::Utc::now().to_rfc3339();
        let id = format!("{spec_id}_{now}");

        self.backend
            .execute(
                T_INSERT_SUPERVISOR,
                &[
                    Arg::Str(id),
                    Arg::Str(spec_id.to_string()),
                    Arg::Str(now),
                    Arg::Str(payload_str),
                ],
            )
            .await
            .map_err(|e| DruidError::Metadata(format!("insert supervisor: {e}")))?;

        Ok(())
    }

    /// Insert MULTIPLE supervisor generations ATOMICALLY, in the given
    /// order, as ONE database transaction.
    ///
    /// Built for restore/import: a per-row autocommit loop that fails midway
    /// (SQLITE_BUSY, disk full) leaves a half-imported history in which a
    /// pre-tombstone ACTIVE spec can be the newest generation, resurrecting
    /// a stopped supervisor on the next restart. Here either every
    /// generation commits or none does.
    ///
    /// Insertion order is preserved exactly (entries are inserted first to
    /// last), so the restored generation values (`rowid`/`seq`) keep
    /// generation order and [`get_supervisor`](Self::get_supervisor)
    /// (generation DESC) still returns the newest generation. Row ids carry
    /// a per-entry index suffix so two generations of the same spec inserted
    /// within one clock reading cannot collide on the primary key and abort
    /// the whole transaction.
    pub async fn insert_supervisors_atomic(
        &self,
        entries: &[(String, serde_json::Value)],
    ) -> Result<()> {
        let mut txn = self
            .backend
            .begin()
            .await
            .map_err(|e| DruidError::Metadata(format!("begin supervisors txn: {e}")))?;

        for (i, (spec_id, spec)) in entries.iter().enumerate() {
            let payload_str = serde_json::to_string(spec)?;
            let now = chrono::Utc::now().to_rfc3339();
            let id = format!("{spec_id}_{now}_{i}");

            txn.execute(
                T_INSERT_SUPERVISOR,
                &[
                    Arg::Str(id),
                    Arg::Str(spec_id.clone()),
                    Arg::Str(now),
                    Arg::Str(payload_str),
                ],
            )
            .await
            .map_err(|e| {
                DruidError::Metadata(format!(
                    "supervisors txn: insert generation for '{spec_id}': {e}"
                ))
            })?;
        }

        txn.commit()
            .await
            .map_err(|e| DruidError::Metadata(format!("commit supervisors txn: {e}")))?;
        Ok(())
    }

    /// Get the latest supervisor spec by `spec_id`.
    ///
    /// Ordered by the monotonic generation column (`rowid` on SQLite,
    /// `seq` on PostgreSQL/MySQL), NOT `created_date`: the id and
    /// `created_date` are wall-clock text, so if the system clock steps
    /// BACKWARD between an active spec and its later shutdown tombstone, a
    /// `created_date DESC` order would return the OLDER active spec and a
    /// restart would RESURRECT a stopped supervisor. The generation column
    /// reflects true insertion order (supervisor rows are only ever
    /// inserted, never deleted), so the newest generation always wins
    /// (Codex R16).
    pub async fn get_supervisor(&self, spec_id: &str) -> Result<Option<serde_json::Value>> {
        let row = self
            .backend
            .fetch_optional(T_GET_SUPERVISOR, &[Arg::Str(spec_id.to_string())])
            .await
            .map_err(|e| DruidError::Metadata(format!("get supervisor: {e}")))?;

        match row {
            Some(r) => {
                let payload_str = r
                    .str("payload")
                    .map_err(|e| DruidError::Metadata(format!("decode supervisor payload: {e}")))?;
                let val: serde_json::Value = serde_json::from_str(&payload_str)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Return all supervisor generations OLDEST-first, by the monotonic
    /// generation column (`rowid`/`seq` — not wall-clock `created_date`).
    ///
    /// Oldest-first matters for backup/restore: an exporter emits this order
    /// and an importer re-inserts it forward, so the restored generation
    /// values preserve generation order and
    /// [`get_supervisor`](Self::get_supervisor) (generation DESC) still
    /// returns the newest generation — a `created_date DESC` export
    /// would otherwise invert the order and resurrect a stopped supervisor
    /// after a restore (Codex R17; same root cause as the R16
    /// get_supervisor fix).
    pub async fn get_all_supervisors(&self) -> Result<Vec<SupervisorRow>> {
        let rows = self
            .backend
            .fetch_all(T_GET_ALL_SUPERVISORS, &[])
            .await
            .map_err(|e| DruidError::Metadata(format!("get all supervisors: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let id = r
                .str("id")
                .map_err(|e| DruidError::Metadata(format!("decode id: {e}")))?;
            let spec_id = r
                .str("spec_id")
                .map_err(|e| DruidError::Metadata(format!("decode spec_id: {e}")))?;
            let created_date = r
                .str("created_date")
                .map_err(|e| DruidError::Metadata(format!("decode created_date: {e}")))?;
            let payload_str = r
                .str("payload")
                .map_err(|e| DruidError::Metadata(format!("decode payload: {e}")))?;
            let payload: serde_json::Value = serde_json::from_str(&payload_str)?;
            out.push(SupervisorRow {
                id,
                spec_id,
                created_date,
                payload,
            });
        }
        Ok(out)
    }

    // ----- Tasks -----------------------------------------------------------

    /// Insert (or replace) a task status row.
    ///
    /// The canonical `druid_tasklogs` row (id / created_date / datasource /
    /// payload) and the additive `druid_task_status` detail row are written
    /// together so both the legacy log view and the lifecycle query view
    /// stay consistent.
    pub async fn insert_task(&self, task: &TaskRow) -> Result<()> {
        let payload_str = serde_json::to_string(&task.payload)
            .map_err(|e| DruidError::Metadata(format!("serialize task payload: {e}")))?;

        let log_tpl = self
            .dialect()
            .upsert("druid_tasklogs", TASKLOG_COLS, "id", &[]);
        self.backend
            .execute(
                &log_tpl,
                &[
                    Arg::Str(task.id.clone()),
                    Arg::Str(task.created_date.clone()),
                    Arg::Str(task.data_source.clone()),
                    Arg::Str(payload_str.clone()),
                ],
            )
            .await
            .map_err(|e| DruidError::Metadata(format!("insert task log: {e}")))?;

        let status_tpl = self
            .dialect()
            .upsert("druid_task_status", TASK_STATUS_COLS, "id", &[]);
        self.backend
            .execute(
                &status_tpl,
                &[
                    Arg::Str(task.id.clone()),
                    Arg::Str(task.task_type.clone()),
                    Arg::Str(task.data_source.clone()),
                    Arg::Str(task.status.clone()),
                    Arg::Str(task.created_date.clone()),
                    Arg::I64(task.attempt),
                    Arg::OptStr(task.worker.clone()),
                    Arg::Str(payload_str),
                ],
            )
            .await
            .map_err(|e| DruidError::Metadata(format!("insert task status: {e}")))?;

        Ok(())
    }

    /// Update the mutable fields of a persisted task (status, attempt,
    /// worker, payload).  No-op if the task does not exist.
    pub async fn update_task_status(&self, task: &TaskRow) -> Result<()> {
        let payload_str = serde_json::to_string(&task.payload)
            .map_err(|e| DruidError::Metadata(format!("serialize task payload: {e}")))?;

        self.backend
            .execute(
                T_UPDATE_TASK_STATUS,
                &[
                    Arg::Str(task.id.clone()),
                    Arg::Str(task.status.clone()),
                    Arg::I64(task.attempt),
                    Arg::OptStr(task.worker.clone()),
                    Arg::Str(payload_str.clone()),
                ],
            )
            .await
            .map_err(|e| DruidError::Metadata(format!("update task status: {e}")))?;

        self.backend
            .execute(
                T_UPDATE_TASKLOG_PAYLOAD,
                &[Arg::Str(task.id.clone()), Arg::Str(payload_str)],
            )
            .await
            .map_err(|e| DruidError::Metadata(format!("update task log payload: {e}")))?;

        Ok(())
    }

    /// Fetch a single task by identifier.
    pub async fn get_task(&self, id: &str) -> Result<Option<TaskRow>> {
        let row = self
            .backend
            .fetch_optional(T_GET_TASK, &[Arg::Str(id.to_string())])
            .await
            .map_err(|e| DruidError::Metadata(format!("get task: {e}")))?;

        match row {
            Some(r) => Ok(Some(row_to_task(&r)?)),
            None => Ok(None),
        }
    }

    /// Return all tasks that are not in a terminal state, i.e. status is
    /// `WAITING`, `PENDING`, or `RUNNING`.
    pub async fn get_active_tasks(&self) -> Result<Vec<TaskRow>> {
        let rows = self
            .backend
            .fetch_all(T_GET_ACTIVE_TASKS, &[])
            .await
            .map_err(|e| DruidError::Metadata(format!("get active tasks: {e}")))?;

        rows.iter().map(row_to_task).collect()
    }

    /// Return every persisted task row, ordered by creation time.
    pub async fn get_all_tasks(&self) -> Result<Vec<TaskRow>> {
        let rows = self
            .backend
            .fetch_all(T_GET_ALL_TASKS, &[])
            .await
            .map_err(|e| DruidError::Metadata(format!("get all tasks: {e}")))?;

        rows.iter().map(row_to_task).collect()
    }

    // ----- Task locks ------------------------------------------------------

    /// Insert a task lock and return its persisted identifier.
    ///
    /// Writes the canonical `druid_tasklocks` row plus the additive
    /// `druid_task_lock_detail` row keyed by the same database-generated
    /// id (`last_insert_rowid()` on SQLite, `RETURNING id` on PostgreSQL,
    /// `LAST_INSERT_ID()` on MySQL).
    pub async fn insert_task_lock(&self, lock: &TaskLockRow) -> Result<String> {
        let payload_str = serde_json::to_string(&lock.payload)
            .map_err(|e| DruidError::Metadata(format!("serialize lock payload: {e}")))?;

        let rowid = self
            .backend
            .execute_returning_id(
                T_INSERT_TASKLOCK,
                &[Arg::Str(lock.task_id.clone()), Arg::Str(payload_str)],
            )
            .await
            .map_err(|e| DruidError::Metadata(format!("insert task lock: {e}")))?;

        self.backend
            .execute(
                T_INSERT_LOCK_DETAIL,
                &[
                    Arg::I64(rowid),
                    Arg::Str(lock.data_source.clone()),
                    Arg::Str(lock.interval_start.clone()),
                    Arg::Str(lock.interval_end.clone()),
                    Arg::Str(lock.lock_type.clone()),
                    Arg::I64(lock.priority),
                    Arg::Bool(lock.revoked),
                ],
            )
            .await
            .map_err(|e| DruidError::Metadata(format!("insert task lock detail: {e}")))?;

        Ok(rowid.to_string())
    }

    /// Return all locks for a data source.
    ///
    /// When `include_revoked` is false, revoked locks are filtered out.
    pub async fn get_locks_for_datasource(
        &self,
        data_source: &str,
        include_revoked: bool,
    ) -> Result<Vec<TaskLockRow>> {
        let tpl = if include_revoked {
            T_GET_LOCKS_DS_ALL
        } else {
            T_GET_LOCKS_DS_ACTIVE
        };

        let rows = self
            .backend
            .fetch_all(tpl, &[Arg::Str(data_source.to_string())])
            .await
            .map_err(|e| DruidError::Metadata(format!("get locks for datasource: {e}")))?;

        rows.iter().map(row_to_lock).collect()
    }

    /// Return all locks currently held by a task (revoked included).
    pub async fn get_locks_for_task(&self, task_id: &str) -> Result<Vec<TaskLockRow>> {
        let rows = self
            .backend
            .fetch_all(T_GET_LOCKS_FOR_TASK, &[Arg::Str(task_id.to_string())])
            .await
            .map_err(|e| DruidError::Metadata(format!("get locks for task: {e}")))?;

        rows.iter().map(row_to_lock).collect()
    }

    /// Mark a lock as revoked (preempted) without deleting it.
    pub async fn revoke_lock(&self, id: &str) -> Result<()> {
        let rowid: i64 = id
            .parse()
            .map_err(|e| DruidError::Metadata(format!("invalid lock id '{id}': {e}")))?;
        self.backend
            .execute(T_REVOKE_LOCK, &[Arg::I64(rowid)])
            .await
            .map_err(|e| DruidError::Metadata(format!("revoke lock: {e}")))?;
        Ok(())
    }

    /// Permanently delete a lock (both canonical and detail rows).
    pub async fn delete_lock(&self, id: &str) -> Result<()> {
        let rowid: i64 = id
            .parse()
            .map_err(|e| DruidError::Metadata(format!("invalid lock id '{id}': {e}")))?;
        self.backend
            .execute(T_DELETE_LOCK_DETAIL, &[Arg::I64(rowid)])
            .await
            .map_err(|e| DruidError::Metadata(format!("delete lock detail: {e}")))?;
        self.backend
            .execute(T_DELETE_LOCK, &[Arg::I64(rowid)])
            .await
            .map_err(|e| DruidError::Metadata(format!("delete lock: {e}")))?;
        Ok(())
    }

    // ----- Config ----------------------------------------------------------

    /// Get a config value by name.
    pub async fn get_config(&self, name: &str) -> Result<Option<serde_json::Value>> {
        let row = self
            .backend
            .fetch_optional(T_GET_CONFIG, &[Arg::Str(name.to_string())])
            .await
            .map_err(|e| DruidError::Metadata(format!("get config: {e}")))?;

        match row {
            Some(r) => {
                let payload_str = r
                    .str("payload")
                    .map_err(|e| DruidError::Metadata(format!("decode config payload: {e}")))?;
                let val: serde_json::Value = serde_json::from_str(&payload_str)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Return all config entries as name-value pairs.
    pub async fn get_all_config(&self) -> Result<Vec<(String, serde_json::Value)>> {
        let rows = self
            .backend
            .fetch_all(T_GET_ALL_CONFIG, &[])
            .await
            .map_err(|e| DruidError::Metadata(format!("get all config: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let name = r
                .str("name")
                .map_err(|e| DruidError::Metadata(format!("decode name: {e}")))?;
            let payload_str = r
                .str("payload")
                .map_err(|e| DruidError::Metadata(format!("decode config payload: {e}")))?;
            let val: serde_json::Value = serde_json::from_str(&payload_str)?;
            out.push((name, val));
        }
        Ok(out)
    }

    /// Set (upsert) a config value.
    pub async fn set_config(&self, name: &str, value: &serde_json::Value) -> Result<()> {
        let payload_str = serde_json::to_string(value)?;

        let tpl = self
            .dialect()
            .upsert("druid_config", CONFIG_COLS, "name", &[]);
        self.backend
            .execute(&tpl, &[Arg::Str(name.to_string()), Arg::Str(payload_str)])
            .await
            .map_err(|e| DruidError::Metadata(format!("set config: {e}")))?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn row_to_segment(r: &MetaRow) -> Result<SegmentMetadataRow> {
    let id = r
        .str("id")
        .map_err(|e| DruidError::Metadata(format!("decode id: {e}")))?;
    let data_source = r
        .str("dataSource")
        .map_err(|e| DruidError::Metadata(format!("decode dataSource: {e}")))?;
    let created_date = r
        .str("created_date")
        .map_err(|e| DruidError::Metadata(format!("decode created_date: {e}")))?;
    let start = r
        .str("start")
        .map_err(|e| DruidError::Metadata(format!("decode start: {e}")))?;
    let end = r
        .str("end")
        .map_err(|e| DruidError::Metadata(format!("decode end: {e}")))?;
    let version = r
        .str("version")
        .map_err(|e| DruidError::Metadata(format!("decode version: {e}")))?;
    let used = r
        .bool("used")
        .map_err(|e| DruidError::Metadata(format!("decode used: {e}")))?;
    let payload_str = r
        .str("payload")
        .map_err(|e| DruidError::Metadata(format!("decode payload: {e}")))?;
    let payload: serde_json::Value = serde_json::from_str(&payload_str)?;

    Ok(SegmentMetadataRow {
        id,
        data_source,
        created_date,
        start,
        end,
        version,
        used,
        payload,
    })
}

fn row_to_task(r: &MetaRow) -> Result<TaskRow> {
    let id = r
        .str("id")
        .map_err(|e| DruidError::Metadata(format!("decode task id: {e}")))?;
    let task_type = r
        .str("task_type")
        .map_err(|e| DruidError::Metadata(format!("decode task_type: {e}")))?;
    let data_source = r
        .str("datasource")
        .map_err(|e| DruidError::Metadata(format!("decode datasource: {e}")))?;
    let status = r
        .str("status")
        .map_err(|e| DruidError::Metadata(format!("decode status: {e}")))?;
    let created_date = r
        .str("created_date")
        .map_err(|e| DruidError::Metadata(format!("decode created_date: {e}")))?;
    let attempt = r
        .i64("attempt")
        .map_err(|e| DruidError::Metadata(format!("decode attempt: {e}")))?;
    let worker = r
        .opt_str("worker")
        .map_err(|e| DruidError::Metadata(format!("decode worker: {e}")))?;
    let payload_str = r
        .str("payload")
        .map_err(|e| DruidError::Metadata(format!("decode task payload: {e}")))?;
    let payload: serde_json::Value = serde_json::from_str(&payload_str)?;

    Ok(TaskRow {
        id,
        task_type,
        data_source,
        status,
        created_date,
        attempt,
        worker,
        payload,
    })
}

fn row_to_lock(r: &MetaRow) -> Result<TaskLockRow> {
    let rowid = r
        .i64("id")
        .map_err(|e| DruidError::Metadata(format!("decode lock id: {e}")))?;
    let task_id = r
        .str("task_id")
        .map_err(|e| DruidError::Metadata(format!("decode lock task_id: {e}")))?;
    let data_source = r
        .str("datasource")
        .map_err(|e| DruidError::Metadata(format!("decode lock datasource: {e}")))?;
    let interval_start = r
        .str("interval_start")
        .map_err(|e| DruidError::Metadata(format!("decode interval_start: {e}")))?;
    let interval_end = r
        .str("interval_end")
        .map_err(|e| DruidError::Metadata(format!("decode interval_end: {e}")))?;
    let lock_type = r
        .str("lock_type")
        .map_err(|e| DruidError::Metadata(format!("decode lock_type: {e}")))?;
    let priority = r
        .i64("priority")
        .map_err(|e| DruidError::Metadata(format!("decode priority: {e}")))?;
    let revoked = r
        .bool("revoked")
        .map_err(|e| DruidError::Metadata(format!("decode revoked: {e}")))?;
    let payload_str = r
        .str("lock_payload")
        .map_err(|e| DruidError::Metadata(format!("decode lock_payload: {e}")))?;
    let payload: serde_json::Value = serde_json::from_str(&payload_str)?;

    Ok(TaskLockRow {
        id: rowid.to_string(),
        task_id,
        data_source,
        interval_start,
        interval_end,
        lock_type,
        priority,
        revoked,
        payload,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::BackendKind;
    use serde_json::json;

    async fn setup() -> MetadataStore {
        let store = MetadataStore::new_in_memory()
            .await
            .expect("create in-memory store");
        store.initialize().await.expect("initialize schema");
        store
    }

    fn make_segment(id: &str, ds: &str) -> SegmentMetadataRow {
        SegmentMetadataRow {
            id: id.to_string(),
            data_source: ds.to_string(),
            created_date: "2024-01-01T00:00:00Z".to_string(),
            start: "2024-01-01T00:00:00Z".to_string(),
            end: "2024-02-01T00:00:00Z".to_string(),
            version: "2024-01-01T00:00:00.000Z".to_string(),
            used: true,
            payload: json!({"dataSource": ds, "dimensions": ["page"]}),
        }
    }

    fn make_task(id: &str, ds: &str, status: &str) -> TaskRow {
        TaskRow {
            id: id.to_string(),
            task_type: "index_parallel".to_string(),
            data_source: ds.to_string(),
            status: status.to_string(),
            created_date: "2024-01-01T00:00:00Z".to_string(),
            attempt: 0,
            worker: None,
            payload: json!({"id": id, "status": status}),
        }
    }

    fn make_lock(task_id: &str, ds: &str, lock_type: &str, prio: i64) -> TaskLockRow {
        TaskLockRow {
            id: String::new(),
            task_id: task_id.to_string(),
            data_source: ds.to_string(),
            interval_start: "2024-01-01T00:00:00Z".to_string(),
            interval_end: "2024-02-01T00:00:00Z".to_string(),
            lock_type: lock_type.to_string(),
            priority: prio,
            revoked: false,
            payload: json!({"task": task_id}),
        }
    }

    // -------------------------------------------------------------------
    // Backend-parametrized store behavior cases.
    //
    // Each case takes a FRESH, initialized store and asserts the same
    // behavior on every backend. The `#[tokio::test]` wrappers below run
    // them in-process on SQLite always; `postgres_store_suite` /
    // `mysql_store_suite` run the full list against real servers when
    // `FERRODRUID_TEST_PG_URI` / `FERRODRUID_TEST_MYSQL_URI` are set.
    // -------------------------------------------------------------------

    async fn case_initialize_schema(store: &MetadataStore) {
        // Schema creation is exercised by construction; re-running it
        // must be idempotent on every backend.
        store.initialize().await.expect("re-initialize is a no-op");
    }

    /// `is_initialized` is a read-only probe: `false` on a dropped schema
    /// and `true` after `initialize()`.  DROPs every table, probes false,
    /// re-initializes, probes true — and LEAVES a clean initialized
    /// schema so the rest of the (serial) suite runs unaffected.
    async fn case_is_initialized(store: &MetadataStore) {
        for t in ALL_TABLES {
            store
                .backend
                .execute_raw(&format!("DROP TABLE IF EXISTS {t}"))
                .await
                .expect("drop table");
        }
        assert!(
            !store.is_initialized().await.expect("probe dropped schema"),
            "a dropped schema is not initialized"
        );
        store.initialize().await.expect("initialize schema");
        assert!(
            store.is_initialized().await.expect("probe initialized"),
            "after initialize the druid_segments table exists"
        );
    }

    async fn case_insert_and_get_segments(store: &MetadataStore) {
        let seg = make_segment("wiki_2024-01_v1_0", "wiki");
        store.insert_segment(&seg).await.expect("insert");

        let segs = store.get_used_segments("wiki").await.expect("get used");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].id, "wiki_2024-01_v1_0");
        assert_eq!(segs[0].data_source, "wiki");
    }

    async fn case_mark_segment_unused(store: &MetadataStore) {
        let seg = make_segment("wiki_2024-01_v1_0", "wiki");
        store.insert_segment(&seg).await.expect("insert");

        store
            .mark_segment_unused("wiki_2024-01_v1_0")
            .await
            .expect("mark unused");

        let segs = store.get_used_segments("wiki").await.expect("get used");
        assert!(segs.is_empty(), "segment should no longer be used");

        // Bool round-trip, part 2: the row itself must decode used=false
        // (not merely fall out of the used filter).
        let row = store
            .get_segment("wiki_2024-01_v1_0")
            .await
            .expect("get")
            .expect("present");
        assert!(!row.used, "used=false must round-trip the decode path");
    }

    async fn case_multiple_data_sources(store: &MetadataStore) {
        store
            .insert_segment(&make_segment("wiki_seg1", "wiki"))
            .await
            .expect("insert wiki");
        store
            .insert_segment(&make_segment("clicks_seg1", "clicks"))
            .await
            .expect("insert clicks");

        let sources = store
            .get_all_data_sources()
            .await
            .expect("get data sources");
        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&"wiki".to_string()));
        assert!(sources.contains(&"clicks".to_string()));
    }

    async fn case_insert_and_get_supervisor(store: &MetadataStore) {
        let spec = json!({
            "type": "kafka",
            "dataSchema": {"dataSource": "wiki"},
            "ioConfig": {"topic": "wiki-events"}
        });
        store
            .insert_supervisor("wiki-kafka", &spec)
            .await
            .expect("insert supervisor");

        let got = store
            .get_supervisor("wiki-kafka")
            .await
            .expect("get supervisor");
        assert!(got.is_some());
        assert_eq!(got.as_ref().expect("some")["type"], "kafka");

        let all = store
            .get_all_supervisors()
            .await
            .expect("get all supervisors");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].spec_id, "wiki-kafka");
    }

    async fn case_get_supervisor_uses_insertion_order_not_wall_clock(store: &MetadataStore) {
        // Codex R16: get_supervisor must return the LATEST-INSERTED generation
        // (by rowid/seq), not by created_date text. If the system clock steps
        // BACKWARD between an active spec and its later shutdown tombstone, a
        // created_date order would resurrect the stopped supervisor on restart.
        //
        // Active spec inserted FIRST with a LATER wall clock; the tombstone
        // inserted SECOND with an EARLIER wall clock (clock stepped back).
        store
            .backend
            .execute(
                T_INSERT_SUPERVISOR,
                &[
                    Arg::Str("s_active".to_string()),
                    Arg::Str("s".to_string()),
                    Arg::Str("2024-01-01T12:00:00Z".to_string()),
                    Arg::Str(r#"{"type":"kafka","suspended":false}"#.to_string()),
                ],
            )
            .await
            .expect("insert active");
        store
            .backend
            .execute(
                T_INSERT_SUPERVISOR,
                &[
                    Arg::Str("s_tomb".to_string()),
                    Arg::Str("s".to_string()),
                    Arg::Str("2024-01-01T11:59:00Z".to_string()),
                    Arg::Str(r#"{"suspended":true}"#.to_string()),
                ],
            )
            .await
            .expect("insert tombstone");

        // Despite the tombstone's EARLIER created_date, it was inserted LAST, so
        // it must win — the supervisor stays suspended (not resurrected).
        let got = store.get_supervisor("s").await.expect("get").expect("some");
        assert_eq!(
            got["suspended"], true,
            "latest generation (tombstone) must win over an earlier-wall-clock active spec"
        );
    }

    async fn case_insert_supervisors_atomic_preserves_generation_order(store: &MetadataStore) {
        // The batch insert must keep the given order (oldest-first, as the
        // exporter emits it), so get_supervisor (generation DESC) returns the
        // LAST entry — the shutdown tombstone must win over the earlier
        // active spec even when both land in the same clock reading.
        store
            .insert_supervisors_atomic(&[
                (
                    "s".to_string(),
                    json!({"type": "kafka", "suspended": false}),
                ),
                ("s".to_string(), json!({"suspended": true})),
            ])
            .await
            .expect("atomic insert");

        let all = store.get_all_supervisors().await.expect("all");
        assert_eq!(all.len(), 2, "both generations must be persisted");
        assert_eq!(
            all[0].payload["suspended"], false,
            "oldest-first order must be preserved"
        );
        assert_eq!(all[1].payload["suspended"], true);

        let got = store.get_supervisor("s").await.expect("get").expect("some");
        assert_eq!(
            got["suspended"], true,
            "the last-inserted generation (tombstone) must win"
        );
    }

    async fn case_get_rule_data_sources_includes_segmentless_datasource(store: &MetadataStore) {
        // Rules set BEFORE any segment exists must still be enumerable —
        // the segment-derived get_all_data_sources cannot see them.
        store
            .set_rules("pre-ingest", &[json!({"type": "loadForever"})])
            .await
            .expect("set rules");
        store
            .insert_segment(&make_segment("wiki_seg1", "wiki"))
            .await
            .expect("insert segment");

        let rule_sources = store
            .get_rule_data_sources()
            .await
            .expect("rule data sources");
        assert_eq!(rule_sources, vec!["pre-ingest".to_string()]);

        // Contrast: the segment-derived enumeration cannot see it.
        let segment_sources = store.get_all_data_sources().await.expect("data sources");
        assert_eq!(segment_sources, vec!["wiki".to_string()]);
    }

    async fn case_config_get_set(store: &MetadataStore) {
        // Initially empty.
        let val = store.get_config("lookups").await.expect("get config");
        assert!(val.is_none());

        // Set.
        let data = json!({"version": 1, "lookups": {}});
        store
            .set_config("lookups", &data)
            .await
            .expect("set config");

        // Get.
        let val = store.get_config("lookups").await.expect("get config");
        assert!(val.is_some());
        assert_eq!(val.expect("some")["version"], 1);
    }

    async fn case_config_upsert(store: &MetadataStore) {
        store
            .set_config("key", &json!({"v": 1}))
            .await
            .expect("set v1");
        store
            .set_config("key", &json!({"v": 2}))
            .await
            .expect("set v2");

        let val = store
            .get_config("key")
            .await
            .expect("get config")
            .expect("some");
        assert_eq!(val["v"], 2);

        let all = store.get_all_config().await.expect("all config");
        assert_eq!(all.len(), 1, "upsert must not duplicate the row");
    }

    async fn case_rules_get_set(store: &MetadataStore) {
        // No rules initially.
        let rules = store.get_rules("wiki").await.expect("get rules");
        assert!(rules.is_empty());

        // Set rules.
        let rule_list = vec![
            json!({"type": "loadByPeriod", "period": "P1M", "tieredReplicants": {"_default_tier": 2}}),
            json!({"type": "dropForever"}),
        ];
        store
            .set_rules("wiki", &rule_list)
            .await
            .expect("set rules");

        let rules = store.get_rules("wiki").await.expect("get rules");
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0]["type"], "loadByPeriod");
    }

    async fn case_get_rules_uses_insertion_order_not_wall_clock(store: &MetadataStore) {
        // Same discipline as get_supervisor (Codex R16), applied to rules:
        // `set_rules` stamps `version` from the system clock, so if the clock
        // steps BACKWARD between two rule updates (NTP step, VM snapshot
        // restore), a `version DESC` order would keep serving the OLDER
        // generation forever. The generation column (rowid/seq) reflects true
        // insertion order, so the newest ACKed update must win.
        //
        // Older generation ACKed FIRST, with a LATER wall-clock version text.
        store
            .backend
            .execute(
                T_INSERT_RULES_ROW,
                &[
                    Arg::Str("wiki_2024-01-01T12:00:00Z".to_string()),
                    Arg::Str("wiki".to_string()),
                    Arg::Str("2024-01-01T12:00:00Z".to_string()),
                    Arg::Str(r#"[{"type":"loadForever"}]"#.to_string()),
                ],
            )
            .await
            .expect("insert first generation");
        // Newer generation ACKed SECOND, with an EARLIER wall-clock version
        // text (the clock stepped back between the two updates).
        store
            .backend
            .execute(
                T_INSERT_RULES_ROW,
                &[
                    Arg::Str("wiki_2024-01-01T11:59:00Z".to_string()),
                    Arg::Str("wiki".to_string()),
                    Arg::Str("2024-01-01T11:59:00Z".to_string()),
                    Arg::Str(r#"[{"type":"dropForever"}]"#.to_string()),
                ],
            )
            .await
            .expect("insert second generation");

        let rules = store.get_rules("wiki").await.expect("get rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0]["type"], "dropForever",
            "the last-ACKed rules update must win despite its earlier wall-clock version"
        );
    }

    async fn case_set_rules_twice_newest_wins(store: &MetadataStore) {
        // The public API path of the R16-species invariant: two set_rules
        // generations for one datasource — the SECOND must be served, on
        // every backend. SQLite gets this from the shipped
        // `INSERT OR REPLACE` (implicit delete + re-insert = fresh rowid);
        // PostgreSQL/MySQL from the explicit delete+insert txn (an
        // ON CONFLICT DO UPDATE port would break the collision edge).
        store
            .set_rules("wiki", &[json!({"type": "loadForever"})])
            .await
            .expect("set v1");
        store
            .set_rules("wiki", &[json!({"type": "dropForever"})])
            .await
            .expect("set v2");

        let rules = store.get_rules("wiki").await.expect("get rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0]["type"], "dropForever",
            "the newest set_rules generation must be served"
        );

        // Fail-safe: the PostgreSQL/MySQL pre-insert DELETE targets the
        // exact colliding primary key `id`, NEVER the datasource — both
        // generations (distinct clock readings → distinct ids) must
        // still be on disk.
        let generations = store
            .backend
            .fetch_all(
                "SELECT id FROM druid_rules WHERE dataSource = {1}",
                &[Arg::Str("wiki".to_string())],
            )
            .await
            .expect("enumerate rule generations");
        assert_eq!(
            generations.len(),
            2,
            "prior rule generations must survive a set_rules (only the exact \
             colliding id may be cleared, never the whole datasource)"
        );
    }

    async fn case_get_used_segments_empty(store: &MetadataStore) {
        let segs = store
            .get_used_segments("nonexistent")
            .await
            .expect("get used");
        assert!(segs.is_empty());
    }

    async fn case_task_insert_get_roundtrip(store: &MetadataStore) {
        let t = make_task("index_parallel_wiki_1", "wiki", "PENDING");
        store.insert_task(&t).await.expect("insert task");

        let got = store
            .get_task("index_parallel_wiki_1")
            .await
            .expect("get task")
            .expect("present");
        assert_eq!(got.status, "PENDING");
        assert_eq!(got.data_source, "wiki");
        assert_eq!(got.attempt, 0);
        assert!(got.worker.is_none());
    }

    async fn case_task_update_status_persists(store: &MetadataStore) {
        let mut t = make_task("t1", "wiki", "PENDING");
        store.insert_task(&t).await.expect("insert");

        t.status = "RUNNING".to_string();
        t.attempt = 1;
        t.worker = Some("worker-a:8100".to_string());
        t.payload = json!({"id": "t1", "status": "RUNNING"});
        store.update_task_status(&t).await.expect("update");

        let got = store.get_task("t1").await.expect("get").expect("present");
        assert_eq!(got.status, "RUNNING");
        assert_eq!(got.attempt, 1);
        assert_eq!(got.worker.as_deref(), Some("worker-a:8100"));
    }

    async fn case_task_active_filter(store: &MetadataStore) {
        store
            .insert_task(&make_task("a", "wiki", "PENDING"))
            .await
            .expect("a");
        store
            .insert_task(&make_task("b", "wiki", "RUNNING"))
            .await
            .expect("b");
        store
            .insert_task(&make_task("c", "wiki", "SUCCESS"))
            .await
            .expect("c");
        store
            .insert_task(&make_task("d", "wiki", "FAILED"))
            .await
            .expect("d");

        let active = store.get_active_tasks().await.expect("active");
        assert_eq!(active.len(), 2);
        assert!(
            active
                .iter()
                .all(|t| t.status == "PENDING" || t.status == "RUNNING")
        );

        let all = store.get_all_tasks().await.expect("all");
        assert_eq!(all.len(), 4);
    }

    async fn case_lock_insert_get_roundtrip(store: &MetadataStore) {
        let id = store
            .insert_task_lock(&make_lock("t1", "wiki", "EXCLUSIVE", 10))
            .await
            .expect("insert lock");
        assert!(!id.is_empty());

        let locks = store
            .get_locks_for_datasource("wiki", false)
            .await
            .expect("get locks");
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].lock_type, "EXCLUSIVE");
        assert_eq!(locks[0].priority, 10);
        assert_eq!(locks[0].task_id, "t1");
        assert_eq!(locks[0].id, id);

        let by_task = store.get_locks_for_task("t1").await.expect("by task");
        assert_eq!(by_task.len(), 1);
    }

    async fn case_lock_revoke_filters_out(store: &MetadataStore) {
        let id = store
            .insert_task_lock(&make_lock("t1", "wiki", "EXCLUSIVE", 10))
            .await
            .expect("insert lock");

        store.revoke_lock(&id).await.expect("revoke");

        let active = store
            .get_locks_for_datasource("wiki", false)
            .await
            .expect("active");
        assert!(active.is_empty(), "revoked lock should be excluded");

        let all = store
            .get_locks_for_datasource("wiki", true)
            .await
            .expect("all");
        assert_eq!(all.len(), 1);
        assert!(all[0].revoked, "revoked=true must round-trip the decode");
    }

    async fn case_lock_delete_removes(store: &MetadataStore) {
        let id = store
            .insert_task_lock(&make_lock("t1", "wiki", "SHARED", 5))
            .await
            .expect("insert lock");

        store.delete_lock(&id).await.expect("delete");

        let all = store
            .get_locks_for_datasource("wiki", true)
            .await
            .expect("all");
        assert!(all.is_empty(), "deleted lock should be gone");
        let by_task = store.get_locks_for_task("t1").await.expect("by task");
        assert!(by_task.is_empty());
    }

    async fn case_segment_replace_on_duplicate(store: &MetadataStore) {
        let mut seg = make_segment("seg1", "wiki");
        store.insert_segment(&seg).await.expect("insert");

        seg.version = "2024-02-01T00:00:00.000Z".to_string();
        store.insert_segment(&seg).await.expect("replace");

        let segs = store.get_used_segments("wiki").await.expect("get used");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].version, "2024-02-01T00:00:00.000Z");
    }

    async fn case_get_segment_and_segment_exists(store: &MetadataStore) {
        assert!(
            store.get_segment("nope").await.expect("get").is_none(),
            "missing id -> None"
        );
        assert!(!store.segment_exists("nope").await.expect("exists"));

        let seg = make_segment("seg1", "wiki");
        store.insert_segment(&seg).await.expect("insert");
        let got = store
            .get_segment("seg1")
            .await
            .expect("get")
            .expect("present");
        assert_eq!(got.data_source, "wiki");
        assert!(store.segment_exists("seg1").await.expect("exists"));

        // segment_exists must see UNUSED rows too (id allocation relies
        // on it to avoid resurrecting/clobbering replaced ids).
        store.mark_segment_unused("seg1").await.expect("unused");
        assert!(
            store.segment_exists("seg1").await.expect("exists"),
            "unused rows still occupy their id"
        );
    }

    /// Codex 2026-07-12 round-2 HIGH #1 (happy path): one call flips the
    /// victims unused AND inserts the new row.
    async fn case_replace_txn_commits_both_writes(store: &MetadataStore) {
        store
            .insert_segment(&make_segment("victim_a", "wiki"))
            .await
            .expect("seed a");
        store
            .insert_segment(&make_segment("victim_b", "wiki"))
            .await
            .expect("seed b");

        let new_row = make_segment("new_seg", "wiki");
        store
            .replace_segments_txn(&["victim_a".to_string(), "victim_b".to_string()], &new_row)
            .await
            .expect("replace txn");

        let used = store.get_used_segments("wiki").await.expect("used");
        assert_eq!(used.len(), 1);
        assert_eq!(used[0].id, "new_seg");
        let all = store.get_all_segments().await.expect("all");
        assert_eq!(all.len(), 3, "victims are kept as unused rows");
        assert_eq!(all.iter().filter(|s| !s.used).count(), 2);
    }

    /// Codex 2026-07-12 round-2 HIGH #1 (atomicity): a failure BETWEEN the
    /// metadata writes must leave NO partial state. The new-row `INSERT`
    /// here fails naturally (id collision) AFTER the victim UPDATE has
    /// already executed inside the transaction — exactly the "crash /
    /// failure between the victim flip and the row insert" window that the
    /// pre-fix autocommit sequence left partially applied (victim unused,
    /// no new row). With a real transaction the victim's used flag must
    /// come back untouched.
    async fn case_replace_txn_failure_between_writes_leaves_no_partial_state(
        store: &MetadataStore,
    ) {
        store
            .insert_segment(&make_segment("victim_a", "wiki"))
            .await
            .expect("seed victim");
        // Pre-existing row occupying the id the new row will try to take.
        store
            .insert_segment(&make_segment("colliding_id", "wiki"))
            .await
            .expect("seed collider");

        let new_row = make_segment("colliding_id", "wiki");
        let err = store
            .replace_segments_txn(&["victim_a".to_string()], &new_row)
            .await
            .expect_err("same-id insert must fail closed");
        assert!(
            format!("{err}").contains("colliding_id"),
            "error should name the colliding id: {err}"
        );

        // Atomic: the victim flip that executed before the failing INSERT
        // was rolled back with it.
        let used: Vec<String> = store
            .get_used_segments("wiki")
            .await
            .expect("used")
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert!(
            used.contains(&"victim_a".to_string()),
            "victim must still be used after the aborted txn (no partial state), got {used:?}"
        );
        assert!(used.contains(&"colliding_id".to_string()));
        assert_eq!(store.get_all_segments().await.expect("all").len(), 2);
    }

    /// Codex 2026-07-12 round-2 HIGH #4 (metadata backstop): the new-row
    /// insert fails closed even when the occupying row is UNUSED — an
    /// overwrite upsert would silently resurrect/overwrite it.
    async fn case_replace_txn_collision_with_unused_row_fails_closed(store: &MetadataStore) {
        store
            .insert_segment(&make_segment("old_id", "wiki"))
            .await
            .expect("seed");
        store.mark_segment_unused("old_id").await.expect("unused");

        let new_row = make_segment("old_id", "wiki");
        assert!(
            store.replace_segments_txn(&[], &new_row).await.is_err(),
            "an unused row with the same id must still fail the insert closed"
        );
        let old = store
            .get_segment("old_id")
            .await
            .expect("get")
            .expect("present");
        assert!(!old.used, "the occupying row is untouched");
    }

    /// Rollback txn: deletes the just-published row and restores the
    /// victim snapshots verbatim, atomically.
    async fn case_rollback_replace_txn_deletes_new_row_and_restores_victims(store: &MetadataStore) {
        let victim = make_segment("victim_a", "wiki");
        store.insert_segment(&victim).await.expect("seed");
        let new_row = make_segment("new_seg", "wiki");
        store
            .replace_segments_txn(&["victim_a".to_string()], &new_row)
            .await
            .expect("replace txn");

        store
            .rollback_replace_txn("new_seg", std::slice::from_ref(&victim))
            .await
            .expect("rollback txn");

        assert!(
            !store.segment_exists("new_seg").await.expect("exists"),
            "rollback must delete the new row (freeing the id for a retry)"
        );
        let used = store.get_used_segments("wiki").await.expect("used");
        assert_eq!(used.len(), 1);
        assert_eq!(used[0].id, "victim_a");
        assert!(used[0].used);
    }

    /// compat-6 Codex H1: case-distinct identities must NOT collide on
    /// any backend.  SQLite and PostgreSQL compare bytes, but a stock
    /// MySQL 8.0 database defaults to the case-insensitive
    /// `utf8mb4_0900_ai_ci` collation — without the explicit
    /// case-/accent-sensitive `utf8mb4_0900_as_cs` collation in the
    /// MySQL DDL, `set_config("Foo")` / `set_config("foo")` land on ONE
    /// row and case-distinct segment / task ids silently overwrite each
    /// other (the upsert takes the "duplicate key" path) — silent
    /// metadata corruption.
    async fn case_case_distinct_identities_do_not_collide(store: &MetadataStore) {
        // Config names.
        store
            .set_config("Foo", &json!({"v": "upper"}))
            .await
            .expect("set Foo");
        store
            .set_config("foo", &json!({"v": "lower"}))
            .await
            .expect("set foo");
        assert_eq!(
            store
                .get_config("Foo")
                .await
                .expect("get Foo")
                .expect("some")["v"],
            "upper",
            "config name 'Foo' must keep its own row"
        );
        assert_eq!(
            store
                .get_config("foo")
                .await
                .expect("get foo")
                .expect("some")["v"],
            "lower",
            "config name 'foo' must keep its own row"
        );
        assert_eq!(
            store.get_all_config().await.expect("all config").len(),
            2,
            "case-distinct config names must be TWO rows, not one"
        );

        // Segment ids differing only in case.
        store
            .insert_segment(&make_segment("wiki_2024-01_v1_A", "wiki"))
            .await
            .expect("insert upper-case id");
        store
            .insert_segment(&make_segment("wiki_2024-01_v1_a", "wiki"))
            .await
            .expect("insert lower-case id");
        assert_eq!(
            store.get_all_segments().await.expect("all segments").len(),
            2,
            "case-distinct segment ids must not collide"
        );
        assert_eq!(
            store
                .get_segment("wiki_2024-01_v1_A")
                .await
                .expect("get")
                .expect("present")
                .id,
            "wiki_2024-01_v1_A"
        );

        // Datasource lookups must also compare case-sensitively — an
        // `ai_ci` collation would make `WHERE dataSource = 'WIKI'`
        // match the 'wiki' rows.
        assert!(
            store
                .get_used_segments("WIKI")
                .await
                .expect("used")
                .is_empty(),
            "datasource lookup must be case-sensitive on every backend"
        );

        // Task ids differing only in case.
        store
            .insert_task(&make_task("Task1", "wiki", "PENDING"))
            .await
            .expect("insert Task1");
        store
            .insert_task(&make_task("task1", "wiki", "RUNNING"))
            .await
            .expect("insert task1");
        assert_eq!(
            store
                .get_task("Task1")
                .await
                .expect("get Task1")
                .expect("present")
                .status,
            "PENDING",
            "'Task1' must keep its own row"
        );
        assert_eq!(
            store
                .get_task("task1")
                .await
                .expect("get task1")
                .expect("present")
                .status,
            "RUNNING",
            "'task1' must keep its own row"
        );
        assert_eq!(store.get_all_tasks().await.expect("all tasks").len(), 2);
    }

    /// compat-6 Codex H3: `partitioned` is omitted from [`SEGMENT_COLS`],
    /// so SQLite's `INSERT OR REPLACE` resets it to its DDL default
    /// (FALSE) on every re-upsert; the PostgreSQL/MySQL upserts must
    /// reach the SAME final row — an unreset `DO UPDATE` would keep the
    /// stale prior value.
    async fn case_segment_upsert_resets_partitioned_to_default(store: &MetadataStore) {
        let seg = make_segment("wiki_2024-01_v1_0", "wiki");
        store.insert_segment(&seg).await.expect("insert");

        // Seed the vestigial column out-of-band (FerroDruid itself never
        // writes it — only external SQL could).
        store
            .backend
            .execute_raw(
                "UPDATE druid_segments SET partitioned = TRUE WHERE id = 'wiki_2024-01_v1_0'",
            )
            .await
            .expect("seed partitioned=TRUE");

        // Re-upsert the same id: every backend must land on SQLite's
        // full-row-replace outcome — partitioned back to FALSE.
        store.insert_segment(&seg).await.expect("re-upsert");

        let row = store
            .backend
            .fetch_optional(
                "SELECT partitioned FROM druid_segments WHERE id = {1}",
                &[Arg::Str("wiki_2024-01_v1_0".to_string())],
            )
            .await
            .expect("fetch partitioned")
            .expect("row present");
        assert!(
            !row.bool("partitioned").expect("decode partitioned"),
            "re-upsert must reset the omitted `partitioned` column to its DDL \
             default (FALSE) on every backend, matching SQLite's full-row replace"
        );
    }

    /// Codex 2026-07-12 round-2 HIGH #2/#3: the same datasource name must
    /// map to the SAME mutex instance (that sharing is what makes publish
    /// and admin-disable mutually exclusive), and distinct datasources
    /// must not contend.
    async fn case_datasource_publish_lock_is_shared_per_datasource(store: &MetadataStore) {
        let a1 = store.datasource_publish_lock("wiki").await;
        let a2 = store.datasource_publish_lock("wiki").await;
        let b = store.datasource_publish_lock("clicks").await;
        assert!(
            std::sync::Arc::ptr_eq(&a1, &a2),
            "same datasource must return the same lock instance"
        );
        assert!(
            !std::sync::Arc::ptr_eq(&a1, &b),
            "distinct datasources must not share a lock"
        );

        // Mutual exclusion is real: while held, a second acquire blocks.
        let guard = a1.lock().await;
        assert!(
            a2.try_lock().is_err(),
            "second handle must observe the held lock"
        );
        drop(guard);
        assert!(a2.try_lock().is_ok());
    }

    // -------------------------------------------------------------------
    // SQLite wrappers (always run in-process; same names/coverage as the
    // pre-refactor suite).
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn initialize_schema() {
        case_initialize_schema(&setup().await).await;
    }

    #[tokio::test]
    async fn insert_and_get_segments() {
        case_insert_and_get_segments(&setup().await).await;
    }

    #[tokio::test]
    async fn mark_segment_unused() {
        case_mark_segment_unused(&setup().await).await;
    }

    #[tokio::test]
    async fn multiple_data_sources() {
        case_multiple_data_sources(&setup().await).await;
    }

    #[tokio::test]
    async fn insert_and_get_supervisor() {
        case_insert_and_get_supervisor(&setup().await).await;
    }

    #[tokio::test]
    async fn get_supervisor_uses_insertion_order_not_wall_clock() {
        case_get_supervisor_uses_insertion_order_not_wall_clock(&setup().await).await;
    }

    #[tokio::test]
    async fn insert_supervisors_atomic_preserves_generation_order() {
        case_insert_supervisors_atomic_preserves_generation_order(&setup().await).await;
    }

    #[tokio::test]
    async fn get_rule_data_sources_includes_segmentless_datasource() {
        case_get_rule_data_sources_includes_segmentless_datasource(&setup().await).await;
    }

    #[tokio::test]
    async fn config_get_set() {
        case_config_get_set(&setup().await).await;
    }

    #[tokio::test]
    async fn config_upsert() {
        case_config_upsert(&setup().await).await;
    }

    #[tokio::test]
    async fn rules_get_set() {
        case_rules_get_set(&setup().await).await;
    }

    #[tokio::test]
    async fn get_rules_uses_insertion_order_not_wall_clock() {
        case_get_rules_uses_insertion_order_not_wall_clock(&setup().await).await;
    }

    #[tokio::test]
    async fn set_rules_twice_newest_wins() {
        case_set_rules_twice_newest_wins(&setup().await).await;
    }

    #[tokio::test]
    async fn get_used_segments_empty() {
        case_get_used_segments_empty(&setup().await).await;
    }

    #[tokio::test]
    async fn task_insert_get_roundtrip() {
        case_task_insert_get_roundtrip(&setup().await).await;
    }

    #[tokio::test]
    async fn task_update_status_persists() {
        case_task_update_status_persists(&setup().await).await;
    }

    #[tokio::test]
    async fn task_active_filter() {
        case_task_active_filter(&setup().await).await;
    }

    #[tokio::test]
    async fn lock_insert_get_roundtrip() {
        case_lock_insert_get_roundtrip(&setup().await).await;
    }

    #[tokio::test]
    async fn lock_revoke_filters_out() {
        case_lock_revoke_filters_out(&setup().await).await;
    }

    #[tokio::test]
    async fn lock_delete_removes() {
        case_lock_delete_removes(&setup().await).await;
    }

    #[tokio::test]
    async fn segment_replace_on_duplicate() {
        case_segment_replace_on_duplicate(&setup().await).await;
    }

    #[tokio::test]
    async fn get_segment_and_segment_exists() {
        case_get_segment_and_segment_exists(&setup().await).await;
    }

    #[tokio::test]
    async fn replace_txn_commits_both_writes() {
        case_replace_txn_commits_both_writes(&setup().await).await;
    }

    #[tokio::test]
    async fn replace_txn_failure_between_writes_leaves_no_partial_state() {
        case_replace_txn_failure_between_writes_leaves_no_partial_state(&setup().await).await;
    }

    #[tokio::test]
    async fn replace_txn_collision_with_unused_row_fails_closed() {
        case_replace_txn_collision_with_unused_row_fails_closed(&setup().await).await;
    }

    #[tokio::test]
    async fn rollback_replace_txn_deletes_new_row_and_restores_victims() {
        case_rollback_replace_txn_deletes_new_row_and_restores_victims(&setup().await).await;
    }

    #[tokio::test]
    async fn datasource_publish_lock_is_shared_per_datasource() {
        case_datasource_publish_lock_is_shared_per_datasource(&setup().await).await;
    }

    #[tokio::test]
    async fn case_distinct_identities_do_not_collide() {
        case_case_distinct_identities_do_not_collide(&setup().await).await;
    }

    #[tokio::test]
    async fn segment_upsert_resets_partitioned_to_default() {
        case_segment_upsert_resets_partitioned_to_default(&setup().await).await;
    }

    // -------------------------------------------------------------------
    // Env-gated PostgreSQL / MySQL runs of the SAME case list.
    // -------------------------------------------------------------------

    /// All Druid metadata tables, for the per-case wipe.
    const ALL_TABLES: &[&str] = &[
        "druid_segments",
        "druid_rules",
        "druid_supervisors",
        "druid_config",
        "druid_audit",
        "druid_tasklogs",
        "druid_tasklocks",
        "druid_task_status",
        "druid_task_lock_detail",
    ];

    /// Connect to `uri`, DROP every metadata table, and re-initialize —
    /// each case gets a pristine schema on the scratch server.
    async fn fresh_env_store(uri: &str) -> MetadataStore {
        let store = MetadataStore::connect(uri).await.expect("connect backend");
        for t in ALL_TABLES {
            store
                .backend
                .execute_raw(&format!("DROP TABLE IF EXISTS {t}"))
                .await
                .expect("drop table");
        }
        store.initialize().await.expect("initialize schema");
        store
    }

    /// The full behavior suite, one fresh store per case.
    async fn run_full_suite(uri: &str) {
        // First case: it DROPs every table and re-initializes, so it must
        // run serially inside the suite (never as a separate parallel
        // #[ignore] fn) against the single shared scratch DB.
        case_is_initialized(&fresh_env_store(uri).await).await;
        case_initialize_schema(&fresh_env_store(uri).await).await;
        case_insert_and_get_segments(&fresh_env_store(uri).await).await;
        case_mark_segment_unused(&fresh_env_store(uri).await).await;
        case_multiple_data_sources(&fresh_env_store(uri).await).await;
        case_insert_and_get_supervisor(&fresh_env_store(uri).await).await;
        case_get_supervisor_uses_insertion_order_not_wall_clock(&fresh_env_store(uri).await).await;
        case_insert_supervisors_atomic_preserves_generation_order(&fresh_env_store(uri).await)
            .await;
        case_get_rule_data_sources_includes_segmentless_datasource(&fresh_env_store(uri).await)
            .await;
        case_config_get_set(&fresh_env_store(uri).await).await;
        case_config_upsert(&fresh_env_store(uri).await).await;
        case_rules_get_set(&fresh_env_store(uri).await).await;
        case_get_rules_uses_insertion_order_not_wall_clock(&fresh_env_store(uri).await).await;
        case_set_rules_twice_newest_wins(&fresh_env_store(uri).await).await;
        case_get_used_segments_empty(&fresh_env_store(uri).await).await;
        case_task_insert_get_roundtrip(&fresh_env_store(uri).await).await;
        case_task_update_status_persists(&fresh_env_store(uri).await).await;
        case_task_active_filter(&fresh_env_store(uri).await).await;
        case_lock_insert_get_roundtrip(&fresh_env_store(uri).await).await;
        case_lock_revoke_filters_out(&fresh_env_store(uri).await).await;
        case_lock_delete_removes(&fresh_env_store(uri).await).await;
        case_segment_replace_on_duplicate(&fresh_env_store(uri).await).await;
        case_get_segment_and_segment_exists(&fresh_env_store(uri).await).await;
        case_replace_txn_commits_both_writes(&fresh_env_store(uri).await).await;
        case_replace_txn_failure_between_writes_leaves_no_partial_state(
            &fresh_env_store(uri).await,
        )
        .await;
        case_replace_txn_collision_with_unused_row_fails_closed(&fresh_env_store(uri).await).await;
        case_rollback_replace_txn_deletes_new_row_and_restores_victims(&fresh_env_store(uri).await)
            .await;
        case_datasource_publish_lock_is_shared_per_datasource(&fresh_env_store(uri).await).await;
        case_case_distinct_identities_do_not_collide(&fresh_env_store(uri).await).await;
        case_segment_upsert_resets_partitioned_to_default(&fresh_env_store(uri).await).await;
    }

    /// Full store suite against a real PostgreSQL server.
    ///
    /// Ignored by default; run explicitly (`cargo test -- --ignored`)
    /// with `FERRODRUID_TEST_PG_URI` pointing at a SCRATCH database —
    /// every metadata table is dropped per case. Driven by
    /// `tests/metadata-compat/`.
    #[tokio::test]
    #[ignore = "needs FERRODRUID_TEST_PG_URI → scratch PostgreSQL (see tests/metadata-compat)"]
    async fn postgres_store_suite() {
        let Ok(uri) = std::env::var("FERRODRUID_TEST_PG_URI") else {
            eprintln!("SKIP postgres_store_suite: FERRODRUID_TEST_PG_URI unset");
            return;
        };
        run_full_suite(&uri).await;
    }

    /// Full store suite against a real MySQL server.
    ///
    /// Ignored by default; run explicitly (`cargo test -- --ignored`)
    /// with `FERRODRUID_TEST_MYSQL_URI` pointing at a SCRATCH database —
    /// every metadata table is dropped per case. Driven by
    /// `tests/metadata-compat/`.
    #[tokio::test]
    #[ignore = "needs FERRODRUID_TEST_MYSQL_URI → scratch MySQL (see tests/metadata-compat)"]
    async fn mysql_store_suite() {
        let Ok(uri) = std::env::var("FERRODRUID_TEST_MYSQL_URI") else {
            eprintln!("SKIP mysql_store_suite: FERRODRUID_TEST_MYSQL_URI unset");
            return;
        };
        run_full_suite(&uri).await;
    }

    /// `is_initialized` is a read-only probe: a fresh (un-`initialize`d)
    /// store reports `false`; after `initialize()` it reports `true`.
    #[tokio::test]
    async fn is_initialized_reflects_schema_presence_sqlite() {
        let store = MetadataStore::new_in_memory()
            .await
            .expect("fresh in-memory store");
        assert!(
            !store.is_initialized().await.expect("probe fresh"),
            "a fresh store has no druid_segments table yet"
        );
        store.initialize().await.expect("initialize schema");
        assert!(
            store.is_initialized().await.expect("probe initialized"),
            "after initialize the druid_segments table exists"
        );
    }

    // -------------------------------------------------------------------
    // Dialect rendering: SQLite byte-identity snapshots + cross-backend
    // sanity.
    // -------------------------------------------------------------------

    /// Every template in this file with its bind-argument count.
    const ALL_TEMPLATES: &[(&str, usize)] = &[
        (T_GET_SEGMENT, 1),
        (T_SEGMENT_EXISTS, 1),
        (T_MARK_SEGMENT_UNUSED, 2),
        (T_INSERT_SEGMENT_PLAIN, 9),
        (T_DELETE_SEGMENT, 1),
        (T_GET_USED_SEGMENTS, 1),
        (T_GET_USED_SEGMENTS_ALL, 0),
        (T_SET_DATASOURCE_USED, 3),
        (T_GET_ALL_DATA_SOURCES, 0),
        (T_GET_ALL_SEGMENTS, 0),
        (T_GET_RULES, 1),
        (T_DELETE_RULES_ROW, 1),
        (T_INSERT_RULES_ROW, 4),
        (T_UPSERT_RULES_ROW_SQLITE, 4),
        (T_GET_RULE_DATA_SOURCES, 0),
        (T_INSERT_SUPERVISOR, 4),
        (T_GET_SUPERVISOR, 1),
        (T_GET_ALL_SUPERVISORS, 0),
        (T_UPDATE_TASK_STATUS, 5),
        (T_UPDATE_TASKLOG_PAYLOAD, 2),
        (T_GET_TASK, 1),
        (T_GET_ACTIVE_TASKS, 0),
        (T_GET_ALL_TASKS, 0),
        (T_INSERT_TASKLOCK, 2),
        (T_INSERT_LOCK_DETAIL, 7),
        (T_GET_LOCKS_DS_ALL, 1),
        (T_GET_LOCKS_DS_ACTIVE, 1),
        (T_GET_LOCKS_FOR_TASK, 1),
        (T_REVOKE_LOCK, 1),
        (T_DELETE_LOCK_DETAIL, 1),
        (T_DELETE_LOCK, 1),
        (T_GET_CONFIG, 1),
        (T_GET_ALL_CONFIG, 0),
    ];

    /// One builder-generated overwrite upsert:
    /// `(table, cols, pk, reset-cols, bind-argument count)`.
    type UpsertSpec = (
        &'static str,
        &'static [&'static str],
        &'static str,
        &'static [(&'static str, &'static str)],
        usize,
    );

    /// Every builder-generated overwrite upsert in this file — mirrors
    /// the `dialect().upsert(..)` call sites in the store.
    const ALL_UPSERTS: &[UpsertSpec] = &[
        ("druid_segments", SEGMENT_COLS, "id", SEGMENT_RESET_COLS, 9),
        ("druid_tasklogs", TASKLOG_COLS, "id", &[], 4),
        ("druid_task_status", TASK_STATUS_COLS, "id", &[], 8),
        ("druid_config", CONFIG_COLS, "name", &[], 2),
    ];

    fn dialect(kind: BackendKind) -> Dialect {
        Dialect { kind }
    }

    /// The SQLite renders must be byte-identical to the pre-refactor
    /// literal SQL (the shipped, Codex-hardened path regresses zero).
    #[test]
    fn sqlite_render_is_byte_identical_to_shipped_sql() {
        let d = dialect(BackendKind::Sqlite);
        let expect: &[(&str, usize, &str)] = &[
            (
                T_GET_SEGMENT,
                1,
                "SELECT id, dataSource, created_date, start, end, version, used, payload \
                 FROM druid_segments WHERE id = ?1",
            ),
            (
                T_SEGMENT_EXISTS,
                1,
                "SELECT 1 FROM druid_segments WHERE id = ?1 LIMIT 1",
            ),
            (
                T_MARK_SEGMENT_UNUSED,
                2,
                "UPDATE druid_segments SET used = FALSE, used_status_last_updated = ?1 \
                 WHERE id = ?2",
            ),
            (
                T_INSERT_SEGMENT_PLAIN,
                9,
                "INSERT INTO druid_segments \
                 (id, dataSource, created_date, start, end, version, used, payload, used_status_last_updated) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            ),
            (
                T_DELETE_SEGMENT,
                1,
                "DELETE FROM druid_segments WHERE id = ?1",
            ),
            (
                T_GET_USED_SEGMENTS,
                1,
                "SELECT id, dataSource, created_date, start, end, version, used, payload \
                 FROM druid_segments WHERE dataSource = ?1 AND used = TRUE",
            ),
            (
                T_GET_USED_SEGMENTS_ALL,
                0,
                "SELECT id, dataSource, created_date, start, end, version, used, payload \
                 FROM druid_segments WHERE used = TRUE ORDER BY dataSource, start",
            ),
            (
                T_SET_DATASOURCE_USED,
                3,
                "UPDATE druid_segments SET used = ?1, used_status_last_updated = ?2 \
                 WHERE dataSource = ?3",
            ),
            (
                T_GET_ALL_SEGMENTS,
                0,
                "SELECT id, dataSource, created_date, start, end, version, used, payload \
                 FROM druid_segments ORDER BY dataSource, start",
            ),
            (
                T_GET_RULES,
                1,
                "SELECT payload FROM druid_rules WHERE dataSource = ?1 ORDER BY rowid DESC LIMIT 1",
            ),
            // Executed only on PostgreSQL/MySQL (the set_rules
            // fresh-generation txn) — snapshotted anyway so NO template
            // is exempt from the completeness guard below.
            (
                T_DELETE_RULES_ROW,
                1,
                "DELETE FROM druid_rules WHERE id = ?1",
            ),
            (
                T_INSERT_RULES_ROW,
                4,
                "INSERT INTO druid_rules (id, dataSource, version, payload) \
                 VALUES (?1, ?2, ?3, ?4)",
            ),
            // The set_rules statement SQLite actually executes — the
            // shipped single-statement write, byte-identical.
            (
                T_UPSERT_RULES_ROW_SQLITE,
                4,
                "INSERT OR REPLACE INTO druid_rules (id, dataSource, version, payload) \
                 VALUES (?1, ?2, ?3, ?4)",
            ),
            (
                T_INSERT_SUPERVISOR,
                4,
                "INSERT INTO druid_supervisors (id, spec_id, created_date, payload) \
                 VALUES (?1, ?2, ?3, ?4)",
            ),
            (
                T_GET_SUPERVISOR,
                1,
                "SELECT payload FROM druid_supervisors \
                 WHERE spec_id = ?1 ORDER BY rowid DESC LIMIT 1",
            ),
            (
                T_GET_ALL_SUPERVISORS,
                0,
                "SELECT id, spec_id, created_date, payload FROM druid_supervisors \
                 ORDER BY rowid ASC",
            ),
            (
                T_UPDATE_TASK_STATUS,
                5,
                "UPDATE druid_task_status \
                 SET status = ?2, attempt = ?3, worker = ?4, payload = ?5 \
                 WHERE id = ?1",
            ),
            (
                T_UPDATE_TASKLOG_PAYLOAD,
                2,
                "UPDATE druid_tasklogs SET payload = ?2 WHERE id = ?1",
            ),
            (
                T_INSERT_TASKLOCK,
                2,
                "INSERT INTO druid_tasklocks (task_id, lock_payload) VALUES (?1, ?2)",
            ),
            (
                T_INSERT_LOCK_DETAIL,
                7,
                "INSERT INTO druid_task_lock_detail \
                 (id, datasource, interval_start, interval_end, lock_type, priority, revoked) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            ),
            (
                T_GET_LOCKS_DS_ALL,
                1,
                "SELECT l.id, l.task_id, l.lock_payload, d.datasource, d.interval_start, \
                 d.interval_end, d.lock_type, d.priority, d.revoked \
                 FROM druid_tasklocks l \
                 JOIN druid_task_lock_detail d ON d.id = l.id \
                 WHERE d.datasource = ?1 ORDER BY d.priority DESC, l.id",
            ),
            (
                T_GET_LOCKS_DS_ACTIVE,
                1,
                "SELECT l.id, l.task_id, l.lock_payload, d.datasource, d.interval_start, \
                 d.interval_end, d.lock_type, d.priority, d.revoked \
                 FROM druid_tasklocks l \
                 JOIN druid_task_lock_detail d ON d.id = l.id \
                 WHERE d.datasource = ?1 AND d.revoked = FALSE ORDER BY d.priority DESC, l.id",
            ),
            (
                T_GET_LOCKS_FOR_TASK,
                1,
                "SELECT l.id, l.task_id, l.lock_payload, d.datasource, d.interval_start, \
                 d.interval_end, d.lock_type, d.priority, d.revoked \
                 FROM druid_tasklocks l \
                 JOIN druid_task_lock_detail d ON d.id = l.id \
                 WHERE l.task_id = ?1 ORDER BY l.id",
            ),
            (
                T_REVOKE_LOCK,
                1,
                "UPDATE druid_task_lock_detail SET revoked = TRUE WHERE id = ?1",
            ),
            (
                T_DELETE_LOCK_DETAIL,
                1,
                "DELETE FROM druid_task_lock_detail WHERE id = ?1",
            ),
            (
                T_DELETE_LOCK,
                1,
                "DELETE FROM druid_tasklocks WHERE id = ?1",
            ),
            (
                T_GET_ALL_DATA_SOURCES,
                0,
                "SELECT DISTINCT dataSource FROM druid_segments ORDER BY dataSource",
            ),
            (
                T_GET_RULE_DATA_SOURCES,
                0,
                "SELECT DISTINCT dataSource FROM druid_rules ORDER BY dataSource",
            ),
            (
                T_GET_TASK,
                1,
                "SELECT id, task_type, datasource, status, created_date, attempt, worker, payload \
                 FROM druid_task_status WHERE id = ?1",
            ),
            (
                T_GET_ACTIVE_TASKS,
                0,
                "SELECT id, task_type, datasource, status, created_date, attempt, worker, payload \
                 FROM druid_task_status \
                 WHERE status IN ('WAITING', 'PENDING', 'RUNNING') \
                 ORDER BY created_date",
            ),
            (
                T_GET_ALL_TASKS,
                0,
                "SELECT id, task_type, datasource, status, created_date, attempt, worker, payload \
                 FROM druid_task_status ORDER BY created_date",
            ),
            (
                T_GET_CONFIG,
                1,
                "SELECT payload FROM druid_config WHERE name = ?1",
            ),
            (
                T_GET_ALL_CONFIG,
                0,
                "SELECT name, payload FROM druid_config ORDER BY name",
            ),
        ];
        for (tpl, argc, want) in expect {
            let got = d.render(tpl, *argc).expect("render");
            assert_eq!(&got.sql, want, "template: {tpl}");
        }

        // Completeness self-guard: the snapshot must cover EVERY entry of
        // ALL_TEMPLATES (template text AND bind count) — a template added
        // to ALL_TEMPLATES without a byte-identity snapshot here FAILS the
        // test instead of silently going uncovered (compat-6 Codex H2:
        // T_DELETE_RULES_ROW was in ALL_TEMPLATES but never snapshotted).
        let snapshotted: std::collections::BTreeMap<&str, usize> =
            expect.iter().map(|(t, n, _)| (*t, *n)).collect();
        let registry: std::collections::BTreeMap<&str, usize> =
            ALL_TEMPLATES.iter().map(|(t, n)| (*t, *n)).collect();
        assert_eq!(
            snapshotted.len(),
            expect.len(),
            "duplicate template entries in the snapshot list"
        );
        assert_eq!(
            snapshotted, registry,
            "the SQLite byte-identity snapshot must cover every template \
             in ALL_TEMPLATES (and nothing else)"
        );

        // The overwrite upserts, generated by the builder, must also be
        // byte-identical to the shipped `INSERT OR REPLACE` statements —
        // and every ALL_UPSERTS entry must have a snapshot (same
        // self-guard as above: a new upsert without one fails here).
        let upsert_expect: &[(&str, &str)] = &[
            (
                "druid_segments",
                "INSERT OR REPLACE INTO druid_segments \
                 (id, dataSource, created_date, start, end, version, used, payload, used_status_last_updated) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            ),
            (
                "druid_tasklogs",
                "INSERT OR REPLACE INTO druid_tasklogs (id, created_date, datasource, payload) \
                 VALUES (?1, ?2, ?3, ?4)",
            ),
            (
                "druid_task_status",
                "INSERT OR REPLACE INTO druid_task_status \
                 (id, task_type, datasource, status, created_date, attempt, worker, payload) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            ),
            (
                "druid_config",
                "INSERT OR REPLACE INTO druid_config (name, payload) VALUES (?1, ?2)",
            ),
        ];
        assert_eq!(
            upsert_expect.len(),
            ALL_UPSERTS.len(),
            "the upsert snapshot list must cover every ALL_UPSERTS entry"
        );
        for (table, cols, pk, reset, argc) in ALL_UPSERTS {
            let want = upsert_expect
                .iter()
                .find(|(t, _)| t == table)
                .map(|(_, sql)| *sql)
                .unwrap_or_else(|| panic!("upsert for table {table} missing from the snapshot"));
            assert_eq!(
                d.render(&d.upsert(table, cols, pk, reset), *argc)
                    .expect("render")
                    .sql,
                want,
                "upsert for table {table}"
            );
        }
    }

    /// Every template must render cleanly for every backend: no leftover
    /// `{`/`}` tokens, and the placeholder count must match the bind
    /// count.
    #[test]
    fn all_templates_render_on_all_backends() {
        for kind in [
            BackendKind::Sqlite,
            BackendKind::Postgres,
            BackendKind::MySql,
        ] {
            let d = dialect(kind);
            let mut templates: Vec<(String, usize)> = ALL_TEMPLATES
                .iter()
                .map(|(t, n)| ((*t).to_string(), *n))
                .collect();
            for (table, cols, pk, reset, argc) in ALL_UPSERTS {
                templates.push((d.upsert(table, cols, pk, reset), *argc));
            }
            for (tpl, argc) in &templates {
                let got = d
                    .render(tpl, *argc)
                    .unwrap_or_else(|e| panic!("render failed for {kind:?}: {e}"));
                assert!(
                    !got.sql.contains('{') && !got.sql.contains('}'),
                    "unrendered token for {kind:?}: {}",
                    got.sql
                );
                assert_eq!(
                    got.bind_order.len(),
                    *argc,
                    "bind count mismatch for {kind:?}: {}",
                    got.sql
                );
                if kind == BackendKind::MySql {
                    assert_eq!(
                        got.sql.matches('?').count(),
                        *argc,
                        "MySQL positional placeholder count mismatch: {}",
                        got.sql
                    );
                }
            }
        }
    }

    /// compat-6 Codex H2: `set_rules` statements are backend-specific.
    /// SQLite executes the single shipped `INSERT OR REPLACE`
    /// (byte-identical; its implicit delete + re-insert takes a fresh
    /// `rowid`, so newest-generation-wins holds), while PostgreSQL and
    /// MySQL — which have no `OR REPLACE` — run an explicit
    /// DELETE-exact-id + INSERT transaction that frees the colliding
    /// primary key and re-inserts at a fresh `seq`. The behavioral halves
    /// live in `case_set_rules_twice_newest_wins` (SQLite in-process;
    /// PostgreSQL/MySQL via the env-gated `run_full_suite`).
    #[test]
    fn set_rules_sql_is_backend_specific() {
        // SQLite: the one statement set_rules executes, byte-identical
        // to the shipped SQL. No separate DELETE statement exists on
        // this path (a distinct DELETE is observably different — e.g.
        // it can fire DELETE triggers the OR REPLACE conflict path does
        // not, and it breaks the byte-identity contract).
        let d = dialect(BackendKind::Sqlite);
        assert_eq!(
            d.render(T_UPSERT_RULES_ROW_SQLITE, 4).expect("render").sql,
            "INSERT OR REPLACE INTO druid_rules (id, dataSource, version, payload) \
             VALUES (?1, ?2, ?3, ?4)"
        );

        // PostgreSQL/MySQL: the two statements of the fresh-generation
        // txn. The DELETE targets the exact PK `id` — never the
        // datasource — so prior generations survive.
        let pg = dialect(BackendKind::Postgres);
        assert_eq!(
            pg.render(T_DELETE_RULES_ROW, 1).expect("render").sql,
            "DELETE FROM druid_rules WHERE id = $1"
        );
        assert_eq!(
            pg.render(T_INSERT_RULES_ROW, 4).expect("render").sql,
            "INSERT INTO druid_rules (id, dataSource, version, payload) VALUES ($1, $2, $3, $4)"
        );
        let my = dialect(BackendKind::MySql);
        assert_eq!(
            my.render(T_DELETE_RULES_ROW, 1).expect("render").sql,
            "DELETE FROM druid_rules WHERE id = ?"
        );
        assert_eq!(
            my.render(T_INSERT_RULES_ROW, 4).expect("render").sql,
            "INSERT INTO druid_rules (id, dataSource, version, payload) VALUES (?, ?, ?, ?)"
        );
    }

    /// PostgreSQL/MySQL specifics for the correctness-critical renders:
    /// generation ordering by `seq`, quoted reserved identifiers, and the
    /// MySQL bind-order permutation for the one out-of-textual-order
    /// template.
    #[test]
    fn dialect_specific_renders() {
        let pg = dialect(BackendKind::Postgres);
        let my = dialect(BackendKind::MySql);

        // Generation ordering: rules + supervisors must order by seq.
        assert_eq!(
            pg.render(T_GET_RULES, 1).expect("render").sql,
            "SELECT payload FROM druid_rules WHERE dataSource = $1 ORDER BY seq DESC LIMIT 1"
        );
        assert_eq!(
            my.render(T_GET_SUPERVISOR, 1).expect("render").sql,
            "SELECT payload FROM druid_supervisors WHERE spec_id = ? ORDER BY seq DESC LIMIT 1"
        );
        assert_eq!(
            pg.render(T_GET_ALL_SUPERVISORS, 0).expect("render").sql,
            "SELECT id, spec_id, created_date, payload FROM druid_supervisors ORDER BY seq ASC"
        );

        // Reserved identifiers are quoted per backend.
        assert_eq!(
            pg.render(T_GET_SEGMENT, 1).expect("render").sql,
            "SELECT id, dataSource, created_date, \"start\", \"end\", version, used, payload \
             FROM druid_segments WHERE id = $1"
        );
        assert_eq!(
            my.render(T_GET_SEGMENT, 1).expect("render").sql,
            "SELECT id, dataSource, created_date, `start`, `end`, version, used, payload \
             FROM druid_segments WHERE id = ?"
        );

        // The overwrite upsert, per backend.
        assert_eq!(
            pg.upsert("druid_config", CONFIG_COLS, "name", &[]),
            "INSERT INTO druid_config (name, payload) VALUES ({1}, {2}) \
             ON CONFLICT (name) DO UPDATE SET payload = EXCLUDED.payload"
        );
        assert_eq!(
            my.upsert("druid_config", CONFIG_COLS, "name", &[]),
            "INSERT INTO druid_config (name, payload) VALUES ({1}, {2}) \
             ON DUPLICATE KEY UPDATE payload = VALUES(payload)"
        );
        // The segments upsert also RESETS the omitted `partitioned`
        // column to its DDL default (compat-6 Codex H3) — matching
        // SQLite's full-row-replace semantics.
        assert_eq!(
            pg.render(
                &pg.upsert("druid_segments", SEGMENT_COLS, "id", SEGMENT_RESET_COLS),
                9
            )
            .expect("render")
            .sql,
            "INSERT INTO druid_segments \
             (id, dataSource, created_date, \"start\", \"end\", version, used, payload, used_status_last_updated) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (id) DO UPDATE SET dataSource = EXCLUDED.dataSource, \
             created_date = EXCLUDED.created_date, \"start\" = EXCLUDED.\"start\", \
             \"end\" = EXCLUDED.\"end\", version = EXCLUDED.version, used = EXCLUDED.used, \
             payload = EXCLUDED.payload, used_status_last_updated = EXCLUDED.used_status_last_updated, \
             partitioned = FALSE"
        );
        assert_eq!(
            my.render(
                &my.upsert("druid_segments", SEGMENT_COLS, "id", SEGMENT_RESET_COLS),
                9
            )
            .expect("render")
            .sql,
            "INSERT INTO druid_segments \
             (id, dataSource, created_date, `start`, `end`, version, used, payload, used_status_last_updated) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE dataSource = VALUES(dataSource), \
             created_date = VALUES(created_date), `start` = VALUES(`start`), \
             `end` = VALUES(`end`), version = VALUES(version), used = VALUES(used), \
             payload = VALUES(payload), used_status_last_updated = VALUES(used_status_last_updated), \
             partitioned = FALSE"
        );

        // MySQL positional binding: T_UPDATE_TASK_STATUS's textual order is
        // {2}{3}{4}{5}{1}, so args (bound as [id, status, attempt, worker,
        // payload]) must be permuted to [1, 2, 3, 4, 0].
        let r = my.render(T_UPDATE_TASK_STATUS, 5).expect("render");
        assert_eq!(
            r.sql,
            "UPDATE druid_task_status SET status = ?, attempt = ?, worker = ?, payload = ? \
             WHERE id = ?"
        );
        assert_eq!(r.bind_order, vec![1, 2, 3, 4, 0]);

        // SQLite/PostgreSQL use numbered placeholders → identity order.
        let r = pg.render(T_UPDATE_TASK_STATUS, 5).expect("render");
        assert_eq!(r.bind_order, vec![0, 1, 2, 3, 4]);
    }

    /// compat-6 Codex H3: `partitioned` is omitted from [`SEGMENT_COLS`],
    /// so SQLite's full-row `INSERT OR REPLACE` resets it to its DDL
    /// default (FALSE) on every re-upsert — while a `DO UPDATE` that only
    /// touches the listed columns would keep the prior row's STALE value
    /// on PostgreSQL/MySQL. Their update clauses must reset it
    /// explicitly; the SQLite render must NOT change (byte-identity).
    #[test]
    fn segment_upsert_resets_omitted_partitioned_on_pg_mysql() {
        for kind in [BackendKind::Postgres, BackendKind::MySql] {
            let sql =
                dialect(kind).upsert("druid_segments", SEGMENT_COLS, "id", SEGMENT_RESET_COLS);
            assert!(
                sql.ends_with("partitioned = FALSE"),
                "{kind:?} druid_segments upsert must reset the omitted \
                 `partitioned` column to its DDL default: {sql}"
            );
        }
        assert!(
            !dialect(BackendKind::Sqlite)
                .upsert("druid_segments", SEGMENT_COLS, "id", SEGMENT_RESET_COLS)
                .contains("partitioned"),
            "SQLite INSERT OR REPLACE already resets omitted columns implicitly; \
             its SQL must stay byte-identical to the shipped statement"
        );
    }

    /// The renderer fails loudly on malformed templates / arg-count
    /// mismatches instead of silently mis-binding.
    #[test]
    fn render_rejects_bad_templates() {
        let d = dialect(BackendKind::MySql);
        // Token beyond the bound arg count.
        assert!(d.render("SELECT {2}", 1).is_err());
        // An arg that is never referenced.
        assert!(d.render("SELECT {1}", 2).is_err());
        // Unknown word token.
        assert!(d.render("SELECT {bogus}", 0).is_err());
        // Unterminated token.
        assert!(d.render("SELECT {1", 1).is_err());
        // Zero is not a valid placeholder.
        assert!(d.render("SELECT {0}", 1).is_err());
    }

    /// `connect` dispatches `:memory:` to a working in-memory SQLite
    /// store (scheme parsing itself is unit-tested in `crate::uri`).
    #[tokio::test]
    async fn connect_memory_dispatches_to_sqlite() {
        let store = MetadataStore::connect(":memory:").await.expect("connect");
        store.initialize().await.expect("initialize");
        store
            .insert_segment(&make_segment("seg1", "wiki"))
            .await
            .expect("insert");
        assert_eq!(
            store.get_used_segments("wiki").await.expect("used").len(),
            1
        );
    }
}
