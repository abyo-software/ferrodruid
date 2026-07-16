// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Core metadata store implementation backed by SQLite (Phase 1).

use ferrodruid_common::{DruidError, Result};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

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
// MetadataStore
// ---------------------------------------------------------------------------

/// Relational metadata store for FerroDruid (SQLite backend for Phase 1).
///
/// Manages the Druid-compatible schema covering segments, rules,
/// supervisors, config, audit entries, task logs, and task locks.
pub struct MetadataStore {
    pool: SqlitePool,
    /// Per-datasource publish mutexes (see [`datasource_publish_lock`]).
    ///
    /// [`datasource_publish_lock`]: MetadataStore::datasource_publish_lock
    publish_locks: tokio::sync::Mutex<std::collections::HashMap<String, PublishLock>>,
}

/// Shared handle to a per-datasource publish mutex.
///
/// See [`MetadataStore::datasource_publish_lock`] for the protocol.
pub type PublishLock = std::sync::Arc<tokio::sync::Mutex<()>>;

impl MetadataStore {
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

        Ok(Self {
            pool,
            publish_locks: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        })
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

        Ok(Self {
            pool,
            publish_locks: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        })
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
    /// the same SQLite file (the standalone role binaries, the
    /// `ferrodruid-migrate` import tool, or any out-of-band SQL writer)
    /// are NOT serialized by it; concurrent used-flag mutation from a
    /// second process during a publish is unsupported.
    pub async fn datasource_publish_lock(&self, data_source: &str) -> PublishLock {
        let mut map = self.publish_locks.lock().await;
        std::sync::Arc::clone(
            map.entry(data_source.to_string())
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Run `CREATE TABLE IF NOT EXISTS` for every Druid metadata table.
    pub async fn initialize(&self) -> Result<()> {
        sqlx::query(SCHEMA_SQL)
            .execute(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("initialize schema: {e}")))?;
        for stmt in SCHEMA_EXT_SQL {
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .map_err(|e| DruidError::Metadata(format!("initialize schema ext: {e}")))?;
        }
        Ok(())
    }

    // ----- Segments --------------------------------------------------------

    /// Insert (or replace) a segment metadata row.
    pub async fn insert_segment(&self, segment: &SegmentMetadataRow) -> Result<()> {
        let payload_str = serde_json::to_string(&segment.payload)
            .map_err(|e| DruidError::Metadata(format!("serialize payload: {e}")))?;

        sqlx::query(
            "INSERT OR REPLACE INTO druid_segments \
             (id, dataSource, created_date, start, end, version, used, payload, used_status_last_updated) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .bind(&segment.id)
        .bind(&segment.data_source)
        .bind(&segment.created_date)
        .bind(&segment.start)
        .bind(&segment.end)
        .bind(&segment.version)
        .bind(segment.used)
        .bind(&payload_str)
        .bind(&segment.created_date)
        .execute(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("insert segment: {e}")))?;

        Ok(())
    }

    /// Fetch a single segment metadata row by id (used or unused).
    pub async fn get_segment(&self, id: &str) -> Result<Option<SegmentMetadataRow>> {
        let row = sqlx::query(
            "SELECT id, dataSource, created_date, start, end, version, used, payload \
             FROM druid_segments WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
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
    /// `INSERT OR REPLACE`, destroying its audit history.
    pub async fn segment_exists(&self, id: &str) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM druid_segments WHERE id = ?1 LIMIT 1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("segment exists: {e}")))?;
        Ok(row.is_some())
    }

    /// Atomically publish a replace: mark every id in `unused_ids` unused
    /// AND insert `new_row`, as ONE SQLite transaction (Codex 2026-07-12
    /// round-2 HIGH #1).
    ///
    /// Pre-fix, the publication critical section issued these as separate
    /// autocommit statements; a crash, panic, or cancellation between them
    /// left partial durable state (some victims unused without the new
    /// row, or vice versa) that no in-memory rollback can undo after a
    /// restart. Here either every write commits or none does — SQLite's
    /// journal guarantees the same atomicity across a process crash
    /// mid-transaction.
    ///
    /// The new row is inserted with a plain `INSERT` (NOT `INSERT OR
    /// REPLACE`): a pre-existing row with the same id — a segment-id
    /// collision that would otherwise silently overwrite another task's
    /// publication (round-2 HIGH #4) — aborts and rolls back the whole
    /// transaction, victims included. Callers must hold the datasource's
    /// [`datasource_publish_lock`] so the plan this write applies cannot
    /// go stale.
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
            .pool
            .begin()
            .await
            .map_err(|e| DruidError::Metadata(format!("begin replace txn: {e}")))?;

        for id in unused_ids {
            sqlx::query(
                "UPDATE druid_segments SET used = FALSE, used_status_last_updated = ?1 \
                 WHERE id = ?2",
            )
            .bind(&now)
            .bind(id)
            .execute(&mut *txn)
            .await
            .map_err(|e| {
                DruidError::Metadata(format!("replace txn: mark segment '{id}' unused: {e}"))
            })?;
        }

        sqlx::query(
            "INSERT INTO druid_segments \
             (id, dataSource, created_date, start, end, version, used, payload, used_status_last_updated) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .bind(&new_row.id)
        .bind(&new_row.data_source)
        .bind(&new_row.created_date)
        .bind(&new_row.start)
        .bind(&new_row.end)
        .bind(&new_row.version)
        .bind(new_row.used)
        .bind(&payload_str)
        .bind(&new_row.created_date)
        .execute(&mut *txn)
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
            .pool
            .begin()
            .await
            .map_err(|e| DruidError::Metadata(format!("begin rollback txn: {e}")))?;

        sqlx::query("DELETE FROM druid_segments WHERE id = ?1")
            .bind(new_segment_id)
            .execute(&mut *txn)
            .await
            .map_err(|e| {
                DruidError::Metadata(format!(
                    "rollback txn: delete new segment row '{new_segment_id}': {e}"
                ))
            })?;

        for seg in restore {
            let payload_str = serde_json::to_string(&seg.payload)
                .map_err(|e| DruidError::Metadata(format!("serialize payload: {e}")))?;
            sqlx::query(
                "INSERT OR REPLACE INTO druid_segments \
                 (id, dataSource, created_date, start, end, version, used, payload, used_status_last_updated) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )
            .bind(&seg.id)
            .bind(&seg.data_source)
            .bind(&seg.created_date)
            .bind(&seg.start)
            .bind(&seg.end)
            .bind(&seg.version)
            .bind(seg.used)
            .bind(&payload_str)
            .bind(&seg.created_date)
            .execute(&mut *txn)
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
            .pool
            .begin()
            .await
            .map_err(|e| DruidError::Metadata(format!("begin delete-segments txn: {e}")))?;
        for id in ids {
            sqlx::query("DELETE FROM druid_segments WHERE id = ?1")
                .bind(id)
                .execute(&mut *txn)
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
        let rows = sqlx::query(
            "SELECT id, dataSource, created_date, start, end, version, used, payload \
             FROM druid_segments WHERE dataSource = ?1 AND used = TRUE",
        )
        .bind(data_source)
        .fetch_all(&self.pool)
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
        let rows = sqlx::query(
            "SELECT id, dataSource, created_date, start, end, version, used, payload \
             FROM druid_segments WHERE used = TRUE ORDER BY dataSource, start",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("get all used segments: {e}")))?;

        rows.iter().map(row_to_segment).collect()
    }

    /// Mark a segment as unused.
    pub async fn mark_segment_unused(&self, id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE druid_segments SET used = FALSE, used_status_last_updated = ?1 WHERE id = ?2",
        )
        .bind(&now)
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("mark segment unused: {e}")))?;
        Ok(())
    }

    /// Set the `used` flag for EVERY segment of a data source ATOMICALLY, in a
    /// single `UPDATE` statement (Codex 2026-07-12). The datasource-wide
    /// disable/enable must not be a loop of per-segment autocommits — a
    /// cancellation or a mid-loop failure would leave only a subset of the
    /// data source disabled/enabled, a durable partial administrative state.
    /// One statement is inherently atomic in SQLite (all rows or none).
    /// Callers hold the datasource's
    /// [`MetadataStore::datasource_publish_lock`] so this cannot interleave
    /// with a publish.
    pub async fn set_datasource_used(&self, data_source: &str, used: bool) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE druid_segments SET used = ?1, used_status_last_updated = ?2 \
             WHERE dataSource = ?3",
        )
        .bind(used)
        .bind(&now)
        .bind(data_source)
        .execute(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("set datasource used={used}: {e}")))?;
        Ok(())
    }

    /// Return the distinct data source names across all segments (used or not).
    pub async fn get_all_data_sources(&self) -> Result<Vec<String>> {
        let rows =
            sqlx::query("SELECT DISTINCT dataSource FROM druid_segments ORDER BY dataSource")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DruidError::Metadata(format!("get all data sources: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let ds: String = r
                .try_get("dataSource")
                .map_err(|e| DruidError::Metadata(format!("decode dataSource: {e}")))?;
            out.push(ds);
        }
        Ok(out)
    }

    /// Return all segments across all data sources.
    pub async fn get_all_segments(&self) -> Result<Vec<SegmentMetadataRow>> {
        let rows = sqlx::query(
            "SELECT id, dataSource, created_date, start, end, version, used, payload \
             FROM druid_segments ORDER BY dataSource, start",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("get all segments: {e}")))?;

        rows.iter().map(row_to_segment).collect()
    }

    // ----- Rules -----------------------------------------------------------

    /// Return the rule list (JSON array) for a data source.
    ///
    /// Ordered by the monotonic insertion `rowid`, NOT the wall-clock
    /// `version` text: [`set_rules`](Self::set_rules) stamps `version` from
    /// the system clock, so if the clock steps BACKWARD between two rule
    /// updates (NTP step, VM snapshot restore), a `version DESC` order would
    /// keep serving the OLDER generation forever — a newer ACKed update
    /// would never be returned again. Rule rows are only ever inserted
    /// (`INSERT OR REPLACE` deletes-then-inserts, taking a fresh rowid, and
    /// `druid_rules` is not `WITHOUT ROWID`), so the highest rowid is the
    /// newest ACKed update — same discipline as the Codex R16
    /// [`get_supervisor`](Self::get_supervisor) fix.
    pub async fn get_rules(&self, data_source: &str) -> Result<Vec<serde_json::Value>> {
        let row = sqlx::query(
            "SELECT payload FROM druid_rules WHERE dataSource = ?1 ORDER BY rowid DESC LIMIT 1",
        )
        .bind(data_source)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("get rules: {e}")))?;

        match row {
            Some(r) => {
                let payload_str: String = r
                    .try_get("payload")
                    .map_err(|e| DruidError::Metadata(format!("decode rules payload: {e}")))?;
                let rules: Vec<serde_json::Value> = serde_json::from_str(&payload_str)?;
                Ok(rules)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Set (replace) the rules for a data source.
    pub async fn set_rules(&self, data_source: &str, rules: &[serde_json::Value]) -> Result<()> {
        let payload_str = serde_json::to_string(rules)?;
        let version = chrono::Utc::now().to_rfc3339();
        let id = format!("{data_source}_{version}");

        sqlx::query(
            "INSERT OR REPLACE INTO druid_rules (id, dataSource, version, payload) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(&id)
        .bind(data_source)
        .bind(&version)
        .bind(&payload_str)
        .execute(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("set rules: {e}")))?;

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
        let rows = sqlx::query("SELECT DISTINCT dataSource FROM druid_rules ORDER BY dataSource")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("get rule data sources: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let ds: String = r
                .try_get("dataSource")
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

        sqlx::query(
            "INSERT INTO druid_supervisors (id, spec_id, created_date, payload) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(&id)
        .bind(spec_id)
        .bind(&now)
        .bind(&payload_str)
        .execute(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("insert supervisor: {e}")))?;

        Ok(())
    }

    /// Insert MULTIPLE supervisor generations ATOMICALLY, in the given
    /// order, as ONE SQLite transaction.
    ///
    /// Built for restore/import: a per-row autocommit loop that fails midway
    /// (SQLITE_BUSY, disk full) leaves a half-imported history in which a
    /// pre-tombstone ACTIVE spec can be the newest generation, resurrecting
    /// a stopped supervisor on the next restart. Here either every
    /// generation commits or none does.
    ///
    /// Insertion order is preserved exactly (entries are inserted first to
    /// last), so the restored rowids keep generation order and
    /// [`get_supervisor`](Self::get_supervisor) (rowid DESC) still returns
    /// the newest generation. Row ids carry a per-entry index suffix so two
    /// generations of the same spec inserted within one clock reading cannot
    /// collide on the primary key and abort the whole transaction.
    pub async fn insert_supervisors_atomic(
        &self,
        entries: &[(String, serde_json::Value)],
    ) -> Result<()> {
        let mut txn = self
            .pool
            .begin()
            .await
            .map_err(|e| DruidError::Metadata(format!("begin supervisors txn: {e}")))?;

        for (i, (spec_id, spec)) in entries.iter().enumerate() {
            let payload_str = serde_json::to_string(spec)?;
            let now = chrono::Utc::now().to_rfc3339();
            let id = format!("{spec_id}_{now}_{i}");

            sqlx::query(
                "INSERT INTO druid_supervisors (id, spec_id, created_date, payload) \
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(&id)
            .bind(spec_id)
            .bind(&now)
            .bind(&payload_str)
            .execute(&mut *txn)
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
    /// Ordered by the monotonic insertion `rowid`, NOT `created_date`: the id
    /// and `created_date` are wall-clock text, so if the system clock steps
    /// BACKWARD between an active spec and its later shutdown tombstone, a
    /// `created_date DESC` order would return the OLDER active spec and a
    /// restart would RESURRECT a stopped supervisor. `rowid` reflects true
    /// insertion order (supervisor rows are only ever inserted, never deleted),
    /// so the newest generation always wins (Codex R16).
    pub async fn get_supervisor(&self, spec_id: &str) -> Result<Option<serde_json::Value>> {
        let row = sqlx::query(
            "SELECT payload FROM druid_supervisors \
             WHERE spec_id = ?1 ORDER BY rowid DESC LIMIT 1",
        )
        .bind(spec_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("get supervisor: {e}")))?;

        match row {
            Some(r) => {
                let payload_str: String = r
                    .try_get("payload")
                    .map_err(|e| DruidError::Metadata(format!("decode supervisor payload: {e}")))?;
                let val: serde_json::Value = serde_json::from_str(&payload_str)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Return all supervisor rows.
    /// Return all supervisor generations OLDEST-first, by monotonic insertion
    /// `rowid` (not wall-clock `created_date`).
    ///
    /// Oldest-first matters for backup/restore: an exporter emits this order
    /// and an importer re-inserts it forward, so the restored `rowid`s preserve
    /// generation order and [`get_supervisor`](Self::get_supervisor) (rowid
    /// DESC) still returns the newest generation — a `created_date DESC` export
    /// would otherwise invert the order and resurrect a stopped supervisor after
    /// a restore (Codex R17; same root cause as the R16 get_supervisor fix).
    pub async fn get_all_supervisors(&self) -> Result<Vec<SupervisorRow>> {
        let rows = sqlx::query(
            "SELECT id, spec_id, created_date, payload FROM druid_supervisors \
             ORDER BY rowid ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("get all supervisors: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let id: String = r
                .try_get("id")
                .map_err(|e| DruidError::Metadata(format!("decode id: {e}")))?;
            let spec_id: String = r
                .try_get("spec_id")
                .map_err(|e| DruidError::Metadata(format!("decode spec_id: {e}")))?;
            let created_date: String = r
                .try_get("created_date")
                .map_err(|e| DruidError::Metadata(format!("decode created_date: {e}")))?;
            let payload_str: String = r
                .try_get("payload")
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

        sqlx::query(
            "INSERT OR REPLACE INTO druid_tasklogs (id, created_date, datasource, payload) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(&task.id)
        .bind(&task.created_date)
        .bind(&task.data_source)
        .bind(&payload_str)
        .execute(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("insert task log: {e}")))?;

        sqlx::query(
            "INSERT OR REPLACE INTO druid_task_status \
             (id, task_type, datasource, status, created_date, attempt, worker, payload) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(&task.id)
        .bind(&task.task_type)
        .bind(&task.data_source)
        .bind(&task.status)
        .bind(&task.created_date)
        .bind(task.attempt)
        .bind(&task.worker)
        .bind(&payload_str)
        .execute(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("insert task status: {e}")))?;

        Ok(())
    }

    /// Update the mutable fields of a persisted task (status, attempt,
    /// worker, payload).  No-op if the task does not exist.
    pub async fn update_task_status(&self, task: &TaskRow) -> Result<()> {
        let payload_str = serde_json::to_string(&task.payload)
            .map_err(|e| DruidError::Metadata(format!("serialize task payload: {e}")))?;

        sqlx::query(
            "UPDATE druid_task_status \
             SET status = ?2, attempt = ?3, worker = ?4, payload = ?5 \
             WHERE id = ?1",
        )
        .bind(&task.id)
        .bind(&task.status)
        .bind(task.attempt)
        .bind(&task.worker)
        .bind(&payload_str)
        .execute(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("update task status: {e}")))?;

        sqlx::query("UPDATE druid_tasklogs SET payload = ?2 WHERE id = ?1")
            .bind(&task.id)
            .bind(&payload_str)
            .execute(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("update task log payload: {e}")))?;

        Ok(())
    }

    /// Fetch a single task by identifier.
    pub async fn get_task(&self, id: &str) -> Result<Option<TaskRow>> {
        let row = sqlx::query(
            "SELECT id, task_type, datasource, status, created_date, attempt, worker, payload \
             FROM druid_task_status WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
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
        let rows = sqlx::query(
            "SELECT id, task_type, datasource, status, created_date, attempt, worker, payload \
             FROM druid_task_status \
             WHERE status IN ('WAITING', 'PENDING', 'RUNNING') \
             ORDER BY created_date",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("get active tasks: {e}")))?;

        rows.iter().map(row_to_task).collect()
    }

    /// Return every persisted task row, ordered by creation time.
    pub async fn get_all_tasks(&self) -> Result<Vec<TaskRow>> {
        let rows = sqlx::query(
            "SELECT id, task_type, datasource, status, created_date, attempt, worker, payload \
             FROM druid_task_status ORDER BY created_date",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("get all tasks: {e}")))?;

        rows.iter().map(row_to_task).collect()
    }

    // ----- Task locks ------------------------------------------------------

    /// Insert a task lock and return its persisted identifier.
    ///
    /// Writes the canonical `druid_tasklocks` row plus the additive
    /// `druid_task_lock_detail` row keyed by the same rowid.
    pub async fn insert_task_lock(&self, lock: &TaskLockRow) -> Result<String> {
        let payload_str = serde_json::to_string(&lock.payload)
            .map_err(|e| DruidError::Metadata(format!("serialize lock payload: {e}")))?;

        let result =
            sqlx::query("INSERT INTO druid_tasklocks (task_id, lock_payload) VALUES (?1, ?2)")
                .bind(&lock.task_id)
                .bind(&payload_str)
                .execute(&self.pool)
                .await
                .map_err(|e| DruidError::Metadata(format!("insert task lock: {e}")))?;

        let rowid = result.last_insert_rowid();

        sqlx::query(
            "INSERT INTO druid_task_lock_detail \
             (id, datasource, interval_start, interval_end, lock_type, priority, revoked) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(rowid)
        .bind(&lock.data_source)
        .bind(&lock.interval_start)
        .bind(&lock.interval_end)
        .bind(&lock.lock_type)
        .bind(lock.priority)
        .bind(lock.revoked)
        .execute(&self.pool)
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
        let sql = if include_revoked {
            "SELECT l.id, l.task_id, l.lock_payload, d.datasource, d.interval_start, \
             d.interval_end, d.lock_type, d.priority, d.revoked \
             FROM druid_tasklocks l \
             JOIN druid_task_lock_detail d ON d.id = l.id \
             WHERE d.datasource = ?1 ORDER BY d.priority DESC, l.id"
        } else {
            "SELECT l.id, l.task_id, l.lock_payload, d.datasource, d.interval_start, \
             d.interval_end, d.lock_type, d.priority, d.revoked \
             FROM druid_tasklocks l \
             JOIN druid_task_lock_detail d ON d.id = l.id \
             WHERE d.datasource = ?1 AND d.revoked = FALSE ORDER BY d.priority DESC, l.id"
        };

        let rows = sqlx::query(sql)
            .bind(data_source)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("get locks for datasource: {e}")))?;

        rows.iter().map(row_to_lock).collect()
    }

    /// Return all locks currently held by a task (revoked included).
    pub async fn get_locks_for_task(&self, task_id: &str) -> Result<Vec<TaskLockRow>> {
        let rows = sqlx::query(
            "SELECT l.id, l.task_id, l.lock_payload, d.datasource, d.interval_start, \
             d.interval_end, d.lock_type, d.priority, d.revoked \
             FROM druid_tasklocks l \
             JOIN druid_task_lock_detail d ON d.id = l.id \
             WHERE l.task_id = ?1 ORDER BY l.id",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("get locks for task: {e}")))?;

        rows.iter().map(row_to_lock).collect()
    }

    /// Mark a lock as revoked (preempted) without deleting it.
    pub async fn revoke_lock(&self, id: &str) -> Result<()> {
        let rowid: i64 = id
            .parse()
            .map_err(|e| DruidError::Metadata(format!("invalid lock id '{id}': {e}")))?;
        sqlx::query("UPDATE druid_task_lock_detail SET revoked = TRUE WHERE id = ?1")
            .bind(rowid)
            .execute(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("revoke lock: {e}")))?;
        Ok(())
    }

    /// Permanently delete a lock (both canonical and detail rows).
    pub async fn delete_lock(&self, id: &str) -> Result<()> {
        let rowid: i64 = id
            .parse()
            .map_err(|e| DruidError::Metadata(format!("invalid lock id '{id}': {e}")))?;
        sqlx::query("DELETE FROM druid_task_lock_detail WHERE id = ?1")
            .bind(rowid)
            .execute(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("delete lock detail: {e}")))?;
        sqlx::query("DELETE FROM druid_tasklocks WHERE id = ?1")
            .bind(rowid)
            .execute(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("delete lock: {e}")))?;
        Ok(())
    }

    // ----- Config ----------------------------------------------------------

    /// Get a config value by name.
    pub async fn get_config(&self, name: &str) -> Result<Option<serde_json::Value>> {
        let row = sqlx::query("SELECT payload FROM druid_config WHERE name = ?1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("get config: {e}")))?;

        match row {
            Some(r) => {
                let payload_str: String = r
                    .try_get("payload")
                    .map_err(|e| DruidError::Metadata(format!("decode config payload: {e}")))?;
                let val: serde_json::Value = serde_json::from_str(&payload_str)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Return all config entries as name-value pairs.
    pub async fn get_all_config(&self) -> Result<Vec<(String, serde_json::Value)>> {
        let rows = sqlx::query("SELECT name, payload FROM druid_config ORDER BY name")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("get all config: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let name: String = r
                .try_get("name")
                .map_err(|e| DruidError::Metadata(format!("decode name: {e}")))?;
            let payload_str: String = r
                .try_get("payload")
                .map_err(|e| DruidError::Metadata(format!("decode config payload: {e}")))?;
            let val: serde_json::Value = serde_json::from_str(&payload_str)?;
            out.push((name, val));
        }
        Ok(out)
    }

    /// Set (upsert) a config value.
    pub async fn set_config(&self, name: &str, value: &serde_json::Value) -> Result<()> {
        let payload_str = serde_json::to_string(value)?;

        sqlx::query("INSERT OR REPLACE INTO druid_config (name, payload) VALUES (?1, ?2)")
            .bind(name)
            .bind(&payload_str)
            .execute(&self.pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("set config: {e}")))?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn row_to_segment(r: &sqlx::sqlite::SqliteRow) -> Result<SegmentMetadataRow> {
    let id: String = r
        .try_get("id")
        .map_err(|e| DruidError::Metadata(format!("decode id: {e}")))?;
    let data_source: String = r
        .try_get("dataSource")
        .map_err(|e| DruidError::Metadata(format!("decode dataSource: {e}")))?;
    let created_date: String = r
        .try_get("created_date")
        .map_err(|e| DruidError::Metadata(format!("decode created_date: {e}")))?;
    let start: String = r
        .try_get("start")
        .map_err(|e| DruidError::Metadata(format!("decode start: {e}")))?;
    let end: String = r
        .try_get("end")
        .map_err(|e| DruidError::Metadata(format!("decode end: {e}")))?;
    let version: String = r
        .try_get("version")
        .map_err(|e| DruidError::Metadata(format!("decode version: {e}")))?;
    let used: bool = r
        .try_get("used")
        .map_err(|e| DruidError::Metadata(format!("decode used: {e}")))?;
    let payload_str: String = r
        .try_get("payload")
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

fn row_to_task(r: &sqlx::sqlite::SqliteRow) -> Result<TaskRow> {
    let id: String = r
        .try_get("id")
        .map_err(|e| DruidError::Metadata(format!("decode task id: {e}")))?;
    let task_type: String = r
        .try_get("task_type")
        .map_err(|e| DruidError::Metadata(format!("decode task_type: {e}")))?;
    let data_source: String = r
        .try_get("datasource")
        .map_err(|e| DruidError::Metadata(format!("decode datasource: {e}")))?;
    let status: String = r
        .try_get("status")
        .map_err(|e| DruidError::Metadata(format!("decode status: {e}")))?;
    let created_date: String = r
        .try_get("created_date")
        .map_err(|e| DruidError::Metadata(format!("decode created_date: {e}")))?;
    let attempt: i64 = r
        .try_get("attempt")
        .map_err(|e| DruidError::Metadata(format!("decode attempt: {e}")))?;
    let worker: Option<String> = r
        .try_get("worker")
        .map_err(|e| DruidError::Metadata(format!("decode worker: {e}")))?;
    let payload_str: String = r
        .try_get("payload")
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

fn row_to_lock(r: &sqlx::sqlite::SqliteRow) -> Result<TaskLockRow> {
    let rowid: i64 = r
        .try_get("id")
        .map_err(|e| DruidError::Metadata(format!("decode lock id: {e}")))?;
    let task_id: String = r
        .try_get("task_id")
        .map_err(|e| DruidError::Metadata(format!("decode lock task_id: {e}")))?;
    let data_source: String = r
        .try_get("datasource")
        .map_err(|e| DruidError::Metadata(format!("decode lock datasource: {e}")))?;
    let interval_start: String = r
        .try_get("interval_start")
        .map_err(|e| DruidError::Metadata(format!("decode interval_start: {e}")))?;
    let interval_end: String = r
        .try_get("interval_end")
        .map_err(|e| DruidError::Metadata(format!("decode interval_end: {e}")))?;
    let lock_type: String = r
        .try_get("lock_type")
        .map_err(|e| DruidError::Metadata(format!("decode lock_type: {e}")))?;
    let priority: i64 = r
        .try_get("priority")
        .map_err(|e| DruidError::Metadata(format!("decode priority: {e}")))?;
    let revoked: bool = r
        .try_get("revoked")
        .map_err(|e| DruidError::Metadata(format!("decode revoked: {e}")))?;
    let payload_str: String = r
        .try_get("lock_payload")
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
// Schema DDL
// ---------------------------------------------------------------------------

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS druid_segments (
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
);

CREATE TABLE IF NOT EXISTS druid_rules (
    id TEXT PRIMARY KEY,
    dataSource TEXT NOT NULL,
    version TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS druid_supervisors (
    id TEXT PRIMARY KEY,
    spec_id TEXT NOT NULL,
    created_date TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS druid_config (
    name TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS druid_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    audit_key TEXT NOT NULL,
    type TEXT NOT NULL,
    author TEXT,
    comment TEXT,
    created_date TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS druid_tasklogs (
    id TEXT PRIMARY KEY,
    created_date TEXT NOT NULL,
    datasource TEXT,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS druid_tasklocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    lock_payload TEXT NOT NULL
);
";

/// Additive, back-compatible column/table extensions applied after
/// [`SCHEMA_SQL`].  Each statement is `IF NOT EXISTS` / additive so
/// re-running against an existing database is a no-op and never drops data.
///
/// SQLite has no `ADD COLUMN IF NOT EXISTS`, so the task-status detail
/// columns live on dedicated additive tables keyed by id rather than via
/// `ALTER TABLE`.  This keeps the original `druid_tasklogs` /
/// `druid_tasklocks` shapes untouched for any other reader.
const SCHEMA_EXT_SQL: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS druid_task_status (
        id TEXT PRIMARY KEY,
        task_type TEXT NOT NULL,
        datasource TEXT NOT NULL,
        status TEXT NOT NULL,
        created_date TEXT NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 0,
        worker TEXT,
        payload TEXT NOT NULL
    );",
    "CREATE TABLE IF NOT EXISTS druid_task_lock_detail (
        id INTEGER PRIMARY KEY,
        datasource TEXT NOT NULL,
        interval_start TEXT NOT NULL,
        interval_end TEXT NOT NULL,
        lock_type TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        revoked BOOLEAN NOT NULL DEFAULT FALSE
    );",
    "CREATE INDEX IF NOT EXISTS idx_task_status_active
        ON druid_task_status (status);",
    "CREATE INDEX IF NOT EXISTS idx_task_lock_ds
        ON druid_task_lock_detail (datasource, revoked);",
];

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn initialize_schema() {
        let _store = setup().await;
        // No panic means the schema was created successfully.
    }

    #[tokio::test]
    async fn insert_and_get_segments() {
        let store = setup().await;
        let seg = make_segment("wiki_2024-01_v1_0", "wiki");
        store.insert_segment(&seg).await.expect("insert");

        let segs = store.get_used_segments("wiki").await.expect("get used");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].id, "wiki_2024-01_v1_0");
        assert_eq!(segs[0].data_source, "wiki");
    }

    #[tokio::test]
    async fn mark_segment_unused() {
        let store = setup().await;
        let seg = make_segment("wiki_2024-01_v1_0", "wiki");
        store.insert_segment(&seg).await.expect("insert");

        store
            .mark_segment_unused("wiki_2024-01_v1_0")
            .await
            .expect("mark unused");

        let segs = store.get_used_segments("wiki").await.expect("get used");
        assert!(segs.is_empty(), "segment should no longer be used");
    }

    #[tokio::test]
    async fn multiple_data_sources() {
        let store = setup().await;
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

    #[tokio::test]
    async fn insert_and_get_supervisor() {
        let store = setup().await;
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

    #[tokio::test]
    async fn get_supervisor_uses_insertion_order_not_wall_clock() {
        // Codex R16: get_supervisor must return the LATEST-INSERTED generation
        // (by rowid), not by created_date text. If the system clock steps
        // BACKWARD between an active spec and its later shutdown tombstone, a
        // created_date order would resurrect the stopped supervisor on restart.
        let store = setup().await;
        // Active spec inserted FIRST with a LATER wall clock; the tombstone
        // inserted SECOND with an EARLIER wall clock (clock stepped back).
        sqlx::query(
            "INSERT INTO druid_supervisors (id, spec_id, created_date, payload) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind("s_active")
        .bind("s")
        .bind("2024-01-01T12:00:00Z")
        .bind(r#"{"type":"kafka","suspended":false}"#)
        .execute(&store.pool)
        .await
        .expect("insert active");
        sqlx::query(
            "INSERT INTO druid_supervisors (id, spec_id, created_date, payload) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind("s_tomb")
        .bind("s")
        .bind("2024-01-01T11:59:00Z")
        .bind(r#"{"suspended":true}"#)
        .execute(&store.pool)
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

    #[tokio::test]
    async fn insert_supervisors_atomic_preserves_generation_order() {
        // The batch insert must keep the given order (oldest-first, as the
        // exporter emits it), so get_supervisor (rowid DESC) returns the
        // LAST entry — the shutdown tombstone must win over the earlier
        // active spec even when both land in the same clock reading.
        let store = setup().await;
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

    #[tokio::test]
    async fn get_rule_data_sources_includes_segmentless_datasource() {
        // Rules set BEFORE any segment exists must still be enumerable —
        // the segment-derived get_all_data_sources cannot see them.
        let store = setup().await;
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

    #[tokio::test]
    async fn config_get_set() {
        let store = setup().await;

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

    #[tokio::test]
    async fn config_upsert() {
        let store = setup().await;

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
    }

    #[tokio::test]
    async fn rules_get_set() {
        let store = setup().await;

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

    #[tokio::test]
    async fn get_rules_uses_insertion_order_not_wall_clock() {
        // Same discipline as get_supervisor (Codex R16), applied to rules:
        // `set_rules` stamps `version` from the system clock, so if the clock
        // steps BACKWARD between two rule updates (NTP step, VM snapshot
        // restore), a `version DESC` order would keep serving the OLDER
        // generation forever. rowid reflects true insertion order, so the
        // newest ACKed update must win.
        let store = setup().await;
        // Older generation ACKed FIRST, with a LATER wall-clock version text.
        sqlx::query(
            "INSERT OR REPLACE INTO druid_rules (id, dataSource, version, payload) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind("wiki_2024-01-01T12:00:00Z")
        .bind("wiki")
        .bind("2024-01-01T12:00:00Z")
        .bind(r#"[{"type":"loadForever"}]"#)
        .execute(&store.pool)
        .await
        .expect("insert first generation");
        // Newer generation ACKed SECOND, with an EARLIER wall-clock version
        // text (the clock stepped back between the two updates).
        sqlx::query(
            "INSERT OR REPLACE INTO druid_rules (id, dataSource, version, payload) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind("wiki_2024-01-01T11:59:00Z")
        .bind("wiki")
        .bind("2024-01-01T11:59:00Z")
        .bind(r#"[{"type":"dropForever"}]"#)
        .execute(&store.pool)
        .await
        .expect("insert second generation");

        let rules = store.get_rules("wiki").await.expect("get rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0]["type"], "dropForever",
            "the last-ACKed rules update must win despite its earlier wall-clock version"
        );
    }

    #[tokio::test]
    async fn get_used_segments_empty() {
        let store = setup().await;
        let segs = store
            .get_used_segments("nonexistent")
            .await
            .expect("get used");
        assert!(segs.is_empty());
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

    #[tokio::test]
    async fn task_insert_get_roundtrip() {
        let store = setup().await;
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

    #[tokio::test]
    async fn task_update_status_persists() {
        let store = setup().await;
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

    #[tokio::test]
    async fn task_active_filter() {
        let store = setup().await;
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

    #[tokio::test]
    async fn lock_insert_get_roundtrip() {
        let store = setup().await;
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

    #[tokio::test]
    async fn lock_revoke_filters_out() {
        let store = setup().await;
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
        assert!(all[0].revoked);
    }

    #[tokio::test]
    async fn lock_delete_removes() {
        let store = setup().await;
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

    #[tokio::test]
    async fn segment_replace_on_duplicate() {
        let store = setup().await;
        let mut seg = make_segment("seg1", "wiki");
        store.insert_segment(&seg).await.expect("insert");

        seg.version = "2024-02-01T00:00:00.000Z".to_string();
        store.insert_segment(&seg).await.expect("replace");

        let segs = store.get_used_segments("wiki").await.expect("get used");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].version, "2024-02-01T00:00:00.000Z");
    }

    #[tokio::test]
    async fn get_segment_and_segment_exists() {
        let store = setup().await;
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
    #[tokio::test]
    async fn replace_txn_commits_both_writes() {
        let store = setup().await;
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
    #[tokio::test]
    async fn replace_txn_failure_between_writes_leaves_no_partial_state() {
        let store = setup().await;
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
    /// `INSERT OR REPLACE` would silently resurrect/overwrite it.
    #[tokio::test]
    async fn replace_txn_collision_with_unused_row_fails_closed() {
        let store = setup().await;
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
    #[tokio::test]
    async fn rollback_replace_txn_deletes_new_row_and_restores_victims() {
        let store = setup().await;
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

    /// Codex 2026-07-12 round-2 HIGH #2/#3: the same datasource name must
    /// map to the SAME mutex instance (that sharing is what makes publish
    /// and admin-disable mutually exclusive), and distinct datasources
    /// must not contend.
    #[tokio::test]
    async fn datasource_publish_lock_is_shared_per_datasource() {
        let store = setup().await;
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
}
