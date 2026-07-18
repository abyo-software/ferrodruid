// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid metadata schema compatibility for FerroDruid.
//!
//! This crate provides strongly-typed Rust representations of the JSON
//! structures stored in Druid's metadata database (segments, rules,
//! supervisors, coordinator dynamic config).  It also exposes a
//! [`validate_druid_metadata`] function that checks whether a given SQLite
//! metadata database contains the expected Druid tables and is readable by
//! FerroDruid, and [`read_druid_source`] — the compat-7 READ-ONLY source
//! reader behind `ferrodruid-migrate import-druid-metadata`, which pulls
//! the `used = true` segment rows (plus rule / supervisor row counts) out
//! of an EXISTING Druid metadata database on PostgreSQL, MySQL, or SQLite.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;

use ferrodruid_common::{DruidError, Result};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

// ---------------------------------------------------------------------------
// Segment payload
// ---------------------------------------------------------------------------

/// Druid segment payload (stored as JSON in `druid_segments.payload`).
///
/// Every segment written by a Druid indexer carries this structure.  The
/// exact shape is dictated by the Druid metadata protocol; field names use
/// `camelCase` on the wire and are mapped to idiomatic Rust names via serde
/// rename attributes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentPayload {
    /// Data source this segment belongs to.
    pub data_source: String,
    /// ISO-8601 interval in `start/end` form.
    pub interval: String,
    /// Version string (typically an ISO-8601 timestamp).
    pub version: String,
    /// Where the segment data lives on deep storage.
    pub load_spec: LoadSpec,
    /// Comma-separated list of dimension column names.
    pub dimensions: String,
    /// Comma-separated list of metric column names.
    pub metrics: String,
    /// Optional sharding specification (called `shardSpec` on the wire).
    #[serde(rename = "shardSpec")]
    pub sharding_spec: Option<ShardingSpec>,
    /// Binary format version of the segment file.
    pub binary_version: Option<i32>,
    /// Size of the segment in bytes.
    pub size: i64,
    /// Canonical segment identifier string.
    pub identifier: String,
}

// ---------------------------------------------------------------------------
// Load spec
// ---------------------------------------------------------------------------

/// Where a segment's data is stored on deep storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum LoadSpec {
    /// Segment stored on the local filesystem.
    #[serde(rename = "local")]
    Local {
        /// Filesystem path to the segment data.
        path: String,
    },
    /// Segment stored in an S3 zip archive.
    #[serde(rename = "s3_zip")]
    S3Zip {
        /// S3 bucket name.
        bucket: String,
        /// S3 object key.
        key: String,
    },
    /// Segment stored on HDFS.
    #[serde(rename = "hdfs")]
    Hdfs {
        /// HDFS path to the segment data.
        path: String,
    },
    /// Segment stored on Google Cloud Storage.
    #[serde(rename = "google")]
    Google {
        /// GCS bucket name.
        bucket: String,
        /// GCS object path.
        path: String,
    },
    /// Segment stored on Azure Blob Storage.
    #[serde(rename = "azure")]
    Azure {
        /// Azure container name.
        container: String,
        /// Azure blob path.
        blob: String,
    },
}

// ---------------------------------------------------------------------------
// Sharding spec
// ---------------------------------------------------------------------------

/// Describes how a segment is partitioned (the `shardSpec` field in the
/// segment payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ShardingSpec {
    /// Numbered sharding: a simple N-of-M scheme.
    #[serde(rename = "numbered")]
    Numbered {
        /// Zero-based partition index.
        #[serde(rename = "partitionNum")]
        partition_num: i32,
        /// Total partition count.
        partitions: i32,
    },
    /// Hash-based sharding on selected dimensions.
    #[serde(rename = "hashed")]
    Hashed {
        /// Zero-based partition index.
        #[serde(rename = "partitionNum")]
        partition_num: i32,
        /// Total partition count.
        partitions: i32,
        /// Subset of dimensions used for hashing (all if absent).
        #[serde(rename = "partitionDimensions")]
        partition_dimensions: Option<Vec<String>>,
    },
    /// Single-partition sharding (the entire interval in one segment).
    #[serde(rename = "single")]
    Single {
        /// Partition number (always 0).
        #[serde(rename = "partitionNum")]
        partition_num: i32,
    },
    /// Linear sharding (incrementally numbered, unknown total).
    #[serde(rename = "linear")]
    Linear {
        /// Zero-based partition index.
        #[serde(rename = "partitionNum")]
        partition_num: i32,
    },
    /// Segment-lock overwrite sharding (`NumberedOverwriteShardSpec`).
    /// The partition number is `partitionId` (NOT `partitionNum`); the
    /// root-partition / minor-version bookkeeping fields are ignored.
    #[serde(rename = "numbered_overwrite")]
    NumberedOverwrite {
        /// The partition id (this variant's partition number).
        #[serde(rename = "partitionId")]
        partition_id: i32,
    },
    /// Range partitioning (`DimensionRangeShardSpec`); the partition
    /// number is `partitionNum` and the range dimensions/bounds are
    /// ignored.
    #[serde(rename = "range")]
    Range {
        /// Zero-based partition index.
        #[serde(rename = "partitionNum")]
        partition_num: i32,
    },
    /// No explicit sharding.
    #[serde(rename = "none")]
    None {},
}

// ---------------------------------------------------------------------------
// Supervisor spec
// ---------------------------------------------------------------------------

/// Druid supervisor spec format (as stored in `druid_supervisors.payload`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorSpec {
    /// Supervisor type, e.g. `"kafka"`, `"kinesis"`.
    #[serde(rename = "type")]
    pub spec_type: String,
    /// The full supervisor specification body (schema varies by type).
    pub spec: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Load rules
// ---------------------------------------------------------------------------

/// Druid load / drop / broadcast rule format (as stored in
/// `druid_rules.payload`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DruidRule {
    /// Retain segments forever with the given replication.
    #[serde(rename = "loadForever")]
    LoadForever {
        /// Tier name → replica count.
        #[serde(rename = "tieredReplicants")]
        tier_replicants: HashMap<String, usize>,
    },
    /// Retain segments whose interval falls within the given interval.
    #[serde(rename = "loadByInterval")]
    LoadByInterval {
        /// ISO-8601 interval to match.
        interval: String,
        /// Tier name → replica count.
        #[serde(rename = "tieredReplicants")]
        tier_replicants: HashMap<String, usize>,
    },
    /// Retain segments whose interval falls within a sliding period window.
    #[serde(rename = "loadByPeriod")]
    LoadByPeriod {
        /// ISO-8601 period, e.g. `"P1M"`.
        period: String,
        /// Whether to include segments in the future.
        #[serde(rename = "includeFuture", default)]
        include_future: bool,
        /// Tier name → replica count.
        #[serde(rename = "tieredReplicants")]
        tier_replicants: HashMap<String, usize>,
    },
    /// Drop all segments unconditionally.
    #[serde(rename = "dropForever")]
    DropForever {},
    /// Drop segments whose interval matches.
    #[serde(rename = "dropByInterval")]
    DropByInterval {
        /// ISO-8601 interval to match.
        interval: String,
    },
    /// Drop segments older than the given period.
    #[serde(rename = "dropByPeriod")]
    DropByPeriod {
        /// ISO-8601 period, e.g. `"P6M"`.
        period: String,
        /// Whether to include segments in the future.
        #[serde(rename = "includeFuture", default)]
        include_future: bool,
    },
    /// Broadcast segments to all servers (for lookup data sources).
    #[serde(rename = "broadcastForever")]
    BroadcastForever {},
}

// ---------------------------------------------------------------------------
// Coordinator dynamic config
// ---------------------------------------------------------------------------

/// Druid coordinator dynamic configuration (stored in `druid_config` under
/// the key `"coordinator.config"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinatorDynamicConfig {
    /// Maximum number of segments to move per coordinator run.
    #[serde(default)]
    pub max_segments_to_move: i32,
    /// Throttle limit for replication (max segments replicating concurrently).
    #[serde(default)]
    pub replication_throttle_limit: i32,
    /// Number of threads for balancer cost computation.
    #[serde(default)]
    pub balancer_compute_threads: i32,
    /// Data sources whose unused segments may be killed.
    #[serde(default)]
    pub kill_data_source_whitelist: Vec<String>,
    /// Data sources whose pending segments should not be killed.
    #[serde(default)]
    pub kill_pending_segments_skip_list: Vec<String>,
    /// Maximum segments allowed in a historical's loading queue.
    #[serde(default)]
    pub max_segments_in_node_loading_queue: i32,
    /// Byte limit for segment merge tasks.
    #[serde(default)]
    pub merge_bytes_limit: i64,
    /// Segment count limit for segment merge tasks.
    #[serde(default)]
    pub merge_segments_limit: i32,
    /// Whether smart (cost-based) segment loading is enabled.
    #[serde(default)]
    pub smart_segment_loading: bool,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// The set of tables that a Druid-compatible metadata database must contain.
const EXPECTED_TABLES: &[&str] = &[
    "druid_segments",
    "druid_rules",
    "druid_supervisors",
    "druid_config",
    "druid_audit",
    "druid_tasklogs",
    "druid_tasklocks",
];

/// Result of validating a Druid metadata database.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// Tables found in the database.
    pub tables_found: Vec<String>,
    /// Expected tables that are missing.
    pub tables_missing: Vec<String>,
    /// Number of segment rows in `druid_segments`.
    pub segment_count: usize,
    /// Number of distinct data sources across segments.
    pub datasource_count: usize,
    /// `true` when all expected tables are present.
    pub is_compatible: bool,
}

/// Validate that a SQLite database contains the Druid metadata schema.
///
/// This queries `sqlite_master` for the expected table names and counts
/// segments / data sources to populate the report.
pub async fn validate_druid_metadata(pool: &SqlitePool) -> Result<ValidationReport> {
    // Discover which tables exist.
    let rows = sqlx::query("SELECT name FROM sqlite_master WHERE type = 'table'")
        .fetch_all(pool)
        .await
        .map_err(|e| DruidError::Metadata(format!("list tables: {e}")))?;

    let existing: Vec<String> = rows
        .iter()
        .map(|r| r.try_get::<String, _>("name").unwrap_or_default())
        .collect();

    let mut tables_found = Vec::new();
    let mut tables_missing = Vec::new();
    for &tbl in EXPECTED_TABLES {
        if existing.iter().any(|n| n == tbl) {
            tables_found.push(tbl.to_string());
        } else {
            tables_missing.push(tbl.to_string());
        }
    }

    let is_compatible = tables_missing.is_empty();

    // Count segments and data sources (only if the table exists).
    let (segment_count, datasource_count) = if tables_found.contains(&"druid_segments".to_string())
    {
        let seg_row = sqlx::query("SELECT COUNT(*) AS cnt FROM druid_segments")
            .fetch_one(pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("count segments: {e}")))?;
        let seg_count: i64 = seg_row
            .try_get("cnt")
            .map_err(|e| DruidError::Metadata(format!("decode count: {e}")))?;

        let ds_row = sqlx::query("SELECT COUNT(DISTINCT dataSource) AS cnt FROM druid_segments")
            .fetch_one(pool)
            .await
            .map_err(|e| DruidError::Metadata(format!("count data sources: {e}")))?;
        let ds_count: i64 = ds_row
            .try_get("cnt")
            .map_err(|e| DruidError::Metadata(format!("decode count: {e}")))?;

        (seg_count as usize, ds_count as usize)
    } else {
        (0, 0)
    };

    Ok(ValidationReport {
        tables_found,
        tables_missing,
        segment_count,
        datasource_count,
        is_compatible,
    })
}

// ---------------------------------------------------------------------------
// Foreign Druid metadata-DB source reader (compat-7)
// ---------------------------------------------------------------------------

/// One `used = true` row of a foreign Druid metadata database's
/// `druid_segments` table, as returned by [`read_druid_source`].
#[derive(Debug)]
pub struct DruidSourceSegmentRow {
    /// The row's `id` column — Druid's segment identifier string.  For
    /// reporting only: the importer synthesizes identity from the
    /// PAYLOAD fields (the `_`-joined identifier string is not
    /// injective), never from this string.
    pub id: String,
    /// The row's `dataSource` column.
    pub data_source: String,
    /// The row's `start` column (ISO-8601 interval start).
    pub start: String,
    /// The row's `end` column (ISO-8601 interval end).
    pub end: String,
    /// The row's `version` column.
    pub version: String,
    /// The decoded `payload` JSON, or a human-readable per-row decode
    /// failure (undecodable columns, non-UTF-8 payload bytes, malformed
    /// JSON, or an unknown `loadSpec`/`shardSpec` type).  A broken row
    /// must become a loud per-segment failure downstream — it never
    /// aborts the whole read.
    pub payload: std::result::Result<SegmentPayload, String>,
}

/// Everything [`read_druid_source`] found in a foreign Druid metadata DB.
#[derive(Debug)]
pub struct DruidSourceReport {
    /// The `used = true` segment rows (after the optional dataSource
    /// filter), in deterministic `ORDER BY id` order.
    pub segments: Vec<DruidSourceSegmentRow>,
    /// The `max_segments` cap was hit: at least one further used row
    /// exists in the source and was NOT returned.
    pub truncated: bool,
    /// Row count of `druid_rules`, or `None` when the table is missing
    /// or unreadable.  Counted only — rules are never imported.
    pub rules_found: Option<u64>,
    /// Row count of `druid_supervisors`, or `None` when the table is
    /// missing or unreadable.  Counted only — supervisors are never
    /// imported.
    pub supervisors_found: Option<u64>,
}

/// The source backends the reader dispatches on.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SourceBackend {
    /// A PostgreSQL source; connect with the original, unmodified URI.
    Postgres,
    /// A MySQL source; connect with the original, unmodified URI.
    MySql,
    /// A SQLite database file at this path, opened READ-ONLY.
    SqlitePath(String),
}

/// Is a query-parameter key the `password` key, comparing on the
/// PERCENT-DECODED, case-folded form?  A driver percent-decodes query
/// keys before matching, so an encoded spelling like `pass%77ord`
/// (== `password`) must be recognized here too — matching on the raw
/// key would let `?pass%77ord=SECRET` slip past the redaction (this was
/// a Codex HIGH in compat-6).
///
/// This intentionally DUPLICATES the shared
/// `ferrodruid_metadata::redact_metadata_uri` / `is_password_key`
/// logic: this low-level schema crate must not depend on the
/// higher-level metadata-store crate (that would be a layering
/// inversion), and the shared helper is only a dev-dependency here.
/// The percent-decoder is a tiny std-only one — a `%XX` triple becomes
/// its byte, a lone or malformed `%` is passed through.
fn is_password_query_key(key: &str) -> bool {
    let b = key.as_bytes();
    let hex = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    };
    let mut decoded: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2]))
        {
            decoded.push((h << 4) | l);
            i += 3;
        } else {
            decoded.push(b[i]);
            i += 1;
        }
    }
    decoded.eq_ignore_ascii_case(b"password")
}

/// Render a source-DB URI safe for error text and logs by masking the
/// authority userinfo (`user:password@` → `***@`) and the value of any
/// `password` query parameter (matched on the percent-decoded key, see
/// [`is_password_query_key`]).  A local, std-only syntactic redactor
/// (the shared `ferrodruid_metadata::redact_metadata_uri` is only a
/// dev-dependency here) so a malformed `scheme://user:secret@…` string
/// can be named in an error WITHOUT leaking the credential to stderr or
/// CI logs.  Never fails; a bare path (no `://`) has no credential
/// surface and passes through unchanged.
fn redact_source_uri(uri: &str) -> String {
    let Some(scheme_end) = uri.find("://") else {
        return uri.to_string();
    };
    let after_scheme = &uri[scheme_end + 3..];
    let authority_end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let authority = &after_scheme[..authority_end];
    let mut out = String::with_capacity(uri.len());
    out.push_str(&uri[..scheme_end + 3]);
    // `rfind`: a literal '@' inside the password must not truncate the
    // host — mask the whole userinfo (username included, it is sensitive
    // too).
    match authority.rfind('@') {
        Some(at) => {
            out.push_str("***@");
            out.push_str(&authority[at + 1..]);
        }
        None => out.push_str(authority),
    }
    let rest = &after_scheme[authority_end..];
    match rest.split_once('?') {
        Some((path, query)) => {
            out.push_str(path);
            out.push('?');
            let mut first = true;
            for pair in query.split('&') {
                if !first {
                    out.push('&');
                }
                first = false;
                match pair.split_once('=') {
                    Some((key, _)) if is_password_query_key(key) => {
                        out.push_str(key);
                        out.push_str("=***");
                    }
                    _ => out.push_str(pair),
                }
            }
        }
        None => out.push_str(rest),
    }
    out
}

/// Parse a source-DB URI into its backend.
///
/// Mirrors the target-store URI grammar (`postgres://…`,
/// `postgresql://…`, `mysql://…`, `sqlite://<path>`, or a bare SQLite
/// file path) but REFUSES in-memory SQLite (an empty throwaway DB cannot
/// be an EXISTING Druid metadata source) and unknown/invalid schemes
/// loudly — never "a SQLite file named after the URI".  A string that
/// contains `://` but whose prefix is not a valid scheme is a MALFORMED
/// URI (rejected, credentials redacted), NOT a file path; only a string
/// with no `://` at all is treated as a bare SQLite path.
fn parse_source_uri(uri: &str) -> Result<SourceBackend> {
    let uri = uri.trim();
    if uri.is_empty() {
        return Err(DruidError::Metadata("source metadata URI is empty".into()));
    }
    let memory_err = || {
        DruidError::Metadata(
            "an in-memory SQLite database cannot be an EXISTING Druid metadata source".into(),
        )
    };
    if uri == ":memory:" || uri.eq_ignore_ascii_case("sqlite::memory:") {
        return Err(memory_err());
    }
    let Some(idx) = uri.find("://") else {
        // No scheme — a bare SQLite file path.
        return Ok(SourceBackend::SqlitePath(uri.to_string()));
    };
    let scheme = &uri[..idx];
    let rest = &uri[idx + 3..];
    let is_scheme = !scheme.is_empty()
        && scheme
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-'));
    if !is_scheme {
        // "://" appeared but the prefix is not a valid scheme — a
        // MALFORMED URI, never a file path (a path-with-`://` would
        // otherwise be opened as SQLite and its error would echo any
        // embedded credential).  Reject loudly with the URI redacted.
        return Err(DruidError::Metadata(format!(
            "unrecognized source metadata URI {} — the text before '://' is not a valid \
             scheme (use postgres://…, mysql://…, sqlite://<path>, or a bare SQLite file \
             path with no '://')",
            redact_source_uri(uri)
        )));
    }
    match scheme.to_ascii_lowercase().as_str() {
        "postgres" | "postgresql" => Ok(SourceBackend::Postgres),
        "mysql" => Ok(SourceBackend::MySql),
        "sqlite" => {
            if rest.is_empty() || rest == ":memory:" {
                Err(memory_err())
            } else {
                Ok(SourceBackend::SqlitePath(rest.to_string()))
            }
        }
        other => Err(DruidError::Metadata(format!(
            "unsupported source metadata URI scheme '{other}://' (supported: postgres://…, \
             postgresql://…, mysql://…, sqlite://<path>, or a bare SQLite file path; Derby \
             has no Rust driver — externalize the Druid metadata to PostgreSQL/MySQL first, \
             standard Druid operations, or use `ferrodruid-migrate attach` on the \
             deep-storage directory)"
        ))),
    }
}

/// Byte cap on a single `druid_segments.payload` transferred from an
/// untrusted source DB.  A Druid segment *descriptor* payload is small
/// JSON (dataSource / interval / loadSpec / shardSpec — typically well
/// under a kilobyte); this bounds the DESCRIPTOR, not the segment DATA.
/// A row whose payload exceeds this is length-gated AT THE SQL LEVEL
/// (the oversized bytes are NEVER transferred — the SELECT returns
/// `NULL` for the payload and the true length in `payload_len`) and
/// becomes that row's per-row `Err`, so one hostile or corrupt row can
/// neither OOM nor abort the whole import.  16 MiB is a generous
/// ceiling far above any legitimate descriptor.
const MAX_SEGMENT_PAYLOAD_BYTES: i64 = 16 * 1024 * 1024;

/// Rows fetched per page by [`read_druid_source`].  The reader pages
/// through the result rather than `fetch_all`-buffering the whole table,
/// so at most this many raw rows (each with a payload already capped to
/// [`MAX_SEGMENT_PAYLOAD_BYTES`]) are resident at once; only the small
/// decoded descriptors accumulate across pages.
const SOURCE_PAGE_ROWS: i64 = 1024;

/// The `payload_len` / capped-`payload` SELECT-list expressions for a
/// backend: `(len_expr, payload_len_column, payload_column)`.  The
/// length is a BYTE length (`CAST(... AS BLOB)` on SQLite so a TEXT
/// column is measured in bytes too); the payload is returned only when
/// within the cap, else `NULL` — the oversized bytes never leave the DB.
fn payload_columns(backend: &SourceBackend) -> String {
    // `len_cmp` is compared against the `i64` cap literal inside the
    // CASE (an `int4 <= int4` comparison on Postgres is fine); `len_proj`
    // is the value read OUT into `payload_len` and MUST decode as `i64`.
    // Postgres `octet_length()` returns `int4`, which sqlx-postgres
    // REFUSES to decode as `i64` — so the projection is cast to
    // `bigint`.  MySQL `OCTET_LENGTH` is already BIGINT and SQLite
    // `length()` is already i64.
    let (len_cmp, len_proj) = match backend {
        SourceBackend::Postgres => ("octet_length(payload)", "octet_length(payload)::bigint"),
        SourceBackend::MySql => ("OCTET_LENGTH(payload)", "OCTET_LENGTH(payload)"),
        SourceBackend::SqlitePath(_) => (
            "length(CAST(payload AS BLOB))",
            "length(CAST(payload AS BLOB))",
        ),
    };
    format!(
        "{len_proj} AS payload_len, \
         CASE WHEN {len_cmp} <= {MAX_SEGMENT_PAYLOAD_BYTES} THEN payload ELSE NULL END AS payload"
    )
}

/// Build the per-backend `druid_segments` SELECT for one page.
///
/// `start` / `end` are reserved words and are quoted per backend
/// (`"start"` on PostgreSQL and SQLite, backticks on MySQL); `used` is a
/// BOOLEAN on PostgreSQL, a TINYINT on MySQL, and an INTEGER in SQLite —
/// `used = TRUE` covers the first two, `used = 1` the third.  The
/// payload is length-gated in the SELECT list (see [`payload_columns`]).
/// `page` / `offset` are internally-generated `i64` page bounds
/// (never user or DB input) embedded as integer literals, so the
/// per-page `LIMIT`/`OFFSET` needs no bind and no backend placeholder
/// dance; only the optional `dataSource` filter is a bound param.  Only
/// SELECTs are ever built here: the source DB is read-only input.
fn build_segments_sql(
    backend: &SourceBackend,
    with_datasource: bool,
    page: i64,
    offset: i64,
) -> String {
    let (q_open, q_close, used_literal) = match backend {
        SourceBackend::Postgres => ("\"", "\"", "TRUE"),
        SourceBackend::MySql => ("`", "`", "TRUE"),
        SourceBackend::SqlitePath(_) => ("\"", "\"", "1"),
    };
    let payload_cols = payload_columns(backend);
    let mut sql = format!(
        "SELECT id, dataSource AS ds, {q_open}start{q_close} AS seg_start, \
         {q_open}end{q_close} AS seg_end, version, {payload_cols} \
         FROM druid_segments WHERE used = {used_literal}"
    );
    if with_datasource {
        // The optional dataSource filter is the ONLY bound param — `$1`
        // on PostgreSQL, `?` elsewhere.
        if matches!(backend, SourceBackend::Postgres) {
            sql.push_str(" AND dataSource = $1");
        } else {
            sql.push_str(" AND dataSource = ?");
        }
    }
    sql.push_str(&format!(" ORDER BY id LIMIT {page} OFFSET {offset}"));
    sql
}

/// Decode a `druid_segments.payload` JSON text into [`SegmentPayload`].
///
/// Public seam so the compat-7 importer (and its tests) exercise exactly
/// the decode the reader applies.  An unknown `loadSpec` / `shardSpec`
/// `type` or any shape mismatch is an `Err` string — a per-row failure,
/// never a panic or a run abort.
pub fn decode_segment_payload(text: &str) -> std::result::Result<SegmentPayload, String> {
    serde_json::from_str::<SegmentPayload>(text)
        .map_err(|e| format!("payload JSON does not decode as a Druid segment payload: {e}"))
}

/// Byte-form payload decode (PostgreSQL BYTEA / MySQL BLOB): UTF-8
/// first, then the JSON decode.
fn decode_segment_payload_bytes(bytes: &[u8]) -> std::result::Result<SegmentPayload, String> {
    match std::str::from_utf8(bytes) {
        Ok(text) => decode_segment_payload(text),
        Err(_) => Err("payload bytes are not valid UTF-8".to_string()),
    }
}

/// Convert one fetched row into a [`DruidSourceSegmentRow`], leniently:
/// the `payload` column is tried as bytes (PostgreSQL BYTEA / MySQL
/// BLOB) then as text (SQLite / non-canonical TEXT columns), and every
/// per-row problem becomes the row's `Err` payload instead of aborting
/// the read.
fn source_segment_from_row<R>(row: &R) -> DruidSourceSegmentRow
where
    R: Row,
    for<'c> &'c str: sqlx::ColumnIndex<R>,
    String: for<'r> sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    Vec<u8>: for<'r> sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    i64: for<'r> sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    let mut column_errors: Vec<String> = Vec::new();
    let mut text = |name: &str| match row.try_get::<String, _>(name) {
        Ok(v) => v,
        Err(e) => {
            column_errors.push(format!("column `{name}` could not be decoded as text: {e}"));
            String::new()
        }
    };
    let id = text("id");
    let data_source = text("ds");
    let start = text("seg_start");
    let end = text("seg_end");
    let version = text("version");
    // The SQL length-gate returns the true byte length in `payload_len`
    // and NULLs the `payload` column when it exceeds the cap, so an
    // oversized payload is refused WITHOUT ever transferring its bytes.
    // Read the (possibly-NULLed) payload column only once the gate has
    // cleared the row.
    let decode_payload = || match row.try_get::<Vec<u8>, _>("payload") {
        Ok(bytes) => decode_segment_payload_bytes(&bytes),
        Err(_) => match row.try_get::<String, _>("payload") {
            Ok(t) => decode_segment_payload(&t),
            Err(e) => Err(format!(
                "payload column could not be decoded as bytes or text: {e}"
            )),
        },
    };
    let payload = if !column_errors.is_empty() {
        Err(format!(
            "source row could not be decoded: {}",
            column_errors.join("; ")
        ))
    } else {
        // FAIL CLOSED: a `payload_len` we cannot read as `i64` must skip
        // the row, never fall through to read a possibly-oversized
        // payload (a `try_get` width mismatch must not silently defeat
        // the size gate — the Postgres int4-vs-i64 landmine).
        match row.try_get::<Option<i64>, _>("payload_len") {
            // Genuinely NULL source payload → the existing decode path,
            // which surfaces the null as a per-row decode error.
            Ok(None) => decode_payload(),
            Ok(Some(n)) if n > MAX_SEGMENT_PAYLOAD_BYTES => Err(format!(
                "segment {id}: payload {n} bytes exceeds {MAX_SEGMENT_PAYLOAD_BYTES} byte cap — \
                 skipped (an implausibly large segment descriptor; its bytes were NOT transferred)"
            )),
            Ok(Some(_)) => decode_payload(),
            Err(e) => Err(format!(
                "segment {id}: could not read payload_len as i64 ({e}) — refusing to read a \
                 possibly-oversized payload"
            )),
        }
    };
    DruidSourceSegmentRow {
        id,
        data_source,
        start,
        end,
        version,
        payload,
    }
}

/// `COUNT(*)` of a FIXED internal table name (never user input), or
/// `None` when the table is missing or unreadable.
async fn count_rows_pg(pool: &sqlx::PgPool, table: &str) -> Option<u64> {
    let sql = format!("SELECT COUNT(*) AS cnt FROM {table}");
    let row = sqlx::query(&sql).fetch_one(pool).await.ok()?;
    let n: i64 = row.try_get("cnt").ok()?;
    u64::try_from(n).ok()
}

/// MySQL variant of [`count_rows_pg`].
async fn count_rows_mysql(pool: &sqlx::MySqlPool, table: &str) -> Option<u64> {
    let sql = format!("SELECT COUNT(*) AS cnt FROM {table}");
    let row = sqlx::query(&sql).fetch_one(pool).await.ok()?;
    let n: i64 = row.try_get("cnt").ok()?;
    u64::try_from(n).ok()
}

/// SQLite variant of [`count_rows_pg`].
async fn count_rows_sqlite(pool: &SqlitePool, table: &str) -> Option<u64> {
    let sql = format!("SELECT COUNT(*) AS cnt FROM {table}");
    let row = sqlx::query(&sql).fetch_one(pool).await.ok()?;
    let n: i64 = row.try_get("cnt").ok()?;
    u64::try_from(n).ok()
}

/// Read the `used = true` segment rows (plus `druid_rules` /
/// `druid_supervisors` row COUNTS) from an EXISTING Apache Druid
/// metadata database — the compat-7 source reader behind
/// `ferrodruid-migrate import-druid-metadata`.
///
/// * `source_uri` — `postgres://…`, `postgresql://…`, `mysql://…`,
///   `sqlite://<path>`, or a bare SQLite file path.  Derby has no Rust
///   driver: externalize the Druid metadata to PostgreSQL/MySQL first,
///   or attach the deep-storage directory with `ferrodruid-migrate
///   attach`.
/// * The source is READ-ONLY input: only `SELECT`s are ever issued
///   (SQLite is additionally opened with the driver-level `read_only`
///   flag), and no FerroDruid schema bootstrap runs against it.
/// * `datasource` filters on the `dataSource` column; `max_segments`
///   bounds the returned row count (`truncated` in the report says
///   whether more used rows exist beyond the cap).
/// * A row whose columns or payload cannot be decoded is returned with a
///   per-row `Err` payload — a loud per-segment failure for the caller,
///   never a whole-run abort.
pub async fn read_druid_source(
    source_uri: &str,
    datasource: Option<&str>,
    max_segments: Option<usize>,
) -> Result<DruidSourceReport> {
    let backend = parse_source_uri(source_uri)?;
    // Rows per page: a small `max_segments` caps the page so we fetch at
    // most `m + 1` rows total (the `+1` detects truncation); otherwise
    // the fixed page size bounds raw-row residency while all matching
    // rows are paged through.
    let page: i64 = match max_segments {
        Some(m) => i64::try_from(m)
            .unwrap_or(i64::MAX)
            .saturating_add(1)
            .min(SOURCE_PAGE_ROWS),
        None => SOURCE_PAGE_ROWS,
    };
    // True once we have gathered enough rows to answer the query (and, if
    // `max_segments` is set, to know whether more exist beyond the cap).
    let enough =
        |segments: &[DruidSourceSegmentRow]| max_segments.is_some_and(|m| segments.len() > m);

    let (mut segments, rules_found, supervisors_found) = match &backend {
        SourceBackend::Postgres => {
            let pool = sqlx::PgPool::connect(source_uri.trim())
                .await
                .map_err(|e| {
                    DruidError::Metadata(format!(
                        "cannot connect to the source Druid metadata DB (postgres): {e}"
                    ))
                })?;
            let mut segments: Vec<DruidSourceSegmentRow> = Vec::new();
            let mut offset: i64 = 0;
            loop {
                let sql = build_segments_sql(&backend, datasource.is_some(), page, offset);
                let mut q = sqlx::query(&sql);
                if let Some(ds) = datasource {
                    q = q.bind(ds);
                }
                let rows = q.fetch_all(&pool).await.map_err(|e| {
                    DruidError::Metadata(format!(
                        "cannot read druid_segments from the source Druid metadata DB \
                         (postgres): {e}"
                    ))
                })?;
                let n = rows.len();
                for row in &rows {
                    segments.push(source_segment_from_row(row));
                }
                offset = offset.saturating_add(n as i64);
                if (n as i64) < page || enough(&segments) {
                    break;
                }
            }
            let rules = count_rows_pg(&pool, "druid_rules").await;
            let sups = count_rows_pg(&pool, "druid_supervisors").await;
            pool.close().await;
            (segments, rules, sups)
        }
        SourceBackend::MySql => {
            let pool = sqlx::MySqlPool::connect(source_uri.trim())
                .await
                .map_err(|e| {
                    DruidError::Metadata(format!(
                        "cannot connect to the source Druid metadata DB (mysql): {e}"
                    ))
                })?;
            let mut segments: Vec<DruidSourceSegmentRow> = Vec::new();
            let mut offset: i64 = 0;
            loop {
                let sql = build_segments_sql(&backend, datasource.is_some(), page, offset);
                let mut q = sqlx::query(&sql);
                if let Some(ds) = datasource {
                    q = q.bind(ds);
                }
                let rows = q.fetch_all(&pool).await.map_err(|e| {
                    DruidError::Metadata(format!(
                        "cannot read druid_segments from the source Druid metadata DB \
                         (mysql): {e}"
                    ))
                })?;
                let n = rows.len();
                for row in &rows {
                    segments.push(source_segment_from_row(row));
                }
                offset = offset.saturating_add(n as i64);
                if (n as i64) < page || enough(&segments) {
                    break;
                }
            }
            let rules = count_rows_mysql(&pool, "druid_rules").await;
            let sups = count_rows_mysql(&pool, "druid_supervisors").await;
            pool.close().await;
            (segments, rules, sups)
        }
        SourceBackend::SqlitePath(path) => {
            let opts = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .read_only(true);
            let pool = SqlitePool::connect_with(opts).await.map_err(|e| {
                DruidError::Metadata(format!(
                    "cannot open the source Druid metadata DB (sqlite, read-only) at {}: {e}",
                    redact_source_uri(path)
                ))
            })?;
            let mut segments: Vec<DruidSourceSegmentRow> = Vec::new();
            let mut offset: i64 = 0;
            loop {
                let sql = build_segments_sql(&backend, datasource.is_some(), page, offset);
                let mut q = sqlx::query(&sql);
                if let Some(ds) = datasource {
                    q = q.bind(ds);
                }
                let rows = q.fetch_all(&pool).await.map_err(|e| {
                    DruidError::Metadata(format!(
                        "cannot read druid_segments from the source Druid metadata DB \
                         (sqlite): {e}"
                    ))
                })?;
                let n = rows.len();
                for row in &rows {
                    segments.push(source_segment_from_row(row));
                }
                offset = offset.saturating_add(n as i64);
                if (n as i64) < page || enough(&segments) {
                    break;
                }
            }
            let rules = count_rows_sqlite(&pool, "druid_rules").await;
            let sups = count_rows_sqlite(&pool, "druid_supervisors").await;
            pool.close().await;
            (segments, rules, sups)
        }
    };

    let truncated = max_segments.is_some_and(|m| segments.len() > m);
    if let Some(m) = max_segments {
        segments.truncate(m);
    }
    Ok(DruidSourceReport {
        segments,
        truncated,
        rules_found,
        supervisors_found,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -- SegmentPayload -------------------------------------------------------

    #[test]
    fn parse_real_druid_segment_payload_local() {
        let json = r#"{
            "dataSource": "wikipedia",
            "interval": "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z",
            "version": "2024-01-01T00:00:00.000Z",
            "loadSpec": {
                "type": "local",
                "path": "/segments/wikipedia/2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z/2024-01-01T00:00:00.000Z/0/index.zip"
            },
            "dimensions": "page,user,language,city",
            "metrics": "count,added,deleted",
            "shardSpec": {"type": "numbered", "partitionNum": 0, "partitions": 1},
            "binaryVersion": 9,
            "size": 12345678,
            "identifier": "wikipedia_2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z_2024-01-01T00:00:00.000Z"
        }"#;
        let payload: SegmentPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.data_source, "wikipedia");
        assert_eq!(
            payload.interval,
            "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"
        );
        assert_eq!(payload.version, "2024-01-01T00:00:00.000Z");
        assert_eq!(payload.dimensions, "page,user,language,city");
        assert_eq!(payload.metrics, "count,added,deleted");
        assert_eq!(payload.binary_version, Some(9));
        assert_eq!(payload.size, 12345678);
        assert!(matches!(payload.load_spec, LoadSpec::Local { .. }));
        if let LoadSpec::Local { path } = &payload.load_spec {
            assert!(path.ends_with("index.zip"));
        }
        assert!(payload.sharding_spec.is_some());
        if let Some(ShardingSpec::Numbered {
            partition_num,
            partitions,
        }) = &payload.sharding_spec
        {
            assert_eq!(*partition_num, 0);
            assert_eq!(*partitions, 1);
        } else {
            panic!("expected Numbered sharding spec");
        }
    }

    #[test]
    fn parse_segment_payload_s3_zip() {
        let json = r#"{
            "dataSource": "clicks",
            "interval": "2024-06-01T00:00:00.000Z/2024-06-02T00:00:00.000Z",
            "version": "2024-06-01T12:00:00.000Z",
            "loadSpec": {
                "type": "s3_zip",
                "bucket": "druid-deep-storage",
                "key": "clicks/2024-06-01/0/index.zip"
            },
            "dimensions": "url,referrer,country",
            "metrics": "count,duration",
            "shardSpec": {"type": "hashed", "partitionNum": 2, "partitions": 4, "partitionDimensions": ["country"]},
            "binaryVersion": 9,
            "size": 98765432,
            "identifier": "clicks_2024-06-01T00:00:00.000Z_2024-06-02T00:00:00.000Z_2024-06-01T12:00:00.000Z_2"
        }"#;
        let payload: SegmentPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.data_source, "clicks");
        if let LoadSpec::S3Zip { bucket, key } = &payload.load_spec {
            assert_eq!(bucket, "druid-deep-storage");
            assert_eq!(key, "clicks/2024-06-01/0/index.zip");
        } else {
            panic!("expected S3Zip load spec");
        }
        if let Some(ShardingSpec::Hashed {
            partition_num,
            partitions,
            partition_dimensions,
        }) = &payload.sharding_spec
        {
            assert_eq!(*partition_num, 2);
            assert_eq!(*partitions, 4);
            assert_eq!(
                partition_dimensions.as_ref().unwrap(),
                &vec!["country".to_string()]
            );
        } else {
            panic!("expected Hashed sharding spec");
        }
    }

    #[test]
    fn parse_segment_payload_hdfs() {
        let json = r#"{
            "dataSource": "events",
            "interval": "2024-03-15T00:00:00.000Z/2024-03-16T00:00:00.000Z",
            "version": "2024-03-15T00:00:00.000Z",
            "loadSpec": {
                "type": "hdfs",
                "path": "hdfs://namenode:8020/druid/segments/events/2024-03-15/0/index.zip"
            },
            "dimensions": "event_type,user_id",
            "metrics": "count",
            "binaryVersion": 9,
            "size": 5555555,
            "identifier": "events_2024-03-15_v1_0"
        }"#;
        let payload: SegmentPayload = serde_json::from_str(json).unwrap();
        assert!(payload.sharding_spec.is_none());
        if let LoadSpec::Hdfs { path } = &payload.load_spec {
            assert!(path.starts_with("hdfs://"));
        } else {
            panic!("expected Hdfs load spec");
        }
    }

    #[test]
    fn parse_segment_payload_google_loadspec() {
        let json = r#"{
            "dataSource": "logs",
            "interval": "2024-04-01T00:00:00.000Z/2024-04-02T00:00:00.000Z",
            "version": "2024-04-01T00:00:00.000Z",
            "loadSpec": {
                "type": "google",
                "bucket": "druid-gcs-bucket",
                "path": "logs/2024-04-01/0/index.zip"
            },
            "dimensions": "level,service",
            "metrics": "count",
            "size": 1234567,
            "identifier": "logs_2024-04-01_v1_0"
        }"#;
        let payload: SegmentPayload = serde_json::from_str(json).unwrap();
        if let LoadSpec::Google { bucket, path } = &payload.load_spec {
            assert_eq!(bucket, "druid-gcs-bucket");
            assert_eq!(path, "logs/2024-04-01/0/index.zip");
        } else {
            panic!("expected Google load spec");
        }
    }

    #[test]
    fn parse_segment_payload_azure_loadspec() {
        let json = r#"{
            "dataSource": "metrics",
            "interval": "2024-05-01T00:00:00.000Z/2024-05-02T00:00:00.000Z",
            "version": "2024-05-01T00:00:00.000Z",
            "loadSpec": {
                "type": "azure",
                "container": "druid-container",
                "blob": "metrics/2024-05-01/0/index.zip"
            },
            "dimensions": "host,metric_name",
            "metrics": "value",
            "size": 7654321,
            "identifier": "metrics_2024-05-01_v1_0"
        }"#;
        let payload: SegmentPayload = serde_json::from_str(json).unwrap();
        if let LoadSpec::Azure { container, blob } = &payload.load_spec {
            assert_eq!(container, "druid-container");
            assert_eq!(blob, "metrics/2024-05-01/0/index.zip");
        } else {
            panic!("expected Azure load spec");
        }
    }

    // -- ShardingSpec variants ------------------------------------------------

    #[test]
    fn parse_sharding_spec_single() {
        let json = r#"{"type": "single", "partitionNum": 0}"#;
        let spec: ShardingSpec = serde_json::from_str(json).unwrap();
        assert!(matches!(spec, ShardingSpec::Single { partition_num: 0 }));
    }

    #[test]
    fn parse_sharding_spec_linear() {
        let json = r#"{"type": "linear", "partitionNum": 3}"#;
        let spec: ShardingSpec = serde_json::from_str(json).unwrap();
        assert!(matches!(spec, ShardingSpec::Linear { partition_num: 3 }));
    }

    #[test]
    fn parse_sharding_spec_none() {
        let json = r#"{"type": "none"}"#;
        let spec: ShardingSpec = serde_json::from_str(json).unwrap();
        assert!(matches!(spec, ShardingSpec::None {}));
    }

    #[test]
    fn numbered_overwrite_shard_spec_deserializes() {
        // A real segment-lock overwrite shardSpec: the partition number
        // is `partitionId`, and the extra bookkeeping fields are ignored.
        let json = r#"{"type":"numbered_overwrite","partitionId":32768,"startRootPartitionId":0,"endRootPartitionId":1,"minorVersion":1,"atomicUpdateGroupSize":1}"#;
        let spec: ShardingSpec = serde_json::from_str(json).expect("numbered_overwrite decodes");
        assert!(matches!(
            spec,
            ShardingSpec::NumberedOverwrite {
                partition_id: 32768
            }
        ));
    }

    #[test]
    fn range_shard_spec_deserializes() {
        // A DimensionRangeShardSpec: `partitionNum` is the partition
        // number; dimensions/start/end are ignored.
        let json = r#"{"type":"range","partitionNum":5,"dimensions":["x"],"start":["a"],"end":["z"],"numCorePartitions":8}"#;
        let spec: ShardingSpec = serde_json::from_str(json).expect("range decodes");
        assert!(matches!(spec, ShardingSpec::Range { partition_num: 5 }));
    }

    // -- SupervisorSpec -------------------------------------------------------

    #[test]
    fn parse_supervisor_spec_kafka() {
        let json = r#"{
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": "wiki-events",
                    "timestampSpec": {"column": "timestamp", "format": "auto"},
                    "dimensionsSpec": {"dimensions": ["page", "user"]}
                },
                "ioConfig": {
                    "topic": "wiki-events",
                    "consumerProperties": {"bootstrap.servers": "kafka:9092"},
                    "taskCount": 1,
                    "replicas": 1
                },
                "tuningConfig": {"type": "kafka", "maxRowsPerSegment": 5000000}
            }
        }"#;
        let spec: SupervisorSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.spec_type, "kafka");
        assert_eq!(spec.spec["ioConfig"]["topic"], "wiki-events");
    }

    #[test]
    fn parse_supervisor_spec_kinesis() {
        let json = r#"{
            "type": "kinesis",
            "spec": {
                "dataSchema": {"dataSource": "events"},
                "ioConfig": {
                    "stream": "events-stream",
                    "endpoint": "kinesis.us-east-1.amazonaws.com"
                }
            }
        }"#;
        let spec: SupervisorSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.spec_type, "kinesis");
    }

    // -- DruidRule variants ---------------------------------------------------

    #[test]
    fn parse_rule_load_forever() {
        let json = r#"{
            "type": "loadForever",
            "tieredReplicants": {"_default_tier": 2, "hot": 1}
        }"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::LoadForever { tier_replicants } = &rule {
            assert_eq!(tier_replicants["_default_tier"], 2);
            assert_eq!(tier_replicants["hot"], 1);
        } else {
            panic!("expected LoadForever");
        }
    }

    #[test]
    fn parse_rule_load_by_interval() {
        let json = r#"{
            "type": "loadByInterval",
            "interval": "2024-01-01/2024-07-01",
            "tieredReplicants": {"_default_tier": 1}
        }"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::LoadByInterval {
            interval,
            tier_replicants,
        } = &rule
        {
            assert_eq!(interval, "2024-01-01/2024-07-01");
            assert_eq!(tier_replicants["_default_tier"], 1);
        } else {
            panic!("expected LoadByInterval");
        }
    }

    #[test]
    fn parse_rule_load_by_period() {
        let json = r#"{
            "type": "loadByPeriod",
            "period": "P3M",
            "includeFuture": true,
            "tieredReplicants": {"_default_tier": 2}
        }"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::LoadByPeriod {
            period,
            include_future,
            tier_replicants,
        } = &rule
        {
            assert_eq!(period, "P3M");
            assert!(include_future);
            assert_eq!(tier_replicants["_default_tier"], 2);
        } else {
            panic!("expected LoadByPeriod");
        }
    }

    #[test]
    fn parse_rule_load_by_period_default_include_future() {
        let json = r#"{
            "type": "loadByPeriod",
            "period": "P1M",
            "tieredReplicants": {"_default_tier": 1}
        }"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::LoadByPeriod { include_future, .. } = &rule {
            assert!(!include_future, "includeFuture should default to false");
        } else {
            panic!("expected LoadByPeriod");
        }
    }

    #[test]
    fn parse_rule_drop_forever() {
        let json = r#"{"type": "dropForever"}"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        assert!(matches!(rule, DruidRule::DropForever {}));
    }

    #[test]
    fn parse_rule_drop_by_interval() {
        let json = r#"{"type": "dropByInterval", "interval": "2020-01-01/2021-01-01"}"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::DropByInterval { interval } = &rule {
            assert_eq!(interval, "2020-01-01/2021-01-01");
        } else {
            panic!("expected DropByInterval");
        }
    }

    #[test]
    fn parse_rule_drop_by_period() {
        let json = r#"{"type": "dropByPeriod", "period": "P6M", "includeFuture": false}"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        if let DruidRule::DropByPeriod {
            period,
            include_future,
        } = &rule
        {
            assert_eq!(period, "P6M");
            assert!(!include_future);
        } else {
            panic!("expected DropByPeriod");
        }
    }

    #[test]
    fn parse_rule_broadcast_forever() {
        let json = r#"{"type": "broadcastForever"}"#;
        let rule: DruidRule = serde_json::from_str(json).unwrap();
        assert!(matches!(rule, DruidRule::BroadcastForever {}));
    }

    #[test]
    fn parse_rule_array() {
        let json = r#"[
            {"type": "loadByPeriod", "period": "P1M", "tieredReplicants": {"_default_tier": 2}},
            {"type": "dropForever"}
        ]"#;
        let rules: Vec<DruidRule> = serde_json::from_str(json).unwrap();
        assert_eq!(rules.len(), 2);
        assert!(matches!(rules[0], DruidRule::LoadByPeriod { .. }));
        assert!(matches!(rules[1], DruidRule::DropForever {}));
    }

    // -- CoordinatorDynamicConfig ---------------------------------------------

    #[test]
    fn parse_coordinator_dynamic_config_full() {
        let json = r#"{
            "maxSegmentsToMove": 100,
            "replicationThrottleLimit": 500,
            "balancerComputeThreads": 4,
            "killDataSourceWhitelist": ["old_events"],
            "killPendingSegmentsSkipList": ["important_ds"],
            "maxSegmentsInNodeLoadingQueue": 500,
            "mergeBytesLimit": 536870912,
            "mergeSegmentsLimit": 100,
            "smartSegmentLoading": true
        }"#;
        let cfg: CoordinatorDynamicConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_segments_to_move, 100);
        assert_eq!(cfg.replication_throttle_limit, 500);
        assert_eq!(cfg.balancer_compute_threads, 4);
        assert_eq!(cfg.kill_data_source_whitelist, vec!["old_events"]);
        assert_eq!(cfg.kill_pending_segments_skip_list, vec!["important_ds"]);
        assert_eq!(cfg.max_segments_in_node_loading_queue, 500);
        assert_eq!(cfg.merge_bytes_limit, 536_870_912);
        assert_eq!(cfg.merge_segments_limit, 100);
        assert!(cfg.smart_segment_loading);
    }

    #[test]
    fn parse_coordinator_dynamic_config_defaults() {
        let json = r#"{}"#;
        let cfg: CoordinatorDynamicConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_segments_to_move, 0);
        assert_eq!(cfg.replication_throttle_limit, 0);
        assert!(cfg.kill_data_source_whitelist.is_empty());
        assert!(!cfg.smart_segment_loading);
    }

    // -- Round-trip serialization ---------------------------------------------

    #[test]
    fn segment_payload_round_trip() {
        let original = SegmentPayload {
            data_source: "test_ds".to_string(),
            interval: "2024-01-01/2024-01-02".to_string(),
            version: "2024-01-01T00:00:00.000Z".to_string(),
            load_spec: LoadSpec::Local {
                path: "/seg/test/0/index.zip".to_string(),
            },
            dimensions: "dim1,dim2".to_string(),
            metrics: "met1".to_string(),
            sharding_spec: Some(ShardingSpec::Single { partition_num: 0 }),
            binary_version: Some(9),
            size: 1024,
            identifier: "test_ds_2024-01-01_v1_0".to_string(),
        };
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: SegmentPayload = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.data_source, original.data_source);
        assert_eq!(deserialized.size, original.size);
        assert_eq!(deserialized.identifier, original.identifier);
    }

    #[test]
    fn druid_rule_round_trip() {
        let rules = vec![
            DruidRule::LoadByPeriod {
                period: "P1M".to_string(),
                include_future: true,
                tier_replicants: HashMap::from([("_default_tier".to_string(), 2)]),
            },
            DruidRule::DropForever {},
        ];
        let serialized = serde_json::to_string(&rules).unwrap();
        let deserialized: Vec<DruidRule> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.len(), 2);
    }

    // -- Serialized field names verify camelCase on the wire -------------------

    #[test]
    fn segment_payload_field_names_are_camel_case() {
        let payload = SegmentPayload {
            data_source: "ds".to_string(),
            interval: "2024-01-01/2024-01-02".to_string(),
            version: "v1".to_string(),
            load_spec: LoadSpec::Local {
                path: "/x".to_string(),
            },
            dimensions: "d".to_string(),
            metrics: "m".to_string(),
            sharding_spec: None,
            binary_version: None,
            size: 0,
            identifier: "id".to_string(),
        };
        let val: serde_json::Value = serde_json::to_value(&payload).unwrap();
        let obj = val.as_object().unwrap();
        assert!(obj.contains_key("dataSource"), "expected camelCase key");
        assert!(obj.contains_key("loadSpec"), "expected camelCase key");
        assert!(
            !obj.contains_key("data_source"),
            "should not have snake_case key"
        );
    }

    // -- validate_druid_metadata ----------------------------------------------

    #[tokio::test]
    async fn validate_empty_db() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        let report = validate_druid_metadata(&pool).await.unwrap();
        assert!(!report.is_compatible);
        assert_eq!(report.tables_missing.len(), 7);
        assert_eq!(report.tables_found.len(), 0);
    }

    #[tokio::test]
    async fn validate_initialized_db() {
        let store = ferrodruid_metadata::MetadataStore::new_in_memory()
            .await
            .unwrap();
        store.initialize().await.unwrap();

        // The MetadataStore uses a single-connection pool for in-memory DBs,
        // so we need to get its pool.  Since we cannot access it directly, we
        // create a second pool on a file-backed temp DB.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let file_store = ferrodruid_metadata::MetadataStore::new_sqlite(db_path.to_str().unwrap())
            .await
            .unwrap();
        file_store.initialize().await.unwrap();

        // Insert a segment so counts are non-zero.
        let seg = ferrodruid_metadata::SegmentMetadataRow {
            id: "seg1".to_string(),
            data_source: "wiki".to_string(),
            created_date: "2024-01-01T00:00:00Z".to_string(),
            start: "2024-01-01T00:00:00Z".to_string(),
            end: "2024-01-02T00:00:00Z".to_string(),
            version: "v1".to_string(),
            used: true,
            payload: json!({}),
        };
        file_store.insert_segment(&seg).await.unwrap();

        // Now validate via a raw pool.
        let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}", db_path.display()))
            .await
            .unwrap();
        let report = validate_druid_metadata(&pool).await.unwrap();
        assert!(report.is_compatible);
        assert_eq!(report.tables_missing.len(), 0);
        assert_eq!(report.tables_found.len(), 7);
        assert_eq!(report.segment_count, 1);
        assert_eq!(report.datasource_count, 1);
    }

    // -- read_druid_source (compat-7 source reader) ----------------------------

    /// A canonical local-loadSpec payload JSON for the reader fixtures.
    fn local_payload_json(ds: &str, path: &str) -> String {
        json!({
            "dataSource": ds,
            "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
            "version": "v1",
            "loadSpec": {"type": "local", "path": path},
            "dimensions": "region",
            "metrics": "value",
            "shardSpec": {"type": "numbered", "partitionNum": 0, "partitions": 1},
            "binaryVersion": 9,
            "size": 1024,
            "identifier": format!("{ds}_2015-09-12_v1_0"),
        })
        .to_string()
    }

    /// Create a writable SQLite fixture DB with a Druid-shaped
    /// `druid_segments` table (`"start"`/`"end"` quoted — `end` is a
    /// SQLite keyword) and hand back the open pool + path.
    async fn make_source_sqlite(dir: &std::path::Path) -> (sqlx::SqlitePool, String) {
        let path = dir.join("druid.db");
        let opts = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
        sqlx::query(
            "CREATE TABLE druid_segments (\
             id VARCHAR(255) NOT NULL PRIMARY KEY, \
             dataSource VARCHAR(255) NOT NULL, \
             created_date VARCHAR(255) NOT NULL, \
             \"start\" VARCHAR(255) NOT NULL, \
             \"end\" VARCHAR(255) NOT NULL, \
             partitioned BOOLEAN NOT NULL, \
             version VARCHAR(255) NOT NULL, \
             used BOOLEAN NOT NULL, \
             payload TEXT NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        (pool, path.to_str().unwrap().to_string())
    }

    /// Insert one druid_segments row with a TEXT payload.
    async fn insert_source_row(
        pool: &sqlx::SqlitePool,
        id: &str,
        ds: &str,
        used: i64,
        payload: &str,
    ) {
        sqlx::query(
            "INSERT INTO druid_segments \
             (id, dataSource, created_date, \"start\", \"end\", partitioned, version, used, payload) \
             VALUES (?, ?, '2026-01-01T00:00:00.000Z', '2015-09-12T00:00:00.000Z', \
             '2015-09-13T00:00:00.000Z', 1, 'v1', ?, ?)",
        )
        .bind(id)
        .bind(ds)
        .bind(used)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn read_druid_source_returns_used_rows_only_in_id_order() {
        let dir = tempfile::tempdir().unwrap();
        let (pool, path) = make_source_sqlite(dir.path()).await;
        insert_source_row(
            &pool,
            "b_used",
            "wiki",
            1,
            &local_payload_json("wiki", "/x/b"),
        )
        .await;
        insert_source_row(
            &pool,
            "a_used",
            "clicks",
            1,
            &local_payload_json("clicks", "/x/a"),
        )
        .await;
        insert_source_row(
            &pool,
            "c_unused",
            "wiki",
            0,
            &local_payload_json("wiki", "/x/c"),
        )
        .await;
        pool.close().await;

        let report = read_druid_source(&format!("sqlite://{path}"), None, None)
            .await
            .unwrap();
        assert_eq!(report.segments.len(), 2, "used = true rows only");
        assert_eq!(report.segments[0].id, "a_used", "ORDER BY id");
        assert_eq!(report.segments[1].id, "b_used");
        assert!(!report.truncated);
        let p = report.segments[0].payload.as_ref().unwrap();
        assert_eq!(p.data_source, "clicks");
        assert!(matches!(&p.load_spec, LoadSpec::Local { path } if path == "/x/a"));
        assert_eq!(report.segments[0].start, "2015-09-12T00:00:00.000Z");
        assert_eq!(report.segments[0].end, "2015-09-13T00:00:00.000Z");
        assert_eq!(report.segments[0].version, "v1");
    }

    #[tokio::test]
    async fn read_druid_source_datasource_filter_and_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let (pool, path) = make_source_sqlite(dir.path()).await;
        for i in 0..3 {
            insert_source_row(
                &pool,
                &format!("wiki_{i}"),
                "wiki",
                1,
                &local_payload_json("wiki", "/x"),
            )
            .await;
        }
        insert_source_row(
            &pool,
            "other",
            "clicks",
            1,
            &local_payload_json("clicks", "/x"),
        )
        .await;
        pool.close().await;
        let uri = format!("sqlite://{path}");

        let filtered = read_druid_source(&uri, Some("wiki"), None).await.unwrap();
        assert_eq!(filtered.segments.len(), 3, "--datasource filter");
        assert!(filtered.segments.iter().all(|s| s.data_source == "wiki"));

        let capped = read_druid_source(&uri, Some("wiki"), Some(2))
            .await
            .unwrap();
        assert_eq!(capped.segments.len(), 2);
        assert!(capped.truncated, "more used rows exist beyond the cap");

        let uncapped = read_druid_source(&uri, Some("wiki"), Some(10))
            .await
            .unwrap();
        assert_eq!(uncapped.segments.len(), 3);
        assert!(!uncapped.truncated);
    }

    #[tokio::test]
    async fn read_druid_source_malformed_payload_is_per_row_err_not_run_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (pool, path) = make_source_sqlite(dir.path()).await;
        insert_source_row(&pool, "bad_json", "wiki", 1, "{ this is not json").await;
        insert_source_row(
            &pool,
            "bad_loadspec",
            "wiki",
            1,
            &json!({
                "dataSource": "wiki",
                "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
                "version": "v1",
                "loadSpec": {"type": "ftp", "host": "example.invalid"},
                "dimensions": "d", "metrics": "m", "size": 1, "identifier": "x",
            })
            .to_string(),
        )
        .await;
        insert_source_row(&pool, "good", "wiki", 1, &local_payload_json("wiki", "/x")).await;
        pool.close().await;

        let report = read_druid_source(&format!("sqlite://{path}"), None, None)
            .await
            .unwrap();
        assert_eq!(
            report.segments.len(),
            3,
            "broken rows are returned, not dropped"
        );
        let by_id = |id: &str| {
            report
                .segments
                .iter()
                .find(|s| s.id == id)
                .unwrap_or_else(|| panic!("row {id} present"))
        };
        assert!(by_id("bad_json").payload.is_err());
        assert!(
            by_id("bad_loadspec").payload.is_err(),
            "an unknown loadSpec type is a per-row decode failure"
        );
        assert!(by_id("good").payload.is_ok());
    }

    #[tokio::test]
    async fn read_druid_source_rejects_oversized_payload() {
        let dir = tempfile::tempdir().unwrap();
        let (pool, path) = make_source_sqlite(dir.path()).await;
        // A normal row that must still decode alongside the bad one.
        insert_source_row(
            &pool,
            "ok_row",
            "wiki",
            1,
            &local_payload_json("wiki", "/x"),
        )
        .await;
        // A payload one byte OVER the cap.  `zeroblob(n)` lets SQLite
        // synthesize the large value without the test process ever
        // allocating it — and the reader must never transfer it either.
        sqlx::query(
            "INSERT INTO druid_segments \
             (id, dataSource, created_date, \"start\", \"end\", partitioned, version, used, payload) \
             VALUES ('huge_row', 'wiki', 'c', '2015-09-12T00:00:00.000Z', \
             '2015-09-13T00:00:00.000Z', 1, 'v1', 1, zeroblob(?))",
        )
        .bind(MAX_SEGMENT_PAYLOAD_BYTES + 1)
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let report = read_druid_source(&format!("sqlite://{path}"), None, None)
            .await
            .expect("one oversized row must not abort the whole read");
        assert_eq!(
            report.segments.len(),
            2,
            "both rows are returned (the oversized one as a per-row Err, not dropped)"
        );
        let by_id = |id: &str| {
            report
                .segments
                .iter()
                .find(|s| s.id == id)
                .unwrap_or_else(|| panic!("row {id} present"))
        };
        assert!(
            by_id("ok_row").payload.is_ok(),
            "the normal row still decodes: {:?}",
            by_id("ok_row").payload
        );
        let err = by_id("huge_row")
            .payload
            .as_ref()
            .expect_err("an oversized payload must be a per-row Err, never a 1GB transfer");
        assert!(
            err.contains("exceeds") && err.contains("cap"),
            "the reason names the byte cap: {err}"
        );
    }

    #[tokio::test]
    async fn read_druid_source_blob_payload_decodes_and_non_utf8_is_per_row_err() {
        let dir = tempfile::tempdir().unwrap();
        let (pool, path) = make_source_sqlite(dir.path()).await;
        // BLOB payload holding valid JSON (the PG BYTEA / MySQL BLOB shape).
        sqlx::query(
            "INSERT INTO druid_segments \
             (id, dataSource, created_date, \"start\", \"end\", partitioned, version, used, payload) \
             VALUES ('blob_ok', 'wiki', 'c', 's', 'e', 1, 'v1', 1, ?)",
        )
        .bind(local_payload_json("wiki", "/x").into_bytes())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO druid_segments \
             (id, dataSource, created_date, \"start\", \"end\", partitioned, version, used, payload) \
             VALUES ('blob_bad', 'wiki', 'c', 's', 'e', 1, 'v1', 1, ?)",
        )
        .bind(vec![0xffu8, 0xfe, 0x00, 0x01])
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let report = read_druid_source(&format!("sqlite://{path}"), None, None)
            .await
            .unwrap();
        assert_eq!(report.segments.len(), 2);
        assert!(report.segments[0].id == "blob_bad" || report.segments[1].id == "blob_bad");
        for s in &report.segments {
            match s.id.as_str() {
                "blob_ok" => assert!(s.payload.is_ok(), "BLOB JSON decodes: {:?}", s.payload),
                "blob_bad" => {
                    let err = s.payload.as_ref().expect_err("non-UTF-8 must be Err");
                    assert!(err.contains("UTF-8"), "reason names UTF-8: {err}");
                }
                other => panic!("unexpected row {other}"),
            }
        }
    }

    #[tokio::test]
    async fn read_druid_source_counts_rules_and_supervisors_or_none() {
        let dir = tempfile::tempdir().unwrap();
        let (pool, path) = make_source_sqlite(dir.path()).await;
        sqlx::query("CREATE TABLE druid_rules (id VARCHAR(255) PRIMARY KEY, payload TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO druid_rules (id, payload) VALUES ('r1', '[]'), ('r2', '[]')")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        let report = read_druid_source(&format!("sqlite://{path}"), None, None)
            .await
            .unwrap();
        assert_eq!(
            report.rules_found,
            Some(2),
            "rules are counted, not imported"
        );
        assert_eq!(
            report.supervisors_found, None,
            "a missing druid_supervisors table is None, not an error"
        );
    }

    #[tokio::test]
    async fn read_druid_source_refuses_memory_unknown_scheme_and_missing_file() {
        for bad in [
            ":memory:",
            "sqlite::memory:",
            "sqlite://",
            "sqlite://:memory:",
        ] {
            let err = read_druid_source(bad, None, None).await.err();
            assert!(err.is_some(), "{bad:?} must be refused");
        }
        let err = read_druid_source("derby://localhost/druid", None, None)
            .await
            .expect_err("unknown scheme is refused")
            .to_string();
        assert!(
            err.contains("derby") && err.contains("externalize"),
            "the Derby refusal must point at the externalization path: {err}"
        );
        // A missing SQLite file must be a loud connect error (read-only
        // open, never create-on-open).
        let missing = read_druid_source("/nonexistent/dir/druid.db", None, None).await;
        assert!(
            missing.is_err(),
            "a missing source DB file must not be created"
        );
    }

    #[test]
    fn parse_source_uri_dispatch() {
        assert_eq!(
            parse_source_uri("postgres://u:p@h:5432/druid").unwrap(),
            SourceBackend::Postgres
        );
        assert_eq!(
            parse_source_uri("postgresql://h/druid").unwrap(),
            SourceBackend::Postgres
        );
        assert_eq!(
            parse_source_uri("mysql://h:3306/druid").unwrap(),
            SourceBackend::MySql
        );
        assert_eq!(
            parse_source_uri("sqlite:///var/druid.db").unwrap(),
            SourceBackend::SqlitePath("/var/druid.db".to_string())
        );
        assert_eq!(
            parse_source_uri("/var/druid.db").unwrap(),
            SourceBackend::SqlitePath("/var/druid.db".to_string())
        );
        assert!(parse_source_uri("").is_err());
        assert!(parse_source_uri("derby://x").is_err());
    }

    #[test]
    fn parse_source_uri_rejects_bad_scheme_with_credentials() {
        // A leading digit makes the scheme invalid; the `://` string must
        // be REJECTED (not silently opened as a SQLite path whose error
        // would echo the password) and the error must redact the secret.
        let result = parse_source_uri("1postgres://alice:SUPERSECRET@host/db");
        let err = result
            .as_ref()
            .expect_err("an invalid scheme must be refused")
            .to_string();
        assert!(
            !err.contains("SUPERSECRET"),
            "the credential must be redacted from the error: {err}"
        );
        assert!(
            err.contains("***@") && err.contains("not a valid"),
            "the error redacts userinfo and explains the malformed scheme: {err}"
        );
        // It is NOT classified as a SQLite path.
        assert!(
            !matches!(result, Ok(SourceBackend::SqlitePath(_))),
            "a bad-scheme URI must never be treated as a SQLite path"
        );
        // A bare path (no `://`) is still a SQLite path.
        assert_eq!(
            parse_source_uri("/var/lib/x.db").unwrap(),
            SourceBackend::SqlitePath("/var/lib/x.db".to_string())
        );
    }

    #[test]
    fn redact_source_uri_masks_percent_encoded_password_key() {
        // A percent-encoded password key (`pass%77ord` == `password`)
        // must be masked — a driver decodes the key before matching, so
        // the raw-key check would leak it.
        let redacted = redact_source_uri("postgres://h/db?pass%77ord=SECRET");
        assert!(
            !redacted.contains("SECRET"),
            "percent-encoded password key must be masked: {redacted}"
        );
        assert!(
            redacted.contains("pass%77ord=***"),
            "the key is preserved, the value masked: {redacted}"
        );
        // A plain `password` query param is still masked.
        let plain = redact_source_uri("mysql://h/db?password=SECRET&sslmode=require");
        assert!(!plain.contains("SECRET"), "plain password masked: {plain}");
        assert!(plain.contains("password=***"), "{plain}");
        assert!(
            plain.contains("sslmode=require"),
            "non-secret kept: {plain}"
        );
        // Userinfo credential is still masked.
        let ui = redact_source_uri("postgres://user:SECRET@host/db");
        assert!(!ui.contains("SECRET"), "userinfo masked: {ui}");
        assert!(ui.contains("***@host"), "{ui}");
        // A bare path (no `://`) is unchanged.
        assert_eq!(redact_source_uri("/var/lib/x.db"), "/var/lib/x.db");
    }

    #[test]
    fn build_segments_sql_quotes_reserved_words_per_backend() {
        let pg = build_segments_sql(&SourceBackend::Postgres, true, 100, 0);
        assert!(pg.contains("\"start\"") && pg.contains("\"end\""), "{pg}");
        assert!(pg.contains("used = TRUE"), "{pg}");
        // The dataSource filter is the ONLY bound param.
        assert!(pg.contains("AND dataSource = $1"), "{pg}");
        // The projected `payload_len` is cast to bigint so sqlx-postgres
        // decodes it as i64 (PG `octet_length` returns int4); the CASE
        // comparison keeps the un-cast int4 expression.
        assert!(
            pg.contains("octet_length(payload)::bigint AS payload_len"),
            "{pg}"
        );
        assert!(
            pg.contains("CASE WHEN octet_length(payload) <= 16777216"),
            "{pg}"
        );
        assert!(pg.ends_with("ORDER BY id LIMIT 100 OFFSET 0"), "{pg}");

        let my = build_segments_sql(&SourceBackend::MySql, true, 50, 10);
        assert!(my.contains("`start`") && my.contains("`end`"), "{my}");
        assert!(
            my.contains("used = TRUE") && my.contains("AND dataSource = ?"),
            "{my}"
        );
        assert!(my.contains("OCTET_LENGTH(payload) AS payload_len"), "{my}");
        assert!(my.ends_with("ORDER BY id LIMIT 50 OFFSET 10"), "{my}");

        let sq = build_segments_sql(&SourceBackend::SqlitePath("x".into()), false, 7, 3);
        assert!(sq.contains("\"start\"") && sq.contains("\"end\""), "{sq}");
        assert!(sq.contains("used = 1"), "{sq}");
        assert!(
            sq.contains("length(CAST(payload AS BLOB)) AS payload_len"),
            "{sq}"
        );
        // The SQL length-gate NULLs an oversized payload at 16 MiB.
        assert!(
            sq.contains("CASE WHEN") && sq.contains("<= 16777216"),
            "{sq}"
        );
        assert!(sq.ends_with("ORDER BY id LIMIT 7 OFFSET 3"), "{sq}");
        for sql in [&pg, &my, &sq] {
            assert!(sql.starts_with("SELECT"), "read-only: SELECT only: {sql}");
            assert!(sql.contains("ORDER BY id"), "deterministic order: {sql}");
        }
    }

    /// Real-PostgreSQL source-reader witness (gated; the docker E2E
    /// harness covers the full import chain).  Point
    /// `FERRODRUID_TEST_PG_URI` at a THROWAWAY database: the test
    /// CREATES a Druid-canonical `druid_segments` (BYTEA payload) —
    /// refusing to run if one already exists — inserts fixture rows,
    /// reads them back through `read_druid_source`, and drops the table.
    #[ignore = "needs FERRODRUID_TEST_PG_URI pointing at a throwaway PostgreSQL database"]
    #[tokio::test]
    async fn read_druid_source_postgres_real_db() {
        let uri = std::env::var("FERRODRUID_TEST_PG_URI").expect("FERRODRUID_TEST_PG_URI");
        let pool = sqlx::PgPool::connect(&uri).await.expect("connect PG");
        // CREATE (not IF NOT EXISTS): refuse to touch a DB that already
        // holds a druid_segments table.
        sqlx::query(
            "CREATE TABLE druid_segments (\
             id VARCHAR(255) NOT NULL PRIMARY KEY, dataSource VARCHAR(255) NOT NULL, \
             created_date VARCHAR(255) NOT NULL, \"start\" VARCHAR(255) NOT NULL, \
             \"end\" VARCHAR(255) NOT NULL, partitioned BOOLEAN NOT NULL, \
             version VARCHAR(255) NOT NULL, used BOOLEAN NOT NULL, payload BYTEA NOT NULL)",
        )
        .execute(&pool)
        .await
        .expect("druid_segments already exists — point FERRODRUID_TEST_PG_URI at a throwaway DB");
        let insert = "INSERT INTO druid_segments \
             (id, dataSource, created_date, \"start\", \"end\", partitioned, version, used, payload) \
             VALUES ($1, $2, 'c', '2015-09-12T00:00:00.000Z', '2015-09-13T00:00:00.000Z', TRUE, 'v1', $3, $4)";
        sqlx::query(insert)
            .bind("pg_used")
            .bind("wiki")
            .bind(true)
            .bind(local_payload_json("wiki", "/x").into_bytes())
            .execute(&pool)
            .await
            .expect("insert used row");
        sqlx::query(insert)
            .bind("pg_unused")
            .bind("wiki")
            .bind(false)
            .bind(local_payload_json("wiki", "/y").into_bytes())
            .execute(&pool)
            .await
            .expect("insert unused row");
        // An oversized used row: `octet_length(payload) > cap` must make
        // the SQL length-gate NULL the payload (never transferring it)
        // and the reader skip it as a per-row Err.
        sqlx::query(insert)
            .bind("pg_oversized")
            .bind("wiki")
            .bind(true)
            .bind(vec![b'a'; (MAX_SEGMENT_PAYLOAD_BYTES + 1) as usize])
            .execute(&pool)
            .await
            .expect("insert oversized row");

        let report = read_druid_source(&uri, None, None)
            .await
            .expect("read PG source");
        sqlx::query("DROP TABLE druid_segments")
            .execute(&pool)
            .await
            .expect("drop fixture table");
        pool.close().await;

        assert_eq!(
            report.segments.len(),
            2,
            "used = TRUE only (PG BOOLEAN); the oversized used row is returned as a per-row Err"
        );
        let by_id = |id: &str| {
            report
                .segments
                .iter()
                .find(|s| s.id == id)
                .unwrap_or_else(|| panic!("row {id} present"))
        };
        let p = by_id("pg_used")
            .payload
            .as_ref()
            .expect("BYTEA payload decodes");
        assert_eq!(p.data_source, "wiki");
        assert!(matches!(&p.load_spec, LoadSpec::Local { path } if path == "/x"));
        let err = by_id("pg_oversized")
            .payload
            .as_ref()
            .expect_err("oversized PG payload must be skipped as a per-row Err");
        assert!(
            err.contains("exceeds") && err.contains("cap"),
            "the reason names the byte cap (PG octet_length gate): {err}"
        );
    }

    /// Real-MySQL source-reader witness (gated) — same protocol as the
    /// PostgreSQL test, with a BLOB payload and TINYINT `used`.  Point
    /// `FERRODRUID_TEST_MYSQL_URI` at a THROWAWAY database.
    #[ignore = "needs FERRODRUID_TEST_MYSQL_URI pointing at a throwaway MySQL database"]
    #[tokio::test]
    async fn read_druid_source_mysql_real_db() {
        let uri = std::env::var("FERRODRUID_TEST_MYSQL_URI").expect("FERRODRUID_TEST_MYSQL_URI");
        let pool = sqlx::MySqlPool::connect(&uri).await.expect("connect MySQL");
        sqlx::query(
            "CREATE TABLE druid_segments (\
             id VARCHAR(255) NOT NULL PRIMARY KEY, dataSource VARCHAR(255) NOT NULL, \
             created_date VARCHAR(255) NOT NULL, `start` VARCHAR(255) NOT NULL, \
             `end` VARCHAR(255) NOT NULL, partitioned BOOLEAN NOT NULL, \
             version VARCHAR(255) NOT NULL, used BOOLEAN NOT NULL, payload LONGBLOB NOT NULL)",
        )
        .execute(&pool)
        .await
        .expect(
            "druid_segments already exists — point FERRODRUID_TEST_MYSQL_URI at a throwaway DB",
        );
        let insert = "INSERT INTO druid_segments \
             (id, dataSource, created_date, `start`, `end`, partitioned, version, used, payload) \
             VALUES (?, ?, 'c', '2015-09-12T00:00:00.000Z', '2015-09-13T00:00:00.000Z', 1, 'v1', ?, ?)";
        sqlx::query(insert)
            .bind("my_used")
            .bind("wiki")
            .bind(1i64)
            .bind(local_payload_json("wiki", "/x").into_bytes())
            .execute(&pool)
            .await
            .expect("insert used row");
        sqlx::query(insert)
            .bind("my_unused")
            .bind("wiki")
            .bind(0i64)
            .bind(local_payload_json("wiki", "/y").into_bytes())
            .execute(&pool)
            .await
            .expect("insert unused row");
        // An oversized used row (LONGBLOB): `OCTET_LENGTH(payload) > cap`
        // must make the SQL length-gate NULL the payload and the reader
        // skip it as a per-row Err.  (Needs the server's
        // `max_allowed_packet` >= ~16 MiB — MySQL 8's default is 64 MiB.)
        sqlx::query(insert)
            .bind("my_oversized")
            .bind("wiki")
            .bind(1i64)
            .bind(vec![b'a'; (MAX_SEGMENT_PAYLOAD_BYTES + 1) as usize])
            .execute(&pool)
            .await
            .expect("insert oversized row");

        let report = read_druid_source(&uri, None, None)
            .await
            .expect("read MySQL source");
        sqlx::query("DROP TABLE druid_segments")
            .execute(&pool)
            .await
            .expect("drop fixture table");
        pool.close().await;

        assert_eq!(
            report.segments.len(),
            2,
            "used = TRUE only (MySQL TINYINT); the oversized used row is a per-row Err"
        );
        let by_id = |id: &str| {
            report
                .segments
                .iter()
                .find(|s| s.id == id)
                .unwrap_or_else(|| panic!("row {id} present"))
        };
        assert!(
            by_id("my_used").payload.is_ok(),
            "BLOB payload decodes: {:?}",
            by_id("my_used").payload
        );
        let err = by_id("my_oversized")
            .payload
            .as_ref()
            .expect_err("oversized MySQL payload must be skipped as a per-row Err");
        assert!(
            err.contains("exceeds") && err.contains("cap"),
            "the reason names the byte cap (MySQL OCTET_LENGTH gate): {err}"
        );
    }
}
