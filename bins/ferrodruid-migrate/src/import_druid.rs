// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `ferrodruid-migrate import-druid-metadata` — import the segments an
//! EXISTING Apache Druid metadata database records (its
//! `druid_segments` table on PostgreSQL, MySQL, or SQLite) into a
//! FerroDruid single-binary deployment (compat-7).
//!
//! The source DB is READ-ONLY input: only `SELECT`s are ever issued
//! against it (`ferrodruid_metadata_schema::read_druid_source` — never
//! FerroDruid's schema bootstrap), and a source URI naming the same
//! database as the target `--metadata-uri` is refused loudly up front.
//!
//! Per `used = true` source row:
//!
//! 1. the identity is derived from the row's authoritative COLUMNS —
//!    `dataSource` / `start`+`end` interval / `version` — with the
//!    PAYLOAD cross-checked against them (dataSource/interval/version
//!    must agree, else the row is a loud per-row skip) and the
//!    `shardSpec.partitionNum` (`0` when the shardSpec is absent) taken
//!    from the payload — the compat-2 R7 full-tuple identity, never the
//!    raw `identifier` string (which is not injective);
//! 2. the payload `loadSpec` is resolved to a LOCAL artifact:
//!    * `local` — the `local.path` as-is when it exists on this host,
//!      else longest-suffix probing under the `--deep-storage` remap
//!      base (the "Druid deep storage copied/mounted here" case);
//!    * `s3_zip` (W-C) — the row's `bucket`/`key` are validated
//!      (untrusted input) and the `index.zip` is FETCHED from S3 into
//!      a per-row staging tempdir through the product's own
//!      [`ferrodruid_deep_storage::S3DeepStorage`] (client from the
//!      `AWS_*` environment; `AWS_ENDPOINT` + `AWS_ALLOW_HTTP=true`
//!      for S3-compatible stores), the tempdir becoming the
//!      containment root — a fetch failure is a loud per-row failure;
//!    * `hdfs` / `google` / `azure` remain per-segment LOUD skips —
//!      the run continues;
//! 3. the segment runs through the compat-2 attach tail VERBATIM
//!    ([`crate::attach::attach_one`]): staged materialize → v9 read
//!    gate → content hash → durable blob upload (**P**) → `used = TRUE`
//!    metadata row (**M**), per-segment fail-safe.
//!
//! Nothing is eagerly loaded: the next `ferrodruid serve` startup's
//! bootstrap reload downloads, hash-verifies, and loads every imported
//! segment — it is then query-visible.
//!
//! ## Honest limitations (compat-7 v1)
//!
//! * **`used = true` rows only** — segments Druid marked unused are NOT
//!   imported (this deliberately differs from `attach`, which has no
//!   better signal than the filesystem and imports every artifact it
//!   finds).  Multiple used versions of one interval are all imported;
//!   FerroDruid's own version semantics resolve visibility.
//! * **`local` + `s3_zip` loadSpecs**: hdfs / google / azure rows are
//!   loud per-segment skips — pull those blobs to a local directory and
//!   re-run with `--deep-storage`.  An `s3_zip` row names its OWN
//!   bucket, which the operator's `AWS_*` credentials will read: scope
//!   the credentials when the source DB is not fully trusted (the
//!   fetched bytes still pass the v9 gate + identity cross-checks, and
//!   the per-object download is capped).  A `--dry-run` still performs
//!   the s3 fetch (the read gate needs the bytes); it writes nothing.
//! * **PostgreSQL / MySQL / SQLite sources.**  Derby has no Rust
//!   driver: externalize the Druid metadata to PostgreSQL/MySQL first
//!   (standard Druid operations), or use `attach` on the deep-storage
//!   directory.
//! * **Rules / supervisors / config are NOT imported** — the report
//!   prints their source row counts only.
//! * The same-database refusal is a BEST-EFFORT footgun guard: exact
//!   string + SQLite-path canonicalize + a normalized network-URI
//!   identity tuple that resolves the connection target from
//!   `hostaddr`/`host`/authority (+ loopback-folding, default ports,
//!   `dbname`/`user` precedence) exactly as sqlx/libpq does for the
//!   COMMON single-host forms.  It deliberately does NOT emulate exotic
//!   libpq features — multi-host lists, `service=`, `passfile`,
//!   unix-socket `host=/path`, or DNS-name-vs-resolved-IP — so two
//!   differently-spelled URIs for one database MAY not be equated in
//!   those cases (documented residual); ensure `--source-uri` ≠
//!   `--metadata-uri`.
//! * Same target scope as `attach`: single-binary deployments, local
//!   FerroDruid deep storage on the target side, run offline (stop the
//!   instance first), restart to become query-visible.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

use ferrodruid_deep_storage::{LocalDeepStorage, create_dir_all_durable};
use ferrodruid_metadata::{MetadataStore, MetadataUri, parse_metadata_uri, redact_metadata_uri};
use ferrodruid_metadata_schema::{
    DruidSourceReport, DruidSourceSegmentRow, LoadSpec, ShardingSpec, read_druid_source,
};

use crate::assess::{self, ArtifactKind, FoundArtifact, SegmentIdentity};
use crate::attach::{AttachParams, AttachStatus, SegmentIdent, attach_one};

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// Everything the `import-druid-metadata` subcommand needs, as parsed
/// by `main`.
pub(crate) struct ImportParams {
    /// SOURCE Druid metadata DB URI (`postgres://…`, `mysql://…`,
    /// `sqlite://<path>`, or a bare SQLite file path).  READ-ONLY.
    pub(crate) source_uri: String,
    /// Optional local base directory the source loadSpec `local.path`
    /// values live under / are remapped into.
    pub(crate) deep_storage: Option<PathBuf>,
    /// FerroDruid deep-storage base directory to import into.
    pub(crate) ferro_deep_storage: PathBuf,
    /// Target FerroDruid metadata store path or URI.
    pub(crate) metadata_uri: String,
    /// Only import segments of this Druid dataSource (filtered in the
    /// source SELECT).
    pub(crate) datasource: Option<String>,
    /// Report without writing anything.
    pub(crate) dry_run: bool,
    /// Replace a segment already present under the same id.
    pub(crate) force: bool,
    /// Read at most N used source rows.
    pub(crate) max_segments: Option<usize>,
    /// OPT-IN lossy mode (`--allow-unreadable-columns`): drop columns
    /// the v9 reader cannot decode instead of failing the segment (see
    /// [`crate::attach::AttachParams::allow_unreadable_columns`] — the
    /// shared attach tail implements it).  Default OFF = strict.
    pub(crate) allow_unreadable_columns: bool,
}

// ---------------------------------------------------------------------------
// Per-row outcome model
// ---------------------------------------------------------------------------

/// Outcome of processing one source row.
enum ImportStatus {
    /// P→M completed through the attach tail.
    Imported {
        /// Row count reported by the v9 reader.
        rows: usize,
        /// An existing row/blob was replaced (`--force`).
        replaced: bool,
    },
    /// Dry run: every gate passed; nothing was written.
    WouldImport {
        /// Row count reported by the v9 reader.
        rows: usize,
    },
    /// A row with the same segment id already exists (no `--force`).
    SkippedExists,
    /// Unsupported loadSpec type (hdfs/google/azure) — loudly skipped.
    SkippedLoadSpec {
        /// Human-readable skip reason naming the loadSpec type.
        reason: String,
    },
    /// Loud per-segment failure; nothing committed for this row.
    Failed {
        /// Human-readable reason, verbatim from the failing layer.
        reason: String,
    },
}

/// One processed source row, for the report.
struct ImportRecord {
    /// The source row's `id` column (reporting only — never an identity
    /// source).
    source_id: String,
    /// The resolved local artifact, once resolution succeeded.
    artifact_path: Option<PathBuf>,
    /// Synthesized ferro segment id, when the attach tail got that far.
    ferro_id: Option<String>,
    status: ImportStatus,
    /// Columns DROPPED because their decode failed — non-empty only
    /// under `--allow-unreadable-columns`; reported loudly per row.
    dropped_columns: Vec<String>,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run the `import-druid-metadata` subcommand.
///
/// Returns `Err` only for operational failures (unreachable source DB,
/// source == target, store or deep-storage base unusable).  Per-row
/// failures and skips are report content — every row is an independent
/// P→M unit — and the process exits 0 once the run completed.
pub(crate) async fn run(params: ImportParams) -> Result<(), Box<dyn std::error::Error>> {
    refuse_same_store(&params.source_uri, &params.metadata_uri)?;
    if let Some(base) = &params.deep_storage
        && !base.is_dir()
    {
        return Err(format!(
            "--deep-storage {} is not a directory (it must be the local base the source \
             loadSpec paths live under / are remapped into)",
            base.display()
        )
        .into());
    }

    // 1. Read the SOURCE rows (used = true only; SELECT-only reader).
    let source = read_druid_source(
        &params.source_uri,
        params.datasource.as_deref(),
        params.max_segments,
    )
    .await?;

    // 2. The TARGET metadata store — a dry run must write NOTHING to the
    //    target: no file creation, no `initialize()` DDL (which would
    //    grow a fresh SQLite target from 0 bytes and run CREATE TABLE on
    //    PG/MySQL).  So under --dry-run we only ever open the store
    //    READ-ONLY and probe whether it is already initialized (for
    //    accurate existing-segment skip reporting); an absent or
    //    uninitialized target is simply `None` (every row reports as
    //    WouldImport).  A real run connects, initializes, and writes.
    let store: Option<MetadataStore> = if params.dry_run {
        match parse_metadata_uri(&params.metadata_uri)? {
            MetadataUri::SqlitePath(path) => {
                if Path::new(&path).is_file() {
                    let s = MetadataStore::open_sqlite_read_only(&path).await?;
                    if s.is_initialized().await? {
                        Some(s)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            MetadataUri::Postgres | MetadataUri::MySql | MetadataUri::SqliteMemory => {
                // Connect only (no DDL); a fresh/uninitialized server DB
                // (and a fresh in-memory SQLite) probes as not initialized.
                let s = MetadataStore::connect(&params.metadata_uri).await?;
                if s.is_initialized().await? {
                    Some(s)
                } else {
                    None
                }
            }
        }
    } else {
        let s = MetadataStore::connect(&params.metadata_uri).await?;
        s.initialize().await?;
        Some(s)
    };

    let storage = LocalDeepStorage::new(params.ferro_deep_storage.clone());
    if !params.dry_run {
        // Durable base BEFORE any upload — same H7/H2 discipline as
        // `attach` and the `serve` startup.
        create_dir_all_durable(&params.ferro_deep_storage).await?;
    }

    // 3. Per-row import through the compat-2 attach tail.  One shared
    //    `seen` map across the run keeps the R7 collision discipline:
    //    two DISTINCT identities colliding onto one ferro_id are a loud
    //    refusal, a duplicate row of the SAME identity a skip.
    let mut seen: HashMap<String, SegmentIdent> = HashMap::new();
    let mut records: Vec<ImportRecord> = Vec::new();
    for row in &source.segments {
        let record = import_one(row, &params, store.as_ref(), &storage, &mut seen).await;
        records.push(record);
    }

    print_report(&params, &records, &source);
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-row pipeline
// ---------------------------------------------------------------------------

/// Process one source row: payload gate → loadSpec resolution →
/// identity synthesis → the compat-2 attach tail.  Never propagates a
/// failure — every failure is a loud per-row status.
async fn import_one(
    row: &DruidSourceSegmentRow,
    params: &ImportParams,
    store: Option<&MetadataStore>,
    storage: &LocalDeepStorage,
    seen: &mut HashMap<String, SegmentIdent>,
) -> ImportRecord {
    let mut record = ImportRecord {
        source_id: row.id.clone(),
        artifact_path: None,
        ferro_id: None,
        status: ImportStatus::Failed {
            reason: String::new(),
        },
        dropped_columns: Vec::new(),
    };

    // 0. A row whose columns/payload could not be decoded is a loud
    //    per-row failure (unknown loadSpec types land here too).
    let payload = match &row.payload {
        Ok(p) => p,
        Err(reason) => {
            record.status = ImportStatus::Failed {
                reason: format!("source row is undecodable: {reason}"),
            };
            return record;
        }
    };

    // 1. Consistency gate: the identity is derived from the row COLUMNS
    //    (below), but the `--datasource` filter ran on the dataSource
    //    COLUMN — a row whose PAYLOAD dataSource disagrees with the
    //    column is inconsistent/hostile and must not slip an unfiltered
    //    identity through.
    if payload.data_source != row.data_source {
        record.status = ImportStatus::Failed {
            reason: format!(
                "payload dataSource {:?} disagrees with the row's dataSource column {:?} — \
                 refusing an inconsistent identity (untrusted source row)",
                payload.data_source, row.data_source
            ),
        };
        return record;
    }
    // 1b. The identity is derived from the row's authoritative
    //     `start`/`end`/`version` COLUMNS (below); refuse a payload whose
    //     interval/version disagree with them, so a hostile/corrupt row
    //     cannot import under a fabricated identity (R5).  Version is an
    //     opaque string Druid copies byte-identically into both places;
    //     the interval is compared semantically (millis) so a benign ISO
    //     formatting difference does not false-mismatch.
    if payload.version != row.version {
        record.status = ImportStatus::Failed {
            reason: format!(
                "payload version {:?} disagrees with the row's version column {:?} — \
                 refusing an inconsistent identity",
                payload.version, row.version
            ),
        };
        return record;
    }
    if let Err(reason) =
        crate::attach::payload_interval_matches_columns(&payload.interval, &row.start, &row.end)
    {
        record.status = ImportStatus::Failed {
            reason: format!("payload interval disagrees with the row start/end columns: {reason}"),
        };
        return record;
    }

    // 2. loadSpec resolution — `local` resolves on this host, `s3_zip`
    //    (W-C) is FETCHED into a per-row staging tempdir; hdfs/google/
    //    azure are LOUD skips and the run continues.  A resolution/fetch
    //    failure is a loud per-row failure.
    let resolved = match &payload.load_spec {
        LoadSpec::Local { path } => {
            match resolve_local_artifact(path, params.deep_storage.as_deref()) {
                Ok(r) => r,
                Err(reason) => {
                    record.status = ImportStatus::Failed { reason };
                    return record;
                }
            }
        }
        LoadSpec::S3Zip { bucket, key } => match resolve_s3_zip_artifact(bucket, key).await {
            Ok(r) => r,
            Err(reason) => {
                record.status = ImportStatus::Failed { reason };
                return record;
            }
        },
        other @ (LoadSpec::Hdfs { .. } | LoadSpec::Google { .. } | LoadSpec::Azure { .. }) => {
            record.status = ImportStatus::SkippedLoadSpec {
                reason: format!(
                    "loadSpec type `{}` is not supported (scope: local deep storage \
                     and s3_zip) — pull the segment blob to a local directory and \
                     re-run with --deep-storage",
                    load_spec_type_name(other)
                ),
            };
            return record;
        }
    };
    record.artifact_path = Some(resolved.artifact.path.clone());

    // 3. Identity synthesis from the row's authoritative COLUMNS
    //    (dataSource/start/end/version) — not the payload descriptor
    //    (R5 defense-in-depth; the gate above guarantees they agree, so
    //    this is equivalent for consistent rows and strictly safer for
    //    inconsistent ones).  The partition still comes from the payload
    //    shardSpec; H5 validation happens inside the attach tail.
    let identity = SegmentIdentity::from_metadata_db(
        row.data_source.clone(),
        format!("{}/{}", row.start, row.end),
        row.version.clone(),
        partition_from(payload.sharding_spec.as_ref()),
    );

    // 4. The compat-2 attach tail, VERBATIM: staged materialize → v9
    //    gate → hash → P (upload) → M (used = TRUE row).  The
    //    per-segment `deep_storage` is the containment root the staged
    //    materialization verifies every source open against.
    let tail_params = AttachParams {
        deep_storage: resolved.root,
        ferro_deep_storage: params.ferro_deep_storage.clone(),
        metadata_uri: params.metadata_uri.clone(),
        datasource: None,
        dry_run: params.dry_run,
        force: params.force,
        max_segments: None,
        allow_unreadable_columns: params.allow_unreadable_columns,
    };
    let attach_record = attach_one(
        &resolved.artifact,
        &identity,
        store,
        storage,
        &tail_params,
        seen,
    )
    .await;
    record.ferro_id = attach_record.ferro_id;
    record.dropped_columns = attach_record.dropped_columns;
    record.status = match attach_record.status {
        AttachStatus::Attached { rows, replaced } => ImportStatus::Imported { rows, replaced },
        AttachStatus::WouldAttach { rows } => ImportStatus::WouldImport { rows },
        AttachStatus::SkippedExists => ImportStatus::SkippedExists,
        AttachStatus::Failed { reason } => ImportStatus::Failed { reason },
    };
    record
}

/// The partition number a payload's shardSpec carries, as a string for
/// the identity tuple: `partitionNum` for every sharded variant, `"0"`
/// for `none` and for an ABSENT shardSpec (Druid's single-shard
/// default).  A negative number is passed through verbatim — the attach
/// tail's H5 partition validation rejects it loudly.
fn partition_from(spec: Option<&ShardingSpec>) -> String {
    match spec {
        None | Some(ShardingSpec::None {}) => "0".to_string(),
        Some(
            ShardingSpec::Numbered { partition_num, .. }
            | ShardingSpec::Hashed { partition_num, .. }
            | ShardingSpec::Single { partition_num }
            | ShardingSpec::Linear { partition_num }
            | ShardingSpec::Range { partition_num },
        ) => partition_num.to_string(),
        // `numbered_overwrite` carries the partition number as
        // `partitionId`; pass it through verbatim like the others.
        Some(ShardingSpec::NumberedOverwrite { partition_id }) => partition_id.to_string(),
    }
}

/// The wire name of a loadSpec type, for the loud skip reason.
fn load_spec_type_name(spec: &LoadSpec) -> &'static str {
    match spec {
        LoadSpec::Local { .. } => "local",
        LoadSpec::S3Zip { .. } => "s3_zip",
        LoadSpec::Hdfs { .. } => "hdfs",
        LoadSpec::Google { .. } => "google",
        LoadSpec::Azure { .. } => "azure",
    }
}

// ---------------------------------------------------------------------------
// loadSpec local-path resolution
// ---------------------------------------------------------------------------

/// A resolved local artifact plus the containment root the attach
/// tail's staged materialization verifies every source open against.
#[derive(Debug)]
struct ResolvedArtifact {
    artifact: FoundArtifact,
    root: PathBuf,
    /// Per-row staging tempdir guard for a REMOTE (s3) artifact — the
    /// fetched `index.zip` (and the containment `root`) live inside it,
    /// so it must outlive the whole attach tail.  `None` for `local`
    /// loadSpecs.
    _staging: Option<tempfile::TempDir>,
}

/// Resolve a `loadSpec.local.path` from an UNTRUSTED source row to a
/// local artifact.
///
/// `--deep-storage <base>` is a MANDATORY containment ceiling for every
/// local loadSpec path: without it NO path resolves — absolute or
/// relative — because an untrusted DB must never name an arbitrary
/// readable file (the `/etc/shadow` class).  The containment root is
/// ALWAYS `base`, never a path derived from the untrusted input.
///
/// Candidates, in priority order (first EXISTING candidate that
/// canonicalizes under `base` wins and is then classified loudly —
/// probing never continues past an existing, contained but unusable
/// path, so a wrong layout cannot silently fall through to a shorter,
/// possibly WRONG suffix match):
///
/// 1. the path as-is, when absolute AND it canonicalizes under
///    `canonicalize(base)` (the "same box / NFS-mounted deep storage"
///    case);
/// 2. longest-suffix probing under `base` (`base/<full path>`, then
///    dropping leading components one at a time) — the "Druid
///    deep-storage dir copied/rsynced here" case, where the Druid-host
///    prefix is unknown.
///
/// Hardening: `..` / `.` components and NUL bytes are rejected up front
/// (path traversal from a hostile DB), a final component that is a
/// symlink is refused, and every selected candidate must satisfy
/// `canonicalize(cand).starts_with(canonicalize(base))` — a
/// canonicalize-based check, so an INTERMEDIATE directory component
/// that is a symlink escaping `base` is caught too, which a lexical
/// check would miss (the attach tail re-verifies containment on every
/// open).
fn resolve_local_artifact(raw: &str, remap: Option<&Path>) -> Result<ResolvedArtifact, String> {
    if raw.is_empty() {
        return Err("loadSpec local.path is empty".to_string());
    }
    if raw.contains('\0') {
        return Err("loadSpec local.path contains a NUL byte — refused".to_string());
    }
    let path = Path::new(raw);
    for comp in path.components() {
        match comp {
            Component::Normal(_) | Component::RootDir => {}
            Component::ParentDir => {
                return Err(format!(
                    "loadSpec local.path {raw:?} contains a `..` component — path \
                     traversal from an untrusted source DB is refused"
                ));
            }
            Component::CurDir => {
                return Err(format!(
                    "loadSpec local.path {raw:?} contains a `.` component — refused \
                     (untrusted source DB)"
                ));
            }
            Component::Prefix(_) => {
                return Err(format!(
                    "loadSpec local.path {raw:?} carries a filesystem prefix — refused"
                ));
            }
        }
    }

    let Some(base) = remap else {
        return Err(format!(
            "loadSpec local.path {raw:?} cannot be resolved without a containment \
             ceiling — pass --deep-storage <dir> as the base directory the untrusted \
             source DB's local paths are read from under (paths are NEVER read as-is)"
        ));
    };
    let canonical_base = std::fs::canonicalize(base).map_err(|e| {
        format!(
            "--deep-storage {} cannot be canonicalized: {e} — it must name an \
             existing directory",
            base.display()
        )
    })?;

    // Candidates in priority order; the containment root of EVERY
    // candidate is `base`.
    let mut candidates: Vec<PathBuf> = Vec::new();
    if path.is_absolute()
        && let Ok(canon) = std::fs::canonicalize(path)
        && canon.starts_with(&canonical_base)
    {
        candidates.push(path.to_path_buf());
    }
    let comps: Vec<&std::ffi::OsStr> = path
        .components()
        .filter_map(|c| match c {
            Component::Normal(n) => Some(n),
            _ => None,
        })
        .collect();
    for skip in 0..comps.len() {
        let mut cand = base.to_path_buf();
        for c in &comps[skip..] {
            cand.push(c);
        }
        candidates.push(cand);
    }

    for cand in &candidates {
        let Ok(meta) = std::fs::symlink_metadata(cand) else {
            continue;
        };
        if meta.file_type().is_symlink() {
            return Err(format!(
                "resolved loadSpec path {} is a symlink — symlinks are never followed \
                 (untrusted source DB)",
                cand.display()
            ));
        }
        // Canonicalize-based containment: an intermediate symlink
        // component escaping the base makes this candidate INVALID
        // (skip it — probing continues, ultimately a loud error).
        let Ok(canon) = std::fs::canonicalize(cand) else {
            continue;
        };
        if !canon.starts_with(&canonical_base) {
            continue;
        }
        // Pin the CANONICAL, symlink-resolved artifact path AND the
        // CANONICAL base as the containment root (R8): if we returned the
        // raw `cand`/`base` (both routed through the untrusted
        // `--deep-storage` symlink), an attacker who atomically repoints
        // that symlink between here and the attach tail's re-canonicalize
        // could swap the whole tree and have foreign bytes imported under
        // the approved identity.  With the real paths pinned, a later
        // root-symlink swap appears in no path we use, and bytes outside
        // canonical A fail the tail's containment check.
        return classify_artifact(&canon, &canonical_base, raw);
    }
    Err(format!(
        "loadSpec local.path {raw:?} does not resolve under --deep-storage {} (probed \
         as-is when absolute-under-base, plus by longest suffix; candidates outside \
         the base are never read) — copy/mount the Druid deep-storage directory and \
         point --deep-storage at it",
        base.display()
    ))
}

/// Classify an EXISTING resolved path as an artifact: a regular file is
/// treated as an `index.zip` archive (the zip gate fails loudly if it
/// is not one); a directory must directly hold `meta.smoosh`, or hold
/// it under `index/` (the Druid 31 raw local layout).
fn classify_artifact(path: &Path, root: &Path, raw: &str) -> Result<ResolvedArtifact, String> {
    if path.is_file() {
        return Ok(ResolvedArtifact {
            artifact: FoundArtifact {
                path: path.to_path_buf(),
                kind: ArtifactKind::IndexZip,
            },
            root: root.to_path_buf(),
            _staging: None,
        });
    }
    if path.is_dir() {
        if path.join("meta.smoosh").is_file() {
            return Ok(ResolvedArtifact {
                artifact: FoundArtifact {
                    path: path.to_path_buf(),
                    kind: ArtifactKind::SmooshDir,
                },
                root: root.to_path_buf(),
                _staging: None,
            });
        }
        let index = path.join("index");
        if index.join("meta.smoosh").is_file() {
            return Ok(ResolvedArtifact {
                artifact: FoundArtifact {
                    path: index,
                    kind: ArtifactKind::SmooshDir,
                },
                root: root.to_path_buf(),
                _staging: None,
            });
        }
        return Err(format!(
            "loadSpec local.path {raw:?} resolved to directory {} which contains neither \
             `meta.smoosh` nor `index/meta.smoosh` — not a recognizable Druid segment layout",
            path.display()
        ));
    }
    Err(format!(
        "loadSpec local.path {raw:?} resolved to {} which is not a regular file or \
         directory",
        path.display()
    ))
}

// ---------------------------------------------------------------------------
// loadSpec s3_zip resolution (W-C)
// ---------------------------------------------------------------------------

/// Resolve an `s3_zip` loadSpec from an UNTRUSTED source row: validate
/// `bucket`/`key`, fetch the object into a fresh per-row staging
/// tempdir as `index.zip` (streamed, capped at
/// [`crate::s3_source::MAX_S3_OBJECT_BYTES`]), and hand back the SAME
/// `FoundArtifact{kind: IndexZip}` + tempdir-containment-root shape the
/// local path produces — the attach tail (zip caps, staged
/// materialize, v9 gate, hash, P→M) runs verbatim on it.
///
/// The client comes from the `AWS_*` environment
/// ([`crate::s3_source::s3_client_from_env`]); note the honest limit:
/// the row names its own bucket, so the operator's credentials read
/// whatever bucket the row names (see the module docs).
async fn resolve_s3_zip_artifact(bucket: &str, key: &str) -> Result<ResolvedArtifact, String> {
    if key.is_empty() {
        return Err("loadSpec s3_zip.key is empty".to_string());
    }
    if key.contains('\0') || key.chars().any(|c| c.is_ascii_control()) {
        return Err(format!(
            "loadSpec s3_zip.key {key:?} carries a NUL/control character — refused \
             (untrusted source row)"
        ));
    }
    // Bucket validation happens inside the client constructor too;
    // calling it explicitly keeps the refusal reason bucket-specific.
    let client = crate::s3_source::s3_client_from_env(bucket)
        .map_err(|e| format!("loadSpec s3_zip: {e}"))?;
    let staging = tempfile::tempdir()
        .map_err(|e| format!("failed to create the per-row s3 staging dir: {e}"))?;
    let root = staging
        .path()
        .canonicalize()
        .map_err(|e| format!("failed to canonicalize the per-row s3 staging dir: {e}"))?;
    let dest = root.join("index.zip");
    client
        .fetch_object_to_file(key, &dest, Some(crate::s3_source::MAX_S3_OBJECT_BYTES))
        .await
        .map_err(|e| format!("s3 fetch s3://{bucket}/{key} failed: {e}"))?;
    Ok(ResolvedArtifact {
        artifact: FoundArtifact {
            path: dest,
            kind: ArtifactKind::IndexZip,
        },
        root,
        _staging: Some(staging),
    })
}

// ---------------------------------------------------------------------------
// Source == target refusal
// ---------------------------------------------------------------------------

/// The hex value of a single ASCII hex digit, or `None`.
fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Percent-decode a URI component: a `%XX` triple (two hex digits)
/// becomes its byte; every other byte, and a lone or malformed `%`
/// (not followed by two hex digits), is passed through verbatim.  The
/// reassembled bytes are read as UTF-8 lossily.  sqlx percent-decodes
/// the host and database components when it connects, so the same-store
/// identity compare must decode them too — otherwise `%64ruid` and
/// `druid`, which open the SAME database, look different.
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (hex_val(b[i + 1]), hex_val(b[i + 2]))
        {
            out.push((h << 4) | l);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The canonical identity of a NETWORK metadata URI
/// (postgres/postgresql/mysql), for the same-store compare:
/// `(scheme family, host token, port, database)`.
///
/// The tuple is computed with libpq connection-URI precedence (the
/// semantics sqlx-postgres applies), because the same DATABASE can be
/// spelled many ways and a naive compare fails OPEN:
///
/// * `postgres` == `postgresql` (one `"pg"` family token).
/// * Query parameters `hostaddr` / `host` / `port` / `dbname` / `user`
///   (keys CASE-SENSITIVE — sqlx/libpq honor only the exact lowercase
///   spellings; other-case keys are ignored, matching the driver;
///   values percent-decoded) OVERRIDE the corresponding URI
///   authority/path components — so `.../decoy?dbname=druid` is the
///   `druid` database, not `decoy`.  Other query params (e.g.
///   `sslmode`) are ignored.
/// * Host = the effective CONNECTION target, `hostaddr` (the actual IP
///   libpq dials) → query `host` (auth/TLS-SNI only when `hostaddr` is
///   set) → authority host; then percent-decoded, lowercased, and the
///   loopback aliases `localhost` / `127.0.0.1` / `::1` / `[::1]`
///   collapse to one token.
/// * Port defaults to the family default (5432 pg, 3306 mysql) when
///   neither a query `port` nor an authority port is present.
/// * Database (PostgreSQL): query `dbname`, else the URI path segment
///   (leading `/`, trailing `/`, `#fragment` stripped) when non-empty,
///   else libpq's "dbname defaults to the (resolved) user".  A PG URI
///   with NO database at all yields `None` for the whole identity —
///   the compare must not invent a confident empty database.
/// * Database (MySQL): the URI path segment only (MySQL has no
///   default-to-user, and we do not rely on a `?database=` override) —
///   `None` when absent.  MySQL host/port stay the authority values.
///
/// A `None` identity (unparseable URI, unknown family, or an
/// undeterminable database) makes the caller fall back to the exact
/// string + SQLite-path canonicalize compares — it never false-refuses
/// and never introduces a false-allow.
///
/// This is a BEST-EFFORT footgun guard that matches sqlx/libpq for the
/// COMMON single-host forms; it deliberately does NOT emulate exotic
/// libpq connection features.  Enumerated residuals — two differently
/// spelled URIs for one database may NOT be equated when they use:
/// multi-host lists (`host=a,b,c` / `hostaddr=ip1,ip2`), a `service=`
/// service-file lookup, a `passfile`, a unix-socket `host=/path`
/// target, or a DNS hostname vs its resolved IP (this check does no
/// name resolution).  For those the operator must ensure `--source-uri`
/// ≠ `--metadata-uri`; the exact-string + SQLite-canonicalize +
/// normalized-tuple layers still catch the realistic accidental
/// flag-swap.
fn network_uri_identity(uri: &str) -> Option<(&'static str, String, u16, String)> {
    let uri = uri.trim();
    let (scheme, rest) = uri.split_once("://")?;
    let family = match scheme.to_ascii_lowercase().as_str() {
        "postgres" | "postgresql" => "pg",
        "mysql" => "mysql",
        _ => return None,
    };
    let default_port: u16 = if family == "pg" { 5432 } else { 3306 };

    // Split `authority[/path]` from the query string (everything after
    // the first `?`); a `#fragment` is not part of the query.
    let (before_query, query) = match rest.split_once('?') {
        Some((b, q)) => (b, Some(q.split('#').next().unwrap_or(""))),
        None => (rest, None),
    };
    let (authority, path) = before_query.split_once('/').unwrap_or((before_query, ""));

    // Query params: percent-decoded values, CASE-SENSITIVE keys — sqlx /
    // libpq honor ONLY the exact lowercase spellings `host`/`port`/
    // `dbname`/`user` as connection targets; any other-case spelling
    // (`DBNAME`, `Host`, …) is ignored by the driver, so we ignore it too
    // and fall back to the authority/path — matching what sqlx actually
    // connects to (otherwise an uppercase `DBNAME=decoy` would fool the
    // guard into believing source ≠ target while both open the same DB).
    let (mut q_hostaddr, mut q_host, mut q_port, mut q_dbname, mut q_user) =
        (None, None, None, None, None);
    if let Some(q) = query {
        for pair in q.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let val = percent_decode(v);
            match k {
                "hostaddr" => q_hostaddr = Some(val),
                "host" => q_host = Some(val),
                "port" => q_port = Some(val),
                "dbname" => q_dbname = Some(val),
                "user" => q_user = Some(val),
                _ => {}
            }
        }
    }

    // Userinfo `[user[:pass]@]` — take the substring after the LAST `@`
    // (a password may itself contain `@`); the user is before the first
    // `:` of the userinfo.
    let auth_user = match authority.rsplit_once('@') {
        Some((userinfo, _)) => {
            let u = userinfo.split_once(':').map_or(userinfo, |(u, _)| u);
            (!u.is_empty()).then(|| percent_decode(u))
        }
        None => None,
    };
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let (auth_host, auth_port): (String, Option<u16>) =
        if let Some(bracketed) = host_port.strip_prefix('[') {
            // IPv6 literal: `[addr]` or `[addr]:port`.
            let (addr, after) = bracketed.split_once(']')?;
            let port = match after.strip_prefix(':') {
                Some(p) => Some(p.parse::<u16>().ok()?),
                None if after.is_empty() => None,
                None => return None,
            };
            (addr.to_string(), port)
        } else if host_port.matches(':').count() >= 2 {
            // A bare (unbracketed) IPv6 literal — port-less by
            // definition, since host:port would be ambiguous.
            (host_port.to_string(), None)
        } else {
            match host_port.rsplit_once(':') {
                Some((h, p)) => (h.to_string(), Some(p.parse::<u16>().ok()?)),
                None => (host_port.to_string(), None),
            }
        };

    // Apply libpq precedence (query param OVERRIDES authority).  The
    // effective CONNECTION target is `hostaddr` (the actual IP libpq
    // dials) → `host` (auth/TLS-SNI only when hostaddr is set) →
    // authority host.
    let user = q_user.or(auth_user);
    let host_source = q_hostaddr
        .or(q_host)
        .unwrap_or_else(|| percent_decode(&auth_host));
    if host_source.is_empty() {
        return None;
    }
    let host_lower = host_source.to_ascii_lowercase();
    let host_token = match host_lower.as_str() {
        "localhost" | "127.0.0.1" | "::1" => "<loopback>".to_string(),
        _ => host_lower,
    };
    let port: u16 = match &q_port {
        Some(p) => p.parse::<u16>().ok()?,
        None => auth_port.unwrap_or(default_port),
    };

    // Database, with the family-specific default.
    let path_db = path
        .split('#')
        .next()
        .unwrap_or("")
        .trim_start_matches('/')
        .trim_end_matches('/');
    let path_db = (!path_db.is_empty()).then(|| percent_decode(path_db));
    let database: Option<String> = if family == "pg" {
        // dbname param > explicit path db > libpq's default-to-user.
        q_dbname.or(path_db).or(user)
    } else {
        // MySQL: no default-to-user; path segment only.
        path_db
    };
    let database = database?;
    Some((family, host_token, port, database))
}

/// Refuse a source URI that names the SAME database as the target
/// metadata URI — the nightmare case is swapped flags, where the
/// "read-only source" would be bootstrapped/written as the target.
///
/// Three layers: exact string compare, SQLite path canonicalization,
/// and a normalized network-URI identity compare
/// ([`network_uri_identity`]) that catches spelling variants of one
/// database (`postgres` vs `postgresql`, loopback aliases, absent
/// default port, and libpq query-param overrides `dbname` / `host` /
/// `port` / `user` — including a PG URI whose database defaults to the
/// username).  Documented residual: a DNS hostname vs its resolved IP
/// address, or two distinct DNS names for one host, are NOT equated —
/// this check does no name resolution.
fn refuse_same_store(source_uri: &str, target_uri: &str) -> Result<(), String> {
    let same_reason = || {
        format!(
            "refusing to run: --source-uri and --metadata-uri point at the SAME database \
             ({}) — the source Druid metadata DB is READ-ONLY input and must not be the \
             FerroDruid target store (were the two flags swapped?)",
            redact_metadata_uri(source_uri)
        )
    };
    if source_uri.trim() == target_uri.trim() {
        return Err(same_reason());
    }
    if let (Ok(MetadataUri::SqlitePath(a)), Ok(MetadataUri::SqlitePath(b))) = (
        parse_metadata_uri(source_uri),
        parse_metadata_uri(target_uri),
    ) {
        let canon = |p: &str| std::fs::canonicalize(p).unwrap_or_else(|_| PathBuf::from(p));
        if canon(&a) == canon(&b) {
            return Err(same_reason());
        }
    }
    if let (Some(a), Some(b)) = (
        network_uri_identity(source_uri),
        network_uri_identity(target_uri),
    ) && a == b
    {
        return Err(same_reason());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// Print the human-readable per-row report + summary.  Untrusted
/// strings (source ids, payload-derived ids, reasons, paths) are
/// sanitized against terminal-escape injection, same as `assess` /
/// `attach`.
fn print_report(params: &ImportParams, records: &[ImportRecord], source: &DruidSourceReport) {
    println!(
        "FerroDruid import-druid-metadata — Druid metadata-DB segment import (compat-7, \
         single-binary deployments)"
    );
    println!(
        "source Druid metadata DB: {} (READ-ONLY)",
        assess::sanitize(&redact_metadata_uri(&params.source_uri))
    );
    match &params.deep_storage {
        Some(base) => println!(
            "loadSpec path base:       {} (--deep-storage)",
            assess::sanitize(&base.display().to_string())
        ),
        None => println!("loadSpec path base:       (none — loadSpec paths used as-is)"),
    }
    println!(
        "ferro deep-storage:       {}",
        assess::sanitize(&params.ferro_deep_storage.display().to_string())
    );
    println!(
        "target metadata store:    {}",
        assess::sanitize(&redact_metadata_uri(&params.metadata_uri))
    );
    println!(
        "NOTE: only rows with used = true are imported — segments Druid marked unused \
         stay unimported (unlike `attach`, which imports every artifact found on disk)."
    );
    if params.dry_run {
        println!("DRY RUN — nothing was written.");
    }
    println!();

    if records.is_empty() {
        println!(
            "No used = true segment rows matched in the source druid_segments table{}.",
            params
                .datasource
                .as_deref()
                .map(|ds| format!(" for dataSource {}", assess::sanitize(ds)))
                .unwrap_or_default()
        );
    }

    let mut imported = 0usize;
    let mut would_import = 0usize;
    let mut skipped_exists = 0usize;
    let mut skipped_loadspec = 0usize;
    let mut failed = 0usize;
    for r in records {
        let id = r
            .ferro_id
            .as_deref()
            .map_or_else(|| "-".to_string(), assess::sanitize);
        match &r.status {
            ImportStatus::Imported { rows, replaced } => {
                imported += 1;
                let suffix = if *replaced {
                    " (replaced existing — --force)"
                } else {
                    ""
                };
                println!("IMPORTED      {rows:>9} rows  {id}{suffix}");
            }
            ImportStatus::WouldImport { rows } => {
                would_import += 1;
                println!("WOULD-IMPORT  {rows:>9} rows  {id}");
            }
            ImportStatus::SkippedExists => {
                skipped_exists += 1;
                println!(
                    "SKIP-EXISTS           -       {id} — a segment with this id is \
                     already in the metadata store (use --force to replace)"
                );
            }
            ImportStatus::SkippedLoadSpec { reason } => {
                skipped_loadspec += 1;
                println!(
                    "SKIP-LOADSPEC         -       source row {}",
                    assess::sanitize(&r.source_id)
                );
                println!("              reason: {}", assess::sanitize(reason));
            }
            ImportStatus::Failed { reason } => {
                failed += 1;
                println!("FAILED                -       {id}");
                println!(
                    "              source row: {}",
                    assess::sanitize(&r.source_id)
                );
                if let Some(artifact) = &r.artifact_path {
                    println!(
                        "              artifact: {}",
                        assess::sanitize(&artifact.display().to_string())
                    );
                }
                println!("              reason: {}", assess::sanitize(reason));
            }
        }
        if matches!(
            r.status,
            ImportStatus::Imported { .. } | ImportStatus::WouldImport { .. }
        ) {
            println!(
                "              source row: {}",
                assess::sanitize(&r.source_id)
            );
            if !r.dropped_columns.is_empty() {
                // LOUD per-row manifest of what was lost under
                // --allow-unreadable-columns — never silent.
                println!(
                    "              dropped {} unreadable column(s): [{}] — NOT imported; \
                     a query naming them behaves as for a column that never existed",
                    r.dropped_columns.len(),
                    assess::sanitize(&r.dropped_columns.join(", "))
                );
            }
        }
    }

    println!();
    match source.rules_found {
        Some(n) => {
            println!("found {n} rule row(s) in druid_rules — NOT imported (out of compat-7 scope)")
        }
        None => println!("druid_rules: table not found in the source (nothing to count)"),
    }
    match source.supervisors_found {
        Some(n) => println!(
            "found {n} supervisor row(s) in druid_supervisors — NOT imported (out of \
             compat-7 scope)"
        ),
        None => println!("druid_supervisors: table not found in the source (nothing to count)"),
    }
    println!();
    println!(
        "Summary: {imported} imported, {would_import} would import (dry run), \
         {skipped_exists} skipped (already present), {skipped_loadspec} skipped \
         (unsupported loadSpec), {failed} failed, {} source row(s) processed.",
        records.len(),
    );
    if failed > 0 {
        println!(
            "WARNING: {failed} row(s) failed and were NOT imported (see reasons above) — \
             successes are unaffected; fix the cause and re-run (already-imported ids \
             are skipped)."
        );
    }
    if source.truncated {
        println!(
            "NOTE: the source read stopped at --max-segments; more used rows exist and \
             were not imported."
        );
    }
    if imported > 0 {
        println!(
            "Next step: restart the FerroDruid instance (`ferrodruid serve`) — its \
             startup bootstrap reload downloads, hash-verifies, and loads every \
             imported segment; it is then query-visible."
        );
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- partition synthesis --------------------------------------------------

    #[test]
    fn partition_from_shard_spec_variants() {
        assert_eq!(partition_from(None), "0", "absent shardSpec defaults to 0");
        assert_eq!(partition_from(Some(&ShardingSpec::None {})), "0");
        assert_eq!(
            partition_from(Some(&ShardingSpec::Numbered {
                partition_num: 3,
                partitions: 8,
            })),
            "3"
        );
        assert_eq!(
            partition_from(Some(&ShardingSpec::Hashed {
                partition_num: 2,
                partitions: 4,
                partition_dimensions: None,
            })),
            "2"
        );
        assert_eq!(
            partition_from(Some(&ShardingSpec::Single { partition_num: 0 })),
            "0"
        );
        assert_eq!(
            partition_from(Some(&ShardingSpec::Linear { partition_num: 7 })),
            "7"
        );
        // A negative partition is passed through verbatim so the attach
        // tail's H5 validation rejects it loudly (never silently fixed).
        assert_eq!(
            partition_from(Some(&ShardingSpec::Linear { partition_num: -1 })),
            "-1"
        );
        // `numbered_overwrite` uses `partitionId`; `range` uses
        // `partitionNum` — both must yield the partition number.
        assert_eq!(
            partition_from(Some(&ShardingSpec::NumberedOverwrite {
                partition_id: 32768,
            })),
            "32768"
        );
        assert_eq!(
            partition_from(Some(&ShardingSpec::Range { partition_num: 5 })),
            "5"
        );
    }

    // -- loadSpec type names --------------------------------------------------

    #[test]
    fn load_spec_type_names_are_the_wire_names() {
        assert_eq!(
            load_spec_type_name(&LoadSpec::Local { path: "x".into() }),
            "local"
        );
        assert_eq!(
            load_spec_type_name(&LoadSpec::S3Zip {
                bucket: "b".into(),
                key: "k".into(),
            }),
            "s3_zip"
        );
        assert_eq!(
            load_spec_type_name(&LoadSpec::Hdfs { path: "p".into() }),
            "hdfs"
        );
        assert_eq!(
            load_spec_type_name(&LoadSpec::Google {
                bucket: "b".into(),
                path: "p".into(),
            }),
            "google"
        );
        assert_eq!(
            load_spec_type_name(&LoadSpec::Azure {
                container: "c".into(),
                blob: "b".into(),
            }),
            "azure"
        );
    }

    // -- resolve_local_artifact -----------------------------------------------

    #[test]
    fn resolve_rejects_traversal_relative_without_base_and_nul() {
        let err = resolve_local_artifact("/a/../b/index.zip", None).expect_err("`..` refused");
        assert!(err.contains(".."), "reason names the traversal: {err}");
        let err = resolve_local_artifact("rel/index.zip", None).expect_err("relative refused");
        assert!(err.contains("--deep-storage"), "hints at the remap: {err}");
        assert!(resolve_local_artifact("", None).is_err());
        assert!(resolve_local_artifact("/a/\0/x", None).is_err());
        let err = resolve_local_artifact("/a/./b", None).expect_err("`.` refused");
        assert!(err.contains('.'), "reason names the component: {err}");
    }

    #[test]
    fn resolve_absolute_existing_file_under_base_is_index_zip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let zip = dir.path().join("index.zip");
        std::fs::write(&zip, b"not really a zip").expect("write");
        let resolved = resolve_local_artifact(zip.to_str().expect("utf8"), Some(dir.path()))
            .expect("absolute path under the base resolves");
        assert_eq!(resolved.artifact.path, zip);
        assert_eq!(resolved.artifact.kind, ArtifactKind::IndexZip);
        assert_eq!(resolved.root, dir.path());
    }

    #[test]
    fn resolve_absolute_without_deep_storage_is_refused() {
        // An EXISTING absolute file: without --deep-storage there is no
        // containment ceiling, so it must be refused even though it
        // exists (the /etc/shadow class — an untrusted DB must never
        // name an arbitrary readable file).
        let dir = tempfile::tempdir().expect("tempdir");
        let zip = dir.path().join("index.zip");
        std::fs::write(&zip, b"z").expect("write");
        let err = resolve_local_artifact(zip.to_str().expect("utf8"), None)
            .expect_err("absolute without --deep-storage refused");
        assert!(err.contains("--deep-storage"), "hints at the flag: {err}");
        // The missing-path spelling is refused too.
        let err = resolve_local_artifact("/some/abs/index.zip", None)
            .expect_err("absolute without --deep-storage refused (missing path)");
        assert!(err.contains("--deep-storage"), "hints at the flag: {err}");
    }

    #[test]
    fn resolve_absolute_path_outside_deep_storage_base_is_refused() {
        let base = tempfile::tempdir().expect("base");
        let seg = base.path().join("seg");
        std::fs::create_dir_all(&seg).expect("mkdir");
        std::fs::write(seg.join("index.zip"), b"z").expect("write");
        // A real, readable file OUTSIDE the base: must NOT be read.
        let outside = tempfile::tempdir().expect("outside");
        let ext = outside.path().join("index.zip");
        std::fs::write(&ext, b"z").expect("write");
        let err = resolve_local_artifact(ext.to_str().expect("utf8"), Some(base.path()))
            .expect_err("an existing file OUTSIDE --deep-storage must never be resolved");
        assert!(err.contains("--deep-storage"), "names the ceiling: {err}");
    }

    #[test]
    fn resolve_absolute_path_inside_deep_storage_base_is_used() {
        let base = tempfile::tempdir().expect("base");
        let seg = base.path().join("seg");
        std::fs::create_dir_all(&seg).expect("mkdir");
        let zip = seg.join("index.zip");
        std::fs::write(&zip, b"z").expect("write");
        let resolved = resolve_local_artifact(zip.to_str().expect("utf8"), Some(base.path()))
            .expect("absolute under base resolves as-is");
        assert_eq!(resolved.artifact.path, zip);
        assert_eq!(resolved.artifact.kind, ArtifactKind::IndexZip);
        assert_eq!(
            resolved.root,
            base.path(),
            "the containment root is ALWAYS the --deep-storage base, never \
             a path derived from the untrusted input"
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_rejects_intermediate_symlink_escape() {
        let base = tempfile::tempdir().expect("base");
        let outside = tempfile::tempdir().expect("outside");
        std::fs::write(outside.path().join("index.zip"), b"z").expect("write");
        std::os::unix::fs::symlink(outside.path(), base.path().join("link")).expect("symlink");
        // Relative form, joined under the base: the INTERMEDIATE `link`
        // component escapes the base — canonicalize containment must
        // catch it (a lexical check would not).
        let err = resolve_local_artifact("link/index.zip", Some(base.path()))
            .expect_err("intermediate symlink escaping the base must be refused");
        assert!(err.contains("--deep-storage"), "names the ceiling: {err}");
        // Absolute form of the same escape.
        let abs = base.path().join("link").join("index.zip");
        assert!(
            resolve_local_artifact(abs.to_str().expect("utf8"), Some(base.path())).is_err(),
            "absolute path through an escaping intermediate symlink must be refused"
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_pins_canonical_root_and_artifact_against_symlinked_base() {
        // The `--deep-storage` base is a SYMLINK to the real tree.  R8:
        // resolution must pin the CANONICAL (symlink-resolved) root and
        // artifact path, so an attacker who atomically repoints the
        // symlink between resolve and the attach tail's re-canonicalize
        // cannot swap the tree — no path we hand downstream routes
        // through the untrusted symlink.
        let real_a = tempfile::tempdir().expect("realA");
        let seg = real_a.path().join("seg");
        std::fs::create_dir_all(&seg).expect("mkdir seg");
        std::fs::write(seg.join("index.zip"), b"z").expect("write artifact");

        let holder = tempfile::tempdir().expect("holder");
        let linkroot = holder.path().join("linkroot");
        std::os::unix::fs::symlink(real_a.path(), &linkroot).expect("symlink linkroot -> realA");

        let resolved = resolve_local_artifact("seg/index.zip", Some(&linkroot))
            .expect("segment under the symlinked base resolves");

        let canonical_link = std::fs::canonicalize(&linkroot).expect("canonicalize linkroot");
        assert_eq!(
            resolved.root, canonical_link,
            "root must be the canonical real path, not the raw linkroot symlink"
        );
        assert!(
            !std::fs::symlink_metadata(&resolved.root)
                .expect("stat root")
                .file_type()
                .is_symlink(),
            "the pinned root must not itself be a symlink"
        );
        assert!(
            resolved.artifact.path.starts_with(&canonical_link),
            "artifact path must live under the canonical real root: {}",
            resolved.artifact.path.display()
        );
        assert!(
            !resolved
                .artifact
                .path
                .components()
                .any(|c| c.as_os_str() == "linkroot"),
            "artifact path must carry no `linkroot` (raw symlink) component: {}",
            resolved.artifact.path.display()
        );
        assert_eq!(resolved.artifact.kind, ArtifactKind::IndexZip);
    }

    #[test]
    fn resolve_dir_layouts_smoosh_and_index_subdir() {
        let dir = tempfile::tempdir().expect("tempdir");
        // <dir>/direct/meta.smoosh
        let direct = dir.path().join("direct");
        std::fs::create_dir_all(&direct).expect("mkdir");
        std::fs::write(direct.join("meta.smoosh"), b"v1").expect("write");
        let resolved = resolve_local_artifact(direct.to_str().expect("utf8"), Some(dir.path()))
            .expect("direct dir");
        assert_eq!(resolved.artifact.kind, ArtifactKind::SmooshDir);
        assert_eq!(resolved.artifact.path, direct);

        // <dir>/nested/index/meta.smoosh — Druid 31 raw layout.
        let nested = dir.path().join("nested");
        std::fs::create_dir_all(nested.join("index")).expect("mkdir");
        std::fs::write(nested.join("index").join("meta.smoosh"), b"v1").expect("write");
        let resolved = resolve_local_artifact(nested.to_str().expect("utf8"), Some(dir.path()))
            .expect("nested dir");
        assert_eq!(resolved.artifact.kind, ArtifactKind::SmooshDir);
        assert_eq!(resolved.artifact.path, nested.join("index"));

        // A dir with neither is a LOUD failure, not a silent pass-through.
        let empty = dir.path().join("empty");
        std::fs::create_dir_all(&empty).expect("mkdir");
        let err = resolve_local_artifact(empty.to_str().expect("utf8"), Some(dir.path()))
            .expect_err("no smoosh layout");
        assert!(
            err.contains("meta.smoosh"),
            "reason names the layout: {err}"
        );
    }

    #[test]
    fn resolve_remaps_foreign_absolute_path_by_longest_suffix() {
        let base = tempfile::tempdir().expect("tempdir");
        // The blob lives at <base>/wiki/iv/v1/0/index.zip; the source DB
        // recorded the Druid host's /opt/druid/var/segments prefix.
        let part = base.path().join("wiki").join("iv").join("v1").join("0");
        std::fs::create_dir_all(&part).expect("mkdir");
        std::fs::write(part.join("index.zip"), b"z").expect("write");
        let raw = "/opt/druid/var/segments/wiki/iv/v1/0/index.zip";
        let resolved =
            resolve_local_artifact(raw, Some(base.path())).expect("suffix remap resolves");
        assert_eq!(resolved.artifact.path, part.join("index.zip"));
        assert_eq!(resolved.artifact.kind, ArtifactKind::IndexZip);
        assert_eq!(
            resolved.root,
            base.path(),
            "containment root is the remap base"
        );

        // A relative path joins under the base directly.
        let resolved = resolve_local_artifact("wiki/iv/v1/0/index.zip", Some(base.path()))
            .expect("relative under base");
        assert_eq!(resolved.artifact.path, part.join("index.zip"));

        // Nothing matching anywhere: loud, names both probes.
        let err = resolve_local_artifact("/nope/missing/index.zip", Some(base.path()))
            .expect_err("missing everywhere");
        assert!(
            err.contains("--deep-storage") && err.contains("suffix"),
            "reason explains the probing: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_refuses_symlink_final_component() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real = dir.path().join("real.zip");
        std::fs::write(&real, b"z").expect("write");
        let link = dir.path().join("link.zip");
        std::os::unix::fs::symlink(&real, &link).expect("symlink");
        let err = resolve_local_artifact(link.to_str().expect("utf8"), Some(dir.path()))
            .expect_err("symlink refused");
        assert!(err.contains("symlink"), "reason names the symlink: {err}");
    }

    // -- source == target refusal ---------------------------------------------

    #[test]
    fn refuse_same_store_catches_spelling_variants() {
        // Identical strings.
        assert!(refuse_same_store("postgres://h/db", "postgres://h/db").is_err());
        // SQLite path vs sqlite:// spelling of the SAME file.
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("meta.db");
        std::fs::write(&db, b"").expect("touch");
        let bare = db.to_str().expect("utf8").to_string();
        let uri = format!("sqlite://{bare}");
        assert!(refuse_same_store(&uri, &bare).is_err());
        assert!(refuse_same_store(&bare, &uri).is_err());
        // Distinct databases pass.
        assert!(refuse_same_store("postgres://h/druid", "postgres://h/ferro").is_ok());
        let other = dir.path().join("other.db");
        assert!(refuse_same_store(&bare, other.to_str().expect("utf8")).is_ok());
        // Genuinely different hosts pass; so do different scheme
        // families on one host.
        assert!(refuse_same_store("postgres://h1/db", "postgres://h2/db").is_ok());
        assert!(refuse_same_store("mysql://h/db", "postgres://h/db").is_ok());
    }

    #[test]
    fn refuse_same_store_normalizes_scheme_and_loopback() {
        // postgres == postgresql; localhost == 127.0.0.1 == ::1; the
        // default pg port 5432 is injected when absent.
        assert!(
            refuse_same_store(
                "postgres://u@localhost:5432/db",
                "postgresql://u2@127.0.0.1/db"
            )
            .is_err(),
            "same DB behind scheme/loopback spelling variants must be refused"
        );
        assert!(
            refuse_same_store("postgres://[::1]:5432/db", "postgresql://localhost/db").is_err(),
            "IPv6 loopback is the same loopback"
        );
    }

    #[test]
    fn refuse_same_store_injects_default_port() {
        assert!(
            refuse_same_store("mysql://h/db", "mysql://h:3306/db").is_err(),
            "absent port must compare equal to the family default"
        );
        // A NON-default explicit port really is a different endpoint.
        assert!(refuse_same_store("postgres://h:5433/db", "postgres://h/db").is_ok());
    }

    #[test]
    fn refuse_same_store_ignores_userinfo_and_non_target_query() {
        // A URI WITH an explicit path database ignores the user (and a
        // non-target query param like `sslmode`) for identity: both name
        // database `db` on host `h`.
        assert!(
            refuse_same_store("postgres://a:pw@h/db?sslmode=require", "postgres://b@h/db").is_err(),
            "same explicit DB via different user/sslmode is still the same DB"
        );
    }

    #[test]
    fn refuse_same_store_decodes_percent_encoded_db() {
        // sqlx percent-decodes the database name, so `%64ruid` opens the
        // SAME database as `druid` — the guard must fold them together.
        assert!(
            refuse_same_store(
                "postgres://u:p@localhost/%64ruid",
                "postgresql://u2@127.0.0.1/druid"
            )
            .is_err(),
            "percent-encoded database name must decode to the same identity"
        );
        // But two genuinely different names stay different after decoding.
        assert!(
            refuse_same_store("postgres://h/%64ruidA", "postgres://h/druidB").is_ok(),
            "distinct databases remain distinct after percent-decoding"
        );
    }

    #[test]
    fn refuse_same_store_honors_pg_dbname_query_param() {
        // libpq: `?dbname=` OVERRIDES the path — both open database
        // `druid`, so `--force` on the target could clobber the source.
        assert!(
            refuse_same_store("postgres://h/decoy?dbname=druid", "postgres://h/druid").is_err(),
            "?dbname= overrides the path database — must be refused"
        );
        // `?host=` is the same class of override.
        assert!(
            refuse_same_store(
                "postgres://decoyhost/db?host=realhost",
                "postgres://realhost/db"
            )
            .is_err(),
            "?host= overrides the authority host — must be refused"
        );
        // The override value is percent-decoded too (`%64ruid` = druid).
        assert!(
            refuse_same_store("postgres://h/x?dbname=%64ruid", "postgres://h/druid").is_err(),
            "a percent-encoded ?dbname= must decode before comparing"
        );
        // A genuinely different ?dbname= stays distinct.
        assert!(
            refuse_same_store("postgres://h/x?dbname=alpha", "postgres://h/x?dbname=beta").is_ok(),
            "distinct ?dbname= targets remain distinct"
        );
    }

    #[test]
    fn refuse_same_store_ignores_uppercase_query_keys() {
        // sqlx/libpq recognize ONLY the exact-lowercase keys — an
        // uppercase `DBNAME` is IGNORED by the driver, so both URIs open
        // the path database `db`; the guard must NOT read `decoy` and
        // wrongly allow overwriting the source.
        assert!(
            refuse_same_store("postgres://h/db?DBNAME=decoy", "postgres://h/db").is_err(),
            "uppercase DBNAME is ignored → both open `db` → must be refused"
        );
        // Mixed-case is ignored too.
        assert!(
            refuse_same_store("postgres://h/db?DbName=decoy", "postgres://h/db").is_err(),
            "mixed-case DbName is ignored → both open `db` → must be refused"
        );
        // An uppercase HOST is ignored, so the source host stays
        // `decoyhost` ≠ `realhost` — sqlx would connect to DIFFERENT
        // hosts, so this is genuinely distinct and must be allowed.
        assert!(
            refuse_same_store(
                "postgres://decoyhost/db?HOST=realhost",
                "postgres://realhost/db"
            )
            .is_ok(),
            "uppercase HOST is ignored → distinct hosts → must be allowed"
        );
    }

    #[test]
    fn refuse_same_store_honors_hostaddr_override() {
        // libpq: `hostaddr` is the ACTUAL IP dialed; `host` becomes
        // auth/TLS-SNI only.  So `decoy?hostaddr=127.0.0.1` connects to
        // loopback `db` — the SAME store as `localhost/db`.
        assert!(
            refuse_same_store(
                "postgres://u@decoy/db?hostaddr=127.0.0.1",
                "postgres://u@localhost/db"
            )
            .is_err(),
            "?hostaddr= is the connection target (loopback) — must be refused"
        );
        // Same explicit IP + db via hostaddr vs authority host.
        assert!(
            refuse_same_store(
                "postgres://u@decoy/db?hostaddr=10.0.0.5",
                "postgres://u@10.0.0.5/db"
            )
            .is_err(),
            "hostaddr IP equals the other's authority host IP + db → same store"
        );
    }

    #[test]
    fn refuse_same_store_hostaddr_distinct_still_ok() {
        // Different hostaddr IPs → genuinely different endpoints.
        assert!(
            refuse_same_store(
                "postgres://u@h1/db?hostaddr=10.0.0.5",
                "postgres://u@h2/db?hostaddr=10.0.0.6"
            )
            .is_ok(),
            "distinct hostaddr IPs are distinct stores"
        );
        // Case-SENSITIVE: an uppercase `HOSTADDR` is ignored by sqlx, so
        // the source host stays `decoy` ≠ loopback → distinct → allowed.
        assert!(
            refuse_same_store(
                "postgres://u@decoy/db?HOSTADDR=127.0.0.1",
                "postgres://u@localhost/db"
            )
            .is_ok(),
            "uppercase HOSTADDR is ignored → source host stays `decoy` → must be allowed"
        );
    }

    #[test]
    fn refuse_same_store_pg_missing_db_defaults_to_user() {
        // No path and no `?dbname=`: PostgreSQL defaults the database to
        // the username, so alice@h and bob@h are DIFFERENT stores — a
        // legitimate migration that must NOT be blocked.
        assert!(
            refuse_same_store("postgres://alice@h", "postgres://bob@h").is_ok(),
            "PG default-to-user makes these distinct databases (alice vs bob)"
        );
        // Same user, loopback-folded host → the same defaulted database.
        assert!(
            refuse_same_store("postgres://alice@localhost", "postgres://alice@127.0.0.1").is_err(),
            "same defaulted database (alice) on the same loopback host is the same store"
        );
        // A wholly databaseless PG URI (no path, no user) is unknown →
        // identity None → falls back to string compare (distinct here).
        assert!(
            refuse_same_store("postgres://h1", "postgres://h2").is_ok(),
            "databaseless PG URIs fall back to string compare, not a confident empty DB"
        );
    }
}
