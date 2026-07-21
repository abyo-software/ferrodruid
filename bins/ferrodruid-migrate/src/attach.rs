// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `ferrodruid-migrate attach` — import existing Apache Druid **v9**
//! segments from a Druid deep-storage tree — a **local directory** or
//! an **`s3://bucket/prefix`** source (W-C) — into a FerroDruid
//! single-binary deployment (compat-2).
//!
//! An s3 source is LISTED and STAGED into a private local tempdir
//! first (`crate::s3_source::stage_s3_tree`, preserving relative key
//! paths, `--datasource`/`--max-segments` applied at listing time
//! where possible); the identical scan/identity/P→M pipeline below
//! then runs over the staging root, so every cap and containment
//! gate is inherited unchanged.
//!
//! The importer is offline and per-segment fail-safe.  For every
//! artifact the `assess` scanner finds (same recognized layouts:
//! `index.zip` or raw smoosh dirs), it runs a **P → M** sequence — the
//! same crash-consistency order as the live publish tails:
//!
//! 1. **identify + verify**: identity from `descriptor.json` /
//!    the `<dataSource>/<interval>/<version>/<partitionNum>` path
//!    (shared `assess` code; a PRESENT-but-broken descriptor is a loud
//!    per-segment failure, never a silent path fallback (H4), and the
//!    interval/partition/version fields are validated, not trusted
//!    (H5)), then the real v9 reader must open the STAGED file set —
//!    zips are extracted (same zip-slip / zip-bomb caps as `assess`)
//!    and raw smoosh dirs are copied into a private staging dir, so
//!    the read gate, the hash, and the upload all see one immutable
//!    byte set (H1);
//! 2. **P (persist)**: the staged file set is content-hashed
//!    ([`ferrodruid_deep_storage::blob_content_hash`]) and uploaded to
//!    the FerroDruid deep-storage layout
//!    (`<ferro-base>/<dataSource>/<segmentId>/`);
//! 3. **M (metadata)**: only after the durable upload is a `used =
//!    TRUE` metadata row committed, its `payload.loadSpec` carrying
//!    `{type, dataSource, segmentId, sha256}` — the shape the live
//!    publish tails stamp and the bootstrap reload verifies.
//!
//! Nothing is eagerly loaded: on the next `ferrodruid serve` startup
//! the existing bootstrap reload (`Overlord::bootstrap_reload_segments`)
//! downloads every used row's blob, re-verifies the sha256, and loads
//! it — the attached segment then becomes query-visible.
//!
//! ## Crash consistency
//!
//! P→M per segment means a failure or crash can only leave (a) nothing,
//! or (b) an uploaded blob with **no** metadata row — a harmless orphan
//! the bootstrap reload ignores and a re-run replaces.  The reverse
//! (a `loadSpec` row without its blob) would abort the next startup
//! fail-loud (H4) and is unreachable from this tool's ordering.  The
//! one exception is `--force` replace: the re-upload REPLACES the
//! existing blob **in place**, so from the first replaced byte until
//! the row update a crash — or a failed upload — leaves the OLD row
//! referencing partially/fully replaced bytes.  The row is never
//! touched unless the upload completed (an upload failure reports
//! loudly and leaves the row as-is), the next startup rejects the
//! mismatched blob loudly on the hash check, and re-running
//! `attach --force` converges.
//!
//! ## Honest limitations
//!
//! * **Single-binary deployments only** (`ferrodruid serve`).  The
//!   distributed role binaries load segments over the historical RPC
//!   path, which does not serve attached v9 blobs — out of scope here.
//! * **Sources: local directory or `s3://bucket/prefix`** (the s3 tree
//!   is downloaded to local staging first — network + staging disk for
//!   the whole prefix, and a `--dry-run` still downloads).  The TARGET
//!   FerroDruid deep storage stays **local**.  For HDFS/GCS/Azure
//!   Druid deep storage, pull the segments to a local dir first.
//! * **Unsupported encodings are un-attachable**: the verdict is the v9
//!   reader's own; segments it cannot open are skipped loudly with the
//!   reader's reason (run `assess` first for a full readability map).
//!   OPT-IN exception: `--allow-unreadable-columns` drops JUST the
//!   undecodable column(s) (e.g. sketch/complex columns FerroDruid
//!   cannot read yet) and attaches the rest — the dropped names are
//!   reported loudly per segment and recorded in the row's
//!   `payload.droppedUnreadableColumns`, the pruned segment is
//!   RE-WRITTEN so the blob genuinely lacks them, and a segment whose
//!   `__time` or every dimension is unreadable still fails loudly.
//!   Default OFF: strict, byte-identical to the historical behavior.
//! * **Interval authority**: the metadata row's start/end come from the
//!   descriptor (or path) interval.  FerroDruid prunes queries by the
//!   segment's actual `__time` values, so a descriptor interval that
//!   disagrees with the data does not corrupt results, but the
//!   datasource's metadata listing will show the descriptor interval.
//! * **No Druid metadata-DB reader**: identity comes from
//!   `descriptor.json` / the path layout, not from Druid's metadata
//!   store, so segments Druid marked unused are attached too if their
//!   blobs are still in deep storage (filter with `--datasource` or
//!   prune the input dir first).
//! * **Run offline**: the tool writes the same SQLite store `serve`
//!   uses; concurrent writers are unsupported (stop the instance, or
//!   attach before starting it).
//! * **Symlink containment residual**: every source open is `O_NOFOLLOW`
//!   plus a canonical resolves-under-root re-check (compat-2 R4 H2), so
//!   an ancestor-dir symlink swap cannot pull EXTERNAL bytes into the
//!   staged/hashed/uploaded set — but a WITHIN-TREE swap and the exact
//!   check-to-read race window remain open without
//!   `openat2(RESOLVE_NO_SYMLINKS)` / cap-std (follow-up); see the
//!   `assess` module's honest limitations.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use ferrodruid_deep_storage::{
    DeepStorage, LocalDeepStorage, blob_content_hash, create_dir_all_durable,
};
use ferrodruid_metadata::{
    MetadataStore, MetadataUri, SegmentMetadataRow, parse_metadata_uri, redact_metadata_uri,
};
use ferrodruid_segment::{SegmentData, write_segment_v9};

use crate::assess::{self, FoundArtifact, IdentitySource, ScanResult, SegmentIdentity};

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// Everything the `attach` subcommand needs, as parsed by `main`.
pub(crate) struct AttachParams {
    /// Druid local deep-storage root to import from.
    pub(crate) deep_storage: PathBuf,
    /// FerroDruid deep-storage base directory to import into
    /// (`<data_dir>/deep-storage` of the target instance).
    pub(crate) ferro_deep_storage: PathBuf,
    /// Metadata store path or URI (`<data_dir>/metadata/ferrodruid.db`,
    /// `sqlite://<path>`, `postgres://…`, or `mysql://…`).
    pub(crate) metadata_uri: String,
    /// Only attach segments of this Druid dataSource.
    pub(crate) datasource: Option<String>,
    /// Report without writing anything.
    pub(crate) dry_run: bool,
    /// Replace a segment already present under the same id.
    pub(crate) force: bool,
    /// Attach at most N artifacts.
    pub(crate) max_segments: Option<usize>,
    /// OPT-IN lossy mode (`--allow-unreadable-columns`): open the source
    /// segment with the LENIENT v9 reader, dropping any column whose
    /// decode fails (e.g. a sketch/complex column FerroDruid cannot read
    /// yet) instead of failing the whole segment.  Default OFF = strict,
    /// byte-identical to the historical behavior.
    pub(crate) allow_unreadable_columns: bool,
}

// ---------------------------------------------------------------------------
// Per-segment outcome model
// ---------------------------------------------------------------------------

/// Outcome of processing one artifact.  `pub(crate)`: the compat-7
/// metadata-DB importer (`crate::import_druid`) reuses the attach tail
/// verbatim and converts this outcome into its own report lines.
pub(crate) enum AttachStatus {
    /// P→M completed: blob uploaded and metadata row committed.
    Attached {
        /// Row count reported by the v9 reader.
        rows: usize,
        /// An existing row/blob was replaced (`--force`).
        replaced: bool,
    },
    /// Dry run: every gate passed; nothing was written.
    WouldAttach {
        /// Row count reported by the v9 reader.
        rows: usize,
    },
    /// A row with the same segment id already exists (no `--force`).
    SkippedExists,
    /// Loud per-segment failure; nothing committed for this segment
    /// (see [`AttachStatus::Failed::reason`]).
    Failed {
        /// Human-readable reason, verbatim from the failing layer.
        reason: String,
    },
}

/// One processed artifact, for the report (fields `pub(crate)` for the
/// compat-7 importer's report conversion).
pub(crate) struct AttachRecord {
    /// Artifact path (the `index.zip` file or the smoosh dir).
    pub(crate) artifact_path: PathBuf,
    /// Synthesized ferro segment id, when identity was established.
    pub(crate) ferro_id: Option<String>,
    /// Where the identity came from.
    pub(crate) identity_source: IdentitySource,
    pub(crate) status: AttachStatus,
    /// Columns DROPPED because their decode failed — non-empty only
    /// under `--allow-unreadable-columns`, and reported LOUDLY per
    /// segment (also recorded in the metadata row's
    /// `payload.droppedUnreadableColumns`).
    pub(crate) dropped_columns: Vec<String>,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run the `attach` subcommand.
///
/// Returns `Err` only for operational failures (missing root, store or
/// deep-storage base unusable).  Per-segment failures are report
/// content — the successes stand on their own (each segment is an
/// independent P→M unit) — and the process exits 0 once the run
/// completed.
pub(crate) async fn run(mut params: AttachParams) -> Result<(), Box<dyn std::error::Error>> {
    // W-C: an `s3://bucket/prefix` source is LISTED + STAGED into a
    // private local tempdir (preserving relative key paths) BEFORE the
    // local-directory check; the existing scan/identity/attach tail
    // then runs over the staging root unchanged, inheriting every cap
    // and containment gate. The staging dir lives until the run ends.
    let source_display = params.deep_storage.display().to_string();
    let mut _s3_staging: Option<crate::s3_source::StagedS3Tree> = None;
    if let Some(parsed) = crate::s3_source::s3_url_of_path(&params.deep_storage) {
        let url = match parsed {
            Ok(u) => u,
            Err(e) => return Err(e.into()),
        };
        let staged = crate::s3_source::stage_s3_tree(
            &url,
            params.datasource.as_deref(),
            params.max_segments,
        )
        .await?;
        let mut note = format!(
            "staged {} object(s) ({} bytes) from {} into a private local staging dir",
            staged.objects_downloaded,
            staged.bytes_downloaded,
            url.display()
        );
        if staged.filtered_keys > 0 {
            note.push_str(&format!(
                " ({} key(s) filtered out by --datasource at listing time)",
                staged.filtered_keys
            ));
        }
        if staged.listing_truncated {
            note.push_str(" (listing stopped at the --max-segments bound)");
        }
        println!("{note}");
        params.deep_storage = staged.root.clone();
        _s3_staging = Some(staged);
    }

    let root = params.deep_storage.clone();
    if !root.is_dir() {
        return Err(format!(
            "deep-storage path {} is not a directory (a local directory or \
             s3://bucket/prefix; for HDFS/GCS/Azure pull the segments to a local \
             dir first)",
            root.display()
        )
        .into());
    }

    let scan_result = assess::scan(&root, params.max_segments)?;

    // The metadata store. The URI scheme is checked FIRST (postgres:// /
    // mysql:// / sqlite:// dispatch — an unknown scheme fails loudly
    // instead of becoming a SQLite file named after the URI); a bare
    // path keeps the historical SQLite behavior.
    //
    // A DRY RUN must write NOTHING to the target — no file creation and
    // no `initialize()` DDL, which would CREATE TABLEs inside an
    // EXISTING SQLite file (even a foreign/uninitialized one, e.g. a
    // mistyped --metadata-uri) and take write locks against a live
    // `serve` store.  So under --dry-run the store is only ever opened
    // READ-ONLY and probed for whether it is already initialized (for
    // accurate collision/skip reporting); an absent or uninitialized
    // target is simply `None` (every artifact reports as WOULD-ATTACH).
    // Same discipline as `import-druid-metadata --dry-run`.  A real run
    // creates + initializes the store like `serve` does.
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
        // Make the deep-storage base itself durable BEFORE any upload:
        // `upload_segment`'s fsync chain stops at (and assumes) a
        // durable root — the same H7/H2 discipline as the `serve`
        // startup's `prepare_data_dirs`.
        create_dir_all_durable(&params.ferro_deep_storage).await?;
    }

    let mut records: Vec<AttachRecord> = Vec::new();
    let mut filtered = 0usize;
    // ferro_id → the STRUCTURED identity already attached (or planned, in
    // a dry run) by THIS run.  Keyed by the joined id but VALUED by the
    // full identity so a duplicate artifact set (a colocated zip + raw dir
    // of the same segment) is de-duplicated, while two DISTINCT identities
    // that collide onto one non-injective ferro_id (R7) are caught.
    let mut seen: HashMap<String, SegmentIdent> = HashMap::new();

    for artifact in &scan_result.found {
        let identity = assess::derive_identity(&root, artifact);
        // The --datasource filter only applies when the identity is
        // known; an identity-less artifact falls through to the loud
        // identity-incomplete failure instead of being silently hidden.
        if let (Some(filter), Some(ds)) = (
            params.datasource.as_deref(),
            identity.data_source.as_deref(),
        ) && ds != filter
        {
            filtered += 1;
            continue;
        }
        let record = attach_one(
            artifact,
            &identity,
            store.as_ref(),
            &storage,
            &params,
            &mut seen,
        )
        .await;
        records.push(record);
    }

    print_report(&ReportInput {
        params: &params,
        source: &source_display,
        records: &records,
        filtered,
        scan: &scan_result,
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-segment P→M
// ---------------------------------------------------------------------------

/// Process one artifact through identify → verify → hash → P (upload)
/// → M (metadata row).  Never propagates a failure: every failure
/// becomes a loud [`AttachStatus::Failed`] and nothing is committed for
/// the segment (P→M order).
///
/// `pub(crate)`: this IS the compat-2 P→M tail, reused verbatim by the
/// compat-7 metadata-DB importer (`crate::import_druid`) — which hands
/// in a DB-synthesized identity ([`IdentitySource::MetadataDb`]) and a
/// per-segment `params.deep_storage` containment root, and shares one
/// `seen` map across its run for the R7 collision discipline.
pub(crate) async fn attach_one(
    artifact: &FoundArtifact,
    identity: &SegmentIdentity,
    store: Option<&MetadataStore>,
    storage: &LocalDeepStorage,
    params: &AttachParams,
    seen: &mut HashMap<String, SegmentIdent>,
) -> AttachRecord {
    let mut record = AttachRecord {
        artifact_path: artifact.path.clone(),
        ferro_id: None,
        identity_source: identity.source(),
        status: AttachStatus::Failed {
            reason: String::new(),
        },
        dropped_columns: Vec::new(),
    };
    let fail = |mut record: AttachRecord, reason: String| {
        record.status = AttachStatus::Failed { reason };
        record
    };

    // 0. A descriptor.json that EXISTS but cannot be used is loud (H4):
    //    the authoritative identity source being broken must never
    //    silently degrade to the path fallback — a hostile/corrupt
    //    descriptor could otherwise attach under a wrong identity.
    if let Some(err) = identity.descriptor_error.as_deref() {
        return fail(
            record,
            format!("identity failure — {err}; refusing the silent path-identity fallback"),
        );
    }

    // 1. Identity must be complete — attach never guesses an id.
    let (Some(ds), Some(interval), Some(version), Some(partition)) = (
        identity.data_source.as_deref(),
        identity.interval.as_deref(),
        identity.version.as_deref(),
        identity.partition.as_deref(),
    ) else {
        return fail(
            record,
            "segment identity incomplete — dataSource/interval/version/partition \
             must all come from a descriptor.json or the \
             <dataSource>/<interval>/<version>/<partitionNum> path layout"
                .to_string(),
        );
    };

    // 2. Validate the identity fields (H5) — they come from an untrusted
    //    descriptor / path layout and become metadata rows and ids, so
    //    they are parsed, not trusted:
    //    * interval → (start, end): descriptor slash form or path
    //      underscore form, both bounds parseable ISO-8601 instants with
    //      start < end;
    //    * partition: a non-negative integer, CANONICALIZED ("00" == "0")
    //      so one real partition cannot attach twice under two spellings;
    //    * version: non-empty.
    let (start, end) = match split_interval(interval) {
        Ok(parts) => parts,
        Err(reason) => return fail(record, reason),
    };
    let partition = match normalize_partition(partition) {
        Ok(p) => p,
        Err(reason) => return fail(record, reason),
    };
    if version.is_empty() {
        return fail(record, "segment version is empty".to_string());
    }

    // 3. ferro_id = the Druid segment id convention
    //    `<ds>_<start>_<end>_<version>_<partition>` — deterministic across
    //    re-runs (the partition is the canonical form, H5).  The joined
    //    string is NOT injective: `_` is the separator yet a `dataSource`
    //    or `version` may itself contain `_`, so two DISTINCT identities
    //    can map to one ferro_id (R7).  The id stays the Druid convention
    //    (the reload keys on it); the collision check below compares the
    //    STRUCTURED `incoming` identity, never the joined string alone.
    let incoming = SegmentIdent {
        data_source: ds.to_string(),
        start: start.clone(),
        end: end.clone(),
        version: version.to_string(),
        partition: partition.clone(),
    };
    let ferro_id = incoming.ferro_id();
    record.ferro_id = Some(ferro_id.clone());

    // 4. Path-component safety pre-check (mirrors the authoritative
    //    validation `upload_segment` re-applies): a hostile descriptor
    //    must not traverse out of the deep-storage base or the store.
    if let Err(reason) = check_path_component("data source", ds) {
        return fail(record, reason);
    }
    if let Err(reason) = check_path_component("segment id", &ferro_id) {
        return fail(record, reason);
    }

    // 5. Collision check (this run + the metadata store), comparing the
    //    STRUCTURED identity — never the non-injective joined id alone
    //    (R7).  A ferro_id already present under the SAME identity is a
    //    legitimate re-attach (idempotent skip, or a `--force` replace of
    //    the same segment).  A ferro_id present under a DIFFERENT identity
    //    is a true collision between two DISTINCT Druid segments: a loud
    //    refusal, never a silent skip or a `--force` overwrite of the
    //    wrong segment (which would lose a distinct segment).
    let exists = if let Some(prev) = seen.get(&ferro_id) {
        if *prev != incoming {
            return fail(record, ferro_id_collision_reason(&ferro_id));
        }
        true
    } else if let Some(store) = store {
        match store.get_segment(&ferro_id).await {
            Ok(Some(row)) => {
                if !row_matches_identity(&row, &incoming) {
                    return fail(record, ferro_id_collision_reason(&ferro_id));
                }
                true
            }
            Ok(None) => false,
            Err(e) => return fail(record, format!("metadata store lookup failed: {e}")),
        }
    } else {
        false
    };
    if exists && !params.force {
        record.status = AttachStatus::SkippedExists;
        return record;
    }

    // 6. Materialize into a PRIVATE staging dir (extract if zipped, COPY
    //    a raw smoosh dir; same caps as assess) and gate on the REAL v9
    //    reader — an unreadable/unsupported segment is un-attachable and
    //    skipped loudly with the reader's reason.  Staging (H1) pins ONE
    //    immutable byte set that the read gate, the content hash, AND
    //    the upload all see: hashing the live source in place would let
    //    a mutation between the hash and the upload commit a sha256 that
    //    does not match the uploaded bytes — bricking the next startup.
    let materialized = match assess::materialize_artifact_staged(&params.deep_storage, artifact) {
        Ok(m) => m,
        Err(reason) => return fail(record, reason),
    };
    let (rows, dropped_columns, lenient_segment) = if params.allow_unreadable_columns {
        // OPT-IN lenient gate: a column whose decode fails is DROPPED
        // (with a manifest) instead of failing the segment — but a
        // segment left with no queryable data is still a loud failure.
        match assess::open_segment_guarded_lenient(materialized.dir()) {
            Ok((segment, dropped)) => {
                if let Err(reason) = check_lenient_still_queryable(&segment, &dropped) {
                    return fail(record, reason);
                }
                let rows = segment.num_rows;
                // Keep the decoded segment only when a rewrite is needed.
                let rewrite = if dropped.is_empty() {
                    None
                } else {
                    Some(segment)
                };
                (rows, dropped, rewrite)
            }
            Err(reason) => {
                return fail(
                    record,
                    format!(
                        "not readable by the v9 reader (even with \
                         --allow-unreadable-columns): {reason}"
                    ),
                );
            }
        }
    } else {
        match assess::open_segment_guarded(materialized.dir()) {
            Ok(segment) => (segment.num_rows, Vec::new(), None),
            Err(reason) => {
                return fail(record, format!("not readable by the v9 reader: {reason}"));
            }
        }
    };
    record.dropped_columns = dropped_columns.clone();

    // 6b. Lenient REWRITE: when columns were dropped, the uploaded blob
    //     must NOT carry the undecodable bytes.  Two reasons: (a) the
    //     bootstrap reload opens blobs with the STRICT reader, so
    //     uploading the source bytes verbatim would brick the next
    //     startup on the very column we just dropped; (b) a dropped
    //     column must be GENUINELY ABSENT from the imported segment — a
    //     query naming it behaves exactly as for a column that never
    //     existed, never a half-present null.  So the pruned segment is
    //     re-written (FerroDruid v9 layout) into a private staging dir,
    //     and THAT byte set is what gets hashed and uploaded (same H1
    //     immutable-byte-set discipline as the staged materialization).
    //     With nothing dropped the staged source bytes are uploaded
    //     verbatim, byte-identical to the strict path.
    let (blob_src, _rewrite_guard): (PathBuf, Option<tempfile::TempDir>) =
        if let Some(mut segment) = lenient_segment {
            segment
                .dimensions
                .retain(|d| !dropped_columns.iter().any(|c| c == d));
            segment
                .metrics
                .retain(|m| !dropped_columns.iter().any(|c| c == m));
            let staging = match tempfile::tempdir() {
                Ok(t) => t,
                Err(e) => {
                    return fail(
                        record,
                        format!("failed to create the lenient-rewrite staging dir: {e}"),
                    );
                }
            };
            let dest = staging.path().join("segment");
            if let Err(e) = write_segment_v9(&segment, &dest) {
                return fail(
                    record,
                    format!(
                        "failed to re-write the segment without its {} unreadable \
                         column(s): {e}",
                        dropped_columns.len()
                    ),
                );
            }
            // 6c. VERIFY the rewrite BEFORE it is hashed/uploaded/committed:
            //     re-open the just-written blob with the STRICT reader — the
            //     exact reader the next `serve` startup's bootstrap reload
            //     uses — and require its column set to be exactly the pruned
            //     set.  Without this, any writer defect (e.g. a column name
            //     the FerroDruid index.drd encoding cannot represent) would
            //     "succeed", commit the malformed blob, and BRICK the next
            //     startup fail-loud.  A failed verification is a clean
            //     per-segment failure: nothing partial reaches deep storage
            //     or the metadata store.
            if let Err(reason) = verify_lenient_rewrite(&dest, &segment, &dropped_columns) {
                return fail(record, reason);
            }
            (dest, Some(staging))
        } else {
            (materialized.dir().to_path_buf(), None)
        };

    // 7. Content hash over the STAGED file set — the exact bytes step 8
    //    uploads (H1) — so the bootstrap reload's re-hash of the
    //    re-downloaded blob reproduces this value (H2).
    let sha256 = match blob_content_hash(&blob_src) {
        Ok(h) => h,
        Err(e) => return fail(record, format!("content hash failed: {e}")),
    };

    if params.dry_run {
        seen.insert(ferro_id, incoming);
        record.status = AttachStatus::WouldAttach { rows };
        return record;
    }

    // 8. P (persist) FIRST: durable blob upload of the staged bytes. On
    //    a fresh attach a failure here commits nothing (no row exists
    //    yet), so the next startup is unaffected.  On a `--force`
    //    REPLACE the upload overwrites the existing blob in place, so a
    //    failure (or crash) anywhere between the first replaced byte and
    //    the row update leaves the OLD row referencing partially
    //    replaced bytes — the row is deliberately NOT touched (H3), the
    //    next startup fails loud on the hash check, and re-running
    //    `attach --force` converges.
    if let Err(e) = storage.upload_segment(ds, &ferro_id, &blob_src).await {
        let reason = if exists {
            format!(
                "deep-storage upload failed while REPLACING an existing blob (--force): \
                 the metadata row was NOT touched, but the previously committed blob may \
                 already be partially overwritten — the next startup will fail loud on \
                 the hash check for this segment until a re-run of `attach --force` \
                 converges: {e}"
            )
        } else {
            format!("deep-storage upload failed (nothing committed): {e}")
        };
        return fail(record, reason);
    }

    // 9. M (metadata) only AFTER the durable blob exists. The loadSpec
    //    shape matches the live publish tails' — the bootstrap reload
    //    keys on (dataSource, segmentId) and verifies sha256.
    let Some(store) = store else {
        // Unreachable: a non-dry run always opens the store.
        return fail(record, "internal: metadata store unavailable".to_string());
    };
    let now_iso = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    let mut payload = serde_json::json!({
        "dataSource": ds,
        "numRows": rows,
        "attachedFrom": artifact.path.display().to_string(),
        "loadSpec": {
            "type": storage.backend_type(),
            "dataSource": ds,
            "segmentId": ferro_id,
            "sha256": sha256,
        },
    });
    if !dropped_columns.is_empty() {
        // Loud, durable manifest of what was LOST (`--allow-unreadable-
        // columns`): the operator can audit every partial import from the
        // metadata store alone, long after the report scrolled away.
        payload["droppedUnreadableColumns"] = serde_json::json!(dropped_columns);
    }
    let row = SegmentMetadataRow {
        id: ferro_id.clone(),
        data_source: ds.to_string(),
        created_date: now_iso,
        start,
        end,
        version: version.to_string(),
        used: true,
        payload,
    };
    if let Err(e) = store.insert_segment(&row).await {
        // The honest post-mortem differs by path (same split as the
        // upload-failure sibling above): on a fresh attach no row exists,
        // so the uploaded blob is an ignorable orphan; on a `--force`
        // REPLACE the PRE-EXISTING row survives and now references the
        // in-place-REPLACED blob bytes, whose hash no longer matches the
        // row's recorded sha256 — anything but harmless.
        let reason = if exists {
            format!(
                "metadata update failed AFTER the blob was REPLACED (--force): \
                 the pre-existing row survives but now references the replaced \
                 blob bytes, which no longer match its recorded sha256 — the \
                 next startup will fail loud on the hash check for this segment \
                 until a re-run of `attach --force` converges: {e}"
            )
        } else {
            format!(
                "metadata insert failed AFTER the blob upload — no row was \
                 committed, so the uploaded blob is a harmless orphan the \
                 bootstrap reload ignores; re-run attach to retry: {e}"
            )
        };
        return fail(record, reason);
    }
    seen.insert(ferro_id, incoming);
    record.status = AttachStatus::Attached {
        rows,
        replaced: exists,
    };
    record
}

// ---------------------------------------------------------------------------
// --allow-unreadable-columns: no-queryable-data guard
// ---------------------------------------------------------------------------

/// Refuse a lenient-opened segment that has NO queryable data left —
/// even under `--allow-unreadable-columns` these are loud per-segment
/// FAILURES, never a partial import:
///
/// * the `__time` column itself is missing or unreadable (a timeless
///   segment cannot be interval-pruned or time-queried);
/// * the segment declared dimensions and EVERY one of them was dropped
///   (metrics without any dimension context are not a useful import).
///
/// A segment that declared no dimensions at all (a pure-rollup metric
/// segment) is NOT failed by the dimension rule — there was nothing to
/// lose.
fn check_lenient_still_queryable(segment: &SegmentData, dropped: &[String]) -> Result<(), String> {
    let manifest = || {
        if dropped.is_empty() {
            "none decoded".to_string()
        } else {
            format!("dropped: [{}]", dropped.join(", "))
        }
    };
    if !segment.columns.contains_key("__time") {
        return Err(format!(
            "--allow-unreadable-columns: the `__time` column itself is missing or \
             unreadable ({}) — a segment without its timestamp column has no queryable \
             data, so it is still a loud per-segment failure, not a partial import",
            manifest()
        ));
    }
    let dims_total = segment.dimensions.len();
    let dims_dropped = segment
        .dimensions
        .iter()
        .filter(|d| dropped.iter().any(|c| c == *d))
        .count();
    if dims_total > 0 && dims_dropped == dims_total {
        return Err(format!(
            "--allow-unreadable-columns: EVERY dimension of this segment is unreadable \
             ({dims_dropped} of {dims_total}; {}) — a segment with no readable dimension \
             is still a loud per-segment failure, not a partial import",
            manifest()
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// --allow-unreadable-columns: rewrite must be STRICT-reloadable
// ---------------------------------------------------------------------------

/// Verify a lenient-rewrite blob is exactly what the next startup needs,
/// BEFORE anything about it is committed: it must re-open under the
/// STRICT reader (the one `Overlord::bootstrap_reload_segments` uses on
/// every committed blob), and its column set must be exactly the pruned
/// set — every kept column present, no dropped column resurrected, same
/// row count.  Any mismatch means the rewrite is NOT safe to commit: a
/// committed blob the strict reader rejects would brick the next
/// startup fail-loud, so the segment becomes a clean per-segment
/// failure instead (nothing uploaded, no metadata row).
fn verify_lenient_rewrite(
    rewritten_dir: &Path,
    pruned: &SegmentData,
    dropped: &[String],
) -> Result<(), String> {
    let reopened = assess::open_segment_guarded(rewritten_dir).map_err(|reason| {
        format!(
            "lenient re-write verification failed: the re-written segment does NOT \
             re-open under the STRICT reader (the reader the next `ferrodruid serve` \
             bootstrap reload uses on every committed blob), so committing it would \
             brick the next startup — clean per-segment failure instead (nothing \
             uploaded, nothing committed): {reason}"
        )
    })?;
    // No dropped column may resurface anywhere (declaration or data).
    for name in dropped {
        if reopened.columns.contains_key(name)
            || reopened.dimensions.iter().any(|d| d == name)
            || reopened.metrics.iter().any(|m| m == name)
        {
            return Err(format!(
                "lenient re-write verification failed: dropped column {name:?} \
                 REAPPEARED in the re-written segment — refusing to commit it"
            ));
        }
    }
    // Declaration lists must survive the rewrite exactly.
    if reopened.dimensions != pruned.dimensions || reopened.metrics != pruned.metrics {
        return Err(format!(
            "lenient re-write verification failed: the re-written segment declares \
             dimensions {:?} / metrics {:?} but the pruned segment expected {:?} / \
             {:?} — refusing to commit a blob that does not match what was verified",
            reopened.dimensions, reopened.metrics, pruned.dimensions, pruned.metrics
        ));
    }
    // Every kept column's data must be present — and nothing extra.
    let mut got: Vec<&str> = reopened.columns.keys().map(String::as_str).collect();
    let mut want: Vec<&str> = pruned.columns.keys().map(String::as_str).collect();
    got.sort_unstable();
    want.sort_unstable();
    if got != want {
        return Err(format!(
            "lenient re-write verification failed: the re-written segment carries \
             column data {got:?} but the pruned segment expected {want:?} — refusing \
             to commit a blob that does not match what was verified"
        ));
    }
    if reopened.num_rows != pruned.num_rows {
        return Err(format!(
            "lenient re-write verification failed: the re-written segment has \
             {} row(s) but the pruned segment expected {} — refusing to commit \
             a blob that does not match what was verified",
            reopened.num_rows, pruned.num_rows
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Segment identity (ferro_id non-injectivity, R7)
// ---------------------------------------------------------------------------

/// The STRUCTURED identity behind a `ferro_id`.
///
/// The `ferro_id` is `<dataSource>_<start>_<end>_<version>_<partition>`
/// — Druid's own segment-id convention — but that `_`-joined string is
/// NOT injective: a `dataSource` or `version` may itself contain `_`, so
/// two distinct field tuples can collapse onto one id (e.g. `ds="x",
/// version="…_v"` vs `ds="x_…", version="v"`).  Collision handling
/// therefore compares this tuple, never the joined string alone — a
/// ferro_id already used by a DIFFERENT tuple is a true collision between
/// two distinct segments, refused loudly instead of silently skipped or
/// `--force`-overwritten (Codex R7 — data loss).
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SegmentIdent {
    data_source: String,
    start: String,
    end: String,
    version: String,
    partition: String,
}

impl SegmentIdent {
    /// The Druid-convention joined id.  Kept as the store key / reload
    /// handle even though it is non-injective (see the type docs).
    fn ferro_id(&self) -> String {
        format!(
            "{}_{}_{}_{}_{}",
            self.data_source, self.start, self.end, self.version, self.partition
        )
    }
}

/// Whether a stored metadata row carries the SAME identity as `incoming`.
///
/// The row and `incoming` are only compared when their `ferro_id`s are
/// already equal (same store key), so equal `data_source`/`start`/`end`/
/// `version` forces the `partition` suffix to be equal too — comparing
/// those four columns is sufficient to prove the full tuple matches.
fn row_matches_identity(row: &SegmentMetadataRow, incoming: &SegmentIdent) -> bool {
    row.data_source == incoming.data_source
        && row.start == incoming.start
        && row.end == incoming.end
        && row.version == incoming.version
}

/// Loud reason for a true `ferro_id` collision between two DISTINCT
/// Druid identities (R7).
fn ferro_id_collision_reason(ferro_id: &str) -> String {
    format!(
        "ferro_id {ferro_id:?} is already used by a DIFFERENT segment identity \
         (dataSource/interval/version/partition) — the Druid `_`-joined id is not \
         injective when a field contains `_`, so refusing to silently skip or \
         `--force`-overwrite a distinct segment (that would lose data); dedupe / \
         rename the colliding source segments and re-run"
    )
}

// ---------------------------------------------------------------------------
// Identity helpers
// ---------------------------------------------------------------------------

/// Split a segment interval into `(start, end)` and VALIDATE it (H5).
///
/// Accepts both encodings the identity sources produce: the
/// `descriptor.json` slash form (`<start>/<end>`) and the path-layout
/// underscore form (`<start>_<end>` — ISO-8601 instants contain no
/// underscore, so the split is unambiguous).  Both bounds must parse as
/// ISO-8601 instants with `start < end` — the values come from an
/// untrusted descriptor/path and become metadata rows and segment ids,
/// so garbage like `"z/a"` or an inverted interval is rejected loudly
/// instead of being attached under a nonsense identity.  The ORIGINAL
/// strings are returned (Druid's own encoding is the canonical row/id
/// form); only their parseability and ordering are checked.
pub(crate) fn split_interval(interval: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = if interval.contains('/') {
        interval.split('/').collect()
    } else {
        interval.split('_').collect()
    };
    match parts.as_slice() {
        [start, end] if !start.is_empty() && !end.is_empty() => {
            let (Some(start_ms), Some(end_ms)) = (
                parse_iso_instant_millis(start),
                parse_iso_instant_millis(end),
            ) else {
                return Err(format!(
                    "segment interval {interval:?} is not a pair of ISO-8601 instants \
                     (identity fields are validated, not trusted)"
                ));
            };
            if start_ms >= end_ms {
                return Err(format!(
                    "segment interval {interval:?} is empty or inverted \
                     (start must be strictly before end)"
                ));
            }
            Ok(((*start).to_string(), (*end).to_string()))
        }
        _ => Err(format!(
            "cannot parse segment interval {interval:?} into start/end \
             (expected `<start>/<end>` from a descriptor.json or \
             `<start>_<end>` from the path layout)"
        )),
    }
}

/// Parse one ISO-8601 interval bound into epoch milliseconds: full
/// RFC-3339 (`2015-09-12T00:00:00.000Z`, offsets allowed) or the bare
/// date form (`2015-09-12`, midnight UTC — a valid Druid interval
/// bound).  Returns `None` for anything else — same acceptance set as
/// the query layer's `parse_iso_millis`.
pub(crate) fn parse_iso_instant_millis(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_millis());
    }
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .map(|d| {
            d.and_time(chrono::NaiveTime::MIN)
                .and_utc()
                .timestamp_millis()
        })
}

/// Verify a segment PAYLOAD's `interval` agrees with the source row's
/// authoritative `start`/`end` COLUMNS (compat-7 R5).  `druid_segments`
/// keeps `start`/`end` as indexed columns and Druid derives the
/// canonical segment id from them — the columns are the identity, the
/// payload is only the descriptor.  A row whose payload claims a
/// different interval than its columns would import under the wrong
/// identity (wrong pruning/results, and `--force` overwriting a
/// fabricated id), so it must be refused.
///
/// The compare is SEMANTIC (epoch millis via [`parse_iso_instant_millis`]),
/// so a benign ISO formatting difference (`…:00.000Z` vs `…:00Z`) between
/// column and payload does NOT false-mismatch.  A malformed payload
/// interval or any bound that will not parse is a loud `Err` naming the
/// side at fault.
pub(crate) fn payload_interval_matches_columns(
    payload_interval: &str,
    col_start: &str,
    col_end: &str,
) -> Result<(), String> {
    let (p_start, p_end) = split_interval(payload_interval)?;
    let Some(p_start_ms) = parse_iso_instant_millis(&p_start) else {
        return Err(format!(
            "payload interval start {p_start:?} is not an ISO-8601 instant"
        ));
    };
    let Some(p_end_ms) = parse_iso_instant_millis(&p_end) else {
        return Err(format!(
            "payload interval end {p_end:?} is not an ISO-8601 instant"
        ));
    };
    let Some(c_start_ms) = parse_iso_instant_millis(col_start) else {
        return Err(format!(
            "row start column {col_start:?} is not an ISO-8601 instant"
        ));
    };
    let Some(c_end_ms) = parse_iso_instant_millis(col_end) else {
        return Err(format!(
            "row end column {col_end:?} is not an ISO-8601 instant"
        ));
    };
    if p_start_ms == c_start_ms && p_end_ms == c_end_ms {
        Ok(())
    } else {
        Err(format!(
            "payload interval {payload_interval:?} does not match the row's \
             start/end columns {col_start:?}/{col_end:?} (the columns are the \
             authoritative identity)"
        ))
    }
}

/// Validate + canonicalize a partition number (H5): a partition must be
/// a non-negative integer, and its CANONICAL decimal form is what the
/// ferro segment id carries — `"00"` and `"0"` are the same partition
/// and must synthesize the same id (one attach + one skip, never a
/// duplicate attach of one real partition under two spellings).
/// Negative (`"-1"`), signed (`"+1"`), non-numeric, and over-range
/// values are rejected loudly.
fn normalize_partition(partition: &str) -> Result<String, String> {
    if partition.is_empty() || !partition.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "segment partition {partition:?} is not a non-negative integer \
             (identity fields are validated, not trusted)"
        ));
    }
    let value: u64 = partition.parse().map_err(|_| {
        format!("segment partition {partition:?} does not fit a 64-bit unsigned integer")
    })?;
    Ok(value.to_string())
}

/// Reject a data source / segment id that is not a single safe path
/// component — the same rule the deep-storage layer enforces
/// authoritatively at upload time (H9), applied here up front so a
/// hostile descriptor fails loudly (and identically) even in a dry run.
fn check_path_component(kind: &str, component: &str) -> Result<(), String> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component.contains('/')
        || component.contains('\\')
        || component.contains('\0')
    {
        return Err(format!(
            "unsafe {kind} {component:?}: not a single path component \
             (path traversal rejected)"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// Everything the report printer needs.
struct ReportInput<'a> {
    params: &'a AttachParams,
    /// The source as the OPERATOR named it — the `s3://bucket/prefix`
    /// URL for a staged s3 run (where `params.deep_storage` was
    /// replaced by the local staging root), the local path otherwise.
    source: &'a str,
    records: &'a [AttachRecord],
    filtered: usize,
    scan: &'a ScanResult,
}

/// Print the human-readable per-segment report + summary.  Untrusted
/// strings (ids derived from descriptors, reader reasons, paths) are
/// sanitized against terminal-escape injection, same as `assess`.
fn print_report(input: &ReportInput<'_>) {
    let params = input.params;
    println!("FerroDruid attach — Druid v9 segment import (single-binary deployments)");
    println!(
        "druid deep-storage root: {}",
        assess::sanitize(input.source)
    );
    println!(
        "ferro deep-storage:      {}",
        assess::sanitize(&params.ferro_deep_storage.display().to_string())
    );
    // NEVER print the metadata URI verbatim: postgres://user:secret@…
    // embeds credentials — redact first (same discipline as `serve`,
    // which logs only the backend kind), then escape-sanitize.
    println!(
        "metadata store:          {}",
        assess::sanitize(&redact_metadata_uri(&params.metadata_uri))
    );
    if params.dry_run {
        println!("DRY RUN — nothing was written.");
    }
    println!();

    if input.records.is_empty() && input.filtered == 0 {
        if input.scan.truncated {
            println!(
                "NOTE: scan stopped at --max-segments; artifacts exist but were not attached."
            );
        } else {
            println!(
                "No segment artifacts found (looked for `index.zip` files and \
                 directories containing `meta.smoosh`)."
            );
        }
    }

    let mut attached = 0usize;
    let mut would_attach = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    for r in input.records {
        let id = r
            .ferro_id
            .as_deref()
            .map_or_else(|| "-".to_string(), assess::sanitize);
        match &r.status {
            AttachStatus::Attached { rows, replaced } => {
                attached += 1;
                let suffix = if *replaced {
                    " (replaced existing — --force)"
                } else {
                    ""
                };
                println!("ATTACHED     {rows:>9} rows  {id}{suffix}");
            }
            AttachStatus::WouldAttach { rows } => {
                would_attach += 1;
                println!("WOULD-ATTACH {rows:>9} rows  {id}");
            }
            AttachStatus::SkippedExists => {
                skipped += 1;
                println!(
                    "SKIP-EXISTS          -       {id} — a segment with this id is \
                     already in the metadata store (use --force to replace)"
                );
            }
            AttachStatus::Failed { reason } => {
                failed += 1;
                println!("FAILED               -       {id}");
                println!(
                    "             artifact: {}",
                    assess::sanitize(&r.artifact_path.display().to_string())
                );
                println!("             reason: {}", assess::sanitize(reason));
            }
        }
        if matches!(
            r.status,
            AttachStatus::Attached { .. } | AttachStatus::WouldAttach { .. }
        ) && !r.dropped_columns.is_empty()
        {
            // LOUD per-segment manifest of what was lost under
            // --allow-unreadable-columns — never silent.
            println!(
                "             dropped {} unreadable column(s): [{}] — NOT imported; a \
                 query naming them behaves as for a column that never existed",
                r.dropped_columns.len(),
                assess::sanitize(&r.dropped_columns.join(", "))
            );
        }
        if !matches!(r.status, AttachStatus::Failed { .. }) {
            println!("             identity: [{}]", r.identity_source.as_str());
        }
    }

    println!();
    println!(
        "Summary: {attached} attached, {would_attach} would attach (dry run), \
         {skipped} skipped (already attached), {failed} failed, {} filtered out \
         (--datasource), {} artifact(s) processed.",
        input.filtered,
        input.records.len(),
    );
    if failed > 0 {
        println!(
            "WARNING: {failed} segment(s) failed and were NOT attached (see reasons \
             above) — successes are unaffected; fix the cause and re-run (already-\
             attached ids are skipped)."
        );
    }
    if attached > 0 {
        println!(
            "Next step: restart the FerroDruid instance (`ferrodruid serve`) — its \
             startup bootstrap reload downloads, hash-verifies, and loads every \
             attached segment; it is then query-visible."
        );
    }
    if input.scan.truncated {
        println!(
            "NOTE: scan stopped at --max-segments; more artifacts exist and were \
             not processed."
        );
    }
    if input.scan.aborted {
        println!(
            "WARNING: the scan hit a hard resource cap and stopped early — this \
             run is INCOMPLETE (see scan warnings)."
        );
    }
    if !input.scan.warnings.is_empty() {
        println!("Scan warnings ({}):", input.scan.warnings.len());
        for w in &input.scan.warnings {
            println!("  - {}", assess::sanitize(w));
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_interval_descriptor_slash_form() {
        let (start, end) =
            split_interval("2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z").expect("split");
        assert_eq!(start, "2015-09-12T00:00:00.000Z");
        assert_eq!(end, "2015-09-13T00:00:00.000Z");
    }

    #[test]
    fn split_interval_path_underscore_form() {
        let (start, end) =
            split_interval("2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z").expect("split");
        assert_eq!(start, "2015-09-12T00:00:00.000Z");
        assert_eq!(end, "2015-09-13T00:00:00.000Z");
    }

    #[test]
    fn split_interval_rejects_malformed() {
        for bad in [
            "",
            "no-separator",
            "a/b/c",
            "a_b_c",
            "/end-only",
            "start-only/",
            "_end-only",
            "start-only_",
        ] {
            assert!(
                split_interval(bad).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn ferro_id_is_the_druid_segment_id_convention() {
        // ds_start_end_version_partition — deterministic, collision-free
        // per Druid's own id uniqueness (partition included).
        let (start, end) =
            split_interval("2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z").expect("split");
        let id = format!("{}_{start}_{end}_{}_{}", "wiki", "v1", "0");
        assert_eq!(
            id,
            "wiki_2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z_v1_0"
        );
    }

    /// H5: identity fields are VALIDATED, not trusted — a segment
    /// interval must be a pair of parseable ISO-8601 instants with
    /// `start < end`.
    #[test]
    fn split_interval_requires_iso_instants_and_ordering() {
        for bad in [
            // not timestamps at all
            "z/a",
            "a_b",
            "1/2",
            // inverted and empty intervals
            "2015-09-13T00:00:00.000Z/2015-09-12T00:00:00.000Z",
            "2015-09-12T00:00:00.000Z/2015-09-12T00:00:00.000Z",
        ] {
            assert!(
                split_interval(bad).is_err(),
                "expected {bad:?} to be rejected as a non-ISO/inverted interval"
            );
        }
        // Bare-date bounds are valid ISO-8601 interval bounds in Druid.
        assert!(split_interval("2015-09-12/2015-09-13").is_ok());
    }

    /// R5: a payload interval must agree SEMANTICALLY with the row's
    /// authoritative `start`/`end` columns (the columns are the
    /// identity, the payload is the descriptor).
    #[test]
    fn payload_interval_matches_columns_semantics() {
        // Exact agreement.
        assert!(
            payload_interval_matches_columns(
                "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z",
                "2024-01-01T00:00:00.000Z",
                "2024-01-02T00:00:00.000Z",
            )
            .is_ok()
        );
        // Benign ISO formatting difference (payload drops the millis /
        // uses a bare date) — SAME instants, so NOT a mismatch.
        assert!(
            payload_interval_matches_columns(
                "2024-01-01T00:00:00Z/2024-01-02",
                "2024-01-01T00:00:00.000Z",
                "2024-01-02T00:00:00.000Z",
            )
            .is_ok(),
            "a semantic compare must not false-mismatch on formatting"
        );
        // A real disagreement (2024 columns vs a 2030 payload).
        let err = payload_interval_matches_columns(
            "2030-01-01T00:00:00.000Z/2030-01-02T00:00:00.000Z",
            "2024-01-01T00:00:00.000Z",
            "2024-01-02T00:00:00.000Z",
        )
        .expect_err("disagreeing intervals must be refused");
        assert!(
            err.contains("2030") && err.contains("2024"),
            "the error names both intervals: {err}"
        );
        // A malformed payload interval is a loud Err.
        assert!(
            payload_interval_matches_columns(
                "not-an-interval",
                "2024-01-01T00:00:00.000Z",
                "2024-01-02T00:00:00.000Z",
            )
            .is_err()
        );
        // An unparseable bound (here a column) is a loud Err naming the
        // side at fault.
        let err = payload_interval_matches_columns(
            "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z",
            "garbage",
            "2024-01-02T00:00:00.000Z",
        )
        .expect_err("an unparseable column bound must be refused");
        assert!(err.contains("start column"), "names the side: {err}");
    }

    /// H5: partitions canonicalize (`"00"` == `"0"` — one id, no
    /// duplicate attach) and non-numeric / negative / signed / oversized
    /// values are rejected.
    #[test]
    fn normalize_partition_canonicalizes_and_rejects() {
        assert_eq!(normalize_partition("0").expect("0"), "0");
        assert_eq!(normalize_partition("00").expect("00"), "0");
        assert_eq!(normalize_partition("007").expect("007"), "7");
        assert_eq!(normalize_partition("12").expect("12"), "12");
        for bad in ["-1", "+1", "", "abc", "1.0", " 1", "18446744073709551616"] {
            assert!(
                normalize_partition(bad).is_err(),
                "expected partition {bad:?} to be rejected"
            );
        }
    }

    /// H1: the staged materialization must pin the exact byte set the
    /// content hash covered, so a source-dir mutation between the hash
    /// and the upload cannot make the committed sha256 diverge from the
    /// uploaded bytes (which would brick the next bootstrap reload).
    #[tokio::test]
    async fn staged_materialization_pins_hash_and_upload_bytes_against_source_mutation() {
        let holder = tempfile::tempdir().expect("tempdir");
        let src_dir = holder.path().join("seg");
        std::fs::create_dir_all(&src_dir).expect("mkdir src");
        std::fs::write(src_dir.join("meta.smoosh"), b"v1,2048,1").expect("write meta");
        std::fs::write(src_dir.join("00000.smoosh"), b"CHUNK-BYTES-A").expect("write chunk");
        let artifact = FoundArtifact {
            path: src_dir.clone(),
            kind: assess::ArtifactKind::SmooshDir,
        };

        // The attach pipeline order: materialize → hash → (mutation
        // races in) → upload.
        let materialized = assess::materialize_artifact_staged(holder.path(), &artifact)
            .expect("staged materialization");
        let committed = blob_content_hash(materialized.dir()).expect("hash staging");

        std::fs::write(src_dir.join("00000.smoosh"), b"MUTATED-AFTER-HASH").expect("mutate src");

        let base = holder.path().join("ferro");
        let storage = LocalDeepStorage::new(base.clone());
        storage
            .upload_segment("wiki", "seg_h1", materialized.dir())
            .await
            .expect("upload staging");

        let uploaded =
            blob_content_hash(&base.join("wiki").join("seg_h1")).expect("re-hash uploaded blob");
        assert_eq!(
            committed, uploaded,
            "H1: the committed sha256 must match the uploaded bytes even \
             when the SOURCE dir is mutated between hash and upload"
        );
    }

    /// `--allow-unreadable-columns` still refuses a segment with no
    /// queryable data: a dropped/missing `__time`, or EVERY dimension
    /// dropped, is a loud failure; a partial drop (or a dimensionless
    /// rollup segment losing a metric) passes.
    #[test]
    fn lenient_guard_refuses_timeless_and_dimensionless_segments() {
        use ferrodruid_segment::SegmentDataBuilder;

        // (a) __time missing/unreadable → refuse.
        let mut timeless = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000])
            .add_string_column("d", vec!["x".into(), "y".into()])
            .build()
            .expect("build");
        timeless.columns.remove("__time");
        let err = check_lenient_still_queryable(&timeless, &["__time".to_string()])
            .expect_err("a timeless segment must be refused");
        assert!(err.contains("__time"), "names __time: {err}");

        // (b) EVERY declared dimension dropped → refuse.
        let mut dimless = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000])
            .add_string_column("a", vec!["x".into(), "y".into()])
            .add_string_column("b", vec!["x".into(), "y".into()])
            .add_long_column("m", true, vec![1, 2])
            .build()
            .expect("build");
        dimless.columns.remove("a");
        dimless.columns.remove("b");
        let err = check_lenient_still_queryable(&dimless, &["a".to_string(), "b".to_string()])
            .expect_err("all dimensions unreadable must be refused");
        assert!(err.contains("EVERY dimension"), "names the rule: {err}");

        // (c) SOME dimensions dropped → allowed (that is the point of
        // the flag).
        let mut partial = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000])
            .add_string_column("a", vec!["x".into(), "y".into()])
            .add_string_column("b", vec!["x".into(), "y".into()])
            .build()
            .expect("build");
        partial.columns.remove("b");
        assert!(check_lenient_still_queryable(&partial, &["b".to_string()]).is_ok());

        // (d) a segment that declared NO dimensions (pure-rollup metric
        // segment) losing a metric → allowed; the dimension rule is
        // vacuous, __time is present.
        let mut rollup = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000])
            .add_long_column("m1", true, vec![1, 2])
            .add_long_column("m2", true, vec![3, 4])
            .build()
            .expect("build");
        rollup.columns.remove("m2");
        assert!(check_lenient_still_queryable(&rollup, &["m2".to_string()]).is_ok());
    }

    /// The lenient-rewrite STRICT-reload guard: a faithful rewrite
    /// passes; a blob the strict reader cannot re-open, a resurrected
    /// dropped column, a missing kept column, or a row-count drift is
    /// refused BEFORE anything is hashed/uploaded/committed (a committed
    /// strict-unreadable blob would brick the next bootstrap reload).
    #[test]
    fn verify_lenient_rewrite_guards_strict_reload() {
        use ferrodruid_segment::SegmentDataBuilder;

        let pruned = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000])
            .add_string_column("d", vec!["x".into(), "y".into()])
            .add_long_column("m", true, vec![1, 2])
            .build()
            .expect("build pruned");
        let dropped = vec!["sketchcol".to_string()];

        // (a) A faithful rewrite strict-reopens and matches → Ok.
        let holder = tempfile::tempdir().expect("tempdir");
        let good = holder.path().join("good");
        write_segment_v9(&pruned, &good).expect("write good");
        assert!(verify_lenient_rewrite(&good, &pruned, &dropped).is_ok());

        // (b) A blob the STRICT reader cannot re-open (writer defect /
        // corruption) → refused, naming the brick hazard.
        let bad = holder.path().join("bad");
        write_segment_v9(&pruned, &bad).expect("write bad");
        std::fs::write(bad.join("meta.smoosh"), "v1,2147483647,1\nnope").expect("corrupt meta");
        let err = verify_lenient_rewrite(&bad, &pruned, &dropped)
            .expect_err("a strict-unreadable rewrite must be refused");
        assert!(
            err.contains("STRICT reader") && err.contains("nothing committed"),
            "names the strict-reload hazard: {err}"
        );

        // (c) A dropped column REAPPEARING in the written blob → refused.
        let resurrected = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000])
            .add_string_column("d", vec!["x".into(), "y".into()])
            .add_long_column("m", true, vec![1, 2])
            .add_long_column("sketchcol", true, vec![7, 8])
            .build()
            .expect("build resurrected");
        let ghost = holder.path().join("ghost");
        write_segment_v9(&resurrected, &ghost).expect("write ghost");
        let err = verify_lenient_rewrite(&ghost, &pruned, &dropped)
            .expect_err("a resurrected dropped column must be refused");
        assert!(err.contains("REAPPEARED"), "names the rule: {err}");

        // (d) A KEPT column missing from the written blob → refused.
        let missing = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000, 2000])
            .add_string_column("d", vec!["x".into(), "y".into()])
            .build()
            .expect("build missing");
        let short = holder.path().join("short");
        write_segment_v9(&missing, &short).expect("write short");
        let err = verify_lenient_rewrite(&short, &pruned, &dropped)
            .expect_err("a missing kept column must be refused");
        assert!(
            err.contains("declares"),
            "names the declaration drift: {err}"
        );

        // (e) Row-count drift (same schema, fewer rows) → refused.
        let fewer = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1000])
            .add_string_column("d", vec!["x".into()])
            .add_long_column("m", true, vec![1])
            .build()
            .expect("build fewer");
        let thin = holder.path().join("thin");
        write_segment_v9(&fewer, &thin).expect("write thin");
        let err = verify_lenient_rewrite(&thin, &pruned, &dropped)
            .expect_err("a row-count drift must be refused");
        assert!(err.contains("row(s)"), "names the row drift: {err}");
    }

    #[test]
    fn check_path_component_mirrors_deep_storage_rules() {
        for bad in ["", ".", "..", "a/b", "a\\b", "x\0y", "../../etc"] {
            assert!(
                check_path_component("segment id", bad).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
        // Colons from ISO-8601 timestamps are legitimate in segment ids.
        assert!(
            check_path_component(
                "segment id",
                "wiki_2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z_v1_0"
            )
            .is_ok()
        );
    }
}
