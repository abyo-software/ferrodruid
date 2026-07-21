// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-C: `s3://` SOURCE support for `assess` / `attach` /
//! `import-druid-metadata` — resolve→pull→attach against real S3 (or
//! any S3-compatible store) by WIRING the existing, proven
//! [`ferrodruid_deep_storage::S3DeepStorage`] (object_store-based) into
//! the offline migration tools. No new S3 client: listing and fetching
//! go through the same code path the product's S3 deep storage uses.
//!
//! Two shapes are supported:
//!
//! * **`attach`/`assess` `--deep-storage s3://bucket/prefix`** — the
//!   prefix is listed ([`S3DeepStorage::list_objects`], stream-backed
//!   so S3 pagination is transparent) and every planned object is
//!   downloaded into a private local staging tempdir PRESERVING its
//!   relative key path ([`stage_s3_tree`]). Every download GETs the
//!   very [`ListedObject`] the listing enumerated — list and fetch
//!   share ONE `object_store` path, with no String round-trip in
//!   which `object_store`'s silent normalization could re-address the
//!   request to a different object (wrong-object substitution,
//!   listing-derived sibling of the R1 direct-GET guard). The existing
//!   local scan/identity/materialize/attach tail then runs over the
//!   staging root UNCHANGED, so the zip-slip/zip-bomb caps, the staged
//!   materialization containment, the content hash, and the P→M
//!   ordering are all inherited verbatim.
//! * **`import-druid-metadata` `s3_zip` loadSpec rows** — the row's
//!   `bucket`/`key` (UNTRUSTED input) are validated, the single
//!   `index.zip` is fetched into a per-row staging tempdir
//!   ([`s3_client_from_env`] + [`S3DeepStorage::fetch_object_to_file`]),
//!   and the row runs the existing attach tail with the tempdir as its
//!   containment root.
//!
//! Client configuration comes from the standard `AWS_*` environment
//! (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, …;
//! `AWS_ENDPOINT` + `AWS_ALLOW_HTTP=true` for S3-compatible stores like
//! MinIO). Retries are bounded so a wrong endpoint fails in seconds,
//! loudly, instead of hanging the run.
//!
//! ## Hardening / honest limits
//!
//! * Downloaded bytes are UNTRUSTED: they flow through the exact same
//!   zip-slip/zip-bomb caps and staged-materialize containment as local
//!   artifacts (nothing is short-cut) — the only S3-specific guards
//!   added here are a per-object download cap
//!   ([`MAX_S3_OBJECT_BYTES`], disk-fill), a listing cap
//!   ([`MAX_S3_KEYS`], enforced WHILE the listing stream is consumed —
//!   memory is bounded by the cap, not the bucket size), and strict
//!   validation of every key component BEFORE it is mirrored onto the
//!   local filesystem.
//! * An `s3_zip` metadata row names its OWN bucket: the operator's
//!   credentials will read whatever bucket the (untrusted) row names.
//!   The bucket name is validated as a plain token, and the fetched
//!   bytes still must pass the v9 read gate under the row's
//!   cross-checked identity — but operators importing from a metadata
//!   DB they do not fully trust should scope their AWS credentials to
//!   the expected bucket(s).
//! * Objects are STREAMED to the staging file chunk-by-chunk (never
//!   whole-object in RAM); local staging disk must hold the artifacts
//!   being processed.
//! * An S3 object whose RAW key carries a leading/trailing `/` is not
//!   addressable through `object_store` (its parser silently strips
//!   the slash) and is therefore SKIPPED by this pipeline — never
//!   mis-fetched: its normalized alias either collides with a real
//!   sibling (loud [`S3DeepStorage::list_objects`] refusal) or fails
//!   the fetch loudly. Druid deep-storage keys never carry
//!   leading/trailing slashes, so this only surfaces on pathological
//!   buckets.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ferrodruid_deep_storage::{
    ListedObject, MAX_S3_KEYS, S3DeepStorage, validate_object_key_segment,
};

/// Per-object download cap — mirrors the extract-side
/// `MAX_UNCOMPRESSED_BYTES` in `assess` (64 GiB): a single hostile key
/// cannot fill the local disk unboundedly.
pub(crate) const MAX_S3_OBJECT_BYTES: u64 = 64 * 1024 * 1024 * 1024;

// Cap on listed keys under one prefix: the SHARED
// `ferrodruid_deep_storage::MAX_S3_KEYS` (1M — same discipline as the
// local scan's `MAX_SCAN_DIRS`/`MAX_DIR_ENTRIES` bounds), imported
// above so the stream-time abort inside `list_objects` and the
// planner's defensive re-check below use the IDENTICAL number.

/// Bounded retry so an unreachable/wrong endpoint fails in seconds
/// (object_store's default retries for minutes).
const S3_MAX_RETRIES: usize = 2;

/// Overall per-request retry budget.
const S3_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// s3:// URL parsing
// ---------------------------------------------------------------------------

/// A parsed `s3://bucket[/prefix]` source location.
#[derive(Debug)]
pub(crate) struct S3SourceUrl {
    /// Bucket name (validated as a plain token).
    pub(crate) bucket: String,
    /// Key prefix under the bucket, WITHOUT a trailing slash; may be
    /// empty (whole bucket).
    pub(crate) key_prefix: String,
}

impl S3SourceUrl {
    /// Canonical `s3://bucket[/prefix]` display form.
    pub(crate) fn display(&self) -> String {
        if self.key_prefix.is_empty() {
            format!("s3://{}", self.bucket)
        } else {
            format!("s3://{}/{}", self.bucket, self.key_prefix)
        }
    }
}

/// Parse an `s3://…` string: `None` when `raw` does not carry the
/// `s3:` scheme at all (the caller falls through to the local-path
/// behavior), `Some(Err)` when it carries the scheme but is not a
/// canonical `s3://bucket[/prefix]` URL (loud refusal, H1 — a typo'd
/// s3 URL must NEVER be silently probed as a local directory, which
/// could attach unrelated local segments sitting at that relative
/// path).
///
/// The scheme match is CASE-INSENSITIVE (RFC 3986 §3.1): `S3://b/p` is
/// ACCEPTED and normalized to the canonical `s3://b/p`. Every other
/// shape under the scheme — `s3:/one-slash`, bare `s3:key`, `s3://`
/// (empty bucket), `s3:///key` — is a hard error, never a local
/// fallthrough.
pub(crate) fn parse_s3_url(raw: &str) -> Option<Result<S3SourceUrl, String>> {
    if !has_s3_scheme(raw) {
        return None;
    }
    // `has_s3_scheme` guarantees the first 3 bytes are ASCII (`s3:` in
    // either case), so slicing at 3 is char-boundary safe.
    let Some(rest) = raw[3..].strip_prefix("//") else {
        return Some(Err(format!(
            "invalid s3 URL {raw:?}: the `s3:` scheme must be followed by \
             `//bucket[/prefix]` — refusing to fall back to a local path"
        )));
    };
    Some(parse_s3_rest(rest, raw))
}

/// `true` when `raw` opens with the `s3:` URL scheme, matched
/// case-insensitively per RFC 3986 §3.1 (`s3:` / `S3:`). Anything that
/// carries the scheme is COMMITTED to being parsed as an s3 URL — it
/// can never fall through to the local-path behavior.
fn has_s3_scheme(raw: &str) -> bool {
    let b = raw.as_bytes();
    b.len() >= 3 && b[0].eq_ignore_ascii_case(&b's') && b[1] == b'3' && b[2] == b':'
}

/// [`parse_s3_url`] over a CLI `--deep-storage` value. A path without
/// the `s3:` scheme is a local path (`None`); a non-UTF-8 path whose
/// lossy form still opens with the scheme cannot be a valid s3 URL and
/// is refused LOUDLY rather than probed as a local path (H1).
pub(crate) fn s3_url_of_path(path: &Path) -> Option<Result<S3SourceUrl, String>> {
    match path.to_str() {
        Some(s) => parse_s3_url(s),
        None => {
            let lossy = path.to_string_lossy();
            if has_s3_scheme(&lossy) {
                Some(Err(format!(
                    "invalid s3 URL {lossy:?}: not valid UTF-8 — refusing to fall \
                     back to a local path"
                )))
            } else {
                None
            }
        }
    }
}

fn parse_s3_rest(rest: &str, raw: &str) -> Result<S3SourceUrl, String> {
    let (bucket, key_prefix) = match rest.split_once('/') {
        Some((b, p)) => (b, p.trim_end_matches('/')),
        None => (rest, ""),
    };
    validate_bucket_name(bucket)
        .map_err(|e| format!("invalid s3 URL {raw:?}: {e} (expected s3://bucket[/prefix])"))?;
    if key_prefix.contains('\0') || key_prefix.chars().any(|c| c.is_ascii_control()) {
        return Err(format!(
            "invalid s3 URL {raw:?}: the key prefix carries a NUL/control character"
        ));
    }
    Ok(S3SourceUrl {
        bucket: bucket.to_string(),
        key_prefix: key_prefix.to_string(),
    })
}

/// Validate a bucket name as a plain single token before it is handed
/// to the S3 client builder or interpolated into error messages —
/// bucket names can come from an UNTRUSTED metadata row (`s3_zip`
/// loadSpec), so anything outside `[A-Za-z0-9._-]` (or `.`/`..`/empty/
/// over-long) is refused loudly.
pub(crate) fn validate_bucket_name(bucket: &str) -> Result<(), String> {
    if bucket.is_empty() {
        return Err("s3 bucket name is empty".to_string());
    }
    if bucket.len() > 255 {
        return Err(format!(
            "s3 bucket name is {} bytes — over the 255-byte cap",
            bucket.len()
        ));
    }
    if bucket == "." || bucket == ".." {
        return Err(format!(
            "s3 bucket name {bucket:?} is a relative path token — refused"
        ));
    }
    if !bucket
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_')
    {
        return Err(format!(
            "s3 bucket name {bucket:?} contains characters outside [A-Za-z0-9._-] — \
             refused (untrusted input is never passed to the S3 client verbatim)"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Client construction (env-configured, bounded retries)
// ---------------------------------------------------------------------------

/// Build the S3 client for `bucket` from the `AWS_*` environment, with
/// an EMPTY store prefix so raw Druid deep-storage keys resolve as-is.
pub(crate) fn s3_client_from_env(bucket: &str) -> Result<S3DeepStorage, String> {
    validate_bucket_name(bucket)?;
    S3DeepStorage::from_env_with_retry(bucket, "", S3_MAX_RETRIES, S3_RETRY_TIMEOUT).map_err(|e| {
        format!(
            "failed to build the S3 client for bucket {bucket:?} from the AWS_* \
             environment (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_REGION; \
             AWS_ENDPOINT + AWS_ALLOW_HTTP=true for S3-compatible stores): {e}"
        )
    })
}

// ---------------------------------------------------------------------------
// Listing plan (pure, unit-tested)
// ---------------------------------------------------------------------------

/// One planned download: where the object lands locally, and WHICH
/// listed object it is — `source` indexes the very listing slice the
/// plan was computed from, so the fetch loop downloads the exact
/// [`ListedObject`] that produced this entry (list→fetch address
/// identity; no key string is ever re-parsed into a fetch address).
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PlannedDownload {
    /// Prefix-relative path to mirror under the staging root.
    pub(crate) rel: String,
    /// Index of the originating key in the listing slice handed to
    /// [`plan_downloads`].
    pub(crate) source: usize,
}

/// What [`stage_s3_tree`] will download, computed PURELY from the
/// listed keys so the rules are unit-testable without a network.
#[derive(Debug)]
pub(crate) struct DownloadPlan {
    /// Planned downloads, component-wise sorted by relative path so one
    /// directory's files are contiguous.
    pub(crate) downloads: Vec<PlannedDownload>,
    /// Keys skipped by the `--datasource` filter at LISTING time.
    pub(crate) filtered: usize,
    /// The plan stopped early at the `--max-segments` bound (whole
    /// artifact-directory groups only — a group is never split).
    pub(crate) truncated: bool,
}

/// Compute the download plan for `keys` (the `key()` strings of the
/// [`S3DeepStorage::list_objects`] result for `key_prefix`, in listing
/// order — each planned entry's `source` indexes this slice):
///
/// * every key is stripped to its prefix-relative path and each
///   component VALIDATED (no `.`/`..`/empty/backslash/control bytes)
///   before it may be mirrored onto the local filesystem — one bad key
///   fails the whole plan loudly (fail-closed);
/// * `--datasource` filters at listing time when the key plausibly
///   matches the Druid deep-storage layout
///   (`<ds>/<interval>/<version>/<partition>/<file>`, ≥ 5 components):
///   shallower keys are downloaded anyway and judged by the local scan,
///   so the filter can only ever skip keys the scan itself would have
///   filtered;
/// * `--max-segments` stops the plan after N directory GROUPS
///   containing an artifact marker (`index.zip` / `meta.smoosh`) have
///   been planned — groups are kept whole so a smoosh dir is never
///   half-downloaded; the local scan re-applies its own authoritative
///   `max_segments` bound afterwards.
pub(crate) fn plan_downloads(
    keys: &[String],
    key_prefix: &str,
    datasource: Option<&str>,
    max_segments: Option<usize>,
) -> Result<DownloadPlan, String> {
    // Defensive re-check only: the REAL memory-exhaustion enforcement
    // happens inside `S3DeepStorage::list_objects`, which aborts the
    // listing stream the moment it would exceed the SAME shared
    // `MAX_S3_KEYS` — an over-cap listing can never reach this planner.
    if keys.len() > MAX_S3_KEYS {
        return Err(format!(
            "{} keys listed — over the {MAX_S3_KEYS}-key cap; narrow the prefix",
            keys.len()
        ));
    }
    let mut items: Vec<(Vec<String>, usize)> = Vec::new();
    let mut filtered = 0usize;
    for (source, key) in keys.iter().enumerate() {
        let rel: &str = if key_prefix.is_empty() {
            key.as_str()
        } else {
            match key
                .strip_prefix(key_prefix)
                .and_then(|s| s.strip_prefix('/'))
            {
                Some(r) if !r.is_empty() => r,
                _ if key == key_prefix => {
                    return Err(format!(
                        "object {key:?} sits AT the prefix path itself — it cannot be \
                         mirrored under a directory tree; point the s3 URL at its parent \
                         prefix"
                    ));
                }
                _ => {
                    return Err(format!(
                        "listed key {key:?} does not sit under the requested prefix \
                         {key_prefix:?} — refusing an inconsistent listing"
                    ));
                }
            }
        };
        let comps: Vec<&str> = rel.split('/').collect();
        for c in &comps {
            validate_rel_component(c).map_err(|e| format!("unsafe S3 key {key:?}: {e}"))?;
        }
        if let Some(f) = datasource
            && comps.len() >= 5
            && comps[0] != f
        {
            filtered += 1;
            continue;
        }
        items.push((comps.into_iter().map(str::to_string).collect(), source));
    }
    // Component-wise sort: a directory's files become contiguous even
    // when sibling dir names make plain string order interleave them
    // (`a/b-x/…` sorts between `a/b/…` keys as strings, not as paths).
    items.sort();

    let mut downloads: Vec<PlannedDownload> = Vec::new();
    let mut truncated = false;
    let mut current_dir: Option<Vec<String>> = None;
    let mut group_has_artifact = false;
    let mut artifact_dirs = 0usize;
    for (comps, source) in items {
        let dir: Vec<String> = comps[..comps.len().saturating_sub(1)].to_vec();
        if current_dir.as_ref() != Some(&dir) {
            if group_has_artifact {
                artifact_dirs += 1;
            }
            group_has_artifact = false;
            if let Some(n) = max_segments
                && artifact_dirs >= n
            {
                truncated = true;
                break;
            }
            current_dir = Some(dir);
        }
        if let Some(name) = comps.last()
            && (name == "index.zip" || name == "meta.smoosh")
        {
            group_has_artifact = true;
        }
        downloads.push(PlannedDownload {
            rel: comps.join("/"),
            source,
        });
    }
    Ok(DownloadPlan {
        downloads,
        filtered,
        truncated,
    })
}

/// Reject a key component that must never be mirrored onto the local
/// filesystem — DELEGATES to the shared
/// [`ferrodruid_deep_storage::validate_object_key_segment`] rule
/// (empty / `.` / `..` / backslash / control bytes; `/` cannot appear
/// post-split), so the download plan and the direct fetch path
/// ([`S3DeepStorage::fetch_object_to_file`]) enforce exactly ONE rule.
fn validate_rel_component(comp: &str) -> Result<(), String> {
    validate_object_key_segment(comp)
}

// ---------------------------------------------------------------------------
// Staging
// ---------------------------------------------------------------------------

/// A staged local mirror of an s3 prefix. The tempdir guard keeps the
/// staging alive for as long as the caller holds the value — `attach`
/// holds it across the whole scan/attach run.
pub(crate) struct StagedS3Tree {
    /// Canonical staging root the local scan runs over.
    pub(crate) root: PathBuf,
    _guard: tempfile::TempDir,
    /// Objects downloaded into the staging root.
    pub(crate) objects_downloaded: usize,
    /// Total bytes downloaded.
    pub(crate) bytes_downloaded: u64,
    /// Keys skipped by the `--datasource` filter at listing time.
    pub(crate) filtered_keys: usize,
    /// The listing stopped early at the `--max-segments` group bound.
    pub(crate) listing_truncated: bool,
}

/// List `url` and download every planned object into a private staging
/// tempdir, preserving relative key paths (see [`plan_downloads`] for
/// the filter/bound/validation rules). Fails loudly on an empty
/// listing — a typo'd bucket/prefix must never be reported as a clean
/// "0 artifacts found" run.
pub(crate) async fn stage_s3_tree(
    url: &S3SourceUrl,
    datasource: Option<&str>,
    max_segments: Option<usize>,
) -> Result<StagedS3Tree, String> {
    let client = s3_client_from_env(&url.bucket)?;
    stage_s3_tree_with_client(&client, url, datasource, max_segments).await
}

/// [`stage_s3_tree`] over an already-built client (separated so the
/// listing→fetch pipeline is testable against an in-process store).
///
/// Every fetch downloads the exact [`ListedObject`] the listing
/// enumerated — the plan only decides WHICH listed objects to mirror
/// and WHERE, never re-derives a fetch address from a key string — so
/// the staged bytes under each relative path are that enumerated
/// object's bytes or the run fails loudly (wrong-object substitution
/// impossible by construction; see the module docs for the
/// leading/trailing-`/` residual).
pub(crate) async fn stage_s3_tree_with_client(
    client: &S3DeepStorage,
    url: &S3SourceUrl,
    datasource: Option<&str>,
    max_segments: Option<usize>,
) -> Result<StagedS3Tree, String> {
    // `MAX_S3_KEYS` is enforced by `list_objects` WHILE the listing
    // stream is consumed (abort at entry cap+1), so memory here is
    // bounded by the cap even against a bucket with tens of millions
    // of objects under the prefix.
    let objects: Vec<ListedObject> = client
        .list_objects(&url.key_prefix, MAX_S3_KEYS)
        .await
        .map_err(|e| format!("failed to list {}: {e}", url.display()))?;
    if objects.is_empty() {
        return Err(format!(
            "no objects found under {} — nothing to stage (check the bucket/prefix \
             and the AWS_* environment)",
            url.display()
        ));
    }
    let keys: Vec<String> = objects.iter().map(|o| o.key().to_string()).collect();
    let plan = plan_downloads(&keys, &url.key_prefix, datasource, max_segments)
        .map_err(|e| format!("refusing to stage {}: {e}", url.display()))?;

    let guard =
        tempfile::tempdir().map_err(|e| format!("failed to create the s3 staging dir: {e}"))?;
    let root = guard
        .path()
        .canonicalize()
        .map_err(|e| format!("failed to canonicalize the s3 staging dir: {e}"))?;

    let mut bytes_downloaded = 0u64;
    for planned in &plan.downloads {
        // The SAME listed object the plan entry was computed from —
        // the fetch address is the enumerated `object_store` path
        // itself, never a re-parsed string.
        let listed = objects.get(planned.source).ok_or_else(|| {
            format!(
                "internal error: planned download {:?} does not map back to a listed \
                 object (index {} of {})",
                planned.rel,
                planned.source,
                objects.len()
            )
        })?;
        // Every component of `rel` was validated by the plan; the
        // staging root is a fresh private tempdir, so the join cannot
        // escape it.
        let mut dest = root.clone();
        for c in planned.rel.split('/') {
            dest.push(c);
        }
        let n = client
            .fetch_listed_to_file(listed, &dest, Some(MAX_S3_OBJECT_BYTES))
            .await
            .map_err(|e| format!("failed to fetch s3://{}/{}: {e}", url.bucket, listed.key()))?;
        bytes_downloaded = bytes_downloaded.saturating_add(n);
    }
    Ok(StagedS3Tree {
        root,
        _guard: guard,
        objects_downloaded: plan.downloads.len(),
        bytes_downloaded,
        filtered_keys: plan.filtered,
        listing_truncated: plan.truncated,
    })
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    /// The planned relative paths (assertion convenience).
    fn rels(plan: &DownloadPlan) -> Vec<String> {
        plan.downloads.iter().map(|d| d.rel.clone()).collect()
    }

    // -- URL parsing ----------------------------------------------------------

    #[test]
    fn parse_s3_url_accepts_bucket_and_prefix_forms() {
        let u = parse_s3_url("s3://bkt/druid/segments")
            .expect("is s3")
            .expect("parses");
        assert_eq!(u.bucket, "bkt");
        assert_eq!(u.key_prefix, "druid/segments");
        assert_eq!(u.display(), "s3://bkt/druid/segments");

        let u = parse_s3_url("s3://bkt").expect("is s3").expect("parses");
        assert_eq!(u.bucket, "bkt");
        assert_eq!(u.key_prefix, "");
        assert_eq!(u.display(), "s3://bkt");

        // Trailing slashes normalize away.
        let u = parse_s3_url("s3://bkt/p/").expect("is s3").expect("parses");
        assert_eq!(u.key_prefix, "p");
    }

    #[test]
    fn parse_s3_url_none_for_non_s3_and_err_for_malformed() {
        assert!(parse_s3_url("/local/dir").is_none());
        // `s3` without the `:` scheme delimiter is an ordinary local
        // name, as is a near-miss scheme.
        assert!(parse_s3_url("s3").is_none());
        assert!(parse_s3_url("s39://bkt").is_none());
        // A single-slash `s3:/…` DOES carry the scheme: loud error (H1
        // — it used to fall through to the local-path behavior).
        let err = parse_s3_url("s3:/half")
            .expect("is s3")
            .expect_err("single-slash s3 URL is malformed");
        assert!(
            err.contains("s3://bucket") || err.contains("//"),
            "explains the shape: {err}"
        );
        let err = parse_s3_url("s3://")
            .expect("is s3")
            .expect_err("empty bucket");
        assert!(err.contains("bucket"), "names the bucket rule: {err}");
        let err = parse_s3_url("s3://bad bucket/x")
            .expect("is s3")
            .expect_err("a space in the bucket token is refused");
        assert!(err.contains("bucket"), "names the bucket rule: {err}");
    }

    #[test]
    fn parse_s3_url_scheme_is_case_insensitive_and_normalized() {
        // RFC 3986 §3.1: URL schemes are case-insensitive — `S3://…`
        // is ACCEPTED and normalized to the canonical `s3://…` form,
        // never treated as a local path (H1).
        let u = parse_s3_url("S3://bkt/druid/segments")
            .expect("uppercase scheme is still an s3 URL, never a local path")
            .expect("canonical body parses");
        assert_eq!(u.bucket, "bkt");
        assert_eq!(u.key_prefix, "druid/segments");
        assert_eq!(u.display(), "s3://bkt/druid/segments");
    }

    #[test]
    fn parse_s3_url_malformed_s3_scheme_is_loud_never_local() {
        // H1: every input carrying the s3 scheme that is NOT a
        // canonical `s3://bucket[/prefix]` must be a LOUD error.
        // `None` would fall through to the LOCAL-path behavior in
        // attach/assess — a typo'd URL could then silently attach
        // unrelated local segments sitting at that relative path.
        for bad in [
            "s3:/b/k", // single slash
            "S3:/b/k", // single slash, uppercase scheme
            "s3://",   // empty bucket
            "S3://",   // empty bucket, uppercase scheme
            "s3:///k", // empty bucket before a key
            "s3:b/k",  // no slashes at all
            "s3:",     // bare scheme
        ] {
            let res = parse_s3_url(bad).unwrap_or_else(|| {
                panic!("{bad:?} carries the s3 scheme — must never fall back to a local path")
            });
            let err = res.expect_err("malformed s3 URL must be a loud error");
            assert!(err.contains("s3"), "{bad:?} error names the s3 rule: {err}");
        }
    }

    #[test]
    fn s3_url_of_path_gates_the_local_fallback() {
        // attach/assess run their LOCAL-path branch only when this
        // returns `None`; any `Some(Err)` is a hard failure at the call
        // site. So `Some(Err)` here IS the assertion that no local path
        // is ever attempted for these inputs.
        for bad in ["s3:/half", "S3:/half", "s3://", "s3:///k"] {
            let res = s3_url_of_path(Path::new(bad)).unwrap_or_else(|| {
                panic!("{bad:?} must be recognized as (malformed) s3, not as a local path")
            });
            assert!(res.is_err(), "{bad:?} must be a loud error");
        }
        let u = s3_url_of_path(Path::new("S3://bkt/p"))
            .expect("is s3")
            .expect("parses");
        assert_eq!(u.display(), "s3://bkt/p");
        assert!(s3_url_of_path(Path::new("/local/dir")).is_none());
        assert!(s3_url_of_path(Path::new("relative/dir")).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn s3_url_of_path_non_utf8_s3_scheme_is_loud() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt as _;
        // Non-UTF-8 bytes that still open with `s3://` cannot be a
        // valid s3 URL — but they must not silently become a local
        // path either.
        let res = s3_url_of_path(Path::new(OsStr::from_bytes(b"s3://bkt/\xFF")))
            .expect("s3-scheme bytes are never a local path");
        assert!(res.is_err(), "non-UTF-8 s3 URL must be refused loudly");
        // Non-UTF-8 WITHOUT the scheme stays a local path.
        assert!(s3_url_of_path(Path::new(OsStr::from_bytes(b"/loc/\xFFal"))).is_none());
    }

    #[test]
    fn validate_bucket_name_rules() {
        for ok in ["bkt", "my-bucket.prod", "B_2", "ferrodruid-compat"] {
            assert!(validate_bucket_name(ok).is_ok(), "{ok:?} should pass");
        }
        for bad in ["", ".", "..", "a/b", "a b", "a\0b", "a\\b", "s3://x"] {
            assert!(
                validate_bucket_name(bad).is_err(),
                "{bad:?} must be refused"
            );
        }
    }

    // -- plan_downloads: prefix strip + validation ---------------------------

    #[test]
    fn plan_strips_prefix_and_keeps_relative_paths() {
        let plan = plan_downloads(
            &keys(&[
                "druid/segments/wiki/iv/v1/0/index.zip",
                "druid/segments/wiki/iv/v1/0/descriptor.json",
            ]),
            "druid/segments",
            None,
            None,
        )
        .expect("plan");
        assert_eq!(
            rels(&plan),
            vec![
                "wiki/iv/v1/0/descriptor.json".to_string(),
                "wiki/iv/v1/0/index.zip".to_string(),
            ]
        );
        assert_eq!(plan.filtered, 0);
        assert!(!plan.truncated);
    }

    #[test]
    fn plan_refuses_traversal_and_control_components() {
        for bad in ["p/../etc/passwd", "p/./x", "p/a\\b/x", "p/evil\u{7}/x"] {
            let err = plan_downloads(&keys(&[bad]), "p", None, None)
                .expect_err("hostile key must fail the plan");
            assert!(err.contains("unsafe S3 key"), "names the unsafe key: {err}");
        }
    }

    #[test]
    fn plan_refuses_object_at_the_prefix_itself() {
        let err = plan_downloads(&keys(&["druid/segments"]), "druid/segments", None, None)
            .expect_err("an object AT the prefix cannot be mirrored");
        assert!(err.contains("AT the prefix"), "explains the shape: {err}");
    }

    /// Defensive planner re-check of the SHARED
    /// [`ferrodruid_deep_storage::MAX_S3_KEYS`] cap: unreachable through
    /// the real pipeline (`list_objects` aborts the listing stream at
    /// cap+1 with the same constant, so an over-cap slice can never be
    /// listed), but a caller handing the planner an oversized slice
    /// directly must still be refused loudly.
    #[test]
    fn plan_defensively_refuses_an_over_cap_key_slice() {
        let oversized: Vec<String> = (0..=MAX_S3_KEYS).map(|i| format!("p/k{i}")).collect();
        let err = plan_downloads(&oversized, "p", None, None)
            .expect_err("an over-cap slice must be refused");
        assert!(
            err.contains(&MAX_S3_KEYS.to_string()) && err.contains("cap"),
            "the refusal names the shared cap: {err}"
        );
    }

    // -- plan_downloads: --datasource at listing time ------------------------

    #[test]
    fn plan_datasource_filter_skips_only_layout_matching_keys() {
        let plan = plan_downloads(
            &keys(&[
                // Layout-matching other datasource: filtered.
                "p/clicks/iv/v1/0/index.zip",
                // The wanted datasource: kept.
                "p/wiki/iv/v1/0/index.zip",
                // Too shallow to be judged at listing time: kept (the
                // local scan decides).
                "p/other/somezip/index.zip",
            ]),
            "p",
            Some("wiki"),
            None,
        )
        .expect("plan");
        assert_eq!(
            rels(&plan),
            vec![
                "other/somezip/index.zip".to_string(),
                "wiki/iv/v1/0/index.zip".to_string(),
            ]
        );
        assert_eq!(plan.filtered, 1);
    }

    // -- plan_downloads: --max-segments group bound --------------------------

    #[test]
    fn plan_max_segments_stops_after_whole_artifact_groups() {
        let plan = plan_downloads(
            &keys(&[
                "p/wiki/iv/v1/0/index.zip",
                "p/wiki/iv/v1/0/descriptor.json",
                "p/wiki/iv/v1/1/index.zip",
                "p/wiki/iv/v1/2/index.zip",
            ]),
            "p",
            None,
            Some(1),
        )
        .expect("plan");
        assert_eq!(
            rels(&plan),
            vec![
                "wiki/iv/v1/0/descriptor.json".to_string(),
                "wiki/iv/v1/0/index.zip".to_string(),
            ],
            "the first artifact group is downloaded WHOLE, later groups not at all"
        );
        assert!(plan.truncated, "the early stop is reported");
    }

    #[test]
    fn plan_max_segments_never_splits_a_smoosh_group() {
        // A raw smoosh dir's files (00000.smoosh, meta.smoosh,
        // version.bin — meta.smoosh is NOT last lexically) must stay
        // together under the bound.
        let plan = plan_downloads(
            &keys(&[
                "p/wiki/iv/v1/0/index/00000.smoosh",
                "p/wiki/iv/v1/0/index/meta.smoosh",
                "p/wiki/iv/v1/0/index/version.bin",
                "p/wiki/iv/v1/1/index/00000.smoosh",
                "p/wiki/iv/v1/1/index/meta.smoosh",
                "p/wiki/iv/v1/1/index/version.bin",
            ]),
            "p",
            None,
            Some(1),
        )
        .expect("plan");
        assert_eq!(
            rels(&plan),
            vec![
                "wiki/iv/v1/0/index/00000.smoosh".to_string(),
                "wiki/iv/v1/0/index/meta.smoosh".to_string(),
                "wiki/iv/v1/0/index/version.bin".to_string(),
            ],
            "the whole first smoosh group survives, incl. files sorting AFTER meta.smoosh"
        );
        assert!(plan.truncated);
    }

    #[test]
    fn plan_groups_dirs_componentwise_not_stringwise() {
        // As plain STRINGS `wiki/iv-x/…` sorts BETWEEN the `wiki/iv/…`
        // keys (`-` < `/`), which would interleave the two directory
        // groups and fool the group bound. Component-wise sorting puts
        // the whole `iv` group first (`"iv"` < `"iv-x"`), so the
        // `--max-segments 1` bound takes exactly that group.
        let plan = plan_downloads(
            &keys(&[
                "p/wiki/iv/v1/0/index.zip",
                "p/wiki/iv-x/v1/0/index.zip",
                "p/wiki/iv/v1/0/descriptor.json",
            ]),
            "p",
            None,
            Some(1),
        )
        .expect("plan");
        assert_eq!(
            rels(&plan),
            vec![
                "wiki/iv/v1/0/descriptor.json".to_string(),
                "wiki/iv/v1/0/index.zip".to_string(),
            ],
            "the contiguous `iv` group is taken whole; `iv-x` is beyond the bound"
        );
        assert!(plan.truncated);
    }

    // -- list→fetch address identity -----------------------------------------

    /// Every planned download's `source` maps back to the ORIGINATING
    /// listed key — through filtering, component-wise re-sorting and
    /// the group bound — so the fetch loop downloads the exact listed
    /// object each plan entry was computed from (wrong-object
    /// substitution guard: the fetch address is never re-derived from
    /// the rel string).
    #[test]
    fn plan_download_sources_map_back_to_the_originating_listed_key() {
        let listing = keys(&[
            "p/wiki/iv-x/v1/0/index.zip",
            "p/clicks/iv/v1/0/index.zip", // filtered by --datasource
            "p/wiki/iv/v1/0/index.zip",
            "p/wiki/iv/v1/0/descriptor.json",
        ]);
        let plan = plan_downloads(&listing, "p", Some("wiki"), None).expect("plan");
        assert_eq!(plan.filtered, 1);
        assert!(!plan.downloads.is_empty());
        for planned in &plan.downloads {
            let origin = listing
                .get(planned.source)
                .expect("source index stays within the listing slice");
            assert_eq!(
                origin,
                &format!("p/{}", planned.rel),
                "planned rel {:?} must come FROM listed key {:?} (source {})",
                planned.rel,
                origin,
                planned.source
            );
        }
    }

    /// The staging pipeline over a real in-process `object_store`
    /// backend: everything listed is fetched BY the listed object and
    /// the staged bytes are byte-identical to the planted objects.
    #[tokio::test]
    async fn stage_s3_tree_stages_exactly_the_enumerated_objects() {
        use ferrodruid_deep_storage::DeepStorage as _;

        let client = S3DeepStorage::with_store(
            Box::new(object_store::memory::InMemory::new()),
            String::new(),
        );
        // Plant via the product upload path: objects land under
        // `wiki/seg_1/<file>`.
        let src = tempfile::tempdir().expect("src dir");
        std::fs::write(src.path().join("index.zip"), b"ZIP-BYTES-EXACT").expect("write");
        std::fs::write(src.path().join("descriptor.json"), b"{\"d\":1}").expect("write");
        client
            .upload_segment("wiki", "seg_1", src.path())
            .await
            .expect("upload fixture objects");

        let url = S3SourceUrl {
            bucket: "mem".to_string(),
            key_prefix: String::new(),
        };
        let staged = stage_s3_tree_with_client(&client, &url, None, None)
            .await
            .expect("staging over the in-process store succeeds");
        assert_eq!(staged.objects_downloaded, 2);
        assert_eq!(
            std::fs::read(staged.root.join("wiki/seg_1/index.zip")).expect("read staged zip"),
            b"ZIP-BYTES-EXACT",
            "staged bytes are the enumerated object's, byte-identical"
        );
        assert_eq!(
            std::fs::read(staged.root.join("wiki/seg_1/descriptor.json"))
                .expect("read staged descriptor"),
            b"{\"d\":1}",
        );
    }
}
