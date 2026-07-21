// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Metadata store URI parsing (`MetadataStore::connect` dispatch).

use ferrodruid_common::{DruidError, Result};

/// The parsed form of a metadata store URI or path, as accepted by
/// [`MetadataStore::connect`](crate::MetadataStore::connect).
///
/// Accepted spellings:
///
/// - `postgres://…` / `postgresql://…` → [`MetadataUri::Postgres`]
/// - `mysql://…` → [`MetadataUri::MySql`]
/// - `sqlite://<path>` (e.g. `sqlite:///var/lib/ferrodruid/meta.db`),
///   a bare filesystem path, → [`MetadataUri::SqlitePath`]
/// - `:memory:`, `sqlite::memory:`, `sqlite://:memory:` →
///   [`MetadataUri::SqliteMemory`]
///
/// Any OTHER `scheme://` prefix — including a MALFORMED one whose
/// prefix is not a syntactically valid scheme, such as Druid's
/// `jdbc:postgresql://…` connectURI spelling — is rejected loudly
/// instead of being treated as a relative SQLite filename (the
/// historical name-trap: `--metadata-uri postgres://…` used to create a
/// SQLite file literally named `postgres://…`).  Only a string with no
/// `://` at all is a bare SQLite path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataUri {
    /// A PostgreSQL URI; connect with the ORIGINAL, unmodified URI (so
    /// query parameters such as `?sslmode=` pass through to the driver).
    Postgres,
    /// A MySQL URI; connect with the original, unmodified URI.
    MySql,
    /// An in-memory SQLite database.
    SqliteMemory,
    /// A SQLite database file at this filesystem path.
    SqlitePath(String),
}

/// Parse a metadata store URI or bare path. See [`MetadataUri`] for the
/// accepted forms.
///
/// # Errors
///
/// Returns [`DruidError::Metadata`] for an empty input, an unsupported
/// `scheme://` prefix, or a malformed URI whose text before `://` is
/// not a valid scheme (credentials redacted in the error).
pub fn parse_metadata_uri(uri: &str) -> Result<MetadataUri> {
    let uri = uri.trim();
    if uri.is_empty() {
        return Err(DruidError::Metadata("metadata URI is empty".into()));
    }
    if uri == ":memory:" || uri.eq_ignore_ascii_case("sqlite::memory:") {
        return Ok(MetadataUri::SqliteMemory);
    }
    let Some(idx) = uri.find("://") else {
        // No scheme — a bare SQLite file path (the historical default).
        return Ok(MetadataUri::SqlitePath(uri.to_string()));
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
        // "://" appeared but the prefix is not a valid URI scheme (e.g.
        // Druid's own `jdbc:postgresql://…` connectURI spelling, or a
        // stray space in `postgres ://…`) — a MALFORMED URI, never a
        // file path.  Falling back to a path here would hand the whole
        // string to `new_sqlite` (`create_if_missing(true)`): silent
        // wrong-backend metadata placement, with any embedded credential
        // persisted in the created file's NAME (re-audit 2026-07-19;
        // mirrors `parse_source_uri` in `ferrodruid-metadata-schema`).
        // Reject loudly, credentials redacted.
        return Err(DruidError::Metadata(format!(
            "malformed metadata URI {} — the text before '://' is not a valid URI scheme \
             (use postgres://…, postgresql://…, mysql://…, sqlite://<path>, or a bare \
             SQLite file path with no '://'; drop any 'jdbc:' prefix from a Druid \
             connectURI)",
            redact_metadata_uri(uri)
        )));
    }
    match scheme.to_ascii_lowercase().as_str() {
        "postgres" | "postgresql" => Ok(MetadataUri::Postgres),
        "mysql" => Ok(MetadataUri::MySql),
        "sqlite" => {
            if rest.is_empty() || rest == ":memory:" {
                Ok(MetadataUri::SqliteMemory)
            } else {
                Ok(MetadataUri::SqlitePath(rest.to_string()))
            }
        }
        other => Err(DruidError::Metadata(format!(
            "unsupported metadata URI scheme '{other}://' (supported: sqlite://<path>, \
             postgres://…, postgresql://…, mysql://…; a bare path or ':memory:' selects SQLite)"
        ))),
    }
}

/// The value of an ASCII hex digit, if `b` is one.
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Whether a RAW query key names the `password` parameter once
/// percent-decoded, ASCII-case-insensitively.
///
/// Drivers (sqlx included) percent-DECODE query keys before matching
/// them, so `pass%77ord=secret` reaches the driver as `password=secret`
/// — comparing the raw spelling would let the secret through unredacted
/// (compat-6 Codex H2). Decoding happens ONCE, mirroring the driver: a
/// double-encoded `pass%2577ord` decodes to `pass%77ord`, which is NOT
/// the `password` key to the driver and is left alone. An invalid
/// escape (`%zz`, truncated `%f`) is kept verbatim, byte for byte.
fn is_password_key(raw_key: &str) -> bool {
    let bytes = raw_key.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while let Some(&b) = bytes.get(i) {
        let escape = if b == b'%' {
            match (
                bytes.get(i + 1).copied().and_then(hex_val),
                bytes.get(i + 2).copied().and_then(hex_val),
            ) {
                (Some(hi), Some(lo)) => Some((hi << 4) | lo),
                _ => None,
            }
        } else {
            None
        };
        match escape {
            Some(d) => {
                decoded.push(d);
                i += 3;
            }
            None => {
                decoded.push(b);
                i += 1;
            }
        }
    }
    decoded.eq_ignore_ascii_case(b"password")
}

/// Render a metadata URI or path safe for logs, reports and error text.
///
/// `postgres://user:secret@host/db` and `mysql://…` URIs embed
/// credentials; printing them verbatim leaks the password (and the
/// username) into stdout, log files and shell history. This masks the
/// entire authority userinfo (`user:password@` → `***@`) and the value
/// of any `password` query parameter (libpq-style URIs accept the
/// password outside the userinfo) — matched on the PERCENT-DECODED,
/// case-folded key, exactly as the driver matches it, so encoded
/// spellings like `pass%77ord` cannot smuggle the secret past the
/// redaction (see [`is_password_key`]). Bare SQLite paths and
/// `:memory:` carry no credentials and pass through unchanged.
///
/// Redaction is purely syntactic: it never fails, and it masks even
/// URIs that [`parse_metadata_uri`] would reject — a helper that only
/// redacted *valid* URIs could still leak the secret in an error path.
#[must_use]
pub fn redact_metadata_uri(uri: &str) -> String {
    let Some(scheme_end) = uri.find("://") else {
        // Bare SQLite path or `:memory:` — no credential surface.
        return uri.to_string();
    };
    let after_scheme = &uri[scheme_end + 3..];
    let authority_end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let authority = &after_scheme[..authority_end];

    let mut out = String::with_capacity(uri.len());
    out.push_str(&uri[..scheme_end + 3]);
    // Mask the userinfo wholesale (username included — it is routinely
    // sensitive too). `rfind`: a literal '@' inside the password must
    // not truncate the host.
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
                    Some((key, _)) if is_password_key(key) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_path_is_sqlite() {
        assert_eq!(
            parse_metadata_uri("/var/lib/ferrodruid/metadata/ferrodruid.db").expect("parse"),
            MetadataUri::SqlitePath("/var/lib/ferrodruid/metadata/ferrodruid.db".to_string())
        );
        assert_eq!(
            parse_metadata_uri("relative/dir/meta.db").expect("parse"),
            MetadataUri::SqlitePath("relative/dir/meta.db".to_string())
        );
    }

    #[test]
    fn sqlite_scheme_strips_prefix() {
        assert_eq!(
            parse_metadata_uri("sqlite:///abs/path/meta.db").expect("parse"),
            MetadataUri::SqlitePath("/abs/path/meta.db".to_string())
        );
        assert_eq!(
            parse_metadata_uri("sqlite://rel/meta.db").expect("parse"),
            MetadataUri::SqlitePath("rel/meta.db".to_string())
        );
    }

    #[test]
    fn memory_forms() {
        assert_eq!(
            parse_metadata_uri(":memory:").expect("parse"),
            MetadataUri::SqliteMemory
        );
        assert_eq!(
            parse_metadata_uri("sqlite::memory:").expect("parse"),
            MetadataUri::SqliteMemory
        );
        assert_eq!(
            parse_metadata_uri("sqlite://:memory:").expect("parse"),
            MetadataUri::SqliteMemory
        );
        assert_eq!(
            parse_metadata_uri("sqlite://").expect("parse"),
            MetadataUri::SqliteMemory
        );
    }

    #[test]
    fn postgres_schemes() {
        assert_eq!(
            parse_metadata_uri("postgres://u:p@host:5432/ferrodruid").expect("parse"),
            MetadataUri::Postgres
        );
        assert_eq!(
            parse_metadata_uri("postgresql://host/db?sslmode=require").expect("parse"),
            MetadataUri::Postgres
        );
        // Schemes are case-insensitive per RFC 3986.
        assert_eq!(
            parse_metadata_uri("POSTGRES://host/db").expect("parse"),
            MetadataUri::Postgres
        );
    }

    #[test]
    fn mysql_scheme() {
        assert_eq!(
            parse_metadata_uri("mysql://u:p@host:3306/ferrodruid").expect("parse"),
            MetadataUri::MySql
        );
    }

    #[test]
    fn unknown_scheme_fails_loud_not_as_filename() {
        // The name-trap: this must NOT become a SQLite file named
        // "postgress://…".
        let err = parse_metadata_uri("postgress://host/db").expect_err("must reject");
        assert!(
            format!("{err}").contains("unsupported metadata URI scheme"),
            "unexpected error: {err}"
        );
        assert!(parse_metadata_uri("mariadb://host/db").is_err());
        assert!(parse_metadata_uri("jdbc://host/db").is_err());
    }

    #[test]
    fn empty_is_rejected() {
        assert!(parse_metadata_uri("").is_err());
        assert!(parse_metadata_uri("   ").is_err());
    }

    /// Re-audit 2026-07-19: a URI containing `://` whose prefix is NOT a
    /// syntactically valid scheme is a MALFORMED URI, never a SQLite file
    /// path.  Pre-fix it fell back to `SqlitePath(whole string)`, so
    /// `new_sqlite` (`create_if_missing(true)`) could CREATE a database
    /// file whose NAME embeds the credential and silently run against
    /// the wrong backend — re-opening the historical name-trap the enum
    /// doc claims is fixed.  Mirrors `parse_source_uri` in
    /// `ferrodruid-metadata-schema`, which already rejects this grammar.
    #[test]
    fn malformed_scheme_with_separator_is_rejected_redacted() {
        // Druid's own connectURI spelling: the ':' makes the prefix an
        // invalid scheme.
        let err = parse_metadata_uri("jdbc:postgresql://druid:s3cret@host:5432/druid")
            .expect_err("jdbc-prefixed URI must be rejected, not treated as a file path");
        let msg = format!("{err}");
        assert!(
            msg.contains("not a valid URI scheme"),
            "unexpected error: {msg}"
        );
        assert!(
            !msg.contains("s3cret"),
            "credential must be redacted from the error: {msg}"
        );

        // A stray space before '://' (typo'd backend selector).
        assert!(parse_metadata_uri("postgres ://host/db").is_err());
        // A digit-led prefix is not a scheme either.
        assert!(parse_metadata_uri("1c://host/db").is_err());
    }

    #[test]
    fn redaction_masks_credentials() {
        // Userinfo (username AND password) is masked; host, port, db
        // and non-secret query parameters survive.
        assert_eq!(
            redact_metadata_uri(
                "postgres://druid:s3cret@db.example:5432/ferrodruid?sslmode=require"
            ),
            "postgres://***@db.example:5432/ferrodruid?sslmode=require"
        );
        assert_eq!(
            redact_metadata_uri("mysql://root:hunter2@localhost:3306/ferrodruid"),
            "mysql://***@localhost:3306/ferrodruid"
        );
        // libpq-style: the password as a query parameter, no userinfo.
        assert_eq!(
            redact_metadata_uri(
                "postgresql://db.example/ferrodruid?password=s3cret&sslmode=require"
            ),
            "postgresql://db.example/ferrodruid?password=***&sslmode=require"
        );
        // A literal '@' inside the password must not leak part of it or
        // truncate the host.
        assert_eq!(
            redact_metadata_uri("postgres://u:p@ss@db.example/ferrodruid"),
            "postgres://***@db.example/ferrodruid"
        );
    }

    #[test]
    fn redaction_masks_encoded_password_key_spellings() {
        // compat-6 Codex H2: sqlx percent-DECODES query keys, so
        // `pass%77ord` reaches the driver as `password` — comparing the
        // RAW key would let the secret through unredacted.
        assert_eq!(
            redact_metadata_uri(
                "postgresql://db.example/ferrodruid?pass%77ord=s3cret&sslmode=require"
            ),
            "postgresql://db.example/ferrodruid?pass%77ord=***&sslmode=require"
        );
        // Fully percent-encoded spelling.
        assert_eq!(
            redact_metadata_uri(
                "postgresql://db.example/ferrodruid?%70%61%73%73%77%6f%72%64=s3cret"
            ),
            "postgresql://db.example/ferrodruid?%70%61%73%73%77%6f%72%64=***"
        );
        // Case variants (raw and percent-encoded).
        assert_eq!(
            redact_metadata_uri("postgresql://db.example/ferrodruid?PASSWORD=s3cret"),
            "postgresql://db.example/ferrodruid?PASSWORD=***"
        );
        assert_eq!(
            redact_metadata_uri("postgresql://db.example/ferrodruid?pass%57ord=s3cret"),
            "postgresql://db.example/ferrodruid?pass%57ord=***"
        );
        assert_eq!(
            redact_metadata_uri("postgresql://db.example/ferrodruid?password=s3cret"),
            "postgresql://db.example/ferrodruid?password=***"
        );
        // A non-credential parameter is left intact, even percent-encoded
        // (`%73slmode` decodes to `sslmode`, not `password`).
        assert_eq!(
            redact_metadata_uri(
                "postgresql://db.example/ferrodruid?application_name=fd&%73slmode=require"
            ),
            "postgresql://db.example/ferrodruid?application_name=fd&%73slmode=require"
        );
        // A key that only decodes to `password` after a SECOND decode is
        // NOT the `password` key to the driver (sqlx decodes once) and
        // must not be masked.
        assert_eq!(
            redact_metadata_uri("postgresql://db.example/ferrodruid?pass%2577ord=x"),
            "postgresql://db.example/ferrodruid?pass%2577ord=x"
        );
    }

    #[test]
    fn redaction_leaves_credential_free_forms_unchanged() {
        // Bare SQLite paths and `:memory:` carry no credentials.
        assert_eq!(
            redact_metadata_uri("/var/lib/ferrodruid/metadata/ferrodruid.db"),
            "/var/lib/ferrodruid/metadata/ferrodruid.db"
        );
        assert_eq!(redact_metadata_uri(":memory:"), ":memory:");
        assert_eq!(
            redact_metadata_uri("sqlite:///var/lib/ferrodruid/meta.db"),
            "sqlite:///var/lib/ferrodruid/meta.db"
        );
        // A server URI without userinfo or a password parameter is
        // already safe.
        assert_eq!(
            redact_metadata_uri("postgres://db.example/ferrodruid"),
            "postgres://db.example/ferrodruid"
        );
    }
}
