// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Aggregation and rendering of the compatibility report.
//!
//! Shapes are grouped by their canonical (literal-stripped) form; each
//! distinct shape is classified once, using its first-seen query as the
//! exemplar. The report gives both a shape-based and a frequency-weighted
//! compatibility percentage, so one hot dashboard query counts once in the
//! former and by its true weight in the latter.
//!
//! Privacy: with redaction on (the default) the report contains only
//! shapes — literal values were already stripped by [`crate::shape`] — and
//! classification reasons pass through the same literal masking (parse
//! errors can echo query fragments). `--no-redact` additionally includes
//! each shape's first-seen query verbatim; even then the tool only ever
//! emits query text, never any table data (it reads none).

use std::collections::HashMap;
use std::io::{BufRead, Read};

use serde_json::Value;

use crate::classify::{Bucket, Classification, classify_native, classify_sql};
use crate::input::{LineOutcome, QueryPayload, parse_line};
use crate::shape::{native_shape, sql_shape};

/// Maximum accepted log-line length in bytes. Longer lines are counted as
/// oversized and skipped without being parsed, so a single pathological
/// line (e.g. a hundreds-of-MiB `"query"`) can never drive unbounded
/// allocation. Real Druid request-log lines are orders of magnitude
/// smaller; 8 MiB leaves generous headroom for giant `IN` lists.
pub const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;

/// Where a log record came from, for exclusion accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RecordOrigin {
    /// A query a client sent on the wire — counted in the percentages.
    Client,
    /// A broker→data-node fan-out sub-query (segment-pinned intervals).
    FanOut,
    /// The broker's Calcite lowering of a SQL request that is also in the
    /// log as a SQL line.
    SqlLowered,
}

/// Whether a shape came from a SQL or native query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryKind {
    /// Druid SQL (`/druid/v2/sql`).
    Sql,
    /// Native JSON (`/druid/v2`).
    Native,
}

/// One distinct query shape with its classification and frequency.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShapeStat {
    /// SQL or native.
    pub kind: QueryKind,
    /// The canonical literal-stripped shape.
    pub shape: String,
    /// Number of log records with this shape.
    pub count: u64,
    /// Where the records came from; only [`RecordOrigin::Client`] shapes
    /// count toward the compatibility percentages.
    pub origin: RecordOrigin,
    /// The classification of this shape's exemplar query.
    #[serde(flatten)]
    pub classification: Classification,
    /// First-seen raw query text (only populated with `--no-redact`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exemplar: Option<String>,
}

/// Line/record counters for the input log.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct InputStats {
    /// Total lines read.
    pub lines: u64,
    /// Client-workload query records (SQL + native, non-internal).
    pub query_records: u64,
    /// Cluster-internal fan-out records (segment-pinned sub-queries).
    pub internal_records: u64,
    /// Broker-generated SQL lowerings (native duplicates of SQL lines).
    pub sql_lowered_records: u64,
    /// Emitter-format lines (unsupported input format, skipped cleanly).
    pub emitter_lines: u64,
    /// Lines carrying no query (blank, stats-only, unrecognized).
    pub non_query_lines: u64,
    /// Lines longer than [`MAX_LINE_BYTES`], skipped without parsing.
    pub oversized_lines: u64,
}

/// Shape/record tallies for one bucket.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct BucketTally {
    /// Distinct shapes in this bucket.
    pub shapes: u64,
    /// Log records (frequency-weighted) in this bucket.
    pub records: u64,
}

/// The assembled compatibility report.
#[derive(Debug, serde::Serialize)]
pub struct Report {
    /// Input counters.
    pub input: InputStats,
    /// Tally of supported shapes/records (client workload only).
    pub supported: BucketTally,
    /// Tally of fail-closed shapes/records.
    pub fail_closed: BucketTally,
    /// Tally of unsupported shapes/records.
    pub unsupported: BucketTally,
    /// Shape-based compatible percentage (supported shapes / all
    /// client-workload shapes), `None` when the log had no queries.
    pub compatible_pct_shapes: Option<f64>,
    /// Frequency-weighted compatible percentage (supported records / all
    /// client-workload records).
    pub compatible_pct_records: Option<f64>,
    /// Every distinct client-workload shape, most frequent first.
    pub shapes: Vec<ShapeStat>,
    /// Distinct excluded shapes — fan-out sub-queries and SQL lowerings
    /// (informational).
    pub excluded_shapes: Vec<ShapeStat>,
}

/// Streaming analyzer: feed log lines, then [`Analyzer::finish`].
#[derive(Debug, Default)]
pub struct Analyzer {
    stats: InputStats,
    /// shape string → index into `shapes`.
    index: HashMap<String, usize>,
    shapes: Vec<ShapeStat>,
    keep_exemplars: bool,
}

impl Analyzer {
    /// Create an analyzer. `keep_exemplars` retains each shape's
    /// first-seen raw query for `--no-redact` reports.
    pub fn new(keep_exemplars: bool) -> Self {
        Self {
            keep_exemplars,
            ..Self::default()
        }
    }

    /// Ingest one log line. Lines longer than [`MAX_LINE_BYTES`] are
    /// counted as oversized and skipped without parsing (see
    /// [`feed_reader`] for the streaming path that never even buffers
    /// them).
    pub fn add_line(&mut self, line: &str) {
        if line.len() > MAX_LINE_BYTES {
            self.add_oversized_line();
            return;
        }
        self.stats.lines += 1;
        match parse_line(line) {
            LineOutcome::Query(payload) => {
                self.stats.query_records += 1;
                self.add_payload(&payload, RecordOrigin::Client);
            }
            LineOutcome::Internal(payload) => {
                self.stats.internal_records += 1;
                self.add_payload(&payload, RecordOrigin::FanOut);
            }
            LineOutcome::SqlLowered(payload) => {
                self.stats.sql_lowered_records += 1;
                self.add_payload(&payload, RecordOrigin::SqlLowered);
            }
            LineOutcome::EmitterFormat => self.stats.emitter_lines += 1,
            LineOutcome::NotAQuery => self.stats.non_query_lines += 1,
        }
    }

    /// Count a line that exceeded [`MAX_LINE_BYTES`] and was skipped
    /// unread.
    pub fn add_oversized_line(&mut self) {
        self.stats.lines += 1;
        self.stats.oversized_lines += 1;
    }

    fn add_payload(&mut self, payload: &QueryPayload, origin: RecordOrigin) {
        let (kind, shape) = match payload {
            QueryPayload::Sql(sql) => (QueryKind::Sql, sql_shape(sql)),
            QueryPayload::Native(v) => (QueryKind::Native, native_shape(v)),
        };
        // Excluded records and client queries are tallied apart even if a
        // shape string collides across the groups.
        let key = if origin == RecordOrigin::Client {
            shape.clone()
        } else {
            format!("excluded\u{0}{shape}")
        };
        if let Some(&i) = self.index.get(&key) {
            self.shapes[i].count += 1;
            return;
        }
        let classification = match payload {
            QueryPayload::Sql(sql) => classify_sql(sql),
            QueryPayload::Native(v) => classify_native(v),
        };
        let exemplar = if self.keep_exemplars {
            Some(match payload {
                QueryPayload::Sql(sql) => sql.clone(),
                QueryPayload::Native(v) => v.to_string(),
            })
        } else {
            None
        };
        self.index.insert(key, self.shapes.len());
        self.shapes.push(ShapeStat {
            kind,
            shape,
            count: 1,
            origin,
            classification,
            exemplar,
        });
    }

    /// Assemble the final report.
    pub fn finish(self) -> Report {
        let mut shapes: Vec<ShapeStat> = Vec::new();
        let mut excluded_shapes: Vec<ShapeStat> = Vec::new();
        for s in self.shapes {
            if s.origin == RecordOrigin::Client {
                shapes.push(s);
            } else {
                excluded_shapes.push(s);
            }
        }
        let by_freq =
            |a: &ShapeStat, b: &ShapeStat| b.count.cmp(&a.count).then(a.shape.cmp(&b.shape));
        shapes.sort_by(by_freq);
        excluded_shapes.sort_by(by_freq);

        let mut supported = BucketTally::default();
        let mut fail_closed = BucketTally::default();
        let mut unsupported = BucketTally::default();
        for s in &shapes {
            let tally = match s.classification.bucket {
                Bucket::Supported => &mut supported,
                Bucket::FailClosed => &mut fail_closed,
                Bucket::Unsupported => &mut unsupported,
            };
            tally.shapes += 1;
            tally.records += s.count;
        }
        let total_shapes = supported.shapes + fail_closed.shapes + unsupported.shapes;
        let total_records = supported.records + fail_closed.records + unsupported.records;
        let pct = |part: u64, whole: u64| {
            if whole == 0 {
                None
            } else {
                #[allow(clippy::cast_precision_loss)] // report percentages
                Some(part as f64 * 100.0 / whole as f64)
            }
        };
        Report {
            input: self.stats,
            supported,
            fail_closed,
            unsupported,
            compatible_pct_shapes: pct(supported.shapes, total_shapes),
            compatible_pct_records: pct(supported.records, total_records),
            shapes,
            excluded_shapes,
        }
    }
}

/// Feed every line of `reader` into the analyzer without ever buffering
/// more than [`MAX_LINE_BYTES`] of a single line: once a line exceeds the
/// limit its bytes are discarded as they stream past and the line is
/// counted as oversized (never parsed).
///
/// Lines that are not valid UTF-8 are fed lossily (Druid logs are UTF-8;
/// this keeps a stray corrupt byte from aborting a multi-GB scan).
///
/// # Errors
///
/// Returns a message describing the I/O error if reading fails.
pub fn feed_reader<R: Read>(analyzer: &mut Analyzer, reader: R) -> Result<(), String> {
    let mut reader = std::io::BufReader::new(reader);
    let mut buf: Vec<u8> = Vec::new();
    let mut oversized = false;
    loop {
        let chunk = match reader.fill_buf() {
            Ok(chunk) => chunk,
            Err(e) => return Err(format!("read error: {e}")),
        };
        if chunk.is_empty() {
            // EOF: flush a final unterminated line, if any.
            if oversized {
                analyzer.add_oversized_line();
            } else if !buf.is_empty() {
                let line = String::from_utf8_lossy(&buf);
                analyzer.add_line(line.trim_end_matches('\r'));
            }
            return Ok(());
        }
        let (consumed, found_newline) = match chunk.iter().position(|&b| b == b'\n') {
            Some(i) => (i + 1, true),
            None => (chunk.len(), false),
        };
        // Line content in this chunk, excluding the newline itself.
        let content = consumed - usize::from(found_newline);
        if !oversized {
            if buf.len() + content > MAX_LINE_BYTES {
                oversized = true;
                buf.clear();
            } else {
                buf.extend_from_slice(&chunk[..content]);
            }
        }
        reader.consume(consumed);
        if found_newline {
            if oversized {
                analyzer.add_oversized_line();
            } else {
                let line = String::from_utf8_lossy(&buf);
                analyzer.add_line(line.trim_end_matches('\r'));
            }
            buf.clear();
            oversized = false;
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Render the report as JSON. `redact` masks literal fragments echoed in
/// classification reasons.
///
/// # Errors
///
/// Returns a `serde_json` error if serialization fails (not expected for
/// this data model).
pub fn render_json(report: &Report, redact: bool) -> Result<String, serde_json::Error> {
    let mut v = serde_json::to_value(report)?;
    if redact {
        redact_reasons(&mut v);
    }
    serde_json::to_string_pretty(&v)
}

/// Mask literal fragments inside every `reason` field of a serialized
/// report.
fn redact_reasons(v: &mut Value) {
    match v {
        Value::Object(map) => {
            for (k, val) in map.iter_mut() {
                if k == "reason" {
                    if let Value::String(s) = val {
                        *val = Value::String(redact_fragment(s));
                    }
                } else {
                    redact_reasons(val);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_reasons(item);
            }
        }
        _ => {}
    }
}

/// Mask literals in an error-message fragment (parse errors can echo
/// query text): reuses the SQL literal masker.
fn redact_fragment(s: &str) -> String {
    sql_shape(s)
}

/// Render the report as Markdown.
pub fn render_markdown(report: &Report, top: usize, redact: bool) -> String {
    use std::fmt::Write as _;
    let mut md = String::new();
    let _ = writeln!(md, "# FerroDruid request-log compatibility report");
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "Generated by `ferro-logcompat` — static parse + plan classification \
         of a Druid request log. Nothing was executed and no data was read; \
         see the crate README for the privacy design."
    );
    let _ = writeln!(md);

    let _ = writeln!(md, "## Input");
    let _ = writeln!(md);
    let _ = writeln!(md, "| metric | value |");
    let _ = writeln!(md, "|---|---:|");
    let s = &report.input;
    let _ = writeln!(md, "| log lines read | {} |", s.lines);
    let _ = writeln!(md, "| client query records | {} |", s.query_records);
    let _ = writeln!(
        md,
        "| cluster-internal fan-out records (excluded) | {} |",
        s.internal_records
    );
    let _ = writeln!(
        md,
        "| broker-generated SQL-lowering records (excluded) | {} |",
        s.sql_lowered_records
    );
    let _ = writeln!(
        md,
        "| emitter-format lines (unsupported input format) | {} |",
        s.emitter_lines
    );
    let _ = writeln!(md, "| non-query lines | {} |", s.non_query_lines);
    let _ = writeln!(
        md,
        "| oversized lines (skipped unread) | {} |",
        s.oversized_lines
    );
    let _ = writeln!(md);

    let _ = writeln!(md, "## Compatibility");
    let _ = writeln!(md);
    let _ = writeln!(md, "| bucket | shapes | records |");
    let _ = writeln!(md, "|---|---:|---:|");
    let _ = writeln!(
        md,
        "| supported | {} | {} |",
        report.supported.shapes, report.supported.records
    );
    let _ = writeln!(
        md,
        "| fail-closed | {} | {} |",
        report.fail_closed.shapes, report.fail_closed.records
    );
    let _ = writeln!(
        md,
        "| unsupported | {} | {} |",
        report.unsupported.shapes, report.unsupported.records
    );
    let _ = writeln!(md);
    match (report.compatible_pct_shapes, report.compatible_pct_records) {
        (Some(ps), Some(pr)) => {
            let _ = writeln!(
                md,
                "**Compatible: {ps:.1}% of distinct query shapes, {pr:.1}% of \
                 log records (frequency-weighted).**"
            );
        }
        _ => {
            let _ = writeln!(md, "**No client queries found in the log.**");
        }
    }
    let _ = writeln!(md);

    let incompatible: Vec<&ShapeStat> = report
        .shapes
        .iter()
        .filter(|s| s.classification.bucket != Bucket::Supported)
        .collect();
    let _ = writeln!(
        md,
        "## Top incompatible shapes ({} of {} shown, by frequency)",
        top.min(incompatible.len()),
        incompatible.len()
    );
    let _ = writeln!(md);
    if incompatible.is_empty() {
        let _ = writeln!(md, "None — every classified query shape plans through.");
    }
    for (i, s) in incompatible.iter().take(top).enumerate() {
        let bucket = match s.classification.bucket {
            Bucket::FailClosed => "fail-closed",
            Bucket::Unsupported => "unsupported",
            Bucket::Supported => "supported",
        };
        let kind = match s.kind {
            QueryKind::Sql => "SQL",
            QueryKind::Native => "native",
        };
        let _ = writeln!(
            md,
            "### {}. [{bucket}] {kind} — {} record(s)",
            i + 1,
            s.count
        );
        let _ = writeln!(md);
        if let Some(reason) = &s.classification.reason {
            let reason = if redact {
                redact_fragment(reason)
            } else {
                reason.clone()
            };
            let _ = writeln!(md, "Reason: {reason}");
            let _ = writeln!(md);
        }
        let fence_lang = match s.kind {
            QueryKind::Sql => "sql",
            QueryKind::Native => "json",
        };
        let _ = writeln!(md, "```{fence_lang}");
        let _ = writeln!(md, "{}", s.shape);
        let _ = writeln!(md, "```");
        if let Some(exemplar) = &s.exemplar {
            let _ = writeln!(md);
            let _ = writeln!(md, "First-seen query (verbatim, `--no-redact`):");
            let _ = writeln!(md);
            let _ = writeln!(md, "```{fence_lang}");
            let _ = writeln!(md, "{exemplar}");
            let _ = writeln!(md, "```");
        }
        let _ = writeln!(md);
    }

    let _ = writeln!(md, "## Notes");
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "- Classification is static (parse + plan through FerroDruid's \
         existing query path); `supported` means the query plans, not that \
         results were compared. Result-level replay diffing is Phase 2."
    );
    let _ = writeln!(
        md,
        "- Shapes group queries that differ only in literal values; every \
         literal is stripped, so this report contains no data values."
    );
    let _ = writeln!(
        md,
        "- Cluster-internal fan-out sub-queries (segment-pinned, emitted \
         broker→data node) are excluded from the percentages: FerroDruid \
         never receives them on the wire."
    );
    if report.input.sql_lowered_records > 0 {
        let _ = writeln!(
            md,
            "- Native queries whose context carries `sqlQueryId` are the \
             broker's own Calcite lowering of SQL requests already counted \
             as SQL lines; {} record(s) were set aside as duplicates \
             (FerroDruid plans SQL with its own planner).",
            report.input.sql_lowered_records
        );
    }
    if (report.input.internal_records > 0 || report.input.sql_lowered_records > 0)
        && !report.excluded_shapes.is_empty()
    {
        let _ = writeln!(
            md,
            "- {} excluded record(s) across {} shape(s) were set aside in total.",
            report.input.internal_records + report.input.sql_lowered_records,
            report.excluded_shapes.len()
        );
    }
    if report.input.emitter_lines > 0 {
        let _ = writeln!(
            md,
            "- {} line(s) look like Druid *emitter* request-log events — an \
             unsupported input format for this tool; use the file request \
             logger (`druid.request.logging.type=file`).",
            report.input.emitter_lines
        );
    }
    if report.input.oversized_lines > 0 {
        let _ = writeln!(
            md,
            "- {} line(s) exceeded the {} MiB per-line safety limit and \
             were skipped unread (not counted as queries).",
            report.input.oversized_lines,
            MAX_LINE_BYTES / (1024 * 1024)
        );
    }
    md
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql_line(sql: &str) -> String {
        format!(
            "2026-07-11T00:00:00.000Z\t127.0.0.1\t\t{{\"sqlQuery/time\":1,\"success\":true}}\t{}",
            serde_json::json!({ "query": sql, "context": {} })
        )
    }

    #[test]
    fn shapes_dedupe_and_weight() {
        let mut a = Analyzer::new(false);
        a.add_line(&sql_line("SELECT COUNT(*) FROM wiki WHERE lang = 'en'"));
        a.add_line(&sql_line("SELECT COUNT(*) FROM wiki WHERE lang = 'ja'"));
        a.add_line(&sql_line("SELECT COUNT(*) FROM wiki WHERE lang = 'de'"));
        a.add_line(&sql_line(
            "SELECT a.p FROM wiki a FULL OUTER JOIN wiki b ON a.p = b.p",
        ));
        let r = a.finish();
        assert_eq!(r.input.query_records, 4);
        assert_eq!(r.shapes.len(), 2, "3 literal variants must share a shape");
        assert_eq!(r.supported.shapes, 1);
        assert_eq!(r.supported.records, 3);
        assert_eq!(r.fail_closed.shapes, 1);
        assert_eq!(r.fail_closed.records, 1);
        let pct_shapes = r.compatible_pct_shapes.unwrap_or_default();
        let pct_records = r.compatible_pct_records.unwrap_or_default();
        assert!((pct_shapes - 50.0).abs() < 1e-9, "{pct_shapes}");
        assert!((pct_records - 75.0).abs() < 1e-9, "{pct_records}");
    }

    #[test]
    fn markdown_redacted_has_no_literals() {
        let mut a = Analyzer::new(false);
        a.add_line(&sql_line(
            "SELECT COUNT(*) FROM wiki WHERE user = 'alice@example.com' AND added > 12345",
        ));
        // Privacy vectors that a naive masker misses:
        // 1. SQL comments carry customer text.
        a.add_line(&sql_line(
            "SELECT COUNT(*) FROM wiki WHERE x = 'ok' -- comment-secret-SSN 078-05-1120",
        ));
        a.add_line(&sql_line(
            "SELECT /* block-comment-secret */ COUNT(*) FROM wiki WHERE y = 2",
        ));
        // 2. A backslash-escaped quote must not end the literal early.
        a.add_line(&sql_line(
            r"SELECT concat(x,'abc\'escape-secret') FROM wiki",
        ));
        // 3. A native query with an unanticipated key whose value sits
        //    where a structurally whitelisted key name ("type") is
        //    expected.
        a.add_line(
            &serde_json::json!({
                "queryType": "timeseries", "dataSource": "wiki",
                "granularity": "all", "intervals": ["2024-01-01/2024-01-02"],
                "aggregations": [{"type": "count", "name": "rows"}],
                "future": {"type": "native-path-secret"}
            })
            .to_string(),
        );
        let r = a.finish();
        let md = render_markdown(&r, 20, true);
        let js = render_json(&r, true).unwrap_or_default();
        for leaked in [
            "alice@example.com",
            "12345",
            "comment-secret-SSN",
            "078-05-1120",
            "block-comment-secret",
            "escape-secret",
            "native-path-secret",
        ] {
            assert!(!md.contains(leaked), "markdown leaked {leaked:?}:\n{md}");
            assert!(!js.contains(leaked), "json leaked {leaked:?}:\n{js}");
        }
    }

    #[test]
    fn oversized_lines_are_counted_and_skipped() {
        // A pathological log line (e.g. a 500 MiB "query") must be
        // skipped and counted, never parsed/allocated as a query. The
        // guard limit is modest so the test stays cheap: one byte over.
        let padding = "p".repeat(MAX_LINE_BYTES - 42);
        let big = sql_line(&format!("SELECT COUNT(*) FROM wiki WHERE x = '{padding}'"));
        assert!(big.len() > MAX_LINE_BYTES, "test line must exceed the cap");
        let mut a = Analyzer::new(false);
        a.add_line(&big);
        a.add_line(&sql_line("SELECT COUNT(*) FROM wiki"));
        let r = a.finish();
        assert_eq!(r.input.lines, 2);
        assert_eq!(r.input.oversized_lines, 1);
        assert_eq!(
            r.input.query_records, 1,
            "oversized line must be skipped, not parsed as a query"
        );
    }

    #[test]
    fn feed_reader_streams_oversized_lines_without_buffering() {
        // The reader path discards an oversized line's bytes as they
        // stream past (never holding more than MAX_LINE_BYTES) and still
        // parses the following lines.
        let mut data: Vec<u8> = Vec::new();
        data.extend_from_slice(b"{\"query\":\"SELECT '");
        data.extend_from_slice(&vec![b'x'; MAX_LINE_BYTES]);
        data.extend_from_slice(b"'\"}\n");
        data.extend_from_slice(sql_line("SELECT COUNT(*) FROM wiki").as_bytes());
        data.push(b'\n');
        let mut a = Analyzer::new(false);
        feed_reader(&mut a, data.as_slice()).expect("feeding succeeds");
        let r = a.finish();
        assert_eq!(r.input.lines, 2);
        assert_eq!(r.input.oversized_lines, 1);
        assert_eq!(r.input.query_records, 1);
        // The oversized skip is visible in the report.
        let md = render_markdown(&r, 20, true);
        assert!(
            md.contains("| oversized lines (skipped unread) | 1 |"),
            "{md}"
        );
    }

    #[test]
    fn no_redact_keeps_exemplar() {
        let mut a = Analyzer::new(true);
        a.add_line(&sql_line("SELECT COUNT(*) FROM wiki WHERE lang = 'en'"));
        let r = a.finish();
        assert_eq!(
            r.shapes[0].exemplar.as_deref(),
            Some("SELECT COUNT(*) FROM wiki WHERE lang = 'en'")
        );
        let md = render_markdown(&r, 20, false);
        // Supported shape: exemplar only rendered for incompatible shapes,
        // but the JSON report carries it.
        let js = render_json(&r, false).unwrap_or_default();
        assert!(js.contains("lang = 'en'"), "{md}");
    }

    #[test]
    fn empty_log_reports_no_percentages() {
        let a = Analyzer::new(false);
        let r = a.finish();
        assert!(r.compatible_pct_shapes.is_none());
        let md = render_markdown(&r, 20, true);
        assert!(md.contains("No client queries found"));
    }
}
