// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 41.OO segment artifact format.
//!
//! For Wave 5 of the v1.0 plan we ship a deliberately **simple** segment
//! artifact format: JSON Lines, with a single header line followed by
//! one row per line. This keeps the historical's segment loader honest
//! (no hidden binary readers, no shared in-process state with the
//! single-binary path) while remaining fast enough for an end-to-end
//! demonstration of the cross-role wire.
//!
//! Real Druid-aligned segment files (columnar, dictionary-encoded,
//! roaring bitmaps) live in `ferrodruid-segment` and are out of scope
//! for the role-split path until W6.
//!
//! ## Wire shape
//!
//! ```text
//! {"segmentId":"wiki_v0_0","dataSource":"wikipedia","columns":[
//!   {"name":"__time","type":"long"},
//!   {"name":"page","type":"string"},
//!   {"name":"count","type":"long"}
//! ]}
//! {"__time":1714694400000,"page":"home","count":3}
//! {"__time":1714694460000,"page":"about","count":1}
//! ```
//!
//! - The first non-empty line is the header (a [`SegmentHeader`]).
//! - Every subsequent non-empty line is a row, encoded as a JSON object
//!   keyed by column name.
//! - Blank lines are ignored.
//! - The artifact is human-readable and hand-writable, so an integration
//!   test can compose a fixture in a few lines without pulling in
//!   compression or dictionary encoders.

use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors emitted when reading or writing a segment artifact.
#[derive(Debug, Error)]
pub enum SegmentArtifactError {
    /// I/O error while reading or writing the artifact file.
    #[error("segment artifact I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON decode error on a line of the artifact.
    #[error("segment artifact JSON parse error at line {line}: {source}")]
    Parse {
        /// 1-based line number where the error occurred.
        line: usize,
        /// Underlying serde error.
        source: serde_json::Error,
    },
    /// The artifact has no header line.
    #[error("segment artifact is missing a header line")]
    MissingHeader,
    /// A row line is missing the `__time` column required for
    /// time-bucketed query types.
    #[error("segment artifact row at line {line} missing __time column")]
    MissingTime {
        /// 1-based line number where the violation occurred.
        line: usize,
    },
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, SegmentArtifactError>;

/// Druid-compatible column type label.
///
/// We carry only the four shapes Wave 41.OO query execution needs;
/// dictionary-encoded `string` arrays, complex sketches, and `multi`
/// dimensions are deferred to W6 alongside the real columnar reader.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    /// Signed 64-bit integer column (also used for the `__time`
    /// column).
    Long,
    /// IEEE-754 double-precision float column.
    Double,
    /// UTF-8 string column.
    String,
    /// JSON-typed pass-through column. Used for opaque dimensions the
    /// query does not need to type-narrow.
    Json,
}

/// Column declaration carried in the segment header.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnSpec {
    /// Column name (e.g. `__time`, `page`, `count`).
    pub name: String,
    /// Logical type.
    #[serde(rename = "type")]
    pub typ: ColumnType,
}

/// Segment artifact header.
///
/// The header carries the Druid-aligned identity (`segment_id`,
/// `data_source`) plus the column schema. Every row in the artifact
/// must conform to this schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Stable segment identifier (Druid-aligned shape).
    #[serde(rename = "segmentId")]
    pub segment_id: String,
    /// Datasource the segment belongs to.
    #[serde(rename = "dataSource")]
    pub data_source: String,
    /// Schema for each column in the segment, in projection order.
    pub columns: Vec<ColumnSpec>,
}

/// One row of a segment, keyed by column name.
///
/// We use [`serde_json::Map`] (preserving insertion order) so the
/// artifact's wire shape is a literal JSON object — easy to hand-write
/// and easy to diff in test failures.
pub type SegmentRow = serde_json::Map<String, serde_json::Value>;

/// In-memory parsed segment artifact.
///
/// Produced by [`Segment::read_jsonl`] / [`Segment::parse_jsonl`] and
/// consumed by the historical's segment store and the lightweight
/// query executors in `ferrodruid-rpc::native_query`.
#[derive(Debug, Clone)]
pub struct Segment {
    /// Schema + identity.
    pub header: SegmentHeader,
    /// Row payload, in artifact order.
    pub rows: Vec<SegmentRow>,
}

impl Segment {
    /// Read a JSON-Lines artifact from disk.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentArtifactError`] if the file cannot be read,
    /// any line fails to parse as JSON, the header is missing, or any
    /// row is missing the `__time` column.
    pub fn read_jsonl(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        let text = String::from_utf8(bytes).map_err(|e| {
            SegmentArtifactError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            ))
        })?;
        Self::parse_jsonl(&text)
    }

    /// Async variant of [`Segment::read_jsonl`].
    ///
    /// # Errors
    ///
    /// Same conditions as [`Segment::read_jsonl`].
    pub async fn read_jsonl_async(path: &Path) -> Result<Self> {
        let bytes = tokio::fs::read(path).await?;
        let text = String::from_utf8(bytes).map_err(|e| {
            SegmentArtifactError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            ))
        })?;
        Self::parse_jsonl(&text)
    }

    /// Parse a JSON-Lines artifact from an in-memory string. Helpful
    /// for tests that compose a fixture inline.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentArtifactError`] on header / row parse errors
    /// or a missing `__time` column on a row.
    pub fn parse_jsonl(text: &str) -> Result<Self> {
        let mut header: Option<SegmentHeader> = None;
        let mut rows: Vec<SegmentRow> = Vec::new();

        for (idx, raw) in text.lines().enumerate() {
            let line_no = idx + 1;
            let line = raw.trim();
            if line.is_empty() {
                continue;
            }
            if header.is_none() {
                let parsed: SegmentHeader =
                    serde_json::from_str(line).map_err(|e| SegmentArtifactError::Parse {
                        line: line_no,
                        source: e,
                    })?;
                header = Some(parsed);
                continue;
            }
            let row: SegmentRow =
                serde_json::from_str(line).map_err(|e| SegmentArtifactError::Parse {
                    line: line_no,
                    source: e,
                })?;
            if !row.contains_key("__time") {
                return Err(SegmentArtifactError::MissingTime { line: line_no });
            }
            rows.push(row);
        }

        let header = header.ok_or(SegmentArtifactError::MissingHeader)?;
        Ok(Self { header, rows })
    }

    /// Write a JSON-Lines artifact to disk. Used by tests and by future
    /// ingestion executors that produce segment artifacts.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentArtifactError`] on I/O or JSON encoding errors.
    pub fn write_jsonl(&self, path: &Path) -> Result<()> {
        let text = self.encode_jsonl()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, text)?;
        Ok(())
    }

    /// Encode this segment as a JSON-Lines string.
    ///
    /// # Errors
    ///
    /// Returns a [`SegmentArtifactError::Io`] wrapping the underlying
    /// `serde_json` error if the header or any row fails to serialise.
    pub fn encode_jsonl(&self) -> Result<String> {
        let mut out = String::new();
        let header_line = serde_json::to_string(&self.header).map_err(|e| {
            SegmentArtifactError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            ))
        })?;
        out.push_str(&header_line);
        out.push('\n');
        for row in &self.rows {
            let line = serde_json::to_string(row).map_err(|e| {
                SegmentArtifactError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                ))
            })?;
            out.push_str(&line);
            out.push('\n');
        }
        Ok(out)
    }

    /// Number of rows in the segment.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Read the `__time` column for a given row.
    ///
    /// Returns `None` when the row is missing `__time` (already filtered
    /// out by [`Segment::parse_jsonl`]) or carries a non-integer value.
    #[must_use]
    pub fn row_time(&self, row_idx: usize) -> Option<i64> {
        self.rows.get(row_idx)?.get("__time")?.as_i64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_columns() -> Vec<ColumnSpec> {
        vec![
            ColumnSpec {
                name: "__time".into(),
                typ: ColumnType::Long,
            },
            ColumnSpec {
                name: "page".into(),
                typ: ColumnType::String,
            },
            ColumnSpec {
                name: "count".into(),
                typ: ColumnType::Long,
            },
        ]
    }

    fn fixture_jsonl() -> String {
        r#"{"segmentId":"wiki_v0_0","dataSource":"wikipedia","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"},{"name":"count","type":"long"}]}
{"__time":1714694400000,"page":"home","count":3}
{"__time":1714694460000,"page":"about","count":1}
{"__time":1714694520000,"page":"home","count":2}
"#
        .to_string()
    }

    #[test]
    fn parse_jsonl_extracts_header_and_rows() {
        let seg = Segment::parse_jsonl(&fixture_jsonl()).expect("parse");
        assert_eq!(seg.header.segment_id, "wiki_v0_0");
        assert_eq!(seg.header.data_source, "wikipedia");
        assert_eq!(seg.header.columns, header_columns());
        assert_eq!(seg.num_rows(), 3);
        assert_eq!(seg.row_time(0), Some(1714694400000));
        assert_eq!(seg.row_time(2), Some(1714694520000));
    }

    #[test]
    fn parse_jsonl_skips_blank_lines() {
        let text = "\n\n{\"segmentId\":\"s\",\"dataSource\":\"d\",\"columns\":[{\"name\":\"__time\",\"type\":\"long\"}]}\n\n{\"__time\":1}\n\n";
        let seg = Segment::parse_jsonl(text).expect("parse");
        assert_eq!(seg.num_rows(), 1);
    }

    #[test]
    fn parse_jsonl_missing_header_errors() {
        let err = Segment::parse_jsonl("").expect_err("missing header");
        match err {
            SegmentArtifactError::MissingHeader => {}
            other => panic!("expected MissingHeader, got {other:?}"),
        }
    }

    #[test]
    fn parse_jsonl_missing_time_errors() {
        let text = r#"{"segmentId":"s","dataSource":"d","columns":[{"name":"page","type":"string"}]}
{"page":"home"}
"#;
        let err = Segment::parse_jsonl(text).expect_err("missing time");
        match err {
            SegmentArtifactError::MissingTime { line } => assert_eq!(line, 2),
            other => panic!("expected MissingTime, got {other:?}"),
        }
    }

    #[test]
    fn parse_jsonl_bad_json_errors_with_line() {
        let text = "{\"segmentId\":\"s\",\"dataSource\":\"d\",\"columns\":[]}\nnot-json\n";
        let err = Segment::parse_jsonl(text).expect_err("bad json");
        match err {
            SegmentArtifactError::Parse { line, .. } => assert_eq!(line, 2),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_via_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("seg.jsonl");
        let original = Segment::parse_jsonl(&fixture_jsonl()).expect("parse");
        original.write_jsonl(&path).expect("write");
        let read_back = Segment::read_jsonl(&path).expect("read");
        assert_eq!(read_back.header, original.header);
        assert_eq!(read_back.rows.len(), original.rows.len());
        for (a, b) in read_back.rows.iter().zip(original.rows.iter()) {
            assert_eq!(a, b);
        }
    }

    #[tokio::test]
    async fn read_jsonl_async_matches_sync() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("seg.jsonl");
        let original = Segment::parse_jsonl(&fixture_jsonl()).expect("parse");
        original.write_jsonl(&path).expect("write");
        let async_seg = Segment::read_jsonl_async(&path).await.expect("read async");
        assert_eq!(async_seg.header, original.header);
        assert_eq!(async_seg.num_rows(), original.num_rows());
    }

    #[test]
    fn encode_jsonl_emits_header_first() {
        let seg = Segment::parse_jsonl(&fixture_jsonl()).expect("parse");
        let text = seg.encode_jsonl().expect("encode");
        let mut lines = text.lines();
        let first = lines.next().expect("header line");
        assert!(first.contains("\"segmentId\":\"wiki_v0_0\""));
        let mut row_count = 0;
        for line in lines {
            if line.is_empty() {
                continue;
            }
            row_count += 1;
        }
        assert_eq!(row_count, 3);
    }

    #[test]
    fn column_type_round_trips_lowercase() {
        for typ in [
            ColumnType::Long,
            ColumnType::Double,
            ColumnType::String,
            ColumnType::Json,
        ] {
            let s = serde_json::to_string(&typ).expect("ser");
            let back: ColumnType = serde_json::from_str(&s).expect("de");
            assert_eq!(back, typ);
        }
    }
}
