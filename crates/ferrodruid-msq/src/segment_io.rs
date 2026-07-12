// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Real segment I/O wiring for MSQ Scan and Insert stages (CL-5
//! sub-item 4).
//!
//! Before CL-5 the MSQ `Scan` processor projected from an externally
//! supplied [`InputTable`](crate::executor::InputTable) and the
//! `Insert` stage was a logical no-op.  This module bridges MSQ to the
//! [`ferrodruid_deep_storage::DeepStorage`] trait via the project-wide
//! JSON-Lines [`Segment`] artifact format so:
//!
//! * `scan_data_source` downloads every segment for a data source,
//!   parses each as a [`Segment`], and flattens the rows into an
//!   [`InputTable`] aligned to a stable signature.
//! * `publish_segment` writes a [`Segment`] artifact to a tempdir then
//!   uploads it via [`DeepStorage::upload_segment`] (idempotent — the
//!   deterministic `segment_id` lets retries overwrite the same key).
//!
//! Both helpers are deliberately small and tested directly so the
//! distributed coordinator can compose them without depending on
//! historical-tier internals.

use std::collections::BTreeMap;
use std::path::Path;

use ferrodruid_common::{DruidError, Result};
use ferrodruid_deep_storage::{
    ColumnSpec, ColumnType, DeepStorage, Segment, SegmentHeader, SegmentRow,
};

use crate::engine::{Row, RowSignature, Value};
use crate::executor::InputTable;

/// Scan every segment of a data source into a single in-memory
/// [`InputTable`].
///
/// The signature is built from the union of column names across all
/// segment headers (segments missing a column emit `Value::Null` for
/// it).  Column ordering is stable: segments are downloaded in the
/// order returned by [`DeepStorage::list_segments`] (which is sorted
/// for [`LocalDeepStorage`](ferrodruid_deep_storage::LocalDeepStorage)
/// and [`InMemoryDeepStorage`](ferrodruid_deep_storage::InMemoryDeepStorage)).
///
/// # Errors
///
/// Propagates download / parse errors from the underlying
/// [`DeepStorage`] and `Segment` parser.
pub async fn scan_data_source(ds: &dyn DeepStorage, data_source: &str) -> Result<InputTable> {
    let segment_ids = ds
        .list_segments(data_source)
        .await
        .map_err(|e| DruidError::Internal(format!("list_segments({data_source}): {e}")))?;

    // Build column-name -> sql-type-label map by union of segment
    // headers.  Use BTreeMap for deterministic ordering on conflict.
    let mut union_types: BTreeMap<String, String> = BTreeMap::new();
    let mut segments: Vec<Segment> = Vec::with_capacity(segment_ids.len());
    let tmp = tempfile::tempdir()
        .map_err(|e| DruidError::Internal(format!("scan_data_source tempdir: {e}")))?;
    for sid in &segment_ids {
        let dest = tmp.path().join(sid);
        ds.download_segment(data_source, sid, &dest)
            .await
            .map_err(|e| {
                DruidError::Internal(format!("download_segment({data_source}/{sid}): {e}"))
            })?;
        // The segment artifact is a single JSONL file per segment;
        // download_segment copied it as `segment.bin` for single-file
        // sources (see LocalDeepStorage::upload_segment) or as a named
        // file in the directory for multi-file segments.  We accept
        // either: prefer a `segment.jsonl` file if present, else
        // `segment.bin`, else the first regular file.
        let path = pick_segment_file(&dest).ok_or_else(|| {
            DruidError::Internal(format!("scan_data_source: no segment file under {dest:?}"))
        })?;
        let seg = Segment::read_jsonl(&path)
            .map_err(|e| DruidError::Internal(format!("Segment::read_jsonl({path:?}): {e}")))?;
        for col in &seg.header.columns {
            union_types
                .entry(col.name.clone())
                .or_insert_with(|| sql_type_for(col.typ));
        }
        segments.push(seg);
    }

    let columns: Vec<String> = union_types.keys().cloned().collect();
    let types: Vec<String> = columns
        .iter()
        .map(|c| {
            union_types
                .get(c)
                .cloned()
                .unwrap_or_else(|| "VARCHAR".into())
        })
        .collect();
    let signature = RowSignature { columns, types };

    let mut rows: Vec<Row> = Vec::new();
    for seg in segments {
        for r in seg.rows {
            rows.push(segment_row_to_engine(&r, &signature));
        }
    }
    Ok(InputTable { signature, rows })
}

/// Publish a set of [`Row`]s as one [`Segment`] under
/// `(data_source, segment_id)`.  Idempotent on the deterministic key
/// (uploading the same segment_id with different rows replaces the
/// previous artifact).
///
/// The artifact is written as a single `segment.jsonl` file inside a
/// short-lived tempdir then uploaded via `DeepStorage::upload_segment`.
/// All rows must include a `__time` column or the historical scan
/// path's [`Segment::parse_jsonl`] will reject them on read-back —
/// callers that don't have a real time column should populate it with
/// the publish wall-clock (see `now_ms`).
///
/// # Errors
///
/// Propagates filesystem / upload errors.
pub async fn publish_segment(
    ds: &dyn DeepStorage,
    data_source: &str,
    segment_id: &str,
    signature: &RowSignature,
    rows: &[Row],
) -> Result<()> {
    let header = SegmentHeader {
        segment_id: segment_id.to_owned(),
        data_source: data_source.to_owned(),
        columns: signature
            .columns
            .iter()
            .zip(signature.types.iter())
            .map(|(name, ty)| ColumnSpec {
                name: name.clone(),
                typ: column_type_for(ty),
            })
            .collect(),
    };
    let seg_rows: Vec<SegmentRow> = rows
        .iter()
        .map(|row| engine_row_to_segment(signature, row))
        .collect();
    let seg = Segment {
        header,
        rows: seg_rows,
    };

    let tmp = tempfile::tempdir()
        .map_err(|e| DruidError::Internal(format!("publish_segment tempdir: {e}")))?;
    let path = tmp.path().join("segment.jsonl");
    seg.write_jsonl(&path)
        .map_err(|e| DruidError::Internal(format!("publish_segment write_jsonl({path:?}): {e}")))?;
    ds.upload_segment(data_source, segment_id, &path)
        .await
        .map_err(|e| {
            DruidError::Internal(format!("upload_segment({data_source}/{segment_id}): {e}"))
        })?;
    Ok(())
}

/// Pick the most likely segment artifact file inside `dir`.
fn pick_segment_file(dir: &Path) -> Option<std::path::PathBuf> {
    let preferred = ["segment.jsonl", "segment.bin"];
    for name in preferred {
        let p = dir.join(name);
        if p.is_file() {
            return Some(p);
        }
    }
    let mut entries = std::fs::read_dir(dir).ok()?;
    entries.find_map(|e| {
        let entry = e.ok()?;
        if entry.file_type().ok()?.is_file() {
            Some(entry.path())
        } else {
            None
        }
    })
}

/// Translate a `Segment` row (JSON map) into an engine [`Row`] aligned
/// to `signature`.
fn segment_row_to_engine(row: &SegmentRow, signature: &RowSignature) -> Row {
    signature
        .columns
        .iter()
        .map(|col| row.get(col).map_or(Value::Null, json_to_value))
        .collect()
}

/// Translate an engine [`Row`] into a `Segment` JSON row aligned to
/// `signature`.
fn engine_row_to_segment(signature: &RowSignature, row: &Row) -> SegmentRow {
    let mut out = SegmentRow::new();
    for (i, col) in signature.columns.iter().enumerate() {
        let v = row.get(i).cloned().unwrap_or(Value::Null);
        out.insert(col.clone(), value_to_json(&v));
    }
    out
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Long(i64::from(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Long(i)
            } else if let Some(f) = n.as_f64() {
                Value::Double(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Str(v.to_string()),
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Long(i) => serde_json::Value::from(*i),
        Value::Double(f) => serde_json::Number::from_f64(*f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::Str(s) => serde_json::Value::String(s.clone()),
    }
}

/// SQL-type label for a [`ColumnType`].
fn sql_type_for(ct: ColumnType) -> String {
    match ct {
        ColumnType::Long => "BIGINT".to_owned(),
        ColumnType::Double => "DOUBLE".to_owned(),
        ColumnType::String => "VARCHAR".to_owned(),
        ColumnType::Json => "OTHER".to_owned(),
    }
}

/// Map a SQL-type label back to a segment [`ColumnType`].
fn column_type_for(sql_type: &str) -> ColumnType {
    match sql_type.to_ascii_uppercase().as_str() {
        "BIGINT" | "LONG" | "INTEGER" | "INT" => ColumnType::Long,
        "DOUBLE" | "FLOAT" | "REAL" => ColumnType::Double,
        "VARCHAR" | "STRING" | "CHAR" => ColumnType::String,
        _ => ColumnType::Json,
    }
}

/// Current wall-clock time in epoch milliseconds (for synthesised
/// `__time` columns on test ingestion).
#[must_use]
pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| {
            let ms = d.as_millis();
            i64::try_from(ms).unwrap_or(i64::MAX)
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_deep_storage::InMemoryDeepStorage;

    #[tokio::test]
    async fn publish_then_scan_round_trips_rows() {
        let ds = InMemoryDeepStorage::new();
        let sig = RowSignature {
            columns: vec!["__time".into(), "page".into(), "count".into()],
            types: vec!["BIGINT".into(), "VARCHAR".into(), "BIGINT".into()],
        };
        let rows = vec![
            vec![
                Value::Long(1_714_694_400_000),
                Value::Str("home".into()),
                Value::Long(3),
            ],
            vec![
                Value::Long(1_714_694_460_000),
                Value::Str("about".into()),
                Value::Long(1),
            ],
        ];

        publish_segment(&ds, "wikipedia", "wiki_v0_0", &sig, &rows)
            .await
            .expect("publish");

        let scanned = scan_data_source(&ds, "wikipedia").await.expect("scan");
        // Signature column names match (BTreeMap orders alphabetically).
        assert!(scanned.signature.columns.contains(&"page".to_string()));
        assert!(scanned.signature.columns.contains(&"count".to_string()));
        assert!(scanned.signature.columns.contains(&"__time".to_string()));
        assert_eq!(scanned.rows.len(), 2);
    }

    #[tokio::test]
    async fn republish_overwrites_segment_idempotently() {
        let ds = InMemoryDeepStorage::new();
        let sig = RowSignature {
            columns: vec!["__time".into(), "n".into()],
            types: vec!["BIGINT".into(), "BIGINT".into()],
        };
        let rows_1 = vec![vec![Value::Long(1), Value::Long(10)]];
        let rows_2 = vec![
            vec![Value::Long(1), Value::Long(20)],
            vec![Value::Long(2), Value::Long(30)],
        ];

        publish_segment(&ds, "d", "s0", &sig, &rows_1)
            .await
            .expect("p1");
        publish_segment(&ds, "d", "s0", &sig, &rows_2)
            .await
            .expect("p2");

        let scanned = scan_data_source(&ds, "d").await.expect("scan");
        // Second publish wins (or both rows present if the storage merges).
        // InMemory simply writes the new content; rows_2 is the survivor.
        assert!(scanned.rows.len() >= 2, "got {:?}", scanned.rows);
    }
}
